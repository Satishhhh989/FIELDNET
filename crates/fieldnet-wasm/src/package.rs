use fieldnet_content::hashing::{hash_bytes, ContentHasher};
use fieldnet_content::identity::ContentId;
use fieldnet_core::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::ApplicationError;
use crate::id::PackageId;
use crate::manifest::ApplicationManifest;
use crate::signature::PackageSignature;

/// An individual content item or file contained within an application package.
///
/// Items are referenced by their Content-Addressed Storage `ContentId`.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct PackageItem {
    /// Relative path within the package bundle (e.g. "app.wasm", "assets/logo.png").
    pub path: String,

    /// Cryptographic SHA-256 Content Identifier of this item's bytes.
    pub content_id: ContentId,

    /// Size of this item in bytes.
    pub size_bytes: u64,

    /// Optional MIME type description.
    #[serde(default)]
    pub mime_type: Option<String>,
}

impl PackageItem {
    pub fn new(
        path: impl Into<String>,
        content_id: ContentId,
        size_bytes: u64,
        mime_type: Option<String>,
    ) -> Result<Self, ApplicationError> {
        let p = path.into();
        let clean = p.trim();
        if clean.is_empty() {
            return Err(ApplicationError::IntegrityError(
                "Package item path cannot be empty".to_string(),
            ));
        }

        if clean.starts_with('/') || clean.starts_with('\\') || clean.contains("..") {
            return Err(ApplicationError::IntegrityError(format!(
                "Package item path '{}' must be relative and safe",
                clean
            )));
        }

        Ok(Self {
            path: clean.to_string(),
            content_id,
            size_bytes,
            mime_type,
        })
    }

    /// Verifies that the provided bytes match this item's declared size and content hash.
    pub fn verify_bytes(&self, bytes: &[u8]) -> Result<(), ApplicationError> {
        if bytes.len() as u64 != self.size_bytes {
            return Err(ApplicationError::IntegrityError(format!(
                "Size mismatch for item '{}': expected {} bytes, got {}",
                self.path,
                self.size_bytes,
                bytes.len()
            )));
        }

        let computed = hash_bytes(bytes);
        if computed != self.content_id {
            return Err(ApplicationError::IntegrityError(format!(
                "Content hash mismatch for item '{}': expected {}, got {}",
                self.path, self.content_id, computed
            )));
        }

        Ok(())
    }
}

/// A complete, portable, signed, content-addressed FIELDNET application package.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ApplicationPackage {
    /// Stable package identity.
    pub package_id: PackageId,

    /// Complete application manifest.
    pub manifest: ApplicationManifest,

    /// All files and assets packaged with this application.
    pub items: Vec<PackageItem>,

    /// Total cumulative size of all package items in bytes.
    pub total_size_bytes: u64,

    /// Timestamp of package creation.
    pub created_at: Timestamp,

    /// Cryptographic publisher provenance signature.
    #[serde(default)]
    pub signature: Option<PackageSignature>,
}

impl ApplicationPackage {
    /// Constructs a new package, validating integrity and ensuring entrypoint presence.
    pub fn new(
        package_id: PackageId,
        manifest: ApplicationManifest,
        mut items: Vec<PackageItem>,
        created_at: Timestamp,
        signature: Option<PackageSignature>,
    ) -> Result<Self, ApplicationError> {
        manifest.validate()?;

        // Sort items deterministically by path for canonical hashing
        items.sort_by(|a, b| a.path.cmp(&b.path));

        // Check for duplicate paths
        for i in 1..items.len() {
            if items[i].path == items[i - 1].path {
                return Err(ApplicationError::IntegrityError(format!(
                    "Duplicate package item path: '{}'",
                    items[i].path
                )));
            }
        }

        // Verify entrypoint exists in package items
        let entrypoint_found = items.iter().any(|it| it.path == manifest.entrypoint);
        if !entrypoint_found {
            return Err(ApplicationError::IntegrityError(format!(
                "Package missing declared entrypoint binary '{}'",
                manifest.entrypoint
            )));
        }

        let total_size_bytes = items.iter().map(|it| it.size_bytes).sum();

        Ok(Self {
            package_id,
            manifest,
            items,
            total_size_bytes,
            created_at,
            signature,
        })
    }

    /// Computes the authoritative, deterministic cryptographic content hash (`package_hash`)
    /// of this application package using SHA-256.
    ///
    /// The hash covers:
    /// 1. Canonical manifest bytes
    /// 2. Sorted items: (path, content_id, size_bytes)
    pub fn compute_package_hash(&self) -> ContentId {
        let mut hasher = ContentHasher::new();

        // 1. Domain prefix
        hasher.update(b"FIELDNET_APP_PACKAGE_V1\n");

        // 2. Canonical manifest
        let manifest_bytes = self.manifest.canonical_bytes().unwrap_or_default();
        hasher.update(&(manifest_bytes.len() as u64).to_be_bytes());
        hasher.update(&manifest_bytes);

        // 3. Items sorted by path
        hasher.update(&(self.items.len() as u64).to_be_bytes());
        for item in &self.items {
            hasher.update(item.path.as_bytes());
            hasher.update(b"\0");
            hasher.update(item.content_id.as_bytes());
            hasher.update(&item.size_bytes.to_be_bytes());
        }

        hasher.finalize()
    }

    /// Finds a package item by its relative path.
    pub fn get_item(&self, path: &str) -> Option<&PackageItem> {
        self.items.iter().find(|it| it.path == path)
    }

    /// Finds the entrypoint package item.
    pub fn entrypoint_item(&self) -> Result<&PackageItem, ApplicationError> {
        self.get_item(&self.manifest.entrypoint).ok_or_else(|| {
            ApplicationError::IntegrityError(format!(
                "Entrypoint '{}' not found in package items",
                self.manifest.entrypoint
            ))
        })
    }

    /// Validates internal integrity and optionally checks provided in-memory item payloads.
    pub fn verify_integrity(
        &self,
        provided_payloads: Option<&HashMap<String, Vec<u8>>>,
    ) -> Result<ContentId, ApplicationError> {
        self.manifest.validate()?;

        // Verify entrypoint exists
        self.entrypoint_item()?;

        // If payloads were supplied, verify size and hashes of each item
        if let Some(payloads) = provided_payloads {
            for item in &self.items {
                let bytes = payloads.get(&item.path).ok_or_else(|| {
                    ApplicationError::IntegrityError(format!(
                        "Missing content payload for package item '{}'",
                        item.path
                    ))
                })?;
                item.verify_bytes(bytes)?;
            }
        }

        Ok(self.compute_package_hash())
    }

    /// Verifies the package signature against the computed package hash and publisher identity.
    pub fn verify_signature(&self) -> Result<(), ApplicationError> {
        let sig = self.signature.as_ref().ok_or_else(|| {
            ApplicationError::SignatureError("Package is unsigned (missing signature)".to_string())
        })?;

        // 1. Publisher in signature must match publisher in manifest
        if sig.publisher != self.manifest.publisher {
            return Err(ApplicationError::SignatureError(format!(
                "Signature publisher '{}' does not match manifest publisher '{}'",
                sig.publisher, self.manifest.publisher
            )));
        }

        // 2. Package hash must match computed package content hash
        let computed_hash = self.compute_package_hash();
        sig.verify_for_package(&computed_hash)?;

        Ok(())
    }
}
