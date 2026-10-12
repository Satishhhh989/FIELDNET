use fieldnet_content::identity::ContentId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use crate::dependencies::AppDependency;
use crate::error::ApplicationError;
use crate::id::{AppVersion, ApplicationId, PublisherIdentity};
use crate::limits::ResourceLimits;
use crate::permissions::AppPermission;

/// Authoritative schema version for FIELDNET application manifests.
pub const CURRENT_MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Versioned declaration of an application, its entrypoint, capability permissions,
/// resource limits, and external dependencies.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ApplicationManifest {
    /// Schema version for format evolvability.
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,

    /// Unique logical sovereign identity of the application.
    pub application_id: ApplicationId,

    /// Human-readable application name.
    pub name: String,

    /// Semantic version of this application package.
    pub version: AppVersion,

    /// Human-readable summary/description.
    #[serde(default)]
    pub description: String,

    /// Sovereign identity of the package publisher.
    pub publisher: PublisherIdentity,

    /// Primary execution binary or entrypoint file inside the package (e.g. "app.wasm").
    pub entrypoint: String,

    /// Declared capability permissions requested by this application.
    #[serde(default)]
    pub permissions: Vec<AppPermission>,

    /// Resource limit constraints configured for future sandbox execution.
    #[serde(default)]
    pub resource_limits: ResourceLimits,

    /// External services, applications, or content dependencies.
    #[serde(default)]
    pub dependencies: Vec<AppDependency>,

    /// Arbitrary application metadata key-values.
    #[serde(default)]
    pub package_metadata: HashMap<String, String>,
}

fn default_schema_version() -> u32 {
    CURRENT_MANIFEST_SCHEMA_VERSION
}

impl ApplicationManifest {
    /// Validates the manifest fields and integrity invariants.
    pub fn validate(&self) -> Result<(), ApplicationError> {
        if self.schema_version == 0 || self.schema_version > CURRENT_MANIFEST_SCHEMA_VERSION {
            return Err(ApplicationError::InvalidManifest(format!(
                "Unsupported schema_version: {} (supported: 1..={})",
                self.schema_version, CURRENT_MANIFEST_SCHEMA_VERSION
            )));
        }

        let name = self.name.trim();
        if name.is_empty() || name.len() > 128 {
            return Err(ApplicationError::InvalidManifest(format!(
                "Application name length must be between 1 and 128 characters, got {}",
                name.len()
            )));
        }

        if self.description.len() > 2048 {
            return Err(ApplicationError::InvalidManifest(format!(
                "Application description exceeds 2048 characters (got {})",
                self.description.len()
            )));
        }

        // Validate entrypoint
        let ep = self.entrypoint.trim();
        if ep.is_empty() {
            return Err(ApplicationError::InvalidManifest(
                "Application entrypoint cannot be empty".to_string(),
            ));
        }

        if ep.contains("..") || ep.contains('/') || ep.contains('\\') || ep.starts_with('.') {
            return Err(ApplicationError::InvalidManifest(format!(
                "Entrypoint '{}' must be a safe, relative filename without path traversal or directory separators",
                ep
            )));
        }

        if !ep.ends_with(".wasm") {
            return Err(ApplicationError::InvalidManifest(format!(
                "Entrypoint '{}' must have a .wasm extension",
                ep
            )));
        }

        // Validate permissions
        let mut seen_permissions = HashSet::new();
        for perm in &self.permissions {
            perm.validate_syntax()?;
            if !seen_permissions.insert(perm.as_str()) {
                return Err(ApplicationError::InvalidManifest(format!(
                    "Duplicate permission declared: '{}'",
                    perm
                )));
            }
        }

        // Validate resource limits
        self.resource_limits.validate()?;

        // Validate dependencies
        let mut seen_deps = HashSet::new();
        for dep in &self.dependencies {
            dep.validate()?;
            if !seen_deps.insert(&dep.identity) {
                return Err(ApplicationError::InvalidManifest(format!(
                    "Duplicate dependency declared: '{}'",
                    dep.identity
                )));
            }
        }

        Ok(())
    }

    /// Serializes the manifest into a deterministic, canonical JSON byte representation.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, ApplicationError> {
        serde_json::to_vec(self).map_err(Into::into)
    }

    /// Computes the content hash of the canonical manifest bytes.
    pub fn compute_manifest_hash(&self) -> ContentId {
        let bytes = self.canonical_bytes().unwrap_or_default();
        fieldnet_content::hashing::hash_bytes(&bytes)
    }

    /// Deserializes a manifest from a JSON string, running full validation.
    pub fn from_json_str(s: &str) -> Result<Self, ApplicationError> {
        let manifest: Self = serde_json::from_str(s)?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Serializes the manifest to pretty JSON.
    pub fn to_json_pretty(&self) -> Result<String, ApplicationError> {
        serde_json::to_string_pretty(self).map_err(Into::into)
    }
}
