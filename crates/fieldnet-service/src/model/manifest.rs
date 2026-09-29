use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use fieldnet_content::ContentId;
use fieldnet_core::{NodeId, Timestamp};
use fieldnet_identity::NodeIdentity;
use serde::{Deserialize, Serialize};

use super::capability::{ServiceCapability, ServicePermissions};
use super::endpoint::{ServiceEndpoint, ServiceProtocol};
use super::id::{ServiceId, ServiceInstanceId};
use super::version::ServiceVersion;
use crate::error::ServiceError;

pub const CURRENT_SERVICE_MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Authoritative, cryptographically signed service declaration.
///
/// A `ServiceManifest` contains all information required to discover, authenticate,
/// and route requests to a concrete service instance on the FIELDNET mesh.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ServiceManifest {
    /// Schema format version (v1).
    pub schema_version: u32,
    /// Logical service interface identity (e.g. "files.service", "fieldnet.search").
    pub service_id: ServiceId,
    /// Unique instance identifier.
    pub instance_id: ServiceInstanceId,
    /// Sovereign Ed25519 identity of the hosting node.
    pub provider_node_id: NodeId,
    /// Provider's 32-byte Ed25519 public verifying key.
    #[serde(with = "hex")]
    pub provider_public_key: [u8; 32],
    /// SemVer service version.
    pub version: ServiceVersion,
    /// Service communication protocol dialect.
    pub protocol: ServiceProtocol,
    /// Reachability endpoint specification.
    pub endpoint: ServiceEndpoint,
    /// Advertised capabilities and feature tags.
    pub capabilities: Vec<ServiceCapability>,
    /// Access control constraints.
    pub permissions: ServicePermissions,
    /// Monotonically increasing sequence number from the provider node.
    pub sequence: u64,
    /// Advertisement time-to-live in seconds.
    pub ttl_seconds: u32,
    /// Creation timestamp (milliseconds since epoch).
    pub created_at: Timestamp,
    /// Absolute expiration timestamp (milliseconds since epoch).
    pub expires_at: Timestamp,
    /// Human-readable title.
    pub display_name: String,
    /// Human-readable description.
    pub description: String,
    /// Optional ContentId of service icon asset in Phase 07 CAS.
    pub icon_content_id: Option<ContentId>,
    /// Optional ContentId of service documentation asset in Phase 07 CAS.
    pub documentation_content_id: Option<ContentId>,
    /// Ed25519 digital signature over canonical signing bytes.
    #[serde(with = "hex")]
    pub signature: [u8; 64],
}

impl ServiceManifest {
    /// Constructs canonical bytes for Ed25519 signing and verification.
    ///
    /// Canonical layout guarantees zero ambiguity, deterministic byte order,
    /// and signature malleability protection across architectures.
    pub fn canonical_signing_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(512);

        // Magic header
        buf.extend_from_slice(b"FIELDNET_SERVICE_MANIFEST_V1");

        // Schema version
        buf.extend_from_slice(&self.schema_version.to_be_bytes());

        // Service ID
        let svc_bytes = self.service_id.as_str().as_bytes();
        buf.extend_from_slice(&(svc_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(svc_bytes);

        // Instance ID
        let inst_bytes = self.instance_id.as_str().as_bytes();
        buf.extend_from_slice(&(inst_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(inst_bytes);

        // Provider NodeId & Public Key
        buf.extend_from_slice(self.provider_node_id.as_bytes());
        buf.extend_from_slice(&self.provider_public_key);

        // Version
        buf.extend_from_slice(&self.version.major.to_be_bytes());
        buf.extend_from_slice(&self.version.minor.to_be_bytes());
        buf.extend_from_slice(&self.version.patch.to_be_bytes());

        // Protocol & Endpoint
        buf.push(self.protocol.as_u8());
        let endpoint_bytes = self.endpoint.canonical_bytes();
        buf.extend_from_slice(&(endpoint_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(&endpoint_bytes);

        // Capabilities (lexicographically sorted for canonical determinism)
        let mut sorted_caps: Vec<&str> = self.capabilities.iter().map(|c| c.as_str()).collect();
        sorted_caps.sort_unstable();
        buf.extend_from_slice(&(sorted_caps.len() as u16).to_be_bytes());
        for cap in sorted_caps {
            let cap_bytes = cap.as_bytes();
            buf.extend_from_slice(&(cap_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(cap_bytes);
        }

        // Permissions
        buf.push(if self.permissions.is_public { 1u8 } else { 0u8 });
        buf.push(if self.permissions.requires_capability_token {
            1u8
        } else {
            0u8
        });
        let mut sorted_roles: Vec<&str> = self
            .permissions
            .required_roles
            .iter()
            .map(|r| r.as_str())
            .collect();
        sorted_roles.sort_unstable();
        buf.extend_from_slice(&(sorted_roles.len() as u16).to_be_bytes());
        for role in sorted_roles {
            let role_bytes = role.as_bytes();
            buf.extend_from_slice(&(role_bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(role_bytes);
        }

        // Monotonic sequence and TTL
        buf.extend_from_slice(&self.sequence.to_be_bytes());
        buf.extend_from_slice(&self.ttl_seconds.to_be_bytes());
        buf.extend_from_slice(&self.created_at.as_millis().to_be_bytes());
        buf.extend_from_slice(&self.expires_at.as_millis().to_be_bytes());

        // Display Name & Description
        let name_bytes = self.display_name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(name_bytes);

        let desc_bytes = self.description.as_bytes();
        buf.extend_from_slice(&(desc_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(desc_bytes);

        // Content IDs (Icon & Docs)
        match &self.icon_content_id {
            Some(cid) => {
                buf.push(1u8);
                buf.extend_from_slice(cid.as_bytes());
            }
            None => buf.push(0u8),
        }

        match &self.documentation_content_id {
            Some(cid) => {
                buf.push(1u8);
                buf.extend_from_slice(cid.as_bytes());
            }
            None => buf.push(0u8),
        }

        buf
    }

    /// Cryptographically signs the manifest using the local sovereign `NodeIdentity`.
    pub fn sign(&mut self, identity: &NodeIdentity) -> Result<(), ServiceError> {
        self.provider_node_id = identity.node_id();
        self.provider_public_key = identity.public_key_bytes();

        let canonical_bytes = self.canonical_signing_bytes();
        self.signature = identity.sign(&canonical_bytes);
        Ok(())
    }

    /// Verifies the cryptographic authenticity and integrity of the manifest.
    pub fn verify_signature(&self) -> Result<(), ServiceError> {
        if self.schema_version != CURRENT_SERVICE_MANIFEST_SCHEMA_VERSION {
            return Err(ServiceError::UnsupportedSchemaVersion(self.schema_version));
        }

        // 1. Verify provider_public_key matches provider_node_id
        let derived_node_id = NodeId::from_ed25519_public_key(&self.provider_public_key);
        if derived_node_id != self.provider_node_id {
            return Err(ServiceError::MismatchedProviderKey {
                expected: derived_node_id,
                got: self.provider_node_id,
            });
        }

        // 2. Compute canonical signing bytes
        let canonical_bytes = self.canonical_signing_bytes();

        // 3. Verify Ed25519 signature
        let verifying_key = VerifyingKey::from_bytes(&self.provider_public_key).map_err(|e| {
            ServiceError::InvalidSignature(format!("Invalid public key bytes: {}", e))
        })?;
        let signature = Signature::from_bytes(&self.signature);

        verifying_key
            .verify(&canonical_bytes, &signature)
            .map_err(|e| ServiceError::InvalidSignature(format!("Verification failed: {}", e)))
    }

    /// Validates structural consistency and timestamps.
    pub fn validate_basics(&self, now: Timestamp) -> Result<(), ServiceError> {
        if self.schema_version != CURRENT_SERVICE_MANIFEST_SCHEMA_VERSION {
            return Err(ServiceError::UnsupportedSchemaVersion(self.schema_version));
        }

        if self.expires_at.as_millis() <= self.created_at.as_millis() {
            return Err(ServiceError::InvalidExpiration {
                now: now.as_millis(),
                expires_at: self.expires_at.as_millis(),
            });
        }

        if self.is_expired(now) {
            return Err(ServiceError::InvalidExpiration {
                now: now.as_millis(),
                expires_at: self.expires_at.as_millis(),
            });
        }

        if self.ttl_seconds == 0 {
            return Err(ServiceError::InvalidExpiration {
                now: now.as_millis(),
                expires_at: self.expires_at.as_millis(),
            });
        }

        if self.display_name.len() > 256 {
            return Err(ServiceError::InvalidServiceId(
                "Display name exceeds 256 characters".to_string(),
            ));
        }

        if self.description.len() > 2048 {
            return Err(ServiceError::InvalidServiceId(
                "Description exceeds 2048 characters".to_string(),
            ));
        }

        Ok(())
    }

    /// Checks if the manifest has expired relative to a given timestamp.
    pub fn is_expired(&self, now: Timestamp) -> bool {
        now.as_millis() >= self.expires_at.as_millis()
    }
}

/// Fluent builder for constructing `ServiceManifest` instances.
pub struct ServiceManifestBuilder {
    service_id: ServiceId,
    instance_id: Option<ServiceInstanceId>,
    version: ServiceVersion,
    protocol: ServiceProtocol,
    endpoint: ServiceEndpoint,
    capabilities: Vec<ServiceCapability>,
    permissions: ServicePermissions,
    sequence: u64,
    ttl_seconds: u32,
    created_at: Option<Timestamp>,
    display_name: String,
    description: String,
    icon_content_id: Option<ContentId>,
    documentation_content_id: Option<ContentId>,
}

impl ServiceManifestBuilder {
    pub fn new(service_id: ServiceId, version: ServiceVersion) -> Self {
        Self {
            service_id,
            instance_id: None,
            version,
            protocol: ServiceProtocol::NativeFieldnetRpc,
            endpoint: ServiceEndpoint::InternalRpc,
            capabilities: Vec::new(),
            permissions: ServicePermissions::default(),
            sequence: 1,
            ttl_seconds: 120,
            created_at: None,
            display_name: String::new(),
            description: String::new(),
            icon_content_id: None,
            documentation_content_id: None,
        }
    }

    pub fn instance_id(mut self, id: ServiceInstanceId) -> Self {
        self.instance_id = Some(id);
        self
    }

    pub fn protocol(mut self, protocol: ServiceProtocol) -> Self {
        self.protocol = protocol;
        self
    }

    pub fn endpoint(mut self, endpoint: ServiceEndpoint) -> Self {
        self.endpoint = endpoint;
        self
    }

    pub fn capability(mut self, cap: ServiceCapability) -> Self {
        self.capabilities.push(cap);
        self
    }

    pub fn capabilities(mut self, caps: Vec<ServiceCapability>) -> Self {
        self.capabilities = caps;
        self
    }

    pub fn permissions(mut self, perms: ServicePermissions) -> Self {
        self.permissions = perms;
        self
    }

    pub fn sequence(mut self, seq: u64) -> Self {
        self.sequence = seq;
        self
    }

    pub fn ttl_seconds(mut self, ttl: u32) -> Self {
        self.ttl_seconds = ttl;
        self
    }

    pub fn created_at(mut self, ts: Timestamp) -> Self {
        self.created_at = Some(ts);
        self
    }

    pub fn display_name(mut self, name: impl Into<String>) -> Self {
        self.display_name = name.into();
        self
    }

    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = desc.into();
        self
    }

    pub fn icon(mut self, icon_id: ContentId) -> Self {
        self.icon_content_id = Some(icon_id);
        self
    }

    pub fn documentation(mut self, doc_id: ContentId) -> Self {
        self.documentation_content_id = Some(doc_id);
        self
    }

    /// Builds and cryptographically signs the `ServiceManifest`.
    pub fn build_and_sign(self, identity: &NodeIdentity) -> Result<ServiceManifest, ServiceError> {
        let created_at = self.created_at.unwrap_or_else(Timestamp::now);
        let expires_at = created_at.add_seconds(self.ttl_seconds as u64);
        let instance_id = self.instance_id.unwrap_or_else(ServiceInstanceId::generate);

        let mut manifest = ServiceManifest {
            schema_version: CURRENT_SERVICE_MANIFEST_SCHEMA_VERSION,
            service_id: self.service_id,
            instance_id,
            provider_node_id: identity.node_id(),
            provider_public_key: identity.public_key_bytes(),
            version: self.version,
            protocol: self.protocol,
            endpoint: self.endpoint,
            capabilities: self.capabilities,
            permissions: self.permissions,
            sequence: self.sequence,
            ttl_seconds: self.ttl_seconds,
            created_at,
            expires_at,
            display_name: self.display_name,
            description: self.description,
            icon_content_id: self.icon_content_id,
            documentation_content_id: self.documentation_content_id,
            signature: [0u8; 64],
        };

        manifest.sign(identity)?;
        Ok(manifest)
    }
}
