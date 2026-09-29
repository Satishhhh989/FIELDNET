use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use fieldnet_content::identity::ContentId;
use fieldnet_core::Timestamp;
use fieldnet_identity::NodeIdentity;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::ApplicationError;
use crate::id::PublisherIdentity;

/// Supported signature algorithms for application packages.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SignatureAlgorithm {
    Ed25519,
}

/// Cryptographic signature establishing package provenance and publisher authenticity.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PackageSignature {
    /// Sovereign publisher identity.
    pub publisher: PublisherIdentity,

    /// Cryptographic content digest of the complete package.
    pub package_hash: ContentId,

    /// Raw 64-byte Ed25519 signature over the package hash and signing context.
    pub signature_bytes: [u8; 64],

    /// Timestamp when this package was signed.
    pub signed_at: Timestamp,

    /// Signature algorithm used.
    pub algorithm: SignatureAlgorithm,
}

impl PackageSignature {
    /// Constructs and signs package metadata using an Ed25519 private signing key seed.
    pub fn sign_with_seed(
        seed: &[u8; 32],
        package_hash: &ContentId,
        signed_at: Timestamp,
    ) -> Result<Self, ApplicationError> {
        let signing_key = SigningKey::from_bytes(seed);
        let verifying_key = signing_key.verifying_key();
        let pubkey_bytes = verifying_key.to_bytes();
        let publisher = PublisherIdentity::from_public_key(pubkey_bytes);

        let signable = Self::construct_signable_payload(package_hash, signed_at);
        let sig: Signature = signing_key.sign(&signable);

        Ok(Self {
            publisher,
            package_hash: *package_hash,
            signature_bytes: sig.to_bytes(),
            signed_at,
            algorithm: SignatureAlgorithm::Ed25519,
        })
    }

    /// Constructs and signs package metadata using a `NodeIdentity` (e.g. for testing or local publisher nodes).
    pub fn sign_with_identity(
        identity: &NodeIdentity,
        package_hash: &ContentId,
        signed_at: Timestamp,
    ) -> Self {
        let publisher = PublisherIdentity::from_public_key(identity.public_key_bytes());
        let signable = Self::construct_signable_payload(package_hash, signed_at);
        let sig_bytes = identity.sign(&signable);

        Self {
            publisher,
            package_hash: *package_hash,
            signature_bytes: sig_bytes,
            signed_at,
            algorithm: SignatureAlgorithm::Ed25519,
        }
    }

    /// Verifies the cryptographic validity of this signature against its declared publisher and package hash.
    pub fn verify(&self) -> Result<(), ApplicationError> {
        let pubkey_bytes = self.publisher.public_key_bytes();
        let verifying_key = VerifyingKey::from_bytes(&pubkey_bytes).map_err(|e| {
            ApplicationError::SignatureError(format!("Invalid publisher public key: {}", e))
        })?;

        let signable = Self::construct_signable_payload(&self.package_hash, self.signed_at);
        let sig = Signature::from_bytes(&self.signature_bytes);

        verifying_key.verify(&signable, &sig).map_err(|e| {
            ApplicationError::SignatureError(format!(
                "Signature verification failed for publisher {}: {}",
                self.publisher, e
            ))
        })?;

        Ok(())
    }

    /// Verifies that this signature corresponds to the expected package hash.
    pub fn verify_for_package(&self, expected_hash: &ContentId) -> Result<(), ApplicationError> {
        if &self.package_hash != expected_hash {
            return Err(ApplicationError::SignatureError(format!(
                "Signature package hash mismatch: expected {}, signature has {}",
                expected_hash, self.package_hash
            )));
        }

        self.verify()
    }

    /// Canonical domain-separated signable byte sequence:
    /// `b"FIELDNET_PACKAGE_V1:" || package_hash (32 bytes) || signed_at_ms (8 bytes BE)`
    fn construct_signable_payload(package_hash: &ContentId, signed_at: Timestamp) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        out.extend_from_slice(b"FIELDNET_PACKAGE_V1:");
        out.extend_from_slice(package_hash.as_bytes());
        out.extend_from_slice(&signed_at.as_millis().to_be_bytes());
        out
    }
}

#[derive(Serialize, Deserialize)]
struct StoredPackageSignature {
    publisher: String,
    package_hash: String,
    signature: String,
    signed_at: u64,
    algorithm: SignatureAlgorithm,
}

impl Serialize for PackageSignature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let stored = StoredPackageSignature {
            publisher: self.publisher.to_string(),
            package_hash: self.package_hash.to_hex(),
            signature: hex::encode(self.signature_bytes),
            signed_at: self.signed_at.as_millis(),
            algorithm: self.algorithm,
        };
        stored.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PackageSignature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let stored = StoredPackageSignature::deserialize(deserializer)?;
        let publisher = stored.publisher.parse().map_err(serde::de::Error::custom)?;
        let package_hash =
            ContentId::from_hex(&stored.package_hash).map_err(serde::de::Error::custom)?;

        let sig_bytes_vec = hex::decode(&stored.signature).map_err(serde::de::Error::custom)?;
        if sig_bytes_vec.len() != 64 {
            return Err(serde::de::Error::custom("Invalid signature length"));
        }

        let mut signature_bytes = [0u8; 64];
        signature_bytes.copy_from_slice(&sig_bytes_vec);

        Ok(Self {
            publisher,
            package_hash,
            signature_bytes,
            signed_at: Timestamp::from_millis(stored.signed_at),
            algorithm: stored.algorithm,
        })
    }
}
