use crate::error::IdentityError;
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use fieldnet_core::NodeId;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs;
use std::path::Path;

#[derive(Serialize, Deserialize)]
struct StoredIdentity {
    node_id: String,
    public_key: String,
    secret_key_seed: String,
}

/// Sovereign Ed25519 node identity.
/// Securely wraps a private signing key and public verifying key.
/// Never exposes or logs the private secret in Debug/Display implementations.
pub struct NodeIdentity {
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
    node_id: NodeId,
}

impl NodeIdentity {
    /// Generates a new random Ed25519 sovereign identity using OsRng.
    pub fn generate() -> Self {
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let verifying_key = signing_key.verifying_key();
        let pubkey_bytes = verifying_key.to_bytes();
        let node_id = NodeId::from_ed25519_public_key(&pubkey_bytes);

        Self {
            signing_key,
            verifying_key,
            node_id,
        }
    }

    /// Reconstructs an identity from a 32-byte secret key seed.
    pub fn from_secret_seed(seed: &[u8; 32]) -> Self {
        let signing_key = SigningKey::from_bytes(seed);
        let verifying_key = signing_key.verifying_key();
        let pubkey_bytes = verifying_key.to_bytes();
        let node_id = NodeId::from_ed25519_public_key(&pubkey_bytes);

        Self {
            signing_key,
            verifying_key,
            node_id,
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn public_key_bytes(&self) -> [u8; 32] {
        self.verifying_key.to_bytes()
    }

    /// Signs a message slice with the sovereign Ed25519 private key.
    pub fn sign(&self, message: &[u8]) -> [u8; 64] {
        let sig: Signature = self.signing_key.sign(message);
        sig.to_bytes()
    }

    /// Verifies a signature against an Ed25519 public key.
    pub fn verify(
        public_key_bytes: &[u8; 32],
        message: &[u8],
        signature_bytes: &[u8; 64],
    ) -> Result<(), IdentityError> {
        let verifying_key = VerifyingKey::from_bytes(public_key_bytes)
            .map_err(|e| IdentityError::InvalidKey(e.to_string()))?;
        let signature = Signature::from_bytes(signature_bytes);
        verifying_key
            .verify(message, &signature)
            .map_err(|e| IdentityError::Signature(e.to_string()))
    }

    /// Saves the sovereign identity to disk as a JSON file.
    pub fn save_to_file(&self, path: &Path) -> Result<(), IdentityError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let stored = StoredIdentity {
            node_id: self.node_id.to_hex(),
            public_key: hex::encode(self.public_key_bytes()),
            secret_key_seed: hex::encode(self.signing_key.to_bytes()),
        };
        let json = serde_json::to_string_pretty(&stored)?;
        fs::write(path, json)?;
        Ok(())
    }

    /// Loads the sovereign identity from disk.
    pub fn load_from_file(path: &Path) -> Result<Self, IdentityError> {
        let content = fs::read_to_string(path)?;
        let stored: StoredIdentity = serde_json::from_str(&content)?;
        let seed_bytes = hex::decode(&stored.secret_key_seed)
            .map_err(|e| IdentityError::InvalidKey(e.to_string()))?;
        if seed_bytes.len() != 32 {
            return Err(IdentityError::InvalidKey(
                "Secret key seed must be 32 bytes".into(),
            ));
        }
        let mut seed = [0u8; 32];
        seed.copy_from_slice(&seed_bytes);
        let id = Self::from_secret_seed(&seed);
        if id.node_id().to_hex() != stored.node_id {
            return Err(IdentityError::InvalidKey(
                "Stored NodeId does not match derived public key".into(),
            ));
        }
        Ok(id)
    }

    /// Loads the identity if the file exists; otherwise generates and saves a new one.
    pub fn load_or_generate(path: &Path) -> Result<(Self, bool), IdentityError> {
        if path.exists() {
            let id = Self::load_from_file(path)?;
            tracing::info!(node_id = %id.node_id(), path = %path.display(), "Loaded existing Node Identity");
            Ok((id, false))
        } else {
            let id = Self::generate();
            id.save_to_file(path)?;
            tracing::info!(node_id = %id.node_id(), path = %path.display(), "Generated and persisted new Node Identity");
            Ok((id, true))
        }
    }
}

impl fmt::Debug for NodeIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NodeIdentity")
            .field("node_id", &self.node_id)
            .field("public_key", &hex::encode(self.public_key_bytes()))
            .finish()
    }
}

impl fmt::Display for NodeIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeIdentity({})", self.node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_and_verify() {
        let identity = NodeIdentity::generate();
        let message = b"Hello from FIELDNET!";
        let signature = identity.sign(message);
        assert!(NodeIdentity::verify(&identity.public_key_bytes(), message, &signature).is_ok());

        let bad_message = b"Tampered message";
        assert!(
            NodeIdentity::verify(&identity.public_key_bytes(), bad_message, &signature).is_err()
        );
    }

    #[test]
    fn test_identity_persistence_and_reload() {
        let temp_dir = std::env::temp_dir().join("fieldnet_test_identity");
        let _ = fs::remove_dir_all(&temp_dir);
        let key_path = temp_dir.join("identity.json");

        // First startup: generated
        let (id1, created) = NodeIdentity::load_or_generate(&key_path).unwrap();
        assert!(created);

        // Second startup: reloaded
        let (id2, created2) = NodeIdentity::load_or_generate(&key_path).unwrap();
        assert!(!created2);
        assert_eq!(id1.node_id(), id2.node_id());
        assert_eq!(id1.public_key_bytes(), id2.public_key_bytes());

        let _ = fs::remove_dir_all(&temp_dir);
    }
}
