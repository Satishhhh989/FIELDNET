use crate::error::NetworkError;
use crate::framing::{read_frame, write_frame};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Nonce};
use fieldnet_core::{NodeId, SessionId};
use fieldnet_identity::NodeIdentity;
use rand::rngs::OsRng;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use x25519_dalek::{EphemeralSecret, PublicKey as X25519PublicKey};

#[derive(Serialize, Deserialize, Debug)]
pub enum HandshakeMessage {
    Init {
        initiator_node_id: NodeId,
        initiator_ed25519_pubkey: [u8; 32],
        ephemeral_x25519_pubkey: [u8; 32],
        nonce: [u8; 32],
    },
    Response {
        responder_node_id: NodeId,
        responder_ed25519_pubkey: [u8; 32],
        ephemeral_x25519_pubkey: [u8; 32],
        nonce: [u8; 32],
        signature: Vec<u8>,
    },
    Complete {
        signature: Vec<u8>,
    },
}

/// An established, authenticated ChaCha20-Poly1305 AEAD secure session.
pub struct SecureSession {
    pub session_id: SessionId,
    pub peer_node_id: NodeId,
    pub peer_public_key: [u8; 32],
    tx_cipher: ChaCha20Poly1305,
    rx_cipher: ChaCha20Poly1305,
    tx_counter: u64,
    rx_counter: u64,
}

impl SecureSession {
    /// Encrypts plaintext bytes using ChaCha20-Poly1305 with an incrementing 12-byte nonce.
    /// Wire format: 12-byte nonce || ciphertext + Poly1305 tag.
    pub fn encrypt(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, NetworkError> {
        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[..8].copy_from_slice(&self.tx_counter.to_be_bytes());
        self.tx_counter += 1;

        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = self
            .tx_cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| NetworkError::Aead(e.to_string()))?;

        let mut output = Vec::with_capacity(12 + ciphertext.len());
        output.extend_from_slice(&nonce_bytes);
        output.extend_from_slice(&ciphertext);
        Ok(output)
    }

    /// Decrypts ciphertext bytes using ChaCha20-Poly1305 and enforces monotonic replay protection.
    pub fn decrypt(&mut self, data: &[u8]) -> Result<Vec<u8>, NetworkError> {
        if data.len() < 12 + 16 {
            return Err(NetworkError::Aead("Ciphertext too short".into()));
        }

        let nonce_bytes = &data[..12];
        let ciphertext = &data[12..];

        let mut counter_bytes = [0u8; 8];
        counter_bytes.copy_from_slice(&nonce_bytes[..8]);
        let incoming_counter = u64::from_be_bytes(counter_bytes);

        if incoming_counter < self.rx_counter {
            return Err(NetworkError::Aead(format!(
                "Replay attack detected: incoming counter {} < expected minimum {}",
                incoming_counter, self.rx_counter
            )));
        }
        self.rx_counter = incoming_counter + 1;

        let nonce = Nonce::from_slice(nonce_bytes);
        self.rx_cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| NetworkError::Aead(e.to_string()))
    }

    /// Splits the session into independent transmit and receive halves for concurrent I/O.
    pub fn into_split(self) -> (SessionTx, SessionRx) {
        (
            SessionTx {
                session_id: self.session_id,
                peer_node_id: self.peer_node_id,
                tx_cipher: self.tx_cipher,
                tx_counter: self.tx_counter,
            },
            SessionRx {
                session_id: self.session_id,
                peer_node_id: self.peer_node_id,
                rx_cipher: self.rx_cipher,
                rx_counter: self.rx_counter,
            },
        )
    }
}

/// Transmit half of a SecureSession.
pub struct SessionTx {
    pub session_id: SessionId,
    pub peer_node_id: NodeId,
    tx_cipher: ChaCha20Poly1305,
    tx_counter: u64,
}

impl SessionTx {
    pub fn encrypt(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, NetworkError> {
        let mut nonce_bytes = [0u8; 12];
        nonce_bytes[..8].copy_from_slice(&self.tx_counter.to_be_bytes());
        self.tx_counter += 1;

        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = self
            .tx_cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| NetworkError::Aead(e.to_string()))?;

        let mut output = Vec::with_capacity(12 + ciphertext.len());
        output.extend_from_slice(&nonce_bytes);
        output.extend_from_slice(&ciphertext);
        Ok(output)
    }
}

/// Receive half of a SecureSession.
pub struct SessionRx {
    pub session_id: SessionId,
    pub peer_node_id: NodeId,
    rx_cipher: ChaCha20Poly1305,
    rx_counter: u64,
}

impl SessionRx {
    pub fn decrypt(&mut self, data: &[u8]) -> Result<Vec<u8>, NetworkError> {
        if data.len() < 12 + 16 {
            return Err(NetworkError::Aead("Ciphertext too short".into()));
        }

        let nonce_bytes = &data[..12];
        let ciphertext = &data[12..];

        let mut counter_bytes = [0u8; 8];
        counter_bytes.copy_from_slice(&nonce_bytes[..8]);
        let incoming_counter = u64::from_be_bytes(counter_bytes);

        if incoming_counter < self.rx_counter {
            return Err(NetworkError::Aead(format!(
                "Replay attack detected: incoming counter {} < expected minimum {}",
                incoming_counter, self.rx_counter
            )));
        }
        self.rx_counter = incoming_counter + 1;

        let nonce = Nonce::from_slice(nonce_bytes);
        self.rx_cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| NetworkError::Aead(e.to_string()))
    }
}

/// Executes mutual Ed25519 authentication + X25519 key agreement as Initiator.
pub async fn handshake_as_initiator<S>(
    stream: &mut S,
    local_identity: &NodeIdentity,
) -> Result<SecureSession, NetworkError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut csprng = OsRng;
    let local_ephemeral_secret = EphemeralSecret::random_from_rng(&mut csprng);
    let local_ephemeral_pubkey = X25519PublicKey::from(&local_ephemeral_secret);

    let mut init_nonce = [0u8; 32];
    csprng.fill_bytes(&mut init_nonce);

    // 1. Send HandshakeMessage::Init
    let init_msg = HandshakeMessage::Init {
        initiator_node_id: local_identity.node_id(),
        initiator_ed25519_pubkey: local_identity.public_key_bytes(),
        ephemeral_x25519_pubkey: *local_ephemeral_pubkey.as_bytes(),
        nonce: init_nonce,
    };
    let init_bytes = serde_json::to_vec(&init_msg)?;
    write_frame(stream, &init_bytes).await?;

    // 2. Receive HandshakeMessage::Response
    let resp_frame = read_frame(stream).await?;
    let resp_msg: HandshakeMessage = serde_json::from_slice(&resp_frame)?;

    let (peer_node_id, peer_ed25519_pubkey, peer_x25519_pubkey, resp_nonce, resp_signature) =
        match resp_msg {
            HandshakeMessage::Response {
                responder_node_id,
                responder_ed25519_pubkey,
                ephemeral_x25519_pubkey,
                nonce,
                signature,
            } => (
                responder_node_id,
                responder_ed25519_pubkey,
                ephemeral_x25519_pubkey,
                nonce,
                signature,
            ),
            _ => {
                return Err(NetworkError::AuthenticationFailed(
                    "Expected HandshakeMessage::Response".into(),
                ))
            }
        };

    // Verify peer identity
    let derived_peer_id = NodeId::from_ed25519_public_key(&peer_ed25519_pubkey);
    if derived_peer_id != peer_node_id {
        return Err(NetworkError::AuthenticationFailed(
            "Responder public key does not match NodeId".into(),
        ));
    }

    // Verify responder's signature over transcript
    let mut transcript_r = Vec::new();
    transcript_r.extend_from_slice(local_ephemeral_pubkey.as_bytes());
    transcript_r.extend_from_slice(&peer_x25519_pubkey);
    transcript_r.extend_from_slice(&init_nonce);
    transcript_r.extend_from_slice(&resp_nonce);
    transcript_r.extend_from_slice(b"FIELDNET_RESPONDER");

    let resp_sig: [u8; 64] = resp_signature.try_into().map_err(|_| {
        NetworkError::AuthenticationFailed("Invalid responder signature length".into())
    })?;

    NodeIdentity::verify(&peer_ed25519_pubkey, &transcript_r, &resp_sig).map_err(|e| {
        NetworkError::AuthenticationFailed(format!("Invalid responder signature: {}", e))
    })?;

    // 3. Compute shared ECDH secret
    let peer_x25519 = X25519PublicKey::from(peer_x25519_pubkey);
    let shared_secret = local_ephemeral_secret.diffie_hellman(&peer_x25519);

    // 4. Send HandshakeMessage::Complete with Initiator's signature
    let mut transcript_i = Vec::new();
    transcript_i.extend_from_slice(&peer_x25519_pubkey);
    transcript_i.extend_from_slice(local_ephemeral_pubkey.as_bytes());
    transcript_i.extend_from_slice(&resp_nonce);
    transcript_i.extend_from_slice(&init_nonce);
    transcript_i.extend_from_slice(b"FIELDNET_INITIATOR");

    let init_signature = local_identity.sign(&transcript_i);
    let complete_msg = HandshakeMessage::Complete {
        signature: init_signature.to_vec(),
    };
    let complete_bytes = serde_json::to_vec(&complete_msg)?;
    write_frame(stream, &complete_bytes).await?;

    // 5. Derive symmetric keys via BLAKE3 KDF
    let (tx_key, rx_key, session_id) = derive_session_keys(
        shared_secret.as_bytes(),
        &init_nonce,
        &resp_nonce,
        true, // initiator
    );

    let tx_cipher = ChaCha20Poly1305::new_from_slice(&tx_key)
        .map_err(|e| NetworkError::Crypto(e.to_string()))?;
    let rx_cipher = ChaCha20Poly1305::new_from_slice(&rx_key)
        .map_err(|e| NetworkError::Crypto(e.to_string()))?;

    Ok(SecureSession {
        session_id,
        peer_node_id,
        peer_public_key: peer_ed25519_pubkey,
        tx_cipher,
        rx_cipher,
        tx_counter: 0,
        rx_counter: 0,
    })
}

/// Executes mutual Ed25519 authentication + X25519 key agreement as Responder.
pub async fn handshake_as_responder<S>(
    stream: &mut S,
    local_identity: &NodeIdentity,
) -> Result<SecureSession, NetworkError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut csprng = OsRng;
    let local_ephemeral_secret = EphemeralSecret::random_from_rng(&mut csprng);
    let local_ephemeral_pubkey = X25519PublicKey::from(&local_ephemeral_secret);

    let mut resp_nonce = [0u8; 32];
    csprng.fill_bytes(&mut resp_nonce);

    // 1. Receive HandshakeMessage::Init
    let init_frame = read_frame(stream).await?;
    let init_msg: HandshakeMessage = serde_json::from_slice(&init_frame)?;

    let (peer_node_id, peer_ed25519_pubkey, peer_x25519_pubkey, init_nonce) = match init_msg {
        HandshakeMessage::Init {
            initiator_node_id,
            initiator_ed25519_pubkey,
            ephemeral_x25519_pubkey,
            nonce,
        } => (
            initiator_node_id,
            initiator_ed25519_pubkey,
            ephemeral_x25519_pubkey,
            nonce,
        ),
        _ => {
            return Err(NetworkError::AuthenticationFailed(
                "Expected HandshakeMessage::Init".into(),
            ))
        }
    };

    // Verify peer identity
    let derived_peer_id = NodeId::from_ed25519_public_key(&peer_ed25519_pubkey);
    if derived_peer_id != peer_node_id {
        return Err(NetworkError::AuthenticationFailed(
            "Initiator public key does not match NodeId".into(),
        ));
    }

    // 2. Sign transcript and send HandshakeMessage::Response
    let mut transcript_r = Vec::new();
    transcript_r.extend_from_slice(&peer_x25519_pubkey);
    transcript_r.extend_from_slice(local_ephemeral_pubkey.as_bytes());
    transcript_r.extend_from_slice(&init_nonce);
    transcript_r.extend_from_slice(&resp_nonce);
    transcript_r.extend_from_slice(b"FIELDNET_RESPONDER");

    let resp_signature = local_identity.sign(&transcript_r);
    let resp_msg = HandshakeMessage::Response {
        responder_node_id: local_identity.node_id(),
        responder_ed25519_pubkey: local_identity.public_key_bytes(),
        ephemeral_x25519_pubkey: *local_ephemeral_pubkey.as_bytes(),
        nonce: resp_nonce,
        signature: resp_signature.to_vec(),
    };
    let resp_bytes = serde_json::to_vec(&resp_msg)?;
    write_frame(stream, &resp_bytes).await?;

    // 3. Receive HandshakeMessage::Complete
    let complete_frame = read_frame(stream).await?;
    let complete_msg: HandshakeMessage = serde_json::from_slice(&complete_frame)?;

    let init_signature = match complete_msg {
        HandshakeMessage::Complete { signature } => signature,
        _ => {
            return Err(NetworkError::AuthenticationFailed(
                "Expected HandshakeMessage::Complete".into(),
            ))
        }
    };

    // Verify initiator signature over transcript
    let mut transcript_i = Vec::new();
    transcript_i.extend_from_slice(local_ephemeral_pubkey.as_bytes());
    transcript_i.extend_from_slice(&peer_x25519_pubkey);
    transcript_i.extend_from_slice(&resp_nonce);
    transcript_i.extend_from_slice(&init_nonce);
    transcript_i.extend_from_slice(b"FIELDNET_INITIATOR");

    let init_sig: [u8; 64] = init_signature.try_into().map_err(|_| {
        NetworkError::AuthenticationFailed("Invalid initiator signature length".into())
    })?;

    NodeIdentity::verify(&peer_ed25519_pubkey, &transcript_i, &init_sig).map_err(|e| {
        NetworkError::AuthenticationFailed(format!("Invalid initiator signature: {}", e))
    })?;

    // 4. Compute shared ECDH secret
    let peer_x25519 = X25519PublicKey::from(peer_x25519_pubkey);
    let shared_secret = local_ephemeral_secret.diffie_hellman(&peer_x25519);

    // 5. Derive symmetric keys via BLAKE3 KDF
    let (tx_key, rx_key, session_id) = derive_session_keys(
        shared_secret.as_bytes(),
        &init_nonce,
        &resp_nonce,
        false, // responder
    );

    let tx_cipher = ChaCha20Poly1305::new_from_slice(&tx_key)
        .map_err(|e| NetworkError::Crypto(e.to_string()))?;
    let rx_cipher = ChaCha20Poly1305::new_from_slice(&rx_key)
        .map_err(|e| NetworkError::Crypto(e.to_string()))?;

    Ok(SecureSession {
        session_id,
        peer_node_id,
        peer_public_key: peer_ed25519_pubkey,
        tx_cipher,
        rx_cipher,
        tx_counter: 0,
        rx_counter: 0,
    })
}

fn derive_session_keys(
    ecdh_secret: &[u8; 32],
    init_nonce: &[u8; 32],
    resp_nonce: &[u8; 32],
    is_initiator: bool,
) -> ([u8; 32], [u8; 32], SessionId) {
    let mut hasher = blake3::Hasher::new_derive_key("FIELDNET_BUILD01_SESSION_KDF");
    hasher.update(ecdh_secret);
    hasher.update(init_nonce);
    hasher.update(resp_nonce);

    let mut output = [0u8; 80]; // 32 key1 + 32 key2 + 16 session_id
    let mut reader = hasher.finalize_xof();
    reader.fill(&mut output);

    let mut key_a = [0u8; 32];
    let mut key_b = [0u8; 32];
    let mut sess_id = [0u8; 16];

    key_a.copy_from_slice(&output[..32]);
    key_b.copy_from_slice(&output[32..64]);
    sess_id.copy_from_slice(&output[64..80]);

    if is_initiator {
        (key_a, key_b, SessionId(sess_id))
    } else {
        (key_b, key_a, SessionId(sess_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn test_secure_session_handshake_and_encryption() {
        let (mut client_io, mut server_io) = duplex(4096);
        let id_client = NodeIdentity::generate();
        let id_server = NodeIdentity::generate();

        let client_id_copy = id_client.node_id();
        let server_id_copy = id_server.node_id();

        let client_task =
            tokio::spawn(async move { handshake_as_initiator(&mut client_io, &id_client).await });

        let server_task =
            tokio::spawn(async move { handshake_as_responder(&mut server_io, &id_server).await });

        let (client_res, server_res) = tokio::join!(client_task, server_task);
        let mut client_session = client_res.unwrap().unwrap();
        let mut server_session = server_res.unwrap().unwrap();

        assert_eq!(client_session.peer_node_id, server_id_copy);
        assert_eq!(server_session.peer_node_id, client_id_copy);
        assert_eq!(client_session.session_id, server_session.session_id);

        // Test encryption from client to server
        let plaintext = b"Confidential FIELDNET payload";
        let ciphertext = client_session.encrypt(plaintext).unwrap();
        let decrypted = server_session.decrypt(&ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);

        // Test encryption from server to client
        let reply = b"Confidential ACK payload";
        let reply_cipher = server_session.encrypt(reply).unwrap();
        let decrypted_reply = client_session.decrypt(&reply_cipher).unwrap();
        assert_eq!(decrypted_reply, reply);
    }

    #[tokio::test]
    async fn test_handshake_authentication_failure_invalid_signature() {
        let (mut client_io, mut server_io) = duplex(4096);
        let id_client = NodeIdentity::generate();

        let client_task =
            tokio::spawn(async move { handshake_as_initiator(&mut client_io, &id_client).await });

        let attacker_task = tokio::spawn(async move {
            let init_frame = read_frame(&mut server_io).await.unwrap();
            let _init_msg: HandshakeMessage = serde_json::from_slice(&init_frame).unwrap();

            let fake_id = NodeIdentity::generate();
            let fake_resp = HandshakeMessage::Response {
                responder_node_id: fake_id.node_id(),
                responder_ed25519_pubkey: fake_id.public_key_bytes(),
                ephemeral_x25519_pubkey: [99u8; 32],
                nonce: [77u8; 32],
                signature: vec![0u8; 64], // invalid signature
            };
            let bytes = serde_json::to_vec(&fake_resp).unwrap();
            write_frame(&mut server_io, &bytes).await.unwrap();
        });

        let (client_res, _) = tokio::join!(client_task, attacker_task);
        let client_result = client_res.unwrap();
        assert!(
            client_result.is_err(),
            "Initiator must reject invalid responder signature"
        );
    }
}
