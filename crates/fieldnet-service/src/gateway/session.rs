use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use tokio::sync::RwLock;

use fieldnet_core::Timestamp;
use rand::Rng;

use crate::model::ServiceCapability;
use crate::resolver::AuthContext;

use super::error::GatewayError;

/// Status of a local browser pairing ceremony.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PairingStatus {
    /// Challenge issued and awaiting user verification.
    Pending,
    /// Successfully confirmed and session token issued (single-use; challenge consumed).
    Confirmed,
    /// Challenge expired before user verification.
    Expired,
    /// Challenge invalidated due to excessive incorrect attempts or explicit cancellation.
    Rejected,
}

impl fmt::Display for PairingStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Confirmed => write!(f, "confirmed"),
            Self::Expired => write!(f, "expired"),
            Self::Rejected => write!(f, "rejected"),
        }
    }
}

/// A short-lived, single-use pairing challenge requiring human verification.
#[derive(Clone, Debug)]
pub struct PairingChallenge {
    pub challenge_id: String,
    pub pin: String,
    pub client_info: String,
    pub created_at: Timestamp,
    pub expires_at: Timestamp,
    pub attempts: usize,
    pub max_attempts: usize,
    pub status: PairingStatus,
}

impl PairingChallenge {
    /// Returns true if the challenge has expired relative to current time.
    pub fn is_expired(&self) -> bool {
        Timestamp::now() >= self.expires_at
    }

    /// Remaining validity duration in seconds.
    pub fn remaining_seconds(&self) -> u64 {
        let now = Timestamp::now().as_millis();
        let exp = self.expires_at.as_millis();
        if exp > now {
            (exp - now) / 1000
        } else {
            0
        }
    }
}

/// Authenticated browser session with bounded lifetime and capability scope.
///
/// NOTE: The browser session is strictly separate from a FIELDNET Node identity (`NodeId`).
/// A browser is an external HTTP client, NOT a cryptographic mesh routing node.
#[derive(Clone, Debug)]
pub struct BrowserSession {
    /// Unique session identifier.
    pub session_id: String,
    /// BLAKE3 hash of the secret bearer token (raw token is never stored in plaintext).
    pub token_hash: [u8; 32],
    /// Identifying information presented by client (e.g., User-Agent, client label).
    pub client_info: String,
    /// Granted capabilities authorizing access to gateway services and content.
    pub capabilities: HashSet<ServiceCapability>,
    /// Optional assigned roles for role-based service manifests.
    pub roles: Vec<String>,
    /// Session creation timestamp.
    pub created_at: Timestamp,
    /// Expiration timestamp after which the session is rejected.
    pub expires_at: Timestamp,
    /// Whether this session has been explicitly revoked.
    pub revoked: bool,
}

impl BrowserSession {
    /// Returns true if the session is currently active, unrevoked, and unexpired.
    pub fn is_valid(&self) -> bool {
        !self.revoked && Timestamp::now() < self.expires_at
    }

    /// Checks if this session holds the requested capability string.
    pub fn has_capability(&self, cap_str: &str) -> bool {
        if !self.is_valid() {
            return false;
        }
        self.capabilities.iter().any(|c| c.as_str() == cap_str)
    }

    /// Converts this browser session into a standard service `AuthContext`.
    /// Note: `caller_node_id` is explicitly `None` to prevent identity confusion.
    pub fn into_auth_context(&self) -> AuthContext {
        let mut ctx = AuthContext::anonymous();
        for r in &self.roles {
            ctx = ctx.with_role(r.clone());
        }
        for cap in &self.capabilities {
            ctx = ctx.with_token(cap.as_str());
        }
        ctx
    }

    /// Remaining session lifetime in seconds.
    pub fn remaining_seconds(&self) -> u64 {
        let now = Timestamp::now().as_millis();
        let exp = self.expires_at.as_millis();
        if exp > now {
            (exp - now) / 1000
        } else {
            0
        }
    }
}

/// Configuration settings for browser pairing and session lifetimes.
#[derive(Clone, Debug)]
pub struct SessionConfig {
    /// Time-to-live for pairing challenges in seconds (default: 60s).
    pub pairing_challenge_ttl_seconds: u64,
    /// Time-to-live for active browser sessions in seconds (default: 3600s = 1 hour).
    pub session_ttl_seconds: u64,
    /// Maximum number of active browser sessions retained in memory (default: 1024).
    pub max_active_sessions: usize,
    /// Maximum pending pairing challenges allowed concurrently (default: 64).
    pub max_pending_challenges: usize,
    /// Maximum incorrect verification attempts allowed before burning challenge (default: 5).
    pub max_failed_attempts: usize,
    /// Default capabilities granted to successfully paired browser sessions.
    pub default_capabilities: Vec<ServiceCapability>,
}

impl Default for SessionConfig {
    fn default() -> Self {
        let default_caps = vec![
            ServiceCapability::new("service.read").unwrap(),
            ServiceCapability::new("service.invoke").unwrap(),
            ServiceCapability::new("content.read").unwrap(),
        ];
        Self {
            pairing_challenge_ttl_seconds: 60,
            session_ttl_seconds: 3600,
            max_active_sessions: 1024,
            max_pending_challenges: 64,
            max_failed_attempts: 5,
            default_capabilities: default_caps,
        }
    }
}

/// Thread-safe in-memory store for browser pairing challenges and authenticated sessions.
#[derive(Clone)]
pub struct BrowserSessionStore {
    config: SessionConfig,
    challenges: Arc<RwLock<HashMap<String, PairingChallenge>>>,
    // Keyed by BLAKE3 hash of bearer token
    sessions_by_token_hash: Arc<RwLock<HashMap<[u8; 32], BrowserSession>>>,
    // Keyed by session_id for lookup and revocation
    sessions_by_id: Arc<RwLock<HashMap<String, [u8; 32]>>>,
    // Rate limit tracking: IP/client_key -> (window_start_ms, count)
    rate_limits: Arc<RwLock<HashMap<String, (u64, usize)>>>,
}

impl Default for BrowserSessionStore {
    fn default() -> Self {
        Self::new(SessionConfig::default())
    }
}

impl BrowserSessionStore {
    /// Creates a new `BrowserSessionStore` with the given configuration.
    pub fn new(config: SessionConfig) -> Self {
        Self {
            config,
            challenges: Arc::new(RwLock::new(HashMap::new())),
            sessions_by_token_hash: Arc::new(RwLock::new(HashMap::new())),
            sessions_by_id: Arc::new(RwLock::new(HashMap::new())),
            rate_limits: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Hashes a raw bearer token with BLAKE3 to prevent plaintext token storage in memory.
    pub fn hash_token(raw_token: &str) -> [u8; 32] {
        *blake3::hash(raw_token.as_bytes()).as_bytes()
    }

    /// Checks rate limit for pairing attempts. Returns true if request is within limits.
    pub async fn check_rate_limit(&self, client_key: &str, max_per_minute: usize) -> bool {
        let now_ms = Timestamp::now().as_millis();
        let mut limits = self.rate_limits.write().await;

        if let Some((start_ms, count)) = limits.get_mut(client_key) {
            if now_ms.saturating_sub(*start_ms) < 60_000 {
                if *count >= max_per_minute {
                    return false;
                }
                *count += 1;
                return true;
            } else {
                *start_ms = now_ms;
                *count = 1;
                return true;
            }
        }

        limits.insert(client_key.to_string(), (now_ms, 1));
        true
    }

    /// Initiates a new pairing challenge for an unknown browser.
    /// Returns the created challenge and the cleartext PIN (for the node host ceremony).
    pub async fn start_pairing(
        &self,
        client_info: String,
    ) -> Result<(PairingChallenge, String), GatewayError> {
        let mut challenges = self.challenges.write().await;

        // Clean expired challenges if near capacity
        if challenges.len() >= self.config.max_pending_challenges {
            let now = Timestamp::now();
            challenges.retain(|_, c| c.status == PairingStatus::Pending && c.expires_at > now);
            if challenges.len() >= self.config.max_pending_challenges {
                return Err(GatewayError::RequestLimitExceeded {
                    max: self.config.max_pending_challenges,
                });
            }
        }

        // Generate high-entropy 32-hex challenge ID
        let challenge_bytes: [u8; 16] = rand::random();
        let challenge_id = hex::encode(challenge_bytes);

        // Generate 6-digit numeric PIN (100000 - 999999) using CSPRNG
        let pin_val: u32 = rand::thread_rng().gen_range(100_000..=999_999);
        let pin = pin_val.to_string();

        let now = Timestamp::now();
        let expires_at = Timestamp::from_millis(
            now.as_millis()
                .saturating_add(self.config.pairing_challenge_ttl_seconds * 1000),
        );

        let challenge = PairingChallenge {
            challenge_id: challenge_id.clone(),
            pin: pin.clone(),
            client_info,
            created_at: now,
            expires_at,
            attempts: 0,
            max_attempts: self.config.max_failed_attempts,
            status: PairingStatus::Pending,
        };

        challenges.insert(challenge_id, challenge.clone());
        Ok((challenge, pin))
    }

    /// Inspects the status of an ongoing pairing challenge.
    pub async fn get_challenge_status(
        &self,
        challenge_id: &str,
    ) -> Option<(PairingStatus, u64, usize)> {
        let mut challenges = self.challenges.write().await;
        if let Some(c) = challenges.get_mut(challenge_id) {
            if c.status == PairingStatus::Pending && c.is_expired() {
                c.status = PairingStatus::Expired;
            }
            Some((c.status, c.remaining_seconds(), c.attempts))
        } else {
            None
        }
    }

    /// Retrieves pending challenge details including PIN for node console/admin inspection.
    pub async fn get_pending_challenge_pin(&self, challenge_id: &str) -> Option<String> {
        let challenges = self.challenges.read().await;
        challenges.get(challenge_id).and_then(|c| {
            if c.status == PairingStatus::Pending && !c.is_expired() {
                Some(c.pin.clone())
            } else {
                None
            }
        })
    }

    /// Verifies the pairing PIN and establishes an authenticated `BrowserSession`.
    /// Returns the session and the raw bearer token (issued once).
    ///
    /// Single-use: The challenge is marked `Confirmed` or removed immediately upon success.
    pub async fn confirm_pairing(
        &self,
        challenge_id: &str,
        submitted_pin: &str,
    ) -> Result<(BrowserSession, String), GatewayError> {
        let mut challenges = self.challenges.write().await;
        let challenge =
            challenges
                .get_mut(challenge_id)
                .ok_or_else(|| GatewayError::PairingRejected {
                    reason: "Challenge not found or invalid".to_string(),
                })?;

        // 1. Check expiration
        if challenge.is_expired() {
            challenge.status = PairingStatus::Expired;
            return Err(GatewayError::PairingRejected {
                reason: "Challenge expired".to_string(),
            });
        }

        // 2. Check status
        if challenge.status != PairingStatus::Pending {
            return Err(GatewayError::PairingRejected {
                reason: format!("Challenge is not pending (status: {})", challenge.status),
            });
        }

        // 3. Check attempt limit
        if challenge.attempts >= challenge.max_attempts {
            challenge.status = PairingStatus::Rejected;
            return Err(GatewayError::PairingRejected {
                reason: "Maximum verification attempts exceeded for this challenge".to_string(),
            });
        }

        // 4. Verify PIN
        if challenge.pin != submitted_pin.trim() {
            challenge.attempts += 1;
            if challenge.attempts >= challenge.max_attempts {
                challenge.status = PairingStatus::Rejected;
            }
            return Err(GatewayError::PairingRejected {
                reason: format!(
                    "Incorrect PIN (attempt {} of {})",
                    challenge.attempts, challenge.max_attempts
                ),
            });
        }

        // 5. Success: consume challenge (single-use)
        challenge.status = PairingStatus::Confirmed;
        let client_info = challenge.client_info.clone();
        drop(challenges);

        // 6. Generate high-entropy bearer token (256-bit CSPRNG)
        let token_bytes: [u8; 32] = rand::random();
        let raw_token = hex::encode(token_bytes);
        let token_hash = Self::hash_token(&raw_token);

        // Generate session ID
        let session_id_bytes: [u8; 16] = rand::random();
        let session_id = hex::encode(session_id_bytes);

        let now = Timestamp::now();
        let expires_at = Timestamp::from_millis(
            now.as_millis()
                .saturating_add(self.config.session_ttl_seconds * 1000),
        );

        let session = BrowserSession {
            session_id: session_id.clone(),
            token_hash,
            client_info,
            capabilities: self.config.default_capabilities.iter().cloned().collect(),
            roles: vec!["browser".to_string()],
            created_at: now,
            expires_at,
            revoked: false,
        };

        // Insert into storage
        {
            let mut sessions = self.sessions_by_token_hash.write().await;
            let mut by_id = self.sessions_by_id.write().await;

            // Enforce capacity bounds
            if sessions.len() >= self.config.max_active_sessions {
                let now_ts = Timestamp::now();
                sessions.retain(|_, s| !s.revoked && s.expires_at > now_ts);
                by_id.retain(|_, h| sessions.contains_key(h));
            }

            sessions.insert(token_hash, session.clone());
            by_id.insert(session_id, token_hash);
        }

        Ok((session, raw_token))
    }

    /// Validates an incoming bearer token.
    /// Returns the associated `BrowserSession` if valid and unexpired.
    pub async fn validate_token(&self, raw_token: &str) -> Result<BrowserSession, GatewayError> {
        let token_hash = Self::hash_token(raw_token);
        let sessions = self.sessions_by_token_hash.read().await;

        if let Some(session) = sessions.get(&token_hash) {
            if session.revoked {
                return Err(GatewayError::SessionUnauthorized {
                    reason: "Session has been revoked".to_string(),
                });
            }
            if Timestamp::now() >= session.expires_at {
                return Err(GatewayError::SessionUnauthorized {
                    reason: "Session has expired".to_string(),
                });
            }
            Ok(session.clone())
        } else {
            Err(GatewayError::SessionUnauthorized {
                reason: "Invalid or unrecognized bearer token".to_string(),
            })
        }
    }

    /// Refreshes the lifetime of an active session.
    pub async fn refresh_session(&self, raw_token: &str) -> Result<BrowserSession, GatewayError> {
        let token_hash = Self::hash_token(raw_token);
        let mut sessions = self.sessions_by_token_hash.write().await;

        if let Some(session) = sessions.get_mut(&token_hash) {
            if session.revoked {
                return Err(GatewayError::SessionUnauthorized {
                    reason: "Session has been revoked".to_string(),
                });
            }
            if Timestamp::now() >= session.expires_at {
                return Err(GatewayError::SessionUnauthorized {
                    reason: "Session has expired".to_string(),
                });
            }

            // Extend lifetime
            session.expires_at = Timestamp::from_millis(
                Timestamp::now()
                    .as_millis()
                    .saturating_add(self.config.session_ttl_seconds * 1000),
            );
            Ok(session.clone())
        } else {
            Err(GatewayError::SessionUnauthorized {
                reason: "Invalid or unrecognized bearer token".to_string(),
            })
        }
    }

    /// Explicitly revokes a session by bearer token.
    pub async fn revoke_session(&self, raw_token: &str) -> Result<String, GatewayError> {
        let token_hash = Self::hash_token(raw_token);
        let mut sessions = self.sessions_by_token_hash.write().await;

        if let Some(session) = sessions.get_mut(&token_hash) {
            session.revoked = true;
            let sid = session.session_id.clone();
            Ok(sid)
        } else {
            Err(GatewayError::SessionUnauthorized {
                reason: "Session not found".to_string(),
            })
        }
    }

    /// Explicitly revokes a session by session ID.
    pub async fn revoke_session_by_id(&self, session_id: &str) -> Result<(), GatewayError> {
        let by_id = self.sessions_by_id.read().await;
        if let Some(token_hash) = by_id.get(session_id) {
            let mut sessions = self.sessions_by_token_hash.write().await;
            if let Some(session) = sessions.get_mut(token_hash) {
                session.revoked = true;
                return Ok(());
            }
        }
        Err(GatewayError::SessionUnauthorized {
            reason: format!("Session ID {} not found", session_id),
        })
    }

    /// Updates the capabilities granted to an existing session (e.g. for testing capability limits).
    pub async fn set_session_capabilities(
        &self,
        raw_token: &str,
        capabilities: HashSet<ServiceCapability>,
    ) -> Result<(), GatewayError> {
        let token_hash = Self::hash_token(raw_token);
        let mut sessions = self.sessions_by_token_hash.write().await;
        if let Some(session) = sessions.get_mut(&token_hash) {
            session.capabilities = capabilities;
            Ok(())
        } else {
            Err(GatewayError::SessionUnauthorized {
                reason: "Session not found".to_string(),
            })
        }
    }

    /// Returns count of active unrevoked sessions.
    pub async fn active_session_count(&self) -> usize {
        let now = Timestamp::now();
        let sessions = self.sessions_by_token_hash.read().await;
        sessions
            .values()
            .filter(|s| !s.revoked && s.expires_at > now)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pairing_lifecycle_and_verification() {
        let store = BrowserSessionStore::default();

        // 1. Start pairing
        let (challenge, pin) = store
            .start_pairing("Browser Test".to_string())
            .await
            .unwrap();
        assert_eq!(challenge.status, PairingStatus::Pending);
        assert_eq!(pin.len(), 6);

        // 2. Query status
        let (status, remaining, attempts) = store
            .get_challenge_status(&challenge.challenge_id)
            .await
            .unwrap();
        assert_eq!(status, PairingStatus::Pending);
        assert!(remaining > 0);
        assert_eq!(attempts, 0);

        // 3. Confirm pairing with correct PIN
        let (session, token) = store
            .confirm_pairing(&challenge.challenge_id, &pin)
            .await
            .unwrap();
        assert_eq!(session.client_info, "Browser Test");
        assert!(session.is_valid());
        assert!(session.has_capability("service.read"));
        assert!(session.has_capability("service.invoke"));
        assert!(session.has_capability("content.read"));
        assert!(!token.is_empty());

        // 4. Validate token
        let validated = store.validate_token(&token).await.unwrap();
        assert_eq!(validated.session_id, session.session_id);

        // 5. Replay attempt on confirmed challenge must fail (single-use)
        let replay_res = store.confirm_pairing(&challenge.challenge_id, &pin).await;
        assert!(replay_res.is_err());
    }

    #[tokio::test]
    async fn test_incorrect_pin_and_max_attempts() {
        let config = SessionConfig {
            max_failed_attempts: 3,
            ..Default::default()
        };
        let store = BrowserSessionStore::new(config);

        let (challenge, pin) = store
            .start_pairing("Client Fail".to_string())
            .await
            .unwrap();

        // Attempt 1: wrong PIN
        let err1 = store
            .confirm_pairing(&challenge.challenge_id, "000000")
            .await
            .unwrap_err();
        assert!(matches!(err1, GatewayError::PairingRejected { .. }));

        // Attempt 2: wrong PIN
        let err2 = store
            .confirm_pairing(&challenge.challenge_id, "111111")
            .await
            .unwrap_err();
        assert!(matches!(err2, GatewayError::PairingRejected { .. }));

        // Attempt 3: wrong PIN (hits max attempts -> burned)
        let err3 = store
            .confirm_pairing(&challenge.challenge_id, "222222")
            .await
            .unwrap_err();
        assert!(matches!(err3, GatewayError::PairingRejected { .. }));

        // Attempt 4: even with correct PIN, challenge is now rejected/burned
        let err4 = store
            .confirm_pairing(&challenge.challenge_id, &pin)
            .await
            .unwrap_err();
        assert!(matches!(err4, GatewayError::PairingRejected { .. }));
    }

    #[tokio::test]
    async fn test_session_revocation() {
        let store = BrowserSessionStore::default();
        let (challenge, pin) = store.start_pairing("Client".to_string()).await.unwrap();
        let (_session, token) = store
            .confirm_pairing(&challenge.challenge_id, &pin)
            .await
            .unwrap();

        assert!(store.validate_token(&token).await.is_ok());

        // Revoke session
        store.revoke_session(&token).await.unwrap();

        // Validating revoked token returns error
        let err = store.validate_token(&token).await.unwrap_err();
        assert!(matches!(err, GatewayError::SessionUnauthorized { .. }));
    }

    #[tokio::test]
    async fn test_auth_context_separation_from_node_id() {
        let store = BrowserSessionStore::default();
        let (challenge, pin) = store.start_pairing("Client".to_string()).await.unwrap();
        let (session, _token) = store
            .confirm_pairing(&challenge.challenge_id, &pin)
            .await
            .unwrap();

        let auth_ctx = session.into_auth_context();
        // Critical invariant: Browser has NO NodeId
        assert_eq!(auth_ctx.caller_node_id, None);
        assert!(auth_ctx.roles.contains(&"browser".to_string()));
        assert!(auth_ctx
            .capability_tokens
            .contains(&"service.read".to_string()));
    }
}
