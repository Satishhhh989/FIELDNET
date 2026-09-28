use serde::{Deserialize, Serialize};
use std::fmt;

use crate::identity::ContentId;
use crate::manifest::model::ChunkAvailabilityBitmap;
use fieldnet_core::{NodeId, Timestamp};

/// Cryptographic trust classification for a remote provider node (Phase 03 integration).
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[repr(u8)]
pub enum TrustTier {
    /// Peer identity is completely unknown or unverified.
    #[default]
    Unknown = 0,
    /// Peer announced existence via discovery, but no mutual cryptographic handshake completed.
    Discovered = 1,
    /// Peer successfully completed mutual Ed25519 authentication and key agreement.
    Authenticated = 2,
    /// Peer possesses verified capability tokens or authorization credentials.
    Authorized = 3,
}

impl fmt::Display for TrustTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => write!(f, "UNKNOWN"),
            Self::Discovered => write!(f, "DISCOVERED"),
            Self::Authenticated => write!(f, "AUTHENTICATED"),
            Self::Authorized => write!(f, "AUTHORIZED"),
        }
    }
}

/// Operational state of a registered content provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ProviderState {
    /// Actively responding to queries and heartbeats with fresh timestamps.
    Active,
    /// Missed query response or transient network timeout; pending re-probe.
    Degraded,
    /// Peer disconnected, rebooted, or missing past expiration window.
    Stale,
    /// Hard TTL expired; excluded from resolution and candidate selection.
    Expired,
    /// Explicitly banned or slashed due to corrupted bytes or falsified claims.
    Banned,
}

impl ProviderState {
    /// Returns true if the provider can be considered for candidate chunk selection.
    pub fn is_usable(&self) -> bool {
        matches!(self, Self::Active | Self::Degraded)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Degraded => "DEGRADED",
            Self::Stale => "STALE",
            Self::Expired => "EXPIRED",
            Self::Banned => "BANNED",
        }
    }
}

impl fmt::Display for ProviderState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Comprehensive record of a remote content provider in the local `ProviderRegistry`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderRecord {
    /// Canonical content identifier.
    pub content_id: ContentId,
    /// Sovereign identity of the remote provider node.
    pub provider_node_id: NodeId,
    /// True if the provider claims to possess all constituent chunks of the object.
    pub has_full_content: bool,
    /// Bitset describing the specific subset of available chunks (None if full or unknown).
    pub available_chunks: Option<ChunkAvailabilityBitmap>,
    /// Total number of chunks in the content object, if known.
    pub total_chunks: Option<u32>,
    /// Total content size in bytes, if advertised.
    pub content_size: Option<u64>,
    /// Physical timestamp when the advertisement was registered or received.
    pub advertised_at: Timestamp,
    /// Hard expiration timestamp after which this provider record expires.
    pub expires_at: Timestamp,
    /// Last observed physical contact or query confirmation.
    pub last_seen_at: Timestamp,
    /// Timestamp of last successfully verified byte transfer from this provider.
    pub last_verified: Option<Timestamp>,
    /// Cryptographic trust tier of the provider node.
    pub trust_tier: TrustTier,
    /// Moving average of observed round-trip latency in milliseconds.
    pub observed_rtt_ms: u32,
    /// Historical transfer success ratio (0..1000 representing 0.0% to 100.0%).
    pub reliability_permille: u16,
    /// Current operational availability state.
    pub availability_state: ProviderState,
    /// Maximum rate capacity advertised by peer (bytes/sec, default 0 for unconstrained/unknown).
    pub max_bandwidth_bps: u64,
    /// Monotonic advertisement sequence number from provider.
    pub sequence: u64,
}

impl ProviderRecord {
    /// Constructs a basic active provider record with the given TTL in seconds.
    pub fn new(
        content_id: ContentId,
        provider_node_id: NodeId,
        has_full_content: bool,
        available_chunks: Option<ChunkAvailabilityBitmap>,
        ttl_seconds: u32,
    ) -> Self {
        let now = Timestamp::now();
        let expires_at = now.add_seconds(ttl_seconds as u64);

        Self {
            content_id,
            provider_node_id,
            has_full_content,
            available_chunks,
            total_chunks: None,
            content_size: None,
            advertised_at: now,
            expires_at,
            last_seen_at: now,
            last_verified: None,
            trust_tier: TrustTier::Unknown,
            observed_rtt_ms: 50,
            reliability_permille: 1000,
            availability_state: ProviderState::Active,
            max_bandwidth_bps: 0,
            sequence: 0,
        }
    }

    /// Checks if this provider record is past its expiration deadline.
    pub fn is_expired(&self, now: Timestamp) -> bool {
        self.availability_state == ProviderState::Expired || now >= self.expires_at
    }

    /// Checks whether this provider is currently usable for chunk resolution.
    pub fn is_usable(&self, now: Timestamp) -> bool {
        !self.is_expired(now) && self.availability_state.is_usable()
    }

    /// Checks if this provider possesses or advertises possession of a given chunk index.
    pub fn has_chunk(&self, chunk_index: u32) -> bool {
        if self.has_full_content {
            if let Some(total) = self.total_chunks {
                return chunk_index < total;
            }
            return true;
        }

        if let Some(bitmap) = &self.available_chunks {
            return bitmap.is_available(chunk_index);
        }

        false
    }

    /// Refreshes the last-seen timestamp and extends the expiration window.
    pub fn touch(&mut self, now: Timestamp, ttl_seconds: u32) {
        self.last_seen_at = now;
        self.expires_at = now.add_seconds(ttl_seconds as u64);
        if self.availability_state == ProviderState::Degraded
            || self.availability_state == ProviderState::Stale
        {
            self.availability_state = ProviderState::Active;
        }
    }

    /// Records a successful chunk or query response, smoothing RTT and reinforcing reliability.
    pub fn record_success(&mut self, rtt_ms: u32, now: Timestamp) {
        self.last_seen_at = now;
        self.last_verified = Some(now);
        // Exponential moving average for RTT (alpha = 0.2)
        self.observed_rtt_ms = (self.observed_rtt_ms * 4 + rtt_ms) / 5;
        // Bump reliability towards 1000
        self.reliability_permille = (self.reliability_permille + 10).min(1000);
        if self.availability_state != ProviderState::Banned {
            self.availability_state = ProviderState::Active;
        }
    }

    /// Records a failed exchange or timeout, slashing reliability and degrading status.
    pub fn record_failure(&mut self, penalty_permille: u16) {
        self.reliability_permille = self.reliability_permille.saturating_sub(penalty_permille);
        if self.reliability_permille < 200 {
            self.availability_state = ProviderState::Banned;
        } else if self.reliability_permille < 600 {
            self.availability_state = ProviderState::Degraded;
        }
    }

    /// Transitions the provider's operational state.
    pub fn transition_state(&mut self, new_state: ProviderState) {
        self.availability_state = new_state;
    }
}
