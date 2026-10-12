use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::id::{ApplicationId, PublisherIdentity};

/// Default operational mode for package sharing.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GlobalSharingMode {
    /// By default, locally installed packages are shared unless explicitly restricted.
    ShareAllowed,
    /// By default, packages are private unless explicitly permitted.
    ShareDenied,
}

/// Node policy governing outbound application package distribution and advertisement.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PackageSharingPolicy {
    /// Global default policy mode.
    pub global_mode: GlobalSharingMode,

    /// Explicitly shared application identifiers (takes precedence when global_mode is ShareDenied).
    pub shared_applications: HashSet<ApplicationId>,

    /// Explicitly private / unshared application identifiers (takes precedence when global_mode is ShareAllowed).
    pub unshared_applications: HashSet<ApplicationId>,
}

impl Default for PackageSharingPolicy {
    fn default() -> Self {
        Self {
            global_mode: GlobalSharingMode::ShareAllowed,
            shared_applications: HashSet::new(),
            unshared_applications: HashSet::new(),
        }
    }
}

impl PackageSharingPolicy {
    pub fn new(global_mode: GlobalSharingMode) -> Self {
        Self {
            global_mode,
            shared_applications: HashSet::new(),
            unshared_applications: HashSet::new(),
        }
    }

    /// Evaluates whether an application package is authorized for external serving and advertisement.
    pub fn is_sharing_allowed(&self, app_id: &ApplicationId) -> bool {
        if self.unshared_applications.contains(app_id) {
            return false;
        }
        if self.shared_applications.contains(app_id) {
            return true;
        }
        self.global_mode == GlobalSharingMode::ShareAllowed
    }

    /// Explicitly authorizes an application for sharing.
    pub fn share_application(&mut self, app_id: ApplicationId) {
        self.unshared_applications.remove(&app_id);
        self.shared_applications.insert(app_id);
    }

    /// Explicitly marks an application as unshared / private.
    pub fn unshare_application(&mut self, app_id: ApplicationId) {
        self.shared_applications.remove(&app_id);
        self.unshared_applications.insert(app_id);
    }

    /// Clears any explicit override for an application.
    pub fn reset_override(&mut self, app_id: &ApplicationId) {
        self.shared_applications.remove(app_id);
        self.unshared_applications.remove(app_id);
    }
}

/// Cryptographic publisher trust policy governing whether a verified package may be installed.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub enum PublisherTrustPolicy {
    /// Any package with a valid, mathematically sound cryptographic signature is permitted.
    #[default]
    TrustAllVerified,

    /// Only packages signed by publishers on the explicit allow-list are approved for installation.
    AllowList(HashSet<PublisherIdentity>),

    /// Any verified publisher is permitted except those on the explicit deny-list.
    DenyList(HashSet<PublisherIdentity>),
}

impl PublisherTrustPolicy {
    /// Evaluates whether a publisher identity is approved under this policy.
    pub fn is_trusted(&self, publisher: &PublisherIdentity) -> bool {
        match self {
            Self::TrustAllVerified => true,
            Self::AllowList(allowed) => allowed.contains(publisher),
            Self::DenyList(denied) => !denied.contains(publisher),
        }
    }

    /// Adds a publisher to the trusted allow-list.
    pub fn allow_publisher(&mut self, publisher: PublisherIdentity) {
        match self {
            Self::AllowList(allowed) => {
                allowed.insert(publisher);
            }
            _ => {
                let mut set = HashSet::new();
                set.insert(publisher);
                *self = Self::AllowList(set);
            }
        }
    }

    /// Adds a publisher to the deny-list.
    pub fn deny_publisher(&mut self, publisher: PublisherIdentity) {
        match self {
            Self::DenyList(denied) => {
                denied.insert(publisher);
            }
            _ => {
                let mut set = HashSet::new();
                set.insert(publisher);
                *self = Self::DenyList(set);
            }
        }
    }
}

/// Configurable resource limits and DoS protection boundaries for package transfers.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct TransferLimits {
    /// Maximum package size permitted for incoming and outgoing transfers (bytes).
    pub max_package_size: u64,

    /// Maximum number of concurrent active incoming and outgoing transfer sessions.
    pub max_concurrent_transfers: usize,

    /// Default chunk size when chunking packages for transmission (bytes).
    pub default_chunk_size: u32,

    /// Maximum permissible chunk size (bytes).
    pub max_chunk_size: u32,

    /// Expiration TTL for transfer requests in milliseconds.
    pub request_ttl_ms: u64,

    /// Maximum number of in-flight pending requests before shedding load.
    pub max_pending_requests: usize,
}

impl Default for TransferLimits {
    fn default() -> Self {
        Self {
            max_package_size: 50 * 1024 * 1024, // 50 MB
            max_concurrent_transfers: 5,
            default_chunk_size: 64 * 1024, // 64 KB
            max_chunk_size: 256 * 1024,    // 256 KB
            request_ttl_ms: 60_000,        // 1 minute
            max_pending_requests: 50,
        }
    }
}
