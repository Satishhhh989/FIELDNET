use fieldnet_content::identity::ContentId;
use fieldnet_core::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::messages::PackageAdvertisement;
use crate::error::ApplicationError;
use crate::id::{AppVersionReq, ApplicationId};

/// Lifecycle and discovery state of a package within the local package catalog.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageCatalogState {
    /// Package is installed and active on the local node.
    Local,
    /// Package advertisement was received from a remote peer; unverified.
    Discovered,
    /// Package metadata and advertisement structure have been validated.
    Available,
    /// Transfer has started and some chunks are available locally.
    Partial,
    /// All package chunks have been received.
    Complete,
    /// Integrity and publisher cryptographic signatures have been verified.
    Verified,
    /// Package is verified, trusted by policy, and all dependencies are resolved.
    Installable,
    /// Package has been installed into the local ApplicationRegistry.
    Installed,
}

impl PackageCatalogState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Discovered => "discovered",
            Self::Available => "available",
            Self::Partial => "partial",
            Self::Complete => "complete",
            Self::Verified => "verified",
            Self::Installable => "installable",
            Self::Installed => "installed",
        }
    }
}

/// Catalog entry tracking a discovered or local application package.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct DiscoveredPackageRecord {
    pub advertisement: PackageAdvertisement,
    pub state: PackageCatalogState,
    pub first_discovered_at: Timestamp,
    pub last_seen_at: Timestamp,
}

/// Local catalog tracking discovered, available, transferring, and installed packages.
#[derive(Clone, Debug, Default)]
pub struct PackageCatalog {
    packages: HashMap<ContentId, DiscoveredPackageRecord>,
}

impl PackageCatalog {
    pub fn new() -> Self {
        Self {
            packages: HashMap::new(),
        }
    }

    /// Records or refreshes a package advertisement in the catalog.
    /// Returns true if this is a newly discovered package hash.
    pub fn add_advertisement(
        &mut self,
        ad: PackageAdvertisement,
        now: Timestamp,
    ) -> Result<bool, ApplicationError> {
        ad.validate()?;

        let hash = ad.package_hash;
        if let Some(existing) = self.packages.get_mut(&hash) {
            existing.last_seen_at = now;
            // Retain advanced state (e.g. Partial, Complete, Installed) if already present
            if existing.state == PackageCatalogState::Discovered {
                existing.state = PackageCatalogState::Available;
            }
            Ok(false)
        } else {
            self.packages.insert(
                hash,
                DiscoveredPackageRecord {
                    advertisement: ad,
                    state: PackageCatalogState::Available,
                    first_discovered_at: now,
                    last_seen_at: now,
                },
            );
            Ok(true)
        }
    }

    /// Removes an advertisement or catalog entry by package hash.
    pub fn remove_advertisement(
        &mut self,
        package_hash: &ContentId,
    ) -> Option<DiscoveredPackageRecord> {
        self.packages.remove(package_hash)
    }

    /// Retrieves a catalog record by package hash.
    pub fn get(&self, package_hash: &ContentId) -> Option<&DiscoveredPackageRecord> {
        self.packages.get(package_hash)
    }

    /// Retrieves a mutable reference to a catalog record.
    pub fn get_mut(&mut self, package_hash: &ContentId) -> Option<&mut DiscoveredPackageRecord> {
        self.packages.get_mut(package_hash)
    }

    /// Updates the catalog state for a specific package hash.
    pub fn update_state(&mut self, package_hash: &ContentId, state: PackageCatalogState) -> bool {
        if let Some(entry) = self.packages.get_mut(package_hash) {
            entry.state = state;
            true
        } else {
            false
        }
    }

    /// Finds all discovered packages matching an ApplicationId and version requirement,
    /// ordered deterministically by highest semantic version first.
    pub fn find_compatible(
        &self,
        app_id: &ApplicationId,
        req: &AppVersionReq,
    ) -> Vec<DiscoveredPackageRecord> {
        let mut matching: Vec<DiscoveredPackageRecord> = self
            .packages
            .values()
            .filter(|rec| {
                &rec.advertisement.application_id == app_id
                    && req.matches(&rec.advertisement.version)
            })
            .cloned()
            .collect();

        // Sort descending by version (highest first)
        matching.sort_by(|a, b| b.advertisement.version.cmp(&a.advertisement.version));
        matching
    }

    /// Lists all packages currently in Discovered or Available state.
    pub fn list_discovered(&self) -> Vec<DiscoveredPackageRecord> {
        self.packages
            .values()
            .filter(|rec| {
                rec.state == PackageCatalogState::Discovered
                    || rec.state == PackageCatalogState::Available
            })
            .cloned()
            .collect()
    }

    /// Lists all records in the catalog.
    pub fn list_all(&self) -> Vec<DiscoveredPackageRecord> {
        self.packages.values().cloned().collect()
    }

    /// Expires and purges stale advertisements based on current timestamp.
    /// Installed and locally verified packages are never expired.
    pub fn expire_stale(&mut self, now: Timestamp) -> Vec<ContentId> {
        let mut expired = Vec::new();

        self.packages.retain(|hash, record| {
            if record.state == PackageCatalogState::Installed
                || record.state == PackageCatalogState::Local
            {
                return true;
            }

            if record.advertisement.is_expired(now) {
                expired.push(*hash);
                false
            } else {
                true
            }
        });

        expired
    }
}
