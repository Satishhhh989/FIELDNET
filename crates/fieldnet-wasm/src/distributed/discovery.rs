use fieldnet_content::identity::ContentId;
use fieldnet_core::Timestamp;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

use super::catalog::{DiscoveredPackageRecord, PackageCatalog};
use super::messages::PackageAdvertisement;
use super::persistence::PackageStorageDb;
use crate::error::ApplicationError;
use crate::events::ApplicationEvent;
use crate::id::{AppVersionReq, ApplicationId};

/// Manages package advertisement ingest, query resolution, and catalog synchronization.
pub struct ApplicationPackageDiscovery {
    catalog: Arc<RwLock<PackageCatalog>>,
    db: Arc<PackageStorageDb>,
    event_tx: Option<broadcast::Sender<ApplicationEvent>>,
}

impl ApplicationPackageDiscovery {
    pub fn new(
        catalog: Arc<RwLock<PackageCatalog>>,
        db: Arc<PackageStorageDb>,
        event_tx: Option<broadcast::Sender<ApplicationEvent>>,
    ) -> Self {
        Self {
            catalog,
            db,
            event_tx,
        }
    }

    /// Ingests, validates, catalogs, and persists a remote package advertisement.
    /// Returns true if the package hash is newly discovered.
    pub async fn handle_advertisement(
        &self,
        ad: PackageAdvertisement,
        now: Timestamp,
    ) -> Result<bool, ApplicationError> {
        ad.validate()?;

        if ad.is_expired(now) {
            return Err(ApplicationError::ValidationError(format!(
                "Advertisement for package {} is already expired",
                ad.package_hash
            )));
        }

        let is_new = {
            let mut cat = self.catalog.write().await;
            cat.add_advertisement(ad.clone(), now)?
        };

        let record = {
            let cat = self.catalog.read().await;
            cat.get(&ad.package_hash).cloned()
        };

        if let Some(rec) = record {
            let _ = self.db.save_discovered_package(&rec);
        }

        if is_new {
            if let Some(ref tx) = self.event_tx {
                let _ = tx.send(ApplicationEvent::PackageDiscovered {
                    application_id: ad.application_id,
                    package_id: ad.package_id,
                    version: ad.version.to_string(),
                    package_hash: ad.package_hash,
                    provider_node: ad.provider_node,
                });
            }
        }

        Ok(is_new)
    }

    /// Handles withdrawal of an application package announcement.
    pub async fn handle_withdrawal(
        &self,
        _app_id: &ApplicationId,
        package_hash: &ContentId,
    ) -> Result<(), ApplicationError> {
        {
            let mut cat = self.catalog.write().await;
            cat.remove_advertisement(package_hash);
        }
        let _ = self.db.delete_discovered_package(package_hash);
        Ok(())
    }

    /// Queries discovered packages matching an application identity and version requirement.
    pub async fn query_compatible(
        &self,
        app_id: &ApplicationId,
        req: &AppVersionReq,
    ) -> Vec<DiscoveredPackageRecord> {
        let cat = self.catalog.read().await;
        cat.find_compatible(app_id, req)
    }

    /// Finds a specific package record by its content hash.
    pub async fn get_package(&self, package_hash: &ContentId) -> Option<DiscoveredPackageRecord> {
        let cat = self.catalog.read().await;
        cat.get(package_hash).cloned()
    }

    /// Lists all currently discovered / available packages.
    pub async fn list_discovered(&self) -> Vec<DiscoveredPackageRecord> {
        let cat = self.catalog.read().await;
        cat.list_discovered()
    }

    /// Sweeps and purges expired advertisements from memory and persistence.
    pub async fn sweep_expired(&self, now: Timestamp) -> Result<usize, ApplicationError> {
        let expired = {
            let mut cat = self.catalog.write().await;
            cat.expire_stale(now)
        };

        for hash in &expired {
            let _ = self.db.delete_discovered_package(hash);
        }

        Ok(expired.len())
    }
}
