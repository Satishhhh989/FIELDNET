use fieldnet_content::identity::ContentId;
use fieldnet_content::store::{ContentStore, PutOptions};
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::ApplicationError;
use crate::package::ApplicationPackage;

/// Helper integrating Application Packages with the FIELDNET Content-Addressed Storage (`ContentStore`).
pub struct CasPackageStorage;

impl CasPackageStorage {
    /// Stores all items of an application package into the `ContentStore`, pinning them against eviction,
    /// and then stores the package descriptor itself, returning its root `ContentId`.
    pub async fn store_package(
        store: &Arc<ContentStore>,
        package: &ApplicationPackage,
        payloads: &HashMap<String, Vec<u8>>,
    ) -> Result<ContentId, ApplicationError> {
        // 1. Store each package item into CAS
        for item in &package.items {
            let bytes = payloads.get(&item.path).ok_or_else(|| {
                ApplicationError::IntegrityError(format!(
                    "Missing payload for item '{}' during CAS storage",
                    item.path
                ))
            })?;

            // Verify content hash matches declared ContentId
            item.verify_bytes(bytes)?;

            let opts = PutOptions {
                is_pinned: true,
                mime_type: item.mime_type.clone(),
                ..Default::default()
            };

            store.put(bytes.as_slice(), Some(opts)).await.map_err(|e| {
                ApplicationError::ContentError(format!(
                    "Failed to store package item '{}' into CAS: {}",
                    item.path, e
                ))
            })?;
        }

        // 2. Store the package descriptor JSON into CAS
        let package_json = serde_json::to_vec(package)?;
        let pkg_opts = PutOptions {
            is_pinned: true,
            mime_type: Some("application/vnd.fieldnet.package+json".to_string()),
            ..Default::default()
        };

        let res = store
            .put(&package_json, Some(pkg_opts))
            .await
            .map_err(|e| {
                ApplicationError::ContentError(format!(
                    "Failed to store package descriptor into CAS: {}",
                    e
                ))
            })?;

        Ok(res.content_id())
    }

    /// Loads and parses an `ApplicationPackage` from `ContentStore` by its package descriptor `ContentId`.
    pub async fn load_package(
        store: &Arc<ContentStore>,
        root_content_id: &ContentId,
    ) -> Result<ApplicationPackage, ApplicationError> {
        let bytes = store.get_verified(root_content_id).await.map_err(|e| {
            ApplicationError::ContentError(format!(
                "Failed to retrieve package descriptor {} from CAS: {}",
                root_content_id, e
            ))
        })?;

        let package: ApplicationPackage = serde_json::from_slice(&bytes)?;
        Ok(package)
    }

    /// Retrieves all item payloads for an `ApplicationPackage` from `ContentStore`.
    pub async fn load_package_payloads(
        store: &Arc<ContentStore>,
        package: &ApplicationPackage,
    ) -> Result<HashMap<String, Vec<u8>>, ApplicationError> {
        let mut payloads = HashMap::with_capacity(package.items.len());

        for item in &package.items {
            let bytes = store.get_verified(&item.content_id).await.map_err(|e| {
                ApplicationError::ContentError(format!(
                    "Failed to retrieve item '{}' ({}) from CAS: {}",
                    item.path, item.content_id, e
                ))
            })?;
            payloads.insert(item.path.clone(), bytes);
        }

        Ok(payloads)
    }
}
