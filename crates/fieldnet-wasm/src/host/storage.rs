use fieldnet_content::identity::ContentId;
use fieldnet_content::store::ContentStore;
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::host::errors::HostError;
use crate::host::permissions::CapabilityContext;
use crate::id::ApplicationId;
use crate::permissions::AppPermission;
use crate::registry::ApplicationRegistry;

/// Storage interface bridging WASM host calls to the FIELDNET Content-Addressed Storage (CAS)
/// and enforcing persistent storage quota accounting.
#[derive(Clone)]
pub struct HostStorage {
    application_id: ApplicationId,
    max_storage_bytes: u64,
    content_store: Arc<ContentStore>,
    registry: Arc<ApplicationRegistry>,
    tokio_handle: tokio::runtime::Handle,
}

impl HostStorage {
    pub fn new(
        application_id: ApplicationId,
        max_storage_bytes: u64,
        content_store: Arc<ContentStore>,
        registry: Arc<ApplicationRegistry>,
        tokio_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            application_id,
            max_storage_bytes,
            content_store,
            registry,
            tokio_handle,
        }
    }

    /// Stores raw data in the FIELDNET Content-Addressed Store (CAS) subject to quota enforcement.
    ///
    /// Requires capability: `storage.write`
    pub fn put(&self, caps: &CapabilityContext, data: &[u8]) -> Result<ContentId, HostError> {
        caps.check_permission(AppPermission::StorageWrite)?;

        if data.is_empty() {
            return Err(HostError::invalid_request("Cannot store empty content"));
        }

        let new_size = data.len() as u64;

        // 1. Storage Quota Accounting Check
        let current_usage = self
            .registry
            .get_storage_usage(&self.application_id)
            .map_err(|e| HostError::internal(format!("Failed to retrieve storage usage: {}", e)))?;

        if current_usage.saturating_add(new_size) > self.max_storage_bytes {
            return Err(HostError::quota_exceeded(format!(
                "Application storage quota exceeded: current {} bytes + new {} bytes > max {} bytes",
                current_usage, new_size, self.max_storage_bytes
            )));
        }

        // 2. Put into CAS
        let put_res = self
            .tokio_handle
            .block_on(async { self.content_store.put(data, None).await })
            .map_err(|e| HostError::internal(format!("CAS storage put failed: {}", e)))?;
        let cid = put_res.content_id();

        // 3. Record Application Reference in Persistent Catalog
        self.registry
            .record_storage_put(&self.application_id, &cid, new_size)
            .map_err(|e| {
                error!(
                    application_id = %self.application_id,
                    content_id = %cid,
                    error = %e,
                    "Failed to record application storage usage"
                );
                HostError::internal("Failed to commit storage accounting")
            })?;

        info!(
            application_id = %self.application_id,
            content_id = %cid,
            size_bytes = new_size,
            total_used_bytes = current_usage + new_size,
            "Application successfully stored content item in CAS"
        );

        Ok(cid)
    }

    /// Retrieves verified content from the CAS by ContentId.
    ///
    /// Requires capability: `storage.read`
    pub fn get(
        &self,
        caps: &CapabilityContext,
        content_id_str: &str,
    ) -> Result<Vec<u8>, HostError> {
        caps.check_permission(AppPermission::StorageRead)?;

        let cid = ContentId::from_hex(content_id_str.trim())
            .map_err(|e| HostError::invalid_id(format!("Invalid ContentId format: {}", e)))?;

        let bytes = self
            .tokio_handle
            .block_on(async { self.content_store.get_verified(&cid).await })
            .map_err(|e| {
                debug!(content_id = %cid, error = %e, "Content retrieval failed");
                HostError::content_unavailable(format!("Content item '{}' unavailable: {}", cid, e))
            })?;

        Ok(bytes)
    }

    /// Checks if a content item exists in the CAS.
    ///
    /// Requires capability: `storage.read`
    pub fn has(&self, caps: &CapabilityContext, content_id_str: &str) -> Result<bool, HostError> {
        caps.check_permission(AppPermission::StorageRead)?;

        let cid = ContentId::from_hex(content_id_str.trim())
            .map_err(|e| HostError::invalid_id(format!("Invalid ContentId format: {}", e)))?;

        self.content_store
            .has(&cid)
            .map_err(|e| HostError::internal(format!("CAS check failed: {}", e)))
    }

    /// Deletes an application's storage item reference, releasing quota.
    ///
    /// Requires capability: `storage.write`
    pub fn delete(
        &self,
        caps: &CapabilityContext,
        content_id_str: &str,
    ) -> Result<bool, HostError> {
        caps.check_permission(AppPermission::StorageWrite)?;

        let cid = ContentId::from_hex(content_id_str.trim())
            .map_err(|e| HostError::invalid_id(format!("Invalid ContentId format: {}", e)))?;

        let removed = self
            .registry
            .record_storage_delete(&self.application_id, &cid)
            .map_err(|e| HostError::internal(format!("Failed to release storage record: {}", e)))?;

        // Also release from underlying CAS if supported
        let _ = self
            .tokio_handle
            .block_on(async { self.content_store.delete(&cid).await });

        info!(
            application_id = %self.application_id,
            content_id = %cid,
            removed = removed,
            "Application released storage item"
        );

        Ok(removed)
    }
}
