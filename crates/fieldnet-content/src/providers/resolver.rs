use std::sync::Arc;
use tracing::debug;

use super::record::ProviderRecord;
use super::registry::ProviderRegistry;
use crate::error::ContentError;
use crate::identity::ContentId;
use crate::manifest::model::ContentManifest;
use crate::metadata::ContentMetadata;
use crate::store::ContentStore;

/// Result of resolving a `ContentId` against local storage and the distributed provider registry.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContentResolution {
    /// Content is complete and immediately available in local `ContentStore`.
    Local {
        content_id: ContentId,
        metadata: ContentMetadata,
        manifest: Option<ContentManifest>,
    },
    /// Content is not locally complete, but candidate remote providers exist.
    Remote {
        content_id: ContentId,
        providers: Vec<ProviderRecord>,
    },
    /// Content is neither available locally nor known to any active registered providers.
    NotFound(ContentId),
}

/// High-level content resolver coordinating local storage cache checks and provider registry discovery.
pub struct ContentResolver {
    content_store: Arc<ContentStore>,
    provider_registry: Arc<ProviderRegistry>,
}

impl ContentResolver {
    /// Constructs a new `ContentResolver`.
    pub fn new(content_store: Arc<ContentStore>, provider_registry: Arc<ProviderRegistry>) -> Self {
        Self {
            content_store,
            provider_registry,
        }
    }

    /// Resolves availability for a target `ContentId`.
    ///
    /// 1. Checks local `ContentStore` (Cache Hit).
    /// 2. If absent or partial, queries `ProviderRegistry` for candidate peers.
    /// 3. Returns `ContentResolution` detailing local metadata or candidate providers.
    pub fn resolve(&self, content_id: &ContentId) -> Result<ContentResolution, ContentError> {
        debug!(content_id = %content_id, "Resolving content availability");

        // 1. Check local ContentStore
        if self.content_store.has(content_id)? {
            if let Ok(meta) = self.content_store.stat(content_id) {
                let manifest = self.content_store.get_manifest(content_id).ok();
                return Ok(ContentResolution::Local {
                    content_id: *content_id,
                    metadata: meta,
                    manifest,
                });
            }
        }

        // 2. Query ProviderRegistry
        let providers = self.provider_registry.find_providers(content_id);
        if !providers.is_empty() {
            return Ok(ContentResolution::Remote {
                content_id: *content_id,
                providers,
            });
        }

        Ok(ContentResolution::NotFound(*content_id))
    }

    /// Finds candidate providers advertising possession of specific missing chunk indices.
    pub fn resolve_chunks(
        &self,
        content_id: &ContentId,
        missing_chunks: &[u32],
    ) -> Vec<ProviderRecord> {
        self.provider_registry
            .find_providers_for_chunks(content_id, missing_chunks)
    }

    /// Provides access to the underlying `ProviderRegistry`.
    pub fn registry(&self) -> &Arc<ProviderRegistry> {
        &self.provider_registry
    }

    /// Provides access to the underlying `ContentStore`.
    pub fn store(&self) -> &Arc<ContentStore> {
        &self.content_store
    }
}
