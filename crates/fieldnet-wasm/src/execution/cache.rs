use fieldnet_content::identity::ContentId;
use std::collections::HashMap;
use std::sync::RwLock;
use tracing::{debug, info};
use wasmtime::Module;

/// Cache key uniquely identifying a compiled WebAssembly module by its content hashes.
///
/// Under no circumstances is a module cached solely by application name or version string.
/// Both the overall package content hash and the specific WASM file content hash must match.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ModuleCacheKey {
    pub package_hash: ContentId,
    pub wasm_content_hash: ContentId,
}

impl ModuleCacheKey {
    pub fn new(package_hash: ContentId, wasm_content_hash: ContentId) -> Self {
        Self {
            package_hash,
            wasm_content_hash,
        }
    }
}

/// Runtime in-memory module cache providing fast, content-addressed reuse of compiled modules.
pub struct ModuleCache {
    modules: RwLock<HashMap<ModuleCacheKey, Module>>,
}

impl Default for ModuleCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ModuleCache {
    pub fn new() -> Self {
        Self {
            modules: RwLock::new(HashMap::new()),
        }
    }

    /// Looks up a compiled module by package hash and WASM content hash.
    pub fn get(&self, package_hash: &ContentId, wasm_content_hash: &ContentId) -> Option<Module> {
        let key = ModuleCacheKey::new(*package_hash, *wasm_content_hash);
        let guard = self.modules.read().ok()?;
        let module = guard.get(&key).cloned();
        if module.is_some() {
            debug!(
                package_hash = %package_hash,
                wasm_hash = %wasm_content_hash,
                "Module cache hit"
            );
        }
        module
    }

    /// Stores a compiled module keyed by package hash and WASM content hash.
    pub fn insert(&self, package_hash: ContentId, wasm_content_hash: ContentId, module: Module) {
        let key = ModuleCacheKey::new(package_hash, wasm_content_hash);
        if let Ok(mut guard) = self.modules.write() {
            guard.insert(key, module);
            debug!(
                package_hash = %package_hash,
                wasm_hash = %wasm_content_hash,
                "Module compiled and cached"
            );
        }
    }

    /// Deterministically invalidates and evicts a specific module from the cache.
    pub fn invalidate(&self, package_hash: &ContentId, wasm_content_hash: &ContentId) -> bool {
        let key = ModuleCacheKey::new(*package_hash, *wasm_content_hash);
        if let Ok(mut guard) = self.modules.write() {
            let removed = guard.remove(&key).is_some();
            if removed {
                info!(
                    package_hash = %package_hash,
                    wasm_hash = %wasm_content_hash,
                    "Module evicted from cache"
                );
            }
            removed
        } else {
            false
        }
    }

    /// Clears all compiled modules from the cache.
    pub fn clear(&self) {
        if let Ok(mut guard) = self.modules.write() {
            guard.clear();
            info!("Module cache cleared");
        }
    }

    /// Returns the number of cached compiled modules.
    pub fn len(&self) -> usize {
        self.modules.read().map(|g| g.len()).unwrap_or(0)
    }

    /// Returns true if the cache contains no modules.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
