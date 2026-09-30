use fieldnet_content::identity::ContentId;
use fieldnet_content::store::ContentStore;
use fieldnet_service::model::{ServiceId, ServiceState};
use fieldnet_service::resolver::ServiceResolver;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::error::ApplicationError;
use crate::id::{AppVersion, AppVersionReq, ApplicationId};
use crate::manifest::ApplicationManifest;
use crate::registry::ApplicationRegistry;

/// Category of dependency declared by an application.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyType {
    /// Dependency on another FIELDNET application package.
    Application,
    /// Dependency on a mesh service (e.g. "files.service").
    Service,
    /// Dependency on a specific content-addressed asset or library.
    Content,
}

/// Declaration of a dependency required by an application.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct AppDependency {
    /// Identifier of the dependency (e.g. "fieldnet.chat", "files.service", or content CID).
    pub identity: String,

    /// Version requirement specification (e.g. ">=1.0.0", "^0.2.0", "~2.0", "*").
    pub version_requirement: AppVersionReq,

    /// Whether this dependency is optional for application startup.
    #[serde(default)]
    pub optional: bool,

    /// Type of dependency.
    pub dependency_type: DependencyType,
}

impl AppDependency {
    pub fn new(
        identity: impl Into<String>,
        version_req: AppVersionReq,
        dependency_type: DependencyType,
        optional: bool,
    ) -> Result<Self, ApplicationError> {
        let id_str = identity.into().trim().to_string();
        if id_str.is_empty() {
            return Err(ApplicationError::DependencyError(
                "Dependency identity cannot be empty".to_string(),
            ));
        }

        Ok(Self {
            identity: id_str,
            version_requirement: version_req,
            optional,
            dependency_type,
        })
    }

    /// Validates the dependency declaration.
    pub fn validate(&self) -> Result<(), ApplicationError> {
        if self.identity.trim().is_empty() {
            return Err(ApplicationError::DependencyError(
                "Dependency identity cannot be empty".to_string(),
            ));
        }
        Ok(())
    }

    /// Evaluates whether an installed/available version satisfies this dependency requirement.
    pub fn is_satisfied_by(&self, version: &AppVersion) -> bool {
        self.version_requirement.matches(version)
    }
}

/// Detailed resolution result for a single application dependency.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ResolvedDependency {
    pub dependency: AppDependency,
    /// Whether this dependency is satisfied (required must be available; optional is satisfied even if missing).
    pub satisfied: bool,
    /// Whether the dependency is currently available in the node or mesh environment.
    pub available: bool,
    /// Concrete version resolved (if available).
    pub resolved_version: Option<String>,
    /// Provider metadata (e.g. provider node ID or package ID).
    pub provider_info: Option<String>,
    /// Non-fatal diagnostic or error description.
    pub error: Option<String>,
}

/// Comprehensive dependency resolution report for an application.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct DependencyResolutionReport {
    pub application_id: ApplicationId,
    pub all_satisfied: bool,
    pub dependencies: Vec<ResolvedDependency>,
    pub missing_required: Vec<String>,
    pub missing_optional: Vec<String>,
}

/// Directed dependency graph for detecting cycles and conflicting versions with safety limits.
#[derive(Clone, Debug, Default)]
pub struct DependencyGraph {
    /// Mapping of node identifier -> direct dependencies
    edges: HashMap<String, Vec<AppDependency>>,
}

impl DependencyGraph {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_NODES: usize = 256;

    pub fn new() -> Self {
        Self {
            edges: HashMap::new(),
        }
    }

    /// Adds a node with its direct dependencies into the graph.
    pub fn add_node(
        &mut self,
        id: String,
        deps: Vec<AppDependency>,
    ) -> Result<(), ApplicationError> {
        if self.edges.len() >= Self::MAX_NODES && !self.edges.contains_key(&id) {
            return Err(ApplicationError::DependencyError(format!(
                "Dependency graph exceeded maximum allowed node count ({})",
                Self::MAX_NODES
            )));
        }
        self.edges.insert(id, deps);
        Ok(())
    }

    /// Detects cycles starting from a root node using iterative/depth-bounded DFS.
    pub fn detect_cycles(&self, root_id: &str) -> Result<(), ApplicationError> {
        let mut visited = HashSet::new();
        let mut path = Vec::new();
        let mut path_set = HashSet::new();

        self.dfs_cycle(root_id, &mut visited, &mut path, &mut path_set, 0)
    }

    fn dfs_cycle(
        &self,
        node: &str,
        visited: &mut HashSet<String>,
        path: &mut Vec<String>,
        path_set: &mut HashSet<String>,
        depth: usize,
    ) -> Result<(), ApplicationError> {
        if depth > Self::MAX_DEPTH {
            return Err(ApplicationError::DependencyError(format!(
                "Dependency traversal exceeded maximum depth ({}) at node '{}'",
                Self::MAX_DEPTH,
                node
            )));
        }

        if path_set.contains(node) {
            let start_idx = path.iter().position(|n| n == node).unwrap_or(0);
            let mut cycle = path[start_idx..].to_vec();
            cycle.push(node.to_string());
            return Err(ApplicationError::DependencyError(format!(
                "Dependency cycle detected: {}",
                cycle.join(" -> ")
            )));
        }

        if visited.contains(node) {
            return Ok(());
        }

        path.push(node.to_string());
        path_set.insert(node.to_string());

        if let Some(deps) = self.edges.get(node) {
            for dep in deps {
                if dep.dependency_type == DependencyType::Application {
                    self.dfs_cycle(&dep.identity, visited, path, path_set, depth + 1)?;
                }
            }
        }

        path_set.remove(node);
        path.pop();
        visited.insert(node.to_string());
        Ok(())
    }

    /// Detects conflicting exact or incompatible version requirements across the graph.
    pub fn detect_conflicts(&self) -> Result<(), ApplicationError> {
        let mut requirements: HashMap<String, Vec<&AppDependency>> = HashMap::new();
        for deps in self.edges.values() {
            for dep in deps {
                requirements
                    .entry(dep.identity.clone())
                    .or_default()
                    .push(dep);
            }
        }

        for (id, reqs) in requirements {
            for i in 0..reqs.len() {
                for j in (i + 1)..reqs.len() {
                    let r1 = reqs[i];
                    let r2 = reqs[j];
                    if let (AppVersionReq::Exact(v1), AppVersionReq::Exact(v2)) =
                        (&r1.version_requirement, &r2.version_requirement)
                    {
                        if v1 != v2 {
                            return Err(ApplicationError::DependencyError(format!(
                                "Conflicting exact version requirements for dependency '{}': '='{} vs '='{}",
                                id, v1, v2
                            )));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// First-class dependency resolver verifying application, service, and content dependencies.
pub struct DependencyResolver {
    registry: Arc<ApplicationRegistry>,
    service_resolver: Arc<std::sync::RwLock<Option<Arc<ServiceResolver>>>>,
    content_store: Option<Arc<ContentStore>>,
}

impl DependencyResolver {
    pub fn new(
        registry: Arc<ApplicationRegistry>,
        service_resolver: Option<Arc<ServiceResolver>>,
        content_store: Option<Arc<ContentStore>>,
    ) -> Self {
        Self {
            registry,
            service_resolver: Arc::new(std::sync::RwLock::new(service_resolver)),
            content_store,
        }
    }

    pub fn with_service_resolver(self, resolver: Arc<ServiceResolver>) -> Self {
        *self.service_resolver.write().unwrap() = Some(resolver);
        self
    }

    pub fn set_service_resolver(&self, resolver: Arc<ServiceResolver>) {
        *self.service_resolver.write().unwrap() = Some(resolver);
    }

    /// Resolves all dependencies for a registered application.
    pub async fn resolve_for_app(
        &self,
        app_id: &ApplicationId,
    ) -> Result<DependencyResolutionReport, ApplicationError> {
        let record = self.registry.get(app_id)?.ok_or_else(|| {
            ApplicationError::NotFound(format!("Application '{}' not found", app_id))
        })?;

        self.resolve_for_manifest(&record.manifest).await
    }

    /// Resolves all dependencies declared in an application manifest, constructing a dependency
    /// graph, checking for cycles and conflicts, and resolving service and content dependencies.
    pub async fn resolve_for_manifest(
        &self,
        manifest: &ApplicationManifest,
    ) -> Result<DependencyResolutionReport, ApplicationError> {
        let app_id_str = manifest.application_id.to_string();
        let mut graph = DependencyGraph::new();
        graph.add_node(app_id_str.clone(), manifest.dependencies.clone())?;

        let mut resolved_deps = Vec::new();
        let mut missing_required = Vec::new();
        let mut missing_optional = Vec::new();

        // Queue for transitive graph traversal
        let mut queue = VecDeque::new();
        for dep in &manifest.dependencies {
            queue.push_back((app_id_str.clone(), dep.clone()));
        }

        let mut visited_apps = HashSet::new();
        visited_apps.insert(app_id_str.clone());

        while let Some((parent_id, dep)) = queue.pop_front() {
            let is_root_dep = parent_id == app_id_str;

            match dep.dependency_type {
                DependencyType::Application => {
                    let dep_app_id = match ApplicationId::new(&dep.identity) {
                        Ok(id) => id,
                        Err(e) => {
                            let err_msg =
                                format!("Invalid ApplicationId '{}': {}", dep.identity, e);
                            if is_root_dep {
                                missing_required.push(dep.identity.clone());
                                resolved_deps.push(ResolvedDependency {
                                    dependency: dep.clone(),
                                    satisfied: dep.optional,
                                    available: false,
                                    resolved_version: None,
                                    provider_info: None,
                                    error: Some(err_msg),
                                });
                            }
                            continue;
                        }
                    };

                    match self.registry.get(&dep_app_id)? {
                        Some(target_record) => {
                            // Check isolation: isolated applications cannot satisfy dependencies
                            if self.registry.is_isolated(&dep_app_id)? {
                                let err_msg = format!(
                                    "Application dependency '{}' is in ISOLATED state and unavailable",
                                    dep.identity
                                );
                                if is_root_dep {
                                    if !dep.optional {
                                        missing_required.push(dep.identity.clone());
                                    } else {
                                        missing_optional.push(dep.identity.clone());
                                    }
                                    resolved_deps.push(ResolvedDependency {
                                        dependency: dep.clone(),
                                        satisfied: dep.optional,
                                        available: false,
                                        resolved_version: Some(
                                            target_record.installed_version.to_string(),
                                        ),
                                        provider_info: Some(target_record.package_id.to_string()),
                                        error: Some(err_msg),
                                    });
                                }
                            } else if dep.is_satisfied_by(&target_record.installed_version) {
                                if is_root_dep {
                                    resolved_deps.push(ResolvedDependency {
                                        dependency: dep.clone(),
                                        satisfied: true,
                                        available: true,
                                        resolved_version: Some(
                                            target_record.installed_version.to_string(),
                                        ),
                                        provider_info: Some(target_record.package_id.to_string()),
                                        error: None,
                                    });
                                }

                                // Add to graph and queue transitive dependencies if not yet visited
                                let target_id_str = dep_app_id.to_string();
                                if !visited_apps.contains(&target_id_str) {
                                    visited_apps.insert(target_id_str.clone());
                                    graph.add_node(
                                        target_id_str.clone(),
                                        target_record.manifest.dependencies.clone(),
                                    )?;
                                    for child_dep in &target_record.manifest.dependencies {
                                        queue.push_back((target_id_str.clone(), child_dep.clone()));
                                    }
                                }
                            } else {
                                let err_msg = format!(
                                    "Application '{}' installed version {} does not satisfy requirement '{}'",
                                    dep.identity,
                                    target_record.installed_version,
                                    dep.version_requirement
                                );
                                if is_root_dep {
                                    if !dep.optional {
                                        missing_required.push(dep.identity.clone());
                                    } else {
                                        missing_optional.push(dep.identity.clone());
                                    }
                                    resolved_deps.push(ResolvedDependency {
                                        dependency: dep.clone(),
                                        satisfied: dep.optional,
                                        available: false,
                                        resolved_version: Some(
                                            target_record.installed_version.to_string(),
                                        ),
                                        provider_info: Some(target_record.package_id.to_string()),
                                        error: Some(err_msg),
                                    });
                                }
                            }
                        }
                        None => {
                            let err_msg = format!(
                                "Application dependency '{}' is not installed",
                                dep.identity
                            );
                            if is_root_dep {
                                if !dep.optional {
                                    missing_required.push(dep.identity.clone());
                                } else {
                                    missing_optional.push(dep.identity.clone());
                                }
                                resolved_deps.push(ResolvedDependency {
                                    dependency: dep.clone(),
                                    satisfied: dep.optional,
                                    available: false,
                                    resolved_version: None,
                                    provider_info: None,
                                    error: Some(err_msg),
                                });
                            }
                        }
                    }
                }
                DependencyType::Service => {
                    if !is_root_dep {
                        continue;
                    }

                    let svc_id = match ServiceId::new(&dep.identity) {
                        Ok(id) => id,
                        Err(e) => {
                            let err_msg = format!("Invalid ServiceId '{}': {}", dep.identity, e);
                            if !dep.optional {
                                missing_required.push(dep.identity.clone());
                            } else {
                                missing_optional.push(dep.identity.clone());
                            }
                            resolved_deps.push(ResolvedDependency {
                                dependency: dep.clone(),
                                satisfied: dep.optional,
                                available: false,
                                resolved_version: None,
                                provider_info: None,
                                error: Some(err_msg),
                            });
                            continue;
                        }
                    };

                    let resolver_opt = self.service_resolver.read().unwrap().clone();
                    if let Some(resolver) = resolver_opt {
                        let records = resolver.registry().get_service_instances(&svc_id).await;
                        // Filter active, healthy instances
                        let mut matching_candidates = Vec::new();
                        for r in records {
                            if matches!(
                                r.health.state,
                                ServiceState::Available | ServiceState::Degraded
                            ) {
                                let ver = AppVersion::new(
                                    r.manifest.version.major as u64,
                                    r.manifest.version.minor as u64,
                                    r.manifest.version.patch as u64,
                                );
                                if dep.is_satisfied_by(&ver) {
                                    matching_candidates.push((ver, r));
                                }
                            }
                        }

                        // Deterministic selection policy: select candidate with highest semver
                        matching_candidates.sort_by(|a, b| b.0.cmp(&a.0));

                        if let Some((ver, best_record)) = matching_candidates.first() {
                            resolved_deps.push(ResolvedDependency {
                                dependency: dep.clone(),
                                satisfied: true,
                                available: true,
                                resolved_version: Some(ver.to_string()),
                                provider_info: Some(
                                    best_record.manifest.provider_node_id.to_string(),
                                ),
                                error: None,
                            });
                        } else {
                            let err_msg = format!(
                                "No active provider found for service '{}' satisfying requirement '{}'",
                                dep.identity, dep.version_requirement
                            );
                            if !dep.optional {
                                missing_required.push(dep.identity.clone());
                            } else {
                                missing_optional.push(dep.identity.clone());
                            }
                            resolved_deps.push(ResolvedDependency {
                                dependency: dep.clone(),
                                satisfied: dep.optional,
                                available: false,
                                resolved_version: None,
                                provider_info: None,
                                error: Some(err_msg),
                            });
                        }
                    } else {
                        let err_msg = "ServiceResolver subsystem unavailable".to_string();
                        if !dep.optional {
                            missing_required.push(dep.identity.clone());
                        } else {
                            missing_optional.push(dep.identity.clone());
                        }
                        resolved_deps.push(ResolvedDependency {
                            dependency: dep.clone(),
                            satisfied: dep.optional,
                            available: false,
                            resolved_version: None,
                            provider_info: None,
                            error: Some(err_msg),
                        });
                    }
                }
                DependencyType::Content => {
                    if !is_root_dep {
                        continue;
                    }

                    let cid_res = ContentId::from_hex(&dep.identity);
                    match cid_res {
                        Ok(cid) => {
                            let exists = match &self.content_store {
                                Some(store) => store.has(&cid).unwrap_or(false),
                                None => false,
                            };

                            if exists {
                                resolved_deps.push(ResolvedDependency {
                                    dependency: dep.clone(),
                                    satisfied: true,
                                    available: true,
                                    resolved_version: None,
                                    provider_info: Some(cid.to_hex()),
                                    error: None,
                                });
                            } else {
                                let err_msg = format!("Content asset '{}' not found in CAS", cid);
                                if !dep.optional {
                                    missing_required.push(dep.identity.clone());
                                } else {
                                    missing_optional.push(dep.identity.clone());
                                }
                                resolved_deps.push(ResolvedDependency {
                                    dependency: dep.clone(),
                                    satisfied: dep.optional,
                                    available: false,
                                    resolved_version: None,
                                    provider_info: None,
                                    error: Some(err_msg),
                                });
                            }
                        }
                        Err(e) => {
                            let err_msg =
                                format!("Invalid ContentId hex '{}': {}", dep.identity, e);
                            if !dep.optional {
                                missing_required.push(dep.identity.clone());
                            } else {
                                missing_optional.push(dep.identity.clone());
                            }
                            resolved_deps.push(ResolvedDependency {
                                dependency: dep.clone(),
                                satisfied: dep.optional,
                                available: false,
                                resolved_version: None,
                                provider_info: None,
                                error: Some(err_msg),
                            });
                        }
                    }
                }
            }
        }

        // Cycle and conflict detection across the collected graph
        graph.detect_cycles(&app_id_str)?;
        graph.detect_conflicts()?;

        let all_satisfied = missing_required.is_empty();

        Ok(DependencyResolutionReport {
            application_id: manifest.application_id.clone(),
            all_satisfied,
            dependencies: resolved_deps,
            missing_required,
            missing_optional,
        })
    }
}
