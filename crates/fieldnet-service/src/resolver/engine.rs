use fieldnet_core::{NodeId, Timestamp};
use std::cmp::Reverse;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, info};

use super::auth::AuthContext;
use super::cache::ResolutionCache;
use super::metrics::ResolverMetrics;
use super::query::{LocalityLevel, LocalityPreference, ServiceQuery};
use super::resolution::{ServiceResolution, ServiceResolutionReason};
use super::route::RouteProvider;
use crate::error::ServiceError;
use crate::events::ServiceEvent;
use crate::model::{ServiceRecord, ServiceState};
use crate::registry::ServiceRegistry;

/// Configuration options governing resolution behavior and constraints.
#[derive(Clone, Debug)]
pub struct ResolverConfig {
    /// Whether resolution result caching is enabled (default false to prevent stale results).
    pub enable_cache: bool,
    /// Cache TTL in seconds if caching is enabled.
    pub cache_ttl_secs: u64,
    /// Maximum network hops acceptable for remote provider reachability (default 16).
    pub max_acceptable_hops: u8,
}

impl Default for ResolverConfig {
    fn default() -> Self {
        Self {
            enable_cache: false,
            cache_ttl_secs: 30,
            max_acceptable_hops: 16,
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct CandidateScore {
    // 1. Availability: Available (1) > Degraded (0)
    availability: u8,
    // 2. Locality rank based on query preference: higher is better
    locality_rank: u8,
    // 3. Version compatibility: higher SemVer is preferred
    version_major: u32,
    version_minor: u32,
    version_patch: u32,
    // 4. Capability match count (more capabilities advertised is preferred)
    capability_count: usize,
    // 5. Network hops: fewer hops is better (255 - hops)
    hop_rank: u8,
    // 6. Health: reliability permille (0..1000)
    reliability_permille: u32,
    // 7. Latency: lower latency is better
    latency_rank: u32,
    // 8. Consecutive failures: fewer failures is better
    failure_rank: u32,
    // 9. Stable deterministic tie-breaker: lexicographically smaller instance ID wins
    tie_breaker: Reverse<String>,
}

struct EvaluatedCandidate {
    record: ServiceRecord,
    is_local: bool,
    hops: u8,
    locality_level: LocalityLevel,
}

/// Central FIELDNET Service Resolver.
///
/// Resolves logical `ServiceQuery` specifications to concrete `ServiceResolution`
/// provider instances using hard constraint filtering, architecture-defined
/// precedence ranking, and deterministic tie-breaking.
pub struct ServiceResolver {
    registry: Arc<ServiceRegistry>,
    route_provider: Option<Arc<dyn RouteProvider>>,
    local_node_id: Option<NodeId>,
    event_tx: Option<broadcast::Sender<ServiceEvent>>,
    metrics: Arc<ResolverMetrics>,
    cache: Option<Arc<ResolutionCache>>,
    config: ResolverConfig,
}

impl ServiceResolver {
    /// Creates a new `ServiceResolver` instance.
    pub fn new(
        registry: Arc<ServiceRegistry>,
        route_provider: Option<Arc<dyn RouteProvider>>,
        local_node_id: Option<NodeId>,
        event_tx: Option<broadcast::Sender<ServiceEvent>>,
        config: ResolverConfig,
    ) -> Self {
        let cache = if config.enable_cache {
            Some(Arc::new(ResolutionCache::new(config.cache_ttl_secs * 1000)))
        } else {
            None
        };

        Self {
            registry,
            route_provider,
            local_node_id,
            event_tx,
            metrics: Arc::new(ResolverMetrics::new()),
            cache,
            config,
        }
    }

    /// Accessor for resolver observability metrics.
    pub fn metrics(&self) -> &Arc<ResolverMetrics> {
        &self.metrics
    }

    /// Accessor for the underlying `ServiceRegistry`.
    pub fn registry(&self) -> &Arc<ServiceRegistry> {
        &self.registry
    }

    /// Invalidates any cached resolution pointing to the specified instance.
    pub async fn invalidate_cache(&self, instance_id: &crate::model::ServiceInstanceId) {
        if let Some(ref cache) = self.cache {
            cache.invalidate_instance(instance_id).await;
        }
    }

    /// Resolves a `ServiceQuery` to a concrete provider instance.
    pub async fn resolve(
        &self,
        query: &ServiceQuery,
        auth: Option<&AuthContext>,
    ) -> Result<ServiceResolution, ServiceError> {
        self.resolve_with_exclusions(query, auth, &[]).await
    }

    /// Resolves a `ServiceQuery` to a concrete provider instance, excluding any instances in `excluded`.
    pub async fn resolve_with_exclusions(
        &self,
        query: &ServiceQuery,
        auth: Option<&AuthContext>,
        excluded: &[crate::model::ServiceInstanceId],
    ) -> Result<ServiceResolution, ServiceError> {
        self.metrics.inc_total();
        let now = Timestamp::now();

        // 1. Check cache if enabled (only when no instances are excluded)
        if excluded.is_empty() {
            if let Some(ref cache) = self.cache {
                if let Some(cached) = cache.get(query, now).await {
                    // Verify cached instance is still callable in the registry
                    if let Some(live) = self.registry.get_instance(cached.instance_id()).await {
                        if live.is_callable() && !live.is_expired(now) {
                            debug!(
                                "Resolver cache hit for service '{}' -> instance '{}'",
                                query.service_id,
                                cached.instance_id()
                            );
                            self.metrics.inc_success();
                            return Ok(cached);
                        }
                    }
                    // Stale cache entry, invalidate it
                    cache.invalidate_instance(cached.instance_id()).await;
                }
            }
        }

        // 2. Fetch all candidates registered for the requested ServiceId
        let all_candidates = self.registry.get_service_instances(&query.service_id).await;

        if all_candidates.is_empty() {
            self.metrics.inc_not_found();
            self.metrics.inc_failed();
            return Err(ServiceError::ServiceNotFound(query.service_id.to_string()));
        }

        // 3. Apply Hard Constraints Filtering
        let mut passed: Vec<EvaluatedCandidate> = Vec::new();
        let mut count_expired = 0;
        let mut count_unavailable = 0;
        let mut count_version_mismatch = 0;
        let mut count_missing_capability = 0;
        let mut count_unsupported_protocol = 0;
        let mut count_unauthenticated = 0;
        let mut count_unauthorized = 0;
        let mut count_unreachable = 0;
        let mut count_locality_rejected = 0;

        let default_auth = AuthContext::anonymous();
        let auth_ctx = auth.unwrap_or(&default_auth);

        for candidate in all_candidates {
            // Exclude already failed/attempted instances
            if excluded.contains(candidate.instance_id()) {
                continue;
            }

            // Hard constraint 1: Expiration check
            if candidate.is_expired(now) {
                count_expired += 1;
                continue;
            }

            // Hard constraint 2: Authoritative lifecycle state must be callable (Available or Degraded)
            if !candidate.state().is_callable() {
                count_unavailable += 1;
                continue;
            }

            // Hard constraint 3: ServiceId exact match
            if candidate.service_id() != &query.service_id {
                continue;
            }

            // Hard constraint 4: Version constraints
            if let Some(ref min_v) = query.min_version {
                if !candidate.manifest.version.is_compatible_with(min_v)
                    || candidate.manifest.version < *min_v
                {
                    count_version_mismatch += 1;
                    continue;
                }
            }
            if let Some(ref max_v) = query.max_version {
                if candidate.manifest.version > *max_v {
                    count_version_mismatch += 1;
                    continue;
                }
            }

            // Hard constraint 5: Required capabilities exact matching
            let mut has_all_caps = true;
            for req_cap in &query.required_capabilities {
                if !candidate.manifest.capabilities.contains(req_cap) {
                    has_all_caps = false;
                    break;
                }
            }
            if !has_all_caps {
                count_missing_capability += 1;
                continue;
            }

            // Hard constraint 6: Required protocol match
            if let Some(req_proto) = query.required_protocol {
                if candidate.manifest.protocol != req_proto {
                    count_unsupported_protocol += 1;
                    continue;
                }
            }

            // Hard constraint 7: Provider authentication (signature + public key)
            if query.trust_requirement.require_authenticated_provider
                && candidate.manifest.verify_signature().is_err()
            {
                count_unauthenticated += 1;
                continue;
            }

            // Hard constraint 8: Authorization check against permissions
            if query.trust_requirement.require_authorized_provider
                && auth_ctx
                    .check_authorization(query.service_id.as_str(), &candidate.manifest.permissions)
                    .is_err()
            {
                count_unauthorized += 1;
                continue;
            }

            // Hard constraint 9: Locality preference and route reachability
            let is_local = candidate.is_local
                || self
                    .local_node_id
                    .map(|local| local == *candidate.provider_node_id())
                    .unwrap_or(false);

            if query.locality_preference == LocalityPreference::LocalOnly && !is_local {
                count_locality_rejected += 1;
                continue;
            }

            let (hops, reachable) = if is_local {
                (0u8, true)
            } else if let Some(ref router) = self.route_provider {
                if let Some(route) = router.get_route(candidate.provider_node_id()).await {
                    if route.hops <= self.config.max_acceptable_hops {
                        (route.hops, true)
                    } else {
                        (route.hops, false)
                    }
                } else {
                    (255u8, false)
                }
            } else {
                // If no router is provided, assume remote candidates are direct (1 hop)
                (1u8, true)
            };

            if !reachable && !query.allow_dtn {
                count_unreachable += 1;
                continue;
            }

            let locality_level = LocalityLevel::from_hops(hops, is_local);

            passed.push(EvaluatedCandidate {
                record: candidate,
                is_local,
                hops,
                locality_level,
            });
        }

        // 4. Return explicit failure if no candidate passed hard constraints
        if passed.is_empty() {
            self.metrics.inc_failed();

            if count_unauthenticated > 0 {
                return Err(ServiceError::InvalidSignature(
                    "Provider manifest cryptographic signature failed verification".to_string(),
                ));
            }

            if count_unauthorized > 0 {
                self.metrics.inc_unauthorized();
                return Err(ServiceError::Unauthorized {
                    service_id: query.service_id.to_string(),
                    reason: "Caller context does not satisfy service permissions".to_string(),
                });
            }

            if count_missing_capability > 0 {
                self.metrics.inc_capability();
                return Err(ServiceError::MissingCapability {
                    service_id: query.service_id.to_string(),
                    missing: query
                        .required_capabilities
                        .iter()
                        .map(|c| c.to_string())
                        .collect(),
                });
            }

            if count_version_mismatch > 0 {
                self.metrics.inc_version();
                return Err(ServiceError::NoCompatibleVersion {
                    service_id: query.service_id.to_string(),
                    requested_min: query.min_version.map(|v| v.to_string()),
                    requested_max: query.max_version.map(|v| v.to_string()),
                });
            }

            if count_unsupported_protocol > 0 {
                self.metrics.inc_protocol();
                return Err(ServiceError::UnsupportedProtocol {
                    service_id: query.service_id.to_string(),
                    requested: format!("{:?}", query.required_protocol.unwrap()),
                });
            }

            if count_locality_rejected > 0 {
                return Err(ServiceError::ResolutionFailed(format!(
                    "No local provider found for service '{}' under LocalOnly preference",
                    query.service_id
                )));
            }

            if count_unreachable > 0 {
                self.metrics.inc_unreachable();
                return Err(ServiceError::NoReachableProvider {
                    service_id: query.service_id.to_string(),
                    candidate_count: count_unreachable,
                });
            }

            if count_unavailable > 0 || count_expired > 0 {
                self.metrics.inc_unavailable();
                return Err(ServiceError::AllProvidersUnavailable {
                    service_id: query.service_id.to_string(),
                    count: count_unavailable + count_expired,
                });
            }

            if !excluded.is_empty() {
                self.metrics.inc_unavailable();
                return Err(ServiceError::AllProvidersUnavailable {
                    service_id: query.service_id.to_string(),
                    count: excluded.len(),
                });
            }

            return Err(ServiceError::ResolutionFailed(format!(
                "No candidate satisfied hard constraints for service '{}'",
                query.service_id
            )));
        }

        // 5. Deterministic Precedence Ranking
        let preference = query.locality_preference;

        let compute_score = |cand: &EvaluatedCandidate| -> CandidateScore {
            // 1. Availability: Available = 1, Degraded = 0
            let availability = if cand.record.state() == ServiceState::Available {
                1
            } else {
                0
            };

            // 2. Locality rank
            let locality_rank = match preference {
                LocalityPreference::PreferLocal | LocalityPreference::LocalOnly => {
                    match cand.locality_level {
                        LocalityLevel::LocalNode => 4,
                        LocalityLevel::DirectPeer => 3,
                        LocalityLevel::NearbyPeer => 2,
                        LocalityLevel::Remote => 1,
                        LocalityLevel::Unknown => 0,
                    }
                }
                LocalityPreference::PreferRemote => match cand.locality_level {
                    LocalityLevel::Remote => 4,
                    LocalityLevel::NearbyPeer => 3,
                    LocalityLevel::DirectPeer => 2,
                    LocalityLevel::LocalNode => 1,
                    LocalityLevel::Unknown => 0,
                },
                LocalityPreference::Any => match cand.locality_level {
                    LocalityLevel::LocalNode => 4,
                    LocalityLevel::DirectPeer => 3,
                    LocalityLevel::NearbyPeer => 2,
                    LocalityLevel::Remote => 1,
                    LocalityLevel::Unknown => 0,
                },
            };

            // 3. Version
            let v = &cand.record.manifest.version;

            // 4. Capability count
            let cap_count = cand.record.manifest.capabilities.len();

            // 5. Hop rank
            let hop_rank = 255u8.saturating_sub(cand.hops);

            // 6. Reliability
            let rel = cand.record.health.reliability_permille;

            // 7. Latency
            let latency_rank = u32::MAX.saturating_sub(cand.record.health.moving_avg_latency_ms);

            // 8. Consecutive failures
            let failure_rank = u32::MAX.saturating_sub(cand.record.health.consecutive_failures);

            // 9. Stable tie breaker (smaller instance ID string wins)
            let tie_breaker = Reverse(cand.record.instance_id().to_string());

            CandidateScore {
                availability,
                locality_rank,
                version_major: v.major,
                version_minor: v.minor,
                version_patch: v.patch,
                capability_count: cap_count,
                hop_rank,
                reliability_permille: rel,
                latency_rank,
                failure_rank,
                tie_breaker,
            }
        };

        // Find candidate with maximum score
        let mut best_idx = 0;
        let mut best_score = compute_score(&passed[0]);
        let mut tiebreak_applied = false;

        for (idx, cand) in passed.iter().enumerate().skip(1) {
            let score = compute_score(cand);
            if score > best_score {
                // Check if all precedence criteria prior to tie_breaker were identical
                tiebreak_applied = score.availability == best_score.availability
                    && score.locality_rank == best_score.locality_rank
                    && score.version_major == best_score.version_major
                    && score.version_minor == best_score.version_minor
                    && score.version_patch == best_score.version_patch
                    && score.capability_count == best_score.capability_count
                    && score.hop_rank == best_score.hop_rank
                    && score.reliability_permille == best_score.reliability_permille
                    && score.latency_rank == best_score.latency_rank
                    && score.failure_rank == best_score.failure_rank;
                best_score = score;
                best_idx = idx;
            } else if score.availability == best_score.availability
                && score.locality_rank == best_score.locality_rank
                && score.version_major == best_score.version_major
                && score.version_minor == best_score.version_minor
                && score.version_patch == best_score.version_patch
                && score.capability_count == best_score.capability_count
                && score.hop_rank == best_score.hop_rank
                && score.reliability_permille == best_score.reliability_permille
                && score.latency_rank == best_score.latency_rank
                && score.failure_rank == best_score.failure_rank
            {
                // Equivalent candidates; tie-breaker decided the winner
                tiebreak_applied = true;
            }
        }

        let selected = passed.remove(best_idx);

        // 6. Build structured explanation
        let health_status = format!(
            "{} (reliability: {}/1000, latency: {}ms, failures: {})",
            selected.record.state(),
            selected.record.health.reliability_permille,
            selected.record.health.moving_avg_latency_ms,
            selected.record.health.consecutive_failures
        );

        let explanation_text = format!(
            "Selected instance '{}' (provider '{}'): version {} compatible, protocol {:?}, {} capability(ies) satisfied, route {} hop(s), locality {:?}, health [{}]{}",
            selected.record.instance_id(),
            selected.record.provider_node_id(),
            selected.record.manifest.version,
            selected.record.manifest.protocol,
            selected.record.manifest.capabilities.len(),
            selected.hops,
            selected.locality_level,
            health_status,
            if tiebreak_applied {
                ", deterministic tie-breaker applied"
            } else {
                ""
            }
        );

        let reason = ServiceResolutionReason {
            matched_service: true,
            compatible_version: selected.record.manifest.version,
            capabilities_satisfied: query.required_capabilities.clone(),
            protocol_satisfied: selected.record.manifest.protocol,
            provider_authenticated: query.trust_requirement.require_authenticated_provider,
            provider_authorized: query.trust_requirement.require_authorized_provider,
            service_available: selected.record.state().is_callable(),
            route_available: selected.hops <= self.config.max_acceptable_hops,
            locality: selected.locality_level,
            health_status,
            deterministic_tiebreak: tiebreak_applied,
            explanation_text,
        };

        let route_hops = if selected.is_local {
            Some(0)
        } else if selected.hops <= self.config.max_acceptable_hops {
            Some(selected.hops)
        } else {
            None
        };

        let resolution = ServiceResolution {
            service_id: query.service_id.clone(),
            instance_id: selected.record.instance_id().clone(),
            provider_node_id: *selected.record.provider_node_id(),
            selected_instance: selected.record,
            reason,
            resolved_at: now,
            route_hops,
            correlation_id: query.correlation_id.clone(),
        };

        // 7. Store in cache if enabled
        if let Some(ref cache) = self.cache {
            cache.put(query.clone(), resolution.clone(), now).await;
        }

        // 8. Emit ServiceEvent::Resolved event
        if let Some(ref tx) = self.event_tx {
            let _ = tx.send(ServiceEvent::Resolved {
                service_id: resolution.service_id.clone(),
                instance_id: resolution.instance_id.clone(),
                provider_node_id: resolution.provider_node_id,
                correlation_id: resolution.correlation_id.clone(),
            });
        }

        self.metrics.inc_success();
        info!(
            "Resolved service '{}' to instance '{}' on provider '{}'",
            resolution.service_id, resolution.instance_id, resolution.provider_node_id
        );

        Ok(resolution)
    }
}
