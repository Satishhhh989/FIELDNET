use async_trait::async_trait;
use fieldnet_core::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Route metadata obtained from Phase 05 routing without exposing internal route tables.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct RouteTarget {
    /// Destination provider sovereign NodeId.
    pub destination: NodeId,
    /// Next-hop node on the overlay mesh.
    pub next_hop: NodeId,
    /// Distance in overlay network hops (1 for direct peer, >1 for multi-hop).
    pub hops: u8,
    /// Route metric or cost.
    pub metric: u32,
}

/// Abstract routing interface allowing the `ServiceResolver` to inspect reachability
/// without depending directly on concrete transport or routing implementations.
#[async_trait]
pub trait RouteProvider: Send + Sync {
    /// Queries whether a reachable active route exists to `destination`.
    async fn get_route(&self, destination: &NodeId) -> Option<RouteTarget>;
}

/// In-memory route table implementation for unit tests and direct node topologies.
pub struct DirectRouteProvider {
    routes: Arc<RwLock<HashMap<NodeId, RouteTarget>>>,
}

impl DirectRouteProvider {
    /// Creates an empty `DirectRouteProvider`.
    pub fn new() -> Self {
        Self {
            routes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Adds or updates a route entry.
    pub async fn add_route(&self, destination: NodeId, next_hop: NodeId, hops: u8, metric: u32) {
        let mut map = self.routes.write().await;
        map.insert(
            destination,
            RouteTarget {
                destination,
                next_hop,
                hops,
                metric,
            },
        );
    }

    /// Removes a route entry (e.g. peer disconnected).
    pub async fn remove_route(&self, destination: &NodeId) {
        let mut map = self.routes.write().await;
        map.remove(destination);
    }

    /// Clears all routes.
    pub async fn clear(&self) {
        let mut map = self.routes.write().await;
        map.clear();
    }
}

impl Default for DirectRouteProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl RouteProvider for DirectRouteProvider {
    async fn get_route(&self, destination: &NodeId) -> Option<RouteTarget> {
        let map = self.routes.read().await;
        map.get(destination).cloned()
    }
}

/// Route provider that returns no routes (used when all remote nodes are unreachable).
pub struct LocalOnlyRouteProvider;

#[async_trait]
impl RouteProvider for LocalOnlyRouteProvider {
    async fn get_route(&self, _destination: &NodeId) -> Option<RouteTarget> {
        None
    }
}
