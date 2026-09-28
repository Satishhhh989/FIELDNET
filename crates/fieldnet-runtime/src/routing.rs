use fieldnet_core::{NodeId, Timestamp};
use fieldnet_protocol::{RouteAdvertisement, RouteEntry};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

pub const DEFAULT_ROUTE_TTL_SECS: u64 = 60;
pub const MAX_ROUTE_HOPS: u8 = 16;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouteInfo {
    pub destination: NodeId,
    pub next_hop: NodeId,
    pub hops: u8,
    pub metric: u32,
    pub sequence: u64,
    pub updated_at: Timestamp,
    pub expires_at: Timestamp,
}

impl RouteInfo {
    pub fn is_expired(&self) -> bool {
        self.expires_at.is_expired()
    }
}

pub struct RoutingTable {
    local_node_id: NodeId,
    routes: Arc<RwLock<HashMap<NodeId, RouteInfo>>>,
}

impl RoutingTable {
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            routes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Looks up the best next hop for a given destination.
    pub async fn get_next_hop(&self, destination: &NodeId) -> Option<NodeId> {
        let routes = self.routes.read().await;
        if let Some(route) = routes.get(destination) {
            if !route.is_expired() {
                return Some(route.next_hop);
            }
        }
        None
    }

    /// Looks up detailed route information for a given destination.
    pub async fn get_route(&self, destination: &NodeId) -> Option<RouteInfo> {
        let routes = self.routes.read().await;
        if let Some(route) = routes.get(destination) {
            if !route.is_expired() {
                return Some(route.clone());
            }
        }
        None
    }

    /// Manually marks a route to a destination as expired (used in testing and fault injection).
    pub async fn expire_route(&self, destination: &NodeId) {
        let mut routes = self.routes.write().await;
        if let Some(route) = routes.get_mut(destination) {
            route.expires_at = Timestamp::now().sub_seconds(10);
        }
    }

    /// Inserts or updates a direct 1-hop neighbor route.
    pub async fn add_direct_neighbor(&self, neighbor: NodeId) {
        if neighbor == self.local_node_id {
            return;
        }

        let now = Timestamp::now();
        let mut routes = self.routes.write().await;
        routes.insert(
            neighbor,
            RouteInfo {
                destination: neighbor,
                next_hop: neighbor,
                hops: 1,
                metric: 1,
                sequence: 1,
                updated_at: now,
                expires_at: now.add_seconds(DEFAULT_ROUTE_TTL_SECS),
            },
        );
        debug!("RoutingTable: added direct 1-hop route to {}", neighbor);
    }

    /// Processes a RouteAdvertisement received from an authenticated neighbor.
    pub async fn update_from_advertisement(
        &self,
        sender: NodeId,
        advert: RouteAdvertisement,
    ) -> bool {
        let mut changed = false;
        let now = Timestamp::now();
        let mut routes = self.routes.write().await;

        for entry in advert.routes {
            // Loop prevention:
            // 1. Never route to ourselves
            if entry.destination == self.local_node_id {
                continue;
            }
            // 2. Reject routes exceeding max hop count
            if entry.hops >= MAX_ROUTE_HOPS {
                continue;
            }

            let new_hops = entry.hops.saturating_add(1);
            let new_metric = new_hops as u32;

            if let Some(existing) = routes.get_mut(&entry.destination) {
                // If the path through sender is shorter, or sender is existing next_hop with refreshed route
                if new_hops < existing.hops
                    || (existing.next_hop == sender && entry.sequence >= existing.sequence)
                    || existing.is_expired()
                {
                    existing.next_hop = sender;
                    existing.hops = new_hops;
                    existing.metric = new_metric;
                    existing.sequence = entry.sequence;
                    existing.updated_at = now;
                    existing.expires_at = now.add_seconds(DEFAULT_ROUTE_TTL_SECS);
                    changed = true;
                    debug!(
                        "RoutingTable: updated route to {} via {} (hops: {})",
                        entry.destination, sender, new_hops
                    );
                }
            } else {
                // New destination discovered
                routes.insert(
                    entry.destination,
                    RouteInfo {
                        destination: entry.destination,
                        next_hop: sender,
                        hops: new_hops,
                        metric: new_metric,
                        sequence: entry.sequence,
                        updated_at: now,
                        expires_at: now.add_seconds(DEFAULT_ROUTE_TTL_SECS),
                    },
                );
                changed = true;
                info!(
                    "RoutingTable: discovered route to {} via {} (hops: {})",
                    entry.destination, sender, new_hops
                );
            }
        }

        changed
    }

    /// Invalidates and removes all routes whose next hop is the specified disconnected peer.
    pub async fn remove_routes_via(&self, next_hop: &NodeId) -> Vec<NodeId> {
        let mut removed = Vec::new();
        let mut routes = self.routes.write().await;
        routes.retain(|dest, info| {
            if &info.next_hop == next_hop {
                removed.push(*dest);
                false
            } else {
                true
            }
        });
        if !removed.is_empty() {
            info!(
                "RoutingTable: removed {} routes through disconnected next hop {}",
                removed.len(),
                next_hop
            );
        }
        removed
    }

    /// Generates a RouteAdvertisement listing currently active routes to advertise to neighbors.
    pub async fn generate_advertisement(&self) -> RouteAdvertisement {
        let routes = self.routes.read().await;
        let mut entries = Vec::new();

        // Always include self as distance 0 so directly connected neighbors establish/refresh 1-hop route
        entries.push(RouteEntry {
            destination: self.local_node_id,
            hops: 0,
            sequence: 1,
        });

        for (dest, info) in routes.iter() {
            if !info.is_expired() && info.hops < MAX_ROUTE_HOPS {
                entries.push(RouteEntry {
                    destination: *dest,
                    hops: info.hops,
                    sequence: info.sequence,
                });
            }
        }

        RouteAdvertisement::new(self.local_node_id, entries)
    }

    /// Purges all expired routes from the table.
    pub async fn purge_expired(&self) -> usize {
        let mut routes = self.routes.write().await;
        let before = routes.len();
        routes.retain(|_, info| !info.is_expired());
        before - routes.len()
    }

    /// Returns a snapshot of all active routes.
    pub async fn get_all_routes(&self) -> Vec<RouteInfo> {
        let routes = self.routes.read().await;
        routes
            .values()
            .filter(|r| !r.is_expired())
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_routing_table_direct_and_advertisement() {
        let node_a = NodeId::from_bytes([1u8; 32]);
        let node_b = NodeId::from_bytes([2u8; 32]);
        let node_c = NodeId::from_bytes([3u8; 32]);

        let table_a = RoutingTable::new(node_a);
        table_a.add_direct_neighbor(node_b).await;

        assert_eq!(table_a.get_next_hop(&node_b).await, Some(node_b));
        assert_eq!(table_a.get_next_hop(&node_c).await, None);

        // B advertises reachability to C (hops=1)
        let advert_b = RouteAdvertisement::new(
            node_b,
            vec![RouteEntry {
                destination: node_c,
                hops: 1,
                sequence: 1,
            }],
        );

        let changed = table_a.update_from_advertisement(node_b, advert_b).await;
        assert!(changed);

        // Now A should know path to C is via B (hops=2)
        assert_eq!(table_a.get_next_hop(&node_c).await, Some(node_b));

        // When B disconnects, routes via B are removed
        let removed = table_a.remove_routes_via(&node_b).await;
        assert!(removed.contains(&node_c));
        assert_eq!(table_a.get_next_hop(&node_c).await, None);
    }
}
