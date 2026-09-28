use fieldnet_core::{NodeId, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct RouteEntry {
    pub destination: NodeId,
    pub hops: u8,
    pub sequence: u64,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct RouteAdvertisement {
    pub originator: NodeId,
    pub routes: Vec<RouteEntry>,
    pub timestamp: Timestamp,
}

impl RouteAdvertisement {
    pub fn new(originator: NodeId, routes: Vec<RouteEntry>) -> Self {
        Self {
            originator,
            routes,
            timestamp: Timestamp::now(),
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_route_advertisement_serde() {
        let node_a = NodeId::from_bytes([1u8; 32]);
        let node_c = NodeId::from_bytes([3u8; 32]);
        let advert = RouteAdvertisement::new(
            node_a,
            vec![RouteEntry {
                destination: node_c,
                hops: 1,
                sequence: 42,
            }],
        );

        let bytes = advert.to_bytes().unwrap();
        let decoded = RouteAdvertisement::from_bytes(&bytes).unwrap();
        assert_eq!(advert, decoded);
    }
}
