use fieldnet_core::{NodeId, Timestamp};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::Mutex;

use crate::error::DataError;

/// Hybrid Logical Clock (HLC) combining physical UTC milliseconds and a logical counter.
/// Serialized as 12 big-endian bytes: [physical: 8B] || [logical: 4B].
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct HybridLogicalClock {
    pub physical: u64,
    pub logical: u32,
}

impl HybridLogicalClock {
    pub const fn new(physical: u64, logical: u32) -> Self {
        Self { physical, logical }
    }

    pub fn now() -> Self {
        Self {
            physical: Timestamp::now().as_millis(),
            logical: 0,
        }
    }

    /// Serializes HLC as a canonical 12-byte big-endian representation.
    pub fn to_bytes(&self) -> [u8; 12] {
        let mut bytes = [0u8; 12];
        bytes[0..8].copy_from_slice(&self.physical.to_be_bytes());
        bytes[8..12].copy_from_slice(&self.logical.to_be_bytes());
        bytes
    }

    /// Deserializes HLC from a 12-byte big-endian slice.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DataError> {
        if bytes.len() != 12 {
            return Err(DataError::CorruptedData(format!(
                "Invalid HLC byte length: expected 12, got {}",
                bytes.len()
            )));
        }
        let mut p_bytes = [0u8; 8];
        p_bytes.copy_from_slice(&bytes[0..8]);
        let mut l_bytes = [0u8; 4];
        l_bytes.copy_from_slice(&bytes[8..12]);

        Ok(Self {
            physical: u64::from_be_bytes(p_bytes),
            logical: u32::from_be_bytes(l_bytes),
        })
    }
}

impl Ord for HybridLogicalClock {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.physical.cmp(&other.physical) {
            Ordering::Equal => self.logical.cmp(&other.logical),
            other_ord => other_ord,
        }
    }
}

impl PartialOrd for HybridLogicalClock {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for HybridLogicalClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HLC({}:{})", self.physical, self.logical)
    }
}

impl fmt::Display for HybridLogicalClock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.physical, self.logical)
    }
}

/// Thread-safe generator and coordinator for Hybrid Logical Clocks.
pub struct HlcEngine {
    latest: Mutex<HybridLogicalClock>,
}

impl HlcEngine {
    pub fn new(initial: HybridLogicalClock) -> Self {
        Self {
            latest: Mutex::new(initial),
        }
    }

    pub fn current(&self) -> HybridLogicalClock {
        *self.latest.lock().unwrap()
    }

    /// Advances the clock for a new local event.
    pub fn tick(&self) -> HybridLogicalClock {
        let p_now = Timestamp::now().as_millis();
        let mut latest = self.latest.lock().unwrap();

        let (p_next, l_next) = if p_now > latest.physical {
            (p_now, 0)
        } else {
            (latest.physical, latest.logical.saturating_add(1))
        };

        *latest = HybridLogicalClock::new(p_next, l_next);
        *latest
    }

    /// Updates the local HLC upon receiving a remote HLC timestamp.
    pub fn update(&self, remote: HybridLogicalClock) -> HybridLogicalClock {
        let p_now = Timestamp::now().as_millis();
        let mut latest = self.latest.lock().unwrap();

        let p_max = p_now.max(latest.physical).max(remote.physical);
        let l_next = if p_max == latest.physical && p_max == remote.physical {
            latest.logical.max(remote.logical).saturating_add(1)
        } else if p_max == latest.physical {
            latest.logical.saturating_add(1)
        } else if p_max == remote.physical {
            remote.logical.saturating_add(1)
        } else {
            0
        };

        *latest = HybridLogicalClock::new(p_max, l_next);
        *latest
    }
}

/// Causal relationship classification between two state vectors.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CausalRelation {
    /// Both vectors represent the identical causal state ($V_A = V_B$).
    Equal,
    /// Self causally precedes Other ($V_A < V_B$; Other dominates Self).
    HappensBefore,
    /// Self causally succeeds Other ($V_A > V_B$; Self dominates Other).
    HappensAfter,
    /// Concurrent mutations with no causal ordering ($V_A \parallel V_B$).
    Concurrent,
}

/// Causal State Vector mapping NodeIds to monotonic sequence counters.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateVector {
    pub entries: HashMap<NodeId, u64>,
}

impl StateVector {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn get(&self, node: &NodeId) -> u64 {
        self.entries.get(node).copied().unwrap_or(0)
    }

    pub fn set(&mut self, node: NodeId, seq: u64) {
        if seq == 0 {
            self.entries.remove(&node);
        } else {
            self.entries.insert(node, seq);
        }
    }

    pub fn advance(&mut self, node: NodeId, seq: u64) {
        let current = self.get(&node);
        if seq > current {
            self.set(node, seq);
        }
    }

    /// Evaluates the formal causal relationship against another StateVector.
    pub fn relation(&self, other: &StateVector) -> CausalRelation {
        let mut all_keys: HashSet<NodeId> = self.entries.keys().copied().collect();
        all_keys.extend(other.entries.keys().copied());

        let mut has_greater = false;
        let mut has_less = false;

        for key in all_keys {
            let v_self = self.get(&key);
            let v_other = other.get(&key);

            if v_self > v_other {
                has_greater = true;
            } else if v_self < v_other {
                has_less = true;
            }

            if has_greater && has_less {
                return CausalRelation::Concurrent;
            }
        }

        match (has_greater, has_less) {
            (false, false) => CausalRelation::Equal,
            (true, false) => CausalRelation::HappensAfter,
            (false, true) => CausalRelation::HappensBefore,
            (true, true) => CausalRelation::Concurrent,
        }
    }

    pub fn dominates(&self, other: &StateVector) -> bool {
        self.relation(other) == CausalRelation::HappensAfter
    }

    pub fn dominated_by(&self, other: &StateVector) -> bool {
        self.relation(other) == CausalRelation::HappensBefore
    }

    pub fn is_concurrent(&self, other: &StateVector) -> bool {
        self.relation(other) == CausalRelation::Concurrent
    }

    pub fn equals(&self, other: &StateVector) -> bool {
        self.relation(other) == CausalRelation::Equal
    }

    /// Merges another state vector by taking the point-wise maximum.
    pub fn merge(&mut self, other: &StateVector) {
        for (node, seq) in &other.entries {
            self.advance(*node, *seq);
        }
    }

    /// Encodes StateVector deterministically into a binary payload.
    /// Format: [count: u32] || ([node_id: 32B] || [seq: 8B])* sorted by NodeId.
    pub fn to_bytes(&self) -> Vec<u8> {
        let sorted: BTreeMap<NodeId, u64> = self.entries.iter().map(|(k, v)| (*k, *v)).collect();
        let mut bytes = Vec::with_capacity(4 + sorted.len() * 40);
        bytes.extend_from_slice(&(sorted.len() as u32).to_be_bytes());

        for (node, seq) in sorted {
            bytes.extend_from_slice(node.as_bytes());
            bytes.extend_from_slice(&seq.to_be_bytes());
        }
        bytes
    }

    /// Decodes StateVector from a deterministic binary payload.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DataError> {
        if bytes.len() < 4 {
            return Err(DataError::CorruptedData(
                "StateVector bytes too short".to_string(),
            ));
        }
        let count = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let expected_len = 4 + count * 40;
        if bytes.len() != expected_len {
            return Err(DataError::CorruptedData(format!(
                "Invalid StateVector byte length: expected {}, got {}",
                expected_len,
                bytes.len()
            )));
        }

        let mut entries = HashMap::with_capacity(count);
        let mut offset = 4;
        for _ in 0..count {
            let mut node_bytes = [0u8; 32];
            node_bytes.copy_from_slice(&bytes[offset..offset + 32]);
            offset += 32;

            let mut seq_bytes = [0u8; 8];
            seq_bytes.copy_from_slice(&bytes[offset..offset + 8]);
            offset += 8;

            let node = NodeId::from_bytes(node_bytes);
            let seq = u64::from_be_bytes(seq_bytes);
            entries.insert(node, seq);
        }

        Ok(Self { entries })
    }
}
