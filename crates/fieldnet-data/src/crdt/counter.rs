use fieldnet_core::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::{Crdt, CrdtType};
use crate::error::DataError;

/// Positive-Negative Counter delta representing an atomic increment or decrement.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PnCounterDelta {
    Increment { actor: NodeId, amount: u64 },
    Decrement { actor: NodeId, amount: u64 },
}

/// Convergent distributed Positive-Negative Counter (PN-Counter).
/// Tracks positive increments (P) and negative decrements (N) monotonically per actor.
/// The logical value is sum(P) - sum(N).
/// Merging two replicas takes the point-wise maximum for each actor,
/// guaranteeing commutativity, associativity, and idempotence.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PnCounter {
    /// Positive increments per actor: NodeId -> cumulative positive sum
    pub p: BTreeMap<NodeId, u64>,
    /// Negative decrements per actor: NodeId -> cumulative negative sum
    pub n: BTreeMap<NodeId, u64>,
}

impl PnCounter {
    /// Creates a new empty PN-Counter initialized to 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increments the counter for a given actor by amount.
    pub fn increment(&mut self, actor: NodeId, amount: u64) {
        let entry = self.p.entry(actor).or_insert(0);
        *entry = entry.saturating_add(amount);
    }

    /// Decrements the counter for a given actor by amount.
    pub fn decrement(&mut self, actor: NodeId, amount: u64) {
        let entry = self.n.entry(actor).or_insert(0);
        *entry = entry.saturating_add(amount);
    }

    /// Generates an increment delta.
    pub fn generate_increment(&self, actor: NodeId, amount: u64) -> PnCounterDelta {
        PnCounterDelta::Increment { actor, amount }
    }

    /// Generates a decrement delta.
    pub fn generate_decrement(&self, actor: NodeId, amount: u64) -> PnCounterDelta {
        PnCounterDelta::Decrement { actor, amount }
    }
}

impl Crdt for PnCounter {
    type Delta = PnCounterDelta;
    type Value = i64;

    fn crdt_family() -> CrdtType {
        CrdtType::PnCounter
    }

    fn read(&self) -> i64 {
        let p_sum: u64 = self.p.values().sum();
        let n_sum: u64 = self.n.values().sum();
        (p_sum as i64) - (n_sum as i64)
    }

    fn apply_delta(&mut self, delta: &Self::Delta) -> Result<(), DataError> {
        match delta {
            PnCounterDelta::Increment { actor, amount } => {
                self.increment(*actor, *amount);
                Ok(())
            }
            PnCounterDelta::Decrement { actor, amount } => {
                self.decrement(*actor, *amount);
                Ok(())
            }
        }
    }

    fn merge(&mut self, other: &Self) -> Result<(), DataError> {
        for (actor, &val) in &other.p {
            let entry = self.p.entry(*actor).or_insert(0);
            *entry = (*entry).max(val);
        }
        for (actor, &val) in &other.n {
            let entry = self.n.entry(*actor).or_insert(0);
            *entry = (*entry).max(val);
        }
        Ok(())
    }

    fn inspect_state(&self) -> String {
        format!(
            "PnCounter(value={}, p_actors={}, n_actors={})",
            self.read(),
            self.p.len(),
            self.n.len()
        )
    }
}
