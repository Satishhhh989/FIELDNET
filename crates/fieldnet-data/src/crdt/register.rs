use fieldnet_core::NodeId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use super::{Crdt, CrdtType};
use crate::causality::HybridLogicalClock;
use crate::error::DataError;

/// Delta for a Last-Write-Wins register assignment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LwwRegisterDelta<T> {
    pub value: T,
    pub hlc: HybridLogicalClock,
    pub author: NodeId,
}

/// Last-Write-Wins Register (LWW-Register).
/// Stores a single scalar value along with the Hybrid Logical Clock (HLC) and author NodeId
/// of the latest write.
/// Merges deterministically by comparing HLC timestamps.
/// In case of identical HLC timestamps, the author NodeId breaks ties deterministically.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LwwRegister<T> {
    pub value: T,
    pub hlc: HybridLogicalClock,
    pub author: NodeId,
}

impl<T: Clone> LwwRegister<T> {
    pub fn new(value: T, hlc: HybridLogicalClock, author: NodeId) -> Self {
        Self { value, hlc, author }
    }

    /// Assigns a new value with updated HLC and author.
    pub fn set(&mut self, value: T, hlc: HybridLogicalClock, author: NodeId) {
        if self.should_update(&hlc, &author) {
            self.value = value;
            self.hlc = hlc;
            self.author = author;
        }
    }

    /// Evaluates whether an incoming write wins against local state according to LWW total order.
    pub fn should_update(&self, remote_hlc: &HybridLogicalClock, remote_author: &NodeId) -> bool {
        if remote_hlc > &self.hlc {
            true
        } else if remote_hlc < &self.hlc {
            false
        } else {
            // Tie-break deterministically via NodeId comparison
            remote_author.as_bytes() > self.author.as_bytes()
        }
    }

    /// Generates a set delta.
    pub fn generate_set(
        &self,
        value: T,
        hlc: HybridLogicalClock,
        author: NodeId,
    ) -> LwwRegisterDelta<T> {
        LwwRegisterDelta { value, hlc, author }
    }
}

impl<T: Clone + Serialize + DeserializeOwned + Debug> Crdt for LwwRegister<T> {
    type Delta = LwwRegisterDelta<T>;
    type Value = T;

    fn crdt_family() -> CrdtType {
        CrdtType::LwwRegister
    }

    fn read(&self) -> T {
        self.value.clone()
    }

    fn apply_delta(&mut self, delta: &Self::Delta) -> Result<(), DataError> {
        if self.should_update(&delta.hlc, &delta.author) {
            self.value = delta.value.clone();
            self.hlc = delta.hlc;
            self.author = delta.author;
        }
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<(), DataError> {
        if self.should_update(&other.hlc, &other.author) {
            self.value = other.value.clone();
            self.hlc = other.hlc;
            self.author = other.author;
        }
        Ok(())
    }

    fn inspect_state(&self) -> String {
        format!("LwwRegister(hlc={}, author={})", self.hlc, self.author)
    }
}

/// A concurrent branch in a Multi-Value Register.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MvRegisterBranch<T> {
    pub value: T,
    pub hlc: HybridLogicalClock,
    pub author: NodeId,
}

/// Multi-Value Register (MV-Register).
/// Retains all concurrent updates as discrete sibling branches until explicitly resolved.
/// Causally dominated branches are pruned automatically upon merge.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvRegister<T> {
    pub branches: Vec<MvRegisterBranch<T>>,
}

impl<T: Clone + PartialEq + Ord> MvRegister<T> {
    /// Creates a new MV-Register with an initial branch.
    pub fn new(value: T, hlc: HybridLogicalClock, author: NodeId) -> Self {
        Self {
            branches: vec![MvRegisterBranch { value, hlc, author }],
        }
    }

    /// Reads all concurrent branch values.
    pub fn read_branches(&self) -> Vec<T> {
        self.branches.iter().map(|b| b.value.clone()).collect()
    }

    /// Checks if there is a conflict (multiple concurrent branches exist).
    pub fn is_conflicted(&self) -> bool {
        self.branches.len() > 1
    }

    /// Local assignment overwrites all existing branches (since it causally succeeds all observed branches).
    pub fn assign(&mut self, value: T, hlc: HybridLogicalClock, author: NodeId) {
        self.branches = vec![MvRegisterBranch { value, hlc, author }];
    }

    /// Explicitly resolves a conflicted register by choosing a value.
    pub fn resolve(&mut self, chosen_value: T, hlc: HybridLogicalClock, author: NodeId) {
        self.assign(chosen_value, hlc, author);
    }

    /// Merges remote MV-Register branches.
    pub fn merge_branches(&mut self, other: &Self) {
        let mut all_branches = self.branches.clone();
        for b in &other.branches {
            if !all_branches.contains(b) {
                all_branches.push(b.clone());
            }
        }

        // Prune any branch from the same author that has a strictly lower HLC
        let mut surviving: Vec<MvRegisterBranch<T>> = Vec::new();
        for b in all_branches {
            let dominated = surviving
                .iter()
                .any(|existing| existing.author == b.author && existing.hlc > b.hlc);
            if !dominated {
                surviving.retain(|existing| !(existing.author == b.author && b.hlc > existing.hlc));
                surviving.push(b);
            }
        }

        surviving.sort_by(|a, b| {
            a.hlc
                .cmp(&b.hlc)
                .reverse()
                .then_with(|| a.author.cmp(&b.author))
        });
        self.branches = surviving;
    }
}
