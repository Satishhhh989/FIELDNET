use fieldnet_core::NodeId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use super::{Crdt, CrdtType};
use crate::causality::HybridLogicalClock;
use crate::error::DataError;

/// Globally unique tag identifying a specific element addition event.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct ElementTag {
    pub actor_id: NodeId,
    pub hlc: HybridLogicalClock,
    pub tag_id: u64,
}

impl ElementTag {
    pub fn new(actor_id: NodeId, hlc: HybridLogicalClock, tag_id: u64) -> Self {
        Self {
            actor_id,
            hlc,
            tag_id,
        }
    }
}

/// Observed-Remove Set delta.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrSetDelta<T> {
    Add {
        item: T,
        tag: ElementTag,
    },
    Remove {
        item: T,
        observed_tags: Vec<ElementTag>,
    },
}

/// Add-Wins Observed-Remove Set (OR-Set).
/// Every addition generates a globally unique tag.
/// A removal observes all tags currently active for the item on the local replica and adds them to tombstones.
/// If an addition is concurrent with a removal, the addition generates a new, unobserved tag
/// that survives the merge (Add-Wins).
/// Tombstones prevent resurrection when lagged or offline replicas reconnect.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrSet<T: Ord> {
    /// Active entries: Item -> Set of active unique tags
    pub entries: BTreeMap<T, BTreeSet<ElementTag>>,
    /// Tombstone set of observed deletions
    pub tombstones: BTreeSet<ElementTag>,
}

impl<T: Ord> Default for OrSet<T> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
            tombstones: BTreeSet::new(),
        }
    }
}

impl<T: Ord + Clone> OrSet<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Checks if the item is an active member of the set.
    pub fn contains(&self, item: &T) -> bool {
        if let Some(tags) = self.entries.get(item) {
            tags.iter().any(|t| !self.tombstones.contains(t))
        } else {
            false
        }
    }

    /// Returns all current active values in deterministic sorted order.
    pub fn values(&self) -> Vec<T> {
        self.entries
            .iter()
            .filter(|(_, tags)| tags.iter().any(|t| !self.tombstones.contains(t)))
            .map(|(item, _)| item.clone())
            .collect()
    }

    /// Returns the number of active elements in the set.
    pub fn len(&self) -> usize {
        self.values().len()
    }

    /// Checks if the set has no active elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Generates an Add delta for an element.
    pub fn generate_add(
        &self,
        item: T,
        actor_id: NodeId,
        hlc: HybridLogicalClock,
        tag_id: u64,
    ) -> OrSetDelta<T> {
        let tag = ElementTag::new(actor_id, hlc, tag_id);
        OrSetDelta::Add { item, tag }
    }

    /// Generates a Remove delta for an element, observing all currently active tags.
    pub fn generate_remove(&self, item: &T) -> OrSetDelta<T> {
        let observed_tags = if let Some(tags) = self.entries.get(item) {
            tags.iter()
                .filter(|t| !self.tombstones.contains(t))
                .copied()
                .collect()
        } else {
            Vec::new()
        };
        OrSetDelta::Remove {
            item: item.clone(),
            observed_tags,
        }
    }

    /// Adds an element directly using the specified tag.
    pub fn add(&mut self, item: T, tag: ElementTag) {
        if !self.tombstones.contains(&tag) {
            self.entries.entry(item).or_default().insert(tag);
        }
    }

    /// Removes an element directly by tombstoning all observed tags.
    pub fn remove(&mut self, item: &T, observed_tags: &[ElementTag]) {
        for tag in observed_tags {
            self.tombstones.insert(*tag);
        }
        if let Some(tags) = self.entries.get_mut(item) {
            tags.retain(|t| !self.tombstones.contains(t));
            if tags.is_empty() {
                self.entries.remove(item);
            }
        }
    }
}

impl<T: Ord + Clone + Serialize + DeserializeOwned + Debug> Crdt for OrSet<T> {
    type Delta = OrSetDelta<T>;
    type Value = Vec<T>;

    fn crdt_family() -> CrdtType {
        CrdtType::OrSet
    }

    fn read(&self) -> Vec<T> {
        self.values()
    }

    fn apply_delta(&mut self, delta: &Self::Delta) -> Result<(), DataError> {
        match delta {
            OrSetDelta::Add { item, tag } => {
                self.add(item.clone(), *tag);
                Ok(())
            }
            OrSetDelta::Remove {
                item,
                observed_tags,
            } => {
                self.remove(item, observed_tags);
                Ok(())
            }
        }
    }

    fn merge(&mut self, other: &Self) -> Result<(), DataError> {
        // 1. Union tombstones
        self.tombstones.extend(other.tombstones.iter().copied());

        // 2. Union entries
        for (item, tags) in &other.entries {
            let local_tags = self.entries.entry(item.clone()).or_default();
            local_tags.extend(tags.iter().copied());
        }

        // 3. Prune tombstoned tags
        for tags in self.entries.values_mut() {
            tags.retain(|t| !self.tombstones.contains(t));
        }

        // 4. Remove items with no active tags remaining
        self.entries.retain(|_, tags| !tags.is_empty());

        Ok(())
    }

    fn inspect_state(&self) -> String {
        format!(
            "OrSet(active_elements={}, total_tombstones={})",
            self.entries.len(),
            self.tombstones.len()
        )
    }
}
