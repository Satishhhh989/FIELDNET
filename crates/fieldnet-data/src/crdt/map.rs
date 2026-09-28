use fieldnet_core::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::{Crdt, CrdtType};
use crate::causality::HybridLogicalClock;
use crate::error::DataError;

/// A field register inside a CrdtMap tracking its value, causal timestamp, and deletion tombstone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldRegister {
    pub value: serde_json::Value,
    pub hlc: HybridLogicalClock,
    pub author: NodeId,
    pub deleted: bool,
}

impl FieldRegister {
    pub fn new(value: serde_json::Value, hlc: HybridLogicalClock, author: NodeId) -> Self {
        Self {
            value,
            hlc,
            author,
            deleted: false,
        }
    }

    pub fn tombstone(hlc: HybridLogicalClock, author: NodeId) -> Self {
        Self {
            value: serde_json::Value::Null,
            hlc,
            author,
            deleted: true,
        }
    }

    /// Evaluates if an incoming remote field update wins over the current local field.
    pub fn is_dominated_by(&self, other_hlc: &HybridLogicalClock, other_author: &NodeId) -> bool {
        if other_hlc > &self.hlc {
            true
        } else if other_hlc < &self.hlc {
            false
        } else {
            other_author.as_bytes() > self.author.as_bytes()
        }
    }
}

/// Map CRDT delta operations for individual fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CrdtMapDelta {
    SetField {
        key: String,
        value: serde_json::Value,
        hlc: HybridLogicalClock,
        author: NodeId,
    },
    DeleteField {
        key: String,
        hlc: HybridLogicalClock,
        author: NodeId,
    },
}

/// Distributed Map CRDT (CrdtMap).
/// Provides field-level independence: concurrent updates to disjoint keys merge without conflict.
/// Each field operates as an independent LWW register with support for field-level deletion tombstones.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CrdtMap {
    pub fields: BTreeMap<String, FieldRegister>,
}

impl CrdtMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Retrieves an active field value if it exists and is not deleted.
    pub fn get(&self, key: &str) -> Option<&serde_json::Value> {
        self.fields
            .get(key)
            .and_then(|reg| if reg.deleted { None } else { Some(&reg.value) })
    }

    /// Sets a field with an atomic value, HLC, and author.
    pub fn set(
        &mut self,
        key: impl Into<String>,
        value: serde_json::Value,
        hlc: HybridLogicalClock,
        author: NodeId,
    ) {
        let key_str = key.into();
        let new_reg = FieldRegister::new(value, hlc, author);
        if let Some(existing) = self.fields.get_mut(&key_str) {
            if existing.is_dominated_by(&hlc, &author) {
                *existing = new_reg;
            }
        } else {
            self.fields.insert(key_str, new_reg);
        }
    }

    /// Deletes a field with a deletion tombstone.
    pub fn delete(&mut self, key: &str, hlc: HybridLogicalClock, author: NodeId) {
        let tomb = FieldRegister::tombstone(hlc, author);
        if let Some(existing) = self.fields.get_mut(key) {
            if existing.is_dominated_by(&hlc, &author) {
                *existing = tomb;
            }
        } else {
            self.fields.insert(key.to_string(), tomb);
        }
    }

    /// Reads all active (non-deleted) fields into a key-value map.
    pub fn to_map(&self) -> BTreeMap<String, serde_json::Value> {
        self.fields
            .iter()
            .filter(|(_, reg)| !reg.deleted)
            .map(|(k, reg)| (k.clone(), reg.value.clone()))
            .collect()
    }

    /// Generates a SetField delta.
    pub fn generate_set(
        &self,
        key: impl Into<String>,
        value: serde_json::Value,
        hlc: HybridLogicalClock,
        author: NodeId,
    ) -> CrdtMapDelta {
        CrdtMapDelta::SetField {
            key: key.into(),
            value,
            hlc,
            author,
        }
    }

    /// Generates a DeleteField delta.
    pub fn generate_delete(
        &self,
        key: impl Into<String>,
        hlc: HybridLogicalClock,
        author: NodeId,
    ) -> CrdtMapDelta {
        CrdtMapDelta::DeleteField {
            key: key.into(),
            hlc,
            author,
        }
    }
}

impl Crdt for CrdtMap {
    type Delta = CrdtMapDelta;
    type Value = BTreeMap<String, serde_json::Value>;

    fn crdt_family() -> CrdtType {
        CrdtType::CrdtMap
    }

    fn read(&self) -> Self::Value {
        self.to_map()
    }

    fn apply_delta(&mut self, delta: &Self::Delta) -> Result<(), DataError> {
        match delta {
            CrdtMapDelta::SetField {
                key,
                value,
                hlc,
                author,
            } => {
                self.set(key.clone(), value.clone(), *hlc, *author);
                Ok(())
            }
            CrdtMapDelta::DeleteField { key, hlc, author } => {
                self.delete(key, *hlc, *author);
                Ok(())
            }
        }
    }

    fn merge(&mut self, other: &Self) -> Result<(), DataError> {
        for (key, other_reg) in &other.fields {
            if let Some(local_reg) = self.fields.get_mut(key) {
                if local_reg.is_dominated_by(&other_reg.hlc, &other_reg.author) {
                    *local_reg = other_reg.clone();
                }
            } else {
                self.fields.insert(key.clone(), other_reg.clone());
            }
        }
        Ok(())
    }

    fn inspect_state(&self) -> String {
        let active = self.fields.iter().filter(|(_, r)| !r.deleted).count();
        let tombstones = self.fields.iter().filter(|(_, r)| r.deleted).count();
        format!(
            "CrdtMap(active_fields={}, tombstones={})",
            active, tombstones
        )
    }
}
