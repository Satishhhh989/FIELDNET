use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

use crate::error::DataError;

pub mod conflict;
pub mod counter;
pub mod map;
pub mod register;
pub mod set;

pub use conflict::{ConflictClassification, ConflictResolver, ConflictStrategy};
pub use counter::{PnCounter, PnCounterDelta};
pub use map::{CrdtMap, CrdtMapDelta, FieldRegister};
pub use register::{LwwRegister, LwwRegisterDelta, MvRegister, MvRegisterBranch};
pub use set::{ElementTag, OrSet, OrSetDelta};

/// Supported CRDT type identifiers in FIELDNET.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum CrdtType {
    PnCounter = 1,
    OrSet = 2,
    LwwRegister = 3,
    MvRegister = 4,
    CrdtMap = 5,
}

impl CrdtType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PnCounter => "pn_counter",
            Self::OrSet => "or_set",
            Self::LwwRegister => "lww_register",
            Self::MvRegister => "mv_register",
            Self::CrdtMap => "crdt_map",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Result<Self, DataError> {
        match s {
            "pn_counter" | "counter" => Ok(Self::PnCounter),
            "or_set" | "set" => Ok(Self::OrSet),
            "lww_register" | "register" => Ok(Self::LwwRegister),
            "mv_register" => Ok(Self::MvRegister),
            "crdt_map" | "map" => Ok(Self::CrdtMap),
            other => Err(DataError::InvalidDocumentType(format!(
                "Unknown CRDT type: {}",
                other
            ))),
        }
    }
}

/// Abstract CRDT trait for convergent distributed data types.
/// CRDTs operate strictly on state and operations/deltas with zero direct networking dependencies.
pub trait Crdt: Sized + Serialize + DeserializeOwned + Clone + Debug {
    type Delta: Serialize + DeserializeOwned + Clone + Debug;
    type Value;

    /// Associated CRDT family type.
    fn crdt_family() -> CrdtType;

    /// Returns the CRDT family type.
    fn crdt_type(&self) -> CrdtType {
        Self::crdt_family()
    }

    /// Reads the materialized logical value of the CRDT.
    fn read(&self) -> Self::Value;

    /// Applies a delta/operation to mutate the CRDT state.
    fn apply_delta(&mut self, delta: &Self::Delta) -> Result<(), DataError>;

    /// Merges state from a remote replica using join semilattice rules.
    fn merge(&mut self, other: &Self) -> Result<(), DataError>;

    /// Serializes the CRDT state deterministically into canonical bytes.
    fn serialize_state(&self) -> Result<Vec<u8>, DataError> {
        serde_json::to_vec(self).map_err(|e| DataError::SerializationFailure(e.to_string()))
    }

    /// Deserializes a CRDT state from canonical bytes.
    fn deserialize_state(bytes: &[u8]) -> Result<Self, DataError> {
        serde_json::from_slice(bytes).map_err(|e| DataError::SerializationFailure(e.to_string()))
    }

    /// Computes a deterministic 32-byte BLAKE3 state hash over canonical serialized state.
    fn state_hash(&self) -> [u8; 32] {
        let bytes = self.serialize_state().unwrap_or_default();
        let hash = blake3::hash(&bytes);
        *hash.as_bytes()
    }

    /// Returns an inspectable summary of internal state for debugging/diagnostics.
    fn inspect_state(&self) -> String;
}
