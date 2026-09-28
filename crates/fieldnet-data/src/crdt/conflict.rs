use fieldnet_core::NodeId;
use serde::{Deserialize, Serialize};

use crate::causality::{CausalRelation, HybridLogicalClock, StateVector};

/// Explicit classification of an incoming operation or state relative to local state.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum ConflictClassification {
    /// Exact same state or duplicate operation; no action required.
    NoConflict,
    /// Inbound operation causally succeeds local state (happens-after).
    CausalUpdate,
    /// Inbound operation is concurrent with local operations; CRDT join semilattice handles merge.
    ConcurrentUpdate,
    /// Non-CRDT or conflicting register update that requires explicit conflict resolution.
    UnmergeableConflict,
    /// Conflict was successfully resolved according to configured strategy.
    Resolved,
}

/// Strategy for resolving unmergeable conflicts.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum ConflictStrategy {
    /// Deterministic Last-Write-Wins based on HLC and NodeId tie-break.
    LastWriteWins,
    /// Preserve both alternatives in a multi-value branch.
    MultiValueBranch,
    /// Application-defined resolution.
    ApplicationDefined,
}

/// Conflict Resolver component for classifying and resolving distributed updates.
pub struct ConflictResolver;

impl ConflictResolver {
    /// Classifies an operation given local and remote causal metadata.
    pub fn classify_operation(
        local_sv: &StateVector,
        remote_actor: &NodeId,
        remote_seq: u64,
        local_hlc: &HybridLogicalClock,
        remote_hlc: &HybridLogicalClock,
        is_crdt: bool,
    ) -> ConflictClassification {
        let local_known_seq = local_sv.get(remote_actor);
        if remote_seq <= local_known_seq {
            // Already observed or superseded
            return ConflictClassification::NoConflict;
        }

        // Check causal precedence
        if remote_hlc > local_hlc {
            ConflictClassification::CausalUpdate
        } else if is_crdt {
            ConflictClassification::ConcurrentUpdate
        } else {
            ConflictClassification::UnmergeableConflict
        }
    }

    /// Classifies state comparison between two state vectors.
    pub fn classify_state_vector(
        local_sv: &StateVector,
        remote_sv: &StateVector,
        is_crdt: bool,
    ) -> ConflictClassification {
        match local_sv.relation(remote_sv) {
            CausalRelation::Equal => ConflictClassification::NoConflict,
            CausalRelation::HappensBefore => ConflictClassification::CausalUpdate,
            CausalRelation::HappensAfter => ConflictClassification::NoConflict,
            CausalRelation::Concurrent => {
                if is_crdt {
                    ConflictClassification::ConcurrentUpdate
                } else {
                    ConflictClassification::UnmergeableConflict
                }
            }
        }
    }

    /// Resolves a register conflict using deterministic Last-Write-Wins rules.
    pub fn resolve_lww<'a>(
        val_a: &'a [u8],
        hlc_a: &HybridLogicalClock,
        actor_a: &NodeId,
        val_b: &'a [u8],
        hlc_b: &HybridLogicalClock,
        actor_b: &NodeId,
    ) -> (&'a [u8], HybridLogicalClock, NodeId) {
        if hlc_a > hlc_b {
            (val_a, *hlc_a, *actor_a)
        } else if hlc_b > hlc_a {
            (val_b, *hlc_b, *actor_b)
        } else if actor_a.as_bytes() > actor_b.as_bytes() {
            (val_a, *hlc_a, *actor_a)
        } else {
            (val_b, *hlc_b, *actor_b)
        }
    }
}
