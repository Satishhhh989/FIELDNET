use serde::{Deserialize, Serialize};
use std::fmt;

/// Content actions governed by the ContentAuthorization layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ContentAction {
    /// Retrieve and view content bytes or chunks.
    Read,
    /// Ingest content, create new versions, or write metadata.
    Write,
    /// Delete content from storage or issue tombstones.
    Delete,
    /// Seed or transfer content copies to remote peers.
    Replicate,
    /// Pin content locally to immunize from garbage collection.
    Pin,
    /// Issue capability tokens or share permissions with peers.
    Share,
}

impl ContentAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Read => "READ",
            Self::Write => "WRITE",
            Self::Delete => "DELETE",
            Self::Replicate => "REPLICATE",
            Self::Pin => "PIN",
            Self::Share => "SHARE",
        }
    }

    pub fn to_bitmask(self) -> u32 {
        match self {
            Self::Read => 1 << 0,
            Self::Write => 1 << 1,
            Self::Delete => 1 << 2,
            Self::Replicate => 1 << 3,
            Self::Pin => 1 << 4,
            Self::Share => 1 << 5,
        }
    }

    pub fn actions_to_bitmask(actions: &[ContentAction]) -> u32 {
        let mut mask = 0;
        for a in actions {
            mask |= a.to_bitmask();
        }
        mask
    }

    pub fn bitmask_to_actions(mask: u32) -> Vec<ContentAction> {
        let mut actions = Vec::new();
        if (mask & (1 << 0)) != 0 {
            actions.push(ContentAction::Read);
        }
        if (mask & (1 << 1)) != 0 {
            actions.push(ContentAction::Write);
        }
        if (mask & (1 << 2)) != 0 {
            actions.push(ContentAction::Delete);
        }
        if (mask & (1 << 3)) != 0 {
            actions.push(ContentAction::Replicate);
        }
        if (mask & (1 << 4)) != 0 {
            actions.push(ContentAction::Pin);
        }
        if (mask & (1 << 5)) != 0 {
            actions.push(ContentAction::Share);
        }
        actions
    }
}

impl fmt::Display for ContentAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
