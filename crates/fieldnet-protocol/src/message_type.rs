use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
#[repr(u8)]
pub enum MessageType {
    Auth = 1,
    Data = 2,
    Ack = 3,
    Error = 4,
    Route = 5,
    Sync = 6,
    File = 7,
}

impl MessageType {
    pub fn is_forwardable(&self) -> bool {
        match self {
            Self::Auth => false,
            Self::Data => true,
            Self::Ack => true,
            Self::Error => true,
            Self::Route => false,
            Self::Sync => true,
            Self::File => true,
        }
    }
}
