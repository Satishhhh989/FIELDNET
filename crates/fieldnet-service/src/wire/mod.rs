pub mod message;

pub use message::{
    ServiceMessage, ServiceQueryResponseWire, ServiceQueryWire, ServiceRpcRequestWire,
    ServiceRpcResponseWire, ServiceWithdrawal, MAX_SERVICE_MESSAGE_SIZE,
};
