//! FIELDNET Local Web Gateway (Phase 08).
//!
//! Provides a browser-facing HTTP reverse proxy and adapter translating
//! standard web requests into decentralized FIELDNET service invocations.

pub mod auth;
pub mod config;
pub mod discovery_api;
pub mod dispatch;
pub mod error;
pub mod model;
pub mod pairing_api;
pub mod router;
pub mod server;
pub mod session;
pub mod state;
pub mod streaming;

pub use config::GatewayConfig;
pub use discovery_api::{BrowserServiceCatalog, BrowserServiceItem};
pub use dispatch::{
    dispatch_service_request, LocalServiceDispatcher, ServiceDispatcher, ServiceRequestHandler,
};
pub use error::GatewayError;
pub use model::{GatewayErrorResponse, GatewayMethod, GatewayRequest, GatewayResponse};
pub use pairing_api::{
    PairConfirmRequest, PairConfirmResponse, PairStartRequest, PairStartResponse, PairStatusQuery,
    PairStatusResponse, SessionRefreshResponse, SessionRevokeResponse,
};
pub use server::LocalWebGateway;
pub use session::{
    BrowserSession, BrowserSessionStore, PairingChallenge, PairingStatus, SessionConfig,
};
pub use state::{GatewayState, GatewayStateMachine};
