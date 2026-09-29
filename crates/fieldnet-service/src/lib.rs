//! FIELDNET Service Discovery, Manifests, Registry, and Local Web Architecture (Phase 08).
//!
//! Provides the canonical service model (`ServiceId`, `ServiceInstanceId`, `ServiceManifest`),
//! cryptographic manifest verification with Ed25519, authoritative lifecycle state transitions (`ServiceState`),
//! passive health and telemetry tracking (`ServiceHealth`), persistent SQLite storage (`ServiceStore`),
//! and the thread-safe `ServiceRegistry`.

pub mod advertisement;
pub mod error;
pub mod events;
pub mod gateway;
pub mod mdns;
pub mod model;
pub mod registry;
pub mod resolver;
pub mod wire;

pub use advertisement::{ServiceAdvertisementHandler, ServiceAdvertiser};
pub use error::ServiceError;
pub use events::ServiceEvent;
pub use gateway::{
    BrowserServiceCatalog, BrowserServiceItem, BrowserSession, BrowserSessionStore, GatewayConfig,
    GatewayError, GatewayErrorResponse, GatewayMethod, GatewayRequest, GatewayResponse,
    GatewayState, GatewayStateMachine, LocalServiceDispatcher, LocalWebGateway, PairingChallenge,
    PairingStatus, ServiceDispatcher, ServiceRequestHandler, SessionConfig,
};
pub use mdns::{
    deterministic_fallback_hostname, get_interface_summaries, get_local_interfaces,
    normalize_hostname, MdnsConfig, MdnsError, MdnsRecord, MdnsService, MdnsState,
    MdnsStateMachine, DEFAULT_HOSTNAME, DEFAULT_MDNS_PORT, DEFAULT_SERVICE_TYPE,
};
pub use model::{
    ServiceCapability, ServiceEndpoint, ServiceHealth, ServiceId, ServiceInstanceId,
    ServiceManifest, ServiceManifestBuilder, ServicePermissions, ServiceProtocol, ServiceRecord,
    ServiceState, ServiceVersion, CURRENT_SERVICE_MANIFEST_SCHEMA_VERSION,
};
pub use registry::{RegistryConfig, ServiceRegistry, ServiceStore};
pub use resolver::{
    AuthContext, DirectRouteProvider, LocalOnlyRouteProvider, LocalityLevel, LocalityPreference,
    ResolutionExplanation, ResolverConfig, ResolverMetrics, ResolverMetricsSnapshot,
    ResourceRequirement, RouteProvider, RouteTarget, ServiceQuery, ServiceQueryBuilder,
    ServiceResolution, ServiceResolutionReason, ServiceResolver, TrustRequirement,
};
pub use wire::{
    ServiceMessage, ServiceQueryResponseWire, ServiceQueryWire, ServiceRpcRequestWire,
    ServiceRpcResponseWire, ServiceWithdrawal, MAX_SERVICE_MESSAGE_SIZE,
};
