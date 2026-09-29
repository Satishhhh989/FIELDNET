use fieldnet_core::NodeId;
use serde::{Deserialize, Serialize};

use crate::model::{ServiceId, ServiceInstanceId};

/// Events emitted across the FIELDNET runtime event bus by the Service subsystem.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum ServiceEvent {
    /// A service instance was registered into the catalog.
    Registered {
        service_id: ServiceId,
        instance_id: ServiceInstanceId,
        provider_node_id: NodeId,
        is_local: bool,
    },
    /// An existing service instance was updated with a newer sequence.
    Updated {
        service_id: ServiceId,
        instance_id: ServiceInstanceId,
        sequence: u64,
    },
    /// A service manifest's cryptographic signature was verified.
    Authenticated {
        service_id: ServiceId,
        instance_id: ServiceInstanceId,
    },
    /// A service instance transitioned to Available.
    Available {
        service_id: ServiceId,
        instance_id: ServiceInstanceId,
        provider_node_id: NodeId,
    },
    /// A service instance transitioned to Degraded due to transient issues.
    Degraded {
        service_id: ServiceId,
        instance_id: ServiceInstanceId,
        reason: String,
    },
    /// A service instance transitioned to Unavailable (peer disconnect, route drop).
    Unavailable {
        service_id: ServiceId,
        instance_id: ServiceInstanceId,
        provider_node_id: NodeId,
        reason: String,
    },
    /// A service instance expired because its TTL was exceeded without refresh.
    Expired {
        service_id: ServiceId,
        instance_id: ServiceInstanceId,
        provider_node_id: NodeId,
    },
    /// A service instance was explicitly unregistered or removed.
    Removed {
        service_id: ServiceId,
        instance_id: ServiceInstanceId,
        provider_node_id: NodeId,
    },
    /// A service query was successfully resolved to a concrete provider instance.
    Resolved {
        service_id: ServiceId,
        instance_id: ServiceInstanceId,
        provider_node_id: NodeId,
        correlation_id: Option<String>,
    },
    /// A new remote service manifest was discovered from the mesh.
    Discovered {
        service_id: ServiceId,
        instance_id: ServiceInstanceId,
        provider_node_id: NodeId,
    },
    /// A service instance was explicitly withdrawn by its hosting provider.
    Withdrawn {
        service_id: ServiceId,
        instance_id: ServiceInstanceId,
        provider_node_id: NodeId,
    },
    /// A service advertisement was dispatched across the network overlay.
    AdvertisementSent {
        service_id: ServiceId,
        instance_id: ServiceInstanceId,
        destination: Option<NodeId>,
    },
    /// A service advertisement was received from a remote peer or origin.
    AdvertisementReceived {
        service_id: ServiceId,
        instance_id: ServiceInstanceId,
        source: NodeId,
    },
    /// A service advertisement was relayed/forwarded across a multi-hop route.
    AdvertisementForwarded {
        service_id: ServiceId,
        instance_id: ServiceInstanceId,
        relay: NodeId,
        next_hop: NodeId,
    },
    /// An inbound service advertisement was rejected (invalid signature, expired, etc.).
    AdvertisementRejected {
        service_id: ServiceId,
        instance_id: ServiceInstanceId,
        source: NodeId,
        reason: String,
    },
    /// A service discovery query was dispatched towards a target node.
    QuerySent {
        service_id: ServiceId,
        query_id: fieldnet_core::MessageId,
        destination: NodeId,
    },
    /// A service discovery query was received from a requester.
    QueryReceived {
        service_id: ServiceId,
        query_id: fieldnet_core::MessageId,
        requester: NodeId,
    },
    /// A response to a service query was sent back to the requester.
    QueryResponseSent {
        service_id: ServiceId,
        query_id: fieldnet_core::MessageId,
        responder: NodeId,
        destination: NodeId,
        count: usize,
    },
    /// A response to a service query was received containing advertised manifests.
    QueryResponseReceived {
        service_id: ServiceId,
        query_id: fieldnet_core::MessageId,
        responder: NodeId,
        count: usize,
    },
    /// Local mDNS subsystem was started and bound.
    MdnsStarted { hostname: String, port: u16 },
    /// Local mDNS subsystem was stopped and advertisements withdrawn.
    MdnsStopped,
    /// An mDNS / DNS-SD service advertisement was activated.
    MdnsAdvertisementStarted {
        service_name: String,
        hostname: String,
        port: u16,
    },
    /// An mDNS / DNS-SD service advertisement was withdrawn (goodbye packet sent).
    MdnsAdvertisementWithdrawn { service_name: String },
    /// A hostname conflict was detected and a deterministic fallback hostname was assigned.
    MdnsHostnameConflict {
        requested_hostname: String,
        assigned_hostname: String,
    },
    /// Network interfaces available for mDNS advertisement changed.
    MdnsInterfaceChanged { interface_count: usize },
    /// An error or degradation occurred in the mDNS subsystem.
    MdnsError { message: String },
    /// Browser initiated a local pairing challenge.
    BrowserPairingStarted {
        challenge_id: String,
        client_info: String,
    },
    /// Browser completed human-verified pairing challenge.
    BrowserPairingVerified {
        challenge_id: String,
        session_id: String,
    },
    /// Browser pairing challenge was rejected or failed verification.
    BrowserPairingRejected {
        challenge_id: String,
        reason: String,
    },
    /// Authenticated browser session was established.
    BrowserSessionCreated { session_id: String },
    /// Authenticated browser session was explicitly revoked.
    BrowserSessionRevoked { session_id: String },
    /// Authenticated browser session expired due to TTL timeout.
    BrowserSessionExpired { session_id: String },
    /// Browser request denied due to missing or insufficient capability.
    BrowserAuthorizationDenied {
        session_id: Option<String>,
        path: String,
        reason: String,
    },
}

static DUMMY_QUERY_INSTANCE: std::sync::LazyLock<ServiceInstanceId> =
    std::sync::LazyLock::new(|| {
        ServiceInstanceId::new("00000000-0000-0000-0000-000000000000").unwrap()
    });

static DUMMY_MDNS_SERVICE_ID: std::sync::LazyLock<ServiceId> =
    std::sync::LazyLock::new(|| ServiceId::new("fieldnet.mdns").unwrap());

static DUMMY_BROWSER_SERVICE_ID: std::sync::LazyLock<ServiceId> =
    std::sync::LazyLock::new(|| ServiceId::new("fieldnet.browser").unwrap());

impl ServiceEvent {
    /// Returns the dot-separated topic string for event routing (e.g. "service.registered").
    pub fn event_name(&self) -> &'static str {
        match self {
            Self::Registered { .. } => "service.registered",
            Self::Updated { .. } => "service.updated",
            Self::Authenticated { .. } => "service.authenticated",
            Self::Available { .. } => "service.available",
            Self::Degraded { .. } => "service.degraded",
            Self::Unavailable { .. } => "service.unavailable",
            Self::Expired { .. } => "service.expired",
            Self::Removed { .. } => "service.removed",
            Self::Resolved { .. } => "service.resolved",
            Self::Discovered { .. } => "service.discovered",
            Self::Withdrawn { .. } => "service.withdrawn",
            Self::AdvertisementSent { .. } => "service.advertisement.sent",
            Self::AdvertisementReceived { .. } => "service.advertisement.received",
            Self::AdvertisementForwarded { .. } => "service.advertisement.forwarded",
            Self::AdvertisementRejected { .. } => "service.advertisement.rejected",
            Self::QuerySent { .. } => "service.query.sent",
            Self::QueryReceived { .. } => "service.query.received",
            Self::QueryResponseSent { .. } => "service.query.response",
            Self::QueryResponseReceived { .. } => "service.query.response.received",
            Self::MdnsStarted { .. } => "mdns.started",
            Self::MdnsStopped => "mdns.stopped",
            Self::MdnsAdvertisementStarted { .. } => "mdns.advertisement_started",
            Self::MdnsAdvertisementWithdrawn { .. } => "mdns.advertisement_withdrawn",
            Self::MdnsHostnameConflict { .. } => "mdns.hostname_conflict",
            Self::MdnsInterfaceChanged { .. } => "mdns.interface_changed",
            Self::MdnsError { .. } => "mdns.error",
            Self::BrowserPairingStarted { .. } => "browser.pairing_started",
            Self::BrowserPairingVerified { .. } => "browser.pairing_verified",
            Self::BrowserPairingRejected { .. } => "browser.pairing_rejected",
            Self::BrowserSessionCreated { .. } => "browser.session_created",
            Self::BrowserSessionRevoked { .. } => "browser.session_revoked",
            Self::BrowserSessionExpired { .. } => "browser.session_expired",
            Self::BrowserAuthorizationDenied { .. } => "browser.authorization_denied",
        }
    }

    /// Returns the associated `ServiceId`.
    pub fn service_id(&self) -> &ServiceId {
        match self {
            Self::Registered { service_id, .. } => service_id,
            Self::Updated { service_id, .. } => service_id,
            Self::Authenticated { service_id, .. } => service_id,
            Self::Available { service_id, .. } => service_id,
            Self::Degraded { service_id, .. } => service_id,
            Self::Unavailable { service_id, .. } => service_id,
            Self::Expired { service_id, .. } => service_id,
            Self::Removed { service_id, .. } => service_id,
            Self::Resolved { service_id, .. } => service_id,
            Self::Discovered { service_id, .. } => service_id,
            Self::Withdrawn { service_id, .. } => service_id,
            Self::AdvertisementSent { service_id, .. } => service_id,
            Self::AdvertisementReceived { service_id, .. } => service_id,
            Self::AdvertisementForwarded { service_id, .. } => service_id,
            Self::AdvertisementRejected { service_id, .. } => service_id,
            Self::QuerySent { service_id, .. } => service_id,
            Self::QueryReceived { service_id, .. } => service_id,
            Self::QueryResponseSent { service_id, .. } => service_id,
            Self::QueryResponseReceived { service_id, .. } => service_id,
            Self::MdnsStarted { .. }
            | Self::MdnsStopped
            | Self::MdnsAdvertisementStarted { .. }
            | Self::MdnsAdvertisementWithdrawn { .. }
            | Self::MdnsHostnameConflict { .. }
            | Self::MdnsInterfaceChanged { .. }
            | Self::MdnsError { .. } => &DUMMY_MDNS_SERVICE_ID,
            Self::BrowserPairingStarted { .. }
            | Self::BrowserPairingVerified { .. }
            | Self::BrowserPairingRejected { .. }
            | Self::BrowserSessionCreated { .. }
            | Self::BrowserSessionRevoked { .. }
            | Self::BrowserSessionExpired { .. }
            | Self::BrowserAuthorizationDenied { .. } => &DUMMY_BROWSER_SERVICE_ID,
        }
    }

    /// Returns the associated `ServiceInstanceId`.
    pub fn instance_id(&self) -> &ServiceInstanceId {
        match self {
            Self::Registered { instance_id, .. } => instance_id,
            Self::Updated { instance_id, .. } => instance_id,
            Self::Authenticated { instance_id, .. } => instance_id,
            Self::Available { instance_id, .. } => instance_id,
            Self::Degraded { instance_id, .. } => instance_id,
            Self::Unavailable { instance_id, .. } => instance_id,
            Self::Expired { instance_id, .. } => instance_id,
            Self::Removed { instance_id, .. } => instance_id,
            Self::Resolved { instance_id, .. } => instance_id,
            Self::Discovered { instance_id, .. } => instance_id,
            Self::Withdrawn { instance_id, .. } => instance_id,
            Self::AdvertisementSent { instance_id, .. } => instance_id,
            Self::AdvertisementReceived { instance_id, .. } => instance_id,
            Self::AdvertisementForwarded { instance_id, .. } => instance_id,
            Self::AdvertisementRejected { instance_id, .. } => instance_id,
            Self::QuerySent { .. }
            | Self::QueryReceived { .. }
            | Self::QueryResponseSent { .. }
            | Self::QueryResponseReceived { .. }
            | Self::MdnsStarted { .. }
            | Self::MdnsStopped
            | Self::MdnsAdvertisementStarted { .. }
            | Self::MdnsAdvertisementWithdrawn { .. }
            | Self::MdnsHostnameConflict { .. }
            | Self::MdnsInterfaceChanged { .. }
            | Self::MdnsError { .. }
            | Self::BrowserPairingStarted { .. }
            | Self::BrowserPairingVerified { .. }
            | Self::BrowserPairingRejected { .. }
            | Self::BrowserSessionCreated { .. }
            | Self::BrowserSessionRevoked { .. }
            | Self::BrowserSessionExpired { .. }
            | Self::BrowserAuthorizationDenied { .. } => &DUMMY_QUERY_INSTANCE,
        }
    }
}
