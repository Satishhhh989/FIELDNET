# FIELDNET Phase 08 — Architecture Specification
**Document ID:** `PHASE-08-03-FINAL-INTEGRATION-ARCHITECTURE`  
**Title:** Service Discovery & Local Web Final Integration Architecture, APIs, State Machines & Implementation Plan  
**Status:** SPECIFICATION APPROVED & IMPLEMENTATION READY  
**Authoritative Reference:** `FIELDNET_MASTER_SPEC.md` (Sections 3.3, 18, 19, 28, 29, 30)  
**Foundation Subsystems:**  
- Phase 01: System Model & Core Principles  
- Phase 02: Runtime Core Architecture  
- Phase 03: Identity, Security & Trust (`fieldnet-identity`, `fieldnet-core`)  
- Phase 04: Peer Discovery & Multi-Transport Networking (`fieldnet-network`)  
- Phase 05: Network Protocol, Routing & DTN (`fieldnet-protocol`, `fieldnet-runtime::routing`)  
- Phase 06: Local-First Data & Synchronization (`fieldnet-data`)  
- Phase 07: Distributed File & Content System (`fieldnet-content`)  
- Phase 08-01: Service Model & Registry Architecture (`docs/architecture/PHASE-08-01-SERVICE-MODEL-AND-REGISTRY.md`)  
- Phase 08-02: Local Web & Gateway Architecture (`docs/architecture/PHASE-08-02-LOCAL-WEB-AND-GATEWAY.md`)

---

## 1. Executive Summary & Integration Objective

FIELDNET Phase 08 unifies two essential capabilities into one seamless, production-ready subsystem:
1. **Service Discovery:** A decentralized, provider-independent, capability-based service mesh where applications request logical services (`search.service`, `maps.service`, `files.service`, `ai.service`) rather than physical nodes or static IP sockets.
2. **Local Web Gateway:** A lightweight, non-invasive, browser-facing HTTP/WebSocket access layer exposing the local computing environment via standard `fieldnet.local` (RFC 6762 / 6763 mDNS/DNS-SD) without requiring custom browser plugins, proprietary extensions, or pre-shared certificate authorities.

This document synthesizes Prompt 01 and Prompt 02 into a single, concrete, implementation-ready architectural contract that bridges:
```text
Browser Client ──> Local Web Gateway ──> Service Resolver ──> Service Registry ──> Mesh Runtime
```
while strictly reusing existing cryptographic, transport, routing, local data, and content primitives from Phases 01–07 without duplicate implementations or architectural drift.

---

## 2. Final Component Graph & Runtime Data Flow

### 2.1 Component Architecture Graph

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                   FIELDNET RUNTIME CORE                                          │
│                                                                                                  │
│  ┌─────────────────────────┐  ┌─────────────────────────┐  ┌──────────────────────────────────┐  │
│  │   Phase 03: Identity    │  │   Phase 05: Routing     │  │   Phase 07: Content Store        │  │
│  │   (NodeIdentity, Ed25519│  │   (RoutingTable, DTN,   │  │   (ContentStore, ChunkStore,     │  │
│  │    CapabilityTokens)    │  │    Distance-Vector)     │  │    TransferManager, CAS)         │  │
│  └────────────┬────────────┘  └────────────┬────────────┘  └────────────────┬─────────────────┘  │
│               │                            │                                │                    │
│               │                            │                                │                    │
│  ┌────────────▼────────────────────────────▼────────────────────────────────▼─────────────────┐  │
│  │                                PHASE 08: SERVICE SUBSYSTEM                                 │  │
│  │                                                                                            │  │
│  │  ┌───────────────────────┐   ┌───────────────────────┐   ┌──────────────────────────────┐  │  │
│  │  │   Service Registry    │◄──┤  Service Advertiser   ├──►│   Service Health Tracker     │  │  │
│  │  │ (Local, Mesh, SQLite) │   │ (Multi-hop wire sync) │   │ (Passive telemetry & RTT)    │  │  │
│  │  └───────────▲───────────┘   └───────────────────────┘   └──────────────┬───────────────┘  │  │
│  │              │                                                          │                  │  │
│  │              └───────────────────────────┬──────────────────────────────┘                  │  │
│  │                                          │                                                 │  │
│  │                              ┌───────────▼───────────┐                                     │  │
│  │                              │   Service Resolver    │                                     │  │
│  │                              │(8-Step Ranking Engine)│                                     │  │
│  │                              └───────────▲───────────┘                                     │  │
│  │                                          │                                                 │  │
│  │  ┌───────────────────────────────────────┴──────────────────────────────────────────────┐  │  │
│  │  │                           LOCAL WEB GATEWAY SUBSYSTEM                                │  │  │
│  │  │  ┌──────────────────────┐  ┌──────────────────────┐  ┌────────────────────────────┐  │  │  │
│  │  │  │   mDNS Responder     │  │  Host & CORS Filter  │  │   Browser Session Store    │  │  │  │
│  │  │  │ (RFC 6762 / 5353)    │  │ (Rebinding & CSRF)   │  │ (PIN Pairing & Cookies)    │  │  │  │
│  │  │  └──────────────────────┘  └──────────┬───────────┘  └─────────────┬──────────────┘  │  │  │
│  │  │                                       │                            │                 │  │  │
│  │  │  ┌────────────────────────────────────▼────────────────────────────▼──────────────┐  │  │  │
│  │  │  │                          Embedded HTTP / WebSocket Router                      │  │  │  │
│  │  │  │  /                       ──> Static Dashboard & UI Console                     │  │  │  │
│  │  │  │  /api/v1/services        ──> Browser-Safe Redacted Service Catalog             │  │  │  │
│  │  │  │  /api/v1/content/<id>    ──> Phase 07 CAS Zero-Copy Streaming (HTTP 200/206)   │  │  │  │
│  │  │  │  /services/<service>/*   ──> Reverse Proxy to Resolved Service Instance        │  │  │  │
│  │  │  │  /ws/services/<service>  ──> Full-Duplex WebSocket Streaming Bridge            │  │  │  │
│  │  │  └────────────────────────────────────────────────────────────────────────────────┘  │  │  │
│  │  └───────────────────────────────────────▲──────────────────────────────────────────────┘  │  │
│  └──────────────────────────────────────────┼─────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────┼────────────────────────────────────────────────────┘
                                              │ HTTP / WebSocket (TCP Port 8080)
                                              ▼
                        ┌───────────────────────────────────────────┐
                        │      Standard Web Browser Client          │
                        │    Safari / Chrome / Firefox / Edge       │
                        │        http://fieldnet.local:8080         │
                        └───────────────────────────────────────────┘
```

### 2.2 End-to-End Request Data Flow

```
1. Discovery Flow:
   Browser ──(mDNS Query: fieldnet.local)──> mDNS Responder ──(mDNS Reply: IP)──> Browser

2. HTTP Service Request Flow:
   Browser ──(GET /services/maps/tile/1/2.png)──> Gateway Router
     ├── Validates Host Header & Origin
     ├── Extracts Browser Session Cookie (Auth Check)
     ├── Calls ServiceResolver::resolve("maps.service", constraints)
     │     ├── Registry filters valid instances
     │     ├── Evaluates HealthTracker telemetry & RoutingTable hops
     │     └── Returns optimal ServiceInstance(maps.service@NodeB)
     ├── Routes to Provider:
     │     ├── If Local: Direct async in-process call / loopback socket
     │     └── If Remote: Encapsulates into MessageType::Service=8 overlay envelope
     │           └── Dispatched via RoutingTable over TCP AEAD transport to NodeB
     ├── Provider executes and returns ServiceRpcResponse
     └── Gateway streams chunked HTTP response (200 OK) back to Browser

3. Static Content Flow (Phase 07):
   Browser ──(GET /api/v1/content/4a5b6c...)──> Gateway Content Handler
     ├── Checks ContentStore for ContentId(4a5b6c...)
     │     ├── If missing: Calls TransferManager to fetch from swarm
     │     └── If present: Opens async zero-copy stream from CAS
     └── Responds with ETag, Cache-Control: immutable, and Range support (HTTP 206)
```

---

## 3. Canonical Data Models (Single Authoritative Definitions)

All Phase 08 data structures reside in `crates/fieldnet-service/src/model/` with zero duplication.

```rust
use fieldnet_content::ContentId;
use fieldnet_core::{NodeId, Timestamp};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

pub const CURRENT_SERVICE_MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Logical interface capability identifier (e.g. "fieldnet.search", "ai.inference").
/// Enforces lowercase alphanumeric and dot-separated namespace: [a-z0-9_-]+(\.[a-z0-9_-]+)+
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct ServiceId(pub String);

/// Unique deployment instance identifier distinguishing an execution on a specific node.
/// 32-character hexadecimal string.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct ServiceInstanceId(pub String);

/// Semantic versioning representation conforming to SemVer 2.0.0.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub struct ServiceVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

/// Reachability endpoint specification for a service instance.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum ServiceEndpoint {
    /// Multi-hop internal FIELDNET overlay RPC routed to provider_node_id.
    InternalRpc,
    /// Local TCP listening port on provider node (loopback or LAN).
    LocalPort(u16),
    /// Local Unix domain socket path.
    LocalSocket(String),
    /// Gateway web mount path (e.g. "/services/search").
    GatewayPath(String),
}

/// Service wire protocol dialect.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum ServiceProtocol {
    NativeFieldnetRpc = 1,
    Http1 = 2,
    WebSocket = 3,
    ContentStream = 4,
}

/// Access control and authorization constraints.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize, Default)]
pub struct ServicePermissions {
    pub is_public: bool,
    pub required_roles: Vec<String>,
    pub requires_capability_token: bool,
}

/// Authoritative cryptographically signed service declaration.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ServiceManifest {
    pub schema_version: u32,
    pub service_id: ServiceId,
    pub instance_id: ServiceInstanceId,
    pub provider_node_id: NodeId,
    #[serde(with = "hex")]
    pub provider_public_key: [u8; 32],
    pub version: ServiceVersion,
    pub protocol: ServiceProtocol,
    pub endpoint: ServiceEndpoint,
    pub capabilities: Vec<String>,
    pub permissions: ServicePermissions,
    pub sequence: u64,
    pub ttl_seconds: u32,
    pub created_at: Timestamp,
    pub expires_at: Timestamp,
    pub display_name: String,
    pub description: String,
    pub icon_content_id: Option<ContentId>,
    pub documentation_content_id: Option<ContentId>,
    #[serde(with = "hex")]
    pub signature: [u8; 64],
}

/// Authoritative lifecycle state of a service instance.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum ServiceState {
    Discovered,
    Authenticated,
    Available,
    Degraded,
    Unavailable,
    Expired,
    Removed,
}

/// Real-time health, performance, and reliability telemetry for an instance.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ServiceHealth {
    pub state: ServiceState,
    pub consecutive_failures: u32,
    pub last_success_at: Option<Timestamp>,
    pub last_failure_at: Option<Timestamp>,
    pub observed_rtt_ms: u32,
    pub moving_avg_latency_ms: u32,
    pub reliability_permille: u32, // 0 - 1000 (1000 = 100% success)
}

/// Full registry record combining manifest with dynamic state.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ServiceRecord {
    pub manifest: ServiceManifest,
    pub health: ServiceHealth,
    pub is_local: bool,
    pub last_refreshed_at: Timestamp,
}

/// Query constraints passed to the ServiceResolver.
#[derive(Clone, Debug, Default)]
pub struct ServiceConstraint {
    pub version_requirement: Option<String>, // e.g. "^1.2.0"
    pub required_capabilities: Vec<String>,
    pub preferred_protocol: Option<ServiceProtocol>,
    pub locality_preference: LocalityPreference,
    pub max_acceptable_rtt_ms: Option<u32>,
    pub required_trust_tier: TrustTier,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum LocalityPreference {
    #[default]
    PreferLocal,
    LocalOnly,
    PreferRemote,
    Any,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub enum TrustTier {
    #[default]
    Public = 0,
    KnownPeer = 1,
    AuthorizedRole = 2,
    CreatorOnly = 3,
}

/// The deterministic, explainable outcome of service resolution.
#[derive(Clone, Debug)]
pub struct ServiceResolution {
    pub record: ServiceRecord,
    pub explanation: ResolutionExplanation,
}

#[derive(Clone, Debug)]
pub struct ResolutionExplanation {
    pub candidate_count: usize,
    pub selected_instance_id: ServiceInstanceId,
    pub selection_score: u64,
    pub selection_reasons: Vec<String>,
}

/// 256-bit high-entropy browser session identity.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct BrowserSessionId(pub String);

/// Active authenticated browser session.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrowserSession {
    pub session_id: BrowserSessionId,
    pub client_addr: SocketAddr,
    pub user_agent: String,
    pub granted_roles: Vec<String>,
    pub created_at: Timestamp,
    pub expires_at: Timestamp,
    pub is_localhost: bool,
}

/// Normalized internal HTTP request abstraction.
#[derive(Clone, Debug)]
pub struct GatewayRequest {
    pub request_id: String,
    pub correlation_id: String,
    pub method: String,
    pub path: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
    pub session: Option<BrowserSession>,
}

/// Normalized internal HTTP response abstraction.
#[derive(Clone, Debug)]
pub struct GatewayResponse {
    pub status_code: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}
```

---

## 4. Service Manifest Schema & Cryptographic Signing

The `ServiceManifest` is the authoritative, self-contained, tamper-proof document announcing a service capability.

### 4.1 Canonical Signing Bytes Specification

Signatures are computed strictly over deterministic canonical bytes using the provider's Ed25519 signing key. The canonical layout guarantees zero ambiguity, signature malleability protection, and endian-safe verification:

```text
FIELDNET_SERVICE_MANIFEST_V1_CANONICAL:
  [0..4]   : Magic Header & Schema Version: "FSM1" (0x46, 0x53, 0x4D, 0x01)
  [4..6]   : service_id_len (u16, Big-Endian)
  [..]     : service_id UTF-8 bytes
  [..+2]   : instance_id_len (u16, Big-Endian)
  [..]     : instance_id UTF-8 bytes
  [..+32]  : provider_node_id (32 raw bytes)
  [..+4]   : version_major (u32, BE)
  [..+4]   : version_minor (u32, BE)
  [..+4]   : version_patch (u32, BE)
  [..+1]   : protocol_tag (u8: 1=Native, 2=Http, 3=WebSocket, 4=ContentStream)
  [..+1]   : endpoint_tag (u8: 1=InternalRpc, 2=LocalPort, 3=LocalSocket, 4=GatewayPath)
  [..+var] : endpoint_payload (canonical port or path bytes)
  [..+8]   : sequence (u64, Big-Endian)
  [..+4]   : ttl_seconds (u32, Big-Endian)
  [..+8]   : created_at (u64, Big-Endian milliseconds)
  [..+8]   : expires_at (u64, Big-Endian milliseconds)
```

### 4.2 Manifest Invariant Rules

1. **What is Signed:** All core metadata, identity, sequence, endpoints, capabilities, and expiration timestamps. Display titles and icon content IDs are included in signed data to prevent UI phishing.
2. **What is Trusted:** An authenticated manifest proves that the owning Ed25519 key authorized this instance. It does *not* prove that the service is healthy or non-malicious (handled by the Health Tracker and Trust Tier filters).
3. **Manifest Changes & Monotonicity:** If a provider updates capabilities, ports, or metadata, it publishes a new manifest with `sequence = old_sequence + 1`. The registry strictly rejects incoming manifests where `sequence <= current_record.sequence`.
4. **Revocation & Invalidation:** A provider revokes a service immediately by broadcasting an explicit tombstone announcement (`ttl_seconds = 0`, `sequence = current + 1`), causing all nodes to immediately purge the record. If a provider crashes silently, the manifest expires automatically at `expires_at = created_at + ttl_seconds`.

---

## 5. Service Registry Architecture & Persistence Model

The `ServiceRegistry` is the central catalog of all known local and mesh services.

### 5.1 Registry State Partitioning

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                 ServiceRegistry                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│ PERSISTENT STATE (SQLite WAL: services.db):                                    │
│  - local_manifests: All services declared and hosted on this node.              │
│  - cached_manifests: Cryptographically valid remote manifests (TTL < 24h).      │
│  - service_acls: Sovereign authorization policies and capability token grants.  │
├─────────────────────────────────────────────────────────────────────────────────┤
│ EPHEMERAL STATE (In-Memory Concurrent Locks):                                   │
│  - instances_by_service: HashMap<ServiceId, HashMap<ServiceInstanceId, Record>>│
│  - instances_by_provider: HashMap<NodeId, HashSet<ServiceInstanceId>>          │
│  - health_table: HashMap<ServiceInstanceId, ServiceHealth>                      │
│  - rtt_moving_averages: HashMap<ServiceInstanceId, MovingAverage>               │
│  - active_in_flight_rpc: HashMap<ServiceInstanceId, AtomicUsize>                │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Registry Event Handlers

| Lifecycle Trigger | Registry Action | Persistence Impact | Mesh Notification |
| :--- | :--- | :--- | :--- |
| **Local Registration** | Validates schema, signs manifest, inserts into memory & SQLite. | Written to `local_manifests` | Emits `ServiceAdvertise` broadcast |
| **Mesh Advertisement**| Verifies Ed25519 signature & sequence. If valid, inserts/updates record. | Written to `cached_manifests` | Emits `ServiceEvent::Registered` |
| **Heartbeat Refresh** | Updates `last_refreshed_at` and extends `expires_at`. | In-memory timestamp updated | None (piggybacked) |
| **TTL Sweep (every 5s)**| Purges records where `Timestamp::now() > expires_at`. | Removed from SQLite cache | Emits `ServiceEvent::Expired` |
| **Peer Disconnection**| Intercepts `ConnectionEvent::PeerDisconnected`. Marks instances `Unavailable`.| In-memory state updated | Emits `ServiceEvent::Unavailable` |
| **Peer Reconnection** | When route restored, re-verifies cached manifest and restores `Available`. | Restored to active | Emits `ServiceEvent::Available` |
| **Registry Restart** | On process boot, reloads `local_manifests` and unexpired `cached_manifests`. | WAL replay verification | Re-announces local services |
| **Database Corruption**| If SQLite fails integrity check, moves DB to `.corrupt`, starts clean DB. | Fresh `services.db` | Re-registers local services |

---

## 6. Deterministic Service Resolution Engine

The `ServiceResolver` translates a high-level `(ServiceId, ServiceConstraint)` query into a single optimal `ServiceResolution`.

### 6.1 Deterministic Precedence Gates

To guarantee predictable, reproducible, and explainable results across all nodes without opaque heuristics:

```
[All Registered Instances for ServiceId]
                    ↓
[Gate 1: Schema & Expiry Check]
   - Verify now < record.manifest.expires_at
                    ↓
[Gate 2: State Validity Check]
   - Filter state ∈ {Available, Degraded} (Rejects Unavailable, Expired, Removed)
                    ↓
[Gate 3: Version Compatibility Check]
   - Evaluate SemVer constraint: VersionReq::matches(&manifest.version)
                    ↓
[Gate 4: Capability Match]
   - Enforce: manifest.capabilities ⊇ constraint.required_capabilities
                    ↓
[Gate 5: Authorization Pre-Check]
   - Caller identity satisfies manifest.permissions (role check & capability token)
                    ↓
[Gate 6: Route Reachability Check]
   - Provider is local OR RoutingTable has valid next_hop for provider_node_id
                    ↓
[Gate 7: Locality Constraint Filter]
   - Enforce LocalityPreference (e.g. LocalOnly rejects remote candidates)
                    ↓
[Deterministic 5-Tier Scoring & Explanation Engine]
```

### 6.2 Deterministic Scoring Function & Explainability

For all candidates passing Gates 1–7, a composite score $S \in [0, 100000]$ is computed:
$$S = W_{\text{locality}} + W_{\text{state}} + W_{\text{reliability}} + W_{\text{latency}} + W_{\text{hops}}$$

Where:
- $W_{\text{locality}} = 50,000$ if provider is local; $0$ if remote.
- $W_{\text{state}} = 25,000$ if `Available`; $10,000$ if `Degraded`.
- $W_{\text{reliability}} = \text{reliability\_permille} \times 15$ (max $15,000$).
- $W_{\text{latency}} = \max(0, 5,000 - (\text{moving\_avg\_latency\_ms} \times 10))$.
- $W_{\text{hops}} = \max(0, 5,000 - (\text{route\_hops} \times 1,000))$.

**Tie-Breaking:** If two candidates produce identical composite scores, the candidate with the lexicographically smallest `ServiceInstanceId` is chosen.

**Explainability:** The resolver outputs a typed `ResolutionExplanation` detailing:
- Candidates evaluated vs. candidates rejected at each gate.
- Criteria satisfied: `"authenticated"`, `"authorized"`, `"compatible_version(1.2.0)"`, `"reachable(1_hop)"`, `"healthy(100%_reliability)"`.

---

## 7. Service-to-Network Integration Boundary

The service layer **NEVER** directly mutates routing tables or sends raw TCP packets.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ SERVICE SUBSYSTEM                                                               │
│   ServiceId: "fieldnet.search"                                                  │
│       │                                                                         │
│       ▼ ServiceResolver::resolve()                                              │
│   ServiceInstanceId: "inst_a7f8e32"                                             │
│       │                                                                         │
│       ▼ Manifest lookup                                                         │
│   ProviderNodeId: NodeId(e16ef3ec7c...)                                         │
└───────┬─────────────────────────────────────────────────────────────────────────┘
        │ Strict API Boundary: NetworkFacadeApi
        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ RUNTIME & NETWORKING SUBSYSTEMS (Phases 04 & 05)                                │
│   RoutingTable::get_route(ProviderNodeId) ──> RouteInfo { next_hop, hops, metric}│
│   ConnectionManager::get_active_session(next_hop) ──> TransportConnection       │
│   Message::sign_and_build(MessageType::Service = 8, payload, ...)               │
│   ChaCha20-Poly1305 AEAD Session Encrypt ──> Direct TCP Socket                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

The service layer communicates with the network exclusively through a defined facade:
```rust
#[async_trait]
pub trait ServiceNetworkBridge: Send + Sync {
    /// Checks route reachability and returns current metric and hop count.
    async fn check_route(&self, node_id: &NodeId) -> Option<RouteMetric>;

    /// Dispatches an internal service RPC message across the overlay mesh.
    async fn dispatch_service_rpc(
        &self,
        target_node: NodeId,
        request: ServiceRpcEnvelope,
    ) -> Result<ServiceRpcEnvelope, ServiceError>;
}
```

---

## 8. Local Web to Service Gateway Integration

When a browser client navigates to a URL on `fieldnet.local:8080`, the gateway maps the URL into an internal service invocation:

```
Browser Request:
  GET http://fieldnet.local:8080/services/maps/tiles/14/2/3.png HTTP/1.1
  Host: fieldnet.local:8080
  Cookie: fieldnet_session=s_9a8b7c...

Gateway Translation Pipeline:
  1. Host & CORS Filter: Host matches allowed list ("fieldnet.local:8080").
  2. Browser Session: Extracts cookie, validates in BrowserSessionStore.
  3. Path Parse: Prefix is "/services/maps". ServiceId = "fieldnet.maps".
  4. Service Resolution:
     resolver.resolve("fieldnet.maps", constraints)
     ──> Selected: maps.service@NodeB (Remote, 1 hop away)
  5. Authorization Check:
     Session has role "user"; Service allows public/user read. Passed.
  6. Subpath Rewriting:
     Target RPC subpath = "/tiles/14/2/3.png".
  7. Dispatch:
     Gateway packs HTTP headers, subpath, and query params into ServiceRpcRequest.
     Dispatches across overlay network to NodeB.
  8. Streaming Response:
     NodeB executes local tile renderer, returns binary PNG stream.
     Gateway streams binary bytes to browser as HTTP 200 OK (image/png).
```

---

## 9. Browser-Facing HTTP API Specification

The Local Web Gateway exposes a strictly bounded, human- and browser-accessible HTTP interface:

| Endpoint | Method | Auth Required | Description | Status Codes |
| :--- | :--- | :--- | :--- | :--- |
| `/` | `GET` | No | Embedded HTML/JS Management Dashboard & Console | 200 |
| `/api/v1/status` | `GET` | No | Node status, mesh peer count, gateway version | 200 |
| `/api/v1/services` | `GET` | Optional | Sanitized browser-safe service catalog | 200 |
| `/api/v1/services/{id}` | `GET` | Optional | Detailed metadata for specific service | 200, 404 |
| `/api/v1/content/{id}` | `GET` | Optional | Direct Phase 07 CAS content streaming | 200, 206, 304, 404 |
| `/api/v1/session/pair` | `POST` | No | Terminal PIN exchange to mint browser session | 200, 401 |
| `/services/{service}/*` | `ANY` | Yes/Policy | Transparent reverse proxy to resolved service | 200, 400, 403, 502, 503, 504 |

### 9.1 RFC 7807 Problem Details Error Representation

All error responses from the gateway return standard `application/problem+json`:
```json
{
  "type": "https://fieldnet.local/errors/service-unavailable",
  "title": "Service Unavailable",
  "status": 503,
  "detail": "No reachable providers available for service: fieldnet.ai",
  "instance": "/services/ai/inference",
  "request_id": "req_8f1a2b3c",
  "code": "SERVICE_NO_CANDIDATES"
}
```

---

## 10. WebSocket Streaming Architecture

The gateway bridges standard RFC 6455 browser WebSockets to internal service streams:

```
Browser (JavaScript WebSocket)
  ws://fieldnet.local:8080/ws/services/workspace.collab
       │
       ▼ (101 Switching Protocols, Sec-WebSocket-Key verified)
Local Web Gateway WebSocket Engine
       │
       ▼ Authenticates BrowserSession; binds to ServiceResolver::resolve("workspace.collab")
Downstream Service Session (Bounded Tokio Async MPSC Channel: 64 msgs / 256 KB)
```

### 10.1 WebSocket Wire Envelope Protocol

Frames exchanged over the WebSocket conform to standard JSON framing:
```json
{
  "protocol": "fieldnet.ws.v1",
  "request_id": "ws_req_101",
  "correlation_id": "corr_44901",
  "type": "DATA",
  "channel": "document_sync",
  "payload": {
    "action": "CRDT_DELTA",
    "document_id": "doc_medical_inventory",
    "delta": "base64..."
  }
}
```

### 10.2 Event Isolation Principle

The gateway **NEVER** connects browser WebSockets directly to the internal Runtime Event Bus. Instead, the downstream service filters and sanitizes application events, emitting only caller-authorized data frames.

---

## 11. Browser Session & Cryptographic Isolation

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ THE SOVEREIGN KEY ISOLATION PRINCIPLE:                                          │
│ - The node's 32-byte Ed25519 sovereign private key NEVER enters browser memory. │
│ - Web browsers hold only ephemeral, random 256-bit session tokens.              │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 11.1 Browser Pairing Protocol

1. **Terminal PIN Display:** When an unauthenticated browser connects, the node prints an out-of-band 6-digit PIN on the terminal console (e.g., `FIELDNET PAIRING PIN: 849-204`).
2. **PIN Submission:** Browser sends: `POST /api/v1/session/pair {"pin": "849204"}`.
3. **Session Issuance:** If valid within 120 seconds, gateway mints a `BrowserSession`, storing it in memory and setting a secure cookie:
   ```http
   Set-Cookie: fieldnet_session=b_9e8d7c6b5a4...; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400
   ```
4. **Localhost Exemption:** When configured with `auto_pair_localhost = true`, connections originating strictly from `127.0.0.1` or `[::1]` bypass PIN entry with standard local user capabilities.

---

## 12. mDNS / DNS-SD Architecture (RFC 6762 / 6763)

| Component | Specification | Description |
| :--- | :--- | :--- |
| **Multicast Group** | `224.0.0.251:5353` (IPv4), `[ff02::fb]:5353` (IPv6) | Standard mDNS link-local group |
| **Socket Configuration** | `SO_REUSEPORT` & `SO_REUSEADDR` | Allows co-existence with Avahi / Bonjour daemons |
| **PTR Records** | `_fieldnet._tcp.local.` & `_http._tcp.local.` | Discovers all FIELDNET nodes on the LAN |
| **SRV Record** | Target: `fieldnet.local.`, Port: `8080` (or dynamic) | Resolves port and hostname |
| **TXT Record** | `txtvers=1`, `node_id=<hex>`, `version=0.1.0` | Node metadata and capabilities |
| **A / AAAA Record** | `fieldnet.local.` $\to$ Node IP (`127.0.0.1` on loopback) | Hostname resolution for standard browsers |
| **Host Record TTL** | 10 seconds | Fast failover if gateway powers off |
| **Goodbye Packets** | Resource Records with `TTL = 0` | Broadcast on shutdown/suspend to clear caches |

---

## 13. `fieldnet.local` Authoritative Semantics

To resolve all ambiguity regarding the semantics of `fieldnet.local`:

1. **What does it resolve to?** It resolves to the IP address of the nearest active FIELDNET gateway node on the client's local physical link.
2. **Which node responds?** On the local host machine running a node, it resolves to `127.0.0.1`. Across a Wi-Fi/Ethernet LAN, the node that wins RFC 6762 probe conflict resolution answers for `fieldnet.local`.
3. **What if multiple nodes advertise it?** Every node advertises `fieldnet.local` as an alias and `fieldnet-<short_id>.local` as its canonical unique name. The node with lexicographically lower `NodeId` claims `fieldnet.local`. Because all gateways route across the mesh, **connecting to any gateway yields identical access to all mesh services**.
4. **What if the active gateway crashes?** The 10-second A-record TTL expires in browser/OS caches. The next active gateway claims `fieldnet.local`, and client-side JavaScript auto-reconnects.
5. **What happens during network partitions?** Each partition has its own gateway answering `fieldnet.local` for its isolated subnet.
6. **What happens when partitions merge?** Routing tables and service registries sync distance vectors and manifests; service catalogs expand automatically.
7. **What does the browser actually connect to?** Standard TCP socket to port 8080 on the resolved gateway IP.

---

## 14. Phase 07 Content System Integration

Static assets and large media files are served directly through Phase 07 Content-Addressed Storage:

```
Browser Request: GET /api/v1/content/a7f8e32... HTTP/1.1
Gateway Content Handler:
  ├── Small Assets (< 256 KB): Read from ContentStore, returned immediately with ETag.
  ├── Large Media (> 256 KB): Streamed chunk-by-chunk using Tokio AsyncRead in 64 KB buffers.
  │     Memory per connection bounded to <= 128 KB.
  ├── HTTP Range Requests: Maps byte offsets to Phase 07 256 KB ChunkStore boundaries.
  │     Returns HTTP 206 Partial Content with Content-Range header.
  └── Missing Content: Triggers Phase 07 TransferManager to fetch from swarm peers.
```

---

## 15. Security Model & Enforceable Invariants

### 15.1 Threat Matrix Analysis

| Threat | Attack Surface | Mitigation | Verification Test |
| :--- | :--- | :--- | :--- |
| **DNS Rebinding** | Malicious web page sends requests with `Host: evil.com`. | Gateway inspects `Host` header on EVERY request; rejects non-whitelisted hosts with 400. | Send HTTP request with `Host: attacker.com`; verify immediate 400 Bad Request. |
| **CSRF Attacks** | Evil site triggers cross-site POST to gateway. | `SameSite=Strict` cookies + mandatory `X-Fieldnet-Client: web-gateway` header on all POST/PUT/DELETE. | Cross-origin POST without custom header; verify 403 Forbidden. |
| **WebSocket Hijack** | Malicious site opens WebSocket to gateway. | Gateway verifies `Origin` header matches allowed hostnames during handshake. | WS handshake with `Origin: http://evil.com`; verify 403 Forbidden. |
| **Key Extraction** | Malicious script attempts to read private key. | Node Ed25519 private key is never in browser; browser only holds ephemeral session token. | Script inspects LocalStorage/DOM; verify zero cryptographic secrets present. |
| **Slowloris DoS** | Attacker opens hundreds of idle TCP sockets. | Connection limit (512 max) + 5-second header read timeout. | Open connection and hold idle; verify gateway drops socket after 5s. |

### 15.2 Enforceable Security Invariants (Phase 08)

1. *Service identity is independent of provider node identity.*
2. *Unauthenticated service advertisements are never trusted.*
3. *Unauthorized service access is rejected.*
4. *Service resolution never bypasses FIELDNET routing.*
5. *Local Web never becomes a generic arbitrary-network proxy.*
6. *Browser clients never receive node private keys.*
7. *Expired service instances are not resolved.*
8. *Provider disappearance does not corrupt the registry.*
9. *Gateway failure does not corrupt FIELDNET state.*
10. *Large content is streamed rather than loaded entirely into memory.*
11. *Content integrity is verified through Phase 07.*
12. *Local Web remains optional to the runtime.*
13. *No internet connection is required for local service access.*
14. *Multiple service providers can coexist.*
15. *Service provider replacement does not require application-level topology knowledge.*
16. *Internal FIELDNET service communication does not require HTTP.*

---

## 16. Authoritative State Machines

### 16.1 Service Instance Lifecycle State Machine

```
              ┌───────────────────────────┐
              │        DISCOVERED         │ (Manifest ingested from wire)
              └─────────────┬─────────────┘
                            │ Ed25519 Signature Verified OK
                            ▼
              ┌───────────────────────────┐
              │       AUTHENTICATED       │
              └─────────────┬─────────────┘
                            │ Route verified in RoutingTable
                            ▼
    ┌───────────────────────────────────────────────┐
    │                   AVAILABLE                   │ ◄───┐
    └───────┬───────────────────────────────┬───────┘     │ RTT Restored
            │ Transient RPC Failure         │ Route Lost  │ or Re-probed
            ▼                               │             │
    ┌───────────────┐                       │             │
    │   DEGRADED    │ ──────────────────────┤             │
    └───────┬───────┘                       │             │
            │ 3 Consecutive Failures        ▼             │
            └───────────────────────> ┌───────────┐       │
                                      │UNAVAILABLE│ ──────┘
                                      └─────┬─────┘
                                            │ TTL Expired (expires_at < now)
                                            ▼
                                      ┌───────────┐
                                      │  EXPIRED  │ ──> [GC Sweep] ──> [REMOVED]
                                      └───────────┘
```

### 16.2 HTTP Request Lifecycle State Machine

```
[Incoming TCP] ──> [Host & Origin Check]
                         ├─ Fail ──> [HTTP 400 / 403] ──> [Connection Terminated]
                         ▼ Pass
                   [Session Auth Check]
                         ├─ Fail ──> [HTTP 401 Unauthorized] ──> [Connection Terminated]
                         ▼ Pass
                   [Route Match & Service Resolution]
                         ├─ No Candidate ──> [HTTP 503 Unavailable]
                         ▼ Candidate Found
                   [Safety & Idempotency Classification]
                         ├── Safe (GET/HEAD): Transparent retry allowed on provider drop
                         └── Unsafe (POST/PUT): Strict no-blind-retry unless Idempotency-Key present
                         ▼
                   [Dispatch to Provider]
                         ├─ Provider Crash (Safe GET) ──> [Reroute to Next Best Candidate]
                         ├─ Provider Crash (Unsafe POST) ──> [HTTP 504 Timeout / Execution Indeterminate]
                         ▼ Success
                   [Streaming Response Body] ──> [HTTP 200/206 Completed]
```

### 16.3 WebSocket Session State Machine

```
[HTTP Upgrade Request] ──> [Origin & Cookie Validation]
                                 ├─ Fail ──> [HTTP 403] ──> [Socket Closed]
                                 ▼ Pass
                           [101 Switching Protocols]
                                 │
                                 ▼
                           [CONNECTED] ──> [Bind to Service Channel]
                                 │                 │
                                 │ Bounded Buffer  │ Buffer Full (>64 msgs)
                                 │ Healthy         ▼
                                 │           [BACKPRESSURE APPLIED]
                                 │                 │ Buffer Drained
                                 │                 ▼
                                 │           [STREAMING RESUMED]
                                 ▼ Provider Node Drops
                           [MIGRATING] ──> Emits Close Code 1012 (Service Restart)
                                 │
                                 ▼
                           [TERMINATED]
```

---

## 17. Comprehensive Failure Matrix

| Failure Mode | Detection Point | Immediate Action | Persistent State Impact | Recovery Action | User-Visible Effect |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Provider Node Crash** | TCP disconnect or overlay timeout | Resolver demotes provider to `Unavailable` | Record updated in SQLite | Traffic reroutes to next best provider | Transparent for GET; 504 prompt for POST |
| **Local Gateway Crash** | Process terminates | OS closes sockets | Zero state corruption (WAL committed) | Node restart restarts gateway on 8080 | Browser UI pings and reconnects |
| **Route Lost (Mesh Split)**| Route disappears from `RoutingTable` | Instances on unreachable nodes become `Unavailable` | In-memory cache invalidated | When link restores, route restored | Services disappear from catalog until heal |
| **mDNS Collision on LAN** | RFC 6762 probe detects another responder | Lower `NodeId` yields `fieldnet.local`; uses `fieldnet-<id>.local` | None | Both nodes remain fully accessible | Zero effect; browser routes through either |
| **Service Expiration** | Background sweep (now > expires_at) | Record removed from registry | Deleted from SQLite cache | Provider publishes new manifest with fresh TTL | Service removed from catalog |
| **Registry DB Corruption**| SQLite returns `SQLITE_CORRUPT` | DB moved to `.corrupt`, new DB initialized | Corrupt cache dropped | Local manifests re-registered automatically | Temporary capability re-discovery |
| **Browser Tab Disconnect**| Downstream TCP socket reset | Cancels Tokio async task | None | Sends `ServiceCancel` to provider | Wasted provider compute avoided |
| **WebSocket Peer Drop** | Downstream channel EOF | Sends WS close frame 1012 | In-memory session closed | Browser auto-reconnects to new provider | Brief UI reconnect indicator (<2s) |
| **CAS Content Provider Drop**| Phase 07 transfer stall | Stream aborted | Partial chunk discarded | Resume via HTTP Range request | Download pauses; resumes automatically |

---

## 18. Runtime Integration & Startup Ordering

The Local Web Gateway is an **optional access layer**, not the runtime itself. The FIELDNET runtime must start and operate flawlessly even if the gateway is disabled or network ports are unavailable.

```
Runtime Start (fieldnet-runtime)
  │
  ├── 1. Load Sovereign Identity (identity.json) ──> [Identity Ready]
  │
  ├── 2. Open Persistent Stores (SQLite dtn.db, doc_store.db)
  │
  ├── 3. Initialize Transport & Networking (TCP listeners, AEAD sessions) ──> [Networking Ready]
  │
  ├── 4. Start Peer Discovery (UDP Beacon 5354) ──> [Discovery Ready]
  │
  ├── 5. Initialize Content Subsystem (ContentStore, ChunkStore) ──> [Content Ready]
  │
  ├── 6. Start Service Subsystem (crates/fieldnet-service):
  │        ├── Open SQLite services.db
  │        ├── Start ServiceRegistry & HealthTracker
  │        ├── Register Local Services & publish ServiceManifests
  │        └── Initialize ServiceResolver
  │
  └── 7. Optional Local Web Gateway Startup (if config.enable_gateway = true):
           ├── Bind HTTP/WS Server (0.0.0.0:8080)
           ├── Start RFC 6762 mDNS Responder (UDP 5353)
           └── Begin mDNS Announcements (fieldnet.local)
```

**Resilience Invariant:** If UDP port 5353 (mDNS) or TCP port 8080 (HTTP) fails to bind (e.g. permission denied or port conflict), the gateway logs an error/warning and disables the web interface, while the **runtime core, mesh routing, DTN, CRDT sync, and content subsystems continue running at 100% capacity**.

---

## 19. System Configuration Specifications

```rust
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct ServiceSubsystemConfig {
    pub data_dir: PathBuf,
    pub max_registered_services: usize, // default: 10,000
    pub manifest_ttl_seconds: u32,       // default: 120s
    pub ttl_sweep_interval_secs: u64,    // default: 5s
    pub max_service_mesh_hops: u8,       // default: 5
    pub gateway: GatewayConfig,
}

#[derive(Clone, Debug)]
pub struct GatewayConfig {
    pub enabled: bool,                   // default: true
    pub bind_address: SocketAddr,        // default: 0.0.0.0:8080
    pub enable_mdns: bool,               // default: true
    pub mdns_hostname: String,           // default: "fieldnet.local"
    pub auto_pair_localhost: bool,       // default: true
    pub session_duration_secs: u64,      // default: 86,400 (24h)
    pub max_body_size_bytes: usize,      // default: 10 * 1024 * 1024 (10 MB)
    pub max_concurrent_connections: usize,// default: 512
    pub request_timeout_ms: u64,         // default: 30,000 ms
    pub allowed_hosts: Vec<String>,
}
```

---

## 20. Observability, Distributed Tracing & Telemetry

Phase 08 integrates directly with existing `tracing` instrumentation. Every request receives a globally unique trace context:

```
[Browser Request]
  X-Request-ID: "req_01h8x..."
  X-Correlation-ID: "corr_01h8x..."
        │
        ▼
[Gateway Scope] tracing::info_span!("http_request", request_id = %req_id, correlation_id = %corr_id)
        │
        ▼ Injects correlation_id into ServiceRpcEnvelope
[Service Mesh Scope] tracing::info_span!("service_rpc", service_id = %svc_id, provider = %node_id)
```

### 20.1 Gateway & Service Telemetry Metrics

- `fieldnet_service_registered_total`: Total active service registrations.
- `fieldnet_service_resolution_latency_ms`: Histogram of resolution time (target: < 2ms).
- `fieldnet_service_resolution_failures_total`: Counter of failed resolution queries.
- `fieldnet_gateway_requests_total`: Counter by HTTP method, status code, and service ID.
- `fieldnet_gateway_active_websockets`: Gauge of active concurrent WebSocket sessions.
- `fieldnet_gateway_bytes_streamed_total`: Counter of CAS and media bytes streamed to clients.
- `fieldnet_gateway_provider_failovers_total`: Counter of transparent failovers to alternate peers.

---

## 21. Formal API Boundaries & Trait Definitions

```rust
use async_trait::async_trait;

#[async_trait]
pub trait ServiceRegistryApi: Send + Sync {
    async fn register_local(&self, manifest: ServiceManifest) -> Result<(), ServiceError>;
    async fn unregister_local(&self, instance_id: &ServiceInstanceId) -> Result<(), ServiceError>;
    async fn ingest_advertisement(&self, manifest: ServiceManifest) -> Result<(), ServiceError>;
    async fn get_record(&self, instance_id: &ServiceInstanceId) -> Option<ServiceRecord>;
    async fn list_by_service(&self, service_id: &ServiceId) -> Vec<ServiceRecord>;
    async fn list_all_available(&self) -> Vec<ServiceRecord>;
}

#[async_trait]
pub trait ServiceResolverApi: Send + Sync {
    async fn resolve(
        &self,
        service_id: &ServiceId,
        constraint: &ServiceConstraint,
    ) -> Result<ServiceResolution, ServiceError>;
}

#[async_trait]
pub trait LocalWebGatewayApi: Send + Sync {
    async fn start(&self) -> Result<(), GatewayError>;
    async fn stop(&self) -> Result<(), GatewayError>;
    fn local_addr(&self) -> SocketAddr;
    async fn generate_pairing_pin(&self) -> Result<String, GatewayError>;
    async fn authenticate_session(&self, token: &BrowserSessionId) -> Option<BrowserSession>;
}
```

---

## 22. Rust Crate & Workspace Organization

Phase 08 introduces one dedicated workspace crate and precise extensions to existing crates:

```
crates/
├── fieldnet-protocol/
│   └── src/
│       ├── message_type.rs          # Extend MessageType with Service = 8
│       └── service.rs               # Wire format for ServiceRpc messages
├── fieldnet-service/                # NEW CRATE (Phase 08 Core)
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs                   # Facade & public exports
│       ├── error.rs                 # Typed ServiceError and GatewayError
│       ├── model/                   # Canonical data types (Manifest, Record, Health)
│       ├── registry/                # ServiceRegistry & SQLite persistence
│       ├── resolver/                # ServiceResolver & 8-step ranking engine
│       ├── health/                  # HealthTracker & passive telemetry
│       ├── protocol/                # Wire message encoding/decoding
│       └── gateway/                 # Local Web Gateway subsystem
│           ├── mod.rs               # Gateway lifecycle & router
│           ├── server.rs            # Axum/Hyper embedded HTTP engine
│           ├── mdns.rs              # RFC 6762 / 6763 mDNS responder
│           ├── session.rs           # BrowserSessionStore & pairing PINs
│           ├── auth.rs              # Host validation, CORS, CSRF
│           ├── proxy.rs             # Reverse proxy dispatcher
│           ├── ws.rs                # WebSocket upgrade & framing
│           └── streaming.rs         # CAS chunked streaming & Range requests
└── fieldnet-runtime/
    └── src/
        ├── runtime.rs               # Instantiate ServiceSubsystem in FieldnetRuntime
        └── lib.rs                   # Re-export fieldnet-service as service
```

---

## 23. Concrete BUILD Implementation Sequence

The implementation of Phase 08 is partitioned into seven rigorous, dependency-aware BUILD prompts:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ BUILD 01: Canonical Service Model, Manifests & Cryptographic Signing            │
│  - Target: crates/fieldnet-service/src/model/, crates/fieldnet-service/src/error│
│  - Deliverables: ServiceId, ServiceManifest, canonical signing bytes, Ed25519   │
│    signing/verification, SemVer versioning, permissions, unit tests.            │
├─────────────────────────────────────────────────────────────────────────────────┤
│ BUILD 02: Service Registry & SQLite Persistence Engine                          │
│  - Target: crates/fieldnet-service/src/registry/                                │
│  - Deliverables: ServiceRegistry, 7-state lifecycle engine, SQLite WAL store    │
│    (services.db), TTL background sweep, capacity bounds, persistence tests.     │
├─────────────────────────────────────────────────────────────────────────────────┤
│ BUILD 03: Service Resolver, Constraints & Deterministic Ranking Engine          │
│  - Target: crates/fieldnet-service/src/resolver/, health/                       │
│  - Deliverables: ServiceResolver, 8-step gate pipeline, 5-tier scoring function,│
│    passive health telemetry tracker, resolution explanation, ranking tests.     │
├─────────────────────────────────────────────────────────────────────────────────┤
│ BUILD 04: Multi-Hop Service Wire Protocol & Runtime Integration                 │
│  - Target: crates/fieldnet-protocol/src/service.rs, fieldnet-service/protocol/  │
│  - Deliverables: MessageType::Service = 8 wire codec, multi-hop mesh dispatch   │
│    through RoutingTable, integration into FieldnetRuntime, multi-node RPC tests.│
├─────────────────────────────────────────────────────────────────────────────────┤
│ BUILD 05: Local Web Gateway Core & RFC 6762 mDNS Responder                      │
│  - Target: crates/fieldnet-service/src/gateway/server.rs, mdns.rs, auth.rs      │
│  - Deliverables: Embedded HTTP server (Axum/Hyper), mDNS advertiser for         │
│    fieldnet.local, Host header rebinding defense, CORS/CSRF middleware.         │
├─────────────────────────────────────────────────────────────────────────────────┤
│ BUILD 06: Service Reverse Proxy, WebSocket Streaming & Phase 07 CAS Integration │
│  - Target: crates/fieldnet-service/src/gateway/proxy.rs, ws.rs, streaming.rs    │
│  - Deliverables: Reverse proxy to resolved services, WebSocket upgrade bridge,  │
│    direct zero-copy Phase 07 ContentStore streaming, HTTP 206 Range responder.  │
├─────────────────────────────────────────────────────────────────────────────────┤
│ BUILD 07: Final End-to-End System Verification, Fault Injection & Phase Audit   │
│  - Target: tests/phase08_e2e_integration.rs                                     │
│  - Deliverables: Multi-node real TCP mesh tests, provider crash failover tests, │
│    browser E2E verification, partition recovery audit, zero warnings/clippy.   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 24. Comprehensive Testing & Verification Pyramid

```
                       ┌─────────────────────────┐
                       │  Tier 6: Browser E2E    │ (Headless Chrome / Playwright)
                       ├─────────────────────────┤
                       │  Tier 5: Security/Fuzz  │ (CSRF, Rebinding, Slowloris)
                      ┌┴─────────────────────────┴┐
                      │ Tier 4: Fault Injection   │ (Provider Crash, Mesh Partition)
                     ┌┴───────────────────────────┴┐
                     │ Tier 3: Multi-Node Overlay  │ (3-Node Real TCP Mesh RPC)
                    ┌┴─────────────────────────────┴┐
                    │ Tier 2: Component Integration │ (Registry + Resolver + Mock Sockets)
                   ┌┴───────────────────────────────┴┐
                   │ Tier 1: Unit Tests (Pure Logic) │ (Manifests, Signing, Scoring, mDNS)
                   └─────────────────────────────────┘
```

---

## 25. Explicit Non-Goals for Phase 08

The following systems are explicitly deferred to subsequent phases:
- **WebAssembly Sandboxed Execution:** Reserved for Phase 09 (`fieldnet-wasm`).
- **Distributed Compute Scheduling & AI Inference Engines:** Reserved for Phase 10 (`fieldnet-compute`).
- **Global Internet Egress Proxying / STUN / TURN:** Reserved for Phase 11.
- **Native GUI Applications (Swift / Kotlin / Desktop):** Platform packaging concerns.
- **Custom Browser Protocol Extensions:** Standard browsers accessing HTTP/1.1 and WebSockets via `fieldnet.local` are strictly required.

---

# PHASE 08 — PROMPT 03 COMPLETE
