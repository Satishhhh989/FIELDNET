# FIELDNET Phase 08 — Architecture Specification
**Document ID:** `PHASE-08-01-SERVICE-MODEL-AND-REGISTRY`  
**Title:** Service Model, Registry, Identity & Resolution Architecture  
**Status:** SPECIFICATION APPROVED  
**Authoritative Reference:** `FIELDNET_MASTER_SPEC.md` (Sections 3.3, 18, 19, 28, 29, 30)  
**Foundation Subsystems:** Phase 01 (System Model), Phase 02 (Runtime Core), Phase 03 (Security & Identity), Phase 04 (Networking & Discovery), Phase 05 (Protocol, Routing & DTN), Phase 06 (Local-First Data & Synchronization), Phase 07 (Distributed File & Content System)

---

## 1. Executive Summary & Core Paradigm

FIELDNET Phase 08 establishes the **Service Discovery & Local Web Architecture**. The fundamental paradigm shift of Phase 08 is:

> **THE SERVICE-CENTRIC ABSTRACTION AXIOM:**  
> Applications request **CAPABILITIES AND SERVICES** (`search.service`, `maps.service`, `files.service`, `ai.service`), never fixed physical devices, hardware MAC addresses, or ephemeral IP sockets.

In legacy centralized, cloud-dependent architectures, applications are hardwired to hostnames (`api.service.com`) or IP:port endpoints (`192.168.1.50:8080`). In an austere, infrastructure-less tactical mesh network where nodes move, partition, reboot, and disappear arbitrarily, host-centric addressing fails. 

FIELDNET Phase 08 completely decouples **SERVICE IDENTITY** from **SERVICE INSTANCE** and **PROVIDER NODE**:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Application / Local Web Browser                          │
│                               "I need maps.service"                             │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │ ServiceId
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    Phase 08: Service Resolution Subsystem                       │
│  ┌────────────────────────┐  ┌────────────────────────┐  ┌───────────────────┐  │
│  │    Service Resolver    │  │    Service Registry    │  │ Local Web Gateway │  │
│  │ (Multi-criteria ranking│  │ (Local, Discovered,    │  │ (fieldnet.local,  │  │
│  │  & constraint filter)  │  │  State & Expiration)   │  │  mDNS/DNS-SD, HTTP)│ │
│  └───────────┬────────────┘  └───────────┬────────────┘  └─────────┬─────────┘  │
└──────────────┼───────────────────────────┼─────────────────────────┼────────────┘
               │ Resolved Instance         │ Service Manifest        │ Browser
               ▼                           ▼ (MessageType::Service=8)│ Traffic
┌────────────────────────────────────────────────────────────────────┼────────────┐
│ Existing FIELDNET Subsystems (Phases 01 - 07):                     │            │
│  - Phase 03: Identity (NodeId, Ed25519 Verifying Keys, Capability Tokens)       │
│  - Phase 05: Protocol Envelopes, Distance-Vector RoutingTable, Persistent DTN  │
│  - Phase 04: AEAD Secure Sessions (ChaCha20-Poly1305), TCP Transport           │
│  - Phase 07: ContentStore, ManifestStore (Static Assets, AI Models, WASM Blobs)│
└─────────────────────────────────────────────────────────────────────────────────┘
```

If Node A hosts `maps.service` and disconnects, Node A's instance expires. If Node B becomes reachable and advertises `maps.service`, the application continues resolving `maps.service` seamlessly without application code changes, reconfiguration, or manual reconnection.

---

## 2. Service Model

The service model establishes a clear three-tier ontological hierarchy:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ TIER 1: SERVICE IDENTITY (Logical Capability)                                   │
│ Example: "maps.service", "ai.inference", "chat.service", "files.service"         │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │ 1 : N (One service, many providers)
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ TIER 2: SERVICE INSTANCE (Concrete Deployment by a Specific Provider)          │
│ Example: "maps.service@node-A", "maps.service@node-B"                            │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │ 1 : 1 (Hosted by a sovereign node)
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ TIER 3: PROVIDER NODE (Sovereign Cryptographic Host)                            │
│ Example: NodeId(e16ef3ec...) with Ed25519 Keypair, Routing Metric & Transports  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 2.1 Core Entities

1. **`ServiceId` (Service Identity):**
   - Canonical, reverse-domain or dot-delimited string identifier specifying the logical service interface (e.g., `fieldnet.search`, `fieldnet.maps`, `ai.inference`, `workspace.collab`).
   - Globally provider-independent.
   - Format: `[a-z0-9_-]+(\.[a-z0-9_-]+)+` (maximum 128 characters).

2. **`InstanceId` (Service Instance Identity):**
   - Unique identifier distinguishing a specific running deployment of a service on a given node.
   - Format: 16-byte cryptographically secure random value or canonical hash of `(ServiceId || ProviderNodeId || Salt)`.
   - Represented as a 32-character hexadecimal string.

3. **`ProviderNodeId`:**
   - 32-byte Ed25519 verifying key of the sovereign node executing the service, directly referencing Phase 01/03 `fieldnet_core::NodeId`.

4. **`ServiceVersion`:**
   - Semantic Versioning 2.0 (`Major.Minor.Patch`).
   - Enables compatibility matching and constraint filtering (e.g., `^1.2.0`).

5. **`ServiceProtocol`:**
   - Communication framing used by the service endpoint:
     - `Native`: Internal FIELDNET multi-hop message RPC (`MessageType::Service = 8`).
     - `Http`: Standard HTTP/1.1 or HTTP/2 over local TCP socket or loopback.
     - `WebSocket`: Full-duplex WebSocket stream.
     - `ContentStream`: Phase 07 streaming CAS content feed.

6. **`ServiceEndpoint`:**
   - Transport routing information for accessing the service instance:
     - `Internal`: Routed purely over FIELDNET multi-hop overlay routing to `ProviderNodeId`. No physical port exposed to the network.
     - `TcpPort(u16)`: Local TCP port on the provider node (accessed via loopback or direct transport).
     - `UnixSocket(PathBuf)`: Local IPC endpoint on the host OS.
     - `UrlPath(String)`: Virtual route on the Local Web Gateway (e.g., `/services/maps`).

7. **`ServiceCapabilities`:**
   - Set of structured capability flags or key-value feature tags advertised by the instance (e.g., `{"model": "llama-3-8b", "quantization": "q4", "tile_cache": "true"}`).

8. **`ServicePermissions`:**
   - Security prerequisites required by the service (e.g., requires valid sovereign auth token, specific ACL role, or public access).

9. **`ServiceMetadata`:**
   - Key-value metadata providing human-readable title, description, documentation link, icon `ContentId`, and resource consumption hints.

10. **`ServiceExpiration`:**
    - Absolute Unix timestamp beyond which an unrefreshed advertisement is treated as dead.

11. **`TrustRequirement`:**
    - Minimum cryptographic trust tier demanded to interact with or resolve the service (`Public`, `KnownPeer`, `AuthorizedRole`, `CreatorOnly`).

---

## 3. Service Manifest Schema & Serialization

The `ServiceManifest` is the authoritative, self-contained, cryptographically signed document announcing a service instance to the network.

### 3.1 Manifest Schema (Rust Definition)

```rust
use fieldnet_core::{NodeId, Timestamp};
use serde::{Deserialize, Serialize};

pub const CURRENT_SERVICE_MANIFEST_SCHEMA_VERSION: u32 = 1;

/// Authoritative signed manifest declaring a concrete service instance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceManifest {
    /// Schema format version (v1).
    pub schema_version: u32,
    /// Logical service interface identity (e.g. "ai.inference").
    pub service_id: String,
    /// Unique instance identifier.
    pub instance_id: String,
    /// Sovereign Ed25519 identity of the hosting node.
    pub provider_node_id: NodeId,
    /// Provider's 32-byte Ed25519 public verifying key.
    #[serde(with = "hex")]
    pub provider_public_key: [u8; 32],
    /// SemVer service version string (e.g. "1.4.0").
    pub version: String,
    /// Service protocol dialect.
    pub protocol: ServiceProtocol,
    /// Reachability endpoint specification.
    pub endpoint: ServiceEndpoint,
    /// Advertised capabilities and feature tags.
    pub capabilities: Vec<String>,
    /// Access control constraints.
    pub permissions: ServicePermissions,
    /// Estimated resource profile (memory, CPU, GPU).
    pub resource_profile: Option<ResourceProfile>,
    /// Monotonically increasing sequence number from the provider node.
    pub sequence: u64,
    /// Advertisement time-to-live in seconds (e.g. 120s).
    pub ttl_seconds: u32,
    /// Creation timestamp (milliseconds since epoch).
    pub created_at: Timestamp,
    /// Absolute expiration timestamp (milliseconds since epoch).
    pub expires_at: Timestamp,
    /// Optional metadata (human-readable title, icon ContentId).
    pub metadata: ServiceMetadata,
    /// Ed25519 digital signature over canonical signing bytes.
    #[serde(with = "hex")]
    pub signature: [u8; 64],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServiceProtocol {
    NativeFieldnetRpc,
    Http1,
    WebSocket,
    ContentStream,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServiceEndpoint {
    /// Multi-hop internal FIELDNET overlay RPC routed to provider_node_id.
    InternalRpc,
    /// Local TCP listening port on provider node.
    LocalPort(u16),
    /// Local Unix domain socket path.
    LocalSocket(String),
    /// Gateway web path on Local Web Gateway (e.g. "/services/search").
    GatewayPath(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServicePermissions {
    pub is_public: bool,
    pub required_roles: Vec<String>,
    pub requires_capability_token: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceProfile {
    pub memory_bytes_estimate: u64,
    pub requires_gpu: bool,
    pub typical_latency_ms: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceMetadata {
    pub display_name: String,
    pub description: String,
    pub icon_content_id: Option<fieldnet_content::ContentId>,
    pub documentation_content_id: Option<fieldnet_content::ContentId>,
}
```

### 3.2 Canonical Signing Bytes Specification

To prevent signature malleability, version downgrade attacks, and cross-domain replay, the signature is calculated strictly over deterministic canonical bytes:

```
FIELDNET_SERVICE_MANIFEST_V1:
  schema_version: u32 (4 bytes, BE)
  service_id_len: u16 (2 bytes, BE)
  service_id_bytes: [u8]
  instance_id_len: u16 (2 bytes, BE)
  instance_id_bytes: [u8]
  provider_node_id: [u8; 32]
  version_len: u16 (2 bytes, BE)
  version_bytes: [u8]
  protocol_tag: u8 (1 byte: 1=Native, 2=Http, 3=WebSocket, 4=ContentStream)
  endpoint_bytes: canonical serialized endpoint
  sequence: u64 (8 bytes, BE)
  ttl_seconds: u32 (4 bytes, BE)
  created_at: u64 (8 bytes, BE)
  expires_at: u64 (8 bytes, BE)
```

The provider signs these canonical bytes using its sovereign Ed25519 private key. Any peer verifying the manifest reassembles these exact bytes and calls `ed25519_dalek::VerifyingKey::verify()`.

---

## 4. Service Registry Architecture

The `ServiceRegistry` is the central catalog of known service instances on a FIELDNET node. It aggregates services from four distinct origins:
1. **Local Services:** Hosted and executed directly on the local node.
2. **Directly Discovered Services:** Advertised by immediate 1-hop physical neighbors over TCP/UDP.
3. **Multi-Hop Discovered Services:** Advertised by distant nodes over the distance-vector routing mesh.
4. **Cached / Historical Services:** Retained in persistent SQLite storage for offline capability awareness.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                 ServiceRegistry                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│ - inner: RwLock<ServiceRegistryInner>                                           │
│   ├── by_service: HashMap<String, HashMap<String, ServiceRecord>>                │
│   │     (ServiceId ──> (InstanceId ──> ServiceRecord))                          │
│   ├── by_provider: HashMap<NodeId, HashSet<String>>                             │
│   │     (ProviderNodeId ──> Set<InstanceId>)                                    │
│   └── instance_index: HashMap<String, String>                                   │
│         (InstanceId ──> ServiceId)                                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│ - persistence: ServiceStore (SQLite WAL database: services.db)                  │
│ - event_tx: broadcast::Sender<ServiceEvent>                                     │
│ - config: RegistryConfig (capacity caps, TTL sweeps, trust tiers)               │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.1 State Hierarchy of Service Instances

The registry tracks every service instance through six authoritative states:

```
┌──────────────┐      Cryptographic
│  DISCOVERED  │ ──> Verification Passes ──> ┌───────────────┐
└──────────────┘                             │ AUTHENTICATED │
       │                                     └───────┬───────┘
       │ Signature Fails or                          │ Provider Reached
       │ Manifest Invalid                            │ & Route Available
       ▼                                             ▼
┌──────────────┐                             ┌───────────────┐
│ REJECTED /   │                             │   AVAILABLE   │ ◄──┐
│   DROPPED    │                             └───────┬───────┘    │ Recovered
└──────────────┘                                     │            │
                         Provider Heartbeat Missed   │ Health Check Degradation
                         or Route Metric Degraded    │
                                                     ▼
                                             ┌───────────────┐    │
                                             │   DEGRADED    │ ───┘
                                             └───────┬───────┘
                                                     │ Provider Route Lost
                                                     │ or Service Stops
                                                     ▼
                                             ┌───────────────┐
                                             │  UNAVAILABLE  │
                                             └───────┬───────┘
                                                     │ TTL Expires
                                                     ▼
                                             ┌───────────────┐
                                             │    EXPIRED    │
                                             └───────────────┘
```

### 4.2 Record Invalidation & Eviction Rules
- **Monotonic Sequence Enforcement:** If an incoming manifest for `(ServiceId, InstanceId)` has a `sequence <= current_record.sequence`, it is rejected as stale or replay.
- **Provider Disconnection Propagation:** When `FieldnetRuntime` emits `ConnectionEvent::PeerDisconnected { peer_id }`, the registry immediately sweeps all instances hosted by `peer_id`. Direct instances transition from `Available` $\to$ `Unavailable` unless an alternate multi-hop route exists in the `RoutingTable`.
- **TTL Expiration Sweep:** A background task runs every 5 seconds, purging expired records and emitting `ServiceEvent::Expired`.

---

## 5. Service Resolver Architecture

The `ServiceResolver` translates a high-level service query into an optimal, callable `ServiceInstance`.

### 5.1 Query & Constraint Specification

```rust
#[derive(Clone, Debug, Default)]
pub struct ServiceQuery {
    pub service_id: String,
    pub version_requirement: Option<semver::VersionReq>,
    pub required_capabilities: Vec<String>,
    pub preferred_protocol: Option<ServiceProtocol>,
    pub locality_preference: LocalityPreference,
    pub max_acceptable_rtt_ms: Option<u32>,
    pub required_trust_tier: TrustTier,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum LocalityPreference {
    #[default]
    PreferLocal,
    LocalOnly,
    PreferRemote,
    Any,
}
```

### 5.2 Deterministic Ranking Algorithm

To ensure predictable behavior across nodes, the resolver applies an 8-step deterministic filter and scoring pipeline:

```
[All Candidates in Registry for ServiceId]
                     ↓
Gate 1: Schema & Expiration Check (now < expires_at)
                     ↓
Gate 2: State Validity (State ∈ {Available, Degraded})
                     ↓
Gate 3: Version Compatibility (VersionReq::matches(&candidate.version))
                     ↓
Gate 4: Capability Match (candidate.capabilities ⊇ required_capabilities)
                     ↓
Gate 5: Authorization Pre-check (Caller satisfies candidate.permissions)
                     ↓
Gate 6: Route Reachability (RoutingTable has next_hop for candidate.provider_node_id)
                     ↓
Gate 7: Locality Preference Filtering
                     ↓
[Candidate Scoring & Ranking Function]
```

#### Ranking Scoring Function
For each candidate passing Gates 1–7, a composite score $S$ ($0 \le S \le 100,000$) is computed:
$$S = W_{\text{loc}} + W_{\text{state}} + W_{\text{rel}} + W_{\text{rtt}} + W_{\text{hops}}$$

Where:
- $W_{\text{loc}} = 50,000$ if provider is local node; $0$ if remote.
- $W_{\text{state}} = 25,000$ if `Available`; $10,000$ if `Degraded`.
- $W_{\text{rel}} = \text{ReliabilityPermille} \times 15$ (max $15,000$).
- $W_{\text{rtt}} = \max(0, 10,000 - (\text{ObservedRTT}_{\text{ms}} \times 20))$.
- $W_{\text{hops}} = \max(0, 5,000 - (\text{Hops} \times 1,000))$.

The candidate with the highest composite score is selected. If scores tie, the lexicographically smallest `InstanceId` wins.

---

## 6. Service Health & Liveness Model

FIELDNET avoids aggressive heartbeat flooding over low-bandwidth tactical mesh channels by employing **Passive Telemetry Correlation**:

1. **Piggybacked Transport Metrics:** The service health tracker correlates with Phase 04 network connection keep-alives and Phase 05 route vector broadcasts. If the hosting node's link is healthy, the service is presumed reachable.
2. **Transactional Feedback:** Every successful service request increments the instance's reliability score; every timed-out or connection-reset request decrements the score and increments consecutive failure counters.
3. **Graceful Degradation:**
   - 1 failed RPC $\to$ Status becomes `Degraded`.
   - 3 consecutive failed RPCs $\to$ Status becomes `Unavailable`, triggering automatic failover.
   - Route lost in routing table $\to$ Status becomes `Unavailable`.
   - Manifest `expires_at` reached without refresh $\to$ Status becomes `Expired`.

---

## 7. Service Lifecycle & Extensibility (WASM Ready)

The lifecycle architecture accommodates native Rust runtime services, external host daemon services, and future Phase 09 sandboxed WebAssembly services:

```
    ┌────────────┐
    │  INSTALL   │ (Package/Binary placed on disk or stored as ContentId in CAS)
    └─────┬──────┘
          │
          ▼
    ┌────────────┐
    │   VERIFY   │ (Cryptographic manifest verified, permissions validated)
    └─────┬──────┘
          │
          ▼
    ┌────────────┐
    │  REGISTER  │ (Manifest submitted to ServiceRegistry)
    └─────┬──────┘
          │
          ▼
    ┌────────────┐
    │   START    │ (Service worker task spawned, endpoint listening)
    └─────┬──────┘
          │
          ▼
    ┌────────────┐
    │  HEALTHY   │ (Serving requests, periodic sequence refresh)
    └─────┬──────┘
          │
          ├────────────────────────┐
          ▼                        ▼
    ┌────────────┐           ┌────────────┐
    │   UPDATE   │           │    STOP    │ (Explicit teardown or node shutdown)
    └────────────┘           └─────┬──────┘
                                   │
                                   ▼
                             ┌────────────┐
                             │   REMOVE   │ (Unregistered, storage reclaimed)
                             └────────────┘
```

Future Phase 09 integration hook: A WASM service package stored as a Phase 07 `ContentId` is installed by launching a Wasmtime runtime instance that exposes an `InternalRpc` or `LocalSocket` endpoint and publishes its signed `ServiceManifest` to `ServiceRegistry`.

---

## 8. Service Security & Authorization Model

Security strictly adheres to Phase 03 sovereign cryptography. The service layer enforces a clear three-tier separation of concerns:

```
1. NODE AUTHENTICATION (Transport Layer)
   - "Is the peer connecting via TCP an authentic Ed25519 sovereign node?"
   - Verified via mutual challenge-response handshake transcript.

2. SERVICE AUTHENTICATION (Service Manifest Layer)
   - "Did the claiming node genuinely author and sign this service advertisement?"
   - Verified via Ed25519 signature over canonical signing bytes.

3. SERVICE AUTHORIZATION (Execution / Access Layer)
   - "Does the calling user/node possess permission to execute this service capability?"
   - Evaluated by ServiceAuthorizer using ACLs and signed CapabilityTokens.
```

### Security Rules:
- **No Implicit Trust:** Being a connected peer in the routing table does NOT grant permission to invoke private services.
- **Fail-Closed Execution:** If a service requires `requires_capability_token: true`, any request lacking a valid, unexpired `CapabilityToken` signed by the service owner is rejected before invoking the service handler.
- **Endpoint Isolation:** Localhost TCP ports or Unix sockets exposed by services bind strictly to `127.0.0.1` or restricted filesystem permissions, preventing unauthenticated local OS processes from bypassing FIELDNET security gates.

---

## 9. Service Replication & Multi-Provider Swarm

A single logical `ServiceId` (e.g., `ai.inference` or `search.service`) can be simultaneously hosted by multiple independent nodes in the mesh.

```
                  ┌──────────────────────┐
                  │    search.service    │
                  └──────────┬───────────┘
         ┌───────────────────┼───────────────────┐
         ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ Instance @ NodeA│ │ Instance @ NodeB│ │ Instance @ NodeC│
│ (v1.2.0, CPU)   │ │ (v1.4.0, GPU)   │ │ (v1.4.0, GPU)   │
│ RTT: 15ms       │ │ RTT: 45ms       │ │ RTT: 120ms      │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

### Swarm Behaviors:
- **Capability-Based Specialization:** A client requesting `ai.inference` with constraint `requires_gpu: true` automatically filters out Node A and selects between Node B and Node C.
- **Seamless Failover:** If Node B runs out of queue capacity or disconnects, the resolver re-evaluates the query and routes subsequent requests to Node C without interrupting client operations.
- **Load Balancing:** When multiple instances have identical scores within a 5% margin, the resolver applies weighted round-robin distribution to prevent hot-spotting.

---

## 10. Service Event Bus Model

Phase 08 integrates directly into the existing `FieldnetRuntime` in-memory Event Bus, introducing the `service.*` topic namespace:

| Event Topic | Payload Struct | Description |
| :--- | :--- | :--- |
| `service.discovered` | `ServiceManifest` | Emitted when a new valid manifest is received from the network. |
| `service.registered` | `ServiceRecord` | Emitted when a service is accepted into the registry. |
| `service.available` | `(ServiceId, InstanceId)` | Emitted when a service transitions to fully usable state. |
| `service.degraded` | `(ServiceId, InstanceId, Reason)` | Emitted when RTT spikes or transient RPC failures occur. |
| `service.unavailable`| `(ServiceId, InstanceId, Reason)` | Emitted when route is lost or provider crashes. |
| `service.expired` | `(ServiceId, InstanceId)` | Emitted when TTL expires without sequence refresh. |
| `service.removed` | `(ServiceId, InstanceId)` | Emitted upon explicit unregistration. |
| `service.resolved` | `(ServiceId, InstanceId, Score)`| Emitted when resolver selects an instance for a client. |
| `service.failover` | `(ServiceId, OldInstance, NewInstance)` | Emitted when active provider fails and traffic reroutes. |

---

## 11. Service Subsystem + Content Subsystem (Phase 07 Integration)

Services leverage Phase 07 Content-Addressed Storage rather than duplicating file transfer:

1. **Service Assets & UI Bundles:** Services providing browser interfaces store their static web assets (HTML/JS/CSS/images) as immutable Phase 07 `ContentId`s.
2. **Machine Learning Model Distribution:** An AI service (`ai.inference`) advertises its required weights file as a Phase 07 `ContentId`. Neighbor nodes replicating the service download the weights via Phase 07 swarm transfer.
3. **Manifest Icon & Doc Links:** `ServiceManifest` fields (`icon_content_id`, `documentation_content_id`) reference Phase 07 digests directly.

---

## 12. Service Persistence Model

To avoid unbounded disk growth while preserving fast restarts, state is partitioned:

### 12.1 Persistent State (SQLite: `services.db`)
- `local_services`: Manifests of services locally registered on this node.
- `trusted_service_cache`: Cryptographically validated manifests of remote services seen within the last 24 hours.
- `service_authorizations`: Sovereign ACLs and capability grants.

### 12.2 Ephemeral State (In-Memory Only)
- Active TCP session links.
- Current observed RTT and latency moving averages.
- Active in-flight RPC counts and request queues.
- `Degraded` / `Available` transient status flags.
- Real-time resolver score cache.

---

## 13. Multi-Hop Service Discovery

Service discovery utilizes a hybrid **Proactive Mesh Broadcast + Reactive Multi-Hop Query** protocol:

```
[Node A] ◄─── (Hop 1) ───► [Node B (Relay)] ◄─── (Hop 2) ───► [Node C (Provider)]
```

### Discovery Mechanics:
1. **Direct 1-Hop Discovery (Proactive):** Nodes broadcast signed compact service announcements over authenticated TCP connections every 30 seconds or upon service state change.
2. **Multi-Hop Relay (Distance-Vector Extension):** Intermediate relays (Node B) forward valid service advertisements to their adjacent peers if `hops < max_service_hops` (default 5 hops).
3. **Reactive Service Query (`ServiceQueryMessage`):** If Node A requires a service not present in its registry, it broadcasts a targeted query (`MessageType::Service = 8`) through the routing mesh. Any node with a valid cached manifest responds with a unicast `ServiceAdvertiseMessage`.

---

## 14. Local Web Gateway Architecture

The Local Web Gateway bridges standard web browsers (Chrome, Safari, Firefox, Edge) to the headless FIELDNET service mesh without requiring browser plugins, custom protocols, or proprietary software.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      Client Device (Laptop, Smartphone, Tablet)                 │
│                                                                                 │
│   Web Browser (Chrome / Safari / Firefox)                                       │
│       │                                                                         │
│       ├── 1. Resolves "fieldnet.local" via standard mDNS/DNS-SD (port 5353)     │
│       │   ──> Answers with 127.0.0.1 or node's local LAN IP                     │
│       │                                                                         │
│       └── 2. Sends standard HTTP request:                                       │
│           GET http://fieldnet.local:8080/services/maps/index.html               │
└───────────────────────────────────────┬─────────────────────────────────────────┘
                                        │ Standard HTTP/1.1 or WebSocket
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│               FIELDNET Local Web Gateway (Embedded HTTP/WS Reverse Proxy)       │
│  - Port: Configurable (default 8080 or 80)                                      │
│  - Host Header Inspection: Recognizes "fieldnet.local" and "*.fieldnet.local"   │
│  - Gateway Router:                                                              │
│      /                           ──> Embedded System Dashboard & Directory      │
│      /services/<service_id>/*    ──> Reverse Proxy to Resolved Service Instance │
│      /api/v1/content/<content_id>──> Direct streaming from Phase 07 ContentStore│
│      /api/v1/services            ──> Service catalog API                        │
└───────────────────────────────────────┬─────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                   Service Resolver (Internal Dispatch Engine)                   │
│  Resolves "/services/maps" ──> ServiceInstance(maps.service@node-C)             │
│  Proxies HTTP payload across multi-hop overlay (MessageType::Service = 8)       │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 15. mDNS & DNS-SD Integration

FIELDNET adheres strictly to **RFC 6762 (mDNS)** and **RFC 6763 (DNS-SD)**:
- **Multicast Group:** IPv4 `224.0.0.251:5353` and IPv6 `[FF02::FB]:5353`.
- **Hostname Resolution:** Advertises `fieldnet.local` pointing to the node's local IP address.
- **Service Enumeration (DNS-SD):** Registers PTR, SRV, and TXT records:
  - Service Type: `_fieldnet._tcp.local.`
  - TXT Records: `node_id=<hex>`, `version=0.1.0`, `services=search,maps,files`.

---

## 16. HTTP / WebSocket Gateway Boundary

The gateway provides standard protocol translation:

```
Browser HTTP Request:
  GET /services/search/query?q=medical HTTP/1.1
  Host: fieldnet.local:8080

Gateway Translation:
  1. Inspects path: target service is "search" (normalized to "fieldnet.search").
  2. Calls ServiceResolver::resolve("fieldnet.search").
  3. If resolved instance is Local:
     - Proxies to local socket/port or executes in-process handler.
  4. If resolved instance is Remote (Node C, 2 hops away):
     - Encapsulates request into ServiceRpcRequest message (MessageType::Service = 8).
     - Dispatches through RoutingTable via next hop (Node B).
     - Awaits ServiceRpcResponse.
     - Streams response body back to browser as HTTP 200 OK.
```

---

## 17. Rust Crate & Module Organization

Phase 08 will introduce a dedicated crate: `crates/fieldnet-service`.

```
crates/fieldnet-service/
├── Cargo.toml
└── src/
    ├── lib.rs                      # Public exports and facade
    ├── error.rs                    # ServiceError typed enumerations
    ├── model/                      # Core data structures
    │   ├── mod.rs
    │   ├── id.rs                   # ServiceId, InstanceId
    │   ├── manifest.rs             # ServiceManifest & canonical signing
    │   ├── protocol.rs             # ServiceProtocol, ServiceEndpoint
    │   └── record.rs               # ServiceRecord & ServiceState
    ├── registry/                   # Central catalog
    │   ├── mod.rs
    │   ├── registry.rs             # ServiceRegistry implementation
    │   ├── store.rs                # SQLite persistence (services.db)
    │   └── config.rs               # Registry limits & TTL policies
    ├── resolver/                   # Query & selection engine
    │   ├── mod.rs
    │   ├── resolver.rs             # ServiceResolver implementation
    │   ├── query.rs                # ServiceQuery & constraints
    │   └── ranking.rs              # 8-step scoring & ranking algorithm
    ├── health/                     # Liveness and health monitoring
    │   ├── mod.rs
    │   └── tracker.rs              # Passive telemetry & failure accounting
    ├── protocol/                   # Wire messages for mesh transport
    │   ├── mod.rs
    │   ├── message.rs              # ServiceMessage, Advertise, Query, RPC
    │   └── codec.rs                # Framing serialization
    └── gateway/                    # Local Web Gateway
        ├── mod.rs
        ├── server.rs               # Embedded HTTP/WS gateway (axum/hyper)
        ├── router.rs               # Reverse proxy route dispatch
        └── mdns.rs                 # RFC 6762 mDNS responder
```

---

## 18. Programmatic Rust API Design

```rust
#[async_trait]
pub trait ServiceRegistryApi: Send + Sync {
    /// Registers a locally hosted service instance and publishes its signed manifest.
    async fn register_local(&self, manifest: ServiceManifest) -> Result<(), ServiceError>;
    
    /// Unregisters a locally hosted service instance.
    async fn unregister_local(&self, instance_id: &str) -> Result<(), ServiceError>;
    
    /// Ingests and cryptographically verifies an incoming service advertisement.
    async fn handle_advertisement(&self, manifest: ServiceManifest) -> Result<(), ServiceError>;
    
    /// Queries a specific service record by instance ID.
    async fn get_instance(&self, instance_id: &str) -> Option<ServiceRecord>;
    
    /// Lists all known records for a given ServiceId.
    async fn list_instances(&self, service_id: &str) -> Vec<ServiceRecord>;
}

#[async_trait]
pub trait ServiceResolverApi: Send + Sync {
    /// Resolves the optimal available instance for a logical ServiceId.
    async fn resolve(&self, service_id: &str) -> Result<ServiceRecord, ServiceError>;
    
    /// Resolves an instance matching strict constraint criteria.
    async fn resolve_with_constraints(&self, query: &ServiceQuery) -> Result<ServiceRecord, ServiceError>;
}
```

---

## 19. Authoritative State Machines

### 19.1 Service Instance Lifecycle State Machine

```
              ┌───────────────────────────┐
              │          INITIAL          │
              └─────────────┬─────────────┘
                            │ Manifest Received
                            ▼
              ┌───────────────────────────┐
              │        DISCOVERED         │
              └─────────────┬─────────────┘
                            │ Signature Verified
                            ▼
              ┌───────────────────────────┐
              │       AUTHENTICATED       │
              └─────────────┬─────────────┘
                            │ Route Verified Available
                            ▼
    ┌───────────────────────────────────────────────┐
    │                   AVAILABLE                   │ ◄───┐
    └───────┬───────────────────────────────┬───────┘     │ RTT Restored
            │ Transient Failure             │ Route Lost  │ or Re-probed
            ▼                               │             │
    ┌───────────────┐                       │             │
    │   DEGRADED    │ ──────────────────────┤             │
    └───────┬───────┘                       │             │
            │ 3 Consecutive Failures        ▼             │
            └───────────────────────> ┌───────────┐       │
                                      │UNAVAILABLE│ ──────┘
                                      └─────┬─────┘
                                            │ TTL Expired
                                            ▼
                                      ┌───────────┐
                                      │  EXPIRED  │
                                      └─────┬─────┘
                                            │ GC Sweep
                                            ▼
                                      ┌───────────┐
                                      │  REMOVED  │
                                      └───────────┘
```

---

## 20. Architectural Trade-off Analysis & Key Decisions

### Decision 1: Manifest-Based Identity vs. DNS-Only Records
- **Problem:** How to announce services across an air-gapped, zero-trust mesh.
- **Chosen Design:** Standalone, cryptographically signed `ServiceManifest` containing full capability specifications, versioning, and Ed25519 signatures.
- **Alternatives Considered:** Lightweight DNS TXT records only; decentralized DHT keys.
- **Rationale:** DNS TXT records are unauthenticated and size-constrained. DHT lookups add multi-roundtrip latency. Signed manifests allow instant 1-hop verification and offline caching.
- **Failure Modes:** Malformed or oversized manifests rejected at wire boundary.

### Decision 2: Overlay Multi-Hop RPC vs. Direct TCP Sockets
- **Problem:** How clients reach services hosted on distant nodes (e.g., 3 hops away).
- **Chosen Design:** Service requests encapsulate into `MessageType::Service = 8` wire envelopes and traverse the existing Phase 05 `RoutingTable` and DTN queue.
- **Alternatives Considered:** Requiring clients to establish direct point-to-point TCP connections to every service provider.
- **Rationale:** Direct TCP cannot span air-gapped relays or mobile carrier nodes without IP routing. Overlay RPC preserves node anonymity and DTN partition survival.

### Decision 3: Standard mDNS + Local Reverse Proxy vs. Custom Browser Protocol
- **Problem:** Enabling standard unmodified web browsers to interact with FIELDNET.
- **Chosen Design:** Implement standard RFC 6762 mDNS responding to `fieldnet.local` + local HTTP reverse proxy mapping paths to FIELDNET service RPCs.
- **Alternatives Considered:** Custom browser extension; proprietary desktop web view wrapper.
- **Rationale:** Fulfills Section 19 of `FIELDNET_MASTER_SPEC.md`: *"Normal browsers should be able to access local services."* Zero install footprint for client browser devices.

---

## 21. Failure Mode Matrix

| Failure Mode | Detection Mechanism | State Transition | Recovery Action | Persistence Impact | Application Impact |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Provider Crash / Shutdown** | TCP socket disconnect or missing beacon | `Available` $\to$ `Unavailable` | Resolver triggers failover to next best instance | Record marked unavailable in SQLite | Current RPC retried on alternate provider |
| **Network Partition** | Route disappears from `RoutingTable` | `Available` $\to$ `Unavailable` | Service requests queued in DTN if policy allows | Staged in SQLite `dtn_bundles` | Delayed execution until partition heals |
| **Stale Advertisement** | Monotonic sequence check ($seq \le seq_{curr}$) | Rejected on ingest | Drop incoming packet | None | Stale data discarded silently |
| **Forged / Tampered Manifest** | Ed25519 signature verification failure | `Discovered` $\to$ `Rejected` | Discard manifest, log security warning | None | Zero compromise of registry state |
| **Unauthorized Invocation** | ACL or `CapabilityToken` verification failure | Rejected at gate | Abort RPC with `PermissionDenied` (403) | Logged in audit log | Client receives typed authorization error |
| **Service Overload** | Provider returns `CapacityExceeded` (429) | `Available` $\to$ `Degraded` | Resolver demotes score, shifts traffic | Ephemeral score updated | Client request routed to alternate instance |
| **Runtime Crash & Reboot** | Process restart | Startup crash recovery | Reloads local manifests and cached records from SQLite | WAL committed | Local services resume automatically |

---

## 22. Security Invariants (Phase 08)

1. **Service Identity is Provider-Independent:** `ServiceId` represents interface semantics and cannot be monopolized by a single node ID.
2. **Every Manifest Must Be Signed:** No service record can enter `Authenticated` state without a valid Ed25519 signature from its declaring provider.
3. **Transport Authentication $\neq$ Service Authorization:** Connecting to a node does not grant permission to execute its private services.
4. **Fail-Closed Authorization:** Missing or expired capability tokens result in immediate rejection before executing service logic.
5. **Replay & Sequence Protection:** Manifests with sequence numbers $\le$ active records are rejected.
6. **No Raw Socket Exposure to the Mesh:** Service endpoints do not expose raw unencrypted network listeners across the mesh; all traffic passes through AEAD sessions.
7. **mDNS Strictly Bounded:** mDNS responder only answers queries for `.local` domains, preventing DNS poisoning or WAN leakage.
8. **Gateway Path Sanitization:** Local Web Gateway strictly sanitizes URL paths, preventing directory traversal or header injection attacks.

---

## 23. Testing & Verification Strategy

All future Phase 08 implementation builds must satisfy rigorous automated testing:

1. **Unit Tests:**
   - Manifest canonical byte construction and deterministic serialization.
   - Ed25519 signature generation and verification.
   - SemVer version matching and capability constraint evaluation.
   - Resolver ranking algorithm score calculations.
2. **Component Tests:**
   - `ServiceRegistry` CRUD operations, sequence updates, and TTL expirations.
   - SQLite WAL persistence across simulated database restarts.
   - `ServiceAuthorizer` ACL evaluations and token validation.
3. **Multi-Node Integration Tests (Real Sockets):**
   - **3-Node E2E Discovery:** Node A hosts `search.service`; Node B discovers and resolves Node A over real TCP; Node C resolves Node A via 2-hop relay through Node B.
   - **Swarm Failover & Re-Resolution:** Node A and Node B both host `maps.service`. Client connects to Node A. Node A process is abruptly killed (`SIGKILL`). Resolver automatically detects failure, switches to Node B, and completes request without failure.
   - **Local Web Gateway Verification:** Send standard HTTP request to `http://fieldnet.local:8080/services/test` using `reqwest` or `hyper`; verify gateway resolves service, forwards RPC across mesh, and returns HTTP 200 with payload.

---

## 24. Implementation Order for Future Phase 08 BUILD Prompts

To maintain rigorous architectural discipline, Phase 08 will be implemented in five distinct builds:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ BUILD 01: Service Model, Manifests & Canonical Cryptographic Signing           │
│           - ServiceId, InstanceId, ServiceManifest, canonical signing bytes     │
│           - Ed25519 signing & verification, unit tests                          │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ BUILD 02: Service Registry & SQLite Persistence Engine                          │
│           - ServiceRegistry, ServiceRecord, 6-state lifecycle engine            │
│           - SQLite WAL persistence (services.db), TTL eviction, capacity bounds │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ BUILD 03: Service Resolver & Multi-Criteria Ranking Engine                      │
│           - ServiceResolver, constraint filtering, 8-step scoring pipeline      │
│           - Swarm multi-provider resolution, failover logic                     │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ BUILD 04: Multi-Hop Service Wire Protocol & Runtime Integration                 │
│           - MessageType::Service = 8, ServiceAdvertise, ServiceQuery, RPC      │
│           - Integration into FieldnetRuntime, multi-hop mesh forwarding         │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ BUILD 05: Local Web Gateway & mDNS Responder                                    │
│           - RFC 6762 mDNS responder for fieldnet.local                          │
│           - Embedded HTTP/WS reverse proxy (axum/hyper)                         │
│           - Real multi-node E2E browser-accessible verification                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 25. Explicit Non-Goals for Phase 08

The following capabilities are deliberately excluded from Phase 08:
- **WebAssembly Runtime Execution:** Reserved for Phase 09 (`fieldnet-wasm`).
- **Distributed Compute Scheduling / AI Inference Engines:** Reserved for Phase 10 (`fieldnet-compute`).
- **Global Internet Egress Proxying / STUN / TURN:** Reserved for Phase 11.
- **Native GUI Client Applications (iOS / Android / Desktop):** Reserved for platform packaging.
- **Proprietary Browser Protocol Development:** Normal browsers accessing standard HTTP/1.1 and WebSockets via `fieldnet.local` are strictly required.
