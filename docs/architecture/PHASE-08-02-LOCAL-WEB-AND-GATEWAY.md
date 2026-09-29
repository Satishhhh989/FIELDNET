# FIELDNET Phase 08 — Architecture Specification
**Document ID:** `PHASE-08-02-LOCAL-WEB-AND-GATEWAY`  
**Title:** Local Web, mDNS/DNS-SD & Service Gateway Architecture  
**Status:** SPECIFICATION APPROVED  
**Authoritative Reference:** `FIELDNET_MASTER_SPEC.md` (Sections 3.3, 18, 19, 28, 29, 30)  
**Foundation Subsystems:** Phase 01 (System Model), Phase 02 (Runtime Core), Phase 03 (Security & Identity), Phase 04 (Networking & Discovery), Phase 05 (Protocol, Routing & DTN), Phase 06 (Local-First Data & Synchronization), Phase 07 (Distributed File & Content System), Phase 08-01 (Service Model & Registry)

---

## 1. Executive Summary & Architectural Axiom

FIELDNET Phase 08 Prompt 01 established the **Service Model, Registry, Identity, and Multi-Criteria Resolution** foundation. In that specification, services are modeled as sovereign, signed, location-independent capabilities (`ServiceId`, `ServiceManifest`, `ServiceRecord`) decoupled from hardware MAC addresses or fixed IP sockets.

FIELDNET Phase 08 Prompt 02 establishes the bridge between ordinary, unmodified consumer web browsers (Apple Safari, Google Chrome, Mozilla Firefox, Microsoft Edge) and the decentralized FIELDNET mesh:

> **THE LOCAL WEB ACCESS AXIOM:**  
> The web browser is an ordinary, untrusted client of the FIELDNET local computing environment.  
> Standard browsers must access decentralized mesh services via standard protocols (HTTP/1.1, HTTP/2, WebSocket) and standard local name resolution (`fieldnet.local`) without browser extensions, custom runtimes, root certificate installations, or knowledge of mesh network topology.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      Standard Browser (Safari / Chrome / Firefox)               │
│                               "http://fieldnet.local"                           │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │ 1. mDNS Query (RFC 6762 / 5353)
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│               mDNS / DNS-SD Subsystem (fieldnet.local ──> 127.0.0.1 or LAN IP)  │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │ 2. Standard HTTP/1.1 / WS Request
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│           FIELDNET Local Web Gateway (Embedded Reverse Proxy & Adapter)         │
│  - Host & Origin validation (CORS, CSRF defense, DNS rebinding mitigation)       │
│  - Browser Session Auth (Pairing tokens, Cookie isolation; Node keys hidden)     │
│  - Static Asset Delivery (Direct Phase 07 ContentStore streaming)               │
│  - Service Route Dispatch (/services/<service_id>/*)                            │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │ 3. Service Query & Constraint Filter
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│           Phase 08-01: Service Resolver (Deterministic Multi-Criteria Scoring)  │
│  - Resolves logical ServiceId ──> Optimal ServiceInstance                        │
│  - Evaluates health, latency, route hop-count, capability flags, permissions    │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │ 4. Request Forwarding
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                  Service Execution & Network Multi-Hop Dispatch                 │
│  ┌──────────────────────────────────┐    ┌──────────────────────────────────┐   │
│  │ Local Service Provider           │    │ Remote Provider (Multi-Hop Mesh) │   │
│  │ (In-process / Loopback / IPC)    │    │ (MessageType::Service = 8)       │   │
│  └──────────────────────────────────┘    └──────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Core Objective & Non-Invasive Browser Integration

A user on a laptop, tablet, or smartphone connected to a local FIELDNET node must be able to open any standard browser and navigate to:

```text
http://fieldnet.local:8080/
```
*(or port 80 if executed with appropriate system capabilities).*

### 2.1 Non-Invasive Requirements

1. **Zero Browser Modification:** No extensions, add-ons, or flags (`--disable-web-security`) required.
2. **Zero Proprietary Runtime:** No Electron wrapper, native agent, or custom executable installed on the client viewing device.
3. **Topology Blindness:** The browser never learns internal node IDs, routing tables, link-layer metrics, or transport socket descriptors.
4. **Standard Protocol Compliance:** All communication conforms to RFC 7230–7235 (HTTP/1.1), RFC 7540 (HTTP/2), RFC 6455 (WebSocket), RFC 6762 (mDNS), and RFC 6763 (DNS-SD).
5. **Human-Centric Navigation:** Users interact with logical services via URL paths (e.g., `/services/search`, `/services/maps`, `/services/files`), while the gateway transparently handles peer discovery, route optimization, and failover.

---

## 3. Important Architectural Rule: Access Layer vs. Central Web Server

A critical pitfall in decentralized systems is allowing a local gateway to morph into a bloated, centralized monolith:

```
ANTI-PATTERN (REJECTED):
Browser ──> Central FIELDNET Server ──> Monolithic Database / Monolithic Logic
```

FIELDNET strictly prohibits this anti-pattern. The **Local Web Gateway is purely an access layer and protocol translation adapter**:

```
AUTHORIZED PATTERN:
Browser
   ↓ (HTTP/WebSocket)
Local Gateway (Lightweight, Stateless Reverse Proxy & Protocol Adapter)
   ↓ (ServiceQuery via ServiceResolverApi)
Service Resolution (Phase 08-01 Registry & Ranking Engine)
   ↓ (Decentralized Multi-Hop Mesh Dispatch)
Appropriate FIELDNET Service Provider (Local Node or Remote Mesh Peer)
   ↓ (Phase 01–07 Subsystems)
Existing FIELDNET Network (RoutingTable, DTN, CAS ContentStore, CRDT Data)
```

### 3.1 Gateway Invariants

- **Stateless Gateway Processing:** The gateway holds no business logic, no domain databases, and no consensus state. It translates HTTP requests into internal service invocations and streams responses back.
- **No Direct Subsystem Bypass:** The gateway cannot bypass the `ServiceResolver` or `ServiceRegistry`. It cannot fabricate raw network packets without routing table inspection.
- **Fail-Fast Boundary:** If a requested service has no available provider in the mesh, the gateway returns a clean, typed HTTP error (e.g., `503 Service Unavailable`) with a standardized JSON problem payload.

---

## 4. mDNS / DNS-SD Architecture (RFC 6762 & RFC 6763)

To enable browser access via `fieldnet.local` without manual DNS configuration or `/etc/hosts` editing, FIELDNET embeds a standard multicast DNS responder and service discovery advertiser.

### 4.1 Networking & Multicast Parameters

| Parameter | IPv4 Value | IPv6 Value | Description |
| :--- | :--- | :--- | :--- |
| **Multicast Address** | `224.0.0.251` | `ff02::fb` | RFC 6762 link-local multicast group |
| **UDP Port** | `5353` | `5353` | Dedicated mDNS port |
| **Socket Options** | `SO_REUSEPORT` / `SO_REUSEADDR` | `SO_REUSEPORT` / `SO_REUSEADDR` | Allows co-existence with system daemons |
| **Default Record TTL** | 120 seconds | 120 seconds | Standard DNS resource record TTL |
| **Host Record TTL** | 10 seconds | 10 seconds | Fast failover for mobile/dynamic nodes |

### 4.2 Record Hierarchy Advertised by a FIELDNET Node

A FIELDNET node advertising Local Web access announces four distinct DNS record types:

```
1. PTR Record (Service Enumeration - RFC 6763):
   _fieldnet._tcp.local.   PTR   fieldnet-<node_prefix>._fieldnet._tcp.local.
   _http._tcp.local.       PTR   fieldnet-<node_prefix>._http._tcp.local.

2. SRV Record (Service Location - RFC 2782):
   fieldnet-<node_prefix>._fieldnet._tcp.local.
       Priority: 0, Weight: 0, Port: 8080, Target: fieldnet.local.

3. TXT Record (Service Metadata - RFC 6763):
   fieldnet-<node_prefix>._fieldnet._tcp.local.
       txtvers=1
       node_id=e16ef3ec7c2b...
       gateway_version=0.1.0
       services=search,maps,files,workspace

4. A / AAAA Records (Hostname Resolution - RFC 6762):
   fieldnet.local.         A      192.168.1.150  (and/or 127.0.0.1 on loopback)
   fieldnet.local.         AAAA   fe80::1a2b:3c4d:5e6f:7a8b
   fieldnet-<node_prefix>.local.  A  192.168.1.150
```

### 4.3 Multicast Interface Dynamics & Lifecycle

FIELDNET nodes operate across dynamic physical networks (Wi-Fi disconnections, cellular roaming, interface renumbering, device sleep/wake):

1. **Interface Tracking:** The mDNS engine listens on all active non-loopback network interfaces (`en0`, `wlan0`, `eth0`) plus loopback (`lo`). When system network change events occur (e.g., netlink route updates on Linux, SCDynamicStore on macOS), the mDNS engine refreshes its interface binding table.
2. **Device Sleep & Departure (Goodbye Packets):** Upon graceful shutdown or power suspend signal, the mDNS responder broadcasts "Goodbye Packets" (Resource Records with `TTL = 0`) across all active multicast sockets, instructing neighbor operating systems and browsers to immediately purge cached entries.
3. **Wake & Network Re-join:** Upon wake or IP address reassignment (DHCP renewal), the node performs a standard RFC 6762 probe sequence before broadcasting unsolicited Announce responses.

---

## 5. `fieldnet.local` Semantics & Multihost Disambiguation

What does the name `fieldnet.local` actually represent? In a local area network, multiple FIELDNET nodes may be present simultaneously. We analyze four competing architectural models:

### 5.1 Analysis of Candidate Semantic Models

| Model | Interpretation | Advantages | Disadvantages | Verdict |
| :--- | :--- | :--- | :--- | :--- |
| **Model A** | The current local physical machine running the browser. | Simple, intuitive for single-node setups. | Breaks when the browser is on a phone and the node is on a separate Raspberry Pi/laptop on the LAN. | Inadequate for distributed LANs. |
| **Model B** | The unified FIELDNET environment as a virtual entity. | Conceptually elegant; aligns with mesh vision. | Browsers resolve DNS to physical IP addresses; browsers do not speak overlay protocols. | Conceptually sound, but requires concrete IP binding. |
| **Model C** | A gateway service discovered on the local network. | Standard DNS-SD semantics; works across LAN devices. | If multiple nodes advertise `fieldnet.local` without collision handling, mDNS collision occurs. | Close, but needs collision resolution. |
| **Model D** | A hybrid access point model with deterministic fallback. | Combines local loopback access, LAN gateway discovery, and unique instance names. | Slightly more complex mDNS logic. | **SELECTED ARCHITECTURE** |

### 5.2 The Chosen Coherent Model: The Local Access Point Gateway

In FIELDNET, `fieldnet.local` is defined as:

> **THE LOCAL ACCESS POINT GATEWAY:**  
> `fieldnet.local` resolves to the nearest reachable FIELDNET gateway node serving the client's current physical link. Any FIELDNET gateway node acts as a transparent portal into the entire distributed mesh.

#### Handling Multiple Nodes on the Same Subnet:

1. **Loopback Priority:** On a machine running both a FIELDNET node and a web browser, the browser resolves `fieldnet.local` to `127.0.0.1` via local hosts/mDNS loopback shortcut, routing traffic directly to the local node.
2. **Unique Hostname Disambiguation (RFC 6762 Probing):**
   - Every node claims primary alias: `fieldnet.local`.
   - Every node ALSO announces its unique canonical host: `fieldnet-<short_node_id>.local` (e.g., `fieldnet-e16ef3.local`).
   - If two nodes on the same LAN detect an A-record collision for `fieldnet.local`, they participate in RFC 6762 Conflict Resolution:
     - The node with lexicographically lower `NodeId` yields `fieldnet.local` and falls back to `fieldnet-<short_node_id>.local`.
     - Crucially, **it does not matter which gateway the browser connects to**. Because every gateway is backed by the Phase 08-01 `ServiceResolver` and Phase 05 `RoutingTable`, connecting to Gateway A or Gateway B grants identical access to all services across the entire mesh.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ LAN Subnet (192.168.1.0/24)                                                     │
│                                                                                 │
│   Client Phone / Browser                                                        │
│   Navigates to "http://fieldnet.local:8080"                                     │
│            │                                                                    │
│            ├── mDNS Query: "Who is fieldnet.local?"                             │
│            └── Responded by Node A (192.168.1.50) [Primary Gateway]             │
│                                                                                 │
│   Node A (192.168.1.50)                          Node B (192.168.1.60)          │
│   - mDNS: fieldnet.local                         - mDNS: fieldnet-b92a4f.local  │
│   - LocalWebGateway                              - LocalWebGateway              │
│   - ServiceRegistry (Mesh-wide)                  - ServiceRegistry (Mesh-wide)  │
│            │                                              ▲                     │
│            │ Forward RPC (Overlay MessageType::Service=8) │                     │
│            └──────────────────────────────────────────────┘                     │
│              (If requested service is hosted on Node B)                         │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Local Web Gateway Architecture & Subsystem Boundaries

The `LocalWebGateway` is a first-class subsystem residing in `crates/fieldnet-service/src/gateway/`.

### 6.1 Subsystem Responsibilities

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            LocalWebGateway Subsystem                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│ 1. Embedded HTTP Engine (Hyper/Axum):                                           │
│    - Listens on configured TCP port (default 8080).                             │
│    - Connection limits, request body bounds, and HTTP/1.1 & HTTP/2 negotiation. │
│ 2. Security Middleware:                                                         │
│    - Host header verification & DNS rebinding defense.                          │
│    - CORS enforcement & CSRF validation.                                        │
│    - Browser Session authentication & pairing token verification.               │
│ 3. Dispatch Router:                                                             │
│    - Static UI Dashboard & Management Console (`/`).                            │
│    - Browser Service Catalog API (`/api/v1/services`).                          │
│    - Phase 07 CAS Content Delivery (`/api/v1/content/<content_id>`).            │
│    - Dynamic Service Reverse Proxy (`/services/<service_id>/*`).                │
│ 4. Protocol Adapters:                                                           │
│    - HTTP-to-Overlay RPC translator (transforms HTTP request into ServiceRpc).  │
│    - WebSocket Upgrade & Framing bridge.                                        │
│    - Chunked response streaming & HTTP 206 Range responder.                    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 6.2 Strict Subsystem Boundary Enforcements

To prevent architectural degradation, strict negative boundaries are enforced:

| Gateway MUST NOT Implement | Owning Subsystem | Rationale |
| :--- | :--- | :--- |
| **Mesh Routing & Path Selection** | `fieldnet-runtime::routing::RoutingTable` | Gateway does not maintain network links or distance vectors. |
| **Peer Discovery & Handshakes** | `fieldnet-network::DiscoveryService` | Discovery is handled via authenticated UDP/TCP transport. |
| **Cryptographic Hash Computation**| `fieldnet-content::hashing::ContentHasher`| Gateway consumes existing `ContentId` CAS primitives. |
| **Data Synchronization & CRDTs** | `fieldnet-data::sync::SyncEngine` | Document replication is completely decoupled from HTTP views. |
| **Sovereign Key Operations** | `fieldnet-identity::NodeIdentity` | Gateway never signs mesh messages using browser session tokens. |
| **Service Scheduling & Workers** | `fieldnet-service::registry::ServiceRegistry` | Gateway is a consumer/client of services, not their supervisor. |

---

## 7. HTTP Request Lifecycle & Safe vs. Unsafe Request Semantics

Every incoming HTTP request traverses a deterministic 10-step lifecycle:

```
[Browser Client]
       │
       ▼ (1. TCP Connect & TLS/Plaintext HTTP Request)
[Gateway: Security Filter] ──> Reject if Host header invalid or body > max_body_size
       │
       ▼ (2. Session & Auth Verification) ──> Reject if missing session cookie/token (401)
[Gateway: URL Parsing]
       │
       ▼ (3. Route Classification: System API vs Content CAS vs Service Proxy)
[Gateway: Service Route Match] (/services/<service_id>/<subpath>)
       │
       ▼ (4. Query ServiceResolver::resolve(service_id))
[ServiceResolver] ──> Return optimal ServiceRecord (or 503 if unavailable)
       │
       ▼ (5. Check Service Permissions against Caller Session) ──> Reject if unauthorized (403)
[Gateway: Request Idempotency & Safety Classification]
       │
       ├─────────────────────────────────┬─────────────────────────────────┐
       ▼                                 ▼                                 ▼
 [Safe / Idempotent: GET, HEAD]    [State-Changing: POST/PUT]        [State-Changing with]
 [Retryable on Provider Failure]   [NO BLIND RETRY on Timeout]      [Idempotency-Key Header]
       │                                 │                          [Retryable with Key]
       └─────────────────────────────────┼─────────────────────────────────┘
                                         │
                                         ▼ (6. Dispatch Request)
                   ┌───────────────────────────────────────────┐
                   │ Local Endpoint: Local TCP / IPC In-Process│
                   │ Remote Endpoint: MessageType::Service = 8 │
                   └─────────────────────┬─────────────────────┘
                                         │
                                         ▼ (7. Execution / Stream Wait)
                                 [Provider Response]
                                         │
                                         ▼ (8. Status & Header Translation)
                                         ▼ (9. Stream Body to Browser)
                                         ▼ (10. Telemetry & Metric Recording)
                                   [Browser Client]
```

### 7.1 Safe vs. Unsafe Request Handling

To protect against duplicate side-effects (e.g., accidental duplicate transactions, duplicated comments, repeated operations):

1. **Safe Idempotent Requests (`GET`, `HEAD`, `OPTIONS`):**
   - If the selected provider drops the connection before returning HTTP headers, the gateway **automatically falls back** to the next-ranked provider in the `ServiceRegistry`.
   - Up to 2 retries on alternate providers are permitted within the global request timeout.
2. **State-Changing Requests (`POST`, `PUT`, `DELETE`, `PATCH`):**
   - **Default Policy (No Blind Retry):** If a network timeout or provider disconnection occurs after the request payload has been dispatched, the gateway **MUST NOT** retry on an alternate provider. Doing so could duplicate state-changing side-effects.
   - The gateway returns `504 Gateway Timeout` or `502 Bad Gateway` with a JSON payload indicating `EXECUTION_INDETERMINATE`.
   - **Idempotency-Key Protocol:** If the browser includes an `Idempotency-Key: <uuid>` header, the gateway passes this key into the service RPC. If a retry is needed, the alternate provider checks the shared Phase 06 CRDT / document state to verify if the idempotency key was already committed.

### 7.2 Timeouts, Cancellation & Correlation

- **`X-Request-ID`:** Generated by gateway (UUIDv4) if absent.
- **`X-Correlation-ID`:** Propagated from browser or synthesized; injected into all downstream overlay RPC envelopes and log entries.
- **Client Cancellation:** If the browser closes the TCP connection mid-request, the gateway immediately cancels its internal Tokio async task and transmits a `ServiceCancel` signal to the provider to halt wasted compute.
- **Default Timeouts:** 
  - Header timeout: 5,000 ms.
  - Service resolution timeout: 1,500 ms.
  - Total request timeout: 30,000 ms (configurable per service via `ServiceManifest`).

---

## 8. Service URL Model & Path Namespace Hierarchy

The Local Web Gateway defines a clean, hierarchical URL namespace that avoids coupling browser paths to node IDs or physical hardware addresses.

```
http://fieldnet.local:8080/
├── /                                    (System Dashboard & Gateway UI)
├── /api/v1/
│   ├── /api/v1/status                   (Node status, mesh connectivity, peer count)
│   ├── /api/v1/services                 (Browser-safe service catalog)
│   ├── /api/v1/session/pair             (Browser pairing & authentication endpoint)
│   └── /api/v1/content/<content_id>     (Direct Phase 07 CAS content streaming)
└── /services/
    ├── /services/<service_id>/          (Reverse proxy mount point for logical service)
    │   ├── /services/search/query?q=...
    │   ├── /services/maps/tiles/12/3/4.png
    │   └── /services/workspace/collab
    └── /ws/
        └── /ws/services/<service_id>    (WebSocket upgrade endpoint for real-time services)
```

### 8.1 Path Rewriting & Prefix Stripping

When the gateway proxies an HTTP request to a resolved service instance:
- Request to: `http://fieldnet.local:8080/services/maps/tiles/10/20.png`
- The gateway strips the `/services/maps` mount prefix.
- The downstream service provider receives: `GET /tiles/10/20.png` with standard headers plus:
  - `X-Forwarded-For: 127.0.0.1`
  - `X-Forwarded-Proto: http`
  - `X-Fieldnet-Service-Id: fieldnet.maps`
  - `X-Fieldnet-Caller-Session: <session_id>`

---

## 9. Browser-Facing Service Discovery (`/api/v1/services`)

The browser discovers available mesh capabilities via `GET /api/v1/services`.

### 9.1 Information Sanitization & Leak Prevention

The browser is an untrusted environment. The gateway strictly redacts sensitive internal data:

```
STRICTLY FORBIDDEN IN BROWSER RESPONSES:
- Raw NodeId Ed25519 public/private keys of other peers
- Internal IP addresses and physical TCP port numbers of providers
- Distance-vector routing tables and next-hop metrics
- DTN storage quotas and private cryptographic auth tags
- Unprocessed error stack traces or operating system paths
```

### 9.2 Browser-Safe Service Catalog JSON Schema

```json
{
  "schema_version": 1,
  "gateway_node": "e16ef3ec...",
  "timestamp": 1727512800000,
  "services": [
    {
      "service_id": "fieldnet.search",
      "display_name": "Mesh Global Search",
      "description": "Distributed keyword search across documents and media",
      "version": "1.2.0",
      "icon_url": "/api/v1/content/a7f8e32...",
      "mount_path": "/services/search",
      "status": "Available",
      "capabilities": ["keyword_index", "stemming", "fast_rank"],
      "protocols": ["Http", "WebSocket"],
      "is_public": true
    },
    {
      "service_id": "fieldnet.maps",
      "display_name": "Tactical Offline Maps",
      "description": "Vector tile provider for tactical navigation",
      "version": "2.0.1",
      "icon_url": "/api/v1/content/bc43190...",
      "mount_path": "/services/maps",
      "status": "Available",
      "capabilities": ["vector_tiles", "elevation_contours"],
      "protocols": ["Http"],
      "is_public": true
    }
  ]
}
```

---

## 10. WebSocket Architecture & Bidirectional Service Streaming

Standard web applications require low-latency, bidirectional streaming for real-time document collaboration (Phase 06 CRDT sync), live chats, telemetry dashboards, and streaming AI tokens.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Browser (JavaScript WebSocket)                                                  │
│   ws://fieldnet.local:8080/ws/services/workspace.collab                         │
└───────────────────────────────────────┬─────────────────────────────────────────┘
                                        │ 1. HTTP/1.1 Upgrade Request (Sec-WebSocket-Key)
                                        │    Origin & Session Cookie Verified
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ LocalWebGateway: WebSocket Handler                                              │
│   - Performs RFC 6455 handshake (101 Switching Protocols).                      │
│   - Resolves "workspace.collab" via ServiceResolver.                            │
│   - Establishes bounded bidirectional async mpsc channel to service worker.     │
└───────────────────────────────────────┬─────────────────────────────────────────┘
                                        │ 2. Bidirectional Frame Framing
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Downstream Service (Local or Remote Mesh Provider)                              │
│   - Local: Bounded Tokio channel or local loopback TCP socket.                  │
│   - Remote: Multiplexed overlay virtual circuit (SessionId + SubstreamId).      │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 10.1 Connection Lifecycle & Flow Control

1. **Authentication during Handshake:** The browser initiates the upgrade. The gateway verifies the `Origin` header and checks for a valid session token (via cookie or query param `?token=...`). If unauthorized, it rejects with `401 Unauthorized` before performing the 101 upgrade.
2. **Subprotocol Negotiation:** Supports `Sec-WebSocket-Protocol: fieldnet.v1.json` or `fieldnet.v1.binary`.
3. **Backpressure & Bounded Buffers:** To prevent slow browser clients or misbehaving mesh peers from causing memory exhaustion:
   - Each WebSocket session is allocated a bounded channel (default: 64 messages / 256 KB max buffer).
   - If the downstream consumer cannot keep up, the gateway stops reading frames from the TCP socket, applying natural TCP window backpressure to the sender.
4. **Heartbeats & Disconnects:** Gateway issues WebSocket `Ping` frames every 15 seconds. If no `Pong` is received within 10 seconds, the connection is aborted.
5. **Provider Disconnection / Migration:** If the remote provider node crashes during an active WebSocket session:
   - The gateway emits a standardized control frame: `{"type": "SERVICE_MIGRATING", "reason": "PROVIDER_OFFLINE"}`.
   - The gateway closes the socket with WebSocket close code `1012 (Service Restart)`.
   - The client application's reconnect logic triggers a reconnect to `/ws/services/<service_id>`, causing the gateway to re-resolve to an alternate provider.

---

## 11. Browser Authentication Boundary & Sovereign Key Isolation

A fundamental security principle of FIELDNET is:

> **THE SOVEREIGN KEY ISOLATION PRINCIPLE:**  
> The 32-byte Ed25519 sovereign private key of a FIELDNET node is NEVER exposed to JavaScript, web browsers, cookies, or browser LocalStorage.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ TIER 1: SOVEREIGN NODE IDENTITY (Physical Machine)                              │
│ - Keypair: Ed25519 (Private Key stored in chmod 600 identity.json)              │
│ - Used for: Mesh AEAD handshakes, wire message signing, DHT proofs.             │
│ - Boundary: Strictly confined to Rust runtime memory.                           │
└───────────────────────────────────────┬─────────────────────────────────────────┘
                                        │ Isolated by Gateway Boundary
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ TIER 2: BROWSER SESSION IDENTITY (Ephemeral Client)                             │
│ - Token: 256-bit CSPRNG high-entropy string (HMAC-SHA256 authenticated)        │
│ - Stored: HttpOnly, SameSite=Strict browser cookie (`fieldnet_session`)         │
│ - Lifecycle: Bound to client User-Agent and loopback/LAN IP; revocable anytime.  │
│ - Capabilities: Carries fine-grained service access grants (read/write ACLs).   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 11.1 Browser Pairing Workflow

When an unauthenticated browser first visits `http://fieldnet.local:8080`:

1. **Unauthenticated Redirect:** The browser is redirected to `/pairing.html`.
2. **Pairing Challenge:** The node console/terminal or local system UI displays a temporary 6-digit PIN or QR code (valid for 120 seconds).
3. **User Approval:** The user enters the PIN into the browser.
4. **Session Minting:** The gateway verifies the PIN, mints a `BrowserSession`, and sets a secure cookie:
   ```http
   Set-Cookie: fieldnet_session=s_8f9c1d2e...; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400
   ```
5. **Localhost Exemption:** For browsers running strictly on `127.0.0.1` / `localhost` on the same physical host, an optional configuration (`auto_pair_localhost: true`) can automatically grant standard local permissions without PIN entry.

---

## 12. Browser Security Model, CORS & CSRF Defense

Because the gateway exposes HTTP ports on the local network, robust defenses against cross-site attacks are mandatory.

### 12.1 Host Header Validation & DNS Rebinding Defense

Malicious websites on the public Internet frequently attempt **DNS Rebinding Attacks** to access internal LAN services (resolving a public attacker domain to `127.0.0.1` or `192.168.1.x`).

- **Mitigation:** The gateway inspects the HTTP `Host` header on EVERY incoming request.
- **Allowed Hosts Whitelist:**
  - `fieldnet.local`, `fieldnet.local:*`
  - `fieldnet-*.local`, `fieldnet-*.local:*`
  - `localhost`, `localhost:*`
  - `127.0.0.1`, `127.0.0.1:*`
  - `[::1]`, `[::1]:*`
  - The node's authenticated primary LAN IP address (e.g., `192.168.1.150:*`).
- Any request with an unlisted `Host` header (e.g., `Host: attacker.com`) is **immediately aborted with HTTP 400 Bad Request**.

### 12.2 CORS & Origin Validation

1. **Origin Verification:** For cross-origin requests and WebSocket upgrades, the `Origin` header must match the whitelisted hostnames.
2. **Strict Headers:**
   ```http
   Access-Control-Allow-Origin: http://fieldnet.local:8080
   Access-Control-Allow-Credentials: true
   Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS
   Access-Control-Allow-Headers: Content-Type, Authorization, X-Request-ID, Idempotency-Key
   ```
3. **No Wildcards:** `Access-Control-Allow-Origin: *` is strictly forbidden.

### 12.3 Anti-CSRF Architecture

To prevent malicious third-party websites from tricking an authenticated browser into issuing state-changing requests:
1. **Cookie Partitioning:** Session cookies are marked `SameSite=Strict`.
2. **Custom Request Header Requirement:** All state-changing requests (`POST`, `PUT`, `DELETE`) require a custom header:
   ```http
   X-Fieldnet-Client: web-gateway
   ```
   Standard HTML form submissions (`<form method="POST">`) cannot set custom headers, defeating cross-site form forgery.

### 12.4 No Universal Open Proxying

The gateway **MUST NEVER** act as a general-purpose forward proxy:
- URL paths are mapped strictly to registered `ServiceId`s or `/api/v1/*` routes.
- The gateway never accepts arbitrary external URLs (e.g., `GET http://192.168.1.1/admin` is flatly rejected).

---

## 13. Static Content Delivery via Phase 07 CAS Integration

Modern decentralized web apps require HTML, CSS, JavaScript, WebAssembly binaries, fonts, and images. FIELDNET avoids creating a second file-serving subsystem by integrating directly with the **Phase 07 Content System**.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Browser                                                                         │
│   GET /api/v1/content/4a5b6c7d8e9f... (or /services/maps/app.js)                │
└───────────────────────────────────────┬─────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ LocalWebGateway Content Handler                                                 │
│   1. Extracts ContentId from URL.                                               │
│   2. Queries Phase 07 ContentStore::get(content_id).                            │
│      ├── If locally present: Opens async reader stream.                         │
│      └── If remote: Dispatches TransferManager::request_content(content_id).    │
│   3. Detects MIME type from magic bytes or Manifest metadata.                    │
│   4. Emits HTTP 200 / 206 with aggressive caching headers.                      │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 13.1 Immutable Caching & ETag Semantics

Because Phase 07 `ContentId` is the cryptographic SHA-256 digest of the underlying content:
- **ETag:** The hexadecimal `ContentId` serves as the exact, immutable `ETag`.
- **Cache-Control:**
  ```http
  Cache-Control: public, max-age=31536000, immutable
  ETag: "4a5b6c7d8e9f0123456789abcdef..."
  ```
- **Conditional Requests:** If the browser presents `If-None-Match: "4a5b6c..."`, the gateway immediately answers with `304 Not Modified` without reading disk bytes.

---

## 14. High-Throughput Streaming & HTTP Range Requests

Large file downloads (video recordings, map datasets, AI weights, container images) must stream efficiently through the gateway without RAM buffer explosion.

### 14.1 Zero-Buffer-Explosion Streaming

- **Chunked Transfer Encoding:** The gateway streams responses via Tokio's asynchronous `AsyncRead` abstractions in fixed 64 KB buffers.
- **Bounded In-Flight Memory:** Regardless of whether the payload is 100 KB or 100 GB, memory consumption per connection is bounded to at most $2 \times 64\text{ KB} = 128\text{ KB}$.

### 14.2 HTTP 206 Partial Content (Range Requests)

Video playback and resumable downloads require standard byte-range support:

```http
Request:
  GET /api/v1/content/4a5b6c... HTTP/1.1
  Range: bytes=1048576-2097151

Gateway Response:
  HTTP/1.1 206 Partial Content
  Content-Range: bytes 1048576-2097151/52428800
  Content-Length: 1048576
  Content-Type: video/mp4
  Accept-Ranges: bytes

  <binary payload stream>
```

The gateway maps byte offsets directly to Phase 07 256 KB chunk boundaries (`start_chunk = offset / 262144`), streaming only the necessary chunks directly from `ChunkStore`.

---

## 15. Provider Failure Handling & Resilient Failover

What happens when a browser is interacting with a service provider node that abruptly powers off, crashes, or roams out of range?

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Browser ──> LocalWebGateway ──> [Provider Node A] (Crashes / Disconnects)        │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 15.1 Failure State Matrix During Request Execution

| In-Flight State | Failure Detection | Gateway Action | Browser Visibility |
| :--- | :--- | :--- | :--- |
| **1. Request Not Dispatched** | Service resolution returns empty or provider route lost before send. | Re-queries `ServiceResolver`; selects next-ranked provider. | Completely transparent; slight latency increase (<50ms). |
| **2. Request Dispatched, No Headers Received (Safe GET)** | TCP socket closed by peer or overlay timeout expires. | Re-resolves service, penalizes Provider A in health tracker, reroutes GET to Provider B. | Completely transparent failover. |
| **3. Request Dispatched, No Headers Received (Unsafe POST without Idempotency Key)** | TCP disconnect or timeout. | **DOES NOT RETRY.** Marks Provider A degraded. Returns `504 Gateway Timeout` with `{"error": "EXECUTION_INDETERMINATE"}`. | Client application notified; must decide whether to re-submit. |
| **4. Request Dispatched, No Headers Received (Unsafe POST WITH Idempotency Key)** | TCP disconnect or timeout. | Re-dispatches to Provider B with identical `Idempotency-Key`. Provider B checks CRDT deduplication. | Transparent and safe retry. |
| **5. Response Partially Streamed to Browser** | Mid-stream socket disconnection. | Closes HTTP stream abruptly (chunked framing incomplete). Cannot restart mid-body without Range. | Browser detects stream truncation; client resumes via HTTP Range request. |

---

## 16. Multiple Gateways, LAN Dynamics & Failover Semantics

In a tactical LAN, multiple nodes may run `LocalWebGateway` instances on port 8080:

```
[Phone / Laptop Browser]
           │
           ├──────────────────────────────┐
           ▼ (mDNS: fieldnet.local)       ▼ (Failover mDNS)
┌──────────────────────┐       ┌──────────────────────┐
│ Node A: Gateway      │       │ Node B: Gateway      │
│ 192.168.1.50:8080    │       │ 192.168.1.60:8080    │
└──────────────────────┘       └──────────────────────┘
```

### 16.1 Browser Reconnection & Client Failover

Browsers do not support transparent live TCP socket migration between different IP addresses. Therefore:
1. **mDNS Fast TTL:** Host A-records for `fieldnet.local` use a short 10-second TTL.
2. **Node A Disappearance:** If Node A crashes:
   - Node A ceases mDNS refresh.
   - Node B detects Node A's disappearance via Phase 04 keep-alives and claims primary `fieldnet.local` ownership.
   - The browser's next request or page refresh triggers an mDNS query, resolving `fieldnet.local` to Node B (`192.168.1.60`).
3. **Gateway UI Reconnect Polling:** The static Web UI includes a lightweight client-side reconnection loop:
   - If an active HTTP/WebSocket call fails, a background JavaScript worker pings `/api/v1/status` with exponential backoff (500ms, 1s, 2s, 5s).
   - Once the new gateway responds, the UI seamlessly reloads active service views.

---

## 17. Network Partition Behavior & Mesh Convergence

Tactical networks experience frequent splits and merges:

```
BEFORE PARTITION:
[Cluster A: Node 1 (Gateway), Node 2] <==== Link ====> [Cluster B: Node 3 (Gateway), Node 4]
Both clusters resolve all services across entire 4-node mesh.

DURING PARTITION (Link Severed):
[Cluster A: Node 1, Node 2]                        [Cluster B: Node 3, Node 4]
- Gateway 1 advertises fieldnet.local               - Gateway 3 advertises fieldnet.local
  (on Subnet A)                                      (on Subnet B)
- Services on Nodes 3 & 4 transition to Unavailable - Services on Nodes 1 & 2 transition to Unavailable
- Browser on Subnet A sees Cluster A services        - Browser on Subnet B sees Cluster B services

PARTITION HEALS (Link Restored):
- Routing tables exchange distance vectors.
- ServiceRegistries exchange signed ServiceManifests.
- Service catalogs expand automatically; browsers receive real-time updates via WebSocket event feed.
```

### 17.1 Consistency Guarantees Across Partitions

- **Read Operations:** Browsers read local/partitioned state with zero coordination delay.
- **Write Operations:** Writes are accepted locally using Phase 06 CRDT / DocumentStore primitives or Phase 07 immutable ContentStore blobs. Upon partition healing, state merges automatically via deterministic CRDT rules without split-brain corruption.

---

## 18. Service-to-Service Boundary (Internal RPC vs. External HTTP)

A vital architectural boundary exists between human-facing browser interactions and internal mesh operations:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ HUMAN BROWSER BOUNDARY (External)                                               │
│ - Protocol: HTTP/1.1, HTTP/2, WebSocket over TCP socket.                        │
│ - Purpose: Human user interface, browser client compatibility, REST/JSON.       │
│ - Gateway role: Terminates HTTP, enforces browser security, parses JSON.        │
└───────────────────────────────────────┬─────────────────────────────────────────┘
                                        │ Gateway Boundary
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ SERVICE-TO-SERVICE MESH BOUNDARY (Internal)                                     │
│ - Protocol: Native FIELDNET Message Envelope (MessageType::Service = 8).        │
│ - Transport: ChaCha20-Poly1305 AEAD encrypted multi-hop TCP overlay.            │
│ - Framing: Compact binary serialization (bincode / canonical signing).          │
│ - Routing: Distance-vector multi-hop routing table + DTN store-and-forward.     │
│ - Identity: Sovereign NodeId and cryptographic capability tokens.               │
└─────────────────────────────────────────────────────────────────────────────────┘
```

Services communicating with other services **NEVER** route through the HTTP gateway. They invoke the native `ServiceResolverApi` and dispatch typed `ServiceRpc` envelopes directly across the mesh.

---

## 19. Observability, Distributed Tracing & Telemetry

The Local Web Gateway integrates seamlessly into the existing `tracing` and metrics runtime without duplicate logging infrastructure.

### 19.1 Correlation ID Propagation

```
Browser Request
  (X-Correlation-ID: c_7a8b9c...)
        │
        ▼
LocalWebGateway [Span: http_request, req_id: r_123, corr_id: c_7a8b9c]
        │
        ▼ Injects correlation_id into ServiceRpcRequest payload
ServiceResolver [Span: service_resolve, service_id: "maps.service"]
        │
        ▼
RoutingTable / Network [Span: overlay_dispatch, dest_node: e16ef3...]
        │
        ▼
Provider Node [Span: execute_service, corr_id: c_7a8b9c]
```

### 19.2 Core Gateway Metrics

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `gateway_http_requests_total` | Counter | `method`, `status_code`, `service_id` | Total requests processed |
| `gateway_http_duration_ms` | Histogram | `service_id`, `route_type` | End-to-end request latency |
| `gateway_active_connections` | Gauge | `protocol` (http, ws) | Current open TCP sockets |
| `gateway_ws_sessions_active` | Gauge | `service_id` | Current active WebSockets |
| `gateway_bytes_streamed_total` | Counter | `direction` (in, out), `content_type` | Total bandwidth through gateway |
| `gateway_provider_failovers_total`| Counter| `service_id`, `reason` | Automated retries on alternate peers |
| `gateway_auth_failures_total` | Counter | `reason` (host, cors, csrf, token) | Security rejections |

---

## 20. Comprehensive Security Threat Model (14 Vectors)

| # | Threat Vector | Attack Surface | Mitigation Strategy | Remaining Limitation | Automated Test Strategy |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | **Malicious Local Browser (XSS)** | Rogue script executing in browser context tries to steal sovereign node key. | Node private key is never stored in browser; browser only holds ephemeral session cookie with `HttpOnly` and `SameSite=Strict`. | Script can execute actions within the granted session permissions. | Inject script; verify `document.cookie` cannot read session and no node key API exists. |
| **2** | **Malicious Service in Mesh** | Rogue node advertises `fieldnet.search` to intercept user search queries. | Manifests require Ed25519 signature; `ServiceResolver` filters by trust tier; clients can specify required provider key. | Untrusted public services can receive queries if caller has no trust constraints. | Register untrusted service; verify resolver filters by `TrustTier`. |
| **3** | **DNS / mDNS Spoofing on LAN** | Attacker on LAN injects fake mDNS replies claiming `fieldnet.local`. | Gateway requires Host header validation; pairing tokens prevent unauthorized access; TLS optional on local gateway. | Cleartext HTTP vulnerable to active ARP/DNS spoofing on untrusted LAN. | Simulate conflicting mDNS reply; verify collision detection and tie-breaking. |
| **4** | **Service Impersonation** | Provider claims to be instance `X` created by another node. | `InstanceId` verification checks Ed25519 signature against manifest declaring `provider_node_id`. | None; signature verification is fail-closed. | Forge manifest signature; verify immediate rejection on ingest. |
| **5** | **Gateway Impersonation** | Attacker sets up fake gateway on port 8080. | Terminal pairing PIN verifies physical ownership; browser session bound to node's public key hash. | User must verify pairing PIN out-of-band. | Connect without pairing PIN; verify 401 Unauthorized rejection. |
| **6** | **Request Forgery (CSRF)** | Third-party website triggers state-changing POST on `fieldnet.local`. | `SameSite=Strict` cookies + mandatory custom header `X-Fieldnet-Client: web-gateway`. | Browser bugs in cookie SameSite implementation. | Send cross-origin POST from external HTML page; verify 403 Forbidden. |
| **7** | **WebSocket Hijacking** | Malicious site opens WebSocket to `ws://fieldnet.local:8080/ws/...`. | Gateway validates `Origin` header during handshake; rejects mismatches before upgrade. | None. | Initiate WS handshake with `Origin: http://evil.com`; verify 403 response. |
| **8** | **Token Theft (Session Token)** | Physical attacker copies session token from disk/RAM. | Session tokens are bound to client IP, User-Agent, and expire after 24 hours. Tokens can be revoked instantly. | Stolen token valid until expired or revoked. | Replay session token from different IP; verify rejection. |
| **9** | **Stale Service Advertisements** | Old manifests advertise services that have stopped. | Monotonic sequence enforcement + TTL expiration sweep every 5 seconds. | Window of up to TTL seconds if provider crashes without sending goodbye. | Kill provider; verify status transitions to `Unavailable` after missed heartbeat. |
| **10**| **Unauthorized Service Access**| Public user invokes private admin service. | `ServiceAuthorizer` enforces `requires_capability_token` and role-based ACLs before routing. | Misconfigured ACL by service author. | Invoke private service without capability token; verify 403 Forbidden. |
| **11**| **Malicious Content Injection** | Attacker stores malicious HTML/JS in Phase 07 CAS. | CAS content served with `Content-Security-Policy: default-src 'self'` and `X-Content-Type-Options: nosniff`. | Stored XSS in application if app evaluates untrusted inputs. | Request CAS file; inspect HTTP response headers for strict CSP and nosniff. |
| **12**| **Oversized HTTP Requests** | Attacker streams 10 GB POST to exhaust gateway RAM. | Request body bounds enforced by Hyper/Axum middleware (default max 10 MB for API, streaming reader for CAS). | Legitimate large uploads must use chunked CAS API. | Send 50 MB body to standard API endpoint; verify `413 Payload Too Large`. |
| **13**| **Connection Exhaustion (Slowloris)**| Attacker opens 10,000 idle TCP connections to port 8080. | Gateway connection limits (max 512 concurrent sockets) + strict header read timeout (5 seconds). | Extreme DoS can degrade gateway for legitimate local clients. | Open connection and hold idle; verify timeout drops socket after 5s. |
| **14**| **Bandwidth Exhaustion** | Client repeatedly requests large CAS blobs. | Bounded streaming channels + per-session token bucket rate limiter (e.g., 20 MB/s). | Local loopback traffic bypasses rate limits by configuration. | Blast high-rate requests; verify token bucket enforces rate limit (429). |

---

## 21. Operating System & Platform Constraints

FIELDNET runs across diverse operating environments. The gateway and mDNS architectures account for specific platform behaviors:

### 21.1 Platform Comparison Matrix

| Platform | mDNS Stack | Port 8080 / 80 Binding | Multicast Networking Caveats | Background Execution |
| :--- | :--- | :--- | :--- | :--- |
| **macOS** | Native `mDNSResponder` / Apple Bonjour. | Port 80 requires root; 8080 unprivileged. | Requires `SO_REUSEPORT` to co-exist with Apple Bonjour daemon on UDP 5353. | App Nap can throttle timers; requires `NSProcessInfo` latency activity. |
| **Linux** | `systemd-resolved` or `avahi-daemon`. | Port 80 requires `CAP_NET_BIND_SERVICE`; 8080 unprivileged. | Netlink route monitoring tracks interface IP changes. `SO_REUSEPORT` supported. | Standard Linux daemon execution; systemd service. |
| **Windows** | Windows 10/11 native mDNS or embedded responder. | Port 8080 unprivileged. Firewall prompt on first bind. | UDP multicast socket binding behavior requires explicit interface selection. | Windows Services; background tasks unaffected. |
| **Android** | Android NSD / Embedded socket. | Ports < 1024 forbidden. Dynamic high ports (8080). | **Wi-Fi Multicast Lock required:** Must acquire `WifiManager.MulticastLock` or multicast packets are silently dropped by kernel. | Doze Mode freezes background sockets. Loopback `127.0.0.1` fallback mandatory. |
| **iOS** | Embedded Bonjour / Network.framework. | Strict sandbox. High ports only. | **Multicast Entitlement required:** `com.apple.developer.networking.multicast` needed for raw UDP 5353. Local Network Privacy prompt. | Background execution heavily restricted; gateway suspends when app backgrounded. |

---

## 22. Rust Module Mapping (`crates/fieldnet-service`)

The Local Web Gateway components are organized inside `crates/fieldnet-service/src/gateway/`:

```
crates/fieldnet-service/src/gateway/
├── mod.rs                  # Subsystem facade, GatewayService lifecycle & start/stop
├── config.rs               # GatewayConfig (listen_addr, port, max_body, timeouts, CORS)
├── server.rs               # Embedded HTTP/WS engine (Axum / Hyper integration)
├── mdns.rs                 # RFC 6762 / 6763 mDNS responder & DNS-SD announcer
├── router.rs               # Dispatch router (Dashboard, API, CAS Content, Services)
├── session.rs              # BrowserSessionStore, pairing PIN generator, cookie management
├── auth.rs                 # Host validation, CORS middleware, CSRF header verification
├── proxy.rs                # HTTP-to-Overlay ServiceRpc request dispatcher & translation
├── ws.rs                   # WebSocket upgrade handler, bidirectional framing bridge
├── streaming.rs            # Chunked transfer streamer & HTTP 206 Range request responder
├── discovery_api.rs        # Browser-safe /api/v1/services catalog generator
└── metrics.rs              # Prometheus/Telemetry metric instrumentation
```

---

## 23. Programmatic Rust API Contracts & Traits

```rust
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct GatewayConfig {
    pub bind_address: SocketAddr,
    pub enable_mdns: bool,
    pub mdns_hostname: String, // default: "fieldnet.local"
    pub auto_pair_localhost: bool,
    pub max_body_size_bytes: usize, // default: 10 MB
    pub request_timeout_ms: u64,    // default: 30,000 ms
    pub allowed_hosts: Vec<String>,
}

#[async_trait]
pub trait LocalWebGatewayApi: Send + Sync {
    /// Starts the HTTP/WS server and the mDNS advertiser.
    async fn start(&self) -> Result<(), GatewayError>;

    /// Gracefully shuts down the gateway, emitting mDNS goodbye packets.
    async fn stop(&self) -> Result<(), GatewayError>;

    /// Returns the active bound local listening address.
    fn local_addr(&self) -> SocketAddr;

    /// Generates a new 6-digit pairing PIN for browser session authentication.
    async fn generate_pairing_pin(&self) -> Result<String, GatewayError>;

    /// Validates a pairing PIN and mints a new BrowserSessionToken.
    async fn verify_pairing_pin(&self, pin: &str) -> Result<BrowserSessionToken, GatewayError>;
}
```

---

## 24. Authoritative State Machines

### 24.1 Local Web Gateway Subsystem State Machine

```
              ┌───────────────────────────┐
              │          STOPPED          │
              └─────────────┬─────────────┘
                            │ start() called
                            ▼
              ┌───────────────────────────┐
              │        INITIALIZING       │ (Bind TCP port, init mDNS sockets)
              └─────────────┬─────────────┘
                            │ Sockets bound & mDNS probing complete
                            ▼
              ┌───────────────────────────┐
              │          SERVING          │ ◄───┐
              └───────┬───────────┬───────┘     │
                      │           │             │ Interface change
     Client Request   │           │ Interface   │ re-bind complete
     Processed OK     │           │ Change      │
                      │           ▼             │
                      │     ┌───────────┐       │
                      │     │REBINDING  │───────┘
                      │     └───────────┘
                      │ stop() called
                      ▼
              ┌───────────────────────────┐
              │          DRAINING         │ (Emit mDNS goodbye, close idle sockets)
              └─────────────┬─────────────┘
                            │ All in-flight requests completed or timed out
                            ▼
              ┌───────────────────────────┐
              │          STOPPED          │
              └───────────────────────────┘
```

### 24.2 Browser Request Lifecycle State Machine

```
   ┌───────────┐
   │ INCOMING  │ (HTTP connection accepted)
   └─────┬─────┘
         │ Security Checks (Host, CORS, CSRF, Session)
         ├──────────────────────────────┐
         │ Passed                       │ Failed
         ▼                              ▼
   ┌───────────┐                  ┌───────────┐
   │  ROUTED   │                  │ REJECTED  │ (400, 401, 403, 413)
   └─────┬─────┘                  └───────────┘
         │ Service Resolved
         ├──────────────────────────────┐
         │ Available                    │ Not Found / Degraded
         ▼                              ▼
   ┌───────────┐                  ┌───────────┐
   │DISPATCHED │                  │UNAVAILABLE│ (503 Service Unavailable)
   └─────┬─────┘                  └───────────┘
         │ Provider Execution
         ├──────────────────────────────┬──────────────────────────────┐
         │ Success                      │ Provider Crash (Safe GET)    │ Provider Crash (POST)
         ▼                              ▼                              ▼
   ┌───────────┐                  ┌───────────┐                  ┌───────────┐
   │ STREAMING │                  │ REROUTING │                  │ FAILED    │ (504 Timeout)
   └─────┬─────┘                  └─────┬─────┘                  └───────────┘
         │ Complete                     │ Dispatches to Provider B
         ▼                              └──────────────┐
   ┌───────────┐                                       ▼
   │ COMPLETED │ (200 / 206)                     [DISPATCHED]
   └───────────┘
```

---

## 25. Comprehensive Failure Mode Matrix

| Failure Scenario | Detection Point | Gateway Reaction | Client Impact | Log / Telemetry Emission |
| :--- | :--- | :--- | :--- | :--- |
| **Port 8080 Already Bound** | `TcpListener::bind` returns `EADDRINUSE`. | Emits fatal error; attempts fallback port if configured or halts. | Gateway fails to start. | `ERROR: Gateway port 8080 already bound` |
| **mDNS Port 5353 Conflict** | UDP bind fails without `SO_REUSEPORT`. | Binds with `SO_REUSEPORT` / `SO_REUSEADDR`; falls back to unicast if OS denies. | Hostname resolution may degrade to direct IP. | `WARN: mDNS socket shared with system daemon` |
| **Target Service Unavailable** | `ServiceResolver` returns empty candidate list. | Returns HTTP 503 Service Unavailable with standard Problem Details JSON. | Browser UI displays "Service temporarily unavailable". | `INFO: Service resolution failed: service_id=X` |
| **Remote Provider Disconnects Mid-Header** | TCP socket drops while awaiting response headers. | If safe request: automatically reroutes to next provider. If unsafe: returns 504. | Safe: transparent. Unsafe: error prompt to user. | `WARN: Provider disconnected during dispatch; rerouting` |
| **Remote Provider Disconnects Mid-Stream** | Async byte reader returns unexpected EOF. | Abruptly terminates chunked HTTP response. | Browser download pauses; resumed via HTTP Range. | `WARN: Content stream interrupted mid-transfer` |
| **Client Disconnects (User Closes Tab)** | Downstream HTTP socket reset by peer. | Cancels Tokio async task; sends `ServiceCancel` to provider. | None. Wasted provider compute avoided. | `DEBUG: Client cancelled in-flight request` |
| **WebSocket Peer Crashes** | Internal mesh channel closes. | Emits WS close frame `1012 (Service Restart)` and closes TCP socket. | Browser JS detects close code 1012; initiates backoff reconnect. | `INFO: WebSocket provider closed; closed client socket` |
| **Oversized Request Upload** | Incoming bytes exceed `max_body_size_bytes`. | Immediately responds with `413 Payload Too Large` and drops socket. | Upload aborted. | `WARN: Request body exceeded limit: size=X` |
| **DNS Rebinding Attempt** | `Host` header does not match whitelisted aliases. | Aborts request with `400 Bad Request`. | Attack thwarted. | `WARN: Rejected request with invalid Host: X` |

---

## 26. Testing & Verification Architecture

To guarantee absolute production reliability without relying on unit tests alone, Phase 08 testing incorporates six distinct testing tiers:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Tier 1: Unit Tests (Isolated Logic)                                             │
│ - mDNS packet encoding/decoding (RFC 6762 A, PTR, SRV, TXT parsing).             │
│ - Host header validation & CORS origin matching logic.                          │
│ - URL route parsing and prefix stripping.                                       │
│ - HTTP Range header parsing (`bytes=100-200`, invalid syntax rejection).         │
│ - Pairing PIN generation, HMAC token hashing, and expiration checks.            │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Tier 2: Component Tests (Mocked Sockets)                                        │
│ - Mock HTTP client hitting Gateway loopback socket.                             │
│ - Verify `/api/v1/services` correctly redacts node keys and routing tables.     │
│ - Verify `/api/v1/content/<id>` serves Phase 07 CAS blobs with correct ETags.   │
│ - WebSocket upgrade loopback test: verify echo through local service mock.      │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Tier 3: Multi-Node Integration Tests (Real Sockets & Overlay Routing)          │
│ - 3-Node Topology: Browser ──> Gateway on Node A ──> Node B (Relay) ──> Node C.│
│ - Node C hosts `maps.service`. Verify browser receives HTTP 200 via 2-hop mesh. │
│ - Content Streaming: Node C hosts 10 MB CAS video; Gateway on Node A streams to │
│   HTTP client using chunked transfer with memory bounded under 256 KB.          │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Tier 4: Fault Injection & Resilience Tests                                      │
│ - Mid-Stream Provider Crash: Kill Node C during large download; verify gateway  │
│   terminates stream cleanly without hanging.                                    │
│ - Provider Failover: Node B and Node C both host `search.service`. Kill Node B  │
│   mid-request; verify gateway transparently reroutes GET query to Node C.       │
│ - Unsafe POST Idempotency: Send POST with `Idempotency-Key`; verify retry does  │
│   not duplicate state.                                                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Tier 5: Security & Fuzzing Tests                                                │
│ - DNS Rebinding: Send requests with `Host: malicious.com`; verify 400 Bad Req. │
│ - CSRF Attack: Send cross-origin POST without `X-Fieldnet-Client` header; verify│
│   403 Forbidden.                                                                │
│ - Slowloris DoS: Open 100 partial-header connections; verify timeout drops them.│
├─────────────────────────────────────────────────────────────────────────────────┤
│ Tier 6: Real Browser E2E Verification                                           │
│ - Automated headless browser (Chrome / Safari / Firefox via WebDriver/CDP).     │
│ - Resolves `fieldnet.local:8080`, performs pairing workflow, navigates catalog, │
│   loads map tiles, and connects to real-time WebSocket collab session.          │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 27. Implementation Sequence for Future Phase 08 BUILD Prompts

To construct this architecture with zero placeholders and complete test coverage:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ BUILD 01: Service Model, Manifests & Canonical Cryptographic Signing           │
│           - ServiceId, InstanceId, ServiceManifest, canonical signing bytes     │
│           - Ed25519 signing & verification, unit tests                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│ BUILD 02: Service Registry & SQLite Persistence Engine                          │
│           - ServiceRegistry, ServiceRecord, 6-state lifecycle engine            │
│           - SQLite WAL persistence (services.db), TTL eviction, capacity bounds │
├─────────────────────────────────────────────────────────────────────────────────┤
│ BUILD 03: Service Resolver & Multi-Criteria Ranking Engine                      │
│           - ServiceResolver, constraint filtering, 8-step scoring pipeline      │
│           - Swarm multi-provider resolution, failover logic                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│ BUILD 04: Multi-Hop Service Wire Protocol & Runtime Integration                 │
│           - MessageType::Service = 8, ServiceAdvertise, ServiceQuery, RPC      │
│           - Integration into FieldnetRuntime, multi-hop mesh forwarding         │
├─────────────────────────────────────────────────────────────────────────────────┤
│ BUILD 05: Local Web Gateway Core & mDNS Responder                               │
│           - RFC 6762 / 6763 mDNS responder for fieldnet.local                   │
│           - Hyper/Axum HTTP server, Host header defense, pairing session store  │
│           - Phase 07 CAS content streaming and range request responder          │
├─────────────────────────────────────────────────────────────────────────────────┤
│ BUILD 06: Service Reverse Proxy & WebSocket Streaming Bridge                    │
│           - Dynamic HTTP-to-ServiceRpc proxy (/services/<service_id>/*)         │
│           - WebSocket upgrade and bidirectional channel bridge (/ws/services/*) │
│           - Multi-node end-to-end integration and browser E2E verification      │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 28. Explicit Non-Goals for Phase 08

The following systems are strictly outside the scope of Phase 08 and are reserved for subsequent phases:
- **WebAssembly Runtime Execution:** Sandboxed WASM service execution is reserved for Phase 09 (`fieldnet-wasm`).
- **Distributed Compute Scheduling & AI Inference Engines:** Task graph scheduling and AI model execution are reserved for Phase 10 (`fieldnet-compute`).
- **Global Internet Egress Proxying / NAT Traversal:** Public Internet STUN/TURN relays and WAN gateway egress are reserved for Phase 11.
- **Native Platform UI Wrappers:** Native desktop/mobile app shells (Swift/Kotlin wrappers) are packaging concerns, not runtime core concerns.
- **Proprietary Browser Protocol Extensions:** Standard browsers accessing HTTP/1.1, HTTP/2, and WebSockets via `fieldnet.local` are strictly required.

---

# PHASE 08 — PROMPT 02 COMPLETE
