# FIELDNET Architecture Specification
## Phase 04 — Document 04: Peer Discovery, Connection Management & Multi-Transport Networking

**Document ID:** `DOC-ARCH-04-NETWORKING`  
**Phase:** Phase 04 (Peer Discovery & Multi-Transport Networking) — Prompts 01, 02, 03 (Final Pass)  
**Status:** Authoritative Architectural Specification  
**Version:** 1.2  
**References:** `FIELDNET_MASTER_SPEC.md`, `docs/architecture/01-system-model.md`, `docs/architecture/02-runtime-architecture.md`, `docs/architecture/03-security.md`  

---

## 1. Executive Summary & Scope of Peer Discovery

In standard cloud computing, network endpoints are static, highly available, and addressed through hierarchical Domain Name System (DNS) infrastructure. In FIELDNET's ad-hoc, off-grid operating environment, none of these assumptions hold. Participating nodes appear dynamically, communicate over heterogeneous physical media (Wi-Fi, Bluetooth LE, direct Ethernet), lose connectivity without warning, and possess no central directory authority.

### 1.1 The Fundamental Question of Peer Discovery
Peer Discovery in FIELDNET has a single, strictly bounded architectural responsibility:

> **THE DISCOVERY PRINCIPLE:**  
> **Peer Discovery answers:** *"Which FIELDNET nodes are currently reachable or potentially reachable?"*  
> **Peer Discovery does NOT answer:** *"Which node should receive this message?"* (That belongs strictly to the Routing Engine in Phase 05).

### 1.2 Strict Separation of the Six Networking Layers
To maintain modularity and avoid conflating physical proximity with logical routing or authorization, FIELDNET enforces an absolute separation across six networking layers:

```text
+--------------------------------------------------------------------------+
| 6. MESSAGING LAYER      : End-to-end payloads, DTN bundles, CRDT state.  |
+--------------------------------------------------------------------------+
                                     │
+--------------------------------------------------------------------------+
| 5. ROUTING LAYER        : Multi-hop path selection, next-hop decisions.  |
+--------------------------------------------------------------------------+
                                     │
+--------------------------------------------------------------------------+
| 4. AUTHENTICATION LAYER : Cryptographic Ed25519 identity verification.   |
+--------------------------------------------------------------------------+
                                     │
+--------------------------------------------------------------------------+
| 3. CONNECTION LAYER     : Active communication sessions, AEAD channels.  |
+--------------------------------------------------------------------------+
                                     │
+--------------------------------------------------------------------------+
| 2. TRANSPORT LAYER      : Physical/logical framing (TCP, BLE, Wi-Fi P2P).|
+--------------------------------------------------------------------------+
                                     │
+--------------------------------------------------------------------------+
| 1. DISCOVERY LAYER      : Proximity awareness, address resolution.       |
+--------------------------------------------------------------------------+
```

1.  **Discovery Layer:** Listens for physical radio beacons and multicasts to detect nearby nodes. Operates on unverified claims.
2.  **Transport Layer:** Implements physical byte framing, MTU segmentation, and socket communication across heterogeneous links.
3.  **Connection Layer:** Manages connection lifecycle state machines, retransmission queues, and link health heartbeats.
4.  **Authentication Layer:** Executes mutual cryptographic challenge-response handshakes to prove sovereign identity key possession (Phase 03).
5.  **Routing Layer:** Evaluates multi-hop network graphs to forward messages toward their ultimate destinations (Phase 05).
6.  **Messaging Layer:** Encapsulates application-level data structures, CRDT delta operations, and content blocks.

---

## 2. Discovery Architecture & Pipeline

Peer Discovery operates as a continuous, asynchronous processing pipeline within the runtime. It aggregates multiple discovery sources into a single coherent, deduplicated, and validated **Peer Table**.

```text
+--------------------------------------------------------------------------+
|                            DISCOVERY SOURCES                             |
|    (UDP Multicast, mDNS / DNS-SD, Bluetooth LE, Wi-Fi Direct, WebRTC)    |
+--------------------------------------------------------------------------+
                                     │ raw beacon frames
                                     ▼
+--------------------------------------------------------------------------+
|                            DISCOVERY MANAGER                             |
|  - Aggregates multi-transport beacon streams                             |
|  - Applies token-bucket rate limiting against discovery flooding         |
|  - Drops duplicate advertisements within sliding window                  |
+--------------------------------------------------------------------------+
                                     │ validated frame
                                     ▼
+--------------------------------------------------------------------------+
|                              PEER VALIDATOR                              |
|  - Validates wire protocol version compatibility                         |
|  - Enforces strict bounds on advertisement size (max 2 KB)               |
|  - Checks syntactic validity of declared NodeId and addresses            |
+--------------------------------------------------------------------------+
                                     │ sanitized peer record
                                     ▼
+--------------------------------------------------------------------------+
|                                PEER TABLE                                |
|  - In-memory authoritative registry of reachable nodes                   |
|  - Tracks observed addresses, transport types, and TTL deadlines         |
|  - Maintains discovery state machine (ACTIVE, STALE, EXPIRED)            |
+--------------------------------------------------------------------------+
                                     │
                                     ▼ emits notifications
+--------------------------------------------------------------------------+
|                         RUNTIME EVENT BUS                                |
|       (`peer.discovered`, `peer.updated`, `peer.expired`, etc.)          |
+--------------------------------------------------------------------------+
               ↙                             ↓                             ↘
+-----------------------------+ +---------------------------+ +----------------------------+
|     CONNECTION MANAGER      | |       ROUTING ENGINE      | |      SERVICE RESOLVER      |
| Evaluates auto-connect      | | Updates mesh topology     | | Updates service directory  |
| policies & starts handshake | | and link cost vectors     | | with remote capabilities   |
+-----------------------------+ +---------------------------+ +----------------------------+
```

### 2.1 Component Responsibilities
*   **Discovery Sources:** Pluggable, transport-specific drivers running under the `PlatformAdapter` abstraction. Each source listens on its physical medium and forwards decoded advertisement packets into the runtime.
*   **Discovery Manager:** The orchestrator that manages active scans, manages broadcast intervals, throttles incoming traffic to prevent CPU exhaustion, and deduplicates repeated beacons.
*   **Peer Validator:** The gatekeeper that drops malformed, oversized, or protocol-incompatible advertisements before they can allocate storage or contaminate the Peer Table.
*   **Peer Table:** The runtime's active memory ledger of all single-hop reachable nodes.
*   **Event Bus:** Propagates discovery lifecycle changes to downstream subsystems without tight coupling.

### 2.2 Heterogeneous Platform Support Variance
FIELDNET runs across diverse operating systems and form factors. The discovery architecture explicitly accommodates asymmetric hardware and platform capabilities:

| Platform | Supported Discovery Sources | Native OS Constraints & Sandboxing |
| :--- | :--- | :--- |
| **Linux (Server/Desktop)** | UDP Multicast, mDNS, BLE, Ethernet | Raw socket permissions required for some radio interfaces; full background execution. |
| **macOS (Desktop/Laptop)** | UDP Multicast, mDNS, BLE (CoreBluetooth) | Background BLE execution requires entitlement; Wi-Fi Direct unavailable (Apple AWDL proprietary). |
| **Windows 10/11** | UDP Multicast, mDNS, BLE, Wi-Fi Direct | Wi-Fi Direct supported via WinRT APIs; firewall prompts on unreserved UDP ports. |
| **Android (Mobile)** | Wi-Fi Direct (P2P), Wi-Fi Aware (NAN), BLE, UDP | Strict OS background execution limits; Wi-Fi Direct requires location permissions; aggressive battery doze. |
| **iOS / iPadOS** | BLE (CoreBluetooth), mDNS (Bonjour), WebRTC | Wi-Fi Direct unavailable; background execution heavily restricted; BLE background scanning limited. |

---

## 3. Node Advertisement Model & Specification

Nodes periodically announce their physical presence by broadcasting a compact, self-contained **Node Advertisement**.

```text
+--------------------------------------------------------------------------+
|                        NODE ADVERTISEMENT SCHEMA                         |
+--------------------------------------------------------------------------+
| Field Name             | Type          | Description                     |
+------------------------+---------------+---------------------------------+
| Node ID                | String (Hex)  | Sovereign Ed25519 public key    |
|                        |               | fingerprint (`ed25519:7F8a9B...`)|
| Protocol Version       | u16           | Wire protocol version (e.g. `1`)|
| Runtime Version        | Semantic Ver  | FIELDNET runtime version (`0.4`)|
| Advertisement Version  | u32           | Monotonic sequence per node boot|
| Supported Transports   | Bitmask (u16) | Flags: `TCP`, `UDP`, `BLE`, `P2P`|
| Reachability Info      | Array<Record> | List of physical socket/link    |
|                        |               | locators (IP, port, BLE UUID)   |
| Public Capabilities    | Bitmask (u32) | High-level role flags:          |
|                        |               | `Router`, `Storage`, `Compute`  |
| Available Services     | Array<String> | Semantic URIs of hosted public  |
|                        |               | services (e.g. `chat`, `gis`)   |
| Expiration TTL         | u16 (seconds) | Validity window (default: 60s)  |
| Signature (Optional)   | 64-byte Sig   | Ed25519 signature over payload  |
+------------------------+---------------+---------------------------------+
```

### 3.1 Information Minimization & Privacy Protection
Advertisements are broadcast over unencrypted, public radio channels. To protect operator security and operational confidentiality, the advertisement format adheres to strict privacy rules:
1.  **Zero Sensitive Data Broadcast:** Advertisements must **NEVER** contain user full names, GPS coordinates, workspace identifiers, file manifests, private encryption keys, or application payloads.
2.  **Optional Pseudonymous Rotation:** In hostile environments, nodes may rotate transient advertisement addresses (e.g., BLE MAC addresses) to prevent physical radio direction-finding tracking.
3.  **Compact Binary Serialization:** Over low-bandwidth links (BLE advertising frames limited to 31 bytes legacy or 255 bytes extended), advertisements use a compressed binary format omitting human-readable strings.

### 3.2 Advertisement Data vs. Authenticated Identity Data
> **ARCHITECTURAL INVARIANT:**  
> **A Node Advertisement is UNTRUSTED until authenticated.**

An advertisement is merely a claim: *"A node claiming to possess NodeId X is reachable at IP Y."* The receiving node must treat this claim as unverified telemetry. The node cannot invoke services, access storage, or route trusted traffic based on an advertisement alone until the Phase 03 cryptographic challenge-response handshake completes successfully.

---

## 4. Peer Table Architecture

The **Peer Table** is the authoritative, in-memory data structure maintained by the runtime to track all currently reachable physical peers.

```text
+--------------------------------------------------------------------------+
|                            PEER TABLE ENTRY                              |
+--------------------------------------------------------------------------+
|  - Node ID              : Canonical Ed25519 Public Key Fingerprint       |
|  - Observed Addresses   : Set of (TransportType, PhysicalAddress, SeenAt)|
|  - Available Transports : Bitmask of currently active transport links    |
|  - First Seen           : Monotonic timestamp of initial discovery       |
|  - Last Seen            : Monotonic timestamp of most recent beacon      |
|  - Discovery Source     : Primary interface (e.g. `UdpMulticast`)        |
|  - Authentication State : Unauthenticated | InProgress | Authenticated   |
|  - Trust State          : UNKNOWN | DISCOVERED | AUTHENTICATED | ...     |
|  - Declared Capabilities: Role bitmask (`Router`, `Storage`, `Gateway`)  |
|  - Expiration Deadline  : Monotonic timestamp when entry expires         |
|  - Reachability         : Direct (1-hop radio) | MultiHop (Route)        |
|  - Connection State     : Disconnected | Connecting | Connected          |
+--------------------------------------------------------------------------+
```

### 4.1 Strict Separation: Peer Table vs. Trust Store vs. Routing Table
To avoid architectural conflation, FIELDNET strictly separates three foundational tables:

```text
+-----------------------+-----------------------+-----------------------+
|      PEER TABLE       |      TRUST STORE      |     ROUTING TABLE     |
+-----------------------+-----------------------+-----------------------+
| Scope: Radio Links    | Scope: Sovereign Trust| Scope: Mesh Topology  |
| Lifetime: Ephemeral   | Lifetime: Permanent   | Lifetime: Dynamic     |
| Location: Volatile RAM| Location: SQLite Store| Location: Volatile RAM|
| Key: NodeId + Links   | Key: NodeId + PubKey  | Key: DestinationNodeId|
| Purpose: "Who is in   | Purpose: "What is this| Purpose: "Which next- |
| radio proximity?"     | peer allowed to do?"  | hop reaches dest?"    |
+-----------------------+-----------------------+-----------------------+
```

---

## 5. Discovery Lifecycle & State Machine

Every entry in the Peer Table transitions through a formal, deterministic state machine driven by incoming beacon frames and monotonic timer expirations.

```text
                  +---------------------------+
                  |          UNKNOWN          |  (Zero record in Peer Table)
                  +---------------------------+
                                │
                                │ Valid advertisement beacon intercepted
                                ▼
                  +---------------------------+
                  |        DISCOVERED         |  (Initial record created)
                  +---------------------------+
                                │
                                │ Inbound traffic confirmed
                                ▼
         ┌───────>+---------------------------+<───────┐
         │        |          ACTIVE           |        │
         │        +---------------------------+        │
         │           │                     │           │
         │ Beacon    │ Silence > 30s       │ New info  │ Re-announced
         │ received  │                     ▼           │
         │           │            +------------------+ │
         │           │            |     UPDATED      |─┘
         │           │            +------------------+
         │           ▼
         │        +---------------------------+
         │        |           STALE           |  (Degraded; routes penalized)
         │        +---------------------------+
         │           │
         │           │ Silence > 90s (TTL Expired)
         │           ▼
         │        +---------------------------+
         │        |          EXPIRED          |  (Purged from active memory)
         │        +---------------------------+
         │                     │
         │                     │ Fresh beacon intercepted
         │                     ▼
         │        +---------------------------+
         └────────|       REDISCOVERED        |
                  +---------------------------+
```

### 5.1 State Semantics & Timer Thresholds
*   **`UNKNOWN`:** The node has never been observed or has been purged from memory. Zero state allocated.
*   **`DISCOVERED`:** A fresh advertisement has been intercepted and passed syntactic validation. The record is inserted into the Peer Table with `FirstSeen = LastSeen = Now()`.
*   **`ACTIVE`:** The peer has transmitted an advertisement or authenticated frame within the active window ($t_{\text{now}} - t_{\text{last\_seen}} \le 30\text{ seconds}$). The peer is considered immediately reachable for direct connections.
*   **`UPDATED`:** A new advertisement arrives with a higher `AdvertisementVersion` or altered transport addresses. The Peer Table entry is atomically updated, and a `peer.updated` event is broadcast.
*   **`STALE`:** The peer has been silent for $> 30\text{ seconds}$ but $\le 90\text{ seconds}$. The peer remains in the table, but the Routing Engine applies a latency penalty to routes traversing this link.
*   **`EXPIRED`:** The peer has exceeded its TTL deadline (silence $> 90\text{ seconds}$). The entry is purged from the Peer Table, active connections are closed, and a `peer.expired` event is broadcast.
*   **`REDISCOVERED`:** An expired peer transmits a fresh beacon. It is re-inserted into the Peer Table. If a prior persistent `PeerTrustRecord` exists in the SQLite database, its trust status is restored immediately without re-pairing.

---

## 6. Discovery Events Taxonomy

All discovery transitions emit strongly typed, asynchronous events onto the runtime's internal Event Bus. Downstream subsystems subscribe to these events to adjust networking, routing, and UI state.

```text
+-----------------------------+----------------------------------------------------+
| Event Identifier            | Trigger Condition & Semantic Meaning               |
+-----------------------------+----------------------------------------------------+
| `peer.discovered`           | A new node has been detected in radio proximity.   |
| `peer.updated`              | An existing peer announced modified locators/caps. |
| `peer.expired`              | A peer timed out due to beacon silence (TTL drop). |
| `peer.reappeared`           | A previously expired peer became reachable again.  |
| `peer.transport.changed`    | A transport link opened or closed for a peer.      |
| `peer.capabilities.changed` | A peer announced updated service or role flags.    |
+-----------------------------+----------------------------------------------------+
```

---

## 7. Common Transport Abstraction

FIELDNET treats physical and logical network media as interchangeable, swappable carriers. All physical communication mechanisms must implement the unified **`Transport`** abstraction.

```text
+--------------------------------------------------------------------------+
|                          TRANSPORT TRAIT BLUEPRINT                       |
+--------------------------------------------------------------------------+
|  pub trait Transport: Send + Sync {                                      |
|      fn name(&self) -> &'static str;                                     |
|      fn capabilities(&self) -> TransportCapabilities;                    |
|      async fn start_discovery(&self) -> Result<DiscoveryStream>;         |
|      async fn broadcast_advertisement(&self, adv: &[u8]) -> Result<()>;  |
|      async fn connect(&self, address: &TransportAddress) -> Result<Conn>;|
|      async fn accept(&self) -> Result<IncomingConnectionStream>;         |
|      async fn health(&self) -> TransportHealth;                          |
|  }                                                                       |
+--------------------------------------------------------------------------+
```

---

## 8. Transport Roles & Media Profiles

FIELDNET organizes supported transports into specific operational roles based on their physical bandwidth, latency, and power profiles:

```text
+---------------------+-------------------+-------------------+--------------------+
| Transport           | Primary Role      | Bandwidth / Range | Power Profile      |
+---------------------+-------------------+-------------------+--------------------+
| **TCP / IP**        | Bulk transfer,    | High (>100 Mbps)  | Medium             |
|                     | stream sync       | 100 meters        | (Active Wi-Fi/NIC) |
+---------------------+-------------------+-------------------+--------------------+
| **UDP / IP**        | Subnet discovery, | Medium            | Medium             |
|                     | heartbeats        | 100 meters        |                    |
+---------------------+-------------------+-------------------+--------------------+
| **Bluetooth LE**    | Zero-infra beacon,| Ultra-Low         | Very Low           |
|                     | control fallback  | 10 - 30 meters    | (Coin-cell viable) |
+---------------------+-------------------+-------------------+--------------------+
| **Wi-Fi Direct /    | High-speed P2P,   | Very High         | High               |
| Wi-Fi Aware**       | local mesh bridge | 50 - 100 meters   | (Fast battery drain|
+---------------------+-------------------+-------------------+--------------------+
| **WebRTC**          | Browser guest,    | Variable          | Medium             |
|                     | NAT traversal     | Global / Internet |                    |
+---------------------+-------------------+-------------------+--------------------+
| **libp2p**          | Modular transport | Variable          | Variable           |
|                     | framing substrate | Multi-hop overlay |                    |
+---------------------+-------------------+-------------------+--------------------+
```

---

## 9. Transport Lifecycle & Fault Containment

Transports in FIELDNET transition through a formal operational lifecycle. A transport becoming degraded or failing completely must **never** crash the node runtime or invalidate peer identities.

```text
                  +---------------------------+
                  |       INITIALIZING        |  (Probing hardware / binding sockets)
                  +---------------------------+
                                │
                                │ Initialization & capabilities verified
                                ▼
         ┌───────>+---------------------------+<───────┐
         │        |         AVAILABLE         |        │
         │        +---------------------------+        │
         │           │                     ▲           │
         │ Link      │ Packet loss > 25%   │ Error rate│ Recovered
         │ recovers  │ High latency        │ drops     │
         │           ▼                     │           │
         │        +---------------------------+        │
         └────────|         DEGRADED          |        │
                  +---------------------------+        │
                             │                         │
                             │ Interface down / Radio off
                             ▼                         │
                  +---------------------------+        │
                  |        UNAVAILABLE        |────────┘
                  +---------------------------+
                                │
                                │ Runtime shutdown initiated
                                ▼
                  +---------------------------+
                  |         STOPPING          |  (Draining in-flight buffers)
                  +---------------------------+
                                │
                                │ Sockets closed, radio unhooked
                                ▼
                  +---------------------------+
                  |          STOPPED          |  (Dormant state)
                  +---------------------------+
```

---

## 10. Connection Manager Architecture & Responsibilities

The **Connection Manager** is the core operational engine responsible for establishing, authenticating, maintaining, monitoring, migrating, and terminating active communication channels with peers.

```text
+--------------------------------------------------------------------------+
|                                PEER TABLE                                |
|          (Registry of discovered peers, addresses, and transports)       |
+--------------------------------------------------------------------------+
                                     │ reads candidates
                                     ▼
+══════════════════════════════════════════════════════════════════════════+
|                           CONNECTION MANAGER                             |
|  - Connection Creation     : Evaluates connect triggers & opens links    |
|  - Connection Authentication: Enforces Phase 03 Ed25519 Handshakes       |
|  - Connection Maintenance  : Dispatches keepalive heartbeats             |
|  - Connection Health       : Monitors packet loss, RTT, and timeouts     |
|  - Transport Selection     : Picks optimal carrier based on payload size |
|  - Transport Migration     : Seamlessly fails over across radios         |
|  - Connection Closure      : Gracefully tears down sessions              |
+══════════════════════════════════════════════════════════════════════════+
                                     │ drives
                                     ▼
+--------------------------------------------------------------------------+
|                        TRANSPORT IMPLEMENTATIONS                         |
|                 (TCP Sockets, BLE GATT, Wi-Fi P2P Channels)              |
+--------------------------------------------------------------------------+
```

---

## 11. Unified Networking Model & Ontological Hierarchy

FIELDNET defines an explicit, unambiguous structural hierarchy for physical hardware, operating system adapters, protocol engines, active connections, cryptographic contexts, and sovereign distributed nodes:

```text
+--------------------------------------------------------------------------+
|                          PHYSICAL DEVICE LAYER                           |
|       (Host Device: Laptop, Smartphone, Raspberry Pi, Edge Gateway)      |
+--------------------------------------------------------------------------+
                                     │ owns 1..N
                                     ▼
+--------------------------------------------------------------------------+
|                        NETWORK INTERFACE LAYER                           |
|       (Host OS Physical/Virtual Adapters: `wlan0`, `en0`, `hci0`)        |
+--------------------------------------------------------------------------+
                                     │ drives 1..N
                                     ▼
+--------------------------------------------------------------------------+
|                           TRANSPORT LAYER                                |
|      (Protocol Engines: `TcpTransport`, `BleTransport`, `UdpTransport`)  |
+--------------------------------------------------------------------------+
                                     │ establishes 1..N
                                     ▼
+--------------------------------------------------------------------------+
|                           CONNECTION LAYER                               |
|        (Active Point-to-Point Link Handles: `ConnectionId: 0x4A1F`)      |
+--------------------------------------------------------------------------+
                                     │ encapsulates 1..1
                                     ▼
+--------------------------------------------------------------------------+
|                         SECURE SESSION LAYER                             |
|   (Cryptographic AEAD State: ChaCha20-Poly1305 Keys, Monotonic Sequence) |
+--------------------------------------------------------------------------+
                                     │ binds strictly to
                                     ▼
+--------------------------------------------------------------------------+
|                         SOVEREIGN PEER LAYER                             |
|            (Canonical Node Identity: `ed25519:7F8a9B01c2...`)            |
+--------------------------------------------------------------------------+
```

### 11.1 The Parallel Discovery and Trust Track
Orthogonal to the connection hierarchy, discovery awareness and authorization progress through a decoupled pipeline:

$$\mathbf{Discovery\ Source} \longrightarrow \mathbf{Untrusted\ Peer\ Record} \longrightarrow \mathbf{Identity\ Verification} \longrightarrow \mathbf{Durable\ Trust\ Record}$$

*   **Discovery Source:** Captures raw, unverified RF beacons over open frequencies.
*   **Peer Record:** Ephemeral proximity metadata in the Peer Table. Indicates physical reachability only.
*   **Trust Record:** Persistent cryptographic ledger in SQLite storing verified public keys and signed capability grants.

---

## 12. Connection States & Finite State Machine

Connections transition through an 8-state deterministic finite state machine:

```text
                  +---------------------------+
                  |        DISCOVERED         |  (Candidate in Peer Table; 0 sockets)
                  +---------------------------+
                                │
                                │ Connection initiated (Outbound connect or Inbound accept)
                                ▼
                  +---------------------------+
                  |        CONNECTING         |  (Physical socket / link establishing)
                  +---------------------------+
                                │
                                │ Socket established; protocol handshake starts
                                ▼
                  +---------------------------+
                  |      AUTHENTICATING       |  (Mutual Ed25519 challenge-response)
                  +---------------------------+
                                │
                                │ Handshake verified; AEAD keys derived; Authz OK
                                ▼
         ┌───────>+---------------------------+<───────┐
         │        |         CONNECTED         |        │
         │        +---------------------------+        │
         │           │                     │           │
         │ Link      │ RTT > threshold     │ Link drop │ Reconnect
         │ recovers  │ Packet loss > 20%   │ detected  │ succeeds
         │           ▼                     ▼           │
         │        +------------------+    +------------------+
         └────────|     DEGRADED     |    |   RECONNECTING   |
                  +------------------+    +------------------+
                             │                     │
                             │ Complete drop       │ Max retries exceeded
                             ▼                     ▼
                  +------------------------------------------+
                  |               DISCONNECTED               |
                  +------------------------------------------+
                                       │
                                       │ Fatal protocol/security error
                                       ▼
                  +------------------------------------------+
                  |                  FAILED                  |  (Quarantined / Purged)
                  +------------------------------------------+
```

---

## 13. Connection Ownership & Application Abstraction Shield

```text
+--------------------------------------------------------------------------+
|                             APPLICATION LAYER                            |
|    (Zero direct socket access, Zero connection handles, Zero raw bytes)  |
+--------------------------------------------------------------------------+
                                     │ invokes Environment API
+══════════════════════════════════════════════════════════════════════════+
|                        ENVIRONMENT API BOUNDARY                          |
+══════════════════════════════════════════════════════════════════════════+
                                     │ dispatches
+--------------------------------------------------------------------------+
|                          FIELDNET RUNTIME CORE                           |
|                                                                          |
|   +------------------------------------------------------------------+   |
|   |                       CONNECTION MANAGER                         |   |
|   |  - Sole owner of active connection instances                     |   |
|   |  - Sole owner of raw OS socket and BLE characteristic handles     |   |
|   |  - Sole manager of connection states and health metrics          |   |
|   +------------------------------------------------------------------+   |
+--------------------------------------------------------------------------+
```

*   **Exclusive Ownership:** The `ConnectionManager` is the **sole subsystem** authorized to open, read, write, migrate, or close transport connections.
*   **Zero Raw Handle Leaks:** OS socket handles (`RawFd`, `SOCKET`, BLE handles) are strictly hidden from applications, services, and UI components.
*   **Application Abstraction:** Guest WASM applications communicate strictly using high-level Environment API primitives (`network_send(dest_node_id, payload)`), completely oblivious to underlying sockets and physical media.

---

## 14. Multi-Interface Nodes & Dynamic Network Changes

A single FIELDNET node frequently possesses multiple physical and virtual network adapters simultaneously:
*   Wi-Fi (Infrastructure Station mode or P2P Direct)
*   Bluetooth Controller (Bluetooth Low Energy HCI)
*   Ethernet (Direct cable, local switch)
*   Cellular / Satellite (USB modem, tactical link)

### 14.1 Dynamic Network Change Handling
The runtime adapts to environmental network changes without restarting or dropping state:

| Network Event | Platform Adapter Action | Runtime Reaction |
| :--- | :--- | :--- |
| **Wi-Fi Connects** | Emits `on_interface_available("wlan0", Wifi)` | Starts UDP/mDNS discovery; binds TCP listener; broadcasts advertisements. |
| **Wi-Fi Disconnects** | Emits `on_interface_unavailable("wlan0")` | Prunes Wi-Fi addresses from Peer Table; migrates active TCP sessions to BLE/Ethernet. |
| **IP Address Changes** | Emits `on_address_changed("wlan0", new_ip)` | Re-binds listeners; broadcasts updated advertisement; existing AEAD sessions migrate. |
| **Bluetooth Powered On**| Emits `on_transport_state_changed(Ble, Available)`| Starts BLE peripheral advertising and central discovery scans. |
| **Bluetooth Disabled** | Emits `on_transport_state_changed(Ble, Unavailable)`| Closes BLE GATT links; falls back to Wi-Fi/UDP. |
| **Device Switches Net** | Emits interface down then up on new subnet | Flushes old discovery caches; initiates rapid neighbor discovery broadcast. |

---

## 15. Declarative Transport Capability Model & Selection

Rather than having applications or higher-level subsystems hard-code a specific transport protocol, FIELDNET uses a **Declarative Capability Model**:

```text
+-----------------------+--------------------------------------------------+
| Capability Token      | Semantic Meaning & Transport Guarantee           |
+-----------------------+--------------------------------------------------+
| `ReliableStream`      | Guaranteed byte-ordered delivery (TCP).          |
| `Datagram`            | Low-latency packet transmission (UDP).           |
| `Broadcast`           | One-to-many local subnet transmission.           |
| `Multicast`           | Subscribed group multi-point transmission.       |
| `LowBandwidth`        | Constrained radio throughput (< 100 kbps, BLE).  |
| `HighBandwidth`       | High-speed channel (> 10 Mbps, Wi-Fi, Ethernet).|
| `LowLatency`          | Real-time round trip (< 10 ms).                  |
| `PeerToPeer`          | Direct ad-hoc link without infrastructure AP.    |
| `LocalOnly`           | Restricted to direct radio / subnet proximity.   |
| `InternetCapable`     | Routable to global IP internet gateway.          |
+-----------------------+--------------------------------------------------+
```

### 15.1 Dynamic Selection Calculus
When the Routing Engine or Messaging Subsystem submits a payload for delivery, it specifies a requirement profile (e.g., `Requirements { stream: true, bandwidth: High }`). The Transport Selector matches the requirement profile against active transports in the Peer Table, choosing TCP over Wi-Fi when available, and falling back gracefully when conditions change.

---

## 16. Transport Migration Semantics & Application Shield

```text
[ Active Connection: TCP ] ──► Link Fails ──► [ Connection Manager ]
                                                        │
                                                        ▼ Consults Peer Table
                                              [ BLE Link Available ]
                                                        │
                                                        ▼ Re-binds Session
                                              [ Active Connection: BLE ]
```

*   **In-Flight Messages:** Unacknowledged frames on the failed transport are re-queued and re-transmitted across the new transport. Replay filters drop duplicates.
*   **Queued Messages:** Pending DTN bundles remain in local SQLite queues; transmission seamlessly resumes on the new link.
*   **Active Secure Session:** The cryptographic session (AEAD keys, sequence counters) persists without re-negotiation, bound to the sovereign `NodeId`.
*   **File Chunk Transfers:** Resumes from the last verified 256 KB content block CID. Zero re-transfer of already-verified chunks.
*   **Service Invocations:** Client RPC calls experience transient latency during failover, but do not fail; idempotent calls are retried automatically.

---

## 17. Connection Health, Heartbeats & Keepalive

```text
+-----------------------+----------------------+-------------------------------------------+
| Health Classification | Link Characteristics | Runtime Action                            |
+-----------------------+----------------------+-------------------------------------------+
| **Healthy**           | Packet loss < 5%     | Normal operation; full bandwidth granted. |
|                       | RTT within baseline  |                                           |
+-----------------------+----------------------+-------------------------------------------+
| **Degraded**          | Packet loss 5% - 25% | Throttle bulk transfers; initiate health  |
|                       | RTT > 3x baseline    | ping probes; prepare fallback transport.  |
+-----------------------+----------------------+-------------------------------------------+
| **Dead**              | Packet loss > 25%    | Mark connection FAILED / DISCONNECTED;    |
|                       | No ping reply > 15s  | trigger transport migration.              |
+-----------------------+----------------------+-------------------------------------------+
```

### 17.1 Critical Distinction: Peer Alive vs. Peer Reachable
> **ARCHITECTURAL INVARIANT:**  
> **A peer may be ALIVE but temporarily UNREACHABLE.**

A dead connection or failed keepalive ping indicates strictly that the **current physical link is broken**. It must **NEVER** be assumed that the remote node has crashed or retired. The peer remains recorded as a valid, trusted entity in the `TrustStore`. FIELDNET avoids aggressive peer deletion: unreachable peers are buffered in the Delay-Tolerant Network (DTN) for store-carry-forward delivery.

---

## 18. Reconnection Semantics & Exponential Backoff

Reconnection attempts use exponential backoff with jitter:

$$t_{\text{wait}} = \min(t_{\text{max}}, t_{\text{base}} \times 2^{\text{attempt}}) \pm \text{Jitter}$$

*   $t_{\text{base}} = 1.0\text{s}$, $t_{\text{max}} = 60.0\text{s}$.
*   Maximum 5 attempts per transport before switching carriers.
*   Reconnection leverages the existing persistent `PeerTrustRecord` in SQLite. Out-of-band pairing is **never** required for normal reconnections.

---

## 19. End-to-End Discovery, Connection & Disconnection Flows

### 19.1 Complete Discovery and Connection Sequence

```text
Step  Subsystem Owner          Action Description
────  ───────────────────────  ───────────────────────────────────────────────────────
1.    Platform Adapter         Detects physical interface up (e.g. Wi-Fi `wlan0`).
2.    Transport Manager        Initializes `TcpTransport` and `UdpTransport`.
3.    Discovery Manager        Starts periodic multicast beacon on UDP port 4242.
4.    Discovery Manager        Intercepts raw UDP beacon from remote Node B.
5.    Peer Validator           Sanitizes beacon, checks protocol version & 2 KB bound.
6.    Peer Table               Creates/updates entry for Node B; status = `ACTIVE`.
7.    Connection Manager       Evaluates policy (data pending for B) -> triggers connect.
8.    Transport Selector       Selects TCP as preferred high-bandwidth carrier.
9.    Connection Manager       Opens TCP socket to Node B (`TcpStream::connect`).
10.   Security Subsystem       Executes Phase 03 mutual Ed25519 challenge-response handshake.
11.   Session Manager          Derives ChaCha20-Poly1305 AEAD keys via X25519 + HKDF-SHA256.
12.   Connection Manager       Marks connection `CONNECTED`; application frames flow.
```

### 19.2 Complete Disconnection and Recovery Sequence

```text
Step  Subsystem Owner          Action Description
────  ───────────────────────  ───────────────────────────────────────────────────────
1.    Connection Manager       Detects physical link drop (TCP FIN / RST or ping timeout).
2.    Connection Manager       Transitions connection state to `DEGRADED` / `DISCONNECTED`.
3.    Transport Manager        Updates transport health metrics (flags link as dead).
4.    Connection Manager       Consults Peer Table for alternate paths (BLE found).
5.    Connection Manager       Initiates migration: re-binds secure session to BLE link.
6.    Peer Table               If NO alternate links exist, marks Node B as `STALE`.
7.    Peer Table               Node B remains known but unreachable; DTN queues buffer data.
8.    Peer Table               After 90s silence, peer record marked `EXPIRED` (Trust preserved).
```

---

## 20. Discovery & Connection Race Condition Semantics

In decentralized wireless meshes, concurrent asynchronous events frequently collide. FIELDNET specifies deterministic handling for all primary race scenarios:

1.  **Duplicate Advertisements:** Filtered out at the `DiscoveryManager` boundary by comparing `(NodeId, AdvertisementVersion, Timestamp)` against an in-memory sliding window cache.
2.  **Simultaneous Discovery Across Multiple Interfaces:** Advertisements heard over Wi-Fi and Bluetooth simultaneously are aggregated into a **single** `PeerTableEntry`, combining observed addresses without duplicating the peer.
3.  **Simultaneous Connection Attempts (Connect Race):** Resolved via deterministic lexicographical tie-breaking: $\text{NodeId}_A < \text{NodeId}_B$. The connection initiated by the lower NodeId is kept; the other is cleanly closed.
4.  **Peer Disappears During Handshake:** The 5.0-second handshake watchdog timer fires. The unauthenticated socket is severed, all allocated handshake buffers are zeroized, and the connection transitions to `DISCONNECTED`.
5.  **Peer Changes Transport Mid-Connection:** If a peer switches from Wi-Fi to BLE while a TCP connection is active, the TCP connection is cleanly closed before the BLE channel transitions to `CONNECTED`.
6.  **Stale Advertisement Followed by Fresh Advertisement:** Advertisements carry monotonically increasing `AdvertisementVersion` counters. Any advertisement with version $V \le V_{\text{current}}$ is discarded immediately.

---

## 21. Peer Record vs. Trust Record Separation

FIELDNET strictly isolates proximity awareness from security authorization:

```text
+------------------------------------+-------------------------------------+
|        PEER RECORD (PHASE 04)      |        TRUST RECORD (PHASE 03)      |
+------------------------------------+-------------------------------------+
| Answers: "Who is reachable?"       | Answers: "Who is authorized?"       |
| Lifetime: Volatile (Seconds/Mins)  | Lifetime: Durable (Survives Reboot) |
| Storage: In-Memory RAM Cache       | Storage: SQLite Relational Database |
| Authority: Unverified Beacons      | Authority: Verified Digital Sigs    |
| State: UNKNOWN -> EXPIRED          | State: UNKNOWN -> AUTHORIZED        |
+------------------------------------+-------------------------------------+
```

### 21.1 The "Known but Untrusted" Principle
A peer frequently exists in the Peer Table as **Known but Untrusted**:
*   Node C is broadcasting valid UDP beacons. Node C is entered into the Peer Table.
*   However, Node C has never paired with Node A and holds zero capability tokens.
*   **Result:** The networking layer knows Node C's IP and signal strength, but the security layer blocks Node C from accessing files, querying services, or routing trusted messages. The networking layer strictly queries the security layer without making independent authorization decisions.

---

## 22. Discovery Information Freshness & Categorization

To prevent stale or poisoned caches from corrupting routing, discovery telemetry is categorized into four distinct confidence tiers:

```text
+------------------+-------------------------------------------------------+--------------------+
| Freshness Tier   | Telemetry Fields Included                             | Invalidation Rule  |
+------------------+-------------------------------------------------------+--------------------+
| **Authoritative**| Sovereign Public Key, Canonical `NodeId`.             | Never expires      |
|                  | (Mathematically bound to private key).                | (Unless revoked).  |
+------------------+-------------------------------------------------------+--------------------+
| **Observed**     | RSSI signal strength, link RTT, arrival interface,    | Re-evaluated on    |
|                  | physical IP/MAC address.                              | every frame arrival|
+------------------+-------------------------------------------------------+--------------------+
| **Cached**       | Declared public capabilities, hosted service URIs,   | Bounded by beacon  |
|                  | advertisement version.                                | TTL (60 seconds).  |
+------------------+-------------------------------------------------------+--------------------+
| **Expired**      | Any discovery record with silence > 90 seconds.       | Purged from RAM    |
|                  |                                                       | immediately.       |
+------------------+-------------------------------------------------------+--------------------+
```

---

## 23. Conceptual Networking API Boundary

The internal module architecture exposes clean, decoupled interfaces across subsystems:

### 1. `DiscoveryManager`
*   **Responsibility:** Scan scheduling, beacon generation, rate limiting, and source multiplexing.
*   **Owned State:** Active source handles, broadcast timers, token buckets.
*   **Consumers:** Connection Manager, Health Manager.
*   **Events:** `peer.discovered`, `peer.updated`, `peer.expired`.

### 2. `PeerTable`
*   **Responsibility:** Authoritative thread-safe registry of reachable peers and addresses.
*   **Owned State:** `DashMap<NodeId, PeerTableEntry>`, TTL expiration wheel.
*   **Consumers:** Discovery Manager, Connection Manager, Routing Engine.
*   **Events:** None directly (emitted by Discovery Manager).

### 3. `ConnectionManager`
*   **Responsibility:** Socket lifecycle, transport selection, keepalive, and failover.
*   **Owned State:** Active connection table (`Map<ConnectionId, Connection>`), peer-to-connection index.
*   **Consumers:** Protocol Engine, Routing Engine, DTN Queue.
*   **Events:** `connection.connected`, `connection.degraded`, `connection.disconnected`.

### 4. `TransportManager`
*   **Responsibility:** Initializing, supervising, and querying registered transport drivers.
*   **Owned State:** Vector of `Box<dyn Transport>`, transport lifecycle states.
*   **Consumers:** Connection Manager, Platform Adapter.
*   **Events:** `transport.state_changed`.

---

## 24. Network Resource Bounds & Abuse Prevention

```text
+-----------------------------------+-------------------------------------+
| Resource Dimension                | Default Architectural Bound         |
+-----------------------------------+-------------------------------------+
| **Max Discovery Rate**            | Token bucket: max 20 beacons/sec    |
| **Max Advertisement Size**        | 2,048 bytes (hard drop if exceeded) |
| **Max Peer Table Size**           | 256 unauthenticated nodes (LRU drop)|
| **Max Simultaneous Connections**  | 64 concurrent physical connections  |
| **Max Connections per Peer**      | 2 concurrent links (e.g. TCP + BLE) |
| **Max Connections per Transport** | 32 concurrent links per transport   |
| **Max In-Flight Handshakes**      | 8 concurrent unauthenticated links  |
| **Handshake Timeout**             | 5.0 seconds (hard drop)             |
| **Connection Attempt Rate Limit** | Token bucket: max 5 attempts/sec    |
| **Keepalive Timeout**             | 15.0 seconds (3 failed probes = Dead|
+-----------------------------------+-------------------------------------+
```

---

## 25. Network Security Boundary & Fail-Closed Enforcement

```text
  [ Raw RF / Socket Arrival ] (UNTRUSTED)
               │
               ▼
  [ Discovery / Peer Table ] (UNTRUSTED PROXIMITY)
               │
               ▼ Connection Attempt
  [ Physical Socket Connect ] (UNAUTHENTICATED)
               │
               ▼
  ═════════════════════════════════════════════════════════════════════════
  SECURITY GATEWAY 1: MUTUAL IDENTITY AUTHENTICATION (Phase 03)
  - Must cryptographically prove Ed25519 key possession via signed challenge
  - FAILS CLOSED: Socket severed instantly if handshake fails
  ═════════════════════════════════════════════════════════════════════════
               │
               ▼
  [ Secure AEAD Session ] (AUTHENTICATED & ENCRYPTED)
               │
               ▼ Operation Requested
  ═════════════════════════════════════════════════════════════════════════
  SECURITY GATEWAY 2: CAPABILITY AUTHORIZATION (Phase 03)
  - Must present valid, unexpired, unrevoked signed CapabilityToken
  - FAILS CLOSED: Returns `Error::PermissionDenied` if token missing
  ═════════════════════════════════════════════════════════════════════════
               │
               ▼
  [ Usable Peer for Applications & Services ]
```

---

## 26. MVP Networking Contract for Phase 05 (Routing & DTN)

When Phase 05 (Protocol, Routing & Delay-Tolerant Networking) begins, the routing engine can rely upon the following firm guarantees provided by Phase 04:

1.  **Stable Sovereign Identifiers:** Peers are identified exclusively by their permanent canonical `NodeId`, never by transient IP addresses or radio links.
2.  **Neighbor Proximity Ledger:** The Peer Table provides an up-to-date, TTL-scrubbed list of immediately reachable single-hop radio neighbors.
3.  **Authenticated Point-to-Point Links:** Frames transmitted to an active connection are guaranteed to be delivered over an authenticated, ChaCha20-Poly1305 encrypted AEAD channel.
4.  **Transport Abstraction Shield:** The routing engine submits packets to a logical `NodeId`; the `ConnectionManager` transparently handles MTU framing, socket selection, and radio migration.
5.  **Fault Visibility without Peer Deletion:** Link failures emit clear `connection.degraded` and `connection.disconnected` events, allowing routing to adjust cost metrics without prematurely deleting unreachable nodes from DTN storage.

---

## 27. Rust Implementation Blueprint

The networking subsystem will be implemented across two dedicated crates within the Cargo workspace:

```text
crates/
├── fieldnet-discovery/              # Pure discovery logic and beacon codecs
│   ├── src/
│   │   ├── beacon.rs                # Advertisement binary codec & schema
│   │   ├── validator.rs             # Syntactic parser and size gatekeeper
│   │   ├── rate_limiter.rs          # Token-bucket flooding defense
│   │   ├── sources/                 # Pluggable discovery source drivers
│   │   │   ├── udp_multicast.rs     # UDP subnet beacon listener/sender
│   │   │   ├── mdns.rs              # mDNS / DNS-SD responder/querier
│   │   │   └── ble_beacon.rs        # BLE advertisement codec
│   │   └── manager.rs               # DiscoveryManager orchestration
└── fieldnet-network/                # Connection management & transport layer
    ├── src/
    │   ├── peer/
    │   │   ├── table.rs             # DashMap-backed thread-safe PeerTable
    │   │   └── entry.rs             # PeerTableEntry and address sets
    │   ├── connection/
    │   │   ├── manager.rs           # ConnectionManager actor
    │   │   ├── state.rs             # 8-state connection FSM
    │   │   └── deduplicator.rs      # Lexicographical tie-breaking
    │   ├── transport/
    │   │   ├── traits.rs            # `trait Transport` definition
    │   │   ├── selector.rs          # Heuristic multi-transport selector
    │   │   ├── tcp.rs               # Tokio TCP framed transport
    │   │   └── udp.rs               # Datagram UDP transport
    │   ├── health/
    │   │   └── keepalive.rs         # Ping/pong heartbeat monitor
    │   └── interface/
    │       └── watcher.rs           # Platform network interface listener
```

---

## 28. Comprehensive Testing Blueprint

To ensure complete resilience under real-world tactical conditions, Phase 04 incorporates a multi-tier testing harness:

### 28.1 Automated Unit & Integration Tests
1.  **Beacon Codec Test:** Verifies binary serialization, deserialization, and information minimization bounds ($< 2\text{ KB}$).
2.  **Rate Limiter Test:** Asserts that flooding 1,000 beacons/sec from a rogue IP results in immediate token-bucket drops.
3.  **Peer Table Expiration Test:** Mocks monotonic clock advancement; asserts that silent peers transition `ACTIVE` $\to$ `STALE` $\to$ `EXPIRED` at exact 30s/90s boundaries.
4.  **Connect Race Tie-Breaking Test:** Spawns concurrent tasks where Node A connects to Node B while Node B connects to Node A; verifies that exactly **one** socket survives based on $\text{NodeId}_A < \text{NodeId}_B$.
5.  **Transport Migration Simulation:** Drops an active TCP socket mid-transfer; verifies that the session re-binds to a secondary transport without resetting AEAD sequence counters.

### 28.2 Physical Two-Device Validation Test (Device A $\leftrightarrow$ Device B)
> **ARCHITECTURAL MANDATE:**  
> **Phase 04 validation must not rely exclusively on mocks.**

A physical end-to-end integration test between two real computing devices (e.g., Mac $\leftrightarrow$ Linux laptop or Phone $\leftrightarrow$ Laptop) must demonstrate:
1.  *Discovery:* Node A and Node B discover each other over UDP Multicast on an isolated Wi-Fi subnet with zero manual configuration.
2.  *Authentication:* Mutual Ed25519 challenge-response handshake completes successfully.
3.  *Bidirectional Stream:* Authenticated ping/pong messages flow across an encrypted TCP connection.
4.  *Physical Disconnection:* Wi-Fi adapter on Node B is disabled; Node A marks connection `DEGRADED` then `DISCONNECTED` without crashing.
5.  *Physical Reconnection:* Wi-Fi adapter on Node B is re-enabled; Node A automatically reconnects using exponential backoff and resumes communication.

---

## 29. Master Networking Architecture Diagram

```text
                 +---------------------------------------------------------+
                 |                      FIELDNET NODE                      |
                 +---------------------------------------------------------+
                                              │
                              Physical Network Interfaces
                 ┌────────────────────────────┼────────────────────────────┐
                 ▼                            ▼                            ▼
         +---------------+            +---------------+            +---------------+
         |     Wi-Fi     |            |   Bluetooth   |            |   Ethernet    |
         |  (`wlan0/en0`)|            |    (`hci0`)   |            |    (`eth0`)   |
         +---------------+            +---------------+            +---------------+
                 │                            │                            │
                 ▼                            ▼                            ▼
         +═════════════════════════════════════════════════════════════════+
         |               PLATFORM ADAPTERS & TRANSPORTS                    |
         |         (TCP Sockets, UDP Multicast, BLE GATT Drivers)          |
         +═════════════════════════════════════════════════════════════════+
                                              │
                                              ▼ raw beacons / link events
         +-----------------------------------------------------------------+
         |                       DISCOVERY MANAGER                         |
         |    - Deduplication, Rate Limiting, Peer Validator Gatekeeper    |
         +-----------------------------------------------------------------+
                                              │
                                              ▼ populates reachable entries
         +-----------------------------------------------------------------+
         |                          PEER TABLE                             |
         |    - In-Memory Proximity Ledger (ACTIVE, STALE, EXPIRED)        |
         +-----------------------------------------------------------------+
                                              │
                                              ▼ drives connection candidate
         +═════════════════════════════════════════════════════════════════+
         |                      CONNECTION MANAGER                         |
         |    - Socket Lifecycle, Transport Selector, Migration Engine     |
         +═════════════════════════════════════════════════════════════════+
                                              │
                                              ▼ establishes physical link
         +-----------------------------------------------------------------+
         |                 ACTIVE POINT-TO-POINT CONNECTION                |
         +-----------------------------------------------------------------+
                                              │
                                              ▼ enforces
         +═════════════════════════════════════════════════════════════════+
         |              SECURITY & AUTHENTICATION BOUNDARY                 |
         |    - Ed25519 Mutual Handshake (Phase 03 Challenge-Response)     |
         |    - Unforgeable Capability Token Evaluation                    |
         +═════════════════════════════════════════════════════════════════+
                                              │
                                              ▼ derives
         +-----------------------------------------------------------------+
         |                    SECURE AEAD SESSION                          |
         |         (ChaCha20-Poly1305 Keys, Monotonic Sequence)            |
         +-----------------------------------------------------------------+
                                              │
                                              ▼ exposes to applications
         +-----------------------------------------------------------------+
         |                     USABLE FIELDNET PEER                        |
         +-----------------------------------------------------------------+
```

---

## 30. Authoritative Networking Invariants

The following 20 authoritative networking invariants govern all Phase 04 implementations:

1.  **Node Identity Is Transport-Independent:** A node's sovereign identity (`NodeId`) is completely decoupled from IP addresses, MAC addresses, and radio media; switching transports preserves peer identity.
2.  **Peer Identity Is Interface-Independent:** A peer reaching a node across Wi-Fi, Ethernet, or Bluetooth resolves to the exact same logical entity in the runtime.
3.  **Discovery Is Untrusted:** Intercepting a discovery beacon conveys zero proof of authenticity; discovered peers remain strictly unauthenticated until the Phase 03 challenge-response handshake succeeds.
4.  **Peer Records Are Not Trust Records:** The Peer Table tracks physical link proximity in volatile memory; the Trust Store tracks cryptographic authorizations in durable SQLite storage.
5.  **Peer Records Are Not Routing Tables:** Discovery identifies single-hop physical neighbors; path calculation and multi-hop forwarding belong strictly to the Routing Engine.
6.  **Connections Are Not Identities:** A network socket is a transient physical pipe; it does not define or alter sovereign peer identity.
7.  **Sessions Are Not Identities:** Cryptographic AEAD sessions are ephemeral contexts that can re-bind across transports without changing node identities.
8.  **Transport Failure Does Not Equal Node Failure:** If an individual radio link drops, the peer is not declared dead if alternative transports remain active.
9.  **Temporary Disconnection Does Not Immediately Delete Peer State:** A failed connection or missed ping does not purge the peer; unreachable nodes are retained for DTN store-carry-forward routing.
10. **Applications Never Access Raw Transports:** Guest applications and UI code are prohibited from opening raw sockets or directly accessing radio hardware; all communication is mediated by the Environment API.
11. **One Logical Peer May Have Multiple Connections:** A single `NodeId` can maintain concurrent active connections over distinct transports (e.g., TCP for bulk, BLE for control).
12. **A Connection Belongs to a Sovereign Peer Identity:** Every active connection is cryptographically bound to a verified `NodeId`. Sockets without identity are rejected.
13. **A Peer May Be Alive but Unreachable:** Link failure or missed keepalive pings indicate physical transport disruption, not that the peer has terminated.
14. **Authentication Occurs Before Trusted Communication:** No application payload may traverse a transport connection until the Phase 03 mutual Ed25519 handshake completes.
15. **Authorization Occurs Before Protected Operations:** Having an authenticated connection does not authorize arbitrary actions; explicit capability tokens are required for operations.
16. **Connection Resources Are Strictly Bounded:** Global and per-peer connection limits prevent memory exhaustion and socket exhaustion attacks.
17. **Duplicate Connections Are Handled Deterministically:** Simultaneous connection races between peers are resolved via lexicographical `NodeId` tie-breaking.
18. **Transport Migration Does Not Change Peer Identity:** Seamless failover between radios preserves the logical peer relationship and session cryptographic state.
19. **Platform Adapters Are Event-Driven:** Platform adapters push reactive OS events to the runtime; busy polling of network hardware is strictly prohibited.
20. **No Central Server Is Required for Local Operation:** Discovery, connection establishment, and transport migration operate 100% peer-to-peer without external internet or infrastructure servers.

---

## 31. Phase 04 Verification Checklist & Master Spec Audit

### Prompt 01 Verification Checklist
- [x] Peer Discovery defined precisely ("Who is reachable?"), decoupled from routing.
- [x] Six networking layers strictly separated.
- [x] Discovery pipeline codified (`Sources` $\to$ `Manager` $\to$ `Validator` $\to$ `Table` $\to$ `Event Bus` $\to$ `Subsystems`).
- [x] Heterogeneous platforms accommodated (Linux, macOS, Windows, Android, iOS).
- [x] Node Advertisement model specified with privacy rules and information minimization.
- [x] Peer Table schema defined; strictly separated from Trust Store and Routing Table.
- [x] Discovery lifecycle 6-state machine defined with timer thresholds.
- [x] Discovery events taxonomy established.
- [x] Common `trait Transport` blueprint specified.
- [x] Transport roles detailed (TCP, UDP, BLE, Wi-Fi Direct, WebRTC, libp2p).

### Prompt 02 Verification Checklist
- [x] Connection Manager architecture, responsibilities, and positioning formalized.
- [x] Ontological distinction codified (Peer, Connection, Transport, Session).
- [x] 8 formal connection states and deterministic transitions defined.
- [x] Connection ownership established; raw handles hidden from applications.
- [x] 6 transport lifecycle states with fault containment codified.
- [x] Multi-transport peer management codified (single logical entity).
- [x] Multi-factor transport selection policy established.
- [x] Transport migration semantics detailed across all subsystems.
- [x] Connection health and keepalive protocol codified; "Peer Alive vs. Peer Reachable" distinguished.
- [x] Reconnection semantics with exponential backoff and jitter defined.
- [x] Connection security and Phase 03 integration verified; raw socket prohibition enforced.
- [x] 9 connection events standardized.
- [x] Resource limits and abuse controls established.
- [x] Duplicate connection deduplication with lexicographical tie-breaking codified.

### Prompt 03 Verification Checklist (Final Architecture Pass)
- [x] Unified networking hierarchy codified (`Device` $\to$ `Interface` $\to$ `Transport` $\to$ `Connection` $\to$ `Session` $\to$ `Peer`).
- [x] Parallel discovery track codified (`Discovery` $\to$ `Peer Record` $\to$ `Trust`).
- [x] 12-step End-to-End Discovery and Connection sequence mapped with subsystem owners.
- [x] 8-step End-to-End Disconnection and Recovery sequence mapped.
- [x] Comprehensive discovery & connection race condition semantics established.
- [x] "Known but Untrusted" peer principle formalized.
- [x] Discovery information freshness categorized into 4 tiers (Authoritative, Observed, Cached, Expired).
- [x] Dynamic network change handling codified (Wi-Fi drop, IP change, BLE power toggle).
- [x] Multi-interface node support established under Application Abstraction Shield.
- [x] Conceptual API boundary specified for `DiscoveryManager`, `PeerTable`, `ConnectionManager`, `TransportManager`.
- [x] Declarative transport capability model defined (`ReliableStream`, `Datagram`, `HighBandwidth`, etc.).
- [x] Network security boundary and fail-closed enforcement codified.
- [x] MVP networking contract for Phase 05 formalized.
- [x] Rust implementation blueprint defined across `fieldnet-discovery` and `fieldnet-network` crates.
- [x] Comprehensive testing blueprint established, including physical two-device validation test (Device A $\leftrightarrow$ Device B).
- [x] Master Networking Architecture diagram created.
- [x] 20 Authoritative Networking Invariants finalized.
- [x] 100% compliance audit against `FIELDNET_MASTER_SPEC.md` verified.
