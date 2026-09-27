# FIELDNET Architecture Specification
## Phase 01 — Document 01: Formal System Model

**Document ID:** `DOC-ARCH-01-SYSTEM-MODEL`  
**Phase:** Phase 01 (System Model & Core Principles) — Prompt 01  
**Status:** Authoritative Architectural Specification  
**Version:** 1.0  
**References:** `FIELDNET_MASTER_SPEC.md`  

---

## 1. Executive Definition and Scope

### 1.1 What FIELDNET Is
**FIELDNET** is a **distributed offline operating environment** that executes on top of existing host operating systems (e.g., Linux, macOS, Windows, Android, iOS). It allows physically proximate or ad-hoc connected computing devices to dynamically discover one another, establish authenticated communication, and form a resilient local computing ecosystem. Within this ecosystem, applications, services, data, and computation continue to operate deterministically regardless of whether global internet connectivity is intermittent, degraded, or completely absent.

FIELDNET treats the global internet as an **opportunistic capability** rather than an essential prerequisite for computing.

### 1.2 What FIELDNET Is Not
To maintain architectural discipline and prevent scope degradation, FIELDNET is explicitly defined by what it is **not**:

*   **NOT a Replacement Operating System:** FIELDNET does not replace the host operating system kernel, hardware drivers, low-level process schedulers, or hardware-level memory management. It operates entirely in userspace on top of host OS primitives.
*   **NOT Merely a Mesh Networking Application:** FIELDNET contains multi-hop and mesh routing capabilities, but its scope extends far beyond packet transport. It provides an application execution runtime (WASM), capability-based access control, distributed storage, delay-tolerant messaging, CRDT state synchronization, and dynamic service resolution.
*   **NOT Merely an Offline Chat Application:** Ephemeral peer-to-peer messaging is only one generic communication primitive within FIELDNET. Chat is a user-plane application, not the system itself.
*   **NOT Merely a File-Sharing Application:** While content-addressed chunking and data replication are built into the runtime, they serve as foundational primitives for application state, software distribution, and persistent storage rather than simple point-to-point file transfers.
*   **NOT a Cloud Replacement:** FIELDNET is not designed to emulate massive hyperscale centralized data centers over local radios. It is optimized for local-first, low-latency, partitioned, and bandwidth-constrained ad-hoc collaboration.
*   **NOT a Central-Server Architecture:** There is no master coordinator, central directory, cloud broker, or anchor server required to bootstrap, validate, route, or maintain state in a FIELDNET environment.

---

## 2. High-Level Conceptual Architecture

The following diagram illustrates the layered execution hierarchy of FIELDNET:

```text
+-----------------------------------------------------------------------+
|                             APPLICATIONS                              |
|   (User Apps / Workspace / Collaborative Docs / Offline Services)    |
+-----------------------------------------------------------------------+
                                   |
                                   v
+-----------------------------------------------------------------------+
|                           ENVIRONMENT API                             |
|  (identity.*  network.*  peer.*  storage.*  content.*  service.*)     |
+-----------------------------------------------------------------------+
                                   |
                                   v
+-----------------------------------------------------------------------+
|                           FIELDNET RUNTIME                            |
|  +-----------------------------------------------------------------+  |
|  | Node Lifecycle | Capability Manager | Sandboxed WASM Engine     |  |
|  | Event Bus      | Local Security / Identity Engine               |  |
|  +-----------------------------------------------------------------+  |
+-----------------------------------------------------------------------+
                                   |
                                   v
+-----------------------------------------------------------------------+
|             SUBSYSTEMS & LOCAL-FIRST CAPABILITY ENGINES               |
|  +-----------------+  +-----------------+  +-----------------------+  |
|  |   NETWORKING    |  |     STORAGE     |  |       SERVICES        |  |
|  | - Transports    |  | - SQLite Store  |  | - Service Registry    |  |
|  | - Route Engine  |  | - Oplog / CRDT  |  | - Dynamic Resolver    |  |
|  | - DTN Store     |  | - Content / CID |  | - Local Web Gateway   |  |
|  +-----------------+  +-----------------+  +-----------------------+  |
|  +-----------------------------------------------------------------+  |
|  |                             COMPUTE                             |  |
|  | - Task Scheduler | Local/Remote Dispatch | Worker Pool          |  |
|  +-----------------------------------------------------------------+  |
+-----------------------------------------------------------------------+
                                   |
                                   v
+-----------------------------------------------------------------------+
|                      OTHER FIELDNET NODES                             |
|        (Direct Links, Multi-Hop Relays, Intermittent Contacts)        |
+-----------------------------------------------------------------------+
```

---

## 3. Formal System Model Definitions

The FIELDNET system model comprises 14 formal concepts. Their formal definitions, attributes, and relationships are specified below.

```text
+-------------+ 1       1 +---------+ 1       * +-------------+
|   Device    |---------->| Runtime |---------->| Application |
+-------------+           +---------+           +-------------+
       |                       |                       |
       | executes              | embodies              | requests
       v                       v                       v
+-------------+ 1       1 +---------+ 1       * +-------------+
| Host OS     |           |  Node   |---------->| Capability  |
+-------------+           +---------+           +-------------+
                               |                       ^
                               | participates in       | authorizes
                               v                       |
                        +-------------+         +-------------+
                        | Environment |         |   Service   |
                        +-------------+         +-------------+
                               |                       ^
                               | contains              | provides
                               v                       |
                        +-------------+ 1       * +-------------+
                        |    Peer     |---------->| Serv.Instance|
                        +-------------+           +-------------+
```

### 3.1 Formal Entity Definitions

#### 1. Environment
*   **Definition:** An ephemeral or durable logical collective formed by two or more participating FIELDNET nodes that have mutually discovered, authenticated, and established communication channels without depending on external infrastructure.
*   **Attributes:** `environment_id`, `epoch`, `membership_set`, `policy`, `security_domain`.
*   **Role:** The boundary of mutual discovery, collaboration, and trust.

#### 2. Node
*   **Definition:** The autonomous logical participant within FIELDNET. A node is the primary cryptographic actor, holding a persistent public/private keypair, managing local state, executing services, and relaying messages.
*   **Attributes:** `node_id` (cryptographic public key), `runtime_version`, `supported_transports`, `capabilities`, `advertised_services`, `trust_state`.
*   **Role:** The foundational agent in the distributed system.

#### 3. Device
*   **Definition:** The underlying physical or virtual hardware platform (e.g., a smartphone, laptop, Raspberry Pi, server) that provides physical resources (CPU, RAM, radios, storage, battery power).
*   **Attributes:** `device_id` (hardware/OS identifier), `form_factor`, `power_state` (battery level, AC powered), `hardware_radios`.
*   **Role:** The physical substrate that hosts the operating system and executes the runtime.

#### 4. Runtime
*   **Definition:** The core software engine (implemented in Rust) running as a userspace process or embedded library on the host operating system. It orchestrates all FIELDNET subsystems, enforces capabilities, manages persistence, and implements protocol lifecycles.
*   **Attributes:** `version`, `build_id`, `lifecycle_state`, `active_subsystems`.
*   **Role:** The abstraction layer that insulates applications from host OS quirks and physical topology changes.

#### 5. Application
*   **Definition:** A sandboxed, user-facing or system-facing software program executed by the FIELDNET runtime (primarily via WebAssembly) or built against the Environment API. Applications interact with the network, storage, and other nodes exclusively through capability grants.
*   **Attributes:** `app_id`, `manifest`, `sandbox_profile`, `granted_capabilities`, `execution_state`.
*   **Role:** Provides direct functionality (notes, collaboration, maps, communications) to users.

#### 6. Service
*   **Definition:** A named, abstract functional capability offered within an environment (e.g., `ai.inference`, `storage.document`, `maps.tile`, `internet.gateway`). A service is decoupled from any specific host, machine, or IP address.
*   **Attributes:** `service_id` (URI/URN identifier), `interface_version`, `schema`, `required_capabilities`.
*   **Role:** The contract against which applications make requests.

#### 7. Service Instance
*   **Definition:** A concrete, active invocation of a service executing on a specific node at a specific time.
*   **Attributes:** `instance_id`, `service_id`, `provider_node_id`, `endpoint`, `load_metric`, `health_status`, `expiration`.
*   **Role:** The target of an resolved service request.

#### 8. Resource
*   **Definition:** Any finite physical or logical capacity provided by a device or node, including compute cycles (CPU/GPU), volatile memory (RAM), durable storage capacity, network bandwidth, and energy (battery).
*   **Attributes:** `resource_type`, `total_capacity`, `available_capacity`, `allocation_policy`.
*   **Role:** The physical commodities managed and constrained by the runtime.

#### 9. Capability
*   **Definition:** An explicit, unforgeable, cryptographically signed token or permission grant authorizing a subject (node, application, or peer) to perform a specific action or access a specific resource/service.
*   **Attributes:** `capability_id`, `grantee`, `scope` (e.g., `storage.read`, `compute.submit`), `constraints`, `expiration`, `signature`.
*   **Role:** The sole mechanism for authorization across FIELDNET.

#### 10. Peer
*   **Definition:** The representation within a local node’s runtime of another remote node that has been discovered or connected over one or more transports.
*   **Attributes:** `peer_node_id`, `addresses`, `transports`, `connection_state`, `latency`, `trust_level`, `last_seen`.
*   **Role:** A remote endpoint for routing, synchronization, and interaction.

#### 11. Message
*   **Definition:** The atomic envelope of communication routed between nodes in FIELDNET. Messages are self-contained, authenticated, uniquely identified, and capable of being routed immediately or buffered across delays.
*   **Attributes:** `message_id`, `source_node_id`, `destination_node_id`, `created_at`, `expires_at`, `ttl`, `priority`, `payload`, `signature`.
*   **Role:** The fundamental currency of communication across the protocol and DTN subsystems.

#### 12. Content
*   **Definition:** Immutable, content-addressed data objects identified exclusively by the cryptographic hash of their contents (e.g., SHA-256). Content is independent of location, transport, or owner.
*   **Attributes:** `content_id` (cryptographic hash), `size_bytes`, `chunk_manifest`, `mime_type`, `integrity_proof`.
*   **Role:** The unit of persistent distributed data and file exchange.

#### 13. Gateway
*   **Definition:** A specialized service instance provided by a node that possesses access to external networks (such as the global internet, cellular networks, or specialized satellite links) and mediates requests between the local FIELDNET environment and external systems.
*   **Attributes:** `gateway_id`, `gateway_type` (HTTP proxy, DNS gateway, package mirror), `upstream_status`, `policy_rules`, `rate_limits`.
*   **Role:** Provides optional, unidirectional or bidirectional egress/ingress to external networks.

#### 14. Connectivity State
*   **Definition:** The instantaneous operating condition of a node relative to its local network neighborhood and external networks.
*   **Attributes:** `mode` (`ONLINE`, `HYBRID`, `LOCAL`, `INTERMITTENT`, `PARTITIONED`), `neighbor_count`, `active_transports`, `gateway_available`.
*   **Role:** Determines runtime behavior regarding discovery frequency, caching, DTN buffering, and synchronization urgency.

---

## 4. Distinction Between Identities and Addresses

In traditional cloud and client-server architectures, identity, location, and network address are conflated into IP addresses, DNS hostnames, and server URLs. In FIELDNET, these concepts are strictly separated:

| Identity Concept | Definition | Mutability | Example |
| :--- | :--- | :--- | :--- |
| **Device Identity** | Hardware or OS-specific physical identifier (e.g., hardware UUID, IMEI, platform serial). | Tied to physical hardware; immutable to user; never exposed to untrusted peers. | `HW-9842-AF20-44BC` |
| **Node Identity** | Cryptographic keypair (Ed25519 public key). The sovereign identity of the logical participant. | Globally unique; permanent across networks, reboots, and IP reassignments. | `ed25519:7F8a9B...c3D` |
| **Network Address** | Transient locator bound to a specific physical or virtual network interface on a given transport. | Highly ephemeral; changes when moving between Wi-Fi networks, cellular, or Bluetooth. | `192.168.4.12:4433`, `ble:D4:36:39:AA:BB:CC` |
| **Service Identity** | Abstract semantic name describing an intended capability or functional contract. | Defined by specification or manifest; independent of provider node or host. | `ai.text.summarize` |
| **Service Instance Identity** | Unique tuple denoting an active deployment of a service on a concrete node. | Ephemeral; created when service starts, expires when service or node terminates. | `inst-98f2@node-7F8a9B` |
| **Content Identity** | Cryptographic hash of an immutable block of bytes (CID). | Absolute immutability. If data changes by even 1 bit, Content Identity changes. | `sha256:e3b0c44298fc...` |

### Architectural Implication
Applications must never bind or connect to a raw **Network Address** or assume a static **Device Identity**. Instead:
1. Applications request a **Service Identity** (e.g., `storage.put`).
2. The runtime resolves that request to an authorized **Service Instance Identity** on an authenticated **Node Identity**.
3. The runtime’s transport and routing engines map the Node Identity to whatever transient **Network Addresses** and transports are currently viable.

---

## 5. Core Architectural Principles

These eleven principles govern all design decisions and code implementations in FIELDNET:

1.  **Local-First Operation:** All core operations (data creation, reading, local search, document editing, local service execution) must function without internet access. Network communication is an asynchronous enhancement, not a synchronous bottleneck.
2.  **No Mandatory Central Server:** The environment must be fully functional if every server on the global internet is completely vaporized. No external coordinator, DNS server, CA authority, or cloud broker may be required.
3.  **Service-Centric Architecture:** Applications declare *what* capability they need, not *who* provides it or *where* it is hosted. Nodes dynamically discover and bind to services based on proximity, capability, and trust.
4.  **Identity Independent of Network Address:** A node's identity is anchored in cryptographic keys. A node retains its identity, open sessions, and permissions even if its IP address changes, if it switches from Wi-Fi to Bluetooth, or if it has no IP address at all.
5.  **Transport Independence:** Subsystems and applications communicate through abstract channels. The protocol layer operates transparently over TCP, UDP, WebRTC, Bluetooth LE, Wi-Fi Direct, Wi-Fi Aware, or raw datagrams.
6.  **Persistent Messages:** When a message cannot be delivered immediately due to route failure or recipient absence, it is not dropped. It transitions into a persistent Delay-Tolerant Network (DTN) queue to be forwarded upon future contact opportunities.
7.  **Eventual Consistency:** Distributed state is managed using state-based or operation-based Conflict-Free Replicated Data Types (CRDTs) and operation logs. Nodes converge deterministically when connectivity is restored, without requiring distributed locks.
8.  **Capability-Based Access:** Possession of a network connection does not confer authorization. Applications execute inside sandboxes and can only invoke operations for which they hold cryptographically verifiable capability tokens.
9.  **Failure as a Normal Operating Condition:** Disconnections, crashes, partitions, timeouts, and route failures are treated as ordinary operational events rather than exceptions. The system is architected to remain stable and progress under continuous partial failure.
10. **Internet as an Optional Capability:** When global internet access is available, it is represented as a discoverable `internet.gateway` service. Nodes utilize it for external traffic, but its absence degrades nothing in the local environment.
11. **Security by Default:** Unauthenticated nodes are untrusted. Discovery does not equal authentication; authentication does not equal authorization. All protocol frames are authenticated, encrypted, and replay-protected.

---

## 6. FIELDNET System Boundary

To ensure proper modularity, a strict boundary is enforced between the Host Operating System and the FIELDNET Runtime:

```text
+--------------------------------------------------------------------------+
|                          HOST OPERATING SYSTEM                           |
|                                                                          |
|  - Hardware Drivers (Wi-Fi, Bluetooth, Storage controllers, Cellular)   |
|  - Filesystem Primitives (Raw read/write, directory creation, sync)      |
|  - Process & Thread Primitives (OS threads, process spawning, signals)   |
|  - Platform Networking APIs (Berkeley sockets, packet filters)           |
|  - Bluetooth / Wi-Fi Platform APIs (CoreBluetooth, Android BLE, BlueZ)   |
|  - GPU / Hardware Device Access (Vulkan, Metal, OpenCL, DirectML)         |
|  - OS Security Facilities (Platform Keystore/Keychain, OS Sandboxing)   |
+--------------------------------------------------------------------------+
                                    |
                          Operating System Boundary
                                    |
                                    v
+--------------------------------------------------------------------------+
|                             FIELDNET RUNTIME                             |
|                                                                          |
|  - Sovereign Cryptographic Identity (Ed25519 key generation, sign/verify) |
|  - Multi-Transport Discovery (mDNS, BLE beacons, Wi-Fi Direct probes)    |
|  - Peer & Session Lifecycle Management (Handshakes, session encryption)  |
|  - Transport Abstraction Layer (Multiplexing over heterogeneous links)   |
|  - Message Framing & Distributed Protocol Engine                         |
|  - Multi-Hop Dynamic Routing (Ad-hoc path selection, distance vector)   |
|  - Delay-Tolerant Networking (DTN) & Store-Carry-Forward Buffer         |
|  - Distributed State Synchronization (CRDTs, causal logs, Merkle sync)   |
|  - Content-Addressed Storage & Transfer (Chunking, SHA-256 verification) |
|  - Dynamic Service Registry & Capability Resolver                        |
|  - Application Sandboxing (Wasmtime runtime, memory/CPU quotas)          |
|  - Distributed Compute Scheduling & Job Dispatch                         |
|  - Internet Gateway Abstraction & Proxy Management                       |
|  - Capability Token Issuance & Cryptographic Permission Enforcement      |
|  - Environment APIs exposed to applications and external web browsers    |
+--------------------------------------------------------------------------+
```

---

## 7. Connectivity States and Operational Semantics

FIELDNET classifies its operating state into five distinct connectivity states:

```text
                  +-----------------------+
                  |        ONLINE         |  (Global Internet direct)
                  +-----------------------+
                             ^
                             | Internet available
                             v
                  +-----------------------+
                  |        HYBRID         |  (Local Mesh + Gateway to Internet)
                  +-----------------------+
                             ^
                             | Internet lost / Gateway lost
                             v
                  +-----------------------+
                  |         LOCAL         |  (Local Mesh active, no Internet)
                  +-----------------------+
                        ^           ^
          Rapid drops   |           | Physical separation
          & reconns     v           v
  +-----------------------+       +-----------------------+
  |     INTERMITTENT      |       |      PARTITIONED      |
  | (Opportunistic links) |       | (Isolated sub-graphs) |
  +-----------------------+       +-----------------------+
```

### 7.1 Detailed State Specifications

#### 1. ONLINE
*   **Condition:** The local node has direct, uninhibited access to the global internet via a local network interface (e.g., cellular data or home broadband) without requiring an ad-hoc peer relay.
*   **Runtime Behavior:**
    *   The node registers itself as an available `internet.gateway` within the local environment if permitted by local owner policy.
    *   Applications can communicate directly with internet endpoints via the gateway abstraction.
    *   Content caches and package repositories can be refreshed from external sources.
    *   Local-first operations continue unchanged; local data is never sent to external servers unless explicitly routed to an external service.

#### 2. HYBRID
*   **Condition:** The node is participating in a local FIELDNET peer environment, and at least one accessible peer is advertising an active `internet.gateway` service, even though the local node may not possess direct internet access.
*   **Runtime Behavior:**
    *   The runtime routes external internet requests transparently through the peer advertising the gateway service, subject to bandwidth and capability policies.
    *   Internal environment traffic remains purely local and is never routed through external infrastructure.
    *   Large downloads are chunked, cached, and content-addressed at the gateway peer and replicated over local transports.

#### 3. LOCAL
*   **Condition:** Two or more nodes are communicating over local transports (Wi-Fi LAN, Wi-Fi Direct, Bluetooth LE, Ethernet). Zero nodes have access to the global internet.
*   **Runtime Behavior:**
    *   All discovery mechanisms focus entirely on local link-layer discovery.
    *   Services are resolved strictly against the local peer registry.
    *   Distributed data sync (CRDTs) proceeds at full local network bandwidth.
    *   External internet requests are suspended, queued, or returned with `CapabilityUnavailable(internet.gateway)` according to application preferences.
    *   The local web gateway (`fieldnet.local`) remains fully functional for standard web browsers on the local network.

#### 4. INTERMITTENT
*   **Condition:** Physical link conditions fluctuate rapidly (e.g., moving devices, weak BLE RSSI, high packet loss, nodes entering and exiting radio range).
*   **Runtime Behavior:**
    *   Direct interactive streams are demoted in favor of datagram messaging and DTN store-and-forward.
    *   Aggressive link-layer keepalives are disabled to prevent battery exhaustion.
    *   Messages are marked with priorities and buffered in the persistent DTN queue.
    *   Opportunistic sync initiates whenever a peer link is established, utilizing compact causal clocks to exchange minimal deltas.

#### 5. PARTITIONED
*   **Condition:** A node is completely isolated from all peers, or a group of nodes is isolated into a partition with no route to the rest of the environment.
*   **Runtime Behavior:**
    *   Nodes continue operating completely autonomously. Applications write to local storage and generate local operation logs without blocking.
    *   Outbound messages for disconnected peers are serialized to persistent SQLite storage.
    *   Local service calls resolve to local providers; remote service calls fail gracefully or queue for deferred execution.
    *   Upon partition healing, nodes perform bidirectional Merkle-tree synchronization to reconcile divergent state.

---

## 8. Failure Is a Normal State

In traditional distributed computing, network partitions, link tears, and host crashes are treated as anomalous failure conditions that trigger panics, dropped connections, user error dialogues, or cascading timeouts.

In FIELDNET, **failure is treated as an ordinary, expected state of daily operation**. The system guarantees the following invariant behaviors under failure:

### 8.1 Specific Failure Handling Modes

| Failure Event | Traditional Architecture Behavior | FIELDNET Runtime Behavior |
| :--- | :--- | :--- |
| **Peer Disappearance** | Socket breaks, application crashes or hangs, ongoing transaction aborts. | Peer state transitions to `DISCONNECTED`. Session keys remain cached. Active requests to that peer are re-routed to an alternate provider or transitioned to the DTN queue. |
| **Route Failure** | ICMP Destination Unreachable, TCP RST, connection closed. | The Routing Engine invalidates the cached path, broadcasts a route inquiry or computes an alternate path via another neighbor (BFS/Dijkstra), without breaking application state. |
| **Transport Failure** (e.g., Wi-Fi drops) | Network stack shuts down, all sockets destroyed. | The Transport Abstraction Layer migrates active logical connections to an alternate physical transport (e.g., fallback from Wi-Fi to Bluetooth LE) using the same Node Identity. |
| **Network Partition** | "Split-brain" errors, distributed locking hangs, total system freeze. | The partition splits gracefully into independent autonomous sub-environments. Local writes proceed via CRDTs. Reconnection triggers conflict-free reconciliation. |
| **Delayed Delivery** | Connection timeouts, HTTP 504 Gateway Timeout, packet drops. | The DTN Engine accepts the message into persistent storage, returns a `Queued` acknowledgement to the local app, and delivers the message hours or days later when contact recurs. |
| **Service Failure** | Unhandled exception, HTTP 500 Internal Server Error. | Service Resolver marks the instance as `FAILED`, invalidates the endpoint, and immediately attempts dynamic failover to another node advertising the same `service_id`. |
| **Device Restart** (Abrupt power loss) | Corrupted cache, uncommitted writes lost, broken state. | Runtime initializes from durable SQLite state, verifies content chunk integrity, restores persistent DTN queues, and resumes background sync without data corruption. |
| **Intermittent Internet** | Infinite spinner, UI freezes, failed API handshakes. | Internet access is an optional capability. The runtime flips the `internet.gateway` status flag to unavailable. Applications degrade to local-only mode seamlessly. |

---

## 9. Comprehensive Reference Scenario

### 9.1 Setup
Three devices—**Device A** (Smartphone), **Device B** (Laptop), and **Device C** (Tablet)—run the FIELDNET runtime and form a local environment.

*   **Node A** (`node_id: A`): Resource-constrained mobile device. Runs a disaster coordination application.
*   **Node B** (`node_id: B`): Powerful laptop with GPU. Advertises `service_id: ai.inference.v1`.
*   **Node C** (`node_id: C`): Tablet with high-capacity flash storage. Advertises `service_id: storage.replicated.v1`.

```text
    +-----------+           +-----------+
    |  Node A   | --------> |  Node B   |  (Provides ai.inference.v1)
    | (Mobile)  |           | (Laptop)  |
    +-----------+           +-----------+
          |
          |
          v
    +-----------+
    |  Node C   |  (Provides storage.replicated.v1)
    | (Tablet)  |
    +-----------+
```

### 9.2 Execution Trace

#### Step 1: Service Request & Dispatch
1.  The user on **Node A** captures field assessment notes and requests semantic categorization and emergency response prioritization.
2.  The application on Node A does **not** invoke an IP address or fixed hostname. It calls:
    ```rust
    env.services().resolve("ai.inference.v1").await?;
    ```
3.  Node A’s runtime inspects its Service Registry, identifies that **Node B** hosts an active `Service Instance` of `ai.inference.v1`, verifies Node B’s cryptographic authorization, and opens an authenticated session over Wi-Fi.
4.  Simultaneously, Node A writes the raw assessment report into the distributed storage system by requesting `storage.replicated.v1`. Node C resolves as the storage provider, and the content chunks (identified by SHA-256) replicate to Node C.

#### Step 2: Global Internet Disappears
1.  A severe storm downs local cellular towers and fiber backhauls.
2.  All three nodes detect that external gateways are unreachable.
3.  **Impact on FIELDNET:** Zero. The environment transitions from `ONLINE` to `LOCAL`. Local radios continue communicating directly. The user on Node A observes no interruption, no error popups, and no blocked inputs.

#### Step 3: Node B Disconnects (Physical Departure)
1.  The operator carrying **Node B** walks away to survey another zone, moving out of Wi-Fi and Bluetooth radio range.
2.  Node A’s Connection Manager detects missed heartbeats from Node B. Node B’s status transitions from `CONNECTED` to `DISCONNECTED`.
3.  The user on Node A submits another assessment task for AI analysis.
4.  **Runtime Action:**
    *   Node A’s Service Resolver detects that no active instance of `ai.inference.v1` is currently reachable.
    *   Because the message priority is non-realtime, the protocol engine queues the job payload into the **DTN Persistent Store**.
    *   The application receives a status of `Accepted(DeferredExecution)`. The UI shows "Queued for processing when compute node returns".
    *   Node A continues operating. The application remains responsive.

#### Step 4: Independent Concurrent Edits
1.  While separated, the user on Node A edits the emergency mission manifest. The document is backed by a CRDT map.
2.  The user on Node B opens the same document locally on their laptop and records incoming field rescue records.
3.  Both nodes append operations to their independent local oplogs.

#### Step 5: Reconnection and Convergence
1.  Node B returns to the base area and enters Bluetooth LE range of Node C, which acts as a bridge, followed by direct Wi-Fi contact with Node A.
2.  **Discovery & Handshake:** Node A and Node B exchange cryptographic discovery nonces and establish an encrypted session.
3.  **DTN Drain:** Node A’s DTN Engine detects that a route to Node B has reappeared. The queued inference job is automatically forwarded to Node B. Node B processes the job and returns the output to Node A.
4.  **State Synchronization:** The Sync Engines of Node A, Node B, and Node C exchange causal state vectors. Missing operations from the disconnected period are streamed and deterministically merged via CRDT rules.
5.  **Result:** All three nodes converge to the exact same document state without manual conflict resolution or data loss.

### 9.3 Why the Application Must Not Know the Physical Topology
If the application on Node A had been written to depend on an IP address (e.g., `http://192.168.1.50:8080/inference`):
*   When Node B moved and acquired a different IP or switched to BLE, the application would have crashed with `ECONNREFUSED` or `ETIMEDOUT`.
*   The application developer would have had to manually write retry loops, local storage queues, reconnection monitors, and transport migrators.
*   By abstracting the network into **Services**, **Capabilities**, and **DTN Messages**, FIELDNET absorbs all topological turbulence inside the runtime, leaving applications simple, robust, and completely offline-capable.

---

## 10. Phase 01 Architectural Invariants

The following invariants must hold true across all subsequent implementation phases:

1.  **Sovereign Private Keys:** Private cryptographic keys are generated locally and must never be transmitted across any network or transport.
2.  **Explicit Authorization:** Discovery does not imply trust. A node or application can only access resources for which it holds an explicit, verifiable capability grant.
3.  **Zero Central Dependencies:** No runtime component may depend on an external cloud server, central database, or fixed coordinator for local operation.
4.  **Persistent Queue Survival:** Runtime restarts or crashes must never silently discard unexpired messages residing in the persistent DTN queue.
5.  **Immutable Content Identity:** Content must be addressed strictly by the cryptographic hash of its payload. Tampered or corrupted chunks must fail verification and be rejected.
6.  **Deterministic Convergence:** Replicated data managed via CRDTs must converge to identical state across all nodes given the same set of operations, regardless of delivery order.
7.  **Capability Sandboxing:** Applications executed by the runtime (e.g., WASM modules) must never bypass the Environment API to access raw host OS memory, filesystems, or sockets.

---

## 11. Scope Boundaries for Phase 01

To ensure disciplined execution, the following are explicitly **non-goals** for Phase 01:
*   Writing production networking, socket, or BLE drivers.
*   Creating mock or fake networking implementations.
*   Building graphical user interfaces or web frontends.
*   Adding external dependencies before the core workspace crates are established.
*   Implementing CRDT algorithms, WASM runtimes, or protocol engines prematurely.

Phase 01 is strictly dedicated to establishing the authoritative conceptual, architectural, and systemic foundation of FIELDNET.
