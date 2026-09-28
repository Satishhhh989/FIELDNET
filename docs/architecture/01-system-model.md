# FIELDNET Architecture Specification
## Phase 01 — Document 01: Formal System & Environment Model

**Document ID:** `DOC-ARCH-01-SYSTEM-MODEL`  
**Phase:** Phase 01 (System Model & Core Principles) — Prompts 01, 02 & 03  
**Status:** Authoritative Architectural Specification  
**Version:** 1.2  
**References:** `FIELDNET_MASTER_SPEC.md`  

---

## 1. Executive Definition and Scope

### 1.1 What FIELDNET Is
**FIELDNET** is a **distributed offline operating environment** that executes in userspace on top of existing host operating systems (e.g., Linux, macOS, Windows, Android, iOS). It allows physically proximate or ad-hoc connected computing devices to dynamically discover one another, establish authenticated communication, and form a resilient local computing ecosystem. Within this ecosystem, applications, services, data, and computation continue to operate deterministically regardless of whether global internet connectivity is intermittent, degraded, or completely absent.

FIELDNET treats the global internet as an **opportunistic capability** rather than an essential prerequisite for computing.

### 1.2 What FIELDNET Is Not
To maintain architectural discipline and prevent scope degradation, FIELDNET is explicitly defined by what it is **not**:

*   **NOT a Replacement Operating System:** FIELDNET does not replace the host operating system kernel, hardware drivers, low-level process schedulers, or hardware-level memory management. It operates entirely in userspace on top of host OS primitives. Rust is utilized as an implementation technology for the core runtime, not as an operating system kernel.
*   **NOT Merely a Mesh Networking Application:** FIELDNET contains multi-hop and mesh routing capabilities, but its scope extends far beyond packet transport. It provides an application execution runtime (WASM), capability-based access control, distributed storage, delay-tolerant messaging, CRDT state synchronization, and dynamic service resolution.
*   **NOT Merely an Offline Chat Application:** Ephemeral peer-to-peer messaging is only one generic communication primitive within FIELDNET. Chat is a user-plane application, not the system itself.
*   **NOT Merely a File-Sharing Application:** While content-addressed chunking and data replication are built into the runtime, they serve as foundational primitives for application state, software distribution, and persistent storage rather than simple point-to-point file transfers.
*   **NOT a Cloud Replacement:** FIELDNET is not designed to emulate massive hyperscale centralized data centers over local radios. It is optimized for local-first, low-latency, partitioned, and bandwidth-constrained ad-hoc collaboration.
*   **NOT a Central-Server Architecture:** There is no master coordinator, central directory, cloud broker, or anchor server required to bootstrap, validate, route, or maintain state in a FIELDNET environment.

---

## 2. Conceptual Architecture Diagrams

### 2.1 Layered Runtime & Environment Hierarchy
The following diagram illustrates the stack within an individual device, from physical hardware up to the shared logical Environment and Applications:

```text
+--------------------------------------------------------------------------+
|                             PHYSICAL DEVICE                              |
|           (Smartphone, Laptop, Tablet, Single-Board Computer)            |
+--------------------------------------------------------------------------+
                                    |
                                    v hosts
+--------------------------------------------------------------------------+
|                          HOST OPERATING SYSTEM                           |
|                    (Linux, macOS, Windows, Android, iOS)                 |
+--------------------------------------------------------------------------+
                                    |
                                    v executes
+--------------------------------------------------------------------------+
|                             FIELDNET RUNTIME                             |
|                    (Userspace Engine / Rust Core Process)                 |
+--------------------------------------------------------------------------+
                                    |
                                    v creates & participates in
+--------------------------------------------------------------------------+
|                           LOGICAL ENVIRONMENT                            |
|  +--------------------------------------------------------------------+  |
|  |                             NODE                                   |  |
|  |   (Cryptographic Keypair, Sovereign Identity, Local State Store)  |  |
|  +--------------------------------------------------------------------+  |
|                                   |                                      |
|    +------------------------------+------------------------------+       |
|    |                              |                              |       |
|    v                              v                              v       |
|  IDENTITY                    NETWORKING                       STORAGE    |
|  - Ed25519 Keys              - Transports (BLE/Wi-Fi/TCP)     - SQLite   |
|  - Capability Tokens         - Routing & DTN Store            - CRDT Log |
|  - Trust Policies            - Protocol Engine                - Content  |
|    |                              |                              |       |
|    +------------------------------+------------------------------+       |
|    |                              |                              |       |
|    v                              v                              v       |
|  SERVICES                    APPLICATIONS                     COMPUTE    |
|  - Service Registry          - Sandboxed WASM Engine          - Scheduler|
|  - Dynamic Resolver          - Environment API                - Dispatch |
|  - Local Web Gateway         - Workspaces & Tools             - Workers  |
|    |                              |                                      |
|    +------------------------------+                                      |
|    |                                                                     |
|    v                                                                     |
|  GATEWAY                                                                 |
|  - Internet Egress Proxy (Optional Capability)                           |
+--------------------------------------------------------------------------+
```

### 2.2 Multi-Device Distributed Environment
A single logical Environment is formed collaboratively across multiple independent physical devices:

```text
+-----------------------+     Ad-Hoc Wi-Fi     +-----------------------+
|       DEVICE A        | <==================> |       DEVICE B        |
|  (Host OS + Runtime)  |                      |  (Host OS + Runtime)  |
|  Node ID: ed25519:A   |                      |  Node ID: ed25519:B   |
|  Provides: Client UI  |                      |  Provides: AI Service |
+-----------------------+                      +-----------------------+
           \                                              /
            \                                            /
  Bluetooth  \                                          /  Wi-Fi Direct
     LE       \                                        /
               v                                      v
            +--------------------------------------------+
            |                  DEVICE C                  |
            |            (Host OS + Runtime)             |
            |            Node ID: ed25519:C              |
            |     Provides: Replicated Storage           |
            |     Provides: internet.gateway (Optional)  |
            +--------------------------------------------+
                                  |
                                  | (When available)
                                  v
                     ===========================
                          GLOBAL INTERNET
                     ===========================

      ==========================================================
             SHARED LOGICAL FIELDNET ENVIRONMENT (UUID / Hash)
       (Distributed State, Content Space, Dynamic Service Registry)
      ==========================================================
```

---

## 3. Formal System & Environment Model

An **Environment** is the logical computing ecosystem formed by FIELDNET nodes that can discover, communicate, exchange state, provide services, share content, and perform computation without requiring external infrastructure.

### 3.1 Entity Matrix

| Entity | Primary Purpose | Identity Form | Lifetime | Persistence Across Reboots? | Can Exist Independently? |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Environment** | Boundary of collaboration, discovery, and trust | `Environment ID` (UUID / Domain Hash) | Dynamic / Persistent domain | Yes (Metadata & cached state) | No (Requires >= 1 Node) |
| **Node** | Autonomous cryptographic participant & state holder | `Node ID` (Ed25519 Public Key) | Permanent across reboots | Yes (Keys & state in SQLite) | Yes (In PARTITIONED state) |
| **Device** | Physical hardware hosting the OS and execution platform | `Device ID` (Hardware / OS UUID) | Physical lifetime of hardware | Yes (Hardware identity) | Yes (Without FIELDNET) |
| **Runtime** | Userspace engine executing FIELDNET subsystems | `Runtime Instance ID` (Process UUID) | Process execution lifetime | No (Recreated on startup) | No (Requires Device & OS) |
| **Application** | Sandboxed software providing user or system capability | `App ID` (Manifest URI / Hash) | Installation & Execution | Yes (Installed code & manifest) | No (Requires Runtime) |
| **Service** | Abstract functional contract (e.g., `ai.inference`) | `Service ID` (Semantic URI) | Specification / Registry scope | Yes (Declared in registry) | No (Requires Specification) |
| **Service Instance** | Concrete running deployment of a service on a node | `Instance ID` (Instance UUID + Node ID) | Service execution lifetime | No (Re-registered on start) | No (Requires Node & Service) |
| **Peer** | Local node's internal model of a remote node | `Peer Node ID` (Remote Node ID) | Session / Encounter lifetime | Yes (Cached trust & route data) | No (Requires Remote Node) |
| **Resource** | Physical/logical commodity (CPU, RAM, GPU, Battery) | `Resource ID` (Type + Specifier) | Continuous physical state | No (Dynamic metrics) | No (Bound to Device) |
| **Capability** | Unforgeable authorization to perform an operation | `Capability ID` (Signed Token) | Defined by token expiration | Yes (If token is durable) | No (Requires Issuer & Grantee) |
| **Message** | Atomic envelope of communication routed between nodes | `Message ID` (Cryptographic Hash/UUID) | Transit or DTN queue lifetime | Yes (In DTN persistent queue) | No (Requires Sender Node) |
| **Content** | Immutable, chunked, content-addressed data object | `Content ID` (Cryptographic Hash / CID) | Indefinite (Subject to GC/pin) | Yes (In Content Store) | Yes (Location independent) |
| **Gateway** | Bridge service mediating external network access | `Gateway ID` (Service Instance ID) | Upstream network availability | No (State is dynamic) | No (Requires Node & Uplink) |
| **Workspace** | Collaborative container for documents, data, & state | `Workspace ID` (UUID / Topic Hash) | Project / Group lifecycle | Yes (Backed by CRDT store) | Yes (Synced across nodes) |

---

## 4. Formal Relational Grammar of FIELDNET

To establish unambiguous architectural clarity without descending into impenetrable academic formalism, the relational grammar of FIELDNET entities is defined as follows:

```text
Environment
    contains ───────> Nodes

Device
    hosts ──────────> Host OS
        executes ───> Runtime
            creates ─> Node

Runtime
    hosts ──────────> Applications
    hosts ──────────> Services
    owns/manages ───> Resources
    exposes ────────> Capabilities

Service
    manifests as ───> Service Instances (0..N)

Node
    participates in ─> Environment Graph (as a Graph Vertex)
    connects to ────> Peers (as Graph Edges)
    maintains ──────> Local Storage (SQLite Store + DTN Store)

Content
    exists independently of ─> Physical Storage Location

Messages
    move through ───> Nodes / Routes / DTN Queues
```

### Key Relational Clarifications
*   **Nodes, Not Services, Form the Connectivity Graph:** The network topology graph consists strictly of **Nodes** (vertices) and physical/logical transport connections (edges). Services and Content reside on nodes; they are not graph routing vertices.
*   **Runtime vs. Node:** A `Runtime` is an OS process instance; a `Node` is a sovereign cryptographic persona. If the runtime process restarts, it re-instantiates the *same* logical Node because the Node's identity is anchored in durable cryptographic keys.
*   **Resources vs. Capabilities:** Resources are physical capacities managed by the runtime; Capabilities are cryptographically validated authorizations to use those capacities.

---

## 5. The Environment Graph: Physical Topology vs. Logical Environment

The FIELDNET Environment is modeled as an abstract, dynamic graph:
$$G_{\text{env}} = (V_{\text{nodes}}, E_{\text{links}})$$
where:
*   $V_{\text{nodes}}$ is the set of active, authenticated logical **Nodes**.
*   $E_{\text{links}}$ is the set of currently viable direct communication links across all active transports (Wi-Fi, BLE, Ethernet, etc.).

```text
[ PHYSICAL TOPOLOGY: Dynamic & Volatile ]
  Device A <--- BLE (Weak RSSI) ---> Device C
     ^                                   ^
     |                                   |
  Wi-Fi Direct (High Loss)          Ad-Hoc Wi-Fi
     |                                   |
     v                                   v
  Device B <-------------------------> Device D

                     VS.

[ LOGICAL ENVIRONMENT: Stable & Continuous ]
+-------------------------------------------------------------+
|                     LOGICAL ENVIRONMENT                     |
|                                                             |
|  - Node ed25519:A   (Client Application)                    |
|  - Node ed25519:B   (ai.inference Provider)                 |
|  - Node ed25519:C   (storage.replicated Provider)           |
|  - Node ed25519:D   (internet.gateway Provider)             |
|                                                             |
|  - Shared CRDT Document Workspaces                          |
|  - Unified Content-Addressed Hash Space (CIDs)              |
|  - Persistent DTN Message Store (Store-Carry-Forward)       |
+-------------------------------------------------------------+
```

### Critical Axiom: PHYSICAL TOPOLOGY $\neq$ LOGICAL ENVIRONMENT
1.  **Topology Is Transient:** Physical links ($E_{\text{links}}$) are inherently volatile. Devices move, battery savers disable radios, walls attenuate 5 GHz signals, and Bluetooth links drop packets.
2.  **The Environment Persists:** The logical Environment remains intact even if $E_{\text{links}}$ degrades to the empty set ($\emptyset$) (e.g., complete network partition).
3.  **Services and Content Are Not Routing Vertices:** Services do not have routing tables; Nodes do. Content does not have an IP address; it is replicated across Nodes. If Node B hosting `ai.inference` departs, the Service Identity persists in the registry schema, and calls gracefully queue or fail over to another provider.

---

## 6. Location Independence Principles

In conventional systems, software is tightly coupled to physical addresses. In FIELDNET, **Location Independence** is enforced through six fundamental architectural separations:

```text
+-----------------------+     is NOT     +-----------------------+
|        Service        |   =========>   |      IP Address       |
+-----------------------+                +-----------------------+
|         Node          |   =========>   |    Network Socket     |
+-----------------------+                +-----------------------+
|        Content        |   =========>   |    Filesystem Path    |
+-----------------------+                +-----------------------+
|      Application      |   =========>   |      Process ID       |
+-----------------------+                +-----------------------+
|      Capability       |   =========>   |    Physical Device    |
+-----------------------+                +-----------------------+
|         Route         |   =========>   | Permanent Connection  |
+-----------------------+                +-----------------------+
```

### Detailed Rationales for Offline Environments
1.  **A Service Is Not an IP Address:** IP addresses change upon reconnecting or switching to Bluetooth. If applications targeted `192.168.1.50`, network roaming would shatter connections. By addressing `fieldnet://services/ai/inference`, the runtime dynamically resolves the best active provider.
2.  **A Node Is Not a Network Socket:** A socket is a transient OS handle. If a Wi-Fi connection drops and switches to BLE, all sockets are destroyed. Because a Node is an Ed25519 public key, logical sessions survive transport handovers.
3.  **Content Is Not a Filesystem Path:** Filesystem paths (`/home/user/map.dat`) are local and arbitrary. Content Identity is the cryptographic hash of the data (`sha256:...`). Nodes can retrieve chunks from any peer carrying the hash without caring about remote path structures.
4.  **An Application Is Not a Process ID:** Process IDs are ephemeral OS allocations. An application in FIELDNET is a sandboxed WASM module identified by manifest and code hash, executable deterministically across heterogeneous architectures.
5.  **A Capability Is Not a Physical Device:** Authorization is not granted because "this is Device X." It is granted via unforgeable cryptographic tokens specifying allowed actions, scopes, and expiration times.
6.  **A Route Is Not a Permanent Connection:** In ad-hoc meshes, static routes do not exist. A route is a transient, opportunistic path that can change on every message frame or degrade into delay-tolerant store-and-forward.

---

## 7. Environment Continuity vs. Network Connectivity

A foundational innovation of FIELDNET is the explicit separation between **Environment Continuity** and **Network Connectivity**.

```text
+--------------------------------------------------------------------------+
|                           NETWORK CONNECTIVITY                           |
|  - Physical radio state (Wi-Fi, Bluetooth, Cellular, Ethernet)           |
|  - Transient link margins (RSSI, Packet Loss, Link Bandwidth)            |
|  - Ephemeral IP leases, BLE connection intervals, Socket handles         |
|  - CHARACTERISTIC: Volatile, intermittent, local, easily severed.       |
+--------------------------------------------------------------------------+
                                     VS.
+--------------------------------------------------------------------------+
|                           ENVIRONMENT CONTINUITY                         |
|  - Sovereign Cryptographic Identities and Trust Domain                   |
|  - Durable CRDT Document States and Causal Operation Logs               |
|  - Content-Addressed Object Cache and Manifest Tree                      |
|  - Persistent DTN Store-Carry-Forward Queues                             |
|  - CHARACTERISTIC: Resilient, durable, partition-tolerant, continuous.   |
+--------------------------------------------------------------------------+
```

### Invariant Scenarios of Continuity
The Environment continues to exist and function under all of the following events:
*   **Internet Disappears:** The environment transitions from `HYBRID` to `LOCAL`. Local services, storage, and collaboration proceed without interruption.
*   **Nodes Disappear:** Peer records transition to `DISCONNECTED`. The remaining nodes continue full local collaboration.
*   **Nodes Restart:** Durable state in SQLite is reloaded. Cryptographic identities, DTN queues, and un-synced oplogs resume safely.
*   **Routes Change:** Multi-hop paths re-converge dynamically via routing tables or fall back to DTN store-and-forward.
*   **Services Move:** If a service provider disconnects, the Service Resolver dynamically redirects subsequent calls to an alternate provider or defers execution.
*   **Content Replicas Disappear:** As long as at least one reachable peer retains the chunks, content remains available. If all replicas depart, the hash remains indexed as `PendingRetrieval`.
*   **Partitions Occur:** Each partition becomes an autonomous sub-environment. Both partitions continue local edits via CRDTs. Reconnection triggers deterministic reconciliation.

---

## 8. Application Perspective & Abstraction Shield

Applications running on FIELDNET execute within a sandboxed environment (WASM) and view the system exclusively through the **Environment API**. The runtime serves as an **Abstraction Shield**, absorbing all distributed systems complexity.

```text
+--------------------------------------------------------------------------+
|                             APPLICATION                                  |
|                 (Collaborative Docs, Notes, Triage UI)                   |
+--------------------------------------------------------------------------+
                                    |
                                    v invokes
+--------------------------------------------------------------------------+
|                           ENVIRONMENT API                                |
|  - identity.*     - network.*      - service.*      - storage.*          |
|  - content.*      - events.*       - sync.*         - compute.*          |
+--------------------------------------------------------------------------+
                                    |
                         THE ABSTRACTION SHIELD
                                    |
                                    v absorbs & handles
+--------------------------------------------------------------------------+
|                         FIELDNET RUNTIME ENGINE                          |
|  - Which physical device hosts a requested service                       |
|  - What IP address or port is being used                                 |
|  - What physical transport (BLE, Wi-Fi, Ethernet) is currently active     |
|  - Whether a message was delivered directly, relayed, or queued in DTN   |
|  - Where content chunks are physically located across disk or peers      |
|  - Whether global internet connectivity is currently present or absent   |
+--------------------------------------------------------------------------+
```

### Architectural Contract
Applications:
1.  **Request capabilities and services**, never devices or IP addresses.
2.  **Read and write local data models**, trusting CRDT synchronization to handle propagation.
3.  **Handle asynchronous events and deferred results**, avoiding synchronous network deadlocks.

---

## 9. Failure Semantics & Categorical Taxonomy

To eliminate ambiguity, FIELDNET establishes a precise semantic taxonomy for all forms of system imperfection:

```text
                       SYSTEM IMPERFECTION EVENT
                                   |
         +-------------------------+-------------------------+
         |                                                   |
         v                                                   v
   DETERMINISTIC                                        TRANSIENT
  (Permanent Failure)                              (Temporary Condition)
         |                                                   |
         | Bad signature                             +-------+-------+
         | Malformed payload                         |       |       |
         | Capability rejected                       v       v       v
         | Resource quota breached                DELAY  PARTITION MIGRATION
                                                     |
                                                     v
                                             RETRYABLE FAULT
                                            (DTN Store / Failover)
```

### 9.1 Semantic Categorization

| Condition | Formal Definition | Operational Semantics | System Response |
| :--- | :--- | :--- | :--- |
| **Failure** | An operation terminates with an error due to non-recoverable invalidity. | The operation cannot succeed in its current form without code or policy modification. | Emit error to caller; do not retry automatically. |
| **Temporary Unavailability** | A service, peer, or gateway exists but cannot currently be reached or has exhausted short-term capacity. | Identity remains valid; endpoint is temporarily unreachable. | Queue request in DTN or fall back to alternate provider. |
| **Partition** | A topological severance splitting the environment graph into disjoint sets. | Communication across sets is impossible; within sets, communication is intact. | Independent local progress; merge state via CRDTs upon healing. |
| **Delay** | Significant latency introduced by multi-hop routing, channel contention, or store-carry-forward buffering. | Delivery time exceeds realtime thresholds but does not indicate data loss. | Acknowledge receipt to local caller as `Queued(DTN)`. |
| **Migration** | A service instance, role, or compute task transitions from one provider node to another. | The abstract service contract remains intact; the provider endpoint changes. | Invalidate old endpoint; resolve new provider instance. |
| **Retryable Failure** | A transient network drop, collision, or timeout during an active transmission. | The failure is circumstantial; repeating the attempt may succeed. | Exponential backoff and retry over alternate transport or route. |
| **Permanent Failure** | A cryptographic verification failure, unauthorized capability request, or corrupted payload. | Retrying will deterministically produce the exact same failure. | Drop frame immediately; log security event; alert application. |

---

## 10. System State Transitions & Lifecycles

The behavior of Nodes, Connectivity, and Services is governed by formal architectural lifecycle state models.

### 10.1 Node Lifecycle
```text
[ STARTUP ]
     |
     v
+--------------+     Configuration loaded,
| INITIALIZING | ──> keys verified from SQLite
+--------------+
     |
     v
+--------------+     Transports initialized,
|    READY     | ──> local services registered
+--------------+
     |
     v
+--------------+     Actively discovering, routing,
|   RUNNING    | <─┐ syncing, and hosting services
+--------------+   │
     |             │ Resource recovery /
     | Resource    │ link restored
     | exhaustion /│
     v low battery │
+--------------+   │
|   DEGRADED   | ──┘ Background tasks throttled,
+--------------+     high-power capabilities suspended
     |
     | Shutdown signal /
     v critical battery
+--------------+
|   STOPPING   | ──> Flush DTN queues, close DB clean
+--------------+
     |
     v
  [ HALT ]
```

### 10.2 Connectivity Lifecycle
```text
                  +-----------------------+
                  |        ONLINE         |  (Direct Global Internet)
                  +-----------------------+
                             ^
                             | Internet link detected
                             v
                  +-----------------------+
                  |        HYBRID         |  (Local Mesh + Gateway Peer to Internet)
                  +-----------------------+
                             ^
                             | Gateway lost / Internet lost
                             v
                  +-----------------------+
                  |         LOCAL         |  (Local Peer Mesh Active, No Internet)
                  +-----------------------+
                        ^           ^
          Rapid drops   |           | Physical distance / Obstacles
          & reconns     v           v
  +-----------------------+       +-----------------------+
  |     INTERMITTENT      |       |      PARTITIONED      |
  | (Opportunistic links) |       | (Isolated Node / Mesh)|
  +-----------------------+       +-----------------------+
```

### 10.3 Service Lifecycle
```text
  [ ADVERTISEMENT RECEIVED ]
              |
              v
       +--------------+
       |  DISCOVERED  |  (Payload seen via beacon / mDNS)
       +--------------+
              |
              | Verify provider node signature
              v
       +---------------+
       | AUTHENTICATED |  (Identity and schema verified)
       +---------------+
              |
              | Endpoint active & capability checked
              v
       +---------------+
       |   AVAILABLE   | <─┐
       +---------------+   │ Provider responds
              |            │ to probe
              | High load /│
              v packet loss│
       +---------------+   │
       |   DEGRADED    | ──┘
       +---------------+
              |
              | Missed heartbeats / node depart
              v
       +---------------+
       |  UNAVAILABLE  | ──> Triggers re-resolution to alternate provider
       +---------------+
```

---

## 11. Identity Separation Model

FIELDNET enforces strict **Identity Separation** across 9 distinct identity categories. These identities must never be used interchangeably.

```text
+--------------------------------------------------------------------------+
|                            IDENTITY TAXONOMY                             |
+--------------------------------------------------------------------------+
|  1. Device Identity       : Hardware / Platform UUID (e.g. Serial, IMEI) |
|  2. Node Identity         : Cryptographic Keypair (Ed25519 Public Key)   |
|  3. User Identity         : Human / Persona Principal Credential         |
|  4. Application Identity  : Package Manifest Hash / Developer Signature |
|  5. Service Identity      : Abstract Contract Name (e.g. ai.inference)   |
|  6. Service Inst. Identity: Concrete Execution Endpoint on a Given Node  |
|  7. Content Identity      : SHA-256 Hash of Payload (CID)                |
|  8. Message Identity      : Envelope Hash / Nonce-Seeded UUID            |
|  9. Environment Identity  : Collective Domain Hash / Genesis UUID        |
+--------------------------------------------------------------------------+
```

### 11.1 Detailed Breakdown of the 9 Identities

| Identity Type | Scope | Conflation Risk | Canonical Example |
| :--- | :--- | :--- | :--- |
| **1. Device Identity** | Physical hardware | Do NOT conflate with Node Identity. A device replacement must allow node migration. | `HW-APPLE-M2-7A9B42` |
| **2. Node Identity** | Logical participant | Do NOT conflate with IP address or network transport. Stable across all network changes. | `ed25519:7F8a9B01c2...` |
| **3. User Identity** | Human/Operator | Do NOT conflate with Node Identity. One user may operate multiple nodes (phone + laptop). | `user:satish@fieldnet.org` |
| **4. Application Identity** | Software package | Do NOT conflate with Service Identity. An app may provide zero, one, or multiple services. | `app:org.fieldnet.notes#v1.2` |
| **5. Service Identity** | Functional contract | Do NOT conflate with the host machine. Multiple nodes can offer the same service. | `fieldnet://services/ai/inference` |
| **6. Service Instance Identity** | Concrete running process | Do NOT conflate with Service Identity. Points to an exact instance on an exact node. | `inst-4a7b@node-7F8a9B` |
| **7. Content Identity** | Data payload | Do NOT conflate with file path or storage device. Derived purely from byte hash. | `sha256:e3b0c44298fc1c14...` |
| **8. Message Identity** | Atomic transmission | Do NOT conflate with payload hash. Distinct messages may carry identical content. | `msg-9821-f04b-bb21` |
| **9. Environment Identity** | Collaborative collective | Do NOT conflate with physical radio range or local Wi-Fi SSID. | `env:disaster-relief-alpha` |

---

## 12. Node vs. Device: Structural & Conceptual Separation

```text
+-------------------------------------------------------------+
|                       PHYSICAL DEVICE                       |
|   (Provides CPU, RAM, Radios, Platform Hardware Keystore)   |
+-------------------------------------------------------------+
                              |
                              v hosts
+-------------------------------------------------------------+
|                      FIELDNET RUNTIME                       |
|           (Userspace Tokio/Rust Execution Process)          |
+-------------------------------------------------------------+
                              |
                              v creates & orchestrates
+-------------------------------------------------------------+
|                        LOGICAL NODE                         |
|  - Sovereign Cryptographic Keypair (Ed25519)                |
|  - SQLite Database (Durable State & Oplogs)                 |
|  - Persistent DTN Store                                     |
|  - Capability Table & Trust Store                           |
+-------------------------------------------------------------+
          |                                         |
          v hosts                                   v hosts
+--------------------+                    +--------------------+
|    APPLICATIONS    |                    |      SERVICES      |
|   (WASM Modules)   |                    | (Service Instances)|
+--------------------+                    +--------------------+
```

### 12.1 Key Separation Principles
1.  **One Device May Host Multiple Nodes:** While typical consumer devices run a single node, the architecture explicitly supports a device hosting multiple independent logical nodes (e.g., for sandboxed multi-tenant profiles, testing topologies, or virtualized edge gateways).
2.  **One Node Belongs to One Runtime Instance:** A logical node is managed by exactly one runtime process at any given moment.
3.  **Node Identity Survives Reboots:** A device may experience sudden power loss, battery depletion, or manual OS restart. The node’s sovereign identity (Ed25519 private key) is securely retained in durable storage and restored upon reboot.
4.  **Decoupling from Hardware Identifiers:** FIELDNET strictly forbids using hardware MAC addresses, IMEI numbers, or CPU serial numbers as logical network identifiers. Hardware addresses change (e.g., MAC randomization on modern mobile OSs) and leak physical tracking information. Node identity is purely cryptographic and sovereign.

---

## 13. Resource vs. Capability: Access Control and Abstraction

Applications running inside FIELDNET must **never directly address physical hardware or raw OS devices**. The runtime enforces a strict separation between physical **Resources** and logical **Capabilities**.

```text
+--------------------------------------------------------------------------+
|                            PHYSICAL RESOURCES                            |
|  [CPU Cores]   [System RAM]   [Storage Flash]   [GPU Cores]   [Battery]  |
+--------------------------------------------------------------------------+
                                    |
                                    v managed and measured by
+--------------------------------------------------------------------------+
|                             FIELDNET RUNTIME                             |
|                        (Resource Control Engine)                         |
+--------------------------------------------------------------------------+
                                    |
                                    v authorized via unforgeable tokens
+--------------------------------------------------------------------------+
|                               CAPABILITIES                               |
|  - `storage.content.read`        - `compute.inference.gpu`               |
|  - `storage.content.write`       - `network.send.reliable`               |
|  - `service.host`                - `internet.gateway.egress`             |
+--------------------------------------------------------------------------+
                                    |
                                    v requested by
+--------------------------------------------------------------------------+
|                               APPLICATIONS                               |
|                     (Executing inside WASM Sandbox)                      |
+--------------------------------------------------------------------------+
```

### 13.1 Definitions
*   **Resource:** A physical or platform commodity present on a node (e.g., 8 CPU cores, 16 GB RAM, 256 GB flash, an NVIDIA/Apple Silicon GPU, 42% battery charge). Resources are finite, consumable, and hardware-specific.
*   **Capability:** A cryptographically verifiable authorization granting permission to execute a specific operation or utilize a bounded amount of a resource (e.g., `compute.inference.submit`, `storage.write.chunk`, `network.relay.forward`).

---

## 14. Core Architectural Principles

These eleven principles govern all design decisions and code implementations in FIELDNET:

1.  **Local-First Operation:** All core operations (data creation, reading, local search, document editing, local service execution) must function without internet access. Network communication is an asynchronous enhancement, not a synchronous bottleneck.
2.  **No Mandatory Central Server:** The environment must be fully functional if every server on the global internet is completely unavailable. No external coordinator, DNS server, CA authority, or cloud broker may be required.
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

## 15. FIELDNET System Boundary

To maintain system integrity, a strict boundary separates the **Host Operating System** and the **FIELDNET Runtime**:

```text
+--------------------------------------------------------------------------+
|                          HOST OPERATING SYSTEM                           |
|                                                                          |
|  - Hardware Drivers (Wi-Fi, Bluetooth, Storage controllers, Cellular)   |
|  - Filesystem Primitives (Raw block I/O, file descriptors, sync flags)   |
|  - Process & Thread Primitives (OS processes, threads, signals, epoll)   |
|  - Platform Networking APIs (BSD sockets, network interface states)      |
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
|  - Sovereign Cryptographic Identity (Ed25519 generation, sign, verify)  |
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
|  - Environment APIs exposed to applications and local web browsers       |
+--------------------------------------------------------------------------+
```

---

## 16. Dynamic Connectivity as State

### 16.1 Connectivity State Matrix

| State | Environmental Condition | What Remains Available | What May Fail | Do Local Apps Continue? | Messages Queued in DTN? | Synchronization Status | Is Internet Required? |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **ONLINE** | Direct, uninhibited global internet via local hardware interface (cellular/fiber). | All local services + external internet endpoints; node can act as `internet.gateway`. | Nothing under normal operations. | Yes | Yes (For offline peers) | Continuous with peers; external sync active | **No** (Optional enhancement) |
| **HYBRID** | Local peer mesh active; at least one peer offers `internet.gateway`. | All local services + external internet mediated via gateway peer. | High-bandwidth internet streams subject to gateway throttling. | Yes | Yes (For offline peers) | Full local sync; proxy-mediated external sync | **No** (Provided via peer) |
| **LOCAL** | Multiple peers connected via local radios (Wi-Fi/BLE/LAN). No internet available anywhere. | All local peer services, distributed storage, local AI, collaborative workspaces. | Direct external web requests (`*.com`) return `GatewayUnavailable`. | Yes | Yes (For absent peers) | Full speed across local transports | **No** |
| **INTERMITTENT** | Link quality fluctuates rapidly; nodes moving in/out of radio range; high packet drop. | Local-first reads/writes; opportunistically established connections. | Interactive streaming; long-lived TCP-like connections. | Yes | Yes (Immediate transition to DTN queue) | Opportunistic causal delta exchange | **No** |
| **PARTITIONED** | Complete isolation; node has zero reachable peers and zero internet. | Sovereign node operation: local database, local files, local compute, local UI. | All remote peer interactions and external services. | Yes | Yes (Buffered locally in durable SQLite) | Paused; queues up local operation log | **No** |

---

## 17. Failure Is a Normal Operating Condition

### 17.1 Comprehensive Failure Handling Catalog

| Failure Condition | Architectural Meaning | Conceptual System Response |
| :--- | :--- | :--- |
| **1. Node Disappears** | A participating node abruptly loses power, crashes, or is destroyed. | Remaining nodes transition the node's peer record to `DISCONNECTED`. Pending messages for it are moved into the persistent DTN queue. |
| **2. Peer Disappears** | A neighboring peer moves out of direct radio range. | Active link sessions are gracefully terminated. The routing engine updates neighbor tables. Multi-hop alternatives are queried. |
| **3. Route Disappears** | An intermediary relay node in a multi-hop path is lost. | Routing Engine invalidates the cached path, broadcasts a local route discovery request, or falls back to DTN store-and-forward. |
| **4. Service Disappears** | A node hosting a specific service instance terminates or unpublishes. | The Service Resolver detects heartbeat failure, marks the instance `UNAVAILABLE`, and automatically falls back to an alternate node offering the same `Service ID`. |
| **5. Gateway Disappears** | The node providing `internet.gateway` loses its uplink or departs. | The runtime transitions the environment connectivity state from `HYBRID` to `LOCAL`. Applications expecting external access degrade gracefully. |
| **6. Device Restarts** | Sudden hardware reboot, battery loss, or kernel panic on host OS. | The runtime restarts, loads sovereign keys and configuration from SQLite, verifies chunk integrity, recovers DTN queues, and resumes cleanly. |
| **7. Network Partitions** | The mesh is physically severed into two or more disjoint groups. | Both partitions continue functioning independently as autonomous sub-environments. Writes proceed via local CRDTs without distributed lock contention. |
| **8. Messages Are Delayed** | Delivery cannot occur immediately due to missing routes or sleep cycles. | Messages remain safe in durable DTN storage with priority, TTL, and backoff metadata until delivery opportunities arise. |
| **9. Messages Arrive Out of Order** | Multi-path transport routing delivers frame B before frame A. | Protocol engine uses monotonically increasing sequence numbers and causal vector clocks to reorder or causally buffer incoming frames. |
| **10. Content Temporarily Unavailable** | Content hash is known, but no reachable peer currently possesses the chunks. | Content Manager marks status as `PendingRetrieval`. A distributed content query remains active until a node carrying the hash is encountered. |
| **11. Compute Worker Disappears** | A remote node executing a dispatched compute job disconnects mid-task. | The Compute Scheduler detects worker timeout, resets the job state from `DISPATCHED` to `QUEUED`, and reschedules execution on another capable node. |

---

## 18. Multi-Device Lifecycle Validation Scenario

```text
    +-----------+           +-----------+
    |  Node A   | <-------> |  Node B   |  (Provides ai.inference.v1)
    | (Mobile)  |           | (Laptop)  |
    +-----------+           +-----------+
          ^                       ^
           \                     /
            \                   /
             v                 v
            +-------------------+
            |      Node C       |  (Provides storage.replicated.v1
            |     (Tablet)      |   Provides internet.gateway)
            +-------------------+
```

### 18.1 Step-by-Step Lifecycle Trace
1.  **Phase 1: Baseline Formation (Internet Available):** Devices A, B, and C discover each other. Node C provides `internet.gateway`. State is `HYBRID` for A and B.
2.  **Phase 2: Global Internet Disappears:** Node C loses uplink. Environment transitions to `LOCAL`. Local AI, notes, and storage proceed without a hiccup.
3.  **Phase 3: Node B Disconnects:** Node B departs. User on A requests AI inference. Request is saved in the **DTN Persistent Store**. Users on A and B make concurrent independent edits to shared CRDT documents.
4.  **Phase 4: Node B Returns:** Contact opportunity established. Node A drains DTN queue to Node B; Node B executes the AI task and returns results. Causal oplogs sync and merge without data loss.
5.  **Phase 5: Internet Returns:** Node C regains uplink. Environment transitions back to `HYBRID`. Background syncing with external clouds resumes.

---

## 19. Architectural Consistency Audit

The Phase 01 architectural specification was audited against the 12 core consistency questions:

| Consistency Question | Audit Finding | Status |
| :--- | :--- | :--- |
| **1. Is FIELDNET clearly NOT a replacement OS?** | Yes. Explicitly established as a userspace runtime running atop host OS primitives. | PASS |
| **2. Is Rust treated as an implementation technology, not a system concept?** | Yes. Rust is specified as the runtime implementation language, not as an architectural primitive. | PASS |
| **3. Is the runtime distinct from the node?** | Yes. Runtime is the OS process engine; Node is the persistent cryptographic participant. | PASS |
| **4. Is the node distinct from the device?** | Yes. Device is hardware; Node is sovereign cryptographic identity. Multi-node devices supported. | PASS |
| **5. Is the environment distinct from the network?** | Yes. Network is the transient link graph; Environment is the persistent collaborative domain. | PASS |
| **6. Is service identity independent from service location?** | Yes. Services are addressed by semantic URI contracts, dynamically resolved to instances. | PASS |
| **7. Is content identity independent from storage location?** | Yes. Content is identified strictly by SHA-256 payload hash (CID), completely decoupled from paths. | PASS |
| **8. Is internet access optional?** | Yes. Internet is modeled strictly as an optional `internet.gateway` capability. | PASS |
| **9. Is there any accidental central-server dependency?** | Zero. Peer discovery, routing, CRDT convergence, and local web gateway operate fully peer-to-peer. | PASS |
| **10. Are applications independent from physical networking details?** | Yes. Applications interact strictly with the Environment API and never touch raw sockets. | PASS |
| **11. Are failures treated as normal?** | Yes. 11 specific failure modes are codified as ordinary steady-state operating conditions. | PASS |
| **12. Are security concepts kept separate from networking concepts?** | Yes. Strict separation: Discovered $\neq$ Authenticated $\neq$ Authorized. | PASS |

---

## 20. System Model Invariants

The following authoritative invariants govern the FIELDNET system model and must remain inviolate throughout all subsequent implementation phases:

1.  **Internet Connectivity Is Optional:** Global internet connectivity is an available capability, never a prerequisite for local computing, storage, or communication.
2.  **A Device Is Not a Node:** A Device is physical hardware; a Node is an autonomous cryptographic participant. A device may host multiple nodes, and a node identity must survive physical device reboots.
3.  **Discovery Does Not Imply Trust:** The ability to discover a peer over a radio transport conveys zero security authorization.
4.  **Authentication Does Not Grant Authorization:** Verifying the cryptographic identity of a peer does not grant access to services or resources without an explicit, verifiable capability token.
5.  **Service Identity Is Independent of Physical Provider:** Applications must address services by semantic contract (e.g., `ai.inference`), never by IP address, MAC address, or fixed device hostname.
6.  **Content Identity Is Independent of Storage Location:** Content is identified exclusively by the cryptographic hash of its payload. A file has the same Content ID regardless of which node stores it.
7.  **Failure of One Node Must Not Invalidate the Environment:** The departure, crash, or compromise of any single node must never prevent remaining nodes from collaborating.
8.  **Local Operation Must Not Require Global Connectivity:** All primary user actions (document editing, reading, local search, local service execution) must function deterministically while completely offline.
9.  **Applications Interact Exclusively via Abstractions:** Applications must never bind directly to physical network sockets or hardware devices; all operations are mediated by the Environment API and capability grants.
10. **State Transitions Preserve Environment Identity:** Shifts between `ONLINE`, `HYBRID`, `LOCAL`, `INTERMITTENT`, and `PARTITIONED` states must never alter or invalidate the logical identity of the Environment.
11. **Private Keys Never Leave Their Node:** Sovereign private cryptographic keys are generated locally and must never be transmitted across any network or transport.
12. **Persistent Messages Survive Restarts:** Unexpired messages residing in the DTN store must never be silently discarded during runtime restarts or node crashes.

---

## 21. Final Phase 01 Verification Checklist

- [x] **FIELDNET Definition:** Distributed offline operating environment running in userspace atop host OS.
- [x] **System Boundary:** Host OS owns hardware/kernel primitives; FIELDNET owns identity, protocol, DTN, CRDTs, and sandboxing.
- [x] **Environment Model:** Defined as the logical computing ecosystem of participating nodes.
- [x] **Node Model:** Autonomous cryptographic participant with sovereign Ed25519 keypair.
- [x] **Device Model:** Physical hardware platform hosting OS and runtime.
- [x] **Runtime Model:** Userspace engine orchestrating subsystems, enforcing capabilities.
- [x] **Application Model:** Sandboxed WASM modules interacting exclusively via Environment API.
- [x] **Service Model:** Semantic contract independent of physical provider or host machine.
- [x] **Resource Model:** Quantifiable physical commodities (CPU, RAM, GPU, storage, battery).
- [x] **Capability Model:** Signed unforgeable tokens governing resource and service access.
- [x] **Identity Separation:** 9 distinct identities strictly demarcated against conflation.
- [x] **Connectivity States:** 5 dynamic states (`ONLINE`, `HYBRID`, `LOCAL`, `INTERMITTENT`, `PARTITIONED`).
- [x] **Environment Graph:** Conceptual graph model ($G_{\text{env}}$) where Physical Topology $\neq$ Logical Environment.
- [x] **Failure Semantics:** Formal taxonomy separating Failure, Temporary Unavailability, Partition, Delay, Migration, Retryable Fault, and Permanent Failure.
- [x] **Environment Continuity:** Defined independently of physical network connectivity.
- [x] **Application Abstraction:** The Abstraction Shield insulating applications from network turbulence.
- [x] **Architectural Invariants:** 12 codified system invariants.
- [x] **Scenario Validation:** 5-phase trace across Devices A, B, and C proving local-first continuity.
- [x] **Master-Spec Alignment:** 100% verified consistency with `FIELDNET_MASTER_SPEC.md`.

---

## 22. Scope Boundaries and Non-Goals for Phase 01

To ensure disciplined execution, the following are explicitly **non-goals** for Phase 01:
*   Writing production networking, socket, or BLE drivers.
*   Creating mock or fake networking implementations.
*   Building graphical user interfaces or web frontends.
*   Adding external dependencies before the core workspace crates are established.
*   Implementing CRDT algorithms, WASM runtimes, or protocol engines prematurely.

Phase 01 is strictly dedicated to establishing the authoritative conceptual, architectural, and systemic foundation of FIELDNET.
