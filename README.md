# FIELDNET — Autonomous Offline Operating Environment & Distributed Runtime

[![Rust](https://img.shields.io/badge/rust-2021%20edition-orange.svg)](https://www.rust-lang.org/)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)
[![Architecture](https://img.shields.io/badge/architecture-local--first%20%7C%20mesh%20%7C%20dtn-green.svg)](#system-architecture)
[![Wasmtime](https://img.shields.io/badge/sandboxing-Wasmtime%2027.0-purple.svg)](#sandboxed-wasm-applications-phase-09)
[![Storage](https://img.shields.io/badge/storage-SQLite%20WAL%20%2B%20CAS-blueviolet.svg)](#local-first-data--crdt-synchronization-phase-06)

---

## 1. Executive Overview

**FIELDNET** is a resilient, distributed, offline-first operating environment and application runtime that enables nearby commodity computing devices (smartphones, laptops, single-board computers, IoT gateways) to form an autonomous local computing ecosystem.

FIELDNET is designed from first principles around a core architectural invariant:
> **The Internet is treated as an available capability, never a prerequisite for computing.**

In traditional computing, modern software architectures assume continuous access to cloud servers, DNS root zones, centralized certificate authorities, and high-bandwidth backbones. When infrastructure collapses—due to natural disasters, power grid failure, warfare, state censorship, or remote expeditionary fieldwork—conventional software completely ceases to function.

FIELDNET solves this failure mode by operating entirely local-first. Devices running the FIELDNET runtime discover each other over local radios (Wi-Fi, Ethernet, and planned BLE / Wi-Fi Direct), mutually authenticate using sovereign Ed25519 cryptographic keypairs, route messages across multi-hop peer topologies, stage delayed communications inside persistent Delay-Tolerant Networking (DTN) queues, replicate state using Conflict-Free Replicated Data Types (CRDTs), share content via BitTorrent-style swarming, expose dynamic services over zero-configuration mDNS (`fieldnet.local`), and safely execute sandboxed WebAssembly applications with deterministic resource metering.

### What FIELDNET Is:
- **A distributed local-first runtime**: Runs as a sovereign process on macOS, Linux, Windows, and mobile operating systems.
- **A peer-to-peer computing fabric**: Nodes collaborate to exchange messages, sync documents, stream files, route traffic, and execute distributed services without a central server.
- **A delay-tolerant store-and-forward mesh**: Messages and data packets survive network partitions and physical isolation, forwarding opportunistically when nodes encounter each other.
- **A sandboxed execution platform**: Distributes and runs signed WebAssembly modules (`.fnapp`) with capability-based security, memory boundaries, and instruction fuel limits.

### What FIELDNET Is NOT:
-  **Not an OS replacement**: It does not replace the Linux kernel, macOS Darwin, Android, or Windows; it executes on top of the host OS as an application runtime.
-  **Not merely a chat app**: While it includes robust messaging, chat is simply one trivial consumer service on top of the runtime.
-  **Not merely a mesh router**: It does not stop at packet forwarding; it provides structured databases, content-addressed storage, service discovery, and compute execution.
-  **Not a cloud surrogate**: It has no concept of master nodes, primary coordinates, or central authorities.

---

## 2. Core Architectural Principles & Invariants

| Principle | Architectural Rule |
| :--- | :--- |
| **Local-First by Default** | All local operations (reads, writes, transactions, compute) must succeed immediately without waiting for or requiring global internet connectivity. |
| **No Mandatory Central Server** | Every node is an autonomous peer. The network forms, operates, partitions, and heals with zero reliance on cloud relays, central databases, or root authorities. |
| **Sovereign Cryptographic Identity** | Identity is mathematically tied to an **Ed25519 public key (`NodeId`)**, not an IP address, domain name, hardware MAC, or phone number. A node's identity remains invariant across radio and network switches. |
| **Service-Centric Capability Addressing** | Applications request capabilities by symbolic service identifier (e.g., `ai.service`, `maps.offline`, `chat.room`), not hard-coded IP:Port addresses. |
| **Failure is Normal** | Node disappearances, link drops, asymmetric signal propagation, partitions, and long delays are treated as standard operational states, not exceptional bugs. |
| **Transport Independence** | Applications interact strictly with high-level environment APIs and structured messages; raw TCP/UDP sockets, physical radio interfaces, and packets are hidden behind an Application Abstraction Shield. |
| **Zero-Trust Connection Pipeline** | Physical connections are completely untrusted until authenticated via an mutual Ed25519 challenge-response handshake, followed by ephemeral X25519 ECDH key exchange with ChaCha20-Poly1305 AEAD encryption. |
| **Deterministic Conflict Convergence** | Concurrent offline edits across nodes converge deterministically without data loss using hybrid logical clocks (HLC) and state/operation CRDTs. |
| **Sandboxed Least-Privilege Execution** | Third-party applications run in isolated Wasmtime WebAssembly runtimes with explicit capability token grants ($\text{Declared} \cap \text{Authorized} = \text{Effective}$). |

---

## 3. High-Level System Architecture

The following diagram illustrates the layered architecture implemented in the FIELDNET Rust workspace:

```
┌────────────────────────────────────────────────────────────────────────┐
│                        Application & Client Layer                      │
│   - CLI Tool (fieldnet-node)          - Web Browsers (fieldnet.local)  │
│   - Sandboxed WASM Apps (.fnapp)      - Native Desktop / Mobile Shells │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│               Service Discovery & Local Web (fieldnet-service)         │
│   - mDNS / DNS-SD Responder (simple-dns)                              │
│   - Local Web Gateway (Axum HTTP/WebSocket Reverse Proxy)              │
│   - Browser Pairing & Session Security (4-Digit PIN + Cookie Auth)     │
│   - Dynamic Service Registry & Locality-Aware Resolver (Hops/Cost)     │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                 Sandboxed Compute & Apps (fieldnet-wasm)               │
│   - Wasmtime 27.0 JIT Sandbox with Fuel (CPU) & Memory Resource Limits │
│   - Sovereign Application Package Verification (.fnapp Ed25519 Sig)    │
│   - SemVer Dependency Graph & Atomic Rollback Registry                │
│   - Secure Host ABI (Storage, Network, Services, Content, Events)      │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                 Runtime Orchestration (fieldnet-runtime)               │
│   - FieldnetRuntime Facade & Single-Process Tokio Concurrency Engine   │
│   - In-Memory Event Bus (broadcast channels)                          │
│   - Distance-Vector Routing Engine & Multi-Hop Relay (A -> B -> C)     │
│   - Persistent Delay-Tolerant Networking (DTN) SQLite Queue            │
└──────────────────┬─────────────────┬─────────────────┬─────────────────┘
                   │                 │                 │
     ┌─────────────┘                 │                 └─────────────┐
     ▼                               ▼                               ▼
┌──────────────────────┐   ┌───────────────────┐   ┌─────────────────────┐
│ Content Subsystem    │   │  Local-First Data │   │ Wire Protocol       │
│  (fieldnet-content)  │   │   (fieldnet-data) │   │ (fieldnet-protocol) │
│ - ContentStore (CAS) │   │ - SQLite WAL Mode │   │ - 14-Field Envelope │
│ - 256KB ChunkStore   │   │ - Operations Log  │   │ - CBOR & JSON Codec │
│ - Merkle Manifests   │   │ - Vector Clocks   │   │ - Anti-Replay Cache │
│ - Swarm Transfer     │   │ - CRDT Engine     │   │ - Monotonic Sequence│
│ - Storage Quotas/GC  │   │ - 3-Phase Sync    │   │ - 4-Tier ACK Model  │
└──────────┬───────────┘   └─────────┬─────────┘   └──────────┬──────────┘
           │                         │                        │
           └─────────────────────────┼────────────────────────┘
                                     │ Encrypted Frames
                                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│                   Networking Layer (fieldnet-network)                  │
│   - UDP Multicast / Broadcast Peer Discovery (239.255.42.99:4242)      │
│   - TCP Stream Framing & Connection Manager (Max 64 links, keepalive)  │
│   - Mutual Ed25519 Handshake & Transcript Authentication               │
│   - Ephemeral X25519 ECDH + ChaCha20-Poly1305 AEAD Encrypted Tunnels   │
└────────────────────────────────────┬───────────────────────────────────┘
                                     │
                                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│                 Identity & Core Primitives Layer                       │
│   - fieldnet-identity: Ed25519 Keypair Gen, identity.json Persistence │
│   - fieldnet-core: NodeId, MessageId, Timestamp, Priority, CoreError   │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Technology Stack & Dependencies

FIELDNET is implemented in 100% safe, modern Rust (2021 edition) across a modular Cargo workspace:

### Core Systems & Libraries
- **Language & Toolchain**: Rust 2021 Edition (`cargo`, `rustc` 1.80+).
- **Asynchronous Concurrency**: `tokio 1.40` (multi-threaded runtime, async tasks, `mpsc`, `broadcast`, `Notify`, `RwLock`), `async-trait 0.1`.
- **Cryptography & Hashing**:
  - `ed25519-dalek 2.1`: Sovereign node identity, digital signatures, mutual handshake authentication, application package verification.
  - `x25519-dalek 2.0`: Ephemeral Diffie-Hellman (ECDH) forward-secret session key exchange.
  - `chacha20poly1305 0.10`: Authenticated Encryption with Associated Data (AEAD) wire protocol encryption.
  - `blake3 1.5`: High-performance cryptographic hashing for unique `MessageId` generation and package integrity.
  - `sha2 0.10`: SHA-256 for Content-Addressed Storage (`ContentId`, `ChunkId`, Merkle roots).
  - `rand 0.8`: Cryptographically secure pseudorandom number generator (CSPRNG).
- **Storage & Databases**:
  - `rusqlite 0.32` (bundled): Embedded SQLite with Write-Ahead Logging (WAL) mode for transactional local documents, operational logs, DTN staging, and registry catalogs.
  - Sharded CAS File Store: High-throughput chunk directory layout for large binary objects.
- **Networking & Web**:
  - `axum 0.8` & `tower-http 0.6`: Embedded HTTP/WebSocket Local Web Gateway with CORS, body limits, and security headers.
  - `simple-dns 0.12`: Zero-configuration multicast DNS (mDNS) responder for `.local` resolution.
  - `socket2 0.5` & `if-addrs 0.13`: Raw network socket control, multicast interface binding, dynamic IP discovery.
- **Sandboxed Execution**:
  - `wasmtime 27.0` & `cranelift`: High-performance, memory-safe WebAssembly runtime with instruction fuel metering and memory bounds.
- **Serialization & CLI**:
  - `serde 1.0`, `serde_json 1.0`: Manifest, package, and wire serialization.
  - `clap 4.5`: Type-safe command-line argument and subcommand parsing.
  - `tracing 0.1`, `tracing-subscriber 0.3`: Structured asynchronous diagnostic logging.

---

## 5. Workspace Crate Architecture

The repository is structured as a single Cargo workspace containing 9 specialized crates:

```
crates/
├── fieldnet-core/       # Shared universal primitives: NodeId, MessageId, Timestamp, Priority, errors
├── fieldnet-identity/   # Sovereign Ed25519 key generation, identity.json management, signature verification
├── fieldnet-protocol/   # Wire message model, 14-field envelope, CBOR/JSON codec, deduplicator
├── fieldnet-network/    # UDP discovery, TCP connection manager, Ed25519 handshake, ChaCha20 AEAD sessions
├── fieldnet-data/       # SQLite document store, HLC causality, CRDTs (LWW, OR-Set, PN-Counter), 3-phase sync
├── fieldnet-content/    # Content-Addressed Storage (CAS), 256KB chunker, Merkle manifests, swarm transfer
├── fieldnet-service/    # Service registry, Ed25519 manifests, locality resolver, mDNS, Local Web Gateway
├── fieldnet-wasm/       # Wasmtime sandbox, .fnapp package manager, fuel metering, host capability ABI
└── fieldnet-runtime/    # Top-level runtime orchestration facade, routing table, DTN queue, CLI binary (fieldnet-node)
```

### Detailed Crate Responsibilities

#### 1. `fieldnet-core`
- **`NodeId`**: 32-byte Ed25519 public key hex-encoded as a sovereign cryptographic identifier.
- **`MessageId`**: 32-byte BLAKE3 cryptographic hash computed deterministically from `BLAKE3(source || seq || created_at || payload)`.
- **`Timestamp`**: Microsecond-precision epoch timestamp with overflow protection and time-to-live (TTL) expiry checks.
- **`Priority`**: Message urgency levels (`Critical`, `High`, `Normal`, `Low`, `Background`) used for priority scheduling and DTN eviction.

#### 2. `fieldnet-identity`
- Generates sovereign Ed25519 keypairs locally using OS entropy.
- Persists cryptographic identities securely to disk (`identity.json`).
- Prevents identity tampering; ensures node identity is mathematically decoupled from IP addresses or hardware identifiers.

#### 3. `fieldnet-protocol`
- **14-Field Wire Envelope**: Contains `version`, `message_type`, `message_id`, `source`, `destination`, `created_at`, `expires_at`, `ttl`, `sequence`, `priority`, `auth_tag`, and payload.
- **10 Message Types**: `HELLO`, `AUTH`, `ACK`, `DATA`, `ROUTE`, `SERVICE`, `SYNC`, `FILE`, `EVENT`, and `ERROR`.
- **Anti-Replay & Deduplication**: Sliding LRU window and sequence tracking to drop replayed, looped, or duplicate packets across mesh routes.

#### 4. `fieldnet-network`
- **Peer Discovery**: Transmits and listens for UDP discovery beacons over `239.255.42.99:4242` every 500ms.
- **Connection Manager**: Supervises active TCP links, connection limits (max 64), keepalives, and automatic reconnections with exponential backoff.
- **Cryptographic Handshake**: Mutual Ed25519 challenge-response handshake that establishes ephemeral X25519 shared secrets.
- **AEAD Session Tunnels**: All TCP payload frames are encrypted using ChaCha20-Poly1305 with incrementing nonces to prevent eavesdropping and MITM attacks.

#### 5. `fieldnet-data`
- **SQLite WAL Storage**: High-concurrency local document storage resilient to sudden process termination or power loss.
- **Causality Engine**: Hybrid Logical Clocks (HLC) and vector clocks ensuring causal ordering across disconnected nodes.
- **Conflict-Free Replicated Data Types (CRDTs)**:
  - `LwwRegister`: Last-Write-Wins scalar registers with deterministic tie-breaking.
  - `PnCounter`: Positive-Negative counters supporting concurrent increment/decrement.
  - `OrSet`: Observed-Removed Sets with unique element tagging for add-wins convergence.
  - `LwwMap`: Conflict-free key-value maps with field-level timestamps.
- **3-Phase Sync Protocol**:
  1. *Handshake*: Negotiate sync session and authorize access.
  2. *Vector Exchange*: Compare local vs. remote vector clocks to compute minimal diffs.
  3. *Delta Streaming*: Stream only missing operations in bounded batches.
- **Compaction & Snapshots**: Consolidates operation logs into periodic snapshots while safely preserving tombstones within a configurable grace period.

#### 6. `fieldnet-content`
- **Content-Addressed Storage (CAS)**: Files are stored by SHA-256 hash in a sharded directory structure (`content/store/xx/yy/...`).
- **256KB Chunking**: Files are sliced into uniform 256 KiB chunks, each indexed by its own `ChunkId`.
- **Merkle Tree Manifests**: Generates hierarchical `ContentManifest` structures with Merkle tree root hashes for end-to-end cryptographic verification.
- **Swarm Transfer Engine**: BitTorrent-style cooperative swarming:
  - Tracks peer availability with `ChunkAvailabilityBitmap`.
  - Schedules parallel chunk downloads from multiple providers using rarest-first and pipelining algorithms.
  - Verifies SHA-256 hashes per chunk on receipt before disk commit.
- **Storage Quotas & GC**: Enforces hard storage bounds with LRU and priority-tiered garbage collection.

#### 7. `fieldnet-service`
- **Canonical Service Model**: Structured `ServiceManifest` containing `ServiceId`, `ServiceVersion`, provider `NodeId`, capabilities, and Ed25519 digital signature.
- **Locality-Aware Service Resolver**: Resolves service queries through a deterministic cost hierarchy:
  1. *Local Node* (0 hops, internal IPC)
  2. *1-Hop Direct Peer* (1 hop, direct TCP link)
  3. *Multi-Hop Mesh Route* (N hops, forwarded through intermediate nodes)
- **Local Web Gateway**: Embedded Axum HTTP/WebSocket server exposing a user-friendly REST/JSON interface for web browsers.
- **Pairing & Browser Security**: Protects browser sessions using a 4-digit pairing challenge code displayed on the node terminal, preventing unauthorized local web client access.
- **mDNS / DNS-SD**: Broadcasts zero-configuration `.local` records so any device on the local network can open `http://fieldnet.local:8080`.

#### 8. `fieldnet-wasm`
- **Sovereign Application Packages (`.fnapp`)**: Signed JSON/archive format containing application manifests, dependencies, declared permissions, and compiled `.wasm` bytecode.
- **Wasmtime Execution Supervisor**: Executes guest WebAssembly modules inside isolated JIT sandboxes with CPU instruction fuel limits and memory caps (64MB–256MB).
- **Host Capability ABI (`FIELDNET_HOST_API_VERSION = 1`)**:
  - `storage.put`, `storage.get`, `storage.has`, `storage.delete`
  - `service.resolve`, `service.describe`
  - `event.publish`
  - `network.send`
- **Atomic Lifecycle Manager**: Manages application installation, verification, start, stop, restart, quarantine/isolation, rollback to prior versions, and uninstallation.

#### 9. `fieldnet-runtime`
- **Runtime Facade (`FieldnetRuntime`)**: Instantiates, wires, and coordinates all underlying subsystems in a single Tokio asynchronous process.
- **Distance-Vector Routing Table**: Maintains dynamic shortest-hop routes via periodic `ROUTE` advertisement beacons.
- **Multi-Hop Relay**: Transparently routes messages across intermediate hops ($A \to B \to C$) while preserving end-to-end payload integrity and decrementing hop TTL.
- **Persistent DTN Queue**: Automatically stages undeliverable messages into SQLite (`dtn_queue.db`) when destinations are unreachable, opportunistically flushing them when target peers reappear.
- **CLI Executable (`fieldnet-node`)**: Provides the main command-line interface for node operators.

---

## 6. How FIELDNET Features Work (Under the Hood)

### 6.1 Peer Discovery & Encrypted Session Establishment
```
Node A (Initiator)                                Node B (Responder)
       │                                                 │
       ├──── UDP Multicast Beacon (port 4242) ──────────>│ [Discovers Node A]
       │<─── UDP Multicast Beacon (port 4242) ───────────┤ [Discovers Node B]
       │                                                 │
       ├──── TCP Connect (Node B TCP Port) ─────────────>│
       │                                                 │
       ├──── Handshake Init (Ed25519 PubKey + Nonce) ───>│
       │<─── Handshake Reply (Ed25519 PubKey + Nonce) ──┤ [Mutual Authentication]
       │                                                 │
       ├──── X25519 Ephemeral ECDH Key Exchange ────────>│
       │<─── X25519 Ephemeral ECDH Key Exchange ────────┤ [Shared Secret Derived]
       │                                                 │
       │═════════════════════════════════════════════════│
       │   Encrypted ChaCha20-Poly1305 AEAD Tunnel       │
       │═════════════════════════════════════════════════│
```
1. Each node broadcasts a UDP discovery beacon every 500ms containing its `NodeId` and TCP listening port.
2. When Node A detects Node B, it checks its peer table and initiates an outbound TCP stream.
3. Both nodes execute a mutual Ed25519 challenge-response handshake over the TCP connection.
4. An ephemeral X25519 Diffie-Hellman exchange computes a high-entropy session key with forward secrecy.
5. All subsequent wire messages are framed, authenticated, and encrypted with ChaCha20-Poly1305.

---

### 6.2 Delay-Tolerant Networking (DTN) & Multi-Hop Relay
```
[Node A] ──(Direct Link)──> [Node B (Relay)] ──(Link Severed)──X [Node C]
   │                               │
Send Data                          │
   │                               ▼
Dispatched ────────────────> [Node B DTN Queue]
                             (Saved to dtn_queue.db)
                                   │
                              [Time Passes]
                                   │
                             [Node C Reconnects]
                                   ▼
                             Flushed from Queue ──(Link Restored)──> [Node C]
```
1. When Node A wants to send a message to Node C, it queries its Distance-Vector Routing Table.
2. If Node C is reachable via Node B (2 hops), Node A sends the message to Node B with a decremented hop TTL.
3. If Node B cannot reach Node C because Node C moved out of range or powered down, Node B does not drop the message.
4. Node B immediately transitions the message state to `NO_ROUTE` and inserts it into its persistent SQLite `dtn_queue.db`.
5. When Node C re-enters radio proximity and discovery beacons resume, Node B's DTN flush monitor awakens, pulls staged messages from the database, and delivers them to Node C.

---

### 6.3 Local-First CRDT Document Synchronization
```
Node A (Offline)                               Node B (Offline)
       │                                              │
Modify Document "doc-1"                       Modify Document "doc-1"
(LwwMap: title = "Meeting A")                 (LwwMap: location = "Room B")
Commit to local SQLite WAL                    Commit to local SQLite WAL
Log Operation with HLC_A                      Log Operation with HLC_B
       │                                              │
       └────────────── Nodes Encounter Each Other ────┘
                               │
               Phase 1: Handshake (Sync Session Open)
                               │
               Phase 2: State Vector Exchange
               Node A sends VectorClock_A; Node B sends VectorClock_B
               Compute Missing Operations Delta
                               │
               Phase 3: Bounded Delta Streaming
               Node A streams Op_A ──────────> Node B applies Op_A
               Node B streams Op_B ──────────> Node A applies Op_B
                               │
                       [Both Nodes Converge]
              doc-1: { title: "Meeting A", location: "Room B" }
```
1. Every write is applied immediately to the local SQLite database; applications never wait for network roundtrips.
2. Each mutation generates an `Operation` record containing a Hybrid Logical Clock (HLC) timestamp and is appended to the operational log.
3. When nodes connect, they run a 3-Phase sync protocol. By exchanging compact state vectors, they determine exactly which operations the other node is missing.
4. Only missing delta batches are transmitted. The receiving node's CRDT engine applies them deterministically, resolving conflicting updates mathematically without central coordination.

---

### 6.4 Content-Addressed Swarm Transfers
```
   [Seed Node 1]                       [Seed Node 2]
  (Has Chunks 0, 1)                   (Has Chunks 2, 3)
         │                                   │
         └─────────────┐       ┌─────────────┘
                       ▼       ▼
               [Leech Node (Downloader)]
               - Manifest: Root SHA-256
               - Queries Provider Registry
               - Rarest-First Chunk Scheduler
               - Downloads Chunk 0 & 1 from Seed 1
               - Downloads Chunk 2 & 3 from Seed 2
               - SHA-256 Verifies Every Chunk
               - Reconstructs Original File
```
1. Files placed into the content system are hashed (SHA-256) and partitioned into uniform 256 KiB chunks.
2. A signed `ContentManifest` with a Merkle root is registered and advertised to the mesh.
3. When a node requests content, it queries the `ProviderRegistry` to find peers holding the manifest.
4. The swarm scheduler evaluates chunk availability bitmaps across providers, prioritizes the rarest chunks, and pipelines concurrent downloads over encrypted sessions.
5. Every received chunk is verified against its declared `ChunkId` hash before writing to disk, ensuring bit-for-bit integrity even across untrusted peers.

---

### 6.5 Wasmtime Sandboxed Application Engine
```
        Developer / Publisher
                  │
     Builds Wasm Bytecode + Manifest
     Signs with Ed25519 Private Key
                  │
          [.fnapp Package]
                  │
                  ▼
         [ApplicationInstaller]
         1. Check SHA-256 Package Integrity
         2. Verify Ed25519 Publisher Signature
         3. Validate Declared Permissions
         4. Resolve Dependency DAG
                  │
                  ▼
       [Execution Supervisor]
  ┌─────────────────────────────────┐
  │         Wasmtime 27.0           │
  │  - JIT Compilation (Cranelift)  │
  │  - Fuel Metering (Max CPU Ops)  │
  │  - Memory Quota (64MB - 256MB)  │
  │  - Zero Direct OS Access        │
  └────────────────┬────────────────┘
                   │ Host ABI Calls (host_call)
                   ▼
       [Capability Context Filter]
   Effective = Declared ∩ Authorized
         │                    │
    [Permitted]          [Denied]
         │                    │
Execute Host Function    Return EACCES
(Storage, Mesh, etc.)
```
1. Applications are compiled to standard WebAssembly (`wasm32-unknown-unknown` or WASI) and packaged into `.fnapp` manifests.
2. The package includes an Ed25519 signature from the publisher. The node verifies both the package hash and the signature prior to installation.
3. The application runs inside Wasmtime with strict CPU fuel metering (preventing infinite loops or DoS) and hard memory limits.
4. The guest module communicates with the outside world strictly through the FIELDNET Host ABI. Each call is filtered through a capability authorization matrix ensuring the app cannot access storage or network capabilities it was not explicitly granted.

---

## 7. Prerequisites & Installation

### System Requirements
- **Operating System**: macOS (Apple Silicon or Intel), Linux (x86_64, aarch64, ARMv7), or Windows 10/11.
- **Rust Toolchain**: Rust 1.80+ with `cargo`.
- **C Compiler**: `clang` or `gcc` (required by bundled SQLite and Wasmtime).

### 1. Install Rust
If you do not have Rust installed, install it using `rustup`:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

### 2. Clone the Repository
```bash
git clone https://github.com/Satishhhh989/FIELDNET.git
cd FIELDNET
```

### 3. Build the Workspace
To build all crates in the workspace in debug mode:
```bash
cargo build --workspace
```

To build an optimized production release binary:
```bash
cargo build --release --bin fieldnet-node
```
The compiled binary will be located at `target/release/fieldnet-node`.

### 4. Run the Test Suites
FIELDNET includes over 250 unit and integration tests verifying cryptographic handshakes, multi-hop routing, CRDT convergence, swarm transfers, and WASM sandboxing:
```bash
# Run all workspace tests
cargo test --workspace

# Run multi-hop routing integration tests
cargo test -p fieldnet-runtime --test three_node_multihop_test

# Run full CRDT synchronization verification
cargo test -p fieldnet-data --test phase06_full_subsystem_verification

# Run distributed swarm transfer tests
cargo test -p fieldnet-content --test phase07_full_verification_tests

# Run WASM sandbox execution tests
cargo test -p fieldnet-wasm --test execution_supervisor_tests
```

---

## 8. CLI Usage Guide (`fieldnet-node`)

The primary node executor is the `fieldnet-node` CLI binary.

```bash
cargo run --bin fieldnet-node -- [OPTIONS] [COMMAND]
```

### 8.1 Global Options & Flags

| Flag | Short | Default | Description |
| :--- | :--- | :--- | :--- |
| `--data-dir <PATH>` | `-d` | `./data` | Directory where node identity (`identity.json`), SQLite databases, and content chunks are stored. |
| `--tcp-port <PORT>` | `-t` | `0` (dynamic) | Local TCP port to bind for incoming encrypted peer links (`0` selects an available OS ephemeral port). |
| `--discovery-port <PORT>`| | `4242` | UDP port for peer multicast/broadcast beacons. |
| `--auto-send` | | `false` | Automatically sends a test data message to the first discovered peer upon connection. |
| `--target-peer <NODE_ID>`| | `None` | Specifies a target peer's 64-character hex `NodeId` to send a message to. |
| `--message <TEXT>` | | `"Hello from FIELDNET Node"` | The payload text to transmit when `--auto-send` or `--target-peer` is used. |
| `--run-seconds <SECS>` | | `None` (indefinite) | Automatically terminates the node process after `N` seconds (ideal for scripts and automated integration tests). |

---

### 8.2 Subcommands Overview

The CLI provides subcommands to inspect services, check zero-conf mDNS status, and manage the sandboxed application lifecycle:

```bash
fieldnet-node [OPTIONS] <COMMAND>

Commands:
  services  Service discovery, inspection, and resolution
  mdns      Inspect mDNS / DNS-SD local gateway advertisement status
  app       Sovereign WebAssembly application management and supervision
  help      Print this message or the help of the given subcommand(s)
```

---

### 8.3 Service Subcommands (`services`)

Manage and query the decentralized service registry:

#### 1. List All Registered Services
Lists all local and mesh-advertised services discovered across the network:
```bash
cargo run --bin fieldnet-node -- --data-dir ./data services list
```
*Example Output:*
```text
REGISTERED SERVICES (2)
SERVICE ID                INSTANCE ID                          PROVIDER        STATE       
------------------------------------------------------------------------------------------
chat.mesh                 inst-4b8a21f8-9a3b                  02e8a1f4c9b2    Active      
maps.offline              inst-77c10b42-12e0                  03f19c8d10aa    Active      
```

#### 2. Inspect a Specific Service
Inspect detailed metadata, version, provider `NodeId`, capabilities, and TTL of a service:
```bash
cargo run --bin fieldnet-node -- --data-dir ./data services inspect chat.mesh
```

#### 3. Resolve a Service Query
Executes the locality-aware resolver to find the optimal provider for a service:
```bash
cargo run --bin fieldnet-node -- --data-dir ./data services resolve chat.mesh
```
*Example Output:*
```text
RESOLVED SERVICE 'chat.mesh':
--------------------------------------------------
Instance ID:   inst-4b8a21f8-9a3b
Provider:      02e8a1f4c9b2e5...
Endpoint:      Tcp("192.168.1.150:52194")
Route Hops:    Some(1)
Local:         false
Reason:        OptimalCostRoute
```

---

### 8.4 mDNS Subcommands (`mdns`)

Inspect the status of the local zero-configuration gateway:
```bash
cargo run --bin fieldnet-node -- --data-dir ./data mdns status
```
*Example Output:*
```text
FIELDNET mDNS / DNS-SD SUBSYSTEM STATUS
==================================================
State:                Running
Hostname:             fieldnet.local
Advertised Port:      8080
Service Type:         _fieldnet._tcp.local
Service Name:         FIELDNET Node 02e8a1f4
Advertisement Status: Active
Queries Received:     14
Responses Sent:       14
Errors Encountered:   0
Interfaces (2):
  - 127.0.0.1 (lo0)
  - 192.168.1.105 (en0)
==================================================
```

---

### 8.5 Application Subcommands (`app`)

Manage sovereign WebAssembly applications, verify signatures, resolve dependencies, and monitor execution:

#### 1. List Installed Applications
```bash
cargo run --bin fieldnet-node -- --data-dir ./data app list
```

#### 2. Inspect Application Details
Inspect declared vs. authorized permissions, storage quotas, dependencies, and rollback history:
```bash
cargo run --bin fieldnet-node -- --data-dir ./data app info app.org.offline-maps
```

#### 3. Verify Application Package File
Verify cryptographic SHA-256 integrity and Ed25519 publisher signature before installation:
```bash
cargo run --bin fieldnet-node -- --data-dir ./data app verify ./packages/offline-maps.fnapp
```

#### 4. Run an Application
Executes a WebAssembly application in the Wasmtime supervisor:
```bash
cargo run --bin fieldnet-node -- --data-dir ./data app run app.org.offline-maps
```

#### 5. List Active Executions
```bash
cargo run --bin fieldnet-node -- --data-dir ./data app executions
```

#### 6. Stop an Execution
```bash
cargo run --bin fieldnet-node -- --data-dir ./data app stop <EXECUTION_ID>
```

#### 7. Quarantine / Isolate an Application
Administratively quarantine an application suspected of misbehavior:
```bash
cargo run --bin fieldnet-node -- --data-dir ./data app isolate app.org.offline-maps --reason "Excessive memory allocation"
```

#### 8. Lift Quarantine (Unisolate)
```bash
cargo run --bin fieldnet-node -- --data-dir ./data app unisolate app.org.offline-maps
```

#### 9. Roll Back Application Version
Atomically restore an application to its previous stable version:
```bash
cargo run --bin fieldnet-node -- --data-dir ./data app rollback app.org.offline-maps --target-version 1.0.4
```

#### 10. Uninstall Application
Cleanly removes an application, cleans its module cache, and releases storage quotas:
```bash
cargo run --bin fieldnet-node -- --data-dir ./data app uninstall app.org.offline-maps
```

---

## 9. Hands-On Step-by-Step Tutorials

### Scenario 1: Running Two Nodes on One Machine

You can simulate a two-node peer-to-peer network on a single computer by using separate data directories:

#### Terminal 1 — Start Node A:
```bash
cargo run --bin fieldnet-node -- \
  --data-dir /tmp/fieldnet-node-a \
  --tcp-port 5001 \
  --discovery-port 4242
```
*Node A will print its sovereign NodeId and listen for UDP discovery beacons.*

#### Terminal 2 — Start Node B (Auto-sends message to Node A):
```bash
cargo run --bin fieldnet-node -- \
  --data-dir /tmp/fieldnet-node-b \
  --tcp-port 5002 \
  --discovery-port 4242 \
  --auto-send \
  --message "Emergency Field Report: Team Alpha Check-in"
```

#### What happens:
1. Node B discovers Node A over UDP multicast.
2. Node B establishes a TCP link to Node A on port 5001.
3. They exchange Ed25519 credentials and derive a ChaCha20-Poly1305 session key.
4. Node B transmits the encrypted message.
5. Node A receives, decrypts, authenticates, and dispatches an acknowledgment (`ACK`).
6. Node B receives the delivery verification ACK:
```text
--------------------------------------------------
SUCCESS: Delivery Verified! ACK received from peer 02e8a1f4c9b2...
Status: Delivered
--------------------------------------------------
```

---

### Scenario 2: Accessing the Local Web Gateway

FIELDNET includes an embedded Web Gateway with mDNS support:

1. Start a node:
   ```bash
   cargo run --bin fieldnet-node -- --data-dir ./data --tcp-port 5001
   ```
2. Open your web browser and navigate to:
   ```text
   http://localhost:8080/api/v1/status
   ```
   *(Or `http://fieldnet.local:8080/api/v1/status` if mDNS is active).*
3. You will receive a structured JSON status report:
   ```json
   {
     "node_id": "02e8a1f4c9b2...",
     "status": "online",
     "active_connections": 1,
     "uptime_seconds": 128,
     "version": "0.1.0"
   }
   ```
4. Query available mesh services:
   ```text
   http://localhost:8080/api/v1/services
   ```
5. Retrieve content from Content-Addressed Storage:
   ```text
   http://localhost:8080/api/v1/content/<CONTENT_ID>
   ```

---

## 10. Security Architecture & Threat Model

FIELDNET is designed under an adversarial zero-trust network model:

1. **Private Key Sovereign Isolation**:
   - The Ed25519 private key is generated locally and stored exclusively in `identity.json`.
   - Private keys are never serialized into wire messages, logs, or gateway responses.
2. **Forward-Secret Encrypted Sessions**:
   - Connections use ephemeral X25519 keys generated per connection. Even if a node's long-term identity key were compromised in the future, past session traffic cannot be decrypted.
3. **Anti-Replay Protection**:
   - Every wire frame includes a monotonically increasing sequence number and a BLAKE3 `MessageId`. Nodes maintain an LRU replay cache; duplicate or old frames are dropped immediately.
4. **Denial-of-Service (DoS) Defenses**:
   - Connection limits: Max 64 total connections, max 2 connections per peer.
   - Handshake timeouts: Unauthenticated sockets drop after 5.0 seconds.
   - Gateway body limits: Maximum 10MB HTTP payload, 64KB headers, 4KB URIs.
5. **DNS Rebinding & CORS Protection**:
   - Gateway enforces strict `Host` header whitelisting (`localhost`, `127.0.0.1`, `fieldnet.local`).
   - Remote browser access requires explicit pairing confirmation with a 4-digit challenge code.
6. **WASM Sandboxing Invariants**:
   - WebAssembly modules execute in ring-3 equivalent userspace inside Wasmtime.
   - No direct filesystem, socket, or OS syscall access.
   - Fuel instruction metering prevents CPU lockups.
   - Strict memory limits (64MB default, 256MB maximum).

---

## 11. Project Status & Roadmap

### Completed Phases (MVP Baseline)

| Phase | Subsystem | Implemented Components | Tests | Status |
| :---: | :--- | :--- | :---: | :---: |
| **01** | System Model & Invariants | 14 formal entities, 5 connectivity states, architectural grammar | Formal Specs | **COMPLETE** |
| **02** | Runtime Core Architecture | Single-process Tokio orchestration, 7-phase lifecycle, event bus | 40 Tests | **COMPLETE** |
| **03** | Sovereign Identity & Trust | Ed25519 identity, X25519 ECDH, ChaCha20-Poly1305 AEAD sessions | Suite Verified | **COMPLETE** |
| **04** | Peer Discovery & Networking | UDP multicast (239.255.42.99:4242), TCP connection manager | Suite Verified | **COMPLETE** |
| **05** | Protocol, Routing & DTN | 14-field envelope, distance-vector routing, SQLite DTN queue | Suite Verified | **COMPLETE** |
| **06** | Local-First Data & CRDTs | SQLite WAL, HLC, LWW-Map, OR-Set, PN-Counter, 3-phase sync | 120 Tests | **COMPLETE** |
| **07** | Content-Addressed Storage | 256KB CAS, Merkle manifests, swarm transfer, quota, GC | 114 Tests | **COMPLETE** |
| **08** | Service Discovery & Gateway | Locality-aware resolver, mDNS (`fieldnet.local`), Axum gateway | Suite Verified | **COMPLETE** |
| **09** | Sandboxed WASM Engine | Wasmtime 27, fuel metering, .fnapp packages, capability ABI | Suite Verified | **COMPLETE** |

---

### Future Implementations & Platform Roadmap

#### Phase 10: Distributed Compute & Local AI Inference
- **Volunteer Task Scheduler**: Distributes batch computation jobs across participating nodes based on available battery and CPU capacity.
- **Local Embedded AI (ONNX / WASM)**: Integrates quantized small language models (SLMs, e.g. Gemma 2B, LLaMA-3 1B) running locally via ONNX Runtime or WebAssembly SIMD for offline summarization, translation, and emergency triage.
- **Vector Embeddings Store**: Local vector database for semantic search across offline documents and message histories.

#### Phase 11: Internet Gateway & Opportunistic Hybrid Mode
- **Opportunistic Egress Detection**: When any node in the local mesh gains global internet connectivity (e.g., satellite uplink, cellular burst, Wi-Fi hotspot), it can securely advertise itself as an egress gateway.
- **Privacy-Preserving Egress Proxy**: Proxies encrypted requests for external APIs with rate limiting and bandwidth quotas.
- **NAT Traversal & Hole Punching**: STUN/TURN integration for bridging isolated FIELDNET islands across the global internet.

#### Phase 12: Network Resilience, Simulation & Physical Radios
- **Mobile Radio Platform Drivers**: Native Bluetooth Low Energy (BLE) and Wi-Fi Direct (Wi-Fi Aware) transport adapters for Android (`WifiP2pManager`) and iOS (`MultipeerConnectivity`).
- **LoRa Transport Layer**: Low-bandwidth, ultra-long-range packet framing for off-grid communication over kilometers.
- **Automated Chaos Testing**: In-memory network topology simulator to inject arbitrary packet loss, latency spikes, and partition splits.

#### Client Shells: Desktop & Mobile UI
- **Tauri / Flutter Client Shell**: Cross-platform native interface providing:
  - Decentralized end-to-end encrypted mesh messaging.
  - Collaborative offline document editor powered by CRDTs.
  - Offline OpenStreetMap tile viewer with shared GPS waypoints.
  - Content explorer and file drop manager.
  - Local Application Store for one-click `.fnapp` installation.

---

## 12. Workspace Directory Structure

```text
FIELDNET/
├── Cargo.lock
├── Cargo.toml
├── README.md                               # Authoritative Project Documentation
├── FIELDNET_MASTER_SPEC.md                 # Product and Technical Master Specification
├── IMPLEMENTATION_STATUS.md                # Phase-by-Phase Verification Ledger
├── FIELDNET-CURRENT-STATE-AUDIT.md         # Comprehensive Architectural Audit Report
├── crates/
│   ├── fieldnet-core/                      # NodeId, MessageId, Priority, Timestamp, errors
│   ├── fieldnet-identity/                  # Sovereign Ed25519 identity, key persistence
│   ├── fieldnet-protocol/                  # Wire envelope, codec, deduplication, routing frames
│   ├── fieldnet-network/                   # UDP discovery, TCP manager, handshake, AEAD crypto
│   ├── fieldnet-data/                      # SQLite document store, CRDTs, HLC, 3-phase sync
│   ├── fieldnet-content/                   # Content-Addressed Storage, 256KB chunker, swarm engine
│   ├── fieldnet-service/                   # Service registry, resolver, mDNS, Local Web Gateway
│   ├── fieldnet-wasm/                      # Wasmtime sandbox, package manager, fuel, host ABI
│   └── fieldnet-runtime/                   # Runtime facade, DTN queue, routing, CLI node binary
└── docs/
    ├── architecture/                       # Detailed specifications for Phases 01 through 09
    └── implementation/                     # Implementation ledgers and verification reports
```

---

## 13. Contributing

We welcome contributions to the FIELDNET project! To contribute:

1. **Fork and clone** the repository.
2. **Create a feature branch**: `git checkout -b feature/my-feature`.
3. **Ensure strict adherence to architectural invariants**:
   - Zero cloud or internet dependencies for core features.
   - All networking must remain transport-agnostic.
   - Write comprehensive unit and integration tests.
4. **Run workspace checks**:
   ```bash
   cargo fmt --check
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   ```
5. **Open a Pull Request** with a detailed explanation of changes.

---

## 14. License

FIELDNET is dual-licensed under either:
- **MIT License** ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
- **Apache License, Version 2.0** ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

at your option.
