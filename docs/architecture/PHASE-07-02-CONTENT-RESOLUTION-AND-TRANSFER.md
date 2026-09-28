# FIELDNET Phase 07 — Architecture Specification
**Document ID:** `PHASE-07-02-CONTENT-RESOLUTION-AND-TRANSFER`  
**Title:** Distributed Content Resolution, Transfer & Provider Architecture  
**Status:** SPECIFICATION APPROVED  
**Authoritative References:** `FIELDNET_MASTER_SPEC.md`, `docs/architecture/01-system-model.md`, `docs/architecture/02-runtime-architecture.md`, `docs/architecture/03-security.md`, `docs/architecture/04-networking.md`, `docs/architecture/05-protocol-routing-dtn.md`, `docs/architecture/PHASE-06-01-LOCAL-FIRST-DATA.md`, `docs/architecture/PHASE-07-01-DISTRIBUTED-CONTENT-SYSTEM.md`

---

## 1. Executive Summary & Resolution Architecture

FIELDNET Phase 07 Prompt 02 specifies the distributed resolution, provider management, chunk scheduling, parallel multi-source swarming, and resumable transfer engine for the FIELDNET Content System.

In a mobile ad-hoc tactical mesh, content cannot rely on static host addresses, persistent socket connections, or central registries. Nodes dynamically appear, cache content, serve chunks over lossy radio links, drop offline mid-transfer, and reappear via different multi-hop routing paths.

The Content Resolution and Transfer architecture establishes a **decentralized, peer-to-peer data pipeline** that treats files as distributed collections of independently verifiable cryptographic chunks:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Application / User Layer                              │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │ 1. resolve(ContentId)
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    Content Resolver (ContentResolver)                           │
│  - Queries local ContentStore (Cache Hit Check)                                 │
│  - Dispatches Content Queries to Mesh / DTN                                     │
│  - Coordinates Provider Discovery & Metadata Fetch                              │
└───────────────────┬─────────────────────────────────────────▲───────────────────┘
                    │ 2. Lookup                               │ 3. Candidates
                    ▼                                         │
┌────────────────────────────────────────┐  ┌─────────────────┴───────────────────┐
│    Provider Registry (ProviderRegistry)│  │   Capability Authorizer             │
│  - Tracks discovered provider nodes    │  │   (ContentAuthorizer)               │
│  - Maintains chunk availability masks  │  │   - Validates Read Capability Token │
│  - Records latency, RTT, trust tiers   │  │   - Filters Unauthorized Candidates │
└───────────────────┬────────────────────┘  └─────────────────▲───────────────────┘
                    │ 4. Verified Candidates                  │
                    ▼                                         │
┌─────────────────────────────────────────────────────────────┴───────────────────┐
│                  Provider Selector (ProviderSelector)                           │
│  - Evaluates multi-factor composite scores (RTT, Trust, Queue, Bandwidth)       │
│  - Assigns optimal chunk-to-provider mappings                                   │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │ 5. Selected Plan
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    Transfer Manager (TransferManager)                           │
│  - Manages persistent TransferSession state                                     │
│  - Drives 11-State Transfer State Machine (Negotiating -> Transferring)         │
│  - Chunk Scheduler (Rarest-First, Sequential Streaming, Endgame Speculation)    │
│  - Rate Limiting, Backpressure & Sliding Window Flow Control                    │
└───────────────────┬─────────────────────────────────────────▲───────────────────┘
                    │ 6. Wire Messages                        │ 7. Inbound Chunks
                    ▼                                         │
┌─────────────────────────────────────────────────────────────┴───────────────────┐
│           FIELDNET Protocol, Routing & DTN Subsystems (Phases 04/05)            │
│  - Envelope Framing: MessageType::File (7)                                      │
│  - Control Plane: Low-latency small DTN bundles (Manifests, Queries, ACKs)      │
│  - Bulk Data Plane: High-bandwidth direct stream sockets (Chunks)               │
└───────────────────┬─────────────────────────────────────────▲───────────────────┘
                    │                                         │
                    ▼                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│            Integrity Verification & Assembly Engine (ContentStore)              │
│  - Tier 1: Leaf Chunk BLAKE3 Verification                                       │
│  - Tier 2: Merkle Branch & Manifest Validation                                  │
│  - Tier 3: Root ContentId Cryptographic Seal                                    │
│  - Materializes to Disk CAS (<data_dir>/content/chunks/xx/yy/...)               │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Component Ownership & Layer Demarcation

To prevent architectural coupling, responsibility creep, and security bypasses, strict operational boundaries are enforced across all subsystem components:

| Component | Strict Ownership & Responsibilities | FORBIDDEN / Non-Goals |
| :--- | :--- | :--- |
| **`ContentResolver`** | Translates application requests into resolution workflows; checks local CAS cache; issues mesh `CONTENT_QUERY` messages; orchestrates transfer creation. | **MUST NOT** manage chunk download sockets, slice buffers, or perform direct TCP/UDP I/O. |
| **`ProviderRegistry`**| Ingests, updates, indexes, and expires provider claims; tracks chunk bitfields, observed RTT, and last-seen timestamps. | **MUST NOT** act as an authorization authority; does not grant permissions or verify user access tokens. |
| **`ContentAuthorizer`**| Cryptographically validates Phase 03 Capability Tokens $\langle P, C, S, R, A, I, E, V \rangle$; enforces capability expiration and scope. | **MUST NOT** perform network transport calls or participate in chunk scheduling. |
| **`ProviderSelector`**| Pure, deterministic algorithmic scoring of candidate providers based on telemetry, distance, and availability. | **MUST NOT** maintain long-lived network connections or store persistent state. |
| **`TransferManager`** | Drives `TransferSession` state machines; schedules chunk requests; monitors timeouts; handles retransmissions and backpressure. | **MUST NOT** bypass authorization; must not commit unverified chunks to the public `ContentStore`. |
| **`ChunkScheduler`** | Computes optimal chunk request orderings (Rarest-First vs. Sequential) and provider assignments. | **MUST NOT** issue network packets directly. |
| **`ContentStore`** | Manages local on-disk chunk storage, deduplication reference counts, Merkle trees, and atomic verification commits. | **MUST NOT** know about remote peer addresses, routes, or transfer protocols. |

---

## 3. Provider Registry Architecture

The `ProviderRegistry` maintains the local node's working knowledge of remote peers possessing content objects or specific chunk subsets.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        PROVIDER REGISTRY DATA MODEL                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│ ContentId: fn-cid-1e20a4b8... (Total Chunks: 100)                               │
├──────────────┬──────────────┬──────────────┬──────────────┬──────────────┬──────┤
│ ProviderNode │ Chunks Owned │ Observed RTT │ Trust Tier   │ Reliability  │ Status│
├──────────────┼──────────────┼──────────────┼──────────────┼──────────────┼──────┤
│ Node_Alpha   │ [0..49]      │ 12 ms        │ AUTHORIZED   │ 99.4% (Fast) │ ACTIVE│
│ Node_Bravo   │ [50..99]     │ 48 ms        │ KNOWN        │ 94.1% (Med)  │ ACTIVE│
│ Node_Charlie │ [0..99] Full │ 180 ms (2hop)│ VERIFIED     │ 88.0% (Slow) │ ACTIVE│
│ Node_Delta   │ [20..35]     │ 15 ms        │ AUTHORIZED   │ 12.0% (Drop) │ STALE │
└──────────────┴──────────────┴──────────────┴──────────────┴──────────────┴──────┘
```

### 3.1 Provider Record Schema (`ProviderRecord`)
```rust
/// Comprehensive record of a remote content provider.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderRecord {
    /// Canonical content identifier.
    pub content_id: ContentId,
    /// Sovereign identity of the provider node.
    pub provider_node_id: NodeId,
    /// Bitfield representing possessed chunks (None if unknown or advertised as complete).
    pub available_chunks: Option<ChunkBitfield>,
    /// Total chunks advertised by provider (if manifest known).
    pub total_chunks: Option<u32>,
    /// Physical timestamp when advertisement was received.
    pub advertised_at: Timestamp,
    /// Hard expiration timestamp after which the record is evicted.
    pub expires_at: Timestamp,
    /// Last verified physical contact timestamp.
    pub last_seen_at: Timestamp,
    /// Cryptographic trust tier from Phase 03 Security Engine.
    pub trust_tier: TrustTier,
    /// Observed network latency over recent exchanges (moving average).
    pub observed_rtt_ms: u32,
    /// Historical transfer success ratio (0..1000 representing 0.0% to 100.0%).
    pub reliability_permille: u16,
    /// Current availability state.
    pub availability_state: ProviderState,
    /// Dynamic rate capacity advertised by peer (bytes/sec).
    pub max_bandwidth_bps: u64,
}

/// Operational state of a registered provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderState {
    /// Actively responding to queries with low latency.
    Active,
    /// Missed heartbeat or query timeout; pending probe.
    Degraded,
    /// No contact within expiration threshold; excluded from selection.
    Stale,
    /// Returned corrupted chunks or falsified bitfields; blacklisted.
    Banned,
}
```

### 3.2 Provider Lifecycle Operations
1. **Registration / Ingestion:** When a `CONTENT_ADVERTISE` message is received, the registry inserts or updates the record. Default advertisement TTL is 10 minutes (`DEFAULT_PROVIDER_TTL = 600s`).
2. **Refresh & Reappearance:** Receipt of valid chunks or query replies resets `last_seen_at` and extends `expires_at`.
3. **Progressive Degradation:** If a provider fails to respond to a chunk request within $3 \times \text{RTT}$, its state transitions to `Degraded`.
4. **Invalidation & Slashing:** If a provider delivers a chunk that fails cryptographic BLAKE3 verification, its reliability score is slashed by 50%. Three consecutive corrupted chunks immediately transition the provider to `Banned` for 1 hour.
5. **Untrusted Claim Principle:** Provider advertisements are treated as purely unverified hints. The system assumes a provider possesses a chunk *only* when the chunk has been received and verified.

---

## 4. Content Query & Discovery Protocol

Content discovery operates via bounded, rate-controlled messages framed inside Phase 05 protocol envelopes using `MessageType::File` (`7`).

```
Requesting Node                                       Remote Peer(s)
       │                                                     │
       │ 1. CONTENT_QUERY (ContentId, ChunkRange, TTL)       │
       ├────────────────────────────────────────────────────►│
       │                                                     │
       │ 2. CONTENT_ADVERTISE (ContentId, Bitfield, Capacity)│
       │◄────────────────────────────────────────────────────┤
       │                                                     │
       │ 3. CONTENT_REQUEST (SessionId, Chunks=[0,1], CapTok)│
       ├────────────────────────────────────────────────────►│
       │                                                     │
       │ 4. CONTENT_RESPONSE (SessionId, Accepted, Window)   │
       │◄────────────────────────────────────────────────────┤
       │                                                     │
       │ 5. CONTENT_CHUNK (SessionId, Index=0, Bytes)        │
       │◄────────────────────────────────────────────────────┤
       │                                                     │
       │ 6. CONTENT_ACK (SessionId, Index=0, Verified=true)  │
       ├────────────────────────────────────────────────────►│
```

### 4.1 Wire Message Schemas
```rust
/// Content System Wire Protocol Envelope (Framed in MessageType::File = 7)
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContentMessage {
    /// Broadcast or routed query discovering providers for a content object.
    Query(ContentQuery),
    /// Provider advertisement announcing content possession.
    Advertise(ContentAdvertise),
    /// Targeted request for specific chunks.
    Request(ContentRequest),
    /// Negotiation response accepting or rejecting request.
    Response(ContentResponse),
    /// Bulk payload containing a single verified chunk.
    Chunk(ChunkPayload),
    /// Acknowledgement of verified chunk receipt.
    Ack(ChunkAck),
    /// Cancellation of transfer session.
    Cancel(TransferCancel),
    /// Notification of successful complete content verification.
    Complete(TransferComplete),
    /// Explicit error frame.
    Error(ContentErrorFrame),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentQuery {
    pub query_id: MessageId,
    pub content_id: ContentId,
    pub desired_chunk_range: Option<(u32, u32)>,
    pub max_response_providers: u8,
    pub requester: NodeId,
    pub expires_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentAdvertise {
    pub query_id: Option<MessageId>,
    pub content_id: ContentId,
    pub provider: NodeId,
    pub has_full_content: bool,
    pub available_chunks: Option<ChunkBitfield>,
    pub total_chunks: u32,
    pub max_chunk_size: u32,
    pub ttl_seconds: u32,
    pub signature: [u8; 64],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentRequest {
    pub session_id: SessionId,
    pub content_id: ContentId,
    pub requested_chunks: Vec<u32>,
    pub capability_token: Vec<u8>,
    pub max_window_bytes: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentResponse {
    pub session_id: SessionId,
    pub content_id: ContentId,
    pub accepted_chunks: Vec<u32>,
    pub rejected_chunks: Vec<(u32, RejectReason)>,
    pub nominal_chunk_size: u32,
    pub total_bytes_pending: u64,
}
```

### 4.2 Query Validation & Fail-Closed Rejection
Receiving nodes discard and log queries fail-closed under any of the following conditions:
1. `expires_at` is in the past ($> 500\text{ ms}$ clock skew allowance).
2. `content_id` multihash codec is unsupported.
3. Requester is blocked or blacklisted in the Phase 03 Trust Store.
4. Local rate limiter detects flooding ($> 10\text{ queries/sec}$ from same `NodeId`).

---

## 5. Decentralized Provider Selection Engine

Rather than relying on a centralized scheduler, the requester evaluates candidate providers locally using a **deterministic multi-factor utility function**.

### 5.1 Provider Selection Interface (`ProviderSelector`)
```rust
/// Pluggable provider evaluation and selection strategy.
pub trait ProviderSelector: Send + Sync {
    /// Scores and ranks candidate providers for a target set of missing chunks.
    fn select_providers(
        &self,
        content_id: &ContentId,
        missing_chunks: &[u32],
        candidates: &[ProviderRecord],
        active_transfers: usize,
    ) -> Vec<ChunkAssignment>;
}

/// Explicit assignment of a chunk to a target provider.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkAssignment {
    pub chunk_index: u32,
    pub selected_provider: NodeId,
    pub estimated_rtt_ms: u32,
    pub priority: u8,
}
```

### 5.2 Mathematical Utility Formulation
For each candidate provider $P$ and chunk $c$, the utility score $U(P, c)$ is computed as:

$$U(P, c) = w_A \cdot \mathbb{I}(c \in P) + w_T \cdot T(P) + w_R \cdot R(P) - w_L \cdot \frac{\text{RTT}(P)}{\text{RTT}_{\max}} - w_Q \cdot \frac{Q(P)}{Q_{\max}} - w_H \cdot \text{Hops}(P)$$

Where:
- $\mathbb{I}(c \in P) \in \{0, 1\}$: Binary indicator whether provider $P$ advertises possession of chunk $c$.
- $T(P) \in [0.0, 1.0]$: Normalized Phase 03 trust tier (`Authorized` = 1.0, `Verified` = 0.8, `Known` = 0.5, `Unknown` = 0.0).
- $R(P) \in [0.0, 1.0]$: Historical transfer reliability ratio.
- $\text{RTT}(P)$: Moving average round-trip latency.
- $Q(P)$: Current in-flight chunk request queue depth assigned to provider $P$.
- $\text{Hops}(P)$: Distance-vector hop count from Phase 05 routing table (direct 1-hop = 0 penalty; multi-hop incurs link cost).
- Normalized Weights: $w_A = 0.35, w_T = 0.20, w_R = 0.15, w_L = 0.15, w_Q = 0.10, w_H = 0.05$.

The chunk is assigned to the provider yielding the highest non-zero utility. If all candidates score below an operational threshold ($U_{\min} = 0.25$), chunk scheduling is deferred.

---

## 6. Chunk-Level Requesting & Pipeline Framing

To maximize throughput over links with high bandwidth-delay products (BDP), chunk retrieval operates via **pipelined sliding windows**:

```
Window Size: W = 4 Chunks (1 MB in-flight)
┌────────────────────────────────────────────────────────┐
│ Chunk 0 (In-Flight) │ Chunk 1 (In-Flight) │ Chunk 2... │
└────────────────────────────────────────────────────────┘
         │ ACK 0 Arrives & Verifies
         ▼
┌────────────────────────────────────────────────────────┐
│ Chunk 1 (In-Flight) │ Chunk 2 (In-Flight) │ Chunk 4... │
└────────────────────────────────────────────────────────┘
```

### 6.1 Chunk Message Payloads
```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkPayload {
    pub session_id: SessionId,
    pub chunk_index: u32,
    pub chunk_id: ChunkId,
    pub offset: u64,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkAck {
    pub session_id: SessionId,
    pub chunk_index: u32,
    pub verified: bool,
    pub bytes_received: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkFailure {
    pub session_id: SessionId,
    pub chunk_index: u32,
    pub reason: ChunkFailReason,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChunkFailReason {
    ChecksumMismatch,
    Timeout,
    ProviderUnreachable,
    StorageError,
}
```

### 6.2 Chunk Verification Invariant
Every received chunk must be immediately validated against its expected `ChunkId`:
$$\text{BLAKE3}(\text{chunk\_payload.data}) == \text{manifest.chunks}[\text{chunk\_index}].\text{chunk\_id}$$
If the digest does not match, the chunk is discarded immediately, zero bytes are committed to persistent CAS storage, and an explicit `ChunkFailure` is logged.

---

## 7. Persistent Resumable Transfer Architecture

In tactical environments, transfers must survive application crashes, node reboots, and multi-hour network partitions.

### 7.1 Persistent Transfer Session Model (`TransferSession`)
All in-flight transfer state is checkpointed to SQLite (`transfer_sessions` table) on every verified chunk:

```rust
pub struct TransferSession {
    /// Unique 16-byte transfer session identifier.
    pub session_id: SessionId,
    /// Target content identifier.
    pub content_id: ContentId,
    /// Cached Merkle manifest (if retrieved).
    pub manifest: Option<ContentManifest>,
    /// Bitfield representing verified chunks stored in staging.
    pub verified_bitfield: ChunkBitfield,
    /// Bitfield representing currently requested in-flight chunks.
    pub in_flight_bitfield: ChunkBitfield,
    /// Active provider assignments: chunk_index -> (provider_node, assigned_at).
    pub active_assignments: HashMap<u32, (NodeId, Timestamp)>,
    /// Accumulated verified bytes on disk.
    pub verified_bytes: u64,
    /// Total expected content bytes.
    pub total_bytes: u64,
    /// Monotonic retry counter per chunk.
    pub chunk_retries: HashMap<u32, u8>,
    /// Current transfer state machine state.
    pub state: TransferState,
    /// Physical creation timestamp.
    pub created_at: Timestamp,
    /// Last forward progress timestamp.
    pub updated_at: Timestamp,
    /// Configured priority level.
    pub priority: Priority,
}
```

### 7.2 Detailed Transfer State Machine
```
                       ┌────────────────┐
                       │   DISCOVERED   │
                       └───────┬────────┘
                               │ User / App initiates transfer
                               ▼
                       ┌────────────────┐
                       │   REQUESTED    │
                       └───────┬────────┘
                               │ Provider responds; Session established
                               ▼
                       ┌────────────────┐
                       │  NEGOTIATING   │
                       └───────┬────────┘
                               │ Manifest acquired & validated
                               ▼
                       ┌────────────────┐  Transport Drop   ┌────────────────┐
                       │  TRANSFERRING  ├──────────────────►│     PAUSED     │
                       └───────┬────────┘                   └───────┬────────┘
                               │                                    │ Reconnect
                               │ All Chunks Verified                ▼
                               │                            ┌────────────────┐
                               │                            │    RESUMING    │
                               │                            └───────┬────────┘
                               │                                    │
                               ▼◄───────────────────────────────────┘
                       ┌────────────────┐
                       │   VERIFYING    │
                       └───────┬────────┘
                               │ Merkle Root Validated; Committed to CAS
                               ▼
                       ┌────────────────┐
                       │    COMPLETE    │
                       └────────────────┘

Terminal Failure States:
[ FAILED ]    - All candidate providers exhausted or max retries exceeded.
[ CANCELLED ] - Terminated via API request or user cancellation.
[ EXPIRED ]   - Inactivity elapsed past session timeout (default: 24h).
[ CORRUPTED ] - Manifest signature invalid or root seal unresolvable.
```

### 7.3 Detailed State Transitions
- **`DISCOVERED` $\to$ `REQUESTED`:** Triggered when the application calls `request_content()`. The system generates a `SessionId` and queries providers for the manifest.
- **`REQUESTED` $\to$ `NEGOTIATING`:** Triggered when at least one candidate provider returns manifest availability. Capability tokens are exchanged and verified.
- **`NEGOTIATING` $\to$ `TRANSFERRING`:** The manifest has been verified. The chunk scheduler begins issuing pipelined chunk requests to providers.
- **`TRANSFERRING` $\to$ `PAUSED`:** Triggered if all active transport links disconnect or the node shuts down. Verified chunks remain locked in staging on disk; in-flight requests are cleared.
- **`PAUSED` $\to$ `RESUMING`:** Triggered on node reboot or peer reconnection. The system reloads `verified_bitfield` from SQLite, verifies staged chunks, re-registers providers, and queries for remaining missing chunks.
- **`RESUMING` $\to$ `TRANSFERRING`:** Missing chunk assignments are re-issued. Zero previously verified chunks are retransmitted.
- **`TRANSFERRING` $\to$ `VERIFYING`:** The last missing chunk is received and verified at Tier 1. The engine computes the complete Merkle root tree over all staged chunks.
- **`VERIFYING` $\to$ `COMPLETE`:** Merkle root matches `manifest.merkle_root` and `ContentId`. Chunks are atomically linked into `<data_dir>/content/chunks/`, and the object transitions to `AVAILABLE`.

---

## 8. Multi-Source Swarm Transfer & Chunk Scheduling

When content is available across multiple nodes, the `ChunkScheduler` orchestrates parallel retrieval to maximize mesh throughput.

```
Example Scenario:
Content Size: 100 MB (400 Chunks of 256 KB)
- Node Alpha:   Holds Chunks 0..199
- Node Bravo:   Holds Chunks 200..399
- Node Charlie: Holds Chunks 100..299 (Overlapping)
- Node Delta:   Holds Chunks 0..399   (Complete, but high latency)

ChunkScheduler Strategy:
- Worker 1 (Alpha):   Pulls Chunks 0..99
- Worker 2 (Charlie): Pulls Chunks 100..199
- Worker 3 (Bravo):   Pulls Chunks 200..399
- Worker 4 (Delta):   Standby reserve / Endgame hedging
```

### 8.1 Piece Selection Strategies
1. **Rarest-First Selection (Standard Mode):**
   - The scheduler computes the mesh frequency of each missing chunk across all discovered provider bitfields:
     $$\text{Rarity}(c) = \sum_{P \in \text{Candidates}} \mathbb{I}(c \in P)$$
   - Chunks with the lowest rarity count are requested first. This prevents the swarm from losing rare chunks if an edge provider disconnects.
2. **Sequential Window Selection (Streaming Mode):**
   - When an application calls `open_stream()`, the scheduler prioritizes chunks sequentially within an active playback buffer window ($[c_{\text{current}} .. c_{\text{current}} + 16]$).
   - Remaining chunks outside the playback window revert to Rarest-First background downloading.
3. **Endgame Mode & Speculative Hedging:**
   - When $> 95\%$ of chunks are completed and only a handful of slow or stalled chunks remain ($N_{\text{remaining}} \le 3$), the scheduler enters **Endgame Mode**.
   - Duplicate speculative requests for the lagging chunks are dispatched concurrently to all remaining active providers.
   - The first valid chunk to arrive is verified and committed; duplicate arrivals from other peers are acknowledged and discarded, preventing tail latency stalls.

---

## 9. Transport Abstraction & Protocol Integration

The Content System is strictly decoupled from specific radio and network hardware.

```
┌────────────────────────────────────────────────────────┐
│             Content System (crates/fieldnet-content)   │
└───────────────────────────┬────────────────────────────┘
                            │ Serialized ContentMessage
                            ▼
┌────────────────────────────────────────────────────────┐
│           Phase 05 Protocol Layer (fieldnet-protocol)  │
│  - Message Envelope: MessageType::File (7)             │
│  - Priority: Low / Normal (Bulk) or High (Control)     │
│  - Sequence, Cryptographic MessageId, Hop TTL          │
└───────────────────────────┬────────────────────────────┘
                            │ Routed Logical Message
                            ▼
┌────────────────────────────────────────────────────────┐
│           Phase 05 Routing Engine (fieldnet-runtime)   │
│  - Distance-Vector Routing Table                       │
│  - Multi-hop Forwarding across Relays                  │
└───────────────────────────┬────────────────────────────┘
                            │ Physical Next-Hop Link
                            ▼
┌────────────────────────────────────────────────────────┐
│          Phase 04 Transport Layer (fieldnet-network)   │
│  - Transport Trait: TCP, UDP, BLE, Wi-Fi Direct        │
│  - ChaCha20-Poly1305 Authenticated Secure Sessions     │
└────────────────────────────────────────────────────────┘
```

### 9.1 Demarcation between Control and Bulk Content Planes
To prevent large file downloads from stalling critical mesh control signals (heartbeats, routing updates, emergency alerts):
1. **Control Messages (`CONTENT_QUERY`, `CONTENT_ADVERTISE`, `CONTENT_REQUEST`, `CONTENT_ACK`):**
   - Maximum size $\le 4\text{ KB}$.
   - Assigned `Priority::Normal` or `Priority::High`.
   - Route through standard message queues and DTN storage.
2. **Bulk Data Messages (`CONTENT_CHUNK`):**
   - Maximum size: $256\text{ KB} + \text{header overhead} \approx 257\text{ KB}$.
   - Assigned `Priority::Low` (Bulk background).
   - Preempted instantly if high-priority control frames enter the transmission queue.
   - Sockets employ non-blocking asynchronous streaming directly from kernel buffers, avoiding double-buffering in user-space heap.

---

## 10. Delay-Tolerant Network (DTN) Integration

In partitioned meshes, content transfers must operate across space and time using physical data couriers.

```
Tactical Data Courier Scenario:
Node A (HQ) ------------> Node C (Vehicle Relay) ------------> Node B (Outpost)
(Isolated from B)         (Carries data physically)           (Isolated from A)
```

### 10.1 DTN Content Transfer Semantics
1. **Manifest Distribution via DTN:**
   - Node A advertises a critical operational briefing (`ContentId_X`).
   - The advertisement and Merkle manifest ($4\text{ KB}$) are queued into Node C's SQLite DTN storage (`dtn_bundles`) with `Priority::High`.
   - Node C drives physically into communication range of Node B.
   - The DTN engine forwards the manifest to Node B. Node B now possesses the cryptographic blueprint for `ContentId_X`.
2. **Opportunistic Chunk Delivery:**
   - Node B issues a `CONTENT_REQUEST` to Node C.
   - While in proximity, Node C streams chunks 0 through 100 to Node B over high-bandwidth ad-hoc Wi-Fi.
   - If Node C drives out of range at chunk 85, Node B freezes the session at `PAUSED` (85 chunks verified).
   - When another vehicle (Node D) arrives carrying the remaining chunks, Node B requests only chunks 86 through 100.
   - Reconstructed content becomes `AVAILABLE` at Node B with zero end-to-end internet or direct connectivity to HQ.

---

## 11. Content Availability & State Invariant Hierarchy

The system defines 7 progressive availability tiers:

```
[ KNOWN ]       -> Node knows ContentId exists (from a document or message).
      ↓
[ DISCOVERED ]  -> Node knows at least one remote provider node holding it.
      ↓
[ REQUESTED ]   -> Transfer session created; negotiation in-flight.
      ↓
[ PARTIAL ]     -> Manifest and >= 1 verified chunk stored in staging.
      ↓
[ COMPLETE ]    -> 100% of chunks received into staging buffer.
      ↓
[ VERIFIED ]    -> Merkle root hash validated against ContentId.
      ↓
[ AVAILABLE ]   -> Materialized to CAS; readable via API; eligible for serving.
```

### 11.1 Immutable Availability Invariants
- **INVARIANT A1:** A manifest alone **NEVER** makes content `AVAILABLE`.
- **INVARIANT A2:** A provider advertisement alone **NEVER** makes content `AVAILABLE`.
- **INVARIANT A3:** `PARTIAL` or `COMPLETE` content in staging is **NEVER** readable by applications or served to remote peers. Only `VERIFIED` content transitioning to `AVAILABLE` may be served.
- **INVARIANT A4:** If a node restarts while content is in `VERIFIED` (prior to disk commit), it must re-validate the Merkle tree before transitioning to `AVAILABLE`.

---

## 12. Verification & Anti-Poisoning Architecture

Integrity verification operates under a strict **Zero-Trust Input Model**:

```
Inbound Wire Packet
        │
        ▼
[ Tier 1: Leaf Chunk Check ] ── Fail ──► Discard Chunk; Slash Provider Reputation
        │ Pass
        ▼
[ Write to Staging File ]
        │ All Chunks Present
        ▼
[ Tier 2: Merkle Branch Check ] ─ Fail ──► Quarantine Session; Drop Corrupted Chunks
        │ Pass
        ▼
[ Tier 3: Root ContentId Seal ] ─ Fail ──► Reject Manifest; Abort Ingestion
        │ Pass
        ▼
[ Atomic CAS Commit (<data_dir>/content/chunks/xx/yy/) ]
        │
        ▼
[ Emit Event: content.transfer.completed ]
```

### 12.1 Explicit Threat Handling
- **Poisoned Chunk Injection:** Attacker sends valid-looking chunk bytes with malicious payload. Verification Tier 1 detects $\text{BLAKE3}(\text{payload}) \neq \text{expected\_id}$. Payload is dropped before touching filesystem CAS.
- **Falsified Merkle Manifest:** Attacker publishes a modified manifest with fake chunk hashes. Verification Tier 3 detects $\text{BLAKE3}(\text{manifest}) \neq \text{requested\_ContentId}$. Manifest is discarded immediately.
- **Truncated Content Attack:** Provider sends fewer bytes than nominal chunk size for an intermediate chunk. Tier 1 rejects because chunk size does not match expected descriptor size.
- **Never Silent Repair:** If a stored chunk fails validation during a read operation (e.g. storage bit-rot), the system logs `content.verification_failed`, marks the chunk as missing in SQLite, and transitions the object from `AVAILABLE` to `PARTIAL` to trigger background mesh re-fetching.

---

## 13. Content Authorization & Capability Enforcement

Network authentication (Phase 04 mutual Ed25519 handshake) guarantees *who* a peer is. It does **not** authorize content access.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     CONTENT CAPABILITY TOKEN STRUCTURE                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Issuer:    NodeId_Admin (Ed25519 Sovereign Authority)                           │
│ Subject:   NodeId_Bravo (Authorized Recipient Node)                             │
│ Resource:  fn-cid-1e20a4b8... (Specific ContentId or Wildcard Namespace)       │
│ Actions:   [ READ, REPLICATE ]                                                  │
│ IssuedAt:  1790520000000 (Physical Timestamp)                                   │
│ ExpiresAt: 1790523600000 (+1 Hour TTL)                                          │
│ Nonce:     8f92b1a0... (Replay Protection)                                     │
│ Signature: [64 Bytes Ed25519 Cryptographic Signature over Token Fields]         │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 13.1 Granular Enforcement Points
1. **Metadata Query:** `CONTENT_QUERY` containing restricted namespaces must present a valid capability token or receive an empty response.
2. **Chunk Request:** `CONTENT_REQUEST` must embed a valid capability token signed by the content owner. The provider validates token signature, subject match, and expiration before transmitting chunks.
3. **Replication Request:** Peers requesting a third party to replicate content must provide a token granting the `REPLICATE` action.
4. **Pinning / Deletion:** Local administrative commands require the caller to possess local node authorization.
5. **Mid-Transfer Revocation:** Providers maintain a lightweight Bloom filter of revoked capability tokens distributed via Phase 06 CRDT sync. If a token is revoked mid-transfer, ongoing chunk streaming aborts immediately.

---

## 14. Replication & Storage Retention Models

Content storage utilizes four distinct retention classes:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       CONTENT RETENTION CLASS MATRIX                            │
├───────────────┬──────────────┬───────────────┬──────────────────────────────────┤
│ Class         │ Immune to GC?│ Sync Trigger  │ Primary Use Case                 │
├───────────────┼──────────────┼───────────────┼──────────────────────────────────┤
│ **`PINNED`**  │ **YES (100%)**│ Explicit user │ Critical maps, operational keys, │
│               │              │ or policy pin │ mission-critical applications.   │
├───────────────┼──────────────┼───────────────┼──────────────────────────────────┤
│ `REPLICATED`  │ Conditional  │ Autonomous    │ Mesh-shared documents, team      │
│               │ (High Thresh)│ mesh policy   │ manuals, decentralized backups.  │
├───────────────┼──────────────┼───────────────┼──────────────────────────────────┤
│ **`CACHED`**  │ **NO**       │ On-demand     │ Recently viewed files, transient │
│               │ (LRU Evicted)│ user reads    │ attachments, remote media.       │
├───────────────┼──────────────┼───────────────┼──────────────────────────────────┤
│ `TEMPORARY`   │ Immediate TTL│ Incomplete    │ Staging part files, chunk        │
│               │ (24h purge)  │ download      │ assembly buffers, thumbnails.    │
└───────────────┴──────────────┴───────────────┴──────────────────────────────────┘
```

### 14.1 Pinning Semantics
- An application or user pins content via `pin_content(ContentId)`.
- The `content_objects` database record updates `retention_tier = RetentionTier::Pinned`.
- **INVARIANT R1:** Automated garbage collection or low-disk-space triggers must **NEVER** delete, evict, or truncate `PINNED` content. If the disk is exhausted by pinned content, new transfers fail with an explicit `StorageExhausted` error rather than evicting pinned data.

---

## 15. Backpressure, Flow Control & Resource Bounds

To prevent adversarial or accidental resource exhaustion, the Content System enforces hard limits across all subsystems:

| Resource Dimension | Tactical Mesh Limit | Enforcement Mechanism |
| :--- | :--- | :--- |
| **Max Concurrent Transfers** | **8 Active Sessions** | Additional requests queued in FIFO order. |
| **Max In-Flight Chunk Requests** | **16 Chunks per Provider** | Sliding-window credit-based flow control. |
| **Max Ingest Chunk Size** | **1,048,576 Bytes (1 MB)** | Hard link-layer truncation and packet drop. |
| **Max Single Content Size** | **10 GB** (Configurable) | Manifest rejected before transfer initialization. |
| **Per-Peer Bandwidth Cap** | Configurable (e.g. 5 MB/s) | Token bucket rate-limiter on socket writes. |
| **Max Memory Ingestion Buffer** | **16 MB per Session** | Backpressure applied to socket read streams. |
| **Max Retry Count per Chunk** | **5 Retries** | Chunk marked failed; alternative provider selected. |
| **Max Provider Candidates** | **32 Nodes per Content** | Provider registry prunes lowest-scoring nodes. |

### 15.1 Dynamic Backpressure Protocol
When a provider node experiences high CPU load, battery tripwires ($< 15\%$), or saturated disk I/O, it throttles incoming requests by returning `CONTENT_RESPONSE { accepted_chunks: [], rejected_chunks: [(id, RejectReason::ServerBusy)] }`. The requester applies exponential backoff ($250\text{ ms} \to 500\text{ ms} \to 1000\text{ ms}$) before retrying.

---

## 16. Comprehensive Failure Taxonomy & Recovery Matrix

The architecture defines deterministic state transitions for all 15 operational failure modes:

| ID | Failure Condition | Subsystem Affected | Deterministic Recovery Behavior |
| :--- | :--- | :--- | :--- |
| **F01** | Provider Disappears Mid-Transfer | `TransferManager` | Timeout detected ($3 \times \text{RTT}$); in-flight chunks reassigned to alternative providers; provider marked `Degraded`. |
| **F02** | Provider Token Revoked Mid-Transfer | `ContentAuthorizer`| Provider terminates session; returns `CONTENT_ERROR { Unauthorized }`; requester drops connection. |
| **F03** | Provider Returns Corrupted Chunk | `ContentStore` | Tier 1 verify fails; chunk dropped; provider reliability slashed; chunk re-requested from alternative peer. |
| **F04** | Transport Connection Disconnects | `TransferSession` | Session state transitions to `PAUSED`; verified chunks retained; auto-resumes on transport re-connection. |
| **F05** | Route Disappears from Routing Table| `TransferManager` | Session pauses; discovery engine issues `CONTENT_QUERY` to locate alternative routing paths or providers. |
| **F06** | Disk Space Reaches High Watermark | `StorageQuota` | LRU eviction purges `CACHED` tier down to low watermark; if still insufficient, transfer halts with `StorageExhausted`. |
| **F07** | Transfer Session Expires (TTL Out)| `TransferSession` | Incomplete staging files purged; session transitions to `EXPIRED`; manifest retained in cache. |
| **F08** | Node Restarts During Download | `RuntimeCore` | On boot, SQLite WAL recovers session; staging chunks verified against bitfield; session resumes from last valid chunk. |
| **F09** | Manifest Missing from Mesh | `ContentResolver` | Resolution fails with `ManifestNotFound`; periodic background query retries with exponential backoff. |
| **F10** | Missing Chunk Across Entire Swarm | `ChunkScheduler` | Missing chunk flagged; transfer pauses at `PARTIAL`; alerts application; resumes when a provider holding chunk joins. |
| **F11** | Stale Provider Advertisement | `ProviderRegistry` | Request times out; registry decrements reliability score; if unresponsive, provider marked `Stale` and evicted. |
| **F12** | Conflicting Manifests from Peers | `IntegrityEngine` | Manifest cryptographic seal must equal `ContentId`; falsified manifests fail verification and are discarded immediately. |
| **F13** | Duplicate Chunk Arrives | `TransferManager` | Deduplication index detects chunk already verified; duplicate acknowledged and dropped with zero disk write. |
| **F14** | Application Cancels Transfer | `TransferManager` | Session transitions to `CANCELLED`; in-flight requests aborted; staging partial files unlinked. |
| **F15** | Capability Revoked During Transfer | `TransferSession` | ContentAuthorizer revokes session ticket; ongoing transfer halts; partial chunks in staging quarantined. |

---

## 17. Structured Observability Framework

Every content operation generates structured, correlation-tagged events dispatched over the FIELDNET Event Bus (`fieldnet-core`):

```rust
/// Comprehensive taxonomy of Content System observability events.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ContentEvent {
    QueryStarted { query_id: MessageId, content_id: ContentId },
    QueryCompleted { query_id: MessageId, providers_found: usize },
    ProviderDiscovered { content_id: ContentId, provider: NodeId, full_copy: bool },
    ProviderExpired { content_id: ContentId, provider: NodeId, reason: String },
    ProviderSelected { content_id: ContentId, provider: NodeId, chunk_index: u32, score: f32 },
    TransferStarted { session_id: SessionId, content_id: ContentId, total_chunks: u32 },
    TransferPaused { session_id: SessionId, reason: String },
    TransferResumed { session_id: SessionId, verified_chunks: u32 },
    ChunkRequested { session_id: SessionId, chunk_index: u32, provider: NodeId },
    ChunkReceived { session_id: SessionId, chunk_index: u32, bytes: u32, provider: NodeId },
    ChunkVerified { session_id: SessionId, chunk_index: u32, duration_us: u64 },
    ChunkRejected { session_id: SessionId, chunk_index: u32, provider: NodeId, reason: String },
    ProviderFailed { session_id: SessionId, provider: NodeId, consecutive_failures: u8 },
    TransferFailed { session_id: SessionId, content_id: ContentId, reason: String },
    TransferCompleted { session_id: SessionId, content_id: ContentId, total_bytes: u64, duration_ms: u64 },
    VerificationFailed { content_id: ContentId, tier: u8, reason: String },
    ReplicationStarted { content_id: ContentId, target_replicas: u8 },
    ReplicationCompleted { content_id: ContentId, current_replicas: u8 },
    ContentEvicted { content_id: ContentId, bytes_reclaimed: u64, tier: RetentionTier },
}
```

---

## 18. Testing & Verification Architecture

Phase 07 mandates automated testing across four verification rings:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         FOUR TESTING ARCHITECTURE RINGS                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Ring 1: In-Memory Unit & Cryptographic Invariant Tests                          │
│ - Deterministic BLAKE3 chunking, Merkle tree generation, bitfield manipulation. │
│ - Property Test: Permuted chunk reconstruction yields identical byte stream.    │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Ring 2: Local Storage & CAS Persistence Tests                                   │
│ - Two-tier fanout filesystem writes, SQLite index transactions, deduplication.  │
│ - Restart durability: Database close/reopen preserves verified bitfields.       │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Ring 3: Simulated Swarm & Resumption Fault Injection                            │
│ - Multi-source parallel transfer across mock channels (A has 0..40, B has 41..).│
│ - Fault Injection: Dropped connections, bit flips in chunks, timeout recovery.  │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Ring 4: Real Network & Multi-Hop Runtime Integration Tests                      │
│ - Three real FIELDNET runtime processes over ephemeral TCP/UDP sockets.         │
│ - Multi-hop relay transfer (A <-> B <-> C) and DTN delayed transfer.            │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 19. Rust Module Boundaries & Architectural Crate Layout

Phase 07 will be implemented cleanly in a dedicated crate **`crates/fieldnet-content`**:

```
crates/fieldnet-content/
├── Cargo.toml
└── src/
    ├── lib.rs                  <-- Public API, ContentSystem trait, error types
    ├── cid.rs                  <-- Canonical ContentId & ChunkId multihash definitions
    ├── chunker.rs              <-- Adaptive fixed-size chunking & Chunker trait
    ├── manifest.rs             <-- ContentManifest schema, Merkle root builder
    ├── merkle.rs               <-- Binary Merkle tree implementation & branch proofs
    ├── storage/
    │   ├── mod.rs
    │   ├── cas.rs              <-- Physical disk CAS manager (chunks/xx/yy/...)
    │   ├── index.rs            <-- SQLite index engine (content.db transactions)
    │   └── quota.rs            <-- Storage quota accounting & LRU eviction engine
    ├── registry/
    │   ├── mod.rs
    │   ├── registry.rs         <-- ProviderRegistry in-memory & SQLite cache
    │   ├── record.rs           <-- ProviderRecord schema & lifecycle states
    │   └── selector.rs         <-- ProviderSelector trait & Multi-Factor Utility engine
    ├── transfer/
    │   ├── mod.rs
    │   ├── manager.rs          <-- TransferManager orchestrator
    │   ├── session.rs          <-- TransferSession state machine & SQLite persistence
    │   ├── scheduler.rs        <-- ChunkScheduler (Rarest-first, sequential, endgame)
    │   └── bitfield.rs         <-- High-performance ChunkBitfield implementation
    ├── protocol/
    │   ├── mod.rs
    │   ├── messages.rs         <-- ContentMessage wire enum (framed in MessageType::File)
    │   └── handler.rs          <-- Inbound message dispatcher & response builder
    ├── auth.rs                 <-- ContentAuthorizer capability token verification
    ├── stream.rs               <-- ContentReader async streaming & range seek reader
    └── events.rs               <-- Structured observability events for Event Bus
```

---

## 20. Application API Surface Specification

```rust
#[async_trait]
pub trait ContentSystem: Send + Sync {
    /// Ingests local stream or file into CAS, producing an immutable ContentId.
    async fn put(&self, data: BoxStream<'static, Bytes>, size: u64) -> Result<ContentId, ContentError>;

    /// Resolves content, checking local CAS first, then initiating mesh discovery.
    async fn resolve_content(&self, content_id: &ContentId) -> Result<ContentResolution, ContentError>;

    /// Discovers active providers currently possessing content chunks.
    async fn find_providers(&self, content_id: &ContentId, timeout: Duration) -> Result<Vec<ProviderRecord>, ContentError>;

    /// Initiates or resumes a multi-source parallel download.
    async fn request_content(&self, content_id: &ContentId, options: TransferOptions) -> Result<TransferHandle, ContentError>;

    /// Opens an asynchronous zero-copy streaming reader supporting range seeks.
    async fn open_stream(&self, content_id: &ContentId) -> Result<Box<dyn ContentReader>, ContentError>;

    /// Pauses an active transfer session, retaining verified chunks in staging.
    async fn pause_transfer(&self, session_id: &SessionId) -> Result<(), ContentError>;

    /// Resumes a paused or interrupted transfer session from verified watermarks.
    async fn resume_transfer(&self, session_id: &SessionId) -> Result<TransferHandle, ContentError>;

    /// Cancels a transfer session and unlinks temporary staging files.
    async fn cancel_transfer(&self, session_id: &SessionId) -> Result<(), ContentError>;

    /// Pins content locally, guaranteeing immunity from automated LRU eviction.
    async fn pin_content(&self, content_id: &ContentId) -> Result<(), ContentError>;

    /// Unpins content, returning it to the evictable CACHED tier.
    async fn unpin_content(&self, content_id: &ContentId) -> Result<(), ContentError>;

    /// Queries the current progress and telemetry of a transfer session.
    async fn get_transfer_status(&self, session_id: &SessionId) -> Result<Option<TransferSessionStatus>, ContentError>;
}
```

---

## 21. Architectural Invariants Contract

The implementation of Phase 07 must strictly conform to these 13 non-negotiable invariants:

- **INVARIANT 01 (Content Sovereignty):** Content identity is strictly determined by its cryptographic hash and is independent of provider node location or filesystem path.
- **INVARIANT 02 (Untrusted Claims):** Provider advertisements are untrusted metadata; provider possession is confirmed only upon successful chunk cryptographic verification.
- **INVARIANT 03 (Demarcated Auth):** Network transport authentication does not imply content authorization; access to non-public content requires explicit Phase 03 Capability Tokens.
- **INVARIANT 04 (Fail-Closed Integrity):** Every accepted chunk must pass BLAKE3 verification before being committed to persistent CAS storage; corrupted bytes are dropped fail-closed.
- **INVARIANT 05 (Availability Shield):** Partial or unverified content is never exposed to user applications as complete or served to other mesh peers.
- **INVARIANT 06 (Durable Resumability):** Active transfer sessions persist verified chunk bitfields in SQLite; connection loss or node restarts resume without retransmitting verified chunks.
- **INVARIANT 07 (Provider Invariance):** The disappearance or failure of a provider node does not invalidate the immutable content identity or corrupt local partial state.
- **INVARIANT 08 (Transport Neutrality):** Content transfer protocols depend strictly on the Phase 04 transport abstraction and Phase 05 protocol routing, not on specific radio hardware.
- **INVARIANT 09 (Traffic Preemption):** Large bulk file chunks are assigned low priority and must never saturate or delay high-priority network control signals or emergency DTN queues.
- **INVARIANT 10 (Swarm Integrity):** Multi-source parallel retrieval across untrusted peers preserves end-to-end Merkle tree cryptographic integrity guarantees.
- **INVARIANT 11 (Pinning Immunity):** Automated garbage collection, quota trimming, and low-disk triggers must never evict or delete `PINNED` content.
- **INVARIANT 12 (Dynamic Revocation):** Capability token revocation dynamically halts in-flight transfer sessions and prevents subsequent chunk delivery.
- **INVARIANT 13 (Decentralized Autonomy):** Content resolution, swarming, verification, and reconstruction operate fully air-gapped without requiring an internet connection or central server.

---

## 22. Implementation Build Roadmap

The Phase 07 implementation roadmap builds sequentially upon Phase 06:

```
BUILD-01: Content Identity & Content Store (ContentId, Chunk CAS, SQLite Index, put/get/has)
    ↓
BUILD-02: Chunking, Manifests & Merkle Engine (Chunker, ContentManifest, Merkle Tree Verification)
    ↓
BUILD-03: Content Discovery & Provider Registry (CONTENT_QUERY/ADVERTISE, ProviderRegistry)
    ↓
BUILD-04: Transfer Engine, Framing & Resumable Sessions (TransferSession, Bitfields, Resumption)
    ↓
BUILD-05: Multi-Source Parallel Swarm Retrieval (ChunkScheduler, Rarest-First, Multi-Peer Streaming)
    ↓
BUILD-06: Quotas, Eviction, Pinning & Encryption (LRU Quotas, Pinning, Capability Enforcement)
    ↓
BUILD-07: Full Multi-Node Mesh Integration & Verification (Real Network 3-Node Swarm, DTN, Faults)
```

---

## 23. Final Status

```
==================================================
PHASE 07 → PROMPT 02 COMPLETE
==================================================
```
The Distributed Content Resolution, Transfer & Provider Architecture specification is complete, internally verified against all Phase 01–06 subsystems, and ready to guide BUILD-01 implementation without architectural ambiguity.
