# FIELDNET Phase 07 — Architecture Specification
**Document ID:** `PHASE-07-03-CONTENT-LIFECYCLE-REPLICATION-AND-RECOVERY`  
**Title:** Distributed Content Lifecycle, Replication, Recovery & Final Architecture Verification  
**Status:** SPECIFICATION APPROVED (PHASE 07 ARCHITECTURE FINAL)  
**Authoritative References:** `FIELDNET_MASTER_SPEC.md`, `docs/architecture/01-system-model.md`, `docs/architecture/02-runtime-architecture.md`, `docs/architecture/03-security.md`, `docs/architecture/04-networking.md`, `docs/architecture/05-protocol-routing-dtn.md`, `docs/architecture/PHASE-06-01-LOCAL-FIRST-DATA.md`, `docs/architecture/PHASE-07-01-DISTRIBUTED-CONTENT-SYSTEM.md`, `docs/architecture/PHASE-07-02-CONTENT-RESOLUTION-AND-TRANSFER.md`

---

## 1. Complete Distributed Content Lifecycle

The distributed content subsystem governs the complete end-to-end operational journey of data inside FIELDNET across 14 formal stages:

$$\text{CREATE} \longrightarrow \text{IDENTIFY} \longrightarrow \text{STORE} \longrightarrow \text{ADVERTISE} \longrightarrow \text{DISCOVER} \longrightarrow \text{REQUEST} \longrightarrow \text{TRANSFER} \longrightarrow \text{VERIFY} \longrightarrow \text{REPLICATE} \longrightarrow \text{SERVE} \longrightarrow \text{CACHE} \longrightarrow \text{PIN} \longrightarrow \text{EVICT} \longrightarrow \text{RECOVER}$$

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      THE 14 STAGES OF CONTENT LIFECYCLE                         │
├────┬─────────────┬──────────────────────────────────────────────────────────────┤
│ #  │ Stage       │ Exact Subsystem Action & State Transformation                │
├────┼─────────────┼──────────────────────────────────────────────────────────────┤
│ 1  │ CREATE      │ Application supplies byte stream or file path to CAS API.    │
├────┼─────────────┼──────────────────────────────────────────────────────────────┤
│ 2  │ IDENTIFY    │ Engine chunks stream (256 KB) & computes BLAKE3 ContentId.   │
├────┼─────────────┼──────────────────────────────────────────────────────────────┤
│ 3  │ STORE       │ Chunks written to <data_dir>/content/chunks/; manifest saved.│
├────┼─────────────┼──────────────────────────────────────────────────────────────┤
│ 4  │ ADVERTISE   │ Node issues CONTENT_ADVERTISE (ContentId, bitfield, TTL).    │
├────┼─────────────┼──────────────────────────────────────────────────────────────┤
│ 5  │ DISCOVER    │ Remote nodes record provider claims in ProviderRegistry.     │
├────┼─────────────┼──────────────────────────────────────────────────────────────┤
│ 6  │ REQUEST     │ Consumer issues CONTENT_REQUEST with Capability Token.       │
├────┼─────────────┼──────────────────────────────────────────────────────────────┤
│ 7  │ TRANSFER    │ Chunks stream across multi-source swarms via sliding window. │
├────┼─────────────┼──────────────────────────────────────────────────────────────┤
│ 8  │ VERIFY      │ 3-tier integrity check (Leaf Chunk -> Merkle Tree -> Root).  │
├────┼─────────────┼──────────────────────────────────────────────────────────────┤
│ 9  │ REPLICATE   │ Autonomous policy replicates object to meet factor target N. │
├────┼─────────────┼──────────────────────────────────────────────────────────────┤
│ 10 │ SERVE       │ Verified node acts as an active seeding provider for peers.  │
├────┼─────────────┼──────────────────────────────────────────────────────────────┤
│ 11 │ CACHE       │ Content retained locally in CACHED tier (LRU evictable).     │
├────┼─────────────┼──────────────────────────────────────────────────────────────┤
│ 12 │ PIN         │ User/policy marks content PINNED (100% immune to eviction).  │
├────┼─────────────┼──────────────────────────────────────────────────────────────┤
│ 13 │ EVICT       │ Low-priority unpinned chunks purged under disk pressure.     │
├────┼─────────────┼──────────────────────────────────────────────────────────────┤
│ 14 │ RECOVER     │ On reboot, SQLite WAL and CAS scans restore verified states. │
└────┴─────────────┴──────────────────────────────────────────────────────────────┘
```

---

## 2. Storage Layers & Authority Matrix

To guarantee crash consistency, deduplication, and crash recovery, storage is structured into seven distinct layers with explicit persistence semantics:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       STORAGE LAYERS ARCHITECTURAL MATRIX                       │
├────────────────────┬──────────────────┬──────────────┬──────────────┬───────────┤
│ Layer Name         │ Physical Medium  │ Persistence  │ Reconstruct? │ Authority │
├────────────────────┼──────────────────┼──────────────┼──────────────┼───────────┤
│ Content Metadata   │ Phase 06 SQLite  │ Persistent   │ No           │ Author    │
│                    │ (documents.db)   │ (WAL Mode)   │ (Authoritative) (Signed) │
├────────────────────┼──────────────────┼──────────────┼──────────────┼───────────┤
│ Chunk Store        │ Filesystem CAS   │ Persistent   │ Yes          │ Self-     │
│                    │ (chunks/xx/yy/)  │ (Direct IO)  │ (From Mesh)  │ Verifying │
├────────────────────┼──────────────────┼──────────────┼──────────────┼───────────┤
│ Manifest Store     │ SQLite & Disk    │ Persistent   │ Yes          │ Publisher │
│                    │ (manifests/)     │ (CAS Indexed)│ (From Mesh)  │ (Signed)  │
├────────────────────┼──────────────────┼──────────────┼──────────────┼───────────┤
│ Transfer Sessions  │ SQLite           │ Persistent   │ No           │ Local     │
│                    │ (content.db)     │ (Checkpointed│ (Resumable)  │ Replicant │
├────────────────────┼──────────────────┼──────────────┼──────────────┼───────────┤
│ Provider Registry  │ RAM + SQLite     │ Ephemeral /  │ Yes          │ Untrusted │
│                    │ (LRU Cache)      │ Cached       │ (Via Query)  │ Hints     │
├────────────────────┼──────────────────┼──────────────┼──────────────┼───────────┤
│ Replication State  │ SQLite           │ Persistent   │ Yes          │ Local     │
│                    │ (content.db)     │              │ (Calculated) │ Policy    │
├────────────────────┼──────────────────┼──────────────┼──────────────┼───────────┤
│ Ingestion Staging  │ Filesystem       │ Disposable   │ Yes          │ Temp      │
│                    │ (staging/*.part) │ (Pre-verify) │ (Re-request) │ Buffer    │
└────────────────────┴──────────────────┴──────────────┴──────────────┴───────────┘
```

### 2.1 Persistence Invariant
A node restart **must never lose knowledge of already verified chunks**. The `local_chunks` index in SQLite and physical files in `content/chunks/` survive crashes, allowing active transfers to resume without re-downloading existing verified byte ranges.

---

## 3. Comprehensive Content Identity Model

All identifiers in Phase 07 are mathematically and semantically distinct:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      FIELDNET CONTENT IDENTIFIER TAXONOMY                       │
├───────────────────┬──────────────┬──────────────────────────────────────────────┤
│ Identifier Type   │ Byte Format  │ Cryptographic Definition & Scope             │
├───────────────────┼──────────────┼──────────────────────────────────────────────┤
│ **`ContentId`**   │ 34 Bytes     │ Self-describing Multihash over root manifest │
│                   │ (Multihash)  │ or raw payload: BLAKE3(PayloadBytes).        │
├───────────────────┼──────────────┼──────────────────────────────────────────────┤
│ **`ChunkId`**     │ 34 Bytes     │ Self-describing Multihash over single chunk: │
│                   │ (Multihash)  │ BLAKE3(ChunkSliceBytes).                     │
├───────────────────┼──────────────┼──────────────────────────────────────────────┤
│ **`ManifestId`**  │ 34 Bytes     │ Cryptographic hash over canonical serialized │
│                   │ (Multihash)  │ ContentManifest struct. Equal to ContentId.  │
├───────────────────┼──────────────┼──────────────────────────────────────────────┤
│ **`FileMetaId`**  │ 32 Bytes     │ Phase 06 DocumentId: BLAKE3("file-meta" ||   │
│                   │ (UUID/Digest)│ AuthorNodeId || LocalSeq).                   │
├───────────────────┼──────────────┼──────────────────────────────────────────────┤
│ **`VersionId`**   │ 12 Bytes     │ Hybrid Logical Clock (HLC) timestamp marking │
│                   │ (HLC)        │ metadata modification sequence in Phase 06.  │
├───────────────────┼──────────────┼──────────────────────────────────────────────┤
│ **`SessionId`**   │ 16 Bytes     │ Random UUID v4 identifying an in-flight      │
│                   │ (UUID)       │ transfer session between local node & swarm. │
├───────────────────┼──────────────┼──────────────────────────────────────────────┤
│ **`NodeId`**      │ 32 Bytes     │ Sovereign Ed25519 identity of peer/provider. │
└───────────────────┴──────────────┴──────────────────────────────────────────────┘
```

### 3.1 Independence from Location Invariant
The `ContentId` is strictly a function of the content's cryptographic bytes. It **never contains hostnames, IP addresses, port numbers, mesh hop paths, or provider IDs**. Content remains permanently addressable and resolvable even if every initial provider goes offline and the content is served exclusively by new replicants.

---

## 4. Multi-Tier Deduplication Engine

Deduplication operates across chunk, object, and metadata layers:

```
           File A ("manual_v1.pdf")        File B ("manual_v2.pdf")
                     │                               │
            Phase 06 Document A             Phase 06 Document B
                     │                               │
           Manifest A (Chunks 0..9)        Manifest B (Chunks 0..9)
                     │                               │
                     ├── Chunk 0 (Identical) ────────┤  --> Stored ONCE
                     ├── Chunk 1 (Identical) ────────┤  --> Stored ONCE
                     ├── Chunk 2 (Modified) ───┐     │  --> Stored Separate
                     │                         │     ├── Chunk 2' (New)
                     └── Chunk 3..9 (Identical)──────┘  --> Stored ONCE
```

### 4.1 Deduplication Rules
1. **Chunk-Level Deduplication:** Every chunk is indexed in `local_chunks` by its `ChunkId`. When multiple files share identical chunks, physical disk storage is allocated exactly once. The SQLite index maintains an atomic `reference_count`.
2. **Object-Level Deduplication:** If an identical file is imported under multiple filenames, only a single `ContentManifest` and underlying chunk set exist on disk.
3. **Encrypted Deduplication:** Confidential files utilize **Ciphertext Hashing**. Files encrypted with different keys produce distinct ciphertexts and different `ContentId`s, preventing cross-user tracking or dictionary confirmation attacks on untrusted nodes.

---

## 5. Replication Models & Semantics

FIELDNET classifies content retention into four strict operational tiers:

| Tier | Primary Purpose | Expected Lifetime | Eviction Eligibility | Storage Priority |
| :--- | :--- | :--- | :--- | :--- |
| **`PINNED`** | Mission-critical orders, tactical maps, operational software. | Permanent (until explicit user unpin). | **IMMUNE (0%)** — Never evicted automatically. | Highest (Priority 1) |
| **`REPLICATED`** | Decentralized mesh backup of team files. | Weeks to months. | Conditional — Evicted only during extreme emergency. | High (Priority 2) |
| **`CACHED`** | Recently downloaded or accessed remote files. | Days (managed by LRU). | **EVICTABLE (100%)** — Reclaimed under disk pressure. | Normal (Priority 3) |
| **`TEMPORARY`** | Incomplete transfer buffers, preview thumbnails. | Hours (24h hard TTL). | **AGGRESSIVE** — Purged first on quota trip. | Lowest (Priority 4) |

---

## 6. Autonomous Mesh Replication Policies

Replication is decentralized, autonomous, and topology-aware. Nodes do not rely on a central master to allocate replicas:

### 6.1 Replication Triggers
1. **Target Mesh Factor ($N$):** Files can specify target replica counts (e.g. $N = 3$). Connected mesh nodes gossip provider bitfields; if verified provider count drops below $N$, healthy nodes automatically volunteer to replicate and pin copies.
2. **Provider Scarcity Re-seeding:** When a node discovers an object with only 1 known provider whose RTT is degrading or whose battery is low, it opportunistically replicates the content to prevent data loss.
3. **Offline Mission Availability:** Users can declare a directory or tag as `OfflineReady`. The runtime proactively downloads and pins all referenced `ContentId`s before the node disconnects from the base station.

---

## 7. Garbage Collection & Safe Eviction Engine

The `GarbageCollector` runs asynchronously on a timer (default: every 15 minutes) or reactively when storage reaches the high watermark threshold:

```
                      [ Low Disk Trigger (90%) ]
                                  │
                                  ▼
               ┌──────────────────────────────────────┐
               │    Phase 1: Mark Active Shields      │
               │  - Filter out PINNED content         │
               │  - Lock active TransferSessions      │
               │  - Lock manifests for retained files │
               └──────────────────┬───────────────────┘
                                  │
                                  ▼
               ┌──────────────────────────────────────┐
               │     Phase 2: Score Evictable Items   │
               │  - Rank CACHED & TEMPORARY objects   │
               │    via composite LRU-Size scoring    │
               └──────────────────┬───────────────────┘
                                  │
                                  ▼
               ┌──────────────────────────────────────┐
               │    Phase 3: Decrement & Sweep        │
               │  - Decrement chunk reference_count   │
               │  - Unlink files where ref_count == 0 │
               └──────────────────┬───────────────────┘
                                  │
                                  ▼
                    [ Target Reached (75%) ]
```

### 7.1 Absolute Eviction Safety Invariants
- **INVARIANT G1:** Pinned content is **100% immune** to automated eviction.
- **INVARIANT G2:** Chunks locked by an active, running `TransferSession` must **NEVER** be deleted during garbage collection.
- **INVARIANT G3:** A chunk referenced by two manifests must not be physically unlinked until **both** manifests have been evicted and their reference counts reach zero.

---

## 8. Storage Quotas & Exhaustion Management

Every node enforces strict quota limits configured via `StorageQuotaConfig`:

```rust
pub struct StorageQuotaConfig {
    pub max_storage_bytes: u64,        // e.g. 50 GB hard cap
    pub high_watermark_ratio: f32,     // 0.90 (Triggers LRU eviction)
    pub low_watermark_ratio: f32,      // 0.75 (Eviction target)
    pub max_single_content_bytes: u64, // e.g. 10 GB max per object
    pub max_staging_bytes: u64,        // e.g. 5 GB max in-flight downloads
}
```

### 8.1 Behavior when Storage is Saturated
If disk usage exceeds `high_watermark_ratio` and eviction of all `CACHED` and `TEMPORARY` data fails to free space below `max_storage_bytes`:
1. In-flight download sessions transition to `PAUSED`.
2. New transfer requests are rejected fail-closed with `ContentError::StorageExhausted`.
3. Pinned content is **never** truncated or deleted.
4. The system logs `content.quota.exhausted` to alert the user.

---

## 9. Crash Consistency & Restart Recovery

The content subsystem assumes abrupt power loss or operating system crashes can occur at any millisecond. On startup, the runtime executes a deterministic **10-Step Recovery Sequence**:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     10-STEP CRASH RECOVERY BOOT SEQUENCE                        │
├────┬───────────────────────┬────────────────────────────────────────────────────┤
│ 1  │ Open SQLite Index     │ Reopen content.db in WAL mode; recover journal.    │
├────┼───────────────────────┼────────────────────────────────────────────────────┤
│ 2  │ Filesystem Sanity     │ Verify <data_dir>/content/ directories exist.      │
├────┼───────────────────────┼────────────────────────────────────────────────────┤
│ 3  │ Orphan Staging Sweep  │ Identify staging/*.part files with no active DB row│
│    │                       │ and delete orphaned temporary files.               │
├────┼───────────────────────┼────────────────────────────────────────────────────┤
│ 4  │ In-Flight Session Scan│ Query transfer_sessions for state != COMPLETE.     │
├────┼───────────────────────┼────────────────────────────────────────────────────┤
│ 5  │ Chunk Bitfield Verify │ For each in-flight transfer, verify chunk checksums│
│    │                       │ against verified_bitfield. Reset invalid chunks.   │
├────┼───────────────────────┼────────────────────────────────────────────────────┤
│ 6  │ State Transition      │ Transition valid in-flight sessions to PAUSED.     │
├────┼───────────────────────┼────────────────────────────────────────────────────┤
│ 7  │ Manifest Validation   │ Inspect all AVAILABLE objects; ensure root Merkle  │
│    │                       │ manifests are intact on disk.                      │
├────┼───────────────────────┼────────────────────────────────────────────────────┤
│ 8  │ Reference Count Audit │ Verify sum(manifest_chunks) == local_chunks.refs.  │
├────┼───────────────────────┼────────────────────────────────────────────────────┤
│ 9  │ Provider Re-announce  │ Generate CONTENT_ADVERTISE for all AVAILABLE pinned│
│    │                       │ and replicated objects to rejoin mesh swarms.      │
├────┼───────────────────────┼────────────────────────────────────────────────────┤
│ 10 │ Auto-Resume Eligible  │ Re-enable background downloads for uncompleted     │
│    │                       │ priority transfers when network interfaces bind.   │
└────┴───────────────────────┴────────────────────────────────────────────────────┘
```

---

## 10. Partial Transfer Recovery Across Reboots

Transfers interrupted mid-stream preserve verified chunks across restarts:

```
Scenario: 100-Chunk Object (25.6 MB)
- Before Crash: Chunks 0..37 VERIFIED, Chunks 38..64 MISSING, Chunks 65..99 VERIFIED
- Power loss occurs during chunk 38 transmission.
- Reboot Recovery:
  1. TransferSession reloaded from SQLite.
  2. Bitfield indicates chunks 0..37 and 65..99 are present.
  3. Engine validates BLAKE3 hash for the 73 existing staged chunks.
  4. Session resumes: Scheduler requests ONLY missing chunks 38..64.
  5. ZERO previously verified chunks are retransmitted.
```

---

## 11. Multi-Level Corruption Handling

Corrupted bytes are quarantined immediately and never allowed to propagate:

```
┌──────────────────┬─────────────────────────────┬────────────────────────────────┐
│ Level            │ Corruption Symptom          │ Remediation & Recovery Path    │
├──────────────────┼─────────────────────────────┼────────────────────────────────┤
│ **Leaf Chunk**   │ BLAKE3 hash != expected     │ Drop chunk immediately; slash  │
│                  │ ChunkId in manifest.        │ provider reliability score;    │
│                  │                             │ re-request from alternative.   │
├──────────────────┼─────────────────────────────┼────────────────────────────────┤
│ **Merkle Node**  │ Branch hash mismatch during │ Discard affected chunk range;  │
│                  │ tree reconstruction.        │ re-verify parent manifest.     │
├──────────────────┼─────────────────────────────┼────────────────────────────────┤
│ **Manifest**     │ Manifest signature invalid  │ Drop manifest; re-download from│
│                  │ or hash != ContentId.       │ authoritative publisher node.  │
├──────────────────┼─────────────────────────────┼────────────────────────────────┤
│ **Local Disk**   │ Bit-rot detected on read of │ Invalidate local chunk in DB;  │
│                  │ previously verified chunk.  │ transition object to PARTIAL;  │
│                  │                             │ fetch fresh chunk from mesh.   │
└──────────────────┴─────────────────────────────┴────────────────────────────────┘
```

---

## 12. Provider Disappearance Handling

Tactical mesh nodes drop offline without warning due to terrain, mobility, or power failure:

1. **Detection:** When a chunk request times out ($3 \times \text{RTT}$, default $5.0\text{ s}$), the `TransferManager` marks the chunk as failed for that provider.
2. **State Transition:** The provider's record in `ProviderRegistry` transitions from `Active` to `Degraded`.
3. **Reassignment:** The `ChunkScheduler` reassigns missing chunks to remaining candidate providers.
4. **Content Invariance:** Provider disappearance **never** invalidates the `ContentId` or purges previously verified chunks.
5. **Swarm Exhaustion:** If all providers disappear, the session transitions to `PAUSED` and waits indefinitely for new advertisements or reconnection.

---

## 13. Network Partitions & DTN Convergence

When physical network partitions occur (e.g. Squad Alpha isolated from Base Camp):

```
Squad Alpha (Nodes A, B)                    Base Camp (Nodes C, D)
      [ Partitioned - No Radio Route Between A/B and C/D ]
```

1. **Local Operation:** Nodes within Squad Alpha can resolve, transfer, and replicate any content already present inside Squad Alpha.
2. **DTN Request Staging:** If Node A requests `ContentId_Z` (which exists only at Base Camp), the `CONTENT_QUERY` is queued into SQLite DTN storage (`dtn_bundles`) with `Priority::Normal`.
3. **Physical Courier Transit:** Vehicle Relay E connects to Squad Alpha, copies the DTN query ticket, drives to Base Camp, and delivers the request.
4. **Return Transit:** Vehicle Relay E receives `ContentId_Z`'s manifest and chunks, stores them, drives back to Squad Alpha, and seeds the file.
5. **State Convergence:** The transfer completes without end-to-end electrical connectivity ever existing between Node A and Base Camp.

---

## 14. Provider Advertisement Consistency

Provider advertisements are **eventually consistent** and untrusted:

- **Advertisement TTL:** Advertisements expire after 10 minutes (`DEFAULT_PROVIDER_TTL = 600s`) unless refreshed.
- **Lazy Re-validation:** The system does not waste battery periodically pinging idle providers. A provider's true availability is validated lazily when chunk requests are dispatched.
- **Stale Detection:** If a provider responds with `NotFound` to a chunk it advertised, its registry entry for that `ContentId` is purged immediately.

---

## 15. Unified Security Boundary

FIELDNET enforces strict separation across five security boundaries:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       FIVE UNIFIED SECURITY BOUNDARIES                          │
├────────────────────┬──────────────────┬─────────────────────────────────────────┤
│ Dimension          │ Mechanism        │ Security Guarantee                      │
├────────────────────┼──────────────────┼─────────────────────────────────────────┤
│ 1. Integrity       │ BLAKE3 Multihash │ Guarantees received bytes match CID.    │
├────────────────────┼──────────────────┼─────────────────────────────────────────┤
│ 2. Authenticity    │ Ed25519 Signature│ Proves which sovereign NodeId published.│
├────────────────────┼──────────────────┼─────────────────────────────────────────┤
│ 3. Confidentiality │ ChaCha20-Poly1305│ Plaintext hidden from untrusted relays. │
├────────────────────┼──────────────────┼─────────────────────────────────────────┤
│ 4. Authorization   │ Capability Tokens│ Verifies caller has permission to read. │
├────────────────────┼──────────────────┼─────────────────────────────────────────┤
│ 5. Transport Link  │ Ephemeral Noise  │ Secures physical radio link from MITM.  │
└────────────────────┴──────────────────┴─────────────────────────────────────────┘
```

---

## 16. Encrypted Content Architecture (MVP Standard)

For the FIELDNET MVP, the system standardizes on **Ciphertext Hashing**:

$$\text{ContentId} = \text{BLAKE3}(\text{CiphertextBytes})$$

### 16.1 Architectural Rationale
1. **Zero Relay Knowledge:** Untrusted tactical relay and caching nodes store and forward opaque ciphertext chunks. They cannot identify file types, inspect metadata, or run dictionary attacks against known plaintexts.
2. **Envelope Encryption:** A unique random 256-bit Document Key ($K_{\text{doc}}$) encrypts the file. $K_{\text{doc}}$ is encapsulated into recipient-specific Phase 03 Capability Tokens using X25519 ECDH.
3. **Decryption at Endpoint:** Chunks are verified against the ciphertext `ContentId` upon receipt. Decryption occurs strictly inside the consumer application buffer after Tier 3 verification succeeds.

---

## 17. Application Access Control & Content Permissions

Content authorization operates under an explicit permission calculus:

```rust
pub enum ContentAction {
    Read,       // Retrieve and view bytes
    Write,      // Create new version in Phase 06 metadata
    Delete,     // Issue tombstone deletion
    Replicate,  // Seed copy to other nodes
    Pin,        // Immunize local copy from eviction
    Share,      // Issue capability tokens to other peers
}
```

- **Separation of Authorities:** Possessing a valid node network connection does **not** grant `Read` access to content.
- Every content query and transfer request must present an unexpired capability token signed by the content owner.

---

## 18. Asynchronous Streaming & Range I/O

The streaming interface allows video players, audio streams, and huge database exports to be consumed incrementally:

```rust
#[async_trait]
pub trait ContentReader: AsyncRead + AsyncSeek + Unpin + Send {
    /// Read exact byte slice: offset .. offset + length.
    async fn read_range(&mut self, offset: u64, length: usize) -> Result<Bytes, ContentError>;
    
    /// Stream constituent chunks as they become available.
    fn chunk_stream(&mut self) -> BoxStream<'static, Result<Bytes, ContentError>>;
}
```

### 18.1 Range Access Semantics
If an application requests byte offset `1,048,576 .. 1,572,864` (Chunk 4):
- The reader checks if Chunk 4 is `AVAILABLE` locally.
- If missing, the reader signals the `ChunkScheduler` to fetch Chunk 4 with high priority.
- Missing ranges block asynchronously until Chunk 4 verifies, or return `ContentError::RangeNotAvailable` on timeout. Missing data is **never** padded with dummy zeroes.

---

## 19. Large Content Handling

To safely process 10 GB+ objects without system out-of-memory (OOM) faults:

1. **Chunk Memory Bounding:** Chunk buffers are allocated from a pooled memory arena capped at $16\text{ MB}$ total heap per transfer session.
2. **Direct Kernel Staging:** Received chunks are written directly to `<data_dir>/content/staging/*.part` files via asynchronous `tokio::fs` file descriptors.
3. **Zero Full-Memory Ingestion:** Files are chunked and hashed in $256\text{ KB}$ sliding windows; multi-gigabyte files are never loaded into a contiguous memory buffer.

---

## 20. Content Serving Engine

A node serving content to remote peers executes a 6-step gatekeeper pipeline:

```
Inbound CONTENT_REQUEST
           │
           ▼
[ Gate 1: Requester Identity Check (Blacklist / Trust Tier) ] ── Fail ──► Reject
           │ Pass
           ▼
[ Gate 2: Capability Token Verification (Signature & Scope) ] ── Fail ──► Reject (Unauthorized)
           │ Pass
           ▼
[ Gate 3: Local Content Existence & State == AVAILABLE ] ────── Fail ──► Reject (NotFound)
           │ Pass
           ▼
[ Gate 4: Requested Chunk Possession Check ] ────────────────── Fail ──► Reject (PartialMissing)
           │ Pass
           ▼
[ Gate 5: Resource Limit Check (Battery > 15%, Active Sessions < 16) ] Fail ──► Reject (Busy)
           │ Pass
           ▼
[ Gate 6: Token Bucket Rate Limiter (Bandwidth Quota) ]
           │ Pass
           ▼
[ Stream CONTENT_CHUNK Frames via Socket ]
```

---

## 21. Content Routing vs. Content Storage Decoupling

FIELDNET enforces complete architectural separation between network routing and content storage:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    SEPARATION OF CONCERNS TAXONOMY                              │
├────────────────────┬─────────────────────────┬──────────────────────────────────┤
│ Subsystem Domain   │ Architectural Question  │ Responsible Module               │
├────────────────────┼─────────────────────────┼──────────────────────────────────┤
│ **Routing**        │ How do packets get from │ Phase 05 Routing Engine          │
│                    │ Node A to Node B?       │ (Distance-Vector / Next-Hop)     │
├────────────────────┼─────────────────────────┼──────────────────────────────────┤
│ **Resolution**     │ Which nodes hold copies │ Phase 07 ContentResolver &       │
│                    │ of ContentId X?         │ ProviderRegistry                 │
├────────────────────┼─────────────────────────┼──────────────────────────────────┤
│ **Storage**        │ Where are chunk bytes   │ Phase 07 ContentStore            │
│                    │ stored on local disk?   │ (CAS Disk Fanout & SQLite Index) │
├────────────────────┼─────────────────────────┼──────────────────────────────────┤
│ **Transfer**       │ How do chunks stream    │ Phase 07 TransferManager &       │
│                    │ and resume over links?  │ ChunkScheduler                   │
├────────────────────┼─────────────────────────┼──────────────────────────────────┤
│ **Replication**    │ Where should additional │ Phase 07 ReplicationPolicy       │
│                    │ copies be maintained?   │ Engine                           │
├────────────────────┼─────────────────────────┼──────────────────────────────────┤
│ **Garbage Collect**│ Which local cached bytes│ Phase 07 GarbageCollector        │
│                    │ can safely be purged?   │ (LRU Eviction Engine)            │
└────────────────────┴─────────────────────────┴──────────────────────────────────┘
```

---

## 22. Master Phase 07 Failure Matrix

| Failure Mode | Detection Mechanism | State Transition | Recovery Action | Persistence Impact | App Impact |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Corrupted Chunk** | BLAKE3 checksum mismatch on receipt. | Discard chunk payload. | Slash provider; re-request chunk from alternative peer. | Zero corrupt bytes written to disk. | Transient latency increase; seamless to user. |
| **Missing Manifest** | Resolver query timeout. | Session `REQUESTED` stalls. | Issue periodic background query with backoff. | Zero disk changes. | Returns `ManifestNotFound` after timeout. |
| **Provider Disappears** | Chunk request socket timeout ($3 \times \text{RTT}$). | Provider marked `Degraded`. | Reassign missing chunks to remaining swarm peers. | Staged chunks remain intact in staging. | Download continues from other providers. |
| **Network Partition** | All peer routes lost in routing table. | Session moves to `PAUSED`. | Request queued in DTN storage; auto-resumes on reconnect. | Verified chunk bitfield checkpointed in DB. | Transfer paused; progress bar frozen. |
| **Transport Disconnect**| Socket error / EOF. | Active transfer `PAUSED`. | Listen for Phase 04 reconnect; resume missing chunks. | Checkpoint preserved in SQLite. | Resumes automatically when radio reconnects. |
| **Runtime Crash** | SIGKILL / power loss. | Process terminates. | Step 5 boot scan reloads session; resumes missing work. | SQLite WAL recovers verified bitfield. | Zero duplicate re-download on restart. |
| **Disk Exhaustion** | Write error / quota high watermark. | Ingestion `PAUSED`. | LRU cleans `CACHED` tier; if still full, aborts with error. | Zero pinned files affected. | Returns `StorageExhausted` if cache cannot free. |
| **Capability Revoked** | Token revocation event on Event Bus. | Session `CANCELLED`. | Abort socket read; delete in-flight staging parts. | Staged unverified chunks deleted. | Immediate `Unauthorized` error. |
| **Stale Provider** | Provider returns `NotFound` for chunk. | Provider record pruned. | Evict provider from registry; re-query mesh. | Provider DB row deleted. | Swarm scheduler selects next candidate. |
| **Invalid ContentId** | Multihash decode error. | Immediate reject. | Log invalid format error; drop packet fail-closed. | Zero DB writes. | Immediate `InvalidContentId` error. |
| **Duplicate Chunk** | Bitfield lookup indicates already verified. | Duplicate ignored. | Send `CONTENT_ACK`; discard duplicate bytes. | Zero redundant write. | Zero application impact. |
| **Interrupted Transfer**| Application shutdown. | Session `PAUSED`. | Checkpoint session state; save bitfield to SQLite. | Fully durable on disk. | Resumes on next application start. |
| **Partial Replication** | Node drops offline during background replicate. | Replicate task halted. | Re-queued when peer contact re-established. | Partial chunks stored safely in staging. | Autonomous mesh heals when link returns. |
| **GC Race Condition** | GC attempts to evict active download chunk. | Lock check trips. | GC skips chunk (Invariant G2); active transfer shielded. | Target chunk retained. | Download completes without corruption. |
| **Malicious Flooding** | Requester exceeds 10 req/s. | Token bucket trips. | Inbound requests dropped; peer penalized. | Zero memory allocated. | Malicious peer rate-limited. |
| **Corrupted Index** | SQLite database corruption error. | Panic hook / crash. | SQLite WAL auto-recovery; rebuilt from CAS chunk scan. | Index rebuilt from disk chunks. | Self-healing local recovery. |

---

## 23. Final Rust Crate & Module Hierarchy

Phase 07 will be implemented in a dedicated crate **`crates/fieldnet-content`**:

```
crates/fieldnet-content/
├── Cargo.toml
└── src/
    ├── lib.rs                  <-- Public API, ContentSystem facade, errors
    ├── cid.rs                  <-- ContentId, ChunkId multihash representations
    ├── chunker.rs              <-- Adaptive fixed-size chunker & Chunker trait
    ├── manifest.rs             <-- ContentManifest model & Merkle tree generator
    ├── merkle.rs               <-- Binary Merkle tree implementation & branch proofs
    ├── storage/
    │   ├── mod.rs
    │   ├── cas.rs              <-- Physical filesystem CAS (content/chunks/xx/yy/)
    │   ├── index.rs            <-- SQLite CAS index engine (content.db)
    │   └── quota.rs            <-- Quota accounting, LRU scoring, GC engine
    ├── registry/
    │   ├── mod.rs
    │   ├── registry.rs         <-- ProviderRegistry in-memory & SQLite cache
    │   ├── record.rs           <-- ProviderRecord schema & lifecycle states
    │   └── selector.rs         <-- ProviderSelector & Multi-Factor Utility engine
    ├── transfer/
    │   ├── mod.rs
    │   ├── manager.rs          <-- TransferManager orchestrator
    │   ├── session.rs          <-- TransferSession state machine & persistence
    │   ├── scheduler.rs        <-- ChunkScheduler (Rarest-first, streaming, endgame)
    │   └── bitfield.rs         <-- High-performance ChunkBitfield implementation
    ├── protocol/
    │   ├── mod.rs
    │   ├── messages.rs         <-- ContentMessage wire framing (MessageType::File = 7)
    │   └── handler.rs          <-- Inbound/outbound message dispatchers
    ├── auth.rs                 <-- ContentAuthorizer capability token validation
    ├── stream.rs               <-- Zero-copy async ContentReader & range seeks
    └── events.rs               <-- Structured observability events for Event Bus
```

---

## 24. Final Application API Surface

```rust
#[async_trait]
pub trait ContentSystem: Send + Sync {
    /// Ingests local stream or file into CAS, producing an immutable ContentId.
    async fn put(&self, data: BoxStream<'static, Bytes>, size: u64) -> Result<ContentId, ContentError>;

    /// Checks if complete verified content is available locally.
    async fn has(&self, content_id: &ContentId) -> Result<bool, ContentError>;

    /// Retrieves content metadata, verification status, and size.
    async fn stat(&self, content_id: &ContentId) -> Result<ContentStat, ContentError>;

    /// Deletes local content copy and decrements chunk references.
    async fn delete(&self, content_id: &ContentId) -> Result<(), ContentError>;

    /// Opens an asynchronous zero-copy streaming reader supporting range seeks.
    async fn open_stream(&self, content_id: &ContentId) -> Result<Box<dyn ContentReader>, ContentError>;

    /// Resolves content, checking local CAS first, then initiating mesh discovery.
    async fn resolve(&self, content_id: &ContentId) -> Result<ContentResolution, ContentError>;

    /// Discovers active providers currently possessing content chunks.
    async fn find_providers(&self, content_id: &ContentId, timeout: Duration) -> Result<Vec<ProviderRecord>, ContentError>;

    /// Initiates a multi-source parallel download.
    async fn request(&self, content_id: &ContentId, options: TransferOptions) -> Result<TransferHandle, ContentError>;

    /// Pins content locally, guaranteeing immunity from automated LRU eviction.
    async fn pin(&self, content_id: &ContentId) -> Result<(), ContentError>;

    /// Unpins content, returning it to the evictable CACHED tier.
    async fn unpin(&self, content_id: &ContentId) -> Result<(), ContentError>;

    /// Replicates content to remote mesh peers to meet target factor N.
    async fn replicate(&self, content_id: &ContentId, target_replicas: u8) -> Result<(), ContentError>;

    /// Queries the current progress and telemetry of a transfer session.
    async fn transfer_status(&self, session_id: &SessionId) -> Result<Option<TransferSessionStatus>, ContentError>;

    /// Pauses an active transfer session, retaining verified chunks in staging.
    async fn pause_transfer(&self, session_id: &SessionId) -> Result<(), ContentError>;

    /// Resumes a paused or interrupted transfer session from verified watermarks.
    async fn resume_transfer(&self, session_id: &SessionId) -> Result<TransferHandle, ContentError>;

    /// Cancels a transfer session and unlinks temporary staging files.
    async fn cancel_transfer(&self, session_id: &SessionId) -> Result<(), ContentError>;
}
```

---

## 25. Comprehensive Test Strategy

The verification matrix requires 9 test categories spanning automated unit, property, and multi-node real network tests:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    PHASE 07 VERIFICATION TEST SUITE TAXONOMY                    │
├───────────────────┬─────────────────────────────────────────────────────────────┤
│ Category          │ Explicit Test Scenarios                                     │
├───────────────────┼─────────────────────────────────────────────────────────────┤
│ **Unit**          │ BLAKE3 multihash codec, fixed-size chunking math, bitfields,│
│                   │ Merkle tree leaf & root computations.                       │
├───────────────────┼─────────────────────────────────────────────────────────────┤
│ **Component**     │ SQLite CAS index CRUD, disk directory fanout, quota manager,│
│                   │ provider registry expiration, LRU scoring.                  │
├───────────────────┼─────────────────────────────────────────────────────────────┤
│ **Integration**   │ End-to-end ingest -> advertise -> discover -> request ->    │
│                   │ transfer -> verify -> materialize.                          │
├───────────────────┼─────────────────────────────────────────────────────────────┤
│ **Property**      │ Arbitrary byte stream reconstruction invariant:             │
│                   │ reassemble(chunk(data)) == data across 1,000 runs.          │
├───────────────────┼─────────────────────────────────────────────────────────────┤
│ **Fault Chaos**   │ Bit-flip in chunk, missing chunks, dead providers mid-flow, │
│                   │ disk full during download, forged capability tokens.        │
├───────────────────┼─────────────────────────────────────────────────────────────┤
│ **Recovery**      │ Abrupt process crash during download; verify bitfield and   │
│                   │ resume from watermark without re-downloading verified bytes.│
├───────────────────┼─────────────────────────────────────────────────────────────┤
│ **Security**      │ Unauthorized read attempts, poisoned chunk injections,      │
│                   │ rate-limit flooding, capability token expiration.           │
├───────────────────┼─────────────────────────────────────────────────────────────┤
│ **Performance**   │ 10 GB file chunking throughput, CAS ingestion rate, zero-   │
│                   │ copy streaming seek latency, memory bounding <= 16 MB.      │
├───────────────────┼─────────────────────────────────────────────────────────────┤
│ **Multi-Node E2E**│ Minimum real topology: Node A, Node B, Node C. Node A seeds;│
│                   │ B downloads; C requests from B; B drops; C falls back to A. │
└───────────────────┴─────────────────────────────────────────────────────────────┘
```

---

## 26. Authoritative Phase 07 Architectural Invariants

The following 18 invariants form the binding architectural contract for Phase 07:

1. **Content Sovereignty:** Content identity is strictly determined by cryptographic hash, completely independent of host node location or filesystem path.
2. **Untrusted Advertisements:** Provider discovery advertisements are unverified hints; chunk possession is established only by cryptographically verified bytes.
3. **Demarcated Authorization:** Network transport authentication does not equal content authorization; non-public content requires explicit capability tokens.
4. **Fail-Closed Integrity:** Every accepted chunk must pass BLAKE3 verification before disk CAS commit; corrupt bytes are dropped fail-closed.
5. **Complete Verification:** Complete content requires complete Merkle tree and manifest signature verification before transitioning to `AVAILABLE`.
6. **Availability Shield:** Partial or unverified content in staging cannot masquerade as complete content or be served to remote peers.
7. **Durable Verified State:** Verified chunks survive node restarts and system crashes without loss of progress.
8. **Resumable Transfers:** Interrupted transfers resume cleanly from verified chunk watermarks without restarting from zero.
9. **Provider Invariance:** The failure or disappearance of a provider does not invalidate content identity or corrupt local progress.
10. **Swarm Multi-Sourcing:** Multiple independent providers holding disjoint chunk subsets can serve a single content object concurrently.
11. **Routing / Storage Decoupling:** Network packet routing and distributed content storage remain strictly isolated architectural layers.
12. **Traffic Preemption:** Large bulk file chunks are assigned low priority and must never delay high-priority control signals or emergency DTN queues.
13. **Pinning Immunity:** Automated garbage collection, quota trimming, and low-disk triggers must never evict or delete `PINNED` content.
14. **Active Transfer Shield:** Chunks locked by an active, running transfer session are protected from automated eviction.
15. **Corrupted Content Isolation:** Corrupted content can never become `AVAILABLE` or be served to the mesh.
16. **Decentralized Autonomy:** Content resolution, transfer, and reconstruction operate fully air-gapped without requiring a central server or internet link.
17. **Transport Neutrality:** Content system APIs remain transport-independent, operating cleanly over TCP, UDP, Bluetooth LE, and Wi-Fi Direct.
18. **Application Abstraction Shield:** Applications consume high-level content APIs (`put`, `get`, `stream`) without manipulating chunk schedules, sockets, or DTN internals.

---

## 27. Final Phase 07 Architecture Verification & Alignment

A comprehensive cross-specification audit was performed across:
- `FIELDNET_MASTER_SPEC.md`
- Phases 01–06 architecture and implementation documents
- `PHASE-07-01-DISTRIBUTED-CONTENT-SYSTEM.md`
- `PHASE-07-02-CONTENT-RESOLUTION-AND-TRANSFER.md`
- `PHASE-07-03-CONTENT-LIFECYCLE-REPLICATION-AND-RECOVERY.md`

### Consistency Findings
1. **Identifier Coherence:** `ContentId` multihash format (BLAKE3-256 primary `0x1e`, SHA-256 `0x12`) is uniform across all documents.
2. **State Machine Alignment:** The 8-state content lifecycle (`UNKNOWN` to `EVICTED`) and 11-state transfer state machine (`DISCOVERED` to `COMPLETE`) match identically.
3. **Protocol Framing:** Wire messages utilize Phase 05 `MessageType::File = 7` with priority preemption.
4. **Data Store Demarcation:** Small mutable metadata resides in Phase 06 CRDT Document Store; large immutable payload chunks reside in Phase 07 ContentStore.
5. **Zero Incompatibilities:** No breaking changes or incompatibilities were introduced to Phases 01–06.

---

## 28. Phase 07 Implementation Build Roadmap

The complete Phase 07 architecture is now approved. Implementation will proceed across 7 sequential builds:

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

```
==================================================
PHASE 07 → PROMPT 03 COMPLETE
==================================================
```
The distributed content lifecycle, storage layers, deduplication, replication, recovery, and final verification specification is complete and ready for BUILD-01 implementation.
