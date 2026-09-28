# FIELDNET Phase 07 — Architecture Specification
**Document ID:** `PHASE-07-01-DISTRIBUTED-CONTENT-SYSTEM`  
**Title:** Distributed File & Content System Architecture  
**Status:** SPECIFICATION APPROVED  
**Authoritative Reference:** `FIELDNET_MASTER_SPEC.md`  
**Foundation Subsystems:** Phase 01 (System Model), Phase 02 (Runtime Core), Phase 03 (Security & Identity), Phase 04 (Networking & Discovery), Phase 05 (Protocol, Routing & DTN), Phase 06 (Local-First Data & Synchronization)

---

## 1. Executive Summary & Design Goals

FIELDNET Phase 07 establishes the **Distributed File & Content System**. The core architectural paradigm of Phase 07 is:

> **THE CONTENT SOVEREIGNTY AXIOM:**  
> A file belongs to its **CONTENT IDENTITY**, never to the physical device or host node currently storing it.

In legacy centralized and client-server architectures, files are addressed by host location (`http://server.domain/path/file.ext` or `/mnt/shared/file.ext`). In an austere, contested tactical mesh network where nodes disconnect, move, partition, and power off arbitrarily, location-addressed file storage fails catastrophically.

FIELDNET Phase 07 replaces location addressing with **Content-Addressed Storage (CAS)**, **Multi-Tier Merkle Trees**, **Multi-Source Swarm Transfer**, **Content-Level Capability Authorization**, and **Delay-Tolerant Store-Carry-Forward Retrieval**.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Application / User Layer                              │
└────────────────────────────────────────┬────────────────────────────────────────┘
                                         │ ContentId / Stream API
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                 Phase 07: Distributed Content System Engine                     │
│  ┌─────────────────────────┐  ┌─────────────────────────┐  ┌─────────────────┐  │
│  │   ContentStore & API    │  │   Transfer Manager      │  │  Replication &  │  │
│  │ (put, get, open, stream)│  │ (Multi-source, Resume)  │  │  Storage Quotas │  │
│  └────────────┬────────────┘  └────────────┬────────────┘  └─────────┬───────┘  │
│               │                            │                         │          │
│               ▼                            ▼                         ▼          │
│  ┌─────────────────────────┐  ┌─────────────────────────┐  ┌─────────────────┐  │
│  │    Merkle Tree Engine   │  │    Provider Registry    │  │ Garbage Collect │  │
│  │ (Chunks, Root ContentId)│  │   (Content Discovery)   │  │ (Pinned/Cached) │  │
│  └────────────┬────────────┘  └────────────┬────────────┘  └─────────┬───────┘  │
└───────────────┼────────────────────────────┼─────────────────────────┼──────────┘
                │                            │                         │
    File Meta   ▼                            ▼ Framed CONTENT_*        │ Local Chunk
    Documents ┌───────────────────────────┐  │ Messages                ▼ Storage
    (CRDTs)   │ Phase 06: Data Store      │  │ (MessageType::File = 7) ┌──────────┐
              │ - FileMetadata Documents  │  ▼                         │ Disk CAS │
              │ - Manifest Registries     │ ┌────────────────────────┐ │ Chunks   │
              │ - Dynamic Version Pointers│ │ Phase 05: Routing/DTN  │ │ Tree     │
              └───────────────────────────┘ └────────────────────────┘ └──────────┘
```

### Primary Design Objectives:
1. **Location Independence:** Content is requested purely by its cryptographic `ContentId`. The runtime retrieves chunks from whichever peer or peers currently possess them (Node A, Node B, Node C, or an opportunistic relay).
2. **Deterministic Deduplication:** Storing identical data multiple times—across different files, applications, or directories on the same node—produces the identical cryptographic digest and occupies storage exactly once.
3. **Resilient Interrupted & Resumable Transfers:** Multi-gigabyte transfers interrupted by jamming, movement, or node shutdown resume from the last verified chunk without re-transmitting previous data.
4. **Multi-Source Parallel Retrieval:** Independent chunks of a file are retrieved simultaneously from multiple available peers (BitTorrent/IPFS style) to saturate tactical mesh bandwidth.
5. **Multi-Level Integrity Verification:** Data integrity is verified mathematically across three independent tiers (Leaf Chunk $\to$ Merkle Branch $\to$ Root `ContentId`). Corrupted or poisoned bytes are isolated fail-closed before reaching user space.
6. **Decoupled File Metadata & Content:** File attributes (name, MIME type, owner, creation time, tags, folder hierarchies) are stored as lightweight CRDT documents in Phase 06, referencing immutable Phase 07 `ContentId`s.
7. **Strict Separation of Cryptographic Concerns:** Content integrity hashing is completely decoupled from publisher authenticity (Ed25519 signatures), access authorization (Phase 03 capabilities), and confidentiality (ChaCha20-Poly1305 encryption).
8. **Opportunistic DTN Transport:** Small metadata and manifests traverse delay-tolerant network queues seamlessly; bulk file chunks utilize direct connections, high-bandwidth pipes, or scheduled store-carry-forward.
9. **Zero Internet Requirement:** The system operates autonomously over air-gapped tactical ad-hoc Wi-Fi, Ethernet, and serial links.

---

## 2. Foundational Principles & Conceptual Separation

A critical flaw in legacy decentralized storage systems is conflating content hashing with authenticity, authorization, and confidentiality. FIELDNET strictly decouples these four orthogonal domains:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      FOUR ORTHOGONAL CRYPTOGRAPHIC DOMAINS                       │
├───────────────────┬───────────────────┬───────────────────┬─────────────────────┤
│   CONTENT HASH    │   AUTHENTICITY    │   AUTHORIZATION   │     ENCRYPTION      │
│  (What is this?)  │   (Who made it?)  │ (Who can read it?)│ (Who can decipher?) │
├───────────────────┼───────────────────┼───────────────────┼─────────────────────┤
│ Cryptographic     │ Ed25519 Digital   │ Capability Token  │ ChaCha20-Poly1305   │
│ Digest (BLAKE3)   │ Signature of      │ Grant from        │ Symmetric Cipher    │
│ over content      │ Author NodeId     │ Authorizing Peer  │ with Wrapped Keys   │
│ bytes.            │ over ContentId.   │ for ContentId.    │ for Authorized Caps │
└───────────────────┴───────────────────┴───────────────────┴─────────────────────┘
```

### 2.1 Content Hash $\neq$ Authenticity
- A cryptographic hash (e.g. `BLAKE3(bytes)`) proves **integrity and identity**—that the bytes retrieved match the identifier requested.
- A hash does **not** prove who authored, published, or verified the file. Anyone can hash malicious payload bytes.
- **Authenticity** is established exclusively by an **Ed25519 signature** generated by an authorized `NodeIdentity` over the `ContentId` and its associated `FileMetadata`.

### 2.2 Content Hash $\neq$ Authorization
- Knowing a `ContentId` does **not** confer the right to retrieve, replicate, or inspect it.
- **Authorization** is governed by Phase 03 **Capability Tokens** $\langle P, C, S, R, A, I, E, V \rangle$. A provider node verifies that the requesting peer possesses a valid, unexpired capability granting `Read` access to that `ContentId` before transmitting chunks.

### 2.3 Content Hash $\neq$ Encryption
- Content identity is computed over the stored bytes.
- For confidential files, content is encrypted using ChaCha20-Poly1305. The `ContentId` identifies the **ciphertext** to prevent untrusted storage nodes from deducing plaintext structure or participating in unauthorized deduplication attacks.
- Encryption keys are distributed strictly out-of-band via encrypted capability tokens, never exposed in public content advertisements.

---

## 3. Content Identity Architecture (`ContentId` & `ChunkId`)

### 3.1 Cryptographic Hash Function Selection
FIELDNET standardizes on **BLAKE3-256** as the default cryptographic hash primitive across all content and chunk identifiers:
- **Performance:** Processes data at up to 7 GB/sec on modern SIMD/NEON hardware, outperforming SHA-256 by a factor of $4\times$ to $8\times$.
- **Native Merkle Tree:** BLAKE3 is internally structured as a binary tree hash, allowing instant sub-tree verification and chunk proof generation without external cryptographic overhead.
- **Algorithmic Agility:** To ensure interoperability and future-proofing, the identity structure adopts a self-describing multihash format supporting SHA-256 fallback.

### 3.2 Canonical `ContentId` Wire & Binary Format
A `ContentId` is a self-describing, immutable 34-byte binary structure:

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| Multihash Code| Digest Length |                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               +
|                                                               |
+                   Cryptographic Digest Bytes                  +
|                          (32 Bytes)                           |
+                                                               +
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

```rust
/// Canonical self-describing Content Identifier.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ContentId {
    /// Multihash algorithm identifier:
    /// 0x1e = BLAKE3-256 (Default FIELDNET primary)
    /// 0x12 = SHA2-256   (Compatibility profile)
    pub codec: u8,
    /// Digest length in bytes (standard: 32).
    pub digest_len: u8,
    /// 32-byte cryptographic output.
    pub digest: [u8; 32],
}

/// Identical structure representing a single chunk within a Merkle tree.
pub type ChunkId = ContentId;
```

### 3.3 Textual Representations
To ensure human readability in logs, CLI commands, and URLs without encoding ambiguity:
1. **Hexadecimal String:** `fn-cid-1e20<64-hex-chars>` (for internal debugging and tracing).
2. **Base32 String:** RFC 4648 Base32 lowercase without padding (e.g. `bafk...`), identical to IPFS CIDv1 conventions for seamless developer ergonomics.

---

## 4. Content-Addressed Storage Subsystem (`ContentStore`)

### 4.1 On-Disk Directory Layout
Chunks are stored on the local filesystem decoupled from document databases. To prevent filesystem inode exhaustion in flat directories, chunks are partitioned across a two-tier hex directory fanout:

```
<data_dir>/content/
├── chunks/
│   ├── 3f/
│   │   └── 9a/
│   │       ├── 3f9a88c2...01.chunk     <-- Raw chunk payload bytes
│   │       └── 3f9a910d...ff.chunk
│   └── c1/
│       └── 4b/
│           └── c14b3012...88.chunk
├── staging/                            <-- Incomplete, unverified transfers
│   └── tx-session-9842.part
├── manifests/                          <-- Serialized Merkle manifests
│   └── 7b/
│       └── 7b21e899...aa.manifest
└── content.db                          <-- SQLite CAS index and tracking
```

### 4.2 SQLite CAS Index Schema (`content.db`)
While chunk payloads live directly on disk for zero-copy kernel streaming, all relational state, lifecycle status, pinning flags, and access statistics are managed via SQLite in WAL mode:

```sql
-- Tracked root content objects
CREATE TABLE IF NOT EXISTS content_objects (
    content_id          BLOB PRIMARY KEY,   -- 34-byte canonical CID
    manifest_id         BLOB NOT NULL,      -- Points to Merkle manifest
    total_size          INTEGER NOT NULL,   -- Complete size in bytes
    chunk_count         INTEGER NOT NULL,   -- Total number of chunks
    crdt_type           TEXT NOT NULL,      -- "file", "bundle", "blob"
    retention_tier      INTEGER NOT NULL,   -- 0=Temp, 1=Cached, 2=Replicated, 3=Pinned
    lifecycle_state     INTEGER NOT NULL,   -- State machine (0..7)
    created_at          INTEGER NOT NULL,   -- Milliseconds timestamp
    last_accessed_at    INTEGER NOT NULL,   -- For LRU eviction
    access_count        INTEGER NOT NULL DEFAULT 0
);

-- Individual physical chunks stored on local disk
CREATE TABLE IF NOT EXISTS local_chunks (
    chunk_id            BLOB PRIMARY KEY,   -- 34-byte ChunkId
    chunk_size          INTEGER NOT NULL,   -- Size in bytes (<= 1MB)
    storage_path        TEXT NOT NULL,      -- Relative path in chunks/
    reference_count     INTEGER NOT NULL,   -- Deduplication ref count
    is_verified         INTEGER NOT NULL,   -- 1 = Integrity validated
    created_at          INTEGER NOT NULL
);

-- Mapping of content manifests to constituent chunks
CREATE TABLE IF NOT EXISTS manifest_chunks (
    content_id          BLOB NOT NULL,
    chunk_index         INTEGER NOT NULL,   -- 0-indexed position
    chunk_id            BLOB NOT NULL,
    offset_bytes        INTEGER NOT NULL,
    PRIMARY KEY (content_id, chunk_index),
    FOREIGN KEY (chunk_id) REFERENCES local_chunks(chunk_id)
);

-- Active transfer sessions
CREATE TABLE IF NOT EXISTS transfer_sessions (
    session_id          BLOB PRIMARY KEY,   -- 16-byte UUID
    content_id          BLOB NOT NULL,
    total_chunks        INTEGER NOT NULL,
    chunks_completed    INTEGER NOT NULL,
    bytes_transferred   INTEGER NOT NULL,
    status              INTEGER NOT NULL,   -- Transfer state
    started_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    resumed_count       INTEGER NOT NULL DEFAULT 0
);

-- Provider discovery cache
CREATE TABLE IF NOT EXISTS content_providers (
    content_id          BLOB NOT NULL,
    provider_node_id    BLOB NOT NULL,
    has_full_content    INTEGER NOT NULL,   -- 1 = Complete, 0 = Partial
    available_chunks    BLOB,               -- Optional compressed bitset
    last_seen_at        INTEGER NOT NULL,
    latency_ms          INTEGER NOT NULL,
    trust_score         INTEGER NOT NULL,
    expires_at          INTEGER NOT NULL,
    PRIMARY KEY (content_id, provider_node_id)
);
```

### 4.3 Deduplication Semantics
- When a chunk is ingested via `put_chunk()`, the system computes `ChunkId = BLAKE3(payload)`.
- If `local_chunks` already contains `ChunkId`, the physical file is not rewritten; the SQLite `reference_count` is atomically incremented by $1$.
- When a file or manifest is deleted, `reference_count` decrements by $1$. The physical file on disk is deleted if and only if `reference_count == 0` and the chunk is unreferenced by any pinned manifest.

---

## 5. File Metadata vs. Content Separation

A critical design requirement is that **content bytes and file metadata reside in separate architectural domains**:

```
┌────────────────────────────────────────────────────────┐
│     Phase 06 Document Store (CRDT Metadata Record)     │
│  - DocumentId: BLAKE3("file-meta" || author || uuid)   │
│  - DocumentType: "file_metadata"                       │
│  - Payload:                                            │
│      filename: "operation_plan_bravo.pdf"              │
│      mime_type: "application/pdf"                      │
│      size: 48,291,040                                  │
│      content_id: fn-cid-1e20a4b8...                    │
│      creator_node: NodeId_A                            │
│      tags: ["tactical", "bravo", "air-support"]        │
│      version: 3 (LWW-Register or CrdtMap)              │
│      created_at: 1790520000000                         │
└───────────────────────────┬────────────────────────────┘
                            │ References ContentId
                            ▼
┌────────────────────────────────────────────────────────┐
│     Phase 07 Content System (Immutable Payload)        │
│  - ContentId: fn-cid-1e20a4b8...                       │
│  - Manifest: 184 chunks (256 KB each)                  │
│  - Raw Chunks: Stored in <data_dir>/content/chunks/    │
│  - Transferred over wire via multi-source retrieval    │
└────────────────────────────────────────────────────────┘
```

### 5.1 Rationale & Guarantees
1. **Zero-Byte File Renames:** If a user renames `report_v1.pdf` to `final_signed_report.pdf`, only a 120-byte CRDT update is distributed via Phase 06 sync. Not a single byte of file content is re-transmitted.
2. **Multi-File Aliasing:** Two independent departments can possess files with different names and folder paths referencing the exact same underlying content ID. The physical content is stored and transferred once.
3. **Directory Structures as CRDT Maps:** Folders and hierarchical workspaces are modeled as `CrdtMap` structures where keys are file paths and values are `ContentId` references.
4. **Path Sanitization Shield:** Metadata filenames are strictly untrusted strings. The runtime never writes content bytes directly to arbitrary OS filesystem paths specified by remote peers. Ingestion occurs strictly under the managed `<data_dir>/content/` hierarchy.

---

## 6. Chunking Architecture & Evaluation

Large payloads must be segmented into discrete chunks for transport framing, verification, and resumability.

### 6.1 Evaluation: Fixed-Size vs. Content-Defined Chunking

| Evaluation Metric | Fixed-Size Chunking (Chosen for Phase 07 MVP) | Content-Defined Chunking (FastCDC / Rabin) |
| :--- | :--- | :--- |
| **Complexity** | Extremely simple, deterministic, $O(1)$ computation. | Complex sliding-window hash; variable chunk boundary math. |
| **CPU Overhead** | Low; single sequential pass directly into BLAKE3. | High; rolling hash computed over every byte offset. |
| **Index Predictability**| Direct byte-offset mapping: $\text{chunk\_idx} = \lfloor\text{offset} / S\rfloor$. | Requires sequential traversal of previous chunk offsets. |
| **Deduplication Power** | Excellent for static media, videos, archives, disk images. | Superior for mutated source code and word documents. |
| **Wire Protocol Framing**| Predictable message sizes; fits static network MTUs. | Variable message sizes requires dynamic MTU packing. |

### 6.2 Chosen Initial Strategy: Adaptive Fixed-Size Chunking
FIELDNET Phase 07 adopts **Adaptive Fixed-Size Chunking**:
- **Standard Tactical Chunk Size:** **256 KB** ($262,144$ bytes). Fits comfortably within Phase 05 `MessageType::File` message bounds ($\le 1\text{ MB}$), maximizes opportunistic transmission over lossy radio links, and limits corrupted retransmissions to 256 KB slices.
- **Small Files ($\le 256\text{ KB}$):** Single chunk. The `ContentId` directly equals the chunk's `ChunkId` without an intermediate multi-level Merkle tree.
- **Large Files ($> 256\text{ KB}$):** Partitioned into uniform 256 KB chunks, with the final chunk containing the remaining tail bytes ($1 \le \text{tail} \le 256\text{ KB}$).
- **Extensibility Interface:** Chunking is decoupled behind `trait Chunker`. The manifest explicitly encodes the chunking algorithm used (`0x01` = Fixed-256KB, `0x02` = Fixed-1MB, `0x03` = FastCDC-v1). Upgrading to Content-Defined Chunking in future releases requires zero changes to the application API.

---

## 7. Content Manifest Specification (`ContentManifest`)

A multi-chunk content object is defined by an immutable, deterministic `ContentManifest`. The manifest encodes the complete topological recipe needed to verify, reconstruct, and seek within the content.

### 7.1 Manifest Schema
```rust
/// Complete Merkle manifest for a distributed content object.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentManifest {
    /// Format schema version (currently 1).
    pub schema_version: u32,
    /// Root ContentId (BLAKE3 hash over the canonical serialized manifest).
    pub content_id: ContentId,
    /// Total logical byte size of reconstructed content.
    pub total_size: u64,
    /// Chunking strategy code (0x01 = Fixed 256 KB).
    pub chunk_codec: u8,
    /// Standard nominal chunk size (e.g. 262,144).
    pub nominal_chunk_size: u32,
    /// Total number of constituent chunks.
    pub chunk_count: u32,
    /// Sequential array of chunk cryptographic descriptors.
    pub chunks: Vec<ChunkDescriptor>,
    /// Balanced Merkle tree root hash over all chunk digests.
    pub merkle_root: [u8; 32],
    /// Original publisher node identity.
    pub publisher: NodeId,
    /// Publisher Ed25519 signature over (content_id || merkle_root || total_size).
    pub signature: [u8; 64],
    /// Physical creation timestamp.
    pub created_at: Timestamp,
}

/// Metadata descriptor for a single constituent chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkDescriptor {
    /// Zero-based sequential chunk index.
    pub index: u32,
    /// Absolute byte offset within logical content.
    pub offset: u64,
    /// Length of this chunk in bytes.
    pub size: u32,
    /// Cryptographic identifier of chunk bytes: BLAKE3(chunk_payload).
    pub chunk_id: ChunkId,
}
```

---

## 8. Merkle Tree & DAG Architecture

To facilitate sub-tree verification, partial retrieval, and parallel multi-source validation, Phase 07 organizes multi-chunk manifests into a binary Merkle tree:

```
                          Root ContentId
                  [ BLAKE3(Manifest Header + Root) ]
                                 │
                         Merkle Root (Node 0)
                      [ Hash(Node 1 || Node 2) ]
                                 │
                ┌────────────────┴────────────────┐
                ▼                                 ▼
         Intermediate Node 1               Intermediate Node 2
      [ Hash(Leaf A || Leaf B) ]        [ Hash(Leaf C || Leaf D) ]
                │                                 │
        ┌───────┴───────┐                 ┌───────┴───────┐
        ▼               ▼                 ▼               ▼
     Chunk 0         Chunk 1           Chunk 2         Chunk 3
     (Leaf A)        (Leaf B)          (Leaf C)        (Leaf D)
    [ 256 KB ]      [ 256 KB ]        [ 256 KB ]      [ 142 KB ]
```

### 8.1 Merkle Properties & Verification Benefits
1. **Incremental Validation:** When a chunk arrives from a remote peer, the receiver does not need to wait for the entire 10 GB file to finish downloading. It verifies `BLAKE3(chunk_bytes) == chunk_descriptor.chunk_id` and checks the Merkle branch path to the root. Invalid chunks are discarded instantly at the link layer.
2. **Subtree Verification:** Large collections of chunks can be validated in batches without scanning untouched sections of disk.
3. **Random-Access Seeks:** A video player streaming 5 minutes into a 2-hour file computes the required byte offset, determines the target chunk index ($\lfloor \text{offset} / 256\text{ KB} \rfloor$), requests only that specific chunk from the mesh, verifies it against the manifest Merkle branch, and plays immediately.

---

## 9. Content Lifecycle & Invariant State Machine

Every content object on a node transitions through an explicit, auditable lifecycle:

```
                   ┌──────────────┐
                   │   UNKNOWN    │
                   └──────┬───────┘
                          │ Content Discovered via Mesh
                          ▼
                   ┌──────────────┐
                   │  DISCOVERED  │
                   └──────┬───────┘
                          │ Transfer Requested by App/Policy
                          ▼
                   ┌──────────────┐
                   │  REQUESTED   │
                   └──────┬───────┘
                          │ First Chunk Staged
                          ▼
                   ┌──────────────┐
       ┌───────────┤   PARTIAL    │◄──────────┐
       │           └──────┬───────┘           │ Chunks Resumed
       │ Error/           │ All Chunks Staged │
       │ Disconnect       ▼                   │
       │           ┌──────────────┐           │
       │           │   COMPLETE   │           │
       │           └──────┬───────┘           │
       │                  │ Merkle Integrity Validated
       │                  ▼                   │
       │           ┌──────────────┐           │
       │           │   VERIFIED   │           │
       │           └──────┬───────┘           │
       │                  │ Committed to ContentStore
       │                  ▼                   │
       │           ┌──────────────┐           │
       └──────────►│  AVAILABLE   │───────────┘
                   └──────┬───────┘
                          │ Eviction / Manual Delete
                          ▼
                   ┌──────────────┐
                   │   EVICTED    │
                   └──────────────┘
```

### 9.1 Lifecycle States Defined
- **`UNKNOWN`:** Content is completely unknown to local node.
- **`DISCOVERED`:** Content identity and provider addresses are known via mesh advertisement, but no chunks or manifests are locally stored.
- **`REQUESTED`:** Application has initiated a retrieval session; negotiation with providers is underway.
- **`PARTIAL`:** Manifest and at least one verified chunk exist locally; transfer is in-flight or suspended.
- **`COMPLETE`:** All raw chunks have been physically received into staging.
- **`VERIFIED`:** All chunks, manifest signatures, and Merkle tree roots have passed cryptographic validation.
- **`AVAILABLE`:** Reconstructed content is committed, accessible via read APIs, and can be served to other peers.
- **`EVICTED`:** Content bytes have been purged under storage pressure. Metadata and provider locations may be retained.

### 9.2 Core State Invariants
- **INVARIANT C1:** A partially downloaded content object must **NEVER** be reported as `AVAILABLE` or returned by `get()`.
- **INVARIANT C2:** An object with a single failed chunk hash must **NEVER** transition to `VERIFIED`. It must transition to `CORRUPTED` or return to `PARTIAL` for chunk re-request.
- **INVARIANT C3:** Content marked `PINNED` must **NEVER** transition to `EVICTED` under automated storage pressure.

---

## 10. Multi-Level Integrity Verification Engine

Integrity verification operates at three distinct defensive tiers:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     THREE-TIER INTEGRITY VERIFICATION PIPELINE                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Tier 1: Leaf Chunk Verification (Synchronous on Ingestion)                      │
│ - Condition: BLAKE3(received_chunk_bytes) == expected_chunk_id                 │
│ - Failure: Chunk dropped immediately; sender penalized; chunk re-requested.    │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Tier 2: Merkle Branch & Manifest Verification (Assembly Phase)                  │
│ - Condition: MerkleTree::compute(chunks) == manifest.merkle_root               │
│ - Condition: Ed25519::verify(publisher, manifest_bytes, signature) == OK       │
│ - Failure: Entire transfer session paused; manifest quarantined.                │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Tier 3: Root ContentId Cryptographic Seal (Commit Phase)                        │
│ - Condition: BLAKE3(canonical_manifest) == manifest.content_id                 │
│ - Condition: Total bytes assembled on disk == manifest.total_size              │
│ - Failure: State machine aborts; zero bytes committed to ContentStore.          │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 11. Resumable Transfers Architecture

In a tactical mobile mesh, connections drop frequently due to distance, terrain, and electronic warfare. Resuming transfers from zero is catastrophic for multi-megabyte files.

### 11.1 Persistent Transfer Session (`TransferSession`)
When a transfer begins, a durable `TransferSession` is persisted in SQLite:
- `session_id`: Unique 16-byte session identifier.
- `content_id`: Target `ContentId`.
- `manifest`: Cached `ContentManifest`.
- `chunk_bitfield`: Dynamic bitmap where bit $i = 1$ indicates chunk $i$ has been received, verified, and durably written to staging.
- `providers`: Active peer pool and their respective chunk possession masks.
- `last_active_at`: Monotonic physical timestamp.

### 11.2 Interruption and Resumption Flow
1. **Disconnection Event:** Node B drops offline while downloading chunk 42 of 100.
2. **State Freeze:** The session transitions to `PAUSED`. Chunks 0 through 41 remain validated on disk in `<data_dir>/content/staging/`.
3. **Re-connection Event:** Node B establishes contact with Node A (or a new peer Node C).
4. **Resumption Handshake:** Node B sends `CONTENT_REQUEST` containing `content_id` and its current `have_bitfield`.
5. **Incremental Delivery:** The provider starts streaming strictly from chunk 42 onward.
6. **Zero Duplicate Bandwidth:** Chunks 0 through 41 are never retransmitted.

---

## 12. Multi-Source Parallel Retrieval Engine

FIELDNET does not mandate that a single peer possess the entire file. Different peers may hold disjoint subsets of chunks:

```
            Node A (Possesses Chunks 0..49)
                  ▲
     Chunk Req 0..4 │ Chunk Stream 0..4
                  ▼
            Node C (Requesting Replicant)
                  ▲
    Chunk Req 50..54│ Chunk Stream 50..54
                  ▼
            Node B (Possesses Chunks 50..99)
```

### 12.1 Swarm Retrieval Algorithm
1. **Discovery:** Node C queries the mesh for `ContentId_X`.
2. **Provider Registration:** Node A responds with bitfield `[0..49]`; Node B responds with bitfield `[50..99]`.
3. **Dynamic Scheduling:**
   - **Rarest-First Selection:** For bulk background downloads, Node C requests chunks with the lowest availability count in the swarm first.
   - **Sequential Selection:** For real-time streaming, Node C prioritizes chunks needed for immediate playback buffer windows.
4. **Adaptive Bandwidth Allocation:** Node C pipelines multiple in-flight chunk requests across Node A and Node B based on observed round-trip latency (RTT) and socket throughput.
5. **Failover:** If Node A drops offline mid-transfer, its uncompleted chunk assignments are instantly reassigned to other nodes advertising those chunks.

---

## 13. Content Discovery & Provider Registry

Content discovery answers the question: *"Who currently possesses content X or its constituent chunks?"*

### 13.1 Decoupling Discovery from Transfer
- Discovery messages are small, lightweight announcements or queries routed across the mesh or cached in DTN queues.
- Data transfers occur point-to-point over dedicated, rate-controlled transport sessions.

### 13.2 Provider Registry Model
Every node maintains an in-memory and SQLite-backed `ProviderRegistry`:

```rust
pub struct ContentProviderRecord {
    pub content_id: ContentId,
    pub provider_node: NodeId,
    pub has_full_content: bool,
    pub chunk_mask: Option<BitVec>,
    pub last_seen: Timestamp,
    pub latency_ms: u32,
    pub trust_tier: TrustTier,
    pub expires_at: Timestamp,
}
```

### 13.3 Untrusted Claim Principle
- A peer claiming to hold `ContentId_X` is **untrusted**.
- The runtime verifies provider possession only when chunks actually arrive and match cryptographic hash digests.
- Peers that advertise content but fail to deliver valid chunks are penalized via local peer reputation scoring.

---

## 14. Replication Models & Retention Semantics

FIELDNET distinguishes between four distinct storage retention tiers:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           STORAGE RETENTION TIERS                               │
├───────────────┬───────────────────────────┬─────────────────────────────────────┤
│ Retention Tier│ Eviction Behavior         │ Intended Use Case                   │
├───────────────┼───────────────────────────┼─────────────────────────────────────┤
│ **`PINNED`**  │ **IMMUNE to Eviction.**   │ Mission-critical operational maps,  │
│               │ Deleted only by user.     │ cryptographic keys, orders.         │
├───────────────┼───────────────────────────┼─────────────────────────────────────┤
│`REPLICATED`   │ Protected; evicted only   │ Mesh-wide shared documents, policy  │
│               │ under severe emergency.   │ manuals, team shared assets.        │
├───────────────┼───────────────────────────┼─────────────────────────────────────┤
│ **`CACHED`**  │ **EVICTABLE via LRU.**    │ Recently viewed files, transient    │
│               │ Purged when disk is full. │ attachments, secondary media.       │
├───────────────┼───────────────────────────┼─────────────────────────────────────┤
│`TEMPORARY`    │ Short TTL (hours).        │ Incomplete downloads, staging parts,│
│               │ Purged aggressively.      │ preview thumbnails.                 │
└───────────────┴───────────────────────────┴─────────────────────────────────────┘
```

---

## 15. Replication Policy Framework

Replication in FIELDNET is autonomous and decentralized, controlled by configurable policy metrics:

1. **Autonomous Mesh Factor ($R$):** Documents can declare a target replication factor (e.g. $R = 3$). Connected mesh nodes collaborate to maintain at least 3 distinct copies across different devices.
2. **Battery & Power Awareness:** Nodes operating below 20% battery automatically refuse background replication tasks while continuing to serve direct user requests.
3. **Transport Optimization:** Replication tasks defer bulk synchronization until high-bandwidth, low-power links (Wi-Fi or Ethernet) become available, avoiding saturated tactical VHF/UHF radio frequencies.

---

## 16. Content Request Protocol (`MessageType::File = 7`)

The Content System communicates by framing structured messages inside FIELDNET's Phase 05 `Message` envelope using dedicated `MessageType::File` (value `7`):

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│               Phase 05 Protocol Message Envelope (MessageType = 7)              │
├─────────────────────────────────────────────────────────────────────────────────┤
│ version=1 | message_id=... | source=NodeA | destination=NodeB | ttl=16 | seq=... │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Payload: Serialized ContentMessage (Protobuf / CBOR / JSON)                     │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 16.1 Content Protocol Message Types
1. **`CONTENT_QUERY`:** Broadcast or unicast request: *"Who possesses ContentId X?"*
2. **`CONTENT_ADVERTISE`:** Response or periodic broadcast: *"I possess ContentId X (chunks: all / bitfield)."*
3. **`CONTENT_REQUEST`:** Targeted chunk demand: *"Send me chunks [12, 13, 14] of ContentId X (Session ID: S)."*
4. **`CONTENT_RESPONSE`:** Negotiation acknowledgment: *"Accepted. Sending 3 chunks totaling 786 KB."*
5. **`CONTENT_CHUNK`:** Direct chunk payload: `[session_id, chunk_index, chunk_id, bytes]`.
6. **`CONTENT_ACK`:** Chunk reception receipt: `[session_id, chunk_index, verified=true]`.
7. **`CONTENT_CANCEL`:** Terminate transfer session immediately.
8. **`CONTENT_COMPLETE`:** Handshake confirming complete content verification.
9. **`CONTENT_ERROR`:** Explicit error notification (`NotFound`, `Unauthorized`, `QuotaExceeded`, `Corrupted`).

---

## 17. Transfer Engine State Machine

The client-side `TransferManager` drives each download through an 11-state deterministic state machine:

```
┌─────────────┐     Start
│ DISCOVERED  ├───────────────┐
└─────────────┘               ▼
                      ┌───────────────┐
                      │   REQUESTED   │
                      └───────┬───────┘
                              │ Peer Responds
                              ▼
                      ┌───────────────┐
                      │  NEGOTIATING  │
                      └───────┬───────┘
                              │ Session Established
                              ▼
                      ┌───────────────┐  Network Loss   ┌───────────────┐
                      │ TRANSFERRING  ├────────────────►│    PAUSED     │
                      └───────┬───────┘                 └───────┬───────┘
                              │                                 │ Reconnect
                              │ All Chunks Received             ▼
                              │                         ┌───────────────┐
                              │                         │   RESUMING    │
                              │                         └───────┬───────┘
                              │                                 │
                              ▼◄────────────────────────────────┘
                      ┌───────────────┐
                      │   VERIFYING   │
                      └───────┬───────┘
                              │ Pass Merkle Check
                              ▼
                      ┌───────────────┐
                      │   COMPLETE    │
                      └───────────────┘

Terminal Failure States:
[ FAILED ]  - Permanent peer refusal or unresolvable error.
[ CANCELLED ] - Aborted by user or calling application.
[ EXPIRED ] - Session TTL elapsed without forward progress.
[ CORRUPTED ] - Received payload failed cryptographic check.
```

---

## 18. DTN & Tactical Mesh Integration

Bulk content transfers require distinct handling in delay-tolerant networks to prevent clogging low-bandwidth store-and-forward queues.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                       DTN TRANSFER DEMARCATION POLICY                           │
├───────────────────────────────────────┬─────────────────────────────────────────┤
│ SMALL METADATA & CONTROL (<= 64 KB)   │ BULK CONTENT CHUNKS (> 64 KB)           │
├───────────────────────────────────────┼─────────────────────────────────────────┤
│ - Content Advertisements              │ - Stored locally in ContentStore        │
│ - Manifests & Availability Bitfields  │ - Transferred via direct TCP/P2P links  │
│ - Capability Authorization Tokens     │ - Store-Carry-Forward only via          │
│ - Content Request Tickets             │   designated Physical Data Couriers     │
│ ───────────────────────────────────── │ ─────────────────────────────────────── │
│ Routed through Phase 05 SQLite DTN    │ Bypasses standard DTN message queue;    │
│ Queue (`dtn_bundles`) with high/normal│ transferred via opportunistic high-rate │
│ priority.                             │ bursts when peer contact is made.       │
└───────────────────────────────────────┴─────────────────────────────────────────┘
```

---

## 19. File Versioning & Immutability Model

FIELDNET enforces an **Absolute Immutability Axiom**:

$$\text{Any modification to content bytes } B \to B' \implies \text{Produces a new } \text{ContentId}'$$

### 19.1 Versioning via Phase 06 Document Pointers
Mutable file versioning is achieved cleanly by coupling Phase 07 immutable `ContentId`s with Phase 06 CRDT Document Records:

```
File Document: "op_briefing.docx" (DocumentId: doc_9812)
├── Version 1: ContentId = fn-cid-1e201111... (Created 08:00 by Node A)
├── Version 2: ContentId = fn-cid-1e202222... (Updated 09:30 by Node B)
└── Version 3: ContentId = fn-cid-1e203333... (Current LWW-Register head)
```

- When Node B edits the document, it publishes the new content object to Phase 07, obtaining `ContentId_3333`.
- Node B then commits a lightweight CRDT update to `doc_9812` in Phase 06: `head_content_id = ContentId_3333`.
- All replicas receive the version pointer update instantly. Nodes that require the actual full document download `ContentId_3333` on-demand from Node B.

---

## 20. Encryption & Confidentiality Framework

To preserve confidentiality in untrusted tactical meshes, sensitive files are encrypted prior to content addressing.

### 20.1 Plaintext vs. Ciphertext Hashing
- **Plaintext Hashing:** `CID = Hash(Plaintext)`. Enables global deduplication across users, but leaks content identity to untrusted storage nodes and facilitates dictionary confirmation attacks.
- **Ciphertext Hashing (Chosen Standard):** `CID = Hash(Ciphertext)`. Untrusted relay and caching nodes store and transfer opaque encrypted chunks. They cannot inspect file contents, determine file types, or identify plaintext documents.

### 20.2 Convergent & Envelope Encryption Scheme
1. **Symmetric Payload Encryption:** File content is encrypted using **ChaCha20-Poly1305** with a randomly generated 256-bit Document Key ($K_{\text{doc}}$).
2. **Key Encapsulation:** $K_{\text{doc}}$ is encapsulated into Phase 03 Capability Tokens encrypted individually for authorized recipient `NodeId`s using **X25519** ECDH key agreement.
3. **Zero Key Leakage:** Encryption keys are never placed in content manifests or mesh advertisements.

---

## 21. Content Access Control & Capability Authorization

Possessing a `ContentId` does not grant permission to download it.

### 21.1 Capability Enforcement Flow
1. **Request:** Node B sends `CONTENT_REQUEST` for `ContentId_X`, attaching a cryptographic **Capability Token** issued by the file owner.
2. **Verification:** The provider node verifies:
   $$\text{Verify}(\text{IssuerPublicKey}, \text{TokenBytes}, \text{TokenSignature}) == \text{Valid}$$
   $$\text{Token}.\text{subject} == \text{NodeB} \quad \land \quad \text{Token}.\text{resource} == \text{ContentId\_X} \quad \land \quad \text{Token}.\text{action} == \text{"read"}$$
   $$\text{Timestamp::now}() \le \text{Token}.\text{expires\_at}$$
3. **Fail-Closed Execution:** If the capability is missing, forged, or expired, the provider returns `CONTENT_ERROR { code: Unauthorized }` and drops the transfer session.

---

## 22. Storage Quotas, Limits & Eviction Engine

Every node maintains hard constraints on disk utilization:

```rust
pub struct StorageQuotaConfig {
    /// Maximum disk space allocated to ContentStore (e.g. 50 GB).
    pub max_storage_bytes: u64,
    /// High watermark threshold to trigger eviction (default: 90%).
    pub high_watermark_ratio: f32,
    /// Low watermark threshold target for eviction (default: 75%).
    pub low_watermark_ratio: f32,
    /// Maximum allowed size for a single content object (e.g. 10 GB).
    pub max_single_content_bytes: u64,
}
```

### 22.1 Eviction Policy
When disk usage reaches `high_watermark_ratio`:
1. The engine scans the `content_objects` database.
2. **Immunity Filter:** All objects marked `PINNED` or actively locked by an in-flight transfer session are completely excluded.
3. **Scoring Function:** Remaining `CACHED` and `TEMPORARY` objects are ordered by a composite eviction score:
   $$\text{Score} = \text{last\_accessed\_timestamp} + \big(\text{access\_count} \times 3600\big) - \big(\text{size\_bytes} / 1024\big)$$
4. Lowest scoring objects have their chunks unlinked until disk usage falls below `low_watermark_ratio`.

---

## 23. Garbage Collection Engine

The `GarbageCollector` runs asynchronously to reclaim orphaned chunks:

1. **Mark Phase:**
   - Scan all `content_objects` in state `AVAILABLE` or `PARTIAL`.
   - Collect all `ChunkId` references from their active manifests.
   - Collect all chunks locked by active `TransferSession`s.
2. **Sweep Phase:**
   - Query `local_chunks` for records where `reference_count == 0` or not in the marked set.
   - Delete physical files from `<data_dir>/content/chunks/`.
   - Remove rows from `local_chunks` table.

---

## 24. Streaming & Range I/O API

Applications consume multi-gigabyte files without loading entire payloads into system RAM:

```rust
/// Asynchronous streaming reader supporting zero-copy and range seeks.
pub trait ContentReader: AsyncRead + AsyncSeek + Unpin + Send {
    /// Total logical size of content.
    fn total_size(&self) -> u64;
    /// Read exact byte range asynchronously.
    async fn read_range(&mut self, offset: u64, length: usize) -> Result<Bytes, ContentError>;
    /// Stream constituent chunks sequentially.
    fn chunk_stream(&mut self) -> BoxStream<'static, Result<Bytes, ContentError>>;
}
```

---

## 25. Application Programming Interface Surface

The primary public entry point exposed to runtime modules and applications:

```rust
#[async_trait]
pub trait ContentSystem: Send + Sync {
    /// Ingests local bytes or stream, producing an immutable ContentId.
    async fn put(&self, data: BoxStream<'static, Bytes>, size: u64) -> Result<ContentId, ContentError>;
    
    /// Checks if content is available locally.
    async fn has(&self, content_id: &ContentId) -> Result<bool, ContentError>;
    
    /// Retrieves content metadata, verification status, and size.
    async fn stat(&self, content_id: &ContentId) -> Result<ContentStat, ContentError>;
    
    /// Opens an asynchronous streaming reader over the content.
    async fn open_stream(&self, content_id: &ContentId) -> Result<Box<dyn ContentReader>, ContentError>;
    
    /// Initiates a multi-source network transfer for remote content.
    async fn request(&self, content_id: &ContentId, options: TransferOptions) -> Result<TransferHandle, ContentError>;
    
    /// Pins content to prevent local automated eviction.
    async fn pin(&self, content_id: &ContentId) -> Result<(), ContentError>;
    
    /// Unpins content, returning it to evictable CACHED tier.
    async fn unpin(&self, content_id: &ContentId) -> Result<(), ContentError>;
    
    /// Deletes local content copy and decrements chunk references.
    async fn delete(&self, content_id: &ContentId) -> Result<(), ContentError>;
}
```

---

## 26. Security Architecture & Threat Model

| Threat / Attack Vector | Severity | Mitigation Architecture |
| :--- | :--- | :--- |
| **Chunk Poisoning** (Attacker injects corrupt bytes) | Critical | **Fail-closed Leaf Verification:** `BLAKE3(bytes)` must match `ChunkId`. Mismatched chunks are dropped immediately; attacker is banned. |
| **Fake Provider Sybil Attack** (Claims to hold all files) | High | **Lazy Verification & Slashing:** Providers are scored on actual verified chunk yield. Repeated non-delivery drops peer trust to zero. |
| **Storage Exhaustion DoS** (Floods node with junk files) | High | **Capability-Gated Ingestion & Hard Quotas:** Transfers require valid capability tokens; storage enforced by hard high-watermark bounds. |
| **Eavesdropping on Sensitive Files** | High | **Ciphertext Hashing & ChaCha20 AEAD:** Data is encrypted at rest and in transit; untrusted mesh relays observe only opaque ciphertext. |
| **Content Tampering** (Modifies files in flight) | Critical | **Cryptographic Merkle Immutability:** Any altered bit changes the root `ContentId`; the client detects tampering and rejects assembly. |

---

## 27. Comprehensive Failure Taxonomy & Recovery Matrix

| Failure Condition | Subsystem Impact | Automated Recovery Behavior |
| :--- | :--- | :--- |
| **Provider Disappears** | Transfer stall | Swarm scheduler detects timeout, re-assigns missing chunks to remaining providers. |
| **Corrupted Chunk** | Tier 1 verify failure | Chunk dropped from memory; provider penalized; chunk re-requested from alternative peer. |
| **Corrupted Manifest** | Tier 2 verify failure | Staging aborted; manifest discarded; re-downloaded from authoritative publisher. |
| **Disk Full during Download** | Storage quota trip | Transfer paused; LRU eviction cleans `CACHED` tier; if still full, session fails cleanly with `QuotaExceeded`. |
| **Runtime Crash during Transfer** | Process termination | On reboot, SQLite WAL recovers session; chunks on disk validated against bitfield; resumes seamlessly. |

---

## 28. Observability & Telemetry Framework

### 28.1 Structured Tracing Events (`content.*`)
- `content.discovered`: `[content_id, provider_node, chunks_available]`
- `content.requested`: `[content_id, session_id, priority]`
- `content.transfer_started`: `[session_id, total_chunks, providers_count]`
- `content.chunk_received`: `[session_id, chunk_index, chunk_size, provider_node]`
- `content.chunk_verified`: `[session_id, chunk_index, duration_us]`
- `content.transfer_paused`: `[session_id, reason]`
- `content.transfer_resumed`: `[session_id, completed_chunks]`
- `content.transfer_completed`: `[session_id, content_id, duration_ms, avg_throughput_mbps]`
- `content.verification_failed`: `[content_id, chunk_index, expected, actual]`
- `content.pinned`: `[content_id]`
- `content.evicted`: `[content_id, bytes_reclaimed]`

### 28.2 Prometheus Operational Metrics
- `fieldnet_content_store_bytes_total{tier="pinned|cached|temp"}`
- `fieldnet_content_transfer_throughput_bytes_per_sec`
- `fieldnet_content_chunk_verify_duration_seconds`
- `fieldnet_content_active_transfer_sessions`
- `fieldnet_content_deduplication_ratio`

---

## 29. Testing & Verification Architecture

Phase 07 mandates an exhaustive multi-tier testing strategy:

1. **Unit Tests:** Deterministic chunking, BLAKE3 multihash serialization, Merkle root computation, manifest encoding/decoding, bitfield operations.
2. **Property-Based Invariant Tests:**
   - Invariant: Reassembling chunks $0..N$ always yields identical original byte stream.
   - Invariant: Permuting chunk arrival orders never alters the verified reconstructed state.
   - Invariant: Storing the same byte sequence $K$ times increments reference count by $K$ and leaves physical storage constant.
3. **Fault Injection & Chaos Tests:** Bit flips in leaf chunks, truncated chunks, dead peer simulations mid-transfer, disk-full triggers during ingestion, and crash recovery mid-stream.
4. **Multi-Node Swarm Verification:** 3 independent runtime instances validating multi-source retrieval (Node A has chunks 0..4, Node B has chunks 5..9, Node C downloads in parallel and verifies).

---

## 30. Implementation Boundaries & Crate Architecture

Phase 07 will be implemented in a dedicated crate **`crates/fieldnet-content`** with direct integration into `fieldnet-runtime` and `fieldnet-protocol`:

```
crates/fieldnet-content/
├── Cargo.toml
└── src/
    ├── lib.rs                  <-- Crate root & ContentSystem facade
    ├── cid.rs                  <-- Canonical ContentId & ChunkId
    ├── chunker.rs              <-- Adaptive fixed-size chunking engine
    ├── manifest.rs             <-- Merkle manifest structure & builder
    ├── merkle.rs               <-- Binary Merkle tree implementation
    ├── storage/
    │   ├── mod.rs
    │   ├── store.rs            <-- Physical disk CAS manager
    │   ├── index.rs            <-- SQLite CAS index (content.db)
    │   └── quota.rs            <-- Eviction & watermark manager
    ├── transfer/
    │   ├── mod.rs
    │   ├── session.rs          <-- TransferSession state machine
    │   ├── manager.rs          <-- Multi-source transfer engine
    │   └── scheduler.rs        <-- Chunk piece picker (rarest-first)
    ├── discovery/
    │   ├── mod.rs
    │   └── registry.rs         <-- ContentProviderRegistry
    ├── stream.rs               <-- Zero-copy async streaming reader
    ├── security.rs             <-- Capability verification & decryptor
    └── error.rs                <-- ContentError enumeration
```

---

## 31. Phase 07 Implementation Build Sequence

Phase 07 implementation is partitioned into 7 sequential, test-driven builds:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    PHASE 07 SEQUENTIAL IMPLEMENTATION ROADMAP                   │
├──────────┬──────────────────────────────────────────┬───────────────────────────┤
│ Build ID │ Scope & Subsystem Focus                  │ Key Deliverables          │
├──────────┼──────────────────────────────────────────┼───────────────────────────┤
│ **B-01** │ Content Identity & Content Store         │ ContentId, disk fanout,   │
│          │                                          │ SQLite index, put/get/has │
├──────────┼──────────────────────────────────────────┼───────────────────────────┤
│ **B-02** │ Chunking, Manifests & Merkle Engine      │ Chunker, ContentManifest, │
│          │                                          │ Merkle verification tree  │
├──────────┼──────────────────────────────────────────┼───────────────────────────┤
│ **B-03** │ Content Discovery & Provider Registry    │ CONTENT_QUERY/ADVERTISE,  │
│          │                                          │ ProviderRegistry ledger   │
├──────────┼──────────────────────────────────────────┼───────────────────────────┤
│ **B-04** │ Transfer Engine, Framing & Resume        │ TransferSession state     │
│          │                                          │ machine, bitfield resume  │
├──────────┼──────────────────────────────────────────┼───────────────────────────┤
│ **B-05** │ Multi-Source Parallel Swarm Retrieval    │ Rarest-first picker,      │
│          │                                          │ multi-peer chunk pipelining│
├──────────┼──────────────────────────────────────────┼───────────────────────────┤
│ **B-06** │ Quotas, Eviction, Pinning & Encryption   │ LRU eviction, GC engine,  │
│          │                                          │ capability auth tokens    │
├──────────┼──────────────────────────────────────────┼───────────────────────────┤
│ **B-07** │ Full Multi-Node Mesh Integration & Tests │ Real network 3-node swarm,│
│          │                                          │ DTN integration, fault test│
└──────────┴──────────────────────────────────────────┴───────────────────────────┘
```

---

## 32. Phase 07 Acceptance Criteria

Phase 07 will be marked complete only when the following criteria are validated with automated test evidence:

- [ ] Content identity is cryptographic, canonical, and self-describing (`ContentId`).
- [ ] Content is addressed by hash identity rather than host location or filesystem path.
- [ ] Storing identical content multiple times deduplicates with zero redundant disk usage.
- [ ] File metadata (name, mime, tags, version) is decoupled from content bytes.
- [ ] Large files chunk into verifiable slices ($\le 1\text{ MB}$).
- [ ] Content manifests encode complete Merkle tree topology and publisher signatures.
- [ ] All 8 content lifecycle states and transition invariants are strictly enforced.
- [ ] Leaf chunk, Merkle branch, and root content integrity verifications pass.
- [ ] Interrupted transfers resume from verified chunks without restarting from zero.
- [ ] Multi-source parallel transfer retrieves chunks concurrently across multiple peers.
- [ ] Content discovery finds providers without transferring bulk payloads.
- [ ] Pinning prevents automated eviction under storage pressure.
- [ ] LRU eviction safely reclaims space from `CACHED` tier when high watermark is reached.
- [ ] Content protocol frames cleanly over Phase 05 `MessageType::File = 7`.
- [ ] Small metadata traverses DTN queues while bulk chunks utilize high-bandwidth paths.
- [ ] Confidential content is encrypted with ChaCha20-Poly1305 and capability-gated.
- [ ] Streaming API enables sequential reading and random seeks without full-file RAM buffering.
- [ ] Corrupted chunks and malicious providers are detected and rejected fail-closed.
- [ ] All unit, property, and multi-node integration tests pass across the workspace.
- [ ] No regression introduced to Phases 01–06.
