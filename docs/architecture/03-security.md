# FIELDNET Architecture Specification
## Phase 03 — Document 03: Security Architecture, Trust Management & Secure Sessions

**Document ID:** `DOC-ARCH-03-SECURITY`  
**Phase:** Phase 03 (Identity, Security & Trust) — Final Architecture Pass (Prompts 01, 02, 03)  
**Status:** Authoritative Architectural Specification  
**Version:** 1.2  
**References:** `FIELDNET_MASTER_SPEC.md`, `docs/architecture/01-system-model.md`, `docs/architecture/02-runtime-architecture.md`  

---

## 1. Executive Security Philosophy & Objectives

In centralized cloud architectures, security relies heavily on perimeter firewalls, central identity brokers (OAuth/OIDC), Certificate Authorities (CAs), and always-on TLS connections to managed servers. In an offline-first, ad-hoc, multi-hop distributed environment like FIELDNET, none of these assumptions hold.

FIELDNET is designed under the axiom that **the physical network is hostile, dynamic, and partitioned**. Security cannot be anchored to IP addresses, central directories, or physical radios. It must be anchored mathematically in **sovereign cryptographic identities, end-to-end authenticated encryption, and unforgeable capability tokens**.

### 1.1 Core Security Objectives

| Objective | Formal Definition in FIELDNET | Architectural Implementation Mechanism |
| :--- | :--- | :--- |
| **Identity** | Sovereign, permanent, cryptographic representation of a node or user. | Ed25519 public keypair generated locally; completely independent of IP or MAC addresses. |
| **Authentication** | Cryptographic verification that an entity is who they claim to be ("Who are you?"). | Mutual challenge-response cryptographic handshake using Ed25519 signatures and ephemeral nonces. |
| **Authorization** | Cryptographic verification of permitted actions ("What are you allowed to do?"). | Explicit, cryptographically signed, unforgeable `CapabilityToken` grants. |
| **Confidentiality** | Protection of data payloads against eavesdropping by untrusted intermediaries. | Ephemeral symmetric AEAD encryption (ChaCha20-Poly1305) negotiated via X25519 Diffie-Hellman. |
| **Integrity** | Detection of any unauthorized modification of messages or content in transit. | Poly1305 MAC tags on transport frames; SHA-256 cryptographic hashes on content blocks. |
| **Authenticity** | Cryptographic proof of origin and non-repudiation for messages and manifests. | Ed25519 digital signatures by the authoritative originating node or publisher. |
| **Replay Protection** | Invalidation of duplicated, re-sent, or delayed historical valid frames. | Monotonically increasing sequence numbers, random session nonces, and sliding-window replay filters. |
| **Availability** | Continuous local operation and graceful degradation despite transport drops. | Local-first storage, DTN persistent queueing, multi-transport fallback, bounded resource limits. |
| **Key Protection** | Strict containment preventing private cryptographic keys from leaving the node. | Isolated keystore access, zero network transmission of private keys, platform secure enclave binding. |
| **Capability Isolation** | Prevention of unauthorized capability escalation across subsystem boundaries. | Cryptographic signature verification on capability tokens before executing privileged operations. |
| **Application Isolation**| Containment of sandboxed applications from accessing raw host OS resources. | WebAssembly (Wasmtime) sandbox with capability-controlled Environment API bindings. |
| **Service Authenticity** | Verification that a service instance conforms to an authorized publisher manifest. | Manifest digital signatures, provider capability checks, and semantic URI validation. |
| **Content Integrity** | Self-verifying data chunks independent of the delivering peer or storage path. | Content-addressed storage (CID) where Content ID is strictly the SHA-256 hash of payload bytes. |

---

## 2. Comprehensive Threat Model

FIELDNET operates in harsh, contested, and infrastructure-deprived environments (e.g., natural disasters, search-and-rescue, remote edge operations). The threat model assumes that adversaries may control intermediary radio relays, inject arbitrary wireless frames, operate compromised nodes, steal physical hardware, or deploy Sybil swarms.

### 2.1 Threat Catalog

#### 1. Identity Spoofing
*   **Attacker Capability:** Attacker can craft arbitrary radio frames and claim to possess a victim's `NodeId`.
*   **Target:** Node authentication and peer tables.
*   **Attack:** Transmitting discovery advertisements with a victim's public key or claims.
*   **Expected Security Property:** Identity Authenticity.
*   **Mitigation Category:** Cryptographic challenge-response handshake; advertisements are strictly untrusted until the peer proves possession of the corresponding Ed25519 private key via signed nonce.
*   **Remaining Limitation:** Does not prevent the attacker from advertising their own valid, but newly generated, pseudonymous identity.

#### 2. Impersonation
*   **Attacker Capability:** Attacker attempts to masquerade as an authorized user, operator, or service provider.
*   **Target:** Authorization subsystems and peer service sessions.
*   **Attack:** Sending messages signed by an untrusted key with forged metadata claims.
*   **Expected Security Property:** Non-repudiation and Authenticity.
*   **Mitigation Category:** Every privileged command requires an explicit, signed capability token issued by an authoritative identity root.
*   **Remaining Limitation:** If an authorized user's private key is physically compromised, the attacker can impersonate that user until revocation is processed.

#### 3. Man-in-the-Middle (MitM)
*   **Attacker Capability:** Attacker controls an intermediary Wi-Fi access point, Bluetooth proxy, or relay node between Node A and Node B.
*   **Target:** In-flight message payloads, credentials, and session keys.
*   **Attack:** Intercepting session negotiation and presenting separate keys to Node A and Node B.
*   **Expected Security Property:** Mutual Authentication and Forward Secrecy.
*   **Mitigation Category:** Authenticated Key Exchange: X25519 ephemeral keys are signed using the nodes' permanent Ed25519 identity keys during the handshake, binding the session key directly to the verified identities.
*   **Remaining Limitation:** Initial out-of-band trust establishment (e.g., QR code scanning, manual pairing) is required to prevent a first-contact MitM if no prior trust root exists.

#### 4. Traffic Interception
*   **Attacker Capability:** Passive RF sniffing of Wi-Fi or Bluetooth LE broadcast frames.
*   **Target:** User communications, document state, private metadata.
*   **Attack:** Recording broadcast or unicast packets over open radio frequencies.
*   **Expected Security Property:** Confidentiality.
*   **Mitigation Category:** End-to-end AEAD payload encryption using ChaCha20-Poly1305 with ephemeral X25519 session keys.
*   **Remaining Limitation:** Radio frequency metadata (packet timing, frame length, approximate peer proximity) remains visible to physical sniffers.

#### 5. Message Modification
*   **Attacker Capability:** Active bit-flipping, payload tampering, or frame injection by a relay node.
*   **Target:** Relayed messages in multi-hop paths or DTN queues.
*   **Attack:** Altering coordinates, commands, or data fields within forwarded frames.
*   **Expected Security Property:** Integrity and Authenticity.
*   **Mitigation Category:** Poly1305 message authentication codes (MAC) on session frames; Ed25519 digital signatures on DTN message envelopes.
*   **Remaining Limitation:** Attacker can corrupt the frame, causing it to be dropped, but cannot forge valid modified content.

#### 6. Replay Attacks
*   **Attacker Capability:** Attacker records a valid, previously transmitted frame or message and re-transmits it later.
*   **Target:** State machines, compute jobs, or transaction ledgers.
*   **Attack:** Replaying an "unlock door" command or replaying a duplicate financial/resource mutation.
*   **Expected Security Property:** Freshness and Uniqueness.
*   **Mitigation Category:** Nonce-based sliding window filters on session frames; monotonically increasing message timestamps combined with unique message UUIDs and bounded expiration TTLs in SQLite deduplication tables.
*   **Remaining Limitation:** Replays within the active sliding-window threshold must be caught by deduplication filters.

#### 7. Fake Services
*   **Attacker Capability:** Attacker advertises malicious services (e.g., a rogue `ai.inference` or `storage.backup`).
*   **Target:** Service Resolver and client applications.
*   **Attack:** Luring client requests to siphon sensitive query data or return falsified outputs.
*   **Expected Security Property:** Service Authenticity and Authorization.
*   **Mitigation Category:** Service advertisements must include signed manifests indicating publisher identity and verified capability grants; sensitive queries require end-to-end encryption to the verified service key.
*   **Remaining Limitation:** If an attacker advertises an open, unauthenticated service (e.g., generic public weather), clients must treat returned outputs as unverified.

#### 8. Malicious Applications
*   **Attacker Capability:** Untrusted third-party software package installed by an operator.
*   **Target:** Local node storage, host OS filesystem, private keys, device hardware.
*   **Attack:** Attempting to read `/etc/passwd`, extract private keys, or exhaust system memory.
*   **Expected Security Property:** Application Isolation and Least Privilege.
*   **Mitigation Category:** WebAssembly (WASM) execution sandbox; zero direct host syscalls; all interactions mediated by capability-gated Environment API.
*   **Remaining Limitation:** Application can consume its allocated memory quota and crash its own sandbox (mitigated by Resource Manager quotas).

#### 9. Compromised Nodes
*   **Attacker Capability:** Attacker seizes software control of an authorized node in the environment.
*   **Target:** Local shared state, mesh routing, compute grid.
*   **Attack:** Reading authorized data, injecting false reports, dropping transit traffic.
*   **Expected Security Property:** Fault Containment and Non-Escalation.
*   **Mitigation Category:** Least privilege capability scopes (a compromised field worker cannot issue administrative revocations); audit logging; peer revoking.
*   **Remaining Limitation:** Cryptography cannot prevent a legitimately authorized node from reading data it has been explicitly authorized to read.

#### 10. Malicious Relay Nodes
*   **Attacker Capability:** Intermediate multi-hop node participates in mesh routing but behaves adversarially.
*   **Target:** Delay-Tolerant Network (DTN) and multi-hop delivery.
*   **Attack:** Selectively dropping packets, corrupting payloads, or misrouting frames.
*   **Expected Security Property:** End-to-End Integrity and Alternate Route Discovery.
*   **Mitigation Category:** End-to-end payload encryption and digital signatures ensure relays cannot read or alter messages; multi-path gossip routing and reputation scoring detect and route around consistently dropping relays.
*   **Remaining Limitation:** If all available physical paths traverse the malicious relay, partition occurs.

#### 11. Sybil Attacks
*   **Attacker Capability:** Attacker generates thousands of synthetic `NodeId` keypairs to flood routing tables or bias elections.
*   **Target:** Peer discovery tables, distributed compute worker selection, gossip fanout.
*   **Attack:** Overwhelming genuine peers with phantom nodes.
*   **Expected Security Property:** Identity Scarcity and Reputation Integrity.
*   **Mitigation Category:** Discovered peers are held in ephemeral, memory-bounded, LRU-evicted caches; unauthenticated peers are granted zero capabilities; pairing requires out-of-band human verification (QR code, SAS) or pre-shared authorization tickets.
*   **Remaining Limitation:** Attacker can flood unauthenticated radio beacons, creating local RF noise.

#### 12. Message Flooding (DoS)
*   **Attacker Capability:** Rogue node broadcasts gigabytes of junk frames across Wi-Fi Direct or Bluetooth LE.
*   **Target:** Local radio link capacity and battery power.
*   **Attack:** Monopolizing Bluetooth LE or Wi-Fi Direct airtime.
*   **Expected Security Property:** Fair Queueing.
*   **Mitigation Category:** Bounded concurrent chunk transfers (max 8 in-flight); per-peer token bucket rate-limiting; battery-aware throttling.
*   **Remaining Limitation:** Ad-hoc wireless bandwidth is naturally shared; local radio throughput will degrade under heavy legitimate and illegitimate contention.

#### 13. Storage Exhaustion
*   **Attacker Capability:** Attacker sends massive payloads or files to fill the victim node's local flash storage.
*   **Target:** Local disk and SQLite storage engine.
*   **Attack:** Forcing node crashes or preventing storage of critical operational data.
*   **Expected Security Property:** Bounded Resource Consumption.
*   **Mitigation Category:** Capability tokens specify strict byte quotas (`max_bytes`); global and per-peer storage quotas enforced by Resource Manager; automatic eviction of unpinned ephemeral cache.
*   **Remaining Limitation:** Once unpinned cache is evicted, legitimate incoming data must be rejected if storage is genuinely full.

#### 14. Bandwidth Exhaustion
*   **Attacker Capability:** High-volume traffic injection targeted at a specific node or multi-hop path.
*   **Target:** Network interface queues and radio bandwidth.
*   **Attack:** Squeezing out mission-critical telemetry.
*   **Expected Security Property:** QoS Priority Queueing.
*   **Mitigation Category:** Traffic prioritization (`Priority::Critical` vs. `Priority::Bulk`); priority-aware DTN queues drop bulk frames first under saturation.
*   **Remaining Limitation:** Heavy physical RF jamming degrades physical SNR.

#### 15. CPU Exhaustion
*   **Attacker Capability:** Attacker sends malformed cryptographic handshakes with expensive parameters or submits infinite-loop compute jobs.
*   **Target:** Host CPU cores and Tokio thread pools.
*   **Attack:** Pinning CPU utilization to 100%, causing device lockup and rapid battery drain.
*   **Expected Security Property:** Bounded Execution.
*   **Mitigation Category:** Fixed cryptographic suites (Ed25519/X25519 have predictable, fast verification costs; no slow RSA keys); WASM instruction counters (gas meters) terminate infinite loops.
*   **Remaining Limitation:** Continuous cryptographic verification still consumes non-zero CPU; rate limits on incoming handshakes are required.

#### 16. Malicious Compute Workers
*   **Attacker Capability:** Rogue node accepts a dispatched compute task (e.g., image analysis) and returns fabricated results.
*   **Target:** Distributed compute grid and calling applications.
*   **Attack:** Returning false positives or corrupted analytics to mislead rescue teams.
*   **Expected Security Property:** Result Verifiability.
*   **Mitigation Category:** Critical compute jobs require capability-attested workers, redundant dual-worker execution with voting, or deterministic WASM execution validation.
*   **Remaining Limitation:** General non-deterministic AI inference (e.g., LLM generation) cannot be validated by simple hash checks without redundant execution.

#### 17. Corrupted Content
*   **Attacker Capability:** Peer serves byte-modified chunks for a requested file.
*   **Target:** Content Store and application documents.
*   **Attack:** Corrupting map tiles, software updates, or medical reference manuals.
*   **Expected Security Property:** Self-Verifying Content Integrity.
*   **Mitigation Category:** Content Addressing: Every chunk is identified by its SHA-256 hash. Upon receipt, the chunk is hashed; if `hash(bytes) != CID`, the chunk is instantly discarded.
*   **Remaining Limitation:** Attacker can refuse to provide the correct chunk, but cannot trick the node into accepting a corrupt one.

#### 18. Stolen Physical Device
*   **Attacker Capability:** Attacker gains physical possession of a smartphone or laptop running FIELDNET.
*   **Target:** Private keys, cached documents, message history.
*   **Attack:** Extracting raw flash storage or dumping memory.
*   **Expected Security Property:** At-Rest Protection.
*   **Mitigation Category:** Sovereign private keys encrypted at rest using platform secure enclaves (Apple Keychain, Android Keystore, Linux TPM/LUKS); operator PIN/passphrase derivation.
*   **Remaining Limitation:** If the host OS has no screen lock or secure hardware, an attacker with physical flash access can extract unencrypted data. FIELDNET cannot secure an unencrypted host OS against physical compromise.

#### 19. Compromised Private Key
*   **Attacker Capability:** Attacker successfully extracts or steals an operator's Ed25519 private key.
*   **Target:** Sovereign identity and all historical/future interactions.
*   **Attack:** Signing malicious statements and decrypting historical sessions.
*   **Expected Security Property:** Forward Secrecy and Revocability.
*   **Mitigation Category:** Forward secrecy via ephemeral X25519 session keys protects past communications; Key Revocation Lists (KRL) broadcast across the mesh invalidate future communications.
*   **Remaining Limitation:** Actions taken prior to revocation propagation appear cryptographically valid.

#### 20. Malicious Gateway Node
*   **Attacker Capability:** Node advertising `internet.gateway` is operated by an adversary.
*   **Target:** External web traffic and DNS queries.
*   **Attack:** Monitoring outbound requests, modifying unencrypted HTTP traffic, DNS spoofing.
*   **Expected Security Property:** Gateway Isolation.
*   **Mitigation Category:** The gateway is treated as an untrusted proxy; all external traffic must use end-to-end TLS (HTTPS); internal FIELDNET identities are never leaked to external servers.
*   **Remaining Limitation:** The gateway can observe destination domains (SNI) and external IP addresses, and can drop traffic.

#### 21. Metadata Leakage
*   **Attacker Capability:** Wireless sniffer observing multi-hop packet routing.
*   **Target:** Social graphs, device movement, role identification.
*   **Attack:** Inferring who is communicating with whom based on packet headers.
*   **Expected Security Property:** Header Protection.
*   **Mitigation Category:** Link-layer frame headers are encrypted per-hop between adjacent peers; internal end-to-end envelopes are encapsulated.
*   **Remaining Limitation:** Radio direction finding and traffic volume analysis cannot be completely eliminated in ad-hoc RF environments.

#### 22. Network Partitioning
*   **Attacker Capability:** Attacker places jamming hardware between two operational sectors.
*   **Target:** Mesh connectivity graph.
*   **Attack:** Severing physical radio links.
*   **Expected Security Property:** Partition Tolerance.
*   **Mitigation Category:** Local-first architecture allows both partitions to continue operating autonomously using local state and CRDTs; DTN queues buffer traffic until physical reconciliation.
*   **Remaining Limitation:** Cross-partition communication is physically impossible during the severance.

#### 23. Denial of Service (DoS) via Protocol Abuse
*   **Attacker Capability:** Malicious peer continuously initiates and aborts protocol handshakes or sends oversized headers.
*   **Target:** Runtime connection managers and parser state machines.
*   **Attack:** Exhausting available connection slots and parser memory.
*   **Expected Security Property:** Robust Framing and Bounded Handshakes.
*   **Mitigation Category:** Strict framing limits (max frame size 64 KB); bounded handshake timeouts (5 seconds); automatic peer blacklisting upon protocol violations.
*   **Remaining Limitation:** Extremely dense local wireless jamming impacts all devices on that frequency band.

---

## 3. Node Identity Architecture & Complete Lifecycle

A FIELDNET Node Identity is sovereign, mathematical, and permanent. It does not depend on any centralized issuer, DNS registry, or mobile carrier. It represents the persistent anchor of the node in the distributed environment.

```text
+--------------------------------------------------------------------------+
|                        COMPLETE IDENTITY LIFECYCLE                       |
+--------------------------------------------------------------------------+
|                                                                          |
|  1. Identity Creation      (Generate Ed25519 keypair via hardware CSPRNG)|
|            │                                                             |
|            ▼                                                             |
|  2. Identity Storage       (Encrypt & store in platform secure keystore) |
|            │                                                             |
|            ▼                                                             |
|  3. Identity Loading       (Decrypt keypair on runtime startup & verify) |
|            │                                                             |
|            ▼                                                             |
|  4. Identity Advertisement (Broadcast public NodeId & ephemeral beacons) |
|            │                                                             |
|            ▼                                                             |
|  5. Peer Authentication    (Mutual Ed25519 challenge-response handshake) |
|            │                                                             |
|            ▼                                                             |
|  6. Trust Establishment    (Pairing approval, capability token exchange) |
|            │                                                             |
|            ▼                                                             |
|  7. Identity Usage         (Sign messages, negotiate AEAD, execute jobs) |
|            │                                                             |
|            ▼                                                             |
|  8. Rotation / Revocation  (Dual-signed key migration or KRL broadcast)  |
|                                                                          |
+--------------------------------------------------------------------------+
```

### 3.1 Identity Data Structure & Fields

The local node identity encapsulates the cryptographic keys, operational metadata, and state:

```text
+--------------------------------------------------------------------------+
|                        NODE IDENTITY RECORD SCHEMA                       |
+--------------------------------------------------------------------------+
| Field Name           | Type             | Description                    |
+----------------------+------------------+--------------------------------+
| Node ID              | Canonical String | Base58 or Hex encoded SHA-256  |
|                      |                  | fingerprint of Ed25519 PubKey  |
|                      |                  | (e.g. `ed25519:7F8a9B01c2...`) |
| Public Key           | 32-byte binary   | Raw Ed25519 public key         |
| Private Key          | 32-byte binary   | Raw Ed25519 secret seed        |
|                      |                  | (STRICTLY LOCAL & PROTECTED)   |
| Identity Metadata    | Struct / JSON    | Display name, device class,    |
|                      |                  | hardware profile, tags         |
| Creation Timestamp   | u64 (epoch ms)   | Monotonic UTC millisecond time |
| Key Version          | u32              | Monotonic sequence (starts 1)  |
| Identity Status      | Enum             | UNINITIALIZED, ACTIVE, etc.    |
+----------------------+------------------+--------------------------------+
```

### 3.2 Stability Across Runtime Restarts
*   **Normal Runtime Restart / Host OS Reboot:** The `NodeId`, `PublicKey`, `PrivateKey`, and `CreationTimestamp` remain unchanged across reboots. Identity material is loaded durably from local storage.
*   **Crash Recovery:** An ungraceful crash must not corrupt or alter identity material. Identity loading uses atomic read-after-verify routines.
*   **Ephemeral Session Invalidation:** While the `NodeId` remains strictly stable across restarts, all in-flight transport connections, ephemeral X25519 session keys, and handshake nonces are immediately purged and regenerated upon reconnection.

---

## 4. Identity Storage & Platform Secure Adapters

The private cryptographic key material is the root of sovereign trust for a FIELDNET node. Compromise of the private key breaks all identity guarantees.

### 4.1 Strict Private Key Invariants
1.  **Local Only:** Private key material exists exclusively on the host device where it was generated.
2.  **Never Transmitted:** Private keys must NEVER be transmitted across any network, transport, radio, inter-process socket, or serial port.
3.  **Never Exposed to Applications:** Guest WASM applications, user-facing Tauri UI code, and external plugins have ZERO direct access to private key bytes.
4.  **Never Logged:** Private key bytes or seed material must NEVER appear in application logs, trace statements, console outputs, or error messages.
5.  **Never Included in Diagnostics:** System status dumps, crash reports, core dumps, and health metrics must scrub all secret key memory areas.
6.  **Zero Plaintext Storage:** Private keys must NEVER be stored in plaintext on disk where secure platform storage is available.

### 4.2 Platform-Specific Secure Storage Adapters
Storage of private identity keys is delegated to the `PlatformAdapter` layer to leverage hardware-backed enclaves and OS keystores:

*   **macOS / iOS Keychain:** Utilizes Apple Keychain Services with `kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly` and Secure Enclave hardware protection where supported.
*   **Android Keystore:** Employs the Android Keystore Provider backed by Hardware Security Modules (TEE or StrongBox Keymaster) to ensure key material cannot be extracted even with root access.
*   **Windows Credential Store / DPAPI:** Employs Windows Data Protection API (`CryptProtectData`) tied to the local user profile and hardware TPM.
*   **Linux Secret Service / TPM:** Binds to `org.freedesktop.secrets` via DBus or TPM 2.0 NVRAM. In headless minimal field servers lacking Secret Service daemons, identity keys are encrypted using an AES-256-GCM envelope key derived via Argon2id from an operator passphrase, stored with strict POSIX `0600` file permissions.

---

## 5. Identity States & Finite State Machine

A FIELDNET Node Identity transitions through a formal finite state machine from creation to retirement or compromise.

```text
                   +------------------+
                   |  UNINITIALIZED   |
                   +------------------+
                            │
                            │ Generate keypair via CSPRNG
                            ▼
                   +------------------+
                   |   INITIALIZING   |
                   +------------------+
                            │
                            │ Keystore write & verification successful
                            ▼
         ┌────────>+------------------+<────────┐
         │         |      ACTIVE      |         │
         │         +------------------+         │
         │           │        │        │        │
         │    Rotate │        │ Lock   │ Unlock │
         │   succeeds│        │        │        │
         │           ▼        ▼        │        │
+------------------+   +------------------+     │
|     ROTATING     |   |     DISABLED     |─────┘
+------------------+   +------------------+
         │                      │
         │ Rotation aborted     │ Operator decommissioning
         │                      ▼
         │             +------------------+
         └────────────>|     REVOKED      | (IRREVERSIBLE TERMINAL)
                       +------------------+
                                ▲
                                │ Key compromise detected
                       +------------------+
                       |   COMPROMISED    |
                       +------------------+
```

### 5.1 State Definitions
*   **`UNINITIALIZED`:** The node has no cryptographic identity. No keys exist in local storage. The runtime cannot communicate on the mesh.
*   **`INITIALIZING`:** Keypair generation is in progress. The node is sampling hardware entropy and writing encrypted records to the platform keystore.
*   **`ACTIVE`:** The node identity is fully initialized, cryptographically verified, and unlocked. The node can sign messages, issue capability tokens, establish sessions, and participate in mesh routing.
*   **`ROTATING`:** The node is in the process of migrating to a new cryptographic keypair. A dual-signed `IdentityMigrationCertificate` is being generated and disseminated across the mesh.
*   **`DISABLED`:** The identity is temporarily locked by operator action or local security policy (e.g., failed PIN attempts, screen lock). The private key is purged from volatile memory; the node cannot sign outbound frames but its identity records remain intact on disk.
*   **`COMPROMISED`:** The private key is suspected or known to have been extracted, stolen, or exposed. Outbound operations are immediately quarantined; the node generates a signed revocation declaration.
*   **`REVOKED`:** The identity is permanently dead. The public key is blacklisted across the mesh. The local runtime purges all associated private key material. This state is **strictly irreversible**.

---

## 6. Peer Trust Record vs. Peer Discovery Record

FIELDNET enforces an absolute structural and lifecycle separation between discovering a radio neighbor and maintaining a trust relationship with a peer.

```text
+------------------------------------+-------------------------------------+
|       PEER DISCOVERY RECORD        |          PEER TRUST RECORD          |
+------------------------------------+-------------------------------------+
| Lifetime: Ephemeral (Minutes)      | Lifetime: Permanent (Survives Boot) |
| Location: Volatile RAM Cache (LRU) | Location: Persistent SQLite Store   |
| Scope: Radio Link / Direct Reach   | Scope: Logical Mesh / Identity      |
| Cryptography: Unverified           | Cryptography: Verified Ed25519 PubKey|
| Access: ZERO Capabilities Granted  | Access: Explicit Capability Tokens  |
+------------------------------------+-------------------------------------+
```

### 6.1 Peer Discovery Record (Ephemeral)
Created automatically when a transport adapter intercepts a physical beacon or advertisement frame:
*   `EphemeralPeerId`: Transient local handle.
*   `TransportType`: `WiFiDirect` | `BLE` | `TCP` | `UDP`.
*   `PhysicalAddress`: MAC address, IP/port, or BLE peripheral UUID.
*   `ObservedSignalStrength`: RSSI / Link quality metric.
*   `DeclaredNodeId`: Raw string advertised by peer (completely untrusted).
*   `LastBeaconTime`: Monotonic clock timestamp.
*   *Eviction:* Automatically purged after 120 seconds of silence; capped at 256 unauthenticated entries to prevent memory exhaustion DoS.

### 6.2 Peer Trust Record (Persistent)
Maintained in the secure local SQLite database for peers with whom trust or cryptographic verification has been established:
*   `NodeId`: Canonical SHA-256 fingerprint of the verified Ed25519 public key.
*   `PublicKey`: 32-byte verified Ed25519 public key.
*   `DisplayName`: Human-readable alias or operator name assigned during pairing.
*   `TrustState`: Current trust status (`UNKNOWN`, `DISCOVERED`, `AUTHENTICATED`, `AUTHORIZED`, `SUSPENDED`, `REVOKED`).
*   `FirstSeen`: UTC epoch millisecond timestamp of initial cryptographic contact.
*   `LastSeen`: UTC epoch millisecond timestamp of most recent authenticated exchange.
*   `AuthenticationState`: `Unauthenticated` | `HandshakeInProgress` | `Authenticated`.
*   `AuthorizationState`: `None` | `PartiallyAuthorized` | `FullyAuthorized`.
*   `GrantedCapabilities`: Set of active, non-expired `CapabilityToken` grants.
*   `KeyVersion`: Monotonically increasing u32 sequence tracking the peer's active key.
*   `RevocationState`: `Valid` | `Suspected` | `Revoked(Timestamp, Reason)`.
*   `Metadata`: Device profile, mutual pairing modality, custom operator tags.

> **ARCHITECTURAL INVARIANT:**  
> **A discovered peer MUST NOT automatically become trusted.** Discovery represents physical proximity; trust represents cryptographically proven authorization.

---

## 7. Trust States & Progression Model

In FIELDNET, trust is never binary or assumed. A peer relationship progresses through six formal trust states governed by cryptographic verification and explicit authorization policies:

```text
                  +-----------------------+
                  |        UNKNOWN        |  (Raw radio frame received)
                  +-----------------------+
                             │
                             │ Valid advertisement parsed
                             ▼
                  +-----------------------+
                  |      DISCOVERED       |  (Public key declared, unverified)
                  +-----------------------+
                             │
                             │ Mutual Ed25519 challenge-response succeeded
                             ▼
                  +-----------------------+
                  |     AUTHENTICATED     |  (Key possession verified mathematically)
                  +-----------------------+
                             │
                             │ Capability tokens granted / policy evaluated
                             ▼
         ┌───────>+-----------------------+<───────┐
         │        |      AUTHORIZED       |        │
         │        +-----------------------+        │
         │                   │                     │
         │ Resume trust      │ Anomaly /           │ Re-activate
         │                   │ Policy tripwire     │
         │                   ▼                     │
         │        +-----------------------+        │
         └────────|       SUSPENDED       |────────┘
                  +-----------------------+
                             │
                             │ Key compromise / Operator revocation
                             ▼
                  +-----------------------+
                  |        REVOKED        |  (PERMANENT BLACKLIST)
                  +-----------------------+
```

### 7.1 Trust State Permissions

| Trust State | Permitted Operations | Prohibited Operations |
| :--- | :--- | :--- |
| **`UNKNOWN`** | Allocate ephemeral link frame buffer (max 64 KB). | Everything else. No state stored. |
| **`DISCOVERED`** | Exchange link-level discovery beacons; initiate handshake. | No storage access, no routing, no service calls, no sync. |
| **`AUTHENTICATED`**| Establish encrypted session; negotiate capability exchange. | Cannot read/write storage, cannot invoke services, cannot egress. |
| **`AUTHORIZED`** | Execute operations strictly matching held `CapabilityTokens`. | Any operation exceeding granted capability scopes. |
| **`SUSPENDED`** | Receive revocation updates; maintain dormant trust record. | All active sessions paused; no message routing; no data access. |
| **`REVOKED`** | None. All packets dropped at physical transport ingress. | Permanent blacklist. All sessions shredded. |

### 7.2 Automated Trust Policy Hooks
While user-controlled pairing is the standard mechanism in tactical and ad-hoc operations, the trust architecture defines formal policy hooks for future automated trust models:
1.  **Ad-Hoc Team Clusters:** Nodes possessing a pre-shared, time-bounded Team Enrollment Token automatically advance from `AUTHENTICATED` to `AUTHORIZED` with a baseline `team.standard` capability set upon mutual handshake.
2.  **Hierarchical Organization Root Attestation:** Nodes carrying an X.509-like lightweight certificate signed by an organizational trust root (e.g., Disaster Relief Command) are automatically authorized for operational data sync.
3.  **Zero-Touch Staging:** Devices provisioned in a physical staging dock via direct Ethernet/NFC can be pre-loaded with mutual `PeerTrustRecords` before deployment into the field.

---

## 8. User-Controlled Pairing Model

Pairing is the fundamental trust-establishment mechanism in FIELDNET. It bridges physical-world human trust into cryptographic distributed trust without requiring cloud authentication servers.

```text
       Node A (Initiator)                               Node B (Responder)
                │                                               │
  1. Discovery  │◄──────── Radio Discovery Beacon ──────────────┤
                │                                               │
  2. Inspection │ User inspects Node B claims                   │ User inspects Node A claims
                │ (Name, Icon, Security Fingerprint)            │ (Name, Icon, Security Fingerprint)
                │                                               │
  3. Approval   │ Operator approves pairing                     │ Operator approves pairing
                │                                               │
  4. Mutual     │ ─── Cryptographic Pairing Handshake (SAS) ───►│
     Auth       │◄─── Mutual Nonces & Public Key Attestation ───┤
                │                                               │
  5. Trust      │ Create persistent `PeerTrustRecord`           │ Create persistent `PeerTrustRecord`
     Creation   │ Status: AUTHENTICATED                         │ Status: AUTHENTICATED
                │                                               │
  6. Capability │ Grant scoped CapabilityToken                  │ Grant scoped CapabilityToken
     Exchange   │ (e.g., `message.send`, `content.read`)        │ (e.g., `message.send`, `content.read`)
                │                                               │
  7. Finalized  │ Status: AUTHORIZED                            │ Status: AUTHORIZED
```

### 8.1 Pairing Modalities
1.  **Cryptographic QR Code Scanning (Primary Visual):** Node A displays dynamic QR containing `(NodeId, PubKey, EphemeralNonce, TransportHints)`. Node B scans with camera. Out-of-band mutual verification immune to radio MitM.
2.  **Short Authentication String / SAS (Voice / Radio Confirmation):** Nodes exchange public keys and nonces over radio; operators visually verify a matching 6-digit code derived via $\text{HKDF-SHA256}$.
3.  **Local Proximity Confirmation:** Host OS biometric/PIN-gated dialog for proximity pairing.
4.  **Hardware NFC Tap:** Proximity key exchange over 13.56 MHz near-field radio ($< 2\text{ cm}$).

---

## 9. Cryptographic Key Hierarchy & Separation

FIELDNET enforces strict hierarchical key derivation and functional role separation. Sovereign identity keys must never be used directly for symmetric encryption or bulk data transmission.

```text
+--------------------------------------------------------------------------+
|                       LONG-TERM NODE IDENTITY KEY                        |
|                  (Ed25519 Keypair / Decades / Disk Keystore)             |
+--------------------------------------------------------------------------+
                                     │
                                     ▼ signs ephemeral exchange
+--------------------------------------------------------------------------+
|                     AUTHENTICATION & KEY AGREEMENT                       |
|               (Ephemeral X25519 Diffie-Hellman + HKDF-SHA256)            |
+--------------------------------------------------------------------------+
                                     │
         ┌───────────────────────────┴───────────────────────────┐
         ▼                                                       ▼
+---------------------------------+     +---------------------------------+
|      INBOUND SESSION KEY        |     |      OUTBOUND SESSION KEY       |
|    (ChaCha20-Poly1305 / 1 Hour) |     |    (ChaCha20-Poly1305 / 1 Hour) |
+---------------------------------+     +---------------------------------+
         │                                                       │
         ▼ per-frame nonce                                       ▼ per-frame nonce
+---------------------------------+     +---------------------------------+
|    MESSAGE ENCRYPTION & MAC     |     |    MESSAGE ENCRYPTION & MAC     |
| (64-bit Monotonic Frame Counter)|     | (64-bit Monotonic Frame Counter)|
+---------------------------------+     +---------------------------------+
```

### 9.1 Why Long-Term Identity Keys Must Never Directly Encrypt Messages
1.  **Key Wear-Out & IV Collision:** Symmetric ciphers have strict cryptographic limits on the number of invocations per key. Reusing a long-term key across millions of frames risks catastrophic nonce collision and plaintext recovery.
2.  **Loss of Forward Secrecy:** If a long-term identity key encrypted messages directly, an adversary who recorded RF traffic could decrypt all historical traffic if the physical device were compromised years later.
3.  **Lack of Replay Protection:** Direct encryption lacks session-bound freshness nonces, enabling cross-session message replay attacks.
4.  **Repudiation Separation:** Digital signatures provide non-repudiation (proof of origin), whereas symmetric session AEAD encryption provides confidentiality and mutual authentication with plausible deniability once ephemeral keys are discarded.
5.  **Blast Radius Containment:** Compromise of an active session key leaks only frames from that single session; the node identity remains secure.

### 9.2 Cryptographic Role Demarcation
*   **Identity Keys:** Long-term, asymmetric (Ed25519). Used strictly for signature verification, identity attestation, and signing ephemeral Diffie-Hellman parameters.
*   **Session Keys:** Ephemeral, symmetric (ChaCha20-Poly1305). Derived via HKDF-SHA256, directional ($K_{\text{send}} \neq K_{\text{recv}}$), volatile in RAM, wiped on session teardown.
*   **Message Nonces:** Strictly unique per frame, derived from a 64-bit monotonically increasing sequence counter. Never reused under the same session key.

---

## 10. Secure Session Architecture & Session Lifecycle

A secure session is the stateful, authenticated, encrypted channel established between two nodes over one or more physical transports.

```text
                  +---------------------------+
                  |        DISCOVERED         |  (Peer beacon detected)
                  +---------------------------+
                                │
                                │ Transport connection established
                                ▼
                  +---------------------------+
                  |        CONNECTING         |  (Socket / BLE GATT open)
                  +---------------------------+
                                │
                                │ Exchange ClientHello / ServerHello
                                ▼
                  +---------------------------+
                  |   IDENTITY_VERIFICATION   |  (Verify remote Ed25519 signature)
                  +---------------------------+
                                │
                                │ X25519 DH + HKDF derivation
                                ▼
                  +---------------------------+
                  |       KEY_AGREEMENT       |  (Generate symmetric session keys)
                  +---------------------------+
                                │
                                │ Handshake complete, test AEAD ping
                                ▼
                  +---------------------------+
                  |    SESSION_ESTABLISHED    |  (Authenticated & Encrypted channel)
                  +---------------------------+
                                │
                                │ Capability tokens evaluated
                                ▼
                  +---------------------------+
                  |        AUTHORIZED         |  (Permitted operations mapped)
                  +---------------------------+
                                │
                                │ Application frames flowing
                                ▼
         ┌───────>+---------------------------+<───────┐
         │        |          ACTIVE           |        │
         │        +---------------------------+        │
         │                      │                      │
         │ Rekeying             │ Idle timeout /       │ Traffic
         │ completes            │ Migration            │ resumes
         │                      ▼                      │
         │        +---------------------------+        │
         └────────|    REKEYING / CLOSING     |────────┘
                  +---------------------------+
                                │
                                │ Explicit close alert or timeout
                                ▼
                  +---------------------------+
                  |          CLOSED           |  (Keys zeroized, session dead)
                  +---------------------------+
```

### 10.1 Layer Separation Matrix

| Layer | Primary Entity | Scope | Lifecycle | Security Role |
| :--- | :--- | :--- | :--- | :--- |
| **Transport** | TCP / BLE / Wi-Fi | Physical link / Socket | Volatile (seconds) | Raw frame delivery, link framing. |
| **Discovery** | Beacon Cache | Local radio range | Ephemeral (minutes)| Proximity awareness, address resolution. |
| **Connection** | Link State | Single interface | Volatile | Connection establishment, MTU negotiation. |
| **Authentication** | Node Identity | Logical Peer | Sovereign / Permanent| Proves mathematical key possession. |
| **Key Agreement** | Ephemeral DH | Handshake exchange | Ephemeral (5 seconds)| Negotiates forward-secure shared secret. |
| **Session** | `SecureSession` | Logical Channel | Bounded (1 hour / rekey)| AEAD encryption, replay filter, counters. |
| **Authorization**| `CapabilityToken` | Principal & Resource | Policy TTL (hours/days)| Grants permission for specific operations. |

### 10.2 Session States & Transition Triggers

```text
+-----------------------+----------------------+-------------------------------------------+
| Session State         | Entry Condition      | Permitted Operations                      |
+-----------------------+----------------------+-------------------------------------------+
| `SessionCreated`      | Handshake initiated  | Frame buffer allocation, timer started.   |
| `SessionActive`       | Handshake & Authz OK | Bidirectional encrypted application data. |
| `SessionIdle`         | No traffic > 30s     | Heartbeat keepalives, low-power mode.     |
| `SessionRekeying`     | 2^24 frames or 1 hr  | Ephemeral DH re-negotiation in background.|
| `SessionExpired`      | Rekey failed / TTL   | Graceful closure, reject new payloads.    |
| `SessionRevoked`      | Peer marked REVOKED  | Immediate abort, zeroize RAM keys.        |
| `SessionClosed`       | Explicit close alert | Final cleanup, remove from active table.  |
+-----------------------+----------------------+-------------------------------------------+
```

*   **Peer Disappears:** Transport watchdog detects silence $> 45\text{s}$; session transitions to `SessionIdle` then `SessionClosed`.
*   **Trust Revoked:** Event Bus emits `trust.revoked`; session manager immediately severs connection and zeroizes keys.
*   **Key Rotation:** Peer presents valid `IdentityMigrationCertificate`; session rekeys under new identity version without dropping state.
*   **Protocol Error:** Sequence violation, bad MAC tag, or oversized frame aborts session immediately (`SessionClosed`).

---

## 11. Cryptographic Key Agreement & Ephemeral Session Keys

FIELDNET employs an authenticated, post-compromise secure key exchange protocol combining **Ed25519 for sovereign identity authentication** and **X25519 for ephemeral key agreement**, protected by **ChaCha20-Poly1305 AEAD**.

```text
       Node A (Initiator)                             Node B (Responder)
    [ Identity: Ed25519_A ]                        [ Identity: Ed25519_B ]
               │                                              │
               │ 1. ClientHello (Ephem_X25519_A, Nonce_A)     │
               ├─────────────────────────────────────────────►│
               │                                              │
               │ 2. ServerHello (Ephem_X25519_B, Nonce_B,     │
               │    Sign_B(Ephem_A || Ephem_B || Nonce_A))    │
               │◄─────────────────────────────────────────────┤
  Verify B's   │                                              │ Verify A's
  Signature    │ 3. ClientAuth (Sign_A(Ephem_B || Ephem_A))   │ Signature
  Derive Keys  ├─────────────────────────────────────────────►│ Derive Keys
               │                                              │
               ▼                                              ▼
    ==============================================================
         ESTABLISHED SECURE SESSION (ChaCha20-Poly1305 AEAD)
         - Forward Secrecy: Compromise of NodeId key cannot
           decrypt past recorded sessions.
         - Mutual Authentication: Both nodes verified via Ed25519.
         - Sliding-window replay protection via Monotonic Counters.
    ==============================================================
```

### 11.1 Session Key Derivation
1.  Both nodes execute Diffie-Hellman: $\text{SharedSecret} = \text{X25519}(\text{sk}_{\text{ephem}}, \text{pk}_{\text{remote\_ephem}})$.
2.  Directional session keys are derived using HKDF-SHA256:
    $$\{K_{\text{send}}, K_{\text{recv}}, K_{\text{mac}}\} = \text{HKDF-Extract-and-Expand}(\text{SharedSecret}, \text{Nonce}_A \parallel \text{Nonce}_B, \text{"FIELDNET-AEAD-v1"})$$
3.  Each frame uses ChaCha20-Poly1305 with a 64-bit sequence counter as the nonce, incremented per frame.

---

## 12. Session Security Properties & Guarantees

A FIELDNET secure session provides the following strict cryptographic guarantees:

1.  **Mutual Peer Authentication:** Both initiator and responder cryptographically prove possession of their respective sovereign Ed25519 private keys before any application payload is processed.
2.  **Payload Confidentiality:** Payloads are encrypted with 256-bit ChaCha20 symmetric stream cipher. Intermediary relays observe only ciphertext.
3.  **Payload Integrity & Authenticity:** Every frame is protected by a 128-bit Poly1305 Message Authentication Code (MAC) computed over the header and ciphertext.
4.  **Replay Resistance:** Monotonically increasing 64-bit sequence numbers combined with a 64-frame sliding window bitmap reject duplicate or out-of-order replayed frames.
5.  **Session Freshness:** Random 32-byte nonces generated per handshake guarantee that every session derives globally unique symmetric keys, even between identical nodes.
6.  **Forward Secrecy:** Ephemeral X25519 keypairs are destroyed in memory immediately after HKDF derivation. Compromising a node's long-term private key cannot decrypt previously recorded sessions.
7.  **Directional Key Separation:** Independent keys are derived for inbound ($K_{\text{recv}}$) and outbound ($K_{\text{send}}$) channels. Compromise of a sending key cannot decrypt incoming frames.
8.  **Session Expiration & Rekeying:** Sessions are bound to a strict maximum lifetime ($3600\text{ seconds}$) and maximum invocation count ($2^{24}\text{ frames}$). Crossing either threshold triggers automatic background rekeying or session teardown.
9.  **Explicit Authenticated Closure:** Sessions terminate via an authenticated `SessionClose` frame signed with the session key, preventing truncation attacks by intermediate network attackers.

---

## 13. Transport Migration & Multi-Transport Session Binding

A FIELDNET node may communicate with a peer across multiple physical radios simultaneously or sequentially:

```text
Node A (Logical NodeId: A)
 ├── TCP Connection (192.168.1.10:4242) ───────────► Node B (Logical NodeId: B)
 ├── Bluetooth LE Link (GATT Characteristic) ──────► Node B (Logical NodeId: B)
 └── Wi-Fi Direct P2P (192.168.49.2:4242) ────────► Node B (Logical NodeId: B)
```

### 13.1 Logical Session Continuity Invariants
1.  **Session Binds to NodeId, Not Socket:** The `SecureSession` state machine resides in `fieldnet-runtime` and is bound strictly to `PeerTrustRecord.NodeId`. Sockets and radio links reside in `PlatformAdapter`.
2.  **Seamless Transport Failover:** If an active TCP socket drops, outbound frames are re-routed to the existing Bluetooth LE connection without re-authenticating or resetting sequence numbers.
3.  **Multipath Transmission:** High-priority telemetry can be dispatched over BLE while bulk content chunks flow over Wi-Fi Direct concurrently, both authenticated under the same sovereign peer identity.
4.  **No Identity Fragmentation:** Changing IP addresses, Wi-Fi networks, or Bluetooth MAC addresses does not create a new logical peer or invalidate existing capability grants.

---

## 14. Message Authentication & Verification Model

Every message envelope traversing FIELDNET carries self-contained cryptographic bindings ensuring origin authenticity and tamper evidence:

```text
+--------------------------------------------------------------------------+
|                     AUTHENTICATED MESSAGE ENVELOPE                       |
+--------------------------------------------------------------------------+
| Header:                                                                  |
|   - Version          : Protocol version (u16)                            |
|   - Message ID       : Cryptographic UUID / Hash (16 bytes)              |
|   - Source Node ID   : Ed25519 Public Key of Author (32 bytes)           |
|   - Dest Node ID     : Ed25519 Public Key of Recipient / Broadcast Topic |
|   - Session ID       : UUID of negotiating session (16 bytes)            |
|   - Timestamp        : Monotonic UNIX epoch milliseconds (u64)           |
|   - Expiration TTL   : Milliseconds until message invalidity (u64)       |
|   - Sequence Number  : Monotonically increasing author sequence (u64)    |
|   - Capability Token : Optional signed capability token                  |
+--------------------------------------------------------------------------+
| Payload:                                                                 |
|   - Encrypted Bytes  : ChaCha20-Poly1305 Ciphertext                      |
|   - Poly1305 MAC     : 16-byte authentication tag                        |
+--------------------------------------------------------------------------+
| Origin Signature:                                                        |
|   - Ed25519 Sig      : Author's digital signature over Header bytes      |
+--------------------------------------------------------------------------+
```

### 14.1 Verification & Attack Detection Matrix

| Attack / Anomaly | Detection Mechanism | Immediate Enforcement Action |
| :--- | :--- | :--- |
| **Duplicate Message** | Message ID found in local SQLite deduplication index or sequence counter marked in sliding window. | Drop frame immediately; emit `telemetry.duplicate_frame`. |
| **Replayed Message** | Sequence number behind window threshold or timestamp $< \text{CurrentTime} - \text{TTL}$. | Discard packet; log potential replay attack. |
| **Tampered Payload** | Poly1305 MAC tag computation mismatch. | Discard packet; increment peer error counter. |
| **Tampered Header** | Ed25519 author digital signature verification failure. | Reject envelope; blacklist connection if repeated. |
| **Expired Message** | Monotonic clock exceeds $t_{\text{timestamp}} + \text{TTL}$. | Drop packet; do not forward across DTN queue. |
| **Unknown Sender** | Source `NodeId` has no entry in `PeerDiscoveryRecord` or `PeerTrustRecord`. | Allocate ephemeral discovery buffer; reject privileged access. |
| **Unauthorized Sender**| Capability token missing, expired, or invalid scope for target topic. | Reject operation with `Error::PermissionDenied`; emit audit event. |

---

## 15. Formal Capability-Based Authorization Model

In FIELDNET, authorization is formally defined as a tuple governing what actions an authenticated principal may execute:

$$\mathbf{CapabilityTuple} = \langle P, C, S, R, A, I, E, V \rangle$$

```text
+-----------------------+----------------------+-------------------------------------------+
| Element               | Type                 | Formal Definition                         |
+-----------------------+----------------------+-------------------------------------------+
| **Principal ($P$)**   | `NodeId` \| `AppId`  | Cryptographically verified identity.      |
| **Capability ($C$)**  | Semantic URI         | Broad permission category (`content.read`)|
| **Scope ($S$)**       | Structural Pattern   | Concrete constraints (`path: "/maps/*"`). |
| **Resource ($R$)**    | Target Identifier    | Concrete CID, service URI, or queue.      |
| **Action ($A$)**      | Operation Enum       | `Read`, `Write`, `Invoke`, `Execute`.     |
| **Issuer ($I$)**      | `NodeId`             | Authorizing sovereign root authority.     |
| **Expiration ($E$)**  | Monotonic Timestamp  | UTC millisecond boundary of validity.     |
| **Revocation ($V$)**  | Status Flag          | Validated against local revocation index. |
+-----------------------+----------------------+-------------------------------------------+
```

### 15.1 Formal Evaluation Calculus
When an operation is requested, the runtime evaluates:

$$\mathbf{Authorize}(P, A, R) \Longleftrightarrow \exists T \in \mathbf{Tokens}(P) \quad \text{such that:}$$
1.  $\mathbf{VerifySignature}(T, \text{Issuer}(T)) \equiv \mathbf{True}$ (Token is cryptographically authentic).
2.  $T.A \equiv A$ (Token covers the requested action).
3.  $R \in T.S$ (Target resource falls strictly within the token's authorized scope).
4.  $\text{CurrentTime}() \le T.E$ (Token has not expired).
5.  $T.\text{TokenId} \notin \mathbf{RevocationIndex}$ (Token has not been revoked).
6.  $\mathbf{QuotaAvailable}(T, \text{RequestedCost}) \equiv \mathbf{True}$ (Byte/instruction quotas not exceeded).

If all conditions hold, the operation is **ALLOWED**; otherwise, it **FAILS CLOSED (DENIED)**.

---

## 16. Authorization Pipeline & Enforcement Boundary

Authorization logic does NOT live scattered across business handlers. It is enforced centrally at the **Environment API and Runtime Security boundary** *strictly before* any protected operation executes:

```text
               INCOMING REQUEST
                      │
                      ▼
         [ 1. IDENTIFY PRINCIPAL ]
         - Extract NodeId from authenticated session or AppId from WASM context.
                      │
                      ▼
         [ 2. VALIDATE SECURE SESSION ]
         - Assert session is ACTIVE; verify sequence counter & replay filter.
                      │
                      ▼
         [ 3. CHECK CAPABILITY TOKEN ]
         - Validate Ed25519 issuer signature; assert token is unrevoked & unexpired.
                      │
                      ▼
         [ 4. CHECK RESOURCE SCOPE ]
         - Match target resource URI/CID against token scope pattern.
                      │
                      ▼
         [ 5. CHECK LOCAL RESOURCE POLICY ]
         - Inspect host quotas (battery level, storage limit, CPU gas).
                      │
                      ▼
              Evaluation Result?
             /                 \
       ALLOWED                 DENIED
          │                       │
          ▼                       ▼
  [ 6. EXECUTE OPERATION ]   [ 6. FAIL CLOSED ]
  - Storage / Service call   - Return `Error::PermissionDenied`
          │                       │
          └───────────┬───────────┘
                      ▼
          [ 7. EMIT AUDIT EVENT ]
          - Record in durable SQLite audit ledger.
```

---

## 17. Service Authorization Architecture

In an offline ad-hoc environment, a single node may host multiple independent service instances (e.g., local AI triage, topography GIS, sensor telemetry).

> **ARCHITECTURAL INVARIANT:**  
> **A service MUST NOT become trusted merely because its provider node is trusted.**

```text
+--------------------------------------------------------------------------+
|                              NODE IDENTITY                               |
|                (Physical Device Host: ed25519:HostNodeA)                 |
+--------------------------------------------------------------------------+
                                     │
                                     ▼ hosts
+--------------------------------------------------------------------------+
|                            SERVICE INSTANCE                              |
|           (Ephemeral Runtime Process: UUID: 3f8a-bc12-...)               |
+--------------------------------------------------------------------------+
                                     │
                                     ▼ belongs to / implements
+--------------------------------------------------------------------------+
|                            SERVICE IDENTITY                              |
|             (Semantic Contract URI: fieldnet://services/gis/topo)        |
+--------------------------------------------------------------------------+
```

### 17.1 Granular Service Capabilities
*   **`service.use`:** Calling permission. Allows a node or application to invoke service RPC methods.
*   **`service.host`:** Advertising permission. Authorizes a node to register the service into the mesh Service Resolver.
*   **`service.admin`:** Administrative permission. Authorizes reconfiguring service parameters, quotas, or worker pools.

### 17.2 Multi-Tier Authorization Interaction
When Node C invokes `fieldnet://services/gis/topo` on Node A:
1.  Node C must hold `service.use` scoped to `fieldnet://services/gis/topo`.
2.  Node A must hold `service.host` issued by the environment authority.
3.  The service binary must match the hash declared in the publisher's signed `VerifiedServiceManifest`.

---

## 18. Application Authorization & WASM Sandbox Integration

Guest applications running on FIELDNET are untrusted third-party code. They execute inside a strict **WebAssembly sandbox** (Wasmtime) and are isolated from the host OS:

```text
+--------------------------------------------------------------------------+
|                       UNTRUSTED APPLICATION (WASM)                       |
|   (Zero direct access to host OS, kernel, sockets, filesystem, or memory)|
+--------------------------------------------------------------------------+
                                     │
                         Capability-Gated Syscalls
                                     │
                                     ▼
+--------------------------------------------------------------------------+
|                        ENVIRONMENT API GATEWAY                           |
|  1. Intercepts host function calls (`storage_put`, `network_send`).      |
|  2. Inspects application's CapabilityToken.                              |
|  3. Validates resource quotas (CPU instructions, memory bounds).         |
|  4. FAILS CLOSED: Returns security fault if unauthorized.                |
+--------------------------------------------------------------------------+
```

### 18.1 Least-Privilege Application Profiles

| Application Profile | Permitted Capability Grants | Explicitly Prohibited Capabilities |
| :--- | :--- | :--- |
| **Field Messaging App** | `network.send`, `content.read`, `content.write` | `gateway.use`, `compute.execute`, `admin.*` |
| **Map Viewer App** | `content.read`, `storage.read` | `network.send` (broadcast), `identity.export` |
| **AI Inference Worker** | `compute.execute`, `content.read`, `content.write` | `gateway.use`, `network.send` (unconstrained) |
| **System Diagnostics** | `telemetry.read`, `health.inspect` | `storage.write` (system tables), `key.export` |

---

## 19. Cryptographic Key Rotation & Identity Continuity

Nodes rotate keys without breaking historical CRDT document continuity or workspace membership:

```text
[ ACTIVE IDENTITY: Key_Old ]
            │
            │ 1. Generate new Ed25519 keypair: Key_New (KeyVersion = N + 1)
            ▼
[ GENERATE MIGRATION CERTIFICATE ]
   - Old Public Key: pk_Old
   - New Public Key: pk_New
   - Key Version: N + 1
   - Migration Timestamp: UTC epoch ms
   - Signature_Old = Sign(sk_Old, MigrationData)
   - Signature_New = Sign(sk_New, MigrationData)
            │
            │ 2. Broadcast Migration Certificate to Mesh via DTN
            ▼
[ REMOTE PEER VERIFICATION ]
   - Remote node verifies Signature_Old using stored trusted pk_Old.
   - Remote node verifies Signature_New using advertised pk_New.
   - Remote node validates monotonic sequence (KeyVersion > current_version).
            │
            │ 3. Update Peer Trust Record
            ▼
[ TRUST CONTINUITY PRESERVED ]
   - PeerTrustRecord updated: PublicKey = pk_New, KeyVersion = N + 1.
   - Historical CRDT documents authored by pk_Old remain cryptographically valid.
```

*   **Planned Rotation:** Dual-signed certificate automatically updates peer trust records.
*   **Compromise Rotation:** If $SK_{\text{old}}$ was stolen, migration certificates are untrusted. Mandatory out-of-band re-pairing (QR/SAS) is required.

---

## 20. Revocation Semantics: Identity vs. Capability

Revocation is strictly categorized into global destructive revocation and granular capability revocation:

```text
+------------------------------------+-------------------------------------+
|        IDENTITY REVOCATION         |        CAPABILITY REVOCATION        |
+------------------------------------+-------------------------------------+
| Scope: Global to the Node          | Scope: Targeted to a Single Action  |
| Impact: Total severance            | Impact: Fine-grained access denial  |
| Sessions: All sessions terminated  | Sessions: Sessions remain active    |
| Capabilities: All grants wiped     | Capabilities: Only target token lost|
| Permanence: Strictly Irreversible  | Permanence: Can be re-granted later |
+------------------------------------+-------------------------------------+
```

### 20.1 Blast Radius of Identity Revocation
When a node is revoked via verified `RevocationDeclaration`:
*   **Transport & Sessions:** All open sockets severed; symmetric session keys zeroized.
*   **Capabilities:** All issued and received tokens marked `REVOKED` in SQLite.
*   **Services:** Delisted from Service Resolver; active invocations canceled.
*   **Storage & Content:** Banned from requesting or uploading chunks.
*   **Compute:** Dispatched tasks aborted; results discarded.
*   **Gateway:** Internet egress proxying denied immediately.

---

## 21. Session vs. Identity vs. Transport Decoupling

$$\mathbf{Node\ Identity} \neq \mathbf{Session\ Identity} \neq \mathbf{Transport\ Connection}$$

*   **One Node Identity $\to$ Many Sessions:** Control session, bulk file sync session, and telemetry session run concurrently under the same sovereign `NodeId`.
*   **One Session $\to$ Many Transports:** Session migrates seamlessly across Wi-Fi Direct, Bluetooth LE, and Ethernet without resetting AEAD state.
*   **Many Transports $\to$ One Logical Peer:** Multiple physical radio links between the same devices resolve to a single persistent `PeerTrustRecord`.

---

## 22. Identity & Content Relationship

$$\mathbf{Content\ Identity\ (CID)} \neq \mathbf{Node\ Identity}$$

*   **Content Addressing:** $\text{CID} \equiv \text{SHA-256}(\text{PayloadBytes})$.
*   **Decoupled Provenance:** Intermediary nodes relay or store content without becoming its owner. Authenticity is proven via author signatures on separate `ContentManifest` structures, strictly separated from immutable content hashing.

---

## 23. Compromised Node & Physical Security Model

### 23.1 Boundary Matrix under Physical Node Compromise

| Property | Cryptography Protects It? | What FIELDNET Guarantees | What Cannot Be Guaranteed |
| :--- | :--- | :--- | :--- |
| **Past Traffic** | **YES** | Forward Secrecy prevents historical decryption. | If attacker compromised memory while session was live. |
| **Future Messages**| **PARTIALLY** | Revocation stops new sessions once propagated. | Traffic sent before revocation reaches all peers. |
| **Transit Data** | **YES** | End-to-end payloads cannot be decrypted by relay. | Attacker relay can drop packets physically. |
| **Local Stored Data**| **YES** | Platform enclaves protect keys at rest. | If host OS has no screen lock / unencrypted flash. |

---

## 24. Security vs. Availability: The Physical Limits of Cryptography

A foundational architectural requirement of FIELDNET is acknowledging the boundary between mathematical security and physical availability:

```text
+--------------------------------------------------------------------------+
|                     WHAT CRYPTOGRAPHY CAN GUARANTEE                      |
+--------------------------------------------------------------------------+
|  1. Confidentiality : Eavesdroppers cannot read encrypted payloads.       |
|  2. Authenticity    : Forged messages and identities are rejected.       |
|  3. Integrity       : Modified or corrupted bits are detected and dropped.|
|  4. Authorization   : Unauthorized actions are blocked (fail closed).    |
+--------------------------------------------------------------------------+
                                     ≠
+--------------------------------------------------------------------------+
|                   WHAT CRYPTOGRAPHY CANNOT GUARANTEE                     |
+--------------------------------------------------------------------------+
|  1. Physical Delivery : A rogue relay can silently drop wireless frames. |
|  2. Link Availability : Physical RF jammers can degrade wireless SNR.    |
|  3. Node Cooperation  : A malicious node can refuse to share its battery. |
|  4. Partition Healing : Partitions cannot heal without physical link.    |
+--------------------------------------------------------------------------+
```

> **ARCHITECTURAL MANDATE:**  
> **FIELDNET must never claim that cryptography guarantees message delivery or network availability.** Delivery in contested environments is addressed via multi-path routing, store-carry-forward DTN queues, and alternative transport fallbacks.

---

## 25. Rate Limiting, Quotas & Abuse Control Architecture

To prevent Denial of Service (DoS) and battery exhaustion in constrained environments, the runtime enforces multi-layered abuse controls:

```text
+-----------------------+----------------------+-------------------------------------------+
| Dimension             | Protective Mechanism | Default Architectural Threshold           |
+-----------------------+----------------------+-------------------------------------------+
| **Auth Attempts**     | Token Bucket + Delay | Max 5 handshakes/sec per peer; exp backoff|
| **Discovery Beacons** | Bounded LRU Cache    | Capped at 256 unauthenticated nodes; 2 min|
| **Message Injection** | Queue Quotas         | Max 50 msgs/sec control; max 5 MB/sec bulk|
| **Service Calls**     | Concurrency Limiter  | Max 8 concurrent calls per remote service |
| **Content Chunks**    | In-Flight Window     | Max 8 chunks in-flight per peer connection|
| **Compute Tasks**     | WASM Gas Counters    | CPU instruction limit & memory execution cap|
| **Gateway Egress**    | Bandwidth Bucket     | Strict MB/hour quota per capability token |
+-----------------------+----------------------+-------------------------------------------+
```

---

## 26. Security Failure Behavior (Fail-Closed Matrix)

All security checks across the runtime must execute under a strict **fail-closed** discipline:

```text
+------------------------------------+-------------------------------------+-------------------------------------+
| Security Event / Failure Condition | Immediate Runtime Action            | Security Audit Log                  |
+------------------------------------+-------------------------------------+-------------------------------------+
| **Authentication Failure**         | Abort handshake; tear down socket.  | `authentication.failure (WARN)`     |
| **Authorization Failure**          | Reject operation; return denied.    | `authorization.denied (WARN)`       |
| **Invalid Frame MAC / Signature**  | Drop frame immediately.             | `frame.corrupted (SECURITY_CRIT)`   |
| **Expired Message (TTL)**          | Drop packet; do not forward.        | `message.expired (INFO)`            |
| **Replayed Sequence Counter**      | Drop packet; log duplicate counter. | `message.replay_detected (WARN)`    |
| **Corrupted Content Chunk**        | Discard chunk; penalize peer score. | `content.integrity_failed (WARN)`   |
| **Revoked Peer Interaction**       | Sever link; drop all traffic.       | `peer.revoked_interaction (CRIT)`   |
| **Unknown Capability Requested**   | Deny by default; return fault.      | `authorization.denied (WARN)`       |
+------------------------------------+-------------------------------------+-------------------------------------+
```

---

## 27. Security Audit Event Taxonomy & Logging Architecture

Security-relevant events are dispatched across the runtime's internal Event Bus and recorded in a persistent, append-only SQLite audit log.

### 27.1 Event Catalog

```text
+----------------------------+----------------------+-------------------+----------------------+
| Event Identifier           | Emitting Subsystem   | Severity Level    | Persistence Required |
+----------------------------+----------------------+-------------------+----------------------+
| `authentication.success`   | Network / Security   | `Info`            | Optional (Telemetry) |
| `authentication.failure`   | Network / Security   | `Warn`            | Mandatory (Durable)  |
| `authorization.granted`    | Security / Auth      | `Info`            | Mandatory (Durable)  |
| `authorization.denied`     | Security / Auth      | `Warn`            | Mandatory (Durable)  |
| `capability.granted`       | Security / Auth      | `Info`            | Mandatory (Durable)  |
| `capability.revoked`       | Security / Auth      | `SecurityCrit`    | Mandatory (Durable)  |
| `session.created`          | Session Manager      | `Info`            | Ephemeral (RAM)      |
| `session.closed`           | Session Manager      | `Info`            | Ephemeral (RAM)      |
| `session.rekeyed`          | Session Manager      | `Info`            | Ephemeral (RAM)      |
| `identity.rotated`         | Identity Subsystem   | `SecurityCrit`    | Mandatory (Durable)  |
| `identity.revoked`         | Identity Subsystem   | `SecurityCrit`    | Mandatory (Durable)  |
| `peer.suspended`           | Security / Policy    | `Warn`            | Mandatory (Durable)  |
| `peer.revoked`             | Security / Identity  | `SecurityCrit`    | Mandatory (Durable)  |
+----------------------------+----------------------+-------------------+----------------------+
```

### 27.2 Audit Record Structure
Every persistent audit log entry encapsulates:
*   `EventId`: Monotonic sequence `u64`.
*   `Timestamp`: UTC epoch millisecond timestamp.
*   `EventName`: String identifier from catalog.
*   `Severity`: `Info` | `Warn` | `SecurityCritical`.
*   `SourceSubsystem`: Subsystem enum.
*   `CorrelationId`: UUID tracing the operation request across subsystems.
*   `Principal`: `NodeId` or `AppId`.
*   `Details`: Structured JSON metadata.
*   **Privacy & Key Protection Rule:** Audit logs must **NEVER** contain private key bytes, secret seeds, symmetric session keys, or unencrypted user message payloads.

---

## 28. Master Security Architecture Diagram & Boundaries

```text
+--------------------------------------------------------------------------+
|                             APPLICATION LAYER                            |
|             (Sandboxed WASM, Desktop Tauri UI, Mobile Shell)             |
+--------------------------------------------------------------------------+
                                     │
                                     ▼ invokes API
+══════════════════════════════════════════════════════════════════════════+
|                     CAPABILITY ENFORCEMENT BOUNDARY                      |
|  - Intercepts calls; validates Principal, CapabilityToken, and Scopes    |
|  - FAILS CLOSED before any runtime subsystem executes                    |
+══════════════════════════════════════════════════════════════════════════+
                                     │
                                     ▼ dispatches authorized call
+--------------------------------------------------------------------------+
|                        FIELDNET RUNTIME SECURITY                         |
+--------------------------------------------------------------------------+
               ↙                                            ↘
+-----------------------------+              +-----------------------------+
|     IDENTITY SUBSYSTEM      |              |       SESSION MANAGER       |
| - Sovereign Ed25519 Keys    |              | - Active Secure Sessions    |
| - Platform Keystore Adapter |              | - Ephemeral X25519 DH Keys  |
| - Rotation / Revocation     |              | - Directional AEAD Ciphers  |
+-----------------------------+              +-----------------------------+
               │                                            │
               ▼                                            ▼
+-----------------------------+              +-----------------------------+
|         TRUST STORE         |              |        SESSION KEYS         |
| - Persistent SQLite Table   |              | - ChaCha20-Poly1305 Keys    |
| - PeerTrustRecord Registry  |              | - Replay Filters & Counters |
| - Capability Token ACLs     |              | - Volatile in Zeroized RAM  |
+-----------------------------+              +-----------------------------+
               ↘                                            ↙
+══════════════════════════════════════════════════════════════════════════+
|                       TRANSPORT SECURITY BOUNDARY                        |
|  - Authenticated AEAD Frames (Poly1305 MAC, Sequence Numbers, Nonces)    |
|  - Encrypted Link Payloads (ChaCha20)                                    |
+══════════════════════════════════════════════════════════════════════════+
                                     │
                                     ▼ delivers frames
+--------------------------------------------------------------------------+
|                    PLATFORM TRANSPORTS (TCP, BLE, WI-FI)                 |
+--------------------------------------------------------------------------+
                                     │
                                     ▼ physical radio transmission
+--------------------------------------------------------------------------+
|                              REMOTE PEER NODE                            |
+--------------------------------------------------------------------------+

+══════════════════════════════════════════════════════════════════════════+
|                       PRIVATE KEY ISOLATION SHIELD                       |
|  - Ed25519 Private Key is strictly confined to local platform enclave    |
|  - ZERO exposure to applications, ZERO network transmission              |
+══════════════════════════════════════════════════════════════════════════+
```

---

## 29. Rust Implementation Blueprint (Architectural Component Contracts)

Without implementing production cryptographic code, the following specifications define the 7 core components required for subsequent engineering phases:

### 1. `IdentityStore`
*   **Responsibility:** Sovereign keypair management, generation, platform secure keystore integration, atomic reading.
*   **Owned State:** Local `NodeId`, `PublicKey`, encrypted keystore handles, key version sequence.
*   **Dependencies:** `PlatformAdapter` (Keychain/Keystore), CSPRNG (`getrandom`).
*   **Public Interface Responsibilities:** `get_node_id()`, `sign_manifest()`, `create_migration_cert()`, `rotate_key()`.

### 2. `TrustStore`
*   **Responsibility:** Durable persistence of peer trust relationships, pairing records, and capability tokens.
*   **Owned State:** SQLite connection, `PeerTrustRecord` index, `RevocationIndex`.
*   **Dependencies:** `StorageSubsystem` (SQLite), Event Bus.
*   **Public Interface Responsibilities:** `get_peer_trust()`, `set_peer_state()`, `record_pairing()`, `blacklist_peer()`.

### 3. `SessionManager`
*   **Responsibility:** Management of active encrypted sessions, transport binding, and rekeying timers.
*   **Owned State:** In-memory map of `SessionId` $\to$ `SecureSession`, peer-to-session routing index.
*   **Dependencies:** `KeyManager`, `NetworkSubsystem`, Tokio runtime.
*   **Public Interface Responsibilities:** `initiate_session()`, `handle_handshake_frame()`, `encrypt_frame()`, `decrypt_frame()`, `close_session()`.

### 4. `KeyManager`
*   **Responsibility:** Cryptographic calculations, ephemeral Diffie-Hellman operations, HKDF expansion, and memory zeroization.
*   **Owned State:** Volatile ephemeral key cache wrapped in `ZeroizeOnDrop` buffers.
*   **Dependencies:** Audited crypto crates (`ed25519-dalek`, `x25519-dalek`, `chacha20poly1305`, `hkdf`).
*   **Public Interface Responsibilities:** `generate_ephemeral_dh()`, `derive_session_keys()`, `verify_signature()`.

### 5. `CapabilityManager`
*   **Responsibility:** Parsing, serializing, signing, and validating capability tokens and constraints.
*   **Owned State:** Local capability grant cache, active token table.
*   **Dependencies:** `IdentityStore`, `StorageSubsystem`.
*   **Public Interface Responsibilities:** `issue_token()`, `validate_token()`, `revoke_token()`, `is_expired()`.

### 6. `AuthorizationEngine`
*   **Responsibility:** Executing the 7-step authorization pipeline on every Environment API invocation.
*   **Owned State:** In-memory authorization evaluation rules and quota trackers.
*   **Dependencies:** `TrustStore`, `CapabilityManager`, `SessionManager`, `SecurityEventRecorder`.
*   **Public Interface Responsibilities:** `authorize_call(principal, action, resource) -> Result<Decision>`.

### 7. `SecurityEventRecorder`
*   **Responsibility:** Auditing all security-relevant events, filtering secrets, and persisting audit records.
*   **Owned State:** Append-only audit table transaction handle, Event Bus publisher.
*   **Dependencies:** `StorageSubsystem`, `EventBus`.
*   **Public Interface Responsibilities:** `record_event(event) -> Result<()>`.

---

## 30. Authoritative Security Invariants

The following 18 authoritative security invariants govern all subsequent implementation phases:

1.  **Private Keys Never Leave Their Node:** Sovereign private cryptographic keys are generated locally, stored in platform secure storage, and must never be transmitted across any network, transport, radio, or inter-process interface.
2.  **Private Keys Are Never Exposed to Applications:** Guest WASM applications and UI code have zero direct access to private key bytes or secret seeds.
3.  **Discovery Never Implies Trust:** Intercepting a radio beacon conveys zero proof of authenticity; discovered peers remain strictly unauthenticated.
4.  **Authentication Never Implies Unrestricted Authorization:** Cryptographically verifying peer identity proves who they are, but grants zero access to local services, storage, or routing without explicit capability tokens.
5.  **Authorization Is Capability-Based:** Privileged operations require unforgeable, cryptographically signed capability tokens constrained by scope, quotas, and expiration.
6.  **Authorization Fails Closed:** Any operation lacking an explicit, verifiable, non-expired, unrevoked capability grant must be rejected immediately (`deny-by-default`).
7.  **Transport Identity Is Separate from Node Identity:** IP addresses, MAC addresses, and BLE UUIDs carry zero trust authority; switching physical radios preserves sovereign peer identity.
8.  **Session Keys Are Separate from Node Identity Keys:** Long-term identity keys must never directly encrypt application messages; session encryption requires ephemeral keys.
9.  **Revoked Peers Cannot Perform Newly Authorized Operations:** Any node marked `REVOKED` is barred from establishing authorized sessions or executing privileged operations.
10. **Replay Protection Is Mandatory:** Monotonically increasing sequence counters and sliding-window replay filters must drop duplicate or out-of-window frames.
11. **Expired Messages Are Rejected:** Frames or messages whose expiration TTL has elapsed must be discarded without processing or forwarding.
12. **Cryptographic Primitives Are Never Implemented from Scratch:** FIELDNET strictly uses mature, audited, peer-reviewed cryptographic libraries.
13. **Security Does Not Imply Guaranteed Delivery:** Cryptography guarantees confidentiality, integrity, and authenticity, but cannot guarantee radio transmission or overcome physical jamming.
14. **Malicious Relays Cannot Bypass End-to-End Protection:** Intermediary forwarding nodes in multi-hop paths cannot read AEAD ciphertext or forge digital signatures.
15. **Node Identity Is Stable Across Normal Restarts:** Normal shutdowns, host reboots, and crash recoveries preserve sovereign node identity without alteration.
16. **Node Authorization Does Not Automatically Authorize Every Service:** Authorizing a peer node does not authorize hosted services; service calls require service-specific capability grants.
17. **Capability Revocation Can Occur Independently of Node Identity:** Individual capabilities can be revoked or suspended without destroying peer authentication or basic messaging trust.
18. **Identity Changes Require Explicit Continuity Rules:** Key rotation requires dual-signed migration certificates; compromise scenarios mandate out-of-band re-pairing.

---

## 31. Cryptographic Library Policy

FIELDNET enforces an absolute prohibition against implementing cryptographic algorithms from scratch:

> **ARCHITECTURAL MANDATE:**  
> **FIELDNET MUST NOT IMPLEMENT CRYPTOGRAPHIC PRIMITIVES FROM SCRATCH.**

### Approved Algorithmic Roles & Candidate Pure-Rust Libraries
*   **Digital Signatures & Identity:** `Ed25519` (Candidate: `ed25519-dalek` / `ring`).
*   **Key Agreement & Ephemeral Handshake:** `X25519` (Candidate: `x25519-dalek`).
*   **Authenticated Encryption (AEAD):** `ChaCha20-Poly1305` (Candidate: `chacha20poly1305` / `ring`).
*   **Cryptographic Hashing & Content Addressing:** `SHA-256` (Candidate: `sha2` / `ring`).
*   **Key Derivation Function (KDF):** `HKDF-SHA256` (Candidate: `hkdf`).
*   **Cryptographic Randomness:** Hardware-backed CSPRNG (Candidate: `getrandom` / `rand_core`).
*   **Memory Zeroization:** Constant-time zeroing of sensitive buffers (Candidate: `zeroize`).

---

## 32. Phase 03 Verification Checklist & Master Spec Audit

### Prompt 01 Audit
- [x] 13 Core Security Objectives defined.
- [x] Comprehensive 23-Threat Model established.
- [x] Trust progression baseline codified.
- [x] Sovereign Node Identity model defined.
- [x] Authenticated key agreement architecture specified.
- [x] Authentication vs. Authorization demarcation codified.
- [x] Capability security baseline defined.
- [x] Cryptographic library policy codified.

### Prompt 02 Audit
- [x] 8-stage Node Identity Lifecycle codified.
- [x] Identity data structures and restart stability specified.
- [x] Platform keystore adapters and private key containment codified.
- [x] 7 Identity States and state machine defined.
- [x] Peer Trust Record vs. Discovery Record separation codified.
- [x] 6-stage Trust Model and automated policy hooks defined.
- [x] User-controlled pairing models (QR, SAS, Local, NFC) specified.
- [x] Capability scoping and 5 lifecycle states defined.
- [x] Trust change propagation via Event Bus specified.
- [x] Key rotation and dual-signed migration certificates defined.
- [x] Revocation blast radius and semantics codified.
- [x] Multi-transport trust binding and identity decoupling codified.

### Prompt 03 Audit
- [x] 9-stage Secure Session lifecycle (`DISCOVERED` $\to$ `CLOSED`) codified.
- [x] Explicit layer separation (Transport, Discovery, Connection, Auth, Key Agreement, Session, Authz) defined.
- [x] 9 Session Security Properties (auth, confidentiality, integrity, replay, freshness, PFS, key separation, expiration, explicit close) guaranteed.
- [x] 4-tier Key Hierarchy and justification for non-encryption by long-term keys codified.
- [x] Session lifecycle states and 7 transition triggers defined.
- [x] Transport migration and multipath continuity without re-authentication codified.
- [x] Message authentication, verification, and 7-scenario attack detection matrix established.
- [x] Formal capability-based authorization calculus $\langle P, C, S, R, A, I, E, V \rangle$ defined.
- [x] 7-step Authorization Pipeline placed at the Environment API boundary before execution.
- [x] Granular service authorization (`service.use`, `service.host`, `service.admin`) and dual-tier model codified.
- [x] Least-privilege application profiles for WASM sandbox defined.
- [x] 13-event Security Audit catalog, severity, correlation IDs, and zero-leakage rules codified.
- [x] Fail-closed security failure behavior matrix established.
- [x] Multi-dimensional Rate Limiting & Abuse Control architecture specified.
- [x] Security vs. Availability demarcation (physical limits of cryptography) established.
- [x] End-to-end Master Security Architecture Diagram with private key isolation created.
- [x] 7-component Rust Implementation Blueprint defined.
- [x] 18 Authoritative Security Invariants finalized.
- [x] Full consistency with `FIELDNET_MASTER_SPEC.md` verified.
