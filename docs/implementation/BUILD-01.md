# FIELDNET — BUILD-01 Implementation Specification
**Milestone:** Real Runtime Foundation & Two-Node Secure Message Exchange  
**Status:** COMPLETE  
**Architecture Compliance:** Strict adherence to Phase 01–05 Architecture Documents (`docs/architecture/01-system-model.md` through `docs/architecture/05-protocol-routing-dtn.md`).

---

## 1. Overview & Goal

BUILD-01 transitions FIELDNET from formal architectural specifications to a fully working, real Rust distributed runtime.

The milestone proves:
> **Two independent, real FIELDNET nodes running on separate operating system processes, discover each other over real local network sockets without any centralized infrastructure, execute mutual Ed25519/X25519 authentication, establish a ChaCha20-Poly1305 AEAD encrypted session, transmit an authenticated DATA message, verify cryptographic signatures, acknowledge receipt with an ACK, and record delivery confirmation.**

There are:
- **NO central servers, coordinators, or cloud dependencies**
- **NO simulated, mock, or in-memory virtual network transports**
- **NO fake peers or simulated network delays**

---

## 2. Workspace & Crate Structure

The repository is organized as a Cargo workspace with five focused, modular crates:

```
crates/
├── fieldnet-core/       # Primitive types: NodeId, MessageId, SessionId, Timestamp, Priority, CoreError
├── fieldnet-identity/   # Sovereign Ed25519 node identities, disk persistence, and signatures
├── fieldnet-protocol/   # Framing-independent message format, serialization, verification, deduplication, ACK
├── fieldnet-network/    # Real UDP discovery, TCP transport framing, mutual handshake, ChaCha20 AEAD sessions
└── fieldnet-runtime/    # Node runtime coordination, connection lifecycle, CLI executable (fieldnet-node)
```

### Crate Descriptions

1. **`fieldnet-core`**
   - [`NodeId`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-core/src/node_id.rs): Sovereign 32-byte cryptographic identifier derived via `BLAKE3(ed25519_verifying_key)`.
   - [`MessageId`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-core/src/message_id.rs): Content-derived 32-byte identifier derived via `BLAKE3(source, sequence, created_at, payload)`.
   - [`SessionId`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-core/src/types.rs): 16-byte link-layer session identifier.
   - [`Timestamp`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-core/src/types.rs): Unix epoch millisecond timestamps with saturation and expiration helpers.

2. **`fieldnet-identity`**
   - [`NodeIdentity`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-identity/src/identity.rs): Manages persistent Ed25519 keypairs using audited `ed25519-dalek = "2.1"`.
   - Supports local disk JSON persistence (`identity.json`).
   - Private key material is never logged and never exposed in `Debug` implementations.

3. **`fieldnet-protocol`**
   - [`Message`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-protocol/src/message.rs): Core wire envelope containing `version`, `message_id`, `message_type`, `source`, `destination`, `created_at`, `expires_at`, `ttl`, `sequence`, `priority`, `payload`, `auth_tag`, and `session_id`.
   - Origination signatures: Digital signatures are computed over canonical immutable signing bytes (excluding mutable hop fields like `ttl` and `session_id`).
   - [`Deduplicator`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-protocol/src/dedupe.rs): O(1) deduplication table with bounded capacity.
   - [`AckPayload`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-protocol/src/ack.rs): Structured acknowledgment records (`AckStatus::Received`).

4. **`fieldnet-network`**
   - [`DiscoveryService`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-network/src/discovery.rs): Real UDP multicast (`239.255.42.99:4242`), loopback, and broadcast beaconing with `SO_REUSEPORT` / `SO_REUSEADDR`.
   - [`framing`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-network/src/framing.rs): 4-byte big-endian length-delimited frame reader/writer over Tokio streams.
   - [`session`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-network/src/session.rs): Mutual Ed25519 authentication, ephemeral X25519 Diffie-Hellman key agreement, BLAKE3 KDF, ChaCha20-Poly1305 AEAD, and monotonic sequence counter replay protection.
   - [`ConnectionManager`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-network/src/connection.rs): Manages TCP listeners, outbound client connects, concurrent I/O task splitting, and session deduplication.

5. **`fieldnet-runtime`**
   - [`FieldnetRuntime`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-runtime/src/runtime.rs): Central node coordinator binding identity, discovery, connection manager, deduplication, and message dispatch.
   - [`main.rs`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-runtime/src/main.rs): `fieldnet-node` CLI binary.

---

## 3. Cryptographic & Protocol Architecture

### 3.1 Mutual Authentication & Handshake Flow

FIELDNET avoids home-grown cryptography and utilizes mature, audited libraries: `ed25519-dalek`, `x25519-dalek`, `chacha20poly1305`, and `blake3`.

```
Initiator (A)                                                     Responder (B)
     |                                                                  |
     | ----- 1. HandshakeMessage::Init -------------------------------> |
     |      { A_node_id, A_ed25519_pubkey, A_ephemeral_x25519, nonceA }|
     |                                                                  |
     |                                                      Verify:     |
     |                                                      BLAKE3(A_pubkey) == A_node_id
     |                                                                  |
     | <---- 2. HandshakeMessage::Response ---------------------------- |
     |      { B_node_id, B_ed25519_pubkey, B_ephemeral_x25519, nonceB,  |
     |        signature: Ed25519_Sign_B(A_x25519 || B_x25519 ||         |
     |                                  nonceA || nonceB || "RESPONDER")}|
     |                                                                  |
Verify:                                                                 |
BLAKE3(B_pubkey) == B_node_id                                           |
Ed25519_Verify_B(...)                                                   |
     |                                                                  |
     | ----- 3. HandshakeMessage::Complete ---------------------------> |
     |      { signature: Ed25519_Sign_A(B_x25519 || A_x25519 ||         |
     |                                  nonceB || nonceA || "INITIATOR")}|
     |                                                                  |
     |                                                      Verify:     |
     |                                                      Ed25519_Verify_A(...)
     |                                                                  |
Key Derivation (both nodes):                                            |
Shared ECDH Secret = X25519(ephemeral_secret, peer_ephemeral_pubkey)    |
KDF = BLAKE3_DeriveKey("FIELDNET_BUILD01_SESSION_KDF", secret, nonces)  |
Keys: tx_key (32B), rx_key (32B), session_id (16B)                     |
AEAD: ChaCha20-Poly1305                                                 |
```

### 3.2 Secure Session Encrypted Wire Format

Frames transmitted over TCP sockets use 4-byte big-endian framing:

```
+-------------------+--------------------+---------------------------------------+
| Frame Length (4B) | Nonce (12 Bytes)   | Ciphertext + Poly1305 Tag (N Bytes)   |
| Big-Endian u32    | [0..8]: Counter    | ChaCha20 Encrypted Payload            |
|                   | [8..12]: Reserved  | + 16-byte Poly1305 MAC                |
+-------------------+--------------------+---------------------------------------+
```

Replay protection is enforced on every frame by ensuring the received 8-byte counter is strictly monotonic.

### 3.3 Simultaneous Connection Tie-Breaking

To prevent connection collisions when both nodes discover each other simultaneously:
- **Rule:** The node with the lexicographically smaller `NodeId` acts as the connection initiator.
- The node with the higher `NodeId` records the peer in its discovery table and awaits the incoming TCP connection.
- If redundant connections occur, session-ID affinity ensures only the active, verified session is retained.

---

## 4. Message Lifecycle

1. **Sender (`Node A`)**:
   - Computes unique `MessageId = BLAKE3(A, seq, timestamp, payload)`.
   - Assembles `Message` with `ttl = 10` and `expires_at = now + 120s`.
   - Originator computes Ed25519 digital signature over canonical immutable bytes.
   - Encrypts via ChaCha20-Poly1305 with session transmit key.
   - Transmits framed ciphertext over real TCP socket.
   - Records delivery state: `MessageDeliveryStatus::Sent`.

2. **Receiver (`Node B`)**:
   - Reads length-prefixed frame from TCP socket.
   - Decrypts and authenticates via ChaCha20-Poly1305 with session receive key.
   - Enforces monotonic counter replay protection.
   - Deserializes JSON message envelope.
   - Checks expiration (`message.is_expired()`).
   - Verifies sender identity matches connection session peer.
   - Queries `Deduplicator` to detect duplicate delivery.
   - Verifies Ed25519 digital signature against Node A's public key.
   - Logs message and adds to received history.
   - Creates and signs an `ACK` message with `AckStatus::Received` for the specific `MessageId`.
   - Transmits encrypted `ACK` back to Node A over the established secure session.

3. **Sender Delivery Confirmation (`Node A`)**:
   - Decrypts `ACK` frame.
   - Deserializes `AckPayload`.
   - Updates delivery status: `Sent` -> `AckReceived { ack_at, status: Received }`.
   - Emits delivery notification event.

---

## 5. Verification & Test Results

### 5.1 Automated Unit & Integration Tests

Run all tests across the workspace:
```bash
cargo test --workspace
```

Output:
```
running 2 tests (fieldnet-core)
test node_id::tests::test_node_id_derivation_and_hex ... ok
test message_id::tests::test_message_id_compute_and_serde ... ok

running 2 tests (fieldnet-identity)
test identity::tests::test_identity_persistence_and_reload ... ok
test identity::tests::test_sign_and_verify ... ok

running 3 tests (fieldnet-network)
test framing::tests::test_framing_roundtrip ... ok
test session::tests::test_secure_session_handshake_and_encryption ... ok
test session::tests::test_handshake_authentication_failure_invalid_signature ... ok

running 3 tests (fieldnet-protocol)
test dedupe::tests::test_deduplicator ... ok
test codec::tests::test_codec_roundtrip ... ok
test message::tests::test_message_sign_verify_and_freshness ... ok

running 1 test (fieldnet-runtime integration)
test test_two_real_nodes_secure_message_exchange ... ok

test result: ok. All tests passed cleanly (0 failed, 0 ignored).
```

### 5.2 Two-Process Live Demonstration

Two independent OS processes can be launched concurrently in separate terminals:

#### Terminal 1 (Node A):
```bash
cargo run --bin fieldnet-node -- --data-dir ./node-a --tcp-port 54201 --discovery-port 4995
```

#### Terminal 2 (Node B):
```bash
cargo run --bin fieldnet-node -- --data-dir ./node-b --tcp-port 54202 --discovery-port 4995 --auto-send --message "Hello from Node B Process"
```

#### Observed Execution Output:
```
Node A:
  INFO Starting FIELDNET Runtime (data_dir: /tmp/fieldnet_node_a_demo)
  INFO Local Node ID: bc97fbc35db1422913fcc3f6ed3cae84d54e216edd2c851e62ea90ed3406bf30
  INFO TCP Listener bound to 0.0.0.0:54201
  INFO Discovery service started on port 4995 (advertising TCP port 54201)
  INFO Mutual authentication successful for incoming peer 0b8ddbbbc3f12b9cf394b3c48c24983addf986166c31ceea7734fb4bed198325 (session: 4eee1978b52d3189c18f87a8a78a2568)
  INFO DATA message b53bafefed20caa8a93f6dbaff20f819aa3be20829b2e7db387870c2a627cf4d verified and received from peer 0b8ddbbbc3f12b9cf394b3c48c24983addf986166c31ceea7734fb4bed198325 (payload len: 25 bytes)
  INFO DATA content from 0b8ddbbbc3f12b9cf394b3c48c24983addf986166c31ceea7734fb4bed198325: "Hello from Node B Process"
  INFO Sending ACK for message b53bafefed20caa8a93f6dbaff20f819aa3be20829b2e7db387870c2a627cf4d back to peer 0b8ddbbbc3f12b9cf394b3c48c24983addf986166c31ceea7734fb4bed198325

Node B:
  INFO Starting FIELDNET Runtime (data_dir: /tmp/fieldnet_node_b_demo)
  INFO Local Node ID: 0b8ddbbbc3f12b9cf394b3c48c24983addf986166c31ceea7734fb4bed198325
  INFO TCP Listener bound to 0.0.0.0:54202
  INFO Discovery service started on port 4995 (advertising TCP port 54202)
  INFO Discovery triggered auto-connect (initiator: local < remote) to peer bc97fbc35db1422913fcc3f6ed3cae84d54e216edd2c851e62ea90ed3406bf30 at 10.156.89.70:54201
  INFO Mutual authentication successful with peer bc97fbc35db1422913fcc3f6ed3cae84d54e216edd2c851e62ea90ed3406bf30 (session: 4eee1978b52d3189c18f87a8a78a2568)
  INFO Sending DATA message b53bafefed20caa8a93f6dbaff20f819aa3be20829b2e7db387870c2a627cf4d to peer bc97fbc35db1422913fcc3f6ed3cae84d54e216edd2c851e62ea90ed3406bf30 (len: 25 bytes, seq: 1)
  INFO Waiting for ACK from bc97fbc35db1422913fcc3f6ed3cae84d54e216edd2c851e62ea90ed3406bf30...
  INFO ACK received from peer bc97fbc35db1422913fcc3f6ed3cae84d54e216edd2c851e62ea90ed3406bf30 for message b53bafefed20caa8a93f6dbaff20f819aa3be20829b2e7db387870c2a627cf4d (status: Received)
  --------------------------------------------------
  SUCCESS: Delivery Verified! ACK received from peer bc97fbc35db1422913fcc3f6ed3cae84d54e216edd2c851e62ea90ed3406bf30
  Status: AckReceived { ack_at: Timestamp(1790509924175), status: Received }
  --------------------------------------------------
```

---

## 6. Known Limitations (BUILD-01 Scope Boundaries)

In accordance with Phase 05 architectural discipline:
1. **Direct Communication Only:** BUILD-01 targets direct single-hop peer communication. Multi-hop routing, forwarding tables, and DTN bundle custody transfer belong to subsequent build milestones.
2. **Local Transport Priority:** TCP over IPv4 and UDP local discovery are implemented. Bluetooth Low Energy and Wi-Fi Direct transports are defined in architecture and will be added in subsequent transport passes.
3. **Wire Format Serialization:** Serde JSON is utilized for BUILD-01 wire envelopes for transparency and debuggability. Future builds may switch to compact canonical CBOR without changing protocol semantics.
