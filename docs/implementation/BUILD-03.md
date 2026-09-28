# FIELDNET — BUILD-03 Implementation Specification
**Milestone:** Routing & DTN Integration, Fault Injection, Hardening & Final Verification  
**Status:** COMPLETE  
**Architecture Compliance:** Strict adherence to Phase 01–05 Architecture Documents (`docs/architecture/01-system-model.md` through `docs/architecture/05-protocol-routing-dtn.md`).

---

## 1. Executive Summary

BUILD-03 delivers the final hardening, fault injection, and verification pass for FIELDNET Phase 05.

It proves that the complete Phase 05 distributed networking stack operates as one coherent, fault-tolerant system under realistic network disruptions and process failures:

$$\text{Identity} \longrightarrow \text{Discovery} \longrightarrow \text{Authentication} \longrightarrow \text{Secure Session} \longrightarrow \text{Protocol} \longrightarrow \text{Routing} \longrightarrow \text{Multi-Hop Forwarding} \longrightarrow \text{Persistent DTN} \longrightarrow \text{ACK} \longrightarrow \text{Recovery}$$

Every verification scenario is tested on real operating system processes communicating over real UDP and TCP sockets with real Ed25519 and ChaCha20-Poly1305 cryptographic primitives. No mocks, simulators, or in-memory transport illusions were used.

---

## 2. Integrated System Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                        FIELDNET Application Layer                      │
│        (send_data_message, wait_for_ack, get_received_messages)        │
└────────────────────────────────────┬───────────────────────────────────┘
                                     │
┌────────────────────────────────────▼───────────────────────────────────┐
│                           Routing Engine                               │
│  - Distance-Vector routing table with 16-hop upper bound               │
│  - Autonomous next-hop resolution (destination NodeId -> next hop)     │
│  - Periodic & triggered RouteAdvertisement (MessageType::Route = 5)    │
│  - Split-horizon & loop suppression (self-route, reverse-hop guard)    │
└──────────────────┬─────────────────────────────────▲───────────────────┘
                   │                                 │
┌──────────────────▼───────────────────┐ ┌───────────┴───────────────────┐
│     Persistent DTN Queue (SQLite)    │ │       Deduplication Table     │
│  - WAL mode (dtn_queue.db)           │ │  - MessageId LRU & in-memory  │
│  - Priority: CRITICAL/HIGH/NORM/LOW  │ │    Bloom filter window        │
│  - Exponential backoff retry loop    │ │  - Prevents replay & dup      │
│  - Survives sudden process kill      │ │    delivery to application    │
└──────────────────┬───────────────────┘ └───────────────────────────────┘
                   │
┌──────────────────▼─────────────────────────────────────────────────────┐
│                       Multi-Hop Forwarding Relay                       │
│  - Intercepts transit packets destined for remote NodeIds              │
│  - Hop TTL decrement with underflow guard                              │
│  - Preserves originator Ed25519 digital signature and MessageId        │
│  - Reverse forwarding of end-to-end cryptographic ACKs                 │
└──────────────────┬─────────────────────────────────────────────────────┘
                   │
┌──────────────────▼─────────────────────────────────────────────────────┐
│                    Connection Manager & Secure Session                 │
│  - Ephemeral X25519 ECDH key exchange with BLAKE3 KDF                  │
│  - ChaCha20-Poly1305 AEAD link encryption (12-byte monotonic nonces)   │
│  - Mutual Ed25519 identity verification against BLAKE3(PubKey) NodeIds │
│  - 4-byte length-delimited TCP transport framing                       │
└──────────────────┬─────────────────────────────────────────────────────┘
                   │
┌──────────────────▼─────────────────────────────────────────────────────┐
│                        Discovery & Identity                            │
│  - Autonomous UDP broadcast / multicast beaconing (SO_REUSEPORT)       │
│  - Sovereign Ed25519 identity generation & disk persistence            │
│  - Deterministic tie-breaking (lexicographical lower NodeId initiates) │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Subsystem Behaviors & Invariants

### 3.1 Distance-Vector Routing
- **Peer Table $\neq$ Routing Table:** The peer table tracks direct link-layer TCP connections (`ConnectionManager`). The routing table tracks multi-hop logical paths across the entire reachable mesh (`RoutingTable`).
- **Authenticated Participation:** Only authenticated peers participate in routing. Unauthenticated sockets are closed during the cryptographic handshake and never affect the routing table.
- **Route Expiration:** Routes have a default TTL of 120 seconds and are automatically purged by the routing background worker. Expired routes are never selected for packet dispatch.
- **Loop Prevention:**
  - Advertisements specifying `destination == self.local_node_id` are rejected.
  - Routes with `hops >= 16` are rejected.
  - Packets are never forwarded back to the incoming peer (`next_hop == peer_id`).
  - Packets originating from `self.node_id()` arriving at the relay are dropped.

### 3.2 Persistent Delay-Tolerant Networking (DTN)
- **Queue Durability:** The queue is backed by an embedded SQLite database operating in Write-Ahead Logging (`WAL`) mode. Bundles survive sudden power loss, process SIGKILL, and OS crashes.
- **Priority Scheduling:** Bundles are retrieved ordered by `priority DESC, next_retry_at ASC`.
  - `Critical` (Priority 3)
  - `High` (Priority 2)
  - `Normal` (Priority 1)
  - `Low` (Priority 0)
- **Exponential Backoff:** Failed transmission attempts increment the retry counter and apply exponential backoff:
  $$\text{backoff} = \min(500\text{ ms} \times 2^{\text{retries}}, 60{,}000\text{ ms})$$
- **Automatic Route Recovery Flush:** Whenever a peer reconnects (`PeerConnected`) or a new route is learned, a notification immediately triggers the DTN queue runner to drain pending bundles.

### 3.3 ACK Semantics
- **Distinction of States:**
  - `Sent`: Message dispatched onto the link.
  - `Forwarded`: Transit relay forwarded packet to downstream hop.
  - `Received`: Destination received and verified message.
  - `Processed`: Application handled message.
- **End-to-End Delivery Confirmation:** The final delivery ACK from destination Node C is routed through intermediate relay B back to originator Node A. Only when A verifies this ACK does A mark the message confirmed and delete it from its SQLite queue.
- **Original Message ID Invariance:** The 32-byte `MessageId` is preserved end-to-end. Relays never re-key or change message identity.

---

## 4. Resource Safety Limits

To protect memory and disk against resource exhaustion attacks or unbounded retries, the following limits are enforced:

| Resource Parameter | Implemented Limit | Behavior on Exceeded |
|:---|:---|:---|
| **Max Queue Capacity** | 1,000 bundles | `DtnError::QueueCapacityExceeded`, enqueues rejected |
| **Max Message Size** | 1,048,576 bytes (1 MB) | `DtnError::MessageTooLarge`, payload rejected |
| **Max DTN Retries** | 10 attempts | Marked `FAILED`, purged from active dispatch loop |
| **Max Route Metric** | 16 hops | Route advertisement discarded as unreachable |
| **Transport Frame Limit**| 16 MB | Connection closed on excessive frame length |
| **Default Route TTL** | 120 seconds | Route purged if not refreshed by advertisement |

---

## 5. Security Boundaries & Zero-Leakage Guarantee

1. **Sovereign Cryptographic Identity:**
   - Identity keypairs are stored locally in `identity.json`.
   - `NodeId` is derived as $\text{BLAKE3}(\text{Ed25519PublicKey})$. Identity spoofing is mathematically infeasible without breaking Ed25519.
2. **Encrypted Transport:**
   - All TCP links are encrypted using ChaCha20-Poly1305 with keys derived from ephemeral X25519 ECDH handshakes.
   - Nonces increment monotonically per message; out-of-order nonces are rejected.
3. **Observability & Secret Hygiene:**
   - Structured tracing logs never output private keys, ephemeral ECDH secrets, session keys, or unauthenticated user plaintexts.
   - Correlation IDs (`node_id`, `message_id`, `session_id`) enable end-to-end tracing without security leakage.

---

## 6. Performance Baseline Measurements

Collected on macOS (Apple Silicon), loopback network sockets, release debuginfo profile:

| Metric | Measured Baseline | Description |
|:---|:---:|:---|
| **Peer Discovery & Mutual Auth** | **15.87 ms** | UDP discovery, TCP connect, Ed25519 handshake, session key derivation |
| **Direct 1-Hop RTT Latency** | **1.09 ms** | Node A $\to$ Node B DATA + Node B $\to$ Node A ACK |
| **Multi-Hop Route Convergence** | **505.62 ms** | Time for A to discover C via B after C comes online |
| **Multi-Hop 2-Hop RTT Latency** | **3.11 ms** | Node A $\to$ Relay B $\to$ Node C + C $\to$ B $\to$ A ACK |
| **DTN Queued Message Recovery** | **209.06 ms** | Time from C coming online to delivery of queued message |
| **Direct Message Throughput** | **937.5 msg/sec** | Sustained burst of 20 verified & acknowledged messages |

---

## 7. Verification Test Suite Summary

The Phase 05 test suite comprises 32 automated tests across unit, integration, fault injection, security hardening, and performance benchmarks:

### 7.1 Integration Test Suites
1. **`three_node_multihop_test.rs`**
   - `test_three_node_multihop_end_to_end_delivery`: Proves $A \leftrightarrow B \leftrightarrow C$ forwarding, TTL decrementing, originator signature preservation, transit isolation, and end-to-end ACK delivery.
2. **`fault_injection_and_recovery_test.rs`**
   - `test_route_failure_and_dtn_retry_recovery`: Proves route invalidation on partition, SQLite DTN staging, and automatic retry delivery upon reconnection.
   - `test_intermediate_node_crash_and_recovery`: Unexpectedly terminates intermediate relay Node B. Proves A and C remain healthy, routes via B are removed, messages are staged, and routing recovers when B restarts.
   - `test_runtime_restart_with_persisted_dtn_queue`: Enqueues message on A to offline peer, kills A, restarts A from disk. Proves Node ID identity preservation, bundle durability across process restart, and delivery to B upon connection.
3. **`security_hardening_test.rs`**
   - `test_duplicate_and_replay_rejection`: Proves deduplication drops duplicate and replayed message IDs.
   - `test_ttl_and_expiration_enforcement`: Proves TTL=0 dropped, expired messages dropped, expired routes purged.
   - `test_malformed_input_robustness`: Injects corrupt JSON routes, malformed ACKs, and tampered signatures; runtime never panics.
   - `test_routing_consistency_loop_prevention_and_diamond_topology`: Tests loop prevention, self-advertisement drop, and diamond topology shortest-path failover.
   - `test_dtn_queue_priority_and_resource_safety`: Tests Priority ordering (Critical $>$ High $>$ Normal $>$ Low), 1 MB message limit, and 10-retry cap.
4. **`performance_baseline_test.rs`**
   - `test_performance_baseline_benchmarks`: Collects deterministic performance metrics.

---

## 8. Forward Look: Phase 06 Preparation

The networking, routing, and DTN foundation is verified complete and hardened. Future phases will build upon this layer:
- **Phase 06:** Distributed state synchronization, CRDTs, and state convergence over Phase 05 DTN transport.
- **Phase 07:** Content-addressed file system and decentralized storage.
- **Phase 08:** Application runtime, local web gateway, and edge APIs.

**Strict Boundary Respected:** No Phase 06 systems (CRDTs, distributed docs, file systems) were introduced in Phase 05.
