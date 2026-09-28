# FIELDNET — BUILD-02 Implementation Specification
**Milestone:** Three-Node Multi-Hop Relay, Distance-Vector Routing & End-to-End ACK  
**Status:** COMPLETE  
**Architecture Compliance:** Strict adherence to Phase 01–05 Architecture Documents (`docs/architecture/01-system-model.md` through `docs/architecture/05-protocol-routing-dtn.md`).

---

## 1. Overview & Goal

BUILD-02 advances FIELDNET from point-to-point two-node communication into a true multi-hop mesh network.

The milestone proves:
> **In a linear three-node topology ($A \leftrightarrow B \leftrightarrow C$) where Node A and Node C cannot communicate directly, Node A sends an authenticated DATA message specifying only `destination = C`. The network automatically discovers routes, resolves next hops, relays traffic transparently through intermediate Node B while decrementing TTL, delivers the unmodified signed message to Node C's application layer, and propagates an end-to-end cryptographic delivery ACK back from C through B to Node A.**

Key Non-Negotiable Invariants:
- **No Manual Next-Hop Specification:** The application layer specifies only the logical destination `NodeId`. The routing engine autonomously determines the next hop.
- **Relay Immutability:** Intermediate relay nodes decrement the transport `ttl`, but never alter payload content, message IDs, or the originator's Ed25519 digital signature.
- **Transit Isolation:** Transit packets passing through a relay node are forwarded without being exposed or delivered to the relay node's application layer.
- **Bi-Directional Multi-Hop Routing:** Data packets forward in the forward direction ($A \to B \to C$) and ACK packets forward in the reverse direction ($C \to B \to A$) using dynamic distance-vector routing state.

---

## 2. Architecture & Components Implemented

### 2.1 Route Control Protocol (`crates/fieldnet-protocol`)

Added control message definitions for distributed distance-vector route discovery:

1. **`MessageType::Route = 5`**
   - Registered in [`MessageType`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-protocol/src/message_type.rs).
   - Classified as a 1-hop link-local control message (`is_forwardable() == false`).

2. **`RouteAdvertisement` & `RouteEntry`**
   - Implemented in [`crates/fieldnet-protocol/src/route.rs`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-protocol/src/route.rs).
   - `RouteEntry { destination: NodeId, hops: u8, sequence: u64 }`.
   - Compact binary JSON wire codec (`to_bytes()` / `from_bytes()`).

### 2.2 Distance-Vector Routing Engine (`crates/fieldnet-runtime/src/routing.rs`)

The [`RoutingTable`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-runtime/src/routing.rs) manages active multi-hop topology:

- **1-Hop Neighbor Management:** Whenever `ConnectionEvent::PeerConnected` fires, the authenticated peer is registered as a direct 1-hop neighbor (`hops: 1, next_hop: peer_id`).
- **Distance-Vector Advertisements:**
  - Nodes advertise themselves as distance 0 to directly connected peers.
  - Nodes advertise all active non-expired routes.
  - Neighbors increment `hops += 1` and update their routing tables if the path is shorter or replaces an expired route.
- **Loop Prevention:**
  - *Self-Route Suppression:* Advertisements for `self.local_node_id` are discarded.
  - *Max Hop Metric:* Routes with `hops >= 16` are rejected as unreachable.
  - *Split Horizon / Reverse Hop Guard:* A relay will never bounce a packet back to the immediate peer from which it was received.
  - *Origination Loop Guard:* Relays discard any message where `message.source == self.node_id()`.
- **Dynamic Failover & Invalidation:**
  - Upon `ConnectionEvent::PeerDisconnected { peer_id }`, all routes whose `next_hop == peer_id` are immediately purged.

### 2.3 Transparent Multi-Hop Forwarding (`crates/fieldnet-runtime/src/runtime.rs`)

When an incoming message arrives at `handle_incoming_message`:
1. Expiration check: `message.is_expired()`.
2. Hop TTL check: `message.ttl == 0`.
3. If `message.destination == self.node_id()`: Delivered locally.
4. If `message.destination != self.node_id()`: Passed to `handle_multi_hop_forward`:
   - Checks `message.message_type.is_forwardable()`.
   - Checks `message.source != self.node_id()` (loop prevention).
   - Decrements `message.ttl` (or drops if TTL exhausted).
   - Resolves next hop from `RoutingTable`.
   - If next hop is connected, transmits via `ConnectionManager`.
   - If next hop is disconnected or route is unavailable, stages into persistent DTN queue.

### 2.4 End-to-End Cryptographic ACK Propagation

1. When destination Node C receives and validates the DATA message, it builds an `AckPayload::new_received(message_id)` and signs an `Ack` message destined for originator Node A.
2. Node C looks up its routing table for Node A $\to$ next hop is Node B.
3. Node B receives the ACK packet, observes `destination == A`, decrements TTL, looks up next hop for Node A $\to$ next hop is Node A.
4. Node A receives the ACK packet, verifies its delivery status, updates delivery state to `AckReceived(Received)`, and purges the message from its persistent DTN queue.

---

## 3. Verification & Test Results

The milestone is validated by [`crates/fieldnet-runtime/tests/three_node_multihop_test.rs`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-runtime/tests/three_node_multihop_test.rs):

```
running 1 test
Node A: 8cc4800a...
Node B: 727c9a88... (Intermediate Relay)
Node C: 45fe6b11...
Waiting for linear topology convergence (A <-> B <-> C)...
Waiting for routing convergence (A learns route to C via B)...
Node A sending message to Node C through relay B...
Relay node B: forwarding multi-hop message ... received from peer A destined for C (remaining ttl: 15)
DATA message ... verified and received at destination from originator A (payload len: 48 bytes, hops: 1)
Sending ACK for message ... to destination A
Relay node B: forwarding multi-hop message ... received from peer C destined for A (remaining ttl: 15)
ACK received from peer B for message ... (status: Received)
Waiting for end-to-end ACK to reach Node A...
Three-node multi-hop integration test PASSED!
test test_three_node_multihop_end_to_end_delivery ... ok (0.83s)
```

### Verified Properties
- [x] Node A and Node C have no direct socket link; all traffic flows through Node B.
- [x] Application specifies only `destination = Node C`; next-hop resolution is autonomous.
- [x] TTL is decremented from 16 to 15 by relay Node B.
- [x] Originator Ed25519 signature is verified at Node C despite multi-hop transit.
- [x] Relay Node B application does not receive transit traffic.
- [x] End-to-end ACK reaches Node A, removing bundle from DTN queue.
