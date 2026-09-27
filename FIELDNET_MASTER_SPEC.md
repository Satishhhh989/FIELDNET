# FIELDNET_MASTER_SPEC.md

**Project:** FIELDNET\
**Document:** Master Architecture and Implementation Specification\
**Status:** Authoritative project specification\
**Version:** 1.0\
**Scope:** Product MVP + long-term platform architecture

------------------------------------------------------------------------

# 1. Executive Definition

FIELDNET is a distributed offline operating environment that allows
nearby computing devices to form a resilient local computing ecosystem
where applications, services, data, and computation can continue
operating despite intermittent or unavailable global connectivity.

The internet is treated as an available capability, not a prerequisite
for computing.

FIELDNET is not:

-   a replacement operating system for Windows, macOS, Android, or iOS
-   merely a chat application
-   merely a mesh networking application
-   merely offline file sharing
-   a cloud replacement
-   a central-server application
-   a collection of unrelated utilities

FIELDNET is a software runtime and application environment that runs on
top of host operating systems and creates a distributed local
environment across participating devices.

A device running the FIELDNET runtime becomes a FIELDNET node.

------------------------------------------------------------------------

# 2. Primary Reference Scenario

The primary product scenario is a disaster or infrastructure-loss
environment.

Example:

Three people have three phones.

There is:

-   no internet
-   no cloud server
-   no central administrator
-   no reliable network infrastructure

They install and open FIELDNET.

The phones discover each other, authenticate, establish local
connectivity, and form an environment.

The users can then:

-   exchange messages
-   share files
-   access offline maps and information
-   collaborate on documents
-   use local services
-   use local AI when available
-   transfer data through intermittent encounters
-   continue working when one device disappears
-   synchronize state when devices reconnect
-   use an internet gateway if one device later gains connectivity

The user should not have to understand networking, routing, CRDTs, DTN,
cryptography, or distributed systems.

The complexity belongs inside FIELDNET.

------------------------------------------------------------------------

# 3. Core Principles

These principles are architectural constraints.

## 3.1 Local-first

Local operation must not depend on internet availability.

## 3.2 No mandatory central server

The core environment must operate without a permanent central
coordinator.

## 3.3 Service-centric architecture

Applications request capabilities and services rather than fixed
machines or IP addresses.

Example:

`ai.service`

rather than:

`192.168.1.42:8080`

## 3.4 Identity is independent of address

A node's cryptographic identity must remain logically stable even if its
IP address, transport, or physical network changes.

## 3.5 Failure is normal

Node disappearance, route failure, network partition, delayed
communication, service failure, and intermittent connectivity are
expected operating conditions.

## 3.6 Transport independence

Applications must not depend directly on TCP, UDP, Bluetooth, Wi-Fi
Direct, WebRTC, or other transports.

## 3.7 Persistent messages

A temporary lack of connectivity must not automatically destroy a
message that can still be delivered later.

## 3.8 Eventual convergence

Replicated state should converge when communication becomes available,
subject to the semantics of the chosen data type.

## 3.9 Content identity

Content should remain addressable even when its physical location
changes.

## 3.10 Security by default

Discovered does not mean trusted.

Connected does not mean authorized.

## 3.11 Capability-based access

Applications should receive explicit capabilities instead of
unrestricted access to host resources.

## 3.12 Graceful degradation

When advanced capabilities disappear, local functionality should
continue where possible.

------------------------------------------------------------------------

# 4. Product Model

FIELDNET has three planes.

## 4.1 User Plane

Normal users interact with:

-   Home / Environment
-   Files
-   Workspace
-   AI
-   Search
-   Services
-   Compute
-   Apps
-   Network

## 4.2 Control Plane

Owners/operators manage:

-   Node
-   Identity and trust
-   Services
-   Storage
-   Compute
-   Gateway
-   Security
-   Runtime
-   Network
-   Applications

## 4.3 Developer Plane

Developers use:

-   FIELDNET SDK
-   Service SDK
-   WASM application tooling
-   CLI
-   protocol tools
-   simulator
-   testing and fault-injection tools

These three planes share one underlying runtime.

------------------------------------------------------------------------

# 5. High-Level Architecture

``` text
                         FIELDNET PLATFORM
                                |
          +---------------------+---------------------+
          |                     |                     |
      USER PLANE           CONTROL PLANE       DEVELOPER PLANE
          |                     |                     |
          +---------------------+---------------------+
                                |
                       Environment API
                                |
                       +--------v--------+
                       |  RUST RUNTIME   |
                       +--------+--------+
                                |
        +-----------+-----------+-----------+-----------+
        |           |           |           |           |
    Identity    Networking    Storage     Services    Compute
        |           |           |           |           |
        +-----------+-----------+-----------+-----------+
                                |
                    Distributed Protocol Layer
                                |
             +------------------+------------------+
             |                  |                  |
           Node A             Node B             Node C
```

Each physical device runs an instance of the FIELDNET runtime.

------------------------------------------------------------------------

# 6. Runtime Architecture

The Rust runtime is the core of FIELDNET.

It is not a separate server.

On mobile, the runtime is packaged into the FIELDNET application and
exposed to the platform UI through a native bridge.

On desktop, the runtime can be packaged with a Tauri shell or run as a
node process with a desktop interface.

Core runtime components:

-   Node Manager
-   Lifecycle Manager
-   Configuration Manager
-   Event Bus
-   Resource Manager
-   Health Monitor
-   Identity Manager
-   Security Manager
-   Discovery Manager
-   Transport Manager
-   Connection Manager
-   Protocol Engine
-   Routing Engine
-   DTN Engine
-   Storage Manager
-   Sync Engine
-   Content Manager
-   Service Registry
-   Service Resolver
-   Application Manager
-   WASM Runtime
-   Compute Scheduler
-   Gateway Manager
-   Observability subsystem

The initial runtime should be a modular Rust process/library rather than
many microservices.

------------------------------------------------------------------------

# 7. Technology Strategy

## 7.1 Core Runtime

Primary language:

-   Rust

Concurrency/runtime:

-   Tokio

Serialization:

-   Serde

Database:

-   SQLite

Cryptography:

-   mature audited Rust cryptographic libraries

WASM:

-   Wasmtime

## 7.2 User Interface

Desktop:

-   Tauri
-   React
-   TypeScript
-   Vite

Mobile:

-   platform-native shell or Flutter/React Native if required
-   shared Rust core through FFI/native bindings

The distributed-system logic must remain in the Rust core.

## 7.3 Networking

FIELDNET supports a transport abstraction over:

-   TCP
-   UDP
-   WebRTC
-   mDNS
-   Bluetooth / BLE
-   Wi-Fi Direct
-   Wi-Fi Aware where platform support exists
-   libp2p

Not every device is required to support every transport.

Transport availability is discovered at runtime.

## 7.4 Content

-   SHA-256 or another approved cryptographic content hash
-   chunking
-   manifests
-   content-addressed objects
-   replication
-   resumable transfer

## 7.5 Synchronization

-   operation logs
-   Lamport clocks
-   vector clocks where appropriate
-   CRDTs
-   Merkle-style comparison for large datasets

------------------------------------------------------------------------

# 8. Node Model

A FIELDNET node is a logical runtime participant.

A node has:

-   node_id
-   cryptographic identity
-   runtime version
-   supported transports
-   capabilities
-   services
-   content availability
-   resource information
-   network addresses
-   trust state
-   last-seen information

Node identity must not be tied to:

-   IP address
-   MAC address
-   Bluetooth address
-   hostname
-   physical device model

------------------------------------------------------------------------

# 9. Identity and Security

FIELDNET uses cryptographic identity.

Preferred primitives:

-   Ed25519 for identity and signatures
-   X25519 for key agreement
-   ChaCha20-Poly1305 or another approved AEAD for encrypted sessions
-   SHA-256 for hashing/content identity where appropriate

Private keys:

-   generated locally
-   never transmitted as identity material
-   stored using secure platform facilities where available

Trust lifecycle:

``` text
UNKNOWN
  |
DISCOVERED
  |
AUTHENTICATED
  |
AUTHORIZED
```

Discovery is not authentication.

Authentication is not authorization.

Authorization uses explicit capabilities.

Example capabilities:

-   `storage.read`
-   `storage.write`
-   `content.read`
-   `content.write`
-   `service.resolve`
-   `sync.document`
-   `compute.submit`
-   `network.send`

Security must include:

-   replay protection
-   message authentication
-   key rotation
-   identity revocation
-   permission enforcement
-   service manifest verification
-   app signature verification
-   rate limiting
-   resource limits

FIELDNET does not claim to solve fully compromised endpoints, physical
device theft, all denial-of-service attacks, or all traffic analysis.

------------------------------------------------------------------------

# 10. Peer Discovery

Discovery answers:

"Who is nearby?"

Initial mechanisms:

-   mDNS / DNS-SD
-   UDP multicast/broadcast where supported
-   platform-native nearby discovery
-   Bluetooth discovery
-   Wi-Fi Direct / Wi-Fi Aware discovery
-   libp2p discovery mechanisms

A node advertisement may contain:

``` text
node_id
runtime_version
supported_transports
capabilities
services
addresses
expiration
```

Advertisements are untrusted until identity validation.

Peer lifecycle:

``` text
DISCOVERED
CONNECTING
AUTHENTICATING
CONNECTED
DEGRADED
DISCONNECTED
EXPIRED
```

Peer table and routing table are separate concepts.

------------------------------------------------------------------------

# 11. Transport Abstraction

Applications must use the environment networking API.

Conceptual interface:

``` text
connect(peer)
send(message)
receive()
close()
health()
capabilities()
```

Transport capabilities include:

-   reliable/unreliable
-   ordered/unordered
-   stream/datagram
-   bandwidth
-   latency
-   range
-   power characteristics
-   availability

Multiple transports may exist between the same nodes.

Transport failure must not automatically mean peer failure.

FIELDNET should support transport migration where feasible.

------------------------------------------------------------------------

# 12. Distributed Protocol

FIELDNET is message-oriented above the transport layer.

Conceptual message:

``` text
version
type
message_id
source
destination
created_at
expires_at
ttl
sequence
payload
authentication
```

Initial message types:

-   HELLO
-   AUTH
-   ACK
-   DATA
-   ROUTE
-   SERVICE
-   SYNC
-   FILE
-   EVENT
-   COMPUTE
-   ERROR

Message lifecycle:

``` text
CREATED
QUEUED
ROUTING
FORWARDED
DELIVERED
```

Alternative:

``` text
CREATED
QUEUED
NO_ROUTE
STORED
ROUTE_APPEARS
FORWARDED
DELIVERED
```

Messages must support:

-   unique IDs
-   deduplication
-   expiration
-   TTL
-   sequencing where necessary
-   acknowledgements
-   authentication

------------------------------------------------------------------------

# 13. Routing

Routing initially supports:

-   direct peer communication
-   multi-hop forwarding
-   dynamic routes
-   route expiration
-   route metrics
-   route failure detection
-   alternate paths

Potential algorithms:

-   BFS for simple unweighted topology
-   Dijkstra for weighted topology

Future research can evaluate:

-   AODV
-   OLSR
-   BATMAN
-   gossip-based routing
-   libp2p routing mechanisms

FIELDNET should not claim a new routing algorithm unless one is actually
designed, implemented, and experimentally evaluated.

------------------------------------------------------------------------

# 14. Delay-Tolerant Networking

DTN is a core feature.

If no route exists, the system may persist a message.

Queue metadata:

``` text
message_id
source
destination
priority
created_at
expires_at
attempts
ttl
payload
status
```

Message priorities:

-   critical
-   high
-   normal
-   low

DTN supports:

-   store-and-forward
-   store-carry-forward
-   contact opportunities
-   delayed delivery
-   retry
-   expiration
-   deduplication

Core principle:

A network path is temporary, but a message does not have to be.

------------------------------------------------------------------------

# 15. Local-First Data

Each node maintains local state.

Core components:

-   local database
-   operation log
-   causality metadata
-   CRDT state
-   synchronization metadata

Applications must be able to make local progress while disconnected.

Sync exchanges missing operations instead of blindly replacing entire
databases.

Sync states:

``` text
LOCAL
PENDING
SYNCING
PARTIALLY_SYNCED
CONVERGED
CONFLICTED
```

------------------------------------------------------------------------

# 16. CRDT and Conflict Handling

Supported structures may include:

-   counters
-   sets
-   maps
-   registers
-   sequences
-   collaborative text structures where justified

Conflict policies must be explicit.

Possible register policies:

-   last-write-wins
-   multi-value
-   application-defined merge
-   user resolution

The system must not silently discard meaningful conflicting state.

CRDT implementations should be tested for convergence properties.

------------------------------------------------------------------------

# 17. Distributed Content System

Files are represented independently of physical location.

Conceptual flow:

``` text
content
   |
cryptographic hash
   |
content_id
```

Large content is chunked.

Each chunk can have its own hash.

A manifest describes the complete content.

Capabilities:

-   put
-   get
-   has
-   stat
-   open
-   stream
-   list
-   delete

Features:

-   deduplication
-   integrity verification
-   resumable transfer
-   multi-source transfer
-   replication
-   pinning
-   cache management
-   garbage collection
-   versioning

A content hash provides integrity and content identity.

A signature provides authenticity/provenance.

These concepts must remain separate.

------------------------------------------------------------------------

# 18. Service Model

Applications request services, not fixed devices.

Example:

``` text
ai.service
search.service
maps.service
translation.service
```

Service instance metadata:

``` text
service_id
instance_id
provider_node
version
capabilities
endpoint
protocol
status
permissions
expiration
```

Service lifecycle:

``` text
DISCOVERED
AUTHENTICATED
AVAILABLE
DEGRADED
UNAVAILABLE
EXPIRED
```

Service resolver may consider:

-   locality
-   trust
-   capabilities
-   latency
-   bandwidth
-   availability
-   load
-   queue length
-   version

Provider failure should trigger re-resolution.

------------------------------------------------------------------------

# 19. Local Web

FIELDNET includes a Local Web Gateway.

The gateway provides browser-accessible local services.

Example:

``` text
fieldnet.local
```

Conceptual path:

``` text
Browser
  |
fieldnet.local
  |
mDNS / DNS-SD
  |
FIELDNET Local Web Gateway
  |
Service Resolver
  |
Local Service
```

Services may expose:

-   HTTP
-   WebSocket
-   local APIs
-   browser interfaces

FIELDNET should not require a proprietary browser.

Normal browsers should be able to access local services.

The gateway is part of the runtime.

Desktop runtimes can continue serving the gateway after the UI closes.

Mobile behavior must respect Android/iOS background execution
restrictions.

`.local` semantics must be implemented consistently with established
mDNS behavior. Do not invent a conflicting naming scheme.

------------------------------------------------------------------------

# 20. Sandboxed Applications

Applications run in WebAssembly.

Package concept:

``` text
app.wasm
manifest.json
assets/
metadata/
```

Manifest declares:

-   application identity
-   version
-   entry point
-   permissions
-   resources
-   dependencies
-   publisher/signature metadata

Application lifecycle:

``` text
DISCOVERED
VERIFIED
INSTALLED
AUTHORIZED
STARTING
RUNNING
SUSPENDED
STOPPED
CRASHED
ISOLATED
```

Applications must not receive arbitrary host access.

Host APIs are capability-controlled.

Example:

``` text
storage_get()
storage_put()
service_resolve()
content_get()
event_subscribe()
compute_submit()
```

Resource controls include:

-   CPU
-   memory
-   storage
-   execution time
-   bandwidth
-   concurrency

Use Wasmtime or another mature WASM runtime.

Never implement a WASM engine from scratch.

------------------------------------------------------------------------

# 21. Distributed Compute

Compute is a discoverable capability.

Resource types:

-   CPU
-   memory
-   GPU
-   accelerator
-   storage
-   AI model/inference capability

Job model:

``` text
job_id
requester
task
inputs
dependencies
required_capabilities
resource_constraints
priority
deadline
output
```

Lifecycle:

``` text
CREATED
QUEUED
SCHEDULED
DISPATCHED
RUNNING
COMPLETED
```

Failure:

``` text
RUNNING
NODE_LOST
REQUEUED
RESCHEDULED
```

Scheduling considers:

-   capability match
-   resource availability
-   data locality
-   trust
-   network quality
-   queue length
-   estimated execution time

Local execution should be preferred when practical.

Remote execution is an optimization/capability, not a requirement.

------------------------------------------------------------------------

# 22. Local AI

AI is implemented as a FIELDNET service rather than being the platform
itself.

Example:

``` text
ai.service
```

Models can be content-addressed.

Model metadata may include:

-   model CID
-   version
-   architecture
-   resource requirements
-   capabilities
-   license metadata

Possible inference adapters:

-   llama.cpp
-   ONNX Runtime
-   Ollama
-   platform-native inference
-   custom local model runtimes

FIELDNET should abstract the provider.

Applications ask for AI capability.

The runtime finds an appropriate provider.

Privacy boundaries must be explicit when data is sent to another node.

------------------------------------------------------------------------

# 23. Internet Gateway

Internet access is an optional capability.

Example:

``` text
internet.gateway
```

Gateway types may include:

-   internet gateway
-   HTTP proxy
-   HTTPS proxy
-   DNS gateway
-   package gateway
-   API gateway

Multiple gateways may exist.

Selection can consider:

-   availability
-   latency
-   bandwidth
-   trust
-   policy
-   load

The gateway must enforce:

-   authorization
-   destination policy
-   bandwidth limits
-   concurrency limits

Interactive connections and delay-tolerant requests must be treated
differently.

External content may be cached into the content system.

------------------------------------------------------------------------

# 24. Mobile Architecture

The mobile application is itself a FIELDNET node.

Conceptual structure:

``` text
Mobile UI
   |
Native bridge / FFI
   |
Rust FIELDNET Core
   |
Platform adapters
   |
Android / iOS networking APIs
   |
Wi-Fi / BLE / Bluetooth / other supported transports
```

The user does not install Rust separately.

The FIELDNET mobile application packages the runtime.

Platform adapters handle OS-specific capabilities.

The core Rust logic should remain shared between desktop and mobile
wherever practical.

Mobile background execution must respect platform restrictions.

FIELDNET must not promise unrestricted background networking after a
user force-quits an application.

------------------------------------------------------------------------

# 25. Hardware Integration

Hardware transport integration is part of FIELDNET.

Supported capabilities depend on platform:

-   Wi-Fi
-   Bluetooth
-   BLE
-   Wi-Fi Direct
-   Wi-Fi Aware
-   local network interfaces
-   platform-native peer discovery

FIELDNET should dynamically determine available transports.

Example:

``` text
Wi-Fi: available
BLE: available
Wi-Fi Direct: unavailable
Internet: unavailable
```

The application should not need to understand the hardware-specific
details.

------------------------------------------------------------------------

# 26. User Applications

The initial product should include:

## Environment Home

Shows:

-   node status
-   nearby devices
-   services
-   compute
-   storage
-   connectivity mode

## Files

Provides:

-   distributed files
-   transfers
-   sharing
-   resumable downloads
-   content verification

## Workspace

Provides:

-   documents
-   notes
-   offline collaboration
-   synchronization

## AI

Provides:

-   local AI
-   distributed AI
-   local document analysis

## Search

Provides:

-   distributed/local search
-   service discovery

## Services

Provides:

-   nearby services
-   capabilities
-   providers
-   availability

## Compute

Provides:

-   job submission
-   resource discovery
-   job state

## Apps

Provides:

-   WASM application discovery
-   installation
-   permissions
-   updates

## Network

Provides:

-   nearby nodes
-   connectivity
-   gateway state
-   environment state

------------------------------------------------------------------------

# 27. Control Plane Applications

Owner/operator tooling includes:

-   Node Manager
-   Identity & Trust
-   Service Manager
-   Storage Manager
-   Compute Manager
-   Gateway Manager
-   Security Center
-   Runtime Inspector
-   Network Inspector
-   Application Manager

These tools should expose system state without becoming a generic
analytics dashboard.

------------------------------------------------------------------------

# 28. Developer Plane

Developer tooling includes:

-   FIELDNET CLI
-   Rust SDK
-   TypeScript SDK
-   Service SDK
-   WASM application template
-   WASM development tools
-   protocol inspector
-   network simulator
-   fault-injection tools
-   test harness

Example CLI:

``` text
fieldnet start
fieldnet node
fieldnet peers
fieldnet services
fieldnet apps
fieldnet content
fieldnet compute
fieldnet routes
fieldnet logs
fieldnet debug
```

------------------------------------------------------------------------

# 29. MVP Boundary

The perfect MVP should prioritize the foundation rather than attempting
every advanced capability simultaneously.

## MVP phases

### Phase 01

System Model & Core Principles

### Phase 02

Rust Runtime

### Phase 03

Identity, Security & Trust

### Phase 04

Peer Discovery & Multi-Transport Networking

### Phase 05

Protocol, Routing & DTN

### Phase 06

Local-First Data & Distributed Synchronization

### Phase 07

Distributed File & Content System

### Phase 08

Service Discovery & Local Web

Phases 09-12 remain part of the platform architecture but can follow the
core MVP.

The MVP success condition is:

Three or more devices can form a FIELDNET environment without a central
server, communicate locally, exchange content, operate during
disconnection, synchronize after reconnection, and access services
through a local web gateway.

------------------------------------------------------------------------

# 30. MVP First Demonstration

Use three or more devices.

Example:

``` text
Node A
Node B
Node C
```

Demonstrate:

1.  Install FIELDNET.
2.  Create an environment on A.
3.  Join from B and C.
4.  Automatic discovery.
5.  Device authentication.
6.  Secure communication.
7.  File transfer.
8.  Disconnect one device.
9.  Continue local operation.
10. Queue data for the disconnected device.
11. Reconnect the device.
12. Deliver queued data.
13. Edit shared data independently while disconnected.
14. Reconnect and synchronize.
15. Access `fieldnet.local`.
16. Discover a local service.
17. Remove the service provider.
18. Demonstrate service failure/re-resolution where another provider
    exists.

The demonstration must use real behavior rather than mocked UI states.

------------------------------------------------------------------------

# 31. Development Order

The implementation must follow dependency order.

``` text
Rust Workspace
    |
Runtime Core
    |
Identity
    |
Storage
    |
Discovery
    |
Transport
    |
Secure Sessions
    |
Protocol
    |
Routing
    |
Persistent Queue
    |
DTN
    |
Local-First Storage
    |
CRDT Sync
    |
Content Store
    |
Content Transfer
    |
Service Registry
    |
Local Web Gateway
    |
User Applications
```

Do not build the polished UI before the corresponding runtime capability
exists.

------------------------------------------------------------------------

# 32. Repository Structure

Recommended initial structure:

``` text
fieldnet/
|
├── Cargo.toml
├── README.md
├── FIELDNET_MASTER_SPEC.md
├── ARCHITECTURE.md
├── IMPLEMENTATION_STATUS.md
├── CHANGELOG.md
|
├── crates/
│   ├── fieldnet-runtime/
│   ├── fieldnet-core/
│   ├── fieldnet-identity/
│   ├── fieldnet-crypto/
│   ├── fieldnet-discovery/
│   ├── fieldnet-transport/
│   ├── fieldnet-protocol/
│   ├── fieldnet-routing/
│   ├── fieldnet-dtn/
│   ├── fieldnet-storage/
│   ├── fieldnet-sync/
│   ├── fieldnet-content/
│   ├── fieldnet-services/
│   ├── fieldnet-wasm/
│   ├── fieldnet-compute/
│   ├── fieldnet-gateway/
│   └── fieldnet-observability/
|
├── apps/
│   ├── desktop/
│   ├── mobile/
│   ├── user/
│   ├── control/
│   └── developer/
|
├── sdk/
│   ├── rust/
│   └── typescript/
|
├── cli/
|
├── protocol/
│   ├── schemas/
│   └── examples/
|
├── simulator/
|
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── property/
│   ├── security/
│   ├── network/
│   └── end-to-end/
|
└── docs/
    ├── architecture/
    ├── protocols/
    ├── security/
    └── experiments/
```

The exact crate structure may evolve, but responsibilities must remain
modular.

------------------------------------------------------------------------

# 33. API Design

The Environment API is the primary boundary between applications and the
runtime.

Conceptual namespaces:

``` text
identity.*
network.*
peer.*
storage.*
content.*
sync.*
service.*
events.*
compute.*
apps.*
```

Examples:

``` text
identity.current()
peer.list()
network.send()
storage.get()
storage.put()
content.get()
content.put()
service.resolve()
events.subscribe()
sync.document()
compute.submit()
```

The API should be versioned.

Breaking changes require explicit versioning.

------------------------------------------------------------------------

# 34. Event System

The runtime uses an internal event bus.

Example events:

``` text
runtime.started
runtime.stopped
peer.discovered
peer.authenticated
peer.connected
peer.disconnected
route.created
route.removed
message.queued
message.forwarded
message.delivered
message.expired
sync.started
sync.completed
content.discovered
content.verified
service.available
service.unavailable
compute.started
compute.completed
compute.failed
gateway.available
gateway.unavailable
app.started
app.crashed
```

Events should have:

-   event type
-   event ID
-   timestamp
-   source
-   correlation ID where relevant
-   structured payload

------------------------------------------------------------------------

# 35. Persistence

Persistent state must survive runtime restart.

Persist at minimum:

-   node identity
-   configuration
-   peer metadata where appropriate
-   message queues
-   content metadata
-   sync metadata
-   operation logs
-   service metadata
-   job state
-   application state
-   event/audit information where required

The system must recover safely after abrupt termination.

------------------------------------------------------------------------

# 36. Failure Model

FIELDNET must explicitly test:

-   node crash
-   runtime crash
-   process crash
-   transport loss
-   peer disappearance
-   route failure
-   network partition
-   packet delay
-   packet reordering
-   packet duplication
-   corrupted messages
-   invalid signatures
-   replay attacks
-   malicious service advertisements
-   service crash
-   WASM crash
-   storage exhaustion
-   compute worker failure
-   gateway failure
-   intermittent internet
-   clock skew
-   partial synchronization
-   concurrent writes

Fault categories:

-   crash
-   omission
-   delay
-   corruption
-   malicious behavior
-   resource exhaustion
-   partition

------------------------------------------------------------------------

# 37. Testing Strategy

Testing layers:

1.  Unit tests
2.  Component tests
3.  Integration tests
4.  Property-based tests
5.  Protocol fuzzing
6.  Network simulation
7.  Fault injection
8.  Security tests
9.  Performance tests
10. Recovery tests
11. Real-device tests
12. End-to-end tests

Tests must validate behavior rather than only code coverage.

------------------------------------------------------------------------

# 38. Critical Invariants

The following must hold:

1.  Private identity keys never leave their owning node.
2.  An unverified peer cannot become trusted.
3.  Unauthorized capabilities are rejected.
4.  Expired messages are not forwarded.
5.  Duplicate messages are safely deduplicated where protocol semantics
    require it.
6.  Corrupted content cannot become verified content.
7.  Runtime restart does not silently destroy persistent queues.
8.  Service identity remains independent from provider address.
9.  WASM applications cannot bypass capability controls.
10. Local operation remains possible when external internet connectivity
    disappears.
11. CRDT-supported replicas converge according to their defined
    semantics.
12. A failed worker does not silently erase recoverable compute state.
13. Gateway availability is not required for local services.
14. Transport failure does not automatically imply node identity
    failure.

------------------------------------------------------------------------

# 39. Experimental Metrics

Measure actual values for:

-   peer discovery time
-   authentication time
-   connection establishment time
-   route convergence time
-   message delivery latency
-   delivery success rate
-   queue persistence
-   duplicate rate
-   sync convergence time
-   synchronization bandwidth
-   content transfer throughput
-   content resume efficiency
-   replication availability
-   service failover time
-   WASM startup time
-   compute scheduling latency
-   compute recovery time
-   gateway failover time
-   CPU overhead
-   memory overhead
-   storage overhead

Never fabricate benchmark results.

------------------------------------------------------------------------

# 40. Resilience Demonstration

The strongest end-to-end demonstration:

``` text
A ─── B ─── C
```

A requests a service on C.

B acts as a relay.

Then B disappears.

The request becomes persistent.

A continues working locally.

A edits a document.

C independently edits the same document.

B returns.

Routes recover.

Messages forward.

CRDT state converges.

Content becomes available again.

A service provider fails.

Another provider is discovered.

A compute worker disappears.

The job is recovered.

Finally one node gains internet connectivity.

It advertises:

`internet.gateway`

The environment enters hybrid mode.

Remove internet again.

The local environment continues.

------------------------------------------------------------------------

# 41. What Must Not Be Done

AI coding agents must not:

-   invent APIs without checking existing interfaces
-   rewrite working subsystems unnecessarily
-   replace Rust networking with a random web backend
-   add a central server as a shortcut
-   make applications communicate directly through IP addresses
-   bypass the Environment API
-   store critical distributed state only in memory
-   treat internet access as mandatory
-   claim support for hardware APIs that have not been tested
-   implement cryptographic primitives manually
-   implement a WASM engine manually
-   silently remove security checks to make a demo work
-   fake distributed behavior in the UI
-   create mock peer data instead of real discovery
-   call a simulated feature "implemented" without clearly labeling it

------------------------------------------------------------------------

# 42. AI Agent Development Rules

Every implementation prompt must instruct the coding agent to:

1.  Read this master specification.
2.  Inspect the current repository.
3.  Inspect existing implementations before modifying them.
4.  Preserve existing working behavior.
5.  Implement only the requested scope.
6.  Avoid unnecessary architectural rewrites.
7.  Use real integrations where the phase requires them.
8.  Add tests for new behavior.
9.  Run relevant tests.
10. Update implementation documentation.
11. Report what is implemented.
12. Report what is simulated.
13. Report what remains incomplete.
14. Never claim a feature works without evidence.

The repository is the source of truth for implementation state.

------------------------------------------------------------------------

# 43. Phase Prompt Strategy

Each phase should be divided into small implementation prompts.

A typical phase uses:

``` text
Prompt A
Architecture and interfaces

Prompt B
Core implementation

Prompt C
Integration

Prompt D
Testing and failure handling

Prompt E
Polish / optimization
```

The agent must not treat each prompt as a fresh project.

Every prompt begins by reading:

-   FIELDNET_MASTER_SPEC.md
-   ARCHITECTURE.md
-   IMPLEMENTATION_STATUS.md
-   relevant existing source files

------------------------------------------------------------------------

# 44. MVP Definition of Done

FIELDNET MVP is complete when:

-   at least three real devices can participate
-   no central server is required
-   devices receive stable cryptographic identities
-   peers discover each other
-   peers authenticate each other
-   at least the supported real transports function
-   devices can exchange authenticated messages
-   multi-hop routing works in the supported test topology
-   messages persist when no route exists
-   delayed messages can be forwarded when contact returns
-   local data remains usable offline
-   selected CRDT state converges
-   files are content-addressed
-   content integrity is verified
-   interrupted transfers can resume
-   services can be discovered
-   local web services are accessible
-   `fieldnet.local` or the final documented local naming mechanism
    works on supported platforms
-   the runtime survives individual subsystem failures
-   the system has real end-to-end tests
-   the final demonstration works without mocked network behavior

------------------------------------------------------------------------

# 45. Long-Term Expansion

After the MVP:

## Phase 09

Sandboxed WASM Applications

## Phase 10

Distributed Compute and Local AI

## Phase 11

Internet Gateway and Hybrid Mode

## Phase 12

Resilience, Testing and Experimental Validation

These are part of the full FIELDNET architecture even when not all are
included in the first MVP.

------------------------------------------------------------------------

# 46. Final Product Philosophy

FIELDNET should make a complex distributed system feel simple.

The user experience should be:

``` text
Install
  |
Open
  |
Create or Join
  |
Nearby devices appear
  |
Work
  |
Internet disappears
  |
Continue working
  |
Devices reconnect
  |
State converges
```

The underlying system is:

``` text
identity
discovery
transport
protocol
routing
DTN
storage
CRDT
content
services
WASM
compute
AI
gateway
resilience
```

The user should not have to understand any of it.

------------------------------------------------------------------------

# 47. Final Definition

FIELDNET is a distributed offline operating environment in which nearby
devices can automatically form a secure local computing ecosystem.

It allows applications and services to operate across changing
connectivity, devices, transports, and resource availability.

The system treats:

-   connectivity as a capability
-   computation as a capability
-   storage as a capability
-   services as capabilities
-   internet access as a capability

rather than treating any one physical machine or network connection as
the permanent foundation.

The central architectural principle is:

> **The internet is an available resource, not a prerequisite for
> computing.**

The central resilience principle is:

> **A network path may disappear, but useful work should not have to
> disappear with it.**

The central product principle is:

> **Complex distributed infrastructure should feel like a normal
> application to the user.**
