# PHASE 09 — BUILD 03: HOST FUNCTIONS & CAPABILITY-GATED ENVIRONMENT APIS

## 1. Executive Summary

Phase 09 Build 03 completes the secure guest-to-host execution boundary for FIELDNET WebAssembly applications. Building directly upon Build 01 (Package System & Manifest Model) and Build 02 (Wasmtime Execution Supervisor & Sandboxing), this build delivers the **FIELDNET Environment API**: a strictly capability-gated, deterministic, versioned interface linking guest WebAssembly applications to FIELDNET core subsystems without exposing raw host resources, filesystems, sockets, or process primitives.

Key architectural achievements:
- **Strict Capability Boundary**: Sandboxed guests cannot invoke host capabilities unless the capability was explicitly declared in the signed application manifest *and* explicitly authorized by the node operator (`Declared ∩ Authorized`).
- **Stable Versioned Host ABI**: Versioned host exports under both `"fieldnet_v1"` and alias `"fieldnet"`, providing unified binary dispatch (`host_call`) and optimized typed host functions with `FIELDNET_HOST_API_VERSION = 1`.
- **Content-Addressed Storage & Quota Accounting**: Guest storage APIs (`storage_put`, `storage_get`, `storage_has`, `storage_delete`) backed by the cryptographic `ContentStore` with atomic persistent quota accounting (`max_storage_bytes`) tracked in SQLite.
- **Service Discovery & Resolution**: Service discovery APIs (`service_resolve`, `service_describe`) gated by `service.discover`, querying `fieldnet-service`'s `ServiceResolver`.
- **Anti-Spoofing Event Bus**: Event publishing (`event_publish`) gated by `events.publish`, enforcing canonical topic namespacing (`app.<app_id>.<event>`) and preventing identity or execution spoofing.
- **DTN & Overlay Network Dispatch**: Message transmission (`network_send`) gated by `network.send`, enforcing runtime-derived source identity (`sender = local_node_id`) and routing through FIELDNET's disruption-tolerant overlay.
- **Structured Guest Error Isolation**: Canonical numeric error codes (`-1` to `-11`) returned safely to guests without leaking internal secrets, filesystem paths, or Rust stack traces.
- **Zero-Mock Verification**: 24 WebAssembly security test suites compiled directly from WebAssembly Text (`wat::parse_str`) plus 4 comprehensive end-to-end integration tests in `fieldnet-runtime`.

---

## 2. Security Boundary & Capability Architecture

### 2.1 Threat Model & Isolation Principles

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                       Guest WebAssembly Sandboxed Memory                    │
│   (No raw OS syscalls, no direct filesystems, no raw sockets, 1 MB stack)   │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │ Guest ABI Call (ptr, len)
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Wasmtime Host Function Linker                        │
│   - Memory Bounds Checks: [ptr .. ptr + len] ⊆ guest_memory                 │
│   - Size Bounds Checks: payload_len ≤ MAX_PAYLOAD_SIZE (1 MB)                │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │ Safe Byte Slice
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Capability Authorization Filter                        │
│              Check: (Declared ∈ Manifest) ∧ (Authorized ∈ Node)              │
│        If check fails ──► Immediate GuestErrorCode::PermissionDenied (-2)    │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │ Authorized Request
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     FIELDNET Host Environment Subsystems                    │
│  ┌──────────────────┬─────────────────┬──────────────────┬────────────────┐ │
│  │   ContentStore   │ ServiceResolver │  Event Broadcaster│ Network Sender │ │
│  │ (CAS + Quotas)   │ (fieldnet-svc)  │ (app.<id>.<ev>)  │ (DTN / Overlay)│ │
│  └──────────────────┴─────────────────┴──────────────────┴────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

1. **Declared ≠ Authorized Principle**:
   - `Declared`: Permissions requested by the publisher in the signed application manifest.
   - `Authorized`: Permissions granted by the local node operator or security policy during or after installation.
   - An application is granted capability access **if and only if** the permission exists in *both* sets:
     $$\text{Effective Capabilities} = \text{Declared Permissions} \cap \text{Authorized Permissions}$$
2. **Runtime-Derived Identity Principle**:
   - Guests cannot declare or spoof their `application_id`, `execution_id`, or source `node_id`.
   - All host function invocations read identity strictly from the supervisor's execution-scoped `HostContext`.
3. **Safe Memory Bounds Invariant**:
   - All pointer offsets and lengths received from guest memory are checked against the guest's allocated memory buffer using checked arithmetic (`start.checked_add(len)`).
   - Any violation triggers an immediate `GuestErrorCode::InvalidRequest` without panicking the host.

---

## 3. Host ABI Specification (`fieldnet_v1`)

The host exports functions under both the canonical module name `"fieldnet_v1"` and the compatibility alias `"fieldnet"`.

### 3.1 Host API Version Discovery

```wat
(import "fieldnet_v1" "host_api_version" (func $version (result i32)))
```
Returns the host API version (currently `1`). Used by guest runtimes to verify compatibility before executing subsystem calls.

### 3.2 Unified `host_call` Dispatcher

```wat
(import "fieldnet_v1" "host_call"
  (func $host_call
    (param $op_code i32)
    (param $req_ptr i32)
    (param $req_len i32)
    (param $resp_ptr i32)
    (param $resp_max_len i32)
    (result i32)))
```

Dispatches operations via numeric operation codes:

| Operation Code | Name | Required Capability | Payload Schema | Description |
|---|---|---|---|---|
| `1` | `STORAGE_PUT` | `storage.write` | Raw bytes | Stores content into CAS; writes CID hex string |
| `2` | `STORAGE_GET` | `storage.read` | CID hex string | Fetches content bytes from CAS |
| `3` | `STORAGE_HAS` | `storage.read` | CID hex string | Checks existence (returns `1` or `0`) |
| `4` | `STORAGE_DELETE` | `storage.write` | CID hex string | Unlinks content from app quota |
| `5` | `SERVICE_RESOLVE` | `service.discover` | JSON `ResolveQuery` | Resolves matching active service records |
| `6` | `SERVICE_DESCRIBE` | `service.discover` | UTF-8 `ServiceId` | Describes registered service definition |
| `7` | `EVENT_PUBLISH` | `events.publish` | JSON `EventPublish` | Emits custom event into node event bus |
| `8` | `NETWORK_SEND` | `network.send` | JSON `NetworkSend` | Routes payload to destination node |

### 3.3 Optimized Typed Host Functions

In addition to `host_call`, typed functions provide zero-overhead bindings for common operations:

| Import Name | Parameters | Return | Description |
|---|---|---|---|
| `storage_put` | `(data_ptr, data_len, out_cid_ptr, out_cid_max_len)` | `i32` (CID len or error) | Store data into CAS |
| `storage_get` | `(cid_ptr, cid_len, out_data_ptr, out_data_max_len)` | `i32` (data len or error) | Retrieve data from CAS |
| `storage_has` | `(cid_ptr, cid_len)` | `i32` (`1`, `0`, or error) | Check existence in CAS |
| `storage_delete` | `(cid_ptr, cid_len)` | `i32` (`1`, `0`, or error) | Delete app CAS link |
| `service_resolve` | `(query_ptr, query_len, out_desc_ptr, out_desc_max_len)` | `i32` (JSON len or error) | Resolve services |
| `service_describe` | `(svc_id_ptr, svc_id_len, out_desc_ptr, out_desc_max_len)` | `i32` (JSON len or error) | Describe service |
| `event_publish` | `(topic_ptr, topic_len, payload_ptr, payload_len)` | `i32` (`0` or error) | Publish application event |
| `network_send` | `(dest_ptr, dest_len, payload_ptr, payload_len)` | `i32` (`0` or error) | Dispatch network message |

---

## 4. Subsystem Integrations

### 4.1 Storage API & Persistent Quota Accounting

The storage subsystem bridges guest data to the node's local cryptographic Content-Addressed Storage (`ContentStore`):
- `storage_put` computes the BLAKE3 `ContentId` of the data, verifies that `current_usage + data_len <= max_storage_bytes`, writes the data into the CAS, and records the entry in the SQLite `application_storage_usage` table.
- `storage_get` verifies the hex CID, loads the bytes from CAS, and copies them to the guest's output buffer.
- `storage_delete` removes the tracking record from SQLite and decrements the application's active storage counter.
- If an application exceeds its declared `max_storage_bytes`, `storage_put` fails immediately with `GuestErrorCode::QuotaExceeded` (`-5`).

### 4.2 Service Discovery & Resolution

- Guest applications query the local service mesh via `service_resolve` (providing optional service ID, capability filter, and local-only flags) or `service_describe`.
- Both operations query `fieldnet-service::ServiceResolver`.
- Results are serialized as clean JSON arrays of `ServiceDescriptor` objects containing `service_id`, `display_name`, `version`, `provider_node_id`, `capabilities`, `endpoints`, and `health`.
- If the application lacks `service.discover`, the call returns `GuestErrorCode::PermissionDenied` (`-2`).

### 4.3 Anti-Spoofing Event Bus

Guest applications emit events via `event_publish(topic, payload)`:
- The host validates that the topic is alphanumeric with dots (`^[a-zA-Z0-9_.-]+$`).
- The host transparently prepends the canonical application prefix:
  $$\text{Canonical Topic} = \text{"app."} + \text{application\_id} + \text{"."} + \text{subtopic}$$
- Emits an `ApplicationEvent::Custom` containing:
  - `application_id`: Execution-scoped identity.
  - `execution_id`: Execution-scoped instance ID.
  - `topic`: Canonical namespaced topic.
  - `payload`: Raw binary payload.
  - `timestamp`: System time in milliseconds.
- Prevents cross-application event forgery and system event hijacking.

### 4.4 Disruption-Tolerant Network Dispatch

Guests transmit network messages via `network_send(destination, payload)`:
- The host validates the destination node ID hex string (64 characters).
- Enforces runtime-derived sender:
  $$\text{source\_node\_id} = \text{local\_node\_id}$$
- Wraps the message into an `ApplicationNetworkMessage` and routes it through `FieldnetRuntime`'s routing engine and DTN queue (`DtnQueue`), allowing store-and-forward mesh delivery across partitioned topologies.

---

## 5. Structured Error Codes

Guest-visible error codes are standard 32-bit negative integers:

| Constant Name | Code | Description |
|---|---|---|
| `INVALID_REQUEST` | `-1` | Malformed parameters, invalid pointers, or payload length exceeds limits |
| `PERMISSION_DENIED` | `-2` | Capability undeclared, unauthorized, or revoked |
| `NOT_FOUND` | `-3` | Requested CID, service, or object was not found |
| `INVALID_ID` | `-4` | Malformed ContentId, ServiceId, or NodeId |
| `QUOTA_EXCEEDED` | `-5` | Operation would exceed `max_storage_bytes` quota |
| `SERVICE_UNAVAILABLE` | `-6` | Service registry or target service is offline |
| `NETWORK_UNAVAILABLE` | `-7` | Network routing subsystem is unavailable |
| `CONTENT_UNAVAILABLE` | `-8` | Content item is missing from content store |
| `TIMEOUT` | `-9` | Operation timed out |
| `INTERNAL_ERROR` | `-10` | Non-fatal internal error (sanitized) |
| `BUFFER_TOO_SMALL` | `-11` | Guest output buffer is too small for returned data |

---

## 6. Verification & Test Matrix

### 6.1 Security Test Suite (`crates/fieldnet-wasm/tests/host_security_tests.rs`)
24 WebAssembly tests compiled directly from WebAssembly Text (`wat::parse_str`), verifying all 22 required security scenarios:

1. `test_01_version_discovery`: Host API version retrieval across `fieldnet_v1` and `fieldnet`.
2. `test_02_storage_put_and_get_authorized`: Roundtrip CAS put and get with full authorization.
3. `test_03_storage_put_denied_undeclared`: `storage.write` not in manifest returns `PERMISSION_DENIED`.
4. `test_04_storage_put_denied_unauthorized`: `storage.write` declared but unauthorized returns `PERMISSION_DENIED`.
5. `test_05_storage_quota_enforcement`: Putting payload exceeding `max_storage_bytes` returns `QUOTA_EXCEEDED`.
6. `test_06_storage_get_denied_undeclared`: `storage.read` not in manifest returns `PERMISSION_DENIED`.
7. `test_07_storage_get_denied_unauthorized`: `storage.read` declared but unauthorized returns `PERMISSION_DENIED`.
8. `test_08_storage_get_nonexistent`: Querying non-existent CID returns `NOT_FOUND`.
9. `test_09_storage_has`: Existence check returns `1` for stored content and `0` for absent content.
10. `test_10_storage_delete_and_quota_reclamation`: Deleting content reclaims quota and verifies unlinking.
11. `test_11_service_resolve_authorized`: Resolving registered services returns valid JSON descriptors.
12. `test_12_service_resolve_denied_undeclared`: `service.discover` not in manifest returns `PERMISSION_DENIED`.
13. `test_13_service_resolve_denied_unauthorized`: `service.discover` declared but unauthorized returns `PERMISSION_DENIED`.
14. `test_14_service_describe`: Describing a registered service returns valid metadata.
15. `test_15_event_publish_authorized_and_namespacing`: Publishing event produces `app.<app_id>.<event>` topic.
16. `test_16_event_publish_denied_undeclared`: `events.publish` not in manifest returns `PERMISSION_DENIED`.
17. `test_17_event_publish_denied_unauthorized`: `events.publish` declared but unauthorized returns `PERMISSION_DENIED`.
18. `test_18_network_send_authorized`: Sending network payload routes to network adapter with runtime node ID.
19. `test_19_network_send_denied_undeclared`: `network.send` not in manifest returns `PERMISSION_DENIED`.
20. `test_20_network_send_denied_unauthorized`: `network.send` declared but unauthorized returns `PERMISSION_DENIED`.
21. `test_21_memory_bounds_safety`: Pointers outside allocated WASM memory return `INVALID_REQUEST`.
22. `test_22_buffer_too_small`: Out buffer smaller than payload returns `BUFFER_TOO_SMALL`.
23. `test_23_invalid_cid_format`: Non-hex CID returns `INVALID_ID`.
24. `test_24_unified_host_call_dispatcher`: Full roundtrip via unified `host_call(op_code, ...)` dispatcher.

### 6.2 End-to-End Runtime Integration Tests (`crates/fieldnet-runtime/tests/phase09_host_api_integration_test.rs`)
4 end-to-end integration tests using full `FieldnetRuntime`:
1. `test_runtime_app_storage_and_quota_lifecycle`: Tests application package installation, authorization, execution, and quota tracking via `app_storage_usage` table.
2. `test_runtime_app_service_discovery`: Tests dynamic discovery of runtime services by sandboxed WASM guest.
3. `test_runtime_app_event_bus_and_anti_spoofing`: Tests event delivery over runtime broadcast channel with anti-spoofing prefix verification.
4. `test_runtime_app_dtn_network_routing`: Tests DTN bundle creation from guest network calls with runtime-derived source identity.
