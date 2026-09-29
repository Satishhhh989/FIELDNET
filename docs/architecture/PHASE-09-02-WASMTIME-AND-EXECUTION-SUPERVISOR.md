# PHASE 09 — BUILD 02: WASMTIME RUNTIME & APPLICATION EXECUTION SUPERVISOR

## 1. Executive Summary

Phase 09 Build 02 establishes the WebAssembly execution engine and application supervisor for FIELDNET. It transforms the declarative application packages, manifests, and content-addressed artifacts delivered in Build 01 into live, isolated, sandboxed WebAssembly execution instances.

Execution is powered by Wasmtime 27 configured under strict isolation constraints:
- **Zero Default Host APIs**: Empty linker, zero unrestricted WASI, zero raw OS filesystem or socket access.
- **Deterministic Resource Sandboxing**: Fine-grained CPU fuel metering, epoch-based execution interruption, 1 MB stack limit, and memory growth limits via `wasmtime::StoreLimits`.
- **Fault-Isolated Execution**: A trapped, aborted, or crashed WebAssembly guest cannot panic or destabilize the FIELDNET host runtime.
- **Content-Addressed Module Cache**: Precompiled WASM modules indexed by `(package_hash, wasm_content_hash)` avoid redundant compilation overhead.
- **Clean Lifecycle & Supervisor Orchestration**: Transitions through `Authorized -> Starting -> Running -> Stopped` (or `Crashed`), enforced by the persistent SQLite `ApplicationRegistry`, with automated startup crash recovery and restart reconciliation.

---

## 2. Architecture & Pipeline

```text
ApplicationId
      │
      ▼
ApplicationRegistry ──► Retrieve ApplicationRecord (Manifest, PackageId, Items)
      │
      ▼
ContentStore (CAS)  ──► Fetch entrypoint WASM binary by ContentId
      │
      ▼
Integrity Check     ──► Verify payload hash matches PackageItem.content_id
      │
      ▼
ModuleCache         ──► Hit? Return Module; Miss? Compile with WasmEngine and insert
      │
      ▼
Store Setup         ──► StoreData (ExecutionId, ResourceLimits), Fuel Metering, Epoch Deadline
      │
      ▼
Linker (Empty)      ──► Instantiate Module (Zero Host Imports)
      │
      ▼
Entrypoint Locate   ──► Find exported "main" or "_start" function
      │
      ▼
Supervisor Spawn    ──► spawn_blocking with cancellation select & background epoch ticker
      │
      ▼
Trap Classification ──► Classify Exit / Trap / Timeout / Fuel / Memory / Cancellation
      │
      ▼
State & Event Bus   ──► Record final state in Registry & emit ApplicationEvent (Stopped / Crashed)
```

---

## 3. Sandboxing & Isolation Guarantees

### 3.1 Strict Wasmtime Configuration (`execution/engine.rs`)

[`WasmEngine`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/execution/engine.rs) instantiates an isolated Cranelift execution engine:
- `consume_fuel(true)`: Every WebAssembly instruction consumes CPU fuel.
- `epoch_interruption(true)`: A dedicated background thread ticks the engine epoch counter every 10 ms (`DEFAULT_EPOCH_TICK_INTERVAL_MS`). When a store's epoch deadline arrives, execution yields with `Trap::Interrupt`.
- `max_wasm_stack(1024 * 1024)`: Maximum guest stack size fixed at 1 MB.
- `nan_canonicalization(true)`: Deterministic Cranelift floating-point behavior across heterogeneous CPU architectures (x86_64 and aarch64).
- `cranelift_opt_level(OptLevel::Speed)`: Optimized compilation.

### 3.2 Dynamic Resource Limiting (`execution/store.rs`)

[`StoreData`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/execution/store.rs) configures guest runtime boundaries:
- `max_cpu_fuel`: Injected into `Store::set_fuel()`. Consumption is measured at termination via `initial_fuel - remaining_fuel`.
- `max_memory_bytes`: Enforced via `wasmtime::ResourceLimiter`. When a guest attempts `memory.grow` beyond the manifest cap, the grow instruction fails cleanly with `-1`.
- `max_execution_time_ms`: Configured via `Store::set_epoch_deadline(ticks)`.
- `max_instances(1)` and `max_tables(1)`: Strict guest limits preventing allocation exhaustion.

### 3.3 Zero Host Imports Boundary

Build 02 enforces a strictly empty `Linker`. Modules requesting unprovided host functions (e.g. storage, networking) fail instantiation cleanly with an `ExecutionError`. All capability-gated host APIs remain strictly deferred to Build 03.

---

## 4. Execution Pipeline & Supervisor (`execution/supervisor.rs`)

[`ApplicationExecutionSupervisor`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/execution/supervisor.rs) coordinates the entire application execution lifecycle:

1. **Registry Lookup**: Retrieves `ApplicationRecord` by `ApplicationId`.
2. **Eligibility Validation**: Verifies state is `Authorized`, `Stopped`, or `Running` (under concurrency limits). Applications in `Installed` state must be authorized first.
3. **Concurrency Enforcement**: Verifies active instances do not exceed `ResourceLimits.max_concurrency`.
4. **State Transition to STARTING**: Emits `ApplicationEvent::Starting`.
5. **CAS Binary Retrieval & Verification**: Fetches entrypoint WASM bytes from `fieldnet-content` and verifies hash against `PackageItem.content_id`.
6. **Module Compilation & Cache**: Queries `ModuleCache`. Compiles on cache miss and stores under `(package_hash, entrypoint_content_id)`.
7. **Store Configuration**: Instantiates `Store<StoreData>`, sets initial fuel, and configures epoch deadline.
8. **State Transition to RUNNING**: Emits `ApplicationEvent::Started`.
9. **Module Instantiation & Entrypoint Resolution**: Resolves exported `main` or `_start`.
10. **Thread Isolation**: Executes inside `tokio::task::spawn_blocking` to avoid blocking async runtime worker threads.
11. **Cancellation & Epoch Interruption**: Listens on a cancellation channel. If cancelled, `advance_epoch(100_000)` immediately forces the running WASM to trap without busy waiting.
12. **Result Processing & Trap Classification**: Unregisters execution, emits `ApplicationEvent::Stopped` or `Crashed`, updates lifecycle state in `ApplicationRegistry`, and produces an `ExecutionReport`.

---

## 5. Crash Isolation & Restart Reconciliation

### 5.1 Host Crash Isolation Invariant

WebAssembly traps (e.g. `unreachable`, division by zero, out-of-bounds memory access, fuel exhaustion) are caught by Wasmtime and classified by the supervisor:
- `OutOfFuel` ──► `ApplicationError::FuelExhausted`
- `Interrupt` ──► `ApplicationError::ExecutionTimeout`
- `MemoryOutOfBounds` ──► `ApplicationError::MemoryLimitExceeded`
- `UnreachableCodeReached` ──► `ApplicationError::TrapError`
- Supervisor cancellation ──► `ApplicationError::ExecutionCancelled`

The host runtime **never panics, crashes, or leaks resources** when a guest traps.

### 5.2 Restart Reconciliation (`reconcile_on_startup`)

Upon node reboot or restart, applications left in-flight from a previous session are automatically reconciled:
- `Starting` ──► `Crashed` (startup was interrupted by node shutdown).
- `Running` ──► `Stopped` (in-flight execution halted on node restart).

---

## 6. Runtime & CLI Extended Commands

### 6.1 `FieldnetRuntime` APIs (`fieldnet-runtime/src/runtime.rs`)

- [`run_application(&self, app_id: &ApplicationId)`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-runtime/src/runtime.rs): Executes application.
- [`run_application_with_id(&self, app_id: &ApplicationId, exec_id: ExecutionId)`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-runtime/src/runtime.rs): Executes application with pre-assigned execution identifier.
- [`stop_application_execution(&self, exec_id: &ExecutionId)`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-runtime/src/runtime.rs): Cleanly halts active execution.
- [`list_application_executions(&self)`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-runtime/src/runtime.rs): Lists running instances.
- [`authorize_application(&self, app_id: &ApplicationId)`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-runtime/src/runtime.rs): Transitions installed application to authorized.

### 6.2 CLI Subcommands (`fieldnet-runtime/src/main.rs`)

```bash
# Execute an authorized application
fieldnet-node app run <app_id>

# Stop an active execution instance
fieldnet-node app stop <execution_id>

# List all currently active executions
fieldnet-node app executions
```

---

## 7. Verification & Test Coverage

### 7.1 Wasmtime Supervisor Test Suite (`execution_supervisor_tests.rs`)
21 real WebAssembly tests (compiled from WAT using `wat::parse_str`):
1. `test_01_wasmtime_engine_creation`
2. `test_02_valid_module_loading`
3. `test_03_invalid_wasm_rejection`
4. `test_04_package_to_module_loading`
5. `test_05_content_hash_mismatch_rejection`
6. `test_06_module_cache_hit_and_invalidation`
7. `test_07_valid_application_startup_and_clean_shutdown`
8. `test_08_invalid_lifecycle_transition_rejection`
9. `test_09_trap_handling_and_crash_state_transition`
10. `test_10_fuel_exhaustion_enforcement`
11. `test_11_invalid_memory_access_trapping`
12. `test_12_memory_limit_enforcement`
13. `test_13_missing_entrypoint_rejection`
14. `test_14_execution_timeout_enforcement`
15. `test_15_supervisor_controlled_cancellation`
16. `test_16_concurrent_applications`
17. `test_17_max_concurrency_enforcement`
18. `test_18_runtime_restart_reconciliation`
19. `test_19_execution_id_uniqueness`
20. `test_20_runtime_survives_application_crash`
21. `test_21_isolation_boundary_zero_host_functions`

### 7.2 End-to-End Runtime Integration (`phase09_execution_integration_test.rs`)
4 full-stack integration tests:
1. `test_fieldnet_runtime_wasm_execution_end_to_end`
2. `test_fieldnet_runtime_crash_isolation`
3. `test_fieldnet_runtime_execution_cancellation`
4. `test_fieldnet_runtime_restart_reconciliation`
