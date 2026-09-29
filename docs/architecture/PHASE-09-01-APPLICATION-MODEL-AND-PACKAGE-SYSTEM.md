# PHASE 09 — BUILD 01: APPLICATION MODEL & PACKAGE SYSTEM

## 1. Executive Summary

Phase 09 Build 01 establishes the foundational application and package architecture for FIELDNET. It defines portable, cryptographically signed, content-addressed application packages that can be verified, registered, updated, rolled back, and inspected across the distributed mesh.

Crucially, this build establishes the data models, state machines, persistent registries, and security boundaries **without prematurely introducing Wasmtime execution or runtime sandboxing**.

---

## 2. Architecture & Subsystem Integration

```text
Application Package (.json / CAS)
    │
    ├── Manifest (schema v1)
    │     ├── application_id ("fieldnet.chat")
    │     ├── version ("1.0.0")
    │     ├── publisher ("pub:<ed25519_hex>")
    │     ├── entrypoint ("app.wasm")
    │     ├── permissions (storage.read, network.send, ...)
    │     ├── resource_limits (memory, time, fuel, storage)
    │     └── dependencies (services, apps, content)
    │
    ├── Items (CAS references)
    │     ├── app.wasm (ContentId: SHA-256)
    │     └── assets/icon.png (ContentId: SHA-256)
    │
    ├── Package Hash (Deterministic ContentId over Manifest + Sorted Items)
    └── Package Signature (Ed25519 signature over Package Hash by Publisher)
```

### Installation Pipeline

```text
PACKAGE RECEIVED
       │
PACKAGE INTEGRITY VERIFIED (Items checked against ContentId hashes)
       │
MANIFEST PARSED & VALIDATED (Entrypoint check, schema check, limits check)
       │
SIGNATURE VERIFIED (Ed25519 verification against Package Hash & Publisher)
       │
DEPENDENCIES VALIDATED (Identity & semver version requirements)
       │
CONTENT COMMITTED TO CAS (fieldnet-content: ContentStore, pinned)
       │
ATOMIC REGISTRY TRANSACTION (SQLite applications table + rollback history)
       │
INSTALLED STATE EMITTED (EventBus: application.installed)
```

---

## 3. Core Modules & Data Types (`fieldnet-wasm`)

### 3.1 Sovereign Application Identity (`id.rs`)
- [`ApplicationId`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/id.rs):
  - Strongly typed, hierarchical, reverse-domain or dot-separated identifier (e.g. `fieldnet.chat`, `com.acme.sensor`).
  - Validation invariants: 3..=128 characters, at least two dot-separated segments, lowercase ASCII alphanumerics, hyphens, and underscores.
  - Rejects colons, slashes, backslashes, whitespace, consecutive dots/hyphens/underscores, and path traversal tokens.
  - Completely independent of node identity (`NodeId`), service identity (`ServiceId`), and filesystem installation paths.
- [`PackageId`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/id.rs):
  - Unique identifier for a specific package release artifact.
- [`PublisherIdentity`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/id.rs):
  - Wraps a sovereign Ed25519 32-byte verifying public key (`pub:<hex>`), distinct from any mesh transport or node identity.
- [`AppVersion`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/id.rs) & [`AppVersionReq`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/id.rs):
  - Full semantic versioning (`major.minor.patch[-prerelease]`) with comparison ordering and requirement matching (`=`, `>=`, `^`, `*`).

### 3.2 Application Manifest (`manifest.rs`)
- [`ApplicationManifest`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/manifest.rs):
  - Versioned schema (`schema_version: 1`).
  - Contains `application_id`, `name`, `version`, `description`, `publisher`, `entrypoint`, `permissions`, `resource_limits`, `dependencies`, and `package_metadata`.
  - Canonical byte serialization via deterministic JSON for cryptographic hashing.
  - Strict validation: entrypoint must end in `.wasm` and contain no directory traversal or path separators.

### 3.3 Application Package & Integrity (`package.rs`)
- [`PackageItem`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/package.rs):
  - Represents an individual file or asset referenced by its SHA-256 [`ContentId`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-content/src/identity.rs) and size in bytes.
- [`ApplicationPackage`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/package.rs):
  - Bundles manifest and sorted items.
  - `compute_package_hash()` computes a deterministic SHA-256 [`ContentId`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-content/src/identity.rs) over domain separation tag, canonical manifest bytes, and all sorted item paths, hashes, and sizes.
  - `verify_integrity()` validates internal consistency, entrypoint existence, and hashes of all item payloads.

### 3.4 Publisher Provenance Signature (`signature.rs`)
- [`PackageSignature`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/signature.rs):
  - Cryptographic Ed25519 signature over `FIELDNET_PACKAGE_V1:` || `package_hash` || `signed_at_ms`.
  - `verify()` verifies that the signature was generated by the publisher's sovereign public key.
  - Proves publisher provenance, decoupled from content hash which proves data integrity.

### 3.5 Capability Permissions (`permissions.rs`)
- [`AppPermission`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/permissions.rs):
  - Declarations only at this stage:
    - `storage.read`, `storage.write`
    - `network.send`
    - `service.discover`, `service.invoke`
    - `events.subscribe`
    - `compute.submit`
    - `content.read`, `content.write`
    - `Custom(String)`
  - Permissions in manifests declare requirements; actual host access must be granted by the node authorization system. Unknown permissions are never silently granted.

### 3.6 Resource Limits Configuration (`limits.rs`)
- [`ResourceLimits`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/limits.rs):
  - Declarative sandboxing resource bounds: memory (default 64MB, hard cap 1GB), execution time (default 30s), CPU fuel (default 1B), storage (default 100MB), network bandwidth (default 10MB/s), and concurrency (default 4).

### 3.7 Dependency Model (`dependencies.rs`)
- [`AppDependency`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/dependencies.rs):
  - Declares requirements for external applications, mesh services, or content assets.
  - Supports semver requirement matching and optional dependency flags.

### 3.8 Operational Lifecycle State Machine (`lifecycle.rs`)
- [`ApplicationState`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/lifecycle.rs):
  - States: `DISCOVERED`, `VERIFIED`, `INSTALLED`, `AUTHORIZED`, `STARTING`, `RUNNING`, `SUSPENDED`, `STOPPED`, `CRASHED`, `ISOLATED`.
  - Explicit state transitions enforced. Disallowed transitions (e.g. `Discovered -> Running` or `Installed -> Running`) are rejected with `InvalidLifecycleTransition`.

### 3.9 Persistent Application Registry & Rollback (`registry.rs`)
- [`ApplicationRegistry`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/registry.rs):
  - Backed by persistent SQLite database (`applications.db`) using existing `rusqlite` infrastructure.
  - Tables:
    - `applications`: tracks active application records, manifests, publisher, state, permissions, resource limits, and metadata.
    - `application_history`: tracks historical package versions for deterministic rollback.
  - Transactional upgrades archive prior package records before activating new versions.
  - `rollback()` restores previous package manifest and version atomically.

### 3.10 Atomic Installer & Event Bus (`installer.rs`, `events.rs`)
- [`ApplicationInstaller`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/installer.rs):
  - Orchestrates package verification, dependency validation, CAS storage, and registry persistence.
  - Emits structured [`ApplicationEvent`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/events.rs) notifications onto the runtime event bus (`broadcast::Sender<ApplicationEvent>`).

### 3.11 Content-Addressed Storage Integration (`cas.rs`)
- [`CasPackageStorage`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-wasm/src/cas.rs):
  - Stores all items into [`ContentStore`](file:///Users/satish/Desktop/fieldnet%20runtime/crates/fieldnet-content/src/store.rs) pinned against eviction.
  - Stores root package descriptor JSON into CAS.
  - Allows verified retrieval and loading of packages directly from CAS by root `ContentId`.

### 3.12 CLI Inspection (`main.rs`)
- `fieldnet-node app list`: Lists all installed applications, versions, publishers, states, and signature statuses.
- `fieldnet-node app info <app-id>`: Displays comprehensive details, manifest, permissions, limits, dependencies, and rollback history.
- `fieldnet-node app verify <package-path>`: Reads and cryptographically verifies an application package file from disk.

---

## 4. Quality Gate Summary

- `cargo fmt --check`: 100% clean.
- `cargo check --workspace`: 100% clean.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: 0 warnings, 0 errors.
- `cargo test --workspace -- --test-threads=1`: 100% clean across all 9 crates (365+ tests passed, 0 failed).
- No `TODO`, `FIXME`, `unimplemented!()`, or placeholder security logic.
