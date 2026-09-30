//! FIELDNET Application Model, Package System, and Sandboxed Runtime Architecture (Phase 09).
//!
//! Provides sovereign application identity, versioned manifests, cryptographic package integrity,
//! Ed25519 publisher provenance signatures, resource limit boundaries, persistent application registry,
//! and atomic installation/upgrade/rollback pipelines.

pub mod cas;
pub mod dependencies;
pub mod error;
pub mod events;
pub mod execution;
pub mod host;
pub mod id;
pub mod installer;
pub mod lifecycle;
pub mod limits;
pub mod manager;
pub mod manifest;
pub mod package;
pub mod permissions;
pub mod registry;
pub mod signature;

pub use cas::CasPackageStorage;
pub use dependencies::{
    AppDependency, DependencyGraph, DependencyResolutionReport, DependencyResolver, DependencyType,
    ResolvedDependency,
};
pub use error::ApplicationError;
pub use events::ApplicationEvent;
pub use execution::{
    ActiveExecutionInfo, ApplicationExecutionSupervisor, ExecutionReport, ModuleCache,
    ModuleCacheKey, StoreData, WasmEngine,
};
pub use host::abi::FIELDNET_HOST_API_VERSION;
pub use host::context::HostContext;
pub use host::errors::GuestErrorCode;
pub use host::network::{ApplicationNetworkMessage, ApplicationNetworkSender};
pub use host::permissions::CapabilityContext;
pub use id::{AppVersion, AppVersionReq, ApplicationId, ExecutionId, PackageId, PublisherIdentity};
pub use installer::ApplicationInstaller;
pub use lifecycle::{ApplicationState, UpdateTransactionState};
pub use limits::ResourceLimits;
pub use manager::{ApplicationLifecycleManager, StartupReconciliationReport};
pub use manifest::{ApplicationManifest, CURRENT_MANIFEST_SCHEMA_VERSION};
pub use package::{ApplicationPackage, PackageItem};
pub use permissions::AppPermission;
pub use registry::{
    ApplicationHistoryRecord, ApplicationRecord, ApplicationRegistry, CrashRecord, SignatureStatus,
    UpdateTransactionRecord,
};
pub use signature::{PackageSignature, SignatureAlgorithm};
