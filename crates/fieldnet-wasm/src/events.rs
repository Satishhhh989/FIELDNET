use fieldnet_content::identity::ContentId;
use serde::{Deserialize, Serialize};

use crate::id::{ApplicationId, ExecutionId, PackageId, PublisherIdentity};
use crate::permissions::AppPermission;

/// Structured application lifecycle events emitted onto the FIELDNET runtime event bus.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApplicationEvent {
    /// A new application package was discovered or announced.
    Discovered {
        application_id: ApplicationId,
        package_id: PackageId,
        publisher: PublisherIdentity,
    },

    /// An application package's content integrity and publisher signature were verified.
    Verified {
        application_id: ApplicationId,
        package_id: PackageId,
        package_hash: ContentId,
    },

    /// Application installation has started.
    Installing {
        application_id: ApplicationId,
        package_id: PackageId,
        version: String,
    },

    /// Application installation has completed and been recorded into the persistent catalog.
    Installed {
        application_id: ApplicationId,
        package_id: PackageId,
        version: String,
    },

    /// Capability permissions were authorized for this application by node policy.
    Authorized {
        application_id: ApplicationId,
        granted_permissions: Vec<AppPermission>,
    },

    /// Dependency checking initiated for an application.
    DependencyChecking {
        application_id: ApplicationId,
        dependency_count: usize,
    },

    /// Dependency resolution failed for an application.
    DependencyFailed {
        application_id: ApplicationId,
        reason: String,
    },

    /// Application update transaction started.
    UpdateStarted {
        application_id: ApplicationId,
        target_version: String,
        package_id: PackageId,
    },

    /// Application update staged into CAS and validated.
    UpdateStaged {
        application_id: ApplicationId,
        version: String,
        package_id: PackageId,
    },

    /// Application update activated and committed.
    UpdateActivated {
        application_id: ApplicationId,
        from_version: String,
        to_version: String,
        package_id: PackageId,
    },

    /// Application update failed and was aborted.
    UpdateFailed {
        application_id: ApplicationId,
        target_version: String,
        reason: String,
    },

    /// Application was upgraded to a new package version.
    Updated {
        application_id: ApplicationId,
        from_version: String,
        to_version: String,
        package_id: PackageId,
    },

    /// Application rollback initiated.
    RollbackStarted {
        application_id: ApplicationId,
        target_version: String,
    },

    /// Application rollback completed successfully.
    RollbackCompleted {
        application_id: ApplicationId,
        restored_version: String,
        package_id: PackageId,
    },

    /// Application was rolled back to a previous valid version.
    Rollback {
        application_id: ApplicationId,
        from_version: String,
        to_version: String,
        package_id: PackageId,
    },

    /// Application execution environment is initializing.
    Starting {
        application_id: ApplicationId,
        package_id: PackageId,
        execution_id: ExecutionId,
        timestamp: u64,
    },

    /// Application execution environment has started and entered running state.
    Started {
        application_id: ApplicationId,
        package_id: PackageId,
        execution_id: ExecutionId,
        timestamp: u64,
    },

    /// Application execution was stopped cleanly.
    Stopped {
        application_id: ApplicationId,
        package_id: Option<PackageId>,
        execution_id: Option<ExecutionId>,
        reason: Option<String>,
        timestamp: u64,
    },

    /// Application execution crashed or encountered an unhandled fault.
    Crashed {
        application_id: ApplicationId,
        package_id: Option<PackageId>,
        execution_id: Option<ExecutionId>,
        error: String,
        timestamp: u64,
    },

    /// Application was quarantined / isolated due to a security violation or fault.
    Isolated {
        application_id: ApplicationId,
        package_id: Option<PackageId>,
        execution_id: Option<ExecutionId>,
        reason: String,
        timestamp: u64,
    },

    /// Application uninstalled from node catalog.
    Uninstalled { application_id: ApplicationId },

    /// A custom event published by an authorized application guest.
    Custom {
        application_id: ApplicationId,
        execution_id: ExecutionId,
        topic: String,
        payload: Vec<u8>,
        timestamp: u64,
    },
}

impl ApplicationEvent {
    /// Canonical topic string matching FIELDNET event naming conventions.
    pub fn topic(&self) -> &str {
        match self {
            Self::Discovered { .. } => "application.discovered",
            Self::Verified { .. } => "application.verified",
            Self::Installing { .. } => "application.installing",
            Self::Installed { .. } => "application.installed",
            Self::Authorized { .. } => "application.authorized",
            Self::DependencyChecking { .. } => "application.dependency_checking",
            Self::DependencyFailed { .. } => "application.dependency_failed",
            Self::UpdateStarted { .. } => "application.update_started",
            Self::UpdateStaged { .. } => "application.update_staged",
            Self::UpdateActivated { .. } => "application.update_activated",
            Self::UpdateFailed { .. } => "application.update_failed",
            Self::Updated { .. } => "application.updated",
            Self::RollbackStarted { .. } => "application.rollback_started",
            Self::RollbackCompleted { .. } => "application.rollback_completed",
            Self::Rollback { .. } => "application.rollback",
            Self::Starting { .. } => "application.starting",
            Self::Started { .. } => "application.started",
            Self::Stopped { .. } => "application.stopped",
            Self::Crashed { .. } => "application.crashed",
            Self::Isolated { .. } => "application.isolated",
            Self::Uninstalled { .. } => "application.uninstalled",
            Self::Custom { topic, .. } => topic.as_str(),
        }
    }

    /// ApplicationId associated with this event.
    pub fn application_id(&self) -> &ApplicationId {
        match self {
            Self::Discovered { application_id, .. } => application_id,
            Self::Verified { application_id, .. } => application_id,
            Self::Installing { application_id, .. } => application_id,
            Self::Installed { application_id, .. } => application_id,
            Self::Authorized { application_id, .. } => application_id,
            Self::DependencyChecking { application_id, .. } => application_id,
            Self::DependencyFailed { application_id, .. } => application_id,
            Self::UpdateStarted { application_id, .. } => application_id,
            Self::UpdateStaged { application_id, .. } => application_id,
            Self::UpdateActivated { application_id, .. } => application_id,
            Self::UpdateFailed { application_id, .. } => application_id,
            Self::Updated { application_id, .. } => application_id,
            Self::RollbackStarted { application_id, .. } => application_id,
            Self::RollbackCompleted { application_id, .. } => application_id,
            Self::Rollback { application_id, .. } => application_id,
            Self::Starting { application_id, .. } => application_id,
            Self::Started { application_id, .. } => application_id,
            Self::Stopped { application_id, .. } => application_id,
            Self::Crashed { application_id, .. } => application_id,
            Self::Isolated { application_id, .. } => application_id,
            Self::Uninstalled { application_id, .. } => application_id,
            Self::Custom { application_id, .. } => application_id,
        }
    }
}
