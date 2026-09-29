use serde::{Deserialize, Serialize};

use crate::error::ApplicationError;
use crate::id::{AppVersion, AppVersionReq};

/// Category of dependency declared by an application.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyType {
    /// Dependency on another FIELDNET application package.
    Application,
    /// Dependency on a mesh service (e.g. "files.service").
    Service,
    /// Dependency on a specific content-addressed asset or library.
    Content,
}

/// Declaration of a dependency required by an application.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct AppDependency {
    /// Identifier of the dependency (e.g. "fieldnet.chat", "files.service", or content CID).
    pub identity: String,

    /// Version requirement specification (e.g. ">=1.0.0", "^0.2.0", "*").
    pub version_requirement: AppVersionReq,

    /// Whether this dependency is optional for application startup.
    #[serde(default)]
    pub optional: bool,

    /// Type of dependency.
    pub dependency_type: DependencyType,
}

impl AppDependency {
    pub fn new(
        identity: impl Into<String>,
        version_req: AppVersionReq,
        dependency_type: DependencyType,
        optional: bool,
    ) -> Result<Self, ApplicationError> {
        let id_str = identity.into().trim().to_string();
        if id_str.is_empty() {
            return Err(ApplicationError::DependencyError(
                "Dependency identity cannot be empty".to_string(),
            ));
        }

        Ok(Self {
            identity: id_str,
            version_requirement: version_req,
            optional,
            dependency_type,
        })
    }

    /// Validates the dependency declaration.
    pub fn validate(&self) -> Result<(), ApplicationError> {
        if self.identity.trim().is_empty() {
            return Err(ApplicationError::DependencyError(
                "Dependency identity cannot be empty".to_string(),
            ));
        }
        Ok(())
    }

    /// Evaluates whether an installed/available version satisfies this dependency requirement.
    pub fn is_satisfied_by(&self, version: &AppVersion) -> bool {
        self.version_requirement.matches(version)
    }
}
