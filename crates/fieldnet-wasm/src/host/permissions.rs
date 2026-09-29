use std::collections::HashSet;

use crate::host::errors::HostError;
use crate::permissions::AppPermission;

/// Execution-scoped capability context enforcing strict granular authorization.
///
/// An application's effective capabilities are the strict intersection of:
/// 1. Permissions explicitly declared in its verified `ApplicationManifest`.
/// 2. Permissions authorized by the node operator/policy during application authorization.
///
/// Every privileged host operation must perform an explicit runtime check against this context.
#[derive(Clone, Debug)]
pub struct CapabilityContext {
    declared: HashSet<AppPermission>,
    authorized: HashSet<AppPermission>,
    effective: HashSet<AppPermission>,
}

impl CapabilityContext {
    /// Creates a new `CapabilityContext` from declared manifest permissions and authorized permissions.
    pub fn new(
        declared: impl IntoIterator<Item = AppPermission>,
        authorized: impl IntoIterator<Item = AppPermission>,
    ) -> Self {
        let declared_set: HashSet<AppPermission> = declared.into_iter().collect();
        let authorized_set: HashSet<AppPermission> = authorized.into_iter().collect();
        // Effective capability requires BOTH declaration in manifest AND explicit authorization
        let effective_set: HashSet<AppPermission> = declared_set
            .intersection(&authorized_set)
            .cloned()
            .collect();

        Self {
            declared: declared_set,
            authorized: authorized_set,
            effective: effective_set,
        }
    }

    /// Creates a context with zero permissions (default for unprivileged guests).
    pub fn empty() -> Self {
        Self {
            declared: HashSet::new(),
            authorized: HashSet::new(),
            effective: HashSet::new(),
        }
    }

    /// Verifies whether the effective capability includes the requested permission.
    /// Returns `Ok(())` if authorized, or `Err(HostError::PermissionDenied)` if unauthorized.
    pub fn check_permission(&self, required: AppPermission) -> Result<(), HostError> {
        if self.effective.contains(&required) {
            Ok(())
        } else {
            Err(HostError::permission_denied(format!(
                "Application lacks required capability '{}'",
                required.as_str()
            )))
        }
    }

    /// Returns `true` if the requested permission is effectively granted.
    pub fn has_permission(&self, required: AppPermission) -> bool {
        self.effective.contains(&required)
    }

    /// Returns all declared permissions.
    pub fn declared(&self) -> &HashSet<AppPermission> {
        &self.declared
    }

    /// Returns all authorized permissions.
    pub fn authorized(&self) -> &HashSet<AppPermission> {
        &self.authorized
    }

    /// Returns the effective permissions granted to the execution instance.
    pub fn effective(&self) -> &HashSet<AppPermission> {
        &self.effective
    }
}
