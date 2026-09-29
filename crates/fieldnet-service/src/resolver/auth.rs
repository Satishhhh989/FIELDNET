use fieldnet_core::NodeId;
use serde::{Deserialize, Serialize};

use crate::error::ServiceError;
use crate::model::ServicePermissions;

/// Identity and authorization credentials presented by a resolving client or node.
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub struct AuthContext {
    /// Sovereign `NodeId` of the requesting node or client.
    pub caller_node_id: Option<NodeId>,
    /// Authorized roles assigned to the caller (e.g. "admin", "telemetry", "operator").
    pub roles: Vec<String>,
    /// Capability tokens presented by the caller for fine-grained authorization.
    pub capability_tokens: Vec<String>,
}

impl AuthContext {
    /// Anonymous or unauthenticated calling context.
    pub fn anonymous() -> Self {
        Self::default()
    }

    /// Context identified by a specific caller `NodeId`.
    pub fn for_node(node_id: NodeId) -> Self {
        Self {
            caller_node_id: Some(node_id),
            roles: Vec::new(),
            capability_tokens: Vec::new(),
        }
    }

    /// Appends an authorized role to this context.
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.roles.push(role.into());
        self
    }

    /// Appends multiple authorized roles to this context.
    pub fn with_roles(mut self, roles: Vec<String>) -> Self {
        self.roles.extend(roles);
        self
    }

    /// Appends a capability token to this context.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.capability_tokens.push(token.into());
        self
    }

    /// Evaluates whether this calling context satisfies a service's `ServicePermissions`.
    pub fn check_authorization(
        &self,
        service_id_str: &str,
        permissions: &ServicePermissions,
    ) -> Result<(), ServiceError> {
        // 1. If service is explicitly declared public, anyone can access it
        if permissions.is_public {
            return Ok(());
        }

        // 2. If service requires a capability token, check that caller provided one
        if permissions.requires_capability_token && self.capability_tokens.is_empty() {
            return Err(ServiceError::Unauthorized {
                service_id: service_id_str.to_string(),
                reason: "Service requires a capability token, but none was provided".to_string(),
            });
        }

        // 3. If service requires specific roles, check that caller has at least one matching role
        if !permissions.required_roles.is_empty() {
            let has_matching_role = permissions
                .required_roles
                .iter()
                .any(|req| self.roles.iter().any(|r| r.eq_ignore_ascii_case(req)));

            if !has_matching_role {
                return Err(ServiceError::Unauthorized {
                    service_id: service_id_str.to_string(),
                    reason: format!(
                        "Caller lacks any of the required roles: {:?}",
                        permissions.required_roles
                    ),
                });
            }
        }

        Ok(())
    }
}
