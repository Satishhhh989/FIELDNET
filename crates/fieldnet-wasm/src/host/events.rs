use fieldnet_core::Timestamp;
use tokio::sync::broadcast;
use tracing::info;

use crate::events::ApplicationEvent;
use crate::host::abi::MAX_ID_LENGTH;
use crate::host::errors::HostError;
use crate::host::permissions::CapabilityContext;
use crate::id::{ApplicationId, ExecutionId};
use crate::permissions::AppPermission;

/// Event publication interface enforcing application namespace isolation.
#[derive(Clone)]
pub struct HostEvents {
    application_id: ApplicationId,
    execution_id: ExecutionId,
    event_tx: broadcast::Sender<ApplicationEvent>,
}

impl HostEvents {
    pub fn new(
        application_id: ApplicationId,
        execution_id: ExecutionId,
        event_tx: broadcast::Sender<ApplicationEvent>,
    ) -> Self {
        Self {
            application_id,
            execution_id,
            event_tx,
        }
    }

    /// Publishes a custom application event onto the FIELDNET runtime event bus.
    ///
    /// Requires capability: `events.publish`
    /// Enforces canonical namespace: `app.<application_id>.<event>`
    pub fn publish(
        &self,
        caps: &CapabilityContext,
        event_type: &str,
        payload: &[u8],
    ) -> Result<String, HostError> {
        caps.check_permission(AppPermission::EventsPublish)?;

        let trimmed = event_type.trim();
        if trimmed.is_empty() || trimmed.len() > MAX_ID_LENGTH {
            return Err(HostError::invalid_request(
                "Event type must be non-empty and within maximum length limits",
            ));
        }

        // 1. Prevent injection into runtime-owned lifecycle namespaces
        let lower = trimmed.to_ascii_lowercase();
        let reserved_prefixes = [
            "application.",
            "node.",
            "service.",
            "content.",
            "dtn.",
            "network.",
            "system.",
        ];
        for prefix in &reserved_prefixes {
            if lower.starts_with(prefix) {
                return Err(HostError::invalid_request(format!(
                    "Reserved runtime event namespace '{}' cannot be injected by applications",
                    prefix
                )));
            }
        }

        // Validate character set: ASCII alphanumeric, dots, hyphens, underscores
        if !trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
        {
            return Err(HostError::invalid_request(
                "Event type contains invalid characters (allowed: alphanumeric, '.', '-', '_')",
            ));
        }

        // 2. Canonical Namespace Construction and Anti-Spoofing Check
        let expected_prefix = format!("app.{}.", self.application_id);
        let canonical_topic = if trimmed.starts_with(&expected_prefix) {
            // Already includes current app prefix
            trimmed.to_string()
        } else if trimmed.starts_with("app.") {
            // Tries to use another app's prefix or malformed prefix
            return Err(HostError::invalid_request(
                "Cannot spoof foreign application event namespaces",
            ));
        } else {
            // Canonicalize by prepending app namespace
            format!("app.{}.{}", self.application_id, trimmed)
        };

        // 3. Emit onto Event Bus
        let event = ApplicationEvent::Custom {
            application_id: self.application_id.clone(),
            execution_id: self.execution_id.clone(),
            topic: canonical_topic.clone(),
            payload: payload.to_vec(),
            timestamp: Timestamp::now().as_millis(),
        };

        let _ = self.event_tx.send(event);

        info!(
            application_id = %self.application_id,
            execution_id = %self.execution_id,
            topic = %canonical_topic,
            payload_size = payload.len(),
            "Application event published"
        );

        Ok(canonical_topic)
    }
}
