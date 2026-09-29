use fieldnet_core::{MessageId, NodeId, Timestamp};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use crate::error::ServiceError;
use crate::model::{ServiceId, ServiceInstanceId};
use crate::resolver::{AuthContext, ServiceQueryBuilder, ServiceResolver};
use crate::wire::{ServiceRpcRequestWire, ServiceRpcResponseWire};

use super::error::GatewayError;
use super::model::{GatewayRequest, GatewayResponse};

/// Trait defining the network RPC dispatch abstraction used by the gateway to reach providers.
#[async_trait::async_trait]
pub trait ServiceDispatcher: Send + Sync {
    async fn dispatch_rpc(
        &self,
        provider_node_id: NodeId,
        request: ServiceRpcRequestWire,
    ) -> Result<ServiceRpcResponseWire, GatewayError>;
}

/// Handler trait for in-process or registered service providers.
#[async_trait::async_trait]
pub trait ServiceRequestHandler: Send + Sync {
    async fn handle_request(
        &self,
        request: ServiceRpcRequestWire,
    ) -> Result<ServiceRpcResponseWire, ServiceError>;
}

/// In-memory dispatcher useful for standalone gateway testing and local service registration.
pub struct LocalServiceDispatcher {
    handlers: Arc<RwLock<HashMap<ServiceId, Arc<dyn ServiceRequestHandler>>>>,
}

impl Default for LocalServiceDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalServiceDispatcher {
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register_handler(
        &self,
        service_id: ServiceId,
        handler: Arc<dyn ServiceRequestHandler>,
    ) {
        let mut guard = self.handlers.write().await;
        guard.insert(service_id, handler);
    }
}

#[async_trait::async_trait]
impl ServiceDispatcher for LocalServiceDispatcher {
    async fn dispatch_rpc(
        &self,
        _provider_node_id: NodeId,
        request: ServiceRpcRequestWire,
    ) -> Result<ServiceRpcResponseWire, GatewayError> {
        let handler = {
            let guard = self.handlers.read().await;
            guard.get(&request.service_id).cloned()
        };

        if let Some(h) = handler {
            h.handle_request(request)
                .await
                .map_err(GatewayError::Service)
        } else {
            Err(GatewayError::Service(ServiceError::ServiceNotFound(
                request.service_id.to_string(),
            )))
        }
    }
}

/// Dispatches an incoming `GatewayRequest` to a resolved service instance via `ServiceResolver`
/// and the provided `ServiceDispatcher`, with safe failover semantics.
pub async fn dispatch_service_request(
    resolver: &Arc<ServiceResolver>,
    dispatcher: &Arc<dyn ServiceDispatcher>,
    local_node_id: NodeId,
    service_id: ServiceId,
    subpath: String,
    request: GatewayRequest,
    auth_context: Option<&AuthContext>,
) -> Result<GatewayResponse, GatewayError> {
    let query = ServiceQueryBuilder::new(service_id.clone())
        .build()
        .map_err(GatewayError::Service)?;
    let is_safe = request.method.is_safe();
    let idempotency_key = request.idempotency_key().map(|s| s.to_string());

    let mut excluded_instances: Vec<ServiceInstanceId> = Vec::new();
    let max_attempts = if is_safe || idempotency_key.is_some() {
        3
    } else {
        1
    };

    let mut last_error = None;

    for attempt in 0..max_attempts {
        // 1. Resolve optimal candidate provider via ServiceResolver boundary (evaluates capabilities, ranking, and excluded providers)
        let resolution = match resolver
            .resolve_with_exclusions(&query, auth_context, &excluded_instances)
            .await
        {
            Ok(res) => res,
            Err(e) => {
                return Err(last_error.unwrap_or(GatewayError::Service(e)));
            }
        };

        let selected_instance_id = resolution.instance_id.clone();
        let provider_node_id = resolution.provider_node_id;

        let request_id = MessageId::from_bytes(rand::random());
        let rpc_req = ServiceRpcRequestWire {
            request_id,
            service_id: service_id.clone(),
            instance_id: selected_instance_id.clone(),
            path: subpath.clone(),
            method: request.method.to_string(),
            query: if request.query_params.is_empty() {
                None
            } else {
                Some(
                    request
                        .query_params
                        .iter()
                        .map(|(k, v)| format!("{}={}", k, v))
                        .collect::<Vec<_>>()
                        .join("&"),
                )
            },
            headers: request.headers.clone(),
            body: request.body.clone(),
            requester_node_id: local_node_id,
            provider_node_id,
            deadline: request.deadline,
            idempotency_key: idempotency_key.clone(),
        };

        tracing::info!(
            "Dispatching HTTP service request '{}' to provider {} instance {} (attempt {}/{})",
            service_id,
            provider_node_id,
            selected_instance_id,
            attempt + 1,
            max_attempts
        );

        let start_time = Instant::now();
        match dispatcher.dispatch_rpc(provider_node_id, rpc_req).await {
            Ok(rpc_resp) => {
                let latency_ms = start_time.elapsed().as_millis().min(u32::MAX as u128) as u32;
                resolver
                    .registry()
                    .record_call_success(&selected_instance_id, latency_ms, Timestamp::now())
                    .await;

                let mut resp = GatewayResponse::new(rpc_resp.status_code, request.request_id);
                resp.headers = rpc_resp.headers;
                resp.content_type = resp.headers.get("content-type").cloned();
                resp.body = rpc_resp.body;
                return Ok(resp);
            }
            Err(e) => {
                tracing::warn!(
                    "Service dispatch failed to provider {} instance {} for '{}': {}",
                    provider_node_id,
                    selected_instance_id,
                    service_id,
                    e
                );

                resolver
                    .registry()
                    .record_call_failure(&selected_instance_id, Timestamp::now())
                    .await;
                resolver.invalidate_cache(&selected_instance_id).await;

                excluded_instances.push(selected_instance_id);
                last_error = Some(e);

                // For state-changing methods without idempotency key, DO NOT RETRY
                if !is_safe && idempotency_key.is_none() {
                    return Err(GatewayError::ExecutionIndeterminate(format!(
                        "Provider {} failed during non-idempotent {} operation; blind retry prohibited",
                        provider_node_id, request.method
                    )));
                }

                // Small backoff before failover retry
                tokio::time::sleep(Duration::from_millis(15)).await;
            }
        }
    }

    Err(last_error.unwrap_or(GatewayError::Service(
        ServiceError::AllProvidersUnavailable {
            service_id: service_id.to_string(),
            count: excluded_instances.len(),
        },
    )))
}
