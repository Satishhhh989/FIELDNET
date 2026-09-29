use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE, HOST, ORIGIN};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use axum::Router;
use fieldnet_content::{ContentId, ContentStore};
use fieldnet_core::{NodeId, Timestamp};
use serde_json::json;
use tokio::sync::broadcast;

use crate::events::ServiceEvent;
use crate::model::ServiceId;
use crate::registry::ServiceRegistry;
use crate::resolver::{AuthContext, ServiceResolver};

use super::auth::{
    extract_bearer_token, inject_security_headers, validate_csrf, validate_host_header,
    validate_origin, validate_proxy_target,
};
use super::config::GatewayConfig;
use super::discovery_api::BrowserServiceCatalog;
use super::dispatch::{dispatch_service_request, ServiceDispatcher};
use super::error::GatewayError;
use super::model::{GatewayErrorResponse, GatewayMethod, GatewayRequest};
use super::pairing_api::{
    handle_pair_confirm, handle_pair_start, handle_pair_status, handle_session_refresh,
    handle_session_revoke,
};
use super::session::BrowserSessionStore;
use super::state::GatewayStateMachine;
use super::streaming::serve_content_stream;

/// Shared application context holding runtime services for the gateway router.
pub struct GatewayContext {
    pub config: GatewayConfig,
    pub state_machine: GatewayStateMachine,
    pub node_id: NodeId,
    pub registry: Arc<ServiceRegistry>,
    pub resolver: Arc<ServiceResolver>,
    pub dispatcher: Arc<dyn ServiceDispatcher>,
    pub content_store: Option<Arc<ContentStore>>,
    pub session_store: Arc<BrowserSessionStore>,
    pub event_tx: Option<broadcast::Sender<ServiceEvent>>,
    pub start_time: Timestamp,
}

/// Helper to determine if the request is targeting the local loopback interface.
pub fn is_loopback_client(host: Option<&str>, origin: Option<&str>) -> bool {
    if let Some(h) = host {
        let clean = if h.starts_with('[') {
            if let Some(end) = h.find(']') {
                &h[..=end]
            } else {
                h
            }
        } else if let Some(idx) = h.find(':') {
            &h[..idx]
        } else {
            h
        };
        let lower = clean.to_lowercase();
        if lower != "localhost" && lower != "127.0.0.1" && lower != "[::1]" && lower != "::1" {
            return false;
        }
    } else {
        return false;
    }

    if let Some(orig) = origin {
        let orig_lower = orig.to_lowercase();
        if !orig_lower.starts_with("http://localhost")
            && !orig_lower.starts_with("http://127.0.0.1")
            && !orig_lower.starts_with("http://[::1]")
        {
            return false;
        }
    }

    true
}

/// Constructs the authoritative Axum router for the Local Web Gateway.
pub fn build_router(ctx: Arc<GatewayContext>) -> Router {
    Router::new()
        // 1. Root and Status APIs
        .route("/", get(handle_root))
        .route("/api/v1/status", get(handle_status))
        // 2. Browser Service Discovery API
        .route("/api/v1/services", get(handle_services_catalog))
        .route("/api/services", get(handle_services_catalog))
        // 3. Browser Pairing & Session APIs (Phase 08 BUILD 07)
        .route("/api/v1/pair/start", post(handle_pair_start))
        .route("/api/v1/pair/status", get(handle_pair_status))
        .route("/api/v1/pair/confirm", post(handle_pair_confirm))
        .route("/api/v1/session/refresh", post(handle_session_refresh))
        .route("/api/v1/session/revoke", post(handle_session_revoke))
        // 4. Phase 07 CAS Content Delivery API
        .route("/api/v1/content/{content_id}", get(handle_content_stream))
        // 5. Dynamic Service Routing: /services/{service_id} and /services/{service_id}/{*subpath}
        .route("/services/{service_id}", any(handle_service_root))
        .route(
            "/services/{service_id}/{*subpath}",
            any(handle_service_subpath),
        )
        // 6. Fallback handler for direct service paths (e.g. /{service_id}/...) or 404
        .fallback(handle_fallback)
        // Resource Limits
        .layer(DefaultBodyLimit::max(ctx.config.max_request_size_bytes))
        .with_state(ctx)
}

/// Helper to serialize a GatewayError into a structured HTTP Response.
pub fn error_to_response(err: &GatewayError, request_id: &str) -> Response<Body> {
    let status =
        StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let payload = GatewayErrorResponse {
        error: err.error_code().to_string(),
        code: status.as_u16(),
        message: err.to_string(),
        request_id: request_id.to_string(),
    };
    let mut resp = Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(payload.to_json_bytes()))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    inject_security_headers(&mut resp);
    resp
}

/// Adds CORS headers to an outgoing response based on client Origin and config.
pub fn inject_cors_headers(
    resp: &mut Response<Body>,
    origin: Option<&str>,
    config: &GatewayConfig,
) {
    if let Some(orig) = origin {
        if validate_origin(orig, config) {
            if let Ok(hdr_val) = HeaderValue::from_str(orig) {
                resp.headers_mut().insert(
                    HeaderName::from_static("access-control-allow-origin"),
                    hdr_val,
                );
                resp.headers_mut().insert(
                    HeaderName::from_static("access-control-allow-credentials"),
                    HeaderValue::from_static("true"),
                );
                resp.headers_mut().insert(
                    HeaderName::from_static("access-control-allow-methods"),
                    HeaderValue::from_static("GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS"),
                );
                resp.headers_mut().insert(
                    HeaderName::from_static("access-control-allow-headers"),
                    HeaderValue::from_static("Content-Type, Authorization, X-Request-ID, Idempotency-Key, X-Fieldnet-Client, Range"),
                );
                resp.headers_mut().insert(
                    HeaderName::from_static("access-control-expose-headers"),
                    HeaderValue::from_static("Content-Range, ETag, Accept-Ranges, Content-Length"),
                );
            }
        }
    }
}

/// GET / — Root environment information
async fn handle_root(State(ctx): State<Arc<GatewayContext>>, headers: HeaderMap) -> Response<Body> {
    let origin = headers.get(ORIGIN).and_then(|h| h.to_str().ok());
    if let Some(host) = headers.get(HOST).and_then(|h| h.to_str().ok()) {
        if let Err(e) = validate_host_header(host, &ctx.config) {
            return error_to_response(&e, "root");
        }
    }

    let uptime = Timestamp::now()
        .as_millis()
        .saturating_sub(ctx.start_time.as_millis())
        / 1000;
    let state = ctx.state_machine.current().await;

    let payload = json!({
        "fieldnet": "LocalWebGateway",
        "version": "0.1.0",
        "node_id": ctx.node_id.to_string(),
        "state": state.to_string(),
        "uptime_seconds": uptime,
        "endpoints": [
            "/",
            "/api/v1/status",
            "/api/v1/services",
            "/api/v1/pair/start",
            "/api/v1/pair/status",
            "/api/v1/pair/confirm",
            "/api/v1/session/refresh",
            "/api/v1/session/revoke",
            "/api/v1/content/{content_id}",
            "/services/{service_id}/*path"
        ]
    });

    let mut resp = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap();

    inject_cors_headers(&mut resp, origin, &ctx.config);
    inject_security_headers(&mut resp);
    resp
}

/// GET /api/v1/status — Node and Gateway health inspection
async fn handle_status(
    State(ctx): State<Arc<GatewayContext>>,
    headers: HeaderMap,
) -> Response<Body> {
    let origin = headers.get(ORIGIN).and_then(|h| h.to_str().ok());
    if let Some(host) = headers.get(HOST).and_then(|h| h.to_str().ok()) {
        if let Err(e) = validate_host_header(host, &ctx.config) {
            return error_to_response(&e, "status");
        }
    }

    let uptime = Timestamp::now()
        .as_millis()
        .saturating_sub(ctx.start_time.as_millis())
        / 1000;
    let state = ctx.state_machine.current().await;
    let active_sessions = ctx.session_store.active_session_count().await;

    let payload = json!({
        "status": "healthy",
        "gateway_state": state.to_string(),
        "node_id": ctx.node_id.to_string(),
        "active_connections": ctx.state_machine.connections(),
        "active_requests": ctx.state_machine.requests(),
        "active_sessions": active_sessions,
        "uptime_seconds": uptime,
    });

    let mut resp = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .header("Cache-Control", "no-store, no-cache, must-revalidate")
        .body(Body::from(payload.to_string()))
        .unwrap();

    inject_cors_headers(&mut resp, origin, &ctx.config);
    inject_security_headers(&mut resp);
    resp
}

/// GET /api/v1/services (and /api/services) — Sanitized Browser Service Discovery Catalog
async fn handle_services_catalog(
    State(ctx): State<Arc<GatewayContext>>,
    headers: HeaderMap,
) -> Response<Body> {
    let origin = headers.get(ORIGIN).and_then(|h| h.to_str().ok());
    if let Some(host) = headers.get(HOST).and_then(|h| h.to_str().ok()) {
        if let Err(e) = validate_host_header(host, &ctx.config) {
            return error_to_response(&e, "catalog");
        }
    }

    let catalog = BrowserServiceCatalog::from_registry(&ctx.registry, ctx.node_id).await;
    let json_bytes = match serde_json::to_vec(&catalog) {
        Ok(b) => b,
        Err(e) => {
            return error_to_response(
                &GatewayError::ServerError(format!("Catalog serialization failed: {}", e)),
                "catalog",
            );
        }
    };

    let mut resp = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(json_bytes))
        .unwrap();

    inject_cors_headers(&mut resp, origin, &ctx.config);
    inject_security_headers(&mut resp);
    resp
}

/// GET /api/v1/content/{content_id} — Stream content from CAS ContentStore
async fn handle_content_stream(
    State(ctx): State<Arc<GatewayContext>>,
    Path(content_id_hex): Path<String>,
    headers: HeaderMap,
) -> Response<Body> {
    let origin = headers.get(ORIGIN).and_then(|h| h.to_str().ok());
    let host = headers.get(HOST).and_then(|h| h.to_str().ok());
    if let Some(h) = host {
        if let Err(e) = validate_host_header(h, &ctx.config) {
            return error_to_response(&e, &content_id_hex);
        }
    }

    // Check authorization for content read
    let token = extract_bearer_token(&headers);
    let is_loopback = is_loopback_client(host, origin);

    if let Some(t) = token {
        match ctx.session_store.validate_token(t).await {
            Ok(session) => {
                if !session.has_capability("content.read") {
                    let err = GatewayError::SessionForbidden {
                        reason: "Session lacks 'content.read' capability".to_string(),
                    };
                    let mut resp = error_to_response(&err, &content_id_hex);
                    inject_cors_headers(&mut resp, origin, &ctx.config);
                    inject_security_headers(&mut resp);
                    return resp;
                }
            }
            Err(e) => {
                let mut resp = error_to_response(&e, &content_id_hex);
                inject_cors_headers(&mut resp, origin, &ctx.config);
                inject_security_headers(&mut resp);
                return resp;
            }
        }
    } else if !(ctx.config.auto_pair_localhost && is_loopback) {
        let err = GatewayError::SessionUnauthorized {
            reason: "Authentication required to stream CAS content".to_string(),
        };
        let mut resp = error_to_response(&err, &content_id_hex);
        inject_cors_headers(&mut resp, origin, &ctx.config);
        inject_security_headers(&mut resp);
        return resp;
    }

    let content_id = match ContentId::from_hex(&content_id_hex) {
        Ok(id) => id,
        Err(_) => {
            return error_to_response(
                &GatewayError::Service(crate::error::ServiceError::InvalidQuery(format!(
                    "Invalid ContentId hex: {}",
                    content_id_hex
                ))),
                &content_id_hex,
            );
        }
    };

    let store = match &ctx.content_store {
        Some(s) => s,
        None => {
            return (
                StatusCode::NOT_IMPLEMENTED,
                "ContentStore is not enabled on this gateway node",
            )
                .into_response();
        }
    };

    let mut resp = serve_content_stream(store, &content_id, &headers).await;
    inject_cors_headers(&mut resp, origin, &ctx.config);
    inject_security_headers(&mut resp);
    resp
}

/// Handles `/services/{service_id}` root requests
async fn handle_service_root(
    State(ctx): State<Arc<GatewayContext>>,
    Path(service_id_raw): Path<String>,
    Query(query_params): Query<HashMap<String, String>>,
    req: Request,
) -> Response<Body> {
    process_service_request(ctx, service_id_raw, "/".to_string(), query_params, req).await
}

/// Handles `/services/{service_id}/{*subpath}` requests
async fn handle_service_subpath(
    State(ctx): State<Arc<GatewayContext>>,
    Path((service_id_raw, subpath)): Path<(String, String)>,
    Query(query_params): Query<HashMap<String, String>>,
    req: Request,
) -> Response<Body> {
    let clean_subpath = if subpath.starts_with('/') {
        subpath
    } else {
        format!("/{}", subpath)
    };
    process_service_request(ctx, service_id_raw, clean_subpath, query_params, req).await
}

/// Unified processing for dynamic service requests
async fn process_service_request(
    ctx: Arc<GatewayContext>,
    service_id_raw: String,
    subpath: String,
    query_params: HashMap<String, String>,
    req: Request,
) -> Response<Body> {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let headers = req.headers().clone();
    let origin = headers
        .get(ORIGIN)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());
    let host = headers.get(HOST).and_then(|h| h.to_str().ok());
    let req_id = headers
        .get("x-request-id")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| hex::encode(rand::random::<[u8; 16]>()));

    // 1. Host Validation
    if let Some(h) = host {
        if let Err(e) = validate_host_header(h, &ctx.config) {
            return error_to_response(&e, &req_id);
        }
    }

    // 2. Arbitrary Proxy Target Validation
    if let Err(e) = validate_proxy_target(uri.path(), &uri.to_string()) {
        return error_to_response(&e, &req_id);
    }

    // 2b. Header Size Limit Validation
    let total_header_bytes: usize = headers
        .iter()
        .map(|(k, v)| k.as_str().len() + v.as_bytes().len() + 4)
        .sum();
    if total_header_bytes > ctx.config.max_header_size_bytes {
        return error_to_response(
            &GatewayError::HeaderTooLarge {
                size: total_header_bytes,
                max: ctx.config.max_header_size_bytes,
            },
            &req_id,
        );
    }

    // 2c. URI Length Limit Validation
    let uri_len = uri.to_string().len();
    if uri_len > ctx.config.max_uri_length_bytes {
        return error_to_response(
            &GatewayError::UriTooLong {
                len: uri_len,
                max: ctx.config.max_uri_length_bytes,
            },
            &req_id,
        );
    }

    // 3. Preflight OPTIONS handling
    if method == Method::OPTIONS {
        let mut resp = Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap();
        inject_cors_headers(&mut resp, origin.as_deref(), &ctx.config);
        inject_security_headers(&mut resp);
        return resp;
    }

    // 4. Anti-CSRF Check for state-changing operations
    let gateway_method = GatewayMethod::from_str_method(method.as_str());
    let client_header = headers
        .get("x-fieldnet-client")
        .and_then(|h| h.to_str().ok());
    if let Err(e) = validate_csrf(
        &gateway_method,
        origin.as_deref(),
        client_header,
        &ctx.config,
    ) {
        return error_to_response(&e, &req_id);
    }

    // 4b. Session Authentication & Capability Authorization
    let token_opt = extract_bearer_token(&headers);
    let is_loopback = is_loopback_client(host, origin.as_deref());
    let auth_context: Option<AuthContext>;

    if let Some(token) = token_opt {
        match ctx.session_store.validate_token(token).await {
            Ok(session) => {
                let required_cap = if gateway_method.is_safe() {
                    "service.read"
                } else {
                    "service.invoke"
                };

                if !session.has_capability(required_cap) {
                    if let Some(ref tx) = ctx.event_tx {
                        let _ = tx.send(ServiceEvent::BrowserAuthorizationDenied {
                            session_id: Some(session.session_id.clone()),
                            path: subpath.clone(),
                            reason: format!(
                                "Session lacks required capability '{}' for HTTP {}",
                                required_cap, gateway_method
                            ),
                        });
                    }
                    let err = GatewayError::SessionForbidden {
                        reason: format!(
                            "Session lacks required capability '{}' for HTTP {}",
                            required_cap, gateway_method
                        ),
                    };
                    let mut resp = error_to_response(&err, &req_id);
                    inject_cors_headers(&mut resp, origin.as_deref(), &ctx.config);
                    inject_security_headers(&mut resp);
                    return resp;
                }

                auth_context = Some(session.into_auth_context());
            }
            Err(e) => {
                if let Some(ref tx) = ctx.event_tx {
                    let _ = tx.send(ServiceEvent::BrowserAuthorizationDenied {
                        session_id: None,
                        path: subpath.clone(),
                        reason: e.to_string(),
                    });
                }
                let mut resp = error_to_response(&e, &req_id);
                inject_cors_headers(&mut resp, origin.as_deref(), &ctx.config);
                inject_security_headers(&mut resp);
                return resp;
            }
        }
    } else if ctx.config.auto_pair_localhost && is_loopback {
        // Loopback client with auto_pair_localhost enabled: assign default loopback context
        auth_context = Some(
            AuthContext::anonymous()
                .with_token("service.read")
                .with_token("service.invoke")
                .with_token("content.read"),
        );
    } else {
        // Unpaired client or non-loopback client without token: reject!
        if let Some(ref tx) = ctx.event_tx {
            let _ = tx.send(ServiceEvent::BrowserAuthorizationDenied {
                session_id: None,
                path: subpath.clone(),
                reason: "Unauthenticated client: Authorization Bearer token required".to_string(),
            });
        }
        let err = GatewayError::SessionUnauthorized {
            reason: "Authentication required: missing Bearer token in Authorization header"
                .to_string(),
        };
        let mut resp = error_to_response(&err, &req_id);
        inject_cors_headers(&mut resp, origin.as_deref(), &ctx.config);
        inject_security_headers(&mut resp);
        return resp;
    }

    // 5. Parse ServiceId
    let service_id = match ServiceId::new(&service_id_raw) {
        Ok(id) => id,
        Err(e) => {
            return error_to_response(&GatewayError::Service(e), &req_id);
        }
    };

    // 6. Read Request Body with limits
    let body_bytes =
        match axum::body::to_bytes(req.into_body(), ctx.config.max_request_size_bytes).await {
            Ok(b) => b.to_vec(),
            Err(_) => {
                return error_to_response(
                    &GatewayError::PayloadTooLarge {
                        size: ctx.config.max_request_size_bytes + 1,
                        max: ctx.config.max_request_size_bytes,
                    },
                    &req_id,
                );
            }
        };

    // Convert safe headers, explicitly rejecting identity-spoofing headers
    let mut safe_headers = HashMap::new();
    for (name, val) in headers.iter() {
        if let Ok(val_str) = val.to_str() {
            let name_str = name.as_str().to_lowercase();
            let is_identity_header = matches!(
                name_str.as_str(),
                "x-node-id"
                    | "x-peer-id"
                    | "x-identity"
                    | "x-caller-id"
                    | "x-requester-id"
                    | "x-client-node-id"
            );

            if !is_identity_header
                && (name_str.starts_with("x-")
                    || name_str == "content-type"
                    || name_str == "accept"
                    || name_str == "idempotency-key")
            {
                safe_headers.insert(name_str, val_str.to_string());
            }
        }
    }

    let deadline = Timestamp::from_millis(
        Timestamp::now()
            .as_millis()
            .saturating_add(ctx.config.request_timeout_ms),
    );
    let gateway_req = GatewayRequest {
        request_id: req_id.clone(),
        method: gateway_method,
        uri: uri.to_string(),
        path: subpath.clone(),
        query_params,
        headers: safe_headers,
        body: body_bytes,
        deadline,
        origin: origin.clone(),
    };

    // 7. Dispatch with Timeout and In-Flight Accounting
    ctx.state_machine.inc_requests();
    let timeout_duration = Duration::from_millis(ctx.config.request_timeout_ms);

    let dispatch_future = dispatch_service_request(
        &ctx.resolver,
        &ctx.dispatcher,
        ctx.node_id,
        service_id,
        subpath,
        gateway_req,
        auth_context.as_ref(),
    );

    let result = tokio::time::timeout(timeout_duration, dispatch_future).await;
    ctx.state_machine.dec_requests();

    match result {
        Ok(Ok(gw_resp)) => {
            let status = StatusCode::from_u16(gw_resp.status_code).unwrap_or(StatusCode::OK);
            let mut builder = Response::builder().status(status);

            for (k, v) in gw_resp.headers {
                if let Ok(hdr_name) = HeaderName::from_bytes(k.as_bytes()) {
                    if let Ok(hdr_val) = HeaderValue::from_str(&v) {
                        builder = builder.header(hdr_name, hdr_val);
                    }
                }
            }

            let mut resp = builder
                .body(Body::from(gw_resp.body))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());

            inject_cors_headers(&mut resp, origin.as_deref(), &ctx.config);
            inject_security_headers(&mut resp);
            resp
        }
        Ok(Err(err)) => {
            let mut resp = error_to_response(&err, &req_id);
            inject_cors_headers(&mut resp, origin.as_deref(), &ctx.config);
            inject_security_headers(&mut resp);
            resp
        }
        Err(_) => {
            let mut resp = error_to_response(
                &GatewayError::Timeout {
                    timeout_ms: ctx.config.request_timeout_ms,
                },
                &req_id,
            );
            inject_cors_headers(&mut resp, origin.as_deref(), &ctx.config);
            inject_security_headers(&mut resp);
            resp
        }
    }
}

/// Fallback route: inspects if the path is `/{service_id}` directly, else returns 404.
async fn handle_fallback(
    State(ctx): State<Arc<GatewayContext>>,
    Query(query_params): Query<HashMap<String, String>>,
    req: Request,
) -> Response<Body> {
    let path = req.uri().path().to_string();

    // Check if path starts with `/<service_name>`
    let segments: Vec<&str> = path.trim_start_matches('/').splitn(2, '/').collect();
    if !segments.is_empty() && !segments[0].is_empty() {
        let candidate_service = segments[0];
        if let Ok(service_id) = ServiceId::new(candidate_service) {
            // Check if this service is registered in ServiceRegistry
            if !ctx
                .registry
                .get_service_instances(&service_id)
                .await
                .is_empty()
            {
                let subpath = if segments.len() > 1 {
                    format!("/{}", segments[1])
                } else {
                    "/".to_string()
                };
                return process_service_request(
                    ctx,
                    candidate_service.to_string(),
                    subpath,
                    query_params,
                    req,
                )
                .await;
            }
        }
    }

    let req_id = hex::encode(rand::random::<[u8; 16]>());
    error_to_response(
        &GatewayError::Service(crate::error::ServiceError::ServiceNotFound(path)),
        &req_id,
    )
}
