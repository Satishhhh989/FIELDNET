use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::header::{HeaderMap, CONTENT_TYPE, HOST, ORIGIN};
use axum::http::StatusCode;
use axum::response::Response;
use serde::{Deserialize, Serialize};

use crate::events::ServiceEvent;

use super::auth::{
    extract_bearer_token, inject_security_headers, validate_csrf, validate_host_header,
};
use super::error::GatewayError;
use super::model::GatewayMethod;
use super::router::{error_to_response, inject_cors_headers, GatewayContext};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PairStartRequest {
    pub client_info: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PairStartResponse {
    pub challenge_id: String,
    pub expires_in_seconds: u64,
    pub status: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PairStatusQuery {
    pub challenge_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PairStatusResponse {
    pub challenge_id: String,
    pub status: String,
    pub remaining_seconds: u64,
    pub attempts: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PairConfirmRequest {
    pub challenge_id: String,
    pub pin: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PairConfirmResponse {
    pub session_id: String,
    pub token: String,
    pub token_type: String,
    pub expires_in_seconds: u64,
    pub capabilities: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionRefreshResponse {
    pub session_id: String,
    pub expires_in_seconds: u64,
    pub capabilities: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionRevokeResponse {
    pub status: String,
    pub session_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingChallengeItem {
    pub challenge_id: String,
    pub pin: String,
    pub remaining_seconds: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingChallengesResponse {
    pub pending: Vec<PendingChallengeItem>,
}

/// POST /api/v1/pair/start — Initiates local browser pairing ceremony
pub async fn handle_pair_start(
    State(ctx): State<Arc<GatewayContext>>,
    headers: HeaderMap,
    body_bytes: axum::body::Bytes,
) -> Response<Body> {
    let origin = headers.get(ORIGIN).and_then(|h| h.to_str().ok());
    if let Some(host) = headers.get(HOST).and_then(|h| h.to_str().ok()) {
        if let Err(e) = validate_host_header(host, &ctx.config) {
            return error_to_response(&e, "pair_start");
        }
    }

    // CSRF protection for POST
    let client_header = headers
        .get("x-fieldnet-client")
        .and_then(|h| h.to_str().ok());
    if let Err(e) = validate_csrf(&GatewayMethod::Post, origin, client_header, &ctx.config) {
        return error_to_response(&e, "pair_start");
    }

    // Rate limiting: allow max 10 pairing starts per minute per origin or host
    let rate_key = origin.unwrap_or("unknown_client");
    if !ctx.session_store.check_rate_limit(rate_key, 10).await {
        let err = GatewayError::PairingRateLimited;
        let mut resp = error_to_response(&err, "pair_start");
        inject_cors_headers(&mut resp, origin, &ctx.config);
        inject_security_headers(&mut resp);
        return resp;
    }

    let req_payload: PairStartRequest = if body_bytes.is_empty() {
        PairStartRequest { client_info: None }
    } else {
        match serde_json::from_slice(&body_bytes) {
            Ok(p) => p,
            Err(e) => {
                let err = GatewayError::Service(crate::error::ServiceError::InvalidQuery(format!(
                    "Invalid JSON body: {}",
                    e
                )));
                let mut resp = error_to_response(&err, "pair_start");
                inject_cors_headers(&mut resp, origin, &ctx.config);
                inject_security_headers(&mut resp);
                return resp;
            }
        }
    };

    let client_info = req_payload
        .client_info
        .unwrap_or_else(|| "Unknown Browser".to_string());

    let (challenge, _pin) = match ctx.session_store.start_pairing(client_info.clone()).await {
        Ok(res) => res,
        Err(e) => {
            let mut resp = error_to_response(&e, "pair_start");
            inject_cors_headers(&mut resp, origin, &ctx.config);
            inject_security_headers(&mut resp);
            return resp;
        }
    };

    // Emit event on Event Bus
    if let Some(ref tx) = ctx.event_tx {
        let _ = tx.send(ServiceEvent::BrowserPairingStarted {
            challenge_id: challenge.challenge_id.clone(),
            client_info,
        });
    }

    let resp_payload = PairStartResponse {
        expires_in_seconds: challenge.remaining_seconds(),
        challenge_id: challenge.challenge_id,
        status: challenge.status.to_string(),
    };

    let body_json = serde_json::to_vec(&resp_payload).unwrap_or_default();
    let mut resp = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/json")
        .header("Cache-Control", "no-store, no-cache, must-revalidate")
        .body(Body::from(body_json))
        .unwrap();

    inject_cors_headers(&mut resp, origin, &ctx.config);
    inject_security_headers(&mut resp);
    resp
}

/// GET /api/v1/pair/status — Query status of active pairing challenge
pub async fn handle_pair_status(
    State(ctx): State<Arc<GatewayContext>>,
    Query(query): Query<PairStatusQuery>,
    headers: HeaderMap,
) -> Response<Body> {
    let origin = headers.get(ORIGIN).and_then(|h| h.to_str().ok());
    if let Some(host) = headers.get(HOST).and_then(|h| h.to_str().ok()) {
        if let Err(e) = validate_host_header(host, &ctx.config) {
            return error_to_response(&e, "pair_status");
        }
    }

    match ctx
        .session_store
        .get_challenge_status(&query.challenge_id)
        .await
    {
        Some((status, remaining, attempts)) => {
            let resp_payload = PairStatusResponse {
                challenge_id: query.challenge_id,
                status: status.to_string(),
                remaining_seconds: remaining,
                attempts,
            };
            let mut resp = Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/json")
                .header("Cache-Control", "no-store, no-cache, must-revalidate")
                .body(Body::from(serde_json::to_vec(&resp_payload).unwrap()))
                .unwrap();
            inject_cors_headers(&mut resp, origin, &ctx.config);
            inject_security_headers(&mut resp);
            resp
        }
        None => {
            let err = GatewayError::PairingRejected {
                reason: "Challenge not found".to_string(),
            };
            let mut resp = error_to_response(&err, "pair_status");
            inject_cors_headers(&mut resp, origin, &ctx.config);
            inject_security_headers(&mut resp);
            resp
        }
    }
}

/// POST /api/v1/pair/confirm — Verifies human pairing PIN and issues bearer session token
pub async fn handle_pair_confirm(
    State(ctx): State<Arc<GatewayContext>>,
    headers: HeaderMap,
    body_bytes: axum::body::Bytes,
) -> Response<Body> {
    let origin = headers.get(ORIGIN).and_then(|h| h.to_str().ok());
    if let Some(host) = headers.get(HOST).and_then(|h| h.to_str().ok()) {
        if let Err(e) = validate_host_header(host, &ctx.config) {
            return error_to_response(&e, "pair_confirm");
        }
    }

    // CSRF protection for POST
    let client_header = headers
        .get("x-fieldnet-client")
        .and_then(|h| h.to_str().ok());
    if let Err(e) = validate_csrf(&GatewayMethod::Post, origin, client_header, &ctx.config) {
        return error_to_response(&e, "pair_confirm");
    }

    let req_payload: PairConfirmRequest = match serde_json::from_slice(&body_bytes) {
        Ok(p) => p,
        Err(e) => {
            let err = GatewayError::Service(crate::error::ServiceError::InvalidQuery(format!(
                "Invalid JSON body: {}",
                e
            )));
            let mut resp = error_to_response(&err, "pair_confirm");
            inject_cors_headers(&mut resp, origin, &ctx.config);
            inject_security_headers(&mut resp);
            return resp;
        }
    };

    match ctx
        .session_store
        .confirm_pairing(&req_payload.challenge_id, &req_payload.pin)
        .await
    {
        Ok((session, raw_token)) => {
            // Emit verified events
            if let Some(ref tx) = ctx.event_tx {
                let _ = tx.send(ServiceEvent::BrowserPairingVerified {
                    challenge_id: req_payload.challenge_id.clone(),
                    session_id: session.session_id.clone(),
                });
                let _ = tx.send(ServiceEvent::BrowserSessionCreated {
                    session_id: session.session_id.clone(),
                });
            }

            let resp_payload = PairConfirmResponse {
                expires_in_seconds: session.remaining_seconds(),
                session_id: session.session_id,
                token: raw_token,
                token_type: "Bearer".to_string(),
                capabilities: session
                    .capabilities
                    .iter()
                    .map(|c| c.as_str().to_string())
                    .collect(),
            };

            let mut resp = Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/json")
                .header("Cache-Control", "no-store, no-cache, must-revalidate")
                .body(Body::from(serde_json::to_vec(&resp_payload).unwrap()))
                .unwrap();
            inject_cors_headers(&mut resp, origin, &ctx.config);
            inject_security_headers(&mut resp);
            resp
        }
        Err(e) => {
            if let Some(ref tx) = ctx.event_tx {
                let _ = tx.send(ServiceEvent::BrowserPairingRejected {
                    challenge_id: req_payload.challenge_id.clone(),
                    reason: e.to_string(),
                });
            }
            let mut resp = error_to_response(&e, "pair_confirm");
            inject_cors_headers(&mut resp, origin, &ctx.config);
            inject_security_headers(&mut resp);
            resp
        }
    }
}

/// POST /api/v1/session/refresh — Extends active session expiration
pub async fn handle_session_refresh(
    State(ctx): State<Arc<GatewayContext>>,
    headers: HeaderMap,
) -> Response<Body> {
    let origin = headers.get(ORIGIN).and_then(|h| h.to_str().ok());
    if let Some(host) = headers.get(HOST).and_then(|h| h.to_str().ok()) {
        if let Err(e) = validate_host_header(host, &ctx.config) {
            return error_to_response(&e, "session_refresh");
        }
    }

    let token = match extract_bearer_token(&headers) {
        Some(t) => t,
        None => {
            let err = GatewayError::SessionUnauthorized {
                reason: "Missing Bearer token in Authorization header".to_string(),
            };
            let mut resp = error_to_response(&err, "session_refresh");
            inject_cors_headers(&mut resp, origin, &ctx.config);
            inject_security_headers(&mut resp);
            return resp;
        }
    };

    match ctx.session_store.refresh_session(token).await {
        Ok(session) => {
            let resp_payload = SessionRefreshResponse {
                expires_in_seconds: session.remaining_seconds(),
                session_id: session.session_id,
                capabilities: session
                    .capabilities
                    .iter()
                    .map(|c| c.as_str().to_string())
                    .collect(),
            };

            let mut resp = Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/json")
                .header("Cache-Control", "no-store, no-cache, must-revalidate")
                .body(Body::from(serde_json::to_vec(&resp_payload).unwrap()))
                .unwrap();
            inject_cors_headers(&mut resp, origin, &ctx.config);
            inject_security_headers(&mut resp);
            resp
        }
        Err(e) => {
            let mut resp = error_to_response(&e, "session_refresh");
            inject_cors_headers(&mut resp, origin, &ctx.config);
            inject_security_headers(&mut resp);
            resp
        }
    }
}

/// POST /api/v1/session/revoke — Explicitly revokes active browser session
pub async fn handle_session_revoke(
    State(ctx): State<Arc<GatewayContext>>,
    headers: HeaderMap,
) -> Response<Body> {
    let origin = headers.get(ORIGIN).and_then(|h| h.to_str().ok());
    if let Some(host) = headers.get(HOST).and_then(|h| h.to_str().ok()) {
        if let Err(e) = validate_host_header(host, &ctx.config) {
            return error_to_response(&e, "session_revoke");
        }
    }

    let token = match extract_bearer_token(&headers) {
        Some(t) => t,
        None => {
            let err = GatewayError::SessionUnauthorized {
                reason: "Missing Bearer token in Authorization header".to_string(),
            };
            let mut resp = error_to_response(&err, "session_revoke");
            inject_cors_headers(&mut resp, origin, &ctx.config);
            inject_security_headers(&mut resp);
            return resp;
        }
    };

    match ctx.session_store.revoke_session(token).await {
        Ok(session_id) => {
            if let Some(ref tx) = ctx.event_tx {
                let _ = tx.send(ServiceEvent::BrowserSessionRevoked {
                    session_id: session_id.clone(),
                });
            }

            let resp_payload = SessionRevokeResponse {
                status: "revoked".to_string(),
                session_id,
            };

            let mut resp = Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/json")
                .header("Cache-Control", "no-store, no-cache, must-revalidate")
                .body(Body::from(serde_json::to_vec(&resp_payload).unwrap()))
                .unwrap();
            inject_cors_headers(&mut resp, origin, &ctx.config);
            inject_security_headers(&mut resp);
            resp
        }
        Err(e) => {
            let mut resp = error_to_response(&e, "session_revoke");
            inject_cors_headers(&mut resp, origin, &ctx.config);
            inject_security_headers(&mut resp);
            resp
        }
    }
}
