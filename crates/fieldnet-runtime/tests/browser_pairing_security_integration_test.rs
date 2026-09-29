use std::collections::{HashMap, HashSet};
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use fieldnet_core::{NodeId, Timestamp};
use fieldnet_identity::NodeIdentity;
use fieldnet_runtime::service::{
    GatewayConfig, ServiceCapability, ServiceEndpoint, ServiceError, ServiceId, ServiceInstanceId,
    ServiceManifestBuilder, ServiceProtocol, ServiceRequestHandler, ServiceRpcRequestWire,
    ServiceRpcResponseWire, ServiceState, ServiceVersion,
};
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ============================================================================
// Raw Async HTTP Client Helpers
// ============================================================================

#[derive(Clone, Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }

    pub fn json(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::from_slice(&self.body)
    }
}

pub async fn http_get(
    addr: SocketAddr,
    path: &str,
    headers: &[(&str, &str)],
) -> Result<HttpResponse, Box<dyn std::error::Error + Send + Sync>> {
    http_request(addr, "GET", path, headers, &[]).await
}

pub async fn http_post(
    addr: SocketAddr,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Result<HttpResponse, Box<dyn std::error::Error + Send + Sync>> {
    http_request(addr, "POST", path, headers, body).await
}

pub async fn http_request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Result<HttpResponse, Box<dyn std::error::Error + Send + Sync>> {
    let mut stream = TcpStream::connect(addr).await?;
    let mut req = format!(
        "{} {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n",
        method, path
    );
    for (k, v) in headers {
        req.push_str(&format!("{}: {}\r\n", k, v));
    }
    if !body.is_empty() {
        req.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    req.push_str("\r\n");

    stream.write_all(req.as_bytes()).await?;
    if !body.is_empty() {
        stream.write_all(body).await?;
    }

    let mut response_bytes = Vec::new();
    stream.read_to_end(&mut response_bytes).await?;
    parse_http_response(&response_bytes)
}

fn parse_http_response(
    raw: &[u8],
) -> Result<HttpResponse, Box<dyn std::error::Error + Send + Sync>> {
    let s = String::from_utf8_lossy(raw);
    let mut parts = s.splitn(2, "\r\n\r\n");
    let header_part = parts.next().ok_or("Empty HTTP response")?;
    let body_part = parts.next().unwrap_or("");

    let mut lines = header_part.lines();
    let status_line = lines.next().ok_or("Missing status line")?;
    let status_code: u16 = status_line
        .split_whitespace()
        .nth(1)
        .ok_or("No status code")?
        .parse()?;

    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }

    Ok(HttpResponse {
        status: status_code,
        headers,
        body: body_part.as_bytes().to_vec(),
    })
}

// ============================================================================
// Mock Service Provider Handlers
// ============================================================================

struct EchoServiceHandler {
    node_id: NodeId,
}

#[async_trait::async_trait]
impl ServiceRequestHandler for EchoServiceHandler {
    async fn handle_request(
        &self,
        req: ServiceRpcRequestWire,
    ) -> Result<ServiceRpcResponseWire, ServiceError> {
        let mut resp_headers = HashMap::new();
        resp_headers.insert("content-type".to_string(), "application/json".to_string());
        let body_json = serde_json::json!({
            "service": req.service_id.to_string(),
            "method": req.method,
            "path": req.path,
            "responder": self.node_id.to_hex(),
            "payload_len": req.body.len(),
        });

        Ok(ServiceRpcResponseWire {
            request_id: req.request_id,
            status_code: 200,
            headers: resp_headers,
            body: serde_json::to_vec(&body_json).unwrap(),
            responder_node_id: self.node_id,
            timestamp: Timestamp::now(),
        })
    }
}

// ============================================================================
// Test 1: Full Pairing Ceremony and Authenticated Service Invocation (Sections 23, 25)
// ============================================================================

#[tokio::test]
async fn test_browser_pairing_ceremony_and_authorized_service_invocation() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_pairing_test_1");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    let id = NodeIdentity::generate();
    id.save_to_file(&base_dir.join("identity.json")).unwrap();

    let config = RuntimeConfig {
        data_dir: base_dir.clone(),
        tcp_port: 0,
        discovery_port: 4860,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config).await.unwrap();

    // Configure gateway with auto_pair_localhost: false to mandate pairing on all connections
    let mut gw_config = GatewayConfig::ephemeral();
    gw_config.auto_pair_localhost = false;

    let gw_addr = runtime.start_gateway(Some(gw_config)).await.unwrap();
    let gw = runtime.gateway().await.unwrap();

    // Register a local service
    let s_id = ServiceId::new("weather.station").unwrap();
    let builder = ServiceManifestBuilder::new(s_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(ServiceInstanceId::generate())
        .display_name("Weather Station")
        .endpoint(ServiceEndpoint::InternalRpc)
        .protocol(ServiceProtocol::NativeFieldnetRpc)
        .capability(ServiceCapability::new("service.read").unwrap())
        .capability(ServiceCapability::new("service.invoke").unwrap())
        .ttl_seconds(120);

    runtime.advertise_service(builder, None).await.unwrap();
    runtime
        .register_service_handler(
            s_id.clone(),
            Arc::new(EchoServiceHandler {
                node_id: runtime.node_id(),
            }),
        )
        .await;

    // 1. Unpaired client attempts to invoke protected service -> 401 Unauthorized
    let unauth_resp = http_get(gw_addr, "/services/weather.station/current", &[])
        .await
        .unwrap();
    assert_eq!(unauth_resp.status, 401);
    let unauth_json = unauth_resp.json().unwrap();
    assert_eq!(unauth_json["error"], "SESSION_UNAUTHORIZED");

    // 2. Browser initiates pairing: POST /api/v1/pair/start
    let start_body = serde_json::json!({
        "client_info": "Firefox Browser 128 (macOS)"
    });
    let start_resp = http_post(
        gw_addr,
        "/api/v1/pair/start",
        &[
            ("Content-Type", "application/json"),
            ("X-Fieldnet-Client", "browser-pairing"),
        ],
        &serde_json::to_vec(&start_body).unwrap(),
    )
    .await
    .unwrap();

    assert_eq!(start_resp.status, 200);
    assert_eq!(
        start_resp
            .headers
            .get("x-content-type-options")
            .map(|s| s.as_str()),
        Some("nosniff")
    );
    assert_eq!(
        start_resp.headers.get("cache-control").map(|s| s.as_str()),
        Some("no-store, no-cache, must-revalidate")
    );
    let start_json = start_resp.json().unwrap();
    let challenge_id = start_json["challenge_id"].as_str().unwrap().to_string();
    assert_eq!(start_json["status"], "pending");
    assert!(start_json["expires_in_seconds"].as_u64().unwrap() > 0);

    // 3. Inspect pairing status via GET /api/v1/pair/status
    let status_path = format!("/api/v1/pair/status?challenge_id={}", challenge_id);
    let status_resp = http_get(gw_addr, &status_path, &[]).await.unwrap();
    assert_eq!(status_resp.status, 200);
    let status_json = status_resp.json().unwrap();
    assert_eq!(status_json["status"], "pending");
    assert_eq!(status_json["attempts"], 0);

    // 4. Node host ceremony: operator views PIN displayed on local node
    let pin = gw
        .session_store()
        .get_pending_challenge_pin(&challenge_id)
        .await
        .expect("Pending challenge PIN must exist for host ceremony");
    assert_eq!(pin.len(), 6);

    // 5. Browser submits verification: POST /api/v1/pair/confirm
    let confirm_body = serde_json::json!({
        "challenge_id": challenge_id,
        "pin": pin
    });
    let confirm_resp = http_post(
        gw_addr,
        "/api/v1/pair/confirm",
        &[
            ("Content-Type", "application/json"),
            ("X-Fieldnet-Client", "browser-pairing"),
        ],
        &serde_json::to_vec(&confirm_body).unwrap(),
    )
    .await
    .unwrap();

    assert_eq!(confirm_resp.status, 200);
    let confirm_json = confirm_resp.json().unwrap();
    let token = confirm_json["token"].as_str().unwrap().to_string();
    let session_id = confirm_json["session_id"].as_str().unwrap().to_string();
    assert_eq!(confirm_json["token_type"], "Bearer");
    assert!(!token.is_empty());
    assert!(!session_id.is_empty());

    // 6. Authorized request succeeds: GET /services/weather.station/current
    let auth_header = format!("Bearer {}", token);
    let service_resp = http_get(
        gw_addr,
        "/services/weather.station/current",
        &[("Authorization", &auth_header)],
    )
    .await
    .unwrap();

    assert_eq!(service_resp.status, 200);
    assert_eq!(
        service_resp
            .headers
            .get("x-content-type-options")
            .map(|s| s.as_str()),
        Some("nosniff")
    );
    assert_eq!(
        service_resp
            .headers
            .get("referrer-policy")
            .map(|s| s.as_str()),
        Some("strict-origin-when-cross-origin")
    );
    assert_eq!(
        service_resp
            .headers
            .get("content-security-policy")
            .map(|s| s.as_str()),
        Some("default-src 'self'")
    );
    let service_json = service_resp.json().unwrap();
    assert_eq!(service_json["service"], "weather.station");

    runtime.stop_gateway().await.unwrap();
    runtime.shutdown();
}

// ============================================================================
// Test 2: Unpaired Client Rejection (Section 24)
// ============================================================================

#[tokio::test]
async fn test_unpaired_client_rejection_and_fake_token() {
    let base_dir = std::env::temp_dir().join("fieldnet_pairing_test_unpaired");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    let id = NodeIdentity::generate();
    id.save_to_file(&base_dir.join("identity.json")).unwrap();

    let config = RuntimeConfig {
        data_dir: base_dir.clone(),
        tcp_port: 0,
        discovery_port: 4861,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config).await.unwrap();

    let mut gw_config = GatewayConfig::ephemeral();
    gw_config.auto_pair_localhost = false;
    let gw_addr = runtime.start_gateway(Some(gw_config)).await.unwrap();

    // 1. Missing Authorization header -> 401
    let resp_no_auth = http_get(gw_addr, "/services/sensor.telemetry", &[])
        .await
        .unwrap();
    assert_eq!(resp_no_auth.status, 401);
    assert_eq!(
        resp_no_auth.json().unwrap()["error"],
        "SESSION_UNAUTHORIZED"
    );

    // 2. Forged / invalid Bearer token -> 401
    let resp_bad_token = http_get(
        gw_addr,
        "/services/sensor.telemetry",
        &[("Authorization", "Bearer 0123456789abcdef0123456789abcdef")],
    )
    .await
    .unwrap();
    assert_eq!(resp_bad_token.status, 401);
    assert_eq!(
        resp_bad_token.json().unwrap()["error"],
        "SESSION_UNAUTHORIZED"
    );

    runtime.stop_gateway().await.unwrap();
    runtime.shutdown();
}

// ============================================================================
// Test 3: Incorrect Verification & Burning Challenges (Section 26)
// ============================================================================

#[tokio::test]
async fn test_wrong_pairing_pin_and_burn_after_max_attempts() {
    let base_dir = std::env::temp_dir().join("fieldnet_pairing_test_wrong_pin");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    let id = NodeIdentity::generate();
    id.save_to_file(&base_dir.join("identity.json")).unwrap();

    let config = RuntimeConfig {
        data_dir: base_dir.clone(),
        tcp_port: 0,
        discovery_port: 4862,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config).await.unwrap();
    let mut gw_config = GatewayConfig::ephemeral();
    gw_config.auto_pair_localhost = false;
    let gw_addr = runtime.start_gateway(Some(gw_config)).await.unwrap();
    let gw = runtime.gateway().await.unwrap();

    // Start pairing
    let start_resp = http_post(
        gw_addr,
        "/api/v1/pair/start",
        &[("X-Fieldnet-Client", "test")],
        b"{}",
    )
    .await
    .unwrap();
    let challenge_id = start_resp.json().unwrap()["challenge_id"]
        .as_str()
        .unwrap()
        .to_string();
    let real_pin = gw
        .session_store()
        .get_pending_challenge_pin(&challenge_id)
        .await
        .unwrap();

    // Send 5 incorrect attempts
    for attempt in 1..=5 {
        let confirm_body = serde_json::json!({
            "challenge_id": challenge_id,
            "pin": format!("00000{}", attempt)
        });
        let resp = http_post(
            gw_addr,
            "/api/v1/pair/confirm",
            &[("X-Fieldnet-Client", "test")],
            &serde_json::to_vec(&confirm_body).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(resp.status, 401);
        let err_json = resp.json().unwrap();
        assert_eq!(err_json["error"], "PAIRING_REJECTED");
    }

    // Attempt 6: even with the genuine PIN, the challenge has been burned
    let confirm_body = serde_json::json!({
        "challenge_id": challenge_id,
        "pin": real_pin
    });
    let burned_resp = http_post(
        gw_addr,
        "/api/v1/pair/confirm",
        &[("X-Fieldnet-Client", "test")],
        &serde_json::to_vec(&confirm_body).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(burned_resp.status, 401);

    // Nonexistent challenge ID
    let nonexistent_body = serde_json::json!({
        "challenge_id": "nonexistent_challenge_id_12345",
        "pin": "123456"
    });
    let non_resp = http_post(
        gw_addr,
        "/api/v1/pair/confirm",
        &[("X-Fieldnet-Client", "test")],
        &serde_json::to_vec(&nonexistent_body).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(non_resp.status, 401);

    runtime.stop_gateway().await.unwrap();
    runtime.shutdown();
}

// ============================================================================
// Test 4: Replay Protection (Sections 16, 29)
// ============================================================================

#[tokio::test]
async fn test_pairing_replay_protection() {
    let base_dir = std::env::temp_dir().join("fieldnet_pairing_test_replay");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    let id = NodeIdentity::generate();
    id.save_to_file(&base_dir.join("identity.json")).unwrap();

    let config = RuntimeConfig {
        data_dir: base_dir.clone(),
        tcp_port: 0,
        discovery_port: 4863,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config).await.unwrap();
    let mut gw_config = GatewayConfig::ephemeral();
    gw_config.auto_pair_localhost = false;
    let gw_addr = runtime.start_gateway(Some(gw_config)).await.unwrap();
    let gw = runtime.gateway().await.unwrap();

    // 1. Start challenge
    let start_resp = http_post(
        gw_addr,
        "/api/v1/pair/start",
        &[("X-Fieldnet-Client", "test")],
        b"{}",
    )
    .await
    .unwrap();
    let challenge_id = start_resp.json().unwrap()["challenge_id"]
        .as_str()
        .unwrap()
        .to_string();
    let pin = gw
        .session_store()
        .get_pending_challenge_pin(&challenge_id)
        .await
        .unwrap();

    let confirm_payload = serde_json::to_vec(&serde_json::json!({
        "challenge_id": challenge_id,
        "pin": pin
    }))
    .unwrap();

    // 2. First confirmation -> succeeds
    let first_resp = http_post(
        gw_addr,
        "/api/v1/pair/confirm",
        &[("X-Fieldnet-Client", "test")],
        &confirm_payload,
    )
    .await
    .unwrap();
    assert_eq!(first_resp.status, 200);

    // 3. Replay exact same confirmation -> must fail (single-use challenge consumed)
    let replayed_resp = http_post(
        gw_addr,
        "/api/v1/pair/confirm",
        &[("X-Fieldnet-Client", "test")],
        &confirm_payload,
    )
    .await
    .unwrap();
    assert_eq!(replayed_resp.status, 401);
    assert_eq!(replayed_resp.json().unwrap()["error"], "PAIRING_REJECTED");

    runtime.stop_gateway().await.unwrap();
    runtime.shutdown();
}

// ============================================================================
// Test 5: Capability-Based Authorization (Sections 11, 12, 27)
// ============================================================================

#[tokio::test]
async fn test_capability_based_authorization_boundary() {
    let base_dir = std::env::temp_dir().join("fieldnet_pairing_test_caps");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    let id = NodeIdentity::generate();
    id.save_to_file(&base_dir.join("identity.json")).unwrap();

    let config = RuntimeConfig {
        data_dir: base_dir.clone(),
        tcp_port: 0,
        discovery_port: 4864,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config).await.unwrap();
    let mut gw_config = GatewayConfig::ephemeral();
    gw_config.auto_pair_localhost = false;
    let gw_addr = runtime.start_gateway(Some(gw_config)).await.unwrap();
    let gw = runtime.gateway().await.unwrap();

    // Register test service
    let s_id = ServiceId::new("device.config").unwrap();
    let builder = ServiceManifestBuilder::new(s_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(ServiceInstanceId::generate())
        .display_name("Device Config")
        .endpoint(ServiceEndpoint::InternalRpc)
        .protocol(ServiceProtocol::NativeFieldnetRpc)
        .capability(ServiceCapability::new("service.read").unwrap())
        .capability(ServiceCapability::new("service.invoke").unwrap())
        .ttl_seconds(120);

    runtime.advertise_service(builder, None).await.unwrap();
    runtime
        .register_service_handler(
            s_id.clone(),
            Arc::new(EchoServiceHandler {
                node_id: runtime.node_id(),
            }),
        )
        .await;

    // Perform pairing
    let start_resp = http_post(
        gw_addr,
        "/api/v1/pair/start",
        &[("X-Fieldnet-Client", "test")],
        b"{}",
    )
    .await
    .unwrap();
    let challenge_id = start_resp.json().unwrap()["challenge_id"]
        .as_str()
        .unwrap()
        .to_string();
    let pin = gw
        .session_store()
        .get_pending_challenge_pin(&challenge_id)
        .await
        .unwrap();

    let confirm_resp = http_post(
        gw_addr,
        "/api/v1/pair/confirm",
        &[("X-Fieldnet-Client", "test")],
        &serde_json::to_vec(&serde_json::json!({
            "challenge_id": challenge_id,
            "pin": pin
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    let token = confirm_resp.json().unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();

    // Restrict this session to ONLY "service.read" (strip "service.invoke")
    let mut restricted_caps = HashSet::new();
    restricted_caps.insert(ServiceCapability::new("service.read").unwrap());
    gw.session_store()
        .set_session_capabilities(&token, restricted_caps)
        .await
        .unwrap();

    let auth_header = format!("Bearer {}", token);

    // 1. Safe GET request (requires service.read) -> SUCCEEDS
    let get_resp = http_get(
        gw_addr,
        "/services/device.config/status",
        &[("Authorization", &auth_header)],
    )
    .await
    .unwrap();
    assert_eq!(get_resp.status, 200);

    // 2. Mutation POST request (requires service.invoke) -> FORBIDDEN (403)
    let post_resp = http_post(
        gw_addr,
        "/services/device.config/update",
        &[
            ("Authorization", &auth_header),
            ("X-Fieldnet-Client", "test"),
        ],
        b"{\"setting\": true}",
    )
    .await
    .unwrap();
    assert_eq!(post_resp.status, 403);
    assert_eq!(post_resp.json().unwrap()["error"], "SESSION_FORBIDDEN");

    runtime.stop_gateway().await.unwrap();
    runtime.shutdown();
}

// ============================================================================
// Test 6: Session Revocation (Sections 15, 28)
// ============================================================================

#[tokio::test]
async fn test_session_revocation_takes_immediate_effect() {
    let base_dir = std::env::temp_dir().join("fieldnet_pairing_test_revoke");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    let id = NodeIdentity::generate();
    id.save_to_file(&base_dir.join("identity.json")).unwrap();

    let config = RuntimeConfig {
        data_dir: base_dir.clone(),
        tcp_port: 0,
        discovery_port: 4865,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config).await.unwrap();
    let mut gw_config = GatewayConfig::ephemeral();
    gw_config.auto_pair_localhost = false;
    let gw_addr = runtime.start_gateway(Some(gw_config)).await.unwrap();
    let gw = runtime.gateway().await.unwrap();

    // Register test service
    let s_id = ServiceId::new("metrics.service").unwrap();
    let builder = ServiceManifestBuilder::new(s_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(ServiceInstanceId::generate())
        .display_name("Metrics")
        .endpoint(ServiceEndpoint::InternalRpc)
        .protocol(ServiceProtocol::NativeFieldnetRpc)
        .capability(ServiceCapability::new("service.read").unwrap())
        .ttl_seconds(120);

    runtime.advertise_service(builder, None).await.unwrap();
    runtime
        .register_service_handler(
            s_id.clone(),
            Arc::new(EchoServiceHandler {
                node_id: runtime.node_id(),
            }),
        )
        .await;

    // Pair browser
    let start_resp = http_post(
        gw_addr,
        "/api/v1/pair/start",
        &[("X-Fieldnet-Client", "test")],
        b"{}",
    )
    .await
    .unwrap();
    let challenge_id = start_resp.json().unwrap()["challenge_id"]
        .as_str()
        .unwrap()
        .to_string();
    let pin = gw
        .session_store()
        .get_pending_challenge_pin(&challenge_id)
        .await
        .unwrap();

    let confirm_resp = http_post(
        gw_addr,
        "/api/v1/pair/confirm",
        &[("X-Fieldnet-Client", "test")],
        &serde_json::to_vec(&serde_json::json!({
            "challenge_id": challenge_id,
            "pin": pin
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    let token = confirm_resp.json().unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let auth_header = format!("Bearer {}", token);

    // Initial service call succeeds
    let initial_resp = http_get(
        gw_addr,
        "/services/metrics.service/all",
        &[("Authorization", &auth_header)],
    )
    .await
    .unwrap();
    assert_eq!(initial_resp.status, 200);

    // Revoke session via POST /api/v1/session/revoke
    let revoke_resp = http_post(
        gw_addr,
        "/api/v1/session/revoke",
        &[
            ("Authorization", &auth_header),
            ("X-Fieldnet-Client", "test"),
        ],
        b"{}",
    )
    .await
    .unwrap();
    assert_eq!(revoke_resp.status, 200);
    assert_eq!(revoke_resp.json().unwrap()["status"], "revoked");

    // Same request immediately fails with 401 Unauthorized
    let post_revoke_resp = http_get(
        gw_addr,
        "/services/metrics.service/all",
        &[("Authorization", &auth_header)],
    )
    .await
    .unwrap();
    assert_eq!(post_revoke_resp.status, 401);
    assert_eq!(
        post_revoke_resp.json().unwrap()["error"],
        "SESSION_UNAUTHORIZED"
    );

    runtime.stop_gateway().await.unwrap();
    runtime.shutdown();
}

// ============================================================================
// Test 7: Session Refresh (Sections 6, 8)
// ============================================================================

#[tokio::test]
async fn test_session_refresh_extends_lifetime() {
    let base_dir = std::env::temp_dir().join("fieldnet_pairing_test_refresh");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    let id = NodeIdentity::generate();
    id.save_to_file(&base_dir.join("identity.json")).unwrap();

    let config = RuntimeConfig {
        data_dir: base_dir.clone(),
        tcp_port: 0,
        discovery_port: 4866,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config).await.unwrap();
    let mut gw_config = GatewayConfig::ephemeral();
    gw_config.auto_pair_localhost = false;
    let gw_addr = runtime.start_gateway(Some(gw_config)).await.unwrap();
    let gw = runtime.gateway().await.unwrap();

    let start_resp = http_post(
        gw_addr,
        "/api/v1/pair/start",
        &[("X-Fieldnet-Client", "test")],
        b"{}",
    )
    .await
    .unwrap();
    let challenge_id = start_resp.json().unwrap()["challenge_id"]
        .as_str()
        .unwrap()
        .to_string();
    let pin = gw
        .session_store()
        .get_pending_challenge_pin(&challenge_id)
        .await
        .unwrap();

    let confirm_resp = http_post(
        gw_addr,
        "/api/v1/pair/confirm",
        &[("X-Fieldnet-Client", "test")],
        &serde_json::to_vec(&serde_json::json!({
            "challenge_id": challenge_id,
            "pin": pin
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    let token = confirm_resp.json().unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();
    let auth_header = format!("Bearer {}", token);

    // Refresh active session
    let refresh_resp = http_post(
        gw_addr,
        "/api/v1/session/refresh",
        &[
            ("Authorization", &auth_header),
            ("X-Fieldnet-Client", "test"),
        ],
        b"{}",
    )
    .await
    .unwrap();
    assert_eq!(refresh_resp.status, 200);
    let refresh_json = refresh_resp.json().unwrap();
    assert!(refresh_json["expires_in_seconds"].as_u64().unwrap() > 0);

    runtime.stop_gateway().await.unwrap();
    runtime.shutdown();
}

// ============================================================================
// Test 8: Multi-Node Overlay Test with Browser Gateway Boundary (Sections 31, 32)
// ============================================================================

#[tokio::test]
async fn test_multinode_browser_gateway_to_remote_overlay_service() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    // Dedicated discovery port for this multi-node test
    let test_discovery_port = 4867;

    // 1. Setup Node A (hosting Local Web Gateway)
    let dir_a = std::env::temp_dir().join("fieldnet_multinode_gw_a");
    let _ = fs::remove_dir_all(&dir_a);
    fs::create_dir_all(&dir_a).unwrap();

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();

    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };
    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let node_a_id = runtime_a.node_id();

    // 2. Setup Node B (hosting remote service on mesh)
    let dir_b = std::env::temp_dir().join("fieldnet_multinode_srv_b");
    let _ = fs::remove_dir_all(&dir_b);
    fs::create_dir_all(&dir_b).unwrap();

    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();

    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();
    let node_b_id = runtime_b.node_id();

    // 3. Wait for mutual TCP connection over mesh
    let mut connected = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let peers_a = runtime_a.get_connected_peers().await;
        let peers_b = runtime_b.get_connected_peers().await;
        if peers_a.contains(&node_b_id) && peers_b.contains(&node_a_id) {
            connected = true;
            break;
        }
    }
    assert!(
        connected,
        "Nodes A and B must establish mutual TCP connection"
    );

    // 4. Register service on Node B and advertise it
    let s_id = ServiceId::new("remote.calculator").unwrap();
    let instance_b = ServiceInstanceId::generate();
    let builder_b = ServiceManifestBuilder::new(s_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(instance_b.clone())
        .display_name("Remote Calculator on Node B")
        .endpoint(ServiceEndpoint::InternalRpc)
        .protocol(ServiceProtocol::NativeFieldnetRpc)
        .capability(ServiceCapability::new("service.read").unwrap())
        .capability(ServiceCapability::new("service.invoke").unwrap())
        .ttl_seconds(120);

    runtime_b.advertise_service(builder_b, None).await.unwrap();
    runtime_b
        .register_service_handler(
            s_id.clone(),
            Arc::new(EchoServiceHandler {
                node_id: runtime_b.node_id(),
            }),
        )
        .await;

    // Node A discovers Node B's advertisement over the overlay network
    let mut discovered = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Some(record) = runtime_a.service_registry().get_instance(&instance_b).await {
            assert_eq!(record.manifest.provider_node_id, node_b_id);
            assert_eq!(record.state(), ServiceState::Available);
            discovered = true;
            break;
        }
    }
    assert!(
        discovered,
        "Node A must discover Node B's service advertisement over overlay"
    );

    // 5. Start Gateway on Node A with auto_pair_localhost: false
    let mut gw_config = GatewayConfig::ephemeral();
    gw_config.auto_pair_localhost = false;
    let gw_addr_a = runtime_a.start_gateway(Some(gw_config)).await.unwrap();
    let gw_a = runtime_a.gateway().await.unwrap();

    // 6. Browser pairs with Node A Gateway
    let start_resp = http_post(
        gw_addr_a,
        "/api/v1/pair/start",
        &[("X-Fieldnet-Client", "browser-test")],
        b"{}",
    )
    .await
    .unwrap();
    let challenge_id = start_resp.json().unwrap()["challenge_id"]
        .as_str()
        .unwrap()
        .to_string();
    let pin = gw_a
        .session_store()
        .get_pending_challenge_pin(&challenge_id)
        .await
        .unwrap();

    let confirm_resp = http_post(
        gw_addr_a,
        "/api/v1/pair/confirm",
        &[("X-Fieldnet-Client", "browser-test")],
        &serde_json::to_vec(&serde_json::json!({
            "challenge_id": challenge_id,
            "pin": pin
        }))
        .unwrap(),
    )
    .await
    .unwrap();
    let token = confirm_resp.json().unwrap()["token"]
        .as_str()
        .unwrap()
        .to_string();

    // 7. Browser sends authorized request to Node A Gateway:
    // Flow: Browser -> Node A Gateway -> Node A ServiceResolver -> Overlay RPC -> Node B
    let auth_header = format!("Bearer {}", token);
    let remote_resp = http_get(
        gw_addr_a,
        "/services/remote.calculator/add",
        &[("Authorization", &auth_header)],
    )
    .await
    .unwrap();

    assert_eq!(remote_resp.status, 200);
    let resp_json = remote_resp.json().unwrap();
    assert_eq!(resp_json["service"], "remote.calculator");
    // Verifies responder is Node B!
    assert_eq!(resp_json["responder"], runtime_b.node_id().to_hex());

    // 8. Identity Invariant: Browser session is NOT a NodeId
    let session = gw_a.session_store().validate_token(&token).await.unwrap();
    let auth_ctx = session.into_auth_context();
    assert_eq!(
        auth_ctx.caller_node_id, None,
        "Browser session MUST NOT have a NodeId"
    );

    runtime_a.stop_gateway().await.unwrap();
    runtime_a.shutdown();
    runtime_b.shutdown();
}
