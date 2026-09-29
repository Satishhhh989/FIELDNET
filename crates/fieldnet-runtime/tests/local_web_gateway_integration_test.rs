use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use fieldnet_core::Timestamp;
use fieldnet_identity::NodeIdentity;
use fieldnet_runtime::service::{
    GatewayConfig, GatewayState, ServiceCapability, ServiceEndpoint, ServiceError, ServiceId,
    ServiceInstanceId, ServiceManifestBuilder, ServiceProtocol, ServiceRequestHandler,
    ServiceRpcRequestWire, ServiceRpcResponseWire, ServiceVersion,
};
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ============================================================================
// Raw Async HTTP Client Helpers (Zero Dependencies)
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
) -> Result<HttpResponse, Box<dyn std::error::Error + Send + Sync>> {
    http_request(addr, "GET", path, &[], &[]).await
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
    node_name: String,
}

#[async_trait::async_trait]
impl ServiceRequestHandler for EchoServiceHandler {
    async fn handle_request(
        &self,
        req: ServiceRpcRequestWire,
    ) -> Result<ServiceRpcResponseWire, ServiceError> {
        let mut headers = HashMap::new();
        headers.insert("content-type".to_string(), "application/json".to_string());
        let payload = serde_json::json!({
            "service": req.service_id.to_string(),
            "path": req.path,
            "method": req.method,
            "query": req.query,
            "provider_node": self.node_name,
            "echo_body": String::from_utf8_lossy(&req.body).to_string(),
        });

        Ok(ServiceRpcResponseWire {
            request_id: req.request_id,
            status_code: 200,
            headers,
            body: payload.to_string().into_bytes(),
            responder_node_id: req.provider_node_id,
            timestamp: Timestamp::now(),
        })
    }
}

struct SlowServiceHandler {
    delay_ms: u64,
}

#[async_trait::async_trait]
impl ServiceRequestHandler for SlowServiceHandler {
    async fn handle_request(
        &self,
        req: ServiceRpcRequestWire,
    ) -> Result<ServiceRpcResponseWire, ServiceError> {
        tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
        Ok(ServiceRpcResponseWire {
            request_id: req.request_id,
            status_code: 200,
            headers: HashMap::new(),
            body: b"slow response".to_vec(),
            responder_node_id: req.provider_node_id,
            timestamp: Timestamp::now(),
        })
    }
}

// ============================================================================
// 1. Root & Status Endpoints and Gateway Lifecycle Test
// ============================================================================

#[tokio::test]
async fn test_gateway_lifecycle_and_root_and_status_endpoints() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_gateway_lifecycle_test");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    let id = NodeIdentity::generate();
    id.save_to_file(&base_dir.join("identity.json")).unwrap();

    let config = RuntimeConfig {
        data_dir: base_dir.clone(),
        tcp_port: 0,
        discovery_port: 4831,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config).await.unwrap();

    // 1. Start gateway on ephemeral port
    let gw_addr = runtime
        .start_gateway(Some(GatewayConfig::ephemeral()))
        .await
        .unwrap();
    assert!(gw_addr.port() > 0);

    let gw = runtime.gateway().await.unwrap();
    assert_eq!(gw.state().await, GatewayState::Running);
    assert!(gw.is_running().await);

    // 2. GET / (Root Endpoint)
    let resp = http_get(gw_addr, "/").await.unwrap();
    assert_eq!(resp.status, 200);
    assert_eq!(
        resp.headers.get("content-type").map(|s| s.as_str()),
        Some("application/json")
    );
    let root_json = resp.json().unwrap();
    assert_eq!(root_json["fieldnet"], "LocalWebGateway");
    assert_eq!(root_json["version"], "0.1.0");
    assert_eq!(root_json["state"], "RUNNING");

    // 3. GET /api/v1/status (Health & State Inspection)
    let status_resp = http_get(gw_addr, "/api/v1/status").await.unwrap();
    assert_eq!(status_resp.status, 200);
    let status_json = status_resp.json().unwrap();
    assert_eq!(status_json["status"], "healthy");
    assert_eq!(status_json["gateway_state"], "RUNNING");

    // 4. Graceful Shutdown
    runtime.stop_gateway().await.unwrap();
    assert_eq!(gw.state().await, GatewayState::Stopped);

    // Connection should be refused after shutdown
    let conn_after_stop = TcpStream::connect(gw_addr).await;
    assert!(
        conn_after_stop.is_err(),
        "Gateway must close TCP listener after stop"
    );

    runtime.shutdown();
}

// ============================================================================
// 2. Browser Service Discovery Catalog API Test
// ============================================================================

#[tokio::test]
async fn test_sanitized_service_discovery_catalog_endpoint() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_gateway_catalog_test");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    let id = NodeIdentity::generate();
    id.save_to_file(&base_dir.join("identity.json")).unwrap();

    let config = RuntimeConfig {
        data_dir: base_dir.clone(),
        tcp_port: 0,
        discovery_port: 4832,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config).await.unwrap();
    let gw_addr = runtime
        .start_gateway(Some(GatewayConfig::ephemeral()))
        .await
        .unwrap();

    // Advertise a local service
    let service_id = ServiceId::new("weather.service").unwrap();
    let builder = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(2, 1, 0))
        .instance_id(ServiceInstanceId::generate())
        .display_name("Decentralized Weather Station")
        .description("Peer-to-peer real-time atmospheric observation telemetry")
        .endpoint(ServiceEndpoint::InternalRpc)
        .protocol(ServiceProtocol::NativeFieldnetRpc)
        .capability(ServiceCapability::new("weather.current").unwrap())
        .capability(ServiceCapability::new("weather.forecast").unwrap())
        .ttl_seconds(120);

    runtime.advertise_service(builder, None).await.unwrap();

    // Query /api/v1/services
    let resp = http_get(gw_addr, "/api/v1/services").await.unwrap();
    assert_eq!(resp.status, 200);
    let catalog = resp.json().unwrap();

    assert_eq!(catalog["schema_version"], 1);
    let services = catalog["services"].as_array().unwrap();
    assert_eq!(services.len(), 1);

    let svc = &services[0];
    assert_eq!(svc["service_id"], "weather.service");
    assert_eq!(svc["display_name"], "Decentralized Weather Station");
    assert_eq!(svc["version"], "2.1.0");
    assert_eq!(svc["mount_path"], "/services/weather.service");
    assert_eq!(svc["status"], "Available");
    let caps = svc["capabilities"].as_array().unwrap();
    assert_eq!(caps.len(), 2);

    // Verify /api/services alias works identically
    let alias_resp = http_get(gw_addr, "/api/services").await.unwrap();
    assert_eq!(alias_resp.status, 200);

    runtime.shutdown();
}

// ============================================================================
// 3. Real Multi-Node HTTP Service Routing Test
// ============================================================================

#[tokio::test]
async fn test_real_two_node_http_service_routing() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_two_node_http_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();

    let test_discovery_port = 4833;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();

    let config_a = RuntimeConfig {
        data_dir: dir_a.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };

    let config_b = RuntimeConfig {
        data_dir: dir_b.clone(),
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();

    // 1. Establish mutual TCP overlay connection
    let mut connected = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_a.get_connected_peers().await.contains(&node_b_id)
            && runtime_b.get_connected_peers().await.contains(&node_a_id)
        {
            connected = true;
            break;
        }
    }
    assert!(connected, "Nodes A and B must establish mutual TCP link");

    // 2. Register service and handler on Node B
    let service_id = ServiceId::new("files.service").unwrap();
    runtime_b
        .register_service_handler(
            service_id.clone(),
            Arc::new(EchoServiceHandler {
                node_name: "Node B".to_string(),
            }),
        )
        .await;

    let builder_b = ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
        .instance_id(ServiceInstanceId::generate())
        .display_name("Node B Files")
        .endpoint(ServiceEndpoint::InternalRpc)
        .protocol(ServiceProtocol::NativeFieldnetRpc)
        .capability(ServiceCapability::new("files.read").unwrap())
        .ttl_seconds(120);

    runtime_b.advertise_service(builder_b, None).await.unwrap();

    // 3. Wait for Node A to discover the service advertisement from Node B
    let mut discovered = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let instances = runtime_a
            .service_registry()
            .get_service_instances(&service_id)
            .await;
        if !instances.is_empty() {
            discovered = true;
            break;
        }
    }
    assert!(discovered, "Node A must discover files.service from Node B");

    // 4. Start Local Web Gateway ONLY on Node A
    let gw_addr_a = runtime_a
        .start_gateway(Some(GatewayConfig::ephemeral()))
        .await
        .unwrap();

    // 5. Client sends HTTP GET /services/files.service/list?page=1 to Node A
    let resp = http_get(gw_addr_a, "/services/files.service/list?page=1")
        .await
        .unwrap();
    assert_eq!(resp.status, 200, "HTTP request through gateway failed");
    let resp_json = resp.json().unwrap();
    assert_eq!(resp_json["service"], "files.service");
    assert_eq!(resp_json["path"], "/list");
    assert_eq!(resp_json["query"], "page=1");
    assert_eq!(resp_json["provider_node"], "Node B");

    // 6. Test canonical direct service URL mapping: GET /files.service/list
    let fallback_resp = http_get(gw_addr_a, "/files.service/list").await.unwrap();
    assert_eq!(fallback_resp.status, 200);
    let fallback_json = fallback_resp.json().unwrap();
    assert_eq!(fallback_json["provider_node"], "Node B");

    runtime_a.shutdown();
    runtime_b.shutdown();
}

// ============================================================================
// 4. Multi-Provider HTTP Routing & Automatic Resolver Failover Test
// ============================================================================

#[tokio::test]
async fn test_multiprovider_resolution_and_failover() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_multiprovider_http_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");
    let dir_c = base_dir.join("node_c");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    fs::create_dir_all(&dir_c).unwrap();

    let test_discovery_port = 4834;

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();
    let id_c = NodeIdentity::generate();
    id_c.save_to_file(&dir_c.join("identity.json")).unwrap();

    let _node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();
    let node_c_id = id_c.node_id();

    let config_a = RuntimeConfig {
        data_dir: dir_a,
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };
    let config_b = RuntimeConfig {
        data_dir: dir_b,
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };
    let config_c = RuntimeConfig {
        data_dir: dir_c,
        tcp_port: 0,
        discovery_port: test_discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();
    let runtime_c = FieldnetRuntime::start(config_c).await.unwrap();

    // Wait for mesh connections (A connects to B and C)
    let mut connected = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let peers_a = runtime_a.get_connected_peers().await;
        if peers_a.contains(&node_b_id) && peers_a.contains(&node_c_id) {
            connected = true;
            break;
        }
    }
    assert!(connected, "Node A must connect to both Node B and Node C");

    let service_id = ServiceId::new("compute.service").unwrap();

    // Register handler on both B and C
    runtime_b
        .register_service_handler(
            service_id.clone(),
            Arc::new(EchoServiceHandler {
                node_name: "Node B".to_string(),
            }),
        )
        .await;

    runtime_c
        .register_service_handler(
            service_id.clone(),
            Arc::new(EchoServiceHandler {
                node_name: "Node C".to_string(),
            }),
        )
        .await;

    // Advertise from both B and C
    let manifest_b = runtime_b
        .advertise_service(
            ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
                .instance_id(ServiceInstanceId::generate())
                .display_name("Node B Compute")
                .endpoint(ServiceEndpoint::InternalRpc)
                .protocol(ServiceProtocol::NativeFieldnetRpc)
                .ttl_seconds(120),
            None,
        )
        .await
        .unwrap();

    let manifest_c = runtime_c
        .advertise_service(
            ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
                .instance_id(ServiceInstanceId::generate())
                .display_name("Node C Compute")
                .endpoint(ServiceEndpoint::InternalRpc)
                .protocol(ServiceProtocol::NativeFieldnetRpc)
                .ttl_seconds(120),
            None,
        )
        .await
        .unwrap();

    // Wait for A to discover both instances
    let mut both_discovered = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let instances = runtime_a
            .service_registry()
            .get_service_instances(&service_id)
            .await;
        if instances.len() >= 2 {
            both_discovered = true;
            break;
        }
    }
    assert!(both_discovered, "Node A must discover both B and C");

    // Start gateway on A
    let gw_addr_a = runtime_a
        .start_gateway(Some(GatewayConfig::ephemeral()))
        .await
        .unwrap();

    // Send HTTP request to /services/compute.service/run
    let resp1 = http_get(gw_addr_a, "/services/compute.service/run")
        .await
        .unwrap();
    assert_eq!(resp1.status, 200);
    let json1 = resp1.json().unwrap();
    let first_provider = json1["provider_node"].as_str().unwrap().to_string();

    // Now withdraw / simulate failure of whichever provider handled the first request
    if first_provider == "Node B" {
        runtime_b
            .withdraw_service(&manifest_b.instance_id, None)
            .await
            .unwrap();
    } else {
        runtime_c
            .withdraw_service(&manifest_c.instance_id, None)
            .await
            .unwrap();
    }

    // Wait briefly for withdrawal propagation
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Send second HTTP request through Node A gateway
    let resp2 = http_get(gw_addr_a, "/services/compute.service/run")
        .await
        .unwrap();
    assert_eq!(
        resp2.status, 200,
        "HTTP request should succeed via resolver failover"
    );
    let json2 = resp2.json().unwrap();
    let second_provider = json2["provider_node"].as_str().unwrap();

    // Must have routed to the other provider
    assert_ne!(first_provider, second_provider);

    runtime_a.shutdown();
    runtime_b.shutdown();
    runtime_c.shutdown();
}

// ============================================================================
// 5. Multi-Hop Route Failure & Recovery Test
// ============================================================================

#[tokio::test]
async fn test_multihop_route_failure_and_recovery() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_multihop_route_failure_test");
    let dir_a = base_dir.join("node_a");
    let dir_b = base_dir.join("node_b");
    let dir_c = base_dir.join("node_c");

    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&dir_a).unwrap();
    fs::create_dir_all(&dir_b).unwrap();
    fs::create_dir_all(&dir_c).unwrap();

    let id_a = NodeIdentity::generate();
    id_a.save_to_file(&dir_a.join("identity.json")).unwrap();
    let id_b = NodeIdentity::generate();
    id_b.save_to_file(&dir_b.join("identity.json")).unwrap();
    let id_c = NodeIdentity::generate();
    id_c.save_to_file(&dir_c.join("identity.json")).unwrap();

    let node_a_id = id_a.node_id();
    let node_b_id = id_b.node_id();
    let node_c_id = id_c.node_id();

    // Topology: A <-> B <-> C (A is isolated from C)
    let config_a = RuntimeConfig {
        data_dir: dir_a,
        tcp_port: 0,
        discovery_port: 4835,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_c_id]),
    };
    let config_b = RuntimeConfig {
        data_dir: dir_b,
        tcp_port: 0,
        discovery_port: 4835,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_a_id, node_c_id]),
        blocked_peers: None,
    };
    let config_c = RuntimeConfig {
        data_dir: dir_c.clone(),
        tcp_port: 0,
        discovery_port: 4835,
        auto_connect: true,
        beacon_interval: Duration::from_millis(150),
        allowed_peers: Some(vec![node_b_id]),
        blocked_peers: Some(vec![node_a_id]),
    };

    let runtime_a = FieldnetRuntime::start(config_a).await.unwrap();
    let runtime_b = FieldnetRuntime::start(config_b).await.unwrap();
    let runtime_c = FieldnetRuntime::start(config_c.clone()).await.unwrap();

    // Wait for routing convergence: A discovers route to C via B
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_a.routing_table().get_next_hop(&node_c_id).await == Some(node_b_id) {
            break;
        }
    }

    // Register service on Node C
    let service_id = ServiceId::new("sensor.service").unwrap();
    runtime_c
        .register_service_handler(
            service_id.clone(),
            Arc::new(EchoServiceHandler {
                node_name: "Node C".to_string(),
            }),
        )
        .await;

    runtime_c
        .advertise_service(
            ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
                .instance_id(ServiceInstanceId::generate())
                .display_name("Node C Sensor Mesh")
                .endpoint(ServiceEndpoint::InternalRpc)
                .protocol(ServiceProtocol::NativeFieldnetRpc)
                .ttl_seconds(180),
            Some(node_a_id),
        )
        .await
        .unwrap();

    // Wait for multi-hop advertisement propagation to Node A
    let mut discovered_at_a = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if !runtime_a
            .service_registry()
            .get_service_instances(&service_id)
            .await
            .is_empty()
        {
            discovered_at_a = true;
            break;
        }
    }
    assert!(
        discovered_at_a,
        "Node A must discover sensor.service across 2 hops"
    );

    // Start gateway on Node A
    let gw_addr_a = runtime_a
        .start_gateway(Some(GatewayConfig::ephemeral()))
        .await
        .unwrap();

    // 1. Initial request through gateway works multi-hop
    let resp1 = http_get(gw_addr_a, "/services/sensor.service/read")
        .await
        .unwrap();
    assert_eq!(resp1.status, 200);
    assert_eq!(resp1.json().unwrap()["provider_node"], "Node C");

    // 2. Break the route by shutting down Node C
    runtime_c.shutdown();

    // Wait for B to detect disconnection and expire route on A
    tokio::time::sleep(Duration::from_millis(200)).await;
    runtime_a.routing_table().expire_route(&node_c_id).await;

    // 3. Request should return failure
    let resp2 = http_get(gw_addr_a, "/services/sensor.service/read")
        .await
        .unwrap();
    assert!(
        resp2.status == 503 || resp2.status == 504 || resp2.status == 500,
        "Gateway must return failure when route is broken, got {}",
        resp2.status
    );

    // 4. Service Registry on Node A remains intact
    assert!(
        !runtime_a
            .service_registry()
            .get_service_instances(&service_id)
            .await
            .is_empty(),
        "Service registry must remain intact despite link break"
    );

    // 5. Recover Node C
    let runtime_c_recovered = FieldnetRuntime::start(config_c).await.unwrap();
    runtime_c_recovered
        .register_service_handler(
            service_id.clone(),
            Arc::new(EchoServiceHandler {
                node_name: "Node C Recovered".to_string(),
            }),
        )
        .await;

    // Wait for route recovery
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if runtime_a.routing_table().get_next_hop(&node_c_id).await == Some(node_b_id) {
            break;
        }
    }

    // 6. Restored route makes the service usable again
    let resp3 = http_get(gw_addr_a, "/services/sensor.service/read")
        .await
        .unwrap();
    assert_eq!(
        resp3.status, 200,
        "Service must become usable again after route recovery"
    );

    runtime_a.shutdown();
    runtime_b.shutdown();
    runtime_c_recovered.shutdown();
}

// ============================================================================
// 6. Content Delivery Streaming Integration Test (Phase 07 ContentStore)
// ============================================================================

#[tokio::test]
async fn test_content_store_streaming_delivery() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_content_stream_test");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    let id = NodeIdentity::generate();
    id.save_to_file(&base_dir.join("identity.json")).unwrap();

    let config = RuntimeConfig {
        data_dir: base_dir.clone(),
        tcp_port: 0,
        discovery_port: 4836,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config).await.unwrap();
    let gw_addr = runtime
        .start_gateway(Some(GatewayConfig::ephemeral()))
        .await
        .unwrap();

    // Store test content (1024 bytes) in the ContentStore
    let sample_data =
        b"FIELDNET Decentralized Content Store Streaming Verification Payload 1234567890"
            .repeat(20);
    let (content_id, _res) = runtime
        .content_put(&sample_data, Some("text/plain".into()))
        .await
        .unwrap();
    let _ = runtime
        .content_store()
        .authorizer()
        .set_public(&content_id, true);
    let content_id_hex = content_id.to_hex();

    // 1. Full content stream request
    let resp = http_get(gw_addr, &format!("/api/v1/content/{}", content_id_hex))
        .await
        .unwrap();
    assert_eq!(resp.status, 200);
    assert_eq!(resp.body, sample_data);
    assert_eq!(
        resp.headers.get("accept-ranges").map(|s| s.as_str()),
        Some("bytes")
    );
    let etag = resp.headers.get("etag").cloned().unwrap();

    // 2. Conditional request with If-None-Match -> 304 Not Modified
    let cond_resp = http_request(
        gw_addr,
        "GET",
        &format!("/api/v1/content/{}", content_id_hex),
        &[("if-none-match", &etag)],
        &[],
    )
    .await
    .unwrap();
    assert_eq!(cond_resp.status, 304);
    assert!(cond_resp.body.is_empty());

    // 3. Range request (bytes=0-49) -> 206 Partial Content
    let range_resp = http_request(
        gw_addr,
        "GET",
        &format!("/api/v1/content/{}", content_id_hex),
        &[("range", "bytes=0-49")],
        &[],
    )
    .await
    .unwrap();
    assert_eq!(range_resp.status, 206);
    assert_eq!(range_resp.body.len(), 50);
    assert_eq!(range_resp.body, &sample_data[0..50]);
    assert!(range_resp.headers.contains_key("content-range"));

    runtime.shutdown();
}

// ============================================================================
// 7. Security Hardening & Threat Vectors Test (14 Attack Vectors)
// ============================================================================

#[tokio::test]
async fn test_http_security_vectors() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_gateway_security_test");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    let id = NodeIdentity::generate();
    id.save_to_file(&base_dir.join("identity.json")).unwrap();

    let config = RuntimeConfig {
        data_dir: base_dir.clone(),
        tcp_port: 0,
        discovery_port: 4837,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config).await.unwrap();

    let mut gw_config = GatewayConfig::ephemeral();
    gw_config.max_request_size_bytes = 1024; // 1 KiB
    gw_config.request_timeout_ms = 500; // 500ms timeout for testing
    let gw_addr = runtime.start_gateway(Some(gw_config)).await.unwrap();

    // Register a valid service for target tests
    let service_id = ServiceId::new("secure.service").unwrap();
    runtime
        .register_service_handler(
            service_id.clone(),
            Arc::new(EchoServiceHandler {
                node_name: "LocalNode".to_string(),
            }),
        )
        .await;

    runtime
        .advertise_service(
            ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
                .instance_id(ServiceInstanceId::generate())
                .display_name("Secure Service")
                .endpoint(ServiceEndpoint::InternalRpc)
                .protocol(ServiceProtocol::NativeFieldnetRpc)
                .ttl_seconds(120),
            None,
        )
        .await
        .unwrap();

    // Vector 1: Malformed Request
    {
        let mut stream = TcpStream::connect(gw_addr).await.unwrap();
        stream
            .write_all(b"NOT_A_VALID_HTTP_REQUEST_LINE\r\n\r\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let resp = parse_http_response(&buf).unwrap();
        assert_eq!(resp.status, 400, "Malformed HTTP must return 400");
    }

    // Vector 2: Oversized Body
    {
        let big_body = vec![b'A'; 2048]; // limit is 1024
        let resp = http_post(
            gw_addr,
            "/services/secure.service/upload",
            &[
                ("content-type", "application/octet-stream"),
                ("x-fieldnet-client", "test"),
            ],
            &big_body,
        )
        .await
        .unwrap();
        assert_eq!(
            resp.status, 413,
            "Oversized payload must return 413 Payload Too Large"
        );
    }

    // Vector 3: Oversized Headers (exceeding limit)
    {
        let big_header = "X".repeat(70 * 1024);
        let resp = http_request(
            gw_addr,
            "GET",
            "/services/secure.service/read",
            &[("x-huge-header", &big_header)],
            &[],
        )
        .await
        .unwrap();
        assert!(
            resp.status == 431 || resp.status == 400,
            "Oversized header must be rejected"
        );
    }

    // Vector 4: DNS Rebinding Protection via Host Header Whitelist
    {
        let mut stream = TcpStream::connect(gw_addr).await.unwrap();
        let req = "GET / HTTP/1.1\r\nHost: evil-attacker.com\r\nConnection: close\r\n\r\n";
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let resp = parse_http_response(&buf).unwrap();
        assert_eq!(
            resp.status, 400,
            "Untrusted Host header must be rejected to prevent DNS rebinding"
        );
    }

    // Vector 5: Arbitrary Proxy URL Attempt
    {
        let resp = http_get(gw_addr, "/services/http://google.com/test")
            .await
            .unwrap();
        assert_eq!(
            resp.status, 400,
            "Arbitrary URL proxying must be rejected with 400"
        );
    }

    // Vector 6: Localhost Proxy Attempt
    {
        let resp = http_get(gw_addr, "/services/localhost:22/admin")
            .await
            .unwrap();
        assert_eq!(
            resp.status, 400,
            "Localhost proxy attempts must be rejected with 400"
        );
    }

    // Vector 7: Private IP Proxy Attempt
    {
        let resp = http_get(gw_addr, "/services/192.168.1.1/setup")
            .await
            .unwrap();
        assert_eq!(
            resp.status, 400,
            "Private IP proxying must be rejected with 400"
        );
    }

    // Vector 8: Invalid Service ID Characters
    {
        let resp = http_get(gw_addr, "/services/invalid$$service!!/read")
            .await
            .unwrap();
        assert_eq!(resp.status, 400, "Invalid service name must return 400");
    }

    // Vector 9: Unknown Service Path
    {
        let resp = http_get(gw_addr, "/services/nonexistent.service/items")
            .await
            .unwrap();
        assert_eq!(resp.status, 404, "Unknown service must return 404");
    }

    // Vector 10: Anti-CSRF on State-Changing Method (POST without Origin or Client Header)
    {
        let resp = http_request(
            gw_addr,
            "POST",
            "/services/secure.service/mutate",
            &[("origin", "http://evil-site.com")],
            b"data",
        )
        .await
        .unwrap();
        assert_eq!(
            resp.status, 403,
            "Untrusted origin without pairing token must be rejected with 403 Forbidden"
        );
    }

    // Vector 11: Anti-CSRF with valid custom header passes
    {
        let resp = http_request(
            gw_addr,
            "POST",
            "/services/secure.service/mutate",
            &[("x-fieldnet-client", "local-app")],
            b"valid-data",
        )
        .await
        .unwrap();
        assert_eq!(
            resp.status, 200,
            "State-changing request with valid client header must succeed"
        );
    }

    // Vector 12: Request Timeout (Slow Service)
    {
        let slow_id = ServiceId::new("slow.service").unwrap();
        runtime
            .register_service_handler(
                slow_id.clone(),
                Arc::new(SlowServiceHandler { delay_ms: 1000 }), // 1000ms > 500ms timeout
            )
            .await;

        runtime
            .advertise_service(
                ServiceManifestBuilder::new(slow_id.clone(), ServiceVersion::new(1, 0, 0))
                    .instance_id(ServiceInstanceId::generate())
                    .display_name("Slow Service")
                    .endpoint(ServiceEndpoint::InternalRpc)
                    .protocol(ServiceProtocol::NativeFieldnetRpc)
                    .ttl_seconds(120),
                None,
            )
            .await
            .unwrap();

        let resp = http_get(gw_addr, "/services/slow.service/wait")
            .await
            .unwrap();
        assert_eq!(
            resp.status, 504,
            "Exceeded deadline must yield 504 Gateway Timeout"
        );
    }

    // Vector 13: Client Disconnect During Streaming (Server survives safely)
    {
        let mut stream = TcpStream::connect(gw_addr).await.unwrap();
        stream
            .write_all(b"GET /services/secure.service/read HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        // Drop stream immediately without reading response
        drop(stream);
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Verify gateway remains operational
        let resp = http_get(gw_addr, "/api/v1/status").await.unwrap();
        assert_eq!(resp.status, 200);
    }

    // Vector 14: CORS Preflight OPTIONS Request
    {
        let resp = http_request(
            gw_addr,
            "OPTIONS",
            "/services/secure.service/read",
            &[
                ("origin", "http://localhost:8080"),
                ("access-control-request-method", "POST"),
            ],
            &[],
        )
        .await
        .unwrap();
        assert_eq!(
            resp.status, 204,
            "CORS OPTIONS preflight must return 204 No Content"
        );
        assert_eq!(
            resp.headers
                .get("access-control-allow-origin")
                .map(|s| s.as_str()),
            Some("http://localhost:8080")
        );
    }

    runtime.shutdown();
}

// ============================================================================
// 8. Performance Baseline & Concurrent Requests Test
// ============================================================================

#[tokio::test]
async fn test_concurrent_http_requests_performance_baseline() {
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    let base_dir = std::env::temp_dir().join("fieldnet_gateway_perf_test");
    let _ = fs::remove_dir_all(&base_dir);
    fs::create_dir_all(&base_dir).unwrap();

    let id = NodeIdentity::generate();
    id.save_to_file(&base_dir.join("identity.json")).unwrap();

    let config = RuntimeConfig {
        data_dir: base_dir.clone(),
        tcp_port: 0,
        discovery_port: 4838,
        auto_connect: false,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config).await.unwrap();
    let gw_addr = runtime
        .start_gateway(Some(GatewayConfig::ephemeral()))
        .await
        .unwrap();

    let service_id = ServiceId::new("perf.service").unwrap();
    runtime
        .register_service_handler(
            service_id.clone(),
            Arc::new(EchoServiceHandler {
                node_name: "LocalPerfNode".to_string(),
            }),
        )
        .await;

    runtime
        .advertise_service(
            ServiceManifestBuilder::new(service_id.clone(), ServiceVersion::new(1, 0, 0))
                .instance_id(ServiceInstanceId::generate())
                .display_name("Performance Service")
                .endpoint(ServiceEndpoint::InternalRpc)
                .protocol(ServiceProtocol::NativeFieldnetRpc)
                .ttl_seconds(120),
            None,
        )
        .await
        .unwrap();

    let concurrency = 30;
    let mut handles = Vec::new();
    let start_time = std::time::Instant::now();

    for i in 0..concurrency {
        let addr = gw_addr;
        let handle = tokio::spawn(async move {
            let path = format!("/services/perf.service/req?id={}", i);
            http_get(addr, &path).await
        });
        handles.push(handle);
    }

    let mut successful = 0;
    for h in handles {
        let resp = h.await.unwrap().unwrap();
        if resp.status == 200 {
            successful += 1;
        }
    }

    let elapsed = start_time.elapsed();
    println!(
        "Processed {} concurrent requests in {:?} (avg: {:?}/req)",
        concurrency,
        elapsed,
        elapsed / concurrency
    );

    assert_eq!(
        successful, concurrency,
        "All {} concurrent requests must succeed",
        concurrency
    );

    runtime.shutdown();
}
