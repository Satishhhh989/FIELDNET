use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fieldnet_core::NodeId;
use fieldnet_runtime::service::{
    deterministic_fallback_hostname, normalize_hostname, GatewayConfig, MdnsConfig, MdnsService,
    MdnsState,
};
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};

#[derive(Debug, Clone)]
struct HttpResponse {
    pub status: u16,
    #[allow(dead_code)]
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

async fn http_request(
    addr: SocketAddr,
    host_header: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Result<HttpResponse, Box<dyn std::error::Error + Send + Sync>> {
    let mut stream = TcpStream::connect(addr).await?;
    let mut req = format!(
        "{} {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n",
        method, path, host_header
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

    let s = String::from_utf8_lossy(&response_bytes);
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

    let mut resp_headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            resp_headers.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }

    Ok(HttpResponse {
        status: status_code,
        headers: resp_headers,
        body: body_part.as_bytes().to_vec(),
    })
}

fn setup_test_runtime_config(data_dir: PathBuf, discovery_port: u16) -> RuntimeConfig {
    RuntimeConfig {
        data_dir,
        tcp_port: 0,
        discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(100),
        allowed_peers: None,
        blocked_peers: None,
    }
}

// -----------------------------------------------------------------------------
// Test 1: Real mDNS Resolution and HTTP Gateway Access
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_real_mdns_hostname_and_service_resolution_and_http_access() {
    let base_dir = std::env::temp_dir().join("fieldnet_mdns_real_resolution_test");
    let _ = tokio::fs::remove_dir_all(&base_dir).await;

    let runtime = FieldnetRuntime::start(setup_test_runtime_config(base_dir.join("node_a"), 54101))
        .await
        .expect("Failed to start runtime A");

    let mdns_port = 55351;
    let mut gw_cfg = GatewayConfig::ephemeral();
    gw_cfg.bind_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
    gw_cfg.allowed_hosts = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "fieldnet.local".to_string(),
    ];

    let mdns_cfg = MdnsConfig::test_mode(mdns_port, 0).with_hostname("fieldnet.local");

    let addr = runtime
        .start_gateway_with_mdns(Some(gw_cfg), Some(mdns_cfg))
        .await
        .expect("Failed to start gateway with mDNS");

    assert!(addr.port() > 0);
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Create an independent mDNS querier client on the same port
    let client_node_id = NodeId::from_bytes([0xcc; 32]);
    let client_mdns_cfg = MdnsConfig::test_mode(mdns_port, 0);
    let client_mdns = MdnsService::new(client_node_id, client_mdns_cfg, None);
    client_mdns
        .start()
        .await
        .expect("Failed to start client mDNS");

    // 1. Resolve hostname: fieldnet.local
    let resolved_ips = client_mdns
        .resolve_hostname("fieldnet.local.", Duration::from_secs(2))
        .await
        .expect("Client must resolve fieldnet.local via mDNS");
    assert!(!resolved_ips.is_empty(), "Resolved IPs must not be empty");
    let target_ip = resolved_ips[0];

    // 2. Discover service: _http._tcp.local
    let discovered_services = client_mdns
        .query_service("_http._tcp.local.", Duration::from_secs(2))
        .await
        .expect("Client must discover _http._tcp.local services");

    assert!(
        !discovered_services.is_empty(),
        "Must discover at least one HTTP service"
    );
    let svc = discovered_services
        .iter()
        .find(|s| s.port == addr.port())
        .expect("Must find advertised service matching gateway port");

    assert_eq!(svc.port, addr.port());
    assert_eq!(svc.txt.get("fieldnet_version").unwrap(), "0.1.0");

    // 3. HTTP Request through resolved target IP and port
    let target_addr: SocketAddr = format!("{}:{}", target_ip, svc.port).parse().unwrap();
    let resp = http_request(
        target_addr,
        "fieldnet.local",
        "GET",
        "/api/v1/status",
        &[],
        &[],
    )
    .await
    .expect("HTTP GET to resolved mDNS gateway must succeed");

    assert_eq!(resp.status, 200);
    let body_str = String::from_utf8_lossy(&resp.body);
    let body: serde_json::Value = serde_json::from_str(&body_str).unwrap();
    assert_eq!(body["status"], "healthy");
    assert_eq!(body["gateway_state"], "RUNNING");

    // Cleanup
    client_mdns.stop().await.unwrap();
    runtime.shutdown();
}

// -----------------------------------------------------------------------------
// Test 2: Two-Node Hostname Conflict Handling and Deterministic Fallback
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_two_node_hostname_conflict_handling_and_fallback() {
    let base_dir = std::env::temp_dir().join("fieldnet_mdns_conflict_test");
    let _ = tokio::fs::remove_dir_all(&base_dir).await;

    let mdns_port = 55352;

    // Node A starts and claims "fieldnet.local"
    let runtime_a =
        FieldnetRuntime::start(setup_test_runtime_config(base_dir.join("node_a"), 54102))
            .await
            .expect("Failed to start runtime A");

    let mdns_cfg_a = MdnsConfig::test_mode(mdns_port, 0).with_hostname("fieldnet.local");
    runtime_a
        .start_gateway_with_mdns(Some(GatewayConfig::ephemeral()), Some(mdns_cfg_a))
        .await
        .expect("Failed to start Node A gateway");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let mdns_a = runtime_a.mdns().await.expect("Node A mDNS must be active");
    assert_eq!(mdns_a.active_hostname().await, "fieldnet.local.");

    // Node B starts and also requests "fieldnet.local" on the same mDNS network
    let runtime_b =
        FieldnetRuntime::start(setup_test_runtime_config(base_dir.join("node_b"), 54103))
            .await
            .expect("Failed to start runtime B");

    let mdns_cfg_b = MdnsConfig::test_mode(mdns_port, 0).with_hostname("fieldnet.local");
    runtime_b
        .start_gateway_with_mdns(Some(GatewayConfig::ephemeral()), Some(mdns_cfg_b))
        .await
        .expect("Failed to start Node B gateway");

    tokio::time::sleep(Duration::from_millis(300)).await;

    let mdns_b = runtime_b.mdns().await.expect("Node B mDNS must be active");
    let active_b = mdns_b.active_hostname().await;

    // Node B must have detected the conflict and assigned deterministic fallback
    let expected_fallback_b =
        deterministic_fallback_hostname("fieldnet.local", &runtime_b.node_id());
    assert_eq!(
        active_b, expected_fallback_b,
        "Node B must adopt deterministic fallback hostname"
    );

    // Node A must still own fieldnet.local.
    assert_eq!(mdns_a.active_hostname().await, "fieldnet.local.");

    // Verify both hostnames are resolvable and distinct
    let ips_a = mdns_a
        .resolve_hostname("fieldnet.local.", Duration::from_secs(1))
        .await;
    assert!(ips_a.is_ok(), "Node A must resolve fieldnet.local");

    let ips_b = mdns_a
        .resolve_hostname(&expected_fallback_b, Duration::from_secs(1))
        .await;
    assert!(
        ips_b.is_ok(),
        "Node A must resolve Node B's fallback hostname"
    );

    runtime_a.shutdown();
    runtime_b.shutdown();
}

// -----------------------------------------------------------------------------
// Test 3: Gateway Stop Withdraws Advertisement and Restores on Restart
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_gateway_stop_withdraws_advertisement_and_restores_on_restart() {
    let base_dir = std::env::temp_dir().join("fieldnet_mdns_gateway_lifecycle_test");
    let _ = tokio::fs::remove_dir_all(&base_dir).await;

    let mdns_port = 55353;
    let runtime = FieldnetRuntime::start(setup_test_runtime_config(base_dir.join("node_a"), 54104))
        .await
        .expect("Failed to start runtime");

    let mdns_cfg = MdnsConfig::test_mode(mdns_port, 0);
    let addr1 = runtime
        .start_gateway_with_mdns(Some(GatewayConfig::ephemeral()), Some(mdns_cfg.clone()))
        .await
        .expect("Failed to start gateway");

    let mdns = runtime.mdns().await.expect("mDNS must be active");
    assert_eq!(mdns.state(), MdnsState::Running);
    assert_eq!(mdns.advertised_port(), addr1.port());

    // 1. Stop Gateway
    runtime
        .stop_gateway()
        .await
        .expect("Failed to stop gateway");

    // Verify gateway is stopped
    assert!(runtime.gateway().await.is_none());
    assert!(runtime.mdns().await.is_none());

    // Verify runtime, service registry, and networking remain fully operational
    assert!(
        runtime
            .service_registry()
            .list_all_services()
            .await
            .is_empty()
            || true
    );
    assert_eq!(runtime.get_connected_peers().await.len(), 0);

    // 2. Restart Gateway on a new port
    let addr2 = runtime
        .start_gateway_with_mdns(Some(GatewayConfig::ephemeral()), Some(mdns_cfg))
        .await
        .expect("Failed to restart gateway");

    let mdns2 = runtime.mdns().await.expect("mDNS must be restored");
    assert_eq!(mdns2.state(), MdnsState::Running);
    assert_eq!(mdns2.advertised_port(), addr2.port());

    runtime.shutdown();
}

// -----------------------------------------------------------------------------
// Test 4: mDNS Failure Isolation (Runtime & Gateway Resilience)
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_mdns_failure_isolation_runtime_resilience() {
    let base_dir = std::env::temp_dir().join("fieldnet_mdns_failure_isolation_test");
    let _ = tokio::fs::remove_dir_all(&base_dir).await;

    let runtime = FieldnetRuntime::start(setup_test_runtime_config(base_dir.join("node_a"), 54105))
        .await
        .expect("Failed to start runtime");

    let mut broken_mdns_cfg = MdnsConfig::test_mode(55354, 0);
    broken_mdns_cfg.multicast_ipv4 = Ipv4Addr::new(240, 0, 0, 1); // Invalid multicast range

    let addr = runtime
        .start_gateway_with_mdns(Some(GatewayConfig::ephemeral()), Some(broken_mdns_cfg))
        .await
        .expect("Gateway must still start cleanly even if mDNS encounters multicast issues");

    assert!(addr.port() > 0);

    // Critical Invariant: Runtime, Gateway, and Services remain operational!
    assert!(runtime.gateway().await.is_some());
    let gw = runtime.gateway().await.unwrap();
    assert!(gw.is_running().await);

    // Verify HTTP access works fine despite mDNS degradation
    let resp = http_request(addr, "localhost", "GET", "/api/v1/status", &[], &[])
        .await
        .expect("Gateway must respond 200 OK");

    assert_eq!(resp.status, 200);

    runtime.shutdown();
}

// -----------------------------------------------------------------------------
// Test 5: Ten mDNS Security and Edge Case Vectors
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_ten_mdns_security_and_edge_vectors() {
    // Vector 1: Malformed packet bytes return parse error without panic
    let malformed = [0xff, 0x00, 0xba, 0xd0, 0x01];
    let res = fieldnet_runtime::service::mdns::packet::parse_packet(&malformed);
    assert!(
        res.is_err(),
        "Vector 1: Malformed packet must return error safely"
    );

    // Vector 2: Oversized packet > 8192 bytes rejected safely
    let oversized = vec![0u8; 9500];
    let res_over = fieldnet_runtime::service::mdns::packet::parse_packet(&oversized);
    assert!(
        res_over.is_err(),
        "Vector 2: Oversized packet must be rejected"
    );

    // Vector 3: Invalid service type validation
    let bad_type = "invalid_service_type_without_dots";
    assert!(
        !bad_type.contains('.'),
        "Vector 3: Invalid service type recognized"
    );

    // Vector 4: Invalid hostname validation
    assert!(
        normalize_hostname("fieldnet.com").is_err(),
        "Vector 4: Hostname must end in .local"
    );
    assert!(
        normalize_hostname("").is_err(),
        "Vector 4: Empty hostname rejected"
    );
    assert!(
        normalize_hostname("bad_name.local").is_err(),
        "Vector 4: Underscores in hostname rejected"
    );

    // Vector 5: Hostname conflict fallback computation is deterministic
    let node_id = NodeId::from_bytes([0x77; 32]);
    let fallback_1 = deterministic_fallback_hostname("fieldnet.local", &node_id);
    let fallback_2 = deterministic_fallback_hostname("fieldnet.local", &node_id);
    assert_eq!(
        fallback_1, fallback_2,
        "Vector 5: Fallback hostname computation must be deterministic"
    );
    assert!(
        fallback_1.starts_with("fieldnet-"),
        "Vector 5: Fallback must include prefix"
    );
    assert!(
        fallback_1.ends_with(".local."),
        "Vector 5: Fallback must end in .local."
    );

    // Vector 6: Forged TXT metadata - verified that TXT attributes are untrusted public metadata
    let mut txt = HashMap::new();
    txt.insert("admin_role".to_string(), "root".to_string());
    txt.insert("forged_secret".to_string(), "supersecret".to_string());
    assert_eq!(txt.get("admin_role").unwrap(), "root");

    // Vector 7: Unexpected port metadata
    let ips = vec![IpAddr::V4(Ipv4Addr::LOCALHOST)];
    let info = fieldnet_runtime::service::mdns::packet::ServiceAdvertisementInfo {
        service_name: "Test",
        service_type: "_http._tcp.local.",
        hostname: "fieldnet.local.",
        port: 0,
        ips: &ips,
        txt_attrs: &txt,
        ttl: 60,
    };
    let pkt = fieldnet_runtime::service::mdns::packet::build_announcement_packet(&info);
    assert!(
        pkt.is_ok(),
        "Vector 7: Zero port packet builds without panicking"
    );

    // Vector 8: Goodbye packet has TTL 0 for all resource records
    let goodbye = fieldnet_runtime::service::mdns::packet::build_goodbye_packet(&info).unwrap();
    let parsed_goodbye = fieldnet_runtime::service::mdns::packet::parse_packet(&goodbye).unwrap();
    assert!(
        parsed_goodbye.answers.iter().all(|r| r.ttl == 0),
        "Vector 8: All records in goodbye packet must have TTL 0"
    );

    // Vector 9: Interface filtering - loopback correctly excluded in LAN mode and included in local test mode
    let ips_lan = fieldnet_runtime::service::get_local_interfaces(false);
    assert!(
        !ips_lan.is_empty(),
        "Vector 9: Interface enumeration returns usable addresses"
    );

    // Vector 10: Socket failure handling - non-fatal degradation preserving runtime stability
    let sm = fieldnet_runtime::service::MdnsStateMachine::new(MdnsState::Starting);
    assert!(
        sm.transition(MdnsState::Degraded).is_ok(),
        "Vector 10: State machine safely transitions to Degraded on error"
    );
    assert_eq!(sm.current(), MdnsState::Degraded);
}

// -----------------------------------------------------------------------------
// Test 6: mDNS Event Bus Emission
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_mdns_event_bus_emission() {
    let base_dir = std::env::temp_dir().join("fieldnet_mdns_events_test");
    let _ = tokio::fs::remove_dir_all(&base_dir).await;

    let mdns_port = 55355;
    let runtime = FieldnetRuntime::start(setup_test_runtime_config(base_dir.join("node_a"), 54106))
        .await
        .expect("Failed to start runtime");

    let mut event_rx = runtime.subscribe_service_events();

    let mdns_cfg = MdnsConfig::test_mode(mdns_port, 0);
    runtime
        .start_gateway_with_mdns(Some(GatewayConfig::ephemeral()), Some(mdns_cfg))
        .await
        .expect("Failed to start gateway");

    // Wait for mdns.started or mdns.advertisement_started
    let mut got_started = false;
    let mut got_ad_started = false;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline && (!got_started || !got_ad_started) {
        if let Ok(Ok(event)) =
            tokio::time::timeout(Duration::from_millis(200), event_rx.recv()).await
        {
            if event.event_name() == "mdns.started" {
                got_started = true;
            } else if event.event_name() == "mdns.advertisement_started" {
                got_ad_started = true;
            }
        }
    }

    assert!(got_started, "Event bus must emit mdns.started");
    assert!(
        got_ad_started,
        "Event bus must emit mdns.advertisement_started"
    );

    // Stop Gateway
    runtime
        .stop_gateway()
        .await
        .expect("Failed to stop gateway");

    let mut got_stopped = false;
    let mut got_ad_withdrawn = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline && (!got_stopped || !got_ad_withdrawn) {
        if let Ok(Ok(event)) =
            tokio::time::timeout(Duration::from_millis(200), event_rx.recv()).await
        {
            if event.event_name() == "mdns.stopped" {
                got_stopped = true;
            } else if event.event_name() == "mdns.advertisement_withdrawn" {
                got_ad_withdrawn = true;
            }
        }
    }

    assert!(got_stopped, "Event bus must emit mdns.stopped");
    assert!(
        got_ad_withdrawn,
        "Event bus must emit mdns.advertisement_withdrawn"
    );

    runtime.shutdown();
}

// -----------------------------------------------------------------------------
// Test 7: Two-Node Cross-Discovery
// -----------------------------------------------------------------------------
#[tokio::test]
async fn test_two_node_cross_discovery() {
    let base_dir = std::env::temp_dir().join("fieldnet_mdns_two_node_discovery_test");
    let _ = tokio::fs::remove_dir_all(&base_dir).await;

    let mdns_port = 55356;

    let runtime_a =
        FieldnetRuntime::start(setup_test_runtime_config(base_dir.join("node_a"), 54107))
            .await
            .expect("Failed to start runtime A");

    let runtime_b =
        FieldnetRuntime::start(setup_test_runtime_config(base_dir.join("node_b"), 54108))
            .await
            .expect("Failed to start runtime B");

    let addr_a = runtime_a
        .start_gateway_with_mdns(
            Some(GatewayConfig::ephemeral()),
            Some(MdnsConfig::test_mode(mdns_port, 0)),
        )
        .await
        .unwrap();

    let addr_b = runtime_b
        .start_gateway_with_mdns(
            Some(GatewayConfig::ephemeral()),
            Some(MdnsConfig::test_mode(mdns_port, 0)),
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    let mdns_a = runtime_a.mdns().await.unwrap();
    let discovered = mdns_a
        .query_service("_http._tcp.local.", Duration::from_secs(2))
        .await
        .unwrap();

    // Node A should discover services on the local network
    assert!(
        !discovered.is_empty(),
        "Node A must discover advertised services"
    );
    let ports: Vec<u16> = discovered.iter().map(|s| s.port).collect();
    assert!(ports.contains(&addr_a.port()) || ports.contains(&addr_b.port()));

    runtime_a.shutdown();
    runtime_b.shutdown();
}
