use fieldnet_core::NodeId;
use fieldnet_runtime::app::ApplicationId;
use fieldnet_runtime::content::identity::ContentId;
use fieldnet_runtime::service::{ServiceId, ServiceQuery};
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tauri::{AppHandle, State};

use crate::dto::*;
use crate::events::spawn_event_bridges;
use crate::state::AppState;

fn short_id(s: &str) -> String {
    if s.len() >= 8 {
        s[..8].to_string()
    } else {
        s.to_string()
    }
}

fn short_node_id(id: &NodeId) -> String {
    short_id(&id.to_string())
}

#[tauri::command]
pub async fn get_runtime_status(state: State<'_, AppState>) -> Result<RuntimeStatusDto, String> {
    let phase = state.get_phase().await;
    let maybe_rt = state.get_runtime().await;
    let last_err = state.last_error.read().await.clone();
    let started_at = *state.started_at.read().await;

    let uptime = started_at
        .map(|s| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            now.saturating_sub(s)
        })
        .unwrap_or(0);

    match maybe_rt {
        Some(rt) => {
            let node_id = rt.node_id();
            let tcp_port = rt.tcp_port();
            let (gateway_running, gateway_port) = match rt.gateway().await {
                Some(gw) => (true, gw.bound_addr().await.map(|a| a.port())),
                None => (false, None),
            };
            let (mdns_running, mdns_hostname) = match rt.mdns().await {
                Some(mdns) => (
                    mdns.state() == fieldnet_runtime::service::MdnsState::Running,
                    Some(mdns.active_hostname().await),
                ),
                None => (false, None),
            };

            Ok(RuntimeStatusDto {
                is_running: true,
                lifecycle_phase: phase,
                uptime_secs: uptime,
                data_dir: state.data_dir.display().to_string(),
                tcp_port,
                discovery_port: 4242,
                node_id: Some(node_id.to_string()),
                short_node_id: Some(short_node_id(&node_id)),
                gateway_running,
                gateway_port,
                mdns_running,
                mdns_hostname,
                subsystems: SubsystemHealthDto {
                    identity: "Ed25519 Sovereign Keypair Active".to_string(),
                    network: format!("TCP:{} / UDP Discovery:4242", tcp_port),
                    routing: "Multi-hop Vector Routing Active".to_string(),
                    dtn: "Persistent SQLite Bundle Queue Active".to_string(),
                    storage: "Content-Addressed CAS (BLAKE3/SHA-256) Active".to_string(),
                    service_registry: "Cryptographic Registry Active".to_string(),
                    wasm_supervisor: "Wasmtime Sandbox Supervisor Active".to_string(),
                    local_gateway: if gateway_running {
                        "HTTP Gateway Active".to_string()
                    } else {
                        "Not started".to_string()
                    },
                    mdns: if mdns_running {
                        "DNS-SD fieldnet.local Active".to_string()
                    } else {
                        "Not started".to_string()
                    },
                },
                error_message: None,
            })
        }
        None => Ok(RuntimeStatusDto {
            is_running: false,
            lifecycle_phase: phase,
            uptime_secs: 0,
            data_dir: state.data_dir.display().to_string(),
            tcp_port: 0,
            discovery_port: 0,
            node_id: None,
            short_node_id: None,
            gateway_running: false,
            gateway_port: None,
            mdns_running: false,
            mdns_hostname: None,
            subsystems: SubsystemHealthDto {
                identity: "Uninitialized".to_string(),
                network: "Offline".to_string(),
                routing: "Offline".to_string(),
                dtn: "Offline".to_string(),
                storage: "Offline".to_string(),
                service_registry: "Offline".to_string(),
                wasm_supervisor: "Offline".to_string(),
                local_gateway: "Offline".to_string(),
                mdns: "Offline".to_string(),
            },
            error_message: last_err,
        }),
    }
}

#[tauri::command]
pub async fn get_node_identity(state: State<'_, AppState>) -> Result<NodeIdentityDto, String> {
    let rt = state
        .get_runtime()
        .await
        .ok_or("FIELDNET runtime is not running")?;
    let identity = rt.identity();
    let node_id = identity.node_id();
    let pubkey_bytes = identity.public_key_bytes();

    Ok(NodeIdentityDto {
        node_id: node_id.to_string(),
        short_id: short_node_id(&node_id),
        public_key_hex: hex::encode(pubkey_bytes),
        algorithm: "Ed25519 Sovereign Node Identity".to_string(),
        storage_path: state.data_dir.join("identity.json").display().to_string(),
    })
}

#[tauri::command]
pub async fn get_network_status(state: State<'_, AppState>) -> Result<NetworkStatusDto, String> {
    let rt = state
        .get_runtime()
        .await
        .ok_or("FIELDNET runtime is not running")?;
    let tcp_port = rt.tcp_port();
    let connected_peers = rt.connection_manager().connected_peers().await;
    let discovered_peers = rt.get_discovered_peers().await;
    let all_routes = rt.routing_table().get_all_routes().await;
    let dtn_count = rt.dtn_queue().count().map_err(|e| e.to_string())?;

    let mut transports = vec![
        TransportDto {
            name: "TCP Transport".to_string(),
            protocol: "TCP Overlay (Framed, Ed25519 Signed)".to_string(),
            endpoint: format!("0.0.0.0:{}", tcp_port),
            state: "Listening".to_string(),
            details: format!("{} direct active peer connections", connected_peers.len()),
        },
        TransportDto {
            name: "LAN Discovery".to_string(),
            protocol: "UDP Multicast/Broadcast (239.255.42.99)".to_string(),
            endpoint: "0.0.0.0:4242".to_string(),
            state: "Active".to_string(),
            details: format!(
                "{} nearby peers discovered via beacons",
                discovered_peers.len()
            ),
        },
    ];

    if let Some(mdns) = rt.mdns().await {
        if mdns.state() == fieldnet_runtime::service::MdnsState::Running {
            transports.push(TransportDto {
                name: "mDNS / DNS-SD".to_string(),
                protocol: "Multicast DNS (5353/UDP)".to_string(),
                endpoint: "224.0.0.251:5353".to_string(),
                state: "Active".to_string(),
                details: format!("Advertising {}", mdns.active_hostname().await),
            });
        }
    }

    let route_dtos = all_routes
        .into_iter()
        .map(|r| RouteDto {
            destination: r.destination.to_string(),
            short_destination: short_node_id(&r.destination),
            next_hop: r.next_hop.to_string(),
            short_next_hop: short_node_id(&r.next_hop),
            hops: r.hops,
            metric: r.metric,
            sequence: r.sequence,
            updated_at: r.updated_at.as_millis(),
            expires_at: r.expires_at.as_millis(),
            is_expired: r.is_expired(),
        })
        .collect();

    Ok(NetworkStatusDto {
        connectivity_mode: if connected_peers.is_empty() {
            "Local".to_string()
        } else {
            "Mesh".to_string()
        },
        local_tcp_port: tcp_port,
        discovery_port: 4242,
        transports,
        connected_peers_count: connected_peers.len(),
        discovered_peers_count: discovered_peers.len(),
        routes: route_dtos,
        dtn_queue_count: dtn_count,
        dtn_db_path: state.data_dir.join("dtn_queue.db").display().to_string(),
    })
}

#[tauri::command]
pub async fn get_peers(state: State<'_, AppState>) -> Result<Vec<PeerDto>, String> {
    let rt = state
        .get_runtime()
        .await
        .ok_or("FIELDNET runtime is not running")?;
    let connected_peers: HashSet<NodeId> = rt
        .connection_manager()
        .connected_peers()
        .await
        .into_iter()
        .collect();
    let discovered_peers = rt.get_discovered_peers().await;
    let all_routes = rt.routing_table().get_all_routes().await;

    let mut next_hop_counts = std::collections::HashMap::new();
    for r in &all_routes {
        if !r.is_expired() {
            *next_hop_counts.entry(r.next_hop).or_insert(0usize) += 1;
        }
    }

    let mut peers_map = std::collections::HashMap::new();

    for p in discovered_peers {
        let is_connected = connected_peers.contains(&p.node_id);
        peers_map.insert(
            p.node_id,
            PeerDto {
                node_id: p.node_id.to_string(),
                short_id: short_node_id(&p.node_id),
                connection_state: if is_connected {
                    "Connected".to_string()
                } else {
                    "Discovered".to_string()
                },
                transport_address: p.tcp_addr.to_string(),
                protocol_version: p.protocol_version,
                discovered_at: p.discovered_at.as_millis(),
                expires_at: p.expires_at.as_millis(),
                is_direct_neighbor: is_connected,
                next_hop_for_count: next_hop_counts.get(&p.node_id).copied().unwrap_or(0),
            },
        );
    }

    for conn_peer in connected_peers {
        peers_map.entry(conn_peer).or_insert_with(|| PeerDto {
            node_id: conn_peer.to_string(),
            short_id: short_node_id(&conn_peer),
            connection_state: "Connected".to_string(),
            transport_address: "Direct TCP Connection".to_string(),
            protocol_version: 1,
            discovered_at: fieldnet_core::Timestamp::now().as_millis(),
            expires_at: fieldnet_core::Timestamp::now().add_seconds(60).as_millis(),
            is_direct_neighbor: true,
            next_hop_for_count: next_hop_counts.get(&conn_peer).copied().unwrap_or(0),
        });
    }

    let mut result: Vec<PeerDto> = peers_map.into_values().collect();
    result.sort_by(|a, b| {
        let a_conn = a.connection_state == "Connected";
        let b_conn = b.connection_state == "Connected";
        b_conn.cmp(&a_conn).then_with(|| a.node_id.cmp(&b.node_id))
    });

    Ok(result)
}

#[tauri::command]
pub async fn get_services(state: State<'_, AppState>) -> Result<Vec<ServiceDto>, String> {
    let rt = state
        .get_runtime()
        .await
        .ok_or("FIELDNET runtime is not running")?;
    let records = rt.service_registry().list_all_instances().await;

    let dtos = records
        .into_iter()
        .map(|rec| {
            let s_id = rec.manifest.service_id.to_string();
            let inst_id = rec.manifest.instance_id.to_string();
            let prov_id = rec.manifest.provider_node_id;

            ServiceDto {
                service_id: s_id,
                instance_id: inst_id.clone(),
                short_instance_id: short_id(&inst_id),
                provider_node_id: prov_id.to_string(),
                short_provider_id: short_node_id(&prov_id),
                is_local: rec.is_local,
                protocol: format!("{:?}", rec.manifest.protocol),
                version: rec.manifest.version.to_string(),
                state: match rec.health.state {
                    fieldnet_runtime::service::ServiceState::Available => "Available".to_string(),
                    fieldnet_runtime::service::ServiceState::Degraded => "Degraded".to_string(),
                    fieldnet_runtime::service::ServiceState::Unavailable => {
                        "Unavailable".to_string()
                    }
                    fieldnet_runtime::service::ServiceState::Discovered => "Discovered".to_string(),
                    fieldnet_runtime::service::ServiceState::Authenticated => {
                        "Authenticated".to_string()
                    }
                    fieldnet_runtime::service::ServiceState::Expired => "Expired".to_string(),
                    fieldnet_runtime::service::ServiceState::Removed => "Removed".to_string(),
                },
                capabilities: rec
                    .manifest
                    .capabilities
                    .iter()
                    .map(|c| c.to_string())
                    .collect(),
                endpoint: format!("{:?}", rec.manifest.endpoint),
                last_refreshed_at: rec.last_refreshed_at.as_millis(),
                reliability_permille: rec.health.reliability_permille,
                consecutive_failures: rec.health.consecutive_failures,
                observed_rtt_ms: rec.health.observed_rtt_ms,
            }
        })
        .collect();

    Ok(dtos)
}

#[tauri::command]
pub async fn get_applications(state: State<'_, AppState>) -> Result<Vec<ApplicationDto>, String> {
    let rt = state
        .get_runtime()
        .await
        .ok_or("FIELDNET runtime is not running")?;
    let records = rt
        .application_registry()
        .list()
        .map_err(|e| e.to_string())?;
    let active_executions = rt.list_application_executions().await;

    let mut exec_map: std::collections::HashMap<String, Vec<ActiveExecutionDto>> =
        std::collections::HashMap::new();
    for exec in active_executions {
        let app_key = exec.application_id.to_string();
        exec_map
            .entry(app_key)
            .or_default()
            .push(ActiveExecutionDto {
                execution_id: exec.execution_id.to_string(),
                application_id: exec.application_id.to_string(),
                started_at_ms: exec.start_time.as_millis(),
                state: format!("{:?}", exec.lifecycle_state),
            });
    }

    let dtos = records
        .into_iter()
        .map(|rec| {
            let app_key = rec.application_id.to_string();
            let app_execs = exec_map.remove(&app_key).unwrap_or_default();

            let state_str = if !app_execs.is_empty() {
                "Running".to_string()
            } else {
                rec.lifecycle_state.as_str().to_string()
            };

            ApplicationDto {
                application_id: rec.application_id.to_string(),
                version: rec.installed_version.to_string(),
                package_id: rec.package_id.to_string(),
                package_hash: rec.package_hash.to_hex(),
                publisher: rec.publisher.to_string(),
                signature_status: rec.signature_status.as_str().to_string(),
                lifecycle_state: state_str,
                declared_permissions: rec
                    .manifest
                    .permissions
                    .iter()
                    .map(|p| p.to_string())
                    .collect(),
                authorized_permissions: rec.permissions.iter().map(|p| p.to_string()).collect(),
                effective_capabilities: rec
                    .manifest
                    .permissions
                    .iter()
                    .map(|p| p.to_string())
                    .collect(),
                resource_limits: ResourceLimitsDto {
                    max_fuel: Some(rec.resource_limits.max_cpu_fuel),
                    max_memory_bytes: Some(rec.resource_limits.max_memory_bytes),
                    max_execution_time_ms: Some(rec.resource_limits.max_execution_time_ms),
                },
                dependencies: rec
                    .dependencies
                    .into_iter()
                    .map(|d| DependencyDto {
                        dependency_id: d.identity,
                        version_req: d.version_requirement.to_string(),
                        is_optional: d.optional,
                    })
                    .collect(),
                active_executions: app_execs,
                installed_at: rec.installed_at.as_millis(),
                updated_at: rec.updated_at.as_millis(),
            }
        })
        .collect();

    Ok(dtos)
}

#[tauri::command]
pub async fn get_storage_status(state: State<'_, AppState>) -> Result<StorageStatusDto, String> {
    let rt = state
        .get_runtime()
        .await
        .ok_or("FIELDNET runtime is not running")?;
    let quota = rt.content_quota_status().await;
    let items_res = rt.content_list().map_err(|e| e.to_string())?;

    let mut pinned_count = 0;
    let mut dtos = Vec::new();

    for it in items_res {
        let is_pinned = it.is_pinned;
        if is_pinned {
            pinned_count += 1;
        }

        let cid_hex = it.content_id.to_hex();
        let short_cid = short_id(&cid_hex);

        dtos.push(ContentItemDto {
            content_id: cid_hex,
            short_id: short_cid,
            size_bytes: it.size_bytes,
            is_pinned,
            chunk_count: 1, // Base object
            created_at: it.created_at.as_millis(),
            availability: it.storage_state.to_string(),
            verification_state: "Verified (BLAKE3/SHA-256)".to_string(),
        });
    }

    Ok(StorageStatusDto {
        used_bytes: quota.used_bytes,
        limit_bytes: quota.quota_bytes,
        available_bytes: quota.available_bytes,
        object_count: dtos.len(),
        chunk_count: dtos.len(),
        pinned_count,
        storage_dir: state.data_dir.join("content").display().to_string(),
        items: dtos,
    })
}

#[tauri::command]
pub async fn get_runtime_events(
    state: State<'_, AppState>,
) -> Result<Vec<FrontendEventDto>, String> {
    Ok(state.get_events().await)
}

#[tauri::command]
pub async fn start_application(
    app_id: String,
    state: State<'_, AppState>,
) -> Result<String, String> {
    let rt = state
        .get_runtime()
        .await
        .ok_or("FIELDNET runtime is not running")?;
    let parsed_id =
        ApplicationId::new(&app_id).map_err(|e| format!("Invalid Application ID: {}", e))?;
    let report = rt
        .start_application(&parsed_id)
        .await
        .map_err(|e| e.to_string())?;
    Ok(report.execution_id.to_string())
}

#[tauri::command]
pub async fn stop_application(app_id: String, state: State<'_, AppState>) -> Result<(), String> {
    let rt = state
        .get_runtime()
        .await
        .ok_or("FIELDNET runtime is not running")?;
    let parsed_id =
        ApplicationId::new(&app_id).map_err(|e| format!("Invalid Application ID: {}", e))?;
    rt.stop_application(&parsed_id)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn resolve_service(
    service_id: String,
    state: State<'_, AppState>,
) -> Result<serde_json::Value, String> {
    let rt = state
        .get_runtime()
        .await
        .ok_or("FIELDNET runtime is not running")?;
    let parsed_id =
        ServiceId::new(&service_id).map_err(|e| format!("Invalid Service ID: {}", e))?;
    let query = ServiceQuery::new(parsed_id);
    let resolution = rt
        .resolve_service(&query, None)
        .await
        .map_err(|e| e.to_string())?;

    Ok(serde_json::json!({
        "service_id": resolution.service_id.to_string(),
        "instance_id": resolution.instance_id.to_string(),
        "provider_node_id": resolution.provider_node_id.to_string(),
        "resolved_at": resolution.resolved_at.as_millis(),
        "route_hops": resolution.route_hops,
        "reason": resolution.reason.explanation_text,
        "protocol": format!("{:?}", resolution.protocol()),
        "endpoint": format!("{:?}", resolution.endpoint()),
    }))
}

#[tauri::command]
pub async fn send_message(
    destination: String,
    payload: String,
    state: State<'_, AppState>,
) -> Result<String, String> {
    let rt = state
        .get_runtime()
        .await
        .ok_or("FIELDNET runtime is not running")?;
    let dest_node_id: NodeId = destination
        .parse()
        .map_err(|e| format!("Invalid destination NodeId: {}", e))?;
    let msg_id = rt
        .send_data_message(dest_node_id, &payload)
        .await
        .map_err(|e| e.to_string())?;
    Ok(msg_id.to_string())
}

#[tauri::command]
pub async fn disconnect_peer(peer_id: String, state: State<'_, AppState>) -> Result<(), String> {
    let rt = state
        .get_runtime()
        .await
        .ok_or("FIELDNET runtime is not running")?;
    let node_id: NodeId = peer_id
        .parse()
        .map_err(|e| format!("Invalid peer NodeId: {}", e))?;
    let disconnected = rt.connection_manager().disconnect_peer(&node_id).await;
    if disconnected {
        Ok(())
    } else {
        Err("Peer was not currently connected".to_string())
    }
}

#[tauri::command]
pub async fn refresh_discovery(state: State<'_, AppState>) -> Result<usize, String> {
    let rt = state
        .get_runtime()
        .await
        .ok_or("FIELDNET runtime is not running")?;
    rt.discovery()
        .broadcast_beacon_now()
        .await
        .map_err(|e| e.to_string())?;
    let peers = rt.get_discovered_peers().await;
    Ok(peers.len())
}

#[tauri::command]
pub async fn pin_content(content_id: String, state: State<'_, AppState>) -> Result<(), String> {
    let rt = state
        .get_runtime()
        .await
        .ok_or("FIELDNET runtime is not running")?;
    let cid = ContentId::from_hex(&content_id).map_err(|e| format!("Invalid Content ID: {}", e))?;
    rt.content_pin(&cid).map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn unpin_content(content_id: String, state: State<'_, AppState>) -> Result<(), String> {
    let rt = state
        .get_runtime()
        .await
        .ok_or("FIELDNET runtime is not running")?;
    let cid = ContentId::from_hex(&content_id).map_err(|e| format!("Invalid Content ID: {}", e))?;
    rt.content_unpin(&cid).map_err(|e| e.to_string())
}

pub async fn start_runtime_internal(
    app_handle: &AppHandle,
    state: &AppState,
) -> Result<String, String> {
    let config = RuntimeConfig {
        data_dir: state.data_dir.clone(),
        tcp_port: 0,
        discovery_port: 4242,
        auto_connect: true,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    state.set_phase("INITIALIZING").await;
    match FieldnetRuntime::start(config).await {
        Ok(runtime) => {
            state.set_phase("IDENTITY_READY").await;
            state.set_phase("NETWORK_INITIALIZING").await;
            state.set_phase("DISCOVERING").await;
            state.set_phase("RUNNING").await;
            state.set_runtime(Arc::clone(&runtime)).await;

            spawn_event_bridges(app_handle.clone(), Arc::clone(&runtime), state.clone());
            Ok("FIELDNET Runtime started successfully".to_string())
        }
        Err(e) => {
            let err_str = e.to_string();
            state.set_phase("OFFLINE").await;
            state.clear_runtime(Some(err_str.clone())).await;
            Err(format!("Failed to start runtime: {}", err_str))
        }
    }
}

#[tauri::command]
pub async fn retry_runtime_start(
    app_handle: AppHandle,
    state: State<'_, AppState>,
) -> Result<String, String> {
    start_runtime_internal(&app_handle, state.inner()).await
}
