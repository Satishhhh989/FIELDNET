use fieldnet_core::NodeId;
use fieldnet_network::ConnectionEvent;
use fieldnet_runtime::content::ContentEvent;
use fieldnet_runtime::service::ServiceEvent;
use fieldnet_runtime::wasm::ApplicationEvent;
use fieldnet_runtime::FieldnetRuntime;
use std::sync::Arc;
use tauri::Emitter;

use crate::dto::FrontendEventDto;
use crate::state::AppState;

fn now_iso() -> String {
    let now = std::time::SystemTime::now();
    let duration = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();
    let millis = duration.subsec_millis();
    let hours = (secs / 3600) % 24;
    let mins = (secs / 60) % 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}.{:03}", hours, mins, s, millis)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn next_id() -> String {
    format!("evt_{}", now_ms())
}

fn short_node_id(id: &NodeId) -> String {
    let s = id.to_string();
    if s.len() >= 8 {
        s[..8].to_string()
    } else {
        s
    }
}

pub fn spawn_event_bridges(
    app_handle: tauri::AppHandle,
    runtime: Arc<FieldnetRuntime>,
    state: AppState,
) {
    // 1. Connection Manager Events
    {
        let mut rx = runtime.connection_manager().subscribe_events();
        let handle = app_handle.clone();
        let st = state.clone();
        tokio::spawn(async move {
            while let Ok(event) = rx.recv().await {
                let dto = match event {
                    ConnectionEvent::PeerConnected {
                        peer_id,
                        session_id,
                    } => FrontendEventDto {
                        id: next_id(),
                        event_type: "peer.connected".to_string(),
                        category: "peer".to_string(),
                        timestamp: now_iso(),
                        timestamp_ms: now_ms(),
                        node_id: Some(peer_id.to_string()),
                        summary: format!("Peer connected: {}", short_node_id(&peer_id)),
                        details: serde_json::json!({
                            "peer_id": peer_id.to_string(),
                            "session_id": session_id,
                        }),
                    },
                    ConnectionEvent::PeerDisconnected { peer_id } => FrontendEventDto {
                        id: next_id(),
                        event_type: "peer.disconnected".to_string(),
                        category: "peer".to_string(),
                        timestamp: now_iso(),
                        timestamp_ms: now_ms(),
                        node_id: Some(peer_id.to_string()),
                        summary: format!("Peer disconnected: {}", short_node_id(&peer_id)),
                        details: serde_json::json!({
                            "peer_id": peer_id.to_string(),
                        }),
                    },
                    ConnectionEvent::StateChanged { peer_id, state } => FrontendEventDto {
                        id: next_id(),
                        event_type: "network.state_changed".to_string(),
                        category: "network".to_string(),
                        timestamp: now_iso(),
                        timestamp_ms: now_ms(),
                        node_id: Some(peer_id.to_string()),
                        summary: format!("Peer {} state: {:?}", short_node_id(&peer_id), state),
                        details: serde_json::json!({
                            "peer_id": peer_id.to_string(),
                            "state": format!("{:?}", state),
                        }),
                    },
                };
                st.push_event(dto.clone()).await;
                let _ = handle.emit("runtime-event", dto);
            }
        });
    }

    // 2. Peer Discovery Events
    {
        let mut rx = runtime.discovery().subscribe();
        let handle = app_handle.clone();
        let st = state.clone();
        tokio::spawn(async move {
            while let Ok(peer) = rx.recv().await {
                let dto = FrontendEventDto {
                    id: next_id(),
                    event_type: "peer.discovered".to_string(),
                    category: "peer".to_string(),
                    timestamp: now_iso(),
                    timestamp_ms: now_ms(),
                    node_id: Some(peer.node_id.to_string()),
                    summary: format!(
                        "Peer discovered on LAN: {} ({})",
                        short_node_id(&peer.node_id),
                        peer.tcp_addr
                    ),
                    details: serde_json::json!({
                        "node_id": peer.node_id.to_string(),
                        "tcp_addr": peer.tcp_addr.to_string(),
                        "protocol_version": peer.protocol_version,
                    }),
                };
                st.push_event(dto.clone()).await;
                let _ = handle.emit("runtime-event", dto);
            }
        });
    }

    // 3. Service Registry Events
    {
        let mut rx = runtime.subscribe_service_events();
        let handle = app_handle.clone();
        let st = state.clone();
        tokio::spawn(async move {
            while let Ok(event) = rx.recv().await {
                let (event_type, summary, node_id, details) = match event {
                    ServiceEvent::Registered {
                        service_id,
                        instance_id,
                        provider_node_id,
                        is_local,
                    } => (
                        "service.registered",
                        format!(
                            "Service registered: {} ({})",
                            service_id,
                            if is_local { "local" } else { "remote" }
                        ),
                        Some(provider_node_id.to_string()),
                        serde_json::json!({
                            "service_id": service_id.to_string(),
                            "instance_id": instance_id.to_string(),
                            "provider": provider_node_id.to_string(),
                            "is_local": is_local,
                        }),
                    ),
                    ServiceEvent::Available {
                        service_id,
                        instance_id,
                        provider_node_id,
                    } => (
                        "service.available",
                        format!("Service available: {}", service_id),
                        Some(provider_node_id.to_string()),
                        serde_json::json!({
                            "service_id": service_id.to_string(),
                            "instance_id": instance_id.to_string(),
                            "provider": provider_node_id.to_string(),
                        }),
                    ),
                    ServiceEvent::Unavailable {
                        service_id,
                        instance_id,
                        provider_node_id,
                        reason,
                    } => (
                        "service.unavailable",
                        format!("Service unavailable: {} ({})", service_id, reason),
                        Some(provider_node_id.to_string()),
                        serde_json::json!({
                            "service_id": service_id.to_string(),
                            "instance_id": instance_id.to_string(),
                            "provider": provider_node_id.to_string(),
                            "reason": reason,
                        }),
                    ),
                    ServiceEvent::Resolved {
                        service_id,
                        instance_id,
                        provider_node_id,
                        ..
                    } => (
                        "service.resolved",
                        format!("Service query resolved: {}", service_id),
                        Some(provider_node_id.to_string()),
                        serde_json::json!({
                            "service_id": service_id.to_string(),
                            "instance_id": instance_id.to_string(),
                            "provider": provider_node_id.to_string(),
                        }),
                    ),
                    ServiceEvent::Withdrawn {
                        service_id,
                        instance_id,
                        provider_node_id,
                    } => (
                        "service.withdrawn",
                        format!("Service withdrawn: {}", service_id),
                        Some(provider_node_id.to_string()),
                        serde_json::json!({
                            "service_id": service_id.to_string(),
                            "instance_id": instance_id.to_string(),
                            "provider": provider_node_id.to_string(),
                        }),
                    ),
                    other => (
                        "service.event",
                        format!("Service event: {:?}", other),
                        None,
                        serde_json::json!({ "raw": format!("{:?}", other) }),
                    ),
                };

                let dto = FrontendEventDto {
                    id: next_id(),
                    event_type: event_type.to_string(),
                    category: "service".to_string(),
                    timestamp: now_iso(),
                    timestamp_ms: now_ms(),
                    node_id,
                    summary,
                    details,
                };
                st.push_event(dto.clone()).await;
                let _ = handle.emit("runtime-event", dto);
            }
        });
    }

    // 4. Application Lifecycle Events
    {
        let mut rx = runtime.subscribe_application_events();
        let handle = app_handle.clone();
        let st = state.clone();
        tokio::spawn(async move {
            while let Ok(event) = rx.recv().await {
                let (event_type, summary, details) = match event {
                    ApplicationEvent::Installed {
                        application_id,
                        version,
                        package_id,
                    } => (
                        "app.installed",
                        format!("App installed: {} v{}", application_id, version),
                        serde_json::json!({
                            "app_id": application_id.to_string(),
                            "version": version,
                            "package_id": package_id.to_string(),
                        }),
                    ),
                    ApplicationEvent::Authorized {
                        application_id,
                        granted_permissions,
                    } => (
                        "app.authorized",
                        format!(
                            "App authorized: {} ({} permissions)",
                            application_id,
                            granted_permissions.len()
                        ),
                        serde_json::json!({
                            "app_id": application_id.to_string(),
                            "permissions": granted_permissions.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
                        }),
                    ),
                    ApplicationEvent::Starting {
                        application_id,
                        execution_id,
                        ..
                    } => (
                        "app.starting",
                        format!("App initializing: {} ({})", application_id, execution_id),
                        serde_json::json!({
                            "app_id": application_id.to_string(),
                            "execution_id": execution_id.to_string(),
                        }),
                    ),
                    ApplicationEvent::Started {
                        application_id,
                        execution_id,
                        ..
                    } => (
                        "app.started",
                        format!("App running: {} ({})", application_id, execution_id),
                        serde_json::json!({
                            "app_id": application_id.to_string(),
                            "execution_id": execution_id.to_string(),
                        }),
                    ),
                    ApplicationEvent::Stopped {
                        application_id,
                        execution_id,
                        reason,
                        ..
                    } => (
                        "app.stopped",
                        format!(
                            "App stopped: {} ({})",
                            application_id,
                            reason.as_deref().unwrap_or("clean shutdown")
                        ),
                        serde_json::json!({
                            "app_id": application_id.to_string(),
                            "execution_id": execution_id.map(|e| e.to_string()),
                            "reason": reason,
                        }),
                    ),
                    ApplicationEvent::Crashed {
                        application_id,
                        execution_id,
                        error,
                        ..
                    } => (
                        "app.crashed",
                        format!("App crashed: {} ({})", application_id, error),
                        serde_json::json!({
                            "app_id": application_id.to_string(),
                            "execution_id": execution_id.map(|e| e.to_string()),
                            "error": error,
                        }),
                    ),
                    ApplicationEvent::Isolated {
                        application_id,
                        reason,
                        ..
                    } => (
                        "app.isolated",
                        format!("App isolated: {} ({})", application_id, reason),
                        serde_json::json!({
                            "app_id": application_id.to_string(),
                            "reason": reason,
                        }),
                    ),
                    other => (
                        "app.event",
                        format!("App event: {:?}", other),
                        serde_json::json!({ "raw": format!("{:?}", other) }),
                    ),
                };

                let dto = FrontendEventDto {
                    id: next_id(),
                    event_type: event_type.to_string(),
                    category: "app".to_string(),
                    timestamp: now_iso(),
                    timestamp_ms: now_ms(),
                    node_id: None,
                    summary,
                    details,
                };
                st.push_event(dto.clone()).await;
                let _ = handle.emit("runtime-event", dto);
            }
        });
    }

    // 5. Content Subsystem Events
    {
        let mut rx = runtime.content_store().event_tx().subscribe();
        let handle = app_handle.clone();
        let st = state.clone();
        tokio::spawn(async move {
            while let Ok(event) = rx.recv().await {
                let (event_type, summary, details) = match event {
                    ContentEvent::Created { id, size_bytes } => {
                        let hex = id.to_hex();
                        let short = if hex.len() >= 8 { &hex[..8] } else { &hex };
                        (
                            "content.created",
                            format!("Content stored: {} ({} bytes)", short, size_bytes),
                            serde_json::json!({
                                "content_id": hex,
                                "size_bytes": size_bytes,
                            }),
                        )
                    }
                    ContentEvent::Available { id, size_bytes } => {
                        let hex = id.to_hex();
                        let short = if hex.len() >= 8 { &hex[..8] } else { &hex };
                        (
                            "content.available",
                            format!("Content verified: {} ({} bytes)", short, size_bytes),
                            serde_json::json!({
                                "content_id": hex,
                                "size_bytes": size_bytes,
                            }),
                        )
                    }
                    ContentEvent::Pinned { id } => {
                        let hex = id.to_hex();
                        let short = if hex.len() >= 8 { &hex[..8] } else { &hex };
                        (
                            "content.pinned",
                            format!("Content pinned: {}", short),
                            serde_json::json!({
                                "content_id": hex,
                            }),
                        )
                    }
                    ContentEvent::Unpinned { id } => {
                        let hex = id.to_hex();
                        let short = if hex.len() >= 8 { &hex[..8] } else { &hex };
                        (
                            "content.unpinned",
                            format!("Content unpinned: {}", short),
                            serde_json::json!({
                                "content_id": hex,
                            }),
                        )
                    }
                    ContentEvent::Deleted { id } => {
                        let hex = id.to_hex();
                        let short = if hex.len() >= 8 { &hex[..8] } else { &hex };
                        (
                            "content.deleted",
                            format!("Content deleted: {}", short),
                            serde_json::json!({
                                "content_id": hex,
                            }),
                        )
                    }
                    other => (
                        "content.event",
                        format!("Content event: {:?}", other),
                        serde_json::json!({ "raw": format!("{:?}", other) }),
                    ),
                };

                let dto = FrontendEventDto {
                    id: next_id(),
                    event_type: event_type.to_string(),
                    category: "content".to_string(),
                    timestamp: now_iso(),
                    timestamp_ms: now_ms(),
                    node_id: None,
                    summary,
                    details,
                };
                st.push_event(dto.clone()).await;
                let _ = handle.emit("runtime-event", dto);
            }
        });
    }
}
