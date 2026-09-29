use fieldnet_core::NodeId;
use socket2::{Domain, Protocol, Socket, Type};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, info, warn};

use crate::events::ServiceEvent;
use crate::mdns::config::MdnsConfig;
use crate::mdns::conflict::{deterministic_fallback_hostname, normalize_hostname};
use crate::mdns::error::MdnsError;
use crate::mdns::interface::get_local_interfaces;
use crate::mdns::packet::{
    build_announcement_packet, build_goodbye_packet, build_probe_packet, build_query_packet,
    build_response_packet, extract_records_from_packet, parse_packet, MdnsRecord,
    ServiceAdvertisementInfo,
};
use crate::mdns::state::{MdnsState, MdnsStateMachine};

/// Core mDNS and DNS-SD daemon managing local network name resolution and service advertisement.
#[derive(Debug)]
pub struct MdnsService {
    local_node_id: NodeId,
    config: MdnsConfig,
    effective_service_name: String,
    state: Arc<MdnsStateMachine>,
    active_hostname: Arc<RwLock<String>>,
    event_tx: Option<broadcast::Sender<ServiceEvent>>,
    shutdown_tx: broadcast::Sender<()>,
    packet_tx: broadcast::Sender<Arc<Vec<u8>>>,
    socket: Arc<RwLock<Option<Arc<UdpSocket>>>>,
    bound_port: Arc<RwLock<u16>>,
    last_error: Arc<RwLock<Option<String>>>,
}

impl MdnsService {
    /// Creates a new `MdnsService` instance.
    pub fn new(
        local_node_id: NodeId,
        config: MdnsConfig,
        event_tx: Option<broadcast::Sender<ServiceEvent>>,
    ) -> Self {
        let (shutdown_tx, _) = broadcast::channel(4);
        let (packet_tx, _) = broadcast::channel(256);
        let initial_hostname = config.hostname.clone();

        let effective_service_name =
            if config.service_name == crate::mdns::config::DEFAULT_SERVICE_NAME {
                let short_id = local_node_id.to_string();
                format!(
                    "{} ({})",
                    crate::mdns::config::DEFAULT_SERVICE_NAME,
                    &short_id[..8.min(short_id.len())]
                )
            } else {
                config.service_name.clone()
            };

        Self {
            local_node_id,
            config,
            effective_service_name,
            state: Arc::new(MdnsStateMachine::new(MdnsState::Stopped)),
            active_hostname: Arc::new(RwLock::new(initial_hostname)),
            event_tx,
            shutdown_tx,
            packet_tx,
            socket: Arc::new(RwLock::new(None)),
            bound_port: Arc::new(RwLock::new(0)),
            last_error: Arc::new(RwLock::new(None)),
        }
    }

    /// Returns current lifecycle state of mDNS subsystem.
    pub fn state(&self) -> MdnsState {
        self.state.current()
    }

    /// Returns the currently active hostname (e.g. "fieldnet.local" or fallback "fieldnet-abc12345.local").
    pub async fn active_hostname(&self) -> String {
        self.active_hostname.read().await.clone()
    }

    /// Returns the advertised gateway TCP port.
    pub fn advertised_port(&self) -> u16 {
        self.config.gateway_port
    }

    /// Returns the standard DNS-SD service type.
    pub fn service_type(&self) -> &str {
        &self.config.service_type
    }

    /// Returns the human-readable service name.
    pub fn service_name(&self) -> &str {
        &self.effective_service_name
    }

    /// Returns the last encountered error, if any.
    pub async fn last_error(&self) -> Option<String> {
        self.last_error.read().await.clone()
    }

    /// Returns query and response metrics.
    pub fn metrics(&self) -> (usize, usize, usize, usize) {
        (
            self.state.queries_count(),
            self.state.responses_count(),
            self.state.errors_count(),
            self.state.active_announcements_count(),
        )
    }

    /// Starts the mDNS daemon: binds socket, probes for conflicts, starts responder and advertiser.
    pub async fn start(&self) -> Result<(), MdnsError> {
        if !self.config.enabled {
            let _ = self.state.transition(MdnsState::Disabled);
            debug!("mDNS service is disabled in configuration");
            return Ok(());
        }

        if self.state.is_running() {
            return Err(MdnsError::AlreadyRunning);
        }

        let _ = self.state.transition(MdnsState::Starting);

        // Normalize requested hostname
        let requested_hostname = match normalize_hostname(&self.config.hostname) {
            Ok(h) => h,
            Err(e) => {
                let err_msg = format!(
                    "Invalid requested hostname '{}': {}",
                    self.config.hostname, e
                );
                *self.last_error.write().await = Some(err_msg.clone());
                let _ = self.state.transition(MdnsState::Degraded);
                self.emit_event(ServiceEvent::MdnsError { message: err_msg });
                return Err(e);
            }
        };

        // 1. Setup UDP socket with SO_REUSEADDR and SO_REUSEPORT (via socket2)
        let mdns_port = self.config.mdns_port;
        let bind_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, mdns_port);

        let socket_res = (|| -> Result<UdpSocket, MdnsError> {
            let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))
                .map_err(|e| MdnsError::SocketBind(e.to_string()))?;

            sock.set_reuse_address(true)
                .map_err(|e| MdnsError::SocketBind(e.to_string()))?;

            #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
            {
                let _ = sock.set_reuse_port(true);
            }

            let _ = sock.set_multicast_loop_v4(true);
            sock.set_broadcast(true)
                .map_err(|e| MdnsError::SocketBind(e.to_string()))?;
            sock.set_nonblocking(true)
                .map_err(|e| MdnsError::SocketBind(e.to_string()))?;

            sock.bind(&bind_addr.into())
                .map_err(|e| MdnsError::SocketBind(e.to_string()))?;

            let std_sock: std::net::UdpSocket = sock.into();
            let tokio_socket =
                UdpSocket::from_std(std_sock).map_err(|e| MdnsError::SocketBind(e.to_string()))?;
            let _ = tokio_socket.set_multicast_loop_v4(true);
            Ok(tokio_socket)
        })();

        let tokio_socket = match socket_res {
            Ok(s) => s,
            Err(e) => {
                let err_msg = format!(
                    "Failed to bind mDNS UDP socket on port {}: {}",
                    mdns_port, e
                );
                warn!("{}", err_msg);
                *self.last_error.write().await = Some(err_msg.clone());
                let _ = self.state.transition(MdnsState::Degraded);
                self.emit_event(ServiceEvent::MdnsError { message: err_msg });
                // Resilience invariant: return Ok so FIELDNET Runtime & Gateway continue
                return Ok(());
            }
        };

        // 2. Join Multicast Group 224.0.0.251
        let mut degraded = false;
        if let Err(e) =
            tokio_socket.join_multicast_v4(self.config.multicast_ipv4, Ipv4Addr::UNSPECIFIED)
        {
            let warn_msg = format!(
                "Could not join mDNS multicast group {}: {}. Will use broadcast/direct.",
                self.config.multicast_ipv4, e
            );
            debug!("{}", warn_msg);
            *self.last_error.write().await = Some(warn_msg.clone());
            degraded = true;
            self.emit_event(ServiceEvent::MdnsError { message: warn_msg });
        }

        let actual_port = tokio_socket
            .local_addr()
            .map(|a| a.port())
            .unwrap_or(mdns_port);
        *self.bound_port.write().await = actual_port;
        let arc_socket = Arc::new(tokio_socket);
        *self.socket.write().await = Some(arc_socket.clone());

        // 3. Build Safe Public TXT Record Attributes
        let mut txt_attrs = HashMap::new();
        txt_attrs.insert("fieldnet_version".to_string(), "0.1.0".to_string());
        txt_attrs.insert("protocol_version".to_string(), "1".to_string());
        txt_attrs.insert("gateway_version".to_string(), "0.1.0".to_string());
        let short_id = self.local_node_id.to_string();
        txt_attrs.insert(
            "node_id_short".to_string(),
            short_id[..8.min(short_id.len())].to_string(),
        );

        let multicast_dest =
            SocketAddr::V4(SocketAddrV4::new(self.config.multicast_ipv4, mdns_port));
        let localhost_dest = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, mdns_port));
        let broadcast_dest = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::BROADCAST, mdns_port));

        // 4. Spawn Sole Inbound UDP Packet Receiver and Query Responder Task
        let arc_socket_listen = arc_socket.clone();
        let state_listen = self.state.clone();
        let mut shutdown_rx_listen = self.shutdown_tx.subscribe();
        let active_hostname_listen = self.active_hostname.clone();
        let txt_attrs_listen = txt_attrs.clone();
        let service_name_listen = self.effective_service_name.clone();
        let service_type_listen = self.config.service_type.clone();
        let gateway_port = self.config.gateway_port;
        let allow_loopback = self.config.allow_loopback;
        let ttl_announce = self.config.ttl_seconds;
        let packet_tx = self.packet_tx.clone();

        tokio::spawn(async move {
            let mut buf = [0u8; 8192];
            loop {
                tokio::select! {
                    _ = shutdown_rx_listen.recv() => {
                        debug!("mDNS receiver listener shutting down");
                        break;
                    }
                    recv_res = arc_socket_listen.recv_from(&mut buf) => {
                        match recv_res {
                            Ok((len, from)) => {
                                let pkt_bytes = Arc::new(buf[..len].to_vec());
                                let _ = packet_tx.send(pkt_bytes);

                                // Only respond to queries when the node is in Running state
                                if !state_listen.is_running() {
                                    continue;
                                }

                                match parse_packet(&buf[..len]) {
                                    Ok(query_packet) => {
                                        if query_packet.questions.is_empty() {
                                            continue;
                                        }

                                        state_listen.increment_queries();
                                        let current_host = active_hostname_listen.read().await.clone();
                                        let ips = get_local_interfaces(allow_loopback);
                                        let info = ServiceAdvertisementInfo {
                                            service_name: &service_name_listen,
                                            service_type: &service_type_listen,
                                            hostname: &current_host,
                                            port: gateway_port,
                                            ips: &ips,
                                            txt_attrs: &txt_attrs_listen,
                                            ttl: ttl_announce,
                                        };
                                        match build_response_packet(&query_packet, &info) {
                                            Ok(Some(response_bytes)) => {
                                                let is_unicast = query_packet.questions.iter().any(|q| q.unicast_response)
                                                    || from.port() != mdns_port;

                                                if is_unicast {
                                                    let _ = arc_socket_listen.send_to(&response_bytes, from).await;
                                                } else {
                                                    let _ = arc_socket_listen.send_to(&response_bytes, multicast_dest).await;
                                                    let _ = arc_socket_listen.send_to(&response_bytes, localhost_dest).await;
                                                    let _ = arc_socket_listen.send_to(&response_bytes, broadcast_dest).await;
                                                    let _ = arc_socket_listen.send_to(&response_bytes, from).await;
                                                }
                                                state_listen.increment_responses();
                                            }
                                            Ok(None) => {}
                                            Err(e) => {
                                                state_listen.increment_errors();
                                                debug!("Error building mDNS response packet: {}", e);
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        state_listen.increment_errors();
                                        debug!("Received malformed/unparseable mDNS packet from {}: {}", from, e);
                                    }
                                }
                            }
                            Err(e) => {
                                state_listen.increment_errors();
                                warn!("mDNS socket recv error: {}", e);
                            }
                        }
                    }
                }
            }
        });

        // 5. Conflict Detection Probing
        let mut chosen_hostname = requested_hostname.clone();
        if self.config.probe_timeout > Duration::ZERO {
            if let Ok(probe_bytes) = build_probe_packet(&requested_hostname) {
                let mut probe_rx = self.packet_tx.subscribe();
                let _ = arc_socket.send_to(&probe_bytes, multicast_dest).await;
                let _ = arc_socket.send_to(&probe_bytes, localhost_dest).await;
                let _ = arc_socket.send_to(&probe_bytes, broadcast_dest).await;

                let probe_deadline = tokio::time::Instant::now() + self.config.probe_timeout;
                let mut conflict_found = false;

                while tokio::time::Instant::now() < probe_deadline {
                    let remaining =
                        probe_deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        break;
                    }

                    match tokio::time::timeout(remaining, probe_rx.recv()).await {
                        Ok(Ok(pkt_bytes)) => {
                            if let Ok(packet) = parse_packet(&pkt_bytes) {
                                let norm_target =
                                    requested_hostname.trim_end_matches('.').to_lowercase();
                                for answer in packet
                                    .answers
                                    .iter()
                                    .chain(packet.additional_records.iter())
                                {
                                    let ans_name = answer
                                        .name
                                        .to_string()
                                        .trim_end_matches('.')
                                        .to_lowercase();
                                    if ans_name == norm_target {
                                        conflict_found = true;
                                        break;
                                    }
                                }
                            }
                        }
                        Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
                        Ok(Err(broadcast::error::RecvError::Closed)) => break,
                        Err(_) => break,
                    }

                    if conflict_found {
                        break;
                    }
                }

                if conflict_found {
                    let fallback =
                        deterministic_fallback_hostname(&self.config.hostname, &self.local_node_id);
                    warn!(
                        "mDNS hostname conflict detected for '{}'. Assigning fallback '{}'",
                        requested_hostname, fallback
                    );
                    self.emit_event(ServiceEvent::MdnsHostnameConflict {
                        requested_hostname: requested_hostname.clone(),
                        assigned_hostname: fallback.clone(),
                    });
                    chosen_hostname = fallback;
                }
            }
        }

        *self.active_hostname.write().await = chosen_hostname.clone();

        // 6. Transition to Running (or Degraded if multicast failed)
        if degraded {
            let _ = self.state.transition(MdnsState::Degraded);
        } else {
            let _ = self.state.transition(MdnsState::Running);
        }

        self.emit_event(ServiceEvent::MdnsStarted {
            hostname: chosen_hostname.clone(),
            port: self.config.gateway_port,
        });
        self.emit_event(ServiceEvent::MdnsAdvertisementStarted {
            service_name: self.effective_service_name.clone(),
            hostname: chosen_hostname.clone(),
            port: self.config.gateway_port,
        });

        // 7. Send Initial Announcement
        self.send_announcement_packet(
            &arc_socket,
            &chosen_hostname,
            &txt_attrs,
            self.config.ttl_seconds,
        )
        .await;

        // 8. Spawn Background Periodic Announcement Task
        let arc_socket_announce = arc_socket.clone();
        let state_announce = self.state.clone();
        let mut shutdown_rx_announce = self.shutdown_tx.subscribe();
        let announce_interval = self.config.announce_interval;
        let chosen_host_announce = chosen_hostname.clone();
        let txt_attrs_announce = txt_attrs.clone();
        let ttl_announce_bg = self.config.ttl_seconds;
        let service_name_announce = self.effective_service_name.clone();
        let service_type_announce = self.config.service_type.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(announce_interval);
            ticker.tick().await; // Consume initial tick

            loop {
                tokio::select! {
                    _ = shutdown_rx_announce.recv() => {
                        debug!("mDNS periodic advertiser shutting down");
                        break;
                    }
                    _ = ticker.tick() => {
                        if !state_announce.is_running() {
                            break;
                        }

                        let ips = get_local_interfaces(allow_loopback);
                        let info = ServiceAdvertisementInfo {
                            service_name: &service_name_announce,
                            service_type: &service_type_announce,
                            hostname: &chosen_host_announce,
                            port: gateway_port,
                            ips: &ips,
                            txt_attrs: &txt_attrs_announce,
                            ttl: ttl_announce_bg,
                        };
                        if let Ok(bytes) = build_announcement_packet(&info) {
                            let _ = arc_socket_announce.send_to(&bytes, multicast_dest).await;
                            let _ = arc_socket_announce.send_to(&bytes, localhost_dest).await;
                            let _ = arc_socket_announce.send_to(&bytes, broadcast_dest).await;
                            state_announce.set_active_announcements(1);
                        }
                    }
                }
            }
        });

        info!(
            "mDNS daemon started successfully: hostname={}, service={}, gateway_port={}, mdns_port={}",
            self.active_hostname.read().await,
            self.config.service_type,
            self.config.gateway_port,
            mdns_port
        );

        Ok(())
    }

    /// Stops mDNS daemon: sends RFC 6762 Goodbye packets and terminates background tasks.
    pub async fn stop(&self) -> Result<(), MdnsError> {
        if !self.state.is_running() {
            return Ok(());
        }

        let _ = self.state.transition(MdnsState::Stopping);

        // Send Goodbye packet (TTL = 0) to flush network caches immediately
        if let Some(sock) = self.socket.read().await.as_ref() {
            let hostname = self.active_hostname.read().await.clone();
            let mut txt_attrs = HashMap::new();
            txt_attrs.insert("fieldnet_version".to_string(), "0.1.0".to_string());

            let ips = get_local_interfaces(self.config.allow_loopback);
            let info = ServiceAdvertisementInfo {
                service_name: &self.effective_service_name,
                service_type: &self.config.service_type,
                hostname: &hostname,
                port: self.config.gateway_port,
                ips: &ips,
                txt_attrs: &txt_attrs,
                ttl: 0,
            };
            if let Ok(goodbye_bytes) = build_goodbye_packet(&info) {
                let mdns_port = self.config.mdns_port;
                let multicast_dest =
                    SocketAddr::V4(SocketAddrV4::new(self.config.multicast_ipv4, mdns_port));
                let localhost_dest =
                    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, mdns_port));
                let broadcast_dest =
                    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::BROADCAST, mdns_port));

                let _ = sock.send_to(&goodbye_bytes, multicast_dest).await;
                let _ = sock.send_to(&goodbye_bytes, localhost_dest).await;
                let _ = sock.send_to(&goodbye_bytes, broadcast_dest).await;
            }
        }

        // Notify background tasks
        let _ = self.shutdown_tx.send(());

        let _ = self.state.transition(MdnsState::Stopped);
        self.state.set_active_announcements(0);

        self.emit_event(ServiceEvent::MdnsAdvertisementWithdrawn {
            service_name: self.effective_service_name.clone(),
        });
        self.emit_event(ServiceEvent::MdnsStopped);

        info!("mDNS daemon stopped successfully (goodbye packet sent)");
        Ok(())
    }

    /// Resolves an mDNS hostname (e.g. "fieldnet.local") by sending an A query and collecting responses.
    pub async fn resolve_hostname(
        &self,
        hostname: &str,
        timeout_dur: Duration,
    ) -> Result<Vec<IpAddr>, MdnsError> {
        let sock_guard = self.socket.read().await;
        let sock = sock_guard.as_ref().ok_or(MdnsError::NotRunning)?;

        let mut ips = Vec::new();
        let norm_host = hostname.trim_end_matches('.').to_lowercase();
        let my_host = self
            .active_hostname
            .read()
            .await
            .trim_end_matches('.')
            .to_lowercase();

        // If querying our own active hostname, include our local IPs immediately
        if norm_host == my_host {
            ips.extend(get_local_interfaces(self.config.allow_loopback));
        }

        let mut packet_rx = self.packet_tx.subscribe();
        let query_bytes = build_query_packet(hostname, simple_dns::TYPE::A)?;
        let mdns_port = self.config.mdns_port;
        let multicast_dest =
            SocketAddr::V4(SocketAddrV4::new(self.config.multicast_ipv4, mdns_port));
        let localhost_dest = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, mdns_port));
        let broadcast_dest = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::BROADCAST, mdns_port));

        let _ = sock.send_to(&query_bytes, multicast_dest).await;
        let _ = sock.send_to(&query_bytes, localhost_dest).await;
        let _ = sock.send_to(&query_bytes, broadcast_dest).await;

        let deadline = tokio::time::Instant::now() + timeout_dur;

        while tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, packet_rx.recv()).await {
                Ok(Ok(pkt_bytes)) => {
                    if let Ok(packet) = parse_packet(&pkt_bytes) {
                        for answer in packet
                            .answers
                            .iter()
                            .chain(packet.additional_records.iter())
                        {
                            let ans_name =
                                answer.name.to_string().trim_end_matches('.').to_lowercase();
                            if ans_name == norm_host {
                                if let simple_dns::rdata::RData::A(a) = &answer.rdata {
                                    ips.push(IpAddr::V4(Ipv4Addr::from(a.address)));
                                }
                            }
                        }
                    }
                }
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
                Ok(Err(broadcast::error::RecvError::Closed)) => break,
                Err(_) => break,
            }

            if !ips.is_empty() && norm_host != my_host {
                break;
            }
        }

        ips.sort();
        ips.dedup();

        if ips.is_empty() {
            Err(MdnsError::Timeout)
        } else {
            Ok(ips)
        }
    }

    /// Discovers DNS-SD services of a specific type (e.g. "_http._tcp.local") by sending a PTR query.
    pub async fn query_service(
        &self,
        service_type: &str,
        timeout_dur: Duration,
    ) -> Result<Vec<MdnsRecord>, MdnsError> {
        let sock_guard = self.socket.read().await;
        let sock = sock_guard.as_ref().ok_or(MdnsError::NotRunning)?;

        let mut packet_rx = self.packet_tx.subscribe();
        let query_bytes = build_query_packet(service_type, simple_dns::TYPE::PTR)?;
        let mdns_port = self.config.mdns_port;
        let multicast_dest =
            SocketAddr::V4(SocketAddrV4::new(self.config.multicast_ipv4, mdns_port));
        let localhost_dest = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, mdns_port));
        let broadcast_dest = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::BROADCAST, mdns_port));

        let _ = sock.send_to(&query_bytes, multicast_dest).await;
        let _ = sock.send_to(&query_bytes, localhost_dest).await;
        let _ = sock.send_to(&query_bytes, broadcast_dest).await;

        let mut discovered = Vec::new();
        let deadline = tokio::time::Instant::now() + timeout_dur;
        let mut idle_deadline = deadline;

        while tokio::time::Instant::now() < deadline {
            let current_limit = deadline.min(idle_deadline);
            let remaining = current_limit.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            match tokio::time::timeout(remaining, packet_rx.recv()).await {
                Ok(Ok(pkt_bytes)) => {
                    if let Ok(packet) = parse_packet(&pkt_bytes) {
                        let records = extract_records_from_packet(&packet);
                        let mut added_any = false;
                        for rec in records {
                            if !discovered.iter().any(|r: &MdnsRecord| {
                                r.instance_name == rec.instance_name && r.port == rec.port
                            }) {
                                discovered.push(rec);
                                added_any = true;
                            }
                        }
                        if added_any {
                            idle_deadline =
                                tokio::time::Instant::now() + Duration::from_millis(150);
                        }
                    }
                }
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => continue,
                Ok(Err(broadcast::error::RecvError::Closed)) => break,
                Err(_) => break,
            }
        }

        Ok(discovered)
    }

    // Helper: sends announcement packet
    async fn send_announcement_packet(
        &self,
        sock: &UdpSocket,
        hostname: &str,
        txt_attrs: &HashMap<String, String>,
        ttl: u32,
    ) {
        let ips = get_local_interfaces(self.config.allow_loopback);
        let info = ServiceAdvertisementInfo {
            service_name: &self.effective_service_name,
            service_type: &self.config.service_type,
            hostname,
            port: self.config.gateway_port,
            ips: &ips,
            txt_attrs,
            ttl,
        };
        if let Ok(bytes) = build_announcement_packet(&info) {
            let mdns_port = self.config.mdns_port;
            let multicast_dest =
                SocketAddr::V4(SocketAddrV4::new(self.config.multicast_ipv4, mdns_port));
            let localhost_dest = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, mdns_port));
            let broadcast_dest = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::BROADCAST, mdns_port));

            let _ = sock.send_to(&bytes, multicast_dest).await;
            let _ = sock.send_to(&bytes, localhost_dest).await;
            let _ = sock.send_to(&bytes, broadcast_dest).await;
            self.state.set_active_announcements(1);
        }
    }

    // Helper: emits event to broadcast channel if present
    fn emit_event(&self, event: ServiceEvent) {
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(event);
        }
    }
}
