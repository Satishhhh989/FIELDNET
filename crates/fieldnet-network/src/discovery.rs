use crate::error::NetworkError;
use fieldnet_core::{NodeId, Timestamp};
use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, Socket, Type};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

pub const DEFAULT_DISCOVERY_PORT: u16 = 4242;
pub const MULTICAST_GROUP: &str = "239.255.42.99";
pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DiscoveryBeacon {
    pub node_id: NodeId,
    pub tcp_port: u16,
    pub protocol_version: u32,
    pub created_at: Timestamp,
    pub expires_at: Timestamp,
}

impl DiscoveryBeacon {
    pub fn new(node_id: NodeId, tcp_port: u16, ttl_secs: u64) -> Self {
        let now = Timestamp::now();
        Self {
            node_id,
            tcp_port,
            protocol_version: PROTOCOL_VERSION,
            created_at: now,
            expires_at: now.add_seconds(ttl_secs),
        }
    }

    pub fn is_valid(&self) -> bool {
        self.protocol_version == PROTOCOL_VERSION && !self.expires_at.is_expired()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscoveredPeer {
    pub node_id: NodeId,
    pub tcp_addr: SocketAddr,
    pub protocol_version: u32,
    pub discovered_at: Timestamp,
    pub expires_at: Timestamp,
}

pub struct DiscoveryConfig {
    pub discovery_port: u16,
    pub beacon_interval: Duration,
    pub beacon_ttl_secs: u64,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            discovery_port: DEFAULT_DISCOVERY_PORT,
            beacon_interval: Duration::from_millis(500),
            beacon_ttl_secs: 15,
        }
    }
}

pub struct DiscoveryService {
    local_node_id: NodeId,
    tcp_port: u16,
    config: DiscoveryConfig,
    socket: Arc<UdpSocket>,
    peer_tx: broadcast::Sender<DiscoveredPeer>,
    shutdown_tx: broadcast::Sender<()>,
}

impl DiscoveryService {
    pub fn new(
        local_node_id: NodeId,
        tcp_port: u16,
        config: DiscoveryConfig,
    ) -> Result<Self, NetworkError> {
        let addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, config.discovery_port);
        let socket2_sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket2_sock.set_reuse_address(true)?;

        #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
        let _ = socket2_sock.set_reuse_port(true);

        socket2_sock.set_broadcast(true)?;
        socket2_sock.set_nonblocking(true)?;
        socket2_sock.bind(&addr.into())?;

        let std_socket: std::net::UdpSocket = socket2_sock.into();
        let tokio_socket = UdpSocket::from_std(std_socket)?;

        // Try joining multicast group, but continue even if interface multicast fails (e.g., in restricted environments)
        let multi_ip: Ipv4Addr = MULTICAST_GROUP.parse().unwrap();
        if let Err(e) = tokio_socket.join_multicast_v4(multi_ip, Ipv4Addr::UNSPECIFIED) {
            debug!(
                "Could not join multicast group (will use local broadcast): {}",
                e
            );
        }

        let (peer_tx, _) = broadcast::channel(128);
        let (shutdown_tx, _) = broadcast::channel(1);

        Ok(Self {
            local_node_id,
            tcp_port,
            config,
            socket: Arc::new(tokio_socket),
            peer_tx,
            shutdown_tx,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<DiscoveredPeer> {
        self.peer_tx.subscribe()
    }

    pub async fn start(&self) -> Result<(), NetworkError> {
        let socket_recv = Arc::clone(&self.socket);
        let socket_send = Arc::clone(&self.socket);
        let local_id = self.local_node_id;
        let tcp_port = self.tcp_port;
        let config_interval = self.config.beacon_interval;
        let config_ttl = self.config.beacon_ttl_secs;
        let discovery_port = self.config.discovery_port;
        let peer_tx = self.peer_tx.clone();
        let mut shutdown_rx1 = self.shutdown_tx.subscribe();
        let mut shutdown_rx2 = self.shutdown_tx.subscribe();

        // Target destinations for discovery announcements: multicast and localhost broadcast
        let multicast_target: SocketAddr = format!("{}:{}", MULTICAST_GROUP, discovery_port)
            .parse()
            .unwrap();
        let localhost_target: SocketAddr = format!("127.0.0.1:{}", discovery_port).parse().unwrap();
        let broadcast_target: SocketAddr = format!("255.255.255.255:{}", discovery_port)
            .parse()
            .unwrap();

        // Spawn beacon sender task
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(config_interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let beacon = DiscoveryBeacon::new(local_id, tcp_port, config_ttl);
                        if let Ok(bytes) = serde_json::to_vec(&beacon) {
                            // Send to multicast, localhost, and subnet broadcast
                            let _ = socket_send.send_to(&bytes, multicast_target).await;
                            let _ = socket_send.send_to(&bytes, localhost_target).await;
                            let _ = socket_send.send_to(&bytes, broadcast_target).await;
                        }
                    }
                    _ = shutdown_rx1.recv() => {
                        debug!("Discovery sender task shutting down");
                        break;
                    }
                }
            }
        });

        // Spawn beacon receiver task
        tokio::spawn(async move {
            let mut buf = [0u8; 4096];
            loop {
                tokio::select! {
                    recv_res = socket_recv.recv_from(&mut buf) => {
                        match recv_res {
                            Ok((len, remote_addr)) => {
                                match serde_json::from_slice::<DiscoveryBeacon>(&buf[..len]) {
                                    Ok(beacon) => {
                                        // Ignore our own beacons
                                        if beacon.node_id == local_id {
                                            continue;
                                        }

                                        // Validate freshness and protocol version
                                        if !beacon.is_valid() {
                                            debug!("Ignoring expired or invalid beacon from {}", beacon.node_id);
                                            continue;
                                        }

                                        let peer_ip = if remote_addr.ip().is_loopback() || remote_addr.ip().is_unspecified() {
                                            Ipv4Addr::LOCALHOST.into()
                                        } else {
                                            remote_addr.ip()
                                        };

                                        let tcp_addr = SocketAddr::new(peer_ip, beacon.tcp_port);
                                        let peer = DiscoveredPeer {
                                            node_id: beacon.node_id,
                                            tcp_addr,
                                            protocol_version: beacon.protocol_version,
                                            discovered_at: Timestamp::now(),
                                            expires_at: beacon.expires_at,
                                        };

                                        debug!("Discovered peer {} at {}", peer.node_id, peer.tcp_addr);
                                        let _ = peer_tx.send(peer);
                                    }
                                    Err(err) => {
                                        // Not a FIELDNET discovery packet, ignore silently
                                        debug!("Non-beacon packet from {}: {}", remote_addr, err);
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Discovery socket recv error: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx2.recv() => {
                        debug!("Discovery receiver task shutting down");
                        break;
                    }
                }
            }
        });

        info!(
            "Discovery service started on port {} (advertising TCP port {})",
            discovery_port, tcp_port
        );
        Ok(())
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}
