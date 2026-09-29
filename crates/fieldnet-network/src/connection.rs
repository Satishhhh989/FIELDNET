use fieldnet_core::{NodeId, SessionId};
use fieldnet_identity::NodeIdentity;
use fieldnet_protocol::Message;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, error, info, warn};

use crate::error::NetworkError;
use crate::framing::{read_frame, write_frame};
use crate::session::{handshake_as_initiator, handshake_as_responder, SecureSession};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionState {
    Discovered,
    Connecting,
    Authenticating,
    Connected,
    Disconnected,
}

#[derive(Clone, Debug)]
pub enum ConnectionEvent {
    StateChanged {
        peer_id: NodeId,
        state: ConnectionState,
    },
    PeerConnected {
        peer_id: NodeId,
        session_id: SessionId,
    },
    PeerDisconnected {
        peer_id: NodeId,
    },
}

pub type PeerConnectionMap = Arc<RwLock<HashMap<NodeId, (SessionId, mpsc::Sender<Message>)>>>;

pub struct ConnectionManager {
    local_identity: Arc<NodeIdentity>,
    tcp_port: u16,
    connections: PeerConnectionMap,
    connecting_peers: Arc<RwLock<HashSet<NodeId>>>,
    event_tx: broadcast::Sender<ConnectionEvent>,
    inbound_msg_tx: mpsc::Sender<(NodeId, Message)>,
    inbound_msg_rx: tokio::sync::Mutex<Option<mpsc::Receiver<(NodeId, Message)>>>,
    shutdown_tx: broadcast::Sender<()>,
}

impl ConnectionManager {
    pub fn new(local_identity: Arc<NodeIdentity>, tcp_port: u16) -> Self {
        let (event_tx, _) = broadcast::channel(128);
        let (inbound_msg_tx, inbound_msg_rx) = mpsc::channel(256);
        let (shutdown_tx, _) = broadcast::channel(1);

        Self {
            local_identity,
            tcp_port,
            connections: Arc::new(RwLock::new(HashMap::new())),
            connecting_peers: Arc::new(RwLock::new(HashSet::new())),
            event_tx,
            inbound_msg_tx,
            inbound_msg_rx: tokio::sync::Mutex::new(Some(inbound_msg_rx)),
            shutdown_tx,
        }
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<ConnectionEvent> {
        self.event_tx.subscribe()
    }

    pub async fn take_inbound_receiver(&self) -> Option<mpsc::Receiver<(NodeId, Message)>> {
        let mut lock = self.inbound_msg_rx.lock().await;
        lock.take()
    }

    pub async fn is_connected(&self, peer_id: &NodeId) -> bool {
        let conns = self.connections.read().await;
        if let Some((_, tx)) = conns.get(peer_id) {
            !tx.is_closed()
        } else {
            false
        }
    }

    pub async fn connected_peers(&self) -> Vec<NodeId> {
        let conns = self.connections.read().await;
        conns
            .iter()
            .filter_map(|(id, (_, tx))| if !tx.is_closed() { Some(*id) } else { None })
            .collect()
    }

    pub async fn send_message(
        &self,
        peer_id: &NodeId,
        message: Message,
    ) -> Result<(), NetworkError> {
        let sender = {
            let conns = self.connections.read().await;
            conns.get(peer_id).map(|(_, tx)| tx.clone())
        };

        if let Some(tx) = sender {
            tx.send(message)
                .await
                .map_err(|_| NetworkError::ConnectionClosed)?;
            Ok(())
        } else {
            Err(NetworkError::PeerNotFound(peer_id.to_string()))
        }
    }

    pub async fn start_listener(&self) -> Result<u16, NetworkError> {
        let bind_addr: SocketAddr = format!("0.0.0.0:{}", self.tcp_port).parse().unwrap();
        let listener = TcpListener::bind(bind_addr).await?;
        let actual_port = listener.local_addr()?.port();

        info!("TCP Listener bound to {}", listener.local_addr()?);

        let local_identity = Arc::clone(&self.local_identity);
        let connections = Arc::clone(&self.connections);
        let event_tx = self.event_tx.clone();
        let inbound_tx = self.inbound_msg_tx.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept_res = listener.accept() => {
                        match accept_res {
                            Ok((stream, remote_addr)) => {
                                debug!("Inbound TCP connection accepted from {}", remote_addr);
                                let id = Arc::clone(&local_identity);
                                let conns = Arc::clone(&connections);
                                let ev_tx = event_tx.clone();
                                let in_tx = inbound_tx.clone();

                                tokio::spawn(async move {
                                    if let Err(e) = Self::handle_inbound_connection(stream, id, conns, ev_tx, in_tx).await {
                                        debug!("Inbound connection handling from {}: {}", remote_addr, e);
                                    }
                                });
                            }
                            Err(e) => {
                                error!("TCP accept error: {}", e);
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        debug!("TCP listener shutting down");
                        break;
                    }
                }
            }
        });

        Ok(actual_port)
    }

    pub async fn connect_peer(
        &self,
        peer_node_id: NodeId,
        addr: SocketAddr,
    ) -> Result<(), NetworkError> {
        if self.is_connected(&peer_node_id).await {
            debug!("Already connected to peer {}", peer_node_id);
            return Ok(());
        }

        // Prevent simultaneous redundant connect attempts
        {
            let mut connecting = self.connecting_peers.write().await;
            if connecting.contains(&peer_node_id) {
                return Ok(());
            }
            connecting.insert(peer_node_id);
        }

        let result = self.do_connect_peer(peer_node_id, addr).await;

        // Clean up connecting lock
        {
            let mut connecting = self.connecting_peers.write().await;
            connecting.remove(&peer_node_id);
        }

        result
    }

    async fn do_connect_peer(
        &self,
        peer_node_id: NodeId,
        addr: SocketAddr,
    ) -> Result<(), NetworkError> {
        let _ = self.event_tx.send(ConnectionEvent::StateChanged {
            peer_id: peer_node_id,
            state: ConnectionState::Connecting,
        });

        let mut stream = TcpStream::connect(addr).await?;
        debug!("Connected TCP socket to {}", addr);

        let _ = self.event_tx.send(ConnectionEvent::StateChanged {
            peer_id: peer_node_id,
            state: ConnectionState::Authenticating,
        });

        let session = handshake_as_initiator(&mut stream, &self.local_identity).await?;

        if session.peer_node_id != peer_node_id {
            let err = format!(
                "Connected peer identity mismatch: expected {}, got {}",
                peer_node_id, session.peer_node_id
            );
            error!("{}", err);
            return Err(NetworkError::AuthenticationFailed(err));
        }

        info!(
            "Mutual authentication successful with peer {} (session: {})",
            session.peer_node_id, session.session_id
        );

        Self::spawn_session_tasks(
            stream,
            session,
            Arc::clone(&self.connections),
            self.event_tx.clone(),
            self.inbound_msg_tx.clone(),
        )
        .await;

        Ok(())
    }

    async fn handle_inbound_connection(
        mut stream: TcpStream,
        local_identity: Arc<NodeIdentity>,
        connections: PeerConnectionMap,
        event_tx: broadcast::Sender<ConnectionEvent>,
        inbound_tx: mpsc::Sender<(NodeId, Message)>,
    ) -> Result<(), NetworkError> {
        let session = handshake_as_responder(&mut stream, &local_identity).await?;

        info!(
            "Mutual authentication successful for incoming peer {} (session: {})",
            session.peer_node_id, session.session_id
        );

        Self::spawn_session_tasks(stream, session, connections, event_tx, inbound_tx).await;
        Ok(())
    }

    async fn spawn_session_tasks(
        stream: TcpStream,
        session: SecureSession,
        connections: PeerConnectionMap,
        event_tx: broadcast::Sender<ConnectionEvent>,
        inbound_tx: mpsc::Sender<(NodeId, Message)>,
    ) {
        let peer_id = session.peer_node_id;
        let session_id = session.session_id;

        let (mut tx_half, mut rx_half) = session.into_split();
        let (mut read_stream, mut write_stream) = stream.into_split();

        let (outbound_tx, mut outbound_rx) = mpsc::channel::<Message>(128);

        // Check if an existing live connection already exists.
        // If so, avoid clobbering the active session.
        let should_start = {
            let mut conns = connections.write().await;
            if let Some((existing_id, existing_sender)) = conns.get(&peer_id) {
                if !existing_sender.is_closed() {
                    debug!(
                        "Existing active session {} with peer {} maintained; closing redundant session {}",
                        existing_id, peer_id, session_id
                    );
                    false
                } else {
                    conns.insert(peer_id, (session_id, outbound_tx));
                    true
                }
            } else {
                conns.insert(peer_id, (session_id, outbound_tx));
                true
            }
        };

        if !should_start {
            return;
        }

        let _ = event_tx.send(ConnectionEvent::StateChanged {
            peer_id,
            state: ConnectionState::Connected,
        });
        let _ = event_tx.send(ConnectionEvent::PeerConnected {
            peer_id,
            session_id,
        });

        // Spawn writer task
        let peer_id_w = peer_id;
        let writer_task = tokio::spawn(async move {
            while let Some(msg) = outbound_rx.recv().await {
                match fieldnet_protocol::serialize_message(&msg) {
                    Ok(plaintext) => match tx_half.encrypt(&plaintext) {
                        Ok(ciphertext) => {
                            if let Err(e) = write_frame(&mut write_stream, &ciphertext).await {
                                debug!("Write error to peer {}: {}", peer_id_w, e);
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Encryption error for peer {}: {}", peer_id_w, e);
                            break;
                        }
                    },
                    Err(e) => {
                        error!("Failed to serialize message: {}", e);
                    }
                }
            }
            debug!("Writer task ended for peer {}", peer_id_w);
        });

        // Spawn reader task
        let peer_id_r = peer_id;
        let conns_cleanup = Arc::clone(&connections);
        let event_tx_cleanup = event_tx.clone();

        tokio::spawn(async move {
            loop {
                match read_frame(&mut read_stream).await {
                    Ok(ciphertext) => match rx_half.decrypt(&ciphertext) {
                        Ok(plaintext) => match fieldnet_protocol::deserialize_message(&plaintext) {
                            Ok(message) => {
                                if let Err(e) = inbound_tx.send((peer_id_r, message)).await {
                                    warn!("Inbound message dispatch failed: {}", e);
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!("Malformed message received from peer {}: {}", peer_id_r, e);
                            }
                        },
                        Err(e) => {
                            debug!("Decryption error from peer {}: {}", peer_id_r, e);
                            break;
                        }
                    },
                    Err(e) => {
                        debug!("Connection closed by peer {}: {}", peer_id_r, e);
                        break;
                    }
                }
            }

            // Clean up on disconnect ONLY if this session is the active one in the table
            {
                let mut conns = conns_cleanup.write().await;
                if let Some((active_session_id, _)) = conns.get(&peer_id_r) {
                    if *active_session_id == session_id {
                        conns.remove(&peer_id_r);
                        let _ = event_tx_cleanup.send(ConnectionEvent::StateChanged {
                            peer_id: peer_id_r,
                            state: ConnectionState::Disconnected,
                        });
                        let _ = event_tx_cleanup
                            .send(ConnectionEvent::PeerDisconnected { peer_id: peer_id_r });
                        info!(
                            "Peer {} disconnected and session {} closed",
                            peer_id_r, session_id
                        );
                    } else {
                        debug!(
                            "Old session {} closed; active session {} remains",
                            session_id, active_session_id
                        );
                    }
                }
            }
            writer_task.abort();
        });
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }
}
