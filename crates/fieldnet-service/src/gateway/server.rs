use fieldnet_content::ContentStore;
use fieldnet_core::{NodeId, Timestamp};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, RwLock};

use crate::events::ServiceEvent;
use crate::registry::ServiceRegistry;
use crate::resolver::ServiceResolver;

use super::config::GatewayConfig;
use super::dispatch::ServiceDispatcher;
use super::error::GatewayError;
use super::router::{build_router, GatewayContext};
use super::session::BrowserSessionStore;
use super::state::{GatewayState, GatewayStateMachine};

/// The authoritative FIELDNET Local Web Gateway (Phase 08).
///
/// Provides a sovereign, browser-accessible HTTP reverse proxy and adapter
/// translating browser interactions into FIELDNET decentralized mesh service invocations.
pub struct LocalWebGateway {
    config: GatewayConfig,
    state_machine: GatewayStateMachine,
    node_id: NodeId,
    registry: Arc<ServiceRegistry>,
    resolver: Arc<ServiceResolver>,
    dispatcher: Arc<dyn ServiceDispatcher>,
    content_store: Option<Arc<ContentStore>>,
    session_store: Arc<BrowserSessionStore>,
    event_tx: Option<broadcast::Sender<ServiceEvent>>,
    bound_addr: Arc<RwLock<Option<SocketAddr>>>,
    shutdown_tx: broadcast::Sender<()>,
    start_time: Arc<RwLock<Option<Timestamp>>>,
}

impl LocalWebGateway {
    /// Constructs a new `LocalWebGateway` in the `Stopped` state.
    pub fn new(
        config: GatewayConfig,
        node_id: NodeId,
        registry: Arc<ServiceRegistry>,
        resolver: Arc<ServiceResolver>,
        dispatcher: Arc<dyn ServiceDispatcher>,
        content_store: Option<Arc<ContentStore>>,
    ) -> Self {
        let (shutdown_tx, _) = broadcast::channel(4);
        Self {
            config,
            state_machine: GatewayStateMachine::new(),
            node_id,
            registry,
            resolver,
            dispatcher,
            content_store,
            session_store: Arc::new(BrowserSessionStore::default()),
            event_tx: None,
            bound_addr: Arc::new(RwLock::new(None)),
            shutdown_tx,
            start_time: Arc::new(RwLock::new(None)),
        }
    }

    /// Configures an event bus sender for emitting gateway and browser pairing events.
    pub fn with_event_tx(mut self, tx: broadcast::Sender<ServiceEvent>) -> Self {
        self.event_tx = Some(tx);
        self
    }

    /// Configures a custom `BrowserSessionStore`.
    pub fn with_session_store(mut self, store: Arc<BrowserSessionStore>) -> Self {
        self.session_store = store;
        self
    }

    /// Returns a reference to the active `BrowserSessionStore`.
    pub fn session_store(&self) -> &Arc<BrowserSessionStore> {
        &self.session_store
    }

    /// Returns the current lifecycle state of the gateway.
    pub async fn state(&self) -> GatewayState {
        self.state_machine.current().await
    }

    /// Returns true if the gateway is actively running and serving requests.
    pub async fn is_running(&self) -> bool {
        self.state_machine.current().await == GatewayState::Running
    }

    /// Returns the active bound local listening address, if running.
    pub async fn local_addr(&self) -> Option<SocketAddr> {
        *self.bound_addr.read().await
    }

    /// Returns a reference to the gateway configuration.
    pub fn config(&self) -> &GatewayConfig {
        &self.config
    }

    /// Starts the HTTP listener and server task.
    ///
    /// Transitions: `Stopped` -> `Starting` -> `Binding` -> `Running`.
    pub async fn start(&self) -> Result<SocketAddr, GatewayError> {
        self.state_machine
            .transition(GatewayState::Starting)
            .await?;

        self.state_machine.transition(GatewayState::Binding).await?;
        let listener = match TcpListener::bind(self.config.bind_address).await {
            Ok(l) => l,
            Err(e) => {
                let _ = self.state_machine.transition(GatewayState::Degraded).await;
                return Err(GatewayError::BindError {
                    addr: self.config.bind_address,
                    source: e,
                });
            }
        };

        let local_addr = match listener.local_addr() {
            Ok(a) => a,
            Err(e) => {
                let _ = self.state_machine.transition(GatewayState::Degraded).await;
                return Err(GatewayError::Io(e));
            }
        };

        {
            let mut addr_guard = self.bound_addr.write().await;
            *addr_guard = Some(local_addr);
        }
        {
            let mut time_guard = self.start_time.write().await;
            *time_guard = Some(Timestamp::now());
        }

        self.state_machine.transition(GatewayState::Running).await?;
        tracing::info!(
            "LocalWebGateway running on http://{} (node: {})",
            local_addr,
            self.node_id
        );

        let context = Arc::new(GatewayContext {
            config: self.config.clone(),
            state_machine: self.state_machine.clone(),
            node_id: self.node_id,
            registry: Arc::clone(&self.registry),
            resolver: Arc::clone(&self.resolver),
            dispatcher: Arc::clone(&self.dispatcher),
            content_store: self.content_store.clone(),
            session_store: Arc::clone(&self.session_store),
            event_tx: self.event_tx.clone(),
            start_time: self.start_time.read().await.unwrap_or_else(Timestamp::now),
        });

        let app = build_router(context);
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let state_machine = self.state_machine.clone();
        let bound_addr_tracker = Arc::clone(&self.bound_addr);

        tokio::spawn(async move {
            let server = axum::serve(listener, app).with_graceful_shutdown(async move {
                let _ = shutdown_rx.recv().await;
                tracing::info!("LocalWebGateway received shutdown signal, draining connections");
            });

            if let Err(e) = server.await {
                tracing::error!("LocalWebGateway server error: {}", e);
                let _ = state_machine.transition(GatewayState::Degraded).await;
            }

            {
                let mut guard = bound_addr_tracker.write().await;
                *guard = None;
            }
            let _ = state_machine.transition(GatewayState::Stopped).await;
            tracing::info!("LocalWebGateway successfully stopped");
        });

        Ok(local_addr)
    }

    /// Initiates a graceful shutdown of the Local Web Gateway.
    ///
    /// Transitions: `Running` / `Degraded` -> `Stopping` -> `Stopped`.
    pub async fn stop(&self) -> Result<(), GatewayError> {
        let current = self.state_machine.current().await;
        if current == GatewayState::Stopped {
            return Ok(());
        }

        self.state_machine
            .transition(GatewayState::Stopping)
            .await?;
        let _ = self.shutdown_tx.send(());

        // Wait up to 5 seconds for background server task to transition to Stopped
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            if self.state_machine.current().await == GatewayState::Stopped {
                break;
            }
        }

        if self.state_machine.current().await != GatewayState::Stopped {
            let _ = self.state_machine.transition(GatewayState::Stopped).await;
        }

        Ok(())
    }

    /// Accessor for the active bound SocketAddr, if running.
    pub async fn bound_addr(&self) -> Option<SocketAddr> {
        let guard = self.bound_addr.read().await;
        *guard
    }
}
