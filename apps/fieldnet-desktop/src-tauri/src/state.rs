use fieldnet_runtime::FieldnetRuntime;
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::dto::FrontendEventDto;

pub const MAX_EVENT_HISTORY: usize = 200;

#[derive(Clone)]
pub struct AppState {
    pub runtime: Arc<RwLock<Option<Arc<FieldnetRuntime>>>>,
    pub lifecycle_phase: Arc<RwLock<String>>,
    pub started_at: Arc<RwLock<Option<u64>>>,
    pub data_dir: PathBuf,
    pub last_error: Arc<RwLock<Option<String>>>,
    pub events: Arc<RwLock<VecDeque<FrontendEventDto>>>,
}

impl AppState {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            runtime: Arc::new(RwLock::new(None)),
            lifecycle_phase: Arc::new(RwLock::new("STARTING".to_string())),
            started_at: Arc::new(RwLock::new(None)),
            data_dir,
            last_error: Arc::new(RwLock::new(None)),
            events: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_EVENT_HISTORY))),
        }
    }

    pub async fn set_phase(&self, phase: &str) {
        let mut p = self.lifecycle_phase.write().await;
        *p = phase.to_string();
    }

    pub async fn get_phase(&self) -> String {
        let p = self.lifecycle_phase.read().await;
        p.clone()
    }

    pub async fn set_runtime(&self, rt: Arc<FieldnetRuntime>) {
        let mut r = self.runtime.write().await;
        *r = Some(rt);
        let mut s = self.started_at.write().await;
        *s = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );
        let mut err = self.last_error.write().await;
        *err = None;
    }

    pub async fn clear_runtime(&self, error: Option<String>) {
        let mut r = self.runtime.write().await;
        if let Some(existing) = r.take() {
            existing.shutdown();
        }
        let mut s = self.started_at.write().await;
        *s = None;
        let mut err = self.last_error.write().await;
        *err = error;
    }

    pub async fn get_runtime(&self) -> Option<Arc<FieldnetRuntime>> {
        let r = self.runtime.read().await;
        r.clone()
    }

    pub async fn push_event(&self, event: FrontendEventDto) {
        let mut events = self.events.write().await;
        if events.len() >= MAX_EVENT_HISTORY {
            events.pop_back();
        }
        events.push_front(event);
    }

    pub async fn get_events(&self) -> Vec<FrontendEventDto> {
        let events = self.events.read().await;
        events.iter().cloned().collect()
    }
}
