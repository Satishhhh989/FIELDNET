use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info};
use wasmtime::{Config, Engine, Module, OptLevel};

use crate::error::ApplicationError;

/// Ticking interval for epoch-based execution interruption (10 ms).
pub const DEFAULT_EPOCH_TICK_INTERVAL_MS: u64 = 10;

/// Maximum WebAssembly call stack size (1 MB).
pub const MAX_WASM_STACK_BYTES: usize = 1024 * 1024;

/// Reusable WebAssembly execution engine configured explicitly for determinism,
/// strict sandboxing, fuel consumption, and epoch-based interruption.
pub struct WasmEngine {
    engine: Engine,
    epoch_ticker_active: Arc<AtomicBool>,
}

impl WasmEngine {
    /// Creates a newly configured WasmEngine with fuel metering, epoch interruption,
    /// and strict sandboxing defaults.
    pub fn new() -> Result<Self, ApplicationError> {
        let mut config = Config::new();

        // 1. Strict fuel metering: instructions consume fuel deterministically
        config.consume_fuel(true);

        // 2. Epoch-based interruption for non-blocking timeout and cancellation enforcement
        config.epoch_interruption(true);

        // 3. Strict call stack limits (1 MB) to prevent host thread stack overflow
        config.max_wasm_stack(MAX_WASM_STACK_BYTES);

        // 4. Deterministic floating-point NaN canonicalization
        config.cranelift_nan_canonicalization(true);

        // 5. Optimization level
        config.cranelift_opt_level(OptLevel::Speed);

        // 6. Explicitly disable memory64 and non-standard extensions
        config.wasm_memory64(false);
        config.wasm_multi_memory(false);

        let engine = Engine::new(&config).map_err(|e| {
            ApplicationError::ExecutionError(format!("Failed to initialize Wasmtime engine: {}", e))
        })?;

        let epoch_ticker_active = Arc::new(AtomicBool::new(true));
        let active_flag = Arc::clone(&epoch_ticker_active);
        let engine_clone = engine.clone();

        // Spawn background epoch ticker task
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_millis(DEFAULT_EPOCH_TICK_INTERVAL_MS));
            while active_flag.load(Ordering::Relaxed) {
                interval.tick().await;
                engine_clone.increment_epoch();
            }
            debug!("WasmEngine epoch ticker terminated");
        });

        info!("WasmEngine initialized with fuel metering and epoch interruption");
        Ok(Self {
            engine,
            epoch_ticker_active,
        })
    }

    /// Accessor for the underlying Wasmtime `Engine`.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Compiles raw verified WebAssembly bytecode into an executable `Module`.
    pub fn compile(&self, wasm_bytes: &[u8]) -> Result<Module, ApplicationError> {
        Module::from_binary(&self.engine, wasm_bytes).map_err(|e| {
            ApplicationError::CompilationError(format!("Failed to compile WASM module: {}", e))
        })
    }

    /// Manually increments the engine epoch counter, triggering immediate timeout/interruption
    /// on stores whose epoch deadline has elapsed.
    pub fn increment_epoch(&self) {
        self.engine.increment_epoch();
    }

    /// Advances the engine epoch counter by the specified number of ticks.
    pub fn advance_epoch(&self, ticks: u64) {
        for _ in 0..ticks {
            self.engine.increment_epoch();
        }
    }
}

impl Drop for WasmEngine {
    fn drop(&mut self) {
        self.epoch_ticker_active.store(false, Ordering::Relaxed);
    }
}
