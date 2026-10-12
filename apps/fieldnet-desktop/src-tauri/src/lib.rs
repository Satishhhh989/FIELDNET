use std::path::PathBuf;

pub mod commands;
pub mod dto;
pub mod events;
pub mod state;

pub use state::AppState;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let data_dir = std::env::var("FIELDNET_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("./data/desktop"));

    let app_state = AppState::new(data_dir);

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .manage(app_state.clone())
        .invoke_handler(tauri::generate_handler![
            commands::get_runtime_status,
            commands::get_node_identity,
            commands::get_network_status,
            commands::get_peers,
            commands::get_services,
            commands::get_applications,
            commands::get_storage_status,
            commands::get_runtime_events,
            commands::start_application,
            commands::stop_application,
            commands::resolve_service,
            commands::send_message,
            commands::disconnect_peer,
            commands::refresh_discovery,
            commands::pin_content,
            commands::unpin_content,
            commands::retry_runtime_start,
        ])
        .setup(move |app| {
            let handle = app.handle().clone();
            let state = app_state.clone();
            tauri::async_runtime::spawn(async move {
                let _ = commands::start_runtime_internal(&handle, &state).await;
            });
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running FIELDNET desktop application");
}
