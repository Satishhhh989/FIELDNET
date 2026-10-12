use clap::{Parser, Subcommand};
use fieldnet_core::NodeId;
use fieldnet_runtime::app::{
    AppVersion, ApplicationId, ApplicationPackage, CapabilityContext, ExecutionId,
};
use fieldnet_runtime::service::{ServiceId, ServiceQuery};
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser, Debug)]
#[command(name = "fieldnet-node")]
#[command(about = "FIELDNET Distributed Runtime Node", long_about = None)]
struct Cli {
    /// Directory to store persistent node identity and data
    #[arg(short, long, default_value = "./data")]
    data_dir: PathBuf,

    /// Local TCP listening port (0 for dynamic port allocation)
    #[arg(short, long, default_value_t = 0)]
    tcp_port: u16,

    /// UDP discovery port (default: 4242)
    #[arg(long, default_value_t = 4242)]
    discovery_port: u16,

    /// Automatically send a message to the first discovered peer
    #[arg(long)]
    auto_send: bool,

    /// Target peer Node ID to send a message to
    #[arg(long)]
    target_peer: Option<String>,

    /// Text payload for the message
    #[arg(long, default_value = "Hello from FIELDNET Node")]
    message: String,

    /// Automatically shut down after N seconds (useful for tests/demos)
    #[arg(long)]
    run_seconds: Option<u64>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Service discovery and registry commands
    Services {
        #[command(subcommand)]
        cmd: ServicesCommand,
    },
    /// Local mDNS / DNS-SD status and discovery inspection
    Mdns {
        #[command(subcommand)]
        cmd: MdnsCommand,
    },
    /// Application and package inspection commands (Phase 09)
    App {
        #[command(subcommand)]
        cmd: AppCommand,
    },
}

#[derive(Subcommand, Debug)]
enum AppCommand {
    /// List all currently installed applications
    List,
    /// Inspect details of a specific application
    Info {
        /// ApplicationId to inspect
        application: String,
    },
    /// Verify an application package JSON file from disk
    Verify {
        /// Path to the application package JSON file
        package: PathBuf,
    },
    /// Run an authorized application through the Wasmtime execution supervisor
    Run {
        /// ApplicationId to execute
        application: String,
    },
    /// Start an authorized application through the lifecycle manager
    Start {
        /// ApplicationId to start
        application: String,
    },
    /// Stop an active application execution by ExecutionId
    Stop {
        /// ExecutionId to stop
        execution: String,
    },
    /// Stop all active executions of an application by ApplicationId
    StopApp {
        /// ApplicationId to stop
        application: String,
    },
    /// Gracefully restart an application
    Restart {
        /// ApplicationId to restart
        application: String,
    },
    /// Inspect dependency resolution status and graph
    Dependencies {
        /// ApplicationId to inspect
        application: String,
    },
    /// Transactionally update an installed application package
    Update {
        /// Path to the new application package JSON file
        package: PathBuf,
    },
    /// Roll back an application to a previous version
    Rollback {
        /// ApplicationId to roll back
        application: String,
        /// Optional specific target version to restore
        #[arg(long)]
        target_version: Option<String>,
    },
    /// Quarantine / isolate an application from running
    Isolate {
        /// ApplicationId to isolate
        application: String,
        /// Reason for administrative or security isolation
        #[arg(long, default_value = "Administrative quarantine")]
        reason: String,
    },
    /// Lift quarantine / isolation from an application
    Unisolate {
        /// ApplicationId to unisolate
        application: String,
    },
    /// Safely uninstall an application
    Uninstall {
        /// ApplicationId to uninstall
        application: String,
    },
    /// List all currently active application executions
    Executions,
}

#[derive(Subcommand, Debug)]
enum MdnsCommand {
    /// Inspect mDNS / DNS-SD local gateway advertisement status
    Status,
}

#[derive(Subcommand, Debug)]
enum ServicesCommand {
    /// List all currently registered services
    List,
    /// Inspect a specific service
    Inspect {
        /// ServiceId to inspect
        service: String,
    },
    /// Resolve a service query using deterministic selection
    Resolve {
        /// ServiceId to resolve
        service: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);

    let cli = Cli::parse();

    println!("==================================================");
    println!("FIELDNET — Autonomous Resilient Distributed Node");
    println!("BUILD-01: Secure Peer Exchange Milestone");
    println!("==================================================");

    let config = RuntimeConfig {
        data_dir: cli.data_dir,
        tcp_port: cli.tcp_port,
        discovery_port: cli.discovery_port,
        auto_connect: true,
        beacon_interval: Duration::from_millis(500),
        allowed_peers: None,
        blocked_peers: None,
    };

    let runtime = FieldnetRuntime::start(config).await?;
    let node_id = runtime.node_id();

    println!("Node ID:       {}", node_id);
    println!("TCP Port:      {}", runtime.tcp_port());
    println!("Discovery Port:{}", cli.discovery_port);
    println!("--------------------------------------------------");

    // Handle CLI commands if specified
    if let Some(Commands::Services { cmd }) = cli.command {
        match cmd {
            ServicesCommand::List => {
                let services = runtime.service_registry().list_all_services().await;
                let instances = runtime.service_registry().list_all_instances().await;
                println!("REGISTERED SERVICES ({})", services.len());
                println!(
                    "{:<25} {:<36} {:<15} {:<12}",
                    "SERVICE ID", "INSTANCE ID", "PROVIDER", "STATE"
                );
                println!("{}", "-".repeat(90));
                for inst in instances {
                    println!(
                        "{:<25} {:<36} {:<15} {:<12?}",
                        inst.manifest.service_id,
                        inst.manifest.instance_id,
                        inst.manifest
                            .provider_node_id
                            .to_string()
                            .chars()
                            .take(12)
                            .collect::<String>(),
                        inst.state()
                    );
                }
            }
            ServicesCommand::Inspect { service } => match ServiceId::new(&service) {
                Ok(sid) => {
                    let records = runtime.service_registry().get_service_instances(&sid).await;
                    println!(
                        "INSPECT SERVICE '{}': {} instance(s) found",
                        sid,
                        records.len()
                    );
                    for r in records {
                        println!("--------------------------------------------------");
                        println!("Instance ID:   {}", r.manifest.instance_id);
                        println!("Provider:      {}", r.manifest.provider_node_id);
                        println!("Version:       {}", r.manifest.version);
                        println!("Endpoint:      {:?}", r.manifest.endpoint);
                        println!("State:         {:?}", r.state());
                        println!("Capabilities:  {:?}", r.manifest.capabilities);
                        println!("Permissions:   {:?}", r.manifest.permissions);
                        println!("TTL:           {}s", r.manifest.ttl_seconds);
                        println!("Expires At:    {}", r.manifest.expires_at);
                    }
                }
                Err(e) => {
                    error!("Invalid Service ID: {}", e);
                }
            },
            ServicesCommand::Resolve { service } => match ServiceId::new(&service) {
                Ok(sid) => {
                    let query = ServiceQuery::new(sid);
                    match runtime.resolve_service(&query, None).await {
                        Ok(res) => {
                            println!("RESOLVED SERVICE '{}':", query.service_id);
                            println!("--------------------------------------------------");
                            println!("Instance ID:   {}", res.instance_id());
                            println!("Provider:      {}", res.provider_node_id());
                            println!("Endpoint:      {:?}", res.endpoint());
                            println!("Route Hops:    {:?}", res.route_hops);
                            println!("Local:         {}", res.is_local());
                            println!("Reason:        {:?}", res.reason);
                        }
                        Err(e) => {
                            println!("RESOLUTION FAILED: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("Invalid Service ID: {}", e);
                }
            },
        }
        runtime.shutdown();
        return Ok(());
    }

    if let Some(Commands::Mdns { cmd }) = cli.command {
        match cmd {
            MdnsCommand::Status => {
                println!("FIELDNET mDNS / DNS-SD SUBSYSTEM STATUS");
                println!("{}", "=".repeat(50));
                if let Some(status) = runtime.mdns_status().await {
                    println!("State:                {:?}", status.state);
                    println!("Hostname:             {}", status.hostname);
                    println!("Advertised Port:      {}", status.advertised_port);
                    println!("Service Type:         {}", status.service_type);
                    println!("Service Name:         {}", status.service_name);
                    println!(
                        "Advertisement Status: {}",
                        if status.active_announcements > 0 {
                            "Active"
                        } else {
                            "Withdrawn/Inactive"
                        }
                    );
                    println!(
                        "Last Error:           {}",
                        status.last_error.as_deref().unwrap_or("None")
                    );
                    println!("Queries Received:     {}", status.queries_count);
                    println!("Responses Sent:       {}", status.responses_count);
                    println!("Errors Encountered:   {}", status.errors_count);
                    println!("Interfaces ({}):", status.interfaces.len());
                    for iface in status.interfaces {
                        println!("  - {}", iface);
                    }
                } else {
                    println!("Status: Disabled or Not Running (gateway not active)");
                }
                println!("{}", "=".repeat(50));
            }
        }
        runtime.shutdown();
        return Ok(());
    }

    if let Some(Commands::App { cmd }) = cli.command {
        match cmd {
            AppCommand::List => {
                let apps = runtime.application_registry().list()?;
                println!("INSTALLED APPLICATIONS ({})", apps.len());
                println!(
                    "{:<30} {:<12} {:<15} {:<12} {:<15}",
                    "APPLICATION ID", "VERSION", "PUBLISHER", "STATE", "SIGNATURE"
                );
                println!("{}", "-".repeat(90));
                for app in apps {
                    println!(
                        "{:<30} {:<12} {:<15} {:<12?} {:<15?}",
                        app.application_id,
                        app.installed_version,
                        app.publisher
                            .to_string()
                            .chars()
                            .take(14)
                            .collect::<String>(),
                        app.lifecycle_state,
                        app.signature_status
                    );
                }
            }
            AppCommand::Info { application } => match ApplicationId::new(&application) {
                Ok(app_id) => {
                    let maybe_app = runtime.application_registry().get(&app_id)?;
                    match maybe_app {
                        Some(app) => {
                            println!("APPLICATION DETAILS: {}", app.application_id);
                            println!("{}", "=".repeat(60));
                            println!("Name:             {}", app.manifest.name);
                            println!("Version:          {}", app.installed_version);
                            println!("Description:      {}", app.manifest.description);
                            println!("Publisher:        {}", app.publisher);
                            println!("Entrypoint:       {}", app.manifest.entrypoint);
                            println!("Package ID:       {}", app.package_id);
                            println!("Package Hash:     {}", app.package_hash);
                            println!("State:            {:?}", app.lifecycle_state);
                            println!("Signature Status: {:?}", app.signature_status);
                            println!("Installed At:     {}", app.installed_at);
                            println!("Updated At:       {}", app.updated_at);
                            println!(
                                "Permissions (Declared: {}, Authorized: {}):",
                                app.manifest.permissions.len(),
                                app.permissions.len()
                            );
                            for p in &app.manifest.permissions {
                                println!("  - [Declared] {}", p);
                            }
                            for p in &app.permissions {
                                println!("  - [Authorized] {}", p);
                            }
                            let caps = CapabilityContext::new(
                                app.manifest.permissions.clone(),
                                app.permissions.clone(),
                            );
                            println!("Effective Capabilities ({}):", caps.effective().len());
                            for p in caps.effective() {
                                println!("  - {}", p);
                            }
                            let storage_usage = runtime
                                .application_registry()
                                .get_storage_usage(&app_id)
                                .unwrap_or(0);
                            println!(
                                "Storage Quota:     {} / {} bytes",
                                storage_usage, app.resource_limits.max_storage_bytes
                            );
                            println!("Dependencies ({}):", app.dependencies.len());
                            for d in &app.dependencies {
                                println!(
                                    "  - {} ({}, optional: {})",
                                    d.identity, d.version_requirement, d.optional
                                );
                            }
                            let history = runtime.application_registry().get_history(&app_id)?;
                            println!("Rollback History ({}):", history.len());
                            for h in history {
                                println!("  - Version {} (hash: {})", h.version, h.package_hash);
                            }
                            if let Ok(crashes) =
                                runtime.application_registry().get_crash_history(&app_id)
                            {
                                println!("Recent Crashes ({}):", crashes.len());
                                for c in &crashes {
                                    println!(
                                        "  - [{}] Version {}: {}",
                                        c.crashed_at, c.version, c.reason
                                    );
                                }
                            }
                            if let Ok(true) = runtime.application_registry().is_isolated(&app_id) {
                                let reason = runtime
                                    .application_registry()
                                    .get_isolation_reason(&app_id)
                                    .unwrap_or(None);
                                println!(
                                    "Isolation:        QUARANTINED (Reason: {})",
                                    reason.as_deref().unwrap_or("None")
                                );
                            }
                            println!("{}", "=".repeat(60));
                        }
                        None => {
                            println!("Application '{}' not found in registry", app_id);
                        }
                    }
                }
                Err(e) => {
                    println!("Invalid ApplicationId '{}': {}", application, e);
                }
            },
            AppCommand::Verify { package } => match std::fs::read_to_string(&package) {
                Ok(content) => match serde_json::from_str::<ApplicationPackage>(&content) {
                    Ok(pkg) => {
                        println!("VERIFYING APPLICATION PACKAGE: {}", pkg.package_id);
                        println!("{}", "=".repeat(60));
                        match pkg.verify_integrity(None) {
                            Ok(computed_hash) => {
                                println!(
                                    "Manifest:         VALID (schema v{})",
                                    pkg.manifest.schema_version
                                );
                                println!("Application ID:   {}", pkg.manifest.application_id);
                                println!("Version:          {}", pkg.manifest.version);
                                println!("Computed Hash:    {}", computed_hash);
                                match pkg.verify_signature() {
                                    Ok(()) => {
                                        println!(
                                            "Publisher Sig:    VALID (signed by {})",
                                            pkg.manifest.publisher
                                        );
                                        println!("RESULT:           PACKAGE INTEGRITY & PROVENANCE VERIFIED");
                                    }
                                    Err(e) => {
                                        println!("Publisher Sig:    FAILED ({})", e);
                                        println!("RESULT:           PACKAGE SIGNATURE INVALID");
                                    }
                                }
                            }
                            Err(e) => {
                                println!("Integrity Error:  {}", e);
                                println!("RESULT:           PACKAGE INTEGRITY VERIFICATION FAILED");
                            }
                        }
                        println!("{}", "=".repeat(60));
                    }
                    Err(e) => {
                        println!(
                            "Failed to parse application package JSON from {:?}: {}",
                            package, e
                        );
                    }
                },
                Err(e) => {
                    println!("Failed to read package file {:?}: {}", package, e);
                }
            },
            AppCommand::Run { application } => match ApplicationId::new(&application) {
                Ok(app_id) => {
                    println!("EXECUTING APPLICATION: {}", app_id);
                    println!("{}", "=".repeat(60));
                    match runtime.run_application(&app_id).await {
                        Ok(report) => {
                            println!("Execution ID:     {}", report.execution_id);
                            println!("Application ID:   {}", report.application_id);
                            println!("Package ID:       {}", report.package_id);
                            println!("Duration:         {} ms", report.duration_ms);
                            println!("Fuel Consumed:    {}", report.fuel_consumed);
                            println!("Exit Code:        {:?}", report.exit_code);
                            println!("Final State:      {:?}", report.final_state);
                            println!("RESULT:           EXECUTION SUCCESS");
                        }
                        Err(e) => {
                            println!("Execution Failed: {}", e);
                            println!("RESULT:           EXECUTION FAILED");
                        }
                    }
                    println!("{}", "=".repeat(60));
                }
                Err(e) => {
                    println!("Invalid ApplicationId '{}': {}", application, e);
                }
            },
            AppCommand::Stop { execution } => match ExecutionId::new(&execution) {
                Ok(exec_id) => match runtime.stop_application_execution(&exec_id).await {
                    Ok(()) => {
                        println!("Execution '{}' stopped successfully.", exec_id);
                    }
                    Err(e) => {
                        println!("Failed to stop execution '{}': {}", exec_id, e);
                    }
                },
                Err(e) => {
                    println!("Invalid ExecutionId '{}': {}", execution, e);
                }
            },
            AppCommand::Executions => {
                let active = runtime.list_application_executions().await;
                println!("ACTIVE APPLICATION EXECUTIONS ({})", active.len());
                println!(
                    "{:<36} {:<24} {:<12} {:<12}",
                    "EXECUTION ID", "APPLICATION ID", "STATE", "STARTED"
                );
                println!("{}", "-".repeat(84));
                for ex in active {
                    println!(
                        "{:<36} {:<24} {:<12?} {:<12}",
                        ex.execution_id, ex.application_id, ex.lifecycle_state, ex.start_time
                    );
                }
            }
            AppCommand::Start { application } => match ApplicationId::new(&application) {
                Ok(app_id) => {
                    println!("STARTING APPLICATION: {}", app_id);
                    println!("{}", "=".repeat(60));
                    match runtime.start_application(&app_id).await {
                        Ok(report) => {
                            println!("Execution ID:     {}", report.execution_id);
                            println!("Application ID:   {}", report.application_id);
                            println!("Duration:         {} ms", report.duration_ms);
                            println!("Fuel Consumed:    {}", report.fuel_consumed);
                            println!("Exit Code:        {:?}", report.exit_code);
                            println!("Final State:      {:?}", report.final_state);
                            println!("RESULT:           START SUCCESS");
                        }
                        Err(e) => {
                            println!("Start Failed:     {}", e);
                            println!("RESULT:           START FAILED");
                        }
                    }
                    println!("{}", "=".repeat(60));
                }
                Err(e) => {
                    println!("Invalid ApplicationId '{}': {}", application, e);
                }
            },
            AppCommand::StopApp { application } => match ApplicationId::new(&application) {
                Ok(app_id) => match runtime.stop_application(&app_id).await {
                    Ok(()) => {
                        println!("Application '{}' stopped successfully.", app_id);
                    }
                    Err(e) => {
                        println!("Failed to stop application '{}': {}", app_id, e);
                    }
                },
                Err(e) => {
                    println!("Invalid ApplicationId '{}': {}", application, e);
                }
            },
            AppCommand::Restart { application } => match ApplicationId::new(&application) {
                Ok(app_id) => {
                    println!("RESTARTING APPLICATION: {}", app_id);
                    println!("{}", "=".repeat(60));
                    match runtime.restart_application(&app_id).await {
                        Ok(report) => {
                            println!("Execution ID:     {}", report.execution_id);
                            println!("Application ID:   {}", report.application_id);
                            println!("Duration:         {} ms", report.duration_ms);
                            println!("Exit Code:        {:?}", report.exit_code);
                            println!("Final State:      {:?}", report.final_state);
                            println!("RESULT:           RESTART SUCCESS");
                        }
                        Err(e) => {
                            println!("Restart Failed:   {}", e);
                            println!("RESULT:           RESTART FAILED");
                        }
                    }
                    println!("{}", "=".repeat(60));
                }
                Err(e) => {
                    println!("Invalid ApplicationId '{}': {}", application, e);
                }
            },
            AppCommand::Dependencies { application } => match ApplicationId::new(&application) {
                Ok(app_id) => {
                    println!("DEPENDENCY ANALYSIS: {}", app_id);
                    println!("{}", "=".repeat(60));
                    match runtime.resolve_application_dependencies(&app_id).await {
                        Ok(report) => {
                            println!(
                                "Resolved Status:  {}",
                                if report.all_satisfied {
                                    "ALL SATISFIED"
                                } else {
                                    "UNSATISFIED"
                                }
                            );
                            println!("Dependencies ({}):", report.dependencies.len());
                            for d in &report.dependencies {
                                let status = if d.satisfied {
                                    if d.available {
                                        "SATISFIED"
                                    } else {
                                        "OPTIONAL-MISSING"
                                    }
                                } else {
                                    "MISSING/INCOMPATIBLE"
                                };
                                println!(
                                    "  - [{}] {} ({}, {:?}, resolved: {:?})",
                                    status,
                                    d.dependency.identity,
                                    d.dependency.version_requirement,
                                    d.dependency.dependency_type,
                                    d.resolved_version
                                );
                            }
                            if !report.missing_required.is_empty() {
                                println!("Missing Required ({}):", report.missing_required.len());
                                for m in &report.missing_required {
                                    println!("  - {}", m);
                                }
                            }
                            if !report.missing_optional.is_empty() {
                                println!("Missing Optional ({}):", report.missing_optional.len());
                                for m in &report.missing_optional {
                                    println!("  - {}", m);
                                }
                            }
                        }
                        Err(e) => {
                            println!("Dependency Resolution Failed: {}", e);
                        }
                    }
                    println!("{}", "=".repeat(60));
                }
                Err(e) => {
                    println!("Invalid ApplicationId '{}': {}", application, e);
                }
            },
            AppCommand::Update { package } => match std::fs::read_to_string(&package) {
                Ok(content) => match serde_json::from_str::<ApplicationPackage>(&content) {
                    Ok(pkg) => {
                        println!("UPDATING APPLICATION: {}", pkg.manifest.application_id);
                        println!("{}", "=".repeat(60));
                        let mut payloads = std::collections::HashMap::new();
                        let base_dir = package
                            .parent()
                            .unwrap_or_else(|| std::path::Path::new("."));
                        for item in &pkg.items {
                            let item_path = base_dir.join(&item.path);
                            if let Ok(bytes) = std::fs::read(&item_path) {
                                payloads.insert(item.path.clone(), bytes);
                            }
                        }
                        match runtime.update_application(&pkg, &payloads).await {
                            Ok(rec) => {
                                println!("Updated To:       Version {}", rec.installed_version);
                                println!("Package ID:       {}", rec.package_id);
                                println!("Package Hash:     {}", rec.package_hash);
                                println!("State:            {:?}", rec.lifecycle_state);
                                println!("RESULT:           UPDATE SUCCESS");
                            }
                            Err(e) => {
                                println!("Update Failed:    {}", e);
                                println!("RESULT:           UPDATE FAILED");
                            }
                        }
                        println!("{}", "=".repeat(60));
                    }
                    Err(e) => {
                        println!("Failed to parse application package JSON: {}", e);
                    }
                },
                Err(e) => {
                    println!("Failed to read package file '{:?}': {}", package, e);
                }
            },
            AppCommand::Rollback {
                application,
                target_version,
            } => match ApplicationId::new(&application) {
                Ok(app_id) => {
                    let parsed_target = match target_version {
                        Some(ref v) => match AppVersion::from_str(v) {
                            Ok(ver) => Some(ver),
                            Err(e) => {
                                println!("Invalid target version '{}': {}", v, e);
                                runtime.shutdown();
                                return Ok(());
                            }
                        },
                        None => None,
                    };

                    println!("ROLLING BACK APPLICATION: {}", app_id);
                    println!("{}", "=".repeat(60));
                    match runtime
                        .rollback_application_to_version(&app_id, parsed_target.as_ref())
                        .await
                    {
                        Ok(record) => {
                            println!("Rolled Back To:   Version {}", record.installed_version);
                            println!("Package ID:       {}", record.package_id);
                            println!("Package Hash:     {}", record.package_hash);
                            println!("State:            {:?}", record.lifecycle_state);
                            println!("RESULT:           ROLLBACK SUCCESS");
                        }
                        Err(e) => {
                            println!("Rollback Failed:  {}", e);
                            println!("RESULT:           ROLLBACK FAILED");
                        }
                    }
                    println!("{}", "=".repeat(60));
                }
                Err(e) => {
                    println!("Invalid ApplicationId '{}': {}", application, e);
                }
            },
            AppCommand::Isolate {
                application,
                reason,
            } => match ApplicationId::new(&application) {
                Ok(app_id) => match runtime.isolate_application(&app_id, &reason).await {
                    Ok(()) => {
                        println!("Application '{}' quarantined/isolated: {}", app_id, reason);
                    }
                    Err(e) => {
                        println!("Failed to isolate application '{}': {}", app_id, e);
                    }
                },
                Err(e) => {
                    println!("Invalid ApplicationId '{}': {}", application, e);
                }
            },
            AppCommand::Unisolate { application } => match ApplicationId::new(&application) {
                Ok(app_id) => match runtime.unisolate_application(&app_id).await {
                    Ok(state) => {
                        println!(
                            "Application '{}' quarantine lifted. Restored state: {:?}",
                            app_id, state
                        );
                    }
                    Err(e) => {
                        println!("Failed to unisolate application '{}': {}", app_id, e);
                    }
                },
                Err(e) => {
                    println!("Invalid ApplicationId '{}': {}", application, e);
                }
            },
            AppCommand::Uninstall { application } => match ApplicationId::new(&application) {
                Ok(app_id) => match runtime.uninstall_application(&app_id).await {
                    Ok(()) => {
                        println!("Application '{}' uninstalled successfully.", app_id);
                    }
                    Err(e) => {
                        println!("Failed to uninstall application '{}': {}", app_id, e);
                    }
                },
                Err(e) => {
                    println!("Invalid ApplicationId '{}': {}", application, e);
                }
            },
        }
        runtime.shutdown();
        return Ok(());
    }

    println!("Discovery active... Listening for peers on local network.");

    // Handle auto-send or targeted send
    if cli.auto_send || cli.target_peer.is_some() {
        let runtime_clone = Arc::clone(&runtime);
        let target_peer_str = cli.target_peer.clone();
        let message_text = cli.message.clone();

        tokio::spawn(async move {
            info!("Waiting for peer connection before sending message...");

            // Wait until a peer is connected
            let mut connected_peer = None;
            for _ in 0..60 {
                // wait up to 30 seconds
                tokio::time::sleep(Duration::from_millis(500)).await;
                let peers = runtime_clone.get_connected_peers().await;
                if let Some(target_str) = &target_peer_str {
                    if let Ok(target_id) = NodeId::from_str(target_str) {
                        if peers.contains(&target_id) {
                            connected_peer = Some(target_id);
                            break;
                        }
                    }
                } else if let Some(first) = peers.first() {
                    connected_peer = Some(*first);
                    break;
                }
            }

            if let Some(dest_peer) = connected_peer {
                info!(
                    "Sending message to peer {}: \"{}\"",
                    dest_peer, message_text
                );
                match runtime_clone
                    .send_data_message(dest_peer, &message_text)
                    .await
                {
                    Ok(msg_id) => {
                        info!("Message dispatched with ID: {}", msg_id);
                        info!("Waiting for ACK from {}...", dest_peer);
                        match runtime_clone
                            .wait_for_ack(msg_id, Duration::from_secs(10))
                            .await
                        {
                            Ok(status) => {
                                println!("--------------------------------------------------");
                                println!(
                                    "SUCCESS: Delivery Verified! ACK received from peer {}",
                                    dest_peer
                                );
                                println!("Status: {:?}", status);
                                println!("--------------------------------------------------");
                            }
                            Err(e) => {
                                error!("Failed waiting for ACK: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to send data message: {}", e);
                    }
                }
            } else {
                error!("No matching peer connected within timeout");
            }
        });
    }

    if let Some(secs) = cli.run_seconds {
        tokio::time::sleep(Duration::from_secs(secs)).await;
        info!("Run duration ({} seconds) completed. Shutting down.", secs);
    } else {
        // Wait for Ctrl+C
        tokio::signal::ctrl_c().await?;
        info!("Shutdown signal received");
    }

    runtime.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;
    println!("FIELDNET Node stopped cleanly.");
    Ok(())
}
