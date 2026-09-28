use clap::Parser;
use fieldnet_core::NodeId;
use fieldnet_runtime::{FieldnetRuntime, RuntimeConfig};
use std::path::PathBuf;
use std::str::FromStr;
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

use std::sync::Arc;
