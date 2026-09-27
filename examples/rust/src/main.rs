/// Subscribe to nats-lens violation events from Rust.
/// Run: cargo run (with nats-lens already running)
use async_nats::ConnectOptions;
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let nc = async_nats::connect("nats://localhost:4222").await?;
    let mut sub = nc.subscribe("nats.lens.health.violations.>").await?;

    println!("Listening for nats-lens violations...");
    while let Some(msg) = sub.next().await {
        let event: serde_json::Value = serde_json::from_slice(&msg.payload)?;
        let vtype     = &event["violation"]["type"];
        let stream    = &event["stream_name"];
        let consumer  = &event["consumer_name"];
        let severity  = &event["severity"];
        let fix       = &event["violation"]["fix_command"];

        println!("[{severity}] {vtype} on {stream}/{consumer}");
        println!("  Fix: {fix}");
    }
    Ok(())
}
