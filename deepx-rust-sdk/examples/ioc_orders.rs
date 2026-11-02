//! Perpetual and spot IOC order workflows with explicit submission.

#[path = "common/timestamp_nonce_order_flow.rs"]
mod timestamp_nonce_order_flow;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    timestamp_nonce_order_flow::run(Some("--ioc")).await
}
