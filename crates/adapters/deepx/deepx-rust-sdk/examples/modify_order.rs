//! Existing perpetual and spot order modification workflows.

#[path = "common/timestamp_nonce_order_flow.rs"]
mod timestamp_nonce_order_flow;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    timestamp_nonce_order_flow::run(Some("--modify-existing")).await
}
