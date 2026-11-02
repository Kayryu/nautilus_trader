//! Fresh or explicit nonce no-op transaction workflow.

#[path = "common/noop_flow.rs"]
mod noop_flow;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    noop_flow::run_example(false).await
}
