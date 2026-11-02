//! Asynchronous perpetual order submission through the SDK ticket lifecycle.

#[path = "common/order_ticket_flow.rs"]
mod order_ticket_flow;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    order_ticket_flow::run(false).await
}
