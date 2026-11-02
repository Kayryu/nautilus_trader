//! Blocking perpetual order submission through the SDK ticket lifecycle.

#[path = "common/order_ticket_flow.rs"]
mod order_ticket_flow;

fn main() -> anyhow::Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(order_ticket_flow::run(false))
}
