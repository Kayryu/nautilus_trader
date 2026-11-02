# DeepX Rust SDK

Rust types and transport for DeepX REST, JSON-RPC, WebSocket protocol messages, and offline
transaction encoding.

- Typed REST market and account models with exact decimal values, plus raw JSON access.
- JSON-RPC and WebSocket protocol operations with live testnet verification examples.
- Offline pallet and EVM transaction encoding, including runtime permit-bound signing.
- Account, order, and EVM bridge examples with explicit submission modes.

## Install

Requires **Rust 1.98 or newer**. To use a local checkout, add a path dependency to your
application's Cargo manifest:

```toml
[dependencies]
deepx-rust-sdk = { path = "/path/to/deepx-rust-sdk" }
```

## Read spot markets

```rust,no_run
use deepx_rust_sdk::{
    deployment::DEEPX_TESTNET_REST_URL,
    rest::{DeepXRestTransport, models::DeepXSpotMarket},
};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let transport = DeepXRestTransport::new(Some(30), None)?;
let markets: Vec<DeepXSpotMarket> = transport
    .get_spot_markets(DEEPX_TESTNET_REST_URL)
    .await?;
println!("{} spot markets", markets.len());
# Ok(())
# }
```

Typed methods return SDK-owned models; raw methods preserve JSON numeric lexemes.
Callers own retries and response freshness. See [REST semantics](docs/protocols-and-signing.md#rest-semantics)
for paging and consistency limits.

## Transaction safety

Offline signing and transaction submission are separate operations. A returned hash does not
establish inclusion or finality, or prove an order was accepted or filled.

Callers must validate runtime identity and account authority, manage nonces, persist transaction
state, and reconcile uncertain delivery and finalized chain events before retrying.
Runtime snapshots and permits do not prove an endpoint is current.

Examples default to read-only previews or dry runs except for their documented submission modes.
Read each workflow's authorization, cleanup, and reconciliation requirements before submitting.

## Documentation

Start with the [documentation index](docs/README.md), or choose a guide:

- [Protocols and signing](docs/protocols-and-signing.md): REST guarantees, quota claims, and runtime permits.
- [Account examples](docs/account-examples.md): queries, quota, delegates, leverage, PnL, onboarding, and no-ops.
- [Order examples](docs/order-examples.md): timestamp nonces, IOC, recovery, modification, and order tickets.
- [EVM bridge](docs/bridge-evm.md): configuration, submission, and destination observation limits.
- [Live verification](docs/live-verification.md): testnet modes, endpoint requirements, and guarantee boundaries.

## Contributing and license

See [CONTRIBUTING.md](CONTRIBUTING.md) for standalone development, format, Clippy, test, and
package checks. The manifest owns package and dependency versions; no parent Cargo workspace is
required for those checks. Publishing to a registry is a separate maintainer action.

Licensed under [LGPL-3.0-only](LICENSE).
