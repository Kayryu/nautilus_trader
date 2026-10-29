# DeepX Rust SDK

Rust types and transport for DeepX REST, JSON-RPC, WebSocket protocol messages, and offline
transaction encoding. The crate does not depend on NautilusTrader runtime crates.

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

Typed REST methods decode market and account responses into SDK-owned models with exact decimal
values. Raw methods preserve JSON numeric lexemes for callers that need to inspect or validate
the original payload. Single-page methods perform one request; bounded history and wallet
collectors read multiple pages or endpoints and return complete SDK-owned types. Wallet account
and position collectors check local account consistency, but the underlying responses are not
block-pinned. Callers own retries and response freshness.

`signing::DeepXOfflinePalletCall` selects typed pallet operations for offline signing. The caller
supplies validated runtime metadata and state to `signing::sign_pallet_call`; the SDK returns
complete extrinsic bytes, hash, signer, and nonce in `DeepXEncodedPalletExtrinsic`. Signing and
transaction submission are separate operations. `signing::sign_offline_evm_transact_extrinsic`
embeds a signed EVM envelope in the runtime's unsigned wrapper using caller-supplied metadata.
`signing::RuntimeSnapshot` binds validated metadata, identity, and Subxt client state. Its
`DeepXRuntimeSnapshotService` tracks in-flight permits, blocks new signing during runtime
refresh, and invalidates permits on connection loss, inconsistent evidence, or client retirement.
The caller must report those observations to the service; creating a snapshot or permit does not
prove an endpoint is current.
`signing::sign_permitted_pallet_call` and `signing::sign_permitted_dynamic_pallet_call` require
an active permit before encoding a signed pallet extrinsic. Their opaque result binds the complete
signed bytes and hash to the approved runtime identity.
Named entry points such as `signing::sign_perp_place_order` and `signing::sign_spot_cancel` use the
same permit-bound path with exact runtime argument types.
A returned transaction hash does not establish inclusion or finality. Callers must independently
verify runtime identity, manage nonces and persistence, and reconcile finalized chain events
before treating an order as accepted or filled.

See `examples/query_account.rs` for a read-only account and chain query.
