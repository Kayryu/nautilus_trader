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

## Live verification examples

The verification examples target DeepX testnet. Most modes are read-only; `delegate-order
--submit` places and cancels a testnet order, and `delegate-order --cancel-existing` submits a
cancellation. Pass a mode and its arguments after `--`:

```bash
cargo run -p deepx-rust-sdk --example deepx-sdk-verify-rest -- perp-market
cargo run -p deepx-rust-sdk --example deepx-sdk-verify-account -- snapshot 0xYOUR_WALLET
cargo run -p deepx-rust-sdk --example deepx-sdk-verify-ws -- prices
cargo run -p deepx-rust-sdk --example deepx-sdk-verify-rpc -- runtime
```

`DEEPX_PROXY_URL` configures the REST and JSON-RPC examples. WebSocket examples connect directly
to the testnet endpoint. These examples require live network access and, for account modes, an
account with relevant testnet history.
The [verification coverage matrix](verification_coverage.md) maps every source program under
`../bin` to its SDK example mode and states where a check remains adapter-specific.

- `deepx-sdk-verify-rest` covers public `verify_rest_*` market, trade, lending, book and
  open-interest programs. Modes are `instruments`, `perp-market`, `spot-market`, `lending`,
  `spot-trades`, `spot-trade-history`, `perp-book`, `perp-trade-history`, and `open-interest`.
  `perp-trade-history` also checks bounded funding-rate history around the latest trade.
- `deepx-sdk-verify-account` covers account, wallet, Spot order/trade, and liquidation
  `verify_rest_*` programs and the read-only ownership checks. Modes are `snapshot`, `directory`,
  `ownership`, `delegate-ownership`, `delegate-order`, `quota`, `quota-history`, `invitation-codes`,
  `balance-changes`, `funding-fees`, `hourly-funding`, `liquidations`,
  `liquidation-execution`, `liquidation-keeper`, `spot-adl`, `perp-open-orders`, `perp-order`,
  `perp-order-tx`, `wallet-orders`, `wallet-trades`, `spot-wallet`, `spot-orders`, `spot-order`,
  and `spot-trades`. `delegate-order <wallet> <subaccount> [--submit]` signs a post-only test order
  with the configured testnet key and requires an accepted RPC dry run. `--submit` places the order,
  verifies finalized dispatch, then cancels it and checks the REST order status. If a submitted
  order still needs cleanup, use `delegate-order <wallet> <subaccount> --cancel-existing <order-id>
  <placement-extrinsic-hash>` after verifying the current order state.
  `perp-open-orders <wallet> <market-id>` checks all wallet subaccounts;
  `perp-open-orders --subaccount <address> <market-id>` checks one subaccount.
  `delegate-ownership <subaccount>` derives the signer from the configured testnet key and
  verifies its delegated authority; `ownership <signer> <subaccount>` checks an explicit address.
- `deepx-sdk-verify-ws` covers the protocol checks from `verify_ws_connection`,
  `verify_ws_public_prices`, `verify_ws_public_subscriptions`, and
  `verify_ws_account_balances`, plus open interest, lending rates, candles, and account metrics.
  Modes are `connection`, `prices`, `public [--recovery]`,
  `book <market-id> <depth> [--updates]`, `spot-live-trades <market-name>`, `balances`,
  `open-interest [--reconcile]`, `lending-rates [--reconcile]`, `candles`, `account-metrics`,
  `perp-open-orders`,
  `perp-open-positions`, `spot-open-orders`, `perp-order-history`, and
  `spot-order-history`, `perp-trade-history`, and `spot-trade-history`. The open-order and position
  modes accept `[--wallet] <address> <market-id>`; Spot orders use `<market-name>` instead.
  Order-history modes take `<wallet> <market-id|market-name>`.
  `perp-trade-history` takes `<wallet> <market-id> [--observe|--rest|--reconcile]`.
  The reconcile option matches bounded WebSocket and REST perpetual trades to canonical finalized
  runtime events.
  `spot-trade-history` takes `<wallet> <market-name> [--reconcile|--finalized]`.
  The finalized option matches bounded WebSocket and REST Spot trades to canonical finalized
  runtime events. Both finalized trade modes validate the RPC role identities and advertised
  methods before reading events.
  `public --recovery` reconnects and reconciles a bounded REST perpetual trade history against
  the fresh WebSocket page.
  `book` requests a depth-limited perpetual snapshot using the REST market tick size and checks
  the best bid and ask; `--updates` checks three consecutive book updates.
  `spot-live-trades` validates a Spot trade subscription against REST market metadata, skips
  initial history, and waits for a new execution.
- `deepx-sdk-verify-rpc` covers the raw RPC portions of `verify_runtime`,
  `verify_current_margin`, and the `verify_finalized_*` account reads. Modes are
  `signing-vectors`, `runtime`,
  `current-margin`, `finalized-margin`, `finalized-positions`, `finalized-orders`,
  `finalized-perp-history <subaccount> <market-id>`, `finalized-spot-orders`,
  `finalized-spot-history <subaccount>`, `finalized-spot-ids <subaccount> [order-id ...]`,
  `finalized-lending`, `finalized-account`, `finalized-wallet`,
  `event-storage <block-number>`, `capture-events <block-number> <absolute-output-root>`, and
  `finalized-trades <block-number> [wallet]`. `runtime` verifies the submission, watch, and
  recovery RPC role identities before checking finalized metadata. The account and current margin
  modes and `finalized-trades` also check each role's advertised RPC methods. The event modes read a
  finalized block's complete event storage twice and check its canonical hash. `capture-events`
  writes immutable spec-376 fixture payloads, the reassembled event value, and a manifest under
  the supplied output root. Set `DEEPX_TESTNET_RPC_URL` to use an archive RPC endpoint for capture;
  the endpoint must report the approved testnet genesis hash.
  `finalized-trades` also decodes perpetual and Spot trades against the runtime metadata at that
  block. With a wallet, it matches perpetual trades against complete REST account trade pages.
  The order-history modes collect IDs from bounded REST history pages, then read those orders from
  one finalized block. Perpetual history also checks identity, quantities, flags, and lifecycle
  fields against the REST records.
  `current-margin` rejects an invalid block hash, repeats the hash-pinned margin read, and checks
  that the observed block remains canonical.
  `signing-vectors` runs offline against captured spec-376 metadata and verifies complete no-op,
  System remark, and perpetual placement extrinsic bytes, EIP-1559 and legacy EVM envelope and
  runtime wrapper bytes, perpetual profit/loss-point precompile hashes, and the runtime/genesis
  binding of a finalized Perp cancel signature. The external Python SDK comparison in
  `bin/verify_sdk_signing.py` still requires its pinned Python source inputs.

The SDK examples use SDK models and protocol operations. The finalized account modes validate
the read-only runtime at the pinned block and verify block-pinned raw reads; `finalized-wallet`
also checks the current REST subaccount directory before and after those reads. `finalized-account`
and `finalized-wallet` check block-selected `eth_call` and agree storage, storage-query, and block
inherent timestamps at that checkpoint. They check endpoint identity and advertised capabilities,
but do not establish historical wallet ownership or the
adapter's complete account and framework mapping evidence. Adapter programs that also
verify NautilusTrader instruments, data client responses, unsubscribe behavior, and shutdown retain
those checks in the adapter crate. The SDK `public` and `spot-live-trades` modes verify their
underlying trade and book protocols without asserting framework event mapping.
