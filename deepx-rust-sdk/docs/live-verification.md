# Live Verification Examples

[Documentation index](README.md) · [SDK README](../README.md)

## Run against testnet

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
`deepx-sdk-query-account` also accepts `DEEPX_TESTNET_RPC_URL` for an alternate testnet RPC
endpoint and checks its genesis hash before querying account state; see
[examples/query_account.rs](../examples/query_account.rs).
The example modes below cover SDK-owned protocol checks.

## REST verification

`deepx-sdk-verify-rest` ([source](../examples/verify_rest.rs)) covers public `verify_rest_*`
market, trade, lending, book and open-interest programs. Modes are `instruments`, `perp-market`,
`spot-market`, `lending`, `spot-trades`, `spot-trade-history`, `perp-book`, `perp-trade-history`,
and `open-interest`. `perp-trade-history` also checks bounded funding-rate history around the
latest trade.

## Account verification

`deepx-sdk-verify-account` ([source](../examples/verify_account.rs)) covers account, wallet, Spot
order/trade, and liquidation `verify_rest_*` programs and the read-only ownership checks. Modes
are `snapshot`, `directory`, `ownership`, `delegate-ownership`, `delegate-order`, `quota`,
`quota-history`, `invitation-codes`, `balance-changes`, `funding-fees`, `hourly-funding`,
`liquidations`, `liquidation-execution`, `liquidation-keeper`, `spot-adl`, `perp-open-orders`,
`perp-order`, `perp-order-tx`, `wallet-orders`, `wallet-trades`, `spot-wallet`, `spot-orders`,
`spot-order`, and `spot-trades`.

`delegate-order <wallet> <subaccount> [--nonce-ms N] [--submit]` signs a post-only test order with
the configured testnet key and requires an accepted RPC dry run. `--submit` places the order,
verifies finalized dispatch, then cancels it and checks the REST order status. `--nonce-ms N`
uses an explicit timestamp nonce as the placement order ID. If a submitted order still needs
cleanup, use `delegate-order <wallet> <subaccount> --cancel-existing <order-id>
<placement-extrinsic-hash>` after verifying the current order state.

`perp-open-orders <wallet> <market-id>` checks all wallet subaccounts;
`perp-open-orders --subaccount <address> <market-id>` checks one subaccount.
`delegate-ownership <subaccount>` derives the signer from the configured testnet key and
verifies its delegated authority; `ownership <signer> <subaccount>` checks an explicit address.

## WebSocket verification

`deepx-sdk-verify-ws` ([source](../examples/verify_ws.rs)) covers the protocol checks from
`verify_ws_connection`, `verify_ws_public_prices`, `verify_ws_public_subscriptions`, and
`verify_ws_account_balances`, plus open interest, lending rates, candles, and account metrics.
Modes are `connection`, `prices`, `public [--recovery]`, `book <market-id> <depth> [--updates]`,
`spot-live-trades <market-name>`, `balances`, `open-interest [--reconcile]`,
`lending-rates [--reconcile]`, `candles`, `account-metrics`, `perp-open-orders`,
`perp-open-positions`, `spot-open-orders`, `perp-order-history`, `spot-order-history`,
`perp-trade-history`, and `spot-trade-history`.

The open-order and position modes accept `[--wallet] <address> <market-id>`; Spot orders use
`<market-name>` instead. Order-history modes take `<wallet> <market-id|market-name>`.

`perp-trade-history` takes `<wallet> <market-id> [--observe|--rest|--reconcile]`.
The reconcile option matches bounded WebSocket and REST perpetual trades to canonical finalized
runtime events. `spot-trade-history` takes `<wallet> <market-name> [--reconcile|--finalized]`.
The finalized option matches bounded WebSocket and REST Spot trades to canonical finalized
runtime events. Both finalized trade modes validate the RPC role identities and advertised
methods before reading events.

`public --recovery` reconnects and reconciles a bounded REST perpetual trade history against
the fresh WebSocket page.
`book` requests a depth-limited perpetual snapshot using the REST market tick size and checks
the best bid and ask; `--updates` checks three consecutive book updates.
`spot-live-trades` validates a Spot trade subscription against REST market metadata, skips
initial history, and waits for a new execution.

## RPC verification

`deepx-sdk-verify-rpc` ([source](../examples/verify_rpc.rs)) covers the raw RPC portions of
`verify_runtime`, `verify_current_margin`, and the `verify_finalized_*` account reads. Modes are
`signing-vectors`, `runtime`, `current-margin`, `finalized-margin`, `finalized-positions`,
`finalized-orders`, `finalized-perp-history <subaccount> <market-id>`, `finalized-spot-orders`,
`finalized-spot-history <subaccount>`, `finalized-spot-ids <subaccount> [order-id ...]`,
`finalized-lending`, `finalized-account`, `finalized-wallet`, `event-storage <block-number>`,
`capture-events <block-number> <absolute-output-root>`, and
`finalized-trades <block-number> [wallet]`.

`runtime` verifies the submission, watch, and recovery RPC role identities before checking
finalized metadata. The account and current margin modes and `finalized-trades` also check each
role's advertised RPC methods.

The event modes read a finalized block's complete event storage twice and check its canonical
hash. `capture-events` writes immutable spec-376 fixture payloads, the reassembled event value,
and a manifest under the supplied output root. Set `DEEPX_TESTNET_RPC_URL` to use an archive RPC
endpoint for capture; the endpoint must report the approved testnet genesis hash.

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
binding of a finalized Perp cancel signature.

## Guarantee boundaries

The SDK examples use SDK models and protocol operations. The finalized account modes validate
the read-only runtime at the pinned block and verify block-pinned raw reads; `finalized-wallet`
also checks the current REST subaccount directory before and after those reads. `finalized-account`
and `finalized-wallet` check block-selected `eth_call` and agree storage, storage-query, and block
inherent timestamps at that checkpoint. They check endpoint identity and advertised capabilities,
but do not establish historical wallet ownership or application-specific event mapping. The SDK
`public` and `spot-live-trades` modes verify their underlying trade and book protocols without
asserting downstream event mapping.
