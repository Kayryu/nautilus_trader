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
`post_quota_claim_once` submits a previously signed wallet claim once and returns the full HTTP
response. The caller must persist its idempotency key and reconcile uncertain delivery before
submitting again.
`post_quota_claim` decodes the public task-creation result, while `wait_quota_claim` polls the
internal task endpoint to confirmation, failure, or a bounded timeout. `examples/quota.rs` reads
the public wallet quota views, finalized-block System V2 account quota, and the runtime's exact
quote-asset amount per purchased quota without a key. It accepts `--claim <idempotency-key>` for
a wallet-owned claim. Claim submission requires that wallet's signing key in
`DEEPX_TESTNET_PRIVATE_KEY`.
`--chain-only` reads the finalized System account and quota price without depending on REST.
`--buy COUNT [--from-subaccount ADDRESS]` signs and dry-runs a purchase using
`DEEPX_TESTNET_PRIVATE_KEY`. Explicit `--submit` submits once, checks the finalized `BuyQuota`
event against the signer, beneficiary, payer, and count, then reads the block-selected quota.
Submission errors and finality timeouts require reconciliation by the printed extrinsic hash.

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
`signing::sign_spot_modify_order` encodes a transactional Spot cancel-and-place call with an
explicit replacement order and timestamp nonce. `rpc::spot_modify::decode_spot_modify_inclusion`
binds finalized dispatch to the exact cancellation and replacement events in order; the caller
must still verify the signed extrinsic and canonical block.
A returned transaction hash does not establish inclusion or finality. Callers must independently
verify runtime identity, manage nonces and persistence, and reconcile finalized chain events
before treating an order as accepted or filled.

See `examples/query_account.rs` for a read-only account and chain query.
`examples/delegate.rs` lists wallet delegates without a key. With the wallet owner's
`DEEPX_TESTNET_PRIVATE_KEY`, it dry-runs set, disable, enable, or remove operations; explicit
`--submit` sends one operation and verifies finalized events plus the delegate directory.
`--lifecycle NAME --submit` runs set, disable, enable, and remove in sequence, provided the
target delegate does not already exist. A failed or uncertain step requires reconciliation by
its printed extrinsic hash before continuing.
`examples/leverage.rs` reads the global, per-market, and effective leverage caps at one finalized
testnet block without a key. Values use the runtime's 1,000-unit leverage scale. It can sign
`--set-global N`, `--clear-global`, `--set-market N`, or `--clear-market` with
`DEEPX_TESTNET_PRIVATE_KEY`, then dry-run the call. Explicit `--submit` sends it once and checks
the finalized dispatch, exact leverage event, and block-selected post-state. Global settings
require the subaccount owner; active trading delegates may set or clear a market override.
`examples/settle_pnl.rs` signs a permissionless, sequential-nonce PnL settlement and checks a
testnet RPC dry run for one market or all markets. It requires `DEEPX_TESTNET_PRIVATE_KEY`.
Explicit `--submit` sends once, then verifies finalized dispatch and exact `SettlePnl` events.
For one market, `--repeat` submits twice with distinct nonces and prints both exact settlement
amounts. Funding can accrue between the calls, so the second amount need not be zero.
`examples/onboarding.rs` lists a wallet's subaccounts without a key. `--info SUBACCOUNT` verifies
one profile; `--orders [MARKET_ID]` reads all open perpetual orders across the wallet's
subaccounts for one or every listed market.
With the wallet owner's
`DEEPX_TESTNET_PRIVATE_KEY`, `--create NAME` or `--delete SUBACCOUNT` signs and dry-runs one
operation. Explicit `--submit` sends once, verifies its finalized dispatch and business event,
then checks the wallet directory and created profile.
`--lifecycle NAME --submit` creates a new subaccount and deletes that same empty subaccount after
creation finalizes and the wallet directory reflects it. An uncertain step requires reconciliation
by its printed extrinsic hash before rerunning the command.
`examples/noop.rs` signs a no-op with a fresh timestamp nonce or `--nonce-ms N`. With
`--replace PENDING_HASH`, it checks that the pending extrinsic belongs to the signer, decodes its
nonce from the approved runtime metadata, and signs a no-op at that nonce. Explicit `--submit`
sends once and verifies finalized dispatch while scanning for the original transaction. A failed
submission or bounded finality wait requires reconciliation by the printed no-op hash.
The replacement dry run may be inconclusive while the original occupies the nonce; this is
reported before explicit submission and is not treated as proof of success.
`examples/noop_replace_pending.rs` accepts `<wallet> <pending-hash> [--submit]` and routes to
that same hash-checked replacement workflow.
`examples/timestamp_nonce_orders.rs` accepts `perp <subaccount> <market-id> <long|short>
<size-raw> <price-raw>` or `spot <subaccount> <pair> <buy|sell> <quote-raw> <base-raw>`.
Amounts are exact runtime integers; Spot amounts currently accept values up to `u128::MAX`.
It checks the chain timestamp and signs a post-only GTC order using `--nonce-ms N` or a fresh
nonce. By default it dry-runs only. Explicit `--submit` verifies finalized placement and stored
open state, then signs and verifies a separate cancellation and stored canceled state. A failed
or uncertain step must be reconciled by its printed order ID and extrinsic hash before rerunning.
For an order left open after interruption, supply its original market, side, and exact raw amounts
with `--cancel-existing <order-id>`. This checks the finalized order identity and open state, then
dry-runs cancellation. Add `--submit` to send the cancellation and verify its finalized event and
stored canceled state. Confirm the printed placement hash separately before using recovery.
`--ioc` instead signs a Perp or Spot limit IOC order with post-only disabled. Explicit `--submit`
checks finalized placement and requires terminal order storage with no active-order index entry;
Perp output includes the exact filled raw size. An IOC may execute immediately, so choose the
amounts and limit price accordingly.
`examples/ioc_orders.rs` accepts the same six Perp or Spot positional arguments and inserts
`--ioc`; an optional `--nonce-ms N` and `--submit` retain the same behavior.
For an existing GTC order, `--modify-existing <old-order-id> <new-first-raw> <new-second-raw>`
verifies the old order's exact terms and active state, then signs an atomic cancel-and-place
modification. For Perp, the new values are total size including any already-filled size and price;
the replacement size is the exact difference. A total equal to the filled size becomes cancel-only,
while a smaller total is rejected. For Spot, the new values are quote and base amounts, and the old
order must be unfilled. Explicit `--submit` verifies ordered finalized events and both stored order
states, then cancels a resting replacement. The replacement has a new order ID; an uncertain modify
or cleanup step requires reconciliation by its printed hash and order IDs.
`examples/modify_order.rs` accepts those six positional arguments followed by
`<old-order-id> <new-first-raw> <new-second-raw> [--submit]` and inserts
`--modify-existing` before calling the shared workflow.
Submission errors and finality timeouts require reconciliation by the printed extrinsic hash;
the example does not automatically retry or manage durable transaction state.
`examples/async_orders.rs` and `examples/sync_orders.rs` accept
`<subaccount> <market-id> <long|short> <size-raw> <price-raw> [--nonce-ms N] [--submit]`.
They use `rpc::order_ticket::DeepXPerpOrderTicket` to distinguish hash-verified node acceptance,
provisional placement in a best-chain block, and finalized placement. Both dry-run by default;
`--submit` sends one post-only GTC order and checks its finalized stored state. The synchronous
example blocks on the same async ticket API. A submitted order remains open; cancel it using the
timestamp order example's `--cancel-existing` mode with the original terms and order ID. Timeouts
leave the ticket's known state intact and require reconciliation by the printed extrinsic hash.
`examples/async_order_monitoring.rs` uses the same arguments and adds a shared listener plus
hash-based ticket lookup for monitored orders.
`examples/bridge_evm.rs` implements the pinned Python EVM-to-EVM USDC flow. Set verified
`BRIDGE_SRC` and `BRIDGE_DST` contract addresses, `BRIDGE_AMOUNT_RAW` in six-decimal USDC units,
and `DEEPX_TESTNET_PRIVATE_KEY`; `BRIDGE_SRC_RPC`, `BRIDGE_DST_RPC`, `SIGN_API_BASE`, and
`BRIDGE_RECIPIENT` may override the testnet defaults. It checks both endpoint chain IDs, token
registration, allowance, fee, and authorizer response before the default read-only preview ends. Explicit
`--submit` also requires `BRIDGE_MAX_FEE_PER_GAS_WEI`, `BRIDGE_PRIORITY_FEE_PER_GAS_WEI`,
`BRIDGE_APPROVE_GAS_LIMIT`, and `BRIDGE_OUT_GAS_LIMIT`. It waits for a confirmed approval when
needed, simulates and submits bridge-out, verifies the source receipt, and observes a confirmed
destination `BridgeIn`. `BRIDGE_EXPECTED_AUTHORIZER` verifies a returned signature against the
reported digest; `BRIDGE_EXPECTED_TX_ID` links a destination event to a known bridge protocol ID.
Without that ID, the destination observation is only a matching recipient, token, and amount
candidate. The SDK does not derive the bridge ID from the source transaction yet.
The [Python example coverage checklist](python_example_coverage.md) tracks the remaining upstream
example workflows.

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
`deepx-sdk-query-account` also accepts `DEEPX_TESTNET_RPC_URL` for an alternate testnet RPC
endpoint and checks its genesis hash before querying account state.
The example modes below cover SDK-owned protocol checks. Adapter-specific framework checks remain
in the programs under `../bin`.

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
  and `spot-trades`. `delegate-order <wallet> <subaccount> [--nonce-ms N] [--submit]` signs a
  post-only test order with the configured testnet key and requires an accepted RPC dry run.
  `--submit` places the order,
  verifies finalized dispatch, then cancels it and checks the REST order status. `--nonce-ms N`
  uses an explicit timestamp nonce as the placement order ID. If a submitted order still needs
  cleanup, use `delegate-order <wallet> <subaccount> --cancel-existing <order-id>
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
