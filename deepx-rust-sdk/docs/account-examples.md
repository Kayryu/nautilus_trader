# Account Examples

[Documentation index](README.md) · [SDK README](../README.md)

Examples default to read-only queries or dry runs unless a submission mode is documented below.
Signing and submission are separate; a returned hash does not establish finality. Reconcile
uncertain operations before retrying. See [protocols and signing](protocols-and-signing.md).

## Query an account

[examples/query_account.rs](../examples/query_account.rs) provides a read-only account and chain
query. Its target, `deepx-sdk-query-account`, accepts `DEEPX_TESTNET_RPC_URL` for an alternate
testnet RPC endpoint and checks its genesis hash before querying account state.

## Quota

[examples/quota.rs](../examples/quota.rs) reads the public wallet quota views, finalized-block
System V2 account quota, and the runtime's exact quote-asset amount per purchased quota without a
key. It accepts `--claim <idempotency-key>` for a wallet-owned claim. Claim submission requires
that wallet's signing key in `DEEPX_TESTNET_PRIVATE_KEY`. Persist the idempotency key and reconcile
uncertain delivery before submitting again; see [quota claim methods](protocols-and-signing.md#quota-claims).

`--chain-only` reads the finalized System account and quota price without depending on REST.
`--buy COUNT [--from-subaccount ADDRESS]` signs and dry-runs a purchase using
`DEEPX_TESTNET_PRIVATE_KEY`. Explicit `--submit` submits once, checks the finalized `BuyQuota`
event against the signer, beneficiary, payer, and count, then reads the block-selected quota.
Submission errors and finality timeouts require reconciliation by the printed extrinsic hash.

## Delegates

[examples/delegate.rs](../examples/delegate.rs) lists wallet delegates without a key. With the
wallet owner's `DEEPX_TESTNET_PRIVATE_KEY`, it dry-runs set, disable, enable, or remove operations;
explicit `--submit` sends one operation and verifies finalized events plus the delegate directory.

`--lifecycle NAME --submit` runs set, disable, enable, and remove in sequence, provided the target
delegate does not already exist. A failed or uncertain step requires reconciliation by its
printed extrinsic hash before continuing.

## Leverage

[examples/leverage.rs](../examples/leverage.rs) reads the global, per-market, and effective
leverage caps at one finalized testnet block without a key. Values use the runtime's 1,000-unit
leverage scale. It can sign `--set-global N`, `--clear-global`, `--set-market N`, or `--clear-market`
with `DEEPX_TESTNET_PRIVATE_KEY`, then dry-run the call. Explicit `--submit` sends it once and
checks the finalized dispatch, exact leverage event, and block-selected post-state. Global
settings require the subaccount owner; active trading delegates may set or clear a market override.

## Settle PnL

[examples/settle_pnl.rs](../examples/settle_pnl.rs) signs a permissionless, sequential-nonce PnL
settlement and checks a testnet RPC dry run for one market or all markets. It requires
`DEEPX_TESTNET_PRIVATE_KEY`. Explicit `--submit` sends once, then verifies finalized dispatch and
exact `SettlePnl` events. For one market, `--repeat` submits twice with distinct nonces and prints
both exact settlement amounts. Funding can accrue between the calls, so the second amount need
not be zero.

## Onboarding

[examples/onboarding.rs](../examples/onboarding.rs) lists a wallet's subaccounts without a key.
`--info SUBACCOUNT` verifies one profile; `--orders [MARKET_ID]` reads all open perpetual orders
across the wallet's subaccounts for one or every listed market.

With the wallet owner's `DEEPX_TESTNET_PRIVATE_KEY`, `--create NAME` or `--delete SUBACCOUNT`
signs and dry-runs one operation. Explicit `--submit` sends once, verifies its finalized dispatch
and business event, then checks the wallet directory and created profile.

`--lifecycle NAME --submit` creates a new subaccount and deletes that same empty subaccount after
creation finalizes and the wallet directory reflects it. An uncertain step requires reconciliation
by its printed extrinsic hash before rerunning the command.

## No-op and pending replacement

[examples/noop.rs](../examples/noop.rs) signs a no-op with a fresh timestamp nonce or `--nonce-ms N`.
With `--replace PENDING_HASH`, it checks that the pending extrinsic belongs to the signer, decodes
its nonce from the approved runtime metadata, and signs a no-op at that nonce. Explicit `--submit`
sends once and verifies finalized dispatch while scanning for the original transaction. A failed
submission or bounded finality wait requires reconciliation by the printed no-op hash.

The replacement dry run may be inconclusive while the original occupies the nonce; this is
reported before explicit submission and is not treated as proof of success.
[examples/noop_replace_pending.rs](../examples/noop_replace_pending.rs) accepts
`<wallet> <pending-hash> [--submit]` and routes to that same hash-checked replacement workflow.
