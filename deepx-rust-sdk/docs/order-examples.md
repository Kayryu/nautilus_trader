# Order Examples

[Documentation index](README.md) · [SDK README](../README.md)

## Timestamp nonce orders

[examples/timestamp_nonce_orders.rs](../examples/timestamp_nonce_orders.rs) accepts
`perp <subaccount> <market-id> <long|short> <size-raw> <price-raw>` or
`spot <subaccount> <pair> <buy|sell> <quote-raw> <base-raw>`.
Amounts are exact runtime integers; Spot amounts currently accept values up to `u128::MAX`.
It checks the chain timestamp and signs a post-only GTC order using `--nonce-ms N` or a fresh
nonce. By default it dry-runs only. Explicit `--submit` verifies finalized placement and stored
open state, then signs and verifies a separate cancellation and stored canceled state. A failed
or uncertain step must be reconciled by its printed order ID and extrinsic hash before rerunning.

## Recover an open order

For an order left open after interruption, supply its original market, side, and exact raw amounts
with `--cancel-existing <order-id>`. This checks the finalized order identity and open state, then
dry-runs cancellation. Add `--submit` to send the cancellation and verify its finalized event and
stored canceled state. Confirm the printed placement hash separately before using recovery.

## IOC orders

`--ioc` instead signs a Perp or Spot limit IOC order with post-only disabled. Explicit `--submit`
checks finalized placement and requires terminal order storage with no active-order index entry;
Perp output includes the exact filled raw size. An IOC may execute immediately, so choose the
amounts and limit price accordingly.

[examples/ioc_orders.rs](../examples/ioc_orders.rs) accepts the same six Perp or Spot positional
arguments and inserts `--ioc`; an optional `--nonce-ms N` and `--submit` retain the same behavior.

## Modify an existing order

For an existing GTC order, `--modify-existing <old-order-id> <new-first-raw> <new-second-raw>`
verifies the old order's exact terms and active state, then signs an atomic cancel-and-place
modification. For Perp, the new values are total size including any already-filled size and price;
the replacement size is the exact difference. A total equal to the filled size becomes cancel-only,
while a smaller total is rejected. For Spot, the new values are quote and base amounts, and the old
order must be unfilled. Explicit `--submit` verifies ordered finalized events and both stored order
states, then cancels a resting replacement. The replacement has a new order ID; an uncertain modify
or cleanup step requires reconciliation by its printed hash and order IDs.

[examples/modify_order.rs](../examples/modify_order.rs) accepts those six positional arguments
followed by `<old-order-id> <new-first-raw> <new-second-raw> [--submit]` and inserts
`--modify-existing` before calling the shared workflow.

Submission errors and finality timeouts require reconciliation by the printed extrinsic hash;
the example does not automatically retry or manage durable transaction state. See also the
[Spot modification decoder guarantees](protocols-and-signing.md#spot-modification-decoding).

## Asynchronous, synchronous, and monitored orders

[examples/async_orders.rs](../examples/async_orders.rs) and
[examples/sync_orders.rs](../examples/sync_orders.rs) accept
`<subaccount> <market-id> <long|short> <size-raw> <price-raw> [--nonce-ms N] [--submit]`.
They use `rpc::order_ticket::DeepXPerpOrderTicket` to distinguish hash-verified node acceptance,
provisional placement in a best-chain block, and finalized placement. Both dry-run by default;
`--submit` sends one post-only GTC order and checks its finalized stored state. The synchronous
example blocks on the same async ticket API. A submitted order remains open; cancel it using the
timestamp order example's `--cancel-existing` mode with the original terms and order ID. Timeouts
leave the ticket's known state intact and require reconciliation by the printed extrinsic hash.

[examples/async_order_monitoring.rs](../examples/async_order_monitoring.rs) uses the same arguments
and adds a shared listener plus hash-based ticket lookup for monitored orders.
