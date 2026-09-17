# DeepX

## Perpetual close event recovery

`verify_perp_close_inclusion_events` binds complete indexed SCALE events to the durable close
call under the exact approved runtime and timestamp nonce domain. Successful dispatch requires
one `PerpMarket.OrderPlaced`. Outer and nested order IDs agree, but the ID comes from the account
sequence and is not compared with the signed timestamp nonce. Owner and market match the call,
the order is reduce-only, points and post-only are absent, and its initial state is open with
zero filled size and full remaining size. Position-derived size and direction are not inferred
from the close call. Nonzero price matches exactly and uses `Stop`; zero price uses
`Market(slippage)` and a runtime-computed price. Failed dispatch needs no order event.

`collect_finalized_perp_close_recovery_scan` combines these checks with bounded canonical scanning.
`DeepXDurableRecoveryObserver::PerpClose` verifies exact retained signed bytes before recovery RPC;
startup selects it for not-included close records. Unresolved evidence still blocks readiness.
This verifies creation of the closing order, not filled execution or complete position closure.
Metadata-encoded synthetic tests do not replace real inclusion captures or enable live trading.

## TP/SL update event recovery

`verify_perp_profit_and_loss_point_inclusion_events` requires complete SCALE `System.Events`
under the exact approved runtime and timestamp nonce domain. A successful target extrinsic must
emit exactly one matching `PerpMarket.PositionUpdated`: outer owner/market and nested position
owner/market match durable inputs, `take_profit` and `stop_loss` apply the chain's zero-to-`None`
conversion, and event `pnl` is zero. Position direction and financial fields are not inferred
from the operation. A failed dispatch does not require a position update.

`collect_finalized_perp_profit_and_loss_point_recovery_scan` uses bounded canonical scanning.
`DeepXDurableRecoveryObserver::PerpProfitAndLossPoint` verifies exact retained signed bytes before
RPC observation, and startup selects this observer for not-included TP/SL records. Unresolved
records still block readiness. These boundaries do not submit, replay or emit framework order
events. Tests use synthetic metadata-encoded events; real inclusion captures remain outstanding.

## Perpetual placement event recovery

`verify_perp_place_inclusion_events` decodes complete SCALE `System.Events` using an exact
approved runtime snapshot. Only events at the requested `ApplyExtrinsic` index count. Successful
dispatch requires exactly one `PerpMarket.OrderPlaced` matching the durable timestamp order ID
in both event fields, subaccount, market, direction, raw `u128` size, order type, TP/SL options,
reduce-only and post-only flags. Limit price must match. The chain's `place_market_order` computes
market price from the oracle/slippage, so that emitted price is not compared to the input
placeholder or interpreted as a fill price. Initial order state is checked as Open with zero
filled size, full remaining size, and creation block equal to the supplied inclusion block.
Leverage remains runtime-provided rather than inferred from signing inputs. Missing, duplicate,
conflicting, truncated, count-mismatched, or trailing-byte evidence fails closed. Failed dispatch
is recorded without requiring a placement event.

`collect_finalized_perp_place_recovery_scan` uses the existing bounded canonical block/hash and
extrinsic-index scanner, reads events at the exact observed block, and applies that verifier.
`DeepXDurableRecoveryObserver::PerpPlace` adds canonical reconstruction of retained signed bytes
before recovery RPC and uses the hash/checkpoint retained in the acknowledged record. The
execution startup reconciliation path selects this observer for not-included perpetual placement
records. Unresolved or conflicting evidence continues to block startup, never authorizing replay,
replacement nonce allocation, order success, or connected readiness.

These tests use synthetic metadata-encoded events and mock canonical RPC, including both captured
approved runtime versions. They prove local binding and recovery invariants, not successful live
placement, current signer/subaccount authorization, private-event decoding, or live runtime event
conformance. No real transactions were submitted to obtain this coverage.

## Opt-in durable REST submission

`submit_rest_transaction_once(client, prepared)` consumes an acknowledged durable
`DeepXPreparedSubmission`. Its operation identity selects the documented `marketType` and
`action` for perpetual placement, close, TP/SL, cancellation, or Spot placement/cancellation.
The exact signed SCALE bytes are sent to `/internal/v1/chain/tx/transact` once, using the
primary endpoint and a redirect-rejecting transport, without read retry or endpoint failover.

Successful acknowledgement requires `data.tx_hash` to equal the recorded and recomputed
Blake2-256 extrinsic hash. Backend action data is retained separately; it is not verified
inclusion, finality, or business evidence and does not advance the durable record. In particular,
`confirmation: pending` requires the documented status query, not resubmission. Failures after
HTTP starts require reconciliation, including runtime failures returned as HTTP 200: they can
already be included and consume the nonce. Framework trading commands and automatic status
polling in the execution client are not activated by this opt-in API. Tests use local mock servers,
not live transactions.

`get_rest_transaction_status(client, hash)` performs one primary-backend GET for the exact
32-byte hash. `poll_rest_transaction_status` accepts an explicit nonzero attempt budget, positive
poll interval, overall timeout, and cancellation token. Every GET consumes one attempt, without
hidden read retry, redirects, or endpoint failover. Only `pending` or retryable HTTP/API read
failures continue; `best` and `decode_failed` stop, as do unknown/missing confirmation states,
hash mismatch, malformed payloads, and nonretryable errors. Deadline and cancellation cover both
requests and delays. Results retain the latest valid observation when later reads fail or polling
is interrupted. Neither function submits an extrinsic, advances durable lifecycle state, nor
constructs an order report.

Returned hashes must match the query exactly. Order identifiers are parsed from decimal strings
as exact `u64` values; absent/empty IDs remain `None`, while wrong types and overflow fail. The
original JSON payload is retained without rounding uninterpreted numeric fields. The backend's
optional `status` string is retained without invented state semantics. `best` is not finality;
`decode_failed` requires investigation, not replay. On 2026-09-14, a read-only zero-hash probe
returned HTTP 200 with `code: 10020`, `fail: true`, and null data. This is preserved as a lookup
error, never as evidence that a prior submission was unsent or absent from the chain. Successful
live backend status observations remain unverified; local tests cover pending/best/decode-failed
responses and all nine supported direct-operation variants through durable submission acceptance.

## Raw perpetual account records

`get_perp_open_orders_raw`, `get_perp_history_orders_raw`, `get_perp_account_trades_raw`, and
`get_perp_funding_fees_raw` read one account history page for an exact testnet subaccount.
`get_perp_positions_raw` reads one position-lifecycle page and explicitly sends
`addressType=subaccount`; the adapter does not expose the documented wallet aggregation mode.
Requests validate the 20-byte hex address, market filter, nonzero page size, nonempty cursor, side
filter where supported, explicit ascending or descending sort, and representable millisecond
bounds. Open-order reads require a positive market ID: despite the OpenAPI marking both `name` and
`marketId` optional, a read-only testnet probe on 2026-09-15 returned `10001` when both were absent.
The adapter exposes only the verified market-ID path. A trade `orderId` is accepted only as an exact
decimal `u64` together with both the documented market and side fields. Query encoding is typed,
and a response that advertises another page without a usable cursor is rejected.

The internal OpenAPI was re-read on 2026-09-15. It documents the endpoints and examples but still
references the generic `SingleResult` response schema. Consequently, each order remains an
uninterpreted JSON `RawValue`: exact decimal and large-integer lexemes and unknown fields are
preserved without assigning order status, quantity, timestamp, fee, taker, or finality semantics.
Fixture-backed `get_perp_history_orders`, `get_perp_account_trades`, `get_perp_funding_fees`, and
`get_perp_positions` decode observed fields with exact decimals while retaining venue enum values
as strings. Their
bounded typed collectors reject duplicate record identities across page boundaries as well as
ownership, market, filter, financial-value, timestamp, and page-size mismatches without returning
partial results. Raw readers remain available when unknown fields must be retained.
`get_perp_history_order_pages_raw`, `get_perp_account_trade_pages_raw`,
`get_perp_funding_fee_pages_raw`, and `get_perp_position_pages_raw` follow cursors only within an
explicit nonzero page budget and preserve page boundaries. Empty continuation pages, repeated
cursors, and budget exhaustion reject the whole collection without returning partial data. Open
orders remain a single-page read because the API documents no stable snapshot across pagination.
The raw boundary does not itself construct framework reports, authenticate account ownership,
complete mass reconciliation, or enable
execution commands. Read-only probes on 2026-09-15 resolved four subaccounts for the
contributor-provided wallet. Its active subaccount returned nonempty order, trade, and open/closed
position pages for market 3. These responses establish observed wire shapes but not stable business
semantics. Tests use local mock servers and no private credentials.
On 2026-09-16, bounded funding-fee reads returned owner-bound nonempty histories for all four
subaccounts, including signed rates and both long and short position observations.
`get_wallet_funding_fees` and its bounded page collector use the dedicated wallet endpoint's one
global opaque cursor and reuse the exact typed funding-fee record boundary. Returned owners must be
valid AccountId20 values, but the reader does not independently join them to the mutable wallet
directory. A live wallet read on 2026-09-16 validated 41 records across markets 3 and 4 in one page.
This does not establish history completeness or cursor stability.
The wallet-wide trade response groups records by subaccount and can return different nested cursors
while accepting only one request cursor. Without a documented traversal rule,
`get_perp_wallet_trades(request)` exposes one validated grouped trade snapshot and preserves each
nested cursor independently. It validates unique market/subaccount
groups, requested market and time scope, exact trade/order identities and decimals, group-local
ordering, page sizes, cursor metadata, and duplicate trade IDs. It allows zero size because the
captured response contains one such historical record; this raw observation is not a framework
fill report. The response does not echo the requested wallet and is not joined to the separately
mutable subaccount directory, so it does not independently establish ownership.

A read-only run on 2026-09-17 queried the contributor wallet and validated two markets, five
market/subaccount groups, and 22 records. Three groups advertised continuation through three
distinct cursors, directly confirming why no aggregate page collector is exposed. Run
`deepx-verify-rest-wallet-trades <wallet-account-id20> [market-id]` for the same credential-free
single-snapshot check. It sends no credentials, follow-up pagination, or transactions.

The separate wallet-wide order endpoint repeats one consistent global `hasNext` and `nextCursor`
pair in every nested group. `get_perp_wallet_orders(request)` rejects disagreement before
normalizing that metadata. `get_perp_wallet_order_pages(request, max_pages)` follows the global
cursor within an explicit nonzero budget, retains all market/subaccount group boundaries, and
rejects repeated cursors, cross-page composite order identities, group-local ordering violations,
scope mismatches, or budget exhaustion without returning a partial collection. Exact decimal
artifacts such as `2403.9885999999997` remain unchanged. Legacy history includes a zero-sized
filled order, which remains raw venue evidence and is not converted into a framework order report.
The response does not echo the wallet and is not joined to the mutable directory, so it does not
independently prove ownership or become a framework mass-status/external-order response. Run
`deepx-verify-rest-wallet-orders <wallet-account-id20> [market-id]` for bounded credential-free
validation.
`get_perp_order_by_id` decodes and validates one exact decimal order identity, owner, market,
financial fields, and timestamps. The optional `avgFillPrice` and `updatedTime` fields follow the
nullable/omitted OpenAPI cancellation example. Run
`deepx-verify-rest-account-order <subaccount> <market-id> <order-id>` for a credential-free live
check. This typed read does not by itself construct a framework report or imply finality.
`get_perp_order_by_tx` validates an exact 32-byte transaction hash and returns the single associated
typed perpetual order. Run `deepx-verify-rest-account-order --tx <tx-hash>` for a credential-free
live check. This mutable REST association is not canonical inclusion or finality evidence. The
position-order projection returned a fail-closed `503` for observed lifecycle IDs on 2026-09-17,
so it remains disabled along with complete bulk order reconciliation.
`deepx-verify-rest-account-snapshot <wallet-account-id20> [market-id]` performs credential-free
validation for wallet statistics and every subaccount returned by the wallet directory, covering
profile, exact asset balances, equity, and margin ratio. When a market ID is supplied it also
validates each nullable liquidation-price observation. It never submits transactions.
`deepx-verify-rest-funding-fees <subaccount> [market-id]` validates bounded typed subaccount
funding-fee history. Its `--wallet <address> [market-id]` mode validates the dedicated globally
ordered wallet endpoint. Signed fees and rates, `isSettled`, and venue order remain observations;
the reader does not infer payment currency, settlement cadence, account state, or framework P&L
events.
`deepx-verify-rest-balance-changes <wallet|subaccount> <account-id20>` validates bounded typed
balance-change history without credentials. Signed deltas retain their venue sign and are not
converted into current balances. Wallet-scoped pages can contain records for multiple subaccounts.
The cursor-bounded reader observes mutable, non-block-pinned REST pages and does not prove complete
account history, free/locked semantics, or framework account state.
`deepx-verify-rest-liquidation-records <wallet|subaccount> <account-id20>` validates bounded typed
liquidation history without credentials. The four chain variants are closed, and raw integer
amounts, fees, and oracle values retain their protocol units without conversion through floating
point. JSON-encoded liquidation details and canceled-order identities are structurally validated
but remain uninterpreted. Wallet pages can span owned subaccounts, while mutable, non-block-pinned
history does not prove snapshot completeness. No Nautilus liquidation or risk event is constructed.
The execution client uses this fixture-gated conversion boundary for a REST order already bound to
locally tracked or retained terminal `OrderContext`. It accepts only matching `Limit` and `Market`
orders, verifies subaccount, market, both order identities, side, quantity, limit price, post-only,
reduce-only, status/quantity consistency, exact filled-quantity precision, and timestamps, and takes
time-in-force only from immutable local context because REST omits it. System-generated `Stop`
orders, `Adaptive` post-only values, quote-denominated quantities, external or unbound orders,
missing `updatedTime`, and incomplete average-fill evidence fail closed. The single-order framework
report method resolves only a locally bound client or venue order ID, validates any supplied
identity against immutable local context, resolves the market only from the failure-atomic startup
catalog snapshot, and performs the bounded configuration-owned REST read. Startup retains matching
instrument-to-market and market-to-instrument indexes plus perpetual quantity precision and clears
them on reset.

`generate_position_status_reports` performs a bounded current-position read for the configured
subaccount, optionally restricted to one preloaded perpetual instrument. It reads at most 100 pages
of 100 lifecycle records and converts only `Open` records, mapping `isLong`, exact
`baseAssetAmount`, `entryPrice`, and `updatedAt` into a net `PositionStatusReport`. Historical
`Closed` records are omitted. Unknown statuses, missing or inconsistent catalog identities,
duplicate open records for one market, invalid timestamps, and quantity precision loss reject the
whole result. Quantities must also be exact multiples of the startup market increment. Command time
bounds filter the resulting current reports by `ts_last`. The method does not synthesize flat rows
or expose the REST lifecycle ID as a stable venue position ID, and
`provides_bulk_position_coverage` remains false because mutable REST pagination does not establish a
complete snapshot. Multi-order, fill, and mass-status report methods remain non-operational; query
commands, submissions, cancellations, private streaming, and execution connection startup remain
disabled.
The pinned Python SDK contributes signing construction but no account-history mapping evidence. The
chain source confirms the underlying order status, taker, fill-direction, and active-position
storage types, but not REST lifecycle IDs, fee-asset presentation, or historical snapshot rules.

## Operational REST instrument discovery

The Rust `DeepXDataClient` now supports read-only testnet REST connection and perpetual
instrument discovery. `connect` requires an initialized live framework data receiver, loads
both public market lists using the configured retry/failover transport, validates the complete
perpetual instrument set before emitting it, and enables connection-snapshot instrument queries.
`request_instrument` and `request_instruments` preserve request correlation, canonical client
and venue routing, parameters when empty, and the injected framework clock's response timestamp.
Time-bounded history, nonempty filters, foreign identities, unknown instruments, and Spot
instrument construction fail explicitly rather than returning misleading successful results.

Disconnect, stop, reset, and dispose clear readiness and instrument state. Repeated connection
is idempotent while the receiver remains live; reconnection reloads the public catalog. Closed
event receivers revoke readiness. REST readiness does not represent WebSocket connectivity:
supported streaming feeds require an explicit subscription, while unsupported subscriptions fail
closed. Historical bars, trades, and funding rates use separate bounded request paths.

`deepx-verify-rest-instruments` exercises connection, instrument publication, correlated query,
and disconnect against the live testnet. It successfully verified nine perpetual instruments on
2026-09-14 without account access, signing, or submission. The public captured market-list
fixture `perp_markets_spec369.json` includes a provenance sidecar and a checked payload SHA-256;
it is associated with independently captured testnet runtime identity, not pinned to its block.
The OpenAPI was re-read for this path. Existing SDK/Subxt signing and execution gates are unchanged.

## Framework historical perpetual bars

The REST-connected data client supports `request_bars` for known perpetual instruments. Requests
must use a standard, externally aggregated, last-price `BarType` whose interval is one of `1m`,
`3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `8h`, `12h`, `1d`, `3d`, `1w`, or `1M`. The client
preserves correlation, client identity, the requested bar type, optional bounds, and parameters in
an ascending `DataResponse::Bars`. The optional limit may not exceed the venue maximum of 5000.

Each response must carry the instrument's raw venue pair and timestamps inside the inclusive
millisecond REST bounds. OHLC prices and base volume are converted only when exactly representable
at the instrument's declared price and size precision; nonpositive prices, precision loss, invalid
OHLC, negative volume, foreign identity, or an unrepresentable timestamp reject the whole response.
The venue's `time` is preserved directly as `ts_event`. The OpenAPI does not define whether it is a
bucket-open or bucket-close timestamp, so the adapter does not shift it or infer completion. Exact
nanosecond bounds filter the validated millisecond response.

The request uses the same owned, epoch-fenced task dispatcher as trade and funding history. Invalid
bar specifications, nonempty parameters, foreign identities, unknown or Spot instruments, negative
or inverted bounds, and excessive limits fail before HTTP. Transport, identity, bounds, or
conversion failures emit no partial response. Streaming bar subscriptions remain unsupported.
Observed testnet JSON can contain binary-float artifacts such as `2481.3799999999997` for a market
with a `0.01` tick; those rows fail exact framework conversion rather than being rounded.

## Framework historical funding-rate samples

The REST-connected data client supports `request_funding_rates` with correlated
`DataResponse::FundingRates` responses for known perpetual instruments. A read-only testnet probe
on 2026-09-14 confirmed the documented descending `1m` request shape: the response preserved the
market ID, exact decimal string rate, UTC minute timestamps, and continuation cursor.
`get_perp_funding_rates_history_limited(request, limit, max_pages)` retains the fixed range on every
page, shrinks page sizes near the record cap, and enforces market identity, strictly descending
unique bucket times, UTC minute alignment, bounds, page sizes, and cursor progress. Reaching an
explicit record cap succeeds; exhausting the page budget before completion fails. Existing raw
ascending single-page reads remain available and unchanged.

Framework results are chronological samples of the latest rate in each nonempty UTC minute bucket,
not funding payments. Decimal rates, including negative and zero, are retained exactly. Bucket
timestamps become `ts_event`; `interval` and `next_funding_ns` stay `None` because neither the
payment cadence nor next payment is proven. Time conversion rejects UnixNanos overflow. Requests
default to 1000 samples, allow at most 10000, use 100-row pages and a 100-page budget. Missing start
means the epoch; missing end snapshots the injected clock. The REST lower bound includes the
containing minute bucket, then samples are filtered against the original nanosecond bounds.
Original optional bounds and parameters are retained in correlated responses, and async receive
timestamps use the live atomic clock.

Bar, trade, and funding requests share instrument/identity validation and the owned, epoch-fenced
task dispatcher. Nonempty parameters, foreign client/venue, unknown or Spot instruments, negative
or inverted bounds, and excessive record caps fail before HTTP. HTTP, cursor, or conversion
failures emit no partial success. Disconnect/reconnect retires the old epoch before new requests
can emit.
Tests cover precise bounds, recent caps, empty histories, signed exact rates, bucket errors,
budget/limit combinations, invalid inputs, and delayed requests across reconnect. The public
history verification binary compares live raw funding samples to the framework response by both
timestamp and exact rate. No live funding subscription, account access, payment event, or trading
command is enabled by this read-only capability.
A live public run on 2026-09-14 verified three framework funding samples against size-one raw
funding pages by exact bucket time/rate and correlation, confirmed unknown payment schedule fields,
and successfully disconnected. The same run also verified three historical trade ticks. These
read-only observations do not establish a block-pinned snapshot or live account/trading readiness.

## Framework historical perpetual trades

The REST-connected `DeepXDataClient::request_trades` emits a correlated `DataResponse::Trades`
for known perpetual instruments. It requests the most recent matching records over an inclusive
millisecond range, converts the REST decimal execution price and base size at exact instrument
precision, then returns chronological ticks. REST `taker=Buyer/Seller` maps to framework
`AggressorSide::Buy/Sell`, consistent with the node's buyer/seller taker selection; position
`filledDirection` is not an aggressor field. Trade IDs and millisecond execution times are retained.
Unknown takers, zero IDs, nonpositive price/size, unsupported precision, invalid time, and
submillisecond execution times reject the whole response. No leverage or fee scaling is applied.

Requests default to 1000 most recent records, allow at most 10000, and use 100-row pages with a
100-page budget. Explicit record limits can complete without a terminal page; budget exhaustion
before completion is an error. Missing start selects the epoch, missing end snapshots the injected
framework clock at dispatch. Original optional bounds are preserved in the response; ticks are
filtered against exact nanosecond bounds after the inclusive millisecond query. Async response
and tick reception timestamps use the live atomic clock, matching other live REST adapters.
Foreign clients/venues, unknown instruments, Spot, nonempty parameters, negative or inverted time
ranges, and excessive record limits fail synchronously before HTTP dispatch.

Owned request tasks are cancelled on disconnect, stop, reset, dispose, and drop. Response emission
is serialized with connection-epoch retirement; reconnect drains the prior task generation before
opening another. A stale task cannot emit into a new connection. HTTP or conversion failures are
logged without emitting partial success, following the framework's asynchronous request convention.
Tests exercise request correlation, chronological output, recent limits, precise time bounds, empty
responses, malformed records, synchronous validation, and delayed requests across every shutdown
path and reconnect. `deepx-verify-rest-trade-history` now also checks the live framework response,
observed latest trade ID, exact price/size, request correlation, chronological order, and disconnect.
This historical path does not enable streaming, account access, or trading.
A public framework probe on 2026-09-14 returned three chronological ticks over the inclusive
range `1789373271239..=1789373272239`, retained the observed latest trade with unchanged decimal
price/size, matched the request correlation, and disconnected successfully. This is a live
read-only path check, not a block-pinned history or a streaming/trading readiness claim.

## Account-scoped direct-pallet signing and restoration

`DeepXDirectPalletCallVerifier::new(snapshot, key, subaccount)` verifies all six currently modeled
direct-pallet operations through their existing exact-call verifiers: perpetual placement, close,
profit/loss point configuration, cancellation, and Spot placement/cancellation. The selected snapshot,
derived signing account, and explicit AccountId20 subaccount must match the durable identity.
Operation-less legacy records and sequential nonce domains have no fallback. A selected snapshot
does not automatically accept records from another runtime version, even if that version has a
separately approved fixture.

`prepare_signed_direct_pallet_transaction(store, lease, committed_created, record, permit, verifier)`
checks that account scope and delegates to the corresponding existing offline preparation function.
All raw arguments and the timestamp nonce come from the reservation. The original signer lease,
Created acknowledgement, runtime permit, byte binding, and exact Signed compare-and-set acknowledgement
remain mandatory. It never allocates replacement nonces, performs unit conversion, or sends a transaction.

`load_verified_direct_pallet_for_signer(store, lease, verifier)` builds on the ordinary committed-record
loader with account-scope checks for every record and complete canonical byte verification for every
retained signed payload. It returns the whole validated set or an error, never a partial set. Created
records are allowed but remain unsigned. A wrong signer lease fails even for an empty result; valid
hashes and persistence acknowledgements cannot hide bytes encoding a different pallet call.

Tests cover mixed-operation Signed and Created sets, ordinary/fast cancellations, Spot buy/sell,
spec366 and spec369, foreign scopes, operation/nonce/price mutations, wrong-call payloads, legacy
records, sequential nonce rejection, and stale signing acknowledgements. These are offline identity
and durability guarantees, not chain authorization, independent SDK parity for every operation,
business event verification, or live execution readiness. Existing factories and trading command
gates are unchanged, and restoration does not authorize submission or replay.

## Captured spec369 offline runtime support

The testnet runtime identity read on 2026-09-14 is spec369/tx1. The capture tool
verified the expected testnet genesis and canonical finalized block 184886760 at
`0x95febbff91cf1eccbb782b20a36aa42333bce15eebbaef91d0fe7fa55f59afe6`, then
captured block-pinned runtime version and metadata. The exact metadata SHA-256 is
`98136fdbab99332fa40828119c9d53a71a219f3e23155844cc0230cc663cba3c`.

`RuntimeSnapshot::approved_testnet` now accepts that exact spec369/tx1 tuple as well
as the original exact spec366/tx1 tuple. Unknown versions, cross-version metadata,
foreign genesis, changed transaction versions, signed-extension mismatches, and
unsupported nonempty extensions remain rejected. Runtime-change quiescence and
explicit snapshot permits are unchanged; no running client is activated automatically.

Independent SDK complete-byte vectors now cover System remark, Subaccount no-op,
and one maximum-u128 perpetual GTC placement for both snapshots. The offline verifier
accepts `--spec-version 369` to select the new captured fixture. These vectors prove
only the stated encodings, not all calls, authorization, financial units, event success,
inclusion/finality, private streams, or complete live adapter readiness.

## Perpetual placement checkpoint and SDK parity

`prepare_perp_place_reservation` durably reserves a timestamp nonce and all ten raw
`PerpMarket.place_order` arguments before signing. `prepare_signed_perp_place_transaction`
uses `DeepXPerpPlaceCallVerifier` to reconstruct the complete canonical signed extrinsic,
check signer/runtime/nonce binding and byte/hash integrity, and commit the exact Signed
checkpoint. Required and optional u128 financial fields serialize as decimal strings,
preserving zero, null, and the full integer range. The verifier can be explicitly selected
for the existing initial-submission preparation API; no execution command is activated.

The internal OpenAPI read on 2026-09-14 declares fixed chain transaction POST endpoints
with a required `signedExtrinsic` hex property. Account report responses still reference
generic `SingleResult` rather than complete typed financial/status contracts. This inspection
does not supply private-stream captures, account initialization, or finality evidence.

The SDK signing source at GitHub blob `cc85676dee70db35bbd996b560938597c3715558`
provides `build_signed_pallet_call_extrinsic` using an ECDSA keypair and
`substrate.create_signed_extrinsic`. `bin/verify_sdk_signing.py` executes those exact reviewed
builder/key-constructor functions with an offline fixture transport. It proves complete-byte
parity for System remark, Subaccount no-op, and a perpetual GTC limit-placement vector with
maximum u128 size/price/take-profit and absent stop-loss, against captured spec366/tx1 metadata.
The Rust test independently compares native signing with that SDK-produced placement vector.
Parity for other calls, parameter combinations, or unlisted runtimes remains unproven.

Native Rust signing continues to use the DeepX subxt fork pinned at
`2904b84ff5d6646481875e06749460dc5ebc6bbc`. Local `deepx-node` sources corroborate the
placement field order and timestamp-derived order ID, but their spec369 behavior does not
substitute for the captured spec366 runtime. Live streaming, financial unit conversion,
account/authorization validation, reports, and operational activation remain incomplete.

## Explicit verified offline preparation

`prepare_signed_perp_profit_and_loss_point_transaction` adds an offline durable checkpoint
for `PerpMarket.set_profit_and_loss_point`. Its operation-specific reservation retains the
subaccount, market ID, and both raw u128 points; durable JSON uses decimal strings for the
points to preserve their complete range. `DeepXPerpProfitAndLossPointCallVerifier` checks
payload integrity, signer, complete runtime identity, and timestamp nonce, then reconstructs
the canonical signed extrinsic and requires exact byte and hash equality.

The preparation API uses the existing lease, exact Created acknowledgement, verified signing,
and Signed compare-and-set boundary. Stale revisions, wrong identities or keys, conflicting
acknowledgements, and unknown commits release no prepared transaction. No nonce allocation,
submission, automatic replay, live command, financial units, authorization, or business success
is enabled or inferred. Independent SDK parity and event/finality evidence remain gates.

`prepare_signed_transaction_with_verifier` accepts an offline signer and an explicitly
selected `DeepXBusinessCallVerifier`. It validates the signer lease, exact Created
acknowledgement, signed-record invariants, and business-call binding before attempting
the durable Signed compare-and-set. Verification failure performs no signed-record write;
an unknown commit or mismatched acknowledgement releases no prepared transaction.
The original `prepare_signed_transaction` remains a low-level compatibility API and
does not provide business-call verification. Neither entry point authorizes submission,
replay, account access, or operational capabilities.

Maintainer approval was confirmed by the user on 2026-09-12 for continuing implementation.
Existing references below to unresolved approval describe earlier milestone evidence;
protocol parity, authorization, and live conformance gates remain unchanged.

## Explicit offline no-op signing

`sign_no_op(permit, key, nonce)` signs the argument-free `Subaccount.no_op` call
using the approved snapshot and pinned Ethereum-compatible ECDSA signer. Captured
spec366/tx1 metadata (SHA-256
`e6b8b68e26fdd49e47e0af2ce4b6fe947f5d4520cb10171f250665e90e7b1c37`)
declares pallet index 19, call index 28, and no fields: exact SCALE call bytes are
`131c`. Tests cover zero, compact-nonce boundaries, u64 maximum, deterministic signing,
nonce sensitivity, complete-extrinsic regression, and runtime-change permit gating.
Existing permits remain bound to their immutable snapshot during quiescence.

This API performs no nonce allocation, persistence, submission, or RPC access. The nonce
is an exact signed-extension integer, not an inferred timestamp or sequential nonce.
There are no operation flags, options, or enums. No-op replacement, pool behavior,
authorization, business success, inclusion/finality, and independent SDK parity remain
unproven; this does not enable operational replacement or automatic replay. The SDK
reference could not be fetched during this milestone. Local spec369 sources do not
prove spec366 behavior. Maintainer approval and live conformance remain gates.

`prepare_signed_perp_cancel_transaction(store, lease, committed_created, record, permit, key)`
prepares ordinary or fast perpetual cancellations entirely offline. It derives the subaccount,
order ID, market ID, fast-cancel flag, and timestamp nonce from the durable identity, verifies
the signing key and runtime, reconstructs the canonical signed binding, and returns only after
the store confirms the exact `Created` to `Signed` compare-and-set acknowledgement. Stale
revisions, mismatched acknowledgements, and unknown commit outcomes fail closed. It grants no
submission permit, activates no live command, and proves neither independent SDK parity nor
subaccount authorization. Spot cancellation preparation remains unavailable.

## Offline durable Spot cancel preparation

`prepare_signed_spot_cancel_transaction(store, lease, committed_created, record, permit, key)`
derives the subaccount, complete bytes32 pair, order ID, buy/sell and fast-cancel flags, and
timestamp nonce exclusively from the durable reservation. It requires the current signer lease,
exact Created acknowledgment, matching key and approved runtime permit, canonical reconstruction
with `DeepXSpotCancelCallVerifier`, and an exact Signed compare-and-set acknowledgment before
returning. A rejected write, stale revision, unknown commit, or conflicting acknowledgment returns
no prepared transaction. Unknown commits require durable reconciliation, not nonce reuse.

Ordinary and fast buy/sell signing is supported only at this offline checkpoint boundary. It grants
no submission or live execution authority and proves neither independent SDK parity nor subaccount
authorization. Fast Spot cancel inclusion and recovery remain blocked by missing approved spec366
event proof; local spec369 behavior is not equivalent to the captured approved spec366 tx1 runtime.

## Offline subaccount deletion signing

`signing::sign_delete_subaccount(permit, key, params, nonce)` accepts
`DeepXDeleteSubaccountParams` with one exact 20-byte `subaccount` identity.
Captured approved spec366/tx1 metadata (SHA-256
`e6b8b68e26fdd49e47e0af2ce4b6fe947f5d4520cb10171f250665e90e7b1c37`)
declares `Subaccount.delete_subaccount` at pallet 19, call 1, with exactly one
direct `subaccount: H160` field. SCALE call bytes are `1301` followed by those
20 bytes, encoded through native subxt dynamic signing under an explicit permit.

Tests assert the captured metadata contract, exact SCALE encoding, zero/maximum
addresses, zero/63/64/u64-maximum nonces, deterministic signing, address/key/nonce
identity sensitivity, missing/extra/wrong-type/wrong-length arguments, and runtime
change gating. Existing permits remain bound to their immutable approved snapshot.

This API performs no network access, submission, nonce allocation, persistence,
replay, or live activation. No ownership, deletion eligibility, balance constraints,
or business success is inferred. Maintainer approval is confirmed; independent SDK
parity, authorization, business-event/finality evidence, and live conformance remain
gates. Local spec369 sources are not authoritative for this captured spec366 schema.

## Offline wallet delegate removal signing

`signing::sign_remove_delegate_account(permit, key, params, nonce)` accepts
`DeepXRemoveDelegateAccountParams` with the exact 20-byte `delegate` runtime identity.
Captured spec366/tx1 metadata (SHA-256
`e6b8b68e26fdd49e47e0af2ce4b6fe947f5d4520cb10171f250665e90e7b1c37`)
declares `Subaccount.remove_delegate_account` at pallet 19, call 29, with one direct
`delegate: H160` field. Exact SCALE call bytes are `131d` followed by the 20 bytes.
Native subxt dynamic encoding requires an explicit immutable runtime snapshot permit.

Tests verify the decoded metadata contract and exact SCALE bytes, all-zero/all-maximum
identities, zero/63/64/u64-maximum nonces, deterministic signing, delegate/key/nonce
identity sensitivity, malformed arguments, and runtime-change permit gating. Existing
permits remain bound to their approved snapshot during quiescence.

This API performs no network access, nonce allocation, persistence, submission, or live
activation. It infers no ownership, authorization, revocation timing, quota, or financial
semantics. Maintainer approval is confirmed; independent SDK parity, authorization,
business-event/finality evidence, and live conformance remain gates. Local spec369
sources are not substituted for the captured fixture.

## Offline perpetual profit and loss point signing

`signing::sign_perp_set_profit_and_loss_point(permit, key, params, nonce)` accepts
`DeepXPerpProfitAndLossPointParams`: 20-byte `subaccount`, u16 `market_id`, and exact
u128 `take_profit_point` and `stop_loss_point`. Approved spec366/tx1 metadata with
SHA-256 `e6b8b68e26fdd49e47e0af2ce4b6fe947f5d4520cb10171f250665e90e7b1c37`
declares `PerpMarket.set_profit_and_loss_point` at pallet 22, call 13 with those
four direct fields in that order. No direct `PerpMarket.modify_order` is present;
this is the simple management-call fallback, not an order modification API.

Encoding is metadata-driven under an explicit snapshot permit. Focused tests cover exact
SCALE bytes, zero and maximum integers, malformed arguments, each field and nonce changing
signed identity, deterministic signing, and runtime-change permit gating. Local spec369
sources corroborate the signature but do not prove spec366 semantics. These tests provide
offline regression evidence, not independent SDK parity or golden signature vectors.

No financial units, zero-value meaning, trigger behavior, or ownership authorization are
inferred. This API performs no network access, nonce allocation, persistence, submission,
or live activation. Maintainer approval is confirmed; independent SDK parity, authorization,
financial semantics, event/finality evidence, and live conformance remain activation gates.

## Offline perpetual close signing

`signing::sign_perp_close(permit, key, params, nonce)` accepts `DeepXPerpCloseParams`:
AccountId20 `subaccount`, u16 `market_id`, raw u128 `price`, and `Option<u64>` raw `slippage`.
The approved spec366/tx1 metadata declares `PerpMarket.close_position` at call index 14
with those four direct arguments, in that order. The signer uses dynamic metadata encoding
and requires a snapshot-service permit. Tests check exact SCALE fields, integer boundaries,
optional slippage, malformed bindings, signed identity sensitivity, and runtime-change gating.

The proof source is the existing runtime fixture under
`crates/adapters/deepx/test_data/runtime/testnet/` with metadata SHA-256
`e6b8b68e26fdd49e47e0af2ce4b6fe947f5d4520cb10171f250665e90e7b1c37`.
No price or slippage unit conversion is inferred. This offline API does not reserve nonces,
persist, submit, reconcile close events, or enable execution commands. Maintainer approval,
independent SDK vectors, authorization, financial units, and live conformance remain gates.
The local spec369 node is not evidence for spec366 semantics.

`DeepXTransactionIdentity::new_perp_close(..., params)` retains all four raw call arguments
as `DeepXTransactionOperation::PerpClose`. Durable JSON stores price as a decimal string to
preserve the complete u128 range through tagged serde; slippage remains an optional u64.
The explicitly selected `DeepXPerpCloseCallVerifier::new(snapshot, key)` checks byte/hash
integrity, reserved signer, complete runtime identity, and timestamp nonce, then reconstructs
the signed close and requires complete byte and hash equality. Its Debug output redacts the key.
Client order ID, instrument, and order side are local context, not additional close arguments.

`prepare_signed_perp_close_transaction(store, lease, committed_created, record, permit, key)`
is an explicit offline durable checkpoint boundary. It verifies the current signer lease and
exact Created acknowledgement, derives all raw close arguments and the timestamp nonce from
that record, signs against the supplied permit, and verifies exact call binding before durable
compare-and-set. Only an acknowledged Signed record is released; stale revisions, wrong keys,
runtime mismatches, non-close identities, and unknown commit outcomes release no prepared record.
This API neither allocates a nonce nor submits, observes inclusion, or claims business success.
The caller must first durably create the operation-specific reservation under the store lease.

This is offline regression evidence, not an independent golden vector. The default unsupported
verifier, recovery observer, event verification, and ExecutionClient activation are unchanged.
Close event/finality evidence, authorization, financial units, independent SDK parity, maintainer
approval, and live conformance remain required before operational activation.

## Explicit offline durable recovery observer

`reconcile_not_included_checkpoint_with_observer` accepts
`DeepXDurableRecoveryObserver::OrdinarySpotCancel` with a `DeepXSpotCancelCallVerifier`, or
`DeepXDurableRecoveryObserver::SpotPlace` with a `DeepXSpotPlaceCallVerifier`.
The opt-in observer verifies restored durable bytes against the verifier's approved snapshot
and signer before RPC, then uses the corresponding Spot cancel/place finalized collector.
Buy and sell placement both require exact durable terms and side-specific business events. Fast Spot cancels,
foreign runtimes, and mismatched operations or identities fail closed. Canonical checkpoint
requirements and non-atomic submission-pool absence handling are unchanged.

The default observer remains unchanged and does not support Spot inclusion recovery. Execution
startup now explicitly selects operation-specific observers for Spot placement, ordinary Spot
cancellation, and all four perpetual operation families. This adds canonical-byte checks before
not-included recovery RPC; fast Spot cancellation remains explicitly unsupported. Startup still
requires every durable transaction to become complete, and action-required records cannot enable
account registration. Local tests cover normal/corrupt startup records for Spot buy/sell placement,
Spot cancellation and ordinary/fast perpetual cancellation. This API does not submit, replay,
emit order events, or enable live commands.
It establishes no independent SDK golden-vector parity; local spec369 node sources cannot
prove spec366 behavior. Maintainer approval, independent SDK parity, authorization evidence,
and live command wiring remain unresolved.

## Read-only wallet delegate directories

The Rust HTTP client provides typed `get_delegate_accounts(wallet)` and
`get_delegator_accounts(delegate)` readers for both directions of the public delegate directory.
Both require a `0x`-prefixed 20-byte address and send only the documented `address` query parameter
through the shared transport, retry, failover, and venue-envelope checks. Returned addresses are
validated and case-insensitive duplicates fail closed. The original `get_delegate_accounts_raw`
reader remains available for preserving future unknown fields.

A nonempty testnet capture on 2026-09-17 established `delegateAddress`, `delegateName`,
`validUntil`, `mode`, `createTime`, and `active`, plus the reverse wallet-address list. The spec-369
chain source independently defines `valid_until` and `create_time` as Unix milliseconds, permits
zero expiry only for legacy records, and defines the four delegate modes. Typed decoding rejects
unknown modes and nonzero expiry timestamps that do not follow creation. The contributor wallet's
`One-Click Trading` delegate was active in `PlaceOrCancelOrder` mode, and its reverse directory
contained the same wallet.

These REST reads are mutable and not block-pinned. The backend-reported `active` flag is retained
as an observation rather than recomputed or treated as permission. Neither direction proves chain
authorization at signing time, wallet-wide effects, freshness, completeness, or mutation behavior.
No signing lease, execution command, or delegate mutation accepts this REST evidence as authority.

## Read-only account identity and state

The Rust HTTP client provides typed, read-only readers for wallet subaccounts, subaccount profiles,
lending balances, equity, and account-wide margin ratios. `get_wallet_subaccounts(address)` rejects
invalid and duplicate
20-byte subaccount identities. `get_subaccount_info(address, expected_authority)` checks the
returned subaccount and, when supplied, its independently derived signer wallet authority.
`get_subaccount_balances(subaccount)` and `get_subaccount_equity(address)` reject response-address
mismatches, duplicate or empty asset symbols, unsupported asset precision, negative lending
amounts, and negative deposit or borrow totals. Equity and unrealized PnL remain signed.

`get_subaccount_margin_ratio(address)` decodes `collateral`, `marginRequired`, and nullable
`marginRatio` directly into exact decimals and rejects negative values. The refreshed internal
OpenAPI inspected on 2026-09-16 was byte-identical to the prior captured document and describes
the endpoint only as an account margin ratio. It does not identify whether `marginRequired` uses
initial or maintenance weighting. The typed reader therefore does not construct a Nautilus
`MarginBalance`, infer free collateral, or claim account initialization semantics.

`get_wallet_account_snapshot(address)` composes the directory, authority-bound profile, exact
balances, equity, and margin-ratio reads for every returned subaccount. It preserves directory
order and returns no partial collection on failure. Because REST responses are not block-pinned,
this API does not claim cross-request venue snapshot atomicity.

`get_balance_changes(request)` decodes one page of signed exact balance deltas for exactly one
wallet or subaccount. `get_balance_change_pages(request, max_pages)` follows cursors within an
explicit nonzero page budget, preserves page boundaries, rejects duplicate record identities
across pages, and returns no partial collection on failure. Wallet-scoped pages can contain
positions owned by multiple subaccounts. Cross-chain extensions remain raw JSON. These mutable,
non-block-pinned REST observations do not prove complete account history, free/locked semantics,
or current balance state and are not converted into Nautilus account state.

A read-only Rust verification on 2026-09-16 queried the contributor-provided wallet and validated
18 `USDC` records across one page, including funding-fee, settlement, liquidation-fee, and
withdrawal changes. Position-linked records spanned three subaccounts. No credentials or
transactions were sent.

`get_liquidation_records(request)` decodes one page of exact raw-unit liquidation records for
exactly one wallet or subaccount. `get_liquidation_record_pages(request, max_pages)` follows the
opaque cursor within an explicit nonzero page budget, preserves requested time order across page
boundaries, rejects duplicate record IDs, and returns no partial typed collection on failure. The
closed variants are `LiquidatePerp`, `LiquidateSpot`, `PerpBankruptcy`, and `SpotBankruptcy`. Raw
integer amounts, fees, and oracle values retain protocol units; JSON-encoded liquidation details
and canceled-order identities are structurally validated but remain uninterpreted. Wallet scope can
span multiple target subaccounts, and no Nautilus liquidation or risk event is constructed.

A read-only Rust verification on 2026-09-17 queried the contributor-provided wallet and validated
14 liquidation records in one terminal page. The mutable REST history was not block-pinned, so the
observation does not prove snapshot completeness, canonical inclusion, or finality. No credentials
or transactions were sent.

Both JSON number and string financial fields decode directly to exact `Decimal` values. Unknown
`spotPositions` profile entries remain raw JSON rather than acquiring inferred business semantics.
The original `get_subaccount_balances_raw` reader remains available when exact unknown fields must
be retained. Fixtures captured from the contributor-provided testnet wallet on 2026-09-15 and
2026-09-16 cover all five endpoint shapes and carry independently captured spec-369 runtime
provenance.

A read-only Rust verification on 2026-09-16 queried wallet
`0x781ed35b167068c93dfadab41dfb680edaca4e50` and validated all four Active subaccounts. Their
collateral values were `970.96`, `1143.56`, `11.08`, and `1.34`; every observation had
`marginRequired=0.0` and a null `marginRatio`. No credentials or transactions were sent. This
zero-requirement observation does not establish nonzero requirement weighting.

`get_user_stats(address)` validates the wallet's subaccount addresses, rejects duplicates, requires
the current count to match the returned directory, requires the cumulative created count not to be
smaller, and preserves `ifStakedQuoteAssetAmount` as an exact nonnegative decimal. The OpenAPI does
not establish its asset, scale, or account-balance meaning, so no currency or Nautilus balance is
constructed. `get_perp_liquidation_price(request)` requires exactly one market name or ID, binds the
returned subaccount and market identity to that request, and preserves a positive exact price or
`None`. It does not infer liquidation methodology, price units, freshness, position state, or
framework risk semantics.

A live read-only verification on 2026-09-16 returned four current and four cumulatively created
subaccounts, an IF staked quote amount of zero, and a null `ETH-USDC` market-3 liquidation price for
all four subaccounts. No credentials or transactions were sent.

`get_hourly_unsettled_funding(request)` preserves signed position, funding-index, mark-price, and
payment values as exact raw on-chain integers. It validates account and market scope, timestamps,
nonzero boundary values, checked funding-index arithmetic, unique event IDs, strict four-field
keyset order, and the requested page size. `get_hourly_unsettled_funding_pages(request, max_pages)`
advances with the final row's timestamp, market, subaccount, and event ID. It rejects duplicate
events, repeated cursors, and page-budget exhaustion without returning a partial collection. A
full terminal page requires one additional empty request because the response has no `hasNext`
field. The reader does not infer asset precision, settlement, current account state, or framework
PnL and funding events.

Captured spec-369 fixtures prove two exact five-row continuations for market 3. A read-only testnet
verification on 2026-09-17 returned all ten boundaries in the captured inclusive time range, then
accepted an empty terminal page. Run `cargo run -p nautilus-deepx --bin
deepx-verify-rest-hourly-unsettled-funding -- <wallet> <market-id>` to repeat the bounded read. No
credentials or transactions are sent.

The documented `account/perp/position-orders` projection remains disabled. Active and closed known
position IDs returned HTTP service code 503 stating that the projection was unavailable,
incomplete, or its cursor had expired. The adapter therefore exposes no typed success model or
reconciliation behavior for that endpoint.

`get_quota_summary(wallet)` reads the public wallet-level quota aggregate. It preserves Spot,
perpetual, and total USD volume strings as exact decimals, binds the returned owner to the request,
and validates nonnegative additive volume totals plus paired RFC 3339 and millisecond trade
timestamps. Quota counters remain venue observations and are not converted into execution limits,
claim eligibility, or account state. The account snapshot verifier independently requires the
reported subaccount count to match the wallet directory.

A live read-only verification on 2026-09-17 returned four subaccounts, zero Spot volume,
perpetual and total volume `2970.527580000000000000`, earned quota `2970`, granted and reserved
quota zero, and pending quota `2970` for the contributor-provided wallet. No credentials or
transactions were sent. The quota history response was empty; typed history records and cursor
handling remain unimplemented until a nonempty deployment fixture proves the wire representation.
Quota claim creation and quota purchase remain disabled.

The wallet directory result retains the wallet address used for its query rather than returning an
unscoped address list. `get_account_ownership_proof` derives AccountId20 from the local secp256k1 key
and requires that it match the directory wallet and profile authority, that the configured exact
AccountId20 appears exactly once in the directory and matches the profile address, and that the
profile is active. It returns an opaque `DeepXAccountOwnershipProof`. The execution startup gate
accepts only a proof matching its own key and configured subaccount, after runtime validation and
before account-stream confirmation; reset discards the proof so reconnect cannot reuse it.

`DeepXWsAccountConnection` implements the documented credential-free `user_balances` subscription
for one exact AccountId20 under `market: all`. Its request disables compression. The connection
buffers a bounded number of matching updates until the server echoes the exact channel/address
descriptor, then delivers only validated balance snapshots for that confirmed subaccount. A
foreign market, channel, address, acknowledgement, or pre-ack buffer overflow makes the connection
terminal. The WebSocket payload uses the same typed model and validation as
`get_subaccount_balances`; both paths retain financial JSON lexemes as exact `Decimal` values. A
matching acknowledgement returns an opaque connection-owned subscription proof. Only that current
connection can use the proof to upgrade a balance update into a confirmed frame; close or terminal
protocol evidence invalidates it.

The OpenAPI inspected on 2026-09-15 defines no authentication action, challenge, token, or signature
for address-scoped user channels. A live read-only capture from the contributor-provided testnet
subaccount confirmed the acknowledgement and `user_balances` frame shapes. The
`deepx-verify-ws-account-balances <subaccount>` binary verifies that boundary through the Rust
transport without credentials or transactions. Subscription acknowledgement proves only that the
server accepted a public address-scoped read; it does not authenticate signer ownership or
authorize account mutations.

These REST and WebSocket observations are mutable and not pinned to a block hash. They do not prove
snapshot completeness, update ordering, free/locked balances, or margin requirements; initialize a
Nautilus account; construct framework reports; authorize lending mutations; or enable trading
commands. Execution startup accepts the connection-owned subscription proof and confirmed balance
frame, but framework account-state construction and network startup coordination remain
disconnected until their semantics are independently proven.

## Public lending market observations

The Rust HTTP client exposes typed, read-only readers for the public lending asset directory,
borrow-rate curve parameters, APR history, and pool-status history. Decimal strings and numbers are
decoded without floating point. The readers validate market and asset scope, directory and bucket
uniqueness, curve-node pairing and utilization ordering, requested timestamp bounds and response
order, nonnegative APR and quantity observations, positive index prices, and utilization within
`[0, 1]`.
History requests use the closed REST interval enum, an explicit lower bound, an optional upper
bound, an optional limit from 1 through 5000, and explicit ascending or descending order.

Captured testnet fixtures retain the observed asset identities, a USDC rate curve, and three hourly
USDC APR and status rows with provenance manifests and checked payload digests. Run
`cargo run -p nautilus-deepx --bin deepx-verify-rest-lending-markets` to verify the public directory,
curves, and a bounded recent USDC history without credentials or transactions.

A live read-only verification on 2026-09-16 returned three assets and three rate curves for market
1, then validated three recent USDC APR rows and three matching-scope pool-status rows. No
credentials or transactions were sent.

These endpoints provide raw market observations only. The adapter does not infer asset precision or
quantity units, directory or bucket completeness, data freshness, `rho` semantics, APR compounding,
collateral behavior, framework instruments, yields, market-status events, balances, or lending
transactions. The responses are mutable and not pinned to a block hash; no PyO3 lending service is
exposed.

## Perpetual order lookup

The Rust HTTP client provides `get_perp_order_by_id_raw(user, market_id, order_id)` for
read-only `GET /internal/v1/account/perp/order-by-id` requests. It validates the subaccount
address and request bounds, encodes query parameters through the shared HTTP transport,
checks the standard venue success envelope, and returns the exact JSON payload as
`Box<serde_json::value::RawValue>`. HTTP and venue failures remain typed errors; a missing
order is not converted into an empty successful report.

`get_perp_order_by_id` decodes the observed order fields and rejects a response unless its exact
decimal u64 order ID, AccountId20 owner, and market ID match the request. It also validates exact
financial values and timestamp ordering. The OpenAPI inspected on 2026-09-15 shows
`avgFillPrice` as nullable and omits `updatedTime` in its cancellation example, so both remain
optional rather than being fabricated. Venue enum strings remain uninterpreted and the raw reader
remains available for unknown fields.

Neither reader converts the response into a Nautilus order report. The API omits time-in-force
from this representation and does not establish snapshot finality, so this boundary does not
enable trading or execution reconciliation.

DeepX is a decentralized exchange protocol with spot, perpetual, lending, account-management,
quota, delegate, and bridge surfaces. The NautilusTrader integration is restricted to
DeepX testnet. Read-only REST perpetual instrument discovery is available; streaming and
operational trading remain unavailable.

Unproven capabilities remain disabled until captured protocol evidence proves each capability. A published
SDK, permissive schema, successful submission response, or inferred behavior is not sufficient
evidence on its own.

## Implementation status

The public Rust offline `sign_spot_cancel(permit, key, params, nonce)` function accepts
`DeepXSpotCancelParams` with an AccountId20 subaccount, deployment-provided bytes32 `pair`,
u64 order ID, buy/sell flag, and fast-cancel flag. It fixes the reason to `UserCanceled` and
encodes `SpotMarket.cancel_order` dynamically under an explicit runtime snapshot permit.
Focused tests bind the field order and exact SCALE arguments to the approved spec366/tx1
metadata fixture (SHA-256 `e6b8b68e26fdd49e47e0af2ce4b6fe947f5d4520cb10171f250665e90e7b1c37`),
cover both flags and u64 boundaries, reject malformed dynamic bindings, and check that each
operation field and nonce changes signed identity. The local spec369 node is not substituted
for that fixture. These are schema/regression checks, not independent SDK signature or complete
extrinsic parity vectors.

The durable `DeepXTransactionOperation::SpotCancel` identity retains the full pair, subaccount,
order ID, buy/sell flag, and fast-cancel flag through transaction-record serialization.
`DeepXTransactionIdentity::new_spot_cancel` constructs it using `DeepXSpotCancelParams`.
The explicitly selected `DeepXSpotCancelCallVerifier` reconstructs canonical signed bytes using
its approved snapshot and key, checks byte/hash integrity, reserved signer, complete runtime
identity, timestamp nonce, each operation field, and consistency with the Nautilus order side.
Ordinary/fast buy/sell round trips and field, runtime, nonce, key, and malformed-evidence tests
are offline regression evidence, not independent SDK golden vectors or authorization proof.

The explicitly selected offline `verify_spot_cancel_inclusion_events` verifies ordinary Spot
cancel inclusion with complete approved-metadata SCALE decoding, exact extrinsic index, unique
System dispatch evidence, and matching pair, order ID, maker, side, and `UserCanceled` reason.
Failed dispatch is authoritative without a successful business event. Synthetic dynamic metadata
tests are regression checks, not captured independent business-event evidence. The local spec369
node's conditional event emission does not approve spec366 fast-cancel semantics; fast Spot cancel
therefore remains rejected. The explicitly selected read-only Rust
`collect_finalized_spot_cancel_recovery_scan` integrates this verifier with the bounded canonical
recovery collector. It requires an ordinary SpotCancel durable identity and its approved runtime
snapshot, and reads events at the exact canonical block hash and extrinsic index. Successful
dispatch still requires the matching business event; failed dispatch is authoritative without it.
Runtime mismatch and fast or unsupported operations fail before recovery RPC access. A changed
durable checkpoint fails closed, and non-atomic submission-pool absence remains unknown rather
than proving `not-included`. This collector does not commit lifecycle changes or authorize replay.
The default collector, durable execution recovery selection, and live ExecutionClient remain
unchanged; no Spot observer is enabled automatically. Mock scans use metadata-encoded events,
not captured golden vectors or local spec369 approval.

Spot cancel remains non-operational: independent runtime-tagged signing parity,
owner/delegate authorization, independent business
event/finality evidence (including fast-cancel semantics), and execution lifecycle integration
remain gates. The default unsupported verifier and generic successful-inclusion business-event
requirement are unchanged. Framework cancel, modify, close, and other operational order commands
remain unsupported. Maintainer approval remains unresolved; this local milestone is not a PR.

The Rust adapter crate and workspace wiring now exist. The following protocol-core foundations are
implemented and covered by unit tests:

- Testnet-only deployment constants, URL resolution, and strict network configuration. Mainnet and
  unknown environments are rejected before endpoint overrides are applied.
- Forward-compatible environment and product enums.
- Product-aware Spot and perpetual symbol parsing and formatting.
- Exact checked conversion between scaled integers and `Decimal`, without floating point.
- Read-only runtime capture tooling which pins the header and state queries to one finalized block,
  records its hash and decoded block number, and supports a `DEEPX_TESTNET_RPC_URL` endpoint
  override while retaining the hard testnet genesis check. Existing immutable fixtures predate the
  header capture; a replacement fixture set has not yet been captured.
- Structured SCALE V14 metadata decoding which validates the metadata prefix and extracts the
  declared signed-extension order; future finalized captures record that order in their manifest.
- An unauthenticated, read-only JSON HTTP transport built on the shared Nautilus HTTP client, with
  strict relative-path validation, typed failures, bounded retries for transient reads, and
  failover across explicitly configured endpoints.
- Typed public Spot and perpetual market-list reads with venue-envelope validation and exact
  `Decimal` parsing for JSON number and string representations. Response models tolerate unknown
  fields while requiring the observed fields used by the protocol boundary.
- Strict perpetual market conversion into `CryptoPerpetual`, preserving the deployment market ID,
  protocol addresses, exact increments, minimum quantity and notional, margins, and fees. Spot
  conversion remains disabled because Spot metadata has no verified order quantity increment.
- A perpetual-only Nautilus `InstrumentProvider` over the failure-atomic catalog. `load_all`
  converts every perpetual market into a standard `InstrumentStore` under a single captured
  conversion timestamp; Spot entries are skipped under the documented quantity-increment gate and
  explicit Spot construction fails closed with a typed error. Loads are failure-atomic: one
  unconvertible market fails the whole load, `load_ids`/`load` refresh while preserving cached
  instruments and report absent identities, and non-empty filters are rejected.
- Typed single-page perpetual funding-rate, long-short ratio, open-interest, raw trade, raw candle,
  raw mark-price, and raw oracle-price history reads, plus raw perpetual volume statistics and the
  raw current last price, with synchronous parameter validation and exact financial-value parsing.
  Funding-rate, long-short-ratio, and trade responses are rejected when their deployment market ID
  differs from the request. These reads preserve venue response order where applicable and do not
  emit Nautilus data events.
- A failure-atomic public market catalog which loads Spot and perpetual metadata concurrently,
  preserves deployment-provided bytes32 pair and numeric market IDs, and indexes entries by
  canonical product-aware Nautilus identities. It also resolves Spot pair and numeric perpetual
  market IDs back to canonical instrument identities, rejecting duplicate deployment IDs without
  changing any index. A separate perpetual-only `InstrumentProvider` layer builds on it.
- Defensive cursor-pagination state which enforces a local page budget and rejects empty-page
  continuation and repeated cursors without assuming endpoint-specific cursor semantics. The
  cursor-based single-page HTTP methods also reject a response which claims another page without
  supplying a non-empty continuation cursor.
- Transport-neutral WebSocket protocol state with monotonic request correlation, connection-epoch
  ownership, protocol-owner-bound request and authentication capabilities, stale send/response
  isolation, generation-fenced authentication attempts, and desired-versus-confirmed subscription
  intent across reconnect resets.
- Single-decode WebSocket text-frame ingress which correlates only unsigned numeric top-level
  request IDs, preserves valid unknown JSON, returns typed errors for malformed JSON, and can admit
  uncorrelated JSON only under a current protocol-owner-bound authenticated session and connection
  epoch. The resulting envelope carries transport provenance, not a private business-event type.
- An address-scoped, credential-free `user_balances` connection which validates exact AccountId20
  intent, aggregate-market scope, acknowledgement echo, balance payload identity and values, and
  bounded pre-ack buffering. Its opaque proof and confirmed frame are bound to one live connection.
  It has a captured testnet fixture and a live Rust verifier. This is public account-data evidence,
  not authentication, authorization, or framework account state.
- Owned WebSocket task lifecycle with generation-specific cancellation, bounded graceful shutdown,
  forced abort followed by join, rejection of overlapping handler generations, and explicit
  shutdown invalidation of the current generation token even when no task is owned.
- A schema-neutral single-owner WebSocket command loop which serializes request registration,
  matching send-failure or caller-timeout cleanup, bounded response waits, one-decode inbound
  correlation, and connection-epoch resets through a fixed-capacity command queue.
- A zeroizing, redacted secp256k1 private-key boundary with typed validation and testnet environment
  resolution.
- A signer-scoped timestamp nonce allocation policy which restores the maximum reservation from a
  caller-supplied complete durable record set, requires bounded local-to-chain clock drift, rejects
  implausible restored state and overflow, and allocates monotonically under thread contention.
- An execution-client nonce restoration boundary which binds the durable store lease to the
  configured signing key and applies a non-zero clock-drift limit, defaulting to five seconds,
  before returning the allocator and its verified durable record snapshot.
- An opt-in execution-client PostgreSQL runtime owner configured through
  `postgres_cache_database_config`. It requires finalized runtime and account ownership evidence,
  acquires one exclusive signer lease, restores the complete durable signer record set, and retains
  the store, lease, and nonce allocator until reset, stop, or disconnect. Initialization grants no
  signing, submission, or replay authority. Startup mass reconciliation has no external store or
  lease injection path and operates only through this retained runtime.
- A fail-closed reservation preparation boundary which revalidates signer ownership, allocates a
  current Unix timestamp nonce with millisecond precision, durably creates the exact `created`
  record, and releases it only after verifying the store's commit acknowledgement.
- A fail-closed signing preparation boundary which verifies the durable `created` reservation,
  invokes an offline direct-pallet signer, and releases the `signed` record only after a
  revision-checked durable commit.
- A fail-closed reconciliation commit boundary which applies pool, inclusion, finality, complete
  absence, and operator evidence only through an exact acknowledged record and revision-checked
  durable commit.
- A fail-closed one-shot extrinsic submission boundary which submits a signed extrinsic through
  `author_submitExtrinsic` exactly once, and only returns evidence after the node-returned hash,
  the recorded extrinsic hash, and a recomputed Blake2-256 hash of the submitted bytes are all
  equal. Node-reported failures and malformed responses fail the boundary without retry and
  without any automatic `NotSent`/`Ambiguous` classification; callers own the lifecycle decision.
  This boundary is not wired into any execution path and enables no submission capability.
- A fail-closed RPC role identity boundary which requires submission, watch, and recovery endpoint
  observations to match their configured URLs and the approved testnet genesis hash before
  releasing a complete validated endpoint set. All three read-only observations complete before
  deterministic role-attributed error handling and validation.
- A fail-closed RPC method capability boundary which concurrently probes all three
  identity-validated role endpoints and requires the minimum submission, finalized runtime watch,
  and canonical recovery method names. Evidence retains its endpoint URL and cannot satisfy
  execution startup for another validated endpoint set. Advertised names do not prove method
  semantics or authorize transaction operations.
- A read-only finalized runtime snapshot collector which uses the ordinary configured RPC endpoint,
  reads that hash's header, pins runtime-version and metadata reads to the same hash, and returns
  only an approved snapshot paired with its strictly decoded finalized hash and block number. It
  does not install the snapshot, select a mortality period, or authorize signing.
- An explicit one-shot runtime snapshot coordinator which observes through the chain-identity-
  validated Watch endpoint and blocks new signing permits on a runtime-version change before
  awaiting metadata, or a raw metadata hash change before SCALE decoding and fixture validation.
  Failed validation or later RPC errors leave the observed fingerprint pending; old permits stay
  immutable, and observing the old identity does not clear the block. Pre-evidence RPC errors do
  not block signing. Installation still requires a matching fixture-approved snapshot and release
  of all old permits. An unchanged identity is idempotent. This is not a watcher or automatic
  refresh loop and does not enable execution or submit transactions.
- A transport-neutral runtime snapshot service which grants immutable snapshots through counted
  signing permits, blocks new permits as soon as a changed runtime identity is observed, and
  installs a matching fixture-validated replacement only after all old permits are released. The
  public offline signer requires one of these permits rather than accepting a bare snapshot.
  Snapshot construction explicitly rejects non-testnet deployment labels, and the immutable
  runtime identity retains its testnet deployment tag.
- A fixture-derived immutable runtime interface catalog which records pallet, call, event, and
  error names with their declared SCALE indices and rejects duplicate identities. Numeric error
  lookup returns pallet and error identities with distinct unknown-pallet and unknown-error
  failures. This is metadata identity support only, not a `DispatchError` decoder; it does not
  classify transaction outcomes, prove business semantics, or authorize signing.
- A protocol-neutral missed-block recovery boundary which plans bounded contiguous ranges, accepts
  each range exactly once in order, rejects incomplete or non-contiguous block evidence, and
  releases a recovery scan only after every planned finalized block has been collected.
- Fail-closed recovery and reorganization classifiers which require complete canonical evidence,
  exact block and inclusion identity, and authoritative submission-pool absence before producing a
  negative outcome. Missing or conflicting evidence requires operator action.
- A capability-bound reorganization observer which reads the recorded inclusion height from the
  validated Watch endpoint and binds the lookup to the durable extrinsic hash. A changed canonical
  block hash proves reorganization; an unchanged block is canonical only when the transaction
  remains at its recorded index. Missing or displaced evidence requires operator action.
- A PostgreSQL durable transaction store over the existing Nautilus `general` table, with versioned
  exact record envelopes, revision-checked compare-and-set, and detached session advisory locks for
  cross-process signer ownership. Recovery and reorganization decisions use this acknowledged CAS
  boundary.
- A testnet-only execution configuration boundary with an explicit direct-pallet or legacy-EVM
  backend, matching DeepX account identity, mandatory subaccount identity, environment credential
  resolution, and redacted debug output.
- A non-operational Nautilus `ExecutionClient` foundation around `ExecutionClientCore` and
  `ExecutionEventEmitter`. Framework identity, account lookup, account-state emission, and
  idempotent lifecycle methods are wired. A tracked single-order report and bounded current
  perpetual-position and fill reports are available. Account queries replay only the latest
  venue-reported cached state at or after the registered current-startup event. Other unsupported
  order, order-query, and report methods return explicit errors rather than the trait's successful
  no-op or composed defaults; mass-status generation does not invoke granular report methods,
  commission inference cannot fall back to a generic formula, and position reports do not claim
  complete bulk coverage. `connect` does not perform network startup and succeeds only after
  the existing ordered startup gate has already completed. The gate remains disconnected until
  instrument preload, caller-supplied context restoration, runtime validation, signer/subaccount
  ownership validation, account-stream confirmation, account-state initialization, startup mass
  reconciliation, and account registration all have authoritative evidence. Account-state
  initialization records the exact
  event identity for the current startup epoch. The final step verifies that event is present in
  the matching account ID and account type in the shared execution cache and revalidates the exact
  account-subscription proof before marking the client connected. Raw startup evidence advancement
  is crate-private. Account-stream confirmation advances only with a connection-owned subscription
  proof which is still current and matches the owned configured subaccount; account state,
  order-context restoration, and account registration additionally require their dedicated
  verification boundaries. Instrument preload only advances through a
  `DeepXMarketProvider` which
  completed its failure-atomic Spot and perpetual catalog load and contains at least one market. An
  uninitialized provider and a successful but empty catalog return distinct typed startup errors
  without advancing. The provider primary REST endpoint must also match the execution client's
  configured REST endpoint; mismatched evidence fails without exposing either URL. This binds
  startup evidence to configuration but does not independently authenticate the HTTP endpoint.
  Runtime validation advances only with a snapshot token produced after fixture-approved metadata
  was observed at one finalized Watch checkpoint and atomically applied to the shared snapshot
  service. The token's deployment and genesis identity must match the execution configuration and
  chain-identity-validated Submission, Watch, and Recovery endpoint selection. Each role must also
  provide endpoint-bound evidence that `rpc_methods` advertised its minimum required method names.
  This proves endpoint selection, chain identity, and advertised names, not RPC method semantics.
  The approved snapshot authorizes only the direct-pallet backend; legacy-EVM configuration fails
  without advancing until separate fixture evidence establishes its runtime interface.
  This public metadata catalog is not itself a trading-precision source: the perpetual-only
  `InstrumentProvider` built on it constructs `CryptoPerpetual` definitions from catalog metadata,
  and Spot construction remains fail-closed. Startup mass reconciliation advances only while
  holding the transaction store's current lease for the configured signing identity and the exact
  account-subscription proof recorded for this startup epoch. Reconciliation rejects a stale proof
  before loading or mutating durable state and requires the same connection to remain current for
  the complete asynchronous operation. After loading the complete durable record set,
  every durable record's signing-time genesis hash must match the chain identity observed from all
  validated endpoint roles before any recovery RPC; a foreign chain record fails startup without
  mutation. Restored in-block records are first reconciled
  against finalized Watch evidence bound to the execution client's configured endpoint roles and
  advertised method capabilities. A covered finalized checkpoint advances a record only when the canonical block
  hash, extrinsic index, and exact signed-extrinsic hash all match, then persists that transition
  with revision-checked compare-and-set. If that exact finality check instead finds conflicting
  canonical evidence, startup runs the record-bound reorganization coordinator against the same
  restored value. A changed canonical block hash durably removes the stale inclusion and returns the
  record to reconciliation; missing or displaced evidence becomes operator action. Both remain
  unresolved and block startup. A checkpoint below the recorded inclusion remains pending without
  a durable write. Restored `not-included` records resume a bounded finalized scan from their exact
  durable checkpoint. An unchanged checkpoint remains unresolved without a write, while later
  submission-pool presence conflicts with the prior absence evidence and is durably committed as
  operator action. Finding the exact extrinsic still fails closed because authoritative dispatch
  and business-event evidence is not yet available. Startup advances only when every acknowledged
  record is finalized with no remaining recovery, submission decision, reconciliation, or operator
  action. An empty complete record set is valid; endpoint or capability mismatch, signer mismatch,
  stale lease, acknowledgement mismatch, and any unresolved record fail without advancing. This
  includes restored `submitting` and `accepted` records: exact presence in the Submission endpoint's
  valid pending pool durably records acceptance, while absence preserves the unresolved record
  because one non-atomic pool snapshot cannot prove non-inclusion. This proves only durable
  transaction recovery readiness for that signer, not protocol-level account or order
  reconciliation. Cache borrow contention fails with a typed error.
  Disconnect resets the
  complete gate, current account-subscription proof, and event identity.
- A protocol-neutral order-context registry which captures complete shared `OrderContext` values,
  permits idempotent restoration, rejects non-DeepX instrument venues, fails closed on conflicting
  client-order identities, and classifies unknown updates as external. A caller-supplied complete
  context snapshot can include previously verified venue order IDs and is validated before it
  atomically replaces the previous tracked contexts and their bidirectional venue identity
  bindings. Duplicate or externally owned venue IDs fail without mutation or startup advancement;
  an explicit empty snapshot clears active tracked ownership. A cache-backed restoration boundary
  derives this snapshot from open DeepX orders assigned to the configured execution account and
  preserves any cached venue order IDs; cache borrow conflicts fail without startup advancement.
  Individual
  registry population does not prove restoration or authorize event emission. Framework-provided
  reconciled external order identity is subject to the same venue check and can be registered with
  conflict-safe client and venue order ID bindings. Execution updates can be classified atomically
  from either or both IDs; tracked venue identity survives terminal transition until bounded
  ownership eviction, registered external identity is returned explicitly, and IDs which resolve to
  different ownership fail closed. These routing bindings do not authorize typed event emission or
  provide report decoding.
  Finished tracked contexts move atomically into a separate bounded FIFO ownership history so late
  updates remain terminal-owned rather than being misclassified as external. Terminal ownership
  survives reconnect startup resets, conflicts with active restoration and external registration,
  and does not itself choose report, suppression, or event behavior before fixture-backed decoding.
- Protocol-neutral bounded replay state for already validated venue trade IDs. A reservation blocks
  concurrent handling of the same ID, commits only after future routing succeeds, and is released
  when handling fails so a replay can recover the fill. Committed IDs survive reconnect startup
  resets and use FIFO eviction. Replay-state lock failure returns a typed error instead of
  panicking, while reservation cleanup remains best-effort during unwinding. No private trade
  decoder, fill report, or event emission is enabled.
- An execution-client-owned, protocol-neutral fill-report overlap boundary for reports which have
  already passed future fixture-backed decoding. It deduplicates only matching venue trade IDs with
  identical economic evidence, ignores locally generated report identity and initialization
  timestamps, orders output deterministically by event time and trade ID, and rejects conflicting
  evidence. Reports for another account or venue are rejected before overlap processing, with
  deterministic validation precedence and results independent of input order. It is not wired into
  report generation and does not enable private decoding or fill reconciliation.
- An equivalent protocol-neutral order-report overlap boundary for reports which have already
  passed future fixture-backed decoding. It deduplicates matching venue order IDs only when every
  venue-derived field agrees, ignores locally generated report identity and initialization
  timestamps, rejects a client order ID split across venue order IDs, and produces deterministic
  results and validation precedence independent of input order. It does not resolve conflicting
  snapshots by timestamp or source precedence, is not wired into report generation, and does not
  enable private decoding or order reconciliation.

These foundations do not make the adapter operational. Apart from the two public market-list reads,
single-page perpetual funding-rate, long-short ratio, and open-interest history primitives, one
descending raw perpetual trades read with bounded range pagination, single ascending raw candle,
mark-price, and oracle-price pages with explicit intervals, and one raw perpetual volume-statistics window, no
other endpoint-specific HTTP API except the raw perpetual last-price read, live WebSocket transport
or channel, account client, operational execution client, or management
service is enabled. A thin Python package exposes canonical identity constants, validated configs,
and factories. The data client now supports REST connection and perpetual instrument discovery
through framework events and correlated queries; the execution factory remains disconnected.
A perpetual-only instrument provider and fixture-gated offline direct-pallet signing APIs exist,
but the one-shot submission primitive is not wired into an execution command path.
Authoritative venue rate-limit policy, automatic history pagination, and other business response
schemas remain unimplemented. The execution client foundation implements the Nautilus execution
trait but does not coordinate any network connection. Possessing or loading a private key does not enable
trading or transaction submission.

## Plan progress

The repository contains the `nautilus-deepx` crate, Rust workspace and adapter test-inventory
wiring, runtime fixture capture and protocol-core code, and this capability document. The
implementation maps to the integration plan as follows:

### Phase advancement decision

As of 2026-09-10, the implementation focus can advance from the pending-pool recovery milestone to
**Phase E execution and reconciliation**. The durable store, signer lease, exact transaction
identity, one-shot submission, canonical scan, finality, reorganization, and fail-closed startup
boundaries are sufficient foundations for developing the execution client. This does not mark
Phase D complete, enable an order capability, or authorize Phase F management services.

Further attempts to infer authoritative non-inclusion from a non-atomic pending-pool snapshot are
not a prerequisite for Phase E implementation. A submission retry must reuse the exact durably
recorded signed extrinsic and therefore the same transaction hash, be bounded, and stop on an
authoritative rejection or exhausted ambiguity budget. It must not allocate a new timestamp nonce,
create another order identity, rebuild or re-sign the call, or emit a rejection merely because the
retry budget was exhausted. Remaining ambiguity is retained for startup reconciliation or operator
resolution. A bounded coordinator now consumes the single durable submission permit and can retry
only caller-classified ambiguous attempts. Every attempt receives the exact same signed bytes and
hash; `not-sent`, authoritative rejection, hash mismatch, and budget exhaustion stop immediately.
A dedicated persistence boundary can bind verified acceptance to the exact `submitting` record and
commit `accepted` with revision-checked compare-and-set. No protocol-proven transport classifier,
retry delay policy, or execution-command wiring exists, so these foundations do not make submission
operational.

The next protocol milestone is the next minimal fixture-backed Phase E execution slice: establish
initial account-state semantics from authoritative venue evidence and complete deterministic
order/report reconciliation for one fixture-proven order capability.
Before advancing to Phase F management services or operational Python examples, the repository
still requires:

- A complete immutable finalized runtime fixture with decoded signed extensions and the header
  number needed for mortality/checkpoint construction.
- Sanitized byte-for-byte direct-pallet order signing vectors and authoritative dispatch plus
  business-event fixtures matched by block extrinsic index.
- Fixture-backed canonical inclusion, dispatch and business events, finality, reorganization, and
  restart reconciliation evidence; current mock tests establish local invariants only.
- Verified initial account-state semantics, followed by network startup coordination and
  deterministic report reconciliation through the `ExecutionClient`.

Phase E preparatory implementation can continue, but its operational capabilities remain blocked
by the evidence listed above. Phase F management services and operational Python examples remain
blocked. The completed Phase G
config, factory, and package projection does not remove the unresolved Phase D and Phase E evidence
gates or enable a live capability. The external Phase A maintainer-approval and competing-work
checks also remain unrecorded, so no milestone is ready for public submission as an approved
integration.

- **Phase A - Partial:** Capability matrix, hard gates, runtime capture tool, and
  runtime-identity fixtures exist.
- **Phase B - Partial:** Crate and workspace wiring, common types, credentials, HTTP read
  transport, pagination, and transport-neutral WebSocket state exist.
- **Phase C - Partial:** Typed public Spot and perpetual market-list reads, a read-only metadata
  catalog, raw chain `SpotMarketSpec` retrieval, strict perpetual `CryptoPerpetual` conversion,
  typed public lending asset, rate-curve, APR-history, and pool-status readers,
  typed single-page perpetual funding-rate, long-short ratio, and open-interest history reads,
  a typed raw perpetual trades read with bounded range pagination, typed single-page raw perpetual
  candle, mark-price history, and oracle-price history reads with closed interval selection, a typed raw perpetual
  volume-statistics read, and a typed raw perpetual last-price read exist. A perpetual-only
  Nautilus `InstrumentProvider` populates a standard `InstrumentStore` from the catalog with
  failure-atomic loads, preserved cached instruments on refresh, and fail-closed Spot
  construction. A read-only REST data client now publishes perpetual instruments and handles
  connection-snapshot instrument requests and bounded historical perpetual bar, trade, and funding
  sample requests, plus current one-shot perpetual L2 book snapshot requests.
  Public trade, quote, and configured L2 book subscriptions have bounded recovery; bar requests
  remain REST-only.
- **Phase D - Partial:** A fixture-backed immutable runtime snapshot explicitly validates and
  retains the testnet deployment tag, then validates the testnet genesis, approved runtime
  versions, exact metadata SHA-256, ordered signed extensions, and unknown extension encodings. It
  retains fixture-derived pallet, call, event, and error names with their declared SCALE indices
  behind typed fail-closed lookups, including numeric pallet/error identity lookup without
  dispatch-error decoding or transaction-outcome changes. The pinned DeepX Subxt fork provides an
  AccountId20/Keccak ECDSA offline dynamic-call signer with an explicit caller-supplied nonce. A
  signer-scoped timestamp nonce policy restores its high-water mark from durable records,
  calibrates against caller-supplied chain time, and allocates monotonically without rollback.
  Reservation preparation revalidates the signer lease and durably creates the exact `created`
  record before exposing it for later signing. Signing preparation verifies that committed
  timestamp reservation and persists matching signed bytes with CAS before exposing the `signed`
  record. Business-call binding for signed extrinsics has two fixture-gated verifiers.
  `DeepXRemarkCallVerifier` binds durable signed bytes to the reserved transaction identity
  (client order ID, instrument, side, timestamp nonce, and runtime spec version) by
  deterministic re-signing equality of a canonical identity-derived `System.remark` payload
  against the approved fixture snapshot. `DeepXPerpCancelCallVerifier` performs the same exact
  comparison for an ordinary or fast `PerpMarket.cancel_order`, binding the signer, subaccount,
  order ID, market ID, fast-cancel flag, timestamp nonce, and approved runtime. Both reject any
  runtime the snapshot does not cover and the unproven sequential-nonce domain.
  Perpetual cancel verification also checks durable byte/hash integrity before re-signing.
  Offline regression tests reject empty, truncated, trailing, and signature-corrupted payloads,
  changed runtime identity components (including spec 369 against the spec-366 snapshot), and
  mismatched timestamp or sequential nonce reservations. This adds no order-command capability.
  Every other DeepX
  order call remains unsupported pending authoritative golden vectors. A fixed testnet
  `System.remark` regression vector now pins the
  exact signer payload, AccountId20, Keccak ECDSA signature envelope, complete version-4
  extrinsic, and Blake2 extrinsic hash produced by the approved runtime fixture and pinned DeepX
  Subxt revision. The approved spec-366 metadata and current chain source agree that
  `Subaccount.no_op` is the zero-argument pallet/call index `19/28`; a second fixed regression
  vector pins its timestamp-nonce signer payload and complete signed extrinsic. Current chain
  source declares spec version 369, so its other call schemas are not assumed to apply to the
  immutable spec-366 fixture. These vectors detect local encoding drift but are not independently
  produced SDK or venue order vectors and do not prove no-op pool replacement or acceptance
  semantics, so the fail-closed `DeepXUnsupportedBusinessCallVerifier` remains the default
  verifier. Canonical perpetual-cancel event verification requires the exact extrinsic index and
  System dispatch outcome; ordinary cancellation additionally requires the matching
  `PerpMarket.OrderCancelled` business event, while fast cancellation follows the runtime and SDK
  inclusion-only contract because that path suppresses the pallet event. Authoritative pool,
  inclusion, finality, complete absence,
  exact best-block reorganization, and operator evidence can be applied through an exact
  acknowledged record and committed with CAS; signing and submission-start observations cannot
  bypass their dedicated preparation boundaries. Reorganization evidence must identify the exact
  non-finalized inclusion, is retained durably, and returns the transaction to reconciliation
  without authorizing replay. Missed finalized blocks can be planned as bounded contiguous ranges
  and collected in order through a single-owner fail-closed boundary before recovery evidence is
  classified. The PostgreSQL store implements exact versioned envelopes, revision-checked CAS, and
  cross-process signer advisory locks over Nautilus's existing general storage table.
  A transport-neutral snapshot service blocks new signing permits after a changed runtime identity
  is observed and prevents replacement until every permit for the old immutable snapshot is
  released. It accepts only an already validated snapshot matching the observed identity, and the
  public offline signing entry point cannot bypass this permit boundary.
  A read-only collector can construct the approved immutable snapshot from genesis, finalized-head,
  finalized-header, pinned runtime-version, and pinned metadata reads against the ordinary
  configured RPC endpoint. It returns the snapshot with the strictly decoded checkpoint hash and
  block number, but the committed fixture still predates header capture, so mortal signing remains
  disabled. An explicit one-shot coordinator observes finalized runtime versions and metadata only
  through the identity-validated Watch endpoint. It detects version or raw metadata hash changes,
  including unknown upgrades, and blocks new signing permits before fixture validation. Later
  failures leave the change latched. It atomically applies only a matching fixture-approved
  snapshot after all old permits are released. It does not poll, retry automatically, approve
  unknown fixtures, automatically resume signing after a failed refresh, or weaken permit
  quiescence. No live
  runtime watcher, order-call model, golden signing vector, configured chain time source, or live
  recovery event decoder exists. A one-shot submission primitive and durable transaction tracker
  exist behind fail-closed preparation and signer-lease boundaries, but no execution command path
  can invoke them. A read-only RPC
  identity collector concurrently queries every configured role endpoint
  for its genesis hash and releases only a complete validated endpoint set. An independent
  read-only capability probe can then require a caller-supplied non-empty method set for one role
  and fails closed unless `rpc_methods` advertises every name. A complete collector concurrently
  applies the adapter's minimum per-role method policy and binds the resulting evidence to each
  endpoint URL; execution startup requires that complete evidence. This does not prove method
  semantics or authorize submission, watching, or recovery. A 2026-09-01 attempt to capture a
  replacement finalized-header fixture failed before any RPC response because the testnet endpoint
  closed the TLS connection.
- **Phase E - Partial:** A strict execution config and disconnected `ExecutionClient` foundation
  exist. The client exposes framework identity, account lookup, account-state emission, idempotent
  lifecycle methods, a strict tracked single-order status report, bounded current perpetual-position
  reports, bounded perpetual fill reports, current-epoch cached account-state queries, and explicit
  unsupported errors for order, order-query, bulk-order, and mass-status commands. It
  reports incomplete bulk-position coverage and cannot coordinate a network connection. The client
  owns `ExecutionClientCore`, `ExecutionEventEmitter`, a redacted credential, and an ordered
  startup gate which requires instrument preload, caller-supplied context restoration, runtime
  validation, signer/subaccount ownership validation, account-stream confirmation,
  account-state initialization, startup mass reconciliation, and account registration before
  connected state. It
  also owns a conflict-safe shared order-context
  registry for tracked/terminal/external update routing, including conflict-safe framework-provided
  external client and venue order identity bindings. External bindings remain report-routed and
  cannot be promoted to tracked ownership without complete shared `OrderContext`. Instrument
  preload now requires the failure-atomic public market provider to be initialized, non-empty, and
  bound to every configured REST failover endpoint before that startup step advances. It atomically
  retains both perpetual market identity directions for report lookup. Its restoration
  boundary validates and atomically installs a complete caller-supplied replacement snapshot before
  advancing startup. It can derive the active snapshot from the shared execution cache by configured
  account and venue, but it does not verify database restoration, cache provenance, or venue-side
  completeness. Account-state initialization requires a confirmed balance frame and revalidates
  its exact connection-owned subscription immediately before event dispatch, so a closed
  connection or a frame from another connection cannot reuse the prior proof or advance startup.
  The `AccountState` remains
  caller-supplied because the proven `user_balances` schema does not expose free/locked balances or
  margin requirements. The OpenAPI exposes address-scoped account channels without an
  authentication handshake; their acknowledgement is subscription evidence, not signer
  authentication or mutation authorization.
  Startup mass reconciliation holds that exact account-subscription proof across durable recovery
  and rejects a stale proof before loading or mutating transaction state. Its final startup boundary
  revalidates the proof and verifies that the exact account-state event recorded for the current
  startup epoch is present in the matching cached account history. This rejects stale account
  entries from a previous startup epoch, but it does not prove the protocol-dependent semantic
  completeness of that account state.
  Bounded trade-ID replay state supports reserve, commit, and failure release for an already
  validated venue `TradeId`; committed IDs survive reconnect startup resets until FIFO eviction.
  Validated fill reports are merged across bounded pagination overlap only when the
  same `TradeId` carries identical economic evidence; conflicts fail closed and output ordering is
  deterministic. The fill report method reads at most 100 pages of 100 account trades for the
  configured subaccount and applies optional instrument, venue-order, and exact nanosecond time
  filters. It resolves every record through the immutable startup market catalog, preserves exact
  price and quantity increments, and attaches known tracked, terminal, or registered external
  client identity only when its instrument and available side agree.
  Current testnet evidence across buyer and seller taker trades and maker rebates shows REST `fee`
  as a signed account-balance delta. The conversion negates it into Nautilus commission, resolves
  an empty `feeAsset` from the market quote currency, and rejects fee assets or fee signs that
  conflict with startup market metadata. The chain implementation independently names the market
  quote asset for both maker and taker fees. These observations do not prove a block-pinned history
  snapshot, completeness under concurrent writes, or private-stream reconciliation.
  Already validated order status reports have the same exact-overlap boundary keyed by
  `VenueOrderId`, with unique non-empty `ClientOrderId` ownership. It compares all venue-derived
  fields, so lifecycle progression, complementary partial fields, and competing source snapshots
  deliberately fail closed until fixtures establish completeness and precedence semantics. Local
  report identity and initialization time do not affect evidence equality. The single-order report
  method uses this conversion only for a locally tracked or retained terminal order with an exact
  venue binding and preloaded perpetual market ID; bulk merge orchestration remains disconnected.
  The WebSocket single-owner path can now preserve an uncorrelated JSON frame in an authenticated
  envelope only when both its opaque session capability and ingress connection epoch are current.
  Correlated responses are completed through their request waiter and are not emitted through this
  path. This proves transport provenance only: the envelope is not connected to a fixture-backed
  private payload decoder, trade replay state, or execution event route.
  Startup mass reconciliation can restore acknowledged durable transaction records, reconcile
  in-block finality and reorganization evidence, resume an exact not-included checkpoint, and
  observe submitting or accepted records against the Submission endpoint's valid pending pool.
  Exact pool presence durably records acceptance; pool absence remains unresolved without a write.
  Pending-pool absence is no longer the active implementation milestone. Phase E work should now
  replace the synthetic private-session gate with proven account-subscription ownership, implement
  order decoding, remaining bulk report orchestration, network startup coordination, one
  fixture-proven order command, and authoritative event emission. None of those operational
  mutation surfaces exists yet.
- **Phase F - Partial:** Read-only wallet quota summary, typed delegate directories, and selected
  offline subaccount/delegate signing primitives exist. Typed quota history, management services,
  authorization conformance, and all quota/delegate/subaccount mutations remain disabled.
- **Phase G - Partial:** This document and the strict Rust data and execution configs exist. The
  execution config exposes a non-zero HTTP read timeout, defaulting to 30 seconds, an optional
  redacted proxy URL, a non-zero canonical recovery scan range size, defaulting to 100 finalized
  blocks, and a
  non-zero timestamp nonce clock-drift limit, defaulting to five seconds. It also exposes ordered
  REST read-failover endpoints and a strictly validated bounded retry policy for idempotent reads.
  A Rust execution factory validates the typed config and constructs a disconnected framework
  client with the DeepX venue, netting OMS, and margin account identity. A separate Rust data
  factory validates `DeepXDataClientConfig` and constructs a disconnected framework client with
  the DeepX identity, read-only cache view, and framework clock. PyO3 registers the canonical
  identity constants, four config classes, two factories, and their config/factory extractors in
  the global registry. The thin Python facade and generated stubs expose only that boundary, with
  public-export and credential-redaction tests. Data startup now supports read-only REST
  perpetual instrument discovery and correlated instrument requests, including a Rust live
  verification binary. Execution startup, streaming subscriptions, historical market-data
  requests, management services, and operational trading examples remain unsupported.
- **Phase H - Not started:** No controlled conformance, benchmarks, fuzz campaigns, or full
  review-readiness run has been recorded.

Within Phase A, the external maintainer-approval and competing-work checks remain unresolved.
Fixture collection currently covers deployment/runtime identity only; market metadata parsers,
the catalog, the perpetual-only `InstrumentProvider`, and perpetual instrument conversion are
covered by sanitized mock responses rather than runtime-tagged protocol evidence. A typed EVM
precompile read retrieves raw Spot `min_order_size`, `tick_size`, and `step_size` integers for a
deployment-provided bytes32 pair, but the verified SDK does not specify their human-unit scaling.
Spot instrument conversion and the complete Spot-inclusive `InstrumentProvider` therefore remain
disabled. No fixtures cover complete REST pages,
WebSocket messages, transactions, reconnects, reorganizations, pagination, or management
operations. Within Phase B, the repository wiring and
local protocol primitives are implemented, but the planned crate skeleton is incomplete because
signing, integration-test, benchmark, fuzz, and example directories do not exist. HTTP support is
limited to unauthenticated idempotent JSON reads, including typed Spot and perpetual market lists,
one page each of perpetual funding-rate, long-short ratio, and open-interest history, one descending
raw perpetual trades read with bounded range pagination, one ascending page of raw perpetual
candles, mark-price history, and oracle-price history with explicit interval selection, one raw
perpetual volume-statistics window, and the raw perpetual last price.

Unit and mock tests cover the implemented common, metadata, HTTP, pagination, WebSocket protocol,
handler, task-lifecycle, and Python config/factory boundaries. These tests establish local
invariants only; they do not satisfy the fixture, live testnet, signing-vector, client-conformance,
management-service, benchmark, or fuzz requirements from later milestones.

:::danger
DeepX execution can submit transactions that affect account balances and positions. Testnet
assets have no intended monetary value, but leaked credentials, incorrect chain identity, nonce
reuse, or an unexpected deployment can still affect accounts controlled by the same wallet.
Never use production credentials with this integration.
:::

## Scope

| Area                  | Planned boundary                                 | Current status | Notes                                                    |
| --------------------- | ------------------------------------------------ | -------------- | -------------------------------------------------------- |
| Environment           | DeepX testnet                                    | Verified       | Deployment identity captured on 2026-09-01.              |
| Protocol core         | Rust types, fixtures, HTTP/WS and runtime state  | Partial        | Offline signer only; no submission or live WS channel.   |
| Mainnet               | None                                             | Unsupported    | No validated deployment or protocol evidence is present. |
| Spot                  | Nautilus data and execution clients              | Planned        | Requires verified asset, market, and trading schemas.    |
| Perpetual futures     | Nautilus data and execution clients              | Planned        | Requires verified market, account, and trading schemas.  |
| Lending               | Separate Rust and PyO3 service client            | Partial        | Public Rust market observations only; no PyO3 or mutations. |
| Subaccount management | Separate Rust and PyO3 service client            | Planned        | Requires verified ownership and authorization behavior.  |
| Delegates             | Separate Rust and PyO3 service client            | Partial        | Typed public directories only; no PyO3, authorization, or mutations. |
| Quota                 | Separate Rust and PyO3 service client            | Partial        | Public Rust summary only; no history, PyO3, claims, or purchases. |
| Bridge                | Separate Rust and PyO3 service client            | Planned        | Requires verified source and destination finality.       |
| Direct pallet backend | Metadata-driven SCALE extrinsics                 | Planned        | Explicit configuration; no automatic backend fallback.   |
| Legacy EVM backend    | EVM transaction wrapped by a Substrate extrinsic | Planned        | Explicit configuration; implemented independently.       |

No row in this page indicates current runtime support. A capability becomes supported only after
its fixture, parser, lifecycle, failure, and controlled testnet tests pass.

## Verified testnet evidence

The following deployment identity is recorded for DeepX testnet on 2026-09-01. The genesis hash,
runtime fields, and metadata are present in the checked-in RPC fixtures; the EVM chain ID is a
deployment constant and is not yet part of the capture manifest:

| Field                 | Captured value                                                       |
| --------------------- | -------------------------------------------------------------------- |
| EVM chain ID          | `4846` (`0x12ee`)                                                    |
| Genesis hash          | `0x86604388e0d446bb3e2238f9836a7da6e46f8c4f26da82de49d51b05d363c50b` |
| Runtime `specName`    | `frontier-template`                                                  |
| Runtime `specVersion` | `366`                                                                |
| Transaction version   | `1`                                                                  |
| State version         | `1`                                                                  |
| Metadata size         | `101473` bytes                                                       |
| Metadata SHA-256      | `e6b8b68e26fdd49e47e0af2ce4b6fe947f5d4520cb10171f250665e90e7b1c37`   |

Structured decoding of that finalized SCALE V14 metadata verifies this signed-extension order:

1. `CheckNonZeroSender`
2. `CheckSpecVersion`
3. `CheckTxVersion`
4. `CheckGenesis`
5. `CheckMortality`
6. `CheckNonce`
7. `CheckWeight`
8. `ChargeTransactionPayment`
9. `CheckPriority`

The order is taken directly from `extrinsic.signed_extensions`; it is not inferred from generic
Substrate defaults. The adapter pins DeepX's Subxt fork at commit
`2904b84ff5d6646481875e06749460dc5ebc6bbc`. The capture path remains block-hash-pinned and uses
the smaller `frame-metadata` decoder, while the signing path independently decodes the same bytes
with the pinned fork. This evidence backs an immutable snapshot value which accepts metadata only
when the observed genesis, runtime versions, metadata SHA-256, extension order, and unknown
extension encodings match the approved identity. A transport-neutral service can quiesce new
signing permits after an identity change and install an already validated replacement once old
permits finish. Public offline signing requires a permit retained for the complete encode. The
service does not watch the chain or fetch metadata itself. An explicit one-shot coordinator can
fetch and fixture-validate a finalized snapshot through the identity-validated Watch endpoint,
then atomically apply it to the service. The one-shot gate detects runtime-version or raw metadata
hash changes, including unknown upgrades, and blocks new signing permits before fixture validation.
Failures before change evidence leave service state unchanged; validation or RPC failures after
change evidence leave the observed fingerprint pending. When old permits prevent replacement, the
candidate remains pending and the caller must retry after those permits drain. Installation still
requires a matching fixture-approved snapshot. No live watcher, polling loop, unknown-runtime
approval, mortality selection, or transaction submission is enabled.

The testnet internal OpenAPI 3.1 document inspected during protocol research identifies itself as
`internal-v1`. The retrieved JSON was
`213669` bytes with SHA-256
`a488414337c679c76a946734d55696a6226c3bae6abbbc9de6dbd2c0aa9dc534`. It documents Spot,
perpetual, lending, account, quota, bridge-signing, chain-relay, transaction-status, and WebSocket
surfaces under `/internal/v1`. The document requires a protected documentation URL to retrieve,
but declares no request security scheme for the described endpoints.

The OpenAPI document is not stored as a repository fixture, so these research notes are not parser
or conformance evidence. Endpoint capabilities remain disabled until sanitized request and response
fixtures prove their runtime behavior.

The OpenAPI server entry uses `http://rest-api-testnet.deepx.fi`, while the captured document and
public responses were retrieved over HTTPS. The adapter must use the verified HTTPS endpoint and
must not derive transport security from the OpenAPI server entry. The documentation access token
is not fixture data and must never be stored in the repository.

The public HTTP transport retries only idempotent GET reads after transport failures, HTTP `408`,
HTTP `429`, or HTTP `5xx`. It uses the shared bounded retry manager and rotates through an ordered
list of explicitly configured HTTP or HTTPS base URLs. Decode failures, invalid local paths or
base URLs, and other HTTP `4xx` responses terminate immediately. Only one official testnet REST
endpoint is currently verified, so the default configuration does not imply an alternate endpoint
and cannot fail over unless an operator explicitly supplies another candidate. Retry count,
initial/maximum delay, jitter, per-attempt timeout, and total elapsed budget are integer-valued,
strictly validated configuration. The exponential factor remains fixed at two and the first retry
is delayed. This policy is not used by mutating requests. No DeepX request quota is configured
until authoritative rate-limit semantics are captured.

The pagination state treats a missing or empty cursor as completion, rejects a continuation cursor
on an empty page, rejects repeated cursors, and stops before a request could exceed its configured
page budget. It deliberately does not define cursor direction, inclusive boundaries, stable row
identity, deduplication, completeness, or freshness; each endpoint must prove those properties from
captured fixtures before using this state. The OpenAPI entries for `/health`, `/live`, and `/ready`
currently describe only successful `200` responses and provide no response schema, so they are not
exposed as typed endpoint methods.

`get_perp_market_by_id` and `get_perp_market_by_name` return a validated partial perpetual-market
model bound to the requested identity. The live single-market response omits directory-only tick
and step sizes, height, network, open-interest, active-order limit, deletion state, and 24-hour
change, so lookup results cannot construct instruments. A 2026-09-17 ETH-USDC lookup reported
`liquidationDustValue=50`, while the contemporaneous directory reported raw `50000000`; both are
preserved exactly and no common scale is inferred. The read-only instrument verifier compares only
stable, same-scale identity, precision, margin, fee, and minimum-order fields across the directory
and both lookup routes. The sanitized lookup fixture and provenance sidecar are
`perp_market_eth_usdc_by_id.*`.

The typed perpetual funding-rate primitive calls
`GET /internal/v1/market/perp/funding_rate` with a deployment market ID, millisecond bounds, an
optional positive limit, and an optional opaque cursor. It fixes the verified request interval to
`1m` and order to `ASC`, preserves rates as exact decimal values, and returns venue response order,
`hasNext`, and `nextCursor` without interpretation. A read-only testnet probe on 2026-09-01 returned
strictly increasing millisecond timestamps and a non-empty continuation cursor for a five-row page.
A second probe returned two consecutive three-row pages without overlap, but also returned a first
bucket timestamp earlier than an unaligned `start` and no row when both bounds exactly equaled an
observed bucket timestamp. Mock tests prove typed query encoding and exact response decoding, but no
sanitized runtime fixture or multi-page capture yet proves boundary inclusion, cursor stability,
deduplication, completeness, freshness, or funding settlement semantics. The adapter therefore
keeps this raw primitive single-page; bounded descending pagination and historical framework sample
responses are provided by the separate APIs described above. Every decoded page must carry
the requested market ID; a different response ID is rejected as a terminal identity mismatch.

The typed perpetual long-short ratio primitive calls
`GET /internal/v1/market/perp/long_short_ratio` with a deployment market ID, millisecond bounds, an
optional positive limit, and an optional opaque cursor. It fixes the verified UTC aggregation
interval to `1m` and order to `ASC`, preserves ratios from venue strings as exact decimal values,
and returns venue response order, `hasNext`, and `nextCursor` without interpretation. The OpenAPI
description states that each row is the latest position snapshot in its UTC interval and that empty
buckets are omitted. A read-only testnet probe on 2026-09-01 confirmed string ratios, integer
millisecond timestamps, and a continuation cursor for a three-row page. Mock tests prove typed query
encoding and exact response decoding, but no sanitized runtime fixture or multi-page capture proves
boundary inclusion, cursor stability, deduplication, completeness, or freshness. The adapter
therefore exposes no automatic pagination or Nautilus ratio event. Every decoded page must carry
the requested market ID; a different response ID is rejected as a terminal identity mismatch.

The typed perpetual open-interest primitive calls
`GET /internal/v1/market/perp/open_interest` with a deployment market ID, millisecond bounds, and an
optional positive limit. It fixes the verified request time frame to `1m` and order to `ASC`,
preserves total open interest and the long-to-short ratio as exact decimal values, and leaves the
venue response order unchanged. Mock tests prove typed query encoding and exact response decoding.
The units of total open interest, aggregation rules, boundary inclusion, completeness, freshness,
and conversion to Nautilus data remain unproven, so no framework open-interest event is emitted.

`get_perp_trades_history` adds bounded cursor pagination over an explicit inclusive millisecond
range with `DeepXPerpTradesHistoryRequest`. It retains market/time/page-size filters on every page,
honors `hasNext`, and enforces the local page budget. Repeated IDs, malformed or out-of-range
timestamps, ascending time order within/across pages, oversized pages, repeated cursors, and empty
continuation pages fail the entire request. Equal timestamps with distinct IDs and empty terminal
pages are valid. A stale terminal cursor is ignored when `hasNext=false`. Exact raw financial
values and taker labels remain uninterpreted in the raw reader. The separate framework history
path described above performs strict execution-record conversion.
`deepx-verify-rest-trade-history` is a public read-only verification program using an observed
latest ETH-USDC trade and page size one. It checks that the inclusive upper boundary retains that
trade, subject to an explicit 100-page budget; it does not claim a block-pinned history snapshot.
A live run on 2026-09-14 returned two ETH-USDC trades across two size-one pages over the inclusive
range `1789372702699..=1789372703699` and retained the observed latest trade at the upper boundary.
The initial live run exposed the decimal leverage schema mismatch; after switching both leverage
fields to exact decimals, the range verification succeeded. This proves the observed two-page path,
not stability under all concurrent writes or framework event semantics.

`get_perp_order_book(request)` reads one exact, potentially price-aggregated REST snapshot for a
required perpetual market ID. The response must echo that identity at the book and every level.
Prices and quantities must be positive, server notionals nonnegative, bid prices strictly
descending, ask prices strictly ascending, and prices unique within each side. Sequence and engine
time are validated, while zero `latestPrice` is retained as observed. The optional positive
aggregation tick is serialized as exact decimal text. `midPrice` is preserved exactly, including
backend binary-float artifacts, rather than being quantized to the requested tick.

This mutable REST snapshot is independent evidence and is not spliced into the sequence-bound
WebSocket framework book. It does not prove full exchange depth, freshness, or atomic agreement
with a stream. A spec-369 ETH-USDC fixture captures a 20-by-20 snapshot at tick `0.01`. Run
`deepx-verify-rest-perp-order-book <market-id>` to repeat the credential-free read; no account,
signing, or transaction path is used.

`get_spot_trades(request)` reads one globally ordered raw Spot execution page using optional market
name or bytes32 pair, wallet, inclusive millisecond bounds, sort order, page size, and opaque cursor
filters. `get_spot_trade_pages(request, max_pages)` follows that single global cursor within an
explicit nonzero page budget. It retains every page boundary and venue `total`, rejects oversized
pages, missing or repeated cursors, duplicate trade IDs, invalid counterparties or order IDs,
market scope mismatches, out-of-range timestamps, and ordering violations without returning a
partial collection. Equal timestamps with distinct IDs are valid. Because the history is mutable
and not block-pinned, totals are observations and are not required to remain constant across pages.

Prices, base and quote amounts, fees, and token values decode from their original JSON numeric
lexemes directly to exact `Decimal` values, including scientific notation. Their asset units and
fee ownership remain uninterpreted. The raw reader does not infer Spot quantity precision,
framework aggressor semantics, or construct `TradeTick` events. A nonempty spec-369 fixture from
2026-09-17 covers two `ETH/USDC` records and a continuation cursor. The read-only global verifier
returned 100 records and reported more than 11 million matching rows. Direct queries filtered by
the contributor-provided wallet returned API code `10012` (`Service temporarily unavailable`),
while the adapter verifier exhausted its retry path with HTTP 504. Successful wallet-filtered Spot
history therefore remains unverified, and neither failure is treated as an empty result. No
credentials or transactions were sent.

The public `get_spot_candles`, `get_spot_last_price`, `get_spot_volume`, and `get_spot_order_book`
readers require exactly one market name or bytes32 pair and preserve all JSON financial values as
exact decimals. Spot candles use the documented closed interval set, ascending non-TradingView
responses, inclusive request bounds, and a maximum limit of 5000. Responses reject a mismatched
echoed market name, nonpositive or inconsistent OHLC, negative volume, duplicate/descending
timestamps, or excessive row counts. Spot books echo both name and bytes32 identity and validate
positive quantities/prices, nonnegative server notionals, unique strictly ordered levels, sequence,
and engine time. The optional aggregation tick is a positive exact Decimal serialized directly as
its decimal text. Live `tickSize=1` evidence showed overlapping aggregated bid/ask buckets, and
notionals may be independently rounded, so the adapter does not invent uncrossed-book or exact
`price * quantity` invariants. Pair-selected candles echo only the market name, while last-price and
volume responses echo no identity at all; those paths cannot independently bind the response to the
requested bytes32 pair. Bucket completion, timestamp meaning, freshness, volume-window inclusion,
full exchange depth, and Spot quantity precision are not inferred, and no framework event is emitted.

A credential-free `ETH/USDC` run on 2026-09-17 validated recent one-minute candles, exact last
price and one-hour volume, plus a 20-by-20 order book aggregated at `0.01`. Run
`deepx-verify-rest-spot-market <market-name>` to repeat this read-only check.

`get_spot_markets` validates every public directory entry before exposing the collection. Market
names must agree with their base/quote symbols; pair IDs and asset addresses must have their exact
bytes32 and AccountId20 shapes; base and quote identities must differ; tick and guard values must
be positive; and case-insensitive market names and pair IDs must be unique. The separate
`get_spot_market_by_name` and `get_spot_market_by_pair` readers additionally bind the returned
identity to the caller's selector. The verifier cross-checks stable metadata across all three paths
without requiring mutable current price or nullable 24-hour change observations to remain equal.
The REST tick is not treated as the independently queried on-chain Spot minimum order or step size;
their human-unit scaling remains unproven, so Spot instrument conversion stays disabled.

Separate account-Spot probes on 2026-09-17 returned terminal empty order and trade pages for each of
the contributor wallet's four registered subaccounts. A subaccount observed in a contemporaneous
public ETH/USDC trade returned nonempty active-order, history-order, and trade pages. A runtime-tagged
three-record history fixture now proves the Spot order wire shape independently of the OpenAPI
example.

`get_spot_open_orders_raw` and `get_spot_history_orders_raw` preserve raw item JSON and cursor
metadata. Their typed counterparts retain all financial JSON lexemes as exact decimals and reject
wrong owners, market identities or requested sides, malformed decimal order IDs, invalid timestamps
or transaction hashes, inconsistent remaining amounts, empty venue enum values, duplicate IDs,
ordering violations, and oversized pages. Active orders require exactly one market name or bytes32
pair; history permits neither selector for a cross-market read. The bounded history collector is
atomic with respect to errors and enforces cursor progress and an explicit page budget. Mutable REST
pages do not prove complete or stable account history. Status, price type, post-only,
transaction-hash type, fee ownership, and lifecycle semantics remain uninterpreted, and no
framework order report is emitted. Run
`deepx-verify-rest-spot-orders <subaccount> <market-name>` for a credential-free live read. Typed
Spot wallet-group conversion remains deferred to its own evidence slice.

`get_spot_order_by_id_raw` preserves the exact lookup payload. Its typed counterpart binds owner,
market name or pair, side, and decimal order ID to the request. `get_spot_order_by_tx` accepts only
a prefixed 32-byte hash and requires the response transaction hash to match. Both reuse the strict
Spot order validation while leaving status, cancellation reason, cancellation height, and hash type
as venue observations. The captured order was `Open` in the earlier history page and `Canceled`
when both lookup endpoints were queried, proving that these REST views are mutable rather than
canonical inclusion or finality evidence. Run
`deepx-verify-rest-spot-order-lookup <subaccount> <market-name> <order-id> <Buy|Sell> <tx-hash>` to
require the ID and transaction paths to return one identical record. No framework report is emitted.

`get_spot_account_trades_raw` retains one exact subaccount-selected trade page without interpreting
records. `get_spot_account_trades` and the bounded raw/typed collectors validate optional exact
order filters, market identity, positive price/base/quote values, timestamps and inclusive bounds,
requested order, unique trade IDs, cursor progress, page size, and an explicit page budget. Exact
signed fees are preserved: the nonempty fixture includes negative maker rebates and empty fee-asset
strings, while the OpenAPI example omits that field; neither form is assigned framework meaning.
Records do not echo the requested subaccount, so ownership cannot be independently rebound from
the response. Taker and order-side
labels remain observations and no fill report is emitted. Run
`deepx-verify-rest-spot-account-trades <subaccount> <market-name>` for a credential-free live read.

The typed raw perpetual trades primitive calls `GET /internal/v1/market/perp/trades` with a
deployment market ID, an optional positive page size, and an optional opaque cursor. It fixes the
only verified request order to `DESC`, preserves trade price, quantity, and fees from their original
JSON number tokens as exact decimal values, and returns venue item order, `hasNext`, and `nextCursor`
without interpretation. It also preserves `createdAt`, `filledDirection`, and `taker` as raw strings.
Every item on a non-empty page must carry the requested market ID; one mismatching item rejects the
whole page as a terminal identity mismatch. The response schema provides no page-level market ID,
so an empty trade page has no response identity available to validate.
A read-only testnet probe on 2026-09-01 confirmed successful pages selected by either market ID or
market name, while an `ASC` request returned venue failure code `10012`; the adapter therefore
exposes only market-ID and descending-order requests, with optional bounded range pagination through
the separate history API described above. Mock tests prove query encoding, inclusive range retention,
cursor progress, and exact high-precision response decoding. Buyer/seller leverage also uses exact
decimals: the testnet emits JSON numbers such as `25.0`, and fractional leverage must not be truncated.
The bounded reader rejects overlapping duplicate IDs rather than silently inferring deduplication
rules. No block-pinned multi-page snapshot establishes stability, completeness under concurrent writes,
or freshness. The framework path maps only the buyer/seller taker role to aggressor side;
the position fill direction remains uninterpreted. No streaming trade event is enabled.

All three raw candle-shaped history endpoints reject empty pair identities, pages exceeding an
explicit requested limit, timestamps outside the requested bounds, duplicate or descending
timestamps, inconsistent OHLC ranges, and negative volume. Empty pages and gaps are accepted;
validation does not infer timestamp boundary semantics, volume units, market identity from a
numeric ID, or candle completeness.

The typed raw perpetual candles primitive calls `GET /internal/v1/market/perp/candles` with a
deployment market ID, millisecond bounds, and an optional limit constrained to the documented
`1..=5000` range. A closed enum exposes exactly `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`,
`8h`, `12h`, `1d`, `3d`, `1w`, and `1M`; requests remain `ASC` with `tradeView=false`. It preserves
volume and OHLC values from their original JSON number tokens as exact decimal values and returns
the venue pair, bucket timestamps, and response order without interpretation. Read-only `1m` and
`3m` testnet probes on 2026-09-15 returned aligned bucket-open timestamps and included the current
incomplete bucket. The same responses contained binary-float artifacts such as
`2481.3799999999997` despite the market's `0.01` price increment. Mock tests prove every documented
interval token, exact high-precision response decoding, and synchronous limit rejection. No
sanitized runtime fixture or multi-page capture proves boundary inclusion, empty-bucket behavior,
completeness, or freshness. The framework request path accepts only exact instrument-precision
OHLCV, preserves the venue time without shifting it, and emits no partial response when a row
contains backend artifacts that would require quantization.

The typed raw perpetual mark-price primitive calls `GET /internal/v1/market/perp/mark_price` with a
deployment market ID, millisecond bounds, one of the same closed interval values, and an optional
limit constrained to `1..=5000`. It fixes order to `ASC` and `tradeView=false`, and reuses the exact
raw candle wire shape without treating it as a trade candle. Mock tests independently prove the
endpoint path, typed interval encoding, exact high-precision OHLCV decoding, and synchronous limit
rejection. The venue labels the payload fields as OHLCV, but the volume meaning, bucket boundary
inclusion, missing-bucket behavior, completeness, and freshness remain unproven. The adapter emits
no Nautilus mark-price or bar event.

The typed raw perpetual oracle-price primitive calls
`GET /internal/v1/market/perp/oracle_price` with a deployment market ID, millisecond bounds, one of
the same closed interval values, and an optional limit constrained to `1..=5000`. It fixes order to
`ASC` and `tradeView=false`, and reuses the exact raw candle wire shape without treating it as a
trade candle. Mock tests independently prove the endpoint path, typed interval encoding, exact
high-precision OHLCV decoding, and synchronous limit rejection. The venue labels the payload fields
as OHLCV, but the volume meaning, bucket boundary inclusion, missing-bucket behavior, completeness,
and freshness remain unproven. The adapter emits no Nautilus oracle-price or bar event.

The typed raw perpetual volume primitive calls `GET /internal/v1/market/perp/volume` with a
deployment market ID and one of the four documented and runtime-probed periods: `1h`, `24h`, `7d`,
or `30d`. It preserves `totalVolume` from its original JSON number token as an exact decimal and
returns `tradeCount`, `startTime`, `endTime`, and `statisticTime` without interpretation. Read-only
testnet probes on 2026-09-01 returned successful objects for all four periods with integer
millisecond window widths matching the requested period; an invalid period returned venue failure
code `10001`. A sanitized `1h` REST fixture records the successful response shape and references the
independently captured testnet runtime identity. The REST response is not block-hash-pinned, so this
reference does not prove that it was produced by that exact runtime snapshot. Fixture and mock tests
prove response decoding, typed query encoding, exact high-precision volume parsing, and synchronous
market-ID rejection. The volume units, trade-count definition, boundary inclusion, rolling-window
alignment, update cadence, freshness, and `statisticTime` semantics remain unproven. The adapter
therefore emits no Nautilus volume or bar event.

The typed raw perpetual last-price primitive calls `GET /internal/v1/market/perp/last_price` with a
deployment market ID and returns the successful scalar JSON-number payload as an exact `Decimal`
without assigning observation-time semantics. A read-only testnet probe on 2026-09-01 confirmed the
successful response shape. Mock tests prove typed query encoding, exact high-precision response
decoding, and synchronous market-ID rejection. No sanitized runtime fixture exists, and the
endpoint supplies no observation timestamp. Until runtime-tagged evidence establishes observation
timing and freshness semantics, the adapter emits no Nautilus trade, quote, or ticker event from
this value.

The WebSocket protocol core registers each request before its send is exposed, resolves responses
strictly by request ID and transport connection epoch, and uses a separate non-wrapping send token
so stale send failures cannot remove a newer registration. Connection replacement drains pending
waiters, invalidates shared authentication state, and returns desired subscriptions for replay via
the shared Nautilus subscription tracker. Replacement epochs must increase strictly; an equal or
stale reset returns a typed error before changing current request, authentication, or subscription
state. Each inbound text frame is decoded from JSON once. A
top-level unsigned numeric `id` can be offered to the request registry, while every other valid JSON
shape remains an explicit unknown frame instead of being silently dropped. Malformed JSON returns a
typed error without panicking. A transport-neutral command handler now owns all mutations to this
state. Its handle registers a waiter before a future transport send, cancels only the matching
registration after a send failure, ingests text with an explicit connection epoch, and resets
connection-owned state. A bounded response wait removes only its matching send-token registration
when it times out; a response completed at the timeout boundary wins the race, and any later frame
remains an explicit unknown response instead of reviving the canceled waiter. Owner cancellation or
closure of every command handle drains all remaining waiters with typed cancellation errors. The
single-owner command queue also serializes desired subscription and unsubscription intent plus
acknowledgment and failure transitions. Subscription acknowledgments and failures are accepted only
for the current connection epoch, so a delayed prior-connection result cannot confirm or alter
replayed intent. Unknown and duplicate acknowledgments are rejected, and subscription or
unsubscription intent returns whether the transport should send a corresponding request. The queue
has a fixed capacity and applies asynchronous backpressure when full, so local callers cannot create
an unbounded command backlog. This is local lifecycle control only; it does not define venue flow
control or a DeepX request rate limit.

The same single-owner boundary can begin and complete authentication attempts using opaque,
monotonic attempt tokens bound to the current connection epoch. Starting a newer attempt resolves
the superseded waiter, duplicate or stale completion is rejected, and connection replacement
immediately resolves the old waiter while clearing authenticated state. Successful completion
issues an opaque authenticated-session receipt bound to both the attempt generation and connection
epoch and an unforgeable protocol-owner identity; attempts, cancellations, and receipts from another
handler cannot mutate or authenticate this owner even when their counters coincide. The protocol
owner can verify that a receipt is still current, and supersession or reconnect invalidates it.
Explicit authentication failure is accepted only for the active attempt; a stale rejection cannot
fail a newer attempt or invalidate a newer authenticated session. Bounded waits cancel only their
matching attempt on timeout, preventing a later completion from authenticating an abandoned attempt;
an already completed result wins at the timeout boundary. This proves only local ownership and
lifecycle behavior: no DeepX authentication payload or acknowledgement decoder is implemented.
Execution startup can consume only a receipt which remains current for the same protocol owner and
connection epoch; generic correlated responses cannot mark a connection authenticated.
The same receipt can admit an uncorrelated JSON frame into a schema-neutral authenticated envelope
only for its current ingress epoch. Reconnects, foreign protocol owners, stale epochs, and
correlated response frames cannot produce such an envelope. This establishes provenance for a
future fixture-backed private decoder without inferring any channel or business schema.

The refreshed internal OpenAPI describes `GET /internal/v1/ws`, JSON subscription and heartbeat
actions, subscription acknowledgements, public and address-filtered channel names, and orderbook
snapshot/delta semantics. It does not establish an authenticated private-session handshake.
A read-only HTTP/1.1 upgrade check on 2026-09-14 returned `101 Switching Protocols`
at `wss://ws-api-testnet.deepx.fi/internal/v1/ws`; a root-path check returned resource-not-found.
`DeepXNetworkConfig::ws_connection_url` adds that path only to a root base URL, preserving explicit
non-root paths and query parameters and rejecting userinfo, fragments, or non-WebSocket schemes.
`DeepXWsReadConnection` uses the project backend-neutral transport to own socket halves directly:
one bounded upgrade, raw frame reads, protocol Ping/Pong, bounded close, no authentication,
retry, auto-reconnect or detached tasks. Explicit application sends are restricted to typed public
perpetual subscription/unsubscription and heartbeat requests. Read timeout retains the same socket;
EOF, transport failure and close are terminal. The `deepx-verify-ws-connection` binary checks
upgrade and close without accessing private accounts or sending application requests. Neither
this raw transport nor its tests establish business payload interpretation.

`DeepXWsPublicConnection` serializes public subscription requests without inventing wire request
IDs, disables compression, and requires an exact perpetual market/channel acknowledgement.
Pre-acknowledgement data is buffered within a caller-supplied nonzero capacity and is released
only after acknowledgement. Wrong market/channel, duplicate acknowledgement channels, buffer
overflow, send failure or acknowledgement timeout makes that connection terminal; no automatic
retry is permitted. Confirmed data frames preserve their raw JSON payload, including numeric
lexemes. Unconfirmed or foreign data is rejected. Quiet-read timeout retains the socket; close
discards all connection-owned subscription evidence.

Read-only testnet sampling on 2026-09-14 returned a BTC market 2 acknowledgement, a public
`trades` page, and `orderbook` snapshots. The server acknowledgement message was descriptive
text rather than the example `ok`; it had no numeric request ID. The trade push included an
initial descending history page, so consumers must handle initial history and deduplication
before claiming live-only `TradeTick` delivery. The `deepx-verify-ws-public-subscriptions` binary
verifies acknowledgement and raw public trade/book envelopes using the Rust transport. This
connection is now used by framework trade subscriptions, but not private execution reports.

`DeepXWsTradeStream` now consumes the public trade-page payload using the existing exact-decimal
trade model. Its first page initializes retained execution identities without publishing history
as new live executions. Later overlapping descending pages return unseen trades chronologically;
numeric IDs are identity keys, not inferred time/sequence order. Same-ID content changes, malformed
timestamps, nonpositive price/size, duplicate IDs within a page, or unknown taker roles are rejected.
An unseen trade older than the delivery watermark requires explicit recovery rather than silent
discard. Retained identities are bounded, but IDs at the current watermark are never evicted;
exceeding that equal-timestamp capacity fails closed. Every rejected batch leaves identity,
initialization and watermark state unchanged.

The public subscription verifier now also requires initial history suppression and a later push
containing novel executions. A read-only testnet run on 2026-09-14 seeded the initial page and
identified six novel executions on the next trade push. This validates the observed continuous
page format and raw stream state, not framework `TradeTick` delivery, universal ID/cursor stability,
or loss-free reconnect. Framework subscriptions, precision conversion and task fencing are now
implemented as described below; explicit REST gap recovery remains outstanding.

`DeepXDataClient::subscribe_trades` validates the configured client/venue and advertised perpetual
instrument before admitting an owned task. It lazily opens one public connection per instrument,
awaits its market/channel acknowledgement, seeds initial trade history, and converts each novel
batch into exact chronological framework `TradeTick` events. Precision failures are detected for
the entire batch before any event is sent. Repeated active subscribe commands are idempotent.
The stream sends the documented application heartbeat every ten seconds, accepts heartbeat
responses, and retains the same connection after a quiet-read timeout.

Each subscription has a mutex-protected publication fence and cancellation signal. Unsubscribe
retires that fence synchronously and closes its dedicated socket; closing the connection removes
all its server subscriptions without relying on an undocumented unsubscribe acknowledgement.
Disconnect, stop, reset, dispose and drop retire both global and per-subscription publication
fences and cancel owned tasks. Old tasks cannot emit into a later connection generation.
Transport and acknowledgement failures allow up to five reconnect attempts with cancellable
exponential delays. The subscription retains its initialized trade state across those connections.
Before a fresh connection can publish trades, complete bounded REST history over an inclusive
range from the retained execution watermark to the fresh page's newest execution must include
every retained boundary ID unchanged. Every fresh-page trade in that range must agree with REST.
Recovery batches publish chronologically before queued live frames, without reseeding missed
executions as initialization history. Pagination is bounded to 100 pages of 100 records with the
configured WebSocket timeout as the full recovery-read deadline; no record-limited/truncated
reader is used. Missing boundaries, inconsistent records, history/precision errors and exhausted
budgets stop without partial recovery publication. Failed-subscription re-admission retains the
boundary in a new fenced task; explicit unsubscribe or connection retirement discards it.
Backend completeness and cursor stability remain independent evidence gaps, not a loss-free claim.
The read-only `deepx-verify-ws-public-subscriptions --recovery` run on 2026-09-14 closed and
reconnected the public transport, then reconciled REST range `1789381849454` to `1789381857574`.
Its three REST records covered the two retained boundary executions and one novel execution.
This tests real public REST/WS agreement; the framework reconnect and cancellation paths are
separately exercised with deterministic local endpoints, not claimed as a live forced-disconnect run.

`deepx-verify-live-trades` received three unique chronological BTC framework trades on a read-only
testnet run on 2026-09-14, then verified unsubscribe/disconnect and absence of further publication.
Local endpoint tests additionally cover initial suppression, overlapping pushes, atomic precision
failure, idempotent admission and all retirement paths. REST connected readiness still does not
prove a live subscription is acknowledged or healthy. Local reconnect tests cover REST pagination,
missing boundary/precision failures, cancellation during recovery, and failed-task re-admission.
Execution-account streams remain incomplete.

The public `mark_price`, `oracle_price`, and `funding_rate` channels now map to framework
`MarkPriceUpdate`, `IndexPriceUpdate`, and `FundingRateUpdate` events. Each subscription owns an
acknowledgement-gated connection and applies the same bounded reconnect, heartbeat, initial-data
deadline, cancellation, and connection-epoch publication fence as live trades. Mark and oracle
values preserve the observed decimal scale rather than being rounded to the instrument's tradable
tick. The public envelope timestamp is `ts_event`. Funding timestamp ordering is validated, while
`interval` and `next_funding_ns` remain `None` because the observed payload does not establish a
payment schedule. Semantic or precision failures stop the subscription without partial emission;
transport failures may reconnect to obtain a fresh current observation, without replaying invented
intermediate updates. The raw `latest_price` channel has no framework mapping.

A read-only `deepx-verify-live-trades --prices` run on 2026-09-16 received exact ETH mark
`2383.328348`, index `2386.075`, and funding rate `0.000211552824917299` framework events, then
verified unsubscribe/disconnect fencing. The verifier sent no account or transaction commands.

`DeepXWsBookStream` now validates and reconstructs the configured perpetual orderbook view.
Prices, quantities and server notionals retain exact decimal values. Snapshots replace both sides;
deltas require a matching `prevLastUpdateId`, and zero quantity removes a price. Duplicate prices,
malformed values, scope mismatches, sequence gaps and retained-level capacity excess invalidate
the cached book: only a fresh snapshot can restore it. The public subscription verifier applies
book updates through this state machine. `subscribe_book_deltas` publishes framework L2 batches,
including snapshot Clear/Add records and subsequent Delete/Update/Add records, with one `F_LAST`
terminator. Entire batches are precision-validated before publication. Explicit depth defaults
to 20 and is bounded to 4096 per side by the adapter; the instrument price increment sets the
server price aggregation size. The configured book view is not full exchange depth.

`subscribe_book_depth10` opens a separate acknowledged orderbook connection with depth 10 and the
instrument price increment. Every accepted snapshot or delta publishes a complete framework
`OrderBookDepth10`, ordered best-to-worst on each side, with exact price and quantity precision,
venue sequence, engine timestamp, snapshot flag, and typed zero padding. DeepX provides aggregated
levels without constituent order counts, so each populated level has count one. The subscription
shares the book stream's initial-data deadline, bounded fresh-snapshot reconnect, idempotency, and
synchronous retirement fencing. It does not implement historical depth requests. The read-only
`deepx-verify-live-trades --depth10` mode verifies this path without account or transaction access.
On 2026-09-16, it received BTC sequence `8310457` at engine time `1789549039088000000`, with best
bid `75643.8000 x 0.0212` and best ask `75645.8000 x 0.0169`, then verified unsubscribe and
disconnect fencing.

`request_book_snapshot` opens a dedicated public connection, requests the caller's depth with the
instrument price increment, waits for the exact acknowledgement, and accepts only the first full
snapshot. The response is a correlated `DataResponse::Book` with the venue sequence and engine
timestamp. Each side must stay within the requested depth; delta-first, malformed, precision-losing,
over-depth, timeout, and retired requests emit no partial response. This one-shot path does not
merge deltas, replay intermediate state, or retry semantic failures. A read-only testnet run on
2026-09-16 returned a BTC 20-by-20 framework L2 book at sequence `8191162`, with best bid
`75475.1000` and best ask `75479.2000`, then closed without account or transaction commands.

Book failures clear the framework book before awaiting socket close. Up to five recovery attempts
use fresh connections, acknowledgements and snapshots, with cancellable exponential delays from
250 ms to 4 s. An initial snapshot deadline uses the configured WebSocket timeout. Recovery
exhaustion stops the subscription and requires explicit readmission; no stale book is retained.
Unsubscribe and all connection retirement paths synchronously fence book publication as well as
trade publication. `deepx-verify-live-trades --book` received two framework snapshots and a four-record
delta batch on testnet on 2026-09-14, then verified unsubscribe/disconnect without further events.
Local tests cover normal delta actions, gap/precision recovery, invalid admission and six retirement
paths. No checksum is invented where the documented protocol provides none.
On 2026-09-14, the read-only testnet verifier accepted three successive BTC book updates with
sequence IDs `75693160`, `75693171` and `75693215`, retaining 20 bids and 20 asks. This confirms
non-unit sequence increments must not be classified as gaps when the predecessor matches.

The adapter also owns cancellation and task handles for one future handler
generation: shutdown first requests cooperative cancellation, then forcibly aborts and still joins
an unresponsive task after a bounded grace period. This prevents detached handler tasks, but does
not wire the separate public transport into the protocol handler or data client, implement a
reconnect I/O loop, or convert business channel data into framework events.
Those remain disabled until
captured fixtures prove their semantics. The command handler's topic delimiter is therefore an
explicit caller input rather than an inferred DeepX protocol constant.

The protocol-reference SDK inspected during protocol research is version `0.2.3`. The research
snapshot used its `main` commit
`496e07793c47c77db2056a72d8b706c5b143f9c6`; the `v0.2.3` tag points to
`4843f856b45873a2a739162fcbdcd091f4fdc0bc`. The SDK remains a reference only and is not a runtime
dependency. These revisions are not stored in the fixture manifest and do not independently prove
runtime behavior.

## Credentials

The testnet private-key environment variable is `DEEPX_TESTNET_PRIVATE_KEY`. Its value must be a
valid 32-byte secp256k1 private scalar encoded as 64 hexadecimal characters, with an optional `0x`
prefix. The adapter stores only decoded key bytes, zeroizes them on drop, and redacts both `Debug`
and `Display` output.

This credential boundary is preparation for independently verified signing implementations. It
does not currently sign requests, extrinsics, or EVM transactions. Mainnet credentials and key
schemes other than secp256k1 are unsupported.

## Python configuration boundary

The `nautilus_trader.adapters.deepx` package currently exports only the canonical `DEEPX`,
`DEEPX_CLIENT_ID`, and `DEEPX_VENUE` identities, the validated data, execution, network, and HTTP
read-retry configs, and the disconnected data and execution factories. It does not export raw HTTP
or WebSocket clients, endpoint helpers, signing primitives, or credentials.

`DeepXNetworkConfig` defaults to testnet and rejects mainnet or unknown environments before any
endpoint override is accepted. `DeepXExecutionClientConfig` requires an explicit subaccount for a
valid execution configuration and accepts only `direct_pallet` or `legacy_evm` as an explicit
backend. Selecting a backend validates configuration only; neither backend is wired to an order
command. Config representations redact private keys, and the public API exposes only
`has_private_key`, never the credential value. The execution config also owns an optional redacted
HTTP proxy URL and a non-zero HTTP timeout, defaulting to 30 seconds, for its read-only report
requests.

`DeepXHttpReadRetryConfig` applies only to idempotent reads. It bounds retry count, initial and
maximum delay, jitter, per-operation timeout, and total elapsed time. Mutating operations do not
inherit this policy, and no Python factory currently starts network I/O.

## Product capabilities

| Capability             | Spot    | Perpetual                           | Evidence gate                                                                                                          |
| ---------------------- | ------- | ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Instrument definitions | Planned | Implemented                         | Perpetual: catalog metadata, precision, limits, margin, fees. Spot: no verified quantity increment.                    |
| Historical candles     | -       | Implemented (strict single page)    | Correlated selectable-interval bars; lossy backend float artifacts reject the complete response.                       |
| Historical trades      | Partial | Implemented                         | Spot: exact bounded raw pages only. Perpetual: correlated framework responses with strict precision and identity.       |
| Order book snapshots   | Planned | Implemented (configured L2 view)    | Correlated one-shot requests, live L2 deltas, and depth-10 snapshots with exact precision, sequence, and engine time.   |
| Order book deltas      | Planned | Implemented (bounded recovery)      | Predecessor continuity, invalidation and fresh-snapshot retries; no documented checksum.                               |
| Live trades            | Planned | Implemented (bounded REST recovery) | Acknowledged TradeTicks, retained boundary/identity checks and atomic reconnect repair; backend completeness unproven. |
| Quotes and ticker      | Planned | Partial                             | Live top-of-book quotes; raw last price has no framework mapping.                                                       |
| Mark and index prices  | -       | Implemented                         | Exact acknowledged mark/oracle updates with bounded reconnect; no intermediate replay or freshness guarantee.          |
| Funding                | -       | Implemented                         | Bounded history and exact live rate updates; no inferred payment interval or next payment time.                         |
| Long-short ratio       | -       | Partial                             | Typed single-page history only; no framework events.                                                                   |
| Open interest          | -       | Partial                             | Typed single-page history only; units remain unproven.                                                                 |
| Volume statistics      | -       | Partial                             | Raw fixed-period window; units and boundaries unproven.                                                                |
| Last price             | -       | Partial                             | Raw exact value only; no timestamp or freshness semantics.                                                             |
| Live bars              | Planned | Planned                             | Streaming candle transport and incomplete-bucket publication remain unsupported.                                      |
| Market status          | Planned | Planned                             | Status values and unknown-value behavior.                                                                              |
| Lending market status  | Partial | -                                   | Raw directory, rate curves, APR and pool status only; precision, units, freshness, and framework events remain unproven. |

The perpetual-only instrument provider constructs `CryptoPerpetual` definitions from catalog
metadata with settlement currency, linear costing, and a unit multiplier asserted as documented
assumptions that remain pending deployment verification. Spot responses do not prove the permitted
quantity increment or order limits, so the provider skips Spot entries and fails closed on
explicit Spot construction; it does not construct `CurrencyPair` instruments.

Unsupported parameters and unknown enum values must return typed errors. The adapter must never
emit an order book assembled from unverified or discontinuous data.

## Order capabilities

| Capability         | Spot    | Perpetual | Evidence gate                                                                      |
| ------------------ | ------- | --------- | ---------------------------------------------------------------------------------- |
| Market order       | Planned | Planned   | Signed vector, submission, business event, inclusion, finality.                    |
| Limit GTC          | Planned | Planned   | Signed vector and authoritative lifecycle evidence.                                |
| Limit IOC          | Planned | Planned   | Time-in-force and partial-fill behavior.                                           |
| Post-only          | Planned | Planned   | Crossing rejection and venue status mapping.                                       |
| Reduce-only        | -       | Planned   | Position-side and over-reduction behavior.                                         |
| Stop order         | Planned | Planned   | Trigger source, direction, and lifecycle behavior.                                 |
| Modify             | Planned | Planned   | Atomicity, identity retention, and failure behavior.                               |
| Atomic replacement | Planned | Planned   | Old/new order identity and ambiguous-outcome recovery.                             |
| Close position     | -       | Planned   | Quantity, side, reduce-only, and residual-position behavior.                       |
| Cancel             | Planned | Partial   | Offline binding and indexed events; command wiring and independent vectors remain. |
| Fast cancel        | Planned | Partial   | Offline binding and indexed dispatch; authorization and command wiring remain.     |
| Cancel all         | Planned | Planned   | Scope and effects on unrelated strategies or subaccounts.                          |
| Batch operations   | Planned | Planned   | Per-item atomicity, result mapping, and partial failure.                           |
| No-op replacement  | Planned | Planned   | Same-nonce replacement and transaction-pool behavior.                              |

No order capability may emit a rejection after an outcome becomes ambiguous. Recovery must merge
relay, stream, REST, transaction-pool, block, event, and finality evidence without blindly
replaying mutating bytes.

## Account and reports

| Capability              | Current status | Evidence gate                                                                             |
| ----------------------- | -------------- | ----------------------------------------------------------------------------------------- |
| Account registration    | Planned        | Private authorization and initial snapshot semantics.                                     |
| Account query           | Implemented    | Current-startup reported cache replay only; no fresh REST snapshot.                        |
| Balances                | Partial        | Typed REST snapshot; locked/free meaning and ordering remain.                             |
| Portfolio state         | Planned        | Margin and collateral semantics for each product.                                         |
| Positions               | Planned        | Side, quantity, entry price, realized and unrealized PnL.                                 |
| Active orders           | Planned        | Stable venue identity and verified pagination.                                            |
| Order status report     | Partial        | Tracked ID lookup and bounded raw wallet history; framework external/bulk coverage remains. |
| Fill reports            | Partial        | Bounded perpetual REST history; snapshot completeness and private-stream recovery remain. |
| Position reports        | Partial        | Bounded current perpetual reads; complete coverage and freshness remain.                  |
| Mass status             | Planned        | Bounded, complete pagination and preloaded instruments.                                   |
| External order tracking | Planned        | Account-stream identity and registration behavior.                                        |
| Startup reconciliation  | Planned        | Restart fixtures and deterministic REST/stream/chain merge.                               |
| Quota summary           | Partial        | Exact wallet REST aggregate only; completeness, freshness, and claim eligibility remain.  |
| Liquidation history     | Partial        | Exact raw-unit bounded REST history only; no risk events or snapshot completeness.         |

The execution client must load all required instruments before reconciliation. Report generation
must not fetch missing instruments dynamically.

## Execution backends

The backend is an explicit configuration choice. The adapter must not switch backends after a
mutation might have been transmitted.

### Direct pallet

The adapter can encode a caller-specified dynamic call against the approved immutable runtime
metadata snapshot and sign it offline with the pinned DeepX Subxt fork's AccountId20/Keccak ECDSA
signer. The caller must provide the nonce explicitly; the primitive does not read a clock, allocate
or persist a nonce, access the network, or submit bytes. The snapshot exposes fixture-derived
pallet, call, and event identities, but metadata presence alone does not prove DeepX business
semantics. SDK reference behavior uses a millisecond timestamp nonce by default. All trading calls
remain unsupported until sanitized golden vectors prove the call values, signature payload,
complete extrinsic, and transaction hash for every enabled action, and a durable nonce owner and
runtime-refresh boundary exist.

### Legacy EVM precompile

The legacy path is expected to encode ABI calldata, sign an EVM transaction, and wrap the decoded
transaction plus signer AccountId20 in an unsigned `Ethereum.transact` extrinsic. The Python SDK
reference uses `create_unsigned_extrinsic` for this wrapper; there is no second outer signature. It
remains unsupported until fixtures prove the precompile address, ABI, chain ID, nonce, gas fields,
transaction format, EVM signature, wrapper bytes, and both transaction hashes.

## Transaction evidence

The adapter now provides a pure, evidence-driven transaction lifecycle with these states:

| State              | Meaning                                                              |
| ------------------ | -------------------------------------------------------------------- |
| `created`          | Identity and nonce reservation exist durably.                        |
| `signed`           | Bytes were signed against one immutable runtime snapshot.            |
| `submitting`       | Transmission started and the outcome may become ambiguous.           |
| `accepted`         | A submission node accepted the transaction into its pool.            |
| `in-block-success` | The extrinsic and expected business event succeeded in a best block. |
| `finalized`        | The recorded success or failure is canonical and finalized.          |
| `in-block-failed`  | An authoritative dispatch or expected business event failure exists. |
| `not-included`     | Authoritative checkpoint-bound evidence proves absence.              |
| `action-required`  | Available recovery evidence is incomplete or conflicting.            |

Pool acceptance is not order acceptance, block inclusion is not business success, and best-block
success is not finality. Events must be matched by block extrinsic index. A mutating timeout after
possible transmission is ambiguous and must not be treated as a venue rejection.

Online inclusion evidence has a fail-closed construction boundary. Dispatch and expected business
event observations must carry the same block extrinsic index. Successful dispatch without an
authoritative expected business event is rejected, as is a failed dispatch paired with any
business event. Only successful dispatch plus expected business success produces
`in-block-success`; expected business failure or dispatch failure produces `in-block-failed`.
Version 3 durable records retain their existing collapsed inclusion shape and are restored only
through the internal validated record codec. No RPC event decoder or business-event schema is yet
connected to this boundary, so live inclusion classification remains disabled.

A read-only network observation boundary now queries only chain-identity-validated endpoints. It
fetches a canonical block hash and body from the Watch endpoint, verifies the returned header
height, decodes every prefixed SCALE extrinsic, and recomputes each Blake2-256 hash before exposing
the unique matching extrinsic index. It separately inspects `author_pendingExtrinsics` through the
Submission endpoint and reports observed pool absence only after every returned entry decodes successfully.
Malformed entries, duplicate target extrinsics, missing blocks, and inconsistent heights fail
closed. These observations prove location or pool membership only: without fixture-backed dispatch
and expected business-event decoding they cannot produce `in-block-success`, `in-block-failed`,
`finalized`, or `not-included` lifecycle evidence.

The network recovery boundary now binds complete role-capability evidence to the validated Recovery
and Submission endpoints. Before resuming, it revalidates both the height and canonical hash of the
durable scan checkpoint; a changed or unavailable checkpoint stops recovery before scanning later
blocks or querying the submission pool. It then pins a bounded scan to the Recovery endpoint's
finalized head and feeds contiguous canonical observations into the pure recovery collector. Pool
presence on the node that accepted submission can produce `accepted`. Pool absence is not atomic
with the finalized scan and therefore remains `unknown`/`action-required`; it cannot produce
`not-included` or authorize a replacement. If the exact extrinsic appears in any scanned block,
collection stops with an explicit event-evidence error because location alone cannot prove dispatch
or business outcome. The boundary performs no durable mutation, automatic replay, lifecycle commit,
or Nautilus event emission.

The reorganization observation boundary binds Watch capability evidence to the configured endpoint,
queries the exact recorded inclusion height using the durable extrinsic hash, and applies the pure
reorganization classifier. It proves `reorganized` only when the canonical block hash changed and
proves `canonical` only when the original block and extrinsic index still match. It returns
`action-required` for missing or displaced transaction evidence. A record-bound coordinator derives
the signed hash, inclusion, record, and acknowledgement from one restored durable value, then
commits the decision through the signer lease and revision-checked compare-and-set boundary.
Ineligible or finalized records are rejected before network access. The coordinator performs no
automatic replay, replacement, submission, or event emission.

The finality observation boundary binds Watch capability evidence to the configured endpoint and
reconciles only a restored `in-block-success` or `in-block-failed` record. It first reads the exact
finalized checkpoint. A checkpoint below the recorded inclusion height preserves the existing
record and durable revision without querying that canonical block. Once the checkpoint covers the
height, the boundary requires the recorded block hash, extrinsic index, and durable extrinsic hash
to match the canonical block exactly before a record-bound coordinator commits `finalized` through
the signer lease and revision-checked compare-and-set boundary. Missing, displaced, or conflicting
canonical evidence fails closed without inferring finality. This coordinator does not submit,
replay, replace, or emit order events. During startup mass reconciliation, this exact conflict is
passed to the reorganization coordinator. A proven changed canonical hash is committed durably and
returns the record to `submitting`; missing or displaced evidence is committed as
`action-required`. Neither outcome authorizes startup to advance.

The lifecycle rejects transitions that skip durable signing, preserves the immutable extrinsic
hash and exact block inclusion evidence, and treats repeated matching observations as idempotent.
`not-included` requires checkpoint-bound authoritative absence evidence that the current network
collector does not produce. Later canonical inclusion can correct an existing negative observation.
An exact reorganization observation can remove a recorded non-finalized
inclusion, retain the reverted block and extrinsic-index evidence, and return the lifecycle to
`submitting` for fresh reconciliation. A later canonical inclusion replaces the reverted evidence;
mismatched or finalized reorganization observations are rejected. Incomplete or conflicting
evidence requires `action-required`.

The lifecycle foundation performs no persistence, networking, submission, automatic replay, or
Nautilus order-event emission. A separate in-memory timestamp nonce policy performs allocation from
the current Unix epoch time in milliseconds, but grants no signing or submission authority and
depends on external durable records, exclusive signer ownership, and authoritative chain time.
Those operational capabilities remain disabled until the runtime and recovery evidence gates below
are resolved.

Submission failures use the same evidence vocabulary intended for the execution boundary:
`not-sent` requires local proof that transmission never started, `venue-rejected` requires an
explicitly decoded authoritative rejection, and `ambiguous` means transmission may have started.
Once the lifecycle enters `submitting`, transport timeouts, connection loss, missing responses, and
unknown response forms remain ambiguous unless later authoritative evidence resolves them. Failure
classification does not itself mutate transaction state, release a nonce, replay bytes, or emit an
order rejection. No existing generic HTTP or WebSocket error is currently mapped to these classes
because a DeepX transaction-submission response schema has not yet been proven.
A bounded retry coordinator can consume the permit released after the durable `submitting` commit.
It retries only outcomes already classified as `ambiguous`, passes an identical copy of the
permitted signed bytes and hash to each attempt, and validates successful node-hash evidence
against that payload. `not-sent` and `venue-rejected` stop on their first occurrence; exhausted
ambiguity remains ambiguous. The coordinator itself performs no classification, delay, lifecycle
commit, nonce allocation, signing, replacement, order-event emission, or execution-client
submission. A separate acceptance boundary verifies that successful node evidence identifies the
durable signed hash before atomically advancing the exact acknowledged `submitting` record to
`accepted`. A lost commit acknowledgement remains `commit-outcome-unknown` and requires durable
reconciliation; it never grants retry authority.

Network configuration assigns explicit JSON-RPC roles for transaction submission, head and
inclusion watching, and bounded recovery scans. Each role can use an independent endpoint and
falls back to the common verified testnet RPC URL when no role-specific override is configured.
Role selection performs the same hard testnet validation as every other endpoint. This separation
does not enable transaction submission or prove that the default endpoint supports every role.
Execution configuration limits each canonical recovery scan range to a non-zero number of finalized
blocks, defaulting to 100. This setting controls scan partitioning only; it does not authorize
submission retries, parallel scans, or broader historical inference.
A pure validation boundary now requires caller-supplied observations for all three roles, rejects
missing or duplicate roles, requires each observed URL to match the configured selection, and
requires every endpoint to report the approved DeepX testnet genesis hash before releasing the
complete endpoint set. URLs remain redacted from `Debug`. The boundary performs no network I/O and
does not prove role-specific RPC method support; an operational client must still collect genesis
identity directly from each endpoint before probing it. A separate read-only probe accepts only an
identity-validated endpoint set and a non-empty caller-supplied list of required methods for one
role. It calls `rpc_methods`, rejects transport or response failures, and returns evidence only when
every required name is advertised. A complete collector requires submission and pending-pool
methods for Submission; canonical block, block-hash, finalized-head, header, runtime-version, and
metadata reads for Watch; and canonical block, block-hash, finalized-head, and header reads for
Recovery. It probes all roles concurrently and returns no partial evidence. Evidence retains the
role, endpoint URL, and required method names, but not unrelated advertised methods. Execution
startup rejects evidence collected from a different endpoint set. These names are the minimum
implied by the current planned flows; they do not prove semantics or enable any operational client.

Direct-pallet transaction reservations have a versioned, strict durable record format. Version 3
adds retained reorganization evidence and uses a distinct cache-key namespace so older record
shapes cannot be silently interpreted as current. A record records the client order ID, signer,
instrument, side, nonce domain, and approved runtime identity before signing. The offline signed
result carries the runtime identity actually used for encoding,
and the record accepts only a matching signer, timestamp nonce, runtime, and Blake2-256 hash of the
signed bytes. Sequential account nonce binding remains unsupported. The current generic dynamic
signer does not prove that pallet call arguments encode the recorded client order ID, instrument,
or side; that binding remains gated on authoritative SDK golden vectors. Restoration rejects
unknown fields, unsupported versions, invalid identifiers, incomplete absence evidence, and
lifecycle state inconsistency. Cache keys are versioned and hex-encode client order ID bytes so
delimiters cannot change the namespace. Records retain the complete signed bytes and verify their
Blake2-256 hash during restoration. These bytes are recovery evidence only: the codec
does not authorize submission or replay, and no future mutating path may resend them without an
authoritative reconciliation policy proving that replay is safe.

Restored records expose a pure fail-closed recovery action. `created` requires reconstruction and
verification of signing inputs, while `signed` requires an external persistence and submission
decision before transmission can begin. `submitting`, `accepted`, both in-block states, and
`not-included` require authoritative reconciliation; `finalized` is complete; and
`action-required` stops automatic recovery for operator review. The classifier performs no I/O or
mutation and never treats retained bytes as replay authority. In particular,
`submission-decision-required` does not make submission operational: the committed-write,
exclusive nonce-owner, call-binding, and protocol-evidence gates still apply.

A separate automatic replay decision gate also returns no bytes or transmission permit. `created`
requires reconstructed signing inputs, `signed` remains subject to the initial-submission policy,
and all submitted or included non-final states require fresh reconciliation. A `not-included`
record with complete canonical-scan and submission-pool absence evidence requires a newly built and
independently validated replacement; the retained signed extrinsic is never replayed. `finalized`
requires no transmission, while `action-required` remains an operator stop. Replacement
construction and transmission are not implemented.

Post-sign submission, pool, inclusion, finality, absence, and operator evidence is applied through
the durable record boundary. Each observation is first evaluated against a candidate lifecycle and
the complete record invariants; the candidate is committed in memory only after both checks pass.
Callers receive no mutable lifecycle reference, so an orphaned extrinsic hash cannot bypass the
retained signed-payload check. This mutation remains pure and does not imply that the updated record
was durably committed.

The record codec does not itself provide committed writes, allocation, locking, or nonce ownership.
The transaction persistence interface now makes the missing capability explicit: an operational
backend must acquire a cross-process signer lease, acknowledge record creation only after its
durability boundary commits, and replace records through revision-checked compare-and-set. A lost
commit acknowledgement is classified as an unknown outcome and retains signer ownership pending
reconciliation. Acknowledgements are bound to the exact cache key and encoded record, so an older
write cannot authorize a newer lifecycle state. The generic cache `add` operation does not satisfy
this interface because its contract does not prove durable commit, CAS, or lease ownership.

The PostgreSQL transaction store persists versioned record envelopes in Nautilus's existing
`general` table and compares the complete expected envelope during revision-checked replacement.
It holds a detached PostgreSQL session advisory lock for the lifetime of each signer lease, so a
pooled connection cannot retain signer ownership after the lease ends. Lost write acknowledgement
remains an unknown commit outcome. This store is a persistence primitive, not a configured signing
or submission service: committed signed bytes remain evidence rather than replay authority, and
sequential account nonce signing and order-call binding remain disabled until captured protocol
vectors prove their domains and exact encoded call arguments. The single proven business-call
binding is the identity-bound `System.remark` verifier below, which grants no order capability.

The timestamp nonce allocator is scoped to one externally leased signer and restores the maximum
timestamp reservation from the complete durable record set supplied by that store. It uses the
greater of caller-supplied local and chain Unix millisecond time, rejects excessive clock drift and
restored values implausibly ahead of calibrated time, and atomically advances by at least one under
same-millisecond contention. Values are never rolled back or released in memory. The caller must
durably commit each reservation before signing; a failed or unknown commit burns the local value and
retains signer ownership pending reconciliation. The reservation preparation boundary enforces this
ordering: it verifies that the current store lease covers the allocator signer, allocates the nonce,
creates the immutable identity, and returns the record only after `create_committed` acknowledges
that record's exact encoding. It performs no signing or submission. The execution client now
retains the configured store, signer lease, allocator, and verified durable snapshot for its
configured signing key using the configured non-zero clock-drift limit, which defaults to five
seconds. No client method yet combines that runtime with an authoritative finalized chain-time
reader, so it does not allocate a new nonce, enable sequential account nonces, sign, or submit a
transaction.

The persistence contract is asynchronous so the PostgreSQL implementation can hold a
transaction-scoped signer fence and commit record changes without blocking the runtime. Signing
preparation verifies the current signer lease and exact committed `created` record before invoking
an offline signer, validates the resulting signer, timestamp nonce, runtime identity, and extrinsic
hash, and compare-and-sets the complete `signed` record before returning it. A stale revision may be
detected only by that CAS after offline signing, but no signed result is released on a conflict or
unknown commit outcome. This boundary does not prove business-call arguments and grants no
submission authority.

Initial submission preparation is a separate atomic boundary: it revalidates the current signer
lease, matches the exact previously committed record bytes, requires a proven business-call
verifier, and compare-and-sets `signed` to `submitting` before releasing a single-use
payload permit. Stale revisions, forged prior records, unproven call bindings, and unknown commit
outcomes release no payload. The default verifier rejects every call because the required vectors
have not been captured; two explicitly selected fixture-gated verifiers exist.
`DeepXRemarkCallVerifier` deterministically
re-signs the canonical `System.remark` payload derived from the reserved identity (client order
ID, instrument, side, timestamp nonce, runtime spec version) against its approved runtime
snapshot and requires byte-for-byte equality with the durable signed extrinsic, so bytes signed
for any other identity, nonce, payload, or runtime cannot pass. It rejects the unproven
sequential-nonce domain and every DeepX order call. `DeepXPerpCancelCallVerifier` applies the same
comparison to the exact ordinary or fast perpetual-cancel operation, including signer,
subaccount, order ID, market ID, fast-cancel flag, timestamp nonce, and runtime. Neither verifier
performs network I/O; the perpetual-cancel verifier is not selected by the execution client and
does not submit or enable a cancellation command. This permit is intentionally unavailable to
restored reconciliation states
and therefore cannot be used for automatic replay.

Authoritative reconciliation observations have a separate durable commit boundary. It revalidates
the current signer lease and exact prior acknowledgement, applies the observation to a candidate
record, and compare-and-sets a changed record before exposing it. Repeated identical evidence is
idempotent and preserves the existing revision. Stale revisions and unknown commit outcomes expose
no candidate record. `signed` and `submission-started` observations are rejected here so they cannot
bypass signing validation or the initial-submission business-call gate. This boundary consumes
already-decoded evidence only: it performs no RPC collection, canonical scanning, pool query,
submission, replay, or Nautilus order-event emission.

Reorganization observations use this same revision-checked commit boundary. The boundary accepts
only the exact recorded non-finalized block hash, block number, extrinsic index, and business
outcome, commits the reverted evidence once, and treats an identical repeated observation as
idempotent without advancing the durable revision. Pure recovery planning splits blocks after the
last complete checkpoint into bounded, contiguous inclusive ranges without wrapping at `u64::MAX`.
A single-owner collector accepts only the next planned range with the exact ordered block count and
cannot release a recovery scan until all ranges reach the finalized boundary. The network recovery
boundary requires the prior checkpoint number and hash to remain canonical before it supplies exact
finalized-block identity, canonical absence, and node-local pending-pool observations for this
collector. It conservatively retains observed pool absence as unknown. Exact reorganization
observation for an acknowledged non-finalized inclusion can be explicitly committed through the
existing signer lease and CAS boundary. No tracker or polling loop invokes it automatically, and
returning a reorganized transaction to `submitting` grants reconciliation, not replay, authority.
An acknowledged `not-included` record can also explicitly resume a bounded scan only from the
exact finalized number and hash retained in its durable absence evidence. Other lifecycle states
are rejected before RPC because they do not retain that checkpoint. An unchanged finalized head is
verified idempotently through the signer lease and prior acknowledgement. Later node-local pool
presence conflicts with the durable absence state and therefore commits `action-required`; pool
absence remains unknown and cannot create fresh `not-included` evidence.
`DeepXDurableRecoveryObserver::PerpCancel` explicitly selects exact durable-byte verification
with the approved Perp signer before any recovery RPC. Ordinary and fast Perp cancellation use
the existing canonical finalized scan and indexed dispatch/business-event verifier. Restored
checkpoints remain idempotent; ambiguous evidence and non-atomic pool absence still require
operator action. This offline boundary is not selected by default or wired into live startup,
and grants no submission or replay authority. Independent SDK vectors, account authorization,
maintainer approval, and live execution command conformance remain required gates.
Live absence and inclusion classification remain disabled until evidence can be bound to an
authoritative chain checkpoint and an event decoder can bind dispatch and expected business
outcomes to the matching block extrinsic index.

## Fixture identity

Every fixture set is immutable and identified by all of these values:

- Genesis hash.
- Runtime metadata hash.
- `specVersion`.
- `transactionVersion`.

Each set also records the deployment name, capture timestamp, endpoint role, signed-extension
order, and whether values were captured from a finalized or best block. Finalized captures also
include the block hash in their directory name so captures with the same runtime identity can
coexist without replacement. A runtime upgrade creates a new fixture set; vectors from different
identities must never be silently combined or replaced.

Fixtures must be sanitized before commit. They must not contain private keys, seed phrases,
credentials, authorization headers, session tokens, personally identifying account labels, or
other account secrets.

## Hard capability gates

The following unresolved questions keep their dependent capabilities disabled:

- Complete endpoint-role behavior beyond the verified REST, WebSocket, and RPC base URLs.
- Spot and perpetual asset and market schema stability.
- Public and private WebSocket authentication and subscription acknowledgements.
- Initial snapshot, update ordering, venue reconnect replay, and acknowledgement behavior.
- Order book snapshot flags, sequence scope, checksum rules, gap recovery, and resnapshot endpoint.
- REST pagination boundaries, overlap, stable identities, and freshness behavior.
- Signed-extension payload semantics, mortality period, checkpoint selection, and runtime-upgrade
  behavior. The read-only collector can pair a finalized hash and block number, but existing
  immutable fixtures do not prove that pair.
- Direct pallet call and event definitions for every action.
- Legacy precompile ABI, transaction envelope, wrapper, and hash semantics.
- Authoritative chain-time source and allowed drift, plus configured runtime ownership of the
  existing timestamp reservation boundary.
- Relay acknowledgement meaning and correlation with stream, REST, and chain evidence.
- Canonical inclusion, reorganization, finality, pool eviction, and missed-block recovery.
- Quota idempotency and the exact EIP-191 claim message.
- Delegate ownership, mode, expiry, revocation, and wallet-wide effects.
- Lending precision, units, freshness, completeness, compounding, collateral, framework semantics,
  and authoritative completion evidence.
- Subaccount ownership, registration, and authorization semantics.
- Bridge source/destination chain identity and finality assumptions.

Unknown or conflicting evidence moves the affected operation to `action-required`; it does not
enable a permissive fallback.

## Milestone test plan

| Milestone              | Required proof                                                         |
| ---------------------- | ---------------------------------------------------------------------- |
| Protocol evidence      | Sanitized runtime-tagged fixtures and an unresolved-question register. |
| Protocol core          | Fixture parsing, malformed input, redaction, and mock transport tests. |
| Instruments            | Exact precision and bidirectional Spot/Perp symbol identity.           |
| Market data            | Request correlation, chronology, replay, and book recovery.            |
| Direct signing         | Byte-for-byte SDK vectors and runtime metadata compatibility.          |
| Legacy signing         | ABI, EVM envelope, wrapper, signatures, and both hashes.               |
| Nonce and recovery     | Concurrency, restart, ambiguity, reorg, finality, and no reuse.        |
| Execution              | Command, reconciliation, race, deduplication, and terminal uniqueness. |
| Management services    | Exact conversion, authorization, ambiguity, and finality per service.  |
| Python boundary        | Public exports, configs, factories, services, and generated stubs.     |
| Controlled conformance | Minimal testnet operations in increasing risk order.                   |

Controlled testnet conformance proceeds from read-only shadow mode to public reconnect and gap
recovery, authenticated account state, startup reconciliation, minimal Spot and perpetual orders,
transport ambiguity, RPC failover, reorganization and finality recovery, restart restoration, and
management operations. Direct and legacy execution backends are tested independently.

Before a capability is marked supported, its focused tests, full adapter tests, strict Clippy,
rustfmt, applicable Python tests, generated-drift checks, benchmarks, and fuzz targets must pass.
Failed or incomplete conformance leaves that capability disabled and documented here.

## Known limitations

- The Python package exposes only validated configs, disconnected factories, and canonical
  identities; it does not expose an operational client or management service.
- No DeepX market data, account, signing, trading, or management capability is currently enabled.
- Mainnet is explicitly unsupported.
- The Python SDK is a protocol reference and golden-vector oracle only; it will not be a runtime
  dependency.
- Current API and SDK schemas are not treated as authoritative without matching captured testnet
  behavior.
- Credentials are limited to zeroizing, redacted secp256k1 private-key storage and validation.
  Wallet, subaccount, and authorization semantics remain unresolved.
- The original runtime fixture predates finalized-block pinning and records a best head. It remains
  immutable alongside a newer fixture whose runtime version and metadata were captured at finalized
  block `0x03e29c08d90b26697535dacbcfa940c8d2ae08653e4b4760ac1dd4a281ced7c6`.
  Both existing manifests predate structured signed-extension extraction and remain immutable with
  `signed_extensions: null`; the finalized metadata bytes now have an exact decoder-backed order
  regression test. They also predate finalized-header capture and therefore do not prove the block
  number required for a mortality checkpoint. New captures record the finalized header and its
  canonical hash at that height, then re-read and validate every serialized payload, runtime
  identity field, metadata digest, and signed-extension order before atomically publishing the
  immutable fixture directory. The default endpoint returned a TLS handshake EOF on 2026-09-02, so
  no replacement fixture was committed. This capture validation does not provide a signed golden
  vector; mortal signing remains disabled pending a complete capture and the other direct-signing
  evidence gates.
- Maintainer approval and confirmation that no competing issue or pull request exists remain
  external contribution process gates; local implementation does not satisfy them.
- Financial values remain integers or exact decimal values until conversion to Nautilus domain
  types. Floating-point conversion is not permitted.
