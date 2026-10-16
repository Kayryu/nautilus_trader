# DeepX

## Perpetual close event recovery

`verify_perp_close_inclusion_events` binds complete indexed SCALE events to the durable close
call under the exact approved runtime and timestamp nonce domain. Successful dispatch requires
one `PerpMarket.OrderPlaced`. Outer and nested order IDs agree, but the ID comes from the account
sequence and is not compared with the signed timestamp nonce. Owner and market match the call,
the order is reduce-only, points and post-only are absent, and its initial state is open with
positive size, nonzero leverage, zero filled size, and full remaining size. Position-derived size
and direction are not inferred from the close call. Nonzero price matches exactly and uses `Stop`; zero price uses
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
before recovery RPC. Current version 8 records also retain the finalized block number and hash
observed before nonce reservation. Execution startup uses that immutable checkpoint to scan restored
`submitting` and `accepted` placements and commits a proven finalized inclusion in separate
in-block and finalized compare-and-set steps. It selects the explicit observer for subsequent
not-included scans. Unresolved or conflicting evidence continues to block startup, never
authorizing replay, replacement nonce allocation, order success, or connected readiness.

On the testnet runtime, standard `System.Events` can be absent. The recovery reader then decodes
`System.Threads(block_number)` as the inclusive last thread index and requires every
`System.EventsMap(block_number, thread)` batch from zero through that index. It validates each
compact event count and reconstructs one complete SCALE event vector; missing or malformed thread
state or any missing batch fails closed. Captured spec369 block `185969410` contains four batches
with 58 total events. Its extrinsic index 8 proves both successful dispatch and the exact Limit GTC
placement for order `1789445053841`; the fixture records the finalized block hash, storage keys,
batch counts, runtime identity, and REST cross-reference.

The same complete spec369 event fixture contains four `PerpMarket.NewTrade` events. A strict
metadata decoder retains each event's block hash, number, global event index, counterparties,
order IDs, and raw financial integers. Event index 51 belongs to the wallet's market 3 order
`1789445053841`; its buyer, taker side, direction, price, size, leverage, and fee agree exactly
with the captured account-trade record after applying the observed market and USDC scales. The
REST trade ID `185969410000051` agrees with that block and index. A second read-only capture at
finalized spec371 block `187071212` contains one complete 29-event batch. Two market 3 events
for the same wallet subaccount match the separate REST partial fills exactly: global indices 14
and 22 correspond to trade IDs `187071212000014` and `187071212000022`. The historical runtime
metadata hash matches the approved spec371 fixture, and repeated storage reads returned the same
batch bytes; an independent `state_queryStorageAt` read agreed at the same block hash. The exact
matcher now requires the observed `block_number * 1_000_000 + event_index`
REST ID and one unique event, checking account, order, market, taker, direction, price, size,
leverage, signed fee, and fee asset in validated market units. It rejects ambiguous duplicates,
inexact decimal conversion, and conflicting IDs. This is a fail-closed check based on captured
spec369/spec371 evidence, not a guarantee that the backend cannot change its ID scheme. Neither
the decoder nor the matcher establishes block canonicality, proves the REST timestamp, or emits
framework fills. The matching RPC responses are consistency evidence, not a Merkle proof.

The historical fixtures prove runtime event conformance for two existing public blocks.
Broader malformed, conflicting and boundary coverage still uses synthetic metadata-encoded events
and mock canonical RPC across both approved runtime versions. This does not prove current
signer/subaccount authorization, private-event decoding, or framework lifecycle coordination. No
transaction was submitted to obtain this coverage.

`classify_perp_place_framework_order_events` derives a pure, non-emitting framework eligibility
decision from durable placement state. Transmission must have crossed the durable `submitting`
boundary before `OrderSubmitted` is eligible. Pool acceptance and non-final inclusion remain
non-terminal. Only finalized matching placement evidence is eligible for `OrderAccepted`, using
the verified timestamp nonce as venue order ID, while finalized authoritative failure is eligible
for `OrderRejected`. Not-included evidence is not rejection, and `ActionRequired` fails closed
because the lifecycle no longer proves its preceding submission state. This decision is not a
delivery receipt; durable staging and crash-safe delivery acknowledgement remain prerequisites for
startup emission. Framework submission stays disabled.

Version 5 added immutable framework context only to framework-owned perpetual placements. The
initial durable reservation retains trader, strategy, instrument, client-order, account, command,
initialized-order, correlation and causation identities, both initialization timestamps, and
distinct preallocated IDs for the future submitted and mutually exclusive terminal event. Scope
and event-ID conflicts fail before nonce allocation. Generic opt-in transaction records have no
framework event authority. Version 6 added a pending-only durable outbox. Version 7 records exact
consumer acknowledgement for every staged event. Version 8 adds durable `PerpModify` call identity,
canonical signed-call verification, and finalized cancel-and-replace event recovery. Revision-checked
CAS staging records every
currently eligible event with its stable ID and first `ts_event`/`ts_init`; repeated staging
preserves those timestamps, terminal payloads must match finalized evidence, and the entry retains
its reconciliation origin. A pure materializer reconstructs only pending Nautilus events in
Submitted-before-terminal order with exact durable fields and command causation. It does not send
or mutate them. Execution ingress now offers a one-shot consumer receipt only after the
exact event is present in canonical order history. Exact replay returns `AlreadyApplied`; missing
orders, invalid transitions, event-ID payload conflicts, and routing paths that cannot observe the
engine remain rejected or unconfirmed. Channel admission never completes this receipt. With no
cache backing it proves in-memory application only. PostgreSQL and Redis cache backings complete a
separate receipt only after their database worker finishes the idempotent event write. PostgreSQL
rejects an existing event ID whose complete stored payload differs. Redis avoids duplicate append
on exact replay and confirms only after order replay and index updates finish. Write failure,
payload conflict, or a dropped worker returns `Failed`; a backing without receipt support stays
`Unconfirmed`. Only an exact `Applied` or `AlreadyApplied` receipt paired with `Persisted` can be
committed through the signer lease and revision-checked CAS. The commit marks that exact event
acknowledged, repeated acknowledgement is idempotent, and a terminal event cannot advance first.
The receipt alone never mutates the outbox, and every other outcome remains pending. After durable
transaction reconciliation proves every record complete, the startup coordinator reloads the
latest revisions, stages eligible framework events, emits only pending entries in transition order,
waits up to 30 seconds for each final receipt, and commits each acknowledgement before proceeding.
Dispatch failure, timeout, sender loss, an unconfirmed receipt, or uncertain CAS blocks startup and
retains pending state. A crash after consumption but before acknowledgement commit replays the same
stable ID; `AlreadyApplied` plus a fresh durable database receipt closes that window.
PostgreSQL restoration rejects version 6 and older records explicitly; there is no migration or
replay of their signed bytes. This coordinator emits recovered order events only and never
transmits a transaction. New framework `submit_order` commands use a separate durable one-shot
submission coordinator.

## Opt-in durable REST submission

`submit_rest_transaction_once(client, prepared, runtime_permit)` consumes an acknowledged durable
`DeepXPreparedSubmission` only while its retained runtime permit is active and matches the durable
runtime identity. A revoked or mismatched permit prevents the HTTP request. The operation identity
selects the documented `marketType` and
`action` for perpetual placement, close, TP/SL, cancellation, or Spot placement/cancellation.
The exact signed SCALE bytes are sent to `/internal/v1/chain/tx/transact` once, using the
primary endpoint and a redirect-rejecting transport, without read retry or endpoint failover.
The OpenAPI action set does not include atomic perpetual modification, so the REST boundary
returns `NotSent` for `PerpModify` before sending a request. Tracked modification uses the direct
RPC submission path.

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
Perpetual account-trade pages also preserve the requested timestamp order across page boundaries.
A read-only `sort=ASC` query on 2026-10-09 for the contributor wallet's market 3 subaccount
returned ten trades in nondecreasing `createdAt` order. The captured trade fixture is descending;
report test servers reverse that fixture when serving the ascending report request.
The documented incremental-only `user_perp_order_history` WebSocket channel acknowledged separate
market-3 subscriptions for all four subaccounts of wallet
`0x781ed35b167068c93dfadab41dfb680edaca4e50` on 2026-10-10. The read-only observer binds
each acknowledgement to its exact market and subaccount. No order update arrived during the bounded
probe, so no data-frame schema, history completeness, or framework event conversion is inferred.
The corresponding `user_spot_order_history` channel acknowledged all four wallet subaccounts for
`ETH/USDC` on 2026-10-10. Its observer also binds the response to the selected REST pair ID.
The Spot channel is incremental-only; no data frame or complete history was observed.
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
Python exposes `get_deepx_wallet_funding_fees` and the bounded
`get_deepx_wallet_funding_fee_pages` collector using the same typed validation. Position size,
fee, and fee rate remain exact decimal strings. The wallet response is mutable and does not
independently prove ownership or authorize account events.
The wallet-wide trade response groups records by subaccount and can return different nested cursors
while accepting only one request cursor. Without a documented traversal rule,
`get_perp_wallet_trades(request)` exposes one validated grouped trade snapshot and preserves each
nested cursor independently. It validates unique market/subaccount
groups, requested market and time scope, exact trade/order identities and decimals, group-local
ordering, page sizes, cursor metadata, and duplicate trade IDs. It allows zero size because the
captured response contains one such historical record; this raw observation is not a framework
fill report. The response does not echo the requested wallet and is not joined to the separately
mutable subaccount directory, so it does not independently establish ownership.
Python exposes `get_deepx_perp_wallet_trades` for one validated grouped response and
`get_deepx_perp_wallet_trade_pages` for bounded pagination of one selected market/subaccount
group. Trade prices, sizes, leverage, and fees are returned as exact decimal strings. These
read-only REST results can change between requests, do not prove wallet ownership, and are not
framework fill reports.

A read-only run on 2026-09-17 queried the contributor wallet and validated two markets, five
market/subaccount groups, and 22 records. Three groups advertised continuation through three
distinct cursors, directly confirming why no aggregate page collector is exposed. Run
`deepx-verify-rest-wallet-trades <wallet-account-id20> [market-id]` for the same credential-free
single-snapshot check. `get_perp_wallet_trade_pages_for_subaccount` follows the cursor of one
explicit market/subaccount group within a page budget. It rejects missing groups, duplicate trade
IDs, reversed time order, repeated cursors, and exhausted budgets without returning partial pages.
The verifier accepts an optional `<subaccount> <max-pages>` pair after the market ID to exercise
that read-only traversal. Neither method proves wallet ownership or a stable historical snapshot.
On 2026-10-09, market 3 for wallet `0x781ed35b167068c93dfadab41dfb680edaca4e50` exposed one
continuation group. Its subaccount `0x977654069311fa41b88f7901dc65fae9a76fd60b` reached a
terminal page after eight requests with 100 records per page, yielding 727 distinct records in
requested time order. This is a mutable read, not a block-pinned completeness proof.
A later read-only market-3 query returned three subaccount groups and 12 first-page trades, with
one continuation group. Subaccount `0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc` returned a
terminal page containing five trades when queried with a page size of 100. These observations do
not establish a stable wallet-wide trade snapshot.

A read-only query on 2026-09-24 repeated the wallet trade observation and counted four distinct
subaccounts across five market/subaccount groups, two markets, and 22 records. Three groups still
advertised distinct continuation cursors. The bounded wallet-order reader returned 70 records over
14 pages, two markets, and four distinct subaccounts. These are mutable REST observations; the
trade request is only its first grouped page, and neither response proves a block-pinned snapshot.

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
`deepx-verify-rest-wallet-open-orders <wallet-account-id20> <market-id>` first resolves the
current subaccount directory, then validates bounded perpetual open-order pages for every listed
subaccount. It reports per-subaccount counts only after every query succeeds. These sequential
REST reads include a final directory membership recheck but are not an atomic wallet snapshot or
finalized order evidence.
The typed open-order reader accepts only `Open` with no fill or `PartiallyFilled` with exact
filled-plus-remaining quantity and a positive average fill price. Terminal, unknown, and
inconsistent states fail closed before a page is returned.
A read-only market-3 query on 2026-10-11 returned zero open orders across the four subaccounts of
wallet `0x781ed35b167068c93dfadab41dfb680edaca4e50`; this does not prove a finalized absence.
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
profile, exact asset balances, equity, margin ratio, quota summary, and invitation-bound status.
When a market ID is supplied it also validates each nullable liquidation-price observation. It
never submits transactions.
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
locally tracked or retained terminal `OrderContext`. Perpetual reports accept matching `Limit` and
`Market` orders; Spot reports accept matching `Limit` orders. Both verify subaccount, market, order
identity, side, quantity, limit price, post-only,
reduce-only, status/quantity consistency, exact filled-quantity precision, and timestamps, and takes
time-in-force only from immutable local context because REST omits it. System-generated `Stop`
orders, `Adaptive` post-only values, quote-denominated quantities, external or unbound orders,
missing Perpetual `updatedTime`, and incomplete average-fill evidence fail closed. Spot reports
reconstruct filled base quantity from bounded order-specific trade history; canceled Spot orders
require the timestamp read from their exact finalized cancellation block. An `IocUnfilled`
cancellation reason must agree with the tracked IOC time-in-force. The single-order framework
report method resolves only a locally bound client or venue order ID, validates any supplied
identity against immutable local context, resolves the market only from the failure-atomic startup
catalog snapshot, and performs the bounded configuration-owned REST read. Startup retains matching
Spot pair and instrument indexes alongside the perpetual market indexes and clears them on reset.

A read-only sample of 600 public ETH/USDC Spot history records on 2026-10-09 contained seven
`Filled` Limit orders whose `quoteAmount` differed from `baseAmount` times the limit `price`.
The report therefore checks the original quote notional for zero-fill orders; nonzero fills
require exact base-quantity reconstruction from order-specific trade records. One improved-price
Buy order had limit price `2450.46` and submitted base quantity `0.2976`, but its single trade at
`2450.44` and Filled record both reported base quantity `0.297602428951535`. For this Filled Buy
case, the report preserves the executed base precision and accepts an overfill only when the
reported quote remains within the submitted quote budget and exactly matches the trade quote sum.
Every reconstructed trade must also respect the tracked Buy or Sell limit price.
These observations do not establish a complete quote-amount lifecycle rule or Spot IOC behavior.

`generate_order_status_reports` provides strict bounded reconciliation for locally tracked Spot and
perpetual orders. It reads active pages for each selected startup market and, when `open_only` is
false, also reads history pages. Each endpoint and market is limited to 100 pages of 100 records.
Every returned order must bind to retained tracked or terminal `OrderContext`, because the REST
schema omits authoritative time-in-force; an external or unbound order fails the whole request.
Canceled Spot reports also require finalized timestamp evidence for the REST cancellation height.
Active pages must contain only open statuses, and conflicting evidence for one venue order across
the mutable active and history reads fails atomically. Optional command bounds filter exact
`ts_last` values, after which identical overlap is deduplicated and output is deterministic. This
does not prove a block-pinned or complete account snapshot and therefore does not authorize
complete mass-status coverage. A credential-free testnet check on 2026-09-23 resolved four Active
subaccounts for wallet `0x781ed35b167068c93dfadab41dfb680edaca4e50`; the known active
subaccount's market-3 active-order query returned one valid terminal page with zero records.

`generate_position_status_reports` reads current positions for the configured subaccount, optionally
restricted to one preloaded perpetual instrument. DirectPallet uses perpetual-position storage and
`Timestamp.Now` from one fixture-approved finalized checkpoint. It converts raw base amount and entry
price only when they exactly match immutable instrument precision and increments. Its `ts_last` is
the checkpoint's `Timestamp.Now` observation time, not a position last-change time. The execution
path reads only perpetual-position storage plus chain time from that exact checkpoint; unrelated
margin, lending, and active-order reads do not gate the report. LegacyEvm retains
the bounded REST path, reading at most 100 pages of 100 lifecycle records and converting only `Open`
records from `isLong`, `baseAssetAmount`, `entryPrice`, and `updatedAt`. Historical `Closed` records
are omitted. Unknown markets, missing or inconsistent catalog identities, duplicate open markets,
invalid timestamps, increment mismatch, and precision loss reject the whole result. Command time
bounds filter by `ts_last`. The method does not synthesize flat rows or expose the REST lifecycle ID
as a stable venue position ID, and `provides_bulk_position_coverage` remains false because neither
path establishes complete product coverage.

`generate_mass_status` composes all current tracked active orders, perpetual fills bounded by the
optional minute lookback, and all current perpetual positions under one initialization timestamp.
The historical window applies only to fills; current active orders and positions are not removed
because their last update predates it. Overflowing lookback arithmetic saturates at the Unix epoch.
All three sources must succeed or no mass status is returned. Successful results always set
`reports_complete=false`: the independently paginated mutable REST reads do not form an atomic
account snapshot, and position-product completeness remains unproven. This deliberately selects
the framework's conservative incomplete-report reconciliation behavior. Perpetual Limit GTC/IOC
and Spot base-quantity Limit GTC/IOC initial submissions use durable DirectPallet paths that record
`OrderSubmitted` before sending; terminal events require finalized reconciliation. Tracked
Perpetual and Spot Limit GTC cancellation also use durable DirectPallet paths. Independent order
lists of at most 20 members use non-atomic durable single-order placement; contingent and linked
lists remain disabled. Modifications and private execution streaming remain disabled. Spot
submission and cancellation have not been validated with live testnet transactions. Execution
connection startup is available only for the strict finalized flat-account subset described below.
The pinned Python SDK contributes signing construction but no account-history mapping evidence. The
chain source confirms the underlying order status, taker, fill-direction, and active-position
storage types, but not REST lifecycle IDs, fee-asset presentation, or historical snapshot rules.

## Operational REST instrument discovery

The Rust `DeepXDataClient` now supports read-only testnet REST connection and perpetual
instrument discovery. `connect` requires an initialized live framework data receiver, loads
both public market lists using the configured retry/failover transport, validates the complete
perpetual instrument set before emitting it. `request_instrument` and `request_instruments`
independently refresh and validate the complete catalog before returning a correlated response.
They preserve canonical client and venue routing, empty parameters, and the injected framework
clock's response timestamp. Failed refreshes emit no partial response or change to the connected
store. Time-bounded history, nonempty filters, foreign identities, unknown instruments, and Spot
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

The REST-connected data client supports `request_bars` for loaded perpetual and Spot instruments.
Requests must use a standard, externally aggregated, last-price `BarType` whose interval is one of `1m`,
`3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `8h`, `12h`, `1d`, `3d`, `1w`, or `1M`. The client
preserves correlation, client identity, the requested bar type, optional bounds, and parameters in
an ascending `DataResponse::Bars`. The optional limit may not exceed the venue maximum of 5000.

Each response must carry the instrument's raw venue pair and timestamps inside the requested
framework bounds. Live Spot and perpetual REST candle queries showed that `end` is exclusive; the
client adds one millisecond to the inclusive framework upper bound before requesting either market.
OHLC prices and base volume are converted only when exactly representable at the instrument's
declared price and size precision; nonpositive prices, precision loss, invalid
OHLC, negative volume, foreign identity, or an unrepresentable timestamp reject the whole response.
The venue's `time` is preserved directly as `ts_event`. The OpenAPI does not define whether it is a
bucket-open or bucket-close timestamp, so the adapter does not shift it or infer completion. Exact
nanosecond bounds filter the validated millisecond response.

The request uses the same owned, epoch-fenced task dispatcher as trade and funding history. Invalid
bar specifications, nonempty parameters, foreign identities, unknown instruments, negative or
inverted bounds, and excessive limits fail before HTTP. Perpetual and Spot candle history is
collected in ascending time pages, starting each continuation one millisecond after the preceding
page's final candle. An explicit request limit caps the total response; otherwise collection stops
at 100 pages of 5000 rows and returns an error without partial data if another page is required.
Market identity changes, non-advancing timestamps, transport, bounds, or conversion failures also
emit no partial response. The public WebSocket documents a perpetual `candles` channel; observed
one-minute frames repeatedly revised the current bucket and the next observed bucket skipped one
minute. Shared decimal decoding accepts scientific JSON numbers only when they are exactly
representable; excess precision rejects the observation. A raw protocol state machine binds
market, channel, interval, and pair, returns a bucket only after a later bucket arrives, and
reports skipped minutes. The read-only
`observe_first_superseded_perp_candle` API returns one observation and closes its connection.
On 2026-10-08, a live market 3 `ETH-USDC` observation returned bucket `1791478800000`, volume
`56.768`, and `gap_after=false`.
A later same-bucket comparison returned WebSocket volume `65.511` and REST volume `52.995`, with
different close and high values. A repeat REST read returned volume `65.511` and the same close as
WebSocket, but REST high remained `2418.1899999999996` against WebSocket `2418.19`. A successor
bucket therefore does not establish final OHLCV, and REST history may lag and carry decimal
artifacts that fail exact instrument-precision conversion.
On 2026-10-09, the first REST read of bucket `1791492060000` reported volume `38.654` while the
superseded WebSocket update reported `65.426`; a later REST read matched the WebSocket OHLCV
exactly. `deepx-verify-ws-candles` now allows up to 90 seconds for the exact bucket to converge,
but does not treat convergence as a finality guarantee. A repeat observation of bucket
`1791492300000` matched on the 33rd REST read without a skipped minute.
Framework streaming bars support only externally aggregated, last-price, one-minute perpetual
candles. A bar is emitted when a newer bucket supersedes it, with the venue bucket timestamp as
`ts_event`. This is an observed update, not a finality guarantee: the venue may revise a bucket
after its successor appears. The subscription stops on malformed data, a skipped bucket, a
disconnect, or 180 seconds without another completed bucket. It does not reconnect or backfill, so
clients must explicitly resubscribe and reconcile any missing interval before relying on a
continuous series. Spot and other intervals remain unsupported.
On 2026-10-11, `deepx-verify-live-trades --bars` published one market-3 framework bar for bucket
`1791670680000` with open `2506.14`, high `2506.22`, low `2505.67`, close `2506.22`, and volume
`26.008`, then retired the subscription. This confirms one observed delivery and shutdown, not
historical continuity or finality.
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

Independent SDK complete-byte vectors now cover System remark, Subaccount no-op, maximum-u128
perpetual Limit GTC, Limit IOC, Market IOC, and ordinary Perp cancellation for the spec366 and
spec369 snapshots. A sell-side reduce-only `MustPostOnly` placement also checks both order flags.
The offline verifier accepts `--spec-version 369` to select the new captured fixture. These vectors
prove only the stated encodings, not all calls, authorization,
financial units, event success, inclusion/finality, private streams, or complete live adapter
readiness.

## Captured spec371 runtime support

A read-only finalized account query on 2026-09-28 observed testnet block `202153410` with
hash `0xc98dd52706b8585459804142c8b43ba2447661a671f405a2c4b0cfce37fa969c`, runtime
spec371/tx1, and metadata SHA-256
`4a620bad9f2482b5511f4637d03d5e7907b903f4bab5af439f664963724cda15`. The checked-in
fixture binds that runtime metadata to its finalized block identity. The pinned Python SDK
verifier now accepts spec371 and reproduces complete bytes for System remark, Subaccount
no-op, perpetual Limit GTC, Limit IOC, Market IOC, and ordinary Perp cancellation. It also
reproduces the sell-side reduce-only `MustPostOnly` placement. The spec371 run additionally covers
Spot placement/cancellation, Perp close and PnL point management, and Subaccount deletion. Delegate
setup, mode change, and removal vectors match under spec366, spec369, and spec371. This does not
authorize submission or prove every
operation, account authorization, financial-unit conversion, or live execution readiness.
An October 8, 2026 read-only RPC check still reported spec371/tx1 and the same metadata SHA-256;
the captured runtime still provides no `Subaccount.PerpOrderModified` sizing event.

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

The SDK signing source at GitHub blob `0108f89d79366f13e747ef59c75ce73a543dc404`
provides `build_signed_pallet_call_extrinsic` using an ECDSA keypair and the separately pinned
`_runtime_compat.py` blob `06b2e9fc84cc36c647452027b9c5b2cbbfcafb8c`.
`bin/verify_sdk_signing.py` executes the reviewed builder, key constructor, and compatibility
helper with an offline fixture transport. The approved snapshots use `CheckMortality`, for which
the helper delegates to `substrate.create_signed_extrinsic`; its separate `CheckNonceEra` path is
not validated or enabled by these vectors. For each captured spec366,
spec369, and spec371 runtime, it proves complete-byte parity for System remark, Subaccount no-op,
delegate setup, both writable delegate modes, delegate removal, perpetual Limit GTC/IOC,
Market IOC, ordinary Perp cancellation, Perp close with absent or 50 raw slippage, Perp profit/loss
point management, and sell-side reduce-only
`MustPostOnly` for spec366, spec369, and spec371. Spot Buy Limit IOC is checked under spec369 and
spec371, as is Spot Buy `Market(None)`. Under spec371, it also checks Spot Limit GTC, Sell IOC,
and Sell `Market(Some(50))` placement, Spot
cancellation, and Subaccount
deletion. These offline bytes do not establish live Spot IOC behavior. The
Limit GTC vector uses maximum u128 size/price/take-profit and an absent stop-loss. Rust
signing tests assert the same SDK-produced vectors, including spec369 `modify_orders`. Parity for
other calls, parameter combinations,
or unlisted runtimes remains unproven.

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
subaccount authorization. The framework Spot cancellation path is described below; this offline
preparation API alone grants no command or submission authority.

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

## Offline subaccount initialization signing

`signing::sign_initialize_subaccount(permit, key, params, nonce)` accepts opaque name bytes in
`DeepXInitializeSubaccountParams`. Captured spec366/tx1 metadata declares
`Subaccount.initialize_subaccount` at pallet 19, call 0 with one `name` field. The read-only chain
source bounds that field to 32 bytes; the typed signer rejects longer names before encoding. Tests
cover empty, 32-byte, and ordinary names, exact SCALE call bytes, and a complete spec371/tx1 signed
extrinsic independently matched to the pinned Python SDK.

The resulting subaccount address depends on chain state and block number. This offline API neither
predicts that address nor checks name uniqueness, owner eligibility, quota, dispatch, or finality.
It reserves no nonce and sends no transaction; live subaccount creation remains disabled.

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

## Offline subaccount renaming signing

`signing::sign_rename_subaccount(permit, key, params, nonce)` encodes the owner-signed
`Subaccount.rename_subaccount` call under an approved runtime permit. The local node source and
approved metadata for specs 366, 369, and 371 agree on pallet 19, call 7, followed by a 20-byte
subaccount and a bounded name of at most 32 bytes. The signer rejects longer names before
encoding. Complete extrinsics for specs 366, 369, and 371 match the pinned Python SDK builder
byte-for-byte.

This offline signer does not check ownership, name uniqueness, dispatch, or finality. It reserves
no nonce and sends no transaction; operational subaccount management remains disabled.

## Offline Spot margin setting signing

`signing::sign_set_spot_margin(permit, key, params, nonce)` signs the owner-scoped
`Subaccount.set_spot_margin` call with an exact subaccount AccountId20 and a Boolean setting.
Approved metadata for specs 366, 369, and 371 declares pallet 19, call 6, with `H160` and `bool`
arguments. Complete signed extrinsics for enabling the setting match the pinned Python SDK under
all three runtime snapshots; local tests also check exact SCALE bytes for both Boolean values.

The inspected chain call verifies ownership and rejects a change while the subaccount has open Spot
orders. It changes `SubaccountInfo` without emitting a dedicated business event. Dispatch success
alone therefore does not prove the resulting flag; an operational workflow still needs a finalized
state read at the same canonical block and a durable transaction lifecycle. A read-only REST query
on 2026-10-10 returned `spotMarginTradingEnabled: false` for all four subaccounts of wallet
`0x781ed35b167068c93dfadab41dfb680edaca4e50`. Those mutable, non-block-pinned profiles do
not establish signing authorization or current chain state. This signer reserves no nonce and
submits no transaction; operational Spot margin changes remain disabled.

`rpc::observe_finalized_spot_margin_evidence` now reads `Subaccount.SubaccountInfo` at an approved
finalized runtime checkpoint. It requires a caller-supplied expected wallet authority, decodes the
complete SCALE value against that checkpoint's approved metadata, checks both account identities,
compares `state_queryStorageAt` with `state_getStorage` at the same block hash, and rechecks the
canonical hash for the checkpoint height. The built-in spec-376 metadata declares the
identity-keyed map. A read-only spec371 capture at finalized block `217394490` for
subaccount `0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc` decoded the flag as `false`;
both storage methods returned identical bytes, and a later height lookup returned the same hash.
These matching RPC responses are consistency evidence, not a cryptographic storage proof. The
public observer assumes node runtime compatibility and retains finalized-height, repeated-storage,
and canonical-hash checks. No Spot-margin mutation or transaction-result confirmation is enabled.

## Offline wallet delegate setup signing

`signing::sign_set_delegate_account(permit, key, params, nonce)` accepts
`DeepXSetDelegateAccountParams` with an exact 20-byte `delegate`, opaque `name` bytes, and a raw
Unix millisecond `valid_until`. The captured spec366/tx1 metadata declares
`Subaccount.set_delegate_account` at pallet 19, call 2 with `H160`, `Vec<u8>`, and `u64` fields in
that order. Subxt encodes these fields against the permitted immutable runtime snapshot. Tests
cover exact SCALE call bytes across empty and compact-length-boundary labels and one complete
signed extrinsic independently matched to the pinned Python SDK under each approved runtime.

This API does not check delegate ownership, expiry against chain time, naming policy, quota, or
authorization. It performs no network request, nonce reservation, persistence, submission, or live
management activation. Runtime dispatch, business-event, finality, and authorization evidence
remain required before an operational delegate command can be enabled.

## Offline wallet delegate mode signing

`signing::sign_update_delegate_mode(permit, key, params, nonce)` accepts an exact 20-byte delegate
address and either `PlaceOrCancelOrder` or `Disable`. Captured spec366/tx1 metadata declares
`Subaccount.update_delegate_mode` at pallet 19, call 4 with `address: H160` and
`new_mode: DelegateMode`. The read-only chain source rejects the enum's `DepositOrWithdraw` and
`UpdateSubaccount` variants during dispatch, so the typed signing API cannot select them. Tests
cover both SCALE variant indices and complete signed bytes for both writable modes under each
approved runtime, independently matched to the pinned Python SDK.

A read-only 2026-10-09 query for wallet `0x781ed35b167068c93dfadab41dfb680edaca4e50`
returned one active delegate in `PlaceOrCancelOrder` mode. That mutable REST observation does not
prove authorization, expiry at execution time, or a successful mode change. This offline signer
does not reserve a nonce or submit a transaction; operational delegate management remains disabled.

`transaction::decode_finalized_delegate_mode_changes` decodes complete `System.Events` bytes with
approved metadata, preserving the supplied block hash and number, global event index, extrinsic
index, owner, delegate, and all four runtime mode variants. It rejects truncated or trailing bytes,
invalid fields, and a mode update outside an extrinsic phase. Metadata-encoded tests cover all four
runtime modes and malformed event storage. The caller must separately prove the block is canonical
and finalized and bind the extrinsic to its signed call; no live mode-change event has been verified
for this wallet.

`transaction::verify_finalized_delegate_mode_change_events` binds exactly one matching mode
change and one successful dispatch to a finalized transaction location from the Watch observer.
It rejects missing, duplicate, failed, or conflicting same-index evidence. The caller must supply
complete event storage read at that location's block hash and independently verify that the located
signed extrinsic encodes the expected wallet call. This event check alone does not activate delegate
management.

`transaction::observe_finalized_delegate_mode_change_at` reads a previously located extrinsic from
the Watch endpoint, rechecks its finalized canonical membership, selects fixture-approved historical
metadata, reads complete event storage twice at that block hash, and verifies the expected event and
dispatch before checking membership again. Local RPC tests reject non-finality, historical runtime
inconsistency, changed storage or canonical hash, unapproved metadata, and malformed storage.
The endpoint observations do not replace a cryptographic storage proof, and the caller still must
bind the target signed extrinsic to the intended wallet call. No live mode change for the specified
wallet has been verified.

`transaction::observe_signed_delegate_mode_change_at` binds an owned offline-signed call to the
chain observation without submitting it. Its `DeepXSignedDelegateModeQuery` carries the delegate,
mode, nonce, and requested height. The observer regenerates exact signed bytes from the retained
runtime permit and key, locates that hash in the requested finalized block, requires the historical
runtime to match the signing permit, and verifies the same-index event and dispatch. A height above
the finalized head returns a pending checkpoint. Local RPC tests cover
exact membership, a changed nonce, pending finality, and signing-runtime mismatch. Historical
success does not establish current delegate permissions or enable management submission.

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
semantics. A complete signed removal vector matches the pinned Python SDK under each approved
spec366, spec369, and spec371 snapshot; other argument combinations remain unproven. Maintainer
approval is confirmed; authorization, business-event/finality evidence, and live conformance
remain gates. Local spec369 sources are not substituted for the captured fixture.

`transaction::decode_finalized_delegate_account_changes` decodes complete `System.Events` under
approved metadata for `DelegateAccountSet` and `DelegateAccountRemoved`. It preserves block identity,
global event and extrinsic indices, owner, delegate, and operation kind. The runtime setup event does
not reveal the label, expiry, or whether an existing delegate was renewed.

`transaction::verify_finalized_delegate_account_change_events` additionally requires one matching
same-index event and one successful dispatch at a supplied finalized transaction location. Local
metadata-encoded tests cover both variants under approved spec-366, 369, and 371 snapshots and
reject incomplete, duplicate, wrong-phase, failed, and conflicting evidence. The caller must
independently source complete event storage at that block hash and bind the located signed call
before treating the result as a wallet action.

`transaction::observe_finalized_delegate_account_change_at` sources those events from the Watch
endpoint at a finalized canonical location. It verifies fixture-approved historical metadata,
reads complete event storage twice at the block hash, checks the expected account event and
successful dispatch, and rechecks exact extrinsic membership. Local RPC tests cover both setup and
removal, changed storage, and a conflicting operation. This observation still does not bind the
signed setup label or expiry and does not establish current delegate permissions.

`transaction::observe_signed_delegate_account_change_at` binds an owned setup or removal call to
that chain observation without submitting it. It regenerates the exact signed bytes from the
retained runtime permit, key, typed operation, and nonce; locates the hash at the requested finalized
height; requires the historical runtime to match the signing permit; and verifies the matching
same-index event and dispatch. Setup label and expiry remain bound through the signed call even
though the event omits them. Local RPC tests cover both operations, pending finality, changed nonce,
label, and expiry. Historical success does not prove present delegate permissions.

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
signed identity, deterministic signing, and runtime-change permit gating. Complete signed vectors
for the same raw point values match the pinned Python SDK under spec366, spec369, and spec371.
Other argument combinations remain unproven. The captured metadata and offline SDK vectors prove
encoding under those versions, not live financial or trigger semantics.

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
Complete signed vectors match the pinned Python SDK under spec366, spec369, and spec371 for
absent slippage and `Some(50)` raw slippage. Other argument combinations remain unproven.

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
startup explicitly selects operation-specific observers for Spot placement, ordinary Spot
cancellation, and the perpetual operation families. This adds canonical-byte checks before
not-included recovery RPC; fast Spot cancellation remains unsupported. Startup still requires every
durable transaction to become complete, and action-required records cannot enable account
registration. Local tests cover normal/corrupt startup records for Spot placement, Spot cancellation,
and perpetual cancellation. This API does not submit, replay, emit order events, or enable commands.
It establishes no independent SDK golden-vector parity; local spec369 node sources cannot
prove spec366 behavior. Maintainer approval, independent SDK parity, and testnet Spot order and
cancellation conformance remain unresolved.

## Read-only wallet delegate directories

The Rust HTTP client provides typed `get_delegate_accounts(wallet)` and
`get_delegator_accounts(delegate)` readers for both directions of the public delegate directory.
Python `get_deepx_delegate_accounts(wallet, config=None)` and
`get_deepx_delegator_accounts(delegate, config=None)` expose the same validated directories as
read-only, blocking one-shot calls. Async Python callers can use `asyncio.to_thread`.
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

A repeat read-only query on 2026-10-09 found the same active delegate at
`0x1b856b9bf1d0ceeb0927a081c759813cd703a54f` in `PlaceOrCancelOrder` mode. The reverse
directory again contained wallet `0x781ed35b167068c93dfadab41dfb680edaca4e50`.

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
amounts, token quantities whose significant scale exceeds each asset's declared decimals, and
negative deposit or borrow totals. Equity and unrealized PnL remain signed.

`get_subaccount_margin_ratio(address)` retains the legacy `collateral`, `marginRequired`, and
nullable `marginRatio` fields as rounded displays. The 2026-10-11 internal OpenAPI and a read-only
testnet response now include `exactMargin.totalCollateral`, `maintMarginReq`, and nullable `uniMMR`
as decimal strings. The reader requires these exact fields, rejects negative values, requires
`uniMMR` to be null exactly when maintenance margin is zero, and checks non-null ratios against
collateral divided by maintenance margin truncated to eight places. For the contributor's market-3
subaccount, the exact collateral is `970.964181` USDC while the legacy display is `970.96`.
Historical responses without `exactMargin` remain parseable as fixtures but are rejected by the
live reader. Exact maintenance margin still does not establish initial margin, free collateral,
or a block-pinned account state, so this reader does not construct a Nautilus `MarginBalance`.

`get_wallet_account_snapshot(address)` composes the directory, user stats, authority-bound profile,
exact balances, equity, and margin-ratio reads for every returned subaccount. It requires the
statistics subaccount set to match the directory, rechecks directory membership after the account
reads, preserves directory order, and returns no partial collection on failure. Because REST
responses are not block-pinned, this API does not claim
cross-request venue snapshot atomicity.

Python `get_deepx_wallet_subaccounts(wallet, config=None)` exposes the validated directory alone.
`get_deepx_wallet_account_snapshot(wallet, config=None)` exposes the complete failure-atomic Rust
composition as a blocking one-shot read. The margin-ratio object exposes the exact collateral,
maintenance requirement, and `uniMMR` separately from legacy rounded displays. Its other
financial fields are decimal strings, and
uninterpreted Spot-position items remain raw JSON strings. Async Python callers can use
`asyncio.to_thread`. Neither function establishes current chain ownership or a synchronized
account balance across its separate REST requests.

`get_balance_changes(request)` decodes one page of signed exact balance deltas for exactly one
wallet or subaccount. `get_balance_change_pages(request, max_pages)` follows cursors within an
explicit nonzero page budget, preserves page boundaries, rejects duplicate record identities
across pages, and returns no partial collection on failure. Wallet-scoped pages can contain
positions owned by multiple subaccounts. Cross-chain extensions remain raw JSON. These mutable,
non-block-pinned REST observations do not prove complete account history, free/locked semantics,
or current balance state and are not converted into Nautilus account state.

Python `get_deepx_balance_changes(wallet=..., ...)` or
`get_deepx_balance_changes(subaccount=..., ...)` returns one validated page with its opaque cursor.
It accepts timestamp, change-type, cursor, and page-size filters and requires exactly one account
scope. Signed changes are exposed as exact decimal strings; optional position identity and raw
cross-chain JSON remain separate. The call is blocking; async Python callers can use
`asyncio.to_thread`. It does not infer a current balance from historical deltas.

Python `get_deepx_balance_change_pages(*, max_pages, wallet=None, subaccount=None, ...)` returns
all validated pages within an explicit positive budget, preserving page boundaries and exact
signed decimal strings. Duplicate record IDs, invalid cursors, and budget exhaustion reject the
whole collection; each page is validated before the next cursor is requested. It remains mutable
REST history rather than a finalized or synchronized account balance.

A read-only Rust verification on 2026-09-16 queried the contributor-provided wallet and validated
18 `USDC` records across one page, including funding-fee, settlement, liquidation-fee, and
withdrawal changes. Position-linked records spanned three subaccounts. No credentials or
transactions were sent.

On 2026-10-11, the bounded Python reader queried the same wallet and validated one terminal page
with 18 records: ten funding fees, six settlements, one liquidation fee, and one withdrawal.
Position-linked records named all four current subaccounts. The mutable REST history was not
reconciled with finalized events; no credentials or transactions were used.

`get_liquidation_records(request)` decodes one page of exact raw-unit liquidation records for
exactly one wallet or subaccount. `get_liquidation_record_pages(request, max_pages)` follows the
opaque cursor within an explicit nonzero page budget, preserves requested time order across page
boundaries, rejects duplicate record IDs, and returns no partial typed collection on failure. The
closed variants are `LiquidatePerp`, `LiquidateSpot`, `PerpBankruptcy`, and `SpotBankruptcy`. Raw
integer amounts, fees, and oracle values retain protocol units; JSON-encoded liquidation details
and canceled-order identities are structurally validated but remain uninterpreted. Wallet scope can
span multiple target subaccounts, and no Nautilus liquidation or risk event is constructed.

Python `get_deepx_liquidation_records(*, wallet=None, subaccount=None, ...)` exposes one typed
page; `get_deepx_liquidation_record_pages(*, max_pages, wallet=None, subaccount=None, ...)`
collects validated pages within a required positive budget. Raw u128 values are decimal strings,
and embedded detail fields remain JSON strings. The bounded collector validates each page before
following its cursor and returns no partial collection on failure.

A read-only Rust verification on 2026-09-17 queried the contributor-provided wallet and validated
14 liquidation records in one terminal page. The mutable REST history was not block-pinned, so the
observation does not prove snapshot completeness, canonical inclusion, or finality. No credentials
or transactions were sent.

On 2026-10-11, the bounded Python reader again validated one terminal wallet page with 14
`LiquidatePerp` records targeting two subaccounts. This current REST observation still does not
prove finalized liquidation events. No credentials or transactions were used.

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

A repeat read-only wallet snapshot on 2026-09-24 returned the same four Active subaccounts with
equity values `970.96`, `1143.57`, `11.08`, and `1.34` USD. Each reported zero borrow and
`marginRequired=0.0`; all `marginRatio` values remained null. The wallet quota reported
`perp_volume_usd=2970.52758`, `quota_pending=2970`, and no granted or reserved quota. The public
delegate directory also reported one active `PlaceOrCancelOrder` delegate. These REST observations
are mutable and do not establish chain authorization or a block-pinned account snapshot. No
credentials or transactions were sent.

`get_user_stats(address)` validates the wallet's subaccount addresses, rejects duplicates, requires
the current count to match the returned directory, requires the cumulative created count not to be
smaller, and preserves `ifStakedQuoteAssetAmount` as an exact nonnegative decimal. The OpenAPI does
not establish its asset, scale, or account-balance meaning, so no currency or Nautilus balance is
constructed. Python `get_deepx_user_stats(wallet, config=None)` exposes the same validated read,
with the staked amount as an exact decimal string. `get_perp_liquidation_price(request)` requires
exactly one market name or ID, binds the returned subaccount and market identity to that request,
and preserves a positive exact price or
`None`. It does not infer liquidation methodology, price units, freshness, position state, or
framework risk semantics. Python `get_deepx_perp_liquidation_price(subaccount, market_id=None,
market_name=None, config=None)` exposes the same validated observation as an exact decimal string
or `None` and requires exactly one market selector.

`get_invitation_status(wallet)` returns the invitation-bound observation. Testnet currently returns a
boolean although OpenAPI shows an object; the reader accepts both forms and validates the returned
address against the request when the object form is used. `get_invitation_codes(wallet)` returns
the wallet's associated code strings without inferring a code format. Run
`deepx-verify-rest-invitation-codes <wallet-account-id20>` for a credential-free read. Neither
reader performs invitation mutations.

A read-only query on 2026-09-29 returned an empty invitation-code list for wallet
`0x781ed35b167068c93dfadab41dfb680edaca4e50`. This is a mutable API observation; no credentials
or mutations were used.

`get_spot_adl_candidates(request)` decodes the observed candidate fields, checks the returned target
and assets against the request, validates candidate addresses and unsigned raw integer strings, and
preserves amounts without financial scaling. It does not assign ranking semantics or execution
eligibility. `get_spot_adl_candidates_raw(request)` remains available for unknown response fields.
The candidate item shape is based on a read-only testnet response because OpenAPI defines only a
generic envelope. Candidate discovery does not execute ADL.
`deepx-verify-rest-spot-adl-candidates <target-subaccount> <asset> <liability>
<min-cover-raw-integer> [limit]` performs a credential-free live read of this typed response.
All amount fields are printed as raw integer strings without financial scaling.
`get_liquidation_adl_records_raw(request)` returns one page of ADL attempt records;
`get_liquidation_adl_record_pages_raw(request, max_pages)` follows opaque cursors under a strict
page budget and returns no partial result on exhaustion. The wallet/subaccount scope, filters,
block/time bounds, and cursor metadata are validated, while record items remain raw JSON because
OpenAPI has no stable item schema. These mutable REST records are not proof of chain finality.
`get_liquidation_keeper_state_raw(subaccount, chain_id)` returns phase-dependent keeper state as
raw JSON because OpenAPI defines no stable item schema. The
`deepx-verify-rest-liquidation-keeper-state <subaccount> [chain-id]` command performs a
credential-free read. The state does not establish liquidation execution or chain finality.
`get_liquidation_execution_record_pages_raw(request, max_pages)`,
`get_liquidation_cranker_record_pages_raw(request, max_pages)`, and
`get_liquidation_tp_sl_record_pages_raw(request, max_pages)` collect bounded cursor pages of
ordinary liquidation, cranker, and TP/SL executor history. The request validates exact scope,
hashes, time and block bounds, page size, and cursor metadata; record items remain raw because
the endpoint item schema is unstable. The
`deepx-verify-rest-liquidation-execution-records <liquidation|cranker|tp-sl> <wallet|subaccount>
<account-id20>` command checks any of the three routes without credentials. Mutable records do
not establish chain inclusion or finality. A read-only `curl` request for wallet
`0x781ed35b167068c93dfadab41dfb680edaca4e50` on 2026-10-09 returned HTTP 200 with one
terminal empty page (`items: []`, `nextCursor: null`, `hasNext: false`) from the ordinary liquidation
route. The Rust read-only verifier independently returned one page with zero records. This
verifies the empty response shape, not nonempty item semantics or chain finality.

Read-only verification on 2026-09-29 and again with the Rust verifier on 2026-10-09 returned four
cranker records and no TP/SL records for wallet
`0x781ed35b167068c93dfadab41dfb680edaca4e50`, all in one terminal page per endpoint. The
cranker responses identify the wallet's subaccount and include submitted transaction hashes; they
remain mutable backend observations and were not reconciled against finalized chain events. No
credentials or transactions were used.

A live read-only verification on 2026-09-16 returned four current and four cumulatively created
subaccounts, an IF staked quote amount of zero, and a null `ETH-USDC` market-3 liquidation price for
all four subaccounts. No credentials or transactions were sent.

Python `get_deepx_perp_positions(subaccount, market_id=None, only_closed=None, cursor=None,
page_size=None, config=None)` returns one validated position-lifecycle page. Each record preserves
the venue lifecycle status, ownership, timestamps, and exact decimal strings; `has_next` and
`next_cursor` retain the venue's pagination boundary. The response is mutable REST history, not a
finalized chain position or a complete account snapshot.

Python `get_deepx_perp_position_pages(subaccount, max_pages, market_id=None, only_closed=None,
cursor=None, page_size=None, config=None)` collects validated pages within a required positive
page budget. It rejects repeated position IDs and invalid cursor progression across pages and
returns no partial collection on failure. Each page is validated before its continuation is
requested. Pages can change between REST requests, so even a
terminal collection is not a block-pinned snapshot.

Python `get_deepx_wallet_perp_positions(wallet, max_subaccounts, max_pages_per_subaccount,
market_id=None, only_closed=None, page_size=None, config=None)` bounds both the wallet directory
and each subaccount's lifecycle pagination. It requires the directory and user stats to agree,
checks every subaccount profile against the wallet, rejects repeated position IDs across the
collection, and rechecks directory membership before returning all pages. A changed result still
cannot be ruled out between mutable REST requests; this is not finalized exposure evidence.

A read-only wallet-wide query on 2026-10-11 for
`0x781ed35b167068c93dfadab41dfb680edaca4e50` and market 3 validated all four current
subaccounts. Their position-lifecycle record counts were 2, 11, 1, and 0, each within one terminal
page; all 14 returned records reported `Closed`. This is current REST history, not a finalized
position snapshot. No credentials or transactions were used.

`get_hourly_unsettled_funding(request)` preserves signed position, funding-index, mark-price, and
payment values as exact raw on-chain integers. It validates account and market scope, timestamps,
nonzero boundary values, checked funding-index arithmetic, unique event IDs, strict four-field
keyset order, and the requested page size. `get_hourly_unsettled_funding_pages(request, max_pages)`
advances with the final row's timestamp, market, subaccount, and event ID. It rejects duplicate
events, repeated cursors, and page-budget exhaustion without returning a partial collection. A
full terminal page requires one additional empty request because the response has no `hasNext`
field. The reader does not infer asset precision, settlement, current account state, or framework
PnL and funding events.
Python exposes the same bounded wallet or subaccount reader as
`get_deepx_hourly_unsettled_funding_pages`. It retains each page and returns signed position,
funding-index, mark-price, and payment values as raw integer strings without assigning precision.
The validated perpetual wallet-order reader is available as `get_deepx_perp_wallet_orders` and
`get_deepx_perp_wallet_order_pages`. It retains market/subaccount groups, exposes exact decimal
strings, and follows the response's shared global cursor within an explicit page budget. These
mutable pages do not establish finalized order state or independently prove wallet ownership.
Python also exposes `get_deepx_perp_open_orders` and `get_deepx_perp_open_order_pages` for one
subaccount and market. They return validated typed records with exact decimal strings; the latter
requires a page budget and rejects invalid or repeated cursor pages without partial results.
The open-order REST response is mutable and does not establish finality or wallet-wide completeness.
Python `get_deepx_perp_history_orders` and `get_deepx_perp_history_order_pages` expose the
validated historical order reader for one subaccount, optionally across markets. They preserve
terminal statuses and signed fees as exact decimal strings; the bounded reader fails without a
partial result on invalid records or pagination.
Python `get_deepx_perp_account_trades` and `get_deepx_perp_account_trade_pages` expose validated
subaccount trade pages with optional exact order filters and timestamp bounds. They retain signed
fees and exact decimal values without producing framework fill reports. Neither mutable REST view
establishes chain finality.
Python `get_deepx_perp_funding_fees` and `get_deepx_perp_funding_fee_pages` expose validated
funding-fee pages for one subaccount with optional market and time bounds. They retain signed fees,
rates, and settlement flags as venue observations without inferring payment currency, cadence, or
framework account state. The bounded collector rejects duplicate chain event identities and
pagination failures without returning partial results.
A read-only testnet query on 2026-10-11 resolved wallet
`0x781ed35b167068c93dfadab41dfb680edaca4e50` to four subaccounts. Its
`0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc` subaccount returned four market-3 historical
orders, five account trades, and four funding-fee records, each in one terminal page. These
current REST records are not a block-pinned reconciliation snapshot.
`get_deepx_spot_open_orders` and `get_deepx_spot_open_order_pages` expose the same read-only
single-subaccount pattern for one Spot market selected by name or bytes32 pair. They retain exact
decimal strings, reject terminal and inconsistent active orders, and require an explicit page
budget for collection. Their mutable pages do not prove finalized state or a complete wallet view.
`get_deepx_spot_history_orders` and `get_deepx_spot_history_order_pages` expose validated Spot
history for one subaccount, optionally across markets. The bounded reader retains page boundaries
and rejects duplicate order IDs, invalid records, cursor failures, or page-budget exhaustion
without returning a partial result. These mutable pages are not finalized order reports.
A read-only testnet query on 2026-10-11 resolved the contributor wallet
`0x781ed35b167068c93dfadab41dfb680edaca4e50` to four subaccounts; each cross-market Spot
history request returned an empty terminal page. This current REST observation does not establish
historical completeness or chain finality.
Python also exposes `get_deepx_perp_order_by_tx` and `get_deepx_spot_order_by_tx` for exact-hash
lookups. Both reject a response bound to another hash and preserve validated order values; the
mutable REST association alone is not canonical inclusion or finality evidence.
`get_deepx_rest_transaction_status` performs one hash-bound backend GET and retains the original
JSON payload. Its `pending`, `best`, and `decode_failed` results do not advance the durable order
lifecycle. A testnet query for a historical hash from wallet
`0x781ed35b167068c93dfadab41dfb680edaca4e50` returned backend `10020`; this is not evidence
that the transaction was unsent or absent from the chain.
`DeepXRestStatusPoller` adds explicit attempt, interval, and deadline limits for an already
submitted hash. Another Python thread can cancel its in-flight read; cancellation is sticky for
the poller instance. The result retains the last valid observation and latest error, but never
submits bytes, marks an order final, or authorizes retrying a transaction.

Captured spec-369 fixtures prove two exact five-row continuations for market 3. A read-only testnet
verification on 2026-09-17 returned all ten boundaries in the captured inclusive time range, then
accepted an empty terminal page. Run `cargo run -p nautilus-deepx --bin
deepx-verify-rest-hourly-unsettled-funding -- <wallet> <market-id>` to repeat the bounded read. No
credentials or transactions are sent.

The 2026-10-11 OpenAPI now documents `account/perp/position-orders` with a fixed ascending
order-ID/side cursor and explicit incomplete-coverage errors. A read-only probe for both known
active and closed position IDs still returned service code 503: historical reconstruction is
required. The adapter therefore exposes no typed success model or reconciliation behavior for
that endpoint.

`get_quota_summary(wallet)` reads the public wallet-level quota aggregate. It preserves Spot,
perpetual, and total USD volume strings as exact decimals, binds the returned owner to the request,
and validates nonnegative additive volume totals plus paired RFC 3339 and millisecond trade
timestamps. Quota counters remain venue observations and are not converted into execution limits,
claim eligibility, or account state. The account snapshot verifier independently requires the
reported subaccount count to match the wallet directory.
Run `cargo run -p nautilus-deepx --bin deepx-verify-rest-quota-summary -- <wallet>` to inspect the
validated aggregate without credentials or transactions.
Python `get_deepx_quota_summary(wallet, config=None)` uses the same validated reader and returns a
read-only `DeepXQuotaSummary`. USD volumes are exact decimal strings. This one-shot call blocks the
calling Python thread; async callers can use `asyncio.to_thread`.
Python `get_deepx_quota_history(...)` returns one validated `DeepXQuotaHistoryPage`. It accepts
wallet, buyer address, history type, cursor, limit, and data-client config arguments. Its records
retain the closed quota and buyer classifications, owner, amount, chain event identity, and
transaction hash.
Follow `next_cursor` only while `has_next` is true; an empty page is valid. This read also blocks
the calling Python thread and grants no claim authority.
`get_deepx_quota_history_pages(wallet, max_pages, ...)` follows the same validated cursor chain
within a caller-supplied page budget. It returns no partial result if a page fails validation or
the budget is exhausted. These mutable REST pages are not a block-pinned snapshot.

`get_quota_claim_raw(claim_id)` reads the backend task state for one claim ID, verifies the
returned integer ID, and leaves the remaining response payload uninterpreted. This is not
chain-finality evidence. The mutating claim
creation endpoint remains disabled. The current OpenAPI now specifies the three-line
`DeepX quota claim` personal-sign message and a 160-byte maximum idempotency key. Its
2026-10-11 revision permits either the wallet or an active enabled delegate to sign while the
claim remains addressed to the wallet.
`sign_quota_claim(key, wallet, idempotency_key)` builds that exact message and a serializable
signature payload offline. It normalizes the wallet to lowercase and never posts a claim. A
separate-signing-key vector matches an independent EIP-191 implementation but does not prove
that the key is a live authorized delegate. A matching signature does not establish wallet or
delegate authorization, retry persistence, backend acceptance, or a finalized quota grant.
`sign_buy_quota(permit, key, params, nonce)` encodes `Lending.buy_quota` with an exact AccountId20,
u32 quota count, and optional source subaccount through the pinned Subxt fork. Complete extrinsic
bytes match the pinned Python SDK with and without a source subaccount on spec366, spec369, and
spec371. This offline signer does not establish quota pricing, payer authorization, or purchase
finality; operational quota purchase remains disabled.

A live read-only verification on 2026-09-17 returned four subaccounts, zero Spot volume,
perpetual and total volume `2970.527580000000000000`, earned quota `2970`, granted and reserved
quota zero, and pending quota `2970` for the contributor-provided wallet. No credentials or
transactions were sent. The quota history response was empty for that wallet; typed history records
and bounded cursor handling are available, but the empty response does not exercise record decoding.
On 2026-10-10, the Python one-page reader again returned zero records, `has_next=false`, and no
cursor for the same wallet. The captured public-wallet purchase fixture exercises typed decoding.
Quota claim creation and quota purchase remain disabled.

The wallet directory result retains the wallet address used for its query rather than returning an
unscoped address list. `get_account_ownership_proof` derives AccountId20 from the local secp256k1 key
and requires that it match the directory wallet and profile authority, that the configured exact
AccountId20 appears exactly once in the directory and matches the profile address, and that the
profile is active. A second directory read rejects membership changes during verification. It
returns an opaque `DeepXAccountOwnershipProof`. The execution startup gate
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

`DeepXLendingService` exposes the same four validated public reads to Python with a reusable
read-only HTTP client. It returns exact decimal strings for curve parameters, APRs, and pool
values. History methods require an explicit start time and retain the Rust reader's market,
range, ordering, and value checks.

These endpoints provide raw market observations only. The adapter does not infer asset precision or
quantity units, directory or bucket completeness, data freshness, `rho` semantics, APR compounding,
collateral behavior, framework instruments, yields, market-status events, balances, or lending
transactions. The responses are mutable and not pinned to a block hash.
The public `lending_interest_rate` WebSocket channel returned an acknowledged batch containing
ETH, SOL, and USDC for market 1 on 2026-10-10. A bounded observer checks acknowledgement order,
asset identities against the validated REST directory, unique items, matching minute-aligned
statistic times, and nonnegative exact APRs while retaining the original JSON. The first batch is
a mutable observation and does not establish continuity, completeness, compounding, or framework
yields.
The optional strict REST comparison found a same-bucket ETH supply-APR disagreement on
2026-10-10: WebSocket `0.000001212754759968964` and REST
`0.0000012127547599689641` at statistic time `1791630660000`, while borrow APR agreed.
The adapter preserves those distinct values and does not round them into a fabricated match.
A later read-only comparison on 2026-10-10 matched ETH, SOL, and USDC in market 1 at statistic
time `1791631020000`; ETH supply APR was `0.0000012127547870728891` in both responses. This
single successful bucket does not resolve the earlier disagreement or prove feed consistency.

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
for that fixture. The same ordinary sell-side Spot cancellation matches complete signed vectors
from the pinned Python SDK under spec366, spec369, and spec371. Other argument combinations remain
unproven.

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
The default collector remains unchanged. Durable execution recovery explicitly selects ordinary
Spot cancel and Spot place observers. Mock scans use metadata-encoded events, not captured golden
vectors or local spec369 approval.

Tracked local perpetual Limit and Spot Limit GTC cancellation are wired through durable reservation,
signing, `OrderPendingCancel` receipt, one-shot submission, and finalized cancellation-event
recovery. Fast Perp finalized recovery and framework event staging accept the event-suppressed
inclusion contract; framework cancellation commands submit ordinary cancels by default, with
`fast_perp_cancel` enabling the Perp fast path. Fast Spot cancellation remains unavailable because
the approved event behavior is not established. Perpetual Limit modification uses one atomic
cancel-and-place extrinsic, durable `OrderPendingUpdate` staging, and finalized evidence for both
ordered transaction legs before publishing an update outcome. The submission gate requires
approved runtime metadata with the exact `Subaccount.PerpOrderModified` sizing-event fields and
numeric SCALE types. The captured spec371 runtime does not declare this event, so live modification
remains disabled. Spot modification, testnet replacement behavior, cancel-all, close-position,
and other unsupported operational order commands remain disabled. Framework batch cancellation
fans out validated items through the durable single-order path with the parent command IDs; it is
non-atomic and does not discover orders absent from the local cache. Venue-native batch
cancellation remains disabled. Testnet Spot owner authorization and dispatch behavior remain
unverified.
Maintainer approval remains unresolved; this local milestone is not a PR.

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
  conversion requires exact runtime constraints from a finalized checkpoint.
- A Nautilus `InstrumentProvider` over the failure-atomic catalog. `load_all` converts every
  perpetual and chain-verified Spot market into a standard `InstrumentStore` under a single captured
  conversion timestamp. Spot loading requires validated Watch RPC endpoints and a shared finalized
  checkpoint. Loads are failure-atomic: one
  unconvertible market fails the whole load, `load_ids`/`load` refresh while preserving cached
  instruments and report absent identities, and non-empty filters are rejected.
- Typed perpetual funding-rate, long-short ratio, open-interest, raw trade, raw candle,
  raw mark-price, and raw oracle-price history reads, plus raw perpetual volume statistics and the
  raw current last price, with synchronous parameter validation and exact financial-value parsing.
  Funding-rate, long-short-ratio, and trade responses are rejected when their deployment market ID
  differs from the request. These reads preserve venue response order where applicable and do not
  emit Nautilus data events.
- A failure-atomic public market catalog which loads Spot and perpetual metadata concurrently,
  preserves deployment-provided bytes32 pair and numeric market IDs, and indexes entries by
  canonical product-aware Nautilus identities. It also resolves Spot pair and numeric perpetual
  market IDs back to canonical instrument identities, rejecting duplicate deployment IDs without
  changing any index. The `InstrumentProvider` layer builds on it and verifies Spot specs before
  publishing its catalog and instrument store.
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
- A read-only finalized checkpoint collector which validates testnet genesis and canonical
  finality, then decodes block-pinned values with the built-in spec-376 metadata. Node runtime
  compatibility is assumed; the adapter does not request or compare online runtime version,
  metadata, or `Core_version` values.
- A transport-neutral runtime snapshot service which grants immutable snapshots through counted
  signing permits backed by the built-in runtime identity. The public offline signer requires one
  of these permits rather than accepting a bare snapshot.
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
- A fail-closed Nautilus `ExecutionClient` foundation around `ExecutionClientCore` and
  `ExecutionEventEmitter`. Framework identity, account lookup, account-state emission, and
  idempotent lifecycle methods are wired. The client provides tracked single-order reports,
  bounded current perpetual-position reports, and bounded Spot and Perpetual fill reports.
  Account queries replay only the latest venue-reported cached state at or after the registered
  current-startup event. Other unsupported order, order-query, and report methods return explicit
  errors rather than the trait's successful no-op or composed defaults. Mass status composes bounded
  granular reports and marks them incomplete. Commission inference has no generic fallback, and
  position reports do not claim complete bulk coverage. `connect` autonomously runs the ordered
  startup gate for the strict finalized flat-account subset. It requires a PostgreSQL durable
  transaction configuration, an
  active signer-owned subaccount selected from the wallet directory, and a current acknowledged
  account WebSocket subscription. It performs instrument preload, context restoration, runtime
  validation, signer/subaccount ownership validation, account-stream confirmation, finalized
  account-state initialization, startup mass reconciliation, and account registration. Any
  non-flat account, partial failure, or cancellation fails closed and clears the startup epoch.
  Account-state initialization records the exact event identity for the current startup epoch. The
  final step verifies that event is present in
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
  operator action. For supported operations, an exact extrinsic found by the bounded scanner is
  decoded from complete standard or threaded system event storage; successful dispatch still
  requires the exact expected business event. Startup advances only when every acknowledged
  record is finalized with no remaining recovery, submission decision, reconciliation, or operator
  action. An empty complete record set is valid; endpoint or capability mismatch, signer mismatch,
  stale lease, acknowledgement mismatch, and any unresolved record fail without advancing. This
  includes restored `submitting` and `accepted` records. Perpetual placements first verify their
  exact retained call and scan every finalized block after their durable pre-submission checkpoint;
  exact event evidence advances through in-block and finalized commits. Exact presence in the
  Submission endpoint's valid pending pool durably records acceptance, while absence preserves the
  unresolved record because one non-atomic pool snapshot cannot prove non-inclusion. This proves
  only durable transaction recovery readiness for that signer, not protocol-level account or
  order reconciliation. Once all records are complete, the coordinator stages eligible framework
  outbox entries and requires exact canonical application plus durable cache persistence before
  committing each acknowledgement. Any dispatch, receipt, persistence, ordering, or CAS failure
  prevents mass reconciliation from advancing. Cache borrow contention fails with a typed error.
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
  different ownership fail closed. Registered external orders can use status-report conversion
  only when the matching full order exists in the shared cache and its client ID, venue ID,
  instrument, and strategy match the registration. Unknown or cache-mismatched orders remain
  unsupported; these routing bindings do not authorize typed event emission or automatic venue
  discovery.
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
and factories. The data client supports REST connection and Spot/Perpetual instrument discovery;
Spot definitions require finalized Watch RPC evidence. Spot book-delta, depth-10, and quote
subscriptions use the node's read-only orderbook JSON-RPC WebSocket method. The execution factory
remains disconnected.
A Spot-inclusive instrument provider and fixture-gated offline direct-pallet signing APIs exist,
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
commit `accepted` with revision-checked compare-and-set. These foundations do not authorize
automatic retry; the tracked Perp Limit command uses one-shot submission and finalized recovery.

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
  catalog, finalized chain `SpotMarketSpec` retrieval, strict Spot `CurrencyPair` and perpetual
  `CryptoPerpetual` conversion,
  typed public lending asset, rate-curve, APR-history, and pool-status readers,
  typed single-page perpetual funding-rate, long-short ratio, and open-interest history reads,
  a typed raw perpetual trades read with bounded range pagination, typed single-page raw perpetual
  candle, mark-price history, and oracle-price history reads with closed interval selection, a typed raw perpetual
  volume-statistics read, and a typed raw perpetual last-price read exist. A Nautilus
  `InstrumentProvider` populates a standard `InstrumentStore` from REST metadata and a shared
  finalized Spot spec checkpoint with failure-atomic loads and preserved cached instruments on
  refresh. A read-only REST data client publishes Spot and perpetual instruments and handles
  fresh instrument requests and bounded historical Spot/Perpetual bars and trades,
  perpetual funding, and current one-shot Spot REST and Perpetual WebSocket L2 book snapshots. Spot
  candle volumes that exceed instrument precision fail closed; this currently occurs in captured
  and live responses. Public trade, quote, and configured L2 book subscriptions have bounded
  recovery; bar requests remain REST-only. Spot L2 delta, depth-10, and quote subscriptions consume
  complete snapshots from the node's read-only orderbook JSON-RPC WebSocket method. Their event
  timestamps use local receive time and their sequence is adapter-assigned because the node payload
  contains neither field. Disconnects clear the book before recovery waits for a new full snapshot.
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
  Read-only collectors use the built-in spec-376 snapshot after validating testnet genesis and a
  finalized canonical checkpoint. They do not fetch or compare node runtime metadata, version, or
  `Core_version`. The execution client's retained snapshot service can perform a one-shot finalized
  chain-time observation, strictly decoding that block's `Timestamp.Now` storage as a non-zero
  SCALE `u64` and requiring repeated timestamp evidence to agree. It performs no
  nonce allocation, signing, persistence mutation, or submission. A separate client-owned
  perpetual placement reservation boundary combines that evidence with the retained signer lease,
  allocator, snapshot permit, and PostgreSQL store. It rejects account/subaccount,
  instrument/market, order-side/direction, and runtime mismatches, holds the permit through the
  durable create, and releases only an exactly acknowledged `created` record. It performs no
  signing or submission. A second client-owned offline boundary accepts that acknowledged record,
  revalidates its full account, market, direction, and runtime scope, reconstructs the call and
  timestamp nonce from the durable identity, and releases signed bytes only after the exact
  `signed` record commits by CAS. It creates no transmission permit and performs no submission.
  A third client-owned boundary revalidates that signed record and its canonical perpetual call,
  commits the `submitting` transition, and only then releases a single-use payload permit. It does
  not consume the permit, perform network I/O, or grant retry or replay authority. The permit can
  be separated from its exact durable submitting context, preserving reconciliation authority after
  consumption. A client-owned acceptance boundary commits `accepted` only when opaque hash-verified
  submission evidence matches that context; it also performs no network I/O. A transport-neutral
  client coordinator consumes the permit exactly once while retaining both signer-lease and runtime
  permits. It selects no endpoint, performs no retry, and returns the durable context for accepted,
  not-sent, rejected, ambiguous, and hash-mismatch outcomes.
  The conservative DirectPallet JSON-RPC transport returns `not-sent` only when local preflight
  rejects empty bytes or a Blake2-256 mismatch before a request. Once `author_submitExtrinsic`
  starts, transport errors, JSON-RPC errors, malformed responses, and returned-hash mismatches are
  all ambiguous because the shared RPC client cannot prove rejection or non-delivery. It performs
  exactly one attempt. Runtime validation retains the exact endpoint and capability evidence; an
  opt-in client-owned method can submit only after full startup through that retained Submission
  endpoint while holding the signer lease and runtime permit. Reset, stop, and disconnect erase the
  evidence. The lower-level method remains disconnected from framework order commands; supported
  Perp Limit and Spot Limit GTC/IOC commands use the retained Submission endpoint through their own
  one-shot paths.
  A separate pure order-mapping boundary accepts only simple perpetual Limit GTC and Limit IOC
  framework orders. It verifies command/event identity, configured subaccount, immutable market
  identity, minimum quantity/notional, step and tick; converts quantity at the market
  `baseDecimal` scale and price at the chain-defined `1e6` scale; and maps direction, reduce-only,
  and supported post-only behavior exactly. Market, Stop, FOK, IOC+post-only, quote quantity,
  advanced fields, inexact values, and out-of-range market IDs fail before nonce allocation,
  persistence, signing, or network access. An explicit client-owned preparation workflow composes
  this mapping with complete connected startup, the exact retained RPC evidence, immutable order
  context, finalized chain time, durable timestamp reservation, offline signing, and the
  CAS-committed `submitting` transition. Mapping and startup failures occur before context mutation
  or nonce allocation. This preparation function performs no transmission or framework event
  emission; framework `submit_order` uses a separate asynchronous coordinator for those steps.
  Spot framework mapping accepts only simple base-quantity Limit GTC/IOC orders without post-only or
  reduce-only flags. It requires a cached Spot CurrencyPair whose constraints came from the
  finalized runtime `SpotMarketSpec`, exactly converts base and quote amounts at their currency
  precisions, and requires the runtime's six-decimal quote scale. Spot placement uses the durable
  outbox and one-shot submission boundaries, then requires the matching indexed
  `SpotMarket.StateOrderBuy` or `SpotMarket.StateOrderSell` event during finalized reconciliation.
  Testnet order validation and fill behavior remain unverified.
  A separate pure REST-evidence verifier binds a typed perpetual order record to one durable
  placement. It requires exact agreement across the timestamp nonce/order ID, subaccount,
  immutable instrument/market indexes, direction, runtime-scaled size and price, supported Limit
  or oracle-guarded Market call, optional points, flags, durable signed/lifecycle hash, REST
  `EXTRINSIC_HASH`, and nonzero
  reported height. The REST schema omits time in force, so GTC versus IOC remains proven only by
  the retained durable call. This is mutable backend/indexer evidence: the reported height is not
  promoted to canonical inclusion, and the verifier proves no dispatch result, business event, or
  finality. It cannot emit a framework order event or enable submission.
  An independent Watch observer waits for the reported height to become finalized, obtains that
  height's canonical block, and locates the exact durable extrinsic by recomputed Blake2-256 hash.
  It returns `Pending` before finality and fails if the finalized block omits or duplicates the
  target. A pure combiner accepts the REST order binding and finalized location only when their
  transaction hashes and block heights agree. This establishes finalized transaction membership
  corroborated by mutable REST state, not runtime dispatch or an authoritative business event. The
  result is intentionally not convertible to `DeepXInclusionEvidence`, cannot mutate the durable
  lifecycle, and cannot emit `OrderAccepted`.
  No live framework command coordinator, complete order-call model, or complete golden signing
  vectors exist. A one-shot submission primitive, durable transaction tracker, and read-only
  finalized recovery event decoder exist behind fail-closed preparation and signer-lease
  boundaries, but no execution command path can invoke them. A read-only RPC
  identity collector concurrently queries every configured role endpoint
  for its genesis hash and releases only a complete validated endpoint set. An independent
  read-only capability probe can then require a caller-supplied non-empty method set for one role
  and fails closed unless `rpc_methods` advertises every name. A complete collector concurrently
  applies the adapter's minimum per-role method policy and binds the resulting evidence to each
  endpoint URL; execution startup requires that complete evidence. This does not prove method
  semantics or authorize submission, watching, or recovery. A 2026-09-01 attempt to capture a
  replacement finalized-header fixture failed before any RPC response because the testnet endpoint
  closed the TLS connection.
- **Phase E - Partial:** A strict execution config and `ExecutionClient` foundation exists. The
  client exposes framework identity, account lookup, account-state emission, idempotent lifecycle
  methods, strict tracked single-order and bounded multi-order status reports, bounded current
  perpetual-position reports, bounded Spot and Perpetual fill reports, current-epoch cached
  account-state queries, an explicitly incomplete bounded mass status, and explicit unsupported
  errors for mutation commands. It reports incomplete bulk-position and account-wide mass-status
  coverage.
  Its autonomous network startup is restricted to finalized flat accounts. The client
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
  retains both perpetual market identity directions for report lookup. A production coordinator
  performs that complete Spot and perpetual load from the execution client's own configured REST
  failover set and advances only after the same checks pass. Its restoration
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
  The runtime startup step has a read-only production coordinator which validates chain identity
  and advertised methods for all three configured RPC roles, then bootstraps and atomically refreshes
  a fixture-approved finalized snapshot from the validated Watch endpoint. It retains evidence only
  after the complete sequence succeeds. This step does not initialize durable transaction state,
  connect the account stream, authorize signing, or enable transaction submission.
  A separate client-owned observer uses those retained endpoints and capabilities, the retained
  subaccount ownership proof, and the transaction runtime's snapshot service to call
  `PerpRuntimeRpcApi_total_collateral_and_margin_required_for` for both initial and maintenance
  directions at one exact finalized checkpoint. The node's custom
  `lending_totalCollateralAndMarginRequiredFor` RPC is not used because its current implementation
  accepts `at` but reads `best_hash`; `lending_batchPositionsFor` has the same defect. Strict SCALE
  decoding requires one exact entry for the owned
  AccountId20 and rejects trailing data. The node defines the aggregate value scale as
  `QUOTE_PRECISION = 10^6` and the quote asset as `USDC`; the owned finalized margin observer
  converts initial and maintenance requirements exactly into a Nautilus account-level
  `DeepXFinalizedMarginBalance`, which retains the checkpoint, runtime identity, and subaccount
  beside the framework `MarginBalance`.
  Aggregate collateral is not emitted as a balance, and this mapping does not provide per-asset
  borrow balances or Nautilus free/locked balances for accounts with exposure; accounts with
  active perpetual or Spot orders are rejected. The mapping can construct only the restricted
  flat-account `AccountState`. The credential-free
  `deepx-verify-finalized-margin` program exercises a separate read-only form of this boundary.
  Its exact specification 371 fixture authorizes finalized evidence reads through a non-signing
  snapshot type. The same captured identity is separately signing-approved after comparing the
  captured metadata and signed-extension/type identity and reproducing complete extrinsics with
  independent Python SDK vectors.
  A companion observer calls `LendingRuntimeRpcApi_batch_position_for` with the complete `u8`
  market-ID domain at one fixture-approved finalized checkpoint, then corroborates every returned
  asset through `pool_accounting_v2`, `effective_deposit_v2`, and `effective_borrow_v2` at the same
  block hash. It enforces the runtime's 64-assets-per-market and 32-byte-symbol bounds, canonical
  map ordering, exact account/market/asset identity, non-zero stored positions, scaled/effective
  presence consistency, and complete SCALE consumption. Each deposit also calls lending precompile
  `0x0450` method `maxWithdrawAmountFor` with an EIP-1898 canonical selector for the same block hash
  and strictly decodes one ABI `uint128`. It retains raw scaled shares, effective amounts, maximum
  withdrawal, decimals, indices, and pool accounting. Maximum withdrawal is capped by account-wide
  initial free collateral, the effective deposit, and pool liquidity. It therefore does not prove
  Nautilus free funds or that the remaining deposit is pending-order locked. Pool
  `locked_liquidity` is explicitly pool-wide and cannot supply a user's locked balance. Empty
  account evidence is valid; partial or contradictory evidence fails closed.
  `deepx-verify-finalized-lending <account-id20>` exercises this credential-free boundary, including
  for the contributor-provided read-only wallet. These values still do not establish Nautilus
  free/locked semantics or a complete `AccountState`. The verifier uses the read-only snapshot
  type and cannot authorize transaction runtime initialization, signing, reservation, or
  submission. The same exact specification 371 identity is approved separately by the signing
  snapshot gate for offline encoding only.
  On 2026-09-23 this path returned valid empty evidence for wallet
  `0x781ed35b167068c93dfadab41dfb680edaca4e50`. A separate read-only run for the known active
  subaccount returned one USDC deposit and a successful `maxWithdrawAmountFor` result pinned to the
  same finalized block as the runtime and pool calls. These mutable observations are not fixtures
  and do not change the balance-semantics limitations above.
  A finalized perpetual-position observer reads `Subaccount.MaxPerpPositions` and the requested
  AccountId20's `PerpMarket.UserPerpPositions` in one `state_queryStorageAt` call at the same
  fixture-approved checkpoint. The specification 371 metadata fixture confirms the plain `u32`
  maximum with default eight and the Identity-hashed AccountId20 map to an exact
  `Vec<PerpPosition>` layout. Decoding is bounded by that maximum and rejects malformed or trailing
  SCALE bytes, foreign embedded owners, duplicate markets, and invalid zero-valued position
  invariants. Results retain the raw runtime amount, price, leverage, funding, realized-PnL,
  take-profit, stop-loss, and settlement fields in canonical market order. They are not REST
  lifecycle records, Nautilus position reports, or established current PnL/funding semantics.
  `deepx-verify-finalized-perp-positions <account-id20>` exercises this credential-free read-only
  path. On 2026-09-23 both the contributor-provided wallet and the known active subaccount returned
  valid empty vectors with runtime maximum eight. No non-empty live position evidence is available;
  REST/chain reconciliation, freshness policy, and complete framework position semantics remain
  unproven. This observer remains confined to the read-only snapshot type and grants no signing,
  reservation, submission, or other mutation authority; the exact specification 371 identity is
  approved separately by the signing snapshot gate.
  A companion finalized active-order observer reads the AccountId20's
  `PerpMarket.ActivePerpOrdersFor` index and every referenced `PerpMarket.OrderInfo` detail at one
  checkpoint. The specification 371 metadata fixture fixes the Identity AccountId20 index to
  `BTreeMap<u16, Vec<ActiveOrder>>`, the double Identity `(AccountId20, u64)` detail key, all 16
  `PerpOrder` fields, and nested enum discriminants, including `Buy = 1` and `Sell = 0`. Decoding is
  bounded before allocation and rejects non-canonical maps, duplicate IDs, ghost orders, foreign
  owners, non-active details, index/detail disagreement, invalid size conservation, and trailing
  data. `deepx-verify-finalized-perp-orders <account-id20>` exposes raw active finalized fields;
  its `--history <market-id>` mode resolves bounded REST history IDs and reconciles the corresponding
  finalized records. On 2026-09-29 the wallet's four subaccounts returned valid empty active
  indexes; a market-3 history read returned four records that matched same-checkpoint storage for
  identity, creation height, quantities, leverage, slippage, type, status, and flags. This verifies
  non-empty history detail decoding, but no non-empty live active index was observed. The
  DirectPallet order-report path also compares REST `height` with stored `create_time` for active
  and historical Perpetual orders, along with runtime-scaled leverage, trigger prices, and market
  slippage; REST timestamps remain the report time source.
  documented REST `position-orders` endpoint still returned service code 503 for active lifecycle
  ID `1659167`, stating that the projection requires repair. Reconciliation and freshness semantics
  remain disabled, and the read-only snapshot grants no mutation authority.
  The Spot order observer reads `SpotMarket.ActiveSpotOrdersFor` and requested double-Identity
  `SpotMarket.OrderInfo` entries at one finalized checkpoint. It bounds and validates the
  `BTreeMap<H256, Vec<(u64, bool)>>` active index, permits missing `ValueQuery` storage as the empty
  map, checks active pair/side membership, and retains exact U256 amount and price bytes. DirectPallet
  order reports compare the complete per-pair REST active-ID set with this finalized index and
  reconcile requested history rows against their retained order details, including creation height.
  `deepx-verify-finalized-spot-orders <subaccount> [--history | order-id ...]` provides a read-only
  check. The contributor wallet's four subaccounts returned empty Spot history and active indexes
  during the latest observation, so non-empty live Spot order decoding remains unverified.
  `observe_finalized_account_evidence` now obtains one read-only approved runtime checkpoint and
  collects its `Timestamp.Now`, margin, lending, perpetual-position, and perpetual and Spot active
  order components against the exact block hash. The Spot account index is decoded even when no
  order IDs are requested; when it is nonempty, every active ID is read from `OrderInfo` and checked
  against the index at that same checkpoint. An empty index supports the flat-balance gate only
  when the endpoint honors the requested checkpoint. The account and wallet verifiers display raw
  order fields without unit conversion. The combined capability gate requires `state_getStorage`
  and `chain_getBlock`. The timestamp must decode as one exact non-zero SCALE `u64` Unix millisecond
  value. A repeated timestamp read and a `state_queryStorageAt` timestamp read must agree with the
  block's `Timestamp.set` inherent, and every account in a batch must have the same timestamp. Any
  component failure rejects the whole result rather than exposing a partial snapshot.
  `deepx-verify-finalized-account <subaccount-account-id20>` exercises this credential-free atomic
  raw-evidence envelope. The raw result does not assign asset currency or free/locked semantics;
  only the restricted flat-account conversion described below can construct `AccountState`.
  General account semantics, REST lifecycle reconciliation, freshness policy, and framework reports
  remain unresolved. Its execution-owned counterpart additionally requires retained endpoint
  capability, ownership, and transaction-runtime evidence; neither entry point signs or mutates
  state.
  `deepx-verify-finalized-wallet <wallet-account-id20>` resolves the wallet through the validated
  public subaccount directory and collects complete finalized account evidence for every returned
  subaccount at one approved runtime checkpoint. Duplicate addresses or any incomplete account
  result reject the entire batch. The mutable REST directory is not chain-level ownership
  evidence, so this command is a read-only discovery aid. It reports raw lending,
  perpetual-position, and active-order fields, and whether the exact flat-balance conversion gate
  accepts each account; it does not construct an `AccountState` or grant mutation authority.
  A live read-only run on 2026-09-29 resolved four directory subaccounts and returned account
  components for all four while requesting spec371 checkpoints. Each reported one lending
  position and no perpetual positions or active perpetual orders. All four passed the exact
  flat-balance conversion on the returned values and reported USDC total equal to free with zero
  locked. The later historical-storage inconsistency means this run did not prove those components
  came from their requested finalized blocks. Querying the wallet address itself as though it were
  a subaccount still correctly fails closed because it has no
  `SubaccountInfo`.
  On 2026-10-08, a read-only per-account run again resolved all four wallet subaccounts and
  returned flat USDC balances while requesting separate spec371 finalized blocks, with no positions
  or active orders. A subsequent wallet batch read returned all four at block `214757490`, hash
  `0xf0a25e180c773f827990169797611919a7d49df3436e89b06adc179a4b5e8de6`, but reported
  `Timestamp.Now` values that differed by seconds despite the identical hash. This does not
  establish coherent live shared-checkpoint account evidence. Other live attempts failed on
  intermittent RPC TLS handshake EOFs. Two direct read-only
  `state_getStorage(Timestamp.Now, fixed_hash)` calls at that same hash returned
  `1791460864166` and `1791460877536` milliseconds, confirming that this endpoint can return
  changing timestamp storage for a fixed finalized hash. The observer now rejects timestamp
  disagreement within an account read or across a wallet batch. Agreement is only a consistency
  check, not proof that other storage and runtime API responses honor the supplied hash. The
  historical `chain_getBlock` response at that hash contained a `Timestamp.set` inherent encoding
  `1791460419316` milliseconds, different from the mutable storage reads; the account observer now
  checks storage against that inherent. Two `state_queryStorageAt` calls at the same hash both
  labeled their result with that hash but returned `1791462177436` and `1791462198716`
  milliseconds for `Timestamp.Now`. The account observer also checks this method's timestamp
  result against the inherent. Identity and method-list probes retain independent observations for
  each RPC role and retry failed read-only requests at most twice. A read-only rerun for
  `0x781ed35b167068c93dfadab41dfb680edaca4e50` resolved four subaccounts, then rejected the
  first account because the block inherent was `1791462893116` milliseconds while storage at its
  requested hash returned `1791462900536`. With independent role probes and bounded read-only
  retries restored, another run again resolved four subaccounts and rejected the first: block
  inherent `1791463510605` milliseconds, requested-hash storage `1791463515715`. Live
  shared-checkpoint conformance remains unverified.
  Historical runtime selection is also unreliable on this endpoint. The captured finalized
  spec-366 hash `0x03e29c08d90b26697535dacbcfa940c8d2ae08653e4b4760ac1dd4a281ced7c6`
  returned spec `371` through both `Core_version` and `state_getRuntimeVersion`. A captured
  spec-369 finalized hash likewise returned spec `371` through `state_getRuntimeVersion`. Earlier
  account evidence and signing-bound chain-time implementations rejected those responses. The
  adapter no longer queries either runtime method and instead assumes compatibility with its
  built-in spec-376 metadata. These observations remain historical endpoint evidence; they do not
  establish historical correctness of every `state_call` response. An `eth_call` with an
  impossible canonical block hash also returned a successful `0x`, so EIP-1898 block selection is
  not yet trustworthy. Complete account evidence now requires a harmless call at the selected
  checkpoint to succeed and the same call with an impossible canonical hash to return a structured
  JSON-RPC error. The live wallet run fails at the earlier runtime guard, so this new `eth_call`
  guard has mock coverage but no end-to-end live acceptance evidence.
  An October 9, 2026 read-only wallet rerun again resolved all four subaccounts and failed the
  spec-369 checkpoint guard because `Core_version` returned `371`. Direct JSON-RPC requests
  confirmed that block `184886760` still maps to the captured hash, but `Core_version` and
  `state_getRuntimeVersion` both returned spec `371` with that hash explicitly supplied. The
  unpinned runtime-version call also returned spec `371`. Both runtime methods rejected an
  impossible hash with `UnknownBlock`, confirming that they parse the supplied hash. The cause of
  the inconsistent result for the known historical hash is unverified.
  On October 10, 2026, another read-only wallet run again resolved four subaccounts and rejected
  `Core_version` returning `371` at that spec-369 checkpoint. The separately advertised
  `chain_getRuntimeVersion` method also returned `371` for the captured spec-369 block hash, so it
  does not supply independent historical runtime evidence.
  On October 11, 2026, the read-only wallet verifier again resolved four directory subaccounts
  and stopped before account storage reads because historical `Core_version` returned spec `371`
  for the captured spec-369 checkpoint. No finalized wallet account result was accepted.
  Current signing-bound account observation does not perform these runtime probes. Timestamp,
  repeated-read, finalized-height, and canonical-hash contradictions still fail closed before
  releasing evidence.
  The local node source also shows that custom `lending_batchPositionsFor` and
  `lending_totalCollateralAndMarginRequiredFor` RPC handlers ignore their `at` argument and
  select `best_hash`. The adapter's finalized account collector instead calls the runtime APIs
  through `state_call` with the requested checkpoint hash. This avoids the known custom-RPC
  behavior, but the public endpoint's `state_call` hash semantics still need live proof.
  A live read-only run on 2026-09-23 exercised the combined boundary for owned subaccount
  `0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc` at finalized block `196050060` and hash
  `0xaac093675fa1ec964b0e969db4dbdb8d7a874b9458cfdd4ed75e4d25cf45e99d`. It returned both
  margin directions, one USDC lending deposit, no perpetual positions with runtime maximum eight,
  and no active perpetual orders. Querying contributor wallet
  `0x781ed35b167068c93dfadab41dfb680edaca4e50` failed closed at margin collection because the
  runtime omits addresses without `SubaccountInfo`; the adapter does not reinterpret a wallet's
  absent subaccount entry as zero margin. That run predates the block-inherent check and does not
  establish historical storage binding.
  `map_finalized_flat_account_balances` adds a framework conversion only for an exposure-free,
  fully withdrawable state. It requires zero initial and maintenance margin, no perpetual position,
  no active perpetual or Spot order, no lending borrow, and exact equality between every effective
  deposit and its finalized `maxWithdrawAmountFor` result. Only then can it prove
  `AccountBalance.total == free` and `locked == 0`; token decimals drive exact integer conversion
  and any precision loss rejects the result. Duplicate assets and empty evidence also fail closed.
  A live check at finalized block
  `196066920` mapped raw `970957430` USDC units for the known owned subaccount to exactly
  `970.95743000 USDC` total/free with zero locked funds. Borrowing, open exposure, reserved margin,
  or withdrawal constraints remain unsupported.
  The execution client can use this restricted conversion to finish startup through
  `complete_startup_with_finalized_flat_account_state`. It requires retained signer-bound
  ownership, durable transaction-runtime evidence, and a current acknowledged `user_balances`
  subscription. The WebSocket frame is used only as subscription-liveness evidence; its balance
  values are never combined with finalized chain values. After the asynchronous finalized
  observation, the client revalidates the subscription and exact runtime and subaccount identities,
  then publishes a reported `AccountState` with finalized `Timestamp.Now` as `ts_event`, local time
  as `ts_init`, empty margins, and the configured account ID and type. Dispatch, reconciliation,
  and exact cache registration remain failure-atomic, and every non-flat account still fails
  closed.
  The next startup step can independently derive the configured signer wallet, retrieve its REST
  subaccount directory and the configured subaccount profile, and retain ownership evidence only
  when both observations bind the same active relationship. Arbitrary query wallets cannot replace
  the signer-derived identity. The proof is not freshness, stream-authentication, or transaction
  authority evidence.
  The execution client can next own the exact acknowledged `user_balances` connection. Foreign
  acknowledgements fail without retaining a socket or advancing startup. Async disconnect performs
  a bounded close handshake, while synchronous reset revokes the proof and drops the task-free
  transport. It can read a confirmed balance frame using only that retained proof. A bounded
  receive timeout preserves the connection for retry, while a closed, malformed, or foreign stream
  revokes both the connection and proof. A separate client-owned initialization boundary accepts a
  caller-provided `AccountState` and atomically revalidates the retained connection, frame
  subscription, framework account identity, and event dispatch. It does not infer free/locked
  balances or margins from the balance payload; the caller must independently establish those
  semantics.
  Production mass reconciliation and final account registration then consume only this
  client-owned connection and the startup evidence already retained by the execution client. Mass
  reconciliation uses the retained validated RPC roles and durable PostgreSQL runtime; registration
  revalidates the same subscription before checking the current-epoch account event in cache. A
  missing or stale owned connection revokes its proof and cannot advance either gate.
  The lower-level production coordinator exposes this as two explicit phases. The first autonomously loads the
  complete catalog, restores cache context, validates runtime and ownership, acquires the PostgreSQL
  signer lease, opens the account stream, and returns its first confirmed frame without converting
  balances. The second accepts an independently authoritative `AccountState`, performs durable
  reconciliation, and waits with a bounded deadline for the exact current-epoch event ID to appear
  in cache. Failure clears the entire startup epoch, releases the runtime and signer lease, and
  closes the socket; cancellation synchronously revokes the epoch before dropping those resources.
  `ExecutionClient::connect` now composes these phases with the finalized flat-account converter.
  It uses the WebSocket frame only for subscription liveness and obtains all account values from
  one finalized checkpoint. Non-flat accounts still fail closed.
  During live-node startup, acknowledged recovery order events are processed immediately through
  the canonical execution-engine and cache-persistence receipt path while execution clients are
  connecting. The clients are restored to the engine on completion, timeout, or cancellation, so
  durable outbox replay cannot wait on an event buffered behind its own connect future.
  Bounded trade-ID replay state supports reserve, commit, and failure release for an already
  validated venue `TradeId`; committed IDs survive reconnect startup resets until FIFO eviction.
  Validated fill reports are merged across bounded pagination overlap only when the
  same `TradeId` carries identical economic evidence; conflicts fail closed and output ordering is
  deterministic. The fill report method reads at most 100 pages of 100 account trades for the
  configured subaccount and applies optional instrument, venue-order, and exact nanosecond time
  filters. Perpetual reports use the immutable startup market catalog. An explicitly selected Spot
  instrument uses the startup pair catalog and its instrument definition already loaded in the
  shared framework cache. An unfiltered request also reads bounded Spot account history and maps
  every returned pair through that catalog. Both paths preserve exact price and quantity
  increments, and attach known tracked, terminal, or registered external client identity only when
  its instrument and available side agree.
  Current testnet evidence across buyer and seller taker trades and maker rebates shows REST `fee`
  as a signed account-balance delta. The conversion negates it into Nautilus commission, resolves
  empty Perpetual `feeAsset` from the market quote currency, and rejects fee assets or fee signs that
  conflict with startup market metadata. Spot reports resolve an empty fee asset from the received
  asset: base for a buyer and quote for a seller. This follows the chain `OrderMatched` event's
  explicit buyer-base/seller-quote fee rule; a supplied REST fee asset must agree. The execution
  commission fallback uses immutable fee rates only for an exact linear perpetual instrument
  match. Quantity and price must be positive exact increment multiples, liquidity must be maker or
  taker, and the calculated `Decimal` must fit the selected fee currency without rounding. Maker
  rebates remain negative commission values. These observations do not prove a block-pinned history
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
  Restored signer sets reject duplicate durable cache keys and timestamp nonces before nonce
  allocator restoration or startup recovery consumes them.
  Exact pool presence durably records acceptance; pool absence remains unresolved without a write.
  Complete framework-owned Spot and Perpetual placements and finalized cancellations replay their
  durable outbox through acknowledged execution ingress and commit exact application-and-
  persistence receipts before startup advances. Outbox replay follows the monotonic timestamp
  nonce so successive finalized modifications restore replacement order IDs in reservation order,
  independent of durable-store result order. Tracked Spot and Perpetual Limit GTC cancellation
  and the currently supported placement commands use durable DirectPallet paths. Spot mutations
  still lack live testnet validation. Remaining Phase E work includes a fixture-backed private
  execution decoder and event route, complete restart and report reconciliation across mutable
  sources, and live validation of each enabled transaction command. Contingent and linked order
  lists, modifications, and other advanced transaction mutations remain unavailable.
- **Phase F - Partial:** Read-only Rust and one-shot Python quota reads, typed Rust and Python
  delegate directories, and selected offline subaccount/delegate signing primitives exist;
  management services, authorization conformance, and all quota/delegate/subaccount mutations
  remain disabled.
- **Phase G - Partial:** This document and the strict Rust data and execution configs exist. The
  execution config exposes a non-zero HTTP read timeout, defaulting to 30 seconds, an optional
  redacted proxy URL, a non-zero canonical recovery scan range size, defaulting to 100 finalized
blocks, and a
  non-zero timestamp nonce clock-drift limit, defaulting to five seconds, plus an opt-in
  `fast_perp_cancel` flag that defaults to false. It also exposes ordered
  REST read-failover endpoints and a strictly validated bounded retry policy for idempotent reads.
  A Rust execution factory validates the typed config and constructs a disconnected framework
  client with the DeepX venue, netting OMS, and margin account identity. A separate Rust data
  factory validates `DeepXDataClientConfig` and constructs a disconnected framework client with
  the DeepX identity, read-only cache view, and framework clock. PyO3 registers the canonical
  identity constants, four config classes, two factories, and their config/factory extractors in
  the global registry. The thin Python facade and generated stubs expose only that boundary, with
  public-export and credential-redaction tests. Data startup supports instrument discovery,
  bounded historical requests, and selected acknowledged market-data subscriptions. Execution
  startup performs durable recovery and wires selected Perpetual order workflows; broader account
  reconciliation, management services, and operational trading examples remain unsupported.
- **Phase H - Not started:** No controlled conformance, benchmarks, fuzz campaigns, or full
  review-readiness run has been recorded.

Within Phase A, the external maintainer-approval and competing-work checks remain unresolved.
Fixture collection currently covers deployment/runtime identity only; market metadata parsers,
the catalog and perpetual instrument conversion are covered by sanitized mock responses rather
than runtime-tagged protocol evidence. Raw Spot
`min_order_size`, `tick_size`, and `step_size` can be read either from the precompile's current
state or through `observe_finalized_spot_market_spec_evidence`, which pins the ABI response to an
approved read-only runtime and canonical finalized block. The Spot batch validates block timestamp
and EIP-1898 selection before reading any pair, then checks the timestamp again after the batch;
disagreement blocks catalog publication. The chain's `MarketSpec` confirms that tick size uses
`1e6` price scale and quantity constraints use base-token atomic units at `baseDecimal` precision.
The pure `parse_spot_instrument` converter requires finalized evidence
and an exact REST tick match. The `InstrumentProvider` now batches every listed Spot pair at one
finalized checkpoint and publishes the complete catalog atomically, but lacks a checked-in
runtime-tagged end-to-end capture. `observe_finalized_perp_order_spec_evidence` cross-checks the
Perp precompile's raw minimum size, tick, and step against metadata-typed pallet storage at one
canonical finalized checkpoint. Storage also supplies the minimum notional omitted by the
precompile. The reader requires timestamp storage to match both the block inherent and an
independent storage query, then rechecks the timestamp after reading the market value. On
2026-10-09, repeated read-only
`state_getStorage(PerpMarket.PerpMarkets[3], fixed_hash)` calls returned different bytes for the
same finalized hash. The reader now requires identical market storage bytes from two reads at
that hash and rejects observed drift, but this cannot prove all responses came from one state.
This endpoint cannot currently validate the reader end to end.
`state_getReadProof` calls for market and timestamp keys returned empty proof arrays at historical
and current finalized hashes, so that endpoint did not provide a verifiable fallback. Timestamp
and repeated market-byte agreement are only consistency checks for this reader. Market status,
price guards, and account
limits remain separate trading gates. Existing fixtures cover selected REST pages, WebSocket
messages, finalized Perpetual events, recovery boundaries, and pagination; coverage remains
incomplete for full REST histories, all reconnect and reorganization cases, and management
operations. Within Phase B,
the repository wiring and
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

| Area                  | Planned boundary                                 | Current status | Notes                                                                |
| --------------------- | ------------------------------------------------ | -------------- | -------------------------------------------------------------------- |
| Environment           | DeepX testnet                                    | Verified       | Deployment identity captured on 2026-09-01.                          |
| Protocol core         | Rust types, fixtures, HTTP/WS and runtime state  | Partial        | Runtime-tagged reads, public WS feeds, and durable direct-pallet lifecycle evidence are partial; production submission is unsupported. |
| Mainnet               | None                                             | Unsupported    | No validated deployment or protocol evidence is present.             |
| Spot                  | Nautilus data and execution clients              | Partial        | REST market data, node-backed books, and durable Limit GTC/IOC submission; testnet validation remains outstanding. |
| Perpetual futures     | Nautilus data and execution clients              | Partial        | Market data and selected durable order workflows exist; broader account, order, and testnet conformance remain incomplete. |
| Lending               | Separate Rust and PyO3 service client            | Partial        | Public Rust market observations only; no PyO3 or mutations.          |
| Subaccount management | Separate Rust and PyO3 service client            | Planned        | Requires verified ownership and authorization behavior.              |
| Delegates             | Separate Rust and PyO3 service client            | Partial        | Typed Rust and Python public directories; no authorization or mutations. |
| Quota                 | Separate Rust and PyO3 service client            | Partial        | Typed Rust and Python summary/bounded history reads; no service, claim creation, or purchases. |
| Bridge                | Separate Rust and PyO3 service client            | Planned        | Requires verified source and destination finality.                   |
| Direct pallet backend | Metadata-driven SCALE extrinsics                 | Partial        | Explicit configuration and durable Perpetual workflows; live trading remains gated on conformance evidence. |
| Legacy EVM backend    | EVM transaction wrapped by a Substrate extrinsic | Planned        | Explicit configuration; implemented independently.                   |

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

A separate finalized capture from 2026-09-23 records specification `371`, transaction version
`1`, block `195884340`, and metadata SHA-256
`4a620bad9f2482b5511f4637d03d5e7907b903f4bab5af439f664963724cda15`. Its 104872 metadata bytes
differ from the specification 369 capture only at offset 92129, where the encoded specification
version changes from `0x71` to `0x73`. The adapter validates its complete six-record manifest,
canonical finalized block binding, metadata hash, decoded extension order, and non-empty unknown
extensions. The checked-in chain source retains the same nine signed extensions and transaction
version. On 2026-09-23 the upstream Python SDK, connected to live specification 371, independently
reproduced the adapter's complete `System.remark`, `Subaccount.no_op`, and
`PerpMarket.place_order` signed extrinsics for fixed inputs and timestamp nonces. The exact captured
identity is therefore accepted by both `DeepXReadOnlyRuntimeSnapshot` and `RuntimeSnapshot`; any
metadata drift or later runtime specification remains rejected before signing.

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
The bounded history reader follows minute-aligned pages and validates pair identity, ordering,
time bounds, and nonnegative values. A read-only testnet run returned four ETH-USDC buckets across
two pages. The public `open_interest` WebSocket observer requires an exact acknowledgement, market
ID and pair, a minute-aligned statistic time, and nonnegative exact values. A 2026-10-10 market-3
run observed bucket `1791632940000` with total open interest `3332.231`, long-short ratio
`2.57875`, and long/short counts of 2063/800. Optional exact REST comparison rejected a later
same-bucket disagreement at `1791633000000`: WebSocket total open interest `3325.824` versus REST
`3326.907`, while the ratio and counts agreed. These mutable observations do not establish
continuous history, units, freshness, or Nautilus event semantics, so no framework open-interest
event is emitted.

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
fee ownership remain uninterpreted by the raw reader. The framework history path uses only the
verified instrument's size precision and the chain-defined Buyer/Seller taker roles to construct
`TradeTick` events; unknown taker values, precision loss, and values outside the finalized tick or
step grid fail the complete response. A
nonempty spec-369 fixture from
2026-09-17 covers two `ETH/USDC` records and a continuation cursor. The read-only global verifier
returned 100 records and reported more than 11 million matching rows. Direct queries filtered by
the contributor-provided wallet returned API code `10012` (`Service temporarily unavailable`),
while the adapter verifier exhausted its retry path with HTTP 504. Successful wallet-filtered Spot
history therefore remains unverified, and neither failure is treated as an empty result. No
credentials or transactions were sent.

The public `get_spot_candles`, `get_spot_last_price`, `get_spot_volume`, and `get_spot_order_book`
readers require exactly one market name or bytes32 pair and preserve all JSON financial values as
exact decimals. Spot candles use the documented closed interval set, ascending non-TradingView
responses, an inclusive `start` and exclusive `end`, and a maximum limit of 5000. Responses reject
a mismatched echoed market name, nonpositive or inconsistent OHLC, negative volume, duplicate/descending
timestamps, or excessive row counts. Spot books echo both name and bytes32 identity and validate
positive quantities/prices, nonnegative server notionals, unique strictly ordered levels, sequence,
and engine time. The optional aggregation tick is a positive exact Decimal serialized directly as
its decimal text. Live `tickSize=1` evidence showed overlapping aggregated bid/ask buckets, and
notionals may be independently rounded, so the adapter does not invent uncrossed-book or exact
`price * quantity` invariants. Pair-selected candles echo only the market name, while last-price and
volume responses echo no identity at all; those paths cannot independently bind the response to the
requested bytes32 pair. Bucket completion, freshness, volume-window inclusion, full exchange depth,
and raw-endpoint Spot quantity precision are not inferred. Candles, last-price, and volume remain
raw observations; `request_book_snapshot` converts Spot book levels into a bounded framework L2
snapshot using the venue sequence and engine time.

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
The REST tick must equal the finalized precompile tick after its `1e6` scale conversion. The chain
defines minimum order and step sizes in base-token atomic units at `baseDecimal` precision. The
pure converter and provider use those finalized values; the provider fails the complete load if
any pair cannot be matched at the shared checkpoint.

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
`get_spot_wallet_orders` and its bounded page collector validate grouped Spot order records,
maker identity, one shared cursor, requested scope, exact financial values, duplicate order
identities, and group-local time order. Python exposes the same validated observations through
`get_deepx_spot_wallet_orders` and `get_deepx_spot_wallet_order_pages`; financial values remain
exact decimal strings. The response does not echo the requested wallet and is mutable, so it does
not independently prove wallet ownership or produce framework order reports.
Python exposes the validated read-only Spot wallet trade observation through
`get_deepx_spot_wallet_trades` and `get_deepx_spot_wallet_trade_pages`. It preserves the globally
repeated cursor, market/subaccount groups, and exact decimal strings for base amount, quote amount,
price, and fee. The REST response is mutable and does not echo wallet ownership. These observations
are not framework fill reports.

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
Python `get_deepx_spot_account_trades` and `get_deepx_spot_account_trade_pages` expose these
validated typed reads with exact decimal strings and an explicit page budget for collection.
A read-only testnet query on 2026-10-11 returned an empty terminal cross-market Spot trade page
for each of the four current subaccounts of wallet
`0x781ed35b167068c93dfadab41dfb680edaca4e50`. The mutable REST result is not proof of
historical completeness or chain finality.

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
timestamps, inconsistent OHLC ranges, and negative volume. Live boundary queries showed inclusive
`start` and exclusive `end` across Spot and all three perpetual candle-shaped endpoints. Empty
pages and gaps are accepted; validation does not infer bucket-open versus bucket-close meaning,
volume units, market identity from a
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
sanitized runtime fixture or multi-page capture proves empty-bucket behavior,
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
An independent read-only `user_perp_open_orders` protocol boundary now serializes an exact
market/subaccount subscription. `observe_initial_perp_open_orders_page` owns the socket,
requires one matching acknowledgement, validates the first complete page, retains order items
as raw JSON after checking order ID, owner, market ID, and duplicate numeric IDs, then closes the
connection. A testnet run for subaccount
`0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc` and market `3` returned zero open orders.
`deepx-verify-ws-perp-open-orders <subaccount> <market-id>` repeats that observation. Incremental
order updates, nonempty item interpretation, reconnect recovery, and execution reports remain
incomplete.

`deepx-verify-ws-account-metrics <wallet>` uses the REST directory to observe initial
`user_total_deposits`, `user_net_value`, `user_equity`, and `user_portfolio_margin_ratio` frames
for each subaccount. The observer requires one exact four-channel ACK and retains each raw JSON
object after checking its address and required numeric fields. Up to four data frames arriving
before the ACK are buffered and withheld until it matches; overflow or a missing ACK fails the
observation. The portfolio frame additionally
binds the nested `exactMargin` address and exposes its decimal strings as exact `Decimal` values,
including nullable ratios. A read-only run for wallet
`0x781ed35b167068c93dfadab41dfb680edaca4e50` on 2026-10-09 returned all four summaries for
four subaccounts. The backend publishes rounded outer JSON numbers and decimal strings inside
`exactMargin`; these separate mutable frames do not prove atomic portfolio state, finalized
exposure, or transaction authority.
An October 10 read-only rerun again returned all 16 initial summaries. The four subaccounts'
`exactMargin.availableMargin` strings were `970.963998`, `1143.574222`, `11.077664`, and
`1.338918`, while the outer values were rounded to two decimal places. This confirms the need to
retain the exact strings for financial interpretation; it does not establish finalized balances.
A subsequent read-only run on the same date accepted all 16 frames with strict decimal parsing.
The four `exactMargin.availableMargin` strings were `970.964025`, `1143.574254`, `11.077664`,
and `1.338918`. A separate finalized-wallet run again resolved all four subaccounts but stopped
because `Core_version` returned spec `371` for the captured spec-369 checkpoint. The WebSocket
observations do not resolve that historical runtime contradiction.
An experimental `addressType: wallet` subscription for the same four channels was acknowledged
with the wallet address, but only deposits, net value, and equity frames arrived during a bounded
observation. Their wallet-addressed values were all zero while the four subaccounts had nonzero
values. The endpoint therefore has no verified wallet aggregate contract for these summaries;
the verifier continues to read each directory subaccount separately.

The wallet mode, `deepx-verify-ws-perp-open-orders --wallet <wallet> <market-id>`, binds the
expanded ACK to the current REST subaccount directory and requires one initial page per address.
For wallet `0x781ed35b167068c93dfadab41dfb680edaca4e50`, a read-only market 3 run on
2026-10-09 returned four empty pages. These separate mutable reads do not prove finalized
ownership or canonical order state.

The read-only Spot `user_spot_open_orders` observer binds one market name, bytes32 pair and
subaccount to its acknowledgement and first complete page. It rejects foreign identities,
pagination gaps and duplicate numeric order IDs, retaining nonempty items as raw JSON. A testnet
run on 2026-10-09 for the contributor wallet's
`0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc` subaccount and ETH/USDC returned zero open
orders. Run `deepx-verify-ws-spot-open-orders <subaccount> <market-name>` to repeat the
credential-free observation. Nonempty Spot WebSocket items and incremental order updates remain
unverified and cannot produce framework execution reports.

`deepx-verify-ws-spot-open-orders --wallet <wallet> <market-name>` queries the REST subaccount
directory and checks that a wallet-scoped subscription ACK expands to exactly those subaccounts.
It then requires one complete initial page for each. The directory and WebSocket pages are mutable,
separate reads; they do not establish block-pinned ownership or finalized order state. A changing
directory can cause the check to fail.
On 2026-10-09, wallet `0x781ed35b167068c93dfadab41dfb680edaca4e50` returned four
matching ETH/USDC subaccount pages, all empty.
The same read-only wallet query on 2026-10-11 again returned four empty ETH/USDC pages. The
typed REST Spot open-order reader additionally rejects terminal or unknown states and requires
`Open` orders to have their full base amount remaining with no average fill price, or
`PartiallyFilled` orders to have a positive remaining base amount below the original amount and a
positive average fill price. The historical Spot order reader still accepts terminal records.

Spot `subscribe_trades` uses a dedicated acknowledged Spot market connection bound to both the
canonical market name and finalized bytes32 pair. The observed `trades` data envelope contains an
`items` array with trade and order IDs, both counterparties, pair identity, price, base and quote
amounts, maker/taker fees, token value, block height, taker role, and RFC3339 trade time. Its first
page is seeded without publishing historical rows. Later pages are identity-deduplicated and
published chronologically only after every row converts exactly to the verified instrument. On
reconnect, bounded cursor-paginated Spot REST history must contain the retained boundary and every
fresh-page trade in the inclusive recovery interval before any recovered event is published.
Failed or partial recovery stops that subscription without emitting a partial batch.

A read-only `ETH/USDC` testnet subscription on 2026-09-30 confirmed the Spot acknowledgement and
initial 100-row `trades` page, including the fields above. No later page arrived during that short
observation window, so continuous Spot pushes, backend history completeness, and cursor stability
remain unproven; reconnect repair is implemented but has not been verified live.
The framework `deepx-verify-live-trades --spot` path has emitted an exact Spot `TradeTick` during
one active window. A later 30-second read-only run received no novel trade and completed
unsubscribe/disconnect fencing successfully; the verifier reports a quiet window without treating
it as a feed failure.

The public `mark_price`, `oracle_price`, and `funding_rate` channels now map to framework
`MarkPriceUpdate`, `IndexPriceUpdate`, and `FundingRateUpdate` events. Each subscription owns an
acknowledgement-gated connection and applies the same bounded reconnect, heartbeat, initial-data
deadline, cancellation, and connection-epoch publication fence as live trades. Mark and oracle
values preserve the observed decimal scale rather than being rounded to the instrument's tradable
tick. Scalar JSON numbers, including scientific notation, are rejected if decimal conversion
would lose precision. The public envelope timestamp is `ts_event`. Funding timestamp ordering is
validated, while `interval` and `next_funding_ns` remain `None` because the observed payload does
not establish a payment schedule. Semantic or precision failures stop the subscription without
partial emission;
transport failures may reconnect to obtain a fresh current observation, without replaying invented
intermediate updates. The raw `latest_price` channel has no framework mapping.

A read-only `deepx-verify-live-trades --prices` run on 2026-09-16 received exact ETH mark
`2383.328348`, index `2386.075`, and funding rate `0.000211552824917299` framework events, then
verified unsubscribe/disconnect fencing. The verifier sent no account or transaction commands.
A 2026-10-10 read-only run after exact scalar decoding received mark `2494.790959`, index
`2498.055`, and funding `0.0000125` events and verified unsubscribe/disconnect fencing.

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
synchronous retirement fencing. Spot current depth requests use a separate Watch RPC
`orderbook_subscribe` connection with the requested depth (default 10, maximum 10), and return one
correlated snapshot with the node's actual per-level order counts. Perpetual current depth requests
use a separate acknowledged public connection and return one correlated snapshot at the requested
depth (default 10, maximum 10), with venue sequence, engine time, typed zero padding, and one count
per aggregated level. Delta-first, malformed, precision-losing, and over-depth responses emit no
framework response. Historical depth requests remain unsupported. The read-only
`deepx-verify-live-trades --depth10` mode verifies this path without account or transaction access.
On 2026-09-16, it received BTC sequence `8310457` at engine time `1789549039088000000`, with best
bid `75643.8000 x 0.0212` and best ask `75645.8000 x 0.0169`, then verified unsubscribe and
disconnect fencing.
The data-client `perpetual_only` setting loads only the Perpetual REST directory and does not
request Spot metadata or finalized Spot specs. The verification binary enables this read-only
scope for its Perpetual modes; its Spot mode and the default data-client configuration still
verify Spot at startup.
On 2026-10-09, `deepx-verify-live-trades --depth-request` used this scope and received a
correlated BTC-USDC Perpetual depth snapshot at sequence `171784273`, engine timestamp
`1791538177203000000`, best bid `82598.2000`, and best ask `82598.4000`. It accessed only public
market data; no account command or transaction was sent. This read-only result does not change the
historical-runtime guard required for Spot or signing.

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
backend. The execution client currently wires tracked Perpetual Limit GTC/IOC and Spot
base-quantity Limit GTC/IOC placement, plus tracked Perpetual and Spot Limit GTC cancellation, to the
`direct_pallet` backend. Advanced order types and `legacy_evm` execution remain unavailable. Spot
transaction behavior has not been validated with live testnet transactions. Config representations
redact private keys, and the public API exposes only
`has_private_key`, never the credential value. The execution config also owns an optional redacted
HTTP proxy URL, a non-zero HTTP timeout for read-only report requests, and a non-zero
`websocket_timeout_secs` for WebSocket operations and startup account-event registration. Both
timeouts default to 30 seconds.

`DeepXHttpReadRetryConfig` applies only to idempotent reads. It bounds retry count, initial and
maximum delay, jitter, per-operation timeout, and total elapsed time. Mutating operations do not
inherit this policy. Factory construction itself does not start network I/O; the framework
execution lifecycle invokes the autonomous startup when it calls `connect`.

## Product capabilities

| Capability             | Spot                        | Perpetual                           | Evidence gate                                                                                                                   |
| ---------------------- | --------------------------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Instrument definitions | Implemented                 | Implemented                         | Spot: REST metadata is paired with exact quantity limits and tick data from one approved finalized checkpoint.                  |
| Historical candles     | Partial                     | Implemented (bounded pagination)    | Both use exact instrument precision; Spot also checks finalized tick/step grids. Bucket-close semantics remain undefined.        |
| Streaming bars         | -                           | Partial (one-minute candles)         | Superseded WebSocket buckets may be revised later; gaps and disconnects stop the subscription without backfill.                 |
| Historical trades      | Implemented                 | Implemented                         | Bounded correlated Spot/Perpetual responses; exact instrument precision and identity checks.                                    |
| Order book snapshots   | Implemented (one-shot REST) | Implemented (configured L2 view)    | Spot REST levels become a bounded L2 snapshot with exact precision, sequence, and engine time.                                  |
| Order book deltas      | Partial                     | Implemented (bounded recovery)      | Spot node RPC currently supplies complete snapshots, published as replacement batches; incremental Spot deltas are unavailable. |
| Live trades            | Implemented (bounded REST recovery) | Implemented (bounded REST recovery) | Acknowledged TradeTicks, retained boundary/identity checks and atomic reconnect repair; backend completeness unproven.          |
| Quotes and ticker      | Partial                     | Partial                             | Current Spot/Perpetual quotes use book snapshots; historical quotes and ticker mapping are unavailable.                         |
| Mark and index prices  | -                           | Implemented                         | Exact acknowledged mark/oracle updates with bounded reconnect; no intermediate replay or freshness guarantee.                   |
| Funding                | -                           | Implemented                         | Bounded history and exact live rate updates; no inferred payment interval or next payment time.                                 |
| Long-short ratio       | -                           | Partial                             | Typed single-page reads and bounded cursor history; no framework events.                                                        |
| Open interest          | -                           | Partial                             | Bounded REST history and raw acknowledged WebSocket observations; units and framework mapping remain unproven.                   |
| Volume statistics      | -                           | Partial                             | Raw fixed-period window; units and boundaries unproven.                                                                         |
| Last price             | -                           | Partial                             | Raw exact value only; no timestamp or freshness semantics.                                                                      |
| Market status          | Partial                     | Partial                             | Explicit `isPaused`/Perp `isDeleted` REST fields are polled every 30 seconds; status timestamp is local observation time.         |
| Lending market status  | Partial                     | -                                   | Raw directory, rate curves, APR and pool status only; precision, units, freshness, and framework events remain unproven.        |

A complete catalog that omits a subscribed market publishes `NotAvailableForTrading` with
`is_trading=false` until a later complete refresh restores its observed status. A failed catalog
read publishes no status change and retains the last observation; it is not venue evidence that
the market stopped trading or was deleted.
On 2026-10-11, `deepx-verify-live-trades --status` observed one market-3 `TRADING` framework
status event with a local observation timestamp, then unsubscribed and disconnected without a
later event. This one read does not prove future transition delivery or catalog freshness.

The instrument provider constructs `CryptoPerpetual` definitions from catalog metadata with
settlement currency, linear costing, and a unit multiplier asserted as documented assumptions that
remain pending deployment verification. Spot instrument loading requires validated Watch RPC
endpoint identity and capabilities. Each REST Spot pair is matched against exact `SpotMarketSpec`
evidence read at one approved finalized checkpoint before the provider publishes any instruments;
RPC or conversion failures preserve the previously published catalog and store.

Unsupported parameters and unknown enum values must return typed errors. The adapter must never
emit an order book assembled from unverified or discontinuous data.

## Order capabilities

| Capability         | Spot    | Perpetual | Evidence gate                                                                                                                                       |
| ------------------ | ------- | --------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| Market order       | Planned | Partial   | Spot Buy `Market(None)` and Sell `Market(Some(50))` match SDK signed bytes; the pallet requires explicit positive quote and base amounts, while framework MarketOrder has no quote budget. Terminal report mapping remains unverified. Perp Market IOC uses an oracle guard. |
| Limit GTC          | Partial | Partial   | Spot supports durable base-quantity Limit GTC submission and indexed finalized order-state evidence; testnet validation and fills remain outstanding. |
| Limit IOC          | Partial | Partial   | Spot maps exact base/quote amounts and signs `Limit(IOC)`; local pallet cancels unmatched quantity as `IocUnfilled`. Testnet fills remain unverified. |
| Post-only          | Planned | Partial   | Perp `MustPostOnly` mapping and durable submission; crossing behavior needs testnet evidence.                                                       |
| Reduce-only        | -       | Partial   | Perp reduce-only mapping and durable submission; position-side and over-reduction behavior need testnet evidence.                                   |
| Stop order         | Planned | Planned   | Trigger source, direction, and lifecycle behavior.                                                                                                  |
| Modify             | Planned | Partial   | Perp Limit modification uses durable atomic cancel-and-place; testnet behavior and fills remain unverified.                                         |
| Atomic replacement | Planned | Partial   | Perp replacement requires finalized cancellation and placement evidence at one extrinsic index before publishing the outcome.                       |
| Close position     | -       | Planned   | Quantity, side, reduce-only, and residual-position behavior.                                                                                        |
| Cancel             | Partial | Partial   | Tracked local Perp and Spot Limit GTC cancellation use durable pending-cancel receipts and finalized terminal evidence; fast Perp is opt-in, and testnet Spot behavior remains unverified. |
| Fast cancel        | Planned | Partial   | Optional Perp fast cancellation uses finalized event-suppressed inclusion recovery; defaults off, testnet behavior remains unverified.               |
| Cancel all         | Planned | Planned   | Scope and effects on unrelated strategies or subaccounts.                                                                                           |
| Batch operations   | Planned | Planned   | Per-item atomicity, result mapping, and partial failure.                                                                                            |
| No-op replacement  | Planned | Planned   | Same-nonce replacement and transaction-pool behavior.                                                                                               |

No order capability may emit a rejection after an outcome becomes ambiguous. Recovery must merge
relay, stream, REST, transaction-pool, block, event, and finality evidence without blindly
replaying mutating bytes.
The current testnet OpenAPI's perpetual and Spot `check-order` routes document only a failure
wrapper for unavailable legacy order-hash sources; they provide no successful response contract.
On 2026-10-10, a read-only perpetual query using an observed order transaction hash returned
service code `10011` with `fail: true` and an empty `data` array.
They cannot establish transaction absence or resolve an ambiguous submission.

## Account and reports

`observe_initial_perp_open_positions_page` reads one acknowledged `user_perp_positions`
WebSocket snapshot for an exact market and subaccount with `status: open`. A read-only testnet
observation for the contributor's market 3 subaccount on 2026-10-09 returned `items: null`,
`totalCount: 0`, and `totalPage: 0`. The observer accepts this documented empty form, preserves
nonempty items as raw JSON, and rejects foreign identities or incomplete pagination. The snapshot
requires each nonempty item to identify the requested owner and market, carry `Open` status, and
have a unique nonzero position ID. No nonempty live WebSocket item was available from this wallet;
its current REST position pages contain only `Closed` records. The snapshot is mutable and does
not establish finalized exposure or produce a framework position report. Run
`deepx-verify-ws-perp-open-positions <subaccount-account-id20> <market-id>` for one credential-free
observation.

The `--wallet <wallet> <market-id>` mode checks that the expanded `open` ACK matches the current
REST subaccount directory and reads one initial page per address. A market 3 run on 2026-10-09 for
wallet `0x781ed35b167068c93dfadab41dfb680edaca4e50` returned four empty pages. The venue
repeated an identical page before all addresses arrived; the observer admits identical repeats but
rejects conflicting content. These mutable reads do not prove finalized ownership or exposure.

The read-only `user_spot_trade_history` observer binds one verified Spot market and exact
subaccount per subscription, validates each incremental trade's pair and account scope, and keeps
its exact decimal values and raw JSON. It accepts at most 20 batches and 100 items per address.
The wallet verifier observes each current directory subaccount, then reports only if every
subscription succeeds. An `ETH/USDC` run on 2026-10-09 for wallet
`0x781ed35b167068c93dfadab41dfb680edaca4e50` acknowledged four subscriptions and received
zero trade batches in each ten-second window. No nonempty Spot WebSocket trade frame was verified;
the observer does not dispatch fills. The optional REST matcher requires an exact subaccount and
pair request, bounded timestamp range, all typed trade fields, a unique venue ID, and no duplicate
matches across batches. A read-only wallet `--reconcile` run likewise observed zero batches, so
nonempty WS-to-REST matching has local test coverage but no live acceptance evidence. REST account
trade rows do not echo the subaccount and are not block-pinned; a match is only mutable candidate
evidence.
The approved-metadata Spot event decoder retains every `SpotMarket.OrderMatched` field and its
global `System.Events` index. A pure exact matcher binds REST trade ID, pair, account, order side,
gross base/quote amounts, runtime price, and side-specific fee asset and sign to one supplied
event. Spec-371 metadata-encoded tests pass for a buyer taker and seller maker. The specified
wallet's current `trades-by-wallet` response has empty `ETH/USDC` and `SOL/USDC` subaccount
groups. No wallet-owned live Spot trade has yet exercised the chain match, and the matcher alone
does not establish canonical finality or authorize fill delivery.
The Spot finalized-block observer requires a fixture-approved historical runtime, checks the
requested height against the finalized head, rejects inconsistent spec-369 historical runtime
responses before reading complete event storage twice at one hash, and rechecks that height's
canonical hash. The Perpetual finalized-trade observer applies the same historical check before
event reads. Local RPC tests cover unchanged events, non-finality, historical runtime inconsistency,
changed storage or hash, unapproved runtime, and malformed storage. The wallet verifier's
`--finalized` mode collects at most 20 batches, 100 items, and 20 distinct event blocks before
printing any three-source match. The specified wallet's read-only `ETH/USDC` run on 2026-10-09
again observed zero batches and matched zero trades. No live nonempty Spot stream or three-source
match has been verified, and no Spot fill dispatch is enabled.
The read-only Perpetual verifier for block `187071212` and the same wallet stopped at the public
RPC's historical `Core_version` response of spec 371 for a captured spec-369 hash; it returned no
finalized trades.

| Capability              | Current status | Evidence gate                                                                                                                   |
| ----------------------- | -------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Account registration    | Partial        | Owned account stream plus finalized flat-account snapshot; non-flat margin semantics remain unavailable.                        |
| Account query           | Partial        | Replays current reported state, then refreshes flat accounts from one finalized checkpoint.                                     |
| Balances                | Partial        | Finalized flat-account balances require zero exposure and unrestricted withdrawal; general margin semantics remain unavailable. |
| Portfolio state         | Partial        | Finalized aggregate initial/maintenance requirements map to account-level USDC `MarginBalance`; collateral balances and per-product portfolio state remain unavailable. |
| Positions               | Partial        | DirectPallet reports current side, quantity and entry price from finalized storage; realized/unrealized PnL fields and historical position changes remain unavailable. |
| Active orders           | Partial        | Bounded REST pagination; DirectPallet active Perpetual and Spot records are checked against finalized storage, but mutable reads are not atomic. |
| Order status report     | Partial        | Tracked and cache-verified registered-external Spot/Perpetual lookup plus bounded active/history reconciliation; DirectPallet details are cross-checked, while history timestamps and snapshot races remain. |
| Fill reports            | Partial        | Bounded Perpetual history and Spot fills; mutable snapshot completeness and private-stream recovery remain. |
| Position reports        | Partial        | DirectPallet finalized storage or LegacyEvm bounded REST; bulk coverage and position-change time remain unavailable.               |
| Mass status             | Partial        | Bounded order, fill, and position reports with preloaded instruments; independently read sources are not an atomic snapshot.     |
| External order tracking | Partial        | Registered identities enrich fill reports; status reports also require an exact cached order match. Automatic discovery and private-stream updates remain unavailable. |
| Startup reconciliation  | Planned        | Restart fixtures and deterministic REST/stream/chain merge.                                                                     |
| Quota summary           | Partial        | Exact wallet REST aggregate only; completeness, freshness, and claim eligibility remain.                                        |
| Liquidation history     | Partial        | Exact raw-unit bounded REST history only; no risk events or snapshot completeness.                                              |

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

`sign_offline_evm_transaction` signs caller-supplied EIP-1559 and legacy envelopes entirely offline
with the pinned SDK's zero-fee fields. `sign_offline_evm_transact_extrinsic` requires an active
approved runtime permit and testnet chain ID `4846`, then embeds either envelope in the unsigned
`Ethereum.transact` wrapper. SDK and Rust regression vectors match complete EVM bytes, EVM
transaction hashes, signer, and complete wrapper bytes for both formats. The caller supplies the
nonce, gas limit, destination, calldata, and value. These primitives do not discover a precompile
address, encode its ABI, reserve a nonce, persist transaction state, or submit bytes. Legacy EVM
execution remains unsupported.

The inspected testnet chain source registers the Perp precompile at `0x044e` and declares
`setProfitAndLossPoint(address,uint16,uint128,uint128)`. The typed
`sign_offline_perp_pnl_evm_transact_extrinsic` entry point encodes this ABI call and produces the
offline EVM envelope and unsigned wrapper. Its EVM and outer hashes match pinned SDK vectors in
both envelope modes. The inspected Spot and Perp precompiles contain no order-placement ABI, so
this evidence does not authorize a legacy order workflow.

The legacy path is expected to encode ABI calldata, sign an EVM transaction, and wrap the decoded
transaction plus signer AccountId20 in an unsigned `Ethereum.transact` extrinsic. The Python SDK
reference uses `create_unsigned_extrinsic` for this wrapper; there is no second outer signature. It
remains unsupported for other calls until their precompile address and ABI are proved, and the
transaction lifecycle is validated end to end.

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
Version 4 durable records added the immutable pre-submission finalized checkpoint used by
perpetual placement recovery. Version 5 added immutable framework command and future event identity
for framework-owned placements. Version 6 added pending-only durable event staging with stable
timestamps. Version 7 adds exact consumer acknowledgement for each staged event. Version 8 adds
transactional perpetual modify identity and recovery. PostgreSQL restoration scans the complete
DeepX transaction key family so a version 7
or older or unknown-version record fails startup explicitly instead of being hidden by the new
namespace. There is no automatic migration or replay of legacy signed bytes.

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
added retained reorganization evidence, version 4 added the pre-submission finalized checkpoint,
version 5 added framework identity for framework-owned placements, version 6 added pending-only
framework event staging, and version 7 adds exact consumer acknowledgement. Each version uses a
distinct cache-key namespace so older record shapes cannot be silently interpreted as current.
Version 8 adds durable perpetual modify identity and canonical finalized recovery. Each record stores
the client order ID, signer,
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
seconds. It also retains the built-in spec-376 runtime snapshot service and can use it to observe
chain time without accepting a caller-supplied snapshot or time value. That operation validates
genesis and one finalized canonical Watch checkpoint, then reads `Timestamp.Now` from the exact
same block hash and requires it to agree with the block's `Timestamp.set` inherent and an
independent `state_queryStorageAt` read. It does not query online runtime version, metadata, or
`Core_version`; node runtime compatibility is assumed. Contradictory block timestamp or canonical
evidence fails closed. The operation does not allocate a new nonce, enable
sequential account nonces, sign, persist a transaction, or submit it. The signer rechecks an
existing permit immediately before encoding, and order submission tasks recheck their retained
permit before starting the transport. A revoked permit at that point is classified as locally not
sent while the durable `submitting` context remains available for reconciliation. A separate
client-owned perpetual placement reservation operation
combines a fresh observation with the retained signer lease, timestamp allocator, snapshot permit,
and PostgreSQL store. It validates the retained ownership proof against the signer and requested
subaccount, binds the instrument to the immutable market catalog, holds the matching runtime permit
through `create_committed`, and requires Nautilus Buy/Sell to match the runtime's `is_long`
direction. It returns only the exact acknowledged `created` record. This burns a timestamp nonce on
any failed or ambiguous create as required, but performs no signing or submission and is not called
by an execution command. A separate client-owned offline signing operation accepts only the
acknowledged reservation, revalidates its signer, subaccount, instrument, market, direction, and
retained runtime, and reconstructs every pallet argument and the timestamp nonce from the durable
identity. It returns signed bytes only after the exact `signed` record commits through
revision-checked CAS. No transmission permit is created, no network submission occurs, and no
execution command invokes it. A third client-owned operation revalidates the durable signed record
and canonical call, commits the exact `submitting` transition, and only then releases a single-use
transmission permit. It does not consume the permit, perform network I/O, authorize retry or replay,
or connect this path to an execution command. Splitting the preparation retains the exact durable
submitting context separately from the single-use permit. A client-owned acceptance operation can
then commit `accepted` only from hash-verified submission evidence matching that context, without
performing submission or any other network I/O. A transport-neutral client coordinator can consume
the permit through exactly one caller-classified attempt while holding the signer lease and runtime
permit. It does not select a network endpoint or retry, and preserves the durable context across all
accepted and unresolved outcomes.

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
performs network I/O; the execution client selects the verifier only for ordinary cancellations
and does not expose the fast-cancel path. This permit is intentionally unavailable to
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
- Quota claim idempotency, wallet or delegate authorization, and executor finality. The exact
  three-line EIP-191 message is documented and can be signed offline, but claim creation remains
  disabled.
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

- The Python package exposes validated configs, client factories, selected read-only services, and
  canonical identities. Operational clients are constructed by the framework factories.
- DeepX remains testnet-only and partial: the capability tables above define which market data,
  account, report, signing, and trading paths have implementation and evidence.
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
