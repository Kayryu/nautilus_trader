# DeepX adapter

The DeepX adapter is under development and restricted to testnet. Framework trading commands
require the validated direct-pallet startup gates. Account support is limited to validated startup
state and current-epoch cache replay.

`DeepXNetworkConfig::ws_connection_url` resolves the documented `/internal/v1/ws` upgrade
path from a base URL while preserving explicit endpoint paths. `DeepXWsReadConnection` owns
read-only socket halves, receives raw frames, answers protocol Ping, and performs bounded close
without credentials, background tasks or automatic reconnect. Typed application sends are
restricted to documented read-only market/account subscriptions and heartbeat commands.
`cargo run -p nautilus-deepx --offline --bin deepx-verify-ws-connection` checks the public
testnet upgrade and close using the project transport. This does not enable live subscriptions
or decode public/private business messages.

`DeepXWsPublicConnection` now supports acknowledgement-gated public perpetual subscriptions,
bounded pre-ack data buffering, exact market/channel binding, and raw data envelopes preserving
numeric lexemes. Conflicting acknowledgements and ambiguous sends/timeouts make the connection
terminal, without replay. `deepx-verify-ws-public-subscriptions` verifies public BTC trades and
orderbook envelopes. `DeepXWsTradeStream` seeds initial history without live delivery, then
deduplicates retained IDs and returns novel raw executions chronologically. Conflicting IDs,
unseen older executions and exhausted timestamp-tie capacity require explicit recovery; failed
batches leave state unchanged. Framework live trade delivery and bounded transport reconnection
are wired into the data client, with explicit REST reconciliation instead of fresh history suppression.

`DeepXWsBookStream` reconstructs the configured public perpetual book with exact decimal levels,
snapshot replacement and zero-quantity removal. Delta continuity uses the documented predecessor
ID without assuming unit increments. Malformed updates, duplicate prices, gaps and capacity excess
invalidate the book until a fresh snapshot. `subscribe_book_deltas` now publishes atomic framework
L2 snapshot/delta batches with exact precision and engine timestamps. Explicit options use the
requested depth (default 20, adapter limit 4096) and instrument price increment. Errors clear the
framework book before socket close, then retry with fresh acknowledgement/snapshot at most five
times, with cancellable exponential delays. An initial snapshot deadline prevents indefinite
uninitialized subscriptions. This configured view does not claim full exchange depth or a
server-side checksum. `deepx-verify-live-trades --book` verifies public framework book delivery
and retirement without transactions. `subscribe_book_depth10` uses a dedicated depth-10
connection and publishes a complete best-to-worst `OrderBookDepth10` after every accepted snapshot
or delta. It preserves exact levels, sequence and engine time, pads absent levels with typed zeroes,
and reports one venue-aggregated entry per populated level because the feed supplies no constituent
order count. `deepx-verify-live-trades --depth10` verifies this public path. It does not provide
historical depth. `request_book_snapshot` uses a separate acknowledged public
connection and returns one correlated L2 `OrderBook` from the first complete snapshot. It enforces
the requested depth on each side, applies an initial-data deadline, and emits no response for a
delta-first, malformed, precision-losing, over-depth, or retired request. It does not merge deltas
or retry semantic failures. `deepx-verify-live-trades --snapshot` verifies this one-shot framework
path against public testnet.

Spot `subscribe_book_deltas`, `subscribe_book_depth10`, and `subscribe_quotes` use the node's
read-only `orderbook_subscribe` JSON-RPC method over the configured Watch RPC WebSocket endpoint.
If the endpoint explicitly rejects that method with JSON-RPC `-32601`, L2 and quote subscriptions
poll validated Spot REST snapshots once per second. They publish only advancing venue update IDs
with nondecreasing engine times. REST L2 batches replace the entire requested-depth book, and
quotes use the best bid and ask. The REST fallback cannot serve depth10 because the response has
no constituent order counts. Other RPC errors continue through bounded recovery.
REST aggregate quantities can fall below the order step after partial fills; they are preserved
exactly up to base-token precision. Unrepresentable levels and crossed books are rejected.
The framework's debug formatter prints quantities above 16 decimal places as raw fixed-point
integers, including in the live data tester; this does not change their stored value.
Each node notification is a complete Spot book snapshot: L2 subscriptions publish replacement
batches, depth10 publishes the best ten levels, and quotes use the best bid and ask. Price and
amount values are converted from the node's fixed atomic scales and must match the loaded
instrument's precision and increments exactly. The node payload has no exchange timestamp or
sequence, so event and initialization timestamps use local receive time and the adapter assigns a
local monotonically increasing sequence. A disconnect clears the framework book before bounded
reconnection; publication resumes only after a new full snapshot. Spot instruments still require
finalized Watch RPC metadata evidence; book subscription itself does not submit Spot orders.
Closing the framework data receiver stops an idle Spot book task and retires its subscription.
`request_book_depth` returns one correlated current Spot snapshot from a separate Watch RPC
subscription. It requests one to ten levels (default ten), preserves actual node order counts, and
rejects an oversized response. Perpetual requests use a dedicated acknowledged public WebSocket
connection and return one complete snapshot with at most ten levels per side, venue sequence and
engine time, and one count per aggregated level. Historical depth requests are unsupported.
`deepx-verify-live-trades --depth-request` checks the read-only Perpetual path with
`perpetual_only=true`. A live framework request on 2026-10-10 returned a market-depth snapshot
without consulting the historical Spot RPC gate.
`request_quotes` returns one correlated current top-of-book quote. Spot uses one REST book snapshot;
Perpetual uses one acknowledged WebSocket book snapshot. Both preserve the snapshot engine time
and reject historical bounds or limits above one. An empty side returns an empty response instead
of a partial quote.

`DeepXDataClient::subscribe_trades` lazily opens a market-bound public connection, verifies its
acknowledgement, suppresses initial history and publishes exact chronological `TradeTick` events.
Whole batches are precision-validated before publication. Duplicate active subscriptions are
idempotent. Unsubscribe retires publication synchronously and closes its dedicated connection;
disconnect/stop/reset/dispose/drop also fence old events and cancel owned tasks. A documented
application heartbeat is sent every ten seconds. Transport/acknowledgement failures trigger at
most five fresh connections with cancellable exponential delays. Before resumed live publication,
complete bounded REST history must cover all retained boundary IDs and agree with the fresh
connection page; recovered executions publish chronologically before queued live frames. Precision,
history inconsistency, missing boundaries and budget exhaustion stop without partial recovery.
Re-admission of a failed subscription retains its boundary with a fresh publication fence; explicit
unsubscribe/disconnect discards it. REST readiness does not claim a healthy live subscription or
prove universal backend historical completeness/cursor stability.
Closing the framework data receiver also stops idle perpetual and Spot trade subscriptions.
`deepx-verify-live-trades` verifies this framework path using public testnet data only.

Externally aggregated one-minute perpetual bar subscriptions publish the last valid WebSocket
update for a bucket only after the next bucket arrives. Exact price/volume and instrument grid
checks apply before publication. Malformed updates, skipped buckets, disconnects, a closed event
receiver, or 180 seconds without a completed bucket stop the subscription; no automatic backfill
or reconnect is claimed.
The venue may revise a superseded bucket later, so these bars do not establish finality.
`deepx-verify-live-trades --bars` exercises one framework bar and synchronous retirement through
the public testnet feed without account credentials or transactions.

`subscribe_mark_prices`, `subscribe_index_prices`, and `subscribe_funding_rates` use dedicated,
acknowledgement-gated public connections for the documented `mark_price`, `oracle_price`, and
`funding_rate` channels. Mark and oracle observations preserve their exact venue decimal scale;
numeric JSON scalars must be exactly representable before framework conversion, including
scientific notation, and are not rounded to the tradable order tick. The envelope millisecond
timestamp becomes `ts_event`. Funding updates preserve the exact rate and validate the observed
calculation, funding, and envelope timestamp order, but do not infer an interval or next payment
time. These feeds share the trade subscription's bounded reconnect, initial-data deadline, heartbeat,
idempotency, and synchronous publication fencing. `latest_price` remains raw-only because no
framework mapping has been established. `deepx-verify-ws-public-prices` verifies the raw channels,
and `deepx-verify-live-trades --prices` verifies the framework events and retirement path.

`subscribe_instrument_status` polls the validated REST market directory every 30 seconds and
publishes changes in `isPaused` or perpetual `isDeleted`, timestamped at local observation time.
Invalid client, venue, instrument, or parameter scopes fail before task admission. Cancellation
or a closed event receiver interrupts a pending catalog read or idle wait. A failed read retains
the prior status; it is not evidence that a market changed state. `deepx-verify-live-trades --status`
verifies one public testnet framework status event and subscription retirement.

Perpetual close recovery verifies the generated reduce-only `OrderPlaced` event and exact
durable close terms. The generated order ID is an account sequence, not the timestamp nonce;
nonzero close prices use `Stop`, while zero prices use `Market(slippage)` with runtime pricing.
`DeepXDurableRecoveryObserver::PerpClose` verifies retained signed bytes before bounded finalized
scanning, and startup selects it for not-included close records. This proves order creation,
not a completed fill or closed position. Real inclusion captures remain outstanding.

TP/SL recovery now binds `PerpMarket.PositionUpdated` to durable raw points, including
zero-to-`None` clearing, outer and nested owner/market identity, and zero event `pnl`.
`DeepXDurableRecoveryObserver::PerpProfitAndLossPoint` verifies canonical retained signed bytes
before bounded finalized scanning; startup selects it for not-included TP/SL updates.
Missing, duplicate, conflicting or malformed evidence fails closed. Synthetic metadata-encoded
tests do not replace real TP/SL inclusion captures or enable live execution.

`verify_perp_place_inclusion_events` binds complete SCALE `System.Events` to one durable
perpetual placement at the exact extrinsic index. Successful dispatch requires a unique matching
`PerpMarket.OrderPlaced`, including both order IDs, owner, market, direction, exact size, limit
price, order type, optional points and flags, and initial order state. Market price is computed
by the runtime and is not compared to the signed placeholder. Failed dispatch needs no placement
event. The offline verifier does not establish block canonicality or fill success.
`collect_finalized_perp_place_recovery_scan` combines this verifier with the existing bounded
canonical-block scanner. Current version 7 durable placement records retain the exact finalized
block number and hash observed before nonce reservation. Startup verifies the retained signed call and
uses that checkpoint to scan restored `submitting` and `accepted` placements; an exact finalized
inclusion commits the in-block and finalized transitions separately. It also selects
`DeepXDurableRecoveryObserver::PerpPlace` for later not-included scans. Pool absence remains
unresolved and never authorizes replay. DeepX stores finalized events in `System.EventsMap` batches
when standard
`System.Events` is absent. Recovery reads the inclusive last index from
`System.Threads(block_number)`, requires every batch, and reconstructs one complete event vector.
The captured spec369 block `185969410` proves successful dispatch and the exact perpetual
placement at extrinsic index 8. Synthetic spec366/spec369 cases retain broader malformed and
field-conflict coverage.

`deepx-verify-finalized-perp-trades <block-number> [wallet-account-id20]` reads a finalized
historical block with approved runtime metadata and repeats complete event-storage reads before
returning decoded `PerpMarket.NewTrade` events. With a wallet, it also checks the wallet's current
subaccount directory and bounded, terminal REST trade pages for at most 20 relevant market and
subaccount groups, with at most ten pages per group. It rechecks each selected first page after
pagination; a changed page, changed market name, or exhausted budget fails the whole check. It
matches each relevant chain event using exact
order, market, price, size, leverage, fee, and venue trade ID fields. Captured bytes for the
contributor wallet's two market-3 partial fills matched event indices 14 and 22 at block
`187071212`. The public endpoint's historical runtime inconsistency prevents a trusted live
recheck. It also provides no storage proof, and the REST page is not a historical snapshot; this
read-only check does not emit framework fills or authorize trading.

`DeepXDirectPalletCallVerifier::new(snapshot, key, subaccount)` provides an explicit
runtime/signer/subaccount-scoped entry point for perpetual placement, close, profit/loss points,
cancel, and Spot placement/cancel. `prepare_signed_direct_pallet_transaction` selects the
existing operation-specific signer from an acknowledged Created record and commits Signed
entirely offline. `load_verified_direct_pallet_for_signer` additionally verifies canonical
retained signed bytes before returning the complete mixed-operation record set. Missing operation
identities, foreign accounts/runtimes, and sequential nonce domains fail closed; one bad record
rejects the whole load. Neither API starts execution, authorizes subaccount access, converts
financial units, nor submits or replays transactions. Both captured runtime versions are tested.

`DeepXDurableRecoveryObserver::SpotPlace` binds canonical retained signed bytes before finalized
Spot placement scanning, for either buy or sell. Execution startup now explicitly selects
operation-specific observers for Spot place/cancel and all perpetual operation families instead
of omitting canonical cancel/place prechecks on the default path. Fast Spot cancel stays
unsupported; ambiguous absence still requires operator action rather than resubmission. These
read-only recovery paths do not enable execution commands or prove real inclusion captures.

Perpetual placement supports operation-specific durable timestamp reservation with
`prepare_perp_place_reservation`, canonical binding through `DeepXPerpPlaceCallVerifier`,
and acknowledged offline signing with `prepare_signed_perp_place_transaction`. The opt-in
verifier also supports the initial-submission boundary used by the framework placement and
ordinary tracked Perp cancellation paths. Unknown durable outcomes never authorize nonce reuse or
blind transaction retransmission.

`submit_rest_transaction_once` consumes an acknowledged `DeepXPreparedSubmission`, requires a
retained active runtime permit matching its durable runtime identity, derives the `transact`
market/action labels from its durable operation, and sends the exact signed bytes once to the
primary REST endpoint. It rejects redirects and never uses read retry or failover. Acceptance
requires the backend `tx_hash` to match both the recorded and recomputed Blake2-256 hash.
Atomic perpetual modification is rejected before HTTP because the documented `transact` action
set has no `ModifyOrder` entry; its tracked execution path uses direct RPC submission.
The acknowledgement retains backend action data; `pending`, `best`, and `decode_failed`
are not verified finality or business evidence and do not advance the durable lifecycle.
Every failure after HTTP starts requires reconciliation, including runtime reverts: these
can already be included and must not be treated as unused nonces or unsent orders.
This opt-in boundary does not activate framework trading commands.

`get_rest_transaction_status` reads one already-submitted hash from the primary backend, and
`poll_rest_transaction_status` adds explicit attempt, interval, deadline, and cancellation limits.
Neither follows redirects, performs hidden retries, switches backends, or resubmits an extrinsic.
Only pending observations and retryable idempotent-read failures continue polling. Best-chain
results, decode failures, invalid hashes/schema, and nonretryable errors stop; interruptions retain
the last valid observation. Backend not-found (`10020`) never proves non-delivery or non-inclusion.
Order IDs are exact decimal `u64` values, with empty/absent IDs retained as `None`; the raw JSON
preserves uninterpreted fields and numeric lexemes. These observations cannot finalize records or
emit order success by themselves. Nine operation-variant tests exercise durable acceptance followed
by pending/best polling while leaving the durable record at Accepted. Automatic execution-client
polling remains unwired, and successful live backend status responses have not been verified.

`get_perp_open_orders_raw`, `get_perp_history_orders_raw`, `get_perp_account_trades_raw`, and
`get_perp_funding_fees_raw` expose one strictly validated, cursor-aware account page for an exact
testnet subaccount. `get_perp_positions_raw` adds the documented position-lifecycle page and always
sends `addressType=subaccount`; wallet aggregation is not exposed. Funding-fee reads support the
documented optional market and millisecond bounds without inferring payment cadence. Open-order
reads require a positive market ID: despite the OpenAPI marking both `name` and `marketId`
optional, a read-only testnet probe on 2026-09-15 returned `10001` when both were absent. The
adapter exposes only the
verified market-ID path. Other optional market, side, cursor, time, page-size, and sort parameters
use the names documented by the internal OpenAPI. A trade
`orderId` filter requires its documented market and side tuple and is parsed as an exact decimal
`u64`. Page metadata is decoded and `hasNext` without a usable `nextCursor` fails closed. Individual
records remain uninterpreted `RawValue` payloads so decimal and large-integer JSON lexemes are not
rounded and fields shown only in OpenAPI examples are not promoted to stable business semantics.
Fixture-backed `get_perp_open_orders`, `get_perp_history_orders`, `get_perp_account_trades`,
`get_perp_funding_fees`, and `get_perp_positions` decode observed fields with exact decimals while
retaining venue enum values as strings. Their bounded page collectors reject duplicate record
identities across page boundaries as well as ownership, market, filter, financial-value,
timestamp, ordering, and page-size mismatches without returning partial typed results. Raw readers
remain available when unknown fields must be retained.
Order ID zero is valid for a first node-generated order. Typed records require a canonical decimal
spelling, including `"0"`, so alternate spellings cannot create distinct runtime `u64` identities.
The raw readers do not construct framework reports, authenticate an account, or enable execution
startup and trading commands. The active-order, history-order, account-trade, funding-fee, and
position page collectors require an explicit nonzero page budget, preserve every page boundary,
and return no partial result on empty/repeated cursors or budget exhaustion.
The active-order raw and typed collectors apply the same explicit page-budget and failure-atomic
rules while preserving that the changing result is not a snapshot. They additionally bind the
requested side and enforce requested time order across page boundaries. Run
`deepx-verify-rest-account-order --open <subaccount> <market-id>` for a credential-free bounded
check. The OpenAPI was re-read for this boundary on 2026-09-17. Read-only probes of the
contributor-provided wallet resolved four testnet subaccounts; its active subaccount returned
nonempty historical order, trade, and open/closed position pages for market 3, while the later
active-order check returned a valid empty terminal page. Tests use local mock servers and no private
credentials.
On 2026-09-16, bounded funding-fee reads returned owner-bound nonempty histories for all four
subaccounts, including signed rates and both long and short position observations.
The wallet-wide funding-fee reader follows the dedicated endpoint's one global opaque cursor and
reuses the exact typed record boundary. Record owners must be valid AccountId20 values, but the
reader does not independently join them to the mutable wallet directory. A live wallet read on
2026-09-16 validated 41 records across markets 3 and 4 in one page. It does not establish history
completeness or cursor stability. The wallet-wide trade response groups records by subaccount and
can return different nested cursors while accepting only one request cursor. Without a documented
traversal rule, `get_perp_wallet_trades` therefore exposes one validated grouped trade snapshot
while preserving every nested cursor independently. It validates
market and subaccount groups, exact record identities and decimals, requested bounds, group-local
ordering, page sizes, and duplicate trade IDs. Captured history includes a zero-sized record, which
is retained as venue evidence rather than converted into a Nautilus fill report. A live read on
2026-09-17 returned two markets, five market/subaccount groups, and 22 records for the contributor
wallet; three groups advertised continuation through three distinct cursors. Run
`deepx-verify-rest-wallet-trades <wallet-account-id20> [market-id [subaccount max-pages]]` to repeat
the credential-free single-snapshot check or bounded selected-subaccount traversal.
`get_perp_wallet_trade_pages_for_subaccount` follows one selected
market/subaccount group's cursor within an explicit page budget, rejecting duplicate IDs,
market-name changes, and cross-page time reversals. The response does not echo its wallet scope or
independently prove subaccount ownership or stable snapshot completeness.
A 2026-10-09 market-3 read for the same wallet returned 12 records across three subaccount groups,
with no zero-sized records in that page and one continuation cursor. Following the cursor for
subaccount `0x977654069311fa41b88f7901dc65fae9a76fd60b` with 100 records per page reached a
terminal page after eight pages and 727 distinct, time-ordered records. This mutable REST traversal
does not prove historical snapshot completeness.

The separate wallet-wide order response repeats one consistent global cursor in every nested group.
`get_perp_wallet_orders` normalizes that metadata after validating exact order fields, enclosing
owner and market identity, optional side/market/time filters, group-local ordering, and the global
page size. `get_perp_wallet_order_pages` follows the cursor within an explicit page budget and
rejects nested metadata disagreement, repeated cursors, or duplicate composite order identities
without returning a partial collection. Legacy history includes a zero-sized filled order, which is
retained as raw venue evidence but is not converted into a framework order report. The response
still does not echo the requested wallet or prove a block-pinned snapshot, and it is not converted
into mass status or external order reports.
Run `deepx-verify-rest-wallet-orders <wallet-account-id20> [market-id]` for bounded credential-free
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
`get_spot_adl_candidates` decodes the observed candidate fields, binds target and asset identity to
the request, validates candidate addresses and unsigned raw integer strings, and preserves the
venue candidate order without assigning rank semantics. Amounts remain strings with no financial
scale. `get_spot_adl_candidates_raw` remains available for unknown fields. The response item shape
comes from a read-only testnet observation because OpenAPI defines only a generic envelope. Neither
method establishes execution eligibility or submits an ADL.
`deepx-verify-rest-spot-adl-candidates <target-subaccount> <asset> <liability>
<min-cover-raw-integer> [limit]` exercises the typed read against testnet without credentials. Its
output keeps all amounts in the endpoint's raw integer unit.
`get_liquidation_adl_records_raw` and `get_liquidation_adl_record_pages_raw` query wallet- or
subaccount-scoped ADL records with the documented filters and cursor under a caller-supplied page
budget. Item payloads remain raw because the endpoint has no stable OpenAPI item schema; pagination
metadata and request ranges are validated. These reads do not indicate that an observed record is
finalized chain state.
`get_liquidation_keeper_state_raw` returns phase-dependent keeper state as uninterpreted JSON for
one subaccount, optionally filtered by chain ID. Run
`deepx-verify-rest-liquidation-keeper-state <subaccount> [chain-id]` for a credential-free read.
The endpoint has no stable OpenAPI item schema; its response does not establish liquidation
execution or chain finality.
`get_liquidation_execution_record_pages_raw`, `get_liquidation_cranker_record_pages_raw`, and
`get_liquidation_tp_sl_record_pages_raw` collect bounded wallet- or subaccount-scoped ordinary
liquidation, cranker, and TP/SL execution history. The common request validates scope,
hashes, time and trigger-block bounds, page size, and cursor progression; record payloads remain
raw. Run
`deepx-verify-rest-liquidation-execution-records <liquidation|cranker|tp-sl> <wallet|subaccount>
<account-id20>` for a credential-free read. These mutable API records do not establish chain
inclusion or finality. The ordinary liquidation route has local mock coverage but no successful
live response capture.
The execution client uses this fixture-gated conversion boundary for a REST order already bound to
locally tracked or retained terminal `OrderContext`. Perpetual reports accept matching `Limit` and
`Market` orders; Spot reports accept matching `Limit` orders. Both verify subaccount, market, order
identity, side, quantity, limit price, post-only,
reduce-only, status/quantity consistency, exact filled-quantity precision, and timestamps, and takes
time-in-force only from immutable local context because REST omits it. System-generated `Stop`
orders, `Adaptive` post-only values, quote-denominated quantities, external or unbound orders,
missing perpetual `updatedTime`, and incomplete average-fill evidence fail closed. Spot status
reports reconstruct filled base quantity from bounded order-specific trade history; canceled orders
require the timestamp read from their exact finalized cancellation block. The single-order framework
report method resolves only a locally bound client or venue order ID, checks any supplied identity
against immutable local context, resolves markets only from the failure-atomic startup catalog
snapshot, and then performs the bounded configuration-owned REST read. The snapshot retains Spot
pair/instrument indexes and both perpetual market indexes plus perpetual quantity precision; it is
cleared on startup reset.
`query_order` applies the same tracked-context and catalog checks to the framework command,
including trader, client, strategy, instrument, client-order, and venue-order identity. It owns the
asynchronous REST lookup in a cancellation-aware task generation and emits the validated report
only while the originating connection epoch remains active. Disconnect, reset, and stop retire the
epoch before canceling its tasks, so a late response cannot enter a later session. External,
unbound, and identity-conflicting queries remain rejected.

`generate_order_status_reports` performs bounded strict reconciliation for locally tracked Spot and
perpetual orders. It reads active pages for every selected startup market and, unless `open_only`
is set, also reads history pages, with one shared 100-page budget across all selected markets,
endpoints, and filled Spot order trade lookups. Reads requiring more pages reject the complete
report.
Spot canceled-order reports additionally require finalized timestamp evidence at each
reported cancellation height. Every venue order must bind to retained tracked or terminal `OrderContext`, because REST
does not provide authoritative time-in-force. An external or unbound order rejects the complete
result rather than being omitted or assigned invented semantics. Active pages must contain only
open statuses; conflicting evidence for one venue order across active and history reads fails
atomically. Optional command bounds filter exact `ts_last` values, and successful reports are
deduplicated and ordered deterministically. These mutable reads do not prove a block-pinned or
complete account snapshot; only the active-order subset is used in the explicitly incomplete mass
status described below. A read-only testnet check on 2026-09-23 resolved four Active subaccounts
for wallet `0x781ed35b167068c93dfadab41dfb680edaca4e50`; the known active subaccount's
market-3 active-order query returned one valid terminal page with zero records.

`generate_position_status_reports` reads either one explicitly requested perpetual market or all
markets for the configured subaccount. DirectPallet uses perpetual-position storage and
`Timestamp.Now` from one fixture-approved finalized checkpoint. It converts the raw base amount and
entry price only when they exactly match the immutable instrument precision and increments. Its
`ts_last` is the checkpoint's `Timestamp.Now` observation time, not a position last-change time.
The execution read pairs only position storage with chain time at that checkpoint, so unrelated
margin, lending, and active-order calls do not gate the report. LegacyEvm retains the bounded REST
pagination path and converts only `Open` lifecycles using
`isLong`, `baseAssetAmount`,
`entryPrice`, and `updatedAt`; closed lifecycles are omitted. Unknown or unloaded markets,
inconsistent startup indexes, duplicate open positions, invalid timestamps, increment mismatch, and
precision loss reject the complete result. Optional command time bounds filter by `ts_last`. No flat
rows are synthesized, lifecycle IDs are not exposed as stable venue position IDs, and
`provides_bulk_position_coverage` remains false because neither path proves complete product
coverage.

`generate_mass_status` composes three bounded sources with one initialization timestamp: all
current tracked active orders, perpetual fills at or after the optional minute lookback, and all
current perpetual positions. Active orders and current positions are not discarded merely because
their last update predates the historical fill window. Excessive lookback arithmetic saturates at
the Unix epoch. Any source failure rejects the whole operation without returning partial state.
Every successful DeepX mass status sets `reports_complete=false`, because these independently
paginated mutable REST reads are not an atomic account snapshot and position-product completeness
is unproven. The framework can therefore apply its conservative incomplete-report reconciliation
path without mistaking the result for authoritative absence. This does not enable submissions,
cancellations, or private execution streaming.
An all-market fill request with only Spot markets in its validated catalog reads Spot trades without
making an unrelated Perpetual trade request. Both targeted and all-market Spot fill requests reject
inconsistent startup pair-to-instrument indexes before accepting even an empty trade page.
When a fill request names a venue order ID, it reads both documented order-side filters for each
selected startup market, requires terminal validated pages from every request, and rejects
conflicting market or side identities. Without an instrument selector it checks all startup markets
in deterministic order under one shared 100-page budget. Requests without an order ID retain
bounded account-wide pagination under the same shared budget across Perpetual and Spot reads.
Fill and position report requests reject unsupported parameters. All-market position reports
require consistent forward and reverse startup perpetual-market indexes before reading account data.
These REST records are mutable and do not establish finalized execution.
The pinned Python SDK provides signing construction but no account-history mapping evidence. The
chain source confirms the underlying order status, taker, fill-direction, and active-position
storage types, but not REST lifecycle IDs, fee-asset presentation, or historical snapshot rules.

`get_wallet_subaccounts`, `get_subaccount_info`, `get_subaccount_balances`, and
`get_subaccount_equity` expose the observed testnet account-directory and point-in-time account
state. They require exact 20-byte addresses and reject returned identity mismatches, duplicate
subaccounts or asset symbols, invalid asset precision, negative lending values, and invalid profile
timestamps. Financial JSON numbers and strings decode directly to `Decimal`; unknown
`spotPositions` entries remain raw JSON. The profile reader can require an independently derived
signer wallet authority. `get_account_ownership_proof` additionally binds the locally derived signer,
the wallet identity retained by the directory query, the configured AccountId20 subaccount, and an
active matching profile into an opaque proof. Execution startup requires that proof after runtime
validation and clears it on reset.
The margin-ratio reader now requires the testnet response's `exactMargin` decimal values. Legacy
`collateral`, `marginRequired`, and `marginRatio` are rounded displays; exact collateral,
maintenance requirement, and dimensionless `uniMMR` remain separate REST observations. The
reader verifies non-null `uniMMR` against the two exact amounts at the documented eight-place
truncation. The current wallet's `0x4ded...eadc` subaccount reported `970.964181` exact USDC
collateral against a `970.96` display on 2026-10-11. The endpoint does not prove initial margin,
free collateral, or chain finality.
`get_wallet_account_snapshot` composes the directory, user stats, authority-bound profile, exact
balances, equity, and margin-ratio reads for every returned subaccount. The user-stats subaccount
set must match the directory. It preserves directory order and returns no partial collection on
failure; because REST is not block-pinned, it does not claim cross-request venue snapshot atomicity.
Python `get_deepx_perp_positions` exposes one validated position-lifecycle page for an exact
subaccount, preserving venue status and cursor data while returning financial values as exact
decimal strings. It does not convert historical lifecycles into current framework positions.

`get_all_subaccounts_raw` and `get_all_subaccounts` expose the separate public global directory.
Their bounded collectors follow its opaque cursor and reject empty continuation pages, repeated
cursors, duplicate subaccount identities, invalid owner/subaccount addresses, malformed creation
times, descending-order violations, and page-size overruns without returning partial results. The
captured live response uses RFC 3339 `createdAt` strings and null statuses, despite the OpenAPI
example showing an integer timestamp. The typed model deliberately follows the observed wire form.
Run `deepx-verify-rest-subaccount-directory` for a credential-free single-page check. This mutable
directory does not prove current key control, completeness, or a block-pinned ownership snapshot.

`get_balance_changes` and `get_balance_change_pages` expose exact signed account deltas for one
wallet or subaccount. The bounded collector preserves page boundaries, rejects duplicate record
identities across pages, and returns no partial collection on cursor or validation failure. Wallet
scope can span multiple subaccounts. These mutable REST observations do not establish complete
history or free, locked, and current balance semantics and are not converted into Nautilus account
state.
On 2026-09-16, the read-only wallet verifier validated 18 `USDC` records across one page, including
funding-fee, settlement, liquidation-fee, and withdrawal changes. Position-linked records spanned
three subaccounts. No credentials or transactions were sent.

`get_liquidation_records` and `get_liquidation_record_pages` expose exact raw-unit liquidation
records for one wallet or subaccount. The bounded collector preserves page boundaries, requested
time order, and opaque cursor traversal, rejects duplicate record IDs across pages, and returns no
partial typed collection on failure. It accepts only `LiquidatePerp`, `LiquidateSpot`,
`PerpBankruptcy`, and `SpotBankruptcy`; embedded chain detail and canceled-order JSON remain
uninterpreted after structural validation. Wallet scope can span multiple target subaccounts.
On 2026-09-17, the read-only wallet verifier validated 14 records in one terminal page. The REST
history was mutable and not block-pinned, so this does not establish snapshot completeness,
canonical inclusion, or framework liquidation semantics. No credentials or transactions were sent.

`get_user_stats` validates a wallet's subaccount list, current and cumulative creation counters,
and exact nonnegative Insurance Fund staked quote amount. The amount's asset, scale, and balance
semantics are not documented and remain uninterpreted. `get_perp_liquidation_price` binds one
nullable exact price observation to the requested subaccount and market identity. It does not infer
liquidation methodology, price units, freshness, position state, or framework risk semantics.
`get_invitation_status(wallet)` returns the invitation-bound flag. Testnet currently returns a
boolean although OpenAPI shows an object; the reader accepts both forms and checks wallet identity
when the object form is returned. `get_invitation_codes(wallet)` reads associated code strings
without inferring a code format. Run
`deepx-verify-rest-invitation-codes <wallet-account-id20>` for a credential-free read. These
methods do not perform invitation mutations.
On 2026-09-16, the read-only verifier observed four current and four created subaccounts, an IF
staked quote amount of zero, and a null `ETH-USDC` liquidation price for all four subaccounts.

`get_hourly_unsettled_funding` and `get_hourly_unsettled_funding_pages` preserve signed position,
funding-index, mark-price, and payment values as exact raw on-chain integers. The bounded collector
uses the response's complete timestamp, market, subaccount, and event-ID keyset, rejects malformed
index arithmetic, scope or order violations, duplicate events, repeated cursors, and page-budget
exhaustion, and returns no partial collection. It does not infer asset precision, settlement,
current account state, or framework PnL and funding events. A read-only testnet verification on
2026-09-17 returned ten market-3 boundaries across two data pages for the contributor-provided
wallet; a final empty request proved termination. No credentials or transactions were sent.

The 2026-10-11 OpenAPI documents `account/perp/position-orders` and its fixed cursor order, but
read-only probes of both known position lifecycles still returned service code 503 requiring
historical reconstruction. The projection remains disabled; no typed success model or
reconciliation behavior is inferred from that endpoint.

`query_account` synchronously replays the latest venue-reported `AccountState` at or after the
current startup event already registered in the shared execution cache. It requires a connected
startup epoch, exact trader/client/account identity, no parameter extensions, and proof that the
current startup account event remains in the cache. It ignores locally calculated account states,
performs no REST read, does not construct balances or margins, and never replays a prior startup
epoch after disconnect or reset.

`DeepXWsAccountConnection` implements the documented credential-free `user_balances` subscription
for one exact subaccount under `market: all`. It disables compression, withholds bounded data until
the server echoes the exact channel and address in its acknowledgement, and terminates on foreign
market, channel, address, acknowledgement, or buffer evidence. The payload reuses the typed REST
balance model and validation, preserving numeric lexemes as exact `Decimal` values. A matching ACK
returns an opaque connection-owned subscription proof; only that live connection can upgrade a
balance update into a confirmed frame, and closing or poisoning the connection invalidates the
proof. A captured testnet frame and `deepx-verify-ws-account-balances <subaccount>` verify this
boundary against the contributor-provided account. The OpenAPI defines no authentication action,
challenge, token, or signature for these address-scoped user channels, so subscription
acknowledgement is not signer authentication or authorization.

`DeepXWsAccountMetricsRequest` observes the initial `user_total_deposits`, `user_net_value`,
`user_equity`, and `user_portfolio_margin_ratio` frames for one subaccount after an exact
four-channel ACK. It validates each channel's address and required JSON-number fields, and checks
the portfolio frame's nested `exactMargin` address and decimal-string fields. The portfolio metric
exposes those fields as exact `Decimal` values, including nullable ratios, while retaining the
unmodified data JSON. These values are venue observations and are not framework account balances.
`deepx-verify-ws-account-metrics <wallet>` resolves the current REST subaccount directory and
repeats that read-only observation for each address. On 2026-10-09, wallet
`0x781ed35b167068c93dfadab41dfb680edaca4e50` returned all four summaries for each of four
subaccounts. The outer JSON numbers are rounded; `exactMargin` contains decimal strings, but it
is still a mutable service observation rather than finalized account evidence. The separate
frames and mutable directory are not an atomic account snapshot and do not establish signing
authority.

`DeepXWsPerpOpenOrdersRequest` and `DeepXWsPerpOpenOrdersFrame` bind the documented
`user_perp_open_orders` channel to one market and subaccount. The read-only
`observe_initial_perp_open_orders_page` boundary owns the socket, requires one exact
acknowledgement before the first complete page, rejects foreign or duplicate numeric order IDs,
and closes it after that page. Item ownership and market ID must match the subscription. The
`deepx-verify-ws-perp-open-orders <subaccount> <market-id>` command exercises that boundary.
A testnet run for the contributor's
`0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc` subaccount and market `3` returned an empty
page. Nonempty items remain raw JSON; no incremental order decoder or execution reports are
enabled by this initial-snapshot boundary.

`deepx-verify-ws-perp-open-orders --wallet <wallet> <market-id>` uses the current REST subaccount
directory to bind a wallet-scoped WebSocket ACK and collect one initial page per subaccount. The
read-only market 3 check for wallet `0x781ed35b167068c93dfadab41dfb680edaca4e50` returned four
matching empty pages on 2026-10-09. The separate REST and WebSocket reads are mutable and do not
prove finalized ownership or order state; a directory change between reads fails verification.

`DeepXWsSpotOpenOrdersRequest` binds `user_spot_open_orders` to one validated Spot market and
subaccount. `observe_initial_spot_open_orders_page` requires its exact ACK and one complete page,
checks the market pair, subaccount, pagination and duplicate order IDs, then closes the socket.
Nonempty items remain raw JSON; their Spot item identity checks use `orderId`, `maker` and `pair`.
`deepx-verify-ws-spot-open-orders <subaccount> <market-name>` returned zero orders for the
contributor wallet's `0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc` subaccount and ETH/USDC
on 2026-10-09. Nonempty WebSocket items, incremental updates and framework reports remain
unverified.

The `--wallet <wallet> <market-name>` mode gets the current REST subaccount directory, sends one
wallet-scoped subscription with `addressType: wallet`, requires the ACK to expand to exactly that
directory, and collects one initial page per subaccount. These reads are mutable and are not a
block-pinned proof of ownership or finalized order state. A directory change between REST and
WebSocket reads causes verification to fail; the command sends no transaction.
On 2026-10-09, the read-only ETH/USDC check for wallet
`0x781ed35b167068c93dfadab41dfb680edaca4e50` matched four subaccounts and returned an empty
initial page for each.

`DeepXWsPerpOpenPositionsRequest` provides the corresponding read-only `user_perp_positions`
initial page with `status: open`. `observe_initial_perp_open_positions_page` requires a matching
acknowledgement and one complete page, accepts the observed `items: null` empty form, and retains
nonempty position items as exact raw JSON after checking owner, market, open status, and unique
position ID. The
`deepx-verify-ws-perp-open-positions <subaccount> <market-id>` command returned zero open
positions for the same subaccount and market 3 on 2026-10-09. The wallet's current REST position
pages contain only closed records, so nonempty WebSocket items lack live conformance evidence.
This mutable observation does not establish finalized exposure or produce framework position
reports.

`deepx-verify-ws-perp-open-positions --wallet <wallet> <market-id>` binds the expanded `open`
subscription ACK to the current REST subaccount directory and collects one initial page per
subaccount. A read-only market 3 run for wallet `0x781ed35b167068c93dfadab41dfb680edaca4e50`
on 2026-10-09 returned four empty pages. The venue can repeat an identical position page before
all initial pages arrive; conflicting repeated content fails verification. The REST directory and
WebSocket pages are separate mutable reads, not finalized ownership or exposure evidence.

`deepx-verify-ws-perp-order-history <wallet> <market-id>` resolves the current REST subaccount
directory and requires one exact `user_perp_order_history` acknowledgement for each subaccount.
A read-only market-3 run for wallet `0x781ed35b167068c93dfadab41dfb680edaca4e50` on
2026-10-10 acknowledged all four subscriptions. This incremental-only channel has no initial
snapshot; the verifier does not decode order updates, establish complete history, or emit
framework order events.
`deepx-verify-ws-spot-order-history <wallet> <market-name>` binds each acknowledgement to the
selected REST market name and pair ID and to one wallet-directory subaccount. A read-only
`ETH/USDC` run for the same wallet on 2026-10-10 acknowledged all four subscriptions. This
incremental-only channel likewise supplies no initial order snapshot, and no order update was
captured or converted into a framework event.

The execution client can retain that exact acknowledged connection and read confirmed balance
frames through its own subscription proof. Bounded receive timeout preserves the connection for a
retry; a closed, malformed, or foreign stream revokes both the connection and proof. This read
boundary does not infer framework free/locked balances. A separate client-owned initialization
boundary accepts a caller-provided `AccountState`, revalidates the retained connection, frame
subscription, and framework identity together, then dispatches that exact event. The caller remains
responsible for independently establishing complete account-state semantics.

The following production startup boundaries also use only the client-owned account connection.
Mass reconciliation consumes the retained validated RPC evidence and durable PostgreSQL runtime;
final account registration revalidates the same subscription before checking the current-epoch
account event in cache. A missing or stale owned connection revokes its retained proof and neither
boundary advances startup.

The lower-level production coordinator is explicitly two-phase.
`prepare_startup_account_state` performs the autonomous catalog, cache restoration, runtime,
ownership, PostgreSQL lease, and account-stream steps, then returns the first confirmed balance
frame without interpreting it as a framework account. `complete_startup_with_account_state`
accepts an independently authoritative
`AccountState`, reconciles durable transactions, and waits with a bounded deadline until the exact
current-epoch event ID is visible in the execution cache. Any failure resets all startup evidence,
drops the transaction runtime and signer lease, and closes the owned socket. Cancellation also
revokes the epoch and its outstanding runtime signing permits synchronously before dropping those
resources. A permit already past its final pre-transport check remains an in-flight attempt that
requires durable reconciliation.

The framework `ExecutionClient::connect` path now coordinates startup autonomously for the strict
flat-account subset. It prepares the startup epoch, builds the authoritative account state from one
finalized chain checkpoint through `complete_startup_with_finalized_flat_account_state`, completes
durable reconciliation and exact cache registration, and only then enables query tasks. This path
requires a configured PostgreSQL durable transaction store, an active signer-owned subaccount, and
a current acknowledged account WebSocket subscription. The configured wallet address itself is
not a substitute for a subaccount: callers must select an active entry from the wallet directory.
Any partial failure or cancellation clears the whole epoch. Repeated `connect` calls are idempotent
only while the completed epoch remains ready and connected.
After autonomous `connect`, a supervised reader owns the acknowledged account stream and retains
only its latest confirmed balance frame for `next_account_balances`. A terminal read revokes
execution readiness, the query epoch, and the shared signing service. A receive timeout preserves
the stream, while explicit close drains the reader before returning. Remote failure still requires
normal durable reconciliation for any transaction attempt already past its final permit check.

The execution client can also load the complete startup market catalog itself through its configured
REST failover set. Spot and perpetual responses are applied atomically, and the first startup gate
advances only after endpoint, non-empty catalog, and bidirectional market identity validation.

Live-node startup processes acknowledged recovery order events while execution clients are still
connecting, using the same canonical engine-application and cache-persistence receipt path as
normal ingress. Execution clients are temporarily detached from the engine during their async
connect futures and restored on completion, timeout, or cancellation. This prevents durable
startup outbox replay from waiting on an event buffered behind its own connection future.

REST and WebSocket observations remain mutable and are used only for ownership and subscription
liveness. The autonomous account-state values come exclusively from one finalized chain checkpoint
and are accepted only when both margin requirements are zero, positions and active orders are
empty, borrowing is absent, and every deposit is fully withdrawable. Any non-flat account fails
closed. This startup support does not authorize mutations or enable trading commands.

`bin/verify_sdk_signing.py` independently checks complete signed regression bytes using the
reviewed DeepX Python SDK `_native_py.py` blob `0108f89d79366f13e747ef59c75ce73a543dc404`,
`_runtime_compat.py` blob `06b2e9fc84cc36c647452027b9c5b2cbbfcafb8c`, and the captured
spec366/tx1, spec369/tx1, or spec371/tx1 runtime (`--spec-version 371`). It uses
`substrate-interface` and takes both raw Python sources or GitHub contents API JSON responses;
no RPC requests leave its offline fixture transport. It checks
System remark, Subaccount no-op, perpetual Limit GTC/IOC, Market IOC, ordinary perpetual and Spot
cancellation, fast perpetual cancellation, plus a sell-side reduce-only `MustPostOnly` placement
and subaccount renaming on all three snapshots. Subaccount Spot margin enablement and Lending quota
purchase signing, with and without a source subaccount, are also checked under all three snapshots.
Spot Buy Limit IOC and Buy Market without a slippage price are checked under
spec369 and spec371. Spec371
also checks fast Spot cancellation, Spot Limit GTC, Sell IOC, and Sell Market with a 50 bps slippage
bound, plus perpetual close, profit/loss point management, Subaccount initialization/deletion, and
delegate setup, mode update, and removal. Run the Rust signing tests to verify the implementation
against those bytes.
These limited vectors do not prove every operation, authorization, financial scaling, live IOC
behavior, or the SDK compatibility helper's
`CheckNonceEra` branch.
Pallet signing explicitly uses an immortal era, matching these SDK vectors. Mortal signing has no
independent SDK vector and is not exposed by this adapter.
On 2026-10-09, the signing tests and the pinned SDK's offline spec 366, 369, and 371 vector
checks passed. The approved fixture metadata uses `CheckMortality` and `CheckNonce`, so this run
still does not exercise the compatibility helper's `CheckNonceEra` branch.

`sign_offline_evm_transaction` signs explicit EIP-1559 and legacy EVM envelopes with the pinned
SDK's zero-fee fields. `sign_offline_evm_transact_extrinsic` binds the testnet chain ID and active
approved runtime permit, then embeds either signed envelope in an unsigned `Ethereum.transact`
extrinsic. The offline verifier checks complete EVM bytes, transaction hashes, signer, and complete
wrapper bytes against the SDK for both formats. Callers supply the nonce, gas limit, destination,
calldata, and value. Neither primitive discovers a precompile address, encodes its ABI, reserves a
nonce, persists transaction state, or submits bytes. Legacy EVM execution remains unsupported.
The testnet chain source registers the Perp precompile at `0x044e` and defines
`setProfitAndLossPoint(address,uint16,uint128,uint128)`. The typed
`sign_offline_perp_pnl_evm_transact_extrinsic` entry point encodes that call, signs it, and builds
the unsigned wrapper offline. Pinned SDK vectors match its EVM and wrapper hashes for both envelope
formats. The inspected Spot and Perp precompiles do not define order-placement calls, and no
legacy order execution is enabled.
`sign_set_spot_margin` also encodes the node's owner-signed `Subaccount.set_spot_margin` call from
an approved runtime permit. Exact SCALE and a pinned SDK spec-371 extrinsic agree. Ownership,
absence of open Spot orders, nonce reservation, durable recovery, and submission remain separate
requirements; this helper enables no margin-setting command.
`sign_set_global_leverage` and `sign_set_per_market_leverage` likewise encode the node's
owner-signed Perpetual leverage settings. Optional leverage values retain the runtime's `None`
reset semantics. Exact SCALE and pinned spec-371 SDK extrinsics agree; these helpers do not check
ownership, market limits, or active position consequences and enable no management command.

Execution startup reconciles version 7 perpetual placements from their durable pre-submission
finalized checkpoint before consulting exact pending-pool membership. Canonical event evidence can
advance a restored submitting or accepted placement through two durable commits to finality. Pool
presence records acceptance; pool absence remains unresolved and does not authorize a new order or
prove non-inclusion. Version 6 and older records are detected and rejected during PostgreSQL
restoration rather than silently omitted. Further pending-pool absence work is not the active
milestone; development now proceeds with the Phase E execution client, private event decoding, and
report reconciliation. Any future submission retry must be bounded and retransmit only the exact
durably recorded signed extrinsic. It must never allocate a new order identity or nonce after an
ambiguous outcome.

The Rust client implements the Nautilus `ExecutionClient` framework boundary for identity,
account-state emission, and lifecycle handling. Its Rust execution factory validates the typed
testnet configuration and constructs a disconnected framework client. Canonical recovery scans
default to ranges of 100 finalized blocks and require a non-zero configured range size. Timestamp
nonce restoration uses a configurable non-zero clock-drift limit which defaults to five seconds.
Execution configuration accepts an optional `postgres_cache_database_config` for the durable
transaction store. Its non-zero `websocket_timeout_secs`, defaulting to 30 seconds, bounds WebSocket
operations and startup account-event registration. After finalized runtime and account ownership
validation, the execution client
can acquire and retain the PostgreSQL signer lease, restore the complete durable signer record set,
and seed its timestamp nonce allocator. Reset, stop, and disconnect drop that runtime and release
the lease. Startup mass reconciliation uses only this client-owned store and lease; callers cannot
inject a temporary persistence boundary. The runtime also retains the fixture-approved snapshot
service. Its read-only chain-time operation first validates and applies runtime version and metadata
from one finalized Watch checkpoint, then strictly decodes `Timestamp.Now` as one non-zero SCALE
`u64` from that exact block hash. A runtime change is latched before any later timestamp-read
failure. This read-only operation does not allocate a nonce, sign, persist a transaction, submit, or
replay. A separate client-owned perpetual placement reservation
boundary combines that observation with the retained signer lease, allocator, snapshot permit, and
store. It rejects account/subaccount and instrument/market mismatches, holds the runtime permit
through the durable create, and releases only the exact acknowledged `created` record. The record
atomically retains the finalized block number and hash from the same runtime and timestamp
observation, providing a complete pre-submission recovery boundary. It also requires the Nautilus
order side to match the runtime's `is_long` direction. It still performs no
signing or submission and is not connected to an execution command. A second client-owned offline
boundary accepts only that acknowledged reservation, revalidates its signer, subaccount, market,
direction, and retained runtime, reconstructs every call argument and the timestamp nonce from the
durable identity, and returns signed bytes only after their exact CAS commit. It creates no
transmission permit, performs no network submission, and remains disconnected from order commands.
A third client-owned boundary revalidates that durable signed record and its canonical call,
commits the `submitting` transition, and only then releases a single-use transmission permit. It
does not consume that permit, perform network I/O, authorize retry or replay, or enable an order
command. The preparation can be split into the permit and its exact durable submitting context, so
consuming the permit never discards reconciliation authority. A client-owned acceptance boundary
commits `accepted` only from opaque hash-verified submission evidence matching that context; it also
performs no network I/O. A transport-neutral client coordinator can consume the permit exactly once
while holding the signer lease and runtime permit. It does not choose an endpoint or retry, and
returns the durable context for accepted, not-sent, rejected, ambiguous, and hash-mismatch outcomes.
`submit_direct_pallet_once_classified` is the conservative JSON-RPC transport for such a one-shot
attempt. Empty bytes and a locally inconsistent Blake2-256 hash return `NotSent` before any request.
After `author_submitExtrinsic` starts, transport errors, JSON-RPC errors, malformed responses, and
returned-hash mismatches are all `Ambiguous`; the shared RPC client cannot prove an authoritative
venue rejection or non-delivery. The transport never retries. An opt-in client-owned method can use
it only after complete startup, from the exact Submission endpoint and capability evidence retained
during runtime validation, while holding the signer lease and runtime permit. Reset, stop, and
disconnect erase that evidence. The framework command composes this transport with durable
reservation, signing, acceptance, and event delivery.
`build_perp_place_params` now provides the pure framework-to-runtime mapping boundary for simple
perpetual Limit GTC, Limit IOC, and Market IOC orders. Market orders map to `Market(None)` and a
zero input price, allowing the runtime oracle guard to set the execution price and enforce current
mark-price notional constraints. Limit orders convert quantity with the market `baseDecimal` and
price with the chain-defined `1e6` scale; all supported orders map buy/sell, reduce-only, and
post-only behavior exactly. It rejects Stop, FOK, Market GTC, IOC+post-only, quote quantity,
advanced order fields, inexact units, and out-of-range IDs before nonce allocation, persistence,
signing, or network access. `prepare_framework_perp_place_submission` composes that
mapping with the exact retained startup evidence, immutable order-context registration, finalized
chain-time observation, durable timestamp reservation, offline signing, and the CAS-committed
`submitting` transition. Mapping and startup failures occur before context mutation or nonce
allocation. The explicit workflow performs no transaction transmission or framework event
emission; the framework command uses a separate asynchronous coordinator for those steps.
`classify_perp_place_framework_order_events` is the pure evidence boundary for durable framework
event coordination. A durably committed `submitting` state is eligible for `OrderSubmitted`, but a
submission-pool acknowledgement is not `OrderAccepted` and best-block inclusion is not terminal.
Only finalized successful `PerpMarket.OrderPlaced` evidence is eligible for `OrderAccepted`, with
the event-verified timestamp nonce as venue order ID; finalized authoritative failure is eligible
for `OrderRejected`. Not-included evidence never implies rejection, and `ActionRequired` fails
classification because its prior submission state is not retained. Eligibility is not an emission
receipt: durable staging and crash-safe delivery acknowledgement are required. These primitives do
not independently enable `submit_order`. Version 5 framework-owned placement records added
the exact trader, strategy, instrument,
client order, account, command, initialized-order, correlation and causation identity, both command
and order initialization timestamps, and distinct preallocated submitted and terminal event IDs.
The framework preparation path validates this context before nonce allocation and commits it with
the initial reservation. Generic opt-in transaction records retain no framework context and cannot
be classified as event authority. Version 6 added a pending-only durable outbox. Version 7 records
exact consumer acknowledgement for each staged event. Revision-checked CAS stages currently
eligible events with stable IDs and first timestamps; repeated staging retains those values,
terminal payloads must match finalized evidence, and generic records cannot carry an outbox. The
staged entry also retains whether its terminal event came from reconciliation.
A pure materializer reconstructs `OrderSubmitted`, `OrderAccepted`, or `OrderRejected` in transition
order with the exact durable fields and the original command as causation. Staging and
materialization perform no channel send. The execution ingress acknowledges only events found
byte-for-byte in canonical order history.
An identical replay returns `AlreadyApplied`, while an event-ID collision, missing order, generic
runner path, or unapplied transition is not acknowledged. Channel admission never completes the
receipt. With no cache backing the receipt proves only in-memory application; with PostgreSQL or
Redis backing the ingress waits for the database worker to complete the idempotent event write.
PostgreSQL accepts an existing ID only when the complete stored payload matches. Redis likewise
avoids a duplicate append on exact replay and confirms only after rebuilding the order and updating
its indexes. A write failure, payload conflict, or dropped worker returns `Failed`; adapters without
receipt support remain `Unconfirmed`. Only an exact `Applied` or `AlreadyApplied` receipt paired
with `Persisted` can be committed through signer-lease and revision-checked CAS. That commit marks
the exact event acknowledged; all other outcomes leave it pending, and a terminal event cannot be
acknowledged before its submitted predecessor. The receipt does not mutate the outbox by itself.
At the end of mass reconciliation, a startup coordinator reloads the latest committed records,
stages eligible events, emits only pending entries in transition order, waits up to 30 seconds per
receipt, and commits each acknowledgement before proceeding. Dispatch failure, timeout, a dropped
receipt sender, an unconfirmed result, or any CAS uncertainty leaves the event pending and blocks
startup. Exact replay after a crash is accepted through `AlreadyApplied` plus a fresh durable
database receipt. This recovered-event path does not transmit transactions; framework
`submit_order` uses the separate one-shot transmission path described below.
`verify_perp_order_record_for_durable_place` separately binds one typed REST order observation to
the exact durable perpetual placement. It requires the timestamp nonce/order ID, subaccount,
three-way instrument/market identity, direction, exact size and price scales, supported Limit
GTC/IOC call, optional points, reduce-only/post-only flags, signed/lifecycle hash, REST
`EXTRINSIC_HASH`, and a nonzero reported height to agree. REST does not expose time in force, so
GTC versus IOC remains a fact retained by the durable signed call rather than REST evidence. The
returned height is explicitly a mutable backend observation: this verifier does not establish
canonical inclusion, dispatch success, a business event, or finality, and it does not emit a
framework event or relax the submission gate.
`observe_finalized_transaction_location` independently waits until the Watch endpoint's finalized
head reaches a requested REST height, then recomputes every extrinsic hash in that exact canonical
block and returns only one unique matching index. A missing or duplicate target fails closed.
`verify_finalized_perp_order_evidence` combines that location with the REST/durable binding only
when transaction hash and block height agree. The combined object deliberately remains separate
from `DeepXInclusionEvidence`: finalized transaction membership plus mutable REST corroboration
alone does not prove runtime dispatch or an authoritative business event. Authoritative event
proof is instead obtained independently by the exact-block recovery scanner. Neither REST
combiner advances the durable lifecycle or emits `OrderAccepted`.
Idempotent public HTTP reads support strictly validated bounded retry timing and ordered testnet
endpoint failover. Cursor-based reads fail closed when a response claims another page without a
usable continuation cursor. Bounded raw perpetual trade, funding-rate, and long-short-ratio
history, Spot and perpetual candle history, and perpetual mark-price and oracle-price history
support explicit pagination. Position-scoped perpetual order reads preserve cursor pages and
validate typed order ownership. Open-interest history remains a single-page endpoint. Execution
startup binds the loaded market catalog to that complete endpoint list. The execution client can
produce tracked single-order status reports, bounded current perpetual-position reports, and
bounded Spot and Perpetual fill reports from that immutable catalog. It can also calculate exact
linear-perpetual maker rebates and taker commissions when the supplied instrument, quantity, price,
and liquidity side match that catalog; values requiring quote-currency rounding fail instead of
being estimated. The raw wallet-grouped reader preserves zero-sized records, while account-trade
validation and framework fill conversion reject non-positive quantities and prices. It can replay
the current account state, refresh it from one finalized checkpoint when fully withdrawable and
exposure-free, and asynchronously query one tracked perpetual order after startup. Open exposure or
incomplete evidence leaves the existing account state unchanged.
DirectPallet perpetual fill reports require every returned REST trade to match the owner, market,
order, financial values, and trade ID of a finalized `PerpMarket.NewTrade` event. The read is
limited to 100 distinct finalized blocks per report request and returns no reports when historical
runtime or event evidence is unavailable. The known testnet historical runtime inconsistency
currently prevents this check for affected blocks. DirectPallet Spot fill reports likewise require
each REST record to match a finalized `SpotMarket.OrderMatched` event at its encoded block height,
using the validated market's base and quote decimals. The same 100-block limit applies separately
to Spot observations. No reports are returned when the historical Spot runtime or event evidence
is unavailable.
An offline execution test serves the contributor wallet's two captured market-3 partial fills and
their complete event batch through local REST and RPC endpoints. It proves report-path wiring under
approved fixture data, not a successful live historical RPC recheck or a storage proof.
Autonomous network startup is operational only for the strict finalized flat-account subset.
Framework `submit_order` supports validated perpetual Limit GTC/IOC and Market IOC orders, plus
Spot base-quantity Limit GTC/IOC orders, through durable DirectPallet pipelines. Each records the
reservation, signed bytes, submitting transition, hash-verified acceptance, and `OrderSubmitted`
delivery acknowledgement before one-shot transmission. Spot amounts use the cached instrument's
finalized quantity/price increments and currency precision; the runtime requires six-decimal quote
precision for this mapping. A client admits each placement client-order ID only once, including
IDs restored from durable startup records; duplicate submit commands do not start another task or
deny an order whose first submission may still be unresolved.
Failures during the read-only chain-time and identity preflight emit `OrderDenied` before any
nonce reservation. Once reservation begins, uncertain persistence or transmission outcomes remain
with durable recovery and do not produce a speculative terminal order event.
Finalized reconciliation alone can authorize a terminal event, and a
Spot acceptance requires its matching indexed `StateOrderBuy` or `StateOrderSell` event. Spot
testnet order validation and fill behavior remain unverified.
Tracked local perpetual and Spot Limit GTC order cancellations use separate durable transaction
keys and `OrderPendingCancel` receipts before one-shot DirectPallet submission. `OrderCanceled` and
`OrderCancelRejected` require matching finalized dispatch and cancellation-event evidence; ambiguous
transmission remains durable for startup reconciliation and is never retried. Spot cancellation
requires an ordinary cancel event. Framework batch cancellation fans out validated items through
the same durable single-order path, preserving the parent command's causation and correlation IDs;
it is non-atomic and does not cover orders absent from the local cache. Fast Spot cancel,
venue-native batch cancellation, modifications, and private execution streaming remain disabled.
Independent order lists of at most 20 orders fan out through durable single-order placement.
Every member is checked before dispatch; the placements are non-atomic, so an earlier member may
proceed if a later dispatch fails. Contingent and linked lists are unsupported.
`cancel_all_orders` filters open cached orders by this client, account, strategy,
instrument, and optional side, then uses the same non-atomic single-order cancellation path.
It cannot cancel venue orders absent from the local cache or without verified local tracking.
Spot testnet cancellation behavior is not yet verified.
Mass status is available only with its explicit incomplete-report marker.

The execution client can collect the runtime startup step through one read-only coordinator. It
validates chain identity and advertised methods for the configured Submission, Watch, and Recovery
RPC roles, bootstraps from the identity-validated Watch endpoint, and atomically applies a second
finalized fixture-approved snapshot before retaining any evidence. Any failure leaves the startup
gate unchanged. This runtime-validation coordinator does not initialize PostgreSQL transaction
state, connect an account stream, or authorize signing; those remain later startup gates.

The following ownership step also has a read-only coordinator. It derives the wallet identity from
the configured signing key, retrieves the wallet directory and configured subaccount profile, and
retains a proof only when both REST observations bind the same active account relationship. A
caller-supplied wallet cannot substitute for the signer-derived identity. This remains point-in-time
REST evidence and does not prove stream authentication, freshness, or transaction authorization.

The execution client can then open and retain the exact address-scoped `user_balances` connection.
It advances only after the acknowledgement matches the retained signer-bound ownership proof.
Foreign acknowledgements are terminal and are never retained. Async disconnect performs a bounded
close handshake; synchronous reset revokes the subscription proof and drops the task-free transport.
This connection step does not interpret balances or emit an account state.

Fill reports read at most 100 pages of 100 account trades for the configured subaccount. They
preserve venue trade and order identities, apply optional instrument, venue-order, and exact
nanosecond time filters, and attach a client order ID only when registered context agrees with the
trade market and side. Perpetual commissions use the immutable market quote currency. Spot reports
resolve an empty `feeAsset` to the received asset, base for a buyer and quote for a seller, as
specified by the chain `OrderMatched` event. Both paths negate the signed REST account-balance
delta into Nautilus commission and reject fee assets or signs that disagree with startup market
metadata. These observations do not establish a block-pinned history snapshot or completeness
under concurrent writes.

The raw perpetual trade, mark-price, and oracle-price candle requests expose exactly the OpenAPI
time frames `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `8h`, `12h`, `1d`, `3d`, `1w`, and
`1M` through a closed Rust enum. Requests remain single-page, ascending, non-TradingView reads with
an explicit limit of at most 5000 records. Read-only `1m` and `3m` probes on 2026-09-15 showed
bucket-open timestamps and included the current incomplete bucket. They also exposed JSON values
such as `2481.3799999999997` for a market with a `0.01` price increment. The raw decoder preserves
those numeric tokens exactly. Framework `request_bars` supports standard, externally aggregated
last-price bars for the same closed interval set. It preserves venue `time` as `ts_event` without
inferring open- or close-time semantics, verifies pair identity and inclusive request bounds, and
collects time pages in order. Live Spot, perpetual trade, mark-price, and oracle-price candle
queries all excluded their `end` timestamp; framework Spot and perpetual requests add one
millisecond to preserve an inclusive upper bound. An explicit request
limit caps the total result; an unlimited request
uses at most 100 pages of 5000 rows and fails without partial data if that budget is exhausted.
Each continuation starts one millisecond after the previous page's last candle, and the echoed
market identity must remain stable across pages. OHLCV conversion proceeds only when every value is
exactly representable at instrument precision. Spot uses
the verified `CurrencyPair` precision; live testnet candle volumes currently contain excess
decimal places, so those responses fail closed instead of being quantized. The request limit is
5000. Framework streaming bars are limited to one-minute perpetual candles. The documented public
WebSocket has a `candles` channel. A testnet `1m` subscription on 2026-10-08 acknowledged the time
frame, then sent repeated updates for the current bucket before a later bucket arrived. The raw
protocol state accepts the exact market, interval, channel, and pair, retains the latest update,
and reports the
preceding bucket when a newer bucket arrives. The read-only
`observe_first_superseded_perp_candle` API returns one such raw observation and its skipped-minute
flag, then closes the WebSocket. A live testnet run on 2026-10-08 observed an `ETH-USDC` market 3
bucket at `1791478800000` with exact volume `56.768` and no gap before its successor. A later
WebSocket-to-REST comparison found different volume, close, and high values for the same bucket;
after another read, REST volume and close converged but its high remained the float artifact
`2418.1899999999996` against WebSocket `2418.19`. The WebSocket observation is not a guaranteed
final OHLCV value, and historical conversion still rejects unrepresentable prices. The raw observer
does not publish framework bars. Historical OHLC values and
volume must also be exact multiples of the finalized tick and step sizes.
The read-only `deepx-verify-ws-candles` command polls the same REST bucket for up to 90 seconds
after the WebSocket successor, requiring exact OHLCV equality. A 2026-10-09 observation initially
disagreed on volume (`38.654` versus `65.426`) before REST converged to the WebSocket value.
A second bucket, `1791492300000`, matched on the 33rd REST read without a skipped minute.

The raw `get_perp_mark_prices` and `get_perp_oracle_prices` methods remain single-page reads.
Their `*_history_limited` counterparts collect ordered time pages under an explicit page budget;
an explicit request limit caps total observations, while unlimited reads fail rather than return
partial history when the page budget is exhausted. They preserve the response pair identity across
pages and use the same one-millisecond continuation cursor as candle history.

The long-short-ratio history collector follows the endpoint's opaque cursor while retaining its
fixed time bounds. It validates ascending timestamps and market identity, and fails without
returning partial history when its page budget is exhausted. A single open-interest page now checks
the venue's minute-rounded bounds, strict ascending order, pair identity, page size, and nonnegative
values before returning typed records. Open-interest history has no cursor,
but `get_perp_open_interest_history` pages over the documented one-minute buckets with a fixed end
and explicit page budget. A live public query confirmed that a one-millisecond `start` increment
repeats the previous bucket, while the minute at `end` is excluded. The collector rounds the query
bounds to preserve inclusive caller bounds and advances to the next minute on continuation. It
rejects changed pair, overlapping or invalid buckets, and budget exhaustion without returning
partial history. A range containing no minute bucket rejects before HTTP because an empty page
cannot carry a verified market pair. `deepx-verify-rest-open-interest-history 3 1788251280000 1788251460000 2 3`
returned four exact historical ETH-USDC buckets across two public testnet pages on 2026-10-10.
`deepx-verify-ws-open-interest 3` observes one acknowledged public `open_interest` update, binds
market 3 and `ETH-USDC` to the validated REST directory, and retains exact decimals and original
JSON. A 2026-10-10 read-only run observed bucket `1791632940000`, total open interest `3332.231`,
ratio `2.57875`, and 2063 long versus 800 short positions. The optional `--reconcile` mode reads
that exact REST minute and reports only on an identical pair and record. A later run rejected bucket
`1791633000000`: WebSocket total open interest was `3325.824`, while REST returned `3326.907`;
the ratio and counts agreed. These mutable reads do not prove continuity, units, or framework
open-interest semantics.

`DeepXHttpClient::get_perp_trades_history` reads a complete inclusive millisecond range with explicit
page-size and page-count budgets. Every page retains the time filters. It rejects duplicate IDs,
out-of-range or invalid timestamps, ascending page boundaries, oversized pages, and cursor loops;
budget exhaustion never returns a partial history. Raw exact prices, sizes, fees, and taker labels
remain uninterpreted. `cargo run -p nautilus-deepx --bin deepx-verify-rest-trade-history` verifies
the public testnet path against a recent observed ETH-USDC trade, without private account access.
The REST-connected framework client also supports `request_trades` for perpetual instruments.
It returns the most recent matching trades in chronological order with correlation/client identity
and exact instrument precision. Unknown takers, zero IDs/sizes, invalid timestamps, and lossy
conversion fail without partial responses. The default record limit is 1000, maximum 10000, with
100-row pages and a 100-page budget. Missing start means the epoch; missing end snapshots the
framework clock at dispatch. Async reception timestamps use the live atomic clock. Retirement
fences response emission and cancels owned tasks; reconnect drains them before opening a fresh
generation. Nanosecond starts advance to the first eligible millisecond before REST pagination;
a range with no eligible millisecond returns an empty correlated response. Spot `request_trades`
uses the verified pair directory, bounded global cursor history, exact execution amounts up to
the base token's atomic precision, finalized price tick checks, and chain-defined Buyer/Seller
taker roles. Partial fills may be smaller than the order step. Unknown takers, foreign pairs,
precision loss, off-tick prices, or malformed times reject the whole response.
`deepx-verify-spot-trade-history`
compares live raw and framework ETH/USDC executions.
A framework testnet run on 2026-09-14 verified three chronological ticks over
`1789373271239..=1789373272239`, including the observed latest trade without price/size rounding,
matching request correlation, and successful disconnect.
A public run on 2026-09-14 verified two ETH-USDC trades across two size-one pages in the
inclusive range `1789372702699..=1789372703699`. The live probe also identified and verified the
fix for decimal JSON leverage values such as `25.0`.

`load_deepx_perp_trade_ticks(market_id, start_ms, end_ms, limit=1000, max_pages=100)` exposes the
same bounded perpetual history and validated current market instrument to Python backtests. It
returns chronological ticks with initialization times set to their event times for replay. The
market metadata is current, so a historical run does not reconstruct past tick sizes, fees, or
margin rules. From the repository root, run the read-only live data tester with
`.venv/bin/python examples/live/deepx/data_tester.py`, or run the simulated trade-tick backtest
with `.venv/bin/python examples/backtest/deepx_ema_cross_ethusdc_trades.py`. The backtest accepts
`--start-ms` and `--end-ms` together for a fixed UTC millisecond window and sends no transactions.
`load_deepx_spot_trade_ticks(market_name, start_ms, end_ms, limit=1000, max_pages=100)` uses the
bounded public Spot history and a finalized-chain-verified current Spot instrument. It likewise
returns chronological replay ticks and does not reconstruct historical trading rules. Run the
simulated Spot example with
`.venv/bin/python examples/backtest/deepx_ema_cross_ethusdc_spot_trades.py`. The public testnet
runtime observed on 2026-10-14 used genesis hash
`0x87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7`, specification
376, and metadata SHA-256
`57f1bedf1e04522176a2b422af9249d5c990693fb92d11327870d8ef751a8447`.
The read-only runtime snapshot and Spot storage decoder approve this captured identity. A public
run loaded 1,000 exact Spot ticks and simulated eight fills. The Spot example disables optional
portfolio analysis and omits the account report because the shared analysis path converts
18-decimal ETH money to `f64` and panics; fill and position reports remain available.
`examples/live/deepx/exec_tester.py` also wires the validated execution client to the framework
test strategy. It first checks the public account snapshot and subaccount directory of wallet
`0x781ed35b167068c93dfadab41dfb680edaca4e50`; select an address from that directory with
`--subaccount` if there is more than one. Startup requires `DEEPX_TESTNET_PRIVATE_KEY` and a durable
PostgreSQL store configured through `POSTGRES_USERNAME`, `POSTGRES_PASSWORD`, and
`POSTGRES_DATABASE` (`POSTGRES_HOST` and `POSTGRES_PORT` are optional). By default the tester only
connects and subscribes. `--submit-order` opts into one ETH-USDC perpetual IOC market buy of
`0.0010` and a reduce-only shutdown close request scoped to the tester's positions. These are real
testnet transactions and are not guaranteed to fill or close. Account ownership, finalized
flat-account state, runtime identity, durable recovery, and signing remain enforced by the Rust
client. The public snapshot preflight rejects inactive accounts, nonzero margin requirements, and
outstanding borrowing; it does not establish the finalized flat-account proof. A 2026-10-13
read-only snapshot returned three owned subaccounts, one with a nonzero margin requirement.
Live execution startup and submission have not been verified against this wallet.

`get_perp_order_book` exposes one exact, potentially price-aggregated REST snapshot for a required
perpetual market ID. It validates the echoed book and per-level market identity, positive prices
and quantities, nonnegative server notionals, unique strictly ordered levels, sequence, and engine
time. The optional positive aggregation tick is serialized as exact decimal text. The mutable
snapshot remains separate from the framework WebSocket book pipeline and does not establish full
exchange depth, freshness, or atomic agreement with a stream. A spec-369 fixture preserves the
observed zero `latestPrice` and exact `midPrice`; backend float artifacts are never quantized.
Run `deepx-verify-rest-perp-order-book <market-id>` for a credential-free live check.

`get_spot_trades` exposes one globally ordered raw Spot execution page selected by optional market
name or bytes32 pair, wallet, inclusive millisecond bounds, sort order, and cursor. Financial JSON
lexemes, including scientific notation, decode directly to exact `Decimal` values. The response
retains its venue `total`; addresses, decimal order IDs, pair identity, positive execution values,
timestamps, ordering, page size, and cursor metadata are validated. `get_spot_trade_pages` follows
the single global cursor within an explicit page budget and rejects duplicate IDs or ordering
violations across pages without returning a partial collection. Mutable totals are not required to
remain constant across pages. The raw reader itself does not infer fee assets, Spot quantity
precision, framework aggressor semantics, or `TradeTick` conversion.

`deepx-verify-rest-spot-trades <market-name> [wallet-account-id20]` validates one credential-free
page. A public `ETH/USDC` run on 2026-09-17 returned 100 exact records and reported a continuation
against more than 11 million matching rows. Direct probes with the contributor wallet filter
returned API code `10012` (`Service temporarily unavailable`); the adapter verifier exhausted its
retry path with HTTP 504. Successful wallet-filtered Spot history therefore remains unverified,
and neither failure is interpreted as an empty history. No credentials or transactions were sent.

`get_spot_candles`, `get_spot_last_price`, `get_spot_volume`, and `get_spot_order_book` expose exact
public Spot market observations selected by exactly one market name or bytes32 pair. Candle requests
are ascending, non-TradingView reads over the same closed interval set as perpetual candles and
accept at most 5000 rows. They validate echoed market names, bounds, strict ordering, positive OHLC,
nonnegative volume, and requested limits. Spot books validate both echoed identities, exact positive
levels, unique strictly ordered prices, sequence, and engine time. A positive optional `tick_size`
controls server aggregation; aggregated buckets can overlap and notionals can be independently
rounded, so no uncrossed-book or exact notional-product invariant is invented. Scalar last-price
and volume responses do not echo the selector. Candles, last price, and volume remain raw
observations without framework event semantics. `request_book_snapshot` converts the validated Spot
book into one bounded L2 `BookResponse`, using the venue sequence and engine time. A read-only `ETH/USDC`
run on 2026-09-17 validated recent candles, last price, one-hour volume, and a 20-by-20 book. A
current raw `ETH/USDC` response contains aggregated volumes such as `7.381581525662676`, which
the exact framework converter rejects against the finalized four-decimal size precision. OHLC
values and volume must also be exact multiples of finalized tick and step sizes. Run
`deepx-verify-rest-spot-market <market-name>` to
repeat the credential-free raw check. `deepx-verify-spot-instruments` verifies Spot construction,
and `deepx-verify-spot-trade-history` verifies framework trade conversion.

`get_spot_markets` now validates the complete public directory, including name/symbol agreement,
bytes32 pair and AccountId20 asset identities, distinct base/quote assets, positive tick and guard
values, and unique case-insensitive names and pairs. `get_spot_market_by_name` and
`get_spot_market_by_pair` bind the returned identity to the exact selector. The live verifier
cross-checks stable ETH/USDC metadata across the directory and both lookups while deliberately
excluding mutable price and nullable 24-hour change observations. The REST tick is checked against
the finalized runtime tick at its `1e6` scale; minimum order and step sizes use base-token atomic
units at `baseDecimal` precision. The instrument verifier loads both listed Spot pairs together
with the Perpetual catalog at one finalized checkpoint.

Separate account-Spot probes on 2026-09-17 found terminal empty order and trade pages for all four
subaccounts registered to the contributor wallet. A subaccount discovered from a contemporaneous
public ETH/USDC trade supplied nonempty active-order, history-order, and trade evidence. The
`get_spot_open_orders_raw` and `get_spot_history_orders_raw` readers retain forward-compatible raw
items; their typed counterparts decode exact decimal order values and validate ownership, requested
name or pair and side, decimal order IDs, timestamps, transaction hashes, financial bounds,
duplicates, requested ordering, page size, and cursor metadata. Active orders require exactly one
market name or bytes32 pair. Historical orders may omit both selectors for all markets, and the
bounded history collector rejects stalled cursors or budget exhaustion without returning a partial
collection. Venue status, price type, post-only, transaction-hash type, fee ownership, and lifecycle
semantics remain uninterpreted, so these readers do not construct framework order reports. Run
`deepx-verify-rest-spot-orders <subaccount> <market-name>` for a credential-free live check. Typed
Spot wallet-group reads use the dedicated order and trade endpoints. Both validate exact market
and subaccount groups, per-group time ordering, one global cursor across nonempty groups, requested
global page size, and duplicate identities across bounded pages. Order makers must match their
enclosing subaccount. Trade records do not echo an owner, and neither response echoes the requested
wallet, so the adapter preserves grouping without independently rebinding wallet ownership. Empty
terminal groups observed for the contributor wallet remain valid. Wallet orders may omit both the
wallet and market selectors for the venue's all-wallet view; wallet trades require an exact wallet
and exactly one name or pair because a read-only selector-free probe returned API code `10011`.
Run `deepx-verify-rest-spot-wallet <wallet-account-id20> <market-name>` for a credential-free
single-page live check; empty terminal pages are reported as zero records, while empty pages that
advertise continuation fail. Venue order, taker, side, and fee semantics remain uninterpreted, and
no framework order or fill report is constructed.

`get_spot_order_by_id_raw` retains a forward-compatible exact lookup payload, while the typed
`get_spot_order_by_id` binds the returned owner, market identity, side, and decimal order ID to the
request. `get_spot_order_by_tx` validates a 32-byte transaction hash and requires the response to
echo it exactly. Both validate the shared order record without assigning lifecycle or finality
semantics. A captured order changed from `Open` in the earlier history fixture to `Canceled` in the
lookup, with `UserCanceled` and a cancellation height; this directly demonstrates mutable REST
state rather than canonical transaction finality. Run
`deepx-verify-rest-spot-order-lookup <subaccount> <market-name> <order-id> <Buy|Sell> <tx-hash>` to
cross-check that both lookup paths currently return the same record.

`get_spot_account_trades_raw` preserves one subaccount-selected execution page without interpreting
its item payloads. `get_spot_account_trades` and the bounded raw/typed page collectors preserve
exact price, base amount, quote amount, and signed fee values; validate market and optional exact
order filters, positive execution values, timestamps and inclusive bounds, venue ordering, unique
trade IDs, cursor progress, and page budgets. The observed maker rebate is negative and its
`feeAsset` is empty while the OpenAPI example omits that field, so neither is assigned framework
fee semantics. The response does not echo the requested subaccount on its records and therefore
cannot independently rebind ownership. Taker and
order-side labels remain uninterpreted, and no fill report is constructed. Run
`deepx-verify-rest-spot-account-trades <subaccount> <market-name>` for a credential-free live check.

`request_funding_rates` returns exact chronological historical minute-bucket samples through
`DataResponse::FundingRates`. Its bounded descending REST reader retains the range across cursor
pages and rejects duplicate/out-of-order buckets, wrong market IDs, unaligned times, oversized
pages, and invalid cursors. The default record cap is 1000, maximum 10000, with a 100-page budget;
missing bounds have the same epoch/dispatch-clock behavior as trade history. Original nanosecond
bounds filter returned bucket times. Payment interval and next funding time remain `None`: the
API's one-minute aggregation is not a payment schedule. Both history types use the same owned
task admission, cancellation, and epoch-fenced emission. The public history verification program
also checks raw/framework funding values and times and absence of an inferred payment schedule.
A public run on 2026-09-14 verified three exact chronological funding samples alongside three
historical trades, matching raw bucket times/rates and request correlation with no payment schedule.

The Rust data factory validates a strict testnet `DeepXDataClientConfig` and constructs a
disconnected framework client with the DeepX identity, read-only cache view, and framework clock.
Its `connect` method loads the public catalog, verifies every Spot spec at a shared finalized
checkpoint, and publishes Spot and Perpetual instruments through the framework data event sender,
then enables `request_instrument` and `request_instruments`. Each explicit request independently
reloads and validates the complete catalog before returning a correlated response; a failed
refresh emits no partial response and does not change the connected instrument store. It
can use `perpetual_only=true` to load and publish only public Perpetual instruments without
requesting Spot metadata or finalized Spot specs. The default remains full discovery. It
requires a live data receiver and rejects unsupported history parameters, filters, and foreign
identities. Disconnect, stop, reset, and dispose
clear readiness and instruments; reconnect reloads. REST readiness alone does not open a socket;
explicit perpetual trade, book, quote, mark, index, and funding subscriptions lazily create public
streaming connections.

`cargo run -p nautilus-deepx --bin deepx-verify-rest-instruments` verifies this read-only path
against testnet without account access, signing, or submission. A live run on 2026-09-17
successfully cross-checked both ETH-USDC lookup routes, published and queried nine perpetual
instruments, and disconnected. The captured public directory response and provenance sidecar are
`test_data/http/testnet/perp_markets_spec369.*`; the response is not block-pinned and establishes
no streaming or trading authority.

`get_perp_market_by_id` and `get_perp_market_by_name` expose validated single-market lookup
responses. Those responses omit the directory's tick size, quantity step, height, network,
open-interest, active-order limit, deletion state, and 24-hour change, so they use a separate
partial model and cannot construct instruments. A 2026-09-17 ETH-USDC capture also reported
`liquidationDustValue=50` while the directory reported raw `50000000`; the adapter preserves each
exact value without assuming a shared scale. The instrument verifier cross-checks only stable,
same-scale identity, precision, margin, fee, and minimum-order fields across both lookup routes.
`observe_finalized_spot_market_spec_evidence` reads raw `min_order_size`, `tick_size`, and
`step_size` from metadata-typed `SpotMarket.SpotMarkets` storage for a bytes32 pair at an approved,
canonical finalized checkpoint. The provider reads all directory pairs at one shared checkpoint
after checking the block timestamp inherent, both timestamp storage routes, and rejection of an
impossible storage block hash. Each hash-pinned storage value must identify the requested pair and
remain unchanged on a repeated read. The provider converts base-token atomic units with
`baseDecimal`, rechecks the timestamp after all
pair reads, and publishes the complete Spot/Perpetual catalog only after every conversion succeeds.
The public RPC accepted an impossible canonical hash for EIP-1898 `eth_call`, so Spot precision
discovery uses Substrate storage; Perp precompile reads retain their independent EIP-1898 gate.
`observe_finalized_perp_order_spec_evidence` reads the Perp precompile's raw minimum size, tick,
and step and the pallet's minimum notional at one approved, canonical finalized checkpoint. The
storage value is decoded with that checkpoint's approved metadata, and the shared fields must
agree. The reader also checks `Timestamp.Now` against the block inherent and an independent
storage query, then rechecks the timestamp after the market reads. This does not by itself
authorize trading; market status, price guards, and account limits also apply.

The typed read-only account snapshot includes wallet/subaccount identity, lending balances,
equity, collateral, and the account-wide margin requirement. The margin-ratio endpoint preserves
exact decimal values and nullable ratio semantics, and rejects negative values. It does not state
whether the requirement uses initial or maintenance weights, so the adapter does not derive a
Nautilus margin balance, free collateral, or account state from it.

The chain runtime exposes direction-specific aggregate collateral and margin requirements through
`PerpRuntimeRpcApi_total_collateral_and_margin_required_for`. The adapter queries both initial and
maintenance directions with `state_call` at one fixture-approved finalized Watch checkpoint and
strictly decodes the exact requested AccountId20 entry. It deliberately does not use the node's
`lending_totalCollateralAndMarginRequiredFor` custom RPC because that implementation currently
accepts an `at` parameter but reads `best_hash` instead. The node's `lending_batchPositionsFor`
implementation has the same block-pinning defect. These aggregates remain raw runtime quote
units: they do not identify per-asset deposits, borrows, pending-order locks, or Nautilus
`AccountBalance` free/locked values, and they cannot construct an `AccountState`. The execution
client exposes the observation only through its retained RPC capabilities, ownership proof, and
transaction runtime snapshot service. The credential-free
`deepx-verify-finalized-margin <subaccount-account-id20>` command instead uses a distinct read-only
runtime snapshot. The same exact specification 371 identity is also signing-approved after its
captured metadata and signed-extension/type identity were compared and independently generated
Python SDK vectors reproduced the complete encoded extrinsics. Unknown metadata or later
specifications remain rejected.

The adapter also calls `LendingRuntimeRpcApi_batch_position_for` with the complete `MarketId=u8`
domain at one fixture-approved finalized checkpoint. It strictly decodes at most one requested
AccountId20, canonical ordered market and asset maps, the runtime's 64-assets-per-market and
32-byte-symbol bounds, non-zero V2 scaled positions, and no trailing SCALE data. For every returned
asset it calls `pool_accounting_v2`, `effective_deposit_v2`, and `effective_borrow_v2` at the same
block hash and requires the pool, market, asset, and scaled/effective presence semantics to agree.
For every deposit it also calls lending precompile `0x0450` method `maxWithdrawAmountFor` using an
EIP-1898 canonical block-hash selector for that checkpoint. The resulting evidence retains scaled
shares, runtime-calculated effective amounts, token decimal, raw interest indices, pool accounting,
and the raw maximum withdrawal without recomputing financial values. Maximum withdrawal is capped
by account-wide initial free collateral, the effective deposit, and pool liquidity, so it is not a
Nautilus free balance or proof that the remainder is pending-order locked. Likewise,
`pool_locked_liquidity` is whole-pool accounting and is not a user's locked balance. Empty account
evidence is valid, but malformed, foreign, contradictory, or partially unavailable evidence fails
closed. The execution client exposes the observation only for its retained owned subaccount; the
credential-free verifier can inspect an arbitrary AccountId20 without caching or publishing account
state:

```bash
cargo run -p nautilus-deepx --offline --bin deepx-verify-finalized-lending -- \
  0x781ed35b167068c93dfadab41dfb680edaca4e50
```

This is raw finalized lending evidence, not Nautilus free/locked semantics or a complete
`AccountState`. The verifier uses the same specification 371 read-only snapshot boundary as the
margin verifier. That approval does not extend to transaction runtime initialization, signing,
reservation, or submission.

On 2026-09-23 the specification 371 verifier returned valid empty evidence for wallet
`0x781ed35b167068c93dfadab41dfb680edaca4e50`. A separate read-only run for the known active
subaccount returned one USDC deposit and successfully exercised `maxWithdrawAmountFor` at the same
finalized block as its runtime and pool evidence. These live values are mutable observations, not
fixtures or balance semantics.

Finalized perpetual positions can be inspected directly from
`PerpMarket.UserPerpPositions`, together with `Subaccount.MaxPerpPositions`, through one
`state_queryStorageAt` request pinned to the fixture-approved finalized checkpoint. The spec 371
metadata fixture independently confirms the Identity-hashed AccountId20 key, exact
`Vec<PerpPosition>` field order, default empty vector, and default maximum of eight positions. The
decoder bounds allocation by the observed runtime maximum, requires the embedded owner to match the
requested subaccount, rejects duplicate markets and invalid zero-valued position invariants, and
requires complete canonical SCALE consumption. It returns the runtime's raw direction, amount,
price, leverage, funding, realized-PnL, owner, take-profit, stop-loss, and settlement fields in
market-ID order. These fields are not a REST position lifecycle, a Nautilus position report, or
proof of current PnL and funding semantics. The credential-free verifier uses the spec 371
read-only runtime gate and cannot authorize signing or mutation:

```bash
cargo run -p nautilus-deepx --offline --bin deepx-verify-finalized-perp-positions -- \
  0x781ed35b167068c93dfadab41dfb680edaca4e50
```

On 2026-09-23 both that wallet and known active subaccount
`0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc` returned valid empty position vectors with runtime
maximum eight. No non-empty live position has yet been captured, so REST/chain reconciliation,
freshness policy, and complete framework position semantics remain unproven.

Active perpetual orders can likewise be inspected from `PerpMarket.ActivePerpOrdersFor` and every
referenced `PerpMarket.OrderInfo` value at one finalized checkpoint. The spec 371 metadata fixture
confirms the Identity-hashed AccountId20 index to
`BTreeMap<market_id, Vec<ActiveOrder>>`, the double Identity `(AccountId20, order_id)` detail key,
the exact 16-field `PerpOrder`, and all nested enum variant indices, including the non-declaration
order `Buy = 1` and `Sell = 0` SCALE values. Decoding bounds markets and total orders before
allocation. It rejects non-canonical maps, duplicate IDs, ghost index entries, foreign owners,
non-active details, field disagreement, trailing bytes, and any order whose filled plus remaining
size does not equal its total size. Results remain raw chain storage; they are not REST lifecycle
records or Nautilus order reports. The credential-free verifier uses only the spec 371 read-only
runtime gate:

```bash
cargo run -p nautilus-deepx --offline --bin deepx-verify-finalized-perp-orders -- \
  0x781ed35b167068c93dfadab41dfb680edaca4e50
```

On 2026-09-23 the contributor wallet and all four subaccounts in its public directory returned
valid empty active-order indexes. The non-empty index-to-detail path is covered by a spec 371 mock,
but no non-empty live order was available for chain/REST reconciliation. A fresh query of the known
active position lifecycle ID `1659167` still returned service code 503 from the documented REST
`position-orders` projection, stating that it requires repair. Framework reconciliation and
freshness semantics therefore remain disabled.

`observe_finalized_account_evidence` combines finalized `Timestamp.Now`, margin, lending,
perpetual-position, and active-order readers after one read-only runtime approval and pins every
request to that exact checkpoint. It requires `state_getStorage` in addition to the component RPC
methods and accepts the timestamp only as one exact non-zero SCALE `u64` Unix millisecond value.
The shared read-only runtime boundary checks both historical runtime methods against the captured
spec-369 checkpoint before any standalone account, market, lending, position, or order evidence is
read. An inconsistent endpoint fails closed without returning those component values.
`deepx-verify-finalized-account <subaccount-account-id20>` exercises the combined credential-free
boundary. Component evidence retains the same strict bounds and identity checks; an unavailable or
invalid component rejects the complete observation without returning a partial snapshot. This is
an atomic raw-evidence envelope, not a Nautilus `AccountState`: asset currency, free/locked balance
semantics, REST lifecycle reconciliation, freshness, and framework report construction remain
unproven. The execution-owned variant additionally requires retained RPC, account-ownership, and
transaction-runtime evidence. The exact spec 371 identity is separately approved by the signing
snapshot gate for offline encoding, but this evidence observer does not authorize signing or
mutation and transaction submission remains disabled.

`deepx-verify-finalized-wallet <wallet-account-id20>` resolves a wallet through the validated
public subaccount directory and attempts complete finalized account evidence for every returned
subaccount at one approved finalized checkpoint. Duplicate addresses or any incomplete account
result reject the entire batch without printing partial account evidence. The mutable REST directory
does not prove chain-level ownership, and the command does not construct a framework account
snapshot or grant mutation authority. A successful batch prints raw lending, perpetual-position,
and active-order fields and reports whether each account satisfies the existing exact flat-balance
conversion gate. The public RPC endpoint has returned the current spec 371 runtime for a captured
spec 369 block hash and has returned changing timestamp storage for a fixed finalized hash. The
observer rejects these contradictions; live shared-checkpoint wallet evidence remains unverified.
On 2026-10-09, the read-only verifier resolved all four subaccounts for wallet
`0x781ed35b167068c93dfadab41dfb680edaca4e50` but rejected the batch before emitting any
account evidence: historical `state_call(Core_version)` returned spec 371 for a checkpoint whose
approved runtime identity required spec 369. All default RPC roles use the same endpoint, so the
configured roles provide no independent source for that inconsistent response. Direct read-only
JSON-RPC requests confirmed that `chain_getBlockHash(184886760)` still returns the captured
spec-369 hash, while both `state_call(Core_version)` and `state_getRuntimeVersion` return spec 371
when explicitly passed that hash. The unpinned runtime-version call also returns spec 371. This
rules out local block-hash parameter construction as the cause of the observed mismatch. Both
runtime methods reject an impossible hash with `UnknownBlock`, so they parse the supplied hash;
the reason they return spec 371 for the known historical hash remains unverified.

A live read-only run on 2026-09-23 verified the combined boundary for owned subaccount
`0x4ded31cb63949b52f9dfc9bcfade4eab7017eadc` at finalized block `196050060`: both margin
directions, one USDC lending deposit with its effective and maximum-withdrawal values, an empty
perpetual-position vector with runtime maximum eight, and an empty active-order index all shared
block hash `0xaac093675fa1ec964b0e969db4dbdb8d7a874b9458cfdd4ed75e4d25cf45e99d`.
The contributor-provided `0x781ed35b167068c93dfadab41dfb680edaca4e50` wallet correctly
failed the subaccount-only aggregate at margin collection: the chain runtime skips addresses that
have no `SubaccountInfo`, so absence cannot be reinterpreted as zero margin. Its public directory
must first supply an exact subaccount for this observer.

`map_finalized_flat_account_balances` exposes one deliberately restricted framework conversion.
It accepts only an exposure-free finalized account: both margin requirements are zero, perpetual
positions and active perpetual orders are empty, every lending borrow is absent, and each effective
deposit exactly equals its `maxWithdrawAmountFor` result. Under those conditions the adapter can
prove `AccountBalance.total == AccountBalance.free` and `locked == 0`; runtime token integers are
converted with their pool decimal and any framework precision loss fails closed. Duplicate or
invalid assets and empty accounts are also rejected. A live run on 2026-09-23 at finalized block
`196066920` mapped the known owned subaccount's raw `970957430` USDC units to exactly
`total=free=970.95743000 USDC` and `locked=0`. This does not generalize to accounts with borrowing,
positions, orders, reserved margin, or pool/account withdrawal constraints.

`complete_startup_with_finalized_flat_account_state` now owns the corresponding restricted startup
path. It first requires the signer-bound ownership proof, durable transaction runtime, and current
acknowledged `user_balances` subscription. The WebSocket frame supplies subscription liveness only;
its balance values are not mixed with finalized chain data. The client observes chain time, margin,
lending, perpetual positions, and active orders at one finalized checkpoint, applies the strict
flat-account mapping, verifies the retained runtime and subaccount identities, and revalidates the
subscription after the asynchronous RPC work. It then publishes a reported `AccountState` using
finalized `Timestamp.Now` as `ts_event`, the local clock as `ts_init`, empty margins, and the
configured framework account ID and type. Startup still fails closed for every non-flat account or
if dispatch, durable reconciliation, exact cache registration, or cleanup fails.

`get_delegate_accounts` and `get_delegator_accounts` expose both directions of the public wallet
delegate directory. They validate all AccountId20 values, reject case-insensitive duplicates,
decode the four chain-defined modes, and enforce the chain's millisecond creation/expiry ordering
while retaining zero as the documented legacy no-expiry value. The account snapshot verifier also
checks that every returned delegate's reverse directory contains the queried wallet. A read-only
testnet run on 2026-09-17 found one active `PlaceOrCancelOrder` delegate for the contributor wallet.
These mutable REST observations do not authorize signing, establish freshness or completeness, or
enable delegate mutations. The raw delegate reader remains available for unknown future fields.

`get_quota_summary` exposes the wallet-level public quota aggregate with exact Spot, perpetual, and
total USD volumes, venue quota counters, and paired RFC 3339/millisecond trade timestamps. It binds
the response owner to the request and rejects negative or inconsistent totals and timestamps. The
account snapshot verifier also requires its subaccount count to match the independently queried
wallet directory. A read-only run for the contributor-provided wallet on 2026-09-17 returned four
subaccounts, total volume `2970.527580000000000000`, earned quota `2970`, and pending quota `2970`.
The quota history endpoint was empty for that wallet. A public directory wallet supplied a
nonempty chain-confirmed purchase record, enabling `get_quota_history` and its bounded collector.
They bind owner and optional buyer/type filters, validate closed history and buyer classifications,
chain identities, transaction hashes, descending timestamps, duplicates, limits, and cursor
progress. Purchase, activate, and free values remain venue records rather than claim authority.
Run `deepx-verify-rest-quota-history <wallet-account-id20>` for a credential-free nonempty
single-page check.
Run `deepx-verify-rest-quota-summary <wallet-account-id20>` for a credential-free aggregate check.
Python `get_deepx_quota_summary(wallet, config=None)` returns the same validated aggregate as a
read-only result with exact USD decimal strings. It blocks the caller during the HTTP request.
Python `get_deepx_quota_history` returns one validated page of confirmed operations with its
opaque continuation cursor. Empty history is valid; the call blocks during the HTTP request.
The summary and history do not prove aggregation completeness, freshness, claim eligibility, or
any mutation authority. `get_quota_claim_raw(claim_id)` reads one asynchronous claim-task record
by ID, requires its integer ID to match the request, and retains its generic OpenAPI payload
without interpreting backend execution state. Claim
creation and quota purchase remain disabled. `sign_quota_claim` builds the current OpenAPI's exact
three-line EIP-191 personal-sign message and a serializable request payload offline. It normalizes
the wallet to lowercase, preserves a caller-owned retry key of at most 160 bytes, and never posts
the claim. Its signed fields cannot be mutated through the public type, and debug output redacts
the replayable signature, message, and retry key. The 2026-10-11 OpenAPI permits an active enabled
delegate to sign for the wallet; an independent offline vector covers a signing key distinct from
the wallet without asserting live delegate authority. The caller must establish wallet or delegate
authority and durable idempotency before using such a signature in a separate claim workflow; no
claim is enabled by this helper.

The Rust HTTP client also exposes public lending asset identities, exact borrow-rate curves, and
bounded APR and pool-status histories. It validates market/asset scope, curve ordering, requested
time order and bounds, duplicate buckets, and exact nonnegative financial values. Run
`cargo run -p nautilus-deepx --bin deepx-verify-rest-lending-markets` for a credential-free live
check. These raw observations do not establish asset precision or units, directory completeness,
freshness, interest compounding, or framework instrument/event semantics. No lending balance,
yield, or transaction capability is inferred or enabled.
`deepx-verify-ws-lending-rates` subscribes to the public `lending_interest_rate` channel and
returns the first acknowledged batch. It binds each item to the current validated REST asset
directory, requires a UTC minute-aligned statistic bucket, and preserves exact APR decimals and
original JSON. A read-only testnet run on
2026-10-10 observed ETH, SOL, and USDC in market 1. This mutable frame does not establish
completeness, freshness, compounding, or framework yield semantics.
The optional `--reconcile` mode checks each observed one-minute bucket against typed REST history
and reports nothing until every exact APR agrees. A 2026-10-10 run rejected ETH's supply APR:
WebSocket `0.000001212754759968964` versus REST `0.0000012127547599689641` for statistic time
`1791630660000`; borrow APR matched. Both values remain distinct exact decimals, and this check
does not infer a tolerance or authorize framework yields.
A later read-only run on 2026-10-10 matched all three market-1 assets at statistic time
`1791631020000`. Its ETH supply APR was `0.0000012127547870728891` on both channels. The
successful bucket does not explain or invalidate the earlier disagreement.
A live read-only run on 2026-09-16 verified three assets, three rate curves, and three recent USDC
rows from each history endpoint without credentials or transactions.

The WebSocket protocol can attach current authenticated-session and connection-epoch provenance to
an uncorrelated JSON frame without interpreting it as an account, order, or trade event. No DeepX
private business-event conversion is enabled without sanitized fixture evidence.
`deepx-verify-ws-perp-trade-history <wallet-account-id20> <market-id>` checks the read-only
`user_perp_trade_history` subscription acknowledgement against the independently queried wallet
directory. A testnet wallet subscription expanded to all four directory subaccounts for market 3;
no trade data arrived during that bounded observation, consistent with the OpenAPI's incremental
only push behavior. The documented data envelope parses into identity-bound, exact decimal
observations while retaining each original JSON item. The OpenAPI example omits a stable venue
trade ID, so the frame parser alone does not deduplicate or dispatch private executions.
The optional `--observe` mode collects up to 100 incremental batches and 10,000 trade items during
a 30-second window after acknowledgement and prints their original trade items. Exceeding either
limit fails without partial results. An empty window is not evidence of empty history; observed
batches remain unconfirmed and are not dispatched as fills.
The `--reconcile` mode instead accepts at most 20 batches and 100 items, reads up to ten validated
REST pages per nonempty batch over the observed trade times, and checks at most 20 distinct
finalized blocks. It prints matches only after every observed item agrees across WS, REST, and
chain evidence. Missing REST history, a block that is not yet finalized, or duplicate trade IDs
across batches fail the run without partial output. An empty observation still proves nothing
about account history or stream continuity, and this command never dispatches framework fills.
A read-only market-3 observation for wallet `0x781ed35b167068c93dfadab41dfb680edaca4e50`
on 2026-10-09 completed with zero incremental batches. No live private trade frame has been
captured or reconciled with REST and finalized chain evidence.
The same wallet's `--reconcile` run also completed with zero observed batches, so its nonempty
REST/chain path remains covered by local fixtures rather than a live private trade frame.
`deepx-verify-ws-spot-trade-history <wallet-account-id20> <market-name>` selects a Spot market
from the current REST directory, resolves the wallet's subaccounts, and observes each address's
documented `user_spot_trade_history` channel for ten seconds. The parser binds acknowledgement,
market, account, and each typed Spot trade to the selected scope, retains exact decimal values and
original JSON, and rejects incomplete batches. Collection is limited to 20 batches and 100 items
per subaccount. The command reports results only after all addresses complete. A read-only
`ETH/USDC` run for wallet `0x781ed35b167068c93dfadab41dfb680edaca4e50` on 2026-10-09
acknowledged all four subscriptions and received zero incremental trade batches. This does not
establish empty history, replay continuity, finalized execution, or permission to dispatch fills.
The optional `--reconcile` mode permits at most 20 observed batches and 100 items across the wallet,
reads at most ten typed REST pages for each nonempty batch over its trade timestamp range, and
requires a unique exact record match for every item. A duplicate trade ID across batches rejects
the whole run before printing matches. REST account records are mutable and do not echo their
subaccount, so these are candidate matches, not finalized fills. The same wallet's read-only
`--reconcile` run observed zero batches and matched zero trades; live nonempty matching remains
unverified.
`decode_finalized_spot_trade_events` now decodes the node's `SpotMarket.OrderMatched` event from
complete `System.Events` bytes under an approved runtime snapshot, preserving its global event
index, pair, both accounts and order IDs, raw U256 amounts and price, taker, and signed fee assets.
`match_finalized_spot_account_trade` requires exact pair, side, account, order ID, raw base/quote
amounts, six-decimal runtime price, fee sign and asset, and the block/event trade ID suggested by
the REST example.
Metadata-encoded spec-371 tests cover buyer-taker and seller-maker matches and reject changed
financial fields, fee assets, IDs, and incomplete event bytes. This pure match requires separately
verified canonical finality and REST request scope; it does not enable Spot fill dispatch. The
specified wallet's current Spot wallet-history response contains empty `ETH/USDC` and `SOL/USDC`
subaccount groups, so no wallet-owned live Spot trade could be matched to a finalized event.
`observe_finalized_spot_trades_at` now reads a requested finalized block through the validated
Watch RPC endpoint, rejects inconsistent spec-369 historical runtime responses before event reads,
requires fixture-approved historical metadata, compares two complete event storage reads, and
rechecks the canonical block hash before returning decoded matches. The
verifier's `--finalized` mode applies this read to at most 20 distinct blocks after bounded
WS-to-REST matching and prints results only after every candidate agrees with chain evidence.
The 2026-10-09 `ETH/USDC` wallet run acknowledged four subscriptions but observed zero batches
and matched zero trades, so its nonempty three-source path remains covered by synthetic tests,
not a live wallet execution. Neither the observer nor the verifier dispatches framework fills.
`match_perp_trade_history_batch_to_rest` can associate an observed batch with uniquely matching
typed REST account records under the same subaccount request using exact financial values and
timestamps, including inclusive request time bounds. Invalid request scope and missing or
ambiguous records reject the whole batch. This association supplies a
candidate venue ID; mutable REST evidence alone does not prove chain finality or authorize fill
delivery.
`match_perp_trade_history_batch_to_finalized` further requires every matched REST trade to agree
with one supplied decoded finalized chain event, including account, order, market, financial values,
and venue trade ID. One missing or conflicting item rejects the entire batch. The caller must still
verify the REST response's request scope and independently establish canonical finality for the
event blocks. A test synthesizes a WS batch from the two captured wallet REST records at block
`187071212` and reconciles both against independently captured chain events; it is not a captured
private WS frame and does not enable fill dispatch.
Both finalized Spot and Perpetual trade observers reject the known historical runtime inconsistency
before reading event storage, so the affected public RPC cannot provide finalized trade evidence.
A read-only Perpetual verifier run for block `187071212` and wallet
`0x781ed35b167068c93dfadab41dfb680edaca4e50` on 2026-10-09 stopped when historical
`state_call(Core_version)` returned spec 371 instead of the captured spec 369.

The `deepx-capture-runtime-fixtures` binary captures public runtime identity responses after
verifying the expected testnet genesis hash. It does not access account data or submit
transactions. For a new, unapproved deployment, supply both `--expected-genesis <hash>` and
`--output-root </absolute/review/path>` after `--` to inspect a fixture outside the repository;
capture does not approve runtime signing. A finalized spec-376 fixture binds metadata
SHA-256 `57f1bedf1e04522176a2b422af9249d5c990693fb92d11327870d8ef751a8447` to block
`19592700` on genesis `0x87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7`.
Its signed extensions use `CheckNonceEra` where the approved old-chain captures use
`CheckMortality`. The read-only permit accepts this exact identity, and a
public verifier rediscovered three Spot and eight Perpetual instruments after the upgrade. The
signing permit does not accept spec 376. The pinned SDK and Rust agree on complete offline spec-376
`Subaccount.no_op`, `System.remark`, and `PerpMarket.place_order` bytes. Separately, an already
finalized spec-376 `PerpMarket.cancel_order` in block `18476010` recovers its encoded signer only
with spec 376, transaction version 1, and the new genesis hash as both implicit hashes. A read-only
`system_dryRunAt` of the old offline remark vector returned transaction-validity code `0x01000b`
(`IndeterminateImplicit`), so that probe does not establish live acceptance. No new transaction or
durable recovery on spec 376 has been validated.
On spec 376, a read-only Spot data tester observed 15 managed 20-level books, quotes, and trades
without recovery warnings. The public Spot backtest replayed 1,000 ticks and submitted two simulated
orders; the Perpetual backtest submitted eight simulated orders. Neither backtest sent a transaction.
The specification 371 finalized capture records block `195884340`, metadata SHA-256
`4a620bad9f2482b5511f4637d03d5e7907b903f4bab5af439f664963724cda15`, and the canonical block-hash
binding. Its metadata differs from the specification 369 capture only at the encoded runtime
version byte. The checked-in runtime source retains the same signed-extension tuple and transaction
version, and the pinned Python SDK independently reproduced exact `System.remark`, `Subaccount.no_op`,
and `PerpMarket.place_order` extrinsics for specification 371. The signing gate accepts the exact
captured old-chain identities; unknown metadata and future upgrades still fail closed.

Sanitized public REST fixtures under `test_data/http/testnet` include sidecar manifests when a
capture is associated with the independently verified testnet runtime identity. REST responses are
not block-hash-pinned unless a manifest explicitly states otherwise; these fixtures prove only the
wire properties named in their tests and limitations.
