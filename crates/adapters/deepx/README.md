# DeepX adapter

The DeepX adapter is under development and restricted to testnet. No framework trading commands
are enabled. Account support is limited to validated startup state and current-epoch cache replay.

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
Each node notification is a complete Spot book snapshot: L2 subscriptions publish replacement
batches, depth10 publishes the best ten levels, and quotes use the best bid and ask. Price and
amount values are converted from the node's fixed atomic scales and must match the loaded
instrument's precision and increments exactly. The node payload has no exchange timestamp or
sequence, so event and initialization timestamps use local receive time and the adapter assigns a
local monotonically increasing sequence. A disconnect clears the framework book before bounded
reconnection; publication resumes only after a new full snapshot. Spot instruments still require
finalized Watch RPC metadata evidence; book subscription itself does not submit Spot orders.
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
`deepx-verify-live-trades` verifies this framework path using public testnet data only.

`subscribe_mark_prices`, `subscribe_index_prices`, and `subscribe_funding_rates` use dedicated,
acknowledgement-gated public connections for the documented `mark_price`, `oracle_price`, and
`funding_rate` channels. Mark and oracle observations preserve their exact venue decimal scale;
they are not rounded to the tradable order tick. The envelope millisecond timestamp becomes
`ts_event`. Funding updates preserve the exact rate and validate the observed calculation,
funding, and envelope timestamp order, but do not infer an interval or next payment time. These
feeds share the trade subscription's bounded reconnect, initial-data deadline, heartbeat,
idempotency, and synchronous publication fencing. `latest_price` remains raw-only because no
framework mapping has been established. `deepx-verify-ws-public-prices` verifies the raw channels,
and `deepx-verify-live-trades --prices` verifies the framework events and retirement path.

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

`submit_rest_transaction_once` consumes an acknowledged `DeepXPreparedSubmission`, derives
the `transact` market/action labels from its durable operation, and sends the exact signed
bytes once to the primary REST endpoint. It rejects redirects and never uses read retry or failover. Acceptance
requires the backend `tx_hash` to match both the recorded and recomputed Blake2-256 hash.
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
`deepx-verify-rest-wallet-trades <wallet-account-id20> [market-id]` to repeat the credential-free
single-snapshot check. The response does not echo its wallet scope or independently prove
subaccount ownership, and no credentials, pagination, or transactions are used.

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
`get_liquidation_cranker_record_pages_raw` and `get_liquidation_tp_sl_record_pages_raw` collect
bounded wallet- or subaccount-scoped execution history. The common request validates scope,
hashes, time and trigger-block bounds, and cursor progression; record payloads remain raw. Run
`deepx-verify-rest-liquidation-execution-records <cranker|tp-sl> <wallet|subaccount>
<account-id20>` for a credential-free read. These mutable API records do not establish chain
inclusion or finality.
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
is set, also reads history pages, with at most 100 pages of 100 records for each endpoint and
market. Spot canceled-order reports additionally require finalized timestamp evidence at each
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
`get_wallet_account_snapshot` composes the directory, authority-bound profile, exact balances, and
equity and margin-ratio reads for every returned subaccount. It preserves directory order and
returns no partial collection on failure; because REST is not block-pinned, it does not claim
cross-request venue snapshot atomicity.

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

The documented `account/perp/position-orders` projection remains disabled. Active and closed known
position IDs returned HTTP service code 503 stating that the projection was unavailable,
incomplete, or its cursor had expired, so no typed success model or reconciliation behavior is
inferred from that endpoint.

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
revokes the epoch synchronously before dropping those resources.

The framework `ExecutionClient::connect` path now coordinates startup autonomously for the strict
flat-account subset. It prepares the startup epoch, builds the authoritative account state from one
finalized chain checkpoint through `complete_startup_with_finalized_flat_account_state`, completes
durable reconciliation and exact cache registration, and only then enables query tasks. This path
requires a configured PostgreSQL durable transaction store, an active signer-owned subaccount, and
a current acknowledged account WebSocket subscription. The configured wallet address itself is
not a substitute for a subaccount: callers must select an active entry from the wallet directory.
Any partial failure or cancellation clears the whole epoch. Repeated `connect` calls are idempotent
only while the completed epoch remains ready and connected.

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
reviewed DeepX Python SDK blob `cc85676dee70db35bbd996b560938597c3715558` and the captured
spec366/tx1, spec369/tx1, or spec371/tx1 runtime (`--spec-version 371`). It uses
`substrate-interface` and takes a GitHub contents API JSON response for
`src/deepx_sdk/_native_py.py`; no RPC requests leave its offline fixture transport. It checks
System remark, Subaccount no-op, perpetual Limit GTC/IOC, Market IOC, and ordinary perpetual
cancellation, plus a sell-side reduce-only `MustPostOnly` placement on all three snapshots. Spec371
also checks Spot placement/cancellation, perpetual close, profit/loss point management, Subaccount
deletion, and delegate removal. Run the Rust signing tests as well to verify the current
implementation against those bytes. These limited vectors do not prove every operation,
authorization, financial scaling, or live conformance.

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
being estimated. It can replay
the current account state, refresh it from one finalized checkpoint when fully withdrawable and
exposure-free, and asynchronously query one tracked perpetual order after startup. Open exposure or
incomplete evidence leaves the existing account state unchanged.
Autonomous network startup is operational only for the strict finalized flat-account subset.
Framework `submit_order` supports validated perpetual Limit GTC/IOC and Market IOC orders, plus
Spot base-quantity Limit GTC orders, through durable DirectPallet pipelines. Each records the
reservation, signed bytes, submitting transition, hash-verified acceptance, and `OrderSubmitted`
delivery acknowledgement before one-shot transmission. Spot amounts use the cached instrument's
finalized quantity/price increments and currency precision; the runtime requires six-decimal quote
precision for this mapping. Finalized reconciliation alone can authorize a terminal event, and a
Spot acceptance requires its matching indexed `StateOrderBuy` or `StateOrderSell` event. Spot
testnet order validation and fill behavior remain unverified.
Tracked local perpetual and Spot Limit GTC order cancellations use separate durable transaction
keys and `OrderPendingCancel` receipts before one-shot DirectPallet submission. `OrderCanceled` and
`OrderCancelRejected` require matching finalized dispatch and cancellation-event evidence; ambiguous
transmission remains durable for startup reconciliation and is never retried. Spot cancellation
requires an ordinary cancel event; fast cancel, cancel-all, batch cancellation, order lists,
modifications, and private execution streaming remain disabled. Spot testnet cancellation behavior
is not yet verified.
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
collects time pages in order. An explicit request limit caps the total result; an unlimited request
uses at most 100 pages of 5000 rows and fails without partial data if that budget is exhausted.
Each continuation starts one millisecond after the previous page's last candle, and the echoed
market identity must remain stable across pages. OHLCV conversion proceeds only when every value is
exactly representable at instrument precision. Spot uses
the verified `CurrencyPair` precision; live testnet candle volumes currently contain excess
decimal places, so those responses fail closed instead of being quantized. The request limit is
5000 and streaming bar subscriptions remain unsupported. OHLC values and volume must also be exact
multiples of the finalized tick and step sizes.

The raw `get_perp_mark_prices` and `get_perp_oracle_prices` methods remain single-page reads.
Their `*_history_limited` counterparts collect ordered time pages under an explicit page budget;
an explicit request limit caps total observations, while unlimited reads fail rather than return
partial history when the page budget is exhausted. They preserve the response pair identity across
pages and use the same one-millisecond continuation cursor as candle history.

The long-short-ratio history collector follows the endpoint's opaque cursor while retaining its
fixed time bounds. It validates ascending timestamps and market identity, and fails without
returning partial history when its page budget is exhausted. Open-interest history remains a
single-page read because its response does not expose a continuation cursor.

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
generation. Spot `request_trades` uses the verified pair directory, bounded global cursor history,
exact instrument precision, finalized tick/step grid checks, and chain-defined Buyer/Seller taker
roles. Unknown takers, foreign pairs, precision loss, off-grid values, or malformed times reject
the whole response. `deepx-verify-spot-trade-history`
compares live raw and framework ETH/USDC executions.
A framework testnet run on 2026-09-14 verified three chronological ticks over
`1789373271239..=1789373272239`, including the observed latest trade without price/size rounding,
matching request correlation, and successful disconnect.
A public run on 2026-09-14 verified two ETH-USDC trades across two size-one pages in the
inclusive range `1789372702699..=1789372703699`. The live probe also identified and verified the
fix for decimal JSON leverage values such as `25.0`.

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
then enables connection-snapshot `request_instrument` and `request_instruments` responses. It
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
`step_size` for a bytes32 pair at an approved, canonical finalized checkpoint. The provider reads
all directory pairs at one shared checkpoint, converts base-token atomic units with `baseDecimal`,
and publishes the complete Spot/Perpetual catalog only after every conversion succeeds.

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
subaccount. Each successful result is pinned to its own finalized checkpoint; failures are reported
per subaccount after the remaining entries are queried, and any failure leaves the command
unsuccessful. The mutable REST directory does not prove chain-level ownership, and the command does
not construct a framework account snapshot or grant mutation authority. Successful snapshots print
raw lending, perpetual-position, and active-order fields and report whether each account satisfies
the existing exact flat-balance conversion gate.

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
The summary and history do not prove aggregation completeness, freshness, claim eligibility, or
any mutation authority. `get_quota_claim_raw(claim_id)` reads one asynchronous claim-task record
by ID and retains its generic OpenAPI payload without interpreting backend execution state. Claim
creation and quota purchase remain disabled.

The Rust HTTP client also exposes public lending asset identities, exact borrow-rate curves, and
bounded APR and pool-status histories. It validates market/asset scope, curve ordering, requested
time order and bounds, duplicate buckets, and exact nonnegative financial values. Run
`cargo run -p nautilus-deepx --bin deepx-verify-rest-lending-markets` for a credential-free live
check. These raw observations do not establish asset precision or units, directory completeness,
freshness, interest compounding, or framework instrument/event semantics. No lending balance,
yield, or transaction capability is inferred or enabled.
A live read-only run on 2026-09-16 verified three assets, three rate curves, and three recent USDC
rows from each history endpoint without credentials or transactions.

The WebSocket protocol can attach current authenticated-session and connection-epoch provenance to
an uncorrelated JSON frame without interpreting it as an account, order, or trade event. No DeepX
private message schema is enabled without sanitized fixture evidence.

The `deepx-capture-runtime-fixtures` binary captures public runtime identity responses after
verifying the expected testnet genesis hash. It does not access account data or submit
transactions. The specification 371 finalized capture records block `195884340`, metadata SHA-256
`4a620bad9f2482b5511f4637d03d5e7907b903f4bab5af439f664963724cda15`, and the canonical block-hash
binding. Its metadata differs from the specification 369 capture only at the encoded runtime
version byte. The checked-in runtime source retains the same signed-extension tuple and transaction
version, and the live Python SDK independently reproduced exact `System.remark`, `Subaccount.no_op`,
and `PerpMarket.place_order` extrinsics for specification 371. The signing gate therefore accepts
only this exact captured identity; unknown metadata and future upgrades still fail closed.

Sanitized public REST fixtures under `test_data/http/testnet` include sidecar manifests when a
capture is associated with the independently verified testnet runtime identity. REST responses are
not block-hash-pinned unless a manifest explicitly states otherwise; these fixtures prove only the
wire properties named in their tests and limitations.
