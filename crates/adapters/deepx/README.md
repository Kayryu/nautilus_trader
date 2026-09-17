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
canonical-block scanner. `DeepXDurableRecoveryObserver::PerpPlace` additionally verifies exact
retained signed bytes before querying recovery RPC. Execution startup selects it for not-included
perpetual placements. Unresolved evidence still blocks readiness; neither boundary submits or
replays orders. Tests encode synthetic events against captured spec366/spec369 metadata; real
perpetual placement inclusion/business-event captures remain outstanding.

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
verifier also supports the existing initial-submission preparation boundary. No live command
is activated, and an unknown durable outcome never authorizes nonce reuse or a new order.

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
report method resolves only a locally bound client or venue order ID, checks any supplied identity
against immutable local context, resolves the market only from the failure-atomic startup catalog
snapshot, and then performs the bounded configuration-owned REST read. The snapshot retains both
instrument-to-market and market-to-instrument indexes plus perpetual quantity precision and is
cleared on startup reset.
`query_order` applies the same tracked-context and catalog checks to the framework command,
including trader, client, strategy, instrument, client-order, and venue-order identity. It owns the
asynchronous REST lookup in a cancellation-aware task generation and emits the validated report
only while the originating connection epoch remains active. Disconnect, reset, and stop retire the
epoch before canceling its tasks, so a late response cannot enter a later session. External,
unbound, Spot, and identity-conflicting queries remain rejected.

`generate_position_status_reports` reads either one explicitly requested perpetual market or all
markets for the configured subaccount through at most 100 pages of 100 records. It converts only
`Open` lifecycles into net `PositionStatusReport` values, using `isLong`, `baseAssetAmount`,
`entryPrice`, and `updatedAt`. Closed lifecycles are historical and omitted. Unknown statuses,
unloaded markets, inconsistent startup indexes, duplicate open positions for one market, invalid
timestamps, quantity-increment mismatch, and precision loss reject the complete result. Optional
command time bounds filter the current reports by `ts_last`. No flat rows are synthesized, lifecycle
IDs are not exposed as stable venue position IDs, and `provides_bulk_position_coverage` remains false
because the REST snapshot is mutable and does not prove complete product coverage. Multi-order and
mass-status report methods remain non-operational; this does not enable submissions, cancellations,
private execution streaming, or autonomous execution connection startup.
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

These REST and WebSocket observations are mutable and not block-pinned. They do not establish
free/locked balances or margin requirements, initialize framework account state, construct reports,
authorize mutations, or enable trading commands. Execution startup accepts the connection-owned
subscription proof and confirmed balance frame, but framework account-state construction and
network startup coordination remain disconnected until their semantics are independently proven.

`bin/verify_sdk_signing.py` independently checks complete signed regression bytes using the
reviewed DeepX Python SDK blob `cc85676dee70db35bbd996b560938597c3715558` and the captured
spec366/tx1 or spec369/tx1 runtime (`--spec-version 369`). It uses `substrate-interface` and takes a GitHub contents API JSON response
for `src/deepx_sdk/_native_py.py`; no RPC requests leave its offline fixture transport. It
checks System remark, Subaccount no-op, and one perpetual limit-placement vector. Run the Rust
signing tests as well to verify the current implementation against those bytes. These limited
vectors do not prove every operation, authorization, financial scaling, or live conformance.

Execution startup can reconcile durable submitting transactions against exact pending-pool
membership. Pool presence records acceptance; pool absence remains unresolved and does not
authorize a new order or prove non-inclusion. Further pending-pool absence work is not the active
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
transaction store. After finalized runtime and account ownership validation, the execution client
can acquire and retain the PostgreSQL signer lease, restore the complete durable signer record set,
and seed its timestamp nonce allocator. Reset, stop, and disconnect drop that runtime and release
the lease. Startup mass reconciliation uses only this client-owned store and lease; callers cannot
inject a temporary persistence boundary. This initialization does not authorize signing,
submission, or replay, and order commands remain disabled.
Idempotent public HTTP reads support strictly validated bounded retry timing and ordered testnet
endpoint failover. Cursor-based reads fail closed when a response claims another page without a
usable continuation cursor. Bounded raw perpetual trade and funding history support explicit
pagination; other history endpoints remain single-page reads. Execution startup binds the loaded
market catalog to that complete endpoint list. The execution client can produce a tracked
single-order status report, bounded current perpetual-position reports, and bounded perpetual fill
reports from that immutable catalog. It can replay the current account state and asynchronously
query one tracked perpetual order after startup. Network startup, order commands, bulk order
reports, and mass status remain non-operational and fail explicitly.

Perpetual fill reports read at most 100 pages of 100 account trades for the configured subaccount.
They preserve venue trade and order identities, apply optional instrument, venue-order, and exact
nanosecond time filters, and attach a client order ID only when registered context agrees with the
trade market and side. The observed REST `fee` is a signed account-balance delta: current testnet
buyer and seller taker fills are negative, while maker rebates are positive. Reports therefore
negate that value into Nautilus commission, resolve an empty `feeAsset` to the immutable market
quote currency, and reject fee assets or signs that disagree with startup market metadata. This
deployment observation and the chain quote-asset implementation support conversion, but do not
establish a block-pinned history snapshot or completeness under concurrent writes.

The raw perpetual trade, mark-price, and oracle-price candle requests expose exactly the OpenAPI
time frames `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `8h`, `12h`, `1d`, `3d`, `1w`, and
`1M` through a closed Rust enum. Requests remain single-page, ascending, non-TradingView reads with
an explicit limit of at most 5000 records. Read-only `1m` and `3m` probes on 2026-09-15 showed
bucket-open timestamps and included the current incomplete bucket. They also exposed JSON values
such as `2481.3799999999997` for a market with a `0.01` price increment. The raw decoder preserves
those numeric tokens exactly. Framework `request_bars` supports standard, externally aggregated
last-price bars for the same closed interval set. It preserves the venue `time` as `ts_event`
without inferring open- or close-time semantics, verifies pair identity and inclusive request
bounds, and converts OHLCV only when every value is exactly representable at instrument precision.
Backend float noise is never quantized: one lossy row rejects the complete asynchronous response.
The request limit is 5000 and streaming bar subscriptions remain unsupported.

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
generation. Framework Spot history remains unsupported.
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
remain constant across pages. Neither reader infers fee assets, Spot quantity precision, framework
aggressor semantics, or `TradeTick` conversion.

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
and volume responses do not echo the selector. All four remain raw observations without freshness,
bar-completion, window-inclusion, full-depth, or framework event semantics. A read-only `ETH/USDC`
run on 2026-09-17 validated recent candles, last price, one-hour volume, and a 20-by-20 book. Run
`deepx-verify-rest-spot-market <market-name>` to repeat the credential-free check. Spot quantity
precision and framework bar/trade conversion remain unsupported.

`get_spot_markets` now validates the complete public directory, including name/symbol agreement,
bytes32 pair and AccountId20 asset identities, distinct base/quote assets, positive tick and guard
values, and unique case-insensitive names and pairs. `get_spot_market_by_name` and
`get_spot_market_by_pair` bind the returned identity to the exact selector. The live verifier
cross-checks stable ETH/USDC metadata across the directory and both lookups while deliberately
excluding mutable price and nullable 24-hour change observations. The REST tick remains separate
from the raw on-chain Spot `min_order_size` and `step_size`, whose human-unit scaling is unproven;
this does not enable Spot instrument construction.

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
single-page live check. Venue order, taker, side, and fee semantics remain uninterpreted, and no
framework order or fill report is constructed.

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
Its `connect` method loads the public catalog and publishes all convertible perpetual instruments
through the framework data event sender, then enables connection-snapshot `request_instrument`
and `request_instruments` responses. It requires a live data receiver and rejects unsupported
history, filters, foreign identities, and Spot construction. Disconnect, stop, reset, and dispose
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

The typed read-only account snapshot includes wallet/subaccount identity, lending balances,
equity, collateral, and the account-wide margin requirement. The margin-ratio endpoint preserves
exact decimal values and nullable ratio semantics, and rejects negative values. It does not state
whether the requirement uses initial or maintenance weights, so the adapter does not derive a
Nautilus margin balance, free collateral, or account state from it.

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
any mutation authority; claim creation and quota purchase remain disabled.

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
transactions.

Sanitized public REST fixtures under `test_data/http/testnet` include sidecar manifests when a
capture is associated with the independently verified testnet runtime identity. REST responses are
not block-hash-pinned unless a manifest explicitly states otherwise; these fixtures prove only the
wire properties named in their tests and limitations.
