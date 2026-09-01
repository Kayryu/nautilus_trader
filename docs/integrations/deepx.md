# DeepX

DeepX is a decentralized exchange protocol with spot, perpetual, lending, account-management,
quota, delegate, and bridge surfaces. The planned NautilusTrader integration is restricted to
DeepX testnet and is not yet available for use.

The adapter remains disabled until captured protocol evidence proves each capability. A published
SDK, permissive schema, successful submission response, or inferred behavior is not sufficient
evidence on its own.

## Implementation status

The Rust adapter crate and workspace wiring now exist. The following protocol-core foundations are
implemented and covered by unit tests:

- Testnet-only deployment constants, URL resolution, and strict network configuration. Mainnet and
  unknown environments are rejected before endpoint overrides are applied. Endpoint chain-identity
  validation is not yet implemented outside the capture tool.
- Forward-compatible environment and product enums.
- Product-aware Spot and perpetual symbol parsing and formatting.
- Exact checked conversion between scaled integers and `Decimal`, without floating point.
- Read-only runtime capture tooling which pins state queries to one finalized block, plus immutable
  best-head and finalized fixture sets for the deployment identity below.
- Structured SCALE V14 metadata decoding which validates the metadata prefix and extracts the
  declared signed-extension order; future finalized captures record that order in their manifest.
- An unauthenticated, read-only JSON HTTP transport built on the shared Nautilus HTTP client, with
  strict relative-path validation, typed failures, bounded retries for transient reads, and
  failover across explicitly configured endpoints.
- Typed public Spot and perpetual market-list reads with venue-envelope validation and exact
  `Decimal` parsing for JSON number and string representations. Response models tolerate unknown
  fields while requiring the observed fields used by the protocol boundary.
- Strict perpetual market conversion into `CryptoPerpetual`, preserving the deployment market ID,
  protocol addresses, exact increments, minimum quantity and notional, margins, and fees. The full
  instrument provider remains disabled because Spot metadata has no verified order quantity
  increment.
- Typed single-page perpetual funding-rate, long-short ratio, open-interest, raw trade, raw candle,
  raw mark-price, and raw oracle-price history reads, plus raw perpetual volume statistics, with
  synchronous parameter validation and exact financial-value parsing. They preserve venue response
  order and do not emit Nautilus data events.
- A failure-atomic public market catalog which loads Spot and perpetual metadata concurrently,
  preserves deployment-provided bytes32 pair and numeric market IDs, and indexes entries by
  canonical product-aware Nautilus identities. It is not an `InstrumentProvider`.
- Defensive cursor-pagination state which enforces a local page budget and rejects empty-page
  continuation and repeated cursors without assuming endpoint-specific cursor semantics.
- Transport-neutral WebSocket protocol state with monotonic request correlation, connection-epoch
  ownership, stale send/response isolation, shared authentication tracking, and desired-versus-
  confirmed subscription intent across reconnect resets.
- Single-decode WebSocket text-frame ingress which correlates only unsigned numeric top-level
  request IDs, preserves valid unknown JSON, and returns typed errors for malformed JSON.
- Owned WebSocket task lifecycle with generation-specific cancellation, bounded graceful shutdown,
  forced abort followed by join, and rejection of overlapping handler generations.
- A schema-neutral single-owner WebSocket command loop which serializes request registration,
  matching send-failure or caller-timeout cleanup, bounded response waits, one-decode inbound
  correlation, and connection-epoch resets through a fixed-capacity command queue.
- A zeroizing, redacted secp256k1 private-key boundary with typed validation and testnet environment
  resolution.

These foundations do not make the adapter operational. Apart from the two public market-list reads,
single-page perpetual funding-rate, long-short ratio, and open-interest history primitives, one
descending page of raw perpetual trades, one ascending page of raw one-minute perpetual candles,
mark-price history, and oracle-price history, and one raw perpetual volume-statistics window, no
other endpoint-specific HTTP API, live WebSocket transport or channel, instrument provider, market
data client, account client, signer, execution client, management service, PyO3 binding, or Python
package is enabled. Authoritative venue rate-limit policy, automatic endpoint pagination, and other
business response schemas remain unimplemented. Possessing or loading a private key does not enable
signing or transaction submission.

## Plan progress

The current Git changes add the `nautilus-deepx` crate, register it in the Rust workspace and
adapter test inventory, update the lockfile, add runtime fixture capture and protocol-core code,
and add this capability document. The implementation maps to the integration plan as follows:

- **Phase A - Partial:** Capability matrix, hard gates, runtime capture tool, and
  runtime-identity fixtures exist.
- **Phase B - Partial:** Crate and workspace wiring, common types, credentials, HTTP read
  transport, pagination, and transport-neutral WebSocket state exist.
- **Phase C - Partial:** Typed public Spot and perpetual market-list reads, a read-only metadata
  catalog, strict perpetual `CryptoPerpetual` conversion, typed single-page perpetual funding-rate,
  long-short ratio, and open-interest history reads, a typed single-page raw perpetual trades read,
  a typed single-page raw one-minute perpetual candle, mark-price history, and oracle-price history
  read, and a typed raw perpetual volume-statistics read exist. No Nautilus instrument provider,
  framework historical request handling, data client, live public stream, or order-book recovery
  exists.
- **Phase D - Not started:** No runtime snapshot service, signer, nonce owner, transaction tracker,
  or recovery scanner exists.
- **Phase E - Not started:** No execution client, account initialization, order commands, reports,
  or reconciliation exists.
- **Phase F - Not started:** No lending, subaccount, delegate, quota, or bridge service client
  exists.
- **Phase G - Partial:** This document exists; configs, factories, PyO3/Python wiring, discovery
  pages, and examples are absent.
- **Phase H - Not started:** No controlled conformance, benchmarks, fuzz campaigns, or full
  review-readiness run has been recorded.

Within Phase A, the external maintainer-approval and competing-work checks remain unresolved.
Fixture collection currently covers deployment/runtime identity only; market metadata parsers and
the catalog and perpetual instrument conversion are covered by sanitized mock responses rather
than runtime-tagged protocol evidence. The Spot market-list response does not expose a verified
order quantity increment, so Spot instrument conversion and the complete `InstrumentProvider`
remain disabled. No fixtures cover complete REST pages, WebSocket messages, transactions, reconnects,
reorganizations, pagination, or management operations. Within Phase B, the repository wiring and
local protocol primitives are implemented, but the planned crate skeleton is incomplete because
signing, integration-test, benchmark, fuzz, and example directories do not exist. HTTP support is
limited to unauthenticated idempotent JSON reads, including typed Spot and perpetual market lists,
one page each of perpetual funding-rate, long-short ratio, and open-interest history, one descending
page of raw perpetual trades, one ascending page of raw one-minute perpetual candles and mark-price
and oracle-price history, and one raw perpetual volume-statistics window. WebSocket support stops
before transport connection, venue messages, heartbeat, authentication, subscriptions, and channel
routing.

Unit and mock tests cover the implemented common, metadata, HTTP, pagination, WebSocket protocol,
handler, and task-lifecycle code. These tests establish local invariants only; they do not satisfy
the fixture, live testnet, signing-vector, client-conformance, Python-boundary, benchmark, or fuzz
requirements from later milestones.

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
| Protocol core         | Rust types, fixtures, HTTP/WS protocol state      | Partial        | Typed public reads; no live WebSocket channel.           |
| Mainnet               | None                                             | Unsupported    | No validated deployment or protocol evidence is present. |
| Spot                  | Nautilus data and execution clients              | Planned        | Requires verified asset, market, and trading schemas.    |
| Perpetual futures     | Nautilus data and execution clients              | Planned        | Requires verified market, account, and trading schemas.  |
| Lending               | Separate Rust and PyO3 service client            | Planned        | Not represented as Nautilus order operations.            |
| Subaccount management | Separate Rust and PyO3 service client            | Planned        | Requires verified ownership and authorization behavior.  |
| Delegates             | Separate Rust and PyO3 service client            | Planned        | Requires verified mode, expiry, and wallet-wide effects. |
| Quota                 | Separate Rust and PyO3 service client            | Planned        | Claim and on-chain purchase remain distinct operations.  |
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
Substrate defaults. DeepX's Subxt fork can independently retrieve and decode metadata, but the
adapter capture path remains block-hash-pinned and uses the smaller `frame-metadata` decoder. This
evidence resolves extension ordering only and does not enable signing.

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
and cannot fail over unless an operator explicitly supplies another candidate. No DeepX request
quota is configured until authoritative rate-limit semantics are captured.

The pagination state treats a missing or empty cursor as completion, rejects a continuation cursor
on an empty page, rejects repeated cursors, and stops before a request could exceed its configured
page budget. It deliberately does not define cursor direction, inclusive boundaries, stable row
identity, deduplication, completeness, or freshness; each endpoint must prove those properties from
captured fixtures before using this state. The OpenAPI entries for `/health`, `/live`, and `/ready`
currently describe only successful `200` responses and provide no response schema, so they are not
exposed as typed endpoint methods.

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
exposes no automatic pagination and emits no Nautilus funding events.

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
therefore exposes no automatic pagination or Nautilus ratio event.

The typed perpetual open-interest primitive calls
`GET /internal/v1/market/perp/open_interest` with a deployment market ID, millisecond bounds, and an
optional positive limit. It fixes the verified request time frame to `1m` and order to `ASC`,
preserves total open interest and the long-to-short ratio as exact decimal values, and leaves the
venue response order unchanged. Mock tests prove typed query encoding and exact response decoding.
The units of total open interest, aggregation rules, boundary inclusion, completeness, freshness,
and conversion to Nautilus data remain unproven, so no framework open-interest event is emitted.

The typed raw perpetual trades primitive calls `GET /internal/v1/market/perp/trades` with a
deployment market ID, an optional positive page size, and an optional opaque cursor. It fixes the
only verified request order to `DESC`, preserves trade price, quantity, and fees from their original
JSON number tokens as exact decimal values, and returns venue item order, `hasNext`, and `nextCursor`
without interpretation. It also preserves `createdAt`, `filledDirection`, and `taker` as raw strings.
A read-only testnet probe on 2026-09-01 confirmed successful pages selected by either market ID or
market name, while an `ASC` request returned venue failure code `10012`; the adapter therefore
exposes only the verified market-ID and descending-order request. Mock tests prove typed query
encoding and exact high-precision response decoding. No sanitized runtime fixture or multi-page
capture proves timestamp semantics, cursor direction or stability, boundary overlap, deduplication,
completeness, or freshness. Fill direction and taker role have not been mapped to Nautilus side or
aggressor semantics. The adapter exposes no automatic pagination and emits no Nautilus trade event.

The typed raw perpetual candles primitive calls `GET /internal/v1/market/perp/candles` with a
deployment market ID, millisecond bounds, and an optional limit constrained to the documented
`1..=5000` range. It fixes the only runtime-probed shape to the `1m` interval, `ASC` order, and
`tradeView=false`. It preserves volume and OHLC values from their original JSON number tokens as
exact decimal values and returns the venue pair, bucket timestamps, and response order without
interpretation. A read-only testnet probe on 2026-09-01 returned three records with strictly
increasing timestamps separated by 60 seconds. Mock tests prove typed query encoding, exact
high-precision response decoding, and synchronous limit rejection. No sanitized runtime fixture or
multi-page capture proves boundary inclusion, empty-bucket behavior, completeness, freshness, or
whether the timestamp identifies the bucket open or close. Other documented intervals remain
unexposed until independently probed. The adapter emits no Nautilus bar event.

The typed raw perpetual mark-price primitive calls `GET /internal/v1/market/perp/mark_price` with a
deployment market ID, millisecond bounds, and an optional limit constrained to `1..=5000`. It fixes
the runtime-probed shape to `1m`, `ASC`, and `tradeView=false`, and reuses the exact raw candle wire
shape without treating it as a trade candle. A read-only testnet probe on 2026-09-01 returned three
records with strictly increasing timestamps separated by 60 seconds. Mock tests independently
prove the endpoint path, typed query encoding, exact high-precision OHLCV decoding, and synchronous
limit rejection. The venue labels the payload fields as OHLCV, but the volume meaning, bucket
boundary inclusion, missing-bucket behavior, completeness, freshness, and timestamp identity remain
unproven. Other intervals remain unexposed. The adapter emits no Nautilus mark-price or bar event.

The typed raw perpetual oracle-price primitive calls
`GET /internal/v1/market/perp/oracle_price` with a deployment market ID, millisecond bounds, and an
optional limit constrained to `1..=5000`. It fixes the runtime-probed shape to `1m`, `ASC`, and
`tradeView=false`, and reuses the exact raw candle wire shape without treating it as a trade candle.
A read-only testnet probe on 2026-09-01 returned three records with strictly increasing timestamps
separated by 60 seconds. Mock tests independently prove the endpoint path, typed query encoding,
exact high-precision OHLCV decoding, and synchronous limit rejection. The venue labels the payload
fields as OHLCV, but the volume meaning, bucket boundary inclusion, missing-bucket behavior,
completeness, freshness, and timestamp identity remain unproven. Other intervals remain unexposed.
The adapter emits no Nautilus oracle-price or bar event.

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

The perpetual last-price endpoint is not exposed. A read-only testnet probe on 2026-09-01 confirmed
a successful scalar JSON-number response. The HTTP financial-value parser now preserves original
JSON number lexemes when creating a `Decimal`, but the endpoint supplies no observation timestamp.
Until runtime-tagged evidence establishes observation timing and freshness semantics, the endpoint
remains unsupported.

The WebSocket protocol core registers each request before its send is exposed, resolves responses
strictly by request ID and transport connection epoch, and uses a separate non-wrapping send token
so stale send failures cannot remove a newer registration. Connection replacement drains pending
waiters, invalidates shared authentication state, and returns desired subscriptions for replay via
the shared Nautilus subscription tracker. Each inbound text frame is decoded from JSON once. A
top-level unsigned numeric `id` can be offered to the request registry, while every other valid JSON
shape remains an explicit unknown frame instead of being silently dropped. Malformed JSON returns a
typed error without panicking. A transport-neutral command handler now owns all mutations to this
state. Its handle registers a waiter before a future transport send, cancels only the matching
registration after a send failure, ingests text with an explicit connection epoch, and resets
connection-owned state. A bounded response wait removes only its matching send-token registration
when it times out; a response completed at the timeout boundary wins the race, and any later frame
remains an explicit unknown response instead of reviving the canceled waiter. Owner cancellation or
closure of every command handle drains all remaining waiters with typed cancellation errors. The
single-owner command queue has a fixed capacity and applies asynchronous backpressure when full, so
local callers cannot create an unbounded command backlog. This is local lifecycle control only; it
does not define venue flow control or a DeepX request rate limit.

The internal OpenAPI currently proves only that `GET /internal/v1/ws` is described as the real-time
WebSocket connection endpoint. It does not define the upgrade headers, venue message envelope,
heartbeat, topic delimiter, authentication payload, subscription acknowledgement, or public/private
channel schemas. The adapter now owns cancellation and task handles for one future handler
generation: shutdown first requests cooperative cancellation, then forcibly aborts and still joins
an unresponsive task after a bounded grace period. This prevents detached handler tasks, but does
not create or call a `WebSocketClient`, serialize or send a venue request, establish a connection,
implement a heartbeat or reconnect I/O loop, or route a channel. Those remain disabled until
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

## Product capabilities

| Capability             | Spot    | Perpetual | Evidence gate                                              |
| ---------------------- | ------- | --------- | ---------------------------------------------------------- |
| Instrument definitions | Planned | Planned   | Complete market metadata, precision, limits, margin, fees. |
| Historical candles     | -       | Partial   | One raw 1m ASC page; no framework events or paging.        |
| Historical trades      | -       | Partial   | One descending raw page; no framework events or paging.    |
| Order book snapshots   | Planned | Planned   | Snapshot flags, depth semantics, precision, and freshness. |
| Order book deltas      | Planned | Planned   | Sequence, gap, checksum, buffering, and recovery rules.    |
| Live trades            | Planned | Planned   | Public subscription acknowledgement and event fixtures.    |
| Quotes and ticker      | Planned | Planned   | Field meaning and empty-book behavior.                     |
| Mark and index prices  | -       | Partial   | Raw 1m mark history only; no events or freshness claim.    |
| Funding                | -       | Partial   | Typed single-page history only; no framework events.        |
| Long-short ratio       | -       | Partial   | Typed single-page history only; no framework events.        |
| Open interest          | -       | Partial   | Typed single-page history only; units remain unproven.      |
| Volume statistics      | -       | Partial   | Raw fixed-period window; units and boundaries unproven.     |
| Bars                   | Planned | Planned   | Interval identity and open/close boundary semantics.       |
| Market status          | Planned | Planned   | Status values and unknown-value behavior.                  |
| Lending market status  | Planned | -         | Asset precision and authoritative status evidence.         |

The metadata catalog does not satisfy the instrument-definition gate. Spot responses do not prove
the permitted quantity increment or order limits. Perpetual responses do not yet prove settlement
currency, linear/inverse costing, or contract-multiplier semantics. The adapter therefore does not
construct `CurrencyPair` or `CryptoPerpetual` instruments from these responses.

Unsupported parameters and unknown enum values must return typed errors. The adapter must never
emit an order book assembled from unverified or discontinuous data.

## Order capabilities

| Capability         | Spot    | Perpetual | Evidence gate                                                   |
| ------------------ | ------- | --------- | --------------------------------------------------------------- |
| Market order       | Planned | Planned   | Signed vector, submission, business event, inclusion, finality. |
| Limit GTC          | Planned | Planned   | Signed vector and authoritative lifecycle evidence.             |
| Limit IOC          | Planned | Planned   | Time-in-force and partial-fill behavior.                        |
| Post-only          | Planned | Planned   | Crossing rejection and venue status mapping.                    |
| Reduce-only        | -       | Planned   | Position-side and over-reduction behavior.                      |
| Stop order         | Planned | Planned   | Trigger source, direction, and lifecycle behavior.              |
| Modify             | Planned | Planned   | Atomicity, identity retention, and failure behavior.            |
| Atomic replacement | Planned | Planned   | Old/new order identity and ambiguous-outcome recovery.          |
| Close position     | -       | Planned   | Quantity, side, reduce-only, and residual-position behavior.    |
| Cancel             | Planned | Planned   | Signed vector and terminal event evidence.                      |
| Fast cancel        | Planned | Planned   | Authorization and authoritative success/failure evidence.       |
| Cancel all         | Planned | Planned   | Scope and effects on unrelated strategies or subaccounts.       |
| Batch operations   | Planned | Planned   | Per-item atomicity, result mapping, and partial failure.        |
| No-op replacement  | Planned | Planned   | Same-nonce replacement and transaction-pool behavior.           |

No order capability may emit a rejection after an outcome becomes ambiguous. Recovery must merge
relay, stream, REST, transaction-pool, block, event, and finality evidence without blindly
replaying mutating bytes.

## Account and reports

| Capability              | Current status | Evidence gate                                               |
| ----------------------- | -------------- | ----------------------------------------------------------- |
| Account registration    | Planned        | Private authorization and initial snapshot semantics.       |
| Balances                | Planned        | Asset precision, locked/free meaning, and update ordering.  |
| Portfolio state         | Planned        | Margin and collateral semantics for each product.           |
| Positions               | Planned        | Side, quantity, entry price, realized and unrealized PnL.   |
| Active orders           | Planned        | Stable venue identity and verified pagination.              |
| Order status report     | Planned        | Client and venue identity lookup with deterministic merge.  |
| Fill reports            | Planned        | Stable trade ID, pagination, and reconnect deduplication.   |
| Position reports        | Planned        | Complete product coverage and freshness.                    |
| Mass status             | Planned        | Bounded, complete pagination and preloaded instruments.     |
| External order tracking | Planned        | Account-stream identity and registration behavior.          |
| Startup reconciliation  | Planned        | Restart fixtures and deterministic REST/stream/chain merge. |

The execution client must load all required instruments before reconciliation. Report generation
must not fetch missing instruments dynamically.

## Execution backends

The backend is an explicit configuration choice. The adapter must not switch backends after a
mutation might have been transmitted.

### Direct pallet

The current protocol path is expected to encode dynamic calls against a validated immutable
runtime metadata snapshot and sign them with secp256k1 ECDSA. It remains unsupported until golden
vectors prove the call bytes, signed-extension order, signature payload, complete extrinsic, and
transaction hash for every enabled action.

### Legacy EVM precompile

The legacy path is expected to encode ABI calldata, sign an EVM transaction, wrap it in
`Ethereum.transact`, and sign the outer extrinsic. It remains unsupported until fixtures prove the
precompile address, ABI, chain ID, nonce, gas fields, transaction format, both signatures, wrapper,
and both transaction hashes.

## Transaction evidence

The planned transaction tracker distinguishes these states:

| State              | Meaning                                                              |
| ------------------ | -------------------------------------------------------------------- |
| `created`          | Identity and nonce reservation exist durably.                        |
| `signed`           | Bytes were signed against one immutable runtime snapshot.            |
| `submitting`       | Transmission started and the outcome may become ambiguous.           |
| `accepted`         | A submission node accepted the transaction into its pool.            |
| `in-block-success` | The extrinsic and expected business event succeeded in a best block. |
| `finalized`        | The successful block is canonical and finalized.                     |
| `in-block-failed`  | An authoritative dispatch or expected business event failure exists. |
| `not-included`     | A complete finalized scan and node-pool check prove absence.         |
| `action-required`  | Available recovery evidence is incomplete or conflicting.            |

Pool acceptance is not order acceptance, block inclusion is not business success, and best-block
success is not finality. Events must be matched by block extrinsic index. A mutating timeout after
possible transmission is ambiguous and must not be treated as a venue rejection.

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
- Signed-extension payload semantics, mortality, checkpoint selection, and runtime-upgrade behavior.
- Direct pallet call and event definitions for every action.
- Legacy precompile ABI, transaction envelope, wrapper, and hash semantics.
- Timestamp order-ID allocation, sequential account-index nonce ownership, and chain-time drift.
- Relay acknowledgement meaning and correlation with stream, REST, and chain evidence.
- Canonical inclusion, reorganization, finality, pool eviction, and missed-block recovery.
- Quota idempotency and the exact EIP-191 claim message.
- Delegate ownership, mode, expiry, revocation, and wallet-wide effects.
- Lending precision, status, interest, collateral, and authoritative completion evidence.
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

- The Rust adapter crate contains protocol-core foundations only; the Python package does not yet
  exist.
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
  regression test. New captures populate this field. Signing remains disabled pending the other
  direct-signing evidence gates.
- Maintainer approval and confirmation that no competing issue or pull request exists remain
  external contribution process gates; local implementation does not satisfy them.
- Financial values remain integers or exact decimal values until conversion to Nautilus domain
  types. Floating-point conversion is not permitted.
