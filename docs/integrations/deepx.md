# DeepX

DeepX is a decentralized perpetual futures exchange. This integration currently provides
testnet market discovery and L2 order book snapshots through the DeepX REST API.

## Installation

:::note
No additional installation extras are required. The adapter is implemented in Rust and compiled
into the core `nautilus_trader` package during the build.
:::

## Examples

- [Python examples](https://github.com/nautechsystems/nautilus_trader/tree/develop/examples/live/deepx/)

## Product support

| Product Type      | Data Feed | Trading | Notes                                 |
| ----------------- | --------- | ------- | ------------------------------------- |
| Perpetual Futures | ✓         | -       | Testnet instruments and L2 snapshots. |
| Spot              | -         | -       | _Not currently implemented_.          |
| Options           | -         | -       | _Not currently implemented_.          |

:::warning[Execution is not available]
The adapter does not register a DeepX execution client or execution factory. Authoritative private
schemas for account bootstrap, open orders, fills, positions, and reconnect reconciliation are not
yet integrated. A relay response containing an order ID and transaction hash is correlation
evidence only; it does not prove chain inclusion, acceptance, execution, or finality.
:::

## Environment

The adapter currently supports `DeepXEnvironment.TESTNET`. The default endpoints are:

| Transport           | Endpoint                              |
| ------------------- | ------------------------------------- |
| REST                | `https://rest-api-testnet.deepx.fi`   |
| Public WebSocket    | `wss://ws-api-testnet.deepx.fi/v1/ws` |
| Substrate WebSocket | `wss://rpc-testnet.deepx.fi`          |

The public WebSocket transport and its protocol models are available internally, but the data
client does not yet expose live subscriptions. Use REST instrument and snapshot requests through
the `DataTester` example.

## Symbology

DeepX perpetual instruments use `{Base}-{Quote}-PERP.DEEPX` in Nautilus. For example:

```python
from nautilus_trader.model import InstrumentId

instrument_id = InstrumentId.from_str("ETH-USDC-PERP.DEEPX")
```

The corresponding DeepX REST symbol omits the product suffix and venue, for example `ETH-USDC`.

## Configuration

Configure the data client with the DeepX factory and testnet environment:

```python
from nautilus_trader.adapters.deepx import DEEPX
from nautilus_trader.adapters.deepx import DeepXDataClientConfig
from nautilus_trader.adapters.deepx import DeepXDataClientFactory
from nautilus_trader.adapters.deepx import DeepXEnvironment

builder.add_data_client(
    DEEPX,
    DeepXDataClientFactory(),
    DeepXDataClientConfig(environment=DeepXEnvironment.TESTNET),
)
```

`DeepXDataClientConfig` also accepts optional REST, WebSocket, and proxy URL overrides, transport
timeouts, and an instrument refresh interval.

## Current data capabilities

The data client loads perpetual instruments when it connects and supports these explicit requests:

- All perpetual instruments.
- One perpetual instrument by `InstrumentId`.
- One L2 order book snapshot with optional depth.

Streaming books, quotes, trades, bars, mark prices, index prices, and funding rates are not yet
exposed through the Nautilus data client.

## Execution transaction semantics

The Rust execution primitives can query live runtime market constraints, validate raw limit-order
values, sign Substrate extrinsics, and submit signed place or cancel payloads to the relay. They do
not constitute a Nautilus execution client.

The low-level Rust HTTP client can concurrently collect raw balances, portfolio, current positions,
open orders, order history, and private trades for one subaccount. This schema-neutral bundle is
intended for capturing canonical fixtures while private response models are being integrated. Its
requests are not atomic, and the raw payloads are not account reconciliation or evidence that
execution is ready. The testnet capture helper recursively redacts 20-byte hexadecimal account
addresses before printing the bundle.

To capture a redacted testnet sample without signing or submitting a transaction, set the
subaccount address and explicitly run the ignored test:

```bash
DEEPX_TESTNET_SUBACCOUNT_ADDRESS=0x... \
cargo test -p nautilus-deepx --lib \
    http::client::tests::captures_raw_testnet_account_snapshot_without_signing \
    -- --exact --ignored --nocapture
```

The capture requests at most 500 recent orders and trades. It is suitable for reviewing response
schemas and producing manually inspected fixtures, not for proving complete account history.

Runtime validation uses exact integer arithmetic for minimum quantity, tick size, step size, and
minimum notional. The runtime's `base_decimal` scales base quantity. No universal raw price scale is
assumed.

Once relay transmission begins, errors may be ambiguous. The execution coordinator keeps these
attempts in `ActionRequired`; a successful relay response remains `Submitting` until an
authoritative private source confirms the outcome. Applications must not interpret either state as
an accepted, filled, canceled, or rejected order.
