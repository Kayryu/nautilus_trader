# DeepX adapter

DeepX integration adapter for NautilusTrader.

The current implementation provides the Rust protocol and transport foundation for DeepX
perpetual futures on testnet:

- Exact REST and WebSocket wire models for perpetual markets, order books, and public trades
- Conversion into Nautilus `CryptoPerpetual`, `OrderBookDeltas`, and `TradeTick` domain objects
- DeepX v1 public WebSocket order book and trade subscriptions with reconnect replay
- Bounded order book sequence recovery using authoritative REST snapshots
- REST clients for instruments, account state, orders, positions, fills, balance events,
  liquidations, candles, trades, funding rates, and open interest
- Metadata-driven `PerpMarket.place_order` and `PerpMarket.cancel_order` extrinsic encoding
- Native secp256k1 signing with DeepX 20-byte accounts and caller-provided nonces
- Signed extrinsic submission through the DeepX REST API
- A Nautilus data client for perpetual instruments, REST snapshots, order book deltas, and trades

Private account and execution streaming, the execution client, and the execution factory are not
yet implemented. Public streaming is limited to verified testnet perpetual order books and trades.
Spot remains unsupported until its canonical market schema is verified. Runtime call encoding is
resolved from live Substrate metadata, so no pallet or call indexes are hardcoded.

## Testnet WebSocket capture

Use the read-only capture example to inspect and preserve testnet protocol evidence:

```bash
DEEPX_WS_CAPTURE_PATH=/tmp/deepx-ws.json \
DEEPX_WS_CAPTURE_SYMBOL=ETH-USDC \
DEEPX_WS_CAPTURE_SECS=30 \
cargo run -p nautilus-deepx --example capture_testnet_ws
```

The example always connects to DeepX testnet and defaults to the public perpetual order book and
trade channels. Set `DEEPX_WS_CAPTURE_REQUESTS` to a JSON array of request objects when probing a
different read-only subscription shape. Captures recursively redact known credential, wallet, and
account fields before writing, but must still be reviewed before sharing.