# DeepX adapter

DeepX integration adapter for NautilusTrader.

The current implementation provides the Rust protocol and transport foundation for DeepX
perpetual futures on testnet:

- Exact REST and WebSocket wire models for perpetual markets, order books, and public trades
- Conversion into Nautilus `CryptoPerpetual`, `OrderBookDeltas`, and `TradeTick` domain objects
- DeepX v1 WebSocket subscription request builders and sequence continuity checks
- REST clients for instruments, account state, orders, positions, fills, balance events,
  liquidations, candles, trades, funding rates, and open interest
- Metadata-driven `PerpMarket.place_order` and `PerpMarket.cancel_order` extrinsic encoding
- Native secp256k1 signing with DeepX 20-byte accounts and caller-provided nonces
- Signed extrinsic submission through the DeepX REST API
- A Nautilus data client that serves perpetual instrument definitions and REST order book snapshots

Streaming subscriptions, the execution client, and factories are not yet implemented. Spot remains
unsupported until its canonical market schema is verified. Runtime call encoding is resolved from
live Substrate metadata, so no pallet or call indexes are hardcoded.