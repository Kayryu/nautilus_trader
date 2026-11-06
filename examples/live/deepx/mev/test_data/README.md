# Runtime metadata fixture

`spec380_metadata.json` is the unedited JSON-RPC response captured from
`https://rpc-testnet.deepx.fi` at block
`0x557d5d380277ea3532b9a3c7281c7deea987972e489c6b11b6062d42ce03de58`.
The chain genesis is
`0x87b3c18538a2c969775ffbd81beb3d6d8c9cdb22ffd259915230e61124b0fdd7`.
Runtime spec version is 380 and transaction version is 1. The SHA-256 of the decoded metadata
is `bc3fd548eb72253c522f6b5acb7c756fbc2af5b09c6bff5e3b3beac170648dc6`.

The fixture contains 115,954 SCALE metadata bytes. It contains no credentials, account balances,
signed orders, or simulated trades. Generate it from its source rather than editing it:

```bash
curl --fail --silent --show-error --max-time 45 \
  --header 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"state_getMetadata","params":["0x557d5d380277ea3532b9a3c7281c7deea987972e489c6b11b6062d42ce03de58"]}' \
  https://rpc-testnet.deepx.fi \
  --output examples/live/deepx/mev/test_data/spec380_metadata.json
```

Execution tests use this metadata to encode synthetic placements and matches and exercise the
pair-sharded active order storage. Test data for spec 376 is reused from the local Rust SDK.
