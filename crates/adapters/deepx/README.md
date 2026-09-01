# DeepX adapter

The DeepX adapter is under development and restricted to testnet. No trading or account
capabilities are currently enabled.

The `deepx-capture-runtime-fixtures` binary captures public runtime identity responses after
verifying the expected testnet genesis hash. It does not access account data or submit
transactions.

Sanitized public REST fixtures under `test_data/http/testnet` include sidecar manifests when a
capture is associated with the independently verified testnet runtime identity. REST responses are
not block-hash-pinned unless a manifest explicitly states otherwise; these fixtures prove only the
wire properties named in their tests and limitations.