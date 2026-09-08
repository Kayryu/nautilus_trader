# DeepX adapter

The DeepX adapter is under development and restricted to testnet. No trading or account
capabilities are currently enabled.

Execution startup can reconcile durable submitting transactions against exact pending-pool
membership. Pool presence records acceptance; pool absence remains unresolved and does not
authorize a new order or prove non-inclusion. Further pending-pool absence work is not the active
milestone; development now proceeds with the Phase E execution client, private event decoding, and
report reconciliation. Any future submission retry must be bounded and retransmit only the exact
durably recorded signed extrinsic. It must never allocate a new order identity or nonce after an
ambiguous outcome.

The Rust client implements the Nautilus `ExecutionClient` framework boundary for identity,
account-state emission, and lifecycle handling. Network startup, order commands, queries, and
reports remain non-operational and fail explicitly.

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