# Protocols and Signing

[Documentation index](README.md) · [SDK README](../README.md)

## REST semantics

Typed REST methods decode market and account responses into SDK-owned models with exact decimal
values. Raw methods preserve JSON numeric lexemes for callers that need to inspect or validate
the original payload. Single-page methods perform one request; bounded history and wallet
collectors read multiple pages or endpoints and return complete SDK-owned types. Wallet account
and position collectors check local account consistency, but the underlying responses are not
block-pinned. Callers own retries and response freshness.

## Quota claims

`post_quota_claim_once` submits a previously signed wallet claim once and returns the full HTTP
response. The caller must persist its idempotency key and reconcile uncertain delivery before
submitting again.

`post_quota_claim` decodes the public task-creation result, while `wait_quota_claim` polls the
internal task endpoint to confirmation, failure, or a bounded timeout. See the
[quota example](account-examples.md#quota) for wallet claims and on-chain quota purchases.

## Offline signing

`signing::DeepXOfflinePalletCall` selects typed pallet operations for offline signing. The caller
supplies validated runtime metadata and state to `signing::sign_pallet_call`; the SDK returns
complete extrinsic bytes, hash, signer, and nonce in `DeepXEncodedPalletExtrinsic`. Signing and
transaction submission are separate operations.

`signing::sign_offline_evm_transact_extrinsic` embeds a signed EVM envelope in the runtime's unsigned
wrapper using caller-supplied metadata.

## Runtime snapshots and permitted signing

`signing::RuntimeSnapshot` binds validated metadata, identity, and Subxt client state. Its
`DeepXRuntimeSnapshotService` tracks in-flight permits, blocks new signing during runtime
refresh, and invalidates permits on connection loss, inconsistent evidence, or client retirement.
The caller must report those observations to the service; creating a snapshot or permit does not
prove an endpoint is current.

`signing::sign_permitted_pallet_call` and `signing::sign_permitted_dynamic_pallet_call` require
an active permit before encoding a signed pallet extrinsic. Their opaque result binds the complete
signed bytes and hash to the approved runtime identity.
Named entry points such as `signing::sign_perp_place_order` and `signing::sign_spot_cancel` use the
same permit-bound path with exact runtime argument types.

## Spot modification decoding

`signing::sign_spot_modify_order` encodes a transactional Spot cancel-and-place call with an
explicit replacement order and timestamp nonce. `rpc::spot_modify::decode_spot_modify_inclusion`
binds finalized dispatch to the exact cancellation and replacement events in order; the caller
must still verify the signed extrinsic and canonical block. See the
[order modification workflow](order-examples.md#modify-an-existing-order).

## Submission and finality limits

A returned transaction hash does not establish inclusion or finality. Callers must independently
verify runtime identity, manage nonces and persistence, and reconcile finalized chain events
before treating an order as accepted or filled.
