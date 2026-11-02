# EVM Bridge

[Documentation index](README.md) · [SDK README](../README.md)

[examples/bridge_evm.rs](../examples/bridge_evm.rs) implements the pinned Python EVM-to-EVM USDC flow.

## Configure the preview

Set verified `BRIDGE_SRC` and `BRIDGE_DST` contract addresses, `BRIDGE_AMOUNT_RAW` in six-decimal
USDC units, and `DEEPX_TESTNET_PRIVATE_KEY`; `BRIDGE_SRC_RPC`, `BRIDGE_DST_RPC`, `SIGN_API_BASE`,
and `BRIDGE_RECIPIENT` may override the testnet defaults. It checks both endpoint chain IDs, token
registration, allowance, fee, and authorizer response before the default read-only preview ends.

## Submit and observe

Explicit `--submit` also requires `BRIDGE_MAX_FEE_PER_GAS_WEI`, `BRIDGE_PRIORITY_FEE_PER_GAS_WEI`,
`BRIDGE_APPROVE_GAS_LIMIT`, and `BRIDGE_OUT_GAS_LIMIT`. It waits for a confirmed approval when
needed, simulates and submits bridge-out, verifies the source receipt, and observes a confirmed
destination `BridgeIn`.

`BRIDGE_EXPECTED_AUTHORIZER` verifies a returned signature against the reported digest;
`BRIDGE_EXPECTED_TX_ID` links a destination event to a known bridge protocol ID. Without that ID,
the destination observation is only a matching recipient, token, and amount candidate. The SDK
does not derive the bridge ID from the source transaction yet.
