# DeepX SDK Documentation

See the [SDK README](../README.md) for installation and a minimal REST example.

- [Protocols and signing](protocols-and-signing.md): REST semantics, quota claim methods, offline
  signing, runtime snapshots and permits, and Spot modification decoding.
- [Account examples](account-examples.md): account queries, quota purchases and claims, delegates,
  leverage, PnL settlement, onboarding, and no-op replacement.
- [Order examples](order-examples.md): exact raw amounts, timestamp nonces, GTC and IOC orders,
  cancellation recovery, atomic modification, and asynchronous and synchronous order tickets.
- [EVM bridge](bridge-evm.md): configuration, preview and submission, and destination matching limits.
- [Live verification](live-verification.md): all REST, account, WebSocket, and RPC verification modes
  and their endpoint and finality caveats.

Signing does not submit a transaction, and a transaction hash is not proof of inclusion or finality.
Validate runtime identity and account authority, manage nonces and persistence, and reconcile
uncertain submissions before retrying. Follow each example's cleanup requirements.

Development and package checks are in [CONTRIBUTING.md](../CONTRIBUTING.md).
The SDK includes the [LGPL-3.0-only license](../LICENSE).
