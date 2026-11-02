# Contributing to DeepX Rust SDK

The SDK is a standalone Rust library. Its public API covers DeepX REST, JSON-RPC, WebSocket
messages, runtime decoding, and offline signing. Changes to live transaction behavior require
focused tests and evidence that affected SDK workflows still work.

Use Rust 1.98 or newer. From this directory, run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
cargo package --allow-dirty
```

`cargo package` verifies the publishable archive. Also run `cargo test --all-targets` from a fresh
standalone checkout to detect accidental dependencies on local files. The package version,
dependency versions, license, and documentation links are declared locally in `Cargo.toml`.

Use `#[rstest]` for synchronous tests and `#[tokio::test]` for asynchronous tests. Keep prices,
quantities, money, fees, and nonces exact. Never put private keys in examples, test data, or logs.
Live testnet submissions need explicit user authorization and must be reconciled by transaction
hash after an uncertain outcome.

Prepare a review-ready change before opening a pull request. Do not publish or change remote state
without explicit authorization.
