# Agent Instructions

Read [AI_POLICY.md](AI_POLICY.md) and [CONTRIBUTING.md](CONTRIBUTING.md) before making changes.
Follow the Rust style in nearby code and the local `rustfmt.toml`.

DeepX transactions can affect real capital. Hold signing, nonce handling, submission, and account
code to a high standard for correctness, reliability, testing, clarity, and maintainability.

- Read affected code and search for existing patterns before editing.
- Keep changes focused and preserve exact arithmetic with integer units or `Decimal`.
- Do not add test-only behavior to production code or weaken tests to obtain a passing result.
- Keep secrets out of source files, command arguments, logs, fixtures, and documentation.
- Verify signed transaction bytes, chain identity, finality, and account ownership before treating
  a live operation as successful. Reconcile uncertain submissions before retrying.
- Do not commit, publish, push, or interact with issues or pull requests unless asked.
- Do not modify generated artifacts by hand.

Before proposing a release, run the format, Clippy, test, and package commands in
`CONTRIBUTING.md`. Verify the package from a fresh standalone checkout.
Publishing itself requires explicit approval.
