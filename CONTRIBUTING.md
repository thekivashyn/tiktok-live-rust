# Contributing

Thanks for helping improve tiktok-live-rust, developed by Tri Nguyen.

Read the [README](README.md), [architecture](docs/ARCHITECTURE.md),
[usage contracts](docs/USAGE.md) and [development workflow](DEVELOPMENT.md).
For a larger change, open an issue describing the concrete problem first.
Small fixes and synthetic regressions can go directly to a pull request.

Use a current stable Rust toolchain. From the repository root:

```sh
cargo fmt --all -- --check
cargo test --all-targets --locked
cargo clippy --all-targets --locked -- -D warnings
cargo build --release --examples --bins --locked
```

On Windows with an isolated development toolchain, `run.ps1` can invoke the same
Cargo commands. A standard Cargo installation does not need that wrapper.
Tests must remain offline; real LIVE probes are opt-in.

## What makes a useful change

- Describe a concrete trigger and the resulting behavior.
- Preserve bounded memory/state, verified TLS, backoff, cancellation and cleanup.
- Keep unknown source values unknown. Do not turn null gift deltas into known totals.
- Add a focused synthetic regression for behavior changes.
- Update `CHANGELOG.md` under `Unreleased`, recording checks actually run and limits.
- Include benchmark methodology when claiming performance changes.

Never attach account cookies, credentials, raw LIVE payloads, real sender profiles
or chat logs. Prefer synthetic fixtures and aggregate diagnostics. Treat received
text and URLs as untrusted input in examples and integrations.

## Good first contribution areas

- Confirm builds and offline tests on Linux/macOS and document required native tools.
- Add small integration examples that consume both child-process output streams.
- Add bounded synthetic cases for new or malformed protocol fields.
- Improve documentation around an observed integration problem.

The Rust library and CLI have no Node runtime requirement. Node packages under
`benchmarks/connector` are optional comparison tools with their own upstream licenses.
Historical scripts that need archived reports or binaries are described in
[scripts/README.md](scripts/README.md).
