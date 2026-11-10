# tiktok-live-rust

**A Rust library and CLI for receiving TikTok LIVE events over a direct guest connection.**

Built by **Tri Nguyen**. Version **0.8.0** · Rust 2024 · MIT

Turn a public LIVE room into typed Rust events or a newline-delimited JSON stream
for overlays, interactive games, dashboards and local tools. The engine handles
connection supervision, bounded decoding, event normalization and backpressure;
your application handles what happens next.

The runtime needs no TikTok account-session input, API key, external signing
service, Node.js or browser. It obtains an anonymous guest cookie from TikTok.
Internet access is required. This is an unofficial, experimental client: the
guest route can change or be refused, and the 0.x API may change.

## Why I built it

I wanted a standalone LIVE event engine that could be embedded in a Rust
application or launched from another language, with explicit limits on memory,
retries and shutdown. The design focuses on keeping network reception responsive
while making delivery uncertainty and consumer failures visible.

## What it does

- Receives **chat, gift, like, follow, share, social, join, viewers and stream_end** events.
- Exposes optional sender profiles and detailed like metadata from received payloads.
- Reconnects with backoff, jitter and server-directed cooldowns; reports connection epochs and possible gaps.
- Keeps finite message-deduplication and gift-combo windows across reconnects to the same room.
- Uses bounded gzip/Protobuf decoding and a bounded CLI event queue.
- Exposes coalesced connection-health snapshots separately from the event stream.
- Stops explicitly when the consumer cannot keep up or closes its channel.
- Supports cancellation and joined decoder cleanup.

## Quick start

Install a current stable Rust toolchain with Cargo. On Windows, use the linker
tools required by your Rust toolchain. Linux builds also need OpenSSL development
headers and `pkg-config` because this project uses native TLS.

Run these commands from the repository root, replacing `USERNAME` with a public
channel that is currently LIVE:

```sh
cargo build --release --locked
cargo run --release --locked -- --user USERNAME --seconds 60 --events --status
```

This opts into live event output on stdout and health/report records on stderr.
Stop early with Ctrl+C. To get only an aggregate report:

```sh
cargo run --release --locked -- --user USERNAME --seconds 60
```

For an offline first look, build and run `--help`, `--version`, or the tests:

```sh
cargo run --locked -- --help
cargo test --all-targets --locked
```

Development has been validated on Windows. Linux/macOS compatibility remains
unverified locally; the included CI workflow is intended to check all three.

## Event output

Each stdout line under `--events` is one JSON object. This is a **synthetic** chat
example; optional sender metadata has been omitted:

```json
{"sequence":1,"connection_epoch":1,"kind":"chat","room_id":"1234567890123456789","message_id":"9876543210987654321","user_id":"1111111111111111111","data":{"text":"Hello from the demo!"}}
```

Large identifiers are decimal strings. Optional `user` and `like_details` fields
appear when available. A changed `connection_epoch` means there may be missing
source events. `sequence` describes local processing order, not complete TikTok
delivery. A gift's `quantity_delta: null` means **unknown**; preserve that meaning
when computing totals.

See the [CLI and event reference](docs/USAGE.md) for options, output contracts,
health states, gift accounting and resource limits.

## Use it in an application

**Rust:** add the local engine as a path dependency, then use `engine::run` or
`engine::run_with_status`, `Config`, `Target`, `DirectSource` and a bounded event
channel. The complete [consumer example](examples/consume.rs) handles events,
health and cancellation concurrently:

```sh
cargo run --locked --example consume -- USERNAME
```

**Other languages:** launch the CLI as a child process using an argument array,
consume NDJSON from stdout, drain stderr concurrently, and handle its exit code.
There is no C ABI or language-specific SDK yet. The package is not published on
crates.io (`publish = false`); pin a commit when using a Git dependency.

## How it works

```text
Public username / room ID
           |
TikTok room lookup + anonymous guest bootstrap
           |
TLS WebSocket / room entry / heartbeat / ACK
           |
Bounded gzip + Protobuf decoder
           |
Per-room normalization / dedupe / gift-combo state
           |
Bounded event channel ------> Rust consumer / NDJSON stdout

Connection supervisor -----> Coalesced health snapshots / final report
```

The [architecture notes](docs/ARCHITECTURE.md) explain the boundaries and the
tradeoffs behind small-frame decoding, bounded state and shutdown.

## Validation and benchmarks

The offline suite covers decoding, metadata limits, reconnects, cancellation,
slow consumers, deduplication and gift accounting without contacting TikTok.

Historical 0.8.0 validation recorded 90 passing tests and a separate ten-minute
LIVE observation with 990 events, zero decode errors and three forced recoveries.
Those are finite observations, not an uptime or source-completeness guarantee.

The [matched benchmark harness](benchmarks/connector/README.md) compares the same
synthetic wire input with a pinned Node connector plus an application adapter.
Node is needed only for those comparisons. Older benchmark numbers belong to
their recorded versions and workloads; they are not current universal speed claims.

See [version history](CHANGELOG.md), [archived technical notes](docs/ENGINE_NOTES.md)
and the [publication audit](docs/PUBLICATION_AUDIT.md). Raw local reports and LIVE
data are excluded from source exports.

## Limits worth understanding

- This is an unofficial guest transport, with no guarantee of access or stability.
- It receives public LIVE events and sends the protocol control traffic needed
  for reception. It does not post comments, send gifts or manage an account.
- Reconnects cannot replay events that TikTok did not deliver during an outage.
- Finite dedupe/combo windows and process restarts prevent an exactly-once guarantee.
- Profiles are a selected projection, not the full upstream User schema. Missing
  values remain null/default, and avatar URLs can expire.
- Likes may be aggregated or omitted by the source; not every viewer action is observable.
- One run targets one room and lasts at most 24 hours. Durable storage, UI, OBS
  integration, battle and commerce events are outside the current engine scope.

## Contributing

Start with [CONTRIBUTING.md](CONTRIBUTING.md) and the
[development workflow](DEVELOPMENT.md). Useful next contributions include
cross-platform validation, synthetic protocol regressions and integration examples.
See [SECURITY.md](SECURITY.md) for reporting sensitive issues.

## License and credits

Copyright © 2026 Tri Nguyen. Released under the [MIT License](LICENSE).
Protocol and transport provenance, including the retained upstream MIT notice,
are documented in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
Benchmark dependencies retain their own licenses and are not runtime dependencies.

This project is independent and is not affiliated with or endorsed by TikTok.
