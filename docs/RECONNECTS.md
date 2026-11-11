# Reconnects and incomplete consumer state

Developed by Tri Nguyen. Added in v0.9.0 following
[@Rhytera's feedback on X](https://x.com/Rhytera), tracked in
[issue #1](https://github.com/thekivashyn/tiktok-live-rust/issues/1).

## Keep the same subscription

Transient failures retry using the same Rust event sender or CLI stdout stream.
The consumer does not resubscribe during those retries. A terminal stop closes
the run; inspect the final report before deciding whether the host should restart.

An attached consumer can still miss upstream events while disconnected. There is
no replay/backfill service and `gap_recovery` remains false. Contiguous local
event sequences, reconnect success and a new Ready notification do not prove
complete delivery or repair application totals. Process restarts lose local state.

## Observe lifecycle without waiting for another event

```rust
use tiktok_live_rust::{engine, lifecycle};

let (lifecycle, mut notifications) = lifecycle::channel();
let observers = engine::Observers { status: None, lifecycle: Some(lifecycle) };
// Pass observers to engine::run_observed instead of run. Poll the engine,
// application event receiver and notifications concurrently; see consume.rs.
```

Use a fresh lifecycle channel per run. The sender is owned by the engine;
retaining an extra sender clone keeps the receiver open after the run. Existing
`run` and `run_with_status` signatures are unchanged. The
[Rust consumer](../examples/consume.rs) handles both lifecycle and watch snapshots.

| Kind | Meaning |
| --- | --- |
| `connecting` | Starting a connection attempt; epoch still refers to the last successful connection, or zero |
| `connected` | Socket opened and room validated; epoch has advanced, but readiness is not established |
| `ready` | First webcast batch on this connection; not necessarily a supported application event |
| `interrupted` | Session failed; includes its structured reason and updated possible-gap count, before socket cleanup/retry |
| `retry_scheduled` | A transient failure will wait for backoff/Retry-After; cancellation or the run deadline may prevent the attempt |
| `stopped` | Engine supervision ended; includes terminal reason and error, if any |

`connected` is emitted even if no event ever arrives on the new connection.
`previous_epoch` is populated on reconnect, otherwise null. Compare `room_id`
and `room_changes` as well as the epoch: a new room resets per-room processing.
Failed handshakes can schedule retries without an `interrupted` notification:
no validated session existed to interrupt. `possible_gaps` counts interrupted
sessions, never missing upstream events. `observed_at_unix_ms` is a local wall
clock observation, not source latency or a precise upstream outage boundary.
Wall clocks can move; use lifecycle `sequence` for ordering.

## Bounded delivery and ordering

The lifecycle channel is a nonblocking Tokio broadcast ring retaining **64**
notifications. Lifecycle sequence numbers increase within one run. A slow observer
receives `RecvError::Lagged(n)` before the retained records; resume receiving rather
than treating that error as channel closure. `n` counts lost notifications, not
lost source events. A dropped receiver explicitly unsubscribes and does not stop
ingestion. This observer channel is not an acknowledged or durable log.

Keep application views conservatively marked incomplete after an interruption
or lag. The next retained record, health snapshot and final report expose current
epochs/counters; they cannot reconstruct omitted transitions or upstream data.
Only reconcile totals against an authoritative source when the application has one.

Application events and lifecycle records use separate channels. There is no
cross-channel/cross-pipe delivery order: a buffered old event may be read after
its interruption notification. Use each event's room/epoch and sequence rather
than assigning all arriving events to the most recently observed connection.
Lifecycle `events_processed` / `events_enqueued` are local boundary watermarks,
not consumer acknowledgements. A `ready` notification is emitted before processing
its first batch, so its watermarks exclude that batch.

Observation never waits for a slow reader and cannot delay heartbeat/cancellation.
The existing event queue retains its stricter policy: a closed or persistently slow
event consumer is a terminal failure, surfaced by `interrupted` and `stopped`.

## CLI and Python

```sh
cargo run --release --locked -- --user USERNAME --events --status --lifecycle
python examples/consume.py --user USERNAME --seconds 60
```

The opt-in `--lifecycle` writes stderr records independently of `--status`:

```json
{"type":"lifecycle","data":{"sequence":4,"kind":"interrupted","connection_epoch":1,"possible_gaps":1}}
{"type":"lifecycle_lagged","data":{"skipped_notifications":2}}
```

The first line is a shortened illustration, not the full schema. Both flags
enable a tagged final `report`; only `--status` enables coalescing health snapshots.
Stdout's existing events/report contract is unchanged. Stderr has one writer,
which drains retained lifecycle records (including explicit lag and `stopped`)
before writing the final report. A blocked pipe can still hit the three-second
diagnostics drain deadline; use a host deadline and drain both pipes concurrently.
The final aggregate report/exit code is authoritative for output-drain failures:
the lifecycle `stopped` describes the engine before those drains.

The Python example flags `view_may_be_incomplete` after interruption, lag or an
event epoch/room boundary. It does not clear that flag just because reconnect
succeeded. It stores aggregates only, not raw chats or profiles.

## Validation scope

Synthetic socket tests cover reconnect before another event, retained subscription,
room boundaries, terminal errors, retry exhaustion, cancellation, event backpressure,
observer drop and lifecycle overflow. CLI tests check lag serialization, draining
before a ready final report, write failures and coalesced-watch compatibility.
Python tests use real owned subprocesses. These checks do not establish actual
TikTok access or upstream completeness on any operating system.
