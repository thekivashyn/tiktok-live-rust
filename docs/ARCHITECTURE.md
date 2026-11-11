# Architecture and engineering decisions

Developed by Tri Nguyen. These notes describe the 0.9.0 engine.

## Boundaries

| Module | Responsibility |
| --- | --- |
| `src/transport.rs` | Validate destinations, resolve public rooms, bootstrap an anonymous guest identity, open verified TLS WebSockets |
| `src/engine.rs` | Supervise connection lifetime, deadlines, retries, readiness, event delivery and aggregate reporting |
| `src/decoder.rs` | Route decode work inline or to one owned blocking worker; cancel and join pending work |
| `src/protocol.rs`, `src/protocol/view.rs` | Bound and validate wire frames, gzip and Protobuf envelopes |
| `src/lib.rs`, `src/event_data.rs` | Normalize supported events; maintain finite per-room dedupe/combo state |
| `src/metadata.rs`, `src/metadata/models.rs` | Validate and expose selected sender/like metadata |
| `src/lifecycle.rs` | Bounded ordered reconnect observations with explicit lag, independent of event backpressure |
| `src/main.rs` | Parse CLI arguments and write NDJSON events, health snapshots and aggregate reports |

The `Source` trait separates transport from supervision. Offline tests substitute
local sources and controlled sockets without contacting TikTok.

## Keep the socket responsive

Recognized uncompressed frames at most 4 KiB can decode inline. The decoder yields
after 32 inline frames. Gzip and larger frames run on a blocking worker, keeping
decompression off the async executor. A session owns at most one in-flight job.
Small compressed inputs still use the worker: compressed wire size is not a bound
on the work needed to expand it.

Batch views retain a shared byte buffer and borrow envelope payloads. The complete
batch is validated before any events are exposed. This reduces intermediate
payload copies without accepting malformed later envelopes after partial output.

## Bounded state makes uncertainty explicit

The CLI queue holds 256 events. Per-room state holds at most 8,192 message IDs and
4,096 gift-combo keys. Reconnects to the same room retain state; room changes reset
it. These windows bound memory, but cannot provide unlimited deduplication.

Gift updates can carry cumulative counts. After combo-window eviction, an unseen
count above one may represent an old combo; the processor emits an unknown delta
and establishes a baseline rather than counting the whole total again.

Optional metadata is checked before typed allocation. A metadata failure preserves
the core event with `metadata_incomplete: true`; invalid core payloads are rejected
and counted. Unknown protocol fields remain ignored.

## Readiness, backpressure and shutdown

An open socket or heartbeat alone does not establish readiness. Health separates
opening a socket, waiting for webcast batches, receiving, retry waits and stopping.
Watch snapshots coalesce, so a slow observer does not accumulate history.

An optional lifecycle broadcast ring retains 64 transitions and reports lag to a
slow observer. It emits socket-open, readiness, session interruption, retry and
terminal observations without blocking the supervisor. This makes a reconnect
observable before another event arrives. Existing watch snapshots and event
queues preserve their contracts. See [ordering and overflow](RECONNECTS.md).

Event enqueueing has a shared per-batch grace deadline. A blocked/closed consumer
is an explicit failure. The engine does not silently discard output to keep running.

Cancellation interrupts connection/backoff/session work. Normal shutdown signals
and joins the decoder and attempts a bounded socket close. CLI writer threads have
drain deadlines. A host must cancel and await the engine rather than abandon its
future if it requires joined cleanup.

## A concrete reliability lesson

In 0.7.2, a watchdog race was fixed by checking one immediately-ready socket read
before declaring an expired health timeout. A delayed receiver could previously
time out despite valid input already waiting. Only one read is allowed for an
unchanged expired deadline, preventing irrelevant traffic from postponing timeouts
indefinitely. This is a bounded readiness check, not an unlimited drain.

The [changelog](../CHANGELOG.md) preserves the controlled regression and the later
LIVE run that still found decode errors. Keeping that failed result visible
separates the watchdog correction from subsequent event-validation fixes.

## Scope of evidence

Synthetic oracles establish agreement for supplied bytes. Loopback benchmarks
exercise a particular local path. Real LIVE observations describe received samples.
None establishes every source action delivered, all-region connectivity, complete
replay or 24/7 stability. See [historical notes](ENGINE_NOTES.md) and the
[benchmark methodology](../benchmarks/connector/README.md).
