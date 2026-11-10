# Archived engine notes through 0.8.0

This preserves the pre-publication technical narrative. Start with the current
[README](../README.md) and [usage reference](USAGE.md). Version-specific test
counts and measurements below are historical, not new checks. Local `reports/`
evidence and archived executables are excluded from public source exports.
Public test-channel handles and the original machine path have been removed;
measurements and unsuccessful outcomes are preserved.

# tiktok-live-rust — 0.8.0

**Developed by Tri Nguyen**

Package/executable: `tiktok-live-rust`. Rust crate: `tiktok_live_rust`.

Development references: [version history](../CHANGELOG.md),
[development and handoff workflow](../DEVELOPMENT.md), and
[agent instructions](../AGENTS.md). Update the changelog with each engine change;
historical results and failed experiments remain attached to their own versions.

Rust engine and CLI for direct, read-only TikTok LIVE connections on the user's PC.
No Euler client, signing service, account-session input, Node/browser runtime, or hosted
application backend. Internet access to TikTok is required. This is an independent Rust
library and CLI, with no dependency on CreatorHub, Affilio or a UI framework. It remains
a hardened prototype, without a production delivery guarantee.

Flow: public room lookup → anonymous guest cookie → TLS WebSocket → room entry →
heartbeat/ACK → bounded gzip/Protobuf decoder → per-room processor → bounded event queue.

## 0.8.0: sender profiles and like details

Every supported event that carries a sender now has an optional `user` object:
`id`, `username`, `display_name`, `avatar_url`, `verified`, `sec_uid` and
`follow_info`. Existing `user_id`, `data` and event names remain available.
Names/URLs are read from the same event payload; missing source values remain
null. No profile lookup, avatar download or persistent profile cache is added.
Avatar selection uses the first HTTP(S) URL without credentials from the large,
medium, then thumbnail images. Returned URLs may expire; the engine does not
rewrite or refresh them.

Like events additionally expose `like_details`. This covers all like-specific
fields in the pinned Connector 2.5.0 v3 root schema, with this API mapping:

| Connector v3 field | Engine field |
| --- | --- |
| `count`, `total` | `data.count`, `data.total` |
| `user` | `user` (documented profile projection) |
| `common` | `like_details.common` |
| `color`, `icon`, `icons` | Same names under `like_details` |
| `specifiedDisplayText` | `like_details.specified_display_text` |
| `effectCnt`, `likeEffect` | `like_details.effect_cnt`, `like_details.like_effect` |
| `publicAreaMessageCommon` | `like_details.public_area_message_common` |
| `roomMessageHeatLevel` | `like_details.room_message_heat_level` |

Common message metadata, display text, image metadata, effects and public-area
structures are typed. 64-bit numeric metadata is serialized as decimal strings.
Nested user records also use a selected identity/profile wire projection; this
does not add the SDK's entire User object, badges or every TikTok event type.
Unknown/new protocol fields remain ignored. Defaults such as `0`, `""`, `[]`
and null mirror the decoded wire model and do not imply TikTok supplied a value.

Metadata is boxed to keep queue elements small. Before allocating typed metadata,
the engine checks a 32 KiB wire bound, 16 KiB aggregate text budget, 512 fields,
128 nested messages, depth 12 and per-list/string limits. Repeated singular
messages merge according to protobuf rules; merged list sizes are checked too.
If optional metadata fails validation, the core event is retained with
`metadata_incomplete: true`; the report increments `metadata_incomplete_events`.
Successful events omit this flag. Dedupe, gift windows and queue caps are unchanged.

Example consumer paths: `event.user?.username`, `event.user?.display_name`,
`event.user?.avatar_url`, `event.data.count`, `event.data.total`,
`event.like_details?.common?.create_time`. Time fields retain source units.

The shared synthetic corpus covers 38 cases, including all 12 like root fields
and nested metadata. An independent decoder from Connector's pinned v3 package
matches Rust on every selected field. A direct 120-second probe on `a public LIVE channel`
also matched 176 events / 42 likes, with all 121 sender-bearing events supplying
username, display name and avatar. See the `reports/profile-like/README.md` (local evidence)
for the engine soak, coverage, RAM and limitations. Tests retain only aggregate
LIVE diagnostics; raw payloads are passed between local test processes in memory.

The separate **10-minute full-engine test on `a public LIVE channel` passed all checks**:
990 events, including 336 likes; 703/703 sender-bearing events carried username,
display name and avatar, and 336/336 likes had `like_details`. Zero decode errors
or incomplete metadata, three successful forced reconnects (2.437–3.521 seconds
to first valid batch), peak working set 10.105 MiB including the probe. The final
five-minute working-set range was 9.844–10.086 MiB. All 90 tests, Clippy, formatting
and release builds pass. This verifies the received samples; it does not establish
that TikTok delivered every viewer action or that every SDK User field is exposed.

## 0.7.3: identified empty chat and bounded decode diagnostics

The 45-minute real LIVE test on the watchdog-fixed 0.7.2 completed all three
reconnects and delivered 5,327 events, but reported 18 decode/validation errors.
That run is **not an entirely passing event-accuracy test**. Its aggregate report
does not identify the rejected payloads. A five-minute wire-shape probe did not
reproduce them; the following instrumented LIVE run reproduced rejection of an
empty-text chat. This confirms one failure category, not the individual causes
of all 18 earlier rejections.

An empty-text chat now remains a chat event when both its message ID and sender
ID are nonzero. Text stays `""`; the engine does not invent text or claim it is
an emote. The common header can supply the message ID. Empty/default payloads
without both identities are still rejected; size limits, history and deduplication
remain active. Reports count accepted empty-text chats in `empty_chats`.

A shared independent synthetic fixture matches the connector's canonical output
byte-for-byte for missing/empty text with identity, common-header fallback,
duplicates, history and Unicode. The archived 0.7.2 binary fails this fixture.

0.7.3 adds `decode_failures` to the final report and live status, alongside the
existing `decode_errors` total. Eleven fixed counters distinguish frame failures,
empty/oversized/wire-invalid chat and failures in other supported event types.
The counters occupy 88 bytes per structure, retain no input content, and cannot
grow with the number of errors. Error totals, retry rules and buffer limits
remain unchanged. 82 tests and Clippy with warnings denied pass.

Final real LIVE verification on `a public LIVE channel`: **15 minutes, all checks pass**,
1,964 delivered events including 149 chat (12 with empty source text), 325 gift,
711 like, 345 join, 405 viewers, 1 follow, 12 share and 16 social. Zero decode
errors, delivered duplicate IDs, sequence errors, invalid output fields or
unqueued events. All three deliberate connection cuts recovered to valid-batch
readiness in 3.129 / 2.824 / 2.371 seconds, including retry wait and handshake.
Peak working set was 9.961 MiB; the last five minutes ranged 9.809–9.953 MiB
(median 9.926). Private peak was 2.859 MiB. No relay or socket remained active.

This final revision ran 15 minutes **after** the initial 45-minute discovery run;
these are distinct runs, not an uninterrupted hour on the final revision. LIVE
RAM includes the bounded relay and validator. Finite observations and output
invariants cannot establish 24/7 stability or recover events never delivered by
TikTok during outages. Full results, hashes and limitations are in
`reports/watchdog-investigation/verification-v0.7.3.json` (local evidence).

## 0.7.2: watchdog readiness race

An expired watchdog used to win over already-buffered input because the receive
loop gave timeout branches priority. If the engine task was delayed for longer
than a three-second test watchdog, it could close a healthy socket and discard a
gift batch already waiting to be read. The regression is reproducible without
TikTok or changing the machine's network settings.

Before declaring startup, socket-idle or batch-idle timeout, the engine now polls
the socket once without waiting. A ready message goes through the ordinary bounded
decoder and event pipeline. Valid traffic can refresh the appropriate health
deadline. The same expired deadline cannot be probed twice: irrelevant/malformed
frames or heartbeat-only input cannot defer a timeout indefinitely. Overall run
deadline, cancellation, input limits, retry backoff and production timeout defaults
are unchanged. This is a conservative single-frame check, not an unlimited drain
or a guarantee against arbitrary scheduler/OS pauses.

Reports add `watchdog_ready_reads`: immediately ready reads taken at an expired
health deadline. A read can include EOF/error; the counter is not a count of proven
false alarms or recovered events. No persistent buffer or production dependency
was added.

Validation:

- Three alternating Windows process-pause pairs suspend only the harness's own
  receiver for four seconds, with the watchdog still at three seconds. The sender
  keeps running. All three 0.7.1 runs close unnecessarily and miss a queued batch;
  all three 0.7.2 runs stay on one connection and match the independent 32,769-event
  output oracle, including gifts, byte counts and checksum. Peers/tasks clean up.
- Deterministic tests cover buffered normal and first batches after scheduler delay,
  and bounded handling of malformed/unknown/heartbeat-only input at expired deadlines.
  Existing true-idle, reconnect, cancellation and long timer-gap tests still pass.
  The suite now has 79 passing tests.
- A normal three-second-watchdog run on 0.7.1 also passes: 30,000 fresh frames,
  240,001 events, three cuts and one silent stall. A short watchdog alone is not
  sufficient to reproduce the problem. A paused connector control exhibits one
  unnecessary watchdog disconnect in three runs. The original failed soak lacked
  receiver timing/error traces, so its precise historical trigger remains unproven.
- A 0.7.2 three-second-watchdog soak also passes: 60,000 fresh frames / 480,001
  events, six abrupt cuts and three silent stalls, nine successful reconnects.
  The three real silences still report `socket_idle`; buffered-input recovery has
  not disabled the watchdog. Both Rust and the connector adapter match the oracle.

An equal-instrumentation loopback comparison (three alternating pairs, 524,289
events per run) measured 132,726 → 131,713 events/s, peak working set 6.19 → 6.39 MiB
and private peak 2.41 → 2.62 MiB. These noisy finite runs are not evidence of a speed
or RAM improvement; the purpose is correct handling of delayed receive tasks.
Cache and queue capacities remain unchanged.

The `reports/watchdog-investigation/README.md` (local evidence) documents
reproduction, raw before/after evidence and the real LIVE validation.

## 0.7.1: matched comparison and gift-window correction

Testing against `tiktok-live-connector` 2.5.0 exposed a gift-accounting edge case:
after the 4,096-combo window evicted an old group, its next cumulative count could
be counted again in full. After window overflow, an unseen combo first observed
above one now keeps `repeat_count` but emits `quantity_delta: null`. The next known
update resumes incremental counting from the new baseline. A genuinely new combo
first seen above one is also conservatively unknown after overflow. This does not
recover missing historical quantities or guarantee unlimited exactly-once totals.

The cache remains capped at 4,096 groups; only two `u64` counters were added to the
processor. Reports expose `gift_streak_evictions` and `gift_streak_resyncs`. Consumers
must preserve null as unknown when computing totals. No new production dependency
or network behavior was introduced. All 76 tests and Clippy with warnings denied pass.

The [matched benchmark harness](../benchmarks/connector/README.md) feeds the same
synthetic wire bytes to both implementations and verifies canonical output against
an independently generated oracle. The connector uses an application adapter for
the same bounded deduplication, history filtering and gift deltas; the socket harness
also supplies its reconnect/watchdog policy. These are not upstream built-in claims.

Median of three alternating pairs on Windows 10 / i5-13500, Node 26.7.0, release Rust:

| Workload | Rust 0.7.1 events/s | Connector 2.5.0 + adapter events/s |
| --- | ---: | ---: |
| Decode → normalize → JSON/CRC, plain | 1,339,779 | 246,048 |
| Decode → normalize → JSON/CRC, 50% gzip | 797,225 | 99,869 |
| Decode → normalize → JSON/CRC, all gzip | 706,727 | 84,726 |
| Large gzip, roughly 4 KB per chat | 240,979 | 44,754 |
| Full loopback WebSocket receive path, 50% gzip | 120,218 | 75,683 |

The loopback workload emits 524,289 events per run. Its median peak receiver working
set is 6.76 MiB versus 135.70 MiB; median process CPU time is 3.61 versus 8.95 seconds.
This includes TCP/WebSocket, ACK, decode and canonical output; the fixture server is
a separate process. Core-only rates exclude network and async worker scheduling.
Neither measurement predicts remote TikTok delivery latency. Small 0.7.0 → 0.7.1
timing changes vary by workload and host load; this patch is a correctness fix, not
a demonstrated speed optimization.

An additional direct 0.7.0/0.7.1 comparison alternated three pairs of 4,194,304 events
per run. Throughput changes were −1.57% plain, −0.24% mixed and +4.73% gzip. Mixed
peak working set was 10.17 → 10.01 MiB. These finite noisy samples establish neither
a universal speed gain nor a statistical equivalence bound; full runs are retained
in `reports/matched-comparison/patch-comparison-v0.7.1.json` (local evidence).

The final paced soak ran for 22.52 minutes on Rust and 22.89 minutes on Node. Each
received 120,000 fresh frames / 960,001 normalized events, including 240,000 gift
updates totaling 360,000 gifts. All counts, canonical bytes and checksums matched.
Both recovered from 13 abrupt cuts and six silent stalls: 19 reconnects, 20
connections, no decode errors, no unexpected fixture exits and zero peers/tasks
remaining on stop. No Rust output events were left unqueued. The fixture replays
the last frame; these results do not prove recovery of undelivered real LIVE events.

Peak receiver working set was 6.80 MiB Rust versus 131.06 MiB Node. Rust working-set
median moved from 6.46 MiB in minute 2 to 6.64 MiB in the final minute; private bytes
moved from 2.00 to 2.29 MiB. Every final-minute Rust sample held at 6.64 MiB working
set / 2.29 MiB private. The earlier 0.7.0 matched soak peaked at 6.14 MiB, so the
correctness patch is not presented as a measured RAM reduction. Runtime scheduling
and allocator behavior can affect these small absolute differences. Node memory
varied with its runtime; this finite experiment does not establish a leak in either
implementation or guarantee 24/7 stability. The first three-second-watchdog soak
was invalidated by unexpected peer exits on both sides; its cause remains unproven
and its partial logs are retained separately.

Raw runs, output hashes and the final soak summary are recorded in
`reports/matched-comparison/verification-v0.7.1.json` (local evidence).
See the harness documentation for accepted runs, retained failed experiments and
the limits of synthetic testing. Historical 0.7 measurements remain below.

## Engine changes in 0.7

- Frames up to 4 KiB take the inline path only when their recognized payload is not gzip.
  Larger frames and all recognized gzip batches still use the blocking decoder worker.
  The wire-size threshold alone never permits a tiny compressed bomb on the async executor.
  Inline decoding yields after every 32 inline frames so continuous input cannot monopolize
  the runtime. The complete batch is still validated before delivery.
- Each session owns its pending decode handle. Cancellation or the overall deadline signals
  the job, cancels it if still queued, and waits for a running job to finish before closing
  the session. Gzip checks cancellation between 8 KiB output chunks; batch validation checks
  between top-level fields. This bounds work by the existing input/output limits and avoids
  carrying an abandoned decoder allocation into another session. It is cooperative, not a
  hard real-time shutdown guarantee. Dropping the entire run future signals cancellation
  through `Drop`; callers should use `CancellationToken` and await `run` for joined shutdown.
- No persistent large-buffer pool, extra runtime dependency or new network request was
  introduced. The bounded queue, 8,192-ID dedupe window, 4,096-combo state, frame/inflate
  caps, TLS verification, ACK ordering and retry policy remain in force.
- Reports add `decode_inline_frames`, `decode_worker_frames` and
  `decoder_jobs_drained_on_stop`. These count routing decisions and pending jobs drained
  during shutdown, not dropped events. The existing event JSON contract is unchanged.

### 0.7 validation and reproducible measurements

The 0.6 release executables were archived in `reports/baseline-v0.6/` before engine changes.
`examples/bench_engine.rs` was first compiled against 0.6, then against 0.7. Its offline
fixture drives the real supervisor, WebSocket parsing, event normalization, bounded queue
and ACK path. Each fresh frame produces chat, two gift updates, like, follow, share, join
and viewer events; it also includes a duplicate gift and historical chat. It validates
sequences and gift deltas. The comparison scripts verify every per-kind count and cleanup.
No captured TikTok messages or sender identities are stored in these measurements.

~~~powershell
.\run.ps1 -CargoArgs @('test','--all-targets','--locked')
.\run.ps1 -CargoArgs @('clippy','--all-targets','--locked','--','-D','warnings')
.\run.ps1 -CargoArgs @('fmt','--all','--','--check')
.\run.ps1 -CargoArgs @('build','--release','--examples','--bins','--locked')
python scripts/compare_engine.py --baseline reports/baseline-v0.6/bench_engine.exe --candidate target/release/examples/bench_engine.exe --pairs 3 --frames 50000 --output reports/engine-comparison-v0.7.json
python scripts/measure_process.py --timeout 750 --output reports/soak-v0.7.json -- target/release/examples/bench_engine.exe 2000000 mixed 5 10000 600
python scripts/compare_soak.py --baseline reports/soak-baseline-v0.6.json --candidate reports/soak-v0.7.json --output reports/soak-comparison-v0.7.json
~~~

The soak uses 600 seconds of real time, a requested 5 ms inter-frame delay and an abrupt
disconnect before ACK every 10,000 fresh frames. Reconnect replays the last frame to check
dedupe and combo accounting. Both versions use the same fixture and pacing; frame totals
can differ because of scheduling and reconnect backoff. Memory samples are OS working set
and private bytes every 250 ms, including the fixture and runtime. Compare medians from
60–120 seconds with the last 60 seconds after caches warm up. Throughput comparisons use
separate fixed-frame paired runs, not the paced soak. Local load and timing noise still apply.

The suite covers 1,000 connections with 999 reconnects, monotonic sequences and exact gift
delta totals, asserting that the previous socket is gone before a new one opens. It also
covers a pending handshake timeout, a 180-second simulated timer gap, replayed gifts, a
cancelled worker wait and cooperative decompression cancellation. Virtual time tests are
not evidence of a real OS suspend, Wi-Fi switch or a multi-hour wall-clock run.

Results and artifact hashes are recorded in `reports/verification-v0.7.json` (local evidence),
with detailed `reports/engine-comparison-v0.7.json` (local evidence),
`reports/soak-comparison-v0.7.json` (local evidence),
`reports/live-v0.7-process.json` (local evidence) and
`reports/reconnect-v0.7-process.json` (local evidence).
Synthetic send-to-ACK latency is not TikTok delivery latency. Reconnect cannot recover
events never delivered during an outage; bounded dedupe is not an exactly-once guarantee.

### Measured 0.6 → 0.7 results — 2026-11-04

Final fixed-frame benchmark: median of three alternating pairs per mode, 50,000 fresh
frames / 400,001 emitted events per run, release builds on the same Windows machine.

| Measurement | 0.6 | 0.7 |
| --- | ---: | ---: |
| Engine throughput, small plain frames | 200,730 events/s | 448,629 events/s |
| Engine throughput, 50% gzip frames | 115,288 events/s | 153,932 events/s |
| Engine throughput, all gzip frames | 101,090 events/s | 101,718 events/s |
| Plain-frame send-to-ACK p95 upper bound | 71 µs | 22 µs |
| CPU time for the fixed plain-frame workload | 2.031 s | 0.891 s |
| Peak process working set, 600-second mixed soak | 7.84 MiB | 6.92 MiB |
| Private RAM median, final minute of soak | 3.78 MiB | 3.20 MiB |

The main gain is small plain frames; gzip throughput is effectively unchanged in these
runs. A separate codec/normalization/JSON benchmark with 7,340,032 events per run measured
1.194M → 1.179M events/s (−1.3%); it does not exercise the async routing improvement.
Incremental requested processing heap remains about 1.08 MiB for the mixed fixture and
5.08 MiB for large gzip fixtures, with all measured allocations released after state drops.
Initial exploratory results are retained separately; timings vary with host load.

The 0.7 soak emitted 746,145 events through nine forced disconnects / ten connections,
with exact per-kind counts and gift totals, zero decode errors, no unqueued events, and
zero fixture peers remaining after stop. Its private-memory median grew from 2.55 MiB in
minute 2 to 3.20 MiB in the last minute; minutes 8–10 held at 3.20 MiB private / 6.47 MiB
working set. This shows a plateau during this run, not proof of zero leaks or 24/7 stability.
The 0.6 soak emitted 712,393 events through eight cuts; paced throughput is not directly
compared because reconnect backoff and scheduling differ.

All 74 tests, format check, Clippy with warnings denied and the release build passed.
A 60-second public LIVE observation of `a public LIVE channel` received 135 events including gifts,
with matching CLI output and zero decode errors; peak working set was 9.60 MiB. A separate
60-second LIVE probe forced one disconnect, received 133 events across both connections
and passed recovery/status/sequence checks. Most observed LIVE frames used the worker
(74 of 81 in the regular run), so plain-frame gains must not be read as a universal LIVE
speedup. The earlier `a public LIVE channel` attempt was offline and is recorded separately.

## Network behavior and ownership

TikTok can observe connections and request metadata. This client does not provide invisibility,
platform approval or a guarantee that TikTok will accept an unofficial connection. Its scope
is receiving public LIVE events; it sends the room-entry, heartbeat, ACK and WebSocket control
traffic needed for reception. It does not post comments, send gifts or change account data.

Connections use fixed TikTok endpoints with certificate verification, without redirects or
ambient environment proxies. The engine uses a fresh anonymous guest identity and has no
account-session import. A run owns one active connection. Reconnects use bounded backoff and
honor Retry-After; HTTP 429 without a usable hint now waits 60 seconds before another attempt.
Refusals are terminal. Cancellation and the overall duration also interrupt cooldown waits.

Chat output is opt-in and is not stored by the engine. Only explicitly requested aggregate
reports are written. Network error reports omit remote body, URL, cookie and raw error-chain
details. Original upstream credits and the MIT notice remain in THIRD_PARTY_NOTICES.md.
The CLI's --help/--version and report engine/developer fields identify tiktok-live-rust and
Tri Nguyen. Existing report consumers should accept these additional metadata fields.

After the rename and move, all 67 tests, Clippy with warnings denied, format check and the
release build passed in this directory. --version and offline Cargo metadata confirm the
new name, author and manifest path. See
`reports/rename-verification-v0.6.json` (local evidence). Registry dependency
versions are unchanged; the previous executable is archived under reports/pre-rename-v0.6/.

## Lower-memory receive path in 0.6

- The engine now uses `protocol::decode_shared(Bytes)` and `Processor::process_ref`.
  An uncompressed frame shares its existing byte storage; gzip uses one decompressed
  buffer. Methods and payloads are borrowed as `EnvelopeRef` values while processing,
  without building a `Vec<Envelope>` or copying each message payload. Emitted events
  still own their strings so they can safely outlive the receive buffer.
- The complete batch is validated before any event is delivered. Field order, repeated
  scalar/string/bytes fields, unknown fields and UTF-8 follow the
  [Protobuf wire format](https://protobuf.dev/programming-guides/encoding/).
  Unknown envelope groups have a depth limit; the existing top-level group restriction,
  frame, batch, message-count and per-event byte limits remain in force.
- Gzip decoding now processes concatenated members, checks their checksums/trailers and
  rejects trailing junk. Large output buffers grow in 1 MiB steps after the first MiB,
  with requested capacity capped at 8 MiB. An overflow probe uses one stack byte beyond
  the data limit instead of growing the heap buffer to 16 MiB. The engine drops its batch
  storage after processing; the transport or allocator can retain backing memory. There
  is no engine-owned persistent large-buffer pool.
- Numeric gift recipients are stored as integers in combo keys. Other recipient strings
  share one immutable allocation between the state map and eviction queue. Empty,
  noncanonical, overflowing and textual IDs remain distinct; output keeps the original
  string. The 8,192-message dedupe and 4,096-combo windows are unchanged.
- WebSocket initial read allocation is 16 KiB instead of the dependency default of
  128 KiB. The 2 MiB frame/message limits still apply. A test reassembles a 128 KiB message
  from fragments with interleaved pings using the same transport configuration.

This is a receive implementation improvement, not a new server-side TikTok protocol.
Heartbeat, room entry, ACK ordering and reconnect semantics are unchanged. No new runtime
dependencies were added. The original owned `protocol::decode` / `Processor::process`
remain available for compatibility; the engine and current benchmarks use the shared path.
The owned decoder retains its older single-member gzip behavior; use decode_shared for
the current validated receive path.

### Reproducing memory measurements

`examples/bench_memory.rs` counts requested Rust heap allocations in deterministic mixed,
large-chat and unknown-message workloads. The two large batches contain 1,024 messages
with roughly 4 KiB payloads each, compressed on the wire. Prepared input and its shared
ownership metadata are excluded from measurement, as are allocator overhead, native TLS,
stacks and OS pages. This measures incremental processing heap, not total process RAM.
The counting allocator exists only in this benchmark; the production engine keeps its
normal allocator. The benchmark verifies that measured allocations are released.

~~~powershell
.\run.ps1 -CargoArgs @('build','--release','--examples','--bins','--locked')
.\target\release\examples\bench_memory.exe
python scripts/measure_process.py --runs 3 --output reports/perf-current.json -- target/release/examples/bench_pipeline.exe 96
~~~

The 0.5 baseline executables are retained locally in ignored reports/baseline-v0.5/.
The CPU workload uses the same prepared frame bytes and checks the same event and JSON-byte
counts. Input preparation and warm-up are excluded from CPU timing. The allocation-counting
benchmark is not used to claim production throughput.

### Measured changes from 0.5 — 2026-11-04

| Measurement | 0.5 | 0.6 |
| --- | ---: | ---: |
| Large gzip chat batch: incremental heap peak | 12,713,677 B | 5,327,171 B |
| Large gzip unknown-message batch: incremental heap peak | 12,708,454 B | 5,327,171 B |
| Mixed workload: allocation/reallocation calls | 2,031,666 | 1,090,610 |
| Mixed workload: retained processor state and reusable JSON output buffer | 1,032,243 B | 884,787 B |
| Mixed workload: incremental heap peak | 1,264,113 B | 1,134,709 B |
| CPU throughput, median of three sequential paired runs | 1,098,005 events/s | 1,386,494 events/s |
| Synthetic-process resident peak, median | 7.21 MiB | 6.92 MiB |

The large-chat processing heap peak fell about 58%; mixed allocation calls fell 46.3%,
retained state/output storage 14.3%, and mixed processing heap peak 10.2%. These percentages
apply to the named workload and metric, not to total LIVE process RAM. Peak resident RAM
also includes the Rust/native runtime and prepared benchmark inputs.

CPU measurements alternate old/new binaries (three runs each), with 2,752,512 events and
549,429,120 JSON bytes in every run. Median throughput rose 26.3% on this machine. This is
local processing speed, not faster TikTok network delivery. Short microbenchmarks have
run-to-run noise. Heap figures use a separate benchmark with 229,376 mixed events or one
1,024-message large batch; its instrumentation is excluded from the CPU comparison.

Reproduce the paired CPU run with:

~~~powershell
python scripts/compare_performance.py --baseline reports/baseline-v0.5/bench_pipeline.exe --candidate target/release/examples/bench_pipeline.exe --pairs 3 --output reports/performance-comparison-v0.6.json
~~~

Evidence: `reports/heap-baseline-v0.5.json` (local evidence),
`reports/heap-v0.6.json` (local evidence),
`reports/performance-comparison-v0.6.json` (local evidence).

### Release and LIVE checks for 0.6

All 66 deterministic tests pass, as do format check, Clippy with warnings denied and release
builds. Tests include a 100-connection simulation; these are not wall-clock soak tests.
The current locked registry versions have no known advisories in the recorded OSV lookup.

- a public LIVE channel: 60 seconds with events/status enabled, 65 events, zero decode errors,
  9.45 MiB observed peak resident working set and 11 sampled peak threads. Output counts
  match the report. Traffic differs from the historical 0.5 run, so this is not a controlled
  percentage reduction of LIVE process RAM.
- a public LIVE channel: a separate concurrent 60-second probe cuts only its own relay after 20 seconds.
  Two connections became ready and 127 events reached the consumer across both epochs,
  including chat/gift/follow. Zero decode errors; one possible source gap remains reported.
- Raw event content was discarded. The probes do not establish 24/7 uptime, all-region
  compatibility, source-event completeness or recovery of events during disconnection.

See `reports/verification-v0.6.json` (local evidence),
`reports/memory-status-v0.6.json` (local evidence) and
`reports/reconnect-v0.6.json` (local evidence) for final-artifact evidence.

## Connection health and longer outage recovery in 0.5

- `engine::run_with_status` adds a `watch` channel for the latest connection snapshot.
  States distinguish `connecting`, `awaiting_events`, `receiving`, `retry_wait`, `stopping`
  and `stopped`. A socket open or heartbeat alone cannot set `receiving`; a webcast batch
  is required, using the existing startup and idle watchdogs.
- State transitions publish immediately; in-session progress publishes at most once per
  second. The channel retains one snapshot, so a slow observer can skip intermediate
  states without accumulating a backlog or using the event queue. It is a health view,
  not a lossless transition log. Rust callers must clone/drop a watch borrow before await
  or slow work: holding its read lock can prevent the publisher from updating.
- `Config.retry_until_deadline = true` / `--retry-until-deadline` allows transient failures
  to retry until the configured run duration or cancellation. Exponential backoff remains
  capped at 30 seconds plus jitter, and a longer server Retry-After is honored in full.
  A healthy connection still resets backoff. Offline, refusal, malformed protocol and
  consumer failures remain terminal. The default eight-retry budget is unchanged.
- Reports and snapshots include `last_protocol_at_unix_ms`, `last_batch_at_unix_ms` and
  `last_event_at_unix_ms`. These are local observation times over the whole run, not
  TikTok timestamps or end-to-end latency. They remain available through an outage;
  inspect state and connection_epoch before assuming the current connection is ready.
- Optional `--status` emits NDJSON diagnostics on stderr, independently of events on
  stdout. Each record has `type: "status"` or `type: "report"` and a `data` object.
  A dedicated writer releases the watch lock before I/O. A blocked diagnostic pipe keeps
  one pending snapshot and one final report, then has a three-second shutdown deadline.
  The final report is authoritative for output-drain errors; the final health snapshot
  describes the engine before draining its output writers. Drain both pipes in the host.
- No dependencies, event payload changes or disk event storage were added. The existing
  `engine::run` API and CLI output without `--status` retain their behavior. Constructing
  a full Rust Config literal now requires retry_until_deadline; `..Config::default()`
  consumers need no change. Opt-in status adds one writer thread in the CLI.

~~~powershell
# Four-hour bounded session, continuing across transient network outages.
.\target\release\tiktok-live-rust.exe --user USERNAME --seconds 14400 --events --status --retry-until-deadline --report reports/run.json

# Measure status-enabled CLI memory and validate both JSON streams; no raw events saved.
python scripts/measure_process.py --timeout 80 --event-stream --status-stream --output reports/memory-status-v0.5.json -- target/release/tiktok-live-rust.exe --user USERNAME --seconds 60 --events --status --retry-until-deadline
~~~

This version improves recovery control and observability. It does not claim a new CPU
throughput gain over 0.4, permanent availability of TikTok's guest route, or recovery of
events sent during disconnection.

### Release validation — 2026-11-04

- All 56 deterministic tests pass; Clippy with warnings denied, format check and release
  builds pass. The exact 162 registry package versions have no known OSV advisories in
  the recorded lookup. This is not a security audit or absence-of-vulnerabilities claim.
- Final 0.5 CLI, a public LIVE channel, 60 seconds with events and status enabled: 82 events,
  zero decode errors, 9.78 MiB peak resident working set and 10 sampled peak threads.
  Output/report counts match. Memory is an observation for this workload, not a fixed
  ceiling or a controlled performance comparison with 0.4.
- a public LIVE channel, 60-second probe: one forced relay cut after 20 seconds, two ready
  connections and 115 consumed events, including gifts/follow; zero decode errors.
  Observed health follows receiving → retry_wait → connecting → awaiting_events → receiving.
  One possible source-event gap is reported and is not recovered.
- With stderr deliberately unread, the final CLI still delivered all 25 locally emitted
  events and exited in 18.125 seconds for a 15-second run. Exit code 1 and
  consumer_slow/status_output identify the blocked diagnostic output. The watchdog did
  not kill the process. Reproduce explicitly with `python scripts/probe_blocked_status.py USERNAME`.
- These are short LIVE observations, run concurrently in separate processes, not a 24/7
  soak or a many-room capacity claim. Raw event content was consumed and discarded.
  See `reports/verification-v0.5.json` (local evidence),
  `reports/memory-status-v0.5.json` (local evidence),
  `reports/reconnect-v0.5.json` (local evidence) and
  `reports/blocked-status-v0.5.json` (local evidence).

## Performance and burst handling in 0.4

- CLI and single-room examples use Tokio's current-thread runtime. The library uses its
  host's runtime and remains compatible with a multi-thread host. Bounded decompression
  still runs on a blocking worker; the CLI's event writer remains separate from the socket.
- Typed EventData payloads move decoded strings into events and serialize directly, without
  constructing a serde_json::Value object tree for each event. The JSON field names, values,
  nulls and string-valued large integers are preserved. Object key order can change.
- Uncompressed Protobuf batches borrow the frame payload during decoding, eliminating a
  redundant full-payload copy. Unknown message types are skipped before parsing their common
  header. ID fallback parses only the ID, without allocating unused display text.
- A full queue now gets a shared 250 ms grace period per batch. This lets a healthy consumer
  drain a burst larger than the queue on a single-thread runtime. The deadline is not reset
  for every event. A continuously slow consumer still stops explicitly, and cancellation
  accounts for an event waiting to enqueue. Config.output_grace can be set to zero for
  immediate failure; its maximum is one second and it must be shorter than heartbeat.
- Message, decompression, queue, dedupe and gift-state limits remain bounded. No durable
  storage or application framework has been added to the engine.

Rust API change: Event.data is now the public EventData enum rather than serde_json::Value.
Rust consumers can match EventData::Gift { quantity_delta, .. }, or explicitly use
serde_json::to_value(&event.data) at a boundary that needs dynamic JSON. Process-based JSON
consumers continue to use the same event shape.

### Reproducible performance workload

examples/bench_pipeline.rs runs the same deterministic synthetic frames through decoding,
normalization and JSON serialization. Each measured run uses 96 passes, 24,576 batches and
2,752,512 supported events: 128 messages per batch, 50% gzip, a mix of gifts/chat/likes/social/
members/viewers/unknown types. Preparation and one warm-up pass are excluded from timing.
This measures local CPU throughput, not TikTok delivery rate or network latency.

On this Windows machine with 20 logical CPUs, median of three release runs:

| Measurement | 0.3 baseline | 0.4 |
| --- | ---: | ---: |
| Events/second | 818,969 | 1,181,604 |
| Time for 2,752,512 events | 3.361 s | 2.329 s |
| JSON output bytes per run | 549,429,120 | 549,429,120 |
| Synthetic-process working-set peak, median | 7.25 MiB | 7.21 MiB |

This workload improved throughput by about 44%; its process memory was essentially unchanged.
It is not a claim of 44% improvement for every workload or lower Internet latency.
Full samples: reports/perf-baseline-v0.3.json and reports/perf-v0.4.json. Historical 0.3
binaries used for comparison are retained locally under the ignored reports/baseline-v0.3/.

~~~powershell
.\run.ps1 -CargoArgs @('build','--release','--examples','--bins','--locked')
python scripts/measure_process.py --runs 3 --output reports/perf-current.json -- target/release/examples/bench_pipeline.exe 96
python scripts/measure_process.py --timeout 80 --event-stream --output reports/memory-current.json -- target/release/tiktok-live-rust.exe --user USERNAME --seconds 60 --events
~~~

The measurement script is Windows-only, uses OS process counters and polls every 250 ms.
Working-set peaks are reported by Windows; thread and private-memory peaks are sampled.
Raw chat events are consumed and discarded. Private committed memory and resident working
set are different measurements; neither is a bound on every future workload.

### Live resource and reconnect checks — 2026-11-04

- Same public channel a public LIVE channel, separate 60-second runs with NDJSON consumed:
  0.3 working-set peak 10.24 MiB / sampled peak 30 threads; 0.4 peak 9.59 MiB / 10 threads.
  The source traffic differed (122 versus 62 frames), so these are observations, not a
  controlled memory-reduction claim. The 0.4 consumer received all 91 locally emitted
  events and the engine reported zero decode errors.
- a public LIVE channel: the 0.4 release reconnect probe intentionally cut its own relay after
  20 seconds. It opened a second direct connection and consumed 114 events across both
  epochs, including gifts and follows, with zero decode errors. One possible source gap
  was reported; events during that gap were not reconstructed.
- Full evidence: `reports/performance-comparison-v0.4.json` (local evidence),
  `reports/memory-v0.4.json` (local evidence), `reports/reconnect-v0.4.json` (local evidence).
  The 100-connection regression is simulated; the live probe forces one reconnect.
  These checks do not establish 24/7 uptime or end-to-end lossless delivery.

## Connection and event fixes in 0.3

- Abrupt socket EOF without a WebSocket close handshake is a retryable network failure.
  Malformed WebSocket frames and policy refusals remain terminal.
- Retry budget applies to an outage, rather than the lifetime of a long-running session.
  It resets only after at least 30 seconds between valid webcast batches on the recovered
  connection. Opening and immediately losing a socket does not reset the budget.
- A socket must receive a webcast batch within 30 seconds. Pings cannot fake readiness.
  Valid protocol traffic has a 60-second idle deadline; a stream with only heartbeats and
  no event batches has a separate 120-second deadline. Timeouts trigger reconnect.
- Received events are processed before ACK. If the socket dies during ACK, already-received
  gifts are delivered and replay on the next connection is deduplicated.
- Added follow, share, join and viewer snapshots, plus a generic social event for ambiguous
  notifications. Display-key tokens take priority over numeric social actions; moderation
  member actions are not classified as joins. Empty chat/gift/like payloads are rejected.
- Gift events now include gift_type and receiver_id; combo accounting stays isolated by
  sender, gift, group and recipient. Consumers use quantity_delta, not event count.

The [upstream event reference](https://ttlc.docs.isaackogan.com/events/chat-and-audience/)
documents the social display-key distinction and warns that individual like updates can
be omitted by TikTok. Our wire models also retain the MIT reference provenance below.
Decoder support does not imply every event is delivered by the source.

## Existing safeguards from 0.2

- Connection supervisor with finite retry budget, exponential backoff plus random jitter,
  and HTTP Retry-After support. Network failures and selected server errors can retry.
  HTTP refusals, policy closes, malformed-protocol failures and offline channels stop.
- Reconnects retain message deduplication and gift-combo state for the same room.
  Resolving a different room resets that state. Each event has a sequence number and
  connection epoch, allowing a consumer to recognize reconnect boundaries.
- Heartbeat, connect/write/idle deadlines, cancellation during connection/backoff/session,
  bounded socket close, and a finite overall run duration.
- A 256-event CLI queue separates socket processing from stdout. A slow or closed consumer
  stops the engine with an explicit error. Output is never silently dropped to stay running.
  A separate writer thread has a three-second drain deadline.
- Fixed TikTok HTTPS/WSS destinations, strict username/numeric room validation, verified TLS,
  no redirects, no automatic environment proxy, and no login-cookie import or local server.
- Only the anonymous guest cookie is forwarded. The owned cookie string is zeroized on drop;
  HTTP header debug rendering marks it sensitive. Library-internal copies are not covered
  by this zeroization. Reports never include cookies, response bodies or raw network errors.
- HTTP JSON body capped at 1 MiB, WebSocket frame/message at 2 MiB, decompressed batch at
  8 MiB, at most 4,096 messages per batch, and 256 KiB per event payload. Batch wire format
  is checked before allocating repeated Protobuf messages. Text and gift metadata are bounded.
- Decompression runs off the async executor. Repeated malformed frames stop the connection.
  Method-count telemetry has bounded cardinality.
- Aggregate reports are written through a temporary file and atomic replacement.
  Dependency versions are pinned by Cargo.lock.

## Reuse in another application

The package and executable are named `tiktok-live-rust`; Rust imports use
`tiktok_live_rust`. The engine owns the TikTok connection and event normalization.
The consuming application owns UI, persistence and business/game rules.

- Rust: add a path dependency on this directory and use `engine::run`, `Config`, `Target`,
  `transport::DirectSource` and a bounded event channel. See [examples/consume.rs](../examples/consume.rs).
  For health observation, create `engine::status_channel()` and pass its sender to
  `engine::run_with_status`. The example consumes events and watches health concurrently.
- Python, C#, C++, Electron and other process-capable applications: launch the CLI with
  argument arrays, read NDJSON from stdout, drain stderr and handle the exit status.
  Cancellation and child-process supervision belong to the host application.
- There is no C ABI/DLL, language-specific SDK or frozen public API contract yet. Pin the
  engine version when integrating; schema compatibility is not guaranteed across 0.x versions.

This directory can be copied into its own repository. It has its own Cargo manifest and
lockfile and no dependencies on a parent Cargo workspace. With a normal Rust installation,
run `cargo test --locked`, `cargo build --release --locked` or
`cargo run --example consume -- USERNAME` from this directory. The example uses the network
only when explicitly run. Linux/macOS builds have not been verified.

## Build and run on this Windows workspace

run.ps1 can use an installed Cargo toolchain or an isolated `.tools/rust` and Zig toolchain
stored outside the engine. Set `-ToolsRoot` to the directory containing `.tools`, or use
`TIKTOK_LIVE_RUST_TOOLS_ROOT`. A local ignored `.build-tools-root` text file can save that
machine-specific directory; a developer may point it at an existing local tools directory.
The engine has no runtime dependency on that workspace. The dlltool wrapper receives the
Zig path through the build environment, so moving the engine does not break relative paths.
No persistent user PATH changes or new toolchain copies are made.

~~~powershell
.\run.ps1 -CargoArgs @('test','--locked')
.\run.ps1 -CargoArgs @('clippy','--all-targets','--locked','--','-D','warnings')
.\run.ps1 -CargoArgs @('build','--release','--locked')

New-Item -ItemType Directory -Force reports | Out-Null
.\target\release\tiktok-live-rust.exe --user USERNAME --seconds 180 --report reports/run.json
.\target\release\tiktok-live-rust.exe --room ROOM_ID --seconds 180 --events
~~~

Default duration is 60 seconds, configurable from 1 to 86,400. This includes setup and
backoff; socket close can add two seconds and event output drain can add three seconds.
Default reconnect budget is eight per outage; --max-reconnects accepts 0..32.
Alternatively use --retry-until-deadline; these two CLI options are mutually exclusive.
The run still ends at --seconds, capped at 24 hours. --status output drain can add a
further three seconds after event-output drain; it never extends the connection deadline.
Ctrl+C cancels. Offline channels do not trigger an endless wait for the next broadcast.

By default stdout contains only an aggregate JSON report. With --events, stdout contains
one JSON event per line and the report goes to stderr. Chat text and sender IDs appear
only in this opt-in stream; consumers must treat them as untrusted data. No raw events
are persisted by the engine. Large identifiers are JSON strings to preserve precision.
With --status, stderr changes to tagged NDJSON health/report records. Stdout's event
format is unchanged; without --events, the aggregate stdout report is still produced.

An event's sequence increments within one engine run. connection_epoch increments on
successful connection, including reconnects; a new epoch means there may be missing
source events. Contiguous local sequence numbers do not prove complete TikTok delivery.
Room changes are visible in room_id.

reports.events counts processed events, including an event that could not be queued.
events_processed is their total and supplies the per-run sequence. ready_connections counts
connections that received webcast batches; retry_budget_resets counts replenished budgets.
events_enqueued and events_written distinguish enqueue from successful stdout flush;
a flush does not prove downstream processing. events_unqueued counts the enqueue failures
encountered before stopping, not all events subsequently missed. queue_high_watermark
is a sample after enqueue. socket_open means a socket was opened during the run, not that
it remains connected after exit.

Exit is nonzero for terminal errors or an observation period that produced no supported
events. NoEvents means there was no usable-event proof, not necessarily a broken connection.
A stream-end event or explicit cancellation can finish normally without events.

## Validation

The deterministic suite contains 74 tests, using in-memory WebSockets and one loopback
HTTP fixture. It does not contact TikTok. It covers reconnect deduplication/combo accounting,
room changes, retry exhaustion, refusal/rate-limit behavior, handshake/backoff cancellation,
idle connections, slow/closed consumers, policy closes, invalid wire data, gzip expansion,
empty-message allocation floods, common-header message IDs, metadata limits and CLI validation.
The 0.3 regressions also cover abrupt EOF/failed ACK, flapping versus healthy reconnections,
startup readiness, heartbeat-only stalls, follow/share classification, member moderation,
viewer snapshots, empty messages and recipient-specific gift combos.
The 0.4 cases additionally check JSON compatibility/escaping, gzip/plain equivalence,
bounded state after 25,000 distinct gifts, a 2,048-event burst through an eight-event queue,
100 simulated connections (expanded to 1,000 in 0.7), and cancellation/slow-consumer batch deadlines.
The 0.5 cases cover status readiness, heartbeat-only telemetry, final/invalid-input states,
absent and slow observers, 10,000 status updates while diagnostics are blocked, tagged CLI
records, recovery after 40 consecutive failures (about 18 simulated minutes), persistent
retry cancellation, terminal failures and Retry-After extending beyond the run deadline.
Report-file failures stay in structured output, including when stderr is blocked.
The 0.6 cases compare both decoders and normalized events over 32,768 mixed messages, check
that plain payload pointers refer to the original shared buffer, exercise reordered/duplicate
and unknown fields, reject malformed later envelopes before delivery, validate multi-member
gzip and CRC/truncation/overflow cases, check bounded capacity and fragment reassembly,
exercise an arbitrary wire corpus, and preserve recipient identity across compact state keys.
The rename/move follow-up also checks that HTTP 429 with a missing or malformed Retry-After
does not trigger another request before its 60-second default cooldown.
Virtual-clock reconnection tests are not a long wall-clock soak test.

~~~powershell
python scripts/audit_dependencies.py
python scripts/smoke_live.py USERNAME --seconds 180
.\run.ps1 -CargoArgs @('run','--release','--example','reconnect_probe','--','USERNAME')
~~~

The dependency script queries the exact Cargo.lock versions against the
[OSV batch API](https://google.github.io/osv.dev/post-v1-querybatch/), including dependencies
for other platforms, and records the lockfile hash. On 2026-11-04, 162 package versions
returned no known advisories. See reports/dependency-audit.json. This is a known-advisory
lookup, not a source-code audit or proof that the program has no vulnerabilities.

The explicit live smoke script starts the release binary, checks event sequence and report
counts, and discards raw events. A watchdog bounds the subprocess lifetime.
Results are written to reports/live-v0.7.json and reports/live-v0.7-output-check.json.
The reconnect probe opens a real TikTok connection through a bounded in-memory WebSocket
relay, abruptly cuts only that relay after 20 seconds, then verifies fresh connection and
events from both connection epochs during a 60-second run. It does not change the machine's
network settings or save raw events. Like every live probe, it is run explicitly, not by tests.
The 0.5 probe also observes retry/receiving/stopped states and checks the final status count.

### Live validation of 0.3 — 2026-11-04

- a public LIVE channel: a controlled abrupt drop 20 seconds into the relay connection,
  followed by automatic reconnect. During the 60-second run, both connections received
  valid batches and the consumer received all 166 locally emitted events: 17 chats,
  2 gift updates, 1 follow, 75 joins, 45 like updates and 26 viewer snapshots.
  Zero decode errors; one possible delivery gap is correctly reported. See
  `reports/reconnect-v0.3.json` (local evidence).
- a public LIVE channel: a separate 60-second release-CLI observation received 146 events
  (4 chats, 3 gift updates, 1 follow, 71 joins, 40 like updates, 27 viewer snapshots),
  with zero decode errors and output/report counts matching. See
  `reports/live-v0.3-output-check.json` (local evidence).
- Both observations ran concurrently in separate processes. This is a short live test,
  not an all-day or many-room capacity result. Share and stream-end behavior were verified
  with fixtures; neither occurred during these live observations.
- The first attempted channel, a public LIVE channel, was offline. The engine stopped explicitly
  before opening a socket; that unsuccessful probe is retained in reports/reconnect-v0.3-offline.json.

The relay cut tests recovery with fresh direct TikTok lookup/bootstrap/connection. It does
not simulate every DNS/TLS/ISP failure or prove recovery of events sent while disconnected.
The probe uses the Rust library channel, so events_written (CLI stdout flushes) stays zero;
events_consumed in the probe is the actual channel-consumer count.

### Historical 0.2 observations

Observed on 2026-11-04 against public channel a public LIVE channel:

- Initial 0.2 release: 180 seconds, 230 frames, 197 events (26 chats, 2 gift updates,
  169 likes), zero decode errors. All 197 enqueued events reached the test consumer.
  See reports/live-output-check-initial-180s.json. This preceded the final connection-epoch
  field and deadline-during-outage correction.
- Final release: 60 seconds, 76 frames, 77 events (24 chats, 53 likes), zero decode
  errors. All 77 events reached the consumer with contiguous sequences and matching
  report counts. The tested executable's SHA-256 is recorded in reports/live-output-check.json.
  Neither live run experienced a natural reconnect; reconnect behavior was tested with
  controlled in-memory disconnects, not inferred from these live observations.
- Final release with stdout deliberately left unread: the 30-second live run exited in
  33.984 seconds with consumer_slow at stdout_drain, rather than hanging. It reported
  33 events enqueued and 21 flushed before the pipe blocked. See reports/blocked-consumer.json.

Earlier 0.1 observations on 2026-11-04: a public LIVE channel (45 seconds, 68 frames, 11 chats,
7 gift updates, 41 likes) and a public LIVE channel (25 seconds, 41 frames, 4 chats, 6 gift updates,
16 likes), both with zero decode errors. Those were debug runs without the new supervisor;
their reports remain in reports/shaunnycash-direct-v2.json and reports/emac1738-direct.json.
Gift update counts are not purchased-gift quantities. TikHub was used once to discover
public test channels; it is not a runtime dependency.

## Remaining limits

- The direct unsigned guest route is observed protocol behavior, not an official API
  contract. TikTok can change or refuse it. There is no hidden paid-service fallback.
- No replay or recovery of events lost during disconnection. possible_gaps explicitly
  records interrupted sessions; gap_recovery remains false.
- Deduplication retains 8,192 message IDs; combo accounting retains 4,096 streak keys.
  These bounded windows are not exactly-once delivery or financial accounting.
  A restarted process loses this in-memory state.
- Since 0.7.1, after combo-window overflow, an unseen cumulative count above one
  emits a null delta and establishes a new baseline. `gift_streak_resyncs` counts
  these uncertain updates; consumers cannot recover the missing quantities from it.
- Missing gift metadata yields a null quantity delta. Combos without a reliable identity
  wait for the final update; missing final messages cannot be reconstructed. An event
  without an envelope/common message ID cannot be deduplicated by ID.
- Normalized types: chat, gift, like, follow, share, social, join, viewers and stream_end.
  Unknown methods are counted. Battle/commerce events, durable storage, OBS and GUI
  integration are outside the current connection-and-events scope.
- Short live runs and synthetic faults do not establish all-day uptime, all-region
  compatibility, completeness, or a performance advantage over Node.

Historical reports naming experiments/live-engine-rust, engines/tiktok-live,
creatorhub-live-engine or tiktok-live-engine refer to artifacts from before the current
directory/package rename. Their JSON and recorded hashes are preserved as historical evidence;
each hash identifies its own tested artifact, not a subsequent tiktok-live-rust rebuild.

The current focus is stable connection, reconnect and correct basic event handling.
Longer live observations and targeted regressions should guide subsequent reliability work.
The Source trait separates the provider from the supervisor and codec, so protocol updates
need not rewrite the consumer UI.

See THIRD_PARTY_NOTICES.md for the MIT reference provenance and retained notice.
