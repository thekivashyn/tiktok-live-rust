# Matched TikTok LIVE receiver benchmarks

The original tables below describe the 0.7.1 comparison. During the 0.7.2 watchdog
investigation, the receiver gained an optional `TLR_BENCH_IDLE_SECONDS` setting
(default 10) and a bounded status transition trace. The 0.7.1/0.7.2 watchdog
comparison uses an archived 0.7.1 receiver with that same instrumentation. These
are fixture settings; the production engine's timeout defaults remain unchanged.

This harness compares `tiktok-live-rust` 0.7.1 with the pinned
`tiktok-live-connector` 2.5.0 package on Node 26.7.0. It uses synthetic public-protocol
messages, not recordings of real people. Both clients consume the same wire bytes
and produce the same application event contract.

The connector runs with the core message types selected. Our application adapter
adds bounded deduplication, gift deltas, history filtering and canonical JSON output.
The socket client additionally supplies reconnect and idle-watchdog policies.
These are harness features, not claims about built-in connector behavior. Its batch
hook feeds the adapter directly; the benchmark does not exercise every upstream
event listener or all of its supported message types.

## Workloads and correctness

- `generate.py`: 8,192 frames per cycle, each containing chat, two gift updates,
  like, follow, share, join and viewer events, plus a duplicate and a historical
  message. Plain, alternating gzip/plain, and all-gzip variants have identical
  logical output. Includes Unicode, escapes, occasional longer chat and IDs above
  JavaScript's safe integer range, within positive signed int64.
- `generate_large.py`: 128 gzip frames, each with 128 roughly 4 KB chat messages.
  This stresses decompression and serialization; it is not typical LIVE traffic.
- `generate_eviction.py`: 4,097 distinct gift groups, then late cumulative updates
  to an evicted group. Confirms the 0.7.1 unknown-delta behavior and subsequent
  resynchronization, without enlarging the 4,096-group cache.

Independent Python generation writes the expected output. Canonical rows are
`[kind, message_id_string, user_id_or_null, data_object]` followed by a newline.
Nondeterministic timestamps and implementation-specific sequence metadata are
excluded. Rust's actual output queue sequence is checked separately. Event counts,
gift quantities, byte counts and incremental CRC32 are checked for every measured
run. Single-cycle and eviction captures also match the oracle by full SHA-256.
CRC32 is an integrity check, not a cryptographic proof.

## Two different speed measurements

`pipeline` measures decoding, application normalization, canonical JSON serialization
and CRC32. It excludes socket I/O, the engine's worker scheduling, TLS and signing.
One warm-up cycle and input loading are excluded from timed throughput, but included
in process memory. It runs three alternating pairs and reports medians.

`socket` uses a real loopback TCP/WebSocket connection, ACKs, the Rust engine's
supervisor and bounded 256-event queue, and the connector's WebSocket/decoder with
the adapter. The fixture is a separate process, excluded from receiver RAM/CPU.
It sends one frame and waits for ACK before the next. The ACK points differ between
implementations, so this is not a directly comparable consumer-latency measurement.
Three sequential pairs with alternating order measure the full local receive path.
Throughput is output events divided by client elapsed time, including connection
setup and shutdown; no remote TikTok latency is implied.

The paced soak sends exactly 120,000 fresh mixed frames per client, requesting a
10 ms delay. Every 6,000 frames it breaks the connection before ACK; every 18,000
frames the failure is instead a 12-second silent stall. The last frame is replayed
after reconnect. Both test clients use a 10-second idle watchdog. Rust retains its
retry policy (with a two-second healthy-session reset in this fixture); the Node
application retries after 1.5 seconds. Consequently, total duration is not a fair
comparison of reconnect speed. Production Rust heartbeat/idle defaults are unchanged.

OS working set and private bytes use a requested 250 ms sampling interval with
Windows APIs; collection overhead increases the interval, and actual timestamps
are retained in every report.
Working-set peak uses the OS peak counter observed during sampling; private-memory
peaks and thread peaks are sampled. CPU is process user + kernel time. Memory
includes runtime, adapter and checksum consumer, not the fixture server or whole
application UI. The soak compares minute 2 with the final minute and the final five
minutes. A plateau during this finite run is not proof of leak-free 24/7 operation.
Paced soaks ran concurrently on this host while short throughput tests ran, so
scheduling noise remains. Do not interpret small before/after changes as significant.

## Reproduce on Windows

Run from the engine directory. Node is a benchmark-only dependency; it is not needed
by the shipped Rust library or CLI. `node_modules` and generated reports are ignored.
The pinned connector package has its own AGPL-3.0 licensing; no upstream schema or
connector source is copied into the production Rust crate.

```powershell
npm ci --prefix benchmarks/connector --ignore-scripts --no-audit
python benchmarks/connector/generate.py
python benchmarks/connector/generate_large.py
python benchmarks/connector/generate_eviction.py
.\run.ps1 -CargoArgs @('build','--release','--examples','--bins','--locked')
.\run.ps1 -CargoArgs @('test','--all-targets','--locked')
.\run.ps1 -CargoArgs @('clippy','--all-targets','--locked','--','-D','warnings')
.\run.ps1 -CargoArgs @('fmt','--all','--','--check')
python benchmarks/connector/measure.py pipeline --label my-pipeline --passes 16 --pairs 3
python benchmarks/connector/measure.py pipeline --label my-large --passes 8 --pairs 3 --large
python benchmarks/connector/measure.py socket --label my-loopback1 --frames 65536 --pace 0 --cut 0 --stall 0 --timeout 180 --sequential
python benchmarks/connector/measure.py socket --label my-loopback2 --frames 65536 --pace 0 --cut 0 --stall 0 --timeout 180 --sequential --reverse
python benchmarks/connector/measure.py socket --label my-loopback3 --frames 65536 --pace 0 --cut 0 --stall 0 --timeout 180 --sequential
python benchmarks/connector/measure.py socket --label my-soak --frames 120000 --pace 10 --cut 6000 --stall 18000 --timeout 2400
```

When the original 0.7.0 binary is available at
`reports/matched-comparison/baseline-v0.7/matched_pipeline.exe`,
`python benchmarks/connector/compare_patch.py` runs three alternating Rust version
pairs, 64 corpus cycles each, to assess the correctness patch's processing cost.
It preserves these results separately from the connector comparison.

Use fresh labels; an existing ready file is rejected. Do not regenerate corpora or
replace executables while a measurement is running. `--candidate` selects the same
Rust receiver under a second executable name, allowing the original 0.7.0 receiver
to remain open during baseline measurement on Windows.

For full-output checks, pass a third capture-path argument after corpus and passes
to `matched_pipeline.exe` or `pipeline.mjs`; use one pass for the eviction corpus.
Compare the resulting bytes with `expected.ndjson` or `expected-eviction.ndjson`.

## Interpretation and retained failed experiments

The results validate this core event subset and supplied replay schedule. They do
not measure signing success, TLS, TikTok changes, real Wi-Fi/OS suspend, arbitrary
event schemas or events never delivered during an outage. Bounded deduplication
and combo history cannot provide unlimited exactly-once accounting. Unknown gift
deltas must remain unknown, not be silently treated as complete totals.

An initial fixture used `u64::MAX` for a control-message ID, outside the shared
positive signed int64 domain. It was corrected before the accepted measurements.
An initial soak used a three-second test watchdog while other benchmarks ran;
unexpected peer exits made that comparison invalid. It was explicitly aborted
and retained as `soak1-aborted.json`. The root cause was not established. Accepted
soaks use a ten-second test watchdog and record fixture event-loop delay, peer
cleanup, replay counts and unexpected exits. They are new runs, not rewritten
versions of the failed experiment.

A report-naming issue was found while the final soak was already running: Python
`with_suffix` truncated labels containing version dots. A watcher preserved each
complete JSON report unchanged under its intended per-client name before the next
client completed. The harness now appends `.json` without replacing label suffixes;
the receiver measurements and fixture reports were unaffected.

The archived reports live under `reports/matched-comparison/`. Run
`python benchmarks/connector/summarize.py` after the named release-validation runs
finish to verify their outputs and regenerate `verification-v0.7.1.json`.
