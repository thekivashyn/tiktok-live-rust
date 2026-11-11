# tiktok-live-rust — Version history

**Developed by Tri Nguyen**

Current engine version: **0.9.0**. These are local development versions, not a
claim of published packages or Git release tags.

History through 0.8.0 was reconstructed on **2026-11-05 (Asia/Saigon)** from the
existing README and retained reports. Earlier entries summarize that evidence;
they are not newly executed tests or a complete commit history. The original
documentation uses short labels 0.1–0.7; they are listed here as 0.x.0.
No unrecorded release dates, commit IDs or measurements have been inferred.

Start with [development and handoff rules](DEVELOPMENT.md). Detailed runtime/API
documentation remains in [README](README.md). Reports under `reports/` are ignored
by Git and may be absent in another checkout; summaries here remain readable.

## Unreleased

### v0.9.0 reconnect lifecycle — implementation in progress, 2026-11-11

- Addresses [issue #1](https://github.com/thekivashyn/tiktok-live-rust/issues/1):
  opt-in typed lifecycle notifications, including reconnects with no later event.
- Adds `engine::run_observed` / `Observers`, `lifecycle::channel()` and CLI
  `--lifecycle`. Existing entry points and event JSON stay compatible.
- Retains 64 lifecycle notifications; slow Rust consumers receive explicit
  `Lagged(n)` and the CLI writes tagged `lifecycle_lagged` records. No blocking
  ingestion, unbounded queues or implicit lossless-notification claim.
- Updates Rust/Python consumers to keep the same subscription, flag incomplete
  application views and distinguish reconnect/readiness from terminal stops.
- Thanks to [@Rhytera on X](https://x.com/Rhytera) for the feedback that prompted
  clearer reconnect visibility and consumer gap handling.
- Validation in progress. Windows checks for the initial implementation passed;
  final Windows v0.9.0 checks passed: 100 Rust tests, 11 Python tests, formatting,
  Clippy with denied warnings, release CLI/examples build and offline demo consumed
  by Python. Source manifest/local links passed for 94 selected files.
  Windows/Linux/macOS CI remains to be completed before publication.
- Limits: no upstream replay/backfill or new LIVE/performance claim. `gap_recovery`
  remains false. See `docs/RECONNECTS.md` for observation/ordering/overflow semantics.


### Follow-up audit and runnable onboarding — 2026-11-10

- Audited transport/input bounds, decoder cancellation, retry/readiness/queue
  handling, per-room state, optional metadata, CLI streams and publication tooling.
  Findings and retained limits are in `docs/SECOND_AUDIT.md`; no confirmed core
  Rust runtime defect requiring a change was found in the reviewed paths.
- Reworked README around a local SVG introduction, runnable offline/LIVE starts,
  Python/Rust/agent integration paths and explicitly versioned benchmark evidence.
  Added `docs/AGENT_GUIDE.md`, including machine output and untrusted-content rules.
- Added `examples/offline_demo.rs`: seven synthetic wire messages become six
  events through the actual bounded decoder/processor, with dedupe and gift deltas.
  Added `examples/consume.py`: concurrent pipe drains, bounded line reads, a host
  deadline, explicit child cleanup and aggregate-only output.
- Added automatic source-manifest/local-link checks and nine real-subprocess /
  synthetic-export Python regressions. Expanded export filters for the new Python
  examples/tests and SVG asset. CI runs them and the offline Rust-to-Python demo;
  help/version invoke the built CLI directly to avoid redundant Cargo rebuilds.
- Compatibility: examples, documentation and tooling only. Engine API/schema,
  core Rust source, registry dependencies and 0.8.0 version remain unchanged.
- Local validation: 91 Rust tests and nine Python regressions passed; all-target
  Clippy with denied warnings, formatting and release CLI/examples build passed.
  The release offline demo emitted six valid events with known gift deltas 1/2;
  the Python consumer received all six and exited successfully. Manifest/local-link
  checks passed for 92 selected files. SVG XML was checked and its rendered preview
  visually reviewed. Core source/old Rust tests/Cargo.lock remain unchanged.
- First [CI run 42768481987](https://github.com/thekivashyn/tiktok-live-rust/actions/runs/42768481987)
  exposed a new manifest-ordering portability defect on Linux/macOS. Preserve that
  failed run. Sort explicit relative POSIX strings instead of platform Path objects;
  the new ordering regression reproduced failure before the fix and passes after it.
- A real invalid-username probe confirmed CLI preflight stderr is plain text.
  The Python consumer now counts it without storing contents and preserves the
  process exit code; an additional subprocess regression covers that contract.
- Fresh [CI run 42768795230](https://github.com/thekivashyn/tiktok-live-rust/actions/runs/42768795230)
  passed on Windows, Linux and macOS at commit
  `9d4cdeda099a406d9ec6777e9a140efe02d7bd36`: 91 Rust tests and nine Python tests
  per platform, manifest/local links, formatting, denied-warning Clippy, release
  builds, CLI help/version and the Rust demo consumed by Python.
- Final evidence/disclosure text and manifest refresh use a documentation-only
  follow-up (`[skip ci]`); tested engine/examples/tooling/workflow stay unchanged.
  No unfinished implementation remains. No new LIVE sample or performance
  benchmark; historical measurements stay historical and excluded raw artifacts
  are now explicitly distinguished from rerunning the current benchmark harness.

### Initial GitHub publication — 2026-11-10

- Repository supplied by the owner: `https://github.com/thekivashyn/tiktok-live-rust`.
  Confirmed public and empty before preparing the initial `main` commit.
- Added Cargo repository metadata and the actual owner/remote to the setup guide.
  Publish only the prepared standalone source; the parent workspace remains separate.
- Normalize selected export text to LF and declare `.gitattributes` so source
  manifest hashes remain valid after Git checkout across operating systems.
- Compatibility: metadata/documentation only; engine 0.8.0, runtime, event schema
  and registry dependency versions are unchanged.
- Validation: reuse the recorded 90-test Windows publication checks; no unrelated
  runtime/LIVE test rerun for repository metadata. Source/hash/link checks precede push.
- Published initial `main` commit `ad947ce72f40757f4be1217941060889145d14ee`.
  [CI run 42765689325](https://github.com/thekivashyn/tiktok-live-rust/actions/runs/42765689325)
  passed on Windows, Linux and macOS: formatting, 90 offline tests per platform,
  Clippy, release CLI/examples builds, and CLI help/version. No runtime fix was needed.
- Configured the ten documented repository topics and enabled private vulnerability
  reporting. Added README clone commands, author link and CI badge after success.
- Final documentation/whitespace follow-up preserves the tested Rust source/tests,
  dependency lock and workflow. Checked staged diff, source manifest hashes and
  documentation links; did not repeat CI for that follow-up (`[skip ci]`).
  Trimmed pre-existing empty EOF lines in four text files without behavior changes.
- Limits/handoff: no new LIVE or performance test, no release tag or package publish.
  The parent engine source and its standalone Git checkout are synchronized; the
  checkout is `.public-export/tiktok-live-rust-public`, tracking `origin/main`.

### Public repository preparation — 2026-11-10

- Replaced the development-heavy README with public onboarding, a synthetic event
  example, use cases, integration paths and explicit delivery/platform limits.
  Retained sanitized historical detail in `docs/ENGINE_NOTES.md`; original text
  remains locally in `reports/publication-audit-2026-11-10/README-original.md`.
- Added `docs/USAGE.md`, `docs/ARCHITECTURE.md`, `docs/GITHUB_SETUP.md`,
  `docs/PUBLICATION_AUDIT.md`, contribution/security guides and issue/PR templates.
- Owner explicitly selected MIT. Added the project LICENSE and Cargo license/readme
  metadata, retaining Tri Nguyen attribution and upstream third-party notices.
- Added read-only GitHub CI for Windows/Linux/macOS and broader local-file ignore
  rules. Added a standalone source exporter with hashes and a script inventory.
- Converted links to excluded historical reports into literal local evidence paths;
  removed real test-channel handles from public prose while preserving measurements
  and failed outcomes. Historical results are not newly executed LIVE tests.
- Compatibility: no runtime, Rust API, JSON schema, registry dependency version or
  engine-version change. Cargo.toml/Cargo.lock remain 0.8.0; `publish = false` stays.
- Validation: 90 offline tests, all-target Clippy with denied warnings, formatting,
  release CLI/examples build and release --help/--version passed on Windows.
  Fresh OSV query covered 162 locked registry versions with no known advisory returned.
  Local evidence: `reports/publication-dependencies-2026-11-10.json`.
- Standalone verification: exported 83 selected source/configuration/document files;
  all manifest SHA-256 hashes and local links in 17 Markdown files matched. The
  exported Cargo manifest resolves to its own workspace root, version 0.8.0 / MIT,
  with registry dependencies only. Its 90-test suite also passed using the existing
  toolchain and shared build directory. Exporter rejected four existing/unsafe
  destinations; CI YAML structure and three-platform matrix parsed successfully.
- Limits/handoff: CI has not run on GitHub; Linux/macOS, LIVE connectivity and
  current performance were not retested. No repository was created or uploaded.

### Documentation — 2026-11-05

- Added this version history, `DEVELOPMENT.md` with a release-entry template, and
  engine-scoped `AGENTS.md` instructions for subsequent agents.
- Linked history and development instructions from the engine README.
- Runtime, event contract, dependencies and package version remain at 0.8.0.
- Validation: checked local Markdown links and version consistency. No runtime
  tests were rerun for this documentation-only change; the 90-test result below
  belongs to the recorded 0.8.0 implementation validation.

## 0.8.0 — Sender profiles and detailed like events

### Changes and compatibility

- Added optional `user`: ID, username, display name, avatar URL, verification,
  secure user ID and follow information for supported sender-bearing events.
- Added `like_details`. Together with existing `data.count`, `data.total` and
  the selected `user` projection, this covers all 12 root like fields in the
  pinned Connector 2.5.0 / tiktok-live-proto 0.2.4 v3 schema.
- Existing event names, `user_id` and `data` remain. Rust callers constructing
  complete Event literals must account for the new fields; JSON consumers must
  accept additive fields. 64-bit metadata uses decimal strings.
- Metadata allocation is bounded before decoding (32 KiB wire, 16 KiB text,
  depth/list/field limits). Invalid optional metadata preserves the core event
  with `metadata_incomplete`; reports count `metadata_incomplete_events`.
- No production dependency, profile lookup, avatar download or profile cache added.

### Validation and evidence

- 90 tests, Clippy with warnings denied, formatting and release builds passed.
- Independent shared-payload synthetic parity: 38 events / 31 likes; all 12 like
  root fields exercised with non-default values, zero mismatches.
- Separate 120-second LIVE parity on `a public LIVE channel`: 176 events / 42 likes,
  zero mismatches; 121/121 sender-bearing events had the three requested fields.
- Full-engine 600-second LIVE run: 990 events / 336 likes; 703/703 sender-bearing
  events had username, display name and avatar; 336/336 likes had details.
  All 14 checks passed; no decode errors, incomplete metadata or delivered
  duplicate IDs. Three forced cuts recovered in 2.437–3.521 seconds from retry
  wait to first valid batch, including backoff and handshake.
- Peak working set 10.105 MiB; final-five-minute range 9.844–10.086 MiB.
  RAM includes the bounded relay and validator, not just the standalone CLI.
- Evidence: `reports/profile-like/README.md` (local evidence),
  `reports/profile-like/verification-v0.8.0.json` (local evidence).

### Limits / next-agent context

- User is a documented profile projection, not the entire upstream User object.
  Unknown new wire fields are ignored; absent source values stay null/default.
- TikTok may omit or aggregate likes. Matching received payloads cannot prove
  receipt of every tap. Two-minute parity and ten-minute soak are separate runs;
  neither establishes 24/7 stability or a speed gain over earlier versions.

## 0.7.3 — Preserve identified empty chat; bounded failure diagnostics

- Kept empty-text chat when both message and sender IDs are nonzero, including
  common-header message-ID fallback. Text remains empty; no emote is invented.
- Added `empty_chats` accounting and eleven fixed decode-failure counters
  (88 bytes per counter structure), without retaining failed payload content.
- Validation: 82 tests and Clippy passed; independent canonical fixtures match.
  Final 15-minute LIVE: 1,964 events, including 12 empty chats, zero decode errors,
  3/3 forced recoveries, peak working set 9.961 MiB including the probe.
- The earlier 45-minute 0.7.2 run had 18 decode/validation errors. A later probe
  confirmed empty-chat rejection as one category, not the cause of all 18.
  These are separate runs, not an uninterrupted hour on 0.7.3.
- Evidence: `reports/watchdog-investigation/README.md` (local evidence),
  `reports/watchdog-investigation/verification-v0.7.3.json` (local evidence).

## 0.7.2 — Fix watchdog priority over already-buffered input

- Before an expired startup/socket/batch watchdog closes a connection, poll one
  immediately-ready socket read through the normal bounded pipeline. Permit only
  one probe for an unchanged deadline, so malformed or irrelevant traffic cannot
  postpone timeout indefinitely. Added `watchdog_ready_reads` diagnostics.
- Production timeouts, retry policy and cache sizes remain unchanged.
- Validation: 79 tests; three alternating process-pause pairs reproduced failure
  in 0.7.1 and passed in 0.7.2. A three-second-watchdog fixture delivered 480,001
  events and recovered from six cuts plus three real idle stalls.
- **Partial LIVE result:** 45 minutes / 5,327 events / 3 recoveries, but 18 decode
  errors. Do not mark this an entirely passing event-accuracy test; see 0.7.3.
- Equal-instrumentation loopback rates were 132,726 → 131,713 events/s and peak
  working set 6.19 → 6.39 MiB. This was a correctness fix, not a measured speed/RAM gain.
- Evidence: `reports/watchdog-investigation/README.md` (local evidence),
  `reports/watchdog-investigation/verification-v0.7.2.json` (local evidence).

## 0.7.1 — Gift-window correctness and matched Connector comparison

- After the 4,096-combo window overflows, an unseen cumulative gift count above
  one now emits `quantity_delta: null` and establishes a new baseline. Later known
  updates resume delta accounting. Added eviction/resync counters without growing
  the cache. Consumers must retain null as unknown, not count it as a known total.
- Validation: 76 tests and Clippy passed. Shared synthetic bytes and independent
  canonical output check both Rust and pinned Connector 2.5.0 plus our adapter.
- Paced fixture soak: 960,001 events per implementation, 19 recoveries each,
  matching counts/bytes/checksums, zero decode errors and clean shutdown.
  Rust peak working set 6.80 MiB versus Node 131.06 MiB in this workload.
- Connector dedupe, gift-delta and reconnect policies here belong to our comparison
  adapter, not upstream built-ins. Synthetic replay does not recover real events
  never delivered during outages. An earlier failed soak remains retained.
- Evidence: [harness and limits](benchmarks/connector/README.md),
  `reports/matched-comparison/verification-v0.7.1.json` (local evidence).

## 0.7.0 — Small-frame scheduling and decoder shutdown

- Decode recognized non-gzip frames up to 4 KiB inline, yielding every 32 inline
  frames. Larger frames and gzip use the blocking worker.
- Each session owns its pending decoder job; cancellation signals it and normal
  shutdown joins it. Added routing/drain counters; event JSON and state caps remain.
- Validation: 74 tests, fmt, Clippy and release builds passed. Ten-minute fixture
  soak: 746,145 events, nine cuts, correct counts/gift totals, clean shutdown.
- Separate 60-second LIVE: 135 events, zero decode errors, peak working set
  9.60 MiB. Separate forced-reconnect LIVE: 133 events and recovery checks passed.
- Cancellation is cooperative; callers should cancel and await `run` for joined
  shutdown. Synthetic gains do not predict TikTok network delivery speed.
- Evidence: `reports/verification-v0.7.json` (local evidence),
  `reports/engine-comparison-v0.7.json` (local evidence),
  `reports/soak-comparison-v0.7.json` (local evidence).

## 0.6.0 — Shared decoding, lower allocation cost, engine rename

- Added `decode_shared(Bytes)` and `process_ref` to borrow envelope payloads;
  validate the full batch before delivery. Kept the older owned API available.
- Bounded gzip growth, validated concatenated members/trailers, reduced WebSocket
  initial read allocation to 16 KiB, and reduced gift-key storage duplication.
- Renamed/moved the standalone engine to `CreatorHubVN/Engines/tiktok-live-rust`;
  crate `tiktok_live_rust`, executable `tiktok-live-rust`, developer Tri Nguyen.
  Honored HTTP 429 cooldown without a usable hint with a 60-second wait.
- Validation: 66 tests for the memory revision, 67 after rename/network changes;
  fmt, Clippy and release checks passed after the move.
- Large-chat incremental processing heap: 12,713,677 → 5,327,171 bytes;
  mixed allocation/reallocation calls: 2,031,666 → 1,090,610 in the named fixtures.
  These are processing-heap measurements, not total LIVE RAM reductions.
- Separate 60-second LIVE runs: 65 events at 9.45 MiB peak working set, and
  127 events across one forced reconnect; zero decode errors.
- The old owned decoder retains its older single-member gzip behavior. Use the
  shared path for the current receive implementation; no server protocol changed.
- Evidence: `reports/verification-v0.6.json` (local evidence),
  `reports/rename-verification-v0.6.json` (local evidence),
  `reports/heap-v0.6.json` (local evidence),
  `reports/performance-comparison-v0.6.json` (local evidence).

## 0.5.0 — Connection health and sustained outage retries

- Added `run_with_status`, bounded latest-snapshot watch channel, connection
  states and last protocol/batch/event observation times. Heartbeat alone does
  not establish receiving readiness.
- Added opt-in `retry_until_deadline` / `--retry-until-deadline`; capped backoff,
  Retry-After and terminal refusal/offline behavior remain. Default retry budget
  remains eight. Added separate `--status` stderr diagnostics with bounded drain.
- Rust complete Config literals need the new field; `..Config::default()` callers
  remain compatible. Hosts must drain both stdout and stderr and release watch
  borrows before awaiting or doing slow work.
- Validation: 56 tests, fmt, Clippy and builds passed. Sixty-second LIVE: 82 events,
  zero decode errors, 9.78 MiB peak working set. Separate reconnect: 115 events
  across two ready connections. Blocked stderr exited with an explicit error.
- Status is a latest-state view, not a lossless transition log. Retry cannot
  recover source events missed while disconnected.
- Evidence: `reports/verification-v0.5.json` (local evidence),
  `reports/memory-status-v0.5.json` (local evidence),
  `reports/reconnect-v0.5.json` (local evidence),
  `reports/blocked-status-v0.5.json` (local evidence).

## 0.4.0 — Typed events, receive cost and burst backpressure

- CLI uses a current-thread Tokio runtime; library uses the host runtime.
- Replaced dynamic `Event.data` JSON trees with typed `EventData`, moving strings
  into output; borrowed uncompressed batch payloads and skipped unknown methods
  early. JSON fields/values remain; object key order may differ.
- Added one shared 250 ms queue grace period per batch, configurable up to one
  second; a continuously slow consumer still stops explicitly.
- Rust API migration: match `EventData` or call `serde_json::to_value` at the
  application boundary; full Config literals need the output-grace field.
- Recorded LIVE: 91 delivered events, zero decode errors, 9.59 MiB peak working
  set. Separate reconnect: 114 events across two connections. Traffic differed
  from the 0.3 run, so these RAM observations are not a controlled reduction.
- Evidence: `reports/performance-comparison-v0.4.json` (local evidence),
  `reports/memory-v0.4.json` (local evidence),
  `reports/reconnect-v0.4.json` (local evidence).

## 0.3.0 — Readiness watchdogs, recovery and audience events

- Made abrupt EOF retryable; reset outage retry budget only after a healthy
  connection. Added startup, protocol-idle and batch-idle watchdogs.
- Process received events before ACK so an ACK failure does not discard the batch.
- Added follow/share/social/join/viewer handling and recipient-aware gift metadata;
  keep gift quantity deltas distinct from cumulative counts and event counts.
- Separate 60-second LIVE runs: 146 events with matching CLI output, and 166
  events across a forced reconnect; zero decode errors. Share and stream-end
  behavior were fixture-tested, not observed in those LIVE samples.
- Evidence: `reports/live-v0.3-output-check.json` (local evidence),
  `reports/reconnect-v0.3.json` (local evidence). An offline attempt is retained in
  `reports/reconnect-v0.3-offline.json` (local evidence).

## 0.2.0 — Bounded supervisor and event delivery

- Added retry/backoff, Retry-After, deadlines/cancellation and bounded shutdown.
  Reconnect preserves bounded dedupe/combo state within a room and resets it on
  room change. Added event sequence and connection epoch.
- Bounded CLI event queue, explicit slow-consumer errors, verified TLS/fixed
  destinations, guest-cookie handling, input/inflate/message limits and atomic
  aggregate report writes. Registry dependency versions pinned by Cargo.lock.
- Final 60-second LIVE: 77 events, zero decode errors, contiguous sequences and
  matching consumer/report counts. Earlier 180-second run preceded final fixes.
  Neither LIVE observation had a natural reconnect; controlled tests cover it.
- Blocked stdout test exited in 33.984 seconds for a 30-second run with explicit
  `consumer_slow`, rather than hanging.
- Evidence: `reports/live-output-check.json` (local evidence),
  `reports/live-output-check-initial-180s.json` (local evidence),
  `reports/blocked-consumer.json` (local evidence).

## 0.1.0 — Initial direct guest prototype

- Historical debug prototype received public LIVE chat, gift updates and likes
  directly without Euler, before the later connection supervisor.
- Recorded observations on 2026-11-04: `a public LIVE channel`, 45 seconds / 68 frames;
  `a public LIVE channel`, 25 seconds / 41 frames; zero decode errors in both.
- These debug observations are not release-build benchmarks or reconnect proof;
  gift update counts do not equal purchased-gift quantities.
- Evidence: `reports/shaunnycash-direct-v2.json` (local evidence),
  `reports/emac1738-direct.json` (local evidence), historical context in [README](README.md).
