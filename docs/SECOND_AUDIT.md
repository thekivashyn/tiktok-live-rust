# Follow-up source and integration audit

Recorded 2026-11-10, Asia/Saigon. Scope: public source following commit
`cda17cb99720758906d79ee2abb5a1878ba7386d`. Developed by Tri Nguyen.

## Findings addressed

| Finding | Consequence | Change |
| --- | --- | --- |
| First meaningful run required a channel currently LIVE | A visitor could not reproduce event output from the repository alone | Added an offline synthetic wire/decoder/processor demo with a regression for dedupe, large IDs and cumulative gift deltas |
| Cross-language integration had prose but no working example | Sequential pipe consumption can block a child; cancellation/exit failures are easy to miss | Added a Python consumer with concurrent drains, bounded line buffers, a host deadline, cleanup and aggregate-only output |
| Manifest hashes were maintained manually without a CI check | Documentation or source updates could silently invalidate the published manifest | Added manifest/link checking and explicit manifest refresh; CI now runs it |
| Export filters excluded new Python examples/tests and vector assets | A future standalone copy could lose its onboarding or checks | Extended only the relevant suffix lists and added export-content/hash regressions |
| CI used Cargo to rerun an already-built CLI | Target-feature differences could cause unnecessary recompilation for help/version | Invoke the built executable directly; include an offline Python-to-Rust demo check |
| README hid the project's strongest engineering choices | Readers had to infer why bounded state, reconnects and JSON output were useful | Added a visual introduction, two runnable starts, integration paths, agent guidance and clearly versioned evidence |

## Runtime paths reviewed

- Fixed destination construction, username/room validation, verified TLS, guest
  cookie handling, redirect/proxy policy and bounded HTTP JSON reads.
- Frame/message/decompression limits, complete-batch validation, borrowed payload
  lifetime, unknown field handling, inline decode routing and joined worker cleanup.
- Retry budgeting, cooldowns, readiness/watchdogs, cancellation and shared
  per-batch enqueue deadline; room changes and finite dedupe/combo windows.
- Event identity, empty chats, gift resynchronization, optional metadata budgets,
  avatar URL projection, health coalescing and final output/error counters.
- CLI stdout/stderr handling, report-file replacement, example subprocess cleanup,
  source export scope, local documentation targets and CI commands.

No confirmed defect requiring a Rust engine runtime change was found in these
reviewed paths. The new code is confined to examples, publication checks and CI;
the existing core engine behavior and dependency lock remain unchanged.

## Limits retained explicitly

- Without `--status`, final aggregate writes are synchronous. A process host must
  drain both pipes and own a watchdog; the new Python example does both.
- Existing historical probes/summarizers remain investigation tools. Some assume
  Windows, old report names or excluded baseline executables. They are not a
  supported cross-platform SDK; see [script inventory](../scripts/README.md).
- Protocol/transport provenance is retained. This pass did not establish a legal
  opinion about interoperability or transitive licenses.
- No fresh TikTok LIVE observation, speed benchmark, release binary distribution
  or long-duration/multi-room capacity claim was added. The old measurements
  retain their versions and limitations.
- The offline demo tests decoding/normalization, not live acceptance, TLS or the
  connection supervisor. Tests cannot establish complete source delivery.

## Validation

The change adds one Rust example regression and seven Python integration/export
regressions. Python tests use real owned subprocesses with synthetic data,
including stderr backpressure, a >64 KiB additive event, nonzero exits, malformed
output and deadline cleanup. Export tests use a temporary synthetic repository.

Commands/results and the actual three-platform CI result are recorded in the
newest `Unreleased` [changelog entry](../CHANGELOG.md), rather than inferred from
the first publication run.
