# Instructions for agents working on tiktok-live-rust

Scope: this engine directory and descendants. **Developed by Tri Nguyen.**

## Read first

- Read `Cargo.toml`, the newest entries in `CHANGELOG.md`, and `DEVELOPMENT.md`.
- Read the relevant API, limits and historical validation sections in `README.md`.
- Inspect the linked evidence for the path being changed when available. Reports
  are local ignored artifacts; their absence is not proof a test passed or failed.

## Maintain the version history

- Every engine/code/API/configuration change must update `CHANGELOG.md` in the same
  task. Use `Unreleased` for incomplete work; record what changed, why, compatibility,
  checks actually run, evidence paths and remaining limitations.
- Follow the entry template and validation guidance in `DEVELOPMENT.md`.
- When versioning, keep Cargo.toml, the root package version in Cargo.lock, README
  and the changelog consistent. Documentation alone does not need a version bump.
- Preserve old measurements and failed runs. Correct history with dated evidence;
  never invent release dates, tests, commit IDs or performance numbers.
- Before stopping, document unfinished work concretely so the next agent can resume
  from these files without needing the conversation.

## Preserve the engine's scope and contracts

- Keep a standalone Rust library/CLI, direct read-only guest connections and the
  existing Tri Nguyen attribution. Node dependencies are benchmark-only.
- Preserve bounded memory/queues/state, verified TLS, retry backoff/cooldowns,
  cancellation and joined cleanup. Do not silently discard consumer failures.
- Missing source metadata stays unknown/null/default. Unknown gift quantities
  must not become known totals. Finite dedupe does not imply exactly-once delivery.
- Keep raw LIVE content, user identities and credentials out of retained test
  diagnostics by default; prefer aggregate checks and synthetic fixtures.
- Use fresh report labels, keep failures, and distinguish matched benchmarks from
  unrelated LIVE samples. Test-owned relays/processes must be cleaned up.
- Run checks appropriate to the change; do not rerun unrelated runtime tests for
  documentation edits. Report actual results and material limits without claiming
  invisibility, every source event delivered, or guaranteed 24/7 stability.
