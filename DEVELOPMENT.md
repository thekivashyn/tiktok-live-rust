# Engine development and version logs

**Developed by Tri Nguyen**

This workflow applies to this standalone engine. Start with [AGENTS.md](AGENTS.md),
the newest entries in [CHANGELOG.md](CHANGELOG.md), [Cargo.toml](Cargo.toml) and the
relevant API/limits sections in [README.md](README.md).

## Where information belongs

| File/location | Purpose |
| --- | --- |
| `Cargo.toml` + package entry in `Cargo.lock` | Actual engine version |
| `CHANGELOG.md` | Newest-first version changes, compatibility, checks, evidence and limits |
| `CHANGELOG.md` → `Unreleased` | In-progress work and documentation-only changes |
| `README.md` | How to use the engine, contracts, current behavior and limits |
| `docs/USAGE.md`, `docs/ARCHITECTURE.md` | Detailed integration contracts and engineering decisions |
| `docs/ENGINE_NOTES.md` | Archived pre-publication narrative, including historical failed runs |
| `docs/GITHUB_SETUP.md`, `docs/PUBLICATION_AUDIT.md` | Repository setup values and publication audit/handoff |
| `AGENTS.md` | Instructions an agent reads before making changes |
| `reports/<topic>/` | Detailed local logs, metrics, fixtures and verification hashes |

`reports/` and build outputs are ignored by Git. Put enough findings and numbers
in the changelog for another checkout to understand the result without them.
Link existing evidence at its original location; use fresh, descriptive run labels
for new measurements. Do not overwrite previous runs or remove failed experiments.
If evidence is unavailable, say so rather than treating a summary as a fresh test.

These are development/release notes, not a request for runtime event logging.
Do not enable persistent raw LIVE payload, identity or chat logging by default.

Publication integrity is checked by `python scripts/check_repository.py` in CI.
After reviewing intended changes, refresh hashes with `--update-manifest` and
rerun the checker. Run the Python integration/export regressions with
`python -m unittest discover -s tests -p "test_*.py" -v` when changing these tools.
Python checks use only synthetic fixtures and owned subprocesses, never TikTok.

## For each change

1. Confirm the manifest version and the newest history entry. Read the relevant
   previous failure/limitation before changing the affected path.
2. Implement within the requested engine scope. Add or update an `Unreleased`
   entry with the concrete trigger, behavior change, relevant files, compatibility
   impact and any unfinished work. Do not imply incomplete work is validated.
3. Run checks appropriate to the change. Record command, outcome and evidence;
   state explicitly when a check was not run and why. Documentation-only changes
   need link/version review, not an unrelated LIVE soak or repeated Rust tests.
4. When cutting a version, update the manifest/lock package version and README
   version together, move the relevant unreleased notes into a newest-first
   version entry, and record the actual validation date/timezone. A documentation
   update alone does not require an engine version bump.
5. Before handing off, leave concrete remaining limitations, failures and useful
   next steps in that entry. Keep unfinished items under `Unreleased`; this makes
   the files sufficient for another agent without relying on chat history.

Keep recorded version entries as historical facts. If a past claim was wrong,
append a clearly dated correction with evidence to that entry; do not silently
replace old measurements or recast a failing run as passing.

## Validation records

For engine code changes, use the existing Windows wrapper as applicable:

```powershell
.\run.ps1 -CargoArgs @('test','--all-targets','--locked')
.\run.ps1 -CargoArgs @('clippy','--all-targets','--locked','--','-D','warnings')
.\run.ps1 -CargoArgs @('fmt','--all','--','--check')
.\run.ps1 -CargoArgs @('build','--release','--examples','--bins','--locked')
```

Select targeted regressions for the actual change: watchdog/reconnect, backpressure,
shutdown, wire decoding, dedupe, gift delta or metadata parity. Full commands and
datasets for existing comparisons are in the linked reports and benchmark README.
Retain source/binary hashes in measurement artifacts when available. Never treat
a new source tree or rebuilt binary as the exact one used in an older measurement.

For performance claims record baseline/candidate, fixture, build/runtime versions,
run duration/pairs, event counts, measurement scope and units. Distinguish requested
Rust heap from process working set/private bytes and from whole-app RAM. State
whether relay/validator processes or threads are included. Use matched inputs for
before/after claims; different LIVE traffic is an observation, not a fair benchmark.

For LIVE checks record channel, date/timezone, actual duration, event/profile
coverage, decoder/metadata errors, deliberate cuts, recovery definition and cleanup.
Preserve partial/failing outcomes. Separate real-time runs from virtual-clock tests;
bounded-state/finite-soak results are not 24/7 guarantees or source completeness.

## Copyable version / handoff entry

Use this in `Unreleased` while work is incomplete, then move the completed record
under its actual version. Replace placeholders; write “not run” where appropriate.

```markdown
## <version> — <concrete change>

Recorded: <YYYY-MM-DD, timezone>. Status: <in progress / validated / partial>.

### Change and reason
- Trigger/problem and resulting behavior:
- Relevant files:

### Compatibility
- Rust API / JSON / configuration / dependency changes (or none):
- Consumer migration, if needed:

### Validation and evidence
- Exact commands and outcomes; checks not run and reason:
- Regression/parity results and evidence links:
- Before/after measurements with the same workload, or not measured:
- LIVE duration, coverage, errors and reconnect results, or not run:
- Evidence labels and source/binary hashes, if captured:

### Limits and handoff
- Known failures, uncertainty and missing coverage:
- Unfinished work / concrete next step, or none:
- Dated correction to earlier findings, if applicable:
```
