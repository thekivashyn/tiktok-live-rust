# Development scripts

Run scripts from the repository root. Python helpers generally use Python 3.11+
and its standard library. Node is required only for the optional connector oracle
and comparative benchmark harness. None is required by the production CLI.

| Category | Entry points | Requirements and output |
| --- | --- | --- |
| Dependency advisories | `audit_dependencies.py` | Online OSV lookup of public Cargo.lock package names/versions; writes aggregate `reports/dependency-audit.json` |
| Explicit LIVE smoke | `smoke_live.py` | Built release executable, public LIVE username, network; consumes events without retaining raw content |
| Metadata parity | `check_metadata.py`, `../benchmarks/connector/generate-metadata.mjs` | Built `metadata_capture` example and installed benchmark packages; synthetic fixture or explicit LIVE mode |
| Windows process measurements | `measure_process.py`, `measure_live_soak.py`, `probe_blocked_status.py` | Windows APIs, built binaries and requested report directories; not portable Python probes |
| Synthetic matched comparison | `../benchmarks/connector/generate*.py`, `../benchmarks/connector/measure.py` | See the [benchmark README](../benchmarks/connector/README.md); generated corpora are local |
| Metadata model maintenance | `generate_metadata_models.py` | Local selected field table; generates engine model files, so review its diff |
| Historical investigations | `compare_engine.py`, `compare_performance.py`, `compare_soak.py`, `compare_watchdog.py`, `watchdog_pause.py`, `check_empty_chat_parity.py`, `summarize*.py` | Some require archived executables or named reports excluded from a clean checkout; not general CI commands |
| Publication export | `prepare_public_repo.py` | Creates a fresh local source copy using a fixed file list; does not initialize Git or upload anything |
| Publication integrity | `check_repository.py` | Offline manifest hashes and local Markdown file targets; `--update-manifest` explicitly refreshes intended changes |

Use fresh labels/directories for measurements. The existing dependency helper
replaces its default report, so preserve any previous report before rerunning it.
Create output parents where required. Historical summary scripts may contain
fixed expected test counts from their original experiments; they do not establish
the result of today's checkout.

Do not attach `reports/`, installed `node_modules`, local toolchain paths or real
LIVE captures to public issues or source exports.
