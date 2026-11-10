# Publication audit — 2026-11-10

Scope: the standalone `tiktok-live-rust` 0.8.0 engine, developed by Tri Nguyen.
Review date: 2026-11-10, Asia/Saigon. This is a focused source/publication review
and automated verification, not an independent penetration test or legal opinion.

## Publication follow-up — 2026-11-10

The prepared source is now published at
[thekivashyn/tiktok-live-rust](https://github.com/thekivashyn/tiktok-live-rust).
Initial commit `ad947ce72f40757f4be1217941060889145d14ee` passed the complete
[three-platform CI run](https://github.com/thekivashyn/tiktok-live-rust/actions/runs/42765689325):
formatting, 90 offline tests per platform, Clippy with denied warnings, release
CLI/examples builds and CLI help/version checks on Windows, Linux and macOS.
Topics and private vulnerability reporting are configured. Source text uses LF
newlines so the 84-file manifest also matches the committed Git blobs.

The sections below retain the original pre-publication audit snapshot. Its
"not yet published" and "CI not run" statements describe that earlier stage.
No new LIVE observation or performance benchmark was added during publication.

## Findings and disposition

| Priority | Finding | Disposition |
| --- | --- | --- |
| High | No project license or Cargo license metadata | Owner selected MIT; added LICENSE with Tri Nguyen attribution and `license = "MIT"` |
| High | The engine lives beside unrelated application source, a parent `.env`, local tools and runtime artifacts | Provide a selected-file standalone exporter; publish its output rather than the parent project |
| Medium | Old README was 869 lines and mixed onboarding, local machine paths, history and current contracts | New 166-line README; dedicated usage/architecture docs; sanitized historical notes retained |
| Medium | Markdown evidence links pointed into ignored `reports/`, producing missing links in a public checkout | Historical evidence references now use literal local paths, with an explicit availability note; public docs link to retained summaries |
| Medium | No public CI, contribution guide or structured issue/PR templates | Added all three; CI targets Windows/Linux/macOS and never intentionally connects to TikTok |
| Medium | Benchmark/history scripts assume Windows or excluded archived binaries | Added scripts inventory and clean-checkout limitations; no claim every historical experiment is immediately reproducible |
| Medium | Old throughput tables could be mistaken for current universal performance claims | Landing README explains scope and points to versioned benchmark methodology |
| Medium | No private security contact was declared | Added SECURITY.md fallback; enable GitHub private vulnerability reporting after repository creation |
| Low | Gitignore covered build output but not common local secret/editor files | Added credential/tool/editor/export exclusions; retained Cargo.lock |

No engine runtime behavior or event schema was changed during this publication work.
The package stays at 0.8.0 and remains unpublished on crates.io.

## Source review observations

Reviewed transport destinations/TLS/guest identity, CLI output and error handling,
decoder scheduling and cancellation, bounded wire/metadata/state, reconnect and
consumer-backpressure paths, dependency manifests, diagnostics and export scope.
No confirmed runtime defect was identified in this pass. That is limited to the
reviewed paths and executed checks, not a guarantee of absence of defects.

Pattern searches across the selected source found no apparent real credentials.
Matches such as `sessionid=secret`, `https://u:p@example.com/a` and
`example.com/avatar.webp?token=x` are synthetic rejection/test fixtures.
The original README and all existing reports remain preserved locally. Public
historical prose removes real test-channel handles and the machine-specific path.

Protocol provenance and the upstream MIT notice remain in THIRD_PARTY_NOTICES.md.
The optional Node comparison project retains its separately licensed dependencies;
installed dependency source, full upstream schema files and `node_modules` are not
included in the export. This pass did not establish a legal conclusion about
protocol interoperability or audit all transitive source licenses.

## Checks executed in this pass

On Windows, with the existing isolated stable toolchain through `run.ps1`:

| Check | Result |
| --- | --- |
| `cargo test --all-targets --locked` | Passed: 90 tests, plus example compilation; no TikTok traffic |
| `cargo clippy --all-targets --locked -- -D warnings` | Passed |
| `cargo fmt --all -- --check` | Passed |
| `cargo build --release --examples --bins --locked` | Passed |
| Release CLI `--help` and `--version` | Passed; identifies tiktok-live-rust 0.8.0 / Tri Nguyen |
| OSV exact Cargo.lock advisory lookup | Complete: 162 registry package versions, no known advisory returned |
| Standalone source export | 83 selected files; manifest hashes matched; excluded artifacts absent |
| Local Markdown link targets | Passed across 17 Markdown files |
| Exported Cargo metadata and offline tests | Own workspace root, 0.8.0 / MIT, registry dependencies only; 90 tests passed with shared toolchain/build artifacts |
| Exporter destination protection | Existing/unsafe destination refused in four cases |
| CI YAML structure | Parsed successfully; three operating systems, expected triggers and read-only permissions |

OSV response time: `2026-11-10T05:48:16.407738+00:00`.
Cargo.lock SHA-256:
`2b8c02e2695a350a1a8274795772e5e4e50321f9c4bbe942a3c844efec08c7a6`.
Local result: `reports/publication-dependencies-2026-11-10.json`.
Only public package names and versions were sent to the
[OSV batch API](https://google.github.io/osv.dev/post-v1-querybatch/).
An empty result is a point-in-time known-advisory lookup, not proof of security.

The CI checkout action is pinned to upstream v7 commit
`3d3c42e5aac5ba805825da76410c181273ba90b1`, resolved from the tag during this pass.
Usage was checked against the [upstream action documentation](https://github.com/actions/checkout).
The workflow has read-only repository permissions and does not persist checkout
credentials. It has not yet run on GitHub; Linux/macOS results remain unknown.

## Remaining limits and launch handoff

- No new LIVE probe or performance benchmark was run. Prior LIVE observations and
  performance numbers stay attached to their versions and finite workloads.
- Direct unsigned guest access is observed behavior, not a supported TikTok API
  contract. The route may change/refuse connections; no replay/24-hour guarantee.
- This workspace was not a Git repository at audit start. There is no commit
  history here to inspect for previously committed secrets, and no remote URL was supplied.
- GitHub Actions, repository security settings and account permissions can only be
  verified after the repository is created. Do not advertise CI as passing yet.
- Metadata is a selected projection. Gift deltas can be unknown after context loss;
  finite deduplication cannot support unlimited exactly-once accounting.

Use [GitHub setup values](GITHUB_SETUP.md) and review the prepared source copy.
Creation, commit and upload are separate launch actions; this pass prepares local
files and does not publish them.
