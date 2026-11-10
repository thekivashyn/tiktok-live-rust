# Security

This is an experimental 0.x library/CLI. Fixes target the current development
version; older releases do not have a separate maintenance commitment.

## Reporting

For a sensitive finding, use the repository's **Security → Advisories → Report a
vulnerability** option when the maintainer has enabled private reporting. If it
is unavailable, open a minimal issue requesting a private reporting channel
without including exploit details, credentials or personal data. No private
contact address is declared in this repository yet.

Include the affected version/commit, platform, impact and a minimal synthetic
reproduction. Do not attach account sessions, cookies, raw LIVE frames, sender
profiles or chat logs. An aggregate report and redacted diagnostics are preferred.

## Trust boundaries

Network bytes, event text and avatar URLs are untrusted. The engine enforces
wire/decompression/metadata limits and does not download avatars. Consumers are
responsible for rendering, storage and any further requests they make.

Guest identities are obtained at runtime; the CLI does not accept account-session
imports. Connection reports omit raw network error chains and cookies. The owned
guest-cookie string is zeroized, but internal dependency copies are not covered
by that guarantee.

The offline tests do not access TikTok. `scripts/audit_dependencies.py` sends only
public crate names and versions from `Cargo.lock` to OSV. A clean advisory lookup
does not establish that the code is free from vulnerabilities.
