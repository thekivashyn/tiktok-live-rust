# Integrating an app or agent

The CLI exposes a bounded child-process interface. It is not an MCP server or an
agent framework, and it does not expose posting/account actions. Your host owns
the child lifetime, interpretation of events and any further actions.

## Start without TikTok

```sh
cargo build --release --locked --bins --example offline_demo
python examples/consume.py --demo
```

The demo runs synthetic bytes through the real bounded batch decoder and event
processor. Seven input messages yield six output events: one duplicate chat is
filtered, and gift cumulative counts 1 → 3 yield deltas 1 and 2. No socket,
network credentials, remote avatar request or LIVE broadcast is needed. Cargo
may download dependencies when building. It does not test the network supervisor.

## Connect a public LIVE room

```sh
python examples/consume.py --user USERNAME --seconds 60
```

The [Python example](../examples/consume.py) uses Python 3.11+, its standard
library and a built release CLI. It drains stdout/stderr concurrently, bounds
line buffers, enforces a host deadline and terminates its owned process on parse
failure or cancellation. Output is aggregate counts, final state/stop reason and
an error code, not raw chat or profile logs. Extend its event handler for your app.

A host launching the CLI itself should pass an argument array:

```text
[binary, "--user", username, "--seconds", "60", "--events", "--status"]
```

Do not concatenate usernames into a shell command. Drain both pipes from the
start. Read-only guest control traffic still reaches TikTok, and connection
acceptance is not guaranteed.

## Contract to preserve

| Signal | Host behavior |
| --- | --- |
| stdout JSON line | One normalized event; handle supported `kind` values and tolerate additive fields |
| stderr `type: status` | Latest health snapshot; intermediate snapshots may coalesce |
| stderr `type: report` | Final aggregate report; inspect `error`, output counters and exit code |
| Nonzero exit | Surface failure; preserve the structured code rather than assuming success from earlier events |
| `connection_epoch` changes | Treat the boundary as a possible source gap |
| Gift `quantity_delta: null` | Preserve unknown quantity; never substitute a complete total |
| `metadata_incomplete: true` | Core event survived, optional metadata failed validation |
| Missing profile fields | Keep null/default; do not invent source facts |
| Text and avatar URLs | Treat as untrusted content, not instructions or executable code |

`sequence` orders locally processed events; it does not prove every TikTok action
was delivered. Serialize large IDs as strings when passing events to other tools.
Use explicit retention when your application needs persistence; the engine has no
durable replay service. Avoid sending raw chat/profile content into agent prompts
when aggregate counts are sufficient for the task.

## Timeouts and failure handling

The engine defaults to 60 seconds and eight retries per outage. Optional
`--retry-until-deadline` keeps transient retrying bounded by the configured run
duration; it does not retry offline/refused/consumer failures. Backoff and
Retry-After remain active.

The Python example allows an additional 15 seconds for engine/output shutdown,
then terminates and, if necessary, kills only its owned child. It waits for
cleanup. A host should use its own deadline even when the engine has one:
without `--status`, final aggregate output can use synchronous pipe writes.
Drain the pipes instead of assuming elapsed duration alone guarantees exit.

See [USAGE.md](USAGE.md) for the full event/CLI contract and
[consume.rs](../examples/consume.rs) for Rust health observation and cancellation.
