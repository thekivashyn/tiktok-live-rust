# CLI and event reference

Version 0.9.0. Developed by Tri Nguyen.

## Build

Use stable Rust/Cargo and run from the repository root:

```sh
cargo build --release --locked
cargo run --release --locked -- --user USERNAME --seconds 180 --events
```

The executable is `target/release/tiktok-live-rust` (`.exe` on Windows). With a
standard toolchain, no parent workspace or custom wrapper is required. Windows
users with an existing isolated toolchain can use `run.ps1 -CargoArgs ...`,
`-ToolsRoot`, or `TIKTOK_LIVE_RUST_TOOLS_ROOT`. A local `.build-tools-root` can store
that machine-specific directory and must stay ignored.

## Options

| Option | Meaning/default |
| --- | --- |
| `--user USERNAME` | Public username, optionally prefixed with `@`; choose exactly one target |
| `--room ROOM_ID` | Positive canonical numeric room ID, as an alternative to username |
| `--seconds N` | Total connection/setup/backoff budget; 1–86,400, default 60 |
| `--max-reconnects N` | Retry budget per outage; 0–32, default 8 |
| `--retry-until-deadline` | Retry transient failures until duration/cancellation; mutually exclusive with explicit `--max-reconnects` |
| `--events` | Emit normalized NDJSON events on stdout |
| `--status` | Emit tagged NDJSON health/report records on stderr |
| `--lifecycle` | Emit bounded reconnect lifecycle records and explicit lag notifications on stderr; includes tagged final report |
| `--report PATH` | Also save the aggregate report; the parent directory must exist |
| `--help` | Display usage without connecting |
| `--version` | Display package version and developer without connecting |

Ctrl+C cancels. Offline rooms and refusals stop; the engine does not wait forever
for a channel to go LIVE. Retry mode still honors backoff, terminal failures and
Retry-After. A missing/malformed Retry-After on HTTP 429 uses a 60-second cooldown.

The configured duration includes setup and retry waits. Socket close can add two
seconds, event-output drain three, and enabled diagnostics-output drain a further three.

## Output streams and exit status

| Invocation | stdout | stderr |
| --- | --- | --- |
| Default | Aggregate JSON report | Diagnostics if needed |
| `--events` | One event JSON object per line | Final aggregate JSON report |
| `--status` | Aggregate JSON report | Tagged NDJSON status/final-report records |
| `--events --status` | One event JSON object per line | Tagged NDJSON status/final-report records |
| `--lifecycle` | Aggregate JSON report | Tagged NDJSON lifecycle/lag/final-report records |
| `--events --lifecycle` | One event JSON object per line | Tagged NDJSON lifecycle/lag/final-report records |

Status records have `{"type":"status","data":{...}}`; final report records use
`"type":"report"`. They are snapshots, not a lossless transition log. A host
application must drain stdout and stderr concurrently, including when it ignores
health information. Avoid logging raw event streams unless your application
explicitly needs to retain them.

The two observation flags can be combined. `--lifecycle` is independent of the
coalescing status channel and reports reconnects before another application event
arrives. It retains 64 records and explicitly reports overflow. See
[reconnect delivery and recovery](RECONNECTS.md) for the full contract, including
separate-pipe ordering and the limits of upstream gap recovery.

Terminal failures return a nonzero exit status. A duration with no supported
events also fails (`NoEvents`); it means no usable-event proof, not necessarily
that no socket opened. Explicit cancellation or stream end may finish normally
without events. A report-file write failure is surfaced in structured output.

## Events

Common fields: `sequence`, `connection_epoch`, `kind`, `room_id`, `message_id`,
optional `user_id`, optional `user`, optional `like_details`, and `data`.
`metadata_incomplete` appears only when optional metadata validation failed.

| Kind | `data` fields |
| --- | --- |
| `chat` | `text` |
| `gift` | `gift_id`, `group_id`, `repeat_count`, `repeat_end`, `quantity_delta`, `gift_type`, `receiver_id`, `gift_name`, `diamonds_per_gift` |
| `like` | `count`, `total` |
| `follow`, `share`, `social` | `action`, `display_key` |
| `join` | `action` |
| `viewers` | `count` |
| `stream_end` | `action` |

IDs and 64-bit counts/metadata are decimal strings where the Rust serializers
specify them. Sequence and connection epoch are local numeric counters. Consumers
must accept additive fields and preserve source time units.

`user` is a selected profile projection: `id`, `username`, `display_name`,
`avatar_url`, `verified`, `sec_uid`, `follow_info`. Missing values remain
null/default. Names and avatars come from the received payload; the engine does
not fetch profiles or download avatars. Avatar URLs may expire.

`like_details` includes `common`, `color`, `icon`, `icons`,
`specified_display_text`, `effect_cnt`, `like_effect`,
`public_area_message_common`, `room_message_heat_level`. Alongside `data.count`,
`data.total` and `user`, these map the 12 root like fields in the pinned v3
comparison schema. This does not imply the entire upstream User schema is exposed.

Identified empty-text chats remain chats with `text: ""`; the engine does not
invent text or infer an emote. Likes may be aggregated/omitted by TikTok.

## Gift accounting and delivery gaps

`repeat_count` can be cumulative. Use `quantity_delta` for a known increment;
null means unknown, not zero or the full repeat count. After the 4,096-key combo
window overflows, an unseen cumulative count above one establishes a new baseline
with null delta. Later known updates can resume incremental accounting.

Missing gift metadata also leaves quantities unknown. A combo without reliable
identity waits for its final update; a missing final message cannot be recreated.
Gift update counts are not purchased-gift quantities or financial accounting.

Sequence increases within one run. Connection epoch increases on each successful
connection. A new epoch may contain a delivery gap even when local sequences are
contiguous. A different `room_id` resets per-room state. The engine has no replay
service; `gap_recovery` remains false. A process restart loses in-memory state.

## Health and aggregate counters

States: `idle`, `connecting`, `awaiting_events`, `receiving`, `retry_wait`,
`stopping`, `stopped`. Batch reception establishes readiness; pings do not.
Observation timestamps are local, not end-to-end delivery latency. Rust watch
consumers must clone and release a borrow before awaiting or doing slow work.

- `events_processed` and `events` describe processing, including an event that
  could not be queued. They are not consumer acknowledgements.
- `events_enqueued` and `events_written` separate queue acceptance from CLI stdout
  flush. A flush does not prove downstream processing. Library consumers do not
  increment the CLI writer counter.
- `events_unqueued` counts encountered enqueue failures before stopping, not all
  later missed source events. Queue high-watermark is sampled after enqueue.
- `ready_connections` counts connections with webcast batches.
- `possible_gaps` reports interrupted sessions; it cannot count undelivered events.
- `socket_open` means a socket opened during the run, not that one remains on exit.
- `decode_errors`, fixed `decode_failures` categories and
  `metadata_incomplete_events` summarize validation failures without raw payloads.

The final report is authoritative for writer-drain errors; the final health
snapshot precedes those drains. See Rust [`Report`](../src/engine.rs) for the
complete current counter structure.

## Resource bounds

| Boundary | Limit |
| --- | --- |
| HTTP JSON body | 1 MiB |
| WebSocket frame/message | 2 MiB |
| Decompressed batch | 8 MiB |
| Messages per batch | 4,096 |
| Event wire payload | 256 KiB |
| CLI event queue | 256 events |
| Lifecycle observation ring | 64 notifications; explicit lag for a slow observer |
| Dedupe window | 8,192 message IDs |
| Gift-combo window | 4,096 keys |
| Optional metadata wire / aggregate text | 32 KiB / 16 KiB |
| Optional metadata fields / nested nodes / depth | 512 / 128 / 12 |

Additional per-list/string limits apply. These bound individual engine structures,
not the total memory of a host application or an unlimited number of room engines.
