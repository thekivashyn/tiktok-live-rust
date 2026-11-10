//! Offline end-to-end supervisor benchmark/soak. No TikTok connection or personal data.
//! Args: max_frames mode[plain|mixed|gzip] pace_ms fault_every duration_seconds.
use anyhow::{Result, ensure};
use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use std::{
    collections::BTreeMap,
    io::Write,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tiktok_live_rust::{
    EventData,
    engine::{self, Config, Connection, Failure, Source, Target},
    protocol::*,
};
use tokio::io::DuplexStream;
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{Message, protocol::Role},
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Latency {
    buckets: Vec<u64>,
    count: u64,
    max_us: u64,
}
impl Latency {
    fn new() -> Self {
        Self {
            buckets: vec![0; 2049],
            ..Self::default()
        }
    }
    fn record(&mut self, elapsed: Duration) {
        let us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        self.buckets[us.min(2048) as usize] += 1;
        self.count += 1;
        self.max_us = self.max_us.max(us);
    }
    fn percentile(&self, percentage: u64) -> u64 {
        let target = (self.count * percentage).div_ceil(100);
        let mut count = 0;
        for (i, n) in self.buckets.iter().enumerate() {
            count += n;
            if count >= target {
                return if i == 2048 { self.max_us } else { i as u64 + 1 };
            }
        }
        0
    }
}
struct Stats {
    active: AtomicU64,
    peak: AtomicU64,
    frames: AtomicU64,
    cuts: AtomicU64,
    replay: AtomicU64,
    failures: AtomicU64,
    latency: Mutex<Latency>,
}
struct PeerGuard(Arc<Stats>);
impl Drop for PeerGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}
struct Fixture {
    stats: Arc<Stats>,
    max_frames: u64,
    mode: String,
    pace: Duration,
    fault_every: u64,
    duration: Duration,
    started: Instant,
    cancel: CancellationToken,
}
fn envelope(method: &str, payload: Vec<u8>, id: u64) -> Envelope {
    Envelope {
        method: method.into(),
        payload,
        msg_id: id,
        is_history: false,
    }
}
fn wire(index: u64, compressed: bool, final_frame: bool) -> Result<Message> {
    let base = index * 16;
    let mut messages = vec![envelope(
        "WebcastChatMessage",
        Chat {
            user: Some(User { id: 7 }),
            content: "synthetic benchmark".into(),
        }
        .encode_to_vec(),
        base,
    )];
    for count in [1, 3] {
        messages.push(envelope(
            "WebcastGiftMessage",
            Gift {
                gift_id: 1,
                repeat_count: count,
                repeat_end: count == 3,
                group_id: index,
                user: Some(User { id: 7 }),
                gift: Some(GiftMeta {
                    kind: 1,
                    diamonds: 1,
                    name: "Rose".into(),
                }),
                receiver_id: "8".into(),
            }
            .encode_to_vec(),
            base + if count == 1 { 1 } else { 2 },
        ));
    }
    messages.push(envelope(
        "WebcastLikeMessage",
        Like {
            count: 2,
            total: index * 2,
            user: Some(User { id: 7 }),
        }
        .encode_to_vec(),
        base + 3,
    ));
    for (offset, key, action) in [(4, "follow", 1), (5, "share", 3)] {
        messages.push(envelope(
            "WebcastSocialMessage",
            Social {
                common: Some(Common {
                    msg_id: base + offset,
                    display_text: Some(DisplayText { key: key.into() }),
                }),
                user: Some(User { id: 7 }),
                action,
            }
            .encode_to_vec(),
            base + offset,
        ));
    }
    messages.push(envelope(
        "WebcastMemberMessage",
        Member {
            user: Some(User { id: 7 }),
            action: 1,
        }
        .encode_to_vec(),
        base + 6,
    ));
    messages.push(envelope(
        "WebcastRoomUserSeqMessage",
        Viewers { count: 123 }.encode_to_vec(),
        base + 7,
    ));
    messages.push(messages[2].clone()); // duplicate gift must not be counted again
    let mut history = messages[0].clone();
    history.msg_id = base + 8;
    history.is_history = true;
    messages.push(history);
    if final_frame {
        messages.push(envelope(
            "WebcastControlMessage",
            Control { action: 3 }.encode_to_vec(),
            u64::MAX,
        ));
    }
    let payload = Batch {
        messages,
        internal_ext: String::new(),
        need_ack: true,
    }
    .encode_to_vec();
    let payload = if compressed {
        let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        gzip.write_all(&payload)?;
        gzip.finish()?
    } else {
        payload
    };
    Ok(Message::Binary(packet("msg", payload, index).into()))
}
async fn acknowledge(socket: &mut WebSocketStream<DuplexStream>, index: u64) -> Result<()> {
    while let Some(message) = socket.next().await {
        match message? {
            Message::Binary(bytes) => {
                let frame = Frame::decode(bytes)?;
                if frame.kind == "ack" {
                    ensure!(frame.log_id == index && frame.payload == b"-", "wrong ACK");
                    return Ok(());
                }
                ensure!(frame.kind == "hb", "unexpected control frame");
            }
            Message::Close(_) => anyhow::bail!("closed before ACK"),
            _ => {}
        }
    }
    anyhow::bail!("EOF before ACK")
}
impl Source for Fixture {
    type Io = DuplexStream;
    async fn connect(&self, _: &Target) -> std::result::Result<Connection<Self::Io>, Failure> {
        let (client, server) = tokio::io::duplex(16 * 1024);
        let stats = self.stats.clone();
        let cancel = self.cancel.clone();
        let mode = self.mode.clone();
        let pace = self.pace;
        let max = self.max_frames;
        let fault = self.fault_every;
        let started = self.started;
        let duration = self.duration;
        let active = stats.active.fetch_add(1, Ordering::SeqCst) + 1;
        stats.peak.fetch_max(active, Ordering::SeqCst);
        tokio::spawn(async move {
            let _guard = PeerGuard(stats.clone());
            let work = async {
                let mut socket = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
                let entry = socket
                    .next()
                    .await
                    .ok_or_else(|| anyhow::anyhow!("missing entry"))??;
                ensure!(matches!(entry, Message::Binary(_)), "entry must be binary");
                let replay = stats.replay.swap(0, Ordering::SeqCst);
                if replay != 0 {
                    socket
                        .send(wire(
                            replay,
                            mode == "gzip" || (mode == "mixed" && replay.is_multiple_of(2)),
                            false,
                        )?)
                        .await?;
                    acknowledge(&mut socket, replay).await?;
                }
                loop {
                    if !pace.is_zero() {
                        tokio::time::sleep(pace).await;
                    }
                    let index = stats.frames.fetch_add(1, Ordering::SeqCst) + 1;
                    let final_frame =
                        index >= max || (!duration.is_zero() && started.elapsed() >= duration);
                    let message = wire(
                        index,
                        mode == "gzip" || (mode == "mixed" && index.is_multiple_of(2)),
                        final_frame,
                    )?;
                    let sent = Instant::now();
                    socket.send(message).await?;
                    if !final_frame && fault != 0 && index.is_multiple_of(fault) {
                        // Abrupt EOF immediately after write, before ACK: reconnect must replay safely.
                        stats.replay.store(index, Ordering::SeqCst);
                        stats.cuts.fetch_add(1, Ordering::SeqCst);
                        return Ok(());
                    }
                    acknowledge(&mut socket, index).await?;
                    stats.latency.lock().unwrap().record(sent.elapsed());
                    if final_frame {
                        return Ok(());
                    }
                }
            };
            tokio::select! { biased; _=cancel.cancelled()=>{}, result=work=> { if result.is_err(){stats.failures.fetch_add(1,Ordering::SeqCst);} } }
        });
        Ok(Connection {
            room_id: "42".into(),
            socket: WebSocketStream::from_raw_socket(client, Role::Client, None).await,
        })
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let number = |i: usize, default: u64| -> Result<u64> {
        Ok(args
            .get(i)
            .map(|s| s.parse())
            .transpose()?
            .unwrap_or(default))
    };
    let frames = number(0, 20_000)?;
    let mode = args.get(1).map(String::as_str).unwrap_or("mixed");
    let pace = number(2, 0)?;
    let fault = number(3, 0)?;
    let duration = number(4, 0)?;
    ensure!(
        (1..=2_000_000).contains(&frames)
            && matches!(mode, "plain" | "mixed" | "gzip")
            && pace <= 100
            && duration <= 3600,
        "invalid benchmark options"
    );
    let stats = Arc::new(Stats {
        active: AtomicU64::new(0),
        peak: AtomicU64::new(0),
        frames: AtomicU64::new(0),
        cuts: AtomicU64::new(0),
        replay: AtomicU64::new(0),
        failures: AtomicU64::new(0),
        latency: Mutex::new(Latency::new()),
    });
    let fixture = Fixture {
        stats: stats.clone(),
        max_frames: frames,
        mode: mode.into(),
        pace: Duration::from_millis(pace),
        fault_every: fault,
        duration: Duration::from_secs(duration),
        started: Instant::now(),
        cancel: CancellationToken::new(),
    };
    let (tx, mut rx) = tokio::sync::mpsc::channel::<tiktok_live_rust::Event>(256);
    let consume = async {
        let mut count = 0;
        let mut gifts = 0;
        let mut kinds = BTreeMap::<String, u64>::new();
        while let Some(event) = rx.recv().await {
            count += 1;
            ensure!(event.sequence == count, "sequence mismatch");
            if let EventData::Gift { quantity_delta, .. } = event.data {
                gifts += u64::from(quantity_delta.unwrap_or(0));
            }
            *kinds.entry(event.kind).or_default() += 1;
        }
        Ok::<_, anyhow::Error>((count, gifts, kinds))
    };
    let start = Instant::now();
    let (report, consumed) = tokio::join!(
        engine::run(
            &fixture,
            Target::Room("42".into()),
            Config {
                duration: Duration::from_secs(if duration == 0 { 120 } else { duration + 120 }),
                retry_until_deadline: true,
                ..Config::default()
            },
            Some(tx),
            CancellationToken::new()
        ),
        consume
    );
    let elapsed = start.elapsed().as_secs_f64();
    fixture.cancel.cancel();
    tokio::time::timeout(Duration::from_secs(2), async {
        while stats.active.load(Ordering::SeqCst) > 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let (events, gifts, kinds) = consumed?;
    let sent = stats.frames.load(Ordering::SeqCst);
    ensure!(
        report.error.is_none()
            && events == sent * 8 + 1
            && gifts == sent * 3
            && report.events_enqueued == events
            && stats.failures.load(Ordering::SeqCst) == 0,
        "event or fixture failure: {report:?}"
    );
    let latency = stats.latency.lock().unwrap();
    println!(
        "{}",
        serde_json::to_string_pretty(
            &serde_json::json!({"version":env!("CARGO_PKG_VERSION"),"mode":mode,"frames":sent,"events":events,"gift_quantity":gifts,"event_counts":kinds,"elapsed_seconds":elapsed,"events_per_second":events as f64/elapsed,
        "ack_latency_us_upper":{"p50":latency.percentile(50),"p95":latency.percentile(95),"p99":latency.percentile(99),"max":latency.max_us},"latency_histogram_overflow":latency.buckets[2048],
        "forced_cuts":stats.cuts.load(Ordering::SeqCst),"server_tasks_after_stop":stats.active.load(Ordering::SeqCst),"peak_server_tasks":stats.peak.load(Ordering::SeqCst),"report":report,
        "limits":"Offline in-memory fixture includes generation, WebSocket, normalization, bounded output and ACK; latency excludes fixture generation and measures send-to-ACK, not TikTok delivery. Fixed-size latency histogram; overflow percentiles use observed maximum."})
        )?
    );
    Ok(())
}
