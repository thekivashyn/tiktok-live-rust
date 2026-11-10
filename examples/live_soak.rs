//! Explicit long LIVE test. Stores aggregate diagnostics only; owns and joins its relay tasks.
use anyhow::{Result, ensure};
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};
use tiktok_live_rust::{
    EventData,
    engine::{self, Config, Connection, Failure, Source, Target},
    transport::DirectSource,
};
use tokio::{
    io::DuplexStream,
    sync::{Mutex, mpsc},
    task::JoinSet,
    time::{Instant, sleep_until, timeout},
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        protocol::{Role, WebSocketConfig},
    },
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Relays {
    active: AtomicU32,
    peak: AtomicU32,
    cuts: AtomicU32,
}
#[derive(Default, serde::Serialize)]
struct ProfileCoverage {
    events: u64,
    with_user_id: u64,
    profiles: u64,
    username: u64,
    display_name: u64,
    avatar_url: u64,
    like_details: u64,
}
struct Active(Arc<Relays>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}
struct ProbedSource {
    direct: DirectSource,
    jobs: Mutex<JoinSet<()>>,
    stats: Arc<Relays>,
    cut_after: Duration,
    max_cuts: u32,
}
fn socket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .read_buffer_size(16 * 1024)
        .write_buffer_size(16 * 1024)
        .max_write_buffer_size(64 * 1024)
        .max_message_size(Some(2 * 1024 * 1024))
        .max_frame_size(Some(2 * 1024 * 1024))
}
impl Source for ProbedSource {
    type Io = DuplexStream;
    async fn connect(&self, target: &Target) -> Result<Connection<Self::Io>, Failure> {
        // End and join the previous relay before obtaining another remote socket.
        self.jobs.lock().await.shutdown().await;
        let mut remote = self.direct.connect(target).await?;
        let room_id = remote.room_id.clone();
        let (client, server) = tokio::io::duplex(128 * 1024);
        let mut relay =
            WebSocketStream::from_raw_socket(server, Role::Server, Some(socket_config())).await;
        let stats = self.stats.clone();
        let active = stats.active.fetch_add(1, Ordering::SeqCst) + 1;
        stats.peak.fetch_max(active, Ordering::SeqCst);
        let guard = Active(stats.clone());
        let inject = stats.cuts.load(Ordering::SeqCst) < self.max_cuts;
        let cut_at = Instant::now() + self.cut_after;
        self.jobs.lock().await.spawn(async move {
            let _guard = guard;
            loop {
                tokio::select! { biased;
                    _ = sleep_until(cut_at), if inject => { stats.cuts.fetch_add(1,Ordering::SeqCst); break; },
                    incoming = relay.next() => {
                        let Some(Ok(message)) = incoming else { break; };
                        let closed = matches!(message, Message::Close(_));
                        if !matches!(timeout(Duration::from_secs(5),remote.socket.send(message)).await,Ok(Ok(()))) || closed { break; }
                    },
                    incoming = remote.socket.next() => {
                        let Some(Ok(message)) = incoming else { break; };
                        let closed = matches!(message, Message::Close(_));
                        if !matches!(timeout(Duration::from_secs(5),relay.send(message)).await,Ok(Ok(()))) || closed { break; }
                    },
                }
            }
        });
        Ok(Connection {
            room_id,
            socket: WebSocketStream::from_raw_socket(client, Role::Client, Some(socket_config()))
                .await,
        })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    ensure!(
        !args.is_empty(),
        "username [seconds=2700] [cut_after_seconds=600] [cuts=3]"
    );
    let seconds: u64 = args.get(1).map(|x| x.parse()).transpose()?.unwrap_or(2700);
    let cut_after: u64 = args.get(2).map(|x| x.parse()).transpose()?.unwrap_or(600);
    let max_cuts: u32 = args.get(3).map(|x| x.parse()).transpose()?.unwrap_or(3);
    ensure!(
        cut_after >= 10 && max_cuts <= 32,
        "invalid injection settings"
    );
    let stats = Arc::new(Relays::default());
    let source = ProbedSource {
        direct: DirectSource::new()?,
        jobs: Mutex::new(JoinSet::new()),
        stats: stats.clone(),
        cut_after: Duration::from_secs(cut_after),
        max_cuts,
    };
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    let control = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal.cancel();
        }
    });
    let (tx, mut rx) = mpsc::channel::<tiktok_live_rust::Event>(256);
    let (status, mut observer) = engine::status_channel();
    let started = Instant::now();
    let health = async {
        let mut previous = None;
        let mut last_log = Instant::now();
        let mut transitions = Vec::new();
        while observer.changed().await.is_ok() {
            let snapshot = observer.borrow_and_update().clone();
            let state = (snapshot.state, snapshot.connection_epoch);
            if previous != Some(state) || last_log.elapsed() >= Duration::from_secs(30) {
                let record = serde_json::json!({"elapsed_seconds":started.elapsed().as_secs_f64(),"status":snapshot,"forced_cuts":stats.cuts.load(Ordering::SeqCst),"active_relays":stats.active.load(Ordering::SeqCst)});
                eprintln!("{record}");
                if previous != Some(state) && transitions.len() < 256 {
                    transitions.push(record);
                }
                previous = Some(state);
                last_log = Instant::now();
            }
        }
        (transitions, observer.borrow().clone())
    };
    let consumer = async {
        let mut counts = BTreeMap::<String, u64>::new();
        let mut epochs = BTreeMap::<u32, u64>::new();
        let mut ids = HashSet::new();
        let mut order = VecDeque::new();
        let mut room = String::new();
        let mut sequence = 0;
        let mut last_epoch = 0;
        let mut sequence_errors = 0;
        let mut duplicate_ids = 0;
        let mut invalid_fields = 0;
        let mut known_gift_quantity = 0u64;
        let mut unknown_gift_updates = 0u64;
        let mut recovered_epochs = 0u32;
        let mut profiles = BTreeMap::<String, ProfileCoverage>::new();
        while let Some(event) = rx.recv().await {
            sequence += 1;
            if event.sequence != sequence || event.connection_epoch < last_epoch {
                sequence_errors += 1;
            }
            if event.connection_epoch != last_epoch {
                recovered_epochs += 1;
                last_epoch = event.connection_epoch;
            }
            if event.room_id != room {
                ids.clear();
                order.clear();
                room = event.room_id.clone();
            }
            match event.message_id.parse::<u64>() {
                Ok(0) => {}
                Ok(id) => {
                    if ids.contains(&id) {
                        duplicate_ids += 1;
                    } else {
                        if ids.len() == 8192 {
                            ids.remove(&order.pop_front().unwrap());
                        }
                        ids.insert(id);
                        order.push_back(id);
                    }
                }
                Err(_) => invalid_fields += 1,
            }
            if event
                .user_id
                .as_ref()
                .is_some_and(|id| id.parse::<u64>().is_err())
            {
                invalid_fields += 1;
            }
            let profile = profiles.entry(event.kind.clone()).or_default();
            profile.events += 1;
            profile.with_user_id += u64::from(event.user_id.is_some());
            if let Some(user) = &event.user {
                profile.profiles += 1;
                profile.username += u64::from(user.username.is_some());
                profile.display_name += u64::from(user.display_name.is_some());
                profile.avatar_url += u64::from(user.avatar_url.is_some());
                if event.user_id.as_ref().and_then(|id| id.parse::<u64>().ok()) != Some(user.id) {
                    invalid_fields += 1;
                }
            }
            if event.like_details.is_some() {
                profile.like_details += 1;
                if event.kind != "like" {
                    invalid_fields += 1;
                }
            } else if event.kind == "like" {
                invalid_fields += 1;
            }
            let shape_ok = match &event.data {
                EventData::Chat { text } => {
                    if text.is_empty()
                        && (event.message_id == "0"
                            || event.user_id.as_deref().is_none_or(|id| id == "0"))
                    {
                        invalid_fields += 1;
                    }
                    event.kind == "chat"
                }
                EventData::Gift {
                    repeat_count,
                    quantity_delta,
                    ..
                } => {
                    if let Some(delta) = quantity_delta {
                        known_gift_quantity += u64::from(*delta);
                        if delta > repeat_count {
                            invalid_fields += 1;
                        }
                    } else {
                        unknown_gift_updates += 1;
                    }
                    event.kind == "gift" && *repeat_count > 0
                }
                EventData::Like { .. } => event.kind == "like",
                EventData::Social { .. } => {
                    matches!(event.kind.as_str(), "social" | "follow" | "share")
                }
                EventData::Member { action } => event.kind == "join" && *action == 1,
                EventData::Viewers { .. } => event.kind == "viewers",
                EventData::Control { action } => {
                    event.kind == "stream_end" && matches!(action, 3 | 4)
                }
            };
            if !shape_ok {
                invalid_fields += 1;
            }
            *counts.entry(event.kind).or_default() += 1;
            if epochs.len() < 128 || epochs.contains_key(&event.connection_epoch) {
                *epochs.entry(event.connection_epoch).or_default() += 1;
            }
        }
        (
            sequence,
            counts,
            epochs,
            recovered_epochs,
            sequence_errors,
            duplicate_ids,
            invalid_fields,
            known_gift_quantity,
            unknown_gift_updates,
            profiles,
        )
    };
    let (report, observed, (transitions, last_status)) = tokio::join!(
        engine::run_with_status(
            &source,
            Target::User(args[0].clone()),
            Config {
                duration: Duration::from_secs(seconds),
                retry_until_deadline: true,
                ..Config::default()
            },
            Some(tx),
            cancel,
            status
        ),
        consumer,
        health
    );
    source.jobs.lock().await.shutdown().await;
    control.abort();
    let _ = control.await;
    let (
        count,
        counts,
        epochs,
        recovered_epochs,
        sequence_errors,
        duplicates,
        invalid_fields,
        gift_quantity,
        unknown_gifts,
        profiles,
    ) = observed;
    let cuts = stats.cuts.load(Ordering::SeqCst);
    let checks = serde_json::json!({
        "engine_ok":report.error.is_none(),"duration_completed":report.stop_reason==engine::StopReason::Duration,
        "expected_cuts":cuts==max_cuts,"recovered_after_cuts":report.reconnects>=cuts && recovered_epochs>cuts,
        "event_counts_match":count==report.events_enqueued && counts==report.events && report.events_unqueued==0,
        "sequence_ok":sequence_errors==0,"dedupe_window_ok":duplicates==0,"field_invariants_ok":invalid_fields==0,
        "decode_ok":report.decode_errors==0,"one_remote_connection":stats.peak.load(Ordering::SeqCst)==1,
        "metadata_complete":report.metadata_incomplete_events==0,
        "relay_cleanup":stats.active.load(Ordering::SeqCst)==0,
        "final_status_ok":last_status.state==engine::ConnectionState::Stopped && last_status.events_enqueued==count,
        "events_received":count>0
    });
    let passed = checks.as_object().unwrap().values().all(|x| x == true);
    println!(
        "{}",
        serde_json::json!({"passed":passed,"checks":checks,"report":report,"events_consumed":count,"events_by_connection":epochs,"epochs_with_events":recovered_epochs,"forced_cuts":cuts,"known_gift_quantity":gift_quantity,"unknown_gift_updates":unknown_gifts,"duplicate_ids":duplicates,"sequence_errors":sequence_errors,"invalid_fields":invalid_fields,"profile_coverage":profiles,"peak_relays":stats.peak.load(Ordering::SeqCst),"active_relays_after_stop":stats.active.load(Ordering::SeqCst),"status_transitions":transitions,"last_status":last_status,"raw_events_saved":false,"measurement_scope":"Includes bounded in-memory relay and validation state; not a server-side ground-truth oracle."})
    );
    ensure!(passed, "live soak checks did not all pass");
    Ok(())
}
