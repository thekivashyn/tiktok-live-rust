//! Explicit live integration probe: cargo run --release --example reconnect_probe -- USERNAME
//! Drops only its own in-memory relay after 20s; never changes system network settings.
//! Aggregates counts only; no chat text, user IDs, cookies or raw frames are saved.
use anyhow::{Context, Result, ensure};
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};
use tiktok_live_rust::{
    engine::{self, Config, Connection, Failure, Source, Target},
    transport::DirectSource,
};
use tokio::{
    io::DuplexStream,
    sync::mpsc,
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

struct DropFirst {
    direct: DirectSource,
    connections: AtomicU32,
    forced_cuts: Arc<AtomicU32>,
}
impl Source for DropFirst {
    type Io = DuplexStream;
    async fn connect(&self, target: &Target) -> Result<Connection<Self::Io>, Failure> {
        let mut direct = self.direct.connect(target).await?;
        let first = self.connections.fetch_add(1, Ordering::Relaxed) == 0;
        let cuts = self.forced_cuts.clone();
        let (client, server) = tokio::io::duplex(128 * 1024);
        let room_id = direct.room_id.clone();
        tokio::spawn(async move {
            let config = WebSocketConfig::default()
                .max_message_size(Some(2 * 1024 * 1024))
                .max_frame_size(Some(2 * 1024 * 1024));
            let mut relay =
                WebSocketStream::from_raw_socket(server, Role::Server, Some(config)).await;
            let cut_at = Instant::now() + Duration::from_secs(20);
            loop {
                tokio::select! { biased;
                    _ = sleep_until(cut_at), if first => {
                        cuts.fetch_add(1, Ordering::Relaxed);
                        break; // Abrupt EOF without a WebSocket close handshake.
                    },
                    incoming = relay.next() => {
                        let Some(Ok(message)) = incoming else { break; };
                        let closed = matches!(message, Message::Close(_));
                        if !matches!(timeout(Duration::from_secs(5), direct.socket.send(message)).await, Ok(Ok(()))) || closed { break; }
                    },
                    incoming = direct.socket.next() => {
                        let Some(Ok(message)) = incoming else { break; };
                        let closed = matches!(message, Message::Close(_));
                        if !matches!(timeout(Duration::from_secs(5), relay.send(message)).await, Ok(Ok(()))) || closed { break; }
                    },
                }
            }
        });
        Ok(Connection {
            room_id,
            socket: WebSocketStream::from_raw_socket(client, Role::Client, None).await,
        })
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let username = std::env::args()
        .nth(1)
        .context("provide a public LIVE username")?;
    let cuts = Arc::new(AtomicU32::new(0));
    let source = DropFirst {
        direct: DirectSource::new()?,
        connections: AtomicU32::new(0),
        forced_cuts: cuts.clone(),
    };
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    let task = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal.cancel();
        }
    });
    let (sender, mut receiver) = mpsc::channel(256);
    let (status, mut observer) = engine::status_channel();
    let running = engine::run_with_status(
        &source,
        Target::User(username),
        Config {
            retry_until_deadline: true,
            ..Config::default()
        },
        Some(sender),
        cancel,
        status,
    );
    let health = async {
        let mut states = Vec::new();
        while observer.changed().await.is_ok() {
            let snapshot = observer.borrow_and_update().clone();
            if states.last() != Some(&snapshot.state) && states.len() < 128 {
                states.push(snapshot.state);
            }
        }
        (states, observer.borrow().clone())
    };
    let consumer = async {
        let mut epochs = BTreeMap::<u32, BTreeMap<String, u64>>::new();
        let mut sequence = 0;
        while let Some(event) = receiver.recv().await {
            sequence += 1;
            ensure!(event.sequence == sequence, "local event sequence gap");
            *epochs
                .entry(event.connection_epoch)
                .or_default()
                .entry(event.kind)
                .or_default() += 1;
        }
        Ok::<_, anyhow::Error>((sequence, epochs))
    };
    let (report, consumed, (states, last_status)) = tokio::join!(running, consumer, health);
    task.abort();
    let (count, epochs) = consumed?;
    let passed = report.error.is_none()
        && cuts.load(Ordering::Relaxed) == 1
        && report.reconnects >= 1
        && report.ready_connections >= 2
        && epochs.contains_key(&1)
        && epochs.contains_key(&2)
        && count == report.events_enqueued
        && states.contains(&engine::ConnectionState::RetryWait)
        && states.contains(&engine::ConnectionState::Receiving)
        && last_status.state == engine::ConnectionState::Stopped
        && last_status.events_enqueued == count;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "passed": passed, "forced_cuts": cuts.load(Ordering::Relaxed), "events_consumed": count,
            "events_by_connection": epochs, "status_transitions": states, "last_status": last_status,
            "report": report, "raw_events_saved": false
        }))?
    );
    ensure!(passed, "live reconnect probe did not meet all checks");
    Ok(())
}
