use anyhow::{Result, ensure};
use std::{
    path::Path,
    time::{Duration, Instant},
};
use tiktok_live_rust::{
    engine::{self, Code, Config, Connection, Failure, Source, Target},
    protocol::MAX_FRAME,
};
use tokio::net::TcpStream;
use tokio_tungstenite::{client_async_with_config, tungstenite::protocol::WebSocketConfig};
use tokio_util::sync::CancellationToken;
#[path = "support/matched.rs"]
mod matched;
struct Local(u16);
impl Source for Local {
    type Io = TcpStream;
    async fn connect(&self, _: &Target) -> std::result::Result<Connection<Self::Io>, Failure> {
        let stream = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, self.0))
            .await
            .map_err(|_| Failure::new(Code::Network, "fixture_connect", true))?;
        stream
            .set_nodelay(true)
            .map_err(|_| Failure::new(Code::Network, "fixture_nodelay", true))?;
        let config = WebSocketConfig::default()
            .read_buffer_size(16384)
            .max_message_size(Some(MAX_FRAME))
            .max_frame_size(Some(MAX_FRAME));
        let (socket, _) =
            client_async_with_config(format!("ws://127.0.0.1:{}/", self.0), stream, Some(config))
                .await
                .map_err(|_| Failure::new(Code::Network, "fixture_handshake", true))?;
        Ok(Connection {
            room_id: "42".into(),
            socket,
        })
    }
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    ensure!(!args.is_empty(), "port [seconds] [capture]");
    let port: u16 = args[0].parse()?;
    let duration: u64 = args.get(1).map(|s| s.parse()).transpose()?.unwrap_or(3600);
    let idle: u64 = std::env::var("TLR_BENCH_IDLE_SECONDS")
        .ok()
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(10);
    let (tx, mut rx) = tokio::sync::mpsc::channel::<tiktok_live_rust::Event>(256);
    let mut output = matched::Output::default();
    if let Some(file) = args.get(2) {
        output.capture = Some(std::fs::File::create(Path::new(file))?);
    }
    let consumer = async {
        while let Some(event) = rx.recv().await {
            ensure!(
                event.sequence == output.events + 1,
                "sequence discontinuity"
            );
            output.push(event)?;
        }
        Ok::<_, anyhow::Error>(output)
    };
    let source = Local(port);
    let start = Instant::now();
    let (status, mut observer) = engine::status_channel();
    let health = async {
        let mut transitions = Vec::new();
        let mut previous = None;
        while observer.changed().await.is_ok() {
            let snapshot = observer.borrow_and_update().clone();
            let identity = (snapshot.state, snapshot.connection_epoch);
            if previous != Some(identity) && transitions.len() < 256 {
                eprintln!(
                    "{}",
                    serde_json::json!({"elapsed_ms":start.elapsed().as_millis(),"status":snapshot})
                );
                transitions.push(
                    serde_json::json!({"elapsed_ms":start.elapsed().as_millis(),"status":snapshot}),
                );
                previous = Some(identity);
            }
        }
        transitions
    };
    let (report, result, transitions) = tokio::join!(
        engine::run_with_status(
            &source,
            Target::Room("42".into()),
            Config {
                duration: Duration::from_secs(duration),
                retry_until_deadline: true,
                heartbeat: Duration::from_secs(1),
                idle_timeout: Duration::from_secs(idle),
                batch_idle_timeout: Duration::from_secs(idle + 5),
                startup_timeout: Duration::from_secs(10),
                healthy_reset_after: Duration::from_secs(2),
                ..Config::default()
            },
            Some(tx),
            CancellationToken::new(),
            status
        ),
        consumer,
        health
    );
    let output = result?;
    println!(
        "{}",
        serde_json::json!({"implementation":"rust","version":env!("CARGO_PKG_VERSION"),"idle_seconds":idle,"elapsed_seconds":start.elapsed().as_secs_f64(),"output":output.summary(),"report":report,"transitions":transitions})
    );
    ensure!(report.error.is_none(), "receiver failed");
    Ok(())
}
