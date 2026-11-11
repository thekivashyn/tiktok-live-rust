use crate::lifecycle::{LifecycleKind, LifecycleSender};
use crate::{Event, Processor, protocol};
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, watch},
    time::{Instant, sleep_until, timeout},
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{self, Message},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Code {
    InvalidInput,
    Network,
    Timeout,
    Offline,
    Refused,
    RateLimited,
    Protocol,
    Capacity,
    Decode,
    ConsumerSlow,
    ConsumerClosed,
    Output,
    RetryExhausted,
    NoEvents,
}

/// Deliberately contains no remote strings, URL, body, cookie, or underlying error chain.
#[derive(Clone, Debug, Serialize)]
pub struct Failure {
    pub code: Code,
    pub stage: &'static str,
    pub retryable: bool,
    pub retry_after_ms: Option<u64>,
}
impl Failure {
    pub fn new(code: Code, stage: &'static str, retryable: bool) -> Self {
        Self {
            code,
            stage,
            retryable,
            retry_after_ms: None,
        }
    }
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} at {}", self.code, self.stage)
    }
}
impl std::error::Error for Failure {}

#[derive(Clone, Debug)]
pub enum Target {
    User(String),
    Room(String),
}
impl Target {
    pub fn validate(&self) -> Result<(), Failure> {
        match self {
            Self::User(name) => {
                let name = name.strip_prefix('@').unwrap_or(name);
                if name.is_empty()
                    || name.len() > 32
                    || !name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_')
                {
                    return Err(Failure::new(Code::InvalidInput, "username", false));
                }
            }
            Self::Room(id) => {
                room_number(id)?;
            }
        }
        Ok(())
    }
}
pub fn room_number(value: &str) -> Result<u64, Failure> {
    let invalid = || Failure::new(Code::InvalidInput, "room_id", false);
    if value.is_empty() || value.len() > 20 || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid());
    }
    let id = value.parse::<u64>().map_err(|_| invalid())?;
    if id == 0 || id.to_string() != value {
        return Err(invalid());
    }
    Ok(id)
}

#[derive(Clone, Debug)]
pub struct Config {
    pub duration: Duration,
    pub max_reconnects: u32,
    /// Retry transient failures until duration/cancellation instead of a retry-count limit.
    /// Backoff and server Retry-After still apply; terminal errors are never retried.
    pub retry_until_deadline: bool,
    pub heartbeat: Duration,
    pub idle_timeout: Duration,
    pub write_timeout: Duration,
    pub connect_timeout: Duration,
    /// Require a webcast batch after opening the socket; pings alone are insufficient.
    pub startup_timeout: Duration,
    /// Replenish retry budget only after a connection has received valid traffic this long.
    pub healthy_reset_after: Duration,
    pub batch_idle_timeout: Duration,
    /// Shared enqueue deadline per batch, so transient bursts can drain without stalling heartbeats.
    pub output_grace: Duration,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(60),
            max_reconnects: 8,
            retry_until_deadline: false,
            heartbeat: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(60),
            write_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(45),
            startup_timeout: Duration::from_secs(30),
            healthy_reset_after: Duration::from_secs(30),
            batch_idle_timeout: Duration::from_secs(120),
            output_grace: Duration::from_millis(250),
        }
    }
}
impl Config {
    pub fn validate(&self) -> Result<(), Failure> {
        if self.duration < Duration::from_secs(1)
            || self.duration > Duration::from_secs(86_400)
            || self.max_reconnects > 32
            || self.heartbeat < Duration::from_secs(1)
            || self.heartbeat > Duration::from_secs(30)
            || self.idle_timeout < self.heartbeat * 2
            || self.idle_timeout > Duration::from_secs(300)
            || self.write_timeout.is_zero()
            || self.write_timeout > Duration::from_secs(15)
            || self.connect_timeout.is_zero()
            || self.connect_timeout > Duration::from_secs(60)
            || self.startup_timeout < Duration::from_secs(1)
            || self.startup_timeout > Duration::from_secs(120)
            || self.healthy_reset_after < self.heartbeat * 2
            || self.healthy_reset_after > Duration::from_secs(600)
            || self.batch_idle_timeout < self.idle_timeout
            || self.batch_idle_timeout > Duration::from_secs(600)
            || self.output_grace > Duration::from_secs(1)
            || self.output_grace >= self.heartbeat
        {
            return Err(Failure::new(Code::InvalidInput, "configuration", false));
        }
        Ok(())
    }
}

pub struct Connection<S> {
    pub room_id: String,
    pub socket: WebSocketStream<S>,
}
pub trait Source: Send + Sync {
    type Io: AsyncRead + AsyncWrite + Unpin + Send;
    fn connect(
        &self,
        target: &Target,
    ) -> impl Future<Output = Result<Connection<Self::Io>, Failure>> + Send;
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    Duration,
    Cancelled,
    StreamEnded,
    Failed,
}

#[derive(Clone, Copy, Debug, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    #[default]
    Idle,
    Connecting,
    AwaitingEvents,
    Receiving,
    RetryWait,
    Stopping,
    Stopped,
}

/// Latest health snapshot, not a lossless transition log. It contains no event content.
/// Receive times cover the whole run and are local observation times, not source latency.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ConnectionStatus {
    pub state: ConnectionState,
    pub state_since_unix_ms: u64,
    pub observed_at_unix_ms: u64,
    pub room_id: String,
    pub connection_epoch: u32,
    pub reconnects: u32,
    pub ready_connections: u32,
    pub possible_gaps: u32,
    pub events_processed: u64,
    pub events_enqueued: u64,
    pub decode_errors: u64,
    pub decode_failures: DecodeFailures,
    pub last_protocol_at_unix_ms: Option<u64>,
    pub last_batch_at_unix_ms: Option<u64>,
    pub last_event_at_unix_ms: Option<u64>,
    pub retry_delay_ms: Option<u64>,
    pub retry_not_before_unix_ms: Option<u64>,
    pub stop_reason: Option<StopReason>,
    pub last_connection_error: Option<Failure>,
    pub error: Option<Failure>,
}

/// A watch channel retains one snapshot even when the observer is slow or disconnected.
pub fn status_channel() -> (
    watch::Sender<ConnectionStatus>,
    watch::Receiver<ConnectionStatus>,
) {
    watch::channel(ConnectionStatus::default())
}

fn publish_status(
    sender: Option<&watch::Sender<ConnectionStatus>>,
    report: &Report,
    state: ConnectionState,
    retry_delay: Option<Duration>,
) {
    let Some(sender) = sender else { return };
    let now = unix_ms();
    sender.send_modify(|status| {
        if status.state != state || status.state_since_unix_ms == 0 {
            status.state_since_unix_ms = now;
        }
        status.state = state;
        status.observed_at_unix_ms = now;
        if status.room_id != report.room_id {
            status.room_id.clone_from(&report.room_id);
        }
        status.connection_epoch = report.connections;
        status.reconnects = report.reconnects;
        status.ready_connections = report.ready_connections;
        status.possible_gaps = report.possible_gaps;
        status.events_processed = report.events_processed;
        status.events_enqueued = report.events_enqueued;
        status.decode_errors = report.decode_errors;
        status.decode_failures.clone_from(&report.decode_failures);
        status.last_protocol_at_unix_ms = report.last_protocol_at_unix_ms;
        status.last_batch_at_unix_ms = report.last_batch_at_unix_ms;
        status.last_event_at_unix_ms = report.last_event_at_unix_ms;
        status.retry_delay_ms = retry_delay.map(|d| d.as_millis().min(u64::MAX as u128) as u64);
        status.retry_not_before_unix_ms = status.retry_delay_ms.map(|d| now.saturating_add(d));
        status.stop_reason =
            (state == ConnectionState::Stopped).then(|| report.stop_reason.clone());
        status
            .last_connection_error
            .clone_from(&report.last_connection_error);
        status.error.clone_from(&report.error);
    });
}

/// Eleven counters (88 bytes), independent of input values or the number of errors.
#[derive(Clone, Debug, Default, Serialize, PartialEq, Eq)]
pub struct DecodeFailures {
    pub frame: u64,
    pub chat_empty: u64,
    pub chat_limit: u64,
    pub chat_wire: u64,
    pub gift: u64,
    pub like: u64,
    pub social: u64,
    pub member: u64,
    pub viewers: u64,
    pub control: u64,
    pub other: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Report {
    pub engine: &'static str,
    pub developer: &'static str,
    pub version: &'static str,
    pub transport: &'static str,
    pub started_at_unix_ms: u64,
    pub elapsed_ms: u64,
    pub retry_until_deadline: bool,
    pub last_protocol_at_unix_ms: Option<u64>,
    pub last_batch_at_unix_ms: Option<u64>,
    pub last_event_at_unix_ms: Option<u64>,
    pub room_id: String,
    pub connections: u32,
    pub reconnects: u32,
    pub retry_budget_resets: u32,
    pub ready_connections: u32,
    pub room_changes: u32,
    pub socket_open: bool,
    pub room_response_seen: bool,
    pub frames: u64,
    pub bytes: u64,
    pub decode_errors: u64,
    /// Fixed-size, content-free breakdown of rejected frames/events.
    pub decode_failures: DecodeFailures,
    pub decode_inline_frames: u64,
    pub decode_worker_frames: u64,
    pub decoder_jobs_drained_on_stop: u32,
    /// Ready socket reads consumed once before acting on an expired health deadline.
    pub watchdog_ready_reads: u64,
    /// Identified chat events whose source text was empty; no text is synthesized.
    pub empty_chats: u64,
    pub metadata_incomplete_events: u64,
    pub duplicates: u64,
    pub history_skipped: u64,
    pub gift_streak_evictions: u64,
    pub gift_streak_resyncs: u64,
    pub possible_gaps: u32,
    pub gap_recovery: bool,
    pub events_enqueued: u64,
    pub events_processed: u64,
    pub events_written: u64,
    pub events_unqueued: u64,
    pub queue_high_watermark: usize,
    pub messages: BTreeMap<String, u64>,
    pub events: BTreeMap<String, u64>,
    pub stop_reason: StopReason,
    pub last_connection_error: Option<Failure>,
    pub error: Option<Failure>,
}
impl Default for Report {
    fn default() -> Self {
        Self {
            engine: env!("CARGO_PKG_NAME"),
            developer: env!("CARGO_PKG_AUTHORS"),
            version: env!("CARGO_PKG_VERSION"),
            transport: "direct_guest_unsigned",
            started_at_unix_ms: unix_ms(),
            elapsed_ms: 0,
            retry_until_deadline: false,
            last_protocol_at_unix_ms: None,
            last_batch_at_unix_ms: None,
            last_event_at_unix_ms: None,
            room_id: String::new(),
            connections: 0,
            reconnects: 0,
            retry_budget_resets: 0,
            ready_connections: 0,
            room_changes: 0,
            socket_open: false,
            room_response_seen: false,
            frames: 0,
            bytes: 0,
            decode_errors: 0,
            decode_failures: DecodeFailures::default(),
            decode_inline_frames: 0,
            decode_worker_frames: 0,
            decoder_jobs_drained_on_stop: 0,
            watchdog_ready_reads: 0,
            empty_chats: 0,
            metadata_incomplete_events: 0,
            duplicates: 0,
            history_skipped: 0,
            gift_streak_evictions: 0,
            gift_streak_resyncs: 0,
            possible_gaps: 0,
            gap_recovery: false,
            events_enqueued: 0,
            events_processed: 0,
            events_written: 0,
            events_unqueued: 0,
            queue_high_watermark: 0,
            messages: BTreeMap::new(),
            events: BTreeMap::new(),
            stop_reason: StopReason::Duration,
            last_connection_error: None,
            error: None,
        }
    }
}
pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
pub fn backoff(attempt: u32, jitter_ms: u64) -> Duration {
    Duration::from_millis(
        (1000u64.saturating_mul(1 << attempt.min(5))).min(30_000) + jitter_ms.min(999),
    )
}

pub fn websocket_error(error: tungstenite::Error, stage: &'static str) -> Failure {
    use tungstenite::Error;
    match error {
        Error::Http(response) => http_failure(
            response.status().as_u16(),
            response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok()),
            stage,
        ),
        Error::Capacity(_) | Error::WriteBufferFull(_) => {
            Failure::new(Code::Capacity, stage, false)
        }
        Error::Protocol(tungstenite::error::ProtocolError::ResetWithoutClosingHandshake) => {
            Failure::new(Code::Network, stage, true)
        }
        Error::Protocol(_) | Error::Utf8(_) | Error::AttackAttempt => {
            Failure::new(Code::Protocol, stage, false)
        }
        Error::Tls(_) => Failure::new(Code::Refused, "tls", false),
        _ => Failure::new(Code::Network, stage, true),
    }
}
pub fn http_failure(status: u16, retry_after: Option<&str>, stage: &'static str) -> Failure {
    let code = if status == 429 {
        Code::RateLimited
    } else if status >= 500 {
        Code::Network
    } else {
        Code::Refused
    };
    let mut error = Failure::new(
        code,
        stage,
        status == 429 || matches!(status, 500 | 502 | 503 | 504),
    );
    if error.retryable {
        error.retry_after_ms = retry_after.and_then(|value| {
            if let Ok(seconds) = value.trim().parse::<u64>() {
                Some(seconds.saturating_mul(1000))
            } else {
                httpdate::parse_http_date(value).ok().map(|date| {
                    date.duration_since(SystemTime::now())
                        .unwrap_or_default()
                        .as_millis()
                        .min(u64::MAX as u128) as u64
                })
            }
        });
    }
    // A rate-limit response without a usable server hint is not an invitation to retry
    // immediately. Keep a conservative wait, bounded by the run/cancel deadline.
    if status == 429 && error.retry_after_ms.is_none() {
        error.retry_after_ms = Some(60_000);
    }
    error
}

pub async fn run<S: Source>(
    source: &S,
    target: Target,
    config: Config,
    output: Option<mpsc::Sender<Event>>,
    cancel: CancellationToken,
) -> Report {
    run_observed(source, target, config, output, cancel, Observers::default()).await
}

/// Same event contract as run(), with optional UI/host observation through a bounded
/// latest-value channel. Clone/drop watch borrows promptly; never hold one across await.
pub async fn run_with_status<S: Source>(
    source: &S,
    target: Target,
    config: Config,
    output: Option<mpsc::Sender<Event>>,
    cancel: CancellationToken,
    status: watch::Sender<ConnectionStatus>,
) -> Report {
    run_observed(
        source,
        target,
        config,
        output,
        cancel,
        Observers {
            status: Some(status),
            lifecycle: None,
        },
    )
    .await
}

/// Optional observations. Use a fresh lifecycle channel for each run.
#[derive(Default)]
pub struct Observers {
    pub status: Option<watch::Sender<ConnectionStatus>>,
    pub lifecycle: Option<LifecycleSender>,
}

/// Same event/retry contract as `run`, with bounded lifecycle and/or health observation.
pub async fn run_observed<S: Source>(
    source: &S,
    target: Target,
    config: Config,
    output: Option<mpsc::Sender<Event>>,
    cancel: CancellationToken,
    observers: Observers,
) -> Report {
    let Observers { status, lifecycle } = observers;
    let mut report = Report {
        retry_until_deadline: config.retry_until_deadline,
        ..Report::default()
    };
    if let Some(status) = &status {
        status.send_replace(ConnectionStatus::default());
    }
    if let Err(error) = target.validate().and_then(|_| config.validate()) {
        report.error = Some(error);
        report.stop_reason = StopReason::Failed;
        publish_status(status.as_ref(), &report, ConnectionState::Stopped, None);
        emit_lifecycle(
            lifecycle.as_ref(),
            LifecycleKind::Stopped,
            &report,
            None,
            report.error.as_ref(),
        );
        return report;
    }
    let started = Instant::now();
    let deadline = started + config.duration;
    let mut processor: Option<Processor> = None;
    let mut failure: Option<Failure> = None;
    let mut retry_attempt = 0u32;
    loop {
        if let Some(error) = failure.take() {
            report.last_connection_error = Some(error.clone());
            if !error.retryable {
                report.error = Some(error);
                break;
            }
            if !config.retry_until_deadline && retry_attempt >= config.max_reconnects {
                report.error = Some(Failure::new(Code::RetryExhausted, "reconnect", false));
                break;
            }
            let delay = backoff(retry_attempt, rand::random_range(0..1000)).max(
                Duration::from_millis(error.retry_after_ms.unwrap_or_default()),
            );
            // Never shorten Retry-After to fit our deadline or backoff cap.
            let wake = Instant::now()
                .checked_add(delay)
                .unwrap_or(deadline)
                .min(deadline);
            publish_status(
                status.as_ref(),
                &report,
                ConnectionState::RetryWait,
                Some(delay),
            );
            emit_lifecycle(
                lifecycle.as_ref(),
                LifecycleKind::RetryScheduled,
                &report,
                Some(delay.as_millis().min(u64::MAX as u128) as u64),
                Some(&error),
            );
            tokio::select! { biased;
                _ = cancel.cancelled() => { report.stop_reason = StopReason::Cancelled; break; },
                _ = sleep_until(deadline) => { report.error = Some(error.clone()); break; },
                _ = sleep_until(wake) => {},
            }
            if Instant::now() >= deadline {
                report.error = Some(error);
                break;
            }
            report.reconnects += 1;
            retry_attempt = retry_attempt.saturating_add(1);
        }
        publish_status(status.as_ref(), &report, ConnectionState::Connecting, None);
        emit_lifecycle(
            lifecycle.as_ref(),
            LifecycleKind::Connecting,
            &report,
            None,
            None,
        );
        let connected = tokio::select! { biased;
            _ = cancel.cancelled() => { report.stop_reason = StopReason::Cancelled; break; },
            _ = sleep_until(deadline) => { report.error = Some(Failure::new(Code::Timeout, "run_deadline_connect", false)); break; },
            result = timeout(config.connect_timeout, source.connect(&target)) => match result {
                Ok(result) => result,
                Err(_) => Err(Failure::new(Code::Timeout, "connect", true)),
            },
        };
        let mut connection = match connected {
            Ok(connection) => connection,
            Err(error) => {
                failure = Some(error);
                continue;
            }
        };
        let room = match room_number(&connection.room_id) {
            Ok(room) => room,
            Err(error) => {
                report.error = Some(error);
                break;
            }
        };
        report.socket_open = true;
        report.connections += 1;
        if report.room_id != connection.room_id {
            if processor.is_some() {
                report.room_changes += 1;
            }
            processor = Some(Processor::new(&connection.room_id));
            report.room_id = connection.room_id.clone();
        }
        let p = processor
            .as_mut()
            .expect("processor set after room validation");
        let before = (
            p.duplicates,
            p.history_skipped,
            p.streak_evictions,
            p.streak_resyncs,
        );
        publish_status(
            status.as_ref(),
            &report,
            ConnectionState::AwaitingEvents,
            None,
        );
        emit_lifecycle(
            lifecycle.as_ref(),
            LifecycleKind::Connected,
            &report,
            None,
            None,
        );
        let mut health = SessionHealth {
            status: status.clone(),
            lifecycle: lifecycle.clone(),
            ..SessionHealth::default()
        };
        let outcome = tokio::select! { biased;
            _ = cancel.cancelled() => { report.stop_reason = StopReason::Cancelled; Ok(false) },
            _ = sleep_until(deadline) => Ok(false),
            result = session(&mut connection.socket, room, p, &config, output.as_ref(), &mut report, &mut health) => result,
        };
        report.duplicates += p.duplicates - before.0;
        report.history_skipped += p.history_skipped - before.1;
        report.gift_streak_evictions += p.streak_evictions - before.2;
        report.gift_streak_resyncs += p.streak_resyncs - before.3;
        publish_status(status.as_ref(), &report, ConnectionState::Stopping, None);
        if let Err(error) = &outcome {
            report.possible_gaps += 1;
            report.last_connection_error = Some(error.clone());
            emit_lifecycle(
                lifecycle.as_ref(),
                LifecycleKind::Interrupted,
                &report,
                None,
                Some(error),
            );
        }
        if health.decoder.shutdown().await {
            report.decoder_jobs_drained_on_stop += 1;
        }
        let _ = timeout(Duration::from_secs(2), connection.socket.close(None)).await;
        match outcome {
            Ok(ended) => {
                if ended {
                    report.stop_reason = StopReason::StreamEnded;
                }
                break;
            }
            Err(error) => {
                if retry_attempt > 0 && health.was_stable(config.healthy_reset_after) {
                    retry_attempt = 0;
                    report.retry_budget_resets += 1;
                }
                failure = Some(error);
            }
        }
    }
    if report.error.is_none()
        && report.stop_reason == StopReason::Duration
        && report.events.values().sum::<u64>() == 0
    {
        report.error = Some(Failure::new(Code::NoEvents, "observation", false));
    }
    if report.error.is_some() {
        report.stop_reason = StopReason::Failed;
    }
    if output.is_some() {
        // Includes an in-flight enqueue cancelled by shutdown or the overall deadline.
        report.events_unqueued = report
            .events_processed
            .saturating_sub(report.events_enqueued);
    }
    report.elapsed_ms = started.elapsed().as_millis() as u64;
    publish_status(status.as_ref(), &report, ConnectionState::Stopped, None);
    emit_lifecycle(
        lifecycle.as_ref(),
        LifecycleKind::Stopped,
        &report,
        None,
        report.error.as_ref(),
    );
    report
}

fn emit_lifecycle(
    sender: Option<&LifecycleSender>,
    kind: LifecycleKind,
    report: &Report,
    delay: Option<u64>,
    error: Option<&Failure>,
) {
    if let Some(sender) = sender {
        sender.emit(kind, report, delay, error);
    }
}

async fn send<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut WebSocketStream<S>,
    message: Message,
    config: &Config,
) -> Result<(), Failure> {
    timeout(config.write_timeout, socket.send(message))
        .await
        .map_err(|_| Failure::new(Code::Timeout, "socket_write", true))?
        .map_err(|error| websocket_error(error, "socket_write"))
}
fn count_method(report: &mut Report, method: &str) {
    let key = if method.len() > 128
        || !method
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        "_invalid"
    } else if report.messages.contains_key(method) || report.messages.len() < 64 {
        method
    } else {
        "_other"
    };
    *report.messages.entry(key.into()).or_default() += 1;
}
async fn session<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut WebSocketStream<S>,
    room: u64,
    processor: &mut Processor,
    config: &Config,
    output: Option<&mpsc::Sender<Event>>,
    report: &mut Report,
    health: &mut SessionHealth,
) -> Result<bool, Failure> {
    send(
        socket,
        Message::Binary(protocol::enter(room).into()),
        config,
    )
    .await?;
    let mut heartbeat =
        tokio::time::interval_at(Instant::now() + config.heartbeat, config.heartbeat);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // No timer polling when status observation is disabled. Progress is coalesced to 1 Hz;
    // state transitions are published immediately, independently of the event queue.
    let mut status_tick = tokio::time::interval_at(
        Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    );
    status_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let opened = Instant::now();
    let mut last_received = opened;
    let mut bad_frames = 0;
    let mut probed_deadline = None;
    loop {
        let mut watchdog = (last_received + config.idle_timeout, "socket_idle");
        let health_deadline = if health.ready_at.is_none() {
            (opened + config.startup_timeout, "room_startup")
        } else {
            (
                health.last_batch.unwrap_or(opened) + config.batch_idle_timeout,
                "stream_idle",
            )
        };
        if health_deadline.0 <= watchdog.0 {
            watchdog = health_deadline;
        }
        let incoming = tokio::select! {
            biased;
            _ = sleep_until(watchdog.0) => {
                // A delayed task can resume with both a timer and already-buffered data
                // ready. Give that data one non-waiting read before discarding the socket.
                // The same deadline cannot be probed twice: pings, unknown or malformed
                // frames must not keep an otherwise stalled connection alive forever.
                if probed_deadline == Some(watchdog.0) {
                    return Err(Failure::new(Code::Timeout, watchdog.1, true));
                }
                probed_deadline = Some(watchdog.0);
                match socket.next().now_or_never() {
                    Some(value) => { report.watchdog_ready_reads += 1; value },
                    None => return Err(Failure::new(Code::Timeout, watchdog.1, true)),
                }
            },
            _ = heartbeat.tick() => {
                send(socket, Message::Binary(protocol::heartbeat(room).into()), config).await?;
                continue;
            },
            _ = status_tick.tick(), if health.status.is_some() => {
                publish_status(health.status.as_ref(), report,
                    if health.ready_at.is_some() { ConnectionState::Receiving } else { ConnectionState::AwaitingEvents }, None);
                continue;
            },
            incoming = socket.next() => incoming,
        };
        let message = incoming
            .ok_or_else(|| Failure::new(Code::Network, "socket_eof", true))?
            .map_err(|error| websocket_error(error, "socket_read"))?;
        match message {
            Message::Binary(bytes) => {
                report.frames += 1;
                report.bytes += bytes.len() as u64;
                let decoded = health.decoder.decode(bytes, report).await?;
                let (frame, batch) = match decoded {
                    Ok(value) => {
                        bad_frames = 0;
                        value
                    }
                    Err(_) => {
                        report.decode_errors += 1;
                        report.decode_failures.frame += 1;
                        bad_frames += 1;
                        if bad_frames >= 8 {
                            return Err(Failure::new(Code::Decode, "consecutive_frames", false));
                        }
                        continue;
                    }
                };
                if frame.kind == "im_enter_room_resp" {
                    report.room_response_seen = true;
                }
                let received_at = unix_ms();
                if matches!(frame.kind.as_str(), "hb" | "im_enter_room_resp") || batch.is_some() {
                    last_received = Instant::now();
                    report.last_protocol_at_unix_ms = Some(received_at);
                }
                let Some(batch) = batch else {
                    continue;
                };
                if batch.has_webcast_messages() {
                    let now = Instant::now();
                    health.last_batch = Some(now);
                    report.last_batch_at_unix_ms = Some(received_at);
                    if health.ready_at.is_none() {
                        health.ready_at = Some(now);
                        report.ready_connections += 1;
                        emit_lifecycle(
                            health.lifecycle.as_ref(),
                            LifecycleKind::Ready,
                            report,
                            None,
                            None,
                        );
                        publish_status(
                            health.status.as_ref(),
                            report,
                            ConnectionState::Receiving,
                            None,
                        );
                    }
                }
                let mut stream_ended = false;
                let output_deadline = Instant::now() + config.output_grace;
                for raw in batch.messages() {
                    let raw =
                        raw.map_err(|_| Failure::new(Code::Decode, "validated_batch", false))?;
                    count_method(report, raw.method);
                    let mut event = match processor.process_ref(raw) {
                        Ok(Some(event)) => event,
                        Ok(None) => continue,
                        Err(error) => {
                            report.decode_errors += 1;
                            let counter = match raw.method {
                                "WebcastChatMessage" => match error.to_string().as_str() {
                                    "empty chat content" => &mut report.decode_failures.chat_empty,
                                    "chat content exceeds limit" => {
                                        &mut report.decode_failures.chat_limit
                                    }
                                    _ => &mut report.decode_failures.chat_wire,
                                },
                                "WebcastGiftMessage" => &mut report.decode_failures.gift,
                                "WebcastLikeMessage" => &mut report.decode_failures.like,
                                "WebcastSocialMessage" => &mut report.decode_failures.social,
                                "WebcastMemberMessage" => &mut report.decode_failures.member,
                                "WebcastRoomUserSeqMessage" => &mut report.decode_failures.viewers,
                                "WebcastControlMessage" => &mut report.decode_failures.control,
                                _ => &mut report.decode_failures.other,
                            };
                            *counter += 1;
                            continue;
                        }
                    };
                    if matches!(&event.data, crate::EventData::Chat { text } if text.is_empty()) {
                        report.empty_chats += 1;
                    }
                    report.metadata_incomplete_events += u64::from(event.metadata_incomplete);
                    report.events_processed += 1;
                    event.sequence = report.events_processed;
                    report.last_event_at_unix_ms = Some(received_at);
                    event.connection_epoch = report.connections;
                    let ended = event.kind == "stream_end";
                    *report.events.entry(event.kind.clone()).or_default() += 1;
                    if let Some(output) = output {
                        match output.try_send(event) {
                            Ok(()) => {
                                report.events_enqueued += 1;
                                report.queue_high_watermark = report
                                    .queue_high_watermark
                                    .max(output.max_capacity() - output.capacity());
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                return Err(Failure::new(
                                    Code::ConsumerClosed,
                                    "event_queue",
                                    false,
                                ));
                            }
                            Err(mpsc::error::TrySendError::Full(event)) => {
                                match tokio::time::timeout_at(output_deadline, output.send(event))
                                    .await
                                {
                                    Ok(Ok(())) => {
                                        report.events_enqueued += 1;
                                        report.queue_high_watermark = report
                                            .queue_high_watermark
                                            .max(output.max_capacity() - output.capacity());
                                    }
                                    Ok(Err(_)) => {
                                        return Err(Failure::new(
                                            Code::ConsumerClosed,
                                            "event_queue",
                                            false,
                                        ));
                                    }
                                    Err(_) => {
                                        return Err(Failure::new(
                                            Code::ConsumerSlow,
                                            "event_queue",
                                            false,
                                        ));
                                    }
                                }
                            }
                        }
                    }
                    if ended {
                        stream_ended = true;
                        break;
                    }
                }
                // Deliver already-decoded data before ACK. A failed ACK must not discard
                // gifts already received; replay after reconnect is handled by dedupe.
                if frame.log_id != 0 {
                    let ack = send(
                        socket,
                        Message::Binary(
                            protocol::packet("ack", b"-".to_vec(), frame.log_id).into(),
                        ),
                        config,
                    )
                    .await;
                    if !stream_ended {
                        ack?;
                    }
                }
                if stream_ended {
                    return Ok(true);
                }
            }
            Message::Ping(payload) => send(socket, Message::Pong(payload), config).await?,
            Message::Close(close) => {
                let code = close.map(|c| u16::from(c.code)).unwrap_or(1005);
                return Err(Failure::new(
                    if matches!(code, 1000 | 1001 | 1005 | 1006 | 1011 | 1012 | 1013) {
                        Code::Network
                    } else {
                        Code::Refused
                    },
                    "socket_close",
                    matches!(code, 1000 | 1001 | 1005 | 1006 | 1011 | 1012 | 1013),
                ));
            }
            _ => {}
        }
    }
}

#[derive(Default)]
struct SessionHealth {
    decoder: crate::decoder::Decoder,
    ready_at: Option<Instant>,
    last_batch: Option<Instant>,
    status: Option<watch::Sender<ConnectionStatus>>,
    lifecycle: Option<LifecycleSender>,
}
impl SessionHealth {
    fn was_stable(&self, interval: Duration) -> bool {
        self.ready_at
            .zip(self.last_batch)
            .is_some_and(|(ready, last)| last.saturating_duration_since(ready) >= interval)
    }
}
