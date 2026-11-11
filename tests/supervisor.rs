use futures_util::{SinkExt, StreamExt};
use prost::Message as ProstMessage;
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tiktok_live_rust::lifecycle::{self, LifecycleEvent, LifecycleKind as L};
use tiktok_live_rust::{Event, engine::*, protocol::*};
use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf},
    sync::mpsc,
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        protocol::{CloseFrame, Role, frame::coding::CloseCode},
    },
};
use tokio_util::sync::CancellationToken;

async fn observe_lifecycle(mut receiver: lifecycle::LifecycleReceiver) -> Vec<LifecycleEvent> {
    let mut events = Vec::new();
    while let Ok(event) = receiver.recv().await {
        events.push(event);
    }
    events
}

#[tokio::test(start_paused = true)]
async fn lifecycle_reconnect_is_visible_without_another_application_event() {
    let script = Script::new(vec![
        Step::Socket("42", vec![batch(vec![gift(1, 1)]), Message::Close(None)]),
        Step::Socket("42", vec![]),
    ]);
    let (sender, mut receiver) = mpsc::channel(8);
    let (lifecycle, mut notifications) = lifecycle::channel();
    let (status, final_status) = status_channel();
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    let observe = async {
        let mut seen = Vec::new();
        while let Ok(event) = notifications.recv().await {
            if event.kind == L::Connected && event.connection_epoch == 2 {
                stop.cancel();
            }
            seen.push(event);
        }
        seen
    };
    let (report, seen) = tokio::join!(
        run_observed(
            &script,
            Target::Room("42".into()),
            config(),
            Some(sender),
            cancel,
            Observers {
                status: Some(status),
                lifecycle: Some(lifecycle)
            }
        ),
        observe
    );
    assert_eq!(
        seen.iter().map(|n| n.kind).collect::<Vec<_>>(),
        vec![
            L::Connecting,
            L::Connected,
            L::Ready,
            L::Interrupted,
            L::RetryScheduled,
            L::Connecting,
            L::Connected,
            L::Stopped
        ]
    );
    assert_eq!(
        seen.iter().map(|n| n.sequence).collect::<Vec<_>>(),
        (1..=8).collect::<Vec<_>>()
    );
    assert_eq!(seen[3].possible_gaps, 1);
    assert!(seen[3].error.is_some());
    assert!(seen[4].retry_delay_ms.unwrap() >= 1000);
    assert_eq!(seen[6].previous_epoch, Some(1));
    assert_eq!(seen[6].events_enqueued, 1);
    assert_eq!(seen[7].stop_reason, Some(StopReason::Cancelled));
    assert_eq!(report.stop_reason, StopReason::Cancelled);
    assert!(report.error.is_none());
    assert_eq!(
        (
            report.connections,
            report.ready_connections,
            report.possible_gaps
        ),
        (2, 1, 1)
    );
    assert_eq!(final_status.borrow().state, ConnectionState::Stopped);
    assert_eq!(receiver.recv().await.unwrap().connection_epoch, 1);
    assert!(receiver.recv().await.is_none());
    assert_eq!(script.active.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn lifecycle_room_changes_preserve_original_event_channel_and_watermarks() {
    let script = Script::new(vec![
        Step::Socket("42", vec![batch(vec![gift(1, 2)]), Message::Close(None)]),
        Step::Socket("43", vec![batch(vec![gift(1, 2), end()])]),
    ]);
    let (sender, mut receiver) = mpsc::channel(8);
    let (lifecycle, notifications) = lifecycle::channel();
    let (report, seen) = tokio::join!(
        run_observed(
            &script,
            Target::User("test".into()),
            config(),
            Some(sender),
            CancellationToken::new(),
            Observers {
                status: None,
                lifecycle: Some(lifecycle)
            }
        ),
        observe_lifecycle(notifications)
    );
    assert!(report.error.is_none());
    let connected = seen
        .iter()
        .filter(|n| n.kind == L::Connected)
        .collect::<Vec<_>>();
    assert_eq!(
        (&*connected[0].room_id, connected[0].previous_epoch),
        ("42", None)
    );
    assert_eq!(
        (
            &*connected[1].room_id,
            connected[1].previous_epoch,
            connected[1].room_changes
        ),
        ("43", Some(1), 1)
    );
    let mut events = Vec::new();
    while let Some(event) = receiver.recv().await {
        events.push(event);
    }
    assert_eq!(
        events
            .iter()
            .map(|e| (e.sequence, e.connection_epoch))
            .collect::<Vec<_>>(),
        vec![(1, 1), (2, 2), (3, 2)]
    );
    let stopped = seen.last().unwrap();
    assert_eq!(
        (
            stopped.kind,
            stopped.events_processed,
            stopped.events_enqueued
        ),
        (L::Stopped, 3, 3)
    );
    assert_eq!(stopped.stop_reason, Some(StopReason::StreamEnded));
    assert!(!report.gap_recovery);
}

#[tokio::test(start_paused = true)]
async fn lifecycle_terminal_failures_and_retry_exhaustion_are_explicit() {
    for (steps, expected, retries) in [
        (
            vec![Step::Fail(Failure::new(Code::Offline, "test", false))],
            Code::Offline,
            0,
        ),
        (
            (0..3)
                .map(|_| Step::Fail(Failure::new(Code::Network, "test", true)))
                .collect(),
            Code::RetryExhausted,
            2,
        ),
    ] {
        let script = Script::new(steps);
        let (lifecycle, notifications) = lifecycle::channel();
        let (report, seen) = tokio::join!(
            run_observed(
                &script,
                Target::Room("42".into()),
                Config {
                    max_reconnects: 2,
                    ..config()
                },
                None,
                CancellationToken::new(),
                Observers {
                    status: None,
                    lifecycle: Some(lifecycle)
                }
            ),
            observe_lifecycle(notifications)
        );
        let stopped = seen.last().unwrap();
        assert_eq!(stopped.kind, L::Stopped);
        assert_eq!(stopped.error.as_ref().unwrap().code, expected);
        assert_eq!(stopped.stop_reason, Some(StopReason::Failed));
        assert_eq!(
            seen.iter().filter(|n| n.kind == L::RetryScheduled).count(),
            retries
        );
        assert_eq!(report.possible_gaps, 0); // Handshake failures are not interrupted sessions.
        assert!(
            !seen
                .iter()
                .any(|n| n.kind == L::Connected || n.kind == L::Interrupted)
        );
    }
}

#[tokio::test(start_paused = true)]
async fn lifecycle_retry_wait_can_be_cancelled_without_another_attempt() {
    let script = Script::new(vec![Step::Fail(http_failure(429, Some("600"), "test"))]);
    let (lifecycle, mut notifications) = lifecycle::channel();
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    let observer = async {
        let mut seen = Vec::new();
        while let Ok(event) = notifications.recv().await {
            if event.kind == L::RetryScheduled {
                assert_eq!(event.retry_delay_ms, Some(600_000));
                stop.cancel();
            }
            seen.push(event);
        }
        seen
    };
    let (report, seen) = tokio::join!(
        run_observed(
            &script,
            Target::Room("42".into()),
            config(),
            None,
            cancel,
            Observers {
                status: None,
                lifecycle: Some(lifecycle)
            }
        ),
        observer
    );
    assert_eq!(report.stop_reason, StopReason::Cancelled);
    assert_eq!(
        seen.last().unwrap().stop_reason,
        Some(StopReason::Cancelled)
    );
    assert_eq!(script.calls.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn lifecycle_slow_observer_reports_lag_and_retains_terminal_notification() {
    let script = Script::new(
        (0..33)
            .map(|_| Step::Fail(Failure::new(Code::Network, "test", true)))
            .collect(),
    );
    let (lifecycle, mut notifications) = lifecycle::channel();
    let report = run_observed(
        &script,
        Target::Room("42".into()),
        Config {
            duration: Duration::from_secs(86_400),
            max_reconnects: 32,
            ..config()
        },
        None,
        CancellationToken::new(),
        Observers {
            status: None,
            lifecycle: Some(lifecycle),
        },
    )
    .await;
    assert_eq!(report.error.unwrap().code, Code::RetryExhausted);
    assert_eq!(
        notifications.recv().await.unwrap_err(),
        lifecycle::RecvError::Lagged(2)
    );
    let mut retained = Vec::new();
    while let Ok(event) = notifications.recv().await {
        retained.push(event);
    }
    assert_eq!(retained.len(), lifecycle::CAPACITY);
    assert_eq!(retained.first().unwrap().sequence, 3);
    assert_eq!(retained.last().unwrap().sequence, 66);
    assert_eq!(retained.last().unwrap().kind, L::Stopped);
    assert_eq!(
        notifications.try_recv().unwrap_err(),
        lifecycle::TryRecvError::Closed
    );
}

#[tokio::test(start_paused = true)]
async fn lifecycle_observer_drop_does_not_stop_ingestion() {
    let script = Script::new(vec![Step::Socket("42", vec![batch(vec![end()])])]);
    let (lifecycle, notifications) = lifecycle::channel();
    drop(notifications);
    let report = run_observed(
        &script,
        Target::Room("42".into()),
        config(),
        None,
        CancellationToken::new(),
        Observers {
            status: None,
            lifecycle: Some(lifecycle),
        },
    )
    .await;
    assert!(report.error.is_none());
    assert_eq!(report.stop_reason, StopReason::StreamEnded);
}

#[tokio::test(start_paused = true)]
async fn lifecycle_consumer_backpressure_stops_explicitly_without_reconnecting() {
    for closed in [false, true] {
        let script = Script::new(vec![Step::Socket(
            "42",
            vec![batch(vec![gift(1, 1), gift(2, 2)])],
        )]);
        let (sender, receiver) = mpsc::channel(1);
        let held = if closed {
            drop(receiver);
            None
        } else {
            Some(receiver)
        };
        let (lifecycle, notifications) = lifecycle::channel();
        let (report, seen) = tokio::join!(
            run_observed(
                &script,
                Target::Room("42".into()),
                config(),
                Some(sender),
                CancellationToken::new(),
                Observers {
                    status: None,
                    lifecycle: Some(lifecycle)
                }
            ),
            observe_lifecycle(notifications)
        );
        assert_eq!(
            report.error.unwrap().code,
            if closed {
                Code::ConsumerClosed
            } else {
                Code::ConsumerSlow
            }
        );
        assert!(report.events_unqueued > 0);
        assert_eq!(seen.last().unwrap().kind, L::Stopped);
        assert!(seen.iter().any(|n| n.kind == L::Interrupted));
        assert!(!seen.iter().any(|n| n.kind == L::RetryScheduled));
        assert_eq!(script.calls.load(Ordering::Relaxed), 1);
        assert_eq!(script.active.load(Ordering::SeqCst), 0);
        drop(held);
    }
}

enum Step {
    Fail(Failure),
    Socket(&'static str, Vec<Message>),
    Timed(&'static str, Vec<(Duration, Message)>, bool),
    Pending,
}
struct Script {
    steps: Mutex<VecDeque<Step>>,
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
}
struct TrackedIo {
    inner: DuplexStream,
    active: Arc<AtomicUsize>,
}
impl Drop for TrackedIo {
    fn drop(&mut self) {
        assert_eq!(self.active.fetch_sub(1, Ordering::SeqCst), 1);
    }
}
impl AsyncRead for TrackedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl AsyncWrite for TrackedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
impl Script {
    fn new(steps: Vec<Step>) -> Self {
        Self {
            steps: Mutex::new(steps.into()),
            calls: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
        }
    }
}
impl Source for Script {
    type Io = TrackedIo;
    async fn connect(&self, _: &Target) -> Result<Connection<Self::Io>, Failure> {
        assert_eq!(
            self.active.load(Ordering::SeqCst),
            0,
            "previous socket still alive on reconnect"
        );
        self.calls.fetch_add(1, Ordering::Relaxed);
        let step = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected connection attempt");
        let (room, messages, abrupt) = match step {
            Step::Fail(e) => return Err(e),
            Step::Pending => return std::future::pending().await,
            Step::Socket(room, messages) => (
                room,
                messages.into_iter().map(|m| (Duration::ZERO, m)).collect(),
                false,
            ),
            Step::Timed(room, messages, abrupt) => (room, messages, abrupt),
        };
        let (client, server) = tokio::io::duplex(128 * 1024);
        assert_eq!(self.active.fetch_add(1, Ordering::SeqCst), 0);
        let client = TrackedIo {
            inner: client,
            active: self.active.clone(),
        };
        tokio::spawn(async move {
            let mut socket = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
            let _ = socket.next().await; // enter room
            for (delay, message) in messages {
                tokio::time::sleep(delay).await;
                if socket.send(message).await.is_err() {
                    return;
                }
            }
            if abrupt {
                return;
            } // TCP-style EOF, no WebSocket close handshake.
            while let Some(Ok(message)) = socket.next().await {
                if matches!(message, Message::Close(_)) {
                    let _ = socket.flush().await;
                    break;
                }
            }
        });
        Ok(Connection {
            room_id: room.into(),
            socket: WebSocketStream::from_raw_socket(client, Role::Client, None).await,
        })
    }
}
fn batch(messages: Vec<Envelope>) -> Message {
    Message::Binary(
        packet(
            "msg",
            Batch {
                messages,
                internal_ext: String::new(),
                need_ack: false,
            }
            .encode_to_vec(),
            9,
        )
        .into(),
    )
}
fn gift(id: u64, count: u32) -> Envelope {
    Envelope {
        method: "WebcastGiftMessage".into(),
        msg_id: id,
        is_history: false,
        payload: Gift {
            gift_id: 1,
            repeat_count: count,
            repeat_end: false,
            group_id: 55,
            user: Some(User { id: 7 }),
            gift: Some(GiftMeta {
                kind: 1,
                diamonds: 1,
                name: "Rose".into(),
            }),
            receiver_id: "8".into(),
        }
        .encode_to_vec(),
    }
}
fn end() -> Envelope {
    Envelope {
        method: "WebcastControlMessage".into(),
        msg_id: u64::MAX,
        is_history: false,
        payload: Control { action: 3 }.encode_to_vec(),
    }
}
fn config() -> Config {
    Config {
        duration: Duration::from_secs(60),
        ..Config::default()
    }
}
async fn collect(script: &Script) -> (Report, Vec<Event>) {
    let (tx, mut rx) = mpsc::channel(32);
    let report = run(
        script,
        Target::User("test".into()),
        config(),
        Some(tx),
        CancellationToken::new(),
    )
    .await;
    let mut events = Vec::new();
    while let Some(event) = rx.recv().await {
        events.push(event);
    }
    (report, events)
}

#[tokio::test(start_paused = true)]
async fn observed_health_distinguishes_open_socket_traffic_and_recovery() {
    let script = Script::new(vec![
        Step::Timed(
            "42",
            vec![
                (
                    Duration::from_secs(2),
                    Message::Binary(packet("hb", vec![], 0).into()),
                ),
                (Duration::from_secs(2), batch(vec![gift(1, 1)])),
                (Duration::from_secs(4), Message::Close(None)),
            ],
            false,
        ),
        Step::Pending,
    ]);
    let (status, mut observer) = status_channel();
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    let task = tokio::spawn(async move {
        run_with_status(
            &script,
            Target::Room("42".into()),
            config(),
            None,
            stop,
            status,
        )
        .await
    });
    let mut opened = false;
    let mut heartbeat_only = false;
    let mut receiving = false;
    let mut retry_wait = false;
    while observer.changed().await.is_ok() {
        let snapshot = observer.borrow_and_update().clone();
        match snapshot.state {
            ConnectionState::AwaitingEvents => {
                opened = true;
                assert_eq!(snapshot.ready_connections, 0);
                assert!(snapshot.last_batch_at_unix_ms.is_none());
                heartbeat_only |= snapshot.last_protocol_at_unix_ms.is_some();
            }
            ConnectionState::Receiving => {
                receiving = true;
                assert_eq!(snapshot.ready_connections, 1);
                assert!(snapshot.last_batch_at_unix_ms.is_some());
            }
            ConnectionState::RetryWait => {
                retry_wait = true;
                assert!(snapshot.retry_delay_ms.unwrap() >= 1000);
                assert!(
                    snapshot.retry_not_before_unix_ms.unwrap()
                        >= snapshot.observed_at_unix_ms + 1000
                );
                assert_eq!(
                    snapshot.last_connection_error.as_ref().unwrap().code,
                    Code::Network
                );
                assert_eq!(snapshot.events_processed, 1);
                assert!(snapshot.last_event_at_unix_ms.is_some());
            }
            ConnectionState::Connecting if snapshot.reconnects == 1 => {
                assert!(snapshot.retry_delay_ms.is_none());
                cancel.cancel();
                break;
            }
            _ => {}
        }
    }
    let report = task.await.unwrap();
    assert!(opened && heartbeat_only && receiving && retry_wait);
    assert_eq!(report.stop_reason, StopReason::Cancelled);
    let final_status = observer.borrow();
    assert_eq!(final_status.state, ConnectionState::Stopped);
    assert_eq!(final_status.stop_reason, Some(StopReason::Cancelled));
    assert_eq!(
        final_status.last_event_at_unix_ms,
        report.last_event_at_unix_ms
    );
}

#[tokio::test(start_paused = true)]
async fn slow_or_absent_status_observer_does_not_block_events() {
    for drop_observer in [false, true] {
        let mut messages: Vec<_> = (1..=2048).map(|i| gift(i, i as u32)).collect();
        messages.push(end());
        let script = Script::new(vec![Step::Socket("42", vec![batch(messages)])]);
        let (status, observer) = status_channel();
        let observer = if drop_observer { None } else { Some(observer) };
        let (tx, mut rx) = mpsc::channel(8);
        let consume = async {
            let mut count = 0;
            while rx.recv().await.is_some() {
                count += 1;
            }
            count
        };
        let (report, count) = tokio::join!(
            run_with_status(
                &script,
                Target::Room("42".into()),
                config(),
                Some(tx),
                CancellationToken::new(),
                status
            ),
            consume
        );
        assert!(report.error.is_none(), "{report:?}");
        assert_eq!(count, 2049);
        assert_eq!(report.events_unqueued, 0);
        if let Some(observer) = observer {
            let snapshot = observer.borrow();
            assert_eq!(snapshot.state, ConnectionState::Stopped);
            assert_eq!(snapshot.events_enqueued, 2049);
            assert!(snapshot.last_event_at_unix_ms.is_some());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn invalid_input_resets_reused_status_and_never_connects() {
    let script = Script::new(vec![]);
    let (status, observer) = status_channel();
    status.send_modify(|s| {
        s.state = ConnectionState::Receiving;
        s.room_id = "999".into();
        s.last_event_at_unix_ms = Some(123);
    });
    let report = run_with_status(
        &script,
        Target::Room("0".into()),
        config(),
        None,
        CancellationToken::new(),
        status,
    )
    .await;
    assert_eq!(report.error.unwrap().code, Code::InvalidInput);
    let snapshot = observer.borrow();
    assert_eq!(snapshot.state, ConnectionState::Stopped);
    assert_eq!(snapshot.stop_reason, Some(StopReason::Failed));
    assert_eq!(snapshot.error.as_ref().unwrap().code, Code::InvalidInput);
    assert!(snapshot.last_event_at_unix_ms.is_none());
    assert!(snapshot.room_id.is_empty());
    assert_eq!(script.calls.load(Ordering::Relaxed), 0);
}

#[tokio::test(start_paused = true)]
async fn retry_until_deadline_recovers_after_long_outage_with_capped_backoff() {
    let mut steps: Vec<_> = (0..40)
        .map(|_| Step::Fail(Failure::new(Code::Network, "test", true)))
        .collect();
    steps.push(Step::Socket("42", vec![batch(vec![gift(1, 1), end()])]));
    let script = Script::new(steps);
    let report = run(
        &script,
        Target::Room("42".into()),
        Config {
            duration: Duration::from_secs(1800),
            max_reconnects: 0,
            retry_until_deadline: true,
            ..config()
        },
        None,
        CancellationToken::new(),
    )
    .await;
    assert!(report.error.is_none(), "{report:?}");
    assert!(report.retry_until_deadline);
    assert_eq!(report.reconnects, 40);
    assert_eq!(report.events_processed, 2);
    assert_eq!(script.calls.load(Ordering::Relaxed), 41);
    // 1 + 2 + 4 + 8 + 16 + 35*30 seconds, plus less than 40 seconds of jitter.
    assert!((1_081_000..1_121_000).contains(&report.elapsed_ms));
}

#[tokio::test(start_paused = true)]
async fn persistent_retry_still_respects_deadline_rate_limit_and_terminal_errors() {
    for failure in [
        http_failure(429, Some("600"), "test"),
        http_failure(403, None, "test"),
        Failure::new(Code::Offline, "test", false),
        Failure::new(Code::Protocol, "test", false),
    ] {
        let expected = failure.code.clone();
        let script = Script::new(vec![Step::Fail(failure)]);
        let (status, observer) = status_channel();
        let report = run_with_status(
            &script,
            Target::Room("42".into()),
            Config {
                retry_until_deadline: true,
                ..config()
            },
            None,
            CancellationToken::new(),
            status,
        )
        .await;
        assert_eq!(report.error.as_ref().unwrap().code, expected);
        assert_eq!(script.calls.load(Ordering::Relaxed), 1);
        assert_eq!(report.reconnects, 0);
        assert_eq!(observer.borrow().state, ConnectionState::Stopped);
        if expected == Code::RateLimited {
            assert_eq!(report.elapsed_ms, 60_000);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn persistent_retry_wait_cancels_without_an_extra_attempt() {
    let script = Script::new(vec![Step::Fail(http_failure(429, Some("600"), "test"))]);
    let (status, mut observer) = status_channel();
    let cancel = CancellationToken::new();
    let watch_cancel = cancel.clone();
    let observe = async {
        while observer.changed().await.is_ok() {
            let snapshot = observer.borrow_and_update().clone();
            if snapshot.state == ConnectionState::RetryWait {
                assert_eq!(snapshot.retry_delay_ms, Some(600_000));
                watch_cancel.cancel();
                break;
            }
        }
    };
    let (report, ()) = tokio::join!(
        run_with_status(
            &script,
            Target::Room("42".into()),
            Config {
                retry_until_deadline: true,
                ..config()
            },
            None,
            cancel,
            status
        ),
        observe
    );
    assert_eq!(report.stop_reason, StopReason::Cancelled);
    assert!(report.error.is_none());
    assert_eq!(script.calls.load(Ordering::Relaxed), 1);
    assert_eq!(observer.borrow().state, ConnectionState::Stopped);
}

#[tokio::test(start_paused = true)]
async fn rate_limit_without_usable_retry_after_does_not_hammer_the_server() {
    for hint in [None, Some("invalid"), Some("")] {
        let script = Script::new(vec![Step::Fail(http_failure(429, hint, "room_lookup"))]);
        let report = run(
            &script,
            Target::Room("42".into()),
            Config {
                duration: Duration::from_secs(10),
                retry_until_deadline: true,
                ..config()
            },
            None,
            CancellationToken::new(),
        )
        .await;
        assert_eq!(script.calls.load(Ordering::Relaxed), 1);
        assert_eq!(report.reconnects, 0);
        assert_eq!(report.elapsed_ms, 10_000);
        let error = report.error.unwrap();
        assert_eq!(error.code, Code::RateLimited);
        assert_eq!(error.retry_after_ms, Some(60_000));
    }
}

#[tokio::test(start_paused = true)]
async fn reconnect_preserves_dedupe_and_gift_streak() {
    let script = Script::new(vec![
        Step::Socket("42", vec![batch(vec![gift(1, 1)]), Message::Close(None)]),
        Step::Socket("42", vec![batch(vec![gift(1, 1), gift(2, 3), end()])]),
    ]);
    let (report, events) = collect(&script).await;
    assert!(report.error.is_none(), "{report:?}");
    assert_eq!(
        (
            report.connections,
            report.reconnects,
            report.duplicates,
            report.possible_gaps
        ),
        (2, 1, 1, 1)
    );
    assert_eq!(report.stop_reason, StopReason::StreamEnded);
    assert_eq!(
        events
            .iter()
            .map(|e| e.connection_epoch)
            .collect::<Vec<_>>(),
        vec![1, 2, 2]
    );
    assert_eq!(
        events
            .iter()
            .filter_map(|e| serde_json::to_value(&e.data).unwrap()["quantity_delta"].as_u64())
            .sum::<u64>(),
        3
    );
    assert_eq!(
        events.iter().map(|e| e.sequence).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
}
#[tokio::test(start_paused = true)]
async fn new_room_resets_streak_and_message_ids() {
    let script = Script::new(vec![
        Step::Socket("42", vec![batch(vec![gift(1, 2)]), Message::Close(None)]),
        Step::Socket("43", vec![batch(vec![gift(1, 2), end()])]),
    ]);
    let (report, events) = collect(&script).await;
    assert!(report.error.is_none(), "{report:?}");
    assert_eq!(report.room_changes, 1);
    assert_eq!(events[1].room_id, "43");
    assert_eq!(
        serde_json::to_value(&events[1].data).unwrap()["quantity_delta"],
        2
    );
}
#[tokio::test(start_paused = true)]
async fn transient_errors_have_a_finite_reconnect_budget() {
    let script = Script::new(
        (0..3)
            .map(|_| Step::Fail(Failure::new(Code::Network, "test", true)))
            .collect(),
    );
    let report = run(
        &script,
        Target::Room("42".into()),
        Config {
            max_reconnects: 2,
            ..config()
        },
        None,
        CancellationToken::new(),
    )
    .await;
    assert_eq!(report.error.unwrap().code, Code::RetryExhausted);
    assert_eq!(script.calls.load(Ordering::Relaxed), 3);
    assert!(report.elapsed_ms >= 3000);
}
#[tokio::test(start_paused = true)]
async fn refusal_is_not_retried_and_rate_limit_wait_is_not_shortened() {
    for (failure, expected) in [
        (http_failure(403, None, "test"), Code::Refused),
        (http_failure(429, Some("120"), "test"), Code::RateLimited),
    ] {
        let script = Script::new(vec![Step::Fail(failure)]);
        let report = run(
            &script,
            Target::Room("42".into()),
            config(),
            None,
            CancellationToken::new(),
        )
        .await;
        assert_eq!(report.error.unwrap().code, expected);
        assert_eq!(script.calls.load(Ordering::Relaxed), 1);
    }
}
#[tokio::test(start_paused = true)]
async fn cancellation_interrupts_handshake_and_backoff() {
    for step in [
        Step::Pending,
        Step::Fail(http_failure(429, Some("120"), "test")),
    ] {
        let script = Script::new(vec![step]);
        let cancel = CancellationToken::new();
        let signal = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            signal.cancel();
        });
        let report = run(&script, Target::Room("42".into()), config(), None, cancel).await;
        assert_eq!(report.stop_reason, StopReason::Cancelled);
        assert!(report.elapsed_ms < 1000);
        assert_eq!(script.calls.load(Ordering::Relaxed), 1);
    }
}
#[tokio::test(start_paused = true)]
async fn idle_socket_does_not_stay_connected_forever() {
    let script = Script::new(vec![Step::Socket("42", vec![])]);
    let report = run(
        &script,
        Target::Room("42".into()),
        Config {
            max_reconnects: 0,
            heartbeat: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(2),
            ..config()
        },
        None,
        CancellationToken::new(),
    )
    .await;
    assert_eq!(report.error.unwrap().code, Code::RetryExhausted);
    assert_eq!(report.last_connection_error.unwrap().stage, "socket_idle");
    assert!(report.elapsed_ms < 5000);
}

struct ManualSocket(Mutex<Option<DuplexStream>>);
impl Source for ManualSocket {
    type Io = DuplexStream;
    async fn connect(&self, _: &Target) -> Result<Connection<Self::Io>, Failure> {
        let io = self
            .0
            .lock()
            .unwrap()
            .take()
            .expect("unnecessary reconnect");
        Ok(Connection {
            room_id: "42".into(),
            socket: WebSocketStream::from_raw_socket(io, Role::Client, None).await,
        })
    }
}

#[tokio::test(start_paused = true)]
async fn buffered_batch_survives_a_three_second_watchdog_scheduler_gap() {
    use futures_util::FutureExt;
    let (client, server) = tokio::io::duplex(128 * 1024);
    let source = ManualSocket(Mutex::new(Some(client)));
    let mut server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
    let (tx, mut rx) = mpsc::channel(8);
    let mut running = Box::pin(run(
        &source,
        Target::Room("42".into()),
        Config {
            max_reconnects: 0,
            heartbeat: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(3),
            ..config()
        },
        Some(tx),
        CancellationToken::new(),
    ));
    assert!(running.as_mut().now_or_never().is_none());
    server.next().await.unwrap().unwrap(); // room entry
    server.send(batch(vec![gift(1, 1)])).await.unwrap();
    assert!(running.as_mut().now_or_never().is_none());
    assert_eq!(rx.try_recv().unwrap().sequence, 1);
    // Data reaches the receive buffer before time advances. The engine future is
    // deliberately not polled while its watchdog expires, as with a delayed task.
    server.send(batch(vec![gift(2, 3), end()])).await.unwrap();
    tokio::time::advance(Duration::from_secs(4)).await;
    let report = running.await;
    assert!(
        report.error.is_none(),
        "buffered data discarded: {report:?}"
    );
    assert_eq!(report.connections, 1);
    assert_eq!(report.reconnects, 0);
    assert_eq!(report.stop_reason, StopReason::StreamEnded);
    assert_eq!(report.events_enqueued, 3);
    assert_eq!(report.watchdog_ready_reads, 1);
    let update = rx.try_recv().unwrap();
    assert_eq!(update.sequence, 2);
    assert!(matches!(
        update.data,
        tiktok_live_rust::EventData::Gift {
            quantity_delta: Some(2),
            ..
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn expired_watchdogs_do_not_drain_unbounded_irrelevant_frames() {
    use futures_util::FutureExt;
    for (initial_batch, payload, expected_stage) in [
        (true, packet("future", vec![], 0), "socket_idle"),
        (true, vec![0xff], "socket_idle"),
        (false, packet("hb", vec![], 0), "room_startup"),
        (true, packet("hb", vec![], 0), "stream_idle"),
    ] {
        let (client, server) = tokio::io::duplex(128 * 1024);
        let source = ManualSocket(Mutex::new(Some(client)));
        let mut server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        let mut running = Box::pin(run(
            &source,
            Target::Room("42".into()),
            Config {
                max_reconnects: 0,
                heartbeat: Duration::from_secs(1),
                idle_timeout: Duration::from_secs(3),
                startup_timeout: Duration::from_secs(if initial_batch { 10 } else { 3 }),
                batch_idle_timeout: Duration::from_secs(if expected_stage == "stream_idle" {
                    3
                } else {
                    8
                }),
                ..config()
            },
            None,
            CancellationToken::new(),
        ));
        assert!(running.as_mut().now_or_never().is_none());
        server.next().await.unwrap().unwrap();
        if initial_batch {
            server.send(batch(vec![gift(1, 1)])).await.unwrap();
            assert!(running.as_mut().now_or_never().is_none());
        }
        for _ in 0..32 {
            server
                .send(Message::Binary(payload.clone().into()))
                .await
                .unwrap();
        }
        tokio::time::advance(Duration::from_secs(4)).await;
        let report = running.await;
        assert_eq!(report.last_connection_error.unwrap().stage, expected_stage);
        assert_eq!(report.watchdog_ready_reads, 1);
        assert_eq!(report.frames, u64::from(initial_batch) + 1);
        assert_eq!(report.connections, 1);
        assert_eq!(report.stop_reason, StopReason::Failed);
    }
}

#[tokio::test(start_paused = true)]
async fn buffered_first_batch_can_establish_readiness_after_scheduler_delay() {
    use futures_util::FutureExt;
    let (client, server) = tokio::io::duplex(128 * 1024);
    let source = ManualSocket(Mutex::new(Some(client)));
    let mut server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
    let mut running = Box::pin(run(
        &source,
        Target::Room("42".into()),
        Config {
            max_reconnects: 0,
            heartbeat: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(3),
            startup_timeout: Duration::from_secs(3),
            ..config()
        },
        None,
        CancellationToken::new(),
    ));
    assert!(running.as_mut().now_or_never().is_none());
    server.next().await.unwrap().unwrap();
    server.send(batch(vec![gift(1, 1), end()])).await.unwrap();
    tokio::time::advance(Duration::from_secs(4)).await;
    let report = running.await;
    assert!(report.error.is_none(), "{report:?}");
    assert_eq!(report.ready_connections, 1);
    assert_eq!(report.events_processed, 2);
    assert_eq!(report.watchdog_ready_reads, 1);
    assert_eq!(report.stop_reason, StopReason::StreamEnded);
}
#[tokio::test(start_paused = true)]
async fn full_queue_fails_explicitly_without_reconnect_loop() {
    let script = Script::new(vec![Step::Socket(
        "42",
        vec![batch(vec![gift(1, 1), gift(2, 2), end()])],
    )]);
    let (tx, _rx) = mpsc::channel(1);
    let report = run(
        &script,
        Target::Room("42".into()),
        config(),
        Some(tx),
        CancellationToken::new(),
    )
    .await;
    assert_eq!(report.error.unwrap().code, Code::ConsumerSlow);
    assert_eq!(
        (
            report.events_enqueued,
            report.events_unqueued,
            report.queue_high_watermark
        ),
        (1, 1, 1)
    );
    assert_eq!(script.calls.load(Ordering::Relaxed), 1);
}

#[tokio::test(start_paused = true)]
async fn decode_diagnostics_keep_fixed_counters_without_payloads() {
    let chats = [
        vec![],
        Chat {
            user: None,
            content: "x".repeat(4097),
        }
        .encode_to_vec(),
        vec![0x1a, 1, 0xff], // invalid UTF-8 in content
    ]
    .into_iter()
    .enumerate()
    .map(|(index, payload)| Envelope {
        method: "WebcastChatMessage".into(),
        msg_id: index as u64 + 1,
        is_history: false,
        payload,
    });
    let mut messages: Vec<_> = chats.collect();
    messages.push(end());
    let script = Script::new(vec![Step::Socket(
        "42",
        vec![Message::Binary(vec![0xff].into()), batch(messages)],
    )]);
    let (status, observer) = status_channel();
    let report = run_with_status(
        &script,
        Target::Room("42".into()),
        config(),
        None,
        CancellationToken::new(),
        status,
    )
    .await;
    assert_eq!(report.stop_reason, StopReason::StreamEnded);
    assert_eq!(report.decode_errors, 4);
    assert_eq!(observer.borrow().decode_errors, 4);
    assert_eq!(observer.borrow().decode_failures, report.decode_failures);
    assert_eq!(report.decode_failures.frame, 1);
    assert_eq!(report.decode_failures.chat_empty, 1);
    assert_eq!(report.decode_failures.chat_limit, 1);
    assert_eq!(report.decode_failures.chat_wire, 1);
    assert_eq!(std::mem::size_of::<DecodeFailures>(), 88);
    let details = serde_json::to_value(&report.decode_failures).unwrap();
    assert_eq!(details.as_object().unwrap().len(), 11);
    assert_eq!(
        details
            .as_object()
            .unwrap()
            .values()
            .map(|v| v.as_u64().unwrap())
            .sum::<u64>(),
        report.decode_errors
    );
}
#[tokio::test(start_paused = true)]
async fn closed_consumer_and_policy_close_are_terminal() {
    let script = Script::new(vec![Step::Socket("42", vec![batch(vec![gift(1, 1)])])]);
    let (tx, rx) = mpsc::channel(1);
    drop(rx);
    let report = run(
        &script,
        Target::Room("42".into()),
        config(),
        Some(tx),
        CancellationToken::new(),
    )
    .await;
    assert_eq!(report.error.unwrap().code, Code::ConsumerClosed);
    let script = Script::new(vec![Step::Socket(
        "42",
        vec![Message::Close(Some(CloseFrame {
            code: CloseCode::Policy,
            reason: "private remote text".into(),
        }))],
    )]);
    let report = run(
        &script,
        Target::Room("42".into()),
        config(),
        None,
        CancellationToken::new(),
    )
    .await;
    assert_eq!(report.error.as_ref().unwrap().code, Code::Refused);
    assert!(
        !serde_json::to_string(&report)
            .unwrap()
            .contains("private remote text")
    );
}
#[tokio::test(start_paused = true)]
async fn malformed_frame_storm_stops_after_threshold() {
    let script = Script::new(vec![Step::Socket(
        "42",
        (0..8).map(|_| Message::Binary(vec![255].into())).collect(),
    )]);
    let report = run(
        &script,
        Target::Room("42".into()),
        config(),
        None,
        CancellationToken::new(),
    )
    .await;
    assert_eq!(report.error.unwrap().code, Code::Decode);
    assert_eq!(report.decode_errors, 8);
}
#[test]
fn backoff_is_capped_and_invalid_targets_cannot_inject_urls() {
    assert_eq!(backoff(0, 0), Duration::from_secs(1));
    assert_eq!(backoff(u32::MAX, u64::MAX), Duration::from_millis(30999));
    for room in ["0", "01", "18446744073709551616", "42&foo=bar", "-1"] {
        assert!(Target::Room(room.into()).validate().is_err());
    }
    for name in ["@@a", "../a", "a?b", "https://example.com", ""] {
        assert!(Target::User(name.into()).validate().is_err());
    }
}

#[tokio::test(start_paused = true)]
async fn deadline_during_outage_does_not_report_success_after_earlier_events() {
    let script = Script::new(vec![
        Step::Socket("42", vec![batch(vec![gift(1, 1)]), Message::Close(None)]),
        Step::Fail(http_failure(429, Some("120"), "test")),
    ]);
    let (report, events) = collect(&script).await;
    assert_eq!(events.len(), 1);
    assert_eq!(report.stop_reason, StopReason::Failed);
    assert_eq!(report.error.unwrap().code, Code::RateLimited);
    assert_eq!(script.calls.load(Ordering::Relaxed), 2);
}

#[tokio::test(start_paused = true)]
async fn abrupt_socket_eof_reconnects_and_preserves_gift_accounting() {
    // ACK races the intentional abrupt drop. Already-received gifts must still be delivered.
    let message = Message::Binary(
        packet(
            "msg",
            Batch {
                messages: vec![gift(1, 1)],
                internal_ext: String::new(),
                need_ack: false,
            }
            .encode_to_vec(),
            9,
        )
        .into(),
    );
    let script = Script::new(vec![
        Step::Timed("42", vec![(Duration::ZERO, message)], true),
        Step::Socket("42", vec![batch(vec![gift(1, 1), gift(2, 2), end()])]),
    ]);
    let (report, events) = collect(&script).await;
    assert!(report.error.is_none(), "{report:?}");
    assert_eq!(report.reconnects, 1);
    assert_eq!(report.last_connection_error.unwrap().code, Code::Network);
    assert_eq!(
        events
            .iter()
            .filter_map(|e| serde_json::to_value(&e.data).unwrap()["quantity_delta"].as_u64())
            .sum::<u64>(),
        2
    );
}

#[tokio::test(start_paused = true)]
async fn stable_traffic_replenishes_budget_but_flapping_does_not() {
    let stable = || {
        Step::Timed(
            "42",
            vec![
                (Duration::ZERO, batch(vec![gift(1, 1)])),
                (Duration::from_secs(3), batch(vec![gift(2, 2)])),
                (Duration::ZERO, Message::Close(None)),
            ],
            false,
        )
    };
    let script = Script::new(vec![
        Step::Fail(Failure::new(Code::Network, "test", true)),
        stable(),
        stable(),
        Step::Socket("42", vec![batch(vec![end()])]),
    ]);
    let cfg = Config {
        max_reconnects: 1,
        heartbeat: Duration::from_secs(1),
        healthy_reset_after: Duration::from_secs(2),
        ..config()
    };
    let report = run(
        &script,
        Target::Room("42".into()),
        cfg.clone(),
        None,
        CancellationToken::new(),
    )
    .await;
    assert!(report.error.is_none(), "{report:?}");
    assert_eq!((report.reconnects, report.retry_budget_resets), (3, 2));
    let script = Script::new(vec![
        Step::Socket("42", vec![batch(vec![gift(1, 1)]), Message::Close(None)]),
        Step::Socket("42", vec![batch(vec![gift(2, 2)]), Message::Close(None)]),
    ]);
    let report = run(
        &script,
        Target::Room("42".into()),
        cfg,
        None,
        CancellationToken::new(),
    )
    .await;
    assert_eq!(report.error.unwrap().code, Code::RetryExhausted);
    assert_eq!(report.retry_budget_resets, 0);
}

#[tokio::test(start_paused = true)]
async fn pings_cannot_fake_room_readiness_or_protocol_activity() {
    let script = Script::new(vec![Step::Timed(
        "42",
        (0..8)
            .map(|_| (Duration::from_millis(200), Message::Ping(vec![1].into())))
            .collect(),
        false,
    )]);
    let report = run(
        &script,
        Target::Room("42".into()),
        Config {
            max_reconnects: 0,
            startup_timeout: Duration::from_secs(1),
            ..config()
        },
        None,
        CancellationToken::new(),
    )
    .await;
    assert_eq!(report.last_connection_error.unwrap().stage, "room_startup");
    assert_eq!(report.ready_connections, 0);
}

#[tokio::test(start_paused = true)]
async fn heartbeat_only_stream_recovers_when_event_batches_stop() {
    let mut messages = vec![(Duration::ZERO, batch(vec![gift(1, 1)]))];
    messages.extend((0..5).map(|_| {
        (
            Duration::from_secs(1),
            Message::Binary(packet("hb", vec![], 0).into()),
        )
    }));
    let script = Script::new(vec![
        Step::Timed("42", messages, false),
        Step::Socket("42", vec![batch(vec![end()])]),
    ]);
    let cfg = Config {
        heartbeat: Duration::from_secs(1),
        idle_timeout: Duration::from_secs(2),
        batch_idle_timeout: Duration::from_secs(3),
        ..config()
    };
    let report = run(
        &script,
        Target::Room("42".into()),
        cfg,
        None,
        CancellationToken::new(),
    )
    .await;
    assert!(report.error.is_none(), "{report:?}");
    assert_eq!(report.last_connection_error.unwrap().stage, "stream_idle");
    assert_eq!(report.ready_connections, 2);
}

#[test]
fn broken_handshake_and_malformed_frames_have_distinct_retry_policy() {
    use tokio_tungstenite::tungstenite::{Error, error::ProtocolError};
    assert!(
        websocket_error(
            Error::Protocol(ProtocolError::ResetWithoutClosingHandshake),
            "test"
        )
        .retryable
    );
    assert!(
        !websocket_error(
            Error::Protocol(ProtocolError::MaskedFrameFromServer),
            "test"
        )
        .retryable
    );
}

#[tokio::test(start_paused = true)]
async fn healthy_consumer_can_drain_a_burst_larger_than_the_queue() {
    let script = Script::new(vec![Step::Socket(
        "42",
        vec![batch(
            (1..=2048)
                .map(|id| gift(id, id as u32))
                .chain([end()])
                .collect(),
        )],
    )]);
    let (tx, mut rx) = mpsc::channel(8);
    let running = run(
        &script,
        Target::Room("42".into()),
        config(),
        Some(tx),
        CancellationToken::new(),
    );
    let consumer = async {
        let mut sequence = 0;
        let mut quantity = 0;
        while let Some(event) = rx.recv().await {
            sequence += 1;
            assert_eq!(event.sequence, sequence);
            if let tiktok_live_rust::EventData::Gift {
                quantity_delta: Some(delta),
                ..
            } = event.data
            {
                quantity += delta;
            }
        }
        (sequence, quantity)
    };
    let (report, (sequence, quantity)) = tokio::join!(running, consumer);
    assert!(report.error.is_none(), "{report:?}");
    assert_eq!((sequence, quantity), (2049, 2048));
    assert_eq!(report.events_unqueued, 0);
}

#[tokio::test(start_paused = true)]
async fn one_thousand_connections_keep_sequences_combo_totals_and_socket_lifetime_correct() {
    let mut steps = Vec::new();
    for index in 0..1000u64 {
        let count = index as u32 + 1;
        let ending = if index == 999 {
            batch(vec![end()])
        } else {
            Message::Close(None)
        };
        steps.push(Step::Timed(
            "42",
            vec![
                (Duration::ZERO, batch(vec![gift(index + 1, count)])),
                (
                    Duration::from_secs(3),
                    batch(vec![gift(index + 1001, count)]),
                ),
                (Duration::ZERO, ending),
            ],
            false,
        ));
    }
    let script = Script::new(steps);
    let (tx, mut rx) = mpsc::channel(2048);
    let cfg = Config {
        duration: Duration::from_secs(10000),
        max_reconnects: 1,
        heartbeat: Duration::from_secs(1),
        healthy_reset_after: Duration::from_secs(2),
        ..config()
    };
    let report = run(
        &script,
        Target::Room("42".into()),
        cfg,
        Some(tx),
        CancellationToken::new(),
    )
    .await;
    assert!(report.error.is_none(), "{report:?}");
    assert_eq!(
        (
            report.connections,
            report.reconnects,
            report.retry_budget_resets
        ),
        (1000, 999, 998)
    );
    let mut sequence = 0;
    let mut quantity = 0;
    while let Some(event) = rx.recv().await {
        sequence += 1;
        assert_eq!(event.sequence, sequence);
        if let tiktok_live_rust::EventData::Gift {
            quantity_delta: Some(delta),
            ..
        } = event.data
        {
            quantity += delta;
        }
    }
    assert_eq!((sequence, quantity), (2001, 1000));
    assert_eq!(script.active.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn connection_timeout_recovers_without_leaving_a_socket() {
    let script = Script::new(vec![
        Step::Pending,
        Step::Socket("42", vec![batch(vec![gift(1, 1), end()])]),
    ]);
    let (report, events) = collect(&script).await;
    assert!(report.error.is_none(), "{report:?}");
    assert_eq!(report.connections, 1);
    assert_eq!(report.reconnects, 1);
    assert_eq!(events.len(), 2);
    assert_eq!(script.calls.load(Ordering::SeqCst), 2);
    assert_eq!(script.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn gift_window_overflow_is_reported_without_guessing_a_cumulative_delta() {
    let mut envelopes = Vec::new();
    for group in 1..=4097 {
        let mut raw = gift(group, 1);
        let mut value = Gift::decode(raw.payload.as_slice()).unwrap();
        value.group_id = group;
        raw.payload = value.encode_to_vec();
        envelopes.push(raw);
    }
    let mut late = gift(9000, 3);
    let mut value = Gift::decode(late.payload.as_slice()).unwrap();
    value.group_id = 1;
    late.payload = value.encode_to_vec();
    envelopes.extend([late, end()]);
    let script = Script::new(vec![Step::Socket(
        "42",
        envelopes
            .chunks(512)
            .map(|chunk| batch(chunk.to_vec()))
            .collect(),
    )]);
    let report = run(
        &script,
        Target::Room("42".into()),
        config(),
        None,
        CancellationToken::new(),
    )
    .await;
    assert!(report.error.is_none(), "{report:?}");
    assert_eq!(
        (report.gift_streak_evictions, report.gift_streak_resyncs),
        (2, 1)
    );
    assert_eq!(report.events_processed, 4099);
    assert_eq!(script.active.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn long_timer_gap_recovers_and_replayed_gift_is_not_counted_twice() {
    let script = Script::new(vec![
        Step::Socket("42", vec![batch(vec![gift(1, 1)])]),
        Step::Socket("42", vec![batch(vec![gift(1, 1), gift(2, 3), end()])]),
    ]);
    let (tx, mut rx) = mpsc::channel(32);
    let running = run(
        &script,
        Target::Room("42".into()),
        Config {
            duration: Duration::from_secs(600),
            ..config()
        },
        Some(tx),
        CancellationToken::new(),
    );
    let consumer = async {
        let first = rx.recv().await.unwrap();
        assert_eq!(first.sequence, 1);
        // Simulates elapsed timer time, not an actual OS suspend or Wi-Fi switch.
        tokio::time::advance(Duration::from_secs(180)).await;
        let mut events = vec![first];
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    };
    let (report, events) = tokio::join!(running, consumer);
    assert!(report.error.is_none(), "{report:?}");
    assert_eq!(report.connections, 2);
    assert_eq!(report.duplicates, 1);
    assert_eq!(events.len(), 3);
    let quantity: u32 = events
        .iter()
        .filter_map(|e| match &e.data {
            tiktok_live_rust::EventData::Gift { quantity_delta, .. } => *quantity_delta,
            _ => None,
        })
        .sum();
    assert_eq!(quantity, 3);
    for (i, event) in events.iter().enumerate() {
        assert_eq!(event.sequence, i as u64 + 1);
    }
    assert_eq!(script.active.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn cancellation_accounts_for_an_event_waiting_to_enqueue() {
    let script = Script::new(vec![Step::Socket(
        "42",
        vec![batch(vec![gift(1, 1), gift(2, 2)])],
    )]);
    let (tx, mut rx) = mpsc::channel(1);
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        signal.cancel();
    });
    let report = run(
        &script,
        Target::Room("42".into()),
        config(),
        Some(tx),
        cancel,
    )
    .await;
    assert_eq!(report.stop_reason, StopReason::Cancelled);
    assert_eq!(
        (
            report.events_processed,
            report.events_enqueued,
            report.events_unqueued
        ),
        (2, 1, 1)
    );
    assert_eq!(rx.recv().await.unwrap().sequence, 1);
    assert!(rx.recv().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn slow_consumer_grace_is_shared_by_the_batch_not_restarted_per_event() {
    let script = Script::new(vec![Step::Socket(
        "42",
        vec![batch((1..=10).map(|id| gift(id, id as u32)).collect())],
    )]);
    let (tx, mut rx) = mpsc::channel(1);
    let cfg = Config {
        output_grace: Duration::from_millis(200),
        ..config()
    };
    let running = run(
        &script,
        Target::Room("42".into()),
        cfg,
        Some(tx),
        CancellationToken::new(),
    );
    let consumer = async {
        loop {
            tokio::time::sleep(Duration::from_millis(80)).await;
            if rx.recv().await.is_none() {
                break;
            }
        }
    };
    let (report, ()) = tokio::join!(running, consumer);
    assert_eq!(report.error.unwrap().code, Code::ConsumerSlow);
    assert!(report.elapsed_ms <= 250);
    assert_eq!(report.events_unqueued, 1);
    assert_eq!(report.reconnects, 0);
}
