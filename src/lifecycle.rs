//! Bounded reconnect observations, independent of application-event backpressure.
use crate::engine::{Failure, Report, StopReason};
use serde::Serialize;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::broadcast;

pub use broadcast::error::{RecvError, TryRecvError};

/// Retained notifications per run, shared by all receivers. This is not a durable log.
pub const CAPACITY: usize = 64;

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleKind {
    Connecting,
    Connected,
    Ready,
    Interrupted,
    RetryScheduled,
    Stopped,
}

/// Ordered within this lifecycle channel, not across separate event/diagnostic pipes.
/// Times and counters are local observations; they cannot quantify upstream loss.
#[derive(Clone, Debug, Serialize)]
pub struct LifecycleEvent {
    pub sequence: u64,
    pub kind: LifecycleKind,
    pub observed_at_unix_ms: u64,
    pub room_id: String,
    pub connection_epoch: u32,
    pub previous_epoch: Option<u32>,
    pub room_changes: u32,
    pub possible_gaps: u32,
    pub events_processed: u64,
    pub events_enqueued: u64,
    pub retry_delay_ms: Option<u64>,
    pub stop_reason: Option<StopReason>,
    pub error: Option<Failure>,
}

/// Pass to one engine run only. Create a fresh channel for each run.
#[derive(Clone)]
pub struct LifecycleSender {
    sender: broadcast::Sender<LifecycleEvent>,
    sequence: Arc<AtomicU64>,
}

pub struct LifecycleReceiver(broadcast::Receiver<LifecycleEvent>);

/// Slow observers explicitly receive `RecvError::Lagged(n)` before retained records.
/// A dropped observer unsubscribes; observation never blocks/cancels event ingestion.
pub fn channel() -> (LifecycleSender, LifecycleReceiver) {
    let (sender, receiver) = broadcast::channel(CAPACITY);
    (
        LifecycleSender {
            sender,
            sequence: Arc::new(AtomicU64::new(0)),
        },
        LifecycleReceiver(receiver),
    )
}

impl LifecycleReceiver {
    pub async fn recv(&mut self) -> Result<LifecycleEvent, RecvError> {
        self.0.recv().await
    }

    pub fn try_recv(&mut self) -> Result<LifecycleEvent, TryRecvError> {
        self.0.try_recv()
    }
}

impl LifecycleSender {
    pub(crate) fn emit(
        &self,
        kind: LifecycleKind,
        report: &Report,
        retry_delay_ms: Option<u64>,
        error: Option<&Failure>,
    ) {
        let event = LifecycleEvent {
            sequence: self.sequence.fetch_add(1, Ordering::Relaxed) + 1,
            kind,
            observed_at_unix_ms: crate::engine::unix_ms(),
            room_id: report.room_id.clone(),
            connection_epoch: report.connections,
            previous_epoch: (kind == LifecycleKind::Connected && report.connections > 1)
                .then(|| report.connections - 1),
            room_changes: report.room_changes,
            possible_gaps: report.possible_gaps,
            events_processed: report.events_processed,
            events_enqueued: report.events_enqueued,
            retry_delay_ms,
            stop_reason: (kind == LifecycleKind::Stopped).then(|| report.stop_reason.clone()),
            error: error.cloned(),
        };
        // A receiver falling behind is reported by broadcast::Receiver, never hidden.
        // No receivers means the host intentionally stopped observing this channel.
        let _ = self.sender.send(event);
    }
}
