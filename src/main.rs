use anyhow::{Context, Result, bail};
use std::{
    io::Write,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tiktok_live_rust::{
    Event,
    engine::{self, Code, Config, ConnectionStatus, Failure, Report, StopReason, Target},
    lifecycle::{self, LifecycleReceiver},
    transport::DirectSource,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

struct Options {
    target: Target,
    config: Config,
    events: bool,
    status: bool,
    lifecycle: bool,
    report: Option<String>,
}
fn parse(args: &[String]) -> Result<Options> {
    let mut target = None;
    let mut config = Config::default();
    let mut events = false;
    let mut status = false;
    let mut lifecycle = false;
    let mut explicit_reconnect_limit = false;
    let mut report = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--events" => events = true,
            "--status" => status = true,
            "--lifecycle" => lifecycle = true,
            "--retry-until-deadline" => config.retry_until_deadline = true,
            "--room" | "--user" | "--seconds" | "--max-reconnects" | "--report" => {
                let value = args.get(i + 1).context("missing option value")?;
                match args[i].as_str() {
                    "--room" | "--user" => {
                        if target.is_some() {
                            bail!("provide exactly one --user or --room");
                        }
                        target = Some(if args[i] == "--room" {
                            Target::Room(value.clone())
                        } else {
                            Target::User(value.clone())
                        });
                    }
                    "--seconds" => {
                        config.duration =
                            Duration::from_secs(value.parse().context("invalid duration")?)
                    }
                    "--max-reconnects" => {
                        config.max_reconnects = value.parse().context("invalid reconnect limit")?;
                        explicit_reconnect_limit = true;
                    }
                    _ => report = Some(value.clone()),
                }
                i += 1;
            }
            _ => bail!("unknown option"),
        }
        i += 1;
    }
    let target = target.context("provide --user or --room")?;
    if config.retry_until_deadline && explicit_reconnect_limit {
        bail!("choose --retry-until-deadline or --max-reconnects");
    }
    target.validate()?;
    config.validate()?;
    Ok(Options {
        target,
        config,
        events,
        status,
        lifecycle,
        report,
    })
}

/// A blocked pipe never blocks heartbeat or creates an unbounded backlog.
fn event_writer(
    mut receiver: mpsc::Receiver<Event>,
    done: oneshot::Sender<bool>,
    written: Arc<AtomicU64>,
    cancel: CancellationToken,
) {
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    let result: Result<()> = (|| {
        while let Some(event) = receiver.blocking_recv() {
            serde_json::to_writer(&mut output, &event)?;
            output.write_all(b"\n")?;
            output.flush()?;
            written.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    })();
    if result.is_err() {
        cancel.cancel();
    }
    let _ = done.send(result.is_ok());
}

#[derive(serde::Serialize)]
struct Diagnostic<'a, T> {
    #[serde(rename = "type")]
    kind: &'static str,
    data: &'a T,
}
fn write_diagnostic(
    output: &mut impl Write,
    kind: &'static str,
    data: &impl serde::Serialize,
) -> Result<()> {
    serde_json::to_writer(&mut *output, &Diagnostic { kind, data })?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

/// Compatibility helper for the snapshot-only diagnostic tests.
#[cfg(test)]
fn status_writer(
    output: impl Write,
    status: watch::Receiver<ConnectionStatus>,
    final_report: oneshot::Receiver<Report>,
) -> Result<()> {
    diagnostics_writer(output, Some(status), None, final_report)
}

fn lifecycle_record(
    output: &mut impl Write,
    record: Result<lifecycle::LifecycleEvent, lifecycle::RecvError>,
) -> Result<bool> {
    match record {
        Ok(event) => write_diagnostic(output, "lifecycle", &event)?,
        Err(lifecycle::RecvError::Lagged(count)) => write_diagnostic(
            output,
            "lifecycle_lagged",
            &serde_json::json!({"skipped_notifications": count}),
        )?,
        Err(lifecycle::RecvError::Closed) => return Ok(false),
    }
    Ok(true)
}

// Drain retained lifecycle records before the final report, including after a slow pipe.
fn drain_lifecycle(output: &mut impl Write, receiver: &mut LifecycleReceiver) -> Result<()> {
    loop {
        match receiver.try_recv() {
            Ok(event) => {
                lifecycle_record(output, Ok(event))?;
            }
            Err(lifecycle::TryRecvError::Lagged(n)) => {
                lifecycle_record(output, Err(lifecycle::RecvError::Lagged(n)))?;
            }
            Err(lifecycle::TryRecvError::Closed | lifecycle::TryRecvError::Empty) => return Ok(()),
        }
    }
}

fn diagnostics_writer(
    mut output: impl Write,
    mut status: Option<watch::Receiver<ConnectionStatus>>,
    mut lifecycle: Option<LifecycleReceiver>,
    mut final_report: oneshot::Receiver<Report>,
) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    runtime.block_on(async {
        let mut watching = status.is_some();
        let mut observing = lifecycle.is_some();
        loop {
            tokio::select! { biased;
                report = &mut final_report => {
                    let report = report.context("report sender closed")?;
                    if let Some(receiver) = lifecycle.as_mut() { drain_lifecycle(&mut output, receiver)?; }
                    if let Some(status) = &status {
                        let snapshot = status.borrow().clone();
                        write_diagnostic(&mut output, "status", &snapshot)?;
                    }
                    write_diagnostic(&mut output, "report", &report)?;
                    return Ok(());
                },
                record = async { lifecycle.as_mut().expect("enabled lifecycle receiver").recv().await }, if observing => {
                    observing = lifecycle_record(&mut output, record)?;
                },
                changed = async { status.as_mut().expect("enabled status receiver").changed().await }, if watching => {
                    if changed.is_err() { watching = false; continue; }
                    // Release the watch read lock BEFORE serializing or writing a pipe.
                    let snapshot = status.as_mut().unwrap().borrow_and_update().clone();
                    write_diagnostic(&mut output, "status", &snapshot)?;
                },
            }
        }
    })
}
fn atomic_report(path: &Path, json: &str) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .context("report path needs a filename")?
        .to_string_lossy();
    let temporary = parent.join(format!(".{name}.{:016x}.tmp", rand::random::<u64>()));
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(json.as_bytes())?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, path)?;
        Ok::<_, std::io::Error>(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result.context("cannot save report atomically")
}
fn serialize_and_save_report(path: Option<&str>, report: &mut Report) -> Result<String> {
    let json = serde_json::to_string_pretty(report)?;
    if let Some(path) = path
        && atomic_report(Path::new(path), &json).is_err()
    {
        // Keep errors in the structured output path. Returning an anyhow error from main
        // could try to lock stderr while a diagnostic writer is blocked on that same pipe.
        report
            .error
            .get_or_insert_with(|| Failure::new(Code::Output, "report_file", false));
        report.stop_reason = StopReason::Failed;
        return Ok(serde_json::to_string_pretty(report)?);
    }
    Ok(json)
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args == ["--version"] {
        println!(
            "{} {}\nDeveloped by {}",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION"),
            env!("CARGO_PKG_AUTHORS")
        );
        return Ok(());
    }
    if args.is_empty() || args == ["--help"] {
        println!(
            "tiktok-live-rust --user USER | --room ROOM [--seconds 60] [--max-reconnects 8 | --retry-until-deadline] [--events] [--status] [--lifecycle] [--report PATH]\nDeveloped by Tri Nguyen\nDirect guest engine. No Euler. Duration 1..86400 seconds; reconnect budget 0..32 per outage.\n--retry-until-deadline: retry transient errors until duration/Ctrl+C; honors backoff and Retry-After.\nDefault: aggregate JSON. --events: NDJSON events on stdout, report on stderr.\n--status: coalesced NDJSON health snapshots on stderr.\n--lifecycle: bounded reconnect notifications on stderr; explicit lag records if slow.\nEither flag adds a tagged final report. Ctrl+C stops."
        );
        return Ok(());
    }
    let options = parse(&args)?;
    let source = DirectSource::new()?;
    let cancel = CancellationToken::new();
    let signal_cancel = cancel.clone();
    let signal_task = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal_cancel.cancel();
        }
    });
    let written = Arc::new(AtomicU64::new(0));
    let (output, writer_done) = if options.events {
        let (sender, receiver) = mpsc::channel(256);
        let (done, finished) = oneshot::channel();
        let counter = written.clone();
        let stop = cancel.clone();
        std::thread::Builder::new()
            .name("event-output".into())
            .spawn(move || event_writer(receiver, done, counter, stop))?;
        (Some(sender), Some(finished))
    } else {
        (None, None)
    };
    let (status, receiver) = if options.status {
        let (sender, receiver) = engine::status_channel();
        (Some(sender), Some(receiver))
    } else {
        (None, None)
    };
    let (lifecycle, lifecycle_receiver) = if options.lifecycle {
        let (sender, receiver) = lifecycle::channel();
        (Some(sender), Some(receiver))
    } else {
        (None, None)
    };
    let (diagnostics, diagnostics_enabled) = if options.status || options.lifecycle {
        let (final_sender, final_receiver) = oneshot::channel();
        let (done, finished) = oneshot::channel();
        let stop = cancel.clone();
        std::thread::Builder::new()
            .name("status-output".into())
            .spawn(move || {
                let stderr = std::io::stderr();
                let result =
                    diagnostics_writer(stderr.lock(), receiver, lifecycle_receiver, final_receiver);
                if result.is_err() {
                    stop.cancel();
                }
                let _ = done.send(result.is_ok());
            })?;
        (Some((final_sender, finished)), true)
    } else {
        (None, false)
    };
    let mut report = engine::run_observed(
        &source,
        options.target,
        options.config,
        output,
        cancel,
        engine::Observers { status, lifecycle },
    )
    .await;
    signal_task.abort();
    if let Some(done) = writer_done {
        let error = match tokio::time::timeout(Duration::from_secs(3), done).await {
            Ok(Ok(true)) => None,
            Ok(_) => Some(Failure::new(Code::ConsumerClosed, "stdout", false)),
            Err(_) => Some(Failure::new(Code::ConsumerSlow, "stdout_drain", false)),
        };
        if error.is_some() {
            if report.error.is_none() {
                report.error = error;
            }
            report.stop_reason = StopReason::Failed;
        }
    }
    report.events_written = written.load(Ordering::Relaxed);
    let mut json = serialize_and_save_report(options.report.as_deref(), &mut report)?;
    if let Some((sender, done)) = diagnostics {
        let _ = sender.send(report.clone());
        let error = match tokio::time::timeout(Duration::from_secs(3), done).await {
            Ok(Ok(true)) => None,
            Ok(_) => Some(Failure::new(Code::ConsumerClosed, "status_output", false)),
            Err(_) => Some(Failure::new(Code::ConsumerSlow, "status_output", false)),
        };
        if let Some(error) = error {
            report.error.get_or_insert(error);
            report.stop_reason = StopReason::Failed;
            json = serialize_and_save_report(options.report.as_deref(), &mut report)?;
        }
    }
    if options.events && !diagnostics_enabled {
        eprintln!("{json}");
    } else if !options.events {
        println!("{json}");
    }
    if report.error.is_some() {
        std::process::exit(1);
    }
    // Do not wait indefinitely for a detached stdout writer blocked by its consumer.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    struct FailingSource;
    impl engine::Source for FailingSource {
        type Io = tokio::io::DuplexStream;
        async fn connect(
            &self,
            _: &Target,
        ) -> std::result::Result<engine::Connection<Self::Io>, Failure> {
            Err(Failure::new(Code::Network, "synthetic_connect", true))
        }
    }

    fn lagged_lifecycle() -> (LifecycleReceiver, Report) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            tokio::time::pause();
            let (sender, receiver) = lifecycle::channel();
            let report = engine::run_observed(
                &FailingSource,
                Target::Room("42".into()),
                Config {
                    duration: Duration::from_secs(86_400),
                    max_reconnects: 32,
                    ..Config::default()
                },
                None,
                CancellationToken::new(),
                engine::Observers {
                    status: None,
                    lifecycle: Some(sender),
                },
            )
            .await;
            (receiver, report)
        })
    }

    #[test]
    fn lifecycle_drain_precedes_ready_final_report_and_reports_lag() {
        let (receiver, report) = lagged_lifecycle();
        let (done, final_report) = oneshot::channel();
        done.send(report).unwrap(); // Both branches are already ready: never skip lifecycle.
        let mut bytes = Vec::new();
        diagnostics_writer(&mut bytes, None, Some(receiver), final_report).unwrap();
        let records = String::from_utf8(bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(records.len(), lifecycle::CAPACITY + 2);
        assert_eq!(records[0]["type"], "lifecycle_lagged");
        assert_eq!(records[0]["data"]["skipped_notifications"], 2);
        assert_eq!(records[1]["data"]["sequence"], 3);
        assert_eq!(records[64]["data"]["kind"], "stopped");
        assert_eq!(records[64]["data"]["error"]["code"], "retry_exhausted");
        assert_eq!(records.last().unwrap()["type"], "report");
        assert!(!records.iter().any(|record| record["type"] == "status"));
    }

    #[test]
    fn lifecycle_diagnostic_write_failure_is_propagated() {
        struct ClosedPipe;
        impl Write for ClosedPipe {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let (receiver, report) = lagged_lifecycle();
        let (done, final_report) = oneshot::channel();
        done.send(report).unwrap();
        assert!(diagnostics_writer(ClosedPipe, None, Some(receiver), final_report).is_err());
    }
    #[test]
    fn invalid_cli_is_rejected_before_network() {
        for input in [
            vec!["--user", "alice", "--room", "42"],
            vec!["--room", "0"],
            vec!["--user", "a/b"],
            vec!["--user", "a", "--seconds", "0"],
            vec!["--user", "a", "--seconds", "86401"],
            vec!["--user", "a", "--max-reconnects", "33"],
            vec![
                "--user",
                "a",
                "--max-reconnects",
                "2",
                "--retry-until-deadline",
            ],
        ] {
            assert!(parse(&input.into_iter().map(String::from).collect::<Vec<_>>()).is_err());
        }
    }
    #[test]
    fn status_and_retry_mode_are_explicit_and_default_compatible() {
        let options = parse(
            &[
                "--user",
                "alice",
                "--status",
                "--lifecycle",
                "--events",
                "--retry-until-deadline",
            ]
            .map(String::from),
        )
        .unwrap();
        assert!(options.status && options.events && options.config.retry_until_deadline);
        assert!(options.lifecycle);
        let default = parse(&["--user", "alice"].map(String::from)).unwrap();
        assert!(
            !default.status
                && !default.events
                && !default.lifecycle
                && !default.config.retry_until_deadline
        );
        let lifecycle_only = parse(&["--room", "42", "--lifecycle"].map(String::from)).unwrap();
        assert!(lifecycle_only.lifecycle && !lifecycle_only.status && !lifecycle_only.events);
    }
    #[test]
    fn diagnostics_end_with_stopped_snapshot_and_tagged_report() {
        let (tx, rx) = engine::status_channel();
        tx.send_modify(|s| s.state = engine::ConnectionState::Stopped);
        drop(tx);
        let (done, report) = oneshot::channel();
        done.send(Report::default()).unwrap();
        let mut bytes = Vec::new();
        status_writer(&mut bytes, rx, report).unwrap();
        let records: Vec<serde_json::Value> = std::str::from_utf8(&bytes)
            .unwrap()
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["type"], "status");
        assert_eq!(records[0]["data"]["state"], "stopped");
        assert_eq!(records[1]["type"], "report");
    }
    #[test]
    fn blocked_diagnostics_release_watch_lock_and_coalesce_updates() {
        struct GatedOutput {
            entered: Option<std::sync::mpsc::SyncSender<()>>,
            release: std::sync::mpsc::Receiver<()>,
        }
        impl Write for GatedOutput {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if let Some(entered) = self.entered.take() {
                    entered.send(()).unwrap();
                    self.release
                        .recv_timeout(Duration::from_secs(3))
                        .map_err(|_| std::io::ErrorKind::TimedOut)?;
                }
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let (status, observer) = engine::status_channel();
        status.send_modify(|s| s.state = engine::ConnectionState::Connecting);
        let (report_tx, report_rx) = oneshot::channel();
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            status_writer(
                GatedOutput {
                    entered: Some(entered_tx),
                    release: release_rx,
                },
                observer,
                report_rx,
            )
        });
        entered_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        let started = std::time::Instant::now();
        for count in 1..=10_000 {
            status.send_modify(|s| s.events_processed = count);
        }
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "blocked stderr held the health read lock"
        );
        status.send_modify(|s| s.state = engine::ConnectionState::Stopped);
        report_tx.send(Report::default()).unwrap();
        release_tx.send(()).unwrap();
        worker.join().unwrap().unwrap();
    }
    #[test]
    fn report_replace_is_complete() {
        let path =
            std::env::temp_dir().join(format!("live-engine-report-{}.json", rand::random::<u64>()));
        atomic_report(&path, r#"{"old":true}"#).unwrap();
        atomic_report(&path, r#"{"new":true}"#).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), r#"{"new":true}"#);
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn report_save_failure_stays_in_structured_output_without_remote_or_path_details() {
        let missing = std::env::temp_dir()
            .join(format!(
                "missing-engine-report-{:016x}",
                rand::random::<u64>()
            ))
            .join("report.json");
        let mut report = Report::default();
        let json = serialize_and_save_report(missing.to_str(), &mut report).unwrap();
        assert_eq!(report.error.unwrap().code, Code::Output);
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["error"]["stage"], "report_file");
        assert_eq!(value["stop_reason"], "failed");
        assert!(!json.contains("missing-engine-report"));
    }
}
