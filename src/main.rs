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
    transport::DirectSource,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

struct Options {
    target: Target,
    config: Config,
    events: bool,
    status: bool,
    report: Option<String>,
}
fn parse(args: &[String]) -> Result<Options> {
    let mut target = None;
    let mut config = Config::default();
    let mut events = false;
    let mut status = false;
    let mut explicit_reconnect_limit = false;
    let mut report = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--events" => events = true,
            "--status" => status = true,
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

/// Blocking stderr writes stay off the socket runtime. There is only one pending snapshot
/// and one final report. Slow diagnostics may skip intermediate states, never queue events.
fn status_writer(
    mut output: impl Write,
    mut status: watch::Receiver<ConnectionStatus>,
    mut final_report: oneshot::Receiver<Report>,
) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    runtime.block_on(async {
        let mut watching = true;
        loop {
            tokio::select! { biased;
                report = &mut final_report => {
                    let report = report.context("report sender closed")?;
                    let snapshot = status.borrow().clone();
                    write_diagnostic(&mut output, "status", &snapshot)?;
                    write_diagnostic(&mut output, "report", &report)?;
                    return Ok(());
                },
                changed = status.changed(), if watching => {
                    if changed.is_err() { watching = false; continue; }
                    // Release the watch read lock BEFORE serializing or writing a pipe.
                    let snapshot = status.borrow_and_update().clone();
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
            "tiktok-live-rust --user USER | --room ROOM [--seconds 60] [--max-reconnects 8 | --retry-until-deadline] [--events] [--status] [--report PATH]\nDeveloped by Tri Nguyen\nDirect guest engine. No Euler. Duration 1..86400 seconds; reconnect budget 0..32 per outage.\n--retry-until-deadline: retry transient errors until duration/Ctrl+C; honors backoff and Retry-After.\nDefault: aggregate JSON. --events: NDJSON events on stdout, report on stderr.\n--status: coalesced NDJSON health snapshots and tagged final report on stderr. Ctrl+C stops."
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
    let (status, diagnostics) = if options.status {
        let (sender, receiver) = engine::status_channel();
        let (final_sender, final_receiver) = oneshot::channel();
        let (done, finished) = oneshot::channel();
        std::thread::Builder::new()
            .name("status-output".into())
            .spawn(move || {
                let stderr = std::io::stderr();
                let result = status_writer(stderr.lock(), receiver, final_receiver);
                let _ = done.send(result.is_ok());
            })?;
        (Some(sender), Some((final_sender, finished)))
    } else {
        (None, None)
    };
    let mut report = if let Some(status) = status {
        engine::run_with_status(
            &source,
            options.target,
            options.config,
            output,
            cancel,
            status,
        )
        .await
    } else {
        engine::run(&source, options.target, options.config, output, cancel).await
    };
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
    if options.events && !options.status {
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
                "--events",
                "--retry-until-deadline",
            ]
            .map(String::from),
        )
        .unwrap();
        assert!(options.status && options.events && options.config.retry_until_deadline);
        let default = parse(&["--user", "alice"].map(String::from)).unwrap();
        assert!(!default.status && !default.events && !default.config.retry_until_deadline);
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
