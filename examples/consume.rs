//! Run explicitly with: cargo run --example consume -- USERNAME
//! Cargo test/check compiles this example without contacting TikTok.
use anyhow::{Context, Result, bail};
use std::collections::BTreeMap;
use tiktok_live_rust::{
    engine::{self, Config, Target},
    transport::DirectSource,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let username = std::env::args()
        .nth(1)
        .context("provide a public LIVE username")?;
    let source = DirectSource::new()?;
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    let signal_task = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal.cancel();
        }
    });
    let (sender, mut receiver) = mpsc::channel(256);
    let (status, mut observer) = engine::status_channel();
    let engine = engine::run_with_status(
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
        let mut latest = observer.borrow().clone();
        while observer.changed().await.is_ok() {
            // Clone and release the watch borrow BEFORE calling UI/async work.
            latest = observer.borrow_and_update().clone();
            // Update a UI model here; do not block the runtime with I/O.
            // Intermediate states can coalesce; the final snapshot remains available.
        }
        latest
    };
    let consumer = async {
        let mut counts = BTreeMap::<String, u64>::new();
        while let Some(event) = receiver.recv().await {
            // Handle the typed Event here. Avoid slow/blocking work in this loop.
            // Track room_id and connection_epoch to detect possible delivery gaps.
            *counts.entry(event.kind).or_default() += 1;
        }
        counts
    };
    let (report, counts, last_status) = tokio::join!(engine, consumer, health);
    signal_task.abort();
    println!(
        "{}",
        serde_json::to_string_pretty(
            &serde_json::json!({"events_consumed": counts, "last_status": last_status, "report": report})
        )?
    );
    if report.error.is_some() {
        bail!("engine stopped with an error; see report");
    }
    Ok(())
}
