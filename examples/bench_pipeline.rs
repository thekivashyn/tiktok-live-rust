//! Offline, deterministic workload. This measures codec + normalization + JSON, not network latency.
use anyhow::{Result, ensure};
use prost::bytes::Bytes;
use std::{hint::black_box, time::Instant};
use tiktok_live_rust::{Processor, protocol::*};

#[path = "support/workload.rs"]
mod workload;
use workload::inputs;

fn process(frames: &[Bytes], passes: u64) -> Result<(u64, u64)> {
    let mut processor = Processor::new("7704162849974782734");
    let mut output = Vec::with_capacity(1024);
    let mut events = 0u64;
    let mut output_bytes = 0u64;
    for _ in 0..passes {
        for frame in frames {
            let (_, batch) = decode_shared(black_box(frame.clone()))?;
            for raw in batch.unwrap().messages() {
                if let Some(event) = processor.process_ref(raw?)? {
                    output.clear();
                    serde_json::to_writer(&mut output, &event)?;
                    output_bytes += output.len() as u64;
                    black_box(&output);
                    events += 1;
                }
            }
        }
    }
    Ok((events, output_bytes))
}

fn main() -> Result<()> {
    let passes: u64 = std::env::args()
        .nth(1)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(32);
    ensure!((1..=4096).contains(&passes), "passes must be 1..4096");
    let frames: Vec<Bytes> = inputs()?.into_iter().map(Bytes::from).collect();
    process(&frames, 1)?; // warm-up; excluded from timing
    let started = Instant::now();
    let (events, bytes) = process(&frames, passes)?;
    let elapsed = started.elapsed().as_secs_f64();
    ensure!(events == passes * 256 * 112, "unexpected event count");
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"), "passes": passes, "frames": passes * 256,
            "events": events, "serialized_bytes": bytes, "elapsed_seconds": elapsed,
            "events_per_second": events as f64 / elapsed,
            "workload": "128 messages/batch; 50% gzip; gift/chat/like/follow/join/viewers/unknown; synthetic",
            "network_latency_measured": false
        }))?
    );
    Ok(())
}
