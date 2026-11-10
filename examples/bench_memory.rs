//! Synthetic heap accounting only. The production engine uses Rust's normal allocator.
//! Input buffers are built before measurement; requested heap bytes exclude allocator/OS overhead.
use anyhow::{Result, ensure};
use prost::{Message, bytes::Bytes};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    io::Write,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tiktok_live_rust::{Processor, protocol::*};
#[path = "support/workload.rs"]
mod workload;

struct Counter;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static CALLS: AtomicUsize = AtomicUsize::new(0);
static REALLOCS: AtomicUsize = AtomicUsize::new(0);
static REQUESTED: AtomicUsize = AtomicUsize::new(0);
static MEASURE: AtomicBool = AtomicBool::new(false);
fn allocated(size: usize, previous: usize, reallocated: bool) {
    let current = if size >= previous {
        LIVE.fetch_add(size - previous, Ordering::Relaxed) + size - previous
    } else {
        LIVE.fetch_sub(previous - size, Ordering::Relaxed) - (previous - size)
    };
    if MEASURE.load(Ordering::Relaxed) {
        PEAK.fetch_max(current, Ordering::Relaxed);
        CALLS.fetch_add(1, Ordering::Relaxed);
        REALLOCS.fetch_add(usize::from(reallocated), Ordering::Relaxed);
        REQUESTED.fetch_add(size, Ordering::Relaxed);
    }
}
// SAFETY: Every operation forwards the exact pointer/layout to System; counters never
// allocate, dereference returned pointers or alter alignment. Null allocations are ignored.
unsafe impl GlobalAlloc for Counter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            allocated(layout.size(), 0, false);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        unsafe {
            System.dealloc(p, layout);
        }
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, p: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let out = unsafe { System.realloc(p, layout, size) };
        if !out.is_null() {
            allocated(size, layout.size(), true);
        }
        out
    }
}
#[global_allocator]
static ALLOC: Counter = Counter;

fn large_batch(unknown: bool) -> Result<Bytes> {
    let mut messages = Vec::new();
    for id in 1..=1024 {
        messages.push(Envelope {
            method: if unknown {
                "WebcastFutureMessage"
            } else {
                "WebcastChatMessage"
            }
            .into(),
            payload: if unknown {
                vec![b'x'; 4096]
            } else {
                Chat {
                    user: Some(User { id: 42 }),
                    content: "x".repeat(4096),
                }
                .encode_to_vec()
            },
            msg_id: id,
            is_history: false,
        });
    }
    let bytes = Batch {
        messages,
        internal_ext: String::new(),
        need_ack: true,
    }
    .encode_to_vec();
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    gzip.write_all(&bytes)?;
    Ok(packet("msg", gzip.finish()?, 1).into())
}

fn measure(name: &str, frames: &[Bytes], passes: usize) -> Result<serde_json::Value> {
    // Bytes lazily creates a shared-owner header on its first clone. It belongs to the
    // prepared input, not the processor: establish that ownership before the baseline.
    for frame in frames {
        std::hint::black_box(frame.clone());
    }
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    CALLS.store(0, Ordering::Relaxed);
    REALLOCS.store(0, Ordering::Relaxed);
    REQUESTED.store(0, Ordering::Relaxed);
    MEASURE.store(true, Ordering::Relaxed);
    let mut processor = Processor::new("7704162849974782734");
    let mut events = 0;
    let mut output = Vec::with_capacity(8192);
    let mut serialized_bytes = 0;
    for _ in 0..passes {
        for frame in frames {
            let (_, batch) = decode_shared(frame.clone())?;
            for raw in batch.unwrap().messages() {
                if let Some(event) = processor.process_ref(raw?)? {
                    output.clear();
                    serde_json::to_writer(&mut output, &event)?;
                    events += 1;
                    serialized_bytes += output.len();
                }
            }
        }
    }
    MEASURE.store(false, Ordering::Relaxed);
    let peak = PEAK.load(Ordering::Relaxed).saturating_sub(baseline);
    let retained = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
    let calls = CALLS.load(Ordering::Relaxed);
    let reallocs = REALLOCS.load(Ordering::Relaxed);
    let requested = REQUESTED.load(Ordering::Relaxed);
    drop(processor);
    drop(output);
    let remaining = LIVE.load(Ordering::Relaxed);
    ensure!(
        remaining == baseline,
        "measured allocations remain in {name}: before {baseline}, after {remaining}"
    );
    Ok(
        serde_json::json!({"name": name, "frames": frames.len()*passes, "events": events,
        "serialized_bytes": serialized_bytes, "peak_incremental_heap_bytes": peak,
        "retained_state_and_output_bytes": retained, "allocation_calls": calls,
        "reallocation_calls": reallocs, "total_requested_allocation_bytes": requested}),
    )
}

fn main() -> Result<()> {
    let mixed: Vec<Bytes> = workload::inputs()?.into_iter().map(Bytes::from).collect();
    let chat = [large_batch(false)?];
    let unknown = [large_batch(true)?];
    let results = [
        measure("mixed", &mixed, 8)?,
        measure("large_gzip_chat", &chat, 1)?,
        measure("large_gzip_unknown", &unknown, 1)?,
    ];
    println!(
        "{}",
        serde_json::to_string_pretty(
            &serde_json::json!({"version": env!("CARGO_PKG_VERSION"), "scenarios": results,
        "limits": "Requested heap only, excluding prepared input, allocator overhead, native libraries, stacks and OS pages. Not production throughput or live RAM."})
        )?
    );
    Ok(())
}
