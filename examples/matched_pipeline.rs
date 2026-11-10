use anyhow::{Result, ensure};
use std::{path::Path, time::Instant};
use tiktok_live_rust::{Processor, protocol::decode_shared};
#[path = "support/matched.rs"]
mod matched;

fn run(
    frames: &[prost::bytes::Bytes],
    passes: u32,
    capture: Option<&str>,
) -> Result<matched::Output> {
    let mut processor = Processor::new("42");
    let mut output = matched::Output::default();
    if let Some(path) = capture {
        output.capture = Some(std::fs::File::create(path)?);
    }
    for _ in 0..passes {
        for frame in frames {
            let (_, batch) = decode_shared(frame.clone())?;
            for envelope in batch.unwrap().messages() {
                if let Some(event) = processor.process_ref(envelope?)? {
                    output.push(event)?;
                }
            }
        }
    }
    Ok(output)
}
fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    ensure!(args.len() >= 2, "corpus passes [capture]");
    let passes: u32 = args[1].parse()?;
    ensure!((1..=1024).contains(&passes), "invalid passes");
    let frames = matched::corpus(Path::new(&args[0]))?;
    run(&frames, 1, None)?;
    let start = Instant::now();
    let output = run(&frames, passes, args.get(2).map(String::as_str))?;
    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "{}",
        serde_json::json!({"implementation":"rust","version":env!("CARGO_PKG_VERSION"),"frames":frames.len() as u64*u64::from(passes),"passes":passes,"elapsed_seconds":elapsed,"events_per_second":output.events as f64/elapsed,"output":output.summary()})
    );
    Ok(())
}
