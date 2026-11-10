//! Test-only oracle pipe. Raw payloads go to stdout; use scripts/check_metadata.py, never a log.
use anyhow::{Result, ensure};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    time::Duration,
};
use tiktok_live_rust::{
    EventData, Processor,
    engine::{Source, Target},
    protocol::{self, EnvelopeRef},
    transport::DirectSource,
};
use tokio::time::{Instant, interval, sleep_until, timeout};
use tokio_tungstenite::tungstenite::Message;

#[derive(Default, serde::Serialize)]
struct Stats {
    events: u64,
    frames: u64,
    decode_errors: u64,
    metadata_incomplete: u64,
    completed: bool,
}
fn emit(
    raw: EnvelopeRef<'_>,
    processor: &mut Processor,
    output: &mut impl Write,
    stats: &mut Stats,
) -> Result<()> {
    let event = match processor.process_ref(raw) {
        Ok(Some(value)) => value,
        Ok(None) => return Ok(()),
        Err(_) => {
            stats.decode_errors += 1;
            return Ok(());
        }
    };
    stats.events += 1;
    stats.metadata_incomplete += u64::from(event.metadata_incomplete);
    let hex: String = raw
        .payload
        .iter()
        .flat_map(|b| {
            [
                char::from(b"0123456789abcdef"[(b >> 4) as usize]),
                char::from(b"0123456789abcdef"[(b & 15) as usize]),
            ]
        })
        .collect();
    let like_core = matches!(&event.data, EventData::Like { .. }).then_some(&event.data);
    serde_json::to_writer(
        &mut *output,
        &serde_json::json!({"method":raw.method,"kind":event.kind,"payload_hex":hex,"metadata_incomplete":event.metadata_incomplete,"expected":{"user":event.user,"like_details":event.like_details,"like_core":like_core}}),
    )?;
    output.write_all(b"\n")?;
    Ok(())
}
#[derive(Deserialize)]
struct Row {
    method: String,
    msg_id: String,
    payload_hex: String,
}
fn unhex(value: &str) -> Result<Vec<u8>> {
    ensure!(
        value.len().is_multiple_of(2) && value.len() <= protocol::MAX_EVENT_WIRE * 2,
        "invalid hex length"
    );
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let a = char::from(pair[0]).to_digit(16);
            let b = char::from(pair[1]).to_digit(16);
            match (a, b) {
                (Some(a), Some(b)) => Ok((a * 16 + b) as u8),
                _ => anyhow::bail!("invalid hex"),
            }
        })
        .collect()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    ensure!(
        args.len() >= 2,
        "--fixture PATH | --user USERNAME [seconds=120]"
    );
    let mut output = BufWriter::new(std::io::stdout());
    let mut stats = Stats::default();
    if args[0] == "--fixture" {
        let mut processor = Processor::new("42");
        for line in BufReader::new(std::fs::File::open(&args[1])?).lines() {
            let row: Row = serde_json::from_str(&line?)?;
            let payload = unhex(&row.payload_hex)?;
            emit(
                EnvelopeRef {
                    method: &row.method,
                    payload: &payload,
                    msg_id: row.msg_id.parse()?,
                    is_history: false,
                },
                &mut processor,
                &mut output,
                &mut stats,
            )?;
        }
        stats.completed = true;
    } else {
        ensure!(args[0] == "--user", "unknown mode");
        let seconds: u64 = args.get(2).map(|v| v.parse()).transpose()?.unwrap_or(120);
        ensure!((10..=900).contains(&seconds), "invalid duration");
        let direct = DirectSource::new()?;
        let mut remote = timeout(
            Duration::from_secs(30),
            direct.connect(&Target::User(args[1].clone())),
        )
        .await??;
        let mut processor = Processor::new(&remote.room_id);
        let room = remote.room_id.parse()?;
        timeout(
            Duration::from_secs(5),
            remote
                .socket
                .send(Message::Binary(protocol::enter(room).into())),
        )
        .await??;
        let deadline = Instant::now() + Duration::from_secs(seconds);
        let mut heartbeat = interval(Duration::from_secs(10));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { biased;
                _=sleep_until(deadline)=>{stats.completed=true;break},
                _=heartbeat.tick()=>{timeout(Duration::from_secs(5),remote.socket.send(Message::Binary(protocol::heartbeat(room).into()))).await??;},
                next=remote.socket.next()=>{
                    let Some(message)=next else {break};
                    match message? {
                        Message::Binary(bytes)=>{
                            stats.frames+=1;
                            let (frame,batch)=match protocol::decode_shared(bytes) {Ok(value)=>value,Err(_)=>{stats.decode_errors+=1;continue}};
                            if let Some(batch)=batch {
                                for raw in batch.messages() { emit(raw?,&mut processor,&mut output,&mut stats)?; }
                                output.flush()?;
                                if frame.log_id!=0 {timeout(Duration::from_secs(5),remote.socket.send(Message::Binary(protocol::packet("ack",b"-".to_vec(),frame.log_id).into()))).await??;}
                            }
                        },
                        Message::Ping(value)=>{timeout(Duration::from_secs(5),remote.socket.send(Message::Pong(value))).await??;},
                        Message::Close(_)=>break,
                        _=>{}
                    }
                }
            }
        }
    }
    output.flush()?;
    eprintln!("{}", serde_json::to_string(&stats)?);
    ensure!(
        stats.completed && stats.decode_errors == 0 && stats.metadata_incomplete == 0,
        "capture validation failed"
    );
    Ok(())
}
