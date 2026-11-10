//! Short direct LIVE diagnosis. Retains wire field shapes and aggregate counts, never payloads.
use anyhow::{Result, ensure};
use futures_util::{SinkExt, StreamExt};
use prost::{
    Message as _,
    encoding::{DecodeContext, decode_key, skip_field},
};
use std::{collections::BTreeMap, time::Duration};
use tiktok_live_rust::{
    Processor,
    engine::{Source, Target},
    protocol,
    transport::DirectSource,
};
use tokio::time::{Instant, interval, sleep_until, timeout};
use tokio_tungstenite::tungstenite::Message;

fn shape(mut bytes: &[u8]) -> Vec<serde_json::Value> {
    let mut result = Vec::new();
    while !bytes.is_empty() && result.len() < 64 {
        let Ok((tag, wire)) = decode_key(&mut bytes) else {
            break;
        };
        let before = bytes.len();
        if skip_field(wire, tag, &mut bytes, DecodeContext::default()).is_err() {
            break;
        }
        result.push(serde_json::json!({"tag":tag,"wire":format!("{wire:?}"),"encoded_value_bytes":before-bytes.len()}));
    }
    result
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    ensure!(!args.is_empty(), "username [seconds=300]");
    let seconds: u64 = args.get(1).map(|x| x.parse()).transpose()?.unwrap_or(300);
    ensure!((1..=900).contains(&seconds), "invalid duration");
    let direct = DirectSource::new()?;
    let mut remote = timeout(
        Duration::from_secs(30),
        direct.connect(&Target::User(args[0].clone())),
    )
    .await??;
    let mut processor = Processor::new(&remote.room_id);
    let room: u64 = remote.room_id.parse()?;
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
    let mut counts = BTreeMap::<String, u64>::new();
    let mut examples = Vec::new();
    loop {
        tokio::select! { biased;
            _ = sleep_until(deadline) => break,
            _ = heartbeat.tick() => { timeout(Duration::from_secs(5),remote.socket.send(Message::Binary(protocol::heartbeat(room).into()))).await??; },
            next = remote.socket.next() => {
                let Some(message) = next else { break };
                match message? {
                    Message::Binary(bytes) => {
                        let (frame,batch) = match protocol::decode_shared(bytes) {
                            Ok(value) => value,
                            Err(_) => { *counts.entry("frame_decode_error".into()).or_default()+=1; continue; }
                        };
                        if let Some(batch) = batch {
                            for raw in batch.messages() {
                                let raw=raw?;
                                let processed=processor.process_ref(raw);
                                if processed.is_err() {
                                    let category=if raw.method=="WebcastChatMessage" { "chat" } else { "other" };
                                    *counts.entry(format!("{category}_process_error")).or_default()+=1;
                                    if category=="chat" {
                                        match protocol::Chat::decode(raw.payload) {
                                            Ok(chat) => {
                                                let reason=if chat.content.is_empty() { "empty_chat" } else if chat.content.len()>4096 { "oversized_chat" } else { "other_chat_error" };
                                                *counts.entry(reason.into()).or_default()+=1;
                                                if examples.len()<16 {
                                                    let sample=serde_json::json!({"reason":reason,"content_bytes":chat.content.len(),"user_present":chat.user.is_some(),"payload_bytes":raw.payload.len(),"fields":shape(raw.payload)});
                                                    eprintln!("{sample}"); examples.push(sample);
                                                }
                                            },
                                            Err(_) => { *counts.entry("chat_wire_error".into()).or_default()+=1; }
                                        }
                                    }
                                } else if let Ok(Some(event))=processed {
                                    *counts.entry(format!("event_{}",event.kind)).or_default()+=1;
                                }
                            }
                            if frame.log_id!=0 {
                                timeout(Duration::from_secs(5),remote.socket.send(Message::Binary(protocol::packet("ack",b"-".to_vec(),frame.log_id).into()))).await??;
                            }
                        }
                    },
                    Message::Ping(bytes) => { timeout(Duration::from_secs(5),remote.socket.send(Message::Pong(bytes))).await??; },
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        }
    }
    println!(
        "{}",
        serde_json::json!({"counts":counts,"error_shapes":examples,"raw_content_saved":false,"requested_seconds":seconds})
    );
    Ok(())
}
