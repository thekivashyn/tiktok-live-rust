//! Synthetic wire → bounded decoder → normalizer → NDJSON. No network or accounts.
use anyhow::Result;
use prost::Message;
use std::io::{self, Write};
use tiktok_live_rust::{
    Event, Processor,
    metadata::WireUser,
    protocol::{self, Batch, Chat, Control, Envelope, Gift, GiftMeta, Like, User, Viewers},
};

fn envelope(method: &str, payload: Vec<u8>, msg_id: u64) -> Envelope {
    Envelope {
        method: method.into(),
        payload,
        msg_id,
        is_history: false,
    }
}

fn demo_events() -> Result<Vec<Event>> {
    let sender = 9_007_199_254_740_993;
    let profile = WireUser {
        id: sender,
        display_id: "demo_creator".into(),
        nickname: "Demo Creator".into(),
        ..Default::default()
    };
    let mut chat = vec![0x12]; // Chat.user (field 2), using our selected profile model.
    prost::encoding::encode_varint(profile.encoded_len() as u64, &mut chat);
    profile.encode(&mut chat)?;
    chat.extend(
        Chat {
            user: None,
            content: "Hello from the offline demo!".into(),
        }
        .encode_to_vec(),
    );
    let gift = |repeat_count, repeat_end| {
        Gift {
            gift_id: 5655,
            repeat_count,
            repeat_end,
            group_id: 77,
            user: Some(User { id: sender }),
            receiver_id: "42".into(),
            gift: Some(GiftMeta {
                kind: 1,
                diamonds: 1,
                name: "Demo Rose".into(),
            }),
        }
        .encode_to_vec()
    };
    let first = envelope("WebcastChatMessage", chat, 101);
    let batch = Batch {
        messages: vec![
            first.clone(),
            first, // The same message twice: one chat is emitted.
            envelope("WebcastGiftMessage", gift(1, false), 102),
            envelope("WebcastGiftMessage", gift(3, true), 103),
            envelope(
                "WebcastLikeMessage",
                Like {
                    count: 5,
                    total: 105,
                    user: Some(User { id: sender }),
                }
                .encode_to_vec(),
                104,
            ),
            envelope(
                "WebcastRoomUserSeqMessage",
                Viewers { count: 42 }.encode_to_vec(),
                105,
            ),
            envelope(
                "WebcastControlMessage",
                Control { action: 3 }.encode_to_vec(),
                106,
            ),
        ],
        ..Default::default()
    };
    let (_, decoded) =
        protocol::decode_shared(protocol::packet("msg", batch.encode_to_vec(), 1).into())?;
    let mut processor = Processor::new("42");
    let mut events = Vec::new();
    for raw in decoded.expect("synthetic message batch").messages() {
        if let Some(mut event) = processor.process_ref(raw?)? {
            event.sequence = events.len() as u64 + 1;
            event.connection_epoch = 1;
            events.push(event);
        }
    }
    Ok(events)
}

fn main() -> Result<()> {
    let mut output = io::BufWriter::new(io::stdout().lock());
    for event in demo_events()? {
        serde_json::to_writer(&mut output, &event)?;
        output.write_all(b"\n")?;
    }
    output.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tiktok_live_rust::EventData;
    #[test]
    fn synthetic_wire_preserves_identity_deduplication_and_gift_deltas() {
        let events = demo_events().unwrap();
        assert_eq!(
            events.iter().map(|e| e.kind.as_str()).collect::<Vec<_>>(),
            ["chat", "gift", "gift", "like", "viewers", "stream_end"]
        );
        assert_eq!(
            events[0].user.as_ref().unwrap().username.as_deref(),
            Some("demo_creator")
        );
        assert_eq!(events[0].user_id.as_deref(), Some("9007199254740993"));
        assert!(matches!(
            events[1].data,
            EventData::Gift {
                quantity_delta: Some(1),
                ..
            }
        ));
        assert!(matches!(
            events[2].data,
            EventData::Gift {
                quantity_delta: Some(2),
                ..
            }
        ));
        assert!(
            events
                .iter()
                .enumerate()
                .all(|(i, e)| e.sequence == i as u64 + 1 && e.connection_epoch == 1)
        );
        assert!(events.iter().all(|e| !e.metadata_incomplete));
    }
}
