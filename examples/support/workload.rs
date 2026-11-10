use anyhow::Result;
use prost::Message;
use std::io::Write;
use tiktok_live_rust::protocol::*;

pub fn inputs() -> Result<Vec<Vec<u8>>> {
    let mut frames = Vec::new();
    for frame in 0..256u64 {
        let mut messages = Vec::new();
        for slot in 0..128u64 {
            let id = frame * 128 + slot + 1;
            let (method, payload) = match slot % 8 {
                0 | 1 => (
                    "WebcastGiftMessage",
                    Gift {
                        gift_id: 5655,
                        repeat_count: 3,
                        repeat_end: true,
                        group_id: id,
                        user: Some(User { id: 42 }),
                        gift: Some(GiftMeta {
                            kind: 1,
                            diamonds: 1,
                            name: "Rose".into(),
                        }),
                        receiver_id: "100000000000000001".into(),
                    }
                    .encode_to_vec(),
                ),
                2 => (
                    "WebcastChatMessage",
                    Chat {
                        user: Some(User { id: 42 }),
                        content: "Synthetic chat, no personal data. Hello LIVE!".into(),
                    }
                    .encode_to_vec(),
                ),
                3 => (
                    "WebcastLikeMessage",
                    Like {
                        count: 5,
                        total: id * 5,
                        user: Some(User { id: 42 }),
                    }
                    .encode_to_vec(),
                ),
                4 => (
                    "WebcastSocialMessage",
                    Social {
                        common: Some(Common {
                            msg_id: id,
                            display_text: Some(DisplayText {
                                key: "pm_main_follow_message_viewer_2".into(),
                            }),
                        }),
                        user: Some(User { id: 42 }),
                        action: 1,
                    }
                    .encode_to_vec(),
                ),
                5 => (
                    "WebcastMemberMessage",
                    Member {
                        user: Some(User { id: 42 }),
                        action: 1,
                    }
                    .encode_to_vec(),
                ),
                6 => (
                    "WebcastRoomUserSeqMessage",
                    Viewers { count: 1234 }.encode_to_vec(),
                ),
                _ => (
                    "WebcastUnknownFutureMessage",
                    MessageHeader {
                        common: Some(Common {
                            msg_id: id,
                            display_text: Some(DisplayText {
                                key: "unknown".repeat(20),
                            }),
                        }),
                    }
                    .encode_to_vec(),
                ),
            };
            messages.push(Envelope {
                method: method.into(),
                payload,
                msg_id: if slot % 2 == 0 { id } else { 0 },
                is_history: false,
            });
        }
        // Keep all supported IDs nonzero, including common-header-free fixtures.
        for (slot, m) in messages.iter_mut().enumerate() {
            if slot % 8 != 7 {
                m.msg_id = frame * 128 + slot as u64 + 1;
            }
        }
        let payload = Batch {
            messages,
            internal_ext: String::new(),
            need_ack: true,
        }
        .encode_to_vec();
        let payload = if frame % 2 == 0 {
            let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            gzip.write_all(&payload)?;
            gzip.finish()?
        } else {
            payload
        };
        frames.push(packet("msg", payload, frame + 1));
    }
    Ok(frames)
}
