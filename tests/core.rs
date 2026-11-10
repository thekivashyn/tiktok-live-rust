use prost::Message;
use std::io::Write;
use tiktok_live_rust::{Processor, protocol::*, transport::socket_url};

fn data(event: &tiktok_live_rust::Event) -> serde_json::Value {
    serde_json::to_value(&event.data).unwrap()
}

fn gift(id: u64, count: u32, end: bool, group: u64) -> Envelope {
    Envelope {
        method: "WebcastGiftMessage".into(),
        msg_id: id,
        is_history: false,
        payload: Gift {
            gift_id: 5655,
            repeat_count: count,
            repeat_end: end,
            group_id: group,
            user: Some(User {
                id: 9007199254740993,
            }),
            gift: Some(GiftMeta {
                kind: 1,
                diamonds: 1,
                name: "Rose".into(),
            }),
            receiver_id: "host".into(),
        }
        .encode_to_vec(),
    }
}

#[test]
fn streak_updates_final_and_duplicates_count_three_not_nine() {
    let mut p = Processor::new("room-a");
    let mut sum = 0;
    for raw in [
        gift(1, 1, false, 10),
        gift(2, 2, false, 10),
        gift(2, 2, false, 10),
        gift(3, 3, false, 10),
        gift(4, 3, true, 10),
    ] {
        if let Some(event) = p.process(&raw).unwrap() {
            sum += data(&event)["quantity_delta"].as_u64().unwrap();
        }
    }
    assert_eq!(sum, 3);
    assert_eq!(p.duplicates, 1);
}

#[test]
fn late_smaller_update_cannot_roll_back_a_streak() {
    let mut p = Processor::new("r");
    for (id, count, expected) in [(1, 3, 3), (2, 2, 0), (3, 4, 1)] {
        assert_eq!(
            data(&p.process(&gift(id, count, false, 4)).unwrap().unwrap())["quantity_delta"],
            expected
        );
    }
}

#[test]
fn evicted_streak_reports_unknown_delta_then_resynchronizes() {
    let mut p = Processor::new("r");
    for group in 1..=4097 {
        assert_eq!(
            data(&p.process(&gift(group, 1, false, group)).unwrap().unwrap())["quantity_delta"],
            1
        );
    }
    // Group 1 has been evicted. Its cumulative count is not a newly received quantity.
    let update = p.process(&gift(9000, 3, false, 1)).unwrap().unwrap();
    assert_eq!(data(&update)["repeat_count"], 3);
    assert!(data(&update)["quantity_delta"].is_null());
    assert_eq!(
        data(&p.process(&gift(9001, 4, false, 1)).unwrap().unwrap())["quantity_delta"],
        1
    );
    assert_eq!(
        data(&p.process(&gift(9002, 4, true, 1)).unwrap().unwrap())["quantity_delta"],
        0
    );
    assert_eq!((p.streak_evictions, p.streak_resyncs), (2, 1));
    assert!(p.process(&gift(9002, 4, true, 1)).unwrap().is_none());
    assert_eq!(p.streak_resyncs, 1);
    // A first-count update is still usable; a fresh room has no lost window context.
    assert_eq!(
        data(&p.process(&gift(9003, 1, false, 10000)).unwrap().unwrap())["quantity_delta"],
        1
    );
    let mut fresh = Processor::new("other-room");
    assert_eq!(
        data(&fresh.process(&gift(1, 3, false, 1)).unwrap().unwrap())["quantity_delta"],
        3
    );
}

#[test]
fn independent_groups_and_rooms_do_not_bleed() {
    let mut a = Processor::new("a");
    let mut b = Processor::new("b");
    let raw = gift(1, 2, true, 1);
    assert_eq!(a.process(&raw).unwrap().unwrap().room_id, "a");
    assert_eq!(b.process(&raw).unwrap().unwrap().room_id, "b");
    assert_eq!(
        data(&a.process(&gift(2, 2, true, 2)).unwrap().unwrap())["quantity_delta"],
        2
    );
}

#[test]
fn missing_streak_id_waits_for_final_and_large_ids_stay_strings() {
    let mut p = Processor::new("7704169130073836318");
    assert!(data(&p.process(&gift(1, 2, false, 0)).unwrap().unwrap())["quantity_delta"].is_null());
    let event = p.process(&gift(u64::MAX, 2, true, 0)).unwrap().unwrap();
    assert_eq!(data(&event)["quantity_delta"], 2);
    assert_eq!(event.user_id.as_deref(), Some("9007199254740993"));
    assert_eq!(event.message_id, u64::MAX.to_string());
}

#[test]
fn history_and_invalid_messages_do_not_poison_dedupe() {
    let mut p = Processor::new("r");
    let mut raw = gift(7, 1, true, 1);
    raw.is_history = true;
    assert!(p.process(&raw).unwrap().is_none());
    raw.is_history = false;
    let valid = raw.payload.clone();
    raw.payload = vec![255];
    assert!(p.process(&raw).is_err());
    raw.payload = valid;
    assert!(p.process(&raw).unwrap().is_some());
    assert_eq!(p.history_skipped, 1);
}

#[test]
fn real_wire_bytes_preserve_chat() {
    // Independent fixed wire fixture: user.id=42, content="hi"; no encode helper under test.
    let raw = Envelope {
        method: "WebcastChatMessage".into(),
        payload: vec![0x12, 2, 8, 42, 0x1a, 2, b'h', b'i'],
        msg_id: 9,
        is_history: false,
    };
    let event = Processor::new("r").process(&raw).unwrap().unwrap();
    assert_eq!(event.user_id.as_deref(), Some("42"));
    assert_eq!(data(&event)["text"], "hi");
}

#[test]
fn gzip_batch_and_unknown_frames_are_supported() {
    let batch = Batch {
        messages: vec![gift(1, 1, true, 1)],
        internal_ext: "opaque".into(),
        need_ack: true,
    };
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gzip.write_all(&batch.encode_to_vec()).unwrap();
    let bytes = packet("msg", gzip.finish().unwrap(), 33);
    let (frame, decoded) = decode(&bytes).unwrap();
    assert_eq!(frame.log_id, 33);
    assert_eq!(decoded.unwrap().messages.len(), 1);
    assert!(
        decode(&packet("future_frame", vec![255], 0))
            .unwrap()
            .1
            .is_none()
    );
}

#[test]
fn oversized_and_invalid_frames_are_rejected() {
    assert!(decode(&[255]).is_err());
    assert!(decode(&vec![0; MAX_FRAME + 1]).is_err());
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    gzip.write_all(&vec![0; MAX_DECOMPRESSED + 1]).unwrap();
    assert!(decode(&packet("msg", gzip.finish().unwrap(), 1)).is_err());
}

#[test]
fn direct_url_has_no_external_signer_and_rejects_injected_room() {
    let url = socket_url("7704169130073836318").unwrap();
    assert_eq!(url.host_str(), Some("webcast-ws.tiktok.com"));
    assert!(url.query_pairs().any(|(k, v)| k == "ws_direct" && v == "1"));
    assert!(!url.query_pairs().any(|(k, _)| k == "X-Gnarly"));
    assert!(socket_url("1&room_id=2").is_err());
}

#[test]
fn room_entry_and_heartbeat_use_protobuf_numeric_room() {
    let frame = Frame::decode(enter(42).as_slice()).unwrap();
    assert_eq!(frame.kind, "im_enter_room");
    assert_eq!(
        frame.payload,
        [
            8, 42, 32, 12, 42, 8, b'a', b'u', b'd', b'i', b'e', b'n', b'c', b'e'
        ]
    );
    assert_eq!(
        Frame::decode(heartbeat(42).as_slice()).unwrap().payload,
        [8, 42]
    );
}

#[test]
fn missing_envelope_id_uses_common_header_for_dedupe() {
    // Common.msg_id=42 plus chat.content="hi", independent wire fixture.
    let raw = Envelope {
        method: "WebcastChatMessage".into(),
        msg_id: 0,
        is_history: false,
        payload: vec![0x0a, 2, 0x10, 42, 0x1a, 2, b'h', b'i'],
    };
    let mut p = Processor::new("42");
    assert_eq!(p.process(&raw).unwrap().unwrap().message_id, "42");
    assert!(p.process(&raw).unwrap().is_none());
    assert_eq!(p.duplicates, 1);
}

#[test]
fn empty_envelope_flood_and_invalid_wire_lengths_are_rejected() {
    let payload: Vec<_> = [0x0a, 0]
        .into_iter()
        .cycle()
        .take((MAX_BATCH_MESSAGES + 1) * 2)
        .collect();
    assert!(decode(&packet("msg", payload, 1)).is_err());
    for payload in [
        vec![0x0a, 0xff, 0xff],
        vec![
            0x0a, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2,
        ],
        vec![0x0a, 8, 1],
        vec![0],
    ] {
        assert!(decode(&packet("msg", payload, 1)).is_err());
    }
}

#[test]
fn oversized_event_fields_are_rejected_without_poisoning_message_id() {
    let mut p = Processor::new("42");
    let mut raw = Envelope {
        method: "WebcastChatMessage".into(),
        msg_id: 1,
        is_history: false,
        payload: Chat {
            user: None,
            content: "x".repeat(4097),
        }
        .encode_to_vec(),
    };
    assert!(p.process(&raw).is_err());
    raw.payload = Chat {
        user: None,
        content: "ok".into(),
    }
    .encode_to_vec();
    assert!(p.process(&raw).unwrap().is_some());
    let mut raw = gift(2, 1, true, 1);
    let mut message = Gift::decode(raw.payload.as_slice()).unwrap();
    message.receiver_id = "x".repeat(129);
    raw.payload = message.encode_to_vec();
    assert!(p.process(&raw).is_err());
}

#[test]
fn arbitrary_wire_smoke_corpus_never_panics() {
    // Deterministic malformed corpus; a regression smoke test, not a fuzzing campaign.
    let mut seed = 0x12345678u64;
    for len in 0..512 {
        let bytes: Vec<u8> = (0..len)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed as u8
            })
            .collect();
        let _ = decode(&bytes);
        let _ = decode(&packet("msg", bytes, 1));
    }
}

fn social_fixture(key: &str, action: u8) -> Envelope {
    // Independent protobuf bytes: common.msgId=42, common.displayText.key, user.id=7.
    let mut common = vec![0x10, 42];
    if !key.is_empty() {
        common.extend([0x42, (key.len() + 2) as u8, 0x0a, key.len() as u8]);
        common.extend(key.as_bytes());
    }
    let mut payload = vec![0x0a, common.len() as u8];
    payload.extend(common);
    payload.extend([0x12, 2, 8, 7, 0x20, action]);
    Envelope {
        method: "WebcastSocialMessage".into(),
        msg_id: 0,
        is_history: false,
        payload,
    }
}

#[test]
fn follow_share_keys_override_action_and_ambiguous_social_stays_social() {
    for (key, action, expected) in [
        ("pm_main_follow_message_viewer_2", 3, "follow"),
        ("pm_mt_guidance_share", 1, "share"),
        ("", 1, "follow"),
        ("", 3, "share"),
        ("", 9, "social"),
        ("pm_unfollow_message", 1, "social"),
        ("pm_future_action", 3, "social"),
        ("pm_follow_share", 1, "social"),
    ] {
        let mut p = Processor::new("42");
        let raw = social_fixture(key, action);
        let event = p.process(&raw).unwrap().unwrap();
        assert_eq!(event.kind, expected, "key={key}");
        assert_eq!(event.user_id.as_deref(), Some("7"));
        assert_eq!(event.message_id, "42");
        assert!(p.process(&raw).unwrap().is_none());
    }
}

#[test]
fn joins_exclude_moderation_and_viewers_are_snapshots() {
    let mut p = Processor::new("42");
    let mut member = Envelope {
        method: "WebcastMemberMessage".into(),
        msg_id: 1,
        is_history: false,
        payload: vec![0x12, 2, 8, 7, 0x50, 2],
    };
    assert!(p.process(&member).unwrap().is_none());
    *member.payload.last_mut().unwrap() = 1;
    assert_eq!(p.process(&member).unwrap().unwrap().kind, "join");
    let viewers = Envelope {
        method: "WebcastRoomUserSeqMessage".into(),
        msg_id: 2,
        is_history: false,
        payload: vec![0x18, 0xac, 2],
    };
    let event = p.process(&viewers).unwrap().unwrap();
    assert_eq!(event.kind, "viewers");
    assert_eq!(data(&event)["count"], "300");
    assert!(event.user_id.is_none());
}

#[test]
fn empty_known_payloads_do_not_create_fake_events_or_poison_dedupe() {
    for method in [
        "WebcastChatMessage",
        "WebcastGiftMessage",
        "WebcastLikeMessage",
        "WebcastSocialMessage",
        "WebcastRoomUserSeqMessage",
    ] {
        let raw = Envelope {
            method: method.into(),
            msg_id: 5,
            is_history: false,
            payload: vec![],
        };
        let mut p = Processor::new("42");
        assert!(p.process(&raw).is_err(), "{method}");
        assert!(p.process(&gift(5, 1, true, 10)).unwrap().is_some());
    }
}

#[test]
fn identified_empty_chat_is_preserved_once_without_inventing_text() {
    let mut processor = Processor::new("42");
    let mut raw = Envelope {
        method: "WebcastChatMessage".into(),
        msg_id: 17,
        is_history: false,
        payload: Chat {
            user: Some(User { id: 123 }),
            content: String::new(),
        }
        .encode_to_vec(),
    };
    let event = processor.process(&raw).unwrap().unwrap();
    assert_eq!(event.kind, "chat");
    assert_eq!(event.user_id.as_deref(), Some("123"));
    assert_eq!(data(&event)["text"], "");
    assert!(processor.process(&raw).unwrap().is_none());
    // The common header can provide identity when the envelope does not.
    raw.msg_id = 0;
    raw.payload.extend(
        MessageHeader {
            common: Some(Common {
                msg_id: 18,
                display_text: None,
            }),
        }
        .encode_to_vec(),
    );
    assert_eq!(processor.process(&raw).unwrap().unwrap().message_id, "18");
}

#[test]
fn empty_chat_requires_both_identities_and_rejections_do_not_poison_dedupe() {
    for (message_id, user) in [
        (7, None),
        (7, Some(User { id: 0 })),
        (0, Some(User { id: 123 })),
    ] {
        let mut processor = Processor::new("42");
        let mut raw = Envelope {
            method: "WebcastChatMessage".into(),
            msg_id: message_id,
            is_history: false,
            payload: Chat {
                user,
                content: String::new(),
            }
            .encode_to_vec(),
        };
        assert!(processor.process(&raw).is_err());
        raw.msg_id = 7;
        raw.payload = Chat {
            user: Some(User { id: 123 }),
            content: String::new(),
        }
        .encode_to_vec();
        assert!(processor.process(&raw).unwrap().is_some());
    }
}

#[test]
fn gift_receiver_is_preserved_and_isolates_combos() {
    let mut p = Processor::new("42");
    let first = gift(1, 3, false, 10);
    let event = p.process(&first).unwrap().unwrap();
    assert_eq!(data(&event)["receiver_id"], "host");
    assert_eq!(data(&event)["gift_type"], 1);
    let mut second = gift(2, 3, false, 10);
    let mut payload = Gift::decode(second.payload.as_slice()).unwrap();
    payload.receiver_id = "cohost".into();
    second.payload = payload.encode_to_vec();
    assert_eq!(
        data(&p.process(&second).unwrap().unwrap())["quantity_delta"],
        3
    );
    assert_eq!(
        data(&p.process(&gift(3, 4, true, 10)).unwrap().unwrap())["quantity_delta"],
        1
    );
}

#[test]
fn typed_gift_preserves_existing_json_contract() {
    let event = Processor::new("7704162849974782734")
        .process(&gift(u64::MAX, 3, true, 55))
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(event).unwrap(),
        serde_json::json!({
            "sequence": 0, "connection_epoch": 0, "kind": "gift", "room_id": "7704162849974782734",
            "message_id": "18446744073709551615", "user_id": "9007199254740993",
            "user": {"id":"9007199254740993","username":null,"display_name":null,"avatar_url":null,"verified":false,"sec_uid":null,"follow_info":null},
            "data": {"gift_id": "5655", "group_id": "55", "repeat_count": 3, "repeat_end": true,
                "quantity_delta": 3, "gift_type": 1, "receiver_id": "host", "gift_name": "Rose", "diamonds_per_gift": 1}
        })
    );
}

#[test]
fn typed_payloads_keep_strings_nulls_and_json_escaping() {
    use tiktok_live_rust::EventData;
    let payloads = [
        (
            EventData::Like {
                count: 1,
                total: u64::MAX,
            },
            serde_json::json!({"count": 1, "total": "18446744073709551615"}),
        ),
        (
            EventData::Viewers { count: u64::MAX },
            serde_json::json!({"count": "18446744073709551615"}),
        ),
        (
            EventData::Social {
                action: 3,
                display_key: "pm_mt_guidance_share".into(),
            },
            serde_json::json!({"action": "3", "display_key": "pm_mt_guidance_share"}),
        ),
        (
            EventData::Chat {
                text: "Xin chào\n\"LIVE\" 🦀".into(),
            },
            serde_json::json!({"text": "Xin chào\n\"LIVE\" 🦀"}),
        ),
        (
            EventData::Gift {
                gift_id: u64::MAX,
                group_id: 0,
                repeat_count: 1,
                repeat_end: false,
                quantity_delta: None,
                gift_type: None,
                receiver_id: String::new(),
                gift_name: None,
                diamonds_per_gift: None,
            },
            serde_json::json!({"gift_id": "18446744073709551615", "group_id": "0", "repeat_count": 1,
                "repeat_end": false, "quantity_delta": null, "gift_type": null, "receiver_id": "", "gift_name": null, "diamonds_per_gift": null}),
        ),
    ];
    for (payload, expected) in payloads {
        let wire = serde_json::to_vec(&payload).unwrap();
        assert!(!wire.contains(&b'\n')); // One NDJSON record, even when chat contains newlines.
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&wire).unwrap(),
            expected
        );
    }
}

#[test]
fn plain_and_compressed_batches_have_identical_content() {
    let original = Batch {
        messages: vec![gift(1, 2, true, 55)],
        internal_ext: "test".into(),
        need_ack: true,
    };
    let bytes = original.encode_to_vec();
    let plain = packet("msg", bytes.clone(), 9);
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    gzip.write_all(&bytes).unwrap();
    let compressed = packet("msg", gzip.finish().unwrap(), 9);
    let (frame, plain_batch) = decode(&plain).unwrap();
    assert_eq!(frame.payload, bytes);
    assert_eq!(plain_batch, Some(original));
    assert_eq!(plain_batch, decode(&compressed).unwrap().1);
}
