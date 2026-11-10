use prost::Message;
use tiktok_live_rust::{
    Processor,
    metadata::*,
    protocol::{Chat, Envelope, Like, User},
};

fn raw(method: &str, payload: Vec<u8>, id: u64) -> Envelope {
    Envelope {
        method: method.into(),
        payload,
        msg_id: id,
        is_history: false,
    }
}
fn profile_chat(profile: WireUser) -> Envelope {
    // Independent embedding: Chat.user is field 2; concatenate a minimal text message.
    let mut bytes = vec![0x12];
    prost::encoding::encode_varint(profile.encoded_len() as u64, &mut bytes);
    profile.encode(&mut bytes).unwrap();
    bytes.extend(
        Chat {
            user: None,
            content: "hello".into(),
        }
        .encode_to_vec(),
    );
    raw("WebcastChatMessage", bytes, 10)
}

#[test]
fn profile_names_and_safe_avatar_fallback_do_not_invent_missing_values() {
    let profile = WireUser {
        id: 123,
        nickname: "Tên hiển thị 🎁".into(),
        display_id: "nf.gta5vn".into(),
        avatar_large: Some(Box::new(Image {
            url_list: vec![
                "javascript:alert(1)".into(),
                "https://u:p@example.com/a".into(),
            ],
            ..Default::default()
        })),
        avatar_medium: Some(Box::new(Image {
            url_list: vec!["https://example.com/avatar.webp?token=x".into()],
            ..Default::default()
        })),
        ..Default::default()
    };
    let event = Processor::new("42")
        .process(&profile_chat(profile))
        .unwrap()
        .unwrap();
    let user = event.user.unwrap();
    assert_eq!(user.id, 123);
    assert_eq!(user.username.as_deref(), Some("nf.gta5vn"));
    assert_eq!(user.display_name.as_deref(), Some("Tên hiển thị 🎁"));
    assert_eq!(
        user.avatar_url.as_deref(),
        Some("https://example.com/avatar.webp?token=x")
    );
    assert!(!event.metadata_incomplete);
    let user = Processor::new("42")
        .process(&profile_chat(WireUser {
            id: 123,
            ..Default::default()
        }))
        .unwrap()
        .unwrap()
        .user
        .unwrap();
    assert!(user.username.is_none() && user.display_name.is_none() && user.avatar_url.is_none());
}

#[test]
fn repeated_embedded_user_fields_merge_and_string_identity_falls_back() {
    let first = WireUser {
        id_str: "9007199254740993".into(),
        nickname: "Name".into(),
        ..Default::default()
    };
    let mut message = profile_chat(first);
    let second = WireUser {
        display_id: "handle".into(),
        ..Default::default()
    }
    .encode_to_vec();
    message.payload.push(0x12);
    prost::encoding::encode_varint(second.len() as u64, &mut message.payload);
    message.payload.extend(second);
    let event = Processor::new("42").process(&message).unwrap().unwrap();
    assert_eq!(event.user_id.as_deref(), Some("9007199254740993"));
    let user = event.user.unwrap();
    assert_eq!(user.display_name.as_deref(), Some("Name"));
    assert_eq!(user.username.as_deref(), Some("handle"));
}

#[test]
fn excessive_profile_is_flagged_without_losing_chat_or_poisoning_other_profiles() {
    let mut processor = Processor::new("42");
    for (id, profile) in [
        WireUser {
            id: 123,
            nickname: "x".repeat(4097),
            ..Default::default()
        },
        WireUser {
            id: 123,
            avatar_large: Some(Box::new(Image {
                url_list: vec!["https://example.com/a".into(); 9],
                ..Default::default()
            })),
            ..Default::default()
        },
    ]
    .into_iter()
    .enumerate()
    {
        let mut message = profile_chat(profile);
        message.msg_id = id as u64 + 1;
        let event = processor.process(&message).unwrap().unwrap();
        assert!(event.metadata_incomplete && event.user.is_none());
        assert_eq!(event.kind, "chat");
    }
    let message = profile_chat(WireUser {
        id: 123,
        nickname: "Now available".into(),
        ..Default::default()
    });
    assert!(processor.process(&message).unwrap().unwrap().user.is_some());
}

#[test]
fn like_details_preserve_large_signed_values_nested_fields_and_core_contract() {
    let details = LikeDetails {
        color: -16776961,
        effect_cnt: 9_007_199_254_740_993,
        like_effect: vec![LikeEffect {
            version: 2,
            effect_cnt: 3,
            effect_interval_ms: 50,
            level: 4,
        }],
        common: Some(Box::new(MessageCommon {
            msg_id: 10,
            room_id: 42,
            create_time: 1793817326,
            ..Default::default()
        })),
        public_area_message_common: Some(Box::new(PublicArea {
            user_interaction_info: Some(Box::new(UserInteraction {
                like_cnt: 12,
                comment_cnt: 3,
                share_cnt: 2,
            })),
            ..Default::default()
        })),
        ..Default::default()
    };
    let mut payload = Like {
        count: 12,
        total: 999,
        user: Some(User { id: 123 }),
    }
    .encode_to_vec();
    payload.extend(details.encode_to_vec());
    let event = Processor::new("42")
        .process(&raw("WebcastLikeMessage", payload, 10))
        .unwrap()
        .unwrap();
    assert!(!event.metadata_incomplete);
    assert_eq!(event.like_details.as_deref(), Some(&details));
    let json = serde_json::to_value(event).unwrap();
    assert_eq!(json["data"], serde_json::json!({"count":12,"total":"999"}));
    assert_eq!(json["like_details"]["effect_cnt"], "9007199254740993");
    assert_eq!(json["like_details"]["color"], -16776961);
}

#[test]
fn malformed_optional_metadata_cannot_drop_like_and_repeated_lists_stay_bounded() {
    let base = Like {
        count: 1,
        total: 2,
        user: Some(User { id: 123 }),
    }
    .encode_to_vec();
    for extra in [
        vec![0x3a, 1, 0xff],
        LikeDetails {
            icons: vec![Image::default(); 17],
            ..Default::default()
        }
        .encode_to_vec(),
    ] {
        let mut payload = base.clone();
        payload.extend(extra);
        let event = Processor::new("42")
            .process(&raw("WebcastLikeMessage", payload, 10))
            .unwrap()
            .unwrap();
        assert!(event.metadata_incomplete && event.like_details.is_none());
        assert_eq!(event.kind, "like");
        assert!(event.user.is_some());
    }
}

#[test]
fn metadata_boxes_keep_the_queue_element_small() {
    assert!(std::mem::size_of::<tiktok_live_rust::Event>() <= 320);
}

#[test]
fn duplicate_parent_fields_cannot_bypass_merged_list_limits() {
    let detail = LikeDetails {
        common: Some(Box::new(MessageCommon {
            display_text: Some(Box::new(Text {
                pieces: vec![TextPiece::default(); 20],
                ..Default::default()
            })),
            ..Default::default()
        })),
        ..Default::default()
    }
    .encode_to_vec();
    let mut payload = Like {
        count: 1,
        total: 2,
        user: Some(User { id: 123 }),
    }
    .encode_to_vec();
    payload.extend_from_slice(&detail);
    payload.extend_from_slice(&detail);
    let event = Processor::new("42")
        .process(&raw("WebcastLikeMessage", payload, 1))
        .unwrap()
        .unwrap();
    assert!(event.metadata_incomplete && event.like_details.is_none());
    assert_eq!(event.kind, "like");
}

#[test]
fn arbitrary_optional_image_wire_never_loses_the_valid_core_or_panics() {
    let mut processor = Processor::new("42");
    let mut state = 0x123456789abcdefu64;
    for index in 1..=1000 {
        let mut bytes = vec![0u8; index % 257];
        for byte in &mut bytes {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }
        let mut payload = Like {
            count: 1,
            total: index as u64,
            user: Some(User { id: 123 }),
        }
        .encode_to_vec();
        payload.push(0x3a);
        prost::encoding::encode_varint(bytes.len() as u64, &mut payload);
        payload.extend(bytes);
        let event = processor
            .process(&raw("WebcastLikeMessage", payload, index as u64))
            .unwrap()
            .unwrap();
        assert_eq!(event.kind, "like");
        assert!(event.user.is_some());
        assert!(serde_json::to_vec(&event).unwrap().len() < 65536);
    }
}
