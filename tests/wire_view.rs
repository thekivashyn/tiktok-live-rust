use anyhow::Result;
use prost::{Message, bytes::Bytes};
use std::io::Write;
use tiktok_live_rust::{Processor, protocol::*};
#[path = "../examples/support/workload.rs"]
mod workload;

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut out = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    out.write_all(bytes).unwrap();
    out.finish().unwrap()
}
fn chat(id: u64) -> Envelope {
    Envelope {
        method: "WebcastChatMessage".into(),
        payload: Chat {
            user: Some(User { id: 42 }),
            content: "Unicode 日本語 😀\nchat".into(),
        }
        .encode_to_vec(),
        msg_id: id,
        is_history: false,
    }
}
fn batch(messages: Vec<Envelope>) -> Vec<u8> {
    Batch {
        messages,
        internal_ext: "ignored metadata".into(),
        need_ack: true,
    }
    .encode_to_vec()
}
fn assert_matches(bytes: Bytes) -> Result<()> {
    let (owned_frame, owned) = decode(&bytes)?;
    let (frame, view) = decode_shared(bytes)?;
    assert_eq!(
        (frame.log_id, &frame.kind),
        (owned_frame.log_id, &owned_frame.kind)
    );
    match (owned, view) {
        (Some(owned), Some(view)) => {
            assert_eq!(view.len(), owned.messages.len());
            assert_eq!(
                view.has_webcast_messages(),
                owned
                    .messages
                    .iter()
                    .any(|m| m.method.starts_with("Webcast") && !m.payload.is_empty())
            );
            let mut old_processor = Processor::new("42");
            let mut new_processor = Processor::new("42");
            for (old, new) in owned.messages.iter().zip(view.messages()) {
                let new = new?;
                assert_eq!(EnvelopeRef::from(old), new);
                assert_eq!(old_processor.process(old)?, new_processor.process_ref(new)?);
            }
        }
        (None, None) => {}
        _ => panic!("batch presence differs"),
    }
    Ok(())
}

#[test]
fn selective_decoder_matches_owned_decoder_and_events_for_mixed_corpus() -> Result<()> {
    for frame in workload::inputs()? {
        assert_matches(frame.into())?;
    }
    for kind in ["hb", "im_enter_room_resp", "future_kind"] {
        assert_matches(packet(kind, vec![1, 2, 3], 9).into())?;
    }
    Ok(())
}

#[test]
fn plain_envelopes_borrow_the_original_input_without_payload_copies() -> Result<()> {
    let bytes: Bytes = packet("msg", batch(vec![chat(1), chat(2)]), 7).into();
    let start = bytes.as_ptr() as usize;
    let end = start + bytes.len();
    let (_, view) = decode_shared(bytes.clone())?;
    let view = view.unwrap();
    for raw in view.messages() {
        let raw = raw?;
        assert!((start..end).contains(&(raw.method.as_ptr() as usize)));
        assert!((start..end).contains(&(raw.payload.as_ptr() as usize)));
        assert!(raw.payload.as_ptr() as usize + raw.payload.len() <= end);
    }
    drop(bytes); // The view owns a shared reference; its slices still remain valid.
    assert_eq!(view.messages().count(), 2);
    Ok(())
}

#[test]
fn duplicate_fields_order_and_unknown_fields_follow_protobuf_semantics() -> Result<()> {
    let first = chat(1);
    let second = Envelope {
        method: "WebcastLikeMessage".into(),
        payload: Like {
            count: 2,
            total: 100,
            user: None,
        }
        .encode_to_vec(),
        msg_id: u64::MAX,
        is_history: true,
    };
    let mut envelope = first.encode_to_vec();
    // Unknown fixed32, fixed64 and nested deprecated groups inside an envelope.
    envelope.extend_from_slice(&[
        0x55, 1, 2, 3, 4, 0x59, 0, 0, 0, 0, 0, 0, 0, 0, 0x63, 0x6b, 0x08, 0x01, 0x6c, 0x64,
    ]);
    envelope.extend(second.encode_to_vec());
    let mut wire = vec![0x48, 0x01, 0x0a]; // need_ack before messages
    prost::encoding::encode_varint(envelope.len() as u64, &mut wire);
    wire.extend(envelope);
    wire.extend_from_slice(&[
        0x2a, 0x03, b'o', b'l', b'd', 0x2a, 0x03, b'n', b'e', b'w', 0x48, 0,
    ]);
    assert_matches(packet("msg", wire, 17).into())
}

#[test]
fn malformed_later_envelope_rejects_whole_batch_before_delivery() {
    let mut wire = batch(vec![chat(1)]);
    for bad in [
        vec![0],
        vec![0x08, 1],
        vec![0x0a, 1, 0xff],
        vec![0x12, 0x80],
        vec![0x63, 0x6c],
    ] {
        let original = wire.len();
        wire.push(0x0a);
        prost::encoding::encode_varint(bad.len() as u64, &mut wire);
        wire.extend(bad);
        assert!(decode_shared(packet("msg", wire.clone(), 1).into()).is_err());
        wire.truncate(original);
    }
}

#[test]
fn gzip_members_are_combined_and_all_trailers_validated() -> Result<()> {
    let mut compressed = gzip(&batch(vec![chat(1)]));
    compressed.extend(gzip(&batch(vec![chat(2)])));
    let (_, view) = decode_shared(packet("msg", compressed.clone(), 1).into())?;
    assert_eq!(
        view.unwrap()
            .messages()
            .map(|m| m.unwrap().msg_id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    compressed.push(0); // a trailing partial gzip header must not be silently dropped
    assert!(decode_shared(packet("msg", compressed, 1).into()).is_err());
    let valid = gzip(&batch(vec![chat(3)]));
    for removed in 1..=8 {
        assert!(
            decode_shared(packet("msg", valid[..valid.len() - removed].to_vec(), 1).into())
                .is_err()
        );
    }
    let mut corrupt = valid;
    let crc = corrupt.len() - 8;
    corrupt[crc] ^= 1;
    assert!(decode_shared(packet("msg", corrupt, 1).into()).is_err());
    Ok(())
}

#[test]
fn view_decoder_preserves_size_count_and_decompression_limits() {
    assert!(decode_shared(vec![0; MAX_FRAME + 1].into()).is_err());
    for count in [MAX_BATCH_MESSAGES + 1, MAX_BATCH_MESSAGES * 2] {
        assert!(decode_shared(packet("msg", [0x0a, 0].repeat(count), 1).into()).is_err());
    }
    let mut raw = chat(1);
    raw.payload = vec![0; MAX_EVENT_WIRE + 1];
    assert!(decode_shared(packet("msg", gzip(&batch(vec![raw])), 1).into()).is_err());
    let bomb = gzip(&vec![0; MAX_DECOMPRESSED + 1]);
    assert!(decode_shared(packet("msg", bomb, 1).into()).is_err());
    let deep = [vec![0x63; 101], vec![0x64; 101]].concat();
    let mut wire = vec![0x0a];
    prost::encoding::encode_varint(deep.len() as u64, &mut wire);
    wire.extend(deep);
    assert!(decode_shared(packet("msg", wire, 1).into()).is_err());
}

#[test]
fn malformed_wire_corpus_never_panics_in_either_receive_layer() {
    let mut seed = 0x1234_5678u64;
    for len in 0..512 {
        let mut bytes = vec![0; len];
        for byte in &mut bytes {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            *byte = seed as u8;
        }
        let _ = decode_shared(bytes.clone().into());
        let _ = decode_shared(packet("msg", bytes.clone(), 1).into());
        let mut batch = vec![0x0a];
        prost::encoding::encode_varint(bytes.len() as u64, &mut batch);
        batch.extend(bytes);
        let _ = decode_shared(packet("msg", batch, 1).into());
    }
}

#[test]
fn compact_recipient_keys_preserve_identity_and_original_json() -> Result<()> {
    let ids = [
        "",
        "0",
        "1",
        "01",
        "+1",
        "18446744073709551615",
        "18446744073709551616",
        "host",
        "HOST",
        "日本語",
    ];
    let mut processor = Processor::new("42");
    for count in [1, 3] {
        for (index, receiver) in ids.iter().enumerate() {
            let raw = Envelope {
                method: "WebcastGiftMessage".into(),
                msg_id: u64::from(count) * 100 + index as u64,
                is_history: false,
                payload: Gift {
                    gift_id: 1,
                    repeat_count: count,
                    repeat_end: false,
                    group_id: 55,
                    user: Some(User { id: 7 }),
                    gift: Some(GiftMeta {
                        kind: 1,
                        diamonds: 1,
                        name: "Rose".into(),
                    }),
                    receiver_id: (*receiver).into(),
                }
                .encode_to_vec(),
            };
            let event = processor.process_ref((&raw).into())?.unwrap();
            let data = serde_json::to_value(&event.data)?;
            assert_eq!(data["receiver_id"], *receiver);
            assert_eq!(data["quantity_delta"], if count == 1 { 1 } else { 2 });
        }
    }
    Ok(())
}
