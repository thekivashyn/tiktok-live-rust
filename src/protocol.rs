use anyhow::{Result, ensure};
use prost::Message;
use std::borrow::Cow;
use std::io::Read;
mod view;
pub use view::{BatchView, EnvelopeRef, FrameInfo, decode_shared};
pub(crate) use view::{decode_cancellable, finish_prepared, prepare_shared};

pub const MAX_FRAME: usize = 2 * 1024 * 1024;
pub const MAX_DECOMPRESSED: usize = 8 * 1024 * 1024;
pub const MAX_EVENT_WIRE: usize = 256 * 1024;
pub const MAX_BATCH_MESSAGES: usize = 4096;

#[derive(Clone, PartialEq, Message)]
pub struct Common {
    #[prost(uint64, tag = "2")]
    pub msg_id: u64,
    #[prost(message, optional, tag = "8")]
    pub display_text: Option<DisplayText>,
}
#[derive(Clone, PartialEq, Message)]
pub struct DisplayText {
    #[prost(string, tag = "1")]
    pub key: String,
}
#[derive(Clone, PartialEq, Message)]
pub struct MessageHeader {
    #[prost(message, optional, tag = "1")]
    pub common: Option<Common>,
}

// Decode only the ID; do not allocate irrelevant display text during deduplication.
#[derive(Clone, PartialEq, Message)]
struct IdCommon {
    #[prost(uint64, tag = "2")]
    msg_id: u64,
}
#[derive(Clone, PartialEq, Message)]
struct IdHeader {
    #[prost(message, optional, tag = "1")]
    common: Option<IdCommon>,
}
pub fn message_id(payload: &[u8]) -> Result<u64> {
    ensure!(payload.len() <= MAX_EVENT_WIRE, "event exceeds limit");
    Ok(IdHeader::decode(payload)?
        .common
        .map(|c| c.msg_id)
        .unwrap_or_default())
}

// Minimal wire models, not a copy of the full upstream AGPL schema set.
// Protocol observations and MIT reference provenance are listed in THIRD_PARTY_NOTICES.md.
#[derive(Clone, PartialEq, Message)]
pub struct Frame {
    #[prost(uint64, tag = "2")]
    pub log_id: u64,
    #[prost(string, tag = "6")]
    pub encoding: String,
    #[prost(string, tag = "7")]
    pub kind: String,
    #[prost(bytes = "vec", tag = "8")]
    pub payload: Vec<u8>,
}
#[derive(Clone, PartialEq, Message)]
pub struct Envelope {
    #[prost(string, tag = "1")]
    pub method: String,
    #[prost(bytes = "vec", tag = "2")]
    pub payload: Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub msg_id: u64,
    #[prost(bool, tag = "6")]
    pub is_history: bool,
}
#[derive(Clone, PartialEq, Message)]
pub struct Batch {
    #[prost(message, repeated, tag = "1")]
    pub messages: Vec<Envelope>,
    #[prost(string, tag = "5")]
    pub internal_ext: String,
    #[prost(bool, tag = "9")]
    pub need_ack: bool,
}
#[derive(Clone, PartialEq, Message)]
pub struct User {
    #[prost(uint64, tag = "1")]
    pub id: u64,
}
#[derive(Clone, PartialEq, Message)]
pub struct Chat {
    #[prost(message, optional, tag = "2")]
    pub user: Option<User>,
    #[prost(string, tag = "3")]
    pub content: String,
}
#[derive(Clone, PartialEq, Message)]
pub struct GiftMeta {
    #[prost(uint32, tag = "11")]
    pub kind: u32,
    #[prost(uint32, tag = "12")]
    pub diamonds: u32,
    #[prost(string, tag = "16")]
    pub name: String,
}
#[derive(Clone, PartialEq, Message)]
pub struct Gift {
    #[prost(uint64, tag = "2")]
    pub gift_id: u64,
    #[prost(uint32, tag = "5")]
    pub repeat_count: u32,
    #[prost(message, optional, tag = "7")]
    pub user: Option<User>,
    #[prost(bool, tag = "9")]
    pub repeat_end: bool,
    #[prost(uint64, tag = "11")]
    pub group_id: u64,
    #[prost(message, optional, tag = "15")]
    pub gift: Option<GiftMeta>,
    #[prost(string, tag = "45")]
    pub receiver_id: String,
}
#[derive(Clone, PartialEq, Message)]
pub struct Like {
    #[prost(uint32, tag = "2")]
    pub count: u32,
    #[prost(uint64, tag = "3")]
    pub total: u64,
    #[prost(message, optional, tag = "5")]
    pub user: Option<User>,
}
#[derive(Clone, PartialEq, Message)]
pub struct Control {
    #[prost(uint32, tag = "2")]
    pub action: u32,
}
#[derive(Clone, PartialEq, Message)]
pub struct Social {
    #[prost(message, optional, tag = "1")]
    pub common: Option<Common>,
    #[prost(message, optional, tag = "2")]
    pub user: Option<User>,
    #[prost(uint64, tag = "4")]
    pub action: u64,
}
#[derive(Clone, PartialEq, Message)]
pub struct Member {
    #[prost(message, optional, tag = "2")]
    pub user: Option<User>,
    #[prost(uint32, tag = "10")]
    pub action: u32,
}
#[derive(Clone, PartialEq, Message)]
pub struct Viewers {
    #[prost(uint64, tag = "3")]
    pub count: u64,
}
#[derive(Clone, PartialEq, Message)]
struct Enter {
    #[prost(uint64, tag = "1")]
    room_id: u64,
    #[prost(uint64, tag = "4")]
    live_id: u64,
    #[prost(string, tag = "5")]
    identity: String,
}
#[derive(Clone, PartialEq, Message)]
struct Heartbeat {
    #[prost(uint64, tag = "1")]
    room_id: u64,
}

pub fn packet(kind: &str, payload: Vec<u8>, log_id: u64) -> Vec<u8> {
    Frame {
        log_id,
        kind: kind.into(),
        encoding: "pb".into(),
        payload,
    }
    .encode_to_vec()
}
pub fn enter(room_id: u64) -> Vec<u8> {
    packet(
        "im_enter_room",
        Enter {
            room_id,
            live_id: 12,
            identity: "audience".into(),
        }
        .encode_to_vec(),
        0,
    )
}
pub fn heartbeat(room_id: u64) -> Vec<u8> {
    packet("hb", Heartbeat { room_id }.encode_to_vec(), 0)
}
pub fn decode(bytes: &[u8]) -> Result<(Frame, Option<Batch>)> {
    ensure!(bytes.len() <= MAX_FRAME, "frame exceeds limit");
    let frame = Frame::decode(bytes)?;
    if frame.kind != "msg" || frame.encoding != "pb" || frame.payload.is_empty() {
        return Ok((frame, None));
    }
    let payload = if frame.payload.starts_with(&[0x1f, 0x8b, 0x08]) {
        let mut output = Vec::new();
        flate2::read::GzDecoder::new(frame.payload.as_slice())
            .take((MAX_DECOMPRESSED + 1) as u64)
            .read_to_end(&mut output)?;
        ensure!(
            output.len() <= MAX_DECOMPRESSED,
            "decompressed batch exceeds limit"
        );
        Cow::Owned(output)
    } else {
        Cow::Borrowed(frame.payload.as_slice())
    };
    preflight_batch(&payload)?;
    let batch = Batch::decode(payload.as_ref())?;
    ensure!(
        batch
            .messages
            .iter()
            .all(|m| m.method.len() <= 128 && m.payload.len() <= MAX_EVENT_WIRE),
        "envelope exceeds limit"
    );
    Ok((frame, Some(batch)))
}

// Bound repeated-message allocation BEFORE prost creates structs, including empty envelopes.
fn preflight_batch(mut bytes: &[u8]) -> Result<()> {
    let mut messages = 0;
    let mut fields = 0;
    while !bytes.is_empty() {
        fields += 1;
        ensure!(fields <= MAX_BATCH_MESSAGES * 4, "too many wire fields");
        let key = varint(&mut bytes)?;
        let field = key >> 3;
        let wire = key & 7;
        ensure!(field != 0 && field <= 0x1fffffff, "invalid field");
        let len = match wire {
            0 => {
                varint(&mut bytes)?;
                0
            }
            1 => 8,
            2 => usize::try_from(varint(&mut bytes)?)?,
            5 => 4,
            _ => anyhow::bail!("unsupported wire type"),
        };
        ensure!(len <= bytes.len(), "truncated field");
        if field == 1 {
            messages += 1;
            ensure!(
                wire == 2 && messages <= MAX_BATCH_MESSAGES && len <= MAX_EVENT_WIRE + 256,
                "message allocation limit"
            );
        }
        bytes = &bytes[len..];
    }
    Ok(())
}
fn varint(bytes: &mut &[u8]) -> Result<u64> {
    let mut value = 0u64;
    for i in 0..10 {
        let (&byte, rest) = bytes
            .split_first()
            .ok_or_else(|| anyhow::anyhow!("truncated varint"))?;
        *bytes = rest;
        ensure!(i != 9 || byte <= 1, "varint overflow");
        value |= u64::from(byte & 127) << (i * 7);
        if byte & 128 == 0 {
            return Ok(value);
        }
    }
    anyhow::bail!("varint overflow")
}
