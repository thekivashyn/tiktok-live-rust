//! Selective receive decoder. Own one shared batch buffer; borrow each envelope on demand.
//! Validate the complete wire batch before exposing any events. No unsafe code or payload copies.
use super::{Envelope, MAX_BATCH_MESSAGES, MAX_DECOMPRESSED, MAX_EVENT_WIRE, MAX_FRAME, varint};
use anyhow::{Result, bail, ensure};
use prost::{Message, bytes::Bytes};
use std::io::Read;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EnvelopeRef<'a> {
    pub method: &'a str,
    pub payload: &'a [u8],
    pub msg_id: u64,
    pub is_history: bool,
}
impl<'a> From<&'a Envelope> for EnvelopeRef<'a> {
    fn from(raw: &'a Envelope) -> Self {
        Self {
            method: &raw.method,
            payload: &raw.payload,
            msg_id: raw.msg_id,
            is_history: raw.is_history,
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct SharedFrame {
    #[prost(uint64, tag = "2")]
    log_id: u64,
    #[prost(string, tag = "6")]
    encoding: String,
    #[prost(string, tag = "7")]
    kind: String,
    #[prost(bytes = "bytes", tag = "8")]
    payload: Bytes,
}
#[derive(Debug)]
pub struct FrameInfo {
    pub log_id: u64,
    pub kind: String,
}
#[derive(Debug)]
pub struct BatchView {
    payload: Bytes,
    count: usize,
    has_webcast: bool,
}
impl BatchView {
    pub fn len(&self) -> usize {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub fn has_webcast_messages(&self) -> bool {
        self.has_webcast
    }
    pub fn messages(&self) -> impl Iterator<Item = Result<EnvelopeRef<'_>>> {
        Envelopes {
            bytes: &self.payload,
        }
    }
}

pub fn decode_shared(bytes: Bytes) -> Result<(FrameInfo, Option<BatchView>)> {
    decode_cancellable(bytes, &|| false)
}

pub(crate) struct PreparedFrame {
    info: FrameInfo,
    payload: Option<Bytes>,
    compressed: bool,
}
impl PreparedFrame {
    pub(crate) fn is_compressed(&self) -> bool {
        self.compressed
    }
}
pub(crate) fn prepare_shared(bytes: Bytes) -> Result<PreparedFrame> {
    ensure!(bytes.len() <= MAX_FRAME, "frame exceeds limit");
    let frame = SharedFrame::decode(bytes)?;
    let info = FrameInfo {
        log_id: frame.log_id,
        kind: frame.kind,
    };
    if info.kind != "msg" || frame.encoding != "pb" || frame.payload.is_empty() {
        return Ok(PreparedFrame {
            info,
            payload: None,
            compressed: false,
        });
    }
    let compressed = frame.payload.starts_with(&[0x1f, 0x8b, 0x08]);
    Ok(PreparedFrame {
        info,
        payload: Some(frame.payload),
        compressed,
    })
}
pub(crate) fn decode_cancellable(
    bytes: Bytes,
    cancelled: &impl Fn() -> bool,
) -> Result<(FrameInfo, Option<BatchView>)> {
    ensure!(!cancelled(), "decode cancelled");
    finish_prepared(prepare_shared(bytes)?, cancelled)
}
pub(crate) fn finish_prepared(
    frame: PreparedFrame,
    cancelled: &impl Fn() -> bool,
) -> Result<(FrameInfo, Option<BatchView>)> {
    ensure!(!cancelled(), "decode cancelled");
    let Some(payload) = frame.payload else {
        return Ok((frame.info, None));
    };
    let payload = if frame.compressed {
        inflate(&payload, cancelled)?.into()
    } else {
        payload
    };
    let (count, has_webcast) = validate_batch(&payload, cancelled)?;
    Ok((
        frame.info,
        Some(BatchView {
            payload,
            count,
            has_webcast,
        }),
    ))
}

fn inflate(input: &[u8], cancelled: &impl Fn() -> bool) -> Result<Vec<u8>> {
    // All gzip members and trailers are checked. Trailing garbage is not silently ignored.
    let mut decoder = flate2::read::MultiGzDecoder::new(input);
    let mut output = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        ensure!(!cancelled(), "decode cancelled");
        // At the cap, read just one byte to distinguish EOF/CRC completion from overflow.
        let limit = (MAX_DECOMPRESSED - output.len()).clamp(1, chunk.len());
        let count = decoder.read(&mut chunk[..limit])?;
        if count == 0 {
            break;
        }
        ensure!(
            count <= MAX_DECOMPRESSED - output.len(),
            "decompressed batch exceeds limit"
        );
        let required = output.len() + count;
        if required > output.capacity() {
            // Small batches grow geometrically. Above 1 MiB, avoid doubling a 4 MiB batch
            // to 8 MiB, or allocating 16 MiB merely to discover a one-byte limit violation.
            let mib = 1024 * 1024;
            let capacity = if output.capacity() < mib {
                (output.capacity() * 2).max(required)
            } else {
                required.div_ceil(mib) * mib
            }
            .min(MAX_DECOMPRESSED);
            output.reserve_exact(capacity - output.len());
        }
        output.extend_from_slice(&chunk[..count]);
    }
    Ok(output)
}

enum Value<'a> {
    Varint(u64),
    Bytes(&'a [u8]),
    Fixed,
    Group,
}
struct Field<'a> {
    number: u32,
    value: Value<'a>,
}
fn key(bytes: &mut &[u8]) -> Result<(u32, u8)> {
    let key = varint(bytes)?;
    let number = key >> 3;
    ensure!(number != 0 && number <= 0x1fff_ffff, "invalid field");
    Ok((number as u32, (key & 7) as u8))
}
fn value<'a>(bytes: &mut &'a [u8], number: u32, wire: u8, depth: usize) -> Result<Value<'a>> {
    let len = match wire {
        0 => return Ok(Value::Varint(varint(bytes)?)),
        1 => 8,
        2 => usize::try_from(varint(bytes)?)?,
        3 => {
            ensure!(depth < 100, "group nesting limit");
            loop {
                let (nested, wire) = key(bytes)?;
                if wire == 4 {
                    ensure!(nested == number, "mismatched end group");
                    break;
                }
                value(bytes, nested, wire, depth + 1)?;
            }
            return Ok(Value::Group);
        }
        5 => 4,
        _ => bail!("unsupported wire type"),
    };
    ensure!(len <= bytes.len(), "truncated field");
    let (payload, rest) = bytes.split_at(len);
    *bytes = rest;
    Ok(if wire == 2 {
        Value::Bytes(payload)
    } else {
        Value::Fixed
    })
}
fn field<'a>(bytes: &mut &'a [u8]) -> Result<Field<'a>> {
    let (number, wire) = key(bytes)?;
    Ok(Field {
        number,
        value: value(bytes, number, wire, 0)?,
    })
}
fn envelope(mut bytes: &[u8]) -> Result<EnvelopeRef<'_>> {
    ensure!(
        bytes.len() <= MAX_EVENT_WIRE + 256,
        "message allocation limit"
    );
    let mut raw = EnvelopeRef::default();
    while !bytes.is_empty() {
        let field = field(&mut bytes)?;
        match (field.number, field.value) {
            (1, Value::Bytes(v)) => raw.method = std::str::from_utf8(v)?,
            (2, Value::Bytes(v)) => raw.payload = v,
            (3, Value::Varint(v)) => raw.msg_id = v,
            (6, Value::Varint(v)) => raw.is_history = v != 0,
            (1 | 2 | 3 | 6, _) => bail!("unexpected envelope wire type"),
            _ => {}
        }
    }
    // Scalars/strings/bytes use the last occurrence, independent of field order.
    ensure!(
        raw.method.len() <= 128 && raw.payload.len() <= MAX_EVENT_WIRE,
        "envelope exceeds limit"
    );
    Ok(raw)
}
fn validate_batch(mut bytes: &[u8], cancelled: &impl Fn() -> bool) -> Result<(usize, bool)> {
    let mut count = 0;
    let mut fields = 0;
    let mut has_webcast = false;
    while !bytes.is_empty() {
        ensure!(!cancelled(), "decode cancelled");
        fields += 1;
        ensure!(fields <= MAX_BATCH_MESSAGES * 4, "too many wire fields");
        let field = field(&mut bytes)?;
        // Preserve the existing top-level batch policy; unknown envelope groups remain supported.
        ensure!(
            !matches!(field.value, Value::Group),
            "unsupported batch group"
        );
        match (field.number, field.value) {
            (1, Value::Bytes(v)) => {
                count += 1;
                ensure!(count <= MAX_BATCH_MESSAGES, "message allocation limit");
                let raw = envelope(v)?;
                has_webcast |= raw.method.starts_with("Webcast") && !raw.payload.is_empty();
            }
            (5, Value::Bytes(v)) => {
                std::str::from_utf8(v)?;
            }
            (9, Value::Varint(_)) => {}
            (1 | 5 | 9, _) => bail!("unexpected batch wire type"),
            _ => {}
        }
    }
    Ok((count, has_webcast))
}
struct Envelopes<'a> {
    bytes: &'a [u8],
}
impl<'a> Iterator for Envelopes<'a> {
    type Item = Result<EnvelopeRef<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        while !self.bytes.is_empty() {
            match field(&mut self.bytes) {
                Ok(Field {
                    number: 1,
                    value: Value::Bytes(bytes),
                }) => return Some(envelope(bytes)),
                Ok(_) => {}
                Err(error) => {
                    self.bytes = &[];
                    return Some(Err(error));
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    #[test]
    fn gzip_cancellation_interrupts_inflation_between_bounded_chunks() {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(&vec![b'x'; MAX_DECOMPRESSED]).unwrap();
        let input = encoder.finish().unwrap();
        let checks = std::cell::Cell::new(0);
        let result = inflate(&input, &|| {
            checks.set(checks.get() + 1);
            checks.get() == 4
        });
        assert_eq!(result.unwrap_err().to_string(), "decode cancelled");
        assert_eq!(checks.get(), 4);
    }
    #[test]
    fn large_gzip_reservation_stays_within_one_mib_slack_and_hard_cap() {
        let mib = 1024 * 1024;
        for len in [4 * mib + 333, MAX_DECOMPRESSED] {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            encoder.write_all(&vec![b'x'; len]).unwrap();
            let output = inflate(&encoder.finish().unwrap(), &|| false).unwrap();
            assert_eq!(output.len(), len);
            assert!(output.capacity() < len + mib);
            assert!(output.capacity() <= MAX_DECOMPRESSED);
        }
    }
}
