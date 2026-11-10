use anyhow::{Result, ensure};
use prost::bytes::Bytes;
use std::{collections::BTreeMap, io::Write, path::Path};
use tiktok_live_rust::{Event, EventData};

#[allow(dead_code)] // Shared helper; socket receiver only uses Output.
pub fn corpus(path: &Path) -> Result<Vec<Bytes>> {
    let input = Bytes::from(std::fs::read(path)?);
    let mut frames = Vec::new();
    let mut pos = 0;
    while pos < input.len() {
        ensure!(pos + 4 <= input.len(), "truncated length");
        let len = u32::from_le_bytes(input[pos..pos + 4].try_into()?) as usize;
        pos += 4;
        ensure!(
            len <= 2 * 1024 * 1024 && pos + len <= input.len(),
            "invalid frame"
        );
        frames.push(input.slice(pos..pos + len));
        pos += len;
    }
    Ok(frames)
}
#[derive(Default)]
pub struct Output {
    buffer: Vec<u8>,
    crc: crc32fast::Hasher,
    pub events: u64,
    pub bytes: u64,
    pub gifts: u64,
    pub kinds: BTreeMap<String, u64>,
    pub capture: Option<std::fs::File>,
}
impl Output {
    pub fn push(&mut self, event: Event) -> Result<()> {
        if let EventData::Gift { quantity_delta, .. } = &event.data {
            self.gifts += u64::from(quantity_delta.unwrap_or_default());
        }
        *self.kinds.entry(event.kind.clone()).or_default() += 1;
        self.buffer.clear();
        serde_json::to_writer(
            &mut self.buffer,
            &(&event.kind, &event.message_id, &event.user_id, &event.data),
        )?;
        self.buffer.push(b'\n');
        self.events += 1;
        self.bytes += self.buffer.len() as u64;
        self.crc.update(&self.buffer);
        if let Some(file) = &mut self.capture {
            file.write_all(&self.buffer)?;
        }
        Ok(())
    }
    pub fn summary(&self) -> serde_json::Value {
        serde_json::json!({"events":self.events,"serialized_bytes":self.bytes,"gift_quantity":self.gifts,"event_counts":self.kinds,"crc32":self.crc.clone().finalize()})
    }
}
