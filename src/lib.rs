mod decoder;
pub mod engine;
pub mod event_data;
pub mod lifecycle;
pub mod metadata;
pub mod protocol;
pub mod transport;

pub use event_data::EventData;
use prost::Message;
use protocol::*;
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

#[derive(Debug, Serialize, PartialEq)]
pub struct Event {
    pub sequence: u64,
    /// Changes after reconnect; consumers must treat a change as a possible delivery gap.
    pub connection_epoch: u32,
    pub kind: String,
    pub room_id: String,
    pub message_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<Box<metadata::UserProfile>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub like_details: Option<Box<metadata::LikeDetails>>,
    /// Optional metadata failed its size/shape checks; the core event was retained.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub metadata_incomplete: bool,
    pub data: EventData,
}

/// State belongs to a single room. Reconnects retain state; changing room creates a new processor.
/// Dedupe and streak memory are bounded; no exactly-once claim beyond this window.
pub struct Processor {
    room_id: String,
    ids: HashSet<u64>,
    order: VecDeque<u64>,
    streaks: HashMap<(u64, u64, u64, RecipientKey), u32>,
    streak_order: VecDeque<(u64, u64, u64, RecipientKey)>,
    pub duplicates: u64,
    pub history_skipped: u64,
    /// Evictions from the bounded combo window; losing context is not an exact quantity reset.
    pub streak_evictions: u64,
    /// Cumulative updates whose previous combo count was unavailable after window overflow.
    pub streak_resyncs: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum RecipientKey {
    Empty,
    Number(u64),
    Text(Arc<str>),
}
impl RecipientKey {
    fn new(value: &str) -> Self {
        if value.is_empty() {
            return Self::Empty;
        }
        // Keep noncanonical IDs distinct: "01", "+1", overflow and arbitrary recipient
        // names must not collide with "1". The emitted event retains the original string.
        if value.len() <= 20
            && (value.len() == 1 || !value.starts_with('0'))
            && value.bytes().all(|b| b.is_ascii_digit())
            && let Ok(id) = value.parse()
        {
            return Self::Number(id);
        }
        Self::Text(Arc::from(value))
    }
}

impl Processor {
    pub fn new(room_id: &str) -> Self {
        Self {
            room_id: room_id.into(),
            ids: HashSet::new(),
            order: VecDeque::new(),
            streaks: HashMap::new(),
            streak_order: VecDeque::new(),
            duplicates: 0,
            history_skipped: 0,
            streak_evictions: 0,
            streak_resyncs: 0,
        }
    }

    pub fn process(&mut self, raw: &Envelope) -> anyhow::Result<Option<Event>> {
        self.process_ref(raw.into())
    }

    pub fn process_ref(&mut self, raw: EnvelopeRef<'_>) -> anyhow::Result<Option<Event>> {
        anyhow::ensure!(raw.payload.len() <= MAX_EVENT_WIRE, "event exceeds limit");
        if raw.is_history {
            self.history_skipped += 1;
            return Ok(None);
        }
        if !matches!(
            raw.method,
            "WebcastChatMessage"
                | "WebcastGiftMessage"
                | "WebcastLikeMessage"
                | "WebcastSocialMessage"
                | "WebcastMemberMessage"
                | "WebcastRoomUserSeqMessage"
                | "WebcastControlMessage"
        ) {
            return Ok(None);
        }
        let message_id = if raw.msg_id != 0 {
            raw.msg_id
        } else {
            protocol::message_id(raw.payload)?
        };
        if message_id != 0 && self.ids.contains(&message_id) {
            self.duplicates += 1;
            return Ok(None);
        }
        let (kind, mut user_id, data) = match raw.method {
            "WebcastChatMessage" => {
                let msg = Chat::decode(raw.payload)?;
                anyhow::ensure!(msg.content.len() <= 4096, "chat content exceeds limit");
                // TikTok can send an identified chat with no text. Preserve its event
                // and empty string, but do not turn an empty/default payload into chat.
                anyhow::ensure!(
                    !msg.content.is_empty()
                        || (message_id != 0 && msg.user.as_ref().is_some_and(|u| u.id != 0)),
                    "empty chat content"
                );
                (
                    "chat",
                    msg.user.map(|u| u.id.to_string()),
                    EventData::Chat { text: msg.content },
                )
            }
            "WebcastGiftMessage" => {
                let msg = Gift::decode(raw.payload)?;
                anyhow::ensure!(msg.gift_id != 0, "missing gift identity");
                anyhow::ensure!(
                    msg.receiver_id.len() <= 128
                        && msg.gift.as_ref().is_none_or(|g| g.name.len() <= 256),
                    "gift metadata exceeds limit"
                );
                let sender = msg.user.as_ref().map(|u| u.id).unwrap_or_default();
                let total = msg.repeat_count.max(1);
                let gift_type = msg.gift.as_ref().map(|g| g.kind);
                let delta = if gift_type == Some(1) && msg.group_id != 0 && sender != 0 {
                    let key = (
                        sender,
                        msg.gift_id,
                        msg.group_id,
                        RecipientKey::new(&msg.receiver_id),
                    );
                    let new_key = !self.streaks.contains_key(&key);
                    if new_key {
                        if self.streaks.len() >= 4096
                            && let Some(old) = self.streak_order.pop_front()
                        {
                            self.streaks.remove(&old);
                            self.streak_evictions += 1;
                        }
                        self.streak_order.push_back(key.clone());
                    }
                    let previous = self.streaks.entry(key).or_default();
                    let increment = total.saturating_sub(*previous);
                    *previous = (*previous).max(total);
                    // After eviction, an unseen cumulative update may belong to an old combo.
                    // Preserve the new baseline but do not report its entire total as new gifts.
                    if new_key && self.streak_evictions > 0 && total > 1 {
                        self.streak_resyncs += 1;
                        None
                    } else {
                        Some(increment)
                    }
                } else if gift_type == Some(1) {
                    // No reliable streak identity: wait for final count, don't invent a group.
                    if msg.repeat_end { Some(total) } else { None }
                } else if gift_type.is_some() {
                    Some(total)
                } else {
                    None
                };
                let (gift_name, diamonds_per_gift) = match msg.gift {
                    Some(gift) => (Some(gift.name), Some(gift.diamonds)),
                    None => (None, None),
                };
                (
                    "gift",
                    msg.user.map(|u| u.id.to_string()),
                    EventData::Gift {
                        gift_id: msg.gift_id,
                        group_id: msg.group_id,
                        repeat_count: total,
                        repeat_end: msg.repeat_end,
                        quantity_delta: delta,
                        gift_type,
                        receiver_id: msg.receiver_id,
                        gift_name,
                        diamonds_per_gift,
                    },
                )
            }
            "WebcastLikeMessage" => {
                let msg = Like::decode(raw.payload)?;
                anyhow::ensure!(msg.count != 0 || msg.total != 0, "empty like update");
                (
                    "like",
                    msg.user.map(|u| u.id.to_string()),
                    EventData::Like {
                        count: msg.count,
                        total: msg.total,
                    },
                )
            }
            "WebcastSocialMessage" => {
                let msg = Social::decode(raw.payload)?;
                let key = msg
                    .common
                    .and_then(|c| c.display_text)
                    .map(|d| d.key)
                    .unwrap_or_default();
                anyhow::ensure!(key.len() <= 256, "social key exceeds limit");
                anyhow::ensure!(
                    msg.user.is_some() || !key.is_empty() || msg.action != 0,
                    "empty social message"
                );
                let kind = social_kind(&key, msg.action);
                (
                    kind,
                    msg.user.map(|u| u.id.to_string()),
                    EventData::Social {
                        action: msg.action,
                        display_key: key,
                    },
                )
            }
            "WebcastMemberMessage" => {
                let msg = Member::decode(raw.payload)?;
                // Other member actions are moderation/role changes, not audience joins.
                if msg.action != 1 {
                    return Ok(None);
                }
                (
                    "join",
                    msg.user.map(|u| u.id.to_string()),
                    EventData::Member { action: msg.action },
                )
            }
            "WebcastRoomUserSeqMessage" => {
                anyhow::ensure!(!raw.payload.is_empty(), "empty audience update");
                let msg = Viewers::decode(raw.payload)?;
                ("viewers", None, EventData::Viewers { count: msg.count })
            }
            "WebcastControlMessage" => {
                let msg = Control::decode(raw.payload)?;
                if ![3, 4].contains(&msg.action) {
                    return Ok(None);
                }
                (
                    "stream_end",
                    None,
                    EventData::Control { action: msg.action },
                )
            }
            _ => return Ok(None),
        };
        if message_id != 0 {
            if self.order.len() >= 8192
                && let Some(old) = self.order.pop_front()
            {
                self.ids.remove(&old);
            }
            self.ids.insert(message_id);
            self.order.push_back(message_id);
        }
        let metadata = metadata::extract(raw.method, raw.payload);
        if user_id.as_deref() == Some("0")
            && let Some(profile) = &metadata.user
            && profile.id != 0
        {
            user_id = Some(profile.id.to_string());
        }
        Ok(Some(Event {
            sequence: 0,
            connection_epoch: 0,
            kind: kind.into(),
            room_id: self.room_id.clone(),
            message_id: message_id.to_string(),
            user_id,
            user: metadata.user,
            like_details: metadata.like,
            metadata_incomplete: metadata.incomplete,
            data,
        }))
    }
}

fn social_kind(key: &str, action: u64) -> &'static str {
    // Prefer the display key: action values can differ across protocol variants.
    // Recognize complete key tokens; e.g. "unfollow" is not a "follow" notification.
    let mut follow = false;
    let mut share = false;
    for token in key.split('_') {
        follow |= token == "follow";
        share |= token == "share";
    }
    match (key.is_empty(), follow, share, action) {
        (_, true, false, _) => "follow",
        (_, false, true, _) => "share",
        (true, _, _, 1) => "follow",
        (true, _, _, 3) => "share",
        _ => "social",
    }
}

#[cfg(test)]
mod memory_tests {
    use super::*;
    #[test]
    fn state_stays_bounded_after_many_distinct_messages_and_streaks() {
        let mut processor = Processor::new("42");
        for id in 1..=25_000 {
            let raw = Envelope {
                method: "WebcastGiftMessage".into(),
                msg_id: id,
                is_history: false,
                payload: Gift {
                    gift_id: 1,
                    repeat_count: 1,
                    repeat_end: true,
                    group_id: id,
                    user: Some(User { id: 42 }),
                    gift: Some(GiftMeta {
                        kind: 1,
                        diamonds: 1,
                        name: "Rose".into(),
                    }),
                    receiver_id: format!("receiver-{id}"),
                }
                .encode_to_vec(),
            };
            assert!(processor.process(&raw).unwrap().is_some());
        }
        assert_eq!((processor.ids.len(), processor.order.len()), (8192, 8192));
        assert_eq!(
            (processor.streaks.len(), processor.streak_order.len()),
            (4096, 4096)
        );
    }
}
