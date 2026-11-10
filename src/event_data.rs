//! Typed payloads avoid allocating a JSON object tree for every event.
//! Serialization preserves the existing NDJSON field names and string-valued large IDs.
use serde::{Serialize, Serializer};

fn decimal_string<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}

#[derive(Debug, Serialize, PartialEq)]
#[serde(untagged)]
pub enum EventData {
    Chat {
        text: String,
    },
    Gift {
        #[serde(serialize_with = "decimal_string")]
        gift_id: u64,
        #[serde(serialize_with = "decimal_string")]
        group_id: u64,
        repeat_count: u32,
        repeat_end: bool,
        quantity_delta: Option<u32>,
        gift_type: Option<u32>,
        receiver_id: String,
        gift_name: Option<String>,
        diamonds_per_gift: Option<u32>,
    },
    Like {
        count: u32,
        #[serde(serialize_with = "decimal_string")]
        total: u64,
    },
    Social {
        #[serde(serialize_with = "decimal_string")]
        action: u64,
        display_key: String,
    },
    Member {
        action: u32,
    },
    Viewers {
        #[serde(serialize_with = "decimal_string")]
        count: u64,
    },
    Control {
        action: u32,
    },
}
