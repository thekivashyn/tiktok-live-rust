//! Bounded sender profiles and like-specific metadata. No profile cache or avatar requests.
mod models;
use anyhow::{Result, ensure};
pub use models::*;
use prost::{
    Message,
    encoding::{DecodeContext, WireType, decode_key, decode_varint, skip_field},
};
use serde::{Serialize, Serializer};
use std::{borrow::Cow, fmt::Display};

pub const MAX_METADATA_WIRE: usize = 32 * 1024;
const MAX_TEXT_BYTES: usize = 16 * 1024;
const MAX_FIELDS: usize = 512;
const MAX_NODES: usize = 128;
const MAX_DEPTH: usize = 12;

fn decimal<T: Display, S: Serializer>(value: &T, serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_str(value)
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct UserProfile {
    #[serde(serialize_with = "decimal")]
    pub id: u64,
    pub username: Option<String>,
    pub display_name: Option<String>,
    pub avatar_url: Option<String>,
    pub verified: bool,
    pub sec_uid: Option<String>,
    pub follow_info: Option<Box<FollowInfo>>,
}

fn nonempty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}
fn safe_avatar(value: &str) -> bool {
    url::Url::parse(value).is_ok_and(|url| {
        matches!(url.scheme(), "https" | "http")
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
    })
}
impl From<WireUser> for UserProfile {
    fn from(user: WireUser) -> Self {
        // Prefer the largest supplied avatar. Never synthesize an endpoint or fetch it.
        let avatar_url = [&user.avatar_large, &user.avatar_medium, &user.avatar_thumb]
            .into_iter()
            .flatten()
            .find_map(|image| image.url_list.iter().find(|url| safe_avatar(url)).cloned());
        Self {
            id: if user.id != 0 {
                user.id
            } else {
                user.id_str.parse().unwrap_or(0)
            },
            username: nonempty(user.display_id),
            display_name: nonempty(user.nickname),
            avatar_url,
            verified: user.verified,
            sec_uid: nonempty(user.sec_uid),
            follow_info: user.follow_info,
        }
    }
}

#[derive(Clone, Copy)]
enum Shape {
    Ignore,
    Scalar,
    Text(usize),
    Nested(fn(u32) -> Rule),
}
#[derive(Clone, Copy)]
struct Rule {
    shape: Shape,
    max_count: usize,
}
impl Rule {
    const IGNORE: Self = Self {
        shape: Shape::Ignore,
        max_count: 0,
    };
}
#[derive(Default)]
struct Budget {
    fields: usize,
    nodes: usize,
    text_bytes: usize,
}

fn bytes_field<'a>(bytes: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len = usize::try_from(decode_varint(bytes)?)?;
    ensure!(len <= bytes.len(), "metadata field is truncated");
    let (field, rest) = bytes.split_at(len);
    *bytes = rest;
    Ok(field)
}

fn preflight(
    mut bytes: &[u8],
    rules: fn(u32) -> Rule,
    budget: &mut Budget,
    depth: usize,
) -> Result<()> {
    budget.nodes += 1;
    ensure!(
        depth <= MAX_DEPTH && budget.nodes <= MAX_NODES,
        "metadata nesting exceeds limit"
    );
    let mut counts = [0u16; 32];
    while !bytes.is_empty() {
        budget.fields += 1;
        ensure!(
            budget.fields <= MAX_FIELDS,
            "metadata field count exceeds limit"
        );
        let (tag, wire) = decode_key(&mut bytes)?;
        let rule = rules(tag);
        if rule.max_count != 0 {
            let count = counts
                .get_mut(tag as usize - 1)
                .ok_or_else(|| anyhow::anyhow!("invalid metadata rule"))?;
            *count += 1;
            ensure!(
                *count as usize <= rule.max_count,
                "metadata list exceeds limit"
            );
        }
        match rule.shape {
            Shape::Text(max) => {
                ensure!(
                    wire == WireType::LengthDelimited,
                    "invalid metadata string wire type"
                );
                let text = bytes_field(&mut bytes)?;
                budget.text_bytes += text.len();
                ensure!(
                    text.len() <= max && budget.text_bytes <= MAX_TEXT_BYTES,
                    "metadata text exceeds limit"
                );
                std::str::from_utf8(text)?;
            }
            Shape::Nested(child) => {
                ensure!(
                    wire == WireType::LengthDelimited,
                    "invalid metadata message wire type"
                );
                preflight(bytes_field(&mut bytes)?, child, budget, depth + 1)?;
            }
            Shape::Ignore | Shape::Scalar => {
                skip_field(wire, tag, &mut bytes, DecodeContext::default())?
            }
        }
    }
    Ok(())
}

fn bounded_decode<M: Message + Default>(bytes: &[u8], rules: fn(u32) -> Rule) -> Result<M> {
    ensure!(
        bytes.len() <= MAX_METADATA_WIRE,
        "metadata wire size exceeds limit"
    );
    preflight(bytes, rules, &mut Budget::default(), 0)?;
    Ok(M::decode(bytes)?)
}

/// Merge repeated occurrences of a singular embedded message as protobuf requires.
fn submessage(mut bytes: &[u8], wanted: u32) -> Result<Option<Cow<'_, [u8]>>> {
    let mut found: Option<Cow<'_, [u8]>> = None;
    while !bytes.is_empty() {
        let (tag, wire) = decode_key(&mut bytes)?;
        if tag == wanted {
            ensure!(
                wire == WireType::LengthDelimited,
                "invalid sender wire type"
            );
            let field = bytes_field(&mut bytes)?;
            let current_len = found.as_ref().map_or(0, |v| v.len());
            ensure!(
                current_len + field.len() <= MAX_METADATA_WIRE,
                "sender exceeds metadata limit"
            );
            if let Some(current) = &mut found {
                current.to_mut().extend_from_slice(field);
            } else {
                found = Some(Cow::Borrowed(field));
            }
        } else {
            skip_field(wire, tag, &mut bytes, DecodeContext::default())?;
        }
    }
    Ok(found)
}

#[derive(Default)]
pub(crate) struct Extracted {
    pub user: Option<Box<UserProfile>>,
    pub like: Option<Box<LikeDetails>>,
    pub incomplete: bool,
}

pub(crate) fn extract(method: &str, payload: &[u8]) -> Extracted {
    let mut result = Extracted::default();
    let user_tag = match method {
        "WebcastChatMessage" | "WebcastMemberMessage" | "WebcastSocialMessage" => Some(2),
        "WebcastGiftMessage" => Some(7),
        "WebcastLikeMessage" => Some(5),
        _ => None,
    };
    if let Some(tag) = user_tag {
        let profile = (|| -> Result<Option<UserProfile>> {
            let Some(bytes) = submessage(payload, tag)? else {
                return Ok(None);
            };
            let user: WireUser = bounded_decode(&bytes, WireUser::rule)?;
            ensure!(user.within_limits(), "merged sender metadata exceeds limit");
            Ok(Some(user.into()))
        })();
        match profile {
            Ok(user) => result.user = user.map(Box::new),
            Err(_) => result.incomplete = true,
        }
    }
    if method == "WebcastLikeMessage" {
        let details = (|| -> Result<LikeDetails> {
            let details: LikeDetails = bounded_decode(payload, LikeDetails::rule)?;
            ensure!(
                details.within_limits(),
                "merged like metadata exceeds limit"
            );
            Ok(details)
        })();
        match details {
            Ok(value) => result.like = Some(Box::new(value)),
            Err(_) => result.incomplete = true,
        }
    }
    result
}
