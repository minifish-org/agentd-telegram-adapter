use std::collections::BTreeMap;
use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ValidationError {
    #[error("delivery destination is not in the configured scope: {destination:?}")]
    WrongPrefix { destination: String, prefix: String },
    #[error("Telegram destination is missing chat id")]
    MissingChatId,
    #[error("Telegram destination chat id must be an integer: {chat_id:?}")]
    InvalidChatId { chat_id: String },
}

pub fn chat_id_from_destination(destination: &str, prefix: &str) -> Result<i64, ValidationError> {
    if !destination.starts_with(prefix) {
        return Err(ValidationError::WrongPrefix {
            destination: destination.to_string(),
            prefix: prefix.to_string(),
        });
    }
    let chat_id = destination[prefix.len()..].trim();
    if chat_id.is_empty() {
        return Err(ValidationError::MissingChatId);
    }
    chat_id
        .parse::<i64>()
        .map_err(|_| ValidationError::InvalidChatId {
            chat_id: chat_id.to_string(),
        })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum TelegramChatId {
    Numeric(i64),
    String(String),
}

impl From<i64> for TelegramChatId {
    fn from(value: i64) -> Self {
        Self::Numeric(value)
    }
}

impl fmt::Display for TelegramChatId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Numeric(value) => value.fmt(f),
            Self::String(value) => value.fmt(f),
        }
    }
}

pub fn telegram_chat_id_from_destination(
    destination: &str,
    prefix: &str,
) -> Result<TelegramChatId, ValidationError> {
    if !destination.starts_with(prefix) {
        return Err(ValidationError::WrongPrefix {
            destination: destination.to_string(),
            prefix: prefix.to_string(),
        });
    }
    let chat_id = destination[prefix.len()..].trim();
    if chat_id.is_empty() {
        return Err(ValidationError::MissingChatId);
    }
    let digits = chat_id
        .strip_prefix('-')
        .or_else(|| chat_id.strip_prefix('+'))
        .unwrap_or(chat_id);
    if !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return chat_id
            .parse::<i64>()
            .map(TelegramChatId::Numeric)
            .map_err(|_| ValidationError::InvalidChatId {
                chat_id: chat_id.to_string(),
            });
    }
    Ok(TelegramChatId::String(chat_id.to_string()))
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TelegramUpdate {
    #[serde(default)]
    pub update_id: Option<i64>,
    #[serde(default)]
    pub message: Option<TelegramMessage>,
    #[serde(default)]
    pub edited_message: Option<TelegramMessage>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TelegramMessage {
    #[serde(default)]
    pub message_id: Option<i64>,
    #[serde(default)]
    pub from: Option<TelegramUser>,
    #[serde(default)]
    pub chat: Option<TelegramChat>,
    #[serde(default)]
    pub date: Option<i64>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub caption: Option<String>,
    #[serde(default)]
    pub document: Option<TelegramFile>,
    #[serde(default)]
    pub voice: Option<TelegramFile>,
    #[serde(default)]
    pub audio: Option<TelegramFile>,
    #[serde(default)]
    pub photo: Vec<TelegramPhotoSize>,
    #[serde(default)]
    pub location: Option<TelegramLocation>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TelegramUser {
    #[serde(default)]
    pub id: Option<i64>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TelegramChat {
    #[serde(default)]
    pub id: Option<i64>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default, rename = "type")]
    pub chat_type: Option<String>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TelegramFile {
    #[serde(default)]
    pub file_id: Option<String>,
    #[serde(default)]
    pub file_unique_id: Option<String>,
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub file_size: Option<u64>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TelegramPhotoSize {
    #[serde(default)]
    pub file_id: Option<String>,
    #[serde(default)]
    pub width: Option<u32>,
    #[serde(default)]
    pub height: Option<u32>,
    #[serde(default)]
    pub file_size: Option<u64>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TelegramLocation {
    #[serde(default)]
    pub latitude: Option<f64>,
    #[serde(default)]
    pub longitude: Option<f64>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DeliveryOutboxRecord {
    #[serde(default)]
    pub delivery_id: Option<Uuid>,
    #[serde(default)]
    pub tenant: Option<String>,
    #[serde(default)]
    pub run_id: Option<Uuid>,
    #[serde(default)]
    pub claim_token: Option<String>,
    #[serde(default)]
    pub destination: Option<String>,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DeliveryPayload {
    #[serde(default)]
    pub reply: Option<String>,
    #[serde(default)]
    pub attachments: Vec<DeliveryAttachment>,
    #[serde(default)]
    pub location: Option<DeliveryLocation>,
    #[serde(default)]
    pub voice_reply: bool,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DeliveryAttachment {
    #[serde(default)]
    pub artifact_ref: Option<String>,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct DeliveryLocation {
    #[serde(default)]
    pub latitude: Option<f64>,
    #[serde(default)]
    pub longitude: Option<f64>,
    #[serde(default, flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::{
        chat_id_from_destination, telegram_chat_id_from_destination, DeliveryOutboxRecord,
        TelegramChatId, TelegramUpdate,
    };

    #[test]
    fn destination_requires_the_configured_prefix_and_integer_chat_id() {
        assert_eq!(chat_id_from_destination("tg:-1007", "tg:").unwrap(), -1007);
        assert_eq!(chat_id_from_destination("tg:42", "tg:").unwrap(), 42);
        assert!(chat_id_from_destination("tg:", "tg:").is_err());
        assert!(chat_id_from_destination("matrix:7", "tg:").is_err());
        assert!(chat_id_from_destination("tg:not-a-number", "tg:").is_err());
    }

    #[test]
    fn delivery_destination_supports_numeric_and_string_telegram_chat_ids() {
        assert_eq!(
            telegram_chat_id_from_destination("tg:-1007", "tg:").unwrap(),
            TelegramChatId::Numeric(-1007)
        );
        assert_eq!(
            telegram_chat_id_from_destination("tg: @channel_name ", "tg:").unwrap(),
            TelegramChatId::String("@channel_name".to_string())
        );
        assert_eq!(
            serde_json::to_value(TelegramChatId::Numeric(42)).unwrap(),
            json!(42)
        );
        assert_eq!(
            serde_json::to_value(TelegramChatId::String("@ops".to_string())).unwrap(),
            json!("@ops")
        );
        assert!(telegram_chat_id_from_destination("tg:", "tg:").is_err());
        assert!(telegram_chat_id_from_destination("matrix:@ops", "tg:").is_err());
        assert!(telegram_chat_id_from_destination("tg:999999999999999999999", "tg:").is_err());
    }

    #[test]
    fn telegram_updates_and_delivery_payloads_accept_unknown_fields() {
        let update: TelegramUpdate = serde_json::from_value(json!({
            "update_id": 77,
            "message": {
                "message_id": 10,
                "text": "hello",
                "from": {"id": 42, "is_bot": false},
                "chat": {"id": -1007, "type": "private", "title": "ops"},
                "unexpected_field": {"nested": true}
            },
            "new_envelope_field": "kept"
        }))
        .unwrap();
        assert_eq!(update.update_id, Some(77));
        assert_eq!(
            update
                .message
                .as_ref()
                .and_then(|message| message.chat.as_ref())
                .and_then(|chat| chat.chat_type.as_deref()),
            Some("private")
        );
        assert!(update.extra.contains_key("new_envelope_field"));
        assert!(update
            .message
            .as_ref()
            .unwrap()
            .extra
            .contains_key("unexpected_field"));

        let delivery: DeliveryOutboxRecord = serde_json::from_value(json!({
            "delivery_id": "7b6f5fb7-8505-4f6d-8fd0-c7709aa7a5a1",
            "destination": "tg:12345",
            "payload": {
                "reply": "hello",
                "attachments": [{"artifact_ref": "artifact://demo/out.txt", "caption": "raw"}],
                "voice_reply": true,
                "future_field": {"x": 1}
            },
            "claim_id": "54e86af8-34d7-4bf7-bf93-1df5d364ca8d",
            "worker_hint": "ignored"
        }))
        .unwrap();
        assert_eq!(delivery.destination.as_deref(), Some("tg:12345"));
        assert_eq!(
            delivery.payload,
            json!({
                "reply": "hello",
                "attachments": [{
                    "artifact_ref": "artifact://demo/out.txt",
                    "caption": "raw"
                }],
                "voice_reply": true,
                "future_field": {"x": 1}
            })
        );
        assert!(delivery.extra.contains_key("worker_hint"));
    }

    #[test]
    fn delivery_outbox_payload_accepts_any_json_value_without_loss() {
        let payloads = [
            json!("reply"),
            Value::Null,
            json!(["reply", {"nested": true}]),
            json!(42),
            json!(true),
            json!({"reply": "hello"}),
        ];

        for expected in payloads {
            let delivery: DeliveryOutboxRecord = serde_json::from_value(json!({
                "payload": expected.clone(),
            }))
            .unwrap();
            assert_eq!(serde_json::to_value(delivery.payload).unwrap(), expected);
        }
    }
}
