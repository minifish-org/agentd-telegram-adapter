use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde_json::{json, Map, Value};
use thiserror::Error;
use tokio::sync::watch;
use url::Url;
use uuid::Uuid;

use crate::agentd::{AgentdApi, AgentdError, DeliveryAck};
use crate::media::transcode_to_ogg_opus;
use crate::model::{
    telegram_chat_id_from_destination, DeliveryAttachment, DeliveryLocation, DeliveryOutboxRecord,
    TelegramChatId,
};
use crate::render::{capped_plain_text, telegram_html};
use crate::telegram::{
    ErrorCategory, TelegramApi, TelegramError, TelegramMessageResult, UploadFile,
};
use crate::Config;

const EMPTY_REPLY: &str = "（agent 没有返回内容）";
const MARKER_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone)]
pub struct DeliveryService {
    config: Config,
    agentd: Arc<dyn AgentdApi>,
    telegram: Arc<dyn TelegramApi>,
}

impl DeliveryService {
    pub fn new(config: Config, agentd: Arc<dyn AgentdApi>, telegram: Arc<dyn TelegramApi>) -> Self {
        Self {
            config,
            agentd,
            telegram,
        }
    }

    pub async fn process_outbox_once(&self) -> usize {
        let deliveries = match self
            .agentd
            .claim_delivery_outbox(self.config.outbox_claim_limit)
            .await
        {
            Ok(deliveries) => deliveries,
            Err(_) => return 0,
        };
        let count = deliveries.len();
        for delivery in deliveries {
            self.process_claimed_delivery(delivery).await;
        }
        count
    }

    pub async fn run_outbox_worker(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), DeliveryWorkerError> {
        let poll = Duration::from_secs_f64(self.config.outbox_poll_secs.max(0.001));
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            self.process_outbox_once().await;
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                _ = tokio::time::sleep(poll) => {}
            }
        }
    }

    async fn process_claimed_delivery(&self, delivery: DeliveryOutboxRecord) {
        let Some(delivery_id) = delivery.delivery_id else {
            return;
        };
        let Some(claim_token) = delivery.claim_token.clone() else {
            return;
        };
        let marker = delivery.run_id.map(|run_id| self.marker_path(run_id));
        let voice_requested = marker.as_ref().is_some_and(|path| path.is_file());
        let result = self.deliver_record(&delivery, voice_requested).await;
        let (outcome, error, retry_after_ms, terminal) = match result {
            Ok(_) => ("delivered".to_string(), None, None, true),
            Err(error) => {
                let terminal = error.is_terminal();
                let detail = error.detail(&delivery).to_string();
                let retry_after_ms = error.retry_after.map(|delay| delay.as_millis() as u64);
                (
                    if terminal { "failed" } else { "retry" }.to_string(),
                    Some(detail),
                    retry_after_ms,
                    terminal,
                )
            }
        };
        let ack = DeliveryAck {
            delivery_id,
            claim_token,
            outcome,
            error,
            retry_after_ms,
        };
        if self.agentd.ack_delivery(ack).await.is_ok() && terminal {
            if let Some(marker) = marker {
                let _ = remove_marker_after_ack(&marker).await;
            }
        }
    }

    async fn deliver_record(
        &self,
        delivery: &DeliveryOutboxRecord,
        voice_requested: bool,
    ) -> Result<Value, DeliveryFailure> {
        let destination = delivery
            .destination
            .as_deref()
            .ok_or_else(|| DeliveryFailure::validation("destination", "missing destination"))?;
        let chat_id =
            telegram_chat_id_from_destination(destination, &self.config.outbox_destination_prefix)
                .map_err(|_| DeliveryFailure::validation("destination", "invalid destination"))?;
        let payload = normalize_delivery_payload(&delivery.payload);
        let sent = self
            .deliver_payload(&chat_id, &payload, voice_requested)
            .await?;
        let first = sent.first();
        Ok(json!({
            "transport": "telegram",
            "chat_id": chat_id,
            "message_id": first.and_then(|message| message.message_id),
            "method": first.map(|message| message.method).unwrap_or("noop"),
            "telegram_messages": sent.iter().map(SentMessage::to_value).collect::<Vec<_>>(),
        }))
    }

    async fn deliver_payload(
        &self,
        chat_id: &TelegramChatId,
        payload: &Map<String, Value>,
        voice_requested: bool,
    ) -> Result<Vec<SentMessage>, DeliveryFailure> {
        let reply = payload
            .get("reply")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim();
        let location = match payload.get("location") {
            None | Some(Value::Null) => None,
            Some(location) => Some(
                serde_json::from_value::<DeliveryLocation>(location.clone())
                    .map_err(|_| DeliveryFailure::validation("location", "invalid location"))?,
            ),
        };
        let attachments = match payload.get("attachments") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(attachments)) => attachments
                .iter()
                .map(|attachment| {
                    serde_json::from_value::<DeliveryAttachment>(attachment.clone()).map_err(|_| {
                        DeliveryFailure::validation("attachments", "invalid attachment")
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
            Some(_) => {
                return Err(DeliveryFailure::validation(
                    "attachments",
                    "attachments must be an array",
                ));
            }
        };
        let mut sent = Vec::new();
        let should_send_placeholder =
            reply.is_empty() && attachments.is_empty() && location.is_none();
        let spoken_text = if should_send_placeholder {
            EMPTY_REPLY
        } else {
            reply
        };
        if !spoken_text.is_empty() {
            sent.push(self.send_text(chat_id, spoken_text).await?);
        }
        if let Some(location) = location {
            sent.push(self.send_location(chat_id, &location).await?);
        }
        for attachment in &attachments {
            sent.push(self.send_attachment(chat_id, attachment).await?);
        }
        if voice_requested && !reply.is_empty() {
            match self.send_voice(chat_id, reply).await {
                Ok(message) => sent.push(message),
                Err(error) => log_voice_failure(&error),
            }
        }
        Ok(sent)
    }

    async fn send_text(
        &self,
        chat_id: &TelegramChatId,
        text: &str,
    ) -> Result<SentMessage, DeliveryFailure> {
        let plain = capped_plain_text(text);
        let html = telegram_html(text);
        if html.chars().count() <= 4096 {
            match self
                .telegram
                .send_message(chat_id, &html, Some("HTML"))
                .await
            {
                Ok(result) => return Ok(SentMessage::new("sendMessage", result)),
                Err(error)
                    if error.category() == ErrorCategory::PermanentBackend
                        && error.status() == Some(400) => {}
                Err(error) => return Err(DeliveryFailure::from_telegram(error)),
            }
        }
        self.telegram
            .send_message(chat_id, &plain, None)
            .await
            .map(|result| SentMessage::new("sendMessage", result))
            .map_err(DeliveryFailure::from_telegram)
    }

    async fn send_location(
        &self,
        chat_id: &TelegramChatId,
        location: &DeliveryLocation,
    ) -> Result<SentMessage, DeliveryFailure> {
        let latitude = location
            .latitude
            .filter(|value| value.is_finite())
            .ok_or_else(|| DeliveryFailure::validation("sendLocation", "missing latitude"))?;
        let longitude = location
            .longitude
            .filter(|value| value.is_finite())
            .ok_or_else(|| DeliveryFailure::validation("sendLocation", "missing longitude"))?;
        self.telegram
            .send_location(chat_id, latitude, longitude)
            .await
            .map(|result| SentMessage::new("sendLocation", result))
            .map_err(DeliveryFailure::from_telegram)
    }

    async fn send_attachment(
        &self,
        chat_id: &TelegramChatId,
        attachment: &DeliveryAttachment,
    ) -> Result<SentMessage, DeliveryFailure> {
        let reference = attachment.artifact_ref.as_deref().ok_or_else(|| {
            DeliveryFailure::validation("artifact_read", "attachment missing artifact_ref")
        })?;
        let artifact = ArtifactReference::parse(reference, &self.config.tenant)?;
        let media = self
            .agentd
            .download_artifact_to_temp(&artifact.tenant, &artifact.path)
            .await
            .map_err(|error| DeliveryFailure::from_agentd("artifact_read", error))?;
        let filename = attachment.filename.as_deref().unwrap_or("file");
        let content_type = attachment
            .content_type
            .clone()
            .or_else(|| Some(media.content_type().to_string()));
        let image = is_image(content_type.as_deref(), filename);
        if image {
            let upload = UploadFile::new(media.path(), filename, content_type.clone());
            match self
                .telegram
                .send_file("sendPhoto", "photo", chat_id, upload, None)
                .await
            {
                Ok(result) => return Ok(SentMessage::new("sendPhoto", result)),
                Err(error) if error.category() == ErrorCategory::PermanentBackend => {}
                Err(error) => return Err(DeliveryFailure::from_telegram(error)),
            }
        }
        let upload = UploadFile::new(media.path(), filename, content_type);
        self.telegram
            .send_file("sendDocument", "document", chat_id, upload, None)
            .await
            .map(|result| SentMessage::new("sendDocument", result))
            .map_err(DeliveryFailure::from_telegram)
    }

    async fn send_voice(
        &self,
        chat_id: &TelegramChatId,
        text: &str,
    ) -> Result<SentMessage, DeliveryFailure> {
        let capped: String = text.chars().take(self.config.tts_text_cap).collect();
        let synthesized = self
            .agentd
            .call_tool(
                "audio_synthesize",
                json!({
                    "text": capped,
                    "policy_intent": "Synthesize a Telegram voice reply requested by the user",
                }),
            )
            .await
            .map_err(|error| DeliveryFailure::from_agentd("audio_synthesize", error))?;
        let reference = synthesized
            .get("artifact_ref")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                DeliveryFailure::validation("audio_synthesize", "missing artifact_ref")
            })?;
        let artifact = ArtifactReference::parse(reference, &self.config.tenant)?;
        let media = self
            .agentd
            .download_artifact_to_temp(&artifact.tenant, &artifact.path)
            .await
            .map_err(|error| DeliveryFailure::from_agentd("artifact_read", error))?;
        let voice = transcode_to_ogg_opus(
            &media,
            &self.config.media_temp_dir,
            &self.config.ffmpeg_path,
        )
        .await
        .map_err(|_| DeliveryFailure::transient("ffmpeg", None, None))?;
        drop(media);
        self.telegram
            .send_file(
                "sendVoice",
                "voice",
                chat_id,
                UploadFile::new(voice.path(), "reply.ogg", Some("audio/ogg".to_string())),
                None,
            )
            .await
            .map(|result| SentMessage::new("sendVoice", result))
            .map_err(DeliveryFailure::from_telegram)
    }

    fn marker_path(&self, run_id: Uuid) -> PathBuf {
        self.config
            .state_dir
            .join("voice-replies")
            .join(run_id.to_string())
    }
}

fn log_voice_failure(error: &DeliveryFailure) {
    eprintln!(
        "{}",
        json!({
            "event": "voice_delivery_failed",
            "category": error.category,
            "method": error.method,
            "status": error.status,
        })
    );
}

#[derive(Debug, Error)]
pub enum DeliveryWorkerError {
    #[error("delivery worker stopped unexpectedly")]
    UnexpectedExit,
}

#[derive(Debug)]
struct SentMessage {
    method: &'static str,
    message_id: Option<i64>,
}

impl SentMessage {
    fn new(method: &'static str, result: TelegramMessageResult) -> Self {
        Self {
            method,
            message_id: result.message_id,
        }
    }

    fn to_value(&self) -> Value {
        json!({"method": self.method, "message_id": self.message_id})
    }
}

#[derive(Debug)]
struct DeliveryFailure {
    category: &'static str,
    method: &'static str,
    status: Option<u16>,
    retry_after: Option<Duration>,
    message: &'static str,
}

impl DeliveryFailure {
    fn validation(method: &'static str, message: &'static str) -> Self {
        Self {
            category: "validation",
            method,
            status: None,
            retry_after: None,
            message,
        }
    }

    fn transient(method: &'static str, status: Option<u16>, retry_after: Option<Duration>) -> Self {
        Self {
            category: "transient_backend",
            method,
            status,
            retry_after,
            message: "backend request failed",
        }
    }

    fn from_telegram(error: TelegramError) -> Self {
        Self {
            category: error.category().as_receipt_category(),
            method: error.method(),
            status: error.status(),
            retry_after: error.retry_after(),
            message: "Telegram request failed",
        }
    }

    fn from_agentd(method: &'static str, error: AgentdError) -> Self {
        match error {
            AgentdError::TenantMismatch
            | AgentdError::InvalidUrl
            | AgentdError::InvalidPath
            | AgentdError::TooLarge { .. } => Self::validation(method, "invalid artifact"),
            AgentdError::HttpStatus { status, .. } if status == 429 || status >= 500 => {
                Self::transient(method, Some(status), None)
            }
            AgentdError::HttpStatus { status, .. } => Self {
                category: "permanent_backend",
                method,
                status: Some(status),
                retry_after: None,
                message: "artifact backend rejected request",
            },
            AgentdError::ToolFailure { transient: true } => Self::transient(method, None, None),
            AgentdError::ToolFailure { transient: false } => Self {
                category: "permanent_backend",
                method,
                status: None,
                retry_after: None,
                message: "agentd tool rejected request",
            },
            AgentdError::Transport | AgentdError::Io(_) | AgentdError::InvalidJson => {
                Self::transient(method, None, None)
            }
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(self.category, "validation" | "permanent_backend")
    }

    fn detail(&self, delivery: &DeliveryOutboxRecord) -> Value {
        let mut detail = json!({
            "transport": "telegram",
            "destination": delivery.destination,
            "error_category": self.category,
            "error": self.message,
            "method": self.method,
        });
        let object = detail.as_object_mut().expect("detail is an object");
        if let Some(status) = self.status {
            object.insert("telegram_status_code".to_string(), json!(status));
        }
        if let Some(retry_after) = self.retry_after {
            object.insert(
                "retry_after_seconds".to_string(),
                json!(retry_after.as_secs()),
            );
        }
        detail
    }
}

fn normalize_delivery_payload(payload: &Value) -> Map<String, Value> {
    let normalized = match payload {
        Value::Object(object) if has_delivery_fields(object) => payload.clone(),
        Value::Object(object) if object.contains_key("payload") => {
            normalize_arbitrary(object.get("payload").expect("key exists"))
        }
        other => normalize_arbitrary(other),
    };
    normalized
        .as_object()
        .cloned()
        .expect("normalization always returns an object")
}

fn normalize_arbitrary(value: &Value) -> Value {
    match value {
        Value::Object(_) => value.clone(),
        Value::String(text) => json!({"reply": text}),
        Value::Null => json!({}),
        other => json!({"reply": serde_json::to_string(other).unwrap_or_default()}),
    }
}

fn has_delivery_fields(object: &Map<String, Value>) -> bool {
    ["reply", "attachments", "location", "voice_reply"]
        .iter()
        .any(|field| object.contains_key(*field))
}

struct ArtifactReference {
    tenant: String,
    path: String,
}

impl ArtifactReference {
    fn parse(reference: &str, expected_tenant: &str) -> Result<Self, DeliveryFailure> {
        let url = Url::parse(reference).map_err(|_| {
            DeliveryFailure::validation("artifact_read", "invalid artifact reference")
        })?;
        let tenant = url.host_str().unwrap_or_default();
        let path = url.path().trim_start_matches('/');
        if url.scheme() != "artifact"
            || tenant != expected_tenant
            || path.is_empty()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(DeliveryFailure::validation(
                "artifact_read",
                "invalid artifact reference",
            ));
        }
        Ok(Self {
            tenant: tenant.to_string(),
            path: path.to_string(),
        })
    }
}

fn is_image(content_type: Option<&str>, filename: &str) -> bool {
    content_type.is_some_and(|content_type| content_type.to_ascii_lowercase().starts_with("image/"))
        || [".jpg", ".jpeg", ".png", ".gif", ".webp"]
            .iter()
            .any(|extension| filename.to_ascii_lowercase().ends_with(extension))
}

pub fn prune_voice_reply_markers_at(state_dir: &Path, now: SystemTime) -> std::io::Result<usize> {
    let directory = state_dir.join("voice-replies");
    let entries = match std::fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error),
    };
    let mut pruned = 0;
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if Uuid::parse_str(name).is_err() {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(metadata) if metadata.is_file() => metadata,
            _ => continue,
        };
        let modified = match metadata.modified() {
            Ok(modified) => modified,
            Err(_) => continue,
        };
        if now
            .duration_since(modified)
            .is_ok_and(|age| age > MARKER_MAX_AGE)
            && std::fs::remove_file(entry.path()).is_ok()
        {
            pruned += 1;
        }
    }
    if pruned > 0 {
        sync_directory(&directory)?;
    }
    Ok(pruned)
}

pub fn prune_voice_reply_markers(state_dir: &Path) -> std::io::Result<usize> {
    prune_voice_reply_markers_at(state_dir, SystemTime::now())
}

async fn remove_marker_after_ack(marker: &Path) -> std::io::Result<()> {
    match tokio::fs::remove_file(marker).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    }
    if let Some(directory) = marker.parent() {
        sync_directory(directory)?;
    }
    Ok(())
}

#[cfg(unix)]
fn sync_directory(directory: &Path) -> std::io::Result<()> {
    std::fs::File::open(directory)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_directory: &Path) -> std::io::Result<()> {
    Ok(())
}
