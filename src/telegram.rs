use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use reqwest::multipart::{Form, Part};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::fs::File;
use tokio_util::io::ReaderStream;
use url::Url;

use crate::model::TelegramChatId;
use crate::Config;

// Telegram method responses are control-plane JSON, not streamed file downloads.
const CONTROL_RESPONSE_BODY_CAP: usize = 1_048_576;

#[async_trait]
pub trait TelegramApi: Send + Sync {
    async fn send_message(
        &self,
        chat_id: &TelegramChatId,
        text: &str,
        parse_mode: Option<&str>,
    ) -> Result<TelegramMessageResult, TelegramError>;
    async fn send_chat_action(
        &self,
        chat_id: &TelegramChatId,
        action: &str,
    ) -> Result<(), TelegramError>;
    async fn send_location(
        &self,
        chat_id: &TelegramChatId,
        latitude: f64,
        longitude: f64,
    ) -> Result<TelegramMessageResult, TelegramError>;
    async fn send_file(
        &self,
        method: &str,
        field: &str,
        chat_id: &TelegramChatId,
        file: UploadFile,
        caption: Option<&str>,
    ) -> Result<TelegramMessageResult, TelegramError>;
    async fn get_file(&self, file_id: &str) -> Result<TelegramFileInfo, TelegramError>;
    fn file_url(&self, file_path: &str) -> Result<Url, TelegramError>;
}

#[derive(Clone)]
pub struct TelegramClient {
    http: reqwest::Client,
    config: Config,
}

impl TelegramClient {
    pub fn new(http: reqwest::Client, config: Config) -> Self {
        Self { http, config }
    }

    fn method_url(&self, method: &str) -> Result<Url, TelegramError> {
        validate_path_segment(method)?;
        let mut url = self.config.telegram_api_base.clone();
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|_| TelegramError::invalid_url())?;
            path.pop_if_empty();
            path.extend([format!("bot{}", self.config.bot_token), method.to_string()]);
        }
        Ok(url)
    }

    async fn post_json<T: Serialize + Sync>(
        &self,
        method: &'static str,
        payload: &T,
    ) -> Result<Value, TelegramError> {
        let response = self
            .http
            .post(self.method_url(method)?)
            .timeout(self.config.submit_timeout)
            .json(payload)
            .send()
            .await
            .map_err(|_| TelegramError::transport(method))?;
        self.parse_response(method, response).await
    }

    async fn parse_response(
        &self,
        method: &'static str,
        response: reqwest::Response,
    ) -> Result<Value, TelegramError> {
        let status = response.status();
        let bytes = bounded_control_response_body(method, status.as_u16(), response).await?;
        let payload: Value = serde_json::from_slice(&bytes)
            .map_err(|_| TelegramError::malformed_http_response(method, status.as_u16()))?;
        let ok = payload.get("ok").and_then(Value::as_bool).unwrap_or(false);
        if status.is_success() && ok {
            return Ok(payload.get("result").cloned().unwrap_or(Value::Null));
        }
        Err(TelegramError::from_payload(
            method,
            status.as_u16(),
            &payload,
            &self.config.bot_token,
        ))
    }
}

#[async_trait]
impl TelegramApi for TelegramClient {
    async fn send_message(
        &self,
        chat_id: &TelegramChatId,
        text: &str,
        parse_mode: Option<&str>,
    ) -> Result<TelegramMessageResult, TelegramError> {
        #[derive(Serialize)]
        struct SendMessage<'a> {
            chat_id: &'a TelegramChatId,
            text: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            parse_mode: Option<&'a str>,
            #[serde(skip_serializing_if = "Option::is_none")]
            disable_web_page_preview: Option<bool>,
        }

        let result = self
            .post_json(
                "sendMessage",
                &SendMessage {
                    chat_id,
                    text,
                    parse_mode,
                    disable_web_page_preview: parse_mode.map(|_| true),
                },
            )
            .await?;
        serde_json::from_value(result).map_err(|_| TelegramError::bad_response("sendMessage"))
    }

    async fn send_chat_action(
        &self,
        chat_id: &TelegramChatId,
        action: &str,
    ) -> Result<(), TelegramError> {
        #[derive(Serialize)]
        struct SendChatAction<'a> {
            chat_id: &'a TelegramChatId,
            action: &'a str,
        }

        self.post_json("sendChatAction", &SendChatAction { chat_id, action })
            .await?;
        Ok(())
    }

    async fn send_location(
        &self,
        chat_id: &TelegramChatId,
        latitude: f64,
        longitude: f64,
    ) -> Result<TelegramMessageResult, TelegramError> {
        #[derive(Serialize)]
        struct SendLocation<'a> {
            chat_id: &'a TelegramChatId,
            latitude: f64,
            longitude: f64,
        }

        let result = self
            .post_json(
                "sendLocation",
                &SendLocation {
                    chat_id,
                    latitude,
                    longitude,
                },
            )
            .await?;
        serde_json::from_value(result).map_err(|_| TelegramError::bad_response("sendLocation"))
    }

    async fn send_file(
        &self,
        method: &str,
        field: &str,
        chat_id: &TelegramChatId,
        file: UploadFile,
        caption: Option<&str>,
    ) -> Result<TelegramMessageResult, TelegramError> {
        validate_path_segment(method)?;
        validate_path_segment(field)?;
        let method = match method {
            "sendDocument" => "sendDocument",
            "sendPhoto" => "sendPhoto",
            "sendVoice" => "sendVoice",
            "sendAudio" => "sendAudio",
            _ => return Err(TelegramError::validation("send_file")),
        };
        let file_handle = File::open(file.path())
            .await
            .map_err(|_| TelegramError::validation(method))?;
        let file_len = file_handle
            .metadata()
            .await
            .map_err(|_| TelegramError::validation(method))?
            .len();
        if file_len > self.config.max_outbound_file_bytes {
            return Err(TelegramError::validation_description(
                method,
                format!(
                    "outbound file is {} bytes and exceeds {} bytes",
                    file_len, self.config.max_outbound_file_bytes
                ),
            ));
        }
        let field = field.to_string();
        let stream = ReaderStream::new(file_handle);
        let body = reqwest::Body::wrap_stream(stream);
        let mut part =
            Part::stream_with_length(body, file_len).file_name(file.filename().to_string());
        if let Some(content_type) = file.content_type() {
            part = part
                .mime_str(content_type)
                .map_err(|_| TelegramError::validation(method))?;
        }
        let mut form = Form::new()
            .text("chat_id", chat_id.to_string())
            .part(field, part);
        if let Some(caption) = caption {
            form = form.text("caption", caption.chars().take(1024).collect::<String>());
        }
        let response = self
            .http
            .post(self.method_url(method)?)
            .timeout(self.config.audio_tool_timeout)
            .multipart(form)
            .send()
            .await
            .map_err(|_| TelegramError::transport(method))?;
        let result = self.parse_response(method, response).await?;
        serde_json::from_value(result).map_err(|_| TelegramError::bad_response(method))
    }

    async fn get_file(&self, file_id: &str) -> Result<TelegramFileInfo, TelegramError> {
        #[derive(Serialize)]
        struct GetFile<'a> {
            file_id: &'a str,
        }

        let result = self.post_json("getFile", &GetFile { file_id }).await?;
        serde_json::from_value(result).map_err(|_| TelegramError::bad_response("getFile"))
    }

    fn file_url(&self, file_path: &str) -> Result<Url, TelegramError> {
        validate_relative_path(file_path)?;
        let mut url = self.config.telegram_file_api_base.clone();
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|_| TelegramError::invalid_url())?;
            path.pop_if_empty();
            path.push(&format!("bot{}", self.config.bot_token));
            path.extend(file_path.split('/'));
        }
        Ok(url)
    }
}

#[derive(Debug, Clone)]
pub struct UploadFile {
    path: std::path::PathBuf,
    filename: String,
    content_type: Option<String>,
}

impl UploadFile {
    pub fn new(
        path: impl Into<std::path::PathBuf>,
        filename: impl Into<String>,
        content_type: Option<String>,
    ) -> Self {
        Self {
            path: path.into(),
            filename: filename.into(),
            content_type,
        }
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TelegramMessageResult {
    #[serde(default)]
    pub message_id: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TelegramFileInfo {
    #[serde(default)]
    pub file_id: Option<String>,
    #[serde(default)]
    pub file_unique_id: Option<String>,
    #[serde(default)]
    pub file_size: Option<u64>,
    #[serde(default)]
    pub file_path: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    TransientBackend,
    PermissionDenied,
    Validation,
    PermanentBackend,
    BadResponse,
}

impl ErrorCategory {
    pub fn as_receipt_category(self) -> &'static str {
        match self {
            Self::TransientBackend => "transient_backend",
            Self::PermissionDenied => "permission",
            Self::Validation => "validation",
            Self::PermanentBackend => "permanent_backend",
            Self::BadResponse => "transient_backend",
        }
    }
}

#[derive(Debug, Error, Clone)]
#[error("Telegram {method} failed: {description}")]
pub struct TelegramError {
    method: &'static str,
    category: ErrorCategory,
    status: Option<u16>,
    description: String,
    retry_after: Option<Duration>,
}

impl TelegramError {
    pub fn from_status(method: &'static str, status: u16, retry_after: Option<Duration>) -> Self {
        Self {
            method,
            category: category_for_payload_status(status),
            status: Some(status),
            description: "request rejected".to_string(),
            retry_after,
        }
    }

    pub fn method(&self) -> &'static str {
        self.method
    }

    pub fn category(&self) -> ErrorCategory {
        self.category
    }

    pub fn status(&self) -> Option<u16> {
        self.status
    }

    pub fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    fn transport(method: &'static str) -> Self {
        Self {
            method,
            category: ErrorCategory::TransientBackend,
            status: None,
            description: "request failed".to_string(),
            retry_after: None,
        }
    }

    fn bad_response(method: &'static str) -> Self {
        Self {
            method,
            category: ErrorCategory::BadResponse,
            status: None,
            description: "malformed Telegram response".to_string(),
            retry_after: None,
        }
    }

    fn validation(method: &'static str) -> Self {
        Self::validation_description(method, "invalid Telegram request".to_string())
    }

    fn validation_description(method: &'static str, description: String) -> Self {
        Self {
            method,
            category: ErrorCategory::Validation,
            status: None,
            description,
            retry_after: None,
        }
    }

    fn invalid_url() -> Self {
        Self::validation("url")
    }

    fn malformed_http_response(method: &'static str, http_status: u16) -> Self {
        Self {
            method,
            category: category_for_http_status(http_status),
            status: Some(http_status),
            description: "invalid Telegram JSON response".to_string(),
            retry_after: None,
        }
    }

    fn oversized_http_response(method: &'static str, http_status: u16) -> Self {
        Self {
            method,
            category: if (200..=299).contains(&http_status) {
                ErrorCategory::BadResponse
            } else {
                category_for_http_status(http_status)
            },
            status: Some(http_status),
            description: format!("Telegram response exceeded {CONTROL_RESPONSE_BODY_CAP} bytes"),
            retry_after: None,
        }
    }

    fn from_payload(
        method: &'static str,
        http_status: u16,
        payload: &Value,
        bot_token: &str,
    ) -> Self {
        let payload_status = payload
            .get("error_code")
            .and_then(Value::as_u64)
            .and_then(|value| u16::try_from(value).ok())
            .unwrap_or(http_status);
        let classified_status = if (200..=299).contains(&http_status) {
            payload_status
        } else {
            http_status
        };
        let retry_after = payload
            .pointer("/parameters/retry_after")
            .and_then(Value::as_u64)
            .map(Duration::from_secs);
        let category = if (200..=299).contains(&http_status) {
            category_for_payload_status(payload_status)
        } else {
            category_for_http_status(http_status)
        };
        let description = redact_token(
            payload
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or("Telegram returned an error")
                .to_string(),
            bot_token,
        );
        Self {
            method,
            category,
            status: Some(classified_status),
            description,
            retry_after,
        }
    }
}

fn category_for_http_status(http_status: u16) -> ErrorCategory {
    match http_status {
        429 => ErrorCategory::TransientBackend,
        500..=599 => ErrorCategory::TransientBackend,
        400..=499 => ErrorCategory::PermanentBackend,
        _ => ErrorCategory::BadResponse,
    }
}

fn category_for_payload_status(status: u16) -> ErrorCategory {
    match status {
        429 => ErrorCategory::TransientBackend,
        500..=599 => ErrorCategory::TransientBackend,
        400..=499 => ErrorCategory::PermanentBackend,
        _ => ErrorCategory::BadResponse,
    }
}

fn redact_token(mut value: String, bot_token: &str) -> String {
    if !bot_token.is_empty() {
        value = value.replace(bot_token, "[redacted]");
    }
    value
}

fn validate_path_segment(segment: &str) -> Result<(), TelegramError> {
    if segment.is_empty() || segment.contains('/') || segment == "." || segment == ".." {
        return Err(TelegramError::validation("url"));
    }
    Ok(())
}

fn validate_relative_path(path: &str) -> Result<(), TelegramError> {
    if path.is_empty()
        || path.starts_with('/')
        || path
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(TelegramError::validation("file_url"));
    }
    Ok(())
}

async fn bounded_control_response_body(
    method: &'static str,
    http_status: u16,
    response: reqwest::Response,
) -> Result<Vec<u8>, TelegramError> {
    if response
        .content_length()
        .is_some_and(|len| len > CONTROL_RESPONSE_BODY_CAP as u64)
    {
        return Err(TelegramError::oversized_http_response(method, http_status));
    }

    let mut bytes = Vec::with_capacity(
        response
            .content_length()
            .unwrap_or_default()
            .min(CONTROL_RESPONSE_BODY_CAP as u64) as usize,
    );
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| TelegramError::transport(method))?;
        if chunk.len() > CONTROL_RESPONSE_BODY_CAP.saturating_sub(bytes.len()) {
            return Err(TelegramError::oversized_http_response(method, http_status));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
