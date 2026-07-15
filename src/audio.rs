use async_trait::async_trait;
use reqwest::multipart::{Form, Part};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

use crate::media::{response_body_to_temp, MediaError, TempMedia};
use crate::Config;

const TRANSCRIPT_BODY_CAP: u64 = 1_048_576;

#[async_trait]
pub trait AudioApi: Send + Sync {
    async fn transcribe(&self, media: &TempMedia, filename: &str) -> Result<String, AudioError>;
    async fn synthesize(&self, text: &str) -> Result<TempMedia, AudioError>;
}

#[derive(Clone)]
pub struct AudioClient {
    http: reqwest::Client,
    config: Config,
}

impl AudioClient {
    pub fn new(http: reqwest::Client, config: Config) -> Self {
        Self { http, config }
    }

    fn endpoint(&self, segment: &str) -> Result<Url, AudioError> {
        let mut url = self.config.audio_api_base.clone();
        let mut path = url
            .path_segments_mut()
            .map_err(|_| AudioError::InvalidUrl)?;
        path.pop_if_empty();
        path.extend(["audio", segment]);
        drop(path);
        Ok(url)
    }

    fn request(&self, endpoint: &str) -> Result<reqwest::RequestBuilder, AudioError> {
        let request = self
            .http
            .post(self.endpoint(endpoint)?)
            .timeout(self.config.audio_timeout);
        Ok(match self.config.audio_api_key.as_deref() {
            Some(key) => request.bearer_auth(key),
            None => request,
        })
    }
}

#[async_trait]
impl AudioApi for AudioClient {
    async fn transcribe(&self, media: &TempMedia, filename: &str) -> Result<String, AudioError> {
        let bytes = tokio::fs::read(media.path())
            .await
            .map_err(|_| AudioError::Io)?;
        let content_type = media
            .content_type()
            .split(';')
            .next()
            .unwrap_or("application/octet-stream")
            .trim();
        let part = Part::bytes(bytes)
            .file_name(filename.to_string())
            .mime_str(content_type)
            .map_err(|_| AudioError::InvalidMediaType)?;
        let form = Form::new()
            .text("model", self.config.asr_model.clone())
            .part("file", part);
        let response = self
            .request("transcriptions")?
            .multipart(form)
            .send()
            .await
            .map_err(|_| AudioError::Transport)?;
        let status = response.status();
        if !status.is_success() {
            return Err(AudioError::HttpStatus(status.as_u16()));
        }
        if response
            .content_length()
            .is_some_and(|length| length > TRANSCRIPT_BODY_CAP)
        {
            return Err(AudioError::ResponseTooLarge);
        }
        let body = response.bytes().await.map_err(|_| AudioError::Transport)?;
        if body.len() as u64 > TRANSCRIPT_BODY_CAP {
            return Err(AudioError::ResponseTooLarge);
        }
        let transcript: Transcript =
            serde_json::from_slice(&body).map_err(|_| AudioError::InvalidJson)?;
        let text = transcript.text.trim();
        if text.is_empty() {
            return Err(AudioError::EmptyTranscript);
        }
        Ok(text.to_string())
    }

    async fn synthesize(&self, text: &str) -> Result<TempMedia, AudioError> {
        if text.trim().is_empty() {
            return Err(AudioError::EmptyText);
        }
        let response = self
            .request("speech")?
            .json(&SpeechRequest {
                model: &self.config.tts_model,
                input: text,
                voice: &self.config.tts_voice,
            })
            .send()
            .await
            .map_err(|_| AudioError::Transport)?;
        let status = response.status();
        if !status.is_success() {
            return Err(AudioError::HttpStatus(status.as_u16()));
        }
        let media = response_body_to_temp(
            response,
            &self.config.media_temp_dir,
            self.config.max_outbound_file_bytes,
        )
        .await
        .map_err(AudioError::from)?;
        if media.is_empty() {
            return Err(AudioError::EmptyAudio);
        }
        Ok(media)
    }
}

#[derive(Deserialize)]
struct Transcript {
    text: String,
}

#[derive(Serialize)]
struct SpeechRequest<'a> {
    model: &'a str,
    input: &'a str,
    voice: &'a str,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AudioError {
    #[error("invalid audio provider URL")]
    InvalidUrl,
    #[error("invalid audio media type")]
    InvalidMediaType,
    #[error("audio provider request failed")]
    Transport,
    #[error("audio provider returned HTTP {0}")]
    HttpStatus(u16),
    #[error("audio provider returned invalid JSON")]
    InvalidJson,
    #[error("audio provider response exceeded its size limit")]
    ResponseTooLarge,
    #[error("audio provider returned an empty transcript")]
    EmptyTranscript,
    #[error("audio provider received empty text")]
    EmptyText,
    #[error("audio provider returned empty audio")]
    EmptyAudio,
    #[error("audio media I/O failed")]
    Io,
    #[error("audio media exceeded {limit} bytes")]
    MediaTooLarge { limit: u64 },
}

impl AudioError {
    pub fn status(&self) -> Option<u16> {
        match self {
            Self::HttpStatus(status) => Some(*status),
            _ => None,
        }
    }

    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Transport | Self::Io | Self::HttpStatus(429 | 500..=u16::MAX)
        )
    }
}

impl From<MediaError> for AudioError {
    fn from(value: MediaError) -> Self {
        match value {
            MediaError::TooLarge { limit } => Self::MediaTooLarge { limit },
            MediaError::Transport | MediaError::Timeout => Self::Transport,
            MediaError::HttpStatus(status) => Self::HttpStatus(status),
            MediaError::Io(_) | MediaError::Ffmpeg(_) => Self::Io,
        }
    }
}
