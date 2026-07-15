use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::fs::File;
use tokio_util::io::ReaderStream;
use url::Url;
use uuid::Uuid;

use crate::media::{response_body_to_temp, MediaError, TempMedia};
use crate::model::DeliveryOutboxRecord;
use crate::Config;

const WAIT_TRANSPORT_GRACE: Duration = Duration::from_secs(5);
// Control-plane responses are JSON metadata, never artifact/media bodies.
const CONTROL_RESPONSE_BODY_CAP: usize = 1_048_576;
const OVERSIZED_RESPONSE_BODY_NOTE: &str = "[response body omitted: exceeded 1048576 bytes]";

#[async_trait]
pub trait AgentdApi: Send + Sync {
    async fn submit_turn(&self, payload: SubmitTurn) -> Result<Value, AgentdError>;
    async fn wait_run(&self, run_id: Uuid, timeout_ms: u64) -> Result<Value, AgentdError>;
    async fn claim_delivery_outbox(
        &self,
        limit: usize,
    ) -> Result<Vec<DeliveryOutboxRecord>, AgentdError>;
    async fn ack_delivery(&self, ack: DeliveryAck) -> Result<Value, AgentdError>;
    async fn download_artifact_to_temp(
        &self,
        tenant: &str,
        path: &str,
    ) -> Result<TempMedia, AgentdError>;
    async fn upload_artifact_from_file(
        &self,
        tenant: &str,
        path: &str,
        media: &TempMedia,
    ) -> Result<Value, AgentdError>;
}

#[derive(Clone)]
pub struct AgentdClient {
    http: reqwest::Client,
    config: Config,
}

impl AgentdClient {
    pub fn new(http: reqwest::Client, config: Config) -> Self {
        Self { http, config }
    }

    fn request(
        &self,
        method: reqwest::Method,
        url: Url,
        timeout: std::time::Duration,
    ) -> reqwest::RequestBuilder {
        let request = self.http.request(method, url).timeout(timeout);
        match self.config.agentd_token.as_deref() {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }

    fn endpoint(&self, segments: &[&str]) -> Result<Url, AgentdError> {
        let mut url = self.config.agentd_url.clone();
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|_| AgentdError::InvalidUrl)?;
            path.pop_if_empty();
            path.extend(segments);
        }
        Ok(url)
    }

    fn raw_artifact_url(&self, tenant: &str, artifact_path: &str) -> Result<Url, AgentdError> {
        self.ensure_tenant(tenant)?;
        validate_relative_path(artifact_path)?;
        let mut url = self.config.agentd_url.clone();
        {
            let mut path = url
                .path_segments_mut()
                .map_err(|_| AgentdError::InvalidUrl)?;
            path.pop_if_empty();
            path.extend(["v1", "tenants", tenant, "artifacts"]);
            path.extend(artifact_path.split('/'));
        }
        Ok(url)
    }

    fn ensure_tenant(&self, tenant: &str) -> Result<(), AgentdError> {
        if tenant == self.config.tenant {
            Ok(())
        } else {
            Err(AgentdError::TenantMismatch)
        }
    }

    async fn json_response<T: serde::de::DeserializeOwned>(
        &self,
        response: reqwest::Response,
    ) -> Result<T, AgentdError> {
        let status = response.status();
        let bytes = bounded_control_response_body(response, status).await?;
        if !status.is_success() {
            return Err(AgentdError::HttpStatus {
                status: status.as_u16(),
                body: self.redact_agentd_token(capped_utf8(&bytes)),
            });
        }
        serde_json::from_slice(&bytes).map_err(|_| AgentdError::InvalidJson)
    }

    fn redact_agentd_token(&self, mut value: String) -> String {
        if let Some(token) = self.config.agentd_token.as_deref() {
            if !token.is_empty() {
                value = value.replace(token, "[redacted]");
            }
        }
        value
    }
}

#[async_trait]
impl AgentdApi for AgentdClient {
    async fn submit_turn(&self, payload: SubmitTurn) -> Result<Value, AgentdError> {
        self.ensure_tenant(&payload.tenant)?;
        let response = self
            .request(
                reqwest::Method::POST,
                self.endpoint(&["v1", "tenants", &payload.tenant, "turns"])?,
                self.config.submit_timeout,
            )
            .json(&payload)
            .send()
            .await
            .map_err(|_| AgentdError::Transport)?;
        self.json_response(response).await
    }

    async fn wait_run(&self, run_id: Uuid, timeout_ms: u64) -> Result<Value, AgentdError> {
        let mut url = self.endpoint(&[
            "v1",
            "tenants",
            &self.config.tenant,
            "runs",
            &run_id.to_string(),
            "wait",
        ])?;
        url.query_pairs_mut()
            .append_pair("timeout_ms", &timeout_ms.to_string());
        let response = self
            .request(
                reqwest::Method::GET,
                url,
                Duration::from_millis(timeout_ms).saturating_add(WAIT_TRANSPORT_GRACE),
            )
            .send()
            .await
            .map_err(|_| AgentdError::Transport)?;
        self.json_response(response).await
    }

    async fn claim_delivery_outbox(
        &self,
        limit: usize,
    ) -> Result<Vec<DeliveryOutboxRecord>, AgentdError> {
        #[derive(Serialize)]
        struct Claim {
            limit: usize,
        }
        #[derive(serde::Deserialize)]
        struct ClaimResponse {
            #[serde(default)]
            deliveries: Vec<DeliveryOutboxRecord>,
        }

        let response = self
            .request(
                reqwest::Method::POST,
                self.endpoint(&["v1", "tenants", &self.config.tenant, "deliveries", "claim"])?,
                self.config.submit_timeout,
            )
            .json(&Claim { limit })
            .send()
            .await
            .map_err(|_| AgentdError::Transport)?;
        Ok(self
            .json_response::<ClaimResponse>(response)
            .await?
            .deliveries)
    }

    async fn ack_delivery(&self, ack: DeliveryAck) -> Result<Value, AgentdError> {
        let url = self.endpoint(&[
            "v1",
            "tenants",
            &self.config.tenant,
            "deliveries",
            &ack.delivery_id.to_string(),
            "ack",
        ])?;
        let response = self
            .request(reqwest::Method::POST, url, self.config.submit_timeout)
            .json(&AckBody::from(ack))
            .send()
            .await
            .map_err(|_| AgentdError::Transport)?;
        self.json_response(response).await
    }

    async fn download_artifact_to_temp(
        &self,
        tenant: &str,
        path: &str,
    ) -> Result<TempMedia, AgentdError> {
        let response = self
            .request(
                reqwest::Method::GET,
                self.raw_artifact_url(tenant, path)?,
                self.config.media_timeout,
            )
            .send()
            .await
            .map_err(|_| AgentdError::Transport)?;
        let status = response.status();
        if !status.is_success() {
            let body = capped_response_body(response).await?;
            return Err(AgentdError::HttpStatus {
                status: status.as_u16(),
                body: self.redact_agentd_token(body),
            });
        }
        response_body_to_temp(
            response,
            &self.config.media_temp_dir,
            self.config.max_outbound_file_bytes,
        )
        .await
        .map_err(AgentdError::from)
    }

    async fn upload_artifact_from_file(
        &self,
        tenant: &str,
        path: &str,
        media: &TempMedia,
    ) -> Result<Value, AgentdError> {
        self.ensure_tenant(tenant)?;
        let file = File::open(media.path())
            .await
            .map_err(|_| AgentdError::Io("failed to open artifact upload file"))?;
        let len = file
            .metadata()
            .await
            .map_err(|_| AgentdError::Io("failed to stat artifact upload file"))?
            .len();
        if len > self.config.max_outbound_file_bytes {
            return Err(AgentdError::TooLarge {
                limit: self.config.max_outbound_file_bytes,
            });
        }
        let stream = ReaderStream::new(file);
        let body = reqwest::Body::wrap_stream(stream);
        let response = self
            .request(
                reqwest::Method::PUT,
                self.raw_artifact_url(tenant, path)?,
                self.config.media_timeout,
            )
            .header(CONTENT_TYPE, media.content_type())
            .header(CONTENT_LENGTH, len)
            .body(body)
            .send()
            .await
            .map_err(|_| AgentdError::Transport)?;
        self.json_response(response).await
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SubmitTurn {
    pub tenant: String,
    #[serde(rename = "agent")]
    pub agent_ref: String,
    pub scope: String,
    pub payload: Value,
    pub wait: bool,
}

#[derive(Debug, Clone)]
pub struct DeliveryAck {
    pub delivery_id: Uuid,
    pub claim_token: String,
    pub outcome: String,
    pub error: Option<String>,
    pub retry_after_ms: Option<u64>,
}

#[derive(Serialize)]
struct AckBody {
    claim_token: String,
    outcome: String,
    error: Option<String>,
    retry_after_ms: Option<u64>,
}

impl From<DeliveryAck> for AckBody {
    fn from(value: DeliveryAck) -> Self {
        Self {
            claim_token: value.claim_token,
            outcome: value.outcome,
            error: value.error,
            retry_after_ms: value.retry_after_ms,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum AgentdError {
    #[error("agentd tenant mismatch")]
    TenantMismatch,
    #[error("invalid agentd URL")]
    InvalidUrl,
    #[error("invalid relative path")]
    InvalidPath,
    #[error("agentd request failed")]
    Transport,
    #[error("agentd transfer exceeded {limit} bytes")]
    TooLarge { limit: u64 },
    #[error("{0}")]
    Io(&'static str),
    #[error("agentd returned HTTP {status}: {body}")]
    HttpStatus { status: u16, body: String },
    #[error("agentd returned invalid JSON")]
    InvalidJson,
}

impl From<MediaError> for AgentdError {
    fn from(value: MediaError) -> Self {
        match value {
            MediaError::TooLarge { limit } => Self::TooLarge { limit },
            MediaError::Transport => Self::Transport,
            MediaError::HttpStatus(status) => Self::HttpStatus {
                status,
                body: String::new(),
            },
            MediaError::Io(message) => Self::Io(message),
            MediaError::Timeout => Self::Transport,
            MediaError::Ffmpeg(_) => Self::Transport,
        }
    }
}

fn validate_relative_path(path: &str) -> Result<(), AgentdError> {
    if path.is_empty()
        || path.starts_with('/')
        || path
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(AgentdError::InvalidPath);
    }
    Ok(())
}

fn capped_utf8(bytes: &[u8]) -> String {
    String::from_utf8_lossy(&bytes[..bytes.len().min(512)]).into_owned()
}

async fn capped_response_body(response: reqwest::Response) -> Result<String, AgentdError> {
    let mut retained = Vec::with_capacity(512);
    let mut stream = response.bytes_stream();
    while retained.len() < 512 {
        let Some(chunk) = stream.next().await else {
            break;
        };
        let chunk = chunk.map_err(|_| AgentdError::Transport)?;
        let remaining = 512 - retained.len();
        retained.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }
    Ok(String::from_utf8_lossy(&retained).into_owned())
}

async fn bounded_control_response_body(
    response: reqwest::Response,
    status: reqwest::StatusCode,
) -> Result<Vec<u8>, AgentdError> {
    if response
        .content_length()
        .is_some_and(|len| len > CONTROL_RESPONSE_BODY_CAP as u64)
    {
        return Err(oversized_control_response(status));
    }

    let mut bytes = Vec::with_capacity(
        response
            .content_length()
            .unwrap_or_default()
            .min(CONTROL_RESPONSE_BODY_CAP as u64) as usize,
    );
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| AgentdError::Transport)?;
        if chunk.len() > CONTROL_RESPONSE_BODY_CAP.saturating_sub(bytes.len()) {
            return Err(oversized_control_response(status));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn oversized_control_response(status: reqwest::StatusCode) -> AgentdError {
    if status.is_success() {
        AgentdError::InvalidJson
    } else {
        AgentdError::HttpStatus {
            status: status.as_u16(),
            body: OVERSIZED_RESPONSE_BODY_NOTE.to_string(),
        }
    }
}
