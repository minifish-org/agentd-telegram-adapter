use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::header::{CONNECTION, CONTENT_TYPE};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use serde_json::{json, Value};
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{timeout, Instant};
use uuid::Uuid;

use crate::agentd::{AgentdApi, AgentdError, SubmitTurn};
use crate::audio::AudioApi;
use crate::media::{download_to_temp, TempMedia};
use crate::model::{TelegramChatId, TelegramFile, TelegramMessage, TelegramUpdate};
use crate::telegram::TelegramApi;
use crate::Config;

const WEBHOOK_BODY_CAP: usize = 1_048_576;
const TELEGRAM_HOSTED_DOWNLOAD_CAP: u64 = 20_971_520;
const CHAT_ACTION_REFRESH: Duration = Duration::from_secs(4);
const TRANSIENT_RETRY_DELAY: Duration = Duration::from_secs(2);
const TELEGRAM_SECRET_HEADER: &str = "x-telegram-bot-api-secret-token";

#[derive(Clone)]
pub struct WebhookState {
    config: Config,
    updates: mpsc::Sender<TelegramUpdate>,
}

impl WebhookState {
    pub fn new(config: Config, updates: mpsc::Sender<TelegramUpdate>) -> Self {
        Self { config, updates }
    }
}

#[derive(Debug, Error)]
pub enum InboundWorkerError {
    #[error("failed to persist voice reply marker for run {run_id}")]
    VoiceReplyMarker {
        run_id: Uuid,
        #[source]
        source: std::io::Error,
    },
}

pub fn spawn_inbound_worker(
    config: Config,
    http: reqwest::Client,
    agentd: Arc<dyn AgentdApi>,
    audio: Arc<dyn AudioApi>,
    telegram: Arc<dyn TelegramApi>,
) -> (
    mpsc::Sender<TelegramUpdate>,
    JoinHandle<Result<(), InboundWorkerError>>,
) {
    let (sender, receiver) = mpsc::channel(config.webhook_queue_capacity);
    let worker = tokio::spawn(run_inbound_worker(
        config, http, agentd, audio, telegram, receiver,
    ));
    (sender, worker)
}

pub fn router(state: WebhookState) -> Router {
    let webhook_path = state.config.webhook_path.clone();
    Router::new()
        .route(&webhook_path, post(admit_webhook).get(decoy_response))
        .fallback(decoy_or_not_found)
        .layer(DefaultBodyLimit::max(WEBHOOK_BODY_CAP))
        .with_state(state)
}

async fn admit_webhook(
    State(state): State<WebhookState>,
    headers: HeaderMap,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Response {
    let authorized = headers
        .get(TELEGRAM_SECRET_HEADER)
        .is_some_and(|provided| provided.as_bytes() == state.config.webhook_secret.as_bytes());
    if !authorized {
        return rejected(StatusCode::FORBIDDEN);
    }

    let body = match body {
        Ok(body) => body,
        Err(rejection) => return rejected(rejection.into_response().status()),
    };
    let value: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return rejected(StatusCode::BAD_REQUEST),
    };
    let update: TelegramUpdate = match serde_json::from_value(value) {
        Ok(update) => update,
        Err(_) => return rejected(StatusCode::BAD_REQUEST),
    };
    if update.update_id.is_none() {
        return rejected(StatusCode::BAD_REQUEST);
    }

    if !state.config.allowed_tg_users.is_empty()
        && !update
            .message
            .as_ref()
            .or(update.edited_message.as_ref())
            .and_then(|message| message.from.as_ref())
            .and_then(|user| user.id)
            .is_some_and(|user_id| state.config.allowed_tg_users.contains(&user_id))
    {
        return StatusCode::OK.into_response();
    }

    match state.updates.try_send(update) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(mpsc::error::TrySendError::Full(_) | mpsc::error::TrySendError::Closed(_)) => {
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

async fn decoy_or_not_found(State(state): State<WebhookState>, method: Method) -> Response {
    decoy(&state, method).await
}

async fn decoy_response(State(state): State<WebhookState>, method: Method) -> Response {
    decoy(&state, method).await
}

async fn decoy(state: &WebhookState, method: Method) -> Response {
    match method {
        Method::HEAD => match tokio::fs::metadata(&state.config.decoy_file).await {
            Ok(_) => (StatusCode::OK, [(CONTENT_TYPE, "text/html; charset=utf-8")]).into_response(),
            Err(_) => rejected(StatusCode::NOT_FOUND),
        },
        _ => match tokio::fs::read(&state.config.decoy_file).await {
            Ok(body) => (
                [(CONTENT_TYPE, "text/html; charset=utf-8")],
                Bytes::from(body),
            )
                .into_response(),
            Err(_) => rejected(StatusCode::NOT_FOUND),
        },
    }
}

fn rejected(status: StatusCode) -> Response {
    let mut response = status.into_response();
    response
        .headers_mut()
        .insert(CONNECTION, "close".parse().expect("valid static header"));
    response
}

pub async fn run_inbound_worker(
    config: Config,
    http: reqwest::Client,
    agentd: Arc<dyn AgentdApi>,
    audio: Arc<dyn AudioApi>,
    telegram: Arc<dyn TelegramApi>,
    mut receiver: mpsc::Receiver<TelegramUpdate>,
) -> Result<(), InboundWorkerError> {
    let mut watchers = JoinSet::new();
    while let Some(update) = receiver.recv().await {
        while watchers.try_join_next().is_some() {}
        let result = process_update(
            &config,
            &http,
            agentd.clone(),
            audio.clone(),
            telegram.clone(),
            update,
            &mut watchers,
        )
        .await;
        if let Err(error) = result {
            while watchers.join_next().await.is_some() {}
            return Err(error);
        }
    }
    while watchers.join_next().await.is_some() {}
    Ok(())
}

async fn process_update(
    config: &Config,
    http: &reqwest::Client,
    agentd: Arc<dyn AgentdApi>,
    audio: Arc<dyn AudioApi>,
    telegram: Arc<dyn TelegramApi>,
    update: TelegramUpdate,
    watchers: &mut JoinSet<()>,
) -> Result<(), InboundWorkerError> {
    let Some(update_id) = update.update_id else {
        return Ok(());
    };
    let Some(message) = update.message.or(update.edited_message) else {
        return Ok(());
    };
    let from_id = message.from.as_ref().and_then(|user| user.id);
    if !config.allowed_tg_users.is_empty()
        && !from_id.is_some_and(|user_id| config.allowed_tg_users.contains(&user_id))
    {
        return Ok(());
    }
    let Some(chat_id) = message.chat.as_ref().and_then(|chat| chat.id) else {
        return Ok(());
    };
    if !is_actionable(&message) {
        return Ok(());
    }

    let action = if has_voice(&message) {
        "record_voice"
    } else {
        "typing"
    };
    let chat_action = start_chat_action(
        telegram.clone(),
        chat_id,
        action,
        config.submit_timeout,
        config.typing_max,
    )
    .await;

    let (text, attachments, voice_reply) = collect_inbound(
        config,
        http,
        agentd.clone(),
        audio,
        telegram.clone(),
        update_id,
        &message,
    )
    .await;
    if text.is_empty() && attachments.is_empty() {
        chat_action.stop().await;
        return Ok(());
    }

    let mut payload = json!({"text": if text.is_empty() { "（用户发来内容）" } else { &text }});
    if !attachments.is_empty() {
        payload
            .as_object_mut()
            .expect("turn payload is an object")
            .insert("attachments".to_string(), Value::Array(attachments));
    }
    let submission = timeout(
        config.submit_timeout,
        agentd.submit_turn(SubmitTurn {
            tenant: config.tenant.clone(),
            agent_ref: config.agent_ref.clone(),
            scope: format!("tg:{chat_id}"),
            payload,
            wait: false,
        }),
    )
    .await;
    let run_id = match submission {
        Ok(Ok(response)) => response
            .get("run_id")
            .and_then(Value::as_str)
            .and_then(|run_id| Uuid::parse_str(run_id).ok()),
        Ok(Err(_)) | Err(_) => None,
    };
    let Some(run_id) = run_id else {
        chat_action.stop().await;
        return Ok(());
    };

    if voice_reply {
        if let Err(source) = persist_voice_reply_marker(&config.state_dir, run_id).await {
            chat_action.stop().await;
            return Err(InboundWorkerError::VoiceReplyMarker { run_id, source });
        }
    }

    let watcher_config = WatcherConfig {
        poll_window: config.typing_poll_window,
        max_duration: config.typing_max,
    };
    watchers.spawn(watch_run(agentd, run_id, watcher_config, chat_action));
    Ok(())
}

fn is_actionable(message: &TelegramMessage) -> bool {
    message.text.as_deref().is_some_and(|text| !text.is_empty())
        || message
            .caption
            .as_deref()
            .is_some_and(|caption| !caption.is_empty())
        || message.document.is_some()
        || message.voice.is_some()
        || message.audio.is_some()
        || !message.photo.is_empty()
        || message.location.is_some()
}

fn has_voice(message: &TelegramMessage) -> bool {
    message
        .voice
        .as_ref()
        .or(message.audio.as_ref())
        .and_then(|file| file.file_id.as_deref())
        .is_some()
}

async fn collect_inbound(
    config: &Config,
    http: &reqwest::Client,
    agentd: Arc<dyn AgentdApi>,
    audio: Arc<dyn AudioApi>,
    telegram: Arc<dyn TelegramApi>,
    update_id: i64,
    message: &TelegramMessage,
) -> (String, Vec<Value>, bool) {
    let mut text = message
        .text
        .clone()
        .or_else(|| message.caption.clone())
        .unwrap_or_default();
    let mut attachments = Vec::new();
    let mut voice_reply = false;

    if let Some(voice) = message.voice.as_ref().or(message.audio.as_ref()) {
        if voice.file_id.is_some() {
            let transcript = transcribe_voice(config, http, audio, telegram.clone(), voice).await;
            if let Some(transcript) = transcript {
                add_note(&mut text, &transcript);
                voice_reply = true;
            } else if text.is_empty() {
                add_note(&mut text, "（语音转写失败，请重试或改用文字）");
            }
        }
    }

    if let Some(location) = &message.location {
        if let (Some(latitude), Some(longitude)) = (location.latitude, location.longitude) {
            add_note(
                &mut text,
                &format!("（用户分享了位置：纬度 {latitude}，经度 {longitude}）"),
            );
        }
    }

    if let Some(document) = &message.document {
        if document.file_id.is_some() {
            if let Some(upload) =
                download_and_upload(config, http, agentd, telegram, update_id, document, "file")
                    .await
            {
                attachments.push(json!({
                    "artifact_ref": upload.artifact_ref,
                    "filename": upload.original_filename,
                    "content_type": document.mime_type,
                }));
            }
        }
    }

    if !message.photo.is_empty() {
        add_note(&mut text, "（用户发来一张图片）");
    }

    (text, attachments, voice_reply)
}

fn add_note(text: &mut String, note: &str) {
    if text.is_empty() {
        text.push_str(note);
    } else {
        text.push(' ');
        text.push_str(note);
        *text = text.trim().to_string();
    }
}

async fn transcribe_voice(
    config: &Config,
    http: &reqwest::Client,
    audio: Arc<dyn AudioApi>,
    telegram: Arc<dyn TelegramApi>,
    voice: &TelegramFile,
) -> Option<String> {
    let downloaded = download_inbound(config, http, telegram, voice, "voice.ogg").await?;
    audio
        .transcribe(&downloaded.media, &downloaded.original_filename)
        .await
        .ok()
}

struct DownloadedInbound {
    media: TempMedia,
    original_filename: String,
}

struct UploadedInbound {
    artifact_ref: String,
    original_filename: String,
}

#[allow(clippy::too_many_arguments)]
async fn download_and_upload(
    config: &Config,
    http: &reqwest::Client,
    agentd: Arc<dyn AgentdApi>,
    telegram: Arc<dyn TelegramApi>,
    update_id: i64,
    file: &TelegramFile,
    default_filename: &str,
) -> Option<UploadedInbound> {
    let downloaded = download_inbound(config, http, telegram, file, default_filename).await?;
    let artifact_path = format!(
        "inbound/telegram/{update_id}/{}",
        sanitize_filename(&downloaded.original_filename)
    );
    let response = timeout(
        config.media_timeout,
        agentd.upload_artifact_from_file(&config.tenant, &artifact_path, &downloaded.media),
    )
    .await
    .ok()?
    .ok()?;
    let artifact_ref = response.get("artifact_ref")?.as_str()?.to_string();
    Some(UploadedInbound {
        artifact_ref,
        original_filename: downloaded.original_filename,
    })
}

async fn download_inbound(
    config: &Config,
    http: &reqwest::Client,
    telegram: Arc<dyn TelegramApi>,
    file: &TelegramFile,
    default_filename: &str,
) -> Option<DownloadedInbound> {
    if file
        .file_size
        .is_some_and(|size| size > TELEGRAM_HOSTED_DOWNLOAD_CAP)
    {
        return None;
    }
    let file_id = file.file_id.as_deref()?;
    let file_info = timeout(config.submit_timeout, telegram.get_file(file_id))
        .await
        .ok()?
        .ok()?;
    if file_info
        .file_size
        .is_some_and(|size| size > TELEGRAM_HOSTED_DOWNLOAD_CAP)
    {
        return None;
    }
    let remote_path = file_info.file_path.as_deref()?;
    let url = telegram.file_url(remote_path).ok()?;
    let media = timeout(
        config.media_timeout,
        download_to_temp(
            http,
            url,
            &config.media_temp_dir,
            TELEGRAM_HOSTED_DOWNLOAD_CAP,
        ),
    )
    .await
    .ok()?
    .ok()?;
    let original_filename = file
        .file_name
        .clone()
        .or_else(|| remote_filename(remote_path))
        .unwrap_or_else(|| default_filename.to_string());
    Some(DownloadedInbound {
        media,
        original_filename,
    })
}

fn remote_filename(path: &str) -> Option<String> {
    path.rsplit('/')
        .find(|segment| !segment.is_empty())
        .map(ToOwned::to_owned)
}

fn sanitize_filename(filename: &str) -> String {
    let basename = filename
        .rsplit(['/', '\\'])
        .find(|segment| !segment.is_empty())
        .unwrap_or("file");
    let sanitized: String = basename
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect();
    let sanitized = sanitized.trim_matches('.');
    if sanitized.is_empty() {
        "file".to_string()
    } else {
        sanitized.to_string()
    }
}

struct ChatAction {
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl ChatAction {
    async fn stop(self) {
        let _ = self.stop.send(true);
        let _ = self.task.await;
    }
}

async fn start_chat_action(
    telegram: Arc<dyn TelegramApi>,
    chat_id: i64,
    action: &'static str,
    request_timeout: Duration,
    max_duration: Duration,
) -> ChatAction {
    let telegram_chat_id = TelegramChatId::Numeric(chat_id);
    let deadline = Instant::now() + max_duration;
    let first_timeout = request_timeout.min(max_duration);
    let _ = timeout(
        first_timeout,
        telegram.send_chat_action(&telegram_chat_id, action),
    )
    .await;
    let (stop, mut stopped) = watch::channel(false);
    let task = tokio::spawn(async move {
        loop {
            let now = Instant::now();
            if now >= deadline || *stopped.borrow() {
                break;
            }
            let sleep_for = CHAT_ACTION_REFRESH.min(deadline.saturating_duration_since(now));
            tokio::select! {
                _ = tokio::time::sleep(sleep_for) => {}
                changed = stopped.changed() => {
                    if changed.is_err() || *stopped.borrow() {
                        break;
                    }
                    continue;
                }
            }
            if Instant::now() >= deadline {
                break;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            let _ = timeout(
                request_timeout.min(remaining),
                telegram.send_chat_action(&telegram_chat_id, action),
            )
            .await;
        }
    });
    ChatAction { stop, task }
}

#[derive(Clone, Copy)]
struct WatcherConfig {
    poll_window: Duration,
    max_duration: Duration,
}

async fn watch_run(
    agentd: Arc<dyn AgentdApi>,
    run_id: Uuid,
    config: WatcherConfig,
    chat_action: ChatAction,
) {
    let deadline = Instant::now() + config.max_duration;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let poll_window = config.poll_window.min(remaining);
        let timeout_ms = poll_window.as_millis().clamp(1, u64::MAX as u128) as u64;
        match timeout(remaining, agentd.wait_run(run_id, timeout_ms)).await {
            Ok(Ok(response)) => {
                if response
                    .get("timed_out")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    continue;
                }
                break;
            }
            Ok(Err(AgentdError::HttpStatus { status, .. })) if (400..500).contains(&status) => {
                break;
            }
            Ok(Err(_)) | Err(_) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                tokio::time::sleep(TRANSIENT_RETRY_DELAY.min(remaining)).await;
            }
        }
    }
    chat_action.stop().await;
}

async fn persist_voice_reply_marker(state_dir: &Path, run_id: Uuid) -> std::io::Result<()> {
    let directory = state_dir.join("voice-replies");
    tokio::fs::create_dir_all(&directory).await?;
    let temporary = directory.join(format!(".{run_id}.{}.tmp", Uuid::new_v4().simple()));
    let final_path = directory.join(run_id.to_string());
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    let result = async {
        let mut file = options.open(&temporary).await?;
        file.write_all(b"voice\n").await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&temporary, &final_path).await?;
        #[cfg(unix)]
        {
            let directory_file = tokio::fs::File::open(&directory).await?;
            directory_file.sync_all().await?;
        }
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temporary).await;
    }
    result
}
