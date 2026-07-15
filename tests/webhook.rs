use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agentd_telegram_adapter::agentd::{AgentdApi, AgentdError, DeliveryAck, SubmitTurn};
use agentd_telegram_adapter::media::TempMedia;
use agentd_telegram_adapter::model::{DeliveryOutboxRecord, TelegramChatId, TelegramUpdate};
use agentd_telegram_adapter::telegram::{
    TelegramApi, TelegramError, TelegramFileInfo, TelegramMessageResult, UploadFile,
};
use agentd_telegram_adapter::webhook::{router, spawn_inbound_worker, WebhookState};
use agentd_telegram_adapter::Config;
use async_trait::async_trait;
use axum::body::{to_bytes, Body};
use axum::response::Response;
use axum::routing::get;
use http::{Method, Request, StatusCode};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::sync::mpsc;
use tower::ServiceExt;
use url::Url;
use uuid::Uuid;

const SECRET_HEADER: &str = "x-telegram-bot-api-secret-token";
const WEBHOOK_PATH: &str = "/tg/webhook";
const VALID_UPDATE: &str = r#"{
    "update_id": 71,
    "message": {
        "message_id": 9,
        "from": {"id": 42},
        "chat": {"id": 1001, "type": "private"},
        "text": "hello"
    }
}"#;

fn test_config(root: &Path, allowed_users: &str) -> Config {
    let decoy = root.join("index.html");
    std::fs::write(&decoy, b"decoy page").unwrap();
    let values = HashMap::from([
        ("BOT_TOKEN", "bot-secret".to_string()),
        ("WEBHOOK_SECRET", "webhook-secret".to_string()),
        ("DECOY_FILE", decoy.display().to_string()),
        ("STATE_DIR", root.display().to_string()),
        ("MEDIA_TEMP_DIR", root.join("media").display().to_string()),
        ("ALLOWED_TG_USERS", allowed_users.to_string()),
    ]);
    Config::from_lookup(|key| values.get(key).cloned()).unwrap()
}

fn webhook_request(body: impl Into<Body>) -> Request<Body> {
    Request::builder()
        .method(Method::POST)
        .uri(WEBHOOK_PATH)
        .header(SECRET_HEADER, "webhook-secret")
        .header("content-type", "application/json")
        .body(body.into())
        .unwrap()
}

#[tokio::test]
async fn admission_get_returns_decoy_and_head_omits_its_body() {
    let temp = TempDir::new().unwrap();
    let (sender, _receiver) = mpsc::channel(64);
    let app = router(WebhookState::new(test_config(temp.path(), "42"), sender));

    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/anything")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(to_bytes(get.into_body(), 128).await.unwrap(), "decoy page");

    let head = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::HEAD)
                .uri("/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    assert!(to_bytes(head.into_body(), 128).await.unwrap().is_empty());

    let exact_get = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(WEBHOOK_PATH)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(exact_get.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(exact_get.into_body(), 128).await.unwrap(),
        "decoy page"
    );

    let exact_head = app
        .oneshot(
            Request::builder()
                .method(Method::HEAD)
                .uri(WEBHOOK_PATH)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(exact_head.status(), StatusCode::OK);
    assert!(to_bytes(exact_head.into_body(), 128)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn admission_wrong_post_path_returns_decoy() {
    let temp = TempDir::new().unwrap();
    let (sender, _receiver) = mpsc::channel(64);
    let app = router(WebhookState::new(test_config(temp.path(), "42"), sender));
    let request = Request::builder()
        .method(Method::POST)
        .uri("/not-the-webhook")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), 128).await.unwrap(),
        "decoy page"
    );
}

#[tokio::test]
async fn admission_missing_or_wrong_secret_is_forbidden() {
    let temp = TempDir::new().unwrap();
    let (sender, mut receiver) = mpsc::channel(64);
    let app = router(WebhookState::new(test_config(temp.path(), "42"), sender));
    let missing = Request::builder()
        .method(Method::POST)
        .uri(WEBHOOK_PATH)
        .body(Body::from(VALID_UPDATE))
        .unwrap();
    let wrong = Request::builder()
        .method(Method::POST)
        .uri(WEBHOOK_PATH)
        .header(SECRET_HEADER, "wrong")
        .body(Body::from(VALID_UPDATE))
        .unwrap();

    assert_eq!(
        app.clone().oneshot(missing).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        app.oneshot(wrong).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn admission_malformed_json_is_bad_request() {
    let temp = TempDir::new().unwrap();
    let (sender, mut receiver) = mpsc::channel(64);
    let app = router(WebhookState::new(test_config(temp.path(), "42"), sender));

    let response = app.oneshot(webhook_request("{")).await.unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn admission_unauthorized_user_is_silently_accepted_without_queueing() {
    let temp = TempDir::new().unwrap();
    let (sender, mut receiver) = mpsc::channel(64);
    let app = router(WebhookState::new(test_config(temp.path(), "7"), sender));

    let response = app.oneshot(webhook_request(VALID_UPDATE)).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn admission_valid_update_is_accepted_and_queued() {
    let temp = TempDir::new().unwrap();
    let (sender, mut receiver) = mpsc::channel(64);
    let app = router(WebhookState::new(test_config(temp.path(), "42"), sender));

    let response = app.oneshot(webhook_request(VALID_UPDATE)).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(receiver.recv().await.unwrap().update_id, Some(71));
}

#[tokio::test]
async fn admission_text_without_update_id_is_bad_request() {
    let temp = TempDir::new().unwrap();
    let (sender, mut receiver) = mpsc::channel(64);
    let app = router(WebhookState::new(test_config(temp.path(), "42"), sender));
    let mut update = message_update(71, json!({"text": "hello"}));
    update.as_object_mut().unwrap().remove("update_id");

    let response = app
        .oneshot(webhook_request(serde_json::to_string(&update).unwrap()))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn admission_media_with_null_update_id_is_bad_request() {
    let temp = TempDir::new().unwrap();
    let (sender, mut receiver) = mpsc::channel(64);
    let app = router(WebhookState::new(test_config(temp.path(), "42"), sender));
    let mut update = message_update(
        72,
        json!({"document": {"file_id": "document-id", "file_name": "report.txt"}}),
    );
    update["update_id"] = Value::Null;

    let response = app
        .oneshot(webhook_request(serde_json::to_string(&update).unwrap()))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn admission_duplicate_json_fields_use_value_parsing_and_unknown_fields_are_accepted() {
    let temp = TempDir::new().unwrap();
    let (sender, mut receiver) = mpsc::channel(64);
    let app = router(WebhookState::new(test_config(temp.path(), "42"), sender));
    let body = VALID_UPDATE.replace(
        "\"update_id\": 71,",
        "\"update_id\": 70, \"update_id\": 71, \"future\": true,",
    );

    let response = app.oneshot(webhook_request(body)).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let update = receiver.recv().await.unwrap();
    assert_eq!(update.update_id, Some(71));
    assert_eq!(
        update.extra.get("future").and_then(|value| value.as_bool()),
        Some(true)
    );
}

#[tokio::test]
async fn admission_body_over_exact_cap_is_payload_too_large() {
    let temp = TempDir::new().unwrap();
    let (sender, mut receiver) = mpsc::channel(64);
    let app = router(WebhookState::new(test_config(temp.path(), "42"), sender));

    let response = app
        .oneshot(webhook_request(vec![b' '; 1_048_577]))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn admission_full_or_closed_queue_is_service_unavailable() {
    let temp = TempDir::new().unwrap();
    let (full_sender, _full_receiver) = mpsc::channel(1);
    full_sender
        .try_send(serde_json::from_str::<TelegramUpdate>(VALID_UPDATE).unwrap())
        .unwrap();
    let full_app = router(WebhookState::new(
        test_config(temp.path(), "42"),
        full_sender,
    ));
    assert_eq!(
        full_app
            .oneshot(webhook_request(VALID_UPDATE))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );

    let (closed_sender, closed_receiver) = mpsc::channel(1);
    drop(closed_receiver);
    let closed_app = router(WebhookState::new(
        test_config(temp.path(), "42"),
        closed_sender,
    ));
    assert_eq!(
        closed_app
            .oneshot(webhook_request(VALID_UPDATE))
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn admission_oversized_unread_body_cannot_become_pipelined_request() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let temp = TempDir::new().unwrap();
    let (sender, mut receiver) = mpsc::channel(64);
    let app = router(WebhookState::new(test_config(temp.path(), "42"), sender));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    let second = format!(
        "POST {WEBHOOK_PATH} HTTP/1.1\r\nHost: {address}\r\n{SECRET_HEADER}: webhook-secret\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{VALID_UPDATE}",
        VALID_UPDATE.len()
    );
    let first = format!(
        "POST {WEBHOOK_PATH} HTTP/1.1\r\nHost: {address}\r\n{SECRET_HEADER}: webhook-secret\r\nContent-Type: application/json\r\nContent-Length: 1048577\r\n\r\n"
    );
    let mut pipelined = first.into_bytes();
    pipelined.resize(pipelined.len() + 1_048_577, b'x');
    pipelined.extend_from_slice(second.as_bytes());

    stream.write_all(&pipelined).await.unwrap();
    stream.shutdown().await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    server.abort();

    let response = String::from_utf8_lossy(&response).to_ascii_lowercase();
    assert_eq!(response.matches("http/1.1").count(), 1, "{response}");
    assert!(response.starts_with("http/1.1 413"), "{response}");
    assert!(response.contains("connection: close"), "{response}");
    assert!(receiver.try_recv().is_err());
}

#[derive(Debug, Clone)]
struct UploadedArtifact {
    path: String,
    content_type: String,
    body: Vec<u8>,
}

struct FakeAgentd {
    run_id: Uuid,
    submits: Mutex<Vec<SubmitTurn>>,
    uploads: Mutex<Vec<UploadedArtifact>>,
    tool_calls: Mutex<Vec<(String, Value)>>,
    transcript: Mutex<Option<String>>,
    submit_fails: AtomicBool,
    wait_calls: AtomicUsize,
    wait_times_out: AtomicBool,
    wait_delay: Mutex<Duration>,
    events: Arc<Mutex<Vec<String>>>,
}

impl FakeAgentd {
    fn new(events: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            run_id: Uuid::new_v4(),
            submits: Mutex::new(Vec::new()),
            uploads: Mutex::new(Vec::new()),
            tool_calls: Mutex::new(Vec::new()),
            transcript: Mutex::new(None),
            submit_fails: AtomicBool::new(false),
            wait_calls: AtomicUsize::new(0),
            wait_times_out: AtomicBool::new(false),
            wait_delay: Mutex::new(Duration::ZERO),
            events,
        }
    }
}

#[async_trait]
impl AgentdApi for FakeAgentd {
    async fn submit_turn(&self, payload: SubmitTurn) -> Result<Value, AgentdError> {
        self.events.lock().unwrap().push("submit".to_string());
        if self.submit_fails.load(Ordering::SeqCst) {
            return Err(AgentdError::Transport);
        }
        self.submits.lock().unwrap().push(payload);
        Ok(json!({"run_id": self.run_id}))
    }

    async fn wait_run(&self, _run_id: Uuid, _timeout_ms: u64) -> Result<Value, AgentdError> {
        self.wait_calls.fetch_add(1, Ordering::SeqCst);
        let delay = *self.wait_delay.lock().unwrap();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        Ok(json!({
            "timed_out": self.wait_times_out.load(Ordering::SeqCst),
            "run": {"final_decision": {"reply": "completed reply"}}
        }))
    }

    async fn call_tool(&self, tool: &str, arguments: Value) -> Result<Value, AgentdError> {
        self.events.lock().unwrap().push(format!("tool:{tool}"));
        self.tool_calls
            .lock()
            .unwrap()
            .push((tool.to_string(), arguments));
        Ok(self
            .transcript
            .lock()
            .unwrap()
            .clone()
            .map_or_else(|| json!({}), |text| json!({"text": text})))
    }

    async fn claim_delivery_outbox(
        &self,
        _limit: usize,
    ) -> Result<Vec<DeliveryOutboxRecord>, AgentdError> {
        Ok(Vec::new())
    }

    async fn ack_delivery(&self, _ack: DeliveryAck) -> Result<Value, AgentdError> {
        Ok(json!({}))
    }

    async fn download_artifact_to_temp(
        &self,
        _tenant: &str,
        _path: &str,
    ) -> Result<TempMedia, AgentdError> {
        Err(AgentdError::Transport)
    }

    async fn upload_artifact_from_file(
        &self,
        tenant: &str,
        path: &str,
        media: &TempMedia,
    ) -> Result<Value, AgentdError> {
        self.events.lock().unwrap().push("upload".to_string());
        let body = tokio::fs::read(media.path())
            .await
            .map_err(|_| AgentdError::Io("fake upload read failed"))?;
        self.uploads.lock().unwrap().push(UploadedArtifact {
            path: path.to_string(),
            content_type: media.content_type().to_string(),
            body,
        });
        Ok(json!({"artifact_ref": format!("artifact://{tenant}/{path}")}))
    }
}

struct FakeTelegram {
    base_url: Url,
    files: Mutex<HashMap<String, TelegramFileInfo>>,
    actions: Mutex<Vec<(i64, String)>>,
    events: Arc<Mutex<Vec<String>>>,
}

impl FakeTelegram {
    fn new(base_url: Url, events: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            base_url,
            files: Mutex::new(HashMap::new()),
            actions: Mutex::new(Vec::new()),
            events,
        }
    }

    fn add_file(&self, file_id: &str, file_path: &str, file_size: Option<u64>) {
        self.files.lock().unwrap().insert(
            file_id.to_string(),
            TelegramFileInfo {
                file_id: Some(file_id.to_string()),
                file_unique_id: None,
                file_size,
                file_path: Some(file_path.to_string()),
            },
        );
    }
}

#[async_trait]
impl TelegramApi for FakeTelegram {
    async fn send_message(
        &self,
        _chat_id: &TelegramChatId,
        _text: &str,
        _parse_mode: Option<&str>,
    ) -> Result<TelegramMessageResult, TelegramError> {
        Ok(TelegramMessageResult {
            message_id: Some(1),
        })
    }

    async fn send_chat_action(
        &self,
        chat_id: &TelegramChatId,
        action: &str,
    ) -> Result<(), TelegramError> {
        let TelegramChatId::Numeric(chat_id) = chat_id else {
            panic!("inbound Telegram updates use numeric chat ids");
        };
        self.events.lock().unwrap().push(format!("action:{action}"));
        self.actions
            .lock()
            .unwrap()
            .push((*chat_id, action.to_string()));
        Ok(())
    }

    async fn send_location(
        &self,
        _chat_id: &TelegramChatId,
        _latitude: f64,
        _longitude: f64,
    ) -> Result<TelegramMessageResult, TelegramError> {
        Ok(TelegramMessageResult {
            message_id: Some(1),
        })
    }

    async fn send_file(
        &self,
        _method: &str,
        _field: &str,
        _chat_id: &TelegramChatId,
        _file: UploadFile,
        _caption: Option<&str>,
    ) -> Result<TelegramMessageResult, TelegramError> {
        Ok(TelegramMessageResult {
            message_id: Some(1),
        })
    }

    async fn get_file(&self, file_id: &str) -> Result<TelegramFileInfo, TelegramError> {
        Ok(self.files.lock().unwrap().get(file_id).cloned().unwrap())
    }

    fn file_url(&self, file_path: &str) -> Result<Url, TelegramError> {
        Ok(self.base_url.join(file_path).unwrap())
    }
}

async fn media_server() -> (Url, tokio::task::JoinHandle<()>) {
    let app = axum::Router::new()
        .route(
            "/files/report.bin",
            get(|| async {
                (
                    [(&http::header::CONTENT_TYPE, "application/octet-stream")],
                    "report",
                )
            }),
        )
        .route(
            "/files/voice.ogg",
            get(|| async { ([(&http::header::CONTENT_TYPE, "audio/ogg")], "voice") }),
        )
        .route(
            "/files/oversized.bin",
            get(|| async {
                Response::builder()
                    .status(StatusCode::OK)
                    .header(http::header::CONTENT_LENGTH, 20_971_521u64)
                    .body(Body::empty())
                    .unwrap()
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (Url::parse(&format!("http://{address}/")).unwrap(), server)
}

async fn run_inbound(
    config: Config,
    agentd: Arc<FakeAgentd>,
    telegram: Arc<FakeTelegram>,
    updates: Vec<Value>,
) {
    let (sender, worker) = spawn_inbound_worker(config, reqwest::Client::new(), agentd, telegram);
    for update in updates {
        sender
            .send(serde_json::from_value(update).unwrap())
            .await
            .unwrap();
    }
    drop(sender);
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .expect("inbound worker stayed alive")
        .expect("inbound worker panicked")
        .expect("inbound worker failed");
}

fn message_update(update_id: i64, message: Value) -> Value {
    json!({
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "from": {"id": 42},
            "chat": {"id": 1001, "type": "private"},
            "date": 1,
            "extra_message_field": true,
            "text": null,
            "caption": null,
            "document": null,
            "voice": null,
            "audio": null,
            "photo": [],
            "location": null
        }
    })
    .as_object()
    .map(|update| {
        let mut update = update.clone();
        update
            .get_mut("message")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .extend(message.as_object().unwrap().clone());
        Value::Object(update)
    })
    .unwrap()
}

#[tokio::test]
async fn inbound_text_and_caption_use_the_stable_turn_shape() {
    let temp = TempDir::new().unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let agentd = Arc::new(FakeAgentd::new(events.clone()));
    let (base_url, server) = media_server().await;
    let telegram = Arc::new(FakeTelegram::new(base_url, events));
    let config = test_config(temp.path(), "42");

    run_inbound(
        config,
        agentd.clone(),
        telegram.clone(),
        vec![
            message_update(101, json!({"text": "hello"})),
            message_update(102, json!({"caption": "caption only"})),
        ],
    )
    .await;
    server.abort();

    let submits = agentd.submits.lock().unwrap();
    assert_eq!(submits.len(), 2);
    assert_eq!(submits[0].tenant, "demo");
    assert_eq!(submits[0].agent_ref, "simple-bot");
    assert_eq!(submits[0].scope, "tg:1001");
    assert!(!submits[0].wait);
    assert_eq!(submits[0].payload, json!({"text": "hello"}));
    assert_eq!(submits[1].payload, json!({"text": "caption only"}));
    assert!(telegram
        .actions
        .lock()
        .unwrap()
        .iter()
        .all(|(_, action)| action == "typing"));
}

#[tokio::test]
async fn inbound_document_streams_to_sanitized_raw_artifact_path() {
    let temp = TempDir::new().unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let agentd = Arc::new(FakeAgentd::new(events.clone()));
    let (base_url, server) = media_server().await;
    let telegram = Arc::new(FakeTelegram::new(base_url, events.clone()));
    telegram.add_file("document-id", "files/report.bin", Some(6));

    run_inbound(
        test_config(temp.path(), "42"),
        agentd.clone(),
        telegram,
        vec![message_update(
            103,
            json!({"document": {
                "file_id": "document-id",
                "file_name": "../../quarter report?.txt",
                "mime_type": "text/plain"
            }}),
        )],
    )
    .await;
    server.abort();

    let uploads = agentd.uploads.lock().unwrap();
    assert_eq!(uploads.len(), 1);
    assert_eq!(uploads[0].path, "inbound/telegram/103/quarter_report_.txt");
    assert_eq!(uploads[0].content_type, "application/octet-stream");
    assert_eq!(uploads[0].body, b"report");
    let submits = agentd.submits.lock().unwrap();
    assert_eq!(
        submits[0].payload,
        json!({
            "text": "（用户发来内容）",
            "attachments": [{
                "artifact_ref": "artifact://demo/inbound/telegram/103/quarter_report_.txt",
                "filename": "../../quarter report?.txt",
                "content_type": "text/plain"
            }]
        })
    );
    assert_eq!(events.lock().unwrap()[0], "action:typing");
}

#[tokio::test]
async fn inbound_voice_uploads_transcribes_sets_record_voice_and_atomically_marks_run() {
    let temp = TempDir::new().unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let agentd = Arc::new(FakeAgentd::new(events.clone()));
    *agentd.transcript.lock().unwrap() = Some(" spoken words ".to_string());
    let (base_url, server) = media_server().await;
    let telegram = Arc::new(FakeTelegram::new(base_url, events.clone()));
    telegram.add_file("voice-id", "files/voice.ogg", Some(5));

    run_inbound(
        test_config(temp.path(), "42"),
        agentd.clone(),
        telegram.clone(),
        vec![message_update(
            104,
            json!({"voice": {"file_id": "voice-id", "mime_type": "audio/ogg"}}),
        )],
    )
    .await;
    server.abort();

    assert_eq!(
        agentd.tool_calls.lock().unwrap().as_slice(),
        &[(
            "audio_transcribe".to_string(),
            json!({"artifact_ref": "artifact://demo/inbound/telegram/104/voice.ogg"})
        )]
    );
    assert_eq!(
        agentd.submits.lock().unwrap()[0].payload,
        json!({"text": "spoken words"})
    );
    assert!(telegram
        .actions
        .lock()
        .unwrap()
        .iter()
        .all(|(_, action)| action == "record_voice"));
    let marker = temp
        .path()
        .join("voice-replies")
        .join(agentd.run_id.to_string());
    assert!(marker.is_file());
    assert_eq!(
        std::fs::read_dir(temp.path().join("voice-replies"))
            .unwrap()
            .count(),
        1
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(marker).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    assert_eq!(
        events.lock().unwrap()[..4],
        [
            "action:record_voice",
            "upload",
            "tool:audio_transcribe",
            "submit"
        ]
    );
}

#[tokio::test]
async fn inbound_failed_voice_transcription_does_not_set_voice_reply_intent() {
    let temp = TempDir::new().unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let agentd = Arc::new(FakeAgentd::new(events.clone()));
    *agentd.transcript.lock().unwrap() = Some("   ".to_string());
    let (base_url, server) = media_server().await;
    let telegram = Arc::new(FakeTelegram::new(base_url, events));
    telegram.add_file("voice-id", "files/voice.ogg", Some(5));

    run_inbound(
        test_config(temp.path(), "42"),
        agentd.clone(),
        telegram,
        vec![message_update(
            105,
            json!({"audio": {"file_id": "voice-id", "mime_type": "audio/ogg"}}),
        )],
    )
    .await;
    server.abort();

    assert_eq!(
        agentd.submits.lock().unwrap()[0].payload,
        json!({"text": "（语音转写失败，请重试或改用文字）"})
    );
    assert!(!temp
        .path()
        .join("voice-replies")
        .join(agentd.run_id.to_string())
        .exists());
}

#[tokio::test]
async fn inbound_photo_and_location_append_transport_neutral_notes() {
    let temp = TempDir::new().unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let agentd = Arc::new(FakeAgentd::new(events.clone()));
    let (base_url, server) = media_server().await;
    let telegram = Arc::new(FakeTelegram::new(base_url, events));

    run_inbound(
        test_config(temp.path(), "42"),
        agentd.clone(),
        telegram,
        vec![message_update(
            106,
            json!({
                "text": "here",
                "photo": [{"file_id": "photo-id", "width": 1, "height": 1}],
                "location": {"latitude": 1.25, "longitude": 103.5}
            }),
        )],
    )
    .await;
    server.abort();

    assert_eq!(
        agentd.submits.lock().unwrap()[0].payload,
        json!({"text": "here （用户分享了位置：纬度 1.25，经度 103.5） （用户发来一张图片）"})
    );
}

#[tokio::test]
async fn inbound_submission_failure_stops_action_and_creates_no_marker() {
    let temp = TempDir::new().unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let agentd = Arc::new(FakeAgentd::new(events.clone()));
    agentd.submit_fails.store(true, Ordering::SeqCst);
    *agentd.transcript.lock().unwrap() = Some("voice text".to_string());
    let (base_url, server) = media_server().await;
    let telegram = Arc::new(FakeTelegram::new(base_url, events));
    telegram.add_file("voice-id", "files/voice.ogg", Some(5));

    run_inbound(
        test_config(temp.path(), "42"),
        agentd.clone(),
        telegram.clone(),
        vec![message_update(
            107,
            json!({"voice": {"file_id": "voice-id", "mime_type": "audio/ogg"}}),
        )],
    )
    .await;
    server.abort();

    assert!(!temp
        .path()
        .join("voice-replies")
        .join(agentd.run_id.to_string())
        .exists());
    let action_count = telegram.actions.lock().unwrap().len();
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(telegram.actions.lock().unwrap().len(), action_count);
}

#[tokio::test]
async fn inbound_voice_marker_failure_is_observable_from_worker() {
    let temp = TempDir::new().unwrap();
    let state_file = temp.path().join("state-is-a-file");
    std::fs::write(&state_file, b"not a directory").unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let agentd = Arc::new(FakeAgentd::new(events.clone()));
    *agentd.transcript.lock().unwrap() = Some("voice text".to_string());
    let (base_url, server) = media_server().await;
    let telegram = Arc::new(FakeTelegram::new(base_url, events));
    telegram.add_file("voice-id", "files/voice.ogg", Some(5));
    let mut config = test_config(temp.path(), "42");
    config.state_dir = state_file;
    let (sender, worker) = spawn_inbound_worker(config, reqwest::Client::new(), agentd, telegram);
    sender
        .send(
            serde_json::from_value(message_update(
                111,
                json!({"voice": {"file_id": "voice-id", "mime_type": "audio/ogg"}}),
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    drop(sender);

    let outcome = tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .expect("inbound worker stayed alive")
        .expect("inbound worker panicked");
    server.abort();

    assert!(outcome.is_err(), "marker failure was reported as success");
}

#[tokio::test]
async fn inbound_hosted_download_over_exact_cap_is_never_uploaded() {
    let temp = TempDir::new().unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let agentd = Arc::new(FakeAgentd::new(events.clone()));
    let (base_url, server) = media_server().await;
    let telegram = Arc::new(FakeTelegram::new(base_url, events));
    telegram.add_file("large-id", "files/oversized.bin", Some(20_971_521));

    run_inbound(
        test_config(temp.path(), "42"),
        agentd.clone(),
        telegram,
        vec![message_update(
            108,
            json!({"document": {"file_id": "large-id", "file_name": "large.bin"}}),
        )],
    )
    .await;
    server.abort();

    assert!(agentd.uploads.lock().unwrap().is_empty());
    assert!(agentd.submits.lock().unwrap().is_empty());
}

#[tokio::test]
async fn inbound_run_polling_and_chat_action_are_bounded_by_config() {
    let temp = TempDir::new().unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let agentd = Arc::new(FakeAgentd::new(events.clone()));
    agentd.wait_times_out.store(true, Ordering::SeqCst);
    *agentd.wait_delay.lock().unwrap() = Duration::from_millis(5);
    let (base_url, server) = media_server().await;
    let telegram = Arc::new(FakeTelegram::new(base_url, events));
    let mut config = test_config(temp.path(), "42");
    config.typing_max = Duration::from_millis(40);
    config.typing_poll_window = Duration::from_millis(10);

    run_inbound(
        config,
        agentd.clone(),
        telegram,
        vec![message_update(109, json!({"text": "bounded"}))],
    )
    .await;
    server.abort();

    let calls = agentd.wait_calls.load(Ordering::SeqCst);
    assert!((1..=10).contains(&calls), "wait calls: {calls}");
}
