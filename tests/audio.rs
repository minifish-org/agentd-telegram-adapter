use std::sync::{Arc, Mutex};

use agentd_telegram_adapter::audio::{AudioApi, AudioClient, AudioError};
use agentd_telegram_adapter::media::TempMedia;
use agentd_telegram_adapter::Config;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::sync::oneshot;
use url::Url;

#[derive(Clone, Default)]
struct Captured {
    auth: Arc<Mutex<Vec<String>>>,
    transcriptions: Arc<Mutex<Vec<Vec<u8>>>>,
    speech: Arc<Mutex<Vec<Value>>>,
}

fn config(root: &std::path::Path, base: Url) -> Config {
    let mut config = Config::from_lookup(|key| match key {
        "BOT_TOKEN" => Some("bot-secret".to_string()),
        "WEBHOOK_SECRET" => Some("webhook-secret".to_string()),
        "AGENTD_URL" => Some("https://agentd.example".to_string()),
        "AUDIO_API_BASE" => Some("https://unused.example/v1".to_string()),
        "AUDIO_API_KEY" => Some("provider-secret".to_string()),
        _ => None,
    })
    .unwrap();
    config.audio_api_base = base;
    config.asr_model = "asr-test".to_string();
    config.tts_model = "tts-test".to_string();
    config.tts_voice = "voice-test".to_string();
    config.media_temp_dir = root.to_path_buf();
    config
}

fn input_media(root: &std::path::Path) -> TempMedia {
    let path = root.join("voice.ogg");
    std::fs::write(&path, b"voice-bytes").unwrap();
    TempMedia::from_existing(path, 11, "audio/ogg".to_string()).unwrap()
}

async fn provider(captured: Captured) -> (Url, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    async fn transcribe(
        State(captured): State<Captured>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Json<Value> {
        captured.auth.lock().unwrap().push(
            headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string(),
        );
        captured.transcriptions.lock().unwrap().push(body.to_vec());
        Json(json!({"text": "  spoken words  "}))
    }

    async fn speech(
        State(captured): State<Captured>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> (HeaderMap, &'static [u8]) {
        captured.auth.lock().unwrap().push(
            headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string(),
        );
        captured.speech.lock().unwrap().push(body);
        let mut response_headers = HeaderMap::new();
        response_headers.insert("content-type", "audio/wav".parse().unwrap());
        (response_headers, b"audio-bytes")
    }

    let app = Router::new()
        .route("/v1/audio/transcriptions", post(transcribe))
        .route("/v1/audio/speech", post(speech))
        .with_state(captured);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let (stop_tx, stop_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stop_rx.await;
            })
            .await
            .unwrap();
    });
    (
        Url::parse(&format!("http://{address}/v1")).unwrap(),
        stop_tx,
        server,
    )
}

#[tokio::test]
async fn audio_client_calls_openai_compatible_asr_and_tts_directly() {
    let temp = TempDir::new().unwrap();
    let captured = Captured::default();
    let (base, stop, server) = provider(captured.clone()).await;
    let client = AudioClient::new(reqwest::Client::new(), config(temp.path(), base));

    let transcript = client
        .transcribe(&input_media(temp.path()), "message.ogg")
        .await
        .unwrap();
    assert_eq!(transcript, "spoken words");
    let audio = client.synthesize("reply text").await.unwrap();
    assert_eq!(tokio::fs::read(audio.path()).await.unwrap(), b"audio-bytes");
    assert_eq!(audio.content_type(), "audio/wav");

    assert_eq!(
        captured.auth.lock().unwrap().as_slice(),
        ["Bearer provider-secret", "Bearer provider-secret"]
    );
    {
        let transcription_requests = captured.transcriptions.lock().unwrap();
        let multipart = String::from_utf8_lossy(&transcription_requests[0]);
        assert!(multipart.contains("name=\"model\""));
        assert!(multipart.contains("asr-test"));
        assert!(multipart.contains("filename=\"message.ogg\""));
        assert!(multipart.contains("voice-bytes"));
    }
    assert_eq!(
        captured.speech.lock().unwrap().as_slice(),
        &[json!({"model": "tts-test", "input": "reply text", "voice": "voice-test"})]
    );

    let _ = stop.send(());
    server.await.unwrap();
}

#[tokio::test]
async fn audio_client_bounds_speech_response_and_cleans_partial_file() {
    let temp = TempDir::new().unwrap();
    let app = Router::new().route(
        "/v1/audio/speech",
        post(|| async {
            (
                [(axum::http::header::CONTENT_TYPE, "audio/wav")],
                b"too-large" as &'static [u8],
            )
        }),
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut config = config(
        temp.path(),
        Url::parse(&format!("http://{address}/v1")).unwrap(),
    );
    config.max_outbound_file_bytes = 3;
    let error = AudioClient::new(reqwest::Client::new(), config)
        .synthesize("reply")
        .await
        .unwrap_err();

    assert_eq!(error, AudioError::MediaTooLarge { limit: 3 });
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
    server.abort();
}

#[test]
fn audio_provider_failures_have_explicit_retryability_without_response_bodies() {
    assert!(AudioError::HttpStatus(429).is_transient());
    assert!(AudioError::HttpStatus(503).is_transient());
    assert!(!AudioError::HttpStatus(400).is_transient());
    assert!(!AudioError::InvalidJson.is_transient());
    assert_eq!(AudioError::HttpStatus(503).status(), Some(503));
    assert_eq!(
        AudioError::HttpStatus(503).to_string(),
        "audio provider returned HTTP 503"
    );
    assert!(!format!("{:?}", AudioError::Transport).contains("provider-secret"));
}
