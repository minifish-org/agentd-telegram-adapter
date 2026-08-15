use std::collections::VecDeque;
use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use agentd_telegram_adapter::agentd::{
    AgentdApi, AgentdClient, AgentdError, DeliveryAck, DeliveryRequest, SubmitTurn,
};
use agentd_telegram_adapter::media::{
    download_to_temp, prepare_voice_for_upload, transcode_to_ogg_opus,
    transcode_to_ogg_opus_with_timeout, MediaError, TempMedia, FFMPEG_TIMEOUT,
};
use agentd_telegram_adapter::model::TelegramChatId;
use agentd_telegram_adapter::telegram::{ErrorCategory, TelegramApi, TelegramClient, UploadFile};
use agentd_telegram_adapter::Config;
use axum::body::{to_bytes, Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::{Json, Router};
use serde_json::{json, Value};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};
use url::Url;
use uuid::Uuid;

#[tokio::test]
async fn client_agentd_uploads_artifact_from_file_with_auth_metadata_and_nested_path() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let base = capture_server(
        captured.clone(),
        vec![json!({
            "tenant": "demo",
            "path": "inbound/telegram/7/nested voice.ogg",
            "sha256": "abc"
        })],
    )
    .await;

    let client = AgentdClient::new(
        reqwest::Client::new(),
        test_config(|config| {
            config.agentd_url = base;
            config.agentd_token = Some("agent-secret".to_string());
            config.tenant = "demo".to_string();
            config.max_inbound_file_bytes = 12;
        }),
    );
    let dir = tempfile::tempdir().unwrap();
    let media = write_temp_media(
        dir.path(),
        "upload-source.ogg",
        "audio/ogg",
        b"\x00\x01not-base64",
    )
    .await;

    client
        .upload_artifact_from_file("demo", "inbound/telegram/7/nested voice.ogg", &media)
        .await
        .unwrap();

    let requests = captured.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, Method::PUT);
    assert_eq!(
        requests[0].path_and_query,
        "/v1/tenants/demo/artifacts/inbound/telegram/7/nested%20voice.ogg"
    );
    assert_eq!(
        requests[0]
            .headers
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer agent-secret")
    );
    assert_eq!(
        requests[0]
            .headers
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("audio/ogg")
    );
    assert_eq!(
        requests[0]
            .headers
            .get("content-length")
            .and_then(|value| value.to_str().ok()),
        Some("12")
    );
    assert_eq!(requests[0].body.as_ref(), b"\x00\x01not-base64");
}

#[tokio::test]
async fn client_agentd_downloads_artifact_to_temp_with_auth_nested_path_exact_limit_and_cleanup() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let base = capture_raw_server(
        captured.clone(),
        StatusCode::OK,
        b"hello".to_vec(),
        Some("text/plain"),
        Some(5),
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let client = AgentdClient::new(
        reqwest::Client::new(),
        test_config(|config| {
            config.agentd_url = base;
            config.agentd_token = Some("agent-secret".to_string());
            config.tenant = "demo".to_string();
            config.max_outbound_file_bytes = 5;
            config.media_temp_dir = dir.path().to_path_buf();
        }),
    );

    let media = client
        .download_artifact_to_temp("demo", "outbound/replies/nested file.txt")
        .await
        .unwrap();
    let path = media.path().to_path_buf();
    let mut body = Vec::new();
    tokio::fs::File::open(media.path())
        .await
        .unwrap()
        .read_to_end(&mut body)
        .await
        .unwrap();

    assert_eq!(media.len(), 5);
    assert_eq!(media.content_type(), "text/plain");
    assert_eq!(body, b"hello");
    let requests = captured.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, Method::GET);
    assert_eq!(
        requests[0].path_and_query,
        "/v1/tenants/demo/artifacts/outbound/replies/nested%20file.txt"
    );
    assert_eq!(
        requests[0]
            .headers
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer agent-secret")
    );
    drop(media);
    assert!(!path.exists());
}

#[tokio::test]
async fn client_agentd_artifact_streaming_allows_exact_limit_and_rejects_plus_one() {
    const RAW_ARTIFACT_LIMIT: u64 = 52_428_800;

    let uploaded_lengths = Arc::new(Mutex::new(Vec::new()));
    let upload_base = counting_upload_server(uploaded_lengths.clone()).await;
    let client = AgentdClient::new(
        reqwest::Client::new(),
        test_config(|config| {
            config.agentd_url = upload_base;
            config.tenant = "demo".to_string();
            config.max_inbound_file_bytes = 20_971_520;
            config.max_outbound_file_bytes = RAW_ARTIFACT_LIMIT;
        }),
    );
    let dir = tempfile::tempdir().unwrap();
    let exact_path = dir.path().join("exact.bin");
    tokio::fs::File::create(&exact_path)
        .await
        .unwrap()
        .set_len(RAW_ARTIFACT_LIMIT)
        .await
        .unwrap();
    let exact = TempMedia::from_existing(
        exact_path,
        RAW_ARTIFACT_LIMIT,
        "application/octet-stream".to_string(),
    )
    .unwrap();
    client
        .upload_artifact_from_file("demo", "uploads/exact.bin", &exact)
        .await
        .unwrap();

    let too_large_path = dir.path().join("too-large.bin");
    tokio::fs::File::create(&too_large_path)
        .await
        .unwrap()
        .set_len(RAW_ARTIFACT_LIMIT + 1)
        .await
        .unwrap();
    let too_large = TempMedia::from_existing(
        too_large_path,
        RAW_ARTIFACT_LIMIT + 1,
        "application/octet-stream".to_string(),
    )
    .unwrap();
    let error = client
        .upload_artifact_from_file("demo", "uploads/too-large.bin", &too_large)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        agentd_telegram_adapter::agentd::AgentdError::TooLarge {
            limit: RAW_ARTIFACT_LIMIT
        }
    ));
    assert_eq!(*uploaded_lengths.lock().await, vec![RAW_ARTIFACT_LIMIT]);

    let download_base = capture_raw_server(
        Arc::new(Mutex::new(Vec::new())),
        StatusCode::OK,
        b"abcd".to_vec(),
        Some("application/octet-stream"),
        None,
    )
    .await;
    let download_client = AgentdClient::new(
        reqwest::Client::new(),
        test_config(|config| {
            config.agentd_url = download_base;
            config.tenant = "demo".to_string();
            config.max_outbound_file_bytes = 3;
            config.media_temp_dir = dir.path().to_path_buf();
        }),
    );
    let error = download_client
        .download_artifact_to_temp("demo", "downloads/too-large.bin")
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        agentd_telegram_adapter::agentd::AgentdError::TooLarge { limit: 3 }
    ));
    assert_eq!(ogg_output_count(dir.path()), 0);
}

#[tokio::test]
async fn client_agentd_download_removes_temp_file_when_stream_errors_after_create() {
    let source = error_after_chunk_server(vec![1; 4]).await;
    let dir = tempfile::tempdir().unwrap();
    let client = AgentdClient::new(
        reqwest::Client::new(),
        test_config(|config| {
            config.agentd_url = source;
            config.tenant = "demo".to_string();
            config.max_outbound_file_bytes = 10;
            config.media_temp_dir = dir.path().to_path_buf();
        }),
    );

    let error = client
        .download_artifact_to_temp("demo", "downloads/reset.bin")
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        agentd_telegram_adapter::agentd::AgentdError::Transport
    ));
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn client_agentd_rejects_artifact_tenant_mismatch_before_request() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let base = capture_server(captured.clone(), vec![json!({})]).await;
    let client = AgentdClient::new(
        reqwest::Client::new(),
        test_config(|config| {
            config.agentd_url = base;
            config.tenant = "demo".to_string();
        }),
    );

    let error = client
        .download_artifact_to_temp("other", "inbound/telegram/7/file.txt")
        .await
        .unwrap_err();

    assert!(error.to_string().contains("tenant mismatch"));
    assert!(captured.lock().await.is_empty());
}

#[tokio::test]
async fn client_agentd_redacts_token_from_http_response_body_and_reflected_request_url() {
    let token = "agent-secret-token";
    let base = agentd_reflecting_error_server(token).await;
    let dir = tempfile::tempdir().unwrap();
    let client = AgentdClient::new(
        reqwest::Client::new(),
        test_config(|config| {
            config.agentd_url = base.join(&format!("{token}/")).unwrap();
            config.agentd_token = Some(token.to_string());
            config.tenant = "demo".to_string();
            config.media_temp_dir = dir.path().to_path_buf();
        }),
    );

    let error = client
        .download_artifact_to_temp("demo", "outbound/reply.txt")
        .await
        .unwrap_err();
    let display = format!("{error}");
    let debug = format!("{error:?}");

    assert!(!display.contains(token));
    assert!(!debug.contains(token));
    assert!(!display.contains(&format!("{token}/v1/tenants")));
    assert!(!debug.contains(&format!("{token}/v1/tenants")));
    assert!(display.contains("[redacted]"));
    assert!(debug.contains("[redacted]"));
}

#[tokio::test]
async fn client_agentd_claim_and_ack_preserve_typed_ids_and_payload_json() {
    let delivery_id = Uuid::new_v4();
    let run_id = Uuid::new_v4();
    let claim_token = "claim-token";
    let captured = Arc::new(Mutex::new(Vec::new()));
    let base = capture_server(
        captured.clone(),
        vec![
            json!({
                "deliveries": [{
                    "delivery_id": delivery_id,
                    "tenant": "demo",
                    "run_id": run_id,
                    "claim_token": claim_token,
                    "destination": "tg:42",
                    "payload": {
                        "reply": "hello",
                        "unknown_nested": {"kept": true}
                    }
                }]
            }),
            json!({"delivery_id": delivery_id, "status": "delivered"}),
        ],
    )
    .await;
    let client = AgentdClient::new(
        reqwest::Client::new(),
        test_config(|config| {
            config.agentd_url = base;
            config.tenant = "demo".to_string();
        }),
    );

    let deliveries = client.claim_delivery_outbox(10).await.unwrap();
    assert_eq!(deliveries[0].run_id, Some(run_id));
    assert_eq!(
        deliveries[0].payload["unknown_nested"],
        json!({"kept": true})
    );

    client
        .ack_delivery(DeliveryAck {
            delivery_id,
            claim_token: claim_token.to_string(),
            outcome: "delivered".to_string(),
            error: None,
            retry_after_ms: None,
        })
        .await
        .unwrap();

    let requests = captured.lock().await;
    assert_eq!(
        requests[0].path_and_query,
        "/v1/tenants/demo/deliveries/claim"
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&requests[0].body).unwrap(),
        json!({
            "limit": 10
        })
    );
    assert_eq!(
        requests[1].path_and_query,
        format!("/v1/tenants/demo/deliveries/{delivery_id}/ack")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&requests[1].body).unwrap(),
        json!({
            "claim_token": claim_token,
            "outcome": "delivered",
            "error": null,
            "retry_after_ms": null
        })
    );
}

#[tokio::test]
async fn client_agentd_submits_only_the_async_turn_contract() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let run_id = Uuid::new_v4();
    let base = capture_server(
        captured.clone(),
        vec![json!({"run_id": run_id, "status": "queued"})],
    )
    .await;
    let client = AgentdClient::new(
        reqwest::Client::new(),
        test_config(|config| config.agentd_url = base),
    );

    let response = client
        .submit_turn(SubmitTurn {
            tenant: "demo".into(),
            agent_ref: "simple-bot".into(),
            scope: "tg:42".into(),
            payload: json!({"text":"hello"}),
            delivery: DeliveryRequest {
                destination: "tg:42".into(),
            },
        })
        .await
        .unwrap();

    assert_eq!(response["run_id"], run_id.to_string());
    let requests = captured.lock().await;
    assert_eq!(requests[0].method, Method::POST);
    assert_eq!(requests[0].path_and_query, "/v1/tenants/demo/turns");
    assert_eq!(
        serde_json::from_slice::<Value>(&requests[0].body).unwrap(),
        json!({
            "agent":"simple-bot",
            "scope":"tg:42",
            "payload":{"text":"hello"},
            "delivery":{"destination":"tg:42"}
        })
    );
}

#[tokio::test]
async fn client_agentd_wait_run_can_outlive_submit_timeout() {
    let app = Router::new().route(
        "/*path",
        any(|| async {
            sleep(Duration::from_millis(100)).await;
            Json(json!({"status": "Succeeded"}))
        }),
    );
    let base = spawn_server(app).await;
    let client = AgentdClient::new(
        reqwest::Client::new(),
        test_config(|config| {
            config.agentd_url = base;
            config.submit_timeout = Duration::from_millis(20);
        }),
    );

    let response = client.wait_run(Uuid::new_v4(), 250).await.unwrap();

    assert_eq!(response["status"], "Succeeded");
}

#[tokio::test]
async fn client_agentd_rejects_control_response_first_byte_over_cap() {
    const CONTROL_BODY_CAP: usize = 1_048_576;

    let body = padded_json_string(CONTROL_BODY_CAP + 1);
    let success_base = chunked_status_server(
        StatusCode::OK,
        vec![
            body[..CONTROL_BODY_CAP].to_vec(),
            body[CONTROL_BODY_CAP..].to_vec(),
        ],
    )
    .await;
    let success_client = AgentdClient::new(
        reqwest::Client::new(),
        test_config(|config| {
            config.agentd_url = success_base;
        }),
    );

    let success_error = success_client
        .wait_run(Uuid::new_v4(), 100)
        .await
        .unwrap_err();
    assert!(matches!(success_error, AgentdError::InvalidJson));

    let error_base = sized_server(
        StatusCode::INTERNAL_SERVER_ERROR,
        padded_json_string(CONTROL_BODY_CAP + 1),
        None,
    )
    .await;
    let error_client = AgentdClient::new(
        reqwest::Client::new(),
        test_config(|config| {
            config.agentd_url = error_base;
            config.agentd_token = Some("agent-secret".to_string());
        }),
    );

    let status_error = error_client
        .wait_run(Uuid::new_v4(), 100)
        .await
        .unwrap_err();
    let AgentdError::HttpStatus { status, body } = status_error else {
        panic!("expected status-preserving oversized response error");
    };
    assert_eq!(status, 500);
    assert_eq!(body, "[response body omitted: exceeded 1048576 bytes]");
    assert!(!body.contains("agent-secret"));
}

#[tokio::test]
async fn client_telegram_429_is_transient_and_preserves_retry_after() {
    let base = telegram_error_server(
        StatusCode::TOO_MANY_REQUESTS,
        json!({
            "ok": false,
            "error_code": 429,
            "description": "Too Many Requests for 123:secret-token",
            "parameters": {"retry_after": 7}
        }),
    )
    .await;
    let error = TelegramClient::new(
        reqwest::Client::new(),
        test_config(|config| {
            config.telegram_api_base = base;
            config.bot_token = "123:secret-token".to_string();
        }),
    )
    .send_message(&TelegramChatId::Numeric(42), "hello", None)
    .await
    .unwrap_err();

    assert_eq!(error.category(), ErrorCategory::TransientBackend);
    assert_eq!(error.retry_after(), Some(Duration::from_secs(7)));
    assert!(!format!("{error}").contains("secret-token"));
    assert!(!format!("{error:?}").contains("secret-token"));
}

#[tokio::test]
async fn client_telegram_http_statuses_follow_error_contract() {
    for (status, expected_category, expected_receipt_category) in [
        (
            StatusCode::BAD_REQUEST,
            ErrorCategory::PermanentBackend,
            "permanent_backend",
        ),
        (
            StatusCode::UNAUTHORIZED,
            ErrorCategory::PermanentBackend,
            "permanent_backend",
        ),
        (
            StatusCode::FORBIDDEN,
            ErrorCategory::PermanentBackend,
            "permanent_backend",
        ),
        (
            StatusCode::NOT_FOUND,
            ErrorCategory::PermanentBackend,
            "permanent_backend",
        ),
        (
            StatusCode::TOO_MANY_REQUESTS,
            ErrorCategory::TransientBackend,
            "transient_backend",
        ),
        (
            StatusCode::BAD_GATEWAY,
            ErrorCategory::TransientBackend,
            "transient_backend",
        ),
    ] {
        let base = telegram_error_server(
            status,
            json!({
                "ok": false,
                "error_code": status.as_u16(),
                "description": "telegram error"
            }),
        )
        .await;
        let error = TelegramClient::new(
            reqwest::Client::new(),
            test_config(|config| {
                config.telegram_api_base = base;
            }),
        )
        .send_message(&TelegramChatId::Numeric(42), "hello", None)
        .await
        .unwrap_err();

        assert_eq!(error.category(), expected_category, "status {status}");
        assert_eq!(
            error.category().as_receipt_category(),
            expected_receipt_category,
            "status {status}"
        );
    }
}

#[tokio::test]
async fn client_telegram_malformed_http_4xx_classifies_by_status() {
    for status in [
        StatusCode::BAD_REQUEST,
        StatusCode::UNAUTHORIZED,
        StatusCode::FORBIDDEN,
        StatusCode::NOT_FOUND,
    ] {
        let base = raw_server(status, "not-json").await;
        let error = TelegramClient::new(
            reqwest::Client::new(),
            test_config(|config| {
                config.telegram_api_base = base;
            }),
        )
        .send_message(&TelegramChatId::Numeric(42), "hello", None)
        .await
        .unwrap_err();

        assert_eq!(error.category(), ErrorCategory::PermanentBackend);
        assert_eq!(error.status(), Some(status.as_u16()));
    }
}

#[tokio::test]
async fn client_telegram_http_status_overrides_contradictory_payload_error_code() {
    for (status, payload_error_code, expected_category) in [
        (
            StatusCode::BAD_REQUEST,
            429u16,
            ErrorCategory::PermanentBackend,
        ),
        (
            StatusCode::BAD_GATEWAY,
            401u16,
            ErrorCategory::TransientBackend,
        ),
    ] {
        let base = telegram_error_server(
            status,
            json!({
                "ok": false,
                "error_code": payload_error_code,
                "description": "contradictory Telegram error",
                "parameters": {"retry_after": 3}
            }),
        )
        .await;
        let error = TelegramClient::new(
            reqwest::Client::new(),
            test_config(|config| {
                config.telegram_api_base = base;
            }),
        )
        .send_message(&TelegramChatId::Numeric(42), "hello", None)
        .await
        .unwrap_err();

        assert_eq!(error.category(), expected_category, "status {status}");
        assert_eq!(error.status(), Some(status.as_u16()));
    }
}

#[tokio::test]
async fn client_telegram_2xx_ok_false_uses_payload_error_code() {
    let base = telegram_error_server(
        StatusCode::OK,
        json!({
            "ok": false,
            "error_code": 429,
            "description": "Too Many Requests",
            "parameters": {"retry_after": 9}
        }),
    )
    .await;

    let error = TelegramClient::new(
        reqwest::Client::new(),
        test_config(|config| {
            config.telegram_api_base = base;
        }),
    )
    .send_message(&TelegramChatId::Numeric(42), "hello", None)
    .await
    .unwrap_err();

    assert_eq!(error.category(), ErrorCategory::TransientBackend);
    assert_eq!(error.status(), Some(429));
    assert_eq!(error.retry_after(), Some(Duration::from_secs(9)));
}

#[tokio::test]
async fn client_telegram_success_with_invalid_json_is_bad_response() {
    let invalid_json = raw_server(StatusCode::OK, "not-json").await;

    assert_eq!(
        TelegramClient::new(
            reqwest::Client::new(),
            test_config(|config| {
                config.telegram_api_base = invalid_json;
            })
        )
        .send_message(&TelegramChatId::Numeric(42), "hello", None)
        .await
        .unwrap_err()
        .category(),
        ErrorCategory::BadResponse
    );
}

#[tokio::test]
async fn client_telegram_rejects_control_response_first_byte_over_cap() {
    const CONTROL_BODY_CAP: usize = 1_048_576;
    const TOKEN: &str = "123:oversized-secret-token";

    for (status, expected_category, chunked) in [
        (StatusCode::OK, ErrorCategory::BadResponse, true),
        (
            StatusCode::FORBIDDEN,
            ErrorCategory::PermanentBackend,
            false,
        ),
        (
            StatusCode::TOO_MANY_REQUESTS,
            ErrorCategory::TransientBackend,
            false,
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCategory::TransientBackend,
            false,
        ),
    ] {
        let mut body = padded_telegram_response(CONTROL_BODY_CAP + 1);
        body[128..128 + TOKEN.len()].copy_from_slice(TOKEN.as_bytes());
        let base = if chunked {
            chunked_status_server(
                status,
                vec![
                    body[..CONTROL_BODY_CAP].to_vec(),
                    body[CONTROL_BODY_CAP..].to_vec(),
                ],
            )
            .await
        } else {
            sized_server(status, body, None).await
        };
        let error = TelegramClient::new(
            reqwest::Client::new(),
            test_config(|config| {
                config.telegram_api_base = base;
                config.bot_token = TOKEN.to_string();
            }),
        )
        .send_message(&TelegramChatId::Numeric(42), "hello", None)
        .await
        .unwrap_err();

        assert_eq!(error.category(), expected_category, "status {status}");
        assert_eq!(error.status(), Some(status.as_u16()));
        assert_eq!(error.retry_after(), None);
        assert!(!format!("{error}").contains(TOKEN));
        assert!(!format!("{error:?}").contains(TOKEN));
    }
}

#[tokio::test]
async fn client_telegram_local_validation_remains_validation() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let api_base = capture_server(captured.clone(), vec![json!({})]).await;
    let client = TelegramClient::new(
        reqwest::Client::new(),
        test_config(|config| {
            config.telegram_api_base = api_base;
        }),
    );

    let error = client.file_url("../secret").unwrap_err();

    assert_eq!(error.category(), ErrorCategory::Validation);
    assert!(captured.lock().await.is_empty());
}

#[tokio::test]
async fn client_telegram_uses_custom_bases_and_token_path_segments() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let api_base = capture_server(
        captured.clone(),
        vec![
            json!({"ok": true, "result": {"file_id": "abc", "file_path": "voice/a b.ogg"}}),
            json!({"ok": true, "result": {"message_id": 9}}),
        ],
    )
    .await;
    let file_base = raw_server(StatusCode::OK, "file-bytes").await;
    let client = TelegramClient::new(
        reqwest::Client::new(),
        test_config(|config| {
            config.telegram_api_base = api_base;
            config.telegram_file_api_base = file_base.clone();
            config.bot_token = "123:secret-token".to_string();
        }),
    );

    let file = client.get_file("abc").await.unwrap();
    assert_eq!(file.file_path.as_deref(), Some("voice/a b.ogg"));
    let download = client.file_url("voice/a b.ogg").unwrap();
    assert!(download
        .as_str()
        .ends_with("/bot123:secret-token/voice/a%20b.ogg"));
    client
        .send_chat_action(&TelegramChatId::Numeric(42), "typing")
        .await
        .unwrap();

    let requests = captured.lock().await;
    assert_eq!(requests[0].path_and_query, "/bot123:secret-token/getFile");
    assert_eq!(
        requests[1].path_and_query,
        "/bot123:secret-token/sendChatAction"
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&requests[0].body).unwrap(),
        json!({"file_id": "abc"})
    );
}

#[tokio::test]
async fn client_telegram_serializes_string_chat_id_in_every_send_method() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let api_base = capture_server(
        captured.clone(),
        vec![
            json!({"ok": true, "result": {"message_id": 1}}),
            json!({"ok": true, "result": true}),
            json!({"ok": true, "result": {"message_id": 2}}),
            json!({"ok": true, "result": {"message_id": 3}}),
        ],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("channel.txt");
    tokio::fs::write(&path, b"channel file").await.unwrap();
    let client = TelegramClient::new(
        reqwest::Client::new(),
        test_config(|config| config.telegram_api_base = api_base),
    );
    let channel = TelegramChatId::String("@channel_name".to_string());

    client.send_message(&channel, "hello", None).await.unwrap();
    client.send_chat_action(&channel, "typing").await.unwrap();
    client.send_location(&channel, 1.25, 103.5).await.unwrap();
    client
        .send_file(
            "sendDocument",
            "document",
            &channel,
            UploadFile::new(path, "channel.txt", Some("text/plain".to_string())),
            None,
        )
        .await
        .unwrap();

    let requests = captured.lock().await;
    assert_eq!(
        serde_json::from_slice::<Value>(&requests[0].body).unwrap()["chat_id"],
        json!("@channel_name")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&requests[1].body).unwrap()["chat_id"],
        json!("@channel_name")
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&requests[2].body).unwrap()["chat_id"],
        json!("@channel_name")
    );
    let multipart = String::from_utf8_lossy(&requests[3].body);
    assert!(multipart.contains("name=\"chat_id\""));
    assert!(multipart.contains("@channel_name"));
}

#[tokio::test]
async fn client_telegram_send_file_accepts_actual_size_equal_to_outbound_cap() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let api_base = capture_server(
        captured.clone(),
        vec![json!({"ok": true, "result": {"message_id": 99}})],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("exact.bin");
    tokio::fs::write(&path, b"abc").await.unwrap();
    let client = TelegramClient::new(
        reqwest::Client::new(),
        test_config(|config| {
            config.telegram_api_base = api_base;
            config.max_outbound_file_bytes = 3;
        }),
    );

    client
        .send_file(
            "sendDocument",
            "document",
            &TelegramChatId::Numeric(42),
            UploadFile::new(
                path,
                "exact.bin",
                Some("application/octet-stream".to_string()),
            ),
            None,
        )
        .await
        .unwrap();

    let requests = captured.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].path_and_query,
        "/bot123:test-token/sendDocument"
    );
}

#[tokio::test]
async fn client_telegram_send_file_rejects_actual_size_above_outbound_cap_before_network() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let api_base = capture_server(captured.clone(), vec![json!({})]).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("oversized.bin");
    tokio::fs::write(&path, b"1234").await.unwrap();
    let client = TelegramClient::new(
        reqwest::Client::new(),
        test_config(|config| {
            config.telegram_api_base = api_base;
            config.max_outbound_file_bytes = 3;
        }),
    );

    let error = client
        .send_file(
            "sendDocument",
            "document",
            &TelegramChatId::Numeric(42),
            UploadFile::new(path, "oversized.bin", None),
            None,
        )
        .await
        .unwrap_err();

    assert_eq!(error.category(), ErrorCategory::Validation);
    assert!(format!("{error}").contains("exceeds 3 bytes"));
    assert!(captured.lock().await.is_empty());
}

#[allow(dead_code)]
fn assert_agentd_trait_object(_: Arc<dyn AgentdApi>) {}

#[allow(dead_code)]
fn assert_telegram_trait_object(_: Arc<dyn TelegramApi>) {}

#[tokio::test]
async fn media_download_rejects_content_length_above_limit_before_file_create() {
    let source = sized_server(StatusCode::OK, vec![1; 11], Some(11)).await;
    let dir = tempfile::tempdir().unwrap();

    let error = download_to_temp(&reqwest::Client::new(), source, dir.path(), 10)
        .await
        .unwrap_err();

    assert!(matches!(error, MediaError::TooLarge { limit: 10 }));
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn media_download_rejects_the_first_byte_above_limit_and_removes_temp_file() {
    let source = chunked_server(vec![vec![1; 7], vec![2; 4]]).await;
    let dir = tempfile::tempdir().unwrap();

    let error = download_to_temp(&reqwest::Client::new(), source, dir.path(), 10)
        .await
        .unwrap_err();

    assert!(matches!(error, MediaError::TooLarge { limit: 10 }));
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn media_download_removes_temp_file_when_upstream_stream_errors_after_create() {
    let source = error_after_chunk_server(vec![1; 4]).await;
    let dir = tempfile::tempdir().unwrap();

    let error = download_to_temp(&reqwest::Client::new(), source, dir.path(), 10)
        .await
        .unwrap_err();

    assert!(matches!(error, MediaError::Transport));
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn media_download_removes_temp_file_when_future_is_cancelled_after_create() {
    let source = hanging_after_chunk_server(vec![1; 4]).await;
    let dir = tempfile::tempdir().unwrap();
    let dir_path = dir.path().to_path_buf();

    let handle = tokio::spawn(async move {
        download_to_temp(&reqwest::Client::new(), source, &dir_path, 10).await
    });
    wait_for_dir_entry(dir.path()).await;

    // Tokio's local file backend does not expose a deterministic suspension
    // point inside File::create; cancel at the first deterministic post-create
    // await while retaining the cleanup regression for the same future.
    handle.abort();
    let join = handle.await.unwrap_err();
    assert!(join.is_cancelled());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn media_download_allows_exact_limit_and_temp_media_deletes_on_drop() {
    let source = chunked_server(vec![vec![1; 7], vec![2; 3]]).await;
    let dir = tempfile::tempdir().unwrap();

    let media = download_to_temp(&reqwest::Client::new(), source, dir.path(), 10)
        .await
        .unwrap();
    let path = media.path().to_path_buf();

    assert_eq!(media.len(), 10);
    assert!(path.exists());
    drop(media);
    assert!(!path.exists());
}

#[tokio::test]
async fn media_download_creates_temp_file_with_0600_mode() {
    let source = chunked_server(vec![b"secret".to_vec()]).await;
    let dir = tempfile::tempdir().unwrap();

    let media = download_to_temp(&reqwest::Client::new(), source, dir.path(), 10)
        .await
        .unwrap();

    assert_eq!(file_mode(media.path()), 0o600);
}

#[tokio::test]
async fn media_direct_ogg_opus_mp3_and_m4a_do_not_transcode() {
    for (filename, content_type) in [
        ("voice.ogg", "audio/ogg"),
        ("voice.opus", "audio/opus"),
        ("song.mp3", "audio/mpeg"),
        ("clip.m4a", "audio/mp4"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let ffmpeg = fake_ffmpeg(dir.path());
        let media = write_temp_media(dir.path(), filename, content_type, b"direct").await;
        let original_path = media.path().to_path_buf();

        let prepared = prepare_voice_for_upload(media, &ffmpeg).await.unwrap();

        assert_eq!(prepared.path(), original_path);
        assert!(!dir.path().join("ffmpeg.args").exists());
    }
}

#[tokio::test]
async fn media_ffmpeg_transcode_uses_required_opus_flags_and_file_paths() {
    let dir = tempfile::tempdir().unwrap();
    let ffmpeg = fake_ffmpeg(dir.path());
    let input = write_temp_media(dir.path(), "reply.wav", "audio/wav", b"wav").await;

    let output = transcode_to_ogg_opus(&input, dir.path(), &ffmpeg)
        .await
        .unwrap();

    let args = std::fs::read_to_string(dir.path().join("ffmpeg.args")).unwrap();
    assert!(args.contains("-i\n"));
    assert!(args.contains(input.path().to_str().unwrap()));
    assert!(args.contains(output.path().to_str().unwrap()));
    assert!(args.contains("-ac\n1\n"));
    assert!(args.contains("-ar\n48000\n"));
    assert!(args.contains("-application\nvoip\n"));
    assert!(args.contains("-b:a\n32k\n"));
    assert!(args.contains("-vbr\non\n"));
    assert!(args.contains("-compression_level\n8\n"));
    assert!(args.contains("-frame_duration\n20\n"));
    assert_eq!(output.content_type(), "audio/ogg");
}

#[tokio::test]
async fn media_ffmpeg_output_file_has_0600_mode() {
    let dir = tempfile::tempdir().unwrap();
    let ffmpeg = fake_ffmpeg(dir.path());
    let input = write_temp_media(dir.path(), "reply.wav", "audio/wav", b"wav").await;

    let output = transcode_to_ogg_opus(&input, dir.path(), &ffmpeg)
        .await
        .unwrap();

    assert_eq!(file_mode(output.path()), 0o600);
}

#[tokio::test]
async fn media_ffmpeg_production_timeout_is_thirty_seconds() {
    assert_eq!(FFMPEG_TIMEOUT, Duration::from_secs(30));
}

#[tokio::test]
async fn media_ffmpeg_nonzero_exit_removes_partial_output() {
    let dir = tempfile::tempdir().unwrap();
    let ffmpeg = fake_ffmpeg_nonzero(dir.path());
    let input = write_temp_media(dir.path(), "reply.wav", "audio/wav", b"wav").await;

    let error = transcode_to_ogg_opus(&input, dir.path(), &ffmpeg)
        .await
        .unwrap_err();

    assert!(matches!(error, MediaError::Ffmpeg(_)));
    assert_eq!(ogg_output_count(dir.path()), 0);
}

#[tokio::test]
async fn media_ffmpeg_large_stderr_is_capped_while_child_exits() {
    let dir = tempfile::tempdir().unwrap();
    let ffmpeg = fake_ffmpeg_large_stderr(dir.path(), 16 * 1024);
    let input = write_temp_media(dir.path(), "reply.wav", "audio/wav", b"wav").await;

    let error = transcode_to_ogg_opus(&input, dir.path(), &ffmpeg)
        .await
        .unwrap_err();
    let MediaError::Ffmpeg(stderr) = error else {
        panic!("expected ffmpeg error");
    };

    assert_eq!(stderr.len(), 512);
    assert!(stderr.bytes().all(|byte| byte == b'e'));
}

#[tokio::test]
async fn media_ffmpeg_timeout_removes_partial_output_and_kills_child() {
    let dir = tempfile::tempdir().unwrap();
    let ffmpeg = fake_ffmpeg_hanging(dir.path());
    let input = write_temp_media(dir.path(), "reply.wav", "audio/wav", b"wav").await;

    let error =
        transcode_to_ogg_opus_with_timeout(&input, dir.path(), &ffmpeg, Duration::from_millis(200))
            .await
            .unwrap_err();

    assert!(matches!(error, MediaError::Timeout));
    assert_eq!(ogg_output_count(dir.path()), 0);
    assert_file_stopped_growing(&dir.path().join("ffmpeg.heartbeat")).await;
}

#[tokio::test]
async fn media_ffmpeg_timeout_kills_process_group_and_bounds_stderr_join() {
    let dir = tempfile::tempdir().unwrap();
    let ffmpeg = fake_ffmpeg_descendant_holds_stderr(dir.path());
    let input_path = dir.path().join("reply.wav");
    tokio::fs::write(&input_path, b"wav").await.unwrap();
    let output_dir = dir.path().to_path_buf();
    let ffmpeg_path = ffmpeg.clone();
    let pid_path = dir.path().join("ffmpeg.descendant.pid");

    let handle = tokio::spawn(async move {
        let input = TempMedia::from_existing(input_path, 3, "audio/wav".to_string()).unwrap();
        transcode_to_ogg_opus_with_timeout(
            &input,
            &output_dir,
            &ffmpeg_path,
            Duration::from_secs(5),
        )
        .await
    });
    wait_for_path(&pid_path).await;

    let result = timeout(Duration::from_secs(7), handle)
        .await
        .unwrap()
        .unwrap();

    let error = result.unwrap_err();
    assert!(matches!(error, MediaError::Timeout));
    assert_eq!(ogg_output_count(dir.path()), 0);
    assert_pid_exited(&pid_path).await;
}

#[tokio::test]
async fn media_ffmpeg_cancellation_kills_process_group_and_removes_output() {
    let dir = tempfile::tempdir().unwrap();
    let ffmpeg = fake_ffmpeg_cancellable_descendant(dir.path());
    let input_path = dir.path().join("reply.wav");
    tokio::fs::write(&input_path, b"wav").await.unwrap();
    let output_dir = dir.path().to_path_buf();
    let ffmpeg_path = ffmpeg.clone();
    let group_pid_path = dir.path().join("ffmpeg.group.pid");
    let descendant_pid_path = dir.path().join("ffmpeg.descendant.pid");

    let handle = tokio::spawn(async move {
        let input = TempMedia::from_existing(input_path, 3, "audio/wav".to_string()).unwrap();
        transcode_to_ogg_opus_with_timeout(
            &input,
            &output_dir,
            &ffmpeg_path,
            Duration::from_secs(30),
        )
        .await
    });
    wait_for_path(&group_pid_path).await;
    wait_for_path(&descendant_pid_path).await;
    let heartbeat_path = dir.path().join("ffmpeg.cancel.heartbeat");
    wait_for_path(&heartbeat_path).await;
    handle.abort();
    assert!(handle.await.unwrap_err().is_cancelled());

    sleep(Duration::from_millis(100)).await;
    let heartbeat_len = std::fs::metadata(&heartbeat_path).unwrap().len();
    sleep(Duration::from_millis(100)).await;
    let heartbeat_stopped = std::fs::metadata(&heartbeat_path).unwrap().len() == heartbeat_len;
    let descendant_pid = read_pid(&descendant_pid_path);
    let descendant_stopped = !process_exists(descendant_pid);
    let group_pid = read_pid(&group_pid_path);
    terminate_test_process_group(group_pid);

    assert!(
        descendant_stopped,
        "FFmpeg descendant survived cancellation"
    );
    assert!(heartbeat_stopped, "FFmpeg heartbeat survived cancellation");
    assert_eq!(ogg_output_count(dir.path()), 0);
}

fn test_config(override_config: impl FnOnce(&mut Config)) -> Config {
    let mut config = Config::from_lookup(|key| match key {
        "BOT_TOKEN" => Some("123:test-token".to_string()),
        "WEBHOOK_SECRET" => Some("test-secret".to_string()),
        "AGENTD_URL" => Some("https://agentd.example".to_string()),
        "AUDIO_API_BASE" => Some("https://audio.example/v1".to_string()),
        _ => None,
    })
    .unwrap();
    override_config(&mut config);
    config
}

#[derive(Debug)]
struct CapturedRequest {
    method: Method,
    path_and_query: String,
    headers: HeaderMap,
    body: Bytes,
}

async fn capture_server(captured: Arc<Mutex<Vec<CapturedRequest>>>, responses: Vec<Value>) -> Url {
    let responses = Arc::new(Mutex::new(VecDeque::from(responses)));
    let app =
        Router::new()
            .route(
                "/*path",
                any(
                    |State(state): State<CaptureState>,
                     method: Method,
                     headers: HeaderMap,
                     uri: axum::http::Uri,
                     body: Body| async move {
                        let body = to_bytes(body, usize::MAX).await.unwrap();
                        state.captured.lock().await.push(CapturedRequest {
                            method,
                            path_and_query: uri
                                .path_and_query()
                                .map(|value| value.as_str().to_string())
                                .unwrap_or_else(|| uri.path().to_string()),
                            headers,
                            body,
                        });
                        let response =
                            state.responses.lock().await.pop_front().unwrap_or_else(
                                || json!({"ok": true, "result": {"message_id": 1}}),
                            );
                        Json(response).into_response()
                    },
                ),
            )
            .with_state(CaptureState {
                captured,
                responses,
            });
    spawn_server(app).await
}

async fn counting_upload_server(uploaded_lengths: Arc<Mutex<Vec<u64>>>) -> Url {
    let app = Router::new().route(
        "/*path",
        any(move |body: Body| {
            let uploaded_lengths = uploaded_lengths.clone();
            async move {
                use futures_util::StreamExt as _;

                let mut length = 0u64;
                let mut stream = body.into_data_stream();
                while let Some(chunk) = stream.next().await {
                    length += chunk.unwrap().len() as u64;
                }
                uploaded_lengths.lock().await.push(length);
                Json(json!({"ok": true}))
            }
        }),
    );
    spawn_server(app).await
}

async fn capture_raw_server(
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
    status: StatusCode,
    body: Vec<u8>,
    content_type: Option<&'static str>,
    content_length: Option<u64>,
) -> Url {
    let handler = any(
        move |method: Method, headers: HeaderMap, uri: axum::http::Uri, request_body: Body| {
            let captured = captured.clone();
            let body = body.clone();
            async move {
                let request_body = to_bytes(request_body, usize::MAX).await.unwrap();
                captured.lock().await.push(CapturedRequest {
                    method,
                    path_and_query: uri
                        .path_and_query()
                        .map(|value| value.as_str().to_string())
                        .unwrap_or_else(|| uri.path().to_string()),
                    headers,
                    body: request_body,
                });
                let mut builder = Response::builder().status(status);
                if let Some(content_type) = content_type {
                    builder = builder.header("content-type", content_type);
                }
                if let Some(content_length) = content_length {
                    builder = builder.header("content-length", content_length.to_string());
                }
                builder.body(Body::from(body)).unwrap()
            }
        },
    );
    let app = Router::new()
        .route("/", handler.clone())
        .route("/*path", handler);
    spawn_server(app).await
}

async fn telegram_error_server(status: StatusCode, payload: Value) -> Url {
    let app = Router::new().route(
        "/*path",
        any(move || async move { (status, Json(payload.clone())) }),
    );
    spawn_server(app).await
}

async fn raw_server(status: StatusCode, body: &'static str) -> Url {
    let app = Router::new().route(
        "/*path",
        any(move || async move {
            Response::builder()
                .status(status)
                .body(Body::from(body))
                .unwrap()
        }),
    );
    spawn_server(app).await
}

async fn agentd_reflecting_error_server(token: &'static str) -> Url {
    let app = Router::new().route(
        "/*path",
        any(move |uri: axum::http::Uri| async move {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!(
                    "agentd token {token} appeared in request URL {uri}"
                )))
                .unwrap()
        }),
    );
    spawn_server(app).await
}

async fn sized_server(status: StatusCode, body: Vec<u8>, content_length: Option<u64>) -> Url {
    let handler = any(move || {
        let body = body.clone();
        async move {
            let mut builder = Response::builder().status(status);
            if let Some(content_length) = content_length {
                builder = builder.header("content-length", content_length.to_string());
            }
            builder.body(Body::from(body)).unwrap()
        }
    });
    let app = Router::new()
        .route("/", handler.clone())
        .route("/*path", handler);
    spawn_server(app).await
}

fn padded_json_string(len: usize) -> Vec<u8> {
    assert!(len >= 2);
    let mut body = Vec::with_capacity(len);
    body.push(b'"');
    body.resize(len - 1, b'a');
    body.push(b'"');
    assert_eq!(body.len(), len);
    body
}

fn padded_telegram_response(len: usize) -> Vec<u8> {
    const PREFIX: &[u8] = br#"{"ok":true,"result":{"message_id":1,"padding":""#;
    const SUFFIX: &[u8] = br#""}}"#;
    assert!(len >= PREFIX.len() + SUFFIX.len());
    let mut body = Vec::with_capacity(len);
    body.extend_from_slice(PREFIX);
    body.resize(len - SUFFIX.len(), b'a');
    body.extend_from_slice(SUFFIX);
    assert_eq!(body.len(), len);
    body
}

async fn chunked_server(chunks: Vec<Vec<u8>>) -> Url {
    chunked_status_server(StatusCode::OK, chunks).await
}

async fn chunked_status_server(status: StatusCode, chunks: Vec<Vec<u8>>) -> Url {
    let handler = any(move || {
        let chunks = chunks.clone();
        async move {
            let stream = futures_util::stream::iter(
                chunks
                    .into_iter()
                    .map(|chunk| Ok::<_, std::io::Error>(Bytes::from(chunk))),
            );
            Response::builder()
                .status(status)
                .body(Body::from_stream(stream))
                .unwrap()
        }
    });
    let app = Router::new()
        .route("/", handler.clone())
        .route("/*path", handler);
    spawn_server(app).await
}

async fn error_after_chunk_server(chunk: Vec<u8>) -> Url {
    let handler = any(move || {
        let chunk = chunk.clone();
        async move {
            let stream = futures_util::stream::iter([
                Ok::<_, std::io::Error>(Bytes::from(chunk)),
                Err(std::io::Error::other("upstream reset")),
            ]);
            Body::from_stream(stream)
        }
    });
    let app = Router::new()
        .route("/", handler.clone())
        .route("/*path", handler);
    spawn_server(app).await
}

async fn hanging_after_chunk_server(chunk: Vec<u8>) -> Url {
    let handler = any(move || {
        let chunk = chunk.clone();
        async move {
            use futures_util::StreamExt as _;

            let first =
                futures_util::stream::once(
                    async move { Ok::<_, std::io::Error>(Bytes::from(chunk)) },
                );
            let stream = first.chain(futures_util::stream::pending());
            Body::from_stream(stream)
        }
    });
    let app = Router::new()
        .route("/", handler.clone())
        .route("/*path", handler);
    spawn_server(app).await
}

async fn wait_for_dir_entry(dir: &Path) {
    timeout(Duration::from_secs(2), async {
        loop {
            if std::fs::read_dir(dir).unwrap().next().is_some() {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

async fn wait_for_path(path: &Path) {
    timeout(Duration::from_secs(5), async {
        loop {
            if path.exists() {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

async fn spawn_server(app: Router) -> Url {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Url::parse(&format!("http://{addr}")).unwrap()
}

#[derive(Clone)]
struct CaptureState {
    captured: Arc<Mutex<Vec<CapturedRequest>>>,
    responses: Arc<Mutex<VecDeque<Value>>>,
}

async fn write_temp_media(
    dir: &Path,
    filename: &str,
    content_type: &str,
    body: &[u8],
) -> TempMedia {
    let path = dir.join(filename);
    tokio::fs::write(&path, body).await.unwrap();
    TempMedia::from_existing(path, body.len() as u64, content_type.to_string()).unwrap()
}

fn fake_ffmpeg(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("fake-ffmpeg.sh");
    let args = dir.join("ffmpeg.args");
    let script = format!(
        "#!/bin/sh\n: > {:?}\nfor arg in \"$@\"; do printf '%s\\n' \"$arg\" >> {:?}; done\nout=\"\"\nfor arg in \"$@\"; do out=\"$arg\"; done\nprintf 'ogg' > \"$out\"\n",
        args, args
    );
    std::fs::write(&path, script).unwrap();
    let mut permissions = std::fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&path, permissions).unwrap();
    path
}

fn fake_ffmpeg_nonzero(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("fake-ffmpeg-nonzero.sh");
    let script = "#!/bin/sh\nout=\"\"\nfor arg in \"$@\"; do out=\"$arg\"; done\nprintf 'partial' > \"$out\"\nprintf 'boom' >&2\nexit 1\n";
    std::fs::write(&path, script).unwrap();
    let mut permissions = std::fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&path, permissions).unwrap();
    path
}

fn fake_ffmpeg_large_stderr(dir: &Path, stderr_len: usize) -> std::path::PathBuf {
    let path = dir.join("fake-ffmpeg-large-stderr.sh");
    let chunk = "e".repeat(1024);
    let iterations = stderr_len.div_ceil(chunk.len());
    let script = format!(
        "#!/bin/sh\nout=\"\"\nfor arg in \"$@\"; do out=\"$arg\"; done\ni=0\nwhile [ \"$i\" -lt {iterations} ]; do printf '%s' '{chunk}' >&2; i=$((i + 1)); done\nprintf 'partial' > \"$out\"\nexit 1\n"
    );
    std::fs::write(&path, script).unwrap();
    let mut permissions = std::fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&path, permissions).unwrap();
    path
}

fn fake_ffmpeg_hanging(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("fake-ffmpeg-hanging.sh");
    let heartbeat = dir.join("ffmpeg.heartbeat");
    std::fs::write(&heartbeat, []).unwrap();
    let script = format!(
        "#!/bin/sh\n: > {:?}\nout=\"\"\nfor arg in \"$@\"; do out=\"$arg\"; done\nprintf 'partial' > \"$out\"\nwhile :; do printf . >> {:?}; sleep 0.01; done\n",
        heartbeat, heartbeat
    );
    std::fs::write(&path, script).unwrap();
    let mut permissions = std::fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&path, permissions).unwrap();
    path
}

fn fake_ffmpeg_descendant_holds_stderr(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("fake-ffmpeg-descendant-stderr.sh");
    let pid_file = dir.join("ffmpeg.descendant.pid");
    let script = format!(
        "#!/bin/sh\nsleep 60 &\nprintf '%s' \"$!\" > {:?}\nout=\"\"\nfor arg in \"$@\"; do out=\"$arg\"; done\nprintf 'partial' > \"$out\"\nwhile :; do sleep 1; done\n",
        pid_file
    );
    std::fs::write(&path, script).unwrap();
    let mut permissions = std::fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&path, permissions).unwrap();
    path
}

fn fake_ffmpeg_cancellable_descendant(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("fake-ffmpeg-cancellable-descendant.sh");
    let group_pid_file = dir.join("ffmpeg.group.pid");
    let descendant_pid_file = dir.join("ffmpeg.descendant.pid");
    let heartbeat = dir.join("ffmpeg.cancel.heartbeat");
    let script = format!(
        r#"#!/bin/sh
printf '%s' "$$" > {:?}
out=""
for arg in "$@"; do out="$arg"; done
printf 'partial' > "$out"
(while :; do printf . >> {:?}; sleep 0.01; done) &
printf '%s' "$!" > {:?}
while :; do sleep 1; done
"#,
        group_pid_file, heartbeat, descendant_pid_file
    );
    std::fs::write(&path, script).unwrap();
    let mut permissions = std::fs::metadata(&path).unwrap().permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&path, permissions).unwrap();
    path
}

fn ogg_output_count(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "ogg"))
        .count()
}

fn file_mode(path: &Path) -> u32 {
    std::fs::metadata(path).unwrap().permissions().mode() & 0o777
}

async fn assert_file_stopped_growing(path: &Path) {
    timeout(Duration::from_secs(2), async {
        loop {
            if path.exists() {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let first = std::fs::metadata(path).unwrap().len();
    for _ in 0..10 {
        sleep(Duration::from_millis(20)).await;
        assert_eq!(first, std::fs::metadata(path).unwrap().len());
    }
}

async fn assert_pid_exited(path: &Path) {
    timeout(Duration::from_secs(2), async {
        loop {
            if path.exists() {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let pid: i32 = std::fs::read_to_string(path).unwrap().parse().unwrap();
    let result = timeout(Duration::from_secs(2), async {
        loop {
            if !process_exists(pid) {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if result.is_err() {
        let details = std::process::Command::new("ps")
            .args([
                "-o",
                "pid=,ppid=,pgid=,stat=,command=",
                "-p",
                &pid.to_string(),
            ])
            .output()
            .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
            .unwrap_or_default();
        panic!("descendant {pid} remained after timeout: {details}");
    }
}

#[cfg(unix)]
fn process_exists(pid: i32) -> bool {
    let output = std::process::Command::new("ps")
        .args(["-o", "stat=", "-p", &pid.to_string()])
        .output();
    let Ok(output) = output else {
        return unsafe { libc::kill(pid, 0) == 0 };
    };
    if !output.status.success() {
        return false;
    }
    let stat = String::from_utf8_lossy(&output.stdout);
    let stat = stat.trim();
    !stat.is_empty() && !stat.starts_with('Z')
}

fn read_pid(path: &Path) -> i32 {
    std::fs::read_to_string(path).unwrap().parse().unwrap()
}

#[cfg(unix)]
fn terminate_test_process_group(group_pid: i32) {
    unsafe {
        libc::kill(-group_pid, libc::SIGKILL);
    }
}
