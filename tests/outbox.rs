use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agentd_telegram_adapter::agentd::{AgentdApi, AgentdError, DeliveryAck, SubmitTurn};
use agentd_telegram_adapter::delivery::DeliveryService;
use agentd_telegram_adapter::media::TempMedia;
use agentd_telegram_adapter::model::{DeliveryOutboxRecord, TelegramChatId};
use agentd_telegram_adapter::telegram::{
    TelegramApi, TelegramError, TelegramFileInfo, TelegramMessageResult, UploadFile,
};
use agentd_telegram_adapter::Config;
use async_trait::async_trait;
use serde_json::{json, Value};
use tempfile::TempDir;
use url::Url;
use uuid::Uuid;

fn config(root: &std::path::Path) -> Config {
    let values = HashMap::from([
        ("BOT_TOKEN", "bot-secret".to_string()),
        ("WEBHOOK_SECRET", "webhook-secret".to_string()),
        ("STATE_DIR", root.display().to_string()),
        ("MEDIA_TEMP_DIR", root.join("media").display().to_string()),
        ("FFMPEG_PATH", root.join("ffmpeg").display().to_string()),
    ]);
    Config::from_lookup(|key| values.get(key).cloned()).unwrap()
}

fn delivery(destination: Option<&str>) -> DeliveryOutboxRecord {
    DeliveryOutboxRecord {
        delivery_id: Some(Uuid::new_v4()),
        tenant: Some("demo".into()),
        run_id: Some(Uuid::new_v4()),
        claim_token: Some("claim-token".into()),
        destination: destination.map(str::to_string),
        payload: json!({"reply": "hello"}),
        ..Default::default()
    }
}

#[derive(Default)]
struct FakeAgentd {
    claims: Mutex<VecDeque<Vec<DeliveryOutboxRecord>>>,
    acks: Mutex<Vec<DeliveryAck>>,
}

#[async_trait]
impl AgentdApi for FakeAgentd {
    async fn submit_turn(&self, _: SubmitTurn) -> Result<Value, AgentdError> {
        unreachable!()
    }
    async fn wait_run(&self, _: Uuid, _: u64) -> Result<Value, AgentdError> {
        unreachable!()
    }
    async fn call_tool(&self, _: &str, _: Value) -> Result<Value, AgentdError> {
        unreachable!()
    }
    async fn claim_delivery_outbox(
        &self,
        _: usize,
    ) -> Result<Vec<DeliveryOutboxRecord>, AgentdError> {
        Ok(self.claims.lock().unwrap().pop_front().unwrap_or_default())
    }
    async fn ack_delivery(&self, ack: DeliveryAck) -> Result<Value, AgentdError> {
        self.acks.lock().unwrap().push(ack);
        Ok(json!({"ok": true}))
    }
    async fn download_artifact_to_temp(&self, _: &str, _: &str) -> Result<TempMedia, AgentdError> {
        unreachable!()
    }
    async fn upload_artifact_from_file(
        &self,
        _: &str,
        _: &str,
        _: &TempMedia,
    ) -> Result<Value, AgentdError> {
        unreachable!()
    }
}

struct FakeTelegram {
    result: Mutex<Result<TelegramMessageResult, TelegramError>>,
}

impl FakeTelegram {
    fn ok() -> Self {
        Self {
            result: Mutex::new(Ok(TelegramMessageResult {
                message_id: Some(1),
            })),
        }
    }
}

#[async_trait]
impl TelegramApi for FakeTelegram {
    async fn send_message(
        &self,
        _: &TelegramChatId,
        _: &str,
        _: Option<&str>,
    ) -> Result<TelegramMessageResult, TelegramError> {
        let mut result = self.result.lock().unwrap();
        std::mem::replace(
            &mut *result,
            Ok(TelegramMessageResult {
                message_id: Some(1),
            }),
        )
    }
    async fn send_chat_action(&self, _: &TelegramChatId, _: &str) -> Result<(), TelegramError> {
        Ok(())
    }
    async fn send_location(
        &self,
        _: &TelegramChatId,
        _: f64,
        _: f64,
    ) -> Result<TelegramMessageResult, TelegramError> {
        unreachable!()
    }
    async fn send_file(
        &self,
        _: &str,
        _: &str,
        _: &TelegramChatId,
        _: UploadFile,
        _: Option<&str>,
    ) -> Result<TelegramMessageResult, TelegramError> {
        unreachable!()
    }
    async fn get_file(&self, _: &str) -> Result<TelegramFileInfo, TelegramError> {
        unreachable!()
    }
    fn file_url(&self, _: &str) -> Result<Url, TelegramError> {
        unreachable!()
    }
}

fn service(
    root: &std::path::Path,
    agentd: Arc<FakeAgentd>,
    telegram: Arc<FakeTelegram>,
) -> DeliveryService {
    DeliveryService::new(config(root), agentd, telegram)
}

#[tokio::test]
async fn successful_delivery_acks_the_claim_token() {
    let temp = TempDir::new().unwrap();
    let agentd = Arc::new(FakeAgentd::default());
    agentd
        .claims
        .lock()
        .unwrap()
        .push_back(vec![delivery(Some("tg:42"))]);

    assert_eq!(
        service(temp.path(), agentd.clone(), Arc::new(FakeTelegram::ok()))
            .process_outbox_once()
            .await,
        1
    );
    let acks = agentd.acks.lock().unwrap();
    assert_eq!(acks[0].claim_token, "claim-token");
    assert_eq!(acks[0].outcome, "delivered");
}

#[tokio::test]
async fn transient_telegram_failure_requests_retry() {
    let temp = TempDir::new().unwrap();
    let agentd = Arc::new(FakeAgentd::default());
    agentd
        .claims
        .lock()
        .unwrap()
        .push_back(vec![delivery(Some("tg:42"))]);
    let telegram = Arc::new(FakeTelegram {
        result: Mutex::new(Err(TelegramError::from_status(
            "sendMessage",
            429,
            Some(Duration::from_secs(3)),
        ))),
    });

    service(temp.path(), agentd.clone(), telegram)
        .process_outbox_once()
        .await;
    let acks = agentd.acks.lock().unwrap();
    assert_eq!(acks[0].outcome, "retry");
    assert_eq!(acks[0].retry_after_ms, Some(3_000));
}

#[tokio::test]
async fn invalid_destination_is_failed_without_direct_reply() {
    let temp = TempDir::new().unwrap();
    let agentd = Arc::new(FakeAgentd::default());
    agentd
        .claims
        .lock()
        .unwrap()
        .push_back(vec![delivery(None)]);

    service(temp.path(), agentd.clone(), Arc::new(FakeTelegram::ok()))
        .process_outbox_once()
        .await;
    assert_eq!(agentd.acks.lock().unwrap()[0].outcome, "failed");
}
