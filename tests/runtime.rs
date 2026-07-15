use std::future::{pending, Future};
use std::net::TcpListener as StdTcpListener;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use agentd_telegram_adapter::agentd::{AgentdApi, AgentdClient};
use agentd_telegram_adapter::runtime::{
    install_redacted_panic_hook, run_with_config, run_with_config_loader, supervise_required_tasks,
    RequiredTask, RuntimeError, TaskKind,
};
use agentd_telegram_adapter::telegram::{TelegramApi, TelegramClient};
use agentd_telegram_adapter::webhook::run_inbound_worker;
use agentd_telegram_adapter::Config;
use axum::routing::post;
use axum::{Json, Router};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{oneshot, watch};

fn config_fixture() -> (Config, TempDir) {
    let temp = tempfile::tempdir().expect("tempdir");
    let state_dir = temp.path().join("state");
    let media_dir = temp.path().join("media");
    let decoy = temp.path().join("index.html");
    std::fs::write(&decoy, "ok").expect("write decoy");
    let config = Config::from_lookup(|key| match key {
        "BOT_TOKEN" => Some("bot-token-that-must-stay-secret".to_string()),
        "WEBHOOK_SECRET" => Some("webhook-secret-that-must-stay-secret".to_string()),
        "LISTEN_HOST" => Some("127.0.0.1".to_string()),
        "LISTEN_PORT" => Some("0".to_string()),
        "AGENTD_TOKEN" => Some("agentd-token-that-must-stay-secret".to_string()),
        "ALLOWED_TG_USERS" => Some("99887766".to_string()),
        "DECOY_FILE" => Some(decoy.display().to_string()),
        "STATE_DIR" => Some(state_dir.display().to_string()),
        "MEDIA_TEMP_DIR" => Some(media_dir.display().to_string()),
        _ => None,
    })
    .expect("valid config");
    (config, temp)
}

fn never_shutdown() -> impl Future<Output = Result<(), RuntimeError>> + Send + 'static {
    pending()
}

#[tokio::test]
async fn configuration_failure_happens_before_socket_bind_and_is_redacted() {
    let occupied = StdTcpListener::bind(("127.0.0.1", 0)).expect("reserve port");
    let port = occupied.local_addr().expect("reserved address").port();
    let (ready_tx, ready_rx) = oneshot::channel();

    let error = run_with_config_loader(
        || {
            Config::from_lookup(|key| match key {
                "BOT_TOKEN" => Some("top-secret-bot-token".to_string()),
                "LISTEN_HOST" => Some("127.0.0.1".to_string()),
                "LISTEN_PORT" => Some(port.to_string()),
                "ALLOWED_TG_USERS" => Some("1122334455".to_string()),
                _ => None,
            })
        },
        never_shutdown(),
        Some(ready_tx),
    )
    .await
    .expect_err("missing webhook secret must fail");

    assert_eq!(error, RuntimeError::Configuration);
    assert!(ready_rx.await.is_err());
    let debug = format!("{error:?}");
    assert!(!debug.contains("top-secret-bot-token"));
    assert!(!debug.contains("1122334455"));
}

#[tokio::test]
async fn startup_binds_ephemeral_loopback_and_serves_requests() {
    let (config, _temp) = config_fixture();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (ready_tx, ready_rx) = oneshot::channel();
    let runtime = tokio::spawn(run_with_config(
        config,
        async move {
            let _ = shutdown_rx.await;
            Ok(())
        },
        Some(ready_tx),
    ));

    let ready = tokio::time::timeout(Duration::from_secs(2), ready_rx)
        .await
        .expect("runtime readiness timeout")
        .expect("runtime readiness sender dropped");
    assert!(ready.local_addr.ip().is_loopback());
    assert_ne!(ready.local_addr.port(), 0);

    let mut stream = tokio::net::TcpStream::connect(ready.local_addr)
        .await
        .expect("connect runtime");
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .expect("write request");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("read response");
    assert!(response.starts_with(b"HTTP/1.1 200"));

    shutdown_tx.send(()).expect("request shutdown");
    runtime
        .await
        .expect("runtime task panicked")
        .expect("runtime");
}

#[tokio::test]
async fn real_runtime_runs_enabled_outbox_against_mock_agentd_and_stops() {
    let claims = Arc::new(AtomicUsize::new(0));
    let handler_claims = claims.clone();
    let agentd_app = Router::new().route(
        "/v1/tenants/demo/deliveries/claim",
        post(move || {
            let claims = handler_claims.clone();
            async move {
                claims.fetch_add(1, Ordering::SeqCst);
                Json(serde_json::json!({"deliveries": []}))
            }
        }),
    );
    let agentd_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("mock agentd listener");
    let agentd_addr = agentd_listener.local_addr().expect("mock agentd address");
    let (agentd_stop_tx, agentd_stop_rx) = oneshot::channel();
    let agentd_server = tokio::spawn(async move {
        axum::serve(agentd_listener, agentd_app)
            .with_graceful_shutdown(async {
                let _ = agentd_stop_rx.await;
            })
            .await
            .expect("mock agentd server");
    });

    let (mut config, _temp) = config_fixture();
    config.agentd_url = format!("http://{agentd_addr}").parse().expect("agentd URL");
    config.outbox_poll_secs = 0.01;
    let (runtime_stop_tx, runtime_stop_rx) = oneshot::channel();
    let (ready_tx, ready_rx) = oneshot::channel();
    let runtime = tokio::spawn(run_with_config(
        config,
        async move {
            let _ = runtime_stop_rx.await;
            Ok(())
        },
        Some(ready_tx),
    ));
    ready_rx.await.expect("runtime ready");

    tokio::time::timeout(Duration::from_secs(2), async {
        while claims.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("outbox claim timeout");
    runtime_stop_tx.send(()).expect("stop runtime");
    runtime
        .await
        .expect("runtime task")
        .expect("runtime result");
    agentd_stop_tx.send(()).expect("stop mock agentd");
    agentd_server.await.expect("mock agentd task");
    assert!(claims.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn real_runtime_prunes_old_voice_marker_before_readiness() {
    let (config, _temp) = config_fixture();
    let marker_dir = config.state_dir.join("voice-replies");
    std::fs::create_dir_all(&marker_dir).expect("marker directory");
    let marker = marker_dir.join("00000000-0000-0000-0000-000000000123");
    std::fs::write(&marker, b"voice\n").expect("voice marker");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&marker)
        .expect("open marker");
    file.set_times(
        std::fs::FileTimes::new()
            .set_modified(SystemTime::now() - Duration::from_secs(24 * 60 * 60 + 1)),
    )
    .expect("age marker");

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (ready_tx, ready_rx) = oneshot::channel();
    let runtime = tokio::spawn(run_with_config(
        config,
        async move {
            let _ = shutdown_rx.await;
            Ok(())
        },
        Some(ready_tx),
    ));
    ready_rx.await.expect("runtime ready");
    assert!(!marker.exists());

    shutdown_tx.send(()).expect("stop runtime");
    runtime
        .await
        .expect("runtime task")
        .expect("runtime result");
}

#[test]
fn binary_empty_environment_exits_nonzero_with_static_error() {
    let output = Command::new(adapter_binary())
        .env_clear()
        .output()
        .expect("run adapter binary");

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert_eq!(
        String::from_utf8(output.stderr).expect("UTF-8 stderr"),
        "{\"error\":\"configuration\",\"event\":\"runtime_failed\"}\n"
    );
}

#[cfg(unix)]
#[test]
fn binary_lifecycle_logs_are_static_and_environment_free() {
    let temp = tempfile::tempdir().expect("tempdir");
    let port = available_port();
    let decoy = temp.path().join("index.html");
    std::fs::write(&decoy, "ok").expect("decoy");
    let mut child = Command::new(adapter_binary())
        .env_clear()
        .env("BOT_TOKEN", "lifecycle-secret-bot-token")
        .env("WEBHOOK_SECRET", "lifecycle-secret-webhook")
        .env("LISTEN_HOST", "127.0.0.1")
        .env("LISTEN_PORT", port.to_string())
        .env("DECOY_FILE", &decoy)
        .env("STATE_DIR", temp.path().join("state"))
        .env("MEDIA_TEMP_DIR", temp.path().join("media"))
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("start adapter binary");

    wait_until_listening(&mut child, port);
    let signal_result = unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    assert_eq!(signal_result, 0, "send SIGTERM");
    let output = child.wait_with_output().expect("wait for adapter");

    assert!(output.status.success(), "adapter failed: {output:?}");
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).expect("UTF-8 stderr");
    assert_eq!(
        stderr,
        "{\"event\":\"runtime_started\"}\n{\"event\":\"runtime_stopped\"}\n"
    );
    for forbidden in [
        &port.to_string(),
        "127.0.0.1",
        "lifecycle-secret-bot-token",
        "lifecycle-secret-webhook",
        &temp.path().display().to_string(),
    ] {
        assert!(!stderr.contains(forbidden));
    }
}

#[tokio::test]
async fn inbound_worker_entrypoint_is_awaited_without_an_inner_spawn() {
    let (config, _temp) = config_fixture();
    let http = reqwest::Client::new();
    let agentd: Arc<dyn AgentdApi> = Arc::new(AgentdClient::new(http.clone(), config.clone()));
    let telegram: Arc<dyn TelegramApi> =
        Arc::new(TelegramClient::new(http.clone(), config.clone()));
    let (updates, receiver) = tokio::sync::mpsc::channel(config.webhook_queue_capacity);
    drop(updates);

    run_inbound_worker(config, http, agentd, telegram, receiver)
        .await
        .expect("closed inbound queue stops cleanly");
}

#[tokio::test]
async fn inbound_worker_stops_cleanly_on_external_shutdown() {
    assert_worker_stops_on_shutdown(TaskKind::InboundWorker).await;
}

#[tokio::test]
async fn outbox_worker_stops_cleanly_on_external_shutdown() {
    assert_worker_stops_on_shutdown(TaskKind::OutboxWorker).await;
}

async fn assert_worker_stops_on_shutdown(kind: TaskKind) {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut worker_shutdown = shutdown_rx.clone();
    let stopped = Arc::new(AtomicUsize::new(0));
    let worker_stopped = stopped.clone();
    let task = RequiredTask::new(kind, async move {
        worker_shutdown.changed().await.expect("shutdown sender");
        assert!(*worker_shutdown.borrow());
        worker_stopped.fetch_add(1, Ordering::SeqCst);
        Ok::<_, FakeWorkerError>(())
    });

    supervise_required_tasks(
        async { Ok(()) },
        shutdown_tx,
        vec![task],
        Duration::from_secs(1),
    )
    .await
    .expect("graceful worker shutdown");
    assert_eq!(stopped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn inbound_worker_typed_error_is_fatal() {
    assert_worker_error_is_fatal(TaskKind::InboundWorker).await;
}

#[tokio::test]
async fn outbox_worker_typed_error_is_fatal() {
    assert_worker_error_is_fatal(TaskKind::OutboxWorker).await;
}

async fn assert_worker_error_is_fatal(kind: TaskKind) {
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);
    let task = RequiredTask::new(kind, async { Err::<(), _>(FakeWorkerError) });
    let error = supervise_required_tasks(
        never_shutdown(),
        shutdown_tx,
        vec![task],
        Duration::from_secs(1),
    )
    .await
    .expect_err("worker error must stop runtime");
    assert_eq!(error, RuntimeError::WorkerFailed(kind));
}

#[tokio::test]
async fn unexpected_normal_worker_exit_is_fatal() {
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);
    let task = RequiredTask::new(TaskKind::InboundWorker, async {
        Ok::<_, FakeWorkerError>(())
    });
    let error = supervise_required_tasks(
        never_shutdown(),
        shutdown_tx,
        vec![task],
        Duration::from_secs(1),
    )
    .await
    .expect_err("normal pre-shutdown exit must stop runtime");
    assert_eq!(error, RuntimeError::WorkerExited(TaskKind::InboundWorker));
}

#[tokio::test]
async fn simultaneous_shutdown_and_worker_completion_is_fatal() {
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);
    let task = RequiredTask::new(TaskKind::InboundWorker, async {
        Ok::<_, FakeWorkerError>(())
    });

    let error = supervise_required_tasks(
        async { Ok(()) },
        shutdown_tx,
        vec![task],
        Duration::from_secs(1),
    )
    .await
    .expect_err("ready worker completion must win over ready shutdown");
    assert_eq!(error, RuntimeError::WorkerExited(TaskKind::InboundWorker));
}

#[tokio::test]
async fn injected_signal_stream_failure_is_typed_and_still_joins_workers() {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut worker_shutdown = shutdown_rx.clone();
    let stopped = Arc::new(AtomicUsize::new(0));
    let worker_stopped = stopped.clone();
    let task = RequiredTask::new(TaskKind::InboundWorker, async move {
        worker_shutdown.changed().await.expect("shutdown sender");
        worker_stopped.fetch_add(1, Ordering::SeqCst);
        Ok::<_, FakeWorkerError>(())
    });

    let error = supervise_required_tasks(
        async { Err(RuntimeError::SignalStreamClosed) },
        shutdown_tx,
        vec![task],
        Duration::from_secs(1),
    )
    .await
    .expect_err("signal stream failure must be fatal");
    assert_eq!(error, RuntimeError::SignalStreamClosed);
    assert_eq!(stopped.load(Ordering::SeqCst), 1);
}

#[test]
fn worker_panic_is_fatal_and_subprocess_stderr_is_redacted() {
    let output = Command::new(std::env::current_exe().expect("runtime test executable"))
        .args(["--exact", "panic_hook_subprocess_probe", "--nocapture"])
        .env("AGENTD_RUNTIME_PANIC_PROBE", "1")
        .output()
        .expect("run panic probe");

    assert!(output.status.success(), "probe failed: {output:?}");
    let stderr = String::from_utf8(output.stderr).expect("UTF-8 stderr");
    assert_eq!(stderr.trim(), r#"{"event":"runtime_panicked"}"#);
    assert!(!stderr.contains("secret-bearing-panic-payload"));
    assert!(!stderr.contains("runtime.rs"));
}

#[test]
fn panic_hook_subprocess_probe() {
    if std::env::var_os("AGENTD_RUNTIME_PANIC_PROBE").is_none() {
        return;
    }
    install_redacted_panic_hook();
    let runtime = tokio::runtime::Runtime::new().expect("probe runtime");
    let error = runtime.block_on(async {
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let task = RequiredTask::new(TaskKind::OutboxWorker, async {
            panic!("secret-bearing-panic-payload");
            #[allow(unreachable_code)]
            Ok::<_, FakeWorkerError>(())
        });
        supervise_required_tasks(
            never_shutdown(),
            shutdown_tx,
            vec![task],
            Duration::from_secs(1),
        )
        .await
        .expect_err("worker panic must stop runtime")
    });
    assert_eq!(error, RuntimeError::WorkerPanicked(TaskKind::OutboxWorker));
}

#[tokio::test]
async fn graceful_external_shutdown_awaits_every_required_worker() {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let completed = Arc::new(AtomicUsize::new(0));
    let tasks = [
        TaskKind::Server,
        TaskKind::InboundWorker,
        TaskKind::OutboxWorker,
    ]
    .into_iter()
    .map(|kind| {
        let mut shutdown = shutdown_rx.clone();
        let completed = completed.clone();
        RequiredTask::new(kind, async move {
            shutdown.changed().await.expect("shutdown sender");
            tokio::time::sleep(Duration::from_millis(10)).await;
            completed.fetch_add(1, Ordering::SeqCst);
            Ok::<_, FakeWorkerError>(())
        })
    })
    .collect();

    supervise_required_tasks(async { Ok(()) }, shutdown_tx, tasks, Duration::from_secs(1))
        .await
        .expect("graceful shutdown");
    assert_eq!(completed.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn shutdown_grace_aborts_and_drops_a_non_cooperative_worker() {
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);
    let dropped = Arc::new(AtomicUsize::new(0));
    let guard = DropCounter(dropped.clone());
    let task = RequiredTask::new(TaskKind::OutboxWorker, async move {
        let _guard = guard;
        pending::<()>().await;
        Ok::<_, FakeWorkerError>(())
    });

    let error = supervise_required_tasks(
        async { Ok(()) },
        shutdown_tx,
        vec![task],
        Duration::from_millis(10),
    )
    .await
    .expect_err("hung worker must exceed grace");
    assert_eq!(error, RuntimeError::ShutdownTimeout);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn bind_error_debug_does_not_expose_configuration_secrets() {
    let occupied = StdTcpListener::bind(("127.0.0.1", 0)).expect("reserve port");
    let port = occupied.local_addr().expect("reserved address").port();
    let (mut config, _temp) = config_fixture();
    config.listen_port = port;

    let error = run_with_config(config, never_shutdown(), None)
        .await
        .expect_err("occupied port must fail");
    assert!(matches!(error, RuntimeError::Bind(_)));
    let debug = format!("{error:?}");
    for secret in [
        "bot-token-that-must-stay-secret",
        "webhook-secret-that-must-stay-secret",
        "agentd-token-that-must-stay-secret",
        "99887766",
    ] {
        assert!(!debug.contains(secret));
    }
    let record: serde_json::Value =
        serde_json::from_str(&error.log_record()).expect("structured error record");
    assert_eq!(record["event"], "runtime_failed");
    assert_eq!(record["error"], "bind");
    for secret in [
        "bot-token-that-must-stay-secret",
        "webhook-secret-that-must-stay-secret",
        "agentd-token-that-must-stay-secret",
        "99887766",
    ] {
        assert!(!record.to_string().contains(secret));
    }
}

#[derive(Debug)]
struct FakeWorkerError;

struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn adapter_binary() -> &'static str {
    env!("CARGO_BIN_EXE_agentd-telegram-adapter")
}

fn available_port() -> u16 {
    let listener = StdTcpListener::bind(("127.0.0.1", 0)).expect("reserve port");
    listener.local_addr().expect("reserved address").port()
}

#[cfg(unix)]
fn wait_until_listening(child: &mut std::process::Child, port: u16) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        if let Some(status) = child.try_wait().expect("poll adapter") {
            panic!("adapter exited before listening: {status}");
        }
        assert!(Instant::now() < deadline, "adapter startup timeout");
        std::thread::sleep(Duration::from_millis(10));
    }
}
