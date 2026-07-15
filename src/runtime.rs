use std::fmt;
use std::future::Future;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::FutureExt;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;

use crate::agentd::{AgentdApi, AgentdClient};
use crate::delivery::{prune_voice_reply_markers, DeliveryService};
use crate::telegram::{TelegramApi, TelegramClient};
use crate::webhook::{self, WebhookState};
use crate::Config;

const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

type BoxTaskFuture = Pin<Box<dyn Future<Output = bool> + Send + 'static>>;
pub type ShutdownFuture = Pin<Box<dyn Future<Output = Result<(), RuntimeError>> + Send + 'static>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskKind {
    Server,
    InboundWorker,
    OutboxWorker,
}

impl fmt::Display for TaskKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Server => "HTTP server",
            Self::InboundWorker => "inbound worker",
            Self::OutboxWorker => "outbox worker",
        })
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RuntimeError {
    #[error("invalid runtime configuration")]
    Configuration,
    #[error("failed to bind runtime listener: {0:?}")]
    Bind(ErrorKind),
    #[error("failed to prepare runtime state: {0:?}")]
    State(ErrorKind),
    #[error("failed to install shutdown signal handler: {0:?}")]
    Signal(ErrorKind),
    #[error("shutdown signal stream closed unexpectedly")]
    SignalStreamClosed,
    #[error("required {0} exited unexpectedly")]
    WorkerExited(TaskKind),
    #[error("required {0} returned an error")]
    WorkerFailed(TaskKind),
    #[error("required {0} panicked")]
    WorkerPanicked(TaskKind),
    #[error("required tasks exceeded the shutdown grace period")]
    ShutdownTimeout,
}

impl RuntimeError {
    pub fn log_record(&self) -> String {
        let error = match self {
            Self::Configuration => "configuration",
            Self::Bind(_) => "bind",
            Self::State(_) => "state",
            Self::Signal(_) | Self::SignalStreamClosed => "signal",
            Self::WorkerExited(_) => "worker_exited",
            Self::WorkerFailed(_) => "worker_failed",
            Self::WorkerPanicked(_) => "worker_panicked",
            Self::ShutdownTimeout => "shutdown_timeout",
        };
        serde_json::json!({"event": "runtime_failed", "error": error}).to_string()
    }
}

pub fn install_redacted_panic_hook() {
    std::panic::set_hook(Box::new(|_| {
        eprintln!(r#"{{"event":"runtime_panicked"}}"#);
    }));
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeReady {
    pub local_addr: SocketAddr,
}

pub struct RequiredTask {
    kind: TaskKind,
    future: BoxTaskFuture,
}

impl RequiredTask {
    pub fn new<F, E>(kind: TaskKind, future: F) -> Self
    where
        F: Future<Output = Result<(), E>> + Send + 'static,
        E: Send + 'static,
    {
        Self {
            kind,
            future: Box::pin(async move { future.await.is_err() }),
        }
    }
}

enum TaskCompletion {
    Exited(TaskKind),
    Failed(TaskKind),
    Panicked(TaskKind),
}

impl TaskCompletion {
    fn into_unexpected_error(self) -> RuntimeError {
        match self {
            Self::Exited(kind) => RuntimeError::WorkerExited(kind),
            Self::Failed(kind) => RuntimeError::WorkerFailed(kind),
            Self::Panicked(kind) => RuntimeError::WorkerPanicked(kind),
        }
    }

    fn into_shutdown_error(self) -> Option<RuntimeError> {
        match self {
            Self::Exited(_) => None,
            Self::Failed(kind) => Some(RuntimeError::WorkerFailed(kind)),
            Self::Panicked(kind) => Some(RuntimeError::WorkerPanicked(kind)),
        }
    }
}

pub async fn supervise_required_tasks<S>(
    shutdown: S,
    shutdown_tx: watch::Sender<bool>,
    tasks: Vec<RequiredTask>,
    grace: Duration,
) -> Result<(), RuntimeError>
where
    S: Future<Output = Result<(), RuntimeError>> + Send,
{
    let mut required = JoinSet::new();
    for task in tasks {
        required.spawn(async move {
            let kind = task.kind;
            match AssertUnwindSafe(task.future).catch_unwind().await {
                Ok(false) => TaskCompletion::Exited(kind),
                Ok(true) => TaskCompletion::Failed(kind),
                Err(_) => TaskCompletion::Panicked(kind),
            }
        });
    }
    tokio::task::yield_now().await;

    tokio::pin!(shutdown);
    tokio::select! {
        biased;
        completion = required.join_next() => {
            let error = joined_completion(completion).into_unexpected_error();
            let _ = shutdown_tx.send(true);
            stop_remaining_tasks(&mut required, grace).await;
            Err(error)
        }
        shutdown_result = &mut shutdown => {
            let _ = shutdown_tx.send(true);
            let drain_result = drain_required_tasks(&mut required, grace).await;
            match shutdown_result {
                Ok(()) => drain_result,
                Err(error) => Err(error),
            }
        }
    }
}

async fn drain_required_tasks(
    required: &mut JoinSet<TaskCompletion>,
    grace: Duration,
) -> Result<(), RuntimeError> {
    let drain = async {
        let mut first_error = None;
        while let Some(completion) = required.join_next().await {
            let completion = joined_completion(Some(completion));
            if first_error.is_none() {
                first_error = completion.into_shutdown_error();
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    };
    match tokio::time::timeout(grace, drain).await {
        Ok(result) => result,
        Err(_) => {
            required.abort_all();
            while required.join_next().await.is_some() {}
            Err(RuntimeError::ShutdownTimeout)
        }
    }
}

async fn stop_remaining_tasks(required: &mut JoinSet<TaskCompletion>, grace: Duration) {
    if tokio::time::timeout(grace, async {
        while required.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        required.abort_all();
        while required.join_next().await.is_some() {}
    }
}

fn joined_completion(
    completion: Option<Result<TaskCompletion, tokio::task::JoinError>>,
) -> TaskCompletion {
    match completion {
        Some(Ok(completion)) => completion,
        Some(Err(_)) | None => TaskCompletion::Panicked(TaskKind::Server),
    }
}

pub async fn run_with_config_loader<L, S>(
    load_config: L,
    shutdown: S,
    ready: Option<oneshot::Sender<RuntimeReady>>,
) -> Result<(), RuntimeError>
where
    L: FnOnce() -> anyhow::Result<Config>,
    S: Future<Output = Result<(), RuntimeError>> + Send + 'static,
{
    let config = load_config().map_err(|_| RuntimeError::Configuration)?;
    run_with_config(config, shutdown, ready).await
}

pub async fn run_with_config<S>(
    config: Config,
    shutdown: S,
    ready: Option<oneshot::Sender<RuntimeReady>>,
) -> Result<(), RuntimeError>
where
    S: Future<Output = Result<(), RuntimeError>> + Send + 'static,
{
    prune_voice_reply_markers(&config.state_dir)
        .map_err(|error| RuntimeError::State(error.kind()))?;

    let http = reqwest::Client::new();
    let agentd: Arc<dyn AgentdApi> = Arc::new(AgentdClient::new(http.clone(), config.clone()));
    let telegram: Arc<dyn TelegramApi> =
        Arc::new(TelegramClient::new(http.clone(), config.clone()));
    let delivery = Arc::new(DeliveryService::new(
        config.clone(),
        agentd.clone(),
        telegram.clone(),
    ));

    let listener = tokio::net::TcpListener::bind((config.listen_host.as_str(), config.listen_port))
        .await
        .map_err(|error| RuntimeError::Bind(error.kind()))?;
    let local_addr = listener
        .local_addr()
        .map_err(|error| RuntimeError::Bind(error.kind()))?;

    let (updates, inbound) = mpsc::channel(config.webhook_queue_capacity);
    let app = webhook::router(WebhookState::new(config.clone(), updates));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mut tasks = Vec::with_capacity(3);
    tasks.push(RequiredTask::new(TaskKind::InboundWorker, {
        let config = config.clone();
        async move { webhook::run_inbound_worker(config, http, agentd, telegram, inbound).await }
    }));

    let server_shutdown = shutdown_rx.clone();
    tasks.push(RequiredTask::new(TaskKind::Server, async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(wait_for_shutdown(server_shutdown))
            .await
            .map_err(|_| ())
    }));

    tasks.push(RequiredTask::new(TaskKind::OutboxWorker, async move {
        delivery.run_outbox_worker(shutdown_rx).await
    }));

    log_event("runtime_started");
    if let Some(ready) = ready {
        let _ = ready.send(RuntimeReady { local_addr });
    }
    let result = supervise_required_tasks(shutdown, shutdown_tx, tasks, SHUTDOWN_GRACE).await;
    if result.is_ok() {
        log_event("runtime_stopped");
    }
    result
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if !*shutdown.borrow() {
        let _ = shutdown.changed().await;
    }
}

fn log_event(event: &str) {
    eprintln!("{}", serde_json::json!({"event": event}));
}

#[cfg(unix)]
pub fn shutdown_signal() -> Result<ShutdownFuture, RuntimeError> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut interrupt =
        signal(SignalKind::interrupt()).map_err(|error| RuntimeError::Signal(error.kind()))?;
    let mut terminate =
        signal(SignalKind::terminate()).map_err(|error| RuntimeError::Signal(error.kind()))?;
    Ok(Box::pin(async move {
        let received = tokio::select! {
            received = interrupt.recv() => received,
            received = terminate.recv() => received,
        };
        match received {
            Some(()) => Ok(()),
            None => Err(RuntimeError::SignalStreamClosed),
        }
    }))
}

#[cfg(not(unix))]
pub fn shutdown_signal() -> Result<ShutdownFuture, RuntimeError> {
    Ok(Box::pin(async {
        tokio::signal::ctrl_c()
            .await
            .map_err(|error| RuntimeError::Signal(error.kind()))
    }))
}
