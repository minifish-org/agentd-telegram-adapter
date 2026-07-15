use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use futures_util::StreamExt;
use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use url::Url;
use uuid::Uuid;

pub const FFMPEG_TIMEOUT: Duration = Duration::from_secs(30);
const FFMPEG_STDERR_CAP: usize = 512;

#[derive(Debug)]
pub struct TempMedia {
    path: PathBuf,
    len: u64,
    content_type: String,
}

impl TempMedia {
    pub fn from_existing(
        path: PathBuf,
        len: u64,
        content_type: String,
    ) -> Result<Self, MediaError> {
        if !path.is_file() {
            return Err(MediaError::Io("temporary media file does not exist"));
        }
        Ok(Self {
            path,
            len,
            content_type,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn content_type(&self) -> &str {
        &self.content_type
    }
}

impl Drop for TempMedia {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub async fn download_to_temp(
    client: &reqwest::Client,
    url: Url,
    dir: &Path,
    limit: u64,
) -> Result<TempMedia, MediaError> {
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|_| MediaError::Transport)?;
    let status = response.status();
    if !status.is_success() {
        return Err(MediaError::HttpStatus(status.as_u16()));
    }
    response_body_to_temp(response, dir, limit).await
}

pub async fn response_body_to_temp(
    response: reqwest::Response,
    dir: &Path,
    limit: u64,
) -> Result<TempMedia, MediaError> {
    if response.content_length().is_some_and(|len| len > limit) {
        return Err(MediaError::TooLarge { limit });
    }

    tokio::fs::create_dir_all(dir)
        .await
        .map_err(|_| MediaError::Io("failed to create media temp directory"))?;
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let path = dir.join(format!("media-{}.tmp", Uuid::new_v4().simple()));
    let mut path_guard = TempPathGuard::armed(path.clone());
    let mut file = create_private_file(&path, "failed to create temporary media file").await?;
    let mut written = 0u64;
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| MediaError::Transport)?;
        written = written
            .checked_add(chunk.len() as u64)
            .ok_or(MediaError::TooLarge { limit })?;
        if written > limit {
            return Err(MediaError::TooLarge { limit });
        }
        file.write_all(&chunk)
            .await
            .map_err(|_| MediaError::Io("failed to write temporary media file"))?;
    }
    file.flush()
        .await
        .map_err(|_| MediaError::Io("failed to flush temporary media file"))?;

    let media = TempMedia::from_existing(path, written, content_type)?;
    path_guard.disarm();
    Ok(media)
}

pub async fn prepare_voice_for_upload(
    media: TempMedia,
    ffmpeg_path: &Path,
) -> Result<TempMedia, MediaError> {
    if is_direct_voice_format(media.content_type(), media.path()) {
        return Ok(media);
    }
    let output = transcode_to_ogg_opus(
        &media,
        media.path().parent().unwrap_or(Path::new(".")),
        ffmpeg_path,
    )
    .await?;
    drop(media);
    Ok(output)
}

pub async fn transcode_to_ogg_opus(
    input: &TempMedia,
    output_dir: &Path,
    ffmpeg_path: &Path,
) -> Result<TempMedia, MediaError> {
    transcode_to_ogg_opus_with_timeout(input, output_dir, ffmpeg_path, FFMPEG_TIMEOUT).await
}

pub async fn transcode_to_ogg_opus_with_timeout(
    input: &TempMedia,
    output_dir: &Path,
    ffmpeg_path: &Path,
    timeout_duration: Duration,
) -> Result<TempMedia, MediaError> {
    tokio::fs::create_dir_all(output_dir)
        .await
        .map_err(|_| MediaError::Io("failed to create media temp directory"))?;
    let output_path = output_dir.join(format!("media-{}.ogg", Uuid::new_v4().simple()));
    let mut output_guard = TempPathGuard::armed(output_path.clone());
    let output_file =
        create_private_file(&output_path, "failed to create ffmpeg output file").await?;
    drop(output_file);
    let mut command = Command::new(ffmpeg_path);
    command
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .arg("-nostdin")
        .arg("-hide_banner")
        .arg("-loglevel")
        .arg("error")
        .arg("-y")
        .arg("-i")
        .arg(input.path())
        .arg("-vn")
        .arg("-ac")
        .arg("1")
        .arg("-ar")
        .arg("48000")
        .arg("-c:a")
        .arg("libopus")
        .arg("-application")
        .arg("voip")
        .arg("-b:a")
        .arg("32k")
        .arg("-vbr")
        .arg("on")
        .arg("-compression_level")
        .arg("8")
        .arg("-frame_duration")
        .arg("20")
        .arg("-f")
        .arg("ogg")
        .arg(&output_path);
    configure_child_process_group(&mut command);
    let child = command
        .spawn()
        .map_err(|_| MediaError::Ffmpeg("failed to start ffmpeg".to_string()))?;
    let mut process_guard = ChildProcessGroupGuard::new(child);
    let mut stderr = process_guard
        .child_mut()
        .stderr
        .take()
        .ok_or_else(|| MediaError::Ffmpeg("failed to capture ffmpeg stderr".to_string()))?;
    let mut stderr_task = AbortOnDropTask::new(tokio::spawn(async move {
        drain_capped_stderr(&mut stderr).await
    }));
    let status = match timeout(timeout_duration, process_guard.child_mut().wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(_)) => {
            return Err(MediaError::Ffmpeg("failed to wait for ffmpeg".to_string()));
        }
        Err(_) => {
            process_guard.terminate();
            let _ = timeout(Duration::from_secs(1), process_guard.child_mut().wait()).await;
            let _ = stderr_task.collect().await;
            return Err(MediaError::Timeout);
        }
    };
    let (stderr, stderr_drained) = stderr_task.collect().await;
    if !stderr_drained {
        process_guard.terminate();
    }
    if !status.success() {
        return Err(MediaError::Ffmpeg(capped_stderr(&stderr)));
    }
    let len = tokio::fs::metadata(&output_path)
        .await
        .map_err(|_| MediaError::Io("ffmpeg did not create output media"))?
        .len();
    let media = TempMedia::from_existing(output_path, len, "audio/ogg".to_string())?;
    process_guard.disarm();
    output_guard.disarm();
    Ok(media)
}

pub fn is_direct_voice_format(content_type: &str, path: &Path) -> bool {
    let content_type = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_ascii_lowercase();
    matches!(
        content_type.as_str(),
        "audio/ogg" | "audio/opus" | "audio/mpeg" | "audio/mp3" | "audio/mp4" | "audio/x-m4a"
    ) || path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "ogg" | "opus" | "mp3" | "m4a"
            )
        })
        .unwrap_or(false)
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MediaError {
    #[error("media transfer exceeded {limit} bytes")]
    TooLarge { limit: u64 },
    #[error("media request failed")]
    Transport,
    #[error("media endpoint returned HTTP {0}")]
    HttpStatus(u16),
    #[error("{0}")]
    Io(&'static str),
    #[error("ffmpeg timed out")]
    Timeout,
    #[error("ffmpeg failed: {0}")]
    Ffmpeg(String),
}

fn capped_stderr(stderr: &[u8]) -> String {
    String::from_utf8_lossy(&stderr[..stderr.len().min(FFMPEG_STDERR_CAP)]).into_owned()
}

async fn create_private_file(path: &Path, error: &'static str) -> Result<File, MediaError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    let file = options
        .open(path)
        .await
        .map_err(|_| MediaError::Io(error))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .await
            .map_err(|_| MediaError::Io(error))?;
    }
    Ok(file)
}

async fn drain_capped_stderr(stderr: &mut tokio::process::ChildStderr) -> Vec<u8> {
    let mut retained = Vec::with_capacity(FFMPEG_STDERR_CAP);
    let mut chunk = [0u8; 8192];
    loop {
        match stderr.read(&mut chunk).await {
            Ok(0) => break,
            Ok(n) => {
                let remaining = FFMPEG_STDERR_CAP.saturating_sub(retained.len());
                if remaining > 0 {
                    retained.extend_from_slice(&chunk[..n.min(remaining)]);
                }
            }
            Err(_) => break,
        }
    }
    retained
}

struct AbortOnDropTask<T> {
    handle: Option<JoinHandle<T>>,
}

impl<T> AbortOnDropTask<T> {
    fn new(handle: JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    async fn collect(&mut self) -> (T, bool)
    where
        T: Default,
    {
        let handle = self.handle.as_mut().expect("task is collected once");
        match timeout(Duration::from_millis(500), handle).await {
            Ok(Ok(value)) => {
                self.handle.take();
                (value, true)
            }
            Ok(Err(_)) => {
                self.handle.take();
                (T::default(), false)
            }
            Err(_) => {
                self.handle.take().expect("task exists").abort();
                (T::default(), false)
            }
        }
    }
}

impl<T> Drop for AbortOnDropTask<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

struct ChildProcessGroupGuard {
    child: Child,
    child_id: Option<u32>,
    armed: bool,
}

impl ChildProcessGroupGuard {
    fn new(child: Child) -> Self {
        let child_id = child.id();
        Self {
            child,
            child_id,
            armed: true,
        }
    }

    fn child_mut(&mut self) -> &mut Child {
        &mut self.child
    }

    fn terminate(&mut self) {
        if self.armed {
            terminate_child_process_group(self.child_id);
            let _ = self.child.start_kill();
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ChildProcessGroupGuard {
    fn drop(&mut self) {
        self.terminate();
    }
}

#[cfg(unix)]
fn configure_child_process_group(command: &mut Command) {
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() >= 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        });
    }
}

#[cfg(not(unix))]
fn configure_child_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn terminate_child_process_group(child_id: Option<u32>) {
    if let Some(child_id) = child_id.and_then(|value| i32::try_from(value).ok()) {
        unsafe {
            libc::kill(-child_id, libc::SIGKILL);
        }
    }
}

#[cfg(not(unix))]
fn terminate_child_process_group(_child_id: Option<u32>) {}

struct TempPathGuard {
    path: PathBuf,
    armed: bool,
}

impl TempPathGuard {
    fn armed(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TempPathGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
