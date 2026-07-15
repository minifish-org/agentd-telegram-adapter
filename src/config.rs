use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use url::Url;

#[derive(Clone)]
pub struct Config {
    pub listen_host: String,
    pub listen_port: u16,
    pub agentd_url: Url,
    pub agentd_token: Option<String>,
    pub audio_api_base: Url,
    pub audio_api_key: Option<String>,
    pub asr_model: String,
    pub tts_model: String,
    pub tts_voice: String,
    pub tenant: String,
    pub agent_ref: String,
    pub bot_token: String,
    pub webhook_secret: String,
    pub webhook_path: String,
    pub decoy_file: PathBuf,
    pub telegram_api_base: Url,
    pub telegram_file_api_base: Url,
    pub allowed_tg_users: Vec<i64>,
    pub outbox_worker_id: String,
    pub outbox_claim_limit: usize,
    pub outbox_lease_ms: u64,
    pub outbox_poll_secs: f64,
    pub outbox_destination_prefix: String,
    pub submit_timeout: Duration,
    pub typing_poll_window: Duration,
    pub typing_max: Duration,
    pub tts_text_cap: usize,
    pub audio_timeout: Duration,
    pub media_timeout: Duration,
    pub webhook_queue_capacity: usize,
    pub max_webhook_body_bytes: usize,
    pub max_inbound_file_bytes: u64,
    pub max_outbound_file_bytes: u64,
    pub ffmpeg_path: PathBuf,
    pub media_temp_dir: PathBuf,
    pub state_dir: PathBuf,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    pub fn from_lookup<F>(mut lookup: F) -> Result<Self>
    where
        F: FnMut(&str) -> Option<String>,
    {
        let bot_token = required_string(&mut lookup, "BOT_TOKEN")?;
        let webhook_secret = required_string(&mut lookup, "WEBHOOK_SECRET")?;
        let audio_api_base = parse_required_url(&mut lookup, "AUDIO_API_BASE")?;
        let generated_worker_id = current_worker_id()?;
        Ok(Self {
            listen_host: string_with_default(&mut lookup, "LISTEN_HOST", "127.0.0.1"),
            listen_port: parse_value_with_default(&mut lookup, "LISTEN_PORT", 80u16)?,
            agentd_url: parse_url_with_default(
                &mut lookup,
                "AGENTD_URL",
                "https://minifish-home.taila2cd17.ts.net",
            )?,
            agentd_token: optional_string(&mut lookup, "AGENTD_TOKEN"),
            audio_api_base,
            audio_api_key: optional_string(&mut lookup, "AUDIO_API_KEY"),
            asr_model: string_with_default(&mut lookup, "ASR_MODEL", "local/asr"),
            tts_model: string_with_default(&mut lookup, "TTS_MODEL", "local/tts"),
            tts_voice: string_with_default(&mut lookup, "TTS_VOICE", "default"),
            tenant: string_with_default(&mut lookup, "TENANT", "demo"),
            agent_ref: string_with_default(&mut lookup, "AGENT_REF", "simple-bot"),
            bot_token,
            webhook_secret,
            webhook_path: string_with_default(&mut lookup, "WEBHOOK_PATH", "/tg/webhook"),
            decoy_file: parse_path_with_default(
                &mut lookup,
                "DECOY_FILE",
                "/home/admin/httpd/index.html",
            ),
            telegram_api_base: parse_url_with_default(
                &mut lookup,
                "TELEGRAM_API_BASE",
                "https://api.telegram.org",
            )?,
            telegram_file_api_base: parse_url_with_default(
                &mut lookup,
                "TELEGRAM_FILE_API_BASE",
                "https://api.telegram.org/file",
            )?,
            allowed_tg_users: parse_csv_i64(&mut lookup, "ALLOWED_TG_USERS")?,
            outbox_worker_id: optional_string(&mut lookup, "OUTBOX_WORKER_ID")
                .unwrap_or(generated_worker_id),
            outbox_claim_limit: parse_non_zero_with_default(
                &mut lookup,
                "OUTBOX_CLAIM_LIMIT",
                10usize,
            )?,
            outbox_lease_ms: parse_non_zero_with_default(
                &mut lookup,
                "OUTBOX_LEASE_MS",
                60_000u64,
            )?,
            outbox_poll_secs: parse_non_zero_float_with_default(
                &mut lookup,
                "OUTBOX_POLL_SECS",
                2.0,
            )?,
            outbox_destination_prefix: string_with_default(
                &mut lookup,
                "OUTBOX_DESTINATION_PREFIX",
                "tg:",
            ),
            submit_timeout: Duration::from_secs(parse_non_zero_with_default(
                &mut lookup,
                "SUBMIT_TIMEOUT_SECS",
                10u64,
            )?),
            typing_poll_window: Duration::from_millis(parse_non_zero_with_default(
                &mut lookup,
                "TYPING_POLL_WINDOW_MS",
                5_000u64,
            )?),
            typing_max: Duration::from_secs(parse_non_zero_with_default(
                &mut lookup,
                "TYPING_MAX_SECS",
                300u64,
            )?),
            tts_text_cap: parse_non_zero_with_default(&mut lookup, "TTS_TEXT_CAP", 800usize)?,
            audio_timeout: Duration::from_secs(parse_non_zero_with_default(
                &mut lookup,
                "AUDIO_TIMEOUT_SECS",
                60u64,
            )?),
            media_timeout: Duration::from_secs(parse_non_zero_with_default(
                &mut lookup,
                "MEDIA_TIMEOUT_SECS",
                60u64,
            )?),
            webhook_queue_capacity: parse_non_zero_with_default(
                &mut lookup,
                "WEBHOOK_QUEUE_CAPACITY",
                64usize,
            )?,
            max_webhook_body_bytes: parse_non_zero_with_default(
                &mut lookup,
                "MAX_WEBHOOK_BODY_BYTES",
                1_048_576usize,
            )?,
            max_inbound_file_bytes: parse_non_zero_with_default(
                &mut lookup,
                "MAX_INBOUND_FILE_BYTES",
                20_971_520u64,
            )?,
            max_outbound_file_bytes: parse_non_zero_with_default(
                &mut lookup,
                "MAX_OUTBOUND_FILE_BYTES",
                52_428_800u64,
            )?,
            ffmpeg_path: parse_path_with_default(&mut lookup, "FFMPEG_PATH", "ffmpeg"),
            media_temp_dir: parse_path_with_default(
                &mut lookup,
                "MEDIA_TEMP_DIR",
                "/var/lib/agentd-tg-adapter/tmp",
            ),
            state_dir: parse_path_with_default(
                &mut lookup,
                "STATE_DIR",
                "/var/lib/agentd-tg-adapter",
            ),
        })
    }
}

pub fn default_worker_id(hostname: &str, pid: u32) -> String {
    format!("tg-webhook-adapter:{hostname}:{pid}")
}

fn current_worker_id() -> Result<String> {
    let hostname = hostname::get().context("failed to determine host name")?;
    Ok(default_worker_id(
        &hostname.to_string_lossy(),
        std::process::id(),
    ))
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("listen_host", &self.listen_host)
            .field("listen_port", &self.listen_port)
            .field("agentd_url", &self.agentd_url)
            .field(
                "agentd_token",
                &self.agentd_token.as_ref().map(|_| "[redacted]"),
            )
            .field("audio_api_base", &self.audio_api_base)
            .field(
                "audio_api_key",
                &self.audio_api_key.as_ref().map(|_| "[redacted]"),
            )
            .field("asr_model", &self.asr_model)
            .field("tts_model", &self.tts_model)
            .field("tts_voice", &self.tts_voice)
            .field("tenant", &self.tenant)
            .field("agent_ref", &self.agent_ref)
            .field("bot_token", &"[redacted]")
            .field("webhook_secret", &"[redacted]")
            .field("webhook_path", &self.webhook_path)
            .field("decoy_file", &self.decoy_file)
            .field("telegram_api_base", &self.telegram_api_base)
            .field("telegram_file_api_base", &self.telegram_file_api_base)
            .field("allowed_tg_users", &self.allowed_tg_users)
            .field("outbox_worker_id", &self.outbox_worker_id)
            .field("outbox_claim_limit", &self.outbox_claim_limit)
            .field("outbox_lease_ms", &self.outbox_lease_ms)
            .field("outbox_poll_secs", &self.outbox_poll_secs)
            .field("outbox_destination_prefix", &self.outbox_destination_prefix)
            .field("submit_timeout", &self.submit_timeout)
            .field("typing_poll_window", &self.typing_poll_window)
            .field("typing_max", &self.typing_max)
            .field("tts_text_cap", &self.tts_text_cap)
            .field("audio_timeout", &self.audio_timeout)
            .field("media_timeout", &self.media_timeout)
            .field("webhook_queue_capacity", &self.webhook_queue_capacity)
            .field("max_webhook_body_bytes", &self.max_webhook_body_bytes)
            .field("max_inbound_file_bytes", &self.max_inbound_file_bytes)
            .field("max_outbound_file_bytes", &self.max_outbound_file_bytes)
            .field("ffmpeg_path", &self.ffmpeg_path)
            .field("media_temp_dir", &self.media_temp_dir)
            .field("state_dir", &self.state_dir)
            .finish()
    }
}

fn required_string<F>(lookup: &mut F, key: &str) -> Result<String>
where
    F: FnMut(&str) -> Option<String>,
{
    optional_string(lookup, key).ok_or_else(|| anyhow!("{key} is required"))
}

fn optional_string<F>(lookup: &mut F, key: &str) -> Option<String>
where
    F: FnMut(&str) -> Option<String>,
{
    lookup(key)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn string_with_default<F>(lookup: &mut F, key: &str, default: &str) -> String
where
    F: FnMut(&str) -> Option<String>,
{
    optional_string(lookup, key).unwrap_or_else(|| default.to_string())
}

fn parse_value_with_default<F, T>(lookup: &mut F, key: &str, default: T) -> Result<T>
where
    F: FnMut(&str) -> Option<String>,
    T: std::str::FromStr + Copy,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match optional_string(lookup, key) {
        Some(raw) => raw
            .parse()
            .with_context(|| format!("{key} must be a valid value")),
        None => Ok(default),
    }
}

fn parse_non_zero_with_default<F, T>(lookup: &mut F, key: &str, default: T) -> Result<T>
where
    F: FnMut(&str) -> Option<String>,
    T: std::str::FromStr + Copy + PartialEq + Default,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let value = parse_value_with_default(lookup, key, default)?;
    if value == T::default() {
        bail!("{key} must be greater than zero");
    }
    Ok(value)
}

fn parse_non_zero_float_with_default<F>(lookup: &mut F, key: &str, default: f64) -> Result<f64>
where
    F: FnMut(&str) -> Option<String>,
{
    let value = parse_value_with_default(lookup, key, default)?;
    if !(value.is_finite() && value > 0.0) {
        bail!("{key} must be greater than zero");
    }
    Ok(value)
}

fn parse_csv_i64<F>(lookup: &mut F, key: &str) -> Result<Vec<i64>>
where
    F: FnMut(&str) -> Option<String>,
{
    let Some(raw) = optional_string(lookup, key) else {
        return Ok(Vec::new());
    };
    raw.split(',')
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            value
                .trim()
                .parse::<i64>()
                .with_context(|| format!("{key} must contain only integer Telegram user ids"))
        })
        .collect()
}

fn parse_path_with_default<F>(lookup: &mut F, key: &str, default: &str) -> PathBuf
where
    F: FnMut(&str) -> Option<String>,
{
    PathBuf::from(string_with_default(lookup, key, default))
}

fn parse_url_with_default<F>(lookup: &mut F, key: &str, default: &str) -> Result<Url>
where
    F: FnMut(&str) -> Option<String>,
{
    let raw = string_with_default(lookup, key, default);
    let normalized = raw.trim_end_matches('/').to_string();
    let candidate = if normalized.is_empty() {
        raw
    } else {
        normalized
    };
    Url::parse(&candidate).with_context(|| format!("{key} must be a valid URL"))
}

fn parse_required_url<F>(lookup: &mut F, key: &str) -> Result<Url>
where
    F: FnMut(&str) -> Option<String>,
{
    let raw = required_string(lookup, key)?;
    let normalized = raw.trim_end_matches('/');
    Url::parse(normalized).with_context(|| format!("{key} must be a valid URL"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::time::Duration;

    use super::{default_worker_id, Config};

    #[test]
    fn current_environment_defaults_are_preserved() {
        let env = TestEnv::new()
            .set("BOT_TOKEN", "123:secret")
            .set("WEBHOOK_SECRET", "hook-secret");
        let config = Config::from_lookup(|key| env.get(key)).unwrap();
        assert_eq!(config.listen_host, "127.0.0.1");
        assert_eq!(config.listen_port, 80);
        assert_eq!(
            config.agentd_url.as_str(),
            "https://minifish-home.taila2cd17.ts.net/"
        );
        assert_eq!(config.tenant, "demo");
        assert_eq!(config.asr_model, "local/asr");
        assert_eq!(config.tts_model, "local/tts");
        assert_eq!(config.tts_voice, "default");
        assert_eq!(config.outbox_claim_limit, 10);
        assert_eq!(config.webhook_queue_capacity, 64);
        assert_eq!(config.max_inbound_file_bytes, 20_971_520);
        assert_eq!(config.max_outbound_file_bytes, 52_428_800);
        assert_eq!(config.state_dir, Path::new("/var/lib/agentd-tg-adapter"));
        assert_eq!(
            config.media_temp_dir,
            Path::new("/var/lib/agentd-tg-adapter/tmp")
        );
    }

    #[test]
    fn secrets_are_required_and_debug_output_is_redacted() {
        let error = Config::from_lookup(|_| None).unwrap_err().to_string();
        assert!(error.contains("BOT_TOKEN"));
        let config = config_fixture();
        let debug = format!("{config:?}");
        assert!(!debug.contains("123:secret"));
        assert!(!debug.contains("hook-secret"));
        assert!(!debug.contains("audio-secret"));
        assert!(debug.contains("[redacted]"));

        let env = TestEnv::default()
            .set("BOT_TOKEN", "123:secret")
            .set("WEBHOOK_SECRET", "hook-secret");
        let error = Config::from_lookup(|key| env.get(key))
            .unwrap_err()
            .to_string();
        assert!(error.contains("AUDIO_API_BASE"));
    }

    #[test]
    fn typed_values_are_parsed_from_environment() {
        let env = TestEnv::new()
            .set("BOT_TOKEN", "123:secret")
            .set("WEBHOOK_SECRET", "hook-secret")
            .set("AGENTD_URL", "https://agentd.example/base/")
            .set("AUDIO_API_BASE", "https://audio.example/v1/")
            .set("AUDIO_API_KEY", "audio-secret")
            .set("ASR_MODEL", "asr-one")
            .set("TTS_MODEL", "tts-one")
            .set("TTS_VOICE", "voice-one")
            .set("TELEGRAM_API_BASE", "https://telegram.example/api/")
            .set("TELEGRAM_FILE_API_BASE", "https://telegram.example/file/")
            .set("ALLOWED_TG_USERS", "42, -7, 99")
            .set("OUTBOX_WORKER_ID", "worker-a")
            .set("OUTBOX_CLAIM_LIMIT", "3")
            .set("OUTBOX_POLL_SECS", "2.5")
            .set("SUBMIT_TIMEOUT_SECS", "11")
            .set("TYPING_POLL_WINDOW_MS", "1500")
            .set("AUDIO_TIMEOUT_SECS", "44")
            .set("MEDIA_TIMEOUT_SECS", "55")
            .set("FFMPEG_PATH", "/usr/local/bin/ffmpeg")
            .set("STATE_DIR", "/srv/adapter/state")
            .set("MEDIA_TEMP_DIR", "/srv/adapter/tmp");

        let config = Config::from_lookup(|key| env.get(key)).unwrap();
        assert_eq!(config.allowed_tg_users, vec![42, -7, 99]);
        assert_eq!(config.outbox_worker_id, "worker-a");
        assert_eq!(config.outbox_claim_limit, 3);
        assert_eq!(config.outbox_poll_secs, 2.5);
        assert_eq!(config.submit_timeout, Duration::from_secs(11));
        assert_eq!(config.typing_poll_window, Duration::from_millis(1500));
        assert_eq!(config.audio_timeout, Duration::from_secs(44));
        assert_eq!(config.media_timeout, Duration::from_secs(55));
        assert_eq!(config.audio_api_base.as_str(), "https://audio.example/v1");
        assert_eq!(config.asr_model, "asr-one");
        assert_eq!(config.tts_model, "tts-one");
        assert_eq!(config.tts_voice, "voice-one");
        assert_eq!(config.ffmpeg_path, Path::new("/usr/local/bin/ffmpeg"));
        assert_eq!(config.state_dir, Path::new("/srv/adapter/state"));
        assert_eq!(config.media_temp_dir, Path::new("/srv/adapter/tmp"));
        assert_eq!(config.agentd_url.as_str(), "https://agentd.example/base");
        assert_eq!(
            config.telegram_api_base.as_str(),
            "https://telegram.example/api"
        );
        assert_eq!(
            config.telegram_file_api_base.as_str(),
            "https://telegram.example/file"
        );
    }

    #[test]
    fn typing_max_has_a_bounded_default_and_allows_an_explicit_override() {
        let env = TestEnv::new()
            .set("BOT_TOKEN", "123:secret")
            .set("WEBHOOK_SECRET", "hook-secret");
        let config = Config::from_lookup(|key| env.get(key)).unwrap();
        assert_eq!(config.typing_max, Duration::from_secs(300));

        let env = env.set("TYPING_MAX_SECS", "7");
        let config = Config::from_lookup(|key| env.get(key)).unwrap();
        assert_eq!(config.typing_max, Duration::from_secs(7));
    }

    #[test]
    fn default_worker_id_uses_hostname_and_pid_format() {
        assert_eq!(
            default_worker_id("test-host", 4242),
            "tg-webhook-adapter:test-host:4242"
        );
    }

    #[test]
    fn explicit_worker_id_override_is_preserved() {
        let env = TestEnv::new()
            .set("BOT_TOKEN", "123:secret")
            .set("WEBHOOK_SECRET", "hook-secret")
            .set("OUTBOX_WORKER_ID", "worker-a");
        let config = Config::from_lookup(|key| env.get(key)).unwrap();
        assert_eq!(config.outbox_worker_id, "worker-a");
    }

    #[test]
    fn malformed_urls_and_zero_limits_are_rejected() {
        let env = TestEnv::new()
            .set("BOT_TOKEN", "123:secret")
            .set("WEBHOOK_SECRET", "hook-secret")
            .set("AGENTD_URL", "not-a-url");
        let error = Config::from_lookup(|key| env.get(key))
            .unwrap_err()
            .to_string();
        assert!(error.contains("AGENTD_URL"));

        let env = TestEnv::new()
            .set("BOT_TOKEN", "123:secret")
            .set("WEBHOOK_SECRET", "hook-secret")
            .set("WEBHOOK_QUEUE_CAPACITY", "0");
        let error = Config::from_lookup(|key| env.get(key))
            .unwrap_err()
            .to_string();
        assert!(error.contains("WEBHOOK_QUEUE_CAPACITY"));
    }

    fn config_fixture() -> Config {
        let env = TestEnv::new()
            .set("BOT_TOKEN", "123:secret")
            .set("WEBHOOK_SECRET", "hook-secret");
        Config::from_lookup(|key| env.get(key)).unwrap()
    }

    #[derive(Default)]
    struct TestEnv {
        values: BTreeMap<String, String>,
    }

    impl TestEnv {
        fn new() -> Self {
            Self::default()
                .set("AUDIO_API_BASE", "https://audio.example/v1")
                .set("AUDIO_API_KEY", "audio-secret")
        }

        fn set(mut self, key: &str, value: &str) -> Self {
            self.values.insert(key.to_string(), value.to_string());
            self
        }

        fn get(&self, key: &str) -> Option<String> {
            self.values.get(key).cloned()
        }
    }
}
