# agentd Telegram adapter

The Telegram integration for [agentd](https://github.com/minifish-org/agentd),
kept as an independent process and repository. It accepts Telegram webhooks,
submits tenant-scoped turns to agentd, and sends replies exclusively from the
delivery outbox.

```text
Telegram -> POST /v1/tenants/:tenant/turns
output_emit -> delivery row
adapter -> claim -> Telegram API -> ack
Telegram voice -> adapter -> ASR provider
voice reply <- adapter <- TTS provider
```

It never reads agentd's database, links agentd crates, uses run output as a
fallback, or replies directly from the webhook path. Every chat uses scope and
lane `tg:<chat_id>`.

## Configuration

Environment variables are parsed in `src/config.rs`. Production credentials
belong in `/etc/tg-adapter.env`; the systemd unit contains no secrets. The
adapter needs an agentd URL and API token, a Telegram bot token and webhook
secret, plus the tenant and agent it should invoke. Audio is adapter-owned:
`AUDIO_API_BASE` points to an OpenAI-compatible provider and
`AUDIO_API_KEY` supplies its optional bearer token. `ASR_MODEL`, `TTS_MODEL`,
and `TTS_VOICE` default to `local/asr`, `local/tts`, and `default`.

Inbound voice files go directly from Telegram to
`/audio/transcriptions`. Voice replies go directly through `/audio/speech`
and ffmpeg before Telegram upload. Neither path uses agentd artifacts or the
operator tool-execution endpoint.

The service also reads `/etc/tg-adapter-decoy.env` for the public decoy response
served at `/`. Telegram webhooks are accepted only on the configured secret
path.

## Verify

```sh
cargo fmt --all -- --check
cargo check --all-targets
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
tests/test_deployment.sh
```

Tests cover successful acknowledgement, retryable delivery, terminal failure,
media handling, webhook admission, and the absence of a direct reply path.

## Deploy

After a clean `main` has been pushed:

```sh
deploy/update-vps.sh singapore
```

The deployment fast-forwards `/opt/agentd-telegram-adapter`, builds the pinned
revision, atomically switches a versioned binary, verifies the systemd service
and loopback decoy endpoint, and restores the previous binary and unit on
failure.
