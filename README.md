# agentd Telegram adapter

The Telegram transport for [agentd](https://github.com/minifish-org/agentd),
kept as an independent process and repository. It admits authenticated Telegram
webhooks, submits tenant-scoped turns, and sends replies exclusively through
agentd's delivery outbox.

```text
Telegram -> webhook -> agentd turn + explicit tg:<chat_id> delivery
agentd    -> atomic run commit -> delivery outbox
adapter   -> claim -> Telegram API -> acknowledge

Telegram voice -> adapter -> ASR provider
voice reply     <- adapter <- TTS provider
```

The adapter never reads agentd's database, links agentd crates, uses run output
as a delivery fallback, or replies directly from the webhook path. Every chat
uses the stable scope `tg:<chat_id>` and explicitly requests delivery to the
same destination.

This is experimental pre-1.0 software. Interfaces and deployment assumptions
may change without a compatibility period.

## Requirements

- Rust 1.92 or a compatible toolchain;
- a running agentd instance;
- a Telegram bot and HTTPS webhook endpoint;
- an OpenAI-compatible audio endpoint for transcription and speech;
- `ffmpeg` for voice replies.

## Configure

Configuration is read from environment variables. Start with the tracked,
credential-free examples:

```sh
cp configs/tg-adapter.env.example .env.local
cp configs/tg-adapter-decoy.env.example .env.decoy.local
chmod 600 .env.local .env.decoy.local
```

Replace every placeholder before running. The required settings are:

| Variable | Purpose |
| --- | --- |
| `BOT_TOKEN` | Telegram Bot API credential |
| `WEBHOOK_SECRET` | Value expected in Telegram's secret-token header |
| `AGENTD_URL` | Base URL of the agentd instance |
| `AUDIO_API_BASE` | Base URL of the OpenAI-compatible audio API |

`AGENTD_TOKEN` and `AUDIO_API_KEY` provide optional bearer credentials.
`TENANT`, `AGENT_REF`, and `ALLOWED_TG_USERS` select the agentd identity and
Telegram admission policy. The source of truth for tuning limits, timeouts,
paths, and defaults is [`src/config.rs`](src/config.rs).

Production credentials belong in `/etc/tg-adapter.env` and
`/etc/tg-adapter-decoy.env`, or in an equivalent secret store. Do not commit
populated environment files.

## Run locally

```sh
set -a
. ./.env.local
. ./.env.decoy.local
set +a
cargo run --release
```

The example binds to `127.0.0.1:18080`. Serve it through an HTTPS reverse proxy
and register `WEBHOOK_PATH` with Telegram using the same `WEBHOOK_SECRET`.
Requests to `/` return the configured decoy file; Telegram webhooks are accepted
only on the configured path with the correct secret header.

Inbound voice files go directly from Telegram to `/audio/transcriptions`.
Voice replies go through `/audio/speech` and `ffmpeg` before Telegram upload.
Neither path uses agentd artifacts.

## Verify

```sh
cargo fmt --all -- --check
cargo check --all-targets
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
tests/test_deployment.sh
```

Tests cover webhook admission, successful acknowledgement, retryable delivery,
terminal failure, media size and timeout boundaries, rendering, and the absence
of a direct reply path.

## Deploy with systemd

The included unit runs with a dynamic user, a private temporary directory, a
read-only system, no Linux capabilities, and bounded memory. Install protected
configuration files before the first deployment, then deploy from a clean,
pushed `main`:

```sh
sudo install -m 600 configs/tg-adapter.env.example /etc/tg-adapter.env
sudo install -m 600 configs/tg-adapter-decoy.env.example /etc/tg-adapter-decoy.env
# Edit both files and create the configured decoy file before continuing.

deploy/update-vps.sh user@example-host
```

The deployment fast-forwards `/opt/agentd-telegram-adapter`, builds the pinned
revision, atomically switches a versioned binary, verifies the service and
loopback endpoint, and restores the previous binary and unit on failure. The
script never writes the environment files.

Review [SECURITY.md](SECURITY.md) before exposing the webhook endpoint.

## License

Apache-2.0.
