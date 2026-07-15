# AGENTS.md

## Purpose

This repository contains the Telegram transport adapter for agentd. It is an
independent HTTP client, not part of the agentd runtime.

## Boundaries

- Receive Telegram webhooks and normalize them into tenant-scoped agentd turns.
- Submit only to agentd's canonical tenant REST API.
- Deliver replies only by claiming and acknowledging agentd delivery rows.
- Keep Telegram-specific rendering, media handling, retry, and webhook policy here.
- Call the configured ASR/TTS provider directly; voice media must not transit
  through agentd tools or artifacts.
- Never link agentd crates, read its database, inspect run output as a fallback, or
  add a direct reply path from webhook handling.
- Keep credentials in environment files; never commit secrets.

## Verification

Run before publishing:

```sh
cargo fmt --all -- --check
cargo check --all-targets
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
tests/test_deployment.sh
```

The VPS deployment must come from a clean, pushed `main` and must preserve
rollback to the prior versioned binary.
