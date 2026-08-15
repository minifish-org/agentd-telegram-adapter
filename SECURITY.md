# Security policy

agentd Telegram adapter is experimental pre-1.0 software. Only the current
`main` branch is maintained; there are no supported release lines yet.

## Reporting a vulnerability

Use GitHub's **Report a vulnerability** flow under the repository's Security
tab. Do not open a public issue for a suspected vulnerability or include
credentials, personal data, production addresses, or exploit details in a
public discussion.

Include the affected commit, deployment shape, sanitized configuration,
attacker prerequisites, a minimal reproduction, observed impact, and any known
mitigation. Complete reports are acknowledged on a best-effort basis within
seven days.

Credential disclosure, webhook-authentication bypass, delivery acknowledgement
errors, cross-chat or cross-tenant routing, unbounded media handling, SSRF, and
command execution outside the documented operator boundary are treated as
security issues.

## Operator boundary

- Store Telegram, agentd, and audio-provider credentials only in protected
  environment files or an equivalent secret store.
- Terminate TLS and apply public-network controls in a reverse proxy; the
  adapter listens on loopback by default.
- Treat `FFMPEG_PATH`, provider URLs, the decoy file, tenant, and agent
  selection as operator-controlled configuration.
- Restrict `ALLOWED_TG_USERS` when the bot is not intended for every Telegram
  user who can reach it.
- The adapter trusts the configured agentd and audio endpoints. It is not an
  isolation boundary for mutually distrustful operators.

The security workflow checks dependency advisories, licenses, and sources and
scans the tracked source tree for secrets. The repository history is also
scanned before a private repository is made public.
