# soty.online

Soty is a PWA for long-lived encrypted links between devices. Text, files,
remote terminal output, wake signals, agent tasks, and explicitly granted
traffic routes use the same trusted relationship.

Soty does not implement an AI agent and is not a universal agent hub. It uses
one production agent: [OpenCode](https://github.com/anomalyco/opencode). The
installed Soty Agent pins and verifies OpenCode, starts it on the selected
computer, and sends its model calls through an authenticated Soty server proxy
to Gonka AI's OpenAI-compatible Chat Completions API. The Gonka key never goes
to the user's computer.

## Architecture

- `src/` — PWA, encrypted links, files, terminal UI, and agent client.
- `server/` — room relay plus durable jobs, ordered events, cancellation, and
  device presence.
- `scripts/soty-connector.mjs` — installed Soty Agent runtime: transport,
  lifecycle, OpenCode launcher, terminal, and traffic capability.
- `scripts/agent-modules/opencode-release.mjs` — pinned official OpenCode
  releases and SHA-256 checksums.
- `public/agent/` — generated runtime release and OS installers.
- `trustlink-kernel` — reusable protocol and Web Crypto primitives.

The complete boundary and migration contract are in
[`docs/architecture.md`](docs/architecture.md).

The shared Identity checkpoint is intentionally separate from room/device
trust. Soty is not the central IdP and does not publish the canonical wire
contract; its off-by-default experimental relying-party adapter, ADR, and
threat model start at
[`docs/identity/architecture-checkpoint.md`](docs/identity/architecture-checkpoint.md).
The direct `STABLE` wire-v1 consumer and ownership boundary are documented in
[`docs/identity/canonical-identity-wire-v1-adapter.md`](docs/identity/canonical-identity-wire-v1-adapter.md).

## Run

```bash
pnpm install
pnpm run typecheck
pnpm run build
pnpm start
```

Default server port: `8080`. For local agent development:

```bash
pnpm run agent
pnpm run agent:ctl -- health
```

The server requires the Gonka configuration:

```json
{
  "SOTY_GONKA_API_KEY": "...",
  "SOTY_GONKA_BASE_URL": "https://gate.joingonka.ai/v1"
}
```

The model is fixed to `deepseek-ai/DeepSeek-V4-Flash-0731`. The installed
runtime authenticates to `/api/connectors/gonka/v1/chat/completions` with its
existing connector token. Arbitrary model IDs and unauthenticated calls are
rejected before they reach Gonka.

Server applications use a separate least-privilege endpoint instead of an
installation token. Configure a JSON object of application tokens in the
server environment:

```json
{
  "SOTY_GONKA_APPLICATION_TOKENS": "{\"peremetrika\":\"<40-160 character random token>\"}"
}
```

The application server sends that token to
`/api/inference/v1/chat/completions`. The browser never receives this token or
the upstream Gonka key. Application and connector tokens are not
interchangeable.

## Verification

```bash
pnpm run agent:selftest
pnpm run agent:release:selftest
pnpm run agent:integration
pnpm run identity:selftest
pnpm run traffic:selftest
pnpm run typecheck
pnpm run build
```

Set `SOTY_OPENCODE_E2E_PATH` to an OpenCode executable when running the
integration test to additionally exercise the real OpenCode → Chat
Completions protocol through the Soty server proxy against a local fake Gonka
endpoint.

`public/agent/soty-agent.mjs` is a byte-identical migration asset for installed
0.x runtimes. Direct current-user installations migrate through the normal
update channel. The legacy Windows machine split install must run the elevated
machine installer once because its old launcher restores the ProgramData copy
before every restart; the new installer removes that launcher and future
updates are automatic. The compatibility asset can be removed after that
installed population has upgraded.
