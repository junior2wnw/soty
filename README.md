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

For production, prefer an owner-only file mounted read-only into the server
container and set `SOTY_GONKA_APPLICATION_TOKENS_FILE` to its container path:

```json
{
  "applications": [
    { "id": "kvartalufa", "token": "<40-160 character base64url token>" }
  ]
}
```

The rollout script persists `${HOME}/.config/soty/application-tokens.json`
with mode `0600` and mounts it at `/run/secrets/soty-application-tokens.json`.
An explicitly configured missing or invalid file, duplicate application ID,
or token reused by multiple applications fails the application proxy closed.
The legacy `SOTY_GONKA_APPLICATION_TOKENS` environment map remains supported;
conflicts between the two sources also fail closed.

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


## Connector storage and delivery

Server runtimes require Node 22.13+ (the production image uses Node 24). Connector state now lives in `connector-store.sqlite`: WAL transactions with synchronous=FULL run in a dedicated worker. Heartbeats update only changed records; normal reads use the last committed in-memory state and do not wait behind pending writes. A separate nonsecret `connector-owner.sqlite` holds an OS-released exclusive writer lock; a second serving process fails closed. Read-only registry tools do not acquire that writer lock.

On first startup an existing v1/v2 JSON store is strictly validated, atomically imported, read back and replaced by a small nonsecret marker. The original full JSON is not continually mirrored. Malformed input, partial jobs/native results, invalid identity/grant bindings, divergent JSON/SQLite authority, and an interrupted rollback stop readiness without resetting history or credentials. Explicit offline retrieval/rollback and the supported configuration-preserving rollout are described in [connector transport migration](docs/connector-transport-migration.md).

Create accepts an optional `requestId` (1–128 ASCII letters/digits/`_.:-`). It is bound to owner/grant/target and normalized content; replay returns the same job even after completion or restart, a changed request returns 409. Retired identities return 409 instead of re-executing. The ledger admits at most 100000 identities; capacity returns 503 without evicting unresolved or retired keys. Browser and corporate bridge persist their caller key before dispatch. Legacy callers remain valid, but an unkeyed caller must reconcile a lost response before creating another job.

CurrentUser 1.2.12 remains unchanged. It can execute after a lost start acknowledgement, so neither an expired lease nor a missing start event permits automatic redelivery. The existing assignment stays pinned and status exposes `executionUncertain`; later authenticated events/native results reconcile the same job. A cancellation request is not a confirmed stop; only the connector's durable native result completes an assigned job. First committed terminal results are immutable.

Mutation acknowledgements and connector cancellation-watch responses omit event history/result text and include an artifact URL. Owner `GET /api/connectors/jobs/:id?view=summary` is bounded; the compatible full GET/`?view=full` explicitly retrieves the complete retained result and events. Event polling pages by sequence (at most32 events/128KB, except one event); `done` stays false until all available events are drained and the final page includes the complete native result. Existing retention remains512 events per job, seven days for finished jobs and2000 retained jobs. Assigned uncertain work is retained until reconciled rather than invented as a timeout result.

Committed notifications and missed-wake versions are scoped to the authorized link/device. Untargeted jobs wake eligible devices on their own link; targeted traffic never wakes another device or link.

Storage readiness is `/api/connectors/storage-ready`; health alone does not prove successful migration. The corporate bridge must be upgraded to the version using `server/connector-registry.js` before migration; old clients of the raw JSON file cannot read the marker. Never run the legacy traffic rollout script for this update.

Relevant deterministic checks:

```
node scripts/connector-persistence-selftest.mjs
node scripts/connector-route-isolation-selftest.mjs
node scripts/connector-durable-protocol-selftest.mjs
node scripts/connector-transport-load-selftest.mjs
node scripts/connector-request-journal-selftest.mjs
node --test deploy/connector/rollout.test.mjs
pnpm run connector:selftest
pnpm run connector:integration
pnpm run identity:selftest
pnpm run traffic:selftest
pnpm run typecheck
pnpm run build
```

The load fixture is synthetic (320 finished jobs,80 events each,61.7MB legacy JSON); its explicit100MB/s persistence-cost model is not a measurement of the production disk. Separate loopback tests execute the unchanged client with dropped create/start/result responses and independent cancellable children. Production deployment still requires the exact image, bridge transition, disposable Linux rollout proof and supervisor receipt.
### Optional application-specific native models

`SOTY_GONKA_APPLICATION_MODEL_POLICY_FILE` may name a separate read-only, nonsecret JSON file with schema `soty.application-model-policy.v1` and `applications: [{id, allowedModels}]`. With no file configured, applications and connectors retain the existing DeepSeek-only binding. A valid explicit application rule may allow the DeepSeek default and/or `MiniMaxAI/MiniMax-M2.7`; unconfigured applications keep DeepSeek, and connector policy is unchanged. Invalid or unreadable configured policy fails application requests/readiness closed. The file is parsed at startup; no runtime reload or fallback is performed. Do not add model fields to the strict application token file. See [the scoped proposal, validation and rollback contract](docs/application-model-policy-proposal.md).
