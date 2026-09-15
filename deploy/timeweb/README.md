# Soty on Timeweb

This deployment moves the existing Soty server and its inference connector as
one service. `xn--n1afe0b.online` and `soty.pochinit.online`, application tokens,
registered devices, room data, job history and durable request identities stay
the same. The templates are preparation, not evidence of a completed cutover.

## Release and host preparation

- Use a supported Linux host with Docker Engine and Compose 2.30 or later.
  `env_file.format: raw` prevents Compose from interpreting dollar signs in the
  existing secrets. Do not print `docker compose config` with live secrets.
- Add the public SSH key named `soty` to the Timeweb server. Keep the private
  key on the operator's computer. Verify SSH access before changing DNS.
- Build the app from this commit with `docker build --build-arg REVISION=<full
  commit> -t soty-online-chat:<release> .`. The Dockerfile fetches and verifies
  Xray itself; a pre-existing local traffic image is no longer required.
- Build the gateway with `docker build -t soty-traffic-gateway:26.7.11
  deploy/traffic`. Select a supported Caddy release and record its digest.
- Set `SOTY_IMAGE`, `SOTY_TRAFFIC_IMAGE` and `SOTY_CADDY_IMAGE` to verified image
  IDs/digests in the deployment shell. Record the app's OCI revision and all
  image IDs in the release receipt. Never use an unverified mutable `latest`.
- Create `/etc/soty` and `/srv/soty/data` with mode 0700. Restore the original
  environment and application-token file to `/etc/soty`, mode 0600.
  Copy all source environment settings; apply only the reviewed inference and
  internal-network overrides. Do not generate new application tokens.
- Preserve the existing `/data/traffic-exit-pool.json` exactly. Generate the
  gateway config from that verified pool and the existing legacy gateway
  config where applicable. Reject a missing/invalid pool before running the
  generator: its generic default creates new credentials. Use the existing
  slot count rather than shrinking the pool. Place the result at
  `/etc/soty/traffic-server.json`; the non-root gateway needs read permission
  on this file, while its host parent directory remains 0700. The app restores
  active client credentials/routes from the migrated traffic-control state.
- Open public TCP 80/443 and UDP 443, plus the chosen SSH management access.
  The app's diagnostic port binds localhost; gateway and control ports have no
  public mappings. Do not disable the host's other proxies or services.

## Transfer without divergent storage

The data directory is an authority, not a disposable cache. See also
`docs/connector-transport-migration.md` and the existing connector maintenance
commands. Two serving processes must never own the same directory, and two
hosts must not accept writes into separate copies of it.

1. Prepare and test the destination with synthetic data. Test the image,
   Caddy configuration, WebSockets and gateway before interrupting production.
   Do not point production agents at a test copy of the registry.
2. Establish valid TLS on the destination before HTTPS cutover. While DNS
   still points to the old server, its HTTP ACME challenge path can be
   forwarded to the new Caddy. Keep all unrelated virtual hosts unchanged.
3. Read maintenance status on the source. Wait for assigned/running jobs to
   finish; do not cancel jobs or delete queue/history to make this check pass.
   A never-leased queue can only be preserved with its exact verified
   fingerprint using the existing maintenance command.
4. Temporarily stop new incoming writes at the old Soty virtual hosts, allow
   accepted requests to settle, then stop the exact serving app container.
   Recheck the stopped container/image ID and offline maintenance status.
   Enter maintenance with the compatible image before taking the final copy.
   If the check fails, resume the source and investigate; do not force it.
5. Take a consistent cold archive of the whole data volume, environment,
   application-token file and required gateway configuration. Encrypt it
   before writing it as a backup. Transfer over authenticated SSH. Preserve
   permissions and every SQLite database/WAL/journal file; never copy a live
   database with ordinary file copying. Keep the decryption key outside the
   archive and outside Git, encrypted for the operator.
6. Restore into an empty destination with no application process running.
   Compare file counts, sizes, content hashes and secrets locally and report
   only equality/count results. Verify SQLite integrity and registry, job,
   durable-request and room counts before admitting traffic.
7. Leave maintenance on the destination with the compatible image and start
   the app. Check `/ready`, then perform authenticated synthetic inference
   and connector checks. `/ready` validates storage/configuration, not actual
   inference availability. Once the destination accepts real writes, its
   data becomes authoritative.
8. Point the old Soty HTTPS virtual hosts at the new server over verified TLS
   (preserve the original Host header). This handles clients with cached DNS
   and makes the new server the sole writer. Validate this path before
   updating the two domain A records at REG.RU. Inspect AAAA records as well;
   retain them only if they route to the new deployment. Do not change MX,
   TXT, NS or unrelated domains.
9. Verify authoritative DNS and independent public resolution, HTTPS,
   WebSockets, existing application keys and registration continuity. Keep
   the old forwarding endpoint during DNS propagation. Retire old serving
   containers only after confirming the final state and encrypted recovery
   material. Never restart the stale source volume after new writes arrive.

Rollback after step 7 must preserve the newest authoritative data. Use a
compatible image on that data, or transfer it back offline. Reverting DNS and
restarting an old snapshot would lose acknowledged work.

## Inference behavior

`inference-settings.example` selects the previously verified JoinGonka key as
primary and the paid OpenBroker key as fallback, both using
`MiniMaxAI/MiniMax-M2.7`. DeepSeek and MiniMax client aliases keep working; this
does not require reissuing `connector.env` or changing connected agents.

- Upstream SSE is also used for ordinary JSON clients. The connector assembles
  the final JSON and preserves usage, Unicode, reasoning and tool arguments.
- JSON requests have a 75-second total budget, allowing an HTTP error before
  a caller's 90-second cURL deadline. The first meaningful output has a
  separate 20-second limit. Healthy SSE can run up to 30 minutes; a 30-second
  gap without meaningful output ends a stalled stream.
- Each provider is attempted at most once. Before visible output, transient
  errors or a stalled response can use the fallback. After text or a tool
  call is visible, the request is never replayed. Interrupted responses carry
  an error, without a false `[DONE]` marker or successful JSON result.
- A provider has eight active slots and a bounded 64-request queue with a
  five-second wait. These are concurrency controls, not a fixed RPM cap.
  Three consecutive transient failures pause a provider for 30 seconds;
  `Retry-After` can extend the pause. One probe tests recovery.
- Client disconnection cancels work. Caddy uses automatic SSE flushing rather
  than negative `flush_interval`, which would prevent prompt cancellation.
  Its 80-second response-header deadline leaves the connector room to return
  its controlled error. The 30-second proxy keepalive is shorter than Node's
  65-second idle lifetime.
- Optional JSON metrics contain request ID, provider label, duration, first
  output time and safe failure code. They exclude prompts, answers, tokens
  and provider account/balance data. Response header `X-Soty-Request-Id` helps
  correlate client failures with these metrics.

Failover cannot guarantee capacity or zero errors, and a canceled upstream
generation may still be billed. This configuration uses bounded failover;
it does not race multiple paid generations or silently truncate user context.

## Validation and receipt

Run `pnpm run inference:selftest`, `pnpm run typecheck` and `pnpm run build`.
Exercise connector storage/integration tests before deploying a new image.
Validate the Compose and Caddy configuration using synthetic environment
files first, then `docker compose config --quiet` against the private files.

Record the destination server and region, SSH fingerprint, Git revision,
image IDs, encrypted archive checksum, equality/integrity results, DNS/TLS
checks and authenticated JSON/SSE/tool-call outcomes. Keep credentials and
user content out of the receipt. Commit and push the reviewed Soty changes;
claim the migration complete only after public checks pass.

References: [Timeweb SSH keys](https://timeweb.cloud/docs/cloud-servers/manage-servers/ssh-keys),
[Caddy proxy lifecycle and timeouts](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy),
[Compose environment files](https://docs.docker.com/reference/compose-file/services/#env_file).
