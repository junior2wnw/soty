# Soty connector maintenance rollout

This is a separate Soty-only path. It does not import or execute deploy/traffic/rollout-server.sh. It never prints Docker inspect/config/Env/logs, and never writes an env-file. Original Config, HostConfig and requested network endpoint settings are cloned in memory; only Image and transaction ownership labels change. Anonymous data volumes are explicitly reattached by their inspected volume name. Candidate config is compared again after Docker creation/start, so unexpected daemon defaults fail closed.

No real Docker or production proof is claimed by the unit suite. Before using this code, root/controller must review an exact integrated image (including server/connector-maintenance.js) and run it against disposable synthetic Docker containers/storage, including fault cases. Candidate application image must have org.opencontainers.image.revision equal to the exact reviewed commit. Existing application tokens, auth, traffic, mounts, resource settings, ports, client and 3D processes remain outside rollout mutation scope.

## Operator CLI

Run on the server host with its existing Docker Unix socket and Node24, after supervisor review. Arguments contain IDs/hashes, never credentials:

```
node deploy/connector/cli.mjs prepare --original-id ORIGINAL64HEX --original-image sha256:ORIGINAL64HEX --candidate-image sha256:CANDIDATE64HEX --revision COMMIT40HEX --transaction UNIQUE16TO40HEX --journal /private/rollout-journal.json
node deploy/connector/cli.mjs promote --original-id ORIGINAL64HEX --original-image sha256:ORIGINAL64HEX --candidate-image sha256:CANDIDATE64HEX --revision COMMIT40HEX --transaction UNIQUE16TO40HEX --journal /private/rollout-journal.json --reviewed-receipt /private/supervisor-review.json --health-origin http://127.0.0.1:18182
```

Supervisor receipt fields: approved:true, legacyNoPendingWritesObserved:true, originalId, candidateId, configurationSha256, revision. The CLI checks these against the inspected original, prepared candidate and exact config fingerprint. This file is an explicit review input, not a cryptographic owner-auth mechanism. Existing owner authorization and controller review are still required. Original must be named soty-online-chat and its exact immutable ID/image must match. Health origin must match the original loopback8080 port binding. The safe journal uses fsync+rename+directory fsync and a single-writer exclusive lock. A retained lock after a crash requires supervised investigation, not automatic deletion or a second rollout.

Prepare creates only the uniquely labelled stopped candidate. It does not enter maintenance or stop the serving container. Promote prechecks no active jobs, stops exact legacy original and confirms stopped, rechecks offline jobs (including queued/leased/running/unknown statuses), then invokes enter. A job created between the first check and stop aborts without migration and restarts the same old container. No job is cancelled. Old c0ca does not honor the marker: confirmed stopped old service is the admission barrier. The legacyNoPendingWritesObserved flag is an external, fresh quiescence prerequisite because legacy c0ca does not expose a pending-write metric; count0 alone does not prove no unacknowledged long write.

Temporary maintenance helpers run the candidate image command `node server/connector-maintenance.js status|enter|leave|rollback`; original Env/mount paths stay in memory. Helpers have no network/ports, readonly image root, no capabilities, 768MiB RAM, 0.5 CPU,64pids, bounded tmpfs. Status must be strictly read-only/no migration. Enter and rollback are offline. Rollback must return compatible history/auth JSON and refuse any accepted durable request IDs or active jobs. Final rollback receipt is validated for legacy-json/count0/bytes/SHA. Candidate readiness requires /health ok and /api/connectors/storage-ready `{ok:true,schema:"soty.connector-storage-ready.v1",storageReady:true,maintenance:true}` before leave.

## Failure boundaries

- Docker request waits have absolute10s bounds, and dropped responses are reconciled by exact owned ID/label plus observed state. Rename/start/stop confirmations never rely solely on response success.
- Before leave, ordinary failure stops the candidate, performs checked offline rollback when maintenance was entered, restores the original name and starts the SAME original ID, verifies its /health, then clears maintenance. Failure to prove this returns recovery_required, not restored.
- An unresolved timed-out Docker stop may still complete later. It is not safe to claim the still-running original is restored. An unresolved maintenance helper might still be mutating storage. Both paths retain the barrier and require supervised exact-ID readback; they do not start another helper or execute blind recovery.
- Leave is the admission commit boundary. If its response drops, status reconciles the marker. Once leave was attempted, this implementation conservatively refuses automatic downgrade on any unresolved outcome; active new jobs/request identities must not be discarded.
- No original/retained candidate container is automatically deleted. Successful short helpers are removed without force/volumes. Unknown/failed helper objects remain for exact-ID supervisor inspection. There is no cleanup of arbitrary names, host services, Docker daemon, Caddy or clients.
- Real Docker endpoint defaults, rootless/usernamespace mount access and static-IP reservations require disposable-container proof. A config fingerprint mismatch or unsupported AutoRemove/container-network original rejects the operation before cutover.

## Checks

`node --test deploy/connector/rollout.test.mjs` runs synthetic injected-engine tests, not real Docker. `node --check` on every deploy/connector/*.mjs verifies parsability. There is no dependency install or application rebuild for these standalone Node built-in modules. Root separately builds the integrated application image and tests real synthetic migration/readiness/rollback before controller acceptance.
