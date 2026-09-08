# Connector transport migration and rollback

This update starts from production c0ca5b810d87e3da73f4ddba6dc98b1e8ac387a0. It changes connector persistence/delivery and browser request identity. It does not change the installed1.2.12 runtime, Roy/OpenCode bindings, upstream model, application credential format, traffic routes, or any other service.

## Authority and crash recovery

The serving worker exclusively locks a separate nonsecret SQLite owner file. SQLite data tables separate connector/grant records, job metadata, input/result bodies, events and request identity tombstones. Transactions use WAL and synchronous FULL; a successful response and notification follow commit. A proven rolled-back engine failure leaves memory/identity unchanged; an ambiguous commit fences reads/writes until restart. Full validation precedes replacement of legacy JSON by its database UUID/hash marker. Unknown schema, missing rows/results, invalid kinds, malformed records and unexpected competing authority never imply empty state.

Only one app process may own a data directory. Registry/status readers query committed SQLite without loading all event/input bodies. The official bridge discovers the same exact historical bootstrap proof, enforces its link/device/scope guards, persists request identity and uses bounded HTTP/reconciliation. Install its exact reviewed package before replacing Soty. Do not retain a rewritten legacy mirror or copy production history/auth into test fixtures.

Offline maintenance commands use the candidate image:

```
node server/connector-maintenance.js status
node server/connector-maintenance.js enter
node server/connector-maintenance.js rollback
node server/connector-maintenance.js leave
```

`status` is read-only and reports only active IDs/status/count. `enter` and `rollback` require exclusive ownership, the exact serving container stopped, and no queued or assigned jobs. The old c0ca process does not understand the maintenance marker: the rollout first prechecks, stops the exact original, and then performs an offline recheck before import. A job arriving before that barrier aborts replacement and restores the original without cancelling the job. A supervisor must establish absence of pending old writes; active-count alone cannot prove it.

The new server honors the marker for create/lease admission while register/events/results/cancel remain available. The proposed rollout checks original configuration fingerprints, creates a stopped candidate before stopping the original, verifies storage readiness, and only then removes the marker. The last step is the automatic rollback boundary. It does not restart the Docker daemon, other containers, clients, Caddy or unrelated work.

Before admissions reopen, rollback exports the same complete state to compatible JSON and removes the SQLite data files. A nonsecret fsynced intent records database UUID and expected export hash before any authority change. All new readers/startups refuse that intent until the same offline rollback resumes and verifies it. Every export/delete crash window is tested. An interrupted atomic schema import with a genuinely empty SQLite database can recover from its validated intact legacy JSON. Malformed nonempty databases are retained for explicit investigation; they are never erased automatically.

Rollback refuses active work or any newly accepted request identity: the old server cannot preserve that contract. After admission, use a transport-compatible previous image or a separately reviewed reconciliation/migration; never point c0ca at a SQLite marker. No plaintext secret-bearing backup is created. The temporary legacy export exists only inside the offline atomic disposition, with restricted permissions, and the intent exposes no credentials/history. `leave` removes the marker only after the selected server is verified healthy.

See [versioned rollout](../deploy/connector/README.md) for exact guards and supervisor receipt. CLI journals contain hashes, container IDs and phases only. The existing traffic rollout is unsuitable because it reconstructs unrelated environment/mounts.

## Operational limits

This is not exactly-once execution across arbitrary legacy client crashes. CurrentUser1.2.12 may fail to submit an actual native result. Such a job remains explicitly uncertain, never auto-requeued or claimed stopped; reconcile the same ID with known native evidence. Existing full-owner artifact reads can still be large by explicit request. Status/ACK and incremental event polling are bounded. Request IDs never silently expire; after100000 identities, admission fails with503 until a reviewed retention/reconciliation policy is implemented.

SQLite and worker APIs are built into the supported runtime: [Node SQLite](https://nodejs.org/api/sqlite.html) and [worker threads](https://nodejs.org/api/worker_threads.html). A local filesystem with working SQLite locks/fsync is required; the candidate must be tested on the actual Linux image/runtime before supervisor rollout.
