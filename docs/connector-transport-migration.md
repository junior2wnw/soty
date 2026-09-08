# Connector transport migration and rollback

This update starts from production c0ca5b810d87e3da73f4ddba6dc98b1e8ac387a0. It changes connector persistence/delivery and browser request identity. It does not change the installed1.2.12 runtime, Roy/OpenCode bindings, upstream model, application credential format, traffic routes, or any other service.

## Authority and crash recovery

The serving worker exclusively locks a separate nonsecret SQLite owner file. SQLite data tables separate connector/grant records, job metadata, input/result bodies, events and request identity tombstones. Transactions use WAL and synchronous FULL; a successful response and notification follow commit. A proven rolled-back engine failure leaves memory/identity unchanged; an ambiguous commit fences reads/writes until restart. Full validation precedes replacement of legacy JSON by its database UUID/hash marker. Unknown schema, missing rows/results, invalid kinds, malformed records and unexpected competing authority never imply empty state.

Only one app process may own a data directory. Registry/status readers query committed SQLite without loading all event/input bodies. The official bridge discovers the same exact historical bootstrap proof, enforces its link/device/scope guards, persists request identity and uses bounded HTTP/reconciliation. Install its exact reviewed package before replacing Soty. Do not retain a rewritten legacy mirror or copy production history/auth into test fixtures.

Offline maintenance commands use the candidate image:

```
node server/connector-maintenance.js status
node server/connector-maintenance.js snapshot
node server/connector-maintenance.js enter
node server/connector-maintenance.js verify
node server/connector-maintenance.js rollback
node server/connector-maintenance.js leave
```

`status` is read-only and counts every status outside succeeded/failed/cancelled as active. The rollout keeps this early check to avoid knowingly stopping assigned work. It then confirms the exact original stopped and takes a strict complete offline `snapshot`. A live write-drain probe is diagnostic, not an admission prerequisite. Legacy c0ca successful mutation responses and delivered poll assignments follow write-to-temporary plus atomic rename. After a confirmed process stop, the renamed canonical JSON is authoritative; an unacknowledged operation is never automatically replayed. This is a process-stop preservation argument, not a power-loss guarantee: legacy writes did not fsync. The offline snapshot fsyncs the canonical file and directory before migration.

The snapshot validates every record and commits hashes of normalized complete state, canonical source bytes, counts/statuses and existing `.next` files. Pending `.next` bytes remain in place as unacknowledged evidence and are never adopted, removed or published. Unknown/malformed state, any nonterminal status, and queued jobs with prior attempts retain the stopped service for explicit reconciliation. They never trigger automatic legacy restart, because c0ca lease expiry could reoffer an already executing job. A failed legacy persistence call can leave a terminal result visible only in RAM through GET; such a GET is not a successful mutating ACK and the offline nonterminal gate must fence it.

`enter` obtains exclusive ownership and writes a maintenance marker bound to the complete snapshot. While it exists, the new server returns503 for every connector mutation before heartbeat/expiry/callback execution; read-only status and readiness remain available. Import checks the exact legacy hash and complete SQLite readback against the marker before replacing JSON. `verify` then compares the full migrated authority and retained temporary evidence before `leave`. This prevents unchanged clients from changing records during the proof. The rollout checks original configuration fingerprints and creates the stopped candidate first. Removing maintenance is the automatic rollback boundary. Docker daemon, other containers, clients, Caddy and unrelated work are outside its mutation scope.

Before admissions reopen, rollback first proves the complete state still matches the stopped snapshot, then exports compatible JSON and removes SQLite data files. A nonsecret fsynced intent records database UUID and expected export hash before any authority change. All new readers/startups refuse that intent until the same offline rollback resumes and verifies it. The rollout rechecks the exported full-state hash and retained temporary evidence, clears maintenance while both servers are stopped, and only then starts the exact original. Changed/uncertain state remains fenced. Every export/delete crash window is tested. An interrupted atomic schema import with a genuinely empty SQLite database can recover from its validated intact legacy JSON. Malformed nonempty databases remain for explicit investigation.

Rollback refuses active work or any newly accepted request identity: the old server cannot preserve that contract. After admission, use a transport-compatible previous image or a separately reviewed reconciliation/migration; never point c0ca at a SQLite marker. No plaintext secret-bearing backup is created. The temporary legacy export exists only inside the offline atomic disposition, with restricted permissions, and the intent exposes no credentials/history. `leave` removes the marker only after the selected server is verified healthy.

Automatic legacy restoration also refuses a baseline containing retained `.next` evidence. c0ca reuses `connector-store.json.<pid>.next`, so its next heartbeat could overwrite those bytes. A successful forward SQLite migration preserves them; a failed forward release keeps maintenance and requires explicit recovery instead of restarting c0ca over the evidence.

See [versioned rollout](../deploy/connector/README.md) for exact guards and supervisor receipt. CLI journals contain hashes, container IDs and phases only. The existing traffic rollout is unsuitable because it reconstructs unrelated environment/mounts.

## Operational limits

This is not exactly-once execution across arbitrary legacy client crashes. CurrentUser1.2.12 may fail to submit an actual native result. Such a job remains explicitly uncertain, never auto-requeued or claimed stopped; reconcile the same ID with known native evidence. Existing full-owner artifact reads can still be large by explicit request. Status/ACK and incremental event polling are bounded. Request IDs never silently expire; after100000 identities, admission fails with503 until a reviewed retention/reconciliation policy is implemented.

SQLite and worker APIs are built into the supported runtime: [Node SQLite](https://nodejs.org/api/sqlite.html) and [worker threads](https://nodejs.org/api/worker_threads.html). A local filesystem with working SQLite locks/fsync is required; the candidate must be tested on the actual Linux image/runtime before supervisor rollout.
