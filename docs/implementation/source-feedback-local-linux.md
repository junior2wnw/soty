# Source jobs: отдельный local Linux placement

Новый закрытый host profile `soty.feedback.local-linux-wsl.v1` живёт отдельно от frozen Dev profile2/packet823d. Root/Std1/Std2/Source Native formats не меняются. Это synthetic lab, `models:false`, `productionReady:false`; actual joint gate ещё не выполнен.

Approved placement literals:

- lab `/home/junio/codex-soty-universal-local-20261007-4c8284c7`, UID1000/mode700; writable **только** новый child `source-feedback-jobs`;
- host CLI `/home/junio/codex-soty-universal-local-20261007-4c8284c7/tools/docker/docker`; fixed argv `--host unix:///home/junio/codex-soty-universal-local-20261007-4c8284c7/source-docker.sock`;
- socket GID1001/mode660; supervisor mount→`/run/soty-docker.sock`, readonly CLI→`/usr/bin/docker`; supervisor runner uses fixed `--host unix:///run/soty-docker.sock`, PATH-only environment. Author JSON/DOCKER_HOST/context do not select daemon;
- worker/toolcheck image remains exact verified `sha256:c03a61d12e03870747e9013860fc36e23e53920da9b07ea1e795b3fef9628ae6`; transfer must preserve actual OCI ID/content. A **fresh whole-green Root image** runs supervisor and supplies exactly four independently hashed public runtime imports.

New engine3 semantic digest pins placement, retention lifecycle commit `208f6af852199de82201d5e650b276a8db82e729`, worker source/image/scenario and unchanged caps. Dev engine2 literal success digest remains `4f32d83bd29982c78037b37e4f2e04241d701b5249e569704708725a781d03a3`. Old grants/profile2 are not repurposed into local engine3.

Supervisor512MiB/pids128/cpus1, private tmpfs256MiB +data1MiB; packet/CLI/lab-parent stub readonly, only exact jobs child RW. Worker has **no socket**, read-only/no-network/no-caps/no-new-priv,128MiB/pids32/cpus1, bounded scratch/data/tmp. Fixed timeout/prlimit/Node worker, actual integer RLIMIT_CPU1..10seconds; fractional or <1s is not-ready. NanoCpus limits speed, not CPU time. Source policy/admitted budgets unchanged; seven synthetic cases use5s wall/1s CPU. No argv/model/user media/Source secrets enter processor.

New scripts `prepare-linux-feedback-local-jobs.mjs`, `fixtures/linux-feedback-local-supervisor.mjs`, `fixtures/linux-feedback-local-jobs.mjs` reuse the same actual signed Root/Human +installed connector +Native SQL path. Same seven cases:success,CPU,wall,local cancel,scratch/output bound,second-OS Native session revoke after real Docker RUNNING. That last case requires250ms Source SQL monitor to abort the owned executor before wall,zero processor receipt/unknown/no rerun; it is bounded detection, not instantaneous distributed revoke.

Before START the host guard verifies exact manifest/source/CLI/image/import pins, socket/mounts/spec. Unknown CREATE never retries; exact proved own container alone may be cleaned. Unknown stop/removal retains the exact owned packet/mounts, returns no Native output and reports cleanupUnknown. Completed process output reaches final Native SQL only after actual stop+removal+packet cleanup; late current-role/Root denial becomes unknown.

Local constructor/guard/lifecycle tests14/14 PASS, including real owned child CLOSE. This is preparation evidence; it does not replace actual Linux joint, Reader3 physical cold, models/quality or production/Windows readiness. Dev heavy RUN remains blocked by capacity. Old failed/frozen packets are retained unchanged.
