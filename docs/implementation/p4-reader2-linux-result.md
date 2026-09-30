# P4-B1b — actual Linux reader2 canary

30.09.2026. Root однократно исполнил exact [review1 artifact](p4-reader2-linux-review.md) после собственного полного чтения controller/fixtures и независимого static review. Bundle **232892 B**, SHA256 `44e2e7a91b8140435d157105805ee442542afba09e759f9e09a344277e49943e`. Оба reviewer сверили prefix/trailer, embedded source hashes и реконструкцию writer/audit. Bundle не импортировался/не исполнялся при review.

## Выполненный проход

Single-send transport использовал прежний документированный SSH alias `dev` и существующий Node transport; stdin содержал только exact bundle. Local exclusive/fsynced intent исключает случайное повторение. Новый wrapper `p4-reader2-linux-send-once.mjs` SHA256 `1a439af09c4a1c7fad0fa39010d67cfa5ed007945734a8bd29aa1197d5822a38`, один bounded262144B stdout buffer, stderr только count/hash. Старые canary/send-once не запускались.

Run **389c0e7bf411eee8fc66d6d0c2b94a41**, 16:53:21–16:53:31 UTC; remote9260ms. SSH exit0, no timeout/input/spawn/parse/overflow errors, stdout13324B, stderr0. Receipt `output/implementation-20260930/p4-reader2-linux-review1.linux-result.json`, SHA256 **`2aeb2c256127cffc90617834b235858ec0fb70b46d4bee02df38617d7a88beb3`**. Remote journal сохранён в `/tmp/soty-reader2-canary-389c0e7bf411eee8fc66d6d0c2b94a41/receipt.json` с task-only0700/0600; это receipt, не приложение/база пользователей.

| Phase | Проверенный результат |
|---|---|
| Runtime | Exact helper image, Linux Node24.15.0 / SQLite3.51.3, FTS5 PASS |
| Writer | Genuine literal1/checkpoint main1, committed v2 WAL и seeded native history. Ready только после UID/GID10001, files0600/directories0700; единственный external KILL, exit137 и OOMKilled=false |
| Current CLI | Notes2/Capabilities2, Rooms/Apps empty, envelope `soty.storage-format.v3` |
| Matrix | Mixed1/2 и2/1 принимаются; future3 → unknown; missing/altered guard и два real Linux file symlink → unreadable |
| Historical parser | Exact reader1 отвергает primary/mixed v2 как unknown |
| Read-only audit | 30 main/WAL/SHM files, 2 links,22dirs;2776680B. Все main/WAL hashes и SQL control hash прежние; foreign ownership PASS после RO probes и close |
| Actual unknown image guard | `storage_reader_unknown`, container не стартовал. Pure `[1,2]` compatibility test отдельно от actual image attestation |

SQL witnesses: nativeNotes4/nativeLedgers4, ordinaryNotes1/ordinaryLedgers1,8immutable proofs,7FTS rows,reserved5/spent5. Proofs/receipts здесь **seeded synthetic reader fixtures**, не повторная проверка бизнес-операции Notes и не cross-store consistency production. SHM существует и имеет нужные owner/mode/size; неизменность SHM bytes не обещается.

## Cleanup и рабочий сервис

Mutation attempts: **1 volume CREATE,7 container CREATE,6 START,1 KILL,0 STOP,7 unforced container DELETE,1 unforced volume DELETE**. Все удаления подтверждены GET404; cleanupComplete=true, needsReconciliation=false. У всех шести terminal programs OOMKilled=false. Runtime/reader/audit были network-none, RO data mount; единственный writer — RW нового owned volume с CapAdd DAC_READ_SEARCH+CHOWN. Остальные — DAC_READ_SEARCH; no FOWNER/privileged/app startup.

До каждой mutation и в конце проверен тот же работающий `soty-online-chat`: container `d86bc0b9b8f88a7693a227cca0b15864ac5388a8c6e478f25e0f1c3c7d9c4ceb`, image `sha256:d07345cb66b2c1ab903861f36902d7b301cc25289778d0d178c81143281ca97e`, StartedAt `2026-09-28T01:15:45.442670947Z`. Serving unchanged=true; production volumes не подключались к probes.

Закрыт Linux reader2 fixture gate: реальный foreign UID/modes, symlinks,128MiB limit, RO WAL/SHM и bounded transport. **Full application image START2, cold bootstrap, coordinated encrypted backup/isolated restore и public release остаются отдельными gates.** Unlabelled serving image не становится совместимым reader2 от этого опыта. OAuth3 потребует своего reader-before-writer passage после literal DDL/source freeze.
