# P4-C1b — actual Linux reader3 canary

01.10.2026 (20:18:37–20:18:49 UTC30.09). Root однократно выполнил новый [review1 artifact](p4-reader3-linux-review.md) после собственного source/bundle review и [независимой проверки](p4-reader3-linux-independent.md). Bundle314647B, SHA256 `1cd3ea95f41084f4e9d3afda939516838b8a6a56946bc1416bf80efad07db46e`; fixed run `be39dd82e9fbc77beab99126007f5684`. Старые bundles/send-once не запускались.

Root прочитал controller, его delta от принятого reader2, fixture writer/audit и отдельно статически восстановил точный prefix+JSON+trailer, pinned Git bytes, writer/audit и прежние command limits. Literal schema3 сравнен с двумя cooked константами исторического модуля через TypeScript AST, без исполнения historical source/bundle. Первая версия этого дополнительного validator ошибочно предполагала одну SQL-константу; исправлен validator, payload неизменён. `p4-reader3-linux-root-review.mjs` итоговый PASS. Независимый reviewer выполнил собственную статическую сверку.

## Единственный запуск

Read-only preflight подтвердил exact serving ID/image/StartedAt и отсутствие reader label у actual image. Новый sender `p4-reader3-linux-send-once.mjs`, SHA256 `2eb62a3f1bd840af3d96675c892ba2413119d5d4752d9060bf417fad73a6a328`, передал exact bundle через прежний документированный SSH alias `dev`/Node transport. До отправки записан fsynced exclusive local intent; повтор не разрешён. Собственный fixed remote namespace также не допускает повторного запуска до Docker mutations.

**Remote PASS,8824ms**. SSH exit0, stdout12800B, stderr0, без timeout/spawn/input/parse/overflow errors. Локальный result `output/implementation-20260930/p4-reader3-linux-review1.linux-result.json`, SHA256 `ea1f96dd5d3f7da21e8967b8c1ff7fc1e0f6ef4da1f704be237bb087d3319f71`. Remote journal: `/tmp/soty-reader3-canary-be39dd82e9fbc77beab99126007f5684/receipt.json`.

| Phase | Actual result |
|---|---|
|Runtime|Linux Node24.15.0 / SQLite3.51.3 / FTS5 PASS в exact helper image|
|Writer|8 genuine main/WAL databases; Caps main2 → logical3, synthetic native/OAuth witnesses; owner10001:10001, directories0700/files0600. Единственный KILL после READY, exit137/OOMKilled=false|
|Exact reader3 CLI1|`/data`: Notes2/Caps3, Rooms/Apps empty|
|Fixed matrix|Notes1/Caps3 и Notes2/Caps2 принимаются; futureCaps4 → unknown, missing OAuth guard → unreadable|
|Exact historical reader2|Primary/mixed13 отвергаются как unknown; baseline22 принимается|
|Read-only audit|24 files/14dirs/0links,2920480B; main/WAL hashes и SQL controlHash неизменны, включая close RO handles; foreign ownership PASS|
|Actual unknown image guard|`storage_reader_unknown`, седьмой container никогда не START; copied container label не заменяет image metadata|

Native witnesses:2 Notes stores,5 ledgers,4 proofs,5FTS rows,reserved5/spent5. OAuth witnesses:4 connections,4approved interactions,4credential links,24artifacts. Это seeded synthetic reader data, не выданные tokens и не расшифровка OAuth. SQL consistency/bytes проверены, но business effect повторно этим canary не исполнялся; SHM bytes не объявляются неизменными.

## Очистка и действующий сервис

Подтверждены **1volume CREATE,7container CREATE,6START,1KILL,0STOP,7unforced container DELETE,1unforced volume DELETE**. Every owned removal завершён GET404; cleanupComplete=true, needsReconciliation=false. Все6 terminal containers имеют OOMKilled=false. Единственный RW mount — новый owned synthetic volume; production volumes не подключались. Контейнеры network-none/rootfsRO, ограничения128MiB/0.5CPU/16PIDs, остальные mountsRO. Remote receipt остаётся для проверки.

Serving unchanged=true: `d86bc0b9b8f88a7693a227cca0b15864ac5388a8c6e478f25e0f1c3c7d9c4ceb`, image `sha256:d07345cb66b2c1ab903861f36902d7b301cc25289778d0d178c81143281ca97e`, StartedAt `2026-09-28T01:15:45.442670947Z`. Identity проверялась перед mutations и в конце, restart/config mutation рабочего сервиса не было.

Закрыт actual Linux reader3 fixture gate. **Полный application image3 START/fallback, первоначальный cold bootstrap, зашифрованный all-store backup с настоящим isolated restore, production migration и HTTPS release ещё открыты.** Compatibility pure declaration тестировалась отдельно от actual image attestation; текущий unlabelled image не получил автоматически новых полномочий чтения. C1/C2 и весь master продолжаются.
