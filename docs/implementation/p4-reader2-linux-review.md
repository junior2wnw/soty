# P4-B1b — Linux reader2 artifact, локальная приёмка

30.09.2026. Выполнена только локальная подготовка по [плану](p4-reader2-linux-plan.md), SHA `8fc0a810b1a8cb5470e7fb5a4a90188a15aadc4a2da74d75b0cf99ab7c407b67`. B1b reader checkpoint принят root в `0c8db3dbdc4a428c68ff98be9597223abc4da699`. **Новый bundle не исполнялся на Linux, Docker или SSH.** Root отдельно решает remote run после собственного и независимого review.

Старые bridge review2/review3/cleanup artifacts и receipts не изменены. Review2 остаётся failed; предыдущий review3 PASS относится к Notes1/Capabilities1. Этот документ их не заменяет и не относится к application bootstrap, migration admission или cold restore.

## Файлы и provenance

Все executable/local-test файлы новые, находятся в ignored `output/implementation-20260930/`:

| Файл | Bytes | SHA256 |
| --- | ---: | --- |
| `p4-reader2-linux-canary.mjs` | 43790 | `8d3fcdceace3c53a46ed9deab69a105c07e150fe199af41a727000773c73938c` |
| `p4-reader2-linux-canary.fixtures.mjs` | 39018 | `5cd535268e12b33fe754eae8bb15c5bf2b9da4791f1ffe9dc8bb67bbe7187ff0` |
| `p4-reader2-linux-reader1.mjs` | 24317 | `d0aed27ce790502f36df20689663a43aee0ed28266e6b424a002cde7eb2f4e17` |
| `p4-reader2-linux-canary.test.mjs` | 41792 | `7a35653cb820cec04dec5585b3a6d0e1540fc73b67fc363fb7b3a61e1cce033d` |
| `p4-reader2-linux-review1.bundle.mjs` | 232892 | `44e2e7a91b8140435d157105805ee442542afba09e759f9e09a344277e49943e` |

`p4-reader2-linux-review1.manifest.json` содержит точные source hashes, measured command bounds, profile каждой phase и ожидаемые mutation counts. Bundle создан `wx`; overwrite и повторное prepare в тот же файл отвергаются. Remote trailer не запускался. Controller prefix, JSON payload и фиксированный trailer подлежат отдельному static review без import/исполнения artifact.

Точный current probe SHA `5d7bdb6cee8c38671ff9a188e26b4bc33e983fbef04ec3e3ac9cabd9338f650a`, guard `95e0a74b818a6bec44b0994109bc0f26019df6240e929be1da5a617ca3454cca`, transport `ef62aaf8a67b742350e5305784139ba20a3686f0b8116ea9904ad547adf024d3`. Prepare отказывает при несовпадении. Source schema/migration modules не входят в executable probe.

Fixture хранит самостоятельные literal DDL. V1 происхождение — `6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e`; additive2 — B1a `5e459abc6afa376861c2032226bd29f78bf0468d`. Notes2 literal2458 LF bytes/SHA `e15a1dc6d57dfe6c3de56e6d5052f42080955612f56b39a70f21fc8c93ff0add`; Capabilities2 literal5122 LF bytes/SHA `64c394ad739eba70bea95e2ffb18c29d2f0753b55c1b4373060620fde650a8c8`. Локальный test сравнивает все SQL objects с неизменённым historical dependency graph и проверяет default-off reopen populated2. Frozen bridge parser — точный Git LF blob `ae914f55e6d8d64628a7279189d2d55dfd05de45`, не импорт нового recognizer с ограниченным флагом.

## Матрица и область выполнения

Один новый namespace `soty-reader2-canary-<32hex>`, один отдельный Docker local volume. В нём10 независимых DB:

- Primary Notes2/Caps2; смешанные Notes1/Caps2 и Notes2/Caps1 — шесть DB.
- Genuine Notes2/Caps2 с последующим committed marker3 — две DB, ожидается `storage_format_unknown`.
- Notes2 без `note_native_create_no_delete` и Caps2 с подменённым `cap_native_note_admission` — две DB, ожидается `storage_format_unreadable`.
- Две дополнительные фиксированные file symlinks на primary main того же volume; новый parser должен отвергнуть каждую до SQLite.

Сначала literal1 COMMIT/checkpoint, затем seed/upgrade2 в отдельном transaction каждого store. Raw main всех10 остаётся1, committed WAL виден как1/2/3 согласно case. Handles не закрываются до внешнего KILL. Нет общей межфайловой migration transaction. Writer устанавливает `synchronous=FULL`, `wal_autocheckpoint=0`, finite busy timeout.

Native Notes witness: immutable proof revision1 при текущей изменённой заметке revision2 и отдельном deleted tombstone revision3. Native Caps witness: retained pending input/uncertain reservation и terminal receipt с purged input; reserved1/spent1. Основная пара согласует registry/native IDs. Это seeded storage proof, **не исполнение native effect или reconciliation**. Control hash включает schema и фиксированные доменные rows; main/WAL hashes и размеры проверяются до/после readers и после закрытия RO handles. SHM bytes не объявляются неизменными.

Inventory строго30 regular main/WAL/SHM files,22 directories и2 объявленных symlinks; walker не следует ссылкам. Никаких неизвестных paths или копирования реального store. Лимит8MiB — предел данного fixture, не физическая квота Docker volume.

Каждая phase использует `node --input-type=module -e <точные emitted bytes>`:

| Phase | Mount | CapAdd | START | Назначение |
| --- | --- | --- | --- | --- |
| `runtime-check` | RO | `DAC_READ_SEARCH` | да | Observed Node24.15.0, SQLite/SQL3.51.3, FTS5 |
| `writer-fixtures` | RW | **`DAC_READ_SEARCH, CHOWN`** | да | Synthetic writes, ownership handoff, ready, external KILL |
| `probe-v2` | RO | `DAC_READ_SEARCH` | да | Неизменённый current CLI, `/data` → Notes2/Caps2 |
| `matrix-v2` | RO | `DAC_READ_SEARCH` | да | Тот же экспорт: mixed pairs и6 negative roots |
| `bridge-v1` | RO | `DAC_READ_SEARCH` | да | Exact old parser отказывает primary и обеим mixed pairs |
| `audit-all` | RO | `DAC_READ_SEARCH` | да | Bytes, schema/row witness и permissions после чтения |
| `unknown-image` | RW declared | `DAC_READ_SEARCH` | **нет** | Actual unlabelled image отказ, copied container label не authority |

Всегда `CapDrop=[ALL]`, без FOWNER. Writer после COMMIT сначала chmod regular files0600/directories0700, затем chown10001:10001. После handoff только read-only checks; retained `DAC_READ_SEARCH` позволяет их выполнить. Links проверяются через lstat/readlink, без chmod-follow. Unsupported ownership/profile отказывает; более permissive fallback отсутствует. Actual foreign-owner evidence пока не получено.

Image pin остаётся `sha256:d07345cb66b2c1ab903861f36902d7b301cc25289778d0d178c81143281ca97e`; перед writer runtime/image/effective-Env gate обязателен. Serving ID/image/StartedAt проверяются без вывода config/Env. Networknone/no ports, NoCopy, readonly rootfs, User0:0, no healthcheck, restartno,128MiB RAM/MemorySwap,0.5CPU,16PIDs,tmpfs16MiB noexec/nosuid, no-new-privileges. Эти ограничения заданы и сверяются с inspect; их actual Linux выполнение ещё не доказано.

## Commands, журнал и ошибки

Разрешены только literal image GET, serving GET inspect и task-scoped volume/container GETs. IDs immutable, pre-ID name допускается лишь для уже journaled exact phase. Полный список GET shapes:

```text
GET /images/<exact encoded image>/json
GET /containers/<exact serving ID>/json
GET /volumes/<exact task volume>
GET /containers/<owned ID or exact journaled pre-ID name>/json
GET /containers/<owned ID>/logs?stdout=true&stderr=false[&tail=4]
GET /containers/<owned ID>/logs?stdout=false&stderr=true&tail=32
GET /containers/json?all=true&filters=<exact encoded {volume:[task],status:[running]}>
GET /containers/json?all=true&filters=<exact encoded {volume:[task]}>
```

Второй list используется только перед volume DELETE, когда все собственные containers уже подтверждённо удалены. Перед CREATE/START/KILL/DELETE проверяются serving, volume, running users и отдельные inspect всех сохранившихся owned IDs/configs. Это избегает накопления семи больших `Command` strings в одном list response. Foreign running ID блокирует START; foreign stopped user в последнем all-users query блокирует volume DELETE. Граница не обещает атомарный lock Docker daemon против стороннего администратора.

Mutation whitelist:1 volume CREATE,7 container CREATE,6 START, ровно1 `kill?signal=KILL` для writer после validated ready, максимум7 unforced container DELETE с `v=false` и1 unforced volume DELETE. **STOP вообще отсутствует в разрешённом коде**, включая timeout/error cleanup. Числа mutation counts — отправленные попытки; подтверждённые удаления отдельно требуют GET404.

До каждой mutation записывается durable intent в exclusive task journal, fsync file+directory. Неоднозначный CREATE/START/KILL/DELETE не повторяется; разрешён только GET readback. Unknown CREATE сохраняет volume даже после404, потому что позднее создание ещё возможно. Running/ambiguous objects сохраняются с `needsReconciliation=true`, требуют отдельного решения root. Cleanup не использует force или prefix sweep. При config drift чужой объект не удаляется.

20s phase /150s work /180s total — deadlines controller, **не TTL ресурсов после потери соединения**. API request≤3s в оставшемся budget. OOMKilled=false требуется на каждом terminal success, включая137. Runtime/ready/phase failure прекращает новые phases. PASS возможен только после всех checks, confirmed cleanup и serving invariance.

Helper image не имеет reader label. Здесь не выдаётся positive actual `soty.storage-start.v3`: current guard обязан отказать `storage_reader_unknown` до START. Separate pure compatibility assertion на fresh format принимает `[1,2]` и отвергает честный `[1]`; это policy test, не image attestation. Full reader2 application/fallback START остаётся root gate.

## Измеренные размеры

Caps сохранены: raw96KiB, encoded Cmd48KiB, CREATE56KiB, bundle512KiB. Inspect cap128KiB только exact serving/owned-ID/pre-ID-name GET; прочие responses64KiB. Budget учитывает обе копии команды и Go escaping, минимум16KiB metadata reserve. Chunked decoder хранит один фиксированный buffer до cap, не массив tiny chunks.

| Program | Raw | Encoded Cmd | CREATE | Conservative duplicated inspect command | Reserve до128KiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| runtime | 694 | 804 | 1566 | 1672 | 129400 |
| writer | 36336 | 36996 | 37767 | 74396 | 56676 |
| current CLI | 36268 | 37367 | 38129 | 75738 | 55334 |
| current matrix | 37911 | 39387 | 40149 | 79778 | 51294 |
| old bridge | 25177 | 25666 | 26428 | 52216 | 78856 |
| audit, worst expected DTO | 10647 | 11071 | 11833 | 22386 | 108686 |
| never-start | 16 | 77 | 976 | 218 | 130854 |

Ready/audit output каждый≤4096 UTF-8 bytes и имеет exact keys. Actual portable writer ready2352 B, audit2486 B, data2776680 B; Linux ownership booleans/links не могут увеличивать эти строки сверх установленного budget. Неизвестные поля, false totals, OOM и oversized raw responses отвергаются. Stderr наружу отдаёт только fixed classifier/count/hash, без текста/SQL/markers.

## Локальные результаты и границы

```powershell
$reader2Runtime = Join-Path (Get-Location).Path 'var/toolchains/node-v24.21.0-win-x64'
$env:PATH = $reader2Runtime + [IO.Path]::PathSeparator + $env:PATH
& (Join-Path $reader2Runtime 'node.exe') --test --test-concurrency=1 output/implementation-20260930/p4-reader2-linux-canary.test.mjs
```

Final: **29 total /27 PASS /0 FAIL /2 explicit skips,2221.3677ms**, Node24.21.0/SQLite3.53.4. Log `p4-reader2-linux-local-final.log`. Дополнительно syntax/whitespace checks; источник exact old bridge и ранее замороженные old controller/fixture/tests bytes совпали.

Первый run был14 PASS/10 FAIL/2 skips. Новый running inventory тогда использовал all-stopped list, а fixture возвращал full inspect; `docker_output_limit` проявился до writer START. Исправлены одновременно реальная потенциальная граница накопленного `Command` и shape mock: running-only summaries + individual owned inspect, all-users только после owned cleanup. Cap не повышался. Промежуточный повтор24 PASS/2 skips, затем добавлены exact filters/foreign stopped/unresolved CREATE assertions; финальный результат выше.

Реальные локальные child processes исполняют выданные writer/CLI/matrix/old parser/audit. Единственные test substitutions — фиксированный `/data` на temp и `LINUX_ONLY=true`→false для отсутствующих Windows UID/chown/symlink возможностей. DB/DDL/seeds/SQL/control/byte checks не подменяются; writer завершается внешним kill, тест ждёт process close до удаления temp. Current CLI и old parser исполняются целиком. Portable marker не принимается remote `validateReady`, поэтому локальный bypass невозможно выдать за permissions success. Два file-symlink cases явно skipped EPERM; foreign UID/modes также **не доказаны** этим тестом. Они остаются обязательными в неизменённом Linux emitted program.

Control-plane tests — модель Docker responses, не actual Docker. Raw bounded HTTP decoder дополнительно проверен настоящим localhost chunked server на exact/+1 limits, обеих command копиях и pre-ID inspect. Immutable prepare test пишет temp bundle, но не исполняет его. Финальный review1 bundle также только создан.

Нужны независимый exact source/bundle review, root source approval и отдельное разрешение одного Linux send. Actual UID/GID/modes, RO volume/WAL/SHM и128MiB behavior, full image/bootstrap/restore этим receipt не объявляются закрытыми. Другие modules, deploy source и реальные данные автор не менял; root владеет remote/result/PROGRESS.
