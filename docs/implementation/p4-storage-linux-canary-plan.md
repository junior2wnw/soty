# P4-B1 — новый bounded Linux canary, план до implementation

30.09.2026. Исходный read-only план принят root; затем отдельно разрешена **локальная** реализация новых scaffold/fixtures/tests и immutable bundle после измерения размеров и local gate. Remote mutations этим документом **не разрешены**. Старые `storage-linux-canary.review*.bundle.mjs` не меняются и не запускаются снова. Новый artifact требует независимого review и отдельного разрешения на одну remote попытку. Исторический local20 checkpoint ниже дополнен фактическим неуспешным review2 run, отдельным root cleanup и исправленным local23/review3; результат Linux не переименован в PASS.

Цель: exact strict-v3 host probe на Linux RO volume для **Notes1 + Capabilities1**, вместе с Rooms2 и Apps6; populated synthetic data, FTS5, committed WAL, отдельные future/corrupt отказы. Это не Notes2/Capabilities2 reader, не migration2, не application rollout, bootstrap/fallback или cold restore proof.

## Проверенные исходные материалы

Старый scaffold прочитан, не исполнен и не отредактирован:

- `C:\Users\Junio\.codex\worktrees\soty-experience-release\соты\output\implementation-20260930\storage-linux-canary.mjs`, SHA256 `73be355645799dfb47c008a6544d24c1e9638cea8a16840d46cee502dece20d5`;
- соседний `storage-linux-canary.test.mjs`, SHA256 `7b92cdca79dadbae61b31fe9cb8ed1245ad3bca9caab40d39f1d86ec0951213e`;
- [старый план](storage-linux-canary-plan-20260930.md) и [результат review3](storage-linux-canary-result-20260930.md). Их Linux PASS относится к старому rooms-only probe, не к v3.

Свежий inventory root: `output/implementation-20260930/p4-linux-readonly-preflight.json`, SHA256 `203fb997f1e1129a7cfa5440380fa31a009cfbd8f8c7e3a145681365677b15ff`, observedAt `2026-09-30T13:59:18.248Z`. Он подтверждает прежние serving ID/image/StartedAt, host Node24.15.0, image metadata Node24.15.0, отсутствие reader label/healthcheck, один declared volume. Автор этого плана не выполнял SSH.

Закреплённые Linux inputs:

| Поле | Значение |
| --- | --- |
| Existing image ID | `sha256:d07345cb66b2c1ab903861f36902d7b301cc25289778d0d178c81143281ca97e` |
| Serving ID | `d86bc0b9b8f88a7693a227cca0b15864ac5388a8c6e478f25e0f1c3c7d9c4ceb` |
| Serving StartedAt | `2026-09-28T01:15:45.442670947Z` |
| Serving revision metadata | `24c2da22d48b89d295da52b100ef067c7b61ef63` |
| Host transport | Только прежний документированный `ssh dev`, после отдельного run approval |
| Current probe SHA | `d51b36c12e316e660fd2b3ab6d8fe2350833aff242f2ae95d296fb63409afd59` |
| Current guard SHA | `cd10d07f1f2b75e2f8c05ca469ae97f3bb7fb35ba76ebf085345eb99530f3a1c` |

Image служит только уже установленным Node interpreter. Он не становится v3 application reader от запуска host-owned script. Положительная проверка image reader/fallback остаётся открытой.

Official Node24.15.0 tag закрепляет SQLite **3.51.3**: [точный sqlite3.h](https://raw.githubusercontent.com/nodejs/node/v24.15.0/deps/sqlite/sqlite3.h), [release record](https://nodejs.org/en/blog/release/v24.15.0). Поэтому первая новая phase должна проверить **реальные** `process.versions.node===24.15.0`, `process.versions.sqlite===3.51.3`, `SELECT sqlite_version()` и FTS5 на in-memory DB. Metadata сама этого не доказывает. Любое расхождение — отказ до persistent fixture writes, без install/pull или ослабления expected runtime. Windows24.21.0/SQLite3.53.4 — отдельная уже проверенная local test среда.

## Малая дельта scaffold

Сохранить control plane: Docker Unix socket API wrapper, exact-ID ownership, durable single-shot intents, bounded response decoding/stderr classification, create/inspect/config validation, watchdog, cleanup/reconcile и immutable `--prepare` output. Изменить только namespace новой попытки, payload inputs, фазовую таблицу, строгие allowed result shapes и fixtures/audit.

Новый prefix `soty-storage-canary-<32hex>` и safe receipt schema `soty.storage-linux-canary.v1`. Один новый standard local volume, новый private `/tmp/<prefix>/receipt.json`, 0700/0600, fsync+rename+directory fsync. Не читать старые run IDs как разрешение на cleanup. Image/serving/sources проверяются вновь до первого CREATE; StartedAt должен совпасть со свежим утверждённым baseline, а не только с концом этого запуска.

Сохраняются все предыдущие исправления:

- только отсутствующий `HostConfig.Mounts[].ReadOnly` нормализуется в false; realized `Mounts.RW`, Source/Name/Target и `NoCopy=true` остаются строгими;
- inherited image Env проверяется по allowlist и exact значимым defaults; effective Env сверяется после CREATE. Никаких runtime hooks, application Env/config или секретов;
- image Healthcheck отсутствует/NONE, Volumes только ожидаемый `/data`; созданный container получает explicit `Healthcheck.Test=['NONE']`; неожиданный auto mount запрещает START и удерживает reconciliation evidence;
- writer после FULL COMMIT выводит фиксированный ready без body/marker/SQL и держит connections/event loop; host проверяет identity/running/ready, записывает intent и отправляет **один внешний SIGKILL**. После ambiguous KILL только inspect, без второго KILL/STOP/START;
- stderr только synthetic exact ID, tail32, bounded classifier; сохраняются коды/classes/count/hash, без raw text;
- DELETE без force и без cascade; отсутствие каждого exact ID/volume подтверждается. Потеря SSH никогда не означает повторить bundle.

## Fixtures: один writer, независимые read-only cases

Чтобы не увеличивать прежний потолок8 containers, независимые negative fixtures создаются заранее в фиксированных подкаталогах того же synthetic volume. Это не subpath mount: **каждый helper получает весь том в `/data`**, без второго mount. Никакие пути не принимаются от CLI/remote data.

| Fixed root | Содержимое и ожидаемый смысл |
| --- | --- |
| `/data` | Полные Rooms2 / Apps6 / Notes1 / Capabilities1 с tiny populated rows, committed WAL |
| `/data/cases/future-notes` | Notes main1 + committed WAL с unsupported version2/marker; другие stores отсутствуют |
| `/data/cases/future-capabilities` | Capabilities main1 + committed WAL с unsupported version2/marker; другие stores отсутствуют |
| `/data/cases/corrupt-notes` | Только повреждённый synthetic Notes main, fsynced fixed bytes |
| `/data/cases/corrupt-capabilities` | Только повреждённый synthetic Capabilities main, fsynced fixed bytes |

Future fixtures — только отрицательные неизвестные markers, **не implementation или proof DDL2**. У future fixtures сначала завершить v1 и checkpoint до main1 (проверить busy0), затем отдельный FULL transaction version2 в WAL. У valid root каждый main должен сохранять старый header, а normal SQLite видеть целевой version и marker из committed WAL. Никакой reader не получает `immutable=1`.

Writer использует только standalone, reviewed synthetic literal DDL/seed payload. Notes1/Cap1 берутся из уже frozen independent fixtures pin `6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e`. Rooms — прежние11 literal statements. Для Apps6 нужен отдельный literal fixture с exact full schema/index/guard set из того же pin; local test сравнивает его со schema6 и точным dependency graph pin, не с произвольной текущей migration. Remote writer и probe **не импортируют candidate application modules**. Если Apps6 fixture не укладывается в согласованные limits, требуется review плана; нельзя молча убрать Apps6 из ожидаемого результата.

Synthetic rows: Rooms marker; Notes active/archive/trash/deleted + receipts и известный FTS MATCH; Capabilities root/child grants, ledger reserved/spent, invocation/dispatch/receipt; Apps6 private grants, bound/tombstone domain, target1/2 со sticky floor2 и active rollback1, saved entry и discussion tombstone. Их размеры малы, поля фиксированы; никаких личных данных и настоящих credentials. Canary не проверяет service ACL/dispatch/runtime binding только наличием этих строк.

Все SQL connections остаются открыты до ready/KILL. Writer проверяет aggregate file count/bytes, synchronous FULL, per-store SQLite version и main/WAL sizes+SHA. В ready — только allowlisted case keys, версии/размеры/hashes и fixed readiness. Main/WAL hashes фиксируются после COMMIT и затем сравниваются RO audit; shared-memory bytes не объявляются постоянными. На файлах/папках synthetic fixture — 0600/0700; это проверка конкретного Docker User0:0/engine profile, не общая rootless/userns сертификация.

## Шесть контейнеров, пять START, один KILL

| Phase | Mount | Действие / обязательный результат |
| --- | --- | --- |
| runtime-check | RO, пустой volume | Exact Node/SQLite и in-memory FTS5; никаких persistent SQL writes |
| writer-fixtures | RW | Один writer для всех фиксированных fixtures; ready → durable KILL intent → один KILL → подтверждённый exit137 |
| probe-valid | RO | **Точные неизменённые bytes** current `storage-probe.mjs`, обычный CLI с `SOTY_STORAGE_PROBE=1`; exact `{ok:true,schema:'soty.storage-format.v3',rooms:2,apps:6,notes:1,capabilities:1}` |
| probe-negative | RO | `SOTY_STORAGE_PROBE=0`; wrapper импортирует **точные pinned probe bytes** и вызывает экспорт `readStorageFormat` последовательно на четырёх fixed roots. Оба future → `storage_format_unknown`; оба corrupt → `storage_format_unreadable`. Никакого rewriting function/source или fallback |
| audit-all | RO | Raw main header, normal SQLite view/marker hashes, FTS result, ledger/Apps witness counts, file inventory и unchanged main/WAL hashes. Не выводит body/input/receipt/domain data |
| unknown-reader | RW declared, never started | Stopped task-owned container с copied v3 label; настоящий actual-image inspect через current guard возвращает `storage_reader_unknown`, ноль START/extra helper mutations |

Негативная phase доказывает **тот же экспортированный parser с четырьмя явными roots**, а не четыре отдельных запуска production CLI `/data`. Это осознанная узкая граница для сохранения container budget; положительный case выполняет настоящий CLI без wrapper. Если review потребует отдельный CLI на каждый negative root, нужен новый план с иным числом фаз/контейнеров, не скрытый subpath mount или повторный START.

Любой неожиданный success/error/extra output field/size mismatch прекращает следующие фазы и ведёт только к exact cleanup/reconciliation. Нельзя считать часть cases успехом всего canary. Отрицательные cases должны реально быть отрицательными до Linux run в local emitted-writer/probe tests.

Каждый terminal inspect обязан иметь `State.OOMKilled === false`, включая writer exit137 после ready/KILL. OOM никогда не считается запланированным crash, даже если версии и hashes совпали. Отдельный mock воспроизводит ready→OOM→terminal137 и требует отказ.

## Сохранённые пределы и небольшое увеличение fixture budget

- Hard ceiling8 containers /1 volume, не более одного running task container; план использует6/1. Никаких резервных retry containers при отказе.
- 20s individual container deadline;150s work budget и180s total,30s оставлено cleanup. Это deadlines работы controller, **не гарантированный TTL ресурса**. Docker API request ≤3s в remaining budget; finite local SQL waits. Deadline не продлевается за каждый pending poll. При ambiguous KILL или потере SSH точные ресурсы могут остаться Running: сохраняются journal/IDs и `needsReconciliation`, PASS запрещён. Controller не повторяет KILL/STOP и не подменяет внешний KILL self-exit, который мог бы checkpoint WAL. После обрыва требуется отдельная read-only reconciliation и отдельно разрешённое recovery.
- Контейнер128MiB RAM/MemorySwap,0.5 CPU,16PIDs; rootfsRO, networknone, no ports, capdropALL + DAC_READ_SEARCH, no-new-privileges; tmpfs16MiB noexec/nosuid. Нет Docker socket внутри, privileged/PID host/devices/binds/nested mounts.
- Совокупно **≤8MiB** main/WAL/SHM/fixed corrupt fixture bytes и≤32 regular files, вместо прежнего1MiB для одного Rooms store. Это предлагаемое явное изменение data budget для восьми DB fixtures; не physical Docker volume quota. Writer проверяет его до ready; превышение — отказ.
- Raw generated container script≤96KiB, encoded JSON `Entrypoint`+`Cmd`≤48KiB, полный CREATE config≤56KiB. После фактического review2 failure prepare также считает **оба** command представления `Path/Args` и `Config.Entrypoint/Cmd`, включая консервативное Go JSON escaping: `max(duplicateCommandBytes, 2*goCommandBytes+64)+16384 ≤ 131072`. Только exact container-inspect GET serving/owned ID или заранее записанного точного phase/run name получает cap128KiB; query/чужое имя не расширяют allowance. Остальные responses остаются≤64KiB. Это явная согласованная correction, не увеличение общего Docker cap. Reserve не гарантирует arbitrary engine metadata: реальный overflow остаётся fail-closed с exact-resource reconciliation. Bundle≤512KiB; strict stdout JSON≤4KiB; stderr retained classifier≤16KiB, без raw text. Fixture, SQL/bytes audit и resource limits не сокращены.
- Synthetic volume name/mountpoint не совпадают с serving mount inventory; эти пути сравниваются in-memory, в receipt только hash. Serving ID/image/running/StartedAt проверяются до/после; любые non-GET serving requests отвергаются API wrapper.

Сохранить все остальные exact request/identity fences старого scaffold. В частности unknown/unresolved CREATE/START/KILL не позволяют создать второго writer, удалить ещё используемый volume или очищать чужой ресурс по префиксу.

## Будущий локальный scope и acceptance

После отдельного разрешения — **новые** файлы только в текущем `soty-platform` workspace:

```text
output/implementation-20260930/p4-storage-linux-canary.mjs
output/implementation-20260930/p4-storage-linux-canary.test.mjs
output/implementation-20260930/p4-storage-linux-canary.fixtures.mjs
output/implementation-20260930/p4-storage-linux-canary.reviewN.bundle.mjs (новый immutable --prepare; никогда overwrite)
docs/implementation/p4-storage-linux-canary-plan.md
docs/implementation/p4-storage-linux-canary-result.md (после фактического run)
```

Не менять frozen `deploy/*` probe/guard/domain source ради canary и не переносить source edits в старый workspace. Prepare читает reviewed source bytes с size/symlink/hash validation, outputs `wx`, без SSH/Docker. Root сначала читает новый script/fixture/tests/bundle и проверяет hash. Независимый reviewer проверяет весь mutation allowlist, все case roots, ready/audit redaction и ограничения. Затем отдельный run approval с exact bundle SHA; перед stdin передачей SHA перепроверяется. Если SSH response потерян — сначала read-only journal/exact resources reconciliation, bundle не повторять.

Локальные tests на isolated Node24.21, последовательно и по test-lock: старые14 ключевых mock/reconcile/deadline cases сохраняют смысл; новые batch-case names/typed results, bad runtime/FTS before writer, legacy image label, byte/count bounds, attempted serving mutation, source hash mismatch, wrong ready, unexpected negative success и unresolved KILL. Реальный generated writer запускается на отдельном temp, завершается извне, затем **generated** positive/negative/audit payload исполняются над ним; path substitution ровно одна reviewed root anchor, без других logic substitutions. Apps6 literal DDL отдельно сравнивается с exact historical6. Local tests не выдаются за Linux/mount execution.

На этапе preflight был создан только этот план. После отдельного разрешения появился локальный checkpoint ниже; SSH/Docker/production mutations не было. External gates после успешного canary всё равно останутся: actual v3 application/fallback bootstrap, Notes/Cap reader2, multi-store encrypted backup/restore, Connect/World compatibility и production migration admission.

## Исторический локальный checkpoint review2 — 30.09.2026

Только три новых source/test файла из утверждённого scope и `wx` review bundles в текущем worktree; старый scaffold, deploy probe/guard и domain modules не менялись. Литералы получены из exact Git pin `6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e` при подготовке fixture и хранятся в новом файле. Local test независимо загружает этот же полный historical dependency graph из Git, сверяет его SHA, сравнивает **каждый SQL object** и открывает populated fixture настоящим старым migrator: Apps6 63 objects, Notes1 12, Capabilities1 20. Rooms сохраняет 11 точных historical statements.

Первая actual generated-writer проверка выявила ошибочный порядок локальной сборки fixture: Rooms prelude с `PRAGMA synchronous` оказался внутри `BEGIN`. Исправлен только fixture runner: два неизменённых historical pragma statements до `BEGIN`, затем DDL/seed в transaction; `busy_timeout=100` восстанавливается до BEGIN. Full WAL/FULL COMMIT/внешний KILL сохраняются. После этого focused real case и весь новый файл прошли.

Финальный author run:

```text
var/toolchains/node-v24.21.0-win-x64/node.exe --test --test-concurrency=1 output/implementation-20260930/p4-storage-linux-canary.test.mjs
20 tests / 20 PASS / 0 FAIL / 0 SKIP, 1197.9195 ms
Node v24.21.0 / SQLite 3.53.4
```

Это включает реальные emitted writer/probe/audit программы над новым локальным temp, остановку writer извне и ожидание закрытия процесса до удаления temp: **20 files / 1,076,968 bytes**, ready JSON **1712 bytes**, audit JSON **1897 bytes**. Main headers valid stores0, future Notes/Cap1; SQLite видит соответственно 2/6/1/1 и отдельные unsupported2 в committed WAL. Exact positive CLI и четыре fixed-root negative вызова успешны; audit подтверждает FTS/ledger/rollback-floor/saved/tombstone witnesses и unchanged main/WAL. Local path substitution меняет ровно один фиксированный root anchor каждой программы; остальная логика не подменяется. Windows fixture не доказывает Linux Docker read-only mount, User0:0, cgroup или Node24.15 runtime.

Mock cases проверяют actual control plane: шесть CREATE / пять START / один KILL, одна volume; no-copy/effective-env/config drift; потерянные ответы без повторных mutations; unresolved KILL остаётся `needsReconciliation`; per-container deadline; OOM137 refusal; wrong runtime/FTS before writer; serving StartedAt mismatch; источник/encoded size mismatch до Docker; strict ready/audit shapes, unexpected negative success, bounded stderr без raw text. Это modelled Docker API, не daemon execution.

Измеренные максимумы для generated payload (UTF-8 bytes, audit рассчитан с верхними значениями snapshot):

| Phase | Raw program | JSON Entrypoint+Cmd | CREATE config | Прежний ошибочный reserve64KiB для одной копии |
| --- | ---: | ---: | ---: | ---: |
| Runtime | 694 | 804 | 1566 | 64732 |
| Writer | 41543 | 42123 | 42886 | 23413 |
| Positive CLI | 24616 | 25333 | 26095 | 40203 |
| Negative roots | 33344 | 33410 | 34172 | 32126 |
| Audit upper bound | 7586 | 7908 | 8670 | 57628 |

Этот расчёт учитывал только одну копию command и оказался недостаточен: actual Docker inspect возвращает также `Path/Args`. Immutable bundle был создан локальным `--prepare`, **162614 bytes**, `node --check` PASS; на момент этого checkpoint он ещё не исполнялся. Повторный prepare в тот же путь отвергается `EEXIST`, исходные bytes сохраняются. Фактическая одна Linux попытка и исправленная граница описаны ниже.

| Artifact | SHA256 |
| --- | --- |
| `p4-storage-linux-canary.mjs` | `137905805516a7982e9853ebbe034bb572954ee45ea80942611ea55575594706` |
| `p4-storage-linux-canary.fixtures.mjs` | `d86ee283667da2538cba6cdcbd53df63b93e1f90d4d4f63dd0979715e735e2c3` |
| `p4-storage-linux-canary.test.mjs` | `5af707dc87ce65dcd7259bc609ae768c487877de8608634f810499f4444e35b1` |
| `p4-storage-linux-canary.review2.bundle.mjs` — выполненный один раз, failed; не повторять | `e2853c4882de7520949b3de9392a6c5037022e8aa18c35f0ce458d7f312c3927` |

После 20/20 удалены только две пустые строки EOF в fixture/test. По согласованию root семантически одинаковый suite не повторялся; все три source/test файла и новый review2 bundle прошли `node --check`, whitespace check не имеет замечаний. Review1 (SHA `268f67405db8ece5c1aed70bac81d5fddb8820a3c313472e7a7667c1b0132783`) оставлен неизменным как superseded local artifact: не исполнялся и не является кандидатом run. Subsequent independent review review2 завершился без найденного source blocker; оно не обнаружило пропущенное второе представление command. Это не Linux success, reader2 или rollout/restore proof.

## Фактический review2 failure и отдельное recovery

Root отправил exact review2 **один раз**, run `2abd529a43c1b4d8a8d68370a936b71c`. Safe result `output/implementation-20260930/p4-storage-linux-canary.review2.linux-result.json` неизменяем. Runtime phase реально подтвердила Linux Node **v24.15.0 / SQLite3.51.3 / FTS5**. После CREATE writer его первый inspect превысил response64KiB; cleanup получил тот же `docker_output_limit`. Writer **не получил START**, поэтому WAL/reader/audit фазы не выполнены. `ok:false`, `needsReconciliation:true`, serving ID/image/StartedAt неизменны. Это частичное evidence, не успешный canary.

Отдельный GET-only reconciliation root установил writer `created/Running=false`, runtime `exited0`, `OOMKilled=false`, restart `no`, ровно два ожидаемых volume users. Полный writer inspect **89 487 bytes**: отдельные JSON `Path/Args`42 116 B и `Config.Entrypoint/Cmd`42 123 B, остальные fields около5.2KiB. Runtime inspect7425 B. Исходный journal SHA `cfc973dd9af9f11fba8fbf778e8c3dcb403c96d5da5dfc5ca3fbed151b94e8e0`; его bytes, review2 bundle/send/result и reconciliation receipt не менялись.

Согласованный отдельный cleanup source `p4-storage-linux-canary.cleanup.mjs` разрешает только DELETE двух точных stopped IDs и затем одного volume, всегда unforced; никаких START/STOP/KILL, prefix sweep или retries. Перед первым DELETE проверяются все resources, перед каждым следующим — актуальные serving/ownership/users/source journal hash. Новый exclusive append-only journal пишет intent с file/directory fsync до mutation; любой error прекращает дальнейшие DELETE и оставляет reconciliation. `mutations.DELETE` — число выданных попыток после intent; подтверждённые эффекты только `deletedContainers`/`volumeRemoved` после GET404.

Авторские cleanup tests **6/6 PASS**,0skip179.0ms на Windows24.21; HTTP decoder реально читает chunked localhost response. Initial failures: fixture неправильно разбирал DELETE query, затем Windows directory fsync дал EPERM. Fixture исправлен; production acquisition теперь закрывает FileHandle при failed sync. В Windows test только directory-fsync вызовы injected (проверены directory+parent), реальные wx/file-fsync/append/reopen сохранены; Linux default строго выполняет directory fsync, ошибка не подавляется.

Root независимо прочитал source и выполнил cleanup ровно один раз: source SHA `8b1de626bcdb6cbdd51927a7b7a11f39f8859b1fe08d054bef076fdb23b6b102`, tests `195b4301ee6664fcdc6dad6b199a641094e99cb5bb8b8268b9fdfc08ddf24b21`, fixed-trailer bundle `c1ae8fe1538462a7be8ee28ccafa31de880b875c1510081f1da9200f63e10212`. Safe Linux receipt `p4-storage-linux-canary.cleanup.review1.linux-result.json`: **2 exact containers + volume deleted/GET404,3DELETE,0START/STOP/KILL,needsReconciliation=false,servingUnchanged=true**. Это результат root; автор не запускал SSH/Docker. Cleanup не меняет исходный failed результат canary. Подробный root журнал остаётся в [отдельном result](p4-storage-linux-canary-result.md).

## Исправленный local23 / review3 freeze

Первопричина — ошибка нашего sizing и mock: считался только `Config` command; injected mock не содержал `Path/Args` и обходил raw response budget. Docker inspect возвращает оба представления. Дополнительный conservative serializer допускает Go HTML escaping; это верхняя граница, не утверждение о режиме actual daemon: например [Moby26.1.5 WriteJSON](https://github.com/moby/moby/blob/v26.1.5/api/server/httputils/httputils.go#L81-L87) явно выключает optional HTML escaping, тогда как [Go encoding/json](https://pkg.go.dev/encoding/json#HTMLEscape) описывает более крупный escaped вариант.

Scaffold меняет только sizing и bounded response transport. Cap128KiB применяется к serving ID или owned entry с точным run/phase/index, в том числе name-based readback после ambiguous CREATE до получения ID. Иные paths/query/methods остаются64KiB. Prepare оставляет≥16KiB metadata reserve после двух command copies; actual cap всё равно проверяется по пришедшим bytes. Mutation whitelist, journal ordering, deadlines, resources, exact fixtures, positive CLI/negative roots и OOM checks прежние.

| Phase | Raw B | CREATE B | Две command copies с conservative escaping B | Budget max(exact,2×single+64) B | Reserve из128KiB B |
| --- | ---: | ---: | ---: | ---: | ---: |
| Runtime | 694 | 1566 | 1612 | 1672 | 129400 |
| Writer | 41543 | 42886 | 84760 | 84820 | 46252 |
| Positive | 24616 | 26095 | 51490 | 51550 | 79522 |
| Negative | 33344 | 34172 | 66824 | 66884 | 64188 |
| Audit upper bound | 7586 | 8670 | 15990 | 16050 | 115022 |

Final serial local gate на Node24.21/SQLite3.53.4: **23/23 PASS,0FAIL,0SKIP,1532.4622ms**. Старые20 cases сохранены; три новых проверяют duplicate/escaping budget до любого request, exact inspect route scope и настоящий chunked HTTP response. Последний воспроизводит old64 failure, new128 success для ID и pre-ID name, plain/escaped representations, обе границы и+1byte overflow. Mock также сериализует оба command представления и применяет cap. Первый прогон15/23 выявил только fixture ordering: CREATE ID уже попал в controller memory, но mock искал его в предыдущем persisted journal; live ID mapping fixture исправлен, production checks не ослаблялись.

Actual emitted SQLite programs вновь прошли на local temp:20files/1 076 968 B, ready1712 B/audit1897 B, все восемь fixtures и main/WAL hashes. Это Windows evidence, не повторный Linux run. Fixture SHA и bridge probe/guard не изменены.

| Current artifact | SHA256 |
| --- | --- |
| `p4-storage-linux-canary.mjs` | `149a28de4980df7fec3feab4d54af5a40acbde8c9ba0fcbd81f97c47c587b684` |
| `p4-storage-linux-canary.test.mjs` | `60ff72bd1b200d74ec8518270b31993b683a569eaf720d930b8824dd74ab778a` |
| `p4-storage-linux-canary.fixtures.mjs` — unchanged | `d86ee283667da2538cba6cdcbd53df63b93e1f90d4d4f63dd0979715e735e2c3` |
| **`p4-storage-linux-canary.review3.bundle.mjs` — новый, не исполнен** | `7c56a65a092cdf4a9e0a1c266316d1963398d167ba1ab92c68ba51cb04089456` |

Review3 создан с `wx`,164266 B; syntax/whitespace checks PASS. Он требует нового независимого source/bundle review и отдельного root разрешения на одну фактическую попытку. Review2/cleanup artifacts не переподготавливаются и не повторяются. Успешный future canary всё равно не заменит reader2 application baseline, cold restore или production migration admission.
