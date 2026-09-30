# P4-B1b — отдельный synthetic Linux reader2 canary

30.09.2026. **Подготовительный план, не разрешение remote run.** Создан только этот документ. Root integrated gate B1b ещё впереди. Основания: [reader2 rollout plan](p4-reader2-rollout-plan.md), [локальный reader2 receipt](p4-reader2-implementation.md), [фактический bridge Linux result](p4-storage-linux-canary-result.md).

Новый canary проверяет Linux отличия host parser для Notes2/Capabilities2. Full application image, default-off serving baseline, первый unlabelled bootstrap и encrypted cold restore остаются отдельной зоной root. Старые review2/review3/cleanup bundles, source и receipts заморожены: их нельзя менять, пересобирать под тем же именем или повторять.

## 1. Что осталось доказать

Windows author coverage65 PASS/1 file-symlink skip уже проверяет exact schemas, все9 empty/1/2 pairs, malformed objects, populated default-off B1a reopen и main1/WAL2. Root bridge review3 реально прошёл Linux Node24.15.0/SQLite3.51.3/FTS с **Notes1/Caps1**. Это ещё не проверка нового reader2.

Здесь нужны только:

- genuine v2 DDL и retained proof/ledger в committed WAL поверх raw main1 после внешнего завершения writer;
- normal read-only SQLite через actual Docker RO/NoCopy mount, включая чужой UID, файлы0600 и существующие WAL/SHM;
- обе смешанные пары1/2 и2/1 без общей межфайловой migration transaction;
- exact frozen bridge отказывает на настоящем2, хотя raw main всё ещё1;
- unknown3 и одна повреждённая native guard definition каждого store отказывают без repair;
- реальные Linux file symlinks отвергаются до SQLite; main/WAL и witnesses не меняются после всех readers.

Официальный SQLite допускает read-only WAL при уже существующих доступных WAL/SHM; это не разрешение игнорировать WAL или обещание любой filesystem topology. Используем обычное read-only открытие, без `immutable=1`. Существующие sidecars — часть конкретного fixture. [SQLite: Read-Only Databases](https://www.sqlite.org/wal.html#read_only_databases)

Rooms/Apps в этом volume отсутствуют и возвращают `empty`. Их DDL и данные не дублируются: прежний Linux result покрывает Rooms2/Apps6, их production recognizer не менялся в B1b. Полные текущие probe bytes всё равно исполняются, без вырезания этих веток.

## 2. Pins и входной gate

До создания executable artifact root должен принять final B1b source/tests и независимый review. Prepare закрепляет **тогда принятый commit и точные bytes**, включая transport dependency. На момент этого плана current review slice:

| Вход | Pin |
| --- | --- |
| Reader2 probe | `5d7bdb6cee8c38671ff9a188e26b4bc33e983fbef04ec3e3ac9cabd9338f650a` |
| Reader2 guard | `95e0a74b818a6bec44b0994109bc0f26019df6240e929be1da5a617ca3454cca` |
| Native DDL | accepted B1a `5e459abc6afa376861c2032226bd29f78bf0468d` |
| Notes2 additive literal | LF2458 B; `e15a1dc6d57dfe6c3de56e6d5052f42080955612f56b39a70f21fc8c93ff0add` |
| Capabilities2 additive literal | LF5122 B; `64c394ad739eba70bea95e2ffb18c29d2f0753b55c1b4373060620fde650a8c8` |
| Historical v1 base | `6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e`, existing frozen literal fixtures/provenance |
| Frozen bridge | `ae914f55e6d8d64628a7279189d2d55dfd05de45`; exact Git LF probe `d0aed27ce790502f36df20689663a43aee0ed28266e6b424a002cde7eb2f4e17` |

Bridge LF bytes24317 отличаются только переводами строк от прежнего reviewed CRLF `d51b36c…`/24616 B. Новый source gate явно подтверждает оба fingerprints; не смешивает Git hash и working bytes.

Actual helper image может остаться прежним `sha256:d07345cb66b2c1ab903861f36902d7b301cc25289778d0d178c81143281ca97e` при свежем GET-only inventory. Его Linux/amd64, image Env/hooks/volumes/healthcheck и отсутствие reader label проверяются заново. Runtime phase требует observed Node24.15.0, SQL/SQLite3.51.3 и FTS result, как в предыдущем actual run. Любое расхождение останавливает writer; metadata не заменяет исполнение. Другой image требует отдельного pin/review, не автоматического обновления ожидаемой версии.

Serving ID/image/StartedAt сверяются со свежим root inventory. Ни environment values приложения, ни credentials, ни production volume не копируются в fixture или вывод. Image defaults проверяются по safe allowlist, effective Env вычисляется с учётом Docker merge, а не только нового `Env` массива.

## 3. Фиксированная синтетическая матрица

Один новый Docker local volume с неповторяемым run ID. Все paths литеральны под его `/data`, не берутся от пользователя. Writer содержит самостоятельные literal v1+v2 DDL, pinned provenance и маленькие seed routines. Он не импортирует current domain modules или candidate migrations.

| Root для parser | Notes / Capabilities | Что лежит физически и ожидается |
| --- | --- | --- |
| `/data` | 2 / 2 | Два populated native stores; main1/WAL2, exact current CLI success |
| `/data/cases/mixed12` | 1 / 2 | Два настоящих stores; Notes1 с ordinary data, Caps2 с native ledger; current export success |
| `/data/cases/mixed21` | 2 / 1 | Notes2 с native proof, Caps1 с ordinary ledger; current export success |
| `/data/cases/future-notes` | unknown3 / empty | Genuine Notes2 base, отдельный committed version3 marker в WAL; `storage_format_unknown` |
| `/data/cases/future-capabilities` | empty / unknown3 | Аналогично Caps; `storage_format_unknown` |
| `/data/cases/bad-notes-guard` | malformed2 / empty | Genuine Notes2, затем удалён `note_native_create_no_delete` в WAL; `storage_format_unreadable` |
| `/data/cases/bad-capabilities-guard` | empty / malformed2 | Genuine Caps2, затем `cap_native_note_admission` заменён на inert body в WAL; `storage_format_unreadable` |
| `/data/cases/link-notes` | refused / empty | `notes/notes.sqlite` — единственный известный symlink на primary Notes main; `storage_format_unreadable` |
| `/data/cases/link-capabilities` | empty / refused | Аналогичный exact symlink на primary Caps main; `storage_format_unreadable` |

Итого **10 реальных DB**, ожидаемо30 regular main/WAL/SHM files и2 explicitly declared links, без иных файлов. Inventory walker не следует symlink: проверяет ровно два разрешённых литеральных link paths/targets; любой другой link/file/directory запрещён. Targets обоих links остаются внутри этого же synthetic volume. Caps/Notes DB каждого mixed root самостоятельны, не hardlink/copy открытого SQLite main без WAL.

Каждый DB создаётся как literal1, COMMIT и явный successful checkpoint до raw main1. После этого writer держит handle открытым, `wal_autocheckpoint=0`, `synchronous=FULL`, finite busy timeout. Затем в отдельном transaction этого DB применяет literal2/metadata и seed либо v1 seed; COMMIT остаётся в WAL. Unknown3/bad guard добавляются последующим COMMIT в том же WAL. Это десять независимо обновляемых stores, **не** cross-file atomic migration. До ready нет открытой write transaction.

Основные v2 witnesses повторяют смысл успешно проверенного локального storage seed:

- Notes: original immutable create proof сохраняется рядом с текущей human-edited заметкой и отдельным tombstone; deterministic native note/mutation identity, origin proof revision1, matching createdAt и FTS active row.
- Capabilities: один held native pending/uncertain input с reservation и один terminal native receipt с purged input; stable invocation/note/mutation identity, native target, reserved1/spent1 и retained receipt digest.
- Primary pair использует согласованные store IDs и deterministic keys, но это **seeded storage evidence**, а не выполнение native admission, Notes effect или reconciliation. Current row text и proof/receipt/control hashes сравниваются локально; наружу только fixed booleans/counters/hashes, без input/body/authority JSON.

Audit проверяет SQL versions/object inventory и main headers, known witnesses, FK для valid fixtures, hashes/размеры каждого main/WAL до/после read и после закрытия RO handles. SHM existence/permissions проверяются; его lock bytes не объявляются неизменными. Изменения версии registry IDs или доменных строк reader не делает.

## 4. Linux permissions и семь фаз

Сохранён профиль: один running task container одновременно, networknone/no ports, readonly rootfs, отдельный volume только `/data`, NoCopy, User0:0, working dir`/`, no image healthcheck, restart`no`,128MiB RAM/MemorySwap,0.5CPU,16PIDs,tmpfs16MiB noexec/nosuid, no-new-privileges, CapDropALL. RO phases имеют только `DAC_READ_SEARCH`.

**Одна предлагаемая scoped permission delta:** writer получает также `CHOWN`, чтобы после всех COMMIT передать только fixed synthetic directories/regular files UID/GID10001:10001 с0700/0600. Mode устанавливается до передачи ownership; дополнительный `FOWNER` не требуется. У links отдельно проверяются type/target, без попытки chmod через ссылку. Это не capability RO readers и не host chown. После передачи ownership writer выполняет лишь read-only snapshot/ready и ждёт external KILL; не продолжает SQL writes. `lstat`/audit подтверждает actual UID/GID/mode regular files/directories. Если Docker/user namespace не позволяет этот exact профиль, canary отказывает; нельзя автоматически вернуть root ownership или world-readable permissions и назвать foreign-owner gate пройденным.

| Phase | Mount | Действие |
| --- | --- | --- |
| runtime-check | RO | Exact runtime/FTS, без persistent writes |
| writer-fixtures | RW | Создать все fixtures, foreign ownership, ready snapshot; один внешний KILL, actual exit137/OOMKilled=false |
| probe-v2 | RO | Точные current probe bytes, обычный CLI `SOTY_STORAGE_PROBE=1`, `/data`, result2/2 |
| matrix-v2 | RO | Тот же current exported parser над двумя mixed и шестью fixed negative roots; все outcomes exact |
| bridge-v1 | RO | Exact frozen ae914f5 probe export над `/data`, mixed12 и mixed21; все три отказывают `storage_format_unknown`; никакого старого migrator |
| audit-all | RO | Полная main/WAL/witness/ownership сверка, включая состояние после rejected reads |
| unknown-image | RW declared, **never START** | Actual-image guard refusal на отдельном stopped owned container несмотря на copied container reader2 label |

План: **7 containers,6 START,1 KILL,0 STOP,1 volume**, потом unforced cleanup7 containers+volume. Это на одну RO phase больше bridge canary: два полных parser source не вкладываются в один Cmd. Ceiling8containers/1volume сохраняется, запас не является разрешением retry container.

Primary positive — настоящий CLI. Matrix/bridge — тот же точный экспорт над фиксированными roots; это не десять отдельных CLI executions `/data`. Runtime import wrapper строит data URL из exact embedded source bytes без source rewriting. Из-за размера reader2 вместо base64 expansion допускается JSON-string embedding + runtime `encodeURIComponent`; static review обязан восстановить **исходные bytes**, а не принять похожую функцию. Не склеивать два parser source в одну команду ради уменьшения phase count.

## 5. Fresh evidence и честная граница START

Каждая завершённая phase получает новый exact container ID; journal связывает result с run/phase/source hash/volume и writer snapshot. Старые `storage-start` receipts из других runs не принимаются, версии не читаются из raw main вместо SQLite, успех предшествующего parser не подменяет новый вызов.

Текущий helper image **unlabelled**. Поэтому здесь нет положительного actual `soty.storage-start.v3` для reader2 application. Unknown-image phase использует настоящий image inspect и current guard: ожидается `storage_reader_unknown` до helper/application START, неизменное число mutations во время refusal. Копированный label container не повышает actual image.

Дополнительно pure policy assertion над свежим format2/2 должна отвергнуть точный честный v3 `[1]` manifest и принять `[1,2]`; это тест политики, **не actual image attestation**. Exact frozen bridge отказ из отдельной phase — фактическое исполнение старого parser на Linux. Оба факта не выдают старому image новый label и не производят фиктивную positive START receipt. Положительный actual reader2 START/full application/fallback — следующий root image gate.

## 6. Byte/resource budgets до любого CREATE

Сохраняются: raw program96KiB, JSON Entrypoint+Cmd48KiB, полный CREATE56KiB, immutable bundle512KiB. Отсутствие Apps6/Rooms fixture literals освобождает место для native definitions и seeds; **действительные размеры пока не измерены**. Перед source freeze измеряются каждая emitted phase и audit с максимальным DTO, иначе bundle не создаётся. Нельзя убрать witnesses или повысить cap незаметно ради прохождения.

Inspect учитывает **обе** command копии — `Path/Args` и `Config.Entrypoint/Cmd`, JSON escaping и newline. Требование prepare:

```text
max(encodedDuplicateCommandBytes, 2 * conservativeEncodedCommandBytes + 64)
  + 16384 metadata reserve <= 131072
```

Консервативное Go HTML escaping — upper bound, не утверждение о настройке actual Docker. Exact serving/owned container GET inspect имеет128KiB cap, включая заранее journaled exact phase/run name до ID после ambiguous CREATE. Иные paths/query/methods и responses остаются64KiB. Raw HTTP decoder проверяет пришедшие bytes и fail-closed overflow; reserve не гарантирует arbitrary daemon metadata. Две копии command и realistic serialized response обязательны в mock; прежняя review2 ошибка не повторяется.

Данные≤8MiB суммарно,≤30 regular files+2 declared links, fixed directory inventory≤24. Это logical fixture ceiling, не Docker volume hard quota. Ready и audit каждый≤4096 UTF-8 bytes, exact key sets; для10DB использовать фиксированную bounded проекцию с hashes/counters, без повторения SQL/текста. Audit не принимает truncated или частичный success. Stderr сохраняет только bounded classifier/count/hash≤16KiB; общий Docker logs response64KiB.

20s per-container,150s work и180s total controller deadlines; API request≤3s в оставшемся budget. Это **не TTL ресурсов при потере SSH/host**. Все terminal inspect обязаны иметь OOMKilled=false, в том числе writer137. Runtime/ready/phase error прекращает дальнейшую работу, не запускает более permissive профиль.

## 7. Journal, crash и cleanup

Новый namespace `soty-reader2-canary-<fresh32hex>` с собственным exclusive journal; старые volume/container IDs запрещены. Каждый CREATE/START/KILL/DELETE имеет один fsynced intent до отправки. Ambiguous response разрешает только GET exact identity/state; mutation не повторяется. KILL допустим только единственному writer после exact ready; self-exit/checkpoint вместо crash не допускается.

До каждой mutation проверяются allowed phase/owned exact name/ID/image/config/mount и running inventory. Serving допускает только выбранные GET, никогда update/STOP/START/DELETE или mount. Config drift, unexpected Env/restart/mount, другой volume user, неясный KILL или OOM означают failure/reconciliation.

Cleanup разрешает unforced DELETE только подтверждённым stopped exact task containers; volume только после проверки отсутствия users. GET404 подтверждает эффект. Counter DELETE означает попытки/intent, не успех. При running/ambiguous/identity error никаких повторных KILL/STOP/force sweeps: сохранить owned IDs/journal, `needsReconciliation=true`, `ok=false`; отдельный reviewed recovery решает остаток. Полный PASS требует всех expected phase outcomes, main/WAL witnesses, confirmed cleanup и serving ID/image/StartedAt invariance до/после.

## 8. Локальная приёмка и следующий scope

После утверждения плана — отдельные новые controller/fixture/test файлы с префиксом `p4-reader2-linux-canary`, без imports из старого executable bundle. Разрешено перенести просмотренную controller логику review3 в новый source с отдельным diff/review; её старые bytes остаются нетронутыми. Root владеет future result document и любой remote командой.

Bounded serial local gate до bundle:

1. Literal1/2 provenance и all-object comparison; фактические emitted writer/matrix/bridge/audit над temp, genuine main1/WAL2 и внешний kill с ожиданием exit до cleanup. Windows permission limitations не маскируются: foreign UID/chown и actual symlinks — явные Linux gates.
2. Все phase counts/limits, ownership и serving allowlist, CAP_CHOWN только writer, OOM137 refusal, missing/extra fields, expired work budget, unknown actual image, one-send reconcile и retained resources при ambiguous KILL.
3. Raw chunked HTTP exact/+1byte limits, оба inspect command fields/escaping, ID и pre-ID name allowlist; actual emitted ready/audit sizes и полный command/CREATE/inspect-reserve table.
4. Static immutable prepare `wx`, pin/source/hash tamper refusal, восстановление всех embedded programs из literal inputs; повтор prepare не overwrite.

Только после final root B1b gate, local source gate, независимого **exact bundle** review и отдельного root разрешения возможна одна remote попытка нового SHA. Сейчас executable source/bundle не создавался, тесты или remote не запускались. Full image, bootstrap, backup/restore и migration admission этим планом не закрываются.
