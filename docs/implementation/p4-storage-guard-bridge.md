# P4-B1 — strict v3 storage bridge, Notes1 / Capabilities1

30.09.2026. Author source/test freeze. Реализован локальный bridge из [preflight](p4-storage-guard-preflight.md): независимые Notes1 и Capabilities1 readers добавлены к прежним Rooms1–2 / Apps1–6. Это не готовность к Notes2/Capabilities2 migration, Linux rollout или production restore. Independent review и итоговую composition regression проводит root/critic отдельно.

## Изменение и граница

Actual image reader label теперь только:

```json
{"version":3,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6],"notes":[1],"capabilities":[1]}}
```

`soty.storage-format.v3` содержит точные `ok,schema,rooms,apps,notes,capabilities`; `soty.storage-start.v3` — `schema,containerId,image,mountSha256,rooms,apps,notes,capabilities`. Manifest, DTO и receipt принимают только известные scalar formats и собственные точные ключи; array→string coercion identity/label/receipt не принимается. Новые readers пока знают только `empty|1`. V1/v2 manifests и receipts отвергаются даже на пустом volume. Historical receipt не переписывается; завершённый helper со старым v2 output остаётся в журнале и не запускается повторно.

`storage-probe.mjs` использует только `node:fs/promises`, `node:path`, `node:sqlite`. Candidate code, Notes/Capabilities migrators и test fixtures не импортируются. Existing Rooms/Apps recognition, image/mount identity, managed single-writer profile, ресурсы helper, RO+NoCopy/no-network и single-shot journal поведение сохранены.

Notes: `/data/notes/notes.sqlite`, user_version1, `soty.notes.sqlite.v1`, project `soty`. Проверяются известные 10 tables (четыре domain, FTS5 virtual и пять shadow), их exact columns/hidden FTS fields, exact virtual definition и два index definitions. Capabilities: `/data/capabilities/capabilities.sqlite`, user_version1, `soty.capabilities.sqlite.v1`, 12 известных STRICT tables с exact columns и восемь index definitions. Unknown/missing tables, columns, views, triggers, indexes, altered FTS module/tokenizer/prefix и non-STRICT Cap table отказывают. Идентификаторы SQL — только host constants.

Каждая новая папка отсутствует/действительно пуста либо содержит main и допустимые SQLite sidecars. Orphan/unknown files, symlink/junction, не-файл main/sidecar, короткий/повреждённый main — отказ, не initialization. Открытие — normal SQLite `readOnly:true`, `query_only=ON`, read transaction, прежний finite SQL busy timeout; нет journal-mode write, checkpoint, migration, FTS rebuild или immutable fallback.

Это **format recognition**, не полная проверка всех B-tree pages, rows, constraints, FTS contents, бизнес-прав или межфайловой Notes↔Invocation согласованности. `LIMIT 0` не проходит все данные. SQLite может игнорировать повреждённый незавершённый WAL suffix; этот bridge не сертифицирует любую последовательность WAL bytes как целую. `-shm` — ephemeral locking/index state; byte-preservation evidence ниже относится к main/WAL, а не к обещанию неизменных shared-memory lock bytes.

## Historical baseline

Pin: `6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e`.

- `notes-v1.fixture.mjs` и `capabilities-v1.fixture.mjs` содержат literal historical DDL, независимо от current migrations.
- `fixtures/notes-v1/schema.mjs` и `fixtures/capabilities-v1/schema.mjs` — неизменённые Git bytes с отдельным provenance. Оба historical schema modules self-contained; нет local dependency substitution.
- Тест исполняет literal и exact old migrator в разных настоящих SQLite и сравнивает все non-internal SQL objects с нормализацией только formatting/case вне quoted literals: Notes12 и Capabilities20.
- Synthetic populated Notes содержат active/archived/trashed/deleted, FTS и receipts; Capabilities — clients, principal, root/child grants, credential digest, audit, budget/reservations, pending/committed invocation, dispatch и receipt. Это storage fixtures, не отдельное доказательство native effect service/API.

SHA source Git LF: Notes `da75ffc702fb0ee4db9878780d374f53bca0d68b46e91eafb56038b45d1e0dfa`; Capabilities `959fdb4938d6b0ebc9359817b001f0eed08be70e437083648e9ac259c56229ad`. Tests допускают только checkout CRLF normalization при сравнении с историческим digest.

## Выполненные проверки

Оба запуска выполнены последовательно после освобождения root test lock. В PATH конкретного shell процесса поставлен `var/toolchains/node-v24.21.0-win-x64`; фактически проверены **Node24.21.0 / SQLite3.53.4**. Глобальные runtime/config настройки не менялись.

```powershell
$env:PATH = (Join-Path $PWD 'var/toolchains/node-v24.21.0-win-x64') + [IO.Path]::PathSeparator + $env:PATH
node --test --test-concurrency=1 deploy/connector/storage-guard.test.mjs deploy/connector/storage-apps.test.mjs deploy/connector/storage-apps-v5.test.mjs deploy/connector/storage-apps-v6.test.mjs
node --test --test-concurrency=1 deploy/connector/storage-notes-capabilities.test.mjs
```

| Авторский запуск | Total | PASS | FAIL | SKIP | Время |
| --- | ---: | ---: | ---: | ---: | ---: |
| Guard + existing Apps regressions | 54 | 53 | 0 | 1 | 34.441 s |
| New Notes / Capabilities | 20 | 19 | 0 | 1 | 8.982 s |

Итого этих двух запусков: **74 / 72 PASS / 0 FAIL / 2 SKIP**. Оба skip явно относятся к Windows permission на file symlink (старый Apps case и новый Notes/Cap subtest); directory junction negatives выполнены. Linux file-symlink/RO-mount gate остаётся обязательным. Whitespace diff checks PASS; предупреждение checkout LF→CRLF не скрывается как test failure.

Новые проверки включают:

- absent/empty без создания store; independent Notes-only/Cap-only/оба; normal cold file reopen, FTS search, retained data/receipts;
- настоящий committed v1 schema+data в WAL при raw main user_version0; normal RO возвращает1, main/WAL hash сохраняются;
- отдельно Notes и Capabilities: child process пишет FULL-synchronous WAL и fixed readiness; родитель завершает только своего writer, ждёт exit, затем проверяет cold main0/WAL1, неизменные main/WAL bytes и набор файлов. Worker завершён до temporary-directory cleanup;
- future version2+marker, записанные **только в committed WAL** поверх main1, отказывают через настоящий `guardStorageStart` с RO probe, без успешного start receipt; это synthetic unsupported marker, не реализованная migration2;
- broken filesystem/layout/metadata, altered indexes/FTS, missing/new columns, unknown triggers; настоящий exclusive SQLite writer → bounded unreadable error, после release тот же store читается;
- v3 strict fields, independent formats, array coercion refusal, actual old-image label refusal до создания helper, старый helper без повторного CREATE/START; прежние Rooms/Apps/WAL/immutable guard assertions не ослаблены.

Все данные synthetic. Настоящий Docker/SSH/production volume или application image не запускались. Контекст Docker API в guard tests — explicit mock; cold-writer subprocess и SQLite/WAL — настоящие локальные процессы/файлы. Полный deploy/world suite автор не запускал.

## Freeze SHA256

Хеши ниже относятся к фактическим bytes рабочей копии на author freeze.

| Файл (от repo root) | SHA256 |
| --- | --- |
| `deploy/connector/storage-probe.mjs` | `d51b36c12e316e660fd2b3ab6d8fe2350833aff242f2ae95d296fb63409afd59` |
| `deploy/connector/storage-guard.mjs` | `cd10d07f1f2b75e2f8c05ca469ae97f3bb7fb35ba76ebf085345eb99530f3a1c` |
| `deploy/connector/storage-guard.test.mjs` | `ffa620ae9e79bcd70fdc9b18d9011376162d9d8a9cae9d3b9968ee3e127e82a2` |
| `deploy/connector/storage-apps.test.mjs` | `813ef3842bd5a495f5d06166a210e7c1b5c4b4981a2c438f8f6fbf0790d0fe55` |
| `deploy/connector/storage-apps-v5.test.mjs` | `6aa215ad3fb81485e28062ae56567de273cc351e43ae1d22b08864cd12f5901b` |
| `deploy/connector/storage-apps-v6.test.mjs` | `eadac11565b783267cb4f5fe6751294cd5fd1d6bf961a8322ca6fa6037408b82` |
| `deploy/connector/storage-notes-capabilities.test.mjs` | `85b2c2884325dc2df46a479c90f85a14dbcb022f249dfe9fa073ce2b6719c7ec` |
| `deploy/connector/notes-v1.fixture.mjs` | `d1c4522d53313265c3d635ce4705b5559103ebc99bde06dd2f0031c2a0c1d1e6` |
| `deploy/connector/capabilities-v1.fixture.mjs` | `7eb6d77bb75124dcafaab181eb3f299fc83a4c4ef3feb74307e822030a6f4539` |
| `deploy/connector/fixtures/notes-v1/provenance.json` | `d2a3ae1ec682d758e5f2c919cd92d86453ef921713e8f5a20ed345bec2bb503d` |
| `deploy/connector/fixtures/capabilities-v1/provenance.json` | `ab12824df88a1d21f85470249cc892665ec783b72d7b95b3e1e887ef746933a4` |

## Следующие gates и ownership

Root владеет Dockerfile, rollout/controller/rebase и их composition tests; автор их не редактировал. Изменение текущих Apps tests ограничено v3 envelope/reader declarations, без переписывания historical schemas. Новый независимый acceptance принадлежит critic. Author source frozen перед независимым review; новые findings сначала сообщаются, затем исправляются в своей зоне.

Strict v3 запрещает automatic rollback первого bridge на SAME old v2-labelled container даже при Notes/Cap1. Root composition должен отказать до STOP/serving mutation, если actual old image недопустим. Нужен отдельный reviewed production bootstrap/recovery, а перед последующими schema2 writes — реально совместимый reader1/2 old baseline либо отдельный холодный recovery. Файл fallback, новый label или выключенный endpoint этого не доказывают.

Connect/World inventory остаётся явно вне этих новых reader declarations. Exact Linux image runtime/FTS5/RO WAL, hooks/default volumes/rootless permissions, serving invariants и lost-response reconciliation ещё не выполнены этим checkpoint. Root registry observation Node24.15.0 — image metadata; Windows24.21.0 test execution не заменяет Linux proof. Cold encrypted all-store backup/isolated restore до production migration также открыт. Reader2 можно объявить только отдельным последующим проверенным checkpoint; этот bridge его не содержит.
