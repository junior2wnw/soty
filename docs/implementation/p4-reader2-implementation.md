# P4-B1b — независимый host reader2

30.09.2026. Локальная реализация по [согласованному подплану](p4-reader2-rollout-plan.md). Source/API заморожены; авторское покрытие после узкой provenance correction — **65 PASS / 1 явный platform skip**, двумя serial прогонами, описанными ниже. Этот документ не является разрешением START, migration или production bootstrap.

## Изменение и границы

`storage-probe.mjs` узнаёт Notes1/2 и Capabilities1/2 независимо. `storage-guard.mjs` сохраняет strict v3 envelope и прежние exports; только разрешённые версии двух новых stores расширены до `[1,2]`. Формат и START receipt содержат те же exact keys, Notes/Capabilities — `'empty' | 1 | 2`. Число0 не является успешным форматом существующего SQLite файла. Unknown3, malformed schema, missing v2 identity и старые manifest/receipt v1/v2 отвергаются.

Reader2 image declaration:

```json
{"version":3,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6],"notes":[1,2],"capabilities":[1,2]}}
```

Image с честным v3 `[1]` распознаётся, но получает `storage_reader_incompatible` на actual2. Новый host code не повышает его возможности. Dockerfile, actual images, rollout/controller и restore принадлежат root; автор их не менял. Exact Dockerfile/manifest assertion в tests сохранён, без обхода через copied container label.

Rooms1–2 и Apps1–6, filesystem checks, normal read-only WAL transaction, NoCopy/RO helper, topology/writer checks и fresh START admission не меняются. Probe импортирует только Node builtins; candidate application/schema/fixtures не исполняются. Он не мигрирует, не создаёт отсутствующие stores, не repair/checkpoint и не выводит registry IDs или строки доменных данных.

## Независимые определения

Frozen v1 literals/source/provenance из `6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e` не менялись. Новые literal additions и provenance закреплены на принятом B1a `5e459abc6afa376861c2032226bd29f78bf0468d`:

| Store | V2 literal LF bytes | SHA256 | Всего объектов без `sqlite_*` |
| --- | ---: | --- | ---: |
| Notes | 2458 | `e15a1dc6d57dfe6c3de56e6d5052f42080955612f56b39a70f21fc8c93ff0add` | 19 |
| Capabilities | 5122 | `64c394ad739eba70bea95e2ffb18c29d2f0753b55c1b4373060620fde650a8c8` | 36 |

Exact host maps проверяют полный SQL двух новых tables (PK/UNIQUE/FK/CHECK/STRICT), 17 guards и четырёх indexes, включая partial predicate. В Notes только новая `note_native_creates` является STRICT: исторические non-STRICT таблицы и FTS virtual/shadow kinds остаются прежними. Capabilities tables STRICT. Полный известный object inventory запрещает extra view/trigger/index/table; fixed projections запрещают missing/extra/hidden columns.

Metadata имеет точные key sets: Notes1 `lineage,project_id`; Notes2 добавляет `registry_id`; Capabilities1 `lineage`; Capabilities2 добавляет `project_id,registry_id`. Проект строго `soty`, lineage соответствует версии, registry — строка32lowercasehex. SQL читает не более4 metadata rows и ограничивает выдаваемые key/value32/128 символами; oversized/BLOB/null/extra metadata fail closed. Это форматное ограничение, не полноформатная проверка происхождения или содержимого store.

## Авторская проверка

Выполненная команда после handoff test slot (PATH также у дочерних writers):

```powershell
$readerRuntime = Join-Path (Get-Location).Path 'var/toolchains/node-v24.21.0-win-x64'
$env:PATH = $readerRuntime + [IO.Path]::PathSeparator + $env:PATH
& (Join-Path $readerRuntime 'node.exe') --test --test-concurrency=1 deploy/connector/storage-native-v2.test.mjs deploy/connector/storage-notes-capabilities.test.mjs deploy/connector/storage-guard.test.mjs
```

Новый файл проверяет все9 mixed pairs; exact SQL и default-off reopen с **неизменённым dependency graph B1a**, восстановленным из Git pin в temp; synthetic retained proof после human edit/tombstone и native pending/terminal ledger seed; main1/WAL2 с открытым и реально завершённым writer; frozen bridge ae914f5 refusal против настоящего v2; byte-identical main/WAL; каждый guard/index и malformed table constraints; exact metadata; future3 и повторный свежий guard после ранее успешного START receipt. Domain state намеренно seeded: это доказательство чтения/сохранности, не выполнения native effect.

Существующий v1 suite сохраняет historical fixtures и unknown marker2 case. Его название уточнено: одна смена marker2 без v2 layout не становится настоящим native2. Guard tests теперь включают2 как допустимый scalar и3 как неизвестный; отдельно доказывают, что old v3 `[1]` после перехода не получает START. Независимые acceptance файлы автор не меняет.

Actual isolated runtime: Node24.21.0 / SQLite3.53.4. Первый serial прогон: **66 total / 61 PASS / 4 FAIL / 1 SKIP,21991.7103ms**. Все четыре fail оказались ошибкой авторского provenance test до вызова старого probe: Git blob ae914f5 имеет LF SHA `d0aed27ce790502f36df20689663a43aee0ed28266e6b424a002cde7eb2f4e17` (24317 B), прежний working/canary source имел CRLF SHA `d51b36c12e316e660fd2b3ab6d8fe2350833aff242f2ae95d296fb63409afd59`. Проверка теперь явно сверяет оба известных представления, затем исполняет точный неизменённый Git LF blob в temp. Production source не менялся после первого прогона.

Повторены только четыре затронутых случая:

```text
node --test --test-concurrency=1 --test-name-pattern="genuine main1|exited real writer" deploy/connector/storage-native-v2.test.mjs
4 tests / 4 PASS / 0 FAIL / 0 SKIP,1142.1208ms
```

Итого по покрытию65 PASS/1skip; это **не** заявляется как один полный зелёный прогон. Initial log `output/implementation-20260930/p4-reader2-author.log` и correction log `p4-reader2-wal-focused.log` сохранены отдельно. Единственный skip — существующий Windows file-symlink privilege case; directory junctions прошли. Новых17 reader2 cases после correction закрыты все. Root Dockerfile assertion также прошёл после его отдельного exact label edit; это текстовый composition proof, не сборка или запуск Linux image.

Syntax семи `.mjs`, provenance JSON и `git diff --check` PASS. Изменённые production probe/guard байты оставались теми же на обоих прогонах. Тестовый слот освобождён после focused gate; независимое review и единый integrated deploy run принадлежат root/reviewer.

## Freeze

| Файл | SHA256 рабочих bytes |
| --- | --- |
| `deploy/connector/storage-probe.mjs` | `5d7bdb6cee8c38671ff9a188e26b4bc33e983fbef04ec3e3ac9cabd9338f650a` |
| `deploy/connector/storage-guard.mjs` | `95e0a74b818a6bec44b0994109bc0f26019df6240e929be1da5a617ca3454cca` |
| `deploy/connector/storage-native-v2.test.mjs` | `767337e09a7c6e0d02e1937a6504890d4969a0f99c9f3645f3a40ae934d08539` |
| `deploy/connector/notes-v2.fixture.mjs` | `2f5ad838eb0e23990c8a97afd4a89654b614c878dcee85ddf611fd551c7fbb2d` |
| `deploy/connector/capabilities-v2.fixture.mjs` | `235c27a9d9d0334f9afb41cca19365ecf8d51be34ee2652942aa7cfdf4f46752` |
| `deploy/connector/fixtures/notes-v2/provenance.json` | `f06e5ba78818934973147470152983544cbc6d5ed49902d1edd55ae0411b0bbf` |
| `deploy/connector/fixtures/capabilities-v2/provenance.json` | `405873597202d5f77a19c19a692fb7059f05a3252501515f6ba7aabe53dc61c0` |
| `deploy/connector/storage-guard.test.mjs` | `10b4b29f0a5b1820dbb29b265523d5004feaa33b595880520dae24b8965e51d5` |
| `deploy/connector/storage-notes-capabilities.test.mjs` | `37487e4ed5ed6622881999dd6e97747d4f95bec61222a9f777ed724b257c94d8` |
| `deploy/connector/README.md` | `220bbc478c1d08e4fc1a0b55ea0199243a9d78069baac9b1e34e6de8931662ff` |

Все исходные v1 fixture/provenance файлы и Rooms/Apps readers/tests не изменялись автором. Независимые acceptance не редактировались. Root владеет actual image label/composition и orchestration; никакого old-reader bypass не добавлено. Коммитов, SSH, remote/container/real-volume действий автор не выполнял.

## Ещё открытые gates

Независимый reviewer и root integrated deploy regression идут после author gate. Нужны новые exact Linux reader2/RO-WAL fixtures и полный default-off application/fallback image, actual first-unlabelled bootstrap, encrypted whole-generation cold restore и отдельное migration admission. Ни label, ни локальный Windows gate не заменяют эти доказательства.

Root выполнил предыдущий **bridge1** review3 один раз: Linux Node24.15.0/SQLite3.51.3/FTS, Rooms2/Apps6/Notes1/Capabilities1, negative2 cases, cleanup и serving unchanged. Это отдельный [root result](p4-storage-linux-canary-result.md). Все canary sources/bundles/receipts заморожены; здесь они не менялись и не повторялись. Reader2 Linux artifact ещё не создан.

Форматный probe не сертифицирует B-tree/row/FTS integrity, валидность каждого retained effect, cross-store snapshot или полный physical storage budget. Неизвестный незакоммиченный WAL tail SQLite может игнорировать. Connect/World также требуют самостоятельных compatibility/restore gates.
