# P4-B1a — Notes/Capabilities reader baseline

Дата: 2026-09-30. Авторский локальный срез после bridge `ae914f55e6d8d64628a7279189d2d55dfd05de45`. Основание — [native storage contract](p4-native-storage-contract.md), SHA256 `2ae6181faef1d38d3d4ddd7825c96398bd1a3d967e827abd21f4b21a1df0c273`, включая принятые уточнения двух native fingerprints после независимого review.

## Результат и граница

Notes и Capabilities читают известные v1/v2. Новая или v1 БД по умолчанию остаётся **actual v1**. Только trusted constructor `allowNativeMigration:true` разрешает v2; `null`, строки, числа и объекты отвергаются до создания файла. В существующей v2 отсутствующий registry ID не восстанавливается и не генерируется заново. Произвольные будущие версии, неизвестные/частичные DDL, изменённые critical guards и несовпадающий проект закрыты.

В этом срезе нет native admission/execution/reconciliation, HTTP write route, OAuth, новых credentials или fake device actor. `notes.createDraft@1` остаётся disabled, semantic digest `95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204`, catalog/input profile/public documentation не изменены. Запрет непредставимого Unicode и проверка Notes defaults до admission остаются B2; v1 текст не нормализуется и не переписывается.

Deployment bridge/Docker labels по-прежнему разрешают Notes `[1]` и Capabilities `[1]`. Этот локальный код не является доказательством запущенного совместимого fallback image. Reader2 manifest, Linux RO/WAL/topology, cold backup/restore и реальный **SAME old container** остаются отдельными gates.

## Export / host integration

```js
// modules/notes/server/schema.mjs -> schema-v2.mjs
SCHEMA_VERSION === 2
SUPPORTED_SCHEMA_VERSIONS === [1, 2]
inspectNotesSchema(db, projectId)
migrateNotes(db, projectId, { allowNativeMigration = false } = {})

// modules/capabilities/server/schema.mjs -> schema-v2.mjs
CAPABILITIES_SCHEMA_VERSION === 2
CAPABILITIES_LINEAGE === 'soty.capabilities.sqlite.v2'
CAPABILITIES_SUPPORTED_SCHEMA_VERSIONS === [1, 2]
inspectCapabilitiesSchema(db, { projectId })
initializeCapabilitiesSchema(db, { projectId, allowNativeMigration = false })
```

Миграторы возвращают frozen `{schemaVersion, registryId}`: actual 1/null или 2/32 lowercase hex. Read-only inspect дополнительно распознаёт пустую БД как 0/null. `projectId` — обязательная trusted строка; v1 Capabilities исторически не имела project metadata, поэтому её durable binding появляется только в явной v2 transaction. Notes сохраняет прежний project binding.

Оба service constructor принимают строгий boolean `allowNativeMigration`, Capabilities теперь обязательно принимает trusted `projectId`. Возвращают `projectId`, `schemaVersion`, `registryId`, `supportedSchemaVersions` как сведения, прочитанные при открытии. Это не живая cross-DB authority. B1 не экспортирует `native.*` executor API. Root владеет HTTP composition и отдельно передаёт projectId; новые options не принимаются из RPC args.

Exact recognizer создаёт маленькую независимую in-memory reference из literal v1 + согласованного v2 DDL и сравнивает known SQL objects, сохраняя содержимое SQL-литералов. Он не импортирует исторический fixture/runtime candidate и не изменяет проверяемую БД. Persistent pragmas выполняются только после первого exact layout/metadata recognition; внутри `BEGIN IMMEDIATE` recognition повторяется. Bounded row iteration не создаёт whole-account массив body.

## Frozen DDL

DDL соответствует SQL принятого contract; новых отступлений нет. Исторические v1 tables/indexes/FTS definitions не меняются.

| Store | Marker / user_version | Добавления |
|---|---|---|
| Notes2 | `soty.notes.sqlite.v2` / 2 | `notes_meta.registry_id`; `note_native_creates`; 6 triggers |
| Capabilities2 | `soty.capabilities.sqlite.v2` / 2 | `cap_metadata.project_id`, `registry_id`; `cap_native_note_intents`; 4 explicit indexes; 11 triggers |

Notes triggers: `note_native_create_no_update`, `note_native_create_no_delete`, `note_native_create_no_replace`, `notes_identity_no_update`, `notes_identity_no_delete`, `notes_identity_no_replace`.

Caps indexes: `cap_invocations_native_identity`, `cap_invocations_account_admission`, `cap_invocations_principal_admission`, `cap_invocations_nonterminal`. Caps triggers: `cap_native_note_admission`, `cap_native_note_no_replace`, `cap_native_note_no_delete`, `cap_native_note_update_guard`, `cap_native_note_input_guard`, `cap_native_receipt_no_update`, `cap_native_receipt_no_delete`, `cap_native_receipt_no_replace`, `cap_identity_no_update`, `cap_identity_no_delete`, `cap_identity_no_replace`.

Registry IDs создаются один раз внутри той же migration COMMIT, что marker/version/DDL. Fault перед COMMIT откатывает весь этот набор. Миграции двух stores независимы: Notes2/Caps1 и Notes1/Caps2 допустимы для обычных операций и не означают readiness native effect. Неатомарный bootstrap двух файлов не маскируется общей transaction.

Существующие rows проверяются один раз перед DDL. Добавляемые native tables пусты, v1 rows не переписываются и native proof для старого Invocation не выдумывается. Notes сохраняет plain `PRAGMA quick_check`, FK, row/FTS/account-usage проверки; Caps сохраняет FK и проверяет новые native row relationships, immutable identity/digests, dispatch marker, input/receipt/settlement shape. Это не криптографическое доказательство содержимого или coordinated restore двух БД.

## Baseline safe-hold

`native-baseline.mjs` проверяет наличие таблицы при каждом guard, включая v1 connection, которую другой совместимый writer перевёл на v2. Отрицательный результат не кешируется навсегда.

- Generic `peekDispatch` исключает native records до limit. `beginDispatch`, `bindJob`, `markUncertain`, `recordResult` отказывают `native_reconciliation_required`.
- Generic direct settlement не может сделать native `spent`/`released`; допускается только удержание `uncertain`.
- `requestCancel` и denied `reconcileAuthorization` сохраняют nonterminal `cancel_requested`, тело и reservation. Ни отсутствие job, ни pending delivery не считаются native negative proof.
- Unresolved record с durable `started_at` показывается как `effectState:'unknown'`. Baseline не выводит «эффекта нет» из старого значения `none` между двумя COMMIT.
- Terminal native receipt остаётся доступным через current read ACL при operational disable; cancellation/reconciliation его не переоткрывают. Generic dispatcher не выполняет такой record повторно.

Это намеренное удержание, а не реализация будущего proof-first recovery. Даже если Notes proof уже есть, baseline не читает его как разрешение settlement. Полный B2 должен добавить отдельный coordinator и external replay-before-readiness; нынешний generic admit не объявляется таким API.

## Исторические fixtures и проверки

Historical pin: `6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e`. Read-only reuse frozen bridge fixtures:

- `deploy/connector/fixtures/notes-v1/schema.mjs`, exact Git LF SHA `da75ffc702fb0ee4db9878780d374f53bca0d68b46e91eafb56038b45d1e0dfa`;
- `deploy/connector/fixtures/capabilities-v1/schema.mjs`, exact Git LF SHA `959fdb4938d6b0ebc9359817b001f0eed08be70e437083648e9ac259c56229ad`;
- независимые literal `notes-v1.fixture.mjs` и `capabilities-v1.fixture.mjs`, не current schema с переименованной версией.

Полный авторский serial run до двух review corrections: **148 tests, 148 PASS, 0 FAIL, 0 SKIP**, ~8.5 s. Включал все существовавшие тогда `modules/notes/test/*.test.mjs` и `modules/capabilities/test/*.test.mjs`; новых storage cases 29 (Notes11 + Caps18). Runtime фактически Node **v24.21.0**, SQLite **3.53.4**, PATH prepended к isolated runtime и унаследован workers. Это локальный Windows результат, не Linux/production proof.

Команда из repository root:

```powershell
$runtimeDir = Join-Path (Get-Location).Path 'var/toolchains/node-v24.21.0-win-x64'
$env:PATH = $runtimeDir + [IO.Path]::PathSeparator + $env:PATH
$cases = (Get-ChildItem modules/notes/test,modules/capabilities/test -Filter '*.test.mjs').FullName
node --test --test-concurrency=1 $cases
```

Новые tests проверяют default-off/fresh/literal-v1, stable IDs/reopen/project mismatch, rollback перед migration COMMIT, two-worker SQLite migration с ожиданием настоящего exit, mixed bootstrap, altered UNIQUE/guard/future3 refusal, no-replace/no-delete/native-input/receipt immutability, actual Notes edit→trash→purge при сохранённом proof, FTS/usage/старые receipts, generic safe-hold и прямой settlement bypass, terminal/no-reopen, index query plans. Real old migrator вызывается и против checkpointed v2 main, и против **genuine v1 main + committed v2 WAL**; при удерживаемом writer main/WAL hashes не меняются. DELETE-mode old refusal действительно меняет persistent journal mode; test фиксирует это как несовместимость, а не как byte-unchanged PASS.

Native ledger/proof rows в новых тестах явно сеются fixture. Один cross-store crash-state case делает настоящую обычную Notes domain write и отдельно сеет future-format immutable proof, затем проверяет reopen/hold baseline между Notes commit и отсутствующим Caps receipt. Это не доказательство native admission, общей atomicity, opaque context или execution under authority fence.

Старый isolated Invocation harness добавляет test-only policy/budget tables. При его reopen fixture больше не вызывает production exact recognizer над заведомо посторонними таблицами; connection pragmas/busy timeout сохраняются. Полные service/schema tests используют неподменённый layout. Остальные legacy fixture изменения — только обязательный trusted projectId.

## Выявленный quick_check / WAL seam

Первая попытка разместить проверку перед переключением DELETE→WAL дала реальное `SQLITE_LOCKED` на последующем checkpoint. Сохранён самостоятельный диагностический script `modules/capabilities/test/support/sqlite-checkpoint-probe.mjs`; он печатает только runtime/version и фиксированные SQL/result, не domain data. Это диагностический вывод, не тест, требующий сохранения дефекта будущего runtime.

На Node24.21/SQLite3.53.4 наблюдалось:

| Последовательность на одной connection | checkpoint |
|---|---|
| DELETE → `quick_check`/`quick_check(1)` `.all()` → WAL | `database table is locked`, SQLite code 6 |
| DELETE → `quick_check(1)` `.get()` → WAL | тот же отказ |
| WAL → plain `quick_check` `.all()` внутри BEGIN → COMMIT | OK |
| `foreign_key_check`, `user_version`, schema `.all()` → WAL | OK |
| После закрытия/reopen каждого диагностического файла | OK |

Наличие/отсутствие BEGIN не изменило первый результат. Причина внутри SQLite/Node этим опытом не установлена; это **не утверждение об утечке cursor Node**. Решение Notes сохраняет исторический lifecycle WAL-before-plain-quick_check, но добавляет exact-format recognition **до** persistent PRAGMA и повтор внутри transaction. При known layout с испорченными rows journal mode уже мог измениться, однако v2 DDL/registry/version не коммитятся. Regression реально повреждает freelist count header offset36, оставляя DDL/rows; `notes_storage_corrupt` блокирует migration. Offset проверен по [SQLite file format §1.3.8](https://www.sqlite.org/fileformat.html#free_page_list). Дополнительный regression доказывает DELETE→reopen/migrate→checkpoint и один integrity/body проход.

Промежуточные failed runs не приписываются final PASS: сначала 21/24 (WAL seam и premature worker read), затем 144/145 (test-only reopen потерял busy timeout), затем отрицательная corruption fixture исправлялась до правильного header offset. Последний полный прогон выше зелёный.

## Независимые findings и повтор после исправления

Критик воспроизвёл persisted RED: terminal native receipt с digest из 64 нулей проходил reopen. Reader теперь пересчитывает единственную согласованную норму `canonicalHash({status,effectState,effects,receipt,disposition,actualCharges})`, success charges1 / failed или cancelled null. Generic v1 completion и DDL не менялись.

B2 preflight автора обнаружил вторую границу, независимо воспроизведённую критиком: input `{title:'',body:'中'.repeat(87350)}` занимает 262072 B, полный Notes document 262131 B, но fingerprint envelope — 262359 B. Общий `canonicalHash` с пределом 262144 B отказывал reader. Новый fixed `native-note-contract.mjs` строит только точный Notes @1 envelope, сохраняет прежние canonical bytes/SHA256 и раздельно ограничивает input 262144 B / envelope 294912 B. Общий hash, pinned input validator и semantic digest не расширены. Этот helper будет единственным источником request fingerprint для B2 admission; сам по себе он не выполняет admission или effect.

После обоих исправлений автор повторил **47/47 PASS, 0 FAIL, 0 SKIP**, ~4.8 s, последовательно на том же runtime: 32 собственных cases (29 storage + 3 нового helper) и 15 независимых cases критика. Оба его persisted RED стали GREEN. Helper tests дополнительно проверяют старую норму малых digest, точный +1 byte overflow, отсутствие caller-selected contract fields и чтения getter. Общий regression после этой узкой delta передан root; старый 148/148 не выдаётся за его повтор.

```text
node --test --test-concurrency=1 modules/capabilities/test/native-note-contract.test.mjs modules/capabilities/test/native-storage.test.mjs modules/notes/test/native-storage.test.mjs modules/capabilities/test/native-storage.acceptance.test.mjs modules/notes/test/native-storage.acceptance.test.mjs
```

README constructor теперь явно передаёт обязательный trusted projectId и описывает actual v1/v2/default-off migration. Независимые fixtures/tests принадлежат критику; автор их не изменял.

## Source freeze (SHA256 working bytes)

| Файл | SHA256 |
|---|---|
| notes/server/schema-v2.mjs | `2a9f8b51f408179a6bb5333da63b2fd9b2452313b15597f01165aeba66be6fda` |
| notes/server/schema.mjs | `ffcf54733ad113c6470e0e1bdbbb4500aa5b738f68879d8fd3fd12c4beaa3a79` |
| notes/server/index.mjs | `970450fa7f65a30ff55cff16675b508233d4e684284db49b274c333a1bc7e5a6` |
| notes/test/native-storage.test.mjs | `c8d3ea633fa6f67f562916e2f31687dede27593cd18a49ad5ac6c1275f18a498` |
| capabilities/server/schema-v2.mjs | `5ca8b4025535566c90a1b1d610e37837b4b41e6074117a96fe720504050af25f` |
| capabilities/server/native-note-contract.mjs | `1f3ab19fa9e6641e9928ce363ab6fdcbcf1421d4785256466a430ce0bcfa9c83` |
| capabilities/server/schema.mjs | `43b20b072618aeade19cb7fae670d5c1b6b3df682486f4bf210b25fb2f29942b` |
| capabilities/server/index.mjs | `0f36bcfd05cfe23d8f4e26f9836dd52511685c8799117ddc3395dcca5ca5b580` |
| capabilities/server/access.mjs | `d3f4a76e63c378b19966aa753bf1645c74b9da2a47686722f630be23cfa2c9a6` |
| capabilities/server/invocations.mjs | `b39566f0c419756035281256aca07de80f9e8f5dcb7e6ab88a6106ce8d77146b` |
| capabilities/server/native-baseline.mjs | `68e877362f32905642772414d9b40448762684d066d59d8810f32d5ca72d25ad` |
| capabilities/test/native-storage.test.mjs | `9cf9df6617a992df012cca2d3a78fe07108cb8f9591d33323b82474559bc1aea` |
| capabilities/test/native-note-contract.test.mjs | `73b87228cc8616302f4217028abc895e1f235a6b36728b94fd017f9d20105d0e` |
| capabilities/test/support/native-storage.mjs | `3d1b7c6d3fcb7d6b7bdf7c00c57c5f1eabc34ea180371da8667f9f006b652eee` |
| capabilities/test/support/sqlite-checkpoint-probe.mjs | `d40c6a86e17e24313c4315de64d92893d7be4c4205591c6e5d3220a22d1476d2` |
| capabilities/README.md | `b0b070f8c759de1e93d0c77b46624bc519a4ae17541a09927ae0a6d4f78703c6` |

Пути в таблице относительны `modules/`. Catalog `498a06025107598b450406114bc331aef71cd0824c384dfdf7927d10dc6fc072` и capabilities validation `ce7a5731ac13dbc67646ac38a94f27d012c20ba9643bcc69fc5a5d5f31fd7076` остались неизменными. Independent B1c review ещё отдельный gate; никакого commit/deploy автор не выполнял.
