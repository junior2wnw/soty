# P4-B1a — независимая приёмка reader baseline

30.09.2026. Проверены текущие Notes/Capabilities schema, service integration и generic native-hold paths. Изменены только два независимых acceptance файла и этот отчёт. Production, author fixtures/tests, Docker labels и serving storage не менялись этим reviewer.

## Вердикт и граница

Независимый gate допускает локальный reader baseline после двух узких исправлений ниже. Новая/историческая v1 по умолчанию остаётся actual v1, явная migration создаёт actual v2 со стабильной identity; generic baseline удерживает native uncertainty. Это не готовый native executor и не доказательство его authority fence, crash recovery, HTTP, Linux reader2, production rollout или согласованного restore. Deployment bridge по-прежнему заявляет Notes1/Capabilities1; `[1,2]` в service API описывает код reader, а не запущенный совместимый fallback image.

Исторические layouts получены из неизменённых schema modules `6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e`, с проверкой исходных bytes перед import. Author seed/helper не импортируется. Notes rows/FTS/receipts сеются самостоятельно; Caps principals, grants, credentials, reservation и generic receipts создаются реальными service API. Future native pins/proofs/terminal purge в fixtures добавлены явно через SQLite. Это **не** подмена отсутствующего B2 execution успешным вызовом.

## Два обнаруженных отказа и исправления

1. **Native terminal receipt принимал повреждённый digest.** Reader проверял только64 hex символа. Независимый persisted case сначала открывает правильный terminal success (`actualCharges=[{unit:'invocations',amount:1}]`), затем изменяет digest на64 нуля, восстанавливая точный SQL immutable trigger. На первоначальном source: **0/1, Missing expected exception**,197.2542 ms. После поправки reader пересчитывает существующую формулу `canonicalHash({status,effectState,effects,receipt,disposition,actualCharges})`: success использует charges1, failed/cancelled — null. Норма согласована до появления native production rows, отражена в контракте; generic v1 records/DDL не изменены. Case стал GREEN и не разрешает silent repair повреждённой записи.
2. **Допустимый Unicode input отвергался как corrupt из-за размера request envelope.** Отдельный persisted case: input262072 B, полный Notes document262131 B, request envelope262359 B. Первые два укладываются в свои262144 B, но прежний `canonicalHash` всего envelope применял input cap. Независимый RED: **0/1, capabilities_storage_corrupt**,178.2872 ms. Fixed native helper теперь отдельно ограничивает input262144 B и только точный Notes@1 envelope294912 B, сохраняя canonical serialization/SHA256. Generic helper и semantic digest @1 не расширены. Future B2 admission должен использовать этот же fixed helper.

Первую проблему нашли независимо root и reviewer; вторую нашёл domain author, reviewer закрепил собственным persisted RED. Все production исправления внёс domain author.

## Исполняемая матрица

| Независимая граница | Наблюдаемый результат |
| --- | --- |
| Populated historical Notes1 / Caps1 → default reopen → explicit2 → checkpoint/reopen | Сохранены исходные rows, IDs, Unicode, FTS/usage/receipts и legacy Invocation; default actual1/null, explicit actual2/stable registry; checkpoint не остаётся locked |
| Warm v1 Caps connection, которую другой connection мигрировал в2 | Новые native rows распознаются без постоянного cached-false bypass; dispatch inventory исключает их |
| Generic begin/bind/uncertain/result, cancel/revoke и повторный reopen | Effect paths отказывают; unresolved input/identity остаются, reservation удержана, started projection unknown, ложный receipt/released не создаётся |
| Unstarted native pending/no-job | Cancellation остаётся nonterminal; legacy быстрый refund не применяется |
| Terminal native success при disabled execution | Own authorized read отдаёт исторический минимальный receipt, без исходного текста; generic paths не переоткрывают результат; revoke закрывает external read |
| Corrupted completion fingerprint | Controlled refusal, повреждённые bytes не исправляются |
| Composite native account orphan | `foreign_key_check` отвергает запись до inner JOIN; строка не исчезает из проверки молча |
| Notes edits35+, trimming до32 обычных receipts, trash/purge | Native proof неизменен после удаления обычного create receipt и очистки Notes; текущий текст недоступен, original ID не воскрешается |
| Missing registry, removed critical guard, unknown table, future3 | Отказ до WAL transition; DELETE-mode main/WAL proof неизменен; нет генерации новой identity или repair |
| Known Notes schema с несогласованным accounting | Migration отвергается, user rows остаются, v2 marker/registry/table не появляются |
| Настоящий main1 + committed v2 WAL отдельно Notes/Caps | Historical migrator видит v2 и отказывает; при удерживаемом writer main/WAL hashes неизменны, main header действительно1 |
| Некорректный migration boolean | Отказ до создания нового каталога/файла |
| Unicode у границы input/document budget | Valid future native row открывается, body не меняется и не попадает в generic dispatcher |

Предположение об orphan через JOIN не стало finding: composite FK и actual negative test закрывают его. Actor callbacks в этой группе — фиксированный trusted synthetic host, не настоящий Connect protocol. Concurrent revoke under Connect fence остаётся B2; эти fixtures этого не доказывают.

## Quick-check и отсутствие повторного полного прохода

Проверено чтением окончательного Notes source: exact layout/metadata recognition выполняется до persistent pragmas; WAL устанавливается до plain `PRAGMA quick_check`; под `BEGIN IMMEDIATE` recognizer повторяется, затем существующие rows проверяются одним вызовом `validateRows`. После DDL выполняется только layout/metadata inspection. Body читаются iterator по одному; повторного вызова full row validation на обычном reopen нет. Account aggregates/FTS lookup — отдельная проверка соответствия, не benchmark производительности.

Собственный executable case подтверждает historical DELETE → default reopen и explicit migration → успешный checkpoint без потери данных. Physical freelist-corruption и инструментированный one-pass regression принадлежат author suite; здесь они не выдаются за собственный тест. `quick_check` не удалён. Для **known layout с corrupt rows** WAL мог уже включиться до отказа; byte-unchanged обещание относится к тестируемому раннему отказу unknown/partial/future format и отдельным уже-WAL cases, а не ко всякой ошибке startup. Последний RW close также может checkpoint; соответствующие WAL proofs удерживают keeper connection.

## Команды и атрибуция

Собственный запуск Node24.21.0 / SQLite3.53.4 после первого исправления: **14/14 PASS,0 FAIL,0 SKIP**,1683.7615 ms. Затем добавлен15-й case с отдельным RED Unicode envelope. После второго исправления domain author выполнил serial focused **47/47 PASS**: его32 + независимые15; оба persisted RED стали GREEN. Этот последний запуск атрибутирован автору, не назван собственным повтором.

Root затем выполнил общий integration regression на окончательном source: **1064 tests,1059 PASS,0 FAIL,5 прежних opt-in SKIP**,101.964 s, включая независимые15. Финальная сводка проверена чтением `output/implementation-20260930/p4-b1a-root-regression.log`; это root run, не параллельный собственный прогон. Typecheck/build и commit принадлежат root и этой сводкой не подменяются.

```powershell
$nativeNode = Join-Path (Get-Location).Path 'var/toolchains/node-v24.21.0-win-x64'
$env:PATH = $nativeNode + [IO.Path]::PathSeparator + $env:PATH
& (Join-Path $nativeNode 'node.exe') --test --test-concurrency=1 modules/notes/test/native-storage.acceptance.test.mjs modules/capabilities/test/native-storage.acceptance.test.mjs
```

RED cases выбирались через `--test-name-pattern='terminal native receipt with a corrupted'` и `--test-name-pattern='native reader accepts a declared-size'` во втором файле. Tests работают только в новых случайных temp directories; cleanup проверяет canonical parent, уникальный prefix и собственный nonce marker. Ранее оставленные/запрещённые cleanup directories не затрагивались. Token values не печатаются.

## Проверенный окончательный source

SHA256 working bytes после repairs:

| Файл | SHA256 |
| --- | --- |
| `modules/notes/server/schema-v2.mjs` | `2a9f8b51f408179a6bb5333da63b2fd9b2452313b15597f01165aeba66be6fda` |
| `modules/notes/server/schema.mjs` | `ffcf54733ad113c6470e0e1bdbbb4500aa5b738f68879d8fd3fd12c4beaa3a79` |
| `modules/notes/server/index.mjs` | `970450fa7f65a30ff55cff16675b508233d4e684284db49b274c333a1bc7e5a6` |
| `modules/capabilities/server/schema-v2.mjs` | `5ca8b4025535566c90a1b1d610e37837b4b41e6074117a96fe720504050af25f` |
| `modules/capabilities/server/native-note-contract.mjs` | `1f3ab19fa9e6641e9928ce363ab6fdcbcf1421d4785256466a430ce0bcfa9c83` |
| `modules/capabilities/server/schema.mjs` | `43b20b072618aeade19cb7fae670d5c1b6b3df682486f4bf210b25fb2f29942b` |
| `modules/capabilities/server/index.mjs` | `0f36bcfd05cfe23d8f4e26f9836dd52511685c8799117ddc3395dcca5ca5b580` |
| `modules/capabilities/server/access.mjs` | `d3f4a76e63c378b19966aa753bf1645c74b9da2a47686722f630be23cfa2c9a6` |
| `modules/capabilities/server/invocations.mjs` | `b39566f0c419756035281256aca07de80f9e8f5dcb7e6ab88a6106ce8d77146b` |
| `modules/capabilities/server/native-baseline.mjs` | `68e877362f32905642772414d9b40448762684d066d59d8810f32d5ca72d25ad` |
| `modules/notes/test/native-storage.acceptance.test.mjs` | `bb0bbc6b796f0878710a2a37cafb8eb263a22e3504a2a44b83d8c0cee19a82eb` |
| `modules/capabilities/test/native-storage.acceptance.test.mjs` | `f1547163bdec27901566c521f142400b72cd00af1d4bf4f2a5ad63c01d670ac9` |
| `docs/implementation/p4-native-storage-contract.md` | `2ae6181faef1d38d3d4ddd7825c96398bd1a3d967e827abd21f4b21a1df0c273` |

Дополнительный read-only B2 host review подтверждает внесённые решения: свежая внешняя read ACL после actorless execution/reconcile, replay до disable/new-admission checks, общий Connect nested/close guard, неизменяемое исходное execution authorization. HTTP plan теперь задаёт raw2 MiB /15 s /8 global readers /2 peer readers /60 POST за60 s /2048 peer records. Это конечные проектные пределы до реализации, не проверенный HTTP результат и не distributed SLA.

## Последующая совместимость теста с B2 native port

30.09.2026. После появления принятого B2 API root разрешил узко заменить прежнее `native === undefined` в первом Notes case. Исторический B1 результат выше сохранён: его source/hash и evidence не переносятся автоматически на B2. Новый port существует и на default v1 reopen, и после explicit v2 migration; без `verifyNativeContext` он должен оставаться закрытым.

Независимый case теперь вызывает настоящий service: pure `validateDraftInput` принимает корректный input, а `storageIdentity`, `readCreateProof` и `createDraftForInvocation` отвечают `native_unavailable` в обеих версиях. До/после отказов совпадают Notes/accounts/FTS/обычные receipts/native proofs/metadata, `user_version`, `data_version` отдельного соединения и hashes main/WAL. Прежние assertions schemaVersion/registryId/default-off/replay/reopen/checkpoint сохранены. Production и author tests не изменялись.

Собственный ограниченный прогон **6/6 PASS,0 FAIL,0 SKIP**,871.6647ms, Node24.21.0/SQLite3.53.4:

```powershell
$independentNode = Join-Path (Get-Location).Path 'var/toolchains/node-v24.21.0-win-x64'
$env:PATH = $independentNode + [IO.Path]::PathSeparator + $env:PATH
& (Join-Path $independentNode 'node.exe') --test --test-concurrency=1 modules/notes/test/native-storage.acceptance.test.mjs
```

| Файл после узкой адаптации | SHA256 |
| --- | --- |
| Independent Notes test | `5b1257eb80d538041ed36c8f07fb3717a122b8a01bbc5075f45208959d205bc1` |
| `modules/notes/server/native.mjs`, read-only | `1dcff0bb9bdae66a855f971363204081e5f70b81253c6bd9abc3413f9cb2cb65` |
| `modules/notes/server/index.mjs`, read-only | `32befd49b596714293f01ae08a7b5a0015e15fc7b9f8eb9311e95d76b8a9dfc7` |

Этот case доказывает закрытый порт без verifier; настоящую opaque authority, effect/reconcile, crash ordering и HTTP composition должны проверять отдельные B2 gates. Полный114-test domain regression здесь не повторялся.
