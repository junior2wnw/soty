# P4-C1 — локальный host reader3

30.09.2026. Авторский source/test freeze для независимого review. Реализован принятый [подплан](p4-reader3-rollout-plan.md) в deploy-only зоне. **Host parser знает Capabilities3; `currentStorageReaders` и Dockerfile по решению root остаются `[1,2]`.** Это промежуточная совместимость helper, не приёмка image3, Linux3, restore или production migration. Remote/Docker/SSH actions, новые canary bundles, labels, serving data и domain implementation этим этапом не менялись.

## Реализованный контракт

`storage-probe.mjs` сохраняет built-ins-only imports, normal read-only SQLite/WAL snapshot, filesystem/topology checks и прежние Rooms1–2, Apps1–6, Notes1–2, Capabilities1–2 branches. Для точного Capabilities3 добавлены четыре полные STRICT table definitions/projections, десять exact indexes и четырнадцать guards с проверкой target table. Проверяются 64 non-`sqlite_*` SQL-объекта: 17 tables,22 explicit indexes,25 triggers. `cap_oauth_credential_row_guard` и `row_no_replace` относятся к прежней `cap_credentials`, expression/partial indexes не узнаются лишь по имени. Прежние native objects обязательны и в3.

`user_version=3` требует lineage `soty.capabilities.sqlite.v3`, project `soty`, неизменный32lowerhex registry ID и ровно три metadata keys. Schema marker сам по себе не делает2 форматом3. Capabilities4, Notes3, Apps7 и Rooms3 остаются неизвестными. Envelope/key sets `soty.storage-format.v3` и `soty.storage-start.v3` прежние; новый допустимый scalar только `capabilities:3`, не строка/массив/coercion. В `storage-guard.mjs` расширен supported parser, **не** current image declaration.

Fixture image с явным reader3 получает квитанцию только после свежего probe. Реальный image `[1,2]` отказывает на actual3 даже при более новом copied container label и ранее успешной START2 receipt. Ни один такой отказ не вызывает application START. Это локальная проверка реального guard с synthetic engine port, не Docker START доказательство.

Host recognizer не сканирует все domain rows и не дешифрует AS payload. Отдельный test намеренно оставляет полную правильную DDL и повреждает plain creator tuple: host format PASS, exact committed default-off domain3 — `capabilities_storage_corrupt`. AS ciphertext в fixtures — явно структурные synthetic30-byte placeholders, не валидные tokens/consent/crypto. Полный AS read/use и original-authorization gates этим тестом не заменяются.

## Исторические данные и pins

Новый [literal3 fixture](../../deploy/connector/capabilities-v3.fixture.mjs) импортирует только неизменный literal2; accepted migration/validator не импортируется в host probe. [Provenance](../../deploy/connector/fixtures/capabilities-v3/provenance.json) содержит blob IDs, SHA256/bytes семи файлов committed baseline3 closure, 28 новых normalized SQL fingerprints и old host2 pin. Tests материализуют эти **точные Git bytes** во временный каталог и запускают настоящие module exports. Текущий WIP domain code не участвует в историческом сравнении.

| Источник | Закрепление |
|---|---|
| Actual baseline3 migrator/validator | `dc1ae217424b33cca0e9a5b60a6e4e719ea0d991` |
| Literal `OAUTH_DDL + OAUTH_GUARDS` | 15,157B; SHA256 `05a3a9b2025bbe351e0ec75e28c67d4b497ba35b6de4932a1a5e439232d0978f` |
| Committed `oauth-schema.mjs` | 15,301B; SHA256 `ca2810310bab42a1d0232459163aa34eb5088de8d610aab411c17324e1002288` |
| Committed `schema-v3.mjs` | 4,850B; SHA256 `ebed724aed700443eadb84fd96476b14fea971c31f44b6779aa3b4b0b098e557` |
| Exact old host2 | `0c8db3dbdc4a428c68ff98be9597223abc4da699`, storage-probe.mjs 35,947B; SHA256 `6dea52e8076bce0ecdb3a1652b14920d256ec539c4a2d26f95129460ec39d6ef` |
| Exact old domain2 | `cc7f65b84e0a8f8b4fd41cf44e498ee3f21015e7`; unchanged `modules/capabilities/test/fixtures/capabilities-v2/provenance.json` |

Historical1/2 literals, source closure and provenance не изменены. Единственная правка старого native-v2 test — название marker-only3 случая: он по-прежнему отрицательный, но полная Capabilities3 теперь известна host. В guard tests future Capabilities value стал4; Notes future остаётся3. Новые tests сравнили все64 SQL-объекта literal3 с genuine old2→committed baseline3 migration, а не только marker/schema count.

Synthetic populated2→3 сохраняет uncertain/terminal native ledger, budget reserved/spent, immutable receipt, registry ID и Notes proof, переживший edit/tombstone. Затем seeded шесть OAuth artifact моделей, approved interaction и credential link проверены default-off reopen без AS key. Законный parent revoke при ещё active connection, удалённый AccessToken artifact с оставшимся original credential/link и явный connection revoke не превращаются в storage corruption. Это storage witnesses, не утверждение о реально исполненном native effect или внешнем OAuth flow.

## Cold restart: найденная граница и исправленное доказательство

Первый run58/58 включал cold old domain через read-only handle. Self-review усилил этот case до обычного writable `DatabaseSync`. Получен настоящий **RED**: frozen old2 возвращает `schema_version_unsupported`, но закрытие последнего RW handle после crashed writer checkpoint-ит committed main2/WAL3 в main3 и убирает/обнуляет WAL. Отказ до persistent PRAGMA и byte preservation — разные свойства. Frozen old code не менялся, RED log сохранён.

Финальные tests разделены:

1. Genuine populated main2/WAL3 после завершения реального child writer: host2 refusal, host3 success и old domain2 на RO handle сохраняют main/WAL hashes и список файлов. SHM bytes не сравниваются.
2. Тот же cold сценарий с обычным old RW handle: ошибка приходит до persistent changes, что проверено **перед close**. После last RW close наблюдается checkpoint; main marker остаётся3, SQL layout, native ledger/receipt/budget и OAuth row побитово в значениях сохранены, downgrade не происходит. Повторный host RO сравнивается со своим непосредственным baseline.

Во время усиления второго fixture обнаружена ещё граница измерения: отдельный RO observer на writable Windows temp directory после удаления sidecars может создать пустой WAL. Первый final run ошибочно сравнивал этот observer вместе с последующим host call; correction явно учитывает absent/empty WAL и снимает baseline непосредственно перед host probe. Strict main2/WAL3 RO assertions не ослаблялись. Это не доказательство файловой read-only mount: настоящий helper также требует Docker RO mount; Linux3 остаётся отдельным gate.

Следствие для выпуска: старое приложение нельзя сначала открыть «чтобы оно само отказало». Host guard обязан отказать actual reader2 **до application START/open**. Cold error code не является сертификатом physical byte equality. Упомянутые checkpoint/sidecar наблюдения относятся к этому local Node24.21.0/SQLite3.53.4 fixture, не обещаются как универсальный lifecycle всех SQLite versions.

## Локальная проверка

Все runs последовательные, после согласования test slot, на изолированном Windows Node24.21.0 / SQLite3.53.4. Heavy world/domain suites не запускались.

| Срез | Результат |
|---|---|
| storage-guard + storage-native-v2 + первоначальные12 reader3 | **58/58 PASS**,0skip,25,082.0497ms |
| Усиленный cold RW case до разделения claims | **1 RED**,671.021ms; physical checkpoint после logical refusal |
| Первый final13, boundary измерения observer/host | 12PASS/1fixture FAIL,0skip,12,674.3496ms |
| Окончательный storage-oauth-v3 | **13/13 PASS**,0skip,13,592.1514ms |

Таким образом покрыт финальный набор59 различных cases:46 существующих на неизменном production slice и13 новых в последнем run. Единого final59 запуска здесь не заявлено. Новые13 включают все12 independent empty/1/2 × empty/1/2/3 pairs; genuine live и crashed main2/WAL3; обе старые reader2 реализации; default-off3; row/format boundary; каждый из14 guards и10 indexes missing/behavior/target; weakened STRICT/FK/PK/CHECK/hidden/extra columns; metadata; marker-only3/unknown4 main и WAL; fresh actual-image START admission; orphan/truncated/extra-file refusal.

Logs сохранены в ignored `output/implementation-20260930/`:

| Log | SHA256 |
|---|---|
| `p4-reader3-author.log` | `fc2bb76721a99856ab3b16ba0866e642ecd6acc5aa4e6f8468bb6b1e1ab27075` |
| `p4-reader3-cold-writable.log` | `682f50b626418624142faf67e5efa69b33df823e78949d19550e3323a06443bc` |
| `p4-reader3-oauth-final.log` | `48419b10ec10e435ed1f4dbbff7b5f6224c0cb096e1c2dd04e6497c4b606b1a2` |
| `p4-reader3-oauth-final2.log` | `847eed800f6bb95079b9dfdb5a793aa74b120849a169bc592c7b85caee45b133` |

Повторяемая команда final file:

```powershell
& .\var\toolchains\node-v24.21.0-win-x64\node.exe --test --test-concurrency=1 deploy/connector/storage-oauth-v3.test.mjs
```

## Source freeze

Ниже actual worktree bytes/SHA256 на этом freeze. Это **не committed Git byte hashes**: существующие Windows файлы частично CRLF. Исторические pins выше получены `git show` bytes; их EOL не преобразовывается. Для будущего bundle надо брать новый принятый commit, а не менять EOL во время runtime.

| Owned file | Bytes | Worktree SHA256 |
|---|---:|---|
| `deploy/connector/storage-probe.mjs` | 55,197 | `6c69ddfe21cb18e61abef743be93a79b90afd05b54f3467693652cdf5125501f` |
| `deploy/connector/storage-guard.mjs` | 11,212 | `faa313877080b2fe23a26b6c1651004f12b3abb9e92b0d8414fb577d32f12d8a` |
| `deploy/connector/storage-guard.test.mjs` | 25,553 | `2319e88b1aae23891e3c3a48a89839fa818e861b0a07793cedc8742087cb47b4` |
| `deploy/connector/storage-native-v2.test.mjs` | 26,831 | `b3475df3c8bafa952ba2330094b56481f50533fbfaca913e1b49fcc5fcd7e425` |
| `deploy/connector/storage-oauth-v3.test.mjs` | 38,258 | `a4a0625268f3d070262eca052e98d130da24e9facfbac8cf8ab5c7a9a6cd257a` |
| `deploy/connector/capabilities-v3.fixture.mjs` | 16,476 | `8d3794a5c62801ca181709a91c438009ed9807812b48ef2ae600ad443ee004e4` |
| `deploy/connector/fixtures/capabilities-v3/provenance.json` | 8,390 | `365e167885e674b73e45f42e103b3084bd796e15890cb1d9415a4c7cabe6c22b` |
| `deploy/connector/README.md` | 20,690 | `7415b1dafd0f88967211f262ef4fe4ef3b03b6c452f1b282ebf18c72f9475e7b` |

Для диагностики EOL, ещё не как committed identity: LF-normalized probe54,873B/SHA `6e634e24f0faa67326ea45178c911ba666d90b205ebfd11756ed5a5a9be64050`; guard11,042B/SHA `04cc9468cb3f6b62ffb01b33ecad02390441ead9e2f3e838f8255b8d91e7a1d7`. Git diff/whitespace и JS syntax checks выполнены; final hash самого receipt сообщается отдельно, чтобы не создавать самоссылку.

## Незакрытые release gates

Независимая приёмка и root integration следуют после этого author freeze. Full application image3/default-off/original-authority, exact labels, actual cold start/fallback, согласованный encrypted backup/restore, первый unlabelled serving bootstrap и production migration остаются у root. Notes2/Capabilities3 не являются cross-file atomic snapshot. Новых OAuth endpoint/issuance/config/key mutations в этой работе нет.

Linux2 PASS не переиспользуется как Linux3 proof и его artifacts не менялись. Независимый review уточнил **обе** границы прежнего `measureConfig`: encoded `Entrypoint+Cmd` не более49,152B и консервативный duplicated/Go-escaped inspect плюс16KiB не более131,072B. Текущий CRLF worktree даёт encoded command56,916B; direct inspect с завершающим newline115,006B, а используемый `max(direct,2*goCommand+64)`115,066B. Вместе с reserve это131,450B, на378B выше inspect cap. LF diagnostic даёт encoded command56,268B и inspect budget с reserve130,154B: **LF укладывает inspect, но всё ещё нарушает48KiB command cap**. Поэтому одного перехода к Git LF недостаточно; прежняя оценка только двух прямых полей была неполной. Нужен отдельный review компактного представления/передачи из точных pinned Git bytes без удаления checks и без молчаливого повышения caps. Source freeze неизменён; ни bundle, ни remote run здесь не подготовлен и не разрешён.
