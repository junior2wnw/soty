# P4-C1 — подплан независимого reader3

30.09.2026. **Подготовительный review-only план; reader3 не реализован и не объявлен image capability.** Основа — [frozen OAuth storage contract](p4-oauth-storage-contract.md), принятый C1a `075ae4d99a54b22b358d74a1f9b34a9b9dd5e704`, действующий [reader2](p4-reader2-implementation.md) и его [actual Linux result](p4-reader2-linux-result.md). Изменён только этот документ. Deploy code, manifests, Dockerfile, fixtures, Linux bundles и данные не менялись; тесты/remote actions не запускались.

Domain author подтвердил: `schema-v3.mjs`, `oauth-schema.mjs`, `oauth-baseline.mjs` ещё WIP; таблицы/индексы следуют контракту, тела 14 guards и startup validator не имеют final source freeze. До принятия их targeted gate нельзя копировать промежуточный SQL в host recognizer или писать `[1,2,3]` в image label.

## 1. Минимальная дельта host reader

Сохраняются exports и strict envelopes `soty.storage-format.v3`, `soty.storage-start.v3`, manifest `version:3`, exact keys четырёх stores. Новое declaration после source gate:

```json
{"version":3,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6],"notes":[1,2],"capabilities":[1,2,3]}}
```

Успешный scalar Capabilities — `'empty' | 1 | 2 | 3`; Notes остаётся `'empty' | 1 | 2`. Число0, строки `"3"`, массивы/coercion, unknown4 и старые envelopes не принимаются. Честный старый v3 label с capabilities `[1,2]` остаётся распознаваемым, но отказывает в START на actual3; новый host не повышает возможности старого image.

Для `/data/capabilities/capabilities.sqlite` добавить ровно version3 branch: `user_version=3`, lineage `soty.capabilities.sqlite.v3`, прежние `project_id=soty` и неизменный `registry_id`32lowerhex, exact три metadata keys. Filename/topology не меняются. Notes/FTS, Rooms1–2, Apps1–6 и historical Cap1/2 definitions остаются прежними.

Probe по-прежнему импортирует только Node built-ins, открывает нормальный read-only SQLite snapshot с WAL, не `immutable=1`, не исполняет candidate/domain imports, не создаёт/repair/checkpoint stores. Существующие filesystem/symlink/orphan checks, NoCopy/RO mount, writer admission, strict START receipt и fresh probe перед START сохраняются. `empty` означает отсутствие store, не пустой/сломанный существующий файл.

## 2. Exact schema/guard freeze

В принятом контракте v2 содержит 13 tables,12 explicit indexes,11 triggers. Delta3 — **4 STRICT tables +10 explicit indexes +14 triggers**: итог **17/22/25 =64 non-`sqlite_*` objects**. Autoindexes PK/UNIQUE не считаются explicit. До реализации сравнить эти числа и все SQL objects с принятой схемой, а не считать этот расчёт доказательством WIP source.

| Новый object | Что независимо закрепляет host |
|---|---|
| `cap_oauth_connections` | Полный CREATE TABLE: durable account/client/principal/root/creator/issuer/static-profile/resource/consent pins; one provider grant; active/revoked и конечные времена |
| `cap_oauth_interactions` | Полный CREATE TABLE: request/browser digests, limits, pending/approved/denied actor/connection tuple и 600s окно |
| `cap_oauth_artifacts` | Полный CREATE TABLE: шесть моделей, composite PK, cipher BLOB30..16412, digest/profile/key/plain identity pins, consumption и retention constraints |
| `cap_oauth_credentials` | Полный CREATE TABLE: immutable credential→connection/token digest/expiry link, PK/UNIQUE/FK |

Проверить fixed projections/column order, hidden columns, STRICT/type/table kind; полный exact SQL новых tables,10 indexes и14 guards, включая FK/CHECK/UNIQUE и partial/expression predicates. Сохранить прежние v1/v2 inventories и native guards; extra/missing table/view/index/trigger или другой `tbl_name` отказывает.

Индексы: `cap_oauth_connections_account`, `cap_oauth_interactions_expiry`, `cap_oauth_artifacts_retention`, `cap_oauth_artifacts_connection`, `cap_oauth_artifacts_grant`, `cap_oauth_artifacts_session`, `cap_oauth_credentials_expiry`, `cap_oauth_credentials_connection`, `cap_invocations_original_credential`, `cap_invocations_oauth_request`. Последние два находятся на прежнем Invocation ledger; JSON path/column order/WHERE в SQL нельзя заменять совпадением имени.

Guards: четыре `cap_oauth_connection_{admission,no_replace,no_delete,update_guard}`; два `cap_oauth_interaction_{no_replace,update_guard}`; два `cap_oauth_artifact_{no_replace,update_guard}`; шесть `cap_oauth_credential_{admission,no_replace,no_update,delete_guard,row_guard,row_no_replace}`. Последние два защищают parent `cap_credentials`, а не link table. Freeze receipt должен назвать exact SQL, target table, normalized SQL SHA, source Git commit/blob и bytes. Независимый literal3 fixture не импортирует текущий migrator.

**Граница row validation:** host probe — format/metadata/projection/exact-guard recognizer, не полный скан строк или сертификат расшифровки. Serving startup независимо проверяет exact schema, FK, native rows и plain OAuth rows. Из этого не следует право объявить ciphertext валидным без AS key: decrypt/digest/full payload checks выполняются на AS read/use. Не переносить весь domain validator в root helper и не обещать, что один `LIMIT 0` обнаруживает row corruption.

Domain/image acceptance обязана различать: испорченные links/account/creator/grant tuples; законно отозванные обычным API client/principal/root при connection state active; законный original credential/link, переживший удалённый expired AT artifact; revoked connection с закрытым root/credentials. Неправильная строка не repair-ится, но исторический законный отзыв/retention не делает базу «повреждённой».

## 3. Исторические fixtures и локальная приёмка

Повторно использовать без правок literal Cap2/Notes2 из принятого B1a и существующие provenance. Для **настоящего old domain reader2** использовать уже замороженную closure `modules/capabilities/test/fixtures/capabilities-v2/` из `cc7f65b84e0a8f8b4fd41cf44e498ee3f21015e7`: exact `schema.mjs`, `schema-v2.mjs`, `validation.mjs`, `native-note-contract.mjs`, рядом SHA/blob/bytes provenance. Old host reader2 закрепить exact Git source из `0c8db3dbdc4a428c68ff98be9597223abc4da699` с dependencies/known LF representation, не текущим файлом с выключенным branch3. Новый literal3 pin берётся только из принятого baseline3 commit; сейчас его нет.

Нужный bounded serial gate под изолированным Node24.21.0, после handoff test slot:

| Случай | Требуемый результат |
|---|---|
| Реальные historical2 + populated3 | Genuine2 создаётся старым migrator/literal; additive3 сохраняет native pending/terminal ledger, budgets, immutable Notes proofs после edit/tombstone и store IDs. OAuth connections/artifacts/links seeded явно как synthetic reader witnesses, не выданные реальные grants |
| Mixed topology | Notes `empty/1/2` × Caps `empty/1/2/3`: каждый store узнаётся независимо. Notes1/Caps3 — readable storage, но не разрешение native effect; Notes2/Caps3 — отдельный domain gate |
| Committed WAL | Checkpoint genuine main2; выполнить настоящую additive3 transaction с новыми objects/rows и оставить COMMIT в WAL. Проверить live writer и exited/crash writer; probe видит3, old2 отказывает. До/после RO success/refusal main/WAL hashes и SQL witnesses неизменны; SHM byte equality не обещается |
| Genuine old2 refusal | Exact frozen host2 и domain2 отказываются на actual3/main2+WAL3 без persistent mutation. Выполнить отдельно от сравнения текущего label. Не менять marker3 обратно на2 и не удалять OAuth tables для «отката» |
| Adversarial format | Каждый новый guard/index missing/altered; weakened CHECK/FK/STRICT/PK, hidden/extra column, extra SQL object, wrong project/registry/lineage, marker-only3, truncated/invalid SQLite и orphan WAL отказываются. Unknown4 отдельно на main и committed WAL |
| Fresh START | Ранее успешный receipt2 не допускает START после writer COMMIT3. Old `[1,2]` label отказывает; valid reader3 принимается только с actual3; malformed/missing labels и hostile DTO scalar shapes отказываются |
| Data boundary | Actual default-off domain3 reopen проверяет corrupted plain row fixture и lawful retention/revoke fixtures. Host format PASS для допустимого DDL не выдавать за полный row PASS; decrypted ciphertext integrity — отдельный AS gate |

Не переписывать historical tests «2» в «3» механически: настоящая unknown3 проверка становится known3 только для полного literal3; прежний marker-only3 остаётся отрицательным. Текущие guard upper-bound tests меняются узко на future4, Notes future3 остаётся future3. Сначала новые cases + affected guard tests, затем root integrated deploy gate; OAuth/Notes heavy regression — у владельцев domain/root.

## 4. AS-off application и rollback

`allowOAuthMigration=false` — default strict boolean, отдельно от `allowNativeMigration`. По принятому контракту fresh/v1/v2 default-off остаются1/1/2; actual3 читается без AS config/key. Только exact2 + explicit true разрешает2→3. Fresh/v1 + OAuth true отказывает `oauth_migration_requires_native_v2` до persistent pragmas, в том числе при двух true flags; нельзя скрыто выполнить цепочку1→2→3. Unsupported4/partial3 отклоняются до persistent изменения и повторно внутри transaction. Сам host probe flags не включает.

**Reader-before-writer здесь означает совместимый код приложения, не только SQL parser.** До первого production3 COMMIT actual SAME old fallback image должен уже читать3 с выключенными OAuth endpoints/issuance и без ключа. Baseline3 сохраняет current/original OAuth authority в общем access resolver: linked credential нельзя использовать как legacy service credential; missing/mismatched link отказывает; generic issue/derive не расширяет linked root. Новая refresh credential не заменяет original credential старого Invocation. Source/expiry/revoke checks действуют и при AS off.

Полный application gate дополнительно проверяет ordinary Notes/Capabilities и original receipts/budgets, proof-positive native reconciliation без дешифровки AS, safe-hold unknown effects, explicit Caps2/3 scheduler admission и запрет unknown4. Отсутствие AS key не разрешает внешнюю выдачу token, но не должно терять уже случившийся Note/receipt. Нельзя рекламировать reader3 до завершения этих обязательных domain/host integration границ.

После3 rollback к старому reader2 запрещён даже при выключенном OAuth. Откат к проверенному AS-off reader3 сохраняет **последние** файлы3 и их authority; downgrade marker/DDL — не rollback. Full code rollback/health на таком baseline, actual label и cold restart — root image gate.

Первый реальный unlabelled serving image по-прежнему не получает новый label от canary. Strict old-image-before-STOP и fresh guard не обходятся. Применяется отдельный ещё согласуемый bootstrap/recovery путь из [reader2 plan §5–6](p4-reader2-rollout-plan.md): stop-only outgoing container, проверенный compatible recovery, maintenance либо fail-stop; никакого автоматического START старого unlabelled image.

Перед первой migration3 нужен coordinated encrypted cold backup одной generation всех authority/effect stores и нужной config/key provenance, затем реальный isolated restore/start. Notes2/Caps3 не являются общей cross-file transaction. Разновременные копии с теми же registry IDs не доказывают согласованность. Возврат общей старой generation после новых writes — отдельное решение о потере данных, не автоматический fallback. Ключи/токены/ciphertext в отчёт не выводятся; отсутствие ключа для AS artifacts — явная operational граница restore.

## 5. Переиспользуемые Linux доказательства и остающийся delta gate

[Linux2 review1](p4-reader2-linux-result.md), однократно исполненный root, уже подтвердил Node24.15.0/SQLite3.51.3/FTS, foreign UID10001/modes0700/0600, RO committed WAL, два file symlinks, single-attempt journal/cleanup, bounded transport и serving invariance. Его bundle/receipt не меняются и не повторяются. Он **не** доказал новый SQL/row layout3, application START3 или fallback3.

После local3/source/image gates потребуется отдельный synthetic Linux3 delta plan: genuine main2/WAL3, native+OAuth plain witnesses, exact old2 refusal, altered new guard/future4 и RO byte preservation. Нового executable bundle/remote разрешения этот документ не создаёт. Перед его review заново измерить actual emitted bytes: Docker inspect содержит **две** сериализованные копии command; сохранять exact GET-inspect128KiB и остальные64KiB, reserve не менее16KiB, проверку overflow. Не расширять лимиты молча из-за 28 новых DDL objects. Старый runtime/cleanup опыт уменьшает дублирование сценариев, но не заменяет проверку нового artifact.

## 6. Предлагаемое владение после отдельного разрешения

- Reader author: `deploy/connector/storage-probe.mjs`, `storage-guard.mjs`, узкий current-shape diff `storage-guard.test.mjs`, новый `storage-oauth-v3.test.mjs`, новый `capabilities-v3.fixture.mjs` + `fixtures/capabilities-v3/provenance.json`, `deploy/connector/README.md`, новый implementation receipt. Historical1/2 fixtures неизменны; current2 test адаптируется только если конкретное новое допустимое3 меняет его прежнюю upper-bound assertion.
- Domain author: final schema3 literals/guards/full row validation, default-off/native/original-authorization invariants и historical2 closure. Его acceptance не подменяется host fixtures.
- Root: разрешение этапов, actual Dockerfile/image label, full application/scheduler composition, rollout/controller/bootstrap/backup/restore и remote. Independent reviewer: отдельный acceptance/read-only аудит, без общих author fixtures.

Порядок: **принятый baseline3 literal/authority freeze → разрешение reader edits → bounded local + independent gate → root integration/image gate → отдельно reviewed Linux3/restore → отдельно migration admission**. Сейчас выполнено только чтение и этот подплан.
