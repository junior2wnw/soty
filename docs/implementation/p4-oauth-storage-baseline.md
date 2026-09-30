# P4-C1 — Domain3 baseline: author receipt

30.09.2026. Source freeze для независимой приёмки. Основание: [storage contract](p4-oauth-storage-contract.md), C1a `075ae4d99a54b22b358d74a1f9b34a9b9dd5e704`. Этот срез реализует только точный reader/migration3 и общую политику сохранённых OAuth-связей. AS issuance/consent ports, encryption, cleanup scheduler, bearer authentication, HTTP mounting и deployment reader labels здесь не реализованы и не объявлены готовыми.

## Публичная и внутренняя граница

`createCapabilitiesService({... , allowOAuthMigration = false})` принимает отдельный strict boolean. Default-off: fresh→1, existing1→1, existing2→2, existing3→3. Явный true разрешён только на уже существующем exact2/3; fresh/v1, включая одновременно два migration flags, получают `oauth_migration_requires_native_v2` до persistent pragmas/DDL. Неверный тип flag — `schema_configuration_invalid`. `supportedSchemaVersions` теперь `[1,2,3]`; `schemaVersion` и `registryId` сообщают действительный открытый store. `service.oauth` пока отсутствует, операции остаются прежними.

`schema.mjs` переэкспортирует `schema-v3.mjs`. Новый reader сначала распознаёт exact sqlite_schema/metadata без persistent pragmas, повторяет распознавание под `BEGIN IMMEDIATE`, затем проверяет FK, неизменные native invariants и OAuth plain rows. `schema-v2.mjs` отличается от прежнего source только экспортом существующей `validateNativeRows`; исторические SQL objects не менялись. Explicit2→3 добавляет пустые объекты, lineage и user_version одним COMMIT, registry/contract pins сохраняются. Malformed старый authorization JSON отклоняется до создания expression index. Future4/partial3/altered guards/project mismatch не исправляются автоматически.

Новые файлы: `oauth-schema.mjs` — literal SQL; `oauth-baseline.mjs` — plain row validator и always-on access policy; `schema-v3.mjs` — recognizer/migration. Добавлено **4 STRICT tables +10 explicit indexes +14 exact triggers**; полный перечень/SQL находится в `oauth-schema.mjs`, predicates — §4 контракта. В единственной согласованной поправке строки контракта уточнён fresh-client admission: ни credentials, ни Invocation, ни других grants/principals до привязки. `INSERT OR REPLACE` не обходит write-once connections/credentials при выключенных recursive triggers.

Общий access resolver определяет OAuth authority по client, principal и actual root из leaf grant. Он требует exact credential link, connection pins, fixed scope, current parent chain/creator и прежний абсолютный срок. Наличие AS config/key не влияет на эту проверку. Обычные `credentials.issue` и `grants.issue/derive` для linked authority отказывают `oauth_managed_authority`; legacy `soty_cap_` parser не выдаёт OAuth actor даже для специально помещённого в fixture legacy-shaped token. Отдельный ordinary service client работает по прежним правилам. Generic owner revoke OAuth credential отзывает всю connection family в той же Caps transaction; sibling не затрагивается. Ordinary principal/root revoke может оставить column state connection='active', но computed current authority закрывается; reader не считает это повреждением.

Reader проверяет cipher bytes/shape и plain relations, **не** AEAD authenticity или decrypted Provider payload. Retained original credential/link может законно пережить удалённый expired AT/Grant artifact. При наличии AT его pins совпадают с link; первоначальный link INSERT требует actual AT row. Для stored native redirects проверяется canonical HTTP loopback localhost/127.0.0.1/[::1] с явным портом, без query/fragment/userinfo. Exact registered callback path/profile — следующий closed port + host configuration; baseline не выдумывает allowlist из содержимого ciphertext.

Native coordinator получает exact admitted `schemaVersion` из service, допускает только Caps2/3 с соответствующей lineage и прежними project/registry/Notes2/digest. Runtime version drift до reopen отказывает; `>=2` нет. Native effect/receipt/Unicode/catalog@1 семантика не изменена. Root HTTP scheduler exact2/3 — отдельная интеграционная правка, этот receipt её не доказывает.

## Исполненная проверка

Изолированный Node24.21.0 / SQLite3.53.4; runtime directory добавлен в PATH для child `node`; все тесты последовательно `--test-concurrency=1`. Никаких production, Docker/Linux, browser или настоящих OAuth CLI вызовов в этом срезе.

Новые `oauth-storage.test.mjs` + `oauth-baseline.test.mjs`: **21/21 PASS, 0 skips, 5749.39 ms**. Log `output/implementation-20260930/p4-oauth-baseline-focused.log`.

Совместный затронутый набор: **85/85 PASS, 0 skips, 14373.72 ms**:

```text
node --test --test-concurrency=1 modules/capabilities/test/access.test.mjs modules/capabilities/test/invocations.test.mjs modules/capabilities/test/native-storage.test.mjs modules/capabilities/test/native-effect.test.mjs modules/capabilities/test/native-lifecycle.test.mjs modules/capabilities/test/oauth-storage.test.mjs modules/capabilities/test/oauth-baseline.test.mjs
```

Log `output/implementation-20260930/p4-oauth-baseline-regression.log`. `git diff --check` PASS. Full World/HTTP/Connect/build не повторялись; independent gate передан критику после освобождения test slot.

Значимые случаи:

- Настоящий copied historical2 migrator из `cc7f65b84e0a8f8b4fd41cf44e498ee3f21015e7` создаёт v2; миграция3 сохраняет все старые SQL objects, semantic pins и registry. Current3 никогда не переименовывался во v2. Frozen closure/provenance: `modules/capabilities/test/fixtures/capabilities-v2/`, без изменений.
- Main header остаётся2, committed WAL содержит3. Отдельный actual Node child запускает frozen old2 migrator, получает `schema_version_unsupported`, полностью выходит; main и WAL byte hashes неизменны, current reader снова открывает3. SHM — transient read-mark state, ему не приписывается immutable-byte обещание.
- Future4/partial layout/altered guard/wrong project отказываются в DELETE mode до смены journal mode. Malformed legacy authorization JSON оставляет все v2 objects/identity без half-migration.
- Прежний issued credential или второй grant/principal/Invocation блокируют connection admission. SQL immutable/replacement, single decision, consumed RT reset, original Session window, orphan/mismatched authority/cipher bounds проверены на actual SQLite rows, а не копии validator.
- Corruption fixture восстанавливает exact trigger SQL после изменения rows; reader отказывает без data repair. Original OAuth credential не заимствует срок нового AT, missing/mismatched link и посторонний leaf через OAuth root не авторизуются. Отзыв sibling не отнимает независимый grant.
- Real Connect + Notes2/Caps2 создают proof и теряют ответ после Notes COMMIT до Caps receipt. Caps upgrade3/reopen с execution off, подписанные trash/purge и credential revoke затем дают proof-first historical completion, один spend, без воскрешения Notes; внешний старый actor отказывает. Отдельно настоящее новое Notes-create на3 сохраняет @1 behavior. Эти два сценария используют **real Connect**, но не OAuth bearer/consent.
- `EXPLAIN QUERY PLAN` и actual lookup среди **12001 Invocation rows** подтверждают `SEARCH` по `cap_invocations_original_credential` и covering `cap_invocations_oauth_request`, без Invocation full scan. Second query содержит account/key + durable issuer/profile/resource/other-connection join; это проверка index utility, ещё не реализация crossconnection admission.

OAuth rows в `support/oauth-baseline.mjs` явно **synthetic future-format fixtures**: SQL parent/connection/link и bounded placeholder ciphertext, не crypto/provider/consent proof. Actual opaque token format, code/refresh rotation, key loss, cleanup batch quotas и connection-key conflict исполнение принадлежат следующему port checkpoint.

Первый 17-case прогон дал9 PASS/8 FAIL: новые test cleanup закрывали service после удаления Windows temp dir; тест использовал несуществующий `notes.delete`; corruption setup не снимал metadata guard; ещё ошибочно требовал неизменности SHM. Исправлены сами fixtures: close до удаления, настоящий signed notes.put→purge, восстановление exact guard после tamper, byte equality main/WAL. Product assertions не ослаблены. Затем17/17; после root redirect review и собственного actual-root lookup audit расширен до21/21. Процесс первого запуска и последующие дети завершены; этот ранний log не считается PASS.

## Frozen file hashes (SHA-256)

```text
ca2810310bab42a1d0232459163aa34eb5088de8d610aab411c17324e1002288  modules/capabilities/server/oauth-schema.mjs
2b7d90dd4764e6c34df9c3c724b0e576ebd483a42910eaf3baffb33b15f0a9c2  modules/capabilities/server/oauth-baseline.mjs
ebed724aed700443eadb84fd96476b14fea971c31f44b6779aa3b4b0b098e557  modules/capabilities/server/schema-v3.mjs
2622db5d571920ca070f45a0839bf0e3269f24f7f26744860c297b77be823fc8  modules/capabilities/server/schema-v2.mjs
e84001c4956e5a7836ab303f4eca63630febd12f3bd0ef0ca71b27c3c4969f80  modules/capabilities/server/schema.mjs
d154ea1900538099e8bfae40445eef2bb87b56d6a152c57472f33845ce24fb5f  modules/capabilities/server/index.mjs
7bec9706662e5835cfbf01a49775c0b9cf6676efecbaafe02e504eff062b6142  modules/capabilities/server/access.mjs
159d81d92f5df6394cb664a63780b092aecf03560e299d2cc21e5396316a10bd  modules/capabilities/server/native-notes.mjs
879daa1275f3f4420e6bdc7acc67bb113ad42a17c6ddd0e30ea83602627c63b0  modules/capabilities/test/oauth-storage.test.mjs
d0d9a1caee12f440189a989bba4900a27f5ab4bf25375ac6f9512361ef5203fb  modules/capabilities/test/oauth-baseline.test.mjs
a50b3c2d0babf2c705d0e1ca9c5c23d469750eabf06ac51239561bbccae96a79  modules/capabilities/test/support/oauth-baseline.mjs
7ed890b543c1b13939b11d423a71bb87c9aa52da4de4486e48b7228c01e774fe  modules/capabilities/README.md
a7af445d3681900bf270f1a0530aa26e1142c095bd98378bb6e4b3db7d4f6438  docs/implementation/p4-oauth-storage-contract.md
```

Все другие frozen plan/fixture files, Notes, pinned catalog/inputvalidator, deployment files и root HTTP не менялись этим автором. Никакого commit/push/deploy; independent findings ещё могут reopen этот freeze.
