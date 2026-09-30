# P4-C1c — encrypted domain ports

Актуальная граница — increment 2 ниже: signed owner decisions и atomic Grant binding, с `readiness.available=false`. Increment 1 сохранён как историческая квитанция; его итоговые SHA и утверждение о ещё не подключённом service относятся к прежнему срезу.

Дата: 2026-09-30. База: принятой domain3 `dc1ae217424b33cca0e9a5b60a6e4e719ea0d991`.

Этот срез реализует **только вспомогательные Session/Interaction artifacts**. Он не подключён к `createCapabilitiesService`, не объявляет `service.oauth.available`, не выдаёт Grant/code/RT/AT и не создаёт actor/consent/native effect. Такой промежуточный срез согласован root до реализации. Следующий этап начинается после независимой проверки этого кода.

## Изменённая область

- Новый `modules/capabilities/server/oauth-profile.mjs`: закрытая конфигурация, снимок own data без getters, bounded canonical JSON, exact unbound payload profile.
- Новый `oauth-crypto.mjs`: AES-256-GCM codec по уже согласованным AAD и wire bytes.
- Новый `oauth-artifacts.mjs`: синхронный SQLite store Session/Interaction, повторное чтение/запись, срок, квота, ограниченная очистка.
- Единственная правка существующего source — `oauth-baseline.mjs/nativeRedirect`: literal decimal port проверяется до URL normalization. Явный `:80` больше не теряется из-за `URL.port === ''`. Root отдельно одобрил эту поправку; DDL не менялся.
- Новые `oauth-profile.test.mjs`, `oauth-artifacts.test.mjs`, `test/support/oauth-artifacts.mjs`.

Schema-v3, OAuth DDL/guards, access/index/native coordinator, Notes, pinned catalog, server/provider/HTTP и reader fixtures этим срезом не менялись.

## Точные внутренние exports

```js
normalizeOAuthConfiguration(value) // undefined -> null; иначе captured internal config
createOAuthUnboundProfile(configuration).snapshot({
  model, id, payload, nowMs, expiresIn?, allowExpired?
}) // {payload, json, createdAt, expiresAt, retainUntil}

createOAuthArtifactCodec({registryId, issuer, artifactKey?, artifactKeyId?})
// {available, seal, open, close}; key отсутствует -> decrypt/encrypt unavailable

createOAuthArtifactStore({
  db, projectId, registryId, schemaVersion, clock,
  transaction, ensureOpen, configuration
})
// configuration — результат normalizeOAuthConfiguration, не внешний DTO
// transaction — штатная sync Caps transaction(fn, {busyMs}), не AS Promise
```

Последний factory возвращает frozen handle:

```text
hasKey() -> boolean                    // не readiness всего OAuth/AS
upsert({model,id,payload,expiresIn?,request?,stagedGrant?}) -> void
find({model,id}) -> checked cloned payload | undefined
findByUid({uid}) -> checked cloned Session | undefined
destroy({model,id}) -> void
cleanup({limit?:1..64}) -> {artifactsDeleted,interactionsDeleted:0,credentialsDeleted:0}
close() -> void
```

Сейчас `model` — только Session/Interaction; известные bound models отказываются `oauth_unavailable`. `consume` и `revokeByGrantId` тоже явно закрыты. `request/stagedGrant` в этом срезе принимаются лишь absent/undefined. В последующем полном adapter их допустимость будет привязана к нужным моделям, а не расширена для произвольных payload.

`Client.find`/неподдерживаемый `findByUserCode` остаются обязанностью host wrapper по принятому контракту. Этот store не заменяет static client configuration.

Конфигурация имеет ровно issuer/resources/withAuthorityFence/isRegisteredRedirect и optional key+keyId. Она проверяет собственные data properties, две фиксированные audiences и синхронные callbacks; key копируется. Codec дополнительно владеет своей копией key и обнуляет её при close. Исходные bytes у вызывающего host не изменяются. Keys/payload не попадают в status/error/log. `withAuthorityFence` здесь ещё не используется: вспомогательная AS metadata сама не даёт прав.

Ошибки — существующий `AccessError`: `oauth_configuration_invalid`, `oauth_invalid_artifact`, `oauth_context_invalid`, `oauth_unavailable`, `oauth_storage_key_unavailable`, `oauth_quota_exceeded`, `oauth_storage_busy`, `capabilities_storage_corrupt`; lifecycle/clock errors сохраняют `service_closed`, `nested_transaction`, `clock_invalid`. SQL text и исходные исключения crypto в ответ не проецируются.

## Проверяемые ограничения

Payload копируется без вызова property getters. До завершения clone действуют budget≤1024 nodes, depth≤12 и canonical≤16384 UTF-8 bytes. Optional undefined известных полей исчезает при serialization; unknown undefined не скрывает неизвестный key. Strings well-formed, numeric поля не coerced. Штатный **первый Interaction не обязан иметь result**: exact installed Provider `authorization/interactions.js` сохраняет его до completion без этого поля. Если result/lastSubmission присутствует, принимается только согласованная login/consent/error форма.

Production C1 resume URL закреплён root как `${issuer}/authorize/${id}`. Старый synthetic `/auth/:uid` не копируется в production validator. Redirect допускает только raw explicit decimal port1..65535 без leading zero на loopback HTTP, без userinfo/query/fragment; canonical pathname проверяется отдельно. Exact static profile/IPv4/path allowlist дополнительно требует literal `true` от captured registered callback. Общая structural часть AS-off reader по-прежнему может распознавать localhost/[::1]; это не разрешает такие callback в root static configuration.

Unbound time predicates:

```text
iat, exp — safe whole seconds
createdAt = iat * 1000
createdAt <= nowMs < expiresAt = exp * 1000
0 < exp - iat <= 600
retainUntil = min(createdAt + 600000, MAX_SAFE_INTEGER)
```

Тот же ID сохраняет исходные createdAt/Session.uid/retainUntil, истёкшая row не оживает через upsert. Новый Session ID после штатного resetIdentifier тоже получает createdAt из **сохранённого payload.iat**, а не из receive-time. Хранилище не может восстановить забытый origin iat после удаления старого ID; его сохранение проверено на actual Provider модели и отдельно на root/critic HTTP lifecycle. Host обязан вычислять remaining TTL до save. Store отклоняет sliding expiry и не исправляет ciphertext/payload молча. При find просроченный artifact не возвращается.

Для будущего bound этапа root уже согласовал отдельную узкую validator поправку: Grant.expiresAt≤connection.expiresAt вместо equality, так как Provider seconds округляются вниз. Bound row.createdAt должен быть фактическим admission nowMs, а не iat*1000; iat может быть раньше createdAt на остаток секунды. **Эта bound поправка здесь ещё не реализована и не проверена**: её non-aligned positive/reopen и beyond-bound negative относятся к следующему этапу. DDL/consent duration не меняются.

AES-GCM использует fresh random nonce12 и tag16; AAD закрепляет registry/issuer/model/idHash/profile/keyId. Find проверяет tag, SHA-256 canonical plaintext, exact profile и plain projected pins, затем возвращает независимый clone. Неверный key ID, другой key или повреждение не запускают reset/delete. Raw Session/Interaction IDs, challenge/state живут только в encrypted payload; lookup использует SHA-256. Session authorizations.grantId сверяется с той же account/static profile/issuer в durable connection. Просроченная/отозванная remembered metadata не считается текущим допуском. Borrowed Interaction.grantId не попадает в family index; destroy такого Interaction не отзывает Grant.

Каждая операция читает actual3/project/registry/lineage внутри короткой Caps transaction. Чужой/изменившийся store не принимается. Busy deadline запрашивается100ms; нет await под lock, внутренней очереди и доступа к OAuth authority. Сохранённый transaction callback после возврата и повторное его выполнение отвергаются. Rejected Promise от transaction/redirect callbacks поглощается после controlled отказа. Это не общее обещание для всех callbacks: clock имеет отдельную trusted synchronous Date.now precondition.

Новые auxiliary rows ограничены1024/global; также проверяется общий65536 artifact ceiling. Exact upsert существующей row не требует нового slot. При заполнении нет eviction живых rows. Отдельный explicit cleanup выбирает≤64 identities через retention index и сейчас удаляет только истёкшие auxiliary artifacts; expiry authority не продлевается, даже если housekeeping ещё не выполнен. Будущий proposal/credential cleanup должен **делить** этот общий64 budget, а не добавлять свои64. Этот срез не заявляет полную AS quota/retention реализацию.

## Выполненная авторская проверка

Изолированный Node24.21.0/SQLite3.53.4, process-local PATH, последовательно:

```powershell
node --test --test-concurrency=1 modules/capabilities/test/oauth-profile.test.mjs modules/capabilities/test/oauth-artifacts.test.mjs
node --test --test-concurrency=1 modules/capabilities/test/oauth-storage.test.mjs modules/capabilities/test/oauth-baseline.test.mjs
```

- **17/17 PASS, 0 skips**,1263.4986ms; `output/implementation-20260930/p4-oauth-artifacts-first.log`.
- **21/21 PASS, 0 skips**,4287.1544ms; `p4-oauth-artifacts-baseline.log`.
- Syntax checks новых source/test файлов и `git diff --check` прошли.

Новые cases включают реальные SQLite writes/reopen, одно удержание второго writer handle,1024 auxiliary rows без eviction,64 cleanup и actual EXPLAIN, key absence/mismatch/tag/AAD/plain-pin tampering без удаления, caller snapshot mutation, no getter invocation, unsupported profile fields, точную byte границу, raw:80 и отрицательные redirect reopen. One-model test использует настоящий установленный Provider9.12.2 Session/Interaction save/find/resetIdentifier и encrypted store. Он **не** выполняет HTTP authorization/Connect consent/token flow. Synthetic connection в одном metadata reference test явно создана SQL baseline helper; это не issuance proof. Два OS процесса/полный AS/CLI/Linux не проверялись.

21 прежних baseline cases подтверждают, что narrow redirect correction не изменила default-off, genuine2 migration, old2 refusal с WAL, native2/3 proof/current authority и immutable storage guards. Полный Caps/server/world suite здесь не повторялся. Serial test slot возвращён root после завершения.

## Freeze inventory

SHA-256 локальных bytes:

| File | SHA-256 |
| --- | --- |
| server/oauth-profile.mjs | e44bf5acd13176b103dd32246af9f9f354258eeaacb202005bd37b4eb97f10b5 |
| server/oauth-crypto.mjs | cb9712d5308cff9309fd0e0446a421002f57e9423ca1c302267869bd4c2417d1 |
| server/oauth-artifacts.mjs | a36a1814bb528561322fc6acc0fe9f3ed979f99765a875252cafc56a5d31faf0 |
| server/oauth-baseline.mjs | 0cbb127273ab43b437fa7eb540124b5de5d7f5a1d3ed8e6580c35398179263f2 |
| test/oauth-profile.test.mjs | c5a30379aea126b19a2e6266600e9c0db7d9fb3d66399c1294f892298e6e792b |
| test/oauth-artifacts.test.mjs | 046c66a4c52444cc07c216063c463a339742b7e18d676031b699e96cf3409da9 |
| test/support/oauth-artifacts.mjs | c090693327c29574cc525c47d59d58ba63dd10336d08cef82355a0e5f8cea7d1 |

Paths в таблице относительно `modules/capabilities/`. Неизменённые guards/schema: oauth-schema SHA `ca2810310bab42a1d0232459163aa34eb5088de8d610aab411c17324e1002288`; schema-v3 SHA `ebed724aed700443eadb84fd96476b14fea971c31f44b6779aa3b4b0b098e557`.

Независимый review и root actual-HTTP composition этого increment ещё не завершены. Следующие owner decision/staging/token/bearer/dedup ports остаются отдельным этапом, а не подразумеваемой готовностью этого store.

## Increment 2 — signed owner decision и atomic Grant binding

База — принятый auxiliary checkpoint `a68cb5f7254f0fdc8aa3112e06f89e91b6e9633e`. Его независимый gate впоследствии прошёл **3/3**: два реальных OS writers для одного UID и последнего auxiliary slot, persisted authenticated ciphertext/digest transplant. Root также проверил actual HTTP Provider → encrypted Interaction, **2/2**. Эти результаты принадлежат коллегам и не выдаются за мой повтор. Reader3 checkpoint `8803fcd` не меняется.

Этот increment создаёт действующую signed owner authority и её provider Grant binding. **Code/RT/AT issuance, consume, bearer actor и OAuth request-key dedup ещё закрыты.** `readiness().available` всегда false; root не должен объявлять завершённый AS по наличию новых методов. Native Notes, catalog@1, схемы/DDL, host/provider/HTTP/UI и deploy readers не изменены.

### Source и composition

- Новый `server/oauth-connections.mjs` — coordinator, safe presentation, signed commands, bounded private staging и Grant adapter.
- `index.mjs` — optional trusted `oauth` composition, экспорт четырёх `OAUTH_OPERATIONS`, закрытие owned keys/handles. Отсутствующая composition сохраняет прежнюю форму service без `oauth`.
- `access.mjs` — private captured `create/live/revoke` seam; approval переиспользует прежние inserts principal/root/budget, common chain проверяет creator и current authority. Public arbitrary authority factory не добавлен.
- `oauth-artifacts.mjs` — закрытый captured transaction core для совместного чтения Interaction/decision и записи signed result в той же Caps transaction, без nested public transaction.
- `oauth-profile.mjs` — exact fixed Grant payload validation.
- `oauth-baseline.mjs` — согласованное Grant expiry≤connection вместо equality и переиспользуемый internal family revoke. Plain-row validation/DDL остаются прежними.
- Новые `test/oauth-connections.test.mjs`, `test/support/oauth-connections.mjs`; README уточняет незавершённость token layer.

Конфигурация остаётся прежней закрытой формой `oauth: {issuer,resources:{http,mcp},withAuthorityFence,isRegisteredRedirect,artifactKey?,artifactKeyId?}`. Constructor не выполняет неявную миграцию. Actual v1/v2 дают `oauth_unavailable` при попытке новых ports. Keyless actual3 сохраняет list/revoke и exact signed decision replay, но не decrypt/encrypt. Creator/account берутся только из уже проверенного signed Connect actor. Запрос не выбирает internal client/principal/grant.

### Точный port checkpoint

```text
service.oauth.readiness() -> {schemaVersion,available:false}
prepareInteraction({interactionId,browserNonce,durationMs=86400000,budgetLimit=20})
readInteraction({interactionId,browserNonce})
  -> {interactionId,contextDigest,clientProfile,resource,scope,durationMs,budgetLimit,
      expiresAt,checkedAt,decision,decidedAccountId}
beginGrantBinding({interactionId,browserNonce})
  -> {context,providerGrantId:null|string,
      connection:{id,accountId,staticClientId,issuer,resource,scope,expiresAt}}
endGrantBinding(context) -> void
cleanup({limit=64}) -> {artifactsDeleted,interactionsDeleted,credentialsDeleted:0}
artifactStore.{upsert,find,findByUid,destroy,revokeByGrantId}
```

Public operations export: `oauth.connections.approve`, `oauth.connections.deny`, `oauth.connections.list`, `oauth.connections.revoke`. Аргументы/ответы точно соответствуют storage contract §6. Owner list≤50 использует account-scoped `(created_at,id)` cursor; projection не содержит provider IDs, digest, ciphertext, nonce, raw state или Notes contents. `active` вычисляется по current common chain, поэтому revocation creator сразу отражается даже при ещё stored active connection. Empty/missing key не мешает safety revoke.

Presentation содержит `checkedAt` из clock данного чтения. `decidedAccountId` null до решения и равен signed account после approved/denied. Это browser-private uid+nonce-bound projection, а не credential. Root completion проверяет expectedAccountId; domain staging всё равно заново проверяет durable decision/current creator. В digest/DDL новые presentation fields не включены.

`artifactStore.upsert({model:'Grant',id,payload,expiresIn?,stagedGrant})` допускает новый Grant только с live private context. Existing identical bound Grant допускает exact retry; другой payload/expiry не исправляется. `find({model:'Grant',id})` возвращает проверенный clone только при текущей live authority; `destroy` и `revokeByGrantId` монотонно отключают свою family без key. Для auxiliary payload `request/stagedGrant` по-прежнему запрещены. Successful `Interaction.result.login/consent` отдельно требует exact signed account + уже bound providerGrantId; arbitrary successful result нельзя сохранить у pending/denied proposal. Production resume route остаётся `/oauth/authorize/:uid`.

### Transaction, lifetime и time predicates

Подписанное approve/deny/list/revoke работает внутри существующего Connect request, без nested Connect fence. Одна Caps transaction создаёт fresh client/principal/root/budget/connection и CAS pending→decision. Ошибка внутри этого COMMIT откатывает всё. Ошибка outer Connect COMMIT/response **после** Caps COMMIT не отменяет решение; exact signed retry возвращает тот же connection. Terminal replay не требует decrypt/new capacity и не создаёт новые rows. Proposal cleanup после expiry может удалить сам decision replay; durable connection/owner history остаются, повтор не восстанавливает старый consent.

`beginGrantBinding` и Grant save/find открывают fresh Connect→Caps fence, busy budget100ms, без await под lock. WeakMap token связан с instance/interaction/nonce/connection; active set≤16. Token истекает через30s monotonic и не позже interaction/connection expiry, end/close инвалидирует его. Сохранённый token из старого instance не переносится на reopen. Grant INSERT и `provider_grant_id IS NULL` CAS коммитятся вместе. Проигравший с другим ID не оставляет orphan Grant. Ошибка outer fence после COMMIT оставляет durable binding, которое следующий completion читает и использует точно.

Для bound Grant:

```text
row.created_at = actual admission nowMs
floor(connection.created_at / 1000) <= payload.iat <= floor(nowMs / 1000)
payload.iat, payload.exp — safe whole seconds; exp > iat
nowMs < payload.exp * 1000 <= connection.expires_at
row.expires_at = payload.exp * 1000
row.retain_until = connection.expires_at
```

Таким образом `iat*1000` может предшествовать millisecond connection creation на <1000ms. Grant может закончиться раньше connection, но не позже. Никакой clamp/rewrite payload и изменения consent duration нет. Non-aligned positive, reopen и exp+1s negative реально проверены. Это одобренная root узкая row-validator delta; hashes DDL неизменны.

Current chain использует исходный creator/root, не глобальное равенство Connect epoch. Отзыв unrelated sibling не ломает Grant save; отзыв создавшего device до записи закрывает его. Обычные legacy credential/grant issue для linked authority остаются запрещены. Family revoke не освобождает/переоткрывает native effect самостоятельно; прежний B2 proof-first путь не изменён.

Context slots, approval capacity и raw artifacts имеют раздельные bounds. Approval≤16 active unexpired/account, lifetime principals1000/account, grants10000/account, connections10000/global; exact replay/deny/list/revoke не требуют нового slot. Proposal≤256 live pending и≤1024 unexpired/global. Cleanup делит **один** limit≤64 между auxiliary artifacts, proposals и expired Grant rows; не удаляет connection pins. Это explicit housekeeping, не гарантия автоматического удаления по wall clock без host scheduler.

Ошибки используют прежний `AccessError`/safe code: `oauth_unavailable`, `oauth_storage_key_unavailable`, `oauth_storage_busy`, `oauth_context_invalid`, `oauth_invalid_artifact`, `oauth_quota_exceeded`, `oauth_interaction_not_found`, `oauth_interaction_conflict`, `oauth_grant_conflict`, прежние owner/account/cursor codes; повреждение row/cipher — `capabilities_storage_corrupt`. Trusted clock — синхронный Date.now; обещание generic Promise callback containment на clock не распространяется.

### Авторские evidence и ограничения

Node24.21.0/SQLite3.53.4, PATH только текущего процесса, `--test-concurrency=1`:

```text
node --test --test-concurrency=1 modules/capabilities/test/oauth-connections.test.mjs
node --test --test-concurrency=1 modules/capabilities/test/oauth-profile.test.mjs modules/capabilities/test/oauth-artifacts.test.mjs modules/capabilities/test/oauth-storage.test.mjs modules/capabilities/test/oauth-baseline.test.mjs modules/capabilities/test/access.test.mjs
```

- Первый owner/Grant run:11/11 PASS,0skip,2472.1416ms (`p4-oauth-connections-first.log`).
- Final расширен двумя причинными checks: **13/13 PASS,0skip,3033.1306ms** (`p4-oauth-connections-final.log`).
- Focused compatibility: **49/49 PASS,0skip,6814.7055ms** (`p4-oauth-connections-regression.log`), aux17 + baseline/storage21 + access11.
- Syntax и targeted `git diff --check` прошли. Logs находятся в `output/implementation-20260930/`.

Fixture создаёт реальный Connect account/device, ECDSA signed requests, отдельные Connect/Caps SQLite files и штатный `withAuthorityFence`. Проверены signed approval/denial, lost outer response, чужой account/cursor, fresh creator revoke и sibling pair,16 connection/context limits, keyless replay/list/revoke, exact current Grant/Interaction pins. SQL triggers внутри approval и binding доказывают rollback всей группы rows; synthetic outer throw после фактического COMMIT доказывает durable replay.65 auxiliary artifacts+65 proposals очищаются как64/64/2, connection остаётся. Настоящий установленный Provider9.12.2 `Grant.save/find` работает через encrypted adapter с fractional clock.

Interaction payload в этих новых tests имеет настоящую принятую форму, но создаётся fixture, а не HTTP authorize. Grant contention тестируется последовательными stale contexts, **не** двумя OS writers. Outer failure seams — явные synthetic errors после actual COMMIT, **не** kill/restart процесса. Нет token flow, browser/account-switch/logout, CLI, Linux, OAuth bearer или native Note effect. Независимый review этого increment и actual root host composition ещё отдельные gates; не подразумеваются по13/13.

### Increment 2 freeze inventory

Paths относительно `modules/capabilities/`, SHA-256 локальных bytes:

| File | SHA-256 |
|---|---|
| server/oauth-connections.mjs | 28ca238064d450a3e7f99478955417901ee48bb82c63fc08cb251d0a8b7e8878 |
| server/oauth-profile.mjs | 89c35028fbfdfc5aed478dd73666e9440814b178f77274fb107a0020d955ff78 |
| server/oauth-artifacts.mjs | 98e1b400085a4057c534783301f3d1477afb2c25089f0ad37e2768b1abbcbd2c |
| server/oauth-baseline.mjs | 41f11dfaecccb8394c6a265751286670bc379d362a76b23468ce788483424f6e |
| server/access.mjs | 0e435ee5c81aa75d238776301ea397698ef1bda701c74e6a4af28d92e44691b6 |
| server/index.mjs | 51cad334e7842c31a61265d871ca5ee7b8d726c0625256c6032d70d0ebecd303 |
| test/oauth-connections.test.mjs | 023187c0fb4e94c4ea9cbace10acfc4e9c6a6d472ea6c9db25ffbcefd6be2ade |
| test/support/oauth-connections.mjs | 69ef4f624928132c7e6e1a0ce02c3cef882d24472dbf68f0a23bde164c222dbe |

Неизменённые `oauth-schema.mjs` SHA `ca2810310bab42a1d0232459163aa34eb5088de8d610aab411c17324e1002288`; `schema-v3.mjs` SHA `ebed724aed700443eadb84fd96476b14fea971c31f44b6779aa3b4b0b098e557`. Код заморожен до конкретного finding; token/bearer/dedup increment не начат.

### Narrow refreeze — Interaction.trusted, 2026-10-01

Независимый source review критика нашёл ошибку прежнего auxiliary profile: `Interaction.trusted` ошибочно проверялся как boolean. В установленном Provider9.12.2 `authorization/interactions.js` сохраняет `oidc.trusted`; `authorization/resume.js` по умолчанию восстанавливает `[]`, а `process_request_object.js` при включённом request-object механизме записывает имена параметров. Boolean относится к отдельной PAR-модели, отключённой в C1. Это не основание включать JAR/PAR.

Единственная production delta — `oauth-profile.mjs`: допускается absent/undefined либо **пустой array**; false/true, непустой array и иной тип отказываются. Schema/DDL/digest, owner/Grant coordinator и права не менялись. Прежний actual-model test сам передавал `trusted:false`, поэтому его PASS не доказывал штатный resume; этот ручной аргумент удалён. Profile negative cases теперь явно проверяют boolean и непустые arrays.

Новый `oauth-trusted.test.mjs` выполняет реальный HTTP на ephemeral loopback с настоящим Provider9.12.2: authorize → fixture login-only completion → resume → следующий native consent. Все Session/Interaction save/find проходят через настоящий encrypted auxiliary store. До поправки: **0/1 RED**, HTTP500/`oauth_invalid_artifact`,565.4253ms (`p4-oauth-trusted-red.log`). После узкой поправки второй сохранённый Interaction действительно содержит `trusted:[]` и прежний login submission, а переход доходит до consent. Проверено отсутствие Grant/token/connection rows.

```text
node --test --test-concurrency=1 modules/capabilities/test/oauth-trusted.test.mjs modules/capabilities/test/oauth-profile.test.mjs modules/capabilities/test/oauth-artifacts.test.mjs modules/capabilities/test/oauth-connections.test.mjs
```

**31/31 PASS,0skip,4918.369ms**, Node24.21.0/SQLite3.53.4; `p4-oauth-trusted-green.log`. Это aux17 + owner/Grant13 + новый HTTP1; прежние49 не прибавляются повторно к новым уникальным cases. Syntax/targeted diff-check PASS. Слот освобождён. Login в fixture синтетический, без signed Connect decision, Grant, code/token или CLI; полный production authorization не объявляется. Token increment не начат.

Эта таблица заменяет только соответствующие предыдущие SHA; остальные increment2 hashes неизменны:

| File (от modules/capabilities/) | SHA-256 |
|---|---|
| server/oauth-profile.mjs | 587b072b45bf7e041a9bd6c4705abe6ab9a5da5c8b0a3e29df748a7be00204c5 |
| test/oauth-profile.test.mjs | 64ecb0615494a5f568804f8fe07aaeeadaf95307fe3234c54fcaa60a4294a32c |
| test/oauth-artifacts.test.mjs | f77e3b6b82658458e2e3edf9ddde02219f455b46d1aa754941cec597693afc3a |
| test/oauth-trusted.test.mjs | 4c575e68c31f59c431cdaa377540800e2afdcc3a81ec3435a7d0a24d65854a39 |
