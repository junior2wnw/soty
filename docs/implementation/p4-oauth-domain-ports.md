# P4-C1c — encrypted domain ports, increment 1

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

Каждая операция читает actual3/project/registry/lineage внутри короткой Caps transaction. Чужой/изменившийся store не принимается. Busy deadline запрашивается100ms; нет await под lock, внутренней очереди и доступа к OAuth authority. Сохранённый transaction callback после возврата и повторное его выполнение отвергаются. Rejected Promise от trusted callback поглощается после controlled отказа.

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
