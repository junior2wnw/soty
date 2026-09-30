# P4-C1 T1 — независимый review token storage

01.10.2026. Независимое чтение token profile/store, узкой композиции owner coordinator, авторских fixtures и установленного `oidc-provider@9.12.2`. Production и авторские tests не изменялись; повторный test process рецензент не запускал. Это приёмка внутреннего T1-среза, а не готовность OAuth endpoint, bearer resource server, CLI или P0–P8.

## Вывод

После двух узких SQL исправлений незакрытых блокеров в прочитанном T1-срезе не найдено. `readiness().available` остаётся `false`, `authenticateBearer` отказывает. DDL, существующий Connect account, native Invocation ledger и budget не меняются.

Основание — собственный source review, прочитанные фактические SQLite query plans и авторский финальный log. **24/24 PASS, 0 skip, 5985.6303 ms** в `output/implementation-20260930/p4-oauth-tokens-final.log`: 11 token cases + 13 owner cases. Это результат автора, не отдельный независимый прогон. Предыдущие 24/24 на раннем source не переносятся на final hash; финальный log относится к SQL corrections ниже.

## Найденные ограничения SQL и их исправления

**Проверка active AT quota.** Независимый review обнаружил, что фильтр по `k.account_id/k.expires_at` расположен на `cap_credentials`, у которой нет подходящего expiry index. Автор проверил точный запрос на actual schema3: `SCAN k` + point lookup `l`. Каждая выдача могла обходить legacy и исторические credential rows под Connect → Caps fence. `p4-oauth-token-quota-plan.log` содержит исходный plan и исправленный.

Исправление использует существующий `cap_oauth_credentials_expiry`, диапазон `l.expires_at > now`, lookup credential по PK и subquery с `LIMIT 64`. Равенство link/credential expiry уже является persisted invariant, поэтому правило квоты не изменилось. Авторский regression захватывает фактический SQL из service admission, возвращает `DatabaseSync.prototype.prepare` в `finally` до любого await и выполняет SQLite `EXPLAIN QUERY PLAN`; capacity64, exact retry, signed list и revoke assertions сохранены. Исчезли полный `SCAN k` и исторический prefix.

Точный предел: `LIMIT 64` ограничивает совпавшие active AT этого account, **не** гарантирует 64 физических чтения. Диапазон может пройти по ещё не истёкшим links других accounts или отозванным credentials. Это не доказательство wall-clock deadline или throughput под нагрузкой.

**Cleanup continuation.** Автор отдельно обнаружил, что прежний OR keyset давал `MULTI-INDEX OR` и `USE TEMP B-TREE FOR ORDER BY`; один `LIMIT` не ограничивал сортируемый suffix. Замена на `(expires_at,credential_id) > (?,?)` использует тот же порядок двух NOT NULL колонок и один covering-index seek. Рецензент прочитал обе версии plan в `p4-oauth-token-cleanup-plan.log` и итоговый source. Regression объясняет реально захваченный cleanup SQL на той же SQLite, а retained-prefix progression и reopen остаются проверенными.

Оба исправления принадлежат автору. Новая схема, миграция или второй quota ledger не понадобились.

## Проверенные source invariants

- Exact token models — code, RT, AT. Opaque ID и S256 challenge имеют ожидаемую 43-character base64url форму; bounded data snapshot не вызывает getters. Поля issuer/account/client/grant/resource/scope проверяются против durable connection. Unsupported claims, session-coupled expiry, scope/resource arrays и расширение сроков не принимаются.
- Сроки сохраняются в whole seconds payload и actual admission milliseconds row. Code ≤60 s, AT ≤300 s, RT ≤connection/Grant expiry. Повтор не продлевает credential. `expiresIn` не подменяет authoritative `payload.exp`.
- Новый AT artifact, существующий cap credential и immutable OAuth link создаются в одной Caps transaction внутри fresh Connect fence. Ошибка link INSERT откатывает всю тройку. Ошибка outer fence после Caps COMMIT не выдаётся за rollback: exact retry возвращается без новой credential или продления expiry.
- Request snapshot проверяет client, resource, scope и grant type до consume CAS. Неверный resource не тратит неиспользованный source. Повтор consumed source возвращает `invalid_grant` после durable family revoke, не бросает исключение до COMMIT и не отменяет отзыв. Поздний save проходит новую live проверку; sibling connection имеет отдельную authority.
- `find` проверяет live creator/common chain, plaintext pins, AEAD/digest и payload; отдаёт clone. Consumed timestamp берётся из durable column. Wrong/expired source не создаёт новую authority.
- Destroy/revoke не требует decrypt: после удаления expired raw AT retained link ещё позволяет найти точную family. Известный Session/Interaction destroy остаётся локальным auxiliary действием, не переносит borrowed Grant в family authority.
- Cleanup делит один selected-identity budget между artifacts, proposals и credentials. Link+credential удаляются атомарно лишь после исчезновения raw AT и при отсутствии любого original Invocation reference; referenced rows, connection и receipt pins не удаляются. Keyset проходит retained prefix, а persisted delete guards дополнительно защищают ссылки.
- Composition не открывает внешний bearer admission и не меняет native execution. Наличие работающего внутреннего token store не повышает readiness до `true`.

## Что фактически доказывают fixtures

`tokensFixture` использует настоящие ECDSA signed Connect requests, Connect/Caps SQLite files и реальный fence для owner approval. Initial Interaction и часть Grant/token payloads созданы test fixture. Отдельный case исполняет настоящие Provider model `save/find/consume`, но request snapshot задаётся тестом; он не доказывает HTTP client authentication или isolation `Provider.ctx`.

Два concurrency cases запускают отдельные OS processes, каждый с собственными Connect/Caps handles. Они проверяют consume/consume и consume/revoke на общих SQLite, ждут завершения процессов и последующий reopen. Это не две полноценных AS HTTP instances.

Retention case вставляет явно synthetic persisted Invocation references. Он проверяет индекс/delete/reopen invariant; реального Notes effect в T1 нет. SQL-abort и post-COMMIT throw — явные fault seams, не kill/restart посередине issuance. Log первого model-case failure относится к fixture configuration `issueRefreshToken`; production по нему не менялась.

Следующий независимый actual host gate должен связать production Provider, настоящий signed consent двух accounts, T2 branded bearer, HTTP resource audience и native admission. Особенно нужны текущий read после эффекта/отзыва, original credential expiry после refresh, keyless AS-off с ещё действующим AT, deny после expiry и отсутствие fallback между legacy/OAuth. Подмена этих gates текущими 24 PASS недопустима.

## Итоговые local byte pins

Пути относительно репозитория; SHA-256 прочитан с диска после SQL corrections.

| File | SHA-256 |
|---|---|
| modules/capabilities/server/oauth-token-profile.mjs | b40b597f02f52ac4bfa272c542832a60442c3287a1fd48da0238d01034a67bc5 |
| modules/capabilities/server/oauth-tokens.mjs | 24a4a9d662da44e7d70aebc1c0ed893dbb8c636ab9849b02c0a8da7b99e2d6de |
| modules/capabilities/server/oauth-connections.mjs | c92ab8adcad852638a0022022a27ad6a992cc72efc6a09fea1d43dad1cc3dadd |
| modules/capabilities/test/oauth-tokens.test.mjs | dd41b88d12fc4163711da27e2bf0c3efe70f9bd3eb7032bf7416ea3dac63db1f |
| modules/capabilities/test/support/oauth-tokens.mjs | b0557febfbff81df8f7028f75d526d105735b768800f1390a50c8bdcf9113e70 |
| modules/capabilities/test/support/oauth-token-worker.mjs | 69ff059fd7ac74e0625543284276cef5990d571df09038e2ae72f0f14cf9cfc8 |
| output/implementation-20260930/p4-oauth-tokens-final.log | 8076e966d906fc8918e16465803af42539bca8680e5927797de8440194e3e583 |
| output/implementation-20260930/p4-oauth-token-quota-plan.log | f7610275148fcc3ed9e851f3fbef0f6a40dde348ed76f6fb256c35570fa12683 |
| output/implementation-20260930/p4-oauth-token-cleanup-plan.log | 0b4e9665ccf5155c9652fe36ca04c18750a9c6a15b931e2f7af7b3006b75f37e |

Первый token source `59c5f0f9919105b7200c4fbb1caa8190ff8c2f8b86863804490001d0aaab21b9` — исторический snapshot до исправлений, не текущий accepted source.
