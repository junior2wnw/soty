# P4-C1 — независимый owner/Grant source review

01.10.2026. Read-only review increment2 поверх `8803fcd`, включая последнюю узкую поправку `Interaction.trusted`; проверенный increment затем зафиксирован root как **`e8edc24`**. Будущие token files T1 в этот снимок не входят. Прочитаны coordinator целиком, изменённые private Access/aux seams, index composition, baseline row checks, signed Connect dispatch и авторские 13 cases/fixture. Production, авторские tests и DDL рецензент не менял; новых executable tests в этом срезе не запускал.

**Нового блокера в проверенном owner/Grant source не найдено.** Принимать можно фиксированное signed решение и его durable Grant binding. Нельзя выводить из этого готовность token issuance/bearer, полного HTTP OAuth или CLI: `readiness().available=false`, Code/RT/AT, consume и authenticateBearer закрыты.

## Проверенные границы

**Два разных входа в authority.** Signed approve/deny/list/revoke вызываются из штатного синхронного Connect extension. Прочитанный `Connect.handle` держит `BEGIN IMMEDIATE`, проверяет proof и строит actor из active installation, а не из request fields. Coordinator проверяет `expectedAccountId`, затем открывает одну Caps transaction; повторно брать Connect fence здесь было бы nested ошибкой. Browser-side begin/save/find Grant, наоборот, входят в собственный свежий Connect→Caps fence. Прямой вызов service.execute из произвольного host кода не становится криптографически подписанным от наличия accountId/deviceId: правильная Connect composition остаётся явной предпосылкой.

**Одна группа authority rows.** Approval переиспользует private inserts прежнего Access и атомарно создаёт новый client, principal, неделегируемый root только `notes.createDraft@1`, прежний invocations budget, connection и terminal decision. Клиент запроса не выбирает эти внутренние IDs. Создание отдельного principal через legacy API продолжает создавать отдельный client; linked client/principal/root не могут получить дополнительные legacy credentials/grants. Нового budget ledger нет. Actor и исходный creator повторно читаются через существующий common chain; unrelated sibling epoch не подменяет creator revocation.

**Решение связано с тем, что показано владельцу.** Prepare читает decrypted bounded Interaction и registered callback; digest фиксирует issuer/UID/client/redirect/resource/scope/S256/challenge/state hash/duration/budget/expiry, browser nonce хранится как hash. Pending approval заново сверяет текущий encrypted intent. Terminal exact replay проверяет прежние digest/nonce/account/decision и не создаёт authority заново. Противоположное решение или другой account получает отказ. Successful Interaction result разрешён только для signed approved account и уже bound exact Grant, включая прежний request digest.

**Keyless safety и неизвестный результат.** Exact signed decision replay, owner list и safety revoke работают без decrypt. Это возврат прежнего решения, а не новое разрешение выдавать Grant. Fresh approve требует encrypted current intent; begin/save/find Grant требуют key и current authority. Caps COMMIT может предшествовать ошибке внешнего Connect COMMIT/доставки; durable decision/binding сохраняются, поэтому ошибка ответа не означает rollback всех databases. Авторские synthetic post-COMMIT failures проверяют именно это, а не настоящий crash/потерю сети.

**Private staging.** Context — объект из private WeakMap конкретного instance, active set≤16, monotonic30s и не дольше Interaction/connection expiry. Copy/foreign/ended/reopened context не даёт новый Grant. `endGrantBinding` и close инвалидируют; сохранённый callback проверяется на lifetime и single invocation. Ни DB transaction, ни Connect fence не удерживаются через await. Grant INSERT и `provider_grant_id IS NULL` CAS происходят в одном Caps COMMIT; contender с другим ID не оставляет orphan. Exact existing Grant разрешён лишь с неизменным payload/expiry и свежей current chain, не как произвольное изменение scope.

**Сроки и plaintext reader.** Bound row.created_at — фактическое admission ms, payload.iat/exp — seconds. Grant.exp может быть округлён вниз относительно connection expiry, но никогда не после него; retain_until остаётся исходным connection expiry. Убрано только ошибочное требование equality в baseline, прежний upper bound остался. Actual3/project/registry/lineage проверяются до и после port transaction. DDL и fingerprints reader3 не изменились; keyless row validator не обещает проверку AEAD без ключа.

**Ограничение объёма и cleanup.** Новое approval требует свободных existing principal/grant/global connection квот. Квота16 считает stored `state='active'` и unexpired connections; `list.active` дополнительно отражает текущую creator/chain authority. Поэтому отозванный creator может дать `active:false` при ещё занимающей slot connection до explicit revoke/expiry — это важная граница для будущего owner UI. List/revoke/deny/replay не требуют нового slot. Cleanup делит один limit≤64 между auxiliary artifacts, proposals и expired Grant rows; не удаляет connection pins, native receipts или original credential references. Автоматический запуск cleanup этим исходником не гарантируется.

## Покрытие и следующий необходимый gate

Авторский `oauth-connections.test.mjs`: **13/13 PASS, 0 skips, 3033.1306 ms**, подтверждено чтением `output/implementation-20260930/p4-oauth-connections-final.log`. Это результат автора, не независимый повтор. Fixture использует настоящие ECDSA-signed Connect requests, два отдельных SQLite stores и штатный fence. SQL ABORT проверяет rollback связанных rows; проверены exact replay, keyless safety, другой account/cursor, creator revoke/sibling,16 contexts, cleanup64 и non-aligned expiry. Прямой actual Provider `Grant.save/find` использует encrypted adapter, но Interaction создаётся fixture.

Ограничения авторских cases названы корректно: contention — последовательные stale contexts, не два OS writers; outer throw — synthetic post-COMMIT, не kill/restart. Эти данные не заменяют предстоящий actual host→signed decision→encrypted bound Grant→resume. Собственный host13 из отдельного [отчёта](p4-oauth-host-review.md) использует synthetic authority adapter; две разные зелёные проверки нельзя склеивать в утверждение о полном интегрированном OAuth.

Source finding `Interaction.trusted` устранён автором по отдельному реальному Provider HTTP login-only→fresh consent через encrypted auxiliary store: прежний boolean профиль дал RED, новый absent/undefined/[] дал GREEN; JAR/PAR не включались. Авторские aux17+owner13+HTTP1:31/31 PASS,4918ms, отдельно от owner13. Ручной `trusted:false` в прежнем model test не считался доказательством штатного resume и удалён.

Перед token readiness остаются решающие integration cases: настоящий signed account A→B через те же encrypted ports; actual Code/RT/AT profile и authenticated resource/client binding; consume/revoke конкурентная граница и late upsert; retained original credential expiry/reference cleanup; неизвестный token response без автоматического повторного effect. Это уже следующий согласованный token/host scope, а не требование добавить второй toy adapter или расширить текущий increment.

## Проверенный снимок

SHA-256 локальных bytes, пути относительно `modules/capabilities/`:

```text
28ca238064d450a3e7f99478955417901ee48bb82c63fc08cb251d0a8b7e8878  server/oauth-connections.mjs
587b072b45bf7e041a9bd6c4705abe6ab9a5da5c8b0a3e29df748a7be00204c5  server/oauth-profile.mjs
98e1b400085a4057c534783301f3d1477afb2c25089f0ad37e2768b1abbcbd2c  server/oauth-artifacts.mjs
41f11dfaecccb8394c6a265751286670bc379d362a76b23468ce788483424f6e  server/oauth-baseline.mjs
0e435ee5c81aa75d238776301ea397698ef1bda701c74e6a4af28d92e44691b6  server/access.mjs
51cad334e7842c31a61265d871ca5ee7b8d726c0625256c6032d70d0ebecd303  server/index.mjs
023187c0fb4e94c4ea9cbace10acfc4e9c6a6d472ea6c9db25ffbcefd6be2ade  test/oauth-connections.test.mjs
69ef4f624928132c7e6e1a0ce02c3cef882d24472dbf68f0a23bde164c222dbe  test/support/oauth-connections.mjs
ca2810310bab42a1d0232459163aa34eb5088de8d610aab411c17324e1002288  server/oauth-schema.mjs
ebed724aed700443eadb84fd96476b14fea971c31f44b6779aa3b4b0b098e557  server/schema-v3.mjs
```

Reader labels, deployment, production DB migration, real CLI login и remote execution не выполнялись и этим review не принимаются.

## Отдельная root delta — owner presentation marker

После e8edc24 прочитан узкий `access.mjs` diff: create/list/revoke используют `publicPrincipal`, который добавляет только `managedBy:'oauth'` при exact durable client/principal/account linkage. Он не сравнивает label, не зависит от AS keys/live state и не выдаёт provider Grant IDs. Legacy principal с тем же названием остаётся без marker; прежние owner checks неизменны. Нового permission/DDL нет, дополнительных блокеров не найдено.

AS-absent UI fallback на прежний `access.principals.revoke` — действительный safety revoke: connection row может остаться `state='active'`, но отозванные client/principal закрывают common chain, `list.active=false`; последующие Grant/token guards не должны её обходить. Это не удаление connection и не освобождение её stored-state slot само по себе. UI должен оставлять такую запись доступной для управления, даже если специальный OAuth list API недоступен.

Root сообщил **projection3 + access11 = 14/14 PASS**. Тест source прочитан, повтор не запускался. Его AS-absent case честно использует actual owner под штатным Connect fence/direct reader; не изображает новый signed login. Проверены same-label negative, другой account, expiry/keyless/revoke и отсутствие создания credentials. Scope этого addendum — только projection delta; будущий token T1 не включён.

```text
4df65c3908214c012f2c3312be4d8afec7360c0690024ac87aede88b7511bced  modules/capabilities/server/access.mjs
dec04fe8e1ef8a9e1bfeeda780ecd0ddb9a3410ff8b583ccdc94832bd556898a  modules/capabilities/test/oauth-principal-projection.test.mjs
```
