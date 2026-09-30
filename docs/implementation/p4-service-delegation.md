# P4-C2b: выдача дочернего service-доступа

Авторский доменный checkpoint поверх принятого C2a `14f419aa12a8151557237f7de6a7bc4ab66e16af`.
Контракт: [p4-mcp-implementation-plan.md](p4-mcp-implementation-plan.md), §8–9.
Этот receipt подтверждает локальные domain/SQLite/Connect/Notes проверки; HTTP adapter, UI, независимая приёмка и реальные CLI принадлежат другим владельцам и здесь не объявлены завершёнными.

## Интерфейс и граница полномочий

Trusted constructor option:

```js
createCapabilitiesService({
  // …existing options
  delegation: { audience: H, withAuthorityFence }
});
service.delegation.derive({ actor, label, expiresAt });
// synchronous → { principal, grant, credential, token }
```

`H` — точный canonical origin: HTTPS либо HTTP только для `localhost`, `127.0.0.1`, `[::1]`; URL с path/trailing slash не заменяет origin. Конфигурация копируется и проверяется до открытия storage. Отсутствующий `delegation` сохраняет остальные возможности сервиса, но `derive` отказывает `delegation_unavailable`. AS runtime, artifact key и Notes readiness для выдачи не требуются: выданное разрешение само по себе не обещает текущую доступность исполнения Notes.

Public port — frozen object с единственным `derive`. Service-операция не добавлена в signed owner `execute/operations`. Private callback захватывается `index.mjs` из Access closure; `resolveActor`, credential reference и произвольные issuer helpers наружу не выдаются. Caller передаёт настоящий opaque actor, полученный именно этим экземпляром сервиса. Копия DTO и actor другого экземпляра не являются полномочием.

Request содержит ровно три собственных enumerable data properties. Getter, symbol, inherited/custom-prototype fields и лишние authority поля отказаны. `label` — строка 1–100 UTF-16 units, well-formed Unicode, с прежними control-character ограничениями; `expiresAt` — safe integer milliseconds. Их значения захватываются до host callback. Не принимаются caller-supplied account, parent/root/client, audience, budget, scope или request ID.

`credential` DTO имеет ровно `{id,audience,expiresAt,createdAt,grantId}`. `principal` и `grant` сохраняют существующие DTO, включая exact IDs и общий root budget. Все уровни результата frozen. Plaintext token возвращается один раз; в SQLite хранится только SHA-256 digest. Логи и audit токен не получают.

## Транзакция, отзыв, неизвестный результат

Порядок: real `Connect.withAuthorityFence` → Caps `BEGIN IMMEDIATE` с временным `busy_timeout=100` → повторный WeakMap/currentCredential/ancestry resolver → quota preflight → inserts → Caps COMMIT → освобождение Connect fence. Это не общая cross-file транзакция. Внутри нет await/network. Предыдущий Caps timeout восстанавливается; Connect самостоятельно сохраняет свой timeout.

Обычный credential должен быть текущим, иметь audience `H`, а вся цепь — действительные principals/clients/creator devices, сроки и сужение scope. OAuth actor не допускается. Child получает ровно `notes.createDraft@1`, `notes:new`, `create`, `soty:notes`, `allowDelegation:false`, `maxDepth:0`; новый grant имеет parent depth+1, прежний root и expiry не позже текущего credential, каждого ancestor и configured max TTL. Creator device наследуется от проверенного parent grant. Это lineage владельца, а не заявление, что человек выполнил выдачу.

До первой вставки проверяются существующие account quotas: 1000 principals, 10000 grants, 10000 ordinary credentials. Затем одной Caps транзакцией создаются client, principal, grant, credential и audit. Audit: `kind='access.grants.derive'`, `object_type='grant'`, exact child grant ID, `actor_type='service'`, actual parent principal ID. Новый budget не создаётся; derive не резервирует/не списывает Note action. Ошибка audit откатывает все пять вставок.

Отзыв parent credential закрывает новые запросы родителя, но не отменяет уже выданного child. Parent grant/principal/creator revoke закрывает child через прежнюю ancestry. Child native invocation сохраняет собственный original credential и абсолютный срок; root policy epoch сам по себе не подменяет эту проверку. Parent/sibling не получают child history; owner видит обе записи. Одинаковый idempotency key разных service clients остаётся разными namespaces.

Callback может исполниться только один раз синхронно; поздний, reentrant, отсутствующий или thenable callback отвергается. Если ошибочный trusted fence сначала выполнил callback/COMMIT, а затем бросил исключение, возвратил thenable/чужой result или попытался вызвать callback повторно, первая выдача уже может существовать. Тесты не объявляют такие ошибки rollback: фиксируют максимум одну вставленную выдачу и неизвестный ответ. Потеря обычного сетевого ACK имеет ту же пользовательскую границу. Нет автоматического повторения `derive`, скрытого восстановления токена или новой receipt-таблицы. Owner читает exact metadata/audit и отзывает доступ; следующая выдача — отдельное осознанное действие.

Fixed errors нового seam: `delegation_configuration_invalid` (constructor), `delegation_unavailable`, `delegation_context_invalid`, `delegation_storage_busy`. Сохраняются `invalid_input`, `invalid_unicode`, `expiry_invalid`, `authorization_required`, `access_denied`, `delegation_denied`, `quota_exceeded`, `service_closed`, host `connect_authority_busy`. Custom catalog без Notes может вернуть существующий `not_found`. HTTP status mapping и no-store/secret response принадлежат root adapter.

## Подсчёт credential quota без DDL

До разрешённой условной правки выполнен populated probe на Node **24.21.0**, SQLite **3.53.4**, 4096 synthetic credentials. Прежний query plan: `SCAN cap_credentials`. Query через существующие `cap_grants_account` и `cap_credentials_grant` дал тот же count и:

```text
SEARCH g USING COVERING INDEX cap_grants_account (account_id=?)
SEARCH k USING INDEX cap_credentials_grant (grant_id=?)
```

Изменена только `legacyCredentialCount` в `oauth-baseline.mjs`. Account grant является исходной стороной CROSS JOIN, credential дополнительно проверяется по account. Прежнее точное `NOT EXISTS` для OAuth link/account/client/principal/root/resource/digest/timestamps сохранено. Сравнение count/query plan проверено на schemas 1/2; в schema 3 проверены исключение настоящего OAuth credential и включение ordinary credential рядом с ним.

Граница: это account-lineage traversal для корректно выданных строк, а не constant-time или гарантия ≤10000 прочитанных rows. `count(*)` рассматривает credentials этого account, в том числе исключаемые OAuth rows; чужие accounts не становятся исходным full-table scan. Никакого нового индекса, migration, retention или silent deletion. Revoked rows продолжают занимать прежние provisioning quotas, поэтому отзыв не обещает освободить место для новой выдачи.

## Авторская проверка

Команда с isolated Node 24.21.0 первым в PATH:

```text
node --test --test-concurrency=1 modules/capabilities/test/delegation.test.mjs modules/capabilities/test/delegation-native.test.mjs
```

**18/18 PASS, 0 skipped, 5173.437 ms.** Log: `output/implementation-20260930/p4-delegation-author-final.log`, SHA-256 `b952475a9c9e939ecccc778e9daa952ec7123778566d712ac8722a1d5fece937`.

- Model 12: private/fake/foreign actor; exact/widened fields; getter/symbol/Unicode; credential/ancestor/TTL limits and inherited creator; grant/principal/creator vs credential revoke; fixed scope; все quota до INSERT; audit rollback; missing/mutated config; late/reentrant/thenable/double/substituted fence; committed lost reply/reopen/revoke; populated query plans; genuine same-service OAuth actor denial и ordinary issuance на AS-absent/keyless schema 3. Большинство model cases используют явно обозначенный synchronous test fence; они не заменяют следующий real Connect gate.
- Native 6: signed owner выдаёт delegable grant; два child создают private Notes с отдельными identities/receipts, same-child retry создаёт одну Note, parent/sibling history отказана, owner читает результат; parent credential revoke не отменяет child original dispatch, root grant revoke отменяет; второй настоящий Connect OS process отзывает creator; два OS writers с разными children борются за последний общий budget unit; реальные Caps/Connect writer locks дают отказ до выдачи с освобождением fence и восстановлением timeout; kill после Caps COMMIT до caller reply, reopen и owner revoke по metadata без токена.
- В race допускается штатный bounded busy отказ. Если он возникает, только idempotent Note request повторяется после завершения workers; derive никогда не повторяется автоматически. Итоговые точные witnesses: одна admission, одна Note/proof/receipt, root spent=1/reserved=0.
- Cleanup ждёт фактического завершения процессов до удаления их отдельных временных SQLite directories. Секреты передаются детям только по IPC и не попадают в stdout, командную строку или сообщения ошибок.

Первый native run: 5 PASS/1 FAIL из-за моего неверного ожидаемого error code `not_found`. Реальный native contract возвращает `invocation_not_found`; исправлен только author assertion. Первоначальный log `p4-delegation-native-first.log` сохранён; это не production RED→GREEN. `git diff --check` для owned files прошёл. Широкие suites, HTTP, браузер и CLI автором этого checkpoint не запускались.

## Freeze исполнявшихся worktree bytes

Это SHA-256 текущих файлов, не обещание идентичного Git LF blob после EOL normalization.

| Файл | SHA-256 |
|---|---|
| `modules/capabilities/server/access.mjs` | `15062f1b4bcf0e1a84adab5cbcf31e08cbc72d5b9d583baa7654997d5c0fb016` |
| `modules/capabilities/server/index.mjs` | `21fb1a683171f01292690f2b6720be8e323273ad7adc27f8f74b1d06c5217b67` |
| `modules/capabilities/server/delegation.mjs` | `bb25f0893a457fdc447aa280e4e41958cc947d7b1a3b14e41ec61555735c6152` |
| `modules/capabilities/server/oauth-baseline.mjs` | `a15d02d78d62f4f1eeea41104d5a85f4a020d69306b1503e4c4c7c7ec6b3a77c` |
| `modules/capabilities/test/delegation.test.mjs` | `e2b54f035c324c52aade820d64a80bdecbe2f11aedd9ef1bdc669b3366642d2e` |
| `modules/capabilities/test/delegation-native.test.mjs` | `b5e53d0946c51f73edef6a18e20683f943d64711b2c1764c1005107f7b73e077` |

Source frozen перед передачей serial test slot UI-автору. Последующий root/independent acceptance атрибутируется отдельно; production/remote/serving данные не менялись.
