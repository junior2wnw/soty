# P4-C2b: independent service delegation review

Статус: **4/4 независимых HTTP cases PASS, 0 FAIL, 0 skip, 2132.4424 ms** на isolated Node 24.21.0. Read-only review frozen domain/HTTP diff не оставил блокеров в этом срезе. Базовый checkpoint C2a: `14f419aa12a8151557237f7de6a7bc4ab66e16af`. Это самостоятельная приёмка C2b transport/authority seams; браузер, реальные CLI и весь master здесь не объявляются завершёнными.

Рецензент владеет только новым `server/test/capabilities-delegation-independent.test.mjs` и этим документом. Production, авторские tests, PROGRESS, CLI credentials и remote не менялись. Дополнительный module test не создавался: самостоятельного полезного сценария сверх авторских двух writers/budget/revoke/rollback пока не выявлено.

## Контракт и пользовательская граница

Прочитаны §8–9 `p4-mcp-implementation-plan.md`, private WeakMap/currentCredential/ancestry в Access, constructor/transaction в Capabilities, прежние native HTTP actions/ingress, literal schema и настоящий signed HTTP fixture.

- Делегирование имеет отдельный opt-in владельца, фиксированный Notes scope, leaf principal и общий root budget. Оно не является новым универсальным MCP tool/executor, новым owner identity или новым ledger.
- Право создать помощника принадлежит действующему ordinary credential. После выдачи срок child ограничен исходным credential/chain, а последующая каскадная зависимость — от grant/principal/creator. Точечный отзыв ключа родителя не означает отзыв уже созданного помощника; UI должен объяснять это различие.
- Новый token выдаётся один раз. При потере ответа владелец проверяет точные child/parent/root IDs и отзывает возможный доступ. Ни digest-only store, ни отсутствие ответа не позволяют обещать восстановление того же plaintext или безопасный автоматический повтор.
- У service audit должен быть настоящий delegating principal. Унаследованный creator device означает линию authority владельца, а не человеческое действие, совершившее выдачу или Note effect.

## Source review и закрытые замечания

1. **HTTPS без OAuth metadata.** В C2a base `server/capabilities-actions.js` вычислял `secureResource` только при `resourceMetadata !== null`. Это оставляло configured HTTPS audience без проверки `req.secure`, если OAuth profile/PRM отсутствовал. Root отвязал predicate от PRM. Независимый actual Express case теперь подтверждает 403 до authentication для forged proxy headers и 201 при явно доверенном loopback proxy. Старый wire RED не запускался: это source finding, исправленный до первого независимого прогона.
2. **Совет повторить one-shot issuance.** Root исключил `Retry-After` из derive errors, сохранив прежние native routes. Source и независимые private responses проверены. Ошибка после возможного COMMIT не обещает отсутствие ключа и не рекомендует повтор без owner inspection; OpenAPI описывает ту же границу.
3. **Well-formed label.** Общий `validation.text()` считает UTF-16 units и запрещает controls, но не отвергает lone surrogate. Frozen coordinator дополнительно вызывает `label.isWellFormed()`. Actual wire отвергает lone surrogate и 102 UTF-16 units; 100-unit emoji label проходит без подмены профиля Unicode code points.

Прочитан весь новый coordinator и связанные изменения Access/index: private capture не включён в public service API; он разрешает настоящий WeakMap actor заново внутри Connect→Caps fence и Caps transaction. Используются current credential, точный H и вся прежняя ancestry; OAuth, copied/foreign actor не становятся новым authority path. Fixed Notes scope, child leaf и общий root budget формируются сервером. Все три account quotas проверяются до первого INSERT. Синхронный callback ограничен одним вызовом и своим временем жизни; returned thenable получает контролируемый отказ с поглощением возможного rejection.

Вызов fence не является cross-file atomic acknowledgement. Если Caps уже COMMIT, последующая ошибка освобождения Connect либо потеря ответа может оставить одну выдачу. Это честный unknown outcome, а не доказанный rollback. Account-indexed `legacyCredentialCount` избегает исходного full-table scan чужих accounts, но всё ещё обходит matching credentials данного account, включая исключаемые OAuth rows. Constant-time или предел в 10000 посещённых строк не заявляется. Точные quota/EXPLAIN и two-writer/budget/rollback tests принадлежат автору и здесь не дублировались.

Узко прочитан новый AccessPanel consent/issuance path: checkbox default-off захватывается до await, grant ACK сверяется до выдачи credential, one-attempt latch не превращает потерю секрета в повтор. Unknown view сохраняет известные exact IDs, предлагает read/list и проверяет ACK точного principal revoke. Child grant назван фактом выдачи, а не доказательством действительности всей цепи. Это source review; author controller tests и native browser root — отдельное evidence.

## Исполненные независимые wire cases

`server/test/capabilities-delegation-independent.test.mjs` использует существующий `nativeHttpFixture` только для настоящих HTTP, signed Connect и SQLite stores. Авторский delegation fixture не импортируется. В каждом случае проверяется новый transport seam:

| Сценарий | Независимый oracle | Граница fixture |
|---|---|---|
| COMMIT → потеря ответа | **PASS:** один POST/201 upstream, discarded response 1192 B, downstream 0 B/reset; один child, один service audit с parent principal, signed owner отзывает точный child; replacement requests=0 | Loopback proxy выбрасывает только ответ настоящего router, token не разбирается и не сохраняется |
| Строгий target/body | **PASS:** четыре literal aliases и 12 malformed/extra-field bodies отказаны; H/mcp credential даёт 401; затем точная 100-unit строка создаёт один leaf | Node HTTP `path`, без URL-нормализации target; настоящая авторизация, без SQL authority injection |
| Revoke во время body read | **PASS:** после настоящей initial authentication signed owner отзывает parent credential; завершение body даёт 401, new children=0 | Transparent authenticate observer только сообщает достигнутую границу; результат и actor не заменяются |
| HTTPS, AS off | **PASS:** forged plain HTTP даёт 403 до auth; explicit trusted loopback proxy даёт 201/один child, OAuth и native effect port отсутствуют | Настоящий Express predicate и второй Capabilities handle с реальным Connect fence; это не TLS certificate/deployment proof |

Вывод тестов ограничен статусами, количеством запросов/строк, байтами и boolean. Parent/child tokens, response plaintext и bearer headers не попадают в diagnostics. Временные stores принадлежат fixture с проверкой пути/owner marker перед cleanup. Fault proxy и sockets закрываются только собственным тестом.

## Запуск и атрибуция

Единственный независимый serial run выполнен после обоих source freeze и root GO; первый запуск сразу 4/4 PASS, без изменения assertions или production ради green. Command-local PATH начинался с `var/toolchains/node-v24.21.0-win-x64`:

```text
node --test --test-concurrency=1 --test-reporter=tap server/test/capabilities-delegation-independent.test.mjs
```

Log: `output/implementation-20260930/p4-delegation-independent-first.log`, SHA-256 `bce1713338479ca1d5bcdc1a8078953a06b4c15d1dc2d9e9f16a91d2ea125b84`. В TAP присутствуют штатные SDK предупреждения о `responseMode: 'json'`; console-clean не утверждается. Слот освобождён для root integrated gate.

Авторские 18/18 из [p4-service-delegation.md](p4-service-delegation.md) прочитаны и атрибутируются автору; они не повторялись и не прибавлены к независимым четырём. Root HTTP/OpenAPI и UI gates также отдельные. Proxy case не доказывает восстановление потерянного plaintext и не обещает однозначно сопоставить неизвестную выдачу среди нескольких одновременных одинаковых child; он доказывает сохранение одной фактической выдачи, безопасные owner metadata и отзыв без секрета.

Реальные CLI, OAuth login, model calls, native browser geometry, TLS deployment, production migration/rollback/restore не запускались. Новый реестр функций, параллельный ledger, иной тип owner identity или пятый MCP tool не добавлены. Неразрешённых блокеров в проверенном C2b domain/transport diff не осталось.

## Freeze проверенных worktree bytes

Хэши сверены после PASS; это bytes рабочего дерева, не обещание идентичного Git LF blob при EOL normalization.

| Файл | SHA-256 |
|---|---|
| `modules/capabilities/server/delegation.mjs` | `bb25f0893a457fdc447aa280e4e41958cc947d7b1a3b14e41ec61555735c6152` |
| `modules/capabilities/server/access.mjs` | `15062f1b4bcf0e1a84adab5cbcf31e08cbc72d5b9d583baa7654997d5c0fb016` |
| `modules/capabilities/server/index.mjs` | `21fb1a683171f01292690f2b6720be8e323273ad7adc27f8f74b1d06c5217b67` |
| `modules/capabilities/server/oauth-baseline.mjs` | `a15d02d78d62f4f1eeea41104d5a85f4a020d69306b1503e4c4c7c7ec6b3a77c` |
| `server/capabilities-actions.js` | `3391a9ff6d2f6d5aa5e1bb96b9ce7d705ed1d8b7084eefc4cc75858fe12293de` |
| `server/capabilities-ingress.js` | `bce60c74ad3e65641e5bf6bbdd21088594fd40f95946b24a34fd72755db15089` |
| `server/capabilities-http-contract.js` | `97060a3d85cc932353ee54435ff2336f2328d1eca805fbc0211985c4833f8a19` |
| `server/capabilities-openapi.js` | `f3051ce2d2de41ffbffaff19d088a2a80f6fe5f4ce531b3a37a2d062ec7918fd` |
| `server/http-app.js` | `0d5be91f099f79b4a71bad0cc72ccc0e96d207b400c24d8047ac17a8944e642f` |
| `server/test/capabilities-delegation-independent.test.mjs` | `29d3efc67da9c8306c325c683efb9414aaaf142d6f7d79f3e9e5b7bab0fe9f2e` |
