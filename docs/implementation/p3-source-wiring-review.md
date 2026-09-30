# C2-B — независимая проверка серверной интеграции

2026-09-30. Проверяющий: publishing_architecture. Это review кода root, отдельно от авторства `runtime-bindings.mjs`. Root source и тесты проверяющий не изменял.

## Область и доказательство

Прочитаны `modules/apps/server/index.mjs`, `inspection.mjs`, `source-observation.mjs`, `server/http-app.js`, подписанный HTTP test и channel negotiation test. Дополнительно проверен существующий Connect dispatch для mutually exclusive sync/async extensions.

Независимо повторён набор:

```text
node --test server/test/app-source-http.test.mjs modules/apps/test/source-channel.test.mjs
```

**7/7 PASS, 0 skips**: настоящий подписанный Connect prepare/promote/history, оба SQL writer lock свободны во время held HEAD, отзыв browser identity после await, чужой owner/expectedAccountId до probe; настоящий WS negotiation/reconnect, повторный auth во время verification, unsupported offers, legacy target1 без права source preparation. Тесты создают собственные loopback upstream и временные базы, используют memory storage только вместо браузерного IDB. Браузер и production не затрагивались.

## Найденный blocker W1: временный legacy transport удаляет законную сессию

Исходная цепочка в `index.mjs`: `invalidateAccess → retainAccess → checkAccess → assertRuntimeBinding`. Последняя проверяет текущий режим connector. Поэтому при Apps4 floor2 и неизменных правах временное подключение legacy1 приводит к удалению session/ticket из карты, хотя публикация, target, epoch и actor остаются действующими.

Отдельный read-only inline probe использовал настоящий fixture `server/test/app-source-http.test.mjs`, импортированный из текущего файла в память без его правки. Действия: signed source promote в floor2, реальный session POST/cookie, остановка v2 runtime, authenticated legacy WS с той же connector identity, вызов публичного `invalidateAccess()` как того же audit path, возвращение v2 runtime/fresh ACK, GET с прежней cookie.

Наблюдение до исправления:

```json
{"before":200,"during":503,"duringError":"app_source_protocol_required","after":403,"afterError":"apps_access_denied","unchangedEpoch":true,"freshLaunchWorks":true}
```

Это не подмена policy/SQLite/actor: изменялся только реальный transport. Секреты/token/cookie не выводились. Первый вспомогательный probe с `fetch` не получил cookie на этапе setup; результат не использован как доказательство. Повтор с `node:http` и точным Host завершился успешно; его synthetic ресурсы закрыты fixture cleanup.

Root получил finding до изменений. Предложенный узкий fix: retention заново проверяет только branded `publications.recheckAccess`; protocol refusal остаётся в request/open/live stream paths. Сравнение DB floor/epoch/target в branded decision сохраняется, поэтому настоящий source switch или внешнее повышение floor по-прежнему отзывает прежние права. Критик добавляет независимый regression в собственный actual fixture.

Root изменил только retention: `holder.decision = publications.recheckAccess(holder.decision)`. Проверяющий повторно прочитал guard и выполнил тот же реальный inline probe. Результат после исправления:

```json
{"before":200,"during":503,"duringError":"app_source_protocol_required","after":200,"afterError":null,"unchangedEpoch":true,"freshLaunchWorks":true}
```

**W1 закрыт.** Та же cookie работает после compatible reconnect, а запрос во время legacy по-прежнему отказывает. После исправления дополнительно повторены исходные 7 тестов вместе с 3 `runtime-binding-floor.test.mjs`: **10/10 PASS, 0 skips**. В частности, actual floor2 на target1 не допускается к launch/session/HTTP/WS через legacy, и reopen не теряет floor. Synthetic ресурсы обоих законченных probes закрыты cleanup. Постоянный независимый regression добавляет критик в свой отдельный actual-runtime fixture.

## Остальные проверенные границы

- Синхронный Apps extension не содержит prepare. Отдельный `sourcePreparationExtension.executeAsync` подключён в HTTP composition; signed Connect завершает proof transaction до await. `authenticatedArgs` сохраняет expectedAccountId и actorActive до dispatch, source registry повторяет authority/CAS после HEAD.
- Один socket не может пройти два параллельных auth: `authenticating/admissionClosed`, readyState и closed повторно проверяются после authenticateConnector. Replacement снимает старые bindings/streams, выдаёт fresh channelId; late старый message и close проверяют actual map identity.
- V2 controls обрабатываются синхронно без promise-count queue. Legacy observation в v2 и server-to-client control в неверном направлении отклоняются. Неподдерживаемое offer без 1/2 не превращается в implicit legacy.
- Каждый stream хранит captured channel и branded reference. До open и после awaited writes/chunk ACK проверяются policy, current socket и exact binding; send не ищет изменяемый глобальный socket. Удаление stream из maps проверяет тождество объекта.
- Source commit callback перечитывает actual DB target; replay старого receipt согласовывает нынешний маршрут, не восстанавливает historical pointer. Heartbeat повторно согласовывает внешние законные записи, сохраняя unchanged ACK.
- Observation read-model принимает фиксированный v2 evidence; target revision/digest сопоставляются с active DB target, а freshness45s остаётся серверной. Configuration ACK не создаёт ready health.
- Module close сначала отменяет transient source preparations и bindings, затем таймеры/подписки/streams/maps и SQLite. Pending prepare не удерживает SQLite transaction. Post-close auth completion проверяет closed и не открывает новый channel.

## Пределы вывода

Этот review не является независимой приёмкой собственного manager, всего connector bundle, браузерного UI или production. Поведенческие source switch/rollback/held ACK/live-stream сценарии критика и immutable bundle выполняются отдельно. DNS/TLS, runtime site isolation, Linux exact-image probe, backup/restore и совместимый fallback не проверялись здесь.

Финальный прочитанный и проверенный `index.mjs` после W1 fix: SHA256 `2c33d937ae3c975d6d8c1ba5f4e8f60ed9f43e1644f346d8557c3e80d204fa23`. Отдельный hash исходной red-версии не фиксировался. Остальные review hashes: HTTP composition `dfb3df19e03ac5a50d9c04a2b61ae227900d97e64b72597faec14ef288d02d32`; signed test `faf4bb801c137e5890e878d247ad8b9a8db08d55ba19cfc1fb982558dedcce2a`; channel test `b40c98ff069bfb3dc3717d8dea9097ef52c33d1f8999cfc2a3f04248563fd4ff`.

В объявленной области итоговый verdict — PASS после W1 fix; других consequential blockers при этом ограниченном source/network review не найдено.
