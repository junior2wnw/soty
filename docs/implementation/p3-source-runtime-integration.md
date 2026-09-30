# P3-C2-B — runtime и подписанная проверка источника

2026-09-30. База C2-A `efe0d9f` отправлена в `origin/codex/human-agent-platform-release`. C2-B локально принят после freeze, общего прогона и независимого review. Production не изменён.

## Разделение работ

- Connector: agent_ecosystem, `scripts/agent-modules/local-apps.mjs` и собственные новые tests.
- Server manager: publishing_architecture, `runtime-bindings.mjs` и14 авторских tests; после freeze отдельный read-only review root wiring.
- Independent critic: whole_product_critic, composed HTTP/WS acceptance и отдельный child-process gate immutable bundle. Production source не редактирует.
- Root: Apps service integration, async Connect adapter, strict admission/stream fences, observation DTO, production HTTP composition, release version и свои signed/channel tests.

## API и рабочее поведение

`apps.source.prepare` зарегистрирован отдельным `sourcePreparationExtension.executeAsync`. Синхронный Apps extension обслуживает `apps.source.promote` и `apps.source.history`. Старые синхронные потребители Apps не получают Promise вместо результата. Optional `expectedAccountId` проверяется до работы; модель повторяет актуальные полномочия после сетевого ожидания и перед записью.

Сервер выбирает v2 по предложенным возможностям прошедшего аутентификацию устройства. Новый случайный channelId принадлежит конкретному соединению; повторная auth во время ожидания закрывает admission, поздний успешный ответ его не воскрешает. Legacy без capabilities сохраняет v1 для floor1 и не может подготовить source switch. Неверные/неподдерживаемые offers не становятся скрытым fallback.

Запуск, обмен ticket на session и открытие HTTP/WS требуют exact binding ACK. Каждый stream хранит captured channel и branded binding reference; после асинхронной записи/ACK допуск проверяется снова. Связь между аккаунтом и session сохраняется при временном offline/pending ACK. После source commit новый epoch инвалидирует старые tickets/cookies/streams; reconnect без изменения epoch требует нового runtime ACK, но не нового входа в аккаунт.

После COMMIT callback перечитывает фактический DB target. Он согласует старый/новый/current connector и остальные каналы, ещё имеющие desired binding данного app. Replay исторического receipt не возвращает прежний источник. Периодический reconcile замечает изменения другого законного writer; неизменившиеся bindings сохраняют ACK. Пользовательские HTTP requests никогда не ставятся в очередь ожидания binding и автоматически не повторяются.

Наблюдение v2 сохраняет собственный evidence label и тот же45s срок. Оно говорит только об ответе локального процесса; config ACK и пользовательская работоспособность остаются разными свойствами. Owner inspection не принимает произвольные runtime flags. CSS, DOM и новая оболочка в C2-B не заменяются; управление переключением появится отдельным C2-C после этого gate.

## Проверено root на текущем срезе

- `app-source-http.test.mjs`: **3/3**, actual `createHttpApp`, настоящий подписанный Connect browser client с memory storage вместо IDB, native WebSocket и два loopback HTTP процесса. Unicode HEAD сериализован, promote/replay не дублирует target, после остановки connector receipt доступен. Пока HEAD удержан, независимые connections получают writer locks обеих Apps/Connect SQLite. Второе устройство действительно enrolled в тот же аккаунт и подписанной командой отзывает первое; поздний HEAD не разрешает переключение. Foreign owner/account mismatch не вызывают HEAD.
- `source-channel.test.mjs`: **4/4**, actual WS negotiation/reconnect, double auth while await, malformed capability offers, genuine legacy peer отказ source.prepare при сохранении target1/floor1.
- `source-observation.test.mjs` + `app-inspection.test.mjs`: **18/18**, v1/v2 evidence и точная граница freshness, strict private owner DTO.
- Прежние `runtime-binding-floor`, `app-runtime`, `app-runtime.acceptance`: **46/46**, legacy grants/public/session/continuous access и floor regression.
- TypeScript проходит. Логи `output/implementation-20260930/p3-c2b-{signed-http,channel,observation,legacy,types}-root.log`.

Это отдельные прогоны, их суммы не заменяют окончательный общий набор на замороженных исходниках. Сетевые tests не являются физическим browser/mobile/TLS proof.

## Перед приёмкой

1. Connector source freeze, real100-message burst, late HEAD/write callbacks, URL/limits/typed reject.
2. Независимые composed tests и read-only review server/connector/root wiring; исправить конкретные замечания.
3. Собрать immutable connector1.4.0; проверить SHA/manifest/selftest и actual child-process запуск именно этих bytes.
4. Общий Apps/world/Notes/HTTP regression, Connect regression, typecheck/build и точная фиксация skip/ограничений.
5. Коммит/push checkpoint; C2-C отдельный UI подплан. Linux compatible candidate/fallback, backup/isolated restore, owned isolated domain и production release сохраняют свои gates.

## Итоговый gate

Пункты1–4 выполнены. [Connector](p3-source-connector.md):24 авторских новых tests; [server manager](p3-source-server-bindings.md):14. Root independently repeated protocol+signed+channel slice **45/45**. [Независимый composed HTTP/WS и bundle gate](p3-source-runtime-independent.md):9/9 source scenarios и отдельный **1/1** actual standalone child process. [Независимый review root wiring](p3-source-wiring-review.md) обнаружил и закрыл W1: временный legacy reconnect больше не сжигает DB-authorized cookie или ticket, при этом запросы через него остаются запрещены. Red/green проход выполнен на той же cookie и неизменном epoch; постоянная регрессия проверяет также неиспользованный ticket.

**Общий root world:539 tests /535 pass /0 fail /4 skip.** Три прежних opt-in skip сохранены; четвёртый — bundle test, который отдельно явно включён и прошёл. Connect **70/70**, typecheck/frontend build PASS, release/update selftests PASS (`1.3.0 →1.4.0`). Логи: `p3-c2b-world-final-root.log`, `p3-c2b-protocol-root.log`, `p3-c2b-connect-root.log`, `p3-c2b-build-root.log`, `p3-c2b-{release,update}-selftest-root.log`.

Первый общий прогон539 дал531pass/4fail/4skip. Три отказа были в прежнем C1 fixture: две ожидавшие v1 evidence проверки и ожидание HEAD для unsafe historical path, который v2 теперь правильно отклоняет до сети. Исправлены точные ожидания и добавлены connected/HEAD0/unknown/HTTP503/emergency restrict assertions; все14 C1 cases прошли. Четвёртый отказ — Windows SHM cleanup до завершения SQLite worker. Устранён сам lifetime race: parent ждёт worker exit после result, cleanup в failure ждёт оба workers. Проверки конкурирующих записей не менялись;12 повторов четырьмя testprocess прошли без EBUSY. Эти изменения тестов не скрывают отказ продукта и не ослабляют негативные сценарии.

Standalone connector **1.4.0**, **189731 bytes**, SHA256 `f4ba827e556a21db97d6ac073272eaa293fde07f151ceb9c9b81b7814836c5ac`. Его exact bytes прошли child-process claim/signed API/HTTP/WS/source switch/rollback/restart proof. Это не установка ОС-службы на физическом устройстве и не Linux candidate/fallback.

Замороженные production inputs: `local-apps.mjs` SHA256 `0cc9657750c987687fc49d35f592c3e38c164214e9fb08df10d561c41eb14ec4`; `runtime-bindings.mjs` `d2af0fe49040d00e0f65e1c7a1eb980773c1ebed8ea5c25a445bf0a63a9c0b2e`; Apps `index.mjs` после W1 `2c33d937ae3c975d6d8c1ba5f4e8f60ed9f43e1644f346d8557c3e80d204fa23`.

Новый source UI и его browser acceptance ещё не реализованы данным checkpoint. Их готовность, внешний DNS/TLS, backup/restore и production не выводятся из успешных локальных gate.
