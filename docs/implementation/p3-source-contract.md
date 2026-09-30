# P3-C2 — смена источника без потери приложения

2026-09-30. Контракт после совместного preflight. Предыдущий C1 принят в `914a91a`. Этот документ задаёт приёмку, а не объявляет её выполненной.

## Результат для человека

У существующей аппки можно выбрать другое подтверждённое устройство, порт и стартовую страницу. Её адреса, участники и обсуждение сохраняются. Проверка кандидата не затрагивает работающую версию. После отдельного подтверждения текущий маршрут меняется атомарно. Возврат выбирает прежний маршрут, заново проверяет устройство и меняет версию доступа; содержимое программы и пользовательские данные не откатываются.

Публичное разрешение относится к точному устройству и всему выбранному порту. Для нового источника и возврата его нужно подтвердить заново. Альтернатива — явно выбрать ограниченный доступ. Автоматического переноса согласия или скрытого изменения аудитории нет.

## Последовательность и владельцы

1. **C2-A — данные и совместимый reader.** Неизменяемые targets, постоянная минимальная версия привязки, сохранённые receipts; атомарная миграция исторических Apps1/2/3 в Apps4. В этот checkpoint source API ещё не подключается к серверу. Сервер с одним legacy-протоколом обязан отказывать любому head2, включая возврат на target1. Автор модели — publishing_architecture; reader/deploy — whole_product_critic; интеграция и реальный legacy wire gate — root; независимый аудит — agent_ecosystem.
2. **C2-B — фактический протокол.** Capability negotiation, socket-owned binding/ACK, transient проверка кандидата, точные pins до HTTP/WS, подключение prepare/promote/history. Реальные два upstream процесса и переподключения; source commit, lost ACK, two-writer конфликты и rollback проходят независимые сценарии.
3. **C2-C — управление источником.** Тот же новый интерфейс C1: выбор → проверка → краткий итог → подтверждение → текущее состояние; история и возврат. Устойчивый pending, сохранение черновика, отсутствие повторного согласия без человека, desktop/mobile/keyboard/reconnect. Классический UI не импортируется.
4. **C2-D — общий gate.** Перекрёстный аудит модели, реального runtime, интерфейса, storage reader и immutable connector bundle; затем checkpoint. Внешние DNS/TLS, настоящий Apps4 candidate/fallback и backup restore остаются обязательными условиями выпуска.

Подзадачи параллельны только внутри одного этапа и не редактируют общие файлы. Root фиксирует переход после тестов и независимого review. Не больше трёх субагентов одновременно.

## Авторитетные данные

- Старые поля `local_apps.connector_key/port/entry_path` сохраняют историческую initial tuple. Рабочий источник определяется единственным `app_publications.active_target_revision` и соответствующим `app_runtime_targets`.
- Apps4 добавляет `app_source_heads` и отдельные bounded `app_source_receipts`. Минимальная версия привязки 1 у нетронутых приложений, 2 после первого переключения; возврат к target1 не понижает её. Missing/invalid head в уже существующей Apps4 — ошибка данных, не повод автоматически вставить 1.
- Точные исторические DDL сохраняются. Исторический Apps3 распознаётся независимо, с корректной единственной initial tuple. Ручной noninitial target в Apps3 не мигрируется молча. SQL guards запрещают изменение/замену immutable target и понижение/удаление binding floor, включая `INSERT OR REPLACE` при стандартных настройках SQLite.
- История возвращается ограниченными страницами; immutable targets не удаляются ради лимита receipts. Последние64 source receipts каждого приложения сохраняются в одной транзакции с эффектом. Лимит истории не блокирует emergency restrict/revoke.
- Занятость `(current connector, port)` проверяется внутри общей write transaction при регистрации, переключении и возврате. Освобождённый исторический порт можно зарегистрировать заново; занятый другой аппкой прежний источник не захватывается при rollback.

## Prepare и promote

`apps.source.prepare`: `appId`, `expectedPolicyEpoch`, `expectedTargetRevision` текущего приложения и ровно один вариант — `source:{hostDeviceId,connectorId,port,entryPath}` либо `targetRevision` из собственной истории. Произвольные URL, shell-команды, чужие устройства и недопустимые пути не принимаются.

Подготовка не пишет target, publication или route. Ограничения включают незавершённые проверки:256 всего,4 на аккаунт,2 на приложение;4 сетевые проверки на connector без бесконечной очереди. Срок подготовки30s от начала, deadline ACK5s, HEAD3s. После await заново проверяются действующий владелец, устройство, точные версии и тот же socket. Данные callback не попадают в публичный DTO целиком.

Подписанный Connect транспорт различает синхронные extensions и `executeAsync`: Promise из синхронного `execute` намеренно запрещён. Поэтому сетевой prepare получает отдельный async adapter, который завершает транзакцию проверки подписи до ожидания устройства. Остальные Apps-команды сохраняют свой синхронный контракт. Прямой unit-вызов registry не заменяет приёмку prepare через настоящий подписанный HTTP/Connect путь.

`apps.source.promote`: новый `requestId`, точный `preparationId`, прежние `expectedPolicyEpoch/expectedTargetRevision`, явные `launchPolicy/listed`, при anyone — новый `exposureAck` с whole-port/revision/digest/profile проверенного кандидата. Выбранные aliases и grants сохраняются, их конкурентная смена участвует в epoch. В restricted listing выключен; публичное размещение требует действующего адреса.

Порядок внутри короткой синхронной `BEGIN IMMEDIATE`: текущий owner → нормализованный intent и retained receipt → точный CAS → непросроченный подготовленный target и текущий runtime proof → проверка занятого источника/согласия → immutable insert либо существующая историческая запись → один active pointer/новый epoch/floor2 → receipt/prune → COMMIT. Сетевого await внутри транзакции нет.

Receipt replay выполняется **до** обращения к ephemeral preparation: принятую команду можно восстановить после перезапуска или истечения проверки. Receipt описывает исторический результат, `current` — новое чтение. После удаления старого receipt исходный CAS не перебазируется автоматически. Отсутствие receipt не называется доказательством отсутствия эффекта. Ошибка уведомления после COMMIT не превращается в фиктивный rollback; повтор сохранённой команды может повторить безопасное согласование маршрута, сохранив тот же результат.

## Runtime v2

Подробный wire contract — [отдельный документ](p3-source-wire-contract.md). `soty.apps-channel.v1` сохраняет additive capability negotiation, но v2 использует отдельные типы сообщений, которые нельзя принять за старые sync/open. Выбранный режим и channelId закреплены за конкретным socket.

Каждый app имеет отдельный binding-set/ACK с target revision/digest/profile и новым syncId при изменении tuple или reconnect. Новый connector пересчитывает digest из точных данных, закрывает прежние streams этого app и подтверждает принятую конфигурацию. Unrelated sync не пересоздаёт неизменившиеся bindings. Каждое сообщение укладывается в72KiB UTF-8; до100 отдельных bindings не превращаются в одну огромную конфигурацию с длинными entry paths.

До локального `httpRequest` connector проверяет exact channelId/syncId/appId/revision/digest/profile; только затем выбирается порт из подтверждённой tuple. Пока ACK нет, сервер возвращает отдельное состояние ожидания, не копит HTTP и не переходит к v1. Head2 никогда не попадает в legacy sync/open. Старые async handlers, HEAD и ответы не могут отправиться в новый socket через общий `state.socket`.

Transient target-prepare не меняет active map. Ответ200–499 означает только «процесс отвечает», включая401/404. ACK установленной конфигурации и наблюдение ответа — разные доказательства. Они не подтверждают неизменность исходного кода, качество приложения или внешний DNS/TLS.

## Обязательная приёмка

1. Genuine historical Apps3 →4, включая WAL; старый runtime/reader/START receipt не начинает несовместимую запись. Unknown/corrupt не исправляются автоматически.
2. Ошибочный prepare, v1 connector, чужой источник, timeout или restart не меняют epoch/aliases/target rows; лимиты не блокируют отзыв доступа.
3. Старый socket/nonce/ACK/HEAD и потерянные полномочия не подтверждают новый источник.
4. HTTP/WS с двумя реальными портами: старый open идёт только к прежней точной binding или отменяется, никогда к новой по одному appId. Reconnect требует fresh ACK.
5. Новое публичное согласие обязательно, в том числе для rollback; reused/wrong tuple отклоняется. Grants/адреса не теряются, новый claim сам не активируется.
6. Два настоящих SQLite writers: один winner при одинаковом CAS, без orphan target/полурезультата. Grants/revoke/active-alias retire конфликтуют; независимое name-only изменение не мешает.
7. До COMMIT/после COMMIT до ACK, reopen/offline/retry/prune: эффект не дублируется, receipt отделён от current, неизвестный исход не скрывается.
8. Rollback требует fresh proof/epoch/floor2 и не изменяет пользовательские данные; занятый прежний порт сохраняет нынешний маршрут. Register/revoke следуют active target.
9. UI: неизвестное/истёкшее состояние не называется готовым; Back/Escape, два окна, обычный draft, durable pending и смена аккаунта безопасны. Проверки браузера отделены от VM/API.

## Пределы выпуска

Чтение Apps4 не означает поддержку протокола v2. Ранний C2-A reader отказывает head2; он не считается рабочим fallback после начала source switches. До такого выпуска нужен отдельный проверенный Apps4/v2 candidate и fallback. Старый уже запущенный Apps3 процесс сам не остановится от новой schema: существующий sole-writer stop gate до миграции обязателен. Здесь не заявлена защита от произвольного внешнего процесса, игнорирующего deployment guard.

## Исследования → решение

[SQLite isolation](https://www.sqlite.org/isolation.html) описывает сериализацию writers и неизменяемый snapshot чтения. Отсюда короткая общая write transaction для pointer/epoch/receipt, повторный CAS после сетевой проверки и отсутствие долгого await внутри SQL.

[SQLite transactions](https://www.sqlite.org/lang_transaction.html) уточняет границы read/write transaction и конкурирующих writers. Успех preflight вне транзакции не принимается за право последующей записи.

[RFC6455](https://www.rfc-editor.org/rfc/rfc6455.html) задаёт транспорт WebSocket. Связывание приложения с версией источника является нашим проверяемым прикладным контрактом, а не свойством самого WebSocket или его handshake.

[WHATWG URL](https://url.spec.whatwg.org/#url-serializing) и [Node24.13.1 URL API](https://nodejs.org/download/release/v24.13.1/docs/api/url.html#class-url) задают общий разбор и сериализацию URL. После проверки допустимости пути HEAD использует `new URL(originalEntryPath, loopbackOrigin).pathname + .search`; исходная строка остаётся в digest. Это решение дополнительно проверено пятью настоящими loopback-запросами: Unicode, dot-segments, encoded query и hash-SPA совпали с WHATWG fetch, опасные нормализованные пути отказали. Это не утверждение о прохождении всех браузеров.

Сериализованный `pathname + search` повторно проходит общий `runtimePath` до HEAD: Unicode может увеличить путь после percent-encoding за предел8192. Такой кандидат не подтверждается как доступный для relay; сохранённая исходная строка и её digest не меняются.
