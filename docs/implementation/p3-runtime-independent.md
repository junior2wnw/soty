# P3-B2 — независимая приёмка runtime

Дата: 2026-09-30. Основание: [подплан транспорта](p3-runtime-transport-plan.md) и [контракт публикации](p3-publication-contract.md).

**Вердикт: PASS в согласованных локальных границах B2.** В независимом наборе 30/30 проверок; дополнительно повторены 12/12 авторских и прежних интеграционных проверок с настоящим connector runtime. Найденный дефект повторного HTTP-соединения исправлен автором и закрыт двумя независимыми регрессиями. Открытых material blockers в проверенном B2 transport scope не осталось. Это не приёмка B3, публичного развёртывания или всех браузеров.

Независимая зона записи: `modules/apps/test/app-runtime.acceptance.test.mjs` и этот документ. Реализация транспорта не изменялась независимым проверяющим.

## Что действительно исполнялось

- Настоящий `createAppsService`, временная SQLite, реальные loopback HTTP и WebSocket соединения, включая отдельное WS-соединение connector channel. Приложение, устройство, адреса и публикация создаются через реальные операции сервиса; прямых INSERT в registry нет.
- Управляемый connector — отдельный протокольный peer, а не production connector. Он позволяет задержать ACK, head или end и отправить кадр точно после изменения прав. Успешная базовая передача проверяет хеш реальных байтов POST/ответа и WS-сообщение.
- Внешнее изменение policy выполняет второй настоящий экземпляр сервиса с отдельным SQLite connection. У него нет локального membership event первого экземпляра.
- Deadline/lease проверены через предусмотренный factory `now`. Это доказательство сравнения и обновления сроков на границе, а не ожидание реального часа. Настоящие 30-секундные head/ACK timers выполнены без подмены часов транспорта; тест ограничен 40 секундами.
- Для проверки после `await write` настоящая запись в socket выполняется, затем удерживается только вызов её callback. После отзыва доступа callback отпускается. Это scheduler fault, проверяющий запрет позднего ACK; он не измеряет физический backpressure или RAM.
- В отдельном повторе авторских тестов используются настоящий `createLocalAppsRuntime` и loopback-приложение, включая assets, POST, WS и интеграцию CSP оболочки.

## Приёмочная матрица

| Группа | Статус и наблюдаемый результат |
| --- | --- |
| Точный адрес и публикация | PASS: активный public/unlisted alias работает без аккаунта; canonical остаётся private; новый claim не наследует публикацию; inactive503, tombstone410, unknown/wrong-port404. |
| Cookie и аудитория | PASS: пустая, неизвестная, повторенная и cookie другого alias не превращаются в guest; отозванный actor теряет прежний допуск. Новый отдельный запрос без cookie может получить public-допуск. |
| Ticket и локальный path | PASS: один успех при параллельном обмене; чужой app/alias, external/reserved path и срок ровно30s отклонены. Delayed body повторно проверяет grants, actor, policy и retire; cookie при отказе не создаётся. |
| Origin | PASS: unsafe POST/PUT/PATCH/DELETE и WS без точного Origin отклонены до connector open, включая null/foreign/duplicate. GET/HEAD без Origin работают. Это браузерная граница, не удостоверение внешнего агента. |
| Private direct entry и восстановление cookie | DEFERRED B3: новый trusted-shell redirect, boot verify/recovery, поведение iframe и действие «Открыть отдельно» этим набором не принимаются. |
| Сроки и текущие права | PASS: account session имеет абсолютный1h, verify не продлевает её и не ставит cookie. Anonymous lease обновляется до истечения, но не оживает на границе30s. Истёкший account stream закрывается, более новый сосед продолжает работать. Внешний SQLite epoch закрывает idle WS через настроенный25ms аудит; тест ждёт не более2.5s, не обещает жёсткий wall-clock SLA event loop. |
| Async boundaries | PASS: после ожидания тела ticket, ACK request chunk и callback реальной записи права проверяются заново; late head/data/end/ACK после внешнего epoch не продолжают поток. Незавершённый200 не выдаётся за полный результат; поздние байты не подтверждаются. Уже доставленные байты отозвать невозможно. |
| Изоляция отказов | PASS: revoke/expiry/source error, запрещённый внешний redirect и informational-only HTTP head не обрывают общий connector и соседние допустимые streams. Проверен следующий успешный запрос. |
| Ёмкость и освобождение | PASS: public24 входят в общий32; signed-public также расходует public quota и после получения membership остаётся на прежнем основании. Grant-basis использует оставшиеся8. HTTP и WS делят одну ёмкость; client disconnect, source failure и настоящие head/ACK timeouts освобождают только свои места; повторное заполнение снова даёт429. |
| Relay profile и регрессии | PASS: Cookie/Authorization и недопустимые forwarded/response headers не передаются; внешнее Location отклонено, локальное307 сохранено; source error не раскрывает синтетические owner/device/port. Named-only policy не требует legacy origin. Дополнительные12 интеграций подтвердили существующие HTTP/assets/WS/limits/ownership и actual shell CSP. |

## Исправленный дефект

При отзыве membership во время ожидания ACK загрузки сервер заканчивал отказ, оставляя соединение объявленным keep-alive. Выход из асинхронного чтения незавершённого body разрушал IncomingMessage. Следующий GET использовал тот же socket (`reusedSocket=true`), зависал и не достигал server request handler. Тот же GET на новой TCP connection отвечал200; общий connector оставался исправным.

Автор изменил `respondFailure`: до раннего ответа при `!req.readableEnded` отключается keep-alive и передаётся `Connection: close`. Проверять только `req.complete` недостаточно: parser мог получить маленький body, когда приложение ещё ждёт его ACK. Независимые случаи16 и147456 байт сохраняют обычный HTTP Agent; принудительное новое соединение не используется как обход. Оба случая и следующий GET проходят.

Первоначальный400 в отрицательном Origin-случае не был дефектом продукта: тестовый DELETE отправлял body без явного Content-Length. Стенд исправлен; ожидаемые403 и отсутствие connector open проверены на корректно оформленных HTTP-запросах.

## Повторяемые команды и срез

```text
node --test modules/apps/test/app-runtime.acceptance.test.mjs
30 tests; 30 pass; 0 fail; 0 skip; duration 33.34s

node --test modules/apps/test/app-runtime.test.mjs modules/apps/test/apps.test.mjs
12 tests; 12 pass; 0 fail; 0 skip

node --check modules/apps/test/app-runtime.acceptance.test.mjs
git diff --check
```

Проверки syntax/diff завершились без ошибок. Базовый HEAD во время проверки: `3ddb7b6`; B2-изменения ещё не были закоммичены. SHA-256 проверенного среза:

| Файл | SHA-256 |
| --- | --- |
| `modules/apps/server/index.mjs` | `83a35eca1f13354aff398dc70ac8aed047a582867ec1eddd72222911ba2bdf1d` |
| `modules/apps/test/app-runtime.acceptance.test.mjs` | `bcb69806580e0c32dcb9317cd7bc3487b0d7e9add476697be5a89fc05ff1fece` |

## Границы вывода

- HTTPS origins в независимом стенде проверяются как exact Host/policy metadata поверх локального HTTP. Настоящие DNS, TLS-сертификаты, Linux image, backup/restore и approved public configuration не проверялись и остаются внешними release gates.
- Заголовки cookie проверены на wire; приём CHIPS/Partitioned браузером, third-party restrictions и поведение установленной PWA этим тестом не доказываются. Они относятся к B3 и браузерной приёмке.
- Отсутствующая cookie не позволяет отличить нового посетителя от браузера, удалившего старую. Запрет guest fallback относится к предъявленной cookie и уже созданному потоку.
- Runtime использует выбранный `RuntimeTarget` через branded decision; target/epoch pinning опирается также на уже принятую B1 модель. Новый сценарий смены runtime target и UI публикации относится к последующим подэтапам. Исторический connector v1 не подтверждает revision ACK, неизменный исходный код или откат внешних эффектов.
- Измерение массовой нагрузки, RSS телефона, физического backpressure и всех сетевых отказов не выполнялось. Исходный capped relay profile остаётся ограничением продукта, а отказ после уже переданных байтов не означает rollback или безопасный автоматический повтор unsafe HTTP.
