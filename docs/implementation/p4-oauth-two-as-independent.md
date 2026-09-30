# P4-C1 — независимый review двух настоящих AS процессов

01.10.2026. **Блокеров в проверенном test-only slice не найдено.** Полностью прочитаны финальные три файла, [авторская квитанция](p4-oauth-multiprocess.md) и два итоговых execution logs. Это независимый source review авторского исполнения, не второй запуск тестов. Reviewer не менял production/test source, не запускал suite, CLI login или remote. Проверки относятся к принятому production `6fd01a7ec64c644c9aff935bd65652cfb6cd6461`; новая root metadata delta находится за границей этих результатов.

## Почему это meaningful process evidence

Два worker действительно запускаются разными OS processes с проверкой разных PID. Они открывают одни и те же Connect/Notes/Capabilities SQLite файлы, один canonical issuer и одинаковые artifact/cookie/JWK keys. Подготовка отдельно и явно создаёт Notes2/Caps3; serving constructors затем открывают их без migration flags. Account bootstrap и каждое owner approval проходят реальные HTTP challenge/signature операции Connect, а actual domain readiness требуется, а не заменяется `true`.

Worker использует production profile, Provider/router, actions, Connect и domain services. Forwarding facade не содержит второго SQL adapter: сначала вызывает настоящий синхронный artifact port и возвращает именно его результат. Barrier находится **после** этого вызова и блокирует отдельную pipe, не production Promise. В выбранных точках parent через другие SQLite handles успешно делает `BEGIN IMMEDIATE`/`ROLLBACK` на Connect и Caps. Значит, test interleave не создан ожиданием внутри удерживаемой authority transaction. Notes transaction в этих token/Grant точках не выполняется.

Front proxy выбирает child по закрытой таблице fixture TCP socket ports; Host/issuer остаются общими. Он не подставляет account, bearer или authority, не использует публичный routing header. Cookie client — тестовый HTTP клиент: он сохраняет реальные Set-Cookie и проходит authorize → signed approval → completion → Provider resume → code → token. Code/AT/RT не засеиваются SQL и не придумываются fixture.

## Наблюдения, поддержанные конкретными assertions

| Сценарий | Сильное утверждение и его граница |
|---|---|
| Два читателя одного code | Обе операции `find` читают unused source до первого consume. A действительно commits consume, B commits reuse revoke, затем поздний AT upsert A отказывает. Exact-source operation events подтверждают один успешный consume, а не случайную сумму соседних выдач. У этой family0credentials/0Notes; после reopen revoke сохранён, sibling создаёт свой Note. |
| Два читателя одного RT | A успевает получить HTTP200 с новым AT/RT; новый AT проходит обычный bearer gate. Более поздний B reuse отзывает и этот уже доставленный AT. После reopen credential IDs/createdAt/expiresAt неизменны, право отозвано, sibling работает. Тест не обещает постоянно работоспособного победителя гонки. |
| Crash после Grant/link | Процесс убивается после return настоящего Grant upsert, до успешного completion response. SQL уже показывает provider link. Другой AS завершает ту же signed interaction без нового client/principal/root/Grant/budget; проверки сравнивают authority IDs и единственность строк. Полученный AT выполняет настоящий Note create. Это не новый consent и не восстановление прав из cookie. |
| Пять token crash windows | Проверены разные фактические порядки библиотеки: code consume → AT → RT; refresh consume → successor RT → AT. После каждого выбранного COMMIT есть SIGKILL до token response, новый PID и reopen. Counts artifacts и joined credential/link rows соответствуют достигнутой границе; retained credential IDs/absolute expiries не меняются. Retry consumed source даёт `invalid_grant` и durable family revoke. Сам crash не выдаётся за rollback или revoke. |
| Native effect | Во всех пяти token crash cases остаются0Invocation/0Note. Token uncertainty не запускает действие автоматически. В race/Grant cases только явно отправленный HTTP create порождает проверенный Note proof. |

AT crash cases дополнительно проверяют зафиксированный AT через обычный production bearer endpoint до и после reuse revoke. Его значение тест получил по private IPC из реального upsert. Это позволяет доказать существование права при потерянном HTTP response, **но не даёт клиенту способ вернуть потерянный token**. Ответ proxy502/transport loss обозначен fixture fault; он не называется OAuth ответом.

Последняя oracle correction необходима и достаточна для выявленного пробела: HTTP response и private event приходят по разным pipes. Теперь consume events фильтруются по digest именно проверяемого code/RT, и test ожидает соответствующее событие до подсчёта. Это убирает влияние sibling setup и порядка доставки; HTTP/durable-state assertions сохранены. Late AT refusal относится к единственному в этот момент незавершённому A exchange; sibling issuance уже закончена до arm.

## Изоляция и пределы

Fixture сохраняет keys/cookies/tokens только в памяти и IPC. Объём event buffers/количество событий ограничены; parent работает с safe error codes, не пишет raw tokens в logs. Child stderr намеренно discard-ится: этот gate не является доказательством отсутствия console diagnostics. HTTP buffer1MiB, command/event timeout10s, HTTP15s, case30s; fault termination направлена только на собственный child. Teardown ждёт `close` и проверяет realpath, parent, prefix и ownership marker до удаления temp directory.

Покрыта локальная Windows Node24.21.0/SQLite3.53.4 композиция с реальными двумя процессами и одним owner account, но двумя независимыми connections. Она не является Linux crash test, проверкой нескольких human accounts, полного `createHttpApp`/World/Apps, public edge/TLS/proxy, native browser/CSP или фактического CLI login. Consent HTML загружается, но не исполняется. Browser cookie/SameSite enforcement не воспроизводится ручным jar.

Тесты используют известные fixture paths authorize/token; discovery-follow compatibility ими не доказана. Отдельный root metadata bug/fix и его HTTP gate не следует объявлять закрытыми этим файлом. Живой процесс остаётся источником authority через Connect/Caps, а session/cookie/profile/client_id сами по себе не становятся attestation владельца или официальности клиента.

## Exact evidence и freeze

| Файл | Bytes | SHA256 |
|---|---:|---|
| `server/test/capabilities-oauth-multiprocess.test.mjs` | 10026 | `e3957ec5fa1f004d0de8d41c74918aa48f7552114567c227e5cf3585fbc0e5b6` |
| `server/test/support/oauth-as-worker.mjs` | 6418 | `f7c161bc5d91e0cb3d4485d74a0a57e6ae64783417d96222ba294135c4938a74` |
| `server/test/support/oauth-as-processes.mjs` | 19794 | `db81a66dfe39c53b2c3e3f2dc43a197466f5a587ebe8fe54f35f69843af6cc7b` |

Авторская квитанция `p4-oauth-multiprocess.md`: SHA256 `28b6b7b5595f086684c792ee52eb0002389086175c4d081afca4e297d3846dd7`.

Прочитанные авторские результаты не объединяются в вымышленный один прогон:

- `output/implementation-20260930/p4-oauth-two-as-first.log`: **8/8 PASS,0fail,0skip,16033.1878ms**, до последней event-correlation delta. SHA256 `e7a49e22e10cdd2c9818c9bcbefbfd84260157b09c0a22e10c3f412c61e4dc0f`.
- `output/implementation-20260930/p4-oauth-two-as-races-final.log`: **2/2 PASS,0fail,0skip,4279.2556ms**, обе затронутые race cases на final snapshot. SHA256 `5fa26e43d083a4dd161954da6aa4f03ab5d4376d7bb6434c6ab11448117fc7d5`.

Остальные шесть scenario bodies и production не изменены этой коррекцией; их evidence относится к предшествующему full-file run. Первый fixture RED, ошибочно считавший sibling code consume, не выдаётся за product defect. Нового независимого executable test ради дублирования этих восьми сценариев не потребовалось. Этот review закрывает выбранный concurrent/crash slice, не весь P4 и не пользовательскую готовность OAuth/MCP.
