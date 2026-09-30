# P4-B2 — независимая HTTP и composition приёмка

30.09.2026. Проверены root-owned `capabilities-actions`, host composition, recovery scheduler и связанный OpenAPI после [domain review](p4-native-effect-independent.md). Собственный узкий wire gate: **2/2 PASS,0 FAIL,0 SKIP,1132.8064ms**, Node24.21.0/SQLite3.53.4. В проверенной области новых блокеров не найдено. Production и авторские tests не менялись.

Это два новых наблюдаемых сценария, не повтор всех root15 cases. Fixture собственная: настоящий `createHttpApp`, реальный loopback Node HTTP с keep-alive Agent, подписанный Connect bootstrap/access RPC, отдельные actual SQLite stores, реальные opaque credentials. Авторский support helper не импортируется. Тестовые миграции2 явно выполняются до запуска host; из этого не следует автоматическая migration production.

## Notes COMMIT без Caps receipt: неизвестность, revoke и recovery

В единственной synthetic Caps DB временный `BEFORE INSERT ON cap_receipts` trigger возвращает SQLite ABORT. Это намеренная fault injection, не естественное повреждение и не kill-process опыт. Реальный Notes handler до этой точки успевает закоммитить документ и permanent create proof; Caps completion transaction откатывается. Входной POST, подпись и обработчик не заменены.

Наблюдения:

1. POST возвращает **202**, `status:'accepted'`, `effectState:'unknown'`, `reused:false` и canonical `Location`. Нет success `result`, receipt, SQL или текста injected exception. Actual Notes title/body побайтно в смысле JS string совпадают с отправленными, proof1; Caps receipt0, reserved1/spent0.
2. GET с `If-None-Match:*` возвращает **200** с той же content-free unknown projection. Он не превращается в304 и не вызывает reconcile/execute: Caps receipt всё ещё0.
3. Подписанный owner отзывает **исходную credential**, сохраняя сам grant. GET с ней возвращает401 и только fixed error; Invocation ID и receipt не раскрываются.
4. После удаления тестового trigger один управляемый timer turn вызывает настоящий `reconcilePage`. Scheduler завершает подтверждённый эффект: checked1/committed1, unavailable=false. Дополнительный execute port не вызывается. Старое удостоверение по-прежнему получает401.
5. Owner выпускает новую credential того же grant. Она законно читает historical success и исходный note artifact revision1; повтор исходного POST/key возвращает reused200. Note/proof остаются единственными, reserved0/spent1, input purged.

Таким образом, swallowed execution exception не выдаёт false failure и не подменяет последующее current-authority read. Positive proof после revoke не превращает внутренний recovery в внешний read permission. Точность callback scheduling контролируется fixture, а не реальным ожиданием30s; доменные транзакции и HTTP реальные.

## Ранний отказ и следующий запрос

Второй сценарий оставляет незавершённый POST с объявленным Content-Length1000000, отправив только JSON prefix и не прислав Authorization. Реальный сервер завершает401 с `Connection:close`, не ждёт оставшуюся загрузку и не создаёт ledger row. Следующий авторизованный POST через **тот же Agent** проходит201; fixture не подменяет проверку `agent:false` и не дописывает тело первого запроса для завершения ответа.

`X-Forwarded-Host:untrusted.invalid` и `X-Forwarded-Proto:https` не меняют authority или URLs: `Location` и Notes deeplink используют только configured origin. HEAD на own history даёт405/Allow:GET/пустое тело. Следующий conditional GET остаётся200 с неизменённой projection; повторной Notes нет.

На actual replies проверены `Cache-Control:no-store`, `nosniff`, `no-referrer`, отсутствие ETag и wildcard CORS, размер≤65536. Private title/body и bearer не входят в ответ. Количество принятых input bytes не смешивается с разрешённым доменным размером документа.

## Source review

- Audience проверяется до открытия storage и происходит из trusted shell origins. Private router использует raw exact target, один Host, optional exact Origin, ровно один bearer, отдельный method. Проксированные host/proto headers не являются authority. Private routes подключены до discovery namespace fallback.
- Body читается без Connect/Caps lock. После чтения admission заново проверяет live authority; после внутреннего результата или исключения выполняется отдельный authorized `get`. На ошибке недочитанный request получает закрытие соединения без неограниченного drain. Ingress slot освобождается в `finally`.
- HTTP output создаётся из явного набора полей; неизвестные internal exception/code сводятся к fixed error через class и allowlist. Success URL строится только при согласованном единственном note artifact/effect revision1. Current Notes content/existence не читаются для исторического ответа.
- `startNativeRecovery` вызывает только `reconcilePage`, максимум16 элементов за turn, один unreferenced timer. Продолжение страницы2s, новый sweep30s; ошибки дают bounded4…60s backoff с сохранённым cursor. Per-item `native_reconciliation_failed` не тормозит следующий item и не выдаётся за отрицательный effect. Close запрещает новые turns до закрытия DB. Возвращённый Promise отклоняется как нарушение sync contract, rejection наблюдается.
- OpenAPI использует общие route/ID constants, две private операции с bearer security; standalone public discovery остаётся отдельным документом. Описаны UTF-16 и byte limits, exact three-string body, 202 uncertainty, own-history authorization, historical artifact и отсутствие гарантии существования Notes. JSON Schema/полный OpenAPI conformance oracle в этом независимом прогоне не запускался; root15 включая его oracle атрибутируются root.

Проверки raw2MiB/15s/8readers/2peer/60POST/2048peer и прежнего Buffer-list memory defect имеют отдельную [ingress квитанцию](p4-capabilities-ingress-independent.md). Здесь они не выданы за новый исчерпывающий нагрузочный прогон. По сообщению root его focused HTTP/OpenAPI/recovery suite:15/15 PASS,6593.9301ms; этот результат не складывается с моими двумя как один запуск.

## Команда и freeze

```powershell
$independentHttpNode = Join-Path (Get-Location).Path 'var/toolchains/node-v24.21.0-win-x64'
$env:PATH = $independentHttpNode + [IO.Path]::PathSeparator + $env:PATH
& (Join-Path $independentHttpNode 'node.exe') --test --test-concurrency=1 server/test/capabilities-actions.independent.test.mjs
```

| Проверенный файл | SHA256 рабочих bytes |
| --- | --- |
| `server/capabilities-actions.js` | `19abdaa5dbbc4e402a4a478f90f2f9da775b6028bd2db9dcfb01c7b199bd8dc6` |
| `server/capabilities-http-contract.js` | `a6574f7a6c2111bfcd1cf6c2794aa69b536248cc38556c97dd809b45843e8228` |
| `server/capabilities-openapi.js` | `cc6ee8575e545a8b0d3b488e39d15e789c1da3a5ab1ebcaf5c90f6a431432af0` |
| `server/capabilities-recovery.js` | `fc430ef10500690d6551e5f6564759e7134913a85ac5c59f5bee4810683a758b` |
| `server/capabilities-ingress.js` | `7619c9eeb217641f9bf6ae6d386f1658de647e6dce161fae4c8a234ffbb72c19` |
| `server/http-app.js` | `bd3cdc134e35d8db421899fe44e1263479ac9e35c151fb0b338260e2d861e400` |
| `server/test/capabilities-actions.independent.test.mjs` | `caf8671432281b37e09f64cd2cdec0bd731a2b399e674f9a4a6936627e00fcd3` |

Все stores находятся в новом `soty-native-http-independent-*` immediate child canonical temp parent; cleanup проверяет prefix, canonical path и nonce marker. Ранее запрещённые directories не затрагиваются. Credentials/keys остаются в памяти и не записываются в отчёт. Это localhost synthetic client, не external-agent/OAuth/MCP, browser/PWA или Linux/release/restore gate. После двух тестов слот возвращён root; больших suites reviewer не запускал.
