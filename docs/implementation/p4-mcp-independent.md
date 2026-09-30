# P4-C2a: independent MCP transport review

Независимая квитанция targeted gate. Проверяются новый MCP transport и изменённая HTTP composition, а не весь P4 или совместимость любых внешних ИИ. Independent test ownership: `server/test/capabilities-mcp-independent.test.mjs`; production, author tests и PROGRESS рецензент не меняет. Первые шесть cases независимо прошли 6/6; дополнительный седьмой case отдельно воспроизвёл early-body defect и после исправления прошёл неизменённым в авторском запуске. Это разные запуски, не единый 7/7. Открытых блокеров в рассмотренном C2a diff не осталось.

## Прочитанные границы

Прочитаны C2 plan, установленные public SDK server/core/client2.2.0 и node2.1.0, MCP transport/ingress/tools, shared `createCapabilityOperations`, actual access/native coordinator и public discovery facade. Ни один из этих read-only разборов не подменяет результаты wire tests ниже.

- SDK legacy `fetch()` возвращает поток до его полного чтения; `createMcpHandler.close()` отслеживает modern instances. Host обязан читать конечный body под cap, затем заново проверять current external read до первого private byte и самостоятельно закрывать legacy exchange.
- `parsedBody` обходит SDK input reader; host byte/UTF8/duplicate-key/depth guards нужны до передачи parsed object. Legacy fallback не наследует ограничение revisions из Server constructor; explicit host revision admission необходим.
- Shared Notes port сохраняет прежний admit/reconcile/execute/fresh-get порядок. Историческая ссылка строится от H, audience проверяется отдельно как H или H/mcp. Request metadata не становится identity; actor остаётся в закрытом WeakMap домена.
- `service.catalog` уже является public-only facade. Catalog errors требуют отдельного безопасного mapping; Notes-oriented HTTP mapper недостаточен. Immutable Notes @1 validator/digest не заменяется AJV semantics.
- Private/readiness/original-credential/Notes proof проверки принадлежат прежнему домену. Новые wire проверки должны доказать, что SDK awaits, streaming и mount order их не обходят.

## Причинные находки

### Slot lifetime

Первый WIP ingress вызывал `release()` из `res.close/finish`, пока SDK/body/cleanup могли ещё выполняться. Это нарушало заявленную границу admitted work, даже при закрытом клиентском socket. Автор принял source finding: socket close теперь только abort, release находится во внешнем finally после cleanup. Авторский held actual `Server.close` regression отдельно показывает 429 до завершения cleanup. Здесь этот результат атрибутирован автору, не заявлен как независимый повтор.

### SDK header diagnostic reflection

Независимый actual-wire RED: `p4-mcp-independent-noecho-red.log`, **0/1, 1158.9965 ms**, Node24.21.0, concurrency1. Четыре запроса использовали действующий service bearer, выданный настоящим signed Connect flow:

| Вариант | HTTP / code | Header canary reflected | ClientInfo canary reflected |
|---|---|---|---|
| Mcp-Method mismatch | 400 / -32020 | true | false |
| Mcp-Name mismatch | 400 / -32020 | true | false |
| Malformed Base64 Mcp-Name sentinel | 400 / -32020 | true | false |
| Valid tools/list with client metadata | 200 | false | false |

Новых Invocation и Notes было 0. Canary — искусственные строки; вывод сохраняет только case/status/code/bytes/boolean, не raw response, bearer или canary values. RED log SHA256: `cc8ef86a2b7394698fd223ee53ffb810c2f2d7f16525725b370130813070d374`.

Причина подтверждена установленным SDK: serving ladder включает raw header в `error.message` и `data.mismatch`. Narrow author repair использует public `deserializeMessage/serializeMessage` только для конечного JSON HTTP error, сохраняет известный protocol code, выдаёт фиксированное сообщение и проверенный closed data shape. Success/SSE не перекодируются. Получен author-attributed GREEN неизменённых no-echo assertions: **1/1, 1187.8627 ms**, `p4-mcp-noecho-green.log`. Затем этот же case независимо прошёл в полном6/6: все три ошибки400/-32020 стали75B, оба canary-reflection boolean=false.

## Выполненная независимая проверка

Всего шесть cases: no-echo; настоящий SDK Client2.2 modern; настоящий SDK Client2.2 legacy; public catalog/escaping/noninterference; уменьшенный output cap; actual Express trusted-proxy predicate.

Оба Client cases используют установленный `Client` и `StreamableHTTPClientTransport`, действующий signed Connect owner/service credential, реальные stores и Notes effect. Наблюдающий fetch не заменяет ответы и сохраняет только method/revision/status/content-type/session-presence. Проверяется штатная negotiation, tools/list, create/get, advertised output validation и text-only domain conflict. Это тест SDK2.2 на legacy revision, не actual OpenCode/SDK1.29 binary proof.

Catalog cases подменяют только public catalog property service facade, используя настоящие `createCatalog/createPublicDiscovery`; auth, WeakMap actor, Connect fences и native ports остаются настоящими. Fixture достигает **262144B raw registry declaration** и **16384B generated documentation budget**, с quotes/backslashes. Это фактические source budgets, а не заявление о достижимости математического384KiB detail maximum. Проверяются final encoded JSON/SSE bytes и полная неизменность public transcript/cursor при private-only additions.

Lower-output-bound case допускает только typed413 либо actual socket close до headers/body; независимый deadline не считается успешным отказом. Positive tools/list до и после запроса доказывает, что endpoint и capacity остались работоспособны. Это trusted test limit, production2MiB не увеличивается.

HTTPS case работает поверх собственного loopback HTTP listener: настоящий Express `trust proxy=false` отвергает spoofed Forwarded/XFP, explicit loopback trust допускает configured proxy. Wrong Host и wrong audience остаются отказом. Это oracle host trust predicate, не проверка реального TLS termination/certificate/deployment.

Из workspace, с command-local PATH на `var/toolchains/node-v24.21.0-win-x64`:

```text
node --test --test-concurrency=1 server/test/capabilities-mcp-independent.test.mjs
```

**6/6 PASS, 0 FAIL, 0 skip, 2976.126 ms**. Log: `output/implementation-20260930/p4-mcp-independent-first.log`, SHA256 `2963d32b53c77c9d5ce39a877f903f2da303a7510095ee9a282e8c4f033649ab`.

| Наблюдение | Фактический результат |
|---|---|
| Client2.2 / modern2026-07-28 | server/discover,5HTTP exchanges,3tool calls,0GET attempts;1Invocation/1Note/spent1 |
| Client2.2 / legacy2025-11-25 | initialize,7HTTP exchanges,3tool calls,1optional GET attempt;1Invocation/1Note/spent1; client protocol error count0 |
| Public detail |279008B; actual wire835046B modern /834951B legacy |
| Private additions |same full public transcript, revision/continuation and final encoded sizes; private/unknown both not_found;0effects |
| Output limit65536B |socket closed before headers/body,0response bytes; following tools/list200 |
| HTTPS predicate |untrusted proxy403, explicit loopback trust200; Host substitution400; wrong audience401 after trusted transport |

Стандартное SDK предупреждение про JSON mode присутствует. Отчёт не утверждает console-clean. Во всех тестах временные stores удалялись только собственным shared fixture cleanup с проверкой absolute parent/path/owner marker; secrets в отчёт/лог не попадали.

## Source inventory для этого6/6

| Файл | SHA256 |
|---|---|
| server/capabilities-mcp.js |a67e1630b74cd7886196f9bf7bc25ce0c69f7706449b065d6d8c353e17285236|
| server/capabilities-mcp-ingress.js |2647b8818b8151b3a40f84a5fe9a163d4131fd9e0846135330f8ef01b635a08d|
| server/capabilities-mcp-tools.js |58ac86a429d2f9f6fbca82beadb42ebc7856c464f568cb3871f901d742ffba2c|
| server/capabilities-actions.js |383bcea466e7d512b4bd592939f045d5f5a5b0d55d8557399bf6abb6ac55f0d8|
| server/http-app.js |5f845a872074d3ad2813bc26248d5f6fc4e934d6a31a143c997ffa8c5d4a927b|
| server/test/capabilities-mcp-independent.test.mjs |299b30b2b9177cad68a5b336503851a9759da9877c5def8686564a59793191c4|

Root lifecycle composition прочитана: MCP устанавливается после настоящего Connect binding, до OAuth fallback; normal close awaits MCP before closing domain stores. Failed startup ещё не принимает network requests. Новый adapter не меняет default-off migration/execution.

## Дополнительный causal early-body RED → GREEN

При final read выявлен отдельный early-body путь: внешний route catch отвечает `wireFailure` на `/mcp?` или encoded alias, не выставляя Connection:close при незавершённом upload. Он расположен до ingress/lease/deadline. Рецензент добавил один raw TCP case, не меняя первые шесть или production. Передан буквальный request target `/mcp?`, без URL-нормализации, Content-Length 1048576 и один байт body. Наблюдались HTTP400, `Connection: keep-alive`, 367B ответа; сервер не закрыл socket за 1200ms. Только ограничивающий таймер теста завершил собственный socket. Авторизация вызывалась 0 раз; Invocation0/Notes0.

Команда: `node --test --test-concurrency=1 --test-name-pattern="^independent malformed MCP route" server/test/capabilities-mcp-independent.test.mjs`. Независимый **RED 0/1, 0 skip, 2295.694 ms**, Node24.21.0. Log `output/implementation-20260930/p4-mcp-independent-held-route-red.log`, SHA256 `5f501bace0c488825bd9031f71c2160f4300589b6c329386b3998c4b85fc0c6a`. Test file после добавления седьмого case: SHA256 `24d47bfce392af6c699d2e5b369d6f2476a9f595284fa56c4f004e2163b44284`.

Автор изменил только две error branches в `server/capabilities-mcp.js`: обе выставляют `Connection: close`, если `!req.complete || !req.readableEnded`. Raw route failure больше не оставляет незавершённое body вне ingress deadline; успешный SDK path и семантика эффектов не меняются. Независимый read-only review этой дельты завершён; рецензент source не правил.

Получен author-attributed **GREEN 1/1, 0 skip, 1090.9005 ms** на неизменённом test hash `24d47bfce392af6c699d2e5b369d6f2476a9f595284fa56c4f004e2163b44284`. Наблюдение: HTTP400, `Connection: close`, actual socket закрыт до deadline, `timedOut:false`, 339B ответа, auth0/Invocation0/Notes0. Log `output/implementation-20260930/p4-mcp-held-route-green.log`, SHA256 `052c3ed5f2b8e964e939e7a01029d7b08f32459c0edc356d8558cb9d808094a8`. Рецензент прочитал фактический лог; собственного повторного GREEN не заявляет.

После этого авторский final gate: **17/17 PASS, 0 skip, 5599.6769 ms**, `p4-mcp-author-final.log`. Он не заменяет независимые шесть cases и не превращает прежний six-case log в проверку нового source. Финальный MCP transport SHA256 — `dd49896da1f84054fb963828ae97052a7990305713327e1b1f947aebb37e8af1`; ingress и tools сохраняют хеши из таблицы выше. Этот дополнительный RED и его исправление сохранены отдельно от фактических 6/6.

## Ограничения

Никакого remote, CLI login, model call или production migration этот reviewer не выполнял.

Восемь предложенных ранее сценариев не объявляются целиком независимо выполненными. Held legacy revoke/abort/subscription author gate и прежние B2/C1 результаты остаются отдельными доказательствами. C2b delegation, C2c реальные Codex/OpenCode, D1/model и D2/HTTPS release сохраняются следующими этапами.
