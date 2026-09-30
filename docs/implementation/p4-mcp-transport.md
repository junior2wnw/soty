# P4-C2a: finite MCP transport receipt

Квитанция автора C2a transport. C2b delegation и C2c настоящие CLI/модельные сценарии им не подменяются. Source freeze выполнен после причинных no-echo и close-on-unread исправлений: финальный авторский gate 17/17 PASS, отдельный неизменённый независимый route regression 1/1 PASS.

## Область и composition

Автор владеет только новыми `server/capabilities-mcp.js`, `capabilities-mcp-ingress.js`, `capabilities-mcp-tools.js`, своими `server/test/capabilities-mcp*.test.mjs` и этой квитанцией. Root отдельно владеет dependency pins, extraction `createCapabilityOperations`, HTTP/OAuth namespace order, composed OpenAPI и sidecar. Domain/schema/ledger/Notes/catalog@1 этот срез не меняет.

`attachCapabilitiesMcp(app,{service,origin,limits?}) → {close():Promise<void>}`. Новый mount не включает execution/AS и не мигрирует storage. `origin` — trusted canonical H, audience — ровно H/mcp. Root передаёт настоящий capabilities service. Mount до OAuth fallback позволяет keyless AS-off bearer/read/replay.

Поддерживаемый SDK public API: `@modelcontextprotocol/server`2.2.0, `Server`, `createMcpHandler`, `projectCallToolResult`; validator — public `validators/ajv`. Установленные версии и lock принадлежат root. Профиль: legacy stateless, modern JSON, keepAlive0, maxSubscriptions0. Только2026-07-28 и2025-11-25; SDK делает classification/codec/dispatch, host сужает допустимые revisions. Прямого импорта SDK internals нет. Персональный actor находится в request-local closure/WeakMap, а не в wire `authInfo`.

Итоговый прямой dependency profile — runtime server/core2.2.0 и devclient2.2.0; root удалил неиспользованный node2.1.0 adapter. Bounded host сам формирует стандартный Request из trusted H, проверенных raw headers и AbortSignal; Node converter добавил бы второе чтение/listener lifecycle, а toNodeHandler начал бы отправку до final authority gate. Это узкое обоснование отсутствия middleware adapter, не собственный MCP codec.

Tools: `catalog_search`, `catalog_get`, `notes_create_draft`, `invocations_get`. Definitions содержат точные самостоятельные JSON Schemas: существующие OpenAPI local refs перекоренены в `$defs`, без изменения семантики. Все inputs/outputs проверяются; для Note result отдельно вызывается pinned catalog `validateOutput({noteId,revision})`. Capability digest остаётся `95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204`.

Успех содержит одинаковый checked JSON в TextContent и structuredContent: это сохраняет cursor/Invocation ID/schema для OpenCode text consumer. Tool error — `isError:true`, один JSON TextContent `{error:{code}}`, без structuredContent; успех-schema не используется для ошибочного envelope. Public catalog ошибки имеют свой узкий allowlist. SDK wire format остаётся соответствующим revision; legacy SSE не перекодируется в invented JSON.

## Ограничения и authority

- POST только по точному `/mcp`; raw duplicate header, Host, TLS/Origin, media/encoding, CL/TE проверяются. Cookies/forwarded peer/clientInfo не дают права. Missing/expired/wrong audience bearer получает MCP PRM challenge; HTTP token и MCP token различаются.
- Отказ при незавершённом или непрочитанном теле (`!req.complete || !req.readableEnded`) устанавливает `Connection: close` как во внешнем route guard, так и в обработчике admitted request. Невалидный upload не дренируется ради повторного использования соединения.
- Вход≤2097152B, absolute read15s, fatal UTF8/no BOM. Перед JSON.parse bounded scan: depth20/nodes10000, decoded duplicate keys запрещены, `_meta`≤16384B, ID safe integer или string≤160. Один input buffer, без массива tiny chunks.
- 8 admitted exchanges/process,2/socket peer,60attempts/60s,2048peer slots. Trusted test limits могут только уменьшаться. Lease не освобождается от одного `finish`/`close`: release только после внешнего finally и SDK cleanup.
- SDK response полностью читается в один bounded buffer≤2097152B. Absolute response/write15s. После последнего await чтения снова выполняются authenticate и, при наличии private result, common authorized read того же Invocation. До проверки не отправляются даже response headers; между проверкой и `res.end(buffer)` нет await.
- Каждому request соответствует AbortController. Отключение, deadline и adapter.close отменяют также legacy stream; reader cancel/release и explicit server.close выполняются до освобождения lease. `sdk.close()` сам по себе не считается закрытием legacy.
- Common create использует прежние durable admission/reconcile/execute/read, exact replay перед readiness. Refresh actor не заменяет original authorization; сбой/abort после Note COMMIT не означает отсутствие эффекта. Исторический get не читает Note body/current existence.

## Причинные проверки

Runtime: isolated Node24.21.0/SQLite3.53.4, command-local PATH. Suites запускаются последовательно по выделенному test slot; TAP logs в `output/implementation-20260930/`.

1. Root shared HTTP первый import RED обнаружил, что `InvalidParamsError` — type-only имя, не runtime export SDK2.2.0. Автор заменил его публичными runtime `ProtocolError`/`INVALID_PARAMS`; import smoke PASS. Это реальная ошибка адаптера до запуска fixture.
2. `p4-mcp-author-first.log`: wire5/7PASS,2RED. Два test-oracle дефекта: modern serverInfo в `_meta` (а не legacy root field); Node URL constructor удалял пустой `?`. Fixture исправлен на public SERVER_INFO_META_KEY и literal request.path. Production для этих двух результатов не менялась.
3. `p4-mcp-protocol-red.log`: исправленные2fixture cases PASS;2настоящих protocolRED:405использовал-32603 вместо-32601; unsupported revision не возвращал fixed supported list. Narrow correction добавила -32601 и безопасные supported/requested, без arbitrary header/meta reflection.
4. `p4-mcp-author-wire-green.log`:13/13PASS,0skip,4423.7563ms. Это9полных HTTP/Connect/Notes/Caps/OAuth cases +4ingress primitive cases. Effect/replay/read, pinned schema/profile, strict framing, purge/restart/disabled, OAuth refresh/keyless, actual public SDK.close held capacity; примитивные тесты отдельно доказывают chunk bounds/deadline/stream cancellation, не pretend-Provider flow.
5. `p4-mcp-lifecycle-first.log`:3новых actual SDK cases PASS: held final legacy SSE EOF→signed revoke→0private wirebytes при уже committed Note; disconnect→reader cancel/slot recovery; valid subscriptions immediate finite refusal. Headerless unsupported initialize дал отдельный RED -32020 вместо-32022; порядок revision admission исправлен узко.

Held-stream seam не подменяет auth/storage/readiness/SDK bytes: public Response.body wrapper задерживает EOF второго (monitored) настоящего SDK SSE response после fetch. Signed revoke идёт настоящим Connect HTTP, SQLite показывает совершённую Note до release. SDK close barrier аналогично держит completion настоящего публичного close, чтобы проверить lease до cleanup. Это инструментирование adversarial scheduling, не заявленный production hook.

Independent header no-echo RED принадлежит критику: `capabilities-mcp-independent.test.mjs`,3SDK paths в1case, headerReflected:true при status400/-32020; metadata не отражена, durable counts0. Файл критика автор не менял. Узкий repair и GREEN зафиксированы ниже.

6. Narrow repair использует public `deserializeMessage`/`serializeMessage` **только для полностью прочитанного application/json SDK HTTP error≥400**. Проверяется single JSON-RPC error, тот же bounded request ID и известный numeric protocol code. Динамические message/data удаляются. Version data содержит только fixed supported и дату либо `unknown`; required-client-capabilities допускает только фиксированные известные имена и пустые листья. Success/SSE bytes не разбираются и не перекодируются. Public `validateStandardRequestHeaders` в установленном SDK не экспортирован; private imports/manual Base64 decoder не добавлены.
7. `p4-mcp-noecho-lifecycle-green.log` вопреки имени содержит **4PASS/1FAIL**, а не окончательный GREEN: known-code guard безопасно вернул500, потому что -32020 — SDK serving-ladder constant вне public ProtocolErrorCode enum. Добавлен ровно этот установленный код, без произвольного диапазона. Own initial version +3lifecycle cases уже прошли в этом запуске.
8. `p4-mcp-noecho-green.log`: неизменённый independent no-echo case1/1PASS,0skip,1187.8627ms. Все3paths:400/-32020, headerReflected:false, metadataReflected:false, Notes/Invocations0. Автор повторил только этот причинно исправленный case, не приписывает себе остальные independent tests.
9. Критик отдельно сообщил6/6PASS,0skip,2976.126ms (`p4-mcp-independent-first.log`): actual SDK Client2.2 modern/legacy с настоящими stores, exact public-only transcript, detail279008B/wire835046B и834951B, lower65536 cap→0wirebytes/recovered capacity, Express TLS/proxy admission. Это независимая атрибуция; её исходник/квитанция принадлежат критику.
10. Дополнительный independent route RED: literal raw `POST /mcp?`, 1 байт из `Content-Length: 1048576` дал HTTP400/keep-alive; сокет оставался открытым все 1200ms до owned test deadline. Authentication/Notes/Invocations — 0. `p4-mcp-independent-held-route-red.log`: 0/1 PASS, 2295.694ms. Авторская delta ограничена close-on-unread в двух error branches. Неизменённый case затем 1/1 PASS, 0 skips, 1090.9005ms (`p4-mcp-held-route-green.log`): HTTP400/Connection close, сокет закрылся до deadline, counts по-прежнему 0. Это ранний route refusal, не проверка SQL rollback.
11. Финальный авторский запуск на хешах ниже: **17/17 PASS, 0 FAIL, 0 SKIP, 5599.6769ms** (`p4-mcp-author-final.log`). Это 13 actual wire cases и 4 ingress primitive cases. Оба новых источника тестов проверены вместе после последней production delta; неизменённый independent route case перед ними запущен отдельно. Широкие shared suites автор повторно не запускал.

Последние команды выполнены последовательно из worktree, после добавления isolated runtime directory в command-local PATH:

```powershell
$env:PATH = (Resolve-Path 'var/toolchains/node-v24.21.0-win-x64').Path + ';' + $env:PATH
& .\var\toolchains\node-v24.21.0-win-x64\node.exe --test --test-concurrency=1 --test-reporter=tap --test-timeout=30000 --test-name-pattern='^independent malformed MCP route' server/test/capabilities-mcp-independent.test.mjs
& .\var\toolchains\node-v24.21.0-win-x64\node.exe --test --test-concurrency=1 --test-reporter=tap --test-timeout=30000 server/test/capabilities-mcp.test.mjs server/test/capabilities-mcp-ingress.test.mjs
```

## Frozen inventory

| Собственный файл | SHA256 |
|---|---|
| `server/capabilities-mcp.js` | `dd49896da1f84054fb963828ae97052a7990305713327e1b1f947aebb37e8af1` |
| `server/capabilities-mcp-ingress.js` | `2647b8818b8151b3a40f84a5fe9a163d4131fd9e0846135330f8ef01b635a08d` |
| `server/capabilities-mcp-tools.js` | `58ac86a429d2f9f6fbca82beadb42ebc7856c464f568cb3871f901d742ffba2c` |
| `server/test/capabilities-mcp.test.mjs` | `0d6c58fc1605ce811142af0c7848da60d9b00874ea5153462474a6cc09dd863b` |
| `server/test/capabilities-mcp-ingress.test.mjs` | `eade205af7f7a3a9792b9d962a366005069305ee226544b7a4e244d19344cadd` |

| Авторский evidence log | SHA256 |
|---|---|
| `p4-mcp-author-first.log` | `854daa9132f8491e076a05ec920ec57ca2869810417e8c3430786ce186bbf547` |
| `p4-mcp-protocol-red.log` | `309c7a0cbc121dc3a5d353ce0504e87bfc0b293eb8dfb76f292ce317e5088084` |
| `p4-mcp-author-wire-green.log` | `a5249bcd821827c9a971f9a6326ac606f9a4a054349438f5d84d9b22e0a55d19` |
| `p4-mcp-lifecycle-first.log` | `2893c4e7c4115a9470e9771f6169902e273134a68d8caa4c669fba465606126c` |
| `p4-mcp-noecho-lifecycle-green.log` (4/5, не final PASS) | `a71763fa34e79fa70345309cd24ba4c5a6cb6e2ccfb1b29f912c5ba0fe004511` |
| `p4-mcp-noecho-green.log` | `2c927727a33e9d0ab2a4bd3b41ff3d5ba31e599a4f52155fb7f19535a0f878f6` |
| `p4-mcp-held-route-green.log` | `052c3ed5f2b8e964e939e7a01029d7b08f32459c0edc356d8558cb9d808094a8` |
| `p4-mcp-author-final.log` | `56c8e2f23edc761724ddd594b1f3facdc780f83541491b2fd893f56985aac156` |

Независимый causal route RED принадлежит критику: `p4-mcp-independent-held-route-red.log`, SHA256 `5f501bace0c488825bd9031f71c2160f4300589b6c329386b3998c4b85fc0c6a`.

## Чего этот срез не доказывает

Не выполнялись реальные Codex/OpenCode OAuth login/tool/model runs, browser owner-visible acceptance, Linux capacity или remote deployment. No fake human, token passthrough, DCR, generic executor, second ledger, child derivation или новый private catalog. Размеры allocations/окон конкретны; фиксированный production RSS/constant-time всех SQLite paths не заявляется. Штатные предупреждения SDK JSON mode и Provider parsed body не являются секретами, но квитанция не утверждает «console clean».
