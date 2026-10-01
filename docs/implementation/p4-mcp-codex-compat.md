# P4 C2c: MCP 2025-06-18 compatibility

Дата: 2026-10-01. Узкая поправка server admission; самостоятельный прогон Codex CLI остаётся gate root.

## Причина и первичные источники

Root наблюдал реальный C2c отказ initial `initialize` с HTTP 400 / `-32022`. Pinned Codex `0.153.4`, commit `3d2ee51ca2d5db578f328aa75e20aa22c0197c9a`, в `mcp_initialize_request_params` явно выбирает `ProtocolVersion::V_2025_06_18`: [официальный исходник](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/codex-mcp/src/rmcp_client.rs). Прежний Soty admission обслуживал только `2026-07-28` и `2025-11-25`. Отказ был несовместимостью выбранного серверного профиля; он не доказывает отказ OAuth.

Спецификация June допускает JSON или SSE ответ на POST, пустой HTTP 202 на notification и HTTP 405 на необязательный GET stream. После initialize клиент обязан посылать согласованный `MCP-Protocol-Version`; session ID необязателен. Проверены [transports](https://modelcontextprotocol.io/specification/2025-06-18/basic/transports) и [lifecycle](https://modelcontextprotocol.io/specification/2025-06-18/basic/lifecycle).

Установленные server/core/client SDK `2.2.0` уже реализуют June. В `createLegacyStatelessFallback` используется штатный per-request transport без session ID; `Server._oninitialize` возвращает запрошенную разрешённую legacy version; `Client._legacyHandshake` берёт первую версию из явного `supportedProtocolVersions`. Прочитаны pinned upstream [Streamable HTTP tests](https://github.com/modelcontextprotocol/typescript-sdk/blob/v2.2.0/packages/server/test/server/streamableHttp.test.ts) (включая June и stateless) и [version negotiation tests](https://github.com/modelcontextprotocol/typescript-sdk/blob/v2.2.0/packages/client/test/client/versionNegotiation.test.ts) (custom June offer). Их собственный suite здесь не выполнялся.

## Изменение

`server/capabilities-mcp.js` хранит закрытый legacy список `[2025-11-25, 2025-06-18]`; полный advertised список равен `[2026-07-28, 2025-11-25, 2025-06-18]`. Только эти две legacy версии допустимы для initial `initialize` без заголовка. Входящие bytes не переписываются. Штатные SDK dispatch, finite SSE collection и stateless lifecycle общие для обоих legacy вариантов.

Заголовок последующих запросов обязателен в принятом профиле. `2025-03-26`, другие старые версии и неизвестные даты по-прежнему не допускаются: не наследуем широкий SDK legacy allowlist/default. Version/header/meta проверки, закрытая error projection, input/output bounds, свежая authority перед первой private wire byte и cleanup admission slot сохранены. Codec, session registry, OAuth audience, Notes input/output и pinned semantic digest не менялись.

Существующие tests автор не редактирует. Root отдельно обновляет только literal `error.data.supported` там, где прежде ожидались две версии. Отрицательные assertions unknown03 (HTTP 400, `-32022`, exact requested date) сохраняются.

## Два новых сценария

`server/test/capabilities-mcp-codex-compat.test.mjs`:

1. Настоящий SDK Client `2.2.0`, legacy mode с единственным `supportedProtocolVersions: [2025-06-18]`; неизменённый fetch отправляет настоящий SDK wire. Existing `oauthNativeFixture` создаёт реальные Connect/Capabilities3/Notes2/Provider и подписанное согласие. Наблюдаем headerless initialize, согласованный June и initialized 202; четыре tools; RU/EN search и exact get из реального search; create/read; owner edit; text-only conflict; keyless AS-off exact replay/read; затем signed connection revoke и фактический 401/403 того же SDK клиента при прежнем create key и read. Ledger/effect count остаётся один, spent один; receipt не содержит исходный/новый текст Note, account ID или секрет.
2. Raw June initialize/notification, отсутствие заголовка у последующих notification/list/create, unknown03/future initial и header, несовпадающий June header/modern meta. Проверяется закрытая безопасная ошибка без client-info reflection и ноль Invocation/Note/budget effects.

Это два причинных compatibility сценария; они не заменяют genuine Codex binary acceptance или прежние modern/November, held-body, authority-after-buffer и capacity gates. OAuth plaintext живёт только в RAM fixture и не печатается.

## Статус проверки

Авторский `node --check` на isolated Node `24.21.0`: оба собственных JS/MJS файла PASS. Никакие tests, HTTP hosts, CLI/auth/model процессы автором на этом этапе не запускались. C2c исходный wire RED атрибутирован root; новый GREEN до его serial gate не заявлен.

Команда для согласованного root gate из worktree, с isolated Node `24.21.0` в локальном PATH:

```powershell
$env:PATH = (Resolve-Path 'var/toolchains/node-v24.21.0-win-x64').Path + ';' + $env:PATH
node --test --test-concurrency=1 --test-reporter=tap server/test/capabilities-mcp-codex-compat.test.mjs
```

Inventory: единственный production файл `server/capabilities-mcp.js`, один новый test выше, эта квитанция. Shared helpers, существующие tests, package/lock, host/CLI config и другие документы автором не изменены.

Source freeze SHA-256 (worktree bytes):

| Файл | SHA-256 |
| --- | --- |
| `server/capabilities-mcp.js` | `385e105d59544e094f17a0471c02dd65df67fb372a94e180c09a21c481c5e45d` |
| `server/test/capabilities-mcp-codex-compat.test.mjs` | `abe2234cd2f12768f0368fa6c09c452e4c3ecff7a31160ebaae8ae18e759749b` |

## Фактический root gate

Serial isolated Node24.21.0: новый файл **2/2 PASS,0skip**,2031.1656ms, `output/implementation-20260930/p4-codex-compat-root-first.log`. Три прежних MCP/ingress/independent файла отдельным прогоном **24/24 PASS,0skip**,8959.0384ms, `p4-codex-compat-affected-root.log`. Изменены только два expected supported-list литерала в прежнем тесте; unknown03 и все error/status/requested assertions сохранены.

Независимый source review не нашёл блокеров: штатный SDK создаёт June wire, наблюдающий fetch его не переписывает; проверки полномочий и отсутствие private reflection сохраняются. Это принятие causal compatibility среза.

[Actual C2c root result](p4-cli-client-result.md) теперь подтверждает настоящий Codex0.153.4: offered/negotiated06, signed owner consent, одна Note и последующая правка в обычной PWA, exact replay без второго эффекта, два отдельных MCP401 после owner revoke. Финальный fresh5523 result SHA `a872c63e11a94f60790055018d26e2fa35f1ee8cfc29d9cf56ebe67bf7b6e44a` независимо сверён с source/dist и холодными RO stores. D1 с настоящей моделью и production остаются отдельными gates.
