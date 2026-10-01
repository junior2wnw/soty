# P4 C2c: закреплённый Codex CLI, прямой OAuth/MCP канал

Статус: **C2c Codex COMPLETE в свежем run5523: семь PASS, exact replay и сохранение правки, два отдельных MCP401 и AS refresh invalid_grant после отзыва**. [Root result](p4-cli-client-result.md) содержит immutable receipt и независимый final audit. Прежний run5521 остаётся PARTIAL, первый отказ MCP revision5519 сохранён ниже. Выбран настоящий `codex.exe app-server --listen stdio://` и его `mcpServer/tool/call`; детерминированный Responses provider не нужен. Это проверка CLI/app-server transport integration, не модельного выбора P4-D1 или внешнего deployment P4-D2. Автор документа выполнял source/offline проверки; фактическими OAuth/browser/runtime запусками владеет root. Новый passive observer применён с начала свежего5523run и не пересчитывает старые результаты.

## Подтверждённая версия и интерфейс

Проверен ровно установленный binary из [раннего CLI preflight](p4-oauth-cli-preflight.md): `codex-cli 0.153.4`, 295408944 B, SHA-256 `444a3f0008050605cae73cd9b7a2dcac61294062dfaab56dd20430fd6498518b`. Читался этот executable, не личные config/auth/keyring. Git tag `rust-v0.153.4` разрешился в commit `3d2ee51ca2d5db578f328aa75e20aa22c0197c9a`.

В отдельном пустом профиле выполнены только следующие команды, все exit 0:

```text
<pinned-codex.exe> --version
<pinned-codex.exe> mcp --help
<pinned-codex.exe> debug --help
<pinned-codex.exe> app-server generate-json-schema --help
<pinned-codex.exe> app-server generate-json-schema --out <own-schemas>
```

Schema generator работал **без `--experimental`**. `ClientRequest.json` содержит `mcpServer/tool/call`; обязательные поля params — `threadId`, `server`, `tool`, необязательные — `arguments`, `_meta`. Ответ содержит `content` и необязательные `structuredContent`, `isError`, `_meta`. `codex mcp` имеет management/login команды, но не самостоятельную `call` команду.

Локальная квитанция: `var/p4-cli-codex-contract/9f5d8f8c-e6b2-4aa8-bb5d-9d68b49db190/receipt.json`; schema и выбранный публичный pinned source рядом. SHA exact generated `v2/McpServerToolCallParams.json`: `7039a7583a1cbf38e9e0f5fe6398ff3711acd4d654b81b0be562514087fce6ba`; response: `04a59b2064922c37b0c49bdb2470b8fff08d943e56dba4f7280a6b34bfa52c68`.

Текущая [официальная App Server документация](https://learn.chatgpt.com/docs/app-server) отдельно описывает этот метод и stdio handshake. Версионным доказательством служат generated schema и [pinned registration](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/app-server-protocol/src/protocol/common.rs#L1184). В [processor](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/app-server/src/request_processors/mcp_processor.rs#L527) вызов загружает реальный thread и вызывает `thread.call_mcp_tool`; [CodexThread](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/core/src/codex_thread.rs#L928) передаёт его в текущий configured MCP runtime. Это тот же клиентский runtime и OAuth credential store, не MCP SDK внутри нашего harness. Processor сам добавляет thread metadata; harness его не изображает.

## Собственный профиль, без модели и личной авторизации

Root создаёт fresh own каталоги home/work/temp/config/cache/data/state и запускает child с **новым env object**, а не spread родительского env. `HOME`, `USERPROFILE`, `APPDATA`, `LOCALAPPDATA`, `XDG_*`, `CODEX_HOME`, `TEMP`, `TMP` направлены в этот namespace. Из хоста нужны только проверенный `SystemRoot` и явно собранные пути binary/Node/System32. Не наследовать OpenAI/API credentials, proxy variables или bearer headers. Top-level `project_root_markers=[]` ограничивает project config discovery собственным cwd; trust не предоставляется. Системные policy/defaults остаются отдельным источником обычного loader: граница проверена ниже. Это изоляция личных config/auth, не доказательство OS network sandbox или обход системных policy.

Основные поля future config; `H` и абсолютные пути — placeholders root-owned run:

```toml
cli_auth_credentials_store = "file"
mcp_oauth_credentials_store = "file"
model = "c2c-no-model"
model_provider = "c2c_local_only"
model_catalog_json = "<absolute-own-model-catalog.json>"
project_root_markers = []
approval_policy = "never"
sandbox_mode = "read-only"
web_search = "disabled"
project_doc_max_bytes = 0
allow_login_shell = false

[model_providers.c2c_local_only]
name = "C2c transport only"
base_url = "<H>/__qa/no-model/v1"
wire_api = "responses"
requires_openai_auth = false
supports_websockets = false
request_max_retries = 0
stream_max_retries = 0

[analytics]
enabled = false
[feedback]
enabled = false
[history]
persistence = "none"
[shell_environment_policy]
inherit = "none"

[features]
apps = false
plugins = false
remote_plugin = false
recommended_plugins = false
use_agent_identity = false
code_mode = false
code_mode_prewarm = false
responses_websockets = false
responses_websockets_v2 = false
shell_snapshot = false
shell_tool = false
memories = false
remote_control = false
skip_host_skill_discovery = true

[mcp_servers.soty]
url = "<H>/mcp"
required = true
scopes = ["notes.createDraft"]
startup_timeout_sec = 20
tool_timeout_sec = 30
enabled_tools = ["catalog_search", "catalog_get", "notes_create_draft", "invocations_get"]

[mcp_servers.soty.oauth]
client_id = "soty-codex-cli"
callback_url = "http://127.0.0.1/callback"
```

Содержимое own model catalog генерирует `codexFixtureCatalog()`: одна metadata-запись `c2c-no-model` со статической обязательной `base_instructions`, без model response. Первоначальное предложение `{"models":[]}` было ошибочным: загрузчик отвергает пустой список до startup; actual отказы сохранены ниже. [Pinned provider](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/model-provider/src/provider.rs#L445) при explicit catalog выбирает `StaticModelsManager`; нет необходимости обслуживать `/responses` или `/models`. `<H>/__qa/no-model/v1` должен только отказывать и считать попадания: ноль — обязательное observed evidence будущего запуска. [Startup source](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/core/src/session_startup_prewarm.rs#L186) показывает, что одного отсутствия `turn/start` недостаточно: существует auth/WebSocket prewarm. Поэтому профиль исключает model credentials/command auth, Agent Identity и WebSockets. Поля проверены по [pinned config schema](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/core/config.schema.json), SHA `692da7699367f6f4fbbd46c0021278c1311440bcebf0bcb9b836690c05e56196`; результат последующего фактического startup root приведён ниже.

File stores также описаны в [официальном config reference](https://developers.openai.com/codex/config-reference/). **Не задавать `oauth_resource`**: предыдущий actual Codex preflight доказал дублирование discovered resource при override. Root отдельно выполняет один `codex mcp login soty --scopes notes.createDraft` в этом же own env, настоящий Connect consent в браузере и ждёт успешного exit. Код, PKCE, state, authorize URL и auth files не попадают в stdout/report; OAuth store читает только CLI. App-server запускается после login с тем же профилем. Это не `codex login` в OpenAI account.

## Минимальная последовательность channel

Команда следующего этапа: `<pinned-codex.exe> app-server --strict-config --listen stdio://`. Один JSON object на строку, корреляция по собственному integer id, уведомления читаются отдельно:

1. `initialize` с `clientInfo:{name:"soty_c2c_codex_transport",version:"1"}`; после ответа — `initialized`. `experimentalApi` не нужен.
2. `thread/start` с `cwd:<own-work>`, `ephemeral:true`, `model:"c2c-no-model"`, `modelProvider:"c2c_local_only"`, `approvalPolicy:"never"`, `sandbox:"read-only"`. Сохранить только возвращённый thread id в памяти. Не посылать `turn/start`, `command/exec`, `process/*`, shell, dynamic tools или model/list.
3. `mcpServerStatus/list` с `{threadId,limit:10,detail:"toolsAndAuthOnly"}`. Принять ровно configured `soty`, четыре exact tool names и реальные input/output schemas. Не подменять `codex mcp list` доказательством `tools/list`. Status snapshot может создать отдельное соединение: не обещаем один TCP/MCP initialize на весь smoke.
4. Четыре последовательных `mcpServer/tool/call` с `{threadId,server:"soty",tool,arguments}`: `catalog_search` → `catalog_get` → `notes_create_draft` → `invocations_get`. Capability id/version берутся из фактического catalog result; create args проверяются по полученному descriptor, а invocationId — **только из фактического create result**. Известная спецификация используется для assertions, не вместо ответов CLI.
5. Root наблюдает собственный endpoint: реальный MCP `initialize` и revision, `tools/list`, четыре `tools/call`; caller channel не делает HTTP MCP calls. Ревизию нельзя выводить лишь из Cargo.lock/default SDK. Создание Notes подтверждается штатным result/receipt и отдельным owner read; response metadata не заменяет effect evidence.
6. Завершить ровно owned child и его pipes; не искать/убивать процессы по имени. После неизвестного результата не генерировать новую Note/key и не повторять весь flow автоматически. Root отдельно определяет точный replay того же create intent или чтение известного invocation.

Успешный RPC envelope не равен успешному tool result: проверить `isError`, `content`, `structuredContent` и согласованность двух проекций. Domain failure и transport failure учитываются раздельно. Direct method доказывает клиентскую передачу и auth, но не TUI approval UX, LLM comprehension, tool selection или качество текста.

## Собственный адаптер и граница проверки

После отдельного root GO записан единственный новый ignored helper — `var/p4-cli-clients/codex-channel.mjs`. Он экспортирует также `codexFixtureConfig({origin,modelCatalogPath})` и `codexFixtureCatalog()` (оба возвращают строку), чтобы root использовал ровно приведённый secret-free profile/catalog, а не дублировал flags:

```js
createCodexChannel({ binary, env, cwd, onSafeEvent, onOwnedChild, origin })
  // Promise<{ callTool({tool,arguments}), close(), summary() }>
```

`callTool` возвращает raw MCP CallToolResult только владельцу в памяти; принимает только четыре tools. Единственный outstanding request, один thread, общий предел 16 explicit call attempts — достаточен для двух search, get/create/get, PWA edit → exact replay/get и owner revoke → refusal. Fail-closed bounds: startup 45 s; RPC 35 s; весь channel 900 s; close grace 5 s, затем максимум один kill exact child и ещё 5 s на подтверждение exit; request frame 320 KiB, response line 1 MiB, совокупный stdout 4 MiB и stderr 64 KiB, не более 512 notifications. Root явно согласовал 900 s вместо исходно предложенных 180 s для человеческой паузы на PWA edit/mobile revoke; per-RPC/call/byte caps прежние, после проверок root вызывает close. Никакого unbounded chunks array. Overflow/timeout закрывает канал и оставляет unknown effect; retry не встроен. `callCount`/`rpcCount` — попытки, не счётчики committed effects. Неподтверждённый exit остаётся `cleanupConfirmed:false` с точным owned PID.

Safe summary/events: version/hash, owned PID, стадия, RPC/tool name, порядковый id, elapsed, byte counts, exit/closed, hashes рекламируемых schemas и typed safe error code. Raw RPC/stderr, synthetic Note body, authorization URL, headers, query, OAuth store и tokens не сохраняются и не печатаются адаптером или квитанцией. Сам CLI сохраняет credential file в собственном профиле; `ephemeral` не является обещанием отсутствия всех диагностических файлов CLI. Origin ограничен exact `http://127.0.0.1:<port>` предоставленного root run; helper не открывает браузер, не авторизует identity, не читает config/auth и не реализует собственный MCP/SDK transport. Root владеет stand/config/login/browser/wire trace, этот helper — только bounded stdio channel. `modelTurns:0` означает, что adapter не отправляет turn/start, а не самостоятельное доказательство отсутствия всех сетевых запросов CLI.

Независимый source review до первого запуска нашёл ошибку cleanup: прежний `child.error` безусловно считался failed spawn. По [Node24.21 Event: error](https://nodejs.org/download/release/v24.21.0/docs/api/child_process.html#event-error) это событие также возникает при неудавшемся kill уже существующего процесса; exit после error не гарантирован. Исправленная ветка допускает `spawnFailedWithoutPid` только при отсутствии spawn event и PID. Для уже созданного child подтверждением служит настоящий `exit`; failed kill оставляет cleanup unresolved. Отсутствующие stdio при failed spawn обрабатываются без потери lifecycle listener.

Optional `onOwnedChild(child)` синхронно передаёт exact handle единственному root-owned registry после установки listeners и до первого await/RPC. Raw handle остаётся только в RAM этого callback; его нет в events/summary/ошибках. Callback обязан сохранить handle синхронно; throw или thenable запрещает дальнейший startup и запускает cleanup exact child. Factory failure сохраняет safe `.pid` и `.cleanupConfirmed`, в том числе до помещения channel в root map. Это source correction, не выполненный kill-failure/runtime тест.

`node --check var/p4-cli-clients/codex-channel.mjs` на isolated Node24.21.0 — PASS. Исходный author gate не запускал app-server runtime, authorize/token, model requests, Notes mutation или тяжёлые suites; source/syntax проверка не считалась runtime PASS. Последующие фактические запуски root описаны ниже. На момент этого исходного gate успешные MCP negotiation и four-tool flow оставались непроверенными; последующий результат 5521 приведён отдельно. Подмена Responses была рассмотрена как запасной путь; наличие прямого метода снимает её необходимость.

## Первый фактический отказ и исправление профиля

Root run `e187b4e6183f8b46f2911cf5ef844e92`: login PID 14508 завершился exit 1, `authorizeReady:false`, без OAuth подключений/effects. Отдельный bounded read-only `mcp list` в том же own profile подтвердил ровно `Error: --strict-config is not supported for codex mcp` (59 B). [Pinned CLI](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/cli/src/main.rs#L2409) явно запрещает этот root flag для `Mcp`, но допускает обычный `AppServer`. Исправленный login argv: `mcp login soty --scopes notes.createDraft`; app-server argv остаётся `app-server --strict-config --listen stdio://`. Это две разные границы CLI. Отказ не был OAuth failure; автоматического повторного login не выполняли.

Read-only probe root без этого flag выявил следующий exit 1 (257 B): пустой `model_catalog_json`. [Config loader](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/core/src/config/mod.rs#L2057) требует хотя бы одну модель, что первоначальное schema-only чтение автора не учло. `codexFixtureCatalog()` теперь возвращает одну запись с exact slug, обязательными полями [ModelInfo](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/protocol/src/openai_models.rs#L392), явными null для необязательных upgrade/verbosity и отключёнными skills/apps/plugin/search/repl metadata. Enum/truncation values сверены с [публичным pinned models.json](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/models-manager/models.json), SHA `d7136a413cfac1b5b1686d9e0dcc5c80ca05bebed5e9fc3911376561d0ef6ee8`. Каталог не выбирает инструменты и не создаёт Invocation ID; genuine MCP runtime/динамические результаты остаются обязательными.

Обычный `mcp` использует [cloud_config loader](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/cli/src/cloud_config.rs#L35) с `strict_config:false`. Strict flag проверяет неизвестные поля, а не выключает источники конфигурации. Explicit own `CODEX_HOME` выбирает user config/auth; Windows loader дополнительно ищет `OpenAI/Codex/config.toml` и `requirements.toml` через системный [KnownFolder ProgramData](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/config/src/loader/mod.rs#L819), независимо от env ProgramData. Root проверяет лишь наличие этих двух файлов; при наличии policy не обходится и silent isolation не заявляется.

Без явной границы ancestor project configs могут читаться, даже когда trust отключает их применение. [find_project_root](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/config/src/loader/mod.rs#L1548) с `project_root_markers=[]` возвращает cwd; [discovery](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/config/src/loader/mod.rs#L1707) ограничен этой директорией. Git metadata lookup может отдельно проверять предков; отсутствие любых filesystem reads не обещается. Root controller уже удаляет `OPENCODE_*` из Codex env; ранее высказанное подозрение о его несовместимости снято по current source, изменения здесь не требовались.

Следующий normal `mcp list` root выявил ещё одно обязательное условие custom deserializer (exit 1, 371 B): у записи отсутствовали и `base_instructions`, и `model_messages.instructions_template`. Проверка десяти обязательных полей `ModelInfo` этого не доказывала; условие находится в [ModelsResponse deserializer](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/protocol/src/openai_models.rs#L796). Добавлена только статическая строка `base_instructions: "Transport-only fixture metadata. No model turn is permitted."`. Она удовлетворяет metadata contract; запрет model turn обеспечивается прежним channel API и отказным provider route, а не текстом этой строки. Изменений RPC, OAuth, scopes, lifecycle или limits нет.

Эти дельты исправили три подтверждённых несовпадения. Последующий actual normal `mcp list` root в own profile завершился exit 0 (405 B): один сервер, exact `soty`, URL собственного origin. Это проверяет семантическую загрузку исправленного catalog/config исполняемым Codex до OAuth. Отказы сохранены; этот read-only результат сам по себе не доказывает login или MCP tools.

## Actual OAuth и отказ MCP revision

Root run `61fa1121207085d066a80b922a68879c`, H=`http://127.0.0.1:5519`: genuine Codex OAuth PID 45472 завершился exit 0; callback listener принадлежал owned child, issuer совпал с AS, token endpoint вернул 200. После этого channel child PID 31016 прошёл app-server `initialize`, но `thread/start` завершился `codex_rpc_error`: actual MCP POST `initialize` получил HTTP 400 / JSON-RPC `-32022`. Notes, Invocation и spend — ноль. Child завершён, `cleanupConfirmed:true`, owned registry пуст. Разрешённое тестовое подключение затем отозвано root через PWA. Failed-seal SHA `d25dfa71f6d30e8af2719696b16b28195999cfd4fe7e7e361825ed893c5f72eb` сохранён; эти факты не переписываются будущим успешным запуском.

Старый QA observer сохранял только две ожидаемые revision и преобразовал другую в null. Поэтому точную **наблюдавшуюся wire-дату** этого run утверждать нельзя. Root исправил только наблюдение safe ISO revision/error.data.requested для следующего запуска; protocol bytes не переводятся и success не выводится из ожидаемой даты.

**Source-known дата — `2025-06-18`.** [Pinned Codex initialize builder](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/codex-mcp/src/rmcp_client.rs#L1034) явно вызывает `.with_protocol_version(ProtocolVersion::V_2025_06_18)`. [McpProtocolMode](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/rmcp-client/src/protocol_mode.rs#L9) по умолчанию Legacy; его preferred и modern fallback также равны `2025-06-18`. Cargo.lock закрепляет rmcp 3.1.3. Собственный `LATEST` этой библиотеки равен `2025-11-25`, но Codex переопределяет его: выводить поведение binary только из dependency default было бы ошибкой.

Maintained SDK2.2.0 уже содержит `2025-06-18` в [legacy constants](https://github.com/modelcontextprotocol/typescript-sdk/blob/v2.2.0/packages/core/src/constants.ts). Public [Server implementation](https://github.com/modelcontextprotocol/typescript-sdk/blob/v2.2.0/packages/server/src/server/server.ts) принимает `supportedProtocolVersions`; `_oninitialize` возвращает exact requested version, когда она входит в разрешённые legacy versions. По [MCP 2025-06-18 lifecycle](https://modelcontextprotocol.io/specification/2025-06-18/basic/lifecycle#version-negotiation) поддерживаемая requested version должна быть возвращена без изменения, а дальнейшие HTTP requests используют согласованный version header. Это позволяет добавить ровно третий профиль `2025-06-18` в существующий SDK `legacy:'stateless'`, сохранив finite SSE/authority/lifetime. Не требуется добавлять все старые версии, переключать Codex на modern feature или переписывать request/response.

Root поручил узкую product compatibility правку protocol author. Здесь нет изменения server/channel/config и нет утверждения, что эта правка уже прошла actual CLI flow. Свежий wire witness после product gate должен подтвердить initialize/negotiated date и настоящие tools; неизвестные revisions должны по-прежнему отказываться.

SHA ниже рассчитаны по exact публичным source bytes, не по CRLF worktree и не по bytes executable:

| Закреплённый первичный источник | SHA-256 |
| --- | --- |
| Codex `3d2ee51c`, `codex-mcp/src/rmcp_client.rs` | `b7ac2a82e39194390f5a8cc921449e33953f2a3c399fd6cd57faeb5ec27d42dc` |
| Codex `3d2ee51c`, `rmcp-client/src/protocol_mode.rs` | `c472aa882c4c66a1d5b1459d4836a1ed507e1c9ecbb86b63b22c7444f0b62cd1` |
| [Codex `3d2ee51c`, Cargo.lock](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/Cargo.lock#L12115) | `3494b8a78d0f643556a83a9cc184e912bcab9f4c5640288952f4223452ba5dc8` |
| [rmcp source `fd14830a`, model.rs](https://github.com/modelcontextprotocol/rust-sdk/blob/fd14830a193a7bb9cf232ce3a6d0e544dc7a7a41/crates/rmcp/src/model.rs#L169) | `fc49cdae250ff899812be8b9efb3a33a03ba445603df699df7bedaa33dc004e0` |
| TypeScript SDK `v2.2.0`, `packages/core/src/constants.ts` | `d83faedc6430321547fa489d697c9a76153c8148b88ffbe9d4bca5647aafc770` |
| TypeScript SDK `v2.2.0`, `packages/server/src/server/server.ts` | `4c2c5191aecfcea1214a42c2c116b00cf454faa316fabce6ca6e39176b099e69` |

Публичный crate `rmcp-3.1.3.crate` (513181 B) проверен только в памяти: SHA `5f17072af977b0f86f714dbd64b3d37d0715bb63064f9d13483f0a1775813374` совпал с Cargo.lock; `.cargo_vcs_info.json` указывает `fd14830a193a7bb9cf232ce3a6d0e544dc7a7a41`. Его source не исполнялся, SDK не подменялся. Code freeze channel остаётся `b27d91d5d8dcf75ecb12d3600d385ee426a5be4662f5a50af7770c3fcf5b25d6`; эта последняя дельта затрагивает только документ.

## Обновление токена до tools/call и отдельный AS denial

По сообщению root, run `e704dc1eb48ca5635500d444b4f1fb7e`, H=`http://127.0.0.1:5521`, подтвердил genuine Codex discovery/create/get/exact replay/get; правка владельца сохранилась. После отзыва через PWA 320 px `revoked_create` завершился RPC `-32603`: новый GET `/mcp` получил 405 с header `2024-11-05`, затем POST `/oauth/token` — 400; нового POST `tools/call` не было. Прежний observer для token endpoint сохранял лишь client/status. Поэтому этот HTTP400 не доказывает ни `invalid_grant`, ни причину отказа и не закрывает прежний MCP POST401/403 gate.

Root штатно остановил стенд через TTY: exit 0, owned children 0. Immutable receipt SHA `af25369251a6b46add02e15e15491ef026cebbc2bd6e1615721f3bdb9bcb8716` и `frozen-source` с 16 точными файлами сохраняют **Codex PARTIAL / OpenCode COMPLETE**; global Notes/Invocation/spent — по 2, по одному effect на клиента, обе правки совпали и оба подключения отозваны. Новый observer не изменяет эти факты или файлы run.

Причина отсутствия MCP POST согласуется с закреплёнными исходниками, но не выводится как доказанная только из 400. [Codex call_tool](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/rmcp-client/src/rmcp_client.rs#L778-L808) прежде операции вызывает `refresh_oauth_if_needed()`. [OAuth expiry](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/rmcp-client/src/oauth.rs#L968-L978) учитывает 30 s skew, при product access-token TTL до 300 s. [Refresh transaction](https://github.com/openai/codex/blob/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/rmcp-client/src/oauth/refresh_transaction.rs#L169-L206) при `TokenRefreshRejected` возвращает `AuthorizationRequired` до инструмента. При такой ветке отсутствие `tools/call` ожидаемо.

[rmcp discovery](https://github.com/modelcontextprotocol/rust-sdk/blob/fd14830a193a7bb9cf232ce3a6d0e544dc7a7a41/crates/rmcp/src/transport/auth.rs#L2695-L2708) использует `2024-11-05` буквально в GET для OAuth metadata. Этот GET не означает смену согласованной MCP revision; наш `/mcp` отвергает метод до bearer authentication. В [обработке refresh](https://github.com/modelcontextprotocol/rust-sdk/blob/fd14830a193a7bb9cf232ce3a6d0e544dc7a7a41/crates/rmcp/src/transport/auth.rs#L2191-L2204) лишь разобранный `error=invalid_grant` становится `TokenRefreshRejected`. По [RFC6749 §5.2](https://www.rfc-editor.org/rfc/rfc6749.html#section-5.2) этот код включает не только отзыв, но также недействительный/истёкший grant или чужую client binding; голый HTTP400 тем более недостаточен.

В новом согласованном oracle поверхности различаются: **MCP denial** — конкретный новый MCP POST401/403; **AS refresh denial** — конкретная штатная попытка CLI обновить токен, HTTP400 и строго выбранный `invalid_grant`. AS denial не является выполненным MCP tool result или доказательством, что ещё свежий bearer дошёл до resource server. Root связывает его с собственным вызовом, настоящим предшествующим согласием/успешным использованием и подтверждённым отзывом connection; проверяет отсутствие новой Note/Invocation/spend, раскрытого результата, выдачи нового доступа и повторного согласия. Observer сам не устанавливает причину domain refusal и не выдаёт общий PASS.

Для самостоятельного future gate свежего bearer достаточно перед отзывом выполнить genuine read-only get после естественного порога обновления и наблюдать успешный штатный refresh/get, затем promptly revoke и два explicit negative calls. [AuthClient](https://github.com/modelcontextprotocol/rust-sdk/blob/fd14830a193a7bb9cf232ce3a6d0e544dc7a7a41/crates/rmcp/src/transport/common/auth/streamable_http_client.rs#L15-L64) после401 сам делает одну попытку обновления; токен или transport harness не подменяет. Это source-supported сценарий, не выполненная здесь проверка.

## Пассивный token observer: подготовленный source freeze

Изменён только task-owned `var/p4-cli-clients/wire-trace.mjs`, добавлен `var/p4-cli-clients/oauth-denial-observer.test.mjs`. Production, Codex channel, реальные wire bytes/headers/body и прежние run data не изменены. Token request-start увеличивает общий `requestStarts` и копирует безопасные `client`, `actionId`, `requestedTool`, `requestSeq`; ссылка на изменяемый action не сохраняется. Поздний ответ не присваивается следующему действию: при завершении требуются те же current action id/client и active client.

Token row дополнена `finished`, `capture`, `requestEnded`, byte counts, `actionMatches`, `requestValid`, `grantType`, `clientBinding`, `resourceBinding`, `selectedCounts:{grantType,clientId,resource}`, `responseValid`, `responseError`, `errorCount`, `asRefreshDenied`. Единственный выводимый grant marker — `refresh_token`, error marker — `invalid_grant`; client/resource — `exact|absent|invalid`, без чужого значения. При отсутствии `client_id`/`resource` сохраняется именно `absent`, а не выдуманное совпадение. Request требует строгий UTF-8/form, непустой refresh token, отсутствие duplicate fields и ровно один `grant_type`; существование секрета проверяется в RAM без записи его значения/digest. Response — завершённый HTTP400 JSON object с единственным `error`, совпадающим с `invalid_grant`; допускаются только стандартные строковые error fields. Дубликаты, включая escaped key, не схлопываются через JSON.parse.

`asRefreshDenied` требует всей цепочки, в том числе привязки к действию при старте и finish. Generic400, другое error, malformed form/JSON/UTF-8, wrong client/resource, overflow, early response до request end, abort, timeout или смена действия дают false. Root дополнительно фильтрует `requestSeq > before` и exact `actionId`; stage/controller остаётся отдельным владельцем acceptance.

Capture ограничен одним request buffer 16384 B и одним response buffer 4096 B, максимум 16 одновременно, 15 s lifetime наблюдения; row cap 256 прежний. Лимит/timeout останавливает только наблюдение, не network request. Буферы автора обнуляются и ссылки освобождаются при finish/abort/timeout/overflow; observer снимает свои listeners и возвращает собственные wrappers, не меняя downstream return values/callbacks/encoding. Секреты, verifier, body, headers и description не входят в rows/report/output. Краткоживущие строки strict decoding/JSON в JS heap и собственная память Provider/CLI не дают обещания криптографического стирания всей process memory.

Подготовлены **7 focused tests**: actual HTTP byte preservation и безопасная проекция; optional bindings/cardinality; 20 отрицательных form/error/binding cases; delayed action/client; abort/incomplete cleanup; byte/concurrency limits; wrapper callback/encoding/return/writeHead semantics. Автор выполнил только `node --check` обоих файлов на Node24.21.0 — PASS; автор тесты/runtime **не запускал**, execution gate принадлежит root:

Независимый source review до запуска заметил false-positive в тесте: substring `authorization` совпадал с прежним безопасным counter `authorizations`. Проверка заменена на exact JSON secret/header keys; canary secret/description assertions сохранены. Production observer при этом не менялся; RED runtime этого теста не заявляется.

Первый root serial run на observer `f5d5c344…` и tests `4c1358f9…`: **6 PASS / 1 FAIL**, 0 skip, 129.2254 ms, сохранён `output/implementation-20260930/p4-oauth-denial-observer-root.log`. Настоящий HTTP case не получил AS marker, хотя fake/event cases прошли. Причина подтверждена [Node24.21 writeHead source](https://github.com/nodejs/node/blob/v24.21.0/lib/_http_server.js#L433-L468): direct `writeHead(status,headers)` может передать headers прямо в `_storeHeader`, не заполнив progressive cache для `getHeader()`. Предыдущее наблюдение ошибочно полагалось на этот cache.

Исправление сохраняет настоящий HTTP fixture с `writeHead`, проверку точных bytes и обязательный `asRefreshDenied:true`; дополнительно fixture утверждает, что Content-Type действительно отсутствует в getHeader, и печатает при failure только выбранные safe markers. Observer пассивно оборачивает `writeHead`, учитывает его object/raw-array/reason overload и читает только собственный Content-Type data descriptor; сохраняет лишь boolean classification `{provided,json}`. Arguments/this/return передаются неизменёнными в оригинал; header cache не заполняется искусственно. Accessor/duplicate/non-string выбранный type не доказывает denial; остальные header values не читаются. Wrapper снимается при том же cleanup. В существующей группе тестов добавлены overload/precedence/accessor assertions. Результат исправленного runtime gate ещё ожидается от root; исходный failed log не изменён.

```powershell
& .\var\toolchains\node-v24.21.0-win-x64\node.exe --test --test-concurrency=1 var/p4-cli-clients/oauth-denial-observer.test.mjs
```

Exact worktree bytes при передаче:

| Файл | Bytes | SHA-256 |
| --- | ---: | --- |
| `var/p4-cli-clients/wire-trace.mjs` | 19716 | `1223f6ffd659b489ccd7d957d0419343fb273c29a5b17a533314cdfad9277002` |
| `var/p4-cli-clients/oauth-denial-observer.test.mjs` | 13802 | `44be97e33cdae9111981c6bda0aaebbbd400908752c325329f643852c256a79b` |

Новый CLI/auth run и подтверждение AS denial ещё не выполнялись автором. Доказательство текущего token oracle не переносится на исторический400 стенда5521.
