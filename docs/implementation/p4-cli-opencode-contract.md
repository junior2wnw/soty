# P4-C2c: закреплённый OpenCode и настоящий MCP tool loop

Дата: 2026-10-01. База продукта: C2b `2103d755c081f03cdad98959fe593c6151ea2eec`. **C2c OpenCode COMPLETE:** root выполнил настоящий OAuth/MCP/PWA run5521, семь PASS, одну Note с сохранением правки и два отдельных401 после отзыва; [root result](p4-cli-client-result.md) и immutable receipt независимо проверены. Ниже сохранены source contract и история author review: автор тогда не запускал `auth`, `serve`, `run` или suites и не читал пользовательские config/auth. Helpers находятся в ignored `var/p4-cli-clients/`. Приёмка настоящей модели D1 и deployment D2 остаются открытыми.

## Выбранный путь

У OpenCode 1.18.15 не найден поддерживаемый прямой вызов произвольного MCP tool в CLI/HTTP API. Выбираем настоящий `opencode --pure serve`, одну штатную session и ограниченный локальный OpenAI-compatible provider. Он выдаёт function calls; **сам OpenCode** выполняет OAuth/MCP и возвращает ему реальные результаты. Ни SDK-клиент вместо binary, ни вызов внутреннего `MCP.tools()` из другого процесса не засчитываются.

Root принял persistent `serve` на `127.0.0.1:5518`: он сохраняет один живой MCP manager во время ручного редактирования в PWA и отзыва. Каждый этап заканчивается полным ответом session API; пауза человека не удерживает модельный HTTP response. `run` без attach тоже штатно использует in-process server, но новый процесс между этапами заново создаёт MCP client. Для проверки уже подключённого клиента после revoke постоянный `serve` точнее и не требует восстановления процесса.

Это **C2c transport/tool integration**, не D1: конечный автомат не доказывает, что настоящая модель самостоятельно найдёт инструмент, поймёт задачу и выберет правильный key. D1 RU/EN corpus и реальные модели остаются отдельными gates. Нет новой продуктовой сущности, нового grant/ledger, DCR или произвольного исполнителя.

## Что проверено по pinned source

| Поверхность | Проверенное свойство | Следствие |
| --- | --- | --- |
| CLI MCP | `add/list/auth/logout/debug`; команды `call` нет | `list/debug` не заменяют tool execution |
| HTTP MCP | status/add, start/callback/authenticate/remove OAuth, connect/disconnect | Нет публичного direct tool-call route |
| Session HTTP | `POST /session`, затем `POST /session/:id/message` с model/agent/text parts | Настоящий модельный цикл доступен headless |
| Experimental tools | Listing IDs/schemas | Наличие схемы не означает вызов |
| MCP catalog adapter | Реальный SDK `callTool`, executable name `<sanitized server>_<sanitized tool>` | Для `soty` нужны четыре `soty_*` names |
| Model projection | `content[type=text]` объединяется в model output; есть bounded truncation | Проверять actual следующий model request, не только structuredContent на MCP wire |

Первичные источники: [CLI mcp.ts](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/opencode/src/cli/cmd/mcp.ts), [pinned MCP routes](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/opencode/src/server/routes/instance/httpapi/groups/mcp.ts), [session routes](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/opencode/src/server/routes/instance/httpapi/groups/session.ts), [session handler](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/opencode/src/server/routes/instance/httpapi/handlers/session.ts), [catalog.ts](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/opencode/src/mcp/catalog.ts), [session/tools.ts](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/opencode/src/session/tools.ts). Текущие [server docs](https://opencode.ai/docs/server/) и [CLI docs](https://opencode.ai/docs/cli/) согласуются с выбранным маршрутом, но не подменяют pinned source/binary.

## Binary, OAuth и revision

- Выбранный файл: `var/toolchains/opencode-1.18.15-win-x64/opencode.exe`, **178673032 bytes**, SHA-256 `945593162d8f67ba4e901c8e73cd7a22b613fe7159b34a7ae10a1d318b1c6240`. Размер/hash перепроверены чтением файла. Actual `--version 1.18.15` и help ранее зафиксированы в [CLI preflight](p4-oauth-cli-preflight.md); в данном срезе executable не запускался.
- [Pinned package.json](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/opencode/package.json) фиксирует MCP SDK **1.29.0** и bundled `@ai-sdk/openai-compatible` **2.0.41**. [SDK1.29 types](https://raw.githubusercontent.com/modelcontextprotocol/typescript-sdk/v1.29.0/src/types.ts) имеет latest `2025-11-25`; ожидается именно поддержанный продуктом legacy Streamable HTTP. Фактическую revision берём из initialize/последующих запросов. Не подставлять заголовок revision за CLI.
- Root stand: `H=http://127.0.0.1:5517`, issuer `H/oauth`, resource ровно `H/mcp`, static client `soty-opencode-cli`, scope `notes.createDraft`, Code/S256. `oauth.clientSecret` и Authorization header в MCP config отсутствуют. Нет DCR. Root создаёт отдельный fresh home/config и запускает обычный `opencode --pure mcp auth soty`.
- Default callback `http://127.0.0.1:19876/mcp/oauth/callback` уже наблюдался в preflight. Перед открытием authorize root проверяет **actual listener ownership** этого child, не только свободный порт до spawn. Pinned callback helper при занятом порте может просто вернуть управление из `ensureRunning`; чужой listener — controlled stop, не повод завершать чужой process. [Callback source](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/opencode/src/mcp/oauth-callback.ts).
- Все восемь разрешённых authorize parameters должны иметь правильные значения и cardinality; raw state/challenge/code/verifier/URL не выводятся. Returned `iss` от AS должен быть один и равен ожидаемому issuer. **OpenCode callback проверяет state и отдаёт дальше только code; проверки returned `iss` здесь нет.** Проверенный issuer emission не выдаём за client rejection неправильного issuer.
- Callback success HTML появляется до token exchange. PASS подключения требует реального `/token` успеха и следующего авторизованного MCP initialize/list, а не зелёной страницы браузера или самого exit status. [OAuth provider](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/opencode/src/mcp/oauth-provider.ts), [actual authenticate path](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/opencode/src/mcp/index.ts).
- `mcp debug` исключён из acceptance полностью: pinned команда выводит prefix/suffix access token. `mcp logout` — удаление локального OAuth state, не доказательство server-side family revoke. stdout `mcp auth` содержит authorize URL; root удерживает его только в bounded RAM и открывает через private control route.

При Streamable HTTP failure OpenCode может пробовать старый SSE transport. Это не разрешает добавлять deprecated SSE endpoint или считать fallback PASS. Нужен успешный штатный POST profile на `/mcp`.

## Точные файлы и порты harness

Владение этого среза ограничено:

| Файл | Обязанность |
| --- | --- |
| `var/p4-cli-clients/opencode-provider.mjs` | Config factory, конечный model endpoint, безопасные наблюдения actual tool messages |
| `var/p4-cli-clients/opencode-channel.mjs` | Pinned child `serve`, ownership/readiness, supported session API, deadlines/cleanup |
| Этот документ | Контракт, первичные источники и границы доказательства |

Root владеет actual AS/PWA stand, login child, signed owner consent/edit/revoke, wire trace, свежими каталогами/config и общей orchestration. Product/server/domain/reader/UI files не меняются этим harness.

```js
openCodeFixtureConfig({ origin }) // JSON object; root сохраняет только в fresh owned config

createOpenCodeFixture({ origin, onSafeEvent })
// -> { handle(req,res):Promise<boolean>, summary():safe, status():safe, close():void }

await createOpenCodeChannel({ binary, env, cwd, origin, onSafeEvent, onOwnedChild /* optional, sync */ })
// -> { runStage(stage):Promise<{stage,sequence,completed:true}>, summary():safe, close():Promise<void> }
```

`origin` — root AS origin H, не URL child server. Fixed child server — `http://127.0.0.1:5518`; fixed fixture route на root stand — `POST /__qa/opencode/v1/chat/completions`. Root передаёт уже изолированное окружение, личное `process.env` channel не наследует. Config factory экспортируется также из channel для удобства композиции. Safe callback поля: `kind/tool/invocationId/noteId/code/reused/status`; произвольные exception/message/result fields в события не передаются. Отдельный optional synchronous `onOwnedChild(child)` передаёт root живой handle только в RAM registry до startup waits. Сам handle не попадает в summary/event/ошибку.

Factory задаёт единственный enabled provider `c2fixture`, model/small_model `c2fixture/finite`, bundled npm adapter, локальный `baseURL=H+/__qa/opencode/v1`. `apiKey` содержит явную невалидную вне fixture константу `c2-local-fixture-not-a-credential`; настоящих model credentials не требуется. Ничего не устанавливается. Permissions: `*` deny и ровно четыре `soty_*` allow, custom primary agent `c2`, максимум 8 iterations. Не включать `--auto`, shell/task/web/плагины. [Provider config](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/core/src/v1/config/provider.ts), [provider loader](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/opencode/src/provider/provider.ts), [agent/steps](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/core/src/v1/config/agent.ts), [permission schema](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/core/src/v1/config/permission.ts).

Root env изолирует HOME/USERPROFILE/APPDATA/LOCALAPPDATA/XDG/TEMP, задаёт OPENCODE_CONFIG, `--pure`, отключает project config, auto-update, models fetch и file watcher. Enabled providers whitelist не является OS network sandbox. [Config/whitelist](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/core/src/v1/config/config.ts), [flags](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/core/src/flag/flag.ts). OAuth получает только отдельное task-owned credential storage; реальные секреты не передаются модели.

Channel запускает `--pure serve --hostname 127.0.0.1 --port 5518`, с временным Basic password в RAM/child env; не печатает его. До API-запросов проверяет exact owning PID listener через read-only Windows query. Занятый порт не переиспользует и не освобождает чужим kill. Проверяет `/global/health` version, `/mcp` connected, создаёт session с явным title. Все instance API requests используют encoded `directory` query, поэтому кириллический Windows path не попадает в HTTP header. [Serve source](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/opencode/src/cli/cmd/serve.ts), [instance routing](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/opencode/src/server/routes/instance/httpapi/middleware/workspace-routing.ts).

## Конечный model loop и этапы

Model endpoint принимает только `model=finite`, `stream=true`, ожидаемый последний user marker и actual offered function schema. Ответ — конечный Chat Completions SSE: один function tool call либо завершающий assistant text. Это model-side fixture, MCP codec/transport не подменяется. Request/response IDs model protocol синтетические; capability version, Invocation ID, Note ID и receipts никогда не синтезируются.

Следующий шаг разрешён только после фактического следующего model request с `role=tool`, совпавшим `tool_call_id` и реальным TextContent. Повторный прочитанный результат должен быть тем же; отсутствие/изменение результата, незаявленный инструмент, незнакомый stage или неожиданный auxiliary model call — отказ fixture. Нет исходящих network/AS/MCP/DB ports. Search/get discovery version берётся из actual RU/EN результатов, а затем сверяется с detail.

| `runStage` | Реальные вызовы / обязательное наблюдение |
| --- | --- |
| `discovery` | `soty_catalog_search` RU `записка`, затем EN `note`; `soty_catalog_get` с выбранными из search `capabilityId/version`; schema дошла до следующего model request |
| `create` | Один `soty_notes_create_draft`: уникальные синтетические, **различные** title/body и стабильный key; получить actual Invocation/Note ID и revision1 |
| `get` | `soty_invocations_get` только с actual returned ID; исторический receipt без исходных title/body |
| Пауза root | Открыть созданную записку в настоящей PWA и сохранить человеческую правку; ничего не отдавать fixture из DB |
| `replay`, затем `get` | Точные прежние title/body/key, `reused=true`, те же ID/revision1; root проверяет один effect/spent1 и сохранённую правку |
| Пауза root | Signed owner отзывает именно эту OAuth connection; сохраняется тот же CLI/session/MCP manager |
| `revoked_create`, `revoked_get` | Точный повтор исходных title/body/key и get прежнего ID должны получить настоящий отказ: revoked actor не получает даже исторический receipt; старая записка остаётся |

Точная последовательность root — семь завершённых этапов: `discovery → create → get → replay → get → revoked_create → revoked_get`. Второй `get` идёт после человеческой правки и replay. Повтор названия `get` имеет новый монотонный sequence и отдельный model/user marker; исходный input и исторические IDs остаются прежними. Fresh-intent negative после revoke в этот C2c сценарий не входит.

Lifecycle проверки разделены: provider принимает следующий stage только после прочитанного фактического tool result и выдачи terminal model marker предыдущего stage. `provider.summary().calls` хранит отдельные stage/sequence/tool/resultDigest и safe readback. Это ещё не получение marker клиентом: `channel.runStage` отмечает completion лишь после полного штатного session response с точным terminal marker. PASS требует всех семи client completions и соответствующего root wire/effect oracle; provider completion отдельно недостаточен.

Fixture record о client error сам по себе не доказывает отзыв: root обязан сопоставить настоящий MCP 401/403/current authority failure и неизменные effect counts. При automatic refresh/новом authorize после revoke тест **не даёт нового согласия**. Если клиент вместо tool error повис/завершился, это controlled incomplete client stage с фактическим wire refusal, а не подставленный успешный tool result. Unknown effect после transport failure не повторять с новым key. Channel на неопределённой stage failure не запускает следующий эффект автоматически.

Вся model conversation уже содержит исходный body из tool arguments. Privacy oracle применяется отдельно к конкретному `invocations_get`/create receipt output, а не требует отсутствия body во всей беседе. Root wire trace отдельно проверяет структурное равенство text/structured JSON, client/domain error codes и отсутствие private body; порядок JSON keys не является различием DTO. Receipt не доказывает существование или текущее содержимое Note.

## Bounds и cleanup

- Model fixture: один активный запрос, до 32 requests/12 stages, до 8 model turns на stage; body1MiB, чтение10s, tool text512KiB, response16KiB. Один input staging buffer; нет массива tiny chunks. Отказ fixture фиксируется безопасным кодом и закрывает дальнейшую выдачу. Это конкретный небольшой каталог/сценарий, не стресс-тест максимально большого catalog detail/truncation.
- Channel: startup30s; обычный API request5s, MCP readiness20s, stage60s; response2MiB; lifetime20min для операторских пауз; stdout/stderr по64KiB, данные немедленно отбрасываются. Provider request timeout15s, header/chunk10s. Нет бесконечного ожидания человека внутри запроса модели.
- На close/lifetime/failure: abort своих requests, bounded session abort, terminate только exact spawned child, wait exit/close, закрыть свои pipe handles. Не удалять auth/config, не завершать процессы по имени, не закрывать пользовательские browser windows. Неполный cleanup явно отражается, а не объявляется успешным.
- Summary/events содержат только safe codes/status/counts/digests, выбранные public capability/version и синтетические actual Invocation/Note IDs. Нет исходного title/body/key, OAuth/Basic/model headers, auth files, raw stdout/stderr/HTTP errors. Не писать сырой model transcript в receipt или harness log.

## Приёмка и текущие ограничения

Перед исполнением root/critic читают оба ignored файла и фиксируют SHA. Первый согласованный run — настоящий login/consent/token и `discovery`; затем последовательно остальные этапы на том же процессе. Root проверяет wire revision/resource, текстовую проекцию, actual owner PWA, read-only store effect/budget counts и revoke. Нельзя засчитать отсутствие второй Note при локальном permission/fixture failure как server rejection.

Конкретные ещё не проверенные границы: acceptance pinned binary config; actual emitted model request/response serialization; MCP outputSchema/local refs в SDK1.29; сохранение live manager через human pause; callback/token/refresh/revoke в CLI. Root C1 HTTP/SDK gates не заменяют этот run. Упомянутые API взяты из pinned source, но это пока не успешные вызовы данного executable.

Наблюдения, требующие честного ограничения: callback `iss` не проверяется OpenCode в указанном source; browser callback success раньше token exchange; old SSE fallback не наш transport; debug раскрывает token fragments; model output может truncation. Эти особенности не исправляются скрытым патчем binary, ослаблением AS policy или synthetic receipts. Материальный runtime failure получает отдельный causal finding и узкое решение перед повтором.

Этот документ и подготовленные helpers не закрывают D1 real-model corpus, внешний HTTPS/production release, все версии OpenCode или все ИИ-клиенты. Новый runtime/полные OAuth/tool results пока не заявлены.

## Source checkpoint до исполнения

На первом source checkpoint два `node --check` на изолированном Node24.21.0 завершились с exit0; импорт/HTTP/CLI lifecycle этим не проверялся. Затем по root review удалён отдельный negative input/key: `revoked_create` теперь использует тот же immutable `input`, что create/replay. Эта узкая поправка перечитана без runtime/test/syntax запусков. Channel в этой поправке не менялся, product diff отсутствовал. Исторический exact-key freeze для чтения root/critic:

| Ignored file | Bytes | SHA-256 |
| --- | ---: | --- |
| `var/p4-cli-clients/opencode-provider.mjs` | 13630 | `cc8f30542f420d789dfc74bc048a7b1e3fd1aa9ad2ff1236f77fc387ab6a0990` |
| `var/p4-cli-clients/opencode-channel.mjs` | 9601 | `31a53dffae351355e7812f57ea7eff9f474b70894bf896324c929e3adc3e9e10` |

Следующее исполнение — только после согласования root и свободного serial slot. Live binary/config/profile ошибки, если возникнут, сохраняются как causal RED; этот source checkpoint не делает предположения об их отсутствии.

## Узкая поправка cleanup перед исполнением

Независимый source review обнаружил, что channel ошибочно приравнивал любое событие child `error` к exit. По [первичной Node24.21 документации](https://nodejs.org/download/release/v24.21.0/docs/api/child_process.html#event-error) это событие может означать также неудачный kill; оно не подтверждает завершение процесса. `close` отдельно подтверждает окончание работы и закрытие stdio.

Исправление только channel: отдельные `spawned/spawnFailed/exited/stdioClosed/childError`; pre-spawn error без PID — не созданный child, post-spawn error не разрешает `childDone` и не ставит `exited`. Ошибка сигнала не прекращает bounded cleanup. `cleanupConfirmed` становится true только после настоящего exit либо подтверждённого no-PID spawn failure, **и** после `close` stdio. Незавершённый cleanup остаётся false.

`onOwnedChild` вызывается после установки lifecycle listeners, до startup HTTP. Root сохраняет handle в общем registry до возврата factory. Если этот синхронный observer бросает исключение, channel выполняет bounded cleanup; не теряет PID и не передаёт raw exception. При factory failure после spawn наружу идёт новый фиксированный `opencode_startup_failed` либо `opencode_owned_child_observer_failed` с безопасными `.pid` и `.cleanupConfirmed`. Synchronous spawn failure без созданного child — `opencode_spawn_failed`, `.pid=null`, `.cleanupConfirmed=true`. Close failure — `opencode_cleanup_incomplete` с теми же безопасными полями. Root обязан удерживать pending ownership при false, даже если factory не попала в `channels`.

Channel summary показывает отдельные lifecycle flags и actual cleanup verdict; `closed` означает запрос на закрытие, а не сам по себе доказанный exit. Provider с exact original-input/key не менялся. Только обновлённый channel прошёл `node --check` на Node24.21 с exit0; actual spawn/kill/observer-failure tests, auth/serve/run не запускались.

| Текущий файл | Bytes | SHA-256 |
| --- | ---: | --- |
| `var/p4-cli-clients/opencode-provider.mjs` | 13630 | `cc8f30542f420d789dfc74bc048a7b1e3fd1aa9ad2ff1236f77fc387ab6a0990` |
| `var/p4-cli-clients/opencode-channel.mjs` | 11354 | `8f6e50cb7ae249a69edb1352e0b2bb6c9d1d2f862ebf69c75a5c74b3914e0678` |

### Handoff процесса проверки порта

Последующий read-only review нашёл тот же ownership gap у PowerShell, запускаемого `ownedListener` через promisified `execFile`. По [Node24.21 execFile contract](https://nodejs.org/download/release/v24.21.0/docs/api/child_process.html#child_processexecfilefile-args-options-callback) возвращённый Promise содержит `.child`; timeout посылает сигнал и сам по себе не доказывает exit.

Узкая дельта: до `await pending` channel синхронно вызывает `onOwnedChild(pending.child, 'channel-probe')`. Используется тот же root RAM registry, нового реестра нет. Throw observer поглощает последующий promise rejection, запрашивает kill exact probe и прерывает startup; root сохраняет actual handle для подтверждения exit/close. Channel `.cleanupConfirmed` относится к основному `serve`; общий finish root отдельно требует завершения **всех** зарегистрированных probe/auth/channel children. Root callback по контракту синхронный.

Обновлённый channel прошёл только `node --check` (Node24.21, exit0). Provider прежний `cc8f3054…0990`; auth/serve/PowerShell probe и tests не исполнялись. Текущий channel: **11756 bytes**, SHA-256 **`4b0fcafae3c37b95dc84dd61de82ee7bce5c2da46de219ef03849e69cfbc0583`**. Предыдущие checkpoint hashes выше сохранены как история review.
