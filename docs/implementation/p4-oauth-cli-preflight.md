# P4 C1: независимая проверка запросов OAuth выбранных CLI

Статус: выбранные **Codex 0.153.4 и OpenCode 1.18.15 фактически сформировали ожидаемые authorize parameters** после настоящих loopback PRM/AS requests. Первоначальный Codex profile выявил duplicate-resource defect; исправленный профиль использует resource из discovery. В финальных двух запусках browser GET не наблюдался за 45 секунд, поэтому это ограниченное доказательство constructed CLI requests. Полный OAuth gate остаётся открытым: consent, code/token, refresh/revoke и MCP tools здесь не выполнялись.

## Точная граница

Проверяется запрос, который выбранный CLI сам формирует для открытия в браузере: configured client ID, callback, resource, scope, Code + S256 и наличие state/challenge. Наблюдение самого browser GET отмечено отдельно для каждого запуска. Sink не является настоящим AS: он никогда не выдаёт code/token, не предлагает consent и не обращается к callback. Browser windows пользователя не закрываются. Public client ID не аттестует официальный executable; hash здесь фиксирует только проверенный локальный файл.

Реализация preflight находится только в task-owned `var/p4-oauth-cli-preflight/`. Личные config/auth/keyring не читаются. Child env создаётся с нуля: из родителя берётся только `SystemRoot`; HOME/USERPROFILE/APPDATA/LOCALAPPDATA/XDG/CODEX_HOME/TEMP указывают на новый каталог каждого запуска. Codex использует file credential stores и untrusted project config. OpenCode использует свой config, pure mode, отключённые project config, auto-update и model-catalog fetch; список enabled providers пуст. Это изоляция конфигурации, а не заявленный OS network sandbox.

## Проверенные binaries и help

| CLI | Actual version | Bytes | SHA-256 |
| --- | --- | ---: | --- |
| Codex CLI | `0.153.4` | 295408944 | `444a3f0008050605cae73cd9b7a2dcac61294062dfaab56dd20430fd6498518b` |
| OpenCode | `1.18.15` | 178673032 | `945593162d8f67ba4e901c8e73cd7a22b613fe7159b34a7ae10a1d318b1c6240` |

Команда `var/toolchains/node-v24.21.0-win-x64/node.exe var/p4-oauth-cli-preflight/runner.mjs help` завершилась с exit 0. Все семь последовательных version/help calls завершились с exit 0. Codex help содержит `mcp login`, `--oauth-client-id`, `--oauth-resource`, `--scopes`; OpenCode — `mcp auth` и `--pure`. В обоих help нет no-browser option. Help receipts:

- `var/p4-oauth-cli-preflight/codex-f3a425a6-a971-4b0c-8350-69515737027f/help-receipt.json`
- `var/p4-oauth-cli-preflight/opencode-d8e16191-586f-462f-a6b2-881e9dfd382b/help-receipt.json`

## Authorize harness и наблюдения

`var/p4-oauth-cli-preflight/authorize-harness.mjs` перед исполнением требует свой reviewed SHA и проверяет SHA выбранного binary. Собственный listener привязан к `127.0.0.1:ephemeral`; Host обязан точно совпасть. PRM и AS metadata объявляют только этот origin, `/mcp`, `/oauth/authorize` и всегда отказывающий `/oauth/token`. Registration endpoint отсутствует.

Deadline CLI — 45 секунд; stdout/stderr ограничены 64 KiB в памяти и не сохраняются harness. Вывод и receipt содержат только разрешённые поля и проверки. Значения state/challenge, полный authorize URL, auth files и raw child output не печатаются. Callback query не сохраняется и означает непрошедшую проверку. Cleanup завершает только exact CLI child и собственные listener/sockets, без закрытия браузера, поиска процессов для убийства по имени или удаления пользовательских файлов. Синтетическое PKCE state может оставаться в собственном файловом хранилище CLI; такие auth files не читаются и не публикуются.

Первый Codex run (`codex-authorize-16194bb3-acd9-4e7e-8311-aaa14320071d`, harness `fef151…`) прочитал PRM/AS, напечатал authorize URL, но за 45 секунд GET не поступил. Старый harness ждал `close` вместо отдельного `exit`, остался собственный Node process; он завершён после проверки exact identity PID 42032. Browser processes не затрагивались. Исправление harness отдельно учитывает exit/close и закрывает только собственные stdout/stderr handles. Это дефект probe cleanup, не OAuth продукта.

Следующий reviewed harness `1afb30bbe593351ae83c2920f4076b346eb2ab6251ae2b73910dd42ccee7603c` добавил explicit Codex `callback_url="http://127.0.0.1/callback"`. Actual GET пришёл на sink: client `soty-codex-cli`, callback `http://127.0.0.1:53736/callback`, `notes.createDraft`, Code/S256, state/challenge present. Но `duplicates=true`, exact-one resource check false. Этот запуск — **не PASS профиля**. Receipt: `var/p4-oauth-cli-preflight/codex-authorize-47b77ba3-4f7e-487f-a451-5e88160170d8/authorize-receipt.json`.

Причина подтверждается pinned source: [Codex Cargo.lock](https://raw.githubusercontent.com/openai/codex/rust-v0.153.4/codex-rs/Cargo.lock) фиксирует rmcp 3.1.3; [его authorize builder](https://raw.githubusercontent.com/modelcontextprotocol/rust-sdk/rmcp-v3.1.3/crates/rmcp/src/transport/auth.rs) уже добавляет discovered resource. [Codex login](https://raw.githubusercontent.com/openai/codex/rust-v0.153.4/codex-rs/rmcp-client/src/perform_oauth_login.rs) затем без dedup дописывает explicit `oauth_resource`. Требуемая поправка клиентской инструкции — убрать override и использовать PRM discovery. Strict duplicate rejection AS сохраняется.

OpenCode на том же reviewed harness сформировал exact request с `soty-opencode-cli`, callback `http://127.0.0.1:19876/mcp/oauth/callback`, собственным resource `/mcp`, `notes.createDraft`, Code/S256; state/challenge present, все проверки true и duplicates false. Источник доказательства — безопасная проекция actual CLI stdout после настоящих PRM/AS запросов. `browserAuthorizeVisited=false` за 45 секунд; причина не установлена, browser policy не обходилась. Exact child exit/close подтверждены. Receipt: `var/p4-oauth-cli-preflight/opencode-authorize-d248b152-8393-464a-ac33-4349696c782f/authorize-receipt.json`.

Финальный reviewed harness `e316282014d2e4adcee75b233b5c02d734ab5db1215ed6d586efe3efcc655205`, syntax PASS, убрал Codex `oauth_resource` и добавил только безопасные cardinalities известных query keys и проверку совпадения всех resource values. Единственный согласованный repeat сформировал callback `http://127.0.0.1:55943/callback`, resource `http://127.0.0.1:55939/mcp`, configured client, scope и Code/S256; все восемь параметров присутствуют ровно один раз, `resourceValuesAllMatchOwnEndpoint=true`. Источник — actual CLI stdout; `browserAuthorizeVisited=false` за 45 секунд. Exact CLI exit SIGTERM подтверждён, собственные pipe handles закрыты, Node harness завершился. Receipt: `var/p4-oauth-cli-preflight/codex-authorize-6cba3bfb-3c20-4426-ad4b-64ccd1f82aea/authorize-receipt.json`.

Итоговая таблица не объединяет различные уровни доказательства:

| Профиль | Фактический callback | Resource | Параметры | Browser authorize GET |
| --- | --- | --- | --- | --- |
| Codex, explicit callback, без `oauth_resource` | `http://127.0.0.1:55943/callback` | собственный `/mcp` из PRM | Все восемь exact-once, Code/S256, заданные client/scope | Не наблюдался |
| OpenCode, configured clientId/scope | `http://127.0.0.1:19876/mcp/oauth/callback` | собственный `/mcp` из PRM | Exact ожидаемые значения, state/challenge present, дублей нет | Не наблюдался |
| Исторический Codex, explicit `oauth_resource` | `http://127.0.0.1:53736/callback` | Exact-one check отказал | Duplicate query, профиль отклонён | Наблюдался один GET |

Наличие callback URL не проверяет доступность callback listener, issuer validation или code exchange: harness туда не обращался. Source `webbrowser::open`/`browser.open` подтверждает попытку открыть браузер; отсутствие observed GET не объясняется догадкой о manual-only режиме, browser policy или OS.

Финальная read-only проверка exact owned PIDs не нашла оставшихся процессов; `var/p4-oauth-cli-preflight/cleanup-receipt.json`. Browser windows оставлены пользователю. Запуски, не увидевшие GET, сохранили непрошедший общий sink verdict; зелёными названы только перечисленные проверки параметров, не CLI login exit status.

## Документируемый клиентский профиль

Для Codex сначала задаётся профиль (URL ниже — символический origin `H`, не выполняемая команда), затем обычный `codex mcp login soty --scopes notes.createDraft`:

```toml
[mcp_servers.soty]
url = "https://H/mcp"
scopes = ["notes.createDraft"]

[mcp_servers.soty.oauth]
client_id = "soty-codex-cli"
callback_url = "http://127.0.0.1/callback"
```

Для pinned Codex **не добавлять `oauth_resource`/`--oauth-resource`** при этом PRM. Explicit portless callback даёт наблюдённый `/callback` с назначенным loopback port. Source default без configured callback может добавлять server-specific path suffix; его нельзя молча зарегистрировать как `/callback`.

OpenCode использует remote MCP config с `oauth.clientId="soty-opencode-cli"`, `oauth.scope="notes.createDraft"` и `url=H+/mcp`; actual `opencode --pure mcp auth preflight` сформировал default callback `/mcp/oauth/callback` на `127.0.0.1:19876`. В preflight server был `enabled:false`, чтобы избежать фонового подключения до явной команды auth; команда auth выполнила PRM/AS discovery. Это не проверка последующего enabled MCP connection.

Будущая регистрация разрешает только проверенные loopback host/path и стандартную port variation. `localhost`, иной path/query, wildcard и client ID как признак доверенного binary этим результатом не разрешаются. Нужны последующие actual Provider + existing Connect consent, token/refresh/revoke и выбранные CLI against implemented MCP; ни один такой gate не закрыт этой квитанцией.

## Первичные источники и предел вывода

[OpenAI config reference](https://developers.openai.com/codex/config-reference/) описывает file credential storage, pre-registered client ID, resource и callback options. [Configuration precedence](https://learn.chatgpt.com/docs/config-file/config-basic#configuration-precedence) отдельно объясняет trusted project config. Эти текущие документы не заменяют фактический запрос pinned Codex 0.153.4.

В pinned [OpenCode oauth-provider.ts](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/opencode/src/mcp/oauth-provider.ts) есть configured clientId и default loopback callback; [MCP authenticate](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/opencode/src/mcp/index.ts) вызывает [browser.open](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/opencode/src/mcp/browser.ts). `mcp debug` с пустым onRedirect не подменяет требуемый actual CLI authorization flow.
