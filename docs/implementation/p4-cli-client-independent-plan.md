# P4-C2c: независимая приёмка настоящих клиентов

Статус: **read-only acceptance/harness design, исполнения нет**. Основание — принятый C2b `2103d755c081f03cdad98959fe593c6151ea2eec`, [root подплан](p4-cli-client-plan.md), [MCP контракт](p4-mcp-implementation-plan.md), [ранний CLI preflight](p4-oauth-cli-preflight.md) и [фиксированный P4-D1 корпус](p4-external-agent-preflight.md), раздел «24 пользовательские задачи». Корпус остаётся неизменным.

Reviewer изменяет только этот новый документ. Production, configs, auth files и данные не менялись; CLI/AS/model/tests не запускались. Future harness требует отдельного read-only source review и root GO. Успех ниже означает совместимость двух выбранных executable с OAuth/MCP/Notes, не «любые ИИ готовы».

## Самый короткий настоящий путь

Один свежий task-owned AS/PWA stand и по новому изолированному home/config для Codex **0.153.4** и OpenCode **1.18.15**. Клиенты проходят последовательно, с отдельными connection/grant и синтетическими input/key. Допустим общий синтетический owner, если exact client/connection/effect attribution сохраняется. Пользователь использует существующие consent, Notes и AccessPanel; отдельная панель QA не становится продуктовой функцией.

Предпочтителен штатный прямой tool-call интерфейс самого выбранного executable. Для Codex автор уже сообщил наличие `mcpServer/tool/call` в pinned generated schema; до исполнения ещё нужно source-backed подтвердить путь через тот же configured MCP manager/OAuth store. Такой app-server RPC — настоящий клиентский transport path, но не модельный выбор инструмента. Внутренний TypeScript SDK или HTTP вызов из parent harness вместо клиента не засчитывается.

Если у выбранного клиента такого интерфейса нет, допустим **deterministic loopback model-provider fixture** со следующими условиями:

- Fixture выдаёт только следующий вызов по фактически предложенному клиентом имени/schema и принимает реальный результат в следующем клиентском запросе. Для `catalog_get` выбирает capability/version из фактического search; для `invocations_get` — Invocation ID из фактического create. Выдуманный ID, seed receipt и чтение SQLite для подсказки следующего вызова запрещены.
- Fixture сама не вызывает AS, MCP, Capabilities или Notes. Все четыре tools проходят executable → действующий MCP → действующий домен → executable → fixture. Нет замены auth/readiness, HTTP ответов или tool implementations. Обычный клиентский discovery/proxy wrapper допустим лишь как его подтверждённый штатный интерфейс.
- Используется именно поддержанный данным pin inference protocol. Рабочий Chat Completions fixture не доказывает Responses compatibility Codex; это прямо разделяет [OpenAI gateway documentation](https://learn.chatgpt.com/docs/enterprise/gateway-compatibility#requests-and-endpoints). Current docs не заменяют pinned source и actual wire.
- Результат называется **CLI transport/tool integration с детерминированным ответом модели**. Умение модели понять просьбу, выбрать действие, сохранить key после потери ответа и безопасно отказаться не доказано. D1 сохраняет все 24 RU/EN задачи для каждого из двух настоящих model profiles; D2 сохраняет внешний HTTPS/release/backup/restore.

## Минимальная приёмка для каждого executable

| Gate | Настоящее доказательство | Что не засчитывается |
|---|---|---|
| 1. Изоляция и pin | Hash/version выбранного binary, новый home/config/cwd, file credential store, отсутствующие личные auth/env/keyring; manifest точных task-owned paths/processes | CLI help, новое окно со старым profile или наличие пакета |
| 2. OAuth | CLI сам делает PRM/AS discovery и authorize; настоящий signed owner consent, callback именно CLI, code exchange и его действующий bearer. Локально проверены exact-one client/resource/scope/response type, S256, state/challenge, redirect и AS-emitted `iss`; client-side issuer validation атрибутируется отдельно | Ранее captured authorize URL, login exit0 без exchange, SDK auth, seeded approval/token injection |
| 3. MCP | На wire наблюдены фактическая revision, initialize/list и ровно четыре объявленных инструмента. Клиент не переключился незаметно на неподдерживаемый старый SSE endpoint | Версия SDK, утверждение о modern revision из документации, ручной вызов endpoint вместо executable |
| 4. Цепочка данных | RU/EN catalog search → exact catalog detail → create → get; реальные schema/ID/result достигают штатного клиента. При model fixture проверяется конкретный returned tool content, а не только серверный log | Четыре заранее сгенерированных «успеха», prose без ID/schema, проверка лишь `tools/list` |
| 5. Человек продолжает | PWA открывает Note нужного owner; человек меняет синтетический body и сохраняет/reload. Точный replay прежних input/key возвращает тот же Invocation/Note; отдельный get содержит исторический receipt, не новый текст | Успех ответа без durable effect, чтение текущей Note через тестовый shortcut, повтор с новым key |
| 6. Текущие права | Exact connection/grant отозван владельцем. Та же клиентская сессия получает отказ, не новый effect или private receipt. Note и человеческая правка остаются | Только UI badge, остановленный процесс вместо отказа, новое согласие после revoke, отсутствие нового Note без наблюдённого запроса |
| 7. Завершение | Final safe trace + RO counts/digests согласуются: одна admission/Note/proof/трата на одно намерение, replay не увеличивает их; exact child exits и собственные listeners закрыты | Один общий зелёный итог, потерянный child, raw transcript с секретами или приписанная cross-store atomic snapshot |

Сначала проверяется один и тот же короткий путь целиком в Codex, затем в OpenCode. Не требуется повторять внутри C2c все принятые C1/C2a/C2b fuzz/concurrency/crash cases. Межклиентский чужой Invocation может дать дополнительный current-authority oracle без нового account, если он включён в конечный manifest; он не заменяет D1 foreign-account сценарии.

## Существенные ловушки harness

**OAuth остаётся реальным.** Callback и authorize URL создаёт клиент. Harness вправе безопасно открыть только проверенный own-origin URL этого клиента, но не заменять state/PKCE/resource/callback и не отправлять callback/code за него. Codex profile сохраняет explicit portless `http://127.0.0.1/callback` и resource из PRM без прежнего duplicate `oauth_resource`. OpenCode использует проверенный `/mcp/oauth/callback` и свой listener port. Занятый port — контролируемый отказ, не wildcard redirect. [Official Codex callback guidance](https://learn.chatgpt.com/docs/extend/mcp#oauth-client-registration-and-callbacks) подтверждает связь callback с issuer metadata и проверкой `iss`; actual pinned поведение фиксируется отдельно. Новый client ID не аттестует официальность executable.

В [pinned OpenCode callback](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/opencode/src/mcp/oauth-callback.ts) проверяются state/code, но `iss` не читается и `pending.resolve` передаёт только code. Поэтому exact AS `iss` emission нельзя назвать OpenCode wrong-issuer rejection. Это явное клиентское ограничение перед внешним/multi-AS gate; AS policy не ослабляется. Там же `ensureRunning` при занятом port просто возвращает управление. Перед открытием authorize проверяется **фактический listener owner** — exact CLI child/owned descendant. Один free-port probe до старта или успешный TCP connect не доказывает ownership; чужой listener не переиспользуется и не завершается нашим cleanup.

**Revocation не превращается в новую авторизацию.** CLI может после 401 попытаться refresh или открыть login. Наблюдаем это как поведение клиента, сохраняем отказ и останавливаем bounded сценарий без нового owner consent. Не стираем auth cache ради «успешного повтора». Refresh можно назвать выполненным только после настоящего refresh request самого CLI; restart/login и ручной HTTP refresh не равнозначны. Не наблюдённый refresh остаётся явно непокрытым этим smoke, без подмены уже имеющегося C1 evidence.

**Receipt не является телом Note.** Model conversation уже содержит исходный synthetic body из create. Поэтому privacy oracle проверяет именно `invocations_get` tool-result envelope, включая его текстовую проекцию: без input/current title/body и без новой edit-canary. Нельзя получить ложный FAIL общим grep всего контекста или ложный PASS отсутствием body только в финальной фразе CLI. Root RO observer знает edit digest независимо от fixture; model fixture не получает current Note из БД.

**Неизвестный outcome сохраняется.** Timeout/обрыв не разрешает новую выдачу, новый consent или новый create key. Повтор Notes, если предусмотрен сценарием, использует тот же captured input/key. Для C2c достаточно наблюдённого exact replay; model lost-ACK decision остаётся отдельным обязательным D1 кейсом. Если в harness включается потеря ответа, first failure и point/count вмешательства фиксируются отдельно, а ответ не заменяется фиктивным JSON.

**Изоляция не равна OS sandbox.** Child получает whitelist environment и собственные config/home/cache/temp/cwd, без наследования model credentials, repository configs/skills/hooks и auto-update. Доступные клиенту built-in shell/web/file tools не выбираются fixture; их наличие не объявляется enforcement. Нет глобальной установки, изменения личного keyring или чтения его содержимого. Auth files task-local всё равно содержат настоящие синтетические credentials: не dump/backup в plaintext, не архивировать вместе с общим log. `mcp debug` и похожие команды, печатающие части tokens, исключаются до запуска.

**Ресурсы конечны до запуска.** Manifest задаёт числовые body/output/request/turn/time caps, один активный CLI, exact loopback endpoints и отдельный конечный deadline ручного consent. Неиспользуемые model/catalog/telemetry/update destinations отключены поддержанным config; неизвестный запрос к fixture не получает успешную заглушку. Model provider имеет bounded raw reader и response, не хранит весь transcript. Переполнение — FAIL с безопасной причиной, не тихий рост cap или бесконечный retry. Cleanup различает child exit и закрытие inherited pipe handles, убирает только exact owned children/listeners/handles; пользовательские browser windows не закрывает. Перед повтором проверяется завершение прежнего child, а не только новый PID.

## Что сохранить и что проверить перед GO

Safe receipt содержит pin/config/harness hashes; версии; actual issuer/resource/profile и callback host/path/port без query; protocol revision; method/tool/status/counts; synthetic Invocation/Note IDs; input/edit/result hashes и boolean comparisons; exact cleanup outcomes. OAuth state, challenge, verifier, code, token, cookie, auth headers, raw authorize URL и Note body не сохраняются даже частично. Проверка секретов проводится локально и выдаёт только boolean, не hash/fragment секрета. stdout/stderr сначала bounded memory-only и allowlist projection; redaction после записи raw log не принимается.

До root GO reviewer читает exact harness bytes, оба client contracts и owner/model fixture, сверяет endpoints/path containment, поддержанные flags, callback opening, lifetime и отсутствие скрытых прямых MCP/DB подсказок. После исполнения отдельно атрибутируются настоящий executable, synthetic model driver (если нужен), реальный домен/PWA и внешний observer. Сейчас ни один новый executable gate этим документом не закрыт.

## Первый foundation source review — до запуска

Прочитаны целиком root `var/p4-cli-clients/{stand,cli-controller,wire-trace}.mjs`. Channel-файлы в этот момент ещё готовились и в данный отзыв не входят. Auth/server/model/tests reviewer не запускал. Это WIP review, не approval будущего совокупного harness; первоначальный план выше не выдаётся за исполнительную квитанцию.

Обнаружены и переданы root четыре точечных исправления:

1. `catalog_get` действительно возвращает schemas внутри `capability`. Проверка `detail.value.inputSchema/outputSchema` в первом controller остановила бы нормальный discovery. Root уже исправляет путь на `detail.value.capability.*`; production catalog менять не требуется.
2. Общий `controller.close()` должен дождаться фактического exit своих login children и завершить собственный PowerShell listener probe. Первый вариант только посылал kill, после чего stand signal handler мог завершить parent с exit0. Подтверждённый channel cleanup не заменяет cleanup остальных owned процессов.
3. Запись trace создавалась на response `finish`; индекс массива до RPC не доказывает, что отказ относится к **новому** запросу данного RPC. Требуется монотонный request-start marker/ограниченный stage context; ранее начавшийся background request не должен засчитываться как новый refusal. При раннем auth отказе body/method могут отсутствовать — это нормальный transport refusal, не выдуманный MCP `isError` ответ.
4. `completed=true` после любого состояния `ready` позволял назвать клиент завершённым даже после одной discovery. Итог должен проверять полный required набор PASS, включая create/get/replay и оба revoke обращения, либо явно называть флаг только фактом закрытия. Root final durable/PWA audit остаётся отдельным от этого флага.

Положительные границы уже видны в source: новый store без owner seed, отдельная копия dist и source pins, ключи AS в памяти, отдельные CLI homes/environment, 64KiB login output только в RAM, проверка exact callback PID перед ручным authorize, real binary RPC и safe trace без raw OAuth values. Эти свойства ещё требуют итогового чтения channel/config composition и actual execution. CLI может сам открыть браузер раньше ручного QA link; гарантию «до любого callback/consent проверен listener» нельзя выводить только из guard этого link — способ запуска/ручной consent должен сохранять эту границу в окончательном сценарии.

Root исправляет собственные helpers; reviewer не меняет их. После общего refreeze нужны новые exact hashes и отдельный final verdict, а не перенос approval на изменившиеся WIP bytes.

## Полный review композиции до исполнения

Прочитаны целиком все шесть helpers: `stand`, `cli-controller`, `wire-trace`, `codex-channel`, `opencode-channel`, `opencode-provider`, а также root подплан и оба client contracts. Codex параметры `mcpServer/tool/call`, форма ответа и `mcpServerStatus/list` дополнительно сверены с сохранёнными actual generated schemas закреплённого executable. Reviewer не импортировал helpers, не запускал CLI/server/auth/model/tests и не менял чужие исходники. Одни syntax checks авторов не объявляются исполнением.

Четыре замечания первого foundation review закрыты в прочитанной композиции: schemas берутся из `detail.capability`; login и PowerShell callback probe зарегистрированы для ожидания exit; MCP trace захватывает монотонный request-start и action ID до обработки; `completed` требует ровно семь PASS в согласованном порядке. Дополнительная root delta `cli-controller` `c4b10417f944781bfc0bece6c2a139ba976acff13886890abb367de077da612a` требует для OpenCode negative stage нового POST 401/403 того же action и для get — фактического `invocations_get` envelope без private body. Marker модельного fixture и его распознанный `client_error` отдельно недостаточны. Исходный input/key используется и после revoke.

Изоляция и доказательные границы согласованы между файлами. Root создаёт новые stores и dist, без owner seed; реальные AS keys остаются в stand RAM, CLI credential files — только в собственных profiles. Codex получает отдельный whitelist env без `OPENCODE_*`, static empty model catalog и отказной `H/__qa/no-model/v1`. Его прямой RPC не требует модели. OpenCode использует одну настоящую session/manager и finite model-side fixture, который не имеет outgoing AS/MCP/DB port; реальные version/Invocation/Note IDs берутся из прочитанных tool results. Body/request/stdio/stage/lifetime caps заданы до запуска. Это конфигурационная изоляция, не OS network sandbox.

Перед первым запуском найден ещё один связанный **cleanup blocker**. В прочитанных первоначальных channel pins (`codex-channel` `7b05be7d…`, `opencode-channel` `31a53dff…`) событие `ChildProcess.error` безусловно помечало процесс как не запущенный/завершившийся. По [Node24.21 API](https://nodejs.org/download/release/v24.21.0/docs/api/child_process.html#event-error) оно возникает также при невозможности послать kill уже существующему child; фактический exit из него не следует. Такой путь мог дать ложный cleanup PASS. Кроме того, startup failure до `channels.set` терял управление child: Codex возвращал safe `pid/cleanupConfirmed`, но controller их отбрасывал; OpenCode подавлял cleanup error без этой проекции. Требуется различать no-PID spawn failure и post-spawn error, сохранить неопределённый cleanup в root state и не выдавать успешный finish/stand exit0. Это source-backed finding до исполнения, не runtime RED. Передано всем трём владельцам; reviewer не исправлял helpers.

Фактический auto-open самого CLI не перехватывается guard ручной QA ссылки. Оператор обязан проверить actual owned callback listener до нажатия Allow даже в автоматически открытом окне; чужое окно/процесс не закрывается. AS-emitted exact `iss` и успешный exchange/авторизованный MCP наблюдаются отдельно: отсутствие проверки callback `iss` в pinned OpenCode не переименовывается в issuer validation. Post-revoke refusal остаётся только отказом после конкретного запроса, не фиктивным MCP success/error response. Финальная human edit/durable counts/revision/budget/cleanup сверка root остаётся обязательной.

На этом промежуточном снимке весь functional/source review завершён; общий pre-run verdict ожидает узкий cleanup refreeze. Ни C2c executable PASS, ни D1 модельный корпус, ни D2 HTTPS/restore данным review не закрыты.

## Final pre-run admission — exact refreeze

**Source admitted для согласованного root последовательного запуска; открытых блокеров не осталось.** Это результат независимого полного чтения композиции и последующих точечных дельт, а не executable PASS. Reviewer не запускал helpers/CLI/auth/model/tests, не импортировал их и не менял product либо авторский source. До запуска заново вычислены SHA всех шести helpers и трёх contract документов:

| Файл | Bytes | SHA-256 |
| --- | ---: | --- |
| `var/p4-cli-clients/stand.mjs` | 14488 | `d138a541a66c920582e26d6799dce93a04034267bd3426c4e373242990632ac2` |
| `var/p4-cli-clients/cli-controller.mjs` | 19462 | `893eb658495cad785b4f9962ac06c1cc2b64b85504f3ba18a66a8bfe9573ef71` |
| `var/p4-cli-clients/wire-trace.mjs` | 8395 | `f95fbc58985f8ac6117e320db11906df5208a31cc49000ce76ee98c0ada1aa4d` |
| `var/p4-cli-clients/codex-channel.mjs` | 15564 | `fb76bf4b96a04e2f6d4e8db9cc4bba95fecd53f83d0ccf072b843eb59bbce333` |
| `var/p4-cli-clients/opencode-channel.mjs` | 11756 | `4b0fcafae3c37b95dc84dd61de82ee7bce5c2da46de219ef03849e69cfbc0583` |
| `var/p4-cli-clients/opencode-provider.mjs` | 13630 | `cc8f30542f420d789dfc74bc048a7b1e3fd1aa9ad2ff1236f77fc387ab6a0990` |
| `docs/implementation/p4-cli-client-plan.md` | 3921 | `aa12be1a852da469cb2c2610152da8ef684febbaa8b8b2b665d9fbde0416faf8` |
| `docs/implementation/p4-cli-codex-contract.md` | 16542 | `8c08f7b0850d0a17d7695f2885d3ddb892ac4dc65e12b7a9469e139a2bde70d7` |
| `docs/implementation/p4-cli-opencode-contract.md` | 29224 | `ec2ba6b9dad0c8dd87bb7e451d707d25bbaad8c26495043ee5a3a8048cf94222` |

Cleanup finding закрыт одним существующим root registry. Оба channel передают основной child синхронно до startup await; OpenCode также передаёт `.child` PowerShell listener probe до ожидания его promise. Persistent error listeners различают failed spawn без PID и ошибку существующего child; последняя не подтверждает exit. Factory failure сохраняет безопасные PID/cleanup fields, а настоящий handle уже остаётся у root даже до `channels.set`. У OpenCode channel cleanup относится к основному serve; общий root finish проверяет также auth/probes.

Повторное чтение выявило и закрыло последний участок той же границы: stop может прийти, пока factory ещё ждёт hash/free-port до spawn. Теперь late registration сначала сохраняет exact handle, затем сигналит его и отказывает startup; login повторно проверяет closing после free-port await. Root close ограниченно ждёт текущий stage, повторно обходит registry и не объявляет успех при busy, незавершённом stage либо живых children. Stand независимо от результата client cleanup закрывает собственные services, обнуляет исходный artifact key buffer, сохраняет безопасный status и возвращает failure вместо exit0 при неподтверждённом завершении. Эти выводы относятся к source paths; реальные kill-failure и stop-during-startup эксперименты не выполнялись.

Допуск сохраняет уже принятую операторскую последовательность: callback ownership проверяется до Allow; Codex идёт первым, затем отдельный OpenCode connection/input. Нельзя назвать исполнение успешным по одному login exit, fixture marker или флагу `completed`. Root должен сопоставить actual authorization/token/MCP wire, все семь этапов, человеческую правку и RO effect/budget/revision evidence, а также фактическое завершение owned процессов. При неизвестном результате нет автоматического нового key, consent или create.

Остаются честные пределы: Codex direct RPC проверит штатный transport/runtime, OpenCode fixture — настоящий tool loop с детерминированной модельной стороной; оба не докажут D1 модельное понимание. OpenCode `iss` limitation, не OS sandbox, ненаблюдённый refresh и отдельные D2 HTTPS/restore критерии не скрываются. Ни один из этих последующих executable gates данным source admission не закрыт.

## Первый actual Codex запуск: ошибки профиля до OAuth

Root сохранил отдельный failed run `e187b4e6183f8b46f2911cf5ef844e92`, host PID49928. Reviewer прочитал только выбранные безопасные поля его `safe-status.json`: Codex child14508 фактически завершился с code1; owned children пусты; authorizations/tokenPosts/MCP requestStarts равны0; Connect accounts1, Notes/proofs/connections/principals/grants/credentials/Invocations/spent/reserved равны0. Это подтверждает границу «CLI не дошёл до OAuth», но не успешную клиентскую совместимость.

Root same-profile read-only diagnostic сообщил фиксированную ошибку `--strict-config is not supported for codex mcp`. Reviewer дополнительно сверил сохранённый actual `mcp --help` и текущий login argv. Удаляется только флаг у `mcp login`; supported `app-server --strict-config` остаётся. Следующий normal-loader diagnostic root обнаружил второй отказ: `model_catalog_json` не принимает пустой `models`. Предыдущий source admission не поймал эти два runtime ограничения; исторические pins и отсутствие executable PASS выше сохраняются без ретроспективного исправления результата.

Выбранный узкий repair — одна полная static metadata запись `c2c-no-model`, а не Responses/model fixture. Отказной no-model endpoint, отсутствие `turn/start` и проверка нулевого числа model requests сохраняются. По повторно прочитанному pinned `StaticModelsManager` local catalog не выполняет remote refresh. Реальное принятие исправленного каталога и последующий OAuth/tool flow ещё требуют исполнения root.

Предел изоляции уточнён по pinned loader исследованию автора: explicit `CODEX_HOME` выбирает отдельный user/auth store, но normal loader также читает системные managed requirements/config из Windows ProgramData; project layers могут быть прочитаны и отключены по trust. Fresh home не означает отсутствия всякого системного чтения. Личные auth/config/environment reviewer не читал; политики не обходятся. Исправленный запуск получает новый own stand/profile, failed run сохраняется отдельно; автоматического повторения его OAuth/effect нет.

После первой static записи root actual normal `mcp list` нашёл третий отказ того же diagnostic пути: custom `ModelsResponse` deserializer требует `base_instructions` либо `model_messages.instructions_template`. Добавлена одна static `base_instructions` строка. Это также пропущенная при прежнем field-only чтении семантика, а не OAuth regression или разрешение запускать модель. Runtime/lifecycle/RPC часть channel не менялась.

Исправленная дельта прочитана и **source admission возобновлён**: controller импортирует и пишет `codexFixtureCatalog()`, login не содержит unsupported flag; channel сохраняет `app-server --strict-config`, закрытый local provider и отсутствие `turn/start`. `project_root_markers=[]` ограничивает project-config discovery собственным cwd; не обещает отсутствия Git metadata или системных policy reads. Root отдельно проверяет semantic config actual `mcp list` до fresh login; reviewer этот executable gate не выполнял и заранее не объявляет его успешным.

| Новый снимок после config findings | SHA-256 |
| --- | --- |
| `var/p4-cli-clients/codex-channel.mjs` | `b27d91d5d8dcf75ecb12d3600d385ee426a5be4662f5a50af7770c3fcf5b25d6` |
| `var/p4-cli-clients/cli-controller.mjs` | `f427b34eda3e5b5f49950704766fa98755cf524aefbdd02280e22daaaa1ef255` |
| `var/p4-cli-clients/stand.mjs` | `4e5af4dfd10439169fd8b91d6091a3ff1ee4358cfb63b74759b475f1c1fbfc00` |
| `docs/implementation/p4-cli-codex-contract.md` | `4b55898d13db2ea47134ed48ab6420ccc8d63215418cc190952cc8c1d0be2bdc` |

Stand delta предоставляет одной owned exec session команду stdin `stop` в тот же graceful close; не добавляет HTTP mutation или другой authority path. Для согласованной однократной остановки source blocker нет. Root передано маленькое замечание о latch: одновременный второй stop event не должен обойти ещё выполняющийся cleanup через ранний return `close`. Этот комментарий не назван выполненным negative test и не меняет неуспешную атрибуцию первого run.

Финальная дельта до нового запуска закрывает замечание о stop: общий `stopRequested` latch допускает один обработчик завершения и не даёт второму событию вернуть преждевременный exit0. Exact origin, Host и listener согласованно перенесены на `http://127.0.0.1:5519`, без port fallback; остальные issuer/audience/config адреса выводятся из того же H. Reviewer прочитал эту дельту и заново сверил текущий stand SHA `e68a88c284827e65993fcbd65a367eace5361116807dd03d2070c9ba9cfea3e9`; controller `f427b34e…1ef255`, Codex channel `b27d91d5…5b25d6` и ранее принятые остальные три helper не менялись. Source GO относится к одному новому bounded run, а предыдущий failed run остаётся отдельным.

Root сообщил фактический **config-only PASS** на прежнем own preauth profile: normal `mcp list --json` завершился с code0/405B; выбранные безопасные поля — один server, `soty=true`, `ownURL=true`, без auth call. Catalog — 839B, SHA `d840b540d4fba1c242cf186b858c10700cd294c0d589442d8a6b8885badd9aa2`. Root также выполнил только presence check двух системных KnownFolder config/policy путей: оба отсутствуют. Это атрибуция root execution, не запуск reviewer. Она закрывает обнаруженные semantic config отказы и не доказывает OAuth login, app-server tool calls, Notes effect либо модельный сценарий; их фактический результат ещё должен быть зафиксирован отдельно.

## Actual Codex MCP refusal: проверка границы версий

В новом stand5519 root сообщил успешный настоящий Codex OAuth, затем отказ app-server discovery до эффекта: initialize HTTP400/code−32022, child31016 завершён с подтверждённым cleanup, owned children пусты; connection1, Note/Invocation/spent0. Первоначальный QA observer скрывал любую дату вне двух прежних разрешённых значений, поэтому **точная дата actual wire этим первым сообщением не доказана**. Безопасная дельта observer допускает только строку даты и выбранное `error.data.requested`, не raw headers/arguments; повторное согласие, новый ключ операции или изменение client wire из отказа не следуют.

Независимое чтение установило конкретный product seam: `server/capabilities-mcp.js` допускает headerless initialize только с `2025-11-25` и отказывает до `sdk.fetch`. Между тем [официальный source закреплённого Codex commit](https://raw.githubusercontent.com/openai/codex/3d2ee51ca2d5db578f328aa75e20aa22c0197c9a/codex-rs/codex-mcp/src/rmcp_client.rs) в `mcp_initialize_request_params()` явно задаёт `ProtocolVersion::V_2025_06_18`. Это установленное поведение pinned source, не вывод из даты по умолчанию rmcp и не подмена будущего wire observation.

Прочитанный локальный SDK2.2.0 знает `2025-06-18` в `core/dist/auth-BNDyLTqp.mjs:6`; `Server._oninitialize` выбирает точную requested дату из своего supported list, а `Protocol.connect` переносит список в HTTP transport. Статический legacy fallback создаёт свежие Server/transport на POST, сохраняет конечный SSE, EOF/abort cleanup и отсутствие session ID. Поэтому рекомендована одна дополнительная явная дата `2025-06-18` в product profile и initial admission; прежние modern/legacy пути, auth, bounds и final fresh read остаются общими. Разрешать весь список SDK или переключать CLI на другой протокол не требуется.

Минимальный gate после исправления: причинный actual SDK HTTP initialize06 → initialized202 → list/catalog_get с negotiated06; прежний unknown/`2025-03-26` и post-init missing-header отказ; затем один настоящий Codex discovery с уже имеющимся lawful grant. Инициализация сама по себе не доказывает дальнейший обмен. На момент этого source review исправление и новые executable результаты ещё не приняты; reviewer не запускал CLI, сервер или тесты и не менял production.

## Узкий June repair: независимый source verdict

Прочитаны final production diff, оба новых теста целиком, авторская квитанция и root изменения двух существующих `supported` literals. **Новых source blockers нет.** `server/capabilities-mcp.js` SHA `385e105d59544e094f17a0471c02dd65df67fb372a94e180c09a21c481c5e45d` меняет только закрытый legacy список November/June, производный общий список modern/November/June и две проверки initialize. Штатный SDK dispatch, авторизация, finite buffering, final fresh read и cleanup остаются прежними. Unknown03/status/code/requested assertions не ослаблены.

Новый `server/test/capabilities-mcp-codex-compat.test.mjs` SHA `abe2234cd2f12768f0368fa6c09c452e4c3ecff7a31160ebaae8ae18e759749b` заставляет настоящий SDK Client2.2 сформировать June handshake. Его fetch только наблюдает и пересылает исходные bytes; фиктивного negotiated ответа нет. Первый case проходит четыре tools через действующие signed Connect/Provider/Notes fixtures, проверяет exact replay, отсутствие private material и новый wire401/403 после revoke. Второй сохраняет headerless-only-initialize, unknown03/future и mismatch/no-echo отказы. Это обоснованные compatibility tests, но не настоящий Codex executable; reviewer их не запускал. Author receipt при чтении: `957a86a85e8286d11dca909bd73e21e36e7590d567ed2da5915499b44e65c7d0`; результаты root gate атрибутируются отдельно.

QA delta также прочитана: stand SHA `c469e48bff2a073c69b42b6cc10416e7205e9484911eaaccc52d7bbefab2c19c` согласованно использует5521; controller `709b86fa89578fcde5ea52a84d3f9b133f393d65a981015088c62f4821477890` добавляет только четыре whitelisted RPC method names и целочисленный code; observer `b7d940000688d0d679edd64986200e3f710f5163ebfc50c9163ddd1e2c8957b8` сохраняет только безопасные date fields, включая refused date. Client wire и OAuth secrets не переписываются и не публикуются.

Root выбрал более простой операторский путь вместо повторного использования процесса с RAM-only AS keys: явно отозвать прежнюю тестовую connection, сохранить known-zero-effect failure, подтвердить graceful shutdown и после wire gate запустить один новый fresh stand5521 с новым ручным согласием. Это допустимая смена известного неисполненного QA сценария; она не является автоматическим переизданием неизвестной Note операции. Не требуются hot reload, экспорт ключей или новый recovery framework. Факт отзыва/завершения/нового CLI discovery должен подтверждаться отдельным исполнением root.

## Итог C2c: независимая квитанция, 2026-10-01

**C2c принят как transport/tool integration двух настоящих клиентов.** Предыдущие разделы сохраняют историю source review и отказов; итоговая атрибуция ниже заменяет их предварительные статусы, не переписывая результаты старых запусков. [Root result](p4-cli-client-result.md) и актуальные вводные обоих CLI contracts проверены отдельно.

| Собственный run | Итог | SHA-256 immutable `result-receipt.json` |
| --- | --- | --- |
| `e704dc1eb48ca5635500d444b4f1fb7e`, origin5521 | OpenCode1.18.15 COMPLETE, семь PASS; Codex0.153.4 PARTIAL, completedfalse | `af25369251a6b46add02e15e15491ef026cebbc2bd6e1615721f3bdb9bcb8716` |
| `5e9d04a7999c7e00d7ea3945d719262c`, origin5523 | Codex0.153.4 COMPLETE, семь PASS | `a872c63e11a94f60790055018d26e2fa35f1ee8cfc29d9cf56ebe67bf7b6e44a` |

Квитанции лежат в `var/p4-cli-clients/<run>/`. Reviewer независимо сверил их hashes, owner/status hashes и embedded status, все16 frozen source copies каждого run и dist50/50; у5523 также frozen observer test. Старый receipt после нового запуска остался byte-identical. Новые live helpers не подменяли сохранённые load-time pins.

Выбранные холодные SELECT через `DatabaseSync({readOnly:true})` подтвердили на каждого клиента/run ровно одну succeeded/committed Invocation, одну active Note revision2, immutable native proof revision1, receipt1 и spent1/reserved0. Input purged; store/account/Invocation/Note bindings совпадают. Connection, root grant и credential revoked; оставшийся active principal не объявлен действующей authority. При чтении main/WAL bytes не изменились. В5521 глобально два эффекта и две траты, в5523 один эффект и одна трата. Это отдельные RO snapshots, не cross-store atomic transaction. Тела записок и OAuth secrets в вывод не попадали.

Трасса OpenCode5521: create34→replay38 сохранили input digest и Invocation/Note IDs; get36/40 имеют privacy=true; отдельные tools/call42/action14 и44/action15 получили401 после отзыва. Трасса Codex5523: offered/negotiated `2025-06-18`, create14→replay16 с теми же digest/IDs, get15/17 privacy=true; tools/call18/action8 и20/action9 получили401. AS refresh19/21 отдельно подтвердили `invalid_grant`, exact client/resource и current action. Эти AS ответы не выданы за MCP envelope; старый token400 Codex5521 не получил ретроактивный PASS. Login child37124/owned callback56897, S256/resource/issuer и token200 нового run сверены по выбранной безопасной квитанции.

Обычные PWA edit/save/reload, сохранение правки после replay/revoke, точный owner revoke с viewport320 и отсутствие overflow — **browser evidence root**. Reviewer сверил его квитанцию с RO Note revision2 и bindings, но не повторял browser workflow. CLI cleanup/ownedChildren0 и channel exit подтверждены safe status; остановка host в собственном TTY и отсутствие listener атрибутированы root. Codex использовал настоящий app-server RPC без model turn; OpenCode — настоящий session tool loop с детерминированным локальным provider fixture.

Итоговый production diff повторно прочитан: только явная June revision в существующем SDK пути и две прежние initialize admission проверки. В существующем тесте меняются ровно два supported-list литерала; unknown03/header/mismatch/error/requested negatives сохранены. Authority, codec, body limits, fresh read и cleanup не расширены.

| Проверенный final artifact | SHA-256 |
| --- | --- |
| `server/capabilities-mcp.js` | `385e105d59544e094f17a0471c02dd65df67fb372a94e180c09a21c481c5e45d` |
| `server/test/capabilities-mcp-codex-compat.test.mjs` | `abe2234cd2f12768f0368fa6c09c452e4c3ecff7a31160ebaae8ae18e759749b` |
| `server/test/capabilities-mcp.test.mjs` | `06bb67a7561fb0db65f8259fbb1d4303cd63610c76ee7aa6f9e73ebc7552f26e` |
| QA `wire-trace.mjs` для5523 | `1223f6ffd659b489ccd7d957d0419343fb273c29a5b17a533314cdfad9277002` |
| QA `oauth-denial-observer.test.mjs` | `44be97e33cdae9111981c6bda0aaebbbd400908752c325329f643852c256a79b` |

Observer source также принят отдельно: request-start/action correlation, bounded raw buffers, строгий typed error и selected Content-Type classification после реального `writeHead` finding. Generic400, delayed action, malformed/aborted/overflow не дают refusal proof; original HTTP bytes/args/this/return сохраняются. Синтетический observer test не назван настоящим OAuth клиентом. Прочитаны три **раздельных root** log; reviewer не запускал эти suites:

| Log в `output/implementation-20260930/` | Результат | SHA-256 |
| --- | --- | --- |
| `p4-codex-compat-root-first.log` | 2/2 PASS,0skip;2031.1656ms | `aa6a10c5a7c150b75eb4dd2f16c633f9f210344e35bc39e7e53ddca03742c6b6` |
| `p4-codex-compat-affected-root.log` | 24/24 PASS,0skip;8959.0384ms | `2140ad274469893a63a1855b0b9dbc2fbb66a1557f27a5da042930ec636d78ca` |
| `p4-oauth-denial-observer-root-final.log` | 7/7 PASS,0skip;128.3559ms | `7319a20f69f3c4158f25dbc75a86377088d48d1f40befeb188dfce9e68dcd78d` |

Материальных блокеров этого C2c среза не осталось. **D1 настоящие модели, D2 HTTPS/restore/release и P5–P8 остаются открытыми.** Не заявлены model autonomy, production deployment, физический телефон, OpenCode wrong-issuer rejection или OS sandbox. Reviewer не запускал CLI/hosts/models/tests, не менял product source и сохранил прежний PARTIAL.
