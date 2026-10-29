# Выпуск UI Сот без замены DATA writer

Source-механизм: `deploy/ui/release.mjs`. Он не устанавливает конфигурацию, не управляет Docker/systemd, не посылает сигналы и не читает учётные данные.

## Контракт

```js
const release = createUiRelease(distDirectory, retainedDistDirectories);
writeUiRelease(release, freshStagingDirectory);
const overlay = await createCaddyOverlay({
  caddyfile: privateOriginalText,
  release,
  root: '/srv/soty-ui/current', // одноразовый bootstrap; следующие UI без Caddy
  host: '4-2.xn--p1ai',
  adapt: privateCaddyAdapter,
});
verifyStructuralDelta(overlay.config, independentlyAdaptedCandidate);
```

`createUiRelease` возвращает `id`, `manifest` и `files: Map` с байтами. `id` — SHA256 точного отсортированного inventory. `writeUiRelease` создаёт только новый каталог; существующий каталог, symlink и изменившиеся байты не принимаются. Частичный staging после ошибки не публикуется.

Допущены только корневые файлы оболочки, известные иконки/шрифты, hashed assets, публичные manifest/изображения обложек, `/help/interaction.html` и конечный `PUBLIC_SKILL_FILES` из `deploy/ui/release.mjs`: каталог `/skills/index.json/html`, пакет `soty-cover-art` и пакет `soty-app-deploy` с перечисленными references. Соседние skills и лишние файлы пакета исключены. Документы skill обновляются с UI и не получают immutable cache. `.env`, source maps, `/agent`, HIVE/другие app entrypoints и прочие файлы не входят в release. Проверяются ссылки index, manifest и все три массива precache worker. Прежние releases добавляют только immutable URLs. Один immutable URL с разными байтами останавливает сборку; обычные `index.html`/worker/manifest меняются вместе с выпуском.

Продуктивный каталог принадлежит root и недоступен для записи runtime/user. Единственное исключение из запрета symlink — контролируемый `soty-ui/current`, указывающий ровно на `releases/<id>`; сам release и его inventory по-прежнему без symlink. Установщик повторно сверяет точные bytes/modes и сохранность всех прежних immutable URLs. Caddy следует symlink: его root не является sandbox, поэтому произвольные цели и writable parent запрещены.

Обычный выпуск выполняет `node deploy/ui/host-publisher.mjs execute publish-plan.json`: один план `action:publish` с public stage, manifest, `base`, `expectedReleaseId` и transaction. Он сам устанавливает sealed release и переключает `current` под Linux kernel flock с CAS. Caddyfile, ключ, PID и DATA не участвуют. `inspect` строго read-only сверяет реальный pointer и исходный журнал; после неизвестного результата повторять execute нельзя. Откат тем же publish меняет mutable UI, сохраняя новые immutable URLs. Полная форма плана и одноразовый bootstrap — [deploy/ui/README.md](../../deploy/ui/README.md).

## Порядок при раздельном выпуске backend и UI

Новый backend может начать выдавать новый UI раньше переключения публичного
`index.html`. Документы `/human-identity/interaction/...` и
`/oauth/interaction/...`, а также разрешённые backend SPA-входы читают
`index.html` из собственного `dist`. Их JS/CSS запрашиваются с того же origin,
но stable Caddy handler берёт эти публичные файлы из текущего UI release.
Поэтому backend-first без подготовки его asset-графа может дать пустое окно
входа: новый HTML ссылается на immutable URL, которого ещё нет в UI root.

До **каждого** backend-переключения выпускай asset-only bridge обычным
publisher. `currentInput` — физический, проверенный каталог текущего release,
не symlink `current`; `exactBackendDist` — публичный `dist` именно того
immutable backend image, который будет запущен следующим.

```js
const assetBridge = createUiRelease(currentInput, [exactBackendDist]);
writeUiRelease(assetBridge, freshAssetBridgeStage);
// Штатный publish: input=freshAssetBridgeStage,
// expectedReleaseId=проверенный ID текущего UI release.
```

Если `assetBridge.id` уже равен проверенному ID текущего release, его inventory
не изменился. После полного публичного readback нужного графа дополнительный
publish не требуется; используй тот же current input для следующего шага.
Одного совпавшего entry JS для такого решения недостаточно.

Основной вход остаётся `currentInput`, поэтому все прежние mutable bytes —
`index.html`, `sw.js`, manifest, документы, фиксированные иконки и шрифты —
должны совпасть с текущим inventory. Retained-вход добавляет только immutable
файлы. Проверяй весь точный граф нового backend, включая dynamic imports,
CSS, hashed font/media assets и другие допущенные immutable URLs; одного
entry JS недостаточно. Конфликт URL с другими байтами останавливает сборку.
Если новый код требует несовместимые новые bytes по mutable URL, такой bridge
не доказывает совместимость: разреши эту зависимость до backend-переключения,
не подменяя mutable UI и не ослабляя public-path policy.

Для двухступенчатого перехода writer4 → writer5 нужны разные asset-графы:

| Шаг | Primary UI input | Retained input | Следующее действие |
|---|---|---|---|
| Asset bridge перед writer4 | Проверенный текущий UI | Точный writer4 image `dist` | HTTP-сверка, затем штатный backend rollout writer4 |
| Asset bridge перед writer5 | Предыдущий asset bridge | Точный writer5 image `dist` | HTTP-сверка, затем штатный backend rollout writer5 |
| Финальный UI | Точный новый UI `dist` | Последний asset bridge | Publish нового index/worker после готовности API5 |

Финальный stage сохраняет объединённые immutable URLs обоих bridge:

```js
const finalUi = createUiRelease(newDist, [assetBridgeInput]);
writeUiRelease(finalUi, freshFinalStage);
```

Каждый publish имеет свой transaction и CAS `expectedReleaseId`, равный
фактически проверенному текущему ID после предыдущего шага. Не подставляй
первоначальный ID повторно и не теряй immutable URLs первого bridge во втором.
После потери ответа используй исходный `inspect`; неизвестное действие не
повторяется. Полный состав и квалификация backend images проверяются своим
штатным rollout: asset-only публикация их не доказывает.

Перед backend-переключением проверь по настоящим публичным URL:

- Root index, `sw.js` и остальные mutable файлы сохранили прежние SHA-256;
  root по-прежнему ссылается на прежний entry. Caddy config не менялся.
- **Все** новые immutable URLs следующего backend отвечают ожидаемыми bytes
  и SHA-256, подходящим Content-Type и immutable cache policy. HTTP200 с HTML
  вместо JS не является успехом. При любом отказе backend не переключается.
  Static overlay выдаёт файл при 200 от Root; остальные ответы сохраняются.
- После штатного backend rollout настоящий документ входа и его lazy imports
  загружаются; API нового способа входа готов до выпуска его основного UI.
  При финальном UI publish дополнительно сверяются новый index/worker, worker
  graph, прежние immutable URLs и работа старой открытой вкладки.

Старый service worker не решает разрыв: protocol-документы всегда идут в сеть,
а отсутствующие в его собственном build graph новые assets тоже запрашиваются
из сети. Сохраняй старые mutable worker bytes на asset-only шагах; не очищай
кэши, не форсируй обновление пользователей и не добавляй runtime fallback.
Этот порядок использует существующий atomic publisher, без изменений Caddy.
Actual URL readback и приёмка сохраняются отдельными release receipts;
доступность из локального `dist` их не заменяет.

## Узкая вставка Caddy

Под выбранным точным host требуется единственный прежний upstream `127.0.0.1:18182`, без rewrite/header mutation/response interception. Его leaf route должен быть ровно `{handle:[proxy]}`. Допускается только распознаваемый многострочный literal proxy с обычным transport-блоком, до64строк; неравные отступы допустимы. Неизвестная или неоднозначная форма означает отказ.

Перед прежним leaf route добавляется один matched proxy для GET/HEAD и точных URL inventory. Query `project`/`hive`, прежние ветви HIVE, специальные пути, API, протоколы, `/agent`, остальные hosts и прежний fallback сохраняются.

Если ancestor route ограничивает пути, его список должен быть единственным plain `path` matcher с `/` и `/assets/*`, заданным одной однозначной строкой `@name path ...`. Для отсутствующих в нём inventory URLs генератор заменяет только эту строку на native Caddy expression: `path(старые пути) || (method("GET", "HEAD") && path(точные новые URLs) && !query({"project":"*"}) && !query({"hive":"*"}))`. Первая ветвь сохраняет старые пути для всех методов/queries; вторая открывает только чтение публичного inventory. Семантику path/query исполняет Caddy. Множественные path boundaries, combined matcher, неизвестные шаблоны и неоднозначные source definitions означают отказ. Специальные соседние routes, включая canonical index redirect, не меняются.

Node сначала обрабатывает исходный запрос. Только его200 заменяется статическим файлом. Редиректы/ошибки остаются прежними. Динамические CSP и остальные защитные заголовки копируются из ответа Node по явному списку; его старые Content-Type/Length/Encoding, ETag и Last-Modified не копируются. У нового файла свои validators, HEAD и range. Upstream не получает If-Match/If-None-Match/If-Modified-Since/If-Unmodified-Since/If-Range/Range; original request остаётся у file_server. Документы/worker/manifest имеют no-store, immutable URLs — годовой immutable cache.

Все кандидаты проверяет настоящий `caddy adapt`. Генератор принимает только ожидаемую вставку перед неизменным original route и, если требуется, точный ancestor expression с его native `name`; весь остальной JSON должен быть равен baseline. Caddy автоматически перенумеровывает route.group; сравнение допускает только биективное переименование `groupN` в HTTP route groups с сохранением их partition. Split/merge групп, пользовательские имена, matcher/header/upstream/transport и поля вне HTTP routes не нормализуются. Негативные тесты проверяют каждый такой отказ.

`file_server.hide` **не игнорируется и не нормализуется**. Все вызовы `adapt(text)` должны иметь одинаковые filename и cwd. Для реальной приёмки применяйте физический `/etc/caddy/Caddyfile` в изолированной tmpfs-копии; stdin `--config -` добавляет другое auto-hide имя и не доказывает совпадение с физическим именем. stdout/stderr адаптера могут содержать секреты — держать их приватными, наружу только безопасные хеши/статусы. Возвращённые `overlay.caddyfile/config` тоже приватны.

Генератор работает от одобренного baseline до overlay, без повторного разбора собственного установленного overlay. При одноразовом переходе со старого literal root нужна прежняя полная сверка active/disk и encrypted backup. Stable handler использует общую fixed/hashed public-path policy, а не inventory конкретного выпуска; следующие UI требуют только publish/CAS pointer. При независимых изменениях ingress требуется новая сверка baseline, а не перезапись старым шаблоном.

## Проверка и установка

```text
node --test deploy/ui/release.test.mjs
SOTY_UI_CADDY_BINARY=/qualified/caddy node --test deploy/ui/release-caddy.acceptance.mjs
```

Второй набор требует Caddy2.11.2 и свободные loopback18182/19081/19082; запускать в отдельном контейнере/namespace без продуктивных mounts и сети. Проверяет реальный proxy/file_server, разные поколения assets, динамические headers,200/304/403/302/206, HEAD, conditional requests, HIVE/special/API/agent, неизменный backend и откат SIGUSR1. Отдельный production-shaped matcher проверяет недостающие JS/font/license: GET/HEAD/304/206 работают; POST/PUT/OPTIONS и hive query сохраняют прежний404, project остаётся HIVE, Node403/302 и explicit index redirect сохраняются. Это не браузерная приёмка PWA и не реальная выдача прав.

Установщик отдельно подтверждает active=disk, config CAS, точный PID/UID/starttime Caddy, читабельность immutable root фактической службой, физический filename, зашифрованную резервную копию и откат. При adminOFF `caddy reload` не работает; SIGUSR1 допустим для исходного `caddy run` без resume/API workflow. DATA writer не заменяется. Reload Caddy по умолчанию закрывает WebSocket: проверить восстановление соединения, не обещать сохранность соединений только по прежнему Node PID.

После установки: публичный root+все worker assets, прежние статусы/headers, старая вкладка с lazy import, установленная PWA, сохранение профиля и черновика, безопасное обновление и откат. Доступность API и локальные тесты не заменяют эти проверки.

## Закреплённые входы сборки и проверочного стенда

- У Docker с containerd `buildx --iidfile` может содержать digest image config, а `docker image inspect` ожидает digest manifest. После успешного export разреши собственный уникальный tag через `docker image inspect TAG`, сохрани actual `.Id` и повторно проверь этот ID, revision и task labels. Различие двух digest не означает неуспешную сборку и не разрешает её повторять. Tag служит только навигацией; запуск закрепляется actual ID.
- Для исходного контекста сборки выполни `python scripts/archive-source.py --commit <полный-40-символьный-SHA> --output-root <существующий-каталог>`. Нужны Git и Python 3; для другого checkout укажи `--repo <путь>`. Скрипт создаёт новую папку `<output-root>/<SHA>` с `source.tar` и `source-receipt.json`, сверяя каждый файл с Git blob и исполняемым битом. Символические ссылки и submodule не поддерживаются; неполный архив или изменённые байты вызывают отказ без receipt. Не формируй tar вручную и не перезаписывай прежнюю попытку. После переноса повторно сверь SHA-256 tar по receipt, затем распакуй в отдельный пустой каталог. Проверки архиватора: `node --test scripts/archive-source.test.mjs`.
- SHA Git blob, проверенный tar и фактически развёрнутые файлы — разные объекты: после распаковки отдельно проверяй bytes/modes. `tar.umask` может менять обычные права, хотя Git хранит только исполняемый бит. Не нормализуй опубликованный пакет или SQL/конфигурацию ради совпадения; допустимая текстовая разница требует отдельного конечного перечня old/new hashes. SDK `.tgz` сверяется побайтно.
- Отдельный UI producer хранит разные `backendRevision/backendImage` и `uiSourceRevision`. Он проверяет неизменность всех production server/module/dependency inputs; исключение тестового файла описывается по точному пути и old/new hashes. Результат — конечный `/dist`, без нового runnable backend. Новая инструкция или frontend-редактор не заменяет квалифицированный DATA writer.
- Перед запуском копии реальных DATA отдельно проверь физические mounts проверочного helper без DATA: конечные mountpoints, читабельность, исполняемый Caddy и его SHA/version. Не вкладывай file bind в read-only directory bind без существующего mountpoint. Ошибка стенда не разрешает ослабить guards или повторить неизвестный START.
- Для HTTP-проверки виртуального host используй `node:http.request` с явным `Host` и loopback connection address. В проверенном Node24 built-in `fetch` игнорирует переданный `Host`; ответ default host нельзя принять за результат нужного сайта. Сначала докажи полученный сервером literal Host на собственном echo fixture. Сохраняй настоящие method/query/body bounds, timeout, redirect и response assertions.
- Сверка active/disk выполняется поддерживаемым `verifyActiveDisk` с физическим именем конфигурации и полным JSON. Фиксируй отдельно cwd адаптера, объявленный systemd cwd и реально прочитанные process fields. Отказ `/proc/.../cwd` или `/proc/.../exe` не доказывает их значение: не подменяй наблюдение предположением и не расширяй capabilities helper ради лишней диагностики.
- В измеренном выпуске установка337файлов с индивидуальным `fsync` заняла79,7с:45с внешнего attach timeout недостаточно. Размер и число файлов входят в бюджет операции. Потеря attach-ответа закрывается исходным exact-ID контейнером, журналом `started → installed` и `verifyInstalledRelease`; повтор установки не нужен. На Windows для `os.fsync` скачанного ciphertext открывай собственный файл с записью (`O_RDWR`), а не read-only handle.
- Собственный config tar должен проходить тот же `TarInventory`, что и восстановление: корневая запись и все точные родительские directory entries обязательны. При GNU tar добавляй строгие deduplicated parents без рекурсии, затем только разрешённые leaf paths с рекурсией; не архивируй содержимое чужих parent directories. Проверь форму одним synthetic `encryptTar → verifyTar` до остановки приложения. Успешный GPG ciphertext сам по себе не доказывает пригодность tar для восстановления.
- Проверяй собранный оператор именно как CLI, с настоящими аргументами и stdin. Импорт bundle как библиотеки не проверяет его точку входа: после bundling проверка `import.meta.url === process.argv[1]` может включить одновременно импортированный и внешний обработчики. В наблюдавшемся случае два JSON-ответа и код1 скрыли успешный второй обработчик за общей ошибкой разбора. Для wrapper допускается удаление только закреплённого единственного bootstrap импортированного CLI при сохранении его функций; неоднозначное совпадение останавливает сборку. До продуктивного применения докажи один ответ, корректный exit code и отсутствие действий на ошибочном аргументе. Сохраняй исходный RED и не повторяй неизвестное применение.

При неизвестном результате используй прежний журнал и read-only reconciliation. При известной ошибке только собственного проверочного стенда сохраняй RED, устраняй конкретную причину и квалифицируй исправленную физическую схему отдельно; новый разрешённый цикл имеет собственный журнал и не переписывает предыдущий.

Основания: [Caddy response interception](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy#intercepting-responses), [native path/method/query expressions](https://caddyserver.com/docs/caddyfile/matchers#expression), [file_server](https://caddyserver.com/docs/caddyfile/directives/file_server), [reload/signals](https://caddyserver.com/docs/command-line#signals).


## Публичная инструкция размещения

Канонический источник — `skills/soty-app-deploy/`. Редактировать public-копии вручную не нужно: `node scripts/sync-public-deploy-skill.mjs` копирует только десять перечисленных файлов в `public/skills/soty-app-deploy/`; `--check` сравнивает точные bytes и проверяет замыкание относительных ссылок. Обычный `prebuild` и packaging tests требуют проверки синхронизации. Приватные configs, operator state, `agents/` и новые соседние файлы автоматически не публикуются. Новый reference требует явного изменения конечного списка и проверки.

Каталог `/skills/index.json` обнаруживает оба пакета; HTML-каталог содержит ссылки для человека. `/help/interaction.html` — пользовательская справка по рабочим назначениям, без runtime API или выдачи прав. Ссылка на документ не означает включённый Source adapter, issuer, MCP или grant. Перед новым UI-выпуском проверять права→вызов→persist→reload, актуальные Source/client capabilities и public byte readback; один packaging PASS не квалифицирует подключение.
