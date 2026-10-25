# Выпуск UI Сот без замены DATA writer

Source-механизм: `deploy/ui/release.mjs`. Он не устанавливает конфигурацию, не управляет Docker/systemd, не посылает сигналы и не читает учётные данные.

## Контракт

```js
const release = createUiRelease(distDirectory, retainedDistDirectories);
writeUiRelease(release, freshStagingDirectory);
const overlay = await createCaddyOverlay({
  caddyfile: privateOriginalText,
  release,
  root: `/srv/soty-ui/releases/${release.id}`,
  host: '4-2.xn--p1ai',
  adapt: privateCaddyAdapter,
});
verifyStructuralDelta(overlay.config, independentlyAdaptedCandidate);
```

`createUiRelease` возвращает `id`, `manifest` и `files: Map` с байтами. `id` — SHA256 точного отсортированного inventory. `writeUiRelease` создаёт только новый каталог; существующий каталог, symlink и изменившиеся байты не принимаются. Частичный staging после ошибки не публикуется.

Допущены только корневые файлы оболочки, известные иконки/шрифты, hashed assets, публичные manifest/изображения обложек, `/help/interaction.html` и конечный `PUBLIC_SKILL_FILES` из `deploy/ui/release.mjs`: каталог `/skills/index.json/html`, пакет `soty-cover-art` и пакет `soty-app-deploy` с перечисленными references. Соседние skills и лишние файлы пакета исключены. Документы skill обновляются с UI и не получают immutable cache. `.env`, source maps, `/agent`, HIVE/другие app entrypoints и прочие файлы не входят в release. Проверяются ссылки index, manifest и все три массива precache worker. Прежние releases добавляют только immutable URLs. Один immutable URL с разными байтами останавливает сборку; обычные `index.html`/worker/manifest меняются вместе с выпуском.

Продуктивный каталог должен принадлежать установщику и быть недоступным для записи runtime/user, без symlink в любом компоненте пути. Генератор проверяет буквальный POSIX-путь, оканчивающийся точным release ID; реальную файловую систему и права перед применением проверяет установщик. Он копирует только этот inventory и повторно сверяет байты.

## Узкая вставка Caddy

Под выбранным точным host требуется единственный прежний upstream `127.0.0.1:18182`, без rewrite/header mutation/response interception. Его leaf route должен быть ровно `{handle:[proxy]}`. Допускается только распознаваемый многострочный literal proxy с обычным transport-блоком, до64строк; неравные отступы допустимы. Неизвестная или неоднозначная форма означает отказ.

Перед прежним leaf route добавляется один matched proxy для GET/HEAD и точных URL inventory. Query `project`/`hive`, прежние ветви HIVE, специальные пути, API, протоколы, `/agent`, остальные hosts и прежний fallback сохраняются.

Если ancestor route ограничивает пути, его список должен быть единственным plain `path` matcher с `/` и `/assets/*`, заданным одной однозначной строкой `@name path ...`. Для отсутствующих в нём inventory URLs генератор заменяет только эту строку на native Caddy expression: `path(старые пути) || (method("GET", "HEAD") && path(точные новые URLs) && !query({"project":"*"}) && !query({"hive":"*"}))`. Первая ветвь сохраняет старые пути для всех методов/queries; вторая открывает только чтение публичного inventory. Семантику path/query исполняет Caddy. Множественные path boundaries, combined matcher, неизвестные шаблоны и неоднозначные source definitions означают отказ. Специальные соседние routes, включая canonical index redirect, не меняются.

Node сначала обрабатывает исходный запрос. Только его200 заменяется статическим файлом. Редиректы/ошибки остаются прежними. Динамические CSP и остальные защитные заголовки копируются из ответа Node по явному списку; его старые Content-Type/Length/Encoding, ETag и Last-Modified не копируются. У нового файла свои validators, HEAD и range. Upstream не получает If-Match/If-None-Match/If-Modified-Since/If-Unmodified-Since/If-Range/Range; original request остаётся у file_server. Документы/worker/manifest имеют no-store, immutable URLs — годовой immutable cache.

Все кандидаты проверяет настоящий `caddy adapt`. Генератор принимает только ожидаемую вставку перед неизменным original route и, если требуется, точный ancestor expression с его native `name`; весь остальной JSON должен быть равен baseline. Caddy автоматически перенумеровывает route.group; сравнение допускает только биективное переименование `groupN` в HTTP route groups с сохранением их partition. Split/merge групп, пользовательские имена, matcher/header/upstream/transport и поля вне HTTP routes не нормализуются. Негативные тесты проверяют каждый такой отказ.

`file_server.hide` **не игнорируется и не нормализуется**. Все вызовы `adapt(text)` должны иметь одинаковые filename и cwd. Для реальной приёмки применяйте физический `/etc/caddy/Caddyfile` в изолированной tmpfs-копии; stdin `--config -` добавляет другое auto-hide имя и не доказывает совпадение с физическим именем. stdout/stderr адаптера могут содержать секреты — держать их приватными, наружу только безопасные хеши/статусы. Возвращённые `overlay.caddyfile/config` тоже приватны.

Генератор работает от одобренного baseline до overlay, без повторного разбора собственного уже установленного overlay. Следующий UI выпускается из сохранённого одобренного baseline; до установки нужно подтвердить, что active/disk равны последнему принятому candidate. При независимых изменениях ingress требуется новая сверка baseline, а не перезапись старым шаблоном.

## Проверка и установка

```text
node --test deploy/ui/release.test.mjs
SOTY_UI_CADDY_BINARY=/qualified/caddy node --test deploy/ui/release-caddy.acceptance.mjs
```

Второй набор требует Caddy2.11.2 и свободные loopback18182/19081/19082; запускать в отдельном контейнере/namespace без продуктивных mounts и сети. Проверяет реальный proxy/file_server, разные поколения assets, динамические headers,200/304/403/302/206, HEAD, conditional requests, HIVE/special/API/agent, неизменный backend и откат SIGUSR1. Отдельный production-shaped matcher проверяет недостающие JS/font/license: GET/HEAD/304/206 работают; POST/PUT/OPTIONS и hive query сохраняют прежний404, project остаётся HIVE, Node403/302 и explicit index redirect сохраняются. Это не браузерная приёмка PWA и не реальная выдача прав.

Установщик отдельно подтверждает active=disk, config CAS, точный PID/UID/starttime Caddy, читабельность immutable root фактической службой, физический filename, зашифрованную резервную копию и откат. При adminOFF `caddy reload` не работает; SIGUSR1 допустим для исходного `caddy run` без resume/API workflow. DATA writer не заменяется. Reload Caddy по умолчанию закрывает WebSocket: проверить восстановление соединения, не обещать сохранность соединений только по прежнему Node PID.

После установки: публичный root+все worker assets, прежние статусы/headers, старая вкладка с lazy import, установленная PWA, сохранение профиля и черновика, безопасное обновление и откат. Доступность API и локальные тесты не заменяют эти проверки.

Основания: [Caddy response interception](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy#intercepting-responses), [native path/method/query expressions](https://caddyserver.com/docs/caddyfile/matchers#expression), [file_server](https://caddyserver.com/docs/caddyfile/directives/file_server), [reload/signals](https://caddyserver.com/docs/command-line#signals).


## Публичная инструкция размещения

Канонический источник — `skills/soty-app-deploy/`. Редактировать public-копии вручную не нужно: `node scripts/sync-public-deploy-skill.mjs` копирует только десять перечисленных файлов в `public/skills/soty-app-deploy/`; `--check` сравнивает точные bytes и проверяет замыкание относительных ссылок. Обычный `prebuild` и packaging tests требуют проверки синхронизации. Приватные configs, operator state, `agents/` и новые соседние файлы автоматически не публикуются. Новый reference требует явного изменения конечного списка и проверки.

Каталог `/skills/index.json` обнаруживает оба пакета; HTML-каталог содержит ссылки для человека. `/help/interaction.html` — пользовательская справка по рабочим назначениям, без runtime API или выдачи прав. Ссылка на документ не означает включённый Source adapter, issuer, MCP или grant. Перед новым UI-выпуском проверять права→вызов→persist→reload, актуальные Source/client capabilities и public byte readback; один packaging PASS не квалифицирует подключение.
