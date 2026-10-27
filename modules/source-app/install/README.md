# Подключить приложение

В исходниках общий вход relay привязан к окну Сот; его возможности и условия
совместимости описаны в [инструкции](../README.md#общий-вход-для-совместимого-relay-приложения).
Наличие исходников не означает, что обновлённые компоненты установлены.
Root workspace ниже — один из поддерживаемых вариантов личного пространства;
подключение входа само по себе не устанавливает и не публикует приложение.

Для нового приложения с личными данными можно использовать **Root workspace** ниже.
Для существующих аккаунтов и своей модели прав используйте Native/storage ports
SDK; приложение сохраняет свою базу и проверяет свой ACL. Старый Ordinary/OIDC
режим описан после быстрого старта. Публикацию во всех режимах выполняет
существующий `soty-app-release` / Root Source prepare/promote.

## Новый личный сервис: Root workspace

Нужен Node24.15+ и утверждённый установленный коннектор с поддержкой workspace2.
Текущий SDK — preview. После выпуска возьмите индекс с **доверенного Root**
`/sdk/source-app/index.json`: точные package version, sourceCommit, URL архива,
SHA256 и размер. Архив имеет адрес `/sdk/source-app/<SHA256>.tgz`. Скачайте его,
проверьте размер и SHA256 **до выполнения кода**, затем в своей папке проекта:

Распаковывайте только в **новую** `.soty-sdk` без symlinks. Если такая папка уже
существует, остановитесь: выберите другую новую папку, желательно с SHA256 в имени,
и замените путь во всех командах. Не выполняйте tar поверх установленной версии.

```text
mkdir .soty-sdk
tar -xzf ./source-app-<SHA256>.tgz -C .soty-sdk --strip-components=1
npm ci --prefix .soty-sdk --ignore-scripts --omit=dev
node .soty-sdk/dist/install.mjs --help
```

Пакет содержит runtime/server/browser/CLI, типы, документацию, шаблон и optional
Python binding. Внутри `package-lock.json` и побайтно равного `npm-shrinkwrap.json`:
ровно production graph
openid-client6.8.4 → jose6.2.12/oauth4webapi3.8.8 с registry integrity. Нет
dev esbuild, install scripts, тестов, реальных ключей/config или истории Git.
`npm ci` загрузит только закреплённые зависимости из registry в `.soty-sdk`;
это не npm публикация `@soty/source-app` и не установка глобального пакета.
Не заменяйте этот шаг установкой tgz как зависимости чужого проекта: вложенный
shrinkwrap не удержал exact tree в проверенном npm11.8. После установки SDK можно
использовать без сети; обычные CLI команды вызывают через `node`, без PATH setup.
`install/README.md`, `install/operator.workspace.template.json` и рабочие
`dist/templates/reading-list/{domain.mjs,assets/...}` находятся внутри `.soty-sdk`.
Серверные exports доступны из `./.soty-sdk/dist/server.mjs`, browser exports из
`./.soty-sdk/dist/browser.mjs`; public HTTP asset SDK остаётся `/assets/source-sdk.js`.
При отсутствующем/неверном индексе не угадывайте package/URL: публичный выпуск
остаётся неподтверждённым; оператор может передать тот же verified packet.
Optional Python требует отдельной установки pinned requirements и проверки
своего Native/ASGI/SQL адаптера; Node install не включает Python автоматически.

Root и connector читают
до64 exact approved profiles при инициализации. Новый app/profile требует
trusted operator установки и reinit соответствующего runtime, **не пересборки
Root**. Уже загруженный exact candidate переключается prepare/promote без
restart Root. Пакет не делает self-service admission и не добавляет wildcard.
Начните в пустой папке проекта:

```text
node .soty-sdk/dist/install.mjs author --title "Мой список" --template reading-list
node .soty-sdk/dist/install.mjs check --config /private/operator.json
node .soty-sdk/dist/install.mjs init --config /private/operator.json
node .soty-sdk/dist/install.mjs serve --config /private/operator.json
```

`author` создаёт `.soty/author.json` и `source/{domain.mjs,assets/...}` без запуска
package scripts. Доменный модуль содержит только SQL приложения. Вход, короткая
Source сессия, шифрование, nonce, текущий личный ресурс, финальная проверка перед
COMMIT и неизменяемая квитанция реализованы общим SDK. Существующие `.soty/app.json`
и папка `source` не перезаписываются; после ошибки частично созданные новые файлы
остаются для проверки. Повторный `init` отказывает вместо перезаписи базы.

Между `author` и `check` оператор заполняет
[`operator.workspace.template.json`](operator.workspace.template.json) из
**утверждённого Root profile**, а не из пожеланий автора: тот же неизменяемый
appId, точный target revision/digest, коннектор и issuer/client/resource.
Для Source-only workspace `nativeOrigin === embedOrigin`: отдельный Native
сайт не обещается, safe status — `nativeReachability:source-only`. Только при
действительно отдельном Native/OIDC/парном входе нужны два различных hostname
и его собственная identity/ACL. Placeholder-шаблон намеренно не принимается loader.
Проверенный source profile — `soty.standard-workspace@2`, а не Ordinary/OIDC.
Root RP client secret этому режиму не нужен. Два отдельных ключа
`secrets/{transport,cipher}.key` содержат canonical base64url32 случайных байта;
config/keys600, private data/secrets dirs700 на Linux. Windows ACL этим POSIX
режимом не доказывается. Секреты не попадают в manifest, аргументы команд или UI.

`policy.personalWorkspaces` по умолчанию в шаблоне `false`. Для **нового** личного
сервиса оператор явно выбирает `true`: SDK создаёт каждому проверенному
`issuer/sub` свой пустой Native principal/resource. Владелец Root карточки не
получает чужие данные или Native administrator/support. Guest и объединение
старых аккаунтов отсутствуют. Для существующей базы с собственной identity/ACL
нужен явный Native/storage adapter; SDK не импортирует её в теневую SQLite базу.

После установки используйте существующий release путь: `soty-app-release init`
с портом и `/embed`, затем plan/verify и утверждённые Root Source prepare/promote.
Сохранение `.soty/app.json` — описание пути, не доступ, admission или публикация.
Для активного Native/Source pair изменение выполняется по текущему pair contract;
этот CLI не снимает пару и не публикует приложение автоматически. Native веб-вход
с собственной сессией, ACL и app-policy guard — отдельная возможность приложения.
`native-portal` Ordinary не создаёт такой вход для Root workspace и здесь отказывает.

В интерфейсе шаблона один `client.connect({requestId})` продолжает короткую
сессию и получает текущий контекст, затем `query` загружает собственные записи.
Запись идёт через `prepareInvoke` → `invoke`. При потере ACK браузер сохраняет
только requestId/inputDigest/старый contextRef в namespace своего **уже
авторизованного** личного ресурса. Текст записи/сессии/токены туда не копируются.
После reload/new frame `recoverReceipt(reference)` делает только чтение с новым
contextRef и сохраняет исходную квитанцию. Отсутствие строки у другого профиля
оставляет результат неизвестным; оно не подтверждает, что старая запись не
случилась. Пока результат неизвестен, форма не отправляет второй запрос.

Внешние assets имеют ровно три адреса: `/assets/ui.js`, `/assets/ui.css`,
`/assets/source-sdk.js`; документ — `/embed`. Имена разрешены existing finite
Source transport. Сервер не выдаёт соседние файлы, config, domain.mjs или SQLite.
Frame locator берёт общий browser SDK из href; locator не даёт разрешений.

## Проверка результата и doctor

```text
soty-source reader --config /private/operator.json
soty-source doctor --config /private/operator.json
```

`check` проверяет форму приватной конфигурации. `reader` проверяет **локальную**
схему и один retained encrypted Source session; пустая база честно показывает
`keyCorrectness:unproved_empty`. Они не подтверждают Root доступ или данные.
Первый `connect` не равен готовности данных: отдельно нужны успешный domain query,
реальная запись, квитанция, reload и revoke.

Работающий workspace сервер записывает только последнюю успешную проверенную
операцию после финальных Root/Native checks в приватный purpose-sealed
`source-readiness.json`. Запись содержит timestamp, статические app/profile/target
pins, operation/status. Нет subject, аргументов, cookies, результата или хеша
частных данных. Ошибка записи, неизвестная схема или заполненный диск не меняют
уже committed business результат и не требуют повторной записи.

`doctor` проверяет seal и точное совпадение pins и показывает историческое
наблюдение: `ready` — continuation; `context_verified` — контекст;
`read_verified` — чтение; `commit_verified` — сервер подтвердил commit;
`receipt_verified` — подтверждена квитанция. Это не подтверждение доставки ACK в
браузер и не текущая авторизация: `currentAuthority:not_checked`,
`clientAcknowledgement:not_observed`, `releaseReady:false` сохраняются даже при
целом файле. Missing/invalid/old pins не превращаются в Ready. Не используйте
файл как grant или обход живого Root/Native check. Existing release doctor с
reported observations остаётся отдельной диагностикой публикации, а не live proof.

Проверенный минимальный consumer — `examples/reading-list`: actual Root/Human,
установленный connector, общий browser client, CLI и SQLite host проходят
чтение/запись, потерянный ACK, cold Source restart/new frame, изоляцию другого
профиля и revoke. Source/PG приложения используют те же BFF/Native/storage и
receipt recovery ports; PostgreSQL business SQL остаётся в приложении. SQLite
host — готовый optional adapter, не обязательный storage framework для всех.

Ограничения фиксированы отдельно: BFF default128 context slots, этот SQLite host
4096 active/retained коротких Source sessions до GC по expiry, 4096 nonce rows и
20000 неизменяемых квитанций на личный principal. Переполнение — retryable503,
без вытеснения валидных lease и без повторного входа. Это не проверка мировой
нагрузки. Ordinary Reader3/image labels не становятся Reader этого нового
`soty.sqlite-root-workspace.v1`: Node CLI проверен отдельно, workspace container,
physical cold/restore и реальный DNS/TLS требуют своей квалификации до релиза.

## Выпуск SDK для maintainer

Выпуск SDK выполняет maintainer существующего Root, а не
автор подключаемого приложения. Единственный producer —
`node scripts/build-source-sdk-release.mjs` в clean Git checkout. Он вызывает
существующий portable builder по exact commit и пишет versioned `.tgz` и safe
index в `public/sdk/source-app`; npm publish и lifecycle scripts не выполняются.
Имена, 28 публичных файлов, источники и production graph проверяются до записи.
Имеющийся URL с другим содержимым не перезаписывается. Публичный namespace
отвергает соседние/private файлы и symlinks; максимум8 retained archives по4MiB,
uncompressed tar16MiB. Ничего старого автоматически не удаляется.

Root prebuild вызывает тот же script с `--check`: проверяет уже frozen capsule,
actual canonical source/lock hashes и finite inventory. Git не требуется и
новый commit hash не придумывается: Docker/git-archive получает готовый public
пакет. При изменении SDK источника maintainer явно выпускает новый capsule до
сборки. Existing UI release доставляет только index и hash-named archive через
свой finite allowlist. Наличие файлов в репозитории ещё не доказывает, что их
свежий выпуск реально доступен человеку по HTTPS.

## Ordinary Native/OIDC

Из папки проекта создайте заявку:

```text
soty-source author --title "Мой проект"
```

Команда определяет framework по package.json, создаёт **только** публичный `.soty/author.json` существующего U1 title-only schema и не запускает scripts. Старые `.soty/app.json`, данные и исходники сохраняются. Заявка ничего не исполняет и не получает Native права.

Дальше в проверенном контексте выбирают Source/доступ, подтверждают предлагаемые вход/обратную связь/действия, выполняют проверку подключения и получают reviewable draft. Этот пакет поставляет исполнимый Source/CLI, а не весь self-service wizard/публикацию. Root registration/admission/client/private connector binding проверяются отдельно; metadata не означает Ready. Обычное приложение может сохранять собственный вход.

## Установка оператором

1. Оператор берёт проверенный packet/пины и одобренный existing installed connector с Std2. В `operator.template.json` заменяет placeholders **проверенными значениями допуска**, не пожеланиями автора. До этого template намеренно не принимается loader. Приватный `/data/install/operator.json` содержит exact reviewed Source profile/client/resource, локальный connectorPort, listener127.0.0.1:port и `/data/native` SQLite directory. Секреты — отдельные files `/data/install/secrets/{client,transport,cipher}.key`, не manifest/editor/.env/arguments. Transport/cipher —32 случайных байта в canonical base64url43; client secret предоставляет существующий reviewed RP client. На Linux config/key files600, secret/data dirs700,UID1000.
2. Для **нового** Source: `soty-source init --config /data/install/operator.json`. Создаётся Native format3 и один выбранный пустой ресурс, **без principal/grant**. Own Native guest/linked policies defaultfalse. Опциональный новый пустой guest создаёт первого Native principal только после fresh actual Root/OIDC и явного Source consent по отдельно одобренной Source policy. Это не доказательство владения старым приложением/tenant, не приглашение произвольных участников и не Root-owner-all-data. Existing Native account подключается с двумя независимыми proof.
3. `soty-source check --config /data/install/operator.json`, затем `reader` и `serve` с тем же `--config`. `reader` доSTART независимо проверяет literal format1/2/3 и FK. Root показывает app transport, затем реальный Native login/selected ACL. Проверяют реальное открытие, Source current query/write/receipt/feedback, revoke/restart и TLS callback. Одобрение/допуск и release отделены от локальной установки. При defaultfalse guest policy собственный Native login/права должен предоставить Source; пустая база сама не открывает owner/support доступ.

Source.cfg не принимает caller identity,grants,owner,key/URL/commands из author manifest/body. Config load выдаёт opaque host handle, JSON.stringify(handle) не содержит конфигурации/секретов. Diagnostic CLI возвращает safe IDs/status, `connected:false`; `jobsReady:false` и `longReady:false` остаются честными. `allowLinkedLogin` только Source-native current login policy, Basic300 не возрождается/не становится Long. Source owner/support независим от Root app owner.

## Какой адрес видит человек

`profile.nativeOrigin` — браузерный адрес Native consent: reviewed публичный HTTPS-домен или explicit same-machine loopback. `listener` — **локальный HTTP port процесса**, к которому обращается trusted installed broker. `connectorPort` — собственный local authority IPC; это не браузерная ссылка. На SSH Dev `localhost` сервера не является `localhost` удалённого человека.

Для remote author нужен отдельный достижимый Native HTTPS entry, отличный от Root embed hostname. `soty-source native-portal --config ...` выдаёт fixed Caddy entry: только GET `/soty/connect` и POST `/soty/authorize`, остальные пути404. Proxy сохраняет exact Host выбранного Native origin; Origin/CSRF/RootMAC/actual OIDC проверяются Source. Forwarded headers не дают identity. HTTPS callback остаётся на approved Root embed `/api/embed/callback`; Root sandbox/COOP не ослабляются. DNS/TLS/достижимость со стороннего браузера — отдельный gate; generated portal не является подтверждением.

Derived Source image использует pinned whole-green Root8fd исключительно как Node/dependency base. Собственный ENTRYPOINT запускает `/app/source-app/install.mjs`; Native SQLite и private config находятся в отдельном named `/data` volume. На Linux Source **trusted BFF only** может использовать approved host-network, чтобы existing loopback connector IPC оставался доступным; процесс слушает только127.0.0.1. Это не разрешение worker использовать host-network: processor остаётся no-network/no-socket и defaultoff.

`compose.template.yaml` — операторская спецификация одного Source, с measured immutable image digest и explicit named volume вместо anonymous `/data`. Она не запускается CLI автора и не создаёт connector/client/grants. На другом host/Root server without installed connector тот же template не доказывает transport. Build выполняют по exact canonical Git packet `Dockerfile`; после build отдельно измеряют image/Reader3 и проверяют compatible baseline BEFORE first format3 write. Публичный packet не включает operator.json, ключи, cookies или Native данные.

При build обязательны `SOURCE_REVISION=<полный SHA Source>` и `PACKET_MANIFEST_SHA256=<из проверенного packet>`; отсутствующие/неверные параметры отказывают. Source image переопределяет унаследованную Root revision собственными Source/packet/Reader labels, parentOCI8fd хранится отдельно. Root storage/universal/connect labels намеренно fail-closed: это не Root rollback image. Source image witness не заменяет actual literal ReaderCLI или Native permission. На Windows данный loader проверяет формы/пути, но POSIX600/700 custody не доказывает Windows ACL; Linux custody/image/cold acceptance отдельны.

## Данные и откат

Новая отдельная startup delta перед `serve` проверяет actual encrypted Source state **read-only**: известные Interaction/Session/NativeLoginProof/ProcessorGrant/ProcessorReceipt columns, Native realm/meta, единый configured KeyId и реальный AES-GCM/AAD decoder. По одному bounded ciphertext на модель доказывает соответствие ключа; это не полный integrity scan и не permission. Wrong shaped32-byte key отказывает до создания listener. При0 encrypted rows explicit empty bootstrap остаётся разрешён, статус `keyCorrectness:unproved_empty`. Нет DDL, healing, новых Native grants или сети внутри probe. Старый образ e8d/407 этих новых проверок не получает задним числом.

ДоimageSTART нужен независимый Reader3. Старый Reader2 отвергает Native3 BEFORESTART; готовая compatible Reader3 baseline нужна до3write. При неподдерживаемом schema/key/Native grant Source отказывает, не открывает legacy bypass. Current/compatible Source image/cold должны сохранить все Native IDs/receipts/FKs/encrypted session bytes и private config/key equality.

Backup полного stopped Source volume/config/secrets только encrypted. Для Root-managed Docker volume уже существует guarded `deploy/connect/backup.mjs` +independent restore/sink: actual STOP/no other RW mounts/offline metadata/RSA3072-AES-GCM/tar bounds. Source packet physical-cold fixture ещё отдельный review/RUN; local temp/restart не считается power-loss/physical-volume acceptance. Restore старого Basic/Root ref не создаёт разрешений: нужен новый явный вход с current Native authority.

Нет внедрения finite24h consumer, Source model processing,managed public reviews,remote broker/self-service production publish в этом пакете. Клиент feedback,source-native receipts и generic fixed query/invoke уже реальные; отсутствующие Source permissions/features остаются not-ready.
