# P3-C2 — точная привязка источника к соединению

2026-09-30. Согласованный wire contract для C2-B. Исходная точка — C1 `914a91a`; общий порядок и модель данных заданы в [p3-source-contract.md](p3-source-contract.md). Этот документ не объявляет реализацию или выпуск завершёнными. На момент записи runtime ещё использует прежний протокол, а C2-A отдельно закрывает его для `requiredBindingVersion=2`.

## 1. Три разных доказательства

1. `ready` подтверждает аутентифицированный канал и выбранную версию протокола.
2. `binding-ack` подтверждает, что конкретный runtime установил точную конфигурацию на этом соединении. Это не проверка HTTP.
3. `target-prepared` или `bound-observation` сообщает результат ограниченного локального HTTP HEAD. Ответ 200–499 означает «процесс отвечает», включая 401/404. Он не доказывает функциональную исправность, неизменность кода, права на данные автора, DNS или TLS.

Ни один из этих ответов не разрешает изменение публикации самостоятельно. Полномочия, CAS, согласие на весь порт и текущая политика проверяются серверной моделью.

## 2. Negotiation и принадлежность socket

Сохраняется envelope `schema: 'soty.apps-channel.v1'`. Новый connector отправляет обычный `auth` с дополнительным полем:

```js
capabilities: { targetBindingVersions: [1, 2] }
```

Новый сервер выбирает версию 2 только после успешной текущей аутентификации и отвечает:

```js
{ type: 'ready', schema: 'soty.apps-channel.v1', bindingVersion: 2, channelId }
```

`channelId` — свежий серверный nonce из 32 случайных байт, base64url без padding, ровно 43 символа. Он не является отдельным credential. Старый сервер, не приславший ни `bindingVersion`, ни `channelId`, выбирает прежний режим 1. Явное неизвестное/неправильное значение не трактуется как fallback. Новый сервер может явно выбрать `bindingVersion: 1` без `channelId`. Режим больше не изменяется внутри соединения.

Для режима 2 используются отдельные события: `binding-set`, `binding-ack`, `binding-rejected`, `binding-remove`, `bound-open`, `bound-observation`, `target-prepare`, `target-prepared`, `target-rejected`. Прежние `sync`, `open`, `observation` в этом режиме недопустимы. Claim-протокол и кадры существующего потока сохраняют типы, но принадлежат тому же captured connection context.

Connection context содержит actual socket, выбранный режим, `channelId`, неизменяемые идентификаторы из его `auth`, его bindings, streams, ожидающие проверки и лимиты. Любой handler проверяет, что context ещё текущий, до работы и после каждого `await`. `send(context, frame)` никогда не выбирает socket через изменяемый глобальный `state.socket`. Старые `open`, `message`, `close`, callback записи или ошибка upstream не могут менять карты нового соединения. Disconnect уничтожает контекст, отменяет его HEAD/streams, отклоняет его pending и запрещает поздние ответы.

Повторный `auth` и несколько параллельно завершающихся аутентификаций одного socket не создают несколько каналов. После серверного `authenticateConnector` повторно проверяются actual socket и принадлежность admission. При замене соединения предыдущий канал больше не принимает claim/observations/ACK, даже если его событие `close` ещё не доставлено.

## 3. Типы и immutable target

Все сообщения — JSON objects. Обязательные поля проверяются по типу и границам; неизвестные поля typed control DTO не используются для прав или маршрутизации. Неподдерживаемые версия/profile отказывают без downgrade. Идентификаторы, порт, путь и digest проходят те же семантические ограничения с обеих сторон; серверная валидация не заменяет runtime-входную проверку.

```js
Target = {
  appId,          // app- + 32 lowercase hex
  revision,       // positive safe integer
  digest,         // 64 lowercase hex
  ownerAccountId, // validated textId; metadata, не полномочие
  port,           // 1024..65535, без control/blocked ports
  entryPath,      // <=8192 символов; local path по runtimePath-контракту
  profile        // ровно soty.relay-restricted.v1
}

Pins = { appId, revision, digest, profile }
```

`connectorKey` выводится из captured `linkId|hostDeviceId|connectorId` этого соединения; вызывающая сторона не подменяет его в target. Runtime пересчитывает существующий digest через уже доступный `deps.digest`:

```js
sha256(JSON.stringify([
  'soty.runtime-target.v1',
  target.appId, target.revision, target.ownerAccountId,
  authenticatedConnectorKey, target.port, target.entryPath, target.profile
]))
```

Формула исторического target1 не меняется. Независимые fixed vectors должны связывать серверный `runtimeTargetDigest` и встроенный runtime. Одинаковый digest, присланный с другим портом/путём/owner/connector, не принимается. Это проверка целостности конфигурации, не подпись кода автора.

Исходный допустимый `entryPath`, включая hash-SPA fragment, сохраняется в digest и target без нормализации. Для реального HTTP HEAD после `runtimePath(originalEntryPath)` и проверки порта выполняется единственная URL serialization:

```js
const probeUrl = new URL(originalEntryPath, `http://127.0.0.1:${validatedPort}`);
const httpPath = runtimePath(probeUrl.pathname + probeUrl.search);
```

Это удаляет fragment, percent-encodes Unicode и нормализует dot-segments так же, как навигация браузера. Не применяется `decodeURI`, повторное кодирование строки или перестройка query через URLSearchParams. Исходный query сохраняет порядок и `%2B/%23/%2F`. Origin никогда не берётся из пользовательского URL; unsafe origin, нормализованный `//`, encoded path и служебный `/_soty` отсекаются общим runtimePath-контрактом до HEAD.

Повторный `runtimePath` проверяет именно сериализованный HTTP target, в том числе существующий предел8192. Например, `/` +2000 кириллических символов имеет допустимую исходную длину2001, но даёт сериализованный pathname12001; такой кандидат получает correlated `invalid_app_path` до HEAD. Это не меняет исходный `entryPath`, его digest или исторические targets и не закрывает соседние приложения.

Независимый loopback check критика на Node24.13.1 сравнил фактический путь пяти HEAD с WHATWG `fetch` HEAD: `/страница?ключ=чай#раздел` → `/%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B0?%D0%BA%D0%BB%D1%8E%D1%87=%D1%87%D0%B0%D0%B9`; `/docs/../board?tag=a%2Bb#item` → `/board?tag=a%2Bb`; `/%2e%2e/board?tag=%23one` → `/board?tag=%23one`; `/#/dashboard` → `/`; `/board?tag=a%2Bb&x=%2F#item` → `/board?tag=a%2Bb&x=%2F`. Прежний raw `httpRequest.path` для Unicode дал `ERR_UNESCAPED_CHARACTERS` без запроса; для остальных мог отправить fragment/dot-segments на провод. Это evidence для изменения C2-B, не утверждение, что изменение уже внесено.

## 4. Per-app binding-set и ACK

```js
// server -> connector
{ type: 'binding-set', channelId, syncId, target: Target }

// connector -> server; после установки конфигурации, без ожидания HEAD
{ type: 'binding-ack', channelId, syncId, ...Pins }

// connector -> server; корректная tuple не разрешена локальной политикой
{
  type: 'binding-rejected', channelId, syncId, ...Pins,
  error: 'invalid_app_port' | 'invalid_app_path' | 'unsupported_profile'
}

// server -> connector; <=100 условных удалений
{ type: 'binding-remove', channelId, bindings: [{ appId, syncId }] }
```

`syncId` — свежий 43-символьный base64url nonce для данной установки. Сервер отправляет отдельный bounded кадр для каждого изменившегося app. Неизменившаяся tuple на том же соединении сохраняет свой `syncId`/ACK при unrelated sync. Изменение tuple, удаление с последующим возвратом и reconnect требуют нового `syncId`. ACK прошлой target1 не восстанавливается из исторического кеша при A→B→A.

Runtime полностью проверяет target и digest до изменения карты. Для изменившегося app он закрывает старые streams, отменяет старые probes, атомарно ставит новую запись, затем ACK. Повтор того же `syncId` с точно тем же payload может повторить ACK; тот же `syncId` с иным payload — ошибка протокола. Нельзя оставить старую запись открытой после неудачной проверки новой конфигурации и объявить новый ACK.

Корректный bounded envelope/pins с совпавшим digest может не пройти локальную политику: например, сервер не знает дополнительный control port конкретного connector. Это `binding-rejected` только для данного app, не отказ соседним приложениям. Сервер очищает прежнюю attestation сразу при изменении desired tuple; runtime до такого correlated отказа удаляет прежнюю binding/streams этого app. Старый порт не остаётся fallback. Сервер хранит bounded reason per app для owner read-model, но не принимает его за HTTP health. Malformed shape, другой channelId или digest tampering остаются ошибкой протокола.

Сервер не отправляет unsafe historical entryPath в `binding-set`: проверяет его общим runtimePath и помечает только эту app как недопущенную. Owner metadata/edit/revoke остаются доступны. Новый source всегда проходит строгий runtimePath до prepare. Это не отменяет локальную проверку портов/пути/profile на connector и не требует падения всего канала от легального отказа конкретному источнику.

Удаление условно: снимается только указанная текущая пара `appId/syncId`. Поздний remove не удаляет более новую binding. Отсутствующая пара — безопасный no-op. Сервер сначала убирает obsolete bindings, затем добавляет новые, чтобы runtime-карта не превышала 100. Отдельный remove-ACK не нужен для права открытия: сервер немедленно исключает запись из своего допуска.

Сервер хранит desired/ACK map не более 100 активных apps на connector и проверяет её против актуального DB target. Изменившиеся `binding-set` отправляются синхронно отдельными кадрами; каждый меньше 72 KiB UTF-8, суммарная отправка и `bufferedAmount` ограничены 4 MiB. Ошибка/backpressure не помечает неотправленные записи подтверждёнными. Канал закрывается либо остаётся честно недоступным с повторным согласованием; пользовательский запрос не отправляется на старый порт. Бесконечных control/retry queues нет. Повтор control использует тот же `syncId` и bounded deadline 5s; истёкший ACK не подтверждает новую generation.

**Синхронные control handlers не учитываются как незавершённые promises.** Реальный WebSocket parser может доставить 100 сообщений до выполнения `.finally` первого async handler. Лимит применяется к настоящим асинхронным работам (`data`/prepare); HEAD/probe имеют отдельные пределы. Ограниченная отправка 100 controls не требует отдельной очереди на 8 элементов. Приёмка обязана использовать реальный socket burst, а не только callbacks с искусственным `await` между кадрами.

## 5. Открытие и существующие stream frames

```js
{
  type: 'bound-open', channelId, syncId, ...Pins,
  id, kind, path, method, headers
}
```

`id`, `kind`, методы, фильтрация headers и byte/time limits сохраняют прежний контракт relay. На сервере перед открытием заново проверяются branded AccessDecision, active target, sticky floor, actual текущий канал и exact ACK. На runtime до создания любого `httpRequest` сравниваются `channelId/syncId/appId/revision/digest/profile` с установленной записью. Порт выбирается исключительно из этой записи. Нет поиска «какого-нибудь app с таким appId» и нет fallback на неподтверждённый target.

Нет ACK — отдельная контролируемая ошибка `app_binding_pending`/503. Нет подходящего binding protocol — `app_source_protocol_required`/503. Пользовательские HTTP bodies не копятся в очереди ожидания ACK. Нет автоматического повтора запроса с возможным эффектом. UI/клиент может повторить явное новое открытие после восстановления.

`requiredBindingVersion=2` никогда не обслуживается режимом 1, даже при `activeTargetRevision=1`. Legacy sync исключает такие apps; launch/session/open/active stream recheck отказывают. Нетронутый floor1/target1 остаётся совместим со старым connector. При согласованном режиме 2 даже floor1 открывается через exact binding, без смешения legacy событий.

После открытия stream хранит captured connection и binding object/pins. Прежние `head`, `data`, `ack`, `end`, `cancel` разрешены только для этого stream в этом connection. После `await write`, chunk ACK, response/upgrade callback и перед каждой последующей отправкой повторно проверяется актуальность stream/context/binding. Отмена старого stream не удаляет новый stream при совпавшем identifier. Смена source закрывает streams этого app, не соседние приложения. Existing limits32 total /24 public, bytes и сроки B2 не ослабляются.

## 6. Transient prepare и наблюдения

```js
// server -> connector
{ type: 'target-prepare', channelId, nonce, target: Target }

// connector -> server; candidate policy/admission отказ без HEAD
{
  type: 'target-rejected', channelId, nonce, ...Pins,
  error: 'invalid_app_port' | 'invalid_app_path' |
    'unsupported_profile' | 'app_prepare_busy'
}

// connector -> server
{
  type: 'target-prepared', channelId, nonce, ...Pins,
  state: 'responding' | 'unreachable',
  httpStatus // integer200..599 при ответе, иначе null
}

// connector -> server, относится только к установленной binding
{
  type: 'bound-observation', channelId, syncId, ...Pins,
  state: 'responding' | 'unreachable',
  httpStatus // integer200..599 при ответе, иначе null
}
```

`nonce` — свежий server-owned 43-символьный base64url correlation. Server preparationId остаётся отдельным UI/model identifier; он не является разрешением runtime. Подготовка не ставит active binding и не разрешает `bound-open`. Candidate может иметь ещё не сохранённую next revision либо точную immutable историческую revision для возврата.

`target-rejected` отделён от сетевого `unreachable`: запрещённый local control port или busy не выдаются за результат HEAD. После проверки bounded envelope/pins и digest policy отказ коррелируется с исходными nonce/pins, завершает только этот candidate и не снимает работающую active binding. Malformed shape/channel/digest не превращаются в мягкую ошибку. UI переводит только фиксированный allowlist, а не показывает произвольное сообщение runtime.

HEAD идёт на `127.0.0.1`, точный разрешённый порт и сериализованный `pathname + search` по §3; исходный target/digest не меняется. Deadline3s, `agent:false`, без авторизационных заголовков, cookies, shell, curl и следования redirect. 200–499 дают `responding`, 500–599/transport failure/timeout — `unreachable`. Unknown/malformed status не объявляется успехом. Digest/profile проверяются до HEAD. Socket/nonce/target перепроверяются после HEAD; старый результат не пересылается на новый канал.

Одновременно не более 4 checks на connector, включая in-flight. Две вкладки одного app разрешены в пределах общего server registry:256 всего,4/account,2/app. Превышение даёт bounded busy, не очередь. Повтор nonce не создаёт второй HEAD; другой payload с тем же nonce отказывает. Disconnect/expiry удаляет слот и отменяет request; запоздавший callback не освобождает слот другого check. Transport ACK deadline5s, preparation expires30s **от начала**, измеряется сервером, а не присланными часами connector. Поздний ответ не продлевает срок.

Периодические probes активных bindings отдельно ограничены: один coalesced sweep, не более четырёх параллельных HEAD и не более одного на binding. Interval20s не запускает бесконечно наложенные sweeps. При смене binding результаты старого sweep пропускаются. Наблюдение имеет exact pins и server receipt time; freshness read-model остаётся45s. Успешный ordinary response может обновлять только собственные pins и не превращает config ACK в функциональную проверку.

Для дедупликации connector хранит завершённый nonce/result отдельно от четырёх in-flight HEAD: не более256 небольших записей на context, срок30s от получения, без продления от повтора. Хранятся digest/pins/result, большой исходный path завершённой проверки не нужен. Заполненный cache даёт `app_prepare_busy`, живые nonce не вытесняются. Этот cache не является серверным runtime proof и не продлевает его срок.

Перед COMMIT модель проверяет current owner/device, исходные CAS, preparation TTL, точный pending target и current runtime proof/socket. Сетевых `await` внутри SQLite transaction нет. После COMMIT source receipt и pointer уже авторитетны: старая связь инвалидируется, новый binding-set/ACK ещё может быть в ожидании. Потеря этого уведомления не откатывает target; retry того же сохранённого intent восстанавливает historical receipt и безопасно повторяет согласование, не эффект. Rollback проходит свежий prepare и новый syncId; floor остаётся2.

## 7. Совместимость модели, регистрации и reader

- Рабочий источник берётся из publication active pointer, не из исторического `local_apps.connector_key/port/entry_path`. Duplicate-port admission, register replay, revoke/sync учитывают active target. При переходе между connectors синхронизируются старое и новое соединения.
- Apps4 хранит `requiredBindingVersion` независимо от target revision. Нет head — controlled corruption. Reopen не дополняет head значением1. AccessDecision/read-model получает значение из model SSOT helper.
- C2-A ещё не подключает prepare/promote HTTP operations. Он умеет читать Apps4 и обязан отказать head2. Такой бинарник не является работающим v2 fallback.
- Чтение схемы4, декларация Docker reader и v2 connector/server support — отдельные условия выпуска. Нет обещания совместимости старого already-running writer. Production sole-writer/backup/fallback gates из основного контракта сохраняются.

## 8. Обязательные проверки до C2-B freeze

1. Negotiation: old server/new connector, old connector/new server floor1; floor2/target1 не попадает в legacy sync/open. Explicit unsupported mode/profile не понижается.
2. Fixed digest vectors и tampering port/path/owner/revision/connector. Ни один ошибочный `bound-open` не достигает spy или реального upstream.
3. Per-app ACK lost/retry, mismatched syncId, conditional remove, A→B→A, unrelated sync, reconnect. Reconnect требует нового ACK, даже для той же tuple.
4. 100 реальных WebSocket controls burst без yield и 100 максимальных entry paths: byte limits, bounded map, отсутствие ложного processing overflow/giant frame.
5. Старый socket после reconnect: HEAD, prepare, write callback, response/upgrade, ACK и close не меняют новый context. Смена binding в одном socket имеет тот же барьер.
6. Два реальных upstream порта: old open идёт только в exact old binding либо отменяется; новый appId mapping никогда не переадресует уже допущенный запрос.
7. Prepare200/401/404/500, no redirect-follow, все пять фактических serialized paths из §3, timeout/disconnect/expiry; отрицательные `/x/..//double`, `/%2F_soty/session`, `/%2e/_soty/session` и исходный допустимый Unicode path с сериализованной длиной выше8192 не вызывают HEAD. 4 checks/connector и две вкладки/app bounded, карта active bindings не меняется. Legitimate blocked port/unsupported profile/busy отклоняет candidate без падения соседних apps; отказ новой active tuple закрывает только старую binding этой app без fallback.
8. Commit до sync ACK/lost notification остаётся committed с honest pending; до COMMIT все ошибки не меняют target/epoch. SQLite transaction не охватывает network wait.
9. Generated immutable single-file connector выполняет те же negotiation/exact-open/race tests; source test или поиск строки в bundle не заменяет поведенческую проверку. SHA/manifest/source/version compatibility проверяются отдельно.

## 9. Сохранённые red repro из preflight

Это два read-only запуска старого `createLocalAppsRuntime`, а не новые production tests. При реализации они переносятся в author regression suite, сначала фиксируют старое поведение, затем проходят на исправленном runtime. Тесты не должны ослабляться до «соединение когда-нибудь восстановилось».

**R1: настоящий 100-message burst.** Node `ws` server на случайном loopback port принимает `auth`, отправляет `ready` и сразу 100 небольших `ack` для неизвестного stream без `await` между `send`. Client — `globalThis.WebSocket` внутри настоящего `createLocalAppsRuntime`. Через500ms получено `{closed:true,connected:false}`. В runtime в момент preflight `message` делает `processing++`, вызывает async `onFrame`, уменьшает счётчик лишь в `.finally`; условие `processing>64` закрывает честный bounded burst. Future acceptance использует 100 валидных `binding-set`, получает 100 ACK и остаётся connected. Это реальный local network, не симуляция event loop.

**R2: старый HEAD на новом socket.** Dependency-injected runtime с управляемыми socket/HTTP callbacks: соединениеA получает sync appX/port4101, его HEAD удержан; `stop/start` создаёт соединениеB с тем же appX/port4102; затем завершается старый HEAD200. Получено `{oldProbePort:4101,newBindingPort:4102,oldProbeObservationsOnNewSocket:1}`. Future acceptance требует ноль, затем проверяет, что собственный HEAD B даёт exact pins B. Аналогичная ветка проверяется без reconnect при смене source того же appId. Это deterministic injected-runtime proof, не браузерная/production проверка.

## 10. Граница текущего документа

Wire contract согласован с root и автором persistent модели. Runtime source не менялся в этом preflight. Apps4 reader, real source switch, frontend, bundle и production deployment не приняты этим документом. Найденные R1/R2 — подтверждённые причины новых инвариантов, а не доказательство их реализации.

## 11. Независимый ограниченный review C2-A

После root freeze отдельно прочитаны `modules/apps/server/index.mjs`, shared `requiredBindingVersion`, decision/recheck и `runtime-binding-floor.test.mjs`. Последний запущен независимо: **3/3 PASS**, без skip. SHA256 просмотренного `index.mjs`: `74f50e8024b98a62714072eee2a1d4e3653d9ec2eafb446e96d23ef41dd090db`; теста: `edc0c01f5f523cee96584c85ca61dd81ea685dbf7d18978728f51d13da6280d8`.

Дополнительный inline probe использовал fixture реального сервера из того теста, настоящие HTTP/WS и внешний SQLite writer, без изменения source. Сначала успешно выдана cookie и проверен session GET200; начат удерживаемый ответ с текстом `before`. Затем writer повысил только floor1→2 без epoch, а исторический wire peer прислал следующий `data`. Получено: session GET403, новое HTTP с той же cookie403, новый guest HTTP503, прежний поток закрыт, `after` не доставлен, новых legacy `open` нет. При первом запуске вспомогательного loader была исправлена только резолюция npm-import `ws` из data-URL; fixture не начиналась. Финальный probe завершился успешно, созданные им временные БД/сервер удалены штатным cleanup.

В пределах C2-A новых блокеров не найдено: launch, ticket POST после await, существующая session, HTTP/WS open и active stream recheck отказывают floor2; sync не отправляет floor2; observation не выдаётся за актуальную; register/revoke обращаются к active target; retained zones проверяются и для v4. Это не независимый аудит всей миграции/reader и не доказательство C2-B. Actual browser, TLS, два реальных локальных приложения, source prepare/promote и immutable bundle остаются следующими gates. R1/R2 относятся к будущему runtime и не закрыты этим review.
