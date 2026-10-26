# Подключить своё приложение к Сотам

## Общий вход для совместимого relay-приложения

При уже активном входе в Соты приложение может запросить идентичность для своего
HTTP-запроса с помощью общего browser SDK:

```js
import { createRelayIdentityFetch } from '@soty/source-app/browser';
const sotyFetch = createRelayIdentityFetch();
const response = await sotyFetch('/api/items');
```

Коннектор доставляет подписанное подтверждение только серверу приложения; оно не
попадает в URL или ответ браузеру. Сервер использует один общий проверяющий модуль,
настроенный **своим доверенным** issuer и appId:

```js
import { createRelayIdentityVerifier, createSqliteRelayNonceStore } from '@soty/source-app/server';
const nonces = createSqliteRelayNonceStore({ databasePath: trustedNonceDatabasePath });
const identityFor = createRelayIdentityVerifier({ issuer: trustedSotyIssuer, appId: trustedAppId, consume: nonces.consume });
// В обработчике запроса:
const person = await identityFor(req); // { issuer, subject }
// Проверить собственные права person и работать с его данными.
```

Пара `issuer`/`subject` совпадает с Root workspace@2; использовать её как
устойчивый ключ своей базы. Заголовки/JSON из браузера, имя профиля и адрес
приложения сами по себе не подтверждают личность. Проверка привязана к appId,
методу, пути и короткому сроку, одно подтверждение принимается один раз.
Экземпляры store и verifier создаются один раз на процесс, не на каждый запрос;
при штатной остановке вызовите `nonces.close()`. Путь — абсолютный, в защищённой
уже существующей папке и указывает на **отдельную** базу SDK, не на базу данных
приложения. Для одного компьютера все процессы используют один файл на локальной
файловой системе с исправными блокировками и fsync. SDK не принимает чужую базу
и не создаёт таблицы в ней. В Windows права на папку и файл задаются ACL самой
системы; POSIX mode не подтверждает их.

Для нескольких хостов передайте вместо SQLite собственный `consume(id, expiresAt)`
с устойчивым атомарным хранилищем, общим всем работникам. Без явного `consume`
verifier не создаётся: скрытого in-memory fallback нет. Нехватка места, отказ
записи, превышение квоты или обратный скачок часов вызывают отказ 503; просроченные
случайные идентификаторы удаляются при следующем запросе. Не записывайте proof в
логи и не возвращайте его клиенту.

Подписанная личность не заменяет ACL и разделение записей на стороне приложения.
Обычный relay предоставляет маршруты всего указанного порта; выбранный Source
ограничивает маршруты и остаётся отдельным режимом. Старый Native аккаунт и его
данные автоматически не связываются по имени или email. На анонимном входе
подтверждение не выдаётся; несовместимый коннектор возвращает ошибку вместо
непроверенного входа. Данный прототип поддерживает HTTP-запросы, не авторизует
WebSocket и не является автоматическим подключением существующих приложений.

### Повторяемые операции в обычном relay

Обновлённые Root и коннектор передают стандартный HTTP-заголовок
`Idempotency-Key` приложению. Если подключённый коннектор не объявил эту
возможность, Root возвращает 503 `app_idempotency_key_unavailable` **до отправки
запроса приложению**; обновите коннектор и проверьте состояние операции перед
повтором. Старый Root может молча убрать заголовок, поэтому сначала необходимо
обновить и проверить обе стороны. В выбранном Source действуют его собственные
requestId и receipt, а не этот заголовок.

Заголовок сам по себе не исключает повторную запись. Приложение должно связать
ключ с проверенным пользователем и конкретным содержимым запроса, атомарно
сохранить эффект и свою квитанцию и предоставлять проверку результата при
действующих правах. Это обычный заголовок: он не подтверждает личность и не
включён в подпись identity proof. Потерянный ответ, 403 после отправки или разрыв соединения
не доказывают, что запись не произошла; сначала проверьте исход без повторного
изменения. Root и SDK не выполняют автоматический повтор такой операции.

Поле `admission` в индексе SDK описывает допуск workspace/Source: в частности,
статический реестр профилей и его ограничение 64. Для обычного relay эти pins не
нужны. Значение поля само по себе не подтверждает и не запрещает relay-вход;
совместимость действующих Root и коннектора проверяется реальным запросом.

## Сохранение незавершённой работы внутри окна

Это необязательный интерфейс для совместимых приложений, а не перенос их данных.
Приложение хранит черновик само и сообщает Сотам о его изменениях:

```js
import { createSotyAppWork } from '@soty/source-app/browser';
const work = createSotyAppWork({
  parentOrigin: trustedSotyOrigin, // точный доверенный origin из конфигурации приложения
  isDirty: () => draftHasUnsavedChanges(),
  save: async () => { await saveDraftDurably(); },
});
// Вызовите сразу после каждого изменения или успешного сохранения черновика:
work.changed();
// При окончательном удалении обработчиков экрана: work.dispose();
```

Перед закрытием или обновлением Соты проверяют это окно и, если требуется,
один раз просят сохранить черновик. Если приложение сообщает об ошибке или
не отвечает, Соты удерживают окно; следующая попытка — только действие человека.
`save` должен завершаться только после сохранения, а `isDirty` — возвращать
`false` только когда текущая работа действительно сохранена. Приложение отвечает
за свой аккаунт, данные и одновременные изменения. Мост не читает DOM, не
предоставляет доступ и не передаёт личность.

Несовместимые приложения продолжают открываться и закрываться как прежде, но
сохранение их черновиков внутри iframe не подтверждено. Перед обновлением Сот
человеку нужно сохранить работу и закрыть такое приложение или скрыть его виджет.
Браузер может закрыться,
а страница внутри iframe — перейти самостоятельно; для этих случаев необходимо
обычное автосохранение приложения. Встраивание на новом origin не означает,
что приложение доверяет ему: `parentOrigin` задаётся его владельцем.

Автор выбирает проверенный Source, задаёт название и доступ, затем проверяет
подключение. Соты предоставляют вход, общий способ найти действия и обратную
связь. Данные, аккаунты, роли и разрешения внутри приложения остаются у приложения.
Единый профиль Сот — стандарт для новых совместимых мини-приложений. У старых
приложений отдельный вход сохраняется до безопасного перехода; статическое
подключение интерфейса без интеграции SDK не передаёт личность пользователя.

Этот пакет добавляет один переносимый серверный BFF/router и клиент обратной связи.
Он переиспользует `openid-client@6.8.4`, общий Source RP и проверенный Apps8 transport.
Приложения, реализующие фиксированный `soty.resource.v1`, используют один compiled
adapter. Второй Source realm не требует нового кода или формата базы Сот.
Установленный connector должен содержать этот compiled adapter: старый binary
отклоняет новый pin. Публикация нового connector/release остаётся отдельной проверкой.
Manifest, регистрация, одобренный профиль и установленный пакет сами по себе не
означают работающее подключение.

Для **нового** личного сервиса есть короткий путь в существующем CLI:
`soty-source author --title "Мой список" --template reading-list`, затем
`check/init/serve --config PRIVATE_FILE` с явно выбранным Root workspace2
профилем. Optional `createSqliteRootWorkspaceHost` уже содержит Source session,
шифрование, nonce, personal issuer/sub isolation и атомарные receipt/recovery
fences. Приложение пишет только синхронные data callbacks и обычный UI.
Иные базы и существующие Native аккаунты используют те же storage-neutral
Native/storage ports; автоматического переноса или Root-owner-all-data нет.
Пошаговый путь, ограничения и честные статусы `doctor` — в
[`install/README.md`](install/README.md#новый-личный-сервис-root-workspace).

## Существующий сайт и личный микровиджет

1. Оставьте сайт на его сервере. Подключите на этом сервере коннектор Сот и
   привяжите устройство к своему аккаунту. Приложение должно быть доступно
   коннектору на локальном порту и поддерживать открытие в iframe.
2. В Сотах выберите «Добавить приложение»: устройство, порт, название и доступ.
   В «Другая начальная страница» можно указать путь, например `/compact`, если
   такой экран уже реализован вашим приложением. Полученная карточка запускает
   сайт через существующий коннектор; для обычного UI Source SDK не требуется.
3. Пользователь открывает нужную страницу через карточку или ссылку запуска Сот
   с нужным `path`, затем выбирает «Закрепить виджет» в меню приложения. Это его
   добровольный выбор. Виджет можно перемещать, скрывать и снова показывать.

Отдельного widget manifest нет: достаточно адаптивного интерфейса или обычного
компактного маршрута приложения. Закрепляется путь запуска Сот; внутренние
переходы iframe автоматически не становятся новым сохранённым deep link.
Для выбранного Source сохраняется одобренный entry `/embed`: закрепление не
добавляет ему произвольные маршруты и не расширяет права.

Состав, пути, видимость и положение окон сохраняются локально в этом браузере
отдельно для каждого аккаунта. Это не синхронизация между устройствами; при
недоступном хранилище интерфейс сообщает, что настройки не сохранены. После
повторного открытия выполняется свежий запуск с проверкой текущего доступа.
Layout не хранит билеты, Source handles или grants и не продлевает истёкшие права.
Сохранение внутренних несохранённых черновиков стороннего iframe не гарантируется:
их долговечность обеспечивает само приложение.

Для общего входа приложение подключает единый relay SDK выше либо совместимый
Source workspace. Source также предоставляет Native ACL, контекст и действия
агента. Простая UI-публикация не переносит cookies и авторизацию исходного домена.
Ни новый Root fork, ни отдельный транспорт для виджета не нужны.

## Что приложение предоставляет

- `createSourceNativeAuthorityPort`: получить настоящий Native proof из своей
  сессии/сохранённой связи и проверить текущие права на точный выбранный ресурс.
  Root account, email, имя, HTTP header или JSON формы не заменяют эту проверку.
- `SourceStoragePort`: долговечные encrypted interaction/session/one-use completion
  receipts, атомарные CAS и nonce store. PKCE, AT/RT и cookie никогда не попадают
  в Root/browser DTO или журналы.
- Для нового Standard2 — Native `rememberLogin/recoverLogin`: Source сохраняет
  независимый Native proof до OIDC и проверяет его текущую сессию/ресурс после
  настоящего HTTPS callback. Marker остаётся в encrypted Source storage и не
  является разрешением из JSON.
- Необязательные `read`, `execute`, `readProof` и `feedback` hooks. Отсутствующий
  hook даёт явное «ещё не подключено». Source сам разделяет reporter/support;
  владелец приложения в Сотах не становится Source support или владельцем данных.

Для связи старого аккаунта нужны одновременно новая проверенная OIDC identity и
текущая независимая Native сессия. Автоматического объединения по email нет.
Реальная проверка двух proof находится в Native `capture(binding, browserRequest)`
и final `linkVerifiedIdentity` внутри транзакции; отдельного невызванного
`verifyLegacy` hook нет. Приложение обязано проверить обе части и текущую сессию.
Explicit guest policy может создать только новый пустой Native principal/resource;
права на старый выбранный ресурс таким способом не создаются.

`execute` и feedback `submit/reply/status/accept` получают private branded
`commit(action)`. Native вызывает его ровно один раз **внутри своей транзакции**,
после проверки operation-specific роли, текущего ресурса и своей квитанции.
Async подготовка и network находятся вне транзакции. Native должен атомарно
сохранить эффект + receipt по requestId/точному digest/своему actor/resource.
SDK проверяет Native authority на границе записи; он не создаёт Native транзакцию
и не делает произвольный legacy write безопасным автоматически.

Для Source с async SQL (например, PostgreSQL) constructor может предоставить
`assertCurrent(proof,binding):Promise<void>`: настоящая свежая SQL-проверка вне
транзакционных locks, без permission DTO/TTL cache. Sync `withCurrent` тогда
остаётся final checkpoint внутри самой Native транзакции. Storage должен до
final callback захватить точные Native rows/locks через свой private request
context, а перед COMMIT выполнить свой final SQL/deadline check. Этот SPI
проверен на actual SQLite/current SQL и BFF; PostgreSQL consumer, миграция и
deployment требуют отдельной проверки. SQL snapshot в RAM не делает PG current.

После входа в mutating action ошибка, отзыв доступа или потеря ACK означают
`source_app_effect_unknown`, а не rollback. SDK не повторяет apply; Source
`readProof` возвращает квитанцию при текущих Native правах. Callback вне операции,
повторный callback, thenable и вложенный commit запрещены. Распределённой атомарной
транзакции Root–issuer–Source нет: начатый запрос может успеть записаться до отзыва.

## Запуск и границы проверки

### Текущий контекст стандартного приложения

Для Standard3 (`STANDARD_SELECTED_SOURCE_V3`, `soty.resource.v1`) общий Root
constructor `createCurrentContextSourceApplication({ appId, target, resource,
sourceProfile })` использует существующий `POST /api/embed/query`. Он сам задаёт
`{ requestId, input: { operation: 'soty.current_context' } }`: модель не выбирает
URL, метод, ресурс или opaque handle. Native реализует эту операцию в своём
существующем `read(proof, binding, args)` и возвращает:

```js
{
  schema: 'soty.current-context.v1',
  selection: { kind: 'soty.resource.v1', nativeId: '...', incarnationId: '...' },
  title: 'Текущий проект',
  role: 'editor', // owner | editor | viewer | reader; только описание
  revision: 1    // настоящая Native версия metadata-проекции, целое >= 1
}
```

`selection` должна точно совпадать с одобренным текущим resource. `title` —
непустая строка до 240 единиц UTF-16 без управляющих символов. Других полей нет.
Source BFF оборачивает результат в обычный `{ ok: true, data }`. Root отдаёт
агенту только `title/role/revision`, повторно читает свежую проекцию после каждого
модельного шага и запрещает результат при её изменении или потере текущих прав.
`role` не выдаёт права записи: любые действия отдельно проходят Native ACL.
В ordinary example `revision` — текущая SQL membership generation, а title/role
также перечитываются и сравниваются; это не версия списка записей или документа.
Пример нормализует только display title: управляющие символы заменяются пробелами,
граница длины не разрывает surrogate pair; сохранённое Native название не меняется.

Автор переиспользует три существующих шага:

1. Регистрирует работающий сайт через устройство/порт/путь и получает карточку.
2. Подключает Source SDK к своему Native proof/ACL; добавляет указанную ветку
   `read`, а для своих действий — существующие `execute` и `readProof`.
3. Использует уже предусмотренные Source admission, вход/consent и разрешённый
   каталог. Для разных приложений меняются данные scope, а не код Root.

Installed constructor, действующий Source grant и ready Native session остаются
обязательными; регистрация карточки сама их не создаёт. Skills/docs передаются
через существующий `externalApplications.guidance`, действия и квитанции — через
`query/invoke/receipt`. Этот мост предоставляет metadata выбранного resource из
профиля. Он ещё не реализует переключение дочерних проектов и не подключает
mutating Source tools к personal agent. Прежние HIVE GET и обычный Source
`/api/embed/context` DTO не меняются.

Сборка в checkout: `node modules/source-app/scripts/build.mjs`.
Она создаёт `dist/server.mjs`, `dist/browser.mjs` и SHA provenance. Вне checkout
server bundle зависит только от Node24 и pinned `openid-client@6.8.4`;
runtime import чужих дисков/репозиториев отсутствует. Пакет ещё не опубликован.

```js
import { createSourceAppBff, createSourceNativeAuthorityPort } from '@soty/source-app/server';
// privateHostConfig — реально одобренный Source/connector/client/resource профиль.
// nativeHooks и encryptedStorage реализует приложение; это не JSON пользователя.
const native = createSourceNativeAuthorityPort(nativeHooks);
const bff = createSourceAppBff({ ...privateHostConfig, native, storage: encryptedStorage });
// В своём HTTP server: if (await bff.handleRequest(req, res)) return;
```

Оригинальные cookies/Origin/CSRF, Native consent и actual OIDC проверяются на Source.
Transport key остаётся на trusted connector/Source hosts. Один exact profile и
fixed routes не допускают произвольные URL/команды/legacy endpoints. UI labels —
public текст trusted constructor, не resource authority. Собственный Native вход
нужно сохранять отдельно от `/soty/*` и `/api/embed/*`.
Source самостоятельно проверяет exact method/path/query multimap, Content-Type,
UTF-8 и byte limits перед dispatch. Неизвестный feedback suffix, повторный query
key и HEAD на мутацию запрещены независимо от compiled Root router.
Native intent/CSRF cookies имеют namespace exact approved appId: разные локальные
приложения могут входить параллельно на localhost. Порты не разделяют browser
cookies. В одном Native browser/app текущая cookie correlation допускает один
выбранный незавершённый Native form: новый вход заменяет старый, старый form
получает `source_app_intent_superseded`409 и требует явного повторного входа.
Standard1 так же проверяет callback cookie; Standard2 после authorize использует
свой долговечный captured proof + one-use CAS + текущий исходный Root slot.
Это не per-intent browser correlation и не гарантия параллельных входов одного app.
Fixed embed cookies остаются в приватном per-slot Root broker; прямые Source
origins на одном hostname нельзя считать изолированными только разными портами.

Пример приложения по умолчанию — **Basic300**, original Root slot и явный Native consent.
Импорт RP49 не включает long session автоматически. Без собственного renewal
consumer `session-continue` отвечает `renewable:false`; Basic не переносится на
новый Root slot.

Для уже выданной finite24h сессии host может добавить необязательный
`storage.findRenewableSession({context, semanticDigest}) -> {idHash} | null` рядом
с существующими `rpSessions` и атомарным `storage.rebindSession`. Hook вызывается
только в `POST /api/embed/session-continue`, когда Source cookie отсутствует,
после проверки свежего Root context. Native сначала определяет свой linked
account по verified issuer/sub, затем ищет среди ограниченного host лимитом
числа его активных Source sessions в точном пространстве приложения. Нельзя
сканировать/дешифровать чужие сессии или выбирать account из HTTP body.

Locator не является правом: BFF заново читает sealed session через `readSession`,
проверяет exact semanticDigest, Root account/device, issuer/sub, finite deadline,
`rpMarker` и `sha256(cookieToken) === idHash`, затем выполняет существующие
RP currentProof, Native ACL и guarded rebind с final callback внутри Native
транзакции. После свежих Root/Native проверок cookie возвращается только через
прежний HttpOnly/Secure/SameSite header; публичный ACK не содержит credentials.
Неверная присутствующая cookie не запускает поиск другой сессии. Нет hook,
Basic, expiry, logout/revoke или смена actor — восстановления нет. Потерянный
ACK восстанавливает ту же сохранённую сессию; BFF не повторяет OIDC issuance и
не заменяет unknown RP refresh новой попыткой. Pins/маршруты/тела RPC неизменны.
`test/durable-resume-http.test.mjs` проверяет два приложения и два Root actor,
реальные OIDC/HTTP/RP49/SQLite, clock >300s, restart, revoke и lost ACK;
Root authority IPC управляется fixture. Это не доказательство live установки.

Feedback идёт только в текущий Source контекст: private «Сообщить проблему».
RAM draft остаётся у исходного binding и не переносится при смене проекта/аккаунта.
Unknown ACK сохраняет тот же frozen request/body/media; Source обязан иметь
idempotent receipt. PNG/JPEG/WebP и Opus ограничены 1 MiB, 3 вложениями и 120 сек.
Выбор файла/preview не отправляет сообщение. ASR/OCR и публичные отзывы не
включены; managed reviews подключаются отдельным проверенным provider/binding.

Проверка: `node --test modules/source-app/test/*.test.mjs`.
Native SQLite commit/revoke/receipt restart проверены на synthetic данных.
BFF HTTP тест использует настоящий maintained Root OIDC и signed consent,
но controlled IPC/RAM store: он **не подтверждает installed channel**, durability,
production TLS, browser UX, long session или готовность стороннего приложения.
Эти проверки нужно отличать от настоящего installed/browser gate ниже.

## Исполнимый обычный Source

`@soty/source-app/example` содержит настоящее новое SQLite приложение и небольшой
интерфейс без React. Native роли, две независимые identity proof, selected consent,
item/feedback receipts и шифрование закрытого BFF состояния проверяются его
Source, а не полем owner в Сотах. Две разные realm работают с одним adapter;
повторный Source не требует Root code/DDL diff. Native support — только Native
владелец; reporter принимает решение о закрытии обращения.

Новый Source использует `STANDARD_SELECTED_SOURCE_V2`: callback находится на
проверенном embed HTTPS `/api/embed/callback`. Standard1/pin1 остаются неизменны.
Native format2 добавляет encrypted pending proof; migration1→2 явная до startup,
старый независимый reader1 отказывается от2, текущий reader принимает1,2.
Legacy Native IDs, receipts и encrypted basic bytes при migration сохраняются.

Проверены реальные signed Apps/Human, установленный локально compiled connector
HTTP/WS и actual Chrome HTTPS: два независимых realm, same iframe после входа,
Source restart, Native revoke/private-route denial, private feedback и mobile390.
Потеря Source ACK после COMMIT + consumed Root map восстанавливает прежнюю
completion receipt после явного нажатия в том же текущем slot: код не
обменивается повторно, новая identity/link/grant не создаётся. Root restart,
foreign/expired/revoked scope не оживляют Basic. Source suite29/29, browser happy
path13 checks и recovery6 checks проходят на synthetic данных.

Это **same-machine** локальный путь. `localhost` автора на SSH Dev недоступен
браузеру другого человека. Для remote publisher нужен отдельно проверенный,
доступный браузеру Native/public HTTPS origin и Source-owned entry/deployment.
`nativeOrigin=embedOrigin` сейчас не работает: compiled embed router не открывает
Native `/soty/connect` и `/soty/authorize`. Example server использует локальный
explicit port; remote consent/broker/onboarding и production image/TLS/release
остаются отдельными gates. Тестовый connector построен локально, не опубликован.

После completion UI проверяет актуальный `session-status` с backoff в том же
iframe; Root «Обновить приложение» создаёт новый slot и не является Basic
recovery. Long session, Source account switch UX, remote deployment, ASR/OCR,
agent queue и managed reviews здесь не внедрены. Точные evidence и команды —
`docs/implementation/source-app-standard2-acceptance.md` в Root checkout.

## Текущий проект внутри приложения

`STANDARD_WORKSPACE_SOURCE` (`soty.standard-workspace@1`) — отдельный opt-in
на существующем Source3 transport. Anchor `soty.workspace.v1` обозначает
стабильный namespace **установленного приложения**, не личный проект автора.
Один app/profile обслуживает разных Root людей; Native определяет доступные
каждому проекты. Старые `soty.resource.v1` и HIVE fixed profiles не расширяются.
Catalog resource и recipient — `workspace:<appId>:<resourceId>`; новый grant
должен явно разрешать этот scope. Descriptor/tool schema не создаёт grants.

Автор переиспользует `createSourceAppBff` и `createSourceNativeAuthorityPort`:

1. Синхронный `currentContext(proof,binding)` внутри `withCurrent` возвращает
   `{key,generation,expiresAt,title,role,revision}` из текущих Native данных.
   `key` включает child/incarnation и остаётся приватным. Native проверяет
   account, namespace, текущий проект и его ACL при capture, чтении и COMMIT.
2. UI сообщает выбор через прежний `POST /api/embed/query`:
   `{requestId,input:{operation:'soty.context.select',nativeId,generation}}`.
   Это ограниченный locator, не разрешение. `selectContext` проверяет Native
   ACL и порядок UI, увеличивает собственную generation и обновляет только
   private proof этого запроса. Остальные поздние proof остаются прежними.
   Root agent adapter запрещает эту operation даже в широком author schema.
3. `soty.current_context` возвращает версию `soty.current-context.v2` с тем же
   anchor и `{context:{ref,generation,expiresAt},title,role,revision}`.
   Maintained Native SDK также возвращает `continuityDigest`: открытый digest
   человека, устройства, namespace и Native child, не разрешение и не token.
   Он сохраняется при смене коротких Root/Source leases, но меняется при смене
   человека, устройства или child. Generation проверяется отдельно.
   Root хранит ref вне model; существующие query/invoke/receipt передают его
   отдельным `contextRef`. Native проверяет его до/после вызова и в COMMIT.

Ref связан с исходным Root slot, Source session, человеком, устройством и
namespace; срок не превышает пяти минут, Root и Native lease. Переключение,
ABA, revoke, close или expiry не позволяют использовать старый ref. Один BFF
по умолчанию держит максимум 128 актуальных leases в RAM. Host может передать
`createSourceAppBff({..., workspaceCapacity:{maxSlots:256}})`; допустимо целое
число 1–65536, только для workspace. Параметр проходит BFF → Native runtime →
authority и не принимается из HTTP, RPC, профиля или descriptor. Истёкшие leases
удаляются; переполнение отказывает новому lease, не вытесняя действующий.
TTL, account/device/slot isolation и Native ACL этим параметром не меняются.
Исчерпание slots возвращает `503 source_workspace_capacity`, а не отказ identity.
Действующие leases не вытесняются; повторный вход не освобождает чужие slots.
Клиент показывает временную занятость и допускает повтор чтения позднее.

Перед **новым** `prepareInvoke` браузерный SDK выполняет два чтения:
`session-continue` и `soty.current_context`. Новый ref допускается только при
совпадении `continuityDigest`, selection, generation и role. Это не требует
нового пользовательского нажатия. Несовпадение закрывает локальный контекст до
явного подключения. Старый Native без digest продолжает работать с прежним
ref; его смена требует явного подключения, без догадок по названию проекта.
Обычное чтение после `authentication_required`/`source_workspace_context_denied`
проходит тот же переход и повторяется максимум один раз. Отправленный intent,
включая unknown, никогда не перепривязывается к новому ref: только чтение
исходной квитанции через `recoverReceipt`. Это правило действует для любого
домена приложения, независимо от его полей и операций.

Это один повтор **business read на уровне workspace SDK**. Ниже работает
`createSourceFrameFetch`: отдельный безопасный RPC может ещё один раз пройти
после bounded wake-up у точного родителя. Поэтому HTTP-попыток может быть
больше двух; это раздельные ограниченные уровни, не цикл. `invoke`, feedback
записи и неизвестный сетевой результат никогда не повторяются автоматически.

Это раздельные release limits, не доказательство массовой нагрузки:

| Контур | Охват | Текущий предел |
|---|---|---|
| Workspace context leases | Один BFF/approved Source profile; все его текущие кадры/аккаунты вместе | Default128; только host может выбрать проверенный `maxSlots` |
| Root scoped Source continuations | Отдельный Root gateway/registry | 256 записей; не запас Source sessions |
| PG Source sessions | Отдельный квалифицируемый PG host adapter/его Source storage | 4096 коротких записей; это не число пользователей и не universal SDK default |

Пределы не суммируются. Root/device/app/profile, Native ACL, память, TTL и storage
проверяют отдельно; повышение host limit само не квалифицирует capacity. Doctor
может сообщать заданный предел и безопасный статус, без текущих счётчиков других
людей. Source RP renewal имеет собственный concurrency budget и не заменяет эти
leases/короткие Source sessions.

До первого выпуска SDK и установки workspace сделана явная финализация
пина `soty.standard-workspace@1`: `context.maxSlots:128` заменено на
`context.slotCapacity:'host-configured'`. Старый невыпущенный digest
`1f8f2251e41e5afcceacf811109f0aaeb32c0b7a80ea19d18b88c9fb83b2c254`
заменён на
`c1d52ca7865dfebf6ff05ee12f37e6d9d14e95b78928a5dee8f7defa331ddd99`
с описанной ниже привязкой instance;
старый digest отклоняется, alias нет. Пины Source1/2/3 не изменены. Fixtures,
installed profiles и Native bundle должны брать экспорт `STANDARD_WORKSPACE_SOURCE`.

Root имеет отдельный host-only `personalAgent.capacity`:
`{maxRetainedTurns:256,maxAccountContexts:256}`. Его можно передать через
`createPersonalAgentComposition({configuration:{...,capacity}})` или существующий
приватный installed JSON с полем `capacity`; каждый предел — целое 1–65536,
default128. `maxRetainedTurns` считает running, completed и unknown записи до
прежнего 30-минутного срока от начала, не только одновременные запросы.
`maxAccountContexts` ограничивает лишь fallback RAM history; штатная Notes history
его не использует. Живые turns/contexts не вытесняются. Эти параметры не меняют
128-account installed allowlist, grants, модельные бюджеты или HTTP/RPC контракт.

После эффекта Native вызывает `final.contextAfter()` **внутри** исходного
`final.commit(action)` и сохраняет результат в той же SQL-транзакции вместе
с исходной durable receipt. Закрытый объект содержит schema
`soty.context-after.v1`, mutationRef, generation и title/role/revision.
mutationRef связывает exact Native requestId, digest inner input и contextRef.
`readProof` возвращает original requestId/inputDigest/outcome/contextAfter и
`workspaceReceipt:{schema:'soty.workspace-receipt.v1',effects,receipt}` из той же
durable записи. `receipt` содержит только verificationMethod=`domain_read` и
artifacts. Root использует один стандартный projector; автор добавляет schema
и descriptor data, а не функцию `projectReceipt` в Root. Core проверяет declared
effects, точное соответствие effects/artifacts и действующие resources/recipients.
Legacy custom projector остаётся прежним; workspace его не принимает.
Новая revision принимается Root лишь после `succeeded + committed` и свежего
чтения того же private ref/generation/slot/actor/ACL. Unknown не повторяет write.

Только workspace `domain_read` receipt использует artifact.sha256 как hash
канонического `{schema:'soty.workspace-context-witness.v1',contextAfter,
artifactIdentity,requestId,inputDigest,sourceBinding}`. artifactIdentity —
type/id/optional revision; sourceBinding — appId/target/sourceProfile/resource.
Смысл старого `artifact_hash` не меняется. Модель не задаёт ref, generation,
mutationRef, witness или committed outcome. После рестарта RAM ref недействителен.
Native данные и original receipt остаются. Старый ref не становится разрешением;
повтор write не используется для восстановления результата.

### Recover an immutable workspace receipt

Workspace1/2 Native adapters may opt into the maintained recovery SPI. Keep the
original `result_json` immutable. Inside its **original** `final.commit(action)`,
after the effect and `final.contextAfter()`, obtain a separate durable binding:

```js
const original = { requestId, inputDigest, outcome: 'committed',
  contextAfter: final.contextAfter(), workspaceReceipt, receipt };
const binding = final.receiptBinding(original);
// Save original AND binding atomically in this same Native transaction.
saveReceipt(original, binding);
return original;
```

`receiptBinding` only accepts that commit's branded `contextAfter`, the exact
request/input digest and current post-effect metadata. It is unavailable after
the commit callback. Do not regenerate a binding for an old row after the fact.
The sidecar is MAC-sealed by the SDK, using a distinct purpose tag and the existing
BFF `transportKey`; no new key/configuration is required. Store it privately beside
the receipt. It binds the canonical original content, old ref, approved profile, Root
actor/device, human identity, resource and Native child/incarnation. It excludes
ephemeral Source session/Root reference IDs and selection generations. Those must
be checked afresh, not revived. A changed key/profile/actor/Native incarnation
fails closed; key rotation does not silently re-sign historical claims.
Before sealing, the SDK reserves the largest permitted fresh metadata/envelope
inside the unchanged 64KiB/node/depth budgets. An oversized original is refused
inside the Native transaction; it must not leave a committed effect/receipt.
The SDK still reports unknown if the mutation callback had already been entered.

The fourth `readProof` argument is a host-only recovery capability:

```js
readProof(proof, binding, args, recovery) {
  return nativeReadTransaction(() => {
    assertCurrentNativeReceiptRead(proof, binding); // own SQL ACL and row locks
    const row = readExactReceipt(args.requestId, args.input.inputDigest);
    if (!row.binding) return row.original; // legacy, exact live-ref check only
    if (!isSourceNativeReceiptRecoveryPort(recovery)) throw unsupported();
    return recovery.recover(row.original, row.binding);
  });
}
```

The SDK checks the sealed sidecar, exact original intent and **old** mutationRef
under the fresh Native/context fence. The response preserves every original field
and adds `recovery:{schema:'soty.workspace-receipt-recovery.v1',contextRef,
receiptDigest,contextAfter}`. Its `contextRef` is historical data, never authority;
its separate `contextAfter` attests the current read's ref/generation/metadata.
The capability is single-use, method-scoped, nonserializable and cannot be replaced
with a JSON envelope or a cloned returned object. No execute/apply is invoked.
The BFF repeats the private workspace lease fence after its last Root await and
before sending data; a late post-COMMIT denial still reports `effect_unknown`.

| Root protocol adapter | Source/Native adapter | Behavior |
|---|---|---|
| Old | Legacy receipt | Original strict same-ref behavior |
| Updated | Legacy or missing-sidecar receipt | Same strict behavior; a new ref cannot reconstruct a sidecar |
| Old | New recovery envelope | Unsupported extra field is refused; never downgrade it to a successful proof |
| Updated | Updated, with original sealed sidecar | Exact historical receipt plus fresh authorised recovery; no second effect |

Upgrade the maintained Root protocol adapter before enabling recovery in a
consumer. No Source pin, existing grant or role is broadened. Missing/unsupported
proof stays denied/unknown; do not invent a new requestId or retry the effect.
The same current Native read rights and actual Root checks are required for both
normal and recovered receipts. This SPI does not migrate a Root external
invocation, reservation, old handle or its immutable outer input into a new
session; that remains a separate Root authority gate.

`examples/ordinary-app/workspace-native.mjs` — реальный SQLite пример этих hooks.
`workspace-current-context-installed.test.mjs` проверяет два приложения/два
проекта и двух Root людей через настоящий local OIDC/HTTP/WS/SQL и agent executor.
Provider/accounting синтетические; жесты UI задаёт HTTP fixture. Это не browser
acceptance и не готовая HIVE интеграция: ordinary example UI/feedback остаются
fixed-resource; HIVE ещё должен подключить собственные account/storage/current
UI hooks и тот же SQL COMMIT fence. Новых служб и per-project Root сборок нет.


### Workspace instance binding

`apps.launch` accepts optional `instanceId`: `main` or an existing `widget-<uuid>`.
This UI locator is not authority. Root binds it to the fresh account/device/app/profile
reference and retains it during `apps.scoped.renew`. AppStage uses `main`; micro-widgets
use their persisted widget IDs. Independent main tabs are not represented.

Workspace Source sessions seal `session.instanceId`. Hosts implementing
`findRenewableSession({context,semanticDigest})` must select only a record with
`session.instanceId === context.instanceId`, alongside existing actor/namespace/Native
predicates. BFF independently checks equality for cookie proof, canonical reread and
rebind. Absence remains absence; old selected profiles and Basic lifetimes are unchanged.

The final prerelease workspace v1 pin adds `context.instanceBinding` and is
`c1d52ca7865dfebf6ff05ee12f37e6d9d14e95b78928a5dee8f7defa331ddd99`.
The earlier capacity-only `5306895df6e3f8b1bd1e2117c2a76f8a343dd3da4f18b7f8cb9a015a455c6a1c`
is rejected without an alias. Source1/2/3 pins are unchanged.

### Root-authenticated workspace2

`STANDARD_ROOT_WORKSPACE_SOURCE` is a separate explicit workspace@2 profile
(`23061fc95610e3011ec42b3ecf229308bce44b0a1268408ca9b6ee138c98f7e9`).
Workspace@1 above remains the RP/OIDC profile. Workspace2 accepts no `rp` or
`rpSessions` configuration and creates no OIDC interaction or refresh family.

Every current-authority read additionally verifies Root RS256 JWS through the
fixed issuer `/jwks`, using maintained JOSE from the existing openid-client
dependency. The dedicated audience/type, original random Source challenge,
full context digest and at-most-10-second expiry must match. Connector MAC is
still required but cannot replace Root's signature. No caller-provided key URL
or JWK is accepted. `readVerifiedRootContext(proof, profile.digest)` exposes
only the original deep-frozen SDK-branded context while its witness is valid;
a copied JSON object is never this private proof.

Native implements `captureRoot(binding, verifiedRootContext)` and synchronous
`commitRootSession(proof,binding)`. The latter runs inside the same transaction
as `storage.createRootSession(record,finalNativeLink)`, returning the existing
Native `{nativeSessionHash,principalId,resourceId}` tuple. Native owns identity
linking, account creation, revocation, ACL and final checks. A replay must still
run the final fence and return the original anchor. The shared SQLite helper
then returns the original Source `idHash`; BFF rereads its sealed random
`cookieToken` and checks its hash. This is not a browser account selector.

Session-continue may acquire a short `rootWitness:true` session only under that
fresh Root reference, account/device/app/profile/instance and Native predicate.
Its deadline is at most Root expiry and 300 seconds. A different fresh Root
reference acquires a new finite session; old rows/deadlines are not extended.
`renewable:true` here means a new authenticated Root acquisition, not an RT
family. Explicitly closed Root authority, revoked Source rows and foreign
cookies do not revive. Native durable UI preferences remain separate from all
session authority. This protocol alone does not promise automatic browser
return after 24 hours or independent main tabs.

Workspace2 may reuse the application's exact existing native/embed origin; the
parent origin remains separate. Its feedback reads and writes require the
captured workspace `contextRef`, checked against the same current Native child
before and after reads and inside the final transaction. Construct
`createSourceFeedbackClient({contextRef: context.ref})` for that captured field,
and recreate it after the workspace context changes. Legacy feedback clients
keep their existing requests. A recovered immutable receipt can precede the
current ticket revision; it retains its original ticket ID and revision.


### Public application, private per-user workspace

Only an exactly admitted `STANDARD_ROOT_WORKSPACE_SOURCE` (workspace@2) may use
`anyone` + `listed` for its application entry. Owner publication uses the closed
`exposureAck` scope `authenticated-workspace`, pinned to the exact target
revision, digest and runtime profile. It opens the maintained `/embed` routes
to authenticated Root users, not an arbitrary port or Native projects. Relay
publication keeps its existing `whole-port` acknowledgement. Other selected
resource and workspace@1 profiles remain restricted.

A signed visitor keeps their own Root account/device and Human subject even
when application access uses the public basis. The Source witness and Native
identity linking, current-child ACL and COMMIT checks remain mandatory. An
anonymous visitor can discover the application but cannot obtain a scoped
Source session, identity witness or private content. No per-user author grant
is needed merely to launch the publicly listed app; agent capabilities remain
separately granted and budgeted.

The Apps8 cold reader validates the exact stored workspace@2 admission and this
acknowledgement. Older binaries reject an active public selected target. Before
returning to such a binary, use an explicit owner publication CAS to make the
selected target restricted/unlisted (or explicitly return to a qualified relay
target), while the new reader still serves. Keep the live database; do not
restore an old database snapshot as a rollback shortcut.

### Same-origin main window and widgets

For workspace@2, Root boot supplies `/embed?sotyFrame=<public-locator>` and a
separate HttpOnly cookie for each frame. Wrap the application's existing Source
fetcher with `createSourceFrameFetch({fetch: existingFetcher})` from the browser
SDK; use that same wrapper for session checks, editor calls and feedback. It
adds `x-soty-frame` only to same-origin `/api/embed/*` requests. It creates no
credential and changes no business DTO or Source profile pin. Standalone pages
without the query retain their original fetcher.

Root consumes the selector and verifies the exact cookie, actor, app, target
and live scoped record before forwarding. Hidden renewal keeps the locator
and changes only that frame's cookie after its existing Source ACK/CAS. Main
and widget remain separate even though the browser shares one origin cookie
jar. Native `/embed` may remain an ordinary static entry; no extra proxy route
is needed for that page. Any direct Native fallback must strip both
`__Host-soty_app_session` and `__Host-soty_app_session_<43 base64url>` cookies,
and the `x-soty-frame` header. These Root credentials must never reach Native.

Only compiled credential-free assets and the fixed Native ingress check may
use another live frame cookie when the ordinary/base frame has closed. The
base cookie must still pass its own current publication check and fixes the
exact actor/device, Human subject, app, profile, target and resource. A bounded
scan considers only presented frame cookies with that same binding, then
rechecks live Root authority. Closing all matching frames denies this path;
a different account's surviving frame cannot supply authority. Private Source
routes always require their explicit frame selector. Forwarded paths are not
trusted as authority, and ingress returns no Source witness or business data.

### Browser quickstart: an existing app with a personal workspace

Use `createSourceWorkspaceClient` from `@soty/source-app/browser` after the host
has installed and qualified the maintained workspace Source profile. Bundle
the browser entry as an ordinary credential-free JavaScript asset, for example
`/assets/soty-source-sdk.js`. This helper does not register an application,
approve publication, replace the host's Native ACL, or grant a user's data.
The same client works with different app-owned operation names and DTOs.

```js
import { createSourceWorkspaceClient } from '/assets/soty-source-sdk.js';

const id = () => crypto.randomUUID();
let alive = true;
const source = createSourceWorkspaceClient({ isCurrent: () => alive });
const workspace = await source.connect({ requestId: id() });
const page = await source.query({
  requestId: id(), input: { operation: 'items.list' }
});
render(page.items, workspace.title);

// Prepare once for this explicit action, then retain the original intent.
const intent = await source.prepareInvoke({
  requestId: id(), input: { operation: 'items.create', title: 'My record' }
});
try {
  const result = await source.invoke(intent);
  // Read the actual app row/list before presenting its current state.
  render((await source.query({
    requestId: id(), input: { operation: 'items.list' }
  })).items);
} catch (error) {
  // An invoke error can follow COMMIT. Keep the same request/body/context.
  showCheckResultAction(async () => {
    await source.connect({ requestId: id() });
    const result = await source.recoverReceipt(intent); // read only, never invoke
    if (result.outcome === 'committed') {
      const currentWorkspace = await source.currentContext({ requestId: id() });
      const currentPage = await source.query({
        requestId: id(), input: { operation: 'items.list' }
      });
      render(currentPage.items, currentWorkspace.title);
    }
    else showNotApplied();
  });
}

// On navigation/account change: remove private UI, stop polling and dispose.
alive = false;
source.dispose();
```

`currentContext` uses the maintained
`{requestId,input:{operation:'soty.current_context'}}` bootstrap. Business reads
and writes carry its exact `contextRef`; callers do not provide a Root account,
Native session cookie, owner ID or credential. An app with multiple admitted
children can explicitly call
`selectContext({requestId,nativeId,generation})`. A new context invalidates
retained old intents and late replies. Clear the old UI before displaying data
from a newly acquired context. A local reference or successful bootstrap is
never a substitute for the server's fresh and final Native checks.
The main `connect` call performs `continueSession` and `currentContext` under
one local account/frame epoch. It acquires/checks the short HttpOnly Source
session under the already selected fresh Root authority, then reads the actual
current workspace. Both low-level methods remain available. The continuation
ACK does not replace a context or business read. Before a new write, and after
one recoverable read denial, the SDK refreshes the session/context only under
the same verified continuity. A changed or unavailable continuity requires
clearing the old private UI and explicitly calling `connect`; unknown writes
keep their original reference. No new UI confirmation is introduced when the
Root frame already has the required authority.

Each write requires an explicit unique request ID. Preparation captures an
immutable JSON body, canonical SHA256 digest, context and generation. Duplicate
activation of the same intent shares its single in-flight request. Once sent,
calling `invoke(intent)` again reads the original receipt instead of repeating
the effect. Changed body under a pending request ID is a conflict. A cloned or
JSON-restored intent cannot call `invoke`; a retained
`{requestId,contextRef,inputDigest}` may call `receipt` in that same current
context. If authority/context changed, keep the outcome unknown and ask the
person to check it in the original context; do not silently resubmit it with a
new context or a new ID. `not_applied` is a receipt result, not an assumption
made from a timeout,401,403 or a lost response.

Keep three separate facts in the UI: the **confirmed original receipt**,
the **separately refreshed current context**, and the **current domain
readback**. `recoverReceipt(originalIntentOrReference)` reads under the current
captured `contextRef` while retaining the original request ID, input digest and
old reference. Native must possess its durable sealed sidecar and recheck the
same actor/device/resource under the fresh Root and Native fence. The browser
validates the immutable original digest and old mutation binding, then the
separate `recovery.contextAfter` against the current context. It returns both;
it does not replace `result.contextAfter`, edit the original intent or resend
an effect. Domain receipts are app-owned JSON, such as `{accepted:true,etag:...}`;
the generic SDK does not require a CRUD `id` or integer domain revision.

The existing `receipt` method remains limited to its exact original current
reference. Use explicit `recoverReceipt` after a new frame/session or Source
restart. An older host without a sealed binding, an inaccessible original,
a changed actor/device/resource or a forged recovery remains unknown. In a
different context, `not_applied` without a sealed original is insufficient to
confirm the old action; the SDK rejects it and never falls back to `invoke`.
Applications should retain the original reference for checking and display
current business data from a new read, rather than treating the historical
receipt revision as the present state.

Reads use `createSourceFrameFetch`, including its exact-parent short-session
wake-up and at most one safe read retry. Invokes, unknown network outcomes and
private effects never enter that automatic retry path. All calls use fixed
same-origin `/api/embed/query`, `/invoke`, `/receipt` routes and the current
frame locator header, with HttpOnly authority remaining on the server. There
is no Native `/ws`, global cookie, Telegram or arbitrary URL fallback. JSON is
bounded by the maintained64KiB route budget, with duplicate keys, malformed
UTF8 and ambiguous request values rejected. The generic client validates the
original request/digest and workspace receipt/context binding; the Native host
still owns the business transaction and the correctness of its artifacts.

For read-only live data, use the application's Source `query` operation and
stop its timer while the page is hidden or disposed. Use one in-flight read,
an interval after completion (for example2500ms), a bounded page size/cursor,
and ignore results when the current local filter/context changed. Show the
server's waiting/stale/live status instead of treating an empty result as a
connection failure. A shared market cache may contain public facts while its
selection/subscription and delivery remain authenticated. This client does
not turn formerly restricted data anonymous, put leases/tokens in URLs or
introduce a new Source WebSocket contract.

The two minimal consumers in
`test/workspace-browser-installed.test.mjs` exercise the same browser client
against an actual local signed Root, installed connector and two Native SQLite
applications: board `items.create` and library `books.add`. They include an
unknown COMMIT ACK, read-only receipt recovery, exact context change refusal
and normal current-row readback. These are local protocol qualification tests;
production publication and a real browser/user rollout are separate gates.
`test/workspace-browser-cold-installed.test.mjs` also starts a Root-only Source
with zero RP client registrations and no pre-acquired Source/Native cookie,
then checks resume, expiry/revocation, departure between handshake stages and
sealed receipt recovery after Native restart/new Root frame. Its test-owned
Native private envelope retains the original SQL bytes; another Root person
cannot recover or read that person's records. Clock expiry is controlled, not
a long wall-clock uptime claim.

### Native policy failure navigation

Python `NativeEntryMiddleware` presents a finite error page for an explicit HTML document GET; its HTTP403/503 remains unchanged. Node hosts can call `sendNativeEntryHttpError(req,res,error)` from their existing HTTP error handler and keep their own fallback when it returns false. API/HEAD and WebSocket handling retain their existing denial semantics; a started response is terminated, never replaced with an error document.

The single manual link opens only `/` on the current Native origin. It does not reuse callback/code/state/query/bearer URLs, auto-refresh, repeat an effect, create a session or extend a lease. After an uncertain action, check its existing receipt/result before trying the action again. The two bindings embed the same static page; no unpinned runtime template is loaded. This UX does not diagnose a transport timeout or change the fixed policy/lease bounds.
