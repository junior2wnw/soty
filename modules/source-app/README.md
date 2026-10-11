# Подключить своё приложение к Сотам

Автор выбирает проверенный Source, задаёт название и доступ, затем проверяет
подключение. Соты предоставляют вход, общий способ найти действия и обратную
связь. Данные, аккаунты, роли и разрешения внутри приложения остаются у приложения.
Собственный вход и приложение только с интерфейсом остаются допустимыми вариантами.

Этот пакет добавляет один переносимый серверный BFF/router и клиент обратной связи.
Он переиспользует `openid-client@6.8.4`, общий Source RP и проверенный Apps8 transport.
Приложения, реализующие фиксированный `soty.resource.v1`, используют один compiled
adapter. Второй Source realm не требует нового кода или формата базы Сот.
Установленный connector должен содержать этот compiled adapter: старый binary
отклоняет новый pin. Публикация нового connector/release остаётся отдельной проверкой.
Manifest, регистрация, одобренный профиль и установленный пакет сами по себе не
означают работающее подключение.

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

Source подключается по желанию для общего входа, Native ACL, контекста и действий
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

Текущий BFF slice — **Basic300**, original Root slot и явный Native consent.
Импорт RP49 не включает long session автоматически. Долговечный long consumer,
Root rebind/ACK recovery и перезапуск — отдельные acceptance gates; текущий
`session-continue` честно отвечает `renewable:false`. Не передавайте Basic на
новый slot и не выдавайте manifest за подтверждение 24 часов.

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
