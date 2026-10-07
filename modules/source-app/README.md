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

## Что приложение предоставляет

- `createSourceNativeAuthorityPort`: получить настоящий Native proof из своей
  сессии/сохранённой связи и проверить текущие права на точный выбранный ресурс.
  Root account, email, имя, HTTP header или JSON формы не заменяют эту проверку.
- `SourceStoragePort`: долговечные encrypted interaction/session/one-use completion
  receipts, атомарные CAS и nonce store. PKCE, AT/RT и cookie никогда не попадают
  в Root/browser DTO или журналы.
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

После входа в mutating action ошибка, отзыв доступа или потеря ACK означают
`source_app_effect_unknown`, а не rollback. SDK не повторяет apply; Source
`readProof` возвращает квитанцию при текущих Native правах. Callback вне операции,
повторный callback, thenable и вложенный commit запрещены. Распределённой атомарной
транзакции Root–issuer–Source нет: начатый запрос может успеть записаться до отзыва.

## Запуск и границы проверки

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
выбранный незавершённый вход: новый вход заменяет старый, старый form/callback
получает `source_app_intent_superseded`409 и требует явного повторного входа.
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
Эти gates добавляются обычным SQLite example и двумя независимыми Source realms.

## Исполнимый обычный Source

`@soty/source-app/example` содержит настоящее новое SQLite приложение и небольшой
интерфейс без React. Native роли, две независимые identity proof, selected consent,
item/feedback receipts и шифрование закрытого BFF состояния проверяются его
Source, а не полем owner в Сотах. Две разные realm работают с одним adapter;
повторный Source не требует Root code/DDL diff. Native support — только Native
владелец; reporter принимает решение о закрытии обращения.

Проверены real OIDC/HTTP +durable Source restart/unknown ACK и два OS writer
processes на synthetic базах. MAC IPC в этом gate контролируемый. Пока есть
blocker: example callback Native `/soty/callback` не совпадает с обязательным
embed HTTPS callback release policy. Установленный канал, browser UX и production
не приняты; наличие bundle не делает приложение Ready. Callback/Native proof
correlation будет согласована отдельной поправкой без ослабления Root policy.
Точные проверки и ограничения — `docs/implementation/source-app-ordinary-acceptance.md`
в Root checkout. ASR/OCR/agent queue и managed reviews здесь не внедрены.
