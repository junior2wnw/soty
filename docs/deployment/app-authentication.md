# Вход в приложение через Соты

**Статус исходников:** встроенный relay-вход выбирает личность по конкретному
окну, а не общему cookie. Он требует вместе обновлённых Root, оболочку Сот,
коннектор и browser SDK. Прежние SDK preview.10 и preview.11 не передают привязку
окна и безопасно отклоняются обновлённым Root; прежний прототип с общим cookie
мог подменять аккаунт между окнами. Код и локальные проверки не означают выпуск, развёртывание
или автоматическое обновление старых приложений. Workspace2 и обычные запросы
без relay-identity сохраняют свой контракт.

Обычный вход совместимого мини-приложения использует тот же аккаунт Сот, в
который вошёл человек. Приложение проверяет личность общим SDK и хранит данные
разных людей отдельно. Дополнительный отдельный логин не нужен. Проверьте, что
установленные Root, коннектор и SDK действительно поддерживают выбранный путь:
скачивание SDK и локальная проверка сами по себе не включают production.

Для поддерживающего общий вход relay доступны `createRelayIdentityFetch` в браузере
и `createSqliteRelayIdentity` на сервере из portable Source SDK для одного хоста.
Для нескольких хостов используйте `createRelayIdentityVerifier` с общим
устойчивым атомарным store; браузерная часть при этом должна поддерживать оконный контракт. Root передаёт
короткое подписанное подтверждение через совместимый коннектор только по запросу
приложения; сервер проверяет issuer, appId, метод, путь, срок и одноразовость.
Используйте проверенные `issuer/subject` с собственной ACL и устойчивым хранилищем.
Для уже допущенного workspace2 сохраняйте его Source SDK и Native ACL. Старые
аккаунты не объединяются автоматически по имени или почте. Собственный
confidential Human OIDC/BFF — отдельный осознанно выбранный путь.

SDK получает список доверенных origin оболочки от Root приложения и проверяет
точный origin и источник сообщения. Конфигурацию извне нельзя подменять адресом
произвольного родителя или `*`. При штатной замене старое окно сохраняется до
подтверждения отзыва разрешения. При закрытии экрана или смене аккаунта запрос
на отзыв не удерживает переход и может не дойти; разрешение живёт максимум 30
секунд, при этом Root проверяет права и устройство перед каждым запросом. Уже
отправленная операция может завершиться после отзыва.

После перезапуска Root кэшированное разрешение перестаёт действовать. При
специальном отказе Root до создания потока SDK сбрасывает его и однократно
повторяет только GET/HEAD. Изменяющий запрос не повторяется автоматически.
Этот признак устанавливает только Root до отправки: ответ приложения и
коннектора не может его передать через фильтр заголовков. На других ошибках
SDK не делает предположения об исходе и не повторяет запрос.

Настольные Chromium, Firefox и WebKit проверены локально на синтетических
пользователях и данных. Для WebKit использованы HTTPS-адреса оболочки и
поддомена приложения в одном site; это не проверка Safari на iPhone/iPad или
production. На разных sites обычный ingress зависит от политики сторонних
cookie браузера: локальный WebKit отказал до SDK. Для такого размещения нужна
отдельная проверка и исправление ingress, ослаблять cookie ради прохождения
теста нельзя. Отдельная вкладка приложения пока не имеет общего relay-входа;
для неё нужен отдельный подтверждённый контракт, а не выбор общего cookie.
Код одного app-origin считается доверенным: защита от вредоносного соседнего
окна того же origin требует отдельной изоляции.

Поле `admission` в индексе SDK относится к выбранному Source/workspace2.
Для поддерживаемого relay общего входа отдельный статический Source-профиль на
каждое приложение не нужен; обычная регистрация и проверка доступа остаются.

Если изменяющий запрос уже отправлен, отказ 403 или обрыв не доказывает, что
приложение не записало данные. Сохраняйте исходный идентификатор операции;
при действующих правах проверяйте его квитанцию или фактический результат,
не повторяя запись вслепую. Для обычного relay приложение само должно атомарно
связать идентификатор, эффект и квитанцию; SDK входа не делает это автоматически.
Перед использованием HTTP `Idempotency-Key` проверьте поддержку всей цепочки.

## Выбор режима

Для уже проверенного Root профиля в приложении с явно установленным
`soty.standard-workspace@2` используется поддерживаемый signed-current-context
через private frame/broker, затем собственный Native ACL. Этот режим не требует
RP registration или OIDC refresh family для самой соты. Активный issuer может
иметь явный `clients: []`: ключи/issuer/origin остаются обязательными, JWKS и
подпись Source authority работают, а незарегистрированные RP получают отказ.
Native/standalone адрес сам по себе не выбирает протокол и не создаёт RP client.
Отдельно выбранный confidential BFF/RP требует approved client. Пустой список
ничего не регистрирует и не включает dynamic registration. Source-only профиль
не обещает отдельный Native сайт. Установка и условия Source SDK описаны в
[действующем skill](../../skills/soty-app-deploy/references/authentication.md) и
[maintained installer](../../modules/source-app/install/README.md); новый протокол
или автоматический admission из этой инструкции не выводятся.

| Задача | Контракт | Что он подтверждает |
|---|---|---|
| Встроенный обычный relay + обновлённый общий вход | Конкретное окно + подписанный Root proof | Действующую личность для HTTP-запроса; приложение отдельно проверяет свои права |
| Действующий approved `soty.standard-workspace@2` и совместимый коннектор | Source SDK / signed-current-context | Проверенный текущий контекст точного Source/ресурса, затем собственный Native ACL; без отдельного RP client |
| Явно выбранный собственный confidential BFF/RP | Human OIDC `/human-identity` + Source RP/BFF | Текущий human subject, отдельная app session, затем собственный ACL |
| Закрытый старый проект одного владельца | Private owner adapter за restricted ingress | Только владельца этой закрытой установки |
| Работа агента с данными/операциями | Root OAuth `/oauth` + MCP + app grants | Разрешённого agent principal и конкретные операции |
| Открыть приложение через коннектор | `/_soty/boot` → ingress cookie → `/_soty/ingress-check` | Допуск к текущему app/source, без human claims |

`/_soty/session` с `{ok:true}` не является `/me`. App ID, владелец карточки,
заголовок от клиента и номер локального пользователя не заменяют `(iss,sub)`.
Приватный адаптер Spreadex из восстановления 20.10.2026 относится к строке
`Private owner adapter`. Он не является универсальной многопользовательской авторизацией.

## Управление собственными входами

В текущем интерфейсе: **Профиль → Безопасность → Входы в приложения**. Чтение
capability — `GET /api/apps/capabilities → humanSignins`; подписанные методы —
`identity.human.signins.list`, `.revoke`, `.checkRevoke`. Server-issued intent
передаётся точно, с expected account и revision. После lost ACK используется
readonly check, без нового revoke ID или автоматической отправки отзыва.
Family-only и forget app/device/version имеют разные scopes; Root Source
короткие сессии и agent grants в этот список не входят. Подробности и ограничения:
[редактор входов S29/S30](../implementation/human-signins-settings.md).

## Собственный BFF/RP: подготовить issuer и approved client

Сначала прочитайте [Human identity](../../modules/human-identity/README.md).
Issuer — точный `https://<проверенный shell origin>/human-identity`. Discovery:
`<issuer>/.well-known/openid-configuration`. Проверяйте exact issuer и HTTPS,
не стройте issuer из непроверенного Host или поля формы.

Приватный entrypoint конфигурации — `server/universal-configuration.js`:
`SOTY_HUMAN_IDENTITY_ENABLED=1`, `SOTY_HUMAN_IDENTITY_ISSUER` и абсолютный путь
`SOTY_HUMAN_IDENTITY_KEYS_FILE`. Файл содержит approved `clients`, приватный
`jwks`, `cookieKeys`, 32-byte base64url `artifactKey` и `artifactKeyId`.
Секреты передаются закрытым каналом и остаются на сервере. В документацию,
план, browser bundle и MCP они не попадают. Резервирование файла — зашифрованное.

Каждое приложение получает отдельный confidential client и точный HTTPS
`redirectUri`; wildcard и dynamic registration не включены. Изменение redirect
требует новой версии клиента. Наличие модуля или карточки не доказывает,
что production issuer включён и клиент одобрен. Выключенный namespace отвечает
503; это конкретная зависимость размещения, а не повод имитировать вход.

## 2. Подключить сервер приложения

Используйте [Source RP](../../modules/source-rp/README.md) и его
[серверный API](../../modules/source-rp/server/index.d.mts), который уже
переиспользует maintained `openid-client`. Не создавайте собственный JWT/OIDC
проверяющий код. Минимальная последовательность API:

```js
import { createSourceRpProtocol } from './modules/source-rp/server/index.mjs';
// approvedProfile приходит из закрытой конфигурации сервера приложения.
const oidc = createSourceRpProtocol(approvedProfile);
await oidc.ready();
const intent = await oidc.start();
// Сохранить verifier/state/nonce server-side, привязать к этому браузеру,
// установить конечный срок и выполнить redirect на intent.location.
// На callback получить только соответствующий одноразовый intent:
const identity = await oidc.exchange(callbackUrl, intent);
// identity.issuer/subject → собственная стабильная связь аккаунта приложения.
```

Это последовательность протокола, не готовый HTTP handler. BFF приложения
обязан ограничить pending intents, связать их с браузером, исключить повторное
использование callback, выставить собственный host-only Secure/HttpOnly cookie
и проверять CSRF/Origin для изменений. Access/refresh token, client secret,
verifier и ключи не выдаются JavaScript, localStorage, URL или модели.

`modules/human-identity/examples/bff.mjs` — **изолированный протокольный fixture**
с тестовыми in-memory sessions и loopback cookies. Он полезен для понимания и
двухпользовательской проверки; его нельзя публиковать как production BFF.
Долговечное хранение, миграции, encryption и native ACL реализуются в Source.

Для существующего Python/FastAPI можно сохранить Native сервер и использовать
maintained OIDC SDK, например Authlib, с теми же protocol/изоляционными gates.
Это не перенос JavaScript fixture в production и не самостоятельный JWT verifier.
Требуйте точное совпадение исходного nonce и при optional `nonce_supported=false`;
подпись/alg/issuer/audience/expiry проверяет SDK. Browser-local preferences/layout
тоже разделяются по аккаунтам; смена app session/выход обновляют другие вкладки.
Callback и персональные ответы не кешируются; authorization code не должен
попадать в access log reverse proxy или приложения.

## 3. Провести вход и проверить текущие права

1. BFF начинает Authorization Code + S256 PKCE, state и nonce.
2. Соты предлагают текущему человеку явное подтверждение через подписанное
   Connect-устройство и consent конкретного клиента.
3. Серверный callback проверяет state, nonce, issuer, подпись, audience, expiry
   и свежий userinfo. Только затем создаётся собственная сессия приложения.
4. Для закрытого действия проверяются текущий subject и native ACL приложения.
   В Basic используется `currentSubject(accessToken, expectedSubject)`; при
   одобренном renewal — `createSourceRpSessionService(...).currentProof(marker)`
   с последующим `assertCurrent()` вокруг асинхронных действий по контракту.
5. Связь `(issuer, subject) → local account` неизменна. Привязка старого аккаунта
   требует доказательства владения обоими аккаунтами. Имя/email не являются
   таким доказательством. Нельзя назначить всем входящим локального user 1.

Базовый access token живёт 300 секунд; проверка одного ID token offline не даёт
мгновенного отзыва. После истечения нужен новый вход, если отдельно не одобрен
finite renewal. Optional finite24h требует reviewed Root/client profile,
явного выбора человека и encrypted durable Source storage/CAS. Конечный срок
не продлевается каждым refresh; unknown RT не переотправляется вслепую.

Схема БД создаётся/мигрирует **до** запуска optional Telegram/agent/worker.
Отключение необязательного канала не должно ломать первый человеческий вход.

Для прежней строго private-owner установки допустима отдельная операторская
миграция до первого human login: закрепить исходный restricted app/source,
принадлежность единственной local row, точного Root app owner и encrypted DB
backup/restore receipt. Только новый verified subject этого владельца получает
прежнюю row. Остальные получают независимые accounts. Эта provenance — старая
сторона доказательства владения, не first-login-wins и не массовое сопоставление
по display name/email. Если прежних пользователей несколько, применяется
обычный двусторонний account linking.

Перед массовой публикацией отдельно проверьте действующие host budgets:
finite families/account, retained rotations, artifact/SQLite bytes и userinfo
concurrency. Встроенные conservative defaults не равны ёмкости Source users
или Basic login. Рост одного численного лимита без нагрузки, retention и
headroom не доказывает готовность к массовому релизу. Новый approved client
не должен менять issuer keys/profile digests/generation прежних клиентов.

## 4. Разместить приложение

Собственные HttpOnly sessions/OIDC callbacks часто требуют native ingress;
обычный isolated gateway фильтрует cookies/authorization/origin и запрещает
часть вложенных frames. Выбор транспорта не меняет права человека.
Проверьте [процесс размещения](app-release.md): фиксированный loopback upstream,
current access/source pin на каждый запрос, точные callback/Origin/ancestors,
сохранённые собственные cookies и CSP приложения. Не ослабляйте общий sandbox.

## 5. Приёмка входа

| Сценарий | Наблюдаемый результат |
|---|---|
| Один человек, два приложения | Один `(iss,sub)`, разные app sessions и собственные ACL |
| Два человека | Разные subjects, чужие записи/роли недоступны |
| Отказ consent, неверные state/nonce/aud | Вход отвергнут, app session не создана |
| Отозванное устройство/grant или выключенный client | Защищённое действие отвергнуто по свежей проверке |
| Истечение Basic/finite срока | Явный новый вход; нет бесконечного silent refresh |
| Запись → полный reload Сот → прежний вход | Та же запись и IDs, прежние ACL |
| Source restart / смена именного origin | Стабильная account association; не новый пустой аккаунт |

Настоящие локальные protocol/reference gates:

```text
node --test --test-concurrency=1 modules/human-identity/test/http.test.mjs modules/human-identity/test/https-cookie.test.mjs
node --test --test-concurrency=1 modules/source-rp/test/protocol-http.test.mjs modules/source-rp/test/session-service.test.mjs modules/source-rp/test/multiprocess.test.mjs
```

Эти тесты не включают production issuer и не заменяют браузерную приёмку
конкретного приложения после реального истечения времени.

### Возврат без лишнего подтверждения

Уже подтверждённый browser-bound interaction после reload завершается через
свежую подпись того же решения выбранным Connect-профилем. Это не новое
согласие: другой аккаунт, срок или набор прав сервер отвергает. При включённом
и ранее выбранном device consent используется отдельный signed continuation;
пока он проверяется, показывается открытие приложения, а не повторная форма.
Если capability недоступна, обещание «запомнить устройство» не показывается.
Первое разрешение, новый client/version/scope и отзыв продолжают требовать
реального решения человека.

Успешный BFF callback должен восстановить исходный внутренний маршрут,
проверенный и сохранённый вместе с одноразовым intent. Для popup необходим
exact-origin/source handshake с исходным окном, а для same-window — прямой
переход. Страница «вход подтверждён» со второй обязательной кнопкой не является
завершённым сценарием. При blocked popup/нет opener/blocked third-party cookie
нужен рабочий top-level fallback, без ослабления sandbox или выдачи токена JS.

Переключение Root-профиля требует отдельной проверки соответствия app session
свежему Root actor. Cookie приложения переживает удаление iframe. При наличии
trusted selected Source контекста сравниваются проверенные issuer/sub; публичный
selector, accountId из query или обычный ingress `{ok:true}` этого не заменяют.

Дополнительный capacity gate:
`node --test modules/human-identity/test/renewal-capacity.test.mjs` проверяет
16 активных families у двух людей, отказ17-й без вытеснения существующих,
а затем reviewed24/12 после restart. Это локальная протокольная граница,
не нагрузочная сертификация и не изменение production defaults16/8.

## Помощь человеку и агенту

`node scripts/soty-app-release.mjs auth-guide --output NEW_FILE` выдаёт общий
путь входа Сот, как read-only MCP `apps_auth_guide({})`. Альтернативные режимы —
`human-oidc`, `private-owner` и `agent`. Команда не меняет конфигурацию,
не выдаёт токены/grants и не сообщает, что живой issuer уже включён.
