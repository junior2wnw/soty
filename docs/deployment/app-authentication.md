# Вход в приложение через Соты

Для общего входа людей используйте **Human OIDC + серверный BFF**. Человек
определяется проверенной парой `(iss, sub)`. Локальный аккаунт, роли и данные
остаются у приложения. Доступ к карточке Сот сам по себе человека внутри
приложения не идентифицирует.

## Выбор режима

| Задача | Контракт | Что он подтверждает |
|---|---|---|
| Вход разных людей в приложение | Human OIDC `/human-identity` + Source RP/BFF | Текущий human subject, отдельная app session, затем собственный ACL |
| Закрытый старый проект одного владельца | Private owner adapter за restricted ingress | Только владельца этой закрытой установки |
| Работа агента с данными/операциями | Root OAuth `/oauth` + MCP + app grants | Разрешённого agent principal и конкретные операции |
| Открыть приложение через коннектор | `/_soty/boot` → ingress cookie → `/_soty/ingress-check` | Допуск к текущему app/source, без human claims |

`/_soty/session` с `{ok:true}` не является `/me`. App ID, владелец карточки,
заголовок от клиента и номер локального пользователя не заменяют `(iss,sub)`.
Приватный адаптер Spreadex из восстановления 20.10.2026 относится ко второй
строке. Он не является универсальной многопользовательской авторизацией.

## 1. Подготовить issuer и approved client

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

## Помощь человеку и агенту

`node scripts/soty-app-release.mjs auth-guide --mode human-oidc --output NEW_FILE`
выдаёт тот же ограниченный guide, что read-only MCP `apps_auth_guide`.
Другие режимы — `private-owner` и `agent`. Команда не меняет конфигурацию,
не выдаёт токены/grants и не сообщает, что живой issuer уже включён.
