# Общий вход людей U2

Этот модуль создаёт отдельный OIDC issuer `https://<Соты>/human-identity` на установленном `oidc-provider@9.12.2`. Два независимых BFF в тестах проходят настоящий Authorization Code + S256 PKCE: получают один `iss/sub`, держат собственные сессии и строки приложения, проверяют `state`, `nonce`, подпись и `aud`. Подтверждение входа использует настоящий подписанный запрос текущего Connect-устройства. Название или email не соединяют старые аккаунты автоматически.

Это реализация для проверяемого включения через явную конфигурацию хоста. Сам факт наличия кода не означает включённый production issuer, перенесённые аккаунты HIVE или регистрацию OAuth-клиентов сторонними авторами. Установленный Pocket и отдельный capability OAuth `/oauth` сохраняют свои протоколы.

## Включение и private ports

`createHumanIdentityHostProfile(options, { shellOrigins })` принимает проверенную конфигурацию хоста:

```text
{
  enabled, issuer, registryId, environmentId,
  clients: [{ id, label, redirectUri, clientSecret, version?: 1 }],
  jwks: { keys: [private RSA RS256 JWK] },
  cookieKeys: [secret],
  artifactKey: Uint8Array(32), artifactKeyId
}
```

Issuer обязан совпадать с одним проверенным shell origin и точным путём `/human-identity`. Redirect каждого клиента фиксируется полностью; wildcard, query, fragment, пользователь в URL и сетевое получение конфигурации не поддерживаются. HTTP разрешён только для loopback fixtures. До включения нужны собственные ключи подписи, cookie и шифрования, переданные отдельным закрытым конфигурационным каналом. Модуль не генерирует production ключи, не пишет их в DB и не показывает в DTO. `undefined` или `enabled:false` оставляет namespace с безопасным 503 без создания identity DB.

`createHumanIdentityService({ databasePath, profile, actorActive, withAuthorityFence, readProfile?, maxDatabaseBytes?, now? })` создаёт private store. Connect вызывает `execute({ op:'identity.human.approve', actor, args })` внутри уже открытой проверки подписанного устройства. `actor` поступает из Connect, а не из payload или HTTP Host. Поздние выдача/использование SDK-доказательств повторно проверяют устройство внутри `withAuthorityFence`. Callback порты синхронны, финальная проверка активности предшествует COMMIT. `readProfile` по умолчанию возвращает `{}`; World и приватные поля аккаунта автоматически не читаются. Допустимы только отдельно разрешённые `name` и `preferred_username`.

`attachHumanIdentity(app, { profile, service, distDir })` подключает HTTP adapter. Interaction document отдаёт `distDir/index.html`; inline scripts не нужны. Root UI связывает captured текущий аккаунт, стабильный request ID и native CSRF POST. Первый вход существующим Connect-профилем требует явного подтверждения. Создание нового профиля — отдельное действие интерфейса; этот модуль не открывает регистрацию Pocket. `soty-connect-proof` обозначает владение программным ключом устройства, без обещания аппаратной/passkey аутентификации.

## HTTP и подтверждение

- `GET /human-identity/.well-known/openid-configuration`, `/jwks`, `/userinfo`.
- `GET /human-identity/authorize`: code, `openid` или `openid profile`, S256, state/nonce, точный approved client/redirect.
- `POST /human-identity/token`, `/revoke`: стандартный confidential BFF `client_secret_basic`. Refresh tokens, dynamic client registration и offline access отключены.
- `GET /human-identity/interaction/:uid[/context]`: SDK cookie + отдельная host-only HttpOnly/Lax browser cookie, закрытый DTO `{schema,interactionId,browserNonce,csrf,client:{id,label},scopes,expiresAt,decision}`. Ни account/device, ни state/nonce/challenge/token не возвращаются в context.
- Подписанный `identity.human.approve`: `{expectedAccountId,interactionId,browserNonce,csrf,requestId,decision:'approve'|'deny'}`. Ответ `{schema:'soty.human-login-decision.v1',interactionId,requestId,decision:'approved'|'denied'}`. Неизменённый request ID повторяет прежнюю квитанцию; изменённый intent даёт conflict. Новый request ID для уже подтверждённого UID допустим только тому же свежему account/device и с тем же решением.
- `POST /human-identity/interaction/:uid/complete`: same-origin form с одним `csrf`, текущее подписанное решение и private SDK completion. Утерянный ACK использует прежний grant; один interaction не создаёт второй. SDK account-switch form сохраняет свой script hash, `Referrer-Policy:same-origin` и допускает в form-action только self и проверенный RP callback.

## Изоляция клиентов и срок жизни

Базовый protocol digest не содержит список клиентов. Собственный client pin включает id, version, точный redirect и базовый протокол. Добавление Gamma не отзывает доказательства Alpha/Beta. Изменение redirect требует новой версии, та же версия с другим содержимым отвергается. Удаление/повторное добавление клиента увеличивает его durable generation: старые grants не оживают. Cookie session может охватывать разные клиенты; токены и grants всегда проверяют собственный client pin/generation. Секрет и display label не расширяют разрешённый redirect и не меняют semantic pin.

SDK TTL: interaction 300s, session/grant 600s, code 60s, access token 300s, ID token 60s. Access token и userinfo проверяют текущую активность Connect-устройства. Уже выданный самостоятельно проверяемый ID token живёт до своего `exp`; мгновенный отзыв нельзя обещать RP, который проверяет его только offline. BFF fixture проверяет свежий userinfo на каждом `/me` и перед привязкой старого аккаунта.

Для служебной очереди установлены 8 живых pending interactions на браузер и 64 на client, 16 новых decision request ID на interaction; точный replay проходит при заполнении. Общие caps: 4096 intents/bindings и 8192 artifacts/decisions. Истёкшие pending intents и encrypted SDK artifacts освобождаются пакетами до 128 строк на таблицу. Approved/denied ACK и binding удаляются лишь спустя 3660s после истечения intent и при отсутствии живого зависимого SDK proof, включая session. Это конечное окно idempotency login-протокола, а не бизнес-квитанция навсегда. Внутри живого 300s окна подтверждение не повторяет эффект. После expiry подписанный API возвращает 410, и старый UID не создаёт новый intent: для него нужен живой исходный SDK Interaction. Immutable client versions/generations не очищаются.

GC срабатывает перед новыми allocation; отдельный host-only `compactExpiredRuntime()` позволяет периодическое обслуживание. Часы `now` — trusted port для тестов, не входящий параметр запроса. Капы текущего одиночного issuer требуют измеренного повышения/разбиения для массового потока; это не неограниченная очередь. История клиентских pins ограничена 4096 строками, её нельзя молча стирать ради нового автора.

## Хранение и откат

`data/human-identity/identity.sqlite` v1 хранит SDK identifiers только как hashes, SDK payload/login parameters — AES-256-GCM с model/id/issuer/keyId AAD. Nonce/state/CSRF/code/token plaintext не находятся в DB. Короткая квитанция решения содержит opaque interaction/request/account IDs; это приватные персональные метаданные, не публичный лог.

Лимит DB по умолчанию 64MiB, trusted host может уменьшить до 64KiB; SQLite FULL возвращает безопасный 503. Уже существующая overquota DB остаётся читаемой и не удаляется. Ключ шифрования один: прозрачной ротации/ring в этой версии нет. Backup/restore требует тот же `artifactKey` и `artifactKeyId` отдельным зашифрованным каналом. Подмена или недоступность ключа — отказ, а не генерация нового ключа и скрытый сброс сессий.

Reader manifest/format v5 включает семь stores и сохраняет чтение v3/v4. Старый v3/v4 image не запускается после появления identity store. Независимый probe проверяет frozen exact normalized DDL, indexes, conditional GC guards и metadata, не читая encrypted artifacts или квитанции. Snapshot сохраняет DB/WAL всех новых stores, ограничен 192MiB и не копирует ключ конфигурации. Candidate и fallback должны иметь v5 reader; отключённый fallback сохраняет stores без входов/записей U2. Для Linux release также нужны build, browser flow и release storage gates.

## Проверка

Из корня checkout на Node >=24.15 (проверено 24.19):

```powershell
node --test modules/human-identity/test/*.test.mjs
node --test deploy/connector/storage-human-identity-v1.test.mjs
```

Fixtures работают только на временных loopback-серверах и временных SQLite с сгенерированными в памяти ключами. Проверяют реальное signed Connect, restart, два BFF, PKCE, claims/privacy, cookie/CSRF/mixed actor, revoke, per-client change, quota, saturation/expiry и live proof retention. `examples/bff.mjs` — проверяемый пример, не production BFF SDK: его local session store находится в памяти. Двусторонняя привязка старой synthetic строки требует одновременно прежнюю локальную сессию и новый issuer proof + CSRF, сохраняет прежние данные. Конкретный HIVE adapter, production key custody, внешняя регистрация клиентов и завершённый operational rollout остаются отдельными интеграциями.
