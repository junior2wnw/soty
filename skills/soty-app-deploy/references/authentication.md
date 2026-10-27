# Аутентификация приложения

**Обычный relay-вход временно отключён.** Старый вариант мог сменить личность в
уже открытом окне при входе другого аккаунта. Новый Root отказывает до подписи и
доставки; старый Root не является безопасным обходом. Примеры SDK в этом разделе
описывают прототип до исправления, а не готовый путь.

Сначала установи действующий app/client/Source profile, issuer, capabilities и
роли. Не меняй рабочий вход ради ещё не квалифицированного способа. При наличии
checkout дополнительные operator/protocol gates находятся в
`docs/deployment/app-authentication.md`; публичная инструкция не выдаёт доступ к
его приватной конфигурации. Наличие кода не означает включённый production issuer.

## Выбрать существующий контракт

| Контур | Подтверждает | Не заменяет |
|---|---|---|
| Gateway / Native ingress | Можно открыть конкретный app/source | Human identity и права на данные внутри приложения |
| Обычный relay + общий вход | Временно отключён до привязки к окну | Native ACL и устойчивое раздельное хранение приложения |
| Human OIDC `/human-identity` + серверный Source RP/BFF | Проверенную identity и собственную сессию приложения | Native tenant/role ACL и согласие на продление |
| SourceSSO по approved Source profile | Поддерживаемый адаптер получает проверенный Root-контекст точного приложения, Source, ресурса и экземпляра | Доступ к данным, отзыв и независимый standalone вход |
| Agent OAuth / MCP | Права actor на ресурс, операции, срок и бюджет | Human identity, другие MCP и Native/project grants |

SourceSSO применим только при действующем approved descriptor/profile, адаптере и
квалифицированном закрытом/server-to-server транспорте. Не выводи его из названия
SSO, пути `/embed`, ingress cookie или заголовка `user_id`. Если адаптера нет,
используй доступный квалифицированный вход либо сохрани точный статус зависимости.
Новый Native profile из спайка не объявляй действующим контрактом.

## Общий вход для совместимого relay

После создания и проверки безопасной привязки к окну для этого прототипа
предусмотрены `createRelayIdentityFetch` из browser SDK и
`createSqliteRelayIdentity` из server SDK для одного хоста. Проверь наличие этой
функции в установленной версии; прежний SDK использует
`createRelayIdentityVerifier` вместе с SQLite nonce store. Проверенный issuer задаёт хост,
appId берётся из регистрации Сот; пользовательский JSON или произвольный
заголовок не являются личностью. На сервере проверяй ACL каждого действия и
отделяй данные по проверенным issuer/subject. Для нескольких процессов или
перезапусков нужна устойчивая атомарная защита от повтора. Не обещай поддержку
старого коннектора, WebSocket identity или существующей базы без проверки.
Поле `admission` в SDK index описывает Source/workspace2; для поддерживаемого
relay отдельный статический Source-профиль на каждую карточку не требуется.

После отправки изменения обрыв или 403 не доказывает, что запись не состоялась.
Сохрани исходный requestId и при текущих правах проверь квитанцию либо реальные
данные, не повторяя запись вслепую. Обычный relay SDK не создаёт бизнес-квитанцию:
приложение должно атомарно связать свой requestId и эффект. Поддержку заголовка
`Idempotency-Key` проверяй на всём пути, включая установленный коннектор.

## Общий Source SDK для нового личного приложения

При наличии действующего approved workspace2 profile и совместимого коннектора
используй существующий `modules/source-app`, portable `@soty/source-app` и его
CLI `soty-source`. Не копируй в каждое приложение свои подписи, Source cookies,
nonce, криптографию или механизм COMMIT/recovery. Preview-пакет и локальная
квалификация не означают, что этот режим уже включён на production Root.
После проверенного выпуска открой [SDK index и SHA256](/sdk/source-app/index.json)
на доверенном Root. Проверь sourceCommit/version, размер и SHA256
архива `/sdk/source-app/<SHA256>.tgz` до исполнения кода. Установка в собственный
проект:

`.soty-sdk` должна быть новой папкой без symlinks. При существующей папке
остановись и выбери другую (желательно с SHA256 в имени), заменив путь во всех
командах; не распаковывай поверх прежней версии. Публичный архив имеет finite
inventory без symlinks, проверенный producer и UI release.

```text
mkdir .soty-sdk
tar -xzf ./source-app-<SHA256>.tgz -C .soty-sdk --strip-components=1
npm ci --prefix .soty-sdk --ignore-scripts --omit=dev
node .soty-sdk/dist/install.mjs --help
```

Runtime package-lock и побайтно равный shrinkwrap закрепляют
openid-client6.8.4 и два точных leaf dependency; dev/install scripts и приватных
config/ключей нет. Пакет содержит server/browser/CLI/types/docs/template и optional
Python binding. Root checkout/SSH автору не нужен. Отсутствующий/неверный index
оставляет публичную дистрибуцию неподтверждённой: не выдумывай npm registry
публикацию. SDK preview и скачивание не означают production Ready или admission.
Root/connector держат до64 exact approved profiles при startup. Новый app/profile
требует trusted operator install/reinit, а не Root rebuild; уже preloaded
candidate проходит prepare/promote без restart. Это не self-service admission
произвольных приложений и не wildcard Source.

После установки полная инструкция находится в
`./.soty-sdk/install/README.md`, приватный workspace config
создают по `./.soty-sdk/install/operator.workspace.template.json`.
Рабочий пример data callbacks —
`./.soty-sdk/dist/templates/reading-list/domain.mjs`;
`author --template reading-list` копирует его и готовые UI assets в ваш проект.
Сборка SDK и `dist` из checkout автору не нужны: используются файлы из проверенного
архива. Установка tgz как dependency чужого проекта не считается frozen путём:
вложенный shrinkwrap не удержал версии в проверенном npm11.8. Команды ниже
выполняются из своей папки с распакованным и установленным `.soty-sdk`.

Короткий путь для **нового** SQLite приложения:

```text
node .soty-sdk/dist/install.mjs author --title "Мой проект" --template reading-list
node .soty-sdk/dist/install.mjs check --config PRIVATE_FILE
node .soty-sdk/dist/install.mjs init --config PRIVATE_FILE
node .soty-sdk/dist/install.mjs serve --config PRIVATE_FILE
```

Оператор один раз задаёт утверждённые appId/profile/target/issuer/connector pins,
private config и два server-only ключа. Режим задаётся явно:
`soty.root-workspace.operator.v1`; старый Ordinary/OIDC не становится SourceSSO.
Для Source-only workspace допустимы одинаковые `nativeOrigin` и `embedOrigin`:
это один Source вход с `nativeReachability:source-only`, без обещания отдельного
Native сайта. Фактический Native/OIDC/парный вход требует различных hostname,
своей Native identity/ACL и проверки app-policy. Entry Source SDK — `/embed`.
Optional `createSqliteRootWorkspaceHost` принимает только data callbacks,
проверяет Native SQL и создаёт новый личный ресурс по verified issuer/sub лишь
при явной `personalWorkspaces:true` policy. Root app owner не получает чужие
Native данные. Existing Native accounts/SQL требуют собственного адаптера к
тем же Native/storage ports; не заводи теневую базу поверх действующей.

Общий browser `createSourceWorkspaceClient().connect({requestId})` делает
continuation и current-context read без нового пользовательского жеста при
действующем разрешении. Затем нужен успешный domain query. UI assets —
`/assets/ui.js`, `/assets/ui.css`, `/assets/source-sdk.js`; private domain/config
не выдаются. Legacy assets сохраняют только по фактическому finite allowlist
своего приложения. Не добавляй произвольный proxy path ради пустого iframe.

Перед новым write SDK автоматически обновляет короткую session/context и
проверяет непрерывность actor/device/Native workspace, selection/generation/role.
Business read допускает один повтор после такого перехода; низкий frame-fetch
отдельно допускает один повтор safe RPC после bounded wake-up точного родителя.
Это разные уровни, поэтому HTTP-попыток может быть больше двух. Смена или потеря
continuity закрывает контекст до явного подключения. Sent/unknown intent не
перепривязывается; write не повторяется автоматически.

При unknown сохранении сохраняй точные requestId/inputDigest/contextRef в
browser namespace текущего уже авторизованного личного ресурса. После reload
или нового frame вызывай только `recoverReceipt(reference)` под новым current
contextRef. Original receipt остаётся неизменяемым, fresh authority envelope
проверяется отдельно. Нельзя повторять invoke, менять старый requestId или
считать отсутствие строки у другого профиля доказательством «не записалось».

`soty-source reader` проверяет локальную схему/ключ, а `doctor --config ...`
может проверить purpose-sealed **историческое** наблюдение действительного SDK
вызова: context/domain read/commit/receipt различимы. Файл не хранит subject,
аргументы, результат, cookies или хеш частных данных. Его запись не меняет
business outcome. Даже verified observation оставляет
`currentAuthority:not_checked`, `clientAcknowledgement:not_observed` и
`releaseReady:false`. Нужны фактические write → read → reload → revoke.

Обычная самостоятельная Native страница и Root Source frame — разные
поверхности. Root-context proof не является сессией Native вне Сот.
Самостоятельный вход использует поддерживаемый Human OIDC/BFF либо собственную
Native сессию/ACL. Парный публичный Native entry дополнительно требует current
pair/точных origins/пинов, отдельного whole-port ACK и текущего app-policy guard
на HTTP и в течение WS. Эти проверки не дают Human identity. При изменении
активной пары используй свежий exact keep/remove contract; старый Source-only
вызов не должен молча снимать Native entry. Проверяй наличие этой capability.
В owner editor «Сохранить отдельный сайт» проверяет оба targets и требует
независимые Source и Native ACK. Для первого Source входа новый адрес сначала
закрепляют на том же app ID, но оставляют выключенным. Не включай его обычным
сохранением доступа, пока primary ещё указывает на прежний обычный порт.

Атомарное включение доступно при `apps.inspect.source.sourceEntryActivation:true`.
Владелец выбирает OWN bound/inactive alias в «Адрес входа Сот». Единственный
кандидат может быть предложен заранее; это ещё не проверка подходящего origin.
Prepare и promote получают один и тот же closed action:
`sourceEntryAction:{action:'activate',domainId:'dom_<32hex>'}` — без URL в action.
Сервер сверяет имя с approved `profile.embedOrigin`; подготовленный ответ содержит
точные `sourceEntryAction` и `sourceEntry:{origin,domainId}`. До подтверждения
переключения адрес остаётся закрытым. Private MAC readiness CHECK проверяет
подключение; полноценного owner Source frame preview до promotion этот контракт
не предоставляет.

Перед окончательным переключением показать проверенный Source origin и
сохраняемый Native origin/target. Source ACK имеет scope
`authenticated-workspace` и включает выбранное действие с именем; отдельный
Native ACK остаётся `whole-port` для прежнего exact Native target. Третьего
дублирующего ACK не требуется. В существующей транзакции promote одновременно
сохраняются Source target/admission/policy/receipt и включается только этот
Source alias; прежние активные имена сохраняются. Это действие поддерживает
`anyone` и Root-workspace Source v2; при restricted оно не предлагается. Отсутствие
capability блокирует выбранную активацию с требованием обновления, без fallback
на публикацию прежнего проекта. Уже активный Source alias без нового action
сохраняет старый wire/hash/receipt контракт.

Сохрани exact action и проверенный Source origin/domain вместе с pending intent.
При lost ACK/reload проверяется прежний requestId; не создавай новую активацию и
не восстанавливай членство адреса по старой квитанции. При отзыве/смене имени,
actor/device/profile или позднем callback не переносить согласия на новый action.
Откат использует свежую текущую пару и сохраняет последние данные; неизвестный
результат не разрешает blind replay, backup rewind или возврат к старому Root
registry без Source admission. «Отключить отдельный сайт» по-прежнему требует
точных текущих origin/revision/digest и своего отдельного подтверждения.

При неожиданном expiry сначала измерь UTC clock difference Root/Source и RTT.
Root JWT срок, public Native app-policy lease и session — разные часы/границы;
локальная lease считается по monotonic clock и включает весь round trip.
Не лечи clock skew увеличением expiry tolerance или отключением проверки
подписи/issuer/audience. Проверь актуальные sandbox/COOP и точный trusted parent
origin: postMessage только в этот origin, не `*`. Script popup и same-window
callback сохраняют исходное намерение; отсутствие opener/ancestorOrigins не
разрешает подменить frame, cookie или добавить анонимный вход. Browser diagnostics
показывают безопасный код и статус без полного callback URL/headers/tokens.

Ограничения учитываются раздельно: BFF default128 context slots, Root256
continuations, qualified PG host4096 Source sessions; optional SQLite host4096
sessions/4096 nonce rows/20000 receipts на личный principal. Это не users limit
и не доказательство мировой нагрузки. Overflow503 означает временную ёмкость,
а не повод повторять вход. Reader3/образ Ordinary не доказывает schema нового
workspace SQLite; physical cold/restore и реальный DNS/TLS — отдельные gates.

## Human OIDC / BFF

1. Проверенный issuer, approved confidential client, точный HTTPS callback.
   Секреты и ключи — только на сервере, резервные копии зашифрованы.
2. Maintained `modules/source-rp`: Code + S256 PKCE, state/nonce, подпись,
   issuer/audience/expiry и свежий userinfo. Connect approval + human consent.
3. Собственная Secure/HttpOnly app session и native ACL. Стабильная связь
   `(iss,sub)` с local account; старый аккаунт привязывать только двусторонним
   proof, не по email/имени и не присваиванием owner ID.
4. Basic300 по умолчанию. Finite renewal требует reviewed host/client profile,
   выбора человека и durable encrypted Source/CAS. Истечение не обходить
   бесконечным refresh. Отозванные устройства/client/grant дают отказ.
   Запомненное устройство и конечное продление — разные capabilities; UI показывает
   только фактически включённый срок. Постоянный профиль Сот не означает постоянную
   сессию каждого приложения.
5. Два пользователя → отдельные данные/роли. Один человек → два приложения с
   одним issuer/sub и отдельными sessions. Write → полный reload → прежние IDs.

`modules/human-identity/examples/bff.mjs` — **test fixture**, не production SDK:
in-memory sessions, loopback cookies. Production API — Source RP плюс собственные
storage/migrations/native guards приложения. Обязательная схема создаётся до
optional Telegram/agent worker, иначе свежая установка не сможет работать.

Ingress `/_soty/session`/cookie разрешает открыть app/source, но не передаёт
human identity. Private single-owner adapter допустим только за restricted
gate, fixed loopback, current ACL/source pin и own CSRF/Origin. Его local user 1
нельзя переносить на многопользовательский вход.

Agent OAuth `/oauth` и MCP требуют собственных audience/scopes/app grants.
Доступность инструмента не означает разрешение читать или менять данные.
Human OIDC не выдаёт агенту права автоматически; Native ACL проверяется отдельно.

Для краткого guide общего входа: `node scripts/soty-app-release.mjs auth-guide --output NEW_FILE`
или Root MCP `apps_auth_guide({})`. Режим `human-oidc` — отдельный выбранный
confidential BFF/RP; другие режимы — private-owner
и agent. Если установленная схема MCP ещё требует `mode`, используй
поддерживаемую старой версией схему, не предполагая наличие нового guide.
Эти read-only помощники ничего не активируют и не требуют секретов.

Для существующего Python/FastAPI сохраняй Native data model; поддерживаемый
серверный OIDC SDK проверяй теми же настоящими Root protocol gates.
Собственный JWT verifier не нужен. Прежнюю private-owner строку можно сохранить
только для точного исходного Root владельца по закреплённой provenance + новому
human proof, до первого входа. Уже многопользовательская база требует отдельной
двусторонней привязки. Не импортируй токены старого канала в браузер.

Раздели также browser-local layout/preferences и обработай смену/выход во всех
вкладках. Проверь Native API/WS с чужим user_id в payload: identity берётся из
server session. Фильтры/избранное → очистка кеша → открытие из Native DB под
прежним ID. Callback/private API не кешируются, auth code не пишется в access log.

## Посмотреть и отозвать входы человека

Единый редактор: **Профиль → Безопасность → Входы в приложения**. Это Human
OIDC-семьи и запомненные согласия, не agent grants из «Действия ИИ и приложений».
Короткие Root Source-сессии здесь не перечислены: пустой список не доказывает
отсутствие доступа к приложениям внутри Сот. Отзыв issuer разрешения блокирует
следующую refresh/userinfo-проверку, но не обещает мгновенный logout или удаление
собственной Native cookie. `expiresAt:null` также не означает бессрочный вход.

Сначала проверь `GET /api/apps/capabilities → humanSignins`, затем capabilities
подписанного `identity.human.signins.list`. Public discovery не выдаёт прав;
schema1/unconfigured означает недоступность, а не пустой успешный список.
Каждый вызов требует свежих Connect actor/device и `expectedAccountId`:

```text
identity.human.signins.list({expectedAccountId, scope:'current-device', limit:20})
identity.human.signins.revoke({expectedAccountId, scope, entryId,
  expectedRevision:entry.revision, requestId:entry.revokeRequestId})
identity.human.signins.checkRevoke(originalExactIntent)
```

Scope `account` («Все мои устройства») выбирается явно и только при поддержке
сервера. `exact-session` закрывает одну семью; `app-device-version` забывает
согласие и семьи только того же приложения/устройства/версии. `clientId` — OIDC
client, не app ID и не адрес запуска. Перед revoke сохрани и перечитай bounded
pending reference отдельно по origin/account/caller-device; недоступное storage
означает ноль отправок. Используй выданные сервером entry/revision/requestId.
Смена профиля/устройства исключает старые callbacks и чужие pending references.

После потерянного ACK или reload выполняется **только readonly checkRevoke** с
прежними точными аргументами. `revoked` — текущий tombstone, не immutable receipt;
`active` — запись не отозвана, не готовность Native сессии; `unavailable` после
GC/CAS/ABA не доказывает отсутствие эффекта. Для нового отзыва человек выбирает
актуальную запись и подтверждает её scope. При checkout точные типы/контракт —
`modules/human-identity/types.d.mts`, `docs/implementation/human-signins-settings.md`.

## Завершить вход без лишней страницы

Сохрани исходное намерение и допустимый локальный return target в проверяемом
state до авторизации. При действующем разрешении продолжай сразу к работе.
Первое согласие, отзыв, новый профиль/client version/scopes и нужная проверка
остаются явными решениями человека.

- Script-opened popup после подтверждённого серверного обмена уведомляет
  исходное окно без токенов. Оно заново проверяет собственную сессию; событие
  BroadcastChannel/postMessage само не является proof. Popup можно закрыть,
  если оно действительно открыто скриптом.
- Same-window callback сразу ведёт к разрешённому исходному маршруту. Не
  принимай внешний `return_to` и не оставляй обязательное «Вход подтверждён →
  открыть приложение ещё раз» после уже успешного входа.
- Blocked popup, отсутствие opener/канала или отмена получают понятный fallback,
  без бесконечной петли окон. Полные callback URLs не попадают в логи.

## Несколько профилей и вкладок

Ожидаемый subject/frame связывается доверенным контрактом с исходным ticket/session
или Source context. Его не берут из UI-селектора или произвольного заголовка.
Совпадение текущих Root и Source cookies недостаточно: после полного входа B обе
cookie могут стать B, пока старый iframe ещё показывает A.

Проверь A открыт → B полностью входит → A делает HTTP, долгий WS, reconnect,
refresh и callback. Результат A либо ясный отказ; никогда данные B в окне A.
Смена/выход исключает устаревшие ответы и чужой кэш. Короткий proof на WS handshake
не является бессрочным разрешением всех сообщений. Неподдерживаемый/недействительный
frame binding не превращается в анонимный вход или fallback на другую cookie.
Если действующий контракт этого не обеспечивает, не обещай независимые профили
одного браузера и оставь этот сценарий явно неквалифицированным.

## Пределы сессий

Перед широким выпуском проверь actual issuer budgets, active families,
rotation/retention и storage: Source users, Basic300 и finite24 capacity не равны.
Два тестовых пользователя или повышенный config не доказывают массовую нагрузку.
При registration клиента сохрани keys и профили прежних clients. Если требуется
reload Root — свежая quiescence, encrypted restore proof и same-image/config CAS;
старый rollout из истории не воспроизводится.
