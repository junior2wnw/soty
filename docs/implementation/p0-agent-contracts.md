# P0 ADR: общий доступ и контракт приложений для внешних ИИ

Дата: 30.09.2026. Статус: решение для реализации P1 и граница P4; не отчёт о готовом OAuth/MCP или проверенной интеграции.

Основание: [мастер-план](../plans/soty-human-agent-platform-20260930.md), прежде всего P1/P4/P6 и разделы 8–12. Исследован release worktree, а не состояние production. Секреты, пользовательские конфигурации клиентов и содержимое рабочих баз не читались.

## 1. Решение и первая полезная поставка

Добавить модуль `modules/capabilities/server` поверх существующего Connect. Он владеет **одним SQLite-хранилищем** допусков, служебных личностей, лимитов, Invocation, dispatch intent и receipts. HTTP, MCP и собственный помощник обращаются к одинаковой доменной политике. Отдельного аккаунта человека или второго журнала выполнения для агентов не будет.

Первая поставка P1: владелец через подписанный Connect создаёт служебную личность, ограниченный допуск и удостоверение; сервер допускает Invocation по реальным актуальным правам и атомарно резервирует общий лимит; повтор, конфликт, отзыв родителя и получение минимального статуса проходят локальные интеграционные проверки. Публичный адаптер принимает только проверенное удостоверение. До регистрации исполнимого обработчика admission не становится разрешением выполнить произвольную команду.

P1 не объявляет выполненными Notes-цепочку P4, OAuth, MCP, исполнение стороннего кода, ограничение shell или платёжный биллинг. Полезный результат P1 — общий работающий контур допуска и долговечной истории, который последующие этапы используют без второй базы и второй модели прав.

## 2. Что установлено чтением кода

| Текущая граница | Следствие |
|---|---|
| `modules/connect/server/index.mjs`: локальный P-256 challenge/response, project/origin/digest binding, host-created actor, extension API | Управление доступом добавляется как Connect extension; проверенный `actor.accountId` является владельцем |
| `modules/connect/server/http.mjs`: обязательный Origin и подписанный RPC | Не ослаблять RPC для bearer-клиентов и не подставлять фиктивный Origin; внешний HTTP получает отдельный адаптер |
| `server/connect-module.js`: project-local `accounts.sqlite`, `sharedSso: false` | Connect не является OAuth/OIDC issuer или общей личностью для других продуктов |
| ADR-0002/0003: canonical shared identity — отдельный Control Plane; OIDC/BFF не заменяется локальным Connect | Новый модуль не выдаёт cross-product login, не объединяет пользователей по email и не меняет эти ADR |
| Connect `isActorActive` проверяет account/device; async extension обязан повторить проверку после ожидания | Перед выдачей допуска и каждым эффектом проверяется живое состояние; callback отзыва после commit не является единственным источником истины |
| Notes `.execute` требует accountId/deviceId и expectedAccountId | ServicePrincipal нельзя проводить через Notes с вымышленным deviceId; нужен отдельный доверенный доменный метод |
| Notes put делает CAS и mutation receipt; get читает текущий документ; receipts ограничены 32 на note | Invocation хранит собственный долговечный минимальный receipt; create-only доступ не читает последующие правки |
| Notes purge оставляет tombstone | Повтор исходного создания не восстанавливает удалённую заметку |
| Текущий durable job уже имеет requestId/fingerprint/lease/cancel/uncertain | Invocation ссылается на job; не создаётся вторая очередь lease/retry |
| Старый connector знает agent/chat/command/script; неизвестный kind может попасть в agent fallback | Новый executor нельзя включать до strict reject, version admission и совместимого reader/runtime |

## 3. Файлы, единственная база и экспорты P1

Хранилище: `data/capabilities/capabilities.sqlite`, Node `DatabaseSync`, WAL, foreign keys, синхронные короткие транзакции. Новый additive schema lineage и reader version проверяются при открытии. Не принимать неизвестную версию как пустую базу. Connect и Notes сохраняют свои базы: атомарность между ними не обещается.

Зоны реализации: `index.mjs`, `schema.mjs`, `access.mjs`, `validation.mjs`, `catalog.mjs` — доступ и контракты; `invocations.mjs` — долговечный вызов и dispatch; composition/HTTP/Connect glue — основной интегратор. Бюджеты принадлежат общей базе, а не HTTP/MCP-адаптерам.

```js
createCapabilitiesService({
  databasePath, clock = Date.now, actorActive, catalog = []
})
// {
//   operations, execute({ op, args, actor }),
//   authenticateCredential({ token, audience }),
//   authorize({ actor, capabilityId, version, resources, effects, recipients }),
//   invocations, close()
// }

createInvocationStore({
  db, clock, transaction, authorize, reserveBudget, settleBudget,
  canonicalHash, newId, limits
})
```

`transaction(fn)` принадлежит service и выполняет BEGIN IMMEDIATE → синхронный fn → COMMIT/ROLLBACK. Promise в такой транзакции запрещён; сетевого ожидания в ней нет. `authorize`, `reserveBudget` и `settleBudget` — синхронные операции над тем же db без вложенной транзакции. Admission заново проверяет actor и всю grant-цепочку, затем резервирует лимит и пишет Invocation/dispatch intent атомарно. Простой предварительный `authorize` не резервирует расход и не является бессрочным разрешением.

Actor и разрешённый descriptor создаются только внутри модуля. Обычный объект из JSON, даже с правильными ID, не становится actor. В памяти применяется проверяемое происхождение объекта; после рестарта актор создаётся заново из реального удостоверения. Авторизованный вызов всё равно повторно проверяет credential, client, principal, grant ancestry и срок по БД.

### Доменное управление через Connect

Предлагаемые операции: `access.principals.create/list/revoke`, `access.grants.issue/derive/list/revoke`, `access.credentials.issue/revoke`. Возвращаемые DTO минимальны: ID, метка, состояние, scope/ресурсы, срок и даты; хеш удостоверения не возвращается. Входной `expectedAccountId` используется только как утверждение совпадения, как в Notes; владельца выводит host-created Connect actor.

Идентичность внешнего клиента и человеческий аккаунт — разные поля. ServicePrincipal всегда связан с account и ClientConnection; `clientId` не берётся из произвольного HTTP-заголовка. Для P1 служебная личность создаётся явно владельцем; регистрация OAuth клиента и получение human-delegated credential добавляются через тот же доменный интерфейс в P4.

Удостоверение — случайное непрозрачное значение достаточной энтропии, в базе только digest. Выдаётся один раз владельцу, не попадает в audit/error, URL, manifest или tool result обычного действия. Потерянный ответ выдачи не восстанавливает секрет из хеша: владелец отзывает его и выпускает новый. Повторные полномочия не выдаются автоматически по авторскому тексту или имени клиента.

### Общая схема хранения

| Таблица | Минимальное содержание / инвариант |
|---|---|
| `cap_clients` | accountId, clientId, label, state, policy epoch; принадлежность проверяется в каждой операции |
| `cap_principals` | principalId, accountId, clientId, kind=service, state, creatorDeviceId; служба не имитирует человека |
| `cap_grants` | grantId, account/client/principal, parent/root ID, scope, ресурсы, эффекты, получатели, время, глубина делегирования, epoch/revokedAt |
| `cap_credentials` | credentialId, digest, audience, principal/client/grant, expiry/revokedAt; секрет не хранится |
| `cap_budgets` | общий root budget и единица измерения; целочисленные nonnegative limit/reserved/spent, никакого floating money |
| `cap_budget_reservations` | invocation/attempt, root budget, зарезервированный предел, settlement state; повтор reservation не списывает дважды |
| `cap_invocations` | account/client/externalKey uniqueness, canonical fingerprint, grant/version/binding snapshot, execution/effect state, revision |
| `cap_dispatch_intents` | постоянный внутренний requestId, invocationId, pinned target/executor/version, dispatch/reconcile состояние, jobId |
| `cap_receipts` | минимальный исторический результат, версия контракта, artifact ID/revision, известный эффект; не тело документа |

В P1 registry может быть небольшим неизменяемым набором проверенных серверных descriptors; загрузка произвольных схем/кода автора отсутствует. Версия контракта и digest входят в Invocation. Durable catalogue publication/migrations появляются по нужде P4/P6, сохраняя эти ID.

## 4. Правила допуска и бюджета

- Scope — точный capability/version, без wildcard `*`, произвольного shell/URL и regex, заданного моделью. Ресурсы, эффекты и получатели сравниваются как нормализованные множества. Серверные поля отсутствуют в input schema.
- Потомок имеет только пересечение родительских прав и не более длинный срок; одинаковый account, проверяемый parent/root, ограниченная глубина и явное разрешение делегирования. Trace parentId сам по себе не создаёт grant.
- Credential, principal, client и вся ancestry проверяются повторно при admission/чтении/выдаче исполнения. Отзыв предка закрывает потомков без ожидания eventual event. Долгая работа требует P5/P6 lease/epoch и подтверждённой остановки; мгновенная остановка при partition здесь не обещается.
- Для P1/P4 бесплатная операция использует целочисленную квоту `invocations`. Все потомки делят root limit. Повтор того же принятого запроса не создаёт новый расход. Дополнительная реальная попытка резервируется отдельно; неизвестный расход удерживает резерв.
- Платное выполнение не включается лишь из-за наличия ledger. До него нужны измеримые upper bounds и enforcement исполнителем; unsupported units/отсутствующий предел дают отказ.
- Решение о доступе проверяется при чтении receipt; create-only право допускает минимальный статус собственного вызова, но не поиск/чтение account-wide Notes.

## 5. Invocation, повтор и границы атомарности

`admit({ actor, capabilityId, version, idempotencyKey, input, target? })` связывает ключ с проверенными account/client. Fingerprint включает immutable capability/version/digest, нормализованный input, разрешённые эффекты/ресурсы и target/binding. Тот же ключ с иной семантикой — conflict; тот же запрос возвращает прежний Invocation только после повторной проверки доступа.

Во внешнем API idempotency key не является паролем или публичным ID результата. Внутренний job requestId выводится из Invocation ID, чтобы не столкнуться с requestId другого клиента в account-wide job store. Последовательность: сохранить admission + dispatch intent → создать/найти job по постоянному requestId → записать jobId. После сбоя повторяется reconciliation, а не произвольный новый запуск. Существующий job остаётся владельцем lease и выполнения.

Execution state и effect state независимы. `cancelRequested` не означает `cancelled`; transport timeout не означает отсутствие эффекта. Успешный exit code не доказывает результат приложения. Отзыв/отмена после создания Notes не даёт права удалить заметку; новое удаление требует отдельного scope и текущей revision.

## 6. Типизированный HTTP и Notes в P4

Предлагаемые независимые маршруты:

| Маршрут | Доступ / назначение |
|---|---|
| `GET /api/capabilities/v1/catalog` | Только публичные метаданные без личного токена; bounded query/cursor |
| `GET /api/capabilities/v1/catalog/:capabilityId/versions/:version` | Описание, JSON Schema, эффекты и прямой typed route |
| `POST /api/capabilities/v1/notes/drafts` | Конкретный create-only grant, строго `{ title, body, idempotencyKey }` |
| `GET /api/capabilities/v1/invocations/:id` | Минимальный статус/receipt своего вызова по актуальному допуску |
| `POST /api/capabilities/v1/grants/derive` | Только явное delegation право; потомок не получает root credential |
| `POST /mcp` | P4 remote MCP адаптер тех же доменных операций |

У Notes вводится доверенный метод `createDraftForAccount` внутри server domain API: авторизованный account, заранее сохранённые noteId/mutationId, title/body; сервер фиксирует expectedRevision=0, items=[], color=plain, pinned=false, state=active. Публичный API не получает общий `notes.put` и не подделывает deviceId. Оригинальный signed Connect путь сохраняет свои проверки.

До вызова Notes сохраняется durable intent со стабильными noteId/mutationId. После потери ACK reconcile должен получить **доказательство исходного создания**, даже после последующих правок/trim обычных Notes receipts. Нужен долговечный create receipt или отдельный безопасный internal reconciliation метод в Notes; одного чтения текущего документа недостаточно. Tombstone и Invocation не удаляются раньше заявленного окна повторов. Точный срок retention фиксируется перед P4.

Ответ содержит invocationId, noteId, original revision, effect state и защищённый deep link. Последний открывает текущее состояние в PWA и не выдаётся за snapshot. Ни последующий body, ни секретный bearer не возвращаются create-only клиенту.

HTTP ошибки: 400 invalid input, 401 missing/invalid credential, 403 insufficient grant, 404 недоступный объект без раскрытия существования, 409 idempotency/revision conflict, 429 quota/rate limit, 503 выбранный handler недоступен. Возвращаются стабильный code и requestId, bounded details без секретов. CORS/Origin проверяется, если Origin присутствует; headless без Origin допустим только на отдельном authenticated API. На Connect прежнее требование Origin сохраняется. Ответы авторизации и частные результаты — no-store; публичные страницы не используют приватную cache key.

## 7. OAuth/MCP: выбранный ограниченный путь

Для P1 headless pilot — явное непрозрачное scoped service credential. Это не OAuth и не пользовательская браузерная сессия. P4 добавляет OAuth resource-server adapter, который после проверки issuer/audience/client/token связывает существующий local account и grant; создание/изменение связи требует доказанного владельца. Ни email, ни внешнее произвольное `sub` не являются основанием объединить аккаунты.

Authorization Server **сейчас отсутствует**. Перед P4 выбрать и закрепить поддерживаемую реализацию AS или доверенный внешний AS; отдельно проверить approval через существующий Connect. Resource authorization для Сот не должен незаметно превратиться в новый cross-product IdP в нарушение ADR-0002. AS и resource server могут размещаться вместе или раздельно; MCP сам по себе не реализует AS. Не писать «OAuth уже есть» по наличию experimental identity endpoints. Это открытая зависимость P4, а не причина задерживать P1.

Для пилота достаточно pre-registered двух клиентов, Authorization Code + PKCE S256, точных redirect URI с документированным loopback исключением, одноразовых коротких code, issuer/resource binding, проверенного revoke/refresh поведения. Публичный unrestricted dynamic registration, CIMD fetching и client_credentials extension не требуются для первого service-token/двухклиентного proof. Не добавлять их до реального клиента и отдельной проверки SSRF/metadata trust.

Resource metadata публикуется по RFC 9728; AS metadata — RFC 8414. Реальные canonical resource URLs фиксируются до выпуска. На невалидный токен — настоящий 401 и `WWW-Authenticate` metadata pointer, на недостаточный scope — 403. Удостоверение другого ресурса отклоняется, access token не принимается в URL, не пересылается авторской app. OAuth scopes выбираются минимальными; `offline_access` не является scope самой операции Notes.

MCP предоставляет постоянный небольшой список typed tools: `catalog_search`, `catalog_get`, `notes_create_draft`, `invocations_get`. Каталог может содержать больше приложений, чем клиент способен вызвать; это явно отражается в ответе. Нет универсального `execute`, скрытого `activate_tool` или полного каталога в каждом контексте. Аннотации read-only/destructive/idempotent описательны; авторизация находится на сервере. Авторские descriptions/results — данные, а не инструкции изменить grant или отправить секреты.

Предлагаемый **compatibility baseline для испытания**: явно выделенный Streamable HTTP adapter редакции MCP `2025-11-25`, SDK `@modelcontextprotocol/sdk@1.31.0`. Это осознанный legacy-профиль, не скрытая смесь с новым stateless core `2026-07-28`. В P4 закрепить lockfile и фактическое negotiated revision обеих сред, либо выбрать отдельно испытанный current adapter. До такого прогона совместимость не объявляется. MCP Tasks/A2A/WebMCP/MCP UI не являются зависимостью первого Notes proof.

## 8. Две выбранные клиентские среды и предел доказательств

| Среда | Проверенный сейчас факт | Проверка P4, которая ещё не выполнена |
|---|---|---|
| Codex CLI на Windows, обнаруженная версия `0.153.4` | CLI доступен; официальная документация описывает remote Streamable HTTP, bearer/OAuth и регистрацию клиента | Реальный handshake с Сотами, negotiated revision, pre-registration/PKCE, token scope/revoke, русские и английские задачи; документация может опережать установленный бинарник |
| OpenCode `1.18.15`, отдельный тестовый профиль | Версия и Windows/Linux release SHA закреплены в `scripts/agent-modules/opencode-release.mjs`; CLI не найден в PATH; официальные docs описывают remote OAuth и headers | Воспроизводимо запустить подписанный/pinned release, подключить именно как внешний клиент, проверить auth/transport/задачи; существующий Soty subprocess не является этим доказательством |
| Node `24.13.1` fetch, контрольный HTTP-клиент | Версия runtime проверена | Та же Notes цепочка без MCP, service principal и child grant; это транспортный proof, а не третий «проверенный ИИ» |

Пользовательские глобальные конфигурации клиентов не меняются ради теста; используется отдельный профиль/стенд. Матрица хранит точную версию server SDK, клиента, auth mode, revision, дату и результаты. Успех двух клиентов не означает совместимость всех ИИ.

## 9. Обязательные проверки

P1: два аккаунта/клиента; поддельный actor; неверные audience/credential/client/account; отозванный credential/principal/ancestor; expiry на границе; child widening/depth/expiry; общая квота параллельных потомков; атомарный отказ без Invocation/резерва; повтор/conflict; потеря ACK и restart; no replay после uncertain; повторный settlement; запрет неизвестной схемы/версии/executor; выборки не раскрывают чужие ID/counts; errors/audit без token/body.

P4: positive create→receipt→PWA; lost ACK до/после эффекта; retry после restart; тот же ключ/иной payload; последующее редактирование и trim обычного Notes receipt; удаление и retry без resurrection; create-only попытки list/get/overwrite/purge; смена аккаунта и отзыв между discovery/execute/get; refresh/revoke/resource mix-up; denied invalid Origin; cancellation без rollback; HTTP/service/child и два реальных MCP клиента. Неподходящий запрос должен корректно отказаться, а не максимизировать tool-call rate.

Приватные данные не попадают в HTML/robots/sitemap, выдачу, hit counts или описание tools. Public HTML, OpenAPI и каталог повышают доступность; они не подключают клиента, не выдают grant и не гарантируют рейтинг в поиске ИИ.

## 10. Первоисточники, перечитанные 30.09.2026

- [MCP authorization 2026-07-28](https://modelcontextprotocol.io/specification/2026-07-28/basic/authorization): разделение AS/resource server, audience/resource, metadata, минимальный scope; OAuth 2.1 и CIMD ещё ссылаются на IETF drafts.
- [MCP transport 2025-11-25](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports) и [tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools): отдельный выбранный compatibility profile, typed tools, Origin и transport errors. Новые правила другого revision не подмешиваются без теста.
- [TypeScript SDK releases](https://github.com/modelcontextprotocol/typescript-sdk/releases/tag/1.31.0): конкретный кандидат SDK; факт выпуска не доказывает совместимость с выбранным клиентом.
- [RFC 9700](https://www.rfc-editor.org/info/rfc9700/) и [RFC 9728](https://www.rfc-editor.org/rfc/rfc9728.html): OAuth security BCP и protected resource metadata; использовать проверенную реализацию и проверять binding, а не собирать псевдо-OAuth вокруг bearer.
- [Codex MCP](https://learn.chatgpt.com/docs/extend/mcp) и [OpenCode MCP](https://opencode.ai/docs/mcp-servers/): источники для двух клиентских профилей; описанные в docs возможности всё равно испытываются на закреплённых бинарниках.

Неопределённости ограничены соответствующими gates: конкретный AS и совместимость клиентов — P4; Notes durable create reconciliation — до публичного create; enforcement долгого стороннего исполнения — P5/P6. Они не скрыты за словами «универсально» и не требуют перепроектировать существующий Connect.

## 11. Уточнения после первого P1 implementation review

В утверждённом стыке сохранён один DatabaseSync. Добавлены durable `cap_contracts` pins и `cap_audit`; изменение semantic digest при прежнем capability/version отклоняется после рестарта. Operational `executionEnabled` исключён из digest: включение уже согласованного обработчика не меняет версию. Списки principals/grants/events ограничены и имеют account/filter-bound cursor; grant DTO показывает reserved/spent/remaining/uncertain в единицах бесплатных вызовов.

Конфигурация лимитов разделена: `limits.access.maxGrantTtlMs` и `limits.invocations.{inputBytes,metadataBytes,pageSize}`. Неизвестные поля отклоняются до открытия базы. Создатель service principal и каждого grant остаётся связан с доверенным Connect device; его отзыв закрывает полномочие при следующей проверке, даже без доставки observer event.

Служебный read/history scoped по account+client+principal+grant. Для человеческого «Доступы и действия» добавлен отдельный подписанный `access.invocations.list`: host проверяет Connect actor и expectedAccountId, затем вызывает internal `listForOwner`; service actor не имитируется. Результат минимален и не содержит input, authorization snapshot или token. `access.events.list` показывает content-free выдачу/отзыв доступа владельцу.

Детали API и фактические команды тестов — [README модуля](../../modules/capabilities/README.md). Реализация P1 не снимает перечисленные выше gates OAuth/MCP/Notes P4 и не объявляет внешние клиентские интеграции проверенными.
