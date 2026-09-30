# P3-D0 — сохранённые приложения и независимое обсуждение

Дата: 2026-09-30. Основание — C2-C `60876fb`. Статус: согласованный архитектурный preflight; реализация D1 ещё не начата. В этом проходе изменён только данный документ, прочитаны текущие Apps/World/Connect и deploy reader, выполнен отдельный синтетический SQLite probe без пользовательских данных.

## Решение о хранении

**Выбран Apps5 в существующем `apps/registry.sqlite`.** Сохранение — личная запись аккаунта, обсуждение — самостоятельный контекст приложения. Ни одно из этих действий не создаёт grant, не вступает в сообщество, не подписывает на уведомления и не переносит историю World.

| Вариант | Оценка |
| --- | --- |
| Новые таблицы Apps5 | App/domain/publication/grants, смена аудитории, сохранённое и message effects получают общий SQLite snapshot и write transaction. Требуется честная миграция4→5, точный reader5 и совместимый fallback. Это предпочтительный вариант. |
| Врезка в World | Существующие `messages` имеют обязательный `community_id`, `memberships` и `requireMember`. Выдача app-ссылки не должна выдавать членство. Новый app conversation всё равно потребует отдельной модели/миграции World и нового release guard; текущий deploy manifest не проверяет World. Переиспользовать её под видом прежнего community chat нельзя. |
| Отдельная engagement DB | Сохраняет модульность файлов, но добавляет ещё одну границу ACL/effect, миграции, backup и rollback. Это не упрощение и не способ обойти reader gate. Сейчас не выбирается. |

Фактические точки: `modules/world/server/schema.mjs:29` — memberships; `:42` — community messages; `modules/world/server/chat.mjs:5` — `requireMember`, авторские/moderator удаления и tombstone; `modules/apps/server/index.mjs:106` — owner/direct/community допуск; `publications.mjs:139` — exact domain/active alias/public policy. В `src/world/app.ts:862` закреплённые apps пока сохраняются локально; они не становятся серверными bookmarks автоматически. Миграция не копирует private community messages.

## Канонический допуск и граница World

Новые операции используют тот же смысл Apps policy: app state, bound exact domain, active alias, текущие grants и публичная политика. Входящий `accountId`, заявленный `accessBasis`, origin или роль не являются authority. Actor приходит только из подписанного Connect; `expectedAccountId` проверяется существующей границей. Публичный посетитель после входа остаётся public-basis, если действующего grant нет.

Нужна action-aware безопасная projection этой политики, а не выдача runtime decision наружу: app identity/name, выбранный permanent entry context, разрешённые действия и доступный conversation. Она не содержит connector key/device/port/route, ticket, target pins, скрытые grants или состав закрытых сообществ. Exact domain берётся из registry; клиент не задаёт произвольный return URL. Offline, отсутствие binding ACK и остановленный upstream сами по себе не запрещают обсуждение или сохранение при действующем доступе.

Одной общей Apps DB недостаточно: `canUse` проверяет World membership **и** текущую административную роль владельца app в предоставившем доступ сообществе. Несколько последовательных чтений разных DB не дают общего атомарного snapshot.

Согласован host-owned синхронный **World authority fence**:

1. Подписанная синхронная extension уже выполняется внутри Connect `BEGIN IMMEDIATE`/savepoint (`modules/connect/server/index.mjs:665`). Поэтому порядок захвата — **Connect → World → Apps**.
2. World открывает короткий `BEGIN IMMEDIATE`; в этом fence нет записей World. Внутри выполняется ограниченная Apps transaction, общая policy и projection/result. Текущие World predicate читают собственный handle в этой же World transaction; membership writer не вклинивается до завершения Apps операции.
3. Apps COMMIT предшествует освобождению World. Callback только доверенный из composition, синхронный, без await/network; thenable отклоняется. Отсутствующий fence, nested/неподходящая transaction и acquisition failure отказывают до effect. Никаких путей или callback из RPC args.
4. Все новые engagement операции сначала проходят этот порядок. Доказуемое исключение public/direct-only операций — будущая измеряемая оптимизация, не первая версия. Reverse Apps→World acquisition запрещён; World membership notifications сейчас вызываются после World COMMIT (`modules/world/server/index.mjs:69`).
5. Busy policy порядка100ms вместо блокировки event loop на5s; типизированный503 позволяет повторить **тот же requestId**. `busy_timeout` не является жёстким wall-clock deadline. Нужны счётчики отказов/длительности без PII и ограниченные страницы/body. Ошибка World release после Apps COMMIT означает неизвестный ACK; retained receipt подтверждает уже принятый effect, он не откатывается повторным исполнением.

Это не обещание двухфайлового атомарного COMMIT: durable новые записи только в Apps. SQLite сериализует writers через `BEGIN IMMEDIATE`; WAL read snapshot сам по себе не блокирует другого writer. [SQLite isolation](https://www.sqlite.org/isolation.html). ATTACH не выбран: SQLite отдельно предупреждает, что WAL не обеспечивает общий crash-atomic commit изменений нескольких DB. [SQLite ATTACH](https://www.sqlite.org/lang_attach.html).

Локальный probe использовал настоящий `createWorldService.canAccessCommunity`, отдельный World fence connection, Apps SQLite и второй worker, меняющий World membership. Под fence worker получил `SQLITE_BUSY(5)`, Apps effect сохранился; после освобождения revoke принят, следующее чтение отказало. Пройдено100 незагруженных коротких секций: p50=0,019ms, p95=0,028ms; это Windows synthetic measurement, не оценка production ёмкости. Настроенные100ms ожидания дали180ms фактического времени заблокированного worker, поэтому жёсткий срок не заявляется. Первый запуск имел ошибку ESM worker fixture до эффекта; исправленный запуск завершён, worker/DB закрыты, только созданный временный каталог удалён. Product fence API пока не реализован; signed integration, rollback/failure и lock-order tests обязательны в D1.

## Сохранённые приложения

Принята ограниченная модель:

- `app_saved_heads`: одна постоянная монотонная revision на аккаунт.
- `app_saved_entries`: только действующие bookmarks, максимум200 на аккаунт; ключ account+app. Содержит permanent domain context, проверенный локальный entryPath фактически открытого входа и ограниченный snapshot законно увиденного названия. Ни boot-ticket, ни session, ни arbitrary launch URL туда не попадают; hash/query допускаются тем же path validator.
- `app_saved_receipts`: последние128 принятых команд аккаунта, exact intent hash/request key, revision и минимальный результат. Никаких свежих приватных карточек в receipt.

Минимальный API: `apps.saved.list {limit,cursor}` и `apps.saved.set {appId,domainId?,entryPath?,saved,expectedRevision,requestId,expectedAccountId}`. У отсутствующих domain/path применим только согласованный текущий разрешённый вход, без угадывания origin из legacy local pin. Это явное желаемое состояние, не toggle. Каждый принятый set, включая semantic no-op, продвигает head; retained replay не продвигает. CAS делает старый запрос безопасным после prune: удалённая запись не воскресает от прежнего save. Remove освобождает active quota и не требует текущего доступа к app; рост квоты не блокирует удаление. Unsaved rows можно удалять без бесконечных per-app tombstones, head сохраняется.

Цена account-wide CAS — конфликт двух одновременных изменений разных apps. UI читает актуальную revision и предлагает явное повторное действие; автоматически перебазировать неизвестный запрос нельзя. List сообщает revision, использует bounded cursor, стабильный порядок и не вытесняет bookmarks ради лимита.

При отзыве app, retirement сохранённого alias или утрате доступа запись остаётся личной пометкой `unavailable`. Можно показать **собственный прежний** label и удалить её; нельзя обновлять название/иконку/участников/counts из нынешних private данных. Новый адрес не выбирается автоматически по старой публичной ссылке. Повторное сохранение с новым проверенным entry context — отдельное явное действие. Existing local pins остаются отдельной настройкой раскладки; автоматического массового upload истории в миграции нет.

## Обсуждение и неизменяемая аудитория

Самостоятельные `conversationId`/messages app, не `communityId`. Минимальная модель Apps5: head текущего conversation, immutable conversation audience, сообщения с durable client operation identity/tombstone, отдельные app moderation grants/bans и личные preferences. Полные DDL/индексы замораживаются до объявления reader5. Если будущий шаг потребует иной DDL после выпуска Apps5, это следующая миграция, не незаметное изменение формата.

Согласованный audience predicate: owner + точные direct account IDs + точные community IDs и mode public/restricted. Membership внутри уже выбранных сообществ динамическая, как в World: **новый участник того же предоставившего доступ сообщества может читать прежнюю app discussion**. Это явный продуктовый контракт; snapshot всех людей и его огромная materialization не вводятся. Читатель не получает имена/состав остальных закрытых групп из audience metadata.

- Изменение нормализованного grant-set или переход public↔restricted создаёт новую lineage в той же Apps transaction, прежняя закрывается для новых сообщений. Public→private→public создаёт новый public conversation; старый поток не переоткрывается автоматически.
- Source switch/rollback, rename и epoch-only изменение не создают новую lineage, если аудитория семантически прежняя. Два активных alias одного app ведут в текущий app conversation, а не две случайные копии истории; retirement entry context всё равно проверяется заново.
- Старое restricted обсуждение читается только при **текущем grant-допуске плюс исходном predicate**. Public-basis никогда не заменяет grant для старой private истории. Потеря membership, административной роли владельца в группе, app grant или отзыв app прекращают обычное чтение. Новая grant-группа не открывает прежнюю историю другой lineage.
- Архивный public разговор не становится private секретом задним числом, но сервер больше не выдаёт его через отозванный/неактивный контекст. Уже полученный человеком текст невозможно отозвать из его копий. Правила выборки архивов проверяются отдельно; UI не объединяет разные audience в общий feed.
- Для revoked app обычные saved fresh metadata и discussion access дают generic unavailable. Root отдельно утвердил **owner-only административное чтение архива и redaction**, без send/reactivate. Проверяются active actor, `actor.accountId === app.owner_account_id` и принадлежность historical conversation этому app. Эта явно отдельная ветка по appId не становится domain/launch bypass; UI показывает владельцу «Приложение закрыто / История обсуждения». D1 saved эта ветка не нужна, она принимается с D2 discussion.

Предварительный минимальный набор операций: `apps.discussion.get`, `.list`, `.send`, `.remove`, `.preferences`, отдельные owner moderation/report действия. В D1 фиксируются точные DTO, без новой архитектуры. Чтение и участие D1 — подписанные; гостевой публичный runtime продолжает открываться без аккаунта, guest chat read не подразумевается. Send принимает exact conversation/audience generation, `clientId`/requestId, bounded plain text и optional replyTo только той же conversation. Новая audience во время ожидания не переносит сообщение автоматически в другой поток.

`send` и receipt хранят один effect identity; одинаковый ID с другим intent даёт conflict. Удалённый message остаётся tombstone с неизменяемым digest/identity, повтор не восстанавливает тело. Результат после revocation — минимальное подтверждение собственной прежней операции, без тела/свежих чужих данных; новую команду оно не авторизует. Page cursor привязан к app/conversation/допуску; после получения cursor ACL проверяется снова. Наружу не выдаётся глобальный raw message sequence, раскрывающий чужую активность. Отдельный bounded changes cursor включает removal/redaction, иначе старое уже загруженное сообщение вне tail останется видимо. После истечения retention — явный `reset_required` с очисткой stale history и сохранением draft/pending, а не ложное «все изменения получены».

Новый подписанный аккаунт может ещё не иметь World profile. Нельзя скрыто вызывать World `ensurePerson` внутри обещанного read-only fence. Author DTO обсуждения ограничивается разрешёнными name/avatar или trusted actor label/нейтральной fallback; bio/interests/contact settings/private memberships не копируются. First-use без World profile входит в signed acceptance. Profile provisioning при необходимости остаётся отдельным явным World потоком.

## Ограничения и модерация первого выпуска

Ограниченные body, страницы и rate buckets обязательны до public участия; стартовые пределы фиксируются в D1 validation, не обещаются как бесконечная ёмкость. Практичная исходная граница — plain text до6000 символов с отдельным byte cap, страница20/максимум50, лимиты по account+conversation и общий app budget. Вложения/произвольный HTML/автоматический unfurl не входят в первый шаг. Storage quota отказывает новым сообщениям явно, но не блокирует delete/report/mute; молчаливого trimming истории нет.

Владелец app и явно назначенные app moderators могут скрывать сообщения и ограничивать участие; World moderator не наследует эти полномочия автоматически. Ban относится к обсуждению, не к запуску приложения. Для ban/unban/remove/report нужны idempotency, content-free audit и явно ограниченные payload/частота. Автор может удалить известное собственное сообщение даже после потери read-доступа; этот узкий ownership action не возвращает чужую историю. Владелец должен иметь возможность модерировать сохранённые записи отключённого приложения без возобновления runtime/public доступа. Это отдельные action-aware правила общей policy, не обход читательского ACL.

Открытие/сохранение/отправка не включают уведомления за человека. Личные mute/read state не меняют membership. Polling/read cursors ограничены, поздние ответы ограждаются account+screen+conversation generation. Счётчики доступны только после тех же проверок аудитории; для unavailable bookmark они отсутствуют, а не равны придуманному нулю.

## Последовательность и владение D1

| Участок | Предлагаемый владелец и точные файлы |
| --- | --- |
| Apps5 DDL/миграция, saved registry | `publishing_architecture`: `modules/apps/server/schema.mjs`, новые `modules/apps/server/saved.mjs`, `modules/apps/test/app-saved.test.mjs`, `modules/apps/test/app-engagement-schema.test.mjs`, `docs/implementation/p3-saved-model.md`. Имена/полный DDL заморозить до reader правок. Не менять root composition. |
| World authority fence и composition | Root: `modules/world/server/index.mjs` и узкий новый World fence test; `modules/apps/server/index.mjs`, `server/http-app.js`; shared action-aware Apps policy adapter/внутренние hooks по согласованному API. Connect proof transaction не переизобретать. |
| Trusted reader5 и release | Root: `deploy/connector/storage-probe.mjs`, `storage-guard.mjs`, related deploy tests, корневой `Dockerfile`, reader receipt. Exact Apps4 historical fixture берётся из60876fb, не создаётся новой миграцией. Rooms предел остаётся2. |
| Независимая приёмка | `whole_product_critic`: отдельные новые acceptance files для saved/World fence, без изменения авторского кода. Исторические fixtures независимые; реальные два SQLite writers, подписанный HTTP, fault/replay/приватность. |
| UI | `agent_ecosystem`: сначала отдельный UI preflight; implementation после заморозки domain DTO и приёмки model. Local pins не подменяют server saved. Discussion UI и registry — следующий последовательный подпункт с отдельным владельцем, без параллельной правки тех же source. |

Registry зависимость для saved: общий `DatabaseSync`, clock, trusted actor validator, host-owned synchronous authority fence и общий safe entry resolver. Store сам ограничивает транзакции; resolver не выполняет сеть. Root передаёт одни и те же canonical policy/World predicates, не самодельный второй `canUse`. Точные factory/operations заморозить перед разделением файлов.

Порядок: полный Apps5 DDL и migration invariants → trusted reader5/historical/future6 refusal → World fence + signed composition → saved model/idempotency/ACL → независимые acceptance → UI и настоящий браузер → discussion registry/lineage/moderation в следующем согласованном подпункте. Не рекламировать reader5 до реально совместимого source. Если discussion DDL не удаётся честно заморозить в D1, saved выпускается с узкой Apps5 схемой, discussion получает Apps6; нельзя скрыто дописывать формат5.

## Обязательная приёмка и выпуск

- Миграция4→5 сохраняет все app/source/domain/grant/publication/receipt строки, floor2 после rollback1, revoked/tombstones; не создаёт grants, сообщений, membership или opt-in уведомлений. Missing/corrupt/unknown future schema отказывает без auto-repair.
- Реальная main4→WAL5; Apps4-only image получает отказ до START/recovery/rollback, Rooms gates сохранены. Старый уже работающий writer остановлен до миграции; форматный marker сам его не останавливает.
- World writer revoke/owner-role removal конкурирует с signed engagement operation; строгое lock order, bounded contention, throw до effect, throw после Apps COMMIT, thenable rejection и close проверяются. Нет ложного заявления о cross-file write atomicity.
- Saved replay после удаления/prune/restart не воскрешает запись; два окна с разными apps получают честный CAS conflict; чужой account/retired entry/unlisted app не раскрывают свежие детали. Remove разрешён при full quota и revoked access.
- Private→public, grants expansion, removal, public→private→public, source-only change и новый член прежней группы проверены на отдельных history lineages. Runtime access, discussion roles и World membership не подменяют друг друга. Reply/cursor/tombstone/receipt не пересекают conversation.
- Release по-прежнему требует свежий backup, изолированное restore, compatible candidate/fallback, exact-image Linux reader probe и уже выделенный runtime DNS/TLS gate. Новый набор таблиц включается в существующую Apps backup единицу; отдельного неучтённого хранилища не появляется.

Модель сопоставлена с [независимым D0 preflight](p3-engagement-critic-preflight.md): согласованы Apps5/fence, saved200/128/head, dynamic membership в исходной группе, новая lineage при смене audience, owner-only revoked archive, минимальный author DTO и changes/tombstone catch-up. Конкретные пороги moderation/storage и DTO — ограниченные решения D1/D2. Публичную запись нельзя включать до P3-E operational abuse/moderation gate; preflight не обещает, что эти механизмы уже работают.
