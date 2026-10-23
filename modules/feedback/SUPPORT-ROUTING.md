# Поддержка Сот: цель и минимальное подключение

Источник решения: действующее уточнение пользователя от23.10.2026 — поддержку
Сот и наших приложений обрабатываем мы с агентом; поддержку чужого приложения
обрабатывает размещатель сам, агентом Сот или внешним агентом. Это уточняет
получателя и полномочия, но не разрешает объединять чужие частные очереди.

Цель — одно понятное место для обращения и обработки с сохранением владельца,
контекста, переписки и подтверждения результата. Подцели: правильный маршрут,
удобный агенту доступ, один сохранённый эффект при потере ответа, проверка автором.

## Подтверждённый приём обращений о самих Сотах

23.10.2026 через существующий owner UI и штатный вход Сот создана ровно одна
очередь «Поддержка Сот»: `fp_e10dd963e039c476c9139a9a2a7d4749`.
После полной перезагрузки кабинета и отдельного чтения «Подключения» ID и ссылка
совпали; прежний проверочный проект сохранён. Account ID интерфейс не раскрывает,
он не угадан по имени профиля. Квитанция
`SOTY-SUPPORT-OWNER-QUEUE-BINDING-20261023.safe.json`, SHA256
`4b3ae084e983ab4fb2f4e0ec60089c082e6a8c7600fc21b0ae5ba6f88591c620`.

Форма: <https://feedback.4-2.xn--p1ai/p/fp_e10dd963e039c476c9139a9a2a7d4749>.
`publicSubmit=true` открывает только приём; содержание очереди остаётся частным.
UI самих Сот использует эту ссылку в меню, профиле, справке и общем экране ошибки.
С поля до формы два нажатия: «Меню» → «Поддержка Сот».
Она открывается отдельно, без query, контекста пользователя и сброса рабочей
области. Публикация UI проверяется отдельно от реализации. Тестовые обращения
не отправлялись; новые management grants не выдавались.

Это готовая привязка публичной формы, а не новый серверный facade, credential
агента или router приложений. `supportOwnerAccountId` для ссылки не нужен.
Наши приложения не добавляются автоматически; чужие очереди не перенаправляются.

## Что уже переиспользуется

`modules/feedback-mcp` — действующий движок проектов и10 MCP tools. Очередь
привязана к настоящему проекту `fp_…`, а не выдуманному Connect account recipient.
Native owner/ограниченный project credential определяет доступ. Публичная форма
может принимать обращения без регистрации; каждый guest читает только свои.
Публичный projectId и website не дают права читать всю очередь и не доказывают
владение сайтом. Внешний MCP имеет собственный OAuth/PKCE audience.

`installed-catalog.mjs`, `source.mjs` и product-agent composition предоставляют
те же операции при установленном Source, текущих grants и Native authority.
Добавленные catalog references по умолчанию не получают authority. Это отдельный
путь от Root OAuth. Штатные receipts, requestId, expectedRevision, revocation,
постраничное чтение, media metadata и reporter acceptance сохраняются.

`apps.withAppAuthority` доказывает текущего размещателя по каноническому Apps
registry и держит authority fence до эффекта. Это не реестр наших приложений.
Наличие домена, названия, Source-профиля или доступность в каталоге тоже не такой
реестр. Принадлежность нашей поддержке должна быть явной проверенной записью.

`server/core-feedback.js` в b2ac — отдельный default-off facade над Feedback1;
он требует настоящий `supportOwnerAccountId` и не обращается к Feedback project.
Он не подключён к новой целевой модели этим изменением. Не менять его immutable
installation owner и не включать fake recipient, чтобы обойти отсутствие binding.

## Очередь чужого приложения и самостоятельный проект

Проверено по Source `a8ba2549d58db36f2896ec90bcf7045fca05389f`, 23.10.2026.
Это два существующих пути; они не требуют нового общего router или копии данных.

| Цель | Existing runtime и сохранение | Остаток |
| --- | --- | --- |
| Сообщить размещателю о его приложении | Stage «Сообщить проблему» → `apps.feedback.context/submit`; SQLite installation `fbi_…` привязана к owner/app/environment. | Для этого не нужен отдельный `fp_` project. Готовность конкретного live приложения проверяется отдельно. |
| Размещателю обработать обращения | Та же форма → «История» → `apps.feedback.list/get/reply/status`; owner видит «Обращения приложения», reporter — только свои. Receipts/revisions сохраняются; результат принимает reporter через `accept`. | Сохранить действующий Apps/Connect authority и агентный контекст; историю не переносить. |
| Обработать самостоятельный Feedback project | Cabinet `/` → свои проекты → очередь; внешний `/mcp` и10 tools используют те же `projects.sqlite`/Feedback engine своего deployment. | Это не Root `fbi_…`; website/название не являются binding к размещению. |
| Подключить внешнего агента к project | OAuth/PKCE → `feedback_projects_list/integration_get/tickets_list/ticket_get`; записи с исходными requestId и expectedRevision. | Production consent сейчас даёт чтение/создание/изменения всех собственных проектов. ProjectId в input не сужает grant. |
| Подключить агента Сот к project | `source_catalog` → actual reference/schema → `source_read_1/source_write_1/source_receipt_1`. | Требуются установленный Source и текущие grants; workspace не становится project-limited от одного projectId. |

`service.issueCredential({projectId,readOnly,durationMs})` уже ограничивает доступ
в движке. Но это host-only API: UI/MCP его не выдаёт, production
`auth.authenticateBearer` принимает OAuth и создаёт account-wide actor. Existing
`support-scope.test.mjs` подставляет fixture credential auth; его GREEN не доказывает
такой production OAuth path. Действующий consent честно описывает широкий доступ.

Минимальное продолжение для project-limited внешнего агента — выбор собственного
проекта и read-only/manage в существующем OAuth consent, проверка выбора сервером,
сохранение projectId/readOnly в существующем encrypted GrantLink и передача этих
ограничений в уже существующий `service.actor`. Старые grants нельзя тихо
переобозначать: сохранить их явный all-own смысл либо запросить новый consent.
Нужны actual OAuth negatives: другой проект того же owner, другой owner,
readonly write, refresh/restart и revoke. Это предложение, production не изменён.

### Реализованный Source slice: очереди приложений через Root MCP

В локальном Source подготовлен adapter семи `apps.feedback.*` операций в текущем
Root capability/MCP контуре. Это серверный/API slice, не подтверждение его
развёртывания или выдачи live grants. Действующие UI, API и агент Сот сохранены;
нового сервера, registry, очереди, OAuth audience или копии истории нет.

1. Под текущим размещателем выдаётся обычный signed Connect grant на выбранные
   `apps.feedback.context/submit/list/get/reply/status/accept` версии1. Обязательны
   base resource `apps.feedback` и точный дополнительный resource
   `apps.feedback:app-<32hex>` для каждого разрешённого размещения; recipients —
   `soty:app-feedback`, effects соответствуют выбранным операциям.
2. Агент использует существующие `apps_catalog_search/get`, актуальные returned
   reference/version/digest и `apps_query` (list/get) либо `apps_invoke` (остальные).
   Отдельные семь MCP tools и per-app регистрация контрактов не создаются.
3. `context` — операция с эффектом: она может создать installation metadata и
   сохраняет квитанцию в прежней `feedback_receipts`. Её artifact даёт настоящий
   `installationId`; он совпадает с действующим `fbi_…` данного размещения.
4. Delegated credential отображается через существующий `withOwnerAuthority`
   на своего текущего grant creator, включая обычного размещателя другого
   аккаунта. Дополнительный resource проверяется по текущему leaf grant до и
   после callback; сам host port новых прав не выдаёт. Apps owner/Connect fences
   действуют до эффекта, чтения и перед выдачей HTTP/MCP результата.
5. List возвращает до10 summaries, get — до20 сообщений в byte-bounded странице
   с `messagesPage.nextOffset` (`-1` означает конец). Вложения — metadata-only:
   SQL не читает BLOB. Старый явно выбранный UI/API get сохраняет оригиналы;
   standalone `feedback_attachment_get` нельзя применять к `fbi_…` IDs.

Записи используют прежние domain intent keys, `expectedRevision`, request receipts
и общий capability ledger/budget. Exact retry одной записи не повторяет эффект.
Exact retry readonly query возвращает только исторические metadata; для нового
разрешённого чтения нужен новый query intent, а не автоматический повтор эффекта.
Отзыв после законного COMMIT не отменяет запись: ответ скрывается, повтор после
отзыва не создаёт вторую. Передача owner существующего placement сейчас не
поддерживается и блокируется FK/immutable target; эти ограничения не отключались.

Existing signed API/Connect SDK умеет выдавать и показывать точные resources.
Нынешняя ручная форма `access-panel.ts` выдаёт только Notes, а Notes OAuth consent
не выдаёт эти grants автоматически. Удобный явный выбор приложения/операций в
существующем Connect UI — следующий отдельный UI шаг. Полная готовность такого
grant UI этим Source slice не заявляется; default grants не расширены.

Проверены disposable actual Root composition: signed placement, normal grant
неадминистративного owner, все7 операций, sibling/foreign/read-only/resource deny,
отзыв до dispatch и перед wire output, один эффект/replay/reload, metadata до I/O.
Схемы БД прежние. Public Source admission остаётся64, внутреннее объединение
ограничено catalog bound128, поэтому прежние64 Source +7 fixed contracts допустимы.

Exact b2ac `core-feedback.js` не является таким adapter: его `CORE_SCOPE` жёстко
равен `soty.core.feedback`, caller appId запрещён, owner берётся из configuration,
а interface — signed Connect extension, без Root MCP binding. Его включение
создаёт отдельную core installation; оно не открывает очереди размещателя.
Не включать его ради app queues и не заменять им подтверждённую центральную форму.

## Контракт маршрутизации, ещё не включённый в runtime

| Случай | Маршрут | Что доказывает доступ |
| --- | --- | --- |
| Соты | Подтверждённый управленческий Feedback project | Текущие Native owner/project-scoped grants и host-approved queue binding |
| Наше приложение | Та же управленческая очередь с контекстом приложения | Явный approved registry + свежая Apps placement/source authority |
| Чужое приложение | Подтверждённая очередь фактического размещателя | Его текущая Apps/Native/Source authority; отдельный ограниченный grant его агенту |
| Нет подтверждённой привязки | Маршрут недоступен до технической привязки | Не выбирать аккаунт, очередь или права из текста/домена/каталога |

Минимальная запись нашей привязки должна ссылаться на точный Feedback origin,
projectId и квитанцию проверки текущего владельца/прав. Для приложения нужны
appId, текущий размещатель и Source/target pin из авторитетного Apps read и явное
одобрение включения в наш registry. Это предложение будущего host-only контракта,
не новая таблица, не схема авторизации и не автоматически сформированный реестр.
Registry по умолчанию пуст; неизвестное приложение не становится нашим.

Центральная очередь — адрес обращений о Сотах и общий рабочий вид.
Старые частные обращения не переносятся автоматически. Если core facade уже
содержит историю, подключение должно сохранить её и показать источник отдельно;
immutable tenant нельзя тихо переназначить новой очереди.
Единый рабочий процесс может выбирать уже разрешённые `fp_…` и `fbi_…` источники
без переноса их данных и без нового центрального router/registry.

## Следующее подключение частного управления агенту

Очередь уже создана; повторно `project_create` для неё не выполнять. Наличие
публичной ссылки и Root OAuth не разрешает агенту читать обращения.

1. Через существующий Feedback MCP или авторизованный Source прочитай
   `feedback_projects_list({limit:50})` с пагинацией и
   `feedback_integration_get({projectId:"fp_e10dd963e039c476c9139a9a2a7d4749"})`.
   Используй текущие owner права либо отдельно разрешённый grant этого проекта.
2. Внутри агента Сот найди references через `source_catalog` и используй схему
   операции `projects_list`/`integration_get`. Не подставляй reference из примера.
   Для записей сохраняются request receipts и fresh read; выдача management grant
   не входит в добавление публичной ссылки.
3. Для наших приложений отдельно сверь current placer/source и явный approved
   registry. Чужие очереди не импортируй; агент размещателя получает доступ
   только к его проекту через существующую authority.
4. Будущий частный router должен сохранить старую историю и доказать права →
   actual call → persistence → reload → denial для другого владельца. Публичный
   ingress самих Сот не подменяет эту работу.

В tool inventory этой задачи Feedback MCP/Root Source tools не подключены.
Actual привязка получена через owner UI; частный доступ агента не настраивался.

Локальные сценарии выполняются только на disposable проектах/аккаунтах. Они
подтверждают пригодность существующего механизма. Создание очереди и readback
подтверждены отдельно через UI; это не проверка отправки обращения, развёртывания
Source, микрофона или готовности private agent management.
