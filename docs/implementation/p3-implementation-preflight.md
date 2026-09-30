# P3 — подготовка именных приложений и публикации

Дата проверки: 30.09.2026. Основание: `docs/plans/soty-human-agent-platform-20260930.md`, §13 и P3.1–P3.6. Это read-only preflight перед реализацией P3; P2 ещё проходит свой checkpoint. Ни код, ни DNS, ни production-конфигурация этим документом не изменены. Фактические секреты и конфигурационные значения не читались и не выводились.

## 1. Вывод и выбранная граница

Основа уже рабочая: стабильный App ID, владелец, привязка устройства/connector, grants аккаунтам и сообществам, одноразовый запуск, HTTP/WebSocket relay и закрытие активного доступа при отзыве. P3 расширяет этот модуль, не создаёт второй реестр, второй Connect или отдельную систему прав.

Главные отсутствующие части: устойчивое имя и его история владения; публичная политика запуска; безопасная классификация всех app-hosts; отдельный разговор приложения; сохранение на сервере; эксплуатационные ограничения публичного входа. Переименование карточки и wildcard DNS не реализуют эти свойства.

**Решение согласовано с root:** именной runtime-origin исполняет чужое приложение. Карточка, сохранение, обсуждение, управление и запуск в рамке принадлежат доверенной оболочке Сот. На runtime-origin не размещаются авторизованный shell, Connect credentials и данные профиля Сот. Прямая ссылка открывает приложение; доверенная карточка имеет собственный стабильный deep link по App ID. Универсальная панель поверх произвольного приложения не является требованием P3, HTML автора не переписывается для её внедрения.

Первый полезный implementation slice после checkpoint P2: **AppDomain + закрытая host-маршрутизация + приватный запуск существующего приложения по выбранному имени**, с сохранением старого App ID/grants и защищённым конфликтом имён. Публичный admission включается только после политики доступа, ограничений, проверок происхождения и реального domain/browser gate из раздела 9.

## 2. Подтверждённый baseline

| Область | Сейчас | Значение для P3 |
|---|---|---|
| `modules/apps/server/index.mjs:22` | SQLite `local_apps`, `app_devices`, нормализованные `local_app_grants`; строковая версия схемы | Расширять эту БД явной миграцией и минимальной версией читателя |
| `index.mjs:38–64` | Текущий Connect actor, owner; account/community grants; проверка членства и полномочий автора community grant | Сохранять повторную серверную проверку; публичность не превращать в поддельного actor |
| `index.mjs:155–191` | Регистрация по connector/port; случайный App ID; private по умолчанию; update имени/grants, revision и revoke | Нет адреса, публичного audience, смены RuntimeTarget и compare-and-swap |
| `index.mjs:174–180` | Signed launch, короткий одноразовый ticket, online connector | Гостевого входа нет; добавление slug к URL само по себе не даёт публичный launch |
| `index.mjs:193–248` | Разбор только случайного `app-*` host; ticket → HttpOnly host cookie; текущая ACL на каждом запросе | Нужны AppDomain lookup, проверка domain/origin в ticket и отдельный публичный доступ |
| `index.mjs:86–115` | Отзыв tickets/sessions/HTTP/WS, проверка connector credentials; in-memory сессии | Хорошая база; политика публикации/смена источника должны участвовать в той же инвалидизации |
| `protocol.mjs:34–35` | Header allowlist исключает Cookie, Authorization и Set-Cookie | Совместимость с произвольной авторизацией веб-приложений пока отсутствует |
| `scripts/agent-modules/local-apps.mjs:126–178` | Запрос только к зарегистрированному `127.0.0.1:port`; ограниченные HTTP/WS-потоки; внешние redirects не допускаются | Сохранять узкий relay, не превращать в произвольный proxy/SSRF |
| `local-apps.mjs:242` | HEAD на entryPath; ответы 200–499 считаются достижимостью | Это не проверка отображения, безопасности, авторизации или правильности приложения |
| `index.mjs:374–393` | TLS разрешён точному enabled App ID; template проверяет отличие origin от shell | Нет проверки site-границы; TLS eligibility сейчас смешана с доступностью runtime |
| `deploy/apps/edge-config.mjs` | Подготовка Caddy candidate с конкретной legacy-зоной и on-demand TLS ask | Потребуется параметризовать проверенную зону; эта подготовка не доказывает текущие DNS/владение |
| `src/world/app.ts`, `application-card.ts` | Карточка, iframe, новый launch при открытии отдельно, private/community форма | Расширять существующие маршруты/компоненты, не делать вторую публикационную оболочку |
| `modules/world/server/chat.mjs`, `schema.mjs` | История и права привязаны к community; отдельного app conversation нет | Публичное обсуждение нельзя получить переиспользованием закрытого чата сообщества |
| `src/world/product.ts` | Сохранение/недавние приложения в локальном account-scoped состоянии | Для надёжного возврата с другого устройства нужен серверный saved-app/Placement |

Проверено текущим кодом: `node --test modules/apps/test/*.test.mjs deploy/apps/edge-config.test.mjs` — **14/14 PASS**. Это включает реальный HTTP/assets/POST/WS через тестовый relay, двух допущенных пользователей и отказ третьему, отзыв активных соединений, ticket replay/wrong Origin, SSRF/порты, bounded streaming, offline/stopped, registry restart, registration retry, установленный runtime и WebSocket framing. Прогоны используют временные тестовые данные; они не подтверждают публичную production-зону, новый guest policy или браузерную совместимость P3.

## 3. Блокеры публичного admission

1. **Unknown app-host может попасть в shell.** Сейчас `appForRequest` возвращает null для имени, не подходящего под случайный App ID; `handleRequest` возвращает false и основной сервер продолжает маршрутизацию. Существующий TLS ask ограничивает этот путь для неизвестных HTTPS-имён, но переход к именам/wildcard-сертификату убирает этот барьер. Каждый host app-зоны, включая неизвестный/отозванный, должен завершаться внутри app-router. Неизвестный host не обслуживает shell, Connect, API и websocket control route.
2. **Отличие origin недостаточно.** `validateTemplate` не проверяет registrable-domain/site. В текущих исходниках допустима общая site-зона shell и пользовательских приложений. Разделение хранения по origin сохраняет значение, но не даёт отдельный cookie/site-контур и не позволяет доверять всем sibling subdomains.
3. **У публичного адреса отсутствует самостоятельный admission.** `launch` всегда требует текущего signed account/device. Нельзя обходить его через фиктивного владельца или выдавать посетителю credentials владельца. Публичный доступ должен быть отдельным типом решения политики, проверяемым при каждом relay-запросе и при отзыве потоков.
4. **Старый адрес не должен отдавать чужое приложение.** Нет атомарного AppDomain registry, aliases/tombstones и долговечного владельца имени. Отзыв исполнения не должен удалять адрес и право показывать безопасный HTTPS-статус. TLS admission не равен runtime admission.
5. **Нынешний relay — ограниченный профиль.** Cookie/Authorization/Set-Cookie не проходят; CSP запрещает service worker, frame, внешнюю сеть; Permissions Policy запрещает микрофон/камеру и другие чувствительные API; OAuth redirects не поддерживаются. Это нельзя обещать как универсальное размещение любого сайта. Публичная публикация целого порта также может открыть API/debug/admin routes, не только entryPath.
6. **Проверка источника не аттестация кода.** Подтверждённый connector связывает маршрут с устройством и разрешённым портом. Автор может заменить процесс за тем же портом. HEAD 401/404 тоже сейчас считается ответом процесса. Ни этот probe, ни авторское превью не подтверждают неизменность сборки или аудит её безопасности.
7. **Публичный runtime не делает публичным разговор.** App, каталог, capability, сохранение и разговор требуют самостоятельной политики. Сообщения community не копируются и не расширяют аудиторию вслед за приложением.

## 4. Origin, cookie и TLS — контракт реализации

### 4.1 Проверяемая граница доменов

- Для публичного пилота выбрать подтверждённую user-content зону, чей registrable domain отличается от всех доверенных shell/auth origins. Проверять через актуальный Public Suffix List, не через последние две части строки. Локальная разработка получает отдельный явный режим; разные localhost-порты не являются доказательством production site isolation. [R1, R5]
- Общий registrable domain между пользовательскими приложениями всё ещё делает их same-site. На первом ограниченном профиле не доверять sibling origin: точные Origin, host-only cookies, закрытый CORS, отсутствие ambient credentials Сот и CSRF-проверки. Отдельная user-content зона отделяет shell от apps; она не обещает site/process isolation каждого приложения от каждого. Если последующий профиль требует такой границы, нужен отдельный проверяемый domain/PSL design; регистрацию PSL нельзя считать мгновенной или уже выполненной.
- Классифицировать нормализованный authority один раз до shell/API/upgrade dispatch. Допустимые ports/trailing dot/IDNA и доверие forwarded headers задаются явно. Пользовательский `X-Forwarded-Host` не выбирает app. Неизвестный, неканонический и tombstone host возвращают bounded отказ/статус с безопасными заголовками, без чужих метаданных.
- Origin-Agent-Cluster сохраняется как браузерная подсказка. Это не доказательство отдельного процесса и не замена ACL, CSP или site-границе. [R9]

### 4.2 Сессия и запросы

- Ticket привязывать к `appId + domainId/exactOrigin + publicationEpoch + runtimeTargetRevision + actor/accessKind + expiry`. После смены audience/target старый ticket не получает новые права. При нескольких aliases нельзя использовать ticket одного host на другом.
- Cookie шлюза остаётся `__Host-`, Secure, HttpOnly, Path=/, без Domain. Не передавать её авторскому backend и не разрешать backend установить одноимённую cookie. Partitioned/SameSite=None проверяются в реальных браузерах: partition зависит от top-level site, поэтому открытие отдельно может требовать нового launch. CHIPS не является авторизацией приложения. [R2, R3]
- Для небезопасных cookie-auth методов нужен точный Origin либо отдельно реализованный проверяемый CSRF-механизм; отсутствие Origin не считать разрешением. WebSocket сохраняет exact-Origin admission. `same-site` в Fetch Metadata не доверять, если sibling apps принадлежат другим авторам. Чувствительные эффекты не должны выполняться по GET. [R4]
- Канонизировать и закрыть служебный namespace одинаково на edge/server/runtime: encoded underscore/slash, backslash, dot segments, повторное декодирование и разные представления одного пути. Нынешний raw `startsWith('/_soty/')` недостаточен как будущая защита закрытого control route. P6 control port остаётся отдельным и запрещённым для обычной регистрации.
- Не ослаблять Connect Origin/proof-контракт ради гостевого просмотра. Author/control операции остаются signed Connect. Anonymous runtime/public metadata получают отдельные узкие маршруты без аккаунтных полномочий.
- Расширение Cookie/Authorization, OAuth, SW, media и external network требует именованного compatibility profile и отдельной приёмки. Не включать это общим «передавать все headers»; учитывать авторскую CSRF/auth-политику, зарезервированные cookies, redirects и границу доверия входного Host/Origin.

### 4.3 TLS, имена и захват адреса

- TLS ask делает быстрый точный lookup по долговечному AppDomain, без DNS-запроса в handshake. Известный отключённый адрес может быть TLS-eligible для безопасного статуса, но не runtime-eligible. Неизвестные имена не разрешают выпуск сертификата; имя проверяется до регистрации. [R8]
- В пилоте отдельно ограничить число имён/выпусков и частоту запросов. On-demand TLS не обещает бесконечную пропускную способность CA. Переход на wildcard challenge и сертификатное хранилище — эксплуатационный выбор с отдельной проверкой, не условная имитация готовности.
- Платформенный name takeover предотвращается уникальным реестром, immutable owner binding и tombstone. DNS takeover предотвращается инвентарём доменов/ресурсов и правильным удалением внешних DNS-привязок до уничтожения ресурса с учётом TTL. Это разные угрозы. Автоматический перенос имени другому владельцу в P3 не поддерживается. [R6]

## 5. Минимальная модель и API

Ниже предложенный совместимый контракт, а не существующие exports. Точные имена закрепляются перед совместным кодом. Все записи остаются в соответствующем уже существующем модуле; второй реестр app или вторая система account identity не вводятся.

### Данные

| Сущность | Минимум | Инвариант |
|---|---|---|
| App | Существующий ID/owner/grants; activeTargetId; publication epoch/revision | ID не зависит от устройства, имени, публикации и runtime |
| AppDomain | Нормализованный FQDN/zone, appId, ownerAccountId, canonical/alias/tombstone state, revision, timestamps | UNIQUE FQDN в БД; старое имя остаётся связанным с прежним App/owner |
| RuntimeTarget | Immutable revision: connector key/device, port, entryPath, compatibility profile; текущий pointer отдельно | Изменение источника не переписывает адрес/разговор; rollback возвращает target, не пользовательские данные |
| Publication | launch policy, explicit account/community grants, listing state, quarantine/revoked, policy epoch | Public launch, listing и capability readiness независимы; private/unlisted исходно |
| Mutation receipt | account, operation, requestId hash, normalized intent digest, result reference | Lost acknowledgement возвращает прежний результат; новый intent с прежним ключом — conflict |
| Saved app / Placement | accountId, appId, createdAt, stable ordering/cursor | Сохранение не даёт доступ, не вступает в community и не подписывает на уведомления |
| Conversation | appId, отдельная аудитория/правила/история/read state | Private community history остаётся private, публичный разговор создаётся отдельно |
| Operational audit/report | App ID, actor where known, reason/state, time; bounded content-free diagnostic fields | Жалобы/ограничения можно разобрать, не записывая tokens и содержимое запросов |

Именование: display name остаётся Unicode, slug — lowercase ASCII letters/digits/internal hyphen, предварительно 3–48 символов. Зарезервировать системные имена и legacy `app-*`; финальный список лежит в одном модуле, не расходится между UI/edge/server. Availability check — только подсказка; право на имя создаёт транзакционный UNIQUE insert. Квоты предотвращают массовое занятие имён. Длительная неоплаченная «бронь без app» не нужна первому этапу.

`По ссылке` нужно определить честно: **unlisted app доступна каждому, кто знает или угадает её адрес; это не секрет**. Confidential sharing выполняется существующими account/community grants. Секретные invitation links, если понадобятся, получат отдельный expiring grant, а не магические права из slug. `В каталоге` включается отдельно и явно.

Миграция старых записей сохраняет App ID и все действующие grants. Она не делает приложения публичными, не включает listing и не расширяет разговоры. Старый случайный hostname регистрируется как прежняя привязка, а не назначается новому автору. Новый человекочитаемый hostname появляется по явному действию владельца.

**Изменение origin не переносит cookies/IndexedDB/localStorage самой app.** App ID continuity сохраняет данные платформы, но не доказывает перенос авторского browser storage. Старые origins остаются обслуживаемыми по прежнему App ID. Перед сменой canonical origin требуется реальный compatibility check и понятное подтверждение повторного входа/переноса данных. Rename не означает автоматическое cross-origin копирование данных или повторное использование старого имени. Самый простой пилот закрепляет canonical runtime-origin после первого публичного выпуска; новые aliases/название карточки не меняют его автоматически.

### Серверные интерфейсы

- Сохранить текущие `apps.register/list/update/revoke/launch`, добавить versioned DTO и совместимый bounded list. Actor всегда приходит из реального Connect guard; authority не берётся из args.
- `apps.name.check({slug})` — bounded advisory result без private owner metadata; частоту ограничивать. `apps.domain.claim({appId,slug,requestId,expectedRevision})` — atomic claim и mutation receipt. Не возвращать данные владельца занятого имени.
- `apps.publication.update({appId,launchPolicy,listing,requestId,expectedRevision})` — независимая политика; before/after summary, recheck owner, source/profile и текущих community прав; смена epoch инвалидирует затронутый доступ.
- `apps.source.prepare/activate` либо один CAS-вызов после probe — только точный ранее подтверждённый connector/port/path/profile, без произвольного URL и shell. Persist target до dispatch; свежий runtime acknowledgement связывается с конкретной target revision. Новый публичный профиль допускается только совместимым runtime; старому runtime неизвестный профиль не подменяется известным.
- `apps.inspect({appId})` — scoped read model: owner получает диагностику/действия исправления, допущенный пользователь — нужный минимум, гость — только разрешённую публичную карточку. Не переиспользовать нынешний внутренний `publicApp` без фильтра: он содержит owner/host-device поля. ACL выполняется до pagination/counts.
- `apps.save/unsave`, bounded `apps.saved` — серверное сохранение App ID, без выдачи доступа. Локальные pins/recent можно оставить быстрым кэшем; миграция локальных сохранений требует текущей ACL.
- Public GET detail/launch маршруты не вызывают signed Connect от лица владельца. Public access descriptor содержит текущую app policy, epoch и пределы; он не является account/service principal credential. Private неизвестные/недопущенные apps не раскрывают название, участников, target и grants.
- Добавить типизированный app-conversation API в world-модуле; существующие community APIs остаются совместимыми. Общий UI может использовать один renderer/adaptor, но app conversation не создаётся как фиктивная community и не копирует её сообщения. Для постинга требуется авторизованный участник соответствующей аудитории и rate limit; аудитория чтения определяется отдельно.

`createAppsService` получает проверенные zone/profile settings и доверенные author/membership callbacks. `server/http-app.js` и `server/index.js` остаются единственными местами общей HTTP/upgrade интеграции. Динамический local connector/control port передаётся в `blockedPorts` и на сервер, и runtime; сейчас runtime знает свой порт, а серверная фабрика не получает эту опцию, что может создать зарегистрированное, но неработающее приложение.

## 6. Интеграция нового UI

Использовать существующие `WorldAppRecord`, `application-card.ts`, `apps-home.ts`, `app.ts` и `platform/local-apps.ts`, после заморозки P2. Не возвращать старую оболочку и не форкать карточку в отдельный «публикатор».

1. **Источник:** существующий подтверждённый компьютер → процесс/порт. Если у одного host несколько connectors, option идентифицируется точным connector binding, а не только hostDeviceId. Порт/путь остаются в дополнительной настройке; пользователь видит источник и его фактическую доступность.
2. **Имя:** Unicode заголовок и понятный адрес; подсказка свободности не обещает бронирование. Конфликт сохраняет введённое, предлагает варианты; серверная ошибка не раскрывает private app.
3. **Предпросмотр:** реально открыть приложение через тот же profile/relay в sandbox; указать время и target revision. Не подменять preview декоративной обложкой. При заблокированных OAuth/media/SW показать конкретную несовместимость, а не сообщение «готово».
4. **Аудитория:** «Только я», существующие явные grants/сообщество, «У кого есть ссылка», «Публично». Отдельный выбор каталога; разговор и capability имеют отдельные состояния. Итог перед нажатием показывает точный адрес, источник и аудиторию, включая предупреждение о зависимости от включённого устройства.
5. **Открытие:** карточка/сохранение/разговор остаются на trusted Soty origin; именованная ссылка — прямой runtime. В PWA используется уже существующий app-frame с кнопкой назад. Private direct-link направляет к проверенному trusted return path, без произвольного redirect, token в query и утечки fragment.
6. **Возврат:** сохранить в аккаунте; открытие с другого устройства обращается к тому же App ID и текущей ACL. Отозванный сохранённый app не исчезает молча: безопасный статус с удалением из личного списка.
7. **Диагностика:** различать «устройство подтверждено», «устройство offline», «источник отвечает», «превью проверено», «процесс недоступен», «доступ закрыт». Хранить observedAt и ограниченный срок свежести. Живой tunnel не означает живой процесс, а живой процесс не означает безопасный проект.

Источник может отвечать на 404, отдавать другой проект или поменяться после preview. Для P3 устанавливается проверяемая привязка маршрута и время проверки, а не криптографическая аттестация сборки. Attested immutable artifact относится к будущему hosting/build этапу; этот термин не должен появляться на карточке локального проекта без соответствующего доказательства.

## 7. Последовательность реализации и зоны ответственности

| Шаг | Работа | Gate перед следующим шагом |
|---|---|---|
| P3-A | Явная DB migration, AppDomain, атомарные claims/receipts, reserved names/tombstones; host classifier и TLS eligibility | Конкурентный claim/restart/lost ACK, unknown host никогда не попадает в shell; legacy apps сохраняют ID/grants |
| P3-B | Typed publication decision, domain/epoch-bound launch; revocation/session/HTTP/WS; exact-origin/CSRF/path guards | Матрица private/granted/unlisted/public; гостю не выданы owner credentials; смена политики закрывает активный доступ |
| P3-C | RuntimeTarget revisions, bounded list/projection, fresh observations, real preview и UI имени/аудитории | Один реальный device project проходит private launch и named launch; source mismatch/offline не маскируются |
| P3-D | Серверные saved apps и отдельные conversations; typed UI adaptor | Другой аккаунт/устройство видит только разрешённое; закрытая community history не попадает в app conversation |
| P3-E | Лимиты/жалобы/quarantine/support; проверенная зона/DNS/TLS; 3–5 реальных проектов и браузеры | Полный чистый visitor flow и принятие эксплуатационных ограничений; только затем публичный пилот |

Непересекающиеся зоны при назначении кода: platform engineer — `modules/apps/server/*` registry/policy/router и свои tests; relay engineer — `scripts/agent-modules/local-apps.mjs`, versioned runtime contract и свои tests; frontend/product engineer — World DTO/adaptor/cards/publishing UI; root — shared server/deploy glue и координация migration/chat. Одновременно не более трёх субагентов; owners согласуют DTO до параллельных изменений. Это предложение распределения, не запуск агентов.

Сначала выкатывается читатель/guard совместимой схемы и runtime-профиля; затем допускаются новые записи/публикации. Rollback не запускает старую бинарную версию, которая может проигнорировать новые audience/domain states. Сначала закрыть admission, затем перейти на совместимый reader или прежний RuntimeTarget. Новые имена, receipts, grants, saved apps и разговоры не удаляются откатом сборки.

## 8. Проверки, которые должны появиться

### Реестр и политика

- Два конкурентных подключения/процесса выбирают одинаковый slug: один владелец имени, второй получает conflict; повтор после потери ACK возвращает прежнее имя. Повтор того же ключа с иными параметрами отклоняется.
- Reserved/invalid/Unicode-in-host/case/trailing-hyphen/oversized имена; aliases не создают цикл; tombstone не переходит другому owner после restart/timeout/revoke. TTL DNS не заменяет registry ownership.
- Миграция populated v1 сохраняет App ID/grants/старые launch origins; private не становится public, listing выключен. Несовместимый старый writer не открывает обновлённую БД для записи.
- Матрица owner/account/community member/non-member/anonymous × private/granted/unlisted/public/quarantined. Отзыв community membership/полномочий автора влияет на текущий launch/HTTP/WS. Private metadata не появляется в counts, cursors, errors и чужих saved списках.
- Preview/update без expected revision не может потерять конкурентное изменение; source/epoch pins не интерпретируют старый ticket как допуск к новому target.

### HTTP, TLS и браузер

- Известный app, неизвестный slug, malformed host, port, suffix-confusion и spoofed forwarded host через настоящий `createHttpApp`/upgrade. App host никогда не получает shell HTML, Connect endpoints или connector control channel.
- TLS allow: неизвестный отказ; известный active/status-only/tombstone по принятой политике; bounded no-store endpoint; отказ certificate admission не используется вместо runtime ACL.
- Wrong/absent/null Origin на unsafe method, sibling same-site Origin, duplicate cookies, ticket replay, ticket на другом alias, stale epoch; redirects и return URL не уводят credential на чужой origin.
- Encoded/reserved paths и разные декодирования tested end-to-end, а не только одним helper; control ports и arbitrary upstream URL не проходят регистрацию/runtime.
- Реальные iframe/top-level launch в поддерживаемых Chromium и WebKit/Firefox средах по доступности; отдельно private/incognito и blocked third-party cookies. Cookie/CHIPS поведение наблюдается, не выводится из успешного Node HTTP теста. Если среда недоступна, совместимость остаётся неизвестной, а не PASS.
- Два злонамеренных sibling-app fixtures пробуют parent-domain cookie, same-site POST и websocket; trusted shell storage/credentials не выдаются. Test shell/app находятся на разных site в реалистичной TLS-среде, не только разных портах localhost.
- Реальный app, использующий OAuth/media/SW/external network, либо проходит объявленный профиль, либо получает точный заранее известный отказ; авторское sensitive API не оказывается доступным лишь потому, что порт опубликован.

### Жизненный цикл и пользователь

- Устройство отключено/процесс остановлен/404 вместо приложения/просроченное observation/source replaced. Адрес сохраняется; ошибочный probe не становится «проверенным preview».
- Длинный HTTP и WS закрываются при revoke/quarantine/source policy change; runtime reconnect не возобновляет прежний запрещённый admission. Bounded stream/byte/timeout limits сохраняются.
- Чистый посетитель открывает именованный public/unlisted app без аккаунта владельца; private требует собственного допуска. В trusted карточке сохраняет, возвращается с другого устройства, открывает отдельное app discussion.
- Закрытая история community и прежний private app conversation никогда не появляется после public publication; новая публичная аудитория получает отдельный conversation по согласованному правилу.
- 320/768/desktop, keyboard/focus/Escape, reduced motion, copy/share feedback, long slug/Unicode title, lost network after submit, account switch during late callback. Действие подтверждается server ACK, а не локальной сменой статуса.
- Quota/report/quarantine tested без token/body в логах, без удаления имени и без отключения всех частных приложений. Выключение public admission сохраняет уже разрешённые private flows.

## 9. Внешние gates и неизвестные

1. **Реальная зона ещё не подтверждена этим аудитом.** Не проверялись живые DNS, registrar custody, сертификаты, edge deployment и registrable-site разделение фактических origins. Source constants не доказывают production-конфигурацию. До этого gate допустима разработка/локальная приёмка, но не заявление «P3 опубликован».
2. **3–5 реальных проектов пока не пройдены.** Нужно выбрать настоящие приложения владельца и записать их auth/storage/media/network/WS/redirect особенности, исходный процесс и browser acceptance. Их наличие на диске не является доказательством совместимости.
3. **Семантика чувствительных действий принадлежит авторскому backend.** Relay не может автоматически определить, что открытый endpoint является административным, платным или опасным. Публичный author flow требует понятной проверки и explicit opt-in; профиль не выдаётся за sandbox авторского устройства.
4. **Именной origin и browser state.** Для уже используемых apps сохранять legacy address до отдельного решения о переносе origin; не обещать автоматический перенос чужих local storage/cookies. Это не блокирует сохранение платформенного App ID/чатов/grants.
5. **Одноузловые ограничения видимы.** Current sessions/tickets/streams находятся в памяти; restart требует нового launch, а горизонтальный запуск требует согласованной маршрутизации/состояния. P3 пилот объявляет реальные лимиты; сам по себе реестр имён не создаёт глобальную масштабируемость.

## 10. Первичные источники → решение → предел

Источники прочитаны при preflight 30.09.2026. Нормативное поведение и поддержка конкретного браузера — разные проверки; браузерная приёмка остаётся обязательной.

- **R1 — [WHATWG HTML: sites](https://html.spec.whatwg.org/multipage/browsers.html#sites).** Scheme и registrable domain определяют site; origin учитывает host/port. Отсюда отдельная проверка site, а не только неравенство origin. Стандарт не подтверждает конфигурацию Сот.
- **R2 — [MDN: Set-Cookie](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Set-Cookie).** Domain расширяет область cookie на поддомены; host-only и `__Host-` требуют точной настройки. HttpOnly не отменяет автоматическую отправку браузером. Отсюда запрет parent-domain cookie шлюза и отдельная CSRF-проверка.
- **R3 — [MDN: CHIPS / partitioned cookies](https://developer.mozilla.org/en-US/docs/Web/Privacy/Guides/Third-party_cookies/Partitioned_cookies).** Partition связан с top-level site; iframe и отдельное окно могут иметь разное cookie-состояние. Нельзя выводить универсальную поддержку старых браузеров или считать partitioned cookie доступом к бизнес-функциям app.
- **R4 — [OWASP: CSRF Prevention](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html).** SameSite и Fetch Metadata дополняют серверную проверку; sibling subdomains нельзя автоматически считать доверенными. Для cookie-auth effects нужны точные origin/token controls. Это не заменяет author-side контроль последствий в самом приложении.
- **R5 — [Public Suffix List: learn](https://publicsuffix.org/learn/).** Список задаёт границы registrable domains/cookie inheritance. Использовать поддерживаемый PSL parser; не считать собственную зону уже публичным suffix и не сводить домены к последним двум labels.
- **R6 — [OWASP: Subdomain Takeover Prevention](https://cheatsheetseries.owasp.org/cheatsheets/Subdomain_Takeover_Prevention_Cheat_Sheet.html).** Dangling DNS после удаления/освобождения внешнего ресурса создаёт отдельный takeover путь. Нужны inventory и порядок decommission с учётом TTL; tombstone внутри Apps DB не исправляет чужую DNS-запись.
- **R7 — [GitHub Pages: verifying a custom domain](https://docs.github.com/en/pages/configuring-a-custom-domain-for-your-github-pages-site/verifying-your-custom-domain-for-github-pages).** Практический первичный пример долговечной проверки domain ownership для предотвращения чужой привязки. Конкретные гарантии GitHub нельзя автоматически переносить на wildcard edge Сот.
- **R8 — [Caddy: on-demand TLS](https://caddyserver.com/docs/caddyfile/options#on-demand-tls).** В production нужен admission от злоупотреблений; ask разрешает сертификат только ответом 2xx и должен быстро проверять разрешение. Это сертификатный gate, а не право посетителя открыть приложение.
- **R9 — [MDN: Origin-Agent-Cluster](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Origin-Agent-Cluster).** Header просит origin-keyed allocation, но не гарантирует dedicated process. Не использовать его как доказательство полной межприложенческой изоляции.

## Приёмочный итог подготовки

Baseline relay/edge — 14/14 тестов PASS; P3-код не реализован. Подготовлен один минимальный путь без новой identity и без произвольного reverse proxy: долговечное имя → явная текущая политика → зарегистрированный target → проверяемый запуск → trusted сохранение/разговор. Полный публичный P3 требует закрыть перечисленные domain, browser, app-compatibility и abuse gates; они не подменяются визуальной готовностью карточки.
