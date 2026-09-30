# Реализация Сот для людей и любых ИИ

Начало: 30 сентября 2026. Основание: [утверждённый план](../plans/soty-human-agent-platform-20260930.md).

Рабочая ветка `codex/human-agent-platform-release`, база `b0af84b`; актуальная независимая копия — `C:\Users\Junio\.codex\worktrees\soty-platform\соты`. До P2 работа велась в прикреплённой `soty-experience-release`, где сохранены локальные browser proofs. Переход сохранил все commits, новую копию снабдили независимыми dependencies; исходные изменения и общая ссылка node_modules в `D:\соты` не изменялись. Максимум три субагента одновременно. Один этап: подплан → реализация → тесты → независимое review → исправления → проверка затронутых пользовательских сценариев → checkpoint. Следующий этап не закрывает недоделки предыдущего молча.

| Этап | Состояние | Проверяемый результат |
|---|---|---|
| P0. Исходная база и приёмка | Локальная база принята | Воспроизводимый стенд, inventory, исходные тесты, карта рисков и зависимостей; внешние gates перечислены ниже |
| P1. Права, контракты, история | Локальная приёмка пройдена | Ограниченные допуски, отзываемые цепочки, устойчивые Invocation/receipt, общий лимит вызовов; runtime enforcement отдельно в P5 |
| P2. Цельный интерфейс | Локальная реализация и финальная приёмка | Apps-first оболочка, графит, новые панели, доступы, Notes offline; внешний UX/установленная PWA остаются отдельными gates |
| P3. Именные приложения | A/B/C1 локально приняты; C2 следующий | Реестр имён, Apps3 reader, публикация, непрерывный HTTP/WS допуск, подтверждённый вход и owner settings; смена источника и публичный пилот ещё впереди |
| P4. Внешние ИИ | Ожидает P1/P2 | HTTP/MCP, служебная личность, Notes и проверенная авторизация |
| P5. Устройства и исполнитель | Ожидает P1 | Реальные ограничения, остановка, отзыв, стоимость |
| P6A. Авторский SDK | Ожидает P1/P4 | Независимая узкая функция без изменения ядра |
| P6B. Длительные функции | Ожидает P6A | Start/status/cancel/reconnect/result без повторного эффекта |
| P6C. Постоянное размещение | Ожидает P3 | Проверенный статический artifact и rollback |
| P7. Обнаружение и сообщества | Ожидает полезных циклов | Каталог, поиск, публичные страницы, связи, модерация, экспорт |
| P8. Расширения | По подтверждённым условиям плана | Изоляция серверных runtime, композиция, дополнительные adapters, media и масштабирование |
| Общая приёмка | После реализации | Все требования пользователя, UI-аудит, приватность, восстановление, эксплуатация |

## P0 — подплан и доказательства

- [x] Проверить Git и повторно использовать свободную прикреплённую release-копию; создать отдельную ветку.
- [x] Зафиксировать согласованные документы и сопоставить текущий исходный код.
- [x] Поднять отдельный локальный стенд: UI `127.0.0.1:5300`, API `127.0.0.1:5301`, данные `var/human-agent-20260930/data`; production-настройки не наследуются.
- [x] Исходные world/apps/notes/platform tests: 113 pass, 0 fail, 2 opt-in skip; Connect: 64 pass, 0 fail.
- [x] Typecheck и production build проходят. Node 24.13.1, pnpm 10.30.0. Сборка не изменила отслеживаемые исходники.
- [x] Браузер загрузил реальный пустой аккаунт и главную; исходный снимок `output/implementation-20260930/p0-home.png` (1440×828).
- [x] Свести три независимые карты приёмки: [продукт/UI](p0-product-acceptance.md), [внешние ИИ/identity](p0-agent-contracts.md), [jobs/runtime/publishing/backup](p0-platform-baseline.md).
- [x] Зафиксировать контракты и зоны файлов P1; провести аудит исходной базы перед первым изменением.

Логи исходных проверок: `output/implementation-20260930/p0-{world,connect,types,build}.log`. Они не заменяют проверки новых изменений. Производственный сервер пока не изменялся.

Дополнительный аудит исполнения: connector-store и persistence (13 fault cases), queued migration (2), durable protocol (7), readiness, inference resilience (29), race-keys (10) прошли на изолированных данных. Backup/verify/edge: 9 pass, 1 opt-in Linux/Docker skip. Полное восстановление production этим не доказано.

Кандидаты для проверки реальных приложений: `D:\созидание` (Tavysh, Vite/media), `D:\etap` (HIVE, vinext/React), `D:\отзывы` (Reviewdock, Express/React/Prisma), `D:\kvartalufa` (vinext/React). Проверены наличие и безопасные поля package.json; приложения не запускались и совместимость размещения не заявляется. Независимый автор, реальные внешние MCP-клиенты, DNS/TLS, production restore и изоляция стороннего исполнения остаются отдельными обязательными gates соответствующих этапов.

## P1 — подплан

- [x] Одна SQLite-база: служебные личности, клиенты, допуски и цепочки делегирования, удостоверения, общий лимит.
- [x] Неизменяемые версионированные контракты и проверка входа; выключенные обработчики нельзя запустить.
- [x] Атомарный admission и резерв лимита; долговечные Invocation/dispatch intent/receipt; повтор после сбоя без новой заявки на эффект.
- [x] Подключить управление к настоящему подписанному Connect; проверить два аккаунта, подмену и отзыв.
- [x] Убрать переход неизвестного типа задания к произвольному agent executor; отдельные проверки строгого выбора исполнителя проходят.
- [x] Независимый аудит допусков, гонок, границ транзакции, редактирования/отмены и безопасного статуса.
- [x] Интеграционные тесты, повторная сборка, regression затронутых модулей, checkpoint.

Зоны файлов не пересекаются: agent_ecosystem — access/catalog/service/schema; publishing_architecture — invocations и его проверки; whole_product_critic — независимые acceptance tests; основной интегратор — HTTP composition, настоящий signed HTTP test и совместимость connector runtime.

Приёмка P1: `capabilities:test` — 51/51 (в том числе 22 независимых acceptance tests, два реальных подписанных HTTP-сценария, конкурентный доступ двух SQLite writers); world regression — 115 pass, 2 прежних opt-in skip; typecheck/build проходят. Connector 1.3.1 собран, release/update selftests и 7 durable protocol scenarios проходят. Найденные в независимом review дефекты snapshot audience/digest, settlement, scope pagination и потери известных effects исправлены до checkpoint. Логи: `output/implementation-20260930/p1-*.log`.

Ограничения P1 зафиксированы в [README модуля](../../modules/capabilities/README.md): бесплатный счётчик вызовов не является денежным бюджетом; disabled Notes нельзя запустить. Native effect gate, OAuth/MCP/внешний HTTP относятся к P4; lease, fencing и OS isolation — P5. `beginDispatch` не даёт процессу бессрочное право действовать. Внешние endpoints и production в P1 не включались.

## P2 — подплан и владение

- [x] P2-A/B/C: реализовать маршруты, новый каркас, нейтральные палитры, настоящие карточки приложений, поиск и состояния — whole_product_critic; `src/world` (кроме access-panel).
- [x] P2-D/E/H: перенести записки/сообщества/разговоры и геометрию поля; сохранить domain state; закрыть clean offline-Notes defect отдельным ограниченным cache.
- [x] P2-F: реализовать постоянного помощника, устойчивую отправку создания приложения и экран «Доступы и действия»; проверить account/device binding и отсутствие ложного подтверждения остановки.
- [x] P2-G: реализовать ограниченный буфер передачи, устойчивый ACK, pending inventory/discard, сохранение отправленного оригинала; отдельные transport/SQLite/browser-helper proofs.
- [x] P2-I, доступная локальная часть: PWA lifecycle/update guards, Back, screenshots 320/390/768/1024/1440, keyboard dialogs/chess, reduced motion, темы, offline Notes и повторная приёмка.
- [ ] P2-I, остальная матрица: 200%/landscape и все негативные UI-пути, физический телефон/клавиатура, установленная PWA и новый участник; точные статусы — в независимой матрице, не считаются пройденными по desktop screenshot.

Изменённый экран сначала проходит собственную сценарную проверку, затем проверку интегратора/другого исполнителя; ошибки сохранности и смешения аккаунтов останавливают дальнейший перенос. Переход к P3/P4 не закрывает незавершённый UI молча.

### P2 — промежуточная приёмка, этап ещё не закрыт

**Актуальный локальный срез:** общий `world:test` — 210 тестов, **207 pass / 0 fail / 3 opt-in skip**; typecheck и production build PASS. Тестовые HTML не входят в dist. P1 capabilities — 51/51 до добавления ещё одного реального подписанного Apps HTTP test; новый HTTP test вместе с двумя прежними и девятью Apps tests прошёл 12/12. Ниже сохранены промежуточные доказательства; последнее состояние дополняют [Notes offline](p2-notes-offline.md), [устойчивая отправка создания приложения](p2-app-create.md) и [независимая матрица](p2-independent-gate.md). Публичный rollout не выполнен.

Финальный перекрёстный аудит дополнительно закрыл: неоднозначный старый device key после исчезновения устройства; невидимую ошибку истории на мобильном; обрезанную кнопку «Проверить отправку»; потерю последнего ввода при delayed identity/quota; снятие guards при смене аккаунта; несовместимость нового `expectedAccountId` с прежним Apps registry. Последнее проверено настоящим подписанным HTTP и реальным Apps service, а не только synthetic UI. Прежние signed клиенты остаются совместимыми; новый account guard проверяется до чтения/изменения и не ослабляет strict operation fields.

На настоящей ширине 320 px повторены pending/error Assistant и сохранение черновика через обновление PWA. `p2-assistant-pending-320.png` получен в исправленном fixture с тем же `.sw-app` style context; текст read-only остаётся светлым и доступным, кнопка помещается в панель. Notes: сохранённая онлайн записка → остановка сервера → reload → видимая локальная копия → новая правка → reload → восстановление сервера → ACK → повторная загрузка с новым текстом. Отдельный IndexedDB fixture — 11 checks; доменные/cache/CAS проверки — 35/35.

Перед применением SQLite rooms v2 к production выявлен несовместимый прежний rollback reader. [Read-only release preflight](release-preflight-20260930.md) фиксирует текущую production-базу, три старых malformed room JSON и незавершённый restore gate. Исполняемая проверка storage/image compatibility до candidate/rollback/recovery start реализуется отдельным подэтапом и проходит свой аудит. Данные сервера при preflight не изменялись.

Новая оболочка использует нейтральный графит и мягкие тёплые акценты, 80px rail/56px header, карточки настоящих приложений и альтернативное поле с устойчивыми позициями. Записки доступны сразу; демонстрационные приложения и люди не добавлены. На телефоне три главных раздела и отдельное добавление. Старый `src/style.css` больше не импортируется: комнаты, файлы, шахматы и инструменты получили отдельный новый `rooms.css` при сохранении контроллеров и данных.

Постоянный помощник подключён к подписанным операциям аккаунта: история, продолжение собственной завершённой задачи, результат, остановка и создание приложения. Черновик привязан к разговору и выбранному устройству; текущий разговор каждой вкладки сохраняется отдельно. Неизвестный исход отправки повторяет постоянный request ID. Сервер ищет уже принятую задачу до изменяемой доступности модели и устойчиво записывает отказы; после подтверждённого отказа та же отправка не исполнится позднее. Параллельные продолжения одного разговора блокируются. OpenCode session ID остаётся внутри сервера. Это прежний доверенный исполнитель владельца с честным указанием его прав, не изолированный внешний runtime P5.

Независимый аудит выявил и закрыл перенос текста между разговорами, незаметную замену отсутствующего устройства, неверный target при восстановлении pending, устаревший target пункта истории, общий active conversation двух вкладок, автоматический новый разговор после отзыва доступа, поздние ответы после смены аккаунта и дедупликацию после изменения readiness. 23 focused tests проходят: 8 server jobs, 13 state (8 независимых), 2 command file parser. Дополнительно 44 world/theme/geometry/Notes tests проходят. Сборка всего P2 и общий повтор после финального transport review ещё впереди.

Реальный браузер на изолированном стенде подтвердил: новая записка с текстом/чеклистом/закреплением сохраняется при уходе, Back и полной перезагрузке; новый разговор не переносит прежний текст, черновик возвращается из истории. Screenshots: `output/implementation-20260930/p2-{home,assistant,notes}-desktop.png`, `p2-notes-mobile.png`. Независимый visual reviewer посмотрел эти снимки; это пока не вся responsive-матрица.

Отдельный dev-only `src/platform/assistant.test.html` проверен через браузер с явно синтетическим транспортом: lost ACK → remount с переставленными A/B → та же одна заявка на A; недоступная job сохраняет draft и блокирует Send; смена аккаунта убирает содержимое. Это component proof, не запуск реального inference. `access-panel.test.html` отдельно проверяет отзыв, недоступную выдачу, потерю ACK и поздний ответ после dispose; реальные HTTP/access права покрывает P1.

В инструментах введён общий native-dialog lifecycle: background inert, Escape, начальный фокус и возврат к opener. QR Escape/возврат и файлы Escape проверены независимо; реальных разрешений/камеры при проверке не выдавали. Production данные и сервисы не изменялись.

Файловая часть расширена до настоящего устойчивого хранения блоков: см. [P2-G evidence](p2-file-storage.md). Реальный 512000000-byte source прошёл AES-GCM → WebSocket ACK → SQLite → restart → stream-to-disk с совпадающим SHA256; при повторном большом прогоне sampled peak Node RSS 133197824 bytes. Отдельный реальный desktop Chrome OPFS helper восстановил и проверил каждый байт 512 MB. Это не полный browser/network E2E и не измерение памяти мобильного браузера. Независимые 36 focused cases проходят, включая reload четырёх partial, отказ пятому, удаление после ACK и освобождение слота. Ошибка приёма показывается сразу, потерянный ACK не выдаётся за отсутствие эффекта, успешное удаление заменяет старое сообщение ошибки. Автоматическое чтение исходного File после закрытия вкладки не обещается.

Общий промежуточный прогон: **171 pass, 0 fail, 3 opt-in skip** (174 tests), TypeScript и production build проходят. Старое ожидание публичного OpenCode session ID заменено проверкой его отсутствия в DTO и сохранения ограниченного внутреннего значения; независимый reviewer подтвердил контракт и повторил 10 server cases. Убрана старая галочка отправки сообщения, которая не была связана с delivery ACK.

Настоящая production-сборка на отдельном `127.0.0.1:5360` подтвердила offline-ready оболочку, загрузку после остановки сервера и сохранность созданного без связи черновика после reload. Найден локальный blocker: уже подтверждённая сервером записка не имела локального snapshot и исчезала из редактора offline. В работе bounded account-scoped cache недавних записок, отдельно от outbox. Дополнительный аудит нашёл второй blocker: старый modal создания приложения держал request ID только в памяти до ACK; в работе устойчивый create draft/admission. Финальный общий прогон и checkpoint выполняются после обоих исправлений. Полная [матрица независимой проверки](p2-independent-gate.md) сохраняет внешние и непроверенные условия отдельно.

### P2 — контрольные точки

`a307f98` сохраняет проверенную локальную реализацию оболочки/помощника/Notes/файлов. `afd10f1` устраняет ложную ошибку при немедленном закрытии формы создания приложения: browser fixture, обычная форма production-preview 320 px, сохранение точного текста после reopen, штатное обновление PWA, 14 state tests и build прошли. Полная внешняя UI-матрица выше остаётся открытой.

`fb4c81d` добавляет проверку совместимости формата комнат до запуска, автоматического перезапуска и восстановления сборки. Интегратор и независимый reviewer получили **108/108 PASS**, без skips. Поддерживаемый профиль — управляемый single writer и стандартный Docker local volume без driver options; probe только read-only, секреты приложения в него не передаются. Это проверка rooms-reader, не Apps-schema, не integrity/import и не восстановление backup. Первый совместимый fallback image и Linux Docker/WAL canary требуют отдельного подтверждения. [Release preflight](release-preflight-20260930.md) фиксирует безопасно прочитанные факты production; rollout не выполнялся.

## P3 — последовательные подпланы

Основание: [P3 preflight](p3-implementation-preflight.md), checkpoint `9bd1712`. Общий App ID, владельцы, grants, прежний canonical origin и браузерные данные сохраняются. Новое имя сначала становится отдельным alias; оно не переключает приложение на другой origin автоматически.

- [x] P3-A1: явная v1→v2 migration и неизменяемый origin baseline; atomic claims/receipts, CAS, квоты, reserved names/tombstones; реальные конкурентные writers/reopen/lost ACK. Независимый общий срез — **48/48 PASS**, после последнего уточнения schema recognizer — ещё **22/22 PASS**.
- [x] P3-A2: единый classifier точного Host, закрытие unknown/nested/malformed app-host до shell/API/channel; TLS eligibility отдельно от runtime ACL. Независимый срез — **59/59 PASS**; настоящий HTTP/WS процесс проверен с traffic upstream и сохранёнными, но отключёнными для новых claims зонами.
- [x] P3-A3, локальная реализация: независимый аудит и deploy regression; Apps-reader gate и отказ несовместимого rollback/recovery. Историческая частичная сборка закрыта после найденного независимым reviewer дефекта.
- [ ] P3-A3, выпуск: проверенный новый host guard, точные совместимые candidate/fallback, Apps Linux proof, свежая защищённая копия и isolated restore; до production migration остановлен прежний единственный writer. Публичная зона по умолчанию отсутствует.
- [ ] P3-B: явная аудитория, exact-host/epoch-bound запуск и сессии, отзыв HTTP/WS, origin/CSRF guards; private/granted/unlisted/public matrix.
- [ ] P3-C: привязанный RuntimeTarget и свежая проверка реального проекта, bounded projections, UI имени/аудитории/статуса; browser walkthrough и аудит прежнего UI.
- [ ] P3-D: сохранение приложений и самостоятельное обсуждение, права на историю, возврат с другого устройства.
- [ ] P3-E: жалобы/quarantine/лимиты, принятая зона/DNS/TLS, 3–5 настоящих проектов и чистый посетитель; только затем публичный пилот.

Текущие непересекающиеся области: agent_ecosystem — Apps registry/service и доменные tests; root — server ingress/config/PSL и интеграция; whole_product_critic — независимый аудит/acceptance tests; publishing_architecture — отдельное испытание совместимого обновления. Последние две роли не меняют код автора незаметно.

Named zone проверяется до открытия данных по PSL (`tldts@7.4.16`, включая private suffixes), всем trusted shell origins и отсутствию overlap с legacy namespace. Localhost исключение ограничено полностью локальным стендом. Это конфигурационная проверка, не доказательство владения DNS/TLS и не публичный выпуск. [Public Suffix List](https://publicsuffix.org/learn/) и [tldts](https://github.com/remusao/tldts) объясняют используемую границу доменов; автоматическая гарантия совместимости всех браузеров не заявляется.

Аудит A1 исправил конкретные случаи: legacy template с префиксом/вложенной меткой скрывал overlap; известная схема могла пройти без critical UNIQUE/FK/NOT NULL; отключённые новые claims обходили проверку сохранённой зоны при изменении trusted aliases. Теперь общий parser определяет зону, известный DDL проверяется до записи, persisted named zones повторно проверяются перед migration/WAL и внутри транзакции. Root signed HTTP acceptance прошла register → claim → retry → отказ другого аккаунта → retire, с сохранением canonical origin; paired device в этом тесте синтетический и никакое приложение публично не запускалось. Следующий gate — A2 HTTP/WS/TLS: доступное имя само по себе не открывает runtime.

`frame-src` legacy iframe также строится из общей зоны: простая замена `{appId}` на `*` давала недопустимые источники CSP для `prefix-{appId}` и `fixed.{appId}`. Новый источник использует допустимый wildcard поддоменов ([MDN CSP](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Security-Policy#host-source)). Named iframe/runtime этим изменением не включён.

Контрольная точка A1/A2: общий `world:test` — **252 pass / 0 fail / 3 opt-in skip** (255 tests до добавления двух независимых host tests), typecheck и production build PASS. Новый независимый host gate покрывает 19 authority-вариантов, forwarded Host, absolute-form WS target, private canonical и отдельный TLS admission; вместе с A1/config/signed API — 59/59. Именованные aliases пока возвращают безопасный статус; гостевой runtime ещё не включён. Новая зона по умолчанию выключена. Истинный TLS/DNS и совместимый Apps rollback reader ещё не приняты.

При следующем ручном обходе P2 найден и исправлен локальный UI-долг: standalone шахматы 1024×768 сохраняли окружающие room tools и обрезали доску; независимый reviewer также обнаружил сжатие доски до20.84px при входе из комнаты на телефоне. Единый игровой workspace принят после настоящих desktop/mobile переходов, сохранения позиции/черновика/ответа, keyboard проверки, typecheck и build. Подробности и границы — [шахматы](p2-chess-workspace.md). Запрошенное увеличение200% в IAB не изменило фактический viewport/DPR; этот сценарий остаётся непроверенным, а не PASS.

Контрольная точка UI — `89ada71`. A3 добавляет независимое распознавание Apps v1/v2 рядом с Rooms, строгие manifest/probe/START receipt v2 и отказ старым incomplete receipts. Авторский полный deploy suite — **150 pass / 0 fail / 2 explicit skip** (152 tests); reviewer независимо проверил прежний полный151test slice и исправленный focused34test slice (33pass/1skip). [Квитанция A3](p3-apps-reader.md) фиксирует границу внешних проверок. Найденный blocker: backend overlay наследовал прежние modules/deps, но объявлял нового Apps reader. Этот неподдерживаемый build path теперь отказывает до COPY/LABEL и указывает на единый полный Dockerfile.

Дополнительный UI-аудит получил фактические landscape844×390 и667×375. Для шахмат устранён пустой80px резерв скрытой rail и обрезание истории; доска252×252/237×237 и чат прошли отдельный browser/независимый визуальный gate. Другие landscape-разделы, физический телефон и200% не объявляются завершёнными.

Root повторил окончательный полный deploy suite: **152tests / 150pass / 0fail / 2skip**, exit0. A3 local gate принят; следующее изменение Apps schema не может полагаться на этот v2-only reader без отдельного обновления.

Отдельный [Linux rooms canary](storage-linux-canary-result-20260930.md) прошёл на synthetic volume: RO WAL/SHM, mainheader0/SQLite2, unknown/corrupt отказы, copied container label не разрешает старый image. Cleanup всех8containers/1volume и неизменность serving baseline подтверждены отдельно. Это проверка закреплённого старого rooms probe, не нового Apps probe, backup restore или rollout.

Следующий [контракт B](p3-publication-contract.md) проверен с позиции доменной модели, посетителя, непрерывных прав и доступности личных приложений. Приняты separate active aliases, explicit whole-port acknowledgement, immutable initial target, monotonic epoch и bounded64 receipts/app без автоматического повторного согласия. До принятой B1 модели aliases остаются status-only; каталог и запуск не смешиваются.

Дополнительный [Notes layout gate](p2-notes-layout.md) устранил смещение корневой оболочки при фокусе, перекрытие текста sticky toolbar и обрезание меню на низком экране. Реальные844×390,667×375,320×760, сохранение/reload, архив/восстановление и keyboard возврат пройдены; Notes35/35, typecheck/build PASS. Физический телефон и оставшаяся negative/recovery матрица этим результатом не заменены.

## Правила качества для каждого следующего этапа

Контрольная точка B1: [модель](p3-publication-model.md) и [независимая приёмка](p3-publication-independent.md) приняты: авторские46/46, независимые36/36, root общий world **302tests / 299pass / 0fail / 3opt-in skip**, typecheck и production build PASS. Общий прогон выявил один latest-schema fixture без обязательной публикации; исправлена только его инициализация, отказ на действительно неполной базе сохранён. Основание допуска public не повышается при получении membership, grant не понижается скрыто после его потери. Исторический receipt не выдаётся за текущее состояние. Логи `p3-b1-{world,types,build}-root.log`.

[Apps3 reader](p3-apps-v3-reader.md) принят одновременно со схемой3: root полный deploy suite **157tests / 155pass / 0fail / 2explicit skip**; независимый focused gate **130pass / 1Windows symlink skip**. Старый Apps2-only fallback блокируется до START. Полный Linux candidate/fallback, свежая зашифрованная копия и настоящее изолированное восстановление остаются отдельными gates. HTTP/WS named runtime в B1 не включён; [последовательность B2/B3](p3-runtime-transport-plan.md) определяет следующий отдельный этап.

Контрольная точка [B2 transport](p3-runtime-transport.md): actual connector/sample3/3, независимые30/30 и root общий **335tests / 332pass / 0fail / 3opt-in skip**, typecheck/build PASS. Закрыт обнаруженный reviewer keep-alive defect при отзыве upload; короткая и большая отправки, следующий обычный запрос и соседний WS проверены. Именные active anyone aliases работают без аккаунта; canonical сохраняет grants; ошибочная cookie не становится guest; public24/total32 и реальные30s timeout освобождения подтверждены. B3 должен подтвердить именно новую cookie в браузере, точный private deep link, account-switch и recovery без циклов. Ни successful POST session, ни iframe.load не считаются доказательством открытого приложения.

[B3 browser entry](p3-entry-browser.md) локально принят: полный root gate **380tests / 377pass / 0fail / 3explicit skip**, typecheck/build PASS; independent network40/40, renderer16/16, launch19/19. Реальный canonical/named iframe и отдельная вкладка, exact query, HTTP/WS, явное public recovery, offline/retry и клавиатурный фокус пройдены. Два визуальных дефекта устранены отдельными повторными опытами: landscape toolbar56px и компактная ошибка в iframe667×199. Общие hex/palette перенесены в `modules/ui`, прежние frontend imports реэкспортируют ту же реализацию. Настоящий identity-switch, принудительно запрещённые cookie, физические устройства и внешние release gates не объявляются завершёнными. Следующий отдельный этап C1 — owner settings существующего источника: адрес, аудитория, честное наблюдение и постоянная ссылка; источник не переключается до versioned C2 runtime.

Контрольная точка [C1 owner settings](p3-owner-settings-integration.md) локально принята: настоящий read-model владельца, адреса и независимая аудитория, CAS без потери черновика, постоянные ссылки, сохранённый pending и честное истечение наблюдения. Root world **447 tests / 444 pass / 0 fail / 3 explicit skip**, Connect **70/70**, typecheck/build PASS. Независимые повторные client/service пробы **17/17** закрыли смешение A → B → A и блокировку запуска медленными метаданными. Реальные два окна, Clipboard, API/connector reconnect, Back/Escape/Tab и desktop/320/667×375 описаны в [browser receipt](p3-owner-settings-browser.md). Физические устройства, browser identity-switch и внешние release gates этим не заменены. Следующий отдельный C2: проверка нового источника, неизменяемая версия, атомарное переключение, свежий versioned connector ACK и совместимый reader/rollback.

C1 checkpoint — `914a91a`, отправлен в `origin/codex/human-agent-platform-release`. Production этим push не менялся. [C2 контракт](p3-source-contract.md) разбит на данные/reader → runtime → UI → общий аудит. На промежуточном C2-A сервер с legacy-протоколом отказывает постоянному binding floor2, source API ещё не выставлен; собственная схема и рабочий v2 runtime не объявляются одним и тем же свойством.

Контрольная точка C2-A локально принята: [модель источников](p3-source-model.md), [независимые12 сценариев](p3-source-model-independent.md) и [Apps4 reader](p3-apps-v4-reader.md). Root общий world — **483 tests / 480 pass / 0 fail / 3 explicit skip**; отдельный reader gate — **116 tests / 115 pass / 0 fail / 1 Windows symlink skip**; typecheck, connector release build и production frontend build PASS. Независимый combined model/floor gate — **36/36**, авторский deploy suite — **164 tests / 162 pass / 0 fail / 2 prior skips**. Логи `p3-c2a-{world,reader,build}-root.log`. Историческая Apps3 мигрируется строго; immutable target/floor2/epoch/receipt сохраняются атомарно, а потерянный после COMMIT ответ восстанавливается после полного reopen. Два настоящих SQLite writers не занимают один порт разными приложениями. Отказ старого Apps3 migrator проверен с WAL и побайтной неизменностью файлов. Runtime доказательство в model tests пока синтетическое; настоящий v2 и signed async prepare — следующий C2-B. Linux candidate/fallback, isolated backup restore и production всё ещё отдельные незакрытые gates.

Контрольная точка [C2-B runtime](p3-source-runtime-integration.md) локально принята: точная связь с версией источника на конкретном соединении, настоящий подписанный async prepare и immutable connector1.4.0. Root world **539tests /535pass /0fail /4skip**, Connect70/70, protocol45/45, typecheck/build и release/update selftests PASS. Bundle opt-in пропуск общего набора отдельно выполнен **1/1** настоящим child process; independent composed source **9/9**. Найденная потеря авторизованной session при временном legacy reconnect исправлена и закреплена red/green и постоянной регрессией. Полный прогон также выявил старые C1 protocol expectations и cleanup race SQLiteworkers; исправлены причины, повтор принят. Production не менялся; следующий отдельный C2-C — интерфейс выбора/проверки/переключения источника и возврата с сохранением черновиков и pending.

Контрольная точка [C2-C интерфейс источника](p3-source-settings.md) локально принята. Root world **598tests /594pass /0fail /4explicit skip**, составной state/service/client gate **91/91**, независимый gate **49/49**, typecheck и production build PASS. Реальный браузер: public A→B→A с возвратом контрольной записи, explicit recheck после30s, недоступный порт, отдельная отмена доступа, конфликт двух окон и сохранность несвязанных черновиков,320×760/667×375. Dev-only компонент **10/10 PASS** отдельно от сети. Исправлены найденные гонки подмены retry и отмены загрузки истории, stale readback и неверная подсказка истории. Источник и owner device name читаются без пересоздания iframe; deviceName скрыт от granted viewer. [Журнал](p3-source-settings-browser.md) содержит реальные результаты и границы. Малый C1-долг текста после принятой публикации остаётся обозначен для общего UI-прохода. Следующий C2-D — совместный аудит модели, runtime и клиента, затем P3-D сохранение/обсуждение. Production не менялся.

1. Права, данные и фактический результат важнее формального наличия кнопки/API.
2. Новая оболочка не импортирует classic CSS/DOM. Сохранение старых данных не означает сохранение старого оформления.
3. Человек видит короткое действие, состояние, результат и способ исправления; ИИ получает точный контракт и ограничения.
4. Никаких вымышленных пользователей, успехов, отзывов, доступности или обещаний совместимости.
5. Дизайн проверяется на нескольких ширинах, с клавиатурой, длинным содержимым и reduced motion. Пустые состояния тоже являются полноценным сценарием.
6. Подключение внешних клиентов, расход, публикация и управление устройством имеют отдельные проверяемые границы; неизвестное не называется готовым.
7. Выпуск включает совместимый откат и проверку данных. При непроверенной внешней предпосылке сохраняется честный статус, а независимая реализация продолжается.
