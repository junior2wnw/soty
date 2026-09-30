# P2 — независимая проверка готовности

Дата: 30 сентября 2026. Срез: 01:36 UTC / 06:36 Asia/Yekaterinburg. Проверяющий: whole_product_critic. Рабочая копия: `soty-experience-release/соты`, ветка `codex/human-agent-platform-20260930`; P2 ещё находится в рабочем дереве после checkpoint P1 `36116cb`.

Основание: [P0, P2-A… I и V01… V20](p0-product-acceptance.md), [мастер-план](../plans/soty-human-agent-platform-20260930.md), [Assistant](p2-assistant.md), [создание приложения](p2-app-create.md), [офлайн-записки](p2-notes-offline.md), [Access и комнаты](p2-access-rooms.md), [файлы](p2-file-storage.md), [журнал этапов](PROGRESS.md).

## Решение и смысл статусов

**Общая приёмка P2 — partial; ограниченный локальный source/component gate — pass.** Offline clean-Notes, потеря pending при повторном открытии создания приложения, account lifecycle guards и strict apps API integration исправлены и повторно проверены. В области этого независимого review воспроизведённых блокеров больше нет. Локальный checkpoint допустим после окончательного общего прогона интегратора. Это не утверждение, что выполнена вся матрица V01… V20, проверена установленная PWA или состоялся production rollout. Внешние и непроведённые сценарии сохраняют свои статусы и не превращаются в `pass` вместе с checkpoint.

- **pass** — пройдена именно названная ограниченная проверка, с указанным источником доказательства.
- **partial** — есть выполненная часть, но весь критерий ещё не проверен либо включает зависимый этап.
- **external** — нужен реальный участник, устройство, установленная PWA либо эксплуатационный контур, отсутствующий в текущем локальном испытании.
- **not-tested** — подходящего доказательства нет; это не синоним обнаруженной ошибки.

Не использованы ошибки промежуточного HMR как доказательство дефекта готовой сборки. Старые снимки Assistant до перестановки мобильного ввода также не являются основанием текущих замечаний. Тесты с синтетическим API, VM service worker и desktop viewport не называются испытаниями настоящего inference, физического телефона или установленной PWA.

## 1. Непосредственно перепроверенные доказательства

Проверяющий повторно запускал следующие команды на актуальных исходниках P2; для тестов использовались временные данные, production не менялся.

| Проверка | Результат | Что она доказывает и чего не доказывает |
|---|---|---|
| `node --test server/test/realtime-file-durability.test.mjs server/test/room-storage-acceptance.test.mjs src/transport/file-transfer.test.mjs src/transport/file-receive-store.test.mjs src/platform/command-file-transfer.test.mjs` | **pass — 36/36** | Настоящие loopback WS/SQLite, rollback/ACK, reload и удаление partial; реальный клиент/crypto с синтетическим socket; OPFS helper с файловой моделью. Это не полный браузерный E2E файл через сеть и не mobile RSS. |
| `node --test src/platform/assistant-state.test.mjs src/platform/assistant-state.acceptance.test.mjs src/platform/pwa.test.mjs src/world/*.test.mjs src/world/theme/*.test.mjs src/geometry/*.test.mjs modules/notes/test/*.test.mjs` | **pass — 62/62** | Notes ownership/CAS/outbox, 13 Assistant state cases, 5 PWA VM cases, chat drafts, стабильность поля, геометрия, контраст. Это не 62 браузерных пользовательских пути. |
| `pnpm typecheck` | **pass** | TypeScript согласован на этом срезе. Не заменяет production build и runtime review. |
| `node --test server/test/jobs-runtime.test.mjs server/test/apps-jobs.test.mjs` | **pass — 10/10** | Реальный подписанный HTTP, runtime assignment/cancel, output bounds, immutable replay и owner continuation. Изменение старого assertion про публичный session ID отдельно проверено: секрет продолжения теперь отсутствует в public DTO, но остаётся ограничен 200 символами в хранилище и используется только сервером. Это изменение контракта, не отключение теста. |
| Последующий `node --test modules/notes/test/*.test.mjs` | **pass — 35/35** | Независимый повтор после recent-cache исправления: все 18 прежних сценариев и 17 новых cache cases. Проверены exact ACK document, monotonic revision, scoped LRU, auth/404/no fallback, CAS/no resurrection и закрытая обработка ошибки final epoch после remove/clear другого экземпляра. Начальная недоступность cache не блокирует достоверный online read. |
| `node --test src/platform/app-create-state.test.mjs` | **pass — 14/14** | Immutable create intent до dispatch; lost ACK/remount/cross-tab, exact receipt, сохранение новых edits, quota/read failure, синхронный volatile до identity await и защита от обратного порядка сохранений. Реальное исполнение не запускается. |
| `npx tsc -p tsconfig.json --noEmit` после resetAccount | **pass** | Последние create state/lifecycle и Notes типы согласованы. |
| `node --test modules/apps/test/apps.test.mjs server/test/capabilities-connect.test.mjs` | **pass — 12/12** | Независимый реальный service/signed HTTP повтор после integration fix: expected account совпадает с actor до чтения/мутации, чужой account отвергается; лишние поля по-прежнему отвергаются. Legacy клиенты без optional guard сохраняют собственный authenticated scope. |
| Новый partial lifecycle, код `sync.ts` → `features/files.ts` → `main.ts` → SQLite | **pass в проверенной области** | Незавершённые файлы показываются без download; имя расшифровывается локально; удаление ждёт ACK; остановлен тот же producer; captured room и dedupe исключают удаление из другой выбранной комнаты. Подробнее ниже. |
| Коррекция файловых ошибок в `main.ts` | **pass, read-only review** | Timeout не утверждает отсутствие эффекта; quota/занятые slots/остановка различаются; ошибка приёма немедленно показывается для своей комнаты; успешный delete ACK заменяет прежний persistent failure. Удалена старая иконка «sent», зависевшая только от локальной стороны пузыря. Проверка финальных DOM-состояний после этой коррекции ещё нужна. |
| Визуальное сравнение сохранённых PNG | **pass для перечисленных снимков** | Проверяющий посмотрел 1440 home/Assistant/Notes, Notes 390, home dark/light 320, Access 320, свежие home 768/1024 и field 320. Не наблюдал горизонтального обрезания главной на этих снимках; одиночная сота не растянута. Снимки не доказывают интеракцию, phone keyboard и заполненный каталог. |

Пути снимков: `output/implementation-20260930/p2-home-desktop.png`, `p2-assistant-desktop.png`, `p2-notes-desktop.png`, `p2-notes-mobile.png`, `p2-home-320.png`, `p2-home-light-320.png`, `p2-access-320.png`, `p2-home-768.png`, `p2-home-1024.png`, `p2-field-320.png`. Использованы тестовые локальные данные.

Интегратор нашёл production IAB tab с подтверждённым `innerWidth=320` и повторил Assistant/Notes. Проверяющий независимо просмотрел актуальные `p2-assistant-320.png`, `p2-assistant-pending-320.png` и `p2-notes-clean-offline-320.png`. Ввод основной формы доступен, текст offline записки и stale label видны. Первоначально нечитаемый pending prompt оказался ошибкой standalone fixture вне реального `.sw-app` CSS scope: контекст fixture исправлен, новый PNG независимо просмотрен, readonly текст читается, кнопка/статус помещаются. Production CSS ради ошибки fixture не менялся. Прежний ошибочно названный снимок переименован в `p2-assistant-1024.png` и не используется как mobile proof. Собственный IAB reviewer недоступен; чужие открытые вкладки и недокументированные обходы не использовались.

Существующие receipts дополнительно сообщают следующее; это результаты других исполнителей, а не повторённые этим reviewer испытания:

- Интегратор в реальном IAB: создание/редактирование/checklist/pin записки → уход → Back → reload сохраняет текст; новый разговор Assistant пуст, прежний черновик открывается из истории; query/kind поиска людей и сообществ возвращаются через Back; 320 light header имеет доступные имена и не переполняет экран.
- Последующая production preview: clean note online → сервер выключен → reload точного deep link показывает полный текст и время последней проверки → offline edit → reload сохраняет текст → сервер включён → retry сохраняет новую версию. Реальный IndexedDB fixture — 11/11. Обновление PWA поверх Assistant draft сохраняет полный текст. Это desktop browser, не установленное приложение физического телефона.
- Create-app dev fixture после input-staging и последующего resetAccount исправления: lost ACK/remount/reorder/model-off retry/late ACK/account switch/повторное использование callbacks/мгновенная защита quota draft — PASS, реальные эффекты 0. Synthetic fixture не проверяет strict apps service argument schema; найденный интегратором конфликт expectedAccountId отдельно закрыт 12 настоящими service/HTTP tests и независимым review.
- Access reviewer: synthetic component fixture проверяет lost revoke/issuance ACK, late response после dispose, masked one-time token, copy ACK, cleanup, 320 dark/light, Escape и стрелки вкладок. Реальная выдача Notes-доступа пока закрыта до P4.
- Rooms reviewer: загруженный `rooms.css`, отсутствие `src/style.css`, desktop/320/768, локальный шахматный ход с клавиатуры, actual profile/files/terminal/QR/actions dialogs и Escape/focus return. Shell commands, камера, реальные разрешения и удалённые файлы при этих UI-проверках не запускались.
- Инженер файлов приложил [машиночитаемый receipt](evidence/p2-file-storage-20260930.json): повторные реальные 512000000 байт / 2000 блоков, AES-GCM → WS ACK → SQLite → restart → расшифрование на диск, одинаковый SHA256 `d002f5af1c28f9dc2477bc1d4929bcf373d51f792aa7532eea1928709e9a2b96`, sampled peak Node RSS 133197824 байт, 248 секунд. Большой прогон предшествовал последним pending-inventory дополнениям, отдельно покрытым 36 focused cases.
- Тот же receipt фиксирует настоящий desktop Chrome OPFS helper: 512000000 байт, каждый выходной байт проверен через `File.stream`, временный cache удалён. Это local disk-backed helper, а не полный TunnelSync/network/browser E2E. Значения `performance.memory` зависят от GC/общего процесса и не задают hard heap/RSS bound; телефон/iOS не проверялись.
- Интегратор сообщил промежуточные 201 pass / 3 skip (204 tests), capabilities 51 pass, typecheck/build PASS после input-staging исправления. Это срез до последних device-key, Notes fence и apps service argument fixes; окончательный общий повтор ещё требуется. Первый старый assertion про публичный session ID проверен отдельно выше.

`p2-file-storage.md` обновлён до реализованного v2 и согласован со свежими 36 тестами и evidence JSON. Исторические значения RSS/числа тестов в прежних промежуточных отчётах не следует смешивать с этим повтором.

## 2. Подэтапы P2-A… I

| Подэтап | Статус всего критерия P0 | Подтверждено | Что ещё не подтверждено |
|---|---|---|---|
| P2-A. Маршруты и поверхности | **partial** | Реальные `#mine`, Notes, Assistant, Access; явный people/community search; Back q/kind; интерфейсы mount/dispose и account guards. Старый controller-route инструментов использует новый shell/CSS. | Полный браузерный обход direct link, logout/account switch и старых invitation URLs. Сохранённое имя `view=classic` само по себе не означает старое оформление; проверяется загруженный UI. |
| P2-B. Система и каркас | **partial** | 80/56 desktop, три mobile destinations, neutral graphite/warm light, Manrope с лицензией, доступные имена и native dialogs; 320/390/768/1024/1440 evidence; численный контраст всех шагов светлоты. | 200% текста/масштаба, landscape, полная Tab-последовательность и intermediate theme states в реальном DOM всех экранов. |
| P2-C. Карточки и поиск | **partial** | Только реальные источники; Notes видны; pin/recent по аккаунту; найденный человек/сообщество открывается явным действием; query переносится и возвращается. Нет вымышленных проектов ради заполнения макета. | Заполненный несколькими настоящими приложениями каталог, раздельные card/menu/save/chat действия, единственный launch, доступность и возврат после запуска runtime. Новые домены/обсуждения — P3. |
| P2-D. Записки | **partial** | 35 доменных/HTTP/session/cache тестов; настоящий bounded clean cache отдельно от outbox, account/project/origin scopes, stale label, CAS; browser clean offline→reload→edit→reload→online save закрывает найденный дефект. | Полный browser quota/conflict в двух вкладках, архив/корзина/восстановление/export и update поверх dirty Notes редактора. |
| P2-E. Люди и разговоры | **partial** | Discovery и community surfaces перенесены; настоящий community контекст подписан; личный Assistant отделён; chat drafts и catch-up тестируются; старый room chat получил новый CSS. | Сквозной browser cycle join/request/invite/revoke/moderation, два разговора, unread, скрытый профиль. App discussion/новый DM нельзя объявлять реализованными community chat; это зависимость P3/P7. |
| P2-F. Помощник, устройства, доступы | **partial** | Постоянный Assistant и Access, owner history, immutable pending target/request ID, свежая restore, per-tab binding; отдельная create-app кнопка теперь тоже durable и account-bound; реальный apps service/guard проверен; Access read/revoke/history и fail-closed availability. | Реальная команда/inference/result на устройстве и новый человек; enforced runtime, OS isolation/lease/cost/child stop — P5, не готовая возможность P2. |
| P2-G. Сохраняемые инструменты | **partial** | Новый внешний вид комнат/шахмат/файлов без старого style import; keyboard chess; ACK/backpressure, исходный sent Blob, SQLite durability, явные partial slots/discard; 36 focused tests. | Финальный browser partial/retry/status после integration; реальный command/traffic/remote tool цикл; физическое устройство/RAM; production inventory legacy и миграционный/backup gate. |
| P2-H. Поле и движение | **partial** | 11 geometry tests, 3 stable-layout tests; единые правильные соты/скругление; фильтр не пересобирает позиции; reduced-motion предусмотрен; поле необязательно. | Плотное реальное поле с длинными именами, клавиатурным обходом, панорамой/возвратом и добавлением; наблюдение reduced motion и forced colors в браузере. |
| P2-I. PWA и интеграция | **partial** | 5 VM tests: precache, dirty другая вкладка, all-ready activation, новая вкладка при prepare, offline shell/API exclusion; реальная production preview offline Notes/reload/reconnect и update с Assistant draft. | Настоящая installed PWA, background/discard/bfcache, phone keyboard, weak-device soak. Ни VM, ни desktop viewport 320 не закрывают эти пункты. |

В таблице намеренно нет «P2 полностью pass» на основании существования файлов. Приёмка конкретных локальных компонентов в §1 может быть пройдена, пока широкий критерий P0 остаётся `partial`.

## 3. Матрица V01… V20

| ID | Статус | Доказательство и оставшаяся граница |
|---|---|---|
| V01 — desktop 1440/1024, одна иерархия | **pass** | Просмотрены оба размера apps-first главной; одна шапка/навигация, поиск/Add/контекст. Rooms receipt отдельно подтверждает отсутствие старого stylesheet. Заполненный каталог проверяется в V06/V15. |
| V02 — mobile 390/320 | **partial** | Главная, Notes, последний основной Assistant при реальном innerWidth320; root DOM no-overflow, peer room/dialog; нижняя навигация видна. Pending в исправленном fixture scope просмотрен отдельно; длинная community strip и реальная экранная клавиатура ещё не покрыты. |
| V03 — 768, landscape, 200% | **partial** | Свежий home 768 и peer rooms/chess 768 просмотрены. Landscape и 200% — **not-tested**. |
| V04 — вся светлота и состояния | **partial** | Тема содержит 11514 численных contrast assertions плюс primary actions/chrome на каждом шаге; dark/light actual evidence. Все computed DOM пары disabled/focus/dialog при 0/25/50/75/100 ещё не замерены. |
| V05 — геометрия, clipping, focus | **partial** | Геометрия/соседи/insets/safe rectangle проходят; single field 320 и room 82×71 наблюдались. Длинный mixed-script контент и focus каждого nested action в плотном поле ещё не пройдены. |
| V06 — действия карточки без двойного launch | **not-tested** | Новые handlers прочитаны, но одного запуска Notes недостаточно для app open/connections/save/chat/menu на настоящем runtime app. |
| V07 — возврат и устойчивое расположение | **partial** | Автотест стабильных identity slots, browser search q/kind Back и Notes Back; account-scoped preferences. Полный app→context→Back/camera/focus и populated field пока не подтверждены. |
| V08 — overlays и keyboard | **partial** | Native dialogs, реальные Escape/opener checks и synthetic Access. Полный trap/Tab across browsers и submit/error над физической phone keyboard — **external**. |
| V09 — name/role/state, клавиатура | **partial** | Header aria-label, real keyboard chess, arrow tabs, native buttons/dialogs. Полный screen-reader обход первичных и вторичных действий не выполнялся. |
| V10 — ошибки и подтверждённый успех | **partial** | ACK/replay tests, Notes не выдаёт ложное Saved, Assistant сохраняет unknown; loading не соседствует с empty, live-region очищается при route change. Финальные file error/delete success состояния требуют завершения интеграционной проверки. |
| V11 — четыре аудитории и draft target | **partial** | Community, старые E2EE room conversations и private Assistant различимы; 13 state cases запрещают перенос draft/target. Будущие app/DM контексты не объявлены готовыми, их полная четвёрка ещё не пройдена. |
| V12 — полный Notes цикл | **partial** | 35 Notes tests и реальный редактор/Back/reload; отдельный clean-cache offline/reconnect путь принят. UI archive/trash/export/conflict/quota, особенно в двух вкладках, ещё не подтверждены полностью. |
| V13 — понятное согласие и отзыв | **partial** | P1 signed owner boundary и Access fixture; видны account/client/scope/expiry/action limit; не обещается возврат данных или отмена эффектов. Реальное подключение нового внешнего клиента — P4; объяснение согласия новым человеком — **external**. |
| V14 — задачи и неизвестный результат | **partial** | Owner send/history/read/cancel contracts и state/fixture paths; unknown не создаёт новый request ID, cancel не означает rollback. Реальные running/needs-input/approval/stop/effects на устройстве — **external**, P5. |
| V15 — настоящие app failures/launch | **not-tested** | Нет пройденного browser набора process stopped/offline/revoked/iframe incompatibility/slow launch/separate tab на настоящих приложениях. Одного пустого каталога и backend теста недостаточно. |
| V16 — installed PWA и жизненный цикл | **partial** | 5 VM cases — локальный **pass**; root production preview подтвердил cached shell, clean offline Notes/reconnect, сохранение Assistant draft при update. Installed mode, полный multi-tab update на настоящей сборке, OS discard/bfcache/phone offline-online — **external**, ещё не пройдены. |
| V17 — A→B/logout/revoked без утечки | **partial** | Notes signed two-account tests, Assistant foreign/account payload tests, Access/Assistant synthetic dispose guards. Полный браузерный цикл через cached app/recent/OPFS/room DOM ещё не выполнен. |
| V18 — исходные байты и bounded files | **partial** | 36 focused tests, включая binary/empty/Unicode, sent Blob, exact command bytes, destroy/reconnect/quota и slots. Большой серверный тест и desktop OPFS helper сообщены отдельным receipt. Полный browser/network 512 MB и память телефона этим не доказаны. |
| V19 — ресурсы и длительная работа | **partial** | Ограничены очереди/crypto/OPFS операции, history pages и replay; dispose/generation проверяются. Нет измеренного многочасового navigation soak и weak-device RAM/frame budget. |
| V20 — новый человек без подсказок | **external** | Такой человек ещё не проходил маршрут app→note→community→device→agent result→revoke. Исполнитель, знающий код и ожидаемые кнопки, не заменяет этого участника. |

## 4. Незавершённые передачи — отдельная независимая приёмка

Закрыт найденный ранее материальный дефект: четыре оборванные загрузки занимали все `receivingFiles` slots, но человеку нечего было удалить. Теперь SQLite возвращает bounded inventory в hello; последующие изменения используют `files.pending`/`file.progress`; клиент расшифровывает имя и показывает отдельную карточку без ложного download. Локальный `fileId` включается в ошибку отправки. Удаление не происходит по TTL.

Повторно пройдены точные сценарии:

1. Четыре первых блока приняты, отправитель переподключается с тем же device ID; все четыре partial присутствуют в hello и новой SQLite connection.
2. Пятый файл не получает ACK и не занимает ещё один слот; отказ явный.
3. Explicit delete одной передачи получает ACK после commit; осталось три partial, reservation уменьшается с 24 до 18 байт тестового набора; пятый файл затем принимается и резерв снова равен 24.
4. Клиент расшифровывает Unicode filename, не вызывает `onFile` для partial, сохраняет карточку до ACK удаления; после ACK убирает её. Следующая часть того же отменённого producer отвергается.
5. Ошибочный финальный блок не оставляет receipt/счётчиков; правильный retry завершает тот же файл. Full history quota не запрещает удаление существующего файла. Deleted IDs не воскресают, неправильная авторизация не получает историю.

Это **pass для ограниченного механизма**. Не проверены реальным браузером последовательность click-discard→потеря ACK→route switch→reload, полная desktop OPFS передача через TunnelSync и физическая память телефона. Новые partial controls требуют финальной интеграционной проверки; helper/WS fixtures не подменяют её.

## 5. Оставшиеся материальные условия и долги проверки

### Перед локальным checkpoint P2

1. **Финальная общая проверка:** повтор production build, всей затронутой suite и ключевых paths после последних исправлений. Промежуточный общий green не доказывает более позднюю правку; итоговый полный receipt принадлежит интегратору.

Apps integration закрыта: expectedAccountId проверяется до операции, operation schema остаётся строгой; 12 реальных cases независимо повторены. Lifecycle закрыт разделением resetAccount/destroy и повтором fixture с quota. Нечитаемый pending prompt был дефектом контекста тестовой страницы; исправленный реальный CSS scope повторно проверен по computed style и свежему PNG. Эти условия больше не являются открытыми блокерами.

**Закрытые local blockers:** clean offline Notes теперь имеют отдельный bounded snapshot cache и пройденный реальный offline/reconnect путь; independent 35 tests/source review подтвердили guards. Потеря ACK создания приложения больше не создаёт новый request ID после remount; immediate input синхронно защищён до identity await, старый delayed save не побеждает новый. 14 state cases и исходный browser fixture пройдены.

Ранее найденные проблемы ложного «не отправилось», невидимой приёмной ошибки и устаревшего delete notice исправлены и повторно прочитаны. Файловый receipt обновлён. Изменение `jobs-runtime.test` не ослабляет лимит/продолжение: публичное раскрытие заменено отдельными проверками private storage и public absence, 10 HTTP/jobs cases проходят.

Других воспроизведённых материальных дефектов нового partial/reload/delete/slot механизма в данном bounded review не осталось. `not-tested` строки V06/V15 и локальные части `partial` требуют отдельного прохождения; отсутствие теста не превращено ни в придуманную ошибку, ни в `pass`.

### Перед общей приёмкой и публичным выпуском

- Дозаполнить локальные сценарии матрицы: реальные приложения и community lifecycle; Notes negative/recovery UI; A→B/logout; 200%/landscape/клавиатура; длительное переключение без потери состояния. Фиксировать конкретные данные, действие, ожидаемый и наблюдаемый результат.
- Пройти installed PWA и физический телефон, доступность первичных действий с экранной клавиатурой, background/discard/обновление между вкладками; измерить память и отзывчивость при реалистичном файле. Desktop 320 px не заменяет это.
- Провести сценарий новым человеком без подсказок автора. Записывать ошибки и незавершённые действия; «выглядит современно» и обещание всеобщего восторга не являются критерием `pass`.
- Перед включением SQLite v2 на production проверить inventory legacy JSON. Источник выше безопасного 64 MiB требует отдельного streaming import; текущий код обязан отказать, сохранив исходник, а не создать пустую комнату. Проверить согласованный backup/restore с WAL, версию reader и запрет downgrade на v1 после новых v2 записей. Локальный backup test не является production restore.

## 6. Что остаётся в следующих этапах и не потеряно

Именные поддомены, постоянная identity приложения и собственное обсуждение — P3; HTTP/MCP и первый настоящий внешний Notes loop — P4; ограничения доверенного исполнителя, lease/изоляция, денежный admission и подтверждение остановки — P5. Независимый автор, длительная функция и постоянный статический hosting проверяются раздельно в P6A/B/C; обнаружение/социальное развитие — P7. Эти пункты не закрываются оформлением новой оболочки.

Существующие функции не объявлены удалёнными: Notes, устройства/команды, комнаты, файлы, шахматы, recovery и личный Assistant сохраняют реальные controllers/domain state. Проверен перенос внешнего вида и названные сценарии, а не все возможные удалённые эффекты. Приоритет внешних ИИ соблюдается fail-closed контрактами и постоянным Access, а не выдачей неработающего ключа или обещанием автоматической совместимости со всеми агентами.

Ни коммит P2, ни push, ни deployment, ни прохождение внешних gates данным документом не заявляются.
