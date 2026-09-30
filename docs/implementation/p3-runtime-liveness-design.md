# P3-R0 — живость внутреннего WebSocket

Дата: 2026-09-30. В разделе R0 зафиксирован согласованный контракт; на этой стадии код, конфигурация, процессы и браузер не изменялись. Последующий авторский R1 receipt приведён в конце. Основание: [диагностика](p3-runtime-idle-preflight.md), последовательность и владение: [R0–R4](p3-runtime-liveness-plan.md).

## Решение

Добавить только на gateway потоковый relay, понимающий границы WebSocket frames. Он передаёт прежние байты и проверяет отдельно браузер и исходное приложение. После тишины посылает стандартный Ping: браузеру — unmasked, источнику через существующий `data/ACK` — masked. Новый JSON wire protocol, зависимость или выпуск коннектора для этого решения не требуются; это ещё предстоит доказать реальным туннелем. HTTP idle120s остаётся прежним.

Полностью принятый структурно допустимый frame обновляет живость **только своего отправителя**. Неполные байты, исходящий трафик, chunk ACK, внешний Ping/Pong, HEAD и binding ACK не являются таким подтверждением. Это транспортная активность, не exact round-trip нашего Ping, работоспособность бизнес-логики или присутствие человека. Поэтому активный endpoint не закрывается только за отсутствие конкретного Pong.

[RFC6455 §5.4–5.5](https://www.rfc-editor.org/rfc/rfc6455.html#section-5.4) допускает control frames между фрагментами сообщения, но не внутри frame; ответ на несколько Ping может ограничиться последним. Собственный masked frame требует свежую непредсказуемую маску. Control payload ограничен125байтами. [WHATWG](https://websockets.spec.whatwg.org/#ping-and-pong-frames) не предоставляет браузерному JavaScript control Ping/Pong API. TCP keepalive из [RFC9293 §3.8.4](https://www.rfc-editor.org/rfc/rfc9293.html#section-3.8.4) не заменяет проверку WS endpoint за прокси.

## Точный стык

Новый `modules/apps/server/websocket-relay.mjs`:

```js
createWebSocketRelay({
  toClient,   // (Buffer) => Promise<void>: одна упорядоченная запись raw socket
  toSource,   // (Buffer) => Promise<void>: существующие seq/data/ACK, без повторного входа в relay
  assertActive, // () => void: синхронный текущий checkStream; отказ бросает исключение
  onFailure, // (error) => void: единожды закрыть только данный stream
  timing,    // {quietMs, insertMs, responseMs, frameMs}; defaults30_000 каждое
  // Только для unit: monotonic now и управляемые setTimeout/clearTimeout.
}) => ({
  start(),                 // после принятого101, не после произвольного head
  clientBytes(bytes),      // Promise<void>; masked ingress, <=48KiB
  sourceBytes(bytes),      // Promise<void>; unmasked ingress, <=48KiB
  close(),                 // идемпотентно, без повторного onFailure
})
```

Тот же модуль экспортирует `normalizeWebSocketLivenessTiming(value)`: `undefined` означает defaults, разрешён частичный объект только с четырьмя указанными ключами и положительными safe integers≤30_000; результат полный и frozen. Root вызывает этот единственный validator до mkdir/DB. Неизвестные ключи, null, массивы, Infinity/NaN/ноль/дроби отвергаются.

`start()` не воскрешает закрытый relay. Root обеспечивает порядок HTTP101 перед любым WS output; если запись заголовка ещё ожидает, допускается только один ограниченный chunk перед тем же gate, без преждевременного ACK. Ошибочный head/отсутствие101 сохраняют head deadline30s и не запускают heartbeat. Upgrade head и большие client reads root делит на последовательные части≤48KiB.

Оба `bytes()` завершаются после обработки **текущего chunk**, даже если frame ещё не закончен. Ожидание всего1MiB frame до ACK48KiB создало бы deadlock. Между вызовами остаются только состояние счётчиков, header≤14байт и control payload≤125байт; тело data не собирается целиком и не размаскируется. Для сравнения служебного Pong достаточно control payload. В процессе допустим один текущий input≤48KiB на направление; отклонить второй конкурентный push, не создавать очередь неизвестного размера. Не удерживать большой backing buffer ради маленького остатка.

Один writer на направление обслуживает и пользовательские байты, и максимум один ожидающий собственный Ping. Входной chunk может содержать много frames: обход потоковый, без массива сегментов всего сообщения. Pending Ping вставляется на ближайшей законченной границе до следующего frame, включая границу внутри chunk. Он никогда не запускает параллельный `sendChunks` и не создаёт второй pending ACK. Действующие маски, payload, fragmentation и порядок приложения не меняются.

На направление разрешён один output staging buffer≤48KiB: соседние маленькие frames объединяются в запись, а не создают отдельный JSON/ACK на каждый2-byte frame. Flush — при заполнении, конце текущего input или собственном probe без input. Служебный control включается в тот же bounded batch только на известной границе; callback batch подтверждает его запись. Ни массива тысяч Promise, ни копирования всего сообщения. Регрессия48KiB нулевых frames должна дать ограниченное число output chunks (не24тысячи), с тем же порядком. Память relay на направление: один input≤48KiB, staging≤48KiB, малые header/control/tag и счётчики.

Структурный parser — один источник правил: расширить/извлечь нынешний `createWebSocketLimiter` в `protocol.mjs`, сохранив его совместимый export; новый relay использует тот же parser. Лимиты/opcodes/RSV/mask/каноническая длина/порядок fragmentation/1MiB сообщения сохраняются. «Допустимый frame» здесь означает эти транспортные проверки и полный payload; это не новое обещание проверки семантики приложения или всей UTF-8 последовательности текста через несколько frames.

## Таймеры и собственные controls

| Состояние одного endpoint | Абсолютный предел | Переход |
| --- | --- | --- |
| Нет полного входящего frame | quiet30s | Поставить единственный Ping в writer, проверить допуск |
| Ping ждёт границы/полной записи | insert30s | Успешная запись начинает response deadline; отказ закрывает stream |
| Ping записан, endpoint молчит | response30s | Любой полный допустимый входящий frame завершает проверку; иначе закрытие |
| Начат header или payload frame | frame30s от первого байта | Только завершение frame снимает deadline; новые байты его не продлевают |
| Получен полный Close с любой стороны | closing30s | Relay передаёт закрытие, прекращает оба heartbeat; завершение/ошибка/срок закрывают stream |

Это выбранный ограниченный streaming profile: даже легитимный1MiB frame, который не успел пройти за30s, будет остановлен. Предел не является требованием RFC. Таймеры используют монотонное время, проверяют истечение и на входе/после await; поздний callback не воскрешает истёкший запрос. Если endpoint прислал полный frame до отправки ожидающего Ping, ненужную ещё не начатую вставку можно снять. Уже начатая запись имеет свой неизменный deadline; ранний ответ до её callback учитывается, а не теряется.

Каждый writer await, включая обычные данные, ограничен тем же `insertMs` (default30s). Таймаут закрывает stream и завершает ожидающие `bytes()` отказом, даже если пользовательский callback не вернулся; его поздний результат не запускает продолжение. Это отдельно от assembly и существующего tunnel ACK deadline.

На весь срок relay генерируются **два независимых случайных32-byte tag**, по одному на endpoint. Это только распознавание, не секрет доступа и не уникальный challenge каждого round-trip. Каждый собственный masked Ping получает новую random mask. Pong, чей payload точно совпадает с tag своего входящего leg, поглощается даже после поступления обычных данных/следующего probe; при этом обновляет живость. Остальные Pong и все чужие Ping передаются. Разные tag не смешивают направления; память постоянная, nonce cache/expiry не нужны. Relay не отвечает за endpoint на чужой Ping. Намеренное копирование служебного tag не даёт прав или иной активности, чем обычный допустимый frame.

Quiet30 + insert30 + response30 ограничивают молчащий endpoint примерно90s с последней активности; partial/write/Close могут завершить раньше. Задержка event loop не является жёстким wall-clock SLA. Старый connector120s получает реальные inner control bytes задолго до своего idle; его таймер не отключается и не считается доказательством успеха — требуется длинный default-тест без ускорения.

Для независимых сетевых тестов root добавляет доверенную service option `webSocketLiveness:{quietMs,insertMs,responseMs,frameMs}`: только конечные положительные значения≤30_000, default30_000, неизвестные поля/нулевое/Infinity отвергаются. Это не пользовательский API. Closing deadline30s, в ускоренных tests не длиннее согласованного responseMs. Unit clock/timers — отдельный seam.

## Авторизация, ресурсы и завершение

Перед каждым ограниченным синхронным input burst≤48KiB, перед **каждой** реальной записью и после каждого await вызывается `assertActive`. Завершённые frames внутри одного burst без await используют уже проверенный допуск; иначе24тысячи нулевых frames превратились бы в24тысячи SQLite/World проверок. После await новый guard обязателен. Root дополнительно сохраняет свой guard перед внешним ACK. Это включает exact socket/channel/binding/target/epoch, действующее право и прежний абсолютный срок account session; public lease продлевается только прежней свежей проверкой. Heartbeat не обновляет credentials и не увеличивает absolute1h.

Остаются32 общих /24 public-basis slots, один ACK,4MiB outer buffered limit, head/ACK30s и1MiB message. Два bounded writer на stream не означают два дополнительных слота. Закрытие очищает все timers, pending promises и references однократно; поздние write/ACK/timer callbacks не посылают данные и не освобождают слот повторно. Невалидный frame, молчание, зависшая запись и revoked access закрывают только свой stream. На ошибке разрешено прежнее raw destroy; не обещаем доставленный Close-code и не посылаем1006 по проводу.

## Внедрение и приёмка

R1: agent_ecosystem — новый relay, единый parser в пределах `createWebSocketLimiter` и авторские unit tests. R2: root — index wiring, timing option, head/write/ACK lifecycle. R3: whole_product_critic — отдельная реальная HTTP/WS acceptance. R4 назначается после этих gate: пример сохраняет несданный ввод и предлагает явное восстановление; произвольный чужой iframe нельзя незаметно переподключить с гарантией сохранения состояния. Никаких повторов пользовательского POST.

Обязательные проверки:

1. Настоящий старый connector и default timings: тишина>120s, потом обмен бизнес-сообщениями; отдельно каждый односторонний трафик.
2. Молчащий browser/source при живом outer channel; другой endpoint и соседнее приложение продолжают работать; слот возвращается один раз.
3. Неполный header и медленно капающий payload не продлевают frame deadline; законченные frames обновляют только своего отправителя.
4. Fragmented/binary1MiB: chunks≤48KiB получают ACK до конца frame; Ping между frames, ни разу внутри; byte identity/порядок/маски и лимит сохраняются.
5. Законный latest-Ping-only peer, собственный поздний Pong после обычного frame, обычный чужой Pong, самостоятельные Ping приложения; нет требования exact собственного ответа.
6. Held socket write/ACK, queued Ping и input, split control payload, early Pong до callback, таймер на той же границе: bounded memory и один writer; нет вечного Promise.48KiB маленьких/нулевых frames не умножаются в тысячи внешних writes/ACK или вызовов authority guard.
7. False/late head, data до101, Close/Pong-only послеClose, reset/ошибка/двойной close, поздний callback послеrevoke/reconnect/source switch — без новых bytes и повторного освобождения.
8. Все32/24 caps, прежние head/ACK и HTTP idle; неизвестные timing options; абсолютный account deadline и public lease expiry не продлеваются heartbeat.

На момент R0 выполнено только чтение кода/RFC и проектирование. Прочитанные SHA256: index `3174f667b7a651fe80f892bd73ad27c2214b65ec0f99df2f917ea1bac5ba974a`; protocol `445c092e6243e190dd4e166c2ea1554ec42f0afb61db45cd6e80191d6774dd0f`; connector `0cc9657750c987687fc49d35f592c3e38c164214e9fb08df10d561c41eb14ec4`.

## R1 — авторская реализация и freeze

После согласования root и независимого разбора критика реализованы новый `websocket-relay.mjs`, единый incremental `createWebSocketFrameParser` в пределах прежнего limiter и отдельный `websocket-relay.test.mjs`. Index, коннектор, схема, UI, конфигурация, процессы и браузер этим автором не менялись. Родительская интеграция R2 и реальная независимая приёмка R3 имеют отдельное владение.

Стык соответствует описанному выше. Инъекция unit-времени: `clock:{now,setTimeout,clearTimeout}`; production по умолчанию `performance.now()`. `close()` отклоняет незавершённые input promises и освобождает parser/staging ссылки, не ожидая чужого callback и не вызывая повторно `onFailure`. Одна таймерная запись на relay пересчитывается на ограниченных границах работы, а не на каждом пустом frame.

Автор лично выполнил `node --test --test-concurrency=1 modules/apps/test/websocket-relay.test.mjs modules/apps/test/websocket-limit.test.mjs`: **23/23 PASS,0 FAIL,0 skips**, около0.2s; отдельно `git diff --check` по своей области — PASS. Это21 новых тестов и2 прежних проверки limiter. Проверяются1MiB streaming без ожидания целого frame, оба направления tiny-frame batching/guard/timer counts, маски/tag/late Pong, чужие controls, fragment boundary, endpoint-only activity, partial/insert/write/Close deadlines, early response, недействительный framing, revoke послеawait и teardown с зависшим callback.

Первый авторский запуск дал20/21: early-response fixture искусственно заставлял **оба** writer callback ждать обработки противоположного queued pump, создавая замкнутую зависимость, которой у TCP write callback нет. Fixture исправлен на два отдельных направления раннего ответа; критерий «ответ до callback не теряется» сохранён. Это не объявлялось исправлением production-дефекта. После этого добавлены отдельные проверки explicit disposal и split late Pong.

Замороженные SHA256:

| Файл | SHA256 |
| --- | --- |
| `modules/apps/server/websocket-relay.mjs` | `586ff21c55bc18dbfe73427a344a06075f4ab9498a56cbae3182df6689104749` |
| `modules/apps/server/protocol.mjs` | `48c0b81e194cbf68f457723c4423283192e04220a642cdbbdd60f406246a9dc4` |
| `modules/apps/test/websocket-relay.test.mjs` | `989403efb0178e3a43c2486749b33c7c49d37185ec9bf58fcadbd0374c2ce688` |

Эти unit checks не являются доказательством DNS/TLS/edge или долгого default-туннеля. R3 запускает их независимо. Сообщённый критиком первый быстрый настоящий туннель дал15 PASS/0 FAIL/1 opt-in skip; здесь это атрибутированное промежуточное свидетельство, не собственный прогон и не закрытие130+s gate. Источники R1 далее не меняются без отдельного finding/review.

## R1 repair — удержание завершённых Promise

Root при независимом чтении повторно открыл R1/R3: прежний `flush()` делал `Promise.race` с одним живущим весь relay `stopped.promise`. Победа writer не снимает реакцию с оставшегося pending Promise. Поэтому прежний freeze выше не доказывал ограниченную память при длительном активном соединении. P4 приостановлен до этого исправления и независимого повторного gate.

Дефект воспроизведён **до изменения relay** новым `websocket-relay-memory.test.mjs`: отдельный Node child с `--expose-gc`, настоящий relay и `async_hooks` наблюдают создание/уничтожение нативных Promise. Наблюдатель хранит только числовые async IDs, не Promise/resource objects. Writer сразу завершает реальную запись relay в тестовом callback; часы заморожены, чтобы исследовать накопление записей отдельно от heartbeat. Обе стороны передают допустимые frames, relay остаётся открытым при всех измерениях и используется ещё раз после последнего GC. Четыре прохода GC и отдельные event-loop turns позволяют обработать destroy callbacks.

| Всего законченных записей в открытом relay | До исправления: живые Promise после GC | После исправления |
| --- | ---: | ---: |
| 500 | 1008 | 6 |
| 4500 | 9008 | 6 |
| 20500 | 41008 | 6 |

До исправления тест дал RED с линейным удержанием двух Promise на запись. После исправления — GREEN; наблюдатель зарегистрировал сборку287037 Promise. Допуск теста — не более64 дополнительных живых Promise между выборками; он не сравнивает внутренние поля relay или текст реализации. Это проверка heap lifecycle данной нагрузки в Node24.13.1, не оценка ёмкости production RAM, общей памяти сервера или неограниченного срока без дефектов.

Удалён общий stop Promise. У каждого pump теперь только `activeWrite` — completion текущей записи. Shutdown отклоняет максимум две текущие completion и освобождает ссылки; `finally` очищает ссылку после обычного завершения. Settlement чужого writer направляется в эту completion; синхронное исключение также проходит через неё. После отмены поздний callback не продолжает отправку. Parser, тайминги, authority/lease checks, index и wire не менялись.

Автор выполнил:

```text
node --test --test-concurrency=1 modules/apps/test/websocket-relay-memory.test.mjs modules/apps/test/websocket-relay.test.mjs modules/apps/test/websocket-limit.test.mjs
```

**26/26 PASS,0 FAIL,0 skips**, около0.53s:23 прежних проверки, реальный memory regression и две дополнительные проверки отмены обеих зависших записей, sync throw/rejected writer/synchronous close. Прежние early-response, revoke после await, timeout и teardown остались зелёными. `git diff --check` по собственной области — без замечаний. Root прочитал и принял узкое исправление; независимый R3 repeat на новом SHA на момент этого receipt ещё ожидается. Ранее сообщённые17 быстрых и130.59s default сетевые PASS относятся к предшествовавшему SHA, не подменяют повторный gate.

Новый авторский freeze:

| Файл | SHA256 |
| --- | --- |
| `modules/apps/server/websocket-relay.mjs` | `e57d674d3fe5f21fa55bb6353c2305666fb643eecfe910b5b384837b667d1e9d` |
| `modules/apps/server/protocol.mjs` — без изменений repair | `48c0b81e194cbf68f457723c4423283192e04220a642cdbbdd60f406246a9dc4` |
| `modules/apps/test/websocket-relay.test.mjs` | `aefbea971cdf5d0e33c421fb4cfa2699c3168b7f807f4059f6b077e836bb7bc1` |
| `modules/apps/test/websocket-relay-memory.test.mjs` | `efae0a487fefd88f0f73717226f8f0c5a2ee84780c4c5e0621628714a237fd34` |

Автор завершает turn; P4 не продолжается до отдельного назначения после R3/R4 checkpoint.
