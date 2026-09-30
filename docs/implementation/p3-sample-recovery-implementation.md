# P3-R4: восстановление примера — авторская реализация

30.09.2026. Основание: [preflight](p3-sample-recovery-preflight.md) и [план P3-R](p3-runtime-liveness-plan.md). Статус: **повторный авторский freeze после двух обнаруженных дефектов; настоящий browser gate отдельно**. Изменены только `modules/apps/examples/sample-app.mjs`, новый `modules/apps/test/sample-recovery.test.mjs` и этот receipt. Relay, Apps protocol, gateway, shell UI и внешние настройки не менялись. Sample API получил согласованную opt-in проекцию с версией; прежние URLs и формат сохранены. Коммита/деплоя не было.

## Поведение

Сохраняется прежний тёплый независимый пример «Покупки». Поле ввода остаётся редактируемым и доступным для копирования при потере связи. Соединение и результат действия имеют разные статусы. «Подключить снова» выполняет только GET и открывает новый WS; прежний socket закрывается, его поздние callbacks игнорируются. Первичная готовность наступает после валидного snapshot текущего WS, а не одного `open` либо успешного GET.

POST фиксирует исходный payload и draft revision. В один момент допустим только один POST. Поле очищается после проверенного ответа **конкретного fetch**, с 2xx и корректной формой JSON, если исходная revision и текст ещё совпадают. Последующие правки, включая A→B→A, не стираются. WS/GET совпадение текста не считается подтверждением отправки.

Если ответ потерян, невалиден, имеет ошибочный status или истёк deadline, состояние становится `unknown`. Оно не утверждает, что сервер откатил эффект. Исходный текст A остаётся в отдельном readonly поле; более новый B остаётся в основном поле. Toggle тоже не повторяется автоматически.

Чтобы продолжить после unknown, человек явно переподключается **после возникновения unknown**, получает валидный snapshot новой connection generation и нажимает «Я проверил список». Это убирает предупреждение и сохранённый текст попытки, как прямо объясняет интерфейс; текущий ввод не меняется. Кнопка новой отправки называется «Добавить как новое», предупреждение о возможном повторе остаётся. Никакого POST при этих действиях нет.

| Состояние | Допустимые действия |
|---|---|
| `connecting` | Ввод/копирование; одна попытка GET/WS, запись закрыта |
| `ready` + нет mutation + свежий snapshot | Явное новое добавление/toggle |
| `disconnected` | Ввод/копирование, явное повторное подключение |
| mutation `pending` | Ввод нового draft, read-only reconnect; второй POST закрыт |
| mutation `unknown` | Сохранён исходный текст; новый POST закрыт до fresh reconnect + явного review |
| подтверждение и список разных запусков | ACK остаётся подтверждённым; новое действие закрыто до следующего explicit reconnect + review |

Connection generation и captured mutation object независимы. Поэтому корректный POST ACK после закрытия WS остаётся подтверждением, не переводит соединение в ready и не перерисовывает список старым ответом. После первого GET preview списком владеют только snapshots текущего WS. Для следующего POST нужны подтверждённый HTTP результат и показанный snapshot **того же запуска с revision не ниже ACK**.

## Исправления независимых findings

Первый авторский срез 15/15 не обнаружил два дефекта. Root в настоящем браузере показал потерю фокуса reconnect и checkbox при `disabled=true`. Независимый reviewer воспроизвёл более серьёзную гонку на настоящем sample HTTP/WS: чужая запись до нашего commit увеличивала клиентский счётчик доставок; затем собственный ACK включал checkbox с ещё старым значением. Второй toggle мог инвертировать уже сохранённый результат.

Минимальный согласованный versioned контракт относится только к этому независимому примеру:

- `/api/items?snapshot=1` (GET/POST) и `/live?snapshot=1` отдают `{instance, revision, items}`. `instance` — случайные 16 bytes в lowercase hex, создаётся на каждый `createSampleApp`; `revision` — неотрицательный safe integer, начиная с 0.
- Каждая применённая мутация, **включая legacy POST**, увеличивает единую revision. Mutation, увеличение revision и сериализация происходят синхронно. HTTP ACK и WS broadcasts одного результата получают один snapshot. При исчерпании `Number.MAX_SAFE_INTEGER` новое изменение отклоняется 503 до эффекта; счётчик не оборачивается.
- Старые `/api/items` и `/live` сохраняют array, а legacy WS также echo. Opt-in клиент не принимает старый array как versioned snapshot. Это не migration Apps wire и не обещание durable sample storage.
- До ACK mutation сама закрывает запись. После ACK требуется `currentWS.instance === ACK.instance` и `currentWS.revision >= ACK.revision`. Чужой broadcast до commit не проходит, независимо от момента доставки. Поздняя меньшая revision того же instance не откатывает показанный список. Равенство текста/массивов не используется.
- Разные instances численно не сравниваются. Сообщение честно говорит: «Изменение принято, но подтверждение и показанный список относятся к разным запускам». Оно не объявляет ACK потерянным и не предполагает, какой запуск старше. Нужны следующий явный reconnect и review; POST не повторяется. Смена instance внутри того же WS считается невалидной и закрывает соединение.
- Временная недоступность reconnect/add/checkbox/reviewed теперь обозначается `aria-disabled`, сохраняя focusability. Все effect handlers проверяют состояние синхронно; add click подавляет native validation до submit, checkbox click/Space не меняет значение при недоступности. Поздний ACK не вызывает focus. Авторский harness теперь моделирует blur при native disabled, но browser gate остаётся отдельным.

## Ограничения и cleanup

- Connection/read и POST имеют конечный deadline **12 000 ms**. Abort после dispatch означает unknown, не отмену эффекта. Время зависит от обслуживания browser event loop, это не жёсткая wall-clock гарантия.
- В каждый момент один актуальный socket/connection deadline и один mutation deadline. Reconnect закрывает/отсоединяет предыдущие callbacks и abort-ит GET. POST не отменяется только из-за переподключения WS.
- JSON snapshot проверяется: строгие scalar instance/revision, array items, уникальные строковые IDs, ограниченные строки, boolean done, известные поля, максимум 10 000 items и 1 MiB UTF-8. Это предел клиентской принимаемой проекции, не новая backend storage quota; `response.text()` не объявляется потоковым RAM ограничителем.
- `pagehide` очищает transport/timers, pending становится unknown. При BFCache return не выполняется автоматический POST или reconnect; текст той же живой страницы остаётся. Это не сохранение после нового документа/reload.
- Никаких localStorage, modal/beforeunload promises, heartbeat бизнес-сообщений, tokens, shell redirects, postMessage или новых зависимостей.
- Form/input и существующие строки списка сохраняют DOM identity. Snapshot не заменяет draft/selection. Явное закрытие unknown возвращает фокус в основное поле; асинхронный ACK не перехватывает фокус. Recovery controls имеют `type=button`, статусы — `role=status`.
- CSS сохраняет исходную палитру, даёт button/label targets минимум 44 px, перенос при 320 px и компактные отступы в низком landscape. Реальная геометрия и поведение нативного Tab/Space требуют browser проверки ниже.

## Выполненная проверка

Команда: `node --test --test-concurrency=1 modules/apps/test/sample-recovery.test.mjs`.

Последний авторский результат: **20 tests / 20 PASS / 0 FAIL / 0 SKIP**. Совместно с двумя независимыми causal tests из `sample-recovery.acceptance.test.mjs`: **22/22 PASS**, 0 FAIL/0 SKIP, около 0.57 s. Чужой тест не редактировался автором. `node --check` и проверка whitespace/diff — PASS. Общие heavy suites не запускались.

Тест получает реальные `/`, `/app.js`, `/styles.css` от собственного `createSampleApp({port:0})`, исполняет **выданный script**, а не копию controller. DOM harness разбирает выданную разметку; его область — события и причинные assertions, не layout engine.

| Доказательство | Покрытие |
|---|---|
| Emitted script + управляемые fetch/WS/timers | Готовность только после WS snapshot; readonly reconnect; A→B и A→B→A; double submit/toggle; pending across close; поздние GET/POST/старый socket; status/shape failures; finite deadlines; pagehide/BFCache; 40 циклов подключения; строгая revision, межзапусковой review и focus/guard assertions |
| Настоящий HTTP origin + loopback fault proxy | Proxy уничтожает ответ только после получения origin response на POST. Записанный пункт существует ровно один раз, клиент показывает unknown. Recovery не добавляет второй пункт; отдельный потерянный toggle не инвертируется повторно |
| Настоящий loopback WebSocket | Выданный controller получает список, реальный socket принудительно закрывается, explicit reconnect создаёт новый socket; draft/selection сохраняются, POST count=0 |
| Настоящие legacy и versioned HTTP/WS | Legacy writer повышает общий revision; versioned HTTP ACK побитово соответствует JSON snapshot WS; legacy array/echo сохранены; разные sample instances различаются |
| Независимый causal RED→GREEN | Реальный сторонний POST broadcast доставлен до ACK и после ACK; собственный snapshot удержан. В обоих случаях доступная семантика blocked и 0 вторых POST до подтверждённого отображения |
| Existing transport smoke | 2/2 PASS: actual connector named-only HTTP/assets/POST/WS и два principals с revoke. Выборочный запуск `node --test --test-concurrency=1 --test-name-pattern="named-only publication serves\|real app HTTP/assets/POST" modules/apps/test/app-runtime.test.mjs modules/apps/test/apps.test.mjs`, около 0.85 s |
| Отдельная ручная браузерная проверка | **Не выполнена автором.** Root повторяет actual D4 после нового freeze: 320×760, 667×375, Tab/Space, native focus, copy/readability и восстановление |

Все network fixtures созданы на собственных ephemeral loopback ports и закрыты. Использовались только synthetic строки. Рабочие QA порты, пользовательские данные, headers/credentials в вывод не попадали. Новый opt-in формат проверяется отдельно от совместимости прежнего API; fixtures других авторов не копировались.

## Не заявляется

Unknown не даёт exactly-once доставки: sample API не имеет idempotency key/creation receipt. Сознательное новое действие после review может повторить прежний эффект. Сопоставление строк общего списка не доказывает принадлежность конкретному POST.

Draft находится только в памяти открытой страницы. Перезапуск браузера, новая iframe navigation и fresh launch после session expiry могут его потерять; интерфейс предлагает заранее скопировать текст. Пример не знает account и не восстанавливает чужую/истёкшую сессию. Sandbox не расширен.

R4 tests не являются доказательством туннельных Ping/Pong, caps, revoke или absolute TTL: это отдельные R1–R3 receipts. Loopback WS не подменяет долгий 130-секундный реальный tunnel test. Авторская VM проверка не подменяет browser accessibility/layout review.

## Frozen hashes

| Файл | SHA256 |
|---|---|
| `modules/apps/examples/sample-app.mjs` | `8a81acb7c3f564ab6bb9e7f09051f45a51dcee26cf897e6baee04b81053d3d6b` |
| `modules/apps/test/sample-recovery.test.mjs` | `d8dd4d599e9e83dc6f1876d0cb24b014234649d259a8379e1fda3caa7a2be443` |

Следующий шаг — независимый R4 read-only review и browser gate интегратора. До consequential finding source остаётся frozen.
