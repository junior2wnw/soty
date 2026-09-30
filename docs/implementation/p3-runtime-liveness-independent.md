# P3-R — независимая приёмка живости WebSocket

Дата: 2026-09-30. Основание: [R0–R4](p3-runtime-liveness-plan.md), [точный контракт R0](p3-runtime-liveness-design.md). D3 принят отдельно в `0644023`; этот отчёт не закрывает D4, публичный выпуск или сохранность произвольного стороннего приложения при переподключении.

## Текущий статус

R3 принят в перечисленных ниже границах на исправленном relay `e57d674…`. Приёмка была переоткрыта после найденной root утечки Promise reactions, несмотря на прежние зелёные сетевые проверки. Повтор memory + fast: **19 tests, 18 PASS, 0 FAIL, 1 ожидаемый default skip**, 14.01s: 17 независимых сетевых сценариев и 1 авторский memory test, запущенный и разобранный независимо. Новый отдельный default опыт: **1/1 PASS, 0 skip**, сценарий 130265.8033ms, весь запуск 130397.9551ms. Прежний результат 130.59s на `586ff21…` остаётся историческим и не использован вместо нового прогона. R4 и D4 остаются отдельными gate.

При разборе потребовались конкретные условия: единственный writer для данных и собственных Ping; ACK текущего48KiB chunk без ожидания всего frame; lifetime tag для поглощения запоздавшего собственного Pong; прекращение heartbeat после Close; отдельный предел каждого write await; coalescing маленьких frames; ограниченная стоимость проверок прав на синхронный входной burst, а не отдельный запрос SQLite на каждый пустой frame. Эти условия включены в согласованный R0.

«Структурно допустимый frame» сохраняет нынешние проверки маски, RSV/opcode, длины, fragmentation и размера сообщения. Это не обещание полной UTF-8/Close-code семантики или здоровья бизнес-логики. Допустимость ответа только на последний Ping сверена с [RFC6455 §5.5.3](https://www.rfc-editor.org/rfc/rfc6455.html#section-5.5.3).

## Независимый стенд и команды

`modules/apps/test/websocket-liveness.acceptance.test.mjs` использует настоящий `createAppsService`, SQLite, действующий `createLocalAppsRuntime`, внешний WebSocket, реальные loopback HTTP/WS источники и клиентов. Он не импортирует авторские fixtures. Нормальные endpoint используют установленный `ws`; один намеренно не завершающий Close raw endpoint нужен для отрицательного сценария. Fault cases явно задерживают настоящий ACK или callback уже выполненной записи, не подменяют результат parser.

Быстрая последовательная приёмка:

```powershell
node --test --test-concurrency=1 modules/apps/test/websocket-liveness.acceptance.test.mjs
```

Повтор после исправления lifecycle памяти:

```powershell
node --test --test-concurrency=1 modules/apps/test/websocket-relay-memory.test.mjs modules/apps/test/websocket-liveness.acceptance.test.mjs
```

Обязательный отдельный опыт с настоящими default timers и тишиной130+s:

```powershell
$env:SOTY_WS_LONG_TEST = '1'
try {
  node --test --test-concurrency=1 --test-name-pattern='R3 default timers' modules/apps/test/websocket-liveness.acceptance.test.mjs
  $livenessExitCode = $LASTEXITCODE
} finally {
  Remove-Item Env:SOTY_WS_LONG_TEST
}
exit $livenessExitCode
```

Длительный опыт opt-in только для стоимости общего прогона; без его отдельного фактического результата R3 не принимается. Его skip в обычной suite не равен PASS. Ускоренные negative timers не заменяют default опыт. Изменение переменной процесса не меняет конфигурацию продукта.

## Проверяемые инварианты

- Тихие inner endpoints продолжают работать дольше120s; бизнес-сообщение после130s проходит в обе стороны без нового соединения. Обычный однонаправленный трафик не считается ответом противоположного endpoint.
- Живой outer канал и ACK не удерживают молчащий browser/source. Соседнее приложение остаётся доступным, слот освобождается и может быть занят вновь.
- Обычный frame вместо собственного Pong удовлетворяет idle-проверку. Запоздавший собственный Pong поглощается; приложение сохраняет свои Ping/Pong.
- Split headers/masks, fragmented binary1MiB и промежуточный Ping сохраняют байты. Frames больше48KiB не приводят к ожиданию ACK только после полного frame.
- Реальный HTTP101 уже записан, но callback удержан: оба направления приложения и ACK входного chunk остаются за gate до его завершения. Нет WS bytes перед подтверждённым завершением101 в текущем lifecycle.
- Неверные mask/RSV/continuation и превышение1MiB отвергаются parser gateway в обоих направлениях, с причиной конкретного stream и живым соседним приложением.
- Тысячи нулевых frames не умножают внешние data/ACK и проверки authority в тысячи операций на один входной chunk.
- Partial-frame trickle не продлевает абсолютный assembly deadline. Невозвращающийся write callback/потерянный ACK не удерживает поток; позднее завершение не продолжает закрытый поток.
- Revoke и source-target promotion на async границе не посылают поздний ACK и не закрывают соседей. Старый binding не воскрешается.
- Close прекращает собственные Ping; продолжающийся Pong-only трафик не отменяет closing deadline.
- Heartbeat не продлевает абсолютную account session и не воскрешает истёкший public lease. Caps24/32 сохраняются, закрытый слот используется повторно.
- Неподдерживаемая trusted timing option отвергается до создания каталога базы.

## Разбор результата

Первая быстрая версия дала15PASS+1long skip. Затем добавлены два существенных сетевых сценария: удержанный callback HTTP101 и невалидные frames. Итоговый быстрый прогон дал17PASS+1skip на прежнем relay `586ff21…`. Эти сетевые проверки не выявили последующую найденную root утечку.

Исторический default опыт на `586ff21…` занял130.59s и прошёл. После изменения write lifecycle новый запуск на `e57d674…` также прошёл: 130.27s самого сценария, 130.40s всего процесса. Сценарий требует не менее130s без бизнес-сообщений ни от источника, ни от клиента. Коннектор работает с неизменёнными120s idle и30s ACK; новые gateway timers не переопределяются. Оба endpoint должны получить не менее3 внутренних Ping; собственные Pong не видны противоположной стороне. После ожидания проверяются запрос/ответ и отдельное сообщение источника на том же WS. Это проверка прежней120s границы, а не только перемещение clock. Для отрицательных account/public expiry сценариев смещается только policy clock; transport timers и socket в быстрых сценариях настоящие.

### Блокер памяти и повтор

Root нашёл, что `Promise.race([writerPromise, stopped.promise])` добавляет реакцию на один долгоживущий pending Promise при каждой записи. Завершение race её не удаляет. Ограниченные byte buffers поэтому не означали ограниченную общую память: состояние открытого relay росло с числом давно завершённых writes. Прежний независимый source review это пропустил; сетевой PASS не являлся heap-lifecycle доказательством.

Автор воспроизвёл RED через `async_hooks` и explicit GC при всё ещё открытом relay:1008/9008/41008 живых native PROMISE после500/4500/20500 writes. Исправление удалило общий pending stop: у каждого pump есть лишь `activeWrite` текущей записи; shutdown отклоняет максимум две текущие completion, `finally` освобождает ссылку. Поздний writer callback не возобновляет закрытый поток.

Независимый повтор автора memory test на новом исходнике дал **6/6/6 live PROMISE**, collected287037, после тех же500/4500/20500 writes. Тест хранит только async IDs, не сами Promise resources, и не закрывает relay перед GC; после замеров тот же relay остаётся usable. Это подтверждает отсутствие обнаруженного линейного удержания в данном lifecycle, не общий RSS-предел Node/браузера и не доказательство всех возможных утечек.

Медленный write тест сначала получает реальные байты на принимающей стороне, затем удерживает только completion callback. Поэтому его вывод — ограничение ожидания/позднего продолжения, а не измерение физического backpressure. Tiny-frame test действительно отправляет8192masked и24576unmasked нулевых frames; проверяет число outer chunks и host authority вызовов. Бизнес-обработчик источника намеренно не отвечает на каждое пустое сообщение, чтобы не приписывать его собственный поток ответов gateway.

Прочитана конечная интеграция: WS head timer снимается только после completion HTTP101; `upgradeReady` допускает один bounded source chunk без раннего ACK. Relay стартует после этой границы; исходный upgrade head очищается, большие client reads последовательно делятся на48KiB. HTTP ветка сохраняет прежний idle, head и ACK; это проверка diff, не новый120s HTTP опыт.

| Проверенный файл | SHA256 |
| --- | --- |
| `modules/apps/server/websocket-relay.mjs` | `e57d674d3fe5f21fa55bb6353c2305666fb643eecfe910b5b384837b667d1e9d` |
| `modules/apps/server/protocol.mjs` | `48c0b81e194cbf68f457723c4423283192e04220a642cdbbdd60f406246a9dc4` |
| `modules/apps/server/index.mjs` | `f60ca894824c4ba447d20b275c838e1d11630f265abb2182b2433002bbc0bb59` |
| `scripts/agent-modules/local-apps.mjs` | `0cc9657750c987687fc49d35f592c3e38c164214e9fb08df10d561c41eb14ec4` |
| `modules/apps/test/websocket-liveness.acceptance.test.mjs` | `933e5487144710b93d0ba72ef95484a213d8e5a4cc0c3a545584df85500b57da` |

## Границы

Node/loopback доказывают указанные реальные сетевые переходы. Held callback доказывает корректность async продолжения после настоящей записи, не физическое заполнение TCP-buffer. Packet hook удерживает конкретное сообщение при живом реальном транспорте, не имитирует всю сеть. Число chunks/authority calls не является измерением общей RAM или capacity production. Долгий quiet опыт не доказывает бесконечную доступность, edge/TLS, работу телефона, background browser throttling или бизнес-логику источника. Отдельно остаётся пользовательское восстановление R4 без повторения POST и без обещания сохранить весь контекст стороннего iframe.
