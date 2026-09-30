# P3-C2-B — независимая приёмка runtime

2026-09-30. Исходная точка — принятый C2-A `efe0d9f`. Статус: **ограниченная независимая приёмка C2-B PASS** на зафиксированных ниже исходниках и standalone artifact 1.4.0. Это не общий release gate или разрешение на production. Зона записи: новые `modules/apps/test/source-runtime.acceptance.test.mjs`, `modules/apps/test/source-runtime-bundle.test.mjs` и настоящий документ; по отдельному разрешению root — узкое обновление старой C1 fixture в `app-settings.acceptance.test.mjs`. Исходники модели, gateway и connector этим reviewer не менялись.

## Проверяемое обещание

При переключении источника существующие адреса и явные права остаются. Проверка кандидата не переводит туда пользовательский трафик. После принятого изменения прежние допуски и потоки не продолжают обращаться к старому источнику; новый запрос достигает только точной подтверждённой конфигурации. Возврат на прежний target требует нового подтверждения и сохраняет минимальную версию протокола2. Это не откат пользовательских данных и не доказательство функциональной исправности программы.

## Принятые границы API и тестового стенда

- `service.execute` остаётся синхронным, включает `apps.source.promote` и `apps.source.history`.
- Prepare вызывается только через `service.sourcePreparationExtension.executeAsync({op:'apps.source.prepare',args,actor})`. Signed Connect HTTP adapter проверяет root отдельно; прямой service-вызов не считается проверкой подписи.
- Стенд использует настоящий `createAppsService`, SQLite, `createLocalAppsRuntime`, HTTP и WebSocket. Два loopback upstream и два аутентифицированных connector моделируют устройства; это не проверка двух физических компьютеров.
- Выборочные fault cases удерживают/теряют конкретный кадр либо callback вокруг настоящего socket. Эти случаи доказывают повторную проверку границ и отсутствие подмены, но не физическую backpressure/RAM или реальную потерю сети.
- Временный disconnect или ожидание ACK не отзывают DB authorization и сами по себе не уничтожают account cookie. Прежний активный stream закрывается. После reconnect при неизменившемся epoch cookie может снова работать с новым точным ACK. Source commit/rollback меняет epoch, поэтому прежние sessions/tickets становятся недействительными.
- Bundle gate выполняется отдельным настоящим child-process запуском сгенерированного immutable runtime. SHA256 проверяется против release manifest до запуска; запускается точная копия этих байтов в отдельном temporary runtime directory. Используется минимальный environment без копирования токенов хоста, обновление отключено, stdout/stderr процесса не публикуются. Это запуск standalone artifact, а не установка службы на пользовательское устройство.

## Ограниченная матрица

| Группа | Обязательное наблюдение | Текущий статус |
| --- | --- | --- |
| A→B→rollback A | HTTP/WS достигают выбранного процесса; canonical, активный alias и grants сохранены; epoch новый, binding floor остаётся2 | PASS: composed actual network; bundle отдельно подтверждает canonical |
| Прежние допуски/потоки | Старые tickets/cookies/HTTP/WS прекращаются после commit; соседняя app продолжает работу | PASS: HTTP continuous response, настоящий WS, старые ticket/cookie и соседний WS |
| Fresh prepare и ACK | Reconnect/expiry не меняют текущий target; без ACK —503, нет upstream GET или переноса на старый порт | PASS; потерю owner во время HEAD проверил root отдельным signed HTTP test |
| Публичное согласие | Для нового источника и rollback нужен whole-port ack точной tuple; старое согласие не даёт записи | PASS: wrong tuple отказ без DB изменений; оба перехода используют точные acknowledgments |
| Потерянное уведомление | Commit остаётся committed; retry того же intent возвращает receipt без второй revision; historical receipt отличается от current | PASS: удержан настоящий binding-set, offline replay после нового перехода |
| Неверные/поздние кадры | Wrong-pin open, прежний ACK/remove и HEAD callback не попадают в неправильный upstream/current channel | PASS: real socket с адресными packet faults; ABA требует нового syncId |
| Локальный отказ | Correlated policy reject кандидата не снимает действующий источник; wrong-pin open не ломает соседей | PASS: заблокированный порт, неизменная БД, сосед работает на том же socket без restart |
| Временный legacy reconnect | Floor2 не передаётся v1, но транспортная недоступность не отзывает действующие права | PASS: v2→v1→audit→v2, та же cookie и unused ticket продолжают работать после нового ACK |
| HTTP адрес проверки | Unicode/dot/hash сериализуются как navigation; исходный entryPath сохранён; неретранслируемая длина отказывает до HEAD | PASS: пять настоящих HEAD request-target и Unicode overflow |
| Сгенерированный runtime | Настоящий child-process: local claim, signed operations, A→B→rollback, process restart/fresh proof, HTTP/WS | PASS: opt-in1/1, manifest version/SHA проверены; raw burst/tamper matrix в bundle отдельно не повторена |

Полную историческую schema/CAS/retention матрицу C2-A заново не дублируем. Подписанный async adapter, интерфейс C2-C, DNS/TLS, физические устройства, backup/restore и фактический Apps4/v2 fallback учитываются отдельными gates.

## Preflight findings

1. Одного удаления fragment перед HEAD недостаточно. Фактический Node24 HTTP подтвердил `ERR_UNESCAPED_CHARACTERS` для `/страница?ключ=чай#раздел`, literal dot-segments и `#` у старого raw probe. Согласована последовательность: проверка runtimePath исходной строки → `new URL(entryPath, loopbackOrigin)` → `pathname + search`; исходная строка остаётся в immutable digest. Пять локальных примеров совпали с WHATWG fetch; браузерный результат не заявляется.
2. Сериализация Unicode увеличивает длину: допустимая исходная строка может превысить8192 в HTTP request-target. Дополнительная проверка сериализованного пути согласована в wire contract; correlated `invalid_app_path` не называется сетевой недоступностью и не подтверждает неретранслируемый источник.
3. Format Apps4 не означает v2 runtime. C2-A отказ head2 должен заменяться только exact ACK admission, а не обходом floor или fallback по appId. В source rollback target1 floor остаётся2.
4. Во время финального review publishing_architecture воспроизвёл временный v1 reconnect под той же identity: общий audit удалял действующую cookie из-за несовместимого транспорта при неизменном epoch. Root разделил retention прав и admission runtime: `retainAccess` проверяет только актуальную DB authority, request/open/stream сохраняют binding floor. Постоянная независимая регрессия подтверждает503/отсутствие upstream во время v1 и работу **той же** cookie и unused ticket после v2 fresh ACK. Снижения floor нет.

## Read-only review

Прочитаны frozen runtime manager, connector, gateway wiring и узкие изменения observer/inspection/HTTP composition. Проверены захват реального channel и binding reference, invalidation до callback, exact pins, conditional remove, deadline без продления, повторная проверка после await, source receipt replay отдельно от current, отсутствие HTTP очереди до ACK и сохранение независимых приложений. Новый consequential blocker после исправления retention не найден.

Наблюдение `responding` по-прежнему включает HTTP404. Ни config ACK, ни ответ HEAD не удостоверяют содержимое, доступность всех маршрутов или полезность программы. Переключение выбирает процесс/порт; не копирует БД, секреты или файлы и не откатывает выполненные действия.

## Команды и результаты

Windows, Node24.13.1. Независимая composed fixture не импортирует авторские fixtures, использует реальную SQLite и actual connector.

```text
node --test modules/apps/test/source-runtime.acceptance.test.mjs
9/9 PASS, 0 fail, 0 skip

node --test modules/apps/test/source-runtime.acceptance.test.mjs modules/apps/test/runtime-bindings.test.mjs modules/apps/test/connector-binding.test.mjs
47/47 PASS, 0 fail, 0 skip

node --test modules/apps/test/app-settings.acceptance.test.mjs
14/14 PASS, 0 fail, 0 skip

$env:SOTY_APPS_BUNDLE_TEST = '1'
node --test modules/apps/test/source-runtime-bundle.test.mjs
1/1 PASS, 0 fail, 0 skip
```

47 содержит те же9 независимых cases и повторенные14 manager +24 connector cases; это не дополнительные47 независимых сценариев. Default bundle command без opt-in честно даёт1 SKIP, а не прохождение artifact gate. В opted run использованы версия1.4.0,189731 bytes и SHA ниже.

Узкое обновление C1 fixture сохраняет прежние проверки безопасности. Два exact evidence assertions теперь ожидают `connector-v2-observation` от нового actual connector. Unsafe legacy entry не ждёт невозможного HEAD: явно проверяются connected,0 probes, `unknown/not-observed`, HTTP503, null share links/no preview и возможность emergency restriction. Сценарий не отключён, alias/grant/CAS assertions сохранены.

Первый незамороженный прогон composed fixture выявил её собственную гонку ожидания старого channel при принудительном stop/start; это не объявлялось product finding. Финальный wrong-pin test дополнительно усилен: он вообще не перезапускает connector и доказывает работу соседнего app на том же соединении.

## Зафиксированные SHA256

| Файл | SHA256 |
| --- | --- |
| `modules/apps/server/index.mjs` | `2c33d937ae3c975d6d8c1ba5f4e8f60ed9f43e1644f346d8557c3e80d204fa23` |
| `modules/apps/server/runtime-bindings.mjs` | `d2af0fe49040d00e0f65e1c7a1eb980773c1ebed8ea5c25a445bf0a63a9c0b2e` |
| `scripts/agent-modules/local-apps.mjs` | `0cc9657750c987687fc49d35f592c3e38c164214e9fb08df10d561c41eb14ec4` |
| `scripts/soty-connector.mjs` | `1aed6979fc50a4aa855497cd5b69e5e0e869466b20583e853dc2ec38ed567891` |
| `server/http-app.js` | `dfb3df19e03ac5a50d9c04a2b61ae227900d97e64b72597faec14ef288d02d32` |
| `modules/apps/server/inspection.mjs` | `321453b0be7ed7b817959f349ad364a15a506f9b4149cfe44a977f599acac01d` |
| `modules/apps/server/source-observation.mjs` | `7cfbf508f040da42ce78e9817f686ae503ba230953237cbfe85fc430a80e0482` |
| `modules/apps/test/source-runtime.acceptance.test.mjs` | `3b2c7a896017dafa77fe335df17263d1d219e5ca953ac7450930f06fe5f369c3` |
| `modules/apps/test/source-runtime-bundle.test.mjs` | `fc7ece090a3f0779a1e32556850633da6f3f342b9c67e420996a18ade30ea0ad` |
| `modules/apps/test/app-settings.acceptance.test.mjs` | `c2fa64d451c156bb5014be1626e17ecd22540123b77687cb3ccc66d11182dfad` |
| `public/agent/soty-connector.mjs` | `f4ba827e556a21db97d6ac073272eaa293fde07f151ceb9c9b81b7814836c5ac` |
| `public/agent/manifest.json` | `3f2c6d5c82fefd002f3c743083bde7baaff52412ed1eacc7c8f0869415944609` |

## Доказательства и вердикт

В пределах описанного **локального C2-B gate блокеров не осталось**. Отдельный bundle case подтверждает действительный standalone процесс и подписанный HTTP путь; полная packet fault/100-control/late callback матрица проверяет runtime source, а не повторяет каждый fault внутри artifact process. Это разделение сохранено, без заявления полного равенства сред.

Общий world/build и release selftests выполняет root; этот документ не подменяет их результат. C2-C UI, браузерное переключение источника, два физических устройства, Linux/service-manager rollout, DNS/TLS, backup/restore, отсутствие внешнего writer и проверенный production Apps4/v2 fallback остаются самостоятельными gates. Запуск temporary child process не доказывает надёжность установленной службы после перезагрузки ОС. Никакого production действия, inference, удаления пользовательских данных или повторения ранее запрещённой cleanup в этой приёмке не было.
