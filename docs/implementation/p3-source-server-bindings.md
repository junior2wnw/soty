# P3-C2-B — серверная привязка приложения к socket

2026-09-30. Авторский manager: `modules/apps/server/runtime-bindings.mjs`. Интеграция HTTP/Connect, connector runtime и их независимые сетевые проверки принадлежат отдельным срезам C2-B. Этот отчёт не объявляет их пройденными.

## API для интеграции

`createRuntimeBindings({channels,send,now,blockedPorts,onBindingInvalidated,timers?})` использует фактическую карту аутентифицированных channels. Канал содержит `{key,identity,ws,bindingVersion,channelId,observations,streams}`. `identity` содержит подтверждённые `linkId/hostDeviceId/connectorId`. Режим и channelId после аутентификации не меняются. `send(channel,frame)` обязан отправлять через переданный socket и синхронно вернуть literal true; Promise не является подтверждением отправки. `onBindingInvalidated(channel,appId)` синхронно закрывает только потоки указанного приложения.

Возвращаемые методы:

- `sync(channel,targets)`: текущие immutable camelCase targets с внутренним `connectorKey`. В wire target он не попадает. Метод рассчитан на negotiated v2; legacy floor1 отдельно обслуживает host.
- `handleFrame(channel,frame)`: синхронный обработчик пяти входящих typed controls; true означает распознанный control, false — другой тип для host dispatcher. Ошибка текущего envelope/pins вызывает protocol failure. Корректный поздний syncId/nonce безопасно игнорируется.
- `requireBinding(channel,decision)`: проверяет актуальный channel, exact desired target и ACK. Возвращает frozen, WeakMap-branded reference `{channelId,syncId,appId,revision,digest,profile,connectorKey}`.
- `assertBindingCurrent(channel,reference)`: проверяет тот же socket/context и тот же живой ACKed entry. Копия reference не проходит. Host вызывает после каждого await и перед отправками существующего stream.
- `openPins(reference)`: после повторной проверки возвращает только `{channelId,syncId,appId,revision,digest,profile}` для `bound-open`.
- `getState(channel,appId)`: `{state:'pending'|'bound'|'rejected'|'unavailable',reason?}` для согласования и owner read-model. `bound` означает configuration ACK, не ответ HTTP.
- `prepareTarget/verifyPreparedTarget`: callbacks, совместимые с `createSourceRegistry`; evidence остаётся непрозрачным объектом.
- `drop(channel)` и идемпотентный `close()`: снимают admission, таймеры и pending. Drop прежнего channel не удаляет заменивший его объект из host-карты.

`timers` — узкая optional test seam с обычными `setTimeout/clearTimeout` по умолчанию. Это не отдельный scheduler и не очередь.

## Границы допуска

При каждом чтении reference/proof сравниваются actual `channels.get(key)`, захваченный channel и ws, readyState, неизменные negotiated mode/channelId/identity. Поздний handler прежнего socket не может создать state для нового соединения. Обработка old socket возвращает распознанный no-op ещё до доступа к replacement observations.

Manager проверяет exact target digest общей `runtimeTargetDigest`; connectorKey выводится из своего channel. Он не заменяет branded AccessDecision: host по-прежнему заново проверяет политику, владельца, grants, floor и срок. `requireBinding` не применяется к legacy floor1; floor2 с mode1 всегда отклоняется host/manager.

Основные ошибки: `app_binding_pending`503 до ACK/после deadline; `app_source_protocol_required`503 при неподходящем протоколе; `app_offline`503 при отсутствии/замене соединения; `app_binding_changed`503 для ранее выданного, но уже недействительного reference. Temporary offline не является самостоятельным отзывом сохранённой cookie; host разделяет проверку прав и готовность транспорта.

## Согласование без гигантской конфигурации

Один channel хранит не более 100 desired entries. Входной snapshot полностью нормализуется до изменения карты. Изменённые и удалённые entries сразу теряют допуск и наблюдение; `onBindingInvalidated` закрывает их live streams. Затем отправляется один bounded conditional `binding-remove` и отдельный `binding-set` каждому изменившемуся app.

Неизменившийся ACK/reference сохраняется. Повтор pending set до deadline использует тот же syncId без продления пяти секунд. После expiry entry недоступен; следующий явный sync снимает прежнюю пару и создаёт fresh syncId. Старый ACK не восстанавливается при возврате A→B→A. Подтверждённый local rejection сохраняется до изменения tuple или reconnect; heartbeat не выдаёт отказ за успех.

Кадр не больше 72KiB UTF-8; сумма control отправки одного sync и фактический socket bufferedAmount ограничены 4MiB. Нет бесконечных retries, Promise очереди или ожидания пользовательского HTTP body. Отказ отправки/переполнение снимает весь captured context и завершает его pending, затем закрывает его socket. Успешная проверка лимитов не равна доставке: только exact ACK даёт допуск.

Unsafe исторический путь/blocked port отключает только соответствующую app с фиксированным reason, не удаляя owner metadata или соседние bindings. Digest/identity corruption — ошибка внутреннего target, не разрешение на другой порт. Raw handler не принимает неизвестные typed DTO поля или произвольный текст причины.

## Наблюдения отдельно от ACK

`binding-ack` не создаёт запись ready. `bound-observation` применяется только к текущему ACKed syncId и exact pins. 200–499 с `responding` дают legacy-compatible `{state:'ready',at,targetRevision,targetDigest,evidence:'connector-v2-observation',httpStatus}`; 500–599 либо null transport outcome с `unreachable` дают stopped. Несогласованные state/status — ошибка протокола. `at` задаёт сервер при приёме; wire timestamp не продлевает freshness. Expiry45s выполняет общий inspection read-model.

## Временное подтверждение кандидата

`prepareTarget` проверяет protocol2, текущий channel, actor owner и immutable target. На соединении не более четырёх pending; две проверки одного app имеют разные nonce и разные proof. Переполнение — `app_prepare_busy`429, без очереди. Actor/target копируются; изменения аргументов вызывающей стороны не меняют отправленный кандидат.

`target-prepare` не меняет desired/ACK map и не создаёт health observation для работающей версии. Deadline ответа — 5s, expiry proof — 30s от начала callback. Registry имеет собственный более ранний start/TTL и повторно проверяет его в promote. Успех200–499 сохраняет opaque WeakMap evidence с captured channel, tuple, actor, preparationId, signal и сроком. Wire nonce отдельно от registry preparationId.

`verifyPreparedTarget` синхронно проверяет всё перечисленное и возвращает boolean. Копия proof, чужое устройство браузера, изменённый target, abort, clock rollback, expiry, disconnect или replacement дают false. Проверка не отправляет сеть и допустима внутри короткой SQLite transaction.

Correlated `target-rejected` принимает только `invalid_app_port`, `invalid_app_path`, `unsupported_profile`, `app_prepare_busy`. HTTP500–599/null дают `app_source_unreachable`503; timeout — `apps_source_probe_timeout`504; явный abort — `apps_source_preparation_stale`409. Ошибка одной подготовки не снимает активную binding. Все pending при drop/close завершаются, поздние ответы не завершают новые ожидания. Завершённые proof хранятся слабой картой без отдельного списка/таймера на каждый результат; срока и принадлежности достаточно для отказа после expiry/drop.

## Авторская проверка

```text
node --test modules/apps/test/runtime-bindings.test.mjs
```

**14/14 PASS, 0 skips.** Проверены branded refs, pending/no health, unchanged ACK, retransmit/deadline, A→B→A и stale ACK, соседние apps, replacement до close, 100 максимальных путей и синхронный ACK burst, отказ101 до мутации, unsafe legacy path, port/profile/digest pins, текущие/stale observations, send failure/backpressure, две подготовки одного app, input capture, proof expiry/abort/reconnect, четыре pending и полное завершение timeout/drop/close, typed rejection и отрицательный HEAD.

Это deterministic manager-тесты с управляемыми часами и callbacks. Они не являются доказательством настоящего WebSocket burst, HTTP HEAD serialization, двух upstream, signed async Connect, браузера или собранного immutable connector. Эти проверки выполняются отдельно при общей приёмке C2-B. Производственные DNS/TLS, backup/restore и совместимый fallback не изменялись.
