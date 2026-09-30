# P3 C2-B: connector binding runtime

Авторский ограниченный gate, 2026-09-30. Workspace: `soty-platform/соты`.
Основание: `p3-source-wire-contract.md`, C2-A checkpoint `efe0d9f`.

## Изменение и границы владения

Изменён только connector runtime `scripts/agent-modules/local-apps.mjs`;
добавлены `modules/apps/test/connector-binding.test.mjs` и этот receipt.
Server binding manager, signed API, SQLite model, UI и release builder принадлежат
другим участникам. Автор этого среза не менял их и не запускал публикацию.

`createLocalAppsRuntime(deps, options)` сохраняет прежний интерфейс
`{start(), stop(), claim(), status()}`. Dependency injection и отсутствие
внутренних imports сохраняют совместимость с однофайловой сборкой. Проверки
`instanceof` конкретной реализации WebSocket не появились. Используются обычные
`addEventListener`, `send`, `close`, `readyState`, `bufferedAmount`.
`probeLocalApp()` по-прежнему возвращает `Promise<boolean>`.

## Что обеспечивает runtime

- `auth` объявляет `[1, 2]`. Старый `ready` без нового поля выбирает режим1;
  `bindingVersion:2` требует корректный `channelId`. Неизвестный режим, повторная
  negotiation или смешение v1 controls с v2 закрывают это соединение.
- Каждое реальное соединение владеет собственными identity, binding map,
  streams, probes и preparations. Callback хранит context, а не обращается к
  переменной «текущий socket». Закрытие прежнего context не закрывает новый.
- `binding-set` проверяет форму target и SHA-256 по согласованному массиву
  `soty.runtime-target.v1` с identity именно этого соединения. Только затем
  применяются local port/path/profile ограничения. ACK означает установленную
  конфигурацию; он отправляется до HEAD.
- Повтор текущего `syncId` с тем же digest повторяет ACK/rejection без нового
  HEAD. Повтор с иной tuple — protocol fault. Remove действует только на точную
  текущую пару `appId/syncId`. Более новое согласование переживает поздний remove.
- Новая tuple сначала снимает старую binding этой app и закрывает её потоки.
  При допустимом envelope, но запрещённом port/path/profile возвращается
  `binding-rejected`. Старый порт не становится fallback; соседние apps работают.
- `bound-open` сверяет channel, sync и все pins до создания HTTP request. Порт
  берётся из установленной binding. Отсутствующие/несовпадающие pins дают cancel
  конкретного stream; не запускают запрос и не ставят body в очередь ожидания.
- После write/ACK и внутри response/upgrade/chunk callbacks повторно проверяются
  context, stream object и binding object. Отмена старого stream удаляет map entry
  только при совпадении объекта; позднее завершение не удаляет новый stream с тем
  же ID. Stream error закрывает свой поток, не все приложения.

Sticky binding floor и право пользователя остаются серверными обязанностями.
Connector не получает из собственного HEAD разрешение на публикацию или вызов.

## Prepare, наблюдения и ограничения

Prepare не меняет active map и не позволяет открыть candidate. Повтор nonce не
запускает второй HEAD; target с иным проверенным digest под тем же живым nonce
закрывает соединение. `target-rejected` отделяет local policy/busy от сетевого
`unreachable`. Disconnect и expiry отменяют принадлежащий context запрос.

| Объект | Предел |
| --- | --- |
| JSON frame | 72 KiB UTF-8 |
| Socket outbound buffer вместе с новым frame | 4 MiB |
| Текущие app configurations | 100 |
| Активные streams | 32; публичный резерв24 контролирует сервер |
| Настоящие незавершённые data write | 64; control handlers синхронны |
| Prepare HEAD | 4 на context, не более2 для одной app; без очереди |
| Завершённые и активные prepare nonce | 256 на context, срок30s от получения |
| Periodic HEAD | 4 параллельно, одна coalesced запись на binding |
| HEAD timeout / periodic interval | 3s / 20s |
| Chunk / HTTP request / HTTP response | 48 KiB / 8 MiB / 64 MiB |
| Initial response / chunk ACK / idle | 30s / 30s / 120s |

Уточнение cache согласовано с root отдельно от исходного wire preflight: записи
живут до30s без продления от повтора; при256 возвращается busy, живой nonce не
вытесняется. Хранятся digest, pins и небольшой результат, исходный большой path
завершённой проверки не удерживается. Четыре in-flight HEAD учитываются отдельно.
Cache не является server proof и не продлевает серверные deadline/TTL. Для
отклонённых binding configurations также сохраняются только pins и ответ.

Синхронные control handlers не занимают async counter. Очередь periodic checks
ограничена текущими bindings; интервал не создаёт перекрывающиеся бесконечные
обходы. Изменённый binding удаляется из очереди, его выполняющийся HEAD отменяется.

HEAD проверяет исходный navigation path, затем отправляет только
`new URL(originalEntryPath, loopback).pathname + .search`; сериализованный
результат повторно проходит тот же path/8192 limit. Оригинальная строка target
и digest неизменны. Hash не отправляется в HTTP, Unicode сериализуется,
query order и `%2B/%23/%2F` не пересобираются. Reserved/normalized paths не
вызывают HTTP. В запросе нет cookies/authorization, `agent:false`, redirects
не следуются. 200–499 — ответивший endpoint, 500–599/transport failure —
unreachable. Это не проверка исходного кода, результата бизнес-функции или
готовности всего приложения.

## Авторская приёмка

Проверено в Node24.13.1 на Windows. Последний совместный запуск:

```text
node --check scripts/agent-modules/local-apps.mjs
node --test modules/apps/test/apps.test.mjs modules/apps/test/app-runtime.test.mjs modules/apps/test/connector-binding.test.mjs
git diff --check -- scripts/agent-modules/local-apps.mjs modules/apps/test/connector-binding.test.mjs
```

Результат: syntax/diff checks PASS; **36/36 tests PASS, 0 skipped, 0 failed**.
Из них24 новых авторских connector tests и12 существующих Apps/runtime tests.
Обычное предупреждение Node о статусе SQLite не является пропуском проверки.

Новые проверки включают:

1. Настоящие HTTP/WS: legacy negotiation; v2 HTTP bytes и WebSocket с
   Unicode/query; два upstream; отказ одной app закрывает её WS, соседний
   продолжает echo. Wrong pins не достигают HTTP.
2. **R1:** настоящий WebSocket parser получает подряд100 `binding-set` с
   entryPath длиной8192. Получены100 ACK, context остаётся connected.
   Preflight старого runtime воспроизводил разрыв на100 controls из-за
   promise counter; новый тест требует точное число ACK, не просто reconnect.
3. **R2:** управляемые реальные runtime callbacks удерживают старый HEAD,
   затем заменяют соединение; старый ответ не появляется на новом socket.
   Собственный HEAD нового context возвращает его pins. Аналогично проверена
   смена binding без reconnect.
4. Deterministic callback barriers: старые write completion, HTTP response,
   upgrade, request error, ACK и close не меняют replacement stream/context.
   Эти тесты используют injected socket/request, не выдаются за real network.
5. Fixed digest vector и tamper app/owner/port/path/profile/revision/connector
   identity. Same sync/nonce с иной tuple, wrong channel, unsupported ready,
   legacy controls в v2 — отказ до untrusted routing.
6. Transient prepare и repeat; limits4/2/256; expiry; реальный3s HEAD timeout;
   cancellation/late result; isolated profile/port rejection; queue bound4;
   UTF-8 frame size и outbound backpressure.
7. Пять browser-equivalent HEAD paths: Unicode, literal и encoded dot segments,
   hash SPA, query с escape. Serialized overlength и три reserved/normalized
   отрицательных пути не создают HEAD. Проверены200/401/404/302/500 и отсутствие
   redirect follow/credential forwarding.

Существующие tests дополнительно прошли с текущей интеграцией: authenticated
Apps access, assets/POST/WS, revoke/owner demotion, bounded streaming, offline
state, registration replay, named publication, session deadline и shell CSP.

## Freeze и следующие gates

Source SHA-256:
`0cc9657750c987687fc49d35f592c3e38c164214e9fb08df10d561c41eb14ec4`.

Author test SHA-256:
`8f70dbef92dc5b2536305450916d744729f65de31cba1a2d2df4815e4201c995`.

Это author source freeze. Root и независимый reviewer уведомлены; изменения
после него требуют конкретного finding и повторной затронутой приёмки.

Этот receipt **не закрывает** независимый full source-switch/signed API gate,
поведенческую проверку generated immutable bundle, production TLS/DNS,
compatible fallback image/backup/restore или пользовательский browser C2 UI.
Root собирает bundle отдельно; production не изменялся этим срезом.
