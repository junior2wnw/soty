# P4: bounded Docker witness stream — preflight

Статус: SOURCE-ONLY / NOT RUN. Это подготовка будущего full-image witness; не реализация R1a/R1b, не доказательство Linux execution, image admission или restore.
Проверены существующий `deploy/connector/docker-api.mjs` и первичные API/source ниже. Код, image plan и PROGRESS не изменялись; runtime/tests/SSH не выполнялись.

1. [Docker Engine API v1.45](https://docs.docker.com/reference/api/engine/version/v1.45/) и [точный OpenAPI Moby v26.0.0, basePath /v1.45](https://raw.githubusercontent.com/moby/moby/v26.0.0/api/swagger.yaml).
2. [Moby v26.0.0 exec HTTP handler](https://raw.githubusercontent.com/moby/moby/v26.0.0/api/server/router/container/exec.go) и [daemon exec lifecycle](https://raw.githubusercontent.com/moby/moby/v26.0.0/daemon/exec.go).
3. [Docker CLI v26.0.0 start/attach ordering](https://raw.githubusercontent.com/docker/cli/v26.0.0/cli/command/container/start.go), [attach/backpressure](https://docs.docker.com/reference/cli/docker/container/attach/), [logging driver none](https://docs.docker.com/engine/logging/configure/).
4. [Node 24.21.0 ClientRequest upgrade/socket/head](https://nodejs.org/download/release/v24.21.0/docs/api/http.html#event-upgrade).

`DockerApi.request()` целиком буферизует ответ до 4 MiB, имеет обычный request deadline и не обрабатывает upgrade; это не transport живого потока.
`helperOutput()` читает `/logs`, затем декодирует уже накопленный ответ; при `LogConfig:none` этот источник отсутствует. Существующий buffered exec status probe в `deploy/connect/host-controller.mjs` не доказывает требуемую streaming/loss-ACK семантику.
Минимальная дельта в будущем: один incremental Docker-frame decoder и узкая HTTP upgrade обёртка для двух разрешённых операций; journal/ownership остаются в существующем controller. Новый SDK/framework не нужен.

До exec по полному owned app ID повторно сверить image, labels, mounts, Running и Tty=false; entrypoint приложения остаётся штатным.
В существующий защищённый journal до CREATE записать intent и hash точного spec: прямой `node /run/owned-witness.mjs <nonce>` из pinned RO mount, согласованный User, без shell/секретов в argv.
Один POST `/v1.45/containers/<ID>/exec`: AttachStdin=false, AttachStdout=true, AttachStderr=true, Tty=false, Privileged=false.
После 201 проверить и сохранить exec ID; GET `/exec/<ID>/json` должен связать ContainerID, command, user и TTY с intent. Затем записать START intent.
Один POST `/exec/<ID>/start` с `{Detach:false,Tty:false}` и upgrade headers; обработать 101 либо предусмотренный 200 на том же запросе, без повторного запроса для смены режима.
Node `upgrade` передаёт уже прочитанный `head`: декодировать его перед последующими socket bytes. Для обычного 200 использовать тот же decoder; остальные ответы дают bounded fixed error без raw Engine body.

200/101 подтверждают только HTTP handshake: в Moby v26.0.0 заголовки отправляются до ContainerExecStart, поздняя ошибка запуска может попасть в framed stdout.
EOF также не является успехом. Нужны целый единственный allowlisted JSON receipt с текущим nonce, корректный конец frames и последующий exec-inspect с точными ID, Running=false, ExitCode=0.
Receipt при ещё работающем exec, один ExitCode=0 без receipt, 404 после потери состояния или HTTP success с текстом ошибки не дают PASS.

Decoder хранит восьмибайтовый header и ограниченные output buffers; не накапливает массив объектов на каждый tiny chunk.
Проверяются reserved bytes, channel, uint32 BE payload length и остаток бюджета до выделения памяти. При stdin=false разрешены только stdout/stderr; неизвестный канал — отказ.
Лимитируются отдельно stdout, stderr и суммарные wire bytes, включая headers, чтобы пустые frames не обходили бюджет. EOF посередине header/payload — ошибка.
Предложенные output bounds: witness stdout 64 KiB, stderr 16 KiB; app collector 1 MiB суммарно. UTF-8/JSON receipt проверяется строго; raw stderr не выводится.
Handshake deadline отделён от конечного exec deadline. Тишина live app collector не должна срабатывать как обычный десятисекундный request timeout; его lifetime ограничен фазой controller.
Конкретные wire/deadline caps и error envelope фиксируются до будущей реализации; эти предложения не повышают принятые image/resource bounds.

Live app collector: POST `/containers/<ID>/attach?stream=1&logs=0&stdin=0&stdout=1&stderr=1` до START приложения; этот порядок есть в pinned Docker CLI.
Collector непрерывно читает тот же framed stream в bounded RAM, сохраняет только allowlisted projections и отбрасывает raw; LogConfig остаётся none.
Медленный attach способен тормозить stdout приложения. Переполнение или разрыв делают gate неполным, а не оставляют непрочитанный поток и не заменяются чтением `/logs`.
Предстартовая подписка и получение самого раннего boot output на exact Engine/image ещё требуют actual Linux проверки; чтение исходников этого не заменяет.

Потеря CREATE ACK: exec ID неизвестен, intent unresolved, CREATE не повторять. Не угадывать принадлежность по похожим командам/списку exec.
Потеря START ACK: можно читать inspect уже известного exec, но без полного receipt PASS нет; START не повторять.
Закрытие socket не считается остановкой процесса. HTTP handler использует background context; нельзя обещать kill-on-disconnect как API гарантию.
Самостоятельный конечный watchdog witness и cleanup уже owned app — отдельные подтверждаемые действия существующего controller; loss связи не доказывает cleanup или resource TTL.

Будущие пять causal gates:
1. Header/payload, раздробленные вплоть до байта, и непустой upgrade head дают точный output без потери/дублирования; обрезанный frame отказывает.
2. Oversize length, совокупный overflow и поток пустых frames отказывают bounded образом до неограниченного allocation/накопления; секретные canary bytes отсутствуют в public errors.
3. Успешный handshake, после которого запуск не состоялся/вернул Engine error, не даёт PASS; live LogConfig:none collector ловит ранний boot sentinel.
4. Полный receipt при Running=true или nonzero exit не завершает gate успешно; только receipt + фактическое завершение подтверждают результат.
5. Потеря CREATE/START ACK не вызывает повторную mutation и не объявляет процесс завершённым; journal сохраняет unresolved identity/phase до read-only reconciliation или подтверждённого cleanup.

Граница доказательства: документированные API плюс конкретный Moby/CLI v26.0.0 source; это не проверка версии/поведения фактического daemon. Недокументированных гарантий идемпотентности, восстановления потерянного exec output или отмены процесса не предполагается.
