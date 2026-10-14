# Существующий клиент: совместимое обновление кода

CODE ONLY, base f7e58b0e. Не установлено, устройства/Source/permissions не выданы.

Существующий установщик теперь имеет узкий `-RuntimeOnly -RuntimeUpdatePlan FILE`
режим. `-CheckOnly` читает metadata и старый локальный health; изменений нет.
Он проверяет manifest/code/config/dependency SHA, собственные PID/start/owner,
parent worker, отсутствие Machine worker и pinned OpenCode из supervisor.
Node/OpenCode не скачиваются. Same-version 1.4.8 принимается лишь из явно
переданного pinned package, обычный сетевой updater по-прежнему отвергает mismatch.

Режим сохраняет native device/install/token, вложенную конфигурацию и старые
папки. Только после отдельного согласия применяются serverUrl текущих Сот,
добавление D:\roy и localJobReceiptsEnabled. Beforeimage config+supervisor
сохраняется DPAPI CurrentUser и проверяется расшифрованием. Публичные `.next`
стадируются до stop; `.previous` сохраняет прежний код. Запись issued эксклюзивна.
Перед stop повторно проверяется idle, точный process/config и существующий mutex.
Новые задания должны оставаться приостановленными до фактического readback.
Idle является снимком, а не admission barrier. Windows idle-stop не объявляется
graceful executor completion. Неизвестный результат не разрешает повтор установки.

Обновляется публичный код существующих CurrentUser и неработающего Machine
runtime, а также существующий ProgramData supervisor. Machine конфигурация,
задачи, worker и Node/OpenCode не меняются. Supervisor больше не переписывает
личную identity/code папкой Machine; новый OS-пользователь получает отдельную
непривязанную installation и не наследует Machine credentials/roots.

Root переносит только текущие owner fences b914: sourceActorDeviceId из Connect,
owned-device recheck при lease/status/finish, currentOwnerChecked, terminal
unknown resolution. Opt-in localExecutorCurrent остаётся OFF по умолчанию.
Для активации нужен его квалифицированный Root counterpart и действительный
Apps-owned device; локальная константа `true` не заменяет проверку владельца.
Чтение исходного task ID отделено от effects; cancel остаётся effect. F7 W2,
multi-app Source references, eight-step/one-effect и unknown rules сохранены.

Node22.12 без flags загружает exact bundled durable constructor (файловые
receipts, SQLite не используется), namespace files=0/start=0/model=0. Отдельный
SQLite-memory probe с --experimental-sqlite не служит этой qualification. Root
тесты запускались на локальном Node24.13.1; это targeted compatibility checks,
не stock 13-gate/image qualification Node24.15 production.

Transit Source CODE: createSelectedTransitSourceApplications объявляет ровно
current_context + transit.native.status READ, effects=[]. Native constructor
createOwnDeviceStatusNative проверяет настоящий SDK Root brand, exact owner,
первичный OS principal/device/install tuple и прежний binding до/после read.
createOwnDeviceStatusBff использует существующий SDK BFF и storage, без нового
listener/ключа/grant. Consumer вызывает qualified Droy readNativeLocalStatus.
Null/unavailable возвращает 503, а не ложный OFF. Invoke/ON/OFF/guest/roles нет.
Real RSA/JWKS/connector-MAC + Native runtime/metadata fixture без SQLite/flags
проверяет own READ и deny copied brand/foreign/reinstalled/write.

Это конструктор/descriptor, не смонтированный HTTP Source. Actual devVV пока
отсутствует в Root. Сначала нужен обычный owner claim в текущем Root profile;
затем actual owned app/profile/transport bundle и Native mounting/storage на
его существующем сервере. Только после этих реальных inputs возможны Source
registration/grants. .soty manifest/skill dbf5 и один HTTP200 не доказывают это.

Следующий human gate после review: обновление существующего CurrentUser клиента,
привязка к текущему профилю Сот и отдельный доступ к D:\roy. TUN/Transit ON/remote
не включаются. Claim — обычный одноразовый файл/Root UI, не Native SQL или
переиспользование чужого actor. При foreign owner остановиться.
