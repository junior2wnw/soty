# P3-A3 — допуск запуска по форматам Rooms и Apps

Дата: 2026-09-30. Основа: checkpoint `60bd426`. Реализация и локальные проверки завершены; независимый review и внешние image/canary-условия отмечаются отдельно. Production, DNS, сертификаты и named runtime этим этапом не изменяются.

## Что допускается

Один источник правил — `deploy/connector/storage-guard.mjs`. Обе существующие схемы выкладки (`deploy/connector/rollout.mjs` и `deploy/connect/host-controller.mjs`) уже вызывают его перед START, разрешением автоматического перезапуска и возвратом старого образа. Их рабочий код в A3 не менялся. Изменены shared guard/probe, Dockerfiles, документация и тесты.

Manifest реального immutable image имеет **точно** структуру:

```json
{"version":2,"readers":{"rooms":[1,2],"apps":[1,2]}}
```

Для каждого хранилища допускается непустой уникальный subset `[1,2]`; оба поля обязательны. Это позволяет корректно отказать образу, который читает комнаты v2, но только Apps v1. Расширенные/неизвестные manifests и прежний rooms-only manifest v1 отклоняются. Метка контейнера не используется как доказательство: guard читает метку самого образа по точному image ID. Метка остаётся декларацией проверенного образа, а не доказательством поведения неизвестного кода.

Этот manifest ставит **корневой полный Dockerfile**, который переносит server/modules/contracts/dependencies из единой проверенной сборки. Независимый reviewer нашёл ошибку первого варианта A3: исторический `deploy/connector/Dockerfile` копировал только новый server, сохраняя старые modules/deps из BASE_IMAGE, и потому не мог честно объявлять Apps `[1,2]`. Одного наследования reader label также недостаточно: формат БД не подтверждает совместимость module API и зависимостей с новым server. Исправление по согласованному решению — ранний неизбежный RUN failure этого overlay-рецепта до любых COPY/LABEL с инструкцией использовать root full application Dockerfile. Собственная reader label удалена. Новый механизм аттестации и будущая совместимость overlay не обещаются; rollout проверенного полного image остаётся доступен.

Результат probe имеет точно поля `ok`, `schema`, `rooms`, `apps`:

```json
{"ok":true,"schema":"soty.storage-format.v2","rooms":2,"apps":2}
```

Каждое значение хранилища — `"empty"`, `1` или `2`. START receipt имеет точно поля `schema`, `containerId`, `image`, `mountSha256`, `rooms`, `apps`; schema — `soty.storage-start.v2`. Внешние пути, имена приложений, ACL, тексты, токены и конфигурация в receipt не попадают. Нет автоматического преобразования старого probe output или pending START receipt в новый формат.

## Независимое чтение формата

Probe передаётся как проверенный исходник host controller закреплённому helper image. Он импортирует только `node:fs/promises`, `node:path`, `node:sqlite`, не приложение, не модуль Apps и не код кандидата.

`/data/apps/registry.sqlite` проверяется отдельно от `/data/rooms-v2.sqlite`:

| Apps | Marker | SQLite user_version | Требуемое представление |
| --- | --- | --- | --- |
| empty | отсутствует | отсутствует | `/data/apps` отсутствует либо это действительно пустой обычный каталог |
| 1 | `soty.apps-registry.v1` | 0 или 1 | `apps_meta`, `app_devices`, `local_apps`, `local_app_grants` и известные проекции |
| 2 | `soty.apps-registry.v2` | 2 | все таблицы v1, четыре domain-таблицы, известные проекции и `legacy_origin_template` |

Marker и user_version должны согласоваться. Неизвестные/недостающие таблицы, подмена таблицы view, нечитабельная проекция, отсутствующие или дублированные schema metadata отказывают. Это распознавание формата, **не** проверка всех строк, CHECK/UNIQUE/FK, всего B-tree или фактической семантики приложения; строгая проверка рабочего Apps schema остаётся у сервиса.

Нулевой/повреждённый основной файл, каталог вместо файла, symlink каталога Apps, основного файла или sidecar отвергаются. Отсутствующий main рядом с orphan `-wal`, `-shm`, `-journal` или любым другим файлом не считается пустым хранилищем. Пустой SQLite без marker также не считается пустым каталогом. Проверки не создают новый registry и не исправляют существующий.

Оба SQLite открываются `readOnly:true`, `query_only=ON` с read transaction и обычной обработкой WAL. `immutable=1` не применяется: committed migration может находиться только в WAL. Наличие Apps v2 не скрывает rooms v2 и наоборот. Будущий Apps v3 отказывает; он не объявлен поддержанным заранее.

## Запуск, потерянный ответ и восстановление

Guard сохраняет существующие барьеры: ровно один `DATA_DIR=/data`, полный обычный Docker local volume без driver options/subpath/вложенных mounts, проверка реального mountpoint и других Docker-писателей до и после probe. Неоднозначный CREATE/START helper не повторяется; проверяется его точная ранее записанная идентичность. Старый output остаётся ошибкой, а сохранённый helper не перезапускается ради нового ответа.

Перед внешним START/изменением restart policy записывается новый receipt. Host recovery отказывает старому/неполному receipt **до** примирения результата операции; наблюдение, что контейнер уже запущен, само по себе не повышает старую квитанцию до новой. Для действительного v2 receipt после проверки исхода требуется ещё свежая совместимость текущих данных. Повторный START для потерянного ответа не отправляется.

После миграции Apps v2 несовместимый Apps v1-only образ не получает ни START, ни автоматический restart, ни RW offline helper старого приложения. Последние SQLite bytes сохраняются. Журнал остаётся незавершённым, прежний контейнер — остановленным; это не выдаётся за успешное восстановление. Connector rollout дополнительно отказывает до legacy connector rollback helper.

## Локальные доказательства

Проверено Node v24.13.1 на Windows в `soty-platform`:

- Финальный прогон после исправления overlay: `node --test deploy/connector/*.test.mjs deploy/connect/*.test.mjs` — **152 теста, 150 PASS, 0 FAIL, 2 SKIP**.
- Первый skip — существующий opt-in Docker archive/foreign-owner тест. Второй — file-symlink проверка Apps: Windows не разрешил создание file symlink. Проверка directory junction и nonregular sidecar прошла. Linux file-symlink доказательство не подменяется этим результатом.
- Реальная Apps v1→v2 migration: основной файл побайтно остаётся v1, committed WAL уже v2, probe возвращает Apps 2; v1-only image declaration отклоняется. Приватное приложение, пустые grants и revision сохраняются. Probe не checkpoint-ит и не переписывает основной файл.
- Реальный committed future marker/user_version 3 в WAL при v2 main возвращает отказ.
- Host recovery тест создаёт настоящий Apps v2 registry после START кандидата, затем ломает readiness. Старый Apps v1-only image с подложенной новой **container** label не запускается; повторная recovery также не возвращает старый писатель и сохраняет bytes новой БД.
- Отдельно проверены old probe output, старые durable START/automatic-restart receipts, recheck после dropped START, отсутствие второго START, несовместимый Apps fallback в connector rollout и сохранение прежних rooms/mount/writer guards.
- Regression для overlay читает настоящую Dockerfile RUN instruction, проверяет отсутствие COPY/ADD/LABEL и выполняет её Node-команду локально: обязательный отказ с инструкцией полной сборки. Это не выдаётся за настоящий Docker build.

Тесты контроллеров используют synthetic Docker engine. Проверки SQLite используют настоящие локальные файлы/WAL. Эти классы доказательств не означают успешный Docker START или Linux read-only mount.

## Внешний gate до production

1. Проверенный и закреплённый host guard/probe с новым форматом должен быть установлен до нового перехода. Старый controller manifest v1 не знает Apps; он не должен оставаться допускающим путь обхода.
2. Нужны точные проверенные candidate **и совместимый fallback** image IDs, умеющие читать текущие rooms и Apps v2, включая приватные ACL и domain registry. Изменение label старой программы не делает её совместимой. Флаг, переписывание marker/receipt или откат данных не заменяют совместимый образ.
3. До первой Apps v2 записи — остановленный единственный писатель, подтверждённые отсутствие host/remote/alias писателей и действующий защищённый snapshot/recovery-процесс. Docker inventory не доказывает отсутствие всех внешних файловых писателей.
4. В отдельном разрешённом Linux/Docker canary нужны настоящий read-only volume + WAL/SHM, ownership/rootless профиль, main/sidecar file symlink отказы, exact-image compatibility и lost-response recovery. Доступ read-only SQLite к WAL зависит от реального профиля прав; невозможность прочитать данные должна закрывать запуск.
5. Будущий Apps v3 потребует отдельного reviewed reader/probe/image перехода. A3 его не разрешает.

## Независимая приёмка

Publisher выявил настоящий дефект: историческая backend overlay-сборка сохраняла modules/dependencies базового image, но объявляла поддержку нового Apps reader. После исправления неподдерживаемый путь отказывает до любых COPY/ADD/LABEL; используется полный корневой Dockerfile. Reviewer проверил frozen source, прежний полный deploy suite151tests (149pass/2skip) и окончательный focused34tests (33pass/1skip), без failures. Интегратор повторил окончательный полный suite: **152tests / 150pass / 0fail / 2skip**, exit0. Лог — `output/implementation-20260930/p3-apps-reader-root.log`.

Локальная A3 принята. Это не закрывает перечисленные выше image/Linux/restore условия. Отдельный Linux rooms canary относится к прежнему точному rooms-only probe и не выдаётся за проверку нового Apps source.
