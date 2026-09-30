# P3-C2-A — формат Apps4 и допуск восстановления

2026-09-30. Локальная реализация reader/deploy gate и авторская регрессия завершены. Основа исторических данных — C1 `914a91a07584c60bafa372763d873f1923d9987f`; новая DDL согласована с автором C2-A. Production, DNS, контейнеры и пользовательские данные этим этапом не менялись. Независимый root review и внешний release gate учитываются отдельно.

## Что именно допускает reader

Версии сообщения `soty.storage-format.v2`, квитанции `soty.storage-start.v2` и image manifest `2` сохранены. Независимые пределы хранилищ:

| Хранилище | Допустимо | Первый неизвестный формат |
| --- | --- | --- |
| Rooms | `empty`, `1`, `2` | `3` — отказ |
| Apps | `empty`, `1`, `2`, `3`, `4` | `5` — отказ |

Корневой полный `Dockerfile` объявляет:

```json
{"version":2,"readers":{"rooms":[1,2],"apps":[1,2,3,4]}}
```

Метка читается у фактического immutable image ID. Скопированная container label не делает Apps3 image совместимым с Apps4. Исторический `deploy/connector/Dockerfile` по-прежнему отказывает: сборка поверх произвольного старого backend не стала допустимой.

Apps4 требует согласованных `apps_meta.schema = soty.apps-registry.v4` и `PRAGMA user_version = 4`, прежних core/domain/publication проекций и двух новых:

- `app_source_heads`: `app_id`, `required_binding_version`;
- `app_source_receipts`: `account_id`, `request_key`, `intent_hash`, `app_id`, `committed_epoch`, `value_json`, `created_at`.

Два исторических immutable-target trigger сохранены. Ещё четыре распознаются только с согласованными именем, целевой таблицей и телом: `app_source_head_no_downgrade`, `app_source_head_no_delete`, `app_source_head_no_replace_downgrade`, `app_runtime_target_no_replace`. Первый также запрещает изменение `app_id`. Проверка не сводится к наличию имени: подмена тела/таблицы, пропуск или посторонний trigger/view отвергаются. В строковых литералах сохраняется регистр; нормализуются лишь SQL-регистр и пробелы вне них.

Trusted probe импортирует только встроенные модули Node. Он не импортирует candidate, Apps migration или fixtures. Read-only SQLite открывается с `query_only` и транзакцией чтения, без `immutable=1`: принятый WAL участвует в распознавании. Вывод содержит только версии формата, без accounts, проектов, ports, данных и секретов.

**Это проверка формата, а не всех строк или runtime.** Probe читает фиксированные проекции и известные определения trigger; он не проверяет каждый FK/index/receipt/digest и не ищет отсутствующую source head в каждой строке. Известная DDL с повреждёнными данными может дать `apps:4`; текущий Apps service обязан отдельно отказать при своей полной проверке. Отсутствующая head на повторном открытии v4 не может восстанавливаться как `1`. Эта семантическая проверка и floor2 runtime denial принадлежат отдельной приёмке C2-A.

## Исторические данные и подтверждённая сохранность

Новый `deploy/connector/apps-v3.fixture.mjs` содержит DDL из committed `914a91a`, использует прежний замороженный Apps2 fixture для неизменённых таблиц и не вызывает текущую миграцию. Все 23 исторических SQL-объекта отдельно сравнены с literal DDL указанного commit. Замена marker у современной базы не используется как исторический образец.

Регрессии на настоящих SQLite-файлах проверяют:

1. Читаемость Apps1 (`user_version 0/1`), Apps2, исторического Apps3 и нового Apps4 без переписывания основного файла и без утечки данных.
2. Миграции Apps1/2/3→4 с WAL и выключенным autocheckpoint. Основной файл побайтно прежний, свежий probe видит `4` в WAL, Apps3-only reader отказывает. После закрытия writer/reopen остаётся Apps4.
3. Побайтово одинаковые прежние SQL-определения и логически одинаковые старые строки. В v3 fixture сохранены public/listed policy, epoch4, whole-port consent target1, active alias, tombstone, grants, domain/publication receipts. Новые heads равны `1`, source receipts пусты. Переход не сбрасывает прежнюю публикацию в private.
4. Принятый Apps5 WAL поверх Apps4 main не игнорируется и даёт отказ. Маркер4 поверх неполной Apps3 DDL не считается корректным Apps4.
5. Отсутствующие таблицы/столбцы, marker/version mismatch, повреждённые guards, orphan journals, нестандартные файлы и junction отвергаются. Для намеренного создания повреждённого test fixture FK выключается только в соответствующей установке; рабочая миграция не ослабляется.

Отдельно исполнены **настоящие старые** `schema.mjs`, `protocol.mjs`, `domain-policy.mjs` из `914a91a`, сохранённые без редактирования исходников. На базе с main header3, принятым WAL4 и headfloor2 старый `migrateAppsSchema` отказал с `apps_schema_unsupported`; основная база и WAL остались побайтно прежними, транзакция не осталась открытой. Это проверка старого migrator, не запуск старого Docker image.

Квитанция, исторические source blobs и синтетическая база сохранены в `output/implementation-20260930/c2-old-apps3-reader-2Vbjek/`; `receipt.json` содержит commit и SHA256 каждого старого исходника. Никакие пользовательские данные туда не копировались.

## START, потерянные ответы и откат

`host-controller.mjs` и `rollout.mjs` не менялись: их существующие точки допуска используют обновлённый shared guard. Дополнительные тесты подтвердили:

- Настоящая SQLite Apps2/3→4 миграция во время synthetic candidate start, затем отказ readiness: старый Apps2/3 image не получает START, автоматический restart или свой rollback helper с RW mount. Повторная recovery сохраняет новую БД.
- Аналогичный отказ Apps3 image перед восстановлением в connector rollout; прежние Rooms и Apps1/2 переходы проверены вместе.
- Pending Apps3 START receipt при уже Apps4 не разрешает settlement. Повторно проверяются actual image и текущий формат, START не отправляется второй раз, неразрешённый intent сохраняется.
- Ранее исполненный, но оставшийся в журнале Apps3 probe helper сверяется один раз; затем выполняется новый probe. Старая квитанция не скрывает Apps4, старый helper не запускается повторно.
- Совместимый image при reconciliation получает свежий Apps4 допуск; неизвестный Apps5 не принимается даже при более старом валидном START receipt.
- Сохранены прежние проверки source pin, standard local volume, единственного `DATA_DIR=/data`, subpath/overlap, писателей до/после probe, exact helper identity, ambiguous CREATE/START и `restored` settlement.

Проверки Docker lifecycle используют synthetic engine; SQLite и WAL настоящие. Они не доказывают работу Docker daemon, Linux прав или удалённого volume. Лексическое сравнение Docker mounts не исключает произвольный host/remote writer. Старый уже запущенный Apps3 процесс нельзя остановить сменой marker: перед миграцией необходим действующий управляемый single-writer gate.

## Локальные результаты

Windows x64, Node `v24.13.1`:

```text
node --test deploy/connector/storage-apps.test.mjs deploy/connector/storage-guard.test.mjs deploy/connect/host-controller.test.mjs deploy/connector/rollout.test.mjs
116 tests; 115 pass; 0 fail; 1 skip; exit 0

node --test deploy/connector/*.test.mjs deploy/connect/*.test.mjs
164 tests; 162 pass; 0 fail; 2 skip; exit 0
```

Логи: `output/implementation-20260930/p3-apps-v4-reader-focused.log` и `p3-apps-v4-reader-author.log`. Пропуски прежние: Docker archive canary требует opt-in, file-symlink требует прав Windows. Junction/nonregular journal выполнены. `git diff --check` для изменённых reader/deploy файлов прошёл.

SHA256 локальных файлов в момент прогона; Git-перенормализация CRLF/LF может изменить файловый hash:

| Файл | SHA256 |
| --- | --- |
| `deploy/connector/storage-probe.mjs` | `28b2eb263670a3fc3241b290cdd70009b3e33f19db485f7d85a3ae514a9bfe2e` |
| `deploy/connector/storage-guard.mjs` | `a863e8dce065cb2b8fcf399d0023cb4b0843650041004aaf37173b39991bd69a` |
| `deploy/connector/apps-v3.fixture.mjs` | `0c7d141b48397b790abb98ce90118c9e574c2376410ec43a2dd6cac8fa49fe26` |
| `Dockerfile` | `037df02281474663026e3d371fdf72411d3d95f7b6ea1cf867e5ddab32602da3` |

## Что остаётся обязательным до выпуска

1. Независимо принять модель Apps4 и обязательный отказ C2-A runtime для `required_binding_version=2` на launch/session/HTTP/WS, включая исключение из legacy sync. Promotion API C2-A остаётся недоступным; эта стадия не выдаёт v1 за pinned v2.
2. **Поддержка чтения Apps4 не равна рабочему откату source-v2.** Ранний Apps4 binary может корректно отказать на head2 и сохранить данные, но не обслужить такое приложение. До первого production head2 необходим отдельно протестированный v2-capable Apps4 fallback, exact image IDs и receipt его реального запуска. Повышение label или изменение marker не заменяет это доказательство.
3. До первого START мигрирующего Apps4 image обновить и закрепить trusted host guard/probe. Проверить актуальные cold encrypted backup/isolated restore, штатный local volume, одного управляемого writer и совместимый fallback на тех же новых данных.
4. Выполнить отдельный Linux canary точного probe/runtime/image с реальными RO volume, WAL/SHM и правами. Прежние Rooms/Apps3 результаты не доказывают Apps4; Docker build здесь не выполнялся.
5. Не откатывать БД поверх новых writes. Source rollback выбирает прежний immutable target с новым подтверждением, epoch и sticky bindingfloor2; он не откатывает данные приложения, browser storage или уже выполненные действия.

Это ограниченный локальный C2-A checkpoint, не готовность всей смены источника, public runtime или production release.
