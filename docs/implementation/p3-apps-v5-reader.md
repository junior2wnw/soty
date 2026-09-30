# P3-D1 — допуск Apps5 и сохранность при откате

2026-09-30. Локальный reader/deploy срез реализован после D0 `4fa5f722403dfbb1dc9fdf67965071a398c3537c` и согласованной DDL Apps5. Apps5 содержит **только сохранённые приложения**; обсуждения будут отдельным Apps6 в D2. Этот отчёт не объявляет D1 saved API, D2, production или внешний release gate принятыми.

## Область изменений

- Независимый trusted host probe: `deploy/connector/storage-probe.mjs`.
- Общая проверка image/probe/start receipt: `deploy/connector/storage-guard.mjs`.
- Метка полного корневого `Dockerfile`, актуальные сведения `deploy/connector/README.md` и `deploy/connect/CONTROLLER.md`.
- Связанные deploy tests, новый literal `apps-v4.fixture.mjs`, неизменённые исторические source blobs в `deploy/connector/fixtures/apps-v4/`, новый `storage-apps-v5.test.mjs`.

Apps service/schema, World fence, server wiring, UI, production и исторический worktree этим автором не менялись. `host-controller.mjs` и `rollout.mjs` тоже не менялись: тестируется использование ими общей обновлённой проверки. Исторический backend-overlay `deploy/connector/Dockerfile` остаётся запрещённым ранним отказом; новый reader label поверх произвольного старого backend не добавлялся.

## Контракт формата

Envelope probe `soty.storage-format.v2`, START receipt `soty.storage-start.v2` и версия manifest `2` сохранены. Пределы независимы:

| Хранилище | Принимается | Следующий формат |
| --- | --- | --- |
| Rooms | `empty`, `1`, `2` | `3` — отказ |
| Apps | `empty`, `1`, `2`, `3`, `4`, `5` | `6` — отказ |

Метка полного image:

```json
{"version":2,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5]}}
```

Для Apps5 одновременно требуются `apps_meta.schema = soty.apps-registry.v5` и SQLite `user_version = 5`. Прежние core/domain/publication/source проекции сохраняются. Добавлены следующие literal host-side проекции:

| Таблица | Столбцы |
| --- | --- |
| `app_saved_heads` | `account_id, revision` |
| `app_saved_entries` | `account_id, app_id, domain_id, origin, path, label, saved_revision, updated_at` |
| `app_saved_receipts` | `account_id, request_key, intent_hash, app_id, saved, committed_revision, created_at` |

Probe также требует точных имён, таблицы и нормализованного SQL тела трёх новых guards:

- `app_saved_head_no_downgrade`: запрет смены account ID и `NEW.revision <= OLD.revision`.
- `app_saved_head_no_delete`: счётчик аккаунта не удаляется.
- `app_saved_head_no_replace`: повторный INSERT существующего account не может обойти монотонность через REPLACE.

Сохраняются два immutable-target и четыре source/floor guards прежних версий. Отсутствие, подмена тела/таблицы/строкового литерала, изменение `<=` на `<` или посторонний table/view/trigger не выдаются за принятую форму. Нормализация SQL меняет только пробелы/регистр вне строковых литералов.

**Это распознавание формата, а не полная integrity-проверка.** Probe не проверяет все индексы/FK/CHECK, каждую строку, digest, квоту, owner или связь bookmark с доменом. В частности, два новых UNIQUE index входят в полную DDL service, но не являются отдельной host-probe аттестацией. Полный Apps recognizer и `validateSavedRows` обязаны проверять их и данные при открытии. Reader compatibility также не доказывает runtime binding-v2, доступность приложения или авторизацию saved API.

## Независимость probe и данные на диске

Probe импортирует только `node:fs/promises`, `node:path`, `node:sqlite`. Текущая миграция, candidate application source и исторические test fixtures не исполняются доверенным host probe. Image label читается у фактического immutable image ID; скопированная container label не делает Apps4 image совместимым с Apps5.

Остаются прежние условия: явный `DATA_DIR=/data`, стандартный управляемый local Docker volume, единственный writer, проверки writer/mount до и после probe. Открытие SQLite — `readOnly:true`, `query_only=ON`, read transaction и обычное чтение WAL. `immutable=1` не используется. Missing main с sidecars, нулевой/повреждённый файл, symlink/junction, неизвестный формат и смешанный marker/version дают отказ; `empty` применяется только к отсутствующей или действительно пустой области Apps.

Результат probe содержит только версии. Он не перечисляет аккаунты, сохранённые заголовки, домены, порты, получателей или тела receipts. RO доступ SQLite может использовать SHM locks/служебное состояние; локальный тест не обещает побайтовую неизменность SHM и не подменяет Linux mount-проверку.

## Исторический Apps4 и реальная миграция с WAL

`apps-v4.fixture.mjs` содержит literal Apps4 additions поверх независимых frozen Apps2/3 fixtures. Он не вызывает текущую миграцию и не переименовывает современный формат в старый. Все **30 исторических SQL-объектов** сопоставлены с настоящим Apps4 migrator после косметической SQL-нормализации.

Для повторяемого отказа старого кода сохранены точные Git blobs из `4fa5f722403dfbb1dc9fdf67965071a398c3537c`:

| Исторический source | SHA256 Git content |
| --- | --- |
| `schema.mjs` | `aa7a07bdfd0b59a38c7dd53f2c8fd4eb135cad64f85de6d80bd47474a54be5c3` |
| `protocol.mjs` | `29d36ce771c300f7dc3a00cbd16f98aed65c960cb5a5c6ee9673363e518ebd7e` |
| `domain-policy.mjs` | `53ca3d124570fe06f197d4f2e630e0220cc91719138166a3f6f4d6a06fcc246a` |

`fixtures/apps-v4/provenance.json` связывает commit, исходные пути и hashes. В тесте допускается только Git checkout CRLF→LF при проверке байтов исходников. Никакие функции миграции не переписаны для ожидаемого отказа. Это test-only старый migrator, **не старый запущенный Docker image**.

Проверенный сценарий:

1. Настоящий Apps4 файл содержит public/listed App, отдельный restricted App, revoked App, direct/community grants, active alias, tombstone, domain/publication receipts и source switch→rollback. У первого App активен original target1, но floor остаётся2; target2 и source receipts сохранены.
2. Включён WAL, autocheckpoint выключен. Миграция Apps4→5 выполняется настоящей новой `migrateAppsSchema`. Прежние SQL objects и строки сохраняются; saved tables изначально пусты — local pins не мигрируют в server save.
3. В Apps5 добавляются синтетические saved head/entry/receipt, включая exact query/hash path и личный заголовок. Это проверка хранилища, не signed API.
4. Основной файл всё ещё имеет header4. Новый RO probe видит committed WAL5 и возвращает только `apps:5`. Apps4-only image не допускается, текущий manifest допускается.
5. Новый отдельный SQLite connection вызывает настоящий старый Apps4 migrator. Он отказывает `apps_schema_unsupported` до DDL/BEGIN эффекта; транзакция не остаётся открытой. Основной файл и WAL побайтно совпадают до/после old migrator и probe. Сохранённые rows остаются.
6. После закрытия writer и нового открытия формат5, exact saved path, floor2 и target history сохранены. Текущий migrator сообщает `migrated:false`.

Дополнительно выполняются реальные Apps1/2/3→5 WAL миграции с сохранением прежних записей и public/private policy, Apps6 WAL поверх Apps5 main с отказом, отсутствие/переименование saved columns, fake marker5 поверх старой DDL4 и отрицательная матрица guards. Будущая схема6 не считается поддержанной потому, что она упомянута в плане.

## START, восстановление и старые receipts

Связанные tests проверяют:

- Настоящую SQLite Apps2/3/4→5 migration во время synthetic candidate start, затем readiness failure. Несовместимый original не получает START, automatic restart policy или legacy rollback helper; повторная recovery сохраняет текущую БД.
- Apps4-only original не восстанавливается после Apps5 migration в connector rollout, даже при новой скопированной container label.
- Pending Apps4 START receipt не разрешает settlement на Apps5: actual image и текущий формат проверяются заново, START не повторяется, unresolved intent остаётся явным.
- Завершённый старый Apps4 probe helper сверяется, затем нужен свежий probe. Он не перезапускается и не скрывает новый Apps5.
- Совместимый running image может подтвердить отложенный START свежим receipt; будущий Apps6, Rooms3, старый/расширенный envelope и rooms-only label остаются отказом.

Docker lifecycle здесь проверяется **synthetic engine**; SQLite/WAL настоящие. Лексическая проверка Docker mounts не исключает произвольный внешний writer. Старый уже работающий процесс не останавливается автоматически от нового marker — перед миграцией необходим действующий single-writer переход.

## Результаты локальной приёмки

Windows x64, Node `v24.13.1`.

```text
node --test deploy/connector/storage-apps-v5.test.mjs deploy/connector/storage-apps.test.mjs deploy/connector/storage-guard.test.mjs deploy/connector/storage-guard.acceptance.test.mjs deploy/connect/host-controller.test.mjs deploy/connector/rollout.test.mjs
134 tests; 133 pass; 0 fail; 1 skip; exit 0

node --test deploy/connector/*.test.mjs deploy/connect/*.test.mjs
175 tests; 173 pass; 0 fail; 2 skip; exit 0
```

Во втором запуске оба glob раскрыты в 13 test-файлов перед передачей Node. Лог: `output/implementation-20260930/p3-apps-v5-reader-deploy.log`. Пропуски: Docker archive canary требует отдельного opt-in; file symlink тест требует недоступного разрешения Windows. Junction и nonregular journal выполнены. Первая промежуточная source-policy assertion ошибочно совпала с поясняющим комментарием `immutable=1`; исправлен тест на фактическое RO opening, а продуктовый source не ослаблялся. Финальные прогоны выше зелёные.

`git diff --check` по изменённым Dockerfile/deploy файлам прошёл. SHA256 локального содержимого; Git CRLF/LF может менять физический hash после checkout:

| Файл | SHA256 |
| --- | --- |
| `deploy/connector/storage-probe.mjs` | `3ddcb0804d836713828338f5846067938b1b8398bbfdbbf951503b14b2c42873` |
| `deploy/connector/storage-guard.mjs` | `ad6b9e7108809ddb1c1174a4fb6e699c0f47a3a45edec2b50639d04bc812a08d` |
| `deploy/connector/apps-v4.fixture.mjs` | `b52e0bdd2e160173cb3fe63c4fa05c06e199cff12addee26a47435270f680ae3` |
| `deploy/connector/storage-apps-v5.test.mjs` | `160e3bf6a6c80924e5c3162b19ce9defaca5f0a427c06907dd8a50ec07924fd4` |
| `Dockerfile` | `a9b7517f21fcebc23a5cf3eba88c0e27a049c4b4528e0ec56e51060b719e7ea9` |

## Что ещё требуется до публичного выпуска

1. Независимо принять actual Apps5 service/saved authorization/fence/API и полный schema recognizer. Локальная reader-матрица не заменяет их.
2. Обновить и закрепить trusted host guard **до первого Apps5 START/write**. Подготовить настоящий полный image и отдельно проверенный Apps5-compatible fallback; оба обязаны обслуживать существующий binding-v2 floor2. Нельзя повысить label старому бинарнику и считать fallback готовым.
3. На синтетическом Linux volume проверить точные probe image/RO mount/WAL+SHM/права/symlinks и управление single writer, включая lost-response recovery. Прежние Rooms/Apps3/Apps4 canaries не доказывают Apps5.
4. Подтвердить актуальный зашифрованный backup и изолированный restore. Откат приложения сохраняет новые saved/source/domain данные; возврат к старому снимку данных — отдельное recovery решение, не автоматический способ заставить reader пройти.

Docker build, Linux Apps5 canary, работа production, DNS/TLS и реальный compatible fallback здесь **не выполнялись**. Это законченный локальный reader-срез, переданный root для независимого review и общего D1 checkpoint.
