# P3-B1 — допуск запуска с Apps v3

2026-09-30. Локальная реализация, авторская регрессия и независимая приёмка пройдены. Основа — принятые A3 `b4b5200` и контракт публикаций `635022f`, затем зафиксированная автором B1a DDL. Изменения этого этапа не запускают образ, не публикуют приложения и не меняют production.

## Контракт и граница изменения

Форма сообщения остаётся `soty.storage-format.v2`, форма подтверждения START — `soty.storage-start.v2`, версия image manifest — `2`. Это версии протокола, а не предел версии каждой БД. Распознавание теперь имеет независимые списки:

| Хранилище | Пустое | Известные версии | Первый неизвестный формат |
| --- | --- | --- | --- |
| Rooms | `empty` | `1`, `2` | `3` — отказ |
| Apps | `empty` | `1`, `2`, `3` | `4` — отказ |

Полный корневой Dockerfile объявляет `io.soty.storage.readers`:

```json
{"version":2,"readers":{"rooms":[1,2],"apps":[1,2,3]}}
```

Оба списка обязательны: каждый — непустое множество только известных версий своего хранилища. Отсутствующие, дополнительные или неверно типизированные поля не допускаются. Manifest читается из фактического immutable image ID; копирование метки в container config не подтверждает reader. Apps2-only image допускает Apps2, но отказывает на Apps3. Rooms3 не становится допустимым из-за появления Apps3.

Сохранён отказ исторического `deploy/connector/Dockerfile`: старый backend overlay не переносит согласованные modules/dependencies и не может объявлять новый reader. Поддерживается полная единая сборка корневым Dockerfile. Изменение LABEL само по себе не доказывает, что неизвестный или непроверенный образ читает данные правильно.

## Независимое распознавание Apps v3

`storage-probe.mjs` по-прежнему использует только встроенные модули Node. Он не импортирует код candidate, миграцию приложения, авторский fixture или пользовательские строки SQL. Открывается обычный read-only SQLite с `query_only` и транзакцией чтения; `immutable=1` не используется, чтобы не пропустить принятый WAL.

Apps3 требует одновременно `apps_meta.schema = soty.apps-registry.v3`, `user_version = 3`, все прежние core/domain таблицы, строковый `legacy_origin_template` и четыре новых читаемых набора столбцов:

- `app_runtime_targets`: app, revision, owner, connector, port, entry path, profile, digest, created time;
- `app_publications`: app/owner, launch policy, listed, policy epoch, active target, exposure acknowledgement, updated time;
- `app_publication_domains`: app, domain, owner;
- `app_publication_receipts`: account/request, intent hash, app, committed epoch, value, created time.

Распознаются ровно два новых trigger: `app_runtime_target_no_update` и `app_runtime_target_no_delete`. Проверяются не только имена, но и целевая таблица и точные замороженные определения с `RAISE(ABORT,'app_runtime_target_immutable')`; допускается лишь регистр SQL и пробелы вне строковых литералов. Подмена тела, другой target, изменение строкового литерала, недостающий или посторонний trigger/view дают отказ. В Apps1/2 triggers по-прежнему не допускаются.

Неизвестная версия, расхождение marker/version, отсутствующая таблица/проекция, нечитаемая БД, orphan journal или symlink не интерпретируются как пустое хранилище. Форматный probe не является полным `integrity_check`, проверкой всех constraints/indexes/строк или доказательством поведения runtime. Полное распознавание DDL и инварианты миграции остаются обязанностью Apps service и его отдельной приёмки.

## Исторический образец и реальные WAL-проверки

`deploy/connector/apps-v2.fixture.mjs` содержит независимый Apps2 DDL из `b4b5200`. Он не вызывает текущую миграцию и не переименовывает Apps3 в Apps2. Этот образец используют format tests и реальная SQLite-проверка восстановления host controller.

В tests на настоящих временных SQLite-файлах:

1. Historical Apps1 `user_version=0/1`, независимый Apps2 и Apps3 распознаются без изменения основного файла и без выдачи названий проектов, owners, devices или ports.
2. Миграции Apps1→3 и Apps2→3 выполняются с WAL и отключённым autocheckpoint. Основной файл остаётся побайтно прежним, его header всё ещё `1`/`2`; свежий probe видит `3` из принятого WAL. Apps2-only reader отказывает. После закрытия writer/reopen Apps3 остаётся читаемым.
3. До/после сравниваются прежние apps, devices, grants, domain heads/zones, aliases/tombstone и domain receipts. Начальная публикация `restricted`, `listed=0`, epoch1, target1; активных named domains нет.
4. При Apps3 main + принятом Apps4 WAL probe отказывает; основной файл не переписывается и WAL не игнорируется.
5. Негативные проверки охватывают markers, таблицы/столбцы, pinned origin, оба известных trigger, неизвестные объекты, nonregular files и orphan journals.

## Восстановление и прежние защиты

Рабочий код `host-controller.mjs` и `rollout.mjs` не менялся. Оба пути используют обновлённый shared guard.

- Реальный Apps2 registry мигрирует в Apps3 на этапе запуска synthetic candidate; затем readiness намеренно отказывает. Host controller не запускает исходный Apps2-only image, не выполняет его старые RW helpers и не возвращает restart policy. Повторная recovery сохраняет новую БД побайтно и также не запускает старый reader.
- Отдельный connector rollout тест сохраняет отказ Apps1-only при Apps2 и добавляет отказ Apps2-only при Apps3, до rollback helper и START.
- Pending START receipt с Apps2 не считается вечным допуском. При обнаружении Apps3 выполняется новая проверка actual image, Apps2-only image отказывает, pending intent остаётся, START не повторяется. Future Apps4 также не проходит свежую проверку.
- Rooms, exact DATA_DIR/mount, standard local volume, subpath/overlay refusal, проверки писателей до/после probe, source pin до recovery, single-shot helper и журналирование intents не ослаблены. Их прежние acceptance tests входят в общий прогон.

## Воспроизводимые локальные результаты

Среда: Windows x64, Node `v24.13.1`.

```text
node --test deploy/connector/storage-apps.test.mjs deploy/connector/storage-guard.test.mjs deploy/connect/host-controller.test.mjs deploy/connector/rollout.test.mjs
109 tests; 108 pass; 0 fail; 1 skip; exit 0

node --test deploy/connector/*.test.mjs deploy/connect/*.test.mjs
157 tests; 155 pass; 0 fail; 2 skip; exit 0
```

Пропуски: существующий Docker archive test требует opt-in; file-symlink test не имеет разрешения Windows. Junction/nonregular journal checks выполнены. `git diff --check` прошёл. Проверки Docker controller используют synthetic engine; WAL/SQLite — настоящие локальные файлы. Docker build и удалённый Apps canary в этом этапе не запускались.

Root повторил полный157-test набор:155 PASS,0 FAIL,2 тех же skips, exit0; лог `output/implementation-20260930/p3-apps-v3-reader-root.log`. Независимый `whole_product_critic` прочитал trusted probe/guard/recovery, отдельно сравнил13 schema objects historical fixture с b4b5200, повторил focused109 (108 PASS/1 skip) и дополнительные22 storage acceptance/rebase checks. Новых blockers не найдено; source hashes четырёх строк таблицы ниже совпали. Это локальная приёмка, не замена внешних условий.

SHA256 исходных локальных bytes в момент авторской регрессии (перенормализация CRLF/LF Git может изменить файловый hash):

| Файл | SHA256 |
| --- | --- |
| `deploy/connector/storage-probe.mjs` | `49af4dc637b6745820ce109cc21727c0c1ad638a2d50b33fbf5226a96b12b87b` |
| `deploy/connector/storage-guard.mjs` | `3bef97e6d5bcfabb53214c61c3c66bf9b9dc36f15bc647f715feca48fd43ddcb` |
| `deploy/connector/apps-v2.fixture.mjs` | `d67d60832417a76dbd0bcb6d71cab1b25d7736531497be5cb515beef2acf256b` |
| Frozen `modules/apps/server/schema.mjs` автора B1a | `85be313a1deea3a377abd3247227f2bca84207a460ab77f9e719426216c3b1ba` |

## До первого production START

1. Завершить независимую приёмку B1 и этого reader; закрепить проверенный host guard/probe и runtime helper до запуска мигрирующего приложения. Прежний rooms-only controller не должен оставаться допускающим обходом. Старый Apps2-aware guard Apps3 не распознаёт и должен отказать.
2. Проверить точные candidate и fallback image IDs, действительно поддерживающие Apps3 и текущий Rooms формат. Совместимый fallback готовится до миграции; старый Apps2 image не возвращается на новый volume. Нельзя чинить отказ сменой label/marker, старым JSON или откатом БД поверх новых writes.
3. Выполнить действующие cold encrypted backup и isolated restore gates, обеспечить один управляемый writer и штатный local volume. Лексическая проверка Docker mounts не доказывает отсутствие произвольного host/remote writer.
4. Отдельно согласовать и выполнить Linux Apps proof с точным новым trusted source, real read-only volume, WAL/SHM и реальным профилем прав. Он не заменяет restore production backup. Завершённый Rooms Linux canary относится к прежнему другому probe и не доказывает Apps3.
5. Учесть, что Apps startup мигрирует в v3 даже при выключенном public runtime. Этот gate нужен перед первым START, а не только перед включением публичных адресов.

Внешние image/Linux/backup/restore условия здесь остаются открытыми. Исторический отчёт A3 `p3-apps-reader.md` описывает Apps≤2; настоящий документ фиксирует отдельный переход до Apps3, не переопределяя прежние результаты.
