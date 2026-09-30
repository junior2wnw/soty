# P4 storage: независимая интеграция Linux canary

30.09.2026. Журнал проверки [плана](p4-storage-linux-canary-plan.md), отдельно от application rollout. Итог: **review2 FAILED, адресная cleanup PASS; новый независимо проверенный review3 Linux PASS**. Каждая immutable программа отправлена ровно один раз. Это strict-v3 bridge для Notes1/Capabilities1, не reader2 или обновление рабочего приложения.

## Root review перед запуском

Просмотрены полный controller, generated writer/audit/negative/runtime programs, synthetic seeds и все 20 локальных сценариев. Mutation wrapper допускает только отдельный task volume и записанные точные container IDs; serving ID исключён из mutations. Перед START сверяются фактические mounts, effective environment, execution command и ограничения. При неоднозначном ответе выполняется inspect, исходная mutation не повторяется. Unresolved KILL остаётся неопределённым; остановка рабочего сайта или очистка по префиксу не предусмотрены.

Положительный probe использует точные bytes CLI; четыре отрицательных case используют тот же экспорт parser над фиксированными roots. Writer создаёт только синтетические данные и завершается внешним KILL после ready. OOMKilled должен быть false. Последующий audit сравнивает main/WAL hashes и доменные контрольные записи; SHM bytes не объявляются неизменными. Ни production volume, ни application credentials/config не монтируются. Ограниченный controller deadline не выдаётся за гарантированный TTL контейнера при потере хоста.

Root отдельно разобрал immutable bundle как текст/JSON **без выполнения** и подтвердил:

- controller prefix полностью совпадает с просмотренным source;
- единственный executable trailer совпадает с фиксированным шаблоном;
- embedded probe/guard/docker-api bytes совпадают с рабочими pinned файлами;
- fixture и generated-program hashes совпадают;
- bundle SHA256 `e2853c4882de7520949b3de9392a6c5037022e8aa18c35f0ce458d7f312c3927`, размер **162614 bytes**.

Авторские 20/20 local tests и их точная среда приведены в плане. Root не выдаёт просмотр этих tests за собственный повторный прогон. Независимое source approval [сохранено отдельно](p4-storage-linux-canary-review.md). Ниже — последующие реальные результаты.

## Review2: зафиксированный отказ и отдельная очистка

В15:03:57–15:04:00 UTC отправлен exact review2 `e2853c48…3927`, run `2abd529a43c1b4d8a8d68370a936b71c`. Actual runtime24.15.0/SQLite3.51.3/FTS5 прошёл; writer container создан, но его START **не отправлен**. Docker inspect вернул больше прежних64KiB, controller и cleanup отказали `docker_output_limit`. Получен полный failure receipt; повтор bundle не выполнялся.

GET-only reconciliation подтвердила actual writer `created`, нулевой StartedAt, Running/OOMKilled=false; runtime container exited0/OOMKilled=false. Ровно два owned container использовали только task volume. Writer inspect — **89487 bytes**: `Path/Args`42116 + `Config.Entrypoint/Cmd`42123 + оставшиеся поля. Прежний расчёт учитывал лишь одну копию команды. Serving ID/image/StartedAt не изменились. Чтение diagnosis допускало256KiB только внутри этой адресной read-only проверки; её результат не подменяет основной guard.

Root полностью прочитал отдельный cleanup source/test. После авторских6/6 PASS выполнил один exact cleanup bundle `c1ae8fe1538462a7be8ee28ccafa31de880b875c1510081f1da9200f63e10212`, source `8b1de626bcdb6cbdd51927a7b7a11f39f8859b1fe08d054bef076fdb23b6b102`. До каждой mutation сверялись оригинальный journal SHA, serving, все оставшиеся stopped objects, exact image/command/mount/labels и volume users. Exclusive fsynced journal не допускает повтор DELETE. Два контейнера и один task volume удалены без force; каждый GET подтвердил404. START/STOP/KILL не выполнялись. `needsReconciliation=false`, serving unchanged. Исходный failed receipt сохранён без изменений.

## Review3: самостоятельная приёмка исправленной программы

Исправление ограничено sizing и transport: exact serving/owned-container inspect GET допускает128KiB; другие responses остаются64KiB. Подготовка учитывает две копии команды и минимум16KiB запаса metadata. Optional Go HTML escaping учтён как консервативная верхняя граница, не как наблюдавшаяся причина. Mock теперь сериализует настоящий duplicated envelope; отдельный bounded HTTP test воспроизводит прежний64KiB отказ, новое прохождение, exact/+1 boundaries и owned-name scope. Авторский final gate **23/23 PASS**, без skips; root просмотрел дельту и не выдаёт это за свой повтор suite.

После независимого review root повторно разобрал bundle как текст/JSON без import/исполнения: exact controller prefix, единственный trailer, три embedded source bytes и hashes всех четырёх generated programs/fixture совпали. SHA **`7c56a65a092cdf4a9e0a1c266316d1963398d167ba1ab92c68ba51cb04089456`**,164266 bytes.

Один send в15:32:03–15:32:12 UTC; remote run **`b6e96ef7f5045b0e39db2e25d0ed7c04`**, controller elapsed **5953 ms**, exit0. Подтверждено:

- Runtime: **Node v24.15.0 / SQLite3.51.3**, SQL version совпал, in-memory FTS5 вернул1 row.
- Writer:20 files,1076968 bytes; внешний KILL после ready, exit137, **OOMKilled=false**.
- Точный неизменённый CLI: `soty.storage-format.v3`, Rooms2 / Apps6 / Notes1 / Capabilities1.
- Тот же exported parser: будущие Notes2/Caps2 markers → `storage_format_unknown`; два corrupt main → `storage_format_unreadable`. Это неизвестные markers, не реализация DDL2.
- Audit: main/WAL SHA и размеры не изменились; normal SQLite видит committed WAL, при этом valid raw main header0 и future main1. FTS/ledger/Apps retained witnesses совпали. SHM bytes не объявляются неизменными.
- Actual unlabelled image отказал `storage_reader_unknown` несмотря на copied container label; этот контейнер ни разу не запускался.
- Всего1 volume CREATE,6 container CREATE,5 START,1 KILL,0 STOP. Все пять завершённых процессов OOMKilled=false. Все6 контейнеров и1 том удалены с подтверждённым отсутствием; `cleanupComplete=true`, `needsReconciliation=false`.
- Serving ID `d86bc0b9…9c4ceb`, image `sha256:d07345cb…97e`, StartedAt `2026-09-28T01:15:45.442670947Z` совпали до/после. Production data/config никогда не монтировались.

Полные локальные receipts сохранены в `output/implementation-20260930/`. SHA точных сохранённых JSON files (не hashes исходной компактной строки stdout):

| Файл | SHA256 |
|---|---|
| `p4-storage-linux-canary.review2.linux-result.json` | `80220d7e88effec677e91979d3f0cd52466c065398bb1357040811a316281cc6` |
| `p4-storage-linux-canary.cleanup.review1.linux-result.json` | `9cda516e85e7d1ffb9a722f61663e5d73b3da494a08edaa52216967a7940c360` |
| `p4-storage-linux-canary.review3.linux-result.json` | `1e0350a640342ec653dcb4b2e84787e3df103299c7ed498db5c74e36b0b5192a` |

Remote journals оставлены как evidence, не удалялись по префиксу. Проверка не устанавливает reader labels на serving image и не доказывает reader2, полный application image, cold bootstrap или согласованный encrypted restore. Эти следующие release gates остаются открытыми.
