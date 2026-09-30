# P3-D2 — Apps6: проверка совместимости хранилища

2026-09-30. Основание — принятый D1 `3794f01febe3c01e2d3d107be1f03dbf4b023b3c` и [контракт обсуждений](p3-discussion-contract.md). Reader обновлён после заморозки DDL6 автором модели. Это отдельная проверка формата и восстановления: она не подтверждает готовность UI обсуждений или production-выпуска.

## Поддерживаемая граница

Текущий manifest корневого полного Dockerfile и shared guard: `{"version":2,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6]}}`. Оболочки `soty.storage-format.v2` и `soty.storage-start.v2` не менялись. Rooms3 и Apps7 отклоняются, в том числе когда будущий Apps marker/version находится только в committed WAL. Исторический backend-overlay Dockerfile остаётся безусловно закрытым для сборки: изменение reader-label не делает старый server/modules/dependencies совместимыми.

Trusted host probe импортирует только встроенные Node-модули. В нём зафиксированы шесть новых проекций Apps6 и точные имена, таблицы и SQL-тела всех13 discussion guards. Прежние target/source/saved guards остаются обязательными. Подмена одного trigger body, его таблицы, строкового кода отказа, условия lineage или redaction отклоняется. Marker6 поверх таблиц5, пропущенные таблицы/поля, неизвестные table/view/trigger не допускаются.

Probe открывает SQLite read-only, с `query_only` и нормальным чтением WAL; режим `immutable=1` не используется. Он возвращает только номера форматов, без app ID, имён, прав, текстов, сохранённых адресов и счётчиков обсуждений. Это распознавание известного формата, **не** полный аудит строк, индексов/FK/CHECK, целостности базы или прав на сообщения. Более строгий recognizer и проверка данных остаются в Apps service.

## Независимая историческая база5

`deploy/connector/apps-v5.fixture.mjs` содержит literal Apps5 DDL поверх независимых historical Apps2/3/4 fixtures. Его 38 SQL-объектов совпали с настоящим Apps5 migrator после косметической SQL-нормализации. Современная миграция не вызывается для получения historical5, marker новой базы не понижается.

В `deploy/connector/fixtures/apps-v5/` сохранены точные исходники принятого commit и `provenance.json`:

| Source | SHA256 Git content |
| --- | --- |
| `schema.mjs` | `894f20556d1d29596bc6b56a8450ab76dcc1a36c5232e1cdc182bebb93b5a272` |
| `protocol.mjs` | `29d36ce771c300f7dc3a00cbd16f98aed65c960cb5a5c6ee9673363e518ebd7e` |
| `domain-policy.mjs` | `53ca3d124570fe06f197d4f2e630e0220cc91719138166a3f6f4d6a06fcc246a` |
| `launch-path.mjs` | `84c184b2943d4c5ff1e92fe2cdaac2c95fceec131b3dad3e2efb72db29e6ae36` |

Это все локальные зависимости old5 schema. Проверка hash нормализует лишь CRLF checkout в Git LF; функции старого кода не переписываются. Эти test-only модули не импортируются production host probe.

Прежний `storage-apps-v5.test.mjs` теперь использует frozen5 migrator. Historical5 setup общего `storage-apps.test.mjs` также закреплён за frozen5. Поэтому последующее обновление service до6 не превратит прежние тесты в ложную «миграцию до5».

## Что действительно проверено

`storage-apps-v6.test.mjs` использует отдельную synthetic database. До миграции она принимается настоящим old5 кодом: есть public и private apps, revoked app, прежние grants, bound/tombstone aliases, publication/domain/source receipts, target history с откатом на revision1 и сохранённым floor2, два saved entries с точными URL/path и квитанциями.

Миграция5→6 добавляет ровно25 SQL-объектов: шесть таблиц, шесть индексов и13 guards. Старые DDL и строки неизменны. Обсуждения автоматически не открываются и не импортируют World history: новые головы/conversations/messages/changes/rates пусты, usage содержит только `(1,0,0,0,0)`.

Проверка с открытым SQLite writer сохраняет main-файл в формате5 и записывает переход6 только в WAL. Затем в Apps6 записаны согласованные synthetic live message, tombstone, changes, usage/rate rows. Независимый probe видит6; old5 manifest не допускается. Реальный old5 migrator из3794f01, открытый через второе соединение, возвращает `apps_schema_unsupported` без оставленной транзакции. Main и WAL до/после отказа и probe побайтно совпадают. После нормального закрытия writer свежий service reopen принимает6 и сохраняет старые saved entries/receipts, private/revoked состояния, floor2 и сообщения. Это не имитация старого Docker image и не signed discussion API proof.

Общий matrix дополнительно проверяет переходы1/2/3/4/5→6 в WAL, совместимость Rooms2, marker/version mismatch, orphan/zero/corrupt databases и journal types. Shared rollout/controller tests проверяют запрет старого Apps5 reader при rollback, START, restart-policy и recovery; copied container label, завершённый old helper или старый receipt не заменяют свежую проверку формата. В controller migration-тестах SQLite настоящая, Docker engine синтетический.

## Прогоны

Windows x64, Node `v24.13.1`:

```text
node --test deploy/connector/storage-apps-v6.test.mjs
5 tests; 5 pass; 0 fail; 0 skip; exit 0
```

Первый полный параллельный deploy-прогон:186 tests,183 pass,1 fail,2 skip. Единственный fail — Windows `spawn ENOMEM` в существующем `rebase-host` fixture при запуске дочернего процесса. Это не PASS и не основание ослаблять тест. Исходный log сохранён в `output/implementation-20260930/p3-apps-v6-reader-deploy.log`.

Тот же полный набор14 файлов из `deploy/connector/*.test.mjs` и `deploy/connect/*.test.mjs` повторён с `--test-concurrency=1`: **186 tests,184 pass,0 fail,2 skip, exit0;65.85s**. Log: `output/implementation-20260930/p3-apps-v6-reader-deploy-serial.log`. Проверки и таймауты не ослаблялись; уменьшен только параллелизм файлов. Пропущены opt-in Docker archive foreign-owner0600 canary и file-symlink cases без доступного Windows permission. Directory junction и nonregular journal проверки прошли. `git diff --check` по изменённой зоне — exit0; предупреждения Git касались только обычного LF/CRLF checkout.

Короткая независимая перепроверка reader-границы:

```text
node --test --test-concurrency=1 deploy/connector/storage-apps-v6.test.mjs deploy/connector/storage-guard.test.mjs
```

В этом коротком наборе29 cases, уже включённых в полный успешный прогон; отдельный повтор root ещё не заявлен. Основные изменённые integration cases находятся также в `deploy/connect/host-controller.test.mjs`, `deploy/connector/rollout.test.mjs`, `deploy/connector/storage-apps.test.mjs`. Прежний `storage-apps-v5.test.mjs` доказывает отдельную историческую4→5 миграцию и old4 refusal; он не подменён текущей6.

## Источники на момент проверки

SHA256 ниже рассчитан по UTF-8 с нормализацией только CRLF→LF; это не hash готового image:

| Source | SHA256 |
| --- | --- |
| `deploy/connector/storage-probe.mjs` | `19e7dec3260a197470e072a294516e7d11c77cbf4087984ca7ba8fe90121b7d6` |
| `deploy/connector/storage-guard.mjs` | `bdfabdb8286aba5d8a95f7163aa837e95e357e4818c5d99a76196068be8d7ada` |
| `Dockerfile` | `66f9196ac7503bc81a00d3d2450aeb7ad50bb0e1926f3b7ce7467ffeeeb30d88` |
| `deploy/connector/storage-apps-v6.test.mjs` | `63bece0744aec7ac3147a954d3fb3fecf5104f307f8e803ee61d41b7019b34e0` |
| `deploy/connector/apps-v5.fixture.mjs` | `c7f0ba4f31013833d424632b030b4bac34e7f57893086257bc28af564dea01b4` |
| Проверенная текущая `modules/apps/server/schema.mjs` | `0dba9ad9c9e0427f56bb34c207242680e247d9f39dc96a9852802309c6fa5786` |

## Внешние release gates

Production, Linux/Docker canary, browser и UI этим этапом не менялись. До первого production Apps6 write нужны обновлённый pinned host guard, проверенный полный image и совместимый fallback, реальные Linux RO WAL/SHM и file-symlink/profile проверки, backup/restore и lost-response сценарии на exact image. Существующий floor2 дополнительно требует реально работающий binding-v2 runtime. Произвольный старый image нельзя допустить новым label; старые результаты Rooms canary не являются Apps6 доказательством.
