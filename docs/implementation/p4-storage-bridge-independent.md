# P4-B1 bridge — независимая локальная приёмка

Дата: 2026-09-30. Проверен strict v3 bridge для исторических Notes1/Capabilities1, без migration2 и без deployment. Независимый набор: **7/7 PASS, 0 FAIL, 0 skip**, 921.655 ms. Новых блокеров в проверенной области не найдено.

Reviewer изменил только новый `deploy/connector/storage-notes-capabilities.acceptance.test.mjs` и этот отчёт. Existing production, авторские fixtures/tests и orchestration tests не редактировались. Запуск выполнен после author freeze и явной передачи тестового слота root; после него слот возвращён root для общего deploy gate.

## Freeze и команда

| Проверенный файл | SHA256 |
| --- | --- |
| `deploy/connector/storage-probe.mjs` | `d51b36c12e316e660fd2b3ab6d8fe2350833aff242f2ae95d296fb63409afd59` |
| `deploy/connector/storage-guard.mjs` | `cd10d07f1f2b75e2f8c05ca469ae97f3bb7fb35ba76ebf085345eb99530f3a1c` |
| `deploy/connector/storage-notes-capabilities.acceptance.test.mjs` | `478ae8a8b018ca7624c21b972def0aef6ca10b4d26804a4fbeecddd882509753` |

Из primary worktree выполнено:

```powershell
$bridgeNode = Join-Path (Get-Location) 'var/toolchains/node-v24.21.0-win-x64'
$env:PATH = $bridgeNode + [System.IO.Path]::PathSeparator + $env:PATH
& (Join-Path $bridgeNode 'node.exe') --test --test-concurrency=1 deploy/connector/storage-notes-capabilities.acceptance.test.mjs
```

Это process-local PATH, system runtime не менялся. Node24.21.0/SQLite3.53.4 ранее фактически проверены в [независимом B1 preflight](p4-b1-independent-preflight.md) и [root runtime receipt](p4-runtime-baseline.md). Никаких дополнительных больших suites в этой приёмке reviewer не запускал.

## Независимые fixtures

Не использованы авторские literal create/seed helpers. Перед dynamic import проверены SHA точных исторических schema modules из commit `6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e`:

- Notes: `da75ffc702fb0ee4db9878780d374f53bca0d68b46e91eafb56038b45d1e0dfa`.
- Capabilities: `959fdb4938d6b0ebc9359817b001f0eed08be70e437083648e9ac259c56229ad`.

Эти настоящие старые migrators создают тестовые DB; reviewer добавляет собственный небольшой synthetic seed: Notes body/FTS/receipt, active row и deleted tombstone, Cap contract/client/audit. Это проверка сохранности storage projections, не выполнение Notes/Capabilities бизнес-операций и не новый native proof.

После прогона root закрыл portability seam: при `core.autocrlf=true` будущий Windows checkout мог изменить exact fixture bytes. Добавлено `.gitattributes` правило `/deploy/connector/fixtures/** text eol=lf`. Reviewer прочитал diff, независимо проверил `git check-attr` для обоих historical schemas и повторно сверил их SHA: bytes не изменились. Строгие hash assertions сохранены. Эта настройка checkout не потребовала повторять SQLite suite.

Все директории созданы тестом под отдельным `soty-storage-bridge-independent-*`. Cleanup ограничен тем же canonical immediate child временного каталога и уникальным marker, проверяемыми перед удалением. Ранее оставленные directories не использовались и не удалялись.

## Что доказывают семь групп

| Проверка | Наблюдаемый результат |
| --- | --- |
| Populated historical Notes1 + Cap1 | Только шесть полей format DTO; никаких private titles/body/client/account IDs. Main/WAL hashes и существующий inventory совпали до/после. Сохранились note states/revisions, FTS match и client epoch |
| Absent/empty против orphan | Отсутствующие и пустые stores дают `empty`. Orphan WAL отдельно в Notes и Cap отказывает; main/другие files не создаются, исходные bytes и inventory прежние |
| Notes future committed WAL после прежнего receipt | Main header действительно остаётся user_version1, живой SQLite view видит committed2. Ранее полученный valid v3 START receipt остаётся только синтаксически валидным; повторный guard выполняет новую actual SQLite probe и отказывает |
| Cap future committed WAL после прежнего receipt | Та же независимая проверка второго store при валидном Notes1. Успех первого store не скрывает неизвестный формат второго. Main/WAL и inventory неизменны после отказа |
| Identity/projection failures | Другой Notes project, duplicate lineage и переименованная Cap column закрыты. Error содержит только безопасный код; нет synthetic content или имени private column. Main/WAL не изменились |
| Manifest / format JSON | v2, строковые числа, boolean/array/object coercion, duplicate/future reader versions, missing/extra stores и собственный `__proto__` key отвергнуты. Exact image ID должен быть строкой; ограниченный Apps1 reader не допускает Apps6 при остальных совместимых stores |
| Persisted START receipt | Нужны все четыре stores и точные container/image/mount identities. Старый schema2, missing Notes, future Cap2, string format, array-coerced identity и лишний `constructor` key не проходят |

WAL2 fixtures намеренно проверяют committed **future marker/version**, не притворяются реализованной Notes2/Capabilities2 схемой. Их создание не является native migration acceptance.

В двух guard cases Docker inspect/image/volume metadata предоставлены минимальным synthetic engine; сам probe читает **настоящие SQLite files и WAL**. Ни actual Docker START, ни Docker mount semantics этими cases не проверены. Движок разрешает только предусмотренные read operations; old receipt не заменяет fresh probe.

Main/WAL проверены побайтовыми хешами. Не заявляется неизменность SHM read marks. Отдельные cold/crashed-WAL, exclusive writer, full literal-schema comparison и Windows symlink cases принадлежат авторскому gate; его результаты не пересчитаны как независимые здесь.

## Source review и границы

Прочитан полный новый Notes/Cap путь probe и strict manifest/format/receipt diff. Он использует fixed host projections, реальные virtual/shadow FTS kinds, hidden-column inventory, exact FTS options и critical explicit index SQL. Unknown objects, unreadable projections, неверные lineage и Notes project не интерпретируются как свежая DB. Формат Cap1 исторически не содержит project/store incarnation; bridge этого доказательства не выдумывает.

Reader declaration теперь относится ко всем четырём stores и берётся из exact image. String checks закрывают прежнюю возможность regex coercion identity. Форматная projection и START receipt перечисляют Notes/Cap независимо, старые v2 DTO не расширяются подразумеваемыми `empty`.

Bounded recognizer не является полным row/FK/CHECK/FTS integrity scan или cross-store transaction proof. Разрешённый незавершённый WAL tail может игнорироваться самой SQLite; данный PASS не обещает обнаружить все повреждения. Случайная дополнительная запись host/root, произвольные mount aliases и hostile modification самого trusted source вне принятого managed topology также не становятся решёнными.

Root-owned ранняя проверка фактического old image и fresh running-original guard перед STOP ранее прочитаны в [B1 preflight](p4-b1-independent-preflight.md). Их общий executable deploy gate запускает root после этой передачи. Этот отчёт не выдаёт синтаксически valid receipt за бессрочное разрешение START и не утверждает готовый automatic rollback первого v2→v3 bridge.

Итог ограничен локальным bridge Notes1/Capabilities1. Остаются отдельные проверки: полный composition regression, first production bootstrap/recovery, exact Linux application/helper image и RO-WAL topology, согласованный cold backup/restore. Будущие Notes2/Capabilities2 readers, default-off migration и native effects имеют собственные следующие gates. SSH, Linux canary и production deployment reviewer не выполнял.

## B1b: отдельное обновление независимых assertions для reader2

30.09.2026. Предыдущий раздел и его hashes фиксируют исторический reader1 bridge. После реализации настоящего reader2 root разрешил узкую адаптацию принадлежащего reviewer acceptance файла; production и авторские fixtures не менялись.

- Manifest/format/START negative для неизвестной версии теперь использует **3**; старые envelope v1/v2, coercion, unknown keys и несовпадающие identities остаются запрещены.
- Добавлены positive reader declaration `[1,2]`, numeric format2 и START receipt2. Отдельные assertions сохраняют отказ реального reader1 declaration на Notes2 и Capabilities2 независимо.
- Marker-only WAL2 случаи остались неизменны: marker без v2 DDL/registry — **malformed2**, его отказ нельзя считать доказательством, что reader2 запрещает подлинную схему2. Исторический вывод о неизвестном marker2 выше относится к старому reader1.

Собственная команда из первого раздела: **7/7 PASS,0 FAIL,0 SKIP,811.0009ms**, Node24.21.0/SQLite3.53.4. Те же проверки RO/main/WAL/inventory и отсутствие private projection сохранены.

Затем отдельно повторены только два подходящих авторских случая; авторский файл и его helpers не импортировались в собственный test:

```powershell
& (Join-Path $bridgeNode 'node.exe') --test --test-concurrency=1 --test-name-pattern='genuine main1/WAL2' deploy/connector/storage-native-v2.test.mjs
```

Результат: **2/2 PASS,0 FAIL,0 SKIP,444.2188ms**. Для каждого store настоящий literal v1 получает полный additive v2 DDL при открытом WAL writer: raw main header1, SQLite committed view2. Новый probe узнаёт2; точный старый probe из `ae914f55e6d8d64628a7279189d2d55dfd05de45` отказывает `storage_format_unknown`, а старый v3 reader declaration `[1]` — `storage_reader_incompatible`. Main/WAL hashes неизменны. Это независимый повтор **авторских fixtures**, не ещё два собственных сценария и не cold-writer/process-kill повтор.

Provenance correction прочитана до прогона: Git LF blob старого probe имеет SHA `d0aed27ce790502f36df20689663a43aee0ed28266e6b424a002cde7eb2f4e17`, исторический Windows CRLF вариант — `d51b36c12e316e660fd2b3ab6d8fe2350833aff242f2ae95d296fb63409afd59`. Авторский test проверяет оба точных представления и исполняет Git LF bytes. Reviewer не сравнивает новый author test со старым SHA до этой поправки.

| Проверенный B1b файл | SHA256 рабочих bytes |
| --- | --- |
| `deploy/connector/storage-probe.mjs` | `5d7bdb6cee8c38671ff9a188e26b4bc33e983fbef04ec3e3ac9cabd9338f650a` |
| `deploy/connector/storage-guard.mjs` | `95e0a74b818a6bec44b0994109bc0f26019df6240e929be1da5a617ca3454cca` |
| `deploy/connector/storage-notes-capabilities.acceptance.test.mjs` | `17f06f859259296e786d115b69aae8740f859d4922c9810f9c9e6eb2768c57d4` |
| `deploy/connector/storage-native-v2.test.mjs` | `767337e09a7c6e0d02e1937a6504890d4969a0f99c9f3645f3a40ae934d08539` |

Новый local PASS не доказывает Linux reader2 image, production bootstrap, DDL2 migration admission или coherent backup/restore. Ранее исполненный root Linux canary проверял bridge1 и не переносится на reader2. После этих трёх коротких последовательных прогонов (own B2, own B1b, selected author WAL) слот передан root; полного deploy/world повторения reviewer не запускал.
