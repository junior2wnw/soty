# Временные файлы Linux platform CI

Измерение относится к exact Root `425561254467cb3a118fa960504f8fc0b5a7ac07` и фиксированному probe packet `313f6bf972340b2c6af73bc855b37f00`. Оно показывает стоимость дисковых операций. Прежняя отмена полного Linux CI на 15 секундах в отдельном probe не воспроизвелась; её причина этим измерением не доказана.

| Disposable fixture storage | SQLite FULL pressure workers | Полное время focused test | Итог |
| --- | ---: | ---: | --- |
| Overlay disk | 0 | 4689 ms | PASS |
| Overlay disk | 6 | 4006 ms | PASS |
| Только fixture TMPDIR в tmpfs256 MiB | 0 | 1002 ms | PASS |
| Только fixture TMPDIR в tmpfs256 MiB | 6 | 992 ms | PASS |

В обоих вариантах Node24.15, те же зависимости и RUN build, CSP/null-Origin case, SQLite synchronous FULL, busy5000 и deadline15s сохранены. Шесть pressure workers создавали только свои ограниченные синтетические SQLite БД на overlay. Их процессы и каталоги очищались в finally. Сырые logs остаются в собственном лабораторном namespace; публичные receipts содержат только закрытые фазы, сроки, выбранные числовые IO/PSI поля. Системный PSI включает соседнюю нагрузку и не измеряет syscall fsync одного процесса.

Перед RUN Root сверил namespace/owner, exact public archive, все десять canonical Git source pins, manifest/runner/Dockerfile, типы и пути tar entries, исходный Docker prefix и фактические builder CpuPeriod100000/CpuQuota600000/memory8GiB. Первая Windows CRLF archive была отклонена до извлечения; новая создана с `git -c core.autocrlf=false archive` и имеет 40192000 bytes, SHA256 `0a6b3f622ef89733f91846f187c712cb26dd874d55f64764e47966f1b41d02d6`.

Probe manifest SHA256 `563d6838f9f123f325f7f97b45683a0526d6efcf358c7c2b160258b775b84ba1`; runner `120ba7261186bef4812d2a2a07fa8cd911725e33ba25f87072e6f6a20ecc4702`; Dockerfile `49bc38de754abb3083c546b08959369f849e8cdd3e3ee1a4158525043f8fc010`. Root controller завершился с ExitCode0; оба target build завершились с ExitCode0. Это четыре focused запуска, не полный image gate.

В Root Dockerfile только disposable `platform:test` получает build-only TMPDIR/TEMP/TMP `/tmp/soty-ci-platform` в bounded512 MiB tmpfs. Переменные заданы лишь этому вызову, runtime ENV и постоянные volumes не меняются. Остальные CI suites сохраняют прежний TMPDIR. Проверки reopen/multiprocess/CAS/readers по-прежнему используют настоящие процессы и SQLite; физическая сохранность после потери питания этим CI не подтверждается. Отдельный encrypted cold restore должен пройти на настоящих дисковых volumes до выпуска.

Эта build-only оптимизация ещё требует следующей полной baseline/feature сборки и нового Apps8 cold gate. Успешный probe не объявляет исправленной задержку production Source и не заменяет сохранённые отказы прежних полных сборок.

Копии публичных квитанций: `D:/соты/output/soty-universal-platform-implementation-20261006/startup-probe-313f6bf9-overlay-public.json` и `startup-probe-313f6bf9-tmpfs-public.json`. Remote evidence: собственный каталог `/home/ai2/codex-soty-universal-20261007-9f8dcd71/startup-probe-313f6bf972340b2c6af73bc855b37f00`.
