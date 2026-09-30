# P4 — независимый review host reader3

30.09.2026. Локальный parser/guard slice принимается: нового блокера в проверенном production diff не найдено. **Это не разрешение объявить image reader3, запустить Linux3 canary или мигрировать production.** Знание формата helper и способность конкретного application image открыть этот формат остаются разными доказательствами.

Изменён только этот документ. Прочитаны [реализация и авторская квитанция](p4-reader3-implementation.md), [исходный подплан](p4-reader3-rollout-plan.md), probe/guard, новый literal/provenance и все 13 reader3 cases, а также места вызова guard в обоих controllers. Никакие candidate modules, fixtures или canary bundles в этом review не импортировались и не исполнялись; SQLite, Docker и remote actions не запускались. Дополнительно выполнена статическая проверка raw Git blobs, JSON literals, SQL fingerprints и размеров сериализованной команды. Авторские source/tests не менялись.

## Что независимо проверено

| Граница | Наблюдение и предел вывода |
|---|---|
| Actual image до START | `guardStorageStart` читает image по immutable `runtime.Image`, проверяет возвращённый ID, его собственный manifest и свежий формат данных. Container label не заменяет image label. После probe повторяются runtime/mount/local-volume/writer checks. `startApplication` вызывает guard до записи намерения и START; running-original guard стоит до STOP. Эти call sites не менялись в reader3 diff. |
| Parser knowledge | Единственная production delta guard — допустимое значение Capabilities3 в parser knowledge. `currentStorageReaders` и Dockerfile сохраняют `[1,2]`. Явный reader3 image в новом тесте — synthetic engine fixture. Actual Docker/image startup им не доказан. |
| Исторический источник | Повторно прочитаны raw `git show` bytes семи файлов baseline3 из `dc1ae217424b33cca0e9a5b60a6e4e719ea0d991`; размеры, SHA256 и Git blob IDs совпали с provenance. Так же проверен old host2 из `0c8db3dbdc4a428c68ff98be9597223abc4da699`. Исторические сравнения используют эти bytes, а не текущий OAuth WIP. |
| Literal3 и SQL | Декодированный literal ровно равен конкатенации закреплённых `OAUTH_DDL` и `OAUTH_GUARDS`: 15,157B, SHA256 `05a3a9b2025bbe351e0ec75e28c67d4b497ba35b6de4932a1a5e439232d0978f`. Все 28 новых normalized SQL fingerprints, object types и target tables совпали. Статический inventory probe: 17 tables +22 explicit indexes +25 triggers =64. Реальное создание/сравнение SQLite layouts относится к прочитанным тестам и root run ниже. |
| Точность распознавания | Новый delta требует четыре полных STRICT table SQL, десять exact index SQL и четырнадцать exact trigger SQL с target table; metadata — ровно три ожидаемых ключа. Marker-only3 и future4 остаются отрицательными. Унаследованные v1 tables проверяются прежними projections/columns/STRICT rules: число 64 не означает, что этот diff впервые добавил полное SQL-сравнение каждой исторической таблицы. |
| Empty и readonly | Существующий повреждённый/нулевой файл, orphan WAL, посторонний файл или symlink не превращаются в empty. Probe использует обычный read-only SQLite/WAL snapshot и фиксированные host identifiers, не `immutable=1` и не domain migrator. Output содержит только версии четырёх stores. |
| Format против domain validity | Авторский negative case сохраняет правильную DDL, но повреждает creator tuple: host узнаёт формат, committed domain3 отказывает. Это ожидаемая граница. Host не подтверждает plaintext rows, AEAD payload, полноту backup или согласованность нескольких БД. |

Строгие guards и сохранённые native объекты не заменены marker/count-only проверкой. Новые tests отдельно проверяют испорченные predicates/targets всех новых indexes/guards, неправильный FK/STRICT/PK/CHECK, новые columns/objects и metadata. Причины для ещё одного дублирующего test suite на этом шаге не найдено.

## Cold WAL: отказ старого приложения не равен отсутствию файловых изменений

Исправленное автором доказательство соответствует коду тестов. Реальный exited child оставляет populated main2/WAL3. Host RO, old host refusal и old domain2 с RO handle сравнивают main/WAL hashes и имена файлов; SHM bytes не обещаны неизменными.

Отдельный old domain2 RW case проверяет `schema_version_unsupported` и отсутствие persistent changes **до close**. Last RW close затем может перенести committed WAL3 в main3. Тест ожидает этот checkpoint и сверяет сохранение SQL layout, native ledger/receipt/budget и OAuth row; downgrade не происходит. Следующий RO probe имеет собственный непосредственный byte baseline, поэтому создание пустого WAL отдельным observer не приписывается host probe.

Это полезное ограничение для оператора: запуск старого бинарника «пусть сам откажет» не является безопасной заменой host admission. Строгий отказ до application START/open обязателен. Наблюдение checkpoint относится к локальному Node24.21.0/SQLite3.53.4; оно не сертификат byte equality на других runtime или настоящем RO Docker mount.

## Конкретное уточнение будущего Linux3 budget

Независимая статическая проверка обнаружила, что одного упомянутого в авторской квитанции direct-inspect overflow недостаточно для полной диагностики. Прежний canary `measureConfig` проверяет также encoded `Entrypoint+Cmd ≤49,152B`, а для inspect использует максимум direct representation с newline и `2*goCommandBytes+64`, затем резервирует 16KiB.

| Представление того же probe | Encoded command, cap49,152B | Conservative inspect +16KiB, cap131,072B |
|---|---:|---:|
| Frozen worktree, 55,197B | **56,916B — отказ** | **131,450B — отказ**, превышение378B |
| Только LF normalization, 54,873B; диагностическая строка, не Git artifact | **56,268B — отказ** | 130,154B |

Для worktree direct inspect с newline занимает115,006B; фактический conservative maximum —115,066B. Поэтому переход к Git LF сам по себе не снимает препятствие: command всё ещё превышает48KiB. Это **блокер механического переноса нового probe в прежний Linux2 transport**, не дефект локального recognizer. Никакой cap здесь не повышен и будущий bundle не подготовлен. Автор принял замечание и уточнил обе границы в своей квитанции; production/tests не менялись.

Минимальный следующий шаг для Linux3 — отдельный reviewed emitter/компактное представление, сохраняющее все SQL checks, с измерениями окончательных pinned bytes по всем прежним command/CREATE/inspect bounds. Произвольная нормализация во время запуска и молчаливое увеличение лимитов не принимаются. Эти canary bounds не следует путать с отдельным общим Docker API decoder production controller.

## Проверки и exact scope

Root предоставил и reviewer прочитал итоговый log `output/implementation-20260930/p4-reader3-root-integrated.log`: **59/59 PASS, 0 FAIL, 0 SKIP, 27,102.7642ms**. SHA256 log: `d927c3eec45b6113722d56f3cf782299529ae69ddd69ec9bbee6d9d132d2bbb0`. Это root run, не повтор независимого reviewer. Прежние author58 и final13 не складываются здесь в вымышленный отдельный прогон.

Проверенные worktree hashes:

| Файл | SHA256 |
|---|---|
| `deploy/connector/storage-probe.mjs` | `6c69ddfe21cb18e61abef743be93a79b90afd05b54f3467693652cdf5125501f` |
| `deploy/connector/storage-guard.mjs` | `faa313877080b2fe23a26b6c1651004f12b3abb9e92b0d8414fb577d32f12d8a` |
| `deploy/connector/storage-oauth-v3.test.mjs` | `a4a0625268f3d070262eca052e98d130da24e9facfbac8cf8ab5c7a9a6cd257a` |
| `deploy/connector/capabilities-v3.fixture.mjs` | `8d3794a5c62801ca181709a91c438009ed9807812b48ef2ae600ad443ee004e4` |
| `deploy/connector/fixtures/capabilities-v3/provenance.json` | `365e167885e674b73e45f42e103b3084bd796e15890cb1d9415a4c7cabe6c22b` |

Исходная прочитанная авторская квитанция имела SHA256 `9ebab3eede8acc6c739dabb3825c870026ec1a3bd7df39f619e93e9d83b4d61e`; после принятой редакторской бюджетной дельты — `5761c87bbcad9bee123226481a5dc49e3323be7ed7edc8f811d52e57251fa3d9`. Перечисленные source hashes не менялись. Worktree bytes здесь не выдаются за будущий committed blob.

До первой migration3 остаются отдельными gates: actual default-off image3 и rollback image3, их честные labels и запуск на genuine3; новый Linux3 artifact; первоначальный unlabelled-serving bootstrap; согласованный encrypted backup и restore rehearsal всей generation. Переданное состояние `[1,2]` намеренно не разрешает START3. Успешный host format probe не закрывает ни один из этих gates и не означает готовность OAuth issuance/consent.
