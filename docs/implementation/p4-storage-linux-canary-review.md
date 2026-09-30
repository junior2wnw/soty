# P4-B1 Linux canary — независимый review плана

30.09.2026. Проверка чтением, **без SSH/Docker/run, source edits и executable tests**. Прочитан полный новый план, safe поля свежего inventory и прежний scaffold как reference. Новый bundle ещё не проверен и remote запуск этим отчётом не разрешается.

| Материал | SHA256 |
| --- | --- |
| `p4-storage-linux-canary-plan.md`, после первых двух поправок | `1df4e027f219dd3c0d5c49ac695248eacccdb7fa47a711c0b327d1f8cf51da29` |
| `output/implementation-20260930/p4-linux-readonly-preflight.json` | `203fb997f1e1129a7cfa5440380fa31a009cfbd8f8c7e3a145681365677b15ff` |
| Старый `soty-experience-release/.../storage-linux-canary.mjs` | `73be355645799dfb47c008a6544d24c1e9638cea8a16840d46cee502dece20d5` |

Структурного препятствия для **local-only реализации** ограниченного canary не найдено. Автор и root приняли пункты1–2; пункт3 передан для обязательной проверки будущего artifact:

1. **96KiB script не помещается в гарантированные 64KiB Docker response.** Inspect возвращает полный `Config.Cmd` с JSON escaping. Текущий probe занимает 24 616B, минимальный JSON с его Cmd — около25 322B; полного Apps6 writer/audit ещё нет. План теперь требует encoded Entrypoint+Cmd≤48KiB и полного CREATE config≤56KiB, с записью фактических размеров всех phases до run. Это запас, а не гарантия размера любых daemon metadata: overflow всё равно fail-closed. Нельзя молча поднять cap или убрать Apps6 fixture.
2. **20s/180s — пределы работы controller, не resource TTL.** Старый scaffold намеренно запрещает повтор STOP/KILL после ambiguous KILL; при потере host process watchdog также не обеспечивает cleanup. План теперь допускает retained exact resources с `needsReconciliation` и запрещает PASS. Следующий шаг — отдельный read-only reconcile/разрешённое recovery, без повтора bundle или self-exit, способного изменить WAL.
3. **Exit137 не отличает planned KILL от OOM.** В старом scaffold `OOMKilled` записывался только в catch diagnostics, а success-path сравнивал exit code. Для нового artifact нужен terminal `OOMKilled===false` и отрицательный mock на ready→OOM→exit137. Это передано root/автору как обязательная точка будущего source audit, без новой фазы или контейнера.

## Поддержанные решения и точные границы

- Шесть task containers, пять START, один writer и один внешний KILL укладываются в прежний потолок8/1. Четыре fixed negative roots в одном новом volume — приемлемое ограничение: negative wrapper вызывает точные pinned `readStorageFormat` bytes, но это **не** четыре отдельных production CLI запуска. Positive `/data` case запускает настоящий CLI без rewriting.
- Новый random namespace и durable single-shot intents должны сохранять exact identity на CREATE/START/KILL/DELETE. Serving ID/image/StartedAt сверяются с утверждённым baseline до первого CREATE и после; никакие non-GET serving requests, чужой cleanup по префиксу или повтор mutation по одному timeout не разрешаются.
- Image используется только как установленный interpreter. Fresh inventory подтверждает прежний serving identity, Node metadata24.15.0, отсутствие image reader label/healthcheck и один declared volume. Оно **не** доказывает actual Linux SQLite/FTS. Runtime-check на empty RO volume должен первым проверить точные Node24.15.0, SQLite3.51.3 и in-memory FTS5; несовпадение прекращает опыт до fixture writes, без install/pull.
- Production data/config не копируются и не монтируются. Inherited image defaults проверяются по allowlist и после CREATE, application Env не переносится. Synthetic volume Name/Source сравниваются с serving mounts только в памяти; receipt содержит hashes. Explicit entrypoint/healthcheck/NoCopy и realized mount checks исключают запуск приложения вместо helper или незамеченный второй mount.
- Per-case ready/audit должны связать header/view/marker/size/hash с конкретным fixed root. Valid roots доказывают реальное чтение committed WAL, future Notes и Cap отдельно сохраняют main1/WAL2, corrupt отдельно даёт unreadable. Future markers не являются реализацией DDL2. Main/WAL hashes сравниваются после KILL и RO phases; SHM bytes не объявляются неизменными.
- 8MiB/32-file budget — измеряемый synthetic payload limit, не physical volume quota. 128MiB/0.5CPU/16PID и networknone/rootfsRO/без ports/socket/binds ограничивают helper. Ни один успешный subcase не заменяет полный audit, cleanup и подтверждённое отсутствие всех exact task resources.

Остаются обязательными local emitted payload tests, полное historical Apps6 DDL/dependency comparison, проверка redaction/byte bounds и adversarial mutation/lost-response tests. Затем отдельно проверяются **фактические** source, неизменённые probe bytes, import mapping guard, ready/audit shapes, mutation allowlist и hash будущего bundle. Этот review не распространяется автоматически на их реализацию.

Даже успешный будущий Linux canary закроет только заявленные synthetic RO-WAL/parser/runtime свойства на одном engine profile. Production rollout, первый bridge bootstrap, actual old-image application compatibility, Notes2/Capabilities2, native effects и согласованный backup/restore остаются отдельными gates. Старый rooms-only Linux PASS не переносится на новый v3 опыт.

## Последующий независимый review точного review2 bundle

30.09.2026, после B1a reader baseline. Этот раздел закрывает оставленный выше source/bundle review. **Новых блокеров в просмотренном artifact не найдено.** Проверены controller, fixtures, generated programs и 20 авторских сценариев; SSH/Docker/run/import bundle и повтор tests не выполнялись. Единственная новая локальная проверка разбирала файлы как текст/JSON и читала исторические Git objects. Решение о remote попытке остаётся у root; Linux результата на момент этой записи ещё нет.

| Проверенный artifact | SHA256 |
| --- | --- |
| `output/implementation-20260930/p4-storage-linux-canary.mjs` | `137905805516a7982e9853ebbe034bb572954ee45ea80942611ea55575594706` |
| `output/implementation-20260930/p4-storage-linux-canary.fixtures.mjs` | `d86ee283667da2538cba6cdcbd53df63b93e1f90d4d4f63dd0979715e735e2c3` |
| `output/implementation-20260930/p4-storage-linux-canary.test.mjs` | `5af707dc87ce65dcd7259bc609ae768c487877de8608634f810499f4444e35b1` |
| `output/implementation-20260930/p4-storage-linux-canary.review2.bundle.mjs` | `e2853c4882de7520949b3de9392a6c5037022e8aa18c35f0ce458d7f312c3927` |

Независимое сравнение подтвердило **162614 bytes**, exact controller prefix и единственный fixed executable trailer. Три embedded source (`storage-probe`, `storage-guard`, `docker-api`) совпали byte-for-byte с рабочими файлами. Writer и audit восстановлены конкатенацией буквального DDL, fixed cases и исходного function text без исполнения этих функций; runtime и negative — фиксированными текстовыми шаблонами. Все четыре результата совпали с bundle, включая связь marker→marker hash. Семь hashes исторических исходников сверены с `git show` pin `6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e`. Это проверка происхождения artifact, а не независимый повтор SQLite migration. Самый большой encoded writer command — **42123 B**, ниже согласованных 49152 B; предел Docker response не повышен.

Прочитанные существенные свойства реализации:

- До первого CREATE сверяются exact interpreter image, serving ID/image/StartedAt и mount inventory. Effective image/container environment проверяется в памяти; production credentials, конфигурация и volumes не передаются helper. Каждый START следует после проверки фактических command, mounts и ограничений созданного task container. Runtime-check на пустом RO volume обязан подтвердить Node24.15.0/SQLite3.51.3/FTS перед созданием writer.
- Durable journal предшествует CREATE/START/KILL/DELETE. Потерянный ответ не разрешает повтор исходной mutation: используется inspect exact name/ID. Один writer, один внешний KILL после checked ready; неоднозначный KILL не превращается в STOP. Неопределённость сохраняет exact resource identities и исключает PASS. Controller deadlines не объявлены сроком жизни контейнеров при потере controller.
- Восемь fixed cases связывают main header, SQLite WAL view, размеры и main/WAL hashes. Future Notes и Capabilities проверяются отдельно как main1/WAL2; corrupt cases отдельно дают unreadable. Writer держит шесть SQLite connections до внешнего KILL. Terminal `OOMKilled` обязан быть ровно `false`, включая exit137: третье замечание первичного review закрыто кодом и отдельным авторским negative case.
- Positive phase запускает неизменённый CLI. Negative phase вызывает тот же parser на четырёх фиксированных roots, без подмены его bytes. Audit читает synthetic FTS/ledger/Apps rollback-floor/saved/tombstone witnesses, проверяет hashes до чтения и после закрытия RO connections; SHM hashes не обещаются. Ready/audit имеют строгие shapes, fixed error classes и byte limits; raw stderr, SQL, marker и environment в receipt не выводятся.
- Cleanup ограничен записанными exact task IDs, non-force deletes и отдельным volume после подтверждения отсутствия подключённых containers. Отвергнутая конфигурация и unresolved START/KILL не позволяют очистить неизвестное состояние. Unknown-reader case лишь создаёт container с ложной container label и требует отказа image-authoritative guard без START/RW helper; это не проверка работы старого application binary с новыми данными.

Авторский local **20/20** на Windows Node24.21.0/SQLite3.53.4, включая actual emitted writer/kill/probe/audit, здесь атрибутируется автору. Root отдельный static review также не заменён этим отчётом. Независимый read-only audit разрешает считать **этот hash** просмотренным кандидатом для отдельного remote решения; review1 не принят и не изменён. Не закрыты самим source review: фактический Linux runtime/mount/cgroup, production rollout, old-image application compatibility, Notes2/Capabilities2, native effects и coherent backup/restore.

## Исправленный review3 после фактического отказа review2

30.09.2026. Независимый review исправления завершён: **новых блокеров в точном review3 artifact не найдено**. Это чтение source/tests и статическая сверка текста/JSON; artifact не импортировался и не исполнялся, SSH/Docker и повтор авторских tests не выполнялись. Решение о следующей remote попытке остаётся у root.

Предыдущий review2 действительно **FAILED**: Linux runtime-check подтвердил Node24.15.0/SQLite3.51.3/FTS, но после CREATE writer его inspect превысил64KiB. Writer не STARTed, поэтому WAL/reader/audit результата нет. Docker inspect повторяет command в `Path/Args` и `Config.Entrypoint/Cmd`; фактический writer response был89487B. Предыдущая проверка размеров, включая этот независимый review, пропустила вторую копию. Отказ остался fail-closed с retained resources и `needsReconciliation=true`; он не переименован в PASS.

Отдельная root recovery-квитанция сообщает удаление двух exact stopped containers и task volume тремя non-force DELETE, без START/STOP/KILL, с последующими404 и неизменным serving. Проверены безопасные поля result-файлов: review2 `80220d7e88effec677e91979d3f0cd52466c065398bb1357040811a316281cc6`, cleanup `9cda516e85e7d1ffb9a722f61663e5d73b3da494a08edaa52216967a7940c360`. Это атрибуция root recovery, не независимое исполнение cleanup и не успешное завершение старого canary.

| Исправленный artifact | SHA256 |
| --- | --- |
| Controller `p4-storage-linux-canary.mjs` | `149a28de4980df7fec3feab4d54af5a40acbde8c9ba0fcbd81f97c47c587b684` |
| Author tests `p4-storage-linux-canary.test.mjs` | `60ff72bd1b200d74ec8518270b31993b683a569eaf720d930b8824dd74ab778a` |
| `p4-storage-linux-canary.review3.bundle.mjs`, **164266B** | `7c56a65a092cdf4a9e0a1c266316d1963398d167ba1ab92c68ba51cb04089456` |

Исправление ограничено двумя controller spans: расчётом config/inspect и bounded Docker reader; далее — подключением этого reader в `runCanary`. Exact GET inspect serving или записанного task ID/ожидаемого имени получает128KiB. Для task имени проверяются runId и фиксированная phase; pre-ID имя поддерживает reconciliation неоднозначного CREATE. Другие paths, query, методы, чужие ID, image inspect и logs остаются64KiB. Увеличение read bound не разрешает никаких новых mutations.

До bundle/run учитываются обе копии command, возможное escaping и минимум16KiB metadata reserve; прежние limits command48KiB/CREATE56KiB сохранены. Независимый текстовый расчёт для writer дал duplicate bytes84760, conservative budget84820 и остаток46252B до128KiB. Это запас, не обещание размера произвольных daemon metadata: фактический overflow по-прежнему прекращает опыт. Проверенный primary Moby `WriteJSON` использует `SetEscapeHTML(false)`; дополнительное escaping `<`, `>`, `&` в sizing намеренно консервативно и не объявляется фактической конфигурацией remote daemon. [Moby v26.1.5, `WriteJSON`](https://github.com/moby/moby/blob/v26.1.5/api/server/httputils/httputils.go#L81-L87).

Статическая сверка подтвердила exact текущий controller prefix, единственный fixed trailer, embedded source bytes/hashes и spec digests. За пределами двух просмотренных spans controller совпадает с review2 после нормализации line endings. Три embedded source неизменны; runtime/negative programs также неизменны, writer/audit отличаются только новым synthetic marker и его hash. Historical fixtures сохраняют прежний SHA `d86ee283667da2538cba6cdcbd53df63b93e1f90d4d4f63dd0979715e735e2c3`; probe/guard не менялись.

Прочитаны три новые авторские проверки: duplicate command sizing/escaping, exact scope128KiB и настоящий loopback chunked HTTP reader с old64 RED/new128 GREEN, exact limit и +1. Итог **23/23 PASS,0skip,1532.4622ms** атрибутируется автору; этот reviewer их не перезапускал. Mock теперь отражает обе копии command; его результат не подменяет remote Docker. Source review не закрывает фактический Linux review3, production rollout, old-image application compatibility, reader2/native effects или coherent backup/restore. На момент записи review3 remote ещё не выполнялся.
