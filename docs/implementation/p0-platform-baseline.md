# P0 — платформа, задания, публикация и восстановление

Дата проверки: 30 сентября 2026. Исполнитель: системный инженер `publishing_architecture`. Основание: `docs/plans/soty-human-agent-platform-20260930.md`, этап P0. Проверена release-копия `C:\Users\Junio\.codex\worktrees\soty-experience-release\соты`, исходный commit `b0af84b8dc6c10f00236ed132565bff3a7212b64`.

Статус: локальная инженерная база подтверждена указанными ниже тестами; актуальное восстановление production, реальные первые авторы и внешние устройства не проверены. Этот документ не закрывает внешние условия P0 и не объявляет P1–P6 реализованными. В этой работе изменён только данный документ. Release generation, build/typecheck, полный world/connect suite, production-сеть, SSH и реальные inference-запросы не запускались. Общие build/typecheck/world/connect проверки выполняет интегратор.

## 1. Воспроизводимый локальный baseline

Среда: Windows, Node `v24.13.1`, команды выполнены из release-копии. Тесты используют отдельные временные каталоги и синтетические идентичности. Сетевые проверки ниже ограничены loopback либо подменённым fetch. Тесты не читают production auth/history и не копируют их в fixtures.

| Команда | Результат 30.09.2026 | Что действительно доказано |
|---|---|---|
| `node scripts/connector-store-selftest.mjs` | PASS, exit 0 | Контракты текущего store, задания, доступ и сохранение |
| `node scripts/connector-persistence-selftest.mjs` | PASS, 13 сценариев | Rollback транзакции; commit-before-notify; полная миграция history/grants/results; fencing второго владельца; повреждённые данные fail closed; crash windows rollback; сохранение request identities; предел ledger |
| `node scripts/connector-queued-migration-selftest.mjs` | PASS, 2 теста | Перенос только точного проверенного набора never-leased jobs; lease ambiguity, изменение тела/набора и неверный fingerprint блокируют перенос |
| `node scripts/connector-durable-protocol-selftest.mjs` | PASS, 7 проверок | Lost create ACK не дублирует исполнение; недоставленный result остаётся uncertain без повторного запуска; поздний native result восстанавливает исход; отмена одного задания не останавливает другое; reopen сохраняет identity/result |
| `node scripts/connector-readiness-selftest.mjs` | PASS | Стабильная проекция конфигурационной готовности; динамические counters не подменяют readiness. Доступность реальной модели не доказана |
| `node scripts/inference-resilience-selftest.mjs` | PASS, 29 проверок | Bounded queue/timeouts; cancel; fallback/race; нет повторной выдачи после видимого результата; корректная сборка tool arguments и освобождение slots |
| `node scripts/inference-race-keys-selftest.mjs` | PASS, 10 сценариев / 5 синтетических ключей | Разделение application/connector identities, отмена проигравшего provider, отсутствие секретов/текста в metrics. Денежного ledger этот тест не проверяет |
| `node --test deploy/connect/backup.test.mjs deploy/connect/verify-backup.test.mjs deploy/apps/edge-config.test.mjs` | 9 PASS, 1 SKIP, 0 FAIL | AES-GCM целостность; настоящий tar/SQLite-header; отказ при tamper/truncation/unsafe paths; пределы metadata; TLS registry gate и сохранение прежней edge-конфигурации |

В durable-protocol перед запуском явно задан `SOTY_TEST_SERVER_ROOT` равным текущей release-копии. Измеренный результат: 83 события в 4 страницах, результат 500000 байт, максимальный ACK 108 байт. Изолированный тестовый connector завершён его cleanup-кодом.

Важно для доказательств: `scripts/connector-durable-protocol-selftest.mjs:59` печатает жёстко заданное `clientVersion: 1.2.12`. Фактически тест запускает текущий `scripts/soty-connector.mjs`, где строка 16 содержит `1.3.0`; SHA-256 проверенного исходника — `8a9e8db8ca90a3cbf0815bdbfb4088ac683d398ef40e37d178dc827fa3035842`. Поэтому результат не является свежим испытанием установленного клиента 1.2.12. Это дефект метаданных квитанции теста, не повод подменять фактическую версию.

`connector-persistence-selftest` уже запускает `connector-rollback-failure-selftest` внутри себя; второй запуск не делался. Единственный skip в backup suite — явный opt-in Linux/Docker-тест чтения чужих файлов 0600 при read-only mounts. На Windows этот сценарий не подтверждён. SQLite ExperimentalWarning присутствует; тесты завершились успешно, предупреждение не скрывалось.

## 2. Существующие границы хранения и API

Пути ниже относятся к выбранному `dataDir`; production-файлы не открывались. Экспорт данных с телами задач, токенами или encrypted vaults допустим только в защищённой памяти/зашифрованной копии, а не в stdout, Git или тестовых fixtures.

| Контур | Состояние / формат | Публичные точки исходника и что сохранить |
|---|---|---|
| Connect | `connect/accounts.sqlite`; schema 3, reader epoch 1 | `server/connect-module.js:10`; `modules/connect/server/schema.mjs:1,121,138`. Account/device IDs, relationships/blocks, vaults, recovery state, challenges и отзыв. `createConnectService`, `isActorActive`, `subscribeRevocations` |
| Connector | `connector-store.sqlite`, authority marker `connector-store.json`, owner lock SQLite; store v2, jobs v2/v3/v4 | `server/connector-persistence.js:65,97,99`; `server/connector-store.js:7,170,191`. Connectors/grants, job metadata/input/result/events, request tombstones и точная ownership binding |
| Apps | `apps/registry.sqlite`; `apps_meta`, `app_devices`, `local_apps`, `local_app_grants` | `modules/apps/server/index.mjs:14,22`. Стабильные app IDs, owner, connector identity, port/path, grants, revision/revoked state. `createAppsService`, `execute`, `invalidateAccess`, `invalidateConnector`, `resolveOwnedDevice`, `allowsTlsDomain` |
| World | `world/world.sqlite`; schema 3 | `server/http-app.js:69`; `modules/world/server/schema.mjs:3`. Самостоятельные люди/сообщества, членство, discovery visibility и community chats; не превращать их в app grants |
| Notes | `notes/notes.sqlite`; schema 1 | `server/http-app.js:70`; `modules/notes/server/index.mjs:39,64,92,108`. noteId/account, revision, mutation receipts, deleted identities; только последние 32 receipts на заметку, полного архива текста нет |
| Legacy rooms | `<roomId>.json` в корне dataDir | `server/room-store.js:15`. auth, encrypted snapshot/updates/files, closed state. Не расшифровывать/переписывать ради agentability |
| Прочие потребители | account-transfer, identity adapters, traffic и существующие integrations | `server/http-app.js:68,101,102,119`. Инвентаризировать и сохранять; новая главная не разрешает удаление этих данных/маршрутов |
| Runtime устройства | Connector identity/config, локальное состояние OpenCode, пользовательские workspaces | Находятся на устройстве, не автоматически внутри server `/data` backup. Нужна отдельная политика восстановления идентичности/рабочих файлов; не собирать секреты в P0 |

Полный read-only программный экспорт Connector уже возможен через `readConnectorState` (`server/connector-registry.js:83`); узкая проекция — `readConnectorRegistry:52`. Они не предназначены для бесконтрольного печатания состояния. `maintenanceStatus` и `queuedFingerprint` (`server/connector-maintenance.js:13,23`) дают безопасные операционные сведения. Самостоятельного production restore/import общего набора всех модулей, доказанного этим локальным прогоном, нет.

## 3. Карта миграций по этапам

### P1 — единый допуск и факт действия

Предлагаемый стык, согласованный с интегратором: **одна новая capabilities SQLite**, общие schema/index/access/catalog; `invocations.mjs` получает тот же `DatabaseSync` и не создаёт свою БД или очередь. В одной синхронной транзакции проверяются актуальный grant/цепочка, резерв бюджета и запись Invocation/dispatch intent. Lease и исполнение остаются у Connector jobs.

Порядок:

1. Зафиксировать reader/runtime compatibility matrix и metadata новой схемы. Создать additive tables без переименования существующих account/app/job IDs. Конкретный каталог БД и версии утверждает общий schema module до реализации.
2. В Connect добавить устойчивую запись revoke/policy epoch в том же commit, где меняется доступ. Observer после commit остаётся ускорителем, не единственным источником истины. Сейчас observer вызывается после commit, ошибки подавляются (`modules/connect/server/index.mjs:706`); подписка host направлена только в apps (`server/http-app.js:95`).
3. В capabilities service принимать только доверенный Principal из host adapter. Не выдавать служебному клиенту фиктивную подписанную Connect installation. Текущий Notes entrypoint требует `actor.accountId/deviceId` (`modules/notes/server/index.mjs:110`), поэтому новый trusted domain entrypoint/адаптер должен быть явным и отдельно проверенным.
4. Reserve Invocation + постоянный внутренний requestId + outbox intent до вызова job store. Current account-job identity = `accountId + requestId`, поэтому caller key не передавать напрямую. Fingerprint включает capability version/digest, execution binding/target, нормализованный input, эффекты и выбранные ресурсы. Смена контракта/target не должна незаметно переисполнить старый ключ.
5. Dispatch использует тот же внутренний ID при каждом восстановлении. Crash после commit job до сохранения jobId разрешается lookup/repeated admission по исходной request identity; второй job не создаётся. Intent/outbox доставляет команду, но не владеет execution lease/retry.
6. Для Notes заранее резервируются noteId и mutationId; `expectedRevision=0`. Receipt/tombstone Invocation сохраняется независимо от ограниченного кольца note receipts. После edit/delete возвращается исторический эффект, не новый текст и не воссозданный объект. Create-only не открывает list/read/update/delete; cancel после commit не является purge.

Инварианты и экспорт: сохраняются все Connector request identities и результаты, Connect revoke/recovery states, account-scoped notes и legacy ciphertext. Новый encrypted backup включает capabilities ledger, policy epochs, outbox, budget reservations, minimal receipts. Сверка до/после — counts, ID sets, normalized hashes/equality в памяти; чувствительные тела не попадают в квитанцию.

Rollback: выключить новые adapters и admission; читать последнюю live БД совместимым reader. Current account jobs уже требуют v4-capable reader (`connector-store.js:205`). Старый образ, который отвечает `/health` и не читает store, непригоден. Новые форматы получают свой минимальный reader; schema downgrade и возврат старого backup не происходят автоматически.

### P3 — App identity, имена и аудитории

1. Ввести проверяемую миграционную lineage/schema metadata для apps: сейчас таблицы создаются через `CREATE TABLE IF NOT EXISTS`, отдельного развитого versioned migrator нет (`modules/apps/server/index.mjs:22`). Перед backfill валидировать существующую структуру, не считать неизвестные строки пустой базой.
2. Сохранить `local_apps.id`, owner, connector binding, текущие grants/revision/revoke. RuntimeTarget представляет существующий loopback-процесс, не создаёт другую App. Старые `app-<id>` origins/deep links остаются разрешимыми.
3. Additive AppDomain/alias/tombstone и аудитории. Прежние приложения не становятся публичными и не теряют индивидуальные/community grants. Переименование карточки не меняет origin; старое имя не переходит постороннему автору.
4. Разделить shell/auth site и runtime site; registry-controlled DNS/TLS, cookie/origin/iframe contracts и реальный чистый браузер — отдельные gates. Текущий CSP запрещает media permissions, workers и внешнюю сеть; нельзя просто объявить совместимость с любым проектом.
5. Самостоятельные app conversations добавляются как новый контекст. Прежний community chat и его история не копируются в публичный app chat. Launch, membership, chat и machine capabilities имеют разные разрешения.

Экспорт: app registry + grants + domains/aliases/tombstones + разговоры, затем нормализованная сверка ID/ACL. Runtime HTTP/WS sessions и tickets не являются переносимой backup-идентичностью: после restart нужен новый launch. Сейчас предел 100 app rows на owner включает revoked; изменение квоты требует отдельной проверяемой retention policy, а не удаления IDs.

Rollback: отключить новые публикации/видимость; сохранить разрешение старых адресов и новые данные разговоров. Для live source нет гарантии неизменяемой сборки. Code rollback не должен незаметно открыть чужую аудиторию или передать hostname новому владельцу.

### P5 — реальное исполнение policy

1. Сохранить trusted owner runtime как отдельный явно широкий режим. Поля `job.permissions` сейчас metadata, runtime их не читает (`connector-store.js:825`, `soty-connector.mjs:576,928,1126`). До enforcement не рекламировать read-only/workspace sandbox для delegated compute.
2. Ввести проверяемую ограниченную идентичность процесса, roots и symlink/junction checks, env allowlist, scoped model credential, egress, CPU/RAM/time и процессную группу/дерево по каждой поддерживаемой ОС. Смена cwd не является sandbox.
3. Durable policy epoch/lease и локальный watchdog. Потеря сети не повышает delegated policy до owner. Сейчас cancellation watcher продолжает работу при transient errors (`soty-connector.mjs:510`), а Unix fallback убивает direct child (`:806`). Не обещать подтверждённую остановку всех потомков до испытания.
4. Budget enforcement связывает уже атомарно зарезервированный максимум с конкретной provider attempt. Учитывать проигравшие race attempts и compatibility retries; неизвестный charge удерживает резерв. Отсутствие enforceable верхнего предела блокирует hard-cap режим. Current proxy разрешает отсутствие max_tokens (`gonka-proxy.js:229`), race запускает нескольких providers (`inference-relay.js:170`).
5. Runtime release provenance и staged compatibility проверяются отдельно от подписанного Connect module. Не менять установленные clients и не генерировать release в P0.

Экспорт/инварианты: текущие connector identities, grants, request ledger, job state/results, pinned runtime version, policy snapshot/lease receipt и budget reservations. Не экспортировать env в audit или prompt. Уже uncertain job не клонируется и не запускается заново при обновлении.

Rollback: запретить новые delegated jobs несовместимому runtime, дождаться/reconcile уже принятых; сохранить историю. Не использовать старый широкий executor как незаметный fallback ограниченному поручению.

### P6A/B/C — типизированный backend, async и hosting

P6A: registered execution binding к backend автора; отдельный loopback control port, эксклюзивно исключённый из публичного app relay. Current proxy фильтрует сырой `/_soty/` prefix (`modules/apps/server/index.mjs:239`), а `requestPath` возвращает исходный encoded path (`protocol.mjs:14`); одного такого startsWith недостаточно для нового служебного endpoint произвольного framework. Выделенный port + signed context проще доказать.

Сначала compatible reader/runtime со strict reject unknown и advertised executor/version, затем новый typed HTTP job. Сейчас persisted неизвестный kind отвергается store (`connector-store.js:690,793`), но runtime превращает неизвестный kind в `agent` (`soty-connector.mjs:456`). Никаких shell/curl/OpenCode обходов. Context связывает Invocation, binding, app/capability digest, устройство, principal/grant chain/epoch, input hash, audience/resources/effects, expiry/deadline. SDK durable dedupe по Invocation не запрещает безопасный retry как «повтор одноразового токена».

P6B: исходный Invocation/job переживает закрытие внешнего клиента. Проверяются start/status/cancel/result, lost ACK, поздний ответ, partition, отзыв и отсутствие второго эффекта в двух реальных клиентах. SDK local receipt не создаёт вторую платформенную очередь. Неподтверждённый авторский результат не называется независимо проверенным.

P6C: независим от наличия agent API. Bounded archive/upload/build, изолированная сборка, immutable artifact, atomic promote, previous version. Static UI availability отдельно от авторского backend/device. App ID, адрес, grants и разговор остаются; app database/user data не откатываются вслед за кодом.

Экспорт: binding/version/digests, connector compatibility, jobs/SDK dedupe identity, immutable artifact inventory и active deployment pointers. Секреты runtime и данные автора имеют отдельную encrypted backup policy. Отозванный binding не оживает после восстановления старого snapshot.

## 4. Предварительный интерфейс для независимой реализации P1

Распределение согласовано с интегратором: `agent_ecosystem` — общий schema/index/access/validation/catalog; `publishing_architecture` — `modules/capabilities/server/invocations.mjs` и отдельные tests; интегратор — HTTP/Connect/Notes glue. Имена методов ниже — контракт для согласования перед кодом, не уже существующий API.

`createInvocationOperations({ db, clock, authorize, canonicalHash, limits })` получает общую capabilities БД. Factory не открывает другой файл, не публикует HTTP и не выполняет jobs. Все state-changing calls синхронны внутри общей service transaction; нельзя удерживать SQLite transaction через сетевой await.

| Операция/стык | Обязательное поведение |
|---|---|
| Admit | Recheck trusted Principal + действующий grant/parent chain, reserve budget, insert Invocation и intent одной транзакцией. Actor/account не из payload модели |
| Request identity | Уникальность выбранного account/client/request scope; digest фиксирует operation/version/target/input/effects. Same identity+same content возвращает прежний Invocation; другие байты/семантика — conflict |
| Dispatch intent / bind job | Постоянный internal requestId из Invocation. Повтор доставки восстанавливает тот же job. Execution leasing/retries принадлежат Connector store, не outbox |
| Cancel | Не отправленный intent можно прекратить; ambiguous dispatch сначала reconciles. Наличие cancelRequested не означает отсутствие job или эффектов. Уже commit Notes не удаляется через create-only grant |
| Observation/receipt | State и effect state отдельно. Не переписывать подтверждённый terminal effect поздним transport failure. Artifact refs и verificationMethod; bodies по умолчанию отсутствуют |
| Budget reserve | Atomic compare/reserve по всем применимым account/grant-chain ограничениям и stable attempt identity. Потомок не получает новый независимый остаток. Caller не задаёт доверенный limit |
| Budget settle | Финальный charge и возврат неиспользованного резерва только при известном исходе. Uncertain удерживает резерв; repeated settle idempotent; расход нельзя вернуть по одному abort |
| Read/reconcile | Host проверяет актуальный доступ; create-only видит только собственные status/minimal receipt. Internal reconciliation не получает полномочия через legacy room link или выдуманный deviceId |

Для P1 нужны fault tests между reserve/dispatch/job commit/bind; concurrent same-key calls; revoke до admission/lease; parent budget shared across parallel attempts; stale epoch; note edit/delete before receipt recovery; create-only result redaction; unknown executor и incompatible reader. Shared schema migration и fixture builder должны принадлежать одному владельцу файлов.

## 5. Реальные fixtures и кандидаты первых проектов

| Найденный код | Реальная возможность | Граница применения |
|---|---|---|
| `modules/apps/examples/sample-app.mjs:8` — «Покупки» | Отдельный Node HTTP/WS проект без зависимости от Сот; loopback `/`, `/api/items`, `/live`; mutations и UI работают; transport tests используют его напрямую | Состояние только в памяти; тестовый Set-Cookie, large/slow/unsafe redirect routes. Хороший инженерный P3 fixture, не production-safe каталог и не доказательство независимого автора. Для первого bounded P6A подходит узкое чтение списка после SDK binding; существующий toggle POST не является идемпотентным контрактом |
| `modules/notes` + `src/world/notes.ts` | Настоящие личные записи с CAS/mutation/outbox, первый полезный встроенный P4 pilot | Не самостоятельный опубликованный внешний backend. Create-only adapter ещё предстоит; исторические E2EE комнаты отдельно |
| `src/features/chess.ts` / legacy sync | Существующая встроенная игра как кандидат сохранения в новой оболочке | Не найден отдельный deployable manifest/backend; не выдавать за готовый независимый app publishing pilot |
| `modules/apps/test/connector-runtime.test.mjs:16` | Полный установленный из исходника connector, exact-Origin claim, signed Connect, HTTP/WS | Синтетическая среда; входит в общий world suite интегратора, повторно здесь не запускалась |
| `server/test/app-agent-runtime.test.mjs:16` | Opt-in реальный установленный OpenCode с локальным управляемым inference fixture и созданием manifest | Не проверяет настоящего paid provider. Не запускался в этой зоне; нельзя засчитать skip за AI acceptance |
| `scripts/world-agent-browser-fixture.mjs:10` | Browser fixture своего помощника, локальная модель, отдельный workspace | Пишет в `output/world-validation-20260928`, требует отдельного контролируемого запуска; в этом P0 не запускался |

В пределах разрешённой release-копии **не найден набор 3–5 независимых production-проектов и не выбран независимый автор P6A**. Встроенные Notes/Chess и HTTP fixtures нельзя посчитать такими авторами. Упомянутый пользователем Тавыш расположен вне этой проверки; владение, stack, source/runtime и требования auth/media должны быть подтверждены интегратором на соответствующем проекте. Traffic/Xray/SpreadEx administrative integrations не предлагаются в публичный каталог ради числа приложений.

До P3 заполнить для каждого выбранного настоящего проекта: автор/право публикации, source revision, команда/среда запуска, HTTP+WS пути, cookies/OAuth, внешние hosts, microphone/camera/SW, state persistence, device-offline semantics и чистый browser proof. До P6A отдельно подтвердить поддерживаемую ОС backend автора, выделенный закрытый control port и typed action без произвольного shell.

## 6. Backup/restore readiness — доказанное и открытое

Безопасно прочитаны только исходники и документы: `docs/connector-transport-migration.md`; `deploy/connector/README.md`; `deploy/connect/README.md`; `deploy/connect/DEPLOYMENT-20260928.md`; `deploy/apps/README.md`. Конфиги, `.env`, private keys, live histories и DPAPI storage не открывались.

Существующий backup (`deploy/connect/backup.mjs:45`) требует остановленный точный container и отсутствие другого writable consumer тома; rootfs/тома helper read-only, network none, единственная capability DAC_READ_SEARCH. Снимок включает весь `/data`, исходную runtime-конфигурацию и известный read-only application-token mount; AES-256-GCM + RSA-OAEP-SHA256. Ключ оператора хранится отдельно в DPAPI и **не восстанавливается из server backup**.

`verify-backup.mjs` проверяет аутентичность, metadata, весь tar, безопасные пути, SQLite headers и наличие connector store; не выдаёт success до завершения проверки. Это **не** полноценный запуск восстановленного сервиса и не domain-level доказательство целостности всех SQLite relationships. Текущий локальный прогон подтвердил encryption/decryption/validation на синтетической копии; рабочая production-копия не расшифровывалась и не восстанавливалась.

Историческая квитанция 28.09.2026 сообщает о проверке encrypted snapshot, 1078 archive entries, 912 room files и сохранении 333 jobs. Это датированное свидетельство прежнего выпуска, не текущий baseline данных и не доказательство наличия сегодня доступного recovery key. Не переносить эти числа в новые assertions.

Перед любой настоящей миграцией требуются:

1. Свежий stopped/single-writer snapshot и сравнение его безопасного digest, без вывода секретов. Не копировать живую SQLite только по основному файлу, теряя WAL; архивировать согласованное состояние всех writers.
2. Проверка доступности recovery key установленным операторским механизмом; расшифрованные конфиги не записывать незашифрованной резервной копией.
3. Restore drill в изолированной среде без outbound/model/public DNS: восстановление всех DB/room files/runtime metadata, compatible reader, SQLite integrity и доменные ID/ACL/tombstone инварианты, ожидаемое состояние queue.
4. Не оживлять уже отозванные устройства, использованные recovery proofs, удалённые notes или выполненные задания. Более свежие revocations/request identities после snapshot требуют отдельного reconcile, а не слепого restore.
5. Обычный code rollback использует последние данные. Старые pending/assigned/uncertain jobs не отменяются и не клонируются ради обновления. Maintenance допускает только доказанное отсутствие active work либо точный проверенный never-leased набор согласно существующему контракту.
6. Отдельно обеспечить данные устройств: workspaces, локальные app databases, connector identity и состояние собственных проектов не покрываются server volume автоматически.

## 7. Сдача P0 и предел дальнейшего допуска

Локально подтверждены durable store/fault recovery, no-replay uncertainty, синтетическая cancellation, bounded relay outputs, model routing resilience, encrypted-backup validation и edge-config gates. Найдены конкретные точки расширения и reader/runtime риски; реализация следующих этапов в этом файле не скрыта.

Остаются внешние/отдельные условия: свежий production restore drill; подтверждённые 3–5 проектов и независимый автор; Linux/Docker filesystem proof на выбранном образе; реальные Windows/Linux sandbox/stop/lease гарантии; DNS/TLS и браузерная cookie/media совместимость; два реальных внешних клиента; измеренные денежные/нагрузочные пределы. Их отсутствие не мешает писать и локально тестировать P1, но запрещает объявлять соответствующие production gates выполненными.

Интегратору: объединить этот отчёт с его build/typecheck/world/connect и браузерным baseline, результатами остальных специалистов и независимой приёмкой P0. Перед стартом P1 утвердить shared schema и factory boundary из раздела 4. Никакая оценка «100/100» не заменяет перечисленных доказательств и честного статуса неизвестных.
