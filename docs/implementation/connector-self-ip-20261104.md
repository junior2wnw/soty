# R09: настоящий IP выбранного коннектора

Дата: 2026-11-04. Реализовано в изолированном worktree `soty-live-platform-20261104`; не опубликовано. Исходное требование: ручной IP/порт должен подключать настоящее приложение и давать соту на едином поле, без молчаливой подмены адреса на loopback.

## Граница первой версии

Поддерживается HTTP на literal IPv4 сетевого интерфейса выбранного собственного коннектора. IPv4 проверяется свежим `networkInterfaces()` перед probe и каждым TCP соединением, фактический `remoteAddress` совпадает с ним. Порт1024–65535 с существующим списком исключений; прежние localhost и selected Native работают по прежним контрактам. Занятый Native порт не становится доступным через другую запись IP. Произвольный LAN/public target без собственного коннектора, DNS, IPv6, HTTPS и переход на другой origin не реализованы.

Выбор этого ограничения основан на эксперименте: прежняя реализация принимала лишнее поле host, но обращалась к127.0.0.1. Два синтетических HTTP сервера на одном порту self-IP/loopback возвращают разные маркеры. Исправленный путь видит только self-IP маркер; намеренно сломанный коннектор с игнорированием host не проходит тот же registration probe и не создаёт приложение. Результаты: `D:/глава/РАБОТА/2026-11-04-soty-live-platform/research/r09-ip-source-results.json` и `r09-universal-proof2.log`.

## Контракты и права

* Профиль `soty.relay-self-ip.v1`; Source DTO `soty.app-http-source.v2`; target DTO `soty.runtime-target.v2`; channel transport остаётся v2. В digest входит scope/scheme/family/address, прежние v1 digest не меняются.
* `GET /api/apps/runtime-capabilities` — bounded, no-store metadata. Constructor `allowSelfIpTargets=false` по умолчанию. Новый коннектор не рекламирует IP профиль, если Root его не объявляет.
* Owner-only `apps.devices` возвращает current-channel `sourceEndpoints`: offline/upgrade-required/pending/ready, адреса и witness. Публичный capability JSON не раскрывает IP и не выдаёт права.
* `apps.registerEndpoint` — асинхронный подписанный Connect путь. Стабильный requestId определяет семантическое намерение, изменённый payload конфликтует. Probe без SQL/Connect lock; commit повторно проверяет оригинального actor, владение, текущий канал/OS inventory и непереносимый prepared proof.
* В одной Apps транзакции создаются app, target, endpoint и receipt. До COMMIT нет успешной нотификации. Потерянный ACK повторяется по прежнему requestId, без нового appId; replay не требует живого probe.
* Universal completion устанавливает feedback через существующий app-contract. Затем текущая creator форма подтверждает аудиторию и ставит настоящую соту на поле. Account-scoped local journal не хранит токены; schema2 содержит IP-намерение, schema1 сохраняется для localhost.
* Hosting Source prepare/promote/retained rollback поддерживают адрес как часть настоящего target. При удалении интерфейса поток закрывается; другой адрес не подставляется.

## Связанные host-only ports

`checkWidgetRuntime({widgetId,appId,actor:{accountId,deviceId},entry,target,phase})` проверяет текущую защиту обычных виджетов перед созданием session, запросом и выдачей ответа/stream/WS frame/renew. widgetId приходит только из Root-private ticket/session; JSON контента не выбирает его. `invalidateWidget({widgetId,accountId?,deviceId?})` отзывает соответствующие tickets, sessions, renewals, streams и sockets. Apps Source ACL остаётся обязательной, отдельное основное приложение не блокируется из-за защиты его виджета.

Для audience-only обновления universal descriptor используется durable `publication-journal.sqlite`. Только сохранённый доверенный pre-publication anchor при неизменных target revision/digest/profile/entry и sourceChangeEpoch допускает reconcile. Сохраняются descriptor/modules и feedback installation. Source A→B→retained A остаётся held, неизвестный stale также held; повтор после lost ACK и перезапуска не заменяет более новый явно принятый descriptor. `withPublicationState/reconcilePublication` — только host ports, не MCP/RPC операции.

## Приёмка и доказательства

| Обязательное свойство | Наблюдаемый критерий | Проверка |
|---|---|---|
| Настоящий ручной IP | HTTP и WS возвращают self-IP маркер, счётчик localhost равен0 | `server/test/self-ip-root.test.mjs`; built bundle case `self-ip-installed-root.test.mjs` |
| Не принять молча игнорируемый host | Тот же probe проваливается с намеренно сломанным коннектором, app не записан | `self-ip-root.test.mjs`, ignored-host negative control |
| Полный creator результат | Реальный app + требуемая audience + placement после retry/reload | signed Root case, `src/world/app-connect-flow.test.mjs` |
| Нет дубля при unknown ACK | Один app/receipt после retry/restart; изменённый payload отказ | `modules/apps/test/self-ip-schema.test.mjs`, signed Root case |
| Revocation/account fences | Отозванный actor/current channel и иной owner не проходят commit | schema+Root tests, onboarding/account-lifecycle cases |
| Source pin и совместимость | v1 bytes прежние; self-IP меняет v2 digest; prepare/promote/rollback реально меняют endpoint | schema+Root Source case |
| Виджет прекращает старый runtime при lock | Held HTTP не отдаёт secret bytes, WS закрыт, old ticket/cookie отказ; main app доступно | signed Root runtime case, `r27-runtime-root.log` |
| Public→automatic feedback | После publication готовый universal и настоящий feedback submit, installation прежний | `server/test/universal-publication-root.test.mjs`; Root creator case |
| Нельзя автоматически принять другой Source | A→B→A и unknown-stale остаются held; newer descriptor не заменяется | universal publication Root tests |
| Старый reader отказывает явно | Exact probe/reader feature gates, offline snapshot сохраняет новые stores; missing guard/extra schema/future version отказ | `server/test/storage-live-root.test.mjs`, deployment storage tests |

Проверки воспроизводят настоящую локальную сеть, signed Root и установленный release bundle в изолированном dataDir. Они не являются физической Android или визуальной browser приёмкой текущего UI. Полный Linux Docker cold-boot/canary и совместимый production image не выполнены; старые исторические образы нельзя считать совместимыми по одному новому label.

## Версии и артефакты

Коннектор1.4.17: `public/agent/soty-connector.mjs`, SHA256 `1f307a11bc7015534ff845c506c0716204ef52391608d780ff2244142bc9be7f`, 270796bytes. Локальный build/selftest и update1.4.16→1.4.17 проверены; установленный пользовательский коннектор и production не менялись.

Endpoint overlay `soty.apps-self-ip-targets.v1` открывается только при явном admission; прежний Apps reader реально отказывает новому DDL. Host storage DTO/reader epoch8 перечисляет19 отдельных stores/features и требует полный probe перед START. Точные поддерживаемые версии — в `deploy/connector/storage-guard.mjs`, нормализованные DDL pins — в `storage-probe.mjs`; текущий image label обязан им совпадать. После отдельных DDL freeze допущены Notes3/Materials2/Vault2, включая19 объектов схемы Vault2 с header/upload, cipher chunks и conversion intents/commands. Миграционные flags по умолчанию выключены.

## Допуск миграции защищённого содержимого

`allowNativeVaultMigration=false` остаётся стандартным поведением Root; локальный dev-флаг не включён этим изменением. Canonical файл Vault — `DATA/canvas/vault.sqlite`. Exact Vault2 normalized DDL pin: SHA256 `ca53acb06500b5f4daa01f3ea36a86467cc06a5f66504509614a03f057aa8712`,19 объектов,6075bytes нормализованной JSON проекции схемы. Новая декларация `canvasVault:[1,2]` относится к проверенному исходному коду, а не к старому контейнеру с заменённым label.

Перед разрешённой миграцией нужно остановить приём новых действий, завершить или явно разобрать pending writes, отключить старые клиенты и runtime/WS, остановить **все** старые binaries, writers и держатели SQLite. Проверенный новый reader guard не исправляет уже работающий старый binary, который не перепроверяет epoch. Затем нужны согласованный зашифрованный backup/restore, точный допуск всех readers по immutable image ID и offline probe, миграция одним разрешённым writer и повторная проверка до возврата клиентов. Текущий новый код также проверяет смену формата у уже открытого service, но это дополнительная защита, а не замена остановке прежнего кода.

Малый isolated Root gate: populate Notes2/Materials1/Vault1 → явный Notes3/Materials2/Vault2 migration → настоящие Note/PNG conversion → restart с флагом false → unlock/read → offline snapshot с SHA256-сверкой файлов → запуск Root с копии и повторный unlock/read. Старый literal Vault1 reader, reader declaration без Vault2, future version, неизвестная таблица, изменённый header column и отсутствующие chunk/conversion guards отказывают. Этот gate проверяет небольшие данные; предел25МиБ и chunk512КиБ остаются контрактными пределами, а не результатом этого малого эксперимента.

Production образ, Linux cold-boot/canary и миграция production не выполнены. Общая Windows deployment матрица остаётся незелёной:10 подтверждённых `directory.sync()`/EPERM сбоев installed-module rotation; durability не ослаблена. Снимок кода и новые reader labels не означают допуска к production или безопасного отката старым binary на новые данные.

Логи и краткое состояние: `D:/глава/РАБОТА/2026-11-04-soty-live-platform/research/r09-implementation-state.md`. Canonical maintained Source capsule: Planner commit `6c614feb489767266ce23676b2bc0df6342f8370`,86 exact Git blobs; integrity, locked install и frontend build проверены в `test-fixtures/planner-source-ci`. Это проверка fixture pin, не подключение production Planner.
