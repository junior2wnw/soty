# P4-D2 — полный application image и совместимый code fallback

01.10.2026. **Source-only план, без build/runtime/tests/SSH/config/key reads.** Изменён только этот документ. Продолжает [release/recovery plan](p4-release-recovery-plan.md); не создаёт второй orchestrator и не означает START, migration или release acceptance.

## 1. Два точных артефакта

| Артефакт | Предлагаемый source и назначение |
| --- | --- |
| C | Base `dda44868fd708f87e5ac2b4f4f854fa05caca2f2`; новая отдельная frozen build revision после reviewed metadata/context дельты ниже |
| R | Base `2103d755c081f03cdad98959fe593c6151ea2eec` (C2b); своя frozen build revision с той же честной metadata/context дельтой, не переименование исторического image |

Сравнение этих base commits по `modules`, `server`, `src`, Dockerfile/package/lock и deploy: product delta — `server/capabilities-mcp.js` допускает Codex revision2025-06-18 и связанные tests; дополнительно R0 verifier API/tests в deploy. `.dockerignore` исключает deploy из application image. Notes/Caps schemas, row validators, OAuth/original-authority и UI в сравненном срезе совпадают. R теряет C2c June negotiation: годится как **AS-off recovery candidate**, не как обещание полной внешней функциональности C.
Git tree `modules/connect` у обоих base commits одинаков: `8e4f924b31f7dac95d0e40070c1dc01df902eb10`; это Git tree ID, не подмена алгоритма `moduleTree()`. R ещё не проверенный fallback. C=R по binary был бы только restart/config recovery.

Root фиксирует C_build/R_build commits, clean tracked build-context SHA, Dockerfile/lock hashes, platform `linux/amd64`, resolved base manifest/config digests, pnpm10.30.0 и Xray artifact hash. `REVISION` указывает на действительную build revision. Итог — два immutable image IDs/digests и selected image metadata; runtime config/secret-bearing pins остаются в protected control evidence. Все последующие проверки/запуски используют эти digests, не tags.
Node base сейчас pinned `node:24-trixie-slim@sha256:4f2b45e32dc7d2caf66b6dbd59fac50e32f8077769efe0ef4d4c3f114672537d`; Alpine/Xray pins уже в Dockerfile. Apt/apk repository resolution и timestamps пока не обеспечивают bit-for-bit rebuild. Сохраняются resolved package versions/build inputs; acceptance относится к полученному digest, повторная сборка требует нового gate. Новый reproducible-build framework здесь не нужен.

## 2. Узкие source gaps перед build

1. `Dockerfile:39` и `deploy/connector/storage-guard.mjs:7` пока объявляют Caps `[1,2]`. Root согласованно вводит proposed manifest envelope3 с Rooms `[1,2]`, Apps `[1,2,3,4,5,6]`, Notes `[1,2]`, Caps `[1,2,3]` **до** сборки C/R; сам recognizer3 уже принят. Old O/исторические manifests неизменны.
2. Обнаружен source-level build blocker: `.dockerignore:14` исключает весь `deploy`, но `Dockerfile:8` запускает `world:test`; `modules/notes/test/native-storage.test.mjs:8–9` и Apps schema tests импортируют historical fixtures. Нужен build-only allowlist точной closure ниже для обоих build sources. Не удалять tests, не включать весь deploy/trust/secrets и не копировать эту closure в final runtime. Это ещё не actual Docker RED.
3. Clean context не включает `.env`, ignored var/output, личные homes или keys. Существующие Docker build gates сохраняются; root выполняет их последовательно с измеренным build resource budget. Dependency downloads при сборке отделены от offline application gate; model/provider calls не допускаются.

Проверены source entrypoints всех RUN: install, typecheck, connect/world/dev tests, identity/inference selftests, обе connector selftests, build/prebuild, prune, traffic download и runtime apt. Для excluded deploy closure нужны **ровно шесть файлов**; пути ниже относительно `deploy/connector`, Git blob IDs совпали у обоих base commits:

| Build-only файл | Exact Git blob |
| --- | --- |
| `apps-v2.fixture.mjs` | `9f192e67e0d4565a413cb5a6b3e3a43b63bb9657` |
| `apps-v3.fixture.mjs` | `2e33ab6e4c2359c3574a82948227edbbf9727dca` |
| `apps-v4.fixture.mjs` | `3eeb3efdbdacf59e59018bd58c20ec29ba0b5487` |
| `apps-v5.fixture.mjs` | `cd64db7f00206e8aeb21b4c1e64f988c36eea3af` |
| `notes-v1.fixture.mjs` | `64ab365eb786eed9887f37e1ae93e172af66f271` |
| `fixtures/notes-v1/schema.mjs` | `faaa676340136a2fcecda259849534e03a0e6e70` |

Apps chain `5→4→3→2`; Notes literal/old schema local imports не имеют. `native-storage.acceptance` читает old Notes schema по URL и проверяет SHA. Реальные server-test dependencies `support/documentation.mjs`/`oauth-artifacts.mjs` не требуют deploy; `capabilities:test`, `connect:release:test`, `connector-readiness-selftest` не вызываются этим Docker RUN. Поэтому Caps fixtures/runtime.mjs и provenance directories для этого build allowlist не нужны; provenance остаётся в pinned source. Смена commands/dependencies требует повторной source closure check, actual clean build ещё впереди.
Final COPY не переносит `/app/deploy`. Однако нынешний COPY `server`/`modules` уже включает их test directories/внутренние historical fixtures: этот план не объявляет runtime полностью очищенным от прежних tests; новая deploy closure туда не добавляется. Полное packaging cleanup не нужно для исправления missing imports.

Manifest входит в **проверяемый** image: после PASS не пересобирать «только label». Label — декларация кандидата, а не результат его acceptance. Публикация/admission именно этого digest следует после proof; нынешний unlabelled O не получает новый label.
`HostController.validate()`/`activate()` переиспользуют `images[ConnectTree]`; whole-app C/R не проходят через этот lookup. Используется согласованный typed first-transition с explicit C/R pins и early recovery dispatch, затем journalled initial `tree → C` в новой generation. Old mapping/module sequence сохраняются; нет overwrite, fake Connect bump или дополнительной deployment loop.

## 3. Offline exact-image gate

Один небольшой root-owned сценарий использует existing Docker API/guard/journal primitives и frozen fixture closures. Fixtures/witness script монтируются отдельно read-only; application запускается штатным `node server/index.js` из C/R, без замены его modules/entrypoint на host implementation. Проверяемый image сам предоставляет Node/SQLite/FTS и packaged dependencies.
Контейнеры имеют `network:none`, без published ports, serving mounts, Docker socket и устройств/executor. Loopback HTTP/WS доступен только внутри namespace для synthetic probes; никакого внешнего model/provider/AS/token/Note-create действия. All-store inspection не добавляет production endpoint.
Конкретный gate transport — один scoped Docker exec/attach adapter родительского controller через существующий daemon channel: exact app ID проверяется, owned `node /run/owned-witness.mjs` из pinned RO mount обращается к loopback **внутри** app namespace и возвращает один allowlisted JSON ≤64KiB. Exec spec/intent записываются до exec-create, полученный exec ID — до exec-start; attached stream bounded, успешный exit подтверждается exec-inspect. Это не замена app entrypoint и не host-loopback `productionReady()`. Нужен отдельный causal gate transport; existing `helperOutput('/logs')` при LogConfig:none неприменим.
Raw app stdout/stderr получает только bounded live attach collector в RAM ≤1MiB; наружу идут fixed failure codes/allowlisted fields, остальное уничтожается без dump. Overflow/оборванный attach не становится успешным diagnostic proof. Ни Docker logs, ни host files с raw application output не создаются; private witness/config/per-file hashes остаются в protected control channel/journal.
Первый профиль: native Notes execution=false, OAuth issuance=false, migration options остаются default false; `SOTY_CAPABILITY_AUDIENCE` пуст для synthetic baseline. На реальной копии прежние issuer/resource/config сохраняются, а surface закрывается approved ingress/config; не переименовывать authority ради offline теста. Keyless AS-off reopen допустим; AS ciphertext/decryption proof этим не объявляется.
AS-off не равен read-only: штатные Apps/Rooms migrations, native proof reconciliation и допустимая OAuth cleanup могут писать на owned copy. Это измеряется, а не подавляется ради byte equality. Notes1/Caps1 и Caps2 не должны скрыто переходить в2/3; явные переходы — отдельные шаги ниже.

| Минимальная матрица | Нужный witness на C и R |
| --- | --- |
| Legacy / fresh | Frozen historical formats покрывают каждый объявленный Rooms1–2/Apps1–6/Notes1–2/Caps1–3 без полного Cartesian product. Fresh defaults и populated old fixtures проходят штатный startup; разрешённые startup migrations фиксируются |
| Committed WAL / mixed | Genuine main1/WAL2 Notes и main2/WAL3 Caps, mixed Notes1/Caps2–3 и Notes2/Caps1–3; Apps source/discussion и Rooms payload witnesses. Host RO guard видит committed state до application open |
| Current authority/effects | Notes owner edit/delete + immutable native proof, Invocation original credential/receipt/root budget, delegated lineage/revocation; lawful OAuth expiry/retention/used refresh namespace и revoked rows. Seeded pending Caps + уже existing Notes proof должны штатным timer перейти в committed/input-purged без нового Note |
| Failure | Unknown future, обязательный missing/altered guard, plain row/FK corruption отказывают; не repair/marker downgrade. Malformed legacy Room остаётся точным on-demand отказом, source сохраняется |
| Cold restart / code fallback | C открывает owned generation → confirmed stop → R на **последних** файлах C → confirmed stop → cold R reopen. Сохранены идентичности, доступ владельца, proofs/receipts/budgets и revoke; O не стартует |

Перед START: exact image/label/config/mount identity, fresh strict guard и baseline witness. После START: exact running PID/container/image, actual Node/SQLite/FTS, реальный shell/static assets и bounded synthetic reads через existing interfaces; Rooms lazy paths проверяются явно. Native reconciliation принимается после наблюдаемого committed/input-purged с сохранёнными original IDs и неизменным Note count: ждать условие до deadline после штатного initial tick1s, не sleep, быстрый `/health` или ручной вызов coordinator. OAuth cleanup tick10s проверяется отдельно только при соответствующем claim. После confirmed stop сохраняются all-store identity/format/row witness и ожидаемая startup delta.
`/ready` сейчас требует model proxies, а `productionReady()` охватывает Connector/Connect, не все stores. Не подкладывать fake provider credentials ради green readiness. Offline proof записывает model readiness отдельно; domain readers + scoped reads дают all-store evidence. Connect/World/connector-store и остальные retained `/data` files входят в inventory, хотя новый reader manifest их не аттестует.
Per-store project/registry IDs, форматы, row hashes и связи original proofs/receipts сверяются в protected control evidence; публично — только разрешённые §5 boolean/count/artifact fields, без bodies, credentials, encrypted artifacts или ключей. Schema/host recognizer PASS не заменяет полный app row admission и не доказывает AS decrypt.

## 4. Serving baseline и отдельные migrations

Synthetic gate выше готовит инструмент, но не заменяет R1 restore: после остановки writers **до backup** root фиксирует независимый cold-source witness (`generationId`, `checkpointSha256`, `inventorySha256`) по required logical inventory/account/store/external identities. R1 manifest producer не является единственным oracle полноты. Совместный omission одной обязательной DB из tar **и** manifest обязан отказать по этому witness.
Согласованная cold encrypted B всех stores/external dependencies → authentic isolated restore именно B → те же точные C/R application gates. Restored B до START сверяется с тем же frozen source inventory; C/R после startup — с теми же identities и только явно ожидаемыми migration/reconcile deltas. Original serving volume до этого не монтируется в candidate. Existing R0 PASS10 не заменяет этот restore.
После actual first-transition/handoff C становится совместимым SAME-old baseline следующего обычного обновления. До первого production2/3 COMMIT R уже должен принимать final formats3 и original authority. Новая B при изменившихся данных/restore → explicit Notes1→2, Caps1→2, затем Caps2→3; per-store transaction и mixed crash/reopen проверяются без объявления cross-file atomicity.
C и R **не меняются** между before/after-migration проверками. Fallback — guarded R на latest generation, AS/native admissions закрыты. Если R не принимает её, fail-stop; old O/downgrade/автоматическое восстановление B не являются fallback. P4 external enable и D1/D2 client acceptance остаются отдельными gates master.

## 5. Конечные ресурсы и receipt

До каждого CREATE root сохраняет owned name/spec/labels/intent, image/config/witness pins и measured bounds; Docker-assigned ID фиксируется после exact create/readback и **до START**. Для run заранее заморожены число CREATE/START/exec и owned namespace. Один app writer одновременно; отдельные owned volumes для fixtures и restored B, никаких copies между живыми writers.
Стартовый application profile для synthetic gate: CPU1, RAM512MiB, PIDs64, readonly root, tmpfs32MiB, `LogConfig: none` + bounded collector1MiB, readiness120s и общий controller deadline10min. Writable paths явно ограничены `/data` и необходимым tmpfs. Реальная копия требует измеренного disk/resource budget; volume bytes — проверяемый admission/monitor bound, не выдуманная Docker quota. При нехватке — отказ/пересмотр profile до нового run, не silent capraise.
Intent сохраняется до mutation, lost ACK не повторяется; timeout не доказывает exit. Exact stopped/removed/OOM/state/mount readback и cleanup каждого owned helper/volume обязательны; unknown остаётся retained/reconciliation-required. Existing journal/control ownership переиспользуется, новый release daemon не создаётся.
Receipt раздельно фиксирует build digest, synthetic app proof, restored-B app proof, first serving handoff и migrations. Public projection — только allowlisted image/source/ciphertext hashes, counts, owned resource IDs и stage/result; secret-bearing config/per-file/plaintext/manifest digests остаются private. Сейчас выполнен только source review/план: **0 builds, 0 app START, 0 migrations, 0 remote/model calls**. Dockerfile/guard/controller/данные не изменены.
