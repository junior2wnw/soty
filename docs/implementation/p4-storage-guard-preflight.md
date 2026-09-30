# P4-B1 — storage compatibility до новых миграций

30.09.2026. Read-only preflight после принятого P4-A `6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e`. Основание: [P4 preflight, §7](p4-external-agent-preflight.md#7-форматы-backup-и-rollback--до-миграции-данных). Root принял **strict manifest/probe/start v3** и отдельный первый checkpoint с `notes:[1]`, `capabilities:[1]`. DDL2, reader2, production migration и новый fallback этим документом не объявляются готовыми.

После отдельной выдачи ownership реализован локальный bridge; его авторские результаты и source freeze записаны отдельно в [implementation receipt](p4-storage-guard-bridge.md). Ниже сохранён preflight и его ограничения, а не повторная декларация production готовности.

В этой работе прочитаны текущие schemas, trusted guard/probe, rollout/host-controller/rebase/backup пути и прежние reader receipts; проверены первичные SQLite/Node/Docker источники. Исторические v1 schemas из точных Git blobs исполнены только в двух новых `:memory:` SQLite для инвентаризации объектов. Файлы данных, source, конфигурация, Docker/SSH не изменялись; единственный новый файл — этот документ. Полные suites не запускались.

## 1. Реальные stores и предел общего guard

Production composition задаёт `DATA_DIR=/data`; пути ниже фиксирует `server/http-app.js`, а не HTTP caller.

| Store | Путь от `/data` | Текущий формат и зависимость |
| --- | --- | --- |
| Rooms | `rooms-v2.sqlite`, ранее room JSON в корне |1/2; существующий guard сохраняется |
| Apps | `apps/registry.sqlite` |1–6; прежние independent projections/точные guards и metadata сохраняются |
| Notes | `notes/notes.sqlite` | `user_version=1`, `notes_meta.lineage=soty.notes.sqlite.v1`, `project_id=soty`; SQLite **FTS5**, стандартный tokenizer `unicode61 remove_diacritics 2`, prefix `2 3` |
| Capabilities | `capabilities/capabilities.sqlite` | `user_version=1`, `cap_metadata.lineage=soty.capabilities.sqlite.v1`; STRICT tables, FK/UNIQUE/CHECK, стандартный SQLite |
| Connect | `connect/accounts.sqlite` | Schema3, `connect.sqlite.local.v1`, reader epoch1; собственные metadata/layout/min_reader и migration checks |
| World | `world/world.sqlite` | Schema3, `soty.world.sqlite.v1`, project/schema metadata и service checks |

Connect/World уже существуют и участвуют в identity/ACL. **Новый v3 label не объявляет их форматы проверенными host probe.** Existing Connect controller/release policy и проверки startup остаются отдельными; World также не получает независимый host reader от этого изменения. Они входят в согласованный cold backup/restore inventory. Аналогично Connector JSON/SQLite, maintenance markers и будущая AS DB не становятся покрытыми одним добавлением Notes/Capabilities. Если P4 меняет их DDL, reader inventory расширяется отдельным утверждённым этапом.

Read-only probe — проверка распознаваемого совместимого формата, не универсальный storage/integrity certificate и не доказательство корректности Invocation→Notes эффекта.

## 2. Первый целый checkpoint: manifest/probe/start v3, только v1

Image label остаётся `io.soty.storage.readers`, меняются version и точные ключи:

```json
{"version":3,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6],"notes":[1],"capabilities":[1]}}
```

Декларации каждого store независимы, непусты, содержат только известные целые версии без дублей. Нет общего `maxVersion`, wildcard, implicit reader или принятия неизвестного поля. В bridge Notes2/Capabilities2 остаются неизвестными, даже если кто-то предъявит `[1,2]` в label.

Успешный probe:

```json
{"ok":true,"schema":"soty.storage-format.v3","rooms":"empty","apps":6,"notes":1,"capabilities":1}
```

`rooms` по-прежнему `empty|1|2`; `apps` — `empty|1|2|3|4|5|6`; новые поля пока `empty|1`. `soty.storage-start.v3` содержит прежние `containerId,image,mountSha256` и **все четыре** store fields. Старые/расширенные format/start DTO не принимаются, неизвестные значения не превращаются в empty.

Root выбрал strict v3-only: manifest v1/v2 отвергается даже при обоих пустых новых stores. Это безопаснее отдельного режима «старый reader разрешён пока пусто»: старый image способен сам создать неизвестное ему для host guard хранилище при startup. Наличие Notes/Capabilities1 уже не даёт старому rooms/apps-only label пройти. Исторические receipts не переписываются и не дополняются выдуманными readers.

После отдельного DDL2 freeze, настоящих migrations/old-code tests и проверки candidate/fallback список Notes и/или Capabilities расширяется до `[1,2]`. Версии не связываются условием `notes===capabilities`: после прерванного обновления возможен смешанный supported format, который native reconciliation должен обработать отдельно. Успешное чтение четырёх форматов не доказывает их межфайловую бизнес-согласованность.

## 3. Узкая topology и trust boundary сохраняются

`storage-guard.mjs` уже требует exact immutable image/ID, ровно один `DATA_DIR=/data`, один RW runtime mount `/data` с Type=volume, без nested mounts/subpath. Volume обязан быть Docker `local`/local scope, без driver options, с точным совпадением имени/mountpoint; profile, runtime state и writer inventory проверяются до и после probe. Новые stores используют именно эту папку; перенаправление Notes/Capabilities через symlink или отдельный nested mount не поддерживается.

Probe исполняет pinned host-owned source через встроенные `node:fs`, `node:path`, `node:sqlite`; никаких imports candidate modules/migrators. Actual image label читается через inspect image, не из copied container label. Historical backend-overlay Dockerfile остаётся закрыт, relabel старого binary запрещён.

Сохраняются mount RO+NoCopy, no network/ports, read-only rootfs, no-new-privileges, bounded memory/CPU/PIDs, exact-ID journal и отсутствие повторного CREATE/START после ambiguous response. NoCopy нужен, чтобы пустой volume не был заполнен содержимым image при mount. [Docker volumes](https://docs.docker.com/engine/storage/volumes/#options-for---mount)

`Env:['SOTY_STORAGE_PROBE=1']` доказывает отсутствие **скопированного application Env**, но Docker наследует defaults image. Перед canary/runtime use отдельно проверяются exact helper image: Node/SQLite/FTS5, `NODE_OPTIONS` и другие hooks, Entrypoint/Cmd, Healthcheck, Volumes/realized mounts. Неожиданные health hooks или auto mounts не проходят review. Existing source не является sandbox произвольных образов.

Lexical writer scan не исключает произвольный host-root/remote writer, bind alias на backing directory или filesystem alias. Поддерживаемый профиль — управляемый single writer с операторским inventory. Это ограничение не ослабляется и не переименовывается в универсальную изоляцию.

## 4. Узкий independent recognizer новых stores

Для каждой новой папки: отсутствует или действительно пуста → `empty`. Отсутствующий main рядом с любым файлом, orphan journal, symlink/junction/non-directory, zero/short/nonregular main или sidecar → отказ. При main присутствуют только ожидаемые main/`-wal`/`-shm`/`-journal`; неизвестные файлы нового store требуют разбора, не предположения о свежем состоянии. Общий `/data` не ограничивается этими файлами: там законно существуют другие stores.

Открытие — `DatabaseSync(filename,{readOnly:true})`, `query_only=ON`, finite busy timeout и read transaction, как в нынешнем probe. Никаких journal_mode изменений, checkpoint, VACUUM, repair, rebuild FTS или `immutable=1`. Node readOnly не создаёт отсутствующий main; extension loading по умолчанию выключен. [Node SQLite](https://nodejs.org/download/release/v24.13.1/docs/api/sqlite.html#new-databasesyncpath-options) `immutable` отключает locking/change detection и не подходит живому/восстанавливаемому WAL. [SQLite URI](https://www.sqlite.org/uri.html#uriimmutable)

Normal SQLite view должен учитывать **committed WAL**. Read-only WAL на read-only media требует доступных sidecars; недоступный/невосстановимый WAL — отказ, не повторное открытие main как immutable. WAL является частью состояния БД; его нельзя выбросить при копировании. [SQLite WAL, §4–5](https://www.sqlite.org/wal.html#read_only_databases)

Recognize marker + SQLite `user_version` + известные SQL objects/projections. В Notes требуется ровно один lineage и `project_id=soty`; Capabilities — ровно один lineage. `user_version=0` с SQLite файлом не означает fresh empty и не наследует Apps1 exception. Сам SQLite не придаёт user_version семантику приложения. [SQLite PRAGMA user_version](https://www.sqlite.org/pragma.html#pragma_user_version)

Notes1 ожидает четыре доменные таблицы, FTS5 virtual table и пять shadow tables, два explicit indexes, без views/triggers:

```text
notes_meta(key,value)
note_accounts(account_id,bytes,identities,active,archived,trashed)
notes(rowid,account_id,id,title,body,items,preview,color,pinned,state,revision,bytes,created_at,updated_at)
note_receipts(account_id,note_id,mutation_id,digest,result,revision)
notes_fts(scope,title,body,items) — fts5, unicode61 remove_diacritics 2, prefix='2 3'
notes_fts_data(id,block)
notes_fts_idx(segid,term,pgno)
notes_fts_content(id,c0,c1,c2,c3)
notes_fts_docsize(id,sz)
notes_fts_config(k,v)
indexes: notes_owner_order; note_receipts_trim
```

FTS5 shadow tables — законная часть этого конкретного формата, а не неизвестные пользовательские таблицы. Проверяются exact FTS virtual definition и известные projections; другой virtual module/options/shadow set запрещён. В `table_xinfo(notes_fts)` есть hidden `notes_fts` и `rank`, их не путать с extra application columns. В read-only probe не запускаются FTS write commands. [SQLite FTS5 structures](https://www.sqlite.org/fts5.html#fts5_data_structures)

Capabilities1 содержит12 таблиц, восемь explicit indexes, без views/triggers. Проекции фиксируются независимо от application imports:

```text
cap_metadata(key,value)
cap_contracts(capability_id,version,digest)
cap_clients(id,account_id,label,state,policy_epoch,created_at,revoked_at)
cap_principals(id,account_id,client_id,kind,label,state,creator_device_id,created_at,revoked_at)
cap_grants(id,account_id,client_id,principal_id,parent_id,root_id,creator_device_id,capabilities_json,resources_json,effects_json,recipients_json,allow_delegation,max_depth,depth,not_before,expires_at,policy_epoch,created_at,revoked_at)
cap_credentials(id,digest,account_id,client_id,principal_id,grant_id,audience,expires_at,created_at,revoked_at)
cap_audit(id,account_id,kind,object_type,object_id,actor_type,actor_id,created_at)
cap_budgets(root_grant_id,unit,limit_amount,reserved_amount,spent_amount)
cap_budget_reservations(id,invocation_id,attempt_id,root_grant_id,unit,amount,actual_amount,disposition,request_digest,created_at,updated_at)
cap_invocations(id,account_id,client_id,principal_id,grant_id,root_grant_id,policy_epoch,capability_id,capability_version,capability_digest,request_key,request_digest,internal_request_id,input_json,target_json,authorization_json,status,effect_state,cancel_requested,effects_json,reservation_id,job_id,created_at,updated_at,completed_at)
cap_dispatch_intents(invocation_id,internal_request_id,state,created_at,updated_at)
cap_receipts(invocation_id,value_json,digest,created_at)
indexes: cap_clients_account; cap_principals_account; cap_grants_account; cap_grants_root;
         cap_credentials_grant; cap_audit_account; cap_invocations_history; cap_dispatch_pending
```

Probe проверяет readable fixed projections/known layout и отказывает при unknown tables/views/triggers, отсутствующем обязательном объекте/поле/marker или SQLite read error. После DDL2 отдельно замораживаются literal новые objects/guards и SQL bodies. Нельзя разрешить все triggers или брать whitelist из candidate code. Существующие Rooms/Apps recognizers не заменяются новым generic ORM/helper.

Это ограниченный format check: `LIMIT 0`/metadata не проверяют каждый B-tree page, FTS term, FK/CHECK, usage counter или согласованность receipts. Полное corruption/integrity/FTS и cross-store effect validation относятся к service startup и isolated restore. Если malformed WAL может быть проигнорирован SQLite как незавершённый хвост, нельзя утверждать, что все повреждения WAL обнаружены; отрицательные fixtures обязаны отделять valid committed tail от повреждённого/незавершённого. Host не делает repair и не выводит тела Notes, credentials, invocation input/IDs или полный schema dump в runtime receipt.

## 5. START, restart policy и холодное recovery

В `deploy/connector/rollout.mjs` guard вызывается перед application START и перед включением автоматического restart. При rollback candidate restart сначала выключается; old image проверяется **до RW rollback helper**, восстановления старой restart policy и START. Ambiguous START/restart/helper остаётся `recovery_required`, не разрешает второй writer.

В `deploy/connect/host-controller.mjs` `action()` сохраняет guarded operation до Docker call; `reconcileOperation()` не повторяет mutation, проверяет persisted receipt и выполняет **fresh probe** после exact state reconciliation. `restore()` проверяет old reader до offline helper; `settle()` снова проверяет actual format даже для старого restored journal. `checkSource()` защищает recovery тем же pinned host checkout, что и forward path.

После перехода на v3 старый pending format/start receipt и helper v2 не становятся разрешением START: fail-closed с retained journal и операторским reconciliation. Перед штатной сменой host guard должно быть `transaction=null`/нет pending operations/helpers; существующий rebase path уже требует quiescent state. Нельзя редактировать исторический receipt для обхода barrier. Существующий уже running image не останавливается молча из-за нового label contract; запрещены новые неподтверждённые START/enable-restart.

**Первый переход v2→v3 имеет отдельный bootstrap gate, даже без миграции данных.** Независимый review обнаружил: strict v3 guard запретит возврат SAME old container с image label v2 и при Notes1/Capabilities1. Проверка только candidate перед STOP оставила бы первый bridge без допустимого автоматического rollback. Root принял composition delta: в обоих rollout путях заранее проверить reader declaration фактического old image **до STOP и других serving mutations**. Если actual old image не проходит новый контракт, штатный rollout должен отказать до остановки; это нельзя выдавать за успешную миграцию или обходить relabel старого image/broad legacy exception.

Production bootstrap требует отдельно рассмотренного recovery route либо реально совместимого baseline, с точными image/IDs и сохранностью данных. Подготовленный файл fallback или новый label сам по себе этого не доказывает. Эта граница включается в release plan и root-owned composition tests; локальную разработку strict v3 bridge она не блокирует. Пока bootstrap не доказан, automatic rollback первого bridge не заявляется.

**Оба текущих rollout пути возвращают SAME old container/image.** Отдельно собранный compatible fallback сам не становится этим old ID. Следующая, независимая граница возникает у image, который на первом startup автоматически пишет Notes2/Capabilities2 поверх bridge `[1]`: automatic rollback на bridge тогда правильно запрещён уже из-за данных. Для следующего этапа нужен один из явно проверенных путей:

1. Reader-before-writer baseline реально читает1/2, но ещё не мигрирует persistent stores автоматически; после его deployment он является настоящим compatible old image для следующего writer.
2. Отдельно утверждённый cold recovery route на exact compatible full image, включая mapping/journal/admission. До доказательства этого пути отказ старого reader означает controlled fail-stop, а не «rollback готов».

Выключение endpoint/native create не превращает старый reader1 в reader2. Это решение future2 composition, не разрешение менять migrators/host в bridge checkpoint. Обе схемы2 и immutable receipt/reconciliation semantics должны быть действительно поддержаны fallback binary; одних labels недостаточно.

## 6. Исторические fixtures и runtime dependencies

Общий historical pin: **`6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e`**. Сохранить literal v1 DDL/seed отдельно от текущего migrator; дополнительно точные self-contained historical schema modules + provenance. Они не имеют local imports; для schema refusal не требуется подменять зависимости на current code.

| Historical source | SHA256 точных Git LF bytes | Последнее изменение source до pin |
| --- | --- | --- |
| `modules/notes/server/schema.mjs` | `da75ffc702fb0ee4db9878780d374f53bca0d68b46e91eafb56038b45d1e0dfa` | `85f36a15d744f0e41b7370548a638a2d57b531a1` |
| `modules/capabilities/server/schema.mjs` | `959fdb4938d6b0ebc9359817b001f0eed08be70e437083648e9ac259c56229ad` | `36116cbe8dc7378db07f8aa4e28210d9e2ca0d08` |

Два выполненных in-memory baseline наблюдения подтвердили Notes1:10 tables+2 indexes, Capabilities1:12 tables+8 indexes и оба `user_version=1`. Это не file-WAL/migration2/old-image доказательство. Literal fixtures затем сравниваются с этими exact migrators по всем сохранённым SQL objects и содержат согласованные synthetic rows: Notes active/archived/trashed/deleted/FTS/receipts; Cap clients/principals/grant roots+children/credentials-digests/budget reservations/invocations/dispatch/receipts. Нельзя получить «historical1» текущей migration2 с пониженным marker.

Старые Notes/Capabilities schemas вызывают journal pragmas до refusal; Notes начинает `BEGIN IMMEDIATE` до version check. Поэтому error `notes_schema_unsupported`/`schema_version_unsupported` **сам не доказывает** нулевые filesystem effects. Old-code tests различают concurrent open writer и cold crashed-WAL reopen, контролируют main/WAL bytes и побочные файлы. Последнее RW connection close может checkpoint/cleanup; обнаруженное отличие сохраняется как evidence, не маскируется под успех. Основной барьер — host отказ **до запуска** старого образа, с RO trusted probe. [SQLite WAL lifecycle](https://www.sqlite.org/wal.html#the_wal_file)

Default local runtime при этом исследовании — Node **24.13.1**, SQLite **3.51.2**. Официальная SQLite документация сообщает WAL-reset race, исправленный в3.51.3+ и backports3.44.6/3.50.7. Это не обнаруженная порча данных, но exact application/helper/test runtime обязан пройти отдельный version/dependency gate. [SQLite WAL-reset bug](https://www.sqlite.org/wal.html#the_wal_reset_bug)

Уточнение root от30.09.2026: изолированный `var/toolchains/node-v24.21.0-win-x64/node.exe` **реально запущен**, сообщает Node **24.21.0 / SQLite3.53.4**; SHA и Authenticode проверены root. Bridge tests должны явно вызывать этот executable, не случайный старый `node` из PATH. Эта атрибуция относится к проверке root, а не к повторному запуску автора этого документа; default/runtime settings здесь не менялись.

Корневой Dockerfile закрепляет `node:24-trixie-slim@sha256:4f2b45e32dc7d2caf66b6dbd59fac50e32f8077769efe0ef4d4c3f114672537d`. Read-only registry inspection root разрешил его linux/amd64 manifest **`c70f2d9b9dcd1f95d51b1f2d9c000637f203dbe2cbeaf06680780584518ca5c3`**, config `NODE_VERSION=24.15.0`, created `2026-05-08T19:46:18.050622766Z`. Это **metadata, не Linux execution proof**: фактические `process.versions.sqlite`, FTS5 и RO WAL/mount поведение exact application/helper image ещё требуют изолированного запуска. Root владеет Dockerfile; изменение его pin сейчас не предполагается. Автор этого preflight не делал upgrade/pull/build/SSH.

## 7. Bounded local acceptance после выдачи владения

Последовательно, `--test-concurrency=1` (или согласованный2); только свои synthetic directories. Не повторять прежний Windows parallel ENOMEM эксперимент.

- **Manifest/receipts:** exact v3 bridge shape; каждая независимая версия/empty; v1/v2 labels и receipts, wrong/extra/missing keys, copied container label, duplicate/future readers rejected. Rooms1/2 и Apps1–6 остаются со всеми прежними checks.
- **Historical schemas:** actual v1 vs literal fixture SQL; normal cold reopen; populated Notes FTS и Cap rows сохраняются после RO probe. Bridge отказывает Notes2 и Capabilities2 по отдельности; реальный1→2 proof появится только после DDL2.
- **Filesystem:** absent/empty, zero/truncated/random main, orphan WAL/SHM/journal/backup, wrong marker/version/project, symlink directory/main/sidecars и nonregular entries. На Windows недоступный file-symlink case честно skip; Linux закроет его. Никакого fresh initialization при damaged store.
- **WAL:** genuine committed marker/schema read через normal RO; несовпадение raw main header и SQLite view. Future marker в WAL должен запретить START, даже если main ещё1. Смешанный Notes/Cap status не скрывается предыдущим успехом другого store. Main/WAL/directory checks до/после; no immutable fallback. Corrupt/unsupported/locked cases fail-closed.
- **Recovery:** обе orchestration зоны — first bridge с actual old label v2 отказывает **до STOP/serving mutation**, даже при Notes1/Capabilities1; обычный first candidate START после совместимого baseline, old rollback до RW helper, включение restart policy, lost START и fresh recovery check, stale v2 receipts/helpers, формат изменился после прежнего receipt, unknown/incompatible old image. Ноль repeated CREATE/START по ambiguity; failure сохранить journal. Не заменять реальные SQLite в важных recovery cases fake enum.
- **Regression:** существующий serial deploy suite Rooms/Apps/topology/source-pin остаётся; текущие DTO fixtures обновляются только до envelope v3 с Notes/Capabilities empty. Historical Apps1–6 DDL и отрицательные assertions не переписываются. Переименование номера DTO не заменяет новые Notes/Cap cases.

## 8. Linux/cold backup — отдельные внешние gates

Предыдущий [Rooms Linux canary](storage-linux-canary-result-20260930.md) доказал узкий старый rooms helper на synthetic volume; он **не** доказал Notes FTS, Capabilities, Apps6, новое probe source, compatible image или restore. [Apps6 reader receipt](p3-apps-v6-reader.md) также оставляет эти внешние проверки открытыми.

Следующий Linux canary — отдельный reviewed script/hash и отдельное разрешение root. Один task-owned synthetic standard local volume, конечное число containers/время, network none/no ports, без serving mounts/env/secrets, exact already-reviewed patched image, прежние bounded resources. Writer подтверждает только safe fixed readiness/version/size, host после exact identity отправляет один journaled SIGKILL; probe получает normal RO mount. Проверить Notes FTS/Cap1, committed WAL, unknown/corrupt отказ, root/userns0600 semantics, source/mount identity, lost-response reconciliation, cleanup exact IDs и неизменный serving baseline. Это план, сейчас не запускался.

Для future2 повторить с настоящими frozen migrations/old-code и exact application+fallback images; bridge canary не переименовывается в этот proof. Compatible image должен читать Notes data/search и retained Cap receipts/budgets, а create/dispatch/OAuth оставаться выключенными в restore environment.

Существующий encrypted backup архивирует весь `/data` остановленного container и проверяет отсутствие writers; это полезная основа, но не выполненный P4 restore. Перед первой реальной миграцией: quiescent stopped writer, согласованный encrypted cold snapshot Connect+World+Notes+Capabilities+Apps+Rooms+connector state/необходимых secret metadata, затем isolated restore без сети/dispatch и проверка связей. Независимые live backups отдельных файлов не доказывают одну межфайловую точку. SQLite online backup API копирует одну database; бизнес-согласованность нескольких файлов требует нашего общего stop/fence. [SQLite backup API](https://www.sqlite.org/backup.html)

Нельзя откатывать Notes отдельно от новых Invocation/receipts или восстанавливать snapshot поверх работающего volume. Endpoint disable/compatible code rollback сохраняет последние данные и reconciliation; restore — отдельная операция с отдельным результатом, не скрытая часть обычного deploy rollback.

## 9. Предлагаемое владение и следующий шаг

До implementation root подтверждает точные файлы; сейчас они не изменены.

| Владелец | Узкий следующий scope |
| --- | --- |
| Storage author | Только точный перечень ниже: два production source, свои storage tests/fixtures и reader docs |
| Root | `Dockerfile`; `deploy/connector/rollout.mjs`, `rollout.test.mjs`; `deploy/connect/host-controller.mjs`, `host-controller.test.mjs`, `rebase-host.mjs`, `rebase-host.test.mjs`; `deploy/connect/README.md`, `CONTROLLER.md` при необходимости. Composition/recovery implementation остаётся root; не каждый перечисленный файл обязан измениться. Также host/source pin, future compatible baseline/cold recovery decision, Linux/backup/restore gates |
| Domain author / independent reviewer | Пока только Notes/Capabilities2 contract. После DDL freeze — schema/native implementation по отдельному разрешению и независимые historical/old-code/read-only WAL/rollback acceptance; не менять storage author's fixtures молча |

Точный запрашиваемый scope storage author после отдельной выдачи implementation:

```text
EXISTING production:
  deploy/connector/storage-probe.mjs
  deploy/connector/storage-guard.mjs
EXISTING author/storage tests:
  deploy/connector/storage-guard.test.mjs
  deploy/connector/storage-apps.test.mjs
  deploy/connector/storage-apps-v5.test.mjs
  deploy/connector/storage-apps-v6.test.mjs
NEW tests/fixtures:
  deploy/connector/storage-notes-capabilities.test.mjs
  deploy/connector/notes-v1.fixture.mjs
  deploy/connector/capabilities-v1.fixture.mjs
  deploy/connector/fixtures/notes-v1/schema.mjs
  deploy/connector/fixtures/notes-v1/provenance.json
  deploy/connector/fixtures/capabilities-v1/schema.mjs
  deploy/connector/fixtures/capabilities-v1/provenance.json
DOCS:
  deploy/connector/README.md
  docs/implementation/p4-storage-guard-preflight.md
  docs/implementation/p4-storage-guard-bridge.md (new)
```

В existing Apps tests меняется только актуальный format/start envelope и связанная reader declaration; literal исторические Apps schemas/markers и смысл отрицательных проверок сохраняются. `deploy/connector/storage-guard.acceptance.test.mjs` **не** входит в авторский scope: необходимую fixture v3 адаптацию и независимые assertions получает root/critic отдельно. Новые независимые acceptance files reviewer также не принадлежат автору. Никаких `modules/*` production schemas, server composition, Dockerfile, shell UI или runtime/CLI edits этим перечнем не запрашивается.

Сначала B1 bridge **Notes1/Capabilities1 only** + полный local regression/freeze/review. Затем отдельный DDL2/reader2/native compatibility checkpoint. Наличие принятого local bridge не разрешает production migration: patched exact runtime, Linux canary, доказанный actual fallback route и cold restore остаются условиями допуска.
