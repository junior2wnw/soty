# P4-D2-R1b — private Linux sink source receipt

**Текущий статус: R1b принят.** Root выполнил Windows **27/27 PASS** и Linux **41/41 PASS** с отдельным RO auditor; результаты и границы собраны в [root verification evidence](p4-restore-sink-root-result.md). Два независимых source/evidence аудита — agent_ecosystem и whole_product_critic — дали GO на этот synthetic R1b checkpoint. Ниже сохранена исходная история авторских SOURCE FREEZE / NOT RUN; она описывает действия автора до runtime-приёмки. Подробная атрибуция исполнения добавлена в конце. R1c handoff, реальный backup/full-image restore и production release этим не подтверждены.

## Исторический авторский source freeze

01.10.2026. **SOURCE FREEZE / NOT RUN.** Основание — [принятый sink plan](p4-restore-sink-plan.md), SHA `fbf32a3e1907535c31d50453dd2ce192f93b68fd00a7b1c72fe636efc8815bee`, и R1a `590e9f3a8d253c96ec78ce2ef15d8db4d24223ca`. Автор подготовил source и тесты, прочитал diff, сверил byte hashes; не запускал syntax, tests, CLI, hosts/SSH, images, models или настоящие keys/config/archive. Ни PASS, ни Linux/restore evidence этим документом не заявлены. Единственный runtime owner — root; ignored Linux harness и отдельную независимую fixture готовит publishing_architecture.

## Контракт и пределы

Private `extractOwnedBackup({input,target,expectedManifestSha256,sourceWitness,limits,signal?})` добавлен в `restore-backup.mjs`. Вызов на не-Linux отказывает `restore_platform_unavailable` до чтения свойств options/stream или filesystem I/O. R0 `verifyEncryptedBackup` и R1a `inspectRestorableBackup` не получили output/callback/stream options; `verify-backup.mjs` и оба прежних test files не менялись.

`input` — принадлежащий одному вызову, ещё не начатый binary Node Readable, `readableFlowing:null`, **effective emitClose:true**, без decoder/objectMode/предыдущего чтения, highWaterMark от1 до65536, без заранее накопленных >65536B. Controlled producer выдаёт chunks≤64KiB и obeys backpressure; библиотека читает максимум64KiB за шаг. Это не защита памяти от произвольного враждебного JS `_read`. Framing прежний: uint32BE metadata length, UTF-8 JSON, tar; нового crypto, envelope, tar parser, sender или CLI нет. Private input ABI pinned к Node24.15/24.21: admission читает `_readableState.emitClose`, поскольку public getter constructor option отсутствует; false/unknown → fixed `restore_archive_invalid` до stream ownership/start и filesystem I/O. Flag не изменяется; generic compatibility с иными внутренними Node layouts не обещается.

`target` ровно `{targetId,mountNamespace:{dev,ino},namespace:RootPin,dataRoot:RootPin,configRoot:RootPin}`; `RootPin={path,dev,ino,mountId}`. targetId32lowerhex, dev/ino native bigint (dev≥0,ino>0), mountId positive safe integer; paths canonical absolute POSIX≤4096UTF8B/component≤255B, не `/`. Data/config paths ровно namespace+`/data` и +`/config`; identities трёх roots различны. Pins получены trusted setup внутри exact helper mount namespace; metadata никогда не определяет destination или эти pins.

Три roots initially helper-owned0700; namespace содержит только пустые data/config. Ancestors проверяются lstat/no-follow directory open; root FD удерживаются, повторно проверяются dev/ino и `/proc/self/fdinfo` mountId, mount namespace — по `/proc/self/ns/mnt`. Enumeration начальных roots ограничена3/1 entries. Внутренние directories имеют bounded identity map; parent должен уже быть создан и оставаться helper-owned0700 до postorder. Node path API не объявлен защитой от privileged-root/concurrent external mount replacement: trusted sole-writer/private local filesystem — обязательный prerequisite.

Receiver limits ровно `{plaintextBytes,fileBytes,extractedBytes,entries,headers,pathBytes,pathDepth,externalFiles,externalBytes,wallMs,idleMs,freeSpaceReserveBytes}`, все positive safe integers, без defaults/archiveBytes. Metadata/extension≤4MiB, paths≤4096B/component≤255B, chunks≤64KiB. BigInt statfs gate суммирует data+config demand один раз при одинаковом st_dev и добавляет reserve; для разных FS проверяет отдельно. Это проверка declared payload bytes, не reservation или гарантия отсутствия ENOSPC/metadata overhead; reserve задаёт оператор.

Один монотонный deadline timer, один текущий input waiter и один close completion. Timeout delay clamp2147483647 с повторной проверкой actual deadline и re-arm; clamp не означает истечение большого допустимого окна. Idle продлевается только после реальных input/write/readback bytes; filesystem операции имеют cooperative checks. Таймер/abort разрушает owned input, не передаёт reason. Async fsync/close нельзя объявить завершённым по timer: hard watchdog/ресурсы/helper removal принадлежат будущему controller profile.

## Проверяемая последовательность source

1. Options/pins/limits snapshots → read-only Linux target admission → только затем первый input read. Общий parser проверяет metadata, exact manifest/pins, независимый sourceWitness, required presence и **все** config hashes до первой materializing write.
2. Единственный shared parser стал async. Verify/dry используют тот же grammar без sink; encrypted reader await-ит feed до wipe. Private extraction связывает parser только с фиксированным Linux sink; наружу plaintext callbacks/results не возвращаются.
3. Regular file создаётся `O_CREAT|O_EXCL|O_NOFOLLOW|O_RDWR`,0600, fstat regular/nlink1/dev/mount. Один текущий FD, awaited partial-write loop. Parser подтверждает input size/hash; далее fsync → bounded positional readback/hash/size/EOF на том же FD → chown/chmod/fsync → fstat identity/metadata → actual awaited close. Config files проходят тот же pipeline под logical IDs.
4. Directories создаются0700, затем после полного input EOF/tar finish переоткрываются deepest-first с повторной identity/no-follow проверкой; metadata применяется пока ancestors ещё helper-owned0700. Архивная root metadata применяется последней. Config root и enclosing namespace остаются700; directory fsync покрывает entries.
5. Parser buffers очищаются, все owned handles и input закрываются с ожиданием фактического завершения, затем последний deadline/abort check и receipt. Cleanup не проходит через предварительный guarded check, который мог бы пропустить close после timeout. Ошибка закрытия не становится success; quarantine остаётся, recursive cleanup/авторетрай отсутствуют.

Private receipt ровно `{extracted:true,targetId,manifestSha256,plaintextSha256,plaintextBytes,entries,fileBytes,readbackVerified:true}`. entries — materialized tar entries, включая root/directories, без external; fileBytes — data+present external; plaintext hash/bytes включают полное framing. Нет authenticated/restored/release-ready. Эти hashes относятся только к protected evidence; human projection — targetId/counts/fixed stage. Будущий sender/controller ещё должен связать authenticated archive pin, оба receipts и actual process completion.

Fixed code/message/stack, без cause/path/config/raw error: `restore_platform_unavailable`, `restore_target_invalid`, `restore_incomplete`, `restore_archive_invalid`, `restore_limit_exceeded`, `restore_timeout`, `restore_io_failed`, `restore_cleanup_pending`. Последний — неподтверждённое завершение owned I/O cleanup, а не удалённая quarantine. Отказ в capture до принятия валидного input не начинает этот stream.

## Prepared tests, не результаты

`restore-sink.test.mjs` имеет один top-level `private extraction obeys the current platform contract`. Windows исполняет только actual unsupported-before-I/O branch без чтения hostile options. Linux требует reviewed root helper CHOWN/FOWNER и последовательно исполняет все14 subtests без skip/name-filter:

1. `restores exact main sidecar config empty and nested bytes with a closed receipt`
2. `preserves foreign ownership and restrictive postorder metadata after same-FD readback`
3. `rejects target inode mount namespace aliases symlink ancestors and contamination before input`
4. `requires manifest witness and all config hashes before the first write`
5. `rejects traversal links unsupported types duplicate entries and changed payload`
6. `enforces plaintext file entry and real free-space bounds`
7. `handles partial writes with one operation in flight and verifies actual stored bytes`
8. `detects actual stored-byte corruption independently of the input payload digest`
9. `waits for a held real write before abort cleanup and never accepts a partial result`
10. `closes held input and every owned descriptor on abort without reading the reason`
11. `cooperative timeout still awaits actual descriptor closes with virtual monotonic time`
12. `does not return success before owned input close and normalizes a failed file close`
13. `rejects truncated disconnected and hostile inputs with fixed diagnostics`
14. `rejects emitClose false before adopting or reading the input`

Fixture использует настоящий SQLite main, явно synthetic WAL sidecar, independently assembled tar/framing/manifest/witness, real FileHandle operations. Byte comparisons только boolean/hash, не raw buffer diffs. Partial-write/corruption/held/close wrappers выполняют реальные underlying I/O; clock virtual только в timeout case, production DI не добавлен. Foreign UID10001/GID10002 и external10003/10004,0600; restrictive dirs0500. Assertions используют actual fstat до закрытия и доступный внешний lstat, не chmod/chown для readback. После assertions только test-owned `/tmp/soty-sink-library-*` получает no-follow top-down permission cleanup для удаления; это не production quarantine API. Отдельный retained fixture и RO auditor DAC_READ_SEARCH после writer exit остаются обязательным независимым gate publishing/root.

Низкое свободное место проверяется actual statfs и заведомо превышающим общую ёмкость reserve; grouped-same-FS arithmetic проверено source-only, не выдаётся за quota provisioning. Wrong mountId oracle отличается от contamination refusal; реальный nested remount не заявлен этим library test без соответствующего trusted setup. Прежние10 R0 и13 R1a top-level (включая3 nested timeout controls) остаются неизменными: ожидаемый full Linux inventory41 cases, Windows27; это ожидаемый набор, не PASS.

## Import closure и freeze

Receiver runtime closure: `deploy/connect/restore-backup.mjs` → `backup-format.mjs`, `restore-sink.mjs`; sink → format; остальные imports только Node built-ins. Для полного unchanged R0/R1a/sink gate нужны также `verify-backup.mjs`, `backup.mjs`, `deploy/connector/docker-api.mjs` и три `.test.mjs`. Нет npm dependencies/new helper processes в библиотеке. Старые tests по-прежнему запускают только synthetic verifier children/system tar.

Первый source freeze (история, **NOT RUN**, до узкого emitClose исправления ниже) имел13 Linux subtests/40 full Linux cases. Исходный receipt SHA был `24a2bcc13d451f5d8fa842dadc8f9614c09a0a53653c133cc786812b0b7b768f`:

| File | Initial SHA256 exact worktree bytes |
|---|---|
| `deploy/connect/backup-format.mjs` | `fe71d35b06a1157afebe5e78fe10b3caf4409307c782852d436469c1fae0ed8e` |
| `deploy/connect/restore-backup.mjs` | `81f14f4300fd2b30b9dfa4b8ba145347875d08278b4e4e141e33641713ff0ae7` |
| `deploy/connect/restore-sink.mjs` | `5bc3246b7ce7869085c604f6b6686668d20be69d46b90bb4ea24fb1ba1c2ddb3` |
| `deploy/connect/restore-sink.test.mjs` | `9edc4e5ff3d3a67a2815828a5db7b1cab90e0b93c45d2bd9e0dd20d185ff7ec5` |

Unchanged pins: `verify-backup.mjs` `a47766031b8c74cb41a485887d982df76881b72ef187c495418560fb0ef42cd3`; old R0 test `1cc80d87a8fd9937feca5dba5df3a827fa2c6191150271702c3915e10b3e84ee`; R1a test `3d0e6898979e274a7b8d77738e9af7e633918b2324164f893228dc4bcd53745c`. Этот receipt hash передаётся отдельно, без самоссылки.

Primary source boundaries: [Node24.21 FileHandle/fs/statfs](https://nodejs.org/download/release/v24.21.0/docs/api/fs.html), [Linux proc fdinfo mount ID](https://www.kernel.org/doc/html/latest/filesystems/proc.html#proc-pid-fdinfo-details-about-open-file-descriptors). Это основания выбора API, не доказательство фактического Linux запуска. Full-image C/R, producer completeness, SSH transfer, authenticated second pass, maintenance/journal integration, release и end-to-end restore остаются отдельными следующими gates.

## Refreeze: close-event input profile — source-only / NOT RUN

Независимый source review обнаружил causal blocker первого freeze: обычный `Readable({emitClose:false})` может завершить `_destroy` и установить closed, не выдав close event; прежний waiter ожидал его бесконечно. Это не hung syscall и не выполненный runtime RED. [Node24.21 readable state/constructor](https://raw.githubusercontent.com/nodejs/node/v24.21.0/lib/internal/streams/readable.js) и [destroy completion/event distinction](https://raw.githubusercontent.com/nodejs/node/v24.21.0/lib/internal/streams/destroy.js) прочитаны автором; root отдельно сверил тот же flag в [Node24.15](https://raw.githubusercontent.com/nodejs/node/v24.15.0/lib/internal/streams/readable.js). [Официальный stream contract](https://nodejs.org/download/release/v24.21.0/docs/api/stream.html#event-close_1) допускает suppression события.

Минимальная поправка — boolean admission effective emitClose:true, без polling, state mutation или undocumented destroy completion callback. Новый bounded Linux case использует обычный false stream с валидными frame/target: ждёт fixed refusal,0 `_read`, неизменные пустые roots, отсутствие adoption/destroy; только после отказа fixture owner вызывает обычный destroy и проверяет actual closed=true при0 close events. Никакого forged close события или `_destroy` replacement в этом case нет. Прежний held-input abort case дополнительно сопоставляет completed-close map со **всеми observed FileHandles**, каждый exact once и fd==-1, вместо прежнего недостаточного `closes>=3`. Sink/core, старые tests и остальные инварианты не менялись.

| File | Current SHA256 exact worktree bytes |
|---|---|
| `deploy/connect/backup-format.mjs` | `fe71d35b06a1157afebe5e78fe10b3caf4409307c782852d436469c1fae0ed8e` |
| `deploy/connect/restore-backup.mjs` | `d2f4a182a48dad950c79395418fd27ef40ddc9b1b885aeacee13d20928f49be3` |
| `deploy/connect/restore-sink.mjs` | `5bc3246b7ce7869085c604f6b6686668d20be69d46b90bb4ea24fb1ba1c2ddb3` |
| `deploy/connect/restore-sink.test.mjs` | `3dbdbe92ea261b9c385fd199e5578be60f28e5706980cf64f5a6bf42ef03709e` |

Текущий закрытый test inventory:10 R0 +13 R1a top-level +3 timeout subtests +1 sink dispatch +14 Linux subtests = **41 Linux**,27 Windows. Import closure не изменился. Всё остаётся **NOT RUN**; author не выполнял syntax/tests/runtime. Новый receipt SHA передаётся отдельно.

## Последующая root runtime-приёмка — исполнение не автора

Root исполнил frozen library/test files на Node24.21.0 Windows: **27/27 PASS, 0 fail/skip/cancel/todo, 4753.5826 ms**. [Windows log](../../output/implementation-20260930/p4-restore-sink-windows-first.log), SHA256 `bb4446d78c183c6be56767cb46ed9e730e8c16c5056ac64d2a896cf574ecf5b7`. Sink на Windows проверен только как unsupported-before-I/O; Linux subtests этим запуском не подтверждались.

Последующий единственный send5 root завершился Linux PASS: run `f41f1c60d038fc6203e854d2261c7d14`, Node24.15.0/GNU tar1.35, elapsed9861ms. [Linux receipt](../../output/implementation-20260930/p4-restore-sink-review5-linux-result.json), SHA256 `7c58549df996c70ce260d60d079c3a264ff2d0bfc9f178c4d3ba019fb08cec3d`. Typed exact inventory — **41/41**, то есть10 R0 +16 R1a с вложенными controls +15 R1b с dispatch; failed/skipped/cancelled/todo0, completed=true. Writer подтвердил synthetic GCM fixture, retained extraction, wrong-mount refusal и неизменность outside sentinel.

После фактического writer exit0 отдельный RO helper с DAC_READ_SEARCH независимо прочитал все retained bytes, включая настоящий fixture WAL, сверил точное дерево, foreign UID/GID,0600 files, restrictive directory modes и sentinel: metadataMatched/independentReadback/outsideSentinelUnchanged=true. Writer и auditor связаны одним заранее сохранённым независимым witness;9 entries/7 regular files. Это проверка заявленной foreign metadata projection synthetic fixture, не доказательство сохранения владельцев из реального production backup. Для readback восстановленные права не менялись.

Оба helper достигли terminal exit0/OOMfalse и были удалены; [SSH send receipt](../../output/implementation-20260930/p4-restore-sink-review5-send-result.json) фиксирует actualChildClose=true/code0/signal=null, без timeout/overflow. Один code PUT ACK, два START, девять recorded mutations; cleanupComplete/servingUnchanged=true, needsReconciliation=false. ServingUnchanged относится к проверенной exact serving identity/start/restart, не к произвольному состоянию приложения.

Автор agent_ecosystem прочитал только source/evidence: подтвердил11 source pins, четыре reviewed pins, manifest/spec/archive и bundle→send binding, literal41 IDs и совпадение witness. Whole_product_critic выполнил второй независимый source/evidence аудит; root принял оба GO. Это не повторные запуски: runtime принадлежит root, библиотеку/fixture/41 cases и ресурсные пределы ради PASS не ослабляли. Подробная хронология остаётся в [root-result](p4-restore-sink-root-result.md).

Прежние preparation RED и Linux review3/review4 остаются историческими отказами. Review4 подтверждает PID-limit enforcement в том helper, но не определяет точный syscall или причину review3. Принят только pinned synthetic Linux R1b quarantine path и независимый retained readback; cold all-store production completeness, authenticated R1c sender/transport, full-image C/R, rollout и end-to-end recovery остаются отдельными gates. Free-space gate не резервирует место; cooperative deadline не доказывает hard cancellation зависшего I/O. Автор не выполнял новых syntax/tests/runtime и не менял code/pins при этом обновлении документа.
