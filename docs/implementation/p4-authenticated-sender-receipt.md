# P4-D2-R1c — authenticated sender library source receipt

01.10.2026. **ACCEPTED — local Windows library52/52.** [Root execution result](p4-authenticated-sender-root-result.md) принят после двух независимых source/evidence audits. Runtime выполнял только root; ниже результат атрибутирован ему, а исходные авторские SOURCE FREEZE / NOT RUN записи сохранены как история. Приёмка ограничена synthetic local Windows library gate; Linux66, Windows ACL/share guard, SSH/Docker handoff, настоящий B и application images не приняты этим результатом.

## Закрытый private контракт

`sendAuthenticatedBackup({file,privateKeyPem,expectedSha256,expectedManifestSha256,sourceWitness,output,limits,signal?})` экспортируется из `restore-backup.mjs`. Options читаются внутри нормализующей границы; unknown keys запрещены, значения pins/witness/limits захватываются до первой асинхронной операции. Path absolute, well-formed UTF-8≤4096B; PEM≤64KiB. Pins64lowerhex, witness ровно `{generationId:32lowerhex,checkpointSha256:64lowerhex,inventorySha256:64lowerhex}`. Требуются все12 прежних dry limits: archiveBytes, plaintextBytes, fileBytes, extractedBytes, entries, headers, pathBytes, pathDepth, externalFiles, externalBytes, wallMs, idleMs; positive safe integers, без defaults. Прежние format caps не повышены.

Success ровно `{authenticated:true,archiveSha256,plaintextSha256,manifestSha256,plaintextBytes}`. Это protected control receipt sender, не plaintext result, receiver acceptance, restored или release-ready. Публичный интерфейс оператора не должен копировать private manifest/plaintext hashes, пути, metadata или ключи. Fixed code/message/stack, без cause/raw error: `restore_archive_invalid`, `restore_incomplete`, `restore_authentication_failed`, `restore_limit_exceeded`, `restore_timeout`, `restore_io_failed`, `restore_cleanup_pending`.

`output` — exclusively owned свежий binary Writable, не Duplex/stdout/stderr, empty/uncorked/not-ended/not-errored, HWM1..65536, native state constructed/autoDestroy/emitClose=true и pendingcb=0. Effective flags читаются по pinned Node24.21 private profile, не меняются. Capture refusal не принимает stream во владение. После capture sender ставит listeners/timer, принимает output и на ошибке разрушает его без raw reason. Условие «новый и единственный владелец» также обязан обеспечить caller: Writable flags не являются доказательством отсутствия любых прошлых действий или стороннего JS.

Trusted wrapper сохраняет native semantics write/end/destroy, а `_write` подтверждает фактическое потребление supplied callback. Синхронный throw после принятия borrowed buffer не доказывает callback/освобождение: sender остаётся pending до callback, если он вообще придёт. Произвольному hostile JS Writable не обещается termination. Transport adapter обязан связать `_final`/`_destroy` со своим реальным underlying channel; немедленный callback пустой обёртки не доказывает pipe closure. SSH child.close отдельно принадлежит будущему controller.

## Один reader, два прохода и lifecycle

В `backup-format.mjs` прежний decrypt/parser loop выделен в непубличный `encryptedPass`. Internal export `readEncryptedPass({handle,privateKeyPem,restore}, ownedOutput?)` используется только фиксированным sender; второй аргумент — его внутренний lifecycle object, не public callback option. `readEncryptedBackup` сохраняет RSA≥3072/key validation **до** open(file), один pass, awaited close, final check и прежний receipt/counts. Новые plaintext digest/count находятся только во внутреннем результате pass. R0 API/CLI, R1a dry и R1b receiver не получили output options; parser/cipher/envelope/format один.

Sender lstat-ит final entry и открывает файл read-only один раз (`O_NOFOLLOW` где поддерживается). Regular/nlink1/size ограничены; fstat dev/ino/size/mtimeNs/ctimeNs совпадают с captured snapshot. Перед первым pass, между passes и после второго выполняются same-FD stat и positional EOF read. Path не переоткрывается. Первый pass завершает GCM final, parser.finish, archive pin, manifest/config hashes и independent witness до первого write/end. Второй повторяет тот же parser/GCM/archive/plaintext digests/count; framing отправляется без перекодирования. Invalid first pass →0write/0end; поздний отказ может оставить plaintext в quarantine, но не даёт sender success/end.

Один chunk≤64KiB и один outstanding write: decrypt→await parser.feed→write→supplied callback плюс необходимый drain на успешном пути→wipe. Drain listener/operation установлены **до** write: pinned Node может выдать drain до callback. При failure native drain может отсутствовать; callback по-прежнему обязателен. Native close/error/abort **не** разрешают wipe, следующий read или settlement активного borrowed chunk. Held `_write` с обычным immediate native `_destroy` — отдельный causal test, не замаскированный held-destroy случай.

Normal completion: второй GCM/hash/parser/stat/EOF→actual awaited file close→final check→один output.end→finish **и** actual close→cleanup listeners/timer→final check→receipt. Обработчик finish устанавливает didFinish только при endIssued и native output.writableFinished===true; преждевременный/ручной finish вызывает fixed refusal, не подтверждает final completion. До end failure означает0end. После вызванного end EOF нельзя отменить: `_final`/close error либо late abort дают no success, destroy и actual close. Если end уже вызван, подтверждённый native finish отсутствует и native pendingcb ещё ненулевой, close не доказывает завершение `_final`: fixed `restore_cleanup_pending`, без обёртки чужого `_final`, polling или нового observer. Это принятое root уточнение плана, не новый API.

Один monotonic timer охватывает оба passes/output; wall не обновляется, idle обновляется реальным read либо завершённым write. Delay clamp2147483647 только re-arms после проверки времени; большой finite limit не превращается в1ms timeout. Cleanup не пропускает close из-за просроченного check/abort. Actual FileHandle close и stream close ожидаются; failure/unconfirmed close не успех. Удержанный syscall/custom `_destroy`/непришедший write callback требуют будущего parent hard watchdog; library timeout не hard cancellation/CPU/RAM isolation.

Caller prerequisite остаётся внешним: transaction-owned immutable ciphertext copy в private namespace, bounded copy+fsync, creation writer closed и trusted archive pin из approved durable backup evidence. На Windows protected file/directory DACL и operator `FileShare.Read` keeper до actual sender child completion доказываются отдельным gate; Node0600/stat/hash/одинFD этого не заменяют. Библиотека не принимает data origin по одному RSA public envelope/GCM и не заявляет защиту от privileged writer. Она не создаёт plaintext file, не передаёт ключи remote и не удаляет copy/quarantine.

## Инвентарь causal tests при source freeze

`restore-sender.test.mjs`: **18 top-level +7 nested =25 cases**; при source freeze вместе с unchanged parity ожидались Windows52 / Linux66 cases. Root выполнил Windows52, результат ниже; Linux66 остаётся невыполненным gate. Каждый top-level имеет15s test deadline; это test-oracle bound, не гарантия отмены произвольной library I/O. Fixture использует неизменённый `encryptBackup`, настоящий system tar/SQLite и независимо составленные inventory/witness/plaintext framing. Только synthetic keys/data. Buffer comparisons — boolean/hash, без raw diff. Mock wrappers вызывают настоящие FileHandle.read/close; clock virtual только в deadline controls; mocks восстанавливаются.

1. `sender emits exact framing through native HWM1 drain before callback after two same-FD authenticated passes`
2. `first-pass crypto and completeness failures write zero bytes and never end their adopted output`
3. `late real second-pass tag mutation reaches output but cannot authenticate or send EOF`
4. `real append truncation header and ciphertext changes between passes never produce sender success or EOF` — nested `append`, `truncate`, `header`, `ciphertext`.
5. `a newly valid GCM archive replacing the same-FD bytes between passes cannot replace the trusted archive pin`
6. `pathname replacement cannot reopen or switch the descriptor used by the second pass`
7. `held native write blocks next file read and keeps borrowed plaintext intact through abort and early native close`
8. `success awaits held final then actual held destroy close after the archive FD has closed`
9. `abort after end and early close reports cleanup pending for an uncompleted native final`
10. `manual finish during a held native final cannot turn actual close into authenticated success`
11. `write final and destroy errors are fixed refusals with correct pre-end and post-end distinctions`
12. `premature native output close refuses before end and still closes the real archive descriptor`
13. `failed actual file close cannot send end or become a successful receipt`
14. `sender captures strict pins and limits before its first asynchronous file operation`
15. `closed option and fresh Writable profiles reject before adoption without exposing hostile values`
16. `first-pass byte and entry limit plus one failures cannot write plaintext`
17. `wall spans both passes and idle safe points use real read and supplied write progress` — nested `control`, `wall`, `idle`.
18. `already aborted native signal destroys only adopted output and never reads the reason`

Late-tag case меняет реальные bytes **до** второго чтения tag после полной первой проверки, затем требует полного ожидаемого plaintext output и отказа GCM final с0end. Valid-GCM replacement отдельно доказывает binding к прежнему archive pin. Joint omission удаляет required DB одновременно из tar+manifest и обновляет producer pins, сохраняя независимый witness. Path replacement test допускает только исходные FD/bytes; filesystem-dependent ctime change может дополнительно вызвать fixed authentication refusal, не открытие нового pathname. Abort/early-close test требует pending sender и неизменный borrowed buffer после actual close; только supplied callback разрешает wipe/refusal, без ожидания подавленного drain. Listener cleanup и каждый observed FD exact-once/fd=-1 проверяются отдельно от safe error shape.

## Freeze и границы дальнейшей приёмки

| Owned file | Bytes | SHA256 exact worktree |
|---|---:|---|
| `deploy/connect/backup-format.mjs` | 28250 | `f4e57a4791bc184e4b06618cc15ee8b6f539dd23de28ef8adee87cca15aa41bb` |
| `deploy/connect/restore-backup.mjs` | 23569 | `171b2b9669dab3c89c6672e3f259f913f3a91a0f9d61c920f5e184ae9234f5a9` |
| `deploy/connect/restore-sender.test.mjs` | 30289 | `f6945200d0da1e98c6cec8f3c39e8849b1c9335723da99cb98dd9abddf43d252` |

Unchanged checked pins: sink `5bc3246b7ce7869085c604f6b6686668d20be69d46b90bb4ea24fb1ba1c2ddb3`; R0 verifier `a47766031b8c74cb41a485887d982df76881b72ef187c495418560fb0ef42cd3`; old R0 test `1cc80d87a8fd9937feca5dba5df3a827fa2c6191150271702c3915e10b3e84ee`; R1a test `3d0e6898979e274a7b8d77738e9af7e633918b2324164f893228dc4bcd53745c`; R1b test `3dbdbe92ea261b9c385fd199e5578be60f28e5706980cf64f5a6bf42ef03709e`. Producer/transport также сверены без изменений. Runtime import closure по-прежнему restore-backup→backup-format/restore-sink +Node built-ins; новых npm/process/helper dependency нет. Test дополнительно импортирует unchanged backup→connector/docker-api и verifier.

Primary source прочитан: [Node24.21 Writable](https://raw.githubusercontent.com/nodejs/node/v24.21.0/lib/internal/streams/writable.js) — native drain/callback и pending final; [Node24.21 destroy](https://raw.githubusercontent.com/nodejs/node/v24.21.0/lib/internal/streams/destroy.js) — actual close не заменяет активный write callback. Это основания source решения, не выполненный тест.

## Root execution и независимая приёмка

Root выполнил один serial gate на pinned Node24.21.0 с unchanged R0/R1a/Windows receiver parity и sender tests: **52/52 PASS, 0 fail/cancelled/skipped/todo, 6638.726ms**, process exit0;42 top-level+10 nested. Из них25 sender cases и27 прежних cases. Log `output/implementation-20260930/p4-authenticated-sender-windows-first.log`,12,415B, SHA256 `2c0f609c303c9bf88d9e99f0da8a06edf60b03e113b23f86c27fab86c79db74d`; команда и source binding приведены в [root-result](p4-authenticated-sender-root-result.md). Это root execution, не авторский повтор.

Whole-product critic и publishing_architecture независимо приняли source и затем exact first-run evidence/source binding; оба дали GO для этого local library checkpoint. Автор тесты не запускал; при doc-only обновлении снова сверены неизменные f4e57…/171b…/f694… pins. Все25 sender cases, включая manual-finish negative, вошли в root gate; source finding старой версии не превращён в runtime RED.

Принят только synthetic Windows library результат. Receiver на Windows доказывает unsupported-before-I/O; Linux66 и новая Linux приёмка shared reader не заявлены. Windows DACL/FileShare keeper/DPAPI, actual SSH/Docker transport/receiver binding, реальные archives/keys/config/B, full C/R image и release/recovery остаются отдельными gates. Исторический R1b Linux41 не переносится автоматически на новые source pins. Следующий operator slice требует отдельного зонального GO root; эта запись не начинает implementation. Receipt SHA передаётся отдельно.

## История первого source freeze и узкий refreeze

Исходная авторская запись: 01.10.2026. **SOURCE FREEZE / NOT RUN.** Основание — [принятый sender plan](p4-authenticated-sender-plan.md), SHA256 `d8c850fe86368152338a167fc4fe7e696e967c22d01ccb5bc6ebfc2742484d3f`, и принятый R1b `c030addbc2cfa378bc1de2e41e3cdcf8971bdb48`. Автор изменил только два owned library files, новый test file и этот receipt; прочитал source/diff, первичный Node source и сверил hashes. Syntax, imports, tests, tar, CLI, SSH, hosts, images, реальные archives/keys/config и model calls автор не запускал. Root — единственный runtime owner; PASS здесь не заявлен.

Первый receipt SHA256 `27e3c7ce674a07decc09dbf02da273670ed0167dc27f361e0be2577609a16ef9` содержал24 prepared cases и был **SOURCE FREEZE / NOT RUN**. Его source pins: backup-format `f4e57a4791bc184e4b06618cc15ee8b6f539dd23de28ef8adee87cca15aa41bb` (не менялся), restore-backup `785ad775eeb8e07a9264bdf55752c802d2917b39786469aaf9a5b807a8731989`, sender test `44431b80b116236c957ea3cd41cd62ba5e8177973b2ec13f5de0a7b2e15fa1e5`. Independent publisher прочитал этот freeze; затем root+critic нашли source asymmetry finish/close. Это source finding, **не runtime RED**.

По отдельному root GO изменён только finish handler, добавлен один causal case10 и обновлён этот receipt. Case удерживает настоящий `_final`, вручную выдаёт finish, вызывает native destroy и ждёт actual close; writableFinished остаётсяfalse/pendingcb>0, ожидается fixed `restore_cleanup_pending`, а actual final callback освобождается в finally. Close не подделывается. Это negative malformed-wrapper oracle, не обещание hard termination произвольного JS. Прежние24 cases, shared reader, sink и старые tests не изменены. Повторный freeze также **SOURCE ONLY / NOT RUN**; первый serial runtime gate выполняет root после affected-only source review.
