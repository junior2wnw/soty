# P4-D2-R1a — shared reader and strict dry admission

Дата: 01.10.2026. **Авторский source-only checkpoint. Код и 12 новых cases подготовлены, но syntax checks, tests, CLI, hosts, SSH и models автором не запускались.** Real keys/config/archive не читались. Runtime gate и независимый evidence audit остаются у root/reviewers; этот документ не заявляет PASS, extraction, restore, full-image или production readiness.

## Граница среза

Root уточнил R1a после первоначального GO: только общий reader и authenticated strict dry admission. `sendAuthenticatedBackup`, второй transfer pass, Writable lifecycle, receiver/Linux sink, output paths, producer manifest integration и operator/SSH/HostController здесь не реализованы. Планы следующих срезов не менялись. Отдельный decrypt/parser/ledger не появился.

Изменены ровно `deploy/connect/backup-format.mjs` (новый internal core), `verify-backup.mjs` (тонкий прежний wrapper), `restore-backup.mjs` (новый private dry port), `restore-backup.test.mjs` (новый fixture gate) и данный receipt. `verify-backup.test.mjs` сохранён без изменений. `backup.mjs`, storage/DDL, keys, controller, deploy manifest и PROGRESS не менялись.

## Точный API

```js
await inspectRestorableBackup({
  file, privateKeyPem, expectedSha256, expectedManifestSha256,
  sourceWitness: { generationId, checkpointSha256, inventorySha256 },
  limits: { archiveBytes, plaintextBytes, fileBytes, extractedBytes,
    entries, headers, pathBytes, pathDepth, externalFiles, externalBytes,
    wallMs, idleMs },
  signal, // optional native AbortSignal; reason is never inspected
});
```

Это private control-plane API из `restore-backup.mjs`, не HTTP/CLI/пользовательский endpoint. Options, witness и limits — закрытые объекты без неизвестных полей; значения захватываются до первого await. Destructuring/getters и cleanup находятся внутри нормализующего error boundary. `output`, callbacks и stream fields отвергаются; plaintext/metadata/file names/manifest или inventory hashes наружу не возвращаются.

Receipt — frozen object ровно с `ok`, `authenticated`, `offline`, `archiveEntries`, `roomFiles`, `sqliteFiles`, `emptySqliteFiles`, `externalFiles`, `verifiedFileBytes`, `sha256`, `strictProfile:'soty.restore-manifest.v1'`, `inventoryMatched:true`. `sha256` — hash всего ciphertext archive; `verifiedFileBytes` — сумма проверенных regular/config payload bytes, **не число записанных/extracted bytes**. Ни `restored`, ни `releaseReady` не выдаются.

R0 `verifyEncryptedBackup({file,privateKeyPem})` сохраняет прежние восемь safe fields, `backup_verification_failed` message/code/fixed stack без cause, import без чтения stdin/вывода, CLI stdin ≤64KiB/BOM и прежний fixed stdout/stderr protocol. Legacy parser остаётся совместим с прежними link/type7/PAX/name правилами; новые ограничения применяются только strict mode.

## Manifest v1 и независимый witness

В прежней authenticated encrypted metadata добавлены `restoreManifest` и `restoreFiles`; новый encryption envelope не вводится. В strict profile старый `secrets` допускается только отсутствующим/пустым: прежний единственный token bind не подменяет полный external inventory. R0 старые archives по-прежнему читает. **Действующий producer ещё не создаёт этот manifest**, поэтому этот срез сам по себе не делает существующий backup пригодным для restore admission.

Manifest имеет ровно `{version:1,generationId,checkpointSha256,inventory:{files,stores,external}}`. `generationId` — 32 lowercase hex, все digests — 64 lowercase hex. Поля fixed projection перечислены ниже в порядке JSON-сериализации:

| Массив | Точная запись |
| --- | --- |
| `files` | `{path,type,size,sha256,uid,gid,mode}`; `type` = `file`/`directory`; directory: size0/hash null; root directory имеет path `''` |
| `stores` | `{id,required,present,format,identitySha256,paths}`; present store имеет непустые distinct references на regular `files`; absent имеет null format/identity и `[]`; required absent запрещён |
| `external` | `{id,required,present,size,sha256,uid,gid,mode}`; required absent запрещён; absent имеет size/uid/gid/mode0 и hash null |

`id` соответствует `[a-z][a-z0-9._-]{0,95}`; present `format` — bounded ASCII identifier ≤128 characters. uid/gid — integer 0…4294967294; permissions — только 0000…0777, special bits запрещены. `paths` в store — только объявленные regular data files; store format и identity digest привязаны к независимому witness, но **не считаются проверенной доменной схемой/identity по SQLite header**.

Перед hash файлы сортируются по `path`, stores/external — по `id`, каждый store.paths — также по строке. Сравнение строк обычное lexicographic JS `<`/`>`, без locale/Unicode normalization. Из известных keys создаются новые объекты в порядке таблицы. `inventorySha256 = SHA256(UTF8(JSON.stringify({files,stores,external})))`; manifest hash — SHA256 такой же сериализации `{version,generationId,checkpointSha256,inventory}`. Нет generic recursive canonicalizer или доверия к объявленному входом digest. Порядок массивов входного JSON может отличаться; duplicate paths/IDs/refs запрещены.

`metadata.restoreFiles` — exact mapping только present external IDs в canonical Base64; decoded size и SHA проверяются, лишние/отсутствующие записи запрещены. Byte limits проверяются до decode; никакие Mounts.Source или names не становятся destination paths. Все values остаются внутри parser.

`expectedSha256` и `expectedManifestSha256` — pins из protected durable approved backup-stage evidence; `sourceWitness` получен отдельным cold inventory до backup. Reader пересчитывает manifest/inventory и file/config hashes, затем требует точного соответствия; omission одной DB одновременно из tar+manifest не проходит с прежним witness. R1a не создаёт witness и не доказывает полноту неправильно проведённого source inventory. Caller обязан передать независимый approved pin, а не взять его из входного архива.

## Порядок проверки и пределы

Один owned read descriptor: bounded envelope → streaming decrypt/metadata/tar checks → `decipher.final()` → полный `parser.finish()` → whole-ciphertext pin → abort/deadline check → awaited descriptor close → финальный abort/deadline check → safe receipt. Plaintext из `update()` существует только в process RAM и сразу поступает внутреннему parser, но **ещё не authenticated** до успешного final. Crypto/RSA public envelope сами по себе не подтверждают origin: его binding задаёт отдельно доверенный archive pin. Основание: [Node24.21 decipher.update/final](https://nodejs.org/download/release/v24.21.0/docs/api/crypto.html#decipherupdate-data-inputencoding-outputencoding).

Core один для R0/R1a: checksum/numeric fields/PAX/GNU long name, tar padding/end, SQLite prefix и empty-file rule. Strict дополнительно принимает materialized types только regular0/directory5; запрещает links, type7, devices/FIFO/sparse/unknown semantic extensions, special permissions. PAX keys только path/size/uid/gid/mtime/atime/ctime, repeated keys в одном накопленном scope запрещены; GNU long-link запрещён. Path-bearing header fields, PAX records и JSON metadata — strict UTF-8; C0/DEL/C1, backslash, absolute/drive/traversal paths запрещены. Tar leading `./` aliases и directory trailing `/` приводятся к canonical key; duplicates/file-directory collision запрещены, parent directory обязан предшествовать child. Manifest paths уже canonical.

Обязательные limits — positive safe integers, missing/unknown/Infinity/overflow отказ до открытия archive. Нет production defaults. Archive size проверяется по descriptor stat; plaintext, bytes одного file, aggregate data+external bytes, entries/store declarations, headers (включая extension/zero blocks), aggregate canonical paths/depth и external declarations/decoded bytes имеют отдельные пределы. Format caps остаются: header ≤16KiB, metadata/extension ≤4MiB, UTF-8 path ≤4096B/component ≤255B, chunk ≤64KiB. Новый PEM input ≤64KiB и archive path ≤4096 UTF-8 bytes проверяются до crypto/open; это не изменения R0 input contract.

Нет массива plaintext chunks: по одному ciphertext/plaintext chunk, одному entry digest, fixed SQLite100B prefix, одному bounded metadata/extension buffer и bounded manifest/maps. Buffers чистятся best-effort; строки JSON/KeyObject/GC memory не объявляются гарантированно стёртыми. Полный материализованный tar или plaintext file не создаётся.

Wall/idle checks используют monotonic `performance.now()` до/после await и перед receipt, reset idle только после actual bytes read. AbortSignal проверяется native getter без `reason`/caller getter. **Это cooperative refusal на safe points, не hard OS syscall deadline, Windows RAM/CPU isolation или доказанное время закрытия зависшего descriptor.** Independent watchdog/cleanup deadline/process exit относятся к будущему operator/helper lifecycle; здесь нет timer/child/sink и нет claim bounded-kill. Signal/close failure не даёт receipt.

Fixed errors: `restore_authentication_failed` (ключ/GCM/archive pin), `restore_incomplete` (manifest/witness/payload inventory), `restore_archive_invalid` (unsupported/ambiguous shape/type/path), `restore_limit_exceeded`, `restore_timeout`, `restore_io_failed` (также abort/unknown I/O). Message=code, stack=`Error: <code>`, без cause/path/secret/raw crypto. Unknown output и прочие поля не читаются. Не обещается общий containment произвольных rejected Promises, созданных вызывающим кодом; port принимает синхронные data options и native signal.

## Подготовленная приёмка, ещё не выполненная

Неизменённые R0 10 cases плюс 12 новых: system tar/real SQLite dry success и прежние fields; legacy/link parity; независимые archive/manifest/generation/checkpoint/inventory pins; joint DB omission; same-length changed body при valid GCM; missing/extra/ownership/config drift; aliases/PAX duplicate/traversal/types/UTF-8; 255/256-byte component edge; exact byte/count+1 limits; GCM/key/end refusal; hostile options/unknown output/abort reasons; capture до await.

Fixture использует прежний `encryptBackup` и system `tar --format=ustar -cf -` без plaintext tar file. Synthetic source files/SQLite создаются только в принадлежащем test temp directory; UID/mode headers заданы отдельно в fixture, поэтому это **не** Linux chown/no-follow/DAC evidence. Независимая literal fixed-key projection строит witness до producer manifest; tested core для expected hashes не импортируется. Sensitive comparisons — boolean/count, без raw buffer diff/secret data. В tar включён synthetic WAL-shaped sidecar; его hash presence не является валидной SQLite WAL/domain recovery проверкой.

Предлагаемый один serial root gate на закреплённом Node24.21.0 (PATH также для child CLI):

```text
node --test --test-concurrency=1 --test-reporter=tap deploy/connect/verify-backup.test.mjs deploy/connect/restore-backup.test.mjs
```

R0 unchanged tests SHA256: `1cc80d87a8fd9937feca5dba5df3a827fa2c6191150271702c3915e10b3e84ee`. Авторские source hashes зафиксированы отдельной передачей root/reviewer вместе с hash этого receipt. Следующий шаг — root runtime gate на exact frozen files; прежний R0 10/10 не присваивается новому reader без повторного исполнения.

## Дополнение: первый root gate и отдельный timeout oracle

Первый source-only freeze выше сохранён как история. Затем **root**, на совпавших пяти frozen SHA и pinned Node24.21.0, сообщил serial результат **22/22 PASS, 0 fail/skip/cancel, 4553.2542 ms**: прежние10 плюс новые12. Первый passed log остаётся самостоятельным evidence root; автор его не переписывал и этот запуск себе не приписывает. Independent reviewer прочитал frozen source без существенных блокеров; это source audit, не дополнительный runtime gate.

В первых12 cases не было отдельной причинной проверки cooperative `wallMs`/`idleMs`. По отдельному разрешению root добавлена только одна новая test group с тремя последовательными subcases: sufficient-limits control, absolute wall timeout при регулярном прогрессе и idle timeout после завершившегося настоящего чтения. Production три файла и прежние12/10 assertions не менялись.

Test-only перехват получает prototype от открытого/закрытого probe `FileHandle`; `t.mock.method(prototype,'read')` вызывает сохранённый настоящий read через `Reflect.apply` и возвращает его неизменённый результат. Виртуальный `performance.now` продвигается только после actual successful read. Wall profile: +1ms за чтение, wall3/idle100; idle profile: gap3ms, idle2/wall1000; control: +1ms, wall1000/idle100. Нет sleeps, ESM export monkeypatch, fake bytes/parser/crypto или нового production clock API.

На каждом фактически наблюдённом descriptor отдельно обёрнут настоящий instance `close`; completion фиксируется только после fulfilled original Promise, затем проверяется actual `fd===-1`. Отрицательные subcases требуют fixed `restore_timeout` и отсутствие receipt, а контроль проходит тот же genuine encrypted fixture. Все mocks восстанавливаются в `finally`, subcases идут с concurrency=false. Это проверка cooperative safe points и actual close на обычном fixture I/O, **не hard syscall timeout/forced process cleanup**.

Новая группа пока **не запускалась автором**; syntax/runtime/CLI/SSH не выполнялись. Root может повторить только её на окончательном source freeze, сохранив первый22-case log:

```text
node --test --test-concurrency=1 --test-reporter=tap --test-name-pattern="cooperative restore deadlines" deploy/connect/restore-backup.test.mjs
```

## Root runtime acceptance — 01.10.2026

Root исполнил два отдельных serial gates на Node24.21.0. Первый, на первоначальном frozen source: **22/22 PASS, 0 fail/skip/cancel, 4553.2542 ms**; log `output/implementation-20260930/p4-restore-reader-root-first.log`, SHA256 `5c358049413912516285743377466a085932cf5576a71f25b739bb14dbd7e453`. Второй, только добавленная timeout group с тремя subcases: **4/4 PASS, 0 fail/skip/cancel, 407.3425 ms**; log `output/implementation-20260930/p4-restore-reader-root-timeouts-first.log`, SHA256 `c4f6f8b660f81896c7840a579b7ec257d636927adee429d0e91c52352e86f098`. Это не один повторный общий suite.

Root повторно сверил unchanged production SHA: core `a8590d3bb27efb8d62ef703120b92bf8bbff81d72da79bdbd58ce74b0502d8ed`, R0 wrapper `a47766031b8c74cb41a485887d982df76881b72ef187c495418560fb0ef42cd3`, dry API `45b8d6c1cd4aee738dfb18fdc144faa1fb1a48a88ece19cf9c4994551604095f`. Старый10-case файл сохраняет pin `1cc80d87a8fd9937feca5dba5df3a827fa2c6191150271702c3915e10b3e84ee`. Удаление только двух новых imports и timeout group из final test восстановило точный первоначальный SHA `f52f39d360fa8c03968c1e893fb54b8c757e958a4be8f353fc99026f1033f6ad`; прежние12 assertions не менялись. Final test SHA `3d0e6898979e274a7b8d77738e9af7e633918b2324164f893228dc4bcd53745c`.

Publishing reviewer и whole-product critic независимо дали source GO без blockers; publishing reviewer сверил первый actual log и unchanged pins. Critic отдельно подтвердил причинность timeout delta и оба самостоятельных actual logs, после чего дал final checkpoint GO. Reviewers ничего не запускали, suite после PASS не повторялся.

Принимается только общий reader и strict dry admission на genuine encrypted synthetic fixtures. Linux extraction/ownership, transfer, полный producer, C/R image, first transition и production release этим не объявляются. Следующий implementation срез — R1b owned Linux quarantine по утверждённому R1-плану.
