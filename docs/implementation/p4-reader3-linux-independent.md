# P4 reader3 Linux canary — независимый review точного артефакта

01.10.2026. **Блокеров для одного синтетического запуска проверенного bundle не найдено.** Решение относится только к `p4-reader3-linux-review1.bundle.mjs`, 314647 B, SHA256 `1cd3ea95f41084f4e9d3afda939516838b8a6a56946bc1416bf80efad07db46e`, runId `be39dd82e9fbc77beab99126007f5684`. Это source/static verdict до remote run, не результат Linux и не разрешение считать production мигрированным.

Прочитаны полностью новый controller, fixtures, 30 локальных tests, [авторская квитанция](p4-reader3-linux-review.md), manifest, frozen transport и узкий root send-once wrapper. Сверены границы с [reader3 plan](p4-reader3-rollout-plan.md) и прежним [независимым review parser/guard](p4-reader3-independent.md). Авторские файлы не менялись. Никакие candidate modules, bundle или wrapper не импортировались/исполнялись; Docker, SSH, SQLite и тесты reviewer не запускал. Статические проверки использовали только чтение bytes, `git show`, JSON parsing, bounded deflate decoding, hashing и построение строк.

## Точная композиция

Независимо совпали следующие части:

- Bundle целиком равен сохранённому controller prefix до `LOCAL_PREPARE_ENTRY`, единственному literal JSON spec и фиксированному trailer. JSON roundtrip не добавляет код; spec runId и все source hashes совпадают с manifest.
- Probe3, guard и Docker API равны raw Git blobs `8803fcd9ea9fdea28e79ee8cab14ed70adfb849c`; old host2 — raw blob `0c8db3dbdc4a428c68ff98be9597223abc4da699`. Текущий OAuth WIP не подставляется вместо закреплённых sources.
- Сохранённые probe/writer/old2 envelopes независимо распакованы как данные: canonical base64, compressed digest/length, полный consumed stream, decoded digest/length и fatal UTF-8 совпали. Emitted loader восстановлен строками из проверенного frozen transport и равен каждому command. Никакой recompression на принимающей стороне для admission не требуется.
- Writer восстановлен из literal DDL, fixed topology и точных текстов функций fixtures; audit template — из тех же snapshot/witness функций. Оба совпали byte-for-byte с JSON spec. Matrix/old2 consumers совпали с fixed roots и проверяют `verifiedModule !== null`.
- Literal3 digest/length совпали с provenance reader8803; все семь raw files исторической domain3 closure `dc1ae217424b33cca0e9a5b60a6e4e719ea0d991` повторно совпали с её hashes/lengths. Сверка literal DDL и всех 64 SQL objects с настоящим migrator — исполняемый авторский case, не выдуманный новый SQLite run reviewer.

| Закреплённый decoded source | Bytes | SHA256 |
|---|---:|---|
| Probe3 | 54873 | `6e634e24f0faa67326ea45178c911ba666d90b205ebfd11756ed5a5a9be64050` |
| Old host2 | 35947 | `6dea52e8076bce0ecdb3a1652b14920d256ec539c4a2d26f95129460ec39d6ef` |
| Writer | 54315 | `fde1627d0b682fcdeb973cf6d49ebbc07666b161cdaed2b067cbdf1a56b3f8f2` |
| Frozen transport | 8398 | `ee35da16306345388747ccb726efc285c7f6eaba37920e6d9506ec6453d91578` |

Все семь строк measurement независимо пересчитаны и совпали с manifest, включая обе command copies, conservative Go escaping и newline. Максимальный encoded command здесь19362 B при cap49152; максимальный CREATE20133 B при cap57344; максимальный inspect command budget38858 B, reserve92214 B при cap131072. Лимиты не повышались. Больший inspect allowance применён только к exact serving/owned IDs и ожидаемым pre-ID names; остальные Docker responses ограничены65536 B. HTTP decoder использует один фиксированный buffer и абсолютный request timeout, не список произвольного числа tiny chunks.

Audit command в manifest правильно обозначен **maximum-size witness**, SHA `d412763b892a32ef894afb858f5cc692d75a0312a2987cef7ecb35e12045b3b6`. Это не обещание SHA фактического runtime command. Тот заполняется из прошедшего exact-key/type/count/byte `validateReady`, повторно проходит command limits и сравнивается с realized container config. Template SHA `66147ed381bb0ad7e5292f106d31465f25ab968b4de7cd54b32fccc77b96e557` совпал независимо.

## Граница необратимых действий

| Область | Что подтверждено чтением кода |
|---|---|
| Один запуск | RunId уже внутри immutable bundle. Exclusive remote `/tmp/soty-reader3-canary-<runId>` создаётся до первого Docker request; существующий каталог не переиспользуется. Перед каждой mutation intent записывается, fsync-ится и переименовывается с fsync journal directory. Внутренние `*Sent` допускают только одну попытку. |
| Serving | Fixed serving ID/image/name/running/StartedAt повторно проверяются. Новая synthetic volume не должна совпасть с serving volume name/mountpoint. Mutation allowlist принимает только exact task entries; serving ID дополнительно запрещён. STOP route отсутствует. Env/config/mount проверки относятся к создаваемым helpers; `servingUnchanged` не является полной проверкой здоровья приложения или побайтовым сравнением production данных. |
| Изоляция | Один новый local volume без driver options; NoCopy и ровно один realized `/data` mount. Fixed immutable image, rootfs RO, network none, restart no, без bind/device/extra namespace/port/volume mounts. Writer CapAdd ровно `DAC_READ_SEARCH,CHOWN`; остальные — только `DAC_READ_SEARCH`; CapDrop ALL. Memory128MiB, swap=memory,0.5CPU,16PIDs, tmpfs16MiB. |
| Writer handoff | Все SQL mutations и COMMIT заканчиваются до chown. chmod выполняется пока root владеет объектом, затем files10001:10001/0600 и directories10001:10001/0700. После handoff только read witness и ожидание; нет SQLite close/checkpoint перед внешним KILL. Symlinks запрещены в этой новой topology. |
| Исполнение | Семь fixed phases, семь CREATE, шесть START. Writer KILL разрешён только один раз, после exact READY и свежих identity/config/inventory checks. Exit137 принимается только вместе с явным `OOMKilled===false`; отсутствие OOM evidence также отказывает. Unknown-image helper создаётся, но никогда не стартует. |
| Concurrent users | Перед действиями читается running-only volume inventory, затем каждый собственный ID inspect-ится отдельно. Foreign running user блокирует работу. Перед volume DELETE читается all-users inventory: даже foreign stopped user блокирует удаление. Большие Command семи stopped helpers не объединяются в64KiB listing. |
| Неизвестный исход | Потерянный ответ CREATE/START/KILL/DELETE разрешается только readback, не повтором. Timeout/running/config drift оставляют exact identities и `needsReconciliation`. Диагностика сохраняет фиксированные классы, counts и stderr digest, не source/body/SQL/raw stderr. |
| Cleanup | Удаляются только проверенные собственные created/exited containers, без force и без volume deletion через container call. После каждой попытки требуется GET404. Volume удаляется только после disappearance всех owned containers и пустого all-users inventory, тоже unforced и с GET404. Неизвестный ID, running helper или непроверенный config не удаляются вслепую. |

Это fail-closed контроль собственного процесса, а не транзакция с любым посторонним Docker administrator. `/tmp` marker не является вечным anti-replay хранилищем после очистки host; durable local intent ниже остаётся отдельным запретом повторной отправки. Deadlines20/150/180s ограничивают работу controller, не гарантируют срок жизни ресурса после process/SSH loss. Эти ограничения не скрыты за `PASS` и не требуют расширять этот узкий canary в общий orchestration framework.

## Что доказывают SQL/WAL witnesses

В новом volume восемь DB: primary2/3, mixed1/3, baseline2/2 и две отдельные Caps negative базы. Для пяти Caps native ledgers initial schema2 и его rows сначала checkpoint-ятся; новая DDL3/marker и OAuth seeds остаются в WAL. Future4 и удалённый exact OAuth guard также записаны в WAL. Notes1/2 сохраняют свой header, а новые rows — WAL. Writer проверяет main header и logical `PRAGMA user_version` отдельно.

READY требует24 regular files,14 directories,0 symlinks, ≤8MiB и foreign ownership. У каждого case есть main/WAL SHA, sizes и version; metadata точной формы не содержит private marker. Witness проверяет FKs, schema counts и bounded row snapshots, Note edit/tombstone proofs, uncertain input, purged success receipt и reserved/spent budget, OAuth account/creator/resource/credential/artifact links. Schema и plain table rows входят в `controlHash`; FTS отдельно проверяется реальным MATCH. Main/WAL hash покрывает также остальные страницы SQLite.

После нового RO reader и отказов old2/future4/bad guard audit сравнивает прежний snapshot. Затем закрывает все RO handles и **ещё раз** сравнивает main/WAL SHA. SHM bytes не заявляются неизменными. Старый host2 здесь действительно другой pinned source, а не выключенный branch3 нового reader.

Это synthetic plain-storage witness. Cipher blobs не являются реально выданными/расшифрованными OAuth artifacts; fake seed не доказывает bearer/consent/refresh semantics. Полный domain3 default-off reopen и genuine migration2→3 присутствуют только в локальном авторском test с materialized committed sources. Реальная application migration не выполняется ни writer, ни host probe.

Unknown-image phase разделяет три факта: pure reader3 declaration совместима с DTO3; pure old declaration2 отказывает; настоящий immutable **unlabelled** image отказывает через `guardStorageStart` до probe/START, несмотря на copied container label3. `actualImageAttested:false` корректен. Ни один пункт не доказывает START будущего application image3.

## Root single-send wrapper

Дополнительно полностью прочитан `output/implementation-20260930/p4-reader3-linux-send-once.mjs`, SHA256 `2eb62a3f1bd840af3d96675c892ba2413119d5d4752d9060bf417fad73a6a328`, и его узкий diff относительно Linux2 wrapper. До spawn он проверяет exact bundle length/hash и создаёт новый fsynced `wx` send-intent. Затем одна передача exact bytes через SSH без shell; прежний fixed host alias/node path. Есть210s timeout, один262144 B stdout buffer, stderr только count/hash. Receipt признаётся известным только при exact schema и runId. Timeout/error/overflow/parse failure/nonzero exit не становится успехом; output/result файлы также `wx`, retry не разрешён.

Wrapper не доказывает, что убийство локального SSH завершило удалённый controller или writer. При неизвестном результате требуется отдельный read-only reconciliation по сохранённым IDs; ни второй send, ни новый случайный runId из этого review не следуют. Wrapper в review не запускался.

## Проверенные версии и предел результата

| Файл | Bytes | SHA256 |
|---|---:|---|
| `p4-reader3-linux-canary.mjs` | 46745 | `7382863a448a970295d7cb5d3cc77c6dd6e9a290f3d2298c73fcbefdb2d4236c` |
| `p4-reader3-linux-canary.fixtures.mjs` | 60575 | `e76f9bc5784385009345942cc2411dcb338d9b0032efd9d632bad34a3c1a9000` |
| `p4-reader3-linux-canary.test.mjs` | 45991 | `e656d6d4380062099f463db62ba655dbfdb51e94594a1043e1871c43e506c1ed` |
| `p4-reader3-linux-review1.bundle.mjs` | 314647 | `1cd3ea95f41084f4e9d3afda939516838b8a6a56946bc1416bf80efad07db46e` |
| `p4-reader3-linux-review1.manifest.json` | 7769 | `df48967f058c7d2d37819ecf8e47c9617e9a15fd3f7c4d7a86a94aebb9d79340` |

Авторский документ имеет SHA `e668674c966580eee8829ad3e0c5487f4505aeb3b09f98c0881a45302d0c1b39`. Прочитанный author log `p4-reader3-linux-canary.author.log`,3570 B/SHA `8af2734ba3f0b8cfd4d0d0dcc0f1e12ba848fba6bd8a33807677b2d0e77b5de0`, сообщает **30/30 PASS,0skip,11143.0706ms**, Windows Node24.21.0/SQLite3.53.4. Reviewer его не повторял. Реальный локальный child crash/WAL witness:24files,2920480 B, READY1928 B,audit2120 B. Portable seam заменяет fixture ROOT и foreign-ownership enforcement; `validateReady` намеренно отказывает этому `foreignOwnership:false` результату как remote evidence.

На момент этого review Linux запуск ещё не выполнен. Actual Node24.15.0/SQLite3.51.3, `/data` CLI1, UID10001 и настоящий RO mount, Docker exit/OOM/cleanup и сохранность serving должны быть подтверждены единственным отдельным root run. Даже его успех не закрывает application reader3 labels/START, same-image rollback, unlabelled-serving bootstrap, согласованный encrypted backup/restore и production migration admission. Старые Linux artifacts и их результаты не изменены и не переименованы в новое доказательство.
