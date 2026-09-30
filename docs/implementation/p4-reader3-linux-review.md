# P4 reader3: новый synthetic Linux canary — локальная подготовка

Статус: **source и review1 bundle заморожены для root/независимого review; remote не выполнялся**. Это реализация оставшегося delta gate из [reader3 rollout plan §5](p4-reader3-rollout-plan.md) с принятым [exact-source transport](p4-reader3-transport.md). Локальный serial gate: **30/30 PASS, 0 FAIL, 0 SKIP, 11 143.0706 ms** на Windows Node24.21.0 / SQLite3.53.4. Он не заменяет фактический Linux `/data`, foreign UID или read-only mount.

Прежние Linux2/bridge bundles, send-once identities, receipts и результаты не изменены и не исполнялись. Production probe/guard, transport, Dockerfile/readers label, host/OAuth/UI, конфигурация и serving данные не менялись. Новые исполняемые файлы находятся только в ignored `output/implementation-20260930/`; root владеет будущим разрешением запуска, самим remote запуском и result receipt.

## Прикреплённые точные исходники

Новый controller читает Git blobs через `git show` как bytes, сверяет длину/SHA до декодирования UTF-8, проверяет обратное bytes roundtrip. Ни CRLF rewrite, ни runtime minify нет. Следующие SHA относятся к **committed Git bytes**, а не к текущему CRLF checkout:

| Source | Commit | Bytes | SHA256 |
|---|---|---:|---|
| Новый `storage-probe.mjs` | `8803fcd9ea9fdea28e79ee8cab14ed70adfb849c` | 54 873 | `6e634e24f0faa67326ea45178c911ba666d90b205ebfd11756ed5a5a9be64050` |
| `storage-guard.mjs` | тот же | 11 042 | `04cc9468cb3f6b62ffb01b33ecad02390441ead9e2f3e838f8255b8d91e7a1d7` |
| `docker-api.mjs` | тот же | 3 439 | `38109ec270748cf8513450bd932160eb630e63f9597faee27af0aa636c97187a` |
| Настоящий прежний host `storage-probe.mjs` с readers≤2 | `0c8db3dbdc4a428c68ff98be9597223abc4da699` | 35 947 | `6dea52e8076bce0ecdb3a1652b14920d256ec539c4a2d26f95129460ec39d6ef` |

Frozen local `p4-reader3-source-transport.mjs` — 8 398 B, SHA `ee35da16306345388747ccb726efc285c7f6eaba37920e6d9506ec6453d91578`; не изменён. Literal1/2 SQL сохраняет прежние pins `6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e` / `5e459abc6afa376861c2032226bd29f78bf0468d`. Literal3 взят из committed fixture/provenance reader8803 и независимо сравнен с настоящей baseline closure `dc1ae217424b33cca0e9a5b60a6e4e719ea0d991`: delta DDL 15 157 B, SHA `05a3a9b2025bbe351e0ec75e28c67d4b497ba35b6de4932a1a5e439232d0978f`, итог 64 SQL objects. Tests материализуют exact Git bytes historical modules локально; remote programs не импортируют candidate application code.

Полный новый writer создаётся из проверенных literal SQL и отдельных synthetic seeds. Default-off reopen настоящим committed domain3 проверен без AS keys: native rows до и после genuine migration2→3 совпадают, полный schema layout совпадает с literal3. Это свидетельство plain storage consistency; synthetic OAuth BLOB не является выданным или расшифрованным token.

## Объём синтетических данных и программы

Новая фиксированная topology — **8 DB, 24 regular files, 0 symlinks, 14 directories, максимум8MiB**. Каждая DB сохраняет main/WAL/SHM. Изменения SQL завершаются до передачи filesystem ownership; после неё writer лишь читает witness и ждёт единственный внешний KILL.

| Case | Notes / Caps | Main header → logical WAL state | Ожидание |
|---|---|---|---|
| primary | 2 / 3 | Caps2 →3 | Новый exact CLI mode1 `/data` принимает |
| mixed13 | 1 / 3 | Caps2 →3 | Независимое распознавание обоих stores |
| baseline22 | 2 / 2 | genuine native2 | Оба новых/старых reader принимают |
| future-capabilities | отсутствует /4 | Caps2 →4 | `storage_format_unknown` |
| bad-capabilities-guard | отсутствует /3 | Caps2 →3, один exact guard удалён | `storage_format_unreadable` |

Первые три строки — по две DB, последние — по одной. Rooms/Apps в этом delta volume пусты: прежний Linux2 coverage не переименован в новый запуск. Новый canary не повторяет старую symlink матрицу.

Witness включает две Notes2 базы с immutable create proofs после edit/tombstone, Notes1/FTS, пять native Caps ledgers с uncertain input и purged committed receipt/budget, четыре OAuth connections/approved interactions/credential links и24 artifacts (по шести моделям). Их plain account/creator/resource/credential/artifact связи проверяются локально; все rows/schema сворачиваются в `controlHash`. Main/WAL имеют отдельные SHA до/после RO success и refusal, в том числе после закрытия RO handles. SHM bytes не заявлены неизменными. В synthetic stdout остаются только shape, sizes, hashes и численные counts; source/body/credential values не выводятся.

Манифест содержит все семь commands и их SHA:

1. `runtime-check`: memory-only SQLite/FTS, ожидаются Node24.15.0 / SQLite3.51.3.
2. `writer-fixtures`: единственный RW creator; READY → единственный KILL, exit137 только при `OOMKilled===false`.
3. `probe-v3`: exact packed Git source, `SOTY_STORAGE_PROBE=1`, положительный CLI `/data`.
4. `matrix-v3`: тот же exact decoded source, mode0, только четыре фиксированных roots.
5. `bridge-v2`: exact frozen old2 source, mode0; настоящий отказ на primary/mixed13 и приём baseline22.
6. `audit-all`: RO native/OAuth/bytes witness. Манифест отдельно помечает SHA template и максимального sizing witness; фактический command получает только проверенный READY snapshot. Этот sizing hash не выдаётся за runtime command hash.
7. `unknown-image`: CREATE с copied container label3, **никогда START**. Actual immutable image не имеет reader label: `guardStorageStart` обязан отказать до probe/START.

Всего **7 CREATE,6 START,1 KILL,0 STOP,1 volume**. Reader3 image compatibility в pure `assertStorageCompatible` обозначена `actualImageAttested:false`; старый declaration2 отказывает. Copied container label не повышает полномочия настоящего unlabelled image. У documented safe bridge остаются самостоятельными gates: host reader3; отказ настоящего old2; отказ fresh START по actual image metadata. **START полного application image3 / migration / fallback / restore этим artifact не доказываются.**

## Transport и ограничение команд

Loader сначала проверяет canonical envelope, полный consumed deflate stream, decoded bytes/length/hash и fatal UTF-8, затем импортирует точные bytes. До Docker admission controller повторно декодирует уже сохранённый envelope и сверяет его с exact source. Он **не recompress-ит** на Linux: иной zlib release не должен менять admission. Это отдельно проверено валидным alternative compression envelope.

Mode1 environment не подменяется; mode0 consumers продолжают только при `verifiedModule !== null`. Frozen decoder failure envelope остаётся ровно `{"ok":false,"code":"source_transport_invalid"}`, exit1, без stack/data URL/payload. Любой такой отказ несовместим с ожидаемым phase result и не превращается в частичный PASS. Native import failure не считается успешным reader execution.

Неизменённые limits: raw98 304 B, encoded command49 152 B, CREATE57 344 B, exact owned/serving/pre-ID-name inspect131 072 B с минимум16 384 B metadata reserve; остальные Docker responses65 536 B; synthetic stdout4 096 B; bundle524 288 B. Inspect sizing включает **обе** копии команды, Go escaping и newline. Response reader использует один bounded buffer, без массива tiny chunks.

| Command | Raw | Encoded | CREATE | Duplicated inspect budget | Metadata reserve |
|---|---:|---:|---:|---:|---:|
| runtime | 694 | 804 | 1 566 | 1 672 | 129 400 |
| writer | 19 207 | 19 362 | 20 133 | 38 858 | 92 214 |
| positive | 18 091 | 18 246 | 19 008 | 36 626 | 94 446 |
| matrix | 18 615 | 18 784 | 19 546 | 37 702 | 93 370 |
| old2 bridge | 14 442 | 14 609 | 15 371 | 29 352 | 101 720 |
| audit, maximum valid-field widths | 11 120 | 11 509 | 12 271 | 23 272 | 107 800 |
| unknown, never START | 16 | 77 | 978 | 218 | 130 854 |

Writer decoded source этой сборки —54 315 B / SHA `fde1627d0b682fcdeb973cf6d49ebbc07666b161cdaed2b067cbdf1a56b3f8f2`. Строка writer в таблице — packed command, не усечённый SQL. Размеры относятся ровно к review1 marker/runId. Общий Node/V8 RSS не выводится из command limits; runtime enforce-ит контейнер128MiB,0.5CPU,16PIDs,16MiB tmpfs, а любое OOM отказывает.

## Lifecycle и cleanup

Новый runId уже закреплён при локальной подготовке: `be39dd82e9fbc77beab99126007f5684`; namespace `soty-reader3-canary-be39dd82e9fbc77beab99126007f5684`. Начальная exclusive mkdir `/tmp/<namespace>` не допускает повторное исполнение этого bundle под новым случайным runId. До любой mutation журнал intent записывается/fsync-ится, затем допускается ровно один запрос; lost response разрешается только GET readback.

Все containers привязаны к одному exact image, owned name/id/label, единственному synthetic volume и mountpoint; `NoCopy:true`, network none, rootfsRO, restart no. Writer CapAdd ровно `[DAC_READ_SEARCH,CHOWN]`, RO ровно `[DAC_READ_SEARCH]`, CapDropALL. После синтетической записи directories10001:10001/0700 и files10001:10001/0600; RO phases имеют только read-search capability. Перед каждым START/KILL/DELETE перепроверяется serving identity/StartedAt, effective environment, mount/config и owned inventory. List running-only не собирает огромные Command всех stopped containers; перед удалением volume отдельный all-users GET требует отсутствие в том числе foreign stopped users.

Cleanup выполняет только unforced DELETE exact stopped containers с GET404, затем unforced DELETE empty owned volume с GET404. Нет повторного CREATE/START/KILL/DELETE, STOP, force или cleanup чужих ресурсов. Counts mutations — **попытки**, а не подтверждённые эффекты; cleanup PASS требует verified disappearance. Ambiguous KILL/running container/changed config сохраняют `needsReconciliation` и journal.20s phase /150s work /180s controller deadlines не являются resource TTL при потере процесса или связи. Serving id/image/running/StartedAt и непересечение mount проверяются повторно; выполнение любых serving/config mutations отсутствует.

## Локальная приёмка и freeze

Команда: изолированный `node --test --test-concurrency=1 output/implementation-20260930/p4-reader3-linux-canary.test.mjs`, один file, serial slot. Первый run28/30 выявил две ошибки нового harness: fixed registry ID не совпал с ID настоящего migrator; JSON trailer extractor нашёл delimiter внутри embedded source. Seed теперь читает actual registry ID, extractor выбирает точный последний suffix. Assertions не ослаблены. Итоговый полный повтор30/30 PASS.

Проверены single-attempt lost outcomes; foreign volume users; exact effective-env/config/no-copy/capabilities; byte/HTTP tiny-chunk bounds и unexpected outputs; OOM137 refusal; scoped128KiB inspect; pinned byte/loader/transport tamper до Docker; alternative compressed stream без runtime recompression; schema1/2/3 provenance; genuine domain2→3/keyless reopen; фактически выданный packed writer и crash-WAL reader/audit; exclusive bundle creation и JSON-only extraction.

Фактический локальный emitted-program witness:24 files,2 920 480 B, READY1 928 B, audit2 120 B. Windows-only seam меняет **только fixture ROOT и Linux ownership assertion**; decoded pinned probes не изменяются. Positive local probe использует exact mode0 export с явным private temp root. Это не positive Linux CLI1 `/data`; `validateReady` намеренно отвергает portable `foreignOwnership:false`. Child закрыт до удаления private temp fixture.

| Новый файл в `output/implementation-20260930/` | Bytes | SHA256 |
|---|---:|---|
| `p4-reader3-linux-canary.mjs` | 46 745 | `7382863a448a970295d7cb5d3cc77c6dd6e9a290f3d2298c73fcbefdb2d4236c` |
| `p4-reader3-linux-canary.fixtures.mjs` | 60 575 | `e76f9bc5784385009345942cc2411dcb338d9b0032efd9d632bad34a3c1a9000` |
| `p4-reader3-linux-canary.test.mjs` | 45 991 | `e656d6d4380062099f463db62ba655dbfdb51e94594a1043e1871c43e506c1ed` |
| `p4-reader3-linux-canary.author.log` | 3 570 | `8af2734ba3f0b8cfd4d0d0dcc0f1e12ba848fba6bd8a33807677b2d0e77b5de0` |
| `p4-reader3-linux-review1.bundle.mjs` | 314 647 | `1cd3ea95f41084f4e9d3afda939516838b8a6a56946bc1416bf80efad07db46e` |
| `p4-reader3-linux-review1.manifest.json` | 7 769 | `df48967f058c7d2d37819ecf8e47c9617e9a15fd3f7c4d7a86a94aebb9d79340` |

Bundle и manifest созданы `wx`, не импортировались и не исполнялись. Статически проверено точное равенство controller prefix + literal JSON spec + fixed trailer, source hashes/namespace и сохранённые caps. Переданные выше hashes новых файлов относятся к их фактическим local bytes; Git input hashes обозначены отдельно. Последующие обязательные gates: root и независимое source/bundle review, отдельное разрешение и единственный actual Linux run, затем root application image/default-off/original-authority/backup-restore/bootstrap gates. Ни один из них не объявлен закрытым этим локальным receipt.
