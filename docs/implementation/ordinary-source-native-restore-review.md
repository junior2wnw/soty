# Source Native3 restore: узкий отдельный profile для review

Статус: отдельно reviewed Source-specific Core implementation, **physical cold ещё не PASS**. Sourcecold51 прошёл seed/archive, отказался в dry_inspect. Его backup/private witness/source volume и пустой target сохраняются. Generic Root/R0 tar parser требует root-level `connector-store.sqlite`; ordinary Source хранит настоящую Native3 БД в `native/native.sqlite`. Добавлять фиктивную Connect DB, переименовывать Source БД, отключать Root invariant или выдавать old port за Native restore нельзя.

## Caller/callee

Новый host-only factory `createOrdinaryNativeRestorePorts({realmId})` возвращает три закрытых функции `inspect(input)`, `send(input)`, `extract(input)`. `realmId` exact `/^[a-z][a-z0-9.-]{0,63}$/` из reviewed private Source configuration; в cold fixture он compile-pinned `cold-synthetic`. Factory бренд WeakMap экземпляра; profile никогда не берётся из manifest/archive/body/RPC. У input те же exact поля/limits, что у существующих fixed ports; `profile`, `path`, `requiredDatabase`, callbacks, commands и alternate output не добавляются в public input.

Используются те же `SOTYBAK1` RSA3072-OAEP/AES-GCM, positional decrypt/authentication, independent ciphertext/manifest/source witness pins, double-pass authenticated sender, strict inventory, OwnedInput/OwnedOutput, sink/mount/dev/ino/UID/mode checks и byte/path/deadline limits. Новый profile меняет **единственную специализацию**: required database/store/realm вместо Root Connect. Legacy `verifyEncryptedBackup` и existing `inspectRestorableBackup/sendAuthenticatedBackup/extractOwnedBackup` продолжают использовать прежний Root profile и отвергать missing Connect. Ни Source OIDC, ни Native role/permission не создаются этим API.

Внутренний callee получает только private branded specialization: fixed required non-empty SQLite path `native/native.sqlite`, logical store `ordinary-native`, format `soty.ordinary-native.v3`, exact constructor realm. `TarVerifier` по этому private profile требует валидные SQLite magic/page shape для **этого** пути. Прежний empty-SQLite exception не применим к required Native DB. Другой путь, empty DB, fake Connect или unknown specialization не засчитываются. Strict inventory/source witness remain mandatory; не появляется произвольный required path или bypass parser.

В sealed metadata добавляется `sourceNativeCheckpoint:{schema:'soty.ordinary-native-checkpoint.v1',realmId,readerFormat:3,readerObjects:29,nativeIdentitySha256}`. Он строится из настоящего **independent literal Reader3** на остановленном Source до архивации; native identity digest уже входит в independently pinned required store. Source-specific dry port проверяет exact checkpoint shape, constructor realm, Native3/29 и соответствие `ordinary-native.identitySha256`. Поле metadata само не является SQL oracle или authority: доказательство realm/format исходит из заранее проверенного Source checkpoint + immutable ciphertext/inventory pins. После extraction отдельный Source-image literal Reader3 **заново** открывает реальные restored SQLite bytes с тем же realm; wrong realm/key/FK/schema отказывается до listener/любой Native grant.

Actual `metadata.original` захватывается через full immutable guarded seed inspect в состоянии exited0/noOOM/noRWwriter **до и после** архивации. ID/image/state timestamps/restart/config/mounts совпадают; выбранный original хранится только внутри encrypted metadata. Не подставляется `Running:false` без наблюдения. Никакие raw witness hashes/keys/paths/env/Docker errors не публикуются; closed errorcode/counts/equality only.

## Точные места изменения

1. `backup-format.mjs`: private branded specialization constructor внутри module; default Root required path остаётся literal. RestoreInventory для Source проверяет fixed store/checkpoint; TarVerifier выбирает только Root default или эту exact private specialization. `PlaintextVerifier`, `encryptedPass`, `createRestoreParser` передают внутренний бренд; no new caller JSON fields.
2. `restore-backup.mjs`: выделить internal implementations с private specialization argument; нынешние exported functions вызывают их с default Root. Отдельный `createOrdinaryNativeRestorePorts` создаёт closed branded specialization и вызывает те же implementations. No duplicate crypto/sink, no public parser/output callback.
3. `cold-runner.mjs`/`cold-extract-fixture.mjs`: вызвать только Source-specific factory, заранее pinned realm; encrypted checkpoint из actual seed witness. Старый Source cold packet immutable; новая canonical source list/artifacts/nonce после review.

## Негативы и приёмка

| Проверка | Требуемый результат |
| --- | --- |
| Root/R0 no-Connect | Real encrypted tar с Native DB, но без Connect по-прежнему DENY existing Root ports. Root legacy tar/receipts/counts остаются прежними. |
| Source known Native | System tar + real Native3/29 SQLite/checkpoint/config из independent reader, encrypt/dry PASS Source-only port; Source image physical restore/current literal reader отдельно. |
| Wrong path/missing/empty Native | Даже authenticated manifest/source witness не переключает fixed path. Wrong required store/format/realm/reader count или missing checkpoint DENY. |
| Same filename, foreign realm | Exact constructor realm mismatch DENY dry checkpoint; restored actual SQLite foreign realm DENY independent Source reader beforeSTART. Если archive/checkpoint пересозданы, original independent source witness/ciphertext pin не позволяет подменить первоначальные bytes. |
| Forged profile/extra input | JSON clone/unknown profile/path/getter/callback не выбирает specialization; existing Root input с Source profile extra field DENY. |
| Crypto/tar/sink regressions | Tamper/tag/key/header/paths/symlinks/hardlinks/duplicate entries/padding/limits/target custody/IOcleanup проверяются теми же methods; no relaxations. |
| Actual cold | Новый guarded packet с actual Sourceffce/B589, two fresh physical volumes, Source before/after original proof, source-specific sealed restore + Reader3/Reader2 refusal + startup key negatives. Не считать proposal/unit/tmpfs cold PASS. |

Новых Native/Root DB formats, OAuth/permissions или deployment здесь нет. Это Source-specific persistence specialization, не универсальный arbitrary Native inventory API. Core diff и новый RUN проходят отдельный review; frozen51 не повторяется. Local tests создают actual Native3 SQLite/system tar/encrypted envelope и проверяют Source-positive/Root-negative/fixed-path/realm/extra-profile/tamper. Docker-shaped original в unit test контролируемый; реальный stopped container proof и two-physical-volume restore засчитываются только отдельным Root RUN.
