# P4 reader3 — ограниченная передача точного source

01.10.2026. Локальная реализация по согласованному подплану. Это helper/emitter и его собственные проверки, **не Linux3 canary, Docker controller, image acceptance, migration или remote run**. Старые review2/review3/Linux2 bundles и receipts не меняются и не исполняются.

## Причина и граница решения

Принятый host reader3 `8803fcd9ea9fdea28e79ee8cab14ed70adfb849c` содержит probe 54 873 bytes, SHA256 `6e634e24f0faa67326ea45178c911ba666d90b205ebfd11756ed5a5a9be64050`. Читаются raw `git show` bytes, без worktree EOL и без нормализации. Неупакованная команда превышает прежние49 152 bytes; LF этого не исправляет. Основание: [независимый reader3 review](p4-reader3-independent.md).

Новое владение ограничено:

- `output/implementation-20260930/p4-reader3-source-transport.mjs`;
- `output/implementation-20260930/p4-reader3-source-transport.test.mjs`;
- этой квитанцией.

В helper нет Docker/HTTP/SSH клиентов, filesystem writes, container configuration mutations, startup/cleanup journal и CLI, выполняющего remote действия. Создание итогового canary bundle и его controller остаются отдельным reviewed шагом. `measureTransportCommand` измеряет JSON; он не разрешает configuration/container/image.

## Контракт

`packSource(Buffer, {bytes, sha256})` сначала копирует и проверяет ожидаемые bytes/hash и UTF-8, затем использует встроенный `deflateRawSync`: level9/windowBits15/memLevel9/default strategy. Envelope версии1 содержит exact source bytes/SHA, compressed bytes/SHA и canonical base64. Source не переписывается, не минифицируется, не дополняется и не исправляется.

`decodeSource(envelope)` и verifier в выданном ESM используют один исходный verifier. До `import()` последовательно проверяются:

1. Exact scalar shape, собственные data properties без getters, known codec/version; source≤98 304 bytes, compressed≤32 768 bytes.
2. Ограниченная canonical base64, actual compressed length/hash.
3. `inflateRawSync` с `maxOutputLength` ровно declared source size; полный consumed input через `info.engine.bytesWritten`. Игнорируемый deflate suffix не принимается.
4. Exact decoded length/hash и fatal UTF-8 validation. В data URL передаются **исходные decoded bytes**, не строка после TextDecoder.

`emitVerifiedSourceModule` проверяет envelope ещё при локальной сборке и выдаёт ESM. После успешного декодирования он импортирует `data:text/javascript;base64,...`, экспортируя `verifiedModule`. При ошибке проверки или импорта выдаёт только `{"ok":false,"code":"source_transport_invalid"}`, ставит exitCode1 и экспортирует null. Он не печатает exception/stack/data URL/payload. Будущий fixed-root consumer обязан продолжать только при `verifiedModule !== null`; эта заготовка не заменяет его обработку ошибок `readStorageFormat`.

Hash является pin целостности из доверенной сборки, не подписью и не авторизацией arbitrary кода. Корректно pinned исполняемый source остаётся доверенным кодом; wrapper не обещает откат его эффектов при последующем runtime exception. Применение ограничено прочитанным probe со встроенными `node:*` imports и без `import.meta`. `storage-guard.mjs` с относительным `./docker-api.mjs` так не переносится.

## Сохранение поведения

Loader не меняет environment. В `SOTY_STORAGE_PROBE=1` точный imported blob выполняет прежний top-level CLI с hardcoded `/data` и собственными ограниченными error codes. Успешный loader не добавляет stdout. В mode0 он сохраняет exports для прежней fixed-root matrix. В обоих режимах сохраняются все SQLite SQL/projection/guard checks.

Node24.15 уже документирует [data URL modules с builtin imports](https://raw.githubusercontent.com/nodejs/node/v24.15.0/doc/api/esm.md) и [zlib maxOutputLength/info/bytesWritten](https://raw.githubusercontent.com/nodejs/node/v24.15.0/doc/api/zlib.md). В [точном исходнике Node24.15 `processChunkSync`](https://raw.githubusercontent.com/nodejs/node/v24.15.0/lib/zlib.js) `bytesWritten` получает накопленный объём consumed input; выход проверяется против `_maxOutputLength` до возврата. Это source evidence поддержки используемых API, не исполнение нового loader на Linux24.15. Относительные imports у data URL не поддерживаются. Новая опция `rejectGarbageAfterEnd` позднейших Node здесь не используется.

Compressed bytes создаются один раз принятой локальной сборкой; Linux выполняет только decode. Воспроизводимость фиксируется для точных source/helper bytes, compression options и builder Node/zlib. Идентичность compressed bytes между произвольными версиями zlib не обещается; decoded source обязан совпасть всегда.

## Неизменённые budgets

| Граница | Bytes |
|---|---:|
| Raw emitted program | 98 304 |
| Encoded `Entrypoint+Cmd` | 49 152 |
| CREATE JSON body — прежний более строгий prepare gate | 57 344 |
| Exact owned-container GET inspect response | 131 072 |
| Minimum reserve сверх duplicated/Go-escaped command fields | 16 384 |
| Прочие Docker responses — отдельный прежний controller | 65 536 |

Измерение inspect сохраняет прежний `max(direct-with-newline, 2*goCommandBytes+64)` для повторов команды в `Path/Args` и `Config`. Учтено Go escaping `<>&` и U+2028/U+2029. Никакой cap не повышается. CREATE measurement не подтверждает допустимость будущих image/mount/capability полей.

## Локальная проверка

Собственный serial gate завершён: **8 tests, 8 PASS, 0 FAIL, 0 SKIP; 1675.7016 ms**. Runtime: Windows, Node **24.21.0**, zlib **1.3.2.1-motley-8002e91**. Команда: `node --test --test-concurrency=1 output/implementation-20260930/p4-reader3-source-transport.test.mjs` из изолированного runtime `var/toolchains/node-v24.21.0-win-x64`.

Проверены exact Git roundtrip/reproducibility; malformed pins/descriptor/accessor/base64; UTF-8 и CRLF без преобразования, imports/exports/env; corrupted/truncated/concatenated stream, size bomb и UTF-8 rejection до sentinel effect; generic syntax/runtime import failures; actual pinned mode1 под запретом `/data`; actual pinned mode0 с private Notes2/Capabilities3 и future4 refusal; прежние command/CREATE/Go-inspect bounds. Негативные runtime cases исполняют фактически выданный loader, а не отдельную копию verifier. Для ошибки до evaluation синтетический source пытается создать sentinel: в каждом таком случае файл отсутствует, stdout содержит только фиксированный отказ.

Mode1 на Windows намеренно исполняется под Node permission с запретом `/data` и сравнивает bounded failure direct exact blob против loader. Это **не positive Linux `/data` proof**. Реальные DB создаются только в отдельных проверяемых temp directories; child завершается до cleanup. Нет доступа к serving данным, mounts или config.

| Измерение принятого probe / loader | Bytes |
|---|---:|
| Exact source из Git | 54 873 |
| Deflate payload | 11 123 |
| Canonical base64 payload | 14 832 |
| Выданный ESM / raw command | 18 091 |
| Encoded `Entrypoint+Cmd` | 18 246 |
| CREATE JSON синтетической конфигурации теста | 19 006 |
| Go-escaped command | 18 281 |
| Два поля inspect с newline | 36 566 |
| Консервативный duplicated-command budget | 36 626 |
| Budget + обязательный reserve16KiB | 53 010 |
| Остаток до inspect cap для metadata | 94 446 |

Raw source representation действительно отвергнута прежними caps. Compressed representation помещается без их изменения. CREATE19 006 относится к конкретному тестовому body: итоговый controller обязан заново измерить каждый полный будущий CREATE и inspect budget. Ограничены decoded bytes/выход zlib и сериализованные команды; общий RSS Node/V8/import cache этим тестом не ограничен.

Фиксация SHA256:

| Артефакт | Bytes | SHA256 |
|---|---:|---|
| `p4-reader3-source-transport.mjs` | 8 398 | `ee35da16306345388747ccb726efc285c7f6eaba37920e6d9506ec6453d91578` |
| `p4-reader3-source-transport.test.mjs` | 14 675 | `385b948071b8d4f3f3b4b08497ff088f8c83a028c89a32d77f1dcd04227e0a11` |
| Deflate payload | 11 123 | `9fcb81098f3d848de35b187d7c5cd190c94c55e27941aed0678fbca1189ca809` |
| Выданный ESM | 18 091 | `a72530905c5c98bd2bb3cc4a32ea197e535a36c67c50f403565cec902dda6117` |
| `p4-reader3-source-transport-author.log` | 1 651 | `a864bc999a46aa2d34226921b8f9c294806226caee12bde20c2617a095e61130` |

Существующие controller/probe/bundles не изменены. Выданные тестовые программы исполнялись только из временных локальных файлов с последующим cleanup; итоговый исполняемый Linux3 bundle не создан. Следующие отдельные gates: независимое/root source review, интеграция в новый controller с повторным измерением полных конфигураций, freeze нового artifact и разрешённый фактический Linux3 run. Данный результат не доказывает Linux3 `/data`/WAL, application image, START3, migration, bootstrap, rollback или restore.
