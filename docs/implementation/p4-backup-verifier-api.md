# P4-D2-R0 — reusable encrypted-backup verifier

Дата: 01.10.2026. Текущий статус: **root serial gate — 10/10 PASS, 0 fail/skip/cancel**, pinned Node24.21.0; отдельная execution attribution ниже. Это только API/CLI extraction, не backup/restore/full-image/production acceptance. В этом обновлении изменены документы; code/tests и исходный log сохранены, новых прогонов не было.

Исторический авторский source-only freeze:

> Дата: 01.10.2026. Статус: **source-only freeze для root serial gate; тесты ещё не выполнялись**. Область: `deploy/connect/verify-backup.mjs`, его существующий test file и этот документ. Runtime/CLI/syntax-check/tests/hosts/SSH/models не запускались, real keys/config не читались. Это API extraction, не backup/restore/release acceptance.

## API и CLI

Из `deploy/connect/verify-backup.mjs` экспортирован:

```js
const receipt = await verifyEncryptedBackup({ file, privateKeyPem });
```

`file` — путь к существующему encrypted archive, `privateKeyPem` — private RSA PEM вызывающей стороны. Значения не печатаются, plaintext/callback/stream наружу не возвращаются. API принимает объект, не stdin. Чтение/destructuring options находится **внутри** нормализующего try/catch, поэтому null, undefined и throwing getter имеют тот же безопасный failure contract.

Успех возвращает ровно прежние поля:

```js
{
  ok: true,
  authenticated: true,
  offline: true,
  archiveEntries,
  roomFiles,
  sqliteFiles,
  emptySqliteFiles,
  sha256
}
```

Counts имеют прежний смысл; `sha256` относится к полному encrypted archive. Promise разрешается только после успешного GCM `final()`, окончания всех tar/SQLite-header проверок и закрытия file handle. Parser может отказать раньше authentication; ни такой отказ, ни неверный tag не возвращают success.

**Любой API failure** становится новым `Error` с `message` и `code` ровно `backup_verification_failed`, `stack` ровно `Error: backup_verification_failed`, без `cause` и без исходного error object. Таким образом, ни path, ни crypto diagnostic, ни сообщение бросающего getter не входят в public exception. `JSON.stringify(error)` содержит только safe code. Исходные причины намеренно не выдаются этому API; дополнительной diagnostic/secret callback нет.

CLI entrypoint отделён в private `main()` и запускается только при прямом вызове данного файла (`import.meta.url === pathToFileURL(process.argv[1]).href`). Обычный import не читает stdin, не пишет stdout/stderr и не меняет exitCode. Прямой CLI сохраняет прежний JSON input, UTF-8 BOM handling, stdin limit и output:

- Success: одна JSON-строка прежнего receipt в stdout, stderr пуст.
- Failure: stdout пуст, stderr `{"ok":false,"code":"backup_verification_failed"}`, exitCode1.

CLI вызывает тот же exported API; криптографическая проверка не продублирована.

## Сохранённые пределы и смысл проверки

| Граница | Неизменное условие |
| --- | --- |
| CLI stdin | Не более 65,536 bytes, включая BOM/JSON/trailing whitespace; API не сериализует свой object input под этот транспортный лимит |
| Archive/header | Regular file, magic `SOTYBAK1`; header JSON минимум2 bytes, prefix12+header не более16,384 bytes; проверяются format/algorithm/keyId |
| Crypto | RSA минимум3072 bit, RSA-OAEP-SHA256, AES-256-GCM; plaintext key32 bytes, IV12, auth tag16; prefix используется как AAD |
| Encrypted metadata | 2..4,194,304 bytes; требуются offline=true, original stopped, tar dataFormat и `/data` volume в metadata |
| Tar | Header512 bytes; extension data не более4 MiB; safeName максимум4096 characters; прежние checksum/length/padding/EOF/type/path проверки |
| Streaming/SQLite | Ciphertext reads по64 KiB, SQLite prefix максимум100 bytes; прежние header/page-size/alignment checks и обязательный nonempty `connector-store.sqlite`; другие допустимые zero-byte SQLite сохраняются |

R0 **не** вводит extractor, plaintext output, новый формат, key rotation или application startup. В существующем verifier нет общего archive-size/entry-count/deadline cap, full SQLite integrity/domain-row проверки или cross-store consistency proof; локальные parser bounds не являются таким обещанием. Приём допустимых link types в tar не делает этот модуль безопасным extractor. Эти прежние пределы отдельно учтены в [release/recovery plan](p4-release-recovery-plan.md).

## Проверки, подготовленные к запуску root — source-only история

В `verify-backup.test.mjs` сохранены все шесть прежних causal tests. Добавлены четыре:

1. **Import с hostile stdin.** Отдельный Node child имеет synthetic unread input и throwing read/async-iterator spies. Actual dynamic import обязан завершиться с нулевыми read counters, без stdout/stderr; через IPC возвращаются только counters/наличие API. Result принимается после child close.
2. **Actual API ↔ actual CLI.** Один настоящий encrypted fixture с system tar, SQLite и empty SQLite открывается обоими путями. Сравниваются exact safe fields, значения receipt и независимо вычисленный encrypted SHA256.
3. **API failure projection.** Wrong RSA key, invalid PEM, bad tag, truncated ciphertext, authenticated corrupt tar, null/undefined и throwing synthetic getter дают только фиксированные message/code/stack, без cause/extra properties. Assertions проверяют boolean, чтобы не печатать regressed raw error в лог.
4. **CLI byte boundary.** Валидный JSON с padding до ровно65,536 bytes проходит и совпадает с API; плюс один byte отказывает. Test helper учитывает возможный EPIPE после отказа child, но успех/отказ всё равно определяется настоящими close/exit/output, не событием записи stdin.

Все fixture keys генерируются только самим будущим test process; private keys и реальные конфигурации пользователя тестам не нужны. Плановый узкий run: pinned Node24.21.0, `--test --test-concurrency=1 --test-reporter=tap deploy/connect/verify-backup.test.mjs`. Здесь это **не выполненная команда** и не PASS10/10. Root запускает serial gate, затем independent reviewer проверяет evidence; результат добавляется отдельной атрибуцией.

Выполнено только статическое сравнение source с исходным HEAD: parser block от constants до `readExactly` совпал, crypto/file verification core совпал после удаления изменённого отступа. Результаты `ParserSourceUnchanged=true`, `VerificationCoreUnchangedExceptIndent=true` не являются runtime/syntax proof. Все старые assertions сохранены; изменён только helper для дополнительного stdin input/EPIPE и добавлены новые tests.

## Root execution — отдельная атрибуция

01.10.2026 root выполнил один serial прогон на **pinned Node24.21.0** с совпавшими frozen source/test SHA ниже:

```text
--test --test-concurrency=1 --test-reporter=tap deploy/connect/verify-backup.test.mjs
```

Итог: **10 tests / 10 PASS / 0 FAIL / 0 SKIP / 0 CANCEL**, 2786.5212 ms. Это шесть сохранённых causal gates и четыре добавленных API/import/CLI-boundary cases. Сведения об исполнении принадлежат root; автор этого документа не повторял suite.

Первый log: [p4-backup-verifier-root-first.log](../../output/implementation-20260930/p4-backup-verifier-root-first.log), SHA256 `a449fa7d1aed4b1e4b82f00909668e584c2dac3af24674c643be873e30756aed`. При документировании read-only сверены hash и TAP summary; log не переписывался. Исходный source-only freeze и его границы выше сохранены как история.

Этот результат подтверждает reusable API, отсутствие CLI side effects при проверенном import, безопасные failures и совместимость прежних verifier/CLI checks на encrypted fixtures. Он не подтверждает backup реальных stores, extractor/isolated restore, полный application image, migration/START или production release.

Независимый reviewer `publishing_architecture` отдельно сверил source/test pins, неизменённый первый TAP log, этот документ и PROGRESS: **GO, без blockers**. Source ordering GCM → parser → file close → receipt соответствует проверенному коду. Reviewer не повторял suite и не присвоил себе root execution; параллельный SSH inventory не включён в R0 acceptance.

## Source freeze

SHA256 working-tree bytes; это не Git LF blob hashes. Hash данного документа сообщается отдельно, чтобы не создавать самоссылку.

| Файл | SHA256 |
| --- | --- |
| `deploy/connect/verify-backup.mjs` | `fa234635bbac7d9d98eb4bd9e7c8f59ebf5c638d0aa8283665fe80b43274c9a1` |
| `deploy/connect/verify-backup.test.mjs` | `1cc80d87a8fd9937feca5dba5df3a827fa2c6191150271702c3915e10b3e84ee` |
