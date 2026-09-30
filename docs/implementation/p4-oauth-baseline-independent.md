# P4-C1 — независимая приёмка Domain3 baseline

30.09.2026. Прочитан текущий frozen diff, а не только авторская квитанция. **Материальных блокеров в проверенной границе не обнаружено.** Независимый набор: **7/7 PASS, 0 FAIL, 0 skips**, первый запуск, 963.8967 ms. Это приёмка reader3 и общей политики сохранённых связей; не готовность OAuth, consent, клиента или deployment.

Основание: [storage contract](p4-oauth-storage-contract.md), [author baseline](p4-oauth-storage-baseline.md), [host baseline](p4-oauth-host-baseline.md). Собственный файл — [oauth-baseline.independent.test.mjs](../../modules/capabilities/test/oauth-baseline.independent.test.mjs). Production, авторские helpers/tests и исторические fixtures этим рецензентом не менялись.

## Что проверено независимо

Новый тест не импортирует `test/support/oauth-baseline.mjs`. Историческую2 создаёт настоящая замороженная import closure из `cc7f65b84e0a8f8b4fd41cf44e498ee3f21015e7`; четыре source hash проверяются собственными константами. Обычные clients/principals/grants создаёт реальный `createCapabilitiesService.execute`. Host predicate локальный детерминированный, **подписанного Connect в этих семи cases нет**. OAuth connection/artifact/link rows — явные SQL fixtures с placeholder ciphertext. Они проверяют plain storage и политику, не шифрование или библиотечную выдачу токенов.

| Case | Наблюдаемое доказательство |
|---|---|
| Genuine2 → default reader2 → explicit3 | До миграции создаётся делегированный legacy grant на отдельный client, credential и authority другого аккаунта. Default reopen не меняет main/WAL, остаётся2 без OAuth-объектов. Explicit3 сохраняет registry, все прежние SQL objects и строки всех прежних таблиц кроме lineage metadata. Старый actor и fresh actor после reopen имеют прежние права/expiry. Последующий отзыв root закрывает delegated actor, другой аккаунт сохраняет доступ. |
| Legacy parent → OAuth principal | Попытка derivation из допустимого legacy parent в managed principal отказывает. Передать `clientId` в создание нового principal нельзя. Чужой owner не получает credential. Legacy-shaped token в synthetic OAuth row не получает legacy actor. Все отказы оставляют counts clients/principals/grants/credentials/audit неизменными; обычное legacy делегирование продолжает работать. |
| Expired family без AS/ключа/artifacts | После expiry и удаления encrypted artifacts baseline открывает3. Чужой аккаунт не может отозвать credential и не создаёт audit event. Владелец может отозвать просроченную семью без ключа; повтор сохраняет первый revoke timestamp и единственное увеличение epoch. Исходная expiry неизменна, fresh credential независимой семьи другого аккаунта действует. |
| Corrupt rows при exact DDL | Отдельно: удалённый parent credential с оставшимся FK link; cross-account connection у link после восстановления exact trigger; AccessToken artifact без link. Exact layout recognizer ещё распознаёт3 — он не обещает row integrity. Полный initializer и service оба отказывают `capabilities_storage_corrupt`, без открытой transaction и без изменения main/WAL. Два последних случая имеют чистый `foreign_key_check`: их ловит проверка отношений rows. |
| Unique-digest REPLACE | При `recursive_triggers=OFF` новый credential ID с digest исходной linked credential не может вытеснить её через `INSERT OR REPLACE`. Исходная строка побайтно по значениям неизменна, новый ID отсутствует, разрешение исходной credential сохраняется. |
| Original reference после terminal/input purge | Synthetic generic terminal Invocation с `input_json='null'` продолжает запрещать удаление original link. Отдельная expired unreferenced credential/link удаляется одной transaction. После удаления artifacts, revoke и reopen остаются ровно исходная связь и её прежняя expiry. Это прямое доказательство общего retention predicate; **не synthetic замена настоящего native proof**. |
| Partial3 / unknown4 | Лишний index и изменённый trigger с прежним именем отказывают exact layout;4 отказывает unsupported. Main/WAL не меняются. При runtime4 ранее открытый сервис также отказывает credential authentication/issue, без authority writes. |

Команда, изолированный Windows Node **24.21.0 / SQLite 3.53.4**, toolchain directory предварительно добавлен в PATH:

```text
node --test --test-concurrency=1 modules/capabilities/test/oauth-baseline.independent.test.mjs
```

Log: `output/implementation-20260930/p4-oauth-baseline-independent-first.log`. Отдельного RED у этого набора не было. После успешного запуска изменено только слово в комментарии — `signed-owner` заменено на `owner access`, чтобы не подразумевать настоящую Connect подпись. Код/assertions не менялись. Слот возвращён root до его общего regression.

Все SQLite файлы находятся в отдельных случайных temp directories с собственным marker. Cleanup закрывает handles, проверяет canonical path, parent, prefix и marker, затем удаляет только этот каталог. Production data/config/credentials, remote и браузер не использовались. Равенство файлов относится к main/WAL; transient SHM не включён в обещание неизменности.

## Source review и границы доверия

Прочитаны все восемь production files из frozen inventory и оба новых author test files. `schema-v2.mjs` меняет только экспорт существующего `validateNativeRows`, без копии нового native validator/DDL. `schema-v3.mjs` распознаёт layout/metadata до persistent pragmas, повторяет распознавание внутри `BEGIN IMMEDIATE`, проверяет FK/native/OAuth rows. Explicit2→3 атомарно добавляет объекты и меняет lineage/version; default-off не означает автоматическую2→3. SQL guards защищают immutable links и исходные времена; проверка DDL сама по себе не подтверждает отношения rows или ciphertext.

Общая политика доступна независимо от AS flag/key. OAuth authority определяется по client, principal и actual root из leaf grant. Требуются exact credential link, account/client/principal/root/resource/time pins и текущая legacy chain/creator authority. Expired/revoked связь может законно сохраняться для истории; это не право нового исполнения. Baseline не создаёт OAuth actor через legacy token parser. Новый обычный principal получает отдельный client; generic issue/derive не расширяют managed authority.

Native coordinator захватывает действительный admitted format **2 или3** и затем требует ту же version, lineage, project и registry. Это не `>=2` и не silent live migration. Original credential snapshot не заменяется более свежей credential. Positive-proof settlement после revoke остаётся внутренней reconciliation, а внешний read должен пройти актуальную авторизацию.

Retention guard защищает любое `authorization_json.credentialId`, включая terminal history, и использует прежний Invocation ledger. Исторический link не требует сохранившегося expired AT artifact; первоначальный INSERT требует такой artifact. Future encrypted port обязан отдельно проверить AEAD/decrypted payload и runtime quotas — baseline этих проверок не имитирует.

## Root integration review и отдельная атрибуция

Read-only проверен one-line diff `server/http-app.js`: scheduler запускается только при Notes2 и Caps **[2,3]**. Ни schema4, ни mixed1 не расширяются; AS key/флаг исполнения не добавлены как условие recovery. Прочитан новый `server/test/capabilities-schema3.test.mjs`: actual host открывает предварительно подготовленную3 без migration admission/key, создаёт один native effect, после disabled reopen сохраняет scheduler, lawful receipt/owner Notes/history, а новый create отказывает без второго effect. Этот рецензент HTTP suite повторно не запускал.

Результаты root отдельно: первоначальный host RED3/2 был вызван null scheduler; затем **11/11 PASS**, 4713.9792 ms, `p4-oauth-host-baseline-green.log`. Источник/границы настоящего Connect+HTTP описаны в [host receipt](p4-oauth-host-baseline.md). Авторский **85/85** и его real Connect proof-after-purge относятся к [author baseline](p4-oauth-storage-baseline.md), не включены в мои7.

Также прочитана узкая root-поправка прежнего `native-storage.acceptance.test.mjs`: advertised support `[1,2,3]`; marker-only3 остаётся отрицательным `schema_layout_invalid`, отдельно добавлен future4 `schema_version_unsupported`. Сохраняются actual1/2, literal history, main/WAL и DELETE journal/no-repair assertions. По сообщению root полный module run дал168 PASS/2 устаревших assertion FAIL, затем изменённый файл9/9 PASS; это два разных запуска, не один зелёный общий log.

## Проверенные hashes

Все восемь production hashes повторно совпали с author freeze после собственного запуска:

```text
ca2810310bab42a1d0232459163aa34eb5088de8d610aab411c17324e1002288  oauth-schema.mjs
2b7d90dd4764e6c34df9c3c724b0e576ebd483a42910eaf3baffb33b15f0a9c2  oauth-baseline.mjs
ebed724aed700443eadb84fd96476b14fea971c31f44b6779aa3b4b0b098e557  schema-v3.mjs
2622db5d571920ca070f45a0839bf0e3269f24f7f26744860c297b77be823fc8  schema-v2.mjs
e84001c4956e5a7836ab303f4eca63630febd12f3bd0ef0ca71b27c3c4969f80  schema.mjs
d154ea1900538099e8bfae40445eef2bb87b56d6a152c57472f33845ce24fb5f  index.mjs
7bec9706662e5835cfbf01a49775c0b9cf6676efecbaafe02e504eff062b6142  access.mjs
159d81d92f5df6394cb664a63780b092aecf03560e299d2cc21e5396316a10bd  native-notes.mjs
f6c918a85f94523e796c6e9722fed96b419f2c4a94165e906349e71a80eca64b  server/http-app.js
52029b973f94d910ff3403526d00715e62ed352cacc363f1b29546feae954cc6  server/test/capabilities-schema3.test.mjs
65e0262d9004796dcef3d2b3b94ca13ad82e5404553415a42492c67b32d016aa  independent test at 7/7 run
fa73dcfe89f34b15f620341c73ec0538c8cae138890dd27a72eec650ccc02b7d  independent test after comment-only clarification
```

Первые восемь paths относятся к `modules/capabilities/server/`. Historical fixture hashes и commit закреплены в тесте и [provenance](../../modules/capabilities/test/fixtures/capabilities-v2/provenance.json). `git diff --check` не нашёл whitespace ошибок; новые собственные файлы проверены отдельно, поскольку они ещё untracked.

**За пределами вердикта:** encrypted AS ports/key rotation; настоящая выдача/consume/refresh/revoke OAuth; signed consent; crossconnection native dedup implementation; cleanup batch/capacity; два AS OS processes; полноценные Codex/OpenCode сценарии; Docker/Linux reader3, actual serving fallback, coherent restore и rollout. Никакие labels, старые reader2 результаты или baseline3 DDL не подменяют эти следующие gates.
