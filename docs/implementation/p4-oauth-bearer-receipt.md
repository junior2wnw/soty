# P4-C1c T2 — linked bearer and native occupied-key gate

2026-10-01. Основа — принятый T1 `8497650`; контракт реализации — [p4-oauth-bearer-plan.md](p4-oauth-bearer-plan.md). Этот срез открывает domain bearer/readiness и deny-only проверку ключа. Он заменяет историческое ограничение T1 «bearer закрыт / available всегда false», не меняя DDL или OAuth wire profile.

## Изменение и API

`service.oauth.authenticateBearer({token,audience})` принимает ровно opaque AT из 43 base64url-символов и настроенный resource. В коротком Connect→Caps fence private Access factory находит credential по SHA-256, проверяет её immutable OAuth link, issuer/resource/account, активную connection, creator и root chain. Возвращается прежняя публичная форма service actor. В WeakMap этого экземпляра хранится закрытая ссылка на connection; копирование полей actor не даёт полномочий. Каждый последующий resolve снова проверяет текущее состояние, включая expiry/revoke. Raw AT не расшифровывается и не сохраняется повторно.

Legacy `authenticateCredential` и его `soty_cap_` parser не изменены. Ни один новый actor factory не возвращается из service API. Captured seams `authenticate({tokenDigest,issuer,audience})` и `scope(actor)` доступны только composition и требуют текущую Caps transaction. Namespace не принимается из пользовательских native arguments.

`service.oauth.readiness() -> {schemaVersion,available}` теперь проверяет exact Caps3 identity, настроенный ключ/ports и настоящую фиксированную native binding с Notes2/project/registry. Это не admission эффекта: `executionEnabled` проверяет прежний native operational gate. Поэтому при выключенном исполнении `oauth.available` может быть true, а `nativeNotes.readiness().ready` — false. Host отдельно проверяет оба условия перед запуском enabled AS. При отсутствии ключа, native port, Notes2 или правильной identity `available=false`. Keyless bearer/read/replay остаются отдельной разрешённой возможностью. После закрытия service методы по-прежнему отказывают.

Неподходящая форма token/audience или credential — `authorization_required`; актуальный deny цепочки может вернуть прежний `access_denied`. Неверная shape аргументов сохраняет `oauth_invalid_artifact`; storage/transaction ошибки используют существующие safe domain codes. Чужая Invocation возвращает прежний `invocation_not_found`, без различения отсутствия и чужого доступа.

## Original authority и повторный ключ

Текущий AT после refresh может читать и точно повторять Invocation той же connection/grant. Dispatch snapshot остаётся прежним: исходная credential, audience и absolute expiresAt. Новая credential не продлевает разрешение на уже принятую операцию. После family/creator/root revoke внешнее чтение запрещено; уже совершившийся Notes effect может завершить proof-first внутренний reconcile. Signed owner history сохраняется независимо от наличия AS.

В `nativeNotes.admit` порядок: fresh current read ACL → прежний same-connection lookup/fingerprint replay → закрытый OAuth namespace → occupied-key deny → fixed store/readiness/invoke/budget/new INSERT. Для другой connection в том же `(account,issuer,staticProfile,resource,requestKey)` результат всегда `invocation_request_conflict`, даже при другом допустимом input, terminal/purged Invocation или истёкшей/отозванной прежней connection. Никаких ID, body, receipt или current Note hints из старой строки. Один бит о занятости opaque key — явно принятый контракт; случайные уникальные ключи рекомендуются, их энтропию сервер не доказывает.

SQL использует существующий covering `cap_invocations_oauth_request(account_id,request_key,client_id)` и join immutable connection по внутреннему client/account. Проверка захватывает фактический runtime SQL и передаёт его SQLite `EXPLAIN QUERY PLAN`: account+request seek, без `SCAN i`, `SCAN c` и temporary sort. Это исключает общий проход по истории; количество совпавших строк данного account/key не объявляется константным. Разные account/profile/resource и обычные service credentials сохраняют независимые пространства ключей. Нового ledger/index/DDL нет.

## Авторская проверка

Команда из active worktree, с process-local PATH на isolated Node24.21.0 / SQLite3.53.4:

```powershell
node --test --test-concurrency=1 modules/capabilities/test/oauth-bearer.test.mjs
```

**8/8 PASS, 0 FAIL, 0 SKIP, 3482.0695 ms.** Лог: `output/implementation-20260930/p4-oauth-bearer-focused.log`. После этого в тест добавлена только безопасная diagnostic строка с уже проверяемым EXPLAIN, без параметров/IDs; assertions и production semantics не менялись. Общий allcaps repeat выполняет root отдельно.

Сценарии используют настоящие Connect/Notes/Caps SQLite stores и подписанные Connect owner operations:

1. Private brand: скопированный и старый после reopen actor, неверный resource, RT/service-token crossover и утраченный exact link отказывают; fresh keyless bearer работает без включения выдачи.
2. AT1 admit → expiry → RT rotation/AT2: current read/replay успешны, changed-input conflict; keyless operational-off replay работает, original dispatch закрыт, reservation освобождена без Note. Persisted original credential/expiry не изменены.
3. Native create → 34 подписанных человеческих правки → purge → refresh → credential cleanup/reopen: original terminal reference сохранена, transient expired credential удалена; внешнее чтение возвращает прежнюю create receipt, не новое содержимое или обещание существования Note.
4. Реальный Notes COMMIT и synthetic потеря возврата до Caps receipt → cancel/family revoke → reopen без OAuth composition → proof-positive settle и owner history. Новое согласие не читает старую Invocation и не создаёт effect/reservation по старому ключу; changed input, истечение прежней connection и disabled execution не меняют generic conflict. Здесь же actual-query EXPLAIN.
5. Другие static profile, resource и account, а также legacy service actor независимо принимают тот же key. Same-connection refresh повторяет прежнюю Invocation.
6. Revoke постороннего устройства/root не ломает нашу действующую chain; revoke нашего root или creator закрывает доступ и будущий effect.
7. Два отдельных OS processes с собственными Connect/Caps/Notes handles одновременно спорят за один namespace/key: один admission/reservation, другой generic conflict; исполнение победителя создаёт один Note/proof и тратит одну единицу. Есть barrier после constructor и ожидание настоящего child close.
8. Operational-off, keyless, absent native, Notes1 и неверный project проверяются отдельно; готовность не подменяется fixture boolean.

Первый запуск: 1/8 PASS, семь `ReferenceError` из новой fixture (`iiat` вместо `iiat: iat`) до token INSERT. Второй: 6/8 PASS; две новые assertions ошибочно ожидали `not_found` вместо существующего `invocation_not_found`. Исправлены только fixture/ожидания, не рабочий ACL. Эти logs сохранены как `p4-oauth-bearer-first.log` и `p4-oauth-bearer-repeat.log`; они не объявляются product RED. После meaningful gate изменений production semantics не потребовалось.

В T2 OAuth payloads создаются fixture и проходят реальные encrypted ports; authenticated Provider request snapshots — fixture input. Это **не** end-to-end Provider HTTP или CLI OAuth proof. Два процесса проверяют domain lock/idempotency, не две публичные AS instances. Момент потери возврата после Notes COMMIT — явная fault injection, не убийство процесса. Нет browser/CLI/remote/Linux/deployment изменений и нет утверждения о завершении P4 целиком. Independent critic прочитал production/test source без нового существенного blocker; его отдельная квитанция и root host/общий gate не подменяются авторскими 8/8.

## Frozen inventory

SHA-256 локальных bytes. Пути относительно `modules/capabilities/`:

| File | SHA-256 |
|---|---|
| server/access.mjs | 6fc166d1fca78efd80a90a9e270ba2b8bb0f980aeaa2e17553ef7af557277586 |
| server/index.mjs | cc6f7b24f33e189606ca1be286e1316691e5b90be6b29f4fa91f2a52d7cd9135 |
| server/native-notes.mjs | bd3f0a13489b97adb021f2955dfa07b8f31cf27d4b02849207869c75a7454675 |
| server/oauth-connections.mjs | 5165b04fad68e395da4057f3052e6040c92897742b5b188242abeaaea2a0c93e |
| test/oauth-bearer.test.mjs | 1f331c22027e75889ab397aaaff1aa76065eb9316920370eadbacbd464b1028c |
| test/support/oauth-bearer.mjs | 0eef0ad60a9dac8367f5cb835af674b5bbc7367047211d59698bce380955fc52 |
| test/support/oauth-bearer-worker.mjs | 223777b718a91b068e7cdb2d2463b4c4fb2f721aac534e7ca8ae2f3d05bff32f |
| test/oauth-connections.test.mjs | b625f29ddddf70bc5b43db4ad0146c1d40141f973a42c653bc08ffa5062422fb |
| test/oauth-tokens.test.mjs | fbff3a0b9c2ac7a389bb5a9850b4951b5fa74c6eb1f53f3bb1f68b0f7ad569e4 |
| README.md | a7e5b5ec0bc20e5db974c5aaa1b81f1cf2702f31a6869231fff4fed72131e716 |

Изменения двух прежних tests ограничены новыми ожиданиями bearer/readiness: их fixtures без native composition по-прежнему unavailable. README обновляет три устаревших описания; историческая квитанция domain ports получила только ссылку на этот срез.

Неизменённые: `server/oauth-schema.mjs` ca2810310bab42a1d0232459163aa34eb5088de8d610aab411c17324e1002288; `server/schema-v3.mjs` ebed724aed700443eadb84fd96476b14fea971c31f44b6779aa3b4b0b098e557; `server/catalog.mjs` 498a06025107598b450406114bc331aef71cd0824c384dfdf7927d10dc6fc072. Semantic digest Notes@1 остаётся `95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204`. Notes/host/provider/UI/readers не редактировались этим owner в T2.
