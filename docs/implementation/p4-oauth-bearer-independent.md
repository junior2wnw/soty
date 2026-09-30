# P4-C1c T2 — независимый review bearer и повторного действия

2026-10-01. База T1 — `8497650`. Независимо прочитан узкий T2 diff в `access.mjs`, `native-notes.mjs`, `oauth-connections.mjs`, `index.mjs`, связанные common policy/native lifecycle и новый авторский fixture/test/OS worker. Production и авторские assertions reviewer не менял. Source review завершён на final freeze автора; нового существенного blocker не выявлено.

## Authority и срок исходного действия

OAuth actor создаёт только captured factory. Публичные поля actor не служат свидетельством: требуется запись в приватном WeakMap того же instance. Reference содержит connection/issuer/static profile/resource. Каждый последующий resolve заново проверяет credential, точный retained link, текущую connection, principal/client, creator device и всю grant chain. Копия объекта или старый actor другого service instance не становится авторизованным. Legacy parser `soty_cap_` сохранён; OAuth factory требует именно managed credential/link.

Keyless bearer authentication читает неизменяемые digest/link и текущую authority. Она не требует доступности шифрования AS и не обходит expiry/revoke. Это позволяет законный доступ к старому результату при отключённой выдаче токенов. Удалённый key или отключённое исполнение не должны превращаться в новую выдачу права.

Новый AT того же Grant даёт текущему actor доступ к разрешённому replay/истории. При dispatch по-прежнему читается **исходный** credential из persisted authorization snapshot и его абсолютный expiry; reference не заменяется токеном повторившего запрос клиента. После Notes COMMIT внутренний reconcile сначала проверяет неизменяемое доказательство эффекта. Отзыв закрывает внешний read, но не стирает уже совершённый эффект и не превращает его в `not_applied`.

## Повторное согласие и занятый ключ

Native admission выполняется в прежнем Connect→Caps fence. Порядок source:

1. Свежая authorization текущего actor.
2. Старый same-client request и точный fingerprint: законный replay до readiness/нового бюджета.
3. Captured OAuth scope, затем indexed account/request-key lookup старой connection с теми же issuer/static profile/resource.
4. Только при отсутствии занятого ключа — проверка identity/binding, invoke authority, quotas/reservation и INSERT.

Crossconnection ответ — только `invocation_request_conflict`: старые ID, receipt, body, status и существование текущей Note не возвращаются. Это принятый leakage одного бита о занятом ключе в своём namespace. Иной валидный input не превращает тот же ключ в новое действие. Нет новой identity или второго ledger.

Lookup присоединяет неизменяемую retained connection, а не временный AT artifact/credential. Поэтому cleanup истёкшего AT, отзыв старой семьи и terminal input purge не освобождают ключ. Same-connection refresh сохраняет прежний replay; другие аккаунты, profiles/resources и legacy clients имеют принятую отдельную область ключей.

## Readiness и исторический результат

`oauth.readiness().available` проверяет schema3/registry/project, key и captured fixed native binding с реальной Notes2 identity. Binding check намеренно не равен `executionEnabled`; host отдельно решает, можно ли стартовать AS. Authorized bearer/read/replay не имеют глобального readiness guard. Новый invoke по-прежнему проходит operational gate.

Исторический receipt остаётся свидетельством создания, а не текущей версией записки. Common projection не читает новый текст/состояние Note; удаление или человеческое редактирование не вызывает восстановления Note либо замены результата. Original native credential/link сохраняются и у terminal Invocation.

## Проверки и точные ограничения

Новый авторский файл содержит8 focused cases: private actor/link/instance, original deadline против refresh, история после34 edits и purge, proof-after-revoke, разделение namespace, creator/root/sibling policy, две OS admission и независимые readiness причины. Reviewer прочитал tests и fixture. В T2 payloads токенов конструирует тестовый код и сохраняет через настоящие encrypted ports; signed Connect, Notes/Caps SQLite и две OS writers настоящие. Это не полный Provider HTTP/PKCE, не два AS instances и не CLI.

Независимое чтение заметило fixture `iiat` вместо объявленного `iat`; автор подтвердил остановку семи зависимых cases и поправил fixture. Это не product defect и не доказательство пройденных веток до исправления. Автор также уточнил ожидаемую существующую ошибку missing Invocation. Production по этому gate не менялся. Промежуточные результаты не суммируются с окончательными.

Reviewer прочитал полный итоговый лог `output/implementation-20260930/p4-oauth-bearer-focused.log`: **8/8 PASS, 0 FAIL, 0 SKIP, 3482.0695 ms**, Windows Node24.21.0 / SQLite3.53.4. SHA256 лога — `a6f6a1f014110e8f77a3135873b22fabbdc348cf7b66bbab65c66ea4d07e2c9e`. Это авторский executable результат, а не новый независимый прогон. Проверка SQL захватывает фактический runtime запрос и выполняет SQLite EXPLAIN с его параметрами: index seek по account/request без общего SCAN или temporary sort. Число совпадений одного account/key не объявляется константным.

После этого прогона автор поправил только устаревший header comment coordinator и добавил безопасный вывод уже проверенного EXPLAIN без IDs/parameters. Эти дельты прочитаны отдельно; assertions/SQL/production behavior не изменены. Совместный прогон root на окончательных bytes остаётся отдельным свидетельством. Две адаптации прежних tests сохраняют unavailable при отсутствующем native binding и заменяют прежнее намеренно закрытое bearer API на проверку actual owned actor; старые проверки шифрования и expiry не удалены.

Повтор авторских8 reviewer не запускал: выбранные инварианты покрывает этот gate, последующая независимая задача — actual host с реальными портами. Нет новых независимых тестов, замыкающих копию implementation. Этот текст не заявляет полной C1, MCP, CLI или production readiness.

## Проверенный final pin

SHA256 фактических worktree bytes после описанных comment/diagnostic дельт. Каждый приведённый hash проверен reviewer локально; исторические T1 pins им не подменяются.

| Файл | SHA256 |
|---|---|
| `modules/capabilities/server/access.mjs` | `6fc166d1fca78efd80a90a9e270ba2b8bb0f980aeaa2e17553ef7af557277586` |
| `modules/capabilities/server/index.mjs` | `cc6f7b24f33e189606ca1be286e1316691e5b90be6b29f4fa91f2a52d7cd9135` |
| `modules/capabilities/server/native-notes.mjs` | `bd3f0a13489b97adb021f2955dfa07b8f31cf27d4b02849207869c75a7454675` |
| `modules/capabilities/server/oauth-connections.mjs` | `5165b04fad68e395da4057f3052e6040c92897742b5b188242abeaaea2a0c93e` |
| `modules/capabilities/test/oauth-bearer.test.mjs` | `1f331c22027e75889ab397aaaff1aa76065eb9316920370eadbacbd464b1028c` |
| `modules/capabilities/test/support/oauth-bearer.mjs` | `0eef0ad60a9dac8367f5cb835af674b5bbc7367047211d59698bce380955fc52` |
| `modules/capabilities/test/support/oauth-bearer-worker.mjs` | `223777b718a91b068e7cdb2d2463b4c4fb2f721aac534e7ca8ae2f3d05bff32f` |
| `docs/implementation/p4-oauth-bearer-receipt.md` | `24a4f7b33e93938d37b1e75c56057538cfef1a0501f92e82dc58a04ca2f3f1e9` |
