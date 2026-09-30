# P4 — независимый review actual OAuth/native HTTP gate

2026-10-01. Узкая область: `server/test/capabilities-oauth-native-flow.test.mjs`, его новый `support/oauth-native-http.mjs` и фактически используемый `support/native-capability-http.mjs`; новый `server/capabilities-oauth-document.js` и только delta `renderError` в Provider wrapper. Общая host composition проверяется отдельно. Production/test source root reviewer не менял, дополнительный suite не запускал.

## Что действительно исполняет стенд

`nativeHttpFixture` заранее создаёт изолированные Notes2/Caps3 явными opt-in migrations, затем запускает настоящий `createHttpApp` на собственном loopback listener. Connect identities bootstrap-ятся через HTTP challenge и реальные ECDSA proof, а approve/deny подписываются тем же маршрутом. Readiness/authority/crypto/storage ports не заменены заглушками. Все code, AT и RT выдаёт actual Provider9.12.2 по HTTP.

Новый helper сохраняет настоящие host-only cookies, проверяет их path/secure/expiry и получает HTML/context. Он сам подписывает owner decision вместо браузерного consent controller. Для смены A→B читает штатный session-bound XSRF из настоящей autoform, POST-ит только точный внутренний `/oauth/session/end/confirm`, затем следует одному дополнительному exact-issuer resume. Эта пропущенная в первом helper версия ступень была fixture defect, а не отказом domain authority. Redirect callback проверяется по зарегистрированному адресу/state/issuer, но никогда не запрашивается. Token requests идут без cookie; raw authorization values не печатаются в evidence.

Это настоящий HTTP/provider/domain сценарий с ручным тестовым клиентом, не native browser, не настоящий CLI и не две AS instances. Cookie jar не объявляется доказательством всей браузерной SameSite/CSP политики. Local callback receiver и полный MCP ещё отдельные gates.

## Review пяти cases

1. A→B→A проходит через один настоящий AS session и новые signed decisions. OAuth create даёт один Notes proof/result; чужой аккаунт получает404. Новая connection A получает только generic occupied-key409. После человеческой правки и refresh прежняя connection получает прежнюю receipt, без нового/старого Note body. Проверка main/WAL ищет именно фактически выданные code/AT/RT, а не заранее подставленные строки.
2. Wrong resource и PKCE не расходуют code: последующий правильный exchange успешен. Неверный resource при refresh не расходует прежний RT. Reuse уже использованного RT durable отзывает его семью и новый AT, а sibling остаётся действующим.
3. Keyless AS-off restart использует новую production app без передачи AS keys. Уже выданный bearer читает/replay-ит своё действие; refresh503 и новое исполнение503. Signed owner revoke затем закрывает старое чтение. Счётчик Invocation остаётся1.
4. Proof Боба с expected account Алисы не создаёт authority. После подписанного deny completion иного аккаунта отвергается; настоящий callback содержит отказ, без code/connection/credential.
5. Неизвестный/истёкший Provider resume возвращает статический usable error400/no-store, без отражения UID, script, form или inline event handlers. Hash фактического style сверяется с CSP.

Reviewer прочитал полный root log `output/implementation-20260930/p4-oauth-native-flow-final.log`: **5/5 PASS, 0 FAIL, 0 SKIP, 8079.1854 ms**. Это root executable evidence; независимый результат — чтение source/fixture/лога. Upstream WARNING об уже разобранном request body присутствует и не скрыт: bounded parser передаёт поддерживаемый Provider `req.body`. Никакого утверждения о снятии этого предупреждения нет.

## Статическая error page

Документ строится один раз из серверных palette/hex helpers. В него не входят request parameters, account, error text или OAuth secrets. Единственный переход — относительный `/`; нет script, form или загрузок сторонних ресурсов. CSP фиксирует style по SHA256 и запрещает script/form/frame ancestors. `renderError` сохраняет no-store и статус Provider.

Существующий post-provider callback CSP middleware работает только на `resume` с status200 и актуальной Interaction; он не расширяет error400 policy нового документа. Общий обработчик ошибок/ingress не изменялся этим narrow delta. Визуальные размер/фокус/контраст здесь source-reviewed, но не проверены reviewer в браузере.

Новых существенных blockers не найдено. Host5 дополняет domain T2; не заменяет two-AS crash/concurrency, CLI, native browser и production release gates.

## Проверенный pin

SHA256 фактических worktree bytes:

| Файл | SHA256 |
|---|---|
| `server/capabilities-oauth-document.js` | `076c830590e3b7fa7517bec93e12e87a65a77863c462c0bb76586fa1f5c616f3` |
| `server/capabilities-oauth-provider.js` | `fc2b2f8b3d8fe363bb08ca991fa3f5120b78cb9fd7547cff809b52651440c432` |
| `server/test/capabilities-oauth-native-flow.test.mjs` | `9e7c4c10b3b2c4f0b5b81827e4fb18d4783a0f1f60780bb6bf9fb62b9c91b657` |
| `server/test/support/oauth-native-http.mjs` | `027f656f4c228f8da4f9c98da660b21786e701dc1bfd861530cdb34119d187d6` |
| `server/test/support/native-capability-http.mjs` | `f121096ab0057318cf0e41c4773bedea614dc737b731ecfa54d47f1fee6342f1` |
| `output/implementation-20260930/p4-oauth-native-flow-final.log` | `4bae87edcb2a0697b802ab7fe1227306b378bc3ef1a1f8f19073b182831dd03a` |
