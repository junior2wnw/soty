# P4-C1 — независимый host/Provider review

01.10.2026. Проверены host profile, wrapper pinned **oidc-provider 9.12.2**, router и клиентский consent controller. Production-файлы рецензент не менял. Собственный executable scope — [capabilities-oauth-account-switch.test.mjs](../../server/test/capabilities-oauth-account-switch.test.mjs).

**Итог: 13/13 PASS, 0 FAIL, 0 skips, 2540.9363 ms** на Node24.21.0/SQLite3.53.4, включая финальную metadata correction. Найденные host-проблемы исправлены интегратором; исходные причинные RED сохранены. Непогашенных блокеров в проверенном host slice не осталось. Это приёмка конкретного router/Provider поведения, а не полного OAuth продукта: production routing ещё не смонтирован, authoritative encrypted token flow не выполнен.

## Существенные findings и исправления

**Смена аккаунта A→B.** Настоящий HTTP flow уже выдал A code/AT/RT, затем новая явно одобренная fixture consent выбрала B. `lib/actions/authorization/resume.js` при отличающемся аккаунте формирует autoform POST на внутренний `/oauth/session/end/confirm`. Прежний host route запрещал этот POST: правильный библиотечный XSRF получал 400, B callback не достигался. Одновременно библиотечное discovery рекламировало публичный RP logout, который не входит в продуктовый контракт. Causal RED: 3 tests, 1 PASS / 2 FAIL, 622.6554 ms; отдельный closed-route fault control прошёл.

Root отключил `rpInitiatedLogout`, явно закрепил `end_session` route и разрешил только внутренний POST confirm с точным Origin и допустимым `Sec-Fetch-Site`. Библиотека сама проверяет Session-bound XSRF. Публичные GET logout/success остаются закрыты. Штатный `Session.destroy` не должен отзывать ранее выданную family: `expiresWithSession:false` записывает `persistsLogout:true`. Это проверяется реальным A refresh после B token, а не только чтением настройки.

Новая autoform — отдельный HTML document, поэтому CSP предыдущей consent page недостаточно. Root post-Provider middleware расширяет только `form-action 'self'` ровно одним зарегистрированным callback из публичного `ctx.oidc.entities.Interaction.params`; существующий inline script SHA библиотеки и остальные directives остаются. Сам callback в HTTP-тесте не запрашивается. Native browser CSP поведение является отдельным root gate.

**Срок browser binding второй consent.** При первой странице в t0 и второй в t590 прежний код сохранял nonce-cookie только до t600; ещё действующее второе подтверждение в t610 получало 403. Root actual HTTP negative использует настоящую зашифрованную Interaction и cookie jar с контролируемым Date, но синтетическую presentation facade без approval. После исправления показ consent перевыпускает тот же ещё действующий nonce с новым signed timestamp/Max-Age600, сохраняя привязку других вкладок. Deadline каждой Interaction не продлевается. Исходное окно AS Session в 600s — отдельное ограничение, а не новая полная сессия на каждую страницу. Root causal RED→GREEN support5/5, 908 ms; этот файл включён и в независимый composed run.

**Доменный `Interaction.trusted`.** По pinned source поле имеет array shape: `interactions.js` сохраняет `oidc.trusted`, `resume.js` использует default `[]`; boolean относится к отдельной PAR модели. Текущий профиль без JAR/PAR должен разрешать отсутствие или пустой массив, отказывать boolean/nonempty array. Finding передан root и domain author. Автор сообщил отдельный causal actual Provider HTTP authorize→fixture login-only→resume→second consent через encrypted aux: RED500/`oauth_invalid_artifact`, затем GREEN с `Interaction.trusted=[]`; 0 Grant/token/connection rows. Его составной aux17+owner13+HTTP1 дал 31/31 PASS, 4918 ms. Это **атрибуция автору**, не 31 дополнительных независимо запущенных теста. Source refreeze `oauth-profile.mjs`: `587b072b45bf7e041a9bd6c4705abe6ab9a5da5c8b0a3e29df748a7be00204c5`.

**Root metadata finding после первого GREEN.** Интегратор обнаружил, что pinned defaults включали PAR и DPoP, хотя C1 их не поддерживает. Расширенный собственный actual discovery test причинно подтвердил оба advertised fields: 3 cases, 2 PASS / 1 FAIL, 721.2253 ms; `request_parameter_supported` уже был false. Root явно отключил `pushedAuthorizationRequests`, `requestObjects`, `dPoP`. Окончательный host13 повтор подтвердил отсутствие PAR endpoint/DPoP algorithms и запрет request-parameter support. Это исправление соблюдает прежний профиль, а не добавляет новые OAuth features. Source finding принадлежит root; независимое воспроизведение — этому review.

## Что именно проверяется

| Граница | Проверка и предел доказательства |
|---|---|
| Origin/Host/HTTPS | Exact configured Host, canonical issuer/resources, удаление произвольных Forwarded/XFH и trusted Express `req.secure` перед выставлением XFP для Provider. Support HTTP проверяет разрешённый loopback proxy и отказ прямому spoof. Правильность производственной trust-proxy сети этим не доказывается. |
| Ключи и клиентские профили | Нет автоматической генерации production keys; private config захвачена копиями и не выдаётся public metadata. Callback — фиксированный путь выбранного client ID с явным IPv4 loopback port, без query/fragment. Client ID не является аттестацией официального бинарника. |
| Ingress | Один bounded buffer, body16KiB, URL8KiB, absolute body deadline10s; 16 active/4 на реальный socket peer, 120 attempts/60s, bounded peer map. Lease живёт до завершения downstream Provider, а не до disconnect. Это не общий deadline выполнения всего OAuth request. Предыдущий отдельный ingress gate не повторялся и не включён в текущие counts. |
| Namespace/ошибки | Disabled endpoints fail closed без SPA; unfinished body закрывается без autodrain. Safe fixed error codes/no-store/Pragma; upstream WARNING о предварительно разобранном body не подавлен и не выдаётся за секретный output. Неизвестная router exception сейчас схлопывается в safe400: это ограничение диагностической точности, не доказанная новая утечка/обход. |
| Authority и completion | Cookie связывает браузер с Interaction и не выдаёт authority. Signed decision идёт через Connect extension с expectedAccountId; completion проверяет durable decidedAccountId и повторное domain binding. Source review controller проверил account generation, late response fences и запрет считать неизвестный ACK отказом/автоповтором. В собственном HTTP test решение синтетическое; actual signed Connect здесь не доказан. |
| Provider context | Request binding берётся из public authenticated `Provider.ctx` только для Code/RT/AT. Grant использует private staged context; Session/Interaction не требуют authenticated client request. `loadExistingGrant` использует только result текущей Interaction, а не remembered Session grant. |
| Сроки | Session TTL — остаток original `iat+600`, не sliding600; Grant/token TTL ограничены staged connection/current Grant expiry. Новые идентификаторы не создают новое original окно. Более ранний отдельный Session test и domain port проверки имеют свои receipts и не входят в этот host count. |

Router пока **не смонтирован в `createHttpApp`**: production namespace остаётся reserved. Подтверждение этой границы — source inspection `server/http-app.js`, а не успешный логин в продукт. Полный encrypted domain flow, bearer, signed consent→tokens, revoke/restore, MCP и реальные CLI остаются следующими gates.

## Исполненный независимый сценарий и отрицательный контроль

Новый тест использует production `attachCapabilitiesOAuth`/wrapper и настоящую HTTP/PKCE/Session/Interaction/Grant/code/token/refresh последовательность Provider9.12.2. Коды и токены получаются через HTTP, не seed. SQLite adapter — явно синтетический `:memory:`; owner decisions — test-only facade. Это не второй production adapter, не AEAD/Connect proof. Endpoint callback проверяется как значение, но никогда не вызывается; модельные API и внешняя сеть не используются.

Три случая: (1) A→B, wrong Origin, invalid XSRF, valid confirmation, B token и сохранённый A refresh, CSP; (2) отдельно включённая fault injection закрытого confirm воспроизводит прежний отказ без ложного B callback; (3) public RP logout/PAR/DPoP не рекламируются, request objects не поддерживаются, GET logout routes закрыты. Весь Session snapshot, включая ID/count/account/state/iat/exp/authorizations, сравнивается канонически после JSON parse. Никакие поля и timestamps не выкинуты. Секреты в assertion output заменены digest; SQL/profile/token values не логируются. После каждого case закрываются listener/DB и только собственный помеченный временный каталог.

Первый fixture draft ошибочно отправлял browser Origin на native token request; pinned `clientBasedCORS:false` корректно отказал. После чтения `shared/cors.js` native token/refresh requests оставлены без Cookie/Origin; browser forms сохраняют Origin. Это исправление fixture, не product bypass.

Следующий post-fix run остановился на raw JSON comparison: `BaseModel` переносит `kind/jti` в конец свойств, `payload.js` сохраняет insertion order, а `shared/session.js` finally пересохраняет Session даже после invalid XSRF. Поэтому raw payload string не является семантическим invariant. Canonical parsed comparison сохраняет все значения; окончательный PASS подтвердил отсутствие изменения любого Session field/ID/count после bad XSRF. Исходный неуспешный лог оставлен как `p4-oauth-host-afterfix-failed.log`, 12 PASS / 1 FAIL / 0 skip, 2454.8431 ms. Product source ради этого assertion не менялся.

## Воспроизводимость

```text
var/toolchains/node-v24.21.0-win-x64/node.exe --test --test-concurrency=1 server/test/capabilities-oauth-account-switch.test.mjs server/test/capabilities-oauth-support-http.test.mjs server/test/capabilities-oauth-profile.test.mjs
```

Все журналы этого review находятся в `output/implementation-20260930/`. Отрицательные результаты не переименовываются в зелёный запуск. Первый `p4-oauth-host-independent-final.log` — 13/13,2381.0704ms до расширенного metadata oracle. Затем `p4-oauth-disabled-features-red.log` сохраняет причинный metadata отказ. Окончательный один serial run **`p4-oauth-host-disabled-features-final.log`** — synthetic account-switch **3/3**, encrypted auxiliary support **5/5**, host profile **5/5**, всего **13/13**,2540.9363ms; log SHA `3895e98fdc560ea2e3a5ea6de716a537abb1114d7916ef693ab2895f21c9f98b`. Ни browser automation, ни domain suites во время этих прогонов не запускались. После завершения тестовый слот освобождён.

Снимок проверенного source SHA-256:

```text
788c0fe3a6e2941ec03ce453c02bd109aa26095b33c5c9588737ce5a36bd0851  server/capabilities-oauth-profile.js
cd8512c1f4aa75345f6e44e4ec6ac6971d81f3e66df5d844415608874018cd17  server/capabilities-oauth-provider.js
0e6700a14445bb58ba0d110ab8b3544099d8c6faaadec1c33bf8bdb1fc4bd961  server/capabilities-oauth.js
24aa0008755b3669ff0d9c73c8ac7a3df06643699bb6bbd010a212e05ffb9d59  server/capabilities-oauth-ingress.js
05ceee4db0abdcf2ea01bbdd645ba96d2ddc7216df787d3d98e4937f5e219829  src/platform/oauth-consent.ts
3386e5d3ececeb6025ec9da571bfb2d1fe207211293c3349330f40e98734a3a5  server/test/capabilities-oauth-account-switch.test.mjs
6c81ec810a3f518cc8ba5c65b894a626d4add9b7fec1199c9310952dbf43110c  server/test/capabilities-oauth-support-http.test.mjs
04fcdbee952b5bb5d16cf1372065868a4bba7c7a106c59f31204d27f5545b2bd  server/test/capabilities-oauth-profile.test.mjs
```

Пути source подтверждают поведение закреплённой версии. Они не обещают его сохранение после library upgrade и не являются заявлением о production rollout/reader capability.
