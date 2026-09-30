# C1 — независимый review document policy и возврата после consent

Дата: 2026-10-01. Base HEAD при чтении: `a7ef7b3253ad6df6d28aab1c15e1861c39c07ae2`. Этот reviewer прочитал исходники, diff, новые tests, безопасные результаты локальных опытов и первичные стандарты. **Тесты, браузер, CLI и remote не запускал; production не менял.** Результаты исполнения ниже принадлежат root и автору HTTP-теста. Исходный срез не утверждал сквозную PWA/CLI-приёмку; итоговая атрибуция браузерного C1 добавлена отдельно в конце. Настоящие CLI этим отчётом не проверены.

## Вывод и граница исправления

Два узких override `Referrer-Policy: same-origin` соответствуют native form navigation: consent HTML и Provider `resume` с `status=200`, `type=text/html` и действительным Interaction. Проверки точного Origin, cookie/nonce, подписанного решения, выбранного account и Provider XSRF сохранены. Literal `Origin: null` не стал разрешённым способом завершить запрос.

В [Fetch: append a request Origin header](https://fetch.spec.whatwg.org/#append-a-request-origin-header) политика `no-referrer` обнуляет сериализованный Origin для рассматриваемого не-CORS POST. `same-origin` сохраняет его для адреса того же origin. [Referrer Policy: same-origin](https://w3c.github.io/webappsec-referrer-policy/#referrer-policy-same-origin) также запрещает Referer на другой origin. Это исправляет несовместимость document policy со строгим Origin guard; сам guard ослаблять не требуется.

Проверенный diff не меняет OAuth/domain authority, DDL, токены либо redirect allowlist. CSP по-прежнему расширяет `form-action` только ранее проверенным callback; Provider autoform сохраняет собственный script hash. Same-origin запросы документа могут передавать его полный Referer, включая непрозрачный interaction/resume path. Это осознанная локальная граница; такой path не уходит native callback на другом origin.

Подтверждено сохранение `no-referrer` на покрытых context/token API, completion/confirmation/callback redirects и ранних403. **В первоначальном срезе это не было утверждением обо всех исключительных ошибках:** после уже установленного HTML override отказ `oauthCallbackPolicy` или `sendFile` мог попасть в JSON error writer с `same-origin`. Даже этот край не разрешал opaque Origin и не отправлял cross-origin Referer. Последующее узкое исправление обоих error writers зафиксировано отдельно в дополнении ниже; исходные hashes и результаты сохранены как история.

## Причинное evidence и его авторство

1. Root провёл отдельный native form опыт с Node HTTP sink без OAuth/Connect. Прочитан его исходник `var/p4-oauth-browser/form-origin.mjs`: фиксированный loopback Host, две document policies, POST body≤1024B, сохранение только категории Origin. Файл `p4-oauth-form-origin.json` содержит две записи: `no-referrer → null`, `same-origin → same-origin`. Это результат браузерного опыта root, а не собственного запуска reviewer.
2. Новый [HTTP-тест](../../server/test/capabilities-oauth-browser-policy.test.mjs) использует настоящий `createHttpApp`, signed Connect, Notes2/Caps3, encrypted OAuth ports и pinned Provider9.12.2. Cookies, code, XSRF и bearer остаются в памяти. Browser вычисление Origin здесь не исполняется: тест явно задаёт строку `null` либо точный origin и вручную следует разрешённым HTTP шагам; callback не запрашивает.
3. В первом случае после signed approve literal null получает403; digest точных persisted connections/interactions/artifacts/credentials/grants/invocations и счётчики Notes/proof не меняются. Тот же decision/cookie/account с точным origin выпускает реальный Provider Grant, code и AT/RT. Авторизация сама по себе не создаёт Note.
4. Во втором случае реальный A→B с общей cookie jar доходит до Provider autoform. Его действительный XSRF вместе с literal null получает403 без изменения Session и обеих семей. Точные origin/fields завершают B; refresh A остаётся допустимым. Не подменены consent, storage, readiness либо полномочия.

До root header patch неизменённый тест: **0PASS / 2FAIL / 0SKIP, 1622.7904ms**. Оба последних assertions получили `no-referrer` вместо `same-origin`; предшествующие отрицательные и положительные пути уже прошли. После двух production override: **2PASS / 0FAIL / 0SKIP, 1564.4232ms**. Reviewer прочитал оба лога, не повторял запуск. Фиксированное предупреждение Provider об upstream parsed body присутствует; это не evidence console-clean.

Прежняя гипотеза о синхронном `form.remove()` не объявляется доказанной причиной отказа PWA. Другой изолированный опыт root, `form-navigation.mjs`, использует синтетические формы без OAuth. Его счётчики `old:2, retained:1` относятся к отдельным явным нажатиям: оба способа действительно отправили POST. Эти данные не доказывают автоматический повтор и не показывают, что старое удаление формы блокировало исходный flow.

## Promise/navigation lifecycle

Новый helper держит одну hidden form до `pagehide`, синхронного отказа, наблюдаемого `form-action` policy event либо8s. На исходе удаляет форму, timer и listeners. Маршрут фиксирован, UID проверен; единственное поле — captured `expectedAccountId`. Ошибка содержит только фиксированные code/message, без `blockedURI`, исходного URL или OAuth параметров.

Controller ожидает Promise. Неизвестный исход переводит UI в `stale`, снимает busy и предлагает явный readback; ни approve, ни второй completion POST автоматически не повторяются. Перед отправкой повторно проверяются account, sequence и monotonic deadline. Поздний reject после account A→B→A или dispose не меняет текст/focus и не отправляет ещё один запрос. `pagehide` означает только уход со страницы, **не серверный ACK и не успешный OAuth**.

Root gate для helper/controller: **22/22 PASS, 0SKIP, 774.2403ms**, плюс typecheck и стандартные prebuild/build PASS; Vite завершён за2.92s. Прочитаны тесты и логи. Это VM/controller evidence с синтетическими DOM/таймерами/ports, не native browser доказательство успешного callback.

**Исправление ошибочного вывода reviewer.** В первой версии этой квитанции (`e83edc957f7c98eb8afc5be94a8b6d1a3c06a261e90420cd85a31445dc5d0d61`) ошибочно утверждалось отсутствие обработки BFCache restore. Повторное чтение полного `src/platform/oauth-consent.ts` и его версии в HEAD подтверждает существующий `pageshow` listener: при `event.persisted` вызывается `location.reload()`. Этот код текущим diff не менялся. Поэтому прежний вывод о source-backed восстановлении busy через BFCache **отозван**; production correction для отсутствующего handler не требуется. Фактический native `persisted:true` этим review не проверен. Root отдельно наблюдал Back с reload и JSON error уже использованного запроса — это другой, presentation-only сценарий, не доказательство BFCache defect.

## Зафиксированные байты

SHA256 относятся к прочитанным working-file bytes; новый checkout с другой EOL может иметь другой raw hash.

| Файл | SHA256 |
|---|---|
| `server/capabilities-oauth.js` | `24c1a33f0438a5f3204b7fd2e7749f246f821e002d1acede45cf5bb705b6e271` |
| `server/capabilities-oauth-provider.js` | `0d57d161631aac39d93cc0370a54bc4c7f3266b959ddf007812a74cfbda483de` |
| `server/test/capabilities-oauth-browser-policy.test.mjs` | `73388388fd70c0925c7d2016b5a102e3d63d19c8efddc3b35b1e2810fd674afc` |
| `server/test/support/oauth-native-http.mjs` | `027f656f4c228f8da4f9c98da660b21786e701dc1bfd861530cdb34119d187d6` |
| `src/platform/oauth-consent.ts` | `c5151ba2d3b4ea1ed756ad10ee9e5ddab53cdbb364d5935d3dd1e3ffbb8714fa` |
| `src/platform/oauth-navigation.ts` | `7c2e8a6f7d27d5cff44e4fd7c20c53f996c5d80e676d055fab58248a72b135d3` |
| `src/platform/oauth-navigation.test.mjs` | `80b54c25c97138b855e43196d32f80b55f4dccb4336c54cc815ee5049a7fc768` |
| `src/world/oauth-consent.ts` | `f384fa45e4d1b4424727c132193970b180444a86d1a697f4dc34bcb9645d868d` |
| `src/world/oauth-consent.acceptance.test.mjs` | `b15ac280c195a14b734f83277778d76fc0d98bf0c100c19352622851adcc1e53` |

Evidence ниже находится в `output/implementation-20260930/`, кроме двух явно указанных source controls.

| Файл | SHA256 |
|---|---|
| `var/p4-oauth-browser/form-origin.mjs` | `67dcd174d6aacce41386d17b09f9255fd236ef6e41fda42957a9d7dc79cb7bc4` |
| `var/p4-oauth-browser/form-navigation.mjs` | `76b3c5f0940ac456059eb8306a4dad8465a0c508bf913d0395b06f38cda5eed7` |
| `p4-oauth-form-origin.json` | `5fbb0eec91c6e80d46659f2df559daeb7958e0a733d1bc275c8ef6985704ab9a` |
| `p4-oauth-form-navigation-counts.json` | `bd6782e79bf1daef02946401bebe377e0f90ea8ab6820652d6c9ce8f9b44f046` |
| `p4-oauth-browser-policy-red.log` | `e79908d5a7a73e9f8d7ddf0fa1e2c1e39c7b4ab0f686c67df7f431462b5f65ef` |
| `p4-oauth-browser-policy-green.log` | `ba3a054f5eff92f4fa793a8fb761c4996689bc52c0906a57ac513ef4844b74ef` |
| `p4-oauth-navigation-policy-tests.log` | `ec5553ace495e4413eb9428adb7ff18977a3991d7e1c5e5d295d2227832c2af9` |
| `p4-oauth-navigation-policy-types.log` | `e64de9c6342eedff10d6229b2ff28b2124b78b6d170f5ed85f39a20bd51ab856` |
| `p4-oauth-navigation-policy-build.log` | `c9630322eea2e36b4b8aaac40526248bf6cdb21ac39fb8c2483aaff301b14457` |

## Отдельное заключение по C2 plan

Полностью прочитан [C2 подплан](p4-mcp-implementation-plan.md), финальный SHA256 `6df0b4a3f315298f6dd4091fd18c8e77de3aa3e8fc03d07d16bc58142c06127b`. Один stateless endpoint, два явных wire profiles, четыре tools, существующие authority/ledger и отдельный one-shot child HTTP — приемлемый конечный scope. Child plaintext после lost response не восстанавливается и не заменяется автоматической новой выдачей; каскад grant/principal отличается от point credential revoke. SDK/CLI transport, D1 model и D2 release gates остаются отдельными.

Независимо сверены public SDK2.2.0 APIs, [Node adapter source](https://raw.githubusercontent.com/modelcontextprotocol/typescript-sdk/v2.2.0/packages/middleware/node/src/toNodeHandler.ts), выбранные safe npm fields существующего `@modelcontextprotocol/node@2.1.0` и pinned [OpenCode text-result path](https://raw.githubusercontent.com/anomalyco/opencode/v1.18.15/packages/opencode/src/mcp/catalog.ts). Ничего не устанавливалось. Внесены три замечания этого review:

- Legacy `handler.fetch` возвращает ещё читаемый SSE body. План теперь требует полностью bounded-read его bytes, затем fresh current authority и только после этого первый private wire byte. Прямой streaming `toNodeHandler` этот порядок не обеспечивает; codec остаётся SDK.
- SDK `handler.close()` не закрывает legacy in-flight. План теперь явно удерживает host AbortController/reader в bounded active set и освобождает их на deadline, client close и shutdown. Это будущий executable lifecycle gate, не уже проверенное отсутствие утечек.
- [Client SDK1.29 `callTool`](https://raw.githubusercontent.com/modelcontextprotocol/typescript-sdk/v1.29.0/src/client/index.ts) валидирует любое present structuredContent по успешной outputSchema даже при `isError:true`. Вместо ошибочного structured error план теперь оставляет один safe JSON TextContent без structuredContent; успешные ответы сохраняют одинаковые text/structured envelopes. Это предотвращает подмену полезного domain code ошибкой client-side schema validation.

После этих точных doc corrections блокера подплана не найдено. Это source/design verdict: не выполненный MCP wire test, не OAuth login двух CLI, не model tool-use и не обещание совместимости любых агентов.

## Дополнение — повторное открытие использованного consent

После предыдущего review root добавил presentation-only обработку ошибки документа. Этот reviewer прочитал точный diff, appended causal case и новые логи; tests/browser/remote по-прежнему не запускал.

`consentDocument` изначально `false`; он становится `true` только внутри совпавшего interaction route без suffix, после ограничений target, Host/TLS/Origin, отсутствия query/encoding, проверки GET и отсутствия request body. Значение устанавливается перед `provider.interactionDetails`, поэтому уже использованный документ тоже получает понятную страницу. `/context` и `/complete` имеют непустой action и сохраняют прежние JSON/protocol error paths. Ранние method/path/transport/ingress отказы не объявляются HTML документами. Нет выбора response mode из произвольного Accept или клиентского поля.

`safeFailure` сохраняет исходный status и Retry-After/connection handling, возвращает fixed `OAUTH_FAILURE_DOCUMENT` только при этом флаге, оставляет no-store и устанавливает standalone CSP. Политика запрещает scripts/forms и допускает только stylesheet с точным hash; динамические error text/UID/cookie/redirect не интерполируются. Wrapper и Provider catch теперь явно возвращают `no-referrer`, если headers ещё не отправлены. Эти два reset закрывают ранее оговорённый исключительный край, не меняя Origin admission.

Нейтральная copy — «Запрос подключения недоступен» / «Проверьте подключение в клиенте. При необходимости начните новый запрос.» — описывает недоступность страницы, а не отрицает уже успешное подключение. Единственная ссылка ведёт на `/`; автоматического redirect, POST, повторного approve или нового токена нет. Общий Provider error document получает ту же нейтральную copy. В прежнем expired-resume test изменён только ожидаемый заголовок, его status/privacy/CSP assertions сохранены.

Новый HTTP case действительно завершает signed consent и обменивает выданный code. Затем два GET исходного документа и GET context проверяют неизменный digest точных durable rows, прежний статус отказа, отсутствие Location, JSON context, отсутствие private reflection и Notes/proof. Последний assertion первоначального RED проверял именно HTML Content-Type. После исправления проверяются также fixed document bytes, отсутствие script/form/input и соответствие stylesheet CSP. Первые9429 bytes двух прежних cases независимо сверены: SHA256 по-прежнему `73388388fd70c0925c7d2016b5a102e3d63d19c8efddc3b35b1e2810fd674afc`.

Авторские результаты, прочитанные из логов: новый case **0PASS/1FAIL, 4286.6845ms** до patch; весь policy файл после patch **3/3 PASS, 0SKIP, 2448.0821ms**. Отдельный affected Provider expired-resume case — **1/1 PASS, 0SKIP, 1016.0563ms**. Это два последовательных GREEN запуска, не один полный OAuth regression. Новый used-document случай не выдаётся за реальный browser Back/BFCache, edit/replay/revoke либо новое CLI evidence. Root отдельно владеет actual PWA/callback/Note201 evidence; оно не подменяется этими HTTP-проверками.

Блокера этой узкой дельты не найдено. Текущие pins ниже заменяют соответствующие исходные строки только для данного дополнения; UI/C2 pins выше не менялись.

| Файл | SHA256 |
|---|---|
| `server/capabilities-oauth.js` | `a9ad1b1a70a5762c8038d2c765e7d831bb47886e9bb2a74f8f46356a08850bd7` |
| `server/capabilities-oauth-provider.js` | `7d273aa714babbc1603a15a56a0d7cd1dd14fe57d96427397f3a9abe67f0c031` |
| `server/capabilities-oauth-document.js` | `70f222f85d49816de4f953ab013d0ec1279b41564b20806ac05b0b2bdd3fe430` |
| `server/test/capabilities-oauth-browser-policy.test.mjs` | `e5d5779f847377bbb949c388646d56d14f3d8b239af2c4247c7645ed8347fc39` |
| `server/test/capabilities-oauth-native-flow.test.mjs` | `055b169f0f8eac0a4451aedd3fb7e90d15df4f938814f907be4abfadfa8f89c4` |
| `output/implementation-20260930/p4-oauth-consumed-document-red.log` | `e0b540632e3613e4f5f1c034b27e80e2a5e393effe3cf84b9fe5dbf3bb5cdcac` |
| `output/implementation-20260930/p4-oauth-consumed-document-green.log` | `c0bc08088b187705de69cabe004195f9a6154042ede36b1e6a313359069975a4` |
| `output/implementation-20260930/p4-oauth-expired-resume-targeted.log` | `170b7489da4aad9d7ac96643575c1c4bb3afe0c9a62afb45e20d704b7f8d440b` |

## Финальная атрибуция браузерного C1

Прочитаны [root browser receipt](p4-oauth-browser-flow.md), новая запись [PROGRESS](PROGRESS.md) и точный safe `completed-snapshot.json` run `f951cfb90e480b7fa119c79d652b25b0` (host5493/client5494). Их сведения согласуются. Это независимая сверка предоставленного evidence, не повтор браузерных действий или SQL.

Root зафиксировал approve→callback/native201, сохранённую человеческую правку, refresh200 и exact replay200/reused=true с прежними Invocation/Note и historical revision1; reload сохранил правку. Затем mobile owner revoke точного подключения дал последующий401, а owner Note осталась отредактированной. Native Back показал нейтральный HTML. Эти последовательные результаты и UI-наблюдения атрибутируются root, не выводятся лишь из итоговых счётчиков и не присваиваются reviewer.

Сам прочитанный snapshot подтверждает accounts1, Invocation1, Note1/proof1/revision2, `editedBodyMatches:true`, budget20/spent1/reserved0 и одну revoked connection. Client показывает1 start/1 callback/1 exchange/1 refresh/3 native attempts; callback Referer отсутствовал. Completion POST учтён с exact Origin. В artifact нет Note body; `crossStoreAtomicSnapshot:false` явно сохраняет границу отдельных RO transactions. Прерванный предыдущий run не смешан с финальным.

Узкая C1 acceptance attribution принята без расхождений. Native `persisted:true` BFCache, настоящие Codex/OpenCode executables, MCP/C2, модельный D1, production HTTPS/restore и весь master этим gate не объявляются завершёнными.

| Прочитанный artifact | SHA256 |
|---|---|
| `docs/implementation/p4-oauth-browser-flow.md` | `bc13a3ac8b8b77967000aa80ebdc1e6c644ef63a9e2f78eec4c7b663e4b3b67f` |
| `docs/implementation/PROGRESS.md` (снимок при чтении) | `7d5416d32ad3e2f83f7eb5f4f3aed49f06d13b59d407ce06b4f9eddd17bbebb8` |
| `var/p4-oauth-pwa-final/runs/f951cfb90e480b7fa119c79d652b25b0/completed-snapshot.json` | `9b4231e8284252a199a506ddaf416ed1e4e379d91ffb93c83c5b937521c61d01` |
