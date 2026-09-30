# P4-C2b — HTTP и общая композиция service delegation

Статус: **C2b принят локально: domain, HTTP, owner UI и actual PWA/headless**. Предыдущий MCP checkpoint `14f419aa12a8151557237f7de6a7bc4ab66e16af` committed/pushed с verified remote. Production и original checkout не меняются. Полный [master P0–P8](../plans/soty-human-agent-platform-20260930.md) сохраняется.

## Подплан и разделение

1. Domain author: закрытый WeakMap resolver, настоящий Connect→Caps fence, leaf scope/TTL/общий budget, quota preflight и service audit; отдельные rollback/two-writer tests. Никаких DDL или второго ledger.
2. Root: один exact HTTP endpoint, strict flat scalar parser, прежний bounded ingress pool, selected DTO/one-shot token и composition; affected HTTP/OpenAPI gates.
3. UI author: явный default-off delegation для manual grant, lineage/shared limit/revoke, честная service/device история, account/keyboard/mobile tests.
4. Critic: независимые raw wire/current-authority/lost-response и HTTPS AS-off cases, read-only source review. После source freeze — собственный serial slot.
5. Root: реальный owner PWA/headless walkthrough, typecheck/build; устранить causal findings, commit/push. Только затем C2c actual CLI; D1/model и D2/HTTPS/restore не подменяются этим checkpoint.

## HTTP seam

`POST H/api/capabilities/v1/grants/derive`, строго `{label,expiresAt}`; raw≤16KiB, fatal UTF-8, decoded duplicate keys/nested/extra fields запрещены. Trusted parser hook в существующем ingress меняет только фиксированный parser/меньший cap; общий process pool, actual socket peer, attempts/deadline остаются прежними. Native three-string parser сохраняет прежний контракт. Срок — safe integer milliseconds; домен проверяет future/credential/ancestor/max TTL.

Только current ordinary `soty_cap_` bearer для H. OAuth и H/mcp не делегируют. Host/Origin/метод/path/query проверяются до выдачи; HTTPS зависит от canonical audience, а не наличия AS/PRM. Independent critic обнаружил прежнюю связку TLS с PRM: root исправил её также в affected native HTTP. Независимый actual HTTP проверяет spoof forwarded header→403 и явно trusted Express proxy→201.

Read body не держит lock. Domain derive повторно разрешает реальный service actor под fence. Перед selected DTO+token response — fresh authentication без следующего await. Никакого native readiness guard у выдачи: выключенное исполнение не маскируется доступностью ключа. Response201/no-store содержит только существующие public principal/grant/credential поля и однократный token; digest/storage/internal lineage input туда не попадают.

Lost/error response после COMMIT может означать уже существующего child. **Нет automatic retry, replacement issuance или восстановимого plaintext**. Derive не выдаёт Retry-After; owner проверяет списки/service audit и отзывает точные IDs. Каскад относится к grant/principal/creator; отзыв только issuer credential не отключает уже подключённых помощников. HTTP errors не утверждают отсутствие результата.

## Приёмка

Проверки выполнены последовательно, с отдельным авторским/независимым слотом. Это отдельные gates, а не сумма одного общего прогона. Logs находятся в `output/implementation-20260930/`.

| Gate | Результат | Log / квитанция |
|---|---|---|
| Domain author: unit + real Connect/native/two writers/kill | 18/18 PASS,0skip;5173.437ms | `p4-delegation-author-final.log`; [domain](p4-service-delegation.md) |
| Independent HTTP wire/current authority/lost response/HTTPS | 4/4 PASS,0skip;2132.4424ms | `p4-delegation-independent-first.log`; [independent](p4-service-delegation-independent.md) |
| Root actual HTTP/strict parser/OpenAPI | 4/4 PASS,0skip;1890.5792ms | `p4-delegation-http-root-first.log` |
| Root affected13files: native, OAuth, ingress, MCP, OpenAPI | 82/82 PASS,0skip;23675.2206ms | `p4-delegation-affected-root.log` |
| UI focused + unchanged OAuth/identity | 8/8 + 13/13 PASS,0skip | `p4-service-delegation-ui-first.log`, `p4-service-delegation-ui-affected.log`; [UI](p4-service-delegation-ui.md) |
| Root types / ordinary prebuild+Vite build | exit0; Vite2.42s | `p4-delegation-types-root.log`, `p4-delegation-build-root.log` |

Первый native author run обнаружил неверный test oracle `not_found`: реальный контракт — `invocation_not_found`; исправлена только assertion. До первого root HTTP run critic нашёл ошибочную тестовую owner operation `invocations.get`; root заменил её существующей `access.invocations.list`, не добавляя API. Эти исправления не маскируются отдельными production features.

## Root browser: synthetic component и actual signed PWA

На5515 проверен настоящий AccessPanel/native DOM/CSS с **синтетическим API**, без Connect/DB и настоящего credential. На320×760 в графитовой теме и667×375 в светлой: checkbox default-off, Space/Tab/disclosure/focus, длинные lineage IDs, отсутствие горизонтального overflow. Lost-ACK mode делает одну выдачу, блокирует повтор, показывает точный cleanup; keyboard cleanup доступен внутри viewport. Это component evidence, не signed flow. Скриншоты просмотрены inline; отдельных PNG artifacts не создано.

Отдельный изолированный stand5513 использует actual `createHttpApp`, Connect, Notes2, Capabilities3 и текущую build; AS выключен. Аккаунт и parent principal/grant/key созданы **через настоящую PWA**, без seeded identity/decision/credential. Owner включает checkbox помощников и root budget3. Отдельный loopback client получает child через actual HTTP201 и создаёт private Note201. Owner открывает записку в PWA, редактирует текст и проверяет autosave/reload. Exact child/key/input replay возвращает200, тот же Invocation и reused. На320 owner отзывает parent grant; последующий child replay получает403. Повторное открытие сохраняет правку человека.

Completed immutable RO snapshot: `var/p4-delegation-pwa/b80f794643446e5e15637922f24d704d/completed-snapshot.json`, SHA256 `a55cdf5daaac7a16f490aadeae1315369193f3e7752ce5e65d4c14be0b2c033f`. Один account; два clients/principals/grants/credentials; одна root budget с limit3,spent1,reserved0; один Invocation,Note,proof; Note revision2, editedBodyMatches=true; один service audit. HTTP derivePosts1,NotesPosts3: создание, exact replay, отказ после отзыва. Terminal input удалён. Parent revoked; собственный child revoked_at остаётся null, но цепочка закрывает действие.

Actual Note ID `n_74799ca03cbc7e76548a635eec831e60555558dfa62c68220a51281d5e23e838`; Invocation `inv_1cd11ff0-ddcb-405f-8896-d0ccd59fef3f`; parent grant `grant_3af07060-c0ae-4175-8896-b1c6a681adee`; child grant `grant_e120d500-caed-4619-b395-f9f576e30184`. Снимок содержит выбранные безопасные metadata/counts/digests, без tokens или Note body. Per-store RO transactions **не объявляются атомарным снимком трёх DB**. Independent critic read-only подтвердил snapshot SHA и все9 source/dist pins, подтвердил logs; root browser не приписан независимому reviewer.

Финальная actual PWA в графитовой теме: body rgb(16,17,18), отсутствие горизонтального overflow на1280 и320, editedBodyPreserved=true после reload. Native viewport overrides сняты. AS-off сообщение недоступности OAuth details сохранено как честное состояние; manual/child rows и revoke работают. Это не actual OAuth/CLI/model acceptance.

## Следующий шаг

C2b source и локальная приёмка завершены; фиксация commit/push — отдельная Git операция. Далее **C2c настоящие Codex/OpenCode**, затем D1 model matrix и D2 HTTPS/restore/release. Нового каталога агентов, панели, ledger или схемы хранения не добавлено. Master P5–P8 и release gates сохраняются.
