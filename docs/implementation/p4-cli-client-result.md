# P4-C2c — настоящие Codex/OpenCode: результат root

Дата: 2026-10-01. База `2103d755c081f03cdad98959fe593c6151ea2eec` уже committed/pushed; remote SHA проверен. Это transport/tool integration с настоящими executable. **D1 с настоящими моделями, D2 и весь master этим документом не закрываются.**

## Доказанный срез 5521

Run `e704dc1eb48ca5635500d444b4f1fb7e`, actual AS/PWA/MCP `http://127.0.0.1:5521`, host PID15040. Аккаунт создан обычной PWA, обе connection выданы через signed owner consent; seeded approval/token injection отсутствовали. Callback listener каждого CLI принадлежал его настоящему login child до разрешения. Returned issuer совпал, реальный code exchange завершился200.

| Клиент | Binary SHA256 | Actual revision | Результат |
| --- | --- | --- | --- |
| Codex0.153.4 | `444a3f0008050605cae73cd9b7a2dcac61294062dfaab56dd20430fd6498518b` | offered/negotiated `2025-06-18` | **PARTIAL**: discovery/create/get/replay/get PASS; revoked_create FAIL; completedfalse |
| OpenCode1.18.15 | `945593162d8f67ba4e901c8e73cd7a22b613fe7159b34a7ae10a1d318b1c6240` | offered/negotiated `2025-11-25` | **COMPLETE**: все семь stages PASS; completedtrue |

Codex использует поддерживаемый app-server `mcpServer/tool/call` без `turn/start`. OpenCode использует persistent `--pure serve` и настоящий session tool loop с ограниченным локальным provider fixture. Fixture передаёт выбор инструментов, а OAuth/MCP выполняет executable; это не доказательство самостоятельного решения настоящей модели. Внешние модели не вызывались, no-model requests0.

OpenCode: Note `n_4e47c3f7da4db4f4f6ba7f6e646e8bcd1a39953aecf6f2584e0f216980359a9f`, Invocation `inv_dafa556a-8769-447a-833d-bfbe3b5eba0c`, connection `oauth_216816d9-cb4d-4f65-8362-150071c65c61`. Exact replay wire34→38 сохранил input digest и оба IDs. Get36/40 не возвращает тело записки. После owner revoke через обычную PWA320 реальные POST tools/call42/action14 и44/action15 получили401. Человек отредактировал Note, дождался сохранения и проверил reload до/после replay и revoke: его правка сохранилась.

Codex: Note `n_ea09d5cb7c8c4a8497a96773c58c32a924a674d26b6ee8ab49269d9e2a77164f`, Invocation `inv_1354f09b-61a5-4905-98f9-d328edbbeabd`, connection `oauth_564cff4b-af6f-4dd0-ae01-d4f51b1f117d`. Replay13→15 сохранил digest/IDs; get14/16 не содержит тела. После отзыва увидены GET405 и token POST400, RPC−32603, но **нет нового MCP POST401/403**. Старый observer не сохранял grant_type/error/action для token endpoint; конкретный `invalid_grant` этим run не доказан. Правка человека сохранилась и после этой неудавшейся проверки. Никакого ретроактивного PASS.

С source понятно, почему длительная ручная проверка может уйти через refresh до MCP: access token TTL300s, Codex refresh skew30s, `call_tool()` обновляет доступ прежде отправки tools/call. Это объяснение реализации, а не подмена отсутствующего wire evidence. Для следующего свежего run был подготовлен отдельный bounded passive AS denial oracle; старые данные сохранены.

## Immutable evidence и независимый аудит

`var/p4-cli-clients/e704dc1eb48ca5635500d444b4f1fb7e/result-receipt.json`, SHA256 **`af25369251a6b46add02e15e15491ef026cebbc2bd6e1615721f3bdb9bcb8716`**. Все16 исходников скопированы в `frozen-source` по load-time owner pins, dist50/50 совпал. Host остановлен командой `stop` в собственном TTY session32829, actual exit0;5521 listener и все CLI/probe children закрыты. Keys не сохранялись стендом; close ждёт client/service cleanup и обнуляет artifact key.

Холодные read-only stores подтверждают **на каждого клиента**: Note active revision2, immutable native proof revision1, один succeeded/committed Invocation с соответствующими store/account bindings, input purged, receipt1, spent1/reserved0. Глобально Notes/proofs/Invocations/spent по2. Обе connection/root grant/credential отозваны; active principal сам по себе не означает действующий доступ. Новых эффектов после отзыва нет. Это отдельные согласованные чтения stores, не обещание cross-store atomic transaction.

Независимый critic прочитал receipt/hash/embedded status, frozen16/16 и dist50/50, повторил выбранные RO SELECT и подтвердил именно PARTIAL/COMPLETE. Browser edit/reload evidence принадлежит root. Actual viewport320/documentWidth320, горизонтального overflow нет; screenshot `output/implementation-20260930/p4-cli-opencode-mobile-revoke.jpg`. Физический телефон этим не проверен.

## Узкое production исправление

[Причинная совместимость Codex](p4-mcp-codex-compat.md): явная legacy revision `2025-06-18` добавлена в существующий maintained SDK transport. Нет собственного codec, понижения authority/privacy/лимитов или client wire rewrite. Missing header, unknown revision03 и modern-header mismatch остаются negative gates.

Root отдельный focused test2/2 PASS0skip и unchanged affected MCP/ingress/independent24/24 PASS0skip; это две отдельные квитанции, не новые UI/model/production результаты.

## Завершённый Codex 5523 и итог C2c

Свежий отдельный run `5e9d04a7999c7e00d7ea3945d719262c`, origin5523, host49916. Настоящий Codex0.153.4 прошёл **discovery→create→get→replay→get→revoked_create→revoked_get, семь PASS, completedtrue**. Новый owner `acct_5IBXB4Ci6QVXiphObuaeo139g5mK5OQ1` создан PWA; login child37124 владел callback56897 до Allow. Авторизация проверила S256/cardinalities/resource, returned issuer и token200. Offered/negotiated revisionJune2025 подтверждена actual wire4/8.

Note `n_33e7ca7e08d5a217bfb3e578abb9773d690b6662b6ce8ab959ac322794c28ed3`, Invocation `inv_a629366c-866f-4963-b763-21fa692dbc45`, connection `oauth_0a36bd00-14e0-4718-8022-92ca7ecbddb4`. Exact replay14→16 совпал по input digest и обоим IDs; get15/17 не содержит private body. Owner изменил Note через её обычный editor, дождался сохранения, проверил reload; exact replay сохранил правку. После отключения connection через PWA320 **два новых POST tools/call18/action8 и20/action9 получили401**. Оба запроса отдельно сопровождались настоящими AS refresh19/21 `invalid_grant` с exact client/resource/current-action markers. Root засчитал именно MCP denial; AS evidence выделен отдельно. Никакой выдачи токена/эффекта после отзыва. Правка сохранилась после обоих отказов.

Immutable `var/p4-cli-clients/5e9d04a7999c7e00d7ea3945d719262c/result-receipt.json`, SHA256 **`a872c63e11a94f60790055018d26e2fa35f1ee8cfc29d9cf56ebe67bf7b6e44a`**: frozen16 load-time sources плюс observer test, dist50/50. Холодные RO stores подтверждают account/connection/Invocation/Note/proof/receipt по1, succeeded/committed, input purged, Note active revision2/proof1 и верные account/store bindings, spent1/reserved0. Connection/root grant/credential revoked. Codex channel exit0/cleanuptrue, ownedChildren0; host остановлен собственным TTY44774 командой stop, exit0, listener5523 отсутствует. Browser evidence — root, actual320 без overflow; `output/implementation-20260930/p4-cli-codex-mobile-revoke.jpg`. Физический телефон и cross-store atomicity не заявлены.

Новый passive observer получил meaningful actual HTTP RED6/1: прямой Node writeHead не заполняет getHeader cache. Исправлена только выбранная классификация Content-Type через own data descriptors; original args/this/return/байты сохранены, наблюдатель не читает getters/значения остальных headers. Первый log сохранён. Итоговый root7/7 PASS0skip128.3559ms, `p4-oauth-denial-observer-root-final.log`; source/test pins `1223f6ffd659b489ccd7d957d0419343fb273c29a5b17a533314cdfad9277002` / `44be97e33cdae9111981c6bda0aaebbbd400908752c325329f643852c256a79b`. Generic400, malformed/oversized/aborted/late-action свидетельства не засчитываются. Этот observer — ignored QA harness, не новый production endpoint или parser.

Независимый critic повторно подтвердил обе неизменённые квитанции, frozen sources/dist, холодные выбранные RO запросы, replay/privacy и отдельные отрицательные wire события. Production diff385e105d и два прежних supported-list литерала проверены, unknown03/header/error/requested negatives сохранены; отдельные root2/2 и24/24 logs прочитаны. **C2c принят как OpenCode COMPLETE из5521 плюс Codex COMPLETE из5523**. Старый Codex5521 остаётся PARTIAL и не смешивается с новым owner/профилем.

Далее commit/push этого локального checkpoint, D1 с фиксированными24 RU/EN задачами на каждый настоящий клиент, D2 внешние HTTPS/полный cold restore/release и P5–P8. C2c не означает модельную или production готовность.
