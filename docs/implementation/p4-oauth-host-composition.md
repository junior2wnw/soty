# P4-C1 — полная host composition

01.10.2026. Следующий подплан после owner/Grant `e8edc24` и host/UI checkpoints. Предполагаемые методы перечислены в [storage contract](p4-oauth-storage-contract.md); T1/T2 ещё разрабатываются. Этот файл не является отчётом о завершённом OAuth.

## Последовательность

1. Подготовить закрытую runtime configuration. Default-off; независимые explicit native Notes и AS flags. Issuer/resources соответствуют доверенному shell origin. Ключи не генерируются при старте, не попадают в статус, URL или ошибки. Для process entry — bounded JSON key file, переданный оператором, с safe fixed error. Проверка флагов/конфигурации до открытия stores. AS disabled с сохранённым issuer может работать keyless для owner/bearer policy; отсутствие любой AS composition сохраняет legacy safety revoke.
2. Добавить optional `oauth` в `createHttpApp`: profile до storage; domain composition использует тот же existing Connect fence. Не включать implicit schema migration. Provider монтируется после реального Connect service, до reserve/static SPA; disabled mode остаётся no-store503. Startup failure закрывает уже созданные handles. Ограниченный cleanup живёт внутри host, завершается при shutdown и никогда не удаляет durable connection/history pins.
3. HTTP resource boundary принимает прежний service credential или новый OAuth bearer через отдельный закрытый resolver. Parser legacy не расширяется. Audience остаётся точным HTTP resource, `/mcp` token не даёт HTTP Notes. При401 корректный protected-resource challenge, без cookie/query token и без fallback после неуспешной аутентификации. Новый branded actor проходит прежний native Notes admission/replay/current read.
4. Authoritative integration использует настоящие Connect, Capabilities3, Notes2, pinned Provider и production host. Две разные signed identity; PKCE/code/RT/AT получаются реальным HTTP, без test token seed и без synthetic permission adapter. Проверки: account switch, отдельные connections одного профиля, same-connection replay, refresh, wrong resource до consume, family revoke, sibling continuity, current read против original expiry и crossconnection occupied key. Никакой production data/network/model billing в fixture.
5. Настоящий browser→signed decision→native callback; общий owner connections/revoke UI. Затем две OS AS instances для общей SQLite/race matrix и два закреплённых CLI. Если CLI требует рабочий MCP initialize, его gate остаётся в C2, без fake transport.

## Новое владение root

`server/capabilities-configuration.js`, его tests; `server/index.js`, `server/http-app.js`, `server/capabilities-actions.js`; новый authoritative integration test/support. Изменение server error page при необходимости использует ту же UI систему; не копирует вторую палитру. Domain T1/T2 и AccessPanel имеют других авторов и не редактируются root одновременно.

## Неизменяемые границы

- Участие host в этом плане не разрешает production schema3 до reader3 Linux/full image/backup/restore gates.
- Один Connect identity и прежние Notes/Invocation/budget; очередного effect ledger нет.
- Человеческий consent явный; внешний static client ID не доказывает разработчика.
- Secret file — runtime input, не репозиторный артефакт. Резервные копии только encrypted.
- Выпуск master P0–P8 не объявляется готовым после C1; C2/D, P3/P5–P7 и условный P8 сохраняются.
