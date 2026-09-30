# P4-C1 — настоящий OAuth → native Notes через HTTP

2026-10-01. Реализованы пункты1–4 [host composition subplan](p4-oauth-host-composition.md), поверх T2 `e24338a`. [Независимая host проверка](p4-oauth-composition-independent.md) и [независимое чтение actual flow](p4-oauth-native-flow-independent.md) не нашли новых blockers. Это локальный integration checkpoint, не полная приёмка C1/CLI/публичного выпуска.

## Конфигурация и lifecycle

Process entry принимает только явно названные поля: `SOTY_CAPABILITY_AUDIENCE`, `SOTY_NATIVE_NOTES_ENABLED`, `SOTY_OAUTH_ENABLED`, `SOTY_OAUTH_ISSUER`, `SOTY_OAUTH_KEYS_FILE`. Флаги по умолчанию выключены; допустимы пустая строка,0,1. Ключи не генерируются автоматически. Операторский абсолютный regular-file ограничен32KiB по stat и actual read, разбирается строгим UTF-8/JSON; точные поля `jwks`, `cookieKeys`, `artifactKey`, `artifactKeyId`. Artifact key — canonical base64url32bytes. Ошибки не содержат пути, JSON, ключей или исходного cause; полный environment не перечисляется. Значения не включены в эту квитанцию.

Profile проверяется до открытия хранилищ. Enabled AS требует реальных Connect, Notes2/Caps3, fixed native binding, ключей и включённого native execution. Host не повышает schema автоматически. Recovery/cleanup запускаются после admission и останавливаются до закрытия stores. OAuth cleanup выполняет одну общую страницу≤64 за timer turn, с фиксированной паузой/backoff, без ключей и authority; retained original credentials остаются ответственностью domain retention.

Configured AS-off с issuer возвращает Protected Resource Metadata без Provider/cookie/key. Старый выданный bearer может читать/повторять собственный результат при действующей цепочке; issuance/новое исполнение проверяются отдельно. Legacy parser остаётся прежним, отказ `soty_cap_` не переключается на OAuth. После reconcile выполняется новый авторизованный read. Для configured HTTPS RS `req.secure` от доверенной Express boundary обязателен до authentication; произвольные Forwarded headers его не заменяют. Это следует требованию TLS для bearer в [RFC6750 §5.2](https://www.rfc-editor.org/rfc/rfc6750.html#section-5.2).

## Проверенный полный путь

Новый `oauth-native-http.mjs` запускает production `createHttpApp`, настоящие private Connect/Notes2/Caps3 и pinned Provider9.12.2. Bootstrap/approve/deny/edit/revoke идут через действительные HTTP challenges/ECDSA. Code, AT и RT выдаёт Provider через HTTP. Readiness, identity, crypto/storage/auth ports не подменены; browser cookie jar и owner decision driver явно тестовые. Callback проверяется по state/issuer/exact registered path и в этом suite не запрашивается.

**5/5 PASS,0SKIP,8079.1854ms** (`p4-oauth-native-flow-final.log`):

- A→B→A в одной AS browser session, отдельные signed connections; OAuth создаёт одну записку/proof. Чужой аккаунт получает404, новое подключение с занятым ключом409 без второго эффекта.
- После человеческой правки и refresh клиент видит прежнюю receipt, без изменённого содержимого. Поиск raw code/AT/RT в main/WAL отрицателен.
- Wrong resource и PKCE не расходуют code; wrong-resource refresh оставляет исходный RT пригодным. Reuse после rotation отзывает только эту family, sibling работает.
- Keyless AS-off restart сохраняет собственный read/replay; token endpoint и новое исполнение503. Signed owner revoke закрывает и прежний read, число Invocation остаётся1.
- Foreign signed account/wrong completion не подтверждают чужое согласие; явный deny не создаёт connection/credential. Expired Provider resume показывает безопасную статическую ошибку400/no-store.

Первый run был3/4: в новом helper после account-switch XSRF POST отсутствовал ещё один issuer resume. Исправлен только bounded переход в fixture, не authority. После этого добавлена отдельная проверка error document. Первоначальный лог `p4-oauth-native-flow-author.log` сохранён. Прежний resource test причинно воспроизвёл отсутствие HTTPS admission (2PASS/2FAIL), затем узкое `req.secure` исправление прошло. Startup test уточнил отдельный `oauth.available` admission вместо случайного Provider исключения. Эти причины не скрываются успешным итоговым suite.

## Общая приёмка и UI ошибки

Финальный root gate всех module capabilities, всех capabilities HTTP suites и executor policy: **338/338 PASS,0FAIL,0SKIP,69707.4823ms**. Лог `output/implementation-20260930/p4-oauth-c1-integrated.log`, SHA256 `39ba9e3fc9584f3cb7268740f3c3d051c22513d10e92353eda1c4842f40b71c6`. Проверены также cleanup/config6/6 и прежний composition13/13; итог338 включает их окончательные bytes. Typecheck и стандартная prebuild/Vite build PASS. В логах остаётся безопасное upstream предупреждение Provider о заранее разобранном body: bounded parser использует поддерживаемый `req.body`, сообщение не замаскировано.

Новый error document использует общие `modules/ui/palette.mjs` и `hex.mjs`, системную светлую/тёмную схему, без JS и внешних запросов. CSP разрешает только точный style hash, запрещает script/form/frame ancestors; request/error data в HTML не попадают. Root осмотрел реальный документ на изолированном visual endpoint:320×760 — main296px, link48px, SVG38×32.90625 (правильный hex ratio), document320;667×375 — main bottom258, action bottom237/height48, document667. Keyboard Tab попадает на основной переход с видимым focus ring. Actual error400/CSP отдельно проверен через настоящий Provider; visual stand не выдаётся за browser OAuth flow. Системная светлая схема проверена native screenshot; тёмная CSS ветка не объявляется отдельно просмотренной.

Открыты: две OS AS instances с реальными crash barriers, browser signed consent/callback/PWA continuity, закреплённые CLI (без fake MCP), C2/headless, D1 task eval, reader3 Linux/full image/backup-restore/HTTPS. Production не мигрировался и не включал AS. Остальной master P0–P8 не снят с плана. Source pins находятся в двух независимых квитанциях; изменения после этих pins отсутствуют.
