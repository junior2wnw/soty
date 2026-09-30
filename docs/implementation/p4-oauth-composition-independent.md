# P4 — независимый review host composition OAuth

01.10.2026. Read-only review текущего root среза: новых блокирующих дефектов в просмотренном diff не найдено. Это принятие границ host composition, **не доказательство полного OAuth AccessToken → Notes flow**, выпуска image или production readiness. В рамках review production, UI, данные и настройки не менялись; собственные тесты, браузер, Docker и remote actions не запускались.

Прочитан [план composition](p4-oauth-host-composition.md), новые configuration/resource/composition tests, их actual HTTP fixture, процессный entrypoint и связанные host profile/provider/ingress/native recovery границы. Scheduler `capabilities-oauth-cleanup.js` появился во время review и также прочитан вместе с его тестом. Замороженная AccessPanel этим review не изменена.

## Проверенные границы

- `capabilities-configuration.js:16–65`: абсолютный operator path; только regular file, `O_NONBLOCK`, отдельные пределы stat/read 32 KiB с дополнительным byte для overflow; fatal UTF-8, точный набор JSON fields и canonical 32-byte artifact key. Любая ошибка заменяется фиксированным `capability_configuration_invalid`, без исходного path, содержимого или cause. Raw read buffer обнуляется. Это ограничение bytes, не обещание общего RSS, удаления всех JS/string copies из памяти или deadline файловой системы.
- `index.js:18` и `http-app.js:26–30`: private configuration/profile проверяются до открытия stores. По умолчанию flags выключены; ключи не генерируются. Environment не перечисляется и не копируется в публичный status/log.
- `capabilities-oauth.js:64–105`: заданный keyless AS-off профиль отдаёт только точный public resource metadata. Provider, browser binding и session cookie в этом режиме не создаются; остальные OAuth endpoints отвечают unavailable. Отсутствующий профиль сохраняет прежнее закрытие namespace. Объявленная resource metadata не является доказательством готовности выдачи token.
- `capabilities-actions.js:119–144`: точные Host/Origin, а для настроенного HTTPS resource — доверенный Express `req.secure`, не произвольные Forwarded headers. Legacy `soty_cap_` и OAuth resolver выбираются раздельно; отказ одного не запускает второй. OAuth wrapper также проверяет транспорт до передачи запроса Provider и задаёт его forwarding context сам.
- `capabilities-actions.js:151,165`: после асинхронного чтения body выполняется новое fenced admission; после actorless reconciliation/execution — новый авторизованный read. Брендированный actor не заменяет current credential/grant checks в `access.mjs` и `native-notes.mjs`. AS key не добавлен как prerequisite чтения уже созданного legacy результата.
- `http-app.js:150–167`: Connect и readiness/provider admission предшествуют созданию recovery/cleanup timers. При отказе startup закрываются уже созданные сервисы и connector persistence; таймеры и subscription снимаются до DB close. Async connector close инициируется sync factory без утверждения, что все OS handles уже освобождены в момент её throw. `index.js` normal shutdown ожидает `closeServices()`.
- Новый cleanup scheduler берёт одну общую страницу `limit:64` за turn, не крутится немедленно по полным страницам, делает `unref`, отклоняет Promise/malformed counts, использует фиксированный backoff и ограждает поздний tick после `close`. Он не читает bearer/key и не выдаёт authority. Прочитанный timer fixture — synthetic clock, не длительный production soak.

## Доказательства и открытые границы

Root сообщил свой gate **13/13 PASS** (`p4-oauth-host-composition-green`). Reviewer прочитал соответствующие tests, но не выдаёт этот результат за собственный повтор. Configuration cases используют только synthetic keys/files. Composition fixture создаёт реальные private SQLite stores, HTTP listener и подписанные Connect requests для legacy credential; проверяет AS-off metadata, read/replay после запрета нового исполнения, safety revoke и отказ startup на несовместимом/выключенном native состоянии без повышения Notes/Capabilities версии.

Resource tests используют настоящую HTTP границу и **synthetic authentication ports** для выбора resolver/TLS/no-fallback. Они не подтверждают реальные OAuth token, issuer/resource binding или доменный OAuth actor. В прочитанном доменном срезе `oauth-connections.mjs` ещё явно возвращал `readiness.available:false`, а `authenticateBearer()` — `oauth_unavailable`; это безопасная незавершённая доменная стадия, которую будущий frozen integration обязан проверить отдельно. Позднейшие изменения domain автора этим receipt не принимаются автоматически.

Startup tests не являются побайтным доказательством неизменности всех stores, проверкой любой ошибки OS close либо восстановлением после process crash. Реальная HTTPS edge/proxy deployment, полный authorization-code/refresh/access-token → native-effect flow, original-credential authority, последующий image/restore и production gates остаются отдельными. Проверок через подменённый auth adapter за такой полный flow здесь нет.

## Exact просмотренный срез

SHA256 ниже относятся к **worktree bytes**, а не к ещё не созданному commit или нормализованному LF blob.

| Файл | SHA256 |
|---|---|
| `server/capabilities-configuration.js` | `23cfc30642f8bf7036bc8f0509ea6d704f61100f389fd108915c79f0b828a191` |
| `server/capabilities-actions.js` | `668f0d40632a7b87cabb5695b474b414883f80ad267e9cab58f33a7bb17fc15e` |
| `server/capabilities-oauth.js` | `6ef07900ff3fb16876ff9819af7a8fad87efddbc531c14a716d89b498698ba4a` |
| `server/capabilities-oauth-cleanup.js` | `3cdd9ccfe525a0c6771b00fa18dcda48f41f1ad78adf0fd3912bbc608a7bd2a8` |
| `server/http-app.js` | `9c69505ae889364aceb5eaa97d39b9cd50f20081ff5ab5e0a79097dbe3feb56b` |
| `server/index.js` | `6fc3ee902f4865ab1f17b47809968fd925149e1c86682395ab1358d69d6b4c0d` |
| `server/test/capabilities-configuration.test.mjs` | `5947f3b928ab508c44deb17a3029c72e3d0727c4681276ffb404c61ac1de6380` |
| `server/test/capabilities-oauth-resource.test.mjs` | `f550399bca29365a551c3a0a8de4ebd374184c45be25ad67d868157a96d48f24` |
| `server/test/capabilities-oauth-composition.test.mjs` | `22a8fefc8ea4568378306d74ec51c67d862ebe4a325328c98292da59c9e92605` |
| `server/test/support/native-capability-http.mjs` | `f121096ab0057318cf0e41c4773bedea614dc737b731ecfa54d47f1fee6342f1` |
