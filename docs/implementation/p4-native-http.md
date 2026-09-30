# P4-B2 — typed HTTP, восстановление и общий результат

30.09.2026. Root host integration по [принятому host-плану](p4-native-host-plan.md) и [доменному B2b](p4-native-effect-b2b.md). Независимые проверки: [ingress](p4-capabilities-ingress-independent.md), [HTTP](p4-native-http-independent.md), [native effect](p4-native-effect-independent.md). Production не включён.

## Реализованный интерфейс

`POST /api/capabilities/v1/notes/drafts` принимает ровно `title`, `body`, `idempotencyKey`; `GET /api/capabilities/v1/invocations/:id` возвращает свой разрешённый исторический статус. Пользовательский account/noteId/path/endpoint не принимаются. Никаких универсальных execute, cookie-auth, чтения текущей записки, OAuth/MCP или нового identity store в этих маршрутах нет.

Host требует явно настроенный canonical audience из разрешённых shell origins, проверяет literal route/Host/Origin/method и единственный Bearer. Body читается без DB lock; перед admission и перед выдачей результата полномочия проверяются заново. Внутренний actorless `execute/reconcile` не становится HTTP-ответом. Ошибка после эффекта не приравнивается к отсутствию эффекта: сервер получает новую разрешённую долговечную проекцию; при отзыве отвечает отказом без receipt.

Результат: `invocation`, для POST также `reused`; при проверенном committed create дополнительно `result:{noteId,revision:1,url}`. URL строится из trusted audience и ведёт к обычной авторизации Notes. Он не обещает, что человек не изменил или не удалил объект. Исторический повтор не обращается к текущему тексту и не воскрешает удалённую записку.

POST:201 новый успех,200 повтор/terminal failure,202 nonterminal. GET:200 доступная история. Безопасный allowlist ошибок различает400/401/403/404/405/408/409/413/415/429/500/503; private ответы всегда `no-store`, `nosniff`, без wildcard CORS. Непрочитанное/незаконченное тело закрывает соединение. Retry-After не является обещанием свободного бюджета. При неопределённом исходе клиент сохраняет исходный key.

Ingress: UTF-8 fatal, дубли JSON keys (включая escape), неизвестные/нестроковые поля и malformed headers отвергаются. Raw body≤2MiB,15s total deadline,8readers/process,2/socket peer,60attempts/60s,2048peer entries. Используется один bounded buffer на body; ранее выявленная зависимость heap от числа крошечных chunks устранена. Это≤16MiB payload buffers, не заявленный общий RSS процесса. Domain canonical/document budgets остаются256KiB независимо от транспортного запаса.

OpenAPI3.1.2 собирается из существующего public документа и реальной host-поверхности; schema Notes@1 заимствуется из единственного каталога, transport fields добавляются снаружи. Общие path/ID patterns используют router и документ. Операционный readiness не кэшируется, contract/документ используют ETag. Публичный schema doc не содержит владельцев, вызовов или credentials.

## Восстановление

Только при уже открытых Notes2/Caps2 host запускает один unref timer. За один turn обрабатывается не больше16идентичностей, затем пауза2s между страницами или30s между проходами; ошибки дают backoff4–60s. Cursor сохраняется на ошибке страницы. Отдельная неудачная запись не скрывает следующую страницу и не объявляется отсутствием эффекта.

Scheduler вызывает только `reconcilePage`, никогда `beginAttempt/execute`: он подтверждает известный эффект/отмену, а не автоматически повторяет работу. Operational disable не выключает восстановление положительного proof. V1/mixed stores не запускают сканер и не мигрируются им. Закрытие host сначала отменяет timer, затем закрывает services. Локальная status-проекция содержит только bounded counters/время/признак недоступности, без IDs/input/exception.

## Проверки и границы

- Root ingress с независимыми cases:12/12PASS после исправления chunk-memory finding; отдельная receipt содержит измерения и точные hashes.
- Root HTTP/OpenAPI/recovery: **15/15PASS,0skips,6593.9301ms**, Node24.21.0, serial; log `output/implementation-20260930/p4-native-http-root.log`. Настоящие owner signatures, HTTP sockets, Notes/Caps SQLite, lost socket after effect, revoke во время body и после эффекта, post-COMMIT exception, edit/purge/disable replay, чужие/sibling scopes, malformed/oversized inputs, no migration on mixed stores. Python JSON Schema2020-12 проверяет фактические DTO против опубликованного OpenAPI.
- Независимый reviewer: **2/2PASS,0skips,1132.8064ms**. Actual Notes COMMIT + synthetic Caps receipt failure→HTTP202/unknown; GET не выполняет recovery; original credential revoke; controlled timer settle; новая credential той же grant области читает receipt/spent1. Отдельно incomplete unauth upload→Connection:close→следующий исправный запрос, HEAD405 и private conditional GET200. Это не process-kill proof; его отдельно даёт доменный B2b.
- [Настоящий PWA-сценарий](p4-native-pwa-proof.md) подтверждает UI consent→HTTP create/replay→история→точный исходный текст→human edit→reload и390px без overflow.

Обычный host имеет `nativeNotesEnabled:false`, audience пуст, migration option не принимает. Внешнее включение требует совместимых reader/image/restore gates; localhost fixture не разрешает миграцию production.

## Общая локальная интеграция

После всех исправлений root запустил один общий Node24.21.0 / SQLite3.53.4 regression с concurrency2: **1141 tests /1136 PASS /0 FAIL /5 opt-in SKIP**,119905.3421ms. Включены Connect, Capabilities, Notes, World, Apps, весь server/test, UI-state/geometry/theme/platform/transport и dev/executor-policy. Лог `output/implementation-20260930/p4-b2-root-regression.log`. Пять прежних opt-in сценариев (immutable installed connector,130s default WS, real OpenCode, registration recovery,512MB full transfer) в этом запуске не выполнялись; прежние результаты не подменяют нынешние.

Typecheck и production build прошли на том же isolated Node; логи `p4-b2-root-types.log` и `p4-b2-root-build.log`. Первый вызов глобального pnpm wrapper попытался выполнить install и остановился на отсутствии TTY, не выполнив проверку. Root не разрешал заменять modules: вызвал существующие локальные TypeScript/Vite через pinned Node и отдельно штатный prebuild. Эти прямые команды выполнились успешно; dependency/lockfile изменений нет.

Deployment reader2 имеет отдельный [принятый локальный gate](p4-reader2-root-gate.md). OAuth, MCP, реальные внешние AI-клиенты и production остаются следующими самостоятельными этапами; локальный HTTP-клиент и loopback UI за них не засчитываются.
