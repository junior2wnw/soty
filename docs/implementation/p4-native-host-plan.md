# P4-B2: host authority и HTTP — подготовка интеграции

30.09.2026. Подплан root до реализации B2; не отчёт о готовом исполнении. B1a reader2 сначала проходит отдельную приёмку. Доменный контракт — [native storage](p4-native-storage-contract.md); общий outcome и неизменяемые маршруты — [P4 preflight](p4-external-agent-preflight.md).

## Области и порядок

1. Domain author: Notes native create/proof и Capabilities admission, marker, execution, reconciliation, quotas/retention в своих modules. Контракт @1 и прежний бюджет остаются единственными источниками истины.
2. Root: Connect host-only serialization fence, composition с тем же Notes handler, typed HTTP create/get и bounded ingress. Сначала узкая проверка fence, затем integration с принятым доменным кодом.
3. Independent reviewer: реальные отдельные writers, revoke/crash/lost response/Unicode/privacy и минимальный внешний HTTP client; source не исправляет незаметно за автором.
4. Совместная локальная приёмка, PWA opens exact note и показывает исходный текст; общий regression и отдельный checkpoint. OAuth/MCP следуют C1/C2; Linux/release gates не закрываются loopback результатом.

## Connect fence

Внутренний метод `withAuthorityFence(action)` принадлежит Connect service. Он не является RPC operation, не принимает HTTP-selected account/device и не создаёт новую identity. Хост передаёт только доверенный синхронный callback фиксированного native coordinator.

Метод проверяет service-open, отсутствие уже открытой Connect transaction и синхронный callback. На время acquisition ставит короткий `busy_timeout=100`, начинает `BEGIN IMMEDIATE`, выполняет callback, отвергает Promise/thenable, завершает transaction и возвращает исходное значение. На ошибке выполняется best-effort rollback; исходная ошибка не подменяется ошибкой rollback. Busy timeout восстанавливается в `finally`; обычные Connect операции сохраняют прежний timeout. Никаких await/network, callback-проекций в logs или автоматического повтора.

Это сериализация с другим Connect writer, включая `device.revoke`; сама блокировка не выдаёт полномочия. Пока она удерживается, coordinator повторно проверяет credential, creator devices и всю grant ancestry под Caps lock. Порядок везде один: Connect → Capabilities → Notes. Попытка вызвать fence из уже выполняющегося signed extension закрывается без rollback внешней Connect transaction. Создание native Notes идёт через отдельный endpoint после проверки service credential, без fake Connect actor.

Transaction, fence, `handle` и `close` используют общий guard состояния Connect. Вложенный вызов отклоняется **до** собственного BEGIN или закрытия DB и не откатывает внешнюю signed transaction. Код rollback/finally сохраняет первичную ошибку даже при ошибке rollback или восстановления PRAGMA. Ошибка callback/thenable/COMMIT после возможного Notes COMMIT не является доказательством отсутствия эффекта: coordinator сохраняет неопределённость либо возвращает уже записанный долговечный receipt после проверки.

Async callback отклоняется до вызова, если это AsyncFunction. Возвращённый thenable тоже закрывается; это дополнительная защита программного контракта, не sandbox для произвольного JS. Поздний Notes вызов закрывается opaque native context, который coordinator аннулирует в `finally`. Callback не получает raw Connect DB.

Приёмка fence: два заранее открытых реальных Connect writers над одной DB; конкурентный revoke не проходит пока fence удержан, после освобождения проходит и закрывает дальнейшую проверку actor. Обратный порядок (revoke завершён первым) запрещает эффект. Проверяются blocked BEGIN без callback, thrown callback, Promise/thenable, nested fence, закрытый service и последующая обычная signed operation. Время ожидания измеряется с запасом среды, 100 ms не выдаётся за общий deadline трёх хранилищ или fsync.

## Composition и admission

Оба stores получают один trusted `projectId='soty'`. Опция явной native migration остаётся отдельной, по умолчанию false; operational execution также по умолчанию false. Наличие v2 DB само по себе не включает новые исполнения, и отключение исполнения не позволяет откатить несовместимый reader. POST остаётся зарегистрированным при execution/readiness=false: существующий ключ сначала проходит current read ACL и проверку fingerprint, затем возвращает прежний terminal/held Invocation. Readiness закрывает только новое намерение. Потерянный ACK перед operational disable не вынуждает создавать новый ключ.

Notes конструктор получает проверяющий callback через closure на единственный coordinator. Coordinator получает только фиксированный Notes native API и host authority fence. V1/v2 mixed bootstrap сохраняет обычные Notes/Capabilities операции, native readiness false до совпадения project/registry identities. Public discovery `notesCreateEnabled` отражает фактическую composition/readiness; на первом deploy не включается автоматически.

Host HTTP не реализует повторно domain ledger или authorization. Exact existing-key replay выполняет native coordinator до новых quota/readiness checks. После admission HTTP может выполнить begin/execute; любой неоднозначный исход сохраняет ID, intent и held budget. Повтор HTTP использует тот же client key. Ответ после отмены запроса не создаёт новый intent, не сообщает ложное отсутствие эффекта и не компенсирует Notes автоматически.

Внутренние execute/reconcile возвращают actorless Invocation: они вправе завершить уже подтверждённый эффект после revoke. HTTP никогда не отправляет этот результат напрямую. После попытки, включая ошибку, он снова получает authorized read projection под Connect → Capabilities с текущими credential, creator device и grant ancestry. Предварительная проверка bearer до чтения body не заменяет проверку внутри admission/execution fence. Новый retry credential не подменяет исходное разрешение на исполнение; для чтения собственного результата нужны его текущие права.

## Узкие HTTP поверхности

- `POST /api/capabilities/v1/notes/drafts`: ровно `{title,body,idempotencyKey}`. Account, device, noteId, получатель и resource URL не принимаются.
- `GET /api/capabilities/v1/invocations/:id`: только текущая own projection; чужой ID и закрытый scope не дают существование/счётчики/body.
- Все остальные private routes остаются закрытыми. Derive относится к C2. Public catalog/HTML/schema A сохраняют свои cache/ETag и CORS отдельно.

Внешний bearer имеет ровно trusted configured resource audience. Его можно создать прежним подписанным owner access API для независимого HTTP пилота; токены не попадают в URL, журналы, screenshots или fixtures receipts. OAuth C1 не подменяется таким техническим пилотом.

Маршруты зарегистрировать перед namespace fallback discovery. Проверить raw request target/method/query, canonical trusted Host, отсутствие неизвестных сегментов и auth ambiguity. JSON only, без сжатия; входной raw byte limit учитывает transport envelope и canonical domain budget отдельно. Fatal UTF-8 decoding, точные scalar fields, rejection lone surrogate до admission; не менять semantic digest @1. На HTTP нет общей browser-cookie авторизации или wildcard credentialed CORS.

Ingress имеет фиксированные проверяемые пределы: raw JSON ≤ 2 MiB (включая экранирование), полный body deadline 15 s от начала чтения без продления каждым chunk, ≤ 8 readers на процесс и ≤ 2 на peer. Canonical input и полный Notes document независимо ограничены доменом до 262144 bytes; транспортный запас не расширяет эти лимиты. POST rate — 60 попыток за 60 s на peer; таблица ≤ 2048 peers, удаление истёкших записей, новые peers при заполненной таблице отклоняются без вытеснения действующего лимита. Peer берётся из socket, proxy headers самостоятельно не трактуются. Это ingress backpressure, не обещание глобального distributed rate limit.

Body reader освобождает slot ровно один раз при success, abort, timeout, parse error и раннем отказе. После истечения deadline тело больше не обрабатывается. Connect/Caps lock не удерживается во время чтения body. GET status не занимает body-reader или new-admission slot; до реализации его отдельный общий HTTP rate limiter не вводится. Новый grant/client не обходит durable account/native quota. Переполнение body/ingress не создаёт ledger row и не маскируется domain quota. Имеющиеся authenticated get/revoke и внутреннее reconciliation не зависят от свободного new-admission ledger slot.

HTTP mapping до source freeze: 400 — shape/UTF-8/path/query; 401 — отсутствующее или недействительное удостоверение; 403 — отсутствующий scope; 404 — недоступный/чужой Invocation; 409 — тот же key с другим fingerprint; 413 — raw/canonical/document bytes; 415 — media type/encoding; 408 — body deadline; 429 — ingress/domain rate или capacity; 503 — busy/not-ready/storage unavailable; непредвиденная ошибка — 500 с фиксированным code. Для retryable отказа задаётся ограниченный Retry-After; текст внутренней ошибки не передаётся. После неоднозначного исполнения доступная fresh projection возвращается 202; при закрытой current ACL применяется отказ без receipt. Конкретные domain codes сводятся в исчерпывающий mapping вместе с интеграцией B2.

Ответ — content-free envelope с Invocation/status/effectState и минимальным receipt. Проверенный success содержит исходные `{noteId,revision:1}` и URL из trusted shell origin + `#notes/<id>`. Это ссылка с обычной PWA авторизацией, без bearer и проверки текущего Notes existence. При ambiguous execution доступный текущему actor Invocation возвращается как accepted/uncertain; если права уже отозваны, данные не раскрываются ради удобного replay. Safe fixed error mapping различает malformed/auth/not-found/conflict/rate/busy; raw SQL/stack/body/credentials не возвращаются. Private responses всегда no-store и nosniff.

## Что проверяется перед включением

Реальный loopback HTTP без MCP: owner создаёт узкий service grant → POST → один Notes object → get/повтор → одна reservation/spent → authorized PWA открывает исходный текст. Два accounts и sibling grants, lost ACK, same key/different payload, revoked/expired grant/creator device, disabled handler replay, terminal no-effect replay, edited/purged note и отсутствие read/existence leak обязательны.

Transport matrix: encoded path/query, wrong Host/audience/method, duplicate/malformed auth, malformed UTF-8, lone surrogates, pair emoji/NFC/NFD, content type/encoding, raw/canonical/document size, slow/aborted/chunked body, concurrent/rate ceilings. Private errors не содержат input/SQL. SDK wire test не засчитывается реальным внешним ИИ; две выбранные CLI проверяются на D1.

## Первичные основания и границы

[SQLite transactions](https://www.sqlite.org/lang_transaction.html) описывает один write transaction и acquisition `BEGIN IMMEDIATE`; [busy_timeout](https://www.sqlite.org/pragma.html#pragma_busy_timeout) задаёт connection-local busy handler. Отсюда выбран порядок блокировок, короткое acquisition и восстановление прежнего timeout. Это проектный вывод, подтверждаемый двумя процессами, а не обещание atomic commit разных DB.

[Node SQLite](https://nodejs.org/api/sqlite.html) документирует синхронный DatabaseSync; actual runtime проверяется отдельно на закреплённой Node24.21.0. Сети/await внутри fence нет. WAL proof/reconcile и согласованный restore остаются обязательными: Connect fence не превращает три commits в один.
