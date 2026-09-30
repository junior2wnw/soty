# P3-D1 — Apps5: сохранённые входы в приложения

Дата: 30 сентября 2026. Основание — [принятый D0](p3-engagement-plan.md), checkpoint `4fa5f72`. Это авторский отчёт модели и миграции. Сетевая интеграция, настоящая World authority fence и независимая приёмка имеют отдельные отчёты.

## Что реализовано

`createSavedRegistry({db, now, assertActor, withAuthorityFence, resolveEntry})` в `modules/apps/server/saved.mjs` использует существующую Apps SQLite. Экспортированы `savedOperations` и `SAVED_LIMITS`. Единственный метод — `execute({op,args,actor})`; он синхронный. Новые операции без host-owned fence закрыты, но создание legacy Apps service без fence само по себе не блокируется.

Одна запись аккаунта на приложение хранит точный `domainId/origin/path`, последний сохранённый `label`, `savedRevision` и `updatedAt`. Resolver получает только `{actor,appId,domainId?,path?}` под Apps transaction и возвращает `null` либо `{appId,domainId,origin,path,name,status,canManage}`. Origin сверяется с durable domain, path — общим `createLaunchPath`, включая лимит encoded bootstrap8192. Канонический вход выбирается только сервером, когда domainId отсутствует. Подмена недоступного alias другим адресом не выполняется.

Сохранение не выдаёт доступ. Для текущей проекции resolver повторно проверяет общий Apps policy. Если доступ/alias отозван, запись возвращает собственный старый снимок и `current:null`. Никаких свежих закрытых name, grants, device/connector, runtime pins или счётчиков. Источник offline разрешён: статусы `ready|starting|offline|stopped` информируют, но не становятся ACL. Неожиданные SQL/shape/authority ошибки не маскируются под unavailable.

## API и ответы

Все аргументы проверяются без преобразования массивов/строк в числа или boolean. `expectedAccountId` проверяет и удаляет перед вызовом registry root adapter; непосредственный registry получает trusted actor. Account/device сверяются до чтения сохранённой квитанции и повторно внутри transaction.

| Операция | Аргументы | Ответ |
|---|---|---|
| `apps.saved.get` | `{appId}` | `{revision,entry:null|Entry}` |
| `apps.saved.list` | `{limit?,cursor?}` | `{revision,entries:Entry[],nextCursor:null|string}` |
| `apps.saved.set` | `{appId,saved:boolean,expectedRevision,requestId,domainId?,path?}` | `{requestId,replayed,receipt,current:{revision,entry}}` |

`Entry = {appId,domainId,origin,path,label,savedRevision,updatedAt,current:null|{name,status,canManage}}`.

`receipt = {appId,saved,revision,committedAt}` — факт исторически принятого desired-state запроса. Квитанция не утверждает, что запись существует сейчас, и не хранит второй снимок закрытых метаданных. Полное исходное wire-намерение защищено его fingerprint. `current` читается отдельно; после позднего remove оно законно содержит более новую revision и `entry:null`.

При `saved:false` domainId/path запрещены даже с явно переданным undefined; нынешний доступ к приложению не нужен. Повторное снятие уже отсутствующей записи — допустимая команда и новая revision. При `saved:true` замена ранее сохранённого alias/path является явным set с CAS, не toggle и не фоновым обновлением карточки. Клиент должен обозначить замену выбранного входа человеку.

## Транзакция, повтор и границы ресурсов

Порядок: trusted Connect actor → host World fence → Apps `BEGIN IMMEDIATE` → проверка receipt/CAS/ACL → head/entry/receipt/prune → Apps COMMIT → World release. Network/await отсутствуют. `assertActor`, resolver и callback results не могут быть thenable; declared async fence отклоняется до вызова. Fence вызывается один раз, поздний/повторный вход callback отклоняется. Это проверка интеграционного контракта, а не sandbox для произвольного внедрённого host-кода.

На синхронный Apps участок временно действует `busy_timeout=100`, после него восстанавливается точное прежнее значение. SQLite BUSY/LOCKED отображается как `apps_saved_busy`503: разрешён точный повтор после освобождения. Это короткая политика ожидания, не обещание wall-clock deadline. Она не меняет ожидание старых Apps операций.

Head хранится постоянно: начальное чтение без head даёт revision0, первая принятая запись создаёт revision1. Любой новый принятый set, включая no-op, увеличивает revision; повтор того же requestId+fingerprint не увеличивает. Head не удаляется/не уменьшается/не заменяется SQL REPLACE. `expectedRevision` препятствует воскрешению записи после удаления и pruning старого receipt. Вариант того же requestId с другими аргументами получает конфликт.

Потерянный ответ после Apps COMMIT, включая ошибку host World release, не объявляется rollback. Сохранённая квитанция обеспечивает точный повтор. Normal host function, ошибочно возвращающая Promise уже после синхронного callback+COMMIT, тоже не может отменить выполненный commit; trusted host обязан сохранять синхронный контракт.

До200 активных записей на аккаунт. Удаление и обновление уже существующей записи работают при полном лимите. До128 последних квитанций по committed revision, не по ненадёжному wall-clock порядку. Unsaved row удаляется, бесконечные tombstones не создаются; один account head остаётся.

Страница default20/max50, порядок savedRevision DESC. Cursor связан с account, device, revision и последней выданной savedRevision. Изменение библиотеки требует новой первой страницы, а не смешивания снимков. Cursor не является полномочием: он проверяется после actor authentication и не даёт чужих rows. Фактический UTF-8 JSON registry response ≤262144 байт, включая cursor; длинные допустимые пути уменьшают количество записей страницы. Read выполняется в bounded transaction; история не материализуется целиком. Объём transport envelope Connect не входит в этот предметный payload budget.

Основные ошибки: `apps_saved_revision_conflict`409, `apps_saved_request_conflict`409, `apps_saved_capacity`409, `apps_saved_revision_exhausted`409, `invalid_saved_cursor`400, `apps_saved_cursor_expired`409, `app_unavailable`404, `apps_authority_fence_required`503, `apps_authority_fence_invalid`500, `apps_saved_busy`503.

## Формат Apps5

Marker `soty.apps-registry.v5`, `PRAGMA user_version=5`. Ровно три новые таблицы; discussion будет отдельным Apps6 checkpoint:

| Таблица | Колонки / ключ |
|---|---|
| `app_saved_heads` | `account_id` PK, `revision` |
| `app_saved_entries` | `(account_id,app_id)` PK, `domain_id,origin,path,label,saved_revision,updated_at` |
| `app_saved_receipts` | `(account_id,request_key)` PK, `intent_hash,app_id,saved,committed_revision,created_at` |

Два UNIQUE indexes: `app_saved_entry_revision(account_id,saved_revision)` и `app_saved_receipt_revision(account_id,committed_revision)`. Три exact head guards: `app_saved_head_no_downgrade` (включая равную revision/смену account), `app_saved_head_no_delete`, `app_saved_head_no_replace`. Полные frozen DDL в `savedDdl()/savedGuards()` schema.mjs. Прежние Rooms/Apps1–4 DDL не переписаны.

Genuine Apps4 мигрируется добавлением пустых saved tables без новых сохранений или изменения ACL/publication/target/domain/source receipt. Reopen Apps5 валидирует FK, привязку app/domain/origin, head bounds и лимиты, safe scalar integer data и реально запускаемый saved path. Missing head/неизвестные объекты/изменённые guards не ремонтируются автоматически. Future Apps6 отклоняется до изменений. Исторические небезопасные runtime target paths не были ужесточены этой saved migration: они по-прежнему отдельно доступны владельцу для исправления.

Reader5, image manifest, old-reader отказ, WAL inspection и backup/restore находятся у отдельного автора выпуска. Этот отчёт не объявляет production переход или старый Apps4 rollback допустимым.

## Авторское доказательство

После окончательного path/timeout уточнения:

`node --test modules/apps/test/app-saved.test.mjs modules/apps/test/app-engagement-schema.test.mjs` — **21/21 PASS**, 0 skipped.

Проверены фактические SQLite close/reopen и два независимых worker connections, точный replay/current после удаления, private-unavailable projection, retired alias, head CAS, quota200, retention128 с обратным временем, Unicode страницы с допустимым boot path, last safe integer, strict scalar inputs, ошибки resolver/fence, fault receipt insert/prune, release-after-COMMIT и busy другого writer с восстановлением timeout/повтором. Worker результат принимается после закрытия DB и exit до удаления synthetic fixture.

Миграционные6 tests используют независимый literal Apps4 fixture из `4fa5f72`, не новую migration под старым marker. Сохранены все прежние rows, в том числе rollback target1 при floor2, revoked app, grants, domain tombstone и старые source receipts. Проверены SQL head guards, missing-head/нецелые данные/cross-app corruption, future6, injected failure после создания первых Apps5 объектов с полным rollback к Apps4.

Ранее после current-version обновлений: совместный ограниченный прогон этих двух файлов и `app-source-schema`, `app-publication`, `app-publication.acceptance`, `newdomains` — **85/85 PASS**. Последнее path уточнение затронуло только новые saved rows/их tests и повторено указанными21 tests. Изменения старых tests ограничены latest5/future6 expectations; frozen historical inputs не изменены. `git diff --check` чистый (только обычные Windows LF→CRLF предупреждения).

Final author SHA256:

- schema.mjs: `894f20556d1d29596bc6b56a8450ab76dcc1a36c5232e1cdc182bebb93b5a272`
- saved.mjs: `60e5a5a2d5088776880d12f60fba71ed04d6a9b2642005e22a96ef04caee11c1`
- app-saved.test.mjs: `db000af2469f5b36f1cb8516ccb34a16c98f1815a6cf54315a6e67804ddd08a2`
- app-engagement-schema.test.mjs: `eac368e6ff560cc96dff58af36e3a81a80b40041992a373690e67a0af88af712`

Предметные author tests используют явно локальный синхронный fence и resolver fixture. Они не доказывают signed HTTP, межпроцессный World membership fence, Linux exact-image WAL, restore production backup, браузерную D3 библиотеку или приложение с миллионами пользователей. Эти проверки не подменяются успешной модельной серией.
