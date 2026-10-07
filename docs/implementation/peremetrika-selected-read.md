# Переметрика: чтение выбранного ресурса

Реализован отдельный opt-in адаптер для существующего generic `apps_query`.
Пользователь выбирает одну страницу либо одну точную версию элемента библиотеки.
Агент получает структуру и, по `blockId`, точные данные одного блока как JSON.
Не требуется новый MCP-клиент, URL, токен или команда в запросе агента.
До private operator admission и действующего Native разрешения возможность
остаётся неподключённой; декларация приложения не даёт прав.

`createPeremetrikaReadonlyQueryAdapter` принимает только приватные constructor
ports `withAuthority`, `resolveCredential`, `assertDestination`, exact origin,
resource/selection/profile/release pins. `peremetrikaReadCatalog` создаёт закрытую
схему существующего Core; `effects:[]`, recipient и resource совпадают с одним
выбранным ресурсом. Root target/source binding закрепляется штатной composition.
Источник проверяет собственного текущего пользователя, его `canPage` и свой ACL;
Root отдельно проверяет signed Connect/App/grant/resource до и после сети.
`pma` имеет Native права записи: это **не readonly credential**. Только данный
reviewed compiled handler ограничен фиксированными GET и не может их использовать
для записи. Владелец Root не превращается в Native владельца. Source participant
и Native agent сохраняют собственные границы. Raw Root ID/header не является SSO.

Source устанавливает `/api/v1/soty-read/pages/:pageId[/authority|/blocks/:blockId]`
и `/api/v1/soty-read/library/:itemId/versions/:version[/authority|/blocks/:blockId]`.
Source format1 и существующие JSON/domain/Native IDs не меняются. Library проверяет
права на SOURCE page через существующий `readLibraryVersionForActor`, даже если
версия имеет отдельный путь. `X-Document-Actor` — дополнительное ограничение,
не credential. Изменившийся actor, revoke, membership или Native revision
отклоняет результат. Root читает authenticated scope до и после данных.

Границы: Native/Root responses ≤64KiB; структура ≤128 блоков, идентификатор блока
≤80 символов; больший ответ отклоняется, не обрезается молча. Никакого HTML-экспорта,
выполнения блоков, redirect, arbitrary forward или публикации. Production origin
HTTPS; HTTP — только явный fixture/approved loopback с портом1024..65535.
`assertDestination` обязан проверять утверждённое назначение на trusted host.
Изменение approved source/binding требует новой версии contract; pin metadata
само по себе не доказывает сетевую доступность, текущие права или executable bytes.

Actual gate использует настоящий Native API/auth/domain на synthetic temp store,
настоящие signed Connect/grants, installed Apps HTTP/WS channel и generic HTTP
query. Provider чтение в данном gate использует exact same-host HTTP port;
installed Apps channel проверяет Root source binding/liveness/ACL. Это не gate
удалённого Root→пользовательского loopback query transport; для него нужен
отдельный reviewed connector broker, ключ остаётся на trusted Native host.
Проверяет page/library, participant/source ACL, revoke обеих сторон во
время await, no-source-write, both restarts, metadata-only retry без повторного
Source чтения, запрет JSON key/URL/command/foreign page. Явно задать
`SOTY_PEREMETRIKA_SOURCE_ROOT` на reviewed Source checkout и запустить
`node --test modules/capabilities/test/peremetrika-readonly-adapter.test.mjs`.
Без пути actual Source tests SKIP, не PASS.

Нет claims shared SSO, Native UI embed, Source RP/renewal, multi-process Native
authority, production deployment или cold fallback. Native1 сейчас single writer:
pages — async queue + atomic JSON rename, team/agent — process-local state + queue.
Существующий Node API/MCP/owner/participant/delegated Native доступ сохранён.

Полный чистый Native check:1019/1016PASS/2FAIL/1SKIP. FAIL — исходные tests требуют
ignored `output/market-research-readable-20260906/page-spec.json` и
`output/market-research-20260906/page-spec.json`; это ENOENT, не read-adapter failure.
Чужие generated/user artifacts не копировались, тесты не отключались. Исходный
`D:/peremetrika` @aa83a392017ff26bbdbf0a9bb63fe0fc58af526b сохранён; отдельная
посторонняя Native-entry WT не использовалась. Новый Source build/targeted ACL
gate и Root5 actual wire/20 existing generic-Human-feedback/TSC учитываются отдельно.
