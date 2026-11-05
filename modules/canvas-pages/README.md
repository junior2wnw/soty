# Публичная страница канонического полотна

Backend R28,04.11.2026, по текущему поручению и CONCEPT.md. Page config хранится отдельно; геометрия читается только из World.field, содержимое/права — из Canvas Objects и собственных Sources. Ни копии полотна, ни выдачи actor владельца посетителю нет. HTTP/UI интегрирует Root; работающий UI/публикация не следуют из наличия модуля.

## Явный выбор личного полотна (format2)

`canvas.pages.get` добавляет `selectionAvailable:boolean`, определяемый реальным Page2 + Object2 + host authority. Config может содержать `selection:{schema:'soty.canvas-page-selection.v1',fieldRevision,items:[{shortcutId,entity:{kind:'object'|'app',id},expectedAccessRevision?}]}`. GET возвращает именно сохранённую entity, а не новую цель shortcut после rebind. Отсутствие selection означает прежнюю normal-public композицию. Явные `items:[]` означают пустой выбор; это не команда показать всё. Новые shortcuts не добавляются автоматически.

`canvas.pages.put` принимает ту же optional selection, до256 уникальных canonical shortcutId. Entity обязательна и должна точно совпасть с текущей canonical целью до Source header; один и тот же shortcut/accessRevision после rebind не означает согласие на новую соту. Source binding клиент не задаёт. Root UI сначала сохраняет canonical field, затем передаёт его текущую revision. Object требует точный current accessRevision; legacy `kind:'app'` не принимает этот pin. Сервер сохраняет shortcut/entity/current binding + accessRevision, без геометрии и содержимого. Старый config request без selection сохраняет прежний выбор. Receipt replay происходит до fresh Source validation; stale field/entity/accessRevision/CAS не превращается в успешную пустую публикацию.

Владелец может делегировать выбранную собственную private Native-соту только через короткий Page frame. Root один раз регистрирует Objects authority с фиксированным списком `soty.notes`, `soty.materials`, `soty.vault`. Политика объекта глобально не меняется, прямые Notes/Materials/Object API остаются приватными. Visitor/anonymous остаётся исходным посетителем. Private чужой Object и apps/widgets не наследуют прав владельца страницы: им нужны normal Object ACL и текущие права настоящего viewer на Source. Hidden.others, удаление, неизвестный provider и Source revoke исключают ref/геометрию.

Page2 selection pin использует Object2 `accessRevision`. Шрифт/маска увеличивают обычную `revision`, но сохраняют policy pin и Page доступ. Visibility/hidden/state и явный `canvas.objects.update.revokePublication:true` увеличивают policy pin; даже private→private с этим флагом отзывает публикацию. Обычное сохранение всех неизменённых settings таким отзывом не считается. Точный Object read context по-прежнему использует `objectRevision` для TOCTOU и добавляет `objectAccessRevision` для scoped парольного permit. Source body остаётся живым.

Парольный gate проверяется до Source payload. Только для selected Page locked-ветки после gate разрешён bounded header `purpose:'protection'`, чтобы удалённый/запрещённый Source не оставлял даже lock geometry; `project`, decrypt и content не вызываются. `canUnlock` истинен только для подписанного normal Object reader. Page-only viewer получает `canUnlock:false`: отдельный Page-scoped unlock **ещё не реализован**, вся R28 этим backend не закрывается.

Снять выбор, закрыть страницу, canonical unlink и отзыв policy/Source прекращают старые weak frames до возврата bytes. JSON clone, late/async contexts не являются grant. Закрытие `enabled:false` или `audience:'private'` с omitted selection не зависит от доступности старого field/Source, сохраняет selection и требует текущий owner CAS.

`allowCanvasPageSelectionMigration` / `SOTY_CANVAS_PAGE_SELECTION_ENABLED=1` — только явный opt-in миграции Object/Page1→2. Maintained reader читает известные1/2 и при false, false не создаёт/не апгрейдит2. Это не kill switch. Старые reader/labels[1] обязаны отказать2; downgrade отсутствует. Две SQLite миграции не являются общей cross-DB транзакцией: при сбое остаётся честная совместимая смесь1/2, selection недоступна без обоих2, перед внедрением нужен согласованный snapshot всего набора. Никаких production migration этот backend gate не выполнял.

Проверки: `selection.test.mjs`, `selection-storage.test.mjs`, `../canvas-objects/access-policy.test.mjs`, `../../server/test/canvas-pages-selection-root.test.mjs`, `../../deploy/connector/storage-page-selection.test.mjs`. Реальные SQLite/подписанный HTTP/anonymous Notes+Materials+Vault, reopen flag-off, visible-only wait/private quiet; контроль с удалённым policy check раскрывает отозванный Object и проваливает тот же oracle. Реальная ошибка SQLite ALTER откатывает даже временно удалённый Object trigger; старые IDs/receipts сохраняются. Старые legacy Page tests остаются обязательными.

## Конфигурация и композиция

Один config/owner: `{schema:'soty.canvas-page.v1',pageId,ownerId,revision,enabled,audience:'public'|'unlisted'|'private'}`. PageId случайный и стабильный; owner определяется оригинальным подписанным Connect actor. `canvas.pages.get` возвращает собственный page/null без implicit create. `canvas.pages.put` требует expectedAccountId/requestId/expectedRevision/enabled/audience. Config+CAS+immutable receipt COMMIT атомарны, replay возвращает исходный receipt и текущий config. В БД нет geometry/source payload. Все ссылки остаются теми же после restart. Enabled=false закрывает чтение; public/unlisted читаются по известному pageId; private — только реальным владельцем. У нынешнего Root все HTTP страницы noindex/nofollow: SEO/public discovery **не заявлены**.

Сервис `read({pageId,actor?:originalIssuedActor,cursor?,limit?})` возвращает:

```js
{schema:'soty.canvas-page-view.v1',pageId,viewRevision,banner,
 field:{schema:'soty.field.v2',contexts,shortcuts},
 items:[{shortcutId,entity,presentation,protection,view}],nextCursor}
```

В legacy композиции без selection `objects.project` получает текущего зрителя, а не владельца страницы. Private/hidden/removed/Source-denied/unknown provider исключаются **до** выдачи ref/координат; явная Native selection действует только в описанной выше ограниченной Page-области. Публичный Object не публикует приватное приложение и не даёт прямой Notes/Materials grant. Страница может содержать Object другого владельца только пока его текущая политика разрешает чтение. Person/community/device/builtin legacy refs не выдаются по умолчанию вообще, даже в виде пустой карточки с ID. Новые типы идут через общий Objects provider.

Выдаются только контексты видимых shortcuts. Их title — «Полотно», contextId/shortcutId заменяются стабильными page-scoped opaque hashes; приватные названия и пустые области не раскрываются. Context x/y и placement сохраняются точно, без уплотнения или второй геометрии. V1 hex документ проецируется в DTO v2 без изменения storage bytes/hash/receipt/reader floor. Hidden.owner и hidden.others применяются к реальному зрителю по существующему Object контракту. Для предпросмотра «как видят другие» Root использует anonymous path, а не личную signed owner projection.

ViewRevision — SHA256 только от отфильтрованной композиции для данного зрителя. Не входят World revision/contentHash/updatedAt, Object revision, приватные contexts/shortcuts/тайтлы или скрытый banner. Изменение частной геометрии/приватной Source/скрытого профиля не меняет публичный hash. Видимые Source edits/moves/revokes меняют его. Source view metadata поступает от своего provider; она не копируется в Page DB.

Chunk cap512KiB, limit1..32/default24. Каждый chunk содержит только свои field contexts/shortcuts/items. Cursor привязан к hash всей видимой композиции и последнему opaque shortcutId; private edits сохраняют cursor, visible edit даёт `canvas_page_cursor_stale`409. Поле ограничено256 shortcuts×64KiB provider projection, без unbounded traversal. Root обязан дочитывать nextCursor; первый chunk не означает всё полотно.

## Authority ports

```js
createCanvasPagesService({databasePath,projectId,actorActive,withActorAuthorityFence,
  withCanvasSource:(request,callback)=>world.withCanvasSource(request,callback),
  objects:canvasObjects,
  withLegacyAppAuthority:(request,callback)=>apps.withPublicCanvasAuthority(request,callback),
  onChange,clock
});
```

World `withCanvasSource({ownerId,viewer:verifiedTuple|null},cb)` — host-only короткий синхронный snapshot, не HTTP API и не право читать Source содержимое. Он не вызывает ensurePerson от имени владельца и не присоединяет contacts/devices/presence/groups. Banner содержит лишь profileId/displayName/avatarColor/avatarRevision при существующей `canViewFullProfile` policy: public/link, свой профиль или актуальные reciprocal friends; иначе generic без ownerId. Контакты берутся через живую Connect fence, не из кеша. Политика link сохраняет существующую World семантику известной ссылки, не делает профиль discoverable.

Нельзя держать World transaction через Objects/Apps provider calls: они сами открывают World authority fence. Pages берёт короткий canonical snapshot, разрешает refs, затем заново сверяет config/source; при изменении максимум одна повторная композиция. World port допускает read внутри уже доверенной World fence для финальной membership проверки. С signed viewer такая внешняя fence должна уже соблюдать порядок Connect→World. Callback не может вызвать World.execute/async/nested source read. Оригинальный actor проверяется в начале/конце и отдельно передаётся в Objects; copied tuple не заменяет Connect proof. Нет общей транзакции нескольких сервисов или распределённой multi-root snapshot гарантии.

Legacy `kind:'app'` проходит новый **host-only** `apps.withPublicCanvasAuthority({actor?:currentViewerTuple,appId},cb)`. Signed path использует существующий current default entry/ACL; anonymous — текущий active bound named PUBLIC alias и Source target path, включая известный unlisted. Внутри тот же Apps Source authority/decideAccess, без catalog или owner impersonation. Private/inactive/retired/revoked не получают fallback. Helper `apps/server/public-canvas-authority.mjs` использует private constructor option общего widget-source-authority; обычный pinned widget по-прежнему требует exact entry. Catalog/agent discovery listed policy не расширяется. View содержит минимальный app record/entry, без connector/port/grants/tickets. Root запускает app через его существующий runtime.

## Чтение содержимого

```js
pages.withPageObjectRead({pageId,objectId,actor?:original},pageContext=>
  pages.withPageObjectAuthority(pageContext,objectContext=>
    nativeSource.readContent({context:objectContext})
  )
);
```

Перед чтением проверяются enabled/audience страницы, **актуальное наличие Object ref в каноническом поле**, затем Object/Source ACL и password barrier. Callback получает WeakMap branded Page context без owner/ref. JSON copy и поздний исходный context не работают. Only `withPageObjectAuthority` делегирует конкретный live Core context Native порту. Содержимое detached/bounded до1MiB, callback синхронный; нет SQL через await. Page config и canonical membership проверяются перед callback и после него, внутри live Core frame. Unpublish/unlink/revoke во время callback отбрасывает результат до возврата bytes. Unrelated private field edits не отменяют разрешённое чтение и не раскрываются revision.

Locked Object может показать только стандартную lock view и разрешённую геометрию своего размещения; Source payload/project/decrypt не вызываются. В selected frame bounded Source header вызывается после gate для fresh liveness/ACL. Пароль и собственное Source право остаются отдельными условиями. Anonymous public page не получает device-bound unlock permit автоматически.

## Root HTTP и проверенные границы

Root adapter: GET `/api/canvas/pages/:pageId`, `.../objects/:objectId/content?part=note|metadata|chunk|vault&index=...`, `.../updates?after=<visibleHash>&waitMs=0..15000`. No-store/noindex; public handler всегда anonymous. Signed foreign read возможен через host service с настоящим actor, без подмены query.actor. Long waits держат RAM listener, а не SQL; global content-free hints лишь повод пересчитать страницу. Private activity не завершает wait, если visibleHash тот же. Disable закрывает старый pending wait и content.

Backend/Root evidence на Node24.22.1:

| Требование | Наблюдаемый критерий и проверка |
|---|---|
| Один канонический field, стабильная ссылка | SQLite config/CAS/receipts/reopen; v1 bytes/hash/epoch unchanged; actual signed Root configure→field→anonymous exact placement/content→restart |
| Private/hidden/unsupported before wire | Скан JSON на секретные Object/device/contact IDs, названия и coords; Source project calls исключённых объектов0 |
| Source ACL независим от страницы | Foreign signed grant/revoke и другой владелец public Object; actual Apps private/unlisted/retired, удаление metadata/geometry |
| Profile privacy | public/link/friends/private/current owner, friends remove; generic banner без identity/contacts/presence |
| Quiet private activity | Hash EXACT same после private geometry/context/profile/owner-hide; actual Root pending wait остаётся открытым после private field/Source edit |
| Realtime видимого Source | Actual Root public Source commit будит wait без reload; new fullcontent по current Native port |
| Membership+fresh context | Public Object вне страницы denied; JSON clone/late proof denied; unpublish/unlink during callback bytes discarded |
| Проверка ловит исходный дефект | Intentional owner-impersonating/CSS-only projection раскрывает private refs/geometry и тот же privacy oracle падает |
| Durable failure | Реальный SQLite receipt trigger откатывает config; тот же retry проходит после снятия fault |
| Bounds | Chunk≤512KiB/current cursor; Root malformed/spoofed query/part/limit/after/wait rejected |

Тесты: `modules/canvas-pages/test.mjs`, `modules/apps/test/public-canvas-authority.test.mjs`, `server/test/canvas-pages-root.test.mjs`; World profile/Field v1/v2 regression suites дополнительно. Local synthetic fixtures, без production. Backend tests не подтверждают браузерный renderer/пагинацию/доступность, SEO, нагрузку, multi-root fanout, backup erasure или независимые app Source servers.
