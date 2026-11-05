# Универсальные объекты полотна

Локальный backend-контракт по CONCEPT.md текущего поручения Сот от04.11.2026. Создан поверх принятого fieldv2, без переноса данных/реестров Native/Apps. HTTP, UI, реальные Notes/Materials/App provider bridges и полный password unlock подключает Root; их готовность из наличия этого пакета не следует.

## Выбор механизма

Локальный widget-state удобен для отдельного прототипа, но создаёт вторую геометрию и не обеспечивает общие права/перезапуск. Перенос всех данных providers в object store создаёт вторую власть над Notes, Apps и медиа. Выбран отдельный небольшой реестр размещаемых объектов: идентичность, binding, представление, политика показа; геометрия остаётся только в каноническом fieldv2, содержимое и разрешения — у providers.

ObjectId является устойчивым instance scope. Несколько объектов могут ссылаться на один source ref. В поле используется только v2 entity:{kind:'object',id:objectId}; старый v1 по-прежнему отвергает object kind.

## DTO и команды

```js
{
 schema:'soty.canvas-object.v1',objectId,ownerId,revision,
 binding:{providerId:'native.note.v1',ref:sourceId},
 presentation:{mask:{shape:'rect'},fontSize:16},
 visibility:{audience:'private',accountIds:[],communityIds:[]},
 hidden:{owner:false,others:false},state:'active',protection:{kind:'none'}
}
```

Binding immutable; изменение источника создаёт другой object. Opaque providerId/ref ограничены128 символами A-Z/a-z/0-9/_.:-; ref не является URL, credential или HTML. Геометрия, содержимое, launch URL и session tokens в DTO не принимаются. Provider settings потребуют собственной типизированной версии SDK, а не произвольного JSON-кода в оболочке.

Операции: canvas.objects.create/get/list/update/remove/restore/project. Все подписанные вызовы требуют expectedAccountId; изменения также requestId. Create требует binding/presentation/visibility/hidden/protection. Update требует objectId/expectedRevision и хотя бы presentation/visibility/hidden; остальные поля защищены. Remove/restore — soft state, данные provider не удаляют. Owner list включает скрытые объекты для управления и восстановления.

Результат изменения: {replayed,receipt:{objectId,revision,committedAt},current}. Receipt immutable; replay возвращает исходный receipt и актуальный owner DTO. Один account не может читать/править metadata другого. CAS/receipt/object COMMIT атомарны; onChange выполняется после COMMIT и его сбой не отменяет результат. Пределы:2048 объектов и50000 команд на владельца.

Host-only notifySourceChanged({providerId,ref,ownerId}) вызывается после COMMIT собственного Source. Он ищет текущие active bindings в SQLite и передаёт content-free onChange без изменения object revision/геометрии. PublicTransition всегда false: свежие read gates определяют нынешних зрителей, а ранее видевшие могут получить конечное removal invalidation. Нельзя превращать каждую последующую приватную Source правку в публичную активность.

visibility: private/public/restricted. В restricted задаются аккаунты и/или World communities; хост проверяет право делиться в community через canShareCommunity и текущего зрителя через canAccessCommunity. Hidden.owner убирает объект с собственного поля, сохраняя управление; hidden.others исключает его для других независимо от grants. Они не меняют ACL источника.

Format2, доступный только по явному `allowFormat2Migration:true`, добавляет `accessRevision` для policy. Maintained reader читает уже известные1/2 и при false; default false сохраняет/создаёт1, где additive accessRevision консервативно равен revision. Presentation-only mask/font и повтор идентичной policy сохраняют pin; эффективные visibility/hidden/state и явный update.revokePublication:true увеличивают его. `objectRevision` слабого read context по-прежнему exact-current; `objectAccessRevision` отдельно передаёт policy pin парольному permit. Повторное «Только мне» для опубликованной соты — явный revokePublication, не автоматический эффект каждого settings save.

Host-only `registerPageSelectionAuthority({verifyContext,nativeProviders})` регистрируется один раз Root. `withPageSelectionTarget` проверяет только конкретно выбранные current binding/accessRevision без provider.project/content. `projectPageObject/withPageObjectRead` принимают opaque live Page context, проверенный WeakMap Pages, и ORIGINAL actor; сериализованная копия не даёт прав. Private Page-исключение действует только для собственных объектов статически разрешённых Native providers, с текущей Source authority и hidden.others veto; apps/widgets/чужие объекты требуют normal viewer ACL. Геометрия остаётся World.field. Locked Page-ветка после password gate может запросить bounded Source header purpose:protection, но не payload/project/decrypt. Обычные Object API от Page-публикации прав не получают.

## Providers и авторитетный read frame

Конструктор createCanvasObjectsService требует databasePath/projectId, actorActive, **withActorAuthorityFence(originalActor,cb)** и providers. Функция fence получает оригинальный выданный Connect actor; его скопированные поля не заменяют транспортное доказательство. Только внутри fence создаётся tuple для provider/context. Подписанная project не открывает вторую вложенную Connect fence.

Host-only registerProvider/unregisterProvider добавляют будущие типы без смены object schema. Входящий app descriptor не может зарегистрировать Root-код. Каждый provider — доверенный код хоста:

```js
{
 providerId:'future.kind.v1',
 withBindingAuthority({actor,objectId,objectOwnerId,ref,mode},callback) {
   // mode=create|read; fresh source permission fence, synchronous callback
   return callback({allowed:true,protection:{kind:'none'}});
 },
 project(context) { /* source-owned ephemeral JSON view, <=64KiB */ }
}
```

withBindingAuthority не создаёт внешние данные до object COMMIT; provider обязан удержать/перепроверить source authority до выхода callback. Он может вернуть allowed:false или canvas_provider_access_denied при актуальном отказе. Сетевой сбой не следует выдавать за отзыв. Асинхронный provider/fence отклоняется: network prepare делается вне этих коротких sync frames и подтверждается свежим frame перед возвратом.

Public project сначала исключает private/hidden/removed, затем ищет provider; содержимое не загружается ради последующего блюра. Внутри provider authority берётся короткий SQLite writer snapshot и заново проверяется object/policy. Context содержит schema soty.canvas-object-authority.v1, right=read, точные objectId/objectRevision/ownerId/providerId/ref, audience и реального viewer. Он маркирован WeakMap и действует только внутри callback. JSON copy/schema сами по себе разрешением не являются.

Provider Native-порта проверяет service.withObjectAuthority(context,cb) **и свой ожидаемый providerId/ref/владение source**. Он делегирует ровно эту заметку/файл, не создаёт actor владельца. App provider отдельно проверяет текущего зрителя через Apps; object visibility не публикует приватное приложение.

Host-only withObjectRead({actor?,objectId},callback) использует тот же read frame, создаёт новый context и возвращает bounded content после повторных проверок. Он принимает только ID объекта; owner/ref не берутся из HTTP. Поддерживаются detached Uint8Array и текст/JSON до1MiB (например,512KiB media chunk). Locked/denied не вызывают callback. Context от metadata projection не используется для будущего chunk. Хост отдельно ограничивает part/range и multipart/HTTP transport.

## Представление

Общий validateCanvasMask: hex/rect/ellipse/polygon; polygon3..128 точек в0..1, finite, без дублей, ненулевой площади и пересечений. Возвращает detached нормализованный контур. Font size8..128 — лишь границы хранения Native текста; реальные размеры/hit areas и доступность определяет renderer. Width/height только в fieldv2. Provider view — данные, не разрешение исполнить HTML/CSS/JS в Root.

## Пароль: что создано и что остаётся

Клиентская password-метка на plaintext source отклоняется canvas_plaintext_password_forbidden. Password принимается только когда доверенный provider в authority frame подтверждает {kind:'password',sealed:true}. App descriptor не может дать это подтверждение. Project показывает locked без чтения содержимого; withObjectRead отказывает до callback. Полного unlock в этом пакете пока нет.

password-container.mjs реализует настоящую криптографическую primitive: random256-bit DEK, AES-256-GCM, случайные12-byte nonce,16-byte tag; DEK завернут ключом scrypt N32768/r8/p3 с16-byte salt и64MiB maxmem. Этот профиль соответствует одной из текущих минимальных конфигураций [OWASP](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html), проверенной04.11.2026. AAD связывает ownerId/objectId/container version и назначение payload/key. Контекст не равен изменяемой revision метаданных. Неверные password/tag/context отвергаются. Rewrap меняет password wrapper без дешифрования payload.

Primitive ограничена16MiB, синхронная и **не подключена напрямую к HTTP**. До unlock endpoint нужны async KDF/worker, bounded parallelism/rate, свежие object/provider gates, opaque account/device/object-scoped сессия и текущая container generation. Пароль/unlock credential не передаётся app iframe. Пароли не входят в Canvas command intent: нельзя добавлять быстрый SHA(password/plaintext) как idempotency oracle.

Полный R27 остаётся открыт: durable encrypted-container provider, атомарное преобразование/удаление первоначальной plaintext копии, password recovery/rotation storage, unlock/relock/TTL и текущие archive/grants. Старые ciphertext backups сохраняют возможность открытия старым password; rewrap не является ретроактивным отзывом backup. Внешний публичный Source URL остаётся внешним публичным URL. Нельзя обещать защиту его данных паролем одного placement.

## Проверка и границы

Node24.22.1: node --test modules/canvas-objects/test.mjs modules/canvas-objects/password-container.test.mjs. Проверяются реальные SQLite/CAS/reopen/commit failure, два trusted fixture providers и новый будущий тип, отсутствие source content/geometry в object store, private/hidden before resolver, source grants, оригинальный branded actor, fresh content frames, fake/late context, lock-before-read, текущие revoke, маски; AES-GCM tamper/AAD/password/rewrap и CSS-only negative control.

Эти проверки не доказывают HTTP/UI/native source integration, multi-root fanout, release storage declarations или нагрузку. Root интегрирует и проверяет полный пользовательский путь отдельно.
