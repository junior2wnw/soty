# Canvas Vault

Проверяемая backend-часть R27,04.11.2026. Новое содержимое шифруется сразу; существующие человеческие Notes/Materials переводятся через durable cipher → Source erasure → checkpoint proof → activation. Формат2 и Native migrations включаются только явно; текущий dev и production не мигрировались. HTTP/UI, браузерные кэши и выпуск проверяет Root. Capability-created Notes пока явно отклоняются: их внешний ledger требует отдельного erasure authority.

## Выбор и данные

CSS blur и encrypted копия при сохранённом открытом оригинале не защищают Source. Поэтому новый контент использует provider `soty.vault`, а converter сохраняет исходные Native IDs/bindings и удаляет открытые данные перед активацией шифрованного Source. Состояние `pending/sealing` не называется защищённым; незавершённый перенос не выдаёт парольный флаг. Без явного допуска новых форматов Native `protect` по-прежнему отвечает `vault_conversion_required`.

Vault — Source, а не вторая геометрия/Apps registry. SQLite format1 содержит owner/vaultId/revision, native или placement, immutable Object/binding snapshot только у placement, ciphertext container и immutable command receipts. Формат2 добавляет encrypted chunks/uploads и журнал conversion claims. `vault_meta.project_id` прежний, `user_version`1/2; `allowFormat2Migration=false` по умолчанию. Explicit1→2 атомарен, literal old reader отвергает2. Title/MIME/имена файлов/plaintext/password/DEK/KEK/thumbnails не сохраняются открыто. Метаданные, layout частей и random receiptKey внутри AES-GCM header. Один Source может обслуживать несколько Object instances с отдельными ACL/permit.

AES-256-GCM: случайный256-bit DEK,12-byte nonce,16-byte tag. DEK wrapped под scrypt N32768/r8/p3, salt16 bytes, maxmem64MiB. Используется общий `canvas-objects/password-container.mjs`; async crypto.scrypt выполняется вне JS event loop, очередь максимум2 active+16 waiting. KDF профиль проверен по [OWASP Password Storage](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html)04.11.2026. Синхронный primitive остаётся host helper, не endpoint.

AAD native: ownerId+canonical encrypted Source vaultId+container version+purpose(payload/key). Placement дополнительно включает SHA256(ObjectId,immutable binding snapshot) в canonical context identity; это не password hash. Write/rotate повышают version, меняют DEK и nonce; payload и wrapper связаны с новой версией. Write сохраняет salt/KEK, rotate получает новый salt/KEK. Старые ciphertext backups/WAL snapshots могут открываться старым паролем; текущая ротация не обещает стереть все исторические копии.

## Host composition

```js
let objects, vault;
objects = createCanvasObjectsService({ /* existing ports */,
  withProtectionAuthority: (request, callback) => vault.withProtectionAuthority(request, callback)
});
vault = createCanvasVaultService({databasePath, projectId,
  actorActive, withActorAuthorityFence,
  withProtectionTarget: (request, callback) => objects.withProtectionTarget(request, callback),
  withObjectAuthority: (context, callback) => objects.withObjectAuthority(context, callback),
  withRuntimeTarget: (request, callback) => objects.withRuntimeProtectionTarget(request, callback),
  placementProviders: ['soty.apps'], onChange, onInvalidate,
  allowFormat2Migration: false,
  nativeProviders: [{providerId:'soty.notes', ...notes.vault},
                    {providerId:'soty.materials', ...materials.vault}]
});
objects.registerProvider(vault.provider);
// После полной композиции: если storageIdentity().schemaVersion===2,
// вызвать reconcileConversions(). Ни один Source не стирается автоматически.
```

Передавать оригинальный Connect actor в authority fence, не скопированный tuple. `withProtectionTarget({actor:original,objectId,mode:'owner'|'read'},cb)` коротко проверяет Object+Source authority, НЕ provider.project и НЕ payload. Два таких frame окружают async KDF; SQL/fence не держится через await. `purpose:'protection'` — host-only Source authority request: Vault разрешает изменение собственного ciphertext после проверки неизменного владельца/binding, обычный read frame требует неизменной Source revision. Другие Source ACL остаются самостоятельными.

`withProtectionAuthority` стоит **после Object ACL, до provider lookup**. Locked project отдаёт стандартный lock view без binding/content; locked content не вызывает reader. Успешный unlock не меняет Object/Source permissions. В `withObjectAuthority` и после callback снова проверяются текущие Object revision, actor, Vault revision/permit. Скопированный или сохранённый context не работает.

Permit только в RAM: accountId/deviceId/ObjectId/ObjectRevision/binding/VaultRevision+TTL, DEK и wrapping key. Пароль не сохраняется; token не выдаётся и не входит в discovery. Default5min, max15min. Метаданные Object, Source version, relock, expiry и restart делают старый permit недействительным. Ключевые buffers зануляются при удалении permit и закрытии сервиса. JS строки и исторически прочитанные клиентом данные невозможно отозвать задним числом. Лимиты:4096 permits/global,8 password operations/device/object/min,1024 encrypted sources/owner,64MiB ciphertext JSON/owner,50000 receipts/owner,64 concurrent commands.

`onChange({schema,vaultId,ownerId,revision,kind,objectId?,reason,nativeBinding?,claimId?})` и `onInvalidate({objectId,ownerId?,widgetId?,accountId?,deviceId?})` содержат только host metadata. Для placement `widgetId` берётся из durable binding. Native binding должен дополнительно вызвать Objects.notifySourceChanged, чтобы обновились все прежние instances исходной заметки/файла; геометрия и Object revision не переписываются. PublicTransition всегда false. Ошибка host callback не откатывает уже durable ciphertext; hint не даёт права чтения.

## Команды и чтение

Все операции `execute` асинхронны, требуют `expectedAccountId`; mutations также `requestId`. Запросы не выбирают provider code, Source URL или owner.

| Operation | Args кроме expectedAccountId | Наблюдаемый результат |
|---|---|---|
| canvas.vault.create | requestId,password,content:{contentType,bytesBase64(base64url)} | receipt/current и binding:{providerId:'soty.vault',ref:vaultId}; потом обычный Objects.create protection=password |
| canvas.vault.protect | requestId,objectId,expectedObjectRevision,password | app placement barrier; Native alias converter только с admitted formats/providers |
| canvas.vault.convert | requestId,objectId,expectedObjectRevision,password | `{schema:'soty.vault-conversion.v1',state:'pending'|'protected',protected,claimId,vaultId,source,revision,reason?}` |
| canvas.vault.conversion.cancel | requestId,objectId | до Source erasure: отменяет claim/части, сохраняет оригинал; после erasure отказ irreversible |
| canvas.vault.status | objectId | locked и expiry собственного permit; никаких ключей/контента |
| canvas.vault.unlock | objectId,password,ttlMs?1000..900000 | scoped permit в RAM; payload/ticket/token в ответе отсутствует |
| canvas.vault.relock | requestId,objectId,scope?:device/all | device relock либо owner-only all; отменяет pending unlock |
| canvas.vault.write | requestId,objectId,expectedRevision(Vault),content | owner+unlocked+CAS; новый ciphertext, прежние permits invalid |
| canvas.vault.rotate | requestId,objectId,expectedRevision,oldPassword,newPassword | owner+current password; новая обёртка/DEK/version, content сохранён |

Create/write intents используют HMAC с random receiptKey внутри encrypted payload. В durable command intent нет дешёвого SHA(password/plaintext). Passwords rotation не записываются даже в intent: повтор того же requestId возвращает исходный receipt; новый password требует нового requestId. После rotate retry проверяется текущим newPassword; старые create retries также требуют текущий пароль для чтения protected receiptKey. Fast fingerprint concurrent request используется только в RAM и удаляется при завершении.

`vault.provider.project` после unlock возвращает `{schema:'soty.canvas-vault-view.v1',kind:'vault',contentType,byteLength,revision,canEdit}`. `readContent({context,index=0})` принимает только ephemeral branded Object context и возвращает detached ≤512KiB chunk/metadata. HTTP использует Objects.withObjectRead и передаёт только ObjectId+part/index. Root wire chunk bytes — base64url; никакого password в iframe. Shared viewers могут unlock только при свежем Object+Source grant; write/rotate остаются owner-only.

Legacy whole-content cap8MiB не равен wire cap: Connect по-прежнему ограничен примерно2MiB args (~1.4MiB binary). Новый путь поддерживает **25MiB,50 частей по512KiB**, не повышая глобальный предел. `upload.begin({requestId,password,contentType,size,metadata≤16KiB})` → vaultId/layout; `upload.chunk({vaultId,index,bytesBase64})` немедленно шифрует часть; `upload.complete({requestId,vaultId})` проверяет весь cipher manifest/AEAD и возвращает binding. Только после complete обычный Objects.create принимает Source. Chunk AAD включает owner/vault/version/index/count/size; plaintext fileSHA не хранится. Exact chunk retry idempotent, изменённый отказ. `upload.resume({vaultId,password})` после restart возвращает received indexes; lease только RAM,5min/device. `upload.cancel({vaultId})` удаляет pending ciphertext, повторяем; completed Source не отменяется. Complete receipt воспроизводится после restart без выдачи ключа или содержимого. Ограничения:8 pending uploads/owner,128 RAM leases/global,512MiB ciphertext/header storage/owner,25MiB/file. Unknown MIME не разрешает выполнение HTML в Root.

Все Notes/Materials/Vault services объявляют readonly `challengeStorage:'memory'`. Root Connect обязан применять bounded single-use RAM challenge policy: сохранённый SHA(args) иначе становится дешёвым oracle пароля или известного содержимого. Старые sensitive SQL rows/freelist/WAL требуют отдельного очистительного release gate. Actual Root tests проверяют отсутствие этих challenge rows и известных digest в DB/WAL; обычный bootstrap остаётся durable. Новые форматы не включать со старым немодифицированным Connect.

## Placement runtime gate

Soty app placement password не закрывает независимый main Source URL приложения. `vault.checkWidgetRuntime({widgetId,actor:{accountId,deviceId}})` — trusted host callback, tuple **только из проверенной Apps session/decision**. Сам JSON actor недостаточен для HTTP-входа; публичный API не публикует этот host port. Core `withRuntimeProtectionTarget` проверяет актуальный Object ACL/revision, внешний Apps path обязан отдельно проверять Source ACL/pin. Если несколько locked Objects используют один widgetId, разрешение требует всех текущих barriers; для независимых placements нужен новый widget instance.

Apps должен сохранять private instance scope в ticket/session/renewal, проверять callback до session/request/renew, после async upstream до ответа, на stream/WS обоих направлений. `apps.invalidateWidget({widgetId,accountId?,deviceId?})` закрывает старые tickets/sessions/streams/WS при protect/relock/TTL. Без этого порта Root передаёт placementProviders=[] и app protect **отклоняется**, а не притворяется защищённым. Это отдельная runtime integration приёмка; простой Vault fixture не доказывает её.

## Перенос существующих Native Sources

Notes3 (`allowVaultMigration=false`) и Materials2 (`allowVaultMigration=false`) добавляют durable Source seal/erasure receipt, сохраняя registry/source IDs. Capture callback синхронный, revision-bound, readChunk≤512KiB истекает после callback. После async KDF проверяются текущие Object/Source authority/CAS. Vault commits claim/header/cipher parts без plaintext на диск; Native erase принимает только актуальный WeakMap-branded durable cipher receipt через `vault.withCipherReceiptAuthority`. Никакой request JSON не выдаёт эту authority.

Native erase COMMIT удаляет Notes body/title/items/preview/FTS/receipts либо Materials chunks/name/fileSHA/manifest/receipts, оставляет Source ID и sealed marker. Старые owner API get/put/purge/read/chunk/commit/replay не возвращают и не восстанавливают plaintext; list/status только generic locked metadata. FTS secure-delete+optimize, SQLite secure_delete и реальный wal_checkpoint(TRUNCATE)busy0/log0/checkpointed0 нужны до confirmed receipt. Удерживаемый reader оставляет перенос pending. Vault активируется только после confirmed proof; restart reconcile завершает лишь уже erased Sources и никогда не стирает первоначальный Source без owner команды. Сбой до erasure сохраняет оригинал; после erasure durable cipher остаётся и reconcile завершает перенос. Active Native Object использует effective password gate до Source provider без переписывания immutable Object flag.

Native Source делегирует расшифровку только через текущий branded Object context/permit. Root part:`vault` возвращает прежний uniform chunk DTO; старые Native IDs/bindings работают. Converted note writeCAS и password rotate обновляют только ciphertext/Vault version; JSON note сохраняет noteId/createdAt, UI берёт revision из bytesDTO. Materials content replacement отдельная операция, не реализована. `notes_vault_external_erasure_required` честно останавливает capability-created Notes: внешний native ledger и его fingerprints ещё не принадлежат этому erasure port. Уже открытые новые readers проверяют текущий schema epoch; немодифицированные старые binaries перед миграцией обязаны быть остановлены. Известные browser caches Root должен purge/refresh отдельно. Исторические backups, чужие копии и offline clients задним числом не стираются. Unprotect/восстановление plaintext Source не реализовано.

## Проверка требований

Node24.22.1, synthetic SQLite/HTTP fixtures. Основные сценарии: `modules/canvas-vault/{test,conversion,storage}.test.mjs`, `modules/notes/test/vault-port.test.mjs`, `modules/materials/test/vault-port.test.mjs`, `server/test/canvas-vault-native-root.test.mjs`; прежние Vault/Core/Source/Root tests сохраняются.

| Обязательное свойство | Критерий и доказательство |
|---|---|
| Real encrypted content, no open copy | Vault unit + actual RootHTTP scan DB/WAL/SHM на уникальные plaintext/password/base64; Native original не создаётся |
| Current ACL + original actor | unit/device/private/revision/revoke + signed Root original/copy actor control; correct password outsider denied |
| Lock before any provider read | fixture calls=[] for locked; gate-disabled control открывает реальный app payload и тот же lock oracle падает |
| Unlock/write/rotate/relock/reopen | unit и actual Root signed full lifecycle, current password/AEAD version/data после restart |
| Failed write not success | actual SQLite trigger fault: source/revision/permit прежние, no live event; same command проходит после снятия fault |
| Async KDF + bounds | queue0/parallel1 test rejects second; timer oracle проходит async, падает на deliberately sync primitive |
| No context replay | JSON clone/late context denied; altered tag/context/Objectbinding/version denied |
| Account-isolated live hints | actual Root two current viewers + outsider silence; final removal, later private write silent |
| Native erasure и восстановление | реальные3SQLite+signed RootHTTP: все old aliases/FTS/chunks закрыты; cipher/source/activation faults, held reader, reopen/reconcile; erase-only и sealed-flag/CSS mutants проваливают тот же erasure oracle |
| Bulk service и wire | реальные25MiB/50 ciphertext parts, все50 signed RPC+HTTP reads/hash, restart/resume/complete replay;26MiB/wholeRPC превышение отказ, индексы/tags/CAS/частичный COMMIT negative controls |
| Форматы/старый reader | default1 unchanged, atomic Notes1→3/Materials1→2/Vault1→2 fault rollback, old reader refuses newformat, stale-open reader epoch guard; guard-disabled oracle fails |
| Runtime existing sessions | host contract + unit predicate; отдельные Root/R09 actualHTTP held bytes/WS/ticket/cookie/TTL tests, main app independent |
| Capability-native Notes / browser purge / bulk frontend / release | **OPEN**, explicit gate; отсутствие UI/release проверки не считается завершением R27 |

Root independently checks UI and running runtime. Tests do not establish production release, backup erasure, multi-root coherence, browser cache purge or security of external app servers.
