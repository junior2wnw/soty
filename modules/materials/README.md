# Личные материалы — независимый provider единого поля

Дата: 2026-11-04. Источник задачи: текущее поручение пользователя и R04 от root. Это локальный модуль; публикация, доступ к production и загрузка личных файлов не выполнялись. Применение на живом полотне и приёмка в браузере принадлежат интеграции root.

`MaterialsService` владеет оригинальными изображениями/аудио/ограниченным WebM-видео. `NotesService` остаётся единственным владельцем записок. World chat attachments не переиспользуются как скрытый чат: оттуда взяты только безопасное имя файла и ограниченный валидатор chunk/hash. Собственный manifest — `soty.material.chunks.v1`. Поле хранит opaque refs `builtin:notes/<id>` и `builtin:materials/<id>`, координаты и метаданные представления универсального объекта. Оно не хранит body, URL, base64 или копии материалов.

```js
import { createMaterialsService } from './modules/materials/server/index.mjs';
const materials = createMaterialsService({ databasePath: '/durable-data/materials.sqlite', projectId: 'soty' });
// Подключить через host wrapper {operations, executeAsync}; не регистрировать Promise execute как sync extension.
// Actor — проверенный transport, никогда request JSON.
const result = await materials.execute({op, args, actor, guard});
await materials.close(); // ожидает уже начатые запросы и decode, закрывает SQLite
```

Каждая операция требует `args.expectedAccountId`, равный `actor.accountId`. Это защита от смены аккаунта в UI, а не способ выбрать владельца. `actor.deviceId` также обязателен. Неизвестные аргументы запрещены. Дополнительный `guard` — синхронная доверенная closure хоста, проверяющая, что подписанное устройство/аккаунт ещё действуют. Она вызывается до выполнения, после асинхронного decode и перед COMMIT. Closure не передаётся через JSON. `execute` возвращает Promise; `storageIdentity()` — фактические project/registry/schema. Для Connect нужен async host adapter `server/materials-host.js`: signed actor → synchronous revocation guard → service.execute, с повторной проверкой actor перед ответом. Сам service object не регистрируется как sync extension.

| Операция | Аргументы помимо expectedAccountId | Результат |
| --- | --- | --- |
| `materials.list` | `bucket?` active/trashed, `cursor?`, `limit?` 1–40 | `{materials,nextCursor,usage}` |
| `materials.get` | `materialId` | `{material}`; active/trashed metadata |
| `materials.begin` | `materialId,mutationId,name,kind,mediaType,size,fileSha256` | `{materialId,state,revision,replayed?}` |
| `materials.chunk` | `materialId,index,sha256,data` canonical base64 | `{materialId,index,size,sha256,replayed?}` |
| `materials.commit` | `materialId,mutationId,manifestSha256` | `{material,replayed?}` |
| `materials.status` | `materialId` | `{materialId,state,revision,received:[{index,size,sha256}],material:null\|metadata}` |
| `materials.read` | `materialId,index` | `{index,size,sha256,data}`; только active |
| `materials.trash` | `materialId,mutationId,expectedRevision` | `{materialId,state,revision,updatedAt,replayed?}` |
| `materials.restore` | то же | то же |
| `materials.cancel` | то же, pending revision=0 | то же; отменяет только незавершённую загрузку |

IDs — 8–96 ASCII `[A-Za-z0-9_-]`, первый символ буква/цифра. Hash — SHA256 lower hex. `kind` image/audio/video. Metadata: `{materialId,kind,name,mediaType,size,fileSha256,manifestSha256,chunks:[{bytes,sha256}],width,height,durationMs,state,revision,createdAt,updatedAt}`. Результат read отдельно хэшируется и сверяется с manifest на сервере и в SDK.

Upload резервирует место целиком до первого chunk. Chunk 512 KiB; файл до 25 MiB. По умолчанию: 256 MiB/2000 identities на аккаунт, 2 GiB/20000 identities на store, 8 pending/аккаунт, TTL pending 24 ч, до 32 mutation receipts/материал. Корзина продолжает занимать квоту; cancel/expiry освобождают reserved bytes и сохраняют identity tombstone. Constructor limits можно только уменьшать. SQLite WAL, FULL synchronous, foreign keys, отдельный registry/project identity. Неподходящая или чужая база не мигрируется и не перезаписывается.

List/get/read не возвращают незавершённые bytes. Успешный commit требует все chunks, file+manifest SHA, media admission и успешный durable SQLite COMMIT. Ошибка SQL или отзыв guard оставляет pending. Повторы exact mutation возвращают прежний ACK; смена payload запрещена. Trash/restore — compare-and-swap; старое состояние не перезаписывает новое. Receipt bounded; после вытеснения старый state-changing запрос с прежней revision не проходит. Для begin есть постоянный digest, препятствующий переиспользованию identity. Chunk index имеет независимую идемпотентность.

Изображения: PNG/JPEG/WebP, настоящий Sharp 0.34.5 decode, не только magic. Пределы: 8192 на сторону, 16 Mi pixels, одна страница. SVG/HTML/GIF не поддерживаются. Оригинальные bytes сохраняются неизменными. Не обещается удаление EXIF; это отдельная настройка приватности до будущего публичного sharing.

Аудио: Opus WebM и Ogg, PCM WAV. Сервер проверяет структуру контейнера и аудиодорожку: ограниченное EBML traversal, только A_OPUS без video track; Ogg single serial/sequence/CRC/Opus headers/EOS; WAV RIFF/fmt/data/frame alignment. До 600 сек. Для streaming WebM без Duration возвращается `durationMs:null`; длительность не выдумывается. Codec playback подтверждается только настоящим плеером браузера. MP3/MP4 пока не поддерживаются этим provider и не заявлены реализованными. Универсальный host может добавлять другие providers.

Видео первого scoped provider: `video/webm`, одна VP8-дорожка и до одной Opus audio. До 4096 на сторону / 4096×2160 pixels / 600 сек / общий fileBytes cap. Проверяются EBML hierarchy/track numbers, codec IDs, pixel bounds, timestamps, настоящий VP8 first keyframe signature/dimensions/partition bounds. Это сильнее magic sniff, но не полный decoder; реальный browser decode/play остаётся обязательной незавершённой приёмкой. VP9/H264/MP4 не принимаются. HTMLVideoElement имеет controls/playsInline/preload=none, bytes только explicit Play.

Primary format references: [WebM container](https://www.webmproject.org/docs/container/), [Ogg Opus RFC 7845](https://www.rfc-editor.org/rfc/rfc7845.html), [VP8 RFC 6386](https://datatracker.ietf.org/doc/html/rfc6386#section-9.1). Это собственные ограниченные admission parsers, не полные реализации кодеков.

Browser SDK:

```js
import {prepareMaterialUpload,uploadMaterial,readMaterialBlob} from './modules/materials/browser/client.mjs';
const intent = await prepareMaterialUpload(file, {kind:'image', name:file.name});
// Удерживать тот же intent для retry неизвестного результата. Не создавать новый ID на timeout.
const material = await uploadMaterial(api,{accountId,isCurrent,signal,onProgress},intent);
const {blob} = await readMaterialBlob(api,{accountId,materialId:material.materialId,isCurrent,signal});
```

SDK восстанавливает server-side upload status при retry, проверяет hashes и reconstructs Blob только после signed read. Prepared intent/Blob живёт в вызывающем UI; восстановление незавершённого выбранного файла после полного browser reload не реализовано и не заявляется. Завершённый материал открывается после reload с сервера.

`src/world/native-field-objects.ts` даёт content provider для универсального renderer host. `createObject(node)` создаёт article, возвращает `{element,update,setVisible,activate,flush,hasUnsavedChanges,dispose}`. `resolve(node)` получает свежие summary: note `{kind:'note',noteId,summary?,fontSize?}` или media `{kind:'image'|'audio'|'video',materialId,summary?}`. Host вызывает `setVisible(true)` на видимом объекте. Notes используют существующие `createNoteSession`, IndexedDB outbox и cache. Metadata refresh не меняет focused DOM/caret и dirty branch. Image chunks lazy по видимости; audio/video bytes только explicit Play. Object URLs отзываются при dispose. Shape/size/placement/mask/ACL/password принадлежат универсальному field object host, здесь нет второй модели layout.

Проверки текущей части:

| Требование | Наблюдаемый критерий / доказательство |
| --- | --- |
| Реальные bytes, сохранение | service.test: begin→chunk→commit→close→reopen→get/read совпадают byte-for-byte |
| Нет pending выдачи | get/read/list до commit закрыты; SQL fault и guard revocation не публикуют material |
| Owner isolation | Bob list пуст, get/status/read/change чужого ID отклонены; намеренно сломанный owner query обнаружен |
| Integrity / границы | неправильный file/manifest/chunk SHA, base64, truncated/SVG/MIME mismatch/oversized decoded image не коммитятся |
| Retry / quotas / trash | lost commit ACK повторяется без duplicate, reservations+TTL+trash+restore+CAS проходят |
| Настоящий inline note | native tests: signed Notes API→textarea edit→flush→provider recreate; durable offline outbox recovery |
| Caret / live refresh | focused DOM и selectionStart сохранены; свежая clean revision обновляется после blur; overwrite mutant обнаружен |
| Lazy photo/audio | fake DOM test проверяет момент IO, explicit audio action, URL cleanup; это не pixel/playback proof |
| Container fixtures | собственные синтетические 0.25s FFmpeg 8.1.1 Opus WebM/Ogg; corrupted/header-only controls отклонены |

Команда: Node 24.22.1 `--test modules/materials/test/*.test.mjs src/world/native-field-objects.test.mjs` — 21/21. TypeScript 5.9.3 `--noEmit` прошёл после добавления provider. Synthetic fixtures — только tone/testsrc2, не личные записи. Для регенерации: `node modules/materials/test/make-audio-fixtures.mjs <ffmpeg.exe>`.

Дополнение trusted source port: `createMaterialsService({verifyObjectAuthority(context,consume),...})`. Root verifier вызывает настоящий `CanvasObjectService.withObjectAuthority(context, proof => consume({providerId:proof.providerId,ownerId:proof.ownerId,ref:proof.ref,right:proof.right}))`. Только original live WeakMap-branded context внутри синхронного ObjectService read frame. `materials.native.readMetadata({context})` → `{material}`, `native.readChunk({context,index})` → verified chunk; providerId строго `soty.materials`, right строго read. Перед чтением и перед возвратом bytes повторно проверяется current authority; owner/ref из JSON claims не принимаются. Без verifier, после frame, copied/serialized context — denied. Это не дополнительные RPC operations и не выдача token/URL.

`native.withSourceAuthority({actor,materialId,ownerId?,mode:'create'|'read'},callback)` — host-only provider binding fence. Create требует own actor source; read ownerId приходит из проверенного объекта Root, не из внешнего request body. Active committed source проверяется до/после синхронного callback, включая owner/revision/hash/state. Callback получает `{allowed:true,protection:{kind:'none'}}`; Source SQL transaction не удерживается вокруг callback, поэтому Object read transaction может вызвать native read transaction. Отзыв/изменение source обнаруживается before return. Plaintext source не считается password sealed. Async callback запрещён.

`object-authority.test.mjs`: шесть tests настоящих Materials+CanvasObjectService, включая public viewer bytes без fake owner actor, ACL revoke, source trash, copied/expired contexts и намеренно сломанный JSON-claim verifier. Native provider tests дополнены немедленным scrub dirty textarea с сохранением существующего account-scoped draft. После расширений приведённая выше команда теперь проверяет 28 scenarios; полное scoped число engine+native/materials фиксируется отдельно в INTEGRATION.md.

Остаётся root integration + настоящий браузер: UI desktop/mobile, реальные кадры изображений, аудио decode/play, ввод с Android-клавиатурой, full reload/конфликт, общий universal app/video/shape/resize/hide/password/page сценарий. Эта часть R04 не объявлена завершённой по unit tests.
