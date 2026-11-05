# Защищённые объекты поля — frontend

04.11.2026. Это client/renderer/create-flow в canonical worktree, не отчёт о production release. Backend: `modules/canvas-vault/README.md`. Backend Native conversion и его erasure/activation — отдельная приёмка; поддержка DTO здесь не доказывает завершение converter.

## Подключение Root

```ts
const content = createCanvasFieldContent({
  api, accountId, isCurrent,
  projection: id => freshProjections.get(id) ?? null,
  onObjectChanged: async id => {
    // Fresh ACL-aware resolve → merge → engine.update(entities), preserving view.
  },
  allowUnlock: signedIn,
  mountApp,
});

const dialog = openCanvasVaultCreateDialog({
  api, accountId, projectId: 'soty', isCurrent,
  place: (id, size) => field.addObject(id, size),
  onDone: result => { /* saved or durable pending canonical placement */ },
  onClose: () => { /* existing dialog ownership cleanup */ },
});

await content.vault.relock(objectId); // local scrub happens BEFORE network.
await content.vault.rotate(objectId, { oldPassword, newPassword });
// Optional stable caller requestId; a durable encrypted intent already preserves it across reload.
// Existing source conversion remains backend-authorized, never a visual flag:
await content.vault.protect(objectId, expectedObjectRevision, password, { requestId });
```

`openCanvasVaultCreateDialog(options)` возвращает обычный `WorldDialog` + `flow`/`dispose`. `createCanvasVaultCreateFlow` отдельно экспортирует `startNote(input,password)`, `startFile(blob,password,{mask?,kind?})`, `resume(operationId,password)`, `listPending`, `removeLocal`, `dispose`. Поддерживает новый encrypted native Source; приложения защищаются существующим backend placement barrier, а не копируются в file provider.

Anonymous public page передаёт `allowUnlock:false`: locked article объясняет вход, password form и Source reads отсутствуют. Signed readonly page может открыть пароль, но `canEdit:false` блокирует edit/session writes; content route должен оставаться Page membership authority, Vault commands — original signed Root API. Пароль не выдаёт ACL.

## Данные и состояние

- Source → Canvas Object → canonical placement. До первой Source mutation journal обязан пройти IDB transaction COMMIT. Source/object request IDs сохраняются. После confirmed Source тело выпускается из encrypted creation journal; FieldDocument содержит только object ref/placement.
- Local DB `soty-canvas-vault-local-v1`, scope origin+project+account. Разрешены только строгие cipher records и публичный salt/profile/IDs. Заголовок AEAD связывает schema/account/project/slot/record/purpose/version. Нет plaintext тела, имени файла, MIME, пароля или CryptoKey в сохранённых records. Реквизиты создания — внутри ciphertext.
- Стандартный WebCrypto AES-GCM256, random12-byte IV на каждую версию, tag128. Локальный password key — PBKDF2-HMAC-SHA256600000, salt16; профиль согласован с актуальным [OWASP Password Storage](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html). Операции и nonextractable CryptoKey используют [W3C Web Cryptography](https://www.w3.org/TR/webcrypto/). Backend scrypt/container не менялся. Nonextractable key только RAM, derive один раз на unlock/session; строки JS/прошлые чтения нельзя форензически отозвать.
- Protected note переиспользует чистый `createNoteSession` state-machine с Vault-only API, encrypted DraftStore и RAM-only NoteCache. **Оба** default Notes disk ports переопределены. `notes.*` — только внутренние UI aliases, обычные NotesService/API/storage не вызываются.
- Exact write payload (включая original noteId/createdAt/timestamp) отдельно шифруется и COMMIT-ится до write. Его cleanup следует после post-ACK NoteSession journal COMMIT, иначе replay после reload мог бы изменить timestamp и конфликтовать с прежним requestId. Проверка с +2сек между сессиями провалилась на пересборе payload и прошла после исправления.
- Full converted note payload сохраняет original `noteId`/`createdAt`. UI имеет отдельный object-scoped synthetic ID только в RAM/encrypted draft, revision берёт из bytes DTO. `expectedRevision` и write payload.revision — VaultSource version, не историческая Notes revision. Payload.revision при write = expectedVaultRevision+1.
- Rotate сначала проверяет прежний пароль до создания durable intent, сохраняет stable requestId без паролей, затем меняет server container и атомарно переносит локальный profile/cipher records под новый ключ. Pending edits требуют сохранения; late old-key branches из гонки не удаляются, а шифруются новым ключом. Старые tabs с прежним profile не могут записать новые records. Неуспешная миграция честно остаётся pending; повтор требует прежний и новый пароль. Исторические ciphertext backups старый пароль продолжает открывать — erasure backup не заявлена.

## Чтение и UI

Любой `protection.kind=password` Native, включая original providers `soty.notes/materials`, идёт в защищённые ports и uniform `canvas.objects.content {objectId,part:'vault',index}`. Root возвращает detached512KiB chunks: `{bytesBase64,contentType,index,chunks,byteLength,revision}`. Client проверяет последовательность, лимит, единый MIME/размер/revision и current session после каждого await. Owner/ref/token не подставляются клиентом.

Locked card ничего не читает. Password submit явный; permit/TTL остаётся в RAM, reopen client требует пароль заново для local drafts. TTL/relock/revoke/account change сразу очищают editor/password fields, RAM cache, img/media src, Blob URL и runtime iframe, до медленного Root metadata fetch. Повторный unlock имеет новую generation, чтобы не оставить уже disposed provider с тем же app pin.

Images lazy по видимости; audio/video только явный Play, no autoplay. Реальный browser decode/event acceptance отдельный. Windows `video/webm` с audio-only Opus классифицируется bounded TrackEntry helper ДО journal; full ordinary Materials validation не менялась. Composer имеет MIME whitelist PNG/JPEG/WebP, audio WebM/Ogg/WAV и video WebM; HTML/SVG MIME отклоняются. Generic Vault хранит opaque encrypted bytes: MIME picker не является доказательством валидности каждого codec/содержимого, поддельный или повреждённый файл должен дать явную browser decode error, а не fake playback success.

`state:sealing, protection:none` — **защита ещё не завершена**: отдельная pending card без plaintext и password claim. Её кнопка читает свежую projection; не выдаёт pending за encrypted activation. Native conversion backend пока должен проходить собственную saga/erasure приёмку, включая прежние plaintext browser caches.

## Ограничения

Frontend single payload ≤1MiB, ≤2 chunks; Connect limit не увеличивался. Большие protected uploads не готовы. Local record≤2MiB, scope≤8MiB/64records; ошибки storage/crypto не маскируются успешным сохранением. Без secure context WebCrypto отсутствует: HTTPS или loopback localhost, иначе понятная ошибка и ноль mutations. Старый Android PASS статического прототипа не проверяет этот путь; Root планирует настоящий secure-origin Android gate.

Источник авторизации — сервер. Client state/UI flag не заменяет ACL, backend Source seal, signed actor, Page membership или Apps runtime gate. Already-read data, OS/browser password managers и привилегированный код страницы не отзываются задним числом.

## Приёмка

Node24.22.1, TypeScript5.9.3, без новых dependencies. Focused files:

| Требование | Наблюдение / проверка |
|---|---|
| Cipher local storage, no credential/key | `canvas-vault-client.test.mjs`: actual WebCrypto, nonextractable export-denied, unique IV, wrong password/tag/AAD, raw IDB records; base64-plaintext mutant fails AEAD oracle |
| COMMIT before Source | client IDB gated transaction + `canvas-vault-create-flow.test.mjs` premature Source mutant fails same committed-cipher observer |
| Stable Source/Object/placement/write | real Vault+Objects+canonical command; lost ACK/reopen/password for all stages; +2sec exact write replay |
| Password not ACL | real private Object viewer rejection; public protected readonly note no API/outbox mutation |
| Current session & chunks | wrong metadata/revision/TTL between chunks rejects whole result; no partial body returned |
| Dirty/private DOM cleanup | actual native renderer/NoteSession with real Vault, controlled DOM; TTL/relock/deny scrubs old refs, draft remains encrypted |
| Both Notes storage defaults overridden | dedicated factories throw; removing cache override provably fails renderer test |
| Protected widget live cleanup | actual field render/bridge with explicit event/iframe transport; pre-metadata relock clears frame, generation reopens; disabled subscription mutant fails |
| Original Native DTO/sealing | bridge routing tests retain original providers but choose encrypted ports; vault-only-router mutant fails; pending has no Source read/password claim |
| Composer/recovery/mobile structure | actual handlers with explicit flow/DOM ports; double submit, mismatch/format/size/no mutation, retry same intent, credential cleanup, footer outside scroller; duplicate Source mutant fails |
| Real browser / physical phone | **Root integration gate**, not implied by DOM/IDB ports or unit counts |

Command: `node --test src/world/canvas-vault-client.test.mjs src/world/canvas-vault-create-flow.test.mjs src/world/canvas-vault-renderer.test.mjs src/world/canvas-vault-create-dialog.test.mjs src/world/canvas-field-content.test.mjs`. Full conversion, browser cache purge, HTTPS/phone, production publication and arbitrary external app servers are not credited by these checks.
