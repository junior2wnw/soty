# Личные записки

Отдельный Connect extension для личных текстов и чеклистов. Старые Yjs-комнаты и их ключи не мигрируются: новый интерфейс открывает их отдельной кнопкой «Записки в прежних сотах».

## Подключение

```js
import { createNotesService } from './modules/notes/server/index.mjs';
const notes = createNotesService({ databasePath: '/durable-data/notes.sqlite', projectId: 'soty' });
const connect = createConnectService({ /* existing options */, extensions: [world, apps, notes] });
// shutdown: connect.close(); notes.close();
```

`execute({op,args,actor})` — только доверенная граница после проверки подписи, аккаунта и действующего устройства в Connect. Никогда не публикуйте её напрямую и не создавайте actor из тела запроса. Владелец всегда `actor.accountId`. `expectedAccountId` обязателен во всех операциях и только обнаруживает смену аккаунта в UI; он не выбирает владельца. Неизвестные поля, включая `accountId`, отклоняются.

База обязана находиться вне заменяемого каталога модуля. WAL, synchronous=FULL, атомарные изменения текста/индекса/квот/receipt. Схема и projectId проверяются при открытии. Записи не зашифрованы сквозным ключом; сервер хранит текст, защищённый авторизацией. Старые зашифрованные комнаты сохраняют прежнюю модель.

Внутренний `notes.native` используется только фиксированным Capabilities coordinator для `notes.createDraft@1`. Constructor option `verifyNativeContext(token, 'create'|'reconcile')` — синхронная доверенная closure этого coordinator; без неё effect/proof методы закрыты. Публичные RPC и их actor-модель не меняются. `validateDraftInput({input:{title,body}})` — чистая проверка полного документа с серверными defaults; она не даёт разрешения на запись. `storageIdentity()` возвращает фактические project/registry/schema2 только для готового native store. `createDraftForInvocation({context,input})` коммитит записку, FTS/квоты и постоянный proof одной Notes transaction; `readCreateProof({context})` читает только этот proof, без нынешнего текста/существования записки. Opaque context проверяется до transaction, внутри неё и перед COMMIT. Он недействителен после синхронного frame coordinator.

Native вход обязан быть корректным Unicode; lone surrogate отклоняется без замены или нормализации. Исторический человеческий input validator не меняется. Производный preview не разрезает корректную surrogate pair на границе 180 UTF-16 единиц. Native methods не открывают новую HTTP/Connect операцию и не включают миграцию: `allowNativeMigration` по-прежнему strict boolean с default `false`.

## RPC

Идентификаторы `noteId`, `mutationId`, checklist item `id`: 8–96 ASCII символов `[A-Za-z0-9_-]`, первый — буква/цифра. Клиент генерирует UUID. `state`: `active | archived | trashed`; `color`: `plain | honey | sage | lilac | blue | coral`.

Все аргументы включают `{ expectedAccountId }`:

| Операция | Дополнительные аргументы | Результат |
| --- | --- | --- |
| `notes.list` | `bucket?` (active), `query?`, `cursor?`, `limit?` (30, максимум 40) | `{ notes: NoteMetadata[], nextCursor: string|null, usage }` |
| `notes.get` | `noteId` | `{ note: Note }` |
| `notes.put` | `noteId, mutationId, expectedRevision, title, body, items, color, pinned, state` | `{ noteId, revision, updatedAt, replayed? }` |
| `notes.purge` | `noteId, mutationId, expectedRevision` | `{ noteId, revision, updatedAt, deleted:true, replayed? }` |

```ts
interface NoteMetadata {
  noteId: string; title: string; preview: string; color: NoteColor; pinned: boolean;
  state: NoteState; revision: number; createdAt: number; updatedAt: number;
}
interface Note extends NoteMetadata { body: string; items: {id:string; text:string; done:boolean}[] }
interface Usage { bytes:number; maxBytes:number; maxNotes:number; counts:{active:number; archived:number; trashed:number} }
```

В списке нет тела или массива чеклиста; preview ограничен 180 символами. Сортировка: закреплённые, время изменения, id. Курсор привязан к аккаунту/разделу/поиску; keyset, не OFFSET. Изменения между страницами могут менять порядок, поэтому обновление списка начинает новую выборку. Статистика квот — отдельная атомарно поддерживаемая строка аккаунта. Поиск — пересечение до 8 префиксов слов (Unicode), максимум 160 символов; FTS5 содержит отдельный токен владельца, SQL дополнительно проверяет владельца. Пользователь не передаёт операторы FTS.

## Сохранение и конфликт

Создание требует `expectedRevision:0`; изменение — точную подтверждённую revision. Последняя запись не побеждает автоматически. Конфликт возвращает `notes_revision_conflict`, не меняет сервер и оставляет локальную ветку. UI предлагает сохранить копию либо открыть актуальную версию, сохраняя конфликтный черновик.

Одна mutation должна повторяться с неизменным содержимым. Последние 32 receipt на записку возвращают исходный ACK даже после более новых изменений; изменение payload даёт `notes_mutation_reused`. Старый запрос после вытеснения receipt не может перезаписать запись: CAS отклоняет прежнюю revision. Tombstone после purge препятствует воскрешению старого create; содержимое и FTS-строка удаляются. Это логическое удаление, не обещание стирания всех резервных копий/страниц файловой системы.

Квоты по умолчанию: 1000 существующих записок включая архив/корзину, 16 MiB суммарного JSON содержимого, 256 KiB на записку, 10000 созданных идентификаторов включая tombstone. Заголовок 160, body 100000 UTF-16 единиц, до 200 checklist строк × 1000 единиц; суммарный лимит всё равно применяется. Для тестов/меньшего тарифа `limits` может только уменьшать значения. Tombstone и bounded receipt не содержат текст.

## UI и локальные черновики

`mountNotes(host, {api,accountId,projectId?,initialNoteId?,openLegacy,onOpenNote?})` находится в `src/world/notes.ts`. `initialNoteId:'new'` открывает новый редактор. `onOpenNote(noteId,title?)` помогает маршрутизации. Handle: `focus`, `dispose`, `flush():Promise<void>`, `reconnect():void`, `hasUnsavedChanges():boolean`.

IndexedDB хранит только нужные локальные ветки, не весь каталог. Scope включает origin+project+verified account; ветка имеет отдельный id. Тела записок и чеклисты не попадают в localStorage; оболочка может сохранять короткое название в истории переходов текущего аккаунта. Максимум 32 ветки / 4 MiB на scope; browser quota/eviction тоже возможны. Успех фиксируется на `IDBTransaction.complete`, не `request.onsuccess`. Восстановление переносит ветку только после durable записи новой; условное удаление не удаляет ветку, изменённую другой вкладкой.

Перед сетью сохраняется outbox с исходным mutationId и expectedRevision. Потерянный ACK после commit повторяется безопасно, в том числе после перезагрузки; новые символы отправляются следующей mutation. Состояния UI: «Новая записка», «Сохраняем…», «На устройстве», «Сохранено», «Две версии», явная ошибка локального сохранения. Онлайн событие и `reconnect()` повторяют отправку текущего черновика и загружают список; оболочка вызывает `reconnect()` по единому PWA-сигналу восстановления сервера, потому что browser `online` не сообщает о перезапуске backend. Вызов после `dispose` ничего не делает. Ручной retry остаётся доступен. Полного cache всех заметок и фоновой синхронизации закрытого приложения нет; offline-start оболочки обеспечивается её service worker.

`flush` ждёт local write и текущую попытку remote save. При сетевой недоступности успешно завершается только при durable draft; при локальной ошибке отклоняется. `hasUnsavedChanges` означает, что текст ещё только в памяти. `dispose` останавливает debounce и сразу ставит актуальный draft в очередь хранения; оболочка перед reload использует `flush`, а не один dispose. `beforeunload` предупреждает только о тексте, ещё не записанном локально. Скачивание `.txt` доступно как явный экспорт.

Если восстановление связи замечено раньше завершения старого запроса, `retry()` дожидается его и делает один повтор оставшегося outbox с прежним mutationId. Одновременные сигналы присоединяются к одному `retryFlight`; повторная ошибка не запускает цикл. После `dispose` новые сетевые отправки не начинаются, а локальный черновик остаётся восстанавливаемым.

## Ошибки и проверки

Ключевые безопасные коды: `notes_account_changed`, `notes_revision_conflict`, `notes_note_not_found`, `notes_note_deleted`, `notes_mutation_reused`, `notes_invalid_arguments`, `notes_invalid_cursor`, `notes_note_too_large`, `notes_storage_quota`, `notes_count_quota`, `notes_identity_quota`, `notes_trash_required`. Browser: `notes_local_unavailable`, `notes_local_quota`. `notesErrorText` экспортирован из UI.

```sh
node --test modules/notes/test/*.test.mjs
npm run typecheck
```

Service tests проверяют изоляцию, CAS/replay/restart, очистку индекса/квот, порядок/cursor, quota rollback и реальный query plan. HTTP тест использует реальные подписанные идентичности и вторую установку. Browser-state tests используют реальный SQLite service и контролируемый транспорт, локальный store в этих unit tests — memory adapter; реальная IndexedDB/DOM проверяется в браузерной приёмке оболочки.
