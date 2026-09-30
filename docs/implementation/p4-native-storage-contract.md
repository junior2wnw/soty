# P4-B1 — Notes / Capabilities native storage contract

Статус: **предложение для review, production DDL не изменён**. База сравнения — `6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e` (P4-A). Дата: 2026-09-30. Область — только хранение первого `notes.createDraft@1` и стык будущего B2. OAuth, MCP sessions, произвольные executors и новый Notes read scope здесь отсутствуют.

## 1. Решение и найденные несовпадения

Добавить по одной доменной таблице в Notes и Capabilities. Notes хранит постоянное доказательство первоначального создания; Capabilities — заранее закреплённую связь Invocation с конкретным Notes store и стабильными IDs. Invocation, dispatch intent, budgets и итоговые receipts остаются в существующей Capabilities DB. Отдельного native job queue или permission store нет.

| Текущий источник | Следствие для B1/B2 |
|---|---|
| Notes `put`: deleted проверяется до replay; обычные receipts ограничены 32 на note | Нельзя использовать обычный receipt или `notes.get` как постоянное доказательство создания. Новый proof не зависит от изменяемого текста и переживает purge |
| Notes `execute` требует человеческий account/device actor | Native API должен быть отдельным внутренним create-only методом; не передавать придуманный deviceId и не расширять общий `executeForAccount` |
| Caps `notDispatched`: pending + no job + no effect считается не начатым | Нельзя впервые ставить dispatching в транзакции, которая ещё может откатиться после Notes COMMIT. Нужен отдельный durable marker COMMIT **до** эффекта |
| Caps `recordResult` не очищает `input_json`; read projection скрывает его | Второй экземпляр текста реально остаётся в Caps. Native terminal receipt, settlement и purge input должны коммититься вместе |
| Caps `admit` вызывает invoke authorization с executionEnabled до key lookup | Lost ACK + operational disable прячет результат от клиента, который ещё не знает Invocation ID. Native replay сначала проходит current read ACL/key/fingerprint, потом только для нового intent проверяет execution readiness |
| Caps schema v1 проверяет lineage, Notes v1 ещё quick_check; оба меняют WAL pragmas до отказа неизвестной версии | v2 recognizer должен распознавать формат до persistent mutations. Код ошибки old reader сам по себе не доказывает отсутствие записи |
| Caps v1 не имеет project/store identity | v2 получает `project_id` и `registry_id`; Notes получает `registry_id`. Отсутствующий обязательный meta ключ в v2 — повреждение, не повод сгенерировать новый |
| Master §12 описывает текущую отметку deleted/unavailable при replay, preflight §4 запрещает create-only existence probe | В этом срезе выдаётся только исторический факт создания/revision 1. Текущее наличие и права проверяет PWA по ссылке; внешний metadata scope не добавляется |

Зафиксированный semantic digest остаётся `95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204`. Input `{title,body}`, output `{noteId,revision}`, ресурсы `notes:new`, эффект `create`, получатель `soty:notes`, native handler/version остаются прежними. Изменение schema/digest или трактовки UTF-16 не является частью миграции хранения.

## 2. Reader-before-writer и независимый bootstrap

План совместимости состоит из двух разных baseline:

1. Host manifest/probe/receipt v3 сначала распознаёт Notes `[1]`, Capabilities `[1]`, Rooms `[1,2]`, Apps `[1,2,3,4,5,6]`. Это ещё не разрешение на v2.
2. Следующий application baseline умеет **читать и обслуживать v1/v2**, но по умолчанию не мигрирует v1. Он должен реально стать serving old container. Лишь после этого отдельное явное migration admission разрешает v2 записи. Rollout восстанавливает тот же old container/image; произвольный заранее собранный fallback с правильной label не подменяет его автоматически.

Предлагаемый конструкторный контракт обоих stores: `allowNativeMigration=false`, строгий boolean. Экспорт `schemaVersion` сообщает фактическую открытую версию, а `supportedSchemaVersions=[1,2]` — возможности reader. Обычные Notes/access операции продолжают работать на v1. На v1 native admission/execution недоступны независимо от наличия capability metadata. На v2 `allowNativeMigration=false` не понижает формат, не очищает proof и не отменяет восстановление уже совершённого эффекта.

Schema API: `migrateNotes(db, projectId, {allowNativeMigration=false}={})` и `initializeCapabilitiesSchema(db, {projectId, allowNativeMigration=false})` возвращают `{schemaVersion, registryId: string|null}`. Новый Capabilities constructor принимает обязательный trusted `projectId`; host передаёт тот же project, что Notes/Connect. Это явная внутренняя API правка с обновлением fixtures, а не default project, выведенный из клиента или пути. Конфигурация валидируется до mkdir/open. Значение null допустимо только для фактического v1, где registry identity ещё не существует.

Совместимый B1 baseline ещё не обязан иметь B2 effect executor. Но уже обязан отличать native intent: исключить его из generic job dispatch, не выдавать no-effect/released по обычному pending shortcut и сохранять unresolved/input/proof при отключённой функции. Если reconciler в этом baseline ещё отсутствует, восстановление остаётся pending до совместимого B2/recovery; это честная остановка, не завершённый reconcile. Reader compatibility сама по себе не означает native execution readiness.

| Открываемое состояние | default false | explicit true |
|---|---|---|
| Настоящая пустая DB, user_version 0, нет пользовательских объектов | создать точный v1 | создать v1 + additive v2 в одной транзакции этого store |
| Точный исторический v1 | открыть v1 без миграции | распознать, затем атомарно мигрировать этот store в v2 |
| Точный v2 | проверить, открыть v2 | проверить, открыть v2; IDs прежние |
| Unknown version, partial schema, неверный marker/project, отсутствующий v2 registry_id | отказ до persistent pragmas/DDL | тот же отказ; не чинить автоматически |

Новые markers: `soty.notes.sqlite.v2` / `soty.capabilities.sqlite.v2`, user_version `2` у каждого. В существующих meta tables сохраняются прежние значения, кроме явно обновляемой lineage; добавляются `registry_id` (32 lowercase hex, random 128 bit, генерируется ровно один раз внутри migration transaction), а в Capabilities также `project_id` из доверенной конфигурации. Notes project_id уже существует. Format version не равен версии capability.

Возможен crash после Notes2, до Caps2, и обратная смешанная пара. Это поддерживаемое состояние запуска совместимого baseline; обычные функции работают, native создаёт отказ до готовности **обоих** stores. Повтор migration завершает только оставшийся store. Не используется внешняя «общая» отметка successful bootstrap и не обещается общий COMMIT. ID не выделяется снаружи до транзакции и не регенерируется после reopen.

Caps v1 не содержит доказательства project identity. Первое добавление project_id — явное связывание установленного trusted data directory с конфигурацией, а не восстановление якобы существовавшей metadata. Перенос исторической DB из неизвестного проекта этим не проверяется. Cold backup/restore должен включать согласованный комплект Connect/Caps/Notes. Store IDs обнаруживают новую/другую DB, но не различают две разновременные копии **того же** store.

До любой миграции host проверяет весь комплект форматов read-only. Затем каждый migrator повторяет проверку под своим `BEGIN IMMEDIATE`, создаёт только свои объекты/metadata, проверяет postconditions и коммитит. Неизвестные схемы распознаются до `journal_mode` и других persistent изменений. Конечный отказ вследствие конкуренции не должен маскироваться как пустая DB.

## 3. Предлагаемый точный additive DDL Notes v2

Все v1 таблицы, FTS5 virtual/shadow tables, индексы и данные сохраняются. Их SQL не переписывается для косметического STRICT. Новая таблица:

```sql
CREATE TABLE note_native_creates(
  source_store_id TEXT NOT NULL
    CHECK(length(source_store_id)=32 AND source_store_id NOT GLOB '*[^0-9a-f]*'),
  invocation_id TEXT NOT NULL
    CHECK(length(invocation_id) BETWEEN 1 AND 160
      AND invocation_id NOT GLOB '*[^A-Za-z0-9_.:-]*'),
  account_id TEXT NOT NULL,
  note_id TEXT NOT NULL
    CHECK(length(note_id)=66 AND substr(note_id,1,2)='n_'
      AND substr(note_id,3) NOT GLOB '*[^0-9a-f]*'),
  mutation_id TEXT NOT NULL
    CHECK(length(mutation_id)=66 AND substr(mutation_id,1,2)='m_'
      AND substr(mutation_id,3) NOT GLOB '*[^0-9a-f]*'),
  input_digest TEXT NOT NULL
    CHECK(length(input_digest)=64 AND input_digest NOT GLOB '*[^0-9a-f]*'),
  capability_digest TEXT NOT NULL
    CHECK(capability_digest='95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204'),
  revision INTEGER NOT NULL CHECK(revision=1),
  created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),
  PRIMARY KEY(source_store_id,invocation_id),
  UNIQUE(account_id,note_id),
  UNIQUE(account_id,mutation_id),
  FOREIGN KEY(account_id,note_id) REFERENCES notes(account_id,id)
) STRICT;

CREATE TRIGGER note_native_create_no_update
BEFORE UPDATE ON note_native_creates BEGIN
  SELECT RAISE(ABORT,'notes_native_proof_immutable');
END;
CREATE TRIGGER note_native_create_no_delete
BEFORE DELETE ON note_native_creates BEGIN
  SELECT RAISE(ABORT,'notes_native_proof_immutable');
END;
CREATE TRIGGER note_native_create_no_replace
BEFORE INSERT ON note_native_creates
WHEN EXISTS(SELECT 1 FROM note_native_creates
  WHERE (source_store_id=NEW.source_store_id AND invocation_id=NEW.invocation_id)
     OR (account_id=NEW.account_id AND note_id=NEW.note_id)
     OR (account_id=NEW.account_id AND mutation_id=NEW.mutation_id))
BEGIN SELECT RAISE(ABORT,'notes_native_proof_immutable'); END;

CREATE TRIGGER notes_identity_no_update
BEFORE UPDATE ON notes_meta
WHEN OLD.key IN ('project_id','registry_id') OR NEW.key IN ('project_id','registry_id')
BEGIN SELECT RAISE(ABORT,'notes_identity_immutable'); END;
CREATE TRIGGER notes_identity_no_delete
BEFORE DELETE ON notes_meta WHEN OLD.key IN ('project_id','registry_id')
BEGIN SELECT RAISE(ABORT,'notes_identity_immutable'); END;
CREATE TRIGGER notes_identity_no_replace
BEFORE INSERT ON notes_meta
WHEN NEW.key IN ('project_id','registry_id')
 AND EXISTS(SELECT 1 FROM notes_meta WHERE key=NEW.key)
BEGIN SELECT RAISE(ABORT,'notes_identity_immutable'); END;
```

Metadata добавляется до создания identity guards. `INSERT OR REPLACE` не является replay: сначала прочитать proof, сравнить все pins, вернуть исходный результат. Нельзя полагаться на включённый recursive_triggers для защиты REPLACE.

Proof содержит только первоначальный факт, не title/body/preview/items. `created_at` является первоначальным временем Notes commit transaction, `revision` всегда 1. Обычный `note_receipts` может содержать исходную mutation для совместимости существующих Notes internals и затем штатно обрезаться. Native proof не обрезается. FK удерживает Notes row/tombstone; штатный purge уже обновляет row, а не удаляет его. Имена Notes/его текста в proof не дублируются.

## 4. Предлагаемый точный additive DDL Capabilities v2

Все 12 v1 tables сохраняются, включая единственные budgets/receipts/dispatch intents. Native intent не является вторым Invocation. Непроиндексированные обходы `SELECT *` для quota не допускаются.

```sql
CREATE UNIQUE INDEX cap_invocations_native_identity
  ON cap_invocations(id,account_id);
CREATE INDEX cap_invocations_account_admission
  ON cap_invocations(account_id,created_at,id);
CREATE INDEX cap_invocations_principal_admission
  ON cap_invocations(account_id,principal_id,created_at,id);
CREATE INDEX cap_invocations_nonterminal
  ON cap_invocations(account_id,principal_id,created_at,id)
  WHERE status NOT IN ('succeeded','failed','cancelled');

CREATE TABLE cap_native_note_intents(
  invocation_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  notes_store_id TEXT NOT NULL
    CHECK(length(notes_store_id)=32 AND notes_store_id NOT GLOB '*[^0-9a-f]*'),
  note_id TEXT NOT NULL
    CHECK(length(note_id)=66 AND substr(note_id,1,2)='n_'
      AND substr(note_id,3) NOT GLOB '*[^0-9a-f]*'),
  mutation_id TEXT NOT NULL
    CHECK(length(mutation_id)=66 AND substr(mutation_id,1,2)='m_'
      AND substr(mutation_id,3) NOT GLOB '*[^0-9a-f]*'),
  input_digest TEXT NOT NULL
    CHECK(length(input_digest)=64 AND input_digest NOT GLOB '*[^0-9a-f]*'),
  input_bytes INTEGER NOT NULL CHECK(input_bytes BETWEEN 1 AND 262144),
  started_at INTEGER CHECK(started_at BETWEEN 0 AND 9007199254740991),
  input_purged_at INTEGER CHECK(input_purged_at BETWEEN 0 AND 9007199254740991),
  UNIQUE(account_id,note_id),
  UNIQUE(account_id,mutation_id),
  FOREIGN KEY(invocation_id,account_id) REFERENCES cap_invocations(id,account_id)
) STRICT;

CREATE TRIGGER cap_native_note_admission
BEFORE INSERT ON cap_native_note_intents
WHEN NOT EXISTS(SELECT 1 FROM cap_invocations i
  WHERE i.id=NEW.invocation_id AND i.account_id=NEW.account_id
    AND i.capability_id='notes.createDraft' AND i.capability_version=1
    AND i.capability_digest='95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204'
    AND i.job_id IS NULL AND i.input_json!='null')
BEGIN SELECT RAISE(ABORT,'native_note_binding_invalid'); END;
CREATE TRIGGER cap_native_note_no_replace
BEFORE INSERT ON cap_native_note_intents
WHEN EXISTS(SELECT 1 FROM cap_native_note_intents
  WHERE invocation_id=NEW.invocation_id
     OR (account_id=NEW.account_id AND note_id=NEW.note_id)
     OR (account_id=NEW.account_id AND mutation_id=NEW.mutation_id))
BEGIN SELECT RAISE(ABORT,'native_note_identity_immutable'); END;
CREATE TRIGGER cap_native_note_no_delete
BEFORE DELETE ON cap_native_note_intents
BEGIN SELECT RAISE(ABORT,'native_note_identity_immutable'); END;
CREATE TRIGGER cap_native_note_update_guard
BEFORE UPDATE ON cap_native_note_intents
WHEN NEW.invocation_id IS NOT OLD.invocation_id OR NEW.account_id IS NOT OLD.account_id
  OR NEW.notes_store_id IS NOT OLD.notes_store_id OR NEW.note_id IS NOT OLD.note_id
  OR NEW.mutation_id IS NOT OLD.mutation_id OR NEW.input_digest IS NOT OLD.input_digest
  OR NEW.input_bytes IS NOT OLD.input_bytes
  OR (OLD.started_at IS NOT NULL AND NEW.started_at IS NOT OLD.started_at)
  OR (OLD.input_purged_at IS NOT NULL AND NEW.input_purged_at IS NOT OLD.input_purged_at)
  OR (NEW.input_purged_at IS NOT NULL AND NOT EXISTS(
    SELECT 1 FROM cap_invocations i JOIN cap_receipts r ON r.invocation_id=i.id
    WHERE i.id=NEW.invocation_id AND i.input_json='null'
      AND i.status IN ('succeeded','failed','cancelled')))
BEGIN SELECT RAISE(ABORT,'native_note_identity_immutable'); END;

CREATE TRIGGER cap_native_note_input_guard
BEFORE UPDATE OF input_json ON cap_invocations
WHEN EXISTS(SELECT 1 FROM cap_native_note_intents n WHERE n.invocation_id=OLD.id)
 AND NEW.input_json IS NOT OLD.input_json
 AND (NEW.input_json!='null' OR NEW.status NOT IN ('succeeded','failed','cancelled')
      OR NOT EXISTS(SELECT 1 FROM cap_receipts r WHERE r.invocation_id=OLD.id))
BEGIN SELECT RAISE(ABORT,'native_note_input_immutable'); END;
CREATE TRIGGER cap_native_receipt_no_update
BEFORE UPDATE ON cap_receipts
WHEN EXISTS(SELECT 1 FROM cap_native_note_intents n WHERE n.invocation_id=OLD.invocation_id)
BEGIN SELECT RAISE(ABORT,'native_note_receipt_immutable'); END;
CREATE TRIGGER cap_native_receipt_no_delete
BEFORE DELETE ON cap_receipts
WHEN EXISTS(SELECT 1 FROM cap_native_note_intents n WHERE n.invocation_id=OLD.invocation_id)
BEGIN SELECT RAISE(ABORT,'native_note_receipt_immutable'); END;
CREATE TRIGGER cap_native_receipt_no_replace
BEFORE INSERT ON cap_receipts
WHEN EXISTS(SELECT 1 FROM cap_native_note_intents n WHERE n.invocation_id=NEW.invocation_id)
 AND EXISTS(SELECT 1 FROM cap_receipts r WHERE r.invocation_id=NEW.invocation_id)
BEGIN SELECT RAISE(ABORT,'native_note_receipt_immutable'); END;

CREATE TRIGGER cap_identity_no_update
BEFORE UPDATE ON cap_metadata
WHEN OLD.key IN ('project_id','registry_id') OR NEW.key IN ('project_id','registry_id')
BEGIN SELECT RAISE(ABORT,'cap_identity_immutable'); END;
CREATE TRIGGER cap_identity_no_delete
BEFORE DELETE ON cap_metadata WHEN OLD.key IN ('project_id','registry_id')
BEGIN SELECT RAISE(ABORT,'cap_identity_immutable'); END;
CREATE TRIGGER cap_identity_no_replace
BEFORE INSERT ON cap_metadata
WHEN NEW.key IN ('project_id','registry_id')
 AND EXISTS(SELECT 1 FROM cap_metadata WHERE key=NEW.key)
BEGIN SELECT RAISE(ABORT,'cap_identity_immutable'); END;
```

Итого proposed delta: Notes +1 table/+6 triggers; Caps +1 table/+4 explicit indexes/+11 triggers. SQL guards защищают критические application invariants; это не защита от OS/DB administrator, способного удалить guards. Существующий v1 SQL и constraints не объявляются внезапно более строгими.

Source registry ID берётся из cap_metadata. Existing `request_digest`, `internal_request_id`, authorization snapshot, resource/effect/recipient pins и reservation остаются в cap_invocations. Новый код при admission проверяет точный native binding, не просто `kind='native'`. Generic `beginDispatch/bindJob/markUncertain/recordResult` не должны позволять обработать native-note intent через connector job. Нативное completion использует приватный transaction-core тех же ledger/budget функций, без вложенного `BEGIN` и без второго DB.

### Row invariants при reopen и COMMIT

- Native row имеет ровно один matching Invocation/account, обязательный dispatch intent и reservation; нет job_id. `input_digest` совпадает с canonical `{title,body}` пока input retained. Размеры и идентичности проверяются без печати текста.
- `started_at IS NULL` допустимо только до native attempt или при terminal cancellation до него. Ненулевой started_at означает долговечную возможность эффекта, даже если receipt отсутствует.
- Nonterminal row сохраняет полный input и `input_purged_at=NULL`. Terminal native row имеет cap_receipt, окончательный settlement, `input_json='null'`, ненулевой input_purged_at и согласованные effects. Успешная native запись — ровно один created Notes effect/revision1 и spent=1.
- Terminal native `cap_receipts.digest` равен `canonicalHash({status,effectState,effects,receipt,disposition,actualCharges})`: для `succeeded` `actualCharges=[{unit:'invocations',amount:1}]`, для `failed/cancelled` — `null`. Reader пересчитывает единственную норму из сохранённых полей; произвольные 64 hex или generic success с `actualCharges:null` не принимаются. Это уточнение native row invariant без изменения DDL и generic v1 completion.
- `input_json='null'` — намеренный JSON sentinel; новый native reader не парсит его как input. Input нельзя восстанавливать из текущей Notes после purge. Старые generic v1 records не backfill-ятся native IDs/proofs и не очищаются этой миграцией.
- Наличие старого Invocation с Notes capability/target само по себе не разрешает пристроить к нему новый native effect. Replay такого key сохраняет прежнюю историю; native execution отказывает `native_legacy_invocation_unsupported`.
- У Notes native proof есть matching Notes row/tombstone и неизменные IDs/revision1. Текущие revision/state/title могут отличаться. Отсутствующая строка proof на месте ожидаемого уже committed эффекта — corruption/recovery incident, а не новый create.

Recognizer сравнивает точные known DDL/индексы/guards и обязательную metadata, затем bounded iteration проверяет row invariants. v1→v2 не превращает malformed v1 в исправную v2. Quick/FK check полезны дополнительно, но не заменяют schema checks. Runtime SQL содержит атомарные проверки даже после успешного startup; reader не обещает content integrity по одним именам таблиц.

## 5. Stable IDs и внутренний API B2

`inputDigest = canonicalHash({title,body})` использует существующий capability profile без normalization. `identityDigest = canonicalHash(['soty.native-note.v1', capRegistryId, invocationId])`; `noteId='n_'+identityDigest`, `mutationId='m_'+identityDigest`. Оба ASCII, 66 символов и допустимы для существующего Notes `id()`. Все значения записываются **в admission transaction** вместе с invocation/reservation/dispatch intent и exact Notes registry ID. Они не вычисляются заново из client key после restart. Cross-DB operation key — `(capRegistryId, invocationId)`; client key остаётся в прежнем hashed account+client namespace.

Native request fingerprint использует тот же canonical JSON и SHA256 прежнего envelope `{capabilityId,version,capabilityDigest,input,target,resources,effects,recipients}`. Единственный fixed helper `nativeNoteRequestDigest(input)` ограничивает сам input прежними 262144 B, а envelope — 294912 B (input + 32768 B метаданных); caller не выбирает target/contract/resources. Это не расширение input и не изменение generic `canonicalHash`. Например input 262072 B и Notes document 262131 B дают envelope 262359 B: служебные поля не должны сделать допустимую native row нечитаемой. Reader2 и будущий B2 admission используют один helper.

Предлагаемый узкий host-only стык:

```ts
type NativeNoteDescriptor = Readonly<{
  projectId: string; sourceStoreId: string; notesStoreId: string;
  invocationId: string; accountId: string;
  noteId: string; mutationId: string;
  inputDigest: string; capabilityDigest: string;
}>;
type NativeCreateProof = NativeNoteDescriptor & {
  revision: 1; createdAt: number;
};

// Notes service: constructor callback is host-owned, never HTTP/Connect args.
verifyNativeContext(token: object, mode: 'create' | 'reconcile'): NativeNoteDescriptor;
notes.native.storageIdentity(): {projectId: string; registryId: string; schemaVersion: 2};
notes.native.createDraftForInvocation({context, input: {title, body}}): NativeCreateProof;
notes.native.readCreateProof({context}): NativeCreateProof | null;

// Caps native coordinator: fixed Notes handler only, not a pluggable execute(fn).
nativeNotes.admit({actor, idempotencyKey, input}): {reused: boolean; invocation: Invocation};
nativeNotes.beginAttempt({invocationId}): {started: boolean};
nativeNotes.execute({invocationId}): {invocation: Invocation};
nativeNotes.reconcile({invocationId}): {outcome: 'committed' | 'not_applied'; invocation: Invocation};
```

Эти методы — внутренний контракт будущего B2, не новые публичные операции B1. Native capability остаётся disabled в B1. Ни один метод Notes не принимает caller-selected actor/account/device; account находится только в проверенном descriptor. Optional `verifyNativeContext` по умолчанию отсутствует и закрывает native methods. Caps выдаёт opaque context из WeakMap только внутри текущего синхронного fence/transaction; JSON copy, прежний context после finally, чужой store и token другого mode отвергаются. Notes проверяет token на входе и непосредственно перед commit; create-mode повторно проверяет живую authority, reconcile-mode вообще не создаёт note. Callback обязан вернуть exact frozen descriptor синхронно, Promise/thenable не принимается. WeakMap — process capability, не сохранённое доказательство после restart; восстановление строится из DB records заново.

`createDraftForInvocation` сначала ищет proof по operation key и сверяет account/store/IDs/digests. При match возвращает его до любых проверок текущего Notes state и mutable quota. Если proof нет, existing noteId/mutationId не присваивается операции: конфликт вместо overwrite. Новая записка — active/plain/не pinned/пустые items, expectedRevision0. Domain insert, FTS, account quota/counters, обычная creation mutation при её использовании и native proof коммитятся одной Notes transaction. Тело проходит те же Notes document limits; sibling concurrent writes сериализуются самой Notes DB.

`readCreateProof` не вызывает notes.get, не читает mutable body/title/items, не подтверждает текущее существование для external caller. Reconcile разрешён внутреннему coordinator после revoke, чтобы записать уже совершённый эффект и расход. Это не даёт отозванному клиенту status/read: внешняя проекция всё равно проходит текущий existing account/client/principal/grant ACL.

`storageIdentity()` на v1 сообщает controlled native unavailable, а обычное `schemaVersion` остаётся 1. Пока обоим stores не известны matching trusted project и Notes registry ID, создание недоступно. Подмена Notes другой DB — `native_store_mismatch`; отсутствие proof в такой DB **не** доказывает отсутствие эффекта.

## 6. Последовательность COMMIT и reconciliation

1. **Caps admission COMMIT**: живая авторизация, exact request digest, replay-first, quota/rate/storage bounds, budget reserve, Invocation, pending dispatch intent и native IDs. Нового Notes эффекта ещё нет. Replay проверяет текущий opaque actor/read ACL, account+client key, прежний principal/grant scope и полный fingerprint **до** executionEnabled, новых quota/rate/storage checks. Поэтому duplicate terminal возвращает прежний receipt, а duplicate unresolved — прежний Invocation без dispatch при выключенном handler. После revoke внешний replay закрыт. Нельзя реализовать это простым вызовом нынешнего invoke-first `invocations.admit` без его узкого transaction-core изменения.
2. **Caps begin-attempt COMMIT**: короткая current authorization проверка под host authority fence, cancel check, `started_at` при первом входе и `cap_dispatch_intents.state='dispatching'` **в одной транзакции**. Если marker не удалось коммитить, к Notes не обращаться. Повторный вызов marker не меняет время/IDs. Marker означает «мог начаться», не «выполнен».
3. **Connect authority fence → Caps `BEGIN IMMEDIATE` → Notes transaction**. Вначале matching proof lookup. Если proof уже есть, только settlement; для нового эффекта — повторная проверка credential, всей grant ancestry, creator devices, текущего contract/binding, cancel и expiry. Caps lock держится через Notes COMMIT. Все writers, способные выполнять native эффект, используют этот порядок. Сети/await внутри нет.
4. **Notes COMMIT** создаёт note и proof. После него effect уже реален даже при падении процесса/ошибке Caps.
5. **Caps receipt COMMIT**: proof pins проверены, `{noteId,revision:1}` проходит `catalog.validateOutput` и доменный ID check; один `created` effect, receipt `verificationMethod:'domain_read'`, artifact Notes ID/revision1, spent=1, status succeeded, body purge. В `cap_receipts` сохраняется прежний content-free receipt format; result/openUrl вычисляются из его artifact, не из текущей Notes. Ни новый результат, ни original body не нужны в отдельной таблице.

До Notes commit повторная authority/clock проверка обеспечивает точку допуска; wall-clock deadline не превращается в физический deadline fsync. Concurrent revoke либо линеаризован перед допуском и запрещает эффект, либо после уже допущенного commit. Same-process check без Connect write fence не покрывает отдельного writer. Короткий busy timeout предлагается 100 ms **на каждую** из трёх блокировок; это не общий 100 ms SLA. Busy/I/O/ambiguous COMMIT не переводится в «эффекта нет». Ключ и held budget сохраняются.

После durable marker обычные cancellation/revocation shortcuts `pending/no-job` не применяются. Это относится ко **всем** generic `requestCancel`, `reconcileAuthorization`, `recordResult` и settlement путям, не только к новому executor. До marker cancellation может завершиться без эффекта, но в native final transaction также нужны receipt и input purge. `peekDispatch` для generic job worker исключает native intents; native recovery читает bounded отдельную проекцию тех же dispatch intents, а не новую competing queue. Native reconciler под тем же сериализующим порядком и в matching Notes store читает proof:

- Есть exact proof: окончательный committed/spent, даже если грант уже отозван или пришёл cancel. CancelRequested не означает отмену совершённого создания.
- Proof отсутствует и Notes read действительно завершён: при отмене/окончательном отказе можно atomically записать failed/cancelled + no effects + released и purge. Отсутствие должно проверяться, пока Caps fence исключает конкурирующий native create; результат из прежнего чтения нельзя использовать позже.
- Proof отсутствует, допуск живой: `reconcile` сам не создаёт. `execute` может повторить тот же intent после свежей проверки, прежними IDs.
- Busy, malformed proof, другой registry, потерянная DB или ошибка чтения: unresolved/held, без возврата лимита и без нового create.

Если output validation падает после Notes commit, не сообщать no effect и не возвращать budget. Сохраняется durable marker; следующий reconcile должен подтвердить факт по Notes proof или оставить incident неизвестным. Не нужны connector job, lease или shell.

Terminal failed/cancelled после доказанного no-effect не переоткрывается при восстановлении grant, новой credential, пополнении quota или enable handler. Exact retry возвращает тот же terminal факт. Возможность повторного исполнения при отсутствии proof выше относится только к **nonterminal** intent. Для новой сознательной попытки нужен новый client key, а прежний остаётся в ledger. Terminal historical success также не требует текущего Notes existence probe для выдачи уже сохранённого receipt.

## 7. Retention, quotas и bounded SQL

Native proof, native identity, immutable final receipt и request-key ledger в пилоте не удаляются автоматически. Notes lifetime identities 10 000/account сохраняет tombstones и ограничивает число успешных заметок; он не ограничивает failed Invocation. Новые admissions получают отдельные пределы из preflight:

| Объект | Pilot default |
|---|---:|
| Nonterminal Invocation на principal / account / глобально | 4 / 16 / 128 |
| Новые native admissions за скользящие 60 секунд, principal / account | 10 / 30 |
| Ledger identities account / глобально | 10 000 / 100 000 |
| Canonical input bytes | 262 144; дополнительно Notes document ≤262 144 |
| Notes active+archived+trashed / accountBytes / lifetime identities | существующие 1000 / 16 MiB / 10 000 |
| Recovery page | 16 identities; последовательные sync операции, без массива body |

Все admission проверки находятся в той же Caps transaction, что reservation+insert. Existing key+same request проверяется перед new-admission limits; повтор не расходует slot/rate/budget. Read/reconcile/revoke/cancel и человеческие Notes операции не блокируются исчерпанием native ledger capacity. Новый child не обходит account cap или общий root budget. Reject-before-admission не создаёт failed ledger row; HTTP request flood требует отдельного ingress bound в B2, здесь это не обещается.

Считается существующий ledger/nonterminal scope, включая исторические records. Миграция не удаляет данные и не объявляет store повреждённым из-за превышенного нового policy limit: новые admissions закрыты, read/recovery остаются. Ограничения конфигурируются положительными целыми не выше pilot defaults; снижение не меняет формат. Rate query использует `created_at >= now-60000` **без** верхней границы `<=now`, чтобы часы назад не исключали уже записанные будущие timestamps. Это консервативный rate limit, не доверенный глобальный clock.

Quota queries используют covering indexes и bounded `LIMIT threshold+1`, не загружают input/authorization_json. Новый partial index совпадает с фактическим terminal predicate. Lifetime global count идёт по существующему узкому ID index. Приёмка включает EXPLAIN QUERY PLAN и populated boundary fixture, а не только маленькую пустую БД.

Input очищается только вместе с final receipt/settlement: `input_json='null'`, `input_purged_at=timestamp`. Неопределённое завершение сохраняет текст. 128 unresolved ×256 KiB означает не более 32 MiB **нового native dispatch input**, а не RAM/disk ceiling всей системы. Legacy input, metadata, FTS, journal и backups в эту оценку не входят. Удаление из live tables не обещает мгновенного удаления из WAL, свободных страниц или backup; отдельная encrypted retention/restore policy остаётся обязательной.

## 8. Unicode / native transport admission

Решение — **явный отказ непредставимого текста на native admission**, а не изменение pinned input validator. Перед новой durable admission фиксированный Notes transport:

1. Принимает UTF-8 JSON с bounded envelope; malformed UTF-8 не заменяется U+FFFD. Затем обычный @1 validation, без schema patch.
2. Проверяет title/body на well-formed UTF-16: каждый high surrogate имеет парный low, одиноких low нет. Нарушение — `invalid_unicode` до reservation/dispatch, без echo тела в ошибке/логах. Никакой normalization, `toWellFormed()`, truncation или replacement.
3. Проверяет Notes document с серверными defaults; слишком большой итоговый документ — прежний `notes_note_too_large` до admission. Mutable Notes account quota повторяется внутри Notes transaction.
4. Same validation выполняется в native coordinator/domain boundary, поэтому HTTP/MCP/внутренний caller не обходят её разными adapters. Но старый generic catalog validator и его unit sentinels сохраняются.

Runtime length остаётся UTF-16: 80 emoji в title допустимы, 81 нет; NFC/NFD не уравниваются, digest различается. Новый sidecar перед B2 enable точно описывает externalWriteAdmission и Notes storage constraint; public revision меняется, semantic digest — нет. Если нужен иной бизнес-контракт с Unicode-scalar length или сохранением произвольных UTF-16 code units, это отдельная version/storage encoding задача. @2 заранее не добавляется. Обязательное совместимое утверждение для клиента: schema-valid не отменяет явный transport/storage admission отказ.

Существующий display preview использует `.slice(0,180)` и может разрезать валидную пару. B2 native/domain helper должен сохранять границу пары при формировании preview (например 179 ASCII + emoji), не менять исходные title/body и не нормализовать их. Это производное отображение, не новый business maxLength. Данный read-only этап этого кода не меняет.

Материальный read-only probe этого анализа: Node24.13.1/SQLite3.51.2, только `:memory:` и `SELECT hex(?), ?`, без таблиц/файлов. Catalog@1 принял `\uD800` и `\uDC00`, binding вернул hex `EFBFBD`/U+FFFD, сравнение с исходной строкой false. `😀`, `é`, `e + combining acute` сохранились точно. В исходниках Node24.13.1 string binding использует `Utf8Value` → `sqlite3_bind_text`; это конкретный механизм, не предположение, что любая SQLite хранит UTF-16 таким образом. [Node source](https://github.com/nodejs/node/blob/v24.13.1/src/node_sqlite.cc#L2015-L2018)

JSON grammar допускает escaped unpaired surrogates, а RFC предупреждает о неустойчивой интероперабельности таких значений. Поэтому «JSON распарсился» недостаточно для точного текстового эффекта. [RFC 8259 §8.2](https://www.rfc-editor.org/rfc/rfc8259.html#section-8.2)

Отдельный byte sentinel: `title=''`, body=`'中'.repeat(87371)+'x'.repeat(9)` — canonical input 262 144 B, но Notes document с defaults 262 203 B. Это должно дать осмысленный отказ **до** создания Invocation, а не silently truncate body или изменить @1 maxBytes.

## 9. Исторические fixtures / recognizer / runtime gate

Historical pin один: `6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e`. Не генерировать v1 новым migrator и не ставить user_version1 поверх v2. Фикстуры должны включать exact Git bytes, provenance manifest и SHA256:

| Git path | SHA256 exact LF blob |
|---|---|
| modules/notes/server/schema.mjs | `da75ffc702fb0ee4db9878780d374f53bca0d68b46e91eafb56038b45d1e0dfa` |
| modules/notes/server/index.mjs | `07de01d28eeecf1d575684784e6dae5b60c4df7caafb1acfe22907dcad6287bd` |
| modules/notes/server/validation.mjs | `48181ad5a4a5b516d21ff40d41f76bedd3b9d8e189977ed4b84267b4e4a9ca0e` |
| modules/capabilities/server/schema.mjs | `959fdb4938d6b0ebc9359817b001f0eed08be70e437083648e9ac259c56229ad` |

Оба historical schema modules self-contained; этого достаточно для настоящего old-migrator refusal. Три Notes files нужны для исторических service-effect fixtures. Для Caps historical service fixture копируются все реальные relative imports того же commit с manifest, а не подставляется новый index/validation. Native миграционный seed содержит реальные v1 Notes active/trashed/deleted, FTS/account counters, обычные receipts и Caps clients/principals/grants/reservation/Invocation/receipt; migration не сочиняет native proof для старых строк.

Trusted probe использует только pinned host code + SQLite built-ins, обычный read-only WAL, не immutable и не candidate app imports. Format recognizer проверяет отдельные Notes/Caps marker/version, точные known table projections, критические indexes/guards и FTS shadow inventory. Это format gate, не полный integrity scan или cross-DB effect verifier. Absent main только явно empty, orphan WAL/zero/corrupt/symlink/unknown не empty. Не выводить rows/body/token/meta ID в публичном receipt: нужны store kind/version/compatibility, не приватное содержимое.

WAL является частью persistent state; даже ATTACH в WAL не даёт atomic commit набора DB. Именно поэтому нужен proof/reconcile, а live копирование отдельных файлов не подтверждает согласованный backup. [SQLite WAL](https://www.sqlite.org/wal.html)

Локальный system Node24.13.1 содержит SQLite3.51.2. SQLite сообщает WAL-reset race до 3.51.2 и исправление в 3.51.3+, с отдельными backports. [SQLite WAL-reset bug](https://www.sqlite.org/wal.html#the_wal_reset_bug) Root сообщил установку изолированного Node24.21.0/SQLite3.53.4; это не изменение production image и не завершённая приёмка в рамках этого документа. B1/B2 migration/concurrency gates должны записывать фактические Node/SQLite versions исправленного runtime и exact image. Проверка версии не заменяет tests или compatible old container.

Нужны actual historical migrator attempts на v2-main и v1-main+committed-v2-WAL с byte hashes main/WAL до/после. Дополнительно DELETE-mode fixture: legacy мигратор меняет journal_mode до отказа, поэтому нельзя заранее обещать byte-unchanged для этого случая. Если historical code действительно пишет, фиксируется incompatibility и trusted guard не допускает его запуска; этот отрицательный результат не «лечится» переписыванием historical fixture.

## 10. Последовательные срезы и приёмка

**B1a — formats + default-off admission.** Доменный автор: Notes schema/service version exposure, Caps schema/service version exposure, fixed native interface boundary, historical fixtures, targeted migration tests. Встроенный create ещё disabled. Freeze точных SQL/objects/row invariants после review; readers не заявляют v2 раньше этой точки. Root — configuration/host composition, без fake identity. Требуемый результат: v1-only serving baseline без автоматической migration, совместимый v2 reader, явный local migration и reopen сохраняют старые данные.

**B1b — reader/rollout compatibility.** Отдельный автор host guards: v3 bridge1, затем approved Notes/Caps `[1,2]`; Rooms2/Apps6 не ослабляются. Exact old serving container действительно совместим с v2, включая recovery/start/restart. Local test receipt не закрывает Linux RO/WAL/symlink topology, encrypted cold backup/restore, immutable full image и production admission gates.

**B1c — independent storage acceptance.** Независимый reviewer: exact historical v1→v2 main/WAL, partial/malformed schema before mutations, normal v1 behavior baseline, mixed bootstrap, stable IDs/reopen, spoofed project/store, critical constraint deletion/REPLACE, FK/tombstone и future3 rejection. Без выдачи B1 за выполненный native эффект.

**B2 effect gate, обязательный до enable:**

- Настоящие отдельные процессы: два одинаковых key, два последних root-budget slots, child конкуренция, quota boundary, concurrent human Notes write; по одному note/effect/reservation и неизменные IDs.
- Fault/crash после admission, marker, Notes COMMIT и перед/после Caps receipt COMMIT; повтор после настоящего reopen. Главный counterexample: marker сохранился, Notes сохранён, Caps final transaction откатился, затем cancel/revoke — proof найден, spent=1, no resurrection.
- Revoke creator device/root/child в другом process при удержании authority/Caps/Notes fences; expiry до последней проверки; busy на каждой границе; fake actor/token, Promise verifier, stale token после finally.
- >32 человеческих edits, archive/trash/purge; исходный create повторяется как historical result1 без текущего текста и без нового объекта. Проверить Notes proof/FTS/counters и quota после restart.
- Подменённый Notes store, несовпадающий account/hash/contract/noteId/mutationId, отсутствующий/corrupt proof: ни новый effect, ни released budget по ошибке чтения. Несогласованный restore не считать автоматически исцелённым.
- Output validation failure после Notes commit, Caps receipt INSERT/COMMIT fault, input purge failure: исходная правда восстановима; unresolved body не теряется; final live-table input отсутствует.
- Literal/escaped lone surrogates reject before admission, emoji boundary, NFC/NFD, newline/tab, CJK bytes/default overhead, exact schema digest; ошибка не содержит body/credential.
- Переполнение admission/rate/storage limits не блокирует replay, revoke, owner history, reconcile или human Notes. Большой fixture + query plan подтверждают отсутствие body scans; startup checks идут iterator, не `all()` над текстами.

Файловые зоны на будущую реализацию предлагаются отдельно root: доменная — `modules/notes/server/{schema,index,native}.mjs`, `modules/capabilities/server/{schema,index,invocations,native-notes}.mjs` и узкие tests; authority fence — root `modules/connect/server/index.mjs` + HTTP composition; reader — отдельный `deploy/connector/storage-*` автор. Это предложение владения, **не разрешение менять все эти файлы сейчас**. Никакой OAuth schema или external executor не предсоздаётся.

## 11. Что реально проверено в этом read-only срезе

Прочитаны актуальные Notes schema/domain validation/transactions, Caps schema/catalog/access/invocations/composition, master P4 и preflight §5/7. Exact historical blob hashes получены через `git show` без checkout. Выполнен только небольшой read-only SQL string-binding probe и byte-size calculation; новые DDL/reader tests не запускались, базы/production source не изменялись. Primary sources проверены 2026-09-30. Нет заявления о Linux, production rollout, crash-safe native execution или готовом внешнем API.

До реализации нужен review этого SQL и внутренних APIs; до v2 production writes — совместимый реально serving baseline и операционные gates; до executionEnabled — полный B2 effect/fence/recovery gate. Повышение reader label или отключение handler по отдельности этих условий не заменяет.
