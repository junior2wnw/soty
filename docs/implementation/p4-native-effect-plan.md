# P4-B2 — один native Notes effect и доказуемый повтор

30.09.2026. Подплан для согласования после B1a checkpoint `5e459abc6afa376861c2032226bd29f78bf0468d`; это не отчёт о реализованном исполнении. Основания: [storage contract](p4-native-storage-contract.md), [B1a baseline](p4-native-storage-baseline.md), [root host plan](p4-native-host-plan.md). Production B2 source в рамках подготовки не менялся. Два обнаруженных fingerprint finding исправлены отдельно в B1a и перечислены в его receipt.

## 1. Минимальный результат

Только фиксированный `notes.createDraft@1`, digest `95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204`. Вход ровно `{title,body}`; output ровно `{noteId,revision:1}`. Target native Notes1, resource `notes:new`, effect `create`, recipient `soty:notes`, расход одна `invocations`. Нет произвольного executor, caller-selected account/device/noteId/target, Notes read/update/delete или нового DDL.

Новый запрос даёт один private Notes object и content-free исторический receipt. Повтор того же ключа с тем же намерением не создаёт новый объект, не восстанавливает удалённый и не читает его нынешний текст/существование. Новая credential той же действующей account/client/principal/grant области может прочитать собственную историю; она не заменяет исходную credential исполнения.

Публичные HTTP create/get, ingress, canonical audience и PWA link остаются у root. OAuth/MCP, UI создания нового согласия и production migration сюда не входят. Миграция default-off и operational enable независимы; окончание B2 unit tests само по себе не включает handler или reader label.

## 2. Предлагаемый точный стык composition

Все методы ниже синхронные. Они не вызываются из уже открытой signed Connect transaction. Ни callback, ни port не берутся из HTTP/Connect args. Один экземпляр coordinator принадлежит одному Capabilities service; он использует существующую connection, ledger и budget, а не второй store.

```ts
type NativeNoteDescriptor = Readonly<{
  projectId: string;
  sourceStoreId: string; // actual Caps registryId
  notesStoreId: string;
  invocationId: string;
  accountId: string;
  noteId: string;
  mutationId: string;
  inputDigest: string;
  capabilityDigest: string;
}>;
type NativeCreateProof = NativeNoteDescriptor & Readonly<{
  revision: 1;
  createdAt: number;
}>;

// New trusted Notes constructor option; absent => native effect methods closed.
verifyNativeContext?: (
  token: object, mode: 'create' | 'reconcile'
) => NativeNoteDescriptor;

type NativeNotesPort = Readonly<{
  storageIdentity(): Readonly<{
    projectId: string; registryId: string; schemaVersion: 2;
  }>;
  validateDraftInput(args: {input: {title: string; body: string}}):
    Readonly<{documentBytes: number}>;
  createDraftForInvocation(args: {
    context: object; input: {title: string; body: string};
  }): NativeCreateProof;
  readCreateProof(args: {context: object}): NativeCreateProof | null;
}>;

// Optional trusted Capabilities constructor composition, not a plugin registry.
nativeNotes?: {
  notes: NativeNotesPort;
  withAuthorityFence: <T>(action: () => T) => T;
};
```

`notes.native.validateDraftInput` — небольшой чистый preflight текущего Notes document limit, без SQL, actor и эффекта. Он добавлен к первоначальному трёхметодному port, чтобы Caps не копировал Notes defaults/limits и мог отказать **до** durable admission. Возвращает только размер, не token и не разрешение на будущую запись. Domain create повторяет проверку внутри своей transaction. Fixed input shape/Unicode/catalog validation дополнительно остаются в coordinator; одного чистого preflight для авторизации недостаточно.

Notes verifier передаётся closure, которая до завершения composition закрыта. После создания Caps она вызывает только `capabilities.nativeNotes.verifyContext`. Constructor Notes не вызывает verifier и не создаёт native effect. Публичные `operations` не расширяются внутренними methods.

Не заводить второй независимый operational флаг в coordinator: новое исполнение требует `entry.executionEnabled` принятого registry, плюс подключённый port/fence и live identities. Host выбирает этот флаг явно и по умолчанию оставляет false. Readiness использует тот же coordinator; `schemaVersion:2` или наличие grant сами по себе не означают готовность. Catalog digest/input/output schemas остаются неизменными; изменение sidecar admission/readiness metadata делает новую public discovery revision.

## 3. Coordinator API и значение результатов

```ts
type Invocation = ExistingContentFreeInvocationProjection;
type NativeOutcome = 'committed' | 'not_applied' | 'retryable' | 'held';

capabilities.nativeNotes: null | Readonly<{
  readiness(): Readonly<{ready: boolean}>;
  admit(args: {
    actor: OpaqueServiceActor;
    idempotencyKey: string;
    input: {title: string; body: string};
  }): {reused: boolean; invocation: Invocation};
  get(args: {
    actor: OpaqueServiceActor; invocationId: string;
  }): {invocation: Invocation};
  beginAttempt(args: {invocationId: string}): {
    started: boolean; invocation: Invocation;
  };
  execute(args: {invocationId: string}): {
    outcome: NativeOutcome; invocation: Invocation;
  };
  reconcile(args: {invocationId: string}): {
    outcome: NativeOutcome; invocation: Invocation;
  };
  reconcilePage(args?: {cursor?: string}): {
    items: Array<{invocationId: string; outcome: NativeOutcome}>;
    nextCursor: string | null;
  };
  verifyContext(token: object, mode: 'create' | 'reconcile'):
    NativeNoteDescriptor;
}>;
```

Точные object keys проверяются. `get` оборачивает прежнюю own projection свежей проверкой под Connect→Caps, не выдаёт body, authorization, input, registry IDs или internal request ID. `beginAttempt/execute/reconcile/reconcilePage/verifyContext` — только внутренний host API. Actorless result этих методов **не является HTTP response**. Даже после успеха root вызывает новый authorized `get` текущим actor. При revoke после эффекта internal settle разрешён, внешний read закрыт.

`started` означает наличие durable marker у nonterminal intent, а не новый claim о выполнении. Повторный begin при marker сохраняет timestamp/IDs; terminal возвращает `started:false` и прежний факт. `execute` требует уже committed marker (`native_attempt_not_started` иначе); marker и effect нельзя незаметно объединить одной Caps transaction. Host последовательно вызывает admit → beginAttempt → execute, между ними нет requirement удерживать один внешний transaction. Ошибка/abort не вызывает новый key или компенсацию Notes.

`committed` — immutable proof подтверждён и Caps final receipt committed. `not_applied` — **terminal** failed/cancelled с доказанным отсутствием эффекта, не просто отсутствие proof сейчас. `retryable` — proof отсутствует, intent nonterminal, исходная authority и operational checks ещё разрешают следующую execute; reconcile ничего не создаёт. `held` — durable marker/неопределённость сохранены, окончательного negative вывода нет. Если даже Caps read/transaction не завершился, метод бросает controlled error, а не конструирует Invocation из памяти.

Terminal row возвращается как исторический факт до обращения к Notes. Legacy generic Invocation без `cap_native_note_intents` может читаться/replay-иться, но begin/execute/reconcile отказывают `native_legacy_invocation_unsupported`; backfill IDs/proof запрещён. Для новой намеренной попытки нужен новый key.

`reconcilePage` читает максимум 16 native nonterminal identities, keyset `(created_at,id)` и bounded cursor. Короткая transaction выбора закрывается до последовательных вызовов `reconcile`; каждый получает новый собственный fence, без nested transaction. Body читается только для одной текущей операции и освобождается до следующей. Это проход восстановления, не новая очередь исполнения и не auto-execute. Cursor не является authority и нигде не доступен внешнему клиенту. Один проход не собирает весь input; host не запускает параллельные recovery loops и не крутит busy/held страницы без задержки.

## 4. Admission: replay раньше новых ограничений

До lock: exact data properties, копия только двух строк, стандартный @1 validator, fixed canonical input ≤262144 B и well-formed Unicode. Getters, Promise/thenable, дополнительные поля не принимаются. Идемпотентный ключ сохраняет прежний bounded string contract (8–160 UTF-16 units, без пробелов/control); не нормализуется. Raw HTTP bytes и fatal UTF-8 decoding принадлежат root.

Далее **Connect fence → Caps BEGIN IMMEDIATE**:

1. Проверить текущий opaque actor для `read/history`, действующую credential, весь parent chain и creator devices. Получить account/client только из этой authority.
2. Найти account+client+hash(key). При existing сначала проверить тот же principal/grant и текущую own read ACL, затем exact fingerprint. Чужая scope получает `invocation_not_found`, а не информацию о payload/conflict чужого запроса.
3. Same fingerprint возвращает прежний Invocation **до** executionEnabled, Notes availability, новых rate/capacity/budget, lowered configurable input limit и текущего Notes document/quota preflight. Исторический terminal не требует Notes DB вообще. Нет нового reservation/audit effect/dispatch. Новый действующий retry actor не переписывает `authorization_json`, expiry или key namespace.
4. Только для отсутствующего key: exact live contract/pins, operational enabled, matching v2/project identities, current invoke authority; текущие admission-only configured limits и `notes.native.validateDraftInput`. Полный Notes document должен поместиться до insert. Account counters проверяются ещё раз при настоящей Notes write, потому что здесь это только preflight.
5. В той же Caps transaction проверить bounded counts/rate/root budget, вставить один Invocation, reservation, pending dispatch intent и native identity с **точным Notes store ID**. Commit. Reject-before-admission не оставляет failed ledger row.

Fixed request fingerprint — уже принятый B1 `nativeNoteRequestDigest(input)`: обычный canonical JSON/SHA256, input≤262144 B, fixed envelope≤294912 B. Общий `canonicalHash` остаётся прежним. Fingerprint не включает новую credential, Notes текущую quota/updatedAt, operational flag или пользовательский URL. `inputDigest=canonicalHash(input)` и note/mutation IDs вычисляются по storage contract при admission и сохраняются; при retry не придумываются заново.

Снижение текущего `limits.invocations.inputBytes`/Notes `noteBytes` после первоначального admission не скрывает старую квитанцию. Напротив, для **нового** key эти лимиты обязательны. Fixed malformed/Unicode/верхняя граница @1 остаются до lookup: такой native intent не мог быть принят совместимым B2; это не способ обойти transport limits через guessed key.

Терминальная запись хранит request digest даже после input purge. Exact replay сравнивает fingerprint присланного caller input, а не читает нынешнюю Notes. Same key+different body —409, не новая версия или implicit overwrite.

## 5. Исходная authority и registry drift

Для создания используются сохранённые credentialId/audience/account/client/principal/grant/root, capability digest/target/resources/effects/recipients/charges и **исходный абсолютный expiresAt**. Свежая проверка берёт именно эту credential из Caps, затем текущие client/principal/полную grant ancestry/creator devices. Current retry credential служит внешнему read/admission lookup; она никогда не продлевает существующее dispatch право.

Нужно проверить оба предела: текущие credential/chain expiry и `now < originalSnapshot.expiresAt`. При revoke/expiry original credential новое удостоверение той же grant не возобновляет старый intent. Согласованная root семантика policyEpoch: это immutable admission snapshot, но не замена проверке полномочий и не blind equality. Текущий root epoch растёт также при отзыве sibling credential/grant; незатронутая цепочка не должна терять право только из-за чужого sibling. Каждый attempt независимо перечитывает current chain и пересечение с исходными immutable scope pins. Парная приёмка: sibling revoke оставляет нашу цепочку рабочей; revoke нашего предка/creator/original credential перед допуском закрывает её.

Coordinator держит frozen server-owned registry entry из реального `createCatalog`, а не JSON-copy клиента. В каждом Caps transaction сверяет current `cap_contracts` pin, invocation digest/target и ожидаемый фиксированный Notes @1 контракт. Output validator получает именно entry этого registry (его identity gate сохраняется). Reopen с новой allowed operational flag не меняет semantic digest. Contract/binding mutation/отсутствие pin не создают новое разрешение и не являются proof отсутствия эффекта.

Service `schemaVersion/registryId` B1 — снимок открытия. B2 `storageIdentity` читает actual marker/project/registry на живой Notes connection; Caps перечитывает собственные identity metadata под своей transaction. Pending intent всегда требует свой persisted `notes_store_id`, а не текущую случайную новую DB. Другой store, пропавший marker, closed service или registry mismatch дают held/fail-closed. Смена Notes store не превращает null proof в отрицательный результат.

Никакой live restore/подмена файла под работающей connection не поддерживается. Равные registry IDs из разновременных backup также не доказывают согласованный restore: B2 не обнаружит исчезнувший effect в старом снимке с тем же ID. Нужен согласованный cold restore/WAL/backup operational gate; здесь не обещается решение произвольной утраты истории.

## 6. Marker, Notes transaction и завершение

**Marker transaction** отдельно от effect: Connect→Caps, live original authorization/cancel/readiness, затем atomic `started_at` и dispatch `state='dispatching'`. Начатый marker не переустанавливается. Если marker COMMIT бросил ошибку, в этом вызове Notes не вызывается; следующий запрос перечитывает долговечное состояние. До успешного marker отсутствие started_at не используется generic refund shortcut: baseline safe-hold сохраняется.

**Effect transaction**: новый Connect fence → Caps BEGIN IMMEDIATE. Читать terminal/intent/identity заново. Сначала mint reconcile context и `Notes.readCreateProof`, независимо от revoke/operational disable. Exact proof есть — перейти сразу к output/settlement. Null proof получен в matching store под Caps lock — только теперь проверять cancellation/исходную authority для нового эффекта. Если закрыто — выполнить proof-first negative completion либо удержать временную недоступность.

Для разрешённого эффекта mint отдельный create context; Notes начинает **BEGIN IMMEDIATE**, повторяет own identity/context/pins, проверяет operation proof. Уже имеющийся exact proof возвращается до current document state/quota. Без proof существующий noteId/mutationId не присваивается вызову: `native_identity_conflict`, held incident, без overwrite. Новая запись использует active/plain/pinned=false/items=[]/revision1; note, FTS, counters/quota, обычный creation receipt при его использовании и immutable native proof коммитятся одной Notes transaction.

Непосредственно перед Notes COMMIT verifier снова проверяет live original authority/cancel/clock на всё ещё удерживаемых Connect/Caps locks. Это последняя точка допуска, не обещание физически остановить fsync ровно в миллисекунду expiry. Конкурирующий authorized revoke либо линеаризован раньше и предотвращает эффект, либо после принятого effect. Notes quota/human write сериализуются Notes DB; сетей/await внутри нет.

После Notes COMMIT Caps lock всё ещё удерживается. Exact proof → validate `{noteId,revision:1}` реальным catalog output validator **и** более узким доменным check stable ID/revision1 → один created effect → immutable receipt → spent1 → terminal succeeded → input purge. Для fixed native contract output revision≥1 из общей schema недостаточно, допускается ровно1 и сохранённый ID.

В одном Caps COMMIT должны оказаться receipt, reservation settlement, status/effects и `input_json='null'` + `input_purged_at`. Порядок SQL учитывает frozen input/receipt triggers: вставить receipt, затем обновить terminal/input, затем native purge timestamp и settlement внутри общей transaction. Ошибка любой записи откатывает всё это, сохраняя ранее committed marker; Notes effect не компенсируется.

Completion digest ровно принятый B1:

```js
canonicalHash({status, effectState, effects, receipt, disposition, actualCharges})
// succeeded: disposition='spent', actualCharges=[{unit:'invocations',amount:1}]
// failed/cancelled: disposition='released', actualCharges=null
```

Negative native completion имеет `effectState:'none'`, пустые effects/artifacts и фиксированный safe errorCode при необходимости. B2 использует `verificationMethod:'domain_read'`, потому что даже отмена до marker подтверждается matching-store read под Caps lock. Reader baseline исторически допускает unverified negative fixture, но это не выбранный B2 путь.

## 7. Proof-first reconcile и ошибки после commit

| Состояние в текущем matching store | Действие |
|---|---|
| Terminal Caps receipt | Исторический факт; никакого create/Notes existence lookup |
| Exact Notes proof при любом текущем revoke/cancel/disable | Final success/spent1; current external ACL проверяется отдельно |
| Null proof и cancel_requested | Final cancelled/released0/purge |
| Null proof и подтверждённый исходный credential/grant/device revoke/expiry | Final failed/released0/purge |
| Null proof и исходный допуск действует | Reconcile возвращает retryable, сам не создаёт |
| Operational disable/неподключённый handler при null proof | Held; не считать временный disable отзывом пользователя |
| Busy/locked, I/O/неоднозначный COMMIT, другой/потерянный store, malformed proof/output/contract | Held/error; не refund, не no-effect |

В начале reconcile доказательство читается прежде authority. Поэтому Notes COMMIT → Caps rollback → revoke не теряет совершённый эффект и не возвращает бюджет. Negative proof не кешируется между calls и не передаётся как доверенный JSON объект в settlement. Caps lock исключает конкурирующий native creator в том же registry до negative COMMIT.

Обычные Notes quota ошибки при попытке создания могут завершить intent failed только после завершённого rollback Notes и нового успешного null-proof read в matching store под тем же fence. Если rollback/read неоднозначен, это held. Contract/storage/clock/programming ошибки не превращаются в permission denial по широкому catch. Terminal failed/cancelled никогда не переоткрывается после enable/восстановления grant/quota; повтор возвращает его прежний факт.

Generic `peekDispatch/beginDispatch/bindJob/recordResult/settleBudget` продолжает отказывать native. Generic cancel лишь сохраняет request, а штатный native reconciliation завершает его при proof. Нужен узкий приватный settlement/ledger core, доступный coordinator внутри текущей Caps transaction; не добавлять публичный `allowNative:true`, произвольный `proof` аргумент или обход native guard через fake actor.

## 8. Opaque context lifetime

Coordinator хранит WeakMap token → descriptor + mode + **текущий transaction/fence generation**. Mint возможен только внутри его синхронного effect/reconcile frame. Descriptor exact/frozen, без input/token/credential. Token не выходит через admission/read/HTTP. `verifyContext` проверяет service-open, текущий активный frame, mode, Caps transaction и неизменные identity/pins; create дополнительно повторяет original authorization. Notes проверяет token на входе и непосредственно перед commit.

Context аннулируется в `finally` при любом return/throw/COMMIT/rollback/fence failure. Захваченный token, JSON clone, reconcile→create, token другого coordinator/store и вызов из microtask после синхронного frame отказывают. AsyncFunction не вызывается там, где нужен sync callback; returned Promise/thenable тоже отвергается. Это доверенная host композиция с защитой lifetime, не sandbox для злонамеренного server plugin.

Если host fence COMMIT/restore timeout падает после Notes/Caps COMMIT, результат не объявляется отсутствующим. HTTP заново получает разрешённую persisted projection; нет попытки повторить create с новым ID. Notes/Caps transaction guards не выполняют rollback чужой уже открытой transaction.

## 9. Unicode, объём и derived preview

Pinned `validation.mjs`/catalog @1 не менять. Root сначала fatal-decodes UTF-8, затем точный typed JSON input попадает в домен. Coordinator и Notes native boundary отвергают lone high/low surrogate как `invalid_unicode` до новой admission/effect; без normalization, replacement, truncation или `toWellFormed()`.

Full document считает **все** серверные defaults: `{title,body,items:[],color:'plain',pinned:false,state:'active'}`. Byte sentinel input262144 / document262203 отказывает `notes_note_too_large` до reservation. Второй sentinel input262072 / document262131 / fingerprint262359 проходит объёмные checks и читается B1 reader2; это разные границы. Lowered configurable Notes document limit применяется к новым admissions и настоящему create, не к historical receipt.

80 emoji в title остаются160 UTF-16 units;81 отвергаются прежним maxLength. NFC/NFD сохраняются разными строками/digests. Для preview исправить только производное усечение: `.slice(0,180)` не должна оставлять половину surrogate pair (179 ASCII + emoji). Исходный body/title не переписываются. Shared Notes helper может обслуживать human/native derived preview, но human input validator и смысл существующих document bytes не меняются.

## 10. Bounded work и точные codes для root

Native trusted limits (strict positive safe integers не выше defaults): `nonterminalPerPrincipal:4`, `nonterminalPerAccount:16`, `nonterminalTotal:128`, `admissionsPerPrincipal:10`, `admissionsPerAccount:30`, `identitiesPerAccount:10000`, `identitiesTotal:100000`, `recoveryPageSize:16`. Окно rate фиксировано60000 ms. Считается прежний ledger соответствующей области, включая historical generic rows; migration не удаляет превышение и не объявляет его corruption.

Queries возвращают IDs с `LIMIT threshold+1`, не body/authorization. Partial nonterminal predicate совпадает с B1 index. Rate `created_at>=now-60000` без верхней границы защищает от обхода при clock rollback. Replay/read/revoke/reconcile/human Notes не занимают новый admission slot. Quota/root-budget checks и insert атомарны в Caps; reservation ровно один.

Transient busy timeout100 ms на каждое acquisition Connect/Caps/Notes восстанавливается в finally. Это не общий deadline и не физический лимит fsync. Root дополнительно владеет raw body2MiB, deadline15s, readers8/global и2/peer, POST60/60s, peers2048 — они не являются domain quota и не удерживают DB lock пока читается сеть.

| Code / группа | Domain meaning / HTTP use |
|---|---|
| `invalid_input`, `invocation_invalid_arguments`, `invalid_unicode` |400; до нового admission, без coercion |
| `payload_too_large`, `invocation_payload_too_large`, `notes_note_too_large` |413; fixed input, configured input или полный document |
| `authorization_required` |401 current external credential; internal original denial обрабатывается только после proof |
| `access_denied` |403 current external scope; internal original denial не означает no-effect без proof |
| `invocation_not_found` |404 unknown/foreign/sibling invocation; raw IDs не подтверждать |
| `invocation_request_conflict` |409 same own key с иным exact fingerprint |
| `budget_exceeded`, `native_admission_limit`, `native_rate_limit`, `native_ledger_limit` |429 новое admission; existing replay не проходит эти gates |
| `notes_count_quota`, `notes_identity_quota`, `notes_storage_quota` |Known Notes business refusal внутри настоящей create transaction; после intent — только verified final negative или held. Pure document preflight не обещает чтение account quota |
| `capability_disabled`, `native_unavailable`, `native_store_mismatch`, `native_storage_busy` |503 для нового/непрочитанного запроса; existing authorized Invocation может остаться202 held |
| `native_legacy_invocation_unsupported`, `native_attempt_not_started` |Internal sequencing/legacy refusal, не trigger нового intent; наружу current projection либо fixed503 |
| `native_context_invalid`, `native_contract_mismatch`, `native_proof_invalid`, `native_identity_conflict`, `native_output_invalid`, `clock_invalid` |Internal invariant/incident; никакого terminal-none/refund через общий catch; fixed500/503 mapping root без деталей |
| `notes_storage_corrupt`, `capabilities_storage_corrupt`, unknown schema/closed service |Fail-closed storage error; safe503 без SQL/body/metadata |

Это предложенный исчерпывающий domain code set для согласования перед implementation. Не принимать произвольный `.code` из неизвестного exception как public message или permission denial. Root transport-only400/408/415/429 и unexpected500 остаются отдельными. Внутренние exceptions не содержат title/body/token/SQL. Если после попытки можно сделать fresh authorized read, HTTP отдаёт persisted terminal/held состояние, а не сырой executor exception. Само202 не свидетельствует об отсутствии или наличии эффекта.

## 11. Приёмка по реальным seams

1. **Pure/domain boundary:** fixed digest не меняется; Unicode pairs/NFC/NFD/control/profile, full-document и request-envelope sentinels, unknown fields/getters; каждый reject-before-admission оставляет0 новых rows/reservations. Нижний current limit не блокирует старый key/receipt после reopen.
2. **Два реальных Caps/Notes writers:** одинаковый key, разные keys на последнем root budget, child principals/account/global/rate boundaries; concurrent human Notes write. Один object/reservation/spent. Проверить populated `EXPLAIN QUERY PLAN`, включая historical scope, clock rollback и лимиты ниже уже существующего ledger.
3. **Crash/reopen:** после admission; после marker; после Notes COMMIT до Caps receipt; после receipt INSERT до input purge; после Caps COMMIT до ответа. Для каждого — реальные DB effects, restart, тот же key/IDs, finite body retention. Главный случай: Notes commit → Caps rollback → revoke/cancel → proof-positive spent1, не refund.
4. **Authority:** real Connect second writer revoke перед/во время fence; original credential expired, но новый same-grant actor читает; sibling/new grant не читает; no fake actor; disabled lostACK replay; failed/cancelled terminal не reopen. Opaque clone/wrong mode/retained token/thenable/late microtask не создают Notes.
5. **Store и proof:** другой Notes registry, отсутствующий store, mixed versions, changed live metadata/contract pin, malformed proof, ID collision, output failure, Notes/Caps busy и COMMIT error. Все uncertain cases held; отрицательный proof только в matching store под lock. Согласованный restore остаётся отдельным operational gate.
6. **Исторический create-only:** >32 обычных edits, archive/trash/purge, reopen → historical revision1 receipt без current note lookup/body/existence hints и без resurrection. FTS/counters/proof остаются согласованными. Отозванный actor не получает даже эту квитанцию.
7. **Root composed HTTP/PWA:** настоящий owner grant → внешний POST/get/retry → один Notes object → protected PWA open с исходным Unicode; exact HTTP ingress/error/privacy matrix из host plan. Unit callback stub не засчитывается real Connect fence или HTTP.

## 12. Последовательные этапы и владение

**B2a, domain boundaries:** автор modules/notes/server — fixed native port, pure full-document preflight, proof-first local transaction, safe preview; modules/capabilities/server — один fixed coordinator и приватный ledger/budget core, при сохранении generic native guard. New focused tests в двух modules. DDL B1 остаётся frozen; любое обнаруженное изменение обсуждается отдельно до записи. Root параллельно владеет только Connect fence/его независимыми tests.

**B2b, durable lifecycle:** admission/marker/execute/reconcile, exact current-vs-original authority, quota/retention/recovery, реальные two-writer/crash tests. Source/API freeze до root HTTP composition. `native-note-contract.mjs` переиспользуется; input validator/catalog semantic @1 не меняются.

**B2c, host integration и independent acceptance:** root связывает port/fence/status/routes/sidecar admission/OpenAPI, затем read-only reviewer проверяет actual network, proof-after-commit и privacy. Никакого native output→HTTP shortcut. Приёмка каждого среза последовательна на закреплённом Node24.21.0/SQLite3.53.4; одновременно тяжёлые suites не запускать. Reader/image/Linux/restore gates не заменяются локальным PASS.

Готовность этого документа означает только прочитанные source/contracts и согласуемый implementation map. Native B2 methods, effect и route пока отсутствуют. До отдельного разрешения root production source не меняется.
