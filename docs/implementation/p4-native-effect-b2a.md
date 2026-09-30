# P4-B2a — fixed Notes port и API coordinator

30.09.2026. Узкий source/API checkpoint после B1a `5e459abc6afa376861c2032226bd29f78bf0468d`, по [принятому плану](p4-native-effect-plan.md). Это локальная реализация доменных границ, а не приёмка crash/restart, Connect HTTP, PWA, migration rollout или production исполнения. B1 DDL, pinned `notes.createDraft@1`, его validator и semantic digest не менялись. API страницы ниже обновлён по согласованному B2b finding; исторические B2a hashes и результаты сохранены. Их заменяет для текущего source [B2b receipt](p4-native-effect-b2b.md).

## Composition

`createNotesService` принимает optional sync `verifyNativeContext(token, mode)`. Он закрыт до готовности Caps; Notes constructor не вызывает его. Все методы `notes.native` синхронные:

```ts
storageIdentity(): {projectId:string; registryId:string; schemaVersion:2};
validateDraftInput({input:{title:string; body:string}}): {documentBytes:number};
readCreateProof({context:object}): NativeCreateProof | null;
createDraftForInvocation({context:object,input:{title:string;body:string}}): NativeCreateProof;
```

`NativeCreateProof` — exact frozen descriptor `{projectId,sourceStoreId,notesStoreId,invocationId,accountId,noteId,mutationId,inputDigest,capabilityDigest,revision:1,createdAt}`. Он внутренний, не HTTP output. `validateDraftInput` добавляет Notes defaults и считает весь document, без SQL/actor/effect. Отдельный notes-native input boundary отказывает lone surrogate и не выполняет replacement/normalization.

`createCapabilitiesService` принимает optional `nativeNotes:{notes:notes.native,withAuthorityFence}`. Port-функции фиксируются при создании service. `nativeNotes` отсутствует → `capabilities.nativeNotes === null`. При переданной композиции coordinator существует и для schema1/mixed; его `readiness()` возвращает false и новая admission не создаётся. Root должен передавать композицию также при operational disable, чтобы прежние authorized receipt/retry оставались доступны.

Notes verifier closure после composition вызывает только `capabilities.nativeNotes.verifyContext(token, mode)`. Реальный `withAuthorityFence` — Connect method, синхронный и не вложенный в signed Connect transaction. Он не берётся из caller args. Единственный operational flag — `notes.createDraft@1.executionEnabled` в registry; built-in default остаётся false. Native schema migration сохраняет прежний default-off и не управляется новым coordinator.

## Точный API для root

```ts
type Outcome = 'committed'|'not_applied'|'retryable'|'held';
readiness(): {ready:boolean};
admit({actor,idempotencyKey,input:{title,body}}): {reused:boolean;invocation:Invocation};
get({actor,invocationId}): {invocation:Invocation};
beginAttempt({invocationId}): {started:boolean;invocation:Invocation};
execute({invocationId}): {outcome:Outcome;invocation:Invocation};
reconcile({invocationId}): {outcome:Outcome;invocation:Invocation};
reconcilePage({cursor?} = {}): {
  items:({invocationId:string;outcome:Outcome}
    | {invocationId:string;errorCode:'native_reconciliation_failed'})[];
  nextCursor:string|null;
};
verifyContext(token:object,mode:'create'|'reconcile'): NativeNoteDescriptor;
```

`Invocation` — прежняя content-free projection: `invocationId,capabilityId,version,status,cancelRequested,effectState,effects,createdAt,updatedAt`, optional `completedAt,receipt`. Никаких input/title/body, auth snapshot, token, registry ID или текущего Notes existence hint. У unresolved started marker projection сообщает `effectState:'unknown'`.

Успех:

```js
receipt = {verificationMethod:'domain_read',artifacts:[{type:'note',id:noteId,revision:1}]};
effects = [{kind:'created',resourceType:'note',resourceId:noteId,revision:1}];
status = 'succeeded'; effectState = 'committed'; outcome = 'committed';
```

Root формирует deeplink из точного persisted artifact и затем защищённого PWA route. Notes lookup для receipt/deeplink не нужен. Negative terminal имеет `failed|cancelled`, `effectState:'none'`, пустые effects/artifacts, `verificationMethod:'domain_read'` и optional safe `errorCode`. `not_applied` используется только для такого терминального факта. Completion digest остаётся единственной нормой B1: `canonicalHash({status,effectState,effects,receipt,disposition,actualCharges})`; success — spent/charges1, negative — released/null.

`admit/get` требуют свежий opaque service actor. Internal marker/execute/reconcile имеют actorless recovery semantics и **никогда не заменяют** внешний authorized `get`: после internal outcome HTTP читает current permitted projection заново. Новый actor/credential повтора не продлевает исходное разрешение эффекта. Same-key historical replay проверяет current read ACL и полный fingerprint до readiness, handler flag, новых quota и текущего Notes byte limit. Legacy invocation может быть прочитан, но не получает native pins автоматически и не исполняется native coordinator.

Новые проверки coordinator бросают `AccessError` (`capabilities/server/validation.mjs`); shared Invocation core может бросить `InvocationError` (`invocations.mjs`), Notes port — `NotesError` (`notes/server/validation.mjs`). Root mapper обязан проверять class и согласованный allowlist, не arbitrary `.code`. SQLite/host/commit ошибки сохраняются внутренними; их нельзя превращать в отрицательное доказательство или публиковать SQL/message. Публичные коды/смыслы перечислены в §10 плана.

## Transaction и ограничение работы

Admission резервирует budget и коммитит invocation, dispatch intent и native pins одной Caps transaction. Marker — отдельный durable commit: `started_at` + delivery `dispatching`. Effect удерживает Connect→Caps, сначала читает permanent Notes proof, затем при действующей исходной authority получает отдельный opaque create context. Notes коммитит документ/FTS/counters/ordinary receipt/permanent proof вместе; Caps receipt/spent/input purge — следующий отдельный commit. Общего двухбазного COMMIT нет. Функционально общий ledger/budget SQL переиспользуется через приватно захваченный transaction core; публичный generic native bypass не добавлен.

Context существует только внутри текущего sync fence/transaction frame. Проверка Notes происходит до BEGIN, внутри snapshot и перед COMMIT. Clone, сохранённый token, неправильный mode, Promise/thenable и поздняя callback работа не получают допуск. Последняя original authorization проверка учитывает полную свежую chain, creator devices, исходную credential/audience/absolute expiry и capability scope; простой global root-epoch equality не используется. Generic dispatch/result/release paths продолжают держать native marker до proof-first completion.

`limits.nativeNotes` strict positive integers, только снижение: unresolved 4/principal,16/account,128/global; admissions за60000ms 10/principal,30/account; retained IDs 10000/account,100000/global; recovery page16. В rate нет верхнего `created_at<=now`, поэтому clock rollback не стирает окно. Checks и inserts под одним Caps lock. Lookup возвращает только ограниченные ID; threshold rows достаточно, чтобы отказать новому превышающему запросу. `reconcilePage` читает IDs по keyset, затем делает отдельную последовательную transaction на каждый ID и никогда не вызывает execute. После B2b ошибки отдельных intents возвращаются как safe `errorCode` без `outcome`, не блокируют последующие IDs и не теряют cursor; ошибка самого выбора страницы бросается. Предыдущие commits не отменяются, повтор proof-safe. Это bounded количество locks, **не** общая deadline страницы/физического fsync. Busy acquisition Notes/Caps100ms восстанавливает прежнее значение; Connect fence даёт свой100ms отдельно.

## Проверки и оставшийся gate

Первый focused запуск `modules/capabilities/test/native-effect.test.mjs`: **10/10 PASS**,0skip,1.43s, Node24.21.0/SQLite3.53.4. Реальные Notes/Caps файлы и ACL; `withAuthorityFence` в fixture намеренно sync stub. Здесь проверены actual1/mixed refusal, чистый preflight, separate marker, один effect/proof/spent/input purge, Unicode/full-document/envelope boundaries, safe preview, baseline reopen, disabled/unready/low-limit historical replay, original expiry vs новая credential, sibling epoch и retained/copied context, actual Notes quota rollback/negative proof.

Финальный own focused запуск: **12/12 PASS**,0skip,1.706s; добавлены refusal caller-selected account/target/copied actor, enumerable/non-enumerable getters без их выполнения и malformed proof без false negative settlement. `node --check` новых native modules и `git diff --check` PASS.

Serial regression дал **114 total / 113 PASS / 1 FAIL / 0 skip**,10.140s. Единственный fail — историческая B1 assertion `baseline.native === undefined` в независимом Notes test; она устарела после принятого добавления закрытого native port. Автор независимой проверки заменил только это ожидание на actual refusal без verifier и подтвердил **6/6 PASS**,0skip,871.6647ms: v1/v2 closed port, pure validation и отсутствие DB/file изменений. Это отдельный его прогон, исходный 114-case результат не переименован в PASS. Продуктовый отказ/integrity gate ради PASS не убирается. Список исходного прогона:

```text
modules/notes/test/service.test.mjs
modules/notes/test/connect-integration.test.mjs
modules/notes/test/native-storage.test.mjs
modules/notes/test/native-storage.acceptance.test.mjs
modules/capabilities/test/access.test.mjs
modules/capabilities/test/acceptance.test.mjs
modules/capabilities/test/invocations.test.mjs
modules/capabilities/test/native-storage.test.mjs
modules/capabilities/test/native-storage.acceptance.test.mjs
modules/capabilities/test/native-note-contract.test.mjs
modules/capabilities/test/native-effect.test.mjs
```

Все запускались с `node --test --test-concurrency=1` и изолированным Node directory первым в локальном `PATH`. Первый regression включал11 own native cases; финальный12-case повтор покрывает последние native-only boundary правки. Полный набор повторно не запускался в B2a. По сообщению root его отдельный `server/test/capabilities-actions.test.mjs` прошёл6/6 через настоящий HTTP/createHttpApp/Connect fence/три stores, включая edited/purged/restarted/disabled replay и isolation. Это атрибуция root, не мой независимый network или crash опыт. Последующий независимый B2 review и собственные lifecycle проверки перечислены в B2b receipt.

## Source freeze B2a

SHA-256 точных рабочих файлов, после final12/12. Schema1/2, `catalog.mjs`, `validation.mjs`, `documentation.mjs` и `native-note-contract.mjs` по `git diff --name-only` неизменны. Root/peer files и server composition сюда не входят.

| Файл | SHA-256 |
|---|---|
| `modules/notes/server/native.mjs` | `1dcff0bb9bdae66a855f971363204081e5f70b81253c6bd9abc3413f9cb2cb65` |
| `modules/notes/server/index.mjs` | `32befd49b596714293f01ae08a7b5a0015e15fc7b9f8eb9311e95d76b8a9dfc7` |
| `modules/capabilities/server/native-notes.mjs` | `8960e0a452cbee3bac0b0722df2133ea093b8cb0301a1fcf2257eafd4aa6c485` |
| `modules/capabilities/server/index.mjs` | `d9984d1aa2b8e6fdbac18d0cdb989ee3e9604ff8b3e3980ad091fab135cf9618` |
| `modules/capabilities/server/access.mjs` | `e837bd8c01813f8a7efb2a7f2872e21bcdb9615c2eadb59f42fcac6b067d15e5` |
| `modules/capabilities/server/invocations.mjs` | `99ac2314cb07e0d07f5ba1b42f5acc1f6fe8dd79bce422d3ff161585a36dde06` |
| `modules/capabilities/test/native-effect.test.mjs` | `fb20fd5a63f9ad2659c5521435bffa7706573d38335821cd88921674dffeccaf` |
| `modules/capabilities/test/support/native-effect.mjs` | `d444e91a8d29a31233ee6bf1d744b6ae92b1e4ac816275edf6d73add8d3fbbe0` |
| `modules/notes/README.md` | `b961ec51c5c9dcf3746d04ae5fc21a97fa87711ba55c0a051bdcbaa7d5cd1388` |
| `modules/capabilities/README.md` | `47355367b7b5d86b3e87465810bd1ebae530830f7fb2123bd0bead6bcbd624d6` |

B2b остаётся отдельным обязательным gate: kill/reopen на каждом COMMIT seam; Notes commit→Caps failure→revoke/cancel proof-positive completion; реальные два writers/budget/quota/query plans; busy/commit/thenable/mode/store/pin faults; >32 human edits/trash/purge без resurrection; реальный Connect fence и external current read; bounded recovery. Эти доказательства нельзя выводить из первых unit PASS. Root HTTP ingress и Connect fence receipts отдельные и не приписываются этому авторскому срезу.
