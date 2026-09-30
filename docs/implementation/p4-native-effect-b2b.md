# P4-B2b — durable native Notes lifecycle

30.09.2026. Авторский локальный gate на изолированном Node24.21.0 / SQLite3.53.4, после [B2a API](p4-native-effect-b2a.md) и по [плану](p4-native-effect-plan.md). Проверки используют реальные файлы Connect, Capabilities и Notes, реальные подписанные installation requests, настоящий `Connect.withAuthorityFence` и отдельные процессы. Один malformed-host test намеренно использует неправильный sync fence; он явно выделен ниже.

Этот документ заменяет исторические B2a hashes трёх исправленных production файлов и прежнее описание остановки всей recovery page. DDL, schema readers, catalog, input/output validation profile, pinned `notes.createDraft@1` digest и fixed request-envelope helper не менялись. Public HTTP/host scheduler, OAuth, deploy reader labels, миграция serving volume и production не входят в авторскую область этого gate. Общую финальную регрессию и checkpoint выполняет root отдельно.

## Воспроизведённые findings и узкие исправления

| Finding | RED | Исправление и GREEN |
|---|---|---|
| Generic authorization reconciliation принимал временный `capability_disabled` за cancel native intent. Следующий native reconcile завершал negative, освобождал budget и стирал input. | Независимый `native-effect.independent.test.mjs`: 1 FAIL,309.0327ms, actual Connect/3DB; наблюдались cancelled/cancel1/purged вместо held. | Только native ветка generic `reconcileAuthorization` возвращает отказ authority без записи cancel при temporary disable. Proof-positive остаётся первым, explicit cancel и реальные revokes не ослаблены. Авторский повтор этого RED: 1/1 PASS,320.9476ms. Независимый финальный 3-case повтор атрибутирован ниже. |
| Normal function port, вернувший rejected Promise, давал controlled sync error, но затем дочерний процесс завершался из-за unhandled rejection. | Собственный child-process RED: после правильного error ответа exit1. | Все результаты sync native ports/fence и Notes verifier распознают thenable, добавляют rejection handler и отказывают; Promise не ожидается. Child cases для identity/readiness, input validation, proof read, create, verifier и fence завершаются exit0 и без Notes effect. |
| Неправильный host fence мог сохранить callback, вернуть управление, а затем вызвать callback и создать admission вне завершившегося вызова. | Собственный RED: поздний callback не бросил ожидаемый `native_context_invalid`. | `accepting` lifetime закрывается в `finally`; callback требует одновременно live invocation, running frame и отсутствие прежнего вызова. Поздний callback получает controlled refusal и оставляет0 invocation rows. Это trusted-wiring fault, не утверждение об async поведении настоящего Connect. |
| Один intent прежней Notes incarnation бросал из `page.map`, скрывал cursor и не позволял завершить следующую здоровую proof-positive запись. | Собственный RED на реальных двух Notes stores и одном Caps: `native_store_mismatch` вместо результата всей страницы. | Отдельная item failure получает safe union без outcome; следующий current-store proof завершается. Старый input/budget остаётся held, новый effect становится spent1. Ошибка выбора страницы по-прежнему бросается. |

Последние три RED были запущены вместе: **0/3 PASS, 3 FAIL**,782.9927ms. После причинных правок полный собственный fault slice прошёл **7/7 PASS**,0skip,4672.980ms. Ошибки не маскировались изменением ожидаемых product assertions. Из B2a production source изменились только `notes/server/native.mjs`, `capabilities/server/native-notes.mjs` и native-only catch в `capabilities/server/invocations.mjs`.

## Текущий API страницы восстановления

```ts
type NativeOutcome = 'committed'|'not_applied'|'retryable'|'held';
reconcilePage({cursor?} = {}): {
  items: Array<
    {invocationId:string; outcome:NativeOutcome}
    | {invocationId:string; errorCode:'native_reconciliation_failed'}
  >;
  nextCursor:string|null;
};
```

Error item не содержит `outcome`, input, store ID, SQL или внутреннюю причину. Он не доказывает отсутствие эффекта. Selection/registry failure бросается до готовой страницы; уже committed результаты предыдущих individual calls не откатываются. Cursor остаётся internal keyset по `(created_at,id)`. Host обрабатывает его после item errors и делает задержку; recovery никогда не вызывает execute. Остальные B2a options/exports/DTO неизменны. Historical receipt остаётся create-only и не сообщает current existence.

## Собственные исполняемые проверки

Три последовательных запуска, а не один общий тестовый процесс:

| Файл | Результат | Что доказывает |
|---|---|---|
| `modules/capabilities/test/native-lifecycle.test.mjs` | 9/9 PASS,0skip,4605.750ms | Пять actual child kills, actual process races, human/native quota,34 signed edits+purge, original expiry/creator revoke. |
| `modules/capabilities/test/native-faults.test.mjs` | 7/7 PASS,0skip,4672.980ms | Promise containment/lifetime, two-store recovery, before/after COMMIT exceptions, last-verifier expiry, wrong-mode/clone/late context, closed/store/pin/output failures. |
| `modules/capabilities/test/native-bounds.test.mjs` | 4/4 PASS,0skip,2604.5532ms | Scope/rate/ledger limits with replay, bounded indexed queries/keyset, actual three-store busy refusals and timeout restoration, delegated ancestry pairs. |

Итого20 авторских cases в этих трёх отдельных запусках. Циклы внутри cases дополнительно проверяют симметричные варианты; они не считаются отдельными test-runner tests. B2a12/12 был отдельным предыдущим срезом, не приписывается этому запуску. Все команды имеют `--test-concurrency=1`; isolated runtime directory был первым в локальном `PATH`, а дочерние процессы используют `process.execPath`. Каждый убитый child ожидается до настоящего `close/exit`, не только IPC checkpoint. Temp directories имеют nonce ownership и проверку абсолютного родителя перед удалением. Credentials/private inputs передаются child по IPC и не выводятся; stderr учитывается только числом bytes.

### Crash и единственность эффекта

Реальный child доходит до каждого seam, посылает checkpoint и блокируется; parent принудительно завершает его, проверяет durable files и повторно открывает services:

1. После admission: одна identity/reservation, нет Notes proof; reconcile только retryable, затем явный execute.
2. После durable marker: тот же intent/IDs и held budget; reconcile не создаёт note.
3. После Notes COMMIT, до Caps receipt: один note/proof, Caps reservation; даже после grant revoke proof-first завершает spent1. Внешний get отозванным actor закрыт.
4. После SQL receipt INSERT, до input purge: незакоммиченная Caps transaction теряется, Notes proof остаётся. Последующий explicit cancel не превращает effect в refund.
5. После Caps COMMIT, до ответа: receipt/spent/input purge уже durable; тот же key/reopen не повторяет effect.

Каждый путь заканчивается ровно одним note, permanent proof, invocation, receipt и spent1/reserved0. Reopen подтверждает историческое terminal состояние. Это process-crash/WAL опыт, не power-loss, fsync-latency или произвольное повреждение диска.

Два реальных service processes одновременно начинают same-key или competing-key операции при последнем budget1. Допустимый busy ответ повторяется тем же key; итог не превышает один effect/charge. Отдельная гонка настоящего подписанного human `notes.put` с native create при notes quota1 оставляет одну identity/active note и завершённый budget. Authority использует настоящий Connect fence; causal revoke-during-fence опыт Connect принадлежит отдельной root/critic приёмке и здесь не дублируется как собственный.

### Исторический receipt, authority и input

34 подписанные human edits, trash и purge после native create не удаляют permanent proof. Reopen/retry/execute возвращают тот же revision1 creation receipt, не нынешний private body; Notes deleted row остаётся пустым, FTS0, resurrection0.

Original credential expiry и actual creator-device revoke закрывают новый эффект. Новая действующая credential того же grant может читать negative history, но не продлевает исполнение. В отдельной delegated chain sibling revoke повышает epoch, сохраняя действующую нашу цепочку; ancestor или original credential revoke даёт proof-negative terminal/no effect. Последний Notes verifier повторяет authority: expiry именно на нём откатывает документ/proof до terminal negative и освобождения budget.

Opaque token нельзя клонировать, перевести из reconcile в create или использовать в поздней microtask. Notes store mismatch, закрытый Notes service, изменённый live contract pin и неверный proof revision не дают false negative completion: marker/input/reservation сохраняются. Malformed proof после реального Notes COMMIT исправляется только последующим чтением настоящего permanent proof. Отдельные injected exceptions до и после настоящего Notes/Caps COMMIT показывают, что exception не равен отсутствию эффекта; после reopen второго объекта нет.

### Ограничение работы

Проверены пониженные nonterminal limits отдельно для principal/account/global, rate principal/account, retained ledger account/global. Exact replay проходит раньше новых quota. Clock rollback не скрывает admission с более поздним `created_at` из rate window.

Instrumentation захватывает реальные admission SQL и параметры, не копию алгоритма: только ID, явный threshold `LIMIT`, scopes используют SQLite indexes по `EXPLAIN QUERY PLAN`. Recovery с настроенной page2 выдаёт два элемента и cursor, затем третий; не вызывает Notes create. Это planner/threshold evidence на малой fixture, не нагрузочный benchmark на100000 rows и не гарантия общей wall-clock deadline страницы.

Отдельные реальные SQLite connections удерживают `BEGIN IMMEDIATE` на Connect, Caps и Notes. Native execute ограниченно отказывает (`connect_authority_busy` либо `native_storage_busy`), не завершает intent/no Notes effect и восстанавливает точное прежнее `busy_timeout` каждой затронутой connection. После освобождения lock тот же intent reconcile/execute проходит. Проверяемый порог этого локального опыта — менее2s на отказ при acquisition100ms, не production SLA/ограничение fsync.

## Независимые и integration результаты

Атрибуция коллегам, без повторного объявления их опыта авторским:

- `whole_product_critic`: `native-effect.independent.test.mjs` **3/3 PASS**,0skip,776.3752ms. Temporary disable→held; actual Notes COMMIT→human purge→cancel+revoke→permanent proof completion без resurrection; fresh read credential не продлевает original execution expiry. Финальные три repair deltas прочитаны им без новых blockers.
- Тот же reviewer адаптировал obsolete B1 native-port shape assertion: Notes `native-storage.acceptance.test.mjs` **6/6 PASS**,0skip,871.6647ms с closed-no-verifier и no-write proof. Прежний failed113/114 regression не переименован в PASS.
- Root: actual HTTP composition6/6 после B2a и последующий HTTP/OpenAPI/recovery slice **15/15 PASS**,0skip,6593.9301ms. Root сам владеет его source/evidence; этот документ не заменяет его HTTP/PWA или общий release gate.

Reader2/deploy peer checks, root full regression, type/build и production roll-out имеют отдельные receipts. B2b не разрешает first v2 write на serving volume, не меняет default-off migration и не доказывает совместимость старого serving image.

## Source/API freeze

`git diff --check` PASS. Точные protected schema/profile/catalog/helper paths сравнивались через `git diff --name-only` с текущим checkpoint — без изменений. Таблица ниже фиксирует рабочие bytes до root общего regression; source не менять после freeze без конкретного finding.

| Файл | SHA-256 |
|---|---|
| `modules/notes/server/native.mjs` | `16371da8a23d9017b0380f01341d57fa26b08d5b44d0fb33cda86a56ae9520f9` |
| `modules/notes/server/index.mjs` | `32befd49b596714293f01ae08a7b5a0015e15fc7b9f8eb9311e95d76b8a9dfc7` |
| `modules/capabilities/server/native-notes.mjs` | `1b3f285692f3cfa4054c3f4ce88b15704db14834bb4ede6fb1ced043a161b02a` |
| `modules/capabilities/server/index.mjs` | `d9984d1aa2b8e6fdbac18d0cdb989ee3e9604ff8b3e3980ad091fab135cf9618` |
| `modules/capabilities/server/access.mjs` | `e837bd8c01813f8a7efb2a7f2872e21bcdb9615c2eadb59f42fcac6b067d15e5` |
| `modules/capabilities/server/invocations.mjs` | `b4e1fb3b48016d820593ef6e0ba04e5192843ae1a566343778ee0009f4ddb836` |
| `modules/capabilities/test/native-effect.test.mjs` | `fb20fd5a63f9ad2659c5521435bffa7706573d38335821cd88921674dffeccaf` |
| `modules/capabilities/test/native-lifecycle.test.mjs` | `5f93c83ac59ff9e39539aca97c3f796c7ee35395d6202cd0aad4134cbc8704e9` |
| `modules/capabilities/test/native-faults.test.mjs` | `984940845963945aed2fed342df7a6278892b2548e710aad10c92c4c24bb67b6` |
| `modules/capabilities/test/native-bounds.test.mjs` | `e70ca2638bf1aa2a87daf72256141ee5720ab09c34c65296866eebb9339c6011` |
| `modules/capabilities/test/support/native-effect.mjs` | `d444e91a8d29a31233ee6bf1d744b6ae92b1e4ac816275edf6d73add8d3fbbe0` |
| `modules/capabilities/test/support/native-connected.mjs` | `e7ff629532b6c7123f9ff0da8cffaf960f771eaef7fd8b9eb93b7ee6eecfb5a0` |
| `modules/capabilities/test/support/native-effect-child.mjs` | `5fe53ec4c57aabcbfcd51511d89a798328e02f0eecd633be591a6bd03814cbd8` |
| `modules/capabilities/README.md` | `04110d63f15e9a9b73bf4ae030f192773a941047bb02c82e8793bd6bab78ad8b` |
| `modules/notes/README.md` | `b961ec51c5c9dcf3746d04ae5fc21a97fa87711ba55c0a051bdcbaa7d5cd1388` |

Целевой финальный повтор root может включить четыре own `native-{effect,lifecycle,faults,bounds}.test.mjs`, independent native-effect и прежние `invocations.test.mjs`, `access.test.mjs`, Notes `service.test.mjs` / `connect-integration.test.mjs`: они проверяют затронутый shared budget/transaction code без необходимости объявлять более широкий неизвестный gate. Команда для каждого bounded slice: `node --test --test-concurrency=1 <caseFiles>` с изолированным runtime в `PATH`.
