# P3-B1 — модель публикации и допуск

2026-09-30. Авторский срез B1a–c; исходники заморожены для независимой приёмки. Основание: [принятый контракт](p3-publication-contract.md). Здесь описана модель данных и синхронное решение о праве доступа. HTTP/WS, обмен ticket, cookies и постоянная проверка stream относятся к B2. Именованные адреса пока возвращают status-only; RPC честно сообщает `runtimeReady:false`.

## Схема и переход

`modules/apps/server/schema.mjs` распознаёт empty, исторические v1 (`user_version` 0/1), v2 (2), v3 (3). Маркер новой схемы — `soty.apps-registry.v3`. Неизвестный формат или изменённые обязательные DDL/index/trigger definitions отказывают до миграции. Это проверка известного формата, не удостоверение всего содержимого диска.

Миграция в одной `BEGIN IMMEDIATE` транзакции сохраняет прежние app IDs, legacy canonical origins, grants, domain receipts и tombstones. Новые значения: private (`restricted`), unlisted, epoch1, target revision1, пустой активный набор aliases. Пустой legacy template остаётся пустым. В v3 недостающие связанные данные автоматически не «исправляются»; операции, требующие их, отказывают.

Новые таблицы:

| Таблица | Назначение и ключ |
| --- | --- |
| `app_runtime_targets` | Immutable маршрут, PK `(app_id,revision)`; owner/device composite FK; connector, port, entry path, profile, digest. |
| `app_publications` | Единственный active target pointer того же app, policy/listed/epoch, подтверждение публикации целого порта. |
| `app_publication_domains` | Точный активный набор, PK `(app_id,domain_id)`, same-app/owner composite FK. Canonical/tombstone запрещаются в транзакционном API. |
| `app_publication_receipts` | PK `(account_id,request_key)`, unique `(app_id,committed_epoch)`; историческое подтверждение принятой команды. |

`app_runtime_target_no_update` и `app_runtime_target_no_delete` запрещают изменение/удаление target. Digest включает namespace, app, revision, owner, connector, port, path, profile. Профиль B — `soty.relay-restricted.v1`. Он не удостоверяет неизменность чужого кода на устройстве. В B source-update API отсутствует; прежние source поля app не становятся второй редактируемой конфигурацией.

DDL зафиксирован для независимого Apps3 reader. Его формат/manifest и совместимый candidate/fallback проверяются отдельным release gate. Локальная миграция не разрешает production-выкладку.

## RPC

Единая точка — `createAppsService(...).execute({actor,op,args})`. Внешний actor приходит из проверенного Connect; клиент не выбирает owner в payload. Необязательный `expectedAccountId` проверяется до чтения и удаляется из аргументов доменной операции.

`apps.publication.get` принимает `{appId}` и возвращает owner-only состояние:

```js
{
  schema: 'soty.app-publication.v1', appId, appState,
  launchPolicy, listed, policyEpoch, activeTargetRevision, activeDomainIds,
  target: {revision, digest, profile, port, entryPath}, updatedAt,
  runtimeReady: false, receiptRetention: {perApp: 64}
}
```

`apps.publication.update` принимает полное новое намерение:

```js
{
  appId, requestId, expectedPolicyEpoch, expectedTargetRevision,
  launchPolicy: 'restricted' | 'anyone', listed: boolean,
  activeDomainIds: string[],
  exposureAck: {scope: 'whole-port', targetRevision, targetDigest, profile}
}
```

Для `restricted` `exposureAck` отсутствует либо null. Для `anyone` требуется точное подтверждение текущего target/profile: публикуется весь порт, включая API приложения автора. `listed:true` требует `anyone` и хотя бы один активный адрес. Набор ограничен 100 уникальными domain IDs; каждый — bound alias того же app/owner. Claim сам ничего не активирует. Право запуска и появление в каталоге независимы; публичная выдача каталога в B1 не добавлена.

Ответ: `{requestId,replayed,receipt,current}`. `requestId` берётся из текущего запроса; БД хранит только его hash. Receipt содержит `schema:'soty.app-publication-receipt.v1'`, `namespace:'apps.publication.update.v1'`, requestKeyHash, appId, policyEpoch, launchPolicy, listed, activeDomainIds, targetRevision/digest/profile, exposureAck, committedAt. `receipt` — исторический принятый результат; `current` может уже быть закрыт или revoked.

Текущие actor и owner проверяются перед историческим replay. Publication ledger отделён от domain claims. Один сохранённый ключ с другим намерением даёт `app_publication_request_conflict` (409). CAS устаревшего epoch — `app_publication_revision_conflict` (409), другой target revision — `app_publication_target_conflict` (409). Неподходящий адрес — `app_publication_domain_unavailable` (409). Неизвестные поля отказывают.

## Транзакции и повтор

Проверка owner, CAS, target/адресов, increment epoch, policy, exact active set, receipt insert и prune выполняются в одной транзакции. Любой accepted publication update, включая no-op, увеличивает epoch. Последние 64 receipts каждого app выбираются по committed epoch, а не времени. После удаления старого receipt исходный повтор с прежним expected epoch отказывает без эффекта. Отсутствие receipt не доказывает прежний исход и не называется «не выполнено»/«receipt expired». Клиент сверяет current view, но не подставляет свежий epoch в неопределённый pending intent автоматически. Новое намерение получает новый ID и подтверждение.

Предела общего количества переключений нет. Retention не мешает emergency restriction. Уникальность изменённого намерения под уже удалённым ключом навсегда не обещается.

`apps.register` атомарно создаёт app, grants index, canonical/head, target1 и publication. Точный повтор регистрации на том же порту возвращает прежний app; он требует существующей согласованной publication, не пересоздаёт её. Изменение grants в `apps.update` и policy epoch атомарны. Name-only и семантически одинаковые grants epoch не меняют; default значения читаются под writer lock.

`apps.revoke` атомарно закрывает app, очищает активный набор, ставит restricted/unlisted и увеличивает epoch. Повтор revoke уже закрытого app идемпотентен. Отзыв grants у `anyone` инвалидирует старые решения, но свежий public visitor по-прежнему допустим.

`apps.domains.retire` атомарно пишет tombstone/domain receipt, удаляет адрес из активного набора и увеличивает policy epoch, если адрес был активным. Если выключен последний активный адрес, listed становится false. Inactive alias не имел допуска и не увеличивает policy epoch; его domain revision всё равно меняется. Повтор receipt не вызывает новые invalidations. Обязательные внутренние hooks `onRetireInTransaction` и `onPolicyChanged` связываются Apps service; без явного coupling создание registry отказывает, notify вызывается после commit. Сетевое прекращение потока по epoch выполняет B2.

## Внутренний AccessDecision

`service.policy.decideAccess({domainId,origin,actor?,ttlMs?})` читает согласованный snapshot. Только bound exact-origin адрес, enabled app, актуальный target/device owner и текущие полномочия допускаются. Для alias нужен активный набор. Anonymous visitor допустим только на alias с `anyone`; legacy canonical всегда требует действующего actor и прежних grants.

Результат deeply frozen и принадлежит локальному `WeakSet` сервиса:

```js
{
  subject: 'account' | 'public', actor: {accountId,deviceId}, // только account
  accessBasis: 'grant' | 'public',
  appId, domainId, origin, policyEpoch, targetRevision, targetDigest, profile,
  expiresAt, route: {connectorKey,port,entryPath}
}
```

Это внутренний объект, не JSON authority. `route` не выдаётся браузеру. Копия, десериализованный объект и решение другого instance отказывают. `subject` описывает личность, `accessBasis` — источник права. Account с owner/account/community grant получает `grant`, account без личного права на `anyone` alias — `public`; anonymous всегда `public`, canonical только `grant`. B2 считает public sublimit по basis, а не наличию аккаунта, чтобы авторизованный публичный посетитель не занимал резерв личных streams.

`recheckAccess(decision,{ttlMs?})` требует неистёкший brand, перечитывает actor/grants/domain/policy/target и сохраняет прежние pins, включая accessBasis. Basis берётся только из исходного branded decision, не из caller DTO/options. Потеря community membership либо прав владельца группы закрывает прежнее granted решение даже на `anyone` alias при прежнем policy epoch; fallback на public запрещён. Прежнее public решение сохраняется при появлении новых grants, пока сам public доступ действует: оно занимает прежний public slot и не повышает права соединения. Новое `decideAccess` может выбрать grant. Без ttl сохраняется deadline. С ttl допускается новая ограниченная lease: subject public ≤30 секунд, account ≤1 часа, только после проверки прежнего ещё действующего решения. Истёкшее решение не продлевается. B2 обязан отдельно сохранять абсолютный deadline account session; rolling lease применима к живому anonymous public stream. Само решение не подтверждает online connector или исполняемость runtime.

## Доказательства и следующие gates

Авторский прогон:

```text
node --test modules/apps/test/app-publication.test.mjs modules/apps/test/newdomains.test.mjs modules/apps/test/newdomain-hosts.test.mjs modules/apps/test/apps.test.mjs
46 passed, 0 failed, 0 skipped
```

В новых 19 тестах: реальные frozen v1/v2→v3/reopen, невалидный формат и rollback, target immutability/same-app FK, owner/account/replay, whole-port ACK, новые inactive aliases, fixed-clock retention64 и поздний retry, insert/prune/epoch faults, register rollback, brand/host/expiry/lease, community/admin recheck, revoke/retire и реальные отдельные SQLite workers для CAS/retry и гонок grants/retire/revoke против publication update. Дополнительно доказаны отказ при partial-v3 без автоматического восстановления и различие account identity/access basis при отзыве community grant у публичного приложения. Прежний hosts fixture теперь создаёт обязательные v3 model rows в одной seed transaction; продукт не менялся ради обхода неполных данных.

Независимая приёмка другого инженера: **36/36 PASS** (26 новых publication cases + 10 прежних domain acceptance), включая sticky public basis при получении membership, отказ grant при потере права и запрет повышения через options. Проверен source SHA256 `3c3f1eafdb2dea841edf3338fb6f3efbfa5ecebac456f4c9e85fe29d5f232d3e` для `publications.mjs`. Подробности — [независимый отчёт](p3-publication-independent.md).

B2 transport, настоящее anonymous/private named открытие в браузере, WS/await-boundary checks и production compatible fallback остаются отдельными непройденными здесь gates. Existing legacy HTTP/WS tests прошли; это не доказательство нового именованного runtime.
