# Ordinary Source: точный consumer finite24h для review

Статус: **proposal only; writer4, migration4, reader4 и Long runtime отсутствуют**. Этот документ уточняет замороженное предложение `58fba1b14efd90292b00ffb0d78ebbf0ac8fd04b`; прежний SQL не исполняется и не заменяется. Basic300, Std1/Std2, RP49, Native format3 и действующие installation/cold packets сохраняются. Чтение этого файла, descriptor или установка image не включает Long.

## Совместимость и включение

Std2 уже описывает optional `finiteSeconds:86400`, fixed `session-continue` и maintained Native RP. Предлагается **Source-only constructor opt-in** внутри этих существующих bounds, без изменения Std2 pin, Root DDL и wire. Он разрешает новое явное Native24h назначение; не меняет смысл Basic. Если проверка реального Root resume потребует другого маршрута, нового body field или более широкого broker permission, это отдельный immutable compiled profile, а не скрытое расширение Std2.

Trusted operator одновременно задаёт branded finite policy, exact approved Human renewal client profile digest/generation и реальный Source storage consumer. При отсутствии хотя бы одного — Basic300; при несовместимой конфигурации — not-ready. Public author manifest не содержит этого opt-in, client secrets, Native identity или grants. Root Human24h approval и Native24h consent — два независимых разрешения.

Первый pilot — только два новых пустых Native ресурса в независимых realms. Legacy link и existing Native resource Long не включаются этим пилотом. Даже новый пустой ресурс создаёт Native principal только через fresh OIDC + явную Source policy внутри Native final SQL. Его последующее использование требует текущего Native session/link/consent/membership.

## Basic и Long — разные реальные записи

Basic остаётся прежним: `source_sessions.expires_at=min(actual AT end,t0+300000)`, encrypted Session без family/rpMarker, exact original `rootReferenceDigest`; новый Root ref не проходит. Нельзя увеличить expiry, добавить marker, создать alias или family для существующего Basic session. После истечения нужен новый явный вход, который сохраняет ранее связанный Native principal только при новом OIDC и текущем независимом Native proof.

Новый Long создаётся только после нового authorized interaction с новым Native finite purpose. Новый `source_sessions` row использует тот же неизменённый формат таблицы, но **новый session hash** и encrypted mode `finite24h`; его expiry равен immutable family/RP end. Это не UPDATE старого Basic row. Long branch выбирается только по согласованным реальным family/head/Session records; JSON `mode`, найденный marker или locator не выбирает authority.

Базовый `resourceConsentDigest(profile)` остаётся прежним. `sourceFiniteConsentDigest(policy,baseDigest)` — отдельный явный Native purpose. Finite grant хранится в новой family как Source-owned факт подтверждения и связан с current Native consent/session. При чтении нужны оба: current selected-resource Native grant и active exact finite family. Отзыв любого закрывает доступ; finite family не создаёт role/support rights.

## Immutable anchor: точные поля, не identity projection

После actual code exchange и финального Native link Source строит host-only anchor. Текстовые opaque IDs сравниваются байт-в-байт; нет lowercasing, email/name matching, percent decoding или другого нормализующего join.

```ts
type FamilyAnchor = {
  schema: 'soty.ordinary-source-family-anchor.v1';
  root: { accountId: string; deviceId: string };
  human: {
    issuer: string; subject: string; clientId: string;
    clientProfileDigest: string; clientGeneration: number;
  };
  source: {
    realmId: string; profile: { id: string; version: number; digest: string };
    nativeOrigin: string; embedOrigin: string; parentOrigin: string;
    transportKeyId: string; cipherKeyId: string;
  };
  resource: {
    registryId: string; tenantId: string; appId: string; environmentId: string;
    resourceId: string;
    selection: { kind: 'soty.resource.v1'; nativeId: string; incarnationId: string };
  };
  native: {
    principalId: string; sessionHash: string; sessionGeneration: number;
    sessionExpiresAt: number; membershipRevision: number;
    linkDigest: string; selectedConsentDigest: string;
    selectedConsentSessionHash: string; finitePurposeDigest: string;
  };
};
```

Root/human поля приходят только из actual verified connector context + fresh private Root authority read. Native поля — из constructor-branded proof и actual final Native SQL; их нельзя передать в HTTP. `linkDigest` фиксирует exact `(issuer,sub,principalId)`, `selectedConsentDigest` — exact существующую Source consent row; `finitePurposeDigest` включает branded policy. Birth family generation=1 и новый random session hash фиксируют конкретное согласие на этот срок; revoke увеличивает generation и никогда не реактивирует этот family ID.

`anchorKey=canonicalHash(anchor)`, RP marker `bindingDigest=canonicalHash({anchor,sessionHash,t0,absoluteEnd})`. Отдельный immutable `actor_key=canonicalHash({realmId,root.accountId,root.deviceId})` нужен для атомарного per-actor capacity SQL; он вычисляется только из verified host proof и не выдаётся в public DTO. RP `profile_digest` фиксирует exact approved RP/client/Source semantic policy, а не full Root target artifact tuple. Hash служит equality/locator, не доказательством прав. Поиск допускает ровно один active exact anchor; missing/ambiguous/tombstoned/future shape — login-required/not-ready. Чужой Root account или другое устройство того же account не находят family. Native role/revision/session/link/key/realm change не ретаргетит family.

Root target revision/digest и full launch profile digest **не входят в Native purpose** и immutable family anchor. Они обязательны в каждой alias/current launch. Новое одобренное UI-only target получает новый Root launch/admission; прежний Native consent может использоваться только после всех новых Root/Source/current Native проверок и неизменного semantic anchor. Автоматический rebind при target change закрыт. Resource/realm/purpose/client/source semantic change требует нового явного consent.

## Закрытые private consumer ports

Предлагается `createOrdinaryFiniteSessionConsumer({policy,store,rootProof,native,rpSessions})`. Он не экспортирует credentials или universal proxy. Все context/proof/prepared values — opaque WeakMap-branded объекты экземпляра; JSON clone, другой consumer и утёкший callback отвергаются. Маркеры для RP49 остаются host-only; отсутствующий consumer не переключает Basic в Long.

| Port | Точный смысл |
| --- | --- |
| `rootProof.capture(verifiedContext)` → `RootProof` | Только context от реального verifier; `assertCurrent(RootProof)` свежо перечитывает тот же installed channel и exact context. Не принимает account/device/body claims. |
| `prepareLogin({interaction,RootProof,verifiedOidc,NativeProof})` → `PreparedLogin` | Вне SQL: actual maintained OIDC/RT shape, original t0, exact approved client, current Root/Native; вычисление только bounded inputs. RT/AT остаются private. |
| `commitLogin(PreparedLogin,finalNative)` → `CommitReceipt` | Одна Source `BEGIN IMMEDIATE`: current interaction CAS, current Native final link/consent, birth anchor, new Long Session + family + RP head + original alias + immutable receipt. Окончательные Native IDs/deadline берутся из actual final link; guest preparation не выдумывает их заранее. Синхронные encrypt/AAD и final Native assertion внутри той же transaction; IO/OIDC/renew отсутствуют. Callback exactly once, synchronous, non-reentrant, permanently closed после return. |
| `prepareResume({RootProof,requestId})` → `PreparedResume` | Exact unique anchor lookup — только locator. Fresh RP49 currentProof + actual userinfo, fresh Native proof и Root до/после awaits. Нельзя использовать `unknown` head для **нового** alias; существующее AT поведение RP49 на старом slot не расширяется. |
| `commitResume(PreparedResume,finalNative)` → `ResumeReceipt` | Одна transaction повторно проверяет anchor/head/family generation/current Native proof/current Source key и reserve capacities, создаёт один exact alias+receipt. Old slot не становится Root authority. |
| `readResumeReceipt({RootProof,requestId})` → `receipt \| unknown` | Read-only exact intent/context/current Native/RP проверка. Не создаёт alias, не вызывает apply/RT/code exchange. Expired/missing receipt не доказывает not-applied. |
| `proveAlias({RootProof,privateCookie})` → `CurrentSourceProof` | Exact current alias/Root ref + active family/head/Source session + fresh userinfo/current Native. Cookie или family locator сам ничего не разрешает. |
| `revokeFamily(FamilyProof)` | Source-owned current actor policy; final SQL family/head revoke + generation++, aliases становятся недействительны. Root App owner не заменяет Native authority. |

`finalNative` не raw function от запроса: private commit port Source Native kernel с current row checks внутри реальной transaction. Async PG вариант сначала держит actual Native row locks, потом вызывает sync branded witness и повторную temporal проверку перед COMMIT. Постпроверка после awaited commit не откатывает эффект: late Root revoke/неизвестный ACK → unknown и exact receipt recovery.

RP49 storage consumer реализует существующие `read/captureSourceAuthority/assertSourceAuthority/claim/finish/block`, не создаёт второй RT head. `claim` и `finish` проверяют точный family/head/Native generation и expiry внутри Source SQL; RT/network вне transaction. `block` только закрывает уже начатый exact claim без требования still-active прав, никогда не открывает head. Source restart `refreshing` → bounded wait/unknown; stale claim не переходит к другому writer. Unknown old RT никогда не отправляется повторно. Exact local already-committed next head можно прочитать по `lastAttemptId`, как в RP49.

## Требуемые изменения к предложению SQL58, до writer review

Четыре новых таблицы из SQL58 сохраняются как назначение, но ещё не являются принятой literal schema; `source_long_families` получает явный immutable hash column `actor_key` для capacity. Все29 Source3 definitions кроме explicit `native_meta` остаются точными. Старые Source3 rows/cipher/FKs/Native IDs не переписываются. Предлагаемый следующий DDL должен явно фиксировать:

1. Family birth trigger проверяет `source_sessions.active=1`, identical expiry и matching Native session/principal/resource; head birth проверяет identical family/source-session t0/end/profile/binding. Absolute end=`min(original authorized t0+86400000,current Native session deadline)` и никогда не UPDATE. AT expiry ≤ end, alias/receipt expiry ≤ actual Root slot expiry и end; прежние `+301000` заменить точным `+300000` bound.
2. Family закрытое состояние active→revoked с generation+1; дальше нет реактивации. Удалять tombstone можно только после immutable end, отсутствия referenced alias/receipt/head и невозможности живого original interaction. Новый явный login может атомарно revoke старую active family и создать **новый** hash после fresh two proofs; old exact intent его не создаёт.
3. Head transitions закрыты: idle→refreshing сохраняет revision/proof; exact refreshing→idle требует revision+1 и exact claim/lastAttempt; refreshing→unknown сохраняет старый proof/revision; idle/refreshing/unknown→revoked допустим только как закрытие. unknown→idle и revoked→любое состояние запрещены. CAS сравнивает все head поля; Reader отвергает незнакомые состояния/claim tuple.
4. Admission/GC/reserve — одна transaction. Total retained families/heads ≤4096 (включая unexpired tombstones), active per exact Root account/device ≤8, total aliases ≤256 и total receipts ≤256. GC batch≤128, receipt удаляется до FK alias; unknown head не удаляется ради свободного места. Capacity failure оставляет старые записи/права и возвращает not-ready, без частичного alias/head.
5. Family cipher AAD: existing Source realm/key/model + session hash + fixed birth revision0. Head cipher AAD: `sourceRpCipherBinding(marker,revision)` дополнительно внутри Source realm/key/model. Alias AAD: session hash+exact ref digest; receipt AAD: session hash+request digest. Cipher records не header/body authority; ciphertext key probe расширяется только на **известные** новые models после reviewed format4.

Количество SQL объектов/полный literal Reader4 vector пересчитывается после этих явных triggers; нельзя объявлять прежние40 объектов уже корректным Reader4. До любых writes4 обязательны отдельные current/compatible-reader images, independent literal reader4 и external old-reader3 refusal **before START**. Reader4 readonly snapshot проверяет FK, cardinalities, exact SQL, cross-table expiry/key/binding/claim/generation consistency и closed decrypt shapes с known AAD. Пустой namespace доказывает shape, не key correctness.

## Фактические проверки до Ready

| Gate | Что должно быть наблюдаемо |
| --- | --- |
| Basic regression | Existing Source3 bytes/expiry/original-ref; expired Basic new-ref deny; новый явный login не создаёт второго Native principal/owner. |
| Два realm, без Root diff | Actual Root24h Human approval, actual Native24h page, maintained OIDC callback, source-owned final SQL; между подключениями только private approved config, Root code/DDL unchanged. |
| Два OS writer | Одно initial interaction/link/head/receipt; один RT send; conflict/capacity/claim revoke на actual SQL boundary, no stale takeover. |
| Wall/restart | >300s через current installed channel, actual Root new slot/private Source ACK/same iframe, Source restart с теми же DB/key/Native grants; не обычный Native API key. |
| Unknown | Code/RT delivery unknown → новый явный login; COMMIT ACK loss → только exact receipt read. Старый ref, другой actor/device/target/source, tombstone/future head, revoked key/session/consent deny. |
| Capture | Fresh ACK both actual Root+Source access remaining≥190s, one fixed trusted RP minimum190 attempt; short provider AT/absolute end deny. Capture lease не продлевает deadline, source/actor change cancel. |
| Physical cold | Два fresh physical volumes, encrypted coherent Source4/config/native/RP restore, exact current+compatible reader, old3 beforeSTART refusal, wrong key/realm/FK/expiry/claim vectors deny. TMPDIR/RAM тесты не заменяют этот gate. |

## Понятное поведение человеку

Первый вход: «Войти через Соты» → «Подключить выбранный ресурс на этом устройстве до 24 часов» с текущими правами → готово. Обычное повторное открытие/смена Root slot использует проверенный current family без повторения Native consent. После отзыва, смены устройства/аккаунта, semantic change или окончания срока показывается «Нужен вход» с одной явной кнопкой; существующий профиль не создаётся заново. При unknown: «Подключение пока не подтверждено» + «Проверить подключение» читает тот же receipt; не запускает новый скрытый intent.

Сейчас реализованы Basic300/new explicit login, RP49 kernel и Source3 installation. Этот Long consumer, format4 migration/reader/images, wall/restart/cold gates **ещё не реализованы**. Фраза «до24 часов» означает верхний finite bound, не SLA непрерывного входа: provider outage, early Native expiry/revoke и short AT могут завершить доступ раньше.
