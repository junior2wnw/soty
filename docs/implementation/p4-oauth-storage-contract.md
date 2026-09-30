# P4-C1 — Capabilities v3: OAuth storage и закрытый domain port

30.09.2026. Контракт к [connection plan](p4-oauth-connection-plan.md), после принятого root [spike oidc-provider 9.12.2](p4-oauth-provider-seam.md). Это **design/API freeze для review**, а не реализованные DDL, production adapter или reader3. Domain production source пока не менялся. Исторические v2 source files скопированы из Git (§10); их миграторы в этой работе не запускались.

Сохраняются Connect account, существующие client/principal/grant/credential/budget/Invocation, Notes v2, Native B2 и semantic digest `notes.createDraft@1` `95008a3424e375b6bdefec6e41bbdfb411dc98e6b4fd4505f387ce552c162204`. C1 добавляет persistent AS artifacts и проверяемую связь с этими правами. Отдельного account/effect/retry ledger нет.

## 1. Принятые константы и единицы

- Provider profile: `oidc-provider-9.12.2-c1`; шесть моделей: `Session`, `Interaction`, `Grant`, `AuthorizationCode`, `RefreshToken`, `AccessToken`. Static profiles ровно `soty-codex-cli` и `soty-opencode-cli`. Client ID публичен, не идентичность executable.
- Scope ровно `notes.createDraft`. HTTP resource — прежний validated `capabilityAudience`; MCP — отдельный `shellOrigin + '/mcp'`. Один connection/code/RT/AT имеет один resource. Issuer — validated `shellOrigin + '/oauth'`; Host/Forwarded не источник issuer.
- Все SQLite времена — безопасные целые **миллисекунды** `0..9007199254740991`. Provider `iat`, `exp`, `consumed`, `authTime`, `iiat`, `loginTs` — целые **секунды**; преобразование проверяется на overflow. `consumed` отдаётся библиотеке как `floor(consumed_at/1000)`, никогда как boolean.
- Interaction/Session — максимум 600 s; code — 60 s; AT — 300 s; RT/Grant — не позже неизменяемого конца connection. Consent C1: не более 24 h и 20 вызовов, меньшие положительные значения допустимы в server-owned presentation. Срок root grant определяется один раз при approve; refresh его не продлевает.
- Host явно задаёт `expiresWithSession:()=>false`: token lifetime зависит от durable connection и текущей authority, не от AS Session. Default pinned provider равен true без offline_access; его Session.findByUid/grantId check иначе ломает600s/24h и независимость двух согласий одного static profile. Scope offline_access ради обхода этого default не добавляется.
- Все ID — own primitive strings, без coercion, ≤160 ASCII characters; provider IDs только base64url, 16..160. Hash — lowercase SHA-256 hex64. Native Invocation request key остаётся прежним `canonicalHash(idempotencyKey)`. OAuth opaque token profile задаёт 256 bits of randomness; AT/RT/code имеют 43 base64url characters. Session/Interaction IDs допускают более короткие штатные provider IDs.
- URI — own well-formed string, ≤2048 UTF-8 bytes, exact canonical value из trusted issuer/resource/redirect allowlist. Нет userinfo, raw fragment или wildcard redirect. Текст/JSON этой новой области well-formed Unicode; это не изменение строковой семантики capability `@1`.

## 2. Открытие и migration admission

`CAPABILITIES_SUPPORTED_SCHEMA_VERSIONS = [1,2,3]`, actual schemaVersion честно возвращается service. Новый формат: `PRAGMA user_version=3`, `cap_metadata.lineage='soty.capabilities.sqlite.v3'`. Metadata имеет **те же три** ключа v2: `lineage`, `project_id`, `registry_id`; никакого нового registry ID или изменённого semantic pin. Старые 13 tables, 12 explicit indexes и 11 guards остаются прежними SQL objects; добавления перечислены ниже.

Новый `allowOAuthMigration=false` — strict boolean, независимый от `allowNativeMigration`. Fresh/v1/v2 при default-off остаются actual1/1/2. Actual3 читается при default-off и при отсутствии AS keys. Только существующий exact v2 и явный `allowOAuthMigration:true` получают 2→3. Fresh/v1 + OAuth flag дают `oauth_migration_requires_native_v2` **до persistent pragmas/DDL**: сначала отдельный явный native1→2 checkpoint, затем reopen/upgrade. Установка сразу двух flags на v1 не делает скрытую цепочку migrations. Known actual3 + true — проверяемый no-op.

Exact format recognizer вызывается до persistent pragmas и повторно внутри `BEGIN IMMEDIATE`; затем foreign keys, прежние v2 native invariants и новые plain row invariants. Migration создаёт только пустые OAuth tables/indexes/guards и обновляет lineage/user_version одним COMMIT. Unsupported/future4, missing/extra objects, altered guards, partial3, неверный project/registry отказываются без repair/default rows. Не обещается общий физический integrity scan или расшифровка artifacts без AS key.

**Обязательная совместимость B2:** `native-notes.mjs:capsIdentity()` сейчас hardcoded2, а root scheduler в `server/http-app.js` — Caps2 only. Domain передаёт native coordinator exact admitted version 2 либо3 и требует соответствующую lineage, прежний registry/project/Notes2/digest; runtime version drift до reopen отказывает. Root включает recovery для Notes2 + known Caps2/3. `>=2` запрещено. `validateNativeRows` выполняется также на3. OAuth-off reader3 обязан учитывать сохранённые OAuth connection/credential pins в original authorization; availability AS не превращает их в обычные service credentials.

## 3. Новые DDL objects

Ниже фиксируются columns, storage classes, keys, constraints и index order. Фактические guard SQL bodies реализуются ровно по predicates §4 и замораживаются для независимого reader **после** source review; текущий документ не разрешает заранее объявить image совместимым3. `TEXT` ID/URI имеют дополнительные primitive/ASCII/UTF-8/allowlist проверки §1 в domain admission и full row recognizer. Они не подменяются проверкой `length()` SQLite.

```sql
CREATE TABLE cap_oauth_connections(
  id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  client_id TEXT NOT NULL UNIQUE REFERENCES cap_clients(id),
  principal_id TEXT NOT NULL UNIQUE REFERENCES cap_principals(id),
  root_grant_id TEXT NOT NULL UNIQUE REFERENCES cap_grants(id),
  creator_device_id TEXT NOT NULL,
  issuer TEXT NOT NULL,
  static_client_id TEXT NOT NULL
    CHECK(static_client_id IN ('soty-codex-cli','soty-opencode-cli')),
  resource TEXT NOT NULL,
  scope TEXT NOT NULL CHECK(scope='notes.createDraft'),
  consent_digest TEXT NOT NULL
    CHECK(length(consent_digest)=64 AND consent_digest NOT GLOB '*[^0-9a-f]*'),
  provider_grant_id TEXT UNIQUE,
  state TEXT NOT NULL CHECK(state IN ('active','revoked')),
  created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),
  expires_at INTEGER NOT NULL CHECK(expires_at BETWEEN 1 AND 9007199254740991),
  revoked_at INTEGER CHECK(revoked_at BETWEEN 0 AND 9007199254740991),
  CHECK(expires_at>created_at AND expires_at-created_at<=86400000),
  CHECK((state='active' AND revoked_at IS NULL)
     OR (state='revoked' AND revoked_at IS NOT NULL AND revoked_at>=created_at))
) STRICT;
CREATE INDEX cap_oauth_connections_account
  ON cap_oauth_connections(account_id,created_at,id);

CREATE TABLE cap_oauth_interactions(
  uid_hash TEXT PRIMARY KEY
    CHECK(length(uid_hash)=64 AND uid_hash NOT GLOB '*[^0-9a-f]*'),
  issuer TEXT NOT NULL,
  static_client_id TEXT NOT NULL
    CHECK(static_client_id IN ('soty-codex-cli','soty-opencode-cli')),
  resource TEXT NOT NULL,
  redirect_uri TEXT NOT NULL,
  request_digest TEXT NOT NULL
    CHECK(length(request_digest)=64 AND request_digest NOT GLOB '*[^0-9a-f]*'),
  browser_nonce_hash TEXT NOT NULL
    CHECK(length(browser_nonce_hash)=64 AND browser_nonce_hash NOT GLOB '*[^0-9a-f]*'),
  duration_ms INTEGER NOT NULL CHECK(duration_ms BETWEEN 1000 AND 86400000),
  budget_limit INTEGER NOT NULL CHECK(budget_limit BETWEEN 1 AND 20),
  created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),
  expires_at INTEGER NOT NULL CHECK(expires_at BETWEEN 1 AND 9007199254740991),
  decision TEXT NOT NULL CHECK(decision IN ('pending','approved','denied')),
  decided_at INTEGER CHECK(decided_at BETWEEN 0 AND 9007199254740991),
  decided_account_id TEXT,
  decided_device_id TEXT,
  connection_id TEXT UNIQUE REFERENCES cap_oauth_connections(id),
  CHECK(expires_at>created_at AND expires_at-created_at<=600000),
  CHECK((decision='pending' AND decided_at IS NULL AND decided_account_id IS NULL
          AND decided_device_id IS NULL AND connection_id IS NULL)
     OR (decision='approved' AND decided_at IS NOT NULL AND decided_account_id IS NOT NULL
          AND decided_device_id IS NOT NULL AND connection_id IS NOT NULL)
     OR (decision='denied' AND decided_at IS NOT NULL AND decided_account_id IS NOT NULL
          AND decided_device_id IS NOT NULL AND connection_id IS NULL)),
  CHECK(decided_at IS NULL OR (decided_at>=created_at AND decided_at<expires_at))
) STRICT;
CREATE INDEX cap_oauth_interactions_expiry
  ON cap_oauth_interactions(expires_at,uid_hash);

CREATE TABLE cap_oauth_artifacts(
  model TEXT NOT NULL CHECK(model IN
    ('Session','Interaction','Grant','AuthorizationCode','RefreshToken','AccessToken')),
  id_hash TEXT NOT NULL
    CHECK(length(id_hash)=64 AND id_hash NOT GLOB '*[^0-9a-f]*'),
  issuer TEXT NOT NULL,
  profile TEXT NOT NULL CHECK(profile='oidc-provider-9.12.2-c1'),
  key_id TEXT NOT NULL,
  payload_cipher BLOB NOT NULL CHECK(length(payload_cipher) BETWEEN 30 AND 16412),
  payload_digest TEXT NOT NULL
    CHECK(length(payload_digest)=64 AND payload_digest NOT GLOB '*[^0-9a-f]*'),
  connection_id TEXT REFERENCES cap_oauth_connections(id),
  provider_grant_id TEXT,
  session_uid_hash TEXT CHECK(session_uid_hash IS NULL OR
    (length(session_uid_hash)=64 AND session_uid_hash NOT GLOB '*[^0-9a-f]*')),
  created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),
  expires_at INTEGER NOT NULL CHECK(expires_at BETWEEN 1 AND 9007199254740991),
  retain_until INTEGER NOT NULL CHECK(retain_until BETWEEN 1 AND 9007199254740991),
  consumed_at INTEGER CHECK(consumed_at BETWEEN 0 AND 9007199254740991),
  PRIMARY KEY(model,id_hash),
  CHECK(expires_at>created_at AND retain_until>=expires_at),
  CHECK((model IN ('Session','Interaction') AND connection_id IS NULL AND provider_grant_id IS NULL)
     OR (model IN ('Grant','AuthorizationCode','RefreshToken','AccessToken')
          AND connection_id IS NOT NULL AND provider_grant_id IS NOT NULL)),
  CHECK(consumed_at IS NULL OR
    (model IN ('AuthorizationCode','RefreshToken')
      AND consumed_at>=created_at AND consumed_at<expires_at)),
  CHECK(model!='Session' OR session_uid_hash IS NOT NULL)
) STRICT;
CREATE INDEX cap_oauth_artifacts_retention
  ON cap_oauth_artifacts(retain_until,model,id_hash);
CREATE INDEX cap_oauth_artifacts_connection
  ON cap_oauth_artifacts(connection_id,model,expires_at,id_hash);
CREATE INDEX cap_oauth_artifacts_grant
  ON cap_oauth_artifacts(provider_grant_id,model,id_hash);
CREATE UNIQUE INDEX cap_oauth_artifacts_session
  ON cap_oauth_artifacts(session_uid_hash) WHERE model='Session';

CREATE TABLE cap_oauth_credentials(
  credential_id TEXT PRIMARY KEY REFERENCES cap_credentials(id),
  connection_id TEXT NOT NULL REFERENCES cap_oauth_connections(id),
  token_digest TEXT NOT NULL UNIQUE
    CHECK(length(token_digest)=64 AND token_digest NOT GLOB '*[^0-9a-f]*'),
  created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),
  expires_at INTEGER NOT NULL CHECK(expires_at BETWEEN 1 AND 9007199254740991),
  CHECK(expires_at>created_at AND expires_at-created_at<=300000)
) STRICT;
CREATE INDEX cap_oauth_credentials_expiry
  ON cap_oauth_credentials(expires_at,credential_id);
CREATE INDEX cap_oauth_credentials_connection
  ON cap_oauth_credentials(connection_id,credential_id);

CREATE INDEX cap_invocations_original_credential
  ON cap_invocations(json_extract(authorization_json,'$.credentialId'));
CREATE INDEX cap_invocations_oauth_request
  ON cap_invocations(account_id,request_key,client_id);
```

Всего **4 tables + 10 explicit indexes** поверх v2; SQLite autoindexes от PK/UNIQUE не считаются explicit. Expression index допускается только после проверки existing authorization JSON при 2→3, чтобы malformed row не вызвал half-migration; отказ откатывает всю migration. Он нужен существующему retention lookup по исходному credential, а не второму ledger. Второй Invocation index ограничивает account/key lookup и покрывает clientId для durable OAuth join (§8). Его окончательное сохранение подтверждается `EXPLAIN QUERY PLAN` на значимой fixture, а не совпадением строки index name с тестом.

## 4. Immutable guards и row invariants

Границы same-account/client/principal/root/creator проверяются **SQL admission guards + full reopen recognizer + общим current credential resolver**. Родительские v2 tables не получают новые дублированные authority columns или изменённые CREATE TABLE. FK дают identity existence; guards проверяют полный tuple. Не создавать четыре искусственных UNIQUE parent indexes только ради той же связи.

| Exact новый guard name | Обязательный predicate/effect |
|---|---|
| `cap_oauth_connection_admission` | INSERT connection требует same-account client + service principal + root grant. Client/principal active; одинаковые client/principal/creator; root self/depth0/parent null, delegation false/maxDepth0, exact Notes@1 scope/resources/effects/recipients, expiry равен connection; существующий invocations budget 1..20. New connection active, providerGrantId null. Internal client создаётся в той же approval операции; до привязки у него нет Invocation, credentials и других principals/grants кроме связываемого tuple. |
| `cap_oauth_connection_no_replace` | BEFORE INSERT отказывает при коллизии id, client_id, principal_id, root_grant_id или nonnull provider_grant_id; `INSERT OR REPLACE` не обходит immutability при выключенных recursive_triggers. |
| `cap_oauth_connection_no_delete` | Durable connection pins не удаляются. Их bound 10 000/global явен; они нужны historical authority и crossconnection key conflict. |
| `cap_oauth_connection_update_guard` | Все pins/created/expiry/consent неизменны. providerGrantId только null→одно значение, затем неизменен; при связывании уже существует matching Grant artifact этой connection/issuer/providerGrantId. Active→revoked один раз, revokedAt write-once; обратного перехода нет. |
| `cap_oauth_interaction_no_replace` | Повтор PK запрещён; API exact повтор читает существующий row, а не заменяет его. |
| `cap_oauth_interaction_update_guard` | Request/browser/limits/expiry pins неизменны. Только pending→approved либо denied; решение/actor/time/connection после этого неизменны. Approved connection имеет тот же issuer/profile/resource/consent digest/account/creator и утверждённые limits. |
| `cap_oauth_artifact_no_replace` | Повтор `(model,id_hash)` через REPLACE запрещён; обычный checked upsert использует UPDATE. |
| `cap_oauth_artifact_update_guard` | model/id/issuer/profile/key/connection/providerGrant/sessionUid/created/retainUntil immutable. consumed null→время один раз, затем exact; нельзя убрать consumption переписыванием payload. У code/RT/AT/Grant expiry и payload_digest immutable. Session/Interaction могут менять только проверенный payload и expiry в пределах первоначального 600s окна; их expiry никогда не становится бессрочным. |
| `cap_oauth_credential_admission` | Link требует существующие AT artifact этой connection и cap credential с теми же digest/account/client/principal/rootGrant/audience/created/expiry. Connection bound/active, expires не позже неё. SQL сравнивает plain projected pins; расшифровку/полный payload проверяет domain до INSERT. |
| `cap_oauth_credential_no_replace` | Запрещены коллизии credential_id и token_digest, включая REPLACE. |
| `cap_oauth_credential_no_update` | Link immutable целиком; refresh всегда другая credential, не UPDATE expiry. |
| `cap_oauth_credential_delete_guard` | DELETE link запрещён при любом Invocation reference на credentialId, включая terminal/purged input. Cleanup сначала проверяет expiry и отсутствие references, затем удаляет link+credential одной transaction. |
| `cap_oauth_credential_row_guard` | UPDATE существующей cap_credentials, имеющей OAuth link, не меняет id/digest/account/client/principal/grant/audience/created/expiry; revokedAt только null→value, затем write-once. |
| `cap_oauth_credential_row_no_replace` | BEFORE INSERT cap_credentials запрещает REPLACE по id/digest существующей OAuth-linked credential. |

Всего **14 новых guards**; DDL names/назначение входят в будущий recognizer. Полная SQL literal freeze делается перед reader work. Clock-dependent admission/expiry/ephemeral artifact deletion решения принадлежат API с injected trusted clock и одной Caps transaction; SQL не получает clock-dependent CHECK или обходной cleanup function. `destroy` code/RT сначала durably отзывает family; cleanup до retainUntil при live family запрещён самим закрытым port. Удаление произвольным внешним SQLite writer не является поддержанным cleanup API; recognizer не обещает обнаружить отсутствующий ephemeral artifact. Неизменяемые business pins/reference links защищены отдельно.

На reopen проверяются связи всех новых rows, bounded types/URI/profile, fixed grant tuple, immutable credential equality и spent/reserved прежним кодом. Существование AT artifact обязательно **при первоначальном INSERT link**, но не на reopen: retained original credential/link законно переживают expired encrypted artifact. Если artifact ещё существует, его plain pins обязаны совпадать. Active connection может иметь отозванный обычным owner API root/client/principal: это нормальный **неживой** grant, не повреждение storage; computed admission отказывает. Revoked connection требует закрытого root grant и OAuth credentials; terminal history остаётся. Reader без storage key проверяет формат/cipher bounds/plain pins, но не объявляет encrypted payload валидным: AS `find/upsert` расшифровывает и проверяет digest/full shape перед использованием.

## 5. Payload profile по фактическому Provider

Root spike использовал настоящие authorize/token/revoke HTTP, две Provider/SQLite instances и synthetic consent. Итоговый log: **4/4 PASS, 0 skips, 975.1677 ms**; ранний промежуточный run был другим, source receipt ссылается на final log. Из него взяты names-only поля; значения tokens/PKCE/cookies не выводились. Это не signed Connect/UI/two-OS-process proof.

Последующий root gate `output/implementation-20260930/p4-oauth-session-profile-green.log`: **11/11 PASS, 0 skips, 1942.8627 ms** (6 Provider +5 ingress). Сначала actual `expiresWithSession:true` дал RED: first RT400 при двух неотозванных families; explicit false дал GREEN — два fresh consent одного profile, Session.destroy, refresh обеих, revoke одной и refresh sibling. Уточнён test adapter: borrowed Interaction.grantId не является family authority, destroy Interaction не отзывает Grant. Имена полей с false profile уже **не содержат expiresWithSession** у code/RT/AT. Это выполнено root, автор документа прочитал log; 600s wall-clock expiry и resetIdentifier lifetime не заменяются immediate destroy опытом.

Все payload — plain own data objects без getters/prototype keys, arrays только dense; canonical JSON ≤16 384 UTF-8 bytes, ≤1024 nodes/depth≤12. Mandatory fields не могут быть undefined/null. У **документированных optional** fields `undefined` удаляется при snapshot, как штатная JSON serialization (`AccessToken.extra` реально присутствует с undefined); неизвестный key не маскируется его undefined. No coercion и no arbitrary new model. `kind===model`, `jti===id`, iat/exp finite safe integer seconds, iat≤now и exp>now; ttl если передан — positive finite numeric duration, но не authority для продления payload.exp. Отсутствующий ttl не означает infinite: source BaseToken допускает его, хотя все наблюдённые spike writes имели конечный ttl.

| Model | Фактически наблюдённые поля кроме `iat,exp,jti,kind` | Допустимые bounded optional формы / pins |
|---|---|---|
| `Interaction` | `cid,params,prompt,result,returnTo` | `session,grantId,lastSubmission,trusted` разрешены для повторного штатного interaction. `params` — fixed code/S256/static client/redirect/resource/scope/state profile; raw state и challenge остаются только в encrypted payload. `prompt` содержит name/reasons/details; bounded provider UI metadata не становится authority. `result/lastSubmission` только login/consent/error формы этого flow, login account сверяется с approved connection при completion. `session` snapshot account/uid/cookie/acr/amr не даёт права автоapprove. `deviceCode/parJti` и другие отключённые features запрещены. returnTo только generated provider resume URL этого issuer. |
| `Session` | `accountId,authorizations,loginTs,transient,uid` | До login account/authorizations могут отсутствовать. Optional `acr,amr,state` — bounded primitive/list/record по library schema. authorizations — только два static client keys и `{sid?,grantId?,persistsLogout?:boolean}`; каждый grantId связан с той же account. `persistsLogout:true` реально добавляется process_response_types.js при явном expiresWithSession:false; это не новая user authority. Session не authentication proof Connect и не remembered consent. Same uid соответствует единственной Session row; provider resetIdentifier сначала destroy старой row. |
| `Grant` | `accountId,clientId,resources` | Ровно один `resources[connection.resource]='notes.createDraft'`; openid/rejected/rar не выдаются C1 и не принимаются непустыми. Новая Grant требует live stagedGrant; существующая — exact durable bound tuple. No scope expansion или expiry renewal. |
| `AuthorizationCode` | `accountId,authTime,clientId,codeChallenge,codeChallengeMethod,expiresWithSession,grantId,redirectUri,resource,scope,sessionUid` | `sessionUid/authTime/expiresWithSession` могут отсутствовать в direct trusted fixture, но не дают отдельной authority. Optional consumed только через persisted consumed_at; S256 и registered redirect exact. OIDC/DPoP/mTLS/RAR/attestation поля C1 не принимает. |
| `RefreshToken` | `accountId,authTime,clientId,expiresWithSession,grantId,gty,iiat,resource,rotations,scope,sessionUid` | `gty` — library code/refresh lineage string, bounded; `rotations` nonnegative integer, `iiat` original seconds≤iat; отсутствие допускается для первого token. Resource ровно scalar string этой connection, не Array. Optional consumed материализуется из column. Нет expiry после connection end, дополнительных scopes/nonce/claims/DPoP/RAR. |
| `AccessToken` | `accountId,aud,clientId,expiresWithSession,extra,grantId,gty,scope,sessionUid` | `aud` — один exact resource string, `extra` absent/undefined либо empty plain object. `expiresWithSession/sessionUid` могут отсутствовать; профиль C1 не делает browser session сроком domain credential. Нет extra claims, JWT envelope, opaque→JWT format fallback или массива audiences. |

`Grant`, code/RT/AT payload authority проверяется против **plain immutable connection + реальных cap rows**, а не наоборот. Library fields `accountId/clientId/grantId/aud/resource/scope` не разрешают найти произвольный другой account. Host wrapper извлекает authenticated static client и parsed params из публичного `Provider.ctx` на каждом вызове; raw Koa context не сохраняется глобально.

В таблице имена `expiresWithSession` описывают observed spike с прежним default. Production C1 принимает это поле только absent/false; true отказывает как несовпадение configured profile. Token.exp, connection/root/creator и common resolver остаются authority. Проверяется настоящий flow после исчезновения AS Session и два согласия одного static profile; seeded code fixture без session не доказывает эту границу.

Session остаётся короткой вспомогательной AS записью: host задаёт bounded TTL так, чтобы повторный `Session.save()` не продлевал первоначальное 600s окно существующего payload; прежний exp при обычном resave сохраняется. Для Session/Interaction `retain_until` фиксирует первоначальный абсолютный предел, и новый exp не может его пересечь. При истечении штатно создаётся новая session/interaction с новым fresh consent. Library `BaseModel.save(ttl)` действительно пересчитывает exp для Session/Interaction; одного хранилищного запрета без host TTL policy недостаточно. Это отдельная узкая host integration assertion, не доказанная synthetic spike с ttl=3600. Host вычисляет TTL по existing exp/iat до save, не заставляет storage молча clamp payload. `resetIdentifier` не сбрасывает исходный iat/absolute window; actual library regression обязателен.

## 6. Closed JS API и transaction ownership

`createCapabilitiesService` получает `allowOAuthMigration` и optional trusted `oauth` composition. Export `OAUTH_OPERATIONS` содержит только signed owner operations ниже. Domain не импортирует `oidc-provider`; AS ошибки создаёт root adapter. Schema3 current credential checks работают даже без `oauth` composition.

```js
oauth: {
  issuer,                         // validated exact issuer URI
  resources: { http, mcp },        // exact distinct allowlist values
  withAuthorityFence,             // existing synchronous Connect host fence
  isRegisteredRedirect,           // same static client config, sync literal boolean
  artifactKey,                   // optional copied Uint8Array, exactly32 bytes
  artifactKeyId: 'configured-id',  // ASCII 1..64; required together with key
}
```

Key копируется в закрытое хранилище; не возвращается, не включается в error/log. Одной пары issuer/resources достаточно для safe bearer resolution/replay; нет key — AS artifact read/write/issuance `oauth_unavailable`, но обычный reader3/owner history/internal native proof не зависят от decrypt. `oauth` отсутствует — внешние OAuth ports unavailable, **сохранённые** OAuth restrictions в access resolver всё равно действуют. Unknown keys/thenable fence/неправильные URI/byte key отвергаются до mkdir/DB.

Host-only `service.oauth`:

```text
readiness() -> {schemaVersion, available:boolean}
prepareInteraction({interactionId,browserNonce,durationMs=86400000,budgetLimit=20})
  -> {interactionId,contextDigest,clientProfile,resource,scope,durationMs,budgetLimit,
      expiresAt,checkedAt,decision:'pending'|'approved'|'denied',decidedAccountId:string|null}
readInteraction({interactionId,browserNonce}) -> та же безопасная presentation
beginGrantBinding({interactionId,browserNonce})
  -> {context,providerGrantId:string|null,
      connection:{id,accountId,staticClientId,issuer,resource,scope,expiresAt}}
endGrantBinding(context) -> void
authenticateBearer({token,audience}) -> existing branded service actor
cleanup({limit=64}) -> {artifactsDeleted,interactionsDeleted,credentialsDeleted}
artifactStore.upsert({model,id,payload,expiresIn?,request?,stagedGrant?}) -> void
artifactStore.find({model,id}) -> checked cloned payload | undefined
artifactStore.findByUid({uid}) -> checked Session payload | undefined
artifactStore.consume({model,id,request})
  -> {status:'consumed'|'invalid_grant'|'invalid_target'}
artifactStore.destroy({model,id}) -> void
artifactStore.revokeByGrantId({providerGrantId}) -> void
```

`Client.find` unknown и `findByUserCode` wrapper возвращает undefined без DB lookup; unknown mutable model — отказ. `request` — свежий snapshot `{clientId,resource?,scope?,grantType?}` из public Provider.ctx, не объект HTTP body и не actor. Для code/RT consume обязательны authenticated clientId + grantType authorization_code/refresh_token соответствующей модели; optional resource/scope только exact own strings. AT/RT/code upsert проверяет current request/client/pins; Grant упоминает staging только при первом сохранении; Session/Interaction не превращают request в actor. Полный raw request, tokens и callback Functions не сериализуются.

`prepareInteraction` **читает настоящий encrypted Interaction artifact**, проверяет exact code params/expiry и server redirect allowlist, сохраняет request pins и SHA256(browserNonce), а не принимает account/client/resource из arbitrary UI. Не вводится второй список redirects: `isRegisteredRedirect({clientId,redirectUri}) -> literal true/false` — closed sync callback по той же static client configuration root. Domain самостоятельно проверяет URI/loopback форму и отсутствие credentials/fragment; callback не может разрешить иной resource/scope/account. Nonce — host-generated 32 bytes base64url, bound cookie; повтор exact uid/nonce/limits возвращает прежнее предложение, иное — conflict. Context digest — canonical hash `['soty.oauth-consent.v1', issuer, uidHash, staticClientId, redirectUri, resource, scope, codeChallenge, 'S256', state === undefined ? null : SHA256(state), durationMs, budgetLimit, expiresAt]`; raw state не попадает в presentation. Optional state — well-formed string≤512 UTF-8 bytes, без coercion; provider возвращает исходное значение.

Уточнение после независимого UX review: presentation содержит `checkedAt` из domain clock текущего чтения и `decidedAccountId` (null до решения, точный signed actor account после решения). Эти поля не меняют digest/DDL и не дают новых полномочий. Browser использует server remaining time с консервативным вычитанием network elapsed и monotonic clock, а не часы компьютера. При смене локального аккаунта завершение решения другого аккаунта блокируется; same-origin completion form несёт только `expectedAccountId`, который host сравнивает с durable decidedAccountId перед binding. Raw code, state и секреты в форме не передаются. Проверка локального аккаунта — защита ясности интерфейса; источником полномочий остаются signed decision и действующий Connect fence.

Signed operations через существующий `service.execute({op,actor,args})`:

```text
oauth.connections.approve {expectedAccountId,interactionId,contextDigest,browserNonce}
 -> {connectionId,approved:true,replayed:boolean}
oauth.connections.deny {expectedAccountId,interactionId,contextDigest,browserNonce}
 -> {denied:true,replayed:boolean}
oauth.connections.list {expectedAccountId,limit?:1..50,cursor?}
 -> {connections:[{id,clientProfile,resource,createdAt,expiresAt,revokedAt,
                  active:boolean,budget:{limit,reserved,spent,remaining}}],nextCursor}
oauth.connections.revoke {expectedAccountId,connectionId}
 -> {connectionId,revoked:true}
```

Owner list содержит только собственные projection/pins, не bearer/provider IDs, digest, payload, Notes contents. `active` — computed common current chain/expiry status, не optimistic copy column state. Pagination account-scoped keyset `(created_at,id)` с opaque bound cursor, ≤50 rows; входной cursor другого account не расширяет selection. Owner history остаётся прежним `access.invocations.list`, не дублируется.

Approve/deny/revoke исполняются внутри уже проверенного signed `Connect.handle` transaction: **не вызывают nested `withAuthorityFence`**. Domain проверяет owner/expectedAccountId, открывает одну Caps transaction, CAS pending→decision. Approval создаёт существующие client, service principal, root grant, invocations budget и connection одним COMMIT; private access helpers не вызывают вложенную public execute transaction. Поздний Connect COMMIT/ответ может потеряться после Caps COMMIT; exact retry сверяет durable interaction, не создаёт новую authority.

`beginGrantBinding` и любой grant/token upsert открывают свежий Connect→Caps fence. Branded context хранится только в private WeakMap/ограниченном active set; максимум16/store, абсолютный monotonic TTL30s и не дольше interaction/connection expiry. Он привязан к service instance, конкретной approved interaction/connection/browser nonce, не сериализуется. `endGrantBinding` идемпотентно инвалидирует в finally; поздний callback/Promise, чужой/скопированный token, AT/RT с этим context отказываются. На Grant.upsert снова проверяются creator/current chain/state, независимо от ранее выданного context.

Root ставит staging context в отдельный AsyncLocalStorage **только вокруг actual Grant.save**. Grant INSERT и write-once connection.providerGrantId CAS — один Caps COMMIT. Concurrent contender с иным ID получает `oauth_grant_conflict` без orphan artifact; host перечитывает durable binding. Exact повтор после COMMIT загружает существующий bound Grant. AS `interactionFinished` вызывается после этих await; следующая issuance имеет новые fresh guards. SQLite lock и Connect fence никогда не удерживаются через await.

Каждая synchronous port transaction проверяет service open и actual admitted registry/lineage. Thenable/rejected Promise от trusted callback поглощается и даёт controlled refusal, не unhandled rejection/late privilege. Auth work после any await root начинает с новой проверки, не продолжает сохранённый borrowed actor.

## 7. CAS outcomes, payload sealing и отказ

**AT upsert** одной Caps transaction: validate plain+full payload → live connection/root/creator → quota/expired-unreferenced cleanup → artifact INSERT → cap credential INSERT → immutable link INSERT → COMMIT. Token digest = SHA256(raw opaque id), raw id только внутри encrypted AS payload. Credential expiry точно `payload.exp*1000`, ≤connection expiry и≤now+300s. Repeated identical AT upsert сохраняет прежний credential ID/expiry; иной digest/pin/expiry по тому же artifact ID отказывает. Нет `soty_cap_` wrapper вокруг AT и нет ослабления прежнего service-token parser.

**Consume** в одном Connect→Caps fence после fresh client/resource/grant checks: valid unconsumed code/RT `UPDATE ... WHERE consumed_at IS NULL`, ровно один winner. Wrong resource/scope/type/client проверяются **до CAS**; wrong resource → `invalid_target` без revoke/consume. Missing source/expired/revoked → `invalid_grant`. Concurrent already-consumed → atomically revoke family/domain grant/credentials, COMMIT, затем status `invalid_grant`; root преобразует status в штатный `new errors.InvalidGrant()`. Нельзя бросить внутри transaction и откатить защитный revoke. Library consumed path `destroy/revokeByGrantId` повторяет тот же идемпотентный revoke.

**Revoke** любого AT/RT через штатный `/oauth/revoke` отключает family. Domain `Grant.destroy`, token `destroy` и `revokeByGrantId` используют тот же connection revoke: monotonic state, root grant revoked и OAuth credentials revoked, затем допустима bounded очистка raw artifacts. Session/Interaction destroy удаляет только их вспомогательный artifact, не чужие connections; raw session cookie не является authority. В частности borrowed `Interaction.payload.grantId` не переносится в plain `provider_grant_id/connection_id`, не участвует в `revokeByGrantId` token selection и не разрешает family revoke при закрытии interaction. Unknown destroy/revoke — no-op. Repeated/parallel calls не оживляют consumed source и не отменяют sibling connection. Прежний non-OAuth credential revoke остаётся точечным.

Consume→new RT save→AT save→HTTP response **не** общая transaction. Успешный consume не гарантирует доставку token; проигравший concurrent refresh может отозвать token победителя. Новый save после family revoke обязан отказать. Ошибка после AT COMMIT не удаляет ни credential, ни уже случившийся Note; неизвестный исход остаётся неизвестным до authorized read/owner history.

Crypto — маленький закрытый AES-256-GCM codec, `nonce12 || tag16 || ciphertext`, ≤16412 bytes. AAD — canonical bytes `['soty.oauth-artifact.v1',registryId,issuer,model,idHash,'oidc-provider-9.12.2-c1',keyId]`. Fresh random nonce для каждой encryption, plaintext canonical JSON≤16384. Digest = SHA256 этих canonical bytes. Find расшифровывает, сверяет digest и полный model shape/pins, выдаёт clone и column consumed; caller mutation не меняет store. Plain pins не берутся из plaintext после неудачного decrypt.

Key отсутствует/не совпадает или ciphertext повреждён — `oauth_storage_key_unavailable`/`capabilities_storage_corrupt`, без удаления/reset AS. Reader3 без key продолжает обычные owner/B2 операции; AS readiness не заявляет, что все ciphertext расшифрованы, пока bounded use не проверил их. Persistent cookie/signing/artifact keys различны по назначению. C1 не делает online re-encryption/key rotation; backup с keys только encrypted. Restore старой snapshot проводится offline с отзывом всех восстановленных OAuth families **до** network, иначе rollback вернёт старое право. Смена encryption key сама по себе не отменяет digest-backed credentials.

Ошибки port — существующий `AccessError` с safe code. `oauth_unavailable`, `oauth_storage_key_unavailable`, `oauth_storage_busy`, `oauth_context_invalid`, `oauth_invalid_artifact`, `oauth_quota_exceeded`, `oauth_interaction_not_found`, `oauth_interaction_conflict`, `oauth_grant_conflict`; row/format corruption — `capabilities_storage_corrupt`, account/owner errors — прежние. consume protocol outcomes перечислены выше. Root mapper возвращает sanitized стандартные invalid_request/client/grant/scope/target либо unavailable; не отдаёт payload/IDs/SQL/internal reason. Native crossconnection key code остаётся прежним `invocation_request_conflict`.

## 8. OAuth-only occupied-key namespace и original credentials

Новая область **только deny**, без crossconnection replay/read: `(accountId,issuer,staticClientId,resource,requestKey)` через durable connections. В прежнем `nativeNotes.admit` под тем же Connect→Caps fence:

1. Прежняя pure input/@1 Unicode validation, current branded actor authorization.
2. Прежний same-account/internal-client/requestKey lookup и full fingerprint/own read проверка; exact replay проходит до executionEnabled/Notes readiness/mutable admission quotas.
3. Для OAuth actor закрытый access helper возвращает checked connection scope. По существующим Invocation + OAuth connection pins ищется тот же key у **иной** connection данной области. Любое совпадение → одинаковый generic `invocation_request_conflict`, без запроса старого body/status/receipt. Проверка выполняется до нового INSERT и до создания budget reservation; для valid new request также до нового effect. Legacy actor получает null scope; пользователь не задаёт namespace.
4. Затем прежние readiness, live invoke authorization, native quotas и one admission COMMIT. Две connections сериализуются одной Caps write transaction: максимум одно новое admission для этого namespace/key.

Query shape:

```sql
SELECT 1
FROM cap_invocations i
JOIN cap_oauth_connections c ON c.client_id=i.client_id AND c.account_id=i.account_id
WHERE i.account_id=? AND i.request_key=?
  AND c.issuer=? AND c.static_client_id=? AND c.resource=? AND c.id!=?
LIMIT 1;
```

State/expiry старой connection и status/purged input Invocation **не фильтруются**. Они не дают полномочий, но сохраняют факт занятого ключа. Sameconnection refresh/replay, другой account/profile/resource и обычные service clients независимы. Sameprofile после re-consent может узнать один used-key bit; это осознанный privacy tradeoff, не доказательство официальности public client. Новые keys должны быть уникальными и непредсказуемыми; энтропию сервер не доказывает. Новый key не предотвращает семантический повтор похожего текста. Owner проверяет историю прежде явного повторения неизвестного действия; новому grant старый receipt не выдаётся.

OAuth actor производится только общим access module через checked token digest/link; closed factory не принимает arbitrary `{accountId}`. WeakMap reference дополнительно помнит OAuth connection identity и требует существующий link на каждом recheck. Persisted original authorization B2 **не меняет форму**: credentialId сохраняется, resolver по immutable link вновь находит connection, проверяет creator/grant/expiry/state. Новая AT credential не подставляется в старый Invocation. Current read после refresh того же grant допустим, original expired execution — нет. AS off, missing encryption key или sibling revoke не отменяют уже случившееся proof-positive reconciliation; external response всё равно fresh authorized read.

**Legacy access не обходит эту связь.** Common `currentCredential` сначала определяет, связан ли её client/principal/root с OAuth connection: если да, отсутствие или несовпадение exact OAuth link — deny, даже при OAuth composition/AS disabled. `access.credentials.issue` и `access.grants.issue/derive` для linked authority отказывают `oauth_managed_authority`, не создают новый `soty_cap_` credential и не расширяют scope/срок. Новый service principal в прежнем API всегда получает отдельный новый client, а не выбранный OAuth client; это свойство сохраняется. Не разрешается прикрепить OAuth connection к ранее используемому service client. Generic owner revoke OAuth-linked credential использует тот же family-revoke transaction; ordinary non-OAuth credential revoke остаётся точечным. Revocation client/principal/grant закрывает текущую общую chain и не восстанавливает family через очередной refresh. Reader-only3 содержит эти проверки независимо от AS keys/operational flag.

## 9. Retention, quotas и bounded работа

Quota checks выполняются под тем же write lock **до нового** approval/token INSERT. Exact retries, revoke, cleanup и authorized history не расходуют новый slot. Quotas не блокируют safety revoke. Значения ниже — C1 limits, не физическая capacity гарантия.

| Область | Bound / cleanup |
|---|---|
| Signed approvals | ≤16 active unexpired connections/account; прежние lifetime principals<1000/account и grants<10000/account; ≤10000 durable OAuth connections/global. Один approval создаёт ровно один client/principal/root/budget. Pins не чистятся вместе с browser artifacts. |
| Interaction proposals | ≤256 live pending/global; ≤1024 total unexpired domain interaction rows/global. AS Session/Interaction unbound artifacts вместе≤1024/global, срок≤600s от первоначального создания; не unlimited orphan Session/Grant после отказа. Grant создаётся только атомарно связанным, orphan Grant не принимается. |
| AS artifacts | ≤1024/connection и≤65536/global, plaintext≤16KiB each. Индексированный cleanup≤64 rows/batch; при заполнении отказ в новой выдаче, без eviction живых proof/consumed rows. |
| OAuth credentials | ≤64 live AT/account, каждый≤300s. Expired unreferenced link+credential удаляются одной Caps transaction, batch≤64. Остальные service credentials не удаляются и имеют прежнюю отдельную quota10000/account. |
| Original references | Любой Invocation reference, в том числе terminal/purged, сохраняет original OAuth credential/link навсегда в пределах прежнего native ledger bound10000/account. Исторические references не входят в transient64; свежий AT для read не блокируется лишь из-за полного effect ledger. |
| Consumed code/RT | retainUntil = connection.expiresAt, даже если code.exp уже прошёл. Connection revoked может быть очищена раньше, поскольку late upsert закрыт durable state. У AT retainUntil=expiresAt; link может пережить artifact. Grant хранится до bound expiry/revoke; Session/Interaction до собственного expiry. |
| Host/in-memory | ≤16 concurrent AS requests, body≤16KiB, URL≤8KiB, body deadline10s; domain staged contexts≤16/store и30s. Один bounded payload snapshot, no tiny-chunk array, no async under SQLite lock. |

`cleanup({limit})` имеет один общий work budget≤64 выбранных identities за вызов, а не64 на каждый необъявленный подцикл; атомарная пара link+credential считается одной identity и двумя физическими deletes. Return counts — фактически удалённые identities по категориям. Expired interaction proposal/artifact могут удаляться согласованной парой; bound сохраняется. Временный cap достигается проверяемыми indexed SQL predicates, не загрузкой всех payload в память.

Expiry cleanup не выполняет unbounded scan Invocation JSON. Reference predicate **точно** совпадает с expression index: `WHERE json_extract(authorization_json,'$.credentialId')=? LIMIT 1`. Проверка reference и DELETE не разделяются await/transaction. Account live-count использует existing connection/account indexes + link/credential expiry; retained rows не превращаются в unbounded `.all()`/JS sort. Долгоживущие consumed RT при нормальном refresh раз в5min: ≤288 RT +≈288 AT за24h плюс code/Grant, то есть1024 допускает обычный профиль; это расчёт, не нагрузочная приёмка. Одна page/batch вызывает ограниченное число SQL, ошибок writer/busy не маскирует очисткой неизвестного store.

Отзыв может использовать update связанных OAuth credentials, однако немедленная authority граница — immutable connection/root state в common resolver. Cleanup raw data после него не является условием вступления отзыва в силу. Service count исключает только **валидно linked** OAuth rows; повреждённый orphan не уменьшает quota и не становится допустимым credential.

## 10. Historical2 provenance и границы владения

Заморожена import closure `modules/capabilities/test/fixtures/capabilities-v2/` из exact commit **`cc7f65b84e0a8f8b4fd41cf44e498ee3f21015e7`**. `provenance.json` содержит source paths, Git blob IDs, bytes и hashes. Entry `schema.mjs` экспортирует `initializeCapabilitiesSchema`; относительные imports только в этой папке и Node built-ins, не текущие v3 files.

| File | Bytes | SHA-256 exact Git blob |
|---|---:|---|
| schema.mjs |184|`9853c33995755cd025ab8e6e02c0ea6e72b77d62b70d775d9c7db2303ff696b6`|
| schema-v2.mjs |23705|`5ca8b4025535566c90a1b1d610e37837b4b41e6074117a96fe720504050af25f`|
| validation.mjs |3306|`b5cd743caa6183af7b82021eeab0381d6bea9dace806bae12b194df18c15fa9d`|
| native-note-contract.mjs |1098|`1f3ab19fa9e6641e9928ce363ab6fdcbcf1421d4785256466a430ce0bcfa9c83`|

Это source provenance, **не executed old-reader refusal/migration proof**. Историческая БД создаётся настоящим copiedv2 migrator или независимым literalv2 fixture; нельзя генерировать current3 и присвоить user_version2. Old2 on main2+WAL3 и future4 refusal с byte/side-effect evidence — отдельная приёмка. Guard envelope3 сохраняется, только capabilities readers расширяется до[1,2,3] после source/DDL freeze. Notes1/2, Apps1..6, Rooms1/2 без изменений. SAME old serving container должен читать3 до first3write; feature-off у старого binary это не заменяет.

| Владелец после отдельного implementation go | Область |
|---|---|
| Domain author | `modules/capabilities/server/oauth-*.mjs`, `schema-v3.mjs`, узкие `schema.mjs/index.mjs/access.mjs/native-notes.mjs` integration; новые module tests/fixtures; эти два OAuth docs/README. Notes, pinned catalog/validator, semantic digest и B2 receipt формы не меняются. |
| Root | Exact package/lock, actual Provider/adapter wrapper/ALS/keys config, signed Connect extension hookup, HTTP/PRM/consent UI/SW, existing HTTP native recovery known2/3 selection. Domain storage не дублируется root adapter. |
| Reader author | После literal source freeze — deploy probe/guard/current manifest/Docker labels/old-reader/Linux gate. Этот документ не разрешает преждевременную advertise3. |
| Critic | Независимые acceptance files/receipts, source review. Его tests не редактируются автором ради PASS. |

## 11. Следующий законченный срез и приёмка

1. **Domain3 baseline:** isolated schema3/default-off migration + validators/guards + historical2 tests; current resolver/native exact2/3/read-only recovery. Никакой OAuth issuance до принятой схемы. EXPLAIN на large realistic rows для двух новых Invocation indexes; неизвестные tables/DDL/rows не «лечить».
2. **Domain OAuth port:** fixed consent/stagedGrant/atomic AT-link/consume/revoke/retention + root adapter composition. Unit tests полезны для unsafe inputs, но не заменяют real Provider/Connect/Caps/Notes effects.
3. **Независимая общая приёмка:** два настоящих AS OS processes, actual signed Connect consent, HTTP resources, CLI/PWA и deployment gate — собственные последующие receipts.

Обязательные реальные cases:

- v1/v2 default-off и mixed Notes2/Caps3; real2→3 сохраняет registry/cap pins/native historical proof. На3 с OAuth disabled/без keys owner history + internal proof reconciliation работают; unknown4 не проходит `>=` обход. Corrupt link/account/parent/replaced guard/cipher отказывает на своей границе без repair.
- Exact signed approval retry после CapsCOMMIT/Connect failure создаёт один client/root/budget. Concurrent grant saves связывают один providerGrant; kill после Grant artifact/link COMMIT до ответа восстанавливает тот же ID. Forged/expired/ended/foreign staged context и поздний Promise получают 0token writes; live creator revoke между stage и save закрывает выдачу.
- Actual Provider wrong PKCE/resource/client до consume: источник после допустимой ошибки пригоден для правильного запроса. Two-process refresh barrier допускает один consume; reuse revoke durable даже при error response, late upsert не оживляет семью; sibling не затронут.
- Lost token/Note response → family revoke → new consent → тот же key: no new Note/receipt disclosure. Changed input и terminal/purged прежний intent дают тот же conflict. Две connections с общим namespace/key — один admission. Sameconnection refresh exact replay; разные accounts/static profiles/resources/legacy service clients независимы.
- AT1 admit → expire → AT2 refresh: fresh own read работает, original effect не получает новую expiry. Family revoke закрывает refresh, новый grant не читает старый receipt. Proof-positive после revoke settles internally; existing Notes >32 edits/purge не раскрываются старому внешнему receipt.
- Legacy owner `credentials.issue`, `grants.issue/derive` на OAuth authority дают0новых прав; no-link credential linked client не authenticates и не dispatches при feature-off. Другой обычный service principal/client по-прежнему работает. Generic credential revoke linked family не затрагивает sibling.
- Reference retention на реальных native rows: expired unreferenced очищается, terminal/unknown original никогда не удаляется; повторный open3 после cleanup валиден. Число credentials не растёт бесконечно при обычной ротации без эффектов. Fullquota не блокирует revoke/owner history; reused key не расходует budget.
- `Provider.ctx` isolation между concurrent requests, authenticated client/resource snapshot не берётся из соседнего запроса. Capture/late callback не переносит creator consent между accounts. Library payload Session/Interaction повторного consent/expiry соответствует600s policy, чужой account в remembered session не autoapprove.
- `expiresWithSession:false` на actual authorize: удаление/expiry Session не обрывает ещё действующий RT. Два sameprofile consent/разные connections остаются независимыми до family revoke каждой. Session.authorizations.persistsLogout boolean принимается, но не служит Connect proof; resetIdentifier/save не продлевает Session окно.
- Реальный encrypted persistence reopen с тем же key, wrong/missing key и altered ciphertext; rawtoken не находится в DB plaintext/logs, используемый old key не генерируется автоматически при restart. Restore-to-older-state проверяется offline с family revoke до AS/RS start.

**Что ещё не доказано:** production domain3 SQL/guards, encrypted adapter, real signed consent, cleanup query plans/limits, два OS processes, OAuth→Notes/PWA/CLI и Linux reader3. Принятый C1a spike подтверждает узкий библиотечный порядок и формы полей; не подменяет перечисленные gates.

Первичные источники: [adapter](https://raw.githubusercontent.com/panva/node-oidc-provider/v9.12.2/example/my_adapter.js), [public Provider.ctx/configuration](https://raw.githubusercontent.com/panva/node-oidc-provider/v9.12.2/docs/README.md), [opaque format](https://raw.githubusercontent.com/panva/node-oidc-provider/v9.12.2/lib/models/formats/opaque.js), [BaseModel](https://raw.githubusercontent.com/panva/node-oidc-provider/v9.12.2/lib/models/base_model.js), [Session](https://raw.githubusercontent.com/panva/node-oidc-provider/v9.12.2/lib/models/session.js), [refresh rotation](https://raw.githubusercontent.com/panva/node-oidc-provider/v9.12.2/lib/actions/grants/refresh_token.js), [revoke](https://raw.githubusercontent.com/panva/node-oidc-provider/v9.12.2/lib/actions/revocation.js). Прочитаны exact installed9.12.2 source, безопасный root names-only spike log и действующие domain files; ссылки фиксируют tag, не плавающий latest.
