# World: people, communities and discovery

World adds account-scoped communities to Soty. It does not migrate, reinterpret or weaken the existing secret-based rooms. There are no seed people, fabricated presence counters or production demo communities.

## Integration boundary

```js
let connect;
const world = createWorldService({ databasePath: '/persistent/world.sqlite', projectId: 'soty',
  withCurrentContacts: (actor, callback) => connect.withCurrentContacts(actor, callback) });
connect = createConnectService({
  databasePath: '/persistent/connect.sqlite', projectId: 'soty', allowedOrigins,
  extensions: [world],
  canRequestContact: (actorId, targetId) => world.canRequestContact(actorId, targetId),
});
```

Only Connect may invoke `world.execute({op,args,actor})`. Connect derives a frozen `{accountId,deviceId,label}` from a verified P-256 proof and an active installation. World is not an HTTP authentication endpoint. An account ID submitted in profile mutation arguments is rejected; `profileId` in a target operation means the target, never the caller. World `profileId` equals Connect `accountId`.

`world.operations` is the exact challenge allowlist. `WorldError.code` is safe for the host to map to a fixed public error. Raw storage exceptions, query text, proofs or key material must not be returned.

New community messages are access-controlled server records in this SQLite database. They are **not end-to-end encrypted** and must not be labelled as such. Existing encrypted rooms retain their existing semantics. Community chat is fetched via bounded signed RPC while the conversation is active; this module does not claim a realtime streaming transport.

## RPC contract

Every method below has the `world.` prefix. All writes reject unknown properties. `expectedRevision` is mandatory for profile/community edits; a conflict requires refreshing before editing again. `requestId`/`clientId` must be 3–160 ASCII letters, digits, `_` or `-` (UUIDs are suitable). Client retries preserve them.

| Method | Arguments | Result |
| --- | --- | --- |
| `profile.get` | `{}` | `{profile}` including own visibility/preferences |
| `profile.update` | `{expectedRevision, displayName?, bio?, interests?, audience?:'private'|'friends'|'link'|'public', discoverable?, showPresence?, showMemberships?, contactPolicy?, avatarColor?}` | `{profile}` |
| `profile.view` | `{profileId}` | `{profile,communities,canRequestContact}`; current full-profile audience, no anonymous API |
| `profile.avatar.set` | `{expectedRevision,avatarUrl,thumbnailUrl}` | `{profile}`; own raster upload, both null to remove |
| `profile.avatar.read` | `{profileId,communityId?}` | `{profileId,avatarUrl,avatarRevision}`; one authorized full image or null |
| `profile.avatars` | `{profileIds: string[1..24],communityId?}` | `{avatars:[{profileId,avatarUrl,avatarRevision}]}`; thumbnails only, unavailable items omitted |
| `discovery.search` | `{query?,kind?:'all'|'people'|'communities',limit?,cursor?}` | `{people,communities,nextCursor,totals:{people,communities,peopleExact?:false,communitiesExact?:false}}` |
| `directory.search` | `{expectedAccountId,query?,kind?:'all'|'people'|'communities',scope?:'all'|'mine'|'public',limit?,cursor?}` | `{schema:'soty.world-directory.v1',people,communities,nextCursor}`; current authorized directory |
| `directory.resolve` | `{expectedAccountId,entities:[{kind:'person'|'community',id}]}` | `{schema,items:[{ref,available,person?/community?,access?}]}`; at most60 identities |
| `field.get` | `{expectedAccountId}` | `{revision,document,contentHash,updatedAt}`; empty field starts at revision0 |
| `field.put` | `{expectedAccountId,expectedRevision,requestId,document,contentHash?}` | `{replayed,receipt:{revision,contentHash,committedAt},current}`; account-local CAS |
| `community.create` | `{requestId,name,description?,topics?,joinPolicy?,showMembers?,showcase?,symbol?,color?}` | `{communityId,community}` |
| `community.get` | `{communityId}` | `{community}` |
| `community.list` | `{}` | `{communities}`; active/requested/invited participation |
| `community.update` | `{communityId,expectedRevision,...editableFields}` | `{community}` |
| `community.archive` | `{communityId,expectedRevision}` | `{archived:true}`; owner only |
| `membership.join` | `{communityId}` | `{community}`; open joins, request creates request, invited accepts invitation |
| `membership.leave` | `{communityId}` | `{community}` or `{left:true,community:null}` for invite-only groups |
| `membership.invite` | `{communityId,profileId}` | `{community}`; manager only, target accepts |
| `membership.decide` | `{communityId,profileId,accept}` | `{community}`; manager handles request |
| `membership.remove/ban/unban` | `{communityId,profileId}` | `{community}`; enforced role boundary |
| `membership.role` | `{communityId,profileId,role:'moderator'|'member'}` | `{community}`; owner only |
| `membership.transfer` | `{communityId,profileId,requestId}` | `{communityId,community}`; owner only, atomic |
| `membership.list` | `{communityId,state?:'active'|'requested'|'invited'|'banned',limit?,cursor?}` | `{members,nextCursor}`; member, manager for non-active states |
| `membership.preferences` | `{communityId,pinned?,muted?,showInProfile?}` | `{community}`; own participation only |
| `chat.list` | `{communityId,after?:seq,before?:seq,limit?}` | `{messages,hasMore}`; one of before/after |
| `chat.send` | `{communityId,clientId,text,replyTo?}` | `{message}`; replay-safe |
| `chat.remove` | `{communityId,messageId}` | `{removed:true}`; own message or manager |
| `chat.read` | `{communityId,throughSeq}` | `{unreadCount}` |

DTO declarations are in `index.d.ts`. Paged lists are limited to 60 items per page; `community.list` returns at most 100 own participations. Discovery returns a single bounded mixed page split into two typed arrays. The field `memberCount` is an exact permitted count, not a presence signal. Empty-query directory totals are exact; text-search totals above 1000 are explicit lower bounds with `peopleExact:false` / `communitiesExact:false` and must be displayed as `1000+`. An absent exactness flag means the corresponding count is exact. Only managers receive `pendingCount` for the request queue. The map and list should use the same search results. Initial chat fetch returns the newest page in ascending order; `before` loads history and `after` loads newly committed messages.

Contact discovery uses the signed core Connect method `contacts.requestAccount({accountId:profile.profileId})`; it never exports a card ID. Its host policy callback applies the World visibility/contact policy, after which normal Connect blocking, contact requests and acceptance remain authoritative.

## Durable audience changes

An own profile advertises `audienceWriteVersion:1` only on the receipt-capable reader. Its closed durable update is `profile.update({expectedAccountId,requestId,expectedRevision,audience})`; all four fields are required together and extra metadata fields are rejected on this branch. Existing updates without either ID retain the legacy argument/reply contract. A current account mismatch gives `profile_account_changed`; a reused ID with different args gives `profile_request_conflict`; a new stale request retains `revision_conflict`.

The durable reply is `{profile,replayed,receipt}`. The immutable receipt contains `{schema:'soty.world-profile-audience.receipt.v1',requestId,accountId,audience,expectedRevision,acceptedRevision}`, with accepted revision exactly base+1. Its intent digest, profile mutation, audit and receipt row commit in the same existing World transaction. Same-intent replay is checked before CAS and returns that historical receipt together with the current own profile, never an old full-profile snapshot. Current Connect device/account authentication still precedes replay.

Clients must durably retain the exact intent before dispatch and retry that same ID/body after an unknown result. An unchanged GET is not proof that a delayed request cannot still commit. A newer deliberate audience change uses a fresh intent after resolving the earlier one; a late original delivery then replays its receipt and cannot overwrite the newer profile. A receipt reports accepted history, while the returned/read-back own profile reports current state. Neither is an application or agent capability grant.

## Privacy and role rules

- Profiles start `private`. Owner always reads the full profile. `friends` permits only current accepted reciprocal, unblocked Connect contacts; `link` permits exact signed profile/resolve/avatar reads but never global name/ID search; `public` enters global discovery. A profile locator is an account ID, not a password or grant. Legacy `discoverable:false/true` means `private/public`; conflicting simultaneous audience/boolean fields reject without writes.
- Public group previews require profile discovery + profile membership publication + this membership publication + group member-preview setting. Exact group size may include hidden members; names do not.
- Active group members still see fellow members and message authors, including hidden profiles, through explicit `WorldIdentity` only: profile ID, display name, avatar color/revision and profile revision. Chat send/list/replay/tombstones, every roster state and public member previews omit bio/interests. Shared membership grants no full profile. Private mute/pin preferences are never returned in another member's listing.
- Invite-only groups cannot be discovered or retrieved by guessed ID. Only active members and explicitly invited recipients see their preview. Recipients cannot read chat until accepting.
- Open groups require an explicit join. Request groups require approval. A ban prevents join/invite until explicitly lifted. Removing a member kicks them; in an open group they can join again unless banned.
- A moderator can moderate regular members. Only the owner changes roles or join policy, transfers ownership or archives. An owner cannot leave an active group without transferring or archiving it.
- Group membership does not create a personal contact or grant device control. Every chat read/write rechecks active membership, and the Connect layer rechecks active device identity.

## Avatars

Schema 2 adds private BLOB storage for raster avatars. Existing profiles migrate with `avatarRevision:null`. Normal profile, discovery, member and chat projections contain only that revision, never a data URL. A page of 60 community previews therefore does not duplicate hundreds of image payloads.

The frontend decodes and crops a selected photo, then exports a full image (at most 512×512 pixels and 96 KiB) and a thumbnail (192×192 and 8 KiB). PNG, JPEG and WebP data URLs must use canonical base64. The server checks MIME against the binary format, container structure, bounds, PNG CRCs and dimensions. Animated content, SVG, arbitrary external URLs and opaque/EXIF metadata are rejected. This is a bounded raster/container validator, not a complete codec decoder or a malware scanner; data is displayed only through `img`, never injected as markup.

Signed thumbnail batches contain at most 24 images, roughly 260 KiB including base64. They read only thumbnail columns from storage. Full-image reads use the current full-profile audience. Only the thumbnail batch has a separate community exception: an active viewer may retrieve a hidden current member or historical message author's thumbnail in an explicit community context. That exception never returns a full image. Leaving or removal ends the viewer's context access. A batch omits unavailable profiles without distinguishing hidden from absent. No unauthenticated asset path or permanent capability URL bypasses these checks.

Client avatar caches belong only to memory and the active account/community context. Clear them on navigation, profile/visibility refresh or membership loss; discard pending requests from an older context. Responses omitted from a fresh batch must not reuse an old image.

Upload error codes: `invalid_avatar_mime`, `invalid_avatar_data`, `avatar_too_large`, `avatar_dimensions`, `revision_conflict`. The two URL fields must be either valid images or both null for deletion.

## Application permissions

`canAccessCommunity(accountId,id)` and `isGroupAdmin(accountId,id)` are live checks. Applications should gate every request with these checks, alongside their own grant and Connect installation checks.

`activeCommunityIds(accountId)` is a fresh indexed host-side lookup for querying an application's grant index. It is not a browser RPC and must not be cached across permission changes.

`subscribeMembership(listener)` emits committed `{communityId,profileId,state,revision}` events. On archive `profileId` is null and state is `archived`. The callback is a local-process invalidation hint to close app sessions immediately; it is not a distributed event bus or substitute for live authorization. Consumer exceptions cannot roll back a committed revocation. Device revocations come from Connect separately.

## Storage and reliability

The database is project-bound and kept outside the replaceable module. Migrations fail closed on metadata/schema disagreement. WAL + FULL synchronization protect committed writes. World read-only requests use a World snapshot; writes use `BEGIN IMMEDIATE`, prepared statements, foreign keys and uniqueness constraints. When configured, the synchronous Connect current-contact writer fence is acquired first, before the World transaction, and expires when the call returns. Missing contact authority denies friends full reads. Owner transfer, membership changes and associated audit record are one World transaction.

Root audience storage uses format5. Version4 is reserved for the separate M02 attachment format and is explicitly rejected by this reader. Fresh format0 starts5. Existing1/2/3 refuse startup with `world_audience_migration_required` before journal/transaction/schema writes unless the trusted host explicitly supplies `allowAudienceMigration:true`. That option is not set by the ordinary HTTP factory and is not a replacement for release admission or a backup. The explicit migration adds the closed audience column, seeds private/public from legacy discovery, and replaces only profile indexing triggers with audience-based public indexing. IDs, content, revisions and existing public projection sequence numbers stay intact. Format5 admission checks the exact audience column, profile trigger inventory/definitions and legacy projection consistency; attachment fragments and old readers are rejected. The current independent deploy/storage probe covers Apps, not World5: these module reader tests do not qualify a production pre-START deployment. M02 attachment+audience migration is separate and remains pending; do not run this Root reader on its store or downgrade a migrated store.

Connect and World are separate stores. Creation and ownership transfer persist request receipts; message IDs are unique per author/client request. A proof retry after an ambiguous outer Connect commit cannot duplicate those effects. Receipts retain identities and re-project current permissions/visibility rather than returning stale full objects. Audit rows store identifiers and action names, not message bodies or credentials.

Schema 3 adds a transactional public directory, exact per-kind counters and SQLite FTS5 (`unicode61`, prefix lengths 2/3/4). Search is deterministic and Unicode case-aware: every word is a literal token prefix, combined with AND; it is not arbitrary substring matching, a fuzzy ranking API or user-supplied FTS syntax. Direct entity-ID lookup still applies public visibility. Hidden people, archived communities and invitation-only communities never enter the public directory. Each public mutation updates the projection and FTS in its own transaction. Pagination follows immutable sequence numbers rather than offsets, so removal from a previous page does not skip the next page. Cursors are scoped to the exact normalized query text and kind; changing either requires a fresh search. Re-showing a hidden entity appends it at a new sequence position. Search is a current-view pagination contract, not a frozen multi-request snapshot. Text-query counts inspect at most 1001 matches per entity kind and mark larger totals as lower bounds. No online status is inferred from discoverability or group size. Presence preferences are stored, but no individual presence signal is published by this module.

## Verification

```powershell
node --test modules/world/test/*.test.mjs
```

The suite includes file-backed restart tests, privacy projections, role boundaries, idempotency, revocation, message pagination, and two independently generated P-256 accounts through Connect and its actual HTTP adapter. Browser workflow verification belongs to the product integration; an API test does not certify its UI or deployment.

## Personal field and authorized directory

The new directory and field operations require `expectedAccountId` to match the verified Connect actor. Browser controllers also pass the captured account to `client.extension(op,args,{expectedAccountId})`; the RPC argument and the browser admission context have different responsibilities. No new operation accepts a request-supplied actor or bypasses Connect. The operation map and result types are exported in `index.d.ts`.

`directory.search` retains public person privacy and adds the caller's own profile, current friends who chose `friends`, and currently active/invited private groups for their participant. `scope:'mine'` includes that own/friends set and active/requested/invited own communities; `public` excludes private profiles and invitation-only groups even for a member. Link profiles are not name-search candidates. There are no global private counts. The seek cursor binds account, installation, normalized query, kind and scope; every continuation rechecks live contacts. Page limits are1–60; always follow `nextCursor`, not a guessed total. Exact `directory.resolve` uses the same full-profile predicate as `profile.view` and full avatars. A known hidden person may also resolve to minimal identity while the actor has active roster access through a shared active community, without bio, interests or shared-group identities. Invitation does not permit chat until membership is active. Unknown and unauthorized identities have the same unavailable result.

The field document is `{schema:'soty.field.v1',contexts:[{contextId,title,x,y}],shortcuts:[{shortcutId,entity:{kind,id},contextId,slot:[q,r]}]}`. The shared strict validator is [Field contract](../field/contract.mjs):24 contexts,256 shortcuts,128KiB UTF-8. `kind` is app/person/community/device/builtin. Two shortcuts may reference the same entity, but a context slot has only one occupant. A shortcut stores an opaque reference, not a cached profile, device credential, membership or grant. Adding, moving, removing, copying and undoing shortcuts have **no admission or publication effect**.

`field.put` uses expected revision0 for the first document; each new accepted request increments it. The server computes a canonical document hash and checks an optional caller hash. Retry the same requestId, expectedRevision and document after an uncertain response. The immutable receipt is returned before a stale-version check for an identical retry. Changing the intent under that ID is rejected. `receipt.revision` describes that accepted intent; `current` may describe a newer arrangement from another device and must not be replaced with the earlier document. Field state is private account data stored under normal server access controls; it is not end-to-end encrypted.

Field storage is an additive `field_schema_version=1` extension with `world_field_documents` and immutable `world_field_receipts` in the same SQLite file. Root World5 preserves these documents/receipts. The frozen pre-audience schema3 reader refuses format5 rather than opening a new privacy format. New readers refuse unknown field epochs, missing guards or invalid document hashes rather than resetting the layout. Existing file snapshots include these tables automatically.

The product adapter uses account-scoped IndexedDB transactions for its bounded durable outbox before dispatch. Lost ACKs replay the original intent; quota errors dispatch nothing. Other-window conflicts preserve pending changes and require an explicit version choice. Only an undurable local buffer triggers the navigation guard. `discardVolatile` restores the latest durable baseline and does not delete any window's outbox. These client guarantees require real-browser verification in addition to API tests.

`createFieldPersistence({localFirst:true})` acknowledges an intent after its IndexedDB transaction, before waiting for the network. It reports `volatile` with `localDurable:true`; this is a device save, not a server confirmation. Its logical `projectedRevision = revision + pendingCount` is the next UI command fence: ordered requests still use server revisions individually, and their ACKs do not change that logical fence. GET/PUT requests run outside the local transaction queue, so a slow server cannot postpone subsequent local saves. Capturing intents and adopting validated ACKs remain serialized. The default adapter mode still awaits the server for callers that require that behavior.

A read-only second window may follow the shared durable outbox and its validated latest head without claiming authorship or issuing a duplicate mutation. Changes from another window conflict with this controller's own pending intent or undurable buffer; merely observing the first window's pending queue does not create a false conflict when its ACK arrives.

An accepted receipt followed by a different newer current document is a visible conflict. The adapter durably retains the desired arrangement separately from that verified current state. Choosing the local version creates a fresh request at the latest revision; choosing the other version makes no duplicate mutation. A same-revision response with a different valid content hash is rejected rather than silently rewriting known state.

The product's `loadMine` also reads authenticated Connect `contacts.list`. Only active accepted relationships appear, with their known account label and no invented avatar, bio or presence. This own-only relationship data never enters World public search. A saved hidden person can fall back to that current private contact label when World profile/roster access is unavailable. Removing or blocking the contact removes this fallback on the next read; it does not grant or revoke unrelated World roster permissions. Raw contact records route to the existing account people panel, not an expanded `profile.view` privilege.

```ts
import type { WorldFieldEnvelope, WorldFieldPutResult } from './modules/world/index';
const accountId = displayedAccountId;
const before = await client.extension<WorldFieldEnvelope>('world.field.get',
  { expectedAccountId: accountId }, { expectedAccountId: accountId });
const result = await client.extension<WorldFieldPutResult>('world.field.put',
  { expectedAccountId: accountId, expectedRevision: before.revision,
    requestId: crypto.randomUUID(), document: validatedDocument }, { expectedAccountId: accountId });
```

Directory/field operations do not execute tools or models, extend capability grants, expose human presence, or publish private chat history. `community.showcase` is explicit owner-authored text; a public discovery card is not permission to read a conversation.

### Field verification

`node --test modules/world/test/field-directory.test.mjs modules/world/test/field-migration.acceptance.test.mjs server/test/field-directory-http.test.mjs src/world/field-directory.test.mjs src/world/field-persistence.test.mjs` checks domain CAS/current field preservation and old-schema refusal, signed HTTP/connector enrollment, and independently controlled client failure ordering. The audience suites additionally cover actual signed Connect contacts, full-profile read paths, minimal identities and literal reader admission. Client outbox unit tests replace IDB with an atomic memory port; they do not claim browser IDB coverage. No production data or model calls are used.

## Scale observations

`node scripts/world-discovery-benchmark.mjs` builds isolated temporary fixtures of 10,000 and 50,000 public profiles with mixed communities. It records 50 warm samples per case, p50/p95 and raw durations in `output/discovery-scale-20260928/benchmark.json`; fixture databases are removed after the run. It does not contact a running server or use production data. Timings are observations, never machine-dependent test thresholds. See [the measured scale report](../../docs/research/discovery-scale-20260928.md) for the hardware/runtime, exclusions and remaining large-community projection cost.
