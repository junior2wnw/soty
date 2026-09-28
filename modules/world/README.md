# World: people, communities and discovery

World adds account-scoped communities to Soty. It does not migrate, reinterpret or weaken the existing secret-based rooms. There are no seed people, fabricated presence counters or production demo communities.

## Integration boundary

```js
const world = createWorldService({ databasePath: '/persistent/world.sqlite', projectId: 'soty' });
const connect = createConnectService({
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
| `profile.update` | `{expectedRevision, displayName?, bio?, interests?, discoverable?, showPresence?, showMemberships?, contactPolicy?, avatarColor?}` | `{profile}` |
| `profile.view` | `{profileId}` | `{profile,communities,canRequestContact}`; unavailable when hidden outside own profile |
| `profile.avatar.set` | `{expectedRevision,avatarUrl,thumbnailUrl}` | `{profile}`; own raster upload, both null to remove |
| `profile.avatar.read` | `{profileId,communityId?}` | `{profileId,avatarUrl,avatarRevision}`; one authorized full image or null |
| `profile.avatars` | `{profileIds: string[1..24],communityId?}` | `{avatars:[{profileId,avatarUrl,avatarRevision}]}`; thumbnails only, unavailable items omitted |
| `discovery.search` | `{query?,kind?:'all'|'people'|'communities',limit?,cursor?}` | `{people,communities,nextCursor,totals:{people,communities,peopleExact?:false,communitiesExact?:false}}` |
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

## Privacy and role rules

- Initial profiles are hidden until the person enables discovery. SQL filters visibility before pagination, including account-ID search. Hiding removes the directory row and its FTS entry within the same SQLite transaction, so it takes effect on the next request without an asynchronous indexing delay.
- Public group previews require profile discovery + profile membership publication + this membership publication + group member-preview setting. Exact group size may include hidden members; names do not.
- Active group members still see fellow members and message authors, including hidden profiles. Private mute/pin preferences are never returned in another member's listing.
- Invite-only groups cannot be discovered or retrieved by guessed ID. Only active members and explicitly invited recipients see their preview. Recipients cannot read chat until accepting.
- Open groups require an explicit join. Request groups require approval. A ban prevents join/invite until explicitly lifted. Removing a member kicks them; in an open group they can join again unless banned.
- A moderator can moderate regular members. Only the owner changes roles or join policy, transfers ownership or archives. An owner cannot leave an active group without transferring or archiving it.
- Group membership does not create a personal contact or grant device control. Every chat read/write rechecks active membership, and the Connect layer rechecks active device identity.

## Avatars

Schema 2 adds private BLOB storage for raster avatars. Existing profiles migrate with `avatarRevision:null`. Normal profile, discovery, member and chat projections contain only that revision, never a data URL. A page of 60 community previews therefore does not duplicate hundreds of image payloads.

The frontend decodes and crops a selected photo, then exports a full image (at most 512×512 pixels and 96 KiB) and a thumbnail (192×192 and 8 KiB). PNG, JPEG and WebP data URLs must use canonical base64. The server checks MIME against the binary format, container structure, bounds, PNG CRCs and dimensions. Animated content, SVG, arbitrary external URLs and opaque/EXIF metadata are rejected. This is a bounded raster/container validator, not a complete codec decoder or a malware scanner; data is displayed only through `img`, never injected as markup.

Signed thumbnail batches contain at most 24 images, roughly 260 KiB including base64. They read only thumbnail columns from storage. A full-image read is separate. Each request rechecks visibility: own profile or currently discoverable; with an explicit community context, an active viewer may also retrieve a hidden current member or historical message author's avatar. Leaving or removal ends that context access. A batch omits unavailable profiles without distinguishing hidden from absent. No unauthenticated asset path or permanent capability URL bypasses these checks.

Client avatar caches belong only to memory and the active account/community context. Clear them on navigation, profile/visibility refresh or membership loss; discard pending requests from an older context. Responses omitted from a fresh batch must not reuse an old image.

Upload error codes: `invalid_avatar_mime`, `invalid_avatar_data`, `avatar_too_large`, `avatar_dimensions`, `revision_conflict`. The two URL fields must be either valid images or both null for deletion.

## Application permissions

`canAccessCommunity(accountId,id)` and `isGroupAdmin(accountId,id)` are live checks. Applications should gate every request with these checks, alongside their own grant and Connect installation checks.

`activeCommunityIds(accountId)` is a fresh indexed host-side lookup for querying an application's grant index. It is not a browser RPC and must not be cached across permission changes.

`subscribeMembership(listener)` emits committed `{communityId,profileId,state,revision}` events. On archive `profileId` is null and state is `archived`. The callback is a local-process invalidation hint to close app sessions immediately; it is not a distributed event bus or substitute for live authorization. Consumer exceptions cannot roll back a committed revocation. Device revocations come from Connect separately.

## Storage and reliability

The database is project-bound and kept outside the replaceable module. Migrations fail closed on metadata/schema disagreement. WAL + FULL synchronization protect committed writes. Read-only requests use a snapshot without acquiring a writer lock; writes use `BEGIN IMMEDIATE`, prepared statements, foreign keys and uniqueness constraints. Owner transfer, membership changes and associated audit record are one transaction.

Connect and World are separate stores. Creation and ownership transfer persist request receipts; message IDs are unique per author/client request. A proof retry after an ambiguous outer Connect commit cannot duplicate those effects. Receipts retain identities and re-project current permissions/visibility rather than returning stale full objects. Audit rows store identifiers and action names, not message bodies or credentials.

Schema 3 adds a transactional public directory, exact per-kind counters and SQLite FTS5 (`unicode61`, prefix lengths 2/3/4). Search is deterministic and Unicode case-aware: every word is a literal token prefix, combined with AND; it is not arbitrary substring matching, a fuzzy ranking API or user-supplied FTS syntax. Direct entity-ID lookup still applies public visibility. Hidden people, archived communities and invitation-only communities never enter the public directory. Each public mutation updates the projection and FTS in its own transaction. Pagination follows immutable sequence numbers rather than offsets, so removal from a previous page does not skip the next page. Cursors are scoped to the exact normalized query text and kind; changing either requires a fresh search. Re-showing a hidden entity appends it at a new sequence position. Search is a current-view pagination contract, not a frozen multi-request snapshot. Text-query counts inspect at most 1001 matches per entity kind and mark larger totals as lower bounds. No online status is inferred from discoverability or group size. Presence preferences are stored, but no individual presence signal is published by this module.

## Verification

```powershell
node --test modules/world/test/*.test.mjs
```

The suite includes file-backed restart tests, privacy projections, role boundaries, idempotency, revocation, message pagination, and two independently generated P-256 accounts through Connect and its actual HTTP adapter. Browser workflow verification belongs to the product integration; an API test does not certify its UI or deployment.

## Scale observations

`node scripts/world-discovery-benchmark.mjs` builds isolated temporary fixtures of 10,000 and 50,000 public profiles with mixed communities. It records 50 warm samples per case, p50/p95 and raw durations in `output/discovery-scale-20260928/benchmark.json`; fixture databases are removed after the run. It does not contact a running server or use production data. Timings are observations, never machine-dependent test thresholds. See [the measured scale report](../../docs/research/discovery-scale-20260928.md) for the hardware/runtime, exclusions and remaining large-community projection cost.
