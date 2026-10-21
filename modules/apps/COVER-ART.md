# App cover integration

Apps12/13/14 add the common `modules/cover-art` engine to the **same registry DB**, retaining audience9/10/11 and runtime base6/7/8 respectively. This package adds no HTTP image route, public byte URL, model provider, paid fallback, timer or generation capability.

## Admission and startup

`allowCoverMigration` is a trusted startup boolean, default **false**. Existing9/10/11 remain byte-for-byte old formats until it is true. The explicit cover choice also enables the required audience layer for an older base. An empty registry may explicitly request the complete format; selected-resource base8 becomes14. Normal default empty-store behavior remains compatible with existing tests and deployments.

The migration commits all common cover tables/indexes/triggers, a registry-owned random stable store ID in `app_cover_usage`, matching marker and user_version together. Startup recognizes their exact DDL using the common fragments; marker-only or hidden tables under an older marker are refused. It does not enable base7/8 runtime profiles in an already admitted lower audience/cover base. Existing Source tuples, canonical IDs, grants, publication policy/receipts, saved entries and discussions are preserved. A new store ID is never accepted from a request or derived from a movable filesystem path.

Provide `coverDecoder` only when the native dependency and actual authority ports exist. Share the same bounded `createCoverDecoder()` instance with the Root profile adapter. `coverLimits` is a host-only optional override of the common engine limits. A configured decoder requires synchronous current contacts and World authority ports; there is no permissive fallback.

The Apps adapter lazily requires the existing common ESM engine when enabled. Node24 is already the project runtime contract. Feature-OFF imports do not load native Sharp. Before enabling production, Root must package Sharp as a real runtime dependency and update independently admitted storage readers/labels for12/13/14.

## Connect registration

`apps.operations` remains the synchronous existing extension. `apps.coverExtension` is either **null** or a separate `{operations,executeAsync}` extension. Register the latter only when the actual decoder, schema and authority ports are enabled. Do not put both `execute` and `executeAsync` on one Connect extension. Root's capability/MCP publication must follow actual registration; this package does not advertise a generator.

All cover payloads require `expectedAccountId` matching the fresh signed actor. Closed operation names:

| Operation | Arguments after expectedAccountId |
| --- | --- |
| `apps.cover.status` | `appId`, optional `requestId`, `draftId` |
| `apps.cover.prepare` | `appId`, `requestId`, expected target/policy/cover revisions, `mime`, `bytesBase64`, optional `crop` |
| `apps.cover.prepareFromVersion` | same expected tuple, `versionId`, optional `crop` |
| `apps.cover.commit` | `appId`, `requestId`, `draftId` |
| `apps.cover.restore` | expected tuple plus `versionId` |
| `apps.cover.remove` | expected tuple, optional `mode` (`none`/`inherit`) |
| `apps.cover.read` | `appId`, optional retained `versionId`, `width` |
| `apps.cover.versions` | `appId`, optional `limit`1..20 |
| `apps.cover.cancel` | `appId`, `requestId`, `draftId` |

Expected tuple names are `expectedTargetRevision`, `expectedPolicyEpoch`, `expectedCoverRevision`. Use status `serverTime` to construct and durably retain the common `cover1.<time>.<hex>` request ID before sending. Unknown-outcome retries reuse it. Prepared drafts do not publish; commit changes only cover versions/head/receipt/counters. It does not bump Source, launch policy or app revisions. Common replay returns the original receipt plus **current** head.

Prepare accepts canonical strict padded base64 of PNG/JPEG/WebP, decoded length at most320KiB; encoded length is checked before decoding. The common real decoder validates decoded pixels and re-encodes bounded crops. A dedicated signed `read` returns one bounded rendition as `{role,width,height,mime,sha256,byteCount,base64}`. All other responses carry refs/metadata; do not expose image bytes through generic directory/tool context, logs or public cache. The Root transport retains its existing no-store authenticated Connect response behavior.

## Rights and Source

Every engine authority window reacquires actual Connect current-contact authority → World fence → the same Apps DB transaction. Decode runs outside those fences; readiness and commit recheck them. The adapter reads the enabled app's actual owner, active immutable runtime target/digest and publication epoch. Existing `resolveEntry` determines viewer access through current grants, reciprocal friends, publication and active named aliases. Selected Source additionally requires the current trusted host pin and persisted immutable admission, preserving base7 versus base8. Temporary connector offline status does not delete an owner's cover or grant another user access.

Owner-only: prepare/commit/restore/remove/cancel/status/history. Authorized viewers can read the current version; retained historical bytes require actual ownership. App/device revocation, contact removal and publication changes take effect on the next read/write even after reload. No old receipt authorizes access after revocation. Common quotas/TTL/GC/replacement-time undo protections remain unchanged.

## Verification and remaining host wiring

`test/app-cover.acceptance.test.mjs` uses real P-256 signed Connect requests, actual World fences, Apps SQLite and Sharp in guarded synthetic stores. Its three bases contain nonempty runtime history; bases7/8 contain actual persisted selected Source admissions and configured pins. Migration preserves old rows; signed prepare/commit/read/restore/replay survive reopen. Current publication/friends/grants, missing host pin, private history, target/policy/cover CAS, malformed base64 and revoked synthetic installation are negative scenarios. The installation revocation and alternate active Source are seeded in the **test's own** SQLite; this is not a device-management UI or a production Native Source call.

Existing tests/fixtures are not rewritten automatically. Default migration-OFF keeps their6/7/8 or9/10/11 expectations intact; targeted new tests explicitly opt into12/13/14. The scoped admission helper now maps all supported format layers to base7/8, fixing its previous7/8-only request check while preserving marker and host-pin validation.

Root still owns `server/http-app.js` registration, capability/MCP grants, production Sharp/Docker packaging, storage reader labels and exact migration admission, frontend upload/crop/status/undo and real UI validation. Do not enable the production migration until the matching reader/rollback image contract is admitted. This source integration alone does not claim deployment, browser/device acceptance or a configured generation provider.
