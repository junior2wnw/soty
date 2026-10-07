# Scoped personal memory

Host-only Node 24 SQLite component. It has no HTTP endpoint, account creation, grant issuance,
provider credential, automatic inference or application-specific agent prompt. It is **not yet a
connected Soty personal-agent service**. The controller must bind it to existing Connect/U1 authority.

## Integration boundary

```js
import { openMemoryPartition } from './modules/personal-agent/memory/index.mjs';

const memory = openMemoryPartition({
  databasePath: controllerOwnedPrivatePath,
  scope: {
    issuer: canonicalIssuer,
    accountId: verifiedAccountId,
    audienceKind: 'personal', // personal | project | community
    audienceId: verifiedAccountId,
    projectId: null,
  },
  context: opaqueCurrentContext,
  verifyContext: trustedCurrentAuthority,
  readRestoreFloor: trustedExternalFloor,
  advanceRestoreFloor: trustedExternalEraseCas,
  // Optional. Without an encoder, recall uses local FTS5 only.
  embedding: { id: pinnedEncoderVersion, dimensions: 384, embed: localEmbed },
});
```

`databasePath`, scope, callbacks and context are controller inputs. A model tool receives only its
closed record/query payload; the controller adds context and signal. Do not expose this constructor
or callback configuration as a user/model tool. Never derive a path from a model-supplied account,
scope or file name. The parent directory must be private/protected by the host OS; the module rejects
a final symlink/file mismatch but does not establish Windows ACLs or defeat a compromised host/admin.

Scope is copied and frozen, bound to the SQLite metadata and includes the exact issuer/account/
audience/project tuple. `personal` requires `audienceId===accountId`; `project` requires
`audienceId===projectId`. Existing data cannot be opened under a different tuple or encoder version.
Schema drift is rejected without automatic migration. Use physically separate files per tuple.

**Connect trust realm `soty`, a project UUID and a Source-native project ID remain separate concepts.**
Resolve the actual canonical identifiers with the existing controller; this component creates none.

## Trusted admission callback

```js
verifyContext({ context, scope, operation, partitionId })
  // synchronous: return { leaseId, epoch, expiresAt } or deny/throw
```

The host validates the opaque context against current device/account admission, audience membership,
project grants, permitted operation and cancellation epoch. This callback is the integration point for
existing authority, not a new grant engine. Boolean `true`, missing fields, an expired lease, Promise,
async callback or thrown error all fail closed. Callback errors do not expose their original message.

The component checks admission before private queries, before starting the encoder, after awaiting it,
inside the SQLite write transaction, before COMMIT and before result delivery. An operation captures
`leaseId`, `epoch` and current restore floor. A changed value rejects late work. Renewing the same live
lease may extend `expiresAt`; a new lease ID/epoch requires a new operation/context. Caller `AbortSignal`
is captured with the invocation; an aborted or closed invocation never dispatches its late write.

`operation` is one of `open`, `remember`, `recall`, `supersede`, `delete`, `export`. Export permission may
be narrower than recall. Root integration must implement all operations explicitly and keep the current
authorization check on every invocation, including receipt replay. Do not use a cached login boolean.

## Data API

All methods take the controller's `context` and optional `signal`. No method accepts tenant, path,
scope, account selector, audience selector, grant or upstream URL.

```js
await memory.remember({ context, mutationId, record: {
  id: stableControllerRecordId,
  type: 'fact', // fact | decision | preference | error
  text: 'A preference the user explicitly chose to save',
  source: 'versioned provenance supplied by the controller',
  freshUntil: freshnessDeadline,
  retainUntil: explicitRetentionDeadline,
  importance: 0.7, // optional, 0..1
  confidence: 0.8, // optional, 0..1; no authority follows from this value
} });

await memory.recall({ context, query: 'saved preference', limit: 8 });

await memory.supersede({ context, mutationId, records: [
  { id: oldRecordId, expectedRevision: 1 },
], record: replacementWithNewStableId });

memory.delete({ context, mutationId, record: { id: recordId, expectedRevision: 1 } });
const exportArtifact = memory.export({ context });
memory.close();
```

Remember creates a new ID at revision1. It does not overwrite an existing ID. Supersede requires all
old IDs/versions in the **bound partition**, creates a new record ID and erases the originals in the
same local transaction. Delete checks the exact revision and leaves a no-content tombstone at the next
revision. Tombstones prevent recreating an erased ID. Neither recall ranking nor model `scope` is an ACL.

Mutation IDs pin canonical input and survive restart. Same ID + different input is rejected. Exact retry
returns the original no-content receipt even after expiry/erasure; it does not recreate the record or
claim that the current record still exists. Current authority/floor is required for historical replay.

`freshUntil` marks a returned record stale. `retainUntil` excludes expired text from semantic/FTS recall
and export. The controller must schedule explicit erasure/derived-index cleanup and independent backup
retention; expiry alone is not physical disk erasure. A replacement that expires while embedding waits
cannot commit. All new retention deadlines are bounded by the configured maximum.

## External floor, erasure and restore

```js
readRestoreFloor({ scope, partitionId })
  // synchronous durable monotonically increasing safe integer, outside restored snapshots

advanceRestoreFloor({ scope, partitionId, expectedFloor, mutationId, erasedIds })
  // synchronous durable CAS + idempotent erase intent; return expectedFloor+1
```

This external state must not be rolled back with the memory snapshot. Integrate it into the existing
trusted controller revocation/deletion authority. Retain exact erase IDs/intents so recovery can prove
which records and derived copies must remain erased. The test fixture is deliberately separate SQLite;
it is not a production authority service or a proposed additional login/grant database.

Supersede/delete first verify versions/capacity, then durably advance the external floor and commit
the local erasure, new floor and no-content receipt together. Any current/restored SQLite whose floor
differs from the external floor rejects all private operations. A deletion also cancels an in-flight
embedding captured before it, even if its lease remains otherwise valid.

There is no distributed atomic commit between these stores. If the external advance succeeds but local
COMMIT/ACK fails, the outcome is unknown and the partition remains fail closed. **Never repair it by
merely copying the newer integer into an old snapshot.** The trusted controller must reconcile durable
erase intents and all affected indices. A missing/empty local database is rejected if the external floor
is nonzero: accepting it would forget old tombstones and allow an old remember intent to recreate erased
data. Quarantine alone does not authorize a new empty store. Recover from a certified current snapshot
or implement an explicit trusted generation/reconciliation mechanism that carries the anti-replay/deletion
history. This module intentionally has no public method to lower/adopt a floor or silently import a stale
snapshot. Recovery, encrypted backup/export storage and global replica deletion are host integration gates
still outstanding.

Deleting SQL rows removes their current text/vector/FTS projection. It does not promise secure erasure
of filesystem pages, WAL, offline exports or already delivered copies. Export contains no vectors,
provider keys or transcript history added implicitly; the controller controls the audience and artifact
encryption/retention.

## Bounds, encoder and operational limits

Defaults:1000 records;16MiB content;64KiB/record;10000 receipts/tombstones; recall max10;64 candidates;
4 concurrent encoder operations;10s encoder deadline;365-day maximum new retention. Host overrides can
only reduce admission. New records and replacements reserve an erase receipt and tombstone for every
remaining live record; churn therefore stops before metadata capacity can strand stored content. Erase
uses this reserved headroom up to the hard defaults even if the host later lowers its admission quotas.
Quotas count expired stored records until explicit deletion. Existing same-ID receipts remain replayable
when quota is full. A bound handle has no unauthenticated count/snippet endpoint. Filesystem/storage failure
still requires trusted recovery; this is not a promise of deletion when the underlying disk is unavailable.

Optional encoder ID/dimensions are pinned to the store. FTS5 and semantic SQL read only the bound file
after admission. Fragmented or unbounded model streams are outside this module. An encoder that ignores
abort retains its capacity slot until its actual Promise settles; timeout/cancel does not permit an
unlimited pile of orphan work. Late rejection is consumed, output discarded, and no late record is
committed. The trusted encoder must itself enforce its local CPU/memory limits. This component makes
no network request and knows no provider bearer.

SQLite uses WAL, synchronous FULL, bounded busy wait, strict tables and exact schema checks. Contended
transactions use live guards and record CAS. Raw OS/process sandboxing is P0.5, not provided by SQLite.
Before server integration, include this store format in the relevant independent reader/snapshot/fallback
manifest; importing this library does not authorize production creation of a new store.

## Verification

Run on Node24.19 or a qualified compatible Node24 runtime:

```text
node --test modules/personal-agent/memory/test/memory.test.mjs modules/personal-agent/memory/test/durable.test.mjs
```

Tests use actual SQLite, synthetic local vectors and no provider/network/real account data. They cover
cross-account/audience recall and supersede, immutable bindings, version conflicts, no-content deletion,
private→group guards, stale lease/epoch/revoke, late read/write/cancel, ignored encoder abort/rejection,
quotas, retention, exact replay, schema drift, old snapshot rejection, external floor restart, fail-closed
unknown erasure and two actual OS processes racing through memory + external-floor SQLite.

The standalone tests prove the component contract. Actual Root Connect/grant binding, OS vault/sandbox,
multi-device UI, restore reconciliation and production rollout remain separate integration gates.
