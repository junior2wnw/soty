# Additive host-only Native async-authority SPI

This delta follows Standard2 e69629c and completed state-only receipt cd1bbad.
It changes no HTTP/JSON wire, Source profile pin, Root DDL, Source native format,
RP49 files or issuer protocol. It does not implement a PG adapter or PG readiness.

Optional constructor hook:

```ts
assertCurrent?(proof: NativePrivateProof, binding: Readonly<NativeBinding>): Promise<void>
```

Runtime accepts only its actual private branded proof and captured immutable
binding before/after the await; close/dispose or returned permission/data DTO
rejects. The hook must query authoritative CURRENT Native SQL session/principal/
profile/resource grant, not reuse a cached row as cross-process authority. It
creates no permission and returns no Native rows, Root credentials or user DTO.

Absent hook retains the actual sync Native withCurrent path. Present hook moves
capture/recover/read/mutation and BFF pre/post current checks outside SQL locks
to the async authoritative port. Root authority is checked after Native awaits
too. Native mutating operation still receives an operation-local branded sync
commit capability. Only final remembered-proof/link/completion/mutation callbacks
use withCurrent inside the actual Native transaction witness. No callback may
be late/double/reentrant/thenable; post-COMMIT denial is UNKNOWN, never rollback.

PG consumer owns its private per-request proof custody/ALS checkpoint, row locks,
actual domain SQL and final temporal check before COMMIT. Its storage claim/
complete/consume must asynchronously lock exact captured Native rows BEFORE
calling the SDK's synchronous final callback, keep those locks through actual
COMMIT, and let withCurrent assert the exact live checkpoint/actor/resource/
generation/deadline. No generic SDK SQL transaction or new prepare HTTP hook is
introduced. External Root/provider/file IO stays outside SQL transaction locks.
Metadata, body identities and Root App ownership remain insufficient for Native
role/resource/support permission. Native read/write hooks also perform their own
operation-specific SQL ACL checks; an async pre/post check is not a commit fence.

Meaningful new gates6/6 (synthetic):

- Actual SQL-backed async current check, final sync callback only inside exact
  live Native transaction witness; no sync outside-TX authorization fallback.
- Another SQL connection revokes after async preparation: final check yields
  zero effect/receipt. An after-COMMIT revoke yields UNKNOWN and retains one
  receipt; readback needs freshly restored Native authority, never another apply.
- Revoke during async post-read check prevents private result delivery.
- JSON proof/returned permission DTO/dispose during await rejected.
- Actual maintained OIDC Source2 BFF: remembered/link/consume and feedback sync
  callbacks run only inside Source SQL transaction; async checks run outside.
  Revoke while awaited prevents context, retains original single ticket.

SQLite is deliberately the runnable test authority for this SPI. These tests
prove hook ordering/current/final fences, not PostgreSQL locking, PG migration,
PG cold restart, long sessions or production access. The PG consumer must pass
its own actual storage/process/HTTP/browser/release gates before claiming Ready.
