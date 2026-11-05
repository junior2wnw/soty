# Реферальный модуль

Переносимый SQLite ledger реферальных начислений. По умолчанию три пригласивших над плательщиком получают **по10% от одной исходной допустимой суммы**, а не10% от комиссии предыдущего уровня. Четвёртая линия ничего не получает. Это учёт обязательств: реальные переводы, кошельки и платёжные провайдеры не подключены, `payoutsEnabled:false`.

## Подключение к host

```js
import { createReferralsService } from './modules/referrals/server/index.mjs';
const referrals = createReferralsService({
  databasePath: '/durable-data/referrals.sqlite', // outside the module/release directory
  projectId: 'soty',
  authorize({actor, appId, right}) {
    // Synchronous host-owned authority; do not use roles supplied in request JSON.
    // read: authenticated active principal + app access + referrals.read grant.
    // configure: same checks + current app owner + referrals.configure grant.
    return hostReferralsAuthority.check(actor, appId, right);
  },
  readTrustedEvent(context) {
    // Context is an opaque proof issued/resolved by the invitation/payment host.
    // Reject missing, revoked, unbound or changed proofs. Resolve their app/payer/
    // eligible amount/currency/refund from authoritative host records, not a body.
    return hostVerifiedEvents.read(context);
  },
});
// Connect extension contract: {operations:Set, execute({op,args,actor}), close()}.
// Add to the host's normal extensions; this module does not edit the host itself.
```

`actor` comes only from the authenticated host after signature/session/device checks. Each public operation requires `expectedAccountId===actor.accountId`; the field detects an account switch, never chooses the owner. Unknown fields are rejected. Inputs use the existing bounded app-contract snapshot, which rejects accessors, unsafe numbers and invalid JSON.

`authorize` is mandatory and synchronous. Its `configure` result **must include current app-owner authorization**, not just a generic app-member grant. It is checked before and inside the transaction and just before COMMIT. An agent's right to discover metadata is not permission to read or configure money records. The module never trusts `actor.roles`, a request's `ownerId`, or client-selected `beneficiaryAccountId`.

`readTrustedEvent` is optional for read/configuration-only hosts. Without it, the internal port fails closed. It is a synchronous closure resolving an opaque context to trusted data and is rechecked inside the transaction and before COMMIT. Do not implement it as `context => context` when the context can originate from public JSON. If upstream verification is asynchronous, finish it in the trusted host adapter first, issue a scoped proof, and keep its lifetime/authority valid through the synchronous transaction.

## Public operations

All arguments contain `expectedAccountId,appId`. App/account/device/event IDs are1–128 ASCII characters `[A-Za-z0-9._:@/-]`, starting with an alphanumeric character; `..` and `//` are rejected. No raw database path, arbitrary SQL, URL, payment-provider endpoint or token is accepted.

| Operation | Arguments in addition to common fields | Result/right |
|---|---|---|
| `referrals.config.get` | none | Current config, `read`. An unconfigured app has virtual revision0 and rates `[1000,1000,1000]`. |
| `referrals.config.put` | `mutationId,expectedRevision,ratesBps` | Owner-only `configure`; CAS update + durable receipt + immutable config history. |
| `referrals.attribution.get` | none | `read`; **current account's** binding only. `bound:false` means not yet bound; `bound:true` without `parentAccountId` means sealed root. |
| `referrals.balance.get` | `currency` | `read`; **current beneficiary's** app/currency balance, `amountMinor` as a decimal string. |
| `referrals.ledger.list` | `cursor?` last entryID, `limit?`1–100, default30 | `read`; current beneficiary/app entries only, optional `nextCursor`. No payer ID or internal payment/event ID exposed. |

Config allows1–10 levels, each0–10000 basis points, sum≤10000. A zero rate disables that level's commission; `[0]` disables accrual. The array's length chooses the number of paid levels. There is no extra residual commission or spill into a fourth default level. New config applies **only to future payments**. A payment retains its config revision, exact rates and original beneficiaries; later configuration never rewrites it or its refunds.

Every new configuration is CAS-versioned. Reusing the same app/mutationID with identical normalized payload returns the original receipt with `replayed:true`, even after later edits/reopen. Changed payload yields `referrals_event_reused`; stale revision yields `referrals_revision_conflict`. Receipts are not evicted, because losing financial idempotency is unacceptable.

## Trusted internal events

Only `referrals.internal.applyEvent({context})` consumes these resolved shapes. This method is deliberately **absent from `operations`, public RPC and all agent capabilities**.

```ts
type Event =
  | {kind:'attribution'; appId:string; eventId:string;
     accountId:string; parentAccountId:string|null}
  | {kind:'payment'; appId:string; eventId:string; // authoritative payment ID
     payerAccountId:string; currency:string; eligibleAmountMinor:number}
  | {kind:'refund'; appId:string; eventId:string; // authoritative refund ID
     paymentId:string; refundAmountMinor:number};
```

- The invitation host binds each app/account parent once. Same-parent reassertion is harmless, changing parent or root is forbidden. Self-referral and cycles are rejected, including a cycle completed later. App scopes are independent. Missing ancestors stop the chain; they are never invented. The ancestry check is bounded at10000 identities and refuses deeper input.
- The payment host decides what **eligible amount** means (for example, excluding tax/fees where its business rules require that). This module does not infer eligibility from a browser's requested purchase amount. Payment IDs are unique per app and event kind; a retry with changed amount, payer or currency is rejected.
- A refund has its own stable refundID and refers to an original payment in the **same app**. Each delta must be positive, and cumulative refund cannot exceed that payment's eligible amount. The currency and beneficiary set come from the original payment. A retry is not a second refund; changed reuse is rejected.

## Exact arithmetic and durability

Input minor units are safe, nonnegative JavaScript integers; fractional/negative/unsafe values and negative zero are refused. Currency is an uppercase three-letter code. A currency's minor-unit exponent belongs to the host business contract; this module does not guess it or convert currency.

For each beneficiary, commission is `floor(eligibleAmountMinor * rateBps / 10000)` using **BigInt multiplication/division**, then a safe integer result. Each ledger row stores exact integer minor units. Independent app/currency balance projections use decimal-text storage and BigInt addition, so repeated large payments remain exact past JavaScript's total safe-integer range; public `amountMinor` balance is a decimal string (up to32digits).

For a partial refund, cumulative reversal for each original commission is:

```text
floor(originalCommissionMinor * cumulativeRefundMinor / originalEligibleMinor)
```

Only the difference from previously reversed amount is appended as a negative ledger entry. A full refund reverses **exactly the original commission**, including rounding remainder. Splitting a refund cannot create or leave extra money. Zero-eligible payments have no refundable amount. Later rate changes have no effect on this calculation.

SQLite uses WAL, synchronous=FULL, foreign_keys=ON and BEGIN IMMEDIATE for writes. Payment/refund, beneficiaries, balance projection and durable receipt commit atomically; success is returned **after COMMIT**. Failure rolls the entire transaction back, including a failure at COMMIT itself. Identical authoritative events may then be retried safely. No background task acknowledges a credit before its storage receipt.

Ledger, receipt, attribution, metadata and config-history rows reject accidental UPDATE/DELETE via SQLite triggers. The registry binds a schema version and `projectId`; reopening with another project or unsupported schema fails, preserving data. Persistent database must live outside the module directory, including symlink resolution; replacing module code does not replace the ledger. `:memory:` is available only when an ephemeral store is intentionally wanted.

## Agent discovery

`manifest.json` describes this module's contract and rights. `createReferralDescriptor` in `descriptor.mjs` generates a validated **`soty.app-agent.v1`** descriptor through the existing app-contract SDK. The host supplies its actual source digest, shared-Soty identity profile, scoped resource, separate read/configure binding pins and required feedback provider pins. No private endpoints, fabricated source pins, credentials or active execution grants are embedded.

```js
import { createReferralDescriptor } from './modules/referrals/descriptor.mjs';
const descriptor = createReferralDescriptor({
  appId: registeredModuleAppId, source: actualSourcePin,
  auth: {mode:'shared-soty', profile: identityProfilePin},
  resource: scopedReferralLedgerPin,
  bindings: {read: readBindingPin, configure: ownerConfigureBindingPin},
  feedback: installedFeedbackProfile,
  docs: [publishedGuidePin], skills: [publishedSkillPin],
});
```

The host admits/publishes this descriptor through its ordinary registry and grants machinery. Metadata generation is not publication or integration. `referrals.read` and `referrals.configure` are different grants; the configure binding must preserve the app-owner check. There is no `credit`, `payment`, `refund`, `withdraw` or `payout` MCP tool. Integration must not wrap `internal.applyEvent` in an agent-accessible endpoint.

## Verification

```powershell
node --test modules/referrals/test/*.test.mjs
```

The suite uses real temporary SQLite files, two app scopes, actual app-contract validation, reopen, config CAS/replay, cycle rejection, original config snapshots, partial/full refunds, bounded own-account paging and exact sums. Negative controls demonstrate a one-unit floating-point error, a duplicate-credit balance error and refund-split rounding loss. A real deferred foreign-key fault makes **COMMIT fail** after tentative ledger/receipt work; no money/event is visible, and the same event succeeds after removing the test fault. Revocation/proof-change tests refuse COMMIT and preserve prior state.

This module does not prove a host's identity/owner adapter, realtime UI, real invitation/payment event producer, external agent admission or public deployment. Those require host integration tests and end-to-end acceptance. No actual money movement is enabled.
