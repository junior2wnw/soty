# Connect browser client

Framework-free ESM for secure browser contexts. `index.mjs` has no import-time I/O. `index.d.ts` and the `.d.mts` entry provide the public TypeScript surface.

```js
import { createConnectClient } from './modules/connect/browser/index.mjs';

const connect = createConnectClient({
  projectId: 'my-product',
  endpoint: '/api/connect',
  onState: state => renderAccount(state),
  onError: error => showStateError(error.code),
});

await connect.bootstrap('My profile');
const { payload, revision } = await connect.loadVault();
await connect.saveVault({ draft: 'example' }, revision);
```

The endpoint must implement this module's project-scoped RPC protocol. It is not an OIDC adapter. This client makes only explicit requested calls; it has no timer-based synchronization or automatic account recovery.

## Durable identity

- IndexedDB version stays at `1`; the record schema is `connect.browser-state.v1`, with `minReaderEpoch: 1`. Unknown namespaced JSON fields in `extensions` are retained. An incompatible reader epoch, future schema, failed read, missing store, invalid key pair or corrupt root produces an error. None causes a replacement identity.
- Initial tabs atomically claim one installation. The nonextractable ECDSA signing key, nonextractable ECDH encryption key, and nonextractable local AES wrapping key are stored together. The random account root is encrypted locally and committed **before** server bootstrap. A successful server account ID cannot be reassigned for that installation.
- These are browser keys, not a promise of hardware protection. Same-origin malicious script may use them. Clearing site storage removes local access unless recovery was previously prepared and confirmed.
- `getLocalState()` and `onState` expose safe metadata only. The current account appears both under `current` and in top-level `accountId`, `deviceId` and `label` aliases. `profiles` lists retained local profiles. Secrets and raw storage records must not be logged.

## Adding an account or device

The recipient explicitly calls `startEnrollment(label)`. It generates and durably stores **new**, initially unregistered installation keys. On the existing authorized device, show `inspectEnrollment(requestId)` to the person before calling `approveEnrollment(requestId, displayedAccountId)`. Passing the displayed account prevents approval under another profile selected by a different tab.

The recipient calls `previewEnrollment(requestId)` using its own signing key. This read exposes the approving account and source-device labels, never the encrypted account key, and does not activate the account. Show both to the person, then call `finishEnrollment(requestId, preview.account.accountId)` on explicit acceptance. The client requires a matching preview and unchanged active profile; the server also binds the finish request to that expected account. After a reload or changed active profile, read a fresh preview before finishing. The UI shows a matching request code on both screens as a check against a substituted QR; labels alone are not proof of a person's identity.

`finishEnrollment` activates the received account and retains earlier profiles and their keys. `switchProfile(accountId)` is an explicit route back. `discardPendingEnrollment()` abandons only local pending UI state; it does not revoke a server grant or delete an old profile.

An expired saved request is not silently replaced. The UI resumes its confirmation check, which can still recover a completed response, or lets the person explicitly create a new QR. This retains existing profile keys.

The transferred root uses ephemeral ECDH P-256, HKDF SHA-256 and AES-GCM. Authenticated context includes schema, project, account, request and recipient encryption-key fingerprint. No source signing private key is exported. A durable approval envelope is reused after a lost response. Recipient keys stay available for retries after interrupted enrollment or recovery.

## Backup and recovery

`saveVault(payload, expectedRevision?, expectedAccountId?)` encrypts finite JSON up to 1 MiB and performs compare-and-swap. Capture the displayed account before asynchronously preparing a snapshot and pass it as `expectedAccountId`; a profile switch then fails before encryption or upload. Its AAD binds project, account, schema and revision. A stale writer receives the server conflict; the client does not overwrite or automatically merge. `loadVault(expectedAccountId?)` returns `{payload, revision}`, verifies the envelope and rejects a server revision older than the verified local revision. Caller-owned application data is not automatically imported. After an asynchronous confirmation, check that the same account is still selected before importing the result; account-isolated product storage should bind its own transaction to that account.

`prepareRecovery()` returns a **secret** `connect.recovery-kit.v1` JSON object. The caller must let the user explicitly save it outside this browser and supply the saved kit to `confirmRecovery(kit)`. Confirmation decrypts and compares the root; it does not by itself prove that an external file was durably saved. The server keeps the old verified recovery method until the new one is confirmed.

`recover(kit, label)` validates and decrypts the kit before redemption, persists new recipient keys before the network call and activates the recovered profile only after confirmation. Existing profiles are retained. The kit is single-use for a new recipient; a retry uses the same durable recipient and exact arguments. It contains no installation private keys. Treat its `secret` and encrypted root together as account-recovery authority: never put them in a URL, QR, analytics or logs. After a successful recovery, create a new recovery kit.

## Other tabs and failures

BroadcastChannel sends only a project and revision notice. Other instances re-read committed IndexedDB data and call `onState`; they do not consume account state from the channel. Without BroadcastChannel, callers can re-read `getLocalState()` on focus. No polling runs automatically. Call `dispose()` when the client is no longer used.

Requests capture their account context. A changed active profile during an awaited response raises `ACTIVE_PROFILE_CHANGED`; a backup payload from the old profile is not returned for the newly active one. Enrollment and recovery keep their pending material if another tab changes the active profile before activation. A rejected result does not prove a remote write was rolled back; re-read its state before retrying a non-idempotent product action.

Explicit operation failures reject their promises. `onState` receives committed state without blocking the operation queue, so it can safely call and await client methods. Synchronous throws and rejected observer promises, including external-tab notifications, are sent to optional `onError` and remain visible as safe `notificationError` metadata; they are not unhandled promises. Callback failure does not reject or roll back a completed server or local operation.

## Validation boundary

`test/browser-core.test.mjs` runs native WebCrypto and the actual in-memory server against injected atomic memory storage. It checks cryptographic separation, AAD, profile preservation, recovery/enrollment lost responses, CAS, corrupt/future state, contact consent, revocation and late cross-tab responses. Real IndexedDB transactions, browser lifecycle and UI accessibility require the separate real-browser integration checks. This module does not claim that revoked devices erase previously obtained plaintext or keys.
