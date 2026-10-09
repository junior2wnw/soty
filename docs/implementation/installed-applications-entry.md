# Operator-installed application entry

`loadInstalledApplicationsEntry(env = process.env)` is a startup-only loader.
No variables means `{ httpOptions: {}, sourceReferences: [], close }`: no module
file, callback, storage or middleware is touched. The sole opt-in is both
`SOTY_INSTALLED_APPLICATIONS_MODULE` (absolute local OS `.mjs` file) and
`SOTY_INSTALLED_APPLICATIONS_SHA256` (approved lowercase raw-source SHA256).
It is operator CODE, never an HTTP/body/catalogue/JSON module selector or RPC.

Preflight validates a bounded regular file (128KiB), no symlink/Windows UNC or
device path, valid UTF8, initial/held/post file stamps and exact raw SHA256 before
execution. The module executes the captured bytes, not a subsequent path read.
File/path/content/errors/keys are not echoed. Module initialization must be pure;
trusted operator CODE is not an untrusted-code sandbox. Imports in captured `.mjs`
must use absolute URLs or built-ins; relative imports have no data-URL base.

The module exports only `default`, a genuine function. It receives an immutable
private API with the existing `loadInstalledExistingApplications`,
`createTavyshNativeDeviceBroker`, `createTavyshDeviceStatusAdapter`, plus `ownClose`.
These are the current Root namespace's constructors/validators, not actor/grant
issuers or new services. No fixture flags, capability toggles or Native authority
options can be returned.

The factory registers each effectful resource's close before creating effects:
`api.ownClose(close)` returns the retained once-only callback. It returns exactly
`{ httpOptions, sourceReferences, close }`; HTTP options admit only original
`externalApplications` and optional `{ handle, close }` Native handlers. Existing
registry and handler validators are reused; references must equal the validated
composition's actual references. Source catalogue/adapter brands and current
Root/Native rights remain independent. A module cannot issue a grant through metadata.

```js
// One private OS-installed, hash-approved module; no production file is installed here.
export default async function(api) {
  const composition = await api.loadInstalledExistingApplications({
    createRegistry: () => approvedExistingRows,
  });
  // approvedExistingRows are actual admitted adapters/current Native ports,
  // not manifest descriptors, fixture flags or a newly invented owner.
  const close = api.ownClose(closeOnlyOwnedInstallation);
  return { ...composition, httpOptions: { ...composition.httpOptions,
    nativeSourceHandlers: approvedOwnBrokerHandlers }, close };
}
```

Register effects during setup even if a later await/validation fails. If a factory
throws or its result is invalid, registered resources close once and startup
receives only `installed_applications_entry_unavailable`. Invalid path/hash/file
preflight never imports/calls the factory. Same close functions in factory/handler
records share once wrappers. On success the returned HTTP lifecycle row binds
cleanup to the accepted Root constructor seam: startup rejection and ordinary
`app.locals.closeServices()` close before dependent stores. Default remains OFF.

The maintained `server/index.js` hookup loads after existing config/operator-flag
validation and before Root data construction:

```js
import { loadInstalledApplicationsEntry } from './installed-applications-entry.js';
const installedApplications = await loadInstalledApplicationsEntry();
// Add ...installedApplications.httpOptions to the existing createHttpApp options.
```

Keep the existing `waitForRejectedHttpStart(error)` and shutdown wiring. An
operator may also await `installedApplications.close()` in an outer bootstrap
failure path; it is idempotent. The maintained tests execute this actual index
with only dependency import URLs adapted in an OWN temporary copy, forked with
own loopback/temp storage. They do not inject a proposed callsite.

Tavysh setup still needs the original Native HTTPS ingress live before the
factory awaits Native owner consent, the same co-located broker/lifetime, exact
existing Native profile/project/instance/current device and Root principal/target.
Use the existing Source private-installation recipe; no HTTP approve, guest/key
bootstrap, publisher-as-Native-owner, full sync carrier or silent expiry renewal.
The loader makes that private CODE reachable from ordinary startup after Root
installs the explicit module and approved callsite; it does not supply missing
Native identity/permissions or activate a production registry by itself.

Checks: `node --test --test-concurrency=1 server/test/installed-applications-entry.test.mjs`
and current `server/test/native-source-http.test.mjs`, plus Types. No browser,
Engine, deployment, real keys, providers, World or Native505 are required here.

An installed CODE factory may also return the optional genuine synchronous
`sourceCredentialFor({ actor, reference })` function. It is captured as DATA,
never from JSON/RPC/catalogue/manifest. Async/generator/proxy functions and
accessors deny; a Promise returned at a later call is refused by the current-port
guard. References remain exclusively derived from the admitted CODE registry,
including exact app-target/Source pins. The factory cannot substitute references
or issue a grant.

Index merges ONLY derived references and this callback into the existing personal
agent configuration. Enabled/gateway/account allowlist/history/local targets keep
their original values and references. The callback must return a private,
already-issued current Root capability credential `{ token, audience }`. Existing
authentication and owner fence require the SAME Root account and creator device
as the signed human actor. Billing credentials, OAuth tokens, Native device claims
and publisher metadata cannot replace it. Actual Native owner/project consent and
approved broker ports remain independent inputs.

The agent exposes a constant catalogue/query tool set for up to the existing
registry64 bound. Multiple Sources require an exact reference or an authorized
catalogue query with exactly ONE admitted readonly match. Ambiguous/denied/absent
matches never query and never fall back to another Source. Omission selects only
a sole admitted Source and grants no authority. The selected set
starts empty per turn and admits at most8 references. A revoked/offline unused
Source does not gate Notes/apps/history. Metadata/query actually used retains its
SAME delegated credential and reference; the existing current guard rechecks it
before model dispatch, after awaits and before delivery. It never resolves a new
token to replace revoked/expired permission. A new Notes-only turn has no selected
Source. Existing Core/runtime/context limits and Source ledger/CAS remain intact.

Catalogue/contract/query reuse existing external search/getContract/query APIs.
Known closed denials can hide catalogue entries; unknown code/storage failures
and cancellation are not treated as permission. Reads retain Root+Apps+Native
admission/current predicates, before/after guards, exact input/output validation
and actual charged receipts. Metadata is `unprobed` availability, not Native SSO,
physical readiness or a write. `apps.list` remains application metadata.

For device-only Tavysh, each enabled query still requires its two genuine signed
Native epoch/device attestations. Its synchronous catalogue predicate checks the
retained live consent/scope, not an IDB transaction or a new device signature
after an arbitrary later model wait. Unsupported full carrier/rememberLogin/
commitIdentity remains DENY. Paid Source/model, original Native HTTPS installation,
physical Native/browser and interapp effects need their separate actual gates.

The unchanged Core offers one nonterminal tool then its terminal answer per turn.
Unique-query selection composes existing search/getContract/query inside that
single callback; it does not widen Core/vendor/policy or require a separate user
catalogue turn. A future bounded readonly batch of explicit references/unique
queries is an eligible existing seam, but is not implemented or qualified here.
