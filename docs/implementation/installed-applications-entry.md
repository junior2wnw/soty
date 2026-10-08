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

Proposed three-line coordinated `server/index.js` hookup (not applied by this
Source branch); place the load after existing config/operator-flag validation:

```js
import { loadInstalledApplicationsEntry } from './installed-applications-entry.js';
const installedApplications = await loadInstalledApplicationsEntry();
// Add ...installedApplications.httpOptions to the existing createHttpApp options.
```

Keep the existing `waitForRejectedHttpStart(error)` and shutdown wiring. An
operator may also await `installedApplications.close()` in an outer bootstrap
failure path; it is idempotent. The actual tests adapt only these loader callsite
lines/import URLs in an OWN temporary copy of the current index and fork it with
own loopback/temp storage. No tracked/shared index or private config is changed.

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
