# Private Native HTTP mount

`createHttpApp(distDir, { ...existingOptions, nativeSourceHandlers })` accepts an
optional private CODE array of own data records `{ handle, close }`. Both fields
are standalone function closures. Capture rejects getters, proxies, sparse arrays,
duplicate handler functions and extra fields before any product storage opens.
The existing installed registry loader and adapter brands remain unchanged.

Pass only the approved broker's narrow callbacks:

```js
const app = createHttpApp(distDir, {
  ...existingOptions,
  nativeSourceHandlers: [{ handle: broker.handle, close: broker.close }],
});
```

The captured function references cannot be replaced through the caller's array or
records. Host/socket/app/traffic guards and security headers precede this mount;
API body parsers and static assets follow it. A handler resolves strictly `true`
when it owns the response, or `false` without consuming/responding so ordinary
Root routes can run. Throws/rejections/invalid return values yield only
`503 native_source_unavailable`; error objects/messages/keys are never serialized.
An already started failed response is terminated rather than forwarded.

An omitted, undefined or empty array adds no middleware and invokes no callbacks.
Input validation alone transfers no ownership on rejection. After successful
capture, a rejected host startup closes only these captured resources once; the
existing `waitForRejectedHttpStart(error)` also waits for their async cleanup.
Normal `app.locals.closeServices()` closes them once before host databases.
Different handlers sharing the identical close function invoke it once. Closing
fences later requests and false/failed async continuations; it creates no new
credential, actor, session, grant, schema, provider or global process action.

The Tavysh implementation still accepts only its fixed four POST paths under
`/api/source/device/v1/` and enforces original Native HTTPS origin, fresh P256
device proof, private response nonce, explicit owner consent and current Native
profile/project fences. Query/consent callbacks are co-located private CODE;
they are not HTTP approve/enqueue/eval methods. Full sync carrier remains denied.

This Source seam does not install a production registry or alter `server/index.js`.
Root must approve the original Native HTTPS route and private installed CODE
factory/callback co-location before physical mounting. Peremetrika, Planner and
Poveday retain their existing Native authority ports and separate approval facts.

Focused checks (Node 24.19.0, own synthetic loopback only):

```text
node --test --test-concurrency=1 server/test/native-source-http.test.mjs
SOTY_TAVYSH_DEVICE_SOURCE_ROOT=<approved-own-Native-e157-source> node --test --test-concurrency=1 server/integration-test/tavysh-native-http-mount.test.mjs
```

The Native import remains an explicit integration prerequisite outside the ordinary
World/server test glob. Source tests do not qualify physical TLS, Native browser
consent, common SSO, private registry installation or whole-program readiness.
