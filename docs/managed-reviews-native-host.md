# Same-process Native Reviews host seam

Source only. This does not qualify real Native RP, PostgreSQL, browser, HTTP
startup, installation or publication. Root login/publisher ownership creates no
Native grant. No Source credential, request or cookie is serialized.

One existing Native application owns its managedService/managedSourceApp and
store. A hash-approved Native CODE module can borrow that minimal view, create
`createManagedReviewsNativeBridge({nativeApplication,providerRef,
withHumanSubjectAuthority})`, then call `createRootStartup({env,httpOptions})`
with reviewed Root settings, reviewsConfiguration and the returned
managedReviewsSource. The Human port is a late closure to that Root instance's
`app.locals.humanIdentityService.withSubjectAuthority`, using the original
signed Root actor. After the Root handle exists the owner explicitly calls
`start()`. Existing Native HTTP routing/bootstrap/admin/seed/GC/mail workers
stay in the Native owner; no second Native service/store/RP is created.

Root factory import is inert. Its start/close do not own process signals,
disconnect, IPC or exit. The ordinary Root index wrapper retains these duties,
ready notification and logs. Embedded owners close the bridge synchronously
before awaiting Root close; only the original Native owner closes Native.
Close promises publish before invoking closers and attempt all existing Root
service closers in their established order. Errors expose only safe codes.
If close begins while start is awaiting its listen acknowledgement, close
rejects that same pending start with root_startup_closed before cleanup awaits.
A late listening event cannot confirm it. Its queued listen error remains
handled once; internal rejection observation does not replace the caller's
rejected promise.
The ordinary wrapper checks its own shutdown state before its listening log
and ready IPC. Its normal ready payload and logs remain unchanged; a late
listening event after shutdown does not publish them.

Each private single-use token correlates one original Native request, original
Root actor and complete Human tuple. Parallel calls sharing a Native request
have different tokens. Full Human fields are rechecked before and after actual
Native withActor and every exposed operation. Callback replay/async/late fence,
foreign Source actor, close, abort or changed identity deny protected output.
Native still owns current RP/grants/target and receipt transactions.

The collectionAttempt marker is set immediately before the SAME Native.collect
delegate. A later error may follow a committed review receipt and becomes
managed_reviews_outcome_unknown; retain the existing stable requestId/draft,
never automatically write again. Earlier Native.withActor may already create
a principal: collectionAttempt=false is NOT an all-Native-effects0/rollback
claim. Root's subsequent Connect/Apps rejection can retain its admission code;
the existing collect UI already treats every failed ACK as unknown and retains
text/nonce. Read results that fail current Human fences are withheld.

Explicit Root env is mandatory for the embedded factory; no global env swap.
Every env-backed Gonka field is passed explicitly, including empty primary,
fallback, application tokens and token-file settings when absent. Metrics
callback is null. Native voice/R10 use only supplied Root selections. HTTP
trust/dev/release/origin settings and Connect origins use the supplied env.
This is not full process-environment isolation: existing connector-store
preserveHistory and traffic-control ambient settings require a clean parent
environment. No paid/provider request is made by this seam itself. Existing
R10 installed-entry ABI and NativeVoice4 are unchanged.

Canonical component tests use actual Root managed extension/port and actual
new factory/cleanup function bodies with controlled Native/Human/transport
ports. They are not genuine Native permission or physical cleanup evidence.
An additional controlled actual Native JSON service qualification can be kept
in research with explicit pinned Native dependencies; it is not an optional
skip-green dependency of the Root test suite.
