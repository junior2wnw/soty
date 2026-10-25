# Optional Native entry binding for Python 3.11+

Install the pinned `requirements.txt`, then import `soty_native_entry` from the
reviewed portable SDK's `python` directory. Its SHA256 is in the SDK provenance
manifest. The Node Source host and frame/broker SDK remain the normal Source
transport; this optional module implements only the exact same Native PUBLIC
app-policy wire and does not implement Human identity or a Source cookie jar.

```python
guard = NativeEntryGuard(operator_configuration)  # same keys/pins as Node
app.add_middleware(NativeEntryMiddleware, guard=guard,
    bypass_paths=("/internal-authenticated-market",))  # CODE-owned MAC port only
pool = fence_asyncpg_pool(existing_pool)  # optional closed asyncpg subset

# Inside existing Native business code, before acquiring SQL locks:
await refresh_native_entry()
async with connection.transaction():
    # Existing Native issuer/sub/session/CSRF/ACL checks still apply.
    await connection.execute(...)
    assert_native_entry_current()  # final synchronous boundary before COMMIT
```

The middleware maintains a <=1s monotonic lease with <=250ms heartbeat for both
HTTP and WS. It refreshes before a WS command and before HTTP response headers,
checks every outgoing private body/frame, and cancels pending endpoint work on
policy loss. OIDC/network waits renew the same scope without a new login. A
policy epoch/admission change does not silently migrate that scope. Use the
final transaction check too: task cancellation is not proof of DB rollback.

The optional `fence_asyncpg_pool` adapter supports acquire/release, explicit
transactions and execute/executemany/fetch/fetchrow/fetchval. It adds a real
transaction around an otherwise autocommit operation, checking the current
local lease after SQL and immediately before COMMIT. Existing explicit
transactions also get a final check. It never awaits Root inside a SQL
transaction; the independent request heartbeat continues. Unsupported
cursor/prepare/copy/raw methods fail closed. It is not a complete asyncpg proxy
or a replacement for domain ACL, idempotency or validation.

Root requests have independent128 capacity (503 without eviction), at most128
active request/stream contexts, a <=1s absolute timeout and <=4096-byte strict
JSON response. Cookies, environment proxies, redirects, browser identity and
unsigned JSON leases are not accepted. Wall-clock skew cannot extend the local
lease: request latency consumes its monotonic duration. The fixture covers
connection capacity, a never-ending error body, forged lease copies, active
stream closure and actual SQLite rollback on revocation. Root HTTP integration
also runs this Python binding against the actual app registry/policy endpoint.

`assert_native_entry_current()` is context-local and synchronous. Background
jobs started outside an HTTP/WS request have no request policy context and keep
their independent authorization. Do not copy a context into unrelated jobs.
Never bypass public/private business routes. Bypass only already authenticated
internal ports whose own reviewed protocol supplies their independent fence.

Root and Native commits remain non-atomic. A still-live lease has a bounded
revocation race; a completed Native effect followed by lost response/policy is
UNKNOWN. Do not replay it automatically. Native autocommit effects cannot be
rolled back by a later output check, which is why final transaction fences are
part of the integration contract. Already delivered bytes cannot be recalled.

Qualification:

```text
python -m unittest -v test_native_entry
SOTY_NATIVE_TEST_PYTHON=<python-with-pinned-httpx> node --test modules/apps/test/native-entry.test.mjs
```
