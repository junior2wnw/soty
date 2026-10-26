"""Optional Python binding for the fixed, public Native app-policy wire.

Requires httpx. This never supplies Human identity, a UID, Native ACL or CSRF.
Use the same operator pins as the Node SDK; no caller/header/JSON lease authority.
"""
import asyncio
import contextvars
import inspect
import json
import re
import time
import weakref
from http.cookiejar import CookieJar, DefaultCookiePolicy
from urllib.parse import urlsplit

import httpx


_NATIVE_ENTRY_HTML = """<!doctype html>
<html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Не удалось открыть приложение</title>
<style>html{color-scheme:light dark}body{margin:0;min-height:100vh;display:grid;place-items:center;background:#f3f5f7;color:#17212b;font:16px/1.55 system-ui,sans-serif}main{box-sizing:border-box;width:min(100%,480px);padding:28px}h1{font-size:clamp(24px,5vw,32px);line-height:1.2;margin:0 0 16px}p{margin:0 0 24px}a{display:inline-block;padding:12px 18px;border-radius:12px;background:#145da0;color:#fff;text-decoration:none;font-weight:600}a:focus-visible{outline:3px solid currentColor;outline-offset:4px}@media(prefers-color-scheme:dark){body{background:#161b22;color:#f0f4f8}}</style>
<main><h1>Не удалось открыть приложение</h1><p>Сейчас не получилось проверить доступ. Начните открытие заново. Если до этого выполняли действие, сначала проверьте его результат.</p><a href="/" rel="noreferrer">Открыть приложение заново</a></main></html>""".encode('utf-8')
_ERROR_VARY = b'Accept, Sec-Fetch-Mode, Sec-Fetch-Dest'


def _document_navigation(scope):
    if scope.get('method') != 'GET':
        return False
    headers = {name.lower(): value.lower() for name, value in scope.get('headers', ())}
    if headers.get(b'sec-fetch-mode', b'navigate') != b'navigate' or headers.get(b'sec-fetch-dest', b'document') not in (b'document', b'iframe', b'frame'):
        return False
    accept = b','.join(value.lower() for name, value in scope.get('headers', ()) if name.lower() == b'accept')
    if len(accept) > 4096:
        return False
    html = False
    for item in accept.split(b','):
        kind, *parameters = item.strip().split(b';')
        quality = 1.0
        for parameter in parameters:
            key, _, value = parameter.strip().partition(b'=')
            if key == b'q':
                try:
                    quality = float(value)
                except ValueError:
                    quality = 0
        if not 0 < quality <= 1:
            continue
        if kind == b'application/json':
            return False
        html |= kind in (b'text/html', b'application/xhtml+xml')
    return html


async def _send_entry_failure(scope, send, status, data):
    document = _document_navigation(scope)
    body = _NATIVE_ENTRY_HTML if document else data
    headers = [(b'content-type', b'text/html; charset=utf-8' if document else b'application/json'),
               (b'cache-control', b'no-store'), (b'x-content-type-options', b'nosniff'), (b'vary', _ERROR_VARY)]
    if document:
        headers.append((b'referrer-policy', b'no-referrer'))
        headers.append((b'content-security-policy', b"default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'"))
    await send({'type': 'http.response.start', 'status': status, 'headers': headers})
    await send({'type': 'http.response.body', 'body': body})


class NativeEntryError(Exception):
    def __init__(self, code, status=403):
        super().__init__(code)
        self.code, self.status = code, status


def _need(value, code='native_entry_configuration_invalid', status=503):
    if not value:
        raise NativeEntryError(code, status)


def _positive(value):
    return type(value) is int and 1 <= value <= 9007199254740991


def _hash(value):
    return isinstance(value, str) and re.fullmatch('[a-f0-9]{64}', value) is not None


def _unique_pairs(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('duplicate policy field')
        value[key] = item
    return value


class _Lease:
    __slots__ = ('_policy_epoch', '_admission_key_hash', '__weakref__')

    def __init__(self, epoch, admission):
        object.__setattr__(self, '_policy_epoch', epoch)
        object.__setattr__(self, '_admission_key_hash', admission)

    @property
    def policy_epoch(self):
        return self._policy_epoch

    @property
    def admission_key_hash(self):
        return self._admission_key_hash

    def __setattr__(self, _name, _value):
        raise AttributeError('immutable Native policy lease')


_current_boundary = contextvars.ContextVar('soty_native_entry_boundary', default=None)


def assert_native_entry_current():
    """Final synchronous Native boundary. Internal background jobs have no
    request context; they retain their independent existing authorization."""
    boundary = _current_boundary.get()
    if boundary is not None:
        boundary.assert_current()


async def refresh_native_entry():
    """Refresh outside a Native SQL transaction, immediately before new work."""
    boundary = _current_boundary.get()
    if boundary is not None:
        await boundary.before_effect()


class NativeEntryGuard:
    def __init__(self, configuration):
        _need(type(configuration) is dict and set(configuration) <= {
            'rootCheckUrl', 'appId', 'origin', 'primaryTargetDigest', 'nativeTargetDigest',
            'timeoutMs', 'heartbeatMs', 'leaseMs', 'maxStreams', 'maxRequests'})
        self.config = dict(configuration)
        try:
            self.endpoint, native = urlsplit(configuration['rootCheckUrl']), urlsplit(configuration['origin'])
        except (KeyError, ValueError):
            raise NativeEntryError('native_entry_configuration_invalid', 503) from None
        _need(native.scheme == 'https' and native.hostname and not native.username and not native.password
              and configuration['origin'] == 'https://' + native.netloc and not native.query and not native.fragment
              and not self.endpoint.username and not self.endpoint.password and not self.endpoint.query
              and not self.endpoint.fragment and self.endpoint.path == '/_soty/ingress-check'
              and (self.endpoint.scheme == 'https' or self.endpoint.scheme == 'http'
                   and self.endpoint.hostname in ('127.0.0.1', '::1')))
        _need(re.fullmatch('app-[a-f0-9]{32}', configuration.get('appId', '')) is not None
              and _hash(configuration.get('primaryTargetDigest')) and _hash(configuration.get('nativeTargetDigest')))
        self.timeout = self._limit('timeoutMs', 750, 1000) / 1000
        self.heartbeat = self._limit('heartbeatMs', 250, 250) / 1000
        self.duration = self._limit('leaseMs', 1000, 1000) / 1000
        self.max_streams = self._limit('maxStreams', 128, 128)
        self.max_requests = self._limit('maxRequests', 128, 128)
        _need(self.heartbeat < self.duration)
        self._client = httpx.AsyncClient(follow_redirects=False, trust_env=False, timeout=self.timeout,
            cookies=CookieJar(policy=DefaultCookiePolicy(allowed_domains=[])),
            limits=httpx.Limits(max_connections=self.max_requests, max_keepalive_connections=min(16, self.max_requests)))
        self._headers = {'Host': native.netloc, 'Accept': 'application/json', 'Accept-Encoding': 'identity',
            'x-soty-ingress-surface': 'native', 'x-soty-ingress-app': configuration['appId'],
            'x-soty-ingress-primary': configuration['primaryTargetDigest'],
            'x-soty-ingress-target': configuration['nativeTargetDigest']}
        self._leases, self._streams, self._requests = weakref.WeakKeyDictionary(), set(), 0
        self._closed = False

    def _limit(self, key, default, maximum):
        value = self.config.get(key, default)
        _need(type(value) is int and 1 <= value <= maximum)
        return value

    def assert_current(self, lease):
        state = self._leases.get(lease) if isinstance(lease, _Lease) else None
        now = time.monotonic()
        _need(not self._closed and state is not None and state[0] <= now < state[1], 'native_entry_lease_closed', 403)
        return lease

    async def acquire(self):
        _need(not self._closed, 'native_entry_closed', 403)
        _need(self._requests < self.max_requests, 'native_entry_request_capacity', 503)
        self._requests += 1
        started = time.monotonic()
        try:
            try:
                async with asyncio.timeout(self.timeout):
                    async with self._client.stream('GET', self.config['rootCheckUrl'], headers=self._headers) as response:
                        _need(response.status_code == 200 and response.headers.get('content-type', '').lower().split(';')[0] == 'application/json',
                              'native_entry_policy_denied', 403)
                        data = bytearray()
                        async for part in response.aiter_bytes():
                            data.extend(part)
                            _need(len(data) <= 4096, 'native_entry_policy_invalid', 503)
            except NativeEntryError:
                raise
            except (TimeoutError, httpx.HTTPError):
                raise NativeEntryError('native_entry_policy_unavailable', 503) from None
            try:
                body = json.loads(data.decode('utf-8'), object_pairs_hook=_unique_pairs)
            except (ValueError, UnicodeError):
                raise NativeEntryError('native_entry_policy_invalid', 503) from None
            _need(type(body) is dict and set(body) == {'schema','appId','origin','primaryTargetRevision','primaryTargetDigest',
                'nativeTargetRevision','nativeTargetDigest','policyEpoch','admissionKeyHash','leaseMs'}
                and body['schema'] == 'soty.native-entry-policy.v1'
                and all(body[key] == self.config[key] for key in ('appId','origin','primaryTargetDigest','nativeTargetDigest'))
                and all(_positive(body[key]) for key in ('primaryTargetRevision','nativeTargetRevision','policyEpoch','leaseMs'))
                and body['leaseMs'] <= 1000 and _hash(body['admissionKeyHash']), 'native_entry_policy_invalid', 503)
            lease = _Lease(body['policyEpoch'], body['admissionKeyHash'])
            self._leases[lease] = (started, started + min(self.duration, body['leaseMs'] / 1000))
            return self.assert_current(lease)
        finally:
            self._requests -= 1

    async def open_stream(self, on_close):
        _need(callable(on_close) and not self._closed, 'native_entry_closed', 403)
        _need(len(self._streams) < self.max_streams, 'native_entry_stream_capacity', 503)
        stream = NativeEntryStream(self, on_close)
        self._streams.add(stream)  # Capacity is reserved before Root I/O.
        try:
            await stream.before_effect()
            stream._heartbeat = asyncio.create_task(stream._watch())
            return stream
        except BaseException:
            stream.close()
            raise

    async def close(self):
        self._closed = True
        for stream in tuple(self._streams):
            stream.close('native_entry_guard_closed')
        await self._client.aclose()


class NativeEntryStream:
    def __init__(self, guard, on_close):
        self._guard, self._on_close, self._lease = guard, on_close, None
        self._closed, self._heartbeat, self._expiry = False, None, None
        self._lock = asyncio.Lock()

    def assert_current(self):
        _need(not self._closed, 'native_entry_lease_closed', 403)
        return self._guard.assert_current(self._lease)

    async def before_effect(self):
        async with self._lock:
            try:
                _need(not self._closed, 'native_entry_lease_closed', 403)
                lease = await self._guard.acquire()
                _need(not self._closed, 'native_entry_lease_closed', 403)
                if self._lease is not None:
                    _need(lease.policy_epoch == self._lease.policy_epoch and lease.admission_key_hash == self._lease.admission_key_hash,
                          'native_entry_policy_changed', 403)
                self._lease = lease
                if self._expiry is not None:
                    self._expiry.cancel()
                self._expiry = asyncio.create_task(self._expire(self._guard._leases[lease][1]))
                return lease
            except BaseException as error:
                self.close(getattr(error, 'code', 'native_entry_policy_unavailable'))
                raise

    async def _expire(self, deadline):
        await asyncio.sleep(max(0, deadline - time.monotonic()))
        self.close('native_entry_lease_expired')

    async def _watch(self):
        try:
            while not self._closed:
                await asyncio.sleep(self._guard.heartbeat)
                await self.before_effect()
        except (NativeEntryError, asyncio.CancelledError):
            pass

    def close(self, reason='native_entry_stream_closed'):
        if self._closed:
            return
        self._closed = True
        self._guard._streams.discard(self)
        for task in (self._heartbeat, self._expiry):
            if task is not None and task is not asyncio.current_task():
                task.cancel()
        try:
            result = self._on_close(reason)
            if inspect.isawaitable(result):
                asyncio.create_task(result)
        except Exception:
            pass


class NativeEntryMiddleware:
    """ASGI app-policy gate; bypass paths are CODE-owned server-only ports.
    Native authentication/Origin/CSRF stays in the wrapped application."""
    def __init__(self, app, guard, bypass_paths=()):
        self.app, self.guard, self.bypass = app, guard, frozenset(bypass_paths)

    async def __call__(self, scope, receive, send):
        if scope['type'] not in ('http', 'websocket') or scope.get('path') in self.bypass:
            return await self.app(scope, receive, send)
        is_ws, started, ws_closed = scope['type'] == 'websocket', False, False
        stream, token, boundary = None, None, None
        policy_closed, completed = False, False
        endpoint_task = asyncio.current_task()
        lock = asyncio.Lock()
        async def raw_send(message):
            nonlocal ws_closed
            async with lock:
                if is_ws:
                    if ws_closed:
                        if message['type'] == 'websocket.close':
                            return
                        raise NativeEntryError('native_entry_lease_closed')
                    if message['type'] == 'websocket.close':
                        ws_closed = True
                await send(message)
        async def policy_close(_reason):
            nonlocal policy_closed
            if completed or policy_closed:
                return
            policy_closed = True
            # Stop application work BEFORE a possibly backpressured WebSocket
            # close/handshake. Closing the client cannot become an effect lease.
            if endpoint_task is not asyncio.current_task() and not endpoint_task.done():
                endpoint_task.cancel('native-entry-policy')
            try:
                if is_ws:
                    await asyncio.wait_for(raw_send({'type': 'websocket.close', 'code': 1013, 'reason': 'app_entry_closed'}),
                                           min(self.guard.duration, .25))
            except Exception:
                pass
        try:
            # Long OIDC/read operations renew the SAME policy scope. A revoked
            # policy cancels pending endpoint work; it never substitutes identity.
            stream = await self.guard.open_stream(policy_close)
            boundary = stream
            token = _current_boundary.set(boundary)
            async def checked_receive():
                message = await receive()
                if is_ws and message['type'] == 'websocket.receive':
                    await stream.before_effect()
                return message
            async def checked_send(message):
                nonlocal started
                if is_ws:
                    if message['type'] != 'websocket.close':
                        stream.assert_current()
                elif message['type'] == 'http.response.start':
                    await boundary.before_effect()
                    started = True
                elif message['type'] == 'http.response.body':
                    boundary.assert_current()
                await raw_send(message)
            await self.app(scope, checked_receive, checked_send)
        except asyncio.CancelledError:
            if not policy_closed:
                raise
            endpoint_task.uncancel()
            if not is_ws and not started:
                data = b'{"code":"native_entry_policy_closed"}'
                await _send_entry_failure(scope, raw_send, 503, data)
            elif not is_ws:
                raise
        except NativeEntryError as error:
            if is_ws:
                await policy_close(error.code)
            elif not started:
                data = json.dumps({'code': error.code}).encode()
                await _send_entry_failure(scope, raw_send, error.status, data)
            else:
                raise  # Headers already sent: terminate, never append a private error body.
        finally:
            completed = True
            if token is not None:
                _current_boundary.reset(token)
            if stream is not None:
                stream.close()


class _FencedTransaction:
    def __init__(self, connection, transaction):
        self.connection, self.transaction = connection, transaction

    async def __aenter__(self):
        self.connection._current()
        await self.transaction.__aenter__()
        self.connection.depth += 1
        return self

    async def __aexit__(self, kind, error, traceback):
        try:
            if kind is None:
                try:
                    self.connection._current()
                except BaseException as denied:
                    await self.transaction.__aexit__(type(denied), denied, denied.__traceback__)
                    raise
            return await self.transaction.__aexit__(kind, error, traceback)
        finally:
            self.connection.depth -= 1


class _FencedConnection:
    def __init__(self, connection, owner, boundary):
        self.connection, self.owner, self.depth, self.released = connection, owner, 0, False
        self.boundary = boundary

    def _current(self):
        _need(not self.released, 'native_entry_connection_released', 503)
        if self.boundary is not None:
            self.boundary.assert_current()
        assert_native_entry_current()

    def transaction(self, *args, **kwargs):
        return _FencedTransaction(self, self.connection.transaction(*args, **kwargs))

    async def _query(self, name, *args, **kwargs):
        self._current()
        if not self.depth:
            # Even a legacy autocommit write gets a final fence BEFORE commit.
            async with self.transaction():
                return await self._query(name, *args, **kwargs)
        result = await getattr(self.connection, name)(*args, **kwargs)
        self._current()
        return result

    def __getattr__(self, name):
        if name in ('execute','executemany','fetch','fetchrow','fetchval'):
            async def query(*args, **kwargs):
                return await self._query(name, *args, **kwargs)
            return query
        if name in ('is_closed','is_in_transaction','get_server_pid','get_server_version'):
            return getattr(self.connection, name)
        raise AttributeError('unsupported fenced Native connection method: ' + name)


class _FencedAcquire:
    def __init__(self, pool, *args, **kwargs):
        self.pool, self.arguments, self.keywords, self.connection = pool, args, kwargs, None

    async def _acquire(self):
        _need(self.connection is None, 'native_entry_connection_reused', 503)
        boundary = _current_boundary.get()
        assert_native_entry_current()
        self.connection = _FencedConnection(await self.pool.pool.acquire(*self.arguments, **self.keywords), self.pool, boundary)
        try:
            self.connection._current()
        except BaseException:
            await self.pool.release(self.connection)
            raise
        return self.connection

    def __await__(self):
        return self._acquire().__await__()

    async def __aenter__(self):
        return await self._acquire()

    async def __aexit__(self, _kind, _error, _traceback):
        await self.pool.release(self.connection)


class _FencedPool:
    def __init__(self, pool):
        self.pool = pool

    def acquire(self, *args, **kwargs):
        return _FencedAcquire(self, *args, **kwargs)

    async def release(self, connection, *args, **kwargs):
        _need(isinstance(connection, _FencedConnection) and connection.owner is self and not connection.released,
              'native_entry_connection_released', 503)
        # Invalidate BEFORE waiting for the raw driver's cleanup. A retained
        # wrapper cannot race release or become usable under a fresh context.
        connection.released = True
        await self.pool.release(connection.connection, *args, **kwargs)

    def __getattr__(self, name):
        if name in ('execute','executemany','fetch','fetchrow','fetchval'):
            async def query(*args, **kwargs):
                async with self.acquire() as connection:
                    return await getattr(connection, name)(*args, **kwargs)
            return query
        if name in ('close','terminate','is_closing','get_size','get_idle_size','get_min_size','get_max_size'):
            return getattr(self.pool, name)
        raise AttributeError('unsupported fenced Native pool method: ' + name)


def fence_asyncpg_pool(pool):
    """Optional closed subset of asyncpg's pool API. No Root/network call is
    made while SQL locks are held. Every query/transaction checks the current
    context; autocommit operations get a real transaction before their fence.
    Unsupported cursor/prepare/copy/raw methods fail closed instead of bypassing.
    Existing background jobs have no request context and keep their own policy.
    """
    if isinstance(pool, _FencedPool):
        return pool
    return _FencedPool(pool)
