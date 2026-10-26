import asyncio
import copy
import json
import sqlite3
import unittest
from types import SimpleNamespace
from soty_native_entry import NativeEntryGuard, NativeEntryError, NativeEntryMiddleware, assert_native_entry_current, refresh_native_entry


class NativePolicyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.requests, self.writers = [], set()
        self.mode, self.epoch, self.gate = 'good', 2, None
        self.closed_error = asyncio.Event()
        self.server = await asyncio.start_server(self.root, '127.0.0.1', 0)
        self.options = {'rootCheckUrl': 'http://127.0.0.1:' + str(self.server.sockets[0].getsockname()[1]) + '/_soty/ingress-check',
            'appId': 'app-' + 'a' * 32, 'origin': 'https://native.apps.test', 'primaryTargetDigest': 'b' * 64, 'nativeTargetDigest': 'c' * 64,
            'leaseMs': 200, 'heartbeatMs': 30, 'timeoutMs': 750}
        self.guard = NativeEntryGuard(self.options)

    async def asyncTearDown(self):
        if self.gate is not None:
            self.gate.set()
        await self.guard.close()
        for writer in tuple(self.writers):
            writer.close()
        self.server.close()
        await self.server.wait_closed()
        await asyncio.sleep(0)

    async def root(self, reader, writer):
        self.writers.add(writer)
        try:
            self.requests.append((await reader.readuntil(b'\r\n\r\n')).decode())
            if self.gate is not None:
                await self.gate.wait()
            if self.mode == 'disconnect':
                return
            if self.mode == 'slow':
                await asyncio.sleep(.12)
            if self.mode == 'error':
                writer.write(b'HTTP/1.1 403 Forbidden\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\nnever ending')
                await writer.drain()
                await reader.read()
                self.closed_error.set()
                return
            body = json.dumps({'schema': 'soty.native-entry-policy.v1', 'appId': self.options['appId'], 'origin': self.options['origin'],
                'primaryTargetRevision': 2, 'primaryTargetDigest': self.options['primaryTargetDigest'], 'nativeTargetRevision': 1,
                'nativeTargetDigest': self.options['nativeTargetDigest'], 'policyEpoch': self.epoch,
                'admissionKeyHash': 'd' * 64, 'leaseMs': 1000}).encode()
            writer.write(b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nSet-Cookie: should_not_persist=1; Path=/\r\nConnection: close\r\nContent-Length: ' + str(len(body)).encode() + b'\r\n\r\n' + body)
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            self.writers.discard(writer)

    async def test_real_wire_brand_monotonic_deadline_and_no_ambient_cookie(self):
        lease = await self.guard.acquire()
        self.guard.assert_current(lease)
        with self.assertRaises(AttributeError):
            lease.policy_epoch = 999
        with self.assertRaises(NativeEntryError):
            self.guard.assert_current(SimpleNamespace(policy_epoch=lease.policy_epoch, admission_key_hash=lease.admission_key_hash))
        self.assertNotIn('cookie:', self.requests[0].lower())
        await self.guard.acquire()
        self.assertNotIn('cookie:', self.requests[1].lower())
        self.assertEqual(len(self.guard._client.cookies), 0)
        self.assertIn('Host: native.apps.test', self.requests[0])
        await asyncio.sleep(.21)
        with self.assertRaises(NativeEntryError):
            self.guard.assert_current(lease)

    async def test_capacity_reserved_before_io_and_never_ending_error_socket_closed(self):
        await self.guard.close()
        self.guard = NativeEntryGuard({**self.options, 'maxRequests': 1})
        self.gate = asyncio.Event()
        pending = asyncio.create_task(self.guard.acquire())
        await asyncio.sleep(.02)
        with self.assertRaisesRegex(NativeEntryError, 'native_entry_request_capacity'):
            await self.guard.acquire()
        self.gate.set()
        await pending
        self.assertEqual(len(self.requests), 1)
        self.mode = 'error'
        with self.assertRaisesRegex(NativeEntryError, 'native_entry_policy_denied'):
            await self.guard.acquire()
        await asyncio.wait_for(self.closed_error.wait(), .3)

    async def test_stream_epoch_change_has_active_lifetime_and_commands_cannot_reopen_it(self):
        ended = asyncio.Event()
        stream = await self.guard.open_stream(lambda _reason: ended.set())
        stream.assert_current()
        self.epoch += 1
        await asyncio.wait_for(ended.wait(), .3)
        with self.assertRaises(NativeEntryError):
            await stream.before_effect()

    async def test_asgi_final_native_sql_boundary_rolls_back_revoked_pending_write_before_commit(self):
        await self.guard.acquire()  # Initialize the local HTTP backend before measuring the Native transaction.
        database = sqlite3.connect(':memory:')
        database.execute('CREATE TABLE effects(value INTEGER)')
        messages, entered = [], []
        async def app(_scope, _receive, send):
            await refresh_native_entry()  # Outside the Native transaction.
            with database:
                database.execute('INSERT INTO effects VALUES(1)')
                entered.append(True)
                self.mode = 'error'
                await asyncio.sleep(.21)
                assert_native_entry_current()  # Final synchronous commit boundary.
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'private'})
        async def send(value):
            messages.append(value)
        await NativeEntryMiddleware(app, self.guard)({'type': 'http', 'path': '/private', 'method': 'POST'}, None, send)
        self.assertEqual(database.execute('SELECT count(*) FROM effects').fetchone()[0], 0)
        self.assertEqual(entered, [True])
        self.assertEqual(messages[0]['status'], 503)
        self.assertNotIn(b'private', messages[-1]['body'])
        database.close()

    async def test_asgi_private_http_rechecks_changed_epoch_and_bypass_is_explicit_code_owned(self):
        messages = []
        async def app(_scope, _receive, send):
            self.epoch += 1
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            await send({'type': 'http.response.body', 'body': b'private'})
        async def send(value):
            messages.append(value)
        middleware = NativeEntryMiddleware(app, self.guard, bypass_paths=('/internal-mac-only',))
        await middleware({'type': 'http', 'path': '/private', 'method': 'GET'}, None, send)
        self.assertEqual(messages[0]['status'], 403)
        self.assertNotIn(b'private', messages[-1]['body'])
        messages.clear()
        await middleware({'type': 'http', 'path': '/internal-mac-only', 'method': 'POST'}, None, send)
        self.assertEqual(messages[0]['status'], 200)
        self.assertEqual(len(self.requests), 2)

    async def test_ws_revocation_cancels_work_before_a_backpressured_close_can_finish(self):
        entered, stopped = asyncio.Event(), asyncio.Event()
        async def app(_scope, _receive, send):
            await send({'type': 'websocket.accept'})
            entered.set()
            try:
                await asyncio.sleep(10)
                self.fail('revoked Native work continued')
            finally:
                stopped.set()
        async def send(message):
            if message['type'] == 'websocket.close':
                await asyncio.sleep(10)
        task = asyncio.create_task(NativeEntryMiddleware(app, self.guard)({'type': 'websocket', 'path': '/ws'}, None, send))
        await asyncio.wait_for(entered.wait(), 2)
        self.mode = 'error'
        await asyncio.wait_for(stopped.wait(), .3)
        await asyncio.wait_for(task, .3)
        self.assertEqual(len(self.guard._streams), 0)
        await asyncio.sleep(.21)  # The independently bounded close callback finishes too.


    async def test_document_failure_is_static_503_and_never_replays_callback_query(self):
        self.mode = 'disconnect'
        calls, messages = [], []
        async def app(*_args):
            calls.append(True)
        async def send(message):
            messages.append(message)
        scope = {'type': 'http', 'path': '/auth/callback', 'method': 'GET',
                 'query_string': b'code=PRIVATE_CODE&state=PRIVATE_STATE&access_token=PRIVATE_BEARER',
                 'headers': [(b'accept', b'text/html'), (b'sec-fetch-mode', b'navigate'), (b'sec-fetch-dest', b'document')]}
        await NativeEntryMiddleware(app, self.guard)(scope, None, send)
        self.assertEqual(calls, [])
        self.assertEqual(messages[0]['status'], 503)
        headers = dict(messages[0]['headers'])
        self.assertEqual(headers[b'content-type'], b'text/html; charset=utf-8')
        self.assertEqual(headers[b'cache-control'], b'no-store')
        self.assertEqual(headers[b'referrer-policy'], b'no-referrer')
        self.assertEqual(headers[b'x-content-type-options'], b'nosniff')
        self.assertIn(b'Accept', headers[b'vary'])
        self.assertIn(b"default-src 'none'", headers[b'content-security-policy'])
        self.assertNotIn(b'frame-ancestors', headers[b'content-security-policy'])
        self.assertNotIn(b'x-frame-options', headers)
        body = messages[1]['body']
        self.assertIn(b'href="/" rel="noreferrer"', body)
        for value in (b'PRIVATE_', b'/auth/callback', b'<script', b'<form', b'http-equiv', b'native_entry_policy_unavailable'):
            self.assertNotIn(value, body)
        self.assertNotIn(b'location', headers)
        self.assertNotIn(b'refresh', headers)

    async def test_api_head_write_and_non_navigation_errors_keep_json_and_status(self):
        self.mode = 'disconnect'
        async def app(*_args):
            self.fail('Denied application must never execute')
        for method, headers in [('POST', [(b'accept', b'text/html')]), ('HEAD', [(b'accept', b'text/html')]),
                                ('GET', [(b'accept', b'application/json')]), ('GET', [(b'accept', b'text/html;q=0')]),
                                ('GET', [(b'accept', b'text/html'), (b'sec-fetch-mode', b'cors')]),
                                ('GET', [(b'accept', b'application/json'), (b'accept', b'text/html')])]:
            messages = []
            async def send(message):
                messages.append(message)
            await NativeEntryMiddleware(app, self.guard)({'type': 'http', 'path': '/api/write', 'method': method, 'headers': headers}, None, send)
            self.assertEqual(messages[0]['status'], 503)
            self.assertEqual(dict(messages[0]['headers'])[b'content-type'], b'application/json')
            self.assertEqual(json.loads(messages[1]['body']), {'code': 'native_entry_policy_unavailable'})

    async def test_denied_document_keeps_403_and_never_executes_app(self):
        self.mode = 'error'
        messages = []
        async def app(*_args):
            self.fail('Revoked policy cannot execute app')
        async def send(message):
            messages.append(message)
        await NativeEntryMiddleware(app, self.guard)({'type': 'http', 'path': '/', 'method': 'GET', 'headers': [(b'accept', b'text/html')]}, None, send)
        self.assertEqual(messages[0]['status'], 403)
        self.assertEqual(dict(messages[0]['headers'])[b'content-type'], b'text/html; charset=utf-8')

    async def test_policy_closed_during_document_work_uses_one_public_503_before_headers(self):
        await self.guard.acquire()
        messages, entered = [], []
        async def app(_scope, _receive, _send):
            entered.append(True)
            self.mode = 'error'
            await asyncio.sleep(10)
        async def send(message):
            messages.append(message)
        await asyncio.wait_for(NativeEntryMiddleware(app, self.guard)({'type': 'http', 'path': '/', 'method': 'GET',
            'headers': [(b'accept', b'text/html')]}, None, send), 1)
        self.assertEqual(entered, [True])
        self.assertEqual([m['type'] for m in messages], ['http.response.start', 'http.response.body'])
        self.assertEqual(messages[0]['status'], 503)
        self.assertEqual(dict(messages[0]['headers'])[b'content-type'], b'text/html; charset=utf-8')

    async def test_policy_failure_after_headers_never_appends_a_second_response(self):
        await self.guard.acquire()
        messages = []
        async def app(_scope, _receive, send):
            await send({'type': 'http.response.start', 'status': 200, 'headers': []})
            self.mode = 'error'
            await asyncio.sleep(10)
        async def send(message):
            messages.append(message)
        with self.assertRaises(asyncio.CancelledError):
            await NativeEntryMiddleware(app, self.guard)({'type': 'http', 'path': '/', 'method': 'GET', 'headers': [(b'accept', b'text/html')]}, None, send)
        self.assertEqual([m['type'] for m in messages], ['http.response.start'])


if __name__ == '__main__':
    unittest.main()
