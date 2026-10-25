import asyncio
import copy
import json
import sqlite3
import unittest
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
        with self.assertRaises(NativeEntryError):
            self.guard.assert_current(copy.copy(lease))
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


if __name__ == '__main__':
    unittest.main()
