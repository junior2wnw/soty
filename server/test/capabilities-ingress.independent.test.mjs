import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { connect } from 'node:net';
import { fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { setTimeout as pause } from 'node:timers/promises';
import { createNativeIngress, parseNativeDraftJson } from '../capabilities-ingress.js';

const draft = { title: 'Exact input', body: 'е\u0301 / 雪 / 👩🏽‍💻', idempotencyKey: 'independent-request' };
const encoded = JSON.stringify(draft);

// This child receives real chunked HTTP. It observes its own process heap,
// never private ingress fields or Buffer allocation calls.
if (process.argv[2] === 'independent-memory-child') {
  const ingress = createNativeIngress({ limits: { bodyTimeoutMs: 10000 } });
  let bytes = 0, baseline;
  const server = createServer(async (req, res) => {
    req.on('data', chunk => {
      bytes += chunk.length;
      if (bytes === 65536) process.send({ phase: 'body-held', bytes });
    });
    let lease;
    try { lease = ingress.enter(req); await lease.read(req); res.writeHead(200).end(); }
    catch { if (!res.destroyed) res.writeHead(400, { connection: 'close' }).end(); }
    finally { lease?.release(); }
  });
  const collect = async () => {
    for (let i = 0; i < 3; i++) { global.gc(); await new Promise(resolve => setImmediate(resolve)); }
    return process.memoryUsage().heapUsed;
  };
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  baseline = await collect();
  process.on('message', async message => {
    if (message === 'measure') {
      const retained = await collect();
      process.send({ phase: 'measured', bytes, retainedHeapBytes: retained - baseline });
    } else if (message === 'finish') {
      server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); process.disconnect();
    }
  });
  process.send({ phase: 'ready', port: server.address().port });
} else {
  async function until(predicate, label) {
    for (let i = 0; i < 200; i++) { if (predicate()) return; await pause(5); }
    assert.fail(label);
  }
  function ipc(child, phase) {
    return new Promise((resolve, reject) => {
      const finish = (error, value) => { clearTimeout(timer); child.off('message', message); child.off('error', failure); child.off('exit', exited); error ? reject(error) : resolve(value); };
      const message = value => { if (value.phase === phase) finish(null, value); };
      const failure = error => finish(error);
      const exited = () => finish(new Error('ingress_probe_ended_before_' + phase));
      const timer = setTimeout(() => finish(new Error('ingress_probe_timeout_' + phase)), 6000);
      child.on('message', message); child.once('error', failure); child.once('exit', exited);
    });
  }
  async function harness(t, limits = {}) {
    const ingress = createNativeIngress({ limits }), state = { admitted: 0, completed: 0, accepted: 0 };
    const server = createServer(async (req, res) => {
      let lease;
      try {
        lease = ingress.enter(req); state.admitted++;
        await lease.read(req); state.accepted++;
        res.writeHead(204).end();
      } catch (error) {
        const status = { ingress_capacity: 429, ingress_rate_limit: 429, request_timeout: 408, payload_too_large: 413, unsupported_media_type: 415, unsupported_encoding: 415 }[error.code] || 400;
        if (!res.destroyed) res.writeHead(status, { connection: 'close', 'content-type': 'application/json' }).end(JSON.stringify({ code: error.code }));
      } finally { lease?.release(); state.completed++; }
    });
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    t.after(async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); });
    function client({ body, end = true, headers = {} } = {}) {
      let req;
      const result = new Promise(resolve => {
        req = request({ hostname: '127.0.0.1', port: server.address().port, method: 'POST', path: '/',
          headers: { 'content-type': 'application/json', ...headers } }, res => {
          const parts = [];
          res.on('data', part => parts.push(part));
          res.on('end', () => resolve({ status: res.statusCode, text: Buffer.concat(parts).toString('utf8') }));
          res.on('error', () => resolve({ disconnected: true }));
        });
        req.on('error', () => resolve({ disconnected: true }));
        if (body !== undefined) req.write(body);
        if (end) req.end(); else req.flushHeaders();
      });
      return { req, result };
    }
    return { state, client, port: server.address().port };
  }
  function raw(port, bytes) {
    return new Promise((resolve, reject) => {
      const socket = connect(port, '127.0.0.1'), parts = [];
      socket.setTimeout(2500, () => socket.destroy(new Error('raw_ingress_timeout')));
      socket.on('data', chunk => parts.push(chunk));
      socket.once('error', reject);
      socket.once('close', () => resolve(Buffer.concat(parts).toString('utf8')));
      socket.once('connect', () => socket.end(bytes));
    });
  }

  test('independent HTTP tiny-chunk upload retains bounded heap while a valid 64 KiB body is pending', async t => {
    const environment = { PATH: process.env.PATH || '', SystemRoot: process.env.SystemRoot || '', TEMP: process.env.TEMP || '', TMP: process.env.TMP || '' };
    const child = fork(fileURLToPath(import.meta.url), ['independent-memory-child'], {
      execPath: process.execPath, execArgv: ['--expose-gc', '--max-old-space-size=64'], env: environment,
      stdio: ['ignore', 'ignore', 'ignore', 'ipc'], windowsHide: true,
    });
    let socket;
    t.after(async () => {
      socket?.destroy();
      if (child.exitCode === null && child.signalCode === null) {
        const exited = new Promise(resolve => child.once('exit', resolve));
        child.kill(); await exited;
      }
    });
    const ready = await ipc(child, 'ready');
    const base = JSON.stringify({ title: 'memory probe', body: '', idempotencyKey: 'synthetic-request' });
    const body = Buffer.from(JSON.stringify({ title: 'memory probe', body: 'x'.repeat(65536 - Buffer.byteLength(base)), idempotencyKey: 'synthetic-request' }));
    assert.equal(body.length, 65536);
    const chunks = Buffer.alloc(body.length * 6);
    for (let i = 0; i < body.length; i++) { chunks.write('1\r\n', i * 6); chunks[i * 6 + 3] = body[i]; chunks.write('\r\n', i * 6 + 4); }
    const held = ipc(child, 'body-held');
    socket = connect(ready.port, '127.0.0.1'); socket.on('error', () => {});
    await new Promise(resolve => socket.once('connect', resolve));
    socket.write('POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n');
    socket.write(chunks); await held;
    const measured = ipc(child, 'measured'); child.send('measure');
    const report = await measured;
    t.diagnostic(JSON.stringify({ bodyBytes: report.bytes, retainedHeapBytes: report.retainedHeapBytes }));
    assert.equal(report.bytes, 65536);
    assert.ok(report.retainedHeapBytes < 2 * 1024 * 1024,
      `64 KiB body retained ${report.retainedHeapBytes} heap bytes; ingress must coalesce data instead of retaining a Buffer per HTTP chunk`);
    socket.end('0\r\n\r\n'); child.send('finish');
  });

  test('independent abort and double release free one reader without letting a third body bypass the limit', async t => {
    const f = await harness(t, { readers: 2, readersPerPeer: 2, bodyTimeoutMs: 1500 });
    const first = f.client({ body: '{', end: false }), second = f.client({ body: '{', end: false });
    await until(() => f.state.admitted === 2, 'both requests enter');
    assert.equal((await f.client({ body: encoded }).result).status, 429);
    first.req.destroy(); await first.result;
    await until(() => f.state.completed >= 2, 'refused request and aborted reader settle');
    const replacement = f.client({ body: '{', end: false });
    await until(() => f.state.admitted === 3, 'one replacement enters');
    assert.equal((await f.client({ body: encoded }).result).status, 429, 'the still-live reader retains its slot');
    replacement.req.destroy(); second.req.destroy(); await Promise.all([replacement.result, second.result]);
    await until(() => f.state.completed >= 5, 'both remaining reads settle');
    assert.equal((await f.client({ body: encoded }).result).status, 204);
    assert.equal(f.state.accepted, 1);
  });

  test('independent raw byte boundary differs from decoded string size and rejects duplicate actual headers', async t => {
    const bytes = Buffer.from(encoded), f = await harness(t, { bodyBytes: bytes.length });
    assert.equal((await f.client({ body: bytes }).result).status, 204);
    const invalids = [Buffer.concat([bytes, Buffer.from(' ')]), Buffer.from(encoded.replace('Exact input', '\\u0045xact input'))];
    for (const body of invalids) {
      assert.equal((await f.client({ body }).result).status, 413);
      assert.equal((await f.client({ body: bytes }).result).status, 204);
    }
    const response = await raw(f.port, Buffer.concat([Buffer.from('POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\ncOnTeNt-TyPe: application/json\r\nContent-Length: ' + bytes.length + '\r\nConnection: close\r\n\r\n'), bytes]));
    assert.match(response, /^HTTP\/1\.1 400 /u);
    assert.match(response, /"code":"invalid_input"/u);
    assert.ok(!response.includes(draft.body));
    assert.equal((await f.client({ body: bytes }).result).status, 204);
  });

  test('independent socket peer rate is not reset by forwarded headers or invalid body retries', async t => {
    const f = await harness(t, { attempts: 2 });
    assert.equal((await f.client({ body: '{', headers: { 'x-forwarded-for': '192.0.2.1' } }).result).status, 400);
    assert.equal((await f.client({ body: encoded, headers: { forwarded: 'for=192.0.2.2' } }).result).status, 204);
    const exhausted = await f.client({ body: encoded, headers: { 'x-forwarded-for': '192.0.2.3', 'x-real-ip': '192.0.2.4' } }).result;
    assert.equal(exhausted.status, 429);
    assert.deepEqual(JSON.parse(exhausted.text), { code: 'ingress_rate_limit' });
    assert.equal(f.state.accepted, 1);
  });

  test('independent parser handles decoded-key identity and rejects malformed UTF-8 before any accepted request', async t => {
    for (const text of [
      '{"title":"x","body":"y","idempotencyKey":"a","idempotency\\u004bey":"b"}',
      '{"title":"x","b\\u006fdy":"y","body":"z","idempotencyKey":"a"}',
      '{"title":"x","body":"y","idempotencyKey":"a"}\u00a0',
      '{"title":"x","body":true,"idempotencyKey":"a"}',
      '{"title":"x","body":"y","idempotencyKey":"a"}{}',
    ]) assert.throws(() => parseNativeDraftJson(text), error => error.code === 'invalid_input');
    const escaped = '{"idempotency\\u004bey":"a","body":"\\u0065\\u0301","title":"\\uD83D\\uDE80"}';
    assert.deepEqual({ ...parseNativeDraftJson(escaped) }, JSON.parse(escaped));
    const f = await harness(t);
    for (const invalid of [[0xed, 0xa0, 0x80], [0xc0, 0xaf], [0xe2, 0x82], [0xf4, 0x90, 0x80, 0x80]]) {
      const body = Buffer.concat([Buffer.from('{"title":"x","body":"'), Buffer.from(invalid), Buffer.from('","idempotencyKey":"a"}')]);
      const result = await f.client({ body }).result;
      assert.equal(result.status, 400); assert.deepEqual(JSON.parse(result.text), { code: 'invalid_input' });
    }
    assert.equal(f.state.accepted, 0);
    assert.equal((await f.client({ body: escaped }).result).status, 204);
  });
}
