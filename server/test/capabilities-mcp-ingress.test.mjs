import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request } from 'node:http';
import { createMcpIngress, parseMcpJson, MCP_LIMITS } from '../capabilities-mcp-ingress.js';

test('JSON guard rejects decoded duplicate keys, batch/response envelopes and bounded nested work', () => {
  const valid = '{"jsonrpc":"2.0","id":"x","method":"tools/call","params":{"name":"notes_create_draft","arguments":{"title":"😀","body":"é"}}}';
  assert.deepEqual(parseMcpJson(valid), JSON.parse(valid));
  for (const value of ['{"jsonrpc":"2.0","method":"a","m\\u0065thod":"b"}',
    '{"jsonrpc":"2.0","method":"a","params":{"x":1,"\\u0078":2}}', '[{}]',
    '{"jsonrpc":"2.0","id":1,"result":{}}', '{"jsonrpc":"2.0","id":null,"method":"a"}',
    '{"jsonrpc":"2.0","method":"a","params":' + '['.repeat(21) + '0' + ']'.repeat(21) + '}']) {
    assert.throws(() => parseMcpJson(value), error => ['invalid_input', 'payload_too_large'].includes(error.code));
  }
  assert.throws(() => parseMcpJson(JSON.stringify({ jsonrpc: '2.0', method: 'a', params: Array(10001).fill(0) })), { code: 'payload_too_large' });
  assert.throws(() => parseMcpJson(JSON.stringify({ jsonrpc: '2.0', method: 'a', params: { _meta: { x: '中'.repeat(6000) } } })), { code: 'payload_too_large' });
  assert.throws(() => parseMcpJson(JSON.stringify({ jsonrpc: '2.0', method: 'a', id: 'x'.repeat(161) })), { code: 'invalid_input' });
});

async function fixture(t, limits, response) {
  const ingress = createMcpIngress(limits), tasks = new Set();
  const server = createServer((req, res) => {
    const task = (async () => {
      let lease;
      try {
        lease = ingress.enter(req, res); const { body } = await lease.read();
        const bytes = await lease.collect(response ? response(body) : Response.json({ admitted: true }));
        const done = lease.completion(); lease.current(); res.end(bytes); await done;
      } catch (error) {
        if (lease?.signal.aborted || res.destroyed) { res.destroy(); return; }
        res.statusCode = error.code === 'request_timeout' ? 408 : error.code === 'payload_too_large' ? 413
          : ['ingress_capacity', 'ingress_rate_limit'].includes(error.code) ? 429 : 400;
        res.setHeader('Connection', 'close'); res.end(JSON.stringify({ code: error.code }));
      } finally { lease?.stop(); lease?.release(); }
    })(); tasks.add(task); void task.finally(() => tasks.delete(task));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => { ingress.close(); server.closeAllConnections(); await Promise.allSettled([...tasks]); await new Promise(resolve => server.close(resolve)); });
  const origin = `http://127.0.0.1:${server.address().port}`;
  const send = (raw, { chunks = false, incomplete = false, headers = {} } = {}) => new Promise((resolve, reject) => {
    const req = request(origin, { method: 'POST', headers: { 'content-type': 'application/json', ...headers } }, res => {
      let text = ''; res.on('data', chunk => { text += chunk; });
      res.once('end', () => { req.destroy(); resolve({ status: res.statusCode, text }); });
      res.once('error', reject);
    });
    req.once('error', reject);
    if (chunks) { for (let index = 0; index < raw.length; index++) req.write(raw.subarray(index, index + 1)); }
    else req.write(raw);
    if (!incomplete) req.end();
  });
  return { send, origin, ingress };
}

test('real chunked input has the same exact bound, fatal decoding and absolute body deadline', async t => {
  const f = await fixture(t, { bodyBytes: 1024, bodyTimeoutMs: 100 });
  const base = Buffer.from('{"jsonrpc":"2.0","method":"ping"}');
  const raw = Buffer.concat([base, Buffer.alloc(1024 - base.length, 32)]);
  assert.equal((await f.send(raw, { chunks: true })).status, 200);
  assert.equal((await f.send(Buffer.concat([raw, Buffer.from(' ')]), { chunks: true })).status, 413);
  assert.equal((await f.send(Buffer.from([0xef, 0xbb, 0xbf, ...base]))).status, 400);
  assert.equal((await f.send(Buffer.from([0xc0, 0xaf]))).status, 400);
  assert.equal((await f.send(base, { headers: { 'content-encoding': 'gzip' } })).status, 400);
  const timed = await f.send(Buffer.from('{'), { incomplete: true }); assert.equal(timed.status, 408);
  assert.equal((await f.send(base)).status, 200, 'reader slot recovered after deadline');
});

test('response collector cancels oversized or stalled streams and releases the reader on every exit', async t => {
  let cancelled = 0;
  const f = await fixture(t, { outputBytes: 32, responseTimeoutMs: 100 }, body => {
    if (body.method === 'large') return new Response(new ReadableStream({ start(controller) { controller.enqueue(new Uint8Array(33)); }, cancel() { cancelled++; } }));
    if (body.method === 'stall') return new Response(new ReadableStream({ cancel() { cancelled++; } }));
    return Response.json({ ok: true });
  });
  for (const method of ['large', 'stall']) await assert.rejects(f.send(Buffer.from(JSON.stringify({ jsonrpc: '2.0', method }))));
  assert.equal(cancelled, 2);
  assert.equal((await f.send(Buffer.from('{"jsonrpc":"2.0","method":"ping"}'))).status, 200);
});

test('trusted bounds cannot grow and socket-peer attempt limits ignore forwarded addresses', async t => {
  for (const limits of [{ readers: 9 }, { responseTimeoutMs: Infinity }, { unknown: 1 }, { bodyBytes: 0 }])
    assert.throws(() => createMcpIngress(limits), { code: 'ingress_configuration_invalid' });
  assert.equal(MCP_LIMITS.bodyBytes, 2097152); assert.equal(MCP_LIMITS.outputBytes, 2097152);
  const f = await fixture(t, { attempts: 2 });
  const body = Buffer.from('{"jsonrpc":"2.0","method":"ping"}');
  assert.equal((await f.send(body)).status, 200); assert.equal((await f.send(body)).status, 200);
  assert.equal((await f.send(body, { headers: { 'X-Forwarded-For': '192.0.2.1' } })).status, 429);
});
