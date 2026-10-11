import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { createGlmProvider } from '../index.mjs';
import { chunk, finish, wire, request, deferred } from './fixture.mjs';

// The Gateway sends DONE before its async attempt observation and HTTP EOF.
// A close during that gap aborts its pipeline before the successful receipt.
async function fixture(t, { trailer = '', timeoutMs = 3000 } = {}) {
  const gate = deferred(), sent = deferred(), receipt = deferred();
  let cancellations = 0;
  const server = http.createServer(async (req, res) => {
    req.resume();
    const controller = new AbortController();
    res.once('close', () => { if (!res.writableFinished) controller.abort(); });
    let pulls = 0;
    const stream = new ReadableStream({
      async pull(output) {
        if (pulls++ === 0) {
          output.enqueue(new TextEncoder().encode(wire([
            chunk({ content: 'Synthetic answer' }),
            finish('stop', { usage: { prompt_tokens: 10, completion_tokens: 2, total_tokens: 12 } }),
          ])));
          return;
        }
        sent.resolve();
        await gate.promise;
        if (controller.signal.aborted) return;
        if (trailer) output.enqueue(new TextEncoder().encode(trailer));
        output.close();
      },
      cancel() { cancellations++; },
    });
    res.setHeader('content-type', 'text/event-stream');
    try {
      await pipeline(Readable.fromWeb(stream), res, { signal: controller.signal });
      receipt.resolve({ status: 'completed' });
    } catch (error) { receipt.resolve({ status: 'failed', code: error.code }); }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const provider = createGlmProvider({ policy: { timeoutMs },
    transport: packet => fetch(`http://127.0.0.1:${server.address().port}/synthetic`, { signal: packet.signal }) });
  t.after(async () => {
    gate.resolve(); provider.close(); server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  });
  return { provider, gate, sent, receipt, cancellations: () => cancellations };
}

test('successful DONE waits for HTTP EOF without cancelling the accounted stream', { timeout: 5000 }, async t => {
  const f = await fixture(t);
  let settled = false;
  const pending = f.provider.complete(request()).finally(() => { settled = true; });
  await f.sent.promise;
  await new Promise(resolve => setTimeout(resolve, 50));
  assert.equal(settled, false, 'DONE is not HTTP EOF; Gateway still owns settlement');
  f.gate.resolve();
  const result = await pending;
  assert.equal(result.message.content, 'Synthetic answer');
  assert.equal(result.usage.status, 'reported');
  assert.deepEqual(await f.receipt.promise, { status: 'completed' });
  assert.equal(f.cancellations(), 0);
});

test('data in a later HTTP chunk after DONE is still rejected', { timeout: 5000 }, async t => {
  const f = await fixture(t, { trailer: `data: ${JSON.stringify(chunk({ content: 'Late data' }))}\n\n` });
  const pending = f.provider.complete(request());
  const rejected = assert.rejects(pending, { code: 'provider_data_after_done' });
  void rejected.catch(() => {});
  await f.sent.promise;
  await new Promise(resolve => setTimeout(resolve, 50));
  f.gate.resolve(); await rejected;
});

for (const mode of ['timeout', 'cancel']) {
  test(`${mode} after DONE still aborts a stalled HTTP body`, { timeout: 5000 }, async t => {
    const f = await fixture(t, { timeoutMs: mode === 'timeout' ? 150 : 3000 });
    const controller = new AbortController();
    const pending = f.provider.complete(request({ signal: controller.signal }));
    const rejected = assert.rejects(pending, { code: mode === 'timeout' ? 'provider_timeout' : 'provider_cancelled' });
    void rejected.catch(() => {});
    await f.sent.promise;
    if (mode === 'cancel') { await new Promise(resolve => setTimeout(resolve, 50)); controller.abort(); }
    await rejected;
    assert.deepEqual(await f.receipt.promise, { status: 'failed', code: 'ABORT_ERR' });
    assert.equal(f.cancellations(), 1);
  });
}
