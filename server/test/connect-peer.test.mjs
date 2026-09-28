import test from 'node:test';
import assert from 'node:assert/strict';
import { Agent, createServer, request } from 'node:http';
import express from 'express';
import { createConnectHandler } from '../../modules/connect/server/http.mjs';
import { createConnectService, digestArgs } from '../../modules/connect/server/index.mjs';

const origin = 'https://soty.test';
const secondOrigin = 'https://other.soty.test';
const challenge = () => ({ protocol: 1, op: 'challenge', args: { operation: 'status', digest: digestArgs({}) } });

async function fixture(t, { framework = false, trustProxy } = {}) {
  let timestamp = 1_800_000_000_000;
  const service = createConnectService({ databasePath: ':memory:', projectId: 'peer-test',
    allowedOrigins: [origin, secondOrigin], clock: () => timestamp });
  const handler = createConnectHandler(service);
  let listener = handler;
  if (framework) {
    const app = express();
    if (trustProxy !== undefined) app.set('trust proxy', trustProxy);
    app.use(handler);
    listener = app;
  }
  const server = createServer(listener);
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const agent = new Agent({ keepAlive: true, maxSockets: 1 });
  t.after(async () => {
    agent.destroy(); server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    service.close();
  });
  return {
    advance(ms) { timestamp += ms; },
    post(body = challenge(), { localAddress = '127.0.0.1', headers = {} } = {}) {
      return new Promise((resolve, reject) => {
        const text = JSON.stringify(body);
        const req = request({ hostname: '127.0.0.1', port: server.address().port, path: '/api/connect/rpc',
          method: 'POST', localAddress, agent, headers: {
            'content-type': 'application/json', 'content-length': Buffer.byteLength(text), origin, ...headers,
          } }, res => {
          const chunks = [];
          res.on('error', reject); res.on('data', chunk => chunks.push(chunk));
          res.on('end', () => {
            try { resolve({ status: res.statusCode, body: JSON.parse(Buffer.concat(chunks).toString('utf8')) }); }
            catch (error) { reject(error); }
          });
        });
        req.setTimeout(5_000, () => req.destroy(new Error('HTTP peer test timed out')));
        req.on('error', reject); req.end(text);
      });
    },
  };
}

async function exhaustChallenges(f, options) {
  for (let count = 0; count < 1200; count += 1) {
    const response = await f.post(challenge(), options);
    assert.equal(response.status, 200, `challenge ${count + 1}: ${response.body.error?.code}`);
  }
}
function limited(response) {
  assert.equal(response.status, 400);
  assert.equal(response.body.error?.code, 'rate_limited');
}

for (const framework of [false, true]) {
  test(`${framework ? 'Express default' : 'neutral HTTP'} cannot replace socket peers with RPC fields or forwarding headers`, async t => {
    const f = await fixture(t, { framework });
    await exhaustChallenges(f);
    limited(await f.post());
    limited(await f.post({ ...challenge(), peer: '198.51.100.91' }));
    limited(await f.post(challenge(), { headers: { 'x-forwarded-for': '198.51.100.92',
      forwarded: 'for=198.51.100.93', 'x-real-ip': '198.51.100.94', origin: secondOrigin } }));

    // Both clients use the same allowed browser origin; only their real socket
    // addresses differ. A busy client must not exhaust everybody on that site.
    assert.equal((await f.post(challenge(), { localAddress: '127.0.0.2' })).status, 200);
    limited(await f.post(challenge(), { headers: { 'x-forwarded-for': '127.0.0.2' } }));
    f.advance(60_000);
    assert.equal((await f.post()).status, 200);
  });
}

test('an explicit proxy allowlist uses the nearest untrusted hop and ignores headers from untrusted sockets', async t => {
  const f = await fixture(t, { framework: true, trustProxy: ['127.0.0.1/32'] });
  const firstClient = { headers: { 'x-forwarded-for': '203.0.113.10, 198.51.100.20' } };
  await exhaustChallenges(f, firstClient);
  limited(await f.post(challenge(), firstClient));
  limited(await f.post({ ...challenge(), peer: '198.51.100.30' }, {
    headers: { 'x-forwarded-for': '203.0.113.99, 198.51.100.20', forwarded: 'for=198.51.100.31' },
  }));
  assert.equal((await f.post(challenge(), { headers: { 'x-forwarded-for': '198.51.100.21' } })).status, 200);

  const untrustedSocket = { localAddress: '127.0.0.2', headers: { 'x-forwarded-for': '198.51.100.20' } };
  await exhaustChallenges(f, untrustedSocket);
  limited(await f.post(challenge(), { ...untrustedSocket, headers: { 'x-forwarded-for': '198.51.100.22' } }));
  assert.equal((await f.post(challenge(), { headers: { 'x-forwarded-for': '198.51.100.21' } })).status, 200);
});
