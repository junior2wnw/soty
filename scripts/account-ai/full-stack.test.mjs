import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { generateKeyPairSync, randomUUID } from 'node:crypto';
import { createRequire } from 'node:module';
import { mkdtemp, mkdir, writeFile, realpath, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createHttpApp } from '../../server/http-app.js';
import { captureAccountAiConfiguration } from '../../server/account-ai-configuration.js';
import { createSignedConnectHttpClient } from '../../server/test/support/signed-connect-client.mjs';
import { migrate } from '../../services/ai-gateway/apps/gateway/src/migrate.mjs';
import { Store } from '../../services/ai-gateway/apps/gateway/src/store.mjs';
import { createApp } from '../../services/ai-gateway/apps/gateway/src/app.mjs';
import { createConnector } from '../../services/ai-gateway/packages/connector/index.mjs';
import { createSotyBridge, validateSotyBridgeConfig } from '../../services/ai-gateway/apps/gateway/src/soty-bridge.mjs';

const requireGateway = createRequire(new URL('../../services/ai-gateway/package.json', import.meta.url));
const { Pool } = requireGateway('pg');
const MODEL = 'zai-org/GLM-5.3-Flash', ISSUER = 'https://4-2.xn--p1ai', GATEWAY = 'urn:soty:ai-gateway:production';
const database = process.env.TEST_DATABASE_URL;
const listen = server => new Promise(done => server.listen(0, '127.0.0.1', done));
const originOf = server => `http://127.0.0.1:${server.address().port}`;

test('actual Soty Root, signed bridge, gateway, provider and PostgreSQL keep one personal account across every ingress and restart', { skip: !database, timeout: 240000 }, async t => {
  assert.ok(new URL(database).pathname.endsWith('_test'), 'Only a dedicated synthetic *_test database may be used');
  const schema = 'cross_test_' + randomUUID().replaceAll('-', ''), bootstrap = new Pool({ connectionString: database, max: 2 });
  await bootstrap.query(`CREATE SCHEMA ${schema}`);
  const pool = new Pool({ connectionString: database, max: 12, options: `-c search_path=${schema}` });
  const directory = await mkdtemp(path.join(await realpath(tmpdir()), 'soty-full-account-ai-')), dist = path.join(directory, 'dist');
  await mkdir(dist); await writeFile(path.join(dist, 'index.html'), '<!doctype html><title>Full account AI qualification</title>');
  const rootKeys = generateKeyPairSync('ed25519'), gatewayKeys = generateKeyPairSync('ed25519'), sockets = new Set();
  const state = { dispatches: 0, bypasses: 0 }; let rootApp, gatewayApp, connector, gatewayPort = 0;
  const track = server => server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  const provider = track(createServer(async (req, res) => {
    if (req.url === '/v1/models') { res.setHeader('content-type', 'application/json'); res.end(JSON.stringify({ data: [{ id: MODEL, context_length: 131072 }] })); return; }
    if (req.url !== '/v1/chat/completions') { res.writeHead(404).end(); return; }
    let encoded = ''; for await (const chunk of req) encoded += chunk;
    const body = JSON.parse(encoded); state.dispatches++;
    if (body.stream) {
      res.setHeader('content-type', 'text/event-stream');
      res.write('data: ' + JSON.stringify({ id: 'synthetic-stream', model: MODEL, choices: [{ index: 0, delta: { role: 'assistant', content: 'Сквозной поток' }, finish_reason: null }] }) + '\n\n');
      res.write('data: ' + JSON.stringify({ id: 'synthetic-stream', model: MODEL, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }], usage: { prompt_tokens: 7, completion_tokens: 3, total_tokens: 10 } }) + '\n\n');
      res.end('data: [DONE]\n\n'); return;
    }
    const native = body.tools?.some(tool => tool.function?.name === 'finish_answer');
    const message = native ? { role: 'assistant', content: null, tool_calls: [{ id: 'synthetic-final', type: 'function', function: {
      name: 'finish_answer', arguments: JSON.stringify({ answer: 'Сквозной личный ответ', selectedEvidenceIds: [], claims: [] }) } }] }
      : { role: 'assistant', content: 'Сквозной ответ' };
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ id: 'synthetic-completion', object: 'chat.completion', model: MODEL,
      choices: [{ index: 0, message, finish_reason: native ? 'tool_calls' : 'stop' }], usage: { prompt_tokens: 7, completion_tokens: 3, total_tokens: 10 } }));
  }));
  const rootServer = track(createServer((req, res) => rootApp ? rootApp(req, res) : res.writeHead(503).end()));
  rootServer.on('upgrade', (req, socket, head) => { if (!rootApp?.locals.appsService.handleUpgrade(req, socket, head)) socket.destroy(); });
  await Promise.all([listen(provider), listen(rootServer)]); const rootOrigin = originOf(rootServer);
  let configuration;
  async function startGateway() {
    await migrate(pool);
    const store = new Store(pool, { pepper: 'synthetic-only-full-stack-pepper-more-than-thirty-two-bytes', dailyServiceBudgetMicrorubles: '50000000' });
    connector = createConnector({ providers: [{ id: 'synthetic', baseUrl: originOf(provider) + '/v1', apiKey: 'synthetic-provider-key' }],
      liveEnabled: true, allowLocalHttp: true, retail: { input: '1000000', output: '1000000' }, store: store.connectorHooks(), onAttempt: attempt => store.recordAttempt(attempt) });
    await connector.refreshCatalog();
    const config = validateSotyBridgeConfig({ schema: 'soty.ai-gateway-bridge.v1', audience: 'urn:soty:ai-gateway:v1',
      issuers: [{ issuer: ISSUER, kid: 'cross-root', publicKey: rootKeys.publicKey }],
      signing: { issuer: GATEWAY, kid: 'cross-gateway', privateKey: gatewayKeys.privateKey },
      root: { origin: rootOrigin, audience: 'urn:soty:root-ai-dispatch:v1', allowPrivateHttp: true } });
    gatewayApp = await createApp({ store, connector, sotyBridge: createSotyBridge({ config, store }), baseUrl: 'https://synthetic.invalid', installEnabled: true });
    await gatewayApp.listen({ host: '127.0.0.1', port: gatewayPort }); gatewayPort = gatewayApp.server.address().port;
    return store;
  }
  const sponsor = 'svc_' + 'S'.repeat(32), appToken = 'synthetic_full_stack_application_'.padEnd(48, 's');
  function startRoot() {
    rootApp = createHttpApp(dist, { env: {}, dataDir: path.join(directory, 'data'), connectOrigins: [rootOrigin], accountAi: configuration,
      universalAppsEnabled: false, personalAgent: { enabled: false }, localExecutorCurrent: false,
      appOriginTemplate: `http://{appId}.localhost:${rootServer.address().port}`, appHosting: { domainProfile: 'separate-site', namedAppZone: '', discoveryOrigin: '' },
      gonka: { baseUrl: 'https://forbidden-provider-bypass.invalid/v1', apiKey: 'synthetic-old-key', applicationTokens: JSON.stringify({ synthetic_app: appToken }), applicationTokensFile: '',
        fetchImpl: async () => { state.bypasses++; throw Error('Shared direct provider path forbidden'); } } });
  }
  t.after(async () => {
    await rootApp?.locals.closeServices(); await gatewayApp?.close(); await connector?.stop(); sockets.forEach(socket => socket.destroy());
    await Promise.all([new Promise(done => rootServer.close(done)), new Promise(done => provider.close(done))]); await pool.end();
    await bootstrap.query(`DROP SCHEMA ${schema} CASCADE`); await bootstrap.end();
    const target = await realpath(directory); assert.equal(path.dirname(target), await realpath(tmpdir())); assert.match(path.basename(target), /^soty-full-account-ai-/u);
    await rm(target, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
  });
  await startGateway();
  configuration = captureAccountAiConfiguration({ schema: 'soty.account-ai-installed.v1', issuer: ISSUER, gatewayOrigin: `http://127.0.0.1:${gatewayPort}`,
    signing: { kid: 'cross-root', privateKey: rootKeys.privateKey.export({ type: 'pkcs8', format: 'pem' }) },
    gatewaySigner: { issuer: GATEWAY, kid: 'cross-gateway', publicKey: gatewayKeys.publicKey.export({ type: 'spki', format: 'pem' }) },
    applications: [{ appId: 'synthetic_app', accountId: sponsor, models: [MODEL] }] }, { allowLoopbackFixture: true });
  startRoot();
  const alice = createSignedConnectHttpClient({ origin: rootOrigin }), bob = createSignedConnectHttpClient({ origin: rootOrigin }), second = createSignedConnectHttpClient({ origin: rootOrigin });
  const a = await alice.bootstrap('Synthetic A'), b = await bob.bootstrap('Synthetic B');
  let aStatus, bStatus, externalKey;
  await t.test('signup automatically gets isolated usable default credentials without key setup', async () => {
    [aStatus, bStatus] = await Promise.all([alice.call('ai.account.get', { expectedAccountId: a.accountId }), bob.call('ai.account.get', { expectedAccountId: b.accountId })]);
    assert.equal(aStatus.state, 'ready'); assert.equal(bStatus.state, 'ready'); assert.notEqual(aStatus.defaultKey.id, bStatus.defaultKey.id);
    assert.equal(aStatus.allowance.grantedMicrorubles, '50000000'); assert.equal(state.dispatches, 0);
  });
  await t.test('ordinary native turn crosses both real services and records settled usage and owner history', async () => {
    const answer = await alice.call('agent.turn', { requestId: 'native-cross-1', prompt: 'Напиши короткую фразу.' });
    assert.equal(answer.status, 'complete'); assert.equal(answer.answer, 'Сквозной личный ответ'); assert.equal(state.dispatches, 1);
    const updated = await alice.call('ai.account.get', { expectedAccountId: a.accountId });
    assert.equal(Number(updated.usage.requests), 1); assert.equal(Number(updated.usage.tokens), 10); assert.ok(BigInt(updated.allowance.spentMicrorubles) > 0n);
    const notes = await alice.call('notes.list', { expectedAccountId: a.accountId }); assert.equal(notes.notes.length, 1);
    const foreign = await bob.call('notes.list', { expectedAccountId: b.accountId }); assert.equal(foreign.notes.length, 0);
  });
  await t.test('a second device keeps the same account, quota and history', async () => {
    const enrollment = await second.enrollmentStart('Second synthetic device');
    await alice.call('enrollment.approve', { requestId: enrollment.requestId, wrappedKey: { schema: 'fixture.encrypted', ciphertext: 'opaque-synthetic' } });
    await second.call('enrollment.finish', { requestId: enrollment.requestId, expectedAccountId: a.accountId });
    const same = await second.call('ai.account.get', { expectedAccountId: a.accountId }); assert.equal(same.defaultKey.id, aStatus.defaultKey.id); assert.equal(Number(same.usage.requests), 1);
  });
  await t.test('external developer key is one-time, owner isolated, and uses the same account ledger', async () => {
    const args = { expectedAccountId: a.accountId, operationId: randomUUID(), name: 'Synthetic external client' };
    const created = await alice.call('ai.keys.create', args); assert.equal(typeof created.secret, 'string'); externalKey = created.key;
    const replay = await second.call('ai.keys.create', args); assert.equal(replay.key.id, created.key.id); assert.equal(replay.secret, null);
    const foreign = await bob.raw('ai.keys.revoke', { expectedAccountId: b.accountId, operationId: randomUUID(), keyId: created.key.id }); assert.equal(foreign.ok, false);
    const response = await fetch(`http://127.0.0.1:${gatewayPort}/v1/chat/completions`, { method: 'POST', headers: { authorization: 'Bearer ' + created.secret, 'content-type': 'application/json', 'idempotency-key': 'cross-external-1' },
      body: JSON.stringify({ model: MODEL, messages: [{ role: 'user', content: 'Synthetic external' }], max_tokens: 16, stream: false }) });
    assert.equal(response.status, 200); await response.json();
    const status = await second.call('ai.account.get', { expectedAccountId: a.accountId }); assert.equal(Number(status.usage.requests), 2);
    const untouched = await bob.call('ai.account.get', { expectedAccountId: b.accountId }); assert.equal(Number(untouched.usage.requests), 0);
  });
  await t.test('static application JSON and SSE use only the explicit bounded service sponsor', async () => {
    for (const stream of [false, true]) {
      const response = await fetch(rootOrigin + '/api/inference/v1/chat/completions', { method: 'POST', headers: { authorization: 'Bearer ' + appToken, 'content-type': 'application/json' },
        body: JSON.stringify({ model: MODEL, messages: [{ role: 'user', content: 'Synthetic service app' }], max_tokens: 16, stream }) });
      assert.equal(response.status, 200); const body = await response.text(); assert.ok(body.includes(stream ? '[DONE]' : 'Сквозной ответ'));
    }
    const rows = (await pool.query('SELECT account_id,count(r.id)::int AS requests FROM soty_accounts a LEFT JOIN inference_requests r ON r.tenant_id=a.tenant_id GROUP BY account_id')).rows;
    assert.equal(rows.find(row => row.account_id === sponsor).requests, 2); assert.equal(rows.find(row => row.account_id === a.accountId).requests, 2);
    assert.equal(state.bypasses, 0);
  });
  await t.test('cold service restart preserves account uniqueness, grants, key revocation and usage', async () => {
    await second.call('ai.keys.revoke', { expectedAccountId: a.accountId, operationId: randomUUID(), keyId: externalKey.id });
    await rootApp.locals.closeServices(); rootApp = null; await gatewayApp.close(); await connector.stop();
    await startGateway(); startRoot();
    const resumed = await second.call('ai.account.get', { expectedAccountId: a.accountId }); assert.equal(resumed.defaultKey.id, aStatus.defaultKey.id); assert.equal(Number(resumed.usage.requests), 2);
    const keys = await second.call('ai.keys.list', { expectedAccountId: a.accountId }); assert.equal(keys.keys.find(key => key.id === externalKey.id).status, 'revoked');
    const counts = (await pool.query("SELECT (SELECT count(*) FROM soty_accounts) AS accounts,(SELECT count(*) FROM ledger_transactions WHERE operation='sponsor_grant') AS grants")).rows[0];
    assert.equal(counts.accounts, '3'); assert.equal(counts.grants, '3'); assert.equal(state.bypasses, 0);
  });
});
