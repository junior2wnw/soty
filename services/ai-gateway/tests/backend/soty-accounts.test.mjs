import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import { once } from 'node:events';
import pg from 'pg';
import { migrate } from '../../apps/gateway/src/migrate.mjs';
import { Store } from '../../apps/gateway/src/store.mjs';
import { createApp } from '../../apps/gateway/src/app.mjs';
import { createConnector } from '../../packages/connector/index.mjs';
import { createSotyBridge, validateSotyBridgeConfig } from '../../apps/gateway/src/soty-bridge.mjs';
import { createBridgeSigner, createBridgeVerifier } from '../../packages/soty-bridge/index.mjs';
import { DEFAULT_SOTY_ALLOWANCE } from '../../apps/gateway/src/soty-policy.mjs';

const database = process.env.TEST_DATABASE_URL;
const MODEL = 'zai-org/GLM-5.3-Flash';
const ISSUER = 'https://4-2.xn--p1ai';
const A = 'account-a'; const B = 'account-b';
const chat = (text = 'synthetic', extra = {}) => ({ model: MODEL, messages: [{ role: 'user', content: text }], max_tokens: 32, autoFallback: false, ...extra });
const completion = (model, known = true) => ({ id: 'chatcmpl-synthetic', object: 'chat.completion', model, choices: [{ index: 0, message: { role: 'assistant', content: 'Synthetic response' }, finish_reason: 'stop' }], ...(known ? { usage: { prompt_tokens: 7, completion_tokens: 3, total_tokens: 10 } } : {}) });

async function until(predicate, timeout = 5000) {
  const deadline = Date.now() + timeout;
  while (!await predicate()) { if (Date.now() >= deadline) throw new Error('Synthetic condition timed out'); await new Promise(resolve => setTimeout(resolve, 10)); }
}

test('signed account AI uses permanent isolated identities, one ledger and final live dispatch authority', { skip: !database, timeout: 240000 }, async t => {
  if (!new URL(database).pathname.endsWith('_test')) throw new Error('A dedicated *_test database is required');
  const schema = `soty_test_${randomUUID().replaceAll('-', '')}`;
  const bootstrap = new pg.Pool({ connectionString: database, max: 2 });
  await bootstrap.query(`CREATE SCHEMA ${schema}`);
  const pool = new pg.Pool({ connectionString: database, max: 12, options: `-c search_path=${schema}` });
  const rootKeys = generateKeyPairSync('ed25519'); const gatewayKeys = generateKeyPairSync('ed25519');
  const rootNonces = new Set(); const authorities = new Map(); const callbacks = []; const received = [];
  let app; let gateRelease; let providerMode = 'normal'; let callbackMode = 'normal';
  const provider = createServer(async (request, response) => {
    if (request.url === '/v1/models') { response.setHeader('content-type', 'application/json'); response.end(JSON.stringify({ data: [{ id: MODEL }] })); return; }
    let raw = ''; for await (const chunk of request) raw += chunk;
    const body = JSON.parse(raw); received.push(body);
    if (body.messages[0].content === 'hold') await new Promise(resolve => { gateRelease = resolve; });
    if (body.messages[0].content === 'revoke-after-primary') { const authority = [...authorities.values()].find(entry => entry.text === 'revoke-after-primary'); authority.active = false; response.statusCode = 503; response.end(); return; }
    if (body.messages[0].content === 'abort-active') { response.once('close', () => {}); return; }
    if (body.stream) {
      response.setHeader('content-type', 'text/event-stream');
      response.write(`data: ${JSON.stringify({ id: 'synthetic', model: body.model, choices: [{ index: 0, delta: { content: 'Synthetic' }, finish_reason: null }] })}\n\n`);
      response.write(`data: ${JSON.stringify({ id: 'synthetic', model: body.model, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] })}\n\n`);
      response.write(`data: ${JSON.stringify({ id: 'synthetic', model: body.model, choices: [], usage: { prompt_tokens: 7, completion_tokens: 3, total_tokens: 10 } })}\n\n`);
      response.end('data: [DONE]\n\n'); return;
    }
    response.setHeader('content-type', 'application/json'); response.end(JSON.stringify(completion(body.model, providerMode !== 'unknown')));
  });
  const rootVerifier = createBridgeVerifier({ audience: 'urn:soty:root-ai-dispatch:v1', issuers: [{ issuer: 'urn:soty:ai-gateway:production', kid: 'gateway', publicKey: gatewayKeys.publicKey }], claimNonce: async proof => { if (rootNonces.has(proof.jti)) return false; rootNonces.add(proof.jti); return true; } });
  const root = createServer(async (request, response) => {
    let raw = ''; for await (const chunk of request) raw += chunk;
    try {
      const proof = await rootVerifier.verify({ method: request.method, path: request.url, body: raw, headers: request.headers });
      const body = JSON.parse(raw); callbacks.push(body);
      const authority = authorities.get(body.authorizationId);
      const valid = proof.operation === 'dispatch.authorize' && authority?.active && body.issuer === ISSUER
        && body.accountId === authority.accountId && body.deviceId === authority.deviceId && body.requestId === authority.requestId
        && body.bodySha256 === authority.bodySha256 && proof.accountId === body.accountId && proof.deviceId === body.deviceId
        && proof.authorizationId === body.authorizationId && proof.requestId === body.requestId && body.model === MODEL;
      if (callbackMode === 'deny' || !valid) { response.statusCode = 403; response.end('{}'); return; }
      response.setHeader('content-type', 'application/json'); response.end('{"authorized":true}');
    } catch { response.statusCode = 401; response.end('{}'); }
  });
  await Promise.all([new Promise(resolve => provider.listen(0, '127.0.0.1', resolve)), new Promise(resolve => root.listen(0, '127.0.0.1', resolve))]);
  try {
    await migrate(pool);
    const store = new Store(pool, { pepper: 'synthetic-test-pepper-with-at-least-thirty-two-bytes', dailyServiceBudgetMicrorubles: '50000000' });
    const providers = ['first', 'second'].map(id => ({ id, baseUrl: `http://127.0.0.1:${provider.address().port}/v1`, apiKey: 'synthetic-key-no-external-provider' }));
    const connector = createConnector({ providers, allowLocalHttp: true, liveEnabled: true, providerConcurrency: 1, retail: { input: '1000000', output: '1000000' }, timeouts: { deadlineMs: 20000, primaryMs: 15000, queueMs: 10000 }, store: store.connectorHooks(), onAttempt: attempt => store.recordAttempt(attempt) });
    await connector.refreshCatalog();
    const config = validateSotyBridgeConfig({ schema: 'soty.ai-gateway-bridge.v1', audience: 'urn:soty:ai-gateway:v1', issuers: [{ issuer: ISSUER, kid: 'root', publicKey: rootKeys.publicKey }], signing: { issuer: 'urn:soty:ai-gateway:production', kid: 'gateway', privateKey: gatewayKeys.privateKey }, root: { origin: `http://127.0.0.1:${root.address().port}`, audience: 'urn:soty:root-ai-dispatch:v1', allowPrivateHttp: true } });
    const bridge = createSotyBridge({ config, store });
    app = await createApp({ store, connector, baseUrl: 'https://synthetic.soty.invalid', sotyBridge: bridge, installEnabled: true });
    const signer = createBridgeSigner({ issuer: ISSUER, audience: config.audience, kid: 'root', privateKey: rootKeys.privateKey });
    function signed(accountId, operation, method, path, payload, { requestId = randomUUID(), authorizationId, deviceId = 'device-a' } = {}) {
      const body = payload === undefined ? '' : JSON.stringify(payload);
      const headers = { ...signer.headers({ method, path, body, operation, accountId, deviceId, requestId, authorizationId }), ...(body ? { 'content-type': 'application/json' } : {}) };
      return { method, url: path, headers, ...(body ? { payload: body } : {}) };
    }
    const ensure = (accountId, deviceId = 'service:provisioner') => app.inject(signed(accountId, 'account.ensure', 'POST', '/internal/soty/v1/accounts/ensure', {}, { deviceId }));
    const status = accountId => app.inject(signed(accountId, 'account.status', 'GET', '/internal/soty/v1/account'));
    const keys = accountId => app.inject(signed(accountId, 'keys.list', 'GET', '/internal/soty/v1/keys'));
    const mutate = (accountId, action, payload, requestId = randomUUID()) => app.inject(signed(accountId, `keys.${action}`, 'POST', `/internal/soty/v1/keys/${action}`, payload, { requestId }));
    function inferenceInput(accountId, payload = chat(), options = {}) {
      const requestId = options.requestId ?? randomUUID(); const authorizationId = randomUUID(); const deviceId = options.deviceId ?? 'device-a';
      const input = signed(accountId, 'chat.complete', 'POST', '/internal/soty/v1/chat/completions', payload, { requestId, authorizationId, deviceId });
      const proof = JSON.parse(Buffer.from(input.headers['x-soty-bridge'].split('.')[0], 'base64url').toString('utf8'));
      authorities.set(authorizationId, { active: true, accountId, deviceId, requestId, bodySha256: proof.bodySha256, text: payload.messages[0].content });
      return { input, authorizationId, requestId };
    }
    const inference = (accountId, payload, options) => app.inject(inferenceInput(accountId, payload, options).input);

    await t.test('concurrent provisioning, second device and restarted Store create one permanent wallet/default key/grant', async () => {
      const first = await Promise.all(Array.from({ length: 8 }, () => ensure(A)));
      assert.equal(first.every(result => result.statusCode === 200), true, first[0].body);
      assert.equal((await ensure(B)).statusCode, 200);
      const a = first[0].json(); const b = (await status(B)).json();
      assert.equal(a.schema, 'soty.ai-account.v1'); assert.equal(a.state, 'ready'); assert.notEqual(a.defaultKey.id, b.defaultKey.id);
      assert.equal(a.allowance.grantedMicrorubles, '50000000'); assert.equal(a.allowance.kind, 'included');
      assert.deepEqual(a.allowance.limits, DEFAULT_SOTY_ALLOWANCE.limits);
      const secondDevice = (await ensure(A, 'device-b')).json(); assert.equal(secondDevice.defaultKey.id, a.defaultKey.id);
      const restarted = new Store(pool, { pepper: store.pepper }); await restarted.ensureSotyAccount({ iss: ISSUER, accountId: A });
      const counts = await pool.query("SELECT (SELECT count(*) FROM soty_accounts) AS accounts,(SELECT count(*) FROM api_keys WHERE purpose='chat') AS defaults,(SELECT count(*) FROM credentials) AS credentials,(SELECT count(*) FROM ledger_transactions WHERE operation='sponsor_grant') AS grants");
      assert.deepEqual(counts.rows[0], { accounts: '2', defaults: '2', credentials: '0', grants: '2' });
      assert.equal((await pool.query("SELECT count(*) FROM ledger_transactions WHERE operation='sponsor_grant' AND NOT is_demo AND metadata->>'serviceFunded'='true'")).rows[0].count, '2');
    });

    await t.test('persistent nonce and exact body/path/operation validation fail before dispatch; service principal cannot infer or manage keys', async () => {
      const input = signed(A, 'account.status', 'GET', '/internal/soty/v1/account');
      assert.equal((await app.inject(input)).statusCode, 200);
      assert.equal((await app.inject(input)).json().error.code, 'bridge_replay');
      const again = createSotyBridge({ config, store: new Store(pool, { pepper: store.pepper }) });
      await assert.rejects(again.verify({ method: input.method, path: input.url, body: '', headers: input.headers }), { code: 'bridge_replay' });
      const changed = signed(A, 'keys.create', 'POST', '/internal/soty/v1/keys/create', { name: 'Original' }); changed.payload = JSON.stringify({ name: 'Altered' });
      assert.equal((await app.inject(changed)).statusCode, 401);
      assert.equal((await app.inject(signed(A, 'account.ensure', 'GET', '/internal/soty/v1/account'))).statusCode, 403);
      assert.equal((await app.inject(signed(A, 'keys.list', 'GET', '/internal/soty/v1/keys', undefined, { deviceId: 'service:application:test' }))).statusCode, 403);
      assert.equal((await inference(A, chat(), { deviceId: 'service:provisioner' })).statusCode, 403);
      assert.equal(received.length, 0);
    });

    let developer;
    await t.test('developer create/update/rotate/revoke are once-only, tenant isolated, and never expose internal keys or stored secrets', async () => {
      const operationId = randomUUID(); const input = { name: 'Developer', limits: { dailyMicrorubles: '1000000' } };
      const created = await mutate(A, 'create', input, operationId); assert.equal(created.statusCode, 201, created.body); developer = created.json(); assert.ok(developer.secret);
      const repeated = (await mutate(A, 'create', input, operationId)).json(); assert.equal(repeated.secret, null); assert.equal(repeated.key.id, developer.key.id);
      assert.equal((await mutate(A, 'create', { name: 'Changed' }, operationId)).statusCode, 409);
      assert.equal((await keys(A)).json().keys.length, 1); assert.equal((await keys(B)).json().keys.length, 0);
      for (const action of ['update', 'rotate', 'revoke']) assert.equal((await mutate(B, action, { keyId: developer.key.id, ...(action === 'update' ? { name: 'stolen' } : {}) })).statusCode, 404);
      const defaultId = (await status(A)).json().defaultKey.id;
      assert.equal((await mutate(A, 'rotate', { keyId: defaultId })).statusCode, 404);
      const updateId = randomUUID(); const body = { keyId: developer.key.id, name: 'Renamed', expectedPolicyRevision: 1 };
      const update = (await mutate(A, 'update', body, updateId)).json(); assert.equal(update.key.policyRevision, 2);
      assert.equal((await mutate(A, 'update', body, updateId)).json().key.policyRevision, 2);
      assert.equal((await mutate(A, 'update', body)).json().error.code, 'policy_revision_conflict');
      const rotateId = randomUUID(); const rotated = (await mutate(A, 'rotate', { keyId: developer.key.id }, rotateId)).json(); assert.ok(rotated.secret); assert.equal(rotated.overlapHours, 24);
      assert.equal((await mutate(A, 'rotate', { keyId: developer.key.id }, rotateId)).json().secret, null);
      const revokeId = randomUUID(); assert.equal((await mutate(A, 'revoke', { keyId: developer.key.id }, revokeId)).json().key.status, 'revoked');
      assert.equal((await mutate(A, 'revoke', { keyId: developer.key.id }, revokeId)).json().replayed, true);
      assert.equal((await mutate(A, 'rotate', { keyId: developer.key.id }, rotateId)).json().secret, null);
      assert.equal((await pool.query('SELECT count(*) FROM credentials WHERE key_id=$1 AND revoked_at IS NULL', [developer.key.id])).rows[0].count, '0');
      const serialized = JSON.stringify((await keys(A)).json()); assert.equal(serialized.includes(developer.secret), false); assert.equal(serialized.includes(rotated.secret), false);
      assert.equal((await pool.query('SELECT count(*) FROM idempotency_operations WHERE safe_result::text LIKE $1', [`%${rotated.secret}%`])).rows[0].count, '0');
    });

    await t.test('signed JSON/SSE and receipts preserve final usage, exact actor callback and idempotency without redispatch', async () => {
      const logical = randomUUID(); const before = received.length;
      const result = await inference(A, chat('bound-body'), { requestId: logical }); assert.equal(result.statusCode, 200, result.body);
      const id = result.headers['x-soty-request-id']; assert.match(id, /^[0-9a-f-]{36}$/); assert.equal(result.headers['x-soty-cost-microrubles'], '10');
      assert.equal((await inference(A, chat('bound-body'), { requestId: logical })).headers['x-idempotent-replay'], 'true');
      assert.equal(received.length, before + 1);
      assert.equal((await inference(A, chat('changed-body'), { requestId: logical })).statusCode, 409);
      const receipt = await app.inject(signed(A, 'request.receipt', 'GET', `/internal/soty/v1/requests/${id}`));
      assert.equal(receipt.statusCode, 200); assert.equal(receipt.json().request.logicalRequestId, logical); assert.equal(Number(receipt.json().request.input_tokens), 7);
      assert.equal((await app.inject(signed(B, 'request.receipt', 'GET', `/internal/soty/v1/requests/${id}`))).statusCode, 404);
      assert.equal(callbacks.at(-1).requestId, logical); assert.equal(callbacks.at(-1).accountId, A);
      const sse = await inference(A, chat('stream', { stream: true })); assert.equal(sse.statusCode, 200, sse.body); assert.match(sse.body, /data: \[DONE\]/);
      assert.equal((await status(A)).json().usage.tokens, 20);
    });

    await t.test('multi-key account quota is atomic and unknown dispatched usage stays charged to quota', async () => {
      await ensure('quota-account'); const account = await store.sotyAccount({ iss: ISSUER, accountId: 'quota-account' });
      await pool.query("UPDATE soty_accounts SET limits=jsonb_set(limits,'{concurrency}','1') WHERE tenant_id=$1", [account.tenant_id]);
      const one = (await mutate('quota-account', 'create', { name: 'One' })).json(); const two = (await mutate('quota-account', 'create', { name: 'Two' })).json();
      const external = await app.inject({ method: 'POST', url: '/v1/chat/completions', headers: { authorization: `Bearer ${one.secret}`, 'content-type': 'application/json' }, payload: JSON.stringify(chat('external-personal-key')) });
      assert.equal(external.statusCode, 200, external.body); assert.equal((await status('quota-account')).json().allowance.spentMicrorubles, '10');
      assert.equal((await app.inject({ method: 'POST', url: '/api/connectors/gonka/v1/chat/completions', headers: { authorization: `Bearer ${one.secret}` }, payload: chat() })).statusCode, 403);
      const model = connector.models()[0]; const args = { model, inputTokens: 100, outputTokens: 50, inputRate: '1000000', outputRate: '1000000', body: chat(), autoFallback: false };
      const accepted = await Promise.allSettled([store.reserve({ ...args, key: await store.authenticate(one.secret), idempotencyKey: randomUUID() }), store.reserve({ ...args, key: await store.authenticate(two.secret), idempotencyKey: randomUUID() })]);
      assert.equal(accepted.filter(result => result.status === 'fulfilled').length, 1);
      assert.equal(accepted.find(result => result.status === 'rejected').reason.code, 'account_concurrency_limit');
      const reservation = accepted.find(result => result.status === 'fulfilled').value;
      await store.finish(reservation.id, { outcome: 'failed', noUpstreamDispatch: true });
      assert.equal((await pool.query('SELECT quota_urub FROM inference_requests WHERE id=$1', [reservation.id])).rows[0].quota_urub, '0');
      providerMode = 'unknown'; const unknown = await inference('quota-account', chat('unknown')); providerMode = 'normal';
      assert.equal(unknown.statusCode, 200, unknown.body);
      const row = (await pool.query('SELECT * FROM inference_requests WHERE id=$1', [unknown.headers['x-soty-request-id']])).rows[0];
      assert.equal(row.status, 'usage_pending'); assert.equal(row.charge_urub, '0'); assert.equal(row.quota_pending, true); assert.ok(BigInt(row.quota_urub) > 0n); assert.ok(BigInt(row.quota_tokens) > 0n);
      const state = (await status('quota-account')).json(); assert.equal(state.allowance.pendingMicrorubles, row.quota_urub);
      await pool.query("UPDATE soty_accounts SET limits=jsonb_set(limits,'{dailyMicrorubles}',to_jsonb($2::text)) WHERE tenant_id=$1", [account.tenant_id, row.quota_urub]);
      const denied = await inference('quota-account', chat('must-not-dispatch')); assert.equal(denied.statusCode, 429); assert.equal(denied.json().error.code, 'account_daily_budget_limit');
      await store.ensureSotyAccount({ iss: ISSUER, accountId: 'quota-account' });
      assert.equal((await status('quota-account')).json().state, 'limited');
    });

    await t.test('known cheaper fallback uses its own admitted rate, while receipt remains final-response usage', async () => {
      await ensure('attempt-prices'); const account = await store.sotyAccount({ iss: ISSUER, accountId: 'attempt-prices' }); const key = await store.getChatKey(account.tenant_id);
      const initialPrice = { inputRate: '10000000', outputRate: '20000000' }; const cheaperPrice = { inputRate: '1000000', outputRate: '2000000' };
      const reservation = await store.reserve({ key, model: connector.models()[0], inputTokens: 100, outputTokens: 50, ...initialPrice, body: chat(), autoFallback: false });
      await store.admitDispatch({ key, reservation, model: MODEL, commit: true });
      await store.recordAttempt({ logicalRequestId: reservation.id, providerId: 'first', model: MODEL, purpose: 'chat', outcome: 'error', inputTokens: 10, outputTokens: 2, priceSnapshot: initialPrice });
      await store.admitDispatch({ key, reservation, model: 'test/cheaper', commit: true });
      await store.recordAttempt({ logicalRequestId: reservation.id, providerId: 'second', model: 'test/cheaper', purpose: 'chat', outcome: 'success', inputTokens: 7, outputTokens: 3, priceSnapshot: cheaperPrice });
      const result = await store.finish(reservation.id, { outcome: 'completed', usage: { prompt_tokens: 7, completion_tokens: 3 }, model: 'test/cheaper', providerId: 'second', reportedPriceSnapshot: cheaperPrice });
      assert.equal(result.chargeMicrorubles, '153');
      const receipt = (await store.requestReceipt(account.tenant_id, reservation.id)).request; assert.equal(Number(receipt.input_tokens), 7); assert.equal(Number(receipt.output_tokens), 3);
      const state = (await status('attempt-prices')).json(); assert.equal(state.allowance.spentMicrorubles, '153'); assert.equal(state.usage.tokens, 22);
      assert.equal((await pool.query('SELECT s.actual_urub FROM service_budget_holds s JOIN inference_requests r ON r.service_budget_hold_id=s.id WHERE r.id=$1', [reservation.id])).rows[0].actual_urub, '153');
    });

    await t.test('all active developer keys remain visible even after more than 200 historical revocations', async () => {
      await ensure('key-history'); const account = await store.sotyAccount({ iss: ISSUER, accountId: 'key-history' });
      const active = (await mutate('key-history', 'create', { name: 'Still active' })).json();
      assert.equal((await mutate('key-history', 'create', { name: 'Unicode policy rejected', allowedModels: ['модель/ИИ'] })).statusCode, 400);
      assert.equal((await mutate('key-history', 'update', { keyId: active.key.id, allowedModels: ['model with spaces'] })).statusCode, 400);
      await pool.query(`INSERT INTO api_keys(id,tenant_id,name,prefix,last_four,status,limits)
        SELECT gen_random_uuid(),$1,'Historical revoked','synthetic','test','revoked',$2::jsonb FROM generate_series(1,201)`, [account.tenant_id, DEFAULT_SOTY_ALLOWANCE.limits]);
      const listed = (await keys('key-history')).json().keys; assert.equal(listed.length, 200); assert.equal(listed[0].id, active.key.id); assert.equal(listed.filter(key => key.status === 'active').length, 1);
      const maximumModels = Array.from({ length: 100 }, (_, index) => `model/${String(index).padStart(3, '0')}/${'x'.repeat(230)}`);
      assert.equal(maximumModels.every(id => id.length === 240), true);
      await pool.query('UPDATE api_keys SET allowed_models=$2::jsonb WHERE tenant_id=$1', [account.tenant_id, JSON.stringify(maximumModels)]);
      const maximum = await keys('key-history'); assert.equal(maximum.statusCode, 200); assert.ok(Buffer.byteLength(maximum.body) < 8 * 1024 * 1024);
      assert.equal(maximum.json().keys.every(key => key.allowedModels.length === 100), true);
    });

    await t.test('live Root revocation after queue admission and callback denial release all holds without provider dispatch', async () => {
      await ensure('queue-first'); await ensure('queue-second');
      const first = inference('queue-first', chat('hold'));
      await until(() => typeof gateRelease === 'function');
      const queued = inferenceInput('queue-second', chat('queued'));
      const pending = app.inject(queued.input);
      await until(() => connector.snapshot().providers[0].queued === 1);
      authorities.get(queued.authorizationId).active = false; gateRelease(); gateRelease = undefined;
      assert.equal((await first).statusCode, 200);
      const rejected = await pending; assert.equal(rejected.statusCode, 403, rejected.body);
      assert.equal(received.some(body => body.messages[0].content === 'queued'), false);
      const wallet = (await status('queue-second')).json().allowance; assert.equal(wallet.heldMicrorubles, '0'); assert.equal(wallet.pendingMicrorubles, '0'); assert.equal(wallet.availableMicrorubles, '50000000');
      callbackMode = 'deny'; const before = received.length; const denied = await inference(B, chat('denied')); callbackMode = 'normal';
      assert.equal(denied.statusCode, 403); assert.equal(received.length, before);
    });

    await t.test('account spend reservation is atomic across different keys, and a repeated ensure never restores spent quota', async () => {
      await ensure('money-race'); const account = await store.sotyAccount({ iss: ISSUER, accountId: 'money-race' });
      await pool.query("UPDATE soty_accounts SET limits=jsonb_set(limits,'{dailyMicrorubles}','\"300\"') WHERE tenant_id=$1", [account.tenant_id]);
      const first = (await mutate('money-race', 'create', { name: 'First' })).json(); const second = (await mutate('money-race', 'create', { name: 'Second' })).json();
      const keyA = await store.authenticate(first.secret); const keyB = await store.authenticate(second.secret);
      const args = { model: connector.models()[0], inputTokens: 50, outputTokens: 50, inputRate: '1000000', outputRate: '1000000', body: chat(), autoFallback: false };
      const results = await Promise.allSettled([store.reserve({ ...args, key: keyA }), store.reserve({ ...args, key: keyB })]);
      assert.equal(results.filter(result => result.status === 'fulfilled').length, 1);
      assert.equal(results.find(result => result.status === 'rejected').reason.code, 'account_daily_budget_limit');
      const admitted = results.find(result => result.status === 'fulfilled').value;
      assert.equal((await status('money-race')).json().allowance.heldMicrorubles, '200');
      await store.finish(admitted.id, { outcome: 'failed', noUpstreamDispatch: true });
      assert.equal((await status('money-race')).json().allowance.heldMicrorubles, '0');
    });

    await t.test('crash reconciliation releases never-dispatched holds, preserves unknown exposure and accepts late measured usage once', async () => {
      await ensure('crash-reconcile'); const account = await store.sotyAccount({ iss: ISSUER, accountId: 'crash-reconcile' });
      const key = await store.getChatKey(account.tenant_id);
      const args = { key, model: connector.models()[0], inputTokens: 100, outputTokens: 50, inputRate: '1000000', outputRate: '1000000', body: chat(), autoFallback: false };
      const neverDispatched = await store.reserve({ ...args, idempotencyKey: randomUUID() });
      await pool.query("UPDATE inference_requests SET lease_expires_at=now()-interval '1 minute' WHERE id=$1", [neverDispatched.id]);
      await store.sweep();
      const released = (await pool.query('SELECT status,quota_urub FROM inference_requests WHERE id=$1', [neverDispatched.id])).rows[0];
      assert.deepEqual(released, { status: 'abandoned', quota_urub: '0' });
      const idempotencyKey = randomUUID(); const dispatched = await store.reserve({ ...args, idempotencyKey });
      await store.admitDispatch({ key, reservation: dispatched, model: MODEL, commit: true });
      await pool.query("UPDATE inference_requests SET lease_expires_at=now()-interval '1 minute' WHERE id=$1", [dispatched.id]);
      await store.sweep();
      const uncertain = (await pool.query('SELECT status,quota_urub,quota_pending,charge_urub FROM inference_requests WHERE id=$1', [dispatched.id])).rows[0];
      assert.deepEqual(uncertain, { status: 'abandoned', quota_urub: '150', quota_pending: true, charge_urub: '0' });
      assert.equal((await store.reserve({ ...args, idempotencyKey })).replay, true);
      await store.recordAttempt({ logicalRequestId: dispatched.id, purpose: 'chat', providerId: 'first', model: MODEL, outcome: 'success', inputTokens: 5, outputTokens: 3, dispatched: true });
      await store.reconcileSotyUsage(dispatched.id);
      const settled = (await pool.query('SELECT status,quota_urub,quota_pending,charge_urub FROM inference_requests WHERE id=$1', [dispatched.id])).rows[0];
      assert.deepEqual(settled, { status: 'abandoned', quota_urub: '8', quota_pending: false, charge_urub: '8' });
      assert.equal((await pool.query('SELECT count(*) FROM ledger_transactions WHERE request_id=$1', [dispatched.id])).rows[0].count, '1');
      const budget = (await pool.query('SELECT s.actual_urub FROM service_budget_holds s JOIN inference_requests r ON r.service_budget_hold_id=s.id WHERE r.id=$1', [dispatched.id])).rows[0]; assert.equal(budget.actual_urub, '8');
      await ensure('crash-reconcile'); assert.equal((await status('crash-reconcile')).json().allowance.availableMicrorubles, '49999992');
    });

    await t.test('every fallback repeats Root authorization; revoked fallback never reaches the second provider', async () => {
      await ensure('fallback'); const before = received.length; const beforeCallbacks = callbacks.length;
      const result = await inference('fallback', chat('revoke-after-primary'));
      assert.equal(result.statusCode, 403, result.body); assert.equal(received.length, before + 1); assert.equal(callbacks.length, beforeCallbacks + 2);
      const account = await store.sotyAccount({ iss: ISSUER, accountId: 'fallback' });
      const attempts = (await pool.query('SELECT a.dispatched FROM provider_attempts a JOIN inference_requests r ON r.id=a.request_id WHERE r.tenant_id=$1 ORDER BY a.recorded_at', [account.tenant_id])).rows;
      assert.deepEqual(attempts.map(row => row.dispatched), [true, false]);
      assert.ok(BigInt((await status('fallback')).json().allowance.pendingMicrorubles) > 0n);
    });

    await t.test('permanent account cannot obtain demo credit; suspension survives ensure and queued key policy is rechecked', async () => {
      const account = await store.sotyAccount({ iss: ISSUER, accountId: B });
      await assert.rejects(store.topupDemo(account.tenant_id, '1000000', randomUUID()), { code: 'demo_credit_not_allowed' });
      await assert.rejects(pool.query("INSERT INTO ledger_transactions(id,tenant_id,operation,business_key,amount_urub) VALUES($1,$2,'demo_topup',$3,1000000)", [randomUUID(), account.tenant_id, randomUUID()]));
      const external = (await mutate(B, 'create', { name: 'Queued policy' })).json();
      const key = await store.authenticate(external.secret); const model = connector.models()[0];
      const reservation = await store.reserve({ key, model, inputTokens: 10, outputTokens: 5, inputRate: '1000000', outputRate: '1000000', body: chat(), autoFallback: false });
      await mutate(B, 'revoke', { keyId: key.id });
      await assert.rejects(store.admitDispatch({ key, reservation, model: MODEL }), { code: 'invalid_api_key' });
      await store.finish(reservation.id, { outcome: 'failed', noUpstreamDispatch: true });
      await pool.query("UPDATE soty_accounts SET state='suspended' WHERE tenant_id=$1", [account.tenant_id]);
      await ensure(B); assert.equal((await status(B)).json().state, 'suspended');
      const before = received.length; assert.equal((await inference(B)).statusCode, 403); assert.equal(received.length, before);
      assert.equal((await pool.query("SELECT count(*) FROM ledger_transactions WHERE tenant_id=$1 AND operation='sponsor_grant'", [account.tenant_id])).rows[0].count, '1');
    });

    await t.test('explicit service sponsor has its own permanent allowance and global exhaustion blocks it without a provider bypass', async () => {
      const serviceId = 'svc_synthetic_explicit_application_owner';
      const provisioned = await ensure(serviceId); assert.equal(provisioned.statusCode, 200);
      const result = await inference(serviceId, chat('service-app'), { deviceId: 'service:application:synthetic-app' }); assert.equal(result.statusCode, 200, result.body);
      const sponsor = await store.sotyAccount({ iss: ISSUER, accountId: serviceId }); const key = await store.getChatKey(sponsor.tenant_id);
      const constrained = new Store(pool, { pepper: store.pepper, dailyServiceBudgetMicrorubles: '1' });
      const before = received.length;
      await assert.rejects(constrained.reserve({ key, model: connector.models()[0], inputTokens: 10, outputTokens: 5, inputRate: '1000000', outputRate: '1000000', body: chat(), autoFallback: false }), { code: 'service_daily_budget_exhausted' });
      assert.equal(received.length, before);
      assert.equal((await constrained.sotyAccountStatus({ iss: ISSUER, accountId: serviceId }, { models: connector.models(), configured: true })).state, 'limited');
      assert.equal(store.dailyServiceBudget.toString(), '50000000');
    });

    await t.test('global operator budget remains unchanged and stopping active HTTP request produces no accepted late result', async () => {
      assert.equal(store.dailyServiceBudget.toString(), '50000000');
      await ensure('active-abort');
      const url = await app.listen({ host: '127.0.0.1', port: 0 });
      const call = inferenceInput('active-abort', chat('abort-active'));
      const controller = new AbortController();
      const pending = fetch(`${url}${call.input.url}`, { method: 'POST', headers: call.input.headers, body: call.input.payload, signal: controller.signal }).catch(error => error);
      await until(() => received.some(body => body.messages[0].content === 'abort-active'));
      controller.abort(); assert.equal((await pending).name, 'AbortError');
      await until(async () => {
        const account = await store.sotyAccount({ iss: ISSUER, accountId: 'active-abort' });
        return (await pool.query("SELECT count(*) FROM inference_requests WHERE tenant_id=$1 AND status='running'", [account.tenant_id])).rows[0].count === '0';
      });
      const allowance = (await status('active-abort')).json().allowance; assert.equal(allowance.heldMicrorubles, '0'); assert.ok(BigInt(allowance.pendingMicrorubles) > 0n);
      const global = (await pool.query('SELECT spent_urub,held_urub FROM service_daily_budget')).rows;
      assert.ok(global.every(row => BigInt(row.spent_urub) + BigInt(row.held_urub) <= store.dailyServiceBudget));
    });
  } finally {
    gateRelease?.();
    await app?.close();
    provider.closeAllConnections(); root.closeAllConnections();
    await Promise.all([new Promise(resolve => provider.close(resolve)), new Promise(resolve => root.close(resolve))]);
    await pool.end(); await bootstrap.query(`DROP SCHEMA ${schema} CASCADE`); await bootstrap.end();
  }
});
