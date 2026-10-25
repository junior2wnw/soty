import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import pg from 'pg';
import { migrate } from '../../apps/gateway/src/migrate.mjs';
import { Store } from '../../apps/gateway/src/store.mjs';
import { createApp } from '../../apps/gateway/src/app.mjs';
import { importLegacyKeys } from '../../apps/gateway/src/legacy-import.mjs';
import { DomainError, publicDigest, createSecret, fingerprint } from '../../packages/domain/index.mjs';

const database = process.env.TEST_DATABASE_URL;
const enabled = Boolean(database);
const origin = 'https://test.soty.invalid';
const model = { id: 'test/model', name: 'Test model', status: 'healthy', contextWindow: 8192, inputPriceMicrorublesPerMillion: '23000000', outputPriceMicrorublesPerMillion: '23000000' };

test('PostgreSQL persistence, scope isolation, exact accounting and API security', { skip: !enabled, timeout: 90000 }, async t => {
  const parsed = new URL(database);
  if (!parsed.pathname.endsWith('_test')) throw new Error('TEST_DATABASE_URL must name a dedicated *_test database');
  const schema = `test_${randomUUID().replaceAll('-', '')}`;
  const bootstrap = new pg.Pool({ connectionString: database, max: 2 });
  await bootstrap.query(`CREATE SCHEMA ${schema}`);
  const pool = new pg.Pool({ connectionString: database, max: 10, options: `-c search_path=${schema}` });
  let app; let totalCalls = 0; let failNext = false; let delayNext = 0; let usageNext = true;
  const pepper = 'test-only-pepper-for-isolated-database-64-chars-aaaaaaaaaaaaaaaa';
  try {
    await migrate(pool);
    const store = new Store(pool, { pepper, dailyServiceBudgetMicrorubles: '50000000' });
    const connector = {
      models: () => [model],
      async complete({ body, logicalRequestId, purpose }) {
        totalCalls++;
        const delay = delayNext; delayNext = 0;
        if (delay) await new Promise(resolve => setTimeout(resolve, delay));
        if (failNext) { failNext = false; await store.recordAttempt({ logicalRequestId, providerId: 'synthetic', model: model.id, purpose, outcome: 'error', errorCode: 'upstream_timeout', durationMs: 1 }); throw new DomainError('upstream_timeout', 'Synthetic timeout', 504); }
        const usage = usageNext ? { prompt_tokens: 10, completion_tokens: 5, total_tokens: 15 } : null; usageNext = true;
        await store.recordAttempt({ logicalRequestId, providerId: 'synthetic', model: model.id, purpose, outcome: 'success', inputTokens: usage?.prompt_tokens, outputTokens: usage?.completion_tokens, durationMs: 5 });
        const response = { id: 'chatcmpl-synthetic', object: 'chat.completion', model: model.id, choices: [{ index: 0, message: { role: 'assistant', content: 'Synthetic technical response' }, finish_reason: 'stop' }], usage };
        if (body.stream) {
          const frames = ['data: {"id":"synthetic","choices":[{"index":0,"delta":{"content":"Synthetic"},"finish_reason":null}]}\n\n', 'data: {"id":"synthetic","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n', 'data: [DONE]\n\n'];
          return { kind: 'stream', stream: new ReadableStream({ start(controller) { for (const frame of frames) controller.enqueue(new TextEncoder().encode(frame)); controller.close(); } }), completion: Promise.resolve({ outcome: 'success', usage }), model: model.id, providerId: 'synthetic' };
        }
        return { kind: 'json', body: response, usage, model: model.id, providerId: 'synthetic' };
      },
    };
    app = await createApp({ store, connector, baseUrl: origin, cookieSecure: true, installEnabled: true });
    async function session() {
      const response = await app.inject({ method: 'POST', url: '/api/session', headers: { origin } });
      assert.equal(response.statusCode, 200);
      assert.match(response.headers['set-cookie'], /HttpOnly; SameSite=Strict/); assert.match(response.headers['set-cookie'], /Secure/);
      return { cookie: response.headers['set-cookie'].split(';')[0], csrf: response.json().csrfToken };
    }
    const a = await session(); const b = await session();
    function headers(identity, extra = {}) { return { cookie: identity.cookie, 'x-csrf-token': identity.csrf, origin, ...extra }; }
    function control(identity, method, url, payload, extra = {}) { return app.inject({ method, url, payload, headers: headers(identity, extra) }); }
    async function createKey(identity, name = 'Test key', limits = {}) {
      const response = await control(identity, 'POST', '/api/keys', { name, limits }, { 'idempotency-key': randomUUID() });
      assert.equal(response.statusCode, 201); return response.json();
    }
    async function topup(identity, amount = '10000000', idempotency = randomUUID()) {
      return control(identity, 'POST', '/api/topups/demo', { amountMicrorubles: amount }, { 'idempotency-key': idempotency });
    }
    function completion(secret, body = {}, idempotency = randomUUID()) {
      return app.inject({ method: 'POST', url: '/v1/chat/completions', headers: { authorization: `Bearer ${secret}`, 'idempotency-key': idempotency }, payload: { model: model.id, messages: [{ role: 'user', content: 'Synthetic question' }], max_tokens: 64, ...body } });
    }

    let credential;
    await t.test('fresh workspaces have no fake activity or money; no cookie grants no access', async () => {
      const response = await control(a, 'GET', '/api/dashboard?days=7');
      assert.equal(response.statusCode, 200); const data = response.json();
      assert.equal(data.wallet.balanceMicrorubles, '0'); assert.equal(data.usage.requests, 0); assert.equal(data.usage.successRate, null); assert.equal(data.usage.periodDays, 7);
      assert.equal(data.usage.daily.length, 7); assert.equal(data.usage.daily.every(day => day.requests === 0 && day.spentMicrorubles === '0'), true);
      assert.equal(data.usage.daily[0].date, data.usage.periodStart); assert.equal(data.usage.daily.at(-1).date, data.usage.periodEnd);
      assert.equal((await app.inject({ url: '/api/dashboard' })).statusCode, 401);
    });

    await t.test('CSRF and cross-origin mutations cannot issue credentials', async () => {
      assert.equal((await app.inject({ method: 'POST', url: '/api/keys', headers: { cookie: a.cookie, origin }, payload: { name: 'blocked' } })).statusCode, 403);
      assert.equal((await control(a, 'POST', '/api/keys', { name: 'blocked' }, { origin: 'https://evil.invalid', 'idempotency-key': randomUUID() })).statusCode, 403);
      assert.equal((await app.inject({ method: 'POST', url: '/api/session', headers: { origin: 'https://evil.invalid' } })).statusCode, 403);
    });

    await t.test('create key exactly once; peer cannot inspect, mutate, rotate or revoke it', async () => {
      const idem = randomUUID();
      const first = await control(a, 'POST', '/api/keys', { name: 'Private key' }, { 'idempotency-key': idem });
      assert.equal(first.statusCode, 201); credential = first.json(); assert.ok(credential.secret);
      const second = await control(a, 'POST', '/api/keys', { name: 'Private key' }, { 'idempotency-key': idem });
      assert.equal(second.statusCode, 201); assert.equal(second.json().secret, null); assert.equal(second.json().key.id, credential.key.id);
      assert.equal((await control(a, 'POST', '/api/keys', { name: 'Different body' }, { 'idempotency-key': idem })).statusCode, 409);
      assert.equal((await control(b, 'GET', '/api/keys')).json().keys.length, 0);
      for (const method of ['PATCH', 'DELETE']) assert.equal((await control(b, method, `/api/keys/${credential.key.id}`, method === 'PATCH' ? { name: 'Hijack' } : undefined)).statusCode, 404);
      assert.equal((await control(b, 'POST', `/api/keys/${credential.key.id}/rotate`, {}, { 'idempotency-key': randomUUID() })).statusCode, 404);
      assert.equal((await control(b, 'GET', `/api/connect/profile?model=${encodeURIComponent(model.id)}&keyId=${credential.key.id}`)).statusCode, 404);
      const rows = await pool.query('SELECT fingerprint FROM credentials WHERE key_id=$1', [credential.key.id]);
      assert.match(rows.rows[0].fingerprint, /^[a-f0-9]{64}$/); assert.notEqual(rows.rows[0].fingerprint, publicDigest(credential.secret));
    });

    await t.test('demo credit is explicit and idempotent; no balance blocks inference before dispatch', async () => {
      const before = totalCalls;
      assert.equal((await completion(credential.secret)).statusCode, 402); assert.equal(totalCalls, before);
      const idem = randomUUID(); const first = await topup(a, '10000000', idem); const second = await topup(a, '10000000', idem);
      assert.equal(first.statusCode, 200); assert.equal(first.json().isDemo, true); assert.equal(second.json().replayed, true);
      assert.equal((await control(a, 'GET', '/api/dashboard')).json().wallet.balanceMicrorubles, '10000000');
      assert.equal((await control(b, 'GET', '/api/dashboard')).json().wallet.balanceMicrorubles, '0');
    });

    let completedRequest;
    await t.test('reserve settles exactly once and encrypted idempotency replay performs no new inference', async () => {
      const idem = randomUUID(); const before = totalCalls;
      const result = await completion(credential.secret, {}, idem);
      assert.equal(result.statusCode, 200); assert.equal(totalCalls, before + 1); assert.equal(result.headers['x-soty-cost-microrubles'], '345');
      completedRequest = result.headers['x-soty-request-id'];
      const duplicate = await completion(credential.secret, {}, idem);
      assert.equal(duplicate.statusCode, 200); assert.equal(totalCalls, before + 1); assert.equal(duplicate.headers['x-idempotent-replay'], 'true');
      assert.equal((await completion(credential.secret, { messages: [{ role: 'user', content: 'Different' }] }, idem)).statusCode, 409);
      const dash = (await control(a, 'GET', '/api/dashboard')).json();
      assert.equal(dash.wallet.balanceMicrorubles, '9999655'); assert.equal(dash.wallet.reservedMicrorubles, '0');
      assert.equal(dash.usage.tokens, 15); assert.equal(dash.usage.requests, 1); assert.equal(dash.usage.successRate, 100);
      assert.equal(dash.usage.daily.reduce((sum, day) => sum + day.tokens, 0), dash.usage.tokens);
      assert.equal(dash.usage.daily.reduce((sum, day) => sum + BigInt(day.spentMicrorubles), 0n).toString(), dash.usage.spentMicrorubles);
      const repeatSettlement = await store.finish(completedRequest, { outcome: 'completed', usage: { prompt_tokens: 10, completion_tokens: 5 } }); assert.equal(repeatSettlement.duplicate, true);
      assert.equal((await pool.query('SELECT count(*) FROM ledger_transactions WHERE request_id=$1', [completedRequest])).rows[0].count, '1');
      const cache = (await pool.query('SELECT encrypted FROM response_cache WHERE request_id=$1', [completedRequest])).rows[0];
      assert.equal(cache.encrypted.includes(Buffer.from('Synthetic technical response')), false);
      assert.equal((await control(b, 'GET', `/api/requests/${completedRequest}`)).statusCode, 404);
      assert.equal((await control(a, 'GET', `/api/requests/${completedRequest}`)).statusCode, 200);
      assert.equal((await control(b, 'GET', '/api/usage/export')).body.includes(completedRequest), false);
    });

    await t.test('failure releases customer hold while conservatively retaining upstream service expense', async () => {
      const before = (await control(a, 'GET', '/api/dashboard')).json().wallet.balanceMicrorubles;
      failNext = true; assert.equal((await completion(credential.secret)).statusCode, 504);
      const after = (await control(a, 'GET', '/api/dashboard')).json().wallet;
      assert.equal(after.balanceMicrorubles, before); assert.equal(after.reservedMicrorubles, '0');
    });

    await t.test('zero budgets deny, concurrent requests cannot overspend one slot, pause/revoke take effect', async () => {
      const zero = await createKey(a, 'Zero budget', { dailyMicrorubles: '0' });
      assert.equal((await completion(zero.secret)).json().error.code, 'daily_budget_limit');
      const single = await createKey(a, 'Single slot', { concurrency: 1 }); delayNext = 80;
      const results = await Promise.all([completion(single.secret), completion(single.secret)]);
      assert.deepEqual(results.map(response => response.statusCode).sort(), [200, 429]);
      assert.equal((await control(a, 'PATCH', `/api/keys/${single.key.id}`, { status: 'paused' })).statusCode, 200);
      assert.equal((await completion(single.secret)).statusCode, 401);
      await control(a, 'PATCH', `/api/keys/${single.key.id}`, { status: 'active' });
      await control(a, 'DELETE', `/api/keys/${single.key.id}`);
      assert.equal((await completion(single.secret)).statusCode, 401);
      assert.equal((await control(a, 'PATCH', `/api/keys/${single.key.id}`, { status: 'active' })).statusCode, 409);
    });

    await t.test('installer activation cannot cross workspaces and does not create duplicate keys', async () => {
      const installer = await control(a, 'POST', '/api/installations', { model: model.id });
      assert.equal(installer.statusCode, 200); assert.match(installer.headers['content-disposition'], /OpenCode-Soty.cmd/);
      const ps = Buffer.from(installer.body.slice(installer.body.lastIndexOf(':SOTY_PAYLOAD') + 13).trim(), 'base64').toString('utf8');
      const encoded = /FromBase64String\('([A-Za-z0-9+/=]+)'\)/.exec(ps)?.[1];
      assert.ok(encoded);
      const { claim } = JSON.parse(Buffer.from(encoded, 'base64').toString('utf8'));
      const installationNonce = randomUUID();
      const redeem = await app.inject({ method: 'POST', url: '/setup/redeem', payload: { claim, installationNonce } });
      assert.equal(redeem.statusCode, 200);
      const delivered = redeem.json();
      const pending = { secret: delivered.secret, key: { id: delivered.keyId } };
      const repeated = await app.inject({ method: 'POST', url: '/setup/redeem', payload: { claim, installationNonce } });
      assert.equal(repeated.statusCode, 200); assert.equal(repeated.json().secret, null); assert.equal(repeated.json().keyId, pending.key.id);
      assert.equal((await app.inject({ method: 'POST', url: '/setup/redeem', payload: { claim, installationNonce: randomUUID() } })).statusCode, 409);
      assert.equal((await completion(pending.secret)).statusCode, 401);
      assert.equal((await control(b, 'GET', '/api/keys')).json().keys.some(key => key.id === pending.key.id), false);
      const wrong = await app.inject({ method: 'POST', url: '/setup/activate', headers: { authorization: `Bearer ${credential.secret}` }, payload: { keyId: pending.key.id } }); assert.equal(wrong.statusCode, 401);
      const activate = await app.inject({ method: 'POST', url: '/setup/activate', headers: { authorization: `Bearer ${pending.secret}` }, payload: { keyId: pending.key.id } });
      assert.equal(activate.statusCode, 200); assert.equal(activate.json().inferenceVerified, false);
      assert.equal((await app.inject({ url: '/v1/models', headers: { authorization: `Bearer ${pending.secret}` } })).statusCode, 200);
    });

    await t.test('rotation preserves key identity and cumulative budget; null policy does not reset recorded usage', async () => {
      const before = (await control(a, 'GET', '/api/keys')).json().keys.find(key => key.id === credential.key.id);
      const rotated = await control(a, 'POST', `/api/keys/${credential.key.id}/rotate`, {}, { 'idempotency-key': randomUUID() });
      assert.equal(rotated.statusCode, 200); assert.equal(rotated.json().key.id, credential.key.id);
      assert.equal((await completion(rotated.json().secret)).statusCode, 200);
      assert.equal((await app.inject({ url: '/v1/models', headers: { authorization: `Bearer ${credential.secret}` } })).statusCode, 200);
      const after = (await control(a, 'GET', '/api/keys')).json().keys.find(key => key.id === credential.key.id);
      assert.equal(BigInt(after.spentMicrorubles), BigInt(before.spentMicrorubles) + 345n);
      const shared = (await pool.query('SELECT count(*) FROM credentials WHERE key_id=$1', [credential.key.id])).rows[0]; assert.equal(shared.count, '2');
    });

    await t.test('successful SSE finishes once and usage without proof never charges the customer', async () => {
      const streamed = await completion(credential.secret, { stream: true });
      assert.equal(streamed.statusCode, 200); assert.match(streamed.headers['content-type'], /text\/event-stream/); assert.match(streamed.body, /\[DONE\]/);
      await new Promise(resolve => setTimeout(resolve, 30));
      usageNext = false;
      const before = (await control(a, 'GET', '/api/dashboard')).json().wallet.balanceMicrorubles;
      const result = await completion(credential.secret); assert.equal(result.statusCode, 200);
      assert.equal(result.headers['x-soty-cost-microrubles'], '0');
      assert.equal((await control(a, 'GET', '/api/dashboard')).json().wallet.balanceMicrorubles, before);
    });

    await t.test('session, credentials and measured statistics survive a fresh Store instance', async () => {
      const fresh = new Store(pool, { pepper });
      const session = await fresh.getSession(a.cookie.slice(a.cookie.indexOf('=') + 1));
      assert.ok(session.tenantId);
      assert.equal((await fresh.authenticate(credential.secret)).id, credential.key.id);
      assert.ok((await fresh.listKeys(session.tenantId)).some(key => BigInt(key.spentMicrorubles) > 0n));
    });

    await t.test('JSON and actual tool qualifications persist for 24 hours without restoring a fake health observation', async () => {
      const hooks = store.connectorHooks();
      const observation = { providerId: 'qualification-test', purpose: 'synthetic-test', outcome: 'success', durationMs: 5 };
      await hooks.saveObservation({ ...observation, model: 'test/json-qualified', profile: 'json_object' });
      await hooks.saveObservation({ ...observation, model: 'test/tools-qualified', profile: 'tools', toolsVerified: true });
      await hooks.saveObservation({ ...observation, model: 'test/tools-with-text-only', profile: 'tools', toolsVerified: false });
      await hooks.saveObservation({ ...observation, model: 'test/json-failed', profile: 'json_object', outcome: 'error' });
      await hooks.saveObservation({ ...observation, model: 'test/json-expired', profile: 'json_object' });
      await pool.query("UPDATE health_observations SET recorded_at=now()-interval '25 hours' WHERE provider_id='qualification-test' AND model='test/json-expired'");
      const qualifications = (await new Store(pool, { pepper }).connectorHooks().loadQualifications()).filter(row => row.providerId === 'qualification-test');
      assert.equal(qualifications.length, 2);
      const json = qualifications.find(row => row.model === 'test/json-qualified');
      const tools = qualifications.find(row => row.model === 'test/tools-qualified');
      assert.ok(Number.isSafeInteger(json.jsonVerifiedAt)); assert.equal(json.toolsVerifiedAt, null);
      assert.ok(Number.isSafeInteger(tools.toolsVerifiedAt)); assert.equal(tools.jsonVerifiedAt, null);
      assert.ok(qualifications.every(row => !Object.hasOwn(row, 'checkedAt') && !Object.hasOwn(row, 'status')));
      const stored = (await pool.query("SELECT profile,tools_verified FROM health_observations WHERE provider_id='qualification-test' AND model='test/tools-qualified'")).rows[0];
      assert.equal(stored.profile, 'tools'); assert.equal(stored.tools_verified, true);
    });

    await t.test('legacy HMAC and legacy SHA-256 remain limited to their original endpoints and bypass demo wallets', async () => {
      const applicationSecret = createSecret('legacy-application-'); const agentSecret = createSecret('legacy-agent-');
      const directory = await mkdtemp(join(tmpdir(), 'soty-legacy-test-')); const filename = join(directory, 'fingerprints.json');
      try {
        await writeFile(filename, JSON.stringify({ keys: [
          { name: 'Legacy app', legacyId: 'test-app', fingerprint: fingerprint(applicationSecret, pepper), fingerprintAlgorithm: 'hmac_sha256', allowedPaths: ['/v1', '/api/inference/v1'] },
          { name: 'Legacy agent', legacyId: 'test-agent', fingerprint: publicDigest(agentSecret), fingerprintAlgorithm: 'legacy_sha256', allowedPaths: ['/api/connectors/gonka/v1'] },
        ] }));
        assert.equal((await importLegacyKeys(store, filename)).imported, 2);
        assert.equal((await importLegacyKeys(store, filename)).unchanged, 2);
        assert.equal((await app.inject({ url: '/api/inference/v1/models', headers: { authorization: `Bearer ${applicationSecret}` } })).statusCode, 200);
        assert.equal((await app.inject({ url: '/api/connectors/gonka/v1/models', headers: { authorization: `Bearer ${applicationSecret}` } })).statusCode, 403);
        assert.equal((await app.inject({ url: '/api/connectors/gonka/v1/models', headers: { authorization: `Bearer ${agentSecret}` } })).statusCode, 200);
        assert.equal((await app.inject({ url: '/v1/models', headers: { authorization: `Bearer ${agentSecret}` } })).statusCode, 403);
        const result = await completion(applicationSecret); assert.equal(result.statusCode, 200); assert.equal(result.headers['x-soty-cost-microrubles'], '0');
        assert.equal((await control(a, 'GET', '/api/keys')).json().keys.some(key => key.legacy), false);
      } finally { await rm(directory, { recursive: true, force: true }); }
    });

    await t.test('append-only balanced ledger rejects destructive or unbalanced writes', async () => {
      await assert.rejects(pool.query('UPDATE ledger_entries SET amount_urub=0'), /append-only record/);
      await assert.rejects(pool.query('DELETE FROM ledger_transactions'), /append-only record/);
      await assert.rejects(store.transaction(async client => {
        await client.query("INSERT INTO ledger_transactions(id,tenant_id,operation,business_key,amount_urub) VALUES($1,$2,'demo_topup',$3,1)", [randomUUID(), await awaitTenant(a), randomUUID()]);
      }), /unbalanced ledger transaction/);
      const posted = (await pool.query('SELECT id FROM ledger_transactions LIMIT 1')).rows[0].id;
      await assert.rejects(pool.query("INSERT INTO ledger_entries(transaction_id,account,amount_urub) VALUES($1,'wallet',1)", [posted]), /unbalanced ledger transaction/);
      await assert.rejects(pool.query('UPDATE wallets SET balance_urub=balance_urub+1 WHERE tenant_id=$1', [await awaitTenant(a)]), /wallet projection differs from ledger/);
    });

    await t.test('global service budget is independent of demo money, key count and browser identity', async () => {
      const before = totalCalls; const previous = store.dailyServiceBudget;
      try {
        store.dailyServiceBudget = 1n;
        assert.equal((await topup(b, '500000000')).statusCode, 200);
        const another = await createKey(b, 'Additional browser key');
        const first = await completion(credential.secret); const second = await completion(another.secret);
        assert.equal(first.json().error.code, 'service_daily_budget_exhausted');
        assert.equal(second.json().error.code, 'service_daily_budget_exhausted');
        assert.equal(totalCalls, before);
      } finally { store.dailyServiceBudget = previous; }
    });

    await t.test('monitoring subcap cannot consume customer headroom or inherit customer spend', async () => {
      const hooks = store.connectorHooks();
      assert.equal(await hooks.loadDailyProbeSpend(), '0');
      const reservation = await hooks.reserveProbeSpend('10000000'); assert.ok(reservation?.id);
      assert.equal(await hooks.loadDailyProbeSpend(), '10000000');
      assert.equal(await hooks.reserveProbeSpend('1'), null);
      await hooks.settleProbeSpend(reservation.id, '10000000');
      assert.equal(await hooks.reserveProbeSpend('1'), null);
      assert.equal((await completion(credential.secret)).statusCode, 200);
      assert.equal(await hooks.loadDailyProbeSpend(), '10000000');
    });

    await t.test('public route preserves strict protocol boundaries and payload limit', async () => {
      const unauth = await app.inject({ url: '/v1/models' }); assert.equal(unauth.statusCode, 401);
      const wrongScope = await app.inject({ url: '/api/connectors/gonka/v1/models', headers: { authorization: `Bearer ${credential.secret}` } }); assert.equal(wrongScope.statusCode, 403);
      const tooLarge = await completion(credential.secret, { messages: [{ role: 'user', content: 'x'.repeat(1_048_577) }] }); assert.equal(tooLarge.statusCode, 413);
      const unsupported = await app.inject({ method: 'POST', url: '/v1/responses', headers: { authorization: `Bearer ${credential.secret}` }, payload: {} }); assert.equal(unsupported.statusCode, 501);
    });

    async function awaitTenant(identity) { return (await store.getSession(identity.cookie.slice(identity.cookie.indexOf('=') + 1))).tenantId; }
  } finally {
    await app?.close(); await pool.end();
    await bootstrap.query(`DROP SCHEMA ${schema} CASCADE`); await bootstrap.end();
  }
});
