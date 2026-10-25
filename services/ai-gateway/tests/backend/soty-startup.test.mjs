import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID, generateKeyPairSync } from 'node:crypto';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve, sep, basename } from 'node:path';
import pg from 'pg';
import { startServer } from '../../apps/gateway/src/server.mjs';
import { createBridgeSigner } from '../../packages/soty-bridge/index.mjs';

test('complete imported runtime starts with mounted bridge config and no paid provider, then automatically provisions an account', { skip: !process.env.TEST_DATABASE_URL, timeout: 90000 }, async () => {
  const database = new URL(process.env.TEST_DATABASE_URL);
  if (!database.pathname.endsWith('_test')) throw new Error('Dedicated *_test database required');
  const schema = `startup_test_${randomUUID().replaceAll('-', '')}`;
  const pool = new pg.Pool({ connectionString: database.href });
  const directory = await mkdtemp(join(tmpdir(), 'soty-startup-'));
  let runtime;
  try {
    await pool.query(`CREATE SCHEMA ${schema}`);
    database.searchParams.set('options', `-c search_path=${schema}`);
    const root = generateKeyPairSync('ed25519'); const gateway = generateKeyPairSync('ed25519');
    const path = join(directory, 'bridge.json');
    await writeFile(path, JSON.stringify({ schema: 'soty.ai-gateway-bridge.v1', audience: 'urn:soty:ai-gateway:v1', issuers: [{ issuer: 'https://4-2.xn--p1ai', kid: 'root', publicKey: root.publicKey.export({ type: 'spki', format: 'pem' }) }], signing: { issuer: 'urn:soty:ai-gateway:production', kid: 'gateway', privateKey: gateway.privateKey.export({ type: 'pkcs8', format: 'pem' }) }, root: { origin: 'http://soty-online-chat:8080', audience: 'urn:soty:root-ai-dispatch:v1', allowPrivateHttp: true } }), { mode: 0o600 });
    runtime = await startServer({ DATABASE_URL: database.href, API_KEY_PEPPER: 'startup-synthetic-pepper-at-least-thirty-two-bytes', SOTY_BRIDGE_CONFIG_FILE: path, HOST: '127.0.0.1', PORT: '0', ADMIN_HOST: '127.0.0.1', ADMIN_PORT: '0', PUBLIC_BASE_URL: 'http://127.0.0.1', COOKIE_SECURE: 'false', CONNECTOR_LIVE_ENABLED: 'false', HEALTH_PROBES_ENABLED: 'false', SERVICE_DAILY_BUDGET_URUB: '50000000' });
    assert.equal((await runtime.publicApp.inject('/healthz')).statusCode, 200);
    assert.equal((await runtime.publicApp.inject('/readyz')).statusCode, 200);
    const signer = createBridgeSigner({ issuer: 'https://4-2.xn--p1ai', audience: 'urn:soty:ai-gateway:v1', kid: 'root', privateKey: root.privateKey });
    const route = '/internal/soty/v1/accounts/ensure';
    const response = await runtime.publicApp.inject({ method: 'POST', url: route, payload: '{}', headers: { 'content-type': 'application/json', ...signer.headers({ method: 'POST', path: route, body: '{}', operation: 'account.ensure', accountId: 'startup-account', deviceId: 'service:provisioner', requestId: randomUUID() }) } });
    assert.equal(response.statusCode, 200); assert.equal(response.json().state, 'unavailable');
    assert.equal(response.json().allowance.grantedMicrorubles, '50000000'); assert.equal(runtime.connector.snapshot().configured, false);
    assert.equal(runtime.store.dailyServiceBudget.toString(), '50000000');
  } finally {
    await runtime?.close();
    if (!resolve(directory).startsWith(`${resolve(tmpdir())}${sep}`) || !basename(directory).startsWith('soty-startup-')) throw new Error('Unexpected synthetic config directory');
    await rm(directory, { recursive: true, force: true });
    await pool.query(`DROP SCHEMA ${schema} CASCADE`); await pool.end();
  }
});
