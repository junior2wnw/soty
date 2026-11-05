import test from 'node:test';
import assert from 'node:assert/strict';
import { join } from 'node:path';
import { Client, StreamableHTTPClientTransport } from '@modelcontextprotocol/client';
import { createHttpApp } from '../http-app.js';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';
import { createMonitoringCapabilities } from '../../modules/monitoring/capabilities.mjs';
import { MonitoringError } from '../../modules/monitoring/server/validation.mjs';
import { AccessError } from '../../modules/capabilities/server/validation.mjs';
import { createCatalog } from '../../modules/capabilities/server/catalog.mjs';

const revision = '2026-07-28', scope = Object.freeze({ kind: 'platform' });
const capabilities = ['monitoring.describe', 'monitoring.query', 'monitoring.config.put'];

async function fixture(t) {
  const operators = [];
  const f = await nativeHttpFixture(t, { enabled: false, appFactory: (dist, options) => createHttpApp(dist, {
    ...options, env: {}, appHosting: {}, monitoring: { storageMode: 'writer', platformPolicy: {
      readAccountIds: [...operators], configureAccountIds: [...operators], adminAccountIds: [...operators],
    } },
  }) });
  const owner = nativeIdentity('MCP actual monitoring operator'), outsider = nativeIdentity('MCP actual monitoring outsider');
  const own = await f.bootstrap(owner), other = await f.bootstrap(outsider); operators.push(own.accountId);
  await f.restart({ enabled: false });
  const call = (actor, op, args = {}) => f.call(actor, op, { expectedAccountId: actor === owner ? own.accountId : other.accountId, ...args });
  async function grant(actor, { configure = true } = {}) {
    const principal = good(await call(actor, 'access.principals.create', { label: 'Monitoring transport2.2', clientLabel: 'Owned synthetic MCP consumer' })).principal;
    const allowed = good(await call(actor, 'access.grants.issue', { principalId: principal.id,
      capabilities: capabilities.filter(capabilityId => configure || capabilityId !== 'monitoring.config.put').map(capabilityId => ({ capabilityId, version: 1 })),
      resources: ['monitoring', 'monitoring:platform'], effects: configure ? ['update'] : [], recipients: ['soty:monitoring'],
      expiresAt: Date.now() + 600000, allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit: 20 },
    })).grant;
    const credential = good(await call(actor, 'access.credentials.issue', { grantId: allowed.id, audience: f.origin + '/mcp' }));
    return { grantId: allowed.id, token: credential.token };
  }
  return { f, owner, outsider, own, other, call, grant };
}

function success(value) {
  assert.notEqual(value.isError, true, value.content?.[0]?.text);
  assert.equal(value.content.length, 1); assert.equal(value.content[0].type, 'text');
  assert.deepEqual(JSON.parse(value.content[0].text), value.structuredContent);
  return value.structuredContent;
}

async function connection(t, f, token) {
  const observations = [];
  const client = new Client({ name: 'monitoring-independent-sdk22', version: '1' }, {
    capabilities: {}, supportedProtocolVersions: [revision], versionNegotiation: { mode: { pin: revision } },
  });
  const transport = new StreamableHTTPClientTransport(new URL(f.origin + '/mcp'), {
    requestInit: { headers: { authorization: 'Bearer ' + token } },
    fetch: async (url, options) => {
      assert.equal(new URL(url).origin, f.origin);
      const response = await fetch(url, options);
      let method = null, name = null;
      if (typeof options?.body === 'string') { const message = JSON.parse(options.body); method = message.method; name = message.params?.name ?? null; }
      observations.push({ method, name, status: response.status, revision: new Headers(options?.headers).get('mcp-protocol-version') });
      return response;
    },
  });
  client.onerror = () => {}; // No raw headers/body/credential enters diagnostics.
  await client.connect(transport, { timeout: 5000 }); assert.equal(transport.protocolVersion, revision);
  let closed = false;
  const close = async () => { if (!closed) { closed = true; await client.close(); } };
  t.after(close);
  return { client, observations, close, call: (name, args) => client.callTool({ name, arguments: args }, { timeout: 5000 }) };
}

function errorCode(value) {
  const payload = value?.content?.[0]?.type === 'text' ? JSON.parse(value.content[0].text) : null;
  return payload?.error?.code ?? null;
}

async function denied(peer, name, args) {
  let value, error; const start = peer.observations.length;
  try { value = await peer.call(name, args); } catch (failure) { error = failure; }
  const observation = peer.observations.slice(start).findLast(row => row.method === 'tools/call' && row.name === name);
  assert.ok(observation, 'an observed new HTTP exchange is required for a denial');
  assert.equal(observation.name, name, 'observe a real completed tools/call response');
  if ([401, 403].includes(observation.status)) { assert.ok(error || value?.isError); return { status: observation.status, code: null }; }
  assert.equal(observation.status, 200); assert.equal(value?.isError, true, 'a tool-level denial must never export data');
  assert.equal(error, undefined); return { status: 200, code: errorCode(value) };
}

test('actual MCPClient2.2 uses signed monitoring grants, domain CAS/receipts, SQL reopen and current revocation/outsider denial', { timeout: 45000 }, async t => {
  const { f, owner, outsider, own, other, call, grant } = await fixture(t), file = join(f.directory, 'monitoring', 'monitoring.sqlite');
  const at = (Math.floor(Date.now() / 60000) - 2) * 60000 + 1000;
  good(await call(owner, 'monitoring.metric.put', { scope, requestId: 'mcp-definition', id: 'custom.mcp', kind: 'counter', unit: 'count' }));
  for (const [index, value] of [3, 7].entries()) good(await call(owner, 'monitoring.metric.record', { scope, eventId: 'mcp-event-' + index, metricId: 'custom.mcp', value, observedAt: at }));
  const ownedGrant = await grant(owner), foreignGrant = await grant(outsider);
  let peer = await connection(t, f, ownedGrant.token);
  const listed = success(await peer.call('apps_catalog_search', { query: 'monitoring', limit: 20 }));
  const reference = id => { const value = listed.items.find(row => row.reference.capabilityId === id)?.reference; assert.ok(value, 'discover the installed granted contract'); return value; };
  const described = success(await peer.call('apps_query', { reference: reference('monitoring.describe'), idempotencyKey: 'mcp-describe-real', input: { scope, paramsJson: '{}' } }));
  assert.equal(JSON.parse(described.result.dataJson).config.enabled, true);
  const query = id => ({ reference: reference('monitoring.query'), idempotencyKey: id, input: { scope,
    paramsJson: JSON.stringify({ metricIds: ['custom.mcp'], from: at - 1000 - 60000, to: at - 1000 + 60000, maxPoints: 2 }) } });
  const data = JSON.parse(success(await peer.call('apps_query', query('mcp-history-real'))).result.dataJson);
  assert.equal(data.series[0].unit, 'count'); assert.deepEqual(data.series[0].points.map(point => point.value), [null, 10]);
  const command = { reference: reference('monitoring.config.put'), idempotencyKey: 'mcp-original-disable', input: { scope, paramsJson: JSON.stringify({ expectedRevision: 0, enabled: false }) } };
  const changed = success(await peer.call('apps_invoke', command));
  assert.equal(changed.invocation.status, 'succeeded'); assert.equal(changed.invocation.effectState, 'committed');
  assert.equal(success(await peer.call('apps_invoke', command)).reused, true);
  const database = () => f.sql(file, db => ({
    config: { ...db.prepare("SELECT enabled,revision FROM monitoring_configs WHERE scope_key='platform'").get() },
    data: { ...db.prepare("SELECT sum(sum) AS value,sum(samples) AS samples FROM monitoring_buckets WHERE scope_key='platform' AND metric_id='custom.mcp'").get() },
    audit: db.prepare("SELECT count(*) AS n FROM monitoring_audit WHERE scope_key='platform' AND operation='collection.setEnabled'").get().n,
    registryId: db.prepare('SELECT registry_id FROM monitoring_meta').get().registry_id,
  }));
  const before = database(); assert.deepEqual(before.config, { enabled: 0, revision: 1 }); assert.deepEqual(before.data, { value: 10, samples: 2 }); assert.equal(before.audit, 1);
  const readGrant = await grant(owner, { configure: false }), readPeer = await connection(t, f, readGrant.token);
  assert.equal(JSON.parse(success(await readPeer.call('apps_query', query('mcp-read-only-read'))).result.dataJson).config.enabled, false);
  const readOnlyReply = await denied(readPeer, 'apps_invoke', { ...command, idempotencyKey: 'read-only-cannot-configure',
    input: { scope, paramsJson: JSON.stringify({ expectedRevision: 1, enabled: true }) } });
  assert.ok([401, 403].includes(readOnlyReply.status) || readOnlyReply.code === 'access_denied', 'read-only grant does not inherit the human owner configure right');
  assert.deepEqual(database(), before); await readPeer.close();
  await peer.close(); await f.restart({ enabled: false }); peer = await connection(t, f, ownedGrant.token);
  assert.equal(success(await peer.call('apps_invoke', command)).reused, true, 'original command after restart does not issue another mutation');
  const reopened = JSON.parse(success(await peer.call('apps_query', query('mcp-after-reopen'))).result.dataJson);
  assert.equal(reopened.config.enabled, false); assert.deepEqual(reopened.series[0].points.map(point => point.value), [null, 10]);
  assert.deepEqual(database(), before);
  const foreignPeer = await connection(t, f, foreignGrant.token);
  const outsiderReply = await denied(foreignPeer, 'apps_query', query('mcp-foreign-scope'));
  good(await call(owner, 'access.grants.revoke', { grantId: ownedGrant.grantId }));
  const revokedReply = await denied(peer, 'apps_query', query('mcp-revoked-grant'));
  t.diagnostic(JSON.stringify({ readOnlyReply, outsiderReply, revokedReply }));
  assert.ok([401, 403].includes(revokedReply.status), 'fresh Root credential validation observes the real revocation');
  assert.ok([401, 403].includes(outsiderReply.status) || outsiderReply.code === 'access_denied', 'an authenticated outsider receives an authorization denial, not an internal error');
  assert.deepEqual(database(), before); assert.notEqual(own.accountId, other.accountId);
  assert.ok(peer.observations.filter(row => row.method === 'tools/call').every(row => row.revision === revision));
});

test('the capability boundary translates only the actual typed permission failure and preserves other fault identity', () => {
  let failure = Object.assign(new Error('synthetic typed-code collision'), { code: 'monitoring_forbidden' });
  // This is a fault-only boundary unit, not a grant/Root fixture. The fence
  // always throws and must never authorize or execute a service callback.
  const adapter = createMonitoringCapabilities({ storageMode: 'reader', service: {},
    connect: () => ({ withAuthorityFence() { throw failure; } }), getCapabilities: () => ({}),
    inAgent() { assert.fail('a failed fence cannot enter the agent/service callback'); },
  });
  const entry = createCatalog(adapter.catalog).get('monitoring.describe', 1);
  const request = { actor: Object.freeze({ accountId: 'fault-only', deviceId: 'fault-only' }), name: 'apps_query',
    args: { reference: { capabilityId: entry.capabilityId, version: entry.version, digest: entry.digest }, input: { scope, paramsJson: '{}' } } };
  const original = failure; assert.throws(() => adapter.beforeCall(request), error => error === original);
  failure = new MonitoringError('monitoring_forbidden', 403);
  assert.throws(() => adapter.beforeCall(request), error => error instanceof AccessError && error.code === 'access_denied');
  failure = new MonitoringError('monitoring_unavailable', 503);
  const unavailable = failure; assert.throws(() => adapter.beforeCall(request), error => error === unavailable);
});
