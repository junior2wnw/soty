import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { mkdtempSync, rmSync, realpathSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, basename } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createAppsService } from '../../apps/server/index.mjs';
import { connectorKey } from '../../apps/server/protocol.mjs';
import { createCapabilitiesService } from '../../capabilities/server/index.mjs';
import { createTrustedReadonlyQueryAdapter } from '../../capabilities/server/readonly-queries.mjs';
import { AccessError } from '../../capabilities/server/validation.mjs';
import { createAppAgentDirectory } from '../server/index.mjs';
import { createAppAgentDescriptor } from '../sdk.mjs';
import { createDescriptor } from '../../app-contract/sdk.mjs';
import { captureExternalApplications, composeExternalApplications } from '../../../server/external-applications.js';
import { createAppAgentDirectoryTransport } from '../../../server/app-agent-directory-transport.js';
import { createAppAgentDirectoryAuthority } from '../../../server/app-agent-directory-authority.js';

const owner = { accountId: 'network-owner', deviceId: 'network-device' }, audience = 'https://root.example/mcp';
const object = properties => ({ type: 'object', properties, required: Object.keys(properties), additionalProperties: false });
const ref = id => ({ id, version: 1, digest: 'a'.repeat(64) });
async function fixture(t) {
  const parent = realpathSync(tmpdir()), directory = mkdtempSync(join(parent, 'soty-directory-network-'));
  let apps, caps, discovery, sourceCurrent = true, active = true, calls = 0, hold = false, release, reached;
  const sockets = new Set();
  const server = createServer((req, res) => {
    if (req.url !== '/read' || req.method !== 'GET' || req.headers.authorization !== 'Bearer synthetic-source-fixture') { res.writeHead(403).end(); return; }
    calls++; const reply = () => { if (res.writableEnded || res.destroyed) return; res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ count: 7 })); };
    if (hold) { reached?.(); release = reply; } else reply();
  });
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  const dbs = [];
  t.after(async () => { release?.(); discovery?.close(); caps?.close(); apps?.close(); dbs.forEach(db => db.close());
    sockets.forEach(socket => socket.destroy()); await new Promise(done => server.close(done));
    const actual = realpathSync(directory); assert.equal(dirname(actual), parent); assert.match(basename(actual), /^soty-directory-network-/u);
    rmSync(actual, { recursive: true, force: true }); });
  await new Promise(done => server.listen(0, '127.0.0.1', done));
  const sourceOrigin = `http://127.0.0.1:${server.address().port}`;
  const connect = { isActorActive: actor => active && actor?.accountId === owner.accountId && actor.deviceId === owner.deviceId,
    withAuthorityFence: callback => callback(),
    withActorAuthorityFence(actor, callback) {
      assert.equal(actor, owner, 'the original signed actor identity must enter the native owner fence');
      if (!connect.isActorActive(actor)) throw new AccessError('access_denied');
      const result = callback();
      if (!connect.isActorActive(actor)) throw new AccessError('access_denied');
      return result;
    } };
  const databasePath = join(directory, 'apps.sqlite');
  apps = createAppsService({ databasePath, actorActive: connect.isActorActive, withAuthorityFence: connect.withAuthorityFence,
    shellOrigins: ['https://root.example'], namedAppZone: 'https://apps.example', appOriginTemplate: 'https://{appId}.legacy.example' });
  const db = new DatabaseSync(databasePath); dbs.push(db);
  const identity = { linkId: 'network-link', hostDeviceId: 'network-host', connectorId: 'network-connector' };
  db.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(connectorKey(identity), owner.accountId, JSON.stringify(identity), 'Network source', 1);
  const register = (name, port) => apps.execute({ actor: owner, op: 'apps.register', args: {
    name, port, entryPath: '/', hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId } }).app;
  const app = register('Independent readonly HTTP fixture', server.address().port), other = register('Unrelated private app', server.address().port + 1);
  const authority = appId => apps.withAppAuthority({ actor: owner, appId, mode: 'owner' }, value => value), pin = authority(app.id).target;
  const namespace = `app.${app.id}`, capabilityId = `${namespace}:read`, resource = `${namespace}:workspace`,
    inputSchema = object({}), outputSchema = object({ count: { type: 'integer', minimum: 0, maximum: 100 } });
  const adapter = createTrustedReadonlyQueryAdapter({
    withAuthority(_request, callback) { if (!sourceCurrent) throw new AccessError('external_resource_denied'); return callback(); },
    async query(_request, signal, current) {
      current();
      // Fixed trusted host destination and credential. Neither comes from an app descriptor.
      const response = await fetch(sourceOrigin + '/read', { signal, headers: { authorization: 'Bearer synthetic-source-fixture' } });
      assert.equal(response.status, 200); const value = await response.json(); current(); return value;
    },
  });
  const entries = captureExternalApplications([{ appId: app.id, target: { revision: pin.revision, digest: pin.digest }, adapter,
    catalog: { capabilityId, version: 1, appId: app.id, title: 'Read count over HTTP', description: 'Isolated synthetic Source, no production data.',
      visibility: 'private', executionEnabled: true, inputSchema, outputSchema, resources: [resource], effects: [], recipients: [resource],
      executionBinding: { kind: 'registered', handler: capabilityId, version: 1, binding: ref(`${namespace}:read-binding`) } } }]);
  caps = createCapabilitiesService({ databasePath: join(directory, 'capabilities.sqlite'), projectId: 'directory-network', actorActive: connect.isActorActive,
    catalog: entries.map(entry => entry.catalog), documentation: [], readonlyQueries: composeExternalApplications(entries, {
      withConnectFence: connect.withAuthorityFence, capabilities: () => caps, apps: () => apps }) });
  const reference = caps.external.contracts[0], access = (op, args) => caps.execute({ actor: owner, op: `access.${op}`, args: { expectedAccountId: owner.accountId, ...args } }),
    principal = access('principals.create', { label: 'Network fixture reader' }).principal,
    grant = access('grants.issue', { principalId: principal.id, capabilities: [{ capabilityId, version: 1 }], resources: [resource], effects: [],
      recipients: [resource], expiresAt: Date.now() + 600000, budget: { unit: 'invocations', limit: 10 } }).grant,
    credential = access('credentials.issue', { grantId: grant.id, audience });
  const actor = caps.authenticateCredential({ token: credential.token, audience });
  const transport = createAppAgentDirectoryTransport({ service: () => caps, origin: 'https://root.example',
    resolveBinding({ authority: current, capability }) {
      if (current.appId !== app.id || current.target.digest !== pin.digest || capability.id !== capabilityId
        || capability.binding.digest !== entries[0].catalog.executionBinding.binding.digest) return null;
      return reference;
    } });
  discovery = createAppAgentDirectory({ databasePath: join(directory, 'directory.sqlite'), projectId: 'directory-network', transport,
    ...createAppAgentDirectoryAuthority({ apps: () => apps, connect: () => connect, capabilities: () => caps }) });
  const optIn = appId => {
    const current = authority(appId), ns = `app.${appId}`;
    const descriptor = createAppAgentDescriptor({ authority: current, namespace: ns,
      feedback: { mode: 'required', provider: ref('soty:feedback'), captureProfile: ref('soty:capture'), retentionProfile: ref('soty:retention'),
        submitAudience: 'members', ticketVisibility: 'reporter-and-support' },
      capabilities: [{ id: `${ns}:read`, version: 1, title: 'Read count', inputSchema, outputSchema, resources: [ref(`${ns}:workspace`)], effects: ['read'],
        recipients: [ref(`${ns}:workspace`)], binding: appId === app.id ? entries[0].catalog.executionBinding.binding : ref(`${ns}:unadmitted`) }] });
    return discovery.execute({ actor: owner, op: 'apps.agentDescriptor.put', args: { expectedAccountId: owner.accountId, appId,
      requestId: `opt-in-${appId}`, expectedRevision: 0, enabled: true, descriptor } });
  };
  optIn(app.id); optIn(other.id);
  const query = (key = 'read-once') => discovery.query({ actor, appId: app.id, capabilityId, input: {}, idempotencyKey: key });
  return { discovery, actor, app, other, query, db, calls: () => calls, caps,
    async brokenRetry() { return (await fetch(sourceOrigin + '/read', { headers: { authorization: 'Bearer synthetic-source-fixture' } })).json(); },
    revokeSource() { sourceCurrent = false; }, revokeGrant() { access('grants.revoke', { grantId: grant.id }); },
    revokeApp() { apps.execute({ actor: owner, op: 'apps.revoke', args: { appId: app.id } }); },
    disableDescriptor() {
      const previous = discovery.execute({ actor: owner, op: 'apps.agentDescriptor.get', args: { expectedAccountId: owner.accountId, appId: app.id } });
      return discovery.execute({ actor: owner, op: 'apps.agentDescriptor.put', args: { expectedAccountId: owner.accountId, appId: app.id,
        expectedRevision: previous.revision, enabled: false, requestId: 'disable-description' } });
    },
    misdescribe() {
      const previous = discovery.execute({ actor: owner, op: 'apps.agentDescriptor.get', args: { expectedAccountId: owner.accountId, appId: app.id } }),
        { digest: _digest, ...capability } = previous.descriptor.capabilities[0];
      const descriptor = createDescriptor({ ...previous.descriptor, capabilities: [{ ...capability,
        outputSchema: object({ count: { type: 'integer', minimum: 0, maximum: 1000 } }) }] });
      return discovery.execute({ actor: owner, op: 'apps.agentDescriptor.put', args: { expectedAccountId: owner.accountId, appId: app.id,
        expectedRevision: previous.revision, enabled: true, requestId: 'misdescribed-contract', descriptor } });
    },
    hold() { hold = true; return new Promise(done => { reached = done; }); }, release() { hold = false; release?.(); } };
}

test('real Apps + genuine Root grant + trusted readonly transport makes one actual network call; duplicate returns ledger receipt', async t => {
  const f = await fixture(t);
  assert.equal(f.discovery.list().items.length, 0, 'private app does not enter anonymous discovery');
  const visible = f.discovery.list({ actor: f.actor });
  assert.deepEqual(visible.items.map(app => app.appId), [f.app.id], 'Root app grant cannot disclose unrelated private app');
  assert.equal(visible.items[0].executionReady, true);
  assert.throws(() => f.discovery.get({ actor: f.actor, appId: f.other.id }), { code: 'apps_access_denied' });
  const result = await f.query(); assert.deepEqual(result.result, { count: 7 }); assert.equal(f.calls(), 1);
  const duplicate = await f.query(); assert.equal(duplicate.reused, true); assert.equal(duplicate.resultUnavailable, true); assert.equal(f.calls(), 1);
  assert.doesNotMatch(JSON.stringify(visible), /synthetic-source-fixture|soty_cap_|credentialId|connectorKey/u);
  await f.brokenRetry();
  assert.throws(() => assert.equal(f.calls(), 1), assert.AssertionError, 'negative control actually dispatches twice and the same count check detects it');
});

test('stale Source credential, revoked app and revoked Root grant deny discovery execution before network dispatch', async t => {
  for (const failure of ['source', 'app', 'grant', 'opt-in']) {
    const f = await fixture(t);
    if (failure === 'source') f.revokeSource(); else if (failure === 'app') f.revokeApp(); else if (failure === 'opt-in') f.disableDescriptor(); else f.revokeGrant();
    await assert.rejects(f.query()); assert.equal(f.calls(), 0, `${failure} rejected before network`);
  }
});

test('revocation during awaited Source response suppresses result bytes after genuine network work', async t => {
  for (const failure of ['source', 'app', 'grant', 'opt-in']) {
    const f = await fixture(t), reached = f.hold(), pending = f.query(`held-${failure}`);
    const refused = assert.rejects(pending);
    await reached;
    if (failure === 'source') f.revokeSource(); else if (failure === 'app') f.revokeApp(); else if (failure === 'opt-in') f.disableDescriptor(); else f.revokeGrant();
    f.release(); await refused; assert.equal(f.calls(), 1);
  }
});

test('author JSON cannot relabel an existing approved adapter with different contract schemas', async t => {
  const f = await fixture(t); f.misdescribe();
  assert.equal(f.discovery.get({ actor: f.actor, appId: f.app.id }).executionReady, false);
  await assert.rejects(f.query(), { code: 'directory_execution_denied' }); assert.equal(f.calls(), 0);
});
