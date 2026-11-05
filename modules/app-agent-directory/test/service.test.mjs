import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, realpathSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, basename } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createAppsService } from '../../apps/server/index.mjs';
import { connectorKey } from '../../apps/server/protocol.mjs';
import { runtimeTargetDigest } from '../../apps/server/schema.mjs';
import { createAppAgentDirectory, appAgentSource } from '../server/index.mjs';
import { createAppAgentDescriptor } from '../sdk.mjs';
import { createDescriptor } from '../../app-contract/sdk.mjs';

const owner = { accountId: 'directory-owner', deviceId: 'owner-device' }, member = { accountId: 'directory-member', deviceId: 'member-device' },
  outsider = { accountId: 'directory-outsider', deviceId: 'outsider-device' };
const ref = id => ({ id, version: 1, digest: 'a'.repeat(64) });
export function descriptor(authority, name = 'read') {
  const namespace = `app.${authority.appId}`;
  return createAppAgentDescriptor({ authority, namespace,
    feedback: { mode: 'required', provider: ref('soty:feedback'), captureProfile: ref('soty:capture'), retentionProfile: ref('soty:retention'),
      submitAudience: authority.visibility === 'public' ? 'public' : 'members', ticketVisibility: 'reporter-and-support' },
    capabilities: [{ id: `${namespace}:${name}`, version: 1, title: name === 'read' ? 'Read notes' : 'Count photos',
      inputSchema: { type: 'object', properties: {}, required: [], additionalProperties: false },
      outputSchema: { type: 'object', properties: { count: { type: 'integer', minimum: 0, maximum: 100 } }, required: ['count'], additionalProperties: false },
      resources: [ref(`${namespace}:workspace`)], effects: ['read'], recipients: [ref(`${namespace}:workspace`)], binding: ref(`${namespace}:readonly`) }] });
}
function fixture(t, { transport } = {}) {
  const parent = realpathSync(tmpdir()), directory = mkdtempSync(join(parent, 'soty-app-directory-')),
    file = join(directory, 'apps.sqlite'), metadata = join(directory, 'metadata.sqlite');
  const identity = { linkId: 'directory-link', hostDeviceId: 'directory-host', connectorId: 'directory-connector' };
  let current = true, counter = 0, service; const extraCleanup = [];
  const actorActive = actor => current && [owner, member, outsider].some(value => value.accountId === actor?.accountId && value.deviceId === actor?.deviceId);
  const apps = createAppsService({ databasePath: file, shellOrigins: ['https://root.example'], namedAppZone: 'https://apps.example',
    appOriginTemplate: 'https://{appId}.legacy.example', actorActive, withAuthorityFence: callback => callback() });
  const db = new DatabaseSync(file);
  db.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(connectorKey(identity), owner.accountId, JSON.stringify(identity), 'Directory host', Date.now());
  const ports = { databasePath: metadata, projectId: 'directory-test', actorActive, withAppAuthority: apps.withAppAuthority,
    withPublicAppAuthority: apps.withPublicAppAuthority, readPublicApps: () => apps.publicCatalog(), transport };
  service = createAppAgentDirectory(ports);
  t.after(() => { extraCleanup.forEach(work => work()); service.close(); apps.close(); db.close(); const actual = realpathSync(directory);
    assert.equal(dirname(actual), parent); assert.match(basename(actual), /^soty-app-directory-/u); rmSync(actual, { recursive: true, force: true }); });
  const call = (op, args, actor = owner) => apps.execute({ op, args, actor });
  const create = (name, grants = { accountIds: [], communityIds: [] }) => call('apps.register', {
    name, port: 8100 + ++counter, entryPath: '/', hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, grants }).app;
  const authority = appId => apps.withAppAuthority({ actor: owner, appId, mode: 'owner' }, value => value);
  const publish = (appId, listed = true) => {
    const domains = call('apps.domains.get', { appId });
    const claim = call('apps.domains.claim', { appId, slug: `project-${++counter}`, requestId: `claim-${counter}`, expectedDomainsRevision: domains.revision }).receipt;
    const current = call('apps.publication.get', { appId });
    return call('apps.publication.update', { appId, requestId: `publish-${++counter}`, expectedPolicyEpoch: current.policyEpoch,
      expectedTargetRevision: current.activeTargetRevision, launchPolicy: 'anyone', listed, activeDomainIds: [claim.domainId],
      exposureAck: { scope: 'whole-port', targetRevision: current.target.revision, targetDigest: current.target.digest, profile: current.target.profile } });
  };
  const put = (appId, options = {}, actor = owner) => {
    const before = service.execute({ actor, op: 'apps.agentDescriptor.get', args: { expectedAccountId: actor.accountId, appId } });
    return service.execute({ actor, op: 'apps.agentDescriptor.put', args: { expectedAccountId: actor.accountId, appId,
      requestId: `metadata-${++counter}`, expectedRevision: before.revision, enabled: true, descriptor: descriptor(authority(appId)), ...options } });
  };
  return { apps, db, create, publish, authority, put, call, ports, directory, cleanup: work => extraCleanup.push(work), get service() { return service; },
    reopen() { service.close(); service = createAppAgentDirectory(ports); }, deactivate() { current = false; },
    changeSource(appId) {
      // Intentional test-only control: emulate the committed next immutable runtime target.
      // The public/owner ports still validate its digest, owner, active domain and publication.
      const previous = db.prepare('SELECT t.* FROM app_runtime_targets t JOIN app_publications p ON p.app_id=t.app_id AND p.active_target_revision=t.revision WHERE t.app_id=?').get(appId),
        revision = previous.revision + 1, port = previous.port + 100;
      const digest = runtimeTargetDigest({ appId, revision, ownerAccountId: previous.owner_account_id, connectorKey: previous.connector_key,
        port, entryPath: previous.entry_path, profile: previous.profile });
      db.prepare('INSERT INTO app_runtime_targets(app_id,revision,owner_account_id,connector_key,port,entry_path,profile,digest,created_at) VALUES(?,?,?,?,?,?,?,?,?)')
        .run(appId, revision, previous.owner_account_id, previous.connector_key, port, previous.entry_path, previous.profile, digest, Date.now());
      db.prepare('UPDATE app_source_heads SET required_binding_version=2 WHERE app_id=?').run(appId);
      const policy = db.prepare('SELECT launch_policy FROM app_publications WHERE app_id=?').get(appId);
      db.prepare('UPDATE app_publications SET active_target_revision=?,policy_epoch=policy_epoch+1,exposure_ack_revision=?,exposure_ack_json=? WHERE app_id=?')
        .run(revision, policy.launch_policy === 'anyone' ? revision : null, policy.launch_policy === 'anyone'
          ? JSON.stringify({ scope: 'whole-port', targetRevision: revision, targetDigest: digest, profile: previous.profile }) : null, appId);
    } };
}

test('two independent real registered app manifests enter the live catalog without a rebuild; opt-in and public ACL are both mandatory', t => {
  const f = fixture(t), notes = f.create('Notes'), gallery = f.create('Gallery'), privateApp = f.create('Private'), link = f.create('Link');
  f.publish(notes.id); f.publish(gallery.id); f.publish(link.id, false);
  assert.deepEqual(f.service.list().items, [], 'registration alone never opts in');
  const brokenWithoutOptIn = f.apps.publicCatalog().apps;
  assert.throws(() => assert.equal(brokenWithoutOptIn.length, 0), assert.AssertionError, 'negative control: registry-only listing lacks opt-in');
  f.put(notes.id); f.put(gallery.id, { descriptor: descriptor(f.authority(gallery.id), 'photos') }); f.put(privateApp.id); f.put(link.id);
  const catalog = f.service.list();
  assert.deepEqual(new Set(catalog.items.map(app => app.appId)), new Set([notes.id, gallery.id]));
  const brokenWithoutAcl = f.service.list({ actor: owner }).items;
  assert.throws(() => assert.equal(brokenWithoutAcl.some(app => app.appId === privateApp.id), false), assert.AssertionError,
    'negative control: using owner scope for a public caller leaks private metadata');
  assert.equal(catalog.items.every(app => app.discoveryReady && !app.executionReady), true);
  assert.equal(catalog.items.flatMap(app => app.capabilities).every(capability => capability.executionReady === false), true);
  assert.equal(f.service.list({ query: 'photos' }).items[0].appId, gallery.id, 'capability text is searchable');
  assert.throws(() => f.service.get({ appId: privateApp.id }), { code: 'apps_access_denied' });
  assert.throws(() => f.service.get({ appId: link.id }), { code: 'apps_access_denied' });
  assert.equal(f.service.get({ actor: owner, appId: privateApp.id }).appId, privateApp.id);
  assert.throws(() => f.service.get({ actor: outsider, appId: privateApp.id }), { code: 'apps_access_denied' });
  assert.doesNotMatch(JSON.stringify(catalog), /connectorKey|connectorId|hostDeviceId|"port"|token|credentialId/u);
  const later = f.create('Registered later'); f.publish(later.id); f.put(later.id);
  assert.equal(f.service.list().items.some(app => app.appId === later.id), true, 'same service instance reads new registration');
});

test('immutable source pin and explicit opt-in remove stale capabilities immediately; same address cannot keep old capability metadata', t => {
  const f = fixture(t), app = f.create('Source'); f.publish(app.id); f.put(app.id);
  const before = f.service.get({ appId: app.id }), brokenStaticCatalog = [before];
  f.changeSource(app.id);
  assert.equal(f.service.list().items.length, 0);
  assert.equal(brokenStaticCatalog.length, 1, 'negative control: static legacy catalog still leaks old source metadata');
  assert.throws(() => assert.equal(brokenStaticCatalog.length, f.service.list().items.length), assert.AssertionError);
  assert.throws(() => f.put(app.id, { descriptor: before.descriptor }), { code: 'directory_source_changed' });
  f.put(app.id); const after = f.service.get({ appId: app.id });
  assert.notEqual(before.source.digest, after.source.digest); assert.deepEqual(before.entry, after.entry);
  const get = () => f.service.execute({ actor: owner, op: 'apps.agentDescriptor.get', args: { expectedAccountId: owner.accountId, appId: app.id } });
  const state = get(); f.put(app.id, { expectedRevision: state.revision, enabled: false });
  assert.equal(f.service.list().items.length, 0); assert.equal(get().enabled, false);
  f.put(app.id); f.call('apps.revoke', { appId: app.id });
  assert.equal(f.service.list().items.length, 0);
});

test('owner CAS, immutable request receipt, scope isolation and reopen durability are enforced', t => {
  const f = fixture(t), first = f.create('A'), second = f.create('B', { accountIds: [member.accountId], communityIds: [] });
  const value = f.put(first.id, { requestId: 'stable-request' });
  const args = { expectedAccountId: owner.accountId, appId: first.id, expectedRevision: 0, requestId: 'stable-request', enabled: true, descriptor: descriptor(f.authority(first.id)) };
  const replay = f.service.execute({ actor: owner, op: 'apps.agentDescriptor.put', args });
  assert.equal(replay.replayed, true); assert.equal(replay.revision, value.revision);
  assert.deepEqual(replay.receipt, value.receipt);
  assert.throws(() => f.service.execute({ actor: owner, op: 'apps.agentDescriptor.put', args: { ...args, enabled: false } }), { code: 'directory_request_conflict' });
  assert.throws(() => f.put(first.id, { expectedRevision: 0 }), { code: 'directory_revision_conflict' });
  assert.throws(() => f.put(first.id, { descriptor: descriptor(f.authority(second.id)) }), { code: 'directory_app_mismatch' });
  f.put(second.id); assert.equal(f.service.get({ actor: member, appId: second.id }).canManage, false);
  assert.throws(() => f.service.execute({ actor: member, op: 'apps.agentDescriptor.get', args: { expectedAccountId: member.accountId, appId: second.id } }), { code: 'apps_owner_required' });
  assert.throws(() => f.service.execute({ actor: owner, op: 'apps.agentDescriptor.get', args: { expectedAccountId: member.accountId, appId: first.id } }), { code: 'directory_account_mismatch' });
  f.reopen(); assert.equal(f.service.get({ actor: owner, appId: first.id }).revision, 1);
  assert.equal(f.service.get({ actor: owner, appId: second.id }).revision, 1);
  const page = f.service.list({ actor: owner, limit: 1 }); assert.equal(page.items.length, 1); assert.ok(page.nextCursor);
  assert.notEqual(f.service.list({ actor: owner, limit: 1, cursor: page.nextCursor }).items[0].appId, page.items[0].appId);
  assert.throws(() => f.service.list({ actor: member, cursor: page.nextCursor }), { code: 'directory_cursor_invalid' });
  f.deactivate(); assert.throws(() => f.service.get({ actor: owner, appId: first.id }), { code: 'directory_authentication_required' });
});

test('late replay returns its immutable historical receipt and current state, never reenables or relabels stale source metadata', t => {
  const f = fixture(t), app = f.create('Late retry');
  const args = { expectedAccountId: owner.accountId, appId: app.id, expectedRevision: 0, requestId: 'late-retry', enabled: true, descriptor: descriptor(f.authority(app.id)) };
  const first = f.service.execute({ actor: owner, op: 'apps.agentDescriptor.put', args });
  f.changeSource(app.id);
  const stale = f.service.execute({ actor: owner, op: 'apps.agentDescriptor.put', args });
  assert.equal(stale.current, false); assert.equal(stale.replayed, true); assert.deepEqual(stale.receipt, first.receipt);
  assert.notEqual(stale.source.digest, first.source.digest);
  f.service.execute({ actor: owner, op: 'apps.agentDescriptor.put', args: { expectedAccountId: owner.accountId, appId: app.id,
    expectedRevision: 1, requestId: 'disable-late-retry', enabled: false } });
  const disabled = f.service.execute({ actor: owner, op: 'apps.agentDescriptor.put', args });
  assert.equal(disabled.enabled, false); assert.equal(disabled.revision, 2); assert.deepEqual(disabled.receipt, first.receipt);
});

test('author JSON cannot select arbitrary endpoints, commands, credentials, effects adapters or another Source', t => {
  const f = fixture(t), app = f.create('Closed JSON'), valid = descriptor(f.authority(app.id));
  for (const field of ['url', 'mcpEndpoint', 'command', 'credentials', 'adapter']) {
    assert.throws(() => f.put(app.id, { descriptor: { ...valid, [field]: 'https://attacker.invalid' } }), { code: 'closed_fields' });
  }
  assert.throws(() => f.put(app.id, { descriptor: { ...valid, app: { ...valid.app, source: { ...valid.app.source, digest: 'b'.repeat(64) } } } }), { code: 'directory_source_changed' });
  assert.throws(() => f.put(app.id, { descriptor: { ...valid, capabilities: [{ ...valid.capabilities[0], binding: { ...valid.capabilities[0].binding, url: 'https://attacker.invalid' } }] } }), { code: 'closed_fields' });
  const state = f.service.execute({ actor: owner, op: 'apps.agentDescriptor.get', args: { expectedAccountId: owner.accountId, appId: app.id } });
  assert.equal(state.revision, 0, 'failed boundary checks never commit metadata');
});

test('COMMIT write failure returns no successful receipt and exact retry is safe after reopen', t => {
  const f = fixture(t), app = f.create('Durable');
  const db = new DatabaseSync(f.ports.databasePath); db.exec('PRAGMA foreign_keys=ON; CREATE TABLE test_parent(id INTEGER PRIMARY KEY); CREATE TABLE test_child(id INTEGER REFERENCES test_parent DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER fail_descriptor AFTER INSERT ON app_agent_descriptors BEGIN INSERT INTO test_child VALUES(999); END;');
  f.cleanup(() => db.close());
  const args = { expectedAccountId: owner.accountId, appId: app.id, expectedRevision: 0, requestId: 'commit-retry', enabled: true, descriptor: descriptor(f.authority(app.id)) };
  assert.throws(() => f.service.execute({ actor: owner, op: 'apps.agentDescriptor.put', args }), /FOREIGN KEY constraint failed/u);
  assert.equal(db.prepare('SELECT count(*) n FROM app_agent_descriptors').get().n, 0);
  assert.equal(db.prepare('SELECT count(*) n FROM app_agent_descriptor_receipts').get().n, 0);
  db.exec('DROP TRIGGER fail_descriptor'); f.reopen();
  assert.equal(f.service.execute({ actor: owner, op: 'apps.agentDescriptor.put', args }).revision, 1);
  assert.equal(f.service.execute({ actor: owner, op: 'apps.agentDescriptor.put', args }).replayed, true);
});

test('valid large app descriptors paginate below the Root MCP envelope budget without dropping the next app', t => {
  const f = fixture(t), expected = new Set();
  for (let index = 0; index < 12; index++) {
    const app = f.create(`Large current descriptor ${index}`); f.publish(app.id); expected.add(app.id);
    const base = descriptor(f.authority(app.id)), { digest: _digest, ...capability } = base.capabilities[0];
    const large = createDescriptor({ ...base, capabilities: Array.from({ length: 12 }, (_, item) => ({ ...capability,
      id: `${base.app.namespace}:read${item}`, description: '\\'.repeat(1600) })) });
    f.put(app.id, { descriptor: large });
  }
  const found = []; let cursor = null, pages = 0;
  do {
    const page = f.service.list({ limit: 60, cursor }); pages++;
    const wire = JSON.stringify({ result: { content: [{ type: 'text', text: JSON.stringify(page) }], structuredContent: page } });
    assert.ok(Buffer.byteLength(wire) <= 2 * 1024 * 1024, 'the actual MCP text + structuredContent envelope must fit Root outputBytes');
    found.push(...page.items.map(value => value.appId)); cursor = page.nextCursor;
  } while (cursor && pages < 10);
  assert.ok(pages > 1); assert.equal(found.length, expected.size); assert.deepEqual(new Set(found), expected);
});
