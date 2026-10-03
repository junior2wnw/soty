import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createAppsService } from '../server/index.mjs';
import { connectorKey } from '../server/protocol.mjs';

const owner = Object.freeze({ accountId: 'projection-owner', deviceId: 'projection-browser' });
const reader = Object.freeze({ accountId: 'projection-reader', deviceId: 'reader-browser' });
const outsider = Object.freeze({ accountId: 'projection-outsider', deviceId: 'outside-browser' });
const identity = { linkId: 'projection-link', hostDeviceId: 'projection-host', connectorId: 'projection-connector' };

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-owner-projection-'));
  const databasePath = join(directory, 'registry.sqlite'); let actorCheck = () => {};
  const options = { databasePath, appOriginTemplate: 'https://{appId}.legacy.example', namedAppZone: 'https://named.example',
    shellOrigins: ['https://shell.example'], actorActive(actor) { actorCheck(actor); return [owner, reader, outsider].includes(actor); } };
  let service = createAppsService(options);
  const db = new DatabaseSync(databasePath); db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000');
  db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run(connectorKey(identity), owner.accountId, JSON.stringify(identity), 'Owner device', 1);
  const call = (op, args = {}, actor = owner) => service.execute({ op, args, actor });
  t.after(async () => {
    service.close(); db.close();
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-owner-projection-/u);
    await rm(directory, { recursive: true, force: true });
  });
  const app = call('apps.register', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, name: 'Project', port: 8111 });
  let sequence = 0;
  const appId = app.app.id;
  const claim = slug => call('apps.domains.claim', { appId, slug, requestId: `claim-${++sequence}`,
    expectedDomainsRevision: call('apps.domains.get', { appId }).revision }).receipt;
  const publish = (domainIds, launchPolicy = 'anyone', listed = false) => {
    const current = call('apps.publication.get', { appId });
    return call('apps.publication.update', { appId, requestId: `policy-${++sequence}`, expectedPolicyEpoch: current.policyEpoch,
      expectedTargetRevision: current.activeTargetRevision, launchPolicy, listed, activeDomainIds: domainIds,
      ...(launchPolicy === 'anyone' ? { exposureAck: { scope: 'whole-port', targetRevision: current.target.revision,
        targetDigest: current.target.digest, profile: current.target.profile } } : {}) });
  };
  const current = () => call('apps.list').apps.find(item => item.id === appId);
  return { call, appId, db, claim, publish, current, catalog: () => service.publicCatalog(), policy: () => service.policy, setActorCheck(fn) { actorCheck = fn; },
    reopenWithoutClaims() { service.close(); service = createAppsService({ ...options, namedAppZone: '' }); } };
}

test('owner projection reports public named policy while canonical and foreign list admission stay restricted', async t => {
  const f = await fixture(t);
  assert.deepEqual(f.current().publication, { launchPolicy: 'restricted', activeNamedAddressCount: 0 });
  const a = f.claim('first-project'), b = f.claim('second-project');
  f.publish([a.domainId, b.domainId]);
  const item = f.current();
  assert.equal(item.state, 'offline', 'publication is independent from live connector health');
  assert.deepEqual(item.publication, { launchPolicy: 'anyone', activeNamedAddressCount: 2 });
  const canonical = f.call('apps.domains.get', { appId: f.appId }).domains.find(domain => domain.role === 'canonical');
  assert.throws(() => f.policy().decideAccess({ domainId: canonical.id, origin: canonical.origin, actor: outsider }), { code: 'apps_access_denied' });
  assert.equal(f.policy().decideAccess({ domainId: a.domainId, origin: a.origin }).accessBasis, 'public');
  assert.deepEqual(f.call('apps.list', {}, outsider).apps, [], 'publication does not add strangers to the private app list');
  f.call('apps.update', { appId: f.appId, grants: { accountIds: [reader.accountId], communityIds: [] } });
  const shared = f.call('apps.list', {}, reader).apps[0];
  for (const field of ['publication', 'grants', 'port', 'entryPath', 'connectorId', 'deviceName']) assert.equal(Object.hasOwn(shared, field), false, field);
  assert.equal(JSON.stringify(shared).includes(a.origin), false);
  assert.equal(JSON.stringify(shared).includes(b.domainId), false);
});

test('public catalog includes only listed admitted named entries, without connector or grant metadata', async t => {
  const f = await fixture(t), alias = f.claim('nfc'), next = f.claim('nfc-two');
  assert.deepEqual(f.catalog(), { schema: 'soty.app-catalog.v1', apps: [] });
  f.publish([alias.domainId]); assert.deepEqual(f.catalog(), { schema: 'soty.app-catalog.v1', apps: [] }, 'unlisted public links stay out of discovery');
  f.publish([alias.domainId], 'anyone', true);
  const catalog = f.catalog(); assert.equal(catalog.apps.length, 1);
  const app = catalog.apps[0];
  assert.equal(app.id, f.appId); assert.equal(app.name, 'Project'); assert.equal(app.state, 'offline');
  assert.equal(app.entry.domainId, alias.domainId); assert.equal(app.entry.origin, alias.origin);
  for (const field of ['port', 'connectorId', 'connectorKey', 'hostDeviceId', 'deviceName', 'grants', 'targetDigest']) assert.equal(Object.hasOwn(app, field), false, field);
  assert.deepEqual(f.call('apps.catalog', {}, outsider), catalog);
  assert.deepEqual(f.call('apps.list', {}, outsider).apps, [], 'listing never grants the canonical entry');
  f.reopenWithoutClaims(); assert.equal(f.catalog().apps.length, 1, 'retained named publications survive claim disable');
  assert.throws(() => f.publish([], 'anyone', true), { code: 'app_publication_domain_required' });
  f.publish([alias.domainId], 'restricted'); assert.deepEqual(f.catalog(), { schema: 'soty.app-catalog.v1', apps: [] });
  f.publish([alias.domainId], 'anyone', true);
  f.call('apps.domains.retire', { appId: f.appId, domainId: alias.domainId, requestId: 'catalog-retire', expectedDomainsRevision: f.call('apps.domains.get', { appId: f.appId }).revision });
  assert.deepEqual(f.catalog(), { schema: 'soty.app-catalog.v1', apps: [] });
  f.publish([next.domainId], 'anyone', true);
  f.call('apps.revoke', { appId: f.appId }); assert.deepEqual(f.catalog(), { schema: 'soty.app-catalog.v1', apps: [] });
});

test('active count excludes merely claimed, disabled and retired aliases, and retained named policy survives claim disable', async t => {
  const f = await fixture(t), a = f.claim('one-address'), b = f.claim('two-address'), spare = f.claim('claimed-only');
  f.publish([a.domainId, b.domainId], 'restricted');
  assert.deepEqual(f.current().publication, { launchPolicy: 'restricted', activeNamedAddressCount: 2 });
  f.publish([a.domainId]);
  assert.deepEqual(f.current().publication, { launchPolicy: 'anyone', activeNamedAddressCount: 1 });
  assert.throws(() => f.policy().decideAccess({ domainId: spare.domainId, origin: spare.origin }), { code: 'apps_access_denied' });
  f.reopenWithoutClaims();
  assert.equal(f.current().publication.activeNamedAddressCount, 1, 'disabling new claims does not silently disable a retained active alias');
  f.call('apps.domains.retire', { appId: f.appId, domainId: a.domainId, requestId: 'retire-active',
    expectedDomainsRevision: f.call('apps.domains.get', { appId: f.appId }).revision });
  assert.deepEqual(f.current().publication, { launchPolicy: 'anyone', activeNamedAddressCount: 0 });
  f.call('apps.revoke', { appId: f.appId });
  assert.equal(f.current().state, 'revoked'); assert.equal(f.current().publication.activeNamedAddressCount, 0);
});

test('projection rereads enabled state and publication after a writer changes the previously selected candidate', async t => {
  const f = await fixture(t), alias = f.claim('race-project'); f.publish([alias.domainId]);
  let calls = 0;
  f.setActorCheck(actor => {
    if (actor === owner && ++calls === 2) f.db.prepare("UPDATE local_apps SET state='revoked' WHERE id=?").run(f.appId);
  });
  // execute authenticates first, then canUse checks the materialized candidates.
  // The second connection commits between that old row and publicApp's SELECT.
  const item = f.current();
  assert.equal(calls >= 2, true); assert.equal(item.state, 'revoked');
  assert.deepEqual(item.publication, { launchPolicy: 'anyone', activeNamedAddressCount: 0 });
});

test('an old foreign candidate cannot expose fresh app metadata after its grant is removed by another writer', async t => {
  const f = await fixture(t);
  f.call('apps.update', { appId: f.appId, grants: { accountIds: [reader.accountId], communityIds: [] } });
  let calls = 0;
  f.setActorCheck(actor => {
    if (actor === reader && ++calls === 2) f.db.prepare('UPDATE local_apps SET grants_json=?,name=? WHERE id=?')
      .run(JSON.stringify({ accountIds: [], communityIds: [] }), 'New private name', f.appId);
  });
  assert.throws(() => f.call('apps.list', {}, reader), { code: 'apps_access_denied' });
  assert.ok(calls >= 2);
});

test('projection counts only exact same-owner same-app named bound aliases even after injected invalid active relations', async t => {
  const f = await fixture(t), alias = f.claim('valid-address'); f.publish([alias.domainId]);
  const other = f.call('apps.register', { hostDeviceId: identity.hostDeviceId, connectorId: identity.connectorId, name: 'Other', port: 8112 }).app;
  const otherAlias = f.call('apps.domains.claim', { appId: other.id, slug: 'other-address', requestId: 'other-claim', expectedDomainsRevision: 0 }).receipt;
  const canonical = f.call('apps.domains.get', { appId: f.appId }).domains.find(domain => domain.role === 'canonical');
  // Deliberate test-only FK bypass verifies the projection's allowlist. This is
  // not a promise that the service repairs or trusts arbitrary database damage.
  f.db.exec('PRAGMA foreign_keys=OFF');
  f.db.prepare('INSERT INTO app_publication_domains VALUES (?,?,?)').run(f.appId, otherAlias.domainId, owner.accountId);
  f.db.prepare('INSERT INTO app_publication_domains VALUES (?,?,?)').run(f.appId, canonical.id, owner.accountId);
  assert.equal(f.current().publication.activeNamedAddressCount, 1);
  f.db.prepare('UPDATE app_publication_domains SET owner_account_id=? WHERE app_id=? AND domain_id=?').run('foreign-owner', f.appId, alias.domainId);
  assert.equal(f.current().publication.activeNamedAddressCount, 0);
  f.db.prepare('UPDATE app_publication_domains SET owner_account_id=? WHERE app_id=? AND domain_id=?').run(owner.accountId, f.appId, alias.domainId);
  f.db.prepare("UPDATE app_domains SET state='tombstone',retired_at=2 WHERE id=?").run(alias.domainId);
  assert.equal(f.current().publication.activeNamedAddressCount, 0);
});
