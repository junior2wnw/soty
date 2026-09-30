import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve, basename } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createAppInspection } from '../server/inspection.mjs';
import { createDomainRegistry } from '../server/domains.mjs';
import { createPublicationRegistry } from '../server/publications.mjs';
import { ensureCanonicalDomain, ensureInitialPublication, migrateAppsSchema } from '../server/schema.mjs';
import { assertApps, connectorKey } from '../server/protocol.mjs';

const owner = Object.freeze({ accountId: 'owner', deviceId: 'owner-browser' });
const other = Object.freeze({ accountId: 'other', deviceId: 'other-browser' });
const appA = `app-${'a'.repeat(32)}`, appB = `app-${'b'.repeat(32)}`, appC = `app-${'c'.repeat(32)}`;
const legacy = 'https://{appId}.legacy.example', named = 'https://apps.example', shell = 'https://shell.example';
const entryPath = '/board?tag=a%2Bb&query=%23one#/dashboard';
const unknown = { state: 'unknown', observedAt: null, freshUntil: null, evidence: 'not-observed' };
const code = expected => error => error?.code === expected;

async function fixture(t, options = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'soty-inspection-'));
  const databasePath = join(directory, 'registry.sqlite'), db = new DatabaseSync(databasePath), peers = [];
  t.after(async () => {
    for (const connection of peers) connection.close();
    db.close();
    assert.equal(dirname(resolve(directory)), resolve(tmpdir())); assert.match(basename(directory), /^soty-inspection-/u);
    await rm(directory, { recursive: true, force: true });
  });
  const active = new Set([owner, other]);
  const assertActor = actor => assertApps(active.has(actor), 'apps_authentication_required', 401);
  const template = options.legacyTemplate ?? legacy;
  const identities = [
    { linkId: 'owner-link', hostDeviceId: 'source-host', connectorId: 'source-connector' },
    { linkId: 'owner-link', hostDeviceId: 'old-host', connectorId: 'old-connector' },
    { linkId: 'other-link', hostDeviceId: 'private-host', connectorId: 'private-connector' },
  ];
  const keys = identities.map(connectorKey);
  db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000');
  migrateAppsSchema(db, { legacyTemplate: template, now: () => 100 });
  db.exec('PRAGMA journal_mode=WAL');
  db.exec('BEGIN IMMEDIATE');
  for (let i = 0; i < identities.length; i++) db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)')
    .run(keys[i], i === 2 ? other.accountId : owner.accountId, JSON.stringify(identities[i]), ['Source device', 'Old device', 'Private device'][i], 100);
  for (const [id, actor, key, port] of [[appA, owner, keys[0], 8301], [appB, owner, keys[0], 8302], [appC, other, keys[2], 9301]]) {
    const grants = id === appA ? { accountIds: [other.accountId], communityIds: ['community-team'] } : { accountIds: [], communityIds: [] };
    db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)')
      .run(id, actor.accountId, key, id === appC ? 'Private app' : 'App', port, options.entryPath ?? entryPath, JSON.stringify(grants), 'enabled', 1, 100, 100);
    for (const principal of grants.accountIds) db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)').run(id, 'account', principal);
    for (const principal of grants.communityIds) db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)').run(id, 'community', principal);
    const row = db.prepare('SELECT * FROM local_apps WHERE id=?').get(id);
    ensureCanonicalDomain(db, row, template); ensureInitialPublication(db, row);
  }
  db.exec('COMMIT');
  const publications = createPublicationRegistry({ db, now: () => 200, assertActor,
    canUse: (actor, app) => active.has(actor) && app.state === 'enabled' && (app.owner_account_id === actor.accountId
      || JSON.parse(app.grants_json).accountIds.includes(actor.accountId)) });
  const domains = createDomainRegistry({ db, now: () => 200, assertActor, legacyTemplate: template, namedAppZone: named,
    domainLimits: options.domainLimits, shellOrigins: [shell], onRetireInTransaction: publications.retireInTransaction, onPolicyChanged: publications.notifyChanged });
  let sequence = 0;
  const domainState = (id = appA, actor = owner) => domains.execute({ actor, op: 'apps.domains.get', args: { appId: id } });
  const publicationState = (id = appA, actor = owner) => publications.execute({ actor, op: 'apps.publication.get', args: { appId: id } });
  const claim = (slug, id = appA, actor = owner) => domains.execute({ actor, op: 'apps.domains.claim', args: {
    appId: id, slug, requestId: `name-${++sequence}`, expectedDomainsRevision: domainState(id, actor).revision,
  } }).receipt.domainId;
  const retire = (domainId, id = appA, actor = owner) => domains.execute({ actor, op: 'apps.domains.retire', args: {
    appId: id, domainId, requestId: `retire-${++sequence}`, expectedDomainsRevision: domainState(id, actor).revision,
  } });
  const publish = (activeDomainIds, launchPolicy = 'restricted', id = appA, actor = owner) => {
    const current = publicationState(id, actor);
    return publications.execute({ actor, op: 'apps.publication.update', args: { appId: id, requestId: `publication-${++sequence}`,
      expectedPolicyEpoch: current.policyEpoch, expectedTargetRevision: current.activeTargetRevision, launchPolicy, listed: false, activeDomainIds,
      ...(launchPolicy === 'anyone' ? { exposureAck: { scope: 'whole-port', targetRevision: current.target.revision,
        targetDigest: current.target.digest, profile: current.target.profile } } : {}),
    } });
  };
  const reader = (overrides = {}) => createAppInspection({ db, assertActor, domains, publications, inspectSource: () => null,
    shellOrigin: shell, nameClaimsEnabled: true, namedAppZone: named, ...overrides });
  const peer = () => {
    const connection = new DatabaseSync(databasePath); connection.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000'); peers.push(connection); return connection;
  };
  return { db, databasePath, keys, identities, active, assertActor, domains, publications, reader, claim, retire, publish, peer, publicationState };
}

test('only the current owner can inspect, before source observation or private metadata work', async t => {
  const f = await fixture(t); const alias = f.claim('public-app'); f.publish([alias], 'anyone');
  let observed = 0, domainsRead = 0;
  const reader = f.reader({ inspectSource: () => { observed++; return null; }, domains: { execute(...args) { domainsRead++; return f.domains.execute(...args); } } });
  for (const actor of [undefined, { ...owner }]) assert.throws(() => reader.read(actor, { appId: appA }), code('apps_authentication_required'));
  for (const id of [appA, `app-${'f'.repeat(32)}`]) assert.throws(() => reader.read(other, { appId: id }), code('apps_owner_required'));
  assert.equal(observed, 0); assert.equal(domainsRead, 0); assert.equal(f.db.isTransaction, false);
  assert.throws(() => reader.read(owner, { appId: appA, expectedAccountId: other.accountId }), code('unexpected_argument'));
  f.active.delete(owner);
  assert.throws(() => reader.read(owner, { appId: appA }), code('apps_authentication_required'));
  assert.equal(observed, 0);
});

test('inspection uses immutable target identity and source fields instead of drifting legacy app fields', async t => {
  const f = await fixture(t);
  f.db.prepare('UPDATE local_apps SET connector_key=?,port=9999,entry_path=? WHERE id=?').run(f.keys[1], '/old-source', appA);
  let callback;
  const reader = f.reader({ inspectSource(context) {
    callback = context;
    for (const value of [context, context.app, context.target, context.device, context.device.identity]) assert.equal(Object.isFrozen(value), true);
    assert.throws(() => { context.target.port = 9999; }, TypeError);
    assert.throws(() => { context.device.identity.hostDeviceId = 'wrong-host'; }, TypeError);
    return { state: 'responding', observedAt: 1000, freshUntil: 46000, evidence: 'connector-v1-observation' };
  } });
  const result = reader.read(owner, { appId: appA });
  assert.equal(result.schema, 'soty.app-inspection.v1'); assert.equal(result.source.port, 8301); assert.equal(result.source.entryPath, entryPath);
  assert.equal(result.source.hostDeviceId, 'source-host'); assert.equal(result.source.connectorId, 'source-connector'); assert.equal(result.source.deviceName, 'Source device');
  assert.equal(callback.target.connectorKey, f.keys[0]); assert.equal(callback.target.revision, 1);
  assert.equal(callback.app.ownerAccountId, owner.accountId); assert.deepEqual(callback.device.identity, f.identities[0]);
  assert.equal(result.source.observation.state, 'responding');
  for (const hidden of ['connectorKey', 'linkId', 'ownerAccountId', 'runtimeReady', 'runtimeMode', 'exposureAck', 'ticket']) assert.equal(JSON.stringify(result).includes(`"${hidden}"`), false, hidden);
  assert.deepEqual(result.app.grants, { accountIds: [other.accountId], communityIds: ['community-team'] });
  result.app.grants.accountIds.length = 0;
  assert.deepEqual(reader.read(owner, { appId: appA }).app.grants.accountIds, [other.accountId]);
});

test('one WAL snapshot covers app, addresses, quotas, policy and target despite a concurrent writer between reads', async t => {
  const f = await fixture(t); const alpha = f.claim('alpha'), retired = f.claim('old-alpha');
  f.publish([alpha]); const writer = f.peer(); let updated = false;
  const reader = f.reader({ domains: { execute(args) {
    assert.equal(f.db.isTransaction, true);
    if (!updated) {
      updated = true; writer.exec('BEGIN IMMEDIATE');
      writer.prepare('UPDATE local_apps SET name=?,revision=revision+1,grants_json=? WHERE id=?').run('New app', JSON.stringify({ accountIds: [], communityIds: [] }), appA);
      writer.prepare("UPDATE app_domains SET state='tombstone',retired_at=300 WHERE id=?").run(retired);
      writer.prepare("INSERT INTO app_domains SELECT ?,zone_id,'late-owner.apps.example','https://late-owner.apps.example','late-owner',?,owner_account_id,'alias','bound',300,NULL FROM app_domains WHERE id=?")
        .run(`dom_${'d'.repeat(32)}`, appB, alpha);
      writer.prepare('UPDATE app_domain_heads SET revision=revision+1 WHERE app_id=?').run(appA);
      writer.prepare('UPDATE app_publications SET policy_epoch=policy_epoch+1 WHERE app_id=?').run(appA);
      writer.exec('COMMIT');
    }
    return f.domains.execute(args);
  } } });
  const original = reader.read(owner, { appId: appA });
  assert.equal(original.app.name, 'App'); assert.equal(original.app.revision, 1);
  assert.deepEqual(original.app.grants.accountIds, [other.accountId]);
  assert.equal(original.addresses.revision, 2); assert.equal(original.addresses.aliases.find(item => item.id === retired).state, 'bound');
  assert.equal(original.addresses.limits.usedByAccount, 2);
  assert.equal(original.publication.policyEpoch, 2); assert.equal(original.source.revision, 1);
  const fresh = reader.read(owner, { appId: appA });
  assert.equal(fresh.app.name, 'New app'); assert.equal(fresh.app.revision, 2); assert.deepEqual(fresh.app.grants.accountIds, []);
  assert.equal(fresh.addresses.revision, 3); assert.equal(fresh.addresses.aliases.find(item => item.id === retired).state, 'tombstone');
  assert.equal(fresh.addresses.limits.usedByAccount, 3);
  assert.equal(fresh.publication.policyEpoch, 3); assert.equal(f.db.isTransaction, false);
});

test('active private links use the trusted shell and preserve full encoded query plus hash SPA path', async t => {
  const f = await fixture(t); const alpha = f.claim('alpha'), inactive = f.claim('inactive'), retired = f.claim('retired');
  f.retire(retired); f.publish([alpha]);
  const result = f.reader().read(owner, { appId: appA });
  for (const address of [result.addresses.canonical, result.addresses.aliases.find(item => item.id === alpha)]) {
    const url = new URL(address.shareUrl); assert.equal(url.origin, shell);
    const [route, query] = url.hash.slice(1).split('?'); assert.equal(route, `launch/${appA}/${address.id}`);
    assert.equal(new URLSearchParams(query).get('path'), entryPath);
    assert.equal(url.search, ''); assert.equal(url.username, ''); assert.equal(url.hash.includes('ticket'), false);
  }
  assert.equal(result.addresses.aliases.find(item => item.id === inactive).shareUrl, null);
  assert.equal(result.addresses.aliases.find(item => item.id === inactive).active, false);
  assert.equal(result.addresses.aliases.find(item => item.id === retired).shareUrl, null);
  assert.equal(result.addresses.aliases.find(item => item.id === retired).active, false);
  assert.equal(result.addresses.aliases.find(item => item.id === retired).state, 'tombstone');
  assert.equal(result.actions.canPreview, true); assert.equal(result.addresses.claimOrigin, named);
});

test('only active anyone aliases get direct stable runtime URLs; canonical remains a private shell link', async t => {
  const f = await fixture(t); const alpha = f.claim('alpha'), inactive = f.claim('inactive'); f.publish([alpha], 'anyone');
  const result = f.reader().read(owner, { appId: appA });
  const active = result.addresses.aliases.find(item => item.id === alpha);
  assert.equal(active.shareUrl, active.origin + entryPath); assert.equal(new URL(active.shareUrl).origin, active.origin);
  assert.equal(new URL(result.addresses.canonical.shareUrl).origin, shell);
  assert.equal(result.addresses.aliases.find(item => item.id === inactive).shareUrl, null);
  assert.equal(result.publication.launchPolicy, 'anyone'); assert.deepEqual(result.publication.activeDomainIds, [alpha]);
});

test('named-only apps can preview an active alias without inventing a canonical origin', async t => {
  const f = await fixture(t, { legacyTemplate: '' }); const alpha = f.claim('alpha'); const reader = f.reader();
  assert.equal(reader.read(owner, { appId: appA }).actions.canPreview, false);
  f.publish([alpha]); const result = reader.read(owner, { appId: appA });
  assert.equal(result.addresses.canonical, null); assert.equal(result.actions.canPreview, true);
  assert.equal(new URL(result.addresses.aliases[0].shareUrl).origin, shell);
});

test('claim availability uses current configuration and account-scoped quotas including tombstones', async t => {
  const f = await fixture(t, { domainLimits: { perApp: 3, perAccount: 4 } });
  f.claim('alpha'); const retired = f.claim('retired'); f.retire(retired);
  f.claim('owner-second', appB); f.claim('other-private', appC, other);
  const reader = f.reader(); const before = reader.read(owner, { appId: appA });
  assert.deepEqual(before.addresses.limits, { perApp: 3, perAccount: 4, usedByApp: 2, usedByAccount: 3 });
  assert.equal(before.actions.canReserveName, true); assert.equal(JSON.stringify(before).includes('other-private'), false);
  const disabled = f.reader({ nameClaimsEnabled: false }).read(owner, { appId: appA });
  assert.equal(disabled.addresses.claimOrigin, null); assert.equal(disabled.actions.canReserveName, false);
  const absent = f.reader({ namedAppZone: '' }).read(owner, { appId: appA });
  assert.equal(absent.addresses.claimOrigin, null); assert.equal(absent.actions.canReserveName, false);
  f.claim('owner-limit', appB);
  assert.equal(reader.read(owner, { appId: appA }).actions.canReserveName, false, 'account quota cannot be inferred from app quota alone');
});

test('revocation keeps address facts but removes all links, actions and live source claims', async t => {
  const f = await fixture(t); const alpha = f.claim('alpha'); f.publish([alpha], 'anyone');
  f.db.exec('BEGIN IMMEDIATE');
  f.db.prepare("UPDATE local_apps SET state='revoked',revision=revision+1 WHERE id=?").run(appA);
  f.publications.revokeInTransaction(appA); f.db.exec('COMMIT');
  let observed = false;
  const result = f.reader({ inspectSource: () => { observed = true; throw new Error('must not observe revoked app'); } }).read(owner, { appId: appA });
  assert.equal(result.app.state, 'revoked'); assert.equal(result.app.revision, 2); assert.equal(observed, false);
  assert.equal(result.addresses.canonical.origin, legacy.replace('{appId}', appA));
  assert.equal(result.addresses.canonical.shareUrl, null); assert.equal(result.addresses.aliases[0].origin, 'https://alpha.apps.example');
  assert.equal(result.addresses.aliases[0].shareUrl, null);
  assert.deepEqual(result.actions, { canReserveName: false, canEdit: false, canPublish: false, canPreview: false });
  assert.deepEqual(result.source.observation, unknown);
});

test('unsafe legacy entry paths remain inspectable and restrictable but never become share or preview links', async t => {
  for (const path of ['/x/..//double', '/x%2f..%2f_soty/session', '/x%2f..%2f%2fdouble']) {
    const f = await fixture(t, { entryPath: path }); const alpha = f.claim('alpha'); f.publish([alpha], 'anyone');
    const result = f.reader().read(owner, { appId: appA });
    assert.equal(result.source.entryPath, path); assert.equal(result.addresses.canonical.shareUrl, null);
    assert.equal(result.addresses.aliases[0].shareUrl, null); assert.equal(result.actions.canPreview, false);
    assert.equal(result.actions.canEdit, true); assert.equal(result.actions.canPublish, true);
  }
});

test('source observations distinguish absent, offline, stale, responding and unreachable without claiming health', async t => {
  const f = await fixture(t);
  const values = [undefined, null, unknown,
    { state: 'offline', observedAt: null, freshUntil: null, evidence: 'connector-offline' },
    { state: 'unknown', observedAt: 100, freshUntil: 45100, evidence: 'connector-v1-observation' },
    { state: 'responding', observedAt: 200, freshUntil: 45200, evidence: 'connector-v1-observation' },
    { state: 'unreachable', observedAt: 300, freshUntil: 45300, evidence: 'connector-v1-observation' },
    { state: 'responding', observedAt: 400, freshUntil: 45400, evidence: 'connector-v2-observation' },
    { state: 'unknown', observedAt: 400, freshUntil: 45400, evidence: 'connector-v2-observation' }];
  for (const value of values) {
    const result = f.reader({ inspectSource: () => value }).read(owner, { appId: appA });
    assert.deepEqual(result.source.observation, value ?? unknown);
    assert.equal(result.actions.canPreview, true, 'preview is a permitted attempt, independent of source observation');
  }
});

test('observer cannot return a replacement identity, ready flag, async observation or inconsistent evidence', async t => {
  const f = await fixture(t);
  for (const value of [{ ...unknown, hostDeviceId: 'wrong' }, { ...unknown, port: 1234 }, { ...unknown, runtimeReady: true },
    { ...unknown, state: 'ready' }, { state: 'responding', observedAt: null, freshUntil: null, evidence: 'not-observed' },
    { state: 'responding', observedAt: 2, freshUntil: 1, evidence: 'connector-v1-observation' },
    { state: 'offline', observedAt: 1, freshUntil: 2, evidence: 'connector-offline' }, Promise.resolve(unknown)]) {
    assert.throws(() => f.reader({ inspectSource: () => value }).read(owner, { appId: appA }), code('apps_observation_invalid'));
    assert.equal(f.db.isTransaction, false);
  }
  assert.deepEqual(f.reader().read(owner, { appId: appA }).source.observation, unknown);
});

test('observer failure or a late synchronous actor invalidation rolls back the read and leaks no result', async t => {
  const f = await fixture(t); const before = f.db.prepare('SELECT total_changes() AS n').get().n;
  assert.throws(() => f.reader({ inspectSource: () => { throw new Error('private observer detail'); } }).read(owner, { appId: appA }),
    error => error.code === 'apps_observation_unavailable' && error.message === 'apps_observation_unavailable');
  assert.equal(f.db.isTransaction, false);
  assert.equal(f.db.prepare('SELECT total_changes() AS n').get().n, before);
  assert.throws(() => f.reader({ inspectSource: () => { f.active.delete(owner); return unknown; } }).read(owner, { appId: appA }), code('apps_authentication_required'));
  assert.equal(f.db.isTransaction, false);
});

test('configuration accepts only exact shell and named origins, and no async actor validator', async t => {
  const f = await fixture(t);
  for (const shellOrigin of ['javascript:evil()', 'https://user:pass@shell.example', 'https://shell.example/path', 'https://shell.example/a/..',
    'https://shell.example?returnUrl=evil', 'https://shell.example/#launch', '//shell.example']) assert.throws(() => f.reader({ shellOrigin }), code('apps_inspection_shell_origin_invalid'));
  for (const namedAppZone of ['javascript:evil()', 'https://user:pass@apps.example', 'https://apps.example/path', 'https://apps.example?x=1'])
    assert.throws(() => f.reader({ namedAppZone }));
  assert.throws(() => f.reader({ nameClaimsEnabled: 'yes' }), code('apps_inspection_configuration_invalid'));
  assert.throws(() => f.reader({ assertActor: async () => true }).read(owner, { appId: appA }), code('apps_inspection_actor_validator_invalid'));
  assert.equal(f.db.isTransaction, false);
});

test('checkedAt captures the server clock after observation and invalid clocks fail without an open read', async t => {
  const f = await fixture(t); let clock = 1000;
  const result = f.reader({ now: () => clock, inspectSource: () => {
    clock = 2000; return { state: 'responding', observedAt: 1000, freshUntil: 46000, evidence: 'connector-v1-observation' };
  } }).read(owner, { appId: appA });
  assert.equal(result.checkedAt, 2000);
  assert.equal(result.source.observation.freshUntil - result.checkedAt, 44000);
  for (const value of [-1, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, undefined, '2000']) {
    assert.throws(() => f.reader({ now: () => value }).read(owner, { appId: appA }), code('apps_inspection_clock_invalid'));
    assert.equal(f.db.isTransaction, false);
  }
  assert.throws(() => f.reader({ now: () => { throw new Error('private clock detail'); } }).read(owner, { appId: appA }),
    error => error.code === 'apps_inspection_clock_invalid' && error.message === 'apps_inspection_clock_invalid');
  assert.throws(() => f.reader({ now: 2000 }), code('apps_inspection_clock_invalid'));
  assert.equal(f.reader({ now: () => 0 }).read(owner, { appId: appA }).checkedAt, 0);
  assert.equal(f.db.isTransaction, false);
});
