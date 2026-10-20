import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPairSync, sign, randomUUID } from 'node:crypto';
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, realpathSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createConnectService, digestArgs } from '../../connect/server/index.mjs';
import { createWorldService } from '../../world/server/index.mjs';
import { createAppsService, operations as appOperations } from '../server/index.mjs';
import { migrateAppsSchema, inspectAppsSchema, ensureCanonicalDomain, ensureInitialPublication } from '../server/schema.mjs';
import { readAppAudience } from '../server/audience.mjs';
import { inspectAppsSchema as frozenOldReader } from './support/audience-old-schema-reader.mjs';
import { createCapabilitiesService, ACCESS_OPERATIONS } from '../../capabilities/server/index.mjs';
import { createTrustedReadonlyQueryAdapter } from '../../capabilities/server/readonly-queries.mjs';
import { fixtureDocumentation } from '../../capabilities/test/support/documentation.mjs';
import { captureExternalApplications, composeExternalApplications } from '../../../server/external-applications.js';
import { readStorageFormat } from '../../../deploy/connector/storage-probe.mjs';
import { assertStorageCompatible, currentStorageReaders, storageReaderLabel } from '../../../deploy/connector/storage-guard.mjs';

const OLD_STORAGE_READERS = '{"version":6,"readers":{"rooms":[1,2],"apps":[1,2,3,4,5,6,7,8],"notes":[1,2],"capabilities":[1,2,3],"appRegistration":[1],"feedback":[1],"humanIdentity":[1,2,3],"connect":[3,4]}}';
const PROJECT = 'app-audience-tests', ORIGIN = 'https://audience.example';
const LEGACY = 'https://{appId}.legacy.example', NAMED = 'https://apps.example';
const code = expected => error => error?.code === expected;
function keys() {
  const a = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), b = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { privateKey: a.privateKey, publicJwk: a.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: b.publicKey.export({ format: 'jwk' }) };
}
function good(value) { assert.equal(value.ok, true, value.error?.code); return value; }
function entryArgs(context) { const { appId, domainId, path: entryPath } = context.entry; return { appId, domainId, path: entryPath }; }
async function rpc(connect, identity, op, args = {}) {
  const challenge = good(await connect.handle({ op: 'challenge', origin: ORIGIN, args: { operation: op, digest: digestArgs(args) } }));
  return connect.handle({ op, args, origin: ORIGIN, proof: { challengeId: challenge.challengeId, publicJwk: identity.publicJwk,
    signature: sign('sha256', Buffer.from(challenge.message), { key: identity.privateKey, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
}
async function fixture(t, options = {}) {
  const parent = realpathSync(tmpdir()), dir = mkdtempSync(path.join(parent, 'soty-app-audience-')), nonce = randomUUID();
  writeFileSync(path.join(dir, 'owned'), nonce, { flag: 'wx' }); mkdirSync(path.join(dir, 'apps'));
  const databasePath = path.join(dir, 'apps', 'registry.sqlite'), connectPath = path.join(dir, 'synthetic-connect.sqlite');
  let apps, caps, contactDepth = 0, serial = 0;
  const connect = createConnectService({ databasePath: connectPath, projectId: PROJECT, allowedOrigins: [ORIGIN], extensions: [
    { operations: appOperations, execute: request => apps.execute(request) },
    { operations: new Set(ACCESS_OPERATIONS), execute: request => caps.execute(request) },
  ] });
  const world = createWorldService({ databasePath: path.join(dir, 'synthetic-world.sqlite'), projectId: PROJECT,
    withCurrentContacts: (actor, fn) => connect.withCurrentContacts(actor, fn) });
  const identities = { owner: keys(), friend: keys(), stranger: keys() }, accounts = {};
  t.after(() => {
    caps?.close(); apps?.close(); world.close(); connect.close();
    const actual = realpathSync(dir); assert.equal(path.dirname(actual), parent); assert.match(path.basename(actual), /^soty-app-audience-/u);
    assert.equal(readFileSync(path.join(actual, 'owned'), 'utf8'), nonce); rmSync(actual, { recursive: true });
  });
  for (const name of Object.keys(identities)) accounts[name] = good(await rpc(connect, identities[name], 'bootstrap', {
    label: 'Synthetic ' + name, encryptionPublicJwk: identities[name].encryptionPublicJwk }));
  const id = 'app-' + 'a'.repeat(32), connector = 'synthetic-link|synthetic-host|synthetic-connector';
  const flags = { allowScopedEmbedMigration: options.base >= 7, allowSelectedResourceMigration: options.base >= 8, allowAudienceMigration: true };
  const db = new DatabaseSync(databasePath); db.exec('PRAGMA foreign_keys=ON');
  try {
    migrateAppsSchema(db, { legacyTemplate: LEGACY, ...flags }); db.exec('BEGIN IMMEDIATE');
    db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run(connector, accounts.owner.accountId,
      '{"linkId":"synthetic-link","hostDeviceId":"synthetic-host","connectorId":"synthetic-connector"}', 'Synthetic offline device', 1);
    const grants = { accountIds: [accounts.stranger.accountId], communityIds: [] };
    db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(id, accounts.owner.accountId, connector, 'Synthetic existing app',
      8080, '/', JSON.stringify(grants), 'enabled', 1, 1, 1);
    db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)').run(id, 'account', accounts.stranger.accountId);
    const row = db.prepare('SELECT * FROM local_apps WHERE id=?').get(id); ensureCanonicalDomain(db, row, LEGACY); ensureInitialPublication(db, row); db.exec('COMMIT');
  } finally { db.close(); }
  function open(extra = {}) {
    apps = createAppsService({ databasePath, appOriginTemplate: LEGACY, namedAppZone: NAMED, shellOrigins: [ORIGIN], ...flags,
      actorActive: actor => connect.isActorActive(actor),
      withCurrentContacts: (actor, fn) => connect.withCurrentContacts(actor, view => { contactDepth++; try { return fn(view); } finally { contactDepth--; } }),
      withAuthorityFence: fn => { assert.ok(contactDepth > 0, 'Connect contact fence precedes the actual World BEGIN'); return world.withCommunityAuthorityFence(fn); },
      canAccessCommunity: (a, c) => world.canAccessCommunity(a, c), activeCommunityIds: a => world.activeCommunityIds(a),
      isGroupAdmin: (a, c) => world.isGroupAdmin(a, c), readCommunityAuthority: (actor, owner, ids) => world.appCommunityAuthority(actor.accountId, owner, ids),
      ...extra });
  }
  open();
  const call = async (name, op, args = {}) => good(await rpc(connect, identities[name], op, { expectedAccountId: accounts[name].accountId, ...args }));
  const raw = (name, op, args = {}) => rpc(connect, identities[name], op, { expectedAccountId: accounts[name].accountId, ...args });
  const inspect = async () => (await call('owner', 'apps.inspect', { appId: id }));
  async function alias(slug) {
    const snapshot = await inspect(); return (await call('owner', 'apps.domains.claim', { appId: id, expectedDomainsRevision: snapshot.addresses.revision,
      requestId: 'alias-' + ++serial, slug })).receipt.domainId;
  }
  async function publish(audience, activeDomainIds) {
    const current = await call('owner', 'apps.publication.get', { appId: id });
    const named = ['link', 'public'].includes(audience), domains = activeDomainIds ?? current.activeDomainIds;
    return call('owner', 'apps.publication.update', { appId: id, requestId: 'policy-' + ++serial, expectedPolicyEpoch: current.policyEpoch,
      expectedTargetRevision: current.activeTargetRevision, audience, launchPolicy: named ? 'anyone' : 'restricted', listed: audience === 'public', activeDomainIds: domains,
      ...(named ? { exposureAck: { scope: 'whole-port', targetRevision: current.target.revision, targetDigest: current.target.digest, profile: current.target.profile } } : {}) });
  }
  async function request() {
    const card = good(await rpc(connect, identities.owner, 'card.get'));
    return good(await rpc(connect, identities.friend, 'contacts.request', { cardId: card.cardId }));
  }
  async function accept() { const pending = await request(); return good(await rpc(connect, identities.owner, 'contacts.accept', { requestId: pending.requestId })); }
  return { dir, databasePath, connect, world, id, identities, accounts, call, raw, inspect, alias, publish, request, accept,
    get apps() { return apps; }, installCaps(value) { caps = value; },
    read(action) { const value = new DatabaseSync(databasePath, { readOnly: true }); try { return action(value); } finally { value.close(); } },
    write(action) { const value = new DatabaseSync(databasePath); value.exec('PRAGMA foreign_keys=ON;BEGIN IMMEDIATE'); try { const result = action(value); value.exec('COMMIT'); return result; } catch (error) { if (value.isTransaction) value.exec('ROLLBACK'); throw error; } finally { value.close(); } },
    reopen(extra) { apps.close(); open(extra); },
  };
}

for (const base of [6, 7, 8]) test('audience format retains base' + base + ', exact rows, old-reader refusal and independent reader admission', async t => {
  const f = await fixture(t, { base });
  assert.equal(f.read(inspectAppsSchema), 'v' + (base + 3));
  assert.equal((await f.inspect()).publication.audience, null);
  const before = f.read(db => JSON.stringify(db.prepare('SELECT * FROM local_apps').all()));
  f.read(db => assert.throws(() => frozenOldReader(db), code('apps_schema_unsupported')));
  const format = await readStorageFormat(f.dir); assert.equal(format.apps, base + 3);
  const image = readers => ({ Id: 'sha256:' + 'a'.repeat(64), Config: { Labels: { [storageReaderLabel]: readers } } });
  assert.throws(() => assertStorageCompatible(image(OLD_STORAGE_READERS), format), code('storage_reader_incompatible'));
  assert.equal(assertStorageCompatible(image(currentStorageReaders), format).apps, base + 3);
  f.reopen(); assert.equal(f.read(db => JSON.stringify(db.prepare('SELECT * FROM local_apps').all())), before);
  assert.equal(f.read(db => Number(db.prepare('PRAGMA user_version').get().user_version)), base + 3);
});

test('explicit private ignores dormant legacy grants, while explicit null restores the prior custom audience', async t => {
  const f = await fixture(t);
  assert.equal((await f.call('stranger', 'apps.list')).apps.length, 1, 'legacy account grant remains effective before explicit mode');
  const original = f.read(db => db.prepare('SELECT grants_json FROM local_apps').get().grants_json);
  await f.publish('private');
  assert.equal((await f.call('stranger', 'apps.list')).apps.length, 0);
  assert.equal((await f.call('owner', 'apps.list')).apps[0].publication.audience, 'private');
  assert.equal((await f.call('stranger', 'apps.directory.resolve', { appIds: [f.id] })).items[0].available, false);
  const foreign = await f.raw('stranger', 'apps.publication.update', { appId: f.id, requestId: 'foreign-mode', expectedPolicyEpoch: 2, expectedTargetRevision: 1,
    audience: 'public', launchPolicy: 'anyone', listed: true, activeDomainIds: [] });
  assert.equal(foreign.ok, false);
  f.reopen(); assert.equal((await f.call('stranger', 'apps.list')).apps.length, 0);
  await f.publish(null); assert.equal((await f.call('stranger', 'apps.list')).apps.length, 1);
  assert.equal(f.read(db => db.prepare('SELECT grants_json FROM local_apps').get().grants_json), original);
});

test('friends discovery and access use current signed reciprocal contacts through removal, block, reload and missing-port denial', async t => {
  const f = await fixture(t); await f.publish('friends');
  const list = async () => (await f.call('friend', 'apps.directory.search', { scope: 'all' })).apps;
  assert.deepEqual(await list(), []); const pending = await f.request(); assert.deepEqual(await list(), []);
  const relation = good(await rpc(f.connect, f.identities.owner, 'contacts.accept', { requestId: pending.requestId }));
  assert.equal((await list())[0].id, f.id); assert.equal((await f.call('friend', 'apps.list')).apps[0].id, f.id);
  assert.equal((await f.call('friend', 'apps.directory.search', { scope: 'public' })).apps.length, 0);
  assert.equal((await f.call('stranger', 'apps.list')).apps.length, 0, 'dormant direct grant is not unioned into friends');
  const entry = (await f.call('friend', 'apps.entry.get', { appId: f.id })).entry;
  const decision = f.apps.policy.decideAccess({ domainId: entry.domainId, origin: entry.origin, actor: f.accounts.friend });
  assert.equal(decision.accessBasis, 'grant');
  good(await rpc(f.connect, f.identities.owner, 'contacts.remove', { relationshipId: relation.relationshipId }));
  assert.throws(() => f.apps.policy.recheckAccess(decision), code('apps_access_denied'));
  assert.deepEqual(await list(), []); f.reopen(); assert.deepEqual(await list(), []);
  await f.accept(); good(await rpc(f.connect, f.identities.friend, 'contacts.block', { peerAccountId: f.accounts.owner.accountId }));
  assert.deepEqual(await list(), []);
  good(await rpc(f.connect, f.identities.friend, 'contacts.unblock', { peerAccountId: f.accounts.owner.accountId }));
  assert.deepEqual(await list(), []); await f.accept(); assert.equal((await list()).length, 1);
  f.reopen({ withCurrentContacts: undefined }); assert.equal((await f.call('friend', 'apps.list')).apps.length, 0);
});

test('link has exact named access without public discovery; public lists; last alias and revoke atomically close the explicit mode', async t => {
  const f = await fixture(t), one = await f.alias('one-address'), two = await f.alias('two-address');
  await f.publish('link', [one, two]);
  assert.equal((await f.call('stranger', 'apps.directory.search', { scope: 'public' })).apps.length, 0);
  const snapshot = await f.inspect(), origin = snapshot.addresses.aliases.find(row => row.id === one).origin;
  assert.equal(f.apps.policy.decideAccess({ domainId: one, origin }).subject, 'public');
  await f.publish('public', [one, two]); assert.equal(f.apps.publicCatalog().apps.length, 1);
  const publicConversation = (await f.call('owner', 'apps.discussion.context', { appId: f.id })).context.conversationId;
  let current = await f.inspect();
  await f.call('owner', 'apps.domains.retire', { appId: f.id, domainId: one, requestId: 'retire-one', expectedDomainsRevision: current.addresses.revision });
  assert.equal((await f.inspect()).publication.audience, 'public');
  current = await f.inspect(); const beforeEpoch = current.publication.policyEpoch;
  await f.call('owner', 'apps.domains.retire', { appId: f.id, domainId: two, requestId: 'retire-last', expectedDomainsRevision: current.addresses.revision });
  current = await f.inspect(); assert.equal(current.publication.audience, 'private'); assert.equal(current.publication.launchPolicy, 'restricted');
  assert.equal(current.publication.listed, false); assert.equal(current.publication.policyEpoch, beforeEpoch + 1);
  assert.notEqual((await f.call('owner', 'apps.discussion.context', { appId: f.id })).context.conversationId, publicConversation);
  assert.equal(f.apps.publicCatalog().apps.length, 0); f.reopen(); assert.equal((await f.inspect()).publication.audience, 'private');
  const again = await f.alias('third-address'); await f.publish('link', [again]);
  await f.call('owner', 'apps.revoke', { appId: f.id });
  assert.equal(f.read(db => readAppAudience(db, f.id)), 'private');
  f.reopen(); assert.equal(f.read(db => readAppAudience(db, f.id)), 'private');
});

test('audience receipts are exact, retained v1 intents recover and neither CAS nor foreign aliases can broaden a mode', async t => {
  const f = await fixture(t), alias = await f.alias('exact-policy');
  const initial = await f.call('owner', 'apps.publication.get', { appId: f.id });
  const args = { appId: f.id, requestId: 'pending-legacy', expectedPolicyEpoch: initial.policyEpoch, expectedTargetRevision: 1,
    launchPolicy: 'restricted', listed: false, activeDomainIds: [alias] };
  const legacy = await f.call('owner', 'apps.publication.update', args); assert.equal(legacy.receipt.schema, 'soty.app-publication-receipt.v1');
  const v2 = await f.publish('friends'); assert.equal(v2.receipt.schema, 'soty.app-publication-receipt.v2'); assert.equal(v2.receipt.audience, 'friends');
  const replay = await f.call('owner', 'apps.publication.update', args); assert.equal(replay.replayed, true); assert.deepEqual(replay.receipt, legacy.receipt);
  const mixed = await f.raw('owner', 'apps.publication.update', { ...args, audience: null }); assert.equal(mixed.ok, false); assert.equal(mixed.error.code, 'app_publication_request_conflict');
  const stale = await f.raw('owner', 'apps.publication.update', { ...args, requestId: 'new-stale', audience: 'private' });
  assert.equal(stale.ok, false); assert.equal(stale.error.code, 'app_publication_revision_conflict');
  const current = await f.call('owner', 'apps.publication.get', { appId: f.id });
  let invalidSerial = 0;
  const foreignAppId = 'app-' + 'b'.repeat(32), foreignKey = 'foreign-link|foreign-host|foreign-connector';
  f.write(db => {
    db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run(foreignKey, f.accounts.stranger.accountId,
      '{"linkId":"foreign-link","hostDeviceId":"foreign-host","connectorId":"foreign-connector"}', 'Foreign synthetic device', 1);
    db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(foreignAppId, f.accounts.stranger.accountId, foreignKey, 'Foreign synthetic app',
      8081, '/', '{"accountIds":[],"communityIds":[]}', 'enabled', 1, 1, 1);
    const row = db.prepare('SELECT * FROM local_apps WHERE id=?').get(foreignAppId); ensureCanonicalDomain(db, row, LEGACY); ensureInitialPublication(db, row);
  });
  const foreignAlias = (await f.call('stranger', 'apps.domains.claim', { appId: foreignAppId, slug: 'foreign-policy', requestId: 'foreign-alias', expectedDomainsRevision: 0 })).receipt.domainId;
  const foreignDomain = await f.raw('owner', 'apps.publication.update', { ...args, requestId: 'deny-foreign-domain', expectedPolicyEpoch: current.policyEpoch,
    audience: 'friends', activeDomainIds: [foreignAlias] });
  assert.equal(foreignDomain.ok, false); assert.equal(foreignDomain.error.code, 'app_publication_domain_unavailable');
  assert.equal((await f.call('owner', 'apps.publication.get', { appId: f.id })).policyEpoch, current.policyEpoch);
  for (const patch of [{ audience: 'private', launchPolicy: 'anyone', listed: false }, { audience: 'friends', launchPolicy: 'restricted', listed: true },
    { audience: 'public', launchPolicy: 'anyone', listed: true, activeDomainIds: [] }, { audience: 'circle' }, { audience: ['friends'] }]) {
    const result = await f.raw('owner', 'apps.publication.update', { ...args, requestId: 'invalid-' + ++invalidSerial, expectedPolicyEpoch: current.policyEpoch, audience: 'friends', ...patch });
    assert.equal(result.ok, false); assert.equal((await f.call('owner', 'apps.publication.get', { appId: f.id })).policyEpoch, current.policyEpoch);
  }
  f.reopen(); assert.equal((await f.call('owner', 'apps.publication.update', args)).replayed, true);
});

test('friends discussion does not expose prior private history and former friends lose current history, changes and archives', async t => {
  const f = await fixture(t); await f.publish('private');
  const privateContext = (await f.call('owner', 'apps.discussion.context', { appId: f.id })).context;
  await f.call('owner', 'apps.discussion.send', { ...entryArgs(privateContext), conversationId: privateContext.conversationId, requestId: 'private-message', body: 'Private before sharing' });
  await f.publish('friends'); const relation = await f.accept();
  const shared = await f.call('friend', 'apps.discussion.context', { appId: f.id }); assert.notEqual(shared.context.conversationId, privateContext.conversationId);
  assert.deepEqual(shared.messages, []); assert.equal(shared.context.audienceMode, 'friends');
  const privateRead = await f.raw('friend', 'apps.discussion.history', { ...entryArgs(shared.context), conversationId: privateContext.conversationId }); assert.equal(privateRead.ok, false);
  const posted = await f.call('friend', 'apps.discussion.send', { ...entryArgs(shared.context), conversationId: shared.context.conversationId, requestId: 'friend-message', body: 'Shared with current friends' });
  assert.equal(posted.message.body, 'Shared with current friends');
  assert.deepEqual((await f.call('friend', 'apps.discussion.archives', entryArgs(shared.context))).entries, []);
  good(await rpc(f.connect, f.identities.owner, 'contacts.remove', { relationshipId: relation.relationshipId }));
  for (const [op, args] of [['history', { conversationId: shared.context.conversationId }], ['changes', { conversationId: shared.context.conversationId, cursor: shared.changeCursor }], ['archives', {}]]) {
    const result = await f.raw('friend', 'apps.discussion.' + op, { ...entryArgs(shared.context), ...args }); assert.equal(result.ok, false);
  }
  f.reopen(); assert.equal((await f.raw('friend', 'apps.discussion.context', { appId: f.id })).ok, false);
  const history = await f.call('owner', 'apps.discussion.history', { ...entryArgs(privateContext), conversationId: privateContext.conversationId }); assert.equal(history.messages[0].body, 'Private before sharing');
});

test('empty audience generations stay bounded and immutable materialized policy refuses corruption on reload', async t => {
  const f = await fixture(t); await f.publish('private'); await f.call('owner', 'apps.discussion.context', { appId: f.id });
  for (let n = 0; n < 16; n++) await f.publish(n % 2 ? 'private' : 'friends');
  const counts = () => f.read(db => [db.prepare('SELECT count(*) AS n FROM app_discussion_head_audiences').get().n,
    db.prepare('SELECT count(*) AS n FROM app_discussion_conversation_audiences').get().n]);
  assert.deepEqual(counts(), [1, 0]);
  const context = (await f.call('owner', 'apps.discussion.context', { appId: f.id })).context;
  await f.call('owner', 'apps.discussion.send', { ...entryArgs(context), conversationId: context.conversationId, requestId: 'materialize-one', body: 'Bound policy' });
  assert.deepEqual(counts(), [1, 1]);
  assert.throws(() => f.write(db => db.prepare("UPDATE app_discussion_conversation_audiences SET audience='public'").run()), /app_discussion_audience_immutable/u);
  assert.throws(() => f.write(db => db.prepare('DELETE FROM app_discussion_conversation_audiences').run()), /app_discussion_audience_immutable/u);
  f.reopen(); assert.deepEqual(counts(), [1, 1]);
});

test('real capability actor and snapshot compose through current Apps participant ACL without promoting a DTO to human proof', async t => {
  const f = await fixture(t); await f.publish('friends'); const relation = await f.accept();
  let sourceChecks = 0;
  const adapter = createTrustedReadonlyQueryAdapter({ withAuthority: (_request, fn) => { sourceChecks++; return fn(); }, query: async () => ({ readOnly: true }) });
  const capabilityId = 'app.audience:status', resource = 'app.audience:existing', binding = { id: 'app.audience:binding', version: 1, digest: 'b'.repeat(64) };
  const catalog = { capabilityId, version: 1, appId: f.id, title: 'Synthetic readonly authority', description: 'Existing Source scope only.', visibility: 'private', executionEnabled: true,
    inputSchema: { type: 'object', properties: {}, additionalProperties: false }, outputSchema: { type: 'object', properties: { readOnly: { type: 'boolean' } }, required: ['readOnly'], additionalProperties: false },
    resources: [resource], effects: [], recipients: [resource], executionBinding: { kind: 'registered', handler: capabilityId, version: 1, binding } };
  const target = (await f.call('owner', 'apps.publication.get', { appId: f.id })).target;
  const entries = captureExternalApplications([{ appId: f.id, target: { revision: target.revision, digest: target.digest }, catalog, adapter }]);
  let caps;
  const composed = composeExternalApplications(entries, { withConnectFence: fn => f.connect.withAuthorityFence(fn), capabilities: () => caps, apps: () => f.apps });
  caps = createCapabilitiesService({ databasePath: path.join(f.dir, 'synthetic-caps.sqlite'), projectId: PROJECT, actorActive: actor => f.connect.isActorActive(actor),
    catalog: entries.map(e => e.catalog), documentation: fixtureDocumentation(entries.map(e => e.catalog)), readonlyQueries: composed });
  f.installCaps(caps);
  const principal = (await f.call('friend', 'access.principals.create', { label: 'Existing synthetic delegated reader' })).principal;
  const grant = (await f.call('friend', 'access.grants.issue', { principalId: principal.id, capabilities: [{ capabilityId, version: 1 }], resources: [resource], effects: [], recipients: [resource],
    expiresAt: Date.now() + 600000, allowDelegation: false, maxDepth: 0, budget: { unit: 'invocations', limit: 2 } })).grant;
  const issued = await f.call('friend', 'access.credentials.issue', { grantId: grant.id, audience: ORIGIN });
  const actor = caps.authenticateCredential({ token: issued.token, audience: ORIGIN });
  const authorization = caps.authorize({ actor, capabilityId, version: 1, action: 'read' });
  assert.equal(composed[0].adapter.withAuthority({ actor }, () => 'actor-source-current'), 'actor-source-current');
  assert.equal(composed[0].adapter.withAuthority({ authorization }, () => 'snapshot-source-current'), 'snapshot-source-current');
  assert.equal(sourceChecks, 2);
  good(await rpc(f.connect, f.identities.owner, 'contacts.remove', { relationshipId: relation.relationshipId }));
  assert.throws(() => composed[0].adapter.withAuthority({ actor }, () => assert.fail()), code('apps_access_denied'));
  assert.throws(() => composed[0].adapter.withAuthority({ authorization }, () => assert.fail()), code('apps_access_denied'));
  assert.equal(sourceChecks, 2, 'no Source authority callback after current app admission is removed');
});


test('retiring the last link alias closes publication and historical exposure in the same epoch transition', async t => {
  const f = await fixture(t), named = await f.alias('link-last'); await f.publish('link', [named]);
  const before = await f.inspect(), origin = before.addresses.aliases.find(row => row.id === named).origin;
  const decision = f.apps.policy.decideAccess({ domainId: named, origin });
  const conversation = (await f.call('owner', 'apps.discussion.context', { appId: f.id })).context.conversationId;
  await f.call('owner', 'apps.domains.retire', { appId: f.id, domainId: named, requestId: 'link-last-retire', expectedDomainsRevision: before.addresses.revision });
  const current = await f.inspect(); assert.equal(current.publication.audience, 'private'); assert.equal(current.publication.launchPolicy, 'restricted');
  assert.equal(current.publication.listed, false); assert.equal(current.publication.policyEpoch, before.publication.policyEpoch + 1);
  assert.throws(() => f.apps.policy.recheckAccess(decision), code('apps_access_denied'));
  assert.notEqual((await f.call('owner', 'apps.discussion.context', { appId: f.id })).context.conversationId, conversation);
  const durable = f.read(db => db.prepare('SELECT exposure_ack_revision,exposure_ack_json FROM app_publications WHERE app_id=?').get(f.id));
  assert.equal(durable.exposure_ack_revision, null); assert.equal(durable.exposure_ack_json, null);
  f.reopen(); assert.equal((await f.inspect()).publication.audience, 'private');
});


test('editing dormant grants does not snapshot friends or reopen a removed friend or rotate the friends conversation', async t => {
  const f = await fixture(t); await f.publish('friends'); const relation = await f.accept();
  const before = (await f.call('friend', 'apps.discussion.context', { appId: f.id })).context.conversationId;
  const snapshot = await f.inspect();
  await f.call('owner', 'apps.update', { appId: f.id, expectedRevision: snapshot.app.revision,
    grants: { accountIds: [f.accounts.friend.accountId, f.accounts.stranger.accountId], communityIds: [] } });
  assert.equal((await f.call('stranger', 'apps.list')).apps.length, 0);
  assert.equal((await f.call('friend', 'apps.discussion.context', { appId: f.id })).context.conversationId, before);
  good(await rpc(f.connect, f.identities.owner, 'contacts.remove', { relationshipId: relation.relationshipId }));
  assert.equal((await f.call('friend', 'apps.list')).apps.length, 0, 'even the dormant manual account entry cannot bypass current friends removal');
  f.reopen(); assert.equal((await f.call('friend', 'apps.list')).apps.length, 0);
});


test('dormant grants on more than one scan window of private apps cannot leak IDs or hidden counts through a directory cursor', async t => {
  const f = await fixture(t); await f.publish('private');
  f.write(db => {
    const connector = db.prepare('SELECT connector_key FROM local_apps WHERE id=?').get(f.id).connector_key;
    for (let n = 16; n < 276; n++) {
      const id = 'app-' + n.toString(16).padStart(32, '0'), grants = JSON.stringify({ accountIds: [f.accounts.stranger.accountId], communityIds: [] });
      db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(id, f.accounts.owner.accountId, connector, 'Hidden existing app ' + n,
        9000 + n, '/', grants, 'enabled', 1, n, n);
      db.prepare('INSERT INTO local_app_grants VALUES (?,?,?)').run(id, 'account', f.accounts.stranger.accountId);
      const row = db.prepare('SELECT * FROM local_apps WHERE id=?').get(id); ensureCanonicalDomain(db, row, LEGACY); ensureInitialPublication(db, row);
      db.prepare('INSERT INTO app_audiences VALUES (?,?)').run(id, 'private');
    }
  });
  const result = await f.call('stranger', 'apps.directory.search', { scope: 'all', limit: 1 });
  assert.deepEqual(result.apps, []); assert.equal(result.nextCursor, null);
  assert.deepEqual((await f.call('stranger', 'apps.list')).apps, []);
});
