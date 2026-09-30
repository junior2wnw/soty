import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { migrateAppsSchema, ensureCanonicalDomain } from '../server/schema.mjs';
import { createPublicationRegistry } from '../server/publications.mjs';
import { createSourceRegistry } from '../server/sources.mjs';
import { createDiscussionRegistry } from '../server/discussions.mjs';
import { createEngagementEntryResolver } from '../server/engagement-access.mjs';

// Integration at the policy transaction boundary. Source prepare evidence is
// deliberately synthetic here; signed transport and live bindings have their
// own suites. SQL faults check atomicity, not connector reachability.
const owner = Object.freeze({ accountId: 'policy_owner', deviceId: 'policy_browser', label: 'Owner' });
const id = `app-${'e'.repeat(32)}`, alias = `dom_${'f'.repeat(32)}`;
const legacy = 'https://{appId}.legacy.example';
const keyA = 'link_A|host_A|connector_A', keyB = 'link_B|host_B|connector_B';
const tables = ['local_apps', 'local_app_grants', 'app_publications', 'app_publication_domains', 'app_publication_receipts',
  'app_runtime_targets', 'app_source_heads', 'app_source_receipts', 'app_discussion_heads', 'app_discussion_conversations',
  'app_discussion_messages', 'app_discussion_changes', 'app_discussion_usage'];

function fixture(t) {
  const db = new DatabaseSync(':memory:'); db.exec('PRAGMA foreign_keys=ON');
  migrateAppsSchema(db, { legacyTemplate: legacy });
  let clock = 1_800_000_000_000, serial = 0;
  const now = () => clock;
  const assertActor = actor => assert.equal(actor?.accountId, owner.accountId);
  const canUse = actor => actor.accountId === owner.accountId;
  const publications = createPublicationRegistry({ db, now, assertActor, canUse });
  for (const [key, suffix] of [[keyA, 'A'], [keyB, 'B']]) {
    db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run(key, owner.accountId,
      JSON.stringify({ linkId: `link_${suffix}`, hostDeviceId: `host_${suffix}`, connectorId: `connector_${suffix}` }), `Host ${suffix}`, clock);
  }
  db.exec('BEGIN IMMEDIATE');
  db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(id, owner.accountId, keyA, 'Policy specimen', 8080, '/start',
    JSON.stringify({ accountIds: [], communityIds: [] }), 'enabled', 1, clock, clock);
  const app = db.prepare('SELECT * FROM local_apps WHERE id=?').get(id);
  ensureCanonicalDomain(db, app, legacy); publications.initForApp(app);
  db.exec("INSERT INTO app_domain_zones VALUES ('zone_named','named','https://{slug}.apps.example','apps.example','https','',1)");
  db.prepare("INSERT INTO app_domains VALUES (?,'zone_named','policy.apps.example','https://policy.apps.example','policy',?,?,'alias','bound',1,NULL)")
    .run(alias, id, owner.accountId);
  db.exec('COMMIT');
  const canonical = db.prepare("SELECT id FROM app_domains WHERE app_id=? AND role='canonical'").get(id).id;
  const entry = { appId: id, domainId: canonical, path: '/start' };
  const discussion = createDiscussionRegistry({ db, now, assertActor, withAuthorityFence: callback => callback(),
    resolveEntry: createEngagementEntryResolver({ db, assertActor, publications, inspectSource: () => ({ state: 'offline' }) }),
    canUse, readCommunityAuthority: () => [], authorLabel: actor => actor.label });
  const sources = createSourceRegistry({ db, now, assertActor, publications,
    prepareTarget: async context => ({ target: context.target }), verifyPreparedTarget: context => context.evidence?.target === context.target });
  t.after(() => { discussion.close(); sources.close(); db.close(); });
  const call = (op, args) => discussion.execute({ actor: owner, op, args });
  const context = () => call('apps.discussion.context', entry);
  const head = () => db.prepare('SELECT * FROM app_discussion_heads WHERE app_id=?').get(id);
  const rows = () => Object.fromEntries(tables.map(table => [table, db.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all()]));
  const policy = () => publications.execute({ actor: owner, op: 'apps.publication.get', args: { appId: id } });
  function publicationArgs(launchPolicy, extra = {}) {
    const current = policy();
    return { appId: id, requestId: `publication_${++serial}`, expectedPolicyEpoch: current.policyEpoch,
      expectedTargetRevision: current.activeTargetRevision, launchPolicy, listed: false, activeDomainIds: [alias],
      ...(launchPolicy === 'anyone' ? { exposureAck: { scope: 'whole-port', targetRevision: current.target.revision,
        targetDigest: current.target.digest, profile: current.target.profile } } : {}), ...extra };
  }
  const publish = args => publications.execute({ actor: owner, op: 'apps.publication.update', args });
  const prepare = () => sources.execute({ actor: owner, op: 'apps.source.prepare', args: { appId: id,
    expectedPolicyEpoch: policy().policyEpoch, expectedTargetRevision: policy().activeTargetRevision,
    source: { hostDeviceId: 'host_B', connectorId: 'connector_B', port: 9000 + ++serial, entryPath: '/next' } } });
  function promotionArgs(prepared, launchPolicy) {
    return { appId: id, requestId: `promotion_${++serial}`, preparationId: prepared.preparationId,
      expectedPolicyEpoch: prepared.expectedPolicyEpoch, expectedTargetRevision: prepared.expectedTargetRevision,
      launchPolicy, listed: false, ...(launchPolicy === 'anyone' ? { exposureAck: { scope: 'whole-port',
        targetRevision: prepared.target.revision, targetDigest: prepared.target.digest, profile: prepared.target.profile } } : {}) };
  }
  const promote = args => sources.execute({ actor: owner, op: 'apps.source.promote', args });
  function send(body = 'Private history') {
    clock += 2500;
    return call('apps.discussion.send', { ...entry, conversationId: context().context.conversationId, body, requestId: `message_${++serial}` });
  }
  return { db, context, head, rows, policy, publish, publicationArgs, prepare, promotionArgs, promote, send };
}

test('D2 policy hooks allocate no unseen head and do not rotate for a label, listing or same audience', t => {
  const f = fixture(t);
  f.publish(f.publicationArgs('anyone'));
  assert.equal(f.head(), undefined);
  f.context(); const before = f.head();
  f.db.prepare('UPDATE local_apps SET name=? WHERE id=?').run('New name', id);
  f.publish(f.publicationArgs('anyone', { listed: true }));
  f.publish(f.publicationArgs('anyone', { activeDomainIds: [], listed: false }));
  assert.equal(f.head().current_id, before.current_id); assert.equal(f.head().generation, before.generation);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM app_discussion_conversations').get().n, 0);
});

test('D2 source-only promotion keeps the conversation; source plus audience change rotates it atomically', async t => {
  const f = fixture(t); f.send(); const before = f.head();
  f.promote(f.promotionArgs(await f.prepare(), 'restricted'));
  assert.equal(f.head().current_id, before.current_id);
  const second = f.promotionArgs(await f.prepare(), 'anyone'); f.promote(second);
  const after = f.head(); assert.notEqual(after.current_id, before.current_id);
  assert.equal(after.generation, before.generation + 1); assert.equal(after.mode, 'anyone');
  assert.equal(f.db.prepare('SELECT mode FROM app_discussion_conversations WHERE id=?').get(before.current_id).mode, 'restricted');
  const replay = f.promote(second); assert.equal(replay.replayed, true); assert.equal(f.head().current_id, after.current_id);
});

test('D2 a discussion rotation fault rolls publication and receipt back; the exact request remains retryable', t => {
  const f = fixture(t); f.send(); const before = f.rows(), args = f.publicationArgs('anyone');
  f.db.exec("CREATE TRIGGER discussion_policy_fault BEFORE UPDATE OF current_id ON app_discussion_heads BEGIN SELECT RAISE(ABORT,'injected_discussion_fault'); END");
  assert.throws(() => f.publish(args), /injected_discussion_fault/); assert.deepEqual(f.rows(), before);
  f.db.exec('DROP TRIGGER discussion_policy_fault');
  const committed = f.publish(args); assert.equal(committed.replayed, false);
  const current = f.head(); assert.equal(current.generation, before.app_discussion_heads[0].generation + 1);
  assert.equal(f.publish(args).replayed, true); assert.equal(f.head().current_id, current.current_id);
});

test('D2 a source receipt fault restores source, policy and rotated audience together before exact retry', async t => {
  const f = fixture(t); f.send(); const args = f.promotionArgs(await f.prepare(), 'anyone'), before = f.rows();
  f.db.exec("CREATE TRIGGER discussion_source_fault BEFORE INSERT ON app_source_receipts BEGIN SELECT RAISE(ABORT,'injected_source_receipt_fault'); END");
  assert.throws(() => f.promote(args), /injected_source_receipt_fault/); assert.deepEqual(f.rows(), before);
  f.db.exec('DROP TRIGGER discussion_source_fault');
  const accepted = f.promote(args); assert.equal(accepted.replayed, false);
  assert.equal(f.head().generation, before.app_discussion_heads[0].generation + 1);
  assert.equal(f.policy().activeTargetRevision, 2);
});
