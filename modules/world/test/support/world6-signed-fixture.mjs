import assert from 'node:assert/strict';
import { generateKeyPairSync, sign, createHash } from 'node:crypto';
import { mkdtempSync, rmSync, realpathSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, basename } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createConnectService, digestArgs } from '../../../connect/server/index.mjs';
import { createWorldService } from '../../server/index.mjs';
import { createCoverDecoder } from '../../../cover-art/server/codec.mjs';
import { migrateWorld as migrateWorld5 } from './world5-schema-before.mjs';
import { migrateField } from '../../server/field.mjs';
import { createFieldDocument } from '../../../field/contract.mjs';
import { pngFixture, asDataUrl } from '../avatar-fixtures.mjs';
export const ORIGIN = 'https://world6-integration.test', PROJECT = 'world6-integration';
export const good = value => { assert.equal(value.ok, true, value.error?.code); return value; };
export const denied = (value, code) => { assert.equal(value.ok, false); if (code) assert.equal(value.error?.code, code); return value; };
export const hash = value => createHash('sha256').update(value).digest('hex');
export function actor(label) {
  const signing = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { key: signing.privateKey, publicJwk: signing.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }), label };
}
export function temporaryStore(t) {
  const base = realpathSync(tmpdir()), folder = mkdtempSync(join(base, 'soty-world6-integration-')); let beforeCleanup = () => {};
  t.after(() => { beforeCleanup(); const actual = realpathSync(folder); assert.equal(dirname(actual).toLowerCase(), base.toLowerCase());
    assert.ok(basename(actual).startsWith('soty-world6-integration-')); rmSync(actual, { recursive: true, force: true }); });
  return { folder, worldPath: join(folder, 'world.sqlite'), connectPath: join(folder, 'connect.sqlite'), beforeCleanup(callback) { beforeCleanup = callback; } };
}
export function seedWorld5(path, accounts, { corruptAvatar = false, now = 1900000000000 } = {}) {
  const db = new DatabaseSync(path); migrateWorld5(db, PROJECT); migrateField(db);
  const [alice, bob, carol] = accounts;
  db.exec('BEGIN IMMEDIATE');
  try {
    for (const [n, person] of accounts.entries()) db.prepare(`INSERT INTO profiles(account_id,display_name,search_text,audience,discoverable,
      revision,created_at,updated_at) VALUES(?,?,?, ?,?,17,?,?)`).run(person.accountId, person.label, person.label.toLowerCase(), n === 0 ? 'public' : 'private', n === 0 ? 1 : 0, now-1000, now-1000);
    const image = corruptAvatar ? Buffer.from('not-a-raster') : pngFixture(6, 4), thumb = pngFixture(2, 2);
    db.prepare('INSERT INTO profile_avatars VALUES(?,?,?,?,?)').run(alice.accountId, 'image/png', image, 'image/png', thumb);
    db.prepare('UPDATE profiles SET avatar_revision=13 WHERE account_id=?').run(alice.accountId);
    db.prepare(`INSERT INTO communities(id,owner_id,name,search_text,join_policy,created_at,updated_at)
      VALUES('community_legacy',?,'Legacy room','legacy room','invite',?,?)`).run(alice.accountId,now-1000,now-1000);
    for (const [person, role, state] of [[alice,'owner','active'],[bob,'member','active'],[carol,'member','banned']])
      db.prepare('INSERT INTO memberships(community_id,account_id,role,state,revision,joined_at,updated_at) VALUES(?,?,?,?,9,?,?)')
        .run('community_legacy',person.accountId,role,state,now-900,now-800);
    db.prepare(`INSERT INTO messages(seq,id,community_id,author_id,client_id,text,created_at)
      VALUES(42,'message_legacy','community_legacy',?,'client_legacy','Existing private text',?)`).run(bob.accountId,now-700);
    db.prepare('INSERT INTO receipts VALUES(?,?,?,?,?,?)').run(alice.accountId,'legacy.operation','legacy_request','accepted_digest','{"accepted":"keep-byte-for-byte"}',now-600);
    db.prepare('INSERT INTO world_audit(seq,account_id,operation,target_id,created_at) VALUES(27,?,?,?,?)').run(alice.accountId,'legacy.operation','community_legacy',now-500);
    const document = JSON.stringify(createFieldDocument()), contentHash = hash(document);
    db.prepare('INSERT INTO world_field_documents VALUES(?,1,?,?,?)').run(alice.accountId,document,contentHash,now-400);
    db.prepare('INSERT INTO world_field_receipts VALUES(?,?,?,?,1,?)').run(alice.accountId,'1'.repeat(64),'2'.repeat(64),contentHash,now-400);
    db.exec('COMMIT'); return { image, thumb };
  } catch (error) { db.exec('ROLLBACK'); throw error; } finally { db.close(); }
}
export function worldSnapshot(path) {
  const db = new DatabaseSync(path, { readOnly: true });
  try {
    const tables = ['profiles','communities','memberships','messages','receipts','world_audit','world_directory','world_directory_counts','world_field_documents','world_field_receipts','sqlite_sequence'];
    return Object.fromEntries(tables.map(name => [name, db.prepare(`SELECT rowid AS __rowid__,* FROM ${name} ORDER BY rowid`).all().map(row => ({ ...row }))]));
  } finally { db.close(); }
}
export async function signedFixture(t, { legacy = false, withDecoder = true, withContacts = true, registerCover = true, decoder,
  corruptAvatar = false } = {}) {
  const paths = temporaryStore(t), actors = [actor('Synthetic Alice'), actor('Synthetic Bob'), actor('Synthetic Carol')];
  let at = 1900000000000, serial = 0, world, connect, registered = new Set();
  const actualDecoder = decoder ?? createCoverDecoder();
  async function proof(person, op, args = {}) {
    const challenge = await connect.handle({ origin: ORIGIN, op: 'challenge', args: { operation: op, digest: digestArgs(args) } });
    if (!challenge.ok) return { denied: challenge };
    return { origin: ORIGIN, op, args, proof: { challengeId: challenge.challengeId, publicJwk: person.publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: person.key, dsaEncoding: 'ieee-p1363' }).toString('base64url') } };
  }
  async function rpc(person, op, args = {}) { const input = await proof(person,op,args); return input.denied ?? connect.handle(input); }
  // Bootstrap real persistent Connect identities before seeding an old World5.
  connect = createConnectService({ databasePath: paths.connectPath, projectId: PROJECT, clock: () => at, allowedOrigins: [ORIGIN] });
  for (const person of actors) Object.assign(person, good(await rpc(person,'bootstrap',{label:person.label,encryptionPublicJwk:person.encryptionPublicJwk})));
  connect.close(); connect = null;
  const legacyPixels = legacy ? seedWorld5(paths.worldPath, actors, { corruptAvatar, now: at }) : null;
  const legacyBefore = legacy ? worldSnapshot(paths.worldPath) : null;
  function open(allowWorld6Migration = false) {
    registered = new Set();
    world = createWorldService({ databasePath: paths.worldPath, projectId: PROJECT, clock: () => at, allowWorld6Migration,
      ...(withContacts ? { withCurrentContacts: (person, callback) => connect.withCurrentContacts(person, callback) } : {}),
      ...(withDecoder ? { profileCoverDecoder: actualDecoder } : {}), profileCoverRegistered: operation => registered.has(operation) });
    const extensions = [world, ...(registerCover && world.profileCoverExtension ? [world.profileCoverExtension] : [])];
    connect = createConnectService({ databasePath: paths.connectPath, projectId: PROJECT, clock: () => at, allowedOrigins: [ORIGIN], extensions,
      canRequestContact: (left,right) => world.canRequestContact(left,right) });
    for (const extension of extensions) for (const operation of extension.operations) registered.add(operation);
  }
  paths.beforeCleanup(() => { connect?.close(); world?.close(); });
  open(legacy);
  const request = () => `cover1.${at}.${(++serial).toString(16).padStart(32,'0')}`;
  const profile = async person => good(await rpc(person,'world.profile.get')).profile;
  const status = async (person,targetId=person.accountId,extra={}) => good(await rpc(person,'world.profile.cover.status',{expectedAccountId:person.accountId,targetId,...extra}));
  async function publish(person, { targetId=person.accountId, bytes=pngFixture(80,60), crop={x:.5,y:.5,zoom:1} } = {}) {
    await profile(person); const context = await status(person,targetId);
    const prepareArgs={expectedAccountId:person.accountId,targetId,requestId:request(),expectedTargetRevision:context.scope.targetRevision,
      expectedPolicyEpoch:context.scope.policyEpoch,expectedCoverRevision:context.scope.coverRevision,mime:'image/png',dataBase64:bytes.toString('base64'),crop};
    const prepared=good(await rpc(person,'world.profile.cover.prepare',prepareArgs));
    const commitArgs={expectedAccountId:person.accountId,targetId,requestId:request(),draftId:prepared.draft.id};
    const committed=good(await rpc(person,'world.profile.cover.commit',commitArgs));return {prepareArgs,prepared,commitArgs,committed};
  }
  async function contact(left,right) {const card=good(await rpc(right,'card.get')),request=good(await rpc(left,'contacts.request',{cardId:card.cardId}));
    return good(await rpc(right,'contacts.accept',{requestId:request.requestId})).relationshipId;}
  return {...paths,actors,rpc,proof,profile,status,publish,request,contact,legacyPixels,legacyBefore,registered:()=>registered,
    get world(){return world;},get connect(){return connect;},get now(){return at;},advance(ms){at+=ms;},
    reopen(){connect.close();world.close();open(false);},readBytes:person=>rpc(person,'world.profile.cover.read',{expectedAccountId:person.accountId,targetId:person.accountId}),
    asDataUrl,
  };
}
