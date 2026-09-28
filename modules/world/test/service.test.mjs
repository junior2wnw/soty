import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, dirname } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { fileURLToPath } from 'node:url';
import { createWorldService } from '../server/index.mjs';

const alice = Object.freeze({ accountId: 'acct_alice', deviceId: 'dev_alice', label: 'Алексей' });
const bob = Object.freeze({ accountId: 'acct_bob', deviceId: 'dev_bob', label: 'Маша' });
const eve = Object.freeze({ accountId: 'acct_eve', deviceId: 'dev_eve', label: 'Евгений' });
function fixture(t) {
  const base = resolve(tmpdir()); const dir = mkdtempSync(join(base, 'soty-world-test-'));
  const databasePath = join(dir, 'world.sqlite'); let now = 1_800_000_000_000;
  const services = [];
  function open() { const result = createWorldService({ databasePath, projectId: 'world-test', clock: () => now }); services.push(result); return result; }
  const service = open();
  t.after(() => {
    services.forEach(item => item.close());
    assert.equal(dirname(resolve(dir)), base); assert.ok(resolve(dir).startsWith(join(base, 'soty-world-test-')));
    rmSync(dir, { recursive: true, force: true });
  });
  const call = (actor, op, args = {}) => service.execute({ actor, op: 'world.' + op, args });
  [alice, bob, eve].forEach(actor => call(actor, 'profile.get'));
  function update(actor, values) { return call(actor, 'profile.update', { expectedRevision: call(actor, 'profile.get').profile.revision, ...values }); }
  let n = 0;
  const group = (actor = alice, args = {}) => call(actor, 'community.create', { requestId: 'request_' + ++n, name: 'Фотоклуб', ...args }).community;
  const send = (actor, communityId, body = 'Привет') => call(actor, 'chat.send', { communityId, clientId: 'message_' + ++n, text: body }).message;
  return { service, call, update, group, send, databasePath, open, advance: ms => { now += ms; } };
}
function denied(fn, code) { assert.throws(fn, error => error.code === code, code); }

test('a profile starts hidden; own identity cannot be selected from request args', t => {
  const f = fixture(t);
  assert.equal(f.call(alice, 'profile.get').profile.discoverable, false);
  assert.equal(f.call(bob, 'discovery.search').totals.people, 0);
  denied(() => f.call(alice, 'profile.get', { accountId: bob.accountId }), 'invalid_arguments');
  denied(() => f.call(alice, 'profile.update', { expectedRevision: 1, accountId: bob.accountId, discoverable: true }), 'invalid_arguments');
  denied(() => f.call(bob, 'profile.view', { profileId: alice.accountId }), 'profile_not_found');
  f.update(alice, { discoverable: true, bio: 'Снимаю природу', interests: ['Фото', 'Лес'] });
  assert.equal(f.call(bob, 'discovery.search', { query: 'фото' }).people[0].profileId, alice.accountId);
  denied(() => f.call(alice, 'profile.update', { expectedRevision: 1, bio: 'stale' }), 'revision_conflict');
  assert.equal(f.call(alice, 'profile.get').profile.bio, 'Снимаю природу');
});

test('open community explicitly grants membership; per-account revocation gates every chat read and write', t => {
  const f = fixture(t); const group = f.group();
  assert.equal(f.call(bob, 'community.get', { communityId: group.communityId }).community.permissions.canWrite, false);
  denied(() => f.call(bob, 'chat.list', { communityId: group.communityId }), 'community_membership_required');
  f.call(bob, 'membership.join', { communityId: group.communityId });
  const sent = f.send(alice, group.communityId);
  assert.equal(f.call(bob, 'chat.list', { communityId: group.communityId }).messages[0].text, 'Привет');
  assert.equal(f.call(bob, 'community.get', { communityId: group.communityId }).community.unreadCount, 1);
  f.call(bob, 'chat.read', { communityId: group.communityId, throughSeq: sent.seq });
  assert.equal(f.call(bob, 'community.get', { communityId: group.communityId }).community.unreadCount, 0);
  f.call(alice, 'membership.remove', { communityId: group.communityId, profileId: bob.accountId });
  assert.equal(f.service.canAccessCommunity(bob.accountId, group.communityId), false);
  denied(() => f.call(bob, 'chat.list', { communityId: group.communityId }), 'community_membership_required');
  denied(() => f.send(bob, group.communityId), 'community_membership_required');
  assert.equal(f.call(alice, 'chat.list', { communityId: group.communityId }).messages.length, 1);
});

test('request groups require approval; moderators cannot promote themselves, remove owners or alter joining policy', t => {
  const f = fixture(t); const group = f.group(alice, { joinPolicy: 'request' }); const communityId = group.communityId;
  assert.equal(f.call(bob, 'membership.join', { communityId }).community.membership.state, 'requested');
  assert.equal(f.call(alice, 'community.get', { communityId }).community.pendingCount, 1);
  assert.equal(Object.hasOwn(f.call(eve, 'community.get', { communityId }).community, 'pendingCount'), false);
  denied(() => f.call(bob, 'membership.decide', { communityId, profileId: bob.accountId, accept: true }), 'community_membership_required');
  f.call(alice, 'membership.decide', { communityId, profileId: bob.accountId, accept: true });
  f.call(alice, 'membership.role', { communityId, profileId: bob.accountId, role: 'moderator' });
  assert.equal(f.service.isGroupAdmin(bob.accountId, communityId), true);
  denied(() => f.call(bob, 'membership.remove', { communityId, profileId: alice.accountId }), 'community_permission_denied');
  denied(() => f.call(bob, 'membership.role', { communityId, profileId: eve.accountId, role: 'moderator' }), 'community_permission_denied');
  denied(() => f.call(bob, 'community.update', { communityId, expectedRevision: 1, joinPolicy: 'open' }), 'community_permission_denied');
  f.call(eve, 'membership.join', { communityId });
  assert.equal(f.call(bob, 'membership.list', { communityId, state: 'requested' }).members.length, 1);
  f.call(bob, 'membership.decide', { communityId, profileId: eve.accountId, accept: true });
  denied(() => f.call(eve, 'membership.list', { communityId, state: 'requested' }), 'community_permission_denied');
  f.call(bob, 'membership.ban', { communityId, profileId: eve.accountId });
  denied(() => f.call(eve, 'membership.join', { communityId }), 'community_banned');
  denied(() => f.call(bob, 'membership.invite', { communityId, profileId: eve.accountId }), 'community_banned');
  f.call(bob, 'membership.unban', { communityId, profileId: eve.accountId });
  assert.equal(f.call(eve, 'membership.join', { communityId }).community.membership.state, 'requested');
});

test('invite communities are absent even for a guessed ID; leaving removes chat and private showcase access', t => {
  const f = fixture(t); const group = f.group(alice, { joinPolicy: 'invite', showcase: 'Только для семьи' }); const communityId = group.communityId;
  assert.equal(f.call(bob, 'discovery.search', { query: communityId }).communities.length, 0);
  denied(() => f.call(bob, 'community.get', { communityId }), 'community_not_found');
  denied(() => f.call(bob, 'membership.join', { communityId }), 'community_not_found');
  f.call(alice, 'membership.invite', { communityId, profileId: bob.accountId });
  assert.equal(f.call(bob, 'community.get', { communityId }).community.showcase, 'Только для семьи');
  denied(() => f.call(bob, 'chat.list', { communityId }), 'community_membership_required');
  f.call(bob, 'membership.join', { communityId }); f.send(bob, communityId);
  assert.equal(f.call(bob, 'membership.leave', { communityId }).community, null);
  denied(() => f.call(bob, 'community.get', { communityId }), 'community_not_found');
  denied(() => f.call(bob, 'chat.list', { communityId }), 'community_membership_required');
});

test('visibility changes immediately affect ID search, counts, public previews and profile projection, not existing chat', t => {
  const f = fixture(t); f.update(alice, { discoverable: true }); f.update(bob, { discoverable: true });
  const group = f.group(); const communityId = group.communityId;
  f.call(bob, 'membership.join', { communityId }); f.send(bob, communityId, 'Мой снимок');
  const visible = f.call(eve, 'discovery.search'); assert.equal(visible.totals.people, 2);
  assert.equal(visible.communities[0].previewMembers.length, 2);
  f.update(bob, { discoverable: false });
  const hidden = f.call(eve, 'discovery.search'); assert.equal(hidden.totals.people, 1);
  assert.equal(hidden.communities[0].memberCount, 2); // public group size never grants name visibility
  assert.equal(hidden.communities[0].previewMembers.length, 1);
  assert.equal(f.call(eve, 'discovery.search', { query: bob.accountId }).people.length, 0);
  assert.equal(f.call(eve, 'discovery.search', { query: 'Маша' }).people.length, 0);
  denied(() => f.call(eve, 'profile.view', { profileId: bob.accountId }), 'profile_not_found');
  const members = f.call(alice, 'membership.list', { communityId }).members;
  assert.equal(members.length, 2); assert.equal(Object.hasOwn(members[1], 'muted'), false);
  assert.equal(f.call(alice, 'chat.list', { communityId }).messages[0].author.displayName, 'Маша');
  f.update(alice, { showMemberships: false });
  assert.equal(f.call(eve, 'profile.view', { profileId: alice.accountId }).communities.length, 0);
  assert.equal(f.call(eve, 'community.get', { communityId }).community.previewMembers.length, 0);
});

test('membership publication can be hidden separately; preferences are not shared with other members', t => {
  const f = fixture(t); f.update(alice, { discoverable: true }); f.update(bob, { discoverable: true });
  const group = f.group(); const communityId = group.communityId;
  f.call(bob, 'membership.join', { communityId }); f.call(bob, 'membership.preferences', { communityId, showInProfile: false, muted: true, pinned: true });
  assert.equal(f.call(eve, 'profile.view', { profileId: bob.accountId }).communities.length, 0);
  assert.equal(f.call(eve, 'community.get', { communityId }).community.previewMembers.length, 1);
  const member = f.call(alice, 'membership.list', { communityId }).members.find(item => item.profile.profileId === bob.accountId);
  assert.equal(Object.hasOwn(member, 'muted'), false); assert.equal(Object.hasOwn(member, 'pinned'), false);
  assert.equal(f.call(bob, 'community.get', { communityId }).community.membership.muted, true);
});

test('create/send retries cannot duplicate or change the original command and do not leak stale projections', t => {
  const f = fixture(t);
  const args = { requestId: 'create_fixed', name: 'Фотоклуб' };
  const original = f.call(alice, 'community.create', args).community;
  f.update(alice, { discoverable: true });
  assert.equal(f.call(alice, 'community.create', { name: 'Фотоклуб', requestId: 'create_fixed' }).community.previewMembers.length, 1);
  assert.equal(f.call(alice, 'community.list').communities.length, 1);
  denied(() => f.call(alice, 'community.create', { ...args, name: 'Другой клуб' }), 'request_conflict');
  const chatArgs = { communityId: original.communityId, clientId: 'send_fixed', text: 'Один раз' };
  const first = f.call(alice, 'chat.send', chatArgs).message;
  assert.equal(f.call(alice, 'chat.send', chatArgs).message.messageId, first.messageId);
  denied(() => f.call(alice, 'chat.send', { ...chatArgs, text: 'Изменённый запрос' }), 'request_conflict');
  assert.equal(f.call(alice, 'chat.list', { communityId: original.communityId }).messages.length, 1);
});

test('owner transfer is atomic and replay-safe; a group is never left with no owner', t => {
  const f = fixture(t); const group = f.group(); const communityId = group.communityId;
  denied(() => f.call(alice, 'membership.leave', { communityId }), 'owner_must_transfer_or_archive');
  f.call(bob, 'membership.join', { communityId });
  const args = { communityId, profileId: bob.accountId, requestId: 'transfer_001' };
  f.call(alice, 'membership.transfer', args); f.call(alice, 'membership.transfer', args);
  assert.equal(f.call(bob, 'community.get', { communityId }).community.membership.role, 'owner');
  assert.equal(f.call(alice, 'community.get', { communityId }).community.membership.role, 'moderator');
  denied(() => f.call(alice, 'membership.transfer', { ...args, requestId: 'transfer_002' }), 'community_permission_denied');
  f.call(alice, 'membership.leave', { communityId });
  const latest = f.call(bob, 'community.get', { communityId }).community;
  f.call(bob, 'community.archive', { communityId, expectedRevision: latest.revision });
  assert.equal(f.service.canAccessCommunity(bob.accountId, communityId), false);
  denied(() => f.call(bob, 'chat.list', { communityId }), 'community_not_found');
});

test('group events are delivered after commit, never on rollback; failing observers do not undo revocation', t => {
  const f = fixture(t); const group = f.group(); const communityId = group.communityId; const events = [];
  f.service.subscribeMembership(event => { events.push(event); assert.equal(f.service.canAccessCommunity(event.profileId, event.communityId), event.state === 'active'); });
  f.service.subscribeMembership(() => { throw new Error('consumer unavailable'); });
  f.call(bob, 'membership.join', { communityId });
  denied(() => f.call(eve, 'membership.ban', { communityId, profileId: bob.accountId }), 'community_membership_required');
  assert.equal(events.length, 1);
  f.call(alice, 'membership.ban', { communityId, profileId: bob.accountId });
  assert.equal(events.length, 2); assert.equal(events[1].state, 'banned');
  assert.equal(f.service.canAccessCommunity(bob.accountId, communityId), false);
});

test('contact requests respect discovery and explicit audience, including hidden shared-group context', t => {
  const f = fixture(t);
  assert.equal(f.service.canRequestContact(alice.accountId, bob.accountId), false);
  f.update(bob, { discoverable: true }); assert.equal(f.service.canRequestContact(alice.accountId, bob.accountId), true);
  f.update(bob, { contactPolicy: 'members' }); assert.equal(f.service.canRequestContact(alice.accountId, bob.accountId), false);
  const group = f.group(); f.call(bob, 'membership.join', { communityId: group.communityId });
  f.update(bob, { discoverable: false }); assert.equal(f.service.canRequestContact(alice.accountId, bob.accountId), true);
  assert.equal(f.service.canRequestContact(eve.accountId, bob.accountId), false);
  f.update(bob, { contactPolicy: 'nobody' }); assert.equal(f.service.canRequestContact(alice.accountId, bob.accountId), false);
});

test('chat pagination, reply boundaries, unread state and moderation preserve truthful ordering', t => {
  const f = fixture(t); const a = f.group(); const b = f.group(alice, { name: 'Другой' }); const communityId = a.communityId;
  f.call(bob, 'membership.join', { communityId });
  const messages = Array.from({ length: 4 }, (_, i) => f.send(alice, communityId, String(i)));
  const tail = f.call(bob, 'chat.list', { communityId, limit: 2 });
  assert.deepEqual(tail.messages.map(item => item.text), ['2', '3']); assert.equal(tail.hasMore, true);
  assert.deepEqual(f.call(bob, 'chat.list', { communityId, before: tail.messages[0].seq, limit: 2 }).messages.map(item => item.text), ['0', '1']);
  assert.deepEqual(f.call(bob, 'chat.list', { communityId, after: messages[0].seq, limit: 2 }).messages.map(item => item.text), ['1', '2']);
  const other = f.send(alice, b.communityId);
  denied(() => f.call(bob, 'chat.send', { communityId, clientId: 'foreign_reply', text: 'no', replyTo: other.messageId }), 'message_not_found');
  denied(() => f.call(bob, 'chat.remove', { communityId, messageId: messages[0].messageId }), 'community_permission_denied');
  f.call(alice, 'chat.remove', { communityId, messageId: messages[0].messageId });
  assert.equal(f.call(bob, 'chat.list', { communityId }).messages[0].text, '');
  assert.equal(f.call(bob, 'community.get', { communityId }).community.unreadCount, 3);
  denied(() => f.call(bob, 'chat.read', { communityId, throughSeq: other.seq }), 'message_not_found');
});

test('discovery is bounded, stable and case-aware in Russian; a cursor is scoped to its query and filter', t => {
  const f = fixture(t); f.update(alice, { discoverable: true }); f.update(bob, { discoverable: true });
  f.group(); f.group(alice, { name: 'Путешествия' });
  const first = f.call(eve, 'discovery.search', { limit: 2 }); const second = f.call(eve, 'discovery.search', { limit: 2, cursor: first.nextCursor });
  assert.equal(first.people.length + first.communities.length, 2);
  assert.equal(second.people.length + second.communities.length, 2); assert.equal(second.nextCursor, null);
  denied(() => f.call(eve, 'discovery.search', { kind: 'people', cursor: first.nextCursor }), 'invalid_cursor');
  assert.equal(f.call(eve, 'discovery.search', { query: 'ФОТОКЛУБ' }).communities[0].name, 'Фотоклуб');
  const exactId = first.communities[0]?.communityId || second.communities[0].communityId;
  assert.equal(f.call(eve, 'discovery.search', { query: exactId }).communities[0].communityId, exactId);
  denied(() => f.call(eve, 'discovery.search', { limit: 9999 }), 'invalid_number');
});

test('data persists across restart; mismatched project/schema never destroys the existing database', t => {
  const f = fixture(t); f.update(alice, { discoverable: true }); const group = f.group();
  f.call(bob, 'membership.join', { communityId: group.communityId }); f.send(bob, group.communityId, 'Сохранено');
  f.service.close(); const reopened = f.open();
  assert.equal(reopened.execute({ actor: alice, op: 'world.chat.list', args: { communityId: group.communityId } }).messages[0].text, 'Сохранено');
  assert.throws(() => createWorldService({ databasePath: f.databasePath, projectId: 'wrong-project' }), /world_project_mismatch/u);
  reopened.close(); const bytes = readFileSync(f.databasePath); assert.ok(bytes.length > 0);
  const db = new DatabaseSync(f.databasePath); db.exec('PRAGMA user_version=999'); db.close();
  assert.throws(() => f.open(), /world_schema_unsupported/u);
  const verify = new DatabaseSync(f.databasePath); assert.equal(verify.prepare('SELECT COUNT(*) AS n FROM messages').get().n, 1); verify.close();
});

test('validation rolls back mutations and module directories cannot own persistent databases', t => {
  const f = fixture(t);
  denied(() => f.group(alice, { name: '', color: 'javascript:alert(1)' }), 'invalid_text');
  assert.equal(f.call(alice, 'community.list').communities.length, 0);
  denied(() => f.update(alice, { avatarColor: 'url(https://tracker.invalid)' }), 'invalid_color');
  assert.equal(f.call(alice, 'profile.get').profile.revision, 1);
  assert.throws(() => createWorldService({ databasePath: fileURLToPath(new URL('../data/forbidden.sqlite', import.meta.url)), projectId: 'scope' }), { code: 'database_must_be_outside_module' });
});
