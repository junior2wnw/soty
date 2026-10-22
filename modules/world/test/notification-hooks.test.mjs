import test from 'node:test';
import assert from 'node:assert/strict';
import { signedFixture, good, denied } from './support/world6-signed-fixture.mjs';

test('message hooks run after commit once; failed observers cannot fail an ACK or invalidate membership listeners', async t => {
  const f = await signedFixture(t), [alice, bob] = f.actors, messages = [], memberships = [];
  const room = good(await f.rpc(alice, 'world.community.create', { requestId: 'notification_room', name: 'Disposable notifications' })).community;
  good(await f.rpc(bob, 'world.membership.join', { communityId: room.communityId }));
  f.world.subscribeMembership(event => memberships.push(event));
  f.world.subscribeMessages(event => { messages.push(event); assert.deepEqual(f.world.notificationRecipients(event), [bob.accountId]); });
  f.world.subscribeMessages(() => { throw Error('Synthetic notifier unavailable'); });
  const args = { communityId: room.communityId, clientId: 'notification_send', text: 'Private content never enters event' };
  const sent = good(await f.rpc(alice, 'world.chat.send', args)).message;
  good(await f.rpc(alice, 'world.chat.send', args));
  assert.deepEqual(messages, [{ kind: 'room', conversationId: room.communityId, messageId: sent.messageId }]);
  assert.deepEqual(memberships, []);
  denied(await f.rpc(alice, 'world.chat.send', { ...args, clientId: 'notification_invalid', replyTo: 'missing_message' }), 'message_not_found');
  assert.equal(messages.length, 1);
});

test('room notification authorization rechecks exact message, mute, read, active membership and deleted state', async t => {
  const f = await signedFixture(t), [alice, bob, carol] = f.actors;
  const room = good(await f.rpc(alice, 'world.community.create', { requestId: 'room_acl_notification', name: 'Disposable room' })).community;
  good(await f.rpc(bob, 'world.membership.join', { communityId: room.communityId }));
  const sent = good(await f.rpc(alice, 'world.chat.send', { communityId: room.communityId, clientId: 'room_acl_one', text: 'Message' })).message;
  const event = { kind: 'room', conversationId: room.communityId, messageId: sent.messageId };
  const target = { kind: 'room', communityId: room.communityId };
  assert.deepEqual(f.world.canNotify(bob, event), { kind: 'room', communityId: room.communityId });
  assert.equal(f.world.canNotify(alice, event), null); assert.equal(f.world.canNotify(carol, event), null);
  assert.deepEqual(f.world.notificationTarget(bob, event), target);
  assert.equal(f.world.notificationTarget(alice, event), null); assert.equal(f.world.notificationTarget(carol, event), null);
  good(await f.rpc(bob, 'world.membership.preferences', { communityId: room.communityId, muted: true }));
  assert.equal(f.world.canNotify(bob, event), null); assert.deepEqual(f.world.notificationRecipients(event), []);
  assert.deepEqual(f.world.notificationTarget(bob, event), target);
  good(await f.rpc(bob, 'world.membership.preferences', { communityId: room.communityId, muted: false }));
  good(await f.rpc(bob, 'world.chat.read', { communityId: room.communityId, throughSeq: sent.seq }));
  assert.equal(f.world.canNotify(bob, event), null);
  assert.deepEqual(f.world.notificationTarget(bob, event), target);
  const second = good(await f.rpc(alice, 'world.chat.send', { communityId: room.communityId, clientId: 'room_acl_two', text: 'Second' })).message;
  const next = { ...event, messageId: second.messageId };
  good(await f.rpc(bob, 'world.membership.leave', { communityId: room.communityId }));
  assert.equal(f.world.canNotify(bob, next), null);
  assert.equal(f.world.notificationTarget(bob, next), null);
  good(await f.rpc(bob, 'world.membership.join', { communityId: room.communityId }));
  good(await f.rpc(alice, 'world.chat.remove', { communityId: room.communityId, messageId: second.messageId }));
  assert.equal(f.world.canNotify(bob, next), null); assert.deepEqual(f.world.notificationRecipients(next), []);
  assert.equal(f.world.notificationTarget(bob, next), null);
  assert.equal(f.world.canNotify(bob, { ...next, arbitrary: 'never trust extra fields' }), null);
});

test('direct notification target follows current relationship and never survives block, read or remove/re-add', async t => {
  const f = await signedFixture(t), [alice, bob, carol] = f.actors, relationshipId = await f.contact(alice, bob);
  const thread = good(await f.rpc(alice, 'world.direct.open', { expectedAccountId: alice.accountId, peerAccountId: bob.accountId, expectedRelationshipId: relationshipId, requestId: 'notification_direct' })).current;
  const base = { expectedAccountId: alice.accountId, threadId: thread.threadId, expectedRelationshipId: relationshipId };
  const sent = good(await f.rpc(alice, 'world.direct.chat.send', { ...base, clientId: 'direct_notification_one', text: 'Private DM' })).message;
  const event = { kind: 'direct', conversationId: thread.threadId, messageId: sent.messageId };
  assert.deepEqual(f.world.notificationRecipients(event), [bob.accountId]);
  assert.deepEqual(f.world.notificationRecipients(event, { limit: 1 }), [bob.accountId]);
  assert.deepEqual(f.world.notificationRecipients(event, { afterAccountId: bob.accountId, limit: 1 }), []);
  assert.deepEqual(f.world.canNotify(bob, event), { kind: 'direct', threadId: thread.threadId, peerAccountId: alice.accountId, relationshipId });
  const target = { kind: 'direct', threadId: thread.threadId, peerAccountId: alice.accountId, relationshipId };
  assert.deepEqual(f.world.notificationTarget(bob, event), target);
  assert.equal(f.world.canNotify(carol, event), null);
  assert.equal(f.world.notificationTarget(carol, event), null);
  good(await f.rpc(bob, 'world.direct.preferences', { ...base, expectedAccountId: bob.accountId, muted: true }));
  assert.equal(f.world.canNotify(bob, event), null);
  assert.deepEqual(f.world.notificationTarget(bob, event), target);
  good(await f.rpc(bob, 'world.direct.preferences', { ...base, expectedAccountId: bob.accountId, muted: false }));
  good(await f.rpc(bob, 'world.direct.chat.read', { ...base, expectedAccountId: bob.accountId, throughSeq: sent.seq }));
  assert.equal(f.world.canNotify(bob, event), null);
  assert.deepEqual(f.world.notificationTarget(bob, event), target);
  const second = good(await f.rpc(alice, 'world.direct.chat.send', { ...base, clientId: 'direct_notification_two', text: 'Second private DM' })).message;
  const next = { ...event, messageId: second.messageId };
  good(await f.rpc(bob, 'contacts.remove', { relationshipId }));
  assert.equal(f.world.canNotify(bob, next), null);
  assert.equal(f.world.notificationTarget(bob, next), null);
  f.advance(1); const nextRelationship = await f.contact(alice, bob);
  good(await f.rpc(alice, 'world.direct.open', { expectedAccountId: alice.accountId, peerAccountId: bob.accountId, expectedRelationshipId: nextRelationship, requestId: 'notification_new_relationship' }));
  assert.equal(f.world.canNotify(bob, next), null); assert.deepEqual(f.world.notificationRecipients(next), []);
  assert.equal(f.world.notificationTarget(bob, next), null);
  const third = good(await f.rpc(alice, 'world.direct.chat.send', { ...base, expectedRelationshipId: nextRelationship, clientId: 'direct_notification_three', text: 'Current relationship message' })).message;
  const current = { ...event, messageId: third.messageId };
  assert.equal(f.world.canNotify(bob, current).relationshipId, nextRelationship);
  good(await f.rpc(bob, 'contacts.block', { peerAccountId: alice.accountId }));
  assert.equal(f.world.canNotify(bob, current), null);
  assert.equal(f.world.notificationTarget(bob, current), null);
});

test('notification recipient pages use a bounded account keyset and keep the legacy call unchanged', async t => {
  const f = await signedFixture(t), [alice, bob, carol] = f.actors;
  const room = good(await f.rpc(alice, 'world.community.create', { requestId: 'recipient_pages_room', name: 'Paged room' })).community;
  for (const actor of [bob, carol]) good(await f.rpc(actor, 'world.membership.join', { communityId: room.communityId }));
  const sent = good(await f.rpc(alice, 'world.chat.send', { communityId: room.communityId, clientId: 'recipient_pages_message', text: 'Page recipients' })).message;
  const event = { kind: 'room', conversationId: room.communityId, messageId: sent.messageId };
  const recipients = [bob.accountId, carol.accountId].sort();
  assert.deepEqual(f.world.notificationRecipients(event), recipients);
  assert.deepEqual(f.world.notificationRecipients(event, {}), recipients);
  assert.deepEqual(f.world.notificationRecipients(event, { afterAccountId: '', limit: 1 }), recipients.slice(0, 1));
  assert.deepEqual(f.world.notificationRecipients(event, { afterAccountId: recipients[0], limit: 1 }), recipients.slice(1));
  assert.deepEqual(f.world.notificationRecipients(event, { afterAccountId: recipients[1], limit: 1 }), []);
  for (const page of [{ limit: 0 }, { limit: 257 }, { limit: 1.5 }, { afterAccountId: '<unsafe>' }, { offset: 1 }]) {
    assert.throws(() => f.world.notificationRecipients(event, page));
  }
  good(await f.rpc(bob, 'world.membership.preferences', { communityId: room.communityId, muted: true }));
  assert.deepEqual(f.world.notificationRecipients(event, { limit: 1 }), [carol.accountId]);
});
