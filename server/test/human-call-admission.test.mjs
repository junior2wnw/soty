import test from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { generateKeyPairSync, sign } from 'node:crypto';
import { createConnectService, digestArgs } from '../../modules/connect/server/index.mjs';
import { createWorldService } from '../../modules/world/server/index.mjs';
import { createHumanCallModule } from '../human-call-module.js';

const ORIGIN = 'https://human-call.test';
const identity = () => {
  const a = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const b = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  return { key: a.privateKey, publicJwk: a.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: b.publicKey.export({ format: 'jwk' }) };
};
class Socket extends EventEmitter {
  readyState = 1; messages = []; closeCalls = 0;
  send(raw) { this.messages.push(JSON.parse(raw)); }
  close() { if (this.readyState === 3) return; this.closeCalls++; this.readyState = 3; this.emit('close'); }
  input(value) { this.emit('message', Buffer.from(JSON.stringify(value))); }
}
const good = value => { assert.equal(value.ok, true, value.error?.code); return value; };
async function fixture(t, count = 2) {
  let at = 1_800_000_000_000, connect;
  const world = createWorldService({ databasePath: ':memory:', projectId: 'human-call-test', clock: () => at });
  const calls = createHumanCallModule({ getConnect: () => connect, getWorld: () => world, allowedOrigins: [ORIGIN], clock: () => at });
  connect = createConnectService({ databasePath: ':memory:', projectId: 'human-call-test', clock: () => at,
    allowedOrigins: [ORIGIN], extensions: [world, calls], canRequestContact: (a, b) => world.canRequestContact(a, b) });
  const wss = new EventEmitter(); calls.attachSignaling(wss);
  t.after(() => { calls.close(); connect.close(); world.close(); });
  async function rpc(actor, op, args = {}) {
    const challenge = good(await connect.handle({ origin: ORIGIN, op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
    return connect.handle({ origin: ORIGIN, op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: actor.key, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  }
  const actors = [];
  for (let i = 0; i < count; i++) {
    const actor = identity(); actor.state = good(await rpc(actor, 'bootstrap', { label: 'Synthetic participant', encryptionPublicJwk: actor.encryptionPublicJwk }));
    good(await rpc(actor, 'world.profile.get')); actors.push(actor);
  }
  const group = good(await rpc(actors[0], 'world.community.create', { requestId: 'group_create_one', name: 'Synthetic group', joinPolicy: 'open' })).community;
  for (const actor of actors.slice(1)) good(await rpc(actor, 'world.membership.join', { communityId: group.communityId }));
  const groupJoin = (actor, requestId = 'group_join_one') => rpc(actor, 'call.join', { requestId, mode: 'group', targetId: group.communityId });
  const personalJoin = (actor, peer, requestId = 'personal_join_one') => rpc(actor, 'call.join', { requestId, mode: 'one-to-one', targetId: peer.state.accountId });
  function socketFor(admission, first = {}) {
    const socket = new Socket(); wss.emit('connection', socket, { headers: { origin: ORIGIN } }, admission.callId);
    socket.input({ type: 'call.bind', callId: admission.callId, epoch: admission.epoch, grantRef: admission.grantRef, ...first }); return socket;
  }
  function pendingSocket(admission) {
    const socket = new Socket(); wss.emit('connection', socket, { headers: { origin: ORIGIN } }, admission.callId); return socket;
  }
  async function contact(left, right) {
    const request = good(await rpc(left, 'contacts.requestAccount', { accountId: right.state.accountId }));
    return good(await rpc(right, 'contacts.accept', { requestId: request.requestId }));
  }
  return { actors, connect, world, calls, rpc, group, groupJoin, personalJoin, socketFor, pendingSocket, contact, advance(ms) { at += ms; } };
}

test('signed join returns bounded admission before any peer wait, with microphone off', async t => {
  const f = await fixture(t); const admission = good(await f.groupJoin(f.actors[0]));
  assert.equal(admission.deviceId === f.actors[0].state.deviceId, true);
  assert.equal(f.calls.status().signalingMounted, true); assert.equal(f.calls.status().turnConfigured, false);
  const socket = f.socketFor(admission), ready = socket.messages.find(value => value.type === 'call.ready');
  assert.equal(Boolean(ready), true); assert.equal(ready.self.microphone, false);
  const mic = await f.rpc(f.actors[0], 'call.microphone', { requestId: 'mic_before_peer', callId: admission.callId, epoch: admission.epoch });
  assert.equal(mic.ok, false); assert.equal(mic.error.code, 'human_call_waiting');
});
test('copied actor JSON and contact predicate outside its original actor fence grant nothing', async t => {
  const f = await fixture(t), a = f.actors[0].state;
  assert.throws(() => f.calls.execute({ op: 'call.join', actor: { accountId: a.accountId, deviceId: a.deviceId },
    args: { requestId: 'forged_actor', mode: 'group', targetId: f.group.communityId } }), { code: 'human_call_denied' });
  assert.throws(() => f.connect.hasCurrentContact({ accountId: a.accountId, deviceId: a.deviceId }, f.actors[1].state.accountId),
    { code: 'connect_authority_actor_invalid' });
  assert.equal(f.calls.status().calls, 0);
});
test('current signed group membership admits four, never a fifth reserved or active participant', async t => {
  const f = await fixture(t, 5);
  for (const actor of f.actors.slice(0, 4)) good(await f.groupJoin(actor));
  const fifth = await f.groupJoin(f.actors[4]); assert.equal(fifth.ok, false); assert.equal(fifth.error.code, 'human_call_full');
  assert.equal(f.calls.status().references, 4);
});
test('claimed hello identity is rejected; relay sender comes only from original signed actor', async t => {
  const f = await fixture(t), a = good(await f.groupJoin(f.actors[0])), b = good(await f.groupJoin(f.actors[1]));
  const bad = f.socketFor(a, { deviceId: b.deviceId }); assert.equal(bad.readyState, 3); assert.equal(bad.messages.length, 0);
  const left = f.socketFor(a), right = f.socketFor(b);
  left.input({ type: 'p2p.offer', callId: a.callId, epoch: a.epoch,
    signal: { id: 'offer_one', kind: 'offer', targetDeviceId: b.deviceId, deviceId: b.deviceId, sdp: 'v=0\r\n' } });
  const offer = right.messages.find(value => value.type === 'p2p.offer');
  assert.equal(Boolean(offer), true); assert.equal(offer.signal.deviceId === a.deviceId, true);
  const replay = f.socketFor(a); assert.equal(replay.readyState, 3); assert.equal(replay.messages.length, 0);
});
test('removed current community membership invalidates previously issued and bound grants', async t => {
  const f = await fixture(t), a = good(await f.groupJoin(f.actors[0])), b = good(await f.groupJoin(f.actors[1]));
  const left = f.socketFor(a), right = f.socketFor(b);
  good(await f.rpc(f.actors[0], 'world.membership.remove', { communityId: f.group.communityId, profileId: f.actors[1].state.accountId }));
  assert.equal(right.readyState, 3);
  left.input({ type: 'p2p.candidate', callId: a.callId, epoch: a.epoch,
    signal: { id: 'candidate_old', targetDeviceId: b.deviceId, candidate: 'synthetic' } });
  assert.equal(right.messages.some(value => value.type === 'p2p.candidate'), false);
});
test('World discoverability never substitutes for accepted reciprocal Connect contact', async t => {
  const f = await fixture(t), [a, b] = f.actors;
  assert.equal((await f.personalJoin(a, b)).ok, false);
  const relationship = await f.contact(a, b);
  const first = good(await f.personalJoin(a, b)); good(await f.personalJoin(b, a));
  good(await f.rpc(a, 'contacts.remove', { relationshipId: relationship.relationshipId }));
  const old = f.socketFor(first); assert.equal(old.readyState, 3); assert.equal(old.messages.length, 0);
  assert.equal((await f.personalJoin(b, a, 'new_after_remove')).ok, false);
});
test('actual Connect block after contact acceptance denies stale microphone grant before publish', async t => {
  const f = await fixture(t), [a, b] = f.actors; await f.contact(a, b);
  const leftAdmission = good(await f.personalJoin(a, b)), rightAdmission = good(await f.personalJoin(b, a));
  const left = f.socketFor(leftAdmission), right = f.socketFor(rightAdmission);
  const mic = good(await f.rpc(a, 'call.microphone', { requestId: 'mic_before_block', callId: leftAdmission.callId, epoch: leftAdmission.epoch }));
  good(await f.rpc(b, 'contacts.block', { peerAccountId: a.state.accountId }));
  left.input({ type: 'call.publish', callId: mic.callId, epoch: mic.epoch, grantRef: mic.grantRef });
  assert.equal(left.readyState, 3); assert.equal(left.messages.some(value => value.type === 'call.published'), false);
  assert.equal(right.messages.some(value => value.type === 'call.peers' && value.peers.some(peer => peer.microphone)), false);
});
test('microphone purpose is single-use and tied to the bound canonical peer, not a second socket', async t => {
  const f = await fixture(t), a = good(await f.groupJoin(f.actors[0])), b = good(await f.groupJoin(f.actors[1]));
  const left = f.socketFor(a); f.socketFor(b);
  const mic = good(await f.rpc(f.actors[0], 'call.microphone', { requestId: 'mic_publish_one', callId: a.callId, epoch: a.epoch }));
  const wrongPurpose = f.socketFor(mic); assert.equal(wrongPurpose.readyState, 3);
  left.input({ type: 'call.publish', callId: mic.callId, epoch: mic.epoch, grantRef: mic.grantRef });
  assert.equal(left.messages.filter(value => value.type === 'call.published').length, 1);
  left.input({ type: 'call.publish', callId: mic.callId, epoch: mic.epoch, grantRef: mic.grantRef });
  assert.equal(left.readyState, 3); assert.equal(left.messages.filter(value => value.type === 'call.published').length, 1);
});
test('mute before permission settles retires its issued microphone reference before any late publish', async t => {
  const f = await fixture(t), a = good(await f.groupJoin(f.actors[0])), b = good(await f.groupJoin(f.actors[1]));
  const left = f.socketFor(a); f.socketFor(b);
  const mic = good(await f.rpc(f.actors[0], 'call.microphone', { requestId: 'mic_late_muted', callId: a.callId, epoch: a.epoch }));
  left.input({ type: 'call.unpublish', callId: a.callId, epoch: a.epoch });
  left.input({ type: 'call.publish', callId: mic.callId, epoch: mic.epoch, grantRef: mic.grantRef });
  assert.equal(left.messages.filter(value => value.type === 'call.published').length, 0);
});
test('mute retires a pending grant so a new explicit microphone request is independently admissible', async t => {
  const f = await fixture(t), a = good(await f.groupJoin(f.actors[0])), b = good(await f.groupJoin(f.actors[1]));
  const left = f.socketFor(a); f.socketFor(b);
  const first = good(await f.rpc(f.actors[0], 'call.microphone', { requestId: 'mic_old_muted', callId: a.callId, epoch: a.epoch }));
  left.input({ type: 'call.unpublish', callId: a.callId, epoch: a.epoch });
  const next = good(await f.rpc(f.actors[0], 'call.microphone', { requestId: 'mic_new_explicit', callId: a.callId, epoch: a.epoch }));
  assert.equal(first.grantRef === next.grantRef, false);
});
test('ninth pending socket in one valid call is denied and an observed close frees exactly one slot', async t => {
  const f = await fixture(t, 1), admission = good(await f.groupJoin(f.actors[0]));
  const pending = Array.from({ length: 8 }, () => f.pendingSocket(admission));
  assert.equal(pending.every(socket => socket.readyState === 1), true);
  const denied = f.pendingSocket(admission); assert.equal(denied.readyState, 3);
  assert.equal(f.calls.status().ownedSockets, 8); assert.equal(f.calls.status().pendingSockets, 8);
  pending[0].close(); const replacement = f.pendingSocket(admission);
  assert.equal(replacement.readyState, 1); assert.equal(f.calls.status().ownedSockets, 8);
});
test('129th owned pending socket across sixteen valid calls is denied without allocating another slot', async t => {
  const f = await fixture(t, 16), admissions = [good(await f.groupJoin(f.actors[0]))];
  for (let n = 1; n < 16; n++) {
    const group = good(await f.rpc(f.actors[n], 'world.community.create', { requestId: 'socket_group_' + n, name: 'Synthetic bounded group', joinPolicy: 'open' })).community;
    admissions.push(good(await f.rpc(f.actors[n], 'call.join', { requestId: 'socket_join_' + n, mode: 'group', targetId: group.communityId })));
  }
  const pending = admissions.flatMap(admission => Array.from({ length: 8 }, () => f.pendingSocket(admission)));
  assert.equal(pending.length, 128); assert.equal(pending.every(socket => socket.readyState === 1), true);
  const denied = f.pendingSocket(admissions[0]); assert.equal(denied.readyState, 3);
  assert.equal(f.calls.status().ownedSockets, 128);
  pending[0].close(); assert.equal(f.pendingSocket(admissions[0]).readyState, 1);
  assert.equal(f.calls.status().ownedSockets, 128);
});
