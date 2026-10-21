import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { generateKeyPairSync, sign } from 'node:crypto';
import { WebSocket, WebSocketServer } from 'ws';
import { createConnectService, digestArgs } from '../../modules/connect/server/index.mjs';
import { createWorldService } from '../../modules/world/server/index.mjs';
import { createHumanCallModule } from '../human-call-module.js';

const ORIGIN = 'https://human-call-media.test';
const good = value => { assert.equal(value.ok, true, value.error?.code); return value; };
async function until(check) { for (let n = 0; n < 1000; n++) { if (check()) return; await new Promise(resolve => setTimeout(resolve, 2)); } throw Error('owned_media_socket_wait'); }
const configuredPolicy = () => ({ schema: 'soty.human-call-media-policy.v1', videoEnabled: true, icePolicy: {
  schema: 'soty.human-call-ice-policy.v1', servers: [{ urls: ['turn:relay.invalid:3478?transport=udp'] }], ttlMs: 120000,
  issueCredentials({ revision, expiresAt }) { return { username: 'synthetic_' + revision, credential: 'synthetic_credential', expiresAt }; } } });
async function fixture(t, { count = 3, policy = configuredPolicy() } = {}) {
  let at = Date.now(), connect;
  const world = createWorldService({ databasePath: ':memory:', projectId: 'media-contract-test', clock: () => at });
  const calls = createHumanCallModule({ getConnect: () => connect, getWorld: () => world, allowedOrigins: [ORIGIN], clock: () => at, mediaPolicy: policy });
  connect = createConnectService({ databasePath: ':memory:', projectId: 'media-contract-test', clock: () => at, allowedOrigins: [ORIGIN],
    extensions: [world, calls], canRequestContact: (a, b) => world.canRequestContact(a, b) });
  const server = createServer(), wss = new WebSocketServer({ noServer: true, maxPayload: 64000 }), clients = [];
  calls.attachSignaling(wss);
  server.on('upgrade', (request, socket, head) => {
    const match = /^\/ws\/human-call\/([A-Za-z0-9-]+)$/u.exec(new URL(request.url, 'http://127.0.0.1').pathname);
    if (!match) { socket.destroy(); return; }
    wss.handleUpgrade(request, socket, head, socket => wss.emit('connection', socket, request, match[1]));
  });
  t.after(async () => {
    calls.close(); for (const socket of clients) socket.terminate();
    await new Promise(resolve => wss.close(resolve)); await new Promise(resolve => server.close(resolve)); connect.close(); world.close();
  });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  async function rpc(actor, op, args = {}) {
    const challenge = good(await connect.handle({ origin: ORIGIN, op: 'challenge', args: { operation: op, digest: digestArgs(args) } }));
    return connect.handle({ origin: ORIGIN, op, args, proof: { challengeId: challenge.challengeId, publicJwk: actor.publicJwk,
      signature: sign('sha256', Buffer.from(challenge.message), { key: actor.key, dsaEncoding: 'ieee-p1363' }).toString('base64url') } });
  }
  const actors = [];
  for (let n = 0; n < count; n++) {
    const keys = generateKeyPairSync('ec', { namedCurve: 'prime256v1' }), encryption = generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
    const actor = { key: keys.privateKey, publicJwk: keys.publicKey.export({ format: 'jwk' }), encryptionPublicJwk: encryption.publicKey.export({ format: 'jwk' }) };
    actor.state = good(await rpc(actor, 'bootstrap', { label: 'Synthetic media actor', encryptionPublicJwk: actor.encryptionPublicJwk }));
    good(await rpc(actor, 'world.profile.get')); actors.push(actor);
  }
  const group = good(await rpc(actors[0], 'world.community.create', { requestId: 'media_group', name: 'Synthetic media group', joinPolicy: 'open' })).community;
  for (const actor of actors.slice(1)) good(await rpc(actor, 'world.membership.join', { communityId: group.communityId }));
  const join = (actor, requestId, mediaProtocol = 2) => rpc(actor, 'call.join', { requestId, mode: 'group', targetId: group.communityId,
    ...(mediaProtocol === 2 ? { mediaProtocol } : {}) });
  async function socketFor(admission) {
    const socket = new WebSocket('ws://127.0.0.1:' + server.address().port + '/ws/human-call/' + admission.callId, { origin: ORIGIN });
    socket.messages = []; clients.push(socket); socket.on('message', raw => socket.messages.push(JSON.parse(raw.toString())));
    await new Promise((resolve, reject) => { socket.once('open', resolve); socket.once('error', reject); });
    socket.send(JSON.stringify({ type: 'call.bind', callId: admission.callId, epoch: admission.epoch, grantRef: admission.grantRef }));
    await until(() => socket.messages.some(value => value.type === 'call.ready')); return socket;
  }
  const camera = (actor, admission, expectedCameraGeneration = 0, requestId = 'camera_one') => rpc(actor, 'call.camera', { requestId,
    callId: admission.callId, epoch: admission.epoch, expectedJoinRef: admission.grantRef, expectedCameraGeneration });
  const ice = (actor, admission, expectedRevision = 0) => rpc(actor, 'call.ice.get', { callId: admission.callId, epoch: admission.epoch,
    expectedJoinRef: admission.grantRef, requestId: 'ice_' + expectedRevision, expectedRevision });
  const send = (socket, admission, type, values = {}) => socket.send(JSON.stringify({ type, callId: admission.callId, epoch: admission.epoch, ...values }));
  async function publish(socket, admission, grant) {
    send(socket, admission, 'call.video.publish', { grantRef: grant.grantRef, cameraGeneration: grant.cameraGeneration });
    await until(() => socket.messages.some(value => value.type === 'call.video.published' && value.grantRef === grant.grantRef));
  }
  function signal(from, to, fromCameraGeneration, toCameraGeneration = 0, extra = {}) {
    return { id: 'video_frame', targetDeviceId: to.deviceId, fromMediaInstance: from.mediaInstance, toMediaInstance: to.mediaInstance,
      fromCameraGeneration, toCameraGeneration, kind: 'offer', sdp: 'v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n', ...extra };
  }
  return { world, connect, calls, actors, rpc, join, socketFor, camera, ice, send, publish, signal, group, advance(ms) { at += ms; } };
}

test('default media is honestly unconfigured and legacy admission/roster remains audio-only', async t => {
  const f = await fixture(t, { policy: null }), [a, b] = f.actors;
  const aa = good(await f.join(a, 'a_legacy', 1)), bb = good(await f.join(b, 'b_media'));
  assert.equal(aa.mediaProtocol, undefined); assert.equal(bb.mediaProtocol, 2); assert.equal(bb.cameraConfigured, false); assert.equal(bb.iceConfigured, false);
  const sa = await f.socketFor(aa); await f.socketFor(bb);
  assert.deepEqual(Object.keys(sa.messages.find(value => value.type === 'call.ready').self).sort(), ['accountId', 'deviceId', 'label', 'microphone']);
  assert.equal((await f.camera(b, bb)).ok, false); assert.equal((await f.ice(b, bb)).ok, false);
  assert.equal(f.calls.status().turnConfigured, false); assert.equal(f.calls.status().videoConfigured, false); assert.equal(f.calls.status().remoteMediaQualified, false);
});
test('signed ICE is bound to current join, canonical revision and retries; no relay traffic is attempted', async t => {
  const f = await fixture(t), a = f.actors[0], aa = good(await f.join(a, 'a_ice'));
  const first = good(await f.ice(a, aa)), retry = good(await f.ice(a, aa));
  assert.deepEqual(retry, first); assert.equal(first.revision, 1); assert.equal(first.joinRef, aa.grantRef);
  assert.equal(first.iceServers[0].urls[0], 'turn:relay.invalid:3478?transport=udp');
  assert.equal((await f.rpc(a, 'call.ice.get', { callId: aa.callId, epoch: aa.epoch, expectedJoinRef: 'wrong_join', requestId: 'ice_0', expectedRevision: 0 })).ok, false);
  assert.equal((await f.rpc(a, 'call.ice.get', { callId: aa.callId, epoch: aa.epoch, expectedJoinRef: aa.grantRef, requestId: 'random_retry', expectedRevision: 0 })).ok, false);
  assert.equal(f.calls.status().turnConfigured, true); assert.equal(f.calls.status().remoteMediaQualified, false);
});
test('closed camera RPC rejects account/device overrides, outsiders and old joins without granting publication', async t => {
  const f = await fixture(t), [a, b, c] = f.actors, aa = good(await f.join(a, 'a_guard')), bb = good(await f.join(b, 'b_guard'));
  await f.socketFor(aa); await f.socketFor(bb);
  const base = { requestId: 'camera_guard', callId: aa.callId, epoch: aa.epoch, expectedJoinRef: aa.grantRef, expectedCameraGeneration: 0 };
  for (const args of [{ ...base, accountId: b.state.accountId }, { ...base, deviceId: b.state.deviceId }, { ...base, expectedJoinRef: bb.grantRef }, { ...base, epoch: aa.epoch + 1 }])
    assert.equal((await f.rpc(a, 'call.camera', args)).ok, false);
  assert.equal((await f.rpc(c, 'call.camera', base)).ok, false);
  const grant = good(await f.camera(a, aa)); assert.equal(grant.accountId, a.state.accountId); assert.equal(grant.deviceId, a.state.deviceId);
});
test('actual owned WebSockets ACK camera, whitelist current video and tombstone old withdrawals', async t => {
  const f = await fixture(t), [a, b] = f.actors, aa = good(await f.join(a, 'a_video')), bb = good(await f.join(b, 'b_video'));
  const sa = await f.socketFor(aa), sb = await f.socketFor(bb), first = good(await f.camera(a, aa));
  assert.equal(sb.messages.some(value => value.type === 'call.peers' && value.peers.some(peer => peer.camera)), false);
  await f.publish(sa, aa, first);
  f.send(sa, aa, 'video.offer', { signal: f.signal(aa, bb, 1) }); await until(() => sb.messages.some(value => value.type === 'video.offer'));
  assert.equal(sb.messages.find(value => value.type === 'video.offer').signal.deviceId, aa.deviceId);
  f.send(sa, aa, 'call.video.withdraw', { cameraGeneration: 1 });
  await until(() => sa.messages.some(value => value.type === 'call.video.withdrawn'));
  const second = good(await f.camera(a, aa, 1, 'camera_two')); await f.publish(sa, aa, second);
  const before = sa.messages.filter(value => value.type === 'call.video.published').length;
  f.send(sa, aa, 'call.video.withdraw', { cameraGeneration: 1 }); f.send(sa, aa, 'call.video.publish', { grantRef: first.grantRef, cameraGeneration: 1 });
  f.send(sa, aa, 'call.ping'); await until(() => sa.messages.some(value => value.type === 'call.pong'));
  assert.equal(sa.messages.filter(value => value.type === 'call.video.published').length, before);
  const current = sa.messages.filter(value => value.type === 'call.peers').at(-1).peers.find(value => value.deviceId === aa.deviceId);
  assert.equal(current.camera, true); assert.equal(current.cameraGeneration, 2); assert.equal(sa.readyState, WebSocket.OPEN);
});
test('same device rejoin changes mediaInstance; stale same-call frames cannot reach the new socket', async t => {
  const f = await fixture(t), [a, b] = f.actors, aa = good(await f.join(a, 'a_stays')), old = good(await f.join(b, 'b_old'));
  const sa = await f.socketFor(aa), sb = await f.socketFor(old), grant = good(await f.camera(a, aa)); await f.publish(sa, aa, grant);
  good(await f.rpc(b, 'call.leave', { callId: old.callId, epoch: old.epoch, expectedJoinRef: old.grantRef })); await until(() => sb.readyState === WebSocket.CLOSED);
  f.send(sa, aa, 'video.offer', { signal: f.signal(aa, old, 1) }); f.send(sa, aa, 'call.ping');
  await until(() => sa.messages.some(value => value.type === 'call.pong')); assert.equal(sa.readyState, WebSocket.OPEN);
  const fresh = good(await f.join(b, 'b_fresh')), next = await f.socketFor(fresh);
  assert.equal(fresh.callId, old.callId); assert.equal(fresh.epoch, old.epoch); assert.notEqual(fresh.mediaInstance, old.mediaInstance);
  f.send(sa, aa, 'video.offer', { signal: f.signal(aa, old, 1, 0, { id: 'stale_old_instance' }) });
  f.send(sa, aa, 'video.offer', { signal: f.signal(aa, fresh, 1, 0, { id: 'current_new_instance' }) });
  await until(() => next.messages.some(value => value.type === 'video.offer' && value.signal.id === 'current_new_instance'));
  // Both frames share this recipient's ordered real WebSocket. A missing
  // instance fence cannot hide behind cross-socket event scheduling.
  assert.deepEqual(next.messages.filter(value => value.type === 'video.offer').map(value => value.signal.id), ['current_new_instance']);
});
test('mixed peers keep audio, reject video to legacy and retain the four-member cap', async t => {
  const f = await fixture(t, { count: 5 }), [a, b, c, d, e] = f.actors;
  const aa = good(await f.join(a, 'a_mixed')), bb = good(await f.join(b, 'b_legacy', 1));
  const sa = await f.socketFor(aa), sb = await f.socketFor(bb);
  assert.equal((await f.camera(a, aa)).ok, false);
  const cc = good(await f.join(c, 'c_mixed')); await f.socketFor(cc); const dd = good(await f.join(d, 'd_mixed')); await f.socketFor(dd);
  assert.equal((await f.join(e, 'e_overflow')).ok, false); assert.equal(f.calls.status().participantLimit, 4);
  const grant = good(await f.camera(a, aa)); await f.publish(sa, aa, grant);
  f.send(sa, aa, 'video.offer', { signal: f.signal(aa, { ...bb, mediaInstance: 'unnegotiated' }, 1) });
  f.send(sa, aa, 'p2p.offer', { signal: { id: 'audio_offer', targetDeviceId: bb.deviceId, kind: 'offer', sdp: 'v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n' } });
  await until(() => sb.messages.some(value => value.type === 'p2p.offer')); assert.equal(sb.messages.some(value => value.type === 'video.offer'), false);
  const mic = good(await f.rpc(a, 'call.microphone', { requestId: 'mic_mixed', callId: aa.callId, epoch: aa.epoch }));
  f.send(sa, aa, 'call.publish', { grantRef: mic.grantRef }); await until(() => sa.messages.some(value => value.type === 'call.published'));
  f.send(sa, aa, 'call.video.withdraw', { cameraGeneration: 1 }); await until(() => sa.messages.some(value => value.type === 'call.video.withdrawn'));
  assert.equal(sa.messages.filter(value => value.type === 'call.peers').at(-1).peers.find(value => value.deviceId === aa.deviceId).microphone, true);
});
test('current membership removal retires pending camera grant, ICE and original socket', async t => {
  const f = await fixture(t), [a, b] = f.actors, aa = good(await f.join(a, 'a_owner')), bb = good(await f.join(b, 'b_removed'));
  await f.socketFor(aa); const sb = await f.socketFor(bb); const grant = good(await f.camera(b, bb));
  good(await f.rpc(a, 'world.membership.remove', { communityId: f.group.communityId, profileId: b.state.accountId }));
  await until(() => sb.readyState === WebSocket.CLOSED); assert.equal((await f.ice(b, bb)).ok, false); assert.equal((await f.camera(b, bb, grant.cameraGeneration)).ok, false);
});
test('one-to-one contact block invalidates camera and ICE through the real current contact fence', async t => {
  const f = await fixture(t), [a, b] = f.actors;
  const request = good(await f.rpc(a, 'contacts.requestAccount', { accountId: b.state.accountId })); good(await f.rpc(b, 'contacts.accept', { requestId: request.requestId }));
  const aa = good(await f.rpc(a, 'call.join', { requestId: 'private_a', mode: 'one-to-one', targetId: b.state.accountId, mediaProtocol: 2 }));
  const bb = good(await f.rpc(b, 'call.join', { requestId: 'private_b', mode: 'one-to-one', targetId: a.state.accountId, mediaProtocol: 2 }));
  const sa = await f.socketFor(aa); await f.socketFor(bb); const grant = good(await f.camera(a, aa));
  good(await f.rpc(b, 'contacts.block', { peerAccountId: a.state.accountId })); assert.equal((await f.ice(a, aa)).ok, false);
  f.send(sa, aa, 'call.video.publish', { grantRef: grant.grantRef, cameraGeneration: grant.cameraGeneration }); await until(() => sa.readyState === WebSocket.CLOSED);
});

test('camera video signaling rejects audio/data SDP and leaves the other participant connected', async t => {
  const f = await fixture(t), [a, b] = f.actors, aa = good(await f.join(a, 'a_video_only')), bb = good(await f.join(b, 'b_video_only'));
  const sa = await f.socketFor(aa), sb = await f.socketFor(bb), grant = good(await f.camera(a, aa)); await f.publish(sa, aa, grant);
  f.send(sa, aa, 'video.offer', { signal: f.signal(aa, bb, 1, 0, { sdp: 'v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n' }) });
  await until(() => sa.readyState === WebSocket.CLOSED); assert.equal(sb.readyState, WebSocket.OPEN);
  assert.equal(sb.messages.some(value => value.type === 'video.offer'), false);
});
