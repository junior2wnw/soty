import { randomUUID } from 'node:crypto';
import { isP2pCandidate, isP2pDescription } from './validators.js';
import { createHumanCallIce } from './human-call-ice.js';
import { createHumanCallVideo } from './human-call-video.js';

const REF_MS = 30_000, LEASE_MS = 30_000, MAX_CALLS = 16, MAX_REFS = 128, MAX_SOCKETS = 128, MAX_PENDING_PER_CALL = 8;
const validId = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,160}$/u.test(value);
class HumanCallError extends Error { constructor(code) { super(code); this.code = code; } }
const check = (ok, code = 'human_call_invalid') => { if (!ok) throw new HumanCallError(code); };
function fields(value, required, optional = []) {
  check(value && typeof value === 'object' && !Array.isArray(value));
  const descriptors = Object.getOwnPropertyDescriptors(value), allowed = new Set([...required, ...optional]);
  check(Reflect.ownKeys(descriptors).every(key => typeof key === 'string' && allowed.has(key)
    && Object.hasOwn(descriptors[key], 'value')) && required.every(key => Object.hasOwn(descriptors, key)));
}

/** Signed Connect extension and a bounded, process-local signaling admission.
 * Root's existing Connect/World instances supply all identity and membership.
 * No media, key store, provider or database is opened by this module. */
export function createHumanCallModule({ getConnect, getWorld, allowedOrigins = [], clock = Date.now, mediaPolicy = null } = {}) {
  check(typeof getConnect === 'function' && typeof getWorld === 'function' && typeof clock === 'function');
  check(Array.isArray(allowedOrigins) && allowedOrigins.length <= 64);
  const origins = new Set(allowedOrigins.map(value => { const url = new URL(value); check(url.origin === value); return value; }));
  if (mediaPolicy !== null) {
    fields(mediaPolicy, ['schema', 'videoEnabled'], ['icePolicy']);
    check(mediaPolicy.schema === 'soty.human-call-media-policy.v1' && typeof mediaPolicy.videoEnabled === 'boolean');
  }
  const ice = createHumanCallIce({ policy: mediaPolicy?.icePolicy ?? null, clock });
  // A trusted explicit policy and an ICE configuration are both required.
  const video = createHumanCallVideo({ enabled: mediaPolicy?.videoEnabled === true && ice.status().configured, clock });
  const operations = new Set(['call.join', 'call.microphone', 'call.leave', 'call.inbox', 'call.decline',
    'call.camera', 'call.camera.cancel', 'call.ice.get']);
  const calls = new Map(), scopes = new Map(), refs = new Map(), sockets = new Map();
  let closed = false, wss = null, timer = null, epoch = 0, unsubscribeDevice, unsubscribeMembership;
  const now = () => { const value = clock(); check(Number.isSafeInteger(value) && value >= 0); return value; };
  const publicMember = member => ({ accountId: member.actor.accountId, deviceId: member.actor.deviceId,
    label: member.actor.label, microphone: member.microphone, ...video.view(member) });
  const peers = call => [...call.members.values()].filter(member => member.ws?.readyState === 1).map(publicMember);
  const send = (ws, value) => { if (ws?.readyState === 1) ws.send(JSON.stringify(value)); };
  const rejectSocket = socket => {
    try { if (typeof socket.terminate === 'function') socket.terminate(); else socket.close(1008, 'human_call_denied'); }
    catch { /* Caller owns this socket; an unobserved close never frees a retained slot. */ }
  };
  const broadcast = call => {
    const message = { type: 'call.peers', callId: call.id, epoch: call.epoch, peers: peers(call) };
    for (const member of [...call.members.values()]) send(member.ws, message);
  };
  function drop(call, member, reason = 'human_call_ended') {
    if (call.members.get(member.actor.deviceId) !== member) return;
    call.members.delete(member.actor.deviceId); member.microphone = false;
    video.retire(member); if (member.admission) ice.retire(member.admission.grantRef);
    for (const ref of refs.values()) if (ref.member === member) ref.state = 'retired';
    const socket = member.ws; member.ws = null;
    try { socket?.close(1000, reason); } catch { /* Local removal already committed. */ }
    if (call.members.size === 0) { calls.delete(call.id); scopes.delete(call.key); }
    else broadcast(call);
  }
  function prune() {
    const at = now();
    for (const [id, ref] of refs) if (ref.expiresAt <= at) refs.delete(id);
    for (const call of [...calls.values()]) for (const member of [...call.members.values()]) {
      if (member.expiresAt <= at) drop(call, member, 'human_call_expired');
    }
  }
  function current(actor, call, action) {
    check(!closed, 'human_call_closed');
    try {
      return getConnect().withActorAuthorityFence(actor, () => getWorld().withCommunityAuthorityFence(() => {
        if (call?.mode === 'group') check(getWorld().canAccessCommunity(actor.accountId, call.targetId), 'human_call_denied');
        if (call?.mode === 'one-to-one') check(getConnect().hasCurrentContact(actor,
          call.parties.find(accountId => accountId !== actor.accountId)), 'human_call_denied');
        return action();
      }));
    } catch (error) { if (error instanceof HumanCallError) throw error; throw new HumanCallError('human_call_denied'); }
  }
  const memberFor = (call, actor) => {
    const member = call?.members.get(actor.deviceId);
    check(member?.actor.accountId === actor.accountId, 'human_call_denied'); return member;
  };
  const mediaScope = (call, member) => ({ accountId: member.actor.accountId, deviceId: member.actor.deviceId,
    callId: call.id, epoch: call.epoch, joinRef: member.admission.grantRef });
  const activePeer = (call, member, negotiated = false) => [...call.members.values()].some(peer => peer !== member
    && peer.ws?.readyState === 1 && peer.expiresAt > now() && (!negotiated || peer.mediaProtocol === 2)
    && getConnect().isActorActive(peer.actor) && (call.mode !== 'group' || getWorld().canAccessCommunity(peer.actor.accountId, call.targetId)));
  const accountHasCall = (accountId, allowedCall = null) => [...calls.values()].some(call => call !== allowedCall
    && [...call.members.values()].some(member => member.actor.accountId === accountId));
  const liveCallerFor = (call, calleeAccountId) => [...call.members.values()].find(member =>
    member.actor.accountId !== calleeAccountId && member.ws?.readyState === 1 && member.expiresAt > now());
  function inbox(actor) {
    current(actor, null, () => true);
    const accountId = actor.accountId;
    const candidates = [...calls.values()].filter(call => call.mode === 'one-to-one'
      && call.parties.includes(accountId) && ![...call.members.values()].some(member => member.actor.accountId === accountId))
      .sort((a, b) => a.epoch - b.epoch);
    for (const call of candidates) {
      const caller = liveCallerFor(call, accountId);
      if (!caller || !getConnect().isActorActive(caller.actor)) continue;
      try {
        // The callee fence checks the reciprocal relationship; caller activity is host-checked outside it.
        current(actor, call, () => true);
        return { invitation: { callId: call.id, epoch: call.epoch, roomId: caller.actor.accountId,
          label: typeof caller.actor.label === 'string' ? caller.actor.label.replace(/[\u0000-\u001f\u007f]/gu, ' ').replace(/\s+/gu, ' ').trim().slice(0, 160) : '' } };
      } catch { /* Stale or no longer reciprocal invitations are omitted. */ }
    }
    return { invitation: null };
  }
  function reference(call, member, purpose, requestId) {
    check(refs.size < MAX_REFS, 'human_call_capacity');
    const id = randomUUID(), expiresAt = now() + REF_MS;
    refs.set(id, { call, member, purpose, requestId, expiresAt, state: 'issued' });
    return { schema: 'soty.human-call-admission.v1', purpose, callId: call.id, epoch: call.epoch,
      accountId: member.actor.accountId, deviceId: member.actor.deviceId, grantRef: id, expiresAt };
  }
  function execute({ op, args = {}, actor } = {}) {
    check(operations.has(op), 'human_call_invalid'); prune();
    check(wss && !closed, 'human_call_unavailable');
    if (op === 'call.inbox') { fields(args, []); return inbox(actor); }
    if (op === 'call.decline') {
      fields(args, ['callId', 'epoch']); check(validId(args.callId) && Number.isSafeInteger(args.epoch) && args.epoch > 0);
      const call = calls.get(args.callId); check(call && call.epoch === args.epoch && call.mode === 'one-to-one', 'human_call_stale');
      check(call.parties.includes(actor?.accountId), 'human_call_denied');
      const caller = liveCallerFor(call, actor?.accountId); check(caller, 'human_call_stale');
      check(![...call.members.values()].some(member => member.actor.accountId === actor.accountId), 'human_call_active');
      check(getConnect().isActorActive(caller.actor), 'human_call_stale');
      return current(actor, call, () => {
        check(calls.get(call.id) === call && call.epoch === args.epoch
          && call.members.get(caller.actor.deviceId) === caller && caller.ws?.readyState === 1 && caller.expiresAt > now(), 'human_call_stale');
        check(![...call.members.values()].some(member => member.actor.accountId === actor.accountId), 'human_call_active');
        drop(call, caller, 'human_call_declined'); return { declined: true };
      });
    }
    if (op === 'call.join') {
      fields(args, ['requestId', 'mode', 'targetId'], ['expectedCallId', 'expectedEpoch', 'mediaProtocol']);
      check(args.mediaProtocol === undefined || args.mediaProtocol === 2);
      check(validId(args.requestId) && validId(args.targetId) && ['one-to-one', 'group'].includes(args.mode));
      const expected = Object.hasOwn(args, 'expectedCallId') || Object.hasOwn(args, 'expectedEpoch');
      check(!expected || (args.mode === 'one-to-one' && validId(args.expectedCallId)
        && Number.isSafeInteger(args.expectedEpoch) && args.expectedEpoch > 0), 'human_call_stale');
      let expectedCall = null;
      if (expected) {
        expectedCall = calls.get(args.expectedCallId);
        check(expectedCall && expectedCall.epoch === args.expectedEpoch && expectedCall.mode === 'one-to-one'
          && expectedCall.parties.includes(actor?.accountId) && expectedCall.parties.includes(args.targetId), 'human_call_stale');
        const caller = liveCallerFor(expectedCall, actor.accountId);
        check(caller?.actor.accountId === args.targetId, 'human_call_stale');
        check(getConnect().isActorActive(caller.actor), 'human_call_stale');
      }
      return current(actor, null, () => {
        check(args.mode !== 'one-to-one' || actor.accountId !== args.targetId);
        const parties = args.mode === 'one-to-one' ? [actor.accountId, args.targetId].sort() : null;
        const key = JSON.stringify([args.mode, parties ?? args.targetId]);
        let call = scopes.get(key);
        if (expected) check(call === expectedCall && call.id === args.expectedCallId && call.epoch === args.expectedEpoch,
          'human_call_stale');
        if (args.mode === 'group') check(getWorld().canAccessCommunity(actor.accountId, args.targetId), 'human_call_denied');
        else check(getConnect().hasCurrentContact(actor, args.targetId), 'human_call_denied');
        if (expected) {
          const caller = liveCallerFor(call, actor.accountId);
          check(caller?.actor.accountId === args.targetId, 'human_call_stale');
        }
        const existing = call && [...call.members.values()].find(member => member.actor.accountId === actor.accountId);
        if (existing) {
          check(existing.actor.deviceId === actor.deviceId && existing.requestId === args.requestId && !existing.ws
            && existing.mediaProtocol === (args.mediaProtocol ?? 1),
            'human_call_active');
          const ref = [...refs.entries()].find(([, value]) => value.member === existing && value.purpose === 'join' && value.state === 'issued');
          check(ref, 'human_call_reconcile_required');
          return { ...existing.admission };
        }
        check(!accountHasCall(actor.accountId, call), 'human_call_account_active');
        if (!call) {
          check(calls.size < MAX_CALLS && epoch < Number.MAX_SAFE_INTEGER, 'human_call_capacity');
          call = { id: randomUUID(), epoch: ++epoch, key, mode: args.mode, targetId: args.targetId, parties, members: new Map() };
          calls.set(call.id, call); scopes.set(key, call);
        }
        check(call.members.size < (args.mode === 'group' ? 4 : 2), 'human_call_full');
        const member = { actor, requestId: args.requestId, ws: null, microphone: false, mediaProtocol: args.mediaProtocol ?? 1, expiresAt: now() + LEASE_MS };
        call.members.set(actor.deviceId, member);
        try {
          member.admission = reference(call, member, 'join', args.requestId);
          video.open(member, mediaScope(call, member), member.mediaProtocol);
          if (member.mediaProtocol === 2) member.admission = Object.freeze({ ...member.admission, mediaProtocol: 2,
            mediaInstance: video.view(member).mediaInstance, cameraConfigured: video.status().configured, iceConfigured: ice.status().configured });
        }
        catch (error) { drop(call, member); throw error; }
        return { ...member.admission };
      });
    }
    if (['call.camera', 'call.camera.cancel', 'call.ice.get'].includes(op)) {
      const extra = op === 'call.camera' ? ['requestId', 'expectedCameraGeneration']
        : op === 'call.camera.cancel' ? ['grantRef', 'cameraGeneration'] : ['requestId', 'expectedRevision'];
      fields(args, ['callId', 'epoch', 'expectedJoinRef', ...extra]);
      check(validId(args.callId) && Number.isSafeInteger(args.epoch) && args.epoch > 0 && validId(args.expectedJoinRef));
      const call = calls.get(args.callId); check(call && call.epoch === args.epoch, 'human_call_stale');
      const member = memberFor(call, actor), scope = mediaScope(call, member);
      check(member.mediaProtocol === 2 && args.expectedJoinRef === scope.joinRef, 'human_call_stale');
      if (op === 'call.ice.get') {
        // The synchronous issuer calls this twice, in separate fresh fences.
        // A pending initial join may obtain ICE before opening its one socket.
        const isCurrent = candidate => {
          try { return current(actor, call, () => calls.get(call.id) === call && call.members.get(actor.deviceId) === member
            && candidate.joinRef === member.admission.grantRef && member.expiresAt > now()
            && (member.ws?.readyState === 1 || member.admission.expiresAt > now())); } catch { return false; }
        };
        return ice.issue(scope, { requestId: args.requestId, expectedRevision: args.expectedRevision }, isCurrent);
      }
      return current(actor, call, () => {
        check(member.ws?.readyState === 1 && member.expiresAt > now(), 'human_call_not_joined');
        if (op === 'call.camera.cancel') {
          video.withdraw(member, scope, { grantRef: args.grantRef, cameraGeneration: args.cameraGeneration }, true);
          broadcast(call); return { withdrawn: true, cameraGeneration: args.cameraGeneration };
        }
        check(activePeer(call, member, true), 'human_call_camera_waiting');
        const admission = video.authorize(member, scope, { requestId: args.requestId, expectedCameraGeneration: args.expectedCameraGeneration });
        broadcast(call); return admission;
      });
    }
    fields(args, ['callId', 'epoch'], op === 'call.microphone' ? ['requestId'] : ['expectedJoinRef']);
    check(validId(args.callId) && Number.isSafeInteger(args.epoch));
    check(args.expectedJoinRef === undefined || validId(args.expectedJoinRef));
    const call = calls.get(args.callId);
    if (!call && op === 'call.leave') return current(actor, null, () => ({ left: true }));
    check(call && call.epoch === args.epoch, 'human_call_stale');
    return current(actor, call, () => {
      if (op === 'call.leave') {
        check(call.mode !== 'one-to-one' || call.parties.includes(actor.accountId), 'human_call_denied');
        const member = call.members.get(actor.deviceId);
        if (member) check(member.actor.accountId === actor.accountId, 'human_call_denied');
        // A socket close may have removed this device before its signed cleanup.
        // The original admission also prevents an older cleanup from ending a rejoin.
        if (!member || args.expectedJoinRef !== undefined && member.admission.grantRef !== args.expectedJoinRef) return { left: true };
        drop(call, member); return { left: true };
      }
      const member = memberFor(call, actor);
      check(validId(args.requestId) && member.ws?.readyState === 1, 'human_call_not_joined');
      check([...call.members.values()].some(value => value !== member && value.ws?.readyState === 1), 'human_call_waiting');
      check(!member.microphone && ![...refs.values()].some(ref => ref.member === member && ref.purpose === 'microphone' && ref.state === 'issued'),
        'human_call_publication_pending');
      return reference(call, member, 'microphone', args.requestId);
    });
  }
  function consume(id, purpose, call, socket) {
    const ref = refs.get(id);
    check(ref && ref.state === 'issued' && ref.purpose === purpose && ref.call === call && ref.expiresAt > now(), 'human_call_grant_invalid');
    const member = ref.member;
    check(call.members.get(member.actor.deviceId) === member && (purpose === 'join' ? !member.ws : member.ws === socket), 'human_call_grant_invalid');
    return current(member.actor, call, () => { ref.state = 'consumed'; return member; });
  }
  function attachSignaling(server) {
    check(!closed && !wss && origins.size > 0 && typeof server?.on === 'function', 'human_call_mount_invalid');
    wss = server;
    unsubscribeDevice = getConnect().subscribeRevocations(event => {
      for (const call of [...calls.values()]) for (const member of [...call.members.values()]) {
        if (member.actor.deviceId === event.deviceId) drop(call, member, 'human_call_revoked');
      }
    });
    unsubscribeMembership = getWorld().subscribeMembership(event => {
      for (const call of [...calls.values()]) if (call.mode === 'group' && call.targetId === event.communityId) {
        for (const member of [...call.members.values()]) {
          try { current(member.actor, call, () => true); } catch { drop(call, member, 'human_call_membership_changed'); }
        }
      }
    });
    server.on('connection', (socket, request, callId) => {
      let call = calls.get(callId), bound = null, windowAt = now(), frames = 0, bytes = 0;
      if (closed || !call || socket.readyState !== 1 || !origins.has(request?.headers?.origin)) { rejectSocket(socket); return; }
      const pending = [...sockets.values()].filter(record => record.call === call && !record.bound).length;
      if (sockets.size >= MAX_SOCKETS || pending >= MAX_PENDING_PER_CALL) { rejectSocket(socket); return; }
      const record = { call, bound: false, ping: null, pingExpiresAt: 0, pulse: null }; sockets.set(socket, record);
      const waiting = setTimeout(() => { if (!bound) socket.close(1008, 'human_call_bind_timeout'); }, 10_000); waiting.unref?.();
      socket.on('close', () => { clearTimeout(waiting); sockets.delete(socket); if (bound?.ws === socket) drop(call, bound); });
      // Browsers answer a native WebSocket ping independently of a background
      // page's JavaScript timers. Sending a ping grants nothing: only its fresh,
      // single-use echo under the current original actor fence renews a lease.
      socket.on('pong', raw => {
        try {
          if (!record.bound) return;
          check(!closed && record.bound && sockets.get(socket) === record && calls.get(call.id) === call
            && bound?.ws === socket && call.members.get(bound.actor.deviceId) === bound
            && socket.readyState === 1 && bound.expiresAt > now(), 'human_call_stale');
          // RFC6455 also permits unsolicited pong. It does not acknowledge our
          // challenge and therefore cannot refresh any authority or deadline.
          if (!Buffer.isBuffer(raw) || !record.ping || record.pingExpiresAt <= now()
            || raw.length !== record.ping.length || !raw.equals(record.ping)) return;
          record.ping = null; record.pingExpiresAt = 0;
          current(bound.actor, call, () => { bound.expiresAt = now() + LEASE_MS; });
        } catch { if (bound?.ws === socket) drop(call, bound, 'human_call_revoked'); else rejectSocket(socket); }
      });
      record.pulse = () => {
        if (!record.bound || bound?.ws !== socket || call.members.get(bound.actor.deviceId) !== bound || socket.readyState !== 1) return;
        if (record.ping && record.pingExpiresAt > now()) return;
        record.ping = Buffer.from(randomUUID()); record.pingExpiresAt = now() + 10_000;
        try { socket.ping(record.ping); } catch { drop(call, bound, 'human_call_heartbeat_failed'); }
      };
      socket.on('message', raw => {
        try {
          prune(); check(!closed && sockets.has(socket) && calls.get(call.id) === call, 'human_call_stale');
          const size = Buffer.byteLength(raw); check(size <= 64_000);
          if (now() - windowAt >= 10_000) { windowAt = now(); frames = 0; bytes = 0; }
          check(++frames <= 128 && (bytes += size) <= 512_000, 'human_call_rate');
          const message = JSON.parse(raw.toString());
          if (!bound) {
            fields(message, ['type', 'callId', 'epoch', 'grantRef']);
            check(message.type === 'call.bind' && message.callId === call.id && message.epoch === call.epoch);
            bound = consume(message.grantRef, 'join', call, socket); bound.ws = socket; record.bound = true; clearTimeout(waiting);
            current(bound.actor, call, () => {
              bound.expiresAt = now() + LEASE_MS;
              send(socket, { type: 'call.ready', callId: call.id, epoch: call.epoch, self: publicMember(bound), peers: peers(call) });
              broadcast(call);
            }); return;
          }
          check(bound.ws === socket && call.members.get(bound.actor.deviceId) === bound, 'human_call_stale');
          if (['call.video.publish', 'call.video.withdraw', 'video.offer', 'video.answer', 'video.candidate'].includes(message.type)) {
            const publication = message.type === 'call.video.publish', withdrawal = message.type === 'call.video.withdraw';
            fields(message, ['type', 'callId', 'epoch', ...(publication ? ['grantRef', 'cameraGeneration'] : withdrawal ? ['cameraGeneration'] : ['signal'])]);
            check(message.callId === call.id && message.epoch === call.epoch);
            current(bound.actor, call, () => {
              check(bound.mediaProtocol === 2 && bound.expiresAt > now(), 'human_call_stale');
              bound.expiresAt = now() + LEASE_MS;
              const scope = mediaScope(call, bound);
              if (publication) {
                if (!activePeer(call, bound, true)) {
                  video.withdraw(bound, scope, { grantRef: message.grantRef, cameraGeneration: message.cameraGeneration }, true);
                  broadcast(call); return;
                }
                const ack = video.publish(bound, scope, { grantRef: message.grantRef, cameraGeneration: message.cameraGeneration });
                if (ack) { send(socket, ack); broadcast(call); } return;
              }
              if (withdrawal) {
                video.withdraw(bound, scope, { cameraGeneration: message.cameraGeneration });
                send(socket, { type: 'call.video.withdrawn', callId: call.id, epoch: call.epoch, cameraGeneration: message.cameraGeneration });
                broadcast(call); return;
              }
              const target = call.members.get(message.signal?.targetDeviceId);
              // A peer can leave while an otherwise valid video frame is in
              // flight. Dropping that stale frame must not tear down audio.
              if (!target || target === bound || target.ws?.readyState !== 1 || target.expiresAt <= now()
                || !getConnect().isActorActive(target.actor)
                || call.mode === 'group' && !getWorld().canAccessCommunity(target.actor.accountId, call.targetId)) return;
              const forwarded = video.route(bound, scope, target, mediaScope(call, target), message.type, message.signal);
              if (forwarded) send(target.ws, forwarded);
            }); return;
          }
          fields(message, ['type', 'callId', 'epoch'], ['signal', 'grantRef']);
          check(message.callId === call.id && message.epoch === call.epoch);
          if (message.type === 'call.publish') {
            check(consume(message.grantRef, 'microphone', call, socket) === bound);
            // Separate fresh fences, never a nested actor-fence call.
            current(bound.actor, call, () => {
              bound.expiresAt = now() + LEASE_MS; bound.microphone = true;
              send(socket, { type: 'call.published', callId: call.id, epoch: call.epoch, grantRef: message.grantRef }); broadcast(call);
            }); return;
          }
          current(bound.actor, call, () => {
            bound.expiresAt = now() + LEASE_MS;
            if (message.type === 'call.ping') { send(socket, { type: 'call.pong', callId: call.id, epoch: call.epoch }); return; }
            if (message.type === 'call.unpublish') {
              bound.microphone = false;
              for (const ref of refs.values()) if (ref.member === bound && ref.purpose === 'microphone' && ref.state === 'issued') ref.state = 'retired';
              broadcast(call); return;
            }
            check(['p2p.offer', 'p2p.answer', 'p2p.candidate'].includes(message.type));
            const signal = message.signal, candidate = message.type === 'p2p.candidate';
            check(candidate ? isP2pCandidate(signal) : isP2pDescription(signal, message.type === 'p2p.offer' ? 'offer' : 'answer'));
            const target = call.members.get(signal.targetDeviceId);
            check(target && target !== bound && target.ws?.readyState === 1 && getConnect().isActorActive(target.actor), 'human_call_peer_denied');
            if (call.mode === 'group') check(getWorld().canAccessCommunity(target.actor.accountId, call.targetId), 'human_call_peer_denied');
            const selected = candidate ? { candidate: signal.candidate, ...(signal.sdpMid !== undefined ? { sdpMid: signal.sdpMid } : {}),
              ...(signal.sdpMLineIndex !== undefined ? { sdpMLineIndex: signal.sdpMLineIndex } : {}) } : { kind: signal.kind, sdp: signal.sdp };
            send(target.ws, { type: message.type, callId: call.id, epoch: call.epoch,
              signal: { id: signal.id, targetDeviceId: target.actor.deviceId, ...selected, deviceId: bound.actor.deviceId } });
          });
        } catch { socket.close(1008, 'human_call_denied'); }
      });
    });
    timer = setInterval(() => {
      try { prune(); for (const call of [...calls.values()]) for (const member of [...call.members.values()]) {
        try { current(member.actor, call, () => true); } catch { drop(call, member, 'human_call_revoked'); }
      }
      for (const record of [...sockets.values()]) record.pulse();
      } catch { close(); }
    }, 5000); timer.unref?.();
    server.on('close', close);
  }
  function close() {
    if (closed) return; closed = true; clearInterval(timer); unsubscribeDevice?.(); unsubscribeMembership?.();
    for (const call of [...calls.values()]) for (const member of [...call.members.values()]) drop(call, member);
    for (const socket of [...sockets.keys()]) rejectSocket(socket);
    refs.clear(); scopes.clear(); ice.close();
  }
  return Object.freeze({ operations, execute, attachSignaling, close,
    status: () => Object.freeze({ signalingMounted: Boolean(wss) && !closed, participantLimit: 4,
      calls: calls.size, references: refs.size, ownedSockets: sockets.size,
      pendingSockets: [...sockets.values()].filter(record => !record.bound).length,
      turnConfigured: ice.status().turnConfigured, videoConfigured: video.status().configured, remoteMediaQualified: false }) });
}
