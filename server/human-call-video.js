import { randomUUID } from 'node:crypto';

const id = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,160}$/u.test(value);
const videoSdp = value => typeof value === 'string' && value.length > 0 && value.length <= 40_000
  && value.split(/\r?\n/u).filter(line => line.startsWith('m=')).length === 1
  && value.split(/\r?\n/u).filter(line => line.startsWith('m=')).every(line => line.startsWith('m=video '));
const generation = value => Number.isSafeInteger(value) && value >= 0;
const check = (value, code = 'human_call_video_invalid') => { if (!value) throw Object.assign(new Error(code), { code }); };
export function videoFields(value, required, optional = []) {
  check(value && typeof value === 'object' && !Array.isArray(value));
  const descriptors = Object.getOwnPropertyDescriptors(value), keys = new Set([...required, ...optional]);
  check(Reflect.ownKeys(descriptors).every(key => typeof key === 'string' && keys.has(key) && Object.hasOwn(descriptors[key], 'value'))
    && required.every(key => Object.hasOwn(descriptors, key)));
}
const sameScope = (left, right) => ['accountId', 'deviceId', 'callId', 'epoch', 'joinRef'].every(key => left[key] === right[key]);

/** Bounded process-local camera grants. The host must enter its fresh signed
 * actor/contact/membership fence before every operation; this stores no identity. */
export function createHumanCallVideo({ enabled = false, clock = Date.now } = {}) {
  check(typeof enabled === 'boolean' && typeof clock === 'function');
  const members = new Map();
  const at = () => { const value = clock(); check(Number.isSafeInteger(value) && value >= 0); return value; };
  function open(member, scope, protocol) {
    videoFields(scope, ['accountId', 'deviceId', 'callId', 'epoch', 'joinRef']);
    check(['accountId', 'deviceId', 'callId', 'joinRef'].every(key => id(scope[key])) && Number.isSafeInteger(scope.epoch) && scope.epoch > 0);
    check(!members.has(member) && members.size < 64 && [1, 2].includes(protocol), 'human_call_capacity');
    members.set(member, { scope: Object.freeze({ ...scope }), protocol, instance: randomUUID(), generation: 0, camera: false, pending: null, published: null });
  }
  function state(member, scope) {
    const value = members.get(member); check(value && sameScope(value.scope, scope), 'human_call_video_stale'); return value;
  }
  function authorize(member, scope, args) {
    videoFields(args, ['requestId', 'expectedCameraGeneration']);
    check(id(args.requestId) && generation(args.expectedCameraGeneration));
    const value = state(member, scope);
    check(enabled && value.protocol === 2, 'human_call_camera_unconfigured');
    const pending = value.pending;
    if (pending?.requestId === args.requestId && pending.expected === args.expectedCameraGeneration) {
      check(pending.admission.expiresAt > at(), 'human_call_camera_expired'); return { ...pending.admission };
    }
    // A generation is a tombstone even after cancel/expiry; a new click must
    // explicitly use that generation. An old intent can never republish it.
    check(args.expectedCameraGeneration === value.generation, 'human_call_video_stale');
    check(!value.camera && (!pending || pending.admission.expiresAt <= at()), 'human_call_camera_pending');
    check(value.generation < Number.MAX_SAFE_INTEGER, 'human_call_capacity');
    const admission = Object.freeze({ schema: 'soty.human-camera-admission.v1', callId: scope.callId, epoch: scope.epoch,
      accountId: scope.accountId, deviceId: scope.deviceId, grantRef: randomUUID(), cameraGeneration: ++value.generation, expiresAt: at() + 30_000 });
    value.pending = { requestId: args.requestId, expected: args.expectedCameraGeneration, admission }; value.published = null;
    return { ...admission };
  }
  function publish(member, scope, args) {
    videoFields(args, ['grantRef', 'cameraGeneration']); check(id(args.grantRef) && generation(args.cameraGeneration) && args.cameraGeneration > 0);
    const value = state(member, scope);
    check(enabled && value.protocol === 2, 'human_call_camera_unconfigured');
    const exact = admission => admission?.grantRef === args.grantRef && admission.cameraGeneration === args.cameraGeneration;
    if (value.camera && exact(value.published)) return ack(value.published);
    if (!exact(value.pending?.admission) || value.pending.admission.expiresAt <= at()) return null;
    value.published = value.pending.admission; value.pending = null; value.camera = true;
    return ack(value.published);
  }
  const ack = admission => Object.freeze({ type: 'call.video.published', callId: admission.callId, epoch: admission.epoch,
    grantRef: admission.grantRef, cameraGeneration: admission.cameraGeneration });
  function withdraw(member, scope, args, signed = false) {
    videoFields(args, signed ? ['grantRef', 'cameraGeneration'] : ['cameraGeneration']);
    check(generation(args.cameraGeneration) && args.cameraGeneration > 0 && (!signed || id(args.grantRef)));
    const value = state(member, scope);
    if (args.cameraGeneration !== value.generation) return false;
    if (signed && ![value.pending?.admission, value.published].some(admission => admission?.grantRef === args.grantRef)) return false;
    value.pending = null; value.published = null; value.camera = false; return true;
  }
  function view(member) {
    const value = members.get(member);
    return value?.protocol === 2 ? { mediaProtocol: 2, mediaInstance: value.instance, camera: value.camera, cameraGeneration: value.generation } : {};
  }
  function route(member, scope, target, targetScope, type, signal) {
    const candidate = type === 'video.candidate', kind = type === 'video.offer' ? 'offer' : 'answer';
    check(['video.offer', 'video.answer', 'video.candidate'].includes(type));
    videoFields(signal, ['id', 'targetDeviceId', 'fromMediaInstance', 'toMediaInstance', 'fromCameraGeneration', 'toCameraGeneration', ...(candidate ? ['candidate'] : ['kind', 'sdp'])],
      candidate ? ['sdpMid', 'sdpMLineIndex'] : []);
    check(id(signal.id) && id(signal.targetDeviceId) && id(signal.fromMediaInstance) && id(signal.toMediaInstance) && generation(signal.fromCameraGeneration) && generation(signal.toCameraGeneration));
    if (candidate) {
      check(typeof signal.candidate === 'string' && signal.candidate.length > 0 && signal.candidate.length <= 8_000);
      check(signal.sdpMid === undefined || signal.sdpMid === null || typeof signal.sdpMid === 'string' && signal.sdpMid.length <= 120);
      check(signal.sdpMLineIndex === undefined || signal.sdpMLineIndex === null || Number.isSafeInteger(signal.sdpMLineIndex) && signal.sdpMLineIndex >= 0 && signal.sdpMLineIndex <= 128);
    } else check(signal.kind === kind && videoSdp(signal.sdp));
    const from = state(member, scope), to = state(target, targetScope);
    // Valid but delayed video frames are ignored, never fatal to healthy audio.
    if (!enabled || from.protocol !== 2 || to.protocol !== 2 || !(from.camera || to.camera)
      || from.instance !== signal.fromMediaInstance || to.instance !== signal.toMediaInstance
      || from.generation !== signal.fromCameraGeneration || to.generation !== signal.toCameraGeneration) return null;
    check(signal.targetDeviceId === to.scope.deviceId && member !== target && from.scope.callId === to.scope.callId
      && from.scope.epoch === to.scope.epoch, 'human_call_peer_denied');
    const selected = candidate ? { candidate: signal.candidate, ...(signal.sdpMid !== undefined ? { sdpMid: signal.sdpMid } : {}),
      ...(signal.sdpMLineIndex !== undefined ? { sdpMLineIndex: signal.sdpMLineIndex } : {}) } : { kind, sdp: signal.sdp };
    return { type, callId: scope.callId, epoch: scope.epoch, signal: { id: signal.id, targetDeviceId: to.scope.deviceId,
      deviceId: from.scope.deviceId, fromMediaInstance: from.instance, toMediaInstance: to.instance, fromCameraGeneration: from.generation, toCameraGeneration: to.generation, ...selected } };
  }
  return Object.freeze({ open, authorize, publish, withdraw, view, route,
    retire(member) { members.delete(member); },
    status: () => Object.freeze({ configured: enabled, retainedMembers: members.size, qualified: false }) });
}
