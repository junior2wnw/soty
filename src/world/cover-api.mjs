/** Signed UI adapter. No origin, art-key, public URL or actor inference. */
const methods = Object.freeze({
  status: [[], ['requestId', 'draftId']],
  prepare: [['requestId', 'expectedTargetRevision', 'expectedPolicyEpoch', 'expectedCoverRevision', 'mime', 'base64'], ['crop']],
  prepareFromVersion: [['requestId', 'expectedTargetRevision', 'expectedPolicyEpoch', 'expectedCoverRevision', 'versionId'], ['crop']],
  commit: [['requestId', 'draftId'], []], restore: [['requestId', 'expectedTargetRevision', 'expectedPolicyEpoch', 'expectedCoverRevision', 'versionId'], []],
  remove: [['requestId', 'expectedTargetRevision', 'expectedPolicyEpoch', 'expectedCoverRevision'], ['mode']],
  read: [[], ['versionId', 'width']], versions: [[], ['limit']], cancel: [['requestId', 'draftId'], []],
});
export const COVER_UPLOAD_LIMIT = 320 * 1024;
export const coverError = code => Object.assign(new Error(code), { code });
const check = (value, code = 'cover_response_invalid') => { if (!value) throw coverError(code); };
export const coverId = value => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$/u.test(value);
export const coverRequestId = value => typeof value === 'string' && /^cover1\.[0-9]{13}\.[a-f0-9]{32}$/u.test(value);
export function closedData(value, required = [], optional = []) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const keys = Reflect.ownKeys(value);
  return required.every(key => keys.includes(key)) && keys.every(key => typeof key === 'string' && (required.includes(key) || optional.includes(key))
    && Object.getOwnPropertyDescriptor(value, key)?.enumerable === true && Object.hasOwn(Object.getOwnPropertyDescriptor(value, key), 'value'));
}
export function coverHead(value) {
  check(value && Number.isSafeInteger(value.revision) && value.revision >= 0 && ['inherit', 'none', 'custom'].includes(value.mode));
  check(value.version === null || coverId(value.version));
  check(value.mode !== 'custom' || coverId(value.version));
  return Object.freeze({ revision: value.revision, mode: value.mode, version: value.version,
    ...(Number.isSafeInteger(value.artVersion) ? { artVersion: value.artVersion } : {}) });
}
export function coverScope(value, { kind, targetId, accountId }) {
  const keys = ['storeInstanceId', 'kind', 'targetId', 'ownerAccountId', 'targetRevision', 'policyEpoch', 'coverRevision'];
  check(closedData(value, keys));
  check(coverId(value.storeInstanceId) && value.kind === kind && value.targetId === targetId && value.ownerAccountId === accountId);
  for (const key of ['targetRevision', 'policyEpoch', 'coverRevision']) check(Number.isSafeInteger(value[key]) && value[key] >= (key === 'coverRevision' ? 0 : 1));
  return Object.freeze(Object.fromEntries(keys.map(key => [key, value[key]])));
}
export const sameCoverScope = (a, b) => !!a && !!b && ['storeInstanceId', 'kind', 'targetId', 'ownerAccountId', 'targetRevision', 'policyEpoch', 'coverRevision'].every(key => a[key] === b[key]);
export function expectedCover(scope) {
  return { expectedTargetRevision: scope.targetRevision, expectedPolicyEpoch: scope.policyEpoch, expectedCoverRevision: scope.coverRevision };
}
export function bytesToBase64(bytes) {
  check(bytes instanceof Uint8Array && bytes.length > 0 && bytes.length <= COVER_UPLOAD_LIMIT, 'cover_input_limit');
  let text = ''; for (let i = 0; i < bytes.length; i += 8192) text += String.fromCharCode(...bytes.subarray(i, i + 8192));
  return btoa(text);
}
export function base64Bytes(value, maximum = COVER_UPLOAD_LIMIT) {
  check(typeof value === 'string' && value.length > 0 && value.length <= 4 * Math.ceil(maximum / 3) && value.length % 4 === 0 && /^[A-Za-z0-9+/]+={0,2}$/u.test(value), 'cover_image_invalid');
  let decoded; try { decoded = atob(value); } catch { throw coverError('cover_image_invalid'); }
  const bytes = Uint8Array.from(decoded, c => c.charCodeAt(0));
  check(bytes.length <= maximum && btoa(decoded) === value, 'cover_image_invalid'); return bytes;
}
export function createCoverApi({ api, kind, targetId, accountId, isCurrent, digest = bytes => crypto.subtle.digest('SHA-256', bytes) }) {
  check(api && typeof api.request === 'function' && ['app', 'profile'].includes(kind) && coverId(targetId) && coverId(accountId) && typeof isCurrent === 'function', 'cover_configuration_invalid');
  const identity = Object.freeze({ kind, targetId, accountId });
  const current = () => check(isCurrent() === true, 'cover_context_changed');
  async function call(method, args = {}) {
    current(); const shape = methods[method]; check(shape && closedData(args, ...shape), 'cover_arguments_invalid');
    const normalized = {};
    for (const key of [...shape[0], ...shape[1]]) if (Object.hasOwn(args, key)) normalized[key] = args[key];
    if (Object.hasOwn(normalized, 'requestId')) check(coverRequestId(normalized.requestId), 'cover_request_invalid');
    if (normalized.crop !== undefined) {
      check(closedData(normalized.crop, ['x', 'y', 'zoom']) && Number.isFinite(normalized.crop.x) && normalized.crop.x >= 0 && normalized.crop.x <= 1
        && Number.isFinite(normalized.crop.y) && normalized.crop.y >= 0 && normalized.crop.y <= 1 && Number.isFinite(normalized.crop.zoom) && normalized.crop.zoom >= 1 && normalized.crop.zoom <= 4, 'cover_crop_invalid');
      normalized.crop = { x: normalized.crop.x, y: normalized.crop.y, zoom: normalized.crop.zoom };
    }
    if (method === 'prepare') {
      check(['image/png', 'image/jpeg', 'image/webp'].includes(normalized.mime), 'cover_input_invalid');
      base64Bytes(normalized.base64); normalized[kind === 'app' ? 'bytesBase64' : 'dataBase64'] = normalized.base64; delete normalized.base64;
    }
    const reply = await api.request(`${kind === 'app' ? 'apps.cover' : 'world.profile.cover'}.${method}`, {
      expectedAccountId: accountId, [kind === 'app' ? 'appId' : 'targetId']: targetId, ...normalized,
    });
    current(); check(reply && typeof reply === 'object'); const head = coverHead(reply.current);
    if (method === 'status') {
      const scope = coverScope(reply.scope, identity); check(scope.coverRevision === head.revision && Number.isSafeInteger(reply.serverTime) && reply.serverTime >= 1_000_000_000_000);
      const draft = reply.draft;
      check(draft === null || draft && coverId(draft.id) && ['processing', 'ready', 'committed', 'cancelled', 'expired', 'failed'].includes(draft.state) && Number.isSafeInteger(draft.expiresAt));
      return { current: head, scope, serverTime: reply.serverTime, receipt: reply.receipt ?? null,
        draft: draft ? { id: draft.id, state: draft.state, expiresAt: draft.expiresAt } : null };
    }
    if (method === 'read') {
      if (reply.image === null) return { current: head, versionId: reply.versionId ?? null, image: null };
      const value = reply.image;
      check(value && value.mime === 'image/webp' && Number.isSafeInteger(value.width) && Number.isSafeInteger(value.height) && value.width > 0 && value.width <= 960 && value.height > 0 && value.height <= 960
        && typeof value.sha256 === 'string' && /^[a-f0-9]{64}$/u.test(value.sha256) && coverId(reply.versionId), 'cover_image_invalid');
      const bytes = base64Bytes(value[kind === 'app' ? 'base64' : 'dataBase64']);
      check(bytes.length === value.byteCount && bytes.length >= 12 && String.fromCharCode(...bytes.subarray(0, 4)) === 'RIFF' && String.fromCharCode(...bytes.subarray(8, 12)) === 'WEBP', 'cover_image_invalid');
      const hash = Array.from(new Uint8Array(await digest(bytes)), n => n.toString(16).padStart(2, '0')).join(''); current();
      check(hash === value.sha256, 'cover_image_invalid');
      return { current: head, versionId: reply.versionId, image: { mime: value.mime, width: value.width, height: value.height, bytes } };
    }
    if (method === 'versions') {
      check(Array.isArray(reply.versions) && reply.versions.length <= 20);
      return { current: head, versions: reply.versions.map(v => { check(coverId(v.id) && Number.isSafeInteger(v.version) && v.version > 0 && Number.isSafeInteger(v.createdAt) && ['inherit', 'none', 'custom'].includes(v.mode) && typeof v.restorable === 'boolean');
        return { id: v.id, version: v.version, mode: v.mode, createdAt: v.createdAt, restorable: v.restorable }; }) };
    }
    check(reply.receipt && typeof reply.receipt.action === 'string');
    if (['commit', 'restore', 'remove'].includes(method)) check(reply.receipt.action === 'committed' && reply.receipt.targetId === targetId
      && coverId(reply.receipt.versionId) && Number.isSafeInteger(reply.receipt.committedRevision) && reply.receipt.committedRevision > 0 && reply.receipt.committedRevision <= head.revision);
    else if (method === 'prepare' || method === 'prepareFromVersion') check(reply.receipt.action === 'prepared' && coverId(reply.receipt.draftId));
    else check(['cancelled', 'committed', 'expired', 'failed'].includes(reply.receipt.action) && coverId(reply.receipt.draftId));
    if (reply.draft) check(coverId(reply.draft.id) && ['processing', 'ready', 'committed', 'cancelled', 'expired', 'failed'].includes(reply.draft.state) && Number.isSafeInteger(reply.draft.expiresAt));
    return { current: head, receipt: { ...reply.receipt }, replayed: reply.replayed === true,
      ...(reply.draft ? { draft: { id: reply.draft.id, state: reply.draft.state, expiresAt: reply.draft.expiresAt } } : {}) };
  }
  return Object.freeze({ identity, call });
}
