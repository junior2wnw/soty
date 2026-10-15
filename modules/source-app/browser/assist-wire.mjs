// Pilot protocol: data only, no selectors, URLs, script or filesystem operations.
export const ASSIST_SCHEMA = 'soty.browser-assist-pilot.v1';
export const exact = (v, keys) => !!v && typeof v === 'object' && !Array.isArray(v)
  && Object.keys(v).length === keys.length && keys.every(k => Object.hasOwn(v, k));
export const nonce = v => typeof v === 'string' && /^[a-f0-9]{32}$/u.test(v);
export function validBinding(v) {
  if (!exact(v, ['appId', 'origin', 'targetRevision', 'targetDigest', 'generation', 'channel', ...(v?.domainId === undefined ? [] : ['domainId'])])) return false;
  if (v.domainId !== undefined && (typeof v.domainId !== 'string' || !/^[A-Za-z0-9_.:-]{1,180}$/u.test(v.domainId))) return false;
  try { return /^app-[a-f0-9]{32}$/u.test(v.appId) && new URL(v.origin).origin === v.origin
    && ['https:', 'http:'].includes(new URL(v.origin).protocol) && Number.isSafeInteger(v.targetRevision) && v.targetRevision > 0
    && /^[a-f0-9]{64}$/u.test(v.targetDigest) && Number.isSafeInteger(v.generation) && v.generation > 0 && nonce(v.channel); } catch { return false; }
}
export function validCommand(v) {
  if (exact(v, ['kind']) && v.kind === 'observe') return true;
  return exact(v, v?.kind === 'fill' ? ['kind', 'snapshot', 'target', 'text'] : ['kind', 'snapshot', 'target'])
    && ['click', 'fill'].includes(v.kind) && nonce(v.snapshot) && /^n[0-9]{1,3}$/u.test(v.target)
    && (v.kind !== 'fill' || typeof v.text === 'string' && v.text.length <= 1000 && !/[\u0000-\u001f\u007f]/u.test(v.text));
}
export const sameBinding = (a, b) => validBinding(a) && validBinding(b)
  && Object.keys(a).length === Object.keys(b).length && Object.keys(a).every(k => a[k] === b[k]);
