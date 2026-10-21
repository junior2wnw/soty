import { parseAppLaunchRoute } from '../world/app-launch.mjs';
import { parseCompactJoinCode } from 'trustlink-kernel';

export const MAX_CONNECTION_INPUT_BYTES = 32_768;
const opaqueId = /^[A-Za-z0-9_-]{16,160}$/u;
const encoder = new TextEncoder();
const only = (parameters, names) => new Set(parameters.keys()).size === names.length
  && names.every(name => parameters.getAll(name).length === 1);

/** Classify an explicit same-origin link. No identity, permission or side effect
 * is obtained here; the selected existing flow still performs its own checks. */
export function parseConnectionIntent(value, expectedOrigin) {
  if (typeof value !== 'string' || typeof expectedOrigin !== 'string'
    || value.length > MAX_CONNECTION_INPUT_BYTES || !value.isWellFormed()
    || encoder.encode(value).length > MAX_CONNECTION_INPUT_BYTES) return null;
  const source = value.trim();
  if (!source || /[\\\u0000-\u0020\u007f]/u.test(source)
    || !/^(?:https?:\/\/[^/?#]+)?\/?(?:[?#]|$)/iu.test(source)) return null;
  let base, url;
  try { base = new URL(expectedOrigin); url = new URL(source, expectedOrigin + '/'); }
  catch { return null; }
  if (base.origin !== expectedOrigin
    || !['http:', 'https:'].includes(base.protocol)
    || url.origin !== expectedOrigin || url.pathname !== '/' || url.username || url.password) return null;
  const hasFragment = source.includes('#');
  const hasQuery = source.split('#', 1)[0].includes('?');
  if (hasQuery) {
    if (hasFragment) return null;
    if (!url.search || url.search.slice(1).split('&').some(part => !part)) return null;
    const parameters = url.searchParams, kind = parameters.get('connect');
    if (kind === 'contact' || kind === 'device') {
      const field = kind === 'contact' ? 'card' : 'request', id = parameters.get(field);
      return only(parameters, ['connect', field]) && opaqueId.test(id ?? '')
        ? Object.freeze({ kind, id }) : null;
    }
    const joinCode = parameters.get('j');
    // The legacy request remains a request. Its room-key exchange is unchanged.
    if (only(parameters, ['j']) && typeof joinCode === 'string' && joinCode.length <= 2048
      && /^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/u.test(joinCode)) {
      try { parseCompactJoinCode(joinCode); } catch { return null; }
      return Object.freeze({ kind: 'legacy-room', joinCode, route: `/?j=${encodeURIComponent(joinCode)}` });
    }
    return null;
  }
  if (!hasFragment || !url.hash) return null;
  if (/^#soty-invite=[A-Za-z0-9_-]{43}$/u.test(url.hash)) {
    // Admission stays with the existing fragment capturer. Never return its token.
    return Object.freeze({ kind: 'platform-invitation' });
  }
  const profile = /^#profile\/([A-Za-z0-9_-]{3,160})$/u.exec(url.hash);
  if (profile) return Object.freeze({ kind: 'profile', id: profile[1], route: url.hash });
  const community = /^#community\/([A-Za-z0-9_-]{3,160})(?:\/(about|chat|apps))?$/u.exec(url.hash);
  if (community) {
    const tab = community[2] ?? 'about';
    return Object.freeze({ kind: 'community', id: community[1], tab, route: `#community/${community[1]}/${tab}` });
  }
  try {
    const launch = parseAppLaunchRoute(url.hash);
    return launch ? Object.freeze({ kind: 'application', launch }) : null;
  } catch { return null; }
}
