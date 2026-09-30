const appIdPattern = /^app-[a-f0-9]{32}$/u;
const domainIdPattern = /^dom_[a-f0-9]{32}$/u;
const contextIdPattern = /^[A-Za-z0-9_-]{3,160}$/u;

export class AppLaunchError extends Error {
  constructor(code) { super(code); this.name = 'AppLaunchError'; this.code = code; }
}
function requireValue(condition, code) { if (!condition) throw new AppLaunchError(code); }

/** A local HTTP target, never a return URL or a platform control endpoint. */
export function validateAppLaunchPath(value) {
  requireValue(typeof value === 'string' && value.length > 0 && value.length <= 8192
    && value.startsWith('/') && !value.startsWith('//') && !/[\\\u0000-\u0020\u007f]/u.test(value), 'invalid_app_path');
  let decoded;
  try { decoded = decodeURIComponent(value.split('?', 1)[0]); } catch { throw new AppLaunchError('invalid_app_path'); }
  requireValue(!decoded.startsWith('//') && !/[\\\u0000-\u001f\u007f]/u.test(decoded), 'invalid_app_path');
  const normalized = new URL(decoded, 'https://app.invalid').pathname;
  const resolved = decodeURIComponent(new URL(value, 'https://app.invalid').pathname);
  for (const path of [decoded, normalized, resolved]) requireValue(!path.startsWith('//') && path !== '/_soty' && !path.startsWith('/_soty/'), 'invalid_app_path');
  return value;
}

export function normalizeAppLaunchTarget(value) {
  requireValue(value && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).every(key => ['appId', 'domainId', 'path'].includes(key)), 'invalid_app_target');
  requireValue(typeof value.appId === 'string' && appIdPattern.test(value.appId), 'invalid_app_id');
  if (value.domainId !== undefined) requireValue(typeof value.domainId === 'string' && domainIdPattern.test(value.domainId), 'invalid_app_domain_id');
  return Object.freeze({ appId: value.appId, ...(value.domainId === undefined ? {} : { domainId: value.domainId }),
    ...(value.path === undefined ? {} : { path: validateAppLaunchPath(value.path) }) });
}

export function formatAppLaunchRoute(value, communityId) {
  const target = normalizeAppLaunchTarget(value);
  if (communityId !== undefined) requireValue(typeof communityId === 'string' && contextIdPattern.test(communityId) && !target.domainId, 'invalid_app_route');
  const base = target.domainId ? `launch/${target.appId}/${target.domainId}` : `app/${target.appId}${communityId ? `/${communityId}` : ''}`;
  return `${base}${target.path === undefined ? '' : `?${new URLSearchParams({ path: target.path })}`}`;
}

/** null means another screen; a malformed app link stays an app-link error. */
export function parseAppLaunchRoute(hash) {
  requireValue(typeof hash === 'string', 'invalid_app_route');
  const source = hash.startsWith('#') ? hash.slice(1) : hash;
  if (!/^(?:app|launch)(?:\/|\?|$)/u.test(source)) return null;
  requireValue(source.length <= 32768 && !source.includes('#'), 'invalid_app_route');
  const queryAt = source.indexOf('?'), parts = (queryAt < 0 ? source : source.slice(0, queryAt)).split('/');
  const parameters = new URLSearchParams(queryAt < 0 ? '' : source.slice(queryAt + 1));
  requireValue([...parameters.keys()].every(key => key === 'path') && parameters.getAll('path').length <= 1, 'invalid_app_route');
  requireValue(parts[0] === 'launch' ? parts.length === 3 : parts.length === 2 || parts.length === 3, 'invalid_app_route');
  let id, context;
  try { id = decodeURIComponent(parts[1]); context = parts[2] === undefined ? undefined : decodeURIComponent(parts[2]); }
  catch { throw new AppLaunchError('invalid_app_route'); }
  const target = normalizeAppLaunchTarget({ appId: id, ...(parts[0] === 'launch' ? { domainId: context } : {}),
    ...(parameters.has('path') ? { path: parameters.get('path') } : {}) });
  if (parts[0] === 'launch') requireValue(!!target.domainId, 'invalid_app_route');
  const communityId = parts[0] === 'app' ? context : undefined;
  return Object.freeze({ kind: parts[0], target, ...(communityId === undefined ? {} : { communityId }), route: formatAppLaunchRoute(target, communityId) });
}

/** The server owns the query string. Validate the boot envelope, not its payload. */
export function validateAppLaunchUrl(value, shellUrl) {
  requireValue(typeof value === 'string' && value.length <= 32768 && /^https?:\/\//u.test(value)
    && !/[\\\u0000-\u0020\u007f]/u.test(value), 'invalid_app_launch_url');
  let url, shell;
  try { url = new URL(value); shell = new URL(shellUrl); } catch { throw new AppLaunchError('invalid_app_launch_url'); }
  requireValue(!url.username && !url.password && url.origin !== shell.origin
    && (shell.protocol !== 'https:' || url.protocol === 'https:')
    && url.pathname === '/_soty/boot' && /^[A-Za-z0-9_-]{43}$/u.test(url.hash.slice(1)), 'invalid_app_launch_url');
  return url.href;
}

/** One screen owns its pending popup. Completed external tabs belong to the user. */
export function createAppLauncher({ target, accountId, shellUrl, isCurrent, request }) {
  requireValue(typeof accountId === 'string' && accountId.length > 0, 'app_account_required');
  const parameters = Object.freeze({ ...normalizeAppLaunchTarget(target), expectedAccountId: accountId });
  let disposed = false, popup = null, externalPending = false;
  const current = () => !disposed && isCurrent(accountId);
  const close = value => { try { value?.close(); } catch { /* A closed/isolated window is already out of our control. */ } };
  async function launch() {
    if (!current()) return null;
    try {
      const result = await request(parameters);
      if (!current()) return null;
      return validateAppLaunchUrl(result?.url, shellUrl);
    } catch (error) { if (!current()) return null; throw error; }
  }
  return {
    parameters,
    launch,
    isCurrent: current,
    async openExternal(openPopup) {
      if (!current()) return 'stale';
      if (externalPending) return 'busy';
      externalPending = true;
      let destination;
      try {
        destination = openPopup();
        if (!destination) return 'blocked';
        popup = destination; destination.opener = null;
        const url = await launch();
        if (!url || !current() || destination.closed) { close(destination); return 'stale'; }
        destination.location.replace(url);
        popup = null;
        return 'opened';
      } catch (error) {
        close(destination);
        if (!current()) return 'stale';
        throw error;
      } finally { popup = null; externalPending = false; }
    },
    dispose() { disposed = true; close(popup); popup = null; },
  };
}
