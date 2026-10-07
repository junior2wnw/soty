const appIdPattern = /^app-[a-f0-9]{32}$/u;
const domainIdPattern = /^dom_[a-f0-9]{32}$/u;
const contextIdPattern = /^[A-Za-z0-9_-]{3,160}$/u;
const conversationIdPattern = /^conv_[a-f0-9]{32}$/u;

export class AppLaunchError extends Error {
  constructor(code) { super(code); this.name = 'AppLaunchError'; this.code = code; }
}
function requireValue(condition, code) { if (!condition) throw new AppLaunchError(code); }

/** A local HTTP target, never a return URL or a platform control endpoint. */
export function validateAppLaunchPath(value) {
  requireValue(typeof value === 'string' && value.length > 0 && value.length <= 8192
    && value.isWellFormed()
    && value.startsWith('/') && !value.startsWith('//') && !/[\\\u0000-\u0020\u007f]/u.test(value), 'invalid_app_path');
  let decoded;
  try { decoded = decodeURIComponent(value.split('?', 1)[0]); } catch { throw new AppLaunchError('invalid_app_path'); }
  requireValue(!decoded.startsWith('//') && !/[\\\u0000-\u001f\u007f]/u.test(decoded), 'invalid_app_path');
  const normalized = new URL(decoded, 'https://app.invalid').pathname;
  const resolved = decodeURIComponent(new URL(value, 'https://app.invalid').pathname);
  for (const path of [decoded, normalized, resolved]) requireValue(!path.startsWith('//') && path !== '/_soty' && !path.startsWith('/_soty/'), 'invalid_app_path');
  return value;
}

/** A server-resolved address. It is a location, never a reusable permission. */
export function validateAppEntry(value, target, shellUrl, launchUrl) {
  const requested = normalizeAppLaunchTarget(target);
  requireValue(value && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).sort().join(',') === 'appId,domainId,origin,path', 'invalid_app_entry');
  const selected = normalizeAppLaunchTarget({ appId: value.appId, domainId: value.domainId, path: value.path });
  requireValue(selected.appId === requested.appId && selected.domainId !== undefined && selected.path !== undefined
    && (requested.domainId === undefined || requested.domainId === selected.domainId)
    && (requested.path === undefined || requested.path === selected.path), 'invalid_app_entry');
  let origin, shell;
  try { origin = new URL(value.origin); shell = new URL(shellUrl); } catch { throw new AppLaunchError('invalid_app_entry'); }
  requireValue(typeof value.origin === 'string' && value.origin.length <= 512 && origin.origin === value.origin
    && ['http:', 'https:'].includes(origin.protocol) && !origin.username && !origin.password
    && origin.origin !== shell.origin && (shell.protocol !== 'https:' || origin.protocol === 'https:'), 'invalid_app_entry');
  if (launchUrl !== undefined) {
    const boot = new URL(validateAppLaunchUrl(launchUrl, shellUrl));
    requireValue(boot.origin === value.origin && [...boot.searchParams.keys()].join(',') === 'path'
      && boot.searchParams.get('path') === selected.path, 'invalid_app_entry');
  }
  return Object.freeze({ appId: selected.appId, domainId: selected.domainId, origin: value.origin, path: selected.path });
}

export function normalizeAppLaunchTarget(value) {
  requireValue(value && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).every(key => ['appId', 'domainId', 'path'].includes(key)), 'invalid_app_target');
  requireValue(typeof value.appId === 'string' && appIdPattern.test(value.appId), 'invalid_app_id');
  if (value.domainId !== undefined) requireValue(typeof value.domainId === 'string' && domainIdPattern.test(value.domainId), 'invalid_app_domain_id');
  return Object.freeze({ appId: value.appId, ...(value.domainId === undefined ? {} : { domainId: value.domainId }),
    ...(value.path === undefined ? {} : { path: validateAppLaunchPath(value.path) }) });
}

function normalizePresentation(value) {
  if (value === undefined) return undefined;
  requireValue(value && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).every(key => ['panel', 'conversationId', 'administrative'].includes(key))
    && value.panel === 'discussion', 'invalid_app_route');
  if (value.conversationId !== undefined) requireValue(typeof value.conversationId === 'string' && conversationIdPattern.test(value.conversationId), 'invalid_app_route');
  if (value.administrative !== undefined) requireValue(value.administrative === true, 'invalid_app_route');
  return Object.freeze({ panel: 'discussion', ...(value.conversationId === undefined ? {} : { conversationId: value.conversationId }),
    ...(value.administrative === undefined ? {} : { administrative: true }) });
}

export function formatAppLaunchRoute(value, communityId, presentation) {
  const target = normalizeAppLaunchTarget(value);
  if (communityId !== undefined) requireValue(typeof communityId === 'string' && contextIdPattern.test(communityId) && !target.domainId, 'invalid_app_route');
  const base = target.domainId ? `launch/${target.appId}/${target.domainId}` : `app/${target.appId}${communityId ? `/${communityId}` : ''}`;
  const parameters = new URLSearchParams(), panel = normalizePresentation(presentation);
  if (target.path !== undefined) parameters.set('path', target.path);
  if (panel) {
    parameters.set('panel', 'discussion');
    if (panel.conversationId !== undefined) parameters.set('conversation', panel.conversationId);
    if (panel.administrative) parameters.set('admin', '1');
  }
  return `${base}${parameters.size ? `?${parameters}` : ''}`;
}

/** null means another screen; a malformed app link stays an app-link error. */
export function parseAppLaunchRoute(hash) {
  requireValue(typeof hash === 'string', 'invalid_app_route');
  const source = hash.startsWith('#') ? hash.slice(1) : hash;
  if (!/^(?:app|launch)(?:\/|\?|$)/u.test(source)) return null;
  requireValue(source.length <= 32768 && !source.includes('#'), 'invalid_app_route');
  const queryAt = source.indexOf('?'), parts = (queryAt < 0 ? source : source.slice(0, queryAt)).split('/');
  const parameters = new URLSearchParams(queryAt < 0 ? '' : source.slice(queryAt + 1));
  requireValue([...parameters.keys()].every(key => ['path', 'panel', 'conversation', 'admin'].includes(key) && parameters.getAll(key).length === 1), 'invalid_app_route');
  requireValue(parameters.has('panel') || (!parameters.has('conversation') && !parameters.has('admin')), 'invalid_app_route');
  if (parameters.has('admin')) requireValue(parameters.get('admin') === '1', 'invalid_app_route');
  const presentation = parameters.has('panel') ? normalizePresentation({ panel: parameters.get('panel'),
    ...(parameters.has('conversation') ? { conversationId: parameters.get('conversation') } : {}),
    ...(parameters.has('admin') ? { administrative: true } : {}) }) : undefined;
  requireValue(parts[0] === 'launch' ? parts.length === 3 : parts.length === 2 || parts.length === 3, 'invalid_app_route');
  let id, context;
  try { id = decodeURIComponent(parts[1]); context = parts[2] === undefined ? undefined : decodeURIComponent(parts[2]); }
  catch { throw new AppLaunchError('invalid_app_route'); }
  const target = normalizeAppLaunchTarget({ appId: id, ...(parts[0] === 'launch' ? { domainId: context } : {}),
    ...(parameters.has('path') ? { path: parameters.get('path') } : {}) });
  if (parts[0] === 'launch') requireValue(!!target.domainId, 'invalid_app_route');
  const communityId = parts[0] === 'app' ? context : undefined;
  return Object.freeze({ kind: parts[0], target, ...(communityId === undefined ? {} : { communityId }),
    ...(presentation === undefined ? {} : { presentation }), route: formatAppLaunchRoute(target, communityId, presentation) });
}

/** Panel navigation is not a new runtime. An omitted path keeps this screen's
 * already resolved initial location; a different explicit path is a new entry. */
export function sameAppLaunchLocation(first, next, resolved) {
  if (!first || !next || first.target.appId !== next.target.appId || first.communityId !== next.communityId) return false;
  const domainMatches = first.target.domainId === next.target.domainId
    || (resolved && next.target.domainId === resolved.domainId);
  if (!domainMatches) return false;
  const initialPath = resolved?.path ?? first.target.path;
  return next.target.path === undefined || next.target.path === initialPath;
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
export function createAppLauncher({ target, accountId, shellUrl, isCurrent, request, resolveEntry, abandonScoped }) {
  requireValue(typeof accountId === 'string' && accountId.length > 0, 'app_account_required');
  const requestedTarget = normalizeAppLaunchTarget(target);
  let selected = null, initializing = null, disposed = false, popup = null, externalPending = false;
  let scoped=null;
  function abandon(value) {if(!value)return;const action=value.cleanup??(typeof abandonScoped==='function'?()=>abandonScoped({appId:requestedTarget.appId,handle:value.handle}):null);if(action)void Promise.resolve(action()).catch(()=>{});}
  const parameters = () => Object.freeze({ ...(selected ? { appId: selected.appId, domainId: selected.domainId, path: selected.path } : requestedTarget), expectedAccountId: accountId });
  const current = () => !disposed && isCurrent(accountId);
  const close = value => { try { value?.close(); } catch { /* A closed/isolated window is already out of our control. */ } };
  function capture(value, requested, url) {
    const entry = validateAppEntry(value, requested, shellUrl, url);
    requireValue(!selected || (selected.domainId === entry.domainId && selected.origin === entry.origin && selected.path === entry.path), 'invalid_app_entry');
    selected = entry;
  }
  async function issue(initial, external = false) {
    const args = parameters(), intended = { appId: args.appId,
      ...(args.domainId === undefined ? {} : { domainId: args.domainId }), ...(args.path === undefined ? {} : { path: args.path }) };
    let received = false,binding=null;
    try {
      const result = await request(args); received = true;
      if(result.runtimeProfile!==undefined||result.scopedCloseHandle!==undefined) {
        requireValue(result.runtimeProfile==='soty.selected-human-embed.v1'&&/^[A-Za-z0-9_-]{43}$/.test(result.scopedCloseHandle??''),'invalid_app_scoped_launch');
        requireValue(result.scopedCleanup===undefined||typeof result.scopedCleanup==='function','invalid_app_scoped_launch');
        binding=Object.freeze({profile:result.runtimeProfile,handle:result.scopedCloseHandle,cleanup:result.scopedCleanup});
      }
      if (!current()) {abandon(binding);return null;}
      const url = validateAppLaunchUrl(result?.url, shellUrl);
      capture(result?.entry, intended, url);
      if(!external){const previous=scoped;scoped=binding;abandon(previous);}
      return url;
    } catch (error) {
      abandon(binding);
      if (!current()) return null;
      // Only a failed admission may resolve an offline location. A successful
      // response with a missing/mismatched DTO must never be guessed later.
      if (initial && !received && typeof resolveEntry === 'function') {
        try {
          const result = await resolveEntry(args);
          if (current()) capture(result?.entry, intended);
        } catch { /* Keep the original launch failure; no substitute address. */ }
      }
      if (!current()) return null;
      throw error;
    }
  }
  async function launch(external = false) {
    if (!current()) return null;
    if (initializing) {
      const outcome = await initializing;
      if (!current()) return null;
      if (!selected) throw outcome.error;
      return issue(false,external);
    }
    if (selected) return issue(false,external);
    let finish;
    initializing = new Promise(resolve => { finish = resolve; });
    let failure;
    try { return await issue(true,external); }
    catch (error) { failure = error; throw error; }
    finally { finish({ error: failure }); initializing = null; }
  }
  return {
    get parameters() { return parameters(); },
    entry: () => current() ? selected : null,
    launch,
    isCurrent: current,
    runtimeProfile:()=>scoped?.profile??null,
    async openExternal(openPopup) {
      if (!current()) return 'stale';
      if (externalPending) return 'busy';
      externalPending = true;
      let destination;
      try {
        destination = openPopup();
        if (!destination) return 'blocked';
        popup = destination; destination.opener = null;
        const url = await launch(true);
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
    dispose() { disposed = true; selected = null; close(popup); popup = null;const previous=scoped;scoped=null;abandon(previous); },
  };
}
