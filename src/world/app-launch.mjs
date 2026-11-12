const appIdPattern = /^app-[a-f0-9]{32}$/u;
const domainIdPattern = /^dom_[a-f0-9]{32}$/u;
const contextIdPattern = /^[A-Za-z0-9_-]{3,160}$/u;
const conversationIdPattern = /^conv_[a-f0-9]{32}$/u;
const messageIdPattern = /^msg_[a-f0-9]{32}$/u;

import {captureSourcePin} from './app-project-capture.mjs';

export const isScopedRuntimeProfile = value => value === 'soty.selected-human-embed.v1' || value === 'soty.selected-human-embed.v2';

// Public Source boot omits private navigation; all other paths still match exactly.
function matchesBootPath(publicPath, privatePath, runtimeProfile) {
  if (publicPath === privatePath) return true;
  if (!isScopedRuntimeProfile(runtimeProfile) || publicPath !== '/embed' || typeof privatePath !== 'string'
    || !privatePath.isWellFormed() || !privatePath.startsWith('/embed#')) return false;
  const fragment = privatePath.slice(7);
  if (fragment.length > 2048 || /[\\#\u0000-\u0020\u007f]/u.test(fragment)) return false;
  try { return !/[\\\u0000-\u001f\u007f]/u.test(decodeURIComponent(fragment)); }
  catch { return false; }
}

export class AppLaunchError extends Error {
  constructor(code) { super(code); this.name = 'AppLaunchError'; this.code = code; }
}
function requireValue(condition, code) { if (!condition) throw new AppLaunchError(code); }

/** Read-only server provenance, never a permission or a caller selector. */
export function validateAppLaunchBinding(value) {
  requireValue(value && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).sort().join(',') === 'bindingFloor,policyEpoch,profile,schema,targetDigest,targetRevision'
    && value.schema === 'soty.app-launch-binding.v1'
    && [value.policyEpoch, value.targetRevision].every(number => Number.isSafeInteger(number) && number > 0)
    && typeof value.targetDigest === 'string' && /^[a-f0-9]{64}$/u.test(value.targetDigest)
    && (value.profile === 'soty.relay-restricted.v1' && [1, 2].includes(value.bindingFloor)
      || value.profile === 'soty.relay-self-ip.v1' && value.bindingFloor === 2), 'invalid_app_launch_binding');
  return Object.freeze({ schema: value.schema, policyEpoch: value.policyEpoch, targetRevision: value.targetRevision,
    targetDigest: value.targetDigest, profile: value.profile, bindingFloor: value.bindingFloor });
}
export function sameAppLaunchBinding(first, next) {
  return !!first && !!next && ['schema', 'policyEpoch', 'targetRevision', 'targetDigest', 'profile', 'bindingFloor']
    .every(key => first[key] === next[key]);
}

/** Root-only continuity proof. It is never a Source credential or ready ACK. */
export function validateAppHostSession(value) {
  requireValue(value && typeof value === 'object' && !Array.isArray(value)
    && Object.keys(value).sort().join(',') === 'expiresAt,frameId,lease,revision,schema'
    && value.schema === 'soty.app-host-session.v1'
    && [value.frameId,value.lease].every(item => typeof item === 'string' && /^[A-Za-z0-9_-]{43}$/u.test(item))
    && [value.revision,value.expiresAt].every(item => Number.isSafeInteger(item) && item > 0), 'invalid_app_host_session');
  return Object.freeze({schema:value.schema,frameId:value.frameId,lease:value.lease,revision:value.revision,expiresAt:value.expiresAt});
}

/** A local HTTP target, never a return URL or a platform control endpoint. */
export function validateAppLaunchPath(value) {
  if (value === '/_soty/guide') return value; // One read-only platform page; every other reserved path stays denied.
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
export function validateAppEntry(value, target, shellUrl, launchUrl, runtimeProfile) {
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
      && matchesBootPath(boot.searchParams.get('path'), selected.path, runtimeProfile), 'invalid_app_entry');
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
    && Object.keys(value).every(key => ['panel', 'conversationId', 'messageId', 'administrative'].includes(key))
    && value.panel === 'discussion', 'invalid_app_route');
  if (value.conversationId !== undefined) requireValue(typeof value.conversationId === 'string' && conversationIdPattern.test(value.conversationId), 'invalid_app_route');
  if (value.messageId !== undefined) requireValue(value.conversationId !== undefined && typeof value.messageId === 'string' && messageIdPattern.test(value.messageId), 'invalid_app_route');
  if (value.administrative !== undefined) requireValue(value.administrative === true, 'invalid_app_route');
  return Object.freeze({ panel: 'discussion', ...(value.conversationId === undefined ? {} : { conversationId: value.conversationId }),
    ...(value.messageId === undefined ? {} : { messageId: value.messageId }),
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
    if (panel.messageId !== undefined) parameters.set('message', panel.messageId);
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
  requireValue([...parameters.keys()].every(key => ['path', 'panel', 'conversation', 'message', 'admin', 'invitation'].includes(key) && parameters.getAll(key).length === 1), 'invalid_app_route');
  requireValue(parameters.has('panel') || (!parameters.has('conversation') && !parameters.has('message') && !parameters.has('admin')), 'invalid_app_route');
  if (parameters.has('admin')) requireValue(parameters.get('admin') === '1', 'invalid_app_route');
  const presentation = parameters.has('panel') ? normalizePresentation({ panel: parameters.get('panel'),
    ...(parameters.has('conversation') ? { conversationId: parameters.get('conversation') } : {}),
    ...(parameters.has('message') ? { messageId: parameters.get('message') } : {}),
    ...(parameters.has('admin') ? { administrative: true } : {}) }) : undefined;
  requireValue(parts[0] === 'launch' ? parts.length === 3 : parts.length === 2 || parts.length === 3, 'invalid_app_route');
  let id, context;
  try { id = decodeURIComponent(parts[1]); context = parts[2] === undefined ? undefined : decodeURIComponent(parts[2]); }
  catch { throw new AppLaunchError('invalid_app_route'); }
  const target = normalizeAppLaunchTarget({ appId: id, ...(parts[0] === 'launch' ? { domainId: context } : {}),
    ...(parameters.has('path') ? { path: parameters.get('path') } : {}) });
  if (parts[0] === 'launch') requireValue(!!target.domainId, 'invalid_app_route');
  const communityId = parts[0] === 'app' ? context : undefined;
  let invitation = parameters.get('invitation');
  requireValue(invitation === null || /^[a-f0-9]{64}$/u.test(invitation) && !target.path && !presentation && parts[0] === 'app' && !context, 'invalid_app_invitation');
  return Object.freeze({ kind: parts[0], target, ...(invitation === null ? {} : { takeInvitation() { const token = invitation; invitation = null; return token; } }), ...(communityId === undefined ? {} : { communityId }),
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

/** A signed ordinary Root frame may use its own browser origin. The logical app
 * entry remains canonical; neither a frame message nor an offline read can
 * select this transport. The server still owns all session/Source authority. */
function ordinaryRuntimeOrigin(value, { instanceId, entry, url, shellUrl, scoped, provenance, hostSession }) {
  const invalid = 'invalid_app_runtime_origin';
  const widget=/^widget-[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}$/u.test(instanceId ?? '');
  requireValue(!scoped && provenance && (widget || instanceId==='main'&&hostSession), invalid);
  let runtime, canonical, boot, shell;
  try { runtime = new URL(value); canonical = new URL(entry.origin); boot = new URL(url); shell = new URL(shellUrl); }
  catch { throw new AppLaunchError(invalid); }
  const prefix = widget?'app-w'+instanceId.slice(7).replaceAll('-', '')+'-':'app-m'+entry.appId.slice(4)+'-';
  const label = runtime.hostname.split('.')[0], dot = canonical.hostname.indexOf('.');
  requireValue(typeof value === 'string' && value.length <= 512 && runtime.origin === value
    && runtime.origin !== canonical.origin && runtime.origin !== shell.origin
    && runtime.protocol === canonical.protocol && runtime.port === canonical.port
    && dot > 0 && label.startsWith(prefix) && /^[a-f0-9]{24}$/u.test(label.slice(prefix.length))
    && runtime.hostname.slice(label.length) === '.' + canonical.hostname.slice(dot + 1)
    && boot.origin === value && [...boot.searchParams.keys()].join(',') === 'path'
    && boot.searchParams.get('path') === entry.path, invalid);
  return value;
}

/** One screen owns its pending popup. Completed external tabs belong to the user. */
export function createAppLauncher({ target, accountId, instanceId, shellUrl, isCurrent, request, resolveEntry, resolveExternalEntry, abandonScoped, abandonHost }) {
  requireValue(typeof accountId === 'string' && accountId.length > 0, 'app_account_required');
  const requestedTarget = normalizeAppLaunchTarget(target);
  let selected = null, initializing = null, disposed = false, popup = null, externalPending = false;
  let scoped=null,frameCutover=null;
  function abandon(value) {if(!value)return;const action=value.cleanup??(typeof abandonScoped==='function'?()=>abandonScoped({appId:requestedTarget.appId,handle:value.handle}):null);if(action)void Promise.resolve(action()).catch(()=>{});}
  function abandonHostProof(value,cleanup){if(!value||value.frameId===hostSession?.hostSession.frameId)return;const action=typeof cleanup==='function'?cleanup:typeof abandonHost==='function'
    ?()=>abandonHost({appId:requestedTarget.appId,frameId:value.frameId,lease:value.lease,revision:value.revision}):null;
    if(action)void Promise.resolve().then(()=>action()).catch(()=>{});}
  let originalBinding = null, latestBinding = null, bindingPinned = false, acceptedRuntimeOrigin = null, hostSession = null;
  let sourceEntryChanged = false;
  const rootWorkspace = source => source?.id === 'soty.standard-workspace' && source.version === 2
    && source.digest === '23061fc95610e3011ec42b3ecf229308bce44b0a1268408ca9b6ee138c98f7e9';
  const parameters = () => Object.freeze({ ...(selected ? { appId: selected.appId, domainId: selected.domainId, path: selected.path } : requestedTarget), expectedAccountId: accountId, ...(instanceId === undefined ? {} : { instanceId }) });
  const current = () => !disposed && isCurrent(accountId);
  const close = value => { try { value?.close(); } catch { /* A closed/isolated window is already out of our control. */ } };
  const needsNativeEntry = () => typeof resolveExternalEntry === 'function' && (rootWorkspace(scoped?.source) || sourceEntryChanged);
  async function readNativeEntry() {
    const result = await resolveExternalEntry({ appId: requestedTarget.appId, expectedAccountId: accountId });
    if (!current()) return null;
    return result?.entry ? validateAppEntry(result.entry, { appId: requestedTarget.appId }, shellUrl) : null;
  }
  function capture(value, requested, url, binding, transport) {
    // A signed launch may replace the historical default only for the exact Root-SSO pin.
    // Offline resolution and every other deep link keep the strict entry contract.
    const savedRootEntry = requested.path === '/' && value?.path === '/embed'
      && binding?.profile === 'soty.selected-human-embed.v2' && binding.source?.id === 'soty.standard-workspace'
      && binding.source.version === 2 && binding.source.digest === '23061fc95610e3011ec42b3ecf229308bce44b0a1268408ca9b6ee138c98f7e9';
    const entry = validateAppEntry(value, savedRootEntry ? { ...requested, path: '/embed' } : requested, shellUrl,
      transport?.runtimeOrigin === undefined ? url : undefined, binding?.profile);
    const nextOrigin = transport?.runtimeOrigin === undefined ? entry.origin : ordinaryRuntimeOrigin(transport.runtimeOrigin,
      { instanceId, entry, url, shellUrl, scoped: binding, provenance: transport.provenance,hostSession:transport.hostSession });
    requireValue(!transport?.requireSameBinding || acceptedRuntimeOrigin === null || acceptedRuntimeOrigin === nextOrigin, 'app_runtime_origin_changed');
    requireValue(!selected || (selected.domainId === entry.domainId && selected.origin === entry.origin
      && (selected.path === entry.path || (savedRootEntry && selected.path === '/'))), 'invalid_app_entry');
    selected = entry;
    if (url !== undefined && !transport?.external) acceptedRuntimeOrigin = nextOrigin;
  }
  async function issue(initial,external=false,requireSameBinding=false) {
    const cutover=external?null:frameCutover;
    const scopedBefore=requireSameBinding&&!originalBinding?scoped:null;
    requireValue(!requireSameBinding||!!originalBinding||!!scopedBefore?.source,'app_launch_binding_required');
    const args = parameters(), intended = { appId: args.appId,
      ...(args.domainId === undefined ? {} : { domainId: args.domainId }), ...(args.path === undefined ? {} : { path: args.path }) };
    let received = false,binding=null,responseHost=null,responseHostCleanup=null;
    try {
      const result = await request(Object.freeze({...args,...(external?{}:{hostSession:true})})); received = true;
      if(result.runtimeProfile!==undefined||result.scopedCloseHandle!==undefined) {
        requireValue(isScopedRuntimeProfile(result.runtimeProfile)&&/^[A-Za-z0-9_-]{43}$/.test(result.scopedCloseHandle??''),'invalid_app_scoped_launch');
        requireValue(result.scopedCleanup===undefined||typeof result.scopedCleanup==='function','invalid_app_scoped_launch');
        const source=captureSourcePin(result.scopedSource);
        requireValue(result.scopedFrameId===undefined||typeof result.scopedFrameId==='string'&&/^[A-Za-z0-9_-]{43}$/.test(result.scopedFrameId)
          &&source?.id==='soty.standard-workspace'&&source.version===2&&source.digest==='23061fc95610e3011ec42b3ecf229308bce44b0a1268408ca9b6ee138c98f7e9','invalid_app_scoped_launch');
        binding=Object.freeze({profile:result.runtimeProfile,handle:result.scopedCloseHandle,cleanup:result.scopedCleanup,
          source,slot:Object.freeze({}),...(result.scopedFrameId?{frameId:result.scopedFrameId}:{})});
      }
      if(result.hostSession!==undefined)responseHost=validateAppHostSession(result.hostSession);
      if(typeof result.hostSessionCleanup==='function')responseHostCleanup=result.hostSessionCleanup;
      requireValue(!external||result.hostSession===undefined&&result.hostSessionExpiresAt===undefined,'invalid_app_host_session');
      if (!current()||cutover&&frameCutover!==cutover) {abandon(binding);abandonHostProof(responseHost,responseHostCleanup);return null;}
      const provenance=result?.launchBinding===undefined?null:validateAppLaunchBinding(result.launchBinding);
      requireValue(!requireSameBinding||(scopedBefore
        ?scoped===scopedBefore&&binding?.profile===scopedBefore.profile&&binding.source
          &&['id','version','digest'].every(key=>binding.source[key]===scopedBefore.source[key])
        :sameAppLaunchBinding(originalBinding,provenance)),'app_launch_source_changed');
      const url = validateAppLaunchUrl(result?.url, shellUrl);
      let host=null;
      if(result.hostSession!==undefined||result.hostSessionExpiresAt!==undefined){
        host=validateAppHostSession(result.hostSession);
        requireValue(!binding&&!!provenance&&Number.isSafeInteger(result.hostSessionExpiresAt)
          &&result.hostSessionExpiresAt>0&&result.hostSessionExpiresAt<=host.expiresAt,'invalid_app_host_session');
        requireValue(result.runtimeOrigin!==undefined,'invalid_app_runtime_origin');
        requireValue(result.hostSessionCleanup===undefined||typeof result.hostSessionCleanup==='function','invalid_app_host_session');
      }
      capture(result?.entry, intended, url, binding, {runtimeOrigin:result.runtimeOrigin,provenance,requireSameBinding,external,hostSession:host});
      if(!bindingPinned&&!external){originalBinding=provenance;bindingPinned=true;}
      if(!external)latestBinding=provenance;
      const nextHost=host?Object.freeze({hostSession:host,hostSessionExpiresAt:result.hostSessionExpiresAt,
        entry:selected,launchBinding:provenance,runtimeOrigin:acceptedRuntimeOrigin,slot:Object.freeze({}),
        ...(result.hostSessionCleanup?{cleanup:result.hostSessionCleanup}:{})}):null;
      if(!external){
        if(cutover){
          const unused=cutover.candidate;cutover.candidate={host:nextHost,scoped:binding};
          abandon(unused?.scoped);abandonHostProof(unused?.host?.hostSession,unused?.host?.cleanup);
        }else{hostSession=nextHost;const previous=scoped;scoped=binding;abandon(previous);}
      }
      return url;
    } catch (error) {
      if (['scoped_embed_source_changed', 'scoped_embed_authority_changed'].includes(error?.code)) sourceEntryChanged = true;
      abandon(binding);
      abandonHostProof(responseHost,responseHostCleanup);
      if (!current()||cutover&&frameCutover!==cutover) return null;
      // Only a failed admission may resolve an offline location. A successful
      // response with a missing/mismatched DTO must never be guessed later.
      if (initial && !received && typeof resolveEntry === 'function') {
        try {
          const { instanceId: _instanceId, ...entryArgs } = args;
          const result = await resolveEntry(entryArgs);
          if (current()) capture(result?.entry, intended);
        } catch { /* Keep the original launch failure; no substitute address. */ }
      }
      if (!current()) return null;
      throw error;
    }
  }
  async function launch({requireSameBinding=false,external=false}={}) {
    if (!current()) return null;
    if (initializing) {
      const outcome = await initializing;
      if (!current()) return null;
      if (!selected) throw outcome.error;
      return issue(false,external,requireSameBinding);
    }
    if (selected) return issue(false,external,requireSameBinding);
    let finish;
    initializing = new Promise(resolve => { finish = resolve; });
    let failure;
    try { return await issue(true,external,requireSameBinding); }
    catch (error) { failure = error; throw error; }
    finally { finish({ error: failure }); initializing = null; }
  }
  async function resolve() {
    if (!current()) return null;
    if (initializing) {
      const outcome = await initializing;
      if (!current()) return null;
      if (!selected) throw outcome.error;
      return selected;
    }
    if (selected) return selected;
    requireValue(typeof resolveEntry === 'function', 'invalid_app_entry');
    let finish, failure;
    initializing = new Promise(done => { finish = done; });
    try {
      const { instanceId: _instanceId, ...args } = parameters();
      const result = await resolveEntry(args);
      if (!current()) return null;
      capture(result?.entry, requestedTarget);
      return selected;
    } catch (error) { failure = error; throw error; }
    finally { finish({ error: failure }); initializing = null; }
  }
  return {
    get parameters() { return parameters(); },
    entry: () => current() ? selected : null,
    runtimeOrigin: () => current() ? acceptedRuntimeOrigin ?? selected?.origin ?? null : null,
    hostSession:()=>current()?hostSession:null,
    /** The frame owner commits only after its old document has actually left. */
    beginFrameCutover(){
      requireValue(current()&&!frameCutover,'app_runtime_cutover_pending');
      const previous={selected,acceptedRuntimeOrigin,originalBinding,latestBinding,bindingPinned};
      const operation={candidate:null};frameCutover=operation;
      return Object.freeze({
        commit(){
          if(!current()||frameCutover!==operation||!operation.candidate)return false;
          const oldHost=hostSession,oldScoped=scoped;
          hostSession=operation.candidate.host;scoped=operation.candidate.scoped;frameCutover=null;
          abandon(oldScoped);abandonHostProof(oldHost?.hostSession,oldHost?.cleanup);return true;
        },
        discard(){
          if(frameCutover!==operation)return false;frameCutover=null;
          if(operation.candidate){
            ({selected,acceptedRuntimeOrigin,originalBinding,latestBinding,bindingPinned}=previous);
            abandon(operation.candidate.scoped);abandonHostProof(operation.candidate.host?.hostSession,operation.candidate.host?.cleanup);
          }
          return true;
        },
      });
    },
    commitHostSession(previous,next){
      if(!current()||hostSession!==previous)return false;
      const session=validateAppHostSession(next.hostSession);
      const sameProof=session.lease===previous.hostSession.lease&&session.revision===previous.hostSession.revision;
      requireValue(session.frameId===previous.hostSession.frameId&&(sameProof?session.expiresAt>=previous.hostSession.expiresAt
        :session.revision===previous.hostSession.revision+1&&session.lease!==previous.hostSession.lease)
        &&sameAppLaunchBinding(previous.launchBinding,next.launchBinding)
        &&['appId','domainId','origin','path'].every(key=>next.entry?.[key]===previous.entry[key])
        &&(next.runtimeOrigin??next.entry.origin)===previous.runtimeOrigin
        &&(next.hostSessionExpiresAt===null||Number.isSafeInteger(next.hostSessionExpiresAt)
          &&next.hostSessionExpiresAt>0&&next.hostSessionExpiresAt<=session.expiresAt),'app_host_session_changed');
      requireValue(next.cleanup===undefined||typeof next.cleanup==='function','invalid_app_host_session');
      hostSession=Object.freeze({...previous,hostSession:session,hostSessionExpiresAt:next.hostSessionExpiresAt,...(next.cleanup?{cleanup:next.cleanup}:{})});return true;
    },
    binding: () => current() ? originalBinding : null,
    latestBinding: () => current() ? latestBinding : null,
    launch, resolve,
    isCurrent: current,
    runtimeProfile:()=>frameCutover?.candidate?frameCutover.candidate.scoped?.profile??null:scoped?.profile??null,
    scopedCapture:()=>current()&&scoped?Object.freeze({handle:scoped.handle,source:scoped.source,slot:scoped.slot,...(scoped.frameId?{frameId:scoped.frameId,entry:selected}:{})}):null,
    // Presentation only. Opening still performs its own fresh signed read.
    async canOpenExternal() { return current() && (!needsNativeEntry() || !!await readNativeEntry()); },
    prepareRenewal(result){requireValue(current()&&scoped&&selected,'invalid_app_scoped_launch');
      requireValue(result.runtimeProfile===scoped.profile&&/^[A-Za-z0-9_-]{43}$/.test(result.scopedCloseHandle??''),'invalid_app_scoped_launch');
      requireValue(result.scopedFrameId===scoped.frameId,'invalid_app_scoped_launch');
      const source=captureSourcePin(result.scopedSource);requireValue(source&&scoped.source&&source.id===scoped.source.id&&source.version===scoped.source.version
        &&source.digest===scoped.source.digest,'invalid_app_scoped_launch');
      const url=validateAppLaunchUrl(result.url,shellUrl),boot=new URL(url);
      requireValue(boot.origin===selected.origin&&[...boot.searchParams.keys()].join(',')==='path,mode'
        &&matchesBootPath(boot.searchParams.get('path'),selected.path,scoped.profile)&&boot.searchParams.get('mode')==='renew','invalid_app_scoped_launch');
      const entry=validateAppEntry(result.entry,{appId:selected.appId,domainId:selected.domainId,path:selected.path},shellUrl);
      requireValue(entry.origin===selected.origin&&Number.isSafeInteger(result.scopedSlotExpiresAt)&&result.scopedSlotExpiresAt>Date.now()
        &&result.scopedSlotExpiresAt<=Date.now()+310000&&typeof result.scopedRenewalRequestId==='string'&&/^[A-Za-z0-9_-]{16,128}$/.test(result.scopedRenewalRequestId),'invalid_app_scoped_launch');
      requireValue(result.scopedRenewalBindingDigest===undefined||/^[a-f0-9]{64}$/.test(result.scopedRenewalBindingDigest),'invalid_app_scoped_launch');
      return Object.freeze({url,handle:result.scopedCloseHandle,requestId:result.scopedRenewalRequestId,source,expiresAt:result.scopedSlotExpiresAt,cleanup:result.scopedCleanup,
        ...(scoped.frameId?{frameId:scoped.frameId}:{}),...(result.scopedRenewalBindingDigest?{bindingDigest:result.scopedRenewalBindingDigest}:{}),slot:Object.freeze({})});},
    commitRenewal(previous,next){if(!current()||scoped?.slot!==previous.slot)return false;const old=scoped;
      scoped=Object.freeze({profile:old.profile,handle:next.handle,source:next.source,cleanup:next.cleanup,slot:next.slot,...(next.frameId?{frameId:next.frameId}:{})});abandon(old);return true;},
    async openExternal(openPopup) {
      if (!current()) return 'stale';
      if (externalPending) return 'busy';
      externalPending = true;
      let destination;
      try {
        destination = openPopup();
        if (!destination) return 'blocked';
        popup = destination; destination.opener = null;
        if (needsNativeEntry()) {
          const native = await readNativeEntry();
          if (!current() || destination.closed) { close(destination); return 'stale'; }
          if (!native) { close(destination); return 'unavailable'; }
          // An explicit Native bookmark keeps its exact path. A Source entry
          // opens the separately admitted Native start page, without a Source
          // ticket, frame credential or silent change to the saved bookmark.
          const path = selected?.domainId === native.domainId && selected.origin === native.origin ? selected.path : native.path;
          destination.location.replace(native.origin + path); popup = null; return 'opened';
        }
        const url = await launch({external:true});
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
    dispose() {if(disposed)return;const pending=frameCutover?.candidate;frameCutover=null;abandon(pending?.scoped);abandonHostProof(pending?.host?.hostSession,pending?.host?.cleanup);disposed = true; selected = null;const host=hostSession;hostSession=null;abandonHostProof(host?.hostSession,host?.cleanup);close(popup); popup = null;const previous=scoped;scoped=null;abandon(previous); },
  };
}
