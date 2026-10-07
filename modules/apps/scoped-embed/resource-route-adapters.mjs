import { capture, hash, need } from './profile.mjs';
import { selectedResourceProfile } from './resource-profile.mjs';
import { STANDARD_SOURCE_CONTRACT, STANDARD_SELECTED_SOURCE, STANDARD_SOURCE_CONTRACT_2, STANDARD_SELECTED_SOURCE_V2 } from '../../source-app/server/standard-profile.mjs';
export { STANDARD_SELECTED_SOURCE, STANDARD_SELECTED_SOURCE_V2 };

// This is host code, not author-supplied routes. Future reviewed Source adapters
// add a handler here without another Apps database format or broader v1 routes.
const MiB = 1048576;
const rule = (kind, methods, path, requestBytes, responseBytes, queries = []) =>
  Object.freeze({ kind, methods: Object.freeze(methods), path, requestBytes, responseBytes, queries: Object.freeze(queries) });
const hiveContract = capture({ schema: 'soty.source-route-adapter.v1', kind: 'hive.project.v1',
  auth: { schema: 'soty.source-embed-auth.v1', cookies: ['soty_rp_session', 'soty_rp_intent', 'soty_rp_link'],
    basicSeconds: 300, finiteSeconds: 86400, proof: 'maintained-native-rp-current-userinfo', nativeConsent: 'selected-current-native-acl',
    nativeHandoffPath: '/soty/connect', nativeCallbackPath: '/account/soty/callback',feedbackPermission:'explicit-current-native-report' },
  rules: [
    rule('public-ui', ['GET', 'HEAD'], '/embed', 0, 4 * MiB),
    rule('auth-start', ['GET', 'POST'], '/api/embed/login', 16384, 65536),
    rule('auth-read', ['GET'], '/api/embed/session-status', 0, 65536),
    rule('auth-continue', ['POST'], '/api/embed/session-continue', 16384, 65536),
    rule('auth-callback', ['GET'], '/api/embed/callback', 0, 65536, ['code', 'state', 'iss', 'error', 'error_description']),
    rule('auth-completion', ['GET'], '/api/embed/complete-link', 0, 65536, ['intent']),
    rule('read', ['GET'], '/api/embed/context', 0, 65536),
    rule('read', ['GET'], '/api/embed/project', 0, 4 * MiB),
    rule('write', ['PUT'], '/api/embed/project', MiB, 4 * MiB),
    rule('write', ['POST'], '/api/embed/operations', MiB, 4 * MiB),
    rule('read', ['GET'], '/api/embed/changes', 0, MiB, ['after', 'limit']),
    rule('read', ['GET'], '/api/embed/live', 0, 65536, ['clientId']),
    rule('read', ['GET'], '/api/embed/presence', 0, 65536, ['clientId']),
    rule('presence', ['POST', 'DELETE'], '/api/embed/presence', 16384, 65536),
    rule('feedback-read', ['GET'], '/api/embed/feedback/context', 0, 65536),
    rule('feedback-read', ['GET'], '/api/embed/feedback', 0, 65536, ['limit', 'cursor']),
    rule('feedback-write', ['POST'], '/api/embed/feedback', 1500000, 262144),
    rule('feedback-read', ['GET'], '/api/embed/feedback/ticket', 0, 2 * MiB, ['ticketId']),
    rule('feedback-write', ['POST'], '/api/embed/feedback/reply', 32768, 262144),
    rule('feedback-write', ['POST'], '/api/embed/feedback/status', 32768, 262144),
    rule('feedback-write', ['POST'], '/api/embed/feedback/accept', 32768, 262144),
  ], assets: { path: '/assets/', extensions: ['js', 'css', 'woff2', 'svg', 'png'], responseBytes: 4 * MiB } });

// Version1 is the accepted kernel contract. Never expand an existing pin when
// a later Native UI build needs additional paths.
export const HIVE_SELECTED_KERNEL_SOURCE = Object.freeze({ id: 'hive.selected-project', version: 1, digest: hash(hiveContract) });
const editorContract=capture({...hiveContract,assets:{responseBytes:4*MiB,credentialFree:true,redirects:false,queries:false,
  paths:[{prefix:'/_next/static/chunks/',extensions:['js']},{prefix:'/_next/static/css/',extensions:['css']},
    {prefix:'/_next/static/media/',extensions:['woff2','svg','png']}]}});
export const HIVE_SELECTED_SOURCE = Object.freeze({ id: 'hive.selected-project', version: 2, digest: hash(editorContract) });
const hive = Object.freeze({ pin: HIVE_SELECTED_KERNEL_SOURCE, kind: hiveContract.kind, contract: hiveContract });
const editor=Object.freeze({pin:HIVE_SELECTED_SOURCE,kind:editorContract.kind,contract:editorContract});
const adapters = new Map([hive,editor].map(adapter=>[adapter.pin.id+':'+adapter.pin.version+':'+adapter.pin.digest,adapter]));
adapters.set(STANDARD_SELECTED_SOURCE.id + ':1:' + STANDARD_SELECTED_SOURCE.digest,
  Object.freeze({ pin: STANDARD_SELECTED_SOURCE, kind: STANDARD_SOURCE_CONTRACT.kind, contract: STANDARD_SOURCE_CONTRACT }));
adapters.set(STANDARD_SELECTED_SOURCE_V2.id + ':2:' + STANDARD_SELECTED_SOURCE_V2.digest,
  Object.freeze({ pin: STANDARD_SELECTED_SOURCE_V2, kind: STANDARD_SOURCE_CONTRACT_2.kind, contract: STANDARD_SOURCE_CONTRACT_2 }));
export function selectedRouteAdapter(input) {
  const profile = selectedResourceProfile(input);
  const pin = profile.sourceProfile, adapter = adapters.get(pin.id + ':' + pin.version + ':' + pin.digest);
  need(adapter && adapter.kind === profile.resource.selection.kind, 'scoped_embed_adapter_unapproved', 403);
  return adapter;
}

/** Exact OAuth callback from reviewed host code. The private registry supplies
 * origins/pins, never an arbitrary callback path or a redirect exception. */
export function selectedClientRedirect(input) {
  const profile = selectedResourceProfile(input), adapter = selectedRouteAdapter(profile);
  const auth = adapter.contract.auth;
  need(auth.callbackOrigin === undefined || auth.callbackOrigin === 'embed', 'scoped_embed_adapter_unapproved', 403);
  return (auth.callbackOrigin === 'embed' ? profile.embedOrigin : profile.nativeOrigin) + auth.nativeCallbackPath;
}

/** HIVE's existing Native RP callback stays exact; a handoff is only a UI
 * continuation to native consent, never a login token or permission receipt. */
export function selectedNativeHandoff(input, value) {
  const profile = selectedResourceProfile(input), adapter = selectedRouteAdapter(profile);
  need(typeof value === 'string' && value.length <= 2048, 'scoped_embed_redirect_denied', 502);
  const url = new URL(value);
  need(url.href === value && url.origin === profile.nativeOrigin && url.pathname === adapter.contract.auth.nativeHandoffPath
    && !url.username && !url.password && !url.hash && [...url.searchParams.keys()].join(',') === 'intent'
    && /^[A-Za-z0-9_-]{43}$/u.test(url.searchParams.get('intent') ?? ''), 'scoped_embed_redirect_denied', 502);
  return url.href;
}

/** Closed source routes. Native ID never becomes a path/query-selected resource. */
export function selectedRoute(input, method, path) {
  const adapter = selectedRouteAdapter(input);
  need(typeof method === 'string' && typeof path === 'string' && path.length <= 8192
    && path.startsWith('/') && !path.startsWith('//') && !/[\\\s\u0000-\u001f\u007f]/u.test(path),
    'scoped_embed_route_denied', 403);
  const url = new URL(path, 'https://fixed.invalid');
  need(url.pathname + url.search === path && !url.hash && !url.pathname.includes('%') && !url.pathname.includes('..'),
    'scoped_embed_route_denied', 403);
  if (adapter===hive && ['GET', 'HEAD'].includes(method) && !url.search
    && /^\/assets\/[A-Za-z0-9_.-]+\.(?:js|css|woff2|svg|png)$/u.test(url.pathname))
    return Object.freeze({ kind: 'public-ui', requestBytes: 0, responseBytes: adapter.contract.assets.responseBytes });
  if(adapter===editor&&['GET','HEAD'].includes(method)&&!url.search){
    for(const asset of adapter.contract.assets.paths){if(!url.pathname.startsWith(asset.prefix))continue;
      const leaf=url.pathname.slice(asset.prefix.length),match=/^[A-Za-z0-9_-][A-Za-z0-9_.-]{0,239}\.([a-z0-9]+)$/u.exec(leaf);
      if(match&&asset.extensions.includes(match[1]))return Object.freeze({kind:'public-ui',requestBytes:0,responseBytes:adapter.contract.assets.responseBytes,credentialFree:true,redirects:false});
    }
  }
  const route = adapter.contract.rules.find(value => value.path === url.pathname && value.methods.includes(method));
  need(route, 'scoped_embed_route_denied', 403);
  const entries = [...url.searchParams.entries()];
  need(entries.length <= route.queries.length && entries.every(([key, value]) => route.queries.includes(key)
    && url.searchParams.getAll(key).length === 1 && value.length <= 4096 && value.isWellFormed()
    && !/[\u0000-\u001f\u007f-\u009f]/u.test(value)), 'scoped_embed_route_denied', 403);
  return route;
}
