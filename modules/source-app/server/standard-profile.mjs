import { deepFreeze, digest, fields, check } from './wire.mjs';

const rule = (kind, methods, path, requestBytes, responseBytes, queries = []) => ({ kind, methods, path, requestBytes, responseBytes, queries });
/** ONE compiled adapter for apps implementing this fixed protocol. It grants no
 * Native permission and loads no author route/code/commands into Root. */
export const STANDARD_SOURCE_CONTRACT = deepFreeze({ schema: 'soty.source-route-adapter.v1', kind: 'soty.resource.v1',
  auth: { schema: 'soty.source-embed-auth.v1', cookies: ['soty_rp_session', 'soty_rp_intent', 'soty_rp_link'],
    basicSeconds: 300, finiteSeconds: 86400, proof: 'maintained-native-rp-current-userinfo', nativeConsent: 'selected-current-native-acl',
    nativeHandoffPath: '/soty/connect', nativeCallbackPath: '/soty/callback', feedbackPermission: 'explicit-current-native-report' },
  rules: [
    rule('public-ui', ['GET', 'HEAD'], '/embed', 0, 4194304),
    rule('auth-start', ['GET', 'POST'], '/api/embed/login', 16384, 65536),
    rule('auth-read', ['GET'], '/api/embed/session-status', 0, 65536),
    rule('auth-continue', ['POST'], '/api/embed/session-continue', 16384, 65536),
    rule('auth-callback', ['GET'], '/api/embed/callback', 0, 65536, ['code', 'state', 'iss', 'error', 'error_description']),
    rule('auth-completion', ['GET'], '/api/embed/complete-link', 0, 65536, ['intent']),
    rule('read', ['GET'], '/api/embed/context', 0, 65536),
    rule('read', ['POST'], '/api/embed/query', 65536, 65536),
    rule('write', ['POST'], '/api/embed/invoke', 65536, 65536),
    rule('read', ['POST'], '/api/embed/receipt', 16384, 65536),
    rule('feedback-read', ['GET'], '/api/embed/feedback/context', 0, 65536),
    rule('feedback-read', ['GET'], '/api/embed/feedback', 0, 65536, ['limit', 'cursor']),
    rule('feedback-write', ['POST'], '/api/embed/feedback', 1500000, 262144),
    rule('feedback-read', ['GET'], '/api/embed/feedback/ticket', 0, 1500000, ['ticketId']),
    rule('feedback-write', ['POST'], '/api/embed/feedback/reply', 32768, 262144),
    rule('feedback-write', ['POST'], '/api/embed/feedback/status', 32768, 262144),
    rule('feedback-write', ['POST'], '/api/embed/feedback/accept', 32768, 262144),
  ], assets: { path: '/assets/', extensions: ['js', 'css', 'woff2', 'svg', 'png'], responseBytes: 4194304 } });
export const STANDARD_SELECTED_SOURCE = deepFreeze({ id: 'soty.standard-resource', version: 1, digest: digest(STANDARD_SOURCE_CONTRACT) });

/** Additive orchestration contract. Profile1 remains byte/semantic immutable.
 * Native proof is captured before OIDC; HTTPS embed callback consumes actual
 * maintained code proof under Source-owned durable state/Native authority. */
const { nativeCallbackPath: _nativePath, ...authV2 } = STANDARD_SOURCE_CONTRACT.auth;
export const STANDARD_SOURCE_CONTRACT_2 = deepFreeze({ ...STANDARD_SOURCE_CONTRACT,
  auth: { ...authV2,
    callbackPath: '/api/embed/callback', callbackOrigin: 'approved-embed-https',
    loginProof: 'durable-source-native-before-oidc.v1', correlation: 'handoff-state-original-slot.v1',
    completionRecovery: 'current-original-slot-readonly-receipt.v1' },
});
export const STANDARD_SELECTED_SOURCE_V2 = deepFreeze({ id: 'soty.standard-resource', version: 2, digest: digest(STANDARD_SOURCE_CONTRACT_2) });

/** Additive wire budget only. A lawful 1MiB attachment plus the permitted
 * conversation fits this detail response; uploads and Native rights stay fixed. */
export const STANDARD_SOURCE_CONTRACT_3 = deepFreeze({ ...STANDARD_SOURCE_CONTRACT_2,
  rules: STANDARD_SOURCE_CONTRACT_2.rules.map(row => row.path === '/api/embed/feedback/ticket'
    ? { ...row, responseBytes: 2097152 } : row),
});
export const STANDARD_SELECTED_SOURCE_V3 = deepFreeze({ id: 'soty.standard-resource', version: 3, digest: digest(STANDARD_SOURCE_CONTRACT_3) });

// A distinct consent/authority anchor, not a widening of any resource1 grant.
// The HTTP, RP, assets and budgets are exactly the maintained Source3 transport.
export const STANDARD_WORKSPACE_CONTRACT = deepFreeze({ ...STANDARD_SOURCE_CONTRACT_3, kind: 'soty.workspace.v1',
  context: { schema: 'soty.current-context.v2', authority: 'application-namespace-and-current-child-native-acl',
    readOperation: 'soty.current_context', uiOperation: 'soty.context.select', maxLeaseMs: 300000, slotCapacity: 'host-configured',
    receipt: 'soty.context-after.v1', projection: 'soty.workspace-receipt.v1', witness: 'soty.workspace-context-witness.v1' } });
export const STANDARD_WORKSPACE_SOURCE = deepFreeze({ id: 'soty.standard-workspace', version: 1, digest: digest(STANDARD_WORKSPACE_CONTRACT) });
export const isWorkspaceSource = pin => pin?.id === STANDARD_WORKSPACE_SOURCE.id && pin.version === 1 && pin.digest === STANDARD_WORKSPACE_SOURCE.digest;

/** A constructor selects one compiled contract, never an author byte limit. */
export function standardSourceContract(input = STANDARD_SELECTED_SOURCE) {
  const pin = fields(input, ['id', 'version', 'digest']);
  for (const [approved, contract] of [[STANDARD_SELECTED_SOURCE, STANDARD_SOURCE_CONTRACT],
    [STANDARD_SELECTED_SOURCE_V2, STANDARD_SOURCE_CONTRACT_2], [STANDARD_SELECTED_SOURCE_V3, STANDARD_SOURCE_CONTRACT_3], [STANDARD_WORKSPACE_SOURCE, STANDARD_WORKSPACE_CONTRACT]]) {
    if (pin.id === approved.id && pin.version === approved.version && pin.digest === approved.digest) return contract;
  }
  check(false, 'source_app_profile_invalid', 503);
}
