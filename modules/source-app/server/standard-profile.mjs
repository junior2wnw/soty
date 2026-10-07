import { deepFreeze, digest } from './wire.mjs';

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
