export class AppReleaseGuidanceError extends Error {
  constructor() { super('invalid_release_guidance'); this.code = 'invalid_release_guidance'; }
}

// Shared by CLI and MCP. No network, credentials, shell commands or mutations.
const object = (properties, required = Object.keys(properties)) => ({ type: 'object', properties, required, additionalProperties: false });
const enumeration = values => ({ type: 'string', enum: values });
const observations = {
  standalone: ['works', 'fails', 'unknown'],
  embedded: ['visible', 'blank', 'unknown'],
  document: ['received', 'http_error', 'stalled_before_connection', 'unknown'],
  debugger: ['attached', 'detached', 'unknown'],
  framing: ['allowed', 'ancestor_blocked', 'child_frame_blocked', 'unknown'],
  websocket: ['works', 'auth_upgrade_rejected', 'origin_rejected', 'unknown'],
  boot: ['continued', 'expired', 'cookie_rejected', 'failed', 'unknown'],
  readiness: ['ready', 'unsupported', 'pending', 'denied', 'unknown'],
  access: ['allowed', 'denied', 'unknown'],
  sourcePin: ['matches', 'changed', 'unknown'],
  identity: ['verified', 'missing', 'expired', 'wrong_subject', 'unknown'],
  persistence: ['verified', 'lost_on_reload', 'not_checked'],
};
const shortText = { type: 'string', maxLength: 1024 };
const steps = { type: 'array', minItems: 1, maxItems: 8, items: shortText };
export const DIAGNOSE_INPUT_SCHEMA = object({ observations: { ...object(Object.fromEntries(
  Object.entries(observations).map(([key, values]) => [key, enumeration(values)])), []), minProperties: 1 } });
export const DIAGNOSE_OUTPUT_SCHEMA = object({
  schema: { const: 'soty.app-diagnosis.v1' }, basis: { const: 'reported_observations' },
  liveVerified: { const: false }, releaseReadiness: { const: 'not_assessed' },
  findings: { type: 'array', minItems: 1, maxItems: 16, items: object({ code: { type: 'string', pattern: '^[a-z_]+$' },
    certainty: enumeration(['observed', 'hypothesis', 'insufficient_evidence']), explanation: shortText, next: steps }) },
});
export const AUTH_GUIDE_INPUT_SCHEMA = object({ mode: enumeration(['human-oidc', 'private-owner', 'agent']) });
export const AUTH_GUIDE_OUTPUT_SCHEMA = object({
  schema: { const: 'soty.app-auth-guide.v1' }, mode: AUTH_GUIDE_INPUT_SCHEMA.properties.mode,
  applied: { const: false }, productionEnabled: { const: 'not_checked' }, identity: shortText,
  steps, acceptance: steps, boundaries: steps,
  references: { type: 'array', minItems: 1, maxItems: 4, items: shortText },
});

function plain(value, keys) {
  if (!value || typeof value !== 'object' || Array.isArray(value)
    || ![Object.prototype, null].includes(Object.getPrototypeOf(value))) throw new AppReleaseGuidanceError();
  const descriptors = Object.getOwnPropertyDescriptors(value);
  for (const key of Reflect.ownKeys(descriptors)) {
    if (typeof key !== 'string' || !keys.includes(key) || !Object.hasOwn(descriptors[key], 'value'))
      throw new AppReleaseGuidanceError();
  }
  return descriptors;
}

export function diagnoseAppRelease(input) {
  const outer = plain(input, ['observations']);
  if (!outer.observations) throw new AppReleaseGuidanceError();
  const fields = plain(outer.observations.value, Object.keys(observations));
  if (!Object.keys(fields).length) throw new AppReleaseGuidanceError();
  const seen = {};
  for (const [key, descriptor] of Object.entries(fields)) {
    if (!observations[key].includes(descriptor.value)) throw new AppReleaseGuidanceError();
    seen[key] = descriptor.value;
  }
  const findings = [], add = (code, certainty, explanation, ...next) => findings.push({ code, certainty, explanation, next });
  if (seen.sourcePin === 'changed') add('source_pin_changed', 'observed', 'The declared source no longer matches the ingress pin.',
    'Read a fresh deployment snapshot and review the exact source revision. Use the supported prepare/promote flow; keep the per-request pin check.');
  if (seen.access === 'denied' || seen.readiness === 'denied') add('access_denied', 'observed', 'A current access check was denied.',
    'Check the intended account, app audience and current grants. Reauthenticate when needed; do not make the app public to bypass a denial.');
  if (seen.boot === 'expired') add('boot_ticket_expired', 'observed', 'An expired boot document cannot establish a new app session.',
    'Launch or refresh the app once from the current Soty card to obtain a fresh ticket. Do not replay a copied boot URL.');
  if (seen.boot === 'cookie_rejected') add('boot_cookie_rejected', 'observed', 'The boot cookie was rejected or unavailable.',
    'Check the exact named HTTPS origin, cookie attributes and current boot flow in the authorized browser. Preserve browser security settings.');
  if (seen.boot === 'failed') add('boot_failed', 'observed', 'The boot flow reported a failure.',
    'Read the bounded error code and current launch state; separate admission, cookie and continuation failures before retrying.');
  if (seen.embedded === 'blank' && seen.document === 'stalled_before_connection') {
    if (seen.debugger === 'attached') add('debugger_interference_suspected', 'hypothesis', 'A debugger is attached while the embedded document stalls before connection. This can be tooling interference, not a server response.',
      'Observe Network timing and the real browser window using an authorized channel. Disconnect only the task debugger if authorized, then launch once with a fresh ticket.',
      'Compare the embedded and standalone app with the debugger detached. Do not reattach the same instrument just to prove the frame works.');
    else add('preconnection_stall_unclassified', 'insufficient_evidence', 'A stall before connection does not establish a CSP, DNS, authentication or application failure.',
      'Check browser Network timing and debugger attachment, then compare a fresh card launch with standalone opening. Do not change server security without evidence.');
  }
  if (seen.framing === 'ancestor_blocked') add('frame_ancestor_policy', 'observed', 'The app rejects its embedding parent.',
    'Check the exact current parent origin against frame-ancestors and X-Frame-Options. Allow only reviewed parents and retain the app CSP.');
  if (seen.framing === 'child_frame_blocked') add('nested_frame_policy', 'observed', 'A nested app frame is blocked.',
    'Check whether isolated transport forbids the app nested frames. If the existing app requires them, review native transport with a fixed loopback upstream, current ACL/source pin and exact Origin checks. Do not weaken the global sandbox.');
  if (seen.websocket === 'auth_upgrade_rejected') add('auth_subrequest_upgrade', 'observed', 'The authentication subrequest is receiving WebSocket upgrade headers.',
    'Regenerate native ingress with the maintained planner. Strip Upgrade, Connection and Sec-WebSocket-* only from forward_auth; preserve the application handshake.',
    'Verify an actual 101 and message exchange, denied access, changed source pin and foreign or missing Origin.');
  if (seen.websocket === 'origin_rejected') add('websocket_origin_rejected', 'observed', 'The WebSocket Origin check rejected the request.',
    'Compare the request Origin with the exact named app origin and the app own allowlist. Keep foreign and missing browser Origins denied.');
  if (['missing', 'expired', 'wrong_subject'].includes(seen.identity)) add('app_identity_required', 'observed', 'Ingress access alone does not identify the human inside the app.',
    'Use apps_auth_guide with mode human-oidc. Verify issuer/sub, fresh userinfo, app session and native ACL; retain stable existing account mappings.');
  if (seen.persistence === 'lost_on_reload') add('persistence_failed', 'observed', 'A verified app change did not survive reload.',
    'Check the active database/volume, stable subject mapping, migration order and single writer. Recover the same record IDs before qualifying the release.');
  if (seen.readiness === 'unsupported' && seen.document === 'received') add('readiness_not_implemented', 'observed', 'The document loaded but this app does not implement scoped authenticated readiness.',
    'Use neutral document-loaded status. Verify real UI, identity, read/write and reload separately; do not emit a fake ready signal.');
  if (seen.document === 'http_error') add('document_http_error', 'observed', 'The document request returned an HTTP error.',
    'Check the safe status/error code, exact route, current access and source pin. A boot HTTP 200 alone does not prove that the application loaded.');
  if (seen.embedded === 'blank' && !findings.length) add('blank_frame_unclassified', 'insufficient_evidence', 'A blank frame alone does not identify its cause.',
    'Compare standalone and embedded opening; capture a bounded Root phase trace and Network timing without cookies, tokens, full boot URLs or child DOM.');
  if (!findings.length) add('no_conclusive_fault', 'insufficient_evidence', 'These observations do not establish a fault or release readiness.',
    'Complete the release acceptance matrix: two users, denied/expired/revoked access, real app data, persistence/reload and rollback.');
  return { schema: 'soty.app-diagnosis.v1', basis: 'reported_observations', liveVerified: false, releaseReadiness: 'not_assessed', findings };
}

const guides = {
  'human-oidc': {
    identity: 'A human is identified by the verified pair (iss, sub). App roles and data permissions remain app-owned.',
    steps: [
      'Check that the target host has explicitly enabled modules/human-identity and approved this confidential BFF client with an exact HTTPS callback. Module presence or an app card does not prove activation.',
      'Use the maintained server-only modules/source-rp library. The two-BFF example in modules/human-identity/examples/bff.mjs is a protocol test fixture, not a production server. Keep client secrets, PKCE verifier, tokens and encryption keys on the server.',
      'Start Authorization Code with S256 PKCE, state and nonce. Complete current Connect-device approval and human consent through the issuer.',
      'At the server callback validate state, nonce, signature, issuer, audience and expiry using the maintained OIDC client; create the app own Secure/HttpOnly session.',
      'Check fresh userinfo/current authority for protected requests and use the app own native ACL. Keep issuer/sub stable across launch, reload and origins.',
      'Run required schema migrations before optional workers. Link an existing account only after proving both accounts; never merge by email, display name or app ownership.',
      'Use the reviewed Basic expiry profile by default. Finite renewal requires explicit host/client approval, user choice and encrypted durable Source storage; do not invent indefinite sessions.',
    ],
    acceptance: ['Two people get different data/roles; the same person in two apps has the same issuer/sub with separate app sessions.',
      'Wrong state/nonce/audience, denied consent, expired/revoked device/grant and disabled client all fail closed.',
      'A real permitted write survives full Soty reload; old record IDs and ACL remain correct; another person cannot read it.'],
    boundaries: ['The Soty ingress cookie and /_soty/session prove app admission, not human identity. Never trust an arbitrary identity header.',
      'Never store client secrets or bearer/refresh tokens in browser JavaScript, localStorage, launch URLs or diagnostics.',
      'Do not enable the issuer, register a client, expand grants or migrate accounts solely because this guide was requested.'],
    references: ['docs/deployment/app-authentication.md', 'modules/human-identity/README.md', 'modules/source-rp/README.md'],
  },
  'private-owner': {
    identity: 'A single-owner compatibility adapter identifies only that private installation owner; it is not a shared human identity service.',
    steps: ['Keep the source listener on fixed loopback behind a restricted Soty app and per-request current access/source-pin checks.',
      'Review server-side session issuance, request origin, cookie policy and the app own ACL. Protect any adapter/session script from foreign-origin reads.',
      'Migrate to human-oidc before admitting multiple people. Reusing local user 1 for different people would mix their data.'],
    acceptance: ['The authorized owner can read/write/reload the same records.',
      'Anonymous, other-owner, foreign-origin, changed-source and expired/revoked admission fail closed.'],
    boundaries: ['Do not publish the listener or reuse its local account ID as a global Soty person.',
      'This mode does not provide multi-user login, account linking, Telegram login or OIDC.'],
    references: ['docs/deployment/app-authentication.md', 'docs/deployment/app-release.md'],
  },
  agent: {
    identity: 'An agent uses the authorized OAuth/MCP principal and exact capability grants; it is not automatically the app human.',
    steps: ['Use current Root OAuth discovery and the intended MCP resource/audience; keep the current account and source binding pins.',
      'Discover the authorized app capability catalog and execute only granted operations with stable intent/request IDs.',
      'For an uncertain effect, recover the same invocation/source proof or retry identical intent; recheck current authority before exposing private results.'],
    acceptance: ['Wrong audience/scope/account, revoked device/grant and changed app pin fail closed.',
      'A permitted operation produces the real app result and survives reload; read-only guidance creates no data or grants.'],
    boundaries: ['Tool availability and public catalog metadata grant no execution or app data rights.',
      'Agent OAuth, human OIDC and app-native ACL are separate authorities. Never copy human tokens into a model prompt.'],
    references: ['docs/deployment/app-authentication.md', 'docs/implementation/p4-mcp-integration.md'],
  },
};
export function appAuthenticationGuide(input) {
  const fields = plain(input, ['mode']), mode = fields.mode?.value;
  if (typeof mode !== 'string' || !Object.hasOwn(guides, mode)) throw new AppReleaseGuidanceError();
  return { schema: 'soty.app-auth-guide.v1', mode, applied: false, productionEnabled: 'not_checked', ...structuredClone(guides[mode]) };
}
