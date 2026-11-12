import { canonicalJson } from '../../modules/connect/browser/crypto.mjs';
import { createAppHostRetirement } from '../platform/app-host-retirement.mjs';

const hostReplies = new Set(['apps.launch', 'apps.session.renew', 'apps.session.confirm', 'apps.session.context']);
const fail = code => { throw Object.assign(new Error(code), { code }); };
const sameScope = (a, b) => a === b || Boolean(a && b && a.accountId === b.accountId && a.deviceId === b.deviceId);
const scopeOf = state => state?.accountId && state.deviceId && !state.current?.revoked
  ? Object.freeze({ accountId: state.accountId, deviceId: state.deviceId }) : null;
function confirmedHostState(operation, args, value) {
  const confirm = operation === 'apps.session.confirm', context = operation === 'apps.session.context';
  if (!confirm && !context) return false;
  const keys = ['ok', 'schema', 'hostSession', 'hostSessionExpiresAt', 'entry', 'launchBinding', 'runtimeOrigin',
    ...(confirm ? ['requestId', 'confirmed'] : ['state'])];
  return Object.keys(value).every(key => keys.includes(key))
    && (value.ok === undefined || value.ok === true) && value.entry?.appId === args.appId
    && ['frameId', 'lease', 'revision'].every(key => value.hostSession[key] === args[key])
    && (confirm ? value.schema === 'soty.app-host-session-confirmation.v1' && value.confirmed === true && value.requestId === args.requestId
      : value.schema === 'soty.app-host-session-context.v1' && value.state === 'current');
}

/** Private Root lifecycle. The portable SDK and its passive selectors stay unchanged. */
export function createRootHostClient(client, { view = globalThis.window, retirement: suppliedRetirement } = {}) {
  let scope = null, generation = 0, disposed = false;
  const purposes = new WeakMap(), callbacks = new WeakMap();
  const observe = state => {
    const next = scopeOf(state);
    if (!sameScope(scope, next)) generation++;
    scope = next;
  };
  const getLocalState = async () => {
    const ticket = generation, state = await client.getLocalState();
    if (!disposed && ticket === generation) {
      const next = scopeOf(state);
      if (!sameScope(scope, next)) generation++;
      scope = next;
    }
    return state;
  };
  const current = (captured, ticket) => !disposed && generation === ticket && sameScope(scope, captured);
  let storage, locks;
  try { storage = view?.sessionStorage; locks = view?.navigator?.locks; } catch { /* Source entries still use their existing admission. */ }
  const retirement = suppliedRetirement ?? createAppHostRetirement({ storage, locks,
    close: async (owner, proof) => {
      const ticket = generation;
      if (!current(owner, ticket)) fail('ACTIVE_PROFILE_CHANGED');
      const state = await client.getLocalState();
      if (!current(owner, ticket) || !sameScope(scopeOf(state), owner)) fail('ACTIVE_PROFILE_CHANGED');
      let result;
      try { result = await client.extension('apps.session.close', proof, { expectedAccountId: owner.accountId }); }
      catch (error) {
        const after = await client.getLocalState();
        if (current(owner, ticket) && sameScope(scopeOf(after), owner) && error?.code === 'app_host_session_closed') return { closed: false };
        throw error;
      }
      const after = await client.getLocalState();
      if (!current(owner, ticket) || !sameScope(scopeOf(after), owner)) fail('ACTIVE_PROFILE_CHANGED');
      if (typeof result?.closed !== 'boolean') fail('app_host_retirement_pending');
      return { closed: result.closed };
    } });
  async function extension(operation, args = {}, context) {
    if (disposed || !hostReplies.has(operation) || operation === 'apps.launch' && args?.hostSession !== true)
      return client.extension(operation, args, context);
    // Match the SDK's synchronous argument capture before lifecycle IO.
    let payload;
    try { payload = JSON.parse(canonicalJson(args)); }
    catch { return client.extension(operation, args, context); }
    const capturedContext = context === undefined ? undefined : { ...context };
    if (capturedContext && (Object.keys(capturedContext).some(key => key !== 'expectedAccountId')
      || typeof capturedContext.expectedAccountId !== 'string' || !capturedContext.expectedAccountId))
      return client.extension(operation, payload, capturedContext);
    if (!scope) await getLocalState();
    const owner = scope, ticket = generation;
    if (!owner || capturedContext && capturedContext.expectedAccountId !== owner.accountId) fail('ACTIVE_PROFILE_CHANGED');
    const isCurrent = () => current(owner, ticket);
    let pendingRetirement = null;
    if (operation === 'apps.launch') {
      try { await retirement.beforeLaunch(owner, isCurrent); }
      catch (error) {
        if (!isCurrent() || error?.code !== 'app_host_retirement_pending') throw error;
        // A Source session allocates no ordinary host member. Diagnose its
        // current transport before letting an unrelated retirement block it.
        let status;
        try { status = await client.extension('apps.authentication.get', { appId: payload.appId, expectedAccountId: owner.accountId },
          { expectedAccountId: owner.accountId }); } catch { throw error; }
        if (!isCurrent() || status?.schema !== 'soty.app-authentication-status.v1'
          || status.appId !== payload.appId || status.transport !== 'source') throw error;
        pendingRetirement = error;
      }
    }
    if (!isCurrent()) fail('ACTIVE_PROFILE_CHANGED');
    const result = await client.extension(operation, payload, capturedContext);
    const cleanup = client.appSlotCleanup(result);
    try {
      const after = await client.getLocalState();
      if (!isCurrent() || !sameScope(scopeOf(after), owner)) fail('ACTIVE_PROFILE_CHANGED');
      if (pendingRetirement && (result?.hostSession !== undefined
        || !['soty.selected-human-embed.v1', 'soty.selected-human-embed.v2'].includes(result?.runtimeProfile))) throw pendingRetirement;
      if (result?.hostSession !== undefined) {
        const markRetired = retirement.record(owner, { appId: result.entry?.appId, hostSession: result.hostSession },
          Boolean(cleanup) && confirmedHostState(operation, payload, result));
        if (cleanup) purposes.set(result, markRetired);
      }
      return result;
    } catch (error) {
      // An unpersisted reservation never reaches an iframe or confirm caller.
      if (cleanup) await cleanup().catch(() => {});
      throw error;
    }
  }
  const appSlotCleanup = reply => {
    const cleanup = client.appSlotCleanup(reply), mark = reply && typeof reply === 'object' ? purposes.get(reply) : null;
    if (!cleanup || !mark) return cleanup;
    let captured = callbacks.get(cleanup);
    if (!captured) {
      captured = { mark, callback: null };
      captured.callback = (...args) => {
        if (args.length) return cleanup(...args);
        try { captured.mark(); } catch { /* Keep the original exact RAM close available if storage fails. */ }
        return cleanup();
      };
      callbacks.set(cleanup, captured);
    } else captured.mark = mark;
    return captured.callback;
  };
  return Object.freeze({ observe, client: Object.freeze({ ...client, getLocalState, extension, appSlotCleanup,
    dispose() { disposed = true; generation++; scope = null; client.dispose(); } }) });
}
