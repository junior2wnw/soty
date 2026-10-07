/** RAM-only renewal. A ready event is only a wake-up; a new slot requires the
 * private installed-channel Source ACK in fresh signed server context. */
import { isScopedRuntimeProfile } from './app-launch.mjs';

/** Stage lifecycle only. The profile comes from the validated launcher, never
 * author metadata; tick/probe still require fresh private server/Source ACK. */
export function attachScopedRenewalLoads({runtime,view,signal,current,profile,renewal,onTickFailure=()=>{}}) {
  let timer=null,disposed=false;
  const eligible=()=>!disposed&&current()&&isScopedRuntimeProfile(profile());
  runtime.addEventListener('load',()=>{
    if(!eligible())return;
    if(timer===null)timer=view.setInterval(()=>{
      if(eligible()&&runtime.ownerDocument.visibilityState==='visible')void renewal.tick().catch(()=>{if(eligible())onTickFailure();});
    },30000);
    void renewal.probe().catch(()=>{});
  },{capture:true,signal});
  return()=>{disposed=true;if(timer!==null){view.clearInterval(timer);timer=null;}};
}

export function createScopedSlotRenewal({ readBinding, readContext, request, bootstrap, commit, isCurrent,
  onState = () => {}, clock = Date.now } = {}) {
  let disposed = false, pending = null, flight = null, witness = null, captureLeases = 0;
  const denialCodes = new Set(['ACCOUNT_MISMATCH', 'ACTIVE_PROFILE_CHANGED', 'DEVICE_REVOKED', 'CLIENT_DISPOSED',
    'BOOTSTRAP_REQUIRED', 'NO_LOCAL_PROFILE', 'CLEANUP_CREDENTIAL_CHANGED', 'authentication_required',
    'apps_authentication_required', 'apps_access_denied', 'app_access_revoked', 'app_access_expired',
    'app_access_changed', 'app_source_changed', 'app_policy_changed', 'app_scoped_context_closed',
    'app_scoped_context_changed', 'app_scoped_renew_conflict', 'app_scoped_close_unavailable']);
  const denied = error => error?.status === 401 || error?.status === 403 || denialCodes.has(error?.code);
  const state = value => { if (!disposed) onState(value); };
  const live = binding => {
    const accepted = !disposed && isCurrent() && readBinding()?.slot === binding.slot;
    if (!accepted) witness = null;
    return accepted;
  };
  async function read(handle) {
    try { return await readContext(handle); }
    catch (error) { if (denied(error)) witness = null; throw error; }
  }
  const sameSource = (context, binding) => context?.ready === true && context?.scopedSource?.id === binding.source?.id
    && context.scopedSource.version === binding.source.version && context.scopedSource.digest === binding.source.digest;
  const sourceReady = (context, binding) => sameSource(context, binding) && context.sourceSession?.ready === true
    && context.sourceSession.renewable === true && Number.isSafeInteger(context.sourceSession.sessionExpiresAt)
    && context.sourceSession.sessionExpiresAt > clock();
  const bindingPin = (context, binding) => JSON.stringify([binding.handle, binding.source.id, binding.source.version,
    binding.source.digest, context.target?.revision ?? null, context.target?.digest ?? null]);
  // The original slot is server-bound to account/device/generation/resource.
  // This witness permits only an attempt after AT expiry, never readiness or
  // Source permission. A same-slot source/target mutation cannot reuse it.
  function mayAttempt(context, binding) {
    if (!sameSource(context, binding)) { witness = null; return false; }
    const pin = bindingPin(context, binding);
    if (witness && (witness.slot !== binding.slot || witness.pin !== pin)) { witness = null; return false; }
    if (sourceReady(context, binding)) {
      if (witness && witness.until !== context.sourceSession.sessionExpiresAt) { witness = null; return false; }
      witness = { slot: binding.slot, pin, source: Object.freeze({ ...binding.source }), until: context.sourceSession.sessionExpiresAt };
    } else if (context.sourceSession !== undefined && context.sourceSession !== null) witness = null;
    return witness?.slot === binding.slot && witness.pin === pin && witness.until > clock();
  }
  async function cleanup(action) {
    const close = action?.reply?.cleanup ?? action?.cleanup;
    if (close) { try { await close(); } catch { /* Revoke independently invalidates the original server slot. */ } }
  }
  async function probe() {
    const binding = readBinding();
    if (!binding || !live(binding)) return false;
    const context = await read(binding.handle);
    return live(binding) && mayAttempt(context, binding);
  }
  async function renew() {
    if (flight) return flight;
    flight = (async () => {
      let action;
      try {
        const binding = readBinding();
        if (!binding || !live(binding)) return false;
        if (!pending) {
          if (!await probe() || !live(binding)) return false;
          pending = { binding, requestId: crypto.randomUUID().replaceAll('-', ''), sessionExpiresAt: witness.until, reply: null };
        }
        action = pending; state('pending');
        if (!live(action.binding)) { await cleanup(action); pending = null; return false; }
        const reply = action.reply ?? await request({ handle: action.binding.handle, requestId: action.requestId });
        action.reply = reply;
        if (!live(action.binding)) { await cleanup(action); pending = null; return false; }
        const hinted = await bootstrap(reply);
        if (!live(action.binding)) { await cleanup(action); pending = null; return false; }
        const context = await read(reply.handle);
        if (!live(action.binding)) { await cleanup(action); pending = null; return false; }
        if (!sourceReady(context, action.binding) || context.sourceSession.sessionExpiresAt !== action.sessionExpiresAt) {
          if (hinted === 'unknown' && context.sourceSession?.ready !== false && context.sourceSession?.renewable !== false) {
            state('unknown'); action.reply = null; action.cleanup = reply.cleanup; return false;
          }
          witness = null; state('login_required'); await cleanup(action); pending = null; return false;
        }
        if (!commit(action.binding, reply)) { await cleanup(action); pending = null; witness = null; return false; }
        pending = null; witness = null; mayAttempt(context, readBinding()); state('ready'); return true;
      } catch (error) {
        if (denied(error)) { witness = null; await cleanup(action ?? pending); pending = null; state('login_required'); }
        else { state('unknown'); if (action?.reply) { action.cleanup = action.reply.cleanup; action.reply = null; } }
        return false;
      } finally { flight = null; }
    })();
    return flight;
  }
  return Object.freeze({ probe, renew,
    beginCaptureLease() {
      if (disposed) return () => {};
      captureLeases++; let released = false;
      return () => { if (!released) { released = true; captureLeases--; } };
    },
    async ensure(minRemaining = 190000) {
      const binding = readBinding();
      if (!binding || !live(binding)) return false;
      const context = await read(binding.handle);
      if (!live(binding)) return false;
      const attempt = mayAttempt(context, binding), source = context?.sourceSession;
      // Missing ACK or Root TTL alone is never readiness. Basic and long
      // sessions require actual readiness and BOTH deadlines long enough.
      if (sameSource(context, binding) && source?.ready === true
        && Number.isSafeInteger(source.accessExpiresAt) && source.accessExpiresAt - clock() >= minRemaining
        && context.expiresAt - clock() >= minRemaining) return true;
      if (!attempt || !await renew()) return false;
      const next = readBinding();
      if (!next || !live(next)) return false;
      const fresh = await read(next.handle);
      return live(next) && sameSource(fresh, next) && fresh.sourceSession?.ready === true
        && Number.isSafeInteger(fresh.sourceSession.accessExpiresAt)
        && fresh.sourceSession.accessExpiresAt - clock() >= minRemaining
        && fresh.expiresAt - clock() >= minRemaining;
    },
    async tick() {
      if (disposed || !isCurrent()) { witness = null; return false; }
      const binding = readBinding();
      if (!binding) { witness = null; return false; }
      const context = await read(binding.handle);
      if (!live(binding)) return false;
      const attempt = mayAttempt(context, binding);
      if (captureLeases > 0) return false;
      return attempt && context.expiresAt - clock() < 220000 ? renew() : false;
    },
    dispose() {
      disposed = true; witness = null; void cleanup(pending); pending = null;
    },
  });
}
