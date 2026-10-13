/** RAM-only renewal. A ready event is only a wake-up; a new slot requires the
 * private installed-channel Source ACK in fresh signed server context. */
import { isScopedRuntimeProfile } from './app-launch.mjs';

// Root identity may acquire a new finite session. RP keeps its family deadline;
// only the explicit private short-session ACK can request a new short carrier.
export const isRootSsoWorkspacePin = value => value?.id === 'soty.standard-workspace' && value.version === 2
  && value.digest === '23061fc95610e3011ec42b3ecf229308bce44b0a1268408ca9b6ee138c98f7e9';

/** Returning to a mounted app asks Root for a fresh signed launch. This never
 * revives a closed stage or treats an expired session as authorization. */
export function attachRootSsoWakeRecovery({ view, document = view.document, signal, current,
  readBinding, lastLaunchAt, recover, clock = Date.now, onFailure = () => {} }) {
  let disposed = false, pending = false;
  const attempted = new WeakSet();
  const eligible = () => !disposed && !signal.aborted && document.visibilityState === 'visible'
    && current() && isRootSsoWorkspacePin(readBinding()?.source) && clock() - lastLaunchAt() >= 180000;
  const wake = (expected = readBinding()) => {
    if (pending || !eligible()) return;
    const slot = expected?.slot;
    if (!slot || typeof slot !== 'object' || readBinding()?.slot !== slot || attempted.has(slot)) return;
    pending = true;
    void Promise.resolve().then(() => {
      if (eligible() && readBinding()?.slot === slot) {
        attempted.add(slot); return recover();
      }
    }).catch(onFailure).finally(() => { pending = false; });
  };
  const onWake = () => wake();
  document.addEventListener('visibilitychange', onWake, { signal });
  view.addEventListener('focus', onWake, { signal });
  const detach = () => {
    disposed = true;
    document.removeEventListener('visibilitychange', onWake);
    view.removeEventListener('focus', onWake);
  };
  return Object.freeze(Object.assign(detach, {
    recoverExpired(error, binding) {
      // ownedContext sweeps a 300s Root lease before returning its closed-slot
      // code. An early close or any ACL/device/source denial is not expiry.
      if (!binding || error?.code !== 'scoped_embed_expired' && !(error?.code === 'app_scoped_context_closed'
        && clock() - lastLaunchAt() >= 300000)) return;
      wake(binding);
    },
  }));
}

/** Stage lifecycle only. The profile comes from the validated launcher, never
 * author metadata; tick/probe still require fresh private server/Source ACK. */
export function attachScopedRenewalLoads({runtime,view,signal,current,profile,renewal,readBinding,onTickFailure=()=>{}}) {
  let timer=null,disposed=false;
  const eligible=()=>!disposed&&!signal.aborted&&current()&&isScopedRuntimeProfile(profile());
  runtime.addEventListener('load',()=>{
    if(!eligible())return;
    if(timer===null)timer=view.setInterval(()=>{
      if(eligible()&&runtime.ownerDocument.visibilityState==='visible') {
        const binding=readBinding?.();
        void renewal.tick().catch(error=>{if(eligible()&&(!readBinding||binding?.slot===readBinding()?.slot))onTickFailure(error,binding);});
      }
    },30000);
    void renewal.probe().catch(()=>{});
  },{capture:true,signal});
  return()=>{disposed=true;if(timer!==null){view.clearInterval(timer);timer=null;}};
}

/** Source-origin wake-up only. No handle, cookie, deadline or write intent is
 * accepted from the frame. Current peer and Root-owned slot are checked twice. */
export function attachSourceFrameRefresh({view,signal,current,readPeer,refresh}) {
  const receive=async event=>{
    const peer=readPeer(),data=event.data;
    if(!current()||signal.aborted||!peer||event.source!==peer.window||event.origin!==peer.origin
      ||!data||data.schema!=='soty.source-frame-refresh.v1'||typeof data.requestId!=='string'
      ||!/^[A-Za-z0-9_-]{16,128}$/.test(data.requestId)
      ||Object.keys(data).length!==2)return;
    let ready=false;try{ready=await refresh();}catch{}
    const fresh=readPeer();
    if(current()&&!signal.aborted&&fresh?.window===peer.window&&fresh.origin===peer.origin)
      peer.window.postMessage({schema:'soty.source-frame-refreshed.v1',requestId:data.requestId,ready:ready===true},peer.origin);
  };
  view.addEventListener('message',receive,{signal});
  return()=>view.removeEventListener('message',receive);
}

export function createScopedSlotRenewal({ readBinding, readContext, request, bootstrap, commit, isCurrent,
  onState = () => {}, clock = Date.now } = {}) {
  let disposed = false, pending = null, flight = null, witness = null, captureLeases = 0;
  const denialCodes = new Set(['ACCOUNT_MISMATCH', 'ACTIVE_PROFILE_CHANGED', 'DEVICE_REVOKED', 'CLIENT_DISPOSED',
    'BOOTSTRAP_REQUIRED', 'NO_LOCAL_PROFILE', 'CLEANUP_CREDENTIAL_CHANGED', 'authentication_required',
    'apps_authentication_required', 'apps_access_denied', 'app_access_revoked', 'app_access_expired',
    'app_access_changed', 'app_source_changed', 'app_policy_changed', 'app_scoped_context_closed',
    'app_scoped_context_changed', 'app_scoped_renew_conflict', 'app_scoped_close_unavailable', 'app_scoped_short_session_required']);
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
    && context.sourceSession.sessionExpiresAt > clock()
    && (!(isRootSsoWorkspacePin(binding.source)||context.sourceSession.shortSession===true)
      || context.sourceSession.sessionExpiresAt <= context.expiresAt)
    && (context.sourceSession.shortSession!==true||context.sourceSession.sessionExpiresAt<=clock()+300000);
  const bindingPin = (context, binding) => JSON.stringify([binding.handle, binding.source.id, binding.source.version,
    binding.source.digest, context.target?.revision ?? null, context.target?.digest ?? null, context.renewalBindingDigest ?? null]);
  // The original slot is server-bound to account/device/generation/resource.
  // This witness permits only an attempt after AT expiry, never readiness or
  // Source permission. A same-slot source/target mutation cannot reuse it.
  function mayAttempt(context, binding) {
    if (!sameSource(context, binding)) { witness = null; return false; }
    const pin = bindingPin(context, binding);
    if (witness && (witness.slot !== binding.slot || witness.pin !== pin)) { witness = null; return false; }
    if (sourceReady(context, binding)) {
      if (witness && witness.until !== context.sourceSession.sessionExpiresAt && !isRootSsoWorkspacePin(binding.source)
        && !(witness.shortSession&&context.sourceSession.shortSession===true)) { witness = null; return false; }
      witness = { slot: binding.slot, pin, source: Object.freeze({ ...binding.source }), until: context.sourceSession.sessionExpiresAt,
        shortSession:context.sourceSession.shortSession===true&&/^[a-f0-9]{64}$/.test(context.renewalBindingDigest??'') };
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
  async function renew(shortSessionHandoff=false) {
    if (flight) return flight;
    flight = (async () => {
      let action;
      try {
        const binding = readBinding();
        if (!binding || !live(binding)) return false;
        if (!pending) {
          if (!shortSessionHandoff&&(!await probe() || !live(binding))) return false;
          if(captureLeases>0||!live(binding))return false;
          // Explicit refresh may outlive the old lease. The server's bounded
          // private short ACK witness authorizes ONLY a same-scope attempt.
          pending = { binding, requestId: crypto.randomUUID().replaceAll('-', ''), sessionExpiresAt: witness?.until,
            // Root workspace refresh uses its existing fresh-identity branch,
            // never the RP-only short-carrier flag or its ACK requirements.
            shortSessionHandoff:!isRootSsoWorkspacePin(binding.source)&&(shortSessionHandoff||witness?.shortSession===true), reply: null };
        }
        action = pending; state('pending');
        if (!live(action.binding)) { await cleanup(action); pending = null; return false; }
        const reply = action.reply ?? await request({ handle: action.binding.handle, requestId: action.requestId,
          ...(action.shortSessionHandoff?{shortSessionHandoff:true}:{}) });
        action.reply = reply;
        if (!live(action.binding)) { await cleanup(action); pending = null; return false; }
        const hinted = await bootstrap(reply);
        if (!live(action.binding)) { await cleanup(action); pending = null; return false; }
        const context = await read(reply.handle);
        if (!live(action.binding)) { await cleanup(action); pending = null; return false; }
        const shortReady=action.shortSessionHandoff&&context.sourceSession?.shortSession===true
          &&/^[a-f0-9]{64}$/.test(reply.bindingDigest??'')&&context.renewalBindingDigest===reply.bindingDigest;
        if (!sourceReady(context, action.binding) || (action.shortSessionHandoff?!shortReady:!isRootSsoWorkspacePin(action.binding.source)
          && context.sourceSession.sessionExpiresAt !== action.sessionExpiresAt)) {
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
  return Object.freeze({ probe, renew,refresh:()=>renew(true),
    beginCaptureLease() {
      if (disposed || flight || pending) throw new Error('capture_context_changed');
      captureLeases++; let released = false;
      return () => { if (!released) { released = true; captureLeases--; } };
    },
    async ensure(minRemaining = 190000) {
      const before = readBinding(), sourcePin = before?.source && { ...before.source };
      if (!before || !live(before)) return false;
      const draining = flight;
      if (draining && !await draining) return false;
      const binding = readBinding();
      if (!binding || !live(binding) || binding.source?.id !== sourcePin?.id
        || binding.source?.version !== sourcePin?.version || binding.source?.digest !== sourcePin?.digest) return false;
      const context = await read(binding.handle);
      if (!live(binding) || flight || pending) return false;
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
      return live(next) && !flight && !pending && sameSource(fresh, next) && fresh.sourceSession?.ready === true
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
