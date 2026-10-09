import { AsyncLocalStorage } from 'node:async_hooks';
import { types } from 'node:util';
import { snapshot, canonicalContractJson } from '../modules/app-contract/json.mjs';

const same = (a, b) => canonicalContractJson(a) === canonicalContractJson(b);
const failure = (code = 'managed_reviews_native_unavailable', status = 503) => Object.assign(new Error(code), { code, status });
const need = (value, code, status) => { if (!value) throw failure(code, status); };
const data = value => { try { return snapshot(value); } catch { throw failure('managed_reviews_native_invalid', 500); } };
const methods = ['context', 'list', 'collect'];

/** Trusted same-process host seam. Native ports are borrowed, never created or
 * closed here. A private per-call token is correlation, not a Native grant. */
export function createManagedReviewsNativeBridge({ nativeApplication, providerRef, withHumanSubjectAuthority, timeoutMs } = {}) {
  need(nativeApplication && typeof withHumanSubjectAuthority === 'function', 'managed_reviews_native_host_required', 500);
  const native = nativeApplication.managedService, sourceApp = nativeApplication.managedSourceApp;
  need(native && sourceApp && typeof native.withActor === 'function' && typeof sourceApp.resolveSourceCredential === 'function'
    && methods.every(name => typeof native[name] === 'function'), 'managed_reviews_native_host_required', 500);
  const profile = data(sourceApp.profile), provider = Object.freeze(data(providerRef));
  need(typeof profile.issuer === 'string' && typeof profile.clientId === 'string', 'managed_reviews_native_host_required', 500);
  const resolve = sourceApp.resolveSourceCredential.bind(sourceApp), withActor = native.withActor.bind(native);
  const nativeMethods = Object.fromEntries(methods.map(name => [name, native[name].bind(native)]));
  const tokens = new WeakMap(), frames = new AsyncLocalStorage();
  let closed = false;
  const live = state => need(!closed && !state?.poisoned && !state?.signal?.aborted, 'managed_reviews_native_retired');

  function currentHuman(actor) {
    live(); let open = true, entered = false, poisoned = false, selected;
    try {
      const returned = withHumanSubjectAuthority({ actor, issuer: profile.issuer, clientId: profile.clientId }, raw => {
        if (!open || entered) { poisoned = true; throw failure('managed_reviews_native_human_fence', 500); }
        entered = true;
        const value = data(raw);
        need(Object.keys(value).sort().join(',') === 'clientGeneration,clientId,clientProfileDigest,issuer,subject'
          && value.issuer === profile.issuer && value.clientId === profile.clientId
          && typeof value.subject === 'string' && value.subject.length > 0
          && /^[a-f0-9]{64}$/u.test(value.clientProfileDigest)
          && Number.isSafeInteger(value.clientGeneration) && value.clientGeneration > 0, 'managed_reviews_native_human_fence', 403);
        selected = Object.freeze(value);
      });
      if (returned && typeof returned.then === 'function') {
        Promise.resolve(returned).catch(() => {}); poisoned = true;
      }
      need(entered && !poisoned && selected, 'managed_reviews_native_human_fence', 403);
      live(); return { value: selected, valid: () => !poisoned };
    } finally { open = false; }
  }

  function check(state) {
    live(state);
    const current = currentHuman(state.actor);
    need(state.human.valid() && current.valid() && same(state.human.value, current.value), 'managed_reviews_native_human_changed', 403);
  }
  function outcome(state, error) { return state.mutationStarted ? failure('managed_reviews_outcome_unknown') : error; }
  const source = {
    async withActor(token, action) {
      const state = tokens.get(token);
      need(state && !state.used && typeof action === 'function' && !types.isProxy(action), 'managed_reviews_native_token_invalid', 403);
      state.used = true; check(state);
      let open = true, entered = false, settled = false, poisoned = false, result;
      try {
        const returned = await withActor(state.request, async actor => {
          if (!open || entered) { poisoned = true; throw failure('managed_reviews_native_actor_fence', 500); }
          entered = true; check(state);
          const frame = { state, actor, active: true, operating: false };
          try {
            result = await frames.run(frame, () => action(actor));
            check(state); settled = true; return result;
          } finally { frame.active = false; }
        });
        need(entered && settled && !poisoned && returned === result, 'managed_reviews_native_actor_fence', 500);
        check(state); return result;
      } catch (error) { throw outcome(state, error); }
      finally { open = false; tokens.delete(token); state.request = undefined; }
    },
  };
  for (const name of methods) source[name] = async (actor, input) => {
    const frame = frames.getStore();
    need(frame?.active && frame.actor === actor && !frame.operating, 'managed_reviews_native_actor_fence', 403);
    check(frame.state); frame.operating = true;
    if (name === 'collect') frame.state.mutationStarted = true;
    try { const result = await nativeMethods[name](actor, input); check(frame.state); return result; }
    catch (error) { throw outcome(frame.state, error); }
    finally { frame.operating = false; }
  };
  // The existing managed Root constructor validates these method slots, while
  // its public extension exposes only context/list/collect. No new admin path.
  for (const name of ['provision', 'publish', 'moderate']) source[name] = () => { throw failure('managed_reviews_operation_unknown', 400); };
  const managedReviewsSource = Object.freeze({ providerRef: provider, source: Object.freeze(source),
    async resolveSourceCredential(request) {
      live(); need(request?.actor && typeof request.actor === 'object' && !types.isProxy(request.actor), 'managed_reviews_native_actor_fence', 403);
      const state = { actor: request.actor, human: currentHuman(request.actor), signal: request.signal, used: false, poisoned: false, mutationStarted: false };
      check(state);
      state.request = await resolve(request, withHumanSubjectAuthority);
      check(state);
      need(state.request && typeof state.request === 'object', 'managed_reviews_native_unavailable');
      const token = Object.freeze(Object.create(null)); tokens.set(token, state); return token;
    }, ...(timeoutMs === undefined ? {} : { timeoutMs }),
  });
  return Object.freeze({ managedReviewsSource, close() { closed = true; } });
}
