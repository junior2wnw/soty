import test from 'node:test';
import assert from 'node:assert/strict';
import { createAppBootRecovery, isAppBootRetryHint } from './app-boot-recovery.mjs';
const appId = 'app-' + 'a'.repeat(32), origin = 'https://app.fixture.invalid';
const binding = { schema: 'soty.app-launch-binding.v1', policyEpoch: 1, targetRevision: 1,
  targetDigest: 'b'.repeat(64), profile: 'soty.relay-restricted.v1', bindingFloor: 1 };
const tick = () => new Promise(resolve => setImmediate(resolve));
function fixture(recover) {
  const listeners = new Set(), timers = new Map(); let serial = 0;
  const state = { active: true, generation: 0, frame: null, pending: false }, sends = [], calls = [], manual = [];
  const view = { crypto, btoa, location: { origin: 'https://shell.fixture.invalid' },
    setTimeout(fn, delay) { const id = ++serial; timers.set(id, { fn, delay }); return id; }, clearTimeout(id) { timers.delete(id); },
    addEventListener(_type, fn) { listeners.add(fn); }, removeEventListener(_type, fn) { listeners.delete(fn); } };
  const controller = createAppBootRecovery({ view, appId, getFrame: () => state.frame,
    isCurrent: () => state.active && state.generation === 0, canRecover: () => !state.pending,
    recover: async current => { calls.push(current); return recover?.(current); }, onFailure: () => manual.push(true) });
  function frame() { const handlers = new Set(); return { src: origin + '/_soty/boot', contentWindow: { postMessage(data, target) { sends.push({ data, target }); } },
    addEventListener(_type, fn) { handlers.add(fn); }, removeEventListener(_type, fn) { handlers.delete(fn); }, load() { for (const fn of [...handlers]) fn(); } }; }
  function bind(value = binding) { const current = frame(); state.frame = current; controller.bind(current, { origin, binding: value }); current.load(); return current; }
  const message = (data, overrides = {}) => { for (const fn of [...listeners]) fn({ data, source: state.frame.contentWindow, origin, ports: [], ...overrides }); };
  const hint = () => ({ schema: 'soty.app-boot-failure.v1', appId, nonce: sends.at(-1).data.nonce, error: 'app_session_check_failed' });
  return { state, sends, calls, manual, timers, listeners, controller, bind, message, hint };
}
test('one exact frame/origin/nonce hint requests only one bounded recovery; repeated hints and new binds do not reset budget', async () => {
  const f = fixture(); f.bind(); assert.equal(f.sends[0].target, origin); f.message(f.hint()); f.message(f.hint()); await tick();
  assert.equal(f.calls.length, 1); assert.equal(f.controller.attempted(), true); f.bind(); f.message(f.hint()); await tick();
  assert.equal(f.calls.length, 1); assert.equal(f.manual.length, 1); assert.equal(f.listeners.size, 0);
});
test('wrong source/origin/nonce/app/error and caller selectors do not request a fresh launch', async () => {
  const f = fixture(); f.bind(); const hint = f.hint();
  for (const data of [null, 'hint', {...hint,nonce:'x'.repeat(43)}, {...hint,appId:'app-'+ 'c'.repeat(32)},
    {...hint,error:'apps_access_denied'}, {...hint,model:'caller'}, {...hint,launchBinding:binding}, {...hint,action:'launch'}]) f.message(data);
  for (const overrides of [{source:{}},{origin:'https://elsewhere.invalid'},{ports:[{}]}]) f.message(hint,overrides);
  await tick(); assert.equal(f.calls.length, 0); assert.equal(f.controller.attempted(), false);
  assert.equal(isAppBootRetryHint({...hint,accountId:'caller'}, {appId,nonce:hint.nonce}), false);
});
test('legacy or malformed provenance leaves boot/manual entry unchanged and never starts a recovery monitor', () => {
  for (const value of [null, {}, {...binding,policyEpoch:0}, {...binding,targetDigest:'wrong'}, {...binding,profile:'unknown'}]) {
    const f = fixture(); f.bind(value); assert.equal(f.sends.length, 0); assert.equal(f.listeners.size, 0);
  }
});
test('Back/dispose or replacement frame denies queued old hints before any recovery action', async () => {
  for (const cancel of [f=>f.controller.dispose(), f=>{f.state.active=false;}, f=>{f.state.frame={};}, f=>f.controller.cancel()]) {
    const f=fixture(); f.bind(); f.message(f.hint()); cancel(f); await tick(); assert.equal(f.calls.length,0);
  }
});
test('revocation followed by same-ID reactivation cannot restore an original screen generation', async () => {
  const f=fixture(); f.bind(); f.state.active=false; f.state.generation++; f.state.active=true; f.message(f.hint()); await tick();
  assert.equal(f.calls.length,0); assert.equal(f.controller.attempted(),false);
});
test('cancel during a pending fresh launch invalidates its post-ACK guard and suppresses old error UI', async () => {
  let settle; const f=fixture(async current=>{await new Promise(resolve=>{settle=resolve;});assert.equal(current(),false);throw Error('synthetic lost ACK');});
  f.bind(); f.message(f.hint()); await tick(); assert.equal(f.calls.length,1); f.controller.cancel(); settle(); await tick();
  assert.equal(f.manual.length,0); assert.equal(f.controller.attempted(),true);
});
test('lost ACK consumes the one budget and leaves manual recovery; it never silently retries again', async () => {
  const f=fixture(async()=>{throw Error('synthetic lost ACK');});f.bind();f.message(f.hint());await tick();assert.equal(f.calls.length,1);assert.equal(f.manual.length,1);
  f.bind();f.message(f.hint());await tick();assert.equal(f.calls.length,1);
});
test('a successful document navigation or bounded nonce deadline ends the boot watch', async () => {
  for (const close of [f=>f.state.frame.load(), f=>{for(const timer of [...f.timers.values()]){assert.equal(timer.delay,25000);timer.fn();}}]) {
    const f=fixture();f.bind();const old=f.hint();close(f);f.message(old);await tick();assert.equal(f.calls.length,0);assert.equal(f.listeners.size,0);
  }
});
test('an explicit manual launch pending takes precedence over a boot hint without consuming automatic budget', async () => {
  const f=fixture();f.bind();f.state.pending=true;f.message(f.hint());await tick();assert.equal(f.calls.length,0);assert.equal(f.controller.attempted(),false);
});
