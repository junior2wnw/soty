import test from 'node:test';
import assert from 'node:assert/strict';
import { createRelayIdentityBridge } from './app-relay-identity-bridge.mjs';

const tick = () => new Promise(done => setTimeout(done, 0));
function fixture({ request, close } = {}) {
  let listener, time = 0, active = true, frame = makeFrame(); const calls = [], closed = [];
  const view = { performance: { now: () => time }, setTimeout, clearTimeout,
    addEventListener(type, value) { assert.equal(type, 'message'); listener = value; }, removeEventListener() { listener = null; } };
  const entry = { appId: 'app-' + '1'.repeat(32), domainId: 'dom_' + '2'.repeat(32), origin: 'https://app.example' };
  const binding = { schema: 'soty.app-launch-binding.v1', policyEpoch: 1, targetRevision: 1, targetDigest: 'a'.repeat(64), profile: 'soty.relay-restricted.v1', bindingFloor: 2 };
  const bridge = createRelayIdentityBridge({ view, isCurrent: () => active, getFrame: () => frame, getEntry: () => entry, getBinding: () => binding,
    request: async value => { calls.push(value); return request ? request(value) : { schema: 'soty.relay-window-grant.v1', grant: 'A'.repeat(43), expiresAt: 100 }; },
    close: async value => { closed.push(value); return close ? close(value) : { closed: true }; } });
  function message(source = frame.contentWindow, origin = entry.origin) { listener?.({ source, origin, data: { schema: 'soty.relay-window-identity.v1', type: 'request', requestId: 'a'.repeat(36) } }); }
  return { bridge, calls, closed, entry, message, frame: () => frame, replace() { frame.isConnected = false; frame = makeFrame(); },
    rejectGrant(grant, source = frame.contentWindow, origin = entry.origin) { listener?.({ source, origin, data: { schema: 'soty.relay-window-identity.v1', type: 'invalidate', grant } }); },
    invalidate() { active = false; }, advance(ms) { time += ms; } };
}
function makeFrame() { return { isConnected: true, contentWindow: { sent: [], postMessage(value, origin) { this.sent.push({ value, origin }); } } }; }

test('only the exact current frame and exact origin get a grant, reused briefly', async () => {
  const f = fixture(); f.message({}); f.message(f.frame().contentWindow, 'https://other.example'); await tick(); assert.equal(f.calls.length, 0);
  f.message(); f.message(); await tick(); assert.equal(f.calls.length, 1); assert.equal(f.frame().contentWindow.sent.length, 2);
  assert.equal(f.frame().contentWindow.sent[0].value.grant, 'A'.repeat(43));
  f.advance(20001); f.message(); await tick(); assert.equal(f.calls.length, 2); f.bridge.dispose();
});

test('replacing a frame closes its grant and the old window cannot obtain another', async () => {
  const f = fixture(), old = f.frame(); f.message(); await tick(); await f.bridge.clear();
  assert.equal(f.closed.length, 1); f.replace(); f.bridge.resume(); f.message(old.contentWindow); f.message(); await tick();
  assert.equal(old.contentWindow.sent.length, 1); assert.equal(f.frame().contentWindow.sent.length, 1); assert.equal(f.calls.length, 2); f.bridge.dispose();
});

test('account change or frame replacement during issue never delivers the response', async () => {
  let release; const pending = new Promise(done => { release = done; }), f = fixture({ request: () => pending });
  const old = f.frame(); f.message(); await tick(); f.replace(); release({ schema: 'soty.relay-window-grant.v1', grant: 'B'.repeat(43), expiresAt: 100 }); await tick();
  assert.equal(old.contentWindow.sent.length, 0); assert.equal(f.frame().contentWindow.sent.length, 0);
  await f.bridge.clear(); assert.equal(f.closed.length, 1); f.bridge.dispose();
});

test('a failed close blocks replacement until an explicit later close succeeds', async () => {
  let fails = true; const f = fixture({ close: () => { if (fails) throw Error('offline'); return { closed: true }; } });
  f.message(); await tick(); await assert.rejects(f.bridge.clear()); f.bridge.resume(); assert.equal(f.closed.length, 1);
  fails = false; await f.bridge.clear(); assert.equal(f.closed.length, 2); f.bridge.dispose();
});

test('a delayed grant remains revocable until the latest possible server expiry', async () => {
  let release; const pending = new Promise(done => { release = done; }), f = fixture({ request: () => pending });
  f.message(); await tick(); f.advance(15000);
  release({ schema: 'soty.relay-window-grant.v1', grant: 'C'.repeat(43), expiresAt: 100 }); await tick();
  f.advance(16000); await f.bridge.clear();
  assert.equal(f.closed.length, 1, 'the server may have issued the grant just before the delayed response');
  f.bridge.dispose();
});

test('only the current iframe can evict its own rejected cached grant', async () => {
  const f = fixture(); f.message(); await tick(); assert.equal(f.calls.length, 1);
  f.rejectGrant('A'.repeat(43), {}); f.rejectGrant('A'.repeat(43), f.frame().contentWindow, 'https://other.example');
  f.rejectGrant('B'.repeat(43)); f.message(); await tick(); assert.equal(f.calls.length, 1);
  f.rejectGrant('A'.repeat(43)); f.message(); await tick(); assert.equal(f.calls.length, 2); f.bridge.dispose();
});
