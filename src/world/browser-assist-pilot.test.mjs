import test from 'node:test';
import assert from 'node:assert/strict';
import { webcrypto } from 'node:crypto';
import { createBrowserAssistHost, pumpBrowserAssist } from './browser-assist-pilot.mjs';
import { ASSIST_SCHEMA } from '../../modules/source-app/browser/assist-wire.mjs';
const binding = { appId: 'app-' + 'a'.repeat(32), origin: 'https://app.test', targetRevision: 1, targetDigest: 'b'.repeat(64), generation: 1, channel: 'c'.repeat(32) };
test('host is off by default; deny, stale app during approval, revoke while pending; wrong origin ignored', async () => {
  const listeners = new Set(), sent = []; let current = structuredClone(binding), permit = async () => false, hold;
  const frame = { postMessage(data) {
    sent.push(data); if (data.revoke || hold) return;
    queueMicrotask(() => {
      const result = { ok: true, data: { snapshot: 'd'.repeat(32), items: [{ id: 'n0', kind: 'click', label: 'Add task' }], untrusted: true } };
      for (const fn of listeners) fn({ source: frame, origin: 'https://evil.test', data: { schema: ASSIST_SCHEMA, channel: binding.channel, requestId: data.requestId, result } });
      for (const fn of listeners) fn({ source: frame, origin: binding.origin, data: { schema: ASSIST_SCHEMA, channel: binding.channel, requestId: data.requestId, result } });
    });
  } };
  const view = { crypto: webcrypto, setTimeout, clearTimeout, addEventListener: (_, fn) => listeners.add(fn), removeEventListener: (_, fn) => listeners.delete(fn) };
  const host = createBrowserAssistHost({ view, readPeer: () => ({ window: frame, binding: current }), approve: c => permit(c) });
  await assert.rejects(host.dispatch({ kind: 'observe' })); assert.equal(sent.length, 0);
  host.enable(); await host.dispatch({ kind: 'observe' });
  await assert.rejects(host.dispatch({ kind: 'observe' }, undefined, async () => { throw Error('account_changed'); }), /account_changed/u); assert.equal(sent.length, 1);
  const click = { kind: 'click', target: 'n0', snapshot: 'd'.repeat(32) };
  await assert.rejects(host.dispatch(click), /approval_denied/u); assert.equal(sent.length, 1);
  let accept; permit = async command => { assert.equal(command.label, 'Add task'); return new Promise(r => { accept = r; }); };
  const pending = host.dispatch(click); current = { ...current, generation: 2 }; accept(true);
  await assert.rejects(pending, /permission_closed/u); assert.equal(sent.length, 1);
  current = structuredClone(binding); host.enable(); hold = true;
  const observe = host.dispatch({ kind: 'observe' }); host.revoke();
  await assert.rejects(observe, /cancelled/u); assert.equal(listeners.size, 0); assert.equal(host.binding(), null);
  host.close(); assert.throws(() => host.enable());
});
test('poll repeats original command after lost ACK without executing DOM effect twice', async () => {
  const abort = new AbortController(); let executions = 0, replies = 0;
  const host = { binding: () => binding, async dispatch() { executions++; return { ok: true, data: { untrusted: true, performed: true } }; } };
  const client = { async extension(op, args, context) {
    assert.equal(context.expectedAccountId, 'owner');
    if (op === 'agent.browser.poll') return { commandId: 'e'.repeat(32), command: { kind: 'click', snapshot: 'd'.repeat(32), target: 'n0' } };
    assert.equal(args.result.ok, true); if (++replies === 2) abort.abort(); return { accepted: true };
  } };
  await pumpBrowserAssist({ client, accountId: 'owner', requestId: 'turn1234', binding, host, signal: abort.signal, assertCurrent: async () => {} });
  assert.equal(executions, 1); assert.equal(replies, 2);
});


test('authority revoked while approving denies the effect at the fresh signed poll', async () => {
  const abort = new AbortController(); let effects = 0, polls = 0;
  const host = { binding: () => binding, async dispatch(command, signal, guard) { await guard(); effects++; return { ok: true, data: { untrusted: true } }; } };
  const client = { async extension(op, args) {
    if (op === 'agent.browser.poll') { if (++polls > 1) throw Error('agent_source_denied');
      return { commandId: 'e'.repeat(32), command: { kind: 'click', snapshot: 'd'.repeat(32), target: 'n0' } }; }
    assert.equal(args.result.ok, false); abort.abort(); return { accepted: true };
  } };
  await pumpBrowserAssist({ client, accountId: 'owner', requestId: 'turn1234', binding, host, signal: abort.signal, assertCurrent: async () => {} });
  assert.equal(effects, 0); assert.equal(polls, 2);
});
