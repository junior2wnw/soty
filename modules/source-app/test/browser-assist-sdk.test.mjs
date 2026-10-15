import test from 'node:test';
import assert from 'node:assert/strict';
import { webcrypto } from 'node:crypto';
import { createSourceBrowserAssist } from '../browser/assist-sdk.mjs';
import { ASSIST_SCHEMA } from '../browser/assist-wire.mjs';
function fixture() {
  const listeners = new Map(), replies = [], parent = { postMessage: value => replies.push(value) }; let clicks = 0;
  const node = (tagName, type, label, extra = {}) => ({ tagName, type, isConnected: true, disabled: false, readOnly: false, autocomplete: '', name: '', id: '',
    closest: () => null, getClientRects: () => [1], getAttribute: () => label, click: () => clicks++, dispatchEvent() {}, ...extra });
  const nodes = [node('BUTTON', 'button', 'Add'), node('INPUT', 'text', 'Task'), node('INPUT', 'password', 'Password'), node('INPUT', 'file', 'File'),
    node('INPUT', 'text', 'Token', { name: 'secret_token' }), node('BUTTON', 'submit', 'Purchase'), node('INPUT', 'text', 'Hidden', { closest: () => ({}) })];
  const observer = { observe() {}, disconnect() {}, takeRecords: () => [] };
  const view = { crypto: webcrypto, parent, location: { origin: 'https://app.test' }, document: { documentElement: {}, querySelectorAll: () => nodes },
    MutationObserver: function () { return observer; }, Event: class {}, getComputedStyle: () => ({ visibility: 'visible' }),
    addEventListener: (k, f) => listeners.set(k, f), removeEventListener: k => listeners.delete(k) };
  const binding = { appId: 'app-' + 'a'.repeat(32), origin: 'https://app.test', targetRevision: 1, targetDigest: 'b'.repeat(64), generation: 1, channel: 'c'.repeat(32) };
  const sdk = createSourceBrowserAssist({ appId: binding.appId, parentOrigin: 'https://root.test', view });
  const send = (command, overrides = {}) => listeners.get('message')?.({ source: parent, origin: 'https://root.test', data: { schema: ASSIST_SCHEMA, binding, requestId: 'd'.repeat(32), command }, ...overrides });
  return { sdk, nodes, binding, send, replies, parent, listeners, clicks: () => clicks };
}
test('author opt-in; wrong origin/window/app; restricted observation; exact replay and stale snapshot', () => {
  const f = fixture(); f.send({ kind: 'observe' }); assert.equal(f.replies.length, 0);
  f.sdk.setEnabled(true);
  f.send({ kind: 'observe' }, { origin: 'https://evil.test' });
  f.send({ kind: 'observe' }, { source: {} });
  f.send({ kind: 'observe' }, { data: { schema: ASSIST_SCHEMA, binding: { ...f.binding, appId: 'app-' + 'e'.repeat(32) }, requestId: 'd'.repeat(32), command: { kind: 'observe' } } });
  assert.equal(f.replies.length, 0);
  f.send({ kind: 'observe' }); const data = f.replies[0].result.data;
  assert.deepEqual(data.items.map(x => x.label), ['Add', 'Task']);
  assert.equal(JSON.stringify(data).includes('value'), false);
  const click = { schema: ASSIST_SCHEMA, binding: f.binding, requestId: 'f'.repeat(32), command: { kind: 'click', snapshot: data.snapshot, target: 'n0' } };
  f.send(null, { data: click }); f.send(null, { data: click }); assert.equal(f.clicks(), 1);
  f.send(null, { data: { ...click, requestId: 'e'.repeat(32) } }); assert.equal(f.clicks(), 1);
  f.sdk.setEnabled(false); f.send(null, { data: click }); assert.equal(f.clicks(), 1);
  f.sdk.setEnabled(true); f.send(null, { data: click }); assert.equal(f.clicks(), 1); // Retired consent cannot reopen.
  f.sdk.close(); assert.equal(f.listeners.has('message'), false);
});


test('fill changes only the labelled safe input and consumes its snapshot', () => {
  const f = fixture(); f.sdk.setEnabled(true); f.send({ kind: 'observe' });
  const data = f.replies[0].result.data;
  const fill = { schema: ASSIST_SCHEMA, binding: f.binding, requestId: 'f'.repeat(32), command: { kind: 'fill', snapshot: data.snapshot, target: 'n1', text: 'Own draft' } };
  f.send(null, { data: fill }); assert.equal(f.nodes[1].value, 'Own draft');
  f.send(null, { data: { ...fill, requestId: 'e'.repeat(32), command: { ...fill.command, text: 'stale overwrite' } } });
  assert.equal(f.nodes[1].value, 'Own draft'); assert.equal(f.nodes[2].value, undefined); f.sdk.close();
});


test('probe reports author consent only to exact parent, without DOM data', () => {
  const f = fixture(), probe = { schema: ASSIST_SCHEMA, probe: 'e'.repeat(32), appId: f.binding.appId };
  f.send(null, { origin: 'https://evil.test', data: probe }); assert.equal(f.replies.length, 0);
  f.send(null, { data: probe }); assert.deepEqual(f.replies.pop(), { schema: ASSIST_SCHEMA, probe: probe.probe, enabled: false });
  f.sdk.setEnabled(true); f.send(null, { data: probe }); assert.equal(f.replies.pop().enabled, true); f.sdk.close();
});
