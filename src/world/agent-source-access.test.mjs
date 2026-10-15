import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import ts from 'typescript';

const source = await readFile(new URL('./agent-source-access.ts', import.meta.url), 'utf8');
const compiled = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 } }).outputText;
const tick = () => new Promise(resolve => setImmediate(resolve));
// Actual UI controller, deterministic DOM ports. Not a claim of browser visual QA.
class Element {
  children = []; listeners = new Map(); disabled = false; value = ''; checked = false; required = false; textContent = '';
  constructor(tag, text = '') { this.tag = tag; this.textContent = text; }
  append(...children) { this.children.push(...children); for (const child of children) child.parent = this; }
  replaceChildren(...children) { this.children = []; this.append(...children); }
  setAttribute() {}
  addEventListener(name, fn) { this.listeners.set(name, fn); }
  remove() { if (this.parent) this.parent.children = this.parent.children.filter(child => child !== this); }
  all(tag) { return this.children.flatMap(child => [...(child.tag === tag ? [child] : []), ...child.all(tag)]); }
  reportValidity() { return this.all('input').every(input => input.disabled || !input.required || input.type !== 'checkbox' || input.checked); }
  event(name) { if (!this.disabled) this.listeners.get(name)?.({ preventDefault() {} }); }
}
function fixture(t, call) {
  const make = (tag, _class = '', text = '') => new Element(tag, text);
  const ports = { './dom': { el: make, labeledField(title, control) { const label = make('label', '', title); label.append(control); return label; },
    button(title, _icon, _class, action) { const node = make('button', '', title); if (action) node.addEventListener('click', action); return node; } } };
  const module = { exports: {} }, host = make('main'); let uuid = 0;
  vm.runInNewContext(compiled, { module, exports: module.exports, require: name => ports[name], Date,
    document: { createTextNode: text => make('text', '', text) }, crypto: { randomUUID: () => `synthetic-request-${++uuid}` } });
  const ui = module.exports.mountAgentSourceAccess(host, { accountId: 'account-current', api: { request: call } }); t.after(() => ui.dispose());
  return { host, ui };
}
const app = { appId: 'app-test', title: '<img src=x onerror=alert(1)>', proposalDigest: 'a'.repeat(64), resources: ['workspace:app-test:own'], recipients: ['workspace:app-test:own'],
  functions: [{ reference: { capabilityId: 'read', version: 1, digest: 'b'.repeat(64) }, title: 'Read own', effects: [] },
    { reference: { capabilityId: 'write', version: 1, digest: 'c'.repeat(64) }, title: 'Create own', effects: ['create'] }], status: { state: 'none', mode: null, expiresAt: null } };

test('explicit generic consent captures own account and stable lost-ACK retry; labels remain text', async t => {
  const calls = []; let attempts = 0;
  const f = fixture(t, async (op, args) => {
    calls.push({ op, args: structuredClone(args) });
    if (op === 'agent.sources.list') return { accountId: 'account-current', deviceId: 'device', apps: [app] };
    if (++attempts === 1) throw Error('lost_ack');
    return { appId: app.appId, status: { state: 'active' } };
  });
  await tick(); const form = f.host.all('form')[0], [mode, duration] = form.all('select'), inputs = form.all('input');
  mode.value = 'write'; duration.value = '3600'; inputs[0].value = '12';
  form.event('submit'); await tick(); assert.equal(calls.length, 1, 'no implicit consent');
  inputs[1].checked = true; form.event('submit'); await tick(); await f.ui.flush();
  assert.equal(calls[1].args.expectedAccountId, 'account-current'); assert.equal(calls[1].args.mode, 'write'); assert.equal(calls[1].args.budget, 12);
  assert.equal(mode.disabled, true); assert.equal(f.host.all('img').length, 0);
  form.event('submit'); await tick(); await f.ui.flush();
  assert.deepEqual(calls[2], calls[1], 'unknown outcome never creates a fresh consent request');
  assert.equal(JSON.stringify(calls).includes('token'), false);
});

test('foreign account response and late disposed response never expose consent controls', async t => {
  const f = fixture(t, async () => ({ accountId: 'other-account', deviceId: 'other', apps: [app] }));
  await tick(); assert.equal(f.host.all('form').length, 0);
  let release; const held = new Promise(resolve => { release = resolve; });
  const g = fixture(t, () => held); g.ui.dispose(); release({ accountId: 'account-current', deviceId: 'device', apps: [app] });
  await tick(); assert.equal(g.host.all('form').length, 0);
});
import { fixture as parentFixture, error as parentError, deferred as parentDeferred, settle as parentSettle } from './test-support/access-panel.mjs';

test('Source profile change closes the actual parent and a late sibling response cannot revive it', async t => {
  const late = parentDeferred();
  const f = parentFixture(t, { handlers: {
    'agent.sources.list': () => { throw parentError('ACTIVE_PROFILE_CHANGED'); },
    'access.principals.list': () => late.promise,
  } });
  await parentSettle();
  assert.ok(f.host.textContent.includes('Доступ к аккаунту изменился'));
  late.resolve({ principals: [], cursor: null }); await parentSettle();
  assert.ok(f.host.textContent.includes('Доступ к аккаунту изменился'));
  assert.equal(f.host.querySelectorAll('form').length, 0);
});

test('parent identity failure disposes a still pending Source response', async t => {
  const late = parentDeferred();
  const f = parentFixture(t, { handlers: {
    'agent.sources.list': () => late.promise,
    'access.principals.list': () => { throw parentError('account_mismatch'); },
  } });
  await parentSettle();
  late.resolve({ accountId: 'account-current', deviceId: 'device', apps: [app] }); await parentSettle();
  assert.ok(f.host.textContent.includes('Доступ к аккаунту изменился'));
  assert.equal(f.host.querySelectorAll('form').length, 0);
});
