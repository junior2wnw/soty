import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';

// Exercise the actual panel's conditional render and Save callback. This port
// reflects markup attributes; it does not claim native browser input coverage.
class ElementPort {
  children = []; handlers = new Map(); attributes = new Map(); dataset = {}; value = ''; textContent = ''; isConnected = true; disabled = false;
  constructor(tagName) { this.tagName = tagName; }
  set innerHTML(markup) {
    this.children = [];
    for (const match of markup.matchAll(/<([a-z][a-z0-9]*)\b([^>]*)>/giu)) {
      const node = new ElementPort(match[1]);
      for (const attribute of match[2].matchAll(/([^\s=]+)(?:="([^"]*)")?/gu)) node.setAttribute(attribute[1], attribute[2] ?? '');
      this.children.push(node);
    }
  }
  setAttribute(key, value) {
    this.attributes.set(key, String(value));
    if (key === 'value') this.value = String(value);
    if (key === 'class') this.className = String(value);
    if (key === 'maxlength') this.maxLength = Number(value);
    if (key.startsWith('data-')) this.dataset[key.slice(5)] = String(value);
  }
  getAttribute(key) { return this.attributes.get(key) ?? null; }
  querySelectorAll(selector) {
    return this.children.flatMap(child => {
      const matches = selector.startsWith('.') ? child.className?.split(' ').includes(selector.slice(1))
        : selector.startsWith('[') ? child.attributes.has(selector.slice(1, -1)) : child.tagName === selector;
      return [...(matches ? [child] : []), ...child.querySelectorAll(selector)];
    });
  }
  querySelector(selector) { return this.querySelectorAll(selector)[0] ?? null; }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = children; }
  addEventListener(type, callback) { this.handlers.set(type, callback); }
  showModal() { this.open = true; }
  close() { this.open = false; this.handlers.get('close')?.(); }
  remove() { this.isConnected = false; }
}
async function until(predicate) {
  const deadline = Date.now() + 2000;
  while (!predicate()) { assert.ok(Date.now() < deadline, 'panel rendered its current view'); await new Promise(resolve => setImmediate(resolve)); }
}
const source = (await readFile(new URL('../ui/index.mjs', import.meta.url), 'utf8'))
  .replace(/^import \{ mountPlatformInvitations \} from '\.\/platform-invitations.mjs';\r?\n/u, '')
  .replace(/^export /gmu, '');

test('fallback profile editor renders the 80-unit boundary and saves all 80 units through its real callback', { timeout: 5000 }, async () => {
  const document = { activeElement: null, body: new ElementPort('body'), createElement: tag => new ElementPort(tag) };
  const calls = [], projected = [], state = { accountId: 'synthetic-A', label: 'Original', profiles: [] };
  const client = {
    async getLocalState() { return { ...state }; }, async status() { return { accountId: state.accountId, label: state.label }; },
    async rename(label, expectedAccountId) { calls.push({ label, expectedAccountId }); state.label = label; },
  };
  const module = { exports: {} };
  vm.runInNewContext(`${source}\nmodule.exports.openConnectPanel = openConnectPanel;`, {
    module, document, setTimeout, clearTimeout, mountPlatformInvitations: () => ({ destroy() {} }),
  });
  const panel = module.exports.openConnectPanel({ client, onRename: async name => { projected.push(name); } });
  try {
    const dialog = document.body.children[0], content = dialog.querySelector('.connect-content');
    await until(() => content.querySelector('[data-name]') && content.getAttribute('aria-busy') === 'false');
    const input = content.querySelector('[data-name]'); assert.equal(input.maxLength, 80);
    input.value = 'я'.repeat(80);
    await content.querySelector('[data-name-save]').handlers.get('click')({ preventDefault() {} });
    assert.deepEqual(calls, [{ label: input.value, expectedAccountId: state.accountId }]); assert.deepEqual(projected, [input.value]);
    assert.equal(content.querySelector('[data-name]').value, input.value);
    assert.equal(dialog.querySelector('.connect-message').textContent, 'Имя сохранено.');
  } finally { panel.close(); }
});

function panelFixture(client, options = {}) {
  const document = { activeElement: null, body: new ElementPort('body'), createElement: tag => new ElementPort(tag) }, module = { exports: {} };
  vm.runInNewContext(`${source}\nmodule.exports.openConnectPanel = openConnectPanel;`, {
    module, document, setTimeout, clearTimeout, confirm: () => true, mountPlatformInvitations: () => ({ destroy() {} }),
  });
  const panel = module.exports.openConnectPanel({ client, bootstrapOnOpen: false, ...options });
  const dialog = document.body.children[0], content = dialog.querySelector('.connect-content');
  return { panel, dialog, content };
}

test('known-profile selector needs no status/recovery request and closes after the selected profile is opened', async () => {
  const calls = [], local = { accountId: 'A', profiles: [{ accountId: 'A', label: 'Current', active: true }, { accountId: 'B', label: 'Other', active: false }, { accountId: 'C', label: 'Revoked', revoked: true }] };
  const f = panelFixture({ async getLocalState() { return local; }, async status() { throw Error('selector must not wait for status'); },
    async switchProfile(id) { calls.push(id); local.accountId = id; } }, { initialTab: 'profiles' });
  await until(() => f.content.querySelector('[data-profile-list]') && f.content.getAttribute('aria-busy') === 'false');
  const profiles = f.content.querySelector('[data-profile-list]').children;
  assert.equal(profiles.length, 2); assert.equal(profiles[0].disabled, true);
  await profiles[1].handlers.get('click')({ preventDefault() {} });
  assert.deepEqual(calls, ['B']); assert.equal(f.dialog.open, false);
});

test('recovery and data copies have separate views and preserve the selected-account vault contract', async () => {
  const calls = [], client = { async getLocalState() { return { accountId: 'A', profiles: [] }; }, async status() { return { accountId: 'A', label: 'A', recovery: { verified: true } }; },
    async saveVault(payload, revision, expectedAccountId) { calls.push({ payload, expectedAccountId }); return { revision: 2 }; } };
  const recovery = panelFixture(client, { initialTab: 'recovery', snapshot: () => ({ value: 'copy' }) });
  await until(() => recovery.content.querySelector('[data-save-recovery]'));
  assert.equal(recovery.content.querySelector('[data-save]'), null); recovery.panel.close();
  const data = panelFixture(client, { initialTab: 'data', snapshot: () => ({ value: 'copy' }) });
  await until(() => data.content.querySelector('[data-save]') && data.content.getAttribute('aria-busy') === 'false');
  assert.equal(data.content.querySelector('[data-save-recovery]'), null);
  await data.content.querySelector('[data-save]').handlers.get('click')({ preventDefault() {} });
  assert.deepEqual(calls, [{ payload: { value: 'copy' }, expectedAccountId: 'A' }]); data.panel.close();
});
