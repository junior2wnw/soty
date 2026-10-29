import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import { watchDeviceEnrollment } from '../ui/device-enrollment.mjs';

// Exercise the actual panel's conditional render and Save callback. This port
// reflects markup attributes; it does not claim native browser input coverage.
class ElementPort {
  children = []; handlers = new Map(); attributes = new Map(); dataset = {}; value = ''; textContent = ''; isConnected = true; disabled = false;
  constructor(tagName) { this.tagName = tagName; }
  classList = { toggle: (name, enabled) => { this.className = enabled ? `${this.className || ''} ${name}` : (this.className || '').split(' ').filter(value => value !== name).join(' '); } };
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
const source = (await readFile(new URL('../ui/credential-forms.mjs', import.meta.url), 'utf8')).replace(/^export /gmu, '') + '\n' + (await readFile(new URL('../ui/index.mjs', import.meta.url), 'utf8'))
  .replace(/^import .*;\r?\n/gmu, '')
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
    module, document, setTimeout, clearTimeout, AbortController, location: { origin: 'https://panel.test' }, watchDeviceEnrollment, mountPlatformInvitations: () => ({ destroy() {} }),
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
    module, document, setTimeout, clearTimeout, AbortController, location: { origin: 'https://panel.test' }, watchDeviceEnrollment, confirm: () => true, mountPlatformInvitations: () => ({ destroy() {} }),
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


test('accepted contact offers a direct message only when the host supports it', async () => {
  const local={accountId:'synthetic-owner',profiles:[]}, selected=[];
  const client={async getLocalState(){return local;},async status(){return{accountId:local.accountId,label:'Owner'};},
    async contacts(){return{contacts:[{peerAccountId:'synthetic-peer',relationshipId:'synthetic-relation',label:'Synthetic peer'}],incoming:[],outgoing:[],invitations:[],blocked:[]};}};
  const active=panelFixture(client,{initialTab:'people',onContactMessage:contact=>selected.push(contact)});
  await until(()=>active.content.querySelector('[data-contacts]')?.children.length===1);
  const row=active.content.querySelector('[data-contacts]').children[0];
  const write=row.children.find(child=>child.textContent==='Написать');assert.ok(write);assert.equal(write.getAttribute('aria-label'),'Написать Synthetic peer');
  await write.handlers.get('click')({preventDefault(){}});
  assert.equal(selected.length,1);assert.equal(selected[0].peerAccountId,'synthetic-peer');
  assert.equal(selected[0].relationshipId,'synthetic-relation');assert.equal(selected[0].label,'Synthetic peer');
  assert.equal(active.dialog.open,false);
  const unsupported=panelFixture(client,{initialTab:'people'});
  await until(()=>unsupported.content.querySelector('[data-contacts]')?.children.length===1);
  assert.equal(unsupported.content.querySelector('[data-contacts]').children[0].children.some(child=>child.textContent==='Написать'),false);
  unsupported.panel.close();
});

function enrollmentClient() {
  const request = { requestId: 'enroll_' + 'q'.repeat(32), expiresAt: Date.now() + 60000 };
  let local = { accountId: 'automatic', deviceId: 'automatic-device', localRevision: 1, profiles: [], pendingEnrollment: null }, approved = false;
  const calls = { starts: 0, previews: 0, finishes: 0, passwords: 0 };
  return { calls, approve() { approved = true; },
    client: { getLocalState: async () => structuredClone(local),
      status: async () => { throw Error('QR entry needs no unrelated status call'); },
      startEnrollment: async () => { calls.starts++; local.pendingEnrollment = request; local.localRevision++; return request; },
      previewEnrollment: async () => { calls.previews++; return { ...request, status: approved ? 'approved' : 'pending', account: approved ? { accountId: 'existing', label: 'Old account' } : null }; },
      finishEnrollment: async () => { calls.finishes++; local = { ...local, accountId: 'existing', deviceId: 'joined-device', localRevision: local.localRevision + 1 }; return local; },
      loginWithCredential: async () => { calls.passwords++; throw Object.assign(Error('Bad password'), { code: 'credential_unavailable' }); },
    },
  };
}

test('compact entry shows its QR immediately and finishes after one remote approval without extra recipient buttons', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const identity = enrollmentClient(), f = panelFixture(identity.client, { initialTab: 'connect' }); t.after(() => f.panel.close());
  await until(() => identity.calls.previews === 1); await new Promise(resolve => setImmediate(resolve));
  assert.equal(identity.calls.starts, 1); assert.equal(identity.calls.finishes, 0);
  assert.equal(f.dialog.querySelector('nav').hidden, true);
  assert.equal(f.content.querySelector('[data-enroll]'), null);
  assert.equal(f.content.querySelector('[data-entry-status]').textContent, 'Ждём подтверждения на прежнем устройстве…');
  identity.approve(); t.mock.timers.tick(1200);
  await until(() => f.dialog.open === false); assert.equal(identity.calls.finishes, 1);
});

test('an incorrect password resumes the same QR without losing the login form or starting another request', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const identity = enrollmentClient(), f = panelFixture(identity.client, { initialTab: 'connect' }); t.after(() => f.panel.close());
  await until(() => identity.calls.previews === 1 && f.content.getAttribute('aria-busy') === 'false');
  const form = f.content.querySelectorAll('form').find(node => node.dataset.credentialKind === 'password');
  const [name, secret] = form.querySelectorAll('input'); name.value = 'my-login'; secret.value = 'not the correct password';
  await form.handlers.get('submit')({ preventDefault() {} });
  await until(() => identity.calls.previews >= 2); await new Promise(resolve => setImmediate(resolve));
  assert.equal(identity.calls.passwords, 1); assert.equal(identity.calls.starts, 1);
  assert.equal(name.value, 'my-login'); assert.equal(secret.value, ''); assert.equal(form.isConnected, true);
  assert.match(f.dialog.querySelector('.connect-message').textContent, /Не удалось войти/u);
  identity.approve(); t.mock.timers.tick(1200); await until(() => f.dialog.open === false);
  assert.equal(identity.calls.finishes, 1);
});

test('the approving device shows the bound account and needs only its one explicit confirmation', async () => {
  const calls = [], requestId = 'enroll_' + 's'.repeat(32);
  const client = { getLocalState: async () => ({ accountId: 'original-account', label: 'My account' }),
    status: async () => { throw Error('approval needs no unrelated status screen'); },
    inspectEnrollment: async () => ({ requestId, accountId: 'original-account', label: 'New device' }),
    approveEnrollment: async (...args) => { calls.push(args); } };
  const f = panelFixture(client, { initialIntent: { kind: 'device', id: requestId } });
  await until(() => f.content.querySelector('[data-approve]') && f.content.getAttribute('aria-busy') === 'false');
  assert.equal(f.dialog.querySelector('nav').hidden, true); assert.equal(calls.length, 0);
  await f.content.querySelector('[data-approve]').handlers.get('click')({ preventDefault() {} });
  assert.deepEqual(calls, [[requestId, 'original-account']]); assert.equal(f.dialog.open, false);
});
