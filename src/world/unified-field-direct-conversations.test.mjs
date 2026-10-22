import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFileSync } from 'node:fs';
import { resolve, sep } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createRequire } from 'node:module';

const root = process.env.SOTY_REVIEW_FIELD_ROOT
  ? pathToFileURL(resolve(process.env.SOTY_REVIEW_FIELD_ROOT) + sep) : new URL('../../', import.meta.url);
const ts = createRequire(new URL('package.json', root))('typescript');
const { fieldEntityKey } = await import(new URL('modules/field/contract.mjs', root));
const parse = (name, override) => ts.createSourceFile(name,
  readFileSync(override || new URL('src/world/' + name, root), 'utf8'), ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
const screen = parse('unified-field-screen.ts', process.env.SOTY_REVIEW_FIELD_SCREEN_SOURCE);
const app = parse('app.ts', process.env.SOTY_REVIEW_FIELD_APP_SOURCE);
const printer = ts.createPrinter(), print = (node, source) => printer.printNode(ts.EmitHint.Unspecified, node, source);
const field = screen.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'createUnifiedFieldScreen');
assert.ok(field?.body);
const actual = name => { const node = field.body.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === name); assert.ok(node, name); return print(node, screen); };
const owner = app.statements.find(node => ts.isClassDeclaration(node) && node.name?.text === 'WorldApplication');
const render = owner?.members.find(node => ts.isMethodDeclaration(node) && node.name?.getText(app) === 'renderUnifiedField');
assert.ok(render);
let binding;
function visit(node) {
  if (ts.isCallExpression(node) && node.expression.getText(app) === 'createUnifiedFieldScreen') binding = node.arguments[0];
  ts.forEachChild(node, visit);
}
visit(render); assert.ok(binding && ts.isObjectLiteralExpression(binding));
const property = name => { const entry = binding.properties.find(node => ts.isPropertyAssignment(node) && node.name.getText(app) === name); assert.ok(entry, name); return print(entry.initializer, app); };
const code = ts.transpileModule([
  'const options = { accountId: deskAccount, directMessagesEnabled: ' + property('directMessagesEnabled') + ', onOpen: ' + property('onOpen') + ' };',
  ...['closePreview', 'openPreview', 'inspect', 'activate'].map(actual),
  'Object.assign(exports, { activate, inspect, preview: () => preview });',
].join('\n'), { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;

// Only DOM, field snapshot and existing consumer ports are controlled. Routing,
// confirmation checks, preview labels and the app's note-save/current gate are actual Source.
function harness(kind = 'person', { version = 1, state = 'active', mode = 'mine' } = {}) {
  const calls = { direct: [], groups: [], accounts: [], notes: 0, focus: 0, pending: null };
  let live = true, defer = false;
  const document = { activeElement: null };
  class NodePort {
    children = []; attributes = new Map(); handlers = new Map(); dataset = {}; isConnected = true; disabled = false;
    classList = { add() {} };
    constructor(tag = 'div', className = '', textContent = '') { Object.assign(this, { tag, className, textContent }); }
    append(...nodes) { this.children.push(...nodes); }
    prepend(...nodes) { this.children.unshift(...nodes); }
    setAttribute(name, value) { this.attributes.set(name, String(value)); }
    addEventListener(type, listener) { this.handlers.set(type, listener); }
    contains(node) { return node === this || this.children.some(child => child.contains?.(node)); }
    remove() { this.isConnected = false; }
    focus() { document.activeElement = this; calls.focus++; }
    getClientRects() { return this.isConnected ? [{}] : []; }
    click() { if (!this.disabled) this.onClick?.(); }
  }
  const node = (tag, className, text) => new NodePort(tag, className, text);
  const item = { entity: { kind, id: 'peer_001' }, title: 'Тестовая цель', source: 'member' };
  const record = kind === 'person' ? { kind: 'contact', relationshipId: 'relation_001', peerAccountId: item.entity.id, label: item.title, createdAt: 1 }
    : { communityId: item.entity.id, membership: { state }, previewMembers: [], memberCount: 2 };
  const key = fieldEntityKey(item.entity), records = new Map([[key, record]]);
  const element = node('section'), fieldControl = node('button');
  element.querySelector = () => fieldControl; document.activeElement = fieldControl;
  const environment = {
    exports: {}, mode, current: () => live, isCurrent: () => live, deskAccount: 'account_001',
    profile: { directMessagesVersion: version }, metadata: new Map([[key, item]]), mineKnownRefs: new Set([key]),
    mineGeneration: 2, mineConfirmedGeneration: 2, summary: { arranging: false },
    directory: { getRecord: ref => records.get(fieldEntityKey(ref)) || null },
    fieldEntityKey, element, document, HTMLElement: NodePort, searchInput: node('input'),
    preview: null, previewOpener: null, selected: null, signal: new AbortController().signal,
    engine: { snapshot: () => ({ contexts: [], shortcuts: [] }) }, CSS: { escape: value => value },
    getComputedStyle: () => ({ visibility: 'visible' }), focusFieldControl: () => fieldControl.focus(),
    el: node, button: (label, symbol, className, onClick) => Object.assign(node('button', className, label), { onClick }),
    iconButton: (label, symbol, onClick) => Object.assign(node('button', '', label), { onClick }),
    avatar: () => node('span'), icon: () => node('svg'), kindLabel: value => value, nounCount: value => String(value),
    run() { throw new Error('No membership mutation permitted by this test'); }, addToContext() {}, updateMineFilter() {},
    afterNoteSaved(action) { calls.notes++; if (defer) calls.pending = action; else action(); },
    openDirectContact: contact => calls.direct.push(contact), openGroup: (id, tab) => calls.groups.push({ id, tab }),
    runHook: action => action(), options: { openAccount: tab => calls.accounts.push(tab) },
  };
  vm.createContext(environment); new vm.Script(code).runInContext(environment);
  const all = node => [node, ...node.children.flatMap(all)];
  return { environment, item, record, records, key, calls, fieldControl, document,
    tap: () => environment.exports.activate(item, 'shortcut_001'),
    inspect: () => environment.exports.inspect(item, 'shortcut_001'),
    preview: () => environment.exports.preview(),
    buttons: () => all(environment.exports.preview()).filter(node => node.tag === 'button'),
    retire() { live = false; }, defer() { defer = true; } };
}

test('confirmed Mine contact reaches the existing guarded DM consumer with its exact relationship in one activation', () => {
  const h = harness(); h.tap();
  assert.equal(h.calls.notes, 1); assert.equal(h.calls.direct.length, 1);
  assert.equal(h.calls.direct[0], h.record); assert.equal(h.calls.accounts.length, 0);
  assert.equal(h.preview(), null); assert.equal(h.calls.focus, 0);
});

test('joined Mine room requests the existing chat consumer, while every unjoined state remains a preview', () => {
  const h = harness('community'); h.tap();
  assert.equal(h.calls.groups.length, 1); assert.equal(h.calls.groups[0].id, h.item.entity.id);
  assert.equal(h.calls.groups[0].tab, 'chat'); assert.equal(h.preview(), null);
  for (const state of ['invited', 'requested', 'banned', 'left', undefined]) {
    const h = harness('community', { state }); h.record.membership.state = state; h.tap();
    assert.equal(h.calls.groups.length, 0, String(state)); assert.ok(h.preview(), String(state));
  }
});

test('World, arranging, unconfirmed generation and unknown entity retain preview instead of direct consumption', () => {
  for (const kind of ['person', 'community']) for (const condition of ['world', 'arranging', 'generation', 'unknown']) {
    const h = harness(kind);
    if (condition === 'world') h.environment.mode = 'search';
    if (condition === 'arranging') h.environment.summary.arranging = true;
    if (condition === 'generation') h.environment.mineConfirmedGeneration--;
    if (condition === 'unknown') h.environment.mineKnownRefs.clear();
    h.tap(); assert.ok(h.preview(), kind + ':' + condition);
    assert.equal(h.calls.direct.length + h.calls.groups.length, 0);
  }
});

test('missing, replaced and mismatched records cannot take a direct route; disabled DM keeps the Contact preview', () => {
  for (const kind of ['person', 'community']) for (const condition of ['missing', 'replaced', 'mismatched']) {
    const h = harness(kind);
    if (condition === 'missing') h.records.clear();
    if (condition === 'replaced') h.records.set(h.key, kind === 'person' ? { profileId: h.item.entity.id } : { communityId: h.item.entity.id, previewMembers: [] });
    if (condition === 'mismatched') h.record[kind === 'person' ? 'peerAccountId' : 'communityId'] = 'other_001';
    h.tap(); assert.ok(h.preview()); assert.equal(h.calls.direct.length + h.calls.groups.length, 0);
  }
  const disabled = harness('person', { version: 0 }); disabled.tap();
  assert.ok(disabled.preview()); assert.ok(disabled.buttons().some(node => node.textContent === 'Контакт'));
  assert.equal(disabled.calls.direct.length, 0);
  const own = harness(); own.item.entity.id = own.environment.deskAccount; own.record.peerAccountId = own.item.entity.id;
  const ownKey = fieldEntityKey(own.item.entity); own.environment.metadata.set(ownKey, own.item);
  own.environment.mineKnownRefs.add(ownKey); own.records.set(ownKey, own.record); own.tap();
  assert.ok(own.buttons().some(node => node.textContent === 'Редактировать профиль'));
  assert.equal(own.calls.direct.length, 0);
});

test('stale screen activation and deferred note-save callback cannot cross account lifetime', () => {
  const stale = harness(); stale.retire(); stale.tap();
  assert.equal(stale.calls.notes, 0); assert.equal(stale.preview(), null);
  const pending = harness(); pending.defer(); pending.tap(); assert.equal(pending.calls.direct.length, 0);
  assert.equal(typeof pending.calls.pending, 'function'); pending.retire(); pending.calls.pending();
  assert.equal(pending.calls.direct.length, 0);
});

test('inspect still exposes placement actions and honest Write label, and Escape restores the field control', () => {
  const h = harness(); h.inspect(); const panel = h.preview(); assert.ok(panel);
  const write = h.buttons().find(node => node.textContent === 'Написать'); assert.ok(write);
  assert.equal(write.attributes.get('aria-label'), 'Написать');
  assert.ok(h.buttons().some(node => node.textContent === 'Переместить'));
  assert.ok(h.buttons().some(node => node.textContent === 'Убрать с поля'));
  assert.equal(h.calls.direct.length, 0);
  let stopped = false; panel.handlers.get('keydown')({ key: 'Escape', stopPropagation() { stopped = true; } });
  assert.equal(stopped, true); assert.equal(h.preview(), null); assert.equal(h.document.activeElement, h.fieldControl);
});
