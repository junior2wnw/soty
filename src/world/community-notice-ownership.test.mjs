import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import vm from 'node:vm';
import ts from 'typescript';

// Execute actual App account/openGroup/toast methods and the actual successful
// community-form response callback. DOM, field IO and timers are explicit ports;
// this is not browser geometry, audio, phone or account-service acceptance.
const source = readFileSync(new URL('./app.ts', import.meta.url), 'utf8');
const ast = ts.createSourceFile('app.ts', source, ts.ScriptTarget.Latest, true), printer = ts.createPrinter();
const appClass = ast.statements.find(node => ts.isClassDeclaration(node) && node.name?.text === 'WorldApplication');
const node = name => appClass.members.find(member => member.name?.getText(ast) === name);
const print = value => printer.printNode(ts.EmitHint.Unspecified, value, ast);
const method = name => print(node(name));
function find(root, predicate) {if (predicate(root)) return root; let found; ts.forEachChild(root, child => {found ??= find(child, predicate);}); return found;}
const response = find(node('openCommunityForm'), value => ts.isCallExpression(value) && ts.isPropertyAccessExpression(value.expression)
  && value.expression.name.text === 'then' && ts.isArrowFunction(value.arguments[0])
  && Boolean(find(value.arguments[0], item => ts.isVariableDeclaration(item) && item.name.getText(ast) === 'opening')))?.arguments[0];
assert.ok(response, 'the real form response continuation must exist');
const callback = print(response), toastGuard = node('toast').body.statements.find(ts.isIfStatement);
function replaceOnce(value, from, to) {assert.equal(value.split(from).length, 2, 'a BROKEN control must change exactly one production guard'); return value.replace(from, to);}
const compile = code => ts.transpileModule(code, {compilerOptions: {target: ts.ScriptTarget.ES2022}}).outputText;
const deferred = () => {let resolve; const promise = new Promise(done => {resolve = done;}); return {promise, resolve};};
const group = id => ({communityId: id, name: id, membership: {state: 'active'}, permissions: {canWrite: true}});

class ElementPort {
  children = []; parent = null; dataset = {}; attributes = {}; isConnected = true; hidden = false; inert = false; rects = true; removed = 0;
  constructor(tag = 'div', className = '', textContent = '') {Object.assign(this, {tag, className, textContent});}
  classList = {add: name => {this.className += ' ' + name;}};
  append(...children) {for (const child of children) {child.parent = this; this.children.push(child);}}
  setAttribute(name, value) {this.attributes[name] = value;}
  getClientRects() {return this.rects ? [{}] : [];}
  closest() {for (let current = this; current; current = current.parent) if (current.hidden || current.inert) return current; return null;}
  querySelector(selector) {
    const owner = /^\[data-notice-owner="([A-Za-z0-9_-]+)"\]$/u.exec(selector);
    for (const child of this.children) {
      if (owner ? child.dataset.noticeOwner === owner[1] : selector === '.sw-toast' && child.className.split(' ').includes('sw-toast')) return child;
      const found = child.querySelector(selector); if (found) return found;
    }
    return null;
  }
  remove() {this.removed++; if (this.parent) this.parent.children = this.parent.children.filter(value => value !== this); this.parent = null; this.isConnected = false;}
}

function fixture({responseCode = callback, toastCode = method('toast'), existing = false, hold = true} = {}) {
  const pending = deferred(), timers = new Map(), cleared = [], updates = [], toastCalls = []; let timerId = 0, held = false;
  const root = new ElementPort(), a = new ElementPort(), b = new ElementPort(); a.dataset.noticeOwner = 'room_A'; b.dataset.noticeOwner = 'room_B'; root.append(a, b);
  const context = vm.createContext({root, CSS: {escape: value => value},
    el: (tag, className, text) => new ElementPort(tag, className, text),
    button: (label, _symbol, className, run) => Object.assign(new ElementPort('button', className, label), {run}),
    iconButton: (label, _symbol, run) => Object.assign(new ElementPort('button', '', label), {run}),
    setTimeout(run, delay) {const id = ++timerId; timers.set(id, {run, delay}); return id;}, clearTimeout(id) {cleared.push(id); timers.delete(id);},
    errorText: () => 'Недоступно',
  });
  vm.runInContext(compile(`class Probe {${method('accountTask')}\n${method('hideMainApplication')}\n${method('isRetainedMain')}\n${method('openGroup')}\n${toastCode}};globalThis.Probe=Probe;`), context);
  const app = new context.Probe();
  Object.assign(app, {root, deskAccount: 'account_A', accountGeneration: 1, destroyed: false, communitySurface: null, communityOpenTicket: 0, communityPending: 0,
    activeRoute: '#mine', groupTab: 'about', group: null, selected: null,
    unifiedField: {openCommunity(id) {if (hold && id === 'room_A' && !held) {held = true; return pending.promise;} return Promise.resolve(group(id));}},
    updateCommunity: value => updates.push(value.communityId), remember() {}, writeCommunityRoute() {}, closeCommunityDialogs() {}, openCommunityApplications() {},
    communityFocusChangedAfterFailure() {this.communitySurface = null;},
  });
  const actualToast = app.toast;
  app.toast = function (...args) {toastCalls.push(args); return actualToast.apply(this, args);};
  const dialog = {element: {open: true}, close() {this.element.open = false;}};
  context.app = app; context.dialog = dialog; context.existing = existing; context.returnTab = 'about'; context.chat = false;
  vm.runInContext(compile(`function bind(){const accountCurrent=this.accountTask();return ${responseCode};}globalThis.invoke=bind.call(app);`), context);
  const seed = (host = b) => {const notice = new ElementPort('div', 'sw-toast', 'Текущее уведомление'); host.append(notice); app.toastTimer = ++timerId; timers.set(timerId, {run: () => notice.remove(), delay: 3800}); return notice;};
  return {app, root, a, b, pending, timers, cleared, updates, toastCalls, dialog, seed, invoke: context.invoke};
}

async function lateScenario(kind, responseCode = callback) {
  const f = fixture({responseCode}), running = f.invoke({community: group('room_A')});
  assert.equal(f.app.communityOpenTicket, 1); assert.equal(f.dialog.element.open, false, 'the real callback reached its awaited openGroup');
  if (kind === 'other-room') await f.app.openGroup('room_B');
  else if (kind === 'same-room-new-generation') await f.app.openGroup('room_A');
  else if (kind === 'account-change') {f.app.deskAccount = 'account_B'; f.app.accountGeneration++;}
  else if (kind === 'account-ABA') {f.app.deskAccount = 'account_B'; f.app.accountGeneration++; f.app.deskAccount = 'account_A'; f.app.accountGeneration++;}
  const expectedSurface = f.app.communitySurface, host = kind === 'same-room-new-generation' ? f.a : f.b, notice = f.seed(host), timer = f.app.toastTimer;
  f.pending.resolve(group('room_A')); await running;
  assert.equal(f.toastCalls.length, 0, 'a retired response must not emit an A notice');
  assert.equal(f.root.querySelector('.sw-toast'), notice, 'a retired response must not remove the current notice');
  assert.equal(notice.removed, 0); assert.equal(f.app.toastTimer, timer); assert.deepEqual(f.cleared, []);
  assert.equal(f.app.communitySurface, expectedSurface, 'the real stale openGroup must not replace the current surface');
  return f;
}

for (const kind of ['other-room', 'same-room-new-generation', 'account-change', 'account-ABA']) test(`late community success ${kind}: no toast or deletion of the newer owner's notice`, () => lateScenario(kind));

async function failedOpen(responseCode = callback) {
  const f = fixture({responseCode}), running = f.invoke({community: group('room_A')});
  f.pending.resolve(null); await running;
  assert.equal(f.app.communitySurface, null);
  assert.equal(f.toastCalls.some(args => !args[1] && args[0] === 'Комната готова'), false, 'failed opening must not announce room-ready');
  // This is still the current A attempt: openGroup legitimately reports its
  // existing error. A false success must not replace that real failure notice.
  assert.equal(f.toastCalls.length, 1); const notice = f.root.querySelector('.sw-toast');
  assert.equal(notice.attributes.role, 'alert'); assert.equal(notice.textContent, 'Недоступно');
  assert.equal(f.a.querySelector('.sw-toast'), null);
}
test('a caught openGroup failure cannot announce a successfully opened room', () => failedOpen());

for (const existing of [false, true]) test(`current ${existing ? 'update' : 'creation'} delivers one owned status after the actual opening and retains 3800ms lifetime`, async () => {
  const f = fixture({existing}), running = f.invoke({community: group('room_A')});
  assert.equal(f.toastCalls.length, 0); f.pending.resolve(group('room_A')); await running;
  assert.equal(f.toastCalls.length, 1); assert.equal(f.toastCalls[0][3], 'room_A');
  const notice = f.a.querySelector('.sw-toast'); assert.ok(notice); assert.equal(f.b.querySelector('.sw-toast'), null);
  assert.equal(notice.attributes.role, 'status'); assert.equal(notice.textContent, existing ? 'Комната обновлена' : 'Комната готова');
  const timer = f.timers.get(f.app.toastTimer); assert.equal(timer.delay, 3800); timer.run(); assert.equal(f.a.querySelector('.sw-toast'), null);
});

function wrongOwner(kind, toastCode = method('toast')) {
  const f = fixture({toastCode}), notice = f.seed(), timer = f.app.toastTimer;
  let owner = 'missing_room';
  if (kind !== 'missing') {owner = 'room_A'; if (kind === 'disconnected') f.a.isConnected = false; else if (kind === 'no-rects') f.a.rects = false; else f.a[kind] = true;}
  let thrown; try {f.app.toast('Поздний статус A', false, undefined, owner);} catch (error) {thrown = error;}
  assert.equal(f.root.querySelector('.sw-toast'), notice, 'invalid notice owner must not remove the current B notice');
  assert.equal(notice.removed, 0); assert.equal(f.app.toastTimer, timer); assert.deepEqual(f.cleared, []);
  assert.equal(f.a.querySelector('.sw-toast'), null); assert.equal(thrown, undefined, 'invalid notice owner must return without throwing');
}
test('toast itself rejects missing, disconnected, hidden, inert and zero-rect owners before touching B or its timer', () => {
  for (const kind of ['missing', 'disconnected', 'hidden', 'inert', 'no-rects']) wrongOwner(kind);
});

test('default global notices, error timeout and actionable notice behavior remain unchanged', () => {
  const f = fixture(), old = f.seed(); f.app.toast('Глобальный статус');
  let notice = f.root.querySelector('.sw-toast'); assert.equal(notice.parent, f.root); assert.equal(old.removed, 1); assert.equal(notice.attributes.role, 'status');
  assert.equal(f.timers.get(f.app.toastTimer).delay, 3800);
  f.app.toast('Глобальная ошибка', true); notice = f.root.querySelector('.sw-toast'); assert.equal(notice.attributes.role, 'alert'); assert.equal(f.timers.get(f.app.toastTimer).delay, 6500);
  let runs = 0; f.app.toast('Повторить', false, {label: 'Повторить', run() {runs++;}}); notice = f.root.querySelector('.sw-toast');
  assert.equal(f.app.toastTimer, null); assert.equal(notice.children.length, 2); notice.children[0].run(); assert.equal(runs, 1); assert.equal(f.root.querySelector('.sw-toast'), null);
});

test('BROKEN missing ticket guard fails the SAME same-room-generation notice oracle', async () => {
  const broken = replaceOnce(callback, 'ticket === this.communityOpenTicket && ', '');
  await assert.rejects(lateScenario('same-room-new-generation', broken), /retired response must not emit/);
});
test('BROKEN missing surface-ID guard fails the SAME unsuccessful-opening notice oracle', async () => {
  const broken = replaceOnce(callback, 'this.communitySurface?.communityId === response.community.communityId', 'true');
  await assert.rejects(failedOpen(broken), /failed opening must not announce/);
});
test('BROKEN missing method owner guard fails the SAME current-B preservation oracle', () => {
  const broken = replaceOnce(method('toast'), print(toastGuard.expression), 'false');
  assert.throws(() => wrongOwner('missing', broken), /invalid notice owner must not remove/);
});
test('Source pin identifies the actual methods and form callback without a browser acceptance claim', t => {
  t.diagnostic('app.ts sha256 ' + createHash('sha256').update(source).digest('hex'));
});
