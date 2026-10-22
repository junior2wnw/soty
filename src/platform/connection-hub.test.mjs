import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve, sep } from 'node:path';
import vm from 'node:vm';
import ts from 'typescript';
import { parseConnectionIntent } from './connection-intent.mjs';
import { createMediaCaptureCoordinator, sharedMediaCaptureCoordinator } from './media-capture-lease.mjs';
import { rasterFileDimensions, safeRasterSize } from '../world/feedback-media.mjs';
import { createClientWithStorage } from '../../modules/connect/browser/client.mjs';
import { validateState } from '../../modules/connect/browser/storage.mjs';
import { ConnectError } from '../../modules/connect/browser/crypto.mjs';
import { createConnectService } from '../../modules/connect/server/index.mjs';

const ORIGIN = 'https://hub.test', CARD = 'card_' + 'a'.repeat(32), REQUEST = 'enroll_' + 'b'.repeat(32);
const contact = `${ORIGIN}/?connect=contact&card=${CARD}`;
const source = await readFile(new URL('./connection-hub.ts', import.meta.url), 'utf8');
const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };
const tick = () => new Promise(resolve => setImmediate(resolve));
async function until(predicate) { const deadline = Date.now() + 5000; while (!predicate()) { assert.ok(Date.now() < deadline, 'bounded native-port operation completed'); await tick(); } }

class EventPort {
  listeners = new Map();
  addEventListener(type, fn, options) { const list = this.listeners.get(type) ?? []; list.push({ fn, once: options?.once }); this.listeners.set(type, list); }
  removeEventListener(type, fn) { this.listeners.set(type, (this.listeners.get(type) ?? []).filter(item => item.fn !== fn)); }
  emit(type, event = {}) { for (const entry of [...(this.listeners.get(type) ?? [])]) { if (entry.once) this.removeEventListener(type, entry.fn); entry.fn({ preventDefault() {}, ...event }); } }
}
class ElementPort extends EventPort {
  children = []; attributes = new Map(); dataset = {}; style = {}; hidden = false; disabled = false; isConnected = true; open = true;
  value = ''; textContent = ''; className = ''; tagName = ''; type = ''; files = null; srcObject = null;
  classList = { add: (...names) => { this.className += ' ' + names.join(' '); } };
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = [...children]; }
  setAttribute(name, value) { this.attributes.set(name, String(value)); }
  getAttribute(name) { return this.attributes.get(name); }
  focus() { this.focused = true; }
  select() { this.selected = true; }
  click() { if (!this.disabled) this.emit('click'); }
  close() { this.open = false; this.emit('close'); }
  play() { return this.playPort?.() ?? Promise.resolve(); }
}
const descendants = node => [node, ...node.children.flatMap(child => child instanceof ElementPort ? descendants(child) : [])];
function fakeState(accountId = 'acct_fixture', deviceId = 'dev_fixture') {
  return { schema: 'connect.local-view.v1', localRevision: 1, accountId, deviceId,
    label: 'Fixture', current: accountId ? { accountId, deviceId, revoked: false } : null,
    profiles: [], pendingEnrollment: null, pendingRecovery: null, recoveryPrepared: false };
}
function fixture(t, overrides = {}) {
  const created = [], document = new EventPort(), window = new EventPort(); document.visibilityState = 'visible';
  const element = (tagName, className = '', textContent = '') => { const node = Object.assign(new ElementPort(), { tagName: tagName.toUpperCase(), className, textContent }); node.focus = () => { node.focused = true; document.activeElement = node; }; created.push(node); return node; };
  let local = overrides.state ?? fakeState(), current = true, observer = null;
  const calls = { bootstrap: 0, acquire: 0, detect: 0, intents: [], account: [], cards: [] };
  const client = overrides.client ?? { getLocalState: async () => local,
    async bootstrap() { calls.bootstrap++; local = fakeState('acct_created', 'dev_created'); return { accountId: local.accountId, deviceId: local.deviceId, label: local.label }; },
    async extension(op, args, context) { calls.cards.push({ op, args, context }); return { cardId: CARD, label: 'Fixture' }; } };
  const scanner = overrides.scanner ?? { acquire: async () => { calls.acquire++; throw new Error('fixture permission denied'); }, detect: async () => { calls.detect++; return ''; }, file: async () => contact };
  let panel = null;
  document.createElement = tag => element(tag);
  document.querySelector = () => panel;
  const dom = { el: element, button(label, _icon, cls = '', handler) { const node = element('button', cls, label); if (handler) node.addEventListener('click', handler); return node; },
    textInput(value = '', placeholder = '', maxLength = 200) { return Object.assign(element('input'), { value, placeholder, maxLength }); } };
  const records = [];
  const connect = { openConnectPanel(options) { records.push(options); panel = element('dialog'); return { close: () => panel.close() }; } };
  const exports = {};
  const modules = {
    'qrcode': { default: { toDataURL: async () => 'data:image/png;base64,AA==' } },
    'jsqr': { default: () => null },
    '../../modules/connect/ui/index.mjs': connect,
    '../world/dom': dom,
    '../world/dialogs': { errorText: error => error.code ?? error.message ?? 'operation failed',
      createDialog(_title, onClose) { const body = element('div'), dialog = element('dialog'); dialog.append(body); return { body, element: dialog, close() { dialog.close(); onClose?.(); } }; } },
    '../world/feedback-media.mjs': { rasterFileDimensions, safeRasterSize },
    './connection-intent.mjs': { parseConnectionIntent, MAX_CONNECTION_INPUT_BYTES: 32768 },
    './media-capture-lease.mjs': { sharedMediaCaptureCoordinator },
  };
  const native = { document, window, navigator: { clipboard: { writeText: async () => {} }, mediaDevices: { getUserMedia: async () => { throw new Error('denied'); } } },
    URL, AbortController, TextEncoder, crypto: globalThis.crypto, setTimeout, clearTimeout,
    createImageBitmap: async () => { throw new Error('native bitmap fixture not installed'); }, console: { warn() {} } };
  vm.runInNewContext(compiled, { exports, module: { exports }, require: name => name.endsWith('.css') ? {} : (assert.ok(Object.hasOwn(modules, name), 'declared native port: ' + name), modules[name]), ...native, ...overrides.native });
  const host = element('div'), captures = createMediaCaptureCoordinator(), handles = [];
  const options = { client, origin: ORIGIN, isCurrent: () => current, scanner, captures,
    observeAccount: fn => { observer = fn; return () => { observer = null; }; },
    qr: async () => 'data:image/png;base64,AA==', onIntent: async (intent, handoff) => { calls.intents.push({ intent, handoff }); },
    ...overrides.options };
  const result = { api: exports, host, client, scanner, captures, document, window, calls, created, records,
    setState(value) { local = value; }, invalidate() { current = false; observer?.(); }, changed() { observer?.(); },
    mount(extra = {}) { const handle = exports.mountConnectionHub(host, { ...options, ...extra }); handles.push(handle); return handle; },
    ensure(extra = {}) { return exports.ensureConnectionAccount(host, { ...options, ...extra }); },
    options,
    find(label) { const node = descendants(host).find(node => node.tagName === 'BUTTON' && node.textContent === label); assert.ok(node, 'visible control exists: ' + label); return node; },
    input() { return descendants(host).find(node => node.attributes.get('aria-label') === 'Ссылка для подключения'); },
    video() { return descendants(host).find(node => node.tagName === 'VIDEO'); },
    status() { return descendants(host).find(node => node.className === 'ch-status')?.textContent ?? ''; },
    async paste(value) { result.input().value = value; result.find('Открыть ссылку').click(); await tick(); },
  };
  t.after(() => { for (const handle of handles) handle.dispose(); }); return result;
}
const ownTrack = () => ({ kind: 'video', enabled: true, readyState: 'live', stops: 0, stop() { this.stops++; this.readyState = 'ended'; } });
const stream = (...tracks) => ({ getTracks: () => tracks });

test('known account bypasses registration and does not open media or a new local identity', async t => {
  const f = fixture(t); const result = await f.ensure();
  assert.equal(result.state.accountId, 'acct_fixture'); assert.equal(result.intent, null);
  assert.equal(f.calls.bootstrap, 0); assert.equal(f.calls.acquire, 0); assert.equal(f.host.children.length, 0);
});

test('camera acquisition is explicit; show and paste never request capture or bootstrap', async t => {
  const f = fixture(t); f.mount(); await until(() => f.calls.cards.length === 1);
  assert.equal(f.calls.acquire, 0); assert.equal(f.calls.bootstrap, 0);
  assert.deepEqual(JSON.parse(JSON.stringify(f.calls.cards[0])), { op: 'card.get', args: {}, context: { expectedAccountId: 'acct_fixture' } });
  await f.paste(contact); assert.equal(f.calls.intents.length, 0);
  f.find('Продолжить').click(); await until(() => f.calls.intents.length === 1);
  assert.equal(f.calls.intents[0].intent.kind, 'contact'); assert.equal(f.calls.acquire, 0); assert.equal(f.calls.bootstrap, 0);
});

test('malformed and competing carriers do not dispatch; cancel is inert', async t => {
  const f = fixture(t); f.mount(); await until(() => f.calls.cards.length === 1);
  for (const value of [contact + '#profile/acct_other', 'https://other.test/?connect=contact&card=' + CARD, '/?j=room.A']) await f.paste(value);
  assert.equal(f.calls.intents.length, 0); assert.equal(f.calls.acquire, 0);
  await f.paste(contact); f.find('Отмена').click(); assert.equal(f.calls.intents.length, 0);
});

test('platform invitation passes only a marker and a private one-use handoff after explicit continuation', async t => {
  const f = fixture(t); f.mount(); await until(() => f.calls.cards.length === 1);
  const url = ORIGIN + '/#soty-invite=' + 'x'.repeat(43);
  await f.paste(url); assert.equal(f.calls.intents.length, 0); assert.equal(f.input().value, '');
  f.find('Продолжить').click(); await until(() => f.calls.intents.length === 1);
  const { intent, handoff } = f.calls.intents[0];
  assert.deepEqual(intent, { kind: 'platform-invitation' }); assert.equal(JSON.stringify(intent).includes('x'.repeat(43)), false);
  assert.equal(handoff.takePlatformInvitationLink(), url); assert.equal(handoff.takePlatformInvitationLink(), null);
  assert.equal(f.calls.bootstrap, 0);
});

test('an incomplete account panel cannot claim success and a late completion after disposal cannot mount', async t => {
  const pending = deferred(), f = fixture(t, { state: fakeState(null, null), options: { openAccount: () => pending.promise } });
  const handle = f.mount({ entry: true }); await until(() => f.find('Открыть мой профиль'));
  let completed = false; void handle.ready.then(() => { completed = true; }, () => {});
  f.find('Открыть мой профиль').click(); await tick(); handle.dispose();
  f.setState(fakeState('acct_late', 'dev_late')); pending.resolve(); await tick(); await tick();
  assert.equal(completed, false); assert.equal(f.calls.bootstrap, 0); assert.equal(f.host.children[0].children.length, 0);
});

test('revoked and unreadable durable state never falls back to creating an account', async t => {
  for (const code of ['CORRUPT_LOCAL_STATE', 'UNSUPPORTED_LOCAL_STATE', 'KEY_MISMATCH', 'DEVICE_REVOKED']) {
    let bootstraps = 0;
    const client = { getLocalState: async () => { throw Object.assign(new Error('synthetic storage failure'), { code }); }, bootstrap: async () => { bootstraps++; } };
    const f = fixture(t, { client }); await assert.rejects(f.ensure(), { code }); assert.equal(bootstraps, 0);
  }
  const revoked = fakeState(); revoked.current.revoked = true;
  const f = fixture(t, { state: revoked }); await assert.rejects(f.ensure(), { code: 'DEVICE_REVOKED' }); assert.equal(f.calls.bootstrap, 0);
});

test('account UI bridge really disables bootstrap and closes when its original scope is invalidated', async t => {
  const f = fixture(t); const pending = f.api.openConnectionAccountPanel(f.options, 'devices');
  await until(() => f.records.length === 1);
  assert.equal(f.records[0].bootstrapOnOpen, false); assert.equal(f.records[0].initialTab, 'devices');
  f.invalidate(); await assert.rejects(pending, { code: 'ACTIVE_PROFILE_CHANGED' }); assert.equal(f.calls.bootstrap, 0);
});

test('foreign video camera lease yields file/paste fallback without requesting or stopping any track', async t => {
  const f = fixture(t), foreign = ownTrack(), videoLease = f.captures.acquire('camera', {});
  f.mount(); await until(() => f.calls.cards.length === 1); f.find('Сканировать').click(); await tick();
  assert.equal(f.calls.acquire, 0); assert.equal(foreign.stops, 0); assert.equal(videoLease.isCurrent(), true);
  assert.match(f.status(), /Камера используется/u); await f.paste(contact); assert.equal(f.calls.intents.length, 0);
  videoLease.release();
});

test('pending capture retains its lease after close until the actual late stream is stopped', async t => {
  const permission = deferred(), f = fixture(t, { scanner: { acquire: () => permission.promise, detect: async () => '', file: async () => contact } });
  const handle = f.mount(); await until(() => f.calls.cards.length === 1); f.find('Сканировать').click(); await tick();
  assert.equal(f.captures.busy('camera'), true); handle.dispose(); assert.equal(f.captures.acquire('camera', {}), null);
  const track = ownTrack(); permission.resolve(stream(track)); await until(() => track.stops === 1);
  assert.equal(f.captures.busy('camera'), false); assert.equal(f.calls.intents.length, 0);
});

test('hidden page stops only its own camera; denied capture releases its lease without touching microphone', async t => {
  const track = ownTrack(), f = fixture(t, { scanner: { acquire: async () => stream(track), detect: async () => '', file: async () => contact } });
  const mic = f.captures.acquire('microphone', {}); f.mount(); await until(() => f.calls.cards.length === 1);
  f.find('Сканировать').click(); await until(() => f.video().srcObject !== null);
  f.document.visibilityState = 'hidden'; f.document.emit('visibilitychange');
  assert.equal(track.stops, 1); assert.equal(f.captures.busy('camera'), false); assert.equal(mic.isCurrent(), true); mic.release();
  const denied = fixture(t); denied.mount(); await until(() => denied.calls.cards.length === 1); denied.find('Сканировать').click();
  await until(() => denied.status().includes('Камера недоступна')); assert.equal(denied.captures.busy('camera'), false);
});

test('old play rejection cannot clear a newer camera preview', async t => {
  const oldPlay = deferred(), tracks = [ownTrack(), ownTrack()]; let captures = 0, plays = 0;
  const f = fixture(t, { scanner: { acquire: async () => stream(tracks[captures++]), detect: async () => '', file: async () => contact } });
  f.mount(); await until(() => f.calls.cards.length === 1); f.video().playPort = () => ++plays === 1 ? oldPlay.promise : Promise.resolve();
  f.find('Сканировать').click(); await until(() => plays === 1); f.find('Выключить камеру').click();
  f.find('Сканировать').click(); await until(() => captures === 2 && plays === 2);
  const fresh = f.video().srcObject; oldPlay.reject(new Error('old synthetic play failed')); await tick(); await tick();
  assert.equal(f.video().srcObject, fresh); assert.equal(f.video().hidden, false); assert.equal(tracks[1].stops, 0);
});

test('late old decode cannot replace a newer pasted device intent', async t => {
  const decoded = deferred(), track = ownTrack(); let detections = 0;
  const f = fixture(t, { scanner: { acquire: async () => stream(track), detect: () => { detections++; return decoded.promise; }, file: async () => contact } });
  f.mount(); await until(() => f.calls.cards.length === 1); f.find('Сканировать').click(); await until(() => detections === 1);
  await f.paste(`/?connect=device&request=${REQUEST}`); decoded.resolve(contact); await tick();
  f.find('Продолжить').click(); await until(() => f.calls.intents.length === 1);
  assert.equal(f.calls.intents[0].intent.kind, 'device'); assert.equal(f.calls.intents[0].intent.id, REQUEST); assert.equal(track.stops, 1);
});

test('camera decoding stays single-flight across cancelled and restarted captures', async t => {
  const decoded = deferred(), tracks = [ownTrack(), ownTrack()], timers = new Map(); let acquired = 0, detections = 0, nextTimer = 0;
  const f = fixture(t, { scanner: { acquire: async () => stream(tracks[acquired++]), detect: () => ++detections === 1 ? decoded.promise : Promise.resolve(''), file: async () => contact },
    native: { setTimeout(callback) { const id = ++nextTimer; timers.set(id, callback); return id; }, clearTimeout(id) { timers.delete(id); } } });
  const fire = () => { const [id, callback] = timers.entries().next().value; timers.delete(id); callback(); };
  f.mount(); await until(() => f.calls.cards.length === 1); f.find('Сканировать').click(); await until(() => detections === 1);
  f.find('Выключить камеру').click(); f.find('Сканировать').click(); await until(() => acquired === 2 && timers.size === 1);
  assert.equal(detections, 1); fire(); assert.equal(detections, 1);
  decoded.resolve(''); await tick(); fire(); await until(() => detections === 2);
  assert.equal(tracks[0].stops, 1); assert.equal(tracks[1].stops, 0);
});

test('a pending local file cannot start a second decode or overwrite a newer pasted intent', async t => {
  const decoded = deferred(); let files = 0;
  const f = fixture(t, { scanner: { acquire: async () => { throw new Error('not requested'); }, detect: async () => '', file: () => { files++; return decoded.promise; } } });
  f.mount(); await until(() => f.calls.cards.length === 1); f.find('Выбрать файл').click();
  const picker = f.created.filter(node => node.type === 'file').at(-1); picker.files = [{ type: 'text/plain', name: 'link.txt', size: contact.length }]; picker.emit('change');
  f.find('Выбрать файл').click(); assert.equal(files, 1); assert.equal(f.created.filter(node => node.type === 'file').length, 1);
  await f.paste(`/?connect=device&request=${REQUEST}`); decoded.resolve(contact); await tick(); await tick();
  f.find('Продолжить').click(); await until(() => f.calls.intents.length === 1);
  assert.equal(f.calls.intents[0].intent.kind, 'device'); assert.equal(f.calls.intents[0].intent.id, REQUEST);
});

test('a durable-read continuation cannot dispatch an intent superseded by a newer paste', async t => {
  const reread = deferred(); let block = false, blockedReads = 0, cards = 0;
  const client = { getLocalState: () => { if (block) { blockedReads++; return reread.promise; } return Promise.resolve(fakeState()); },
    bootstrap: async () => { throw new Error('not authorized by this action'); }, extension: async () => { cards++; return { cardId: CARD }; } };
  const f = fixture(t, { client }); f.mount(); await until(() => cards === 1); await f.paste(contact);
  block = true; f.find('Продолжить').click(); await until(() => blockedReads === 1);
  await f.paste(`/?connect=device&request=${REQUEST}`); block = false; reread.resolve(fakeState()); await tick(); await tick();
  assert.equal(f.calls.intents.length, 0); f.find('Продолжить').click(); await until(() => f.calls.intents.length === 1);
  assert.equal(f.calls.intents[0].intent.kind, 'device'); assert.equal(f.calls.intents[0].intent.id, REQUEST);
});

async function assertCancelledDurableRead(t, entry, heldRead) {
  const delayed = deferred(); let armed = false, reads = 0, cards = 0;
  const client = {
    getLocalState: () => armed && ++reads === heldRead ? delayed.promise : Promise.resolve(fakeState()),
    bootstrap: async () => { throw new Error('not authorized'); },
    extension: async () => { cards++; return { cardId: CARD }; },
  };
  const f = fixture(t, { client }), handle = f.mount(entry ? { entry: true, initialInput: contact } : {});
  let result = null;
  if (entry) {
    void handle.ready.then(value => { result = value; }, () => {});
    await until(() => descendants(f.host).some(node => node.tagName === 'BUTTON' && node.textContent === 'Продолжить'));
  } else { await until(() => cards === 1); await f.paste(contact); }
  armed = true; f.find('Продолжить').click(); await until(() => reads === heldRead);
  f.find('Отмена').click(); await tick(); await tick();
  assert.equal(f.calls.intents.length, 0);
  if (entry) {
    // Cancel must resolve without waiting for the older outstanding durable read.
    assert.notEqual(result, null, 'cold Cancel completes independently of the older busy Continue');
    assert.equal(result.intent, null); assert.equal(result.state.accountId, 'acct_fixture');
  }
  delayed.resolve(fakeState()); await tick(); await tick(); await tick();
  assert.equal(f.calls.intents.length, 0, 'late durable read cannot dispatch a cancelled intent');
  if (entry) { assert.equal(result.intent, null); assert.equal(result.state.accountId, 'acct_fixture'); }
}

test('warm Cancel invalidates Continue held at durable read 2', async t => {
  await assertCancelledDurableRead(t, false, 2);
});
test('warm Cancel invalidates Continue held at durable read 3', async t => {
  await assertCancelledDurableRead(t, false, 3);
});
test('cold Cancel completes with no intent while durable read 2 is still pending', async t => {
  await assertCancelledDurableRead(t, true, 2);
});
test('cold Cancel completes with no intent while durable read 3 is still pending', async t => {
  await assertCancelledDurableRead(t, true, 3);
});

test('a failed own stop keeps the camera lease busy while file selection remains usable', async t => {
  const track = ownTrack(); track.stop = () => { throw new Error('synthetic native stop failure'); };
  const f = fixture(t, { scanner: { acquire: async () => stream(track), detect: async () => '', file: async () => contact } });
  f.mount(); await until(() => f.calls.cards.length === 1); f.find('Сканировать').click(); await until(() => f.video().srcObject !== null);
  f.find('Выключить камеру').click(); assert.equal(f.captures.busy('camera'), true);
  f.find('Выбрать файл').click(); const picker = f.created.filter(node => node.type === 'file').at(-1);
  picker.files = [{ name: 'synthetic-qr.txt', size: contact.length, type: 'text/plain' }]; picker.emit('change');
  await until(() => descendants(f.host).some(node => node.textContent === 'Продолжить'));
  assert.equal(f.calls.intents.length, 0);
});

test('late QR rendering and an invalidated account scope cannot leave a private card or dispatch', async t => {
  const qr = deferred(), f = fixture(t, { options: { qr: () => qr.promise } });
  f.mount(); await until(() => f.calls.cards.length === 1); f.invalidate(); qr.resolve('data:image/png;base64,AA=='); await tick(); await tick();
  assert.equal(descendants(f.host).some(node => node.tagName === 'IMG'), false); assert.equal(f.calls.intents.length, 0);
});

test('native file fallback is local and bounded before image decode, and closes a decoded bitmap on failure', async t => {
  let bitmapCalls = 0, closes = 0;
  const f = fixture(t, { native: { createImageBitmap: async () => { bitmapCalls++; return { width: 10, height: 10, close() { closes++; } }; } } });
  const scanner = f.api.createConnectionScanner();
  assert.equal(await scanner.file({ type: 'text/plain', name: 'link.txt', size: contact.length, text: async () => contact }), contact);
  await assert.rejects(scanner.file({ type: 'text/plain', name: 'huge.txt', size: 32769, text: async () => { throw new Error('must not read oversized file'); } }), /connection_file_too_large/u);
  const png = (width, height) => { const bytes = new Uint8Array(24); bytes.set([137,80,78,71,13,10,26,10]); bytes.set([73,72,68,82],12); const view = new DataView(bytes.buffer); view.setUint32(16,width); view.setUint32(20,height); return bytes.buffer; };
  await assert.rejects(scanner.file({ type: 'image/png', name: 'bomb.png', size: 24, arrayBuffer: async () => png(100000,100000) }), /feedback_image_dimensions/u);
  assert.equal(bitmapCalls, 0);
  await assert.rejects(scanner.file({ type: 'image/png', name: 'fixture.png', size: 24, arrayBuffer: async () => png(10,10) }));
  assert.equal(bitmapCalls, 1); assert.equal(closes, 1);
});

const projectId = 'hub-real-connect', endpoint = ORIGIN + '/rpc';
function storage() {
  let value = null;
  return { read: async () => value === null ? null : structuredClone(value),
    claim: async candidate => { if (value === null) { validateState(candidate, { projectId, endpoint }); value = structuredClone(candidate); } return structuredClone(value); },
    compareAndSwap: async (revision, candidate) => { if (value?.localRevision !== revision) throw new ConnectError('LOCAL_CONFLICT', 'fixture conflict'); validateState(candidate, { projectId, endpoint }); value = structuredClone(candidate); return structuredClone(value); } };
}
async function realConnect(t) {
  const dir = await mkdtemp(join(tmpdir(), 'soty-hub-connect-')), databasePath = join(dir, 'account.sqlite');
  let service = createConnectService({ databasePath, projectId, allowedOrigins: [ORIGIN] }), drop = null;
  const traffic = [], clients = [];
  const fetcher = async (_url, options) => {
    const body = JSON.parse(options.body); traffic.push(body.op);
    const result = await service.handle({ ...body, origin: ORIGIN });
    if (drop === body.op && result.ok) { drop = null; throw new TypeError('Synthetic lost ACK'); }
    return new Response(JSON.stringify(result), { status: result.ok ? 200 : 400, headers: { 'Content-Type': 'application/json' } });
  };
  const client = (store = storage()) => { const value = createClientWithStorage({ projectId, endpoint, fetch: fetcher }, store); clients.push(value); return { client: value, store }; };
  t.after(async () => { for (const value of clients) value.dispose(); service.close(); assert.ok(resolve(dir).startsWith(resolve(tmpdir()) + sep)); await rm(dir, { recursive: true }); });
  return { traffic, client, lose: op => { drop = op; },
    async inviteOnly(ownerAccountId, owner) { service.close(); service = createConnectService({ databasePath, projectId, allowedOrigins: [ORIGIN], platformAdminAccountIds: [ownerAccountId] });
      await owner.extension('platform.admission.update', { requestId: 'policy_hub_fixture', expectedRevision: 0, inviteOnly: true, defaultQuota: 3 }, { expectedAccountId: ownerAccountId }); } };
}

test('genuine cold entry: invite-only existing-profile enrollment precedes bootstrap, preserves pending intent and survives lost finish ACK/reload/revoke', async t => {
  const real = await realConnect(t), owner = real.client(), recipient = real.client();
  const original = await owner.client.bootstrap('Owner'); await real.inviteOnly(original.accountId, owner.client);
  const before = real.traffic.filter(op => op === 'bootstrap').length;
  const f = fixture(t, { client: recipient.client, options: { openAccount: async tab => {
    assert.equal(tab, 'devices'); const request = await recipient.client.startEnrollment('Second device');
    await owner.client.approveEnrollment(request.requestId, original.accountId);
    const preview = await recipient.client.previewEnrollment(request.requestId); assert.equal(preview.account.accountId, original.accountId);
    real.lose('enrollment.finish'); await assert.rejects(recipient.client.finishEnrollment(request.requestId, original.accountId), { code: 'NETWORK_ERROR' });
    const again = await recipient.client.previewEnrollment(request.requestId); await recipient.client.finishEnrollment(request.requestId, again.account.accountId);
  } } });
  const ready = f.ensure({ initialInput: contact }); let earlyFailure = null; void ready.catch(error => { earlyFailure = error; });
  await until(() => earlyFailure || descendants(f.host).some(node => node.textContent === 'Добавить человека'));
  assert.equal(earlyFailure, null, 'existing-profile cold entry must not attempt new-account bootstrap');
  assert.equal(real.traffic.filter(op => op === 'bootstrap').length, before);
  // The first button belongs to the typed preview; its account action is explicit.
  const previewAction = descendants(f.host).find(node => node.tagName === 'BUTTON' && node.textContent === 'Открыть мой профиль'); previewAction.click();
  await until(() => descendants(f.host).some(node => node.tagName === 'BUTTON' && node.textContent === 'Продолжить'));
  f.find('Продолжить').click(); const result = await ready;
  assert.equal(result.state.accountId, original.accountId); assert.equal(result.intent.kind, 'contact'); assert.equal(result.intent.id, CARD);
  assert.equal(real.traffic.filter(op => op === 'bootstrap').length, before);
  const reloaded = real.client(recipient.store).client, confirmed = await reloaded.getLocalState();
  assert.equal(confirmed.accountId, original.accountId); assert.notEqual(confirmed.deviceId, original.deviceId);
  const quotas = await owner.client.extension('platform.invites.list', {}, { expectedAccountId: original.accountId }); assert.equal(quotas.used, 0);
  await owner.client.revokeDevice(confirmed.deviceId); await assert.rejects(reloaded.status(), { code: 'device_revoked' });
});

test('genuine explicit new profile retains invite-only admission and never silently replaces it with enrollment', async t => {
  const real = await realConnect(t), owner = real.client(), newcomer = real.client();
  const original = await owner.client.bootstrap('Owner'); await real.inviteOnly(original.accountId, owner.client);
  const f = fixture(t, { client: newcomer.client }); const handle = f.mount({ entry: true });
  await until(() => !f.find('Создать новый профиль').hidden); const before = real.traffic.filter(op => op === 'bootstrap').length;
  assert.equal(real.traffic.filter(op => op === 'bootstrap').length, before);
  f.find('Создать новый профиль').click(); await until(() => f.status().includes('нужно приглашение'));
  assert.equal((await newcomer.client.getLocalState()).accountId, null);
  assert.equal(real.traffic.filter(op => op === 'bootstrap').length, before + 1);
  assert.equal(real.traffic.includes('enrollment.start'), false); handle.dispose();
});

test('genuine new-account registration happens only after the explicit create action and a durable reread', async t => {
  const real = await realConnect(t), newcomer = real.client(), f = fixture(t, { client: newcomer.client });
  const ready = f.ensure(); await until(() => descendants(f.host).some(node => node.tagName === 'BUTTON' && node.textContent === 'Создать новый профиль' && !node.hidden));
  assert.equal(real.traffic.includes('bootstrap'), false);
  f.find('Создать новый профиль').click(); const result = await ready;
  assert.ok(result.state.accountId); assert.equal(result.intent, null);
  assert.equal((await newcomer.client.getLocalState()).accountId, result.state.accountId);
  assert.equal(real.traffic.filter(op => op === 'bootstrap').length, 1);
});


// Native paste is observed after the browser has applied selection replacement/maxLength.
const pasteEvent = (f, value, inputType = 'insertFromPaste') => {
  const input = f.input(); input.value = value; input.emit('input', { inputType }); return input;
};
const hubPreview = f => descendants(f.host).find(node => node.className === 'ch-preview');
const cardShown = f => descendants(f.host).some(node => node.tagName === 'IMG');

test('shown friend QR removes the duplicate action and returns it when the card is replaced', async t => {
  const f = fixture(t); f.mount(); const input = f.input(); input.focus(); await until(() => cardShown(f));
  assert.equal(f.document.activeElement, input, 'initial card never steals focus');
  assert.equal(f.find('Мой QR').hidden, true);
  await f.paste(contact);
  assert.equal(cardShown(f), false); assert.equal(f.find('Мой QR').hidden, false);
  f.find('Мой QR').focus(); f.find('Мой QR').click(); await until(() => cardShown(f));
  assert.equal(f.find('Мой QR').hidden, true);
  assert.equal(f.document.activeElement, f.find('Скопировать ссылку'), 'focus moves from the hidden action to the visible card');
  assert.equal(hubPreview(f).hidden, true, 'returning to my QR closes the previous connection purpose');
  assert.equal(f.calls.intents.length, 0); assert.equal(f.calls.acquire, 0); assert.equal(f.calls.bootstrap, 0);
});

test('native paste previews each purpose without dispatch, lost draft, moved focus or a second action', async t => {
  for (const [value, kind, label] of [
    [contact, 'contact', 'Добавить человека'],
    [`/?connect=device&request=${REQUEST}`, 'device', 'Подтвердить своё устройство'],
    [ORIGIN + '/#soty-invite=' + 'x'.repeat(43), 'platform-invitation', 'Приглашение в Соты'],
  ]) {
    const f = fixture(t); f.mount(); await until(() => cardShown(f));
    const input = f.input(); input.focus(); pasteEvent(f, value);
    assert.equal(hubPreview(f).hidden, false); assert.equal(hubPreview(f).children[0].textContent, label);
    assert.equal(input.value, value); assert.equal(input.focused, true); assert.notEqual(f.find('Продолжить').focused, true);
    assert.equal(f.calls.intents.length, 0); assert.equal(f.records.length, 0); assert.equal(f.calls.acquire, 0); assert.equal(f.calls.bootstrap, 0);
    f.find('Отмена').click(); assert.equal(input.value, value); assert.equal(hubPreview(f).hidden, true);
    pasteEvent(f, value); f.find('Продолжить').click(); f.find('Продолжить').click();
    await until(() => f.calls.intents.length === 1); await tick();
    assert.equal(f.calls.intents.length, 1); assert.equal(f.calls.intents[0].intent.kind, kind);
    if (kind === 'platform-invitation') {
      assert.deepEqual(f.calls.intents[0].intent, { kind });
      assert.equal(f.calls.intents[0].handoff.takePlatformInvitationLink(), value);
      assert.equal(f.calls.intents[0].handoff.takePlatformInvitationLink(), null);
    }
  }
});

test('typing, absent paste metadata and invalid carriers keep explicit submit and never auto-route', async t => {
  const f = fixture(t); f.mount(); await until(() => cardShown(f));
  for (const value of ['https://other.test/?connect=contact&card=' + CARD, contact + '#profile/acct_other', contact + '&request=' + REQUEST]) {
    pasteEvent(f, value); assert.equal(hubPreview(f).hidden, true); assert.equal(f.input().value, value);
  }
  pasteEvent(f, contact, 'insertText'); assert.equal(hubPreview(f).hidden, true);
  f.input().emit('keydown', { key: 'Enter', isComposing: false });
  assert.equal(hubPreview(f).hidden, false); assert.equal(f.input().value, ''); assert.equal(f.calls.intents.length, 0);
  f.find('Отмена').click(); f.input().value = contact; f.input().emit('input', {});
  assert.equal(hubPreview(f).hidden, true); f.find('Открыть ссылку').click();
  assert.equal(hubPreview(f).hidden, false); assert.equal(f.calls.intents.length, 0);
});

test('editing or busy paste invalidates the old intent; late reads and disposed events cannot dispatch it', async t => {
  const delayed = deferred(); let block = false, reads = 0;
  const client = { getLocalState: () => { if (block) { reads++; return delayed.promise; } return Promise.resolve(fakeState()); },
    bootstrap: async () => { throw new Error('not authorized'); }, extension: async () => ({ cardId: CARD }) };
  const f = fixture(t, { client }), handle = f.mount(); await until(() => cardShown(f));
  pasteEvent(f, contact); pasteEvent(f, 'unfinished draft', 'insertText');
  assert.equal(hubPreview(f).hidden, true); assert.equal(f.input().value, 'unfinished draft');
  pasteEvent(f, contact); block = true; f.find('Продолжить').click(); await until(() => reads === 1);
  const device = `/?connect=device&request=${REQUEST}`; pasteEvent(f, device);
  assert.equal(hubPreview(f).hidden, true); assert.equal(f.input().value, device);
  block = false; delayed.resolve(fakeState()); await tick(); await tick(); assert.equal(f.calls.intents.length, 0);
  f.find('Открыть ссылку').click(); f.find('Продолжить').click(); await until(() => f.calls.intents.length === 1);
  assert.equal(f.calls.intents[0].intent.kind, 'device'); assert.equal(f.calls.intents[0].intent.id, REQUEST);
  const input = f.input(); handle.dispose(); input.value = contact; input.emit('input', { inputType: 'insertFromPaste' });
  await tick(); assert.equal(f.calls.intents.length, 1); assert.equal(f.host.children[0].children.length, 0);
});
