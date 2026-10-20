import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve, sep } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createRequire } from 'node:module';
import vm from 'node:vm';

const root = process.env.SOTY_REVIEW_FIELD_ROOT
  ? pathToFileURL(`${resolve(process.env.SOTY_REVIEW_FIELD_ROOT)}${sep}`) : new URL('../../', import.meta.url);
const ts = createRequire(new URL('package.json', root))('typescript');
const source = readFileSync(process.env.SOTY_REVIEW_SHEET_SOURCE || new URL('src/world/sheet-grip-dismiss.ts', root), 'utf8');
const compiled = ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText;
const context = vm.createContext({ exports: {}, AbortController });
new vm.Script(compiled).runInContext(context);
const { bindSheetGripDismiss } = context.exports;
assert.equal(typeof bindSheetGripDismiss, 'function', 'actual production export');

// Small event/capture DOM ports. The entire shipped helper executes unchanged.
class Target {
  listeners = new Map();
  addEventListener(type, callback, options = {}) {
    if (options.signal?.aborted) return;
    const row = { callback, options };
    if (!this.listeners.has(type)) this.listeners.set(type, []);
    this.listeners.get(type).push(row);
    options.signal?.addEventListener('abort', () => this.removeEventListener(type, callback), { once: true });
  }
  removeEventListener(type, callback) { this.listeners.set(type, (this.listeners.get(type) || []).filter(row => row.callback !== callback)); }
  emit(type, event = {}) {
    for (const row of [...(this.listeners.get(type) || [])]) {
      if (row.options.signal?.aborted) continue;
      row.callback(event);
      if (row.options.once) this.removeEventListener(type, row.callback);
      if (event.stopped) break;
    }
  }
  count() { return [...this.listeners.values()].reduce((sum, rows) => sum + rows.filter(row => !row.options.signal?.aborted).length, 0); }
}
const fixture = ({ aborted = false } = {}) => {
  const doc = new Target(), view = new Target(), grip = new Target(), content = new Target(), captures = new Set(), signal = new AbortController();
  const state = { closes: 0, current: true, rejectCapture: false };
  doc.defaultView = view; grip.ownerDocument = doc; grip.isConnected = true;
  grip.hasPointerCapture = id => captures.has(id);
  grip.setPointerCapture = id => { if (state.rejectCapture) throw new Error('capture unavailable'); captures.add(id); };
  grip.releasePointerCapture = id => captures.delete(id);
  if (aborted) signal.abort();
  const dispose = bindSheetGripDismiss(grip, () => state.closes++, { signal: signal.signal, isCurrent: () => state.current });
  const event = (type, overrides = {}) => {
    const event = { pointerId: 1, pointerType: 'touch', button: 0, buttons: 1, isPrimary: true, clientX: 100, clientY: 100, target: grip,
      cancelable: true, defaultPrevented: false, detail: 1,
      preventDefault() { this.defaultPrevented = true; }, stopImmediatePropagation() { this.stopped = true; }, ...overrides };
    if (['pointerdown', 'pointermove', 'pointerup', 'pointercancel'].includes(type)) doc.emit(type, event);
    if (event.target === grip && ['pointerdown', 'click', 'lostpointercapture'].includes(type)) grip.emit(type, event);
    if (['blur', 'pagehide'].includes(type)) view.emit(type, event);
    return event;
  };
  return { state, grip, content, signal, captures, dispose, event, listeners: () => doc.count() + view.count() + grip.count() };
};
const swipe = (f, dx = 0, dy = 90) => { f.event('pointerdown'); f.event('pointermove', { clientX: 100 + dx, clientY: 100 + dy }); return f.event('pointerup', { clientX: 100 + dx, clientY: 100 + dy }); };

test('tap closes on native click, never on pointerdown or pointerup; keyboard button activation also closes', () => {
  const f = fixture(); f.event('pointerdown'); assert.equal(f.state.closes, 0); f.event('pointerup'); assert.equal(f.state.closes, 0);
  assert.equal(f.event('click').defaultPrevented, false); assert.equal(f.state.closes, 1);
  const k = fixture(); assert.equal(k.event('click', { detail: 0 }).defaultPrevented, false); assert.equal(k.state.closes, 1);
});

test('a downward handle swipe closes only on release beyond 64px and consumes its native trailing click', () => {
  const f = fixture(); f.event('pointerdown'); f.event('pointermove', { clientY: 190 }); assert.equal(f.state.closes, 0);
  f.event('pointerup', { clientY: 190 }); assert.equal(f.state.closes, 1); assert.equal(f.captures.size, 0);
  assert.equal(f.event('click').defaultPrevented, true); assert.equal(f.state.closes, 1);
});

test('short, upward, horizontal and diagonal drags remain open and consume accidental physical click', () => {
  for (const [dx, dy] of [[0, 64], [0, -100], [90, 20], [100, 90]]) {
    const f = fixture(); swipe(f, dx, dy); assert.equal(f.state.closes, 0, `${dx},${dy}`);
    assert.equal(f.event('click').defaultPrevented, true); assert.equal(f.captures.size, 0);
    assert.equal(f.event('click', { detail: 0 }).defaultPrevented, false, 'explicit keyboard action remains available'); assert.equal(f.state.closes, 1);
  }
});

test('release coordinates enforce the threshold even when no intermediate move sample arrived', () => {
  const f = fixture(); f.event('pointerdown'); assert.equal(f.state.closes, 0); f.event('pointerup', { clientY: 190 });
  assert.equal(f.state.closes, 1); assert.equal(f.event('click').defaultPrevented, true);
});

test('a gesture beginning in menu content keeps native scroll and never belongs to the handle', () => {
  const f = fixture(); f.event('pointerdown', { target: f.content }); const move = f.event('pointermove', { target: f.content, clientY: 240 });
  assert.equal(move.defaultPrevented, false); f.event('pointerup', { target: f.content, clientY: 240 }); assert.equal(f.state.closes, 0); assert.equal(f.captures.size, 0);
});

test('a second pointer anywhere in this document cancels the owned handle gesture until a fresh sequence', () => {
  const f = fixture(); f.event('pointerdown'); f.event('pointermove', { clientY: 190 });
  f.event('pointerdown', { pointerId: 2, isPrimary: false, target: f.content }); assert.equal(f.captures.size, 0);
  f.event('pointerup', { clientY: 210 }); f.event('pointerup', { pointerId: 2, isPrimary: false, target: f.content });
  assert.equal(f.state.closes, 0); assert.equal(f.event('click').defaultPrevented, true);
  swipe(f); assert.equal(f.state.closes, 1);
});

test('starting on the handle with a second nonprimary pointer cannot acquire or close it', () => {
  const f = fixture(); f.event('pointerdown', { target: f.content }); f.event('pointerdown', { pointerId: 2, isPrimary: false });
  f.event('pointermove', { pointerId: 2, clientY: 200 }); f.event('pointerup', { pointerId: 2, clientY: 200 }); f.event('pointerup', { target: f.content });
  assert.equal(f.state.closes, 0); assert.equal(f.captures.size, 0); assert.equal(f.event('click').defaultPrevented, true);
});

for (const end of ['pointercancel', 'blur', 'pagehide', 'lostpointercapture']) test(`${end} cancels without closing on late release/click`, () => {
  const f = fixture(); f.event('pointerdown'); f.event('pointermove', { clientY: 190 });
  if (end === 'lostpointercapture') f.captures.clear();
  f.event(end); f.event('pointerup', { clientY: 240 }); assert.equal(f.state.closes, 0); assert.equal(f.captures.size, 0);
  assert.equal(f.event('click').defaultPrevented, true);
});

test('a stale lostcapture notification cannot cancel a pointer whose new capture is still owned', () => {
  const f = fixture(); f.event('pointerdown'); f.event('lostpointercapture'); f.event('pointerup', { clientY: 190 }); assert.equal(f.state.closes, 1);
});

test('non-touch zero-buttons move cancels a lost release while touch zero-buttons remains valid', () => {
  for (const pointerType of ['mouse', 'touch']) {
    const f = fixture(); f.event('pointerdown', { pointerType }); f.event('pointermove', { pointerType, buttons: 0, clientY: 190 });
    f.event('pointerup', { pointerType, clientY: 190 }); assert.equal(f.state.closes, pointerType === 'touch' ? 1 : 0);
  }
});

test('capture failure and handled/nonprimary/right-button starts cannot become a swipe close', () => {
  for (const config of [{ rejectCapture: true }, { defaultPrevented: true }, { isPrimary: false }, { button: 2 }]) {
    const f = fixture(); f.state.rejectCapture = !!config.rejectCapture; f.event('pointerdown', config);
    f.event('pointermove', { clientY: 200 }); f.event('pointerup', { clientY: 200 }); assert.equal(f.state.closes, 0); assert.equal(f.captures.size, 0);
  }
});

test('a dialog/account ownership change or disconnected handle denies both release and click', () => {
  for (const change of [f => { f.state.current = false; }, f => { f.grip.isConnected = false; }]) {
    const f = fixture(); f.event('pointerdown'); change(f); f.event('pointerup', { clientY: 200 });
    assert.equal(f.state.closes, 0); assert.equal(f.event('click').defaultPrevented, true); assert.equal(f.captures.size, 0);
  }
});

test('dispose/abort remove all listeners and release capture; late old events cannot close a replacement sheet', () => {
  for (const abort of [false, true]) {
    const f = fixture(); f.event('pointerdown'); assert.ok(f.listeners() > 0);
    if (abort) f.signal.abort(); else { f.dispose(); f.dispose(); }
    assert.equal(f.listeners(), 0); assert.equal(f.captures.size, 0);
    f.event('pointerup', { clientY: 200 }); f.event('click'); assert.equal(f.state.closes, 0);
  }
  const closed = fixture({ aborted: true }); assert.equal(closed.listeners(), 0); closed.event('click'); assert.equal(closed.state.closes, 0);
});
