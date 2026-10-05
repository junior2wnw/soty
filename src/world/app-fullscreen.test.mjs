import test from 'node:test';
import assert from 'node:assert/strict';
import { mountAppFullscreen } from './app-fullscreen.mjs';

function fixture({ supported = true, reject = false, delayed = false } = {}) {
  const doc = Object.assign(new EventTarget(), { fullscreenEnabled: supported, fullscreenElement: null });
  const classes = new Set(), states = []; let connected = true, current = true, finish, calls = 0, exits = 0;
  const shell = { tagName: 'NAV', inert: false }, alreadyInert = { tagName: 'ASIDE', inert: true };
  const parent = { children: [], parentElement: null }; doc.body = parent;
  const frame = { src: '/same-runtime', draft: 'unsaved text', connected: true };
  const screen = { ownerDocument: doc, parentElement: parent, children: [frame],
    get isConnected() { return connected; },
    classList: { toggle(name, enabled) { if (enabled) classes.add(name); else classes.delete(name); } },
    requestFullscreen() {
      calls++;
      if (reject) return Promise.reject(new Error('permission rejected'));
      const apply = () => { doc.fullscreenElement = screen; doc.dispatchEvent(new Event('fullscreenchange')); };
      if (delayed) return new Promise(done => { finish = () => { apply(); done(); }; });
      apply(); return Promise.resolve();
    } };
  parent.children.push(shell, screen, alreadyInert);
  doc.exitFullscreen = async () => { exits++; doc.fullscreenElement = null; doc.dispatchEvent(new Event('fullscreenchange')); };
  const controller = mountAppFullscreen({ screen, isCurrent: () => current, onChange: state => states.push(state) });
  return { controller, doc, screen, frame, shell, alreadyInert, classes, states,
    calls: () => calls, exits: () => exits, finish: () => finish(), stale() { current = false; connected = false; } };
}
test('native enter, button restore and browser Escape keep the same connected runtime and draft', async () => {
  const f = fixture(); const frame = f.screen.children[0];
  await f.controller.toggle(); assert.equal(f.doc.fullscreenElement, f.screen); assert.equal(f.states.at(-1).active, true);
  await f.controller.toggle(); assert.equal(f.doc.fullscreenElement, null); assert.equal(f.states.at(-1).active, false);
  await f.controller.toggle(); f.doc.fullscreenElement = null; f.doc.dispatchEvent(new Event('fullscreenchange'));
  assert.equal(f.states.at(-1).active, false);
  assert.equal(f.screen.children[0], frame); assert.equal(frame.connected, true); assert.equal(frame.draft, 'unsaved text');
  f.controller.dispose();
});
test('unsupported or refused API expands the existing stage in the window and restores surrounding focusability', async () => {
  for (const options of [{ supported: false }, { reject: true }]) {
    const f = fixture(options); await f.controller.toggle();
    assert.equal(f.classes.has('is-window-expanded'), true); assert.equal(f.shell.inert, true);
    const escape = new Event('keydown', { cancelable: true }); Object.defineProperty(escape, 'key', { value: 'Escape' });
    f.doc.dispatchEvent(escape); await new Promise(done => setImmediate(done));
    assert.equal(f.classes.has('is-window-expanded'), false); assert.equal(f.shell.inert, false); assert.equal(f.alreadyInert.inert, true);
    assert.equal(f.screen.children[0], f.frame); f.controller.dispose();
  }
});

test('a delivered Escape exits the current native stage without replacing its runtime', async () => {
  const f = fixture(); await f.controller.toggle();
  const escape = new Event('keydown', { cancelable: true }); Object.defineProperty(escape, 'key', { value: 'Escape' });
  f.doc.dispatchEvent(escape); await new Promise(done => setImmediate(done));
  assert.equal(f.doc.fullscreenElement, null); assert.equal(f.states.at(-1).active, false);
  assert.equal(f.exits(), 1); assert.equal(f.screen.children[0], f.frame); assert.equal(f.frame.draft, 'unsaved text');
  // A managed dialog or another application's fullscreen keeps its own Escape.
  const other = {}; f.doc.fullscreenElement = other;
  const unrelated = new Event('keydown', { cancelable: true }); Object.defineProperty(unrelated, 'key', { value: 'Escape' });
  f.doc.dispatchEvent(unrelated); assert.equal(f.doc.fullscreenElement, other); assert.equal(unrelated.defaultPrevented, false);
  f.doc.fullscreenElement = f.screen;
  const handled = new Event('keydown', { cancelable: true }); Object.defineProperty(handled, 'key', { value: 'Escape' }); handled.preventDefault();
  f.doc.dispatchEvent(handled); assert.equal(f.doc.fullscreenElement, f.screen); assert.equal(f.exits(), 1);
  f.controller.dispose();
});
test('leaving an app during a delayed request exits only its own late fullscreen', async () => {
  const f = fixture({ delayed: true }); const request = f.controller.toggle();
  void f.controller.toggle(); assert.equal(f.calls(), 1, 'no duplicate request while pending');
  f.stale(); f.controller.dispose(); f.finish(); await request;
  assert.equal(f.doc.fullscreenElement, null); assert.equal(f.exits(), 1);
  assert.equal(f.screen.children[0], f.frame);
});
test('disposing or clicking a stale stage never exits another fullscreen app', async () => {
  const f = fixture(), other = {};
  f.doc.fullscreenElement = other; await f.controller.toggle(); f.stale(); f.controller.dispose();
  assert.equal(f.doc.fullscreenElement, other); assert.equal(f.exits(), 0); assert.equal(f.calls(), 0);
});
test('fallback teardown restores inert state and does not overwrite an independently restored sibling', async () => {
  const f = fixture({ supported: false }); await f.controller.toggle(); f.shell.inert = false;
  f.controller.dispose(); assert.equal(f.shell.inert, false); assert.equal(f.alreadyInert.inert, true);
  assert.equal(f.classes.has('is-window-expanded'), false);
});
