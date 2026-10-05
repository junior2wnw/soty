import test from 'node:test';
import assert from 'node:assert/strict';
import { createAssistantFocusHandoff } from './app-builder-focus.mjs';

function page() {
  const neutral = {}, outside = { isConnected: true }, root = { isConnected: true };
  let active = neutral;
  const inside = new Set(); root.contains = node => inside.has(node);
  const tracker = createAssistantFocusHandoff({ root, getActiveElement: () => active, isNeutral: node => node === neutral || !node });
  const node = name => {
    const value = { name, isConnected: true, disabled: false, focus: () => { active = value; tracker.observe(value); } };
    inside.add(value); return value;
  };
  return { root, outside, neutral, tracker, node, get active() { return active; },
    blur: () => { active = neutral; },
    focus: value => { active = value; tracker.observe(value); },
    remove: value => { value.isConnected = false; inside.delete(value); if (active === value) active = neutral; } };
}

test('task completion and register ACK move stranded focus to the next useful action', () => {
  const p = page(), submit = p.node('submit'), status = p.node('activity'); p.focus(submit);
  const start = p.tracker.capture(); p.remove(submit); assert.equal(p.tracker.restore(start, status), true); assert.equal(p.active, status);
  const complete = p.tracker.capture(); p.remove(status); const add = p.node('accept');
  assert.equal(p.tracker.restore(complete, add), true); assert.equal(p.active, add);
  add.disabled = true; p.blur(); const accepted = p.tracker.capture(); p.remove(add); const launch = p.node('launch');
  assert.equal(p.tracker.restore(accepted, launch), true); assert.equal(p.active, launch);
});

test('async replacement never steals focus after the user moved to another part of the page', () => {
  const p = page(), add = p.node('accept'), launch = p.node('launch'); p.focus(add);
  const accepted = p.tracker.capture(); p.focus(p.outside); p.remove(add);
  assert.equal(p.tracker.restore(accepted, launch), false); assert.equal(p.active, p.outside);
  p.focus(launch); const next = p.tracker.capture(); p.tracker.releaseOutside(p.outside); p.blur(); p.remove(launch);
  assert.equal(p.tracker.restore(next, p.node('new')), false); assert.equal(p.active, p.neutral);
});

test('a fresh neutral page has no focus claim; retained controls preserve their own identity', () => {
  const p = page(); assert.equal(p.tracker.capture(), null);
  const reset = p.node('reset'), launch = p.node('launch'); p.focus(reset);
  const claim = p.tracker.capture();
  assert.equal(p.tracker.restore(claim, launch), true); assert.equal(p.active, reset);
});

test('a handoff is restricted to its replacement area, even inside the same assistant', () => {
  const p = page(), add = p.node('accept'), elsewhere = p.node('details'), launch = p.node('launch');
  const actionArea = { contains: node => node === add };
  p.focus(add); const accepted = p.tracker.capture(actionArea); p.focus(elsewhere); p.remove(add);
  assert.equal(p.tracker.restore(accepted, launch), false); assert.equal(p.active, elsewhere);
});
