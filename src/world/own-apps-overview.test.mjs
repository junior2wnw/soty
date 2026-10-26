import test from 'node:test';
import assert from 'node:assert/strict';
import { unplacedOwnApps } from './own-apps-overview.mjs';

const app = (digit, owner = 'owner', patch = {}) => ({ id: `app-${digit.repeat(32)}`, name: `App ${digit}`, ownerAccountId: owner, state: 'ready', createdAt: Number(digit), ...patch });
test('only current owner active apps outside the saved field are shown, newest first', () => {
  const first = app('1'), second = app('2'), third = app('3'), foreign = app('4', 'another');
  const saved = [{ entity: { kind: 'app', id: first.id }, contextId: 'other-space' }];
  assert.deepEqual(unplacedOwnApps([first, second, third, foreign, app('5', 'owner', { state: 'revoked' }), second], 'owner', saved).map(item => item.id), [third.id, second.id]);
  assert.equal(saved[0].entity.id, first.id);
});
test('removing a shortcut does not recreate it; the app becomes directly available in the overview', () => {
  const registered = app('1'), doc = [{ entity: { kind: 'app', id: registered.id } }];
  assert.deepEqual(unplacedOwnApps([registered], 'owner', doc), []);
  doc.pop(); assert.equal(unplacedOwnApps([registered], 'owner', doc)[0].id, registered.id); assert.equal(doc.length, 0);
});
test('invalid or unowned records cannot become clickable personal entries', () => {
  const valid = app('1', 'owner', { state: 'offline' });
  assert.deepEqual(unplacedOwnApps([valid, app('2', 'other'), app('3', 'owner', { name: ' ' }), app('4', 'owner', { id: '#route' }), app('5', 'owner', { state: 'revoked' })], 'owner', []).map(item => item.id), [valid.id]);
  assert.throws(() => unplacedOwnApps(null, 'owner', []), /invalid_apps_overview/u);
});
