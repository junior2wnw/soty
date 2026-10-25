import test from 'node:test';
import assert from 'node:assert/strict';
import { readWorkspaceReturn } from './workspace-return.mjs';

test('a return retains the exact scoped search and list, but crosses no account boundary', () => {
  const value = { accountId: 'A', route: '#world?q=a%2Bb&kind=app', scroll: 240, presentation: 'list', listQuery: 'доска', focusEntity: 'app:one' };
  assert.deepEqual(readWorkspaceReturn(value, 'A'), value);
  assert.equal(readWorkspaceReturn(value, 'B'), null);
});

test('a return cannot launch another app, escape the shell, or smuggle arbitrary state', () => {
  for (const route of ['https://other.invalid', '#app/one', '#launch/one', '#mine\n#world', '#mine-other'])
    assert.equal(readWorkspaceReturn({ accountId: 'A', route }, 'A'), null);
  assert.deepEqual(readWorkspaceReturn({ accountId: 'A', route: '#messages/thread', scroll: -4, token: 'must-not-copy', presentation: 'admin' }, 'A'),
    { accountId: 'A', route: '#messages/thread', scroll: 0 });
});
