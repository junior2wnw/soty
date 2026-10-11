import test from 'node:test';
import assert from 'node:assert/strict';
import { copyVisibleText } from './copy-visible-text.ts';

test('copy starts synchronously in the explicit click and preserves exact visible answer formatting', async () => {
  const text = 'Synthetic reply\n\nSecond paragraph'; const notices = []; let written;
  const operation = copyVisibleText({ text, write: value => { written = value; return Promise.resolve(); },
    isCurrent: () => true, onStatus: value => notices.push(value), success: 'copied', failure: 'failed' });
  assert.equal(written, text); assert.equal(await operation, true); assert.deepEqual(notices, ['copied']);
});
test('closed or changed account UI cannot initiate clipboard export', async () => {
  assert.equal(await copyVisibleText({ text: 'Synthetic private answer', write: () => assert.fail('Must not write'),
    isCurrent: () => false, onStatus: () => assert.fail('No stale status'), success: 'copied', failure: 'failed' }), false);
});
for (const rejected of [false, true]) test(`late clipboard ${rejected ? 'rejection' : 'completion'} cannot update a replaced answer`, async () => {
  let current = true, settle; const notices = [];
  const pending = copyVisibleText({ text: 'Synthetic answer',
    write: () => new Promise((resolve, reject) => { settle = () => rejected ? reject(new Error('unavailable')) : resolve(); }),
    isCurrent: () => current, onStatus: value => notices.push(value), success: 'copied', failure: 'failed' });
  current = false; settle(); assert.equal(await pending, false); assert.deepEqual(notices, []);
});
test('unavailable or denied clipboard leaves the answer intact and reports a manual-selection fallback', async () => {
  for (const write of [undefined, () => { throw new Error('denied'); }, () => Promise.reject(new Error('denied'))]) {
    const notices = [];
    assert.equal(await copyVisibleText({ text: 'Synthetic answer', write, isCurrent: () => true,
      onStatus: value => notices.push(value), success: 'copied', failure: 'select text' }), false);
    assert.deepEqual(notices, ['select text']);
  }
});
