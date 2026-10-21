import test from 'node:test';
import assert from 'node:assert/strict';
import { checkedScopedEntryPath, createScopedLaunchPath } from '../server/launch-path.mjs';

test('selected navigation admits only exact /embed with a bounded opaque same-document fragment', () => {
  for (const path of ['/embed', '/embed#', '/embed#document=pag_x&block=hero&invite=opaque',
    '/embed#/reader/42?tab=comments', '/embed#раздел%20два', '/embed#' + 'a'.repeat(2048),
    '/embed#next=https%3A%2F%2Fother.example%2F_soty%2Fsession']) {
    const result = createScopedLaunchPath(path);
    assert.equal(result.entryPath, path);
    const boot = new URL(result.bootPath, 'https://app.test');
    assert.equal(boot.searchParams.get('path'), '/embed');
    assert.equal(boot.searchParams.size, 1);
    assert.equal(result.bootPath, '/_soty/boot?path=%2Fembed');
    assert.equal(boot.hash, '');
    const destination = new URL(path, 'https://app.test');
    assert.equal(destination.origin, 'https://app.test');
    assert.equal(destination.pathname, '/embed');
    assert.equal(destination.search, '');
  }
});

test('selected navigation rejects path/query/redirect/reserved escapes and malformed or unbounded fragments', () => {
  for (const path of ['/embed?x=1#document=pag_x', '/embed?#x', '/embed/?x', '/other#x',
    'https://other.example/embed#x', '//other.example/embed#x', '/%65mbed#x', '/a/../embed#x',
    '/embed/../_soty/session#x', '/_soty/boot#x', '/_soty/guide', '/embed%23document=x',
    '/embed#x#y', '/embed#x\\y', '/embed#x y', '/embed#%00', '/embed#%0a', '/embed#%0D',
    '/embed#%7f', '/embed#%5c', '/embed#%', '/embed#%C0%AF', '/embed#\ud800',
    '/embed#' + 'a'.repeat(2049)]) {
    assert.equal(checkedScopedEntryPath(path), null, path);
    assert.throws(() => createScopedLaunchPath(path), { code: 'app_scoped_admission_required' });
  }
});
