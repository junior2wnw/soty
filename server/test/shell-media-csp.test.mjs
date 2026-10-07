import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { createHttpApp } from '../http-app.js';

test('served shell permits local RAM audio previews while script, frame and device boundaries stay closed', async t => {
  const parent = resolve(tmpdir()), directory = await mkdtemp(join(parent, 'soty-shell-media-csp-'));
  const dist = join(directory, 'dist');
  let app, server;
  t.after(async () => {
    if (server?.listening) { server.closeAllConnections(); await new Promise(done => server.close(done)); }
    await app?.locals.closeServices();
    assert.equal(dirname(resolve(directory)), parent);
    assert.ok(directory.startsWith(join(parent, 'soty-shell-media-csp-')));
    await rm(directory, { recursive: true, force: true });
  });
  await mkdir(dist);
  await writeFile(join(dist, 'index.html'), '<!doctype html><title>RAM audio preview boundary</title>');
  app = createHttpApp(dist, { dataDir: join(directory, 'data') });
  server = createServer(app); server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const response = await fetch(`http://127.0.0.1:${server.address().port}/`, { signal: AbortSignal.timeout(5000) });
  assert.equal(response.status, 200);
  assert.match(await response.text(), /RAM audio preview boundary/);
  const policy = response.headers.get('content-security-policy');
  const directives = policy.split(';').map(part => part.trim().split(/\s+/u));
  const values = name => directives.filter(part => part[0] === name).map(part => part.slice(1));
  assert.deepEqual(values('media-src'), [["'self'", 'blob:', 'data:']]);
  assert.deepEqual(values('default-src'), [["'self'"]]);
  assert.deepEqual(values('script-src'), [["'self'"]]);
  assert.deepEqual(values('object-src'), [["'none'"]]);
  assert.deepEqual(values('frame-ancestors'), [["'none'"]]);
  assert.deepEqual(values('form-action'), [["'self'"]]);
  assert.deepEqual(values('frame-src'), [["'self'"]]);
  assert.match(response.headers.get('permissions-policy'), /(?:^|, )microphone=\(self\)(?:,|$)/u);
  assert.equal(response.headers.get('cross-origin-opener-policy'), 'same-origin');
  assert.equal(response.headers.get('x-frame-options'), 'DENY');
});
