import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request as httpRequest } from 'node:http';
import { once } from 'node:events';
import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { createHttpApp } from '../http-app.js';

test('immutable covers survive repeat requests while a changed manifest is revalidated', async t => {
  const parent = resolve(tmpdir()), directory = await mkdtemp(join(parent, 'soty-art-cache-'));
  const dist = join(directory, 'dist'), cover = 'app-art/notes/v001-123456abcdef/cover-640.abcdef123456.webp';
  await mkdir(dirname(join(dist, cover)), { recursive: true });
  await writeFile(join(dist, cover), 'cover bytes');
  await writeFile(join(dist, 'app-art/manifest.json'), JSON.stringify({ version: 1 }));
  await writeFile(join(dist, 'app-art/cover.webp'), 'unversioned bytes');
  const app = createHttpApp(dist, { dataDir: join(directory, 'data') });
  const server = createServer(app);
  t.after(async () => {
    server.closeAllConnections(); await new Promise(resolveClose => server.close(resolveClose));
    await app.locals.closeServices();
    assert.equal(dirname(resolve(directory)), parent);
    assert.ok(directory.startsWith(join(parent, 'soty-art-cache-')));
    await rm(directory, { recursive: true, force: true });
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const origin = `http://127.0.0.1:${server.address().port}`;
  const request = (path, headers = {}) => new Promise((resolveResponse, reject) => {
    const req = httpRequest(origin + '/' + path, { headers }, res => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk)); res.on('error', reject);
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString('utf8');
        resolveResponse({ status: res.statusCode, headers: new Headers(res.headers), text: async () => body, json: async () => JSON.parse(body) });
      });
    });
    req.setTimeout(5000, () => req.destroy(new Error('test_request_timeout')));
    req.on('error', reject); req.end();
  });
  const image = await request(cover); assert.equal(image.status, 200);
  assert.equal(image.headers.get('cache-control'), 'public, max-age=31536000, immutable');
  assert.equal(await image.text(), 'cover bytes');
  const cached = await request(cover, { 'if-none-match': image.headers.get('etag') });
  assert.equal(cached.status, 304);
  const manifest = await request('app-art/manifest.json');
  assert.doesNotMatch(manifest.headers.get('cache-control'), /immutable/u);
  assert.deepEqual(await manifest.json(), { version: 1 });
  await writeFile(join(dist, 'app-art/manifest.json'), JSON.stringify({ version: 200 }));
  const changed = await request('app-art/manifest.json', { 'if-none-match': manifest.headers.get('etag') });
  assert.equal(changed.status, 200); assert.deepEqual(await changed.json(), { version: 200 });
  const unversioned = await request('app-art/cover.webp');
  assert.doesNotMatch(unversioned.headers.get('cache-control'), /immutable/u);
});
