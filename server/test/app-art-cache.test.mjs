import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request as httpRequest } from 'node:http';
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import { mkdtemp, mkdir, writeFile, readFile, readdir, cp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHttpApp } from '../http-app.js';

const immutable = 'public, max-age=31536000, immutable';
const shell = '<!doctype html><title>Static cache boundary</title>';
const shippedArt = fileURLToPath(new URL('../../public/app-art/', import.meta.url));

async function fixture(t, prepare = async () => {}) {
  const parent = resolve(tmpdir()), directory = await mkdtemp(join(parent, 'soty-art-cache-'));
  const dist = join(directory, 'dist');
  let app, server;
  t.after(async () => {
    if (server?.listening) { server.closeAllConnections(); await new Promise(resolveClose => server.close(resolveClose)); }
    await app?.locals.closeServices();
    assert.equal(dirname(resolve(directory)), parent);
    assert.ok(directory.startsWith(join(parent, 'soty-art-cache-')));
    await rm(directory, { recursive: true, force: true });
  });
  await mkdir(join(dist, 'app-art'), { recursive: true });
  await writeFile(join(dist, 'index.html'), shell);
  await prepare({ dist, directory });
  app = createHttpApp(dist, { dataDir: join(directory, 'data') });
  server = createServer(app);
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  const origin = `http://127.0.0.1:${server.address().port}`;
  const request = (path, headers = {}, method = 'GET') => new Promise((resolveResponse, reject) => {
    const req = httpRequest(origin + '/' + path, { headers, method }, res => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk)); res.on('error', reject);
      res.on('end', () => {
        const bytes = Buffer.concat(chunks);
        resolveResponse({ status: res.statusCode, headers: new Headers(res.headers), bytes,
          text: async () => bytes.toString('utf8'), json: async () => JSON.parse(bytes.toString('utf8')) });
      });
    });
    req.setTimeout(5000, () => req.destroy(new Error('test_request_timeout')));
    req.on('error', reject); req.end();
  });
  return { dist, directory, request };
}

test('every shipped card, field and retained historical WebP has exact bytes and immutable HTTP caching', async t => {
  const { request } = await fixture(t, async ({ dist }) => cp(shippedArt, join(dist, 'app-art'), { recursive: true }));
  const selected = new Set();
  for (const name of ['manifest.json', 'field-profile.json']) {
    const manifest = JSON.parse(await readFile(join(shippedArt, name), 'utf8'));
    const widths = name === 'field-profile.json' ? [160, 320, 640, 960] : [320, 640, 960, 1440];
    const assets = Object.values(manifest.assets);
    assert.ok(assets.length > 0);
    for (const asset of assets) {
      assert.deepEqual(asset.renditions.map(item => item.width), widths, `${name}: ${asset.key}`);
      for (const item of asset.renditions) selected.add(item.url.slice('/app-art/'.length));
    }
  }
  const files = (await readdir(shippedArt, { recursive: true })).filter(file => file.endsWith('.webp'));
  const shipped = new Set(files.map(file => file.split(/[\\/]/u).join('/')));
  for (const url of selected) assert.ok(shipped.has(url), `Selected rendition is missing: ${url}`);
  assert.ok(files.length > selected.size, 'A retained historical version must remain covered, not just current manifests');
  for (const file of files) {
    const url = 'app-art/' + file.split(/[\\/]/u).join('/');
    const expected = await readFile(join(shippedArt, file));
    const hash = createHash('sha256').update(expected).digest('hex');
    assert.ok(file.endsWith('.' + hash.slice(0, 12) + '.webp'), url);
    assert.equal(expected.toString('ascii', 0, 4), 'RIFF');
    assert.equal(expected.toString('ascii', 8, 12), 'WEBP');
    const image = await request(url);
    assert.equal(image.status, 200, url);
    assert.equal(image.headers.get('content-type'), 'image/webp', url);
    assert.equal(image.headers.get('cache-control'), immutable, url);
    assert.deepEqual(image.bytes, expected, url);
    const cached = await request(url, { 'if-none-match': image.headers.get('etag') });
    assert.equal(cached.status, 304, url);
    assert.equal(cached.headers.get('cache-control'), immutable, url);
    assert.equal(cached.bytes.length, 0);
    const head = await request(url, {}, 'HEAD');
    assert.equal(head.status, 200, url);
    assert.equal(head.headers.get('cache-control'), immutable, url);
    assert.equal(head.headers.get('content-length'), String(expected.length), url);
    assert.equal(head.bytes.length, 0);
  }
  t.diagnostic(`Current renditions: ${selected.size}; all shipped including historical: ${files.length}`);
});

test('card and field manifests remain mutable and revalidate their changed bytes', async t => {
  const { dist, request } = await fixture(t, async ({ dist }) => {
    for (const name of ['manifest.json', 'field-profile.json']) await writeFile(join(dist, 'app-art', name), JSON.stringify({ version: 1 }));
  });
  for (const name of ['manifest.json', 'field-profile.json']) {
    const url = 'app-art/' + name, manifest = await request(url);
    assert.equal(manifest.status, 200);
    assert.equal(manifest.headers.get('cache-control'), 'public, max-age=0');
    assert.deepEqual(await manifest.json(), { version: 1 });
    assert.equal((await request(url, { 'if-none-match': manifest.headers.get('etag') })).status, 304);
    await writeFile(join(dist, url), JSON.stringify({ version: 200 }));
    const changed = await request(url, { 'if-none-match': manifest.headers.get('etag') });
    assert.equal(changed.status, 200); assert.deepEqual(await changed.json(), { version: 200 });
    assert.equal(changed.headers.get('cache-control'), 'public, max-age=0');
  }
});

test('unversioned or unsupported art names never receive immutable caching', async t => {
  const candidates = ['app-art/cover.webp', 'app-art/notes/cover-160.abcdef123456.webp',
    'app-art/notes/v001-123456abcdef/cover-80.abcdef123456.webp',
    'app-art/notes/v001-123456abcdef/cover-160.not-a-hash.webp'];
  const { request } = await fixture(t, async ({ dist }) => {
    for (const file of candidates) { await mkdir(dirname(join(dist, file)), { recursive: true }); await writeFile(join(dist, file), 'unapproved fixture bytes'); }
  });
  for (const file of candidates) {
    const response = await request(file);
    assert.equal(response.status, 200);
    assert.equal(response.headers.get('cache-control'), 'public, max-age=0', file);
  }
});

test('missing covers and private operator routes retain the no-store shell without exposing source bytes', async t => {
  const sentinel = 'PRIVATE_OPERATOR_SOURCE_SENTINEL';
  const { request } = await fixture(t, async ({ directory }) => {
    const privateFile = join(directory, 'output', 'app-art-history', 'notes', 'v001-123456abcdef', 'source.abcdef123456.png');
    await mkdir(dirname(privateFile), { recursive: true }); await writeFile(privateFile, sentinel);
  });
  for (const url of ['app-art/field-canvas/v001-123456abcdef/cover-160.abcdef123456.webp',
    'app-art/notes/v001-123456abcdef/source.abcdef123456.png',
    'app-art/notes/v001-123456abcdef/provenance.json', 'scripts/app-art/registry.json',
    'output/app-art-history/notes/v001-123456abcdef/source.abcdef123456.png']) {
    const response = await request(url);
    assert.equal(response.status, 200, 'Preserve the existing SPA fallback for unknown static URLs');
    assert.match(response.headers.get('content-type'), /^text\/html/u);
    assert.equal(response.headers.get('cache-control'), 'no-store', url);
    assert.equal(await response.text(), shell);
    assert.doesNotMatch(await response.text(), /PRIVATE_OPERATOR_SOURCE_SENTINEL/u);
  }
});
