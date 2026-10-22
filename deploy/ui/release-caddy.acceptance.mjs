import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { once } from 'node:events';
import { createUiRelease, writeUiRelease, createCaddyOverlay } from './release.mjs';
import { fixtureDist } from './test-support.mjs';

const binary = process.env.SOTY_UI_CADDY_BINARY;
assert.ok(binary, 'run explicitly with SOTY_UI_CADDY_BINARY pointing to the qualified Caddy2.11.2 binary');
assert.match(execFileSync(binary, ['version'], { encoding: 'utf8' }), /^v2\.11\.2\b/u);
const adapt = text => {
  try { return JSON.parse(execFileSync(binary, ['adapt', '--config', '-', '--adapter', 'caddyfile'], { input: text, maxBuffer: 16 * 1024 * 1024, stdio: ['pipe', 'pipe', 'pipe'] })); }
  catch { throw new Error('caddy_adapt_rejected'); }
};
const configuration = `{
 admin off
 auto_https off
}
http://127.0.0.1:19081 {
 @hive query project=*
 handle @hive {\n respond "HIVE original"\n }
 handle /old-special {\n respond "old special"\n }
 handle {
  encode gzip
  handle {
   reverse_proxy 127.0.0.1:18182 {
    flush_interval -1
    transport http {\n dial_timeout 5s\n }
     }
  }
 }
}`;

test('actual Caddy2.11.2 preserves routes/security/status and serves both UI generations without restarting the backend', { timeout: 20000 }, async t => {
  const root = mkdtempSync(path.join(tmpdir(), 'soty-ui-caddy-')); t.after(() => rmSync(root, { recursive: true, force: true }));
  const dist = fixtureDist(path.join(root, 'dist')), old = fixtureDist(path.join(root, 'old'), { 'assets/old-12345678.js': 'old lazy import' });
  const release = createUiRelease(dist, [old]), immutableRoot = path.join(root, release.id);
  writeUiRelease(release, immutableRoot);
  const overlay = await createCaddyOverlay({ caddyfile: configuration, release, root: immutableRoot, adapt, host: '127.0.0.1' });
  assert.notEqual(overlay.candidateSha256, overlay.baselineSha256);
  for (const tamper of ['route', 'group partition']) {
    const changedAdapt = text => {
      const config = adapt(text);
      if (text.includes('soty_ui_release') && text.includes('HIVE original')) {
        const routes = config.apps.http.servers.srv0.routes[0].handle[0].routes;
        if (tamper === 'route') routes[0].match = [{ path: ['/stolen'] }]; else routes[0].group = 'group99999';
      }
      return config;
    };
    await assert.rejects(createCaddyOverlay({ caddyfile: configuration, release, root: immutableRoot, adapt: changedAdapt, host: '127.0.0.1' }), { code: 'ui_fallback_unknown_or_ambiguous' });
  }
  for (const changed of [configuration.replace('flush_interval -1', 'header_up X-Unsupported yes'), configuration.replace('handle /old-special', 'handle /old-special') + '\nhttp://127.0.0.1:19082 {\n reverse_proxy 127.0.0.1:18182\n}']) {
    await assert.rejects(createCaddyOverlay({ caddyfile: changed, release, root: immutableRoot, adapt, host: '127.0.0.1' }), { code: 'ui_fallback_unknown_or_ambiguous' });
  }
  const requests = [], backend = createServer((req, res) => {
    requests.push({ url: req.url, headers: req.headers });
    res.setHeader('Content-Security-Policy', "default-src 'self'; frame-src https://dynamic.example");
    res.setHeader('Permissions-Policy', 'microphone=(self)'); res.setHeader('X-Frame-Options', 'DENY');
    res.setHeader('Content-Type', 'text/html'); res.setHeader('ETag', '"old-validator"');
    if (req.url.startsWith('/api/')) { res.statusCode = 403; res.end('original API denied'); return; }
    if (req.url === '/?blocked=1') { res.statusCode = 403; res.end('original denied'); return; }
    if (req.url === '/?redirect=1') { res.statusCode = 302; res.setHeader('Location', '/old-special'); res.end(); return; }
    if (req.headers['if-none-match']) { res.statusCode = 304; res.end(); return; }
    if (req.headers['if-match'] || req.headers['if-unmodified-since']) { res.statusCode = 412; res.end(); return; }
    if (req.url.startsWith('/agent/')) { res.end('original agent'); return; }
    res.end('OLD UI');
  });
  backend.listen(18182, '127.0.0.1'); await once(backend, 'listening');
  t.after(async () => { backend.closeAllConnections(); await new Promise(resolve => backend.close(resolve)); });
  const configFile = path.join(root, 'Caddyfile'); writeFileSync(configFile, overlay.caddyfile);
  const caddy = spawn(binary, ['run', '--config', configFile, '--adapter', 'caddyfile'], { stdio: ['ignore', 'ignore', 'pipe'],
    env: { ...process.env, XDG_CONFIG_HOME: path.join(root, 'config'), XDG_DATA_HOME: path.join(root, 'data') } });
  caddy.stderr.resume();
  t.after(async () => { if (caddy.exitCode === null) { caddy.kill('SIGTERM'); await once(caddy, 'exit'); } });
  const base = 'http://127.0.0.1:19081';
  for (let attempt = 0; ; attempt++) {
    try { if ((await fetch(base)).ok) break; } catch { /* Wait only for this test's own process. */ }
    assert.ok(attempt < 100 && caddy.exitCode === null, 'synthetic Caddy starts'); await new Promise(resolve => setTimeout(resolve, 20));
  }
  let response = await fetch(base); assert.match(await response.text(), /NEW UI/u);
  assert.equal(response.headers.get('content-security-policy'), "default-src 'self'; frame-src https://dynamic.example");
  assert.equal(response.headers.get('permissions-policy'), 'microphone=(self)'); assert.equal(response.headers.get('cache-control'), 'no-store');
  const validator = response.headers.get('etag'); assert.notEqual(validator, '"old-validator"');
  response = await fetch(base, { headers: { 'If-None-Match': validator } }); assert.equal(response.status, 304);
  response = await fetch(base, { headers: { 'If-Match': validator } }); assert.equal(response.status, 200); assert.match(await response.text(), /NEW UI/u);
  response = await fetch(base, { method: 'HEAD' }); assert.equal(await response.text(), ''); assert.equal(response.status, 200);
  for (const url of ['/sw.js', '/manifest.webmanifest']) {
    response = await fetch(base + url); assert.equal(response.status, 200); assert.equal(response.headers.get('cache-control'), 'no-store');
    assert.notEqual(response.headers.get('content-type'), 'text/html');
  }
  for (const [url, expected] of [['/assets/index-abcdefgh.js', 'export const version = "new";'], ['/assets/old-12345678.js', 'old lazy import']]) {
    response = await fetch(base + url); assert.equal(await response.text(), expected); assert.equal(response.headers.get('cache-control'), 'public, max-age=31536000, immutable');
  }
  response = await fetch(base + '/assets/index-abcdefgh.js', { headers: { Range: 'bytes=0-5', 'If-Range': '"old-validator"' } });
  assert.equal(response.status, 200, 'new validator controls If-Range, not the old upstream validator');
  response = await fetch(base + '/assets/index-abcdefgh.js', { headers: { Range: 'bytes=0-5' } }); assert.equal(response.status, 206); assert.equal(await response.text(), 'export');
  response = await fetch(base + '/assets/index-abcdefgh.js', { headers: { 'If-Unmodified-Since': 'Fri, 01 Jan 2100 00:00:00 GMT' } }); assert.equal(response.status, 200);
  for (const item of requests.filter(item => item.url === '/' || item.url.startsWith('/assets/'))) {
    for (const name of ['if-none-match', 'if-modified-since', 'if-match', 'if-unmodified-since', 'range', 'if-range']) assert.equal(item.headers[name], undefined);
  }
  for (const [url, status, text] of [['/api/test', 403, 'original API denied'], ['/?blocked=1', 403, 'original denied'], ['/?project=retained', 200, 'HIVE original'],
    ['/?hive=retained', 200, 'OLD UI'], ['/old-special', 200, 'old special'], ['/agent/manifest.json', 200, 'original agent'], ['/projects/example/index.html', 200, 'OLD UI'], ['/unknown.js', 200, 'OLD UI']]) {
    response = await fetch(base + url); assert.equal(response.status, status); assert.equal(await response.text(), text);
  }
  response = await fetch(base + '/?redirect=1', { redirect: 'manual' }); assert.equal(response.status, 302); assert.equal(response.headers.get('location'), '/old-special');
  response = await fetch(base + '/', { method: 'POST' }); assert.equal(await response.text(), 'OLD UI');
  writeFileSync(configFile, configuration); caddy.kill('SIGUSR1');
  for (let attempt = 0; ; attempt++) {
    if (await (await fetch(base)).text() === 'OLD UI') break;
    assert.ok(attempt < 100, 'rollback reload completes'); await new Promise(resolve => setTimeout(resolve, 20));
  }
  assert.equal(backend.listening, true); assert.equal(caddy.exitCode, null);
});
