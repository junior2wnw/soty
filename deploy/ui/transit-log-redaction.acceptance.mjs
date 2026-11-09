import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { createHash, randomBytes } from 'node:crypto';
import { once } from 'node:events';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const binary = process.env.SOTY_UI_CADDY_BINARY;
assert.ok(binary, 'SOTY_UI_CADDY_BINARY must name the qualified Caddy');
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const directory = await mkdtemp(join(tmpdir(), 'soty-transit-log-gate-'));
const config = join(directory, 'Caddyfile'), runtimeLog = join(directory, 'runtime.log'), accessLog = join(directory, 'access.log');
const uriSecret = randomBytes(32).toString('base64url'), refererSecret = randomBytes(32).toString('base64url');
const redaction = [
  'format filter {', 'wrap json', 'fields {',
  'request>uri regexp `(/transit/subscriptions/)[^/?#]+` `${1}[redacted]`',
  'uri regexp `(/transit/subscriptions/)[^/?#]+` `${1}[redacted]`',
  'msg regexp `(/transit/subscriptions/)[^/?#]+` `${1}[redacted]`',
  'request>headers>Referer regexp `(/transit/subscriptions/)[^/?#]+` `${1}[redacted]`', '}', '}',
].join('\n');
const upstream = createServer((_req, res) => { res.writeHead(200, { 'content-type': 'application/json' }); res.end('{"ok":true}'); });
upstream.listen(0, '127.0.0.1'); await once(upstream, 'listening');
const upstreamPort = upstream.address().port;
const reserve = createServer(); reserve.listen(0, '127.0.0.1'); await once(reserve, 'listening');
const port = reserve.address().port; await new Promise(resolve => reserve.close(resolve));
const text = ['{', 'admin off', 'auto_https off', 'log default {', `output file ${runtimeLog}`, redaction, '}', '}',
  `http://127.0.0.1:${port} {`, 'log {', `output file ${accessLog}`, redaction, '}',
  'log_skip /transit/subscriptions/*', `reverse_proxy 127.0.0.1:${upstreamPort}`, '}'].join('\n');
await writeFile(config, text, { mode: 0o600 });
const adapted = spawnSync(binary, ['adapt', '--config', config, '--adapter', 'caddyfile'], { encoding: 'utf8', timeout: 10000 });
assert.equal(adapted.status, 0, 'installed Caddy must accept array-aware scoped redaction');
let child;
try {
  child = spawn(binary, ['run', '--config', config, '--adapter', 'caddyfile'], { stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env, HOME: directory, XDG_CONFIG_HOME: directory, XDG_DATA_HOME: directory } });
  let diagnostics = ''; for (const stream of [child.stdout, child.stderr]) stream.on('data', bytes => { diagnostics += bytes; if (diagnostics.length > 131072) child.kill(); });
  const origin = `http://127.0.0.1:${port}`, path = `/transit/subscriptions/${uriSecret}/profile.json`;
  const referer = `${origin}/transit/subscriptions/${refererSecret}/profile.json`;
  const deadline = Date.now() + 10000;
  for (;;) { try { const ready = await fetch(origin + '/ready'); if (ready.status === 200) break; } catch {} assert.ok(Date.now() < deadline); await new Promise(resolve => setTimeout(resolve, 25)); }
  const success = await fetch(origin + path, { headers: { Referer: referer } });
  assert.equal(success.status, 200); assert.deepEqual(await success.json(), { ok: true });
  await new Promise(resolve => upstream.close(resolve));
  const failure = await fetch(origin + path, { headers: { Referer: referer } }); assert.equal(failure.status, 502); await failure.arrayBuffer();
  const ordinary = await fetch(origin + '/ordinary', { headers: { Referer: referer } }); assert.equal(ordinary.status, 502); await ordinary.arrayBuffer();
  child.kill('SIGTERM'); await once(child, 'exit'); child = undefined;
  const runtime = await readFile(runtimeLog, 'utf8'), access = await readFile(accessLog, 'utf8');
  const all = runtime + access + diagnostics;
  assert.ok(!all.includes(uriSecret), 'URI capability reached a log'); assert.ok(!all.includes(refererSecret), 'Referer capability reached a log');
  const errors = runtime.trim().split('\n').map(line => JSON.parse(line)).filter(row => row.logger?.startsWith('http.log.error'));
  assert.ok(errors.length >= 2, 'Caddy did not record both unavailable upstream failures'); const denied = errors.find(row => row.request?.uri?.startsWith('/transit/subscriptions/'));
  assert.equal(denied.request.uri, '/transit/subscriptions/[redacted]/profile.json');
  assert.ok(Array.isArray(denied.request.headers.Referer), 'Caddy encodes Referer as an array');
  assert.equal(denied.request.headers.Referer.length, 1); assert.ok(denied.request.headers.Referer[0].includes('/[redacted]/'));
  const accessRows = access.trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
  assert.ok(accessRows.every(row => !row.request?.uri?.startsWith('/transit/subscriptions/')), 'capability path access logging is skipped');
  console.log(JSON.stringify({ ok: true, binarySha256: sha(await readFile(binary)), adaptedSha256: sha(adapted.stdout),
    realSuccess: true, unavailableUpstream: true, uriRedacted: true, distinctRefererRedacted: true, refererArrayVerified: true,
    capabilityAccessLogSkipped: true, ordinaryRoutePreserved: true, runtimeErrorsObserved: errors.length,
    runtimeLogSha256: sha(runtime), accessLogSha256: sha(access) }));
} finally {
  if (child) { child.kill('SIGTERM'); await once(child, 'exit'); }
  if (upstream.listening) await new Promise(resolve => upstream.close(resolve));
  await rm(directory, { recursive: true });
}
