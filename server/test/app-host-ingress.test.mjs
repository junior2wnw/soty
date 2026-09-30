import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { connect } from 'node:net';
import { fork } from 'node:child_process';
import { mkdtemp, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createHttpApp } from '../http-app.js';

function rawRequest(port, headers, target = '/health', upgrade = false) {
  return new Promise((resolve, reject) => {
    const socket = connect(port, '127.0.0.1');
    let body = '';
    socket.setTimeout(3_000, () => socket.destroy(new Error('test_request_timeout')));
    socket.on('error', reject);
    socket.on('data', bytes => { body += bytes; });
    socket.once('end', () => resolve(body));
    socket.once('connect', () => socket.write(`GET ${target} HTTP/1.1\r\n${headers}\r\nConnection: ${upgrade ? 'Upgrade' : 'close'}\r\n${upgrade ? 'Upgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n' : ''}\r\n`));
  });
}

test('an unsafe named zone fails before any data directory is written', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'soty-zone-config-'));
  try {
    assert.throws(() => createHttpApp(dir, { dataDir: dir, connectOrigins: ['https://shell.pochinit.online'],
      appOriginTemplate: '', namedAppZone: 'https://apps.pochinit.online' }), /apps_zone_separate_site_required/u);
    assert.deepEqual(await readdir(dir), []);
  } finally { await rm(dir, { recursive: true, force: true }); }
});

test('real HTTP rejects duplicate Host and app-zone misses cannot reach the shell or Connect APIs', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'soty-host-ingress-'));
  await writeFile(join(dir, 'index.html'), '<html><body>TRUSTED_SHELL_SENTINEL</body></html>');
  let trafficRequests = 0;
  const app = createHttpApp(dir, { dataDir: join(dir, 'data'), connectOrigins: ['http://localhost:8080'],
    appOriginTemplate: 'http://{appId}.legacy.localhost:8080', namedAppZone: 'http://named.localhost:8080',
    trafficTunnel: { handleRequest() { trafficRequests++; return false; } }, gonka: { apiKey: '', baseUrl: 'http://127.0.0.1:1' } });
  const server = createServer(app);
  try {
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    const port = server.address().port;
    const duplicate = await rawRequest(port, 'Host: localhost:8080\r\nhOSt: missing.named.localhost:8080');
    assert.match(duplicate, /^HTTP\/1\.1 400 /u);
    assert.equal(trafficRequests, 0);
    for (const host of ['missing.named.localhost:8080', 'nested.missing.named.localhost:8080', 'missing.legacy.localhost:8080',
      'named.localhost:8080', 'missing.named.localhost:8081', 'missing%2enamed.localhost:8080']) {
      for (const target of ['/', '/api/connect/capabilities', '/api/apps/capabilities', '/api/apps/tls-allow?domain=missing.named.localhost']) {
        const response = await rawRequest(port, `Host: ${host}`, target);
        assert.match(response, /^HTTP\/1\.1 (?:400|403|404|421|503) /u, `${host} ${target}`);
        assert.doesNotMatch(response, /TRUSTED_SHELL_SENTINEL|"projectId"|"agentConfigured"/u);
      }
    }
    assert.equal(trafficRequests, 0, 'app namespace must be consumed before the legacy traffic router');
    const shell = await rawRequest(port, 'Host: localhost:8080', '/');
    assert.match(shell, /TRUSTED_SHELL_SENTINEL/u);
    assert.equal(trafficRequests, 1);
  } finally {
    await app.locals.closeServices();
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
    await rm(dir, { recursive: true, force: true });
  }
});

test('the real process consumes unknown app upgrades before the connector channel and rejects duplicate Host', { timeout: 20_000 }, async () => {
  const dir = await mkdtemp(join(tmpdir(), 'soty-app-upgrade-'));
  const environment = Object.fromEntries(['PATH', 'Path', 'SystemRoot', 'WINDIR', 'ComSpec', 'TEMP', 'TMP', 'HOME', 'USERPROFILE']
    .filter(key => process.env[key] !== undefined).map(key => [key, process.env[key]]));
  const child = fork(new URL('../index.js', import.meta.url), [], { execArgv: [], silent: true, env: { ...environment,
    HOST: '127.0.0.1', PORT: '0', DATA_DIR: dir, SOTY_DIST_DIR: dir, SOTY_CONNECT_ORIGINS: 'http://localhost:8080',
    SOTY_APP_ORIGIN_TEMPLATE: 'http://{appId}.legacy.localhost:8080', SOTY_NAMED_APP_ZONE: 'http://named.localhost:8080' } });
  child.stdout.resume(); child.stderr.resume();
  try {
    const port = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('test_server_not_ready')), 5_000);
      child.on('message', message => { if (message?.type === 'soty:ready') { clearTimeout(timeout); resolve(message.port); } });
      child.once('exit', () => { clearTimeout(timeout); reject(new Error('test_server_exited')); });
    });
    const duplicate = await rawRequest(port, 'Host: localhost:8080\r\nHost: unknown.named.localhost:8080', '/api/apps/channel', true);
    assert.match(duplicate, /^HTTP\/1\.1 400 /u);
    for (const host of ['unknown.named.localhost:8080', 'nested.unknown.named.localhost:8080', 'unknown.legacy.localhost:8080']) {
      for (const target of ['/api/apps/channel', '/api/traffic/ws', '/ws/room_123456789abcdef']) {
        const response = await rawRequest(port, `Host: ${host}`, target, true);
        assert.match(response, /^HTTP\/1\.1 (?:400|403|404|421|503) /u);
        assert.doesNotMatch(response, /101 Switching Protocols/u);
      }
    }
    const health = await rawRequest(port, 'Host: localhost:8080');
    assert.match(health, /^HTTP\/1\.1 200 /u);
  } finally {
    if (child.exitCode === null && child.signalCode === null) {
      const stopped = new Promise(resolve => child.once('exit', resolve));
      child.kill('SIGTERM');
      const deadline = setTimeout(() => child.kill('SIGKILL'), 3_000);
      await stopped; clearTimeout(deadline);
    }
    await rm(dir, { recursive: true, force: true });
  }
});
