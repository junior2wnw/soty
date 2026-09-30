import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { connect } from 'node:net';
import { fork } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createAppsService } from '../../modules/apps/server/index.mjs';
import { ensureCanonicalDomain } from '../../modules/apps/server/schema.mjs';
import { parseAppAuthority } from '../../modules/apps/server/hosts.mjs';

const actor = { accountId: 'host_acceptance_owner', deviceId: 'host_acceptance_device' };
const appId = `app-${'4'.repeat(32)}`;
const revokedId = `app-${'5'.repeat(32)}`;
const legacy = 'https://{appId}.legacy.other.online';
const named = 'https://named.soty.online';
const trustedHost = 'shell.pochinit.online';

async function seed(directory) {
  const configuration = { dataDir: directory, appOriginTemplate: legacy, namedAppZone: named,
    shellOrigins: [`https://${trustedHost}`], actorActive: value => value?.accountId === actor.accountId && value?.deviceId === actor.deviceId };
  createAppsService(configuration).close();
  const db = new DatabaseSync(join(directory, 'apps', 'registry.sqlite'));
  try {
    db.exec('PRAGMA foreign_keys=ON; BEGIN IMMEDIATE');
    db.prepare('INSERT INTO app_devices VALUES (?,?,?,?,?)').run('host_acceptance_connector', actor.accountId,
      JSON.stringify({ linkId: 'host_acceptance_link', hostDeviceId: 'host_acceptance_host', connectorId: 'host_acceptance_connector' }),
      'PRIVATE_DEVICE_SENTINEL', 1);
    for (const [index, id] of [appId, revokedId].entries()) {
      db.prepare('INSERT INTO local_apps VALUES (?,?,?,?,?,?,?,?,?,?,?)').run(id, actor.accountId, 'host_acceptance_connector',
        'PRIVATE_APP_SENTINEL', 9400 + index, '/', JSON.stringify({ accountIds: [], communityIds: [] }), 'enabled', 1, 1, 1);
      ensureCanonicalDomain(db, { id, owner_account_id: actor.accountId, created_at: 1 }, legacy);
    }
    db.exec('COMMIT');
  } finally { db.close(); }
  const service = createAppsService(configuration);
  try {
    const execute = (op, args) => service.execute({ actor, op, args });
    execute('apps.domains.claim', { appId, slug: 'friendly-name', requestId: 'friendly', expectedDomainsRevision: 0 });
    const retired = execute('apps.domains.claim', { appId, slug: 'retired-name', requestId: 'retired', expectedDomainsRevision: 1 });
    execute('apps.domains.retire', { appId, domainId: retired.receipt.domainId, requestId: 'retire', expectedDomainsRevision: 2 });
    execute('apps.revoke', { appId: revokedId });
  } finally { service.close(); }
}

function requestRaw(port, host, path = '/', { upgrade = false, extraHeaders = [] } = {}) {
  return new Promise((resolveResponse, reject) => {
    const socket = connect(port, '127.0.0.1');
    const parts = []; let length = 0, settled = false;
    const finish = error => {
      if (settled) return; settled = true;
      socket.destroy();
      if (error) { reject(error); return; }
      const raw = Buffer.concat(parts).toString('utf8');
      const split = raw.indexOf('\r\n\r\n');
      const head = split < 0 ? raw : raw.slice(0, split);
      const lines = head.split('\r\n');
      const headers = Object.fromEntries(lines.slice(1).filter(line => line.includes(':')).map(line => {
        const colon = line.indexOf(':'); return [line.slice(0, colon).toLowerCase(), line.slice(colon + 1).trim()];
      }));
      resolveResponse({ status: Number(lines[0].match(/^HTTP\/1\.1 (\d{3}) /u)?.[1]), headers, body: split < 0 ? '' : raw.slice(split + 4), raw });
    };
    socket.setTimeout(3_000, () => finish(new Error('host_acceptance_timeout')));
    socket.on('error', error => finish(error));
    socket.on('data', data => {
      parts.push(data); length += data.length;
      if (length > 64 * 1024) { finish(new Error('host_acceptance_response_too_large')); return; }
      const raw = Buffer.concat(parts).toString('utf8');
      // If a regression upgrades a forbidden endpoint, end the test request now
      // instead of waiting for an application/connector handshake timeout.
      if (/^HTTP\/1\.1 101 /u.test(raw) && raw.includes('\r\n\r\n')) finish();
    });
    socket.once('end', () => finish());
    socket.once('close', () => finish());
    socket.once('connect', () => socket.write([
      `GET ${path} HTTP/1.1`, `Host: ${host}`, `Connection: ${upgrade ? 'Upgrade' : 'close'}`,
      ...(upgrade ? ['Upgrade: websocket', 'Sec-WebSocket-Version: 13', 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ=='] : ['Accept: text/html']),
      ...extraHeaders, '', '',
    ].join('\r\n')));
  });
}

async function stopChild(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  let forced = false;
  const exited = new Promise(resolveExit => child.once('exit', resolveExit));
  child.kill('SIGTERM');
  const timeout = setTimeout(() => { forced = true; child.kill('SIGKILL'); }, 3_000);
  try { await exited; assert.equal(forced, false, 'the isolated application process must stop without forced termination'); }
  finally { clearTimeout(timeout); }
}

test('authority parsing distinguishes canonical IDNA wire names from malformed DNS and bracketed IP forms', () => {
  assert.deepEqual(parseAppAuthority('APP.XN--N1AFE0B.ONLINE:443'), { hostname: 'app.xn--n1afe0b.online', port: '443' });
  assert.deepEqual(parseAppAuthority('[2001:DB8::1]:65535'), { hostname: '[2001:db8::1]', port: '65535' });
  assert.deepEqual(parseAppAuthority('[::ffff:127.0.0.1]:80'), { hostname: '[::ffff:127.0.0.1]', port: '80' });
  const maximumHost = [63, 63, 63, 61].map(length => 'a'.repeat(length)).join('.');
  assert.equal(maximumHost.length, 253);
  assert.equal(parseAppAuthority(`${maximumHost}:65535`)?.hostname, maximumHost);
  for (const authority of ['app.соты.online', 'app\u3002named.soty.online', 'app.named.soty.online:000443',
    'app.named.soty.online:+443', 'app.named.soty.online:443.', 'app.named.soty.online:65536',
    'user:pass@app.named.soty.online', 'app.named.soty.online,localhost', 'app.named.soty.online\\@localhost',
    '[::1%25lo0]', '[::1]:', '[::1]:00080', '[::ffff:999.1.1.1]', '2001:db8::1', '[::1]/path',
    `a.${maximumHost}`, `${'a'.repeat(64)}.named.soty.online`]) {
    assert.equal(parseAppAuthority(authority), null, authority);
  }
});

test('the actual server keeps retained aliases and malformed authorities ahead of HTTP, WS, traffic and control routing', { timeout: 30_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-host-independent-'));
  const upstreamSockets = new Set();
  let httpUpstream = 0, wsUpstream = 0;
  const upstream = createServer((_request, response) => { httpUpstream++; response.writeHead(418); response.end('UPSTREAM_SENTINEL'); });
  upstream.on('connection', socket => { upstreamSockets.add(socket); socket.once('close', () => upstreamSockets.delete(socket)); });
  upstream.on('upgrade', (_request, socket) => { wsUpstream++; socket.end('HTTP/1.1 418 Test Upstream\r\nConnection: close\r\nContent-Length: 0\r\n\r\n'); });
  let child;
  try {
    await seed(directory);
    await writeFile(join(directory, 'index.html'), '<!doctype html><title>TRUSTED_SHELL_SENTINEL</title>');
    await new Promise(resolveListen => upstream.listen(0, '127.0.0.1', resolveListen));
    const upstreamPort = upstream.address().port;
    const env = Object.fromEntries(['SystemRoot', 'WINDIR', 'TEMP', 'TMP']
      .filter(key => process.env[key] !== undefined).map(key => [key, process.env[key]]));
    child = fork(new URL('../index.js', import.meta.url), [], { execArgv: [], silent: true, env: {
      ...env, HOST: '127.0.0.1', PORT: '0', DATA_DIR: directory, SOTY_DIST_DIR: directory,
      SOTY_CONNECT_ORIGINS: `https://${trustedHost}`, SOTY_APP_ORIGIN_TEMPLATE: legacy,
      SOTY_NAMED_APP_ZONE: '', SOTY_TRUST_PROXY: 'loopback',
      SOTY_TRAFFIC_TUNNEL_TARGET: `http://127.0.0.1:${upstreamPort}/upstream`,
      SOTY_TRAFFIC_WS_TARGET: `ws://127.0.0.1:${upstreamPort}/upstream-ws`,
    } });
    child.stdout.resume(); child.stderr.resume();
    const port = await new Promise((resolveReady, reject) => {
      const timeout = setTimeout(() => reject(new Error('host_acceptance_server_not_ready')), 6_000);
      child.on('message', value => { if (value?.type === 'soty:ready') { clearTimeout(timeout); resolveReady(value.port); } });
      child.once('exit', () => { clearTimeout(timeout); reject(new Error('host_acceptance_server_exited')); });
    });
    const cases = [
      ['friendly-name.named.soty.online', 503], ['FRIENDLY-NAME.NAMED.SOTY.ONLINE:443', 503],
      ['retired-name.named.soty.online', 410], ['retired-name.named.soty.online:443', 410],
      ['friendly-name.named.soty.online:80', 404], ['retired-name.named.soty.online:8443', 404],
      ['nested.friendly-name.named.soty.online', 404], ['named.soty.online', 404],
      ['missing.named.soty.online', 404], ['missing.legacy.other.online', 404],
      [`${appId}.legacy.other.online:8443`, 404],
      ['friendly-name%2enamed.soty.online', 400], ['friendly-name.named.soty.online.', 400],
      ['friendly-name.named.soty.online:0443', 400], ['friendly-name.named.soty.online:0', 400],
      ['user@friendly-name.named.soty.online', 400], ['friendly-name.named.soty.online\\evil', 400],
      ['аpp.named.soty.online', 400], ['[::1]garbage', 400],
    ];
    const extraHeaders = [`X-Forwarded-Host: ${trustedHost}`, `Forwarded: host=${trustedHost};proto=https`,
      'Cookie: __Host-soty_app_session=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'];
    for (const [host, expected] of cases) {
      for (const path of ['/', '/api/connect/capabilities', '/api/apps/tls-allow?domain=friendly-name.named.soty.online', '/api/traffic/tunnel']) {
        const response = await requestRaw(port, host, path, { extraHeaders });
        assert.equal(response.status, expected, `${host} ${path}`);
        assert.equal(response.headers['set-cookie'], undefined);
        assert.equal(response.headers.location, undefined);
        assert.equal(response.headers['cache-control'], 'no-store');
        assert.doesNotMatch(response.body, /TRUSTED_SHELL_SENTINEL|UPSTREAM_SENTINEL|PRIVATE_APP_SENTINEL|PRIVATE_DEVICE_SENTINEL|host_acceptance_owner|host_acceptance_connector|"projectId"/u);
      }
      for (const path of ['/api/apps/channel', '/api/traffic/ws', '/ws/room_123456789abcdef']) {
        const response = await requestRaw(port, host, path, { upgrade: true, extraHeaders });
        assert.equal(response.status, expected, `upgrade ${host} ${path}`);
        assert.notEqual(response.status, 101);
      }
    }
    const absolute = await requestRaw(port, 'friendly-name.named.soty.online', `https://${trustedHost}/api/apps/channel`, { upgrade: true });
    assert.equal(absolute.status, 503, 'absolute-form request targets do not replace the Host classifier');
    assert.equal(httpUpstream, 0); assert.equal(wsUpstream, 0);

    const canonicalHost = `${appId}.legacy.other.online`;
    for (const path of ['/', '/api/connect/capabilities', '/api/traffic/tunnel']) {
      const response = await requestRaw(port, canonicalHost, path);
      assert.equal(response.status, 401);
      assert.doesNotMatch(response.body, /TRUSTED_SHELL_SENTINEL|UPSTREAM_SENTINEL|PRIVATE_APP_SENTINEL/u);
    }
    assert.equal((await requestRaw(port, canonicalHost, '/api/apps/channel', { upgrade: true })).status, 403);
    assert.equal(httpUpstream, 0); assert.equal(wsUpstream, 0);

    for (const domain of ['friendly-name.named.soty.online', 'retired-name.named.soty.online', `${revokedId}.legacy.other.online`]) {
      assert.equal((await requestRaw(port, trustedHost, `/api/apps/tls-allow?domain=${domain}`)).status, 204, domain);
    }
    for (const domain of ['missing.named.soty.online', 'named.soty.online', 'FRIENDLY-NAME.NAMED.SOTY.ONLINE', 'friendly-name.named.soty.online:443']) {
      assert.equal((await requestRaw(port, trustedHost, `/api/apps/tls-allow?domain=${encodeURIComponent(domain)}`)).status, 403, domain);
    }

    const shell = await requestRaw(port, trustedHost, '/', { extraHeaders: ['X-Forwarded-Host: friendly-name.named.soty.online'] });
    assert.match(shell.body, /TRUSTED_SHELL_SENTINEL/u);
    assert.equal((await requestRaw(port, trustedHost, '/api/traffic/tunnel')).status, 418);
    assert.equal((await requestRaw(port, trustedHost, '/api/traffic/ws', { upgrade: true })).status, 418);
    assert.equal(httpUpstream, 1); assert.equal(wsUpstream, 1, 'the WS proxy was enabled, so prior zero ingress is meaningful');
    assert.equal(child.exitCode, null);
  } finally {
    if (child) await stopChild(child);
    for (const socket of upstreamSockets) socket.destroy();
    await new Promise(resolveClose => upstream.close(resolveClose));
    assert.equal(dirname(resolve(directory)), resolve(tmpdir()));
    assert.ok(basename(directory).startsWith('soty-host-independent-'));
    await rm(directory, { recursive: true, force: true });
  }
});
