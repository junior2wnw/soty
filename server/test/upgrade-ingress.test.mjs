import test from 'node:test';
import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { connect } from 'node:net';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createAppsService } from '../../modules/apps/server/index.mjs';

test('malformed upgrade target is rejected by the standalone application gateway', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-upgrade-gateway-'));
  const service = createAppsService({ dataDir: directory, shellOrigins: ['http://localhost'] });
  try {
    let response = '';
    const handled = service.handleUpgrade({ url: 'http://[', headers: {} }, { end(value) { response = value; } }, Buffer.alloc(0));
    assert.equal(handled, true);
    assert.match(response, /^HTTP\/1\.1 400 Bad Request\r\n/u);
  } finally { service.close(); await rm(directory, { recursive: true, force: true }); }
});

test('a malformed raw websocket upgrade cannot terminate the real HTTP process', { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-upgrade-ingress-'));
  const child = fork(new URL('../index.js', import.meta.url), [], { execArgv: [], silent: true,
    env: { ...process.env, HOST: '127.0.0.1', PORT: '0', DATA_DIR: directory,
      SOTY_DIST_DIR: directory, SOTY_TRAFFIC_TUNNEL_TARGET: '', SOTY_TRAFFIC_WS_TARGET: '' } });
  child.stdout.resume(); child.stderr.resume();
  try {
    const port = await new Promise((resolve, reject) => {
      child.on('message', message => { if (message?.type === 'soty:ready') resolve(message.port); });
      child.once('exit', code => reject(new Error(`server_exited_before_ready:${code}`)));
    });
    const response = await new Promise((resolve, reject) => {
      const socket = connect(port, '127.0.0.1'); let received = '';
      socket.on('error', reject); socket.setTimeout(4000, () => socket.destroy(new Error('upgrade_timeout')));
      socket.on('data', chunk => { received += chunk.toString(); });
      socket.once('end', () => resolve(received));
      socket.once('connect', () => socket.write('GET http://[ HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n'));
    });
    assert.match(response, /^HTTP\/1\.1 400 Bad Request\r\n/u);
    const health = await fetch(`http://127.0.0.1:${port}/health`);
    assert.equal(health.status, 200); assert.equal((await health.json()).ok, true);
    assert.equal(child.exitCode, null);
  } finally {
    if (child.exitCode === null) {
      child.kill();
      await new Promise(resolve => child.once('exit', resolve));
    }
    await rm(directory, { recursive: true, force: true });
  }
});

test('a malformed raw HTTP target cannot terminate the local connector inside its error handler', { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'soty-connector-ingress-'));
  const child = fork(new URL('../../scripts/soty-connector.mjs', import.meta.url), [], { execArgv: [], silent: true,
    env: { ...process.env, SOTY_CONNECTOR_DATA_DIR: directory, SOTY_CONNECTOR_COMPANION: '1', SOTY_CONNECTOR_PORT: '0',
      SOTY_CONNECTOR_MANAGED: '0', SOTY_AGENT_MANAGED: '0', SOTY_CONNECTOR_AUTO_UPDATE: '0', SOTY_AGENT_AUTO_UPDATE: '0',
      SOTY_CONNECTOR_LINK_ID: '', SOTY_AGENT_RELAY_ID: '', SOTY_CONNECTOR_SERVER_URL: 'http://127.0.0.1:1' } });
  child.stderr.resume();
  try {
    const port = await new Promise((resolve, reject) => {
      let stdout = '';
      child.stdout.on('data', chunk => { stdout += chunk; const match = stdout.match(/soty-connector:(\d+)/u); if (match) resolve(Number(match[1])); });
      child.once('exit', code => reject(new Error(`connector_exited_before_ready:${code}`)));
    });
    const response = await new Promise((resolve, reject) => {
      const socket = connect(port, '127.0.0.1'); let received = '';
      socket.on('error', reject); socket.setTimeout(4000, () => socket.destroy(new Error('connector_ingress_timeout')));
      socket.on('data', chunk => { received += chunk.toString(); });
      socket.once('end', () => resolve(received));
      socket.once('connect', () => socket.write('GET http://[ HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n'));
    });
    assert.match(response, /^HTTP\/1\.1 400 Bad Request\r\n/u);
    const health = await fetch(`http://127.0.0.1:${port}/health`);
    assert.equal(health.status, 200); assert.equal((await health.json()).ok, true);
    assert.equal(child.exitCode, null);
  } finally {
    if (child.exitCode === null) { child.kill(); await new Promise(resolve => child.once('exit', resolve)); }
    await rm(directory, { recursive: true, force: true });
  }
});
