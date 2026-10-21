import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createServer as createTcpServer } from 'node:net';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFile, spawn } from 'node:child_process';
import { promisify } from 'node:util';
import { buildTrafficBridgeConfig } from './agent-modules/traffic-core.mjs';

const exec = promisify(execFile), wait = ms => new Promise(resolve => setTimeout(resolve, ms));
const core = process.env.SOTY_XRAY_BINARY;
test('the pinned core blocks literal and DNS loopback; removing each guard restores the canary', { skip: !core }, async () => {
  const dir = await mkdtemp(join(tmpdir(), 'soty-traffic-private-test-'));
  const canary = createServer((_req, res) => res.end('private-canary'));
  await new Promise(resolve => canary.listen(0, '127.0.0.1', resolve));
  const targetPort = canary.address().port;
  try {
    for (const mode of ['guarded', 'routing-only', 'final-only', 'negative-control']) {
      const reservation = createTcpServer(); await new Promise(resolve => reservation.listen(0, '127.0.0.1', resolve));
      const port = reservation.address().port; await new Promise(resolve => reservation.close(resolve));
      const config = buildTrafficBridgeConfig({ gatewayHost: 'gateway.example', gatewayPath: '/tunnel',
        bridgeId: '11111111-1111-4111-8111-111111111111', reverseTag: 'reverse-test' });
      config.inbounds = [{ tag: 'reverse-test', listen: '127.0.0.1', port, protocol: 'socks', settings: { auth: 'noauth', udp: false } }];
      config.outbounds = config.outbounds.filter(row => row.tag !== 'soty-reverse-link');
      if (mode === 'final-only' || mode === 'negative-control') config.routing.rules.shift();
      if (mode === 'routing-only' || mode === 'negative-control') config.outbounds[1].settings.finalRules.shift();
      const file = join(dir, `${mode}.json`); await writeFile(file, JSON.stringify(config));
      await exec(core, ['run', '-test', '-c', file], { windowsHide: true, timeout: 10000 });
      const child = spawn(core, ['run', '-c', file], { windowsHide: true, stdio: 'ignore' });
      const closed = new Promise(resolve => child.once('exit', resolve));
      try {
        await wait(700);
        for (const host of ['127.0.0.1', 'localhost']) {
          let output = null;
          try { output = await exec('curl.exe', ['--silent', '--show-error', '--fail', '--max-time', '3', '--proxy', `socks5h://127.0.0.1:${port}`, `http://${host}:${targetPort}/`], { windowsHide: true, timeout: 5000 }); } catch {}
          assert.equal(output?.stdout === 'private-canary', mode === 'negative-control', `${mode} ${host}`);
        }
      } finally { child.kill(); await Promise.race([closed, wait(2000)]); if (child.exitCode === null && !child.signalCode) child.kill('SIGKILL'); }
    }
  } finally { await new Promise(resolve => canary.close(resolve)); await rm(dir, { recursive: true, force: true }); }
});
