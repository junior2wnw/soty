#!/usr/bin/env node
import { writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { parseDeviceLink } from '../src/platform/device-link.mjs';
const args = {};
for (let i = 2; i < process.argv.length; i += 2) {
  const key = process.argv[i]; if (!['--origin', '--connector', '--output'].includes(key) || args[key] || !process.argv[i + 1]) throw Error('invalid_arguments');
  args[key] = process.argv[i + 1];
}
try {
  const origin = new URL(args['--origin']), connector = new URL(args['--connector']);
  if (origin.protocol !== 'https:' || origin.href !== origin.origin + '/' || origin.username || origin.password
    || connector.protocol !== 'http:' || connector.hostname !== '127.0.0.1' || connector.href !== connector.origin + '/' || connector.username || connector.password || !args['--output']) throw Error('invalid_arguments');
  const response = await fetch(new URL('/apps/claim', connector), { method: 'POST', headers: { Origin: origin.origin, 'Content-Type': 'application/json' }, body: '{}', signal: AbortSignal.timeout(8000) });
  const value = await response.json();
  if (!response.ok || value.ok !== true) throw Error('connector_unavailable');
  const link = { schema: 'soty.device-link.v1', origin: origin.origin, hostDeviceId: value.hostDeviceId, connectorId: value.connectorId, claimCode: value.claimCode };
  parseDeviceLink(JSON.stringify(link), origin.origin);
  const output = resolve(args['--output']); await writeFile(output, JSON.stringify(link), { mode: 0o600, flag: 'wx' });
  process.stdout.write(JSON.stringify({ ok: true, file: output, expiresInSeconds: 300 }) + '\n');
} catch {
  process.stdout.write(JSON.stringify({ ok: false, code: 'device_link_export_failed' }) + '\n'); process.exitCode = 1;
}
