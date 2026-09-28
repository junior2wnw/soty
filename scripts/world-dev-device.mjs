import { spawn } from 'node:child_process';
import { mkdir, writeFile, access } from 'node:fs/promises';
import { randomBytes } from 'node:crypto';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createSampleApp } from '../modules/apps/examples/sample-app.mjs';

// Explicit, loopback-only development device. Never reads the installed connector's
// configuration and never launches an LLM task. The sample app is a separate project.
const root = dirname(dirname(fileURLToPath(import.meta.url)));
const data = join(root, 'output', 'world-validation-20260928', 'test-device');
const workspace = join(data, 'workspace');
await mkdir(workspace, { recursive: true });
const config = join(data, 'connector-config.json');
try { await access(config); }
catch { await writeFile(config, JSON.stringify({ workspaceRoot: workspace, allowedRoots: [workspace], linkId: randomBytes(32).toString('base64url') }), { mode: 0o600 }); }
const sample = await createSampleApp({ port: 5308 });
const allowedEnvironment = Object.fromEntries(Object.entries(process.env).filter(([name]) => ['PATH', 'PATHEXT', 'SYSTEMROOT', 'WINDIR', 'COMSPEC', 'TEMP', 'TMP', 'USERPROFILE', 'HOMEDRIVE', 'HOMEPATH', 'LOCALAPPDATA', 'APPDATA'].includes(name.toUpperCase())));
const connector = spawn(process.execPath, [join(root, 'scripts', 'soty-connector.mjs')], {
  cwd: root, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'], env: { ...allowedEnvironment,
    SOTY_CONNECTOR_DATA_DIR: data, SOTY_CONNECTOR_PORT: '49429', SOTY_CONNECTOR_SERVER_URL: 'http://localhost:5200',
    SOTY_CONNECTOR_DEVICE_ID: 'world-validation-laptop', SOTY_CONNECTOR_DEVICE_NICK: 'Тестовый ноутбук',
    SOTY_CONNECTOR_SCOPE: 'Dev', SOTY_CONNECTOR_AUTO_UPDATE: '0', SOTY_CONNECTOR_MANAGED: '0',
  },
});
connector.stdout.on('data', chunk => { if (String(chunk).includes('soty-connector:49429')) console.log('Test connector listening on 49429'); });
connector.stderr.on('data', () => {}); // Internal diagnostics may contain local paths; acceptance tests inspect failures separately.
console.log('Sample application: http://127.0.0.1:5308; isolated test device, no inference calls');
let stopping = false;
async function stop() { if (stopping) return; stopping = true; connector.kill(); await sample.close(); }
process.once('SIGINT', () => void stop()); process.once('SIGTERM', () => void stop());
connector.once('exit', () => { void stop(); });
