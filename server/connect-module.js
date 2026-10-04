import path from 'node:path';
import { readFileSync } from 'node:fs';
import { createConnectService } from '../modules/connect/server/index.mjs';
import { createConnectHandler } from '../modules/connect/server/http.mjs';
const moduleVersion = JSON.parse(readFileSync(new URL('../modules/connect/package.json', import.meta.url), 'utf8')).version;

export function attachConnectModule(app, { dataDir, origins, extensions = [], canRequestContact } = {}) {
  const allowedOrigins = connectAllowedOrigins(origins);
  const service = createConnectService({
    databasePath: path.join(dataDir || path.resolve('data'), 'connect', 'accounts.sqlite'),
    projectId: 'soty', allowedOrigins, extensions, canRequestContact
  });
  app.use(createConnectHandler(service));
  app.get('/api/connect/capabilities', (_req, res) => {
    res.setHeader('Cache-Control', 'no-store');
    res.json({ module: '@soty/connect', version: moduleVersion, protocol: 1, projectId: 'soty',
      accountScope: 'project', sharedSso: false, encryptedWorkspace: true,
      legacyRoomRevocation: false, updates: 'signed-host-release' });
  });
  return service;
}

export function connectAllowedOrigins(origins) {
  const port = Number(process.env.PORT || 8080);
  if (origins) return origins;
  const configured = process.env.SOTY_CONNECT_ORIGINS
    ? process.env.SOTY_CONNECT_ORIGINS.split(',').map(value => new URL(value.trim()).origin)
    : ['https://xn--n1afe0b.online', 'https://soty.pochinit.online', `http://127.0.0.1:${port}`, `http://localhost:${port}`];
  // The original origin still serves /__soty so its device vault remains usable.
  // Only the recognised production deployment gains the new primary origin.
  if (configured.includes('https://xn--n1afe0b.online') || configured.includes('https://soty.pochinit.online')) {
    return [...new Set([...configured, 'https://4-2.xn--p1ai'])];
  }
  return configured;
}
