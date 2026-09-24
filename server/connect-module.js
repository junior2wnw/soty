import path from 'node:path';
import { readFileSync } from 'node:fs';
import { createConnectService } from '../modules/connect/server/index.mjs';
import { createConnectHandler } from '../modules/connect/server/http.mjs';
const moduleVersion = JSON.parse(readFileSync(new URL('../modules/connect/package.json', import.meta.url), 'utf8')).version;

export function attachConnectModule(app, { dataDir, origins } = {}) {
  const port = Number(process.env.PORT || 8080);
  const allowedOrigins = origins || (process.env.SOTY_CONNECT_ORIGINS
    ? process.env.SOTY_CONNECT_ORIGINS.split(',').map(value => new URL(value.trim()).origin)
    : ['https://xn--n1afe0b.online', 'https://soty.pochinit.online', `http://127.0.0.1:${port}`, `http://localhost:${port}`]);
  const service = createConnectService({
    databasePath: path.join(dataDir || path.resolve('data'), 'connect', 'accounts.sqlite'),
    projectId: 'soty', allowedOrigins
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
