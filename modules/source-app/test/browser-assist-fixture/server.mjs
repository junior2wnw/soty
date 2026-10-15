// Local fixture only. No production imports, identity, model, config, uploads or directory listing.
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
const files = new Map([
  ['/', new URL('./root.html', import.meta.url)], ['/app', new URL('./app.html', import.meta.url)],
  ['/app-enabled', new URL('./app-enabled.html', import.meta.url)],
  ['/host.mjs', new URL('../../../../src/world/browser-assist-pilot.mjs', import.meta.url)],
  ['/sdk.mjs', new URL('../../browser/assist-sdk.mjs', import.meta.url)],
  ['/assist-wire.mjs', new URL('../../browser/assist-wire.mjs', import.meta.url)],
  ['/modules/source-app/browser/assist-wire.mjs', new URL('../../browser/assist-wire.mjs', import.meta.url)],
]);
const servers = [5198, 5199].map(port => createServer(async (req, res) => {
  const file = files.get(new URL(req.url, 'http://127.0.0.1').pathname);
  if (req.method !== 'GET' || !file) { res.writeHead(404); res.end(); return; }
  res.setHeader('Content-Type', file.pathname.endsWith('.mjs') ? 'text/javascript' : 'text/html; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store');res.end(await readFile(file));
}).listen(port, '127.0.0.1'));
for (const server of servers) server.on('error', () => { for (const s of servers) s.close(); process.exitCode = 1; });
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => { for (const server of servers) server.close(); });
console.log('Local fixture: http://127.0.0.1:5198 (manual opt-in, no model)');
