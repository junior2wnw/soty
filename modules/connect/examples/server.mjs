import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createConnectService } from '../server/index.mjs';
import { createConnectHandler } from '../server/http.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const port = Number(process.env.PORT || 8099);
const origin = `http://127.0.0.1:${port}`;
// Product data deliberately lives outside the copied module.
const databasePath = path.resolve(process.env.CONNECT_DATA_DIR || 'connect-example-data', 'accounts.sqlite');
const service = createConnectService({ databasePath, projectId: 'example', allowedOrigins: [origin] });
const rpc = createConnectHandler(service);
const server = createServer(async (req, res) => {
  let url, relative;
  try {
    url = new URL(req.url || '/', origin);
    relative = url.pathname === '/' ? 'examples/index.html' : decodeURIComponent(url.pathname).slice(1);
  } catch { res.writeHead(400).end(); return; }
  if (url.pathname === '/api/connect/rpc') { await rpc(req, res); return; }
  if (!/^(browser|ui|examples)\/[a-zA-Z0-9_.\/-]+$/.test(relative) || relative.split('/').includes('..')) { res.writeHead(404).end(); return; }
  try {
    const type = relative.endsWith('.html') ? 'text/html' : relative.endsWith('.css') ? 'text/css' : 'text/javascript';
    res.setHeader('Content-Type', `${type}; charset=utf-8`); res.setHeader('Cache-Control', 'no-store');
    res.setHeader('Referrer-Policy', 'no-referrer'); res.setHeader('X-Content-Type-Options', 'nosniff');
    res.end(await readFile(path.join(root, relative)));
  } catch { res.writeHead(404).end(); }
});
server.listen(port, '127.0.0.1', () => console.log(`Connect example: ${origin}`));
for (const event of ['SIGINT', 'SIGTERM']) process.on(event, () => server.close(() => { service.close(); process.exit(0); }));
