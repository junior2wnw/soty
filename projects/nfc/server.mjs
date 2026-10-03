import { createServer } from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { resolve, extname, relative, isAbsolute } from 'node:path';

const root = fileURLToPath(new URL('./dist/', import.meta.url));
const types = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.mjs': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.png': 'image/png', '.json': 'application/json; charset=utf-8' };
export function createNfcServer() {
  return createServer(async (req, res) => {
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Referrer-Policy', 'no-referrer');
    if (!['GET', 'HEAD'].includes(req.method)) { res.writeHead(405, { Allow: 'GET, HEAD' }); res.end(); return; }
    try {
      const url = new URL(req.url, 'http://localhost');
      if (url.pathname === '/health') { res.writeHead(200, { 'Content-Type': types['.json'] }); res.end(req.method === 'HEAD' ? '' : JSON.stringify({ ok: true, app: 'soty-nfc', version: '1.0.0' })); return; }
      const pathname = decodeURIComponent(url.pathname);
      if (pathname.includes('\\') || pathname.includes('\0')) throw new Error('invalid_path');
      const file = resolve(root, '.' + (pathname === '/' ? '/index.html' : pathname));
      const part = relative(root, file);
      if (part.startsWith('..') || isAbsolute(part)) throw new Error('invalid_path');
      if (!(await stat(file)).isFile()) throw new Error('not_found');
      const bytes = await readFile(file);
      res.writeHead(200, { 'Content-Type': types[extname(file)] || 'application/octet-stream', 'Content-Length': bytes.length,
        'Cache-Control': pathname.startsWith('/assets/') ? 'public, max-age=31536000, immutable' : 'no-cache' });
      res.end(req.method === 'HEAD' ? undefined : bytes);
    } catch { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('Страница не найдена'); }
  });
}
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const port = Number(process.env.PORT || 5318), host = process.env.HOST || '127.0.0.1';
  if (!Number.isSafeInteger(port) || port < 1024 || port > 65535) throw new Error('invalid_port');
  const server = createNfcServer();
  server.listen(port, host, () => console.log('NFC application listening on port ' + port));
  for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, () => server.close(() => process.exit(0)));
}
