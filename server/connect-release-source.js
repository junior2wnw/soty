import path from 'node:path';
import { constants } from 'node:fs';
import { lstat, open } from 'node:fs/promises';

const PREFIX = '/releases/connect';
const MAX_RELEASE_BYTES = 12 * 1024 * 1024;

/** Mount before the SPA fallback. The configured directory is read-only to Soty. */
export function attachConnectReleaseSource(app, { directory } = {}) {
  if (typeof directory !== 'string' || !path.isAbsolute(directory)) throw new Error('connect_release_directory_required');
  const root = path.resolve(directory);
  const handler = async (req, res, next) => {
    const target = req.originalUrl || req.url || '/';
    const rawPath = target.split('?', 1)[0];
    let decodedPath;
    try { decodedPath = decodeURIComponent(rawPath); } catch { decodedPath = rawPath; }
    let normalizedPath;
    try { normalizedPath = new URL(target, 'http://localhost').pathname; } catch { normalizedPath = rawPath; }
    const inNamespace = candidate => candidate.toLowerCase() === PREFIX || candidate.toLowerCase().startsWith(`${PREFIX}/`);
    if (!inNamespace(rawPath) && !inNamespace(decodedPath) && !inNamespace(normalizedPath)) { next(); return; }
    const error = (status, code) => {
      if (res.destroyed || res.writableEnded) return;
      res.statusCode = status;
      res.setHeader('Cache-Control', 'no-store');
      res.setHeader('Content-Type', 'application/json; charset=utf-8');
      res.setHeader('X-Content-Type-Options', 'nosniff');
      res.end(JSON.stringify({ ok: false, error: code }));
    };
    if (req.method !== 'GET') { res.setHeader('Allow', 'GET'); error(405, 'method_not_allowed'); return; }
    const match = /^\/releases\/connect\/(stable\.json|release-([1-9]\d*)\.json)$/.exec(rawPath);
    if (!match || rawPath !== target || (match[2] && !Number.isSafeInteger(Number(match[2])))) {
      error(404, 'release_not_found'); return;
    }
    let handle;
    try {
      const rootStat = await lstat(root);
      if (!rootStat.isDirectory() || rootStat.isSymbolicLink()) { error(503, 'release_source_unavailable'); return; }
      const file = path.join(root, match[1]);
      const before = await lstat(file);
      if (!before.isFile() || before.isSymbolicLink() || before.size > MAX_RELEASE_BYTES) {
        error(503, 'release_source_unavailable'); return;
      }
      handle = await open(file, constants.O_RDONLY | (constants.O_NOFOLLOW || 0));
      const after = await handle.stat();
      if (!after.isFile() || after.dev !== before.dev || after.ino !== before.ino || after.size > MAX_RELEASE_BYTES) {
        error(503, 'release_source_unavailable'); return;
      }
      const bytes = await handle.readFile();
      if (bytes.length > MAX_RELEASE_BYTES) { error(503, 'release_source_unavailable'); return; }
      res.statusCode = 200;
      res.setHeader('Content-Type', 'application/json; charset=utf-8');
      res.setHeader('X-Content-Type-Options', 'nosniff');
      res.setHeader('Cache-Control', match[1] === 'stable.json' ? 'no-store' : 'public, max-age=31536000, immutable');
      res.setHeader('Content-Length', bytes.length);
      res.end(bytes);
    } catch (cause) {
      error(cause.code === 'ENOENT' ? 404 : 503, cause.code === 'ENOENT' ? 'release_not_found' : 'release_source_unavailable');
    } finally { if (handle) await handle.close().catch(() => {}); }
  };
  // Do not use app.get: Express implicitly accepts HEAD for GET routes. This
  // middleware also terminates unknown paths in the namespace before the SPA.
  app.use((req, res, next) => { handler(req, res, next).catch(() => {
    if (!res.destroyed && !res.writableEnded) {
      res.statusCode = 503; res.setHeader('Cache-Control', 'no-store'); res.end();
    }
  }); });
  return handler;
}
