/** Framework-neutral Node HTTP adapter. Attach before a product's SPA fallback. */
export function createConnectHandler(service, { path = '/api/connect/rpc', maxBytes = 2 * 1024 * 1024 + 32 * 1024 } = {}) {
  return async function connectHttp(req, res, next) {
    const reply = (status, value) => {
      if (res.destroyed || res.writableEnded) return;
      res.setHeader('Cache-Control', 'no-store');
      res.setHeader('Content-Type', 'application/json; charset=utf-8');
      res.setHeader('X-Content-Type-Options', 'nosniff');
      res.statusCode = status; res.end(JSON.stringify(value));
    };
    const error = code => ({ ok: false, error: { code, message: code } });
    let pathname;
    try { pathname = new URL(req.url || '/', 'http://localhost').pathname; }
    catch { reply(400, error('invalid_url')); return true; }
    if (pathname !== path) { if (next) next(); return false; }
    if (req.method !== 'POST') { reply(405, error('method_not_allowed')); return true; }
    if (String(req.headers['content-type'] || '').split(';', 1)[0].trim().toLowerCase() !== 'application/json') { reply(415, error('json_required')); return true; }
    const origin = req.headers.origin;
    if (typeof origin !== 'string' || origin === 'null') { reply(403, error('origin_required')); return true; }
    try {
      const tooLarge = () => {
        res.setHeader('Connection', 'close');
        reply(413, error('request_too_large'));
        req.resume();
      };
      if (Number(req.headers['content-length']) > maxBytes) { tooLarge(); return true; }
      const chunks = []; let length = 0;
      for await (const chunk of req.iterator({ destroyOnReturn: false })) {
        length += chunk.length;
        if (length > maxBytes) { tooLarge(); return true; }
        chunks.push(chunk);
      }
      const body = JSON.parse(Buffer.concat(chunks).toString('utf8'));
      if (!body || typeof body !== 'object' || Array.isArray(body) || body.protocol !== 1) {
        reply(400, error('protocol_unsupported')); return true;
      }
      const result = await service.handle({ op: body.op, args: body.args || {}, proof: body.proof, origin });
      reply(result.ok ? 200 : 400, result);
    } catch { reply(400, error('request_failed')); }
    return true;
  };
}
