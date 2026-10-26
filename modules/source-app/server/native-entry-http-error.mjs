// Public, finite failure UI only. Never uses the requested URL or app identity.
const page = `<!doctype html>
<html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Не удалось открыть приложение</title>
<style>html{color-scheme:light dark}body{margin:0;min-height:100vh;display:grid;place-items:center;background:#f3f5f7;color:#17212b;font:16px/1.55 system-ui,sans-serif}main{box-sizing:border-box;width:min(100%,480px);padding:28px}h1{font-size:clamp(24px,5vw,32px);line-height:1.2;margin:0 0 16px}p{margin:0 0 24px}a{display:inline-block;padding:12px 18px;border-radius:12px;background:#145da0;color:#fff;text-decoration:none;font-weight:600}a:focus-visible{outline:3px solid currentColor;outline-offset:4px}@media(prefers-color-scheme:dark){body{background:#161b22;color:#f0f4f8}}</style>
<main><h1>Не удалось открыть приложение</h1><p>Сейчас не получилось проверить доступ. Начните открытие заново. Если до этого выполняли действие, сначала проверьте его результат.</p><a href="/" rel="noreferrer">Открыть приложение заново</a></main></html>`;
const deniedPage = page.replace('Сейчас не получилось проверить доступ. Начните открытие заново. Если до этого выполняли действие, сначала проверьте его результат.', 'Этот вход недоступен. Адрес приложения или доступ могли измениться. Проверьте актуальный вход в Сотах.');
const vary = 'Accept, Sec-Fetch-Mode, Sec-Fetch-Dest';
function documentNavigation(request) {
  if (request.method !== 'GET') return false;
  const header = name => { const value = request.headers?.[name]; return (Array.isArray(value) ? value.join(',') : value ?? '').toLowerCase(); };
  const mode = header('sec-fetch-mode'), destination = header('sec-fetch-dest');
  if (mode && mode !== 'navigate' || destination && !['document','iframe','frame'].includes(destination)) return false;
  const accept = header('accept'); if (accept.length > 4096) return false;
  let html = false;
  for (const item of accept.split(',')) {
    const [kind, ...parameters] = item.trim().split(';'); let quality = 1;
    for (const parameter of parameters) { const [key, value] = parameter.trim().split('='); if (key === 'q') quality = value?.trim() ? Number(value) : 0; }
    if (!(quality > 0 && quality <= 1)) continue;
    if (kind === 'application/json') return false;
    html ||= ['text/html','application/xhtml+xml'].includes(kind);
  }
  return html;
}

/** Opt-in HTTP host error handler. WS and unrelated errors remain host-owned. */
export function sendNativeEntryHttpError(request, response, error) {
  if (!error || !/^native_entry_[a-z_]+$/u.test(error.code ?? '') || ![403,503].includes(error.status)
    || request.headers?.upgrade?.toLowerCase() === 'websocket') return false;
  if (response.writableEnded || response.destroyed) return true;
  if (response.headersSent) { response.destroy(); return true; }
  const previousCsp = response.getHeader('Content-Security-Policy'), previousVary = response.getHeader('Vary');
  // An app may have queued its original body, cookie or callback headers before
  // the final policy fence failed. None belong to this public error response.
  for (const name of ['Content-Length','Content-Encoding','Content-Range','Content-Disposition','Content-Location',
    'Transfer-Encoding','Trailer','ETag','Last-Modified','Location','Refresh','Set-Cookie','Set-Cookie2','Link','Retry-After',
    'WWW-Authenticate','Proxy-Authenticate','Authentication-Info','Proxy-Authentication-Info']) response.removeHeader(name);
  const document = documentNavigation(request), body = document ? (error.status === 403 ? deniedPage : page) : JSON.stringify({code:error.code});
  const headers = {'Content-Type':document ? 'text/html; charset=utf-8' : 'application/json',
    'Cache-Control':'no-store','X-Content-Type-Options':'nosniff','Vary':previousVary ? `${previousVary}, ${vary}` : vary};
  if (document) headers['Referrer-Policy'] = 'no-referrer';
  if (document) headers['Content-Security-Policy'] = [...(previousCsp === undefined ? [] : Array.isArray(previousCsp) ? previousCsp : [String(previousCsp)]),
    "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'"];
  response.writeHead(error.status, headers); response.end(request.method === 'HEAD' ? undefined : body); return true;
}
