import { createHash } from 'node:crypto';
import { createPalette } from '../modules/ui/palette.mjs';
import { roundedHexPath, SQRT3 } from '../modules/ui/hex.mjs';

const tokens = scheme => Object.entries(createPalette(scheme)).map(([name, value]) => `${name}:${value}`).join(';');
const css = `:root{${tokens('light')};color-scheme:light}@media(prefers-color-scheme:dark){:root{${tokens('dark')};color-scheme:dark}}
*{box-sizing:border-box}body{margin:0;min-height:100dvh;display:grid;place-items:center;padding:24px 16px;background:var(--sw-bg);color:var(--sw-ink);font-family:system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
main{width:min(100%,440px);min-width:0;padding:30px;border:1px solid var(--sw-line);border-radius:28px;background:var(--sw-surface);box-shadow:var(--sw-shadow-small)}
.brand{display:flex;align-items:center;gap:10px;margin:0 0 28px;font-size:18px;font-weight:650;letter-spacing:-.035em}.brand svg{width:38px;height:auto;flex:none;fill:var(--sw-honey)}
h1{font-size:clamp(26px,5vw,34px);font-weight:650;letter-spacing:-.045em;line-height:1.15;margin:0 0 18px;overflow-wrap:anywhere}
p{font-size:15px;line-height:1.65;color:var(--sw-muted);margin:0}
a{display:flex;align-items:center;justify-content:center;min-height:48px;margin-top:24px;padding:11px 14px;border-radius:14px;background:var(--sw-action);color:var(--sw-action-ink);font-size:14px;font-weight:600;text-decoration:none;text-align:center}
a:hover{background:var(--sw-action-hover)}a:focus-visible{outline:2px solid var(--sw-focus);outline-offset:4px}
@media(max-width:420px){body{padding:16px 12px}main{padding:23px 19px;border-radius:23px}.brand{margin-bottom:23px}}
@media(max-height:500px){body{place-items:start center;padding:14px}main{padding:20px}.brand{margin-bottom:16px}}
@media(forced-colors:active){main,a{border:1px solid CanvasText}.brand svg{fill:CanvasText}}`;

// A fixed document, independent of request parameters or Provider error text.
// Even an expired/malformed authorization stays usable without loading the PWA.
export const OAUTH_FAILURE_POLICY = `default-src 'none'; base-uri 'none'; object-src 'none'; script-src 'none'; style-src 'sha256-${createHash('sha256').update(css).digest('base64')}'; frame-ancestors 'none'; form-action 'none'`;
export const OAUTH_FAILURE_DOCUMENT = `<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="color-scheme" content="light dark"><title>Подключение — Соты</title><style>${css}</style></head><body><main aria-labelledby="title"><div class="brand"><svg aria-hidden="true" viewBox="0 0 40 ${20 * SQRT3}"><path d="${roundedHexPath(20, undefined, { x: 20, y: 10 * SQRT3 })}"/></svg><span>Соты</span></div><h1 id="title">Запрос подключения недоступен</h1><p>Проверьте подключение в клиенте. При необходимости начните новый запрос.</p><a href="/">Открыть Соты</a></main></body></html>`;
