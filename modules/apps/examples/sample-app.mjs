import { createServer } from 'node:http';
import { WebSocketServer } from 'ws';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

// A genuinely separate local project used by transport/browser acceptance.
// It has no dependency on Soty and can also run directly on its loopback port.
export async function createSampleApp({ port = 0 } = {}) {
  let items = [{ id: 'bread', text: 'Хлеб', done: false }, { id: 'milk', text: 'Молоко', done: true }];
  const requests = [];
  const wss = new WebSocketServer({ noServer: true, maxPayload: 1024 * 1024 });
  const app = createServer(async (req, res) => {
    requests.push({ path: req.url, headers: { ...req.headers } });
    if (req.url === '/api/items') {
      if (req.method === 'POST') {
        const parts = []; for await (const chunk of req) parts.push(chunk);
        const input = JSON.parse(Buffer.concat(parts).toString('utf8'));
        if (input.id) items = items.map(item => item.id === input.id ? { ...item, done: !item.done } : item);
        else if (typeof input.text === 'string' && input.text.trim()) items.push({ id: `item-${items.length}`, text: input.text.trim().slice(0, 100), done: false });
        for (const ws of wss.clients) ws.send(JSON.stringify(items));
      }
      res.writeHead(200, { 'Content-Type': 'application/json', 'Set-Cookie': 'local_secret=do-not-forward; Path=/' }); res.end(JSON.stringify(items)); return;
    }
    if (req.url === '/large') { res.writeHead(200, { 'Content-Type': 'application/octet-stream' }); for (let i = 0; i < 64; i++) res.write(Buffer.alloc(64 * 1024, i)); res.end(); return; }
    if (req.url === '/slow') {
      res.writeHead(200, { 'Content-Type': 'text/plain' }); res.write('first\n');
      const timer = setInterval(() => res.write('next\n'), 100); res.on('close', () => clearInterval(timer)); return;
    }
    if (req.url === '/redirect') { res.writeHead(302, { Location: 'http://169.254.169.254/latest/meta-data/' }); res.end(); return; }
    if (req.url === '/app.js') { res.writeHead(200, { 'Content-Type': 'text/javascript' }); res.end(sampleScript); return; }
    if (req.url === '/styles.css') { res.writeHead(200, { 'Content-Type': 'text/css' }); res.end(sampleStyles); return; }
    // Keep the real query intact in requests while serving the same app shell.
    if (new URL(req.url, 'http://localhost').pathname !== '/') { res.writeHead(404); res.end(); return; }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' }); res.end(sampleHtml);
  });
  app.on('upgrade', (req, socket, head) => {
    if (req.url !== '/live') { socket.destroy(); return; }
    wss.handleUpgrade(req, socket, head, ws => { ws.send(JSON.stringify(items)); ws.on('message', bytes => ws.send(bytes)); });
  });
  await new Promise(resolve => app.listen(port, '127.0.0.1', resolve));
  return { port: app.address().port, requests, server: app, close: async () => { for (const ws of wss.clients) ws.terminate(); wss.close(); app.closeAllConnections(); await new Promise(resolve => app.close(resolve)); } };
}
const sampleHtml = '<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Покупки</title><link rel="stylesheet" href="/styles.css"><main><span class="eyebrow">СЕМЕЙНЫЙ СПИСОК</span><h1>Покупки<span aria-hidden="true">◈</span></h1><p id="status" role="status">Подключаемся…</p><div id="items"></div><form><label for="item">Добавить в список</label><div class="input"><input id="item" placeholder="Что купить?" maxlength="100" required><button aria-label="Добавить">+</button></div></form></main><script src="/app.js"></script></html>';
const sampleStyles = 'html{color-scheme:light}*{box-sizing:border-box}body{margin:0;background:#f8f6ef;color:#262822;font:16px system-ui,sans-serif}main{max-width:620px;margin:auto;padding:40px 26px}.eyebrow{font-size:10px;letter-spacing:.22em;color:#78786b}h1{font-size:38px;font-weight:550;letter-spacing:-.055em;margin:15px 0 4px;display:flex;justify-content:space-between}h1 span{color:#b88735}#status{font-size:12px;color:#77836e;margin-bottom:28px}.item{display:flex;align-items:center;gap:14px;padding:20px 0;border-bottom:1px solid #e6e2d7}.item input{width:22px;height:22px;accent-color:#ad843d}.item input:checked+span{text-decoration:line-through;color:#96978d}label{font-size:12px;color:#77786e}form{margin-top:34px}.input{display:flex;margin-top:10px;border:1px solid #ddd8c9;border-radius:12px;overflow:hidden}input[type=text],input:not([type]){font:inherit;border:0;background:transparent;padding:15px;min-width:0;flex:1}button{border:0;background:#d9b267;width:55px;font-size:26px;cursor:pointer}button:hover{background:#caa456}input:focus-visible,button:focus-visible{outline:3px solid #997938;outline-offset:2px}';
const sampleScript = `const list=document.querySelector('#items'),status=document.querySelector('#status');
function render(items){list.replaceChildren(...items.map(item=>{const row=document.createElement('label');row.className='item';const check=document.createElement('input');check.type='checkbox';check.checked=item.done;check.addEventListener('change',()=>fetch('/api/items',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id:item.id})}).then(r=>r.json()).then(render));const text=document.createElement('span');text.textContent=item.text;row.append(check,text);return row}))}
fetch('/api/items').then(r=>r.json()).then(render);
const socket=new WebSocket(location.origin.replace(/^http/,'ws')+'/live');socket.onopen=()=>status.textContent='Общий список · подключено';socket.onmessage=e=>render(JSON.parse(e.data));socket.onclose=()=>{status.textContent='Соединение закрыто';document.querySelectorAll('input,button').forEach(e=>e.disabled=true)};
document.querySelector('form').onsubmit=e=>{e.preventDefault();const input=document.querySelector('#item');fetch('/api/items',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text:input.value})}).then(r=>r.json()).then(render);input.value=''};`;

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const sample = await createSampleApp();
  console.log(`Покупки: http://127.0.0.1:${sample.port} — этот порт можно добавить в Соты.`);
  let closing = false;
  const close = () => { if (!closing) { closing = true; void sample.close(); } };
  process.once('SIGINT', close); process.once('SIGTERM', close);
}
