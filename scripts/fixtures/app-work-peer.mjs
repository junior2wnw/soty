// Synthetic local-only cross-origin app; never connects to an account or production.
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
const port = Number(process.argv[2] ?? 5199), parentPort = Number(process.argv[3] ?? 5198), delayMs = Number(process.argv[4] ?? 0);
if (![port, parentPort].every(value => Number.isInteger(value) && value > 1024 && value < 65536) || port === parentPort) throw Error('fixture_ports_invalid');
if (!Number.isInteger(delayMs) || delayMs < 0 || delayMs > 5000) throw Error('fixture_delay_invalid');
const sdk = await readFile(new URL('../../modules/source-app/browser/app-work.mjs', import.meta.url));
const html = `<!doctype html><html lang="ru"><meta charset="utf-8"><title>Тестовый черновик</title>
<style>body{font:16px system-ui;padding:24px}textarea{display:block;width:80%;height:120px;margin:12px 0}label{display:block;margin:12px 0}</style>
<h1>Тестовый черновик</h1><label>Текст<textarea id="draft"></textarea></label>
<label>Состояние сохранения <select id="mode"><option value="ok">Работает</option><option value="error">Ошибка</option><option value="lost">Нет ответа</option></select></label>
<p id="status">Подготовка</p><script type="module">
import {createSotyAppWork} from '/work.mjs';
const input=document.querySelector('#draft'), mode=document.querySelector('#mode'), status=document.querySelector('#status');
let saved=localStorage.getItem('synthetic-draft')??'',resume=null;input.value=saved;
if (${delayMs}) await new Promise(resolve=>setTimeout(resolve,${delayMs}));
const work=createSotyAppWork({parentOrigin:'http://127.0.0.1:${parentPort}',isDirty:()=>input.value!==saved,
 save:async()=>{status.textContent='Сохранение';if(mode.value==='error')throw Error('synthetic_save_error');
 if(mode.value==='lost')await new Promise(resolve=>{resume=resolve});saved=input.value;localStorage.setItem('synthetic-draft',saved);status.textContent='Сохранено';}});
input.addEventListener('input',()=>work.changed());mode.addEventListener('change',()=>{if(mode.value==='ok'&&resume){resume();resume=null;}});status.textContent='Готово';
</script></html>`;
const server = createServer((req, res) => {
  if (req.method !== 'GET') return res.writeHead(405).end();
  const path = new URL(req.url, 'http://127.0.0.1').pathname;
  if (path === '/work.mjs') return res.writeHead(200, { 'content-type': 'text/javascript', 'cache-control': 'no-store' }).end(sdk);
  if (path === '/_soty/boot') return res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' }).end(html);
  return res.writeHead(404).end();
});
server.listen(port, '127.0.0.1', () => console.log(JSON.stringify({ synthetic: true, origin: `http://127.0.0.1:${port}`, parentOrigin: `http://127.0.0.1:${parentPort}` })));
