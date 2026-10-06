// Own synthetic UI fixture; does not attest an installed Apps slot/native ACL.
import { createServer } from 'node:http';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { validateFeedbackAttachments } from '../../modules/feedback/server/media.mjs';
const files = {
  '/capture.mjs': new URL('../../src/world/project-feedback-capture.mjs', import.meta.url),
  '/picker.mjs': new URL('../../src/world/project-feedback-picker.mjs', import.meta.url),
  '/feedback-media.mjs': new URL('../../src/world/feedback-media.mjs', import.meta.url),
  '/picker.css': new URL('../../src/world/project-feedback-picker.css', import.meta.url),
};
const alias = { '/project-feedback-capture.mjs': '/capture.mjs', '/project-feedback-picker.mjs': '/picker.mjs' };
const evidence = { selected: 0, submissions: 0, validatedImage: 0, validatedAudio: 0, sourceMicrophoneDelegated: null };
let rootOrigin, sourceOrigin;
function staticFile(req, res) {
  const pathname = new URL(req.url, 'http://fixture.invalid').pathname, file = files[alias[pathname] || pathname];
  if (!file) return false;
  res.writeHead(200, { 'Content-Type': pathname.endsWith('.css') ? 'text/css; charset=utf-8' : 'text/javascript; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(readFileSync(file)); return true;
}
const parent = createServer((req, res) => {
  if (staticFile(req, res)) return;
  res.setHeader('Permissions-Policy', 'microphone=(self), camera=()');
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.end(`<!doctype html><html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/picker.css"><title>Изолированная проверка вложений</title></head><body style="margin:12px;font:16px system-ui"><h1 style="font-size:20px">Вложения для проекта</h1><iframe title="Проект в изолированной проверке" src="${sourceOrigin}/" style="display:block;width:100%;height:360px;border:0"></iframe><script type="module">
    import {mountProjectCaptureBridge} from '/capture.mjs';import {pickProjectFeedbackMedia} from '/picker.mjs';
    const frame=document.querySelector('iframe'),slot={};
    const peer={approved:true,window:frame.contentWindow,origin:${JSON.stringify(sourceOrigin)},sourceId:'source-fixture',appId:'app-fixture',accountId:'account-fixture',generation:1,slot,title:'Изолированный проект'};
    mountProjectCaptureBridge({view:window,readPeer:()=>peer,assertPeer:async()=>true,capture:pickProjectFeedbackMedia});
  </script></body></html>`);
});
const source = createServer(async (req, res) => {
  if (staticFile(req, res)) return;
  res.setHeader('Cache-Control', 'no-store');
  if (req.method === 'GET' && req.url === '/evidence') { res.setHeader('Content-Type', 'application/json'); res.end(JSON.stringify(evidence)); return; }
  if (req.method === 'POST' && ['/selected', '/submit'].includes(req.url)) {
    const parts = []; let size = 0;
    for await (const part of req) { size += part.length; if (size > 1500000) { res.writeHead(413).end(); return; } parts.push(part); }
    try {
      if (req.headers.origin !== sourceOrigin) throw new Error('fixture_origin');
      const input = JSON.parse(Buffer.concat(parts).toString('utf8'));
      if (req.url === '/selected') { evidence.selected++; evidence.sourceMicrophoneDelegated = input.sourceMicrophoneDelegated; }
      else {
        const media = validateFeedbackAttachments(input.attachments);
        evidence.submissions++; evidence.validatedImage += media.filter(item => item.kind === 'image').length;
        evidence.validatedAudio += media.filter(item => item.kind === 'audio').length;
      }
      res.setHeader('Content-Type', 'application/json'); res.end('{"ok":true}');
    } catch { res.writeHead(400).end(); }
    return;
  }
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.end(`<!doctype html><html lang="ru"><head><meta charset="utf-8"><style>body{font:16px system-ui;margin:12px}button{min-height:44px;padding:10px;margin:6px}img{max-width:100%;max-height:100px}audio{max-width:100%}</style></head><body><h2>Частный проект</h2><button id="image">Добавить скриншот</button><button id="audio">Добавить голос</button><p id="state" role="status">Обращение не отправлено</p><div id="preview"></div><button id="send" disabled>Отправить обращение</button><script type="module">
    import {requestProjectCapture} from '/capture.mjs';let selected=[];const state=document.querySelector('#state');
    for(const kind of ['image','audio'])document.querySelector('#'+kind).onclick=async()=>{try{
      const attachments=await requestProjectCapture({parent:window.parent,parentOrigin:${JSON.stringify(rootOrigin)},sourceId:'source-fixture',projectId:' Частный проект/А ',contextRevision:1,kind,isCurrent:()=>true});
      selected=attachments;state.textContent='Вложение выбрано. Обращение ещё не отправлено.';document.querySelector('#send').disabled=false;
      const preview=document.querySelector('#preview');preview.replaceChildren();for(const a of attachments){const m=document.createElement(a.kind==='image'?'img':'audio');m.src='data:'+a.mimeType+';base64,'+a.dataBase64;if(a.kind==='audio')m.controls=true;preview.append(m);}
      await fetch('/selected',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({sourceMicrophoneDelegated:document.permissionsPolicy?.allowsFeature('microphone')??document.featurePolicy?.allowsFeature('microphone')??null})});
    }catch{state.textContent='Вложение не передано.'}};
    document.querySelector('#send').onclick=async()=>{const r=await fetch('/submit',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({attachments:selected})});state.textContent=r.ok?'Вложение проверено и обращение отправлено':'Вложение не прошло проверку';};
  </script></body></html>`);
});
await new Promise(resolve => parent.listen(0, '127.0.0.1', resolve));
rootOrigin = 'http://127.0.0.1:' + parent.address().port;
await new Promise(resolve => source.listen(0, '127.0.0.1', resolve));
sourceOrigin = 'http://127.0.0.1:' + source.address().port;
console.log(JSON.stringify({ ready: true, rootOrigin, sourceOrigin, ownedSynthetic: true, actualRootSlot: false }));
const stop = async () => { parent.closeAllConnections(); source.closeAllConnections();
  await Promise.all([new Promise(resolve => parent.close(resolve)), new Promise(resolve => source.close(resolve))]); process.exit(0); };
process.on('SIGINT', stop); process.on('SIGTERM', stop);
