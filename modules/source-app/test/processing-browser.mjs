// Controlled browser fixture: exercises the real UI and SDK, not Root identity.
import {createServer} from 'node:http';
import {mkdtemp,readFile,rm} from 'node:fs/promises';
import {join,dirname,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {digest} from '../server/wire.mjs';
import {SOURCE_FEEDBACK_LIMITS} from '../shared/feedback-wire.mjs';
import {buildSourceAppPackage} from '../scripts/build.mjs';

const directory=await mkdtemp(join(tmpdir(),'soty-processing-ui-'));
const sdk=process.env.SOTY_SOURCE_TEST_SDK_DIRECTORY,itemsMode=process.argv.includes('--items'),feedbackUnavailable=process.argv.includes('--feedback-unavailable');
if(!sdk)await buildSourceAppPackage({outdir:directory});
const browser=await readFile(sdk?join(sdk,'dist','browser.mjs'):join(directory,'browser.mjs'));
const processing=await readFile(sdk?join(sdk,'dist','assets','processing.js'):new URL('../examples/ordinary-app/assets/processing.js',import.meta.url));
const itemsApp=await readFile(sdk?join(sdk,'dist','assets','app.js'):new URL('../examples/ordinary-app/assets/app.js',import.meta.url));
const inbox=await readFile(sdk?join(sdk,'dist','assets','feedback-inbox.js'):new URL('../examples/ordinary-app/assets/feedback-inbox.js',import.meta.url));
const calls={A:[],B:[]},committed={A:false,B:false};let feedbackContextRequests=0;
const json=(res,status,data)=>res.writeHead(status,{'content-type':'application/json','cache-control':'no-store'}).end(JSON.stringify(data));
const server=createServer(async(req,res)=>{try{
  const url=new URL(req.url,'http://localhost'),scope=/fixture_profile=B(?:;|$)/u.test(req.headers.cookie??'')?'B':'A';
  if(url.pathname==='/__fixture/state')return json(res,200,{calls,committed,feedbackContextRequests});
  if(url.pathname==='/__fixture/commit'){committed[scope]=true;return json(res,200,{ok:true});}
  if(url.pathname==='/__fixture/stop'){res.end('stopped');setImmediate(()=>void close());return;}
  if(url.pathname==='/assets/source-sdk.js')return res.writeHead(200,{'content-type':'text/javascript'}).end(browser);
  if(url.pathname==='/assets/processing.js')return res.writeHead(200,{'content-type':'text/javascript'}).end(processing);
  if(url.pathname==='/assets/ordinary-app.js')return res.writeHead(200,{'content-type':'text/javascript'}).end(itemsApp);
  if(url.pathname==='/assets/feedback-inbox.js')return res.writeHead(200,{'content-type':'text/javascript'}).end(inbox);
  if(url.pathname==='/'&&itemsMode)return res.writeHead(200,{'content-type':'text/html; charset=utf-8'}).end(`<!doctype html><meta charset="utf-8"><title>Проверка записей</title>
    <main data-native-origin="http://localhost" data-profile-version="1" data-processing="false"><p id="state" role="status"></p><button id="connect" hidden></button>
    <section id="content" hidden><form id="create"><label>Название записи<input id="title" required></label><button>Добавить запись</button></form><ul id="items"></ul>
    <button id="feedback-open"></button><button id="feedback-inbox-open"></button><section id="feedback-inbox" hidden></section><section id="feedback" hidden>
    <p id="recipient"></p><textarea id="feedback-body"></textarea><input id="feedback-file" type="file"><button id="feedback-send"></button><p id="feedback-state"></p></section></section></main>
    <script type="module" src="/assets/ordinary-app.js"></script>`);
  if(url.pathname==='/')return res.writeHead(200,{'content-type':'text/html; charset=utf-8'}).end(`<!doctype html><meta charset="utf-8"><title>Проверка обработки</title>
    <button id="processing-open">Обработка материалов обращения</button><div id="processing-panel" hidden><ul id="processing-tickets"></ul><p id="processing-state" role="status"></p></div>
    <script type="module">import{mountProcessingUi}from'/assets/processing.js';mountProcessingUi();</script>`);
  if(url.pathname==='/api/embed/feedback/context'){
    feedbackContextRequests++;if(feedbackUnavailable)return json(res,503,{ok:false,error:{code:'fixture_feedback_unavailable'}});
    return json(res,200,{ok:true,data:{schema:'soty.source-feedback.context.v1',bindingDigest:scope.toLowerCase().repeat(64),
      ready:true,canSubmit:true,recipientLabel:'Учебный владелец',limits:SOURCE_FEEDBACK_LIMITS,capabilities:{text:true,voice:true,screenshot:true,asr:false}}});
  }
  if(url.pathname==='/api/embed/feedback')return json(res,200,{ok:true,data:{tickets:[{id:'fixture-ticket',revision:1,status:'received',body:'Учебное обращение',
    createdAt:1,updatedAt:1,canReply:true,canManage:false,canAccept:false,attachments:[],messages:[]}],nextCursor:null}});
  if(url.pathname==='/api/embed/context')return json(res,200,{ready:true});
  const chunks=[];for await(const chunk of req)chunks.push(chunk);const body=JSON.parse(Buffer.concat(chunks).toString('utf8'));
  if(url.pathname==='/api/embed/query'){
    if(body.input.operation==='items.list')return json(res,200,{ok:true,data:{requestId:body.requestId,items:[]}});
    if(body.input.operation==='items.pending-scope')return json(res,200,{ok:true,data:{requestId:body.requestId,scope:scope.toLowerCase().repeat(64)}});
    if(body.input.operation==='feedback.processing.context')return json(res,200,{ok:true,data:{schema:'soty.feedback.processing-context.v1',localOnly:true,
      requiresReporterConsent:true,purposes:['ocr'],policyRef:{id:'policy',version:1,digest:'a'.repeat(64)},maxBudget:{},engines:[]}});
    if(body.input.operation==='feedback.processing.ticket')return json(res,200,{ok:true,data:{ticketId:'fixture-ticket',ticketRevision:1,attachmentDigest:'a'.repeat(64),
      canConsent:true,canGrant:false,hasImage:true,hasAudio:false}});
  }
  if(url.pathname==='/api/embed/invoke'){calls[scope].push({requestId:body.requestId,inputDigest:digest(body.input)});
    return json(res,503,{ok:false,error:{code:'source_app_effect_unknown'}});}
  if(url.pathname==='/api/embed/receipt'){
    const found=calls[scope].find(call=>call.requestId===body.requestId&&call.inputDigest===body.input.inputDigest);
    if(committed[scope]&&found)return json(res,200,{ok:true,data:{...found,outcome:'committed'}});
    return json(res,409,{ok:false,error:{code:'source_receipt_unknown'}});
  }
  res.writeHead(404).end();
}catch{res.writeHead(503).end();}});
let closed=false;async function close(){if(closed)return;closed=true;server.closeAllConnections();await new Promise(done=>server.close(done));
  if(dirname(resolve(directory))!==resolve(tmpdir()))throw Error('fixture_cleanup_path');await rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:30});}
process.on('SIGINT',()=>void close());process.on('SIGTERM',()=>void close());
await new Promise(done=>server.listen(0,'127.0.0.1',done));
console.log(JSON.stringify({fixture:'controlled-api-not-Root-auth',itemsMode,port:server.address().port,installedSdk:Boolean(sdk)}));
