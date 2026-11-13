/** Isolated real-Chrome voice regression. Start feedback-browser.mjs first.
 * Requires Playwright in node resolution or SOTY_QA_PLAYWRIGHT_ROOT/package.json.
 * Never targets production or a persistent human profile. Evidence stays external.
 */
import assert from 'node:assert/strict';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import {resolve,join} from 'node:path';

const root=fileURLToPath(new URL('../../',import.meta.url));
const args=process.argv.slice(2),output=resolve(args[args.indexOf('--output')+1]||'');
if(!args.includes('--output')||output===resolve(root))throw Error('external-output-directory-required');
const origin='http://127.0.0.1:5237',require=createRequire(import.meta.url);
const playwright=process.env.SOTY_QA_PLAYWRIGHT_ROOT?createRequire(join(resolve(process.env.SOTY_QA_PLAYWRIGHT_ROOT),'package.json'))('playwright'):await import('playwright');
const chrome=process.env.SOTY_QA_CHROME||join(process.env.ProgramFiles||'C:/Program Files','Google/Chrome/Application/chrome.exe');
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
const sourcePath=join(root,'src/world/app-feedback.ts'),sourceSha256=sha(await readFile(sourcePath));
const runtimePaths=['modules/feedback/server/index.mjs','server/app-feedback-capabilities.js','server/http-app.js','scripts/fixtures/feedback-browser.mjs'];
const runtimeSourcePins=Object.fromEntries(await Promise.all(runtimePaths.map(async path=>[path,sha(await readFile(join(root,path)))])));
const record={ok:false,production:false,ownSyntheticBrowserOnly:true,physicalPhone:false,physicalMicrophone:false,humanListening:false,
  actualMediaRecorder:true,fakeAudioCapture:true,actualSignedConnect:true,actualSqlite:true,browserOwnerLogin:false,syntheticServerOwnerSigner:true,
  sourceSha256,runtimeSourcePins,cases:[],errors:[],exactChecksNotRelaxed:true};
const entryHtml=String.raw`<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Isolated voice feedback QA</title><style>body{font:14px system-ui;margin:16px;background:#eef2f8}body>button{min-height:44px;padding:10px;margin:4px}#status{overflow-wrap:anywhere}</style><p>Локальная проверка. Синтетический звук, аккаунты и очередь.</p><button id="open">Открыть обратную связь</button><button id="owner">Очередь владельца</button><p id="status">Подготовка</p><main id="host" class="sw-app" style="position:relative;inset:auto;z-index:auto;height:auto;min-height:0"></main>
<script type="module">
import '/src/world/world.css';
import {mountAppFeedback} from '/src/world/app-feedback.ts';
import {createConnectClient} from '/modules/connect/browser/index.mjs';
const host=document.querySelector('#host'),status=document.querySelector('#status');
let handle,client,info,accountId,mode='reporter',submissions=[],lastTicketId=null;
const sha=async x=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(x)))).map(x=>x.toString(16).padStart(2,'0')).join('');
async function helper(path,body){const response=await fetch('/__fixture/'+path,{method:body===undefined?'GET':'POST',headers:body===undefined?{}:{'content-type':'application/json'},body:body===undefined?undefined:JSON.stringify(body),cache:'no-store'});const value=await response.json();if(!response.ok)throw Error(value.code||'fixture-request-failed');return value;}
async function setup(owner=false){
 handle?.dispose();info=await helper('info');mode=owner?'owner':'reporter';
 if(!owner){client=createConnectClient({projectId:'soty',endpoint:location.origin+'/api/connect/rpc',dbName:'voice-qa:'+info.namespace+':reporter'});let local=await client.getLocalState();if(!local.accountId)await client.bootstrap('Синтетический голосовой отправитель');accountId=(await client.getLocalState()).accountId;
   if(localStorage.getItem('voice-qa-granted')!==accountId){await helper('grant',{accountId});localStorage.setItem('voice-qa-granted',accountId);}}
 handle=mountAppFeedback(host,{accountId:owner?info.ownerAccountId:accountId,appId:info.appId,title:'Синтетическая голосовая очередь',isCurrent:()=>true,
   api:{async request(method,args){if(method==='apps.feedback.submit')submissions.push({requestIdHash:await sha(args.requestId),fingerprint:await sha(JSON.stringify(args)),audioAttachments:args.attachments.filter(a=>a.kind==='audio').length});
     const response=owner?await helper('owner-feedback',{method,args}):await client.extension(method,args,{expectedAccountId:accountId});if(response.receipt?.ticketId)lastTicketId=response.receipt.ticketId;return response;}}});
 status.textContent=owner?'Синтетический owner UI через настоящий подписанный серверный signer':'Готово';handle.open();
}
document.querySelector('#open').onclick=()=>handle?.open();document.querySelector('#owner').onclick=()=>void setup(true);
window.__voiceQa={ready:false,hasUnsaved:()=>handle?.hasUnsavedChanges()??null,flush:()=>handle.flush(),helper,evidence:()=>({mode,submissions:[...submissions],ticketIdPresent:!!lastTicketId,synthetic:true,browserOwnerLogin:false}),
 async counts(){return helper('counts',{});}};
setup().then(()=>window.__voiceQa.ready=true).catch(error=>{status.textContent=error.message;window.__voiceQa.error=error.name;});
</script></html>
`;
await mkdir(output,{recursive:true});
const rate=16000,samples=rate*12,wav=Buffer.alloc(44+samples*2);wav.write('RIFF');wav.writeUInt32LE(wav.length-8,4);wav.write('WAVEfmt ',8);wav.writeUInt32LE(16,16);wav.writeUInt16LE(1,20);wav.writeUInt16LE(1,22);wav.writeUInt32LE(rate,24);wav.writeUInt32LE(rate*2,28);wav.writeUInt16LE(2,32);wav.writeUInt16LE(16,34);wav.write('data',36);wav.writeUInt32LE(samples*2,40);for(let i=0;i<samples;i++)wav.writeInt16LE(Math.round(10000*Math.sin(2*Math.PI*440*i/rate)),44+i*2);const tonePath=join(output,'synthetic-tone.wav');await writeFile(tonePath,wav);
let browser;
const save=()=>writeFile(join(output,'voice-browser.safe.json'),JSON.stringify(record,null,2));
async function pageIn(context){const page=await context.newPage();page.setDefaultTimeout(5000);await page.route(origin+'/__voiceqa',route=>route.fulfill({contentType:'text/html',body:entryHtml}));await page.goto(origin+'/__voiceqa',{waitUntil:'domcontentloaded',timeout:10000});await page.waitForFunction(()=>window.__voiceQa?.ready===true,{},{timeout:8000});return page;}
async function startRecording(page){const dialog=page.locator('dialog[open]');await dialog.getByRole('button',{name:'Записать голос',exact:true}).click();await dialog.getByRole('button',{name:/Остановить ·/}).waitFor({state:'visible'});await page.waitForTimeout(1200);const unsaved=await page.evaluate(()=>window.__voiceQa.hasUnsaved());assert.equal(unsaved,true,'active recording must block loss');return dialog;}
async function audioEvidence(page){return page.locator('dialog[open] audio').evaluate(async audio=>{const bytes=await(await fetch(audio.src)).arrayBuffer();const digest=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes))).map(x=>x.toString(16).padStart(2,'0')).join('');const context=new AudioContext();try{const pcm=await context.decodeAudioData(bytes.slice(0));return {sha256:digest,bytes:bytes.byteLength,decodedDuration:pcm.duration,decodedChannels:pcm.numberOfChannels,decodedFrames:pcm.length,nonSilent:pcm.getChannelData(0).some(v=>Math.abs(v)>.0001),htmlDurationFinite:Number.isFinite(audio.duration)};}finally{await context.close();}});}
async function download(page,name){const link=page.locator('dialog[open]').getByRole('link',{name:/^Скачать аудио:/});assert.equal(await link.count(),1,'one filename-specific audio download link required');const pending=page.waitForEvent('download',{timeout:5000});await link.click();const item=await pending;const path=join(output,name);await item.saveAs(path);const bytes=await readFile(path);return {sha256:sha(bytes),bytes:bytes.length,suggestedName:item.suggestedFilename()};}
async function closeDialog(page){await page.locator('dialog[open]').getByRole('button',{name:'Закрыть',exact:true}).click();await page.locator('dialog[open]').waitFor({state:'detached'});}
async function context(){return browser.newContext({viewport:{width:390,height:844},permissions:['microphone'],acceptDownloads:true});}
try{
 const proof=await fetch(origin+'/__fixture/info',{signal:AbortSignal.timeout(5000)}).then(r=>r.json());assert.equal(proof.synthetic,true,'only local synthetic fixture allowed');
 browser=await playwright.chromium.launch({executablePath:chrome,headless:true,args:['--use-fake-device-for-media-stream','--use-fake-ui-for-media-stream','--use-file-for-fake-audio-capture='+tonePath]});record.browserVersion=browser.version();record.nodeVersion=process.version;
 const first=await context();
 try{
  const page=await pageIn(first),test={name:'active-close-reload-lostAck-restart-owner',activeUnsaved:true};record.cases.push(test);await save();
  await startRecording(page);await closeDialog(page);await page.getByRole('button',{name:'Открыть обратную связь',exact:true}).click();await page.locator('dialog[open] audio').waitFor({state:'visible'});test.closePreview=await audioEvidence(page);assert(test.closePreview.nonSilent&&test.closePreview.decodedDuration>0);
  await page.reload({waitUntil:'domcontentloaded'});await page.waitForFunction(()=>window.__voiceQa?.ready===true);await page.locator('dialog[open] audio').waitFor({state:'visible'});test.reloadPreview=await audioEvidence(page);assert.equal(test.reloadPreview.sha256,test.closePreview.sha256);
  await page.screenshot({path:join(output,'voice-draft-390.png')});await save();
  test.countsBefore=await page.evaluate(()=>window.__voiceQa.counts());await page.evaluate(()=>window.__voiceQa.helper('lose-next-submit-ack',{}));await page.locator('dialog[open]').getByRole('button',{name:'Отправить',exact:true}).click();
  await page.locator('dialog[open]').getByRole('button',{name:'Проверить отправку',exact:true}).waitFor({state:'visible'});await page.waitForFunction(()=>{const b=[...document.querySelectorAll('dialog[open] button')].find(b=>b.textContent.trim()==='Проверить отправку');return b&&!b.disabled;});
  test.countsAfterLostAck=await page.evaluate(()=>window.__voiceQa.counts());assert.equal(test.countsAfterLostAck.tickets-test.countsBefore.tickets,1);assert.equal(test.countsAfterLostAck.attachments-test.countsBefore.attachments,1);
  await page.locator('dialog[open]').getByRole('button',{name:'Проверить отправку',exact:true}).click();await page.waitForFunction(()=>{const b=[...document.querySelectorAll('dialog[open] button')].find(b=>b.textContent.trim()==='Отправить');return b&&!b.disabled;});
  test.submitEvidence=await page.evaluate(()=>window.__voiceQa.evidence());assert.equal(test.submitEvidence.submissions.length,2);assert.deepEqual(test.submitEvidence.submissions[0],test.submitEvidence.submissions[1]);test.countsAfterRetry=await page.evaluate(()=>window.__voiceQa.counts());assert.deepEqual(test.countsAfterRetry,test.countsAfterLostAck);
  test.serverRestart=await page.evaluate(()=>window.__voiceQa.helper('restart',{}));assert.equal(test.serverRestart.restarted,true);
  await closeDialog(page);await page.getByRole('button',{name:'Очередь владельца',exact:true}).click();await page.locator('dialog[open]').getByRole('button',{name:'История',exact:true}).click();await page.locator('[data-feedback-ticket-id]').waitFor({state:'visible'});await page.locator('[data-feedback-ticket-id]').first().click();await page.locator('.sf-ticket audio').waitFor({state:'visible'});
  test.ownerAudio=await audioEvidence(page);assert.equal(test.ownerAudio.sha256,test.closePreview.sha256);test.ownerDownload=await download(page,'voice-owner.webm');assert.equal(test.ownerDownload.sha256,test.closePreview.sha256);
  await page.locator('.sf-ticket audio').evaluate(audio=>audio.play());await page.waitForTimeout(350);test.ownerPlayback=await page.locator('.sf-ticket audio').evaluate(audio=>{const value={advanced:audio.currentTime>0,readyState:audio.readyState,paused:audio.paused};audio.pause();return value;});assert.equal(test.ownerPlayback.advanced,true);
  await page.screenshot({path:join(output,'voice-owner-390.png')});test.ownerUiServerSignerNotHumanLogin=true;test.exactBytesAfterRestart=true;await save();
 }finally{await first.close();}
 const second=await context();
 try{
  const page=await pageIn(second),test={name:'flush-final-chunk-reload',activeUnsaved:true};record.cases.push(test);await startRecording(page);await page.evaluate(()=>window.__voiceQa.flush());await page.locator('dialog[open] audio').waitFor({state:'visible'});test.beforeReload=await audioEvidence(page);assert.equal(await page.evaluate(()=>window.__voiceQa.hasUnsaved()),false);await page.reload({waitUntil:'domcontentloaded'});await page.waitForFunction(()=>window.__voiceQa?.ready===true);await page.locator('dialog[open] audio').waitFor({state:'visible'});test.afterReload=await audioEvidence(page);assert.equal(test.afterReload.sha256,test.beforeReload.sha256);test.preserved=true;await save();
 }finally{await second.close();}
 const third=await context();
 try{
  const page=await pageIn(third),test={name:'saving-off-blocked-close-retains-download'};record.cases.push(test);await page.locator('dialog[open]').getByRole('checkbox',{name:'Сохранять черновик в этом браузере на сутки',exact:true}).uncheck();await startRecording(page);await page.locator('dialog[open]').getByRole('button',{name:'Закрыть',exact:true}).click();await page.locator('dialog[open] audio').waitFor({state:'visible'});assert.equal(await page.locator('dialog[open]').count(),1);assert.equal(await page.evaluate(()=>window.__voiceQa.hasUnsaved()),true);test.closeBlocked=true;test.audio=await audioEvidence(page);test.download=await download(page,'voice-unsaved.webm');assert.equal(test.download.sha256,test.audio.sha256);await page.locator('dialog[open]').getByRole('checkbox',{name:'Сохранять черновик в этом браузере на сутки',exact:true}).check();await closeDialog(page);test.cleanupSavedAndClosed=true;await save();
 }finally{await third.close();}
 record.ok=true;
}catch(error){record.errors.push({name:error.name,code:error.code??'qa-failure',line:Number(error.stack?.match(/feedback-voice\.browser\.mjs:(\d+):/)?.[1]??0)});process.exitCode=1;}
finally{if(browser)await browser.close();record.closedOwnBrowser=true;record.sourcePinStillMatch=sha(await readFile(sourcePath))===sourceSha256;record.runtimeSourcePinsStillMatch=(await Promise.all(runtimePaths.map(async path=>sha(await readFile(join(root,path)))===runtimeSourcePins[path]))).every(Boolean);if(!record.sourcePinStillMatch||!record.runtimeSourcePinsStillMatch){record.ok=false;process.exitCode=1;}await save();console.log(JSON.stringify({ok:record.ok,cases:record.cases.length,sourcePinStillMatch:record.sourcePinStillMatch,runtimeSourcePinsStillMatch:record.runtimeSourcePinsStillMatch,closedOwnBrowser:true,errors:record.errors}));}
