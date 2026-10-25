// Opt-in, disposable actual signed Human/RSA/OIDC/SQLite + real DOM fixture.
// The browser RPC bridge is test-owned; this is not a human acceptance pilot.
import {createServer} from 'node:http';
import {readFile,mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,dirname,resolve,basename} from 'node:path';
import {fileURLToPath} from 'node:url';
import {build} from 'esbuild';
import {environment} from '../../modules/human-identity/test/support/renewal-fixture.mjs';
const directory=await mkdtemp(join(tmpdir(),'human-signins-browser-')),cleanups=[];
const f=await environment({after:fn=>cleanups.push(fn)},{deviceConsent:true});
if(f.identity.signinCapabilities?.checkRevoke!==true)throw Error('reviewed_human_signins_backend_required');
const other=await f.backupDevice(),actors={A:f.actor,B:f.outsider,'A-device2':other};let mode='A',drop=false,closed=false;
async function remember(signer,rp){const flow=await f.begin(rp,f.freshWire());await f.approve(flow,signer,{stayInAppSeconds:86400,rememberForDevice:true});
  const result=await f.complete(flow);if((await flow.session.request(result.callback)).status!==200)throw Error('fixture_callback_denied');}
await remember(f.actor,f.rps[0]);await remember(f.actor,f.rps[1]);await remember(other,f.rps[1]);await remember(f.outsider,f.rps[0]);
await build({entryPoints:[fileURLToPath(new URL('./human-signins.browser-fixture.ts',import.meta.url))],outfile:join(directory,'app.js'),bundle:true,format:'esm',platform:'browser',target:'es2022',logLevel:'silent'});
const script=await readFile(join(directory,'app.js')),style=await readFile(join(directory,'app.css')),counts={list:0,revoke:0,checkRevoke:0};
const page='<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Входы в приложения · проверка</title><link rel="stylesheet" href="/app.css"><script type="module" src="/app.js"></script><body></body></html>';
const methods=new Set(['identity.human.signins.list','identity.human.signins.revoke','identity.human.signins.checkRevoke']);
const server=createServer(async(req,res)=>{const path=new URL(req.url,'http://127.0.0.1').pathname;
  const json=(status,value)=>res.writeHead(status,{'content-type':'application/json','cache-control':'no-store'}).end(JSON.stringify(value));
  try{
    if(path==='/__fixture/stop'){res.end('stopped');setImmediate(()=>void close());return;}
    if(path==='/app.js'){res.writeHead(200,{'content-type':'text/javascript'}).end(script);return;}
    if(path==='/app.css'){res.writeHead(200,{'content-type':'text/css'}).end(style);return;}
    if(path==='/favicon.ico'){res.writeHead(204).end();return;}
    if(path==='/api/apps/capabilities'){json(200,{humanSignins:f.identity.signinCapabilities});return;}
    if(path==='/__fixture/identity'){const signer=actors[mode];json(200,{accountId:signer.account.accountId,deviceId:signer.account.deviceId,label:mode==='B'?'Профиль Б':'Мой профиль'});return;}
    if(path==='/__fixture/counts'){json(200,counts);return;}
    if(req.method==='POST'){
      let length=0;const chunks=[];for await(const chunk of req){length+=chunk.length;if(length>16384)throw Error('fixture_body_limit');chunks.push(chunk);}
      const body=chunks.length?JSON.parse(Buffer.concat(chunks)):{};
      if(path==='/__fixture/profile'){if(!Object.hasOwn(actors,body.mode))throw Error('fixture_profile_invalid');mode=body.mode;json(200,{ok:true});return;}
      if(path==='/__fixture/drop-next-ack'){drop=true;json(200,{ok:true});return;}
      if(path==='/__fixture/rpc'){
        if(!methods.has(body.method))throw Error('fixture_method_invalid');const key=body.method.split('.').at(-1);counts[key]++;
        const signer=actors[mode],value=await signer.client.extension(body.method,body.args,{expectedAccountId:signer.account.accountId});
        // An entirely empty response can make Chromium retry the test bridge's
        // POST. Lose the ACK body AFTER headers, so this case proves unknown UI
        // recovery rather than the browser's transport retry implementation.
        if(drop&&key==='revoke'){drop=false;res.writeHead(200,{'content-type':'application/json'});res.flushHeaders();res.write('{"incomplete":');setTimeout(()=>res.destroy(),20);return;}json(200,value);return;
      }
    }
    if(path==='/'){res.writeHead(200,{'content-type':'text/html','cache-control':'no-store'}).end(page);return;}
    res.writeHead(404).end();
  }catch(error){json(503,{code:typeof error.code==='string'?error.code:'fixture_rpc_failed'});}
});
async function close(){if(closed)return;closed=true;server.closeAllConnections();await new Promise(done=>server.close(done));for(const cleanup of cleanups)await cleanup();
  if(dirname(resolve(directory))!==resolve(tmpdir())||!basename(directory).startsWith('human-signins-browser-'))throw Error('cleanup_path');await rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:50});}
process.on('SIGINT',()=>void close());process.on('SIGTERM',()=>void close());
await new Promise(done=>server.listen(0,'127.0.0.1',done));console.log(JSON.stringify({fixture:'actual-signed-Human-RSA-OIDC-SQLite-and-DOM',port:server.address().port,humanPilot:false}));
