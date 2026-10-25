// Browser-only component fixture with a controlled SDK API boundary. It proves
// draft UI behavior, NOT Root identity/SQL authority (separate installed tests).
import {createServer} from 'node:http';
import {mkdtemp,readFile,rm} from 'node:fs/promises';
import {join,dirname,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {buildSourceAppPackage} from '../scripts/build.mjs';
import {verifyPublicSourceSdk,readSourceSdkArchive} from '../public-release.mjs';
import {fileURLToPath} from 'node:url';
const directory=await mkdtemp(join(tmpdir(),'sdk-template-browser-'));
const publicPacket=process.argv.includes('--public-sdk');let load;
if(publicPacket){const root=fileURLToPath(new URL('../../../',import.meta.url));verifyPublicSourceSdk(root);
  const index=JSON.parse(await readFile(join(root,'public/sdk/source-app/index.json'))),sdk=readSourceSdkArchive(await readFile(join(root,'public',index.artifact.url.slice(1))));
  load=name=>Promise.resolve(sdk.files.get(name==='browser.mjs'?'dist/browser.mjs':'dist/templates/reading-list/assets/'+name));
}else{await buildSourceAppPackage({outdir:directory});const source=new URL('../examples/reading-list/assets/',import.meta.url);
  load=name=>name==='browser.mjs'?readFile(join(directory,name)):readFile(new URL(name,source));}
const assets=new Map([
  ['/embed',{bytes:await load('embed.html'),type:'text/html'}],
  ['/assets/ui.js',{bytes:await load('ui.js'),type:'text/javascript'}],
  ['/assets/ui.css',{bytes:await load('ui.css'),type:'text/css'}],
  ['/assets/source-sdk.js',{bytes:await load('browser.mjs'),type:'text/javascript'}],
]);
let invokes=0,closed=false;
const server=createServer(async(req,res)=>{try{
  const path=new URL(req.url,'http://localhost').pathname;
  if(path==='/__fixture/stop'){res.end('stopped');setImmediate(()=>void close());return;}
  if(path==='/__fixture/state'){res.setHeader('content-type','application/json');res.end(JSON.stringify({invokes}));return;}
  const asset=assets.get(path);if(asset){res.writeHead(200,{'content-type':asset.type+';charset=utf-8','cache-control':'no-store'}).end(asset.bytes);return;}
  if(req.method!=='POST'||!path.startsWith('/api/embed/')){res.writeHead(404).end();return;}
  const chunks=[];let size=0;for await(const chunk of req){size+=chunk.length;if(size>65536)throw Error('fixture_limit');chunks.push(chunk);}
  const args=JSON.parse(Buffer.concat(chunks)),scope=/fixture_profile=B(?:;|$)/u.test(req.headers.cookie??'')?'B':'A',ref=scope.repeat(43);
  let value;
  if(path.endsWith('/session-continue'))value={schema:'soty.source-session-continuation.v1',ready:true,renewable:true,
    sessionExpiresAt:1900000000000,accessExpiresAt:1900000000000,receiptDigest:'0'.repeat(64)};
  else if(args.input?.operation==='soty.current_context')value={ok:true,data:{schema:'soty.current-context.v2',selection:{kind:'soty.workspace.v1',nativeId:'personal',incarnationId:'one'},
    context:{ref,generation:1,expiresAt:1900000000000},title:'Мой список',role:'owner',revision:1}};
  else if(path.endsWith('/query'))value={ok:true,data:{requestId:args.requestId,storageScope:'opaque-personal-'+scope,items:[]}};
  else{if(path.endsWith('/invoke'))invokes++;res.writeHead(503,{'content-type':'application/json'}).end('{"ok":false,"error":{"code":"fixture_no_write"}}');return;}
  res.writeHead(200,{'content-type':'application/json','cache-control':'no-store'}).end(JSON.stringify(value));
}catch{res.writeHead(503).end();}});
async function close(){if(closed)return;closed=true;server.closeAllConnections();await new Promise(done=>server.close(done));
  if(dirname(resolve(directory))!==resolve(tmpdir()))throw Error('fixture_cleanup_path');await rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:30});}
process.on('SIGINT',()=>void close());process.on('SIGTERM',()=>void close());
await new Promise(done=>server.listen(0,'127.0.0.1',done));
console.log(JSON.stringify({fixture:'controlled-sdk-api-not-Root-auth',publicPacket,port:server.address().port,entry:'/embed?sotyFrame='+'F'.repeat(43)}));
