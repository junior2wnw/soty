// Isolated signed Root/connector fixture. Never loaded by the consumer launcher.
import assert from 'node:assert/strict';
import {createServer,request} from 'node:http';
import {createHmac,generateKeyPairSync,randomBytes} from 'node:crypto';
import {mkdtemp,mkdir,symlink,rm,readFile} from 'node:fs/promises';
import {dirname,join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {createRequire} from 'node:module';
import {pathToFileURL,fileURLToPath} from 'node:url';
import {createInterface} from 'node:readline';
import {prepareReadingListProject} from '../prepare-project.mjs';
import {createReadingListServer} from '../server.mjs';
import {selectedResourceProfile} from '../../../../apps/scoped-embed/resource-profile.mjs';
import {createResourceSourceProofSigner} from '../../../../apps/scoped-embed/resource-proof.mjs';
import {digest} from '../../../../source-app/server/wire.mjs';

const require=createRequire(import.meta.url),oidcRequire=createRequire(require.resolve('openid-client'));
const {SignJWT,importJWK}=await import(pathToFileURL(oidcRequire.resolve('jose')).href);
const listen=server=>new Promise(done=>server.listen(0,'127.0.0.1',done));
const origin=server=>'http://127.0.0.1:'+server.address().port;
const stop=async server=>{server.closeAllConnections();if(server.listening)await new Promise(done=>server.close(done));};
const random=()=>randomBytes(32).toString('base64url');

export async function fixture({ui,mode}={}){
 const directory=await mkdtemp(join(tmpdir(),'soty-reading-widget-')),project=join(directory,'project');
 let source,profile,context,rawProfile,signer,closed=false,rootLive=true,holdNext=false,held=null,releaseHeld=null;
 const transport=[];
 const key=randomBytes(32),cipherKey=randomBytes(32),clock=()=>Date.now();
 const pair=generateKeyPairSync('rsa',{modulusLength:2048});
 const jwk={...pair.publicKey.export({format:'jwk'}),kid:'reading-fixture',alg:'RS256',use:'sig'};
 const signing=await importJWK({...pair.privateKey.export({format:'jwk'}),kid:jwk.kid,alg:'RS256',use:'sig'},'RS256');
 const widgetContext=mode?await readFile(new URL('../../../../../vendor/optical-glass/src/widget-context.js',import.meta.url)):null;
 const parent=createServer((req,res)=>{
  if(req.url==='/fixture-widget-context.js'&&widgetContext){res.writeHead(200,{'content-type':'text/javascript'}).end(widgetContext);return;}
  if(req.url==='/'){res.writeHead(200,{'content-type':'text/html;charset=utf-8'}).end(`<!doctype html><meta charset="utf-8"><title>Reading widget isolated fixture</title><style>body{margin:0;background:#dce7ed}iframe{display:block;width:720px;height:720px;border:0}</style><iframe title="Список чтения" src="${origin(gateway)}/embed"></iframe><script>window.workStates=[];const session=crypto.randomUUID();addEventListener('message',event=>{if(event.origin!==${JSON.stringify(origin(gateway))})return;const data=event.data;if(data?.schema!=='soty.app-work.v1')return;if(data.type==='hello')event.source.postMessage({schema:data.schema,type:'probe',session},event.origin);else window.workStates.push(data);});</script>${mode?`<script type="module">import{createGlassWidgetHost}from'/fixture-widget-context.js';window.widgetHost=createGlassWidgetHost(document.querySelector('iframe'),{origin:${JSON.stringify(origin(gateway))},initialMode:${JSON.stringify(mode)}});</script>`:''}`);return;}
  res.writeHead(200,{'content-type':'application/json'}).end(JSON.stringify({keys:[jwk]}));
 });
 const authority=createServer(async(req,res)=>{try{
  const chunks=[];for await(const chunk of req)chunks.push(chunk);const input=JSON.parse(Buffer.concat(chunks));
  if(!rootLive){res.writeHead(403).end();return;}
  const now=clock(),claims={iss:profile.issuer,aud:'urn:soty:source-authority',sub:context.humanPrincipal.subject,
   iat:Math.floor(now/1000),exp:Math.floor(Math.min(now+10000,context.expiresAt)/1000),sourceChallenge:input.nonce,contextDigest:digest(context)};
  const identityWitness=await new SignJWT(claims).setProtectedHeader({alg:'RS256',typ:'soty-source-authority+jwt',kid:jwk.kid}).sign(signing);
  const body=JSON.stringify({nonce:input.nonce,context,identityWitness});
  res.writeHead(200,{'content-type':'application/json','x-soty-source-mac':createHmac('sha256',key).update('authority-result\0'+body).digest('base64url')}).end(body);
 }catch{res.writeHead(503).end();}});
 const gateway=createServer(async(req,res)=>{try{
  const path=new URL(req.url,'http://fixture.invalid').pathname;
  const chunks=[];for await(const chunk of req)chunks.push(chunk);const body=Buffer.concat(chunks),headers={host:new URL(profile.embedOrigin).host};
  for(const name of ['content-type','accept','cookie'])if(req.headers[name])headers[name]=req.headers[name];
  if(path.startsWith('/api/embed/'))Object.assign(headers,{origin:profile.embedOrigin,
   ...signer.headers({context,method:req.method,path,body,cookie:req.headers.cookie??''})});
  const result=await new Promise((done,reject)=>{
   const upstream=request({host:'127.0.0.1',port:source.server.address().port,path:req.url,method:req.method,headers},response=>{
    const data=[];response.on('data',chunk=>data.push(chunk));response.once('end',()=>done({status:response.statusCode,headers:response.headers,bytes:Buffer.concat(data)}));response.once('error',reject);});
   upstream.once('error',reject);upstream.setTimeout(10000,()=>upstream.destroy(Error('fixture_upstream_timeout')));upstream.end(body);
  });
  if(path==='/api/embed/invoke'&&holdNext){holdNext=false;held=true;await new Promise(done=>{releaseHeld=done;});held=null;releaseHeld=null;}
  res.writeHead(result.status,result.headers);res.end(result.bytes);
 }catch{if(!res.headersSent)res.writeHead(503,{'content-type':'application/json'}).end('{"ok":false,"error":{"code":"fixture_transport_unavailable"}}');else res.destroy();}});
 async function close(){if(closed)return;closed=true;releaseHeld?.();await source?.close();
  await Promise.all([stop(gateway),stop(authority),stop(parent)]);
  assert.equal(dirname(resolve(directory)),resolve(tmpdir()));await rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:40});}
 try{
 await Promise.all([listen(parent),listen(authority),listen(gateway)]);
 const reservation=createServer();await listen(reservation);const sourcePort=reservation.address().port;await stop(reservation);
 await prepareReadingListProject({directory:project,parentOrigin:origin(parent),port:sourcePort});
 const sdkDirectory=join(project,'.soty-sdk');await mkdir(join(sdkDirectory,'node_modules'));
 await symlink(dirname(dirname(require.resolve('openid-client'))),join(sdkDirectory,'node_modules/openid-client'),process.platform==='win32'?'junction':'dir');
 const sdk=await import(pathToFileURL(join(sdkDirectory,'dist/server.mjs')).href);
 const browserSdk=await import(pathToFileURL(join(sdkDirectory,'dist/browser.mjs')).href);
 const appId='app-'+'e'.repeat(32),issuer=origin(parent)+'/human-identity';
 rawProfile={schema:'soty.selected-human-embed.v2',appId,connector:{linkId:'reading-fixture',hostDeviceId:'fixture-host',connectorId:'fixture-connector'},
  target:{revision:1,digest:'1'.repeat(64)},sourceProfile:sdk.STANDARD_ROOT_WORKSPACE_SOURCE,
  resource:{registryId:'soty',environmentId:'fixture',tenantId:'reading-author',appId,resourceId:'personal-workspaces',selection:{kind:'soty.workspace.v1',nativeId:'personal-workspaces',incarnationId:'fixture-one'}},
  issuer,clientId:'reading-list-fixture',embedOrigin:origin(gateway),nativeOrigin:origin(gateway),parentOrigin:origin(parent)};
 profile=selectedResourceProfile(rawProfile);signer=createResourceSourceProofSigner({profile,key,clock});
 function frame(subject='alice'){
  context={schema:'soty.verified-launch-continuation.v2',reference:{id:random(),version:1,digest:randomBytes(32).toString('hex')},
   profileDigest:profile.digest,appId,sourceProfile:profile.sourceProfile,resource:profile.resource,instanceId:'main',
   rootPrincipal:{accountId:'fixture-'+subject,deviceId:'fixture-device-'+subject},
   humanPrincipal:{issuer,subject,clientId:profile.clientId,clientProfileDigest:'2'.repeat(64),clientGeneration:1},
   entry:{domainId:'reading-fixture-domain',origin:profile.embedOrigin},target:profile.target,policyEpoch:1,expiresAt:clock()+300000};
 }
 frame();
 const databasePath=join(directory,'reading.sqlite');
 const glassCss=await readFile(new URL('../../../../../vendor/optical-glass/src/ui-kit.css',import.meta.url));
 async function start(initialize=false){source=await createReadingListServer({sdkDirectory,glassCss,
  profile:rawProfile,transportKey:key,connectorPort:authority.address().port,databasePath,cipherKey,keyId:'fixture-key',initialize,personalWorkspaces:true,
  listen:{host:'127.0.0.1',port:sourcePort},clock,...(ui?{ui}:{})});
  source.server.prependListener('request',req=>transport.push({path:req.url,host:req.headers.host,expectedHost:new URL(profile.embedOrigin).host,
   origin:req.headers.origin,proof:typeof req.headers['x-soty-selected-proof']==='string',mac:typeof req.headers['x-soty-selected-mac']==='string'}));
  await source.listen();}
 await start(true);
 function browser(){let cookie='',loseNextInvoke=false;
  const fetcher=async(path,options={})=>{const response=await fetch(origin(gateway)+path,{...options,headers:{...options.headers,...(cookie?{cookie}:{})}});
   const set=response.headers.get('set-cookie');if(set)cookie=set.split(';')[0];
   if(path==='/api/embed/invoke'&&loseNextInvoke){loseNextInvoke=false;await response.arrayBuffer();throw Error('fixture_reply_lost_after_commit');}
   return response;};
  return{client:browserSdk.createSourceWorkspaceClient({fetch:fetcher}),request:fetcher,
   get cookie(){return cookie;},set cookie(value){cookie=value;},loseNextInvokeResponse(){loseNextInvoke=true;}};
 }
 return{directory,project,sdkDirectory,databasePath,profile,parentOrigin:origin(parent),embedOrigin:origin(gateway),upstreamOrigin:'http://127.0.0.1:'+sourcePort,browser,frame,
  host:()=>source.host,transport,async restart(){await source.close();frame();await start();},holdNextInvoke(){holdNext=true;},held:()=>held,
  releaseInvoke(){releaseHeld?.();},closeRoot(){rootLive=false;},close};
 }catch(error){await close();throw error;}
}

if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const legacy=process.argv.indexOf('--legacy-script');
 const f=await fixture({...(process.argv.includes('--glance')?{mode:'glance'}:{}),...(legacy>=0?{ui:{script:await readFile(resolve(process.argv[legacy+1]))}}:{})});
 console.log(JSON.stringify({fixture:'isolated-signed-Native-Source-not-live-Root',url:f.parentOrigin,source:f.embedOrigin}));
 const lines=createInterface({input:process.stdin});lines.on('line',line=>{void(async()=>{try{const command=JSON.parse(line).command;
  if(command==='restart')await f.restart();else if(command==='hold')f.holdNextInvoke();else if(command==='release')f.releaseInvoke();
  else if(command==='stop'){await f.close();lines.close();}else throw Error('unknown_fixture_command');
  console.log(JSON.stringify({command,ok:true}));}catch(error){console.log(JSON.stringify({ok:false,error:error.message}));}})();});
 lines.once('close',()=>void f.close());
 process.on('SIGINT',()=>void f.close());process.on('SIGTERM',()=>void f.close());
}
