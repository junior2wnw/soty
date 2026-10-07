// Synthetic DEV only. Actual signed Connect/Human and installed connector;
// approved new-empty Native policy, no browser cookie/session injection.
import { createServer as createTlsServer } from 'node:https';
import { execFile } from 'node:child_process';
import { readFile, writeFile, mkdir, mkdtemp, rm } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { tmpdir } from 'node:os';
import { getCACertificates, setDefaultCACertificates } from 'node:tls';
import { createServer as createViteServer } from 'vite';
import { createOrdinaryInstalledFixture, freePort } from '../../modules/source-app/test/support/ordinary-installed.mjs';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const frontPort = Number(process.env.SOTY_ORDINARY_FIXTURE_PORT || 5261), backendPort = await freePort(), appPort = await freePort();
if (!Number.isInteger(frontPort) || frontPort < 1024 || frontPort > 65535) throw new Error('fixture_port_invalid');
const origin = 'https://127.0.0.1:' + frontPort;
let fixture, closeFixture, vite, tls, tlsDirectory, closed = false; const sockets = new Set();
const safeCode = error => /^[A-Za-z0-9_:-]{1,120}$/.test(error?.code || '') ? error.code : 'fixture_failed';
function send(res,status,value) { res.writeHead(status,{'content-type':'application/json','cache-control':'no-store'}).end(JSON.stringify(value)); }
async function input(req) { const parts=[];let size=0;for await(const part of req){size+=part.length;if(size>2048)throw new Error('fixture_input_too_large');parts.push(part);}return JSON.parse(Buffer.concat(parts).toString('utf8')); }
async function helper(req,res) {
  if(!fixture)return send(res,503,{synthetic:true,code:'fixture_starting'});
  if(req.url==='/__fixture/info'&&req.method==='GET')return send(res,200,{synthetic:true,apps:fixture.realms.map(realm=>({id:realm.appId,name:'Synthetic '+realm.realmId,realmId:realm.realmId}))});
  if(req.url==='/__fixture/grant'&&req.method==='POST'){const value=await input(req);if(Object.keys(value).join(',')!=='accountId'||!/^acct_[A-Za-z0-9_-]{20,80}$/.test(value.accountId))return send(res,400,{code:'fixture_account_invalid'});
    await fixture.grantBrowser(value.accountId);return send(res,200,{synthetic:true,rootAppGranted:true,nativePermissionsCreated:false});}
  if(req.url==='/__fixture/restart'&&req.method==='POST'){for(const realm of fixture.realms)await realm.restart();return send(res,200,{synthetic:true,restarted:true});}
  if(req.url==='/__fixture/drop-callback'&&req.method==='POST'){fixture.realms[0].dropNextCallback=true;return send(res,200,{synthetic:true,dropSourceAckOnce:true});}
  if(req.url==='/__fixture/revoke'&&req.method==='POST'){for(const realm of fixture.realms)realm.instance.store.db.prepare('UPDATE native_memberships SET active=0,revision=revision+1').run();return send(res,200,{synthetic:true,revoked:true});}
  if(req.url==='/__fixture/proof'&&req.method==='GET')return send(res,200,{synthetic:true,realms:fixture.realms.map(realm=>({realm:realm.realmId,format:realm.instance.store.format,
    principals:realm.instance.store.db.prepare('SELECT count(*) AS n FROM native_principals').get().n,links:realm.instance.store.db.prepare('SELECT count(*) AS n FROM native_links').get().n,
    consents:realm.instance.store.db.prepare('SELECT count(*) AS n FROM native_consents').get().n,items:realm.instance.store.db.prepare('SELECT count(*) AS n FROM native_items').get().n,
    tickets:realm.instance.store.db.prepare('SELECT count(*) AS n FROM native_tickets').get().n,interactions:realm.instance.store.db.prepare('SELECT count(*) AS n FROM source_interactions').get().n,
    sessions:realm.instance.store.db.prepare('SELECT count(*) AS n FROM source_sessions').get().n,nativeForm:realm.lastNativeForm??null})),sourceCookiesInjected:false,humanCookiesInjected:false,oauthCounts:fixture.oauthCounts});
  return send(res,404,{code:'fixture_route_not_found'});
}
async function cleanup(){if(closed)return;closed=true;sockets.forEach(socket=>socket.destroy());await Promise.allSettled([vite?.close(),tls?new Promise(done=>tls.close(done)):undefined]);await closeFixture?.();if(tlsDirectory)await rm(tlsDirectory,{recursive:true,force:true,maxRetries:5,retryDelay:30});}
for(const name of ['SIGINT','SIGTERM'])process.on(name,()=>void cleanup().then(()=>process.exit(0)));
for(const name of ['uncaughtException','unhandledRejection'])process.once(name,error=>{console.error(safeCode(error));void cleanup().then(()=>process.exit(1));});
try {
  await mkdir(join(root,'output','playwright'),{recursive:true});
  tlsDirectory=await mkdtemp(join(tmpdir(),'soty-ordinary-browser-tls-'));const keyFile=join(tlsDirectory,'tls.key'),certFile=join(tlsDirectory,'tls.crt');
  const openssl=process.platform==='win32'?'C:/Program Files/Git/usr/bin/openssl.exe':'openssl';
  await promisify(execFile)(openssl,['req','-x509','-newkey','rsa:2048','-nodes','-keyout',keyFile,'-out',certFile,'-days','1','-subj','/CN=localhost','-addext','subjectAltName=DNS:*.localhost,DNS:localhost,IP:127.0.0.1'],{windowsHide:true,maxBuffer:16384});
  const key=await readFile(keyFile),cert=await readFile(certFile);
  // Trust only this ephemeral certificate in this synthetic Node process and
  // its child; never disable TLS verification or install OS trust globally.
  setDefaultCACertificates([...getCACertificates('default'),cert.toString('utf8')]);
  // This test's HTTP/1 reverse-proxy matches the production backend transport;
  // only ephemeral local TLS is accepted, not a production certificate claim.
  vite=await createViteServer({root,configFile:join(root,'vite.config.ts'),server:{host:'127.0.0.1',port:frontPort,strictPort:true,https:{key,cert,
    ALPNCallback:({protocols})=>protocols.includes('http/1.1')?'http/1.1':undefined},proxy:{
    '/api':{target:'https://127.0.0.1:'+backendPort,ws:true},'/human-identity':{target:'https://127.0.0.1:'+backendPort,changeOrigin:false}}},plugins:[{name:'ordinary-source-test-only',configureServer(server){server.middlewares.use((req,res,next)=>{
      if(!req.url?.startsWith('/__fixture/'))return next();if(req.headers.host!=='127.0.0.1:'+frontPort||req.headers.origin&&req.headers.origin!==origin||req.method==='POST'&&req.headers.origin!==origin)return send(res,403,{code:'fixture_origin_denied',safeRequest:{method:req.method,host:req.headers.host??'absent',origin:req.headers.origin??'absent'}});
      void helper(req,res).catch(error=>send(res,400,{code:safeCode(error)}));});}}]});await vite.listen();
  fixture=await createOrdinaryInstalledFixture({after:callback=>{closeFixture=callback;}},{frontPort,frontOrigin:origin,backendPort,backendTls:{key,cert},appPort,distDir:root,emptyGuestBrowser:true,extraCa:certFile});
  const hosts=new Set(fixture.realms.map(realm=>new URL(realm.embedded).host));
  tls=createTlsServer({key,cert},(req,res)=>{if(!hosts.has(req.headers.host))return res.writeHead(403).end();fixture.root()(req,res);});
  tls.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});await new Promise(done=>tls.listen(appPort,'127.0.0.1',done));
  const browserConfig=join(fixture.directory,'playwright-config.json');await writeFile(browserConfig,JSON.stringify({browser:{browserName:'chromium',launchOptions:{channel:'chrome'},contextOptions:{ignoreHTTPSErrors:true}}}));
  console.log(JSON.stringify({synthetic:true,state:'ready',url:origin+'/src/world/app-source-ordinary.test.html',browserConfig,certificate:'ephemeral-local-only',productionHttpsValidated:false,credentialsLogged:false}));
} catch(error){await cleanup();console.error(safeCode(error));process.exitCode=1;}
