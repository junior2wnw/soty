import test from 'node:test';
import assert from 'node:assert/strict';
import {execFileSync,spawn} from 'node:child_process';
import {createServer} from 'node:http';
import {mkdtemp,readFile,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,dirname,resolve,parse} from 'node:path';
import {once} from 'node:events';
import {fileURLToPath} from 'node:url';
import {createUiRelease,writeUiRelease,createCaddyOverlay} from './release.mjs';
import {fixtureDist} from './test-support.mjs';
import {verifyPublicSourceSdk,sourceSdkHash} from '../../modules/source-app/public-release.mjs';

const binary=process.env.SOTY_UI_CADDY_BINARY;
assert.ok(binary,'explicit qualified Caddy binary required');
assert.match(execFileSync(binary,['version'],{encoding:'utf8'}),/^v2\.11\.2\b/u);
const repository=fileURLToPath(new URL('../../',import.meta.url));
const adapt=text=>JSON.parse(execFileSync(binary,['adapt','--config','-','--adapter','caddyfile'],{input:text,maxBuffer:16777216,stdio:['pipe','pipe','pipe']}));

test('frozen public Source SDK passes the real finite Caddy overlay; private neighbours and non-GET methods remain closed',{timeout:20000},async t=>{
  verifyPublicSourceSdk(repository);
  const root=await mkdtemp(join(tmpdir(),'source-sdk-caddy-'));let backend,caddy;
  t.after(async()=>{
    if(caddy?.exitCode===null){const stopped=once(caddy,'exit');caddy.kill('SIGTERM');await stopped;}
    if(backend?.listening){backend.closeAllConnections();await new Promise(resolve=>backend.close(resolve));}
    assert.equal(dirname(resolve(root)),resolve(tmpdir()));await rm(root,{recursive:true,force:true,maxRetries:5,retryDelay:50});
  });
  const indexBytes=await readFile(join(repository,'public/sdk/source-app/index.json')),index=JSON.parse(indexBytes),name=index.artifact.url.slice(1);
  const archive=await readFile(join(repository,'public',name));assert.equal(sourceSdkHash(archive),index.artifact.sha256);
  const dist=fixtureDist(join(root,'dist'),{'sdk/source-app/index.json':indexBytes,[name]:archive});
  const release=createUiRelease(dist),immutableRoot=join(root,release.id);writeUiRelease(release,immutableRoot);
  backend=createServer((req,res)=>{
    res.setHeader('Content-Security-Policy',"default-src 'self'");res.setHeader('Content-Type','text/html');
    if(req.url.startsWith('/api/')){res.writeHead(403);res.end('original denied');return;}res.end('OLD UI');
  });
  // This exact upstream is deliberately part of the maintained overlay guard.
  // A busy port fails before any Caddy process starts; no existing server is used.
  const started=once(backend,'listening');backend.listen(18182,'127.0.0.1');await started;
  const reserve=createServer();const reserving=once(reserve,'listening');reserve.listen(0,'127.0.0.1');await reserving;
  const port=reserve.address().port;await new Promise(resolve=>reserve.close(resolve));
  const configuration=`{\n admin off\n auto_https off\n}\nhttp://127.0.0.1:${port} {\n @shell path / /assets/* /api/* /sw.js /manifest.webmanifest /icon.svg\n handle @shell {\n encode gzip\n handle {\n reverse_proxy 127.0.0.1:18182 {\n flush_interval -1\n transport http {\n dial_timeout 5s\n }\n }\n }\n }\n handle {\n respond "outside original" 404\n }\n}`;
  // The production guard accepts literal POSIX roots. On Windows run this
  // disposable Caddy on the temp directory's drive, where /Users/... refers
  // to that same directory, without broadening the production root validator.
  const caddyRoot=process.platform==='win32'?immutableRoot.slice(2).replaceAll('\\','/'):immutableRoot;
  const overlay=await createCaddyOverlay({caddyfile:configuration,release,root:caddyRoot,adapt,host:'127.0.0.1'});
  const config=join(root,'Caddyfile');await writeFile(config,overlay.caddyfile);
  caddy=spawn(binary,['run','--config',config,'--adapter','caddyfile'],{cwd:parse(immutableRoot).root,windowsHide:true,stdio:['ignore','ignore','pipe'],
    env:{...process.env,XDG_CONFIG_HOME:join(root,'config'),XDG_DATA_HOME:join(root,'data')}});caddy.stderr.resume();
  const base='http://127.0.0.1:'+port;
  for(let attempt=0;;attempt++){
    try{if((await fetch(base)).ok)break;}catch{}
    assert.ok(attempt<100&&caddy.exitCode===null,'owned synthetic Caddy starts');await new Promise(resolve=>setTimeout(resolve,20));
  }
  let response=await fetch(base+'/sdk/source-app/index.json');assert.equal(response.status,200);assert.equal(response.headers.get('cache-control'),'no-store');
  assert.deepEqual(Buffer.from(await response.arrayBuffer()),indexBytes);assert.equal(response.headers.get('content-security-policy'),"default-src 'self'");
  response=await fetch(base+index.artifact.url);assert.equal(response.status,200);assert.deepEqual(Buffer.from(await response.arrayBuffer()),archive);
  assert.equal(response.headers.get('cache-control'),'public, max-age=31536000, immutable');const etag=response.headers.get('etag');assert.ok(etag);
  response=await fetch(base+index.artifact.url,{headers:{'If-None-Match':etag}});assert.equal(response.status,304);
  response=await fetch(base+index.artifact.url,{method:'HEAD'});assert.equal(response.status,200);assert.equal(await response.text(),'');
  response=await fetch(base+index.artifact.url,{headers:{Range:'bytes=0-15'}});assert.equal(response.status,206);assert.deepEqual(Buffer.from(await response.arrayBuffer()),archive.subarray(0,16));
  for(const path of ['/sdk/source-app/operator.json','/sdk/source-app/private.key','/sdk/source-app/index.json/extra','/sdk/other/index.json']){
    response=await fetch(base+path);assert.equal(response.status,404);assert.equal(await response.text(),'outside original');
  }
  for(const method of ['POST','PUT','OPTIONS']){response=await fetch(base+index.artifact.url,{method});assert.equal(response.status,404);}
  response=await fetch(base+'/api/anything');assert.equal(response.status,403);assert.equal(await response.text(),'original denied');
  response=await fetch(base+index.artifact.url);assert.equal(sourceSdkHash(Buffer.from(await response.arrayBuffer())),index.artifact.sha256);
});
