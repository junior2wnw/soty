import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,readFile,writeFile,mkdir,rm,symlink} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createServer} from 'node:net';
import {createRelayStarter} from '../install/relay-starter.mjs';
import {createRelayPersonalServer} from '../server/relay-personal-server.mjs';
import {createDomain} from '../examples/reading-list/domain.mjs';

async function directory(t){
  const path=await mkdtemp(join(tmpdir(),'soty-relay-starter-'));
  t.after(()=>rm(path,{recursive:true,force:true,maxRetries:3}));return path;
}
async function freePort(){
  const server=createServer();await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const port=server.address().port;await new Promise(resolve=>server.close(resolve));return port;
}
test('the shared-login starter preserves an installed SDK and produces an app without its own login',async t=>{
  const path=await directory(t);await mkdir(join(path,'.soty-sdk'));
  await writeFile(join(path,'.soty-sdk','keep'),'installed-sdk');
  const result=await createRelayStarter({directory:path,title:'Новая аппка',port:30123});
  assert.equal(result.authentication,'soty-window');assert.equal(result.connected,false);
  assert.equal(await readFile(join(path,'.soty-sdk','keep'),'utf8'),'installed-sdk');
  assert.deepEqual(JSON.parse(await readFile(join(path,'.soty','app.json'),'utf8')),
    {schema:'soty.local-app.v1',name:'Новая аппка',port:30123,entryPath:'/'});
  assert.deepEqual(JSON.parse(await readFile(join(path,'.soty','app-auth.json'),'utf8')),
    {schema:'soty.app-auth-configuration.v1',appId:null,issuer:null});
  const server=await readFile(join(path,'app','server.mjs'),'utf8'),ui=await readFile(join(path,'app','ui.html'),'utf8');
  assert.match(server,/createRelayPersonalServer/);assert.match(ui,/createRelayIdentityFetch/);
  assert.doesNotMatch(ui,/(?:type\s*=\s*["']password|\/api\/(?:login|signup)|target\s*=\s*["']_blank)/iu);
  await assert.rejects(createRelayStarter({directory:path,title:'Overwrite'}),{code:'relay_starter_exists'});
  assert.equal(JSON.parse(await readFile(join(path,'.soty','app.json'),'utf8')).name,'Новая аппка');
});
test('the shared-login starter refuses a symlink output and invalid ports before writing',async t=>{
  const path=await directory(t),elsewhere=await directory(t);
  await symlink(elsewhere,join(path,'app'),process.platform==='win32'?'junction':'dir');
  await assert.rejects(createRelayStarter({directory:path,title:'Cannot overwrite'}),{code:'relay_starter_exists'});
  await assert.rejects(createRelayStarter({directory:path,title:'X',port:49424}),{code:'relay_starter_port_invalid'});
  await assert.rejects(readFile(join(path,'.soty','app.json')),{code:'ENOENT'});
});
test('the reusable relay host serves the exact browser SDK and denies anonymous data',async t=>{
  let app;t.after(()=>app?.close());
  const path=await directory(t),port=await freePort(),sdk='export const sharedWindowLogin=true;';
  app=createRelayPersonalServer({appId:'app-'+'1'.repeat(32),issuer:'https://soty.fixture.invalid/human-identity',
    databasePath:join(path,'app.sqlite'),nonceDatabasePath:join(path,'nonces.sqlite'),initialize:true,
    domain:createDomain(),ui:'<!doctype html><p>Shared login</p>',browserSdk:sdk,listen:{host:'127.0.0.1',port}});
  await app.listen();const origin='http://127.0.0.1:'+port;
  const asset=await fetch(origin+'/soty-sdk.mjs');assert.equal(asset.status,200);
  assert.match(asset.headers.get('content-type'),/^text\/javascript/);assert.equal(await asset.text(),sdk);
  const anonymous=await fetch(origin+'/api/query',{method:'POST',headers:{'content-type':'application/json'},
    body:JSON.stringify({requestId:'probe-anonymous-identity',input:{operation:'reading.list'}})});
  const result=await anonymous.json();assert.equal(anonymous.status,401,JSON.stringify(result));assert.equal(result.error.code,'soty_identity_invalid');
  assert.equal((await fetch(origin+'/api/login',{method:'POST'})).status,404);
});
