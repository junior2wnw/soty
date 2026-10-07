import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,readFile,rm,symlink} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomBytes} from 'node:crypto';
import {createServer} from 'node:net';
import {request as httpRequest} from 'node:http';
import {STANDARD_SELECTED_SOURCE_V2} from '../server/standard-profile.mjs';
import {loadSourceOperatorConfiguration,sourceOperatorStatus,disposeSourceOperatorConfiguration,withSourceOperatorConfiguration} from '../install/config.mjs';
import {initializeInstalledSource,startInstalledSource,inspectInstalledSource} from '../install/runtime.mjs';
import {renderSourceNativePortal} from '../install/native-portal.mjs';
import {createSourceAuthorDraft} from '../install/author.mjs';
import {createOrdinaryAppStore} from '../examples/ordinary-app/store.mjs';

async function freePort(){const s=createServer();await new Promise(resolve=>s.listen(0,'127.0.0.1',resolve));const port=s.address().port;await new Promise(resolve=>s.close(resolve));return port;}
async function fixture(t){
  const directory=await mkdtemp(join(tmpdir(),'soty-source-install-')),appId='app-'+'a'.repeat(32),port=await freePort();
  const cleanups=[];t.after(async()=>{for(const close of cleanups.reverse())await close();await rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:30});});
  await mkdir(join(directory,'secrets'),{mode:0o700});await mkdir(join(directory,'data'),{mode:0o700});
  for(const name of ['cipher.key','transport.key','client.key'])await writeFile(join(directory,'secrets',name),randomBytes(32).toString('base64url')+'\n',{mode:0o600});
  const profile={schema:'soty.selected-human-embed.v2',appId,connector:{linkId:'synthetic-install-link',hostDeviceId:'synthetic-host',connectorId:'synthetic-connector'},
    target:{revision:2,digest:'a'.repeat(64)},sourceProfile:STANDARD_SELECTED_SOURCE_V2,
    resource:{registryId:'soty',environmentId:'production',tenantId:'synthetic-root',appId,resourceId:'synthetic-resource',selection:{kind:'soty.resource.v1',nativeId:'selected',incarnationId:'one'}},
    issuer:'https://soty.example.test/human-identity',clientId:'synthetic-source',embedOrigin:'https://'+appId+'.soty.example.test',nativeOrigin:'https://native.example.test',parentOrigin:'https://soty.example.test'};
  const config={schema:'soty.ordinary-source.operator.v1',profile,connectorPort:await freePort(),listener:{host:'127.0.0.1',port},
    storage:{directory:join(directory,'data'),keyId:'synthetic',cipherKeyFile:'cipher.key'},rp:{clientSecretFile:'client.key',transportKeyFile:'transport.key'},
    native:{realmId:'synthetic',resourceTitle:'Synthetic Native resource'},policy:{newEmptyGuest:false,linkedLogin:false}};
  const path=join(directory,'operator.json');const save=cfg=>writeFile(path,JSON.stringify(cfg),{mode:0o600});await save(config);
  return{directory,path,config,save,cleanup:fn=>cleanups.push(fn)};
}

test('private install config exposes only an opaque handle/safe status; author manifest/body identity/key/URL cannot provide it',async t=>{
  const f=await fixture(t),handle=await loadSourceOperatorConfiguration(f.path);f.cleanup(()=>disposeSourceOperatorConfiguration(handle));
  assert.equal(JSON.stringify(handle),'{}');const status=sourceOperatorStatus(handle);assert.equal(status.authentication,'Basic300');assert.equal(status.connected,false);assert.equal(status.jobsReady,false);assert.equal(status.longReady,false);
  assert.equal(status.nativeReachability,'public-https-unverified');assert.equal(status.sourceNativePolicy.newEmptyGuest,false);
  assert.equal(JSON.stringify(status).includes('clientSecret'),false);assert.throws(()=>sourceOperatorStatus(structuredClone(handle)));
  for(const extra of [{owner:true},{grants:{all:true}},{issuer:'https://caller.test'},{clientSecret:'body-value'}]){
    await f.save({...f.config,...extra});await assert.rejects(loadSourceOperatorConfiguration(f.path));}
  await f.save({schema:'soty.app-author-draft.v1',title:'Public draft'});await assert.rejects(loadSourceOperatorConfiguration(f.path));
});

test('exact public Native HTTPS origin is separate from fixed private listener/Root embed; arbitrary host/path/secret traversal is denied',async t=>{
  const f=await fixture(t);
  for(const patch of [{listener:{host:'0.0.0.0',port:f.config.listener.port}},{profile:{...f.config.profile,nativeOrigin:f.config.profile.embedOrigin}},
    {profile:{...f.config.profile,nativeOrigin:'http://publisher.example.test:5123'}},{rp:{...f.config.rp,clientSecretFile:'../outside.key'}}]){
    await f.save({...f.config,...patch});await assert.rejects(loadSourceOperatorConfiguration(f.path));}
  await f.save(f.config);const h=await loadSourceOperatorConfiguration(f.path);f.cleanup(()=>disposeSourceOperatorConfiguration(h));
  const portal=renderSourceNativePortal(h);assert.match(portal,/method GET\s+path \/soty\/connect/u);assert.match(portal,/method POST\s+path \/soty\/authorize/u);
  assert.match(portal,/header_up Host native\.example\.test/u);assert.match(portal,/respond 404/u);assert.equal(portal.includes('x-root-account'),false);
  assert.equal(portal.includes('/api/embed'),false);assert.equal(portal.includes('source.key'),false);
});

test('actual installed ordinary Source starts on internal port for public HTTPS Native host; init creates no principal/grant, spoofed Root header stays denied',{timeout:15000},async t=>{
  const f=await fixture(t),h=await loadSourceOperatorConfiguration(f.path);f.cleanup(()=>disposeSourceOperatorConfiguration(h));
  const start=performance.now(),stage=phase=>t.diagnostic(JSON.stringify({phase,elapsedMs:Math.ceil(performance.now()-start)}));stage('config');
  const initialized=await initializeInstalledSource(h);assert.equal(initialized.nativeReader,3);assert.equal(initialized.nativePrincipalsCreated,0);assert.equal(initialized.nativeGrantsCreated,0);
  stage('initialized');
  await assert.rejects(initializeInstalledSource(h),error=>error.code==='source_install_already_initialized');
  assert.equal(inspectInstalledSource(h).reader.objects,29);
  const app=await startInstalledSource(h);f.cleanup(()=>app.close());
  stage('listening');
  assert.equal(app.server.address().port,f.config.listener.port);
  assert.equal(app.store.db.prepare('SELECT count(*) n FROM native_principals').get().n,0);assert.equal(app.store.db.prepare('SELECT guest_empty FROM native_resources').get().guest_empty,0);
  const status=await new Promise((resolve,reject)=>{const request=httpRequest({hostname:'127.0.0.1',port:f.config.listener.port,path:'/api/embed/context',agent:false,
    headers:{host:new URL(f.config.profile.embedOrigin).host,'x-root-account-id':'synthetic-root','x-root-owner':'true'},signal:AbortSignal.timeout(3000)},res=>{res.resume();res.on('end',()=>resolve(res.statusCode));});
    request.on('error',reject);request.end();});
  assert.ok([401,403].includes(status),'missing authenticated Root proof is denied');
  stage('denied');
});

test('title-only folder draft autodetects framework without scripts/auth/grants and preserves existing publication files',async t=>{
  const f=await fixture(t),folder=join(f.directory,'author');await mkdir(folder);await mkdir(join(folder,'.soty'));
  await writeFile(join(folder,'.soty','app.json'),'preserve-existing-deployment');
  await writeFile(join(folder,'package.json'),JSON.stringify({dependencies:{vite:'1.0.0'},scripts:{postinstall:'must-not-run'},privateCredential:'must-not-be-copied'}));
  const result=await createSourceAuthorDraft({directory:folder,title:'Мой новый проект'});assert.equal(result.framework,'vite');assert.equal(result.metadataOnly,true);assert.equal(result.connected,false);
  const draft=JSON.parse(await readFile(join(folder,'.soty','author.json'),'utf8'));assert.deepEqual(Object.keys(draft).sort(),['schema','title']);assert.equal(draft.title,'Мой новый проект');
  assert.equal(await readFile(join(folder,'.soty','app.json'),'utf8'),'preserve-existing-deployment');await assert.rejects(createSourceAuthorDraft({directory:folder,title:'Other title'}),error=>error.code==='EEXIST');
});
test('exclusive Source installation cannot overwrite an existing resource when two initializers race',async t=>{
  const f=await fixture(t),h=await loadSourceOperatorConfiguration(f.path);f.cleanup(()=>disposeSourceOperatorConfiguration(h));
  const outcomes=await Promise.allSettled([initializeInstalledSource(h),initializeInstalledSource(h)]);
  assert.equal(outcomes.filter(value=>value.status==='fulfilled').length,1);
  const denied=outcomes.find(value=>value.status==='rejected');assert.equal(denied.reason.code,'source_install_already_initialized');
  const app=await startInstalledSource(h);f.cleanup(()=>app.close());
  assert.equal(app.store.db.prepare('SELECT count(*) n FROM native_resources').get().n,1);
  assert.equal(app.store.db.prepare('SELECT count(*) n FROM native_principals').get().n,0);
});
