import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdir,writeFile,symlink} from 'node:fs/promises';
import {join,dirname} from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {execFileSync} from 'node:child_process';
import {createRequire} from 'node:module';
import {DatabaseSync} from 'node:sqlite';
import {STANDARD_ROOT_WORKSPACE_SOURCE} from '../server/index.mjs';
import {buildSourceAppPackage} from '../scripts/build.mjs';
import {createOrdinaryInstalledFixture} from './support/ordinary-installed.mjs';

// An opt-in qualification can reuse the freshly extracted, npm-ci installed
// public packet. The ordinary offline suite still builds in its own temp tree.
const qualifiedSdk=process.env.SOTY_SOURCE_TEST_SDK_DIRECTORY;
const {createSourceWorkspaceClient}=await import(qualifiedSdk?pathToFileURL(join(qualifiedSdk,'dist/browser.mjs')).href:'../browser/index.mjs');

// The consumer contributes only its data module and these fixed public files.
// Root Apps/Human, installed connector, Source broker, Native SQLite and shared
// browser client are all actual maintained implementations, not authority DTOs.
async function nativeFactory({realm,initialize,directory}) {
  const folder=join(directory,'author-'+realm.realmId),configPath=join(folder,'operator.json');
  const sdkDirectory=qualifiedSdk?join(qualifiedSdk,'dist'):join(directory,'portable-sdk');
  if(initialize&&!qualifiedSdk){await buildSourceAppPackage({outdir:sdkDirectory});await mkdir(join(directory,'node_modules'),{recursive:true});
    const require=createRequire(import.meta.url);
    await symlink(dirname(dirname(require.resolve('openid-client'))),join(directory,'node_modules','openid-client'),process.platform==='win32'?'junction':'dir');}
  const {loadSourceOperatorConfiguration,disposeSourceOperatorConfiguration,startInstalledSource,diagnoseInstalledSource}
    =await import(pathToFileURL(join(sdkDirectory,'install.mjs')).href);
  const cli=(...args)=>JSON.parse(execFileSync(process.execPath,[join(sdkDirectory,'install.mjs'),...args],{cwd:folder,windowsHide:true,encoding:'utf8'}));
  if(initialize){
    await mkdir(folder,{mode:0o700});await mkdir(join(folder,'secrets'),{mode:0o700});await mkdir(join(folder,'data'),{mode:0o700});
    const author=cli('author','--title','Мой список','--template','reading-list');assert.equal(author.connected,false);assert.equal(author.template,'reading-list');
    const config={schema:'soty.root-workspace.operator.v1',profile:realm.profile,connectorPort:realm.options.connectorPort,
      listener:{host:'127.0.0.1',port:realm.port},storage:{directory:join(folder,'data'),keyId:'reading-list',cipherKeyFile:'cipher.key'},
      transportKeyFile:'transport.key',native:{resourceTitle:'Мой список',domainModule:join(folder,'source','domain.mjs'),uiDirectory:join(folder,'source','assets')},
      policy:{personalWorkspaces:true}};
    await writeFile(configPath,JSON.stringify(config),{mode:0o600});
    await writeFile(join(folder,'secrets','cipher.key'),realm.cipherKey.toString('base64url')+'\n',{mode:0o600});
    await writeFile(join(folder,'secrets','transport.key'),realm.key.toString('base64url')+'\n',{mode:0o600});
    const checked=cli('check','--config',configPath);assert.equal(checked.authentication,'RootSSO300');assert.equal(checked.connected,false);
    assert.equal(checked.nativeReachability,'source-only');assert.equal(checked.nativeOrigin,realm.embedded);
    const installed=cli('init','--config',configPath);assert.equal(installed.nativePrincipalsCreated,0);assert.equal(installed.nativeReader,'soty.sqlite-root-workspace.v1');
    const doctor=cli('doctor','--config',configPath);assert.equal(doctor.integrity,'missing');assert.equal(doctor.currentAuthority,'not_checked');assert.equal(doctor.releaseReady,false);
  }
  realm.databasePath=join(folder,'data','native.sqlite');
  const handle=await loadSourceOperatorConfiguration(configPath),app=await startInstalledSource(handle);
  // Fixture controls listen timing; the production start path already opened it.
  return Object.freeze({...app,listen:async()=>realm.port,doctor:()=>diagnoseInstalledSource(handle),
    async close(){try{await app.close();}finally{disposeSourceOperatorConfiguration(handle);}}});
}

test('same-origin Source-only reading-list quickstart: installed CLI and actual Root connect, private data, restart receipt and revoke', {timeout:120000},async t=>{
  const f=await createOrdinaryInstalledFixture(t,{workspace:true,sourceOnly:true,realmIds:['reading'],withoutRpClients:['reading'],
    sourceProfile:STANDARD_ROOT_WORKSPACE_SOURCE,nativeFactory,
    connectorScript:fileURLToPath(new URL('../../../scripts/soty-connector.mjs',import.meta.url))});
  const realm=f.realms[0],clients=[],paths=[];
  t.after(()=>clients.forEach(client=>client.dispose()));
  async function frame(signer=f.reader){
    const launch=await signer.client.extension('apps.launch',{appId:realm.appId});
    const boot=await f.wire(realm.embedded+'/_soty/session',{body:{ticket:new URL(launch.launchUrl).hash.slice(1)}});
    assert.equal(boot.status,200);
    const client=createSourceWorkspaceClient({href:realm.embedded+'/embed?sotyFrame='+boot.value.frame,
      fetch:async(path,init)=>{paths.push(path);const response=await f.wire(realm.embedded+path,{method:init.method,body:JSON.parse(init.body),headers:Object.fromEntries(new Headers(init.headers))});
        return new Response(response.text,{status:response.status,headers:response.headers});}});
    clients.push(client);return{client,launch,frame:boot.value.frame};
  }
  const first=await frame();await first.client.connect({requestId:'reading-installed-connect-001'});
  assert.deepEqual(paths,['/api/embed/session-continue','/api/embed/query']);
  for(const path of ['/embed','/assets/ui.js','/assets/ui.css','/assets/source-sdk.js']){
    const response=await f.wire(realm.embedded+path,{headers:{'x-soty-frame':first.frame}});assert.equal(response.status,200,path+' '+response.text.slice(0,100));
  }
  for(const path of ['/assets/domain.mjs','/assets/operator.json','/assets/../native.sqlite','/.soty/author.json'])
    assert.notEqual((await f.wire(realm.embedded+path,{headers:{'x-soty-frame':first.frame}})).status,200,path);
  assert.deepEqual((await first.client.query({requestId:'reading-installed-empty-001',input:{operation:'reading.list'}})).items,[]);
  const intent=await first.client.prepareInvoke({requestId:'reading-installed-write-001',input:{operation:'reading.add',title:'Private reading item'}});
  realm.dropPath='/api/embed/invoke';await assert.rejects(first.client.invoke(intent),error=>error.effectUnknown===true);
  first.client.dispose();await realm.restart();
  await f.reader.client.extension('apps.scoped.close',{appId:realm.appId,handle:first.launch.scopedCloseHandle});
  const second=await frame();await second.client.connect({requestId:'reading-installed-reconnect-001'});
  assert.notEqual(second.client.context().context.ref,intent.contextRef);
  const writes=paths.filter(path=>path==='/api/embed/invoke').length;
  const receipt=await second.client.recoverReceipt({requestId:intent.requestId,inputDigest:intent.inputDigest,contextRef:intent.contextRef});
  assert.equal(receipt.outcome,'committed');assert.equal(receipt.recovery.contextRef,intent.contextRef);
  const doctor=await realm.instance.doctor();assert.equal(doctor.integrity,'verified');assert.equal(doctor.lastObservation.status,'receipt_verified');
  assert.equal(doctor.currentAuthority,'not_checked');assert.equal(doctor.clientAcknowledgement,'not_observed');assert.equal(doctor.releaseReady,false);
  assert.equal(JSON.stringify(doctor).includes('Private reading item'),false);assert.equal(JSON.stringify(doctor).includes('subject'),false);
  assert.deepEqual((await second.client.query({requestId:'reading-installed-reload-001',input:{operation:'reading.list'}})).items,[{id:intent.requestId,title:'Private reading item'}]);
  assert.equal(paths.filter(path=>path==='/api/embed/invoke').length,writes,'recovery never invokes again');
  await f.grantBrowser(f.foreign.account.accountId);const foreign=await frame(f.foreign);
  await foreign.client.connect({requestId:'reading-installed-foreign-001'});
  assert.deepEqual((await foreign.client.query({requestId:'reading-installed-isolation-001',input:{operation:'reading.list'}})).items,[]);
  await assert.rejects(foreign.client.recoverReceipt(intent),error=>error.effectUnknown===true);
  assert.equal(Object.entries(f.oauthCounts).filter(([name])=>/^(token|userinfo):/u.test(name)).reduce((sum,[,count])=>sum+count,0),0);
  await f.owner.client.extension('apps.update',{appId:realm.appId,grants:{accountIds:[],communityIds:[]}});
  await assert.rejects(second.client.query({requestId:'reading-installed-revoked-001',input:{operation:'reading.list'}}));
  const db=new DatabaseSync(realm.databasePath);try{assert.equal(db.prepare('SELECT count(*) n FROM reading_items').get().n,1);}finally{db.close();}
});
