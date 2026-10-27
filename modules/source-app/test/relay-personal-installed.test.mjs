import test from 'node:test';
import assert from 'node:assert/strict';
import {request as httpsRequest} from 'node:https';
import {mkdtemp,readFile,rm,mkdir,symlink} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,dirname,resolve} from 'node:path';
import {pathToFileURL,fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';
import {createRequire} from 'node:module';
import {digest} from '../server/wire.mjs';
import {buildSourceAppPackage} from '../scripts/build.mjs';
import {createOrdinaryInstalledFixture,freePort} from './support/ordinary-installed.mjs';

const qualifiedSdk=process.env.SOTY_SOURCE_TEST_SDK_DIRECTORY;
test('one relay host implementation serves two domains and two signed users with durable personal data', {timeout:120000},async t=>{
  const directory=await mkdtemp(join(tmpdir(),'soty-relay-personal-tls-'));
  t.after(async()=>{assert.equal(dirname(resolve(directory)),resolve(tmpdir()));await rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:40});});
  const executable=process.env.SOTY_TEST_OPENSSL||(process.platform==='win32'?'C:/Program Files/Git/usr/bin/openssl.exe':'openssl');
  const keyPath=join(directory,'key.pem'),certPath=join(directory,'cert.pem');
  const generated=spawnSync(executable,['req','-x509','-newkey','rsa:2048','-nodes','-keyout',keyPath,'-out',certPath,'-days','1',
    '-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost,DNS:*.apps.localhost,IP:127.0.0.1','-addext','basicConstraints=critical,CA:TRUE'],
    {stdio:'ignore',timeout:15000,windowsHide:true});assert.equal(generated.status,0,'temporary local HTTPS certificate');
  const key=await readFile(keyPath),cert=await readFile(certPath),port=await freePort(),origin='https://127.0.0.1:'+port;
  async function fetchIssuer(url,init={}){
    assert.equal(String(url),origin+'/human-identity/jwks');
    return new Promise((resolve,reject)=>{const req=httpsRequest(url,{ca:cert,agent:false,signal:init.signal},res=>{
      const chunks=[];res.on('data',part=>chunks.push(part));res.on('end',()=>resolve(new Response(Buffer.concat(chunks),{status:res.statusCode,headers:res.headers})));});
      req.on('error',reject);req.end();});
  }
  async function nativeFactory({realm,initialize,directory:fixtureDirectory}){
    const sdk=qualifiedSdk?join(qualifiedSdk,'dist'):join(fixtureDirectory,'relay-sdk');
    if(initialize&&realm.index===0&&!qualifiedSdk){await buildSourceAppPackage({outdir:sdk});await mkdir(join(fixtureDirectory,'node_modules'),{recursive:true});
      const require=createRequire(import.meta.url);await symlink(dirname(dirname(require.resolve('openid-client'))),join(fixtureDirectory,'node_modules','openid-client'),process.platform==='win32'?'junction':'dir');}
    const {createRelayPersonalServer}=await import(pathToFileURL(join(sdk,'server.mjs')).href);
    const domainModule=realm.index===0?pathToFileURL(join(sdk,'templates','reading-list','domain.mjs')):new URL('./fixtures/personal-notes-domain.mjs',import.meta.url);
    const {createDomain}=await import(domainModule.href);
    const original=createDomain(),domain={...original,execute(current,args){
      if(args.input.operation==='fixture.rollback'){current.personal.create(realm.index===0?'reading-items':'notes',args.requestId,{title:'rolled back'});throw Error('fixture rollback');}
      return original.execute(current,args);}};
    return createRelayPersonalServer({appId:realm.appId,issuer:origin+'/human-identity',databasePath:realm.databasePath,
      nonceDatabasePath:join(fixtureDirectory,realm.realmId+'-nonce.sqlite'),domain,ui:'<!doctype html><title>Тестовое приложение</title>',
      listen:{host:'127.0.0.1',port:realm.port},initialize,fetch:fetchIssuer});
  }
  const f=await createOrdinaryInstalledFixture(t,{realmIds:['reading','notes'],withoutRpClients:['reading','notes'],relayOnly:true,
    backendTls:{key,cert},backendPort:port,frontOrigin:origin,extraCa:certPath,nativeFactory,appPort:port,
    appOriginTemplate:`https://{appId}.apps.localhost:${port}`,
    realmOrigins:({appId,port:appPort})=>({embed:`https://${appId}.apps.localhost:${port}`,native:`http://localhost:${appPort}`}),
    connectorScript:fileURLToPath(new URL('../../../scripts/soty-connector.mjs',import.meta.url))});
  await f.grantBrowser(f.foreign.account.accountId);
  async function frame(realm,signer){const launch=await signer.client.extension('apps.launch',{appId:realm.appId});
    const boot=await f.wire(realm.embedded+'/_soty/session',{body:{ticket:new URL(launch.launchUrl).hash.slice(1)}});assert.equal(boot.status,200);
    const value=await signer.client.extension('apps.relay.identity',{appId:realm.appId,domainId:launch.entry.domainId,launchBinding:launch.launchBinding});
    return{realm,grant:value.grant};}
  async function call(frame,path,body){return f.wire(frame.realm.embedded+path,{method:'POST',body,
    headers:{'x-soty-identity-request':'1','x-soty-window-grant':frame.grant}});}
  const definitions=[{realm:f.realms[0],op:'reading',field:'title'},{realm:f.realms[1],op:'notes',field:'text'}],records=[];
  for(const {realm,op,field}of definitions){for(const [label,signer]of [['one',f.reader],['two',f.foreign]]){
    const window=await frame(realm,signer),query={requestId:`relay-${op}-${label}-query`,input:{operation:op+'.list'}};
    assert.deepEqual((await call(window,'/api/query',query)).value.data.items,[]);
    const write={requestId:`relay-${op}-same-request`,input:{operation:op+'.add',[field]:`${op} ${label}`}};
    const first=await call(window,'/api/invoke',write);assert.equal(first.status,200,first.text);assert.equal(first.value.data.outcome,'committed');
    assert.deepEqual((await call(window,'/api/invoke',write)).value,first.value,'retry returns original receipt');
    assert.equal((await call(window,'/api/invoke',{...write,input:{...write.input,[field]:'changed input'}})).status,409);
    assert.deepEqual((await call(window,'/api/query',query)).value.data.items,[{id:write.requestId,[field]:`${op} ${label}`}]);
    let lost;
    if(label==='one'){
      const failed={requestId:`relay-${op}-rollback`,input:{operation:'fixture.rollback'}};
      assert.notEqual((await call(window,'/api/invoke',failed)).status,200);
      const uncertain=await call(window,'/api/receipt',{requestId:failed.requestId,inputDigest:digest(failed.input)});
      assert.equal(uncertain.value.data.outcome,'unknown');
      assert.deepEqual((await call(window,'/api/query',query)).value.data.items,[{id:write.requestId,[field]:`${op} ${label}`}]);
      lost={requestId:`relay-${op}-lost-ack`,input:{operation:op+'.add',[field]:`${op} lost response`}};realm.dropPath='/api/invoke';
      let reply;try{reply=await call(window,'/api/invoke',lost);}catch{}
      assert.notEqual(reply?.status,200,'the committed write response was deliberately lost');
    }
    records.push({realm,op,field,label,signer,write,lost});
  }}
  for(const {realm}of definitions)await realm.restart();await f.restartRoot();
  for(const record of records){const window=await frame(record.realm,record.signer),query={requestId:`relay-${record.op}-${record.label}-reload`,input:{operation:record.op+'.list'}};
    const expected=[...(record.lost?[{id:record.lost.requestId,[record.field]:`${record.op} lost response`}]:[]),{id:record.write.requestId,[record.field]:`${record.op} ${record.label}`}];
    assert.deepEqual((await call(window,'/api/query',query)).value.data.items,expected);
    const receipt=await call(window,'/api/receipt',{requestId:record.write.requestId,inputDigest:digest(record.write.input)});
    assert.equal(receipt.status,200,receipt.text);assert.equal(receipt.value.data.outcome,'committed');
    if(record.lost){const recovered=await call(window,'/api/receipt',{requestId:record.lost.requestId,inputDigest:digest(record.lost.input)});
      assert.equal(recovered.status,200,recovered.text);assert.equal(recovered.value.data.outcome,'committed');
      assert.deepEqual((await call(window,'/api/invoke',record.lost)).value.data,recovered.value.data,'retry after restart returns the original receipt');
      assert.deepEqual((await call(window,'/api/query',query)).value.data.items,expected,'no second write after lost ACK');}}
  // An earlier request can still be arriving when a receipt is read. Pause
  // its body after delivery to the actual upstream, then complete it later.
  const heldWindow=await frame(f.realms[0],f.reader),held={requestId:'relay-held-request-0001',input:{operation:'reading.add',title:'held response'}};
  const encoded=Buffer.from(JSON.stringify(held)),host=new URL(heldWindow.realm.embedded).host;
  let reached;const reachedUpstream=new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('held_request_not_delivered')),5000);
    reached=req=>{if(req.url!=='/api/invoke')return;req.once('data',()=>{clearTimeout(timer);heldWindow.realm.instance.server.off('request',reached);resolve();});};
    heldWindow.realm.instance.server.prependListener('request',reached);});
  const pending=httpsRequest({hostname:'127.0.0.1',port,path:'/api/invoke',method:'POST',ca:cert,agent:false,
    headers:{host,origin:heldWindow.realm.embedded,'content-type':'application/json','content-length':encoded.length,
      'x-soty-identity-request':'1','x-soty-window-grant':heldWindow.grant}});
  const completed=new Promise((resolve,reject)=>{pending.on('response',res=>{const chunks=[];res.on('data',chunk=>chunks.push(chunk));
    res.on('end',()=>resolve({status:res.statusCode,text:Buffer.concat(chunks).toString('utf8')}));});pending.on('error',reject);});
  pending.write(encoded.subarray(0,8));await reachedUpstream;
  const before=await call(heldWindow,'/api/receipt',{requestId:held.requestId,inputDigest:digest(held.input)});
  assert.equal(before.value.data.outcome,'unknown','an absent receipt is not proof that an in-flight request failed');
  const retry=await call(heldWindow,'/api/invoke',held);assert.equal(retry.status,200,retry.text);
  assert.equal(retry.value.data.outcome,'committed','same request and body may commit while its original is still in flight');
  pending.end(encoded.subarray(8));const delivered=await completed;assert.equal(delivered.status,200,delivered.text);
  assert.deepEqual(JSON.parse(delivered.text).data,retry.value.data,'the delayed original returns the same durable receipt');
  const after=await call(heldWindow,'/api/receipt',{requestId:held.requestId,inputDigest:digest(held.input)});
  assert.equal(after.value.data.outcome,'committed');assert.deepEqual((await call(heldWindow,'/api/invoke',held)).value.data,after.value.data);
  const paged=await frame(f.realms[0],f.reader),titles=[];
  for(let i=0;i<50;i++){const requestId=`relay-page-write-${String(i).padStart(4,'0')}`,title='漢'.repeat(240);titles.push({id:requestId,title});
    assert.equal((await call(paged,'/api/invoke',{requestId,input:{operation:'reading.add',title}})).status,200);}
  let cursor=null;const seen=[];do{const response=await call(paged,'/api/query',{requestId:'relay-page-query-0001',input:{operation:'reading.list',...(cursor?{cursor}:{})}});
    assert.equal(response.status,200,response.text);seen.push(...response.value.data.items);cursor=response.value.data.nextCursor;}while(cursor);
  assert.deepEqual(seen.slice(0,50),titles.reverse());assert.equal(seen.length,53);
  assert.equal(seen.filter(item=>item.id===held.requestId).length,1,'overlapping deliveries create only one item');
  for(const {realm,op}of definitions){const reader=await frame(realm,f.reader);
    await f.owner.client.extension('apps.update',{appId:realm.appId,grants:{accountIds:[f.foreign.account.accountId],communityIds:[]}});
    assert.notEqual((await call(reader,'/api/query',{requestId:`relay-${op}-revoked-query`,input:{operation:op+'.list'}})).status,200);
    const other=await frame(realm,f.foreign);assert.equal((await call(other,'/api/query',{requestId:`relay-${op}-remaining-query`,input:{operation:op+'.list'}})).value.data.items.length,1);}
  assert.ok(Object.entries(f.oauthCounts).filter(([name])=>name.startsWith('jwks:200')).reduce((sum,[,count])=>sum+count,0)>0,'verifier obtained the actual Root HTTPS JWKS');
});
