import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {createHmac,generateKeyPairSync,randomBytes} from 'node:crypto';
import {mkdtemp,rm,mkdir,symlink} from 'node:fs/promises';
import {join,dirname,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {createRequire} from 'node:module';
import {pathToFileURL} from 'node:url';
import {DatabaseSync} from 'node:sqlite';
import {createSourceAppBff,STANDARD_ROOT_WORKSPACE_SOURCE} from '../server/index.mjs';
import {createSqliteRootWorkspaceHost} from '../server/sqlite-root-workspace-host.mjs';
import {createSourceSessionCipher} from '../server/session-cipher.mjs';
import {selectedResourceProfile} from '../../apps/scoped-embed/resource-profile.mjs';
import {createResourceSourceProofSigner} from '../../apps/scoped-embed/resource-proof.mjs';
import {digest,fields,check} from '../server/wire.mjs';
import {buildSourceAppPackage} from '../scripts/build.mjs';

const require=createRequire(import.meta.url),oidcRequire=createRequire(require.resolve('openid-client'));
const {SignJWT,importJWK}=await import(pathToFileURL(oidcRequire.resolve('jose')).href);
const random=()=>randomBytes(32).toString('base64url');

// Consumer code has no signing, Source cookie, nonce, Root proof or checkpoint
// implementation. Its SQL always selects the SDK-verified personal principal.
function listDomain({afterWrite}={}) {return {
  initialize(db){db.exec('CREATE TABLE reading_items(person TEXT NOT NULL,id TEXT NOT NULL,title TEXT NOT NULL,PRIMARY KEY(person,id))');},
  inspect(db){assert.deepEqual(db.prepare('PRAGMA table_info(reading_items)').all().map(row=>row.name),['person','id','title']);},
  read({database,principalId},{input}){
    fields(input,['operation']);check(input.operation==='reading.list');
    return{items:database.prepare('SELECT id,title FROM reading_items WHERE person=? ORDER BY id').all(principalId)};
  },
  execute({database,principalId},{requestId,input}){
    fields(input,['operation','title']);check(input.operation==='reading.add'&&typeof input.title==='string'&&input.title.length<=240);
    database.prepare('INSERT INTO reading_items VALUES(?,?,?)').run(principalId,requestId,input.title);afterWrite?.(database);
    return{result:{id:requestId,title:input.title},workspaceReceipt:{schema:'soty.workspace-receipt.v1',
      effects:[{kind:'created',resourceType:'reading-item',resourceId:requestId,revision:1}],
      receipt:{verificationMethod:'domain_read',artifacts:[{type:'reading-item',id:requestId,revision:1}]}}};
  },
};}

async function fixture(t,{personalWorkspaces=true,domain=listDomain(),onVerifiedOperation,portable=false}={}) {
  const directory=await mkdtemp(join(tmpdir(),'source-personal-sqlite-'));
  let createHost=createSqliteRootWorkspaceHost,createBff=createSourceAppBff;
  if(portable){await buildSourceAppPackage({outdir:join(directory,'sdk')});await mkdir(join(directory,'node_modules'));
    await symlink(dirname(dirname(require.resolve('openid-client'))),join(directory,'node_modules','openid-client'),process.platform==='win32'?'junction':'dir');
    const module=await import(pathToFileURL(join(directory,'sdk','server.mjs')).href);createHost=module.createSqliteRootWorkspaceHost;createBff=module.createSourceAppBff;}
  let now=Date.now(),context,profile,bff,host,rootLive=true,rootHook;
  const clock=()=>now,key=randomBytes(32),cipherKey=randomBytes(32),pair=generateKeyPairSync('rsa',{modulusLength:2048});
  const publicJwk={...pair.publicKey.export({format:'jwk'}),kid:'fixture',alg:'RS256',use:'sig'};
  const signing=await importJWK({...pair.privateKey.export({format:'jwk'}),kid:'fixture',alg:'RS256',use:'sig'},'RS256');
  const issuerServer=createServer((_req,res)=>res.writeHead(200,{'content-type':'application/json'}).end(JSON.stringify({keys:[publicJwk]})));
  const authority=createServer(async(req,res)=>{try{
    const chunks=[];for await(const chunk of req)chunks.push(chunk);const input=JSON.parse(Buffer.concat(chunks));rootHook?.();
    if(!rootLive){res.writeHead(403).end();return;}
    const claims={iss:profile.issuer,aud:'urn:soty:source-authority',sub:context.humanPrincipal.subject,
      iat:Math.floor(now/1000),exp:Math.floor(Math.min(now+10000,context.expiresAt)/1000),sourceChallenge:input.nonce,contextDigest:digest(context)};
    const identityWitness=await new SignJWT(claims).setProtectedHeader({alg:'RS256',typ:'soty-source-authority+jwt',kid:'fixture'}).sign(signing);
    const body=JSON.stringify({nonce:input.nonce,context,identityWitness});
    res.writeHead(200,{'content-type':'application/json','x-soty-source-mac':createHmac('sha256',key).update('authority-result\0'+body).digest('base64url')}).end(body);
  }catch{res.writeHead(503).end();}});
  const server=createServer((req,res)=>void bff.handleRequest(req,res));
  for(const listener of [issuerServer,authority,server])await new Promise(done=>listener.listen(0,'127.0.0.1',done));
  const origin='http://127.0.0.1:'+server.address().port,issuer='http://127.0.0.1:'+issuerServer.address().port+'/human-identity',appId='app-'+'b'.repeat(32);
  const rawProfile={schema:'soty.selected-human-embed.v2',appId,connector:{linkId:'fixture',hostDeviceId:'host',connectorId:'connector'},
    target:{revision:1,digest:'1'.repeat(64)},sourceProfile:STANDARD_ROOT_WORKSPACE_SOURCE,
    resource:{registryId:'soty',environmentId:'fixture',tenantId:'author',appId,resourceId:'personal-workspaces',selection:{kind:'soty.workspace.v1',nativeId:'personal-workspaces',incarnationId:'one'}},
    issuer,clientId:'reading-list',embedOrigin:origin,nativeOrigin:origin.replace('127.0.0.1','localhost'),parentOrigin:new URL(issuer).origin};
  profile=selectedResourceProfile(rawProfile);
  const options={profile:rawProfile,databasePath:join(directory,'native.sqlite'),cipherKey,keyId:'fixture',clock,personalWorkspaces,domain};
  function start(initialize=false){host=createHost({...options,initialize});bff=createBff({profile:rawProfile,transportKey:key,connectorPort:authority.address().port,storage:host.storage,native:host.native,clock,
    ...(onVerifiedOperation?{onVerifiedOperation}:{})});}
  start(true);
  function frame(subject='alice',accountId=subject,deviceId='device-'+subject){context={schema:'soty.verified-launch-continuation.v2',reference:{id:random(),version:1,digest:randomBytes(32).toString('hex')},
    profileDigest:profile.digest,appId,sourceProfile:profile.sourceProfile,resource:profile.resource,instanceId:'main',rootPrincipal:{accountId,deviceId},
    humanPrincipal:{issuer,subject,clientId:profile.clientId,clientProfileDigest:'2'.repeat(64),clientGeneration:1},entry:{domainId:'domain',origin},target:profile.target,policyEpoch:1,expiresAt:now+300000};}
  frame();
  const signer=createResourceSourceProofSigner({profile,key,clock});
  function browser(){let token;
    return{get token(){return token;},set token(value){token=value;},async request(path='/api/embed/session-continue',input={requestId:'source-personal-continue-0001'}){
      const body=Buffer.from(JSON.stringify(input)),cookie=token?'soty_rp_session='+token:'';
      const response=await fetch(origin+path,{method:'POST',headers:{origin,'content-type':'application/json',...(cookie?{cookie}:{}),...signer.headers({context,method:'POST',path,body,cookie})},body});
      const set=response.headers.get('set-cookie');if(set)token=set.split(';')[0].split('=')[1];return{status:response.status,body:await response.json(),cookie:!!set};
    }};
  }
  t.after(async()=>{bff?.close();host?.close();for(const listener of [server,authority,issuerServer]){listener.closeAllConnections();await new Promise(done=>listener.close(done));}
    assert.equal(dirname(resolve(directory)),resolve(tmpdir()));await rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:40});});
  return{browser,frame,profile,directory,origin,options,host:()=>host,inspect:()=>host.inspect(),
    restart(){bff.close();host.close();start();},revoke(subject,active=false){host.setIdentityAccess({issuer,subject},active);},
    closeRoot(){rootLive=false;},onRootRead(hook){rootHook=hook;},advance(ms){now+=ms;}};
}

const current=async browser=>{const response=await browser.request('/api/embed/query',{requestId:'reading-current-0001',input:{operation:'soty.current_context'}});assert.equal(response.status,200,JSON.stringify(response.body));return response.body.data.context;};
const read=async(browser,ref)=>browser.request('/api/embed/query',{requestId:'reading-list-0001',contextRef:ref,input:{operation:'reading.list'}});
const add=(browser,ref,id='reading-item-0001',title='Alice private item')=>browser.request('/api/embed/invoke',{requestId:id,contextRef:ref,input:{operation:'reading.add',title}});

test('generic SQLite Source host: real signed HTTP, personal isolation, cold recovery and no reapply',async t=>{
  const f=await fixture(t),alice=f.browser(),bob=f.browser();
  assert.equal((await alice.request()).status,200);const first=await current(alice);
  const result=await add(alice,first.ref);assert.equal(result.status,200,JSON.stringify(result.body));
  const original=structuredClone(result.body.data);
  assert.deepEqual((await read(alice,first.ref)).body.data.items,[{id:'reading-item-0001',title:'Alice private item'}]);
  f.frame('bob');bob.token=alice.token; // A stale Source cookie cannot bind B to A's data.
  const stale=await read(bob,first.ref);assert.ok(stale.status>=400);assert.equal(stale.body.data,undefined);
  assert.equal((await bob.request()).status,401,'foreign locator cannot silently rebind');bob.token=undefined;
  assert.equal((await bob.request()).status,200);const bobContext=await current(bob);
  assert.notEqual(bobContext.ref,first.ref);assert.deepEqual((await read(bob,bobContext.ref)).body.data.items,[]);
  assert.equal((await add(bob,bobContext.ref,'reading-item-0001','Bob private item')).status,200);
  f.restart();f.frame('alice');assert.equal((await alice.request()).status,200);const fresh=await current(alice);assert.notEqual(fresh.ref,first.ref);
  const recovered=await alice.request('/api/embed/receipt',{requestId:'reading-item-0001',contextRef:fresh.ref,input:{inputDigest:digest({operation:'reading.add',title:'Alice private item'})}});
  assert.equal(recovered.status,200,JSON.stringify(recovered.body));const{recovery,...immutable}=recovered.body.data;
  assert.deepEqual(immutable,original);assert.equal(recovery.contextRef,first.ref);
  assert.deepEqual((await read(alice,fresh.ref)).body.data.items,[{id:'reading-item-0001',title:'Alice private item'}]);
  const conflict=await add(alice,fresh.ref,'reading-item-0001','Changed input');assert.ok(conflict.status>=400);
  assert.deepEqual((await read(alice,fresh.ref)).body.data.items,[{id:'reading-item-0001',title:'Alice private item'}]);
  f.revoke('alice');const denied=await read(alice,fresh.ref);assert.ok(denied.status>=400);assert.equal(denied.body.data,undefined);
  f.revoke('alice',true);assert.ok((await read(alice,fresh.ref)).status>=400,'ABA cannot revive old Source/context authority');
  f.frame('alice');assert.equal((await alice.request()).status,200);const rejoined=await current(alice);
  assert.deepEqual((await read(alice,rejoined.ref)).body.data.items,[{id:'reading-item-0001',title:'Alice private item'}]);
  f.closeRoot();assert.ok((await read(alice,rejoined.ref)).status>=400);
});

test('personal document callbacks are transaction-scoped and a failed write is rolled back',async t=>{
  let retained;
  const domain={personalDocuments:true,
    read(context,{input}){assert.deepEqual(Object.keys(context).sort(),['personal','resourceId']);retained=context.personal;
      if(input.operation==='reading.write-in-read')context.personal.create('items','invalid-read','not written');
      return{items:context.personal.list('items')};},
    execute(context,{requestId,input}){context.personal.create('items',requestId,{title:input.title});
      if(input.title==='roll back')throw Error('application rejected staged change');
      return{workspaceReceipt:{schema:'soty.workspace-receipt.v1',effects:[{kind:'created',resourceType:'reading-item',resourceId:requestId,revision:1}],
        receipt:{verificationMethod:'domain_read',artifacts:[{type:'reading-item',id:requestId,revision:1}]}}};},
  };
  const f=await fixture(t,{domain}),browser=f.browser();assert.equal((await browser.request()).status,200);const context=await current(browser);
  assert.deepEqual((await read(browser,context.ref)).body.data.items,[]);
  assert.throws(()=>retained.list('items'),{code:'source_workspace_personal_context_closed'});
  assert.ok((await browser.request('/api/embed/query',{requestId:'reading-denied-write-in-read',contextRef:context.ref,input:{operation:'reading.write-in-read'}})).status>=400);
  assert.ok((await add(browser,context.ref,'reading-rollback-0001','roll back')).status>=400);
  assert.deepEqual((await read(browser,context.ref)).body.data.items,[]);
  assert.equal((await add(browser,context.ref,'reading-committed-001','kept')).status,200);
  assert.deepEqual((await read(browser,context.ref)).body.data.items,[{id:'reading-committed-001',value:{title:'kept'}}]);
});

test('opting a legacy database into new document mode requires migration and leaves its old data usable',async t=>{
  const f=await fixture(t),browser=f.browser();assert.equal((await browser.request()).status,200);const context=await current(browser);
  assert.equal((await add(browser,context.ref)).status,200);
  assert.throws(()=>createSqliteRootWorkspaceHost({...f.options,domain:{personalDocuments:true,read(){return{};}}}),
    {code:'source_workspace_host_schema_mismatch'});
  assert.deepEqual((await read(browser,context.ref)).body.data.items,[{id:'reading-item-0001',title:'Alice private item'}]);
});

test('personal creation is explicit; Root app-owner metadata never grants another Native identity',async t=>{
  const f=await fixture(t,{personalWorkspaces:false}),browser=f.browser();
  const denied=await browser.request();assert.equal(denied.status,403);assert.equal(denied.cookie,false);
  assert.equal(f.inspect().personalWorkspaces,false);assert.equal(f.inspect().keyCorrectness,'unproved_empty');
  const db=new DatabaseSync(join(f.directory,'native.sqlite'));try{assert.equal(db.prepare('SELECT count(*) n FROM soty_workspace_people').get().n,0);}finally{db.close();}
});

test('Source storage survives cold start, rejects wrong key and cannot create a session with JSON proof',async t=>{
  const f=await fixture(t),browser=f.browser();assert.equal((await browser.request()).status,200);
  assert.equal(f.inspect().keyCorrectness,'sample_verified');
  assert.throws(()=>createSqliteRootWorkspaceHost({...f.options,cipherKey:randomBytes(32)}),error=>error.code==='source_session_storage_corrupt');
  f.restart();assert.equal(f.inspect().keyCorrectness,'sample_verified');
});

test('failed diagnostics cannot turn a committed Native write into a retry or retain private args',async t=>{
  const events=[],f=await fixture(t,{onVerifiedOperation(event){events.push(structuredClone(event));throw Error('synthetic_disk_full');}}),browser=f.browser();
  assert.equal((await browser.request()).status,200);const selected=await current(browser);
  const applied=await add(browser,selected.ref);assert.equal(applied.status,200);
  assert.equal(events.at(-1).status,'commit_verified');assert.equal(events.at(-1).operation,'execute');
  assert.equal(JSON.stringify(events).includes('Alice private item'),false);assert.equal(JSON.stringify(events).includes('subject'),false);
  assert.deepEqual((await read(browser,selected.ref)).body.data.items,[{id:'reading-item-0001',title:'Alice private item'}]);
  const observed=events.length;f.revoke('alice');assert.ok((await read(browser,selected.ref)).status>=400);assert.equal(events.length,observed);
});

test('session cipher binds app/profile namespace, row id and key id; ciphertext is not portable authority',()=>{
  const key=randomBytes(32),id=digest('one'),a=createSourceSessionCipher({key,keyId:'one',namespace:digest('A')}),b=createSourceSessionCipher({key,keyId:'one',namespace:digest('B')});
  const encrypted=a.seal(id,{private:'session'});assert.deepEqual(a.open(id,encrypted,'one'),{private:'session'});
  for(const action of [()=>a.open(digest('other'),encrypted,'one'),()=>b.open(id,encrypted,'one'),()=>a.open(id,encrypted,'two'),()=>a.open(id,encrypted.slice(1),'one')])assert.throws(action);
  a.close();b.close();assert.throws(()=>a.seal(id,{}));
});

test('portable built SDK shares the private Root/Native brands and retains cold receipts on its own SQLite host',async t=>{
  const f=await fixture(t,{portable:true}),browser=f.browser();assert.equal((await browser.request()).status,200);
  const old=await current(browser),applied=await add(browser,old.ref);assert.equal(applied.status,200);
  f.restart();f.frame('alice');assert.equal((await browser.request()).status,200);const next=await current(browser);
  const proof=await browser.request('/api/embed/receipt',{requestId:'reading-item-0001',contextRef:next.ref,input:{inputDigest:digest({operation:'reading.add',title:'Alice private item'})}});
  assert.equal(proof.status,200);assert.equal(proof.body.data.recovery.contextRef,old.ref);
  assert.deepEqual((await read(browser,next.ref)).body.data.items,[{id:'reading-item-0001',title:'Alice private item'}]);
});

test('a staged domain error rolls back both data and receipt before Native commit',async t=>{
  const f=await fixture(t,{domain:listDomain({afterWrite(){throw Error('synthetic_domain_failure');}})}),browser=f.browser();
  assert.equal((await browser.request()).status,200);const selected=await current(browser);assert.ok((await add(browser,selected.ref)).status>=400);
  assert.deepEqual((await read(browser,selected.ref)).body.data.items,[]);
  const db=new DatabaseSync(join(f.directory,'native.sqlite'));try{assert.equal(db.prepare('SELECT count(*) n FROM soty_workspace_receipts').get().n,0);}finally{db.close();}
});

test('an authority deadline reached after a staged domain write rolls back data, revision and receipt',async t=>{
  let fixtureState;
  const f=await fixture(t,{domain:listDomain({afterWrite(){fixtureState.advance(300001);}})});fixtureState=f;
  const browser=f.browser();assert.equal((await browser.request()).status,200);const selected=await current(browser);
  const reply=await add(browser,selected.ref);assert.ok(reply.status>=400);assert.equal(reply.body.data,undefined);
  const db=new DatabaseSync(join(f.directory,'native.sqlite'));
  try{assert.equal(db.prepare('SELECT count(*) n FROM reading_items').get().n,0);
    assert.equal(db.prepare('SELECT count(*) n FROM soty_workspace_receipts').get().n,0);
    assert.equal(db.prepare('SELECT revision FROM soty_workspace_people').get().revision,1);
  }finally{db.close();}
});

test('turning off explicit personal issuance preserves existing people and blocks a new identity',async t=>{
  const f=await fixture(t),alice=f.browser();assert.equal((await alice.request()).status,200);
  const before=await current(alice);assert.equal((await add(alice,before.ref)).status,200);
  f.options.personalWorkspaces=false;f.restart();f.frame('alice');assert.equal((await alice.request()).status,200);
  const after=await current(alice);assert.equal((await read(alice,after.ref)).body.data.items.length,1);
  f.frame('bob');const fresh=f.browser(),denied=await fresh.request();assert.equal(denied.status,403);assert.equal(denied.cookie,false);
  assert.equal(f.inspect().personalWorkspaces,false);
});
