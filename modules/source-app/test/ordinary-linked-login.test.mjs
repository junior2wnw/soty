import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {mkdtemp,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {spawn} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {createOrdinaryAppStore} from '../examples/ordinary-app/store.mjs';
import {createOrdinaryAppNativePort} from '../examples/ordinary-app/native.mjs';
import {createNativeAuthorityRuntime} from '../server/native-authority.mjs';

async function fixture(t,{enabled=true}={}){
  const directory=await mkdtemp(join(tmpdir(),'soty-ordinary-linked-'));
  const options={databasePath:join(directory,'native.sqlite'),realmId:'linked',key:randomBytes(32),keyId:'synthetic-linked',initialize:true};
  let store=createOrdinaryAppStore(options),runtime;
  store.createResource({id:'selected',incarnationId:'one',title:'Synthetic returning resource',guestEmpty:true});
  function start(){runtime=createNativeAuthorityRuntime(createOrdinaryAppNativePort({store,resourceId:'selected',incarnationId:'one',allowEmptyGuest:true,allowLinkedLogin:enabled}));}
  start();t.after(async()=>{runtime.close();store.close();await rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:40});});
  const binding={identity:{issuer:'https://issuer.test/human-identity',subject:'synthetic-linked-human'},rootPrincipal:{accountId:'synthetic-root',deviceId:'synthetic-device'},
    humanPrincipal:{issuer:'https://issuer.test/human-identity',subject:'synthetic-linked-human',clientId:'synthetic-linked'},
    resource:{selection:{kind:'soty.resource.v1',nativeId:'selected',incarnationId:'one'}},semanticDigest:'a'.repeat(64),operation:'link'};
  const count=()=>Object.fromEntries(['native_principals','native_sessions','native_memberships','native_links','native_consents'].map(table=>[table,store.db.prepare('SELECT count(*) n FROM '+table).get().n]));
  async function create(){const proof=await runtime.capture(binding,{headers:{}});const linked=store.tx(()=>runtime.commitIdentity(proof,binding.identity,{createEmptyGuest:true}));return{proof,linked};}
  return{get store(){return store;},get runtime(){return runtime;},binding,count,create,options,
    restart(){runtime.close();store.close();store=createOrdinaryAppStore({...options,initialize:false});start();}};
}

test('linked login is constructor opt-in; initial empty guest and returning candidate cannot read or mutate before verified identity',async t=>{
  const f=await fixture(t),initial=await f.runtime.capture(f.binding,{headers:{}});
  await assert.rejects(f.runtime.call(initial,'read',{requestId:'linked-read-0001',input:{operation:'items.list'}}),error=>error.code==='ordinary_native_login_only');
  f.store.tx(()=>f.runtime.commitIdentity(initial,f.binding.identity,{createEmptyGuest:true}));
  await f.runtime.call(initial,'execute',{requestId:'linked-initial-write',input:{operation:'items.create',title:'Retained synthetic item'}});
  const before=f.count(),candidate=await f.runtime.capture(f.binding,{headers:{}});
  for(const [operation,input] of [['read',{requestId:'linked-read-0002',input:{operation:'items.list'}}],['execute',{requestId:'linked-write-0002',input:{operation:'items.create',title:'Must be denied'}}],
    ['readProof',{requestId:'linked-initial-write',input:{inputDigest:'b'.repeat(64)}}]])
    await assert.rejects(f.runtime.call(candidate,operation,input),error=>error.code==='ordinary_native_login_only');
  await assert.rejects(f.runtime.feedback(candidate,'context'),error=>error.code==='ordinary_native_login_only');
  await assert.rejects(f.runtime.feedback(candidate,'submit',{requestId:'linked-feedback-0002',body:'Must be denied',attachments:[]}),error=>error.code==='ordinary_native_login_only');
  const same=f.store.tx(()=>f.runtime.commitIdentity(candidate,f.binding.identity,{createEmptyGuest:true}));
  assert.equal(same.principalId,f.store.db.prepare('SELECT principal_id FROM native_links').get().principal_id);
  assert.deepEqual(f.count(),before);assert.equal(f.store.db.prepare('SELECT count(*) n FROM native_items').get().n,1);
  // A commitIdentity call does not turn this returning login candidate into a
  // data proof. Actual BFF creates a new Source session after fresh OIDC.
  await assert.rejects(f.runtime.call(candidate,'read',{requestId:'linked-read-0003',input:{operation:'items.list'}}),error=>error.code==='ordinary_native_login_only');
  f.restart();const restored=await f.runtime.capture(f.binding,{headers:{}});f.store.tx(()=>f.runtime.commitIdentity(restored,f.binding.identity,{createEmptyGuest:true}));assert.deepEqual(f.count(),before);
  const disabled=await fixture(t,{enabled:false});await disabled.create();
  await assert.rejects(disabled.runtime.capture(disabled.binding,{headers:{}}),error=>error.code==='ordinary_guest_existing_resource_denied');
});

test('returning login cannot widen identity/device/semantic/resource scope or overwrite an invalid Native cookie',async t=>{
  const f=await fixture(t);await f.create();
  const changed=[{identity:{...f.binding.identity,subject:'foreign-human'}},{rootPrincipal:{...f.binding.rootPrincipal,deviceId:'foreign-device'}},
    {semanticDigest:'b'.repeat(64)},{resource:{selection:{kind:'soty.resource.v1',nativeId:'selected',incarnationId:'two'}}}];
  for(const change of changed)await assert.rejects(f.runtime.capture({...f.binding,...change},{headers:{}}),error=>error.status===403);
  await assert.rejects(f.runtime.capture(f.binding,{headers:{cookie:'ordinary_native_linked=invalid-current-cookie'}}),error=>error.status===403);
  assert.equal(f.count().native_principals,1);
});

test('current Native SQL generation/role/session expiration denies a captured returning login at its final transaction',async t=>{
  for(const mode of ['session-revoked','session-expired','membership-revoked','membership-changed']){
    const f=await fixture(t),{linked}=await f.create(),candidate=await f.runtime.capture(f.binding,{headers:{}}),before=f.count();
    if(mode==='session-revoked')f.store.tx(()=>f.store.db.prepare('UPDATE native_sessions SET active=0,generation=generation+1').run());
    if(mode==='session-expired')f.store.tx(()=>f.store.db.prepare('UPDATE native_sessions SET expires_at=?').run(f.store.clock()-1));
    if(mode==='membership-revoked')f.store.revokeMembership('selected',linked.principalId);
    if(mode==='membership-changed')f.store.tx(()=>f.store.db.prepare("UPDATE native_memberships SET role='participant',revision=revision+1").run());
    assert.throws(()=>f.store.tx(()=>f.runtime.commitIdentity(candidate,f.binding.identity,{createEmptyGuest:true})),error=>error.code==='ordinary_native_access_denied');
    assert.deepEqual(f.count(),before);
    if(mode==='membership-changed'){
      const current=await f.runtime.capture(f.binding,{headers:{}});
      f.store.tx(()=>f.runtime.commitIdentity(current,f.binding.identity,{createEmptyGuest:true}));
      assert.equal(f.store.db.prepare('SELECT role FROM native_memberships').get().role,'participant','fresh new login keeps the reduced current Native role');
    }else await assert.rejects(f.runtime.capture(f.binding,{headers:{}}),error=>error.status===403);
  }
});

test('two real OS returning-login writers reuse the immutable Native link and do not duplicate principal/session/grant',async t=>{
  const f=await fixture(t);await f.create();const before=f.count();
  const config={options:{databasePath:f.options.databasePath,realmId:f.options.realmId,keyId:f.options.keyId},keyBase64:f.options.key.toString('base64'),binding:f.binding};
  const run=()=>new Promise((resolve,reject)=>{
    const child=spawn(process.execPath,[fileURLToPath(new URL('./support/ordinary-linked-login-worker.mjs',import.meta.url))],{windowsHide:true,stdio:['pipe','pipe','pipe']});
    let output='',overflow=false,closed=false;const timer=setTimeout(()=>child.kill(),10000);
    child.stdout.on('data',part=>{if(Buffer.byteLength(output)+part.length>4096){overflow=true;child.kill();}else output+=part;});child.stderr.resume();
    child.once('error',reject);child.once('close',code=>{closed=true;clearTimeout(timer);if(code!==0||overflow)return reject(new Error('synthetic_linked_worker_failed'));
      try{resolve(JSON.parse(output));}catch{reject(new Error('synthetic_linked_worker_invalid'));}});child.stdin.end(JSON.stringify(config));
    t.after(()=>{if(!closed)child.kill();});
  });
  const results=await Promise.all([run(),run()]);assert.deepEqual(results,[{linked:true},{linked:true}]);assert.deepEqual(f.count(),before);
});
