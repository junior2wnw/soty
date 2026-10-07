import test from 'node:test';
import assert from 'node:assert/strict';
import {setTimeout as pause} from 'node:timers/promises';
import {createOrdinaryInstalledFixture} from './support/ordinary-installed.mjs';

const nativeTables=['native_principals','native_sessions','native_memberships','native_links','native_consents'];
const counts=realm=>Object.fromEntries(nativeTables.map(name=>[name,realm.instance.store.db.prepare('SELECT count(*) n FROM '+name).get().n]));

async function returningLogin(f,realm){
  const before=counts(realm),sessions=realm.instance.store.db.prepare('SELECT count(*) n FROM source_sessions').get().n;
  await realm.restart();await realm.launch();await f.login(realm);
  const context=await f.wire(realm.embedded+'/api/embed/context');assert.equal(context.status,200);assert.equal(context.value.ready,true);
  const continuation=await f.wire(realm.embedded+'/api/embed/session-continue',{body:{requestId:'return-continuation-'+realm.realmId}});
  assert.equal(continuation.status,200,'safe continuation code='+String(continuation.value?.error?.code??continuation.value?.code??'absent'));assert.equal(continuation.value.renewable,false,'a new explicit Basic login does not create Long');
  assert.deepEqual(counts(realm),before);assert.equal(realm.instance.store.db.prepare('SELECT count(*) n FROM source_sessions').get().n,sessions+1);
  const read=await f.wire(realm.embedded+'/api/embed/query',{body:{requestId:'return-read-'+realm.realmId,input:{operation:'items.list'}}});
  assert.equal(read.status,200);assert.equal(read.value.data.items.length,1);
}

test('two installed new-empty realms: controlled Source-session expiry requires NEW OIDC; restart preserves Native principal/grant/data without Basic resurrection', {timeout:120000},async t=>{
  const f=await createOrdinaryInstalledFixture(t,{emptyGuestBrowser:true,allowLinkedLogin:true});
  for(const realm of f.realms){
    await realm.launch();await f.login(realm);
    const created=await f.wire(realm.embedded+'/api/embed/invoke',{body:{requestId:'return-create-'+realm.realmId,input:{operation:'items.create',title:'Retained returning item'}}});assert.equal(created.status,200);
    const before=counts(realm);assert.equal(before.native_principals,1);assert.equal(before.native_consents,1);
    realm.instance.store.tx(()=>realm.instance.store.db.prepare('UPDATE source_sessions SET expires_at=?').run(Date.now()-1));
    assert.equal((await f.wire(realm.embedded+'/api/embed/context')).status,401);
    await returningLogin(f,realm);
  }
});

test('returning installed OIDC lost callback ACK recovers only the same completion after restart; current Native revoke and foreign identity deny', {timeout:120000},async t=>{
  const f=await createOrdinaryInstalledFixture(t,{emptyGuestBrowser:true,allowLinkedLogin:true}),[first,second]=f.realms;
  await first.launch();await f.login(first);const before=counts(first);
  first.instance.store.tx(()=>first.instance.store.db.prepare('UPDATE source_sessions SET expires_at=?').run(Date.now()-1));
  await first.launch();const flow=await f.authorize(first);first.dropNextCallback=true;
  const lost=await f.wire(flow.callbackUrl);assert.ok([502,503].includes(lost.status));
  assert.equal((await f.wire(flow.callbackUrl)).status,403,'consumed Root map cannot re-exchange a code');
  assert.equal(first.instance.store.db.prepare('SELECT count(*) n FROM source_sessions').get().n,2);assert.deepEqual(counts(first),before);
  await first.restart();const recover=await f.wire(first.embedded+'/api/embed/login',{body:{}});assert.equal(recover.status,200);
  const native=await f.wire(recover.value.nativeUrl);assert.equal(native.status,303);assert.equal(native.location.searchParams.has('code'),false);
  await f.complete(native.location);assert.deepEqual(counts(first),before);assert.equal(first.instance.store.db.prepare('SELECT count(*) n FROM source_sessions').get().n,2);
  first.instance.store.tx(()=>first.instance.store.db.prepare('UPDATE native_sessions SET active=0,generation=generation+1').run());
  assert.equal((await f.wire(first.embedded+'/api/embed/context')).status,403);
  await first.launch();const denied=await f.wire(first.embedded+'/api/embed/login',{body:{}});assert.equal(denied.status,200);
  const page=await f.wire(denied.value.nativeUrl);assert.equal(page.status,200);
  const fields=Object.fromEntries([...page.text.matchAll(/name="([^"]+)" value="([^"]*)"/gu)].map(match=>[match[1],match[2]]));fields.consent='yes';
  assert.equal((await f.wire(first.native+'/soty/authorize',{fields})).status,403,'revoked Native authority cannot silently re-login');
  assert.deepEqual(counts(first),before);

  await second.launch();await f.login(second);const secondBefore=counts(second);
  await f.grantBrowser(f.foreign.account.accountId);await second.launch(f.foreign);
  const foreignStart=await f.wire(second.embedded+'/api/embed/login',{body:{}});assert.equal(foreignStart.status,200);
  const foreignPage=await f.wire(foreignStart.value.nativeUrl);assert.equal(foreignPage.status,200);
  const foreignFields=Object.fromEntries([...foreignPage.text.matchAll(/name="([^"]+)" value="([^"]*)"/gu)].map(match=>[match[1],match[2]]));foreignFields.consent='yes';
  assert.equal((await f.wire(second.native+'/soty/authorize',{fields:foreignFields})).status,403,'Root app grant is not an existing Native identity grant');
  assert.deepEqual(counts(second),secondBefore);
});

test('installed Basic real wall >300s: expired original slot cannot read/resume; explicit new OIDC reuses current Native grant in both persisted realms',
  {timeout:420000,skip:process.env.SOTY_SOURCE_BASIC_WALL!=='1'},async t=>{
    const f=await createOrdinaryInstalledFixture(t,{emptyGuestBrowser:true,allowLinkedLogin:true});
    for(const realm of f.realms){await realm.launch();await f.login(realm);
      const created=await f.wire(realm.embedded+'/api/embed/invoke',{body:{requestId:'wall-create-'+realm.realmId,input:{operation:'items.create',title:'Synthetic real-wall retained item'}}});assert.equal(created.status,200);}
    const started=Date.now();await pause(310000);
    assert.ok(Date.now()-started>=310000);
    for(const realm of f.realms){
      const old=await f.wire(realm.embedded+'/api/embed/context');assert.ok([401,403].includes(old.status),'expired original Root slot/session must deny');
      assert.ok([401,403].includes((await f.wire(realm.embedded+'/api/embed/session-continue',{body:{requestId:'wall-continuation-'+realm.realmId}})).status),'Basic has no silent resume');
      await returningLogin(f,realm);
    }
    t.diagnostic(JSON.stringify({synthetic:true,wallElapsedMs:Date.now()-started,realms:2,nativePrincipalsUnchanged:true,nativeGrantsUnchanged:true,newOidcRequired:true,basicRenewable:false}));
  });
