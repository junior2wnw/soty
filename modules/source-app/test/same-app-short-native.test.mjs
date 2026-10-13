// Real reviewed Root/Apps/Human OIDC + maintained SDK + Native283 storage.
// The two EventTargets model the mounted window boundary, not an authority.
// Logical HTTPS uses local fixture sockets; this is not browser/TLS acceptance.
import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {pathToFileURL} from 'node:url';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {setTimeout as pause} from 'node:timers/promises';
import {createSourceFrameFetch} from '../browser/frame-fetch.mjs';
import {createAppLauncher} from '../../../src/world/app-launch.mjs';
import {createScopedSlotRenewal,attachSourceFrameRefresh} from '../../../src/world/app-scoped-renewal.mjs';

const nativeRoot=process.env.PEREMETRIKA_NATIVE_ROOT;
const native=path=>import(pathToFileURL(join(nativeRoot,path)).href);
const createPeremetrikaInstalledFixture=nativeRoot?(await native('tests/fixtures/source-editor-installed-root.mjs')).createPeremetrikaInstalledFixture:null;
const createSelectedDocumentTransport=nativeRoot?(await native('src/page-builder/sourceTransport.mjs')).createSelectedDocumentTransport:null;
const qualification={timeout:120000,skip:nativeRoot?false:'Explicit reviewed PEREMETRIKA_NATIVE_ROOT fixture required'};
const journal=()=>{const rows=new Map();return{getItem:k=>rows.get(k)??null,setItem:(k,v)=>rows.set(k,v),removeItem:k=>rows.delete(k)};};
const digest=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const state=realm=>{const db=new DatabaseSync(realm.nativeFixture.filename,{readOnly:true});try{
  return{link:digest(db.prepare("SELECT revision,expires_at,cipher FROM native_source_records WHERE model='Link'").get()),
    sessions:db.prepare("SELECT id,expires_at FROM native_source_records WHERE model='Session' ORDER BY expires_at").all(),
    consents:db.prepare("SELECT count(*) n FROM native_source_records WHERE model='Consent'").get().n};
}finally{db.close();}};
const emit=(view,data,source,origin)=>{const event=new Event('message');Object.assign(event,{data,source,origin});view.dispatchEvent(event);};
const wireFetch=(f,realm)=>async(input,init)=>{const url=new URL(typeof input==='string'?input:input.url,realm.embedded);
  const result=await f.wire(url,init?.body?{body:JSON.parse(init.body)}:{});
  return new Response(result.text,{status:result.status,headers:{'content-type':'application/json'}});};

async function ready(t,{grantForeign=false,...options}={}){
  const f=await createPeremetrikaInstalledFixture(t,{rememberedDevice:true,rpRenewal:true,shortSessionMs:3000,...options});
  const [realm]=f.realms;if(grantForeign)await f.grantBrowser(f.foreign.account.accountId);
  await realm.launch();const flow=await f.authorize(realm,{approvalArgs:{rememberForDevice:true,stayInAppSeconds:86400}});await f.complete(flow.callbackUrl);
  const initial=await f.wire(realm.embedded+'/api/embed/session-continue',{body:{requestId:crypto.randomUUID()}});
  assert.equal(initial.status,200);assert.equal(initial.value.shortSession,true);
  const current=await f.reader.client.extension('apps.scoped.context',{appId:realm.appId,handle:realm.lastLaunch.scopedCloseHandle});
  assert.equal(current.sourceSession.shortSession,true);
  return{f,realm,initial,current};
}
async function stage(t,f,realm){
  let current=true,renewals=0,commits=0;const controller=new AbortController(),rootView=new EventTarget(),sourceView=new EventTarget();
  sourceView.setTimeout=setTimeout;sourceView.clearTimeout=clearTimeout;
  sourceView.parent={postMessage(data,origin){assert.equal(origin,f.origin);emit(rootView,data,sourceView,realm.embedded);}};
  sourceView.postMessage=(data,origin)=>{assert.equal(origin,realm.embedded);emit(sourceView,data,sourceView.parent,f.origin);};
  const launcher=createAppLauncher({target:{appId:realm.appId},accountId:f.reader.account.accountId,shellUrl:f.origin+'/',
    isCurrent:()=>current,request:async()=>({...realm.lastLaunch,url:realm.lastLaunch.launchUrl}),
    abandonScoped:args=>f.reader.client.extension('apps.scoped.close',args)});
  await launcher.launch();const readBinding=()=>launcher.scopedCapture();
  const renewal=createScopedSlotRenewal({readBinding,isCurrent:()=>current,
    readContext:handle=>f.reader.client.extension('apps.scoped.context',{appId:realm.appId,handle}),
    async request(args){renewals++;const reply=await f.reader.client.extension('apps.scoped.renew',{appId:realm.appId,...args});
      return launcher.prepareRenewal({...reply,url:reply.launchUrl});},
    async bootstrap(reply){
      // Exactly the existing hidden boot session-check/continue wire. Private
      // Source ACK is independently reread by the actual signed Root API.
      const issue=await f.wire(realm.embedded+'/_soty/session',{body:{ticket:new URL(reply.url).hash.slice(1)}});
      assert.equal(issue.status,200);assert.equal(issue.headers['set-cookie'],undefined);
      const headers={'x-soty-boot-check':issue.value.sessionCheck};
      assert.equal((await f.wire(realm.embedded+'/_soty/session',{headers})).status,200);
      const result=await f.wire(realm.embedded+'/api/embed/session-continue',{headers,body:{requestId:issue.value.scopedRequestId}});
      return result.status===200&&result.value?.ready===true?'ready':'login_required';
    },commit(old,next){if(!current||!launcher.commitRenewal(old,next))return false;commits++;return true;}});
  attachSourceFrameRefresh({view:rootView,signal:controller.signal,current:()=>current,
    readPeer:()=>current?{window:sourceView,origin:realm.embedded}:null,refresh:()=>renewal.refresh()});
  const fetch=createSourceFrameFetch({fetch:wireFetch(f,realm),href:realm.embedded+'/embed',shortSessionHandoff:true,view:sourceView,parentOrigin:f.origin});
  t.after(()=>{current=false;controller.abort();renewal.dispose();launcher.dispose();});
  return{fetch,renewal,sourceView,rootView,binding:readBinding,counts:()=>({renewals,commits}),
    setCurrent(value){current=value;}};
}
const expire=async realm=>{const before=state(realm);await pause(Math.max(0,before.sessions.at(-1).expires_at-Date.now()+40));return before;};

test('actual same-app SDK handoff accepts a new honest <=300s Native carrier from private ACK, preserves principal/link/scope and also renews an expired same-cookie',qualification,async t=>{
  const {f,realm,initial,current}=await ready(t,{allowEdit:false,earlyRpRefresh:true}),before=state(realm),mounted=await stage(t,f,realm);
  const originalSession=await realm.instance.sourceHost.adapter.storage.readSession(before.sessions.at(-1).id);
  const old=mounted.binding(),visibleWindow=mounted.sourceView,userinfo=f.oauthCounts['userinfo:200'];
  await expire(realm);assert.ok([401,403].includes((await f.wire(realm.embedded+'/api/embed/context')).status));
  const response=await mounted.fetch('/api/embed/context');assert.equal(response.status,200);assert.equal((await response.json()).ready,true);
  const next=mounted.binding();assert.notEqual(next.slot,old.slot);assert.equal(mounted.sourceView,visibleWindow);
  assert.deepEqual(mounted.counts(),{renewals:1,commits:1});
  const ack=await f.reader.client.extension('apps.scoped.context',{appId:realm.appId,handle:next.handle});
  assert.equal(ack.renewalBindingDigest,current.renewalBindingDigest);assert.equal(ack.sourceSession.shortSession,true);
  assert.ok(ack.sourceSession.sessionExpiresAt>initial.value.sessionExpiresAt);
  assert.ok(ack.sourceSession.sessionExpiresAt<=Date.now()+300000&&ack.sourceSession.sessionExpiresAt<=ack.expiresAt);
  assert.equal(state(realm).link,before.link);assert.equal(state(realm).consents,before.consents);assert.ok(f.oauthCounts['userinfo:200']>userinfo);
  const newSession=await realm.instance.sourceHost.adapter.storage.readSession(state(realm).sessions.at(-1).id);
  for(const field of ['principalId','nativeSessionHash','resourceId','consentId','consentEpoch','linkEpoch','policyDigest','rootPrincipal','identity','semanticDigest'])
    assert.deepEqual(newSession[field],originalSession[field],field);
  const reader=createSelectedDocumentTransport({fetch:mounted.fetch,storage:journal()});t.after(()=>reader.close());
  const page=await reader.read();assert.equal(page.permissions.edit,false);assert.equal(page.permissions.manage,false);

  // A separate real pair keeps the original Root frame/cookie, specifically
  // covering the SDK BFF's formerly unreachable expired-cookie lookup seam.
  const second=await ready(t),prior=state(second.realm),oldHandle=second.realm.lastLaunch.scopedCloseHandle;await expire(second.realm);
  const resumed=await second.f.wire(second.realm.embedded+'/api/embed/session-continue',{body:{requestId:crypto.randomUUID()}});
  assert.equal(resumed.status,200);assert.equal(resumed.value.shortSession,true);assert.ok(resumed.value.sessionExpiresAt>second.initial.value.sessionExpiresAt);
  assert.equal(state(second.realm).link,prior.link);assert.equal(second.realm.lastLaunch.scopedCloseHandle,oldHandle);
});

test('actual Root rejects foreign device/account/scope and Native Source revoke; forged/changed frame peer never starts or commits a handoff',qualification,async t=>{
  const {f,realm}=await ready(t,{grantForeign:true}),mounted=await stage(t,f,realm),before=state(realm),old=mounted.binding();
  const args={appId:realm.appId,handle:old.handle,requestId:crypto.randomUUID().replaceAll('-',''),shortSessionHandoff:true};
  await assert.rejects(f.foreign.client.extension('apps.scoped.renew',args),error=>error.code==='app_scoped_context_closed');
  const device=await f.anotherDevice();await assert.rejects(device.client.extension('apps.scoped.renew',args),error=>error.code==='app_scoped_context_closed');
  const data={schema:'soty.source-frame-refresh.v1',requestId:crypto.randomUUID().replaceAll('-','')};
  emit(mounted.rootView,data,{},realm.embedded);emit(mounted.rootView,data,mounted.sourceView,'https://foreign.invalid');
  emit(mounted.rootView,{...data,handle:old.handle},mounted.sourceView,realm.embedded);await pause(5);
  assert.deepEqual(mounted.counts(),{renewals:0,commits:0});
  realm.instance.sourceHost.adapter.storage.revokeSession(before.sessions.at(-1).id);
  const denied=await mounted.fetch('/api/embed/context');assert.ok([401,403].includes(denied.status));
  assert.equal(mounted.counts().commits,0);assert.equal(mounted.binding().slot,old.slot);assert.equal(state(realm).sessions.length,before.sessions.length);
  await f.owner.client.extension('apps.update',{appId:realm.appId,grants:{accountIds:[],communityIds:[]}});
  await assert.rejects(f.reader.client.extension('apps.scoped.renew',{...args,requestId:crypto.randomUUID().replaceAll('-','')}));
  assert.equal(state(realm).link,before.link);
});

test('actual unknown Native write stays at one invoke after same-app refresh; original UI reconciles its original receipt without moving or resending intent',qualification,async t=>{
  const {f,realm}=await ready(t),mounted=await stage(t,f,realm),storage=journal();
  const transport=createSelectedDocumentTransport({fetch:mounted.fetch,storage});t.after(()=>transport.close());
  const page=await transport.read(),desired=structuredClone(page.spec);desired.sections[0].blocks[0].title='One actual durable same-app write';
  const attempt={key:'same-app-unknown-write-0001',actorId:page.uiScope,pageId:page.pageId,revision:page.revision,spec:desired};
  realm.dropPath='/api/embed/invoke';await assert.rejects(transport.save(attempt));assert.equal(realm.invokeRequests,1);
  const committed=(await f.wire(realm.native+'/api/v1/pages/'+page.pageId)).value.data.revision;assert.equal(committed,page.revision+1);
  const visibleWindow=mounted.sourceView;await expire(realm);await transport.resume();
  assert.equal(mounted.sourceView,visibleWindow);assert.deepEqual(mounted.counts(),{renewals:1,commits:1});
  assert.equal((await transport.read()).revision,committed);assert.equal(realm.invokeRequests,1);
  const result=await transport.save(attempt);assert.equal(result.revision,committed);assert.equal(realm.invokeRequests,1);
  assert.equal((await f.wire(realm.native+'/api/v1/pages/'+page.pageId)).value.data.revision,committed);
});
