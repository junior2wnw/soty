import test from 'node:test';
import assert from 'node:assert/strict';
import {createScopedGatewayFixture,sourceAvailable} from './support/scoped-gateway-fixture.mjs';
import {readStorageFormat} from '../../../deploy/connector/storage-probe.mjs';
import {assertStorageCompatible,currentStorageReaders,storageReaderLabel} from '../../../deploy/connector/storage-guard.mjs';
import {join} from 'node:path';
import {createClientWithStorage} from '../../connect/browser/client.mjs';

test('actual installed Apps channel → private Human subject → selected Source BFF/OIDC/native consent → scoped HTTP, close and current revoke',{
  skip:sourceAvailable?false:'Optional packaged selected Planner Source absent',timeout:90000},async t=>{
  const f=await createScopedGatewayFixture({t});
  const format=await readStorageFormat(join(f.directory,'root'));assert.equal(format.apps,7);
  const image={Id:'sha256:'+'a'.repeat(64),Config:{Labels:{[storageReaderLabel]:currentStorageReaders}}};
  assert.equal(assertStorageCompatible(image,format).apps,7);
  const old=JSON.parse(currentStorageReaders);old.readers.apps=[1,2,3,4,5,6];
  assert.throws(()=>assertStorageCompatible({...image,Config:{Labels:{[storageReaderLabel]:JSON.stringify(old)}}},format),{code:'storage_reader_incompatible'});
  await assert.rejects(f.foreign.client.extension('apps.launch',{appId:f.appId}));
  const launch=await f.launch();assert.equal(launch.runtimeProfile,'soty.selected-human-embed.v1');
  await assert.rejects(f.foreign.client.extension('apps.scoped.close',{appId:f.appId,handle:launch.scopedCloseHandle}),error=>error.code==='app_scoped_close_unavailable');
  assert.ok(/^[A-Za-z0-9_-]{43}$/.test(launch.scopedCloseHandle));
  const login=await f.login();assert.equal(login.status,200);
  assert.equal((await f.nativeConsent(login)).status,200);
  const state=await f.wire.request(f.embedded+'/api/embed/state');assert.equal(state.status,200);assert.equal(state.body.workspaces.length,1);assert.equal(state.body.workspaces[0].id,f.workspaceId);
  assert.equal(state.body.entities.some(value=>value.workspaceId===f.foreignWorkspaceId),false);
  const created=await f.wire.request(f.embedded+'/api/embed/entities',{body:{workspaceId:f.workspaceId,title:'Synthetic real gateway WorkItem'}});
  assert.equal(created.status,201);assert.equal(created.body.entities.at(-1).plan.start,null);
  for(const path of ['/api/state','/api/embed/agent/keys','/api/embed/workspaces','/api/agent/tools'])assert.ok((await f.wire.request(f.embedded+path)).status>=400);
  const forbidden=await f.wire.request(f.embedded+'/api/embed/entities',{body:{workspaceId:f.foreignWorkspaceId,title:'Must not create'}});assert.equal(forbidden.status,403);
  await f.restartSource();assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,200);
  const repeat=await f.launch();const repeated=await f.login();assert.equal(repeated.status,200);assert.ok(!repeated.text.includes('/soty/connect?intent='),'same exact semantic binding does not need native consent again');
  await assert.rejects(f.reader.client.extension('apps.scoped.close',{appId:'app-'+'f'.repeat(32),handle:repeat.scopedCloseHandle}));
  await f.reader.client.extension('apps.scoped.close',{appId:f.appId,handle:launch.scopedCloseHandle});
  assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,200,'old close must not close new slot');
  await f.reader.client.extension('apps.scoped.close',{appId:f.appId,handle:repeat.scopedCloseHandle});
  assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,403);
  await f.launch();const mismatch=await f.login(f.owner);assert.equal(mismatch.status,401,'a different actual OIDC sub cannot join the original selected launch');
  const ownerLaunch=await f.launch(f.owner),pending=await f.login(f.owner);assert.equal(pending.status,200);
  await f.owner.client.extension('apps.scoped.close',{appId:f.appId,handle:ownerLaunch.scopedCloseHandle});
  const deferred=/<a href="([^"]+\/soty\/connect\?intent=[A-Za-z0-9_-]+)">/.exec(pending.text)?.[1];assert.ok(deferred);
  assert.ok((await f.wire.request(deferred)).status>=400,'delayed native approval after close cannot revive a slot');
  await f.launch();await f.login();
  await f.owner.client.extension('apps.update',{appId:f.appId,grants:{accountIds:[],communityIds:[]}});
  assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,403);
  assert.equal(f.planner().store.read().entities.filter(value=>value.workspaceId===f.foreignWorkspaceId).length,1);
});

test('actual SDK retained close capability keeps reads fenced after cross-tab switch, rejects stolen handle and automatically closes stale launch ACK', {
  skip:sourceAvailable?false:'Optional packaged selected Planner Source absent',timeout:90000},async t=>{
  const f=await createScopedGatewayFixture({t}),kit=await f.foreign.client.prepareRecovery();await f.foreign.client.confirmRecovery(kit);
  const recovered=await f.reader.client.recover(kit,'Synthetic explicit second profile');await f.reader.client.switchProfile(f.reader.account.accountId);
  const launch=await f.launch(),cleanup=f.reader.client.appSlotCleanup(launch);assert.equal(typeof cleanup,'function');
  assert.equal(f.reader.client.appSlotCleanup(JSON.parse(JSON.stringify(launch))),null);
  await assert.rejects(cleanup('caller cannot select args'),error=>error.code==='INVALID_ARGUMENT');
  const other=createClientWithStorage({projectId:'soty',endpoint:f.backendOrigin+'/api/connect/rpc',scopedAppCleanup:true,fetch:f.reader.fetch},f.reader.storage);t.after(()=>other.dispose());
  await other.switchProfile(recovered.accountId);
  assert.equal(f.root().locals.connectService.isActorActive(f.reader.account),true,'local profile switch is not server device revocation');
  await assert.rejects(f.reader.client.extension('apps.inspect',{appId:f.appId},{expectedAccountId:f.reader.account.accountId}),error=>error.code==='ACTIVE_PROFILE_CHANGED');
  await assert.rejects(f.reader.client.extension('apps.scoped.close',{appId:f.appId,handle:launch.scopedCloseHandle}),error=>error.code==='app_scoped_close_unavailable');
  await cleanup();await cleanup();assert.equal((await f.wire.request(f.embedded+'/api/embed/session-status')).status,403);
  await other.switchProfile(f.reader.account.accountId);
  let reply;f.reader.hooks.afterResponse=async(op,response)=>{if(op!=='apps.launch')return;reply=await response.clone().json();await other.switchProfile(recovered.accountId);};
  await assert.rejects(f.reader.client.extension('apps.launch',{appId:f.appId},{expectedAccountId:f.reader.account.accountId}),error=>error.code==='ACTIVE_PROFILE_CHANGED');
  f.reader.hooks.afterResponse=null;assert.equal(f.root().locals.connectService.isActorActive(f.reader.account),true);
  const ticket=new URL(reply.launchUrl).hash.slice(1);
  assert.equal((await f.wire.request(f.embedded+'/_soty/session',{body:{ticket}})).status,403,'late signed A reply is closed by A without permitting B to acquire its slot');
  await other.switchProfile(f.reader.account.accountId);const last=await f.launch(),closed=f.reader.client.appSlotCleanup(last);
  f.reader.client.dispose();assert.equal(f.reader.client.appSlotCleanup(last),null);await assert.rejects(closed(),error=>error.code==='CLIENT_DISPOSED');
});

test('actual installed prepare requires Origin, one-use CSRF and original slot; cancellation removes unknown prepared login without replay', {
  skip:sourceAvailable?false:'Optional packaged selected Planner Source absent',timeout:90000},async t=>{
  const f=await createScopedGatewayFixture({t});await f.launch();
  const prepare=()=>f.wire.request(f.embedded+'/api/embed/login',{headers:{accept:'application/json'}});
  const first=await prepare();assert.equal(first.status,200);assert.equal(first.body.schema,'planner.embed-login-preparation.v1');
  assert.equal((await f.wire.request(f.embedded+'/api/embed/login',{body:{}})).status,403);
  assert.equal((await f.wire.request(f.embedded+'/api/embed/login',{origin:'https://foreign.invalid',body:{csrf:first.body.csrf}})).status,403);
  const duplicate=Buffer.from('{"csrf":"'+first.body.csrf+'","csrf":"'+first.body.csrf+'"}');
  assert.equal((await f.wire.request(f.embedded+'/api/embed/login',{body:duplicate})).status,403);
  await f.launch();assert.equal((await f.wire.request(f.embedded+'/api/embed/login',{body:{csrf:first.body.csrf}})).status,401,'another slot never inherits the Source private cookie');
  const second=await prepare(),ready=await f.wire.request(f.embedded+'/api/embed/login',{body:{csrf:second.body.csrf}});
  assert.equal(ready.status,200);assert.equal(ready.body.schema,'planner.embed-login-authorization.v1');
  const state=new URL(ready.body.authorizationUrl).searchParams.get('state');
  assert.equal((await f.wire.request(f.embedded+'/api/embed/login',{body:{csrf:second.body.csrf}})).status,401,'prepared CSRF cannot be replayed');
  assert.equal((await f.wire.request(f.embedded+'/api/embed/login',{body:{csrf:second.body.csrf,cancel:true}})).status,200);
  assert.equal((await f.wire.request(f.embedded+'/api/embed/callback?state='+encodeURIComponent(state)+'&code=synthetic',{withoutAppCookie:true})).status,403);
  const old=await prepare();await f.owner.client.extension('apps.update',{appId:f.appId,grants:{accountIds:[],communityIds:[]}});
  assert.equal((await f.wire.request(f.embedded+'/api/embed/login',{body:{csrf:old.body.csrf}})).status,403);
});

test('actual revocation rejects original cleanup credentials and independently invalidates its cached slot', {
  skip:sourceAvailable?false:'Optional packaged selected Planner Source absent',timeout:90000},async t=>{
  const f=await createScopedGatewayFixture({t});
  const store={value:null,async read(){return structuredClone(this.value);},async claim(value){this.value??=structuredClone(value);return structuredClone(this.value);},
    async compareAndSwap(revision,value){assert.equal(this.value.localRevision,revision);this.value=structuredClone(value);return structuredClone(value);}};
  const sibling=createClientWithStorage({projectId:'soty',endpoint:f.backendOrigin+'/api/connect/rpc',fetch:f.reader.fetch},store);t.after(()=>sibling.dispose());
  const start=await sibling.startEnrollment('Synthetic authorized second device');
  await f.reader.client.approveEnrollment(start.requestId,f.reader.account.accountId);await sibling.previewEnrollment(start.requestId);await sibling.finishEnrollment(start.requestId,f.reader.account.accountId);
  const launch=await f.launch(),cleanup=f.reader.client.appSlotCleanup(launch);
  await sibling.revokeDevice(f.reader.account.deviceId);assert.equal(f.root().locals.connectService.isActorActive(f.reader.account),false);
  await assert.rejects(cleanup(),error=>['device_revoked','DEVICE_REVOKED'].includes(error.code));
  assert.equal((await f.wire.request(f.embedded+'/api/embed/session-status')).status,403);
  await assert.rejects(f.reader.client.extension('apps.launch',{appId:f.appId}),error=>error.code==='device_revoked');
});

test('actual Source prepare committed behind an unknown HTTP ACK can be cancelled without authorization replay', {
  skip:sourceAvailable?false:'Optional packaged selected Planner Source absent',timeout:90000},async t=>{
  const f=await createScopedGatewayFixture({t});await f.launch();
  const preparation=await f.wire.request(f.embedded+'/api/embed/login',{headers:{accept:'application/json'}});
  assert.equal(preparation.status,200);
  let state,intercepted=false;
  const loseAck=(request,response)=>{
    if(intercepted || request.method!=='POST' || request.url!=='/api/embed/login' || request.headers.host!==new URL(f.embedded).host)return;
    intercepted=true;const parts=[];
    response.write=(chunk,encoding,callback)=>{parts.push(Buffer.from(chunk));const done=typeof encoding==='function'?encoding:callback;if(done)queueMicrotask(done);return true;};
    response.end=(chunk)=>{
      if(chunk)parts.push(Buffer.from(chunk));
      const committed=JSON.parse(Buffer.concat(parts).toString('utf8'));
      assert.equal(committed.schema,'planner.embed-login-authorization.v1');
      state=new URL(committed.authorizationUrl).searchParams.get('state');
      response.destroy();return response;
    };
  };
  f.server.prependListener('request',loseAck);t.after(()=>f.server.off('request',loseAck));
  await assert.rejects(f.wire.request(f.embedded+'/api/embed/login',{body:{csrf:preparation.body.csrf}}));
  assert.equal(intercepted,true);assert.ok(state,'the real Source committed its one-use OIDC state before the response was lost');
  assert.equal((await f.wire.request(f.embedded+'/api/embed/login',{body:{csrf:preparation.body.csrf,cancel:true}})).status,200);
  assert.equal((await f.wire.request(f.embedded+'/api/embed/callback?state='+encodeURIComponent(state)+'&code=synthetic',{withoutAppCookie:true})).status,403);
  assert.equal((await f.wire.request(f.embedded+'/api/embed/login',{body:{csrf:preparation.body.csrf}})).status,401);
  assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,401,'cancelling prepared authentication creates no Source access');
});

test('Apps7 exact admission composes real universal registration and mandatory feedback metadata, and missing private approval never falls back', {
  skip:sourceAvailable?false:'Optional packaged selected Planner Source absent',timeout:90000},async t=>{
  const f=await createScopedGatewayFixture({t});
  const current=await f.owner.client.extension('apps.universal.get',{appId:f.appId,expectedAccountId:f.owner.account.accountId});
  await f.owner.client.extension('apps.universal.admit',{appId:f.appId,expectedAccountId:f.owner.account.accountId,requestId:'scoped-admit-fixture',expectedRevision:current.registration.revision,
    proposal:{kind:'author-draft',draft:{title:'Synthetic selected-source app'}}});
  const context=await f.reader.client.extension('apps.feedback.context',{appId:f.appId});assert.equal(context.readiness,'ready');assert.ok(context.context.installationId);
  await f.restartRoot({scopedEmbedProfiles:[]});
  await assert.rejects(f.owner.client.extension('apps.universal.get',{appId:f.appId,expectedAccountId:f.owner.account.accountId}),error=>error.code==='app_scoped_admission_required');
  await assert.rejects(f.reader.client.extension('apps.feedback.context',{appId:f.appId}),error=>error.code==='app_scoped_admission_required');
});
