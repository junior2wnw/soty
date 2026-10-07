import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes,createHash} from 'node:crypto';
import {createScopedGatewayFixture,renewalSourceAvailable,until} from './support/scoped-gateway-fixture.mjs';
const random=()=>randomBytes(32).toString('base64url');
async function ready(t){const f=await createScopedGatewayFixture({t,renewal:true});await f.launch();const page=await f.login();
  const consent=await f.nativeConsent(page);assert.equal(consent.status,200);return f;}
async function continuation(f,value,requestId=random()) {
  const session=await f.wire.request(f.embedded+'/_soty/session',{body:{ticket:new URL(value.launchUrl).hash.slice(1)}});
  assert.equal(session.status,200);const response=await f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId}});
  return{session,response,requestId};
}
test('actual installed channel Root renew→Source RP/Native ACL ACK reuses consent and same request receipt', {skip:!renewalSourceAvailable},async t=>{
  const f=await ready(t),initial=await f.reader.client.extension('apps.launch',{appId:f.appId});
  const first=await continuation(f,initial);assert.equal(first.response.status,200);assert.equal(first.response.body?.ready,true,'Source actual continuation ready');
  const nativeCounts=()=>({links:f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_links').get().n,
    grants:f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_grants').get().n,
    heads:f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_rp_heads').get().n,
    receipts:f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_rebind_receipts').get().n});
  const before=nativeCounts(),requestId=random(),args={appId:f.appId,handle:initial.scopedCloseHandle,requestId};
  const value=await f.reader.client.extension('apps.scoped.renew',args),renewed=await continuation(f,value,requestId);
  assert.equal(renewed.response.status,200);assert.equal(renewed.response.body?.ready,true);
  const context=await f.reader.client.extension('apps.scoped.context',{appId:f.appId,handle:value.scopedCloseHandle});
  assert.equal(context.sourceSession?.ready,true);assert.equal(context.sourceSession?.renewable,true);
  const repeated=await f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId}});
  assert.equal(repeated.status,200);assert.equal(repeated.body?.receiptDigest===renewed.response.body?.receiptDigest,true);
  const replay=await f.reader.client.extension('apps.scoped.renew',args);assert.equal(replay.scopedCloseHandle===value.scopedCloseHandle,true);
  const after=nativeCounts();assert.equal(after.links,before.links);assert.equal(after.grants,before.grants);assert.equal(after.heads,before.heads);
  assert.equal(after.receipts,before.receipts+1);assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,200);
  await assert.rejects(f.foreign.client.extension('apps.scoped.renew',args),error=>error.code==='app_scoped_renew_conflict');
  await f.reader.client.extension('apps.scoped.close',{appId:f.appId,handle:initial.scopedCloseHandle});
  assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,200,'old close never closes new slot');
});
test('actual Source restart/Root RAM loss resumes proof-backed anchor with fresh launch, no native consent', {skip:!renewalSourceAvailable},async t=>{
  const f=await ready(t),initial=await f.reader.client.extension('apps.launch',{appId:f.appId}),first=await continuation(f,initial);
  assert.equal(first.response.body?.ready,true);const links=f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_links').get().n;
  await f.restartSource();await f.restartRoot();
  await until(async()=>{try{return(await f.reader.client.extension('apps.list')).apps.find(app=>app.id===f.appId)?.state==='ready';}catch{return false;}},60000);
  const next=await f.reader.client.extension('apps.launch',{appId:f.appId}),resumed=await continuation(f,next);
  assert.equal(resumed.response.status,200);assert.equal(resumed.response.body?.ready,true);assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,200);
  assert.equal(f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_links').get().n,links);
  await f.grant(f.foreign.account.accountId);
  const other=await f.foreign.client.extension('apps.launch',{appId:f.appId}),denied=await continuation(f,other);
  assert.equal(denied.response.status,200);assert.equal(denied.response.body?.ready,false);assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,401);
});
test('actual Source revoke/missing Origin/changed intent fail closed; no new alias/grant after revoke', {skip:!renewalSourceAvailable},async t=>{
  const f=await ready(t),initial=await f.reader.client.extension('apps.launch',{appId:f.appId});assert.equal((await continuation(f,initial)).response.body?.ready,true);
  const bad=await f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId:random()},headers:{origin:'https://foreign.invalid'}});assert.equal(bad.status,403);
  const disconnected=await f.wire.request(f.native+'/soty/disconnect',{fields:{}});assert.equal(disconnected.status,200);
  const renewed=await f.reader.client.extension('apps.scoped.renew',{appId:f.appId,handle:initial.scopedCloseHandle,requestId:random()});
  const denied=await continuation(f,renewed);assert.equal(denied.response.status,403);
  const current=await f.reader.client.extension('apps.scoped.context',{appId:f.appId,handle:renewed.scopedCloseHandle});assert.equal(current.sourceSession,undefined);
  assert.equal(f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_grants').get().n,0);
});

test('actual signed Root renew lost ACK retries the same action and retains the committed slot', {skip:!renewalSourceAvailable},async t=>{
  const f=await ready(t),initial=await f.reader.client.extension('apps.launch',{appId:f.appId});
  assert.equal((await continuation(f,initial)).response.body?.ready,true);
  const args={appId:f.appId,handle:initial.scopedCloseHandle,requestId:random()};let committedHandle=null,dropped=false;
  f.reader.hooks.afterResponse=async(operation,response)=>{
    if(operation!=='apps.scoped.renew'||dropped)return;dropped=true;
    const body=await response.clone().json();committedHandle=body.result?.scopedCloseHandle??body.scopedCloseHandle??null;
    throw new Error('fixture_unknown_response_after_commit');
  };
  await assert.rejects(f.reader.client.extension('apps.scoped.renew',args),error=>error.code==='NETWORK_ERROR');
  f.reader.hooks.afterResponse=null;
  const replay=await f.reader.client.extension('apps.scoped.renew',args);
  assert.equal(typeof committedHandle,'string');assert.equal(replay.scopedCloseHandle===committedHandle,true);
  assert.equal((await continuation(f,replay,args.requestId)).response.body?.ready,true);
  assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,200);
});

test('actual Source continuation lost wire ACK reads the durable receipt, without another alias or native grant', {skip:!renewalSourceAvailable},async t=>{
  const f=await ready(t),initial=await f.reader.client.extension('apps.launch',{appId:f.appId});
  assert.equal((await continuation(f,initial)).response.body?.ready,true);
  const count=()=>({grants:f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_grants').get().n,
    anchors:f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_rp_anchors').get().n,
    aliases:f.planner().store.db.prepare("SELECT count(*) AS n FROM planner_soty_private WHERE kind='session'").get().n,
    receipts:f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_rebind_receipts').get().n});
  const before=count(),args={appId:f.appId,handle:initial.scopedCloseHandle,requestId:random()},value=await f.reader.client.extension('apps.scoped.renew',args);
  assert.equal((await f.wire.request(f.embedded+'/_soty/session',{body:{ticket:new URL(value.launchUrl).hash.slice(1)}})).status,200);
  let dropped=false;
  const loseResponse=(req,res)=>{
    if(req.url!=='/api/embed/session-continue'||dropped)return;
    const end=res.end;res.end=function(chunk,...rest){
      let body;try{body=JSON.parse(String(chunk));}catch{}
      if(!dropped&&body?.ready===true){dropped=true;res.socket?.destroy();return this;}
      return end.call(this,chunk,...rest);
    };
  };
  f.planner().server.prependListener('request',loseResponse);
  const unknown=await f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId:args.requestId}});
  f.planner().server.removeListener('request',loseResponse);
  assert.equal(dropped,true);assert.ok(unknown.status>=500);
  const committed=count();assert.equal(committed.grants,before.grants);assert.equal(committed.anchors,before.anchors);
  assert.equal(committed.aliases,before.aliases+1);assert.equal(committed.receipts,before.receipts+1);
  const pending=await f.reader.client.extension('apps.scoped.context',{appId:f.appId,handle:value.scopedCloseHandle});
  assert.equal(pending.sourceSession,undefined,'committed Source alias alone is not a Root ACK');
  await f.restartSource();
  const recovered=await f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId:args.requestId}});
  assert.equal(recovered.status,200);assert.equal(recovered.body?.ready,true);assert.deepEqual(count(),committed);
  assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,200);
  assert.equal((await f.reader.client.extension('apps.scoped.context',{appId:f.appId,handle:value.scopedCloseHandle})).sourceSession?.ready,true);
});

test('actual Source popup login HTML hashes browser-normalized inline script bytes on Windows too', {skip:!renewalSourceAvailable},async t=>{
  const f=await createScopedGatewayFixture({t,renewal:true});await f.launch();let evidence=null;
  const inspectOwnSourceResponse=(req,res)=>{
    if(req.url!=='/embed')return;const end=res.end;
    res.end=function(chunk,...rest){
      const script=/<script>([\s\S]*?)<\/script>/.exec(String(chunk))?.[1];
      if(script){const browserBytes=script.replace(/\r\n?/gu,'\n'),digest=createHash('sha256').update(browserBytes).digest('base64');
        evidence={matches:String(res.getHeader('content-security-policy')).includes("'sha256-"+digest+"'"),hasCr:script.includes('\r')};}
      return end.call(this,chunk,...rest);
    };
  };
  f.planner().server.prependListener('request',inspectOwnSourceResponse);
  const login=await f.wire.request(f.embedded+'/embed');assert.equal(login.status,200);
  f.planner().server.removeListener('request',inspectOwnSourceResponse);
  assert.deepEqual(evidence,{matches:true,hasCr:false});
});

test('actual Basic300 ACK confirms only current alias/slot with no renewal head or new grant', {skip:!renewalSourceAvailable},async t=>{
  const f=await createScopedGatewayFixture({t,renewal:true}),initial=await f.launch();
  assert.equal((await f.nativeConsent(await f.login(f.reader,false))).status,200);
  const counts=()=>({aliases:f.planner().store.db.prepare("SELECT count(*) AS n FROM planner_soty_private WHERE kind='session'").get().n,
    anchors:f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_rp_anchors').get().n,
    grants:f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_grants').get().n,
    receipts:f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_rebind_receipts').get().n});
  const before=counts(),requestId=random(),read=await f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId}});
  assert.equal(read.status,200);assert.equal(read.body?.ready,true);assert.equal(read.body?.renewable,false);
  assert.equal(read.body?.accessExpiresAt,read.body?.sessionExpiresAt);assert.ok(read.body?.sessionExpiresAt<=initial.scopedSlotExpiresAt);
  assert.deepEqual(counts(),before);assert.equal(before.anchors,0);assert.equal(before.receipts,0);
  const context=await f.reader.client.extension('apps.scoped.context',{appId:f.appId,handle:initial.scopedCloseHandle});
  assert.equal(context.sourceSession?.ready,true);assert.equal(context.sourceSession?.renewable,false);
  assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,200);
  assert.equal((await f.wire.request(f.native+'/soty/disconnect',{fields:{}})).status,200);
  assert.equal((await f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId:random()}})).status,403);
});

test('actual Basic300 cannot restore cookie onto a fresh/foreign Root slot or after original Root RAM loss', {skip:!renewalSourceAvailable},async t=>{
  const f=await createScopedGatewayFixture({t,renewal:true});await f.launch();
  assert.equal((await f.nativeConsent(await f.login(f.reader,false))).status,200);
  const next=await f.reader.client.extension('apps.launch',{appId:f.appId}),denied=await continuation(f,next);
  assert.equal(denied.response.status,200);assert.equal(denied.response.body?.ready,false);
  assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,401);
  await f.restartSource();await f.restartRoot();
  await until(async()=>{try{return(await f.reader.client.extension('apps.list')).apps.find(app=>app.id===f.appId)?.state==='ready';}catch{return false;}},60000);
  const relaunched=await f.reader.client.extension('apps.launch',{appId:f.appId});assert.equal((await continuation(f,relaunched)).response.body?.ready,false);
  assert.equal(f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_rp_anchors').get().n,0);
});
