import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {createScopedGatewayFixture,renewalSourceAvailable} from './support/scoped-gateway-fixture.mjs';
import {createClientWithStorage} from '../../connect/browser/client.mjs';

const random=()=>randomBytes(32).toString('base64url');
const deferred=()=>{let resolve;const promise=new Promise(done=>{resolve=done;});return{promise,resolve};};
async function ready(t,{long=true}={}){
  assert.equal(renewalSourceAvailable,true,'Explicit maintained Source renewal package required');
  const f=await createScopedGatewayFixture({t,renewal:true});
  const initial=await f.launch();await f.nativeConsent(await f.login(f.reader,long));
  assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,200);
  return{f,initial};
}
function holdContinuation(f){
  const server=f.planner().server;
  const entered=deferred(),release=deferred();let held=false;
  const listener=(req,res)=>{
    if(held||req.method!=='POST'||req.url!=='/api/embed/session-continue')return;
    const end=res.end;res.end=function(...args){if(held)return end.apply(this,args);held=true;entered.resolve();void release.promise.then(()=>end.apply(this,args));return this;};
  };
  server.prependListener('request',listener);
  return{entered:entered.promise,release:()=>release.resolve(),dispose(){release.resolve();server.removeListener('request',listener);}};
}
async function candidate(f,initial){
  const requestId=random(),value=await f.reader.client.extension('apps.scoped.renew',{appId:f.appId,handle:initial.scopedCloseHandle,requestId});
  const prior=f.wire.cookies.get(new URL(f.embedded).hostname);
  const issue=await f.wire.request(f.embedded+'/_soty/session',{body:{ticket:new URL(value.launchUrl).hash.slice(1)}});
  assert.equal(issue.status,200);
  assert.equal(f.wire.cookies.get(new URL(f.embedded).hostname)===prior,true,'Hidden candidate must not overwrite the running frame cookie before Source ACK');
  assert.equal(issue.headers['set-cookie']===undefined,true);
  return{value,issue,requestId,prior,headers:{'x-soty-boot-check':issue.body.sessionCheck}};
}

test('actual two-frame cookie jar stays on the running session until deferred maintained Source ACK, then switches once', {timeout:90000},async t=>{
  const{f,initial}=await ready(t),next=await candidate(f,initial),gate=holdContinuation(f);t.after(()=>gate.dispose());
  assert.equal((await f.wire.request(f.embedded+'/_soty/session',{headers:next.headers})).status,200);
  const pending=f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId:next.requestId},headers:next.headers});
  await gate.entered;
  assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,200,'The original visible frame still uses its own valid Source alias');
  assert.equal(f.wire.cookies.get(new URL(f.embedded).hostname)===next.prior,true);
  const current=await f.reader.client.extension('apps.scoped.context',{appId:f.appId,handle:next.value.scopedCloseHandle});
  assert.equal(current.sourceSession===undefined,true,'Neither candidate metadata nor a committed Source row is a Root ACK');
  gate.release();const ack=await pending;const errorCode=ack.body?.error?.code??ack.body?.error;
  assert.equal(ack.status,200,typeof errorCode==='string'&&/^[a-z_]{1,80}$/.test(errorCode)?errorCode:'Closed Source ACK expected');assert.equal(ack.body.ready,true);
  assert.equal(Array.isArray(ack.headers['set-cookie']),true);
  assert.equal(f.wire.cookies.get(new URL(f.embedded).hostname)===next.prior,false);
  assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,200);
  const confirmed=await f.reader.client.extension('apps.scoped.context',{appId:f.appId,handle:next.value.scopedCloseHandle});
  assert.equal(confirmed.sourceSession.ready,true);
});

test('candidate proof is bound to the original browser cookie and only boot-check/continuation, never a write or a second ticket mint', {timeout:90000},async t=>{
  const{f,initial}=await ready(t),next=await candidate(f,initial);
  for(const path of ['/api/embed/state','/api/embed/entities','/api/embed/login']){
    const denial=await f.wire.request(f.embedded+path,{...(path.endsWith('/entities')?{body:{workspaceId:f.workspaceId,title:'Must not create'}}:{}),headers:next.headers});
    assert.equal(denial.status,403);
  }
  assert.equal((await f.wire.request(f.embedded+'/_soty/session',{headers:next.headers,cookie:''})).status,403);
  assert.equal((await f.wire.request(f.embedded+'/_soty/session',{headers:{'x-soty-boot-check':random()}})).status,403);
  const issued=await f.reader.client.extension('apps.scoped.renew',{appId:f.appId,handle:initial.scopedCloseHandle,requestId:random()});
  const body={ticket:new URL(issued.launchUrl).hash.slice(1)};
  assert.equal((await f.wire.request(f.embedded+'/_soty/session',{body,headers:next.headers})).status,403);
  assert.equal((await f.wire.request(f.embedded+'/_soty/session',{body})).status,200,'Rejected header did not consume or mint from the ticket');
  assert.equal(f.planner().store.read().entities.filter(e=>e.title==='Must not create').length,0);
  assert.equal(f.wire.cookies.get(new URL(f.embedded).hostname)===next.prior,true);
});

test('Basic300 cannot become a new current session from candidate metadata or ready:false Source ACK', {timeout:90000},async t=>{
  const{f,initial}=await ready(t,{long:false}),next=await candidate(f,initial);
  const response=await f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId:next.requestId},headers:next.headers});
  assert.equal(response.status,200);assert.equal(response.body.ready,false);assert.equal(response.headers['set-cookie']===undefined,true);
  assert.equal(f.wire.cookies.get(new URL(f.embedded).hostname)===next.prior,true);
  assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,200);
});

for(const[action,close]of[
  ['candidate close',(f,initial,next)=>f.reader.client.extension('apps.scoped.close',{appId:f.appId,handle:next.value.scopedCloseHandle})],
  ['original slot close',(f,initial)=>f.reader.client.extension('apps.scoped.close',{appId:f.appId,handle:initial.scopedCloseHandle})],
  ['current app grant revoke',f=>f.owner.client.extension('apps.update',{appId:f.appId,grants:{accountIds:[],communityIds:[]}})],
])test('deferred actual Source ACK cannot promote after '+action,{timeout:90000},async t=>{
  const{f,initial}=await ready(t),next=await candidate(f,initial),gate=holdContinuation(f);t.after(()=>gate.dispose());
  const pending=f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId:next.requestId},headers:next.headers});await gate.entered;
  await close(f,initial,next);gate.release();const response=await pending;
  assert.equal(response.status>=400,true);assert.equal(response.headers['set-cookie']===undefined,true);
  assert.equal(f.wire.cookies.get(new URL(f.embedded).hostname)===next.prior,true);
});

test('a late successful Source ACK cannot overwrite a newer promoted candidate for the same original browser family', {timeout:90000},async t=>{
  const{f,initial}=await ready(t),first=await candidate(f,initial),second=await candidate(f,initial),gate=holdContinuation(f);t.after(()=>gate.dispose());
  const late=f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId:first.requestId},headers:first.headers});await gate.entered;
  const winner=await f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId:second.requestId},headers:second.headers});
  assert.equal(winner.status,200);assert.equal(winner.body.ready,true);const cookie=f.wire.cookies.get(new URL(f.embedded).hostname);
  gate.release();const denied=await late;assert.equal(denied.status>=400,true);assert.equal(denied.headers['set-cookie']===undefined,true);
  assert.equal(f.wire.cookies.get(new URL(f.embedded).hostname)===cookie,true);
  await assert.rejects(f.reader.client.extension('apps.scoped.context',{appId:f.appId,handle:first.value.scopedCloseHandle}),error=>error.code==='app_scoped_context_closed');
  assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,200,'Late rejection cannot tear down the shared installed channel');
});

test('actual current device revocation prevents a deferred candidate from becoming the primary cookie', {timeout:90000},async t=>{
  const{f,initial}=await ready(t),next=await candidate(f,initial),gate=holdContinuation(f);t.after(()=>gate.dispose());
  const storage={value:null,async read(){return structuredClone(this.value);},async claim(value){this.value??=structuredClone(value);return structuredClone(this.value);},
    async compareAndSwap(revision,value){assert.equal(this.value.localRevision,revision);this.value=structuredClone(value);return structuredClone(this.value);}};
  const sibling=createClientWithStorage({projectId:'soty',endpoint:f.backendOrigin+'/api/connect/rpc',fetch:f.reader.fetch},storage);t.after(()=>sibling.dispose());
  const enrollment=await sibling.startEnrollment('Synthetic device revoke gate');await f.reader.client.approveEnrollment(enrollment.requestId,f.reader.account.accountId);
  await sibling.previewEnrollment(enrollment.requestId);await sibling.finishEnrollment(enrollment.requestId,f.reader.account.accountId);
  const pending=f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId:next.requestId},headers:next.headers});await gate.entered;
  await sibling.revokeDevice(f.reader.account.deviceId);gate.release();const response=await pending;
  assert.equal(f.root().locals.connectService.isActorActive(f.reader.account),false);assert.equal(response.status>=400,true);
  assert.equal(response.headers['set-cookie']===undefined,true);assert.equal(f.wire.cookies.get(new URL(f.embedded).hostname)===next.prior,true);
});

test('a removed current approved Source profile cannot promote a deferred candidate across Root restart', {timeout:90000},async t=>{
  const{f,initial}=await ready(t),next=await candidate(f,initial),gate=holdContinuation(f);t.after(()=>gate.dispose());
  const pending=f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId:next.requestId},headers:next.headers});await gate.entered;
  await f.restartRoot({scopedEmbedProfiles:[]});gate.release();const response=await pending;
  assert.equal(response.status>=400,true);assert.equal(response.headers['set-cookie']===undefined,true);
  assert.equal(f.wire.cookies.get(new URL(f.embedded).hostname)===next.prior,true);
});

test('missing Origin and caller ready:true do not promote or create a Source receipt', {timeout:90000},async t=>{
  const{f,initial}=await ready(t),next=await candidate(f,initial);
  const before=f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_rebind_receipts').get().n;
  for(const options of[{origin:'https://foreign.invalid',body:{requestId:next.requestId}},{body:{requestId:next.requestId,ready:true}}]){
    const response=await f.wire.request(f.embedded+'/api/embed/session-continue',{...options,headers:next.headers});
    assert.equal(response.status>=400,true);assert.equal(response.headers['set-cookie']===undefined,true);
  }
  assert.equal(f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_rebind_receipts').get().n,before);
  assert.equal(f.wire.cookies.get(new URL(f.embedded).hostname)===next.prior,true);
});

test('expired candidate routing proof fails before Source admission after real bounded 30-second boot lifetime', {timeout:90000},async t=>{
  const{f,initial}=await ready(t),next=await candidate(f,initial);
  const before=f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_rebind_receipts').get().n;
  await new Promise(done=>setTimeout(done,31000));
  const response=await f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId:next.requestId},headers:next.headers});
  assert.equal(response.status,403);assert.equal(response.headers['set-cookie']===undefined,true);
  assert.equal(f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_rebind_receipts').get().n,before);
  assert.equal(f.wire.cookies.get(new URL(f.embedded).hostname)===next.prior,true);
});

test('unknown Source ACK keeps the original primary cookie; same candidate/intent reads the durable receipt once after Source restart', {timeout:90000},async t=>{
  const{f,initial}=await ready(t),next=await candidate(f,initial),server=f.planner().server;let dropped=false;
  const counts=()=>({receipts:f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_rebind_receipts').get().n,
    aliases:f.planner().store.db.prepare("SELECT count(*) AS n FROM planner_soty_private WHERE kind='session'").get().n,
    grants:f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_grants').get().n});
  const before=counts(),lose=(req,res)=>{if(req.url!=='/api/embed/session-continue'||dropped)return;
    const end=res.end;res.end=function(chunk,...args){let value;try{value=JSON.parse(String(chunk));}catch{}
      if(!dropped&&value?.ready===true){dropped=true;res.socket?.destroy();return this;}return end.call(this,chunk,...args);};};
  server.prependListener('request',lose);t.after(()=>server.removeListener('request',lose));
  const unknown=await f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId:next.requestId},headers:next.headers});server.removeListener('request',lose);
  assert.equal(dropped,true);assert.equal(unknown.status>=500,true);assert.equal(unknown.headers['set-cookie']===undefined,true);
  assert.equal(f.wire.cookies.get(new URL(f.embedded).hostname)===next.prior,true);
  const committed=counts();assert.equal(committed.receipts,before.receipts+1);assert.equal(committed.aliases,before.aliases+1);assert.equal(committed.grants,before.grants);
  await f.restartSource();
  const replay=await f.reader.client.extension('apps.scoped.renew',{appId:f.appId,handle:initial.scopedCloseHandle,requestId:next.requestId});
  const issue=await f.wire.request(f.embedded+'/_soty/session',{body:{ticket:new URL(replay.launchUrl).hash.slice(1)}});
  assert.equal(issue.status,200);assert.equal(issue.body.sessionCheck===next.issue.body.sessionCheck,true);assert.equal(issue.headers['set-cookie']===undefined,true);
  const recovered=await f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId:next.requestId},headers:next.headers});
  assert.equal(recovered.status,200);assert.equal(recovered.body.ready,true);assert.deepEqual(counts(),committed);
  assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,200);
});

test('lost browser response after promotion replays the same confirmed cookie, not another Source grant or family head', {timeout:90000},async t=>{
  const{f,initial}=await ready(t),next=await candidate(f,initial);let dropped=false;
  const lose=(req,res)=>{if(dropped||req.url!=='/api/embed/session-continue')return;
    const end=res.end;res.end=function(chunk,...args){if(res.hasHeader('set-cookie')&&!dropped){dropped=true;res.destroy();return this;}return end.call(this,chunk,...args);};};
  f.server.prependListener('request',lose);t.after(()=>f.server.removeListener('request',lose));
  const unknown=f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId:next.requestId},headers:next.headers});
  await assert.rejects(unknown);f.server.removeListener('request',lose);assert.equal(dropped,true);
  const cookie=f.wire.cookies.get(new URL(f.embedded).hostname);
  const before=f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_rebind_receipts').get().n;
  const replay=await f.wire.request(f.embedded+'/api/embed/session-continue',{body:{requestId:next.requestId},headers:next.headers});
  assert.equal(replay.status,200);assert.equal(replay.body.ready,true);
  assert.equal(f.wire.cookies.get(new URL(f.embedded).hostname)===cookie,false);
  assert.equal(f.planner().store.db.prepare('SELECT count(*) AS n FROM planner_soty_rebind_receipts').get().n,before);
  assert.equal((await f.wire.request(f.embedded+'/api/embed/state')).status,200);
});
