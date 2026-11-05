import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createAppLauncher} from './app-launch.mjs';
import {createAppHostSessionRenewal} from './app-host-session-renewal.mjs';

// Real launcher/controller with an explicit protocol model. This is not Root,
// browser-cookie or Source authority acceptance; the backend has its own gates.
const appId='app-'+ 'a'.repeat(32),domainId='dom_'+ 'b'.repeat(32);
const instanceId='widget-11111111-1111-4111-8111-111111111111';
const origin='https://app-w11111111111141118111111111111111-'+ 'c'.repeat(24)+'.apps.example';
const entry={appId,domainId,origin:'https://reader.apps.example',path:'/reader?page=2'};
const pin={schema:'soty.app-launch-binding.v1',policyEpoch:1,targetRevision:2,targetDigest:'d'.repeat(64),profile:'soty.relay-restricted.v1',bindingFloor:2};
const clone=value=>structuredClone(value),error=(code,status)=>Object.assign(Error(code),{code,status});
const boot=(renew=false)=>origin+'/_soty/boot?path='+encodeURIComponent(entry.path)+(renew?'&mode=renew':'')+'#'+'T'.repeat(43);

async function fixture(options={}){
 let time=1800000000000,epoch=0,ids=0,candidate=null,checked=false,http=!options.reservation,currentId=null,override=null,admitted=!options.reservation;
 let member={schema:'soty.app-host-session.v1',frameId:'F'.repeat(43),lease:'A'.repeat(43),revision:1,expiresAt:time+(options.memberExpiresIn??(options.reservation?30000:86400000))};
 let httpUntil=Math.min(time+(options.httpExpiresIn??120000),member.expiresAt);
 const calls=[],states=[],timers=new Map();let timerId=0;
 const same=args=>args.frameId===member.frameId&&args.lease===member.lease&&args.revision===member.revision;
 const dto=(proof,extra={})=>({hostSession:clone(proof),hostSessionExpiresAt:httpUntil,entry:clone(entry),launchBinding:clone(pin),runtimeOrigin:origin,...extra});
 const launcher=createAppLauncher({target:{appId,domainId,path:entry.path},accountId:'owner',instanceId,shellUrl:'https://soty.example/#mine',isCurrent:()=>epoch===0,
  request:async args=>{assert.equal(args.hostSession,true);return{url:boot(),...dto(member),...(options.initialCleanup?{hostSessionCleanup:options.initialCleanup}:{})};}});
 await launcher.launch();
 const context=async args=>{
  calls.push({kind:'context',args:clone(args)});assert.equal(args.currentEntryPath,entry.path);
  if(options.context)return options.context(args,{member,candidate,dto});
  if(!same(args))throw error('app_host_session_conflict',409);
  return dto(member,{schema:'soty.app-host-session-context.v1',state:!admitted?'admitting':http?'current':'renew-required',hostSessionExpiresAt:admitted&&http?httpUntil:null});
 };
 const request=async args=>{
  calls.push({kind:'request',args:clone(args)});assert.equal(args.parentOrigin,'https://soty.example');assert.equal(args.currentEntryPath,entry.path);
  assert.equal(admitted,true,'renewal must not overtake the initial cookie admission');
  assert.ok(same(args),'only the current explicit member may prepare');
  if(!candidate){candidate={...member,lease:String.fromCharCode(66+member.revision).repeat(43),revision:member.revision+1,expiresAt:time+86400000};currentId=args.requestId;checked=false;}
  assert.equal(args.requestId,currentId,'a pending candidate uses its original correlation');
  if(options.request)await options.request(args,{member,candidate});
  const prepared=dto(candidate,{launchUrl:boot(true),expiresAt:time+30000,hostSessionRequestId:args.requestId,hostSessionExpiresAt:time+120000});
  return options.reply?options.reply(prepared):prepared;
 };
 const confirm=async args=>{
  calls.push({kind:'confirm',args:clone(args)});assert.equal(args.requestId,currentId);
  if(candidate&&args.lease===candidate.lease&&checked){member=candidate;candidate=null;http=true;httpUntil=time+120000;}
  else if(!same(args)||!http)throw error('app_host_session_unconfirmed',409);
  if(options.confirm)await options.confirm(args,{member,restart(){http=false;}});
  return dto(member,{schema:'soty.app-host-session-confirmation.v1',requestId:args.requestId,confirmed:true});
 };
 const ports={readBinding:()=>override??launcher.hostSession(),isCurrent:()=>epoch===0,shellUrl:'https://soty.example/#mine',context,request,confirm,
  bootstrap:async(reply,{signal})=>{calls.push({kind:'bootstrap'});if(options.bootstrap)return options.bootstrap(reply,{signal,verify(){checked=true;}});checked=true;},
  commit:(before,value)=>{const {cleanup,...wire}=value;calls.push({kind:'commit',value:clone(wire)});return launcher.commitHostSession(before,value);},
  close:async args=>{calls.push({kind:'close',args:clone(args)});return{closed:true};},onState:(state,reason)=>states.push({state,code:reason?.code}),
  ...(options.cleanupForReply?{cleanupForReply:options.cleanupForReply}:{}),
  clock:()=>time,randomId:()=>`correlation-${++ids}-0123456789`,schedule:(fn,ms)=>{const id=++timerId;timers.set(id,{fn,at:time+ms});return id;},cancel:id=>timers.delete(id)};
 const helper=(options.create??createAppHostSessionRenewal)(ports);
 return{helper,launcher,calls,states,timers,ports,member:()=>member,binding:()=>launcher.hostSession(),
  retire(){epoch++;},replaceSlot(){override={...launcher.hostSession(),slot:{}};},restart(){http=false;},verify(){checked=true;},refreshHttpDeadline(){httpUntil=time+120000;},
  admit(){admitted=true;member={...member,expiresAt:time+86400000};http=true;httpUntil=time+120000;},
  advance(ms){time+=ms;},fire(){const next=[...timers].sort((a,b)=>a[1].at-b[1].at)[0];assert.ok(next);timers.delete(next[0]);time=next[1].at;next[1].fn();},
  dispose(){helper.dispose();launcher.dispose();}};
}

test('reset confirms only a new ordinary host transport and keeps canonical/runtime binding',async t=>{
 const f=await fixture();t.after(()=>f.dispose());const old=f.binding();f.restart();
 assert.equal(await f.helper.renew('reset'),true);assert.equal(f.binding().hostSession.revision,2);assert.equal(f.binding().slot,old.slot);
 assert.deepEqual(f.binding().entry,entry);assert.equal(f.binding().runtimeOrigin,origin);
 assert.deepEqual(f.calls.map(x=>x.kind),['context','commit','request','bootstrap','confirm','commit']);assert.equal(f.calls[1].value.hostSessionExpiresAt,null,'the context metadata update is not a transport confirmation');
 assert.equal(f.calls.some(x=>/source|notes|POST/iu.test(x.kind)),false);
});

test('near-expiry scheduling stays single-timer and does not poll remote state while the lease is far away',async t=>{
 const f=await fixture();t.after(()=>f.dispose());f.helper.start();assert.equal(f.timers.size,1);f.helper.start();assert.equal(f.timers.size,1);
 assert.equal(await f.helper.renew('near-expiry'),true);assert.equal(f.calls.length,0);
 f.advance(91000);assert.equal(await f.helper.renew('near-expiry'),true);assert.equal(f.calls.filter(x=>x.kind==='context').length,1);
 assert.equal(f.calls.filter(x=>x.kind==='request').length,1,'near expiry requires actual renewal, not a deadline extended by the context read');assert.equal(f.timers.size,1);
});

test('scheduled retry remembers a reset that hit app_offline despite the stale one-hour HTTP deadline',async t=>{
 let attempts=0;
 const f=await fixture({httpExpiresIn:3600000,context:async(args,{member,dto})=>{
  if(++attempts===1)throw error('app_offline',400);
  assert.equal(args.lease,member.lease);assert.equal(args.revision,member.revision);
  return dto(member,{schema:'soty.app-host-session-context.v1',state:'renew-required',hostSessionExpiresAt:null});
 }});t.after(()=>f.dispose());const initial=f.binding();f.helper.start();f.restart();
 assert.equal(initial.hostSessionExpiresAt-f.ports.clock(),3600000);
 await assert.rejects(f.helper.renew('reset'),{code:'app_offline'});
 assert.equal(f.calls.filter(value=>value.kind==='context').length,1);assert.equal(f.calls.some(value=>value.kind==='request'),false);
 assert.equal(f.states.at(-1).state,'unavailable');assert.equal(f.timers.size,1);
 f.fire();
 for(let turn=0;turn<5&&f.binding().hostSession.revision!==2;turn++)await new Promise(resolve=>setImmediate(resolve));
 assert.equal(f.calls.filter(value=>value.kind==='context').length,2,'a failed reset cannot be skipped by the cached old HTTP deadline');
 assert.equal(f.calls.filter(value=>value.kind==='request').length,1);assert.equal(f.calls.filter(value=>value.kind==='bootstrap').length,1);
 assert.equal(f.calls.filter(value=>value.kind==='confirm').length,1);assert.equal(f.binding().hostSession.revision,2);
 assert.equal(f.binding().slot,initial.slot,'host recovery keeps the existing application slot');assert.equal(f.timers.size,1);
});

test('start captures an initial member that was not available when the controller was constructed',async()=>{
 let mounted=false;const f=await fixture({create:ports=>createAppHostSessionRenewal({...ports,readBinding:()=>mounted?ports.readBinding():null})});
 const first=f.binding().hostSession;mounted=true;f.helper.start();f.dispose();await new Promise(resolve=>setImmediate(resolve));
 assert.ok(f.calls.some(value=>value.kind==='close'&&value.args.lease===first.lease&&value.args.revision===first.revision));
});

test('a genuinely refreshed actual HTTP deadline prevents a tight context-poll loop',async t=>{
 const f=await fixture();t.after(()=>f.dispose());f.helper.start();f.advance(91000);f.refreshHttpDeadline();
 assert.equal(await f.helper.renew('near-expiry'),true);assert.equal(await f.helper.renew('near-expiry'),true);
 assert.equal(f.calls.filter(x=>x.kind==='context').length,1);assert.equal(f.calls.filter(x=>x.kind==='request').length,0);assert.equal(f.timers.size,1);
});

test('an explicitly admitting initial reservation never starts a competing renewal and refreshes only its same proof after real admission',async t=>{
 const f=await fixture({reservation:true});t.after(()=>f.dispose());const original=f.binding().hostSession;
 assert.equal(await f.helper.renew('reset'),false);assert.equal(f.calls.some(value=>value.kind==='request'),false);assert.equal(f.binding().hostSessionExpiresAt,null);
 f.admit();assert.equal(await f.helper.renew('near-expiry'),true);assert.equal(f.calls.some(value=>value.kind==='request'),false);
 assert.equal(f.binding().hostSession.lease,original.lease);assert.equal(f.binding().hostSession.revision,original.revision);assert.ok(f.binding().hostSession.expiresAt>original.expiresAt);assert.ok(f.binding().hostSessionExpiresAt>0);
});

test('an admitted revision-one lease near its final deadline is not misidentified as an initial reservation',async t=>{
 const f=await fixture({memberExpiresIn:20000});t.after(()=>f.dispose());f.restart();assert.equal(await f.helper.renew('reset'),true);assert.equal(f.member().revision,2);
});

test('ordinary confirmation does not depend on an optional wakeup message or Source SDK ACK',async t=>{
 const f=await fixture({bootstrap:async(_reply,{verify})=>{verify();throw Error('wakeup lost');}});t.after(()=>f.dispose());
 assert.equal(await f.helper.renew('reset'),true);assert.equal(f.calls.filter(x=>x.kind==='confirm').length,1);
});

async function unconfirmedScenario(create=createAppHostSessionRenewal){
 const f=await fixture({create,bootstrap:async()=>{}});
 try{assert.equal(await f.helper.renew('reset'),false,'a message without a real server cookie check must not confirm');
  assert.equal(f.binding().hostSession.revision,1,'unconfirmed candidate must not become the current lease');return f.calls;
 }finally{f.dispose();}
}
test('an unverified cookie check cannot become a successful host session',()=>unconfirmedScenario());

test('retry retains the same original request/candidate and may reissue only its one-use Root bootstrap',async t=>{
 let boots=0;const f=await fixture({bootstrap:async(_reply,{verify})=>{if(++boots>1)verify();}});t.after(()=>f.dispose());
 assert.equal(await f.helper.renew('reset'),false);assert.equal(await f.helper.renew('reset'),true);
 const requests=f.calls.filter(x=>x.kind==='request');assert.equal(requests.length,2);assert.equal(requests[0].args.requestId,requests[1].args.requestId);assert.equal(f.member().revision,2);
});

test('three transient boot failures do not strand a retained window after the network resumes',async t=>{
 let boots=0;const f=await fixture({bootstrap:async(_reply,{verify})=>{if(++boots>3)verify();}});t.after(()=>f.dispose());f.helper.start();
 assert.equal(await f.helper.renew('reset'),false);
 for(let step=0;step<2;step++){assert.equal(f.timers.size,1);f.fire();assert.equal(await f.helper.renew('near-expiry'),false);}
 assert.equal(boots,3);assert.equal(f.timers.size,1);f.fire();assert.equal(await f.helper.renew('near-expiry'),true);
 const requests=f.calls.filter(value=>value.kind==='request');assert.equal(requests.length,4);assert.equal(new Set(requests.map(value=>value.args.requestId)).size,1);assert.equal(f.member().revision,2);
});

test('lost prepare ACK retries its original correlation instead of creating another candidate',async t=>{
 let attempts=0;const f=await fixture({request:async()=>{if(++attempts===1)throw Error('network offline');}});t.after(()=>f.dispose());
 await assert.rejects(f.helper.renew('reset'),/network offline/u);assert.equal(await f.helper.renew('reset'),true);
 const requests=f.calls.filter(x=>x.kind==='request');assert.equal(requests[0].args.requestId,requests[1].args.requestId);assert.equal(f.member().revision,2);
});

test('lost confirm ACK is read back by exact idempotent confirm without a second bootstrap',async t=>{
 let attempts=0;const f=await fixture({confirm:async()=>{if(++attempts===1)throw Error('confirmation ACK lost');}});t.after(()=>f.dispose());
 await assert.rejects(f.helper.renew('reset'),/ACK lost/u);assert.equal(f.member().revision,2);assert.equal(f.binding().hostSession.revision,1);
 assert.equal(await f.helper.renew('reset'),true);assert.equal(f.binding().hostSession.revision,2);assert.equal(f.calls.filter(x=>x.kind==='bootstrap').length,1);
});

test('confirm COMMIT plus lost ACK and API restart adopts only its own proved current lease, never fake HTTP readiness',async t=>{
 let attempts=0;const f=await fixture({confirm:async(_args,{restart})=>{if(++attempts===1){restart();throw Error('ACK lost before API restart');}}});t.after(()=>f.dispose());
 await assert.rejects(f.helper.renew('reset'));assert.equal(await f.helper.renew('reset'),false);
 assert.equal(f.binding().hostSession.revision,2);assert.equal(f.binding().hostSessionExpiresAt,null,'durable proof is not a ready transport');
 assert.equal(f.states.at(-1).state,'pending');assert.equal(await f.helper.renew('reset'),true);assert.equal(f.binding().hostSession.revision,3);
 assert.notEqual(f.calls.filter(x=>x.kind==='request')[0].args.requestId,f.calls.filter(x=>x.kind==='request').at(-1).args.requestId);
});

test('a current signed context cannot smuggle a different successor into this member',async t=>{
 const f=await fixture({context:async(_args,{member,dto})=>dto({...member,lease:'Z'.repeat(43),revision:member.revision+1},{schema:'soty.app-host-session-context.v1',state:'current'})});t.after(()=>f.dispose());
 await assert.rejects(f.helper.renew('reset'),{code:'app_host_session_changed'});assert.equal(f.binding().hostSession.revision,1);assert.equal(f.calls.filter(x=>x.kind==='request').length,0);
});

test('target, policy, path, runtime, member and expanded DTO mismatches never reach the hidden carrier',async t=>{
 for(const change of [value=>({...value,launchBinding:{...pin,targetDigest:'e'.repeat(64)}}),value=>({...value,launchBinding:{...pin,policyEpoch:2}}),
  value=>({...value,entry:{...entry,path:'/other'}}),value=>({...value,runtimeOrigin:origin.replace('c'.repeat(24),'e'.repeat(24))}),
  value=>({...value,hostSession:{...value.hostSession,frameId:'Q'.repeat(43)}}),value=>({...value,sourceReady:true}),value=>({...value,ok:false})]){
  const f=await fixture({reply:change});t.after(()=>f.dispose());await assert.rejects(f.helper.renew('reset'));assert.equal(f.calls.filter(x=>x.kind==='bootstrap').length,0);assert.equal(f.binding().hostSession.revision,1);
 }
});

test('a held old-account bootstrap cannot promote a lease into a new account generation',async t=>{
 let release,begun;const start=new Promise(resolve=>{begun=resolve;}),gate=new Promise(resolve=>{release=resolve;});
 const f=await fixture({bootstrap:async(_reply,{verify})=>{begun();await gate;verify();}});t.after(()=>f.dispose());
 const pending=f.helper.renew('reset');await start;f.retire();release();assert.equal(await pending,false);assert.equal(f.calls.filter(x=>x.kind==='commit').length,0);
 await new Promise(resolve=>setImmediate(resolve));assert.ok(f.calls.some(x=>x.kind==='close'&&x.args.revision===2));
});

test('a replaced Root slot rejects a late bootstrap even if the account and app still match',async t=>{
 let release,begun;const start=new Promise(resolve=>{begun=resolve;}),gate=new Promise(resolve=>{release=resolve;});
 const f=await fixture({bootstrap:async(_reply,{verify})=>{begun();await gate;verify();}});t.after(()=>f.dispose());
 const pending=f.helper.renew('reset');await start;f.replaceSlot();release();assert.equal(await pending,false);assert.equal(f.calls.filter(x=>x.kind==='commit').length,0);
});

test('concurrent reset and near-expiry triggers share one current operation',async t=>{
 let release,begun;const start=new Promise(resolve=>{begun=resolve;}),gate=new Promise(resolve=>{release=resolve;});
 const f=await fixture({bootstrap:async(_reply,{verify})=>{begun();await gate;verify();}});t.after(()=>f.dispose());
 const one=f.helper.renew('reset');await start;const two=f.helper.renew('near-expiry');release();assert.deepEqual(await Promise.all([one,two]),[true,true]);assert.equal(f.calls.filter(x=>x.kind==='request').length,1);
});

test('dispose aborts its carrier, removes its timer and closes only its known physical member/candidate',async()=>{
 let begun;const start=new Promise(resolve=>{begun=resolve;});
 const f=await fixture({bootstrap:async(_reply,{signal})=>{begun();await new Promise(resolve=>signal.addEventListener('abort',resolve,{once:true}));}});
 f.helper.start();const pending=f.helper.renew('reset');await start;f.dispose();assert.equal(await pending,false);assert.equal(f.timers.size,0);
 await new Promise(resolve=>setImmediate(resolve));const closes=f.calls.filter(x=>x.kind==='close');assert.ok(closes.length>=2);assert.ok(closes.every(x=>x.args.frameId==='F'.repeat(43)));
});

test('authoritative denial stops periodic attempts instead of falling back to anonymous or a new ID',async t=>{
 const f=await fixture({context:async()=>{throw error('app_host_session_closed',403);}});t.after(()=>f.dispose());f.helper.start();
 await assert.rejects(f.helper.renew('reset'),{code:'app_host_session_closed'});assert.equal(f.timers.size,0);assert.equal(await f.helper.renew('reset'),false);assert.equal(f.calls.length,1);
});

test('Connect identity-loss codes remain authoritative even when the RPC envelope uses HTTP400',async t=>{
 const f=await fixture({context:async()=>{throw error('device_revoked',400);}});t.after(()=>f.dispose());f.helper.start();
 await assert.rejects(f.helper.renew('reset'),{code:'device_revoked'});assert.equal(f.states.at(-1).state,'denied');assert.equal(f.timers.size,0);assert.equal(await f.helper.renew('reset'),false);
});

test('signed same-proof metadata refresh replaces only the private close purpose and keeps it outside wire state',async()=>{
 const closes=[],initial=async()=>{closes.push('initial');},fresh=async()=>{closes.push('fresh');};
 const f=await fixture({initialCleanup:initial,cleanupForReply:reply=>reply.hostSession.revision===1?fresh:async()=>{closes.push('candidate');}});
 f.advance(91000);f.refreshHttpDeadline();assert.equal(await f.helper.renew('near-expiry'),true);assert.equal(f.binding().cleanup,fresh);
 assert.equal(f.calls.filter(value=>value.kind==='request').length,0);f.dispose();await new Promise(resolve=>setImmediate(resolve));
 assert.ok(closes.includes('fresh'));assert.equal(closes.includes('initial'),false);assert.equal(f.calls.some(value=>value.kind==='close'),false,'a trusted close never falls back to an active-profile RPC');
 assert.ok(f.calls.filter(value=>value.kind==='commit').every(value=>!Object.hasOwn(value.value,'cleanup')));
});

test('a failing trusted original-owner close cannot fall back to the newly active identity',async()=>{
 const f=await fixture({initialCleanup:async()=>{throw Error('old credential unavailable');}});f.helper.start();f.dispose();await new Promise(resolve=>setImmediate(resolve));
 assert.equal(f.calls.some(value=>value.kind==='close'),false);
});

test('a confirmation-bypass mutant fails the unchanged no-cookie oracle',async()=>{
 const url=new URL('./app-host-session-renewal.mjs',import.meta.url),source=readFileSync(url,'utf8'),anchor='return await finish(action)===true;';
 assert.equal(source.split(anchor).length,2,'mutate precisely the final authoritative confirmation');
 const code=source.replace(anchor,'return adopt(action,action.reply);').replace(/from '(\.?\.?\/[^']+)'/gu,(_match,path)=>"from '"+new URL(path,url).href+"'");
 const broken=await import('data:text/javascript;base64,'+Buffer.from(code).toString('base64'));
 await assert.rejects(unconfirmedScenario(broken.createAppHostSessionRenewal),/message without a real server cookie check/u);
});
