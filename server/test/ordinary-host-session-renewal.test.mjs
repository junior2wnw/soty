import test from 'node:test';
import assert from 'node:assert/strict';
import { request } from 'node:http';
import {DatabaseSync} from 'node:sqlite';
import {join} from 'node:path';
import WebSocket from 'ws';
import { createInvitationFixture } from '../../modules/invitations/test/http-fixture.mjs';

function http(port, origin, path, {method='GET',cookie='',body,headers={}}={}) {
  return new Promise((resolve,reject)=>{
    const req=request({hostname:'127.0.0.1',port,path,method,headers:{host:new URL(origin).host,origin,...(cookie?{cookie}:{}),...(body?{'content-type':'application/json'}:{}),...headers}},res=>{
      const chunks=[];res.on('data',chunk=>chunks.push(chunk));res.on('error',reject);res.on('end',()=>resolve({status:res.statusCode,headers:res.headers,body:Buffer.concat(chunks).toString()}));
    });req.on('error',reject);req.end(body?JSON.stringify(body):undefined);
  });
}
function jar() {
  const values=new Map();
  return {get:()=>[...values].map(([name,value])=>name+'='+value).join('; '),apply(response){for(const field of response.headers['set-cookie']??[]){const part=field.split(';')[0],at=part.indexOf('=');if(/(?:^|;)\s*Max-Age=0(?:;|$)/i.test(field))values.delete(part.slice(0,at));else values.set(part.slice(0,at),part.slice(at+1));}},size:()=>values.size};
}
async function until(check,label,timeout=1800){const end=Date.now()+timeout;while(!check()){assert.ok(Date.now()<end,label);await new Promise(done=>setTimeout(done,10));}}
async function socket(t,f,instance){
  const ws=new WebSocket(`ws://127.0.0.1:${f.port}/live`,{headers:{Host:new URL(instance.origin).host,Origin:instance.origin,Cookie:instance.cookies.get()}}),messages=[];
  ws.on('message',value=>messages.push(value.toString()));ws.on('error',()=>{});t.after(()=>ws.terminate());
  await until(()=>messages.length>0,'actual Source WebSocket initial snapshot');return{ws,messages};
}
async function events(t,f,instance){
  const stream={chunks:[],closed:false};
  stream.req=request({hostname:'127.0.0.1',port:f.port,path:'/events',headers:{Host:new URL(instance.origin).host,Origin:instance.origin,Cookie:instance.cookies.get()}},res=>{
    stream.res=res;stream.status=res.statusCode;stream.contentType=res.headers['content-type'];res.on('data',value=>stream.chunks.push(value.toString()));res.on('close',()=>stream.closed=true);res.on('error',()=>{});
  });stream.req.on('error',()=>stream.closed=true);stream.req.end();t.after(()=>stream.req.destroy());
  await until(()=>stream.chunks.length>0||stream.closed,'actual Source SSE first event');assert.equal(stream.status,200);assert.match(stream.contentType,/text\/event-stream/u);return stream;
}
async function setup(t,options) {
  const f=await createInvitationFixture(options);t.after(()=>f.close());const port=f.server.address().port;
  const wire=[];f.sample.server.on('request',req=>wire.push({method:req.method,path:req.url}));
  const call=(op,args)=>f.call(f.owner,op,{expectedAccountId:f.owner.account.accountId,...args});
  const widgets=[],objects=[];
  for(const name of ['one','two']){
    const widget=(await call('canvas.widgets.create',{requestId:'renew_widget_'+name,appId:f.appId})).current;
    const object=(await call('canvas.objects.create',{requestId:'renew_object_'+name,binding:{providerId:'soty.apps',ref:widget.widgetId},presentation:{mask:{shape:'rect'}},visibility:{audience:'public',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'none'}})).current;objects.push(object);
    widgets.push(widget);
  }
  async function launch(widget,cookies=jar()) {
    const result=await call('apps.launch',{appId:f.appId,hostSession:true,...(widget?{...widget.entry,instanceId:widget.widgetId}:{})}),url=new URL(result.launchUrl),origin=result.runtimeOrigin??result.entry.origin;
    const response=await http(port,origin,'/_soty/session',{method:'POST',body:{ticket:url.hash.slice(1)}});assert.equal(response.status,200);cookies.apply(response);
    const issued=JSON.parse(response.body),checked=await http(port,origin,'/_soty/session',{cookie:cookies.get(),headers:{'x-soty-boot-check':issued.sessionCheck}});assert.equal(checked.status,200);
    return{result,origin,cookies};
  }
  const read=instance=>http(port,instance.origin,'/api/items',{cookie:instance.cookies.get()});
  const prepare=(instance,requestId)=>call('apps.session.renew',{appId:f.appId,frameId:instance.result.hostSession.frameId,lease:instance.result.hostSession.lease,revision:instance.result.hostSession.revision,requestId,parentOrigin:f.origin,currentEntryPath:instance.result.entry.path});
  async function mint(instance,reply,{apply=true,check=true}={}) {
    const url=new URL(reply.launchUrl),response=await http(port,instance.origin,'/_soty/session',{method:'POST',cookie:instance.cookies.get(),body:{ticket:url.hash.slice(1)}});
    assert.equal(response.status,200);if(apply)instance.cookies.apply(response);
    if(check){const value=JSON.parse(response.body);assert.equal((await http(port,instance.origin,'/_soty/session',{cookie:instance.cookies.get(),headers:{'x-soty-boot-check':value.sessionCheck}})).status,200);}
    return response;
  }
  const confirm=(reply,requestId)=>call('apps.session.confirm',{appId:f.appId,frameId:reply.hostSession.frameId,lease:reply.hostSession.lease,revision:reply.hostSession.revision,requestId});
  async function renew(instance,requestId) {
    const current=instance.result.hostSession;
    assert.ok(current,'ordinary signed launch must expose its own passive continuity handle');
    const args={appId:f.appId,frameId:current.frameId,lease:current.lease,revision:current.revision,requestId,parentOrigin:f.origin,currentEntryPath:instance.result.entry.path};
    const reply=await call('apps.session.renew',args),url=new URL(reply.launchUrl);
    assert.equal(url.origin,instance.origin,'renewal keeps the existing browser origin');
    const minted=await http(port,instance.origin,'/_soty/session',{method:'POST',cookie:instance.cookies.get(),body:{ticket:url.hash.slice(1)}});assert.equal(minted.status,200);instance.cookies.apply(minted);
    const issued=JSON.parse(minted.body);
    assert.equal(JSON.stringify(issued).includes(reply.hostSession.lease),false,'Root proof never reaches carrier DTO');
    const checked=await http(port,instance.origin,'/_soty/session',{cookie:instance.cookies.get(),headers:{'x-soty-boot-check':issued.sessionCheck}});assert.equal(checked.status,200);
    const confirmed=await call('apps.session.confirm',{appId:f.appId,frameId:reply.hostSession.frameId,lease:reply.hostSession.lease,revision:reply.hostSession.revision,requestId});
    assert.equal(confirmed.confirmed,true);instance.result={...instance.result,...reply};return{reply,confirmed,minted};
  }
  return{...f,port,call,widgets,objects,launch,read,renew,wire,prepare,mint,confirm};
}

test('actual ordinary HTTP: same plain Source main and two service widgets regain host sessions after API restart', {timeout:30000},async t=>{
  const f=await setup(t),instances=await Promise.all([f.launch(),...f.widgets.map(widget=>f.launch(widget))]);
  for(const instance of instances)assert.equal((await f.read(instance)).status,200);
  const sourceRequests=f.wire.length;
  await f.restart();
  for(const instance of instances)assert.equal((await f.read(instance)).status,403,'present cookie has lost its RAM transport; no fallback authorization');
  for(const [index,instance]of instances.entries()){await f.renew(instance,'own_restart_renew_'+index);assert.equal((await f.read(instance)).status,200);}
  assert.equal(f.wire.slice(sourceRequests).filter(item=>item.method==='GET'&&item.path==='/').length,0,'hidden renewal never reloads the Source document (connector HEAD health probes are separate)');
  assert.equal(f.wire.slice(sourceRequests).filter(item=>item.path==='/api/embed/session-continue').length,0,'ordinary has no Source ACK');
});

test('actual HTTP headers: late R1 cookie cannot overwrite R2 current lease; stale confirm/cleanup cannot retire successor',{timeout:30000},async t=>{
  const f=await setup(t),instance=await f.launch(f.widgets[0]);
  const one=await f.prepare(instance,'held_headers_one_2026'),two=await f.prepare(instance,'held_headers_two_2026');
  const held=await f.mint(instance,one,{apply:false,check:false});
  assert.equal(held.headers['set-cookie'].some(value=>value.startsWith('__Host-soty_app_session=')),false,'renewal never changes shared family selector');
  await f.mint(instance,two);await f.confirm(two,'held_headers_two_2026');
  instance.cookies.apply(held);assert.equal((await f.read(instance)).status,200,'actual current GET survives delivery of the obsolete cookie header');
  const lateCheck=await http(f.port,instance.origin,'/_soty/session',{cookie:instance.cookies.get(),headers:{'x-soty-boot-check':JSON.parse(held.body).sessionCheck}});
  assert.equal(lateCheck.status,409,'stale candidate cannot be checked after a successor became current');
  await assert.rejects(f.confirm(one,'held_headers_one_2026'),error=>error.code==='app_host_session_conflict');
  const old=instance.result.hostSession;
  assert.equal((await f.call('apps.session.close',{appId:f.appId,frameId:old.frameId,lease:old.lease,revision:old.revision})).closed,false);
  assert.equal((await f.read(instance)).status,200);
  const again=await f.confirm(two,'held_headers_two_2026');assert.equal(again.confirmed,true,'lost confirm ACK is idempotently readable');
});

test('same logical widget physical duplicates share only exact family; independent member close preserves remaining cookie authority',{timeout:30000},async t=>{
  const f=await setup(t),left=await f.launch(f.widgets[0]),right=await f.launch(f.widgets[0],left.cookies);
  assert.equal(left.origin,right.origin);assert.notEqual(left.result.hostSession.frameId,right.result.hostSession.frameId);
  const first=left.result.hostSession;assert.equal((await f.call('apps.session.close',{appId:f.appId,frameId:first.frameId,lease:first.lease,revision:first.revision})).closed,true);
  assert.equal((await f.read(left)).status,200);assert.equal((await f.read(right)).status,200,'B current cookie remains after A closes');
  await assert.rejects(f.prepare(left,'closed_left_prepare'),error=>error.code==='app_host_session_closed');
  await f.renew(right,'still_right_renewal');assert.equal((await f.read(right)).status,200);
});

test('actual ordinary shared-family WS and SSE survive member turnover and own CAS; last family member closes them',{timeout:15000},async t=>{
  const f=await setup(t),source=f.sample.server,delegates=source.listeners('request'),headers=[];let eventRequests=0,upgrades=0;
  source.removeAllListeners('request');source.on('request',(req,res)=>{
    headers.push({rootCookie:/(?:^|;\s*)__Host-soty_app_session(?:=|_h)/u.test(req.headers.cookie??'')});
    if(req.url!=='/events'){for(const handle of delegates)handle.call(source,req,res);return;}
    eventRequests++;res.writeHead(200,{'content-type':'text/event-stream','cache-control':'no-store'});res.write('data: admitted\n\n');
    const timer=setInterval(()=>res.write('data: next\n\n'),40);res.on('close',()=>clearInterval(timer));
  });source.on('upgrade',req=>{upgrades++;headers.push({rootCookie:/(?:^|;\s*)__Host-soty_app_session(?:=|_h)/u.test(req.headers.cookie??'')});});
  const a=await f.launch(f.widgets[0]),aSocket=await socket(t,f,a),aEvents=await events(t,f,a),initialEvents=aEvents.chunks.length;
  assert.equal(eventRequests,1);assert.equal(upgrades,1);
  await f.renew(a,'live_stream_member_cas');aSocket.ws.send('same-upstream-after-cas');
  await until(()=>aSocket.messages.includes('same-upstream-after-cas'),'original WebSocket still echoes after lease promotion');
  await until(()=>aEvents.chunks.length>initialEvents,'original SSE still receives after lease promotion');
  assert.equal(eventRequests,1,'CAS does not replay or reopen Source SSE');assert.equal(upgrades,1,'CAS does not reopen Source WebSocket');
  const b=await f.launch(f.widgets[0],a.cookies),bSocket=await socket(t,f,b),bEvents=await events(t,f,b);
  assert.equal(a.origin,b.origin,'same logical scope intentionally shares origin');assert.notEqual(a.result.hostSession.frameId,b.result.hostSession.frameId);
  // A new browser-native request carries the shared A+B jar and can be selected
  // as B. It cannot be physically attributed to B without an explicit carrier.
  const lateASocket=await socket(t,f,a),lateAEvents=await events(t,f,a),before=[aEvents,bEvents,lateAEvents].map(value=>value.chunks.length);
  const bProof=b.result.hostSession;assert.equal((await f.call('apps.session.close',{appId:f.appId,frameId:bProof.frameId,lease:bProof.lease,revision:bProof.revision})).closed,true);
  for(const[value,index]of [aSocket,bSocket,lateASocket].map((value,index)=>[value,index])){assert.equal(value.ws.readyState,WebSocket.OPEN);value.ws.send('same-family-after-close-'+index);}
  await until(()=>[aSocket,bSocket,lateASocket].every((value,index)=>value.messages.includes('same-family-after-close-'+index))
    &&[aEvents,bEvents,lateAEvents].every((value,index)=>!value.closed&&value.chunks.length>before[index]),'B close keeps the same original streams through still-current A');
  assert.equal(eventRequests,3);assert.equal(upgrades,3,'family member turnover never reconnects Source');
  const other=await f.launch(f.widgets[1]),otherSocket=await socket(t,f,other),otherEvents=await events(t,f,other),otherBefore=otherEvents.chunks.length;
  const aProof=a.result.hostSession;assert.equal((await f.call('apps.session.close',{appId:f.appId,frameId:aProof.frameId,lease:aProof.lease,revision:aProof.revision})).closed,true);
  await until(()=>[aSocket,bSocket,lateASocket].every(value=>value.ws.readyState===WebSocket.CLOSED)
    &&[aEvents,bEvents,lateAEvents].every(value=>value.closed),'last family member must close its WS and SSE',1100);
  assert.equal(otherSocket.ws.readyState,WebSocket.OPEN);otherSocket.ws.send('other-family-stays');
  await until(()=>otherSocket.messages.includes('other-family-stays')&&otherEvents.chunks.length>otherBefore,'different widget is independent and never used as fallback authority');
  assert.ok(headers.length>=4);assert.equal(headers.some(value=>value.rootCookie),false,'Root host-session auth cookies never reach ordinary Source HTTP or WS');
});

test('exact request replay retains candidate proof, conflicting payload denied; known promoted candidate recovers lost ACK across API restart',{timeout:30000},async t=>{
  const f=await setup(t),instance=await f.launch(),id='replay_restart_request';
  const one=await f.prepare(instance,id),again=await f.prepare(instance,id);
  assert.equal(JSON.stringify(again.hostSession)===JSON.stringify(one.hostSession),true);assert.equal(again.launchUrl!==one.launchUrl,true,'ticket is reissued, not replayed');
  await assert.rejects(f.call('apps.session.renew',{appId:f.appId,frameId:instance.result.hostSession.frameId,lease:instance.result.hostSession.lease,revision:instance.result.hostSession.revision,requestId:id,parentOrigin:f.origin,currentEntryPath:'/other'}),error=>error.code==='app_host_session_request_conflict');
  await assert.rejects(f.confirm(again,id),error=>error.code==='app_host_session_unconfirmed');
  await f.mint(instance,again);await f.confirm(again,id);
  await f.restart();
  const candidate=again.hostSession,context=await f.call('apps.session.context',{appId:f.appId,frameId:candidate.frameId,lease:candidate.lease,revision:candidate.revision,currentEntryPath:instance.result.entry.path});
  assert.equal(context.state,'renew-required');assert.equal(context.hostSessionExpiresAt,null);assert.equal(JSON.stringify(context.hostSession)===JSON.stringify(candidate),true);
  await assert.rejects(f.confirm(again,id),error=>error.code==='app_host_session_unconfirmed');
  await assert.rejects(f.prepare(instance,'stale_original_after_restart'),error=>error.code==='app_host_session_conflict');
  instance.result={...instance.result,...again};await f.renew(instance,'known_candidate_after_restart');assert.equal((await f.read(instance)).status,200);
});

test('Vault relock rejects held ordinary renewal and cannot poison main or W2; restart cannot recover a lost password permit',{timeout:30000},async t=>{
  const f=await setup(t),password='Own renewal password 2026!';
  await f.call('canvas.vault.protect',{requestId:'renew_protect',objectId:f.objects[0].objectId,expectedObjectRevision:f.objects[0].revision,password});
  await f.call('canvas.vault.unlock',{objectId:f.objects[0].objectId,password});
  const main=await f.launch(),left=await f.launch(f.widgets[0]),right=await f.launch(f.widgets[1]),held=await f.prepare(left,'vault_held_renewal');
  await f.call('canvas.vault.relock',{requestId:'renew_relock',objectId:f.objects[0].objectId,scope:'device'});
  const response=await http(f.port,left.origin,'/_soty/session',{method:'POST',cookie:left.cookies.get(),body:{ticket:new URL(held.launchUrl).hash.slice(1)}});
  assert.equal(response.status,403);assert.equal((await f.read(left)).status,403);assert.equal((await f.read(main)).status,200);assert.equal((await f.read(right)).status,200);
  await f.call('canvas.vault.unlock',{objectId:f.objects[0].objectId,password});
  await assert.rejects(f.prepare(left,'closed_lease_after_unlock'),error=>error.code==='app_host_session_closed','fresh unlock must not resurrect the retired physical member');
  const fresh=await f.launch(f.widgets[0]);await f.restart();
  await assert.rejects(f.prepare(fresh,'lost_permit_restart'),error=>error.code==='canvas_object_locked');
});

test('candidate concurrency and obsolete cookie accumulation are bounded without deleting a successor',{timeout:30000},async t=>{
  const f=await setup(t),instance=await f.launch();
  for(let index=0;index<25;index++){await f.renew(instance,'bounded_renewal_'+String(index).padStart(3,'0'));assert.ok(instance.cookies.size()<=3);}
  await f.prepare(instance,'bounded_pending_one');await f.prepare(instance,'bounded_pending_two');
  await assert.rejects(f.prepare(instance,'bounded_pending_three'),error=>error.code==='apps_sessions_busy');
  assert.equal((await f.read(instance)).status,200);
});

test('actual HTTP renewal expires at most sixteen observed obsolete cookie names per response',{timeout:30000},async t=>{
  const f=await setup(t),instance=await f.launch(),observed=jar();
  // Retain genuinely received old auth names, as an already-in-flight browser
  // request can do before later cleanup headers arrive. No credential is minted.
  for(let index=0;index<19;index++){
    const result=await f.renew(instance,'exact_cleanup_prior_'+String(index).padStart(3,'0'));
    observed.apply({headers:{'set-cookie':result.minted.headers['set-cookie'].filter(value=>!value.includes('Max-Age=0'))}});
  }
  const candidate=await f.prepare(instance,'exact_cleanup_budget_request'),selector=instance.cookies.get().split('; ').find(value=>value.startsWith('__Host-soty_app_session='));
  const response=await http(f.port,instance.origin,'/_soty/session',{method:'POST',cookie:selector+'; '+observed.get(),body:{ticket:new URL(candidate.launchUrl).hash.slice(1)}});
  assert.equal(response.status,200);
  assert.equal(response.headers['set-cookie'].filter(value=>value.includes('Max-Age=0')).length,16,'exact expired-cookie budget applies independently of initial selector header');
});

test('actual principal main namespace: late initial A headers cannot select A authority in B document or corrupt B cookie check',{timeout:30000},async t=>{
  const f=await setup(t);
  await f.call('apps.update',{appId:f.appId,grants:{accountIds:[f.member.account.accountId],communityIds:[]}});
  const a=await f.call('apps.launch',{appId:f.appId,hostSession:true}),b=await f.member.client.extension('apps.launch',{appId:f.appId,hostSession:true,expectedAccountId:f.member.account.accountId},{expectedAccountId:f.member.account.accountId});
  assert.equal(a.entry.origin,b.entry.origin,'canonical app identity stays unchanged');
  const aOrigin=a.runtimeOrigin??a.entry.origin,bOrigin=b.runtimeOrigin??b.entry.origin,jars=new Map();
  const cookies=origin=>{if(!jars.has(origin))jars.set(origin,jar());return jars.get(origin);};
  const held=await http(f.port,aOrigin,'/_soty/session',{method:'POST',body:{ticket:new URL(a.launchUrl).hash.slice(1)}});assert.equal(held.status,200);
  const fresh=await http(f.port,bOrigin,'/_soty/session',{method:'POST',body:{ticket:new URL(b.launchUrl).hash.slice(1)}});assert.equal(fresh.status,200);cookies(bOrigin).apply(fresh);
  const bCheck=JSON.parse(fresh.body).sessionCheck;
  assert.equal((await http(f.port,bOrigin,'/_soty/session',{cookie:cookies(bOrigin).get(),headers:{'x-soty-boot-check':bCheck}})).status,200);
  cookies(aOrigin).apply(held);
  assert.equal((await http(f.port,bOrigin,'/_soty/session',{cookie:cookies(bOrigin).get(),headers:{'x-soty-boot-check':bCheck}})).status,200,'B remains B after late A cookie delivery');
  assert.notEqual(aOrigin,bOrigin,'ordinary principals require distinct native storage origins');
  assert.equal((await http(f.port,bOrigin,'/api/items',{cookie:cookies(bOrigin).get()})).status,200);
  assert.equal((await http(f.port,bOrigin,'/api/items',{cookie:cookies(aOrigin).get()})).status,403,'A credentials cannot enter B main origin');
});

test('initial reservation is explicit admitting until real cookie GET, then signed same-proof expiry refresh is admitted',{timeout:30000},async t=>{
  const f=await setup(t),result=await f.call('apps.launch',{appId:f.appId,hostSession:true}),descriptor=result.hostSession;
  const context=()=>f.call('apps.session.context',{appId:f.appId,frameId:descriptor.frameId,lease:descriptor.lease,revision:descriptor.revision,currentEntryPath:result.entry.path});
  const first=await context();assert.equal(first.state,'admitting');assert.equal(first.hostSessionExpiresAt,null);
  await assert.rejects(f.call('apps.session.renew',{appId:f.appId,frameId:descriptor.frameId,lease:descriptor.lease,revision:descriptor.revision,requestId:'too_early_initial_renew',parentOrigin:f.origin,currentEntryPath:result.entry.path}),error=>error.code==='app_host_session_unconfirmed');
  const origin=result.runtimeOrigin,cookies=jar(),minted=await http(f.port,origin,'/_soty/session',{method:'POST',body:{ticket:new URL(result.launchUrl).hash.slice(1)}});assert.equal(minted.status,200);cookies.apply(minted);
  const afterPost=await context();assert.equal(afterPost.state,'admitting');assert.equal(afterPost.hostSessionExpiresAt,null);
  assert.equal((await http(f.port,origin,'/api/items',{cookie:cookies.get()})).status,409,'POST alone does not activate a current Source transport');
  assert.equal((await http(f.port,origin,'/_soty/session',{cookie:cookies.get(),headers:{'x-soty-boot-check':JSON.parse(minted.body).sessionCheck}})).status,200);
  const current=await context();assert.equal(current.state,'current');assert.equal(current.hostSession.lease===descriptor.lease,true);assert.equal(current.hostSession.revision,descriptor.revision);
  assert.ok(current.hostSession.expiresAt>descriptor.expiresAt);assert.ok(current.hostSessionExpiresAt<=current.hostSession.expiresAt);
});

test('sixteen abandoned initial launch replies expire without evicting an actually admitted widget member',{timeout:45000},async t=>{
  const f=await setup(t),active=await f.launch(f.widgets[0]);
  for(let index=0;index<16;index++)await f.call('apps.launch',{appId:f.appId,hostSession:true});
  await assert.rejects(f.call('apps.launch',{appId:f.appId,hostSession:true}),error=>error.code==='apps_sessions_busy');
  assert.equal((await f.read(active)).status,200);
  await new Promise(resolve=>setTimeout(resolve,30100));
  const next=await f.launch();assert.equal((await f.read(next)).status,200);assert.equal((await f.read(active)).status,200,'an admitted member is not a reservation eviction victim');
});

test('main runtime TLS and anonymous boundaries use exact issued current full binding',{timeout:30000},async t=>{
  const f=await setup(t,{appScheme:'https'}),main=await f.launch(),host=new URL(main.origin).hostname,service=f.app.locals.appsService;
  assert.equal(service.allowsTlsDomain(host),true);
  const forged=host.replace(/-[a-f0-9]{24}\./u,'-'+'0'.repeat(24)+'.');
  assert.equal(service.allowsTlsDomain(forged),false);
  assert.equal((await http(f.port,main.origin,'/api/items')).status,401);
  assert.equal((await http(f.port,main.origin,'/_soty/session',{method:'DELETE'})).status,401);
  assert.equal((await http(f.port,main.origin,'/_soty/relay-identity-config')).status,401);
  const canonical=await f.call('apps.entry.get',{appId:f.appId});assert.equal(canonical.entry.origin,main.result.entry.origin);
});

test('actual SDK original-actor cleanup closes only A member after legitimate local enrollment switches the client to B',{timeout:30000},async t=>{
  const f=await setup(t,{scopedAppCleanup:true}),client=f.owner.client,aId=f.owner.account.accountId,bId=f.member.account.accountId;
  const enrollment=await client.startEnrollment('Own synthetic local B');
  await f.member.client.approveEnrollment(enrollment.requestId,bId);await client.previewEnrollment(enrollment.requestId);await client.finishEnrollment(enrollment.requestId,bId);
  await client.switchProfile(aId);
  await f.call('apps.update',{appId:f.appId,grants:{accountIds:[bId],communityIds:[]}});
  const a=await f.launch();let descriptor=a.result.hostSession;
  const firstContext=await f.call('apps.session.context',{appId:f.appId,frameId:descriptor.frameId,lease:descriptor.lease,revision:descriptor.revision,currentEntryPath:a.result.entry.path});
  assert.equal(firstContext.state,'current');const oldCleanup=client.appSlotCleanup(firstContext);assert.equal(typeof oldCleanup,'function');
  const requestId='sdk_actual_transport_handoff',candidate=await f.prepare(a,requestId),candidateCleanup=client.appSlotCleanup(candidate);assert.equal(typeof candidateCleanup,'function');
  await f.mint(a,candidate);const confirmed=await f.confirm(candidate,requestId);
  assert.equal(client.appSlotCleanup(confirmed)===candidateCleanup,true,'actual candidate/confirmation decoder retains its exact callback');
  descriptor=confirmed.hostSession;await oldCleanup();assert.equal((await f.read(a)).status,200,'stale cleanup cannot close the promoted own successor');
  const context=await f.call('apps.session.context',{appId:f.appId,frameId:descriptor.frameId,lease:descriptor.lease,revision:descriptor.revision,currentEntryPath:a.result.entry.path});
  const cleanup=client.appSlotCleanup(context);assert.equal(typeof cleanup,'function');assert.equal(cleanup===candidateCleanup,true);
  assert.equal(client.appSlotCleanup({...context}),null,'cloning JSON does not mint an original-actor capability');
  await client.switchProfile(bId);
  const callB=(op,args)=>client.extension(op,{...args,expectedAccountId:bId},{expectedAccountId:bId});
  const b=await callB('apps.launch',{appId:f.appId,hostSession:true}),origin=b.runtimeOrigin,cookies=jar();
  const minted=await http(f.port,origin,'/_soty/session',{method:'POST',body:{ticket:new URL(b.launchUrl).hash.slice(1)}});assert.equal(minted.status,200);cookies.apply(minted);
  assert.equal((await http(f.port,origin,'/_soty/session',{cookie:cookies.get(),headers:{'x-soty-boot-check':JSON.parse(minted.body).sessionCheck}})).status,200);
  await assert.rejects(client.extension('apps.session.close',{appId:f.appId,frameId:descriptor.frameId,lease:descriptor.lease,revision:descriptor.revision,expectedAccountId:aId},{expectedAccountId:aId}),error=>error.code==='ACTIVE_PROFILE_CHANGED','general inactive-account RPC stays forbidden');
  await cleanup();
  assert.equal((await f.read(a)).status,403,'original A cookie is retired through the fixed captured-actor close');
  assert.equal((await http(f.port,origin,'/api/items',{cookie:cookies.get()})).status,200,'B current member remains authorized');
});

test('host selector cannot fall back to old base cookie, other widget family, or current cookie with wrong Host',{timeout:30000},async t=>{
  const f=await setup(t),left=await f.launch(f.widgets[0]),right=await f.launch(f.widgets[1]),main=await f.launch();
  assert.equal((await http(f.port,right.origin,'/api/items',{cookie:left.cookies.get()})).status,403);
  const rightSelector=right.cookies.get().split('; ').find(value=>value.startsWith('__Host-soty_app_session='));
  const leftAuth=left.cookies.get().split('; ').filter(value=>!value.startsWith('__Host-soty_app_session=')).join('; ');
  assert.equal((await http(f.port,left.origin,'/api/items',{cookie:rightSelector+'; '+leftAuth})).status,403,'family is mandatory, not any live widget');
  const legacy=await f.call('apps.launch',{appId:f.appId}),minted=await http(f.port,legacy.entry.origin,'/_soty/session',{method:'POST',body:{ticket:new URL(legacy.launchUrl).hash.slice(1)}}),oldCookie=minted.headers['set-cookie'][0].split(';')[0];
  assert.equal((await http(f.port,left.origin,'/api/items',{cookie:rightSelector+'; '+oldCookie+'; '+leftAuth})).status,401,'duplicate old base cannot shadow the selector');
  assert.equal((await f.read(main)).status,200);
});

test('delayed cleanup cannot delete an observed future candidate after its promotion; closed sender cannot provide fallback',{timeout:30000},async t=>{
  const f=await setup(t),left=await f.launch(f.widgets[0]),right=await f.launch(f.widgets[0],left.cookies);
  const b=await f.prepare(right,'observed_b_pending');await f.mint(right,b);
  const a=await f.prepare(left,'held_a_cleanup_headers'),held=await f.mint(left,a,{apply:false,check:false});
  const current=left.result.hostSession;await f.call('apps.session.close',{appId:f.appId,frameId:current.frameId,lease:current.lease,revision:current.revision});
  await f.confirm(b,'observed_b_pending');left.cookies.apply(held);
  assert.equal((await f.read(right)).status,200,'late A cleanup cannot remove the only surviving B cookie');
});

test('late initial same-family selector is stable and legacy external boot reuses current authority without a durable member or cookie replacement',{timeout:30000},async t=>{
  const f=await setup(t),widget=f.widgets[0];
  const early=await f.call('apps.launch',{appId:f.appId,hostSession:true,...widget.entry,instanceId:widget.widgetId}),origin=early.runtimeOrigin;
  const held=await http(f.port,origin,'/_soty/session',{method:'POST',body:{ticket:new URL(early.launchUrl).hash.slice(1)}});
  assert.equal(held.status,200);
  const current=await f.launch(widget),old=early.hostSession;
  await f.call('apps.session.close',{appId:f.appId,frameId:old.frameId,lease:old.lease,revision:old.revision});
  current.cookies.apply(held);assert.equal((await f.read(current)).status,200);
  const count=()=>{const db=new DatabaseSync(join(f.directory,'data','apps','registry.sqlite'),{readOnly:true});try{return db.prepare("SELECT count(*) n FROM app_frame_continuity WHERE json_extract(binding_json,'$.kind')='soty.app-host-session.v1'").get().n;}finally{db.close();}};
  const before=count(),legacy=await f.call('apps.launch',{appId:f.appId,...widget.entry,instanceId:widget.widgetId});
  assert.equal(legacy.hostSession,undefined);
  const admitted=await http(f.port,origin,'/_soty/session',{method:'POST',cookie:current.cookies.get(),body:{ticket:new URL(legacy.launchUrl).hash.slice(1)}});
  assert.equal(admitted.status,200);assert.equal(admitted.headers['set-cookie'],undefined,'legacy external must not replace Root selector/authcookie');
  const checked=await http(f.port,origin,'/_soty/session',{cookie:current.cookies.get(),headers:{'x-soty-boot-check':JSON.parse(admitted.body).sessionCheck}});
  assert.equal(checked.status,200);assert.equal(count(),before);assert.equal((await f.read(current)).status,200);
});
