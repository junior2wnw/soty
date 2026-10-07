import test from 'node:test';
import assert from 'node:assert/strict';
import {createScopedSlotRenewal} from './app-scoped-renewal.mjs';
import {mountProjectCaptureBridge} from './project-feedback-capture.mjs';
import {MessageChannel} from 'node:worker_threads';
test('client 120/180s capture lease suppresses only routine renew; deadline does not slide',async()=>{
  let time=1000000,deadline=time+60000,requests=0,binding={handle:'old',slot:{},source:{id:'source',version:1,digest:'a'.repeat(64)}};
  const helper=createScopedSlotRenewal({readBinding:()=>binding,isCurrent:()=>true,clock:()=>time,
    async readContext(){return{ready:true,scopedSource:binding.source,expiresAt:deadline,sourceSession:{ready:true,renewable:true,sessionExpiresAt:10000000,accessExpiresAt:time+300000}};},
    async request(){requests++;deadline=time+300000;return{handle:'new'+requests,slot:{},source:binding.source,expiresAt:deadline};},async bootstrap(){return'ready';},
    commit(_old,next){binding=next;return true;}});
  await helper.ensure(190000);const originalDeadline=deadline,release=helper.beginCaptureLease();
  time+=120000;assert.equal(await helper.tick(),false);time+=60000;assert.equal(await helper.tick(),false);
  assert.equal(deadline,originalDeadline);assert.equal(requests,1);release();release();assert.equal(await helper.tick(),true);assert.equal(requests,2);helper.dispose();
});
test('lease releases exactly once at timeout even when media capture ignores abort',async t=>{
  let listener,release;const activity=[],waiting=new Promise(done=>{release=done;}),peer={approved:true,window:{},origin:'https://source.example',sourceId:'source',appId:'app',accountId:'actor',generation:1,slot:{},title:'Synthetic'};
  const bridge=mountProjectCaptureBridge({view:{addEventListener(_name,handler){listener=handler;},removeEventListener(){}},readPeer:()=>peer,
    async assertPeer(){return true;},onCaptureActive:active=>activity.push(active),timeoutMs:20,async capture(){await waiting;return[{kind:'audio',name:'synthetic.webm',mimeType:'audio/webm',dataBase64:'AQID'}];}});
  const channel=new MessageChannel();t.after(()=>{bridge.dispose();channel.port1.close();channel.port2.close();});
  const reply=new Promise(done=>channel.port2.once('message',done));listener({origin:peer.origin,source:peer.window,ports:[channel.port1],data:{schema:'soty.feedback.capture.v1',type:'capture_request',requestId:'synthetic-one',sourceId:'source',projectId:'one',contextRevision:1,kind:'audio'}});
  assert.equal((await reply).code,'capture_timeout');assert.deepEqual(activity,[true,false]);release();await new Promise(done=>setImmediate(done));bridge.dispose();assert.deepEqual(activity,[true,false]);
});

test('AT expires during 180s preview: historical Source ACK permits only one fresh-ACK attempt after release',async()=>{
  let time=1000000,requests=0,swaps=0,newAck=false,binding={handle:'old',slot:{},source:{id:'source',version:1,digest:'a'.repeat(64)}};
  const initial=time,sourceEnd=initial+86400000;
  const helper=createScopedSlotRenewal({readBinding:()=>binding,isCurrent:()=>true,clock:()=>time,
    async readContext(handle){return{ready:true,scopedSource:binding.source,expiresAt:initial+300000,
      ...((handle==='old'&&time<initial+30000||handle!=='old'&&newAck)?{sourceSession:{ready:true,renewable:true,sessionExpiresAt:sourceEnd}}:{})};},
    async request(){requests++;return{handle:'new',slot:{},source:binding.source,cleanup:async()=>{}};},async bootstrap(){return'unknown';},
    commit(_old,next){binding=next;swaps++;return true;}});
  assert.equal(await helper.probe(),true);const release=helper.beginCaptureLease();time+=180000;
  assert.equal(await helper.tick(),false);assert.equal(requests,0);release();
  assert.equal(await helper.tick(),false);assert.equal(requests,1);assert.equal(swaps,0,'no fresh ACK means no ready/swap');
  newAck=true;assert.equal(await helper.renew(),true);assert.equal(requests,2);assert.equal(swaps,1);helper.dispose();
});

test('explicit Source login-required/unknown-refresh state clears historical witness instead of auto retry',async()=>{
  for(const state of [{ready:false,reason:'login_required'},{ready:true,renewable:false,sessionExpiresAt:10000000}]){
    let acknowledged=true,requests=0;const binding={handle:'old',slot:{},source:{id:'source',version:1,digest:'a'.repeat(64)}};
    const helper=createScopedSlotRenewal({readBinding:()=>binding,isCurrent:()=>true,clock:()=>1000000,
      async readContext(){return{ready:true,scopedSource:binding.source,expiresAt:1050000,sourceSession:acknowledged?{ready:true,renewable:true,sessionExpiresAt:10000000}:state};},
      async request(){requests++;throw new Error('must_not_send');}});
    assert.equal(await helper.probe(),true);acknowledged=false;assert.equal(await helper.tick(),false);assert.equal(requests,0);helper.dispose();
  }
});

test('Root authority denial and current slot/profile change erase historical renewal witness',async()=>{
  for(const mode of ['denial','slot','profile']){
    let time=1000000,acknowledged=true,denied=false,current=true,requests=0,binding={handle:'old',slot:{},source:{id:'source',version:1,digest:'a'.repeat(64)}};
    const helper=createScopedSlotRenewal({readBinding:()=>binding,isCurrent:()=>current,clock:()=>time,
      async readContext(){if(denied)throw Object.assign(new Error('revoked'),{code:'app_access_revoked'});return{ready:true,scopedSource:binding.source,expiresAt:1300000,
        ...(acknowledged?{sourceSession:{ready:true,renewable:true,sessionExpiresAt:10000000}}:{})};},
      async request(){requests++;throw new Error('must_not_send');}});
    assert.equal(await helper.probe(),true);acknowledged=false;time+=180000;
    if(mode==='denial'){denied=true;await assert.rejects(helper.tick(),error=>error.code==='app_access_revoked');denied=false;}
    if(mode==='slot')binding={...binding,slot:{}};
    if(mode==='profile'){current=false;assert.equal(await helper.tick(),false);current=true;}
    assert.equal(await helper.tick(),false);assert.equal(requests,0);helper.dispose();
  }
});

test('fresh Source ACK cannot slide the original absolute session end during automatic rebind',async()=>{
  let swaps=0,closes=0,binding={handle:'old',slot:{},source:{id:'source',version:1,digest:'a'.repeat(64)}};
  const helper=createScopedSlotRenewal({readBinding:()=>binding,isCurrent:()=>true,clock:()=>1000000,
    async readContext(handle){return{ready:true,scopedSource:binding.source,expiresAt:1300000,sourceSession:{ready:true,renewable:true,sessionExpiresAt:handle==='old'?10000000:10000001}};},
    async request(){return{handle:'new',slot:{},source:binding.source,cleanup:async()=>{closes++;}};},async bootstrap(){return'ready';},
    commit(_old,next){binding=next;swaps++;return true;}});
  assert.equal(await helper.renew(),false);assert.equal(swaps,0);assert.equal(closes,1);helper.dispose();
});

test('same-slot source/target mutation cannot borrow a prior long-session witness',async()=>{
  for(const field of ['source','target']){
    let acknowledged=true,requests=0,target={revision:1,digest:'a'.repeat(64)},binding={handle:'old',slot:{},source:{id:'source',version:1,digest:'a'.repeat(64)}};
    const helper=createScopedSlotRenewal({readBinding:()=>binding,isCurrent:()=>true,clock:()=>1000000,
      async readContext(){return{ready:true,scopedSource:binding.source,target,expiresAt:1100000,
        ...(acknowledged?{sourceSession:{ready:true,renewable:true,sessionExpiresAt:10000000}}:{})};},
      async request(){requests++;throw new Error('must_not_send');}});
    assert.equal(await helper.probe(),true);acknowledged=false;
    if(field==='source')binding={...binding,source:{...binding.source,digest:'b'.repeat(64)}};
    else target={revision:2,digest:'b'.repeat(64)};
    assert.equal(await helper.tick(),false);assert.equal(requests,0);helper.dispose();
  }
});

test('capture ensure needs actual ready ACK and both deadlines; Basic ACK never enables auto renew',async()=>{
  const source={id:'source',version:1,digest:'a'.repeat(64)},binding={handle:'old',slot:{},source};
  for(const context of [
    {ready:true,scopedSource:source,expiresAt:1300000},
    {ready:true,scopedSource:source,expiresAt:1300000,sourceSession:{ready:false,reason:'login_required'}},
    {ready:true,scopedSource:{...source,id:'foreign'},expiresAt:1300000,sourceSession:{ready:true,accessExpiresAt:1300000}},
    {ready:true,scopedSource:source,expiresAt:1300000,sourceSession:{ready:true,renewable:false,accessExpiresAt:1050000,sessionExpiresAt:1050000}},
  ]){
    const helper=createScopedSlotRenewal({readBinding:()=>binding,isCurrent:()=>true,clock:()=>1000000,async readContext(){return context;}});
    assert.equal(await helper.ensure(190000),false);helper.dispose();
  }
  let requests=0;const helper=createScopedSlotRenewal({readBinding:()=>binding,isCurrent:()=>true,clock:()=>1000000,
    async readContext(){return{ready:true,scopedSource:source,expiresAt:1300000,sourceSession:{ready:true,renewable:false,sessionExpiresAt:1300000,accessExpiresAt:1300000}};},
    async request(){requests++;}});
  assert.equal(await helper.ensure(190000),true);assert.equal(await helper.tick(),false);assert.equal(requests,0);helper.dispose();
});

test('explicit renew auth denial abandons pending slot and witness; network unknown retains exact intent',async()=>{
  for(const stage of ['request','bootstrap']){
    let requests=0,closed=0,denied=true;const states=[],binding={handle:'old',slot:{},source:{id:'source',version:1,digest:'a'.repeat(64)}};
    const helper=createScopedSlotRenewal({readBinding:()=>binding,isCurrent:()=>true,clock:()=>1000000,onState:value=>states.push(value),
      async readContext(){return{ready:true,scopedSource:binding.source,expiresAt:1100000,
        ...(denied?{sourceSession:{ready:true,renewable:true,sessionExpiresAt:10000000}}:{})};},
      async request(){requests++;if(stage==='request')throw Object.assign(new Error('denied'),{status:403,code:'app_access_revoked'});
        return{handle:'new',slot:{},source:binding.source,cleanup:async()=>{closed++;}};},
      async bootstrap(){throw Object.assign(new Error('source denied'),{status:403});}});
    assert.equal(await helper.renew(),false);assert.equal(states.at(-1),'login_required');assert.equal(closed,stage==='bootstrap'?1:0);
    denied=false;assert.equal(await helper.renew(),false);assert.equal(requests,1);helper.dispose();
  }
});

test('unknown request response retains its exact action, while later fresh ACK is required to commit',async()=>{
  let calls=0,swaps=0,binding={handle:'old',slot:{},source:{id:'source',version:1,digest:'a'.repeat(64)}};const requests=[];
  const helper=createScopedSlotRenewal({readBinding:()=>binding,isCurrent:()=>true,clock:()=>1000000,
    async readContext(){return{ready:true,scopedSource:binding.source,expiresAt:1300000,sourceSession:{ready:true,renewable:true,sessionExpiresAt:10000000,accessExpiresAt:1300000}};},
    async request(args){requests.push(args);if(++calls===1)throw Object.assign(new Error('unknown'),{code:'NETWORK_ERROR'});
      return{handle:'new',slot:{},source:binding.source,cleanup:async()=>{}};},async bootstrap(){return'ready';},commit(_old,next){binding=next;swaps++;return true;}});
  assert.equal(await helper.renew(),false);assert.equal(swaps,0);assert.equal(await helper.renew(),true);
  assert.equal(requests.length,2);assert.equal(requests[0].requestId,requests[1].requestId);assert.equal(requests[0].handle,requests[1].handle);assert.equal(swaps,1);helper.dispose();
});

test('capture ensure rechecks NEW ACK/AT and Root TTL after a successful slot renewal',async()=>{
  for(const outcome of ['short-at','short-root','expired','false','good']){
    let reads=0,swaps=0,binding={handle:'old',slot:{},source:{id:'source',version:1,digest:'a'.repeat(64)}};
    const helper=createScopedSlotRenewal({readBinding:()=>binding,isCurrent:()=>true,clock:()=>1000000,
      async readContext(handle){reads++;const postCommit=swaps===1;
        return{ready:true,scopedSource:binding.source,expiresAt:handle==='old'?1050000:postCommit&&outcome==='short-root'?1130000:1300000,
          sourceSession:postCommit&&outcome==='false'?{ready:false,reason:'login_required'}:{ready:true,renewable:true,sessionExpiresAt:10000000,
            accessExpiresAt:handle==='old'||outcome==='short-at'?1130000:postCommit&&outcome==='expired'?999999:1300000}};},
      async request(){return{handle:'new',slot:{},source:binding.source,cleanup:async()=>{}};},async bootstrap(){return'ready';},
      commit(_old,next){binding=next;swaps++;return true;}});
    assert.equal(await helper.ensure(190000),outcome==='good',outcome);assert.equal(swaps,1,'slot admitted but capture separately checked');assert.ok(reads>=4);helper.dispose();
  }
});
