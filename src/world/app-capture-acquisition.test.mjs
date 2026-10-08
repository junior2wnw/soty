import test from 'node:test';
import assert from 'node:assert/strict';
import {MessageChannel} from 'node:worker_threads';
import {createScopedSlotRenewal} from './app-scoped-renewal.mjs';
import {mountProjectCaptureBridge} from './project-feedback-capture.mjs';

const flush=()=>new Promise(resolve=>setImmediate(resolve));
const source={id:'source',version:1,digest:'a'.repeat(64)};
const peer=(binding,generation=1)=>({approved:true,window:binding.window,origin:'https://source.fixture.test',sourceId:'source',appId:'app',accountId:'actor',generation,slot:binding.slot,title:'Synthetic'});
function request(listener,value,channel){listener({origin:value.origin,source:value.window,ports:[channel.port1],data:{schema:'soty.feedback.capture.v1',type:'capture_request',requestId:'synthetic-capture',sourceId:'source',projectId:'fixture-project',contextRevision:1,kind:'audio'}});}
function fixture(){
 let binding={handle:'old',slot:{},window:{},source},generation=1,revoke=false,complete,entered;
 const requested=new Promise(resolve=>entered=resolve),next={handle:'new',slot:{},window:binding.window,source};
 const helper=createScopedSlotRenewal({readBinding:()=>binding,isCurrent:()=>true,clock:()=>0,
  readContext:async()=>{if(revoke)throw Object.assign(new Error('revoked'),{status:403,code:'app_access_revoked'});return{ready:true,scopedSource:binding.source,expiresAt:binding===next?300000:200000,sourceSession:{ready:true,renewable:true,sessionExpiresAt:86400000,accessExpiresAt:300000}};},
  request:()=>{entered();return new Promise(resolve=>complete=resolve);},bootstrap:async()=> 'ready',commit:(_old,value)=>{binding=value;generation++;return true;}});
 return{helper,requested,get binding(){return binding;},readPeer:()=>peer(binding,generation),complete(){complete(next);},revoke(){revoke=true;},accountAba(){binding={handle:'account-B',slot:{},window:{},source};binding={handle:'account-A-new',slot:{},window:{},source};}};
}

test('capture still closes at overall180s, never at179999ms',async t=>{
 t.mock.timers.enable({apis:['setTimeout','setInterval']});let listener,aborted=false;const binding={slot:{},window:{},source},value=peer(binding);
 const bridge=mountProjectCaptureBridge({view:{addEventListener(_name,handler){listener=handler;},removeEventListener(){}},readPeer:()=>value,assertPeer:async()=>true,
  capture:({signal})=>new Promise((_resolve,reject)=>signal.addEventListener('abort',()=>{aborted=true;reject(new DOMException('Aborted','AbortError'));},{once:true}))});
 const channel=new MessageChannel();t.after(()=>{bridge.dispose();channel.port1.close();channel.port2.close();});const response=new Promise(resolve=>channel.port2.once('message',resolve));request(listener,value,channel);await flush();
 t.mock.timers.tick(179999);assert.equal(aborted,false);t.mock.timers.tick(1);assert.equal((await response).code,'capture_timeout');assert.equal(aborted,true);
});
test('capture lease still suppresses future routine renewal',async()=>{
 const f=fixture();assert.equal(await f.helper.ensure(190000),true);const release=f.helper.beginCaptureLease();assert.equal(await f.helper.tick(),false);release();f.helper.dispose();
});
test('already-inflight renewal finishes before capture enters and does not cancel picker',async t=>{
 t.mock.timers.enable({apis:['setTimeout','setInterval']});const f=fixture(),running=f.helper.tick();await f.requested;
 let listener,release,aborted=false,enteredCapture,capturedSlot;const entered=new Promise(resolve=>enteredCapture=resolve);
 const bridge=mountProjectCaptureBridge({view:{addEventListener(_name,handler){listener=handler;},removeEventListener(){}},readPeer:f.readPeer,preparePeer:()=>f.helper.ensure(190000),assertPeer:async()=>true,
  onCaptureActive(active){if(active)release=f.helper.beginCaptureLease();else release?.();},capture:({signal})=>new Promise((_resolve,reject)=>{capturedSlot=f.binding.slot;enteredCapture();signal.addEventListener('abort',()=>{aborted=true;reject(new DOMException('Aborted','AbortError'));},{once:true});})});
 const channel=new MessageChannel();t.after(()=>{bridge.dispose();f.helper.dispose();channel.port1.close();channel.port2.close();});request(listener,f.readPeer(),channel);
 setImmediate(()=>f.complete());await entered;await running;t.mock.timers.tick(250);await flush();assert.equal(aborted,false);assert.equal(capturedSlot,f.binding.slot);
});
test('capture acquisition denies a new pending renewal instead of installing a lease',async()=>{
 const f=fixture(),running=f.helper.tick();await f.requested;assert.throws(()=>f.helper.beginCaptureLease(),/capture_context_changed/u);f.complete();await running;assert.equal(await f.helper.ensure(190000),true);f.helper.beginCaptureLease()();f.helper.dispose();
});
for(const mode of ['revoke','accountAba','stop'])test('waiting capture preflight denies '+mode,async()=>{
 const f=fixture(),running=f.helper.tick();await f.requested;let settled=false;const waiting=f.helper.ensure(190000).then(value=>{settled=true;return value;});await flush();assert.equal(settled,false);
 if(mode==='stop')f.helper.dispose();else f[mode]();f.complete();await running;assert.equal(await waiting,false);f.helper.dispose();
});
