import test from 'node:test';
import assert from 'node:assert/strict';
import {createScopedSlotRenewal} from './app-scoped-renewal.mjs';
// Client lifecycle fixtures only. Actual signed/channel/Source tests live under modules/apps/test.
function fixture(change={}){let time=1000000,current=true,active={handle:'old',source:{id:'source-one',version:1,digest:'a'.repeat(64)},slot:{}},
  requests=[],swaps=0,closes=0,nextReady=true,hint='ready';
  const context=handle=>({ready:true,scopedSource:active.source,expiresAt:time+(handle==='old'?60000:300000),
    ...(handle==='old'||nextReady?{sourceSession:{ready:true,renewable:true,sessionExpiresAt:time+3600000,accessExpiresAt:time+300000}}:{})});
  const helper=createScopedSlotRenewal({readBinding:()=>active,isCurrent:()=>current,clock:()=>time,
    readContext:async handle=>context(handle),async request(input){requests.push(input);return{handle:'new',source:active.source,slot:{},url:'https://app.example/_soty/boot',
      requestId:input.requestId,expiresAt:time+300000,cleanup:async()=>{closes++;}};},async bootstrap(){return hint;},commit(old,next){if(active.slot!==old.slot)return false;active=next;swaps++;return true;},...change});
  return{helper,counts:()=>({requests,swaps,closes}),setCurrent(value){current=value;},setReady(value){nextReady=value;},setHint(value){hint=value;},active:()=>active};}
test('client renews before 180s capture on a 240s slot and commits only server-ACK/current pins',async()=>{
  const f=fixture();assert.equal(await f.helper.ensure(190000),true);assert.equal(f.counts().swaps,1);assert.equal(f.active().handle,'new');});
test('fake boot ready without server Source ACK cannot commit a native session',async()=>{
  const f=fixture();f.setReady(false);assert.equal(await f.helper.renew(),false);assert.equal(f.counts().swaps,0);assert.equal(f.counts().closes,1);});
test('unknown boot ACK keeps same request intent and recovers current server state',async()=>{
  const f=fixture();f.setReady(false);f.setHint('unknown');assert.equal(await f.helper.renew(),false);assert.equal(f.counts().closes,0);
  f.setReady(true);assert.equal(await f.helper.renew(),true);const requests=f.counts().requests;
  assert.equal(requests.length,2);assert.equal(requests[0].requestId,requests[1].requestId);assert.equal(requests[0].handle,requests[1].handle);});
test('profile switch while awaiting boot closes exact new slot and never swaps old UI',async()=>{
  let begun,release;const started=new Promise(done=>{begun=done;}),waiting=new Promise(done=>{release=done;});
  const f=fixture({async bootstrap(){begun();await waiting;return'ready';}}),pending=f.helper.renew();await started;f.setCurrent(false);release();
  assert.equal(await pending,false);assert.equal(f.counts().swaps,0);assert.equal(f.counts().closes,1);});
test('concurrent requests share one private renewal operation',async()=>{
  const f=fixture();const results=await Promise.all([f.helper.renew(),f.helper.renew()]);assert.equal(results.every(Boolean),true);
  assert.equal(f.counts().requests.length,1);assert.equal(f.counts().swaps,1);});
test('unconfirmed Source declaration cannot enable automatic renewal',async()=>{
  const f=fixture({async readContext(){return{ready:true,scopedSource:{id:'source-one',version:1,digest:'a'.repeat(64)},expiresAt:1100000};}});
  assert.equal(await f.helper.tick(),false);assert.equal(f.counts().requests.length,0);});
