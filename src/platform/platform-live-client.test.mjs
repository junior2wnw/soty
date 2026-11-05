import test from 'node:test';
import assert from 'node:assert/strict';
import {createPlatformLiveClient} from './platform-live-client.mjs';

const settle=()=>new Promise(resolve=>setImmediate(resolve));
const response=(cursor,extra={})=>({schema:'soty.platform-live.v1',cursor,changed:false,reset:false,...extra});
function fixture(extra={}){
  const calls=[],tasks=new Map(),states=[];let id=0,current=true;
  const api={request(operation,args){return new Promise((resolve,reject)=>calls.push({operation,args,resolve,reject}));}};
  const client=createPlatformLiveClient({api,accountId:'account-a',isCurrent:()=>current,onInvalidate:()=>{},onStatus:value=>states.push(value),
    schedule:(fn,delay)=>{const key=++id;tasks.set(key,{fn,delay});return key;},cancel:key=>tasks.delete(key),...extra});
  return {client,calls,tasks,states,retire:()=>{current=false;},async tick(){const first=tasks.entries().next().value;assert.ok(first,'a next wait is scheduled');tasks.delete(first[0]);first[1].fn();await settle();}};
}

test('live wait applies a fresh read before acknowledging its cursor and then waits again',async()=>{
  let reads=0,finish;const f=fixture({watch:()=>[{kind:'app',id:'app-a'},{kind:'app',id:'app-a'}],onInvalidate:()=>{reads++;return new Promise(resolve=>{finish=resolve;});}});
  await f.tick();assert.equal(f.calls[0].operation,'platform.live.wait');assert.equal(f.calls[0].args.expectedAccountId,'account-a');assert.deepEqual(f.calls[0].args.watch,[{kind:'app',id:'app-a'}]);
  f.calls[0].resolve(response('cursor-1',{reset:true}));await settle();assert.equal(reads,1);assert.equal(f.tasks.size,0);
  finish();await settle();await f.tick();assert.equal(f.calls[1].args.cursor,'cursor-1');assert.equal(f.states.at(-1),'live');f.client.dispose();
});

test('failed dependent refresh retains the old cursor so an update cannot silently disappear',async()=>{
  let fail=false,reads=0;const f=fixture({onInvalidate:()=>{reads++;if(fail)throw new Error('directory offline');}});
  await f.tick();f.calls[0].resolve(response('before',{reset:true}));await settle();await f.tick();
  fail=true;f.calls[1].resolve(response('after',{changed:true}));await settle();assert.equal(f.states.at(-1),'reconnecting');assert.equal([...f.tasks.values()][0].delay,500);
  fail=false;await f.tick();assert.equal(f.calls[2].args.cursor,'before');f.calls[2].resolve(response('after',{changed:true}));await settle();assert.equal(reads,3);assert.equal(f.states.at(-1),'live');f.client.dispose();
});

test('reconnect retires the old generation without overlapping waits or applying a stale update',async()=>{
  let reads=0;const f=fixture({onInvalidate:()=>{reads++;}});await f.tick();f.client.reconnect();await f.tick();assert.equal(f.calls.length,1);
  f.calls[0].resolve(response('old',{changed:true}));await settle();assert.equal(reads,0);await f.tick();assert.equal(f.calls.length,2);assert.equal(f.calls[1].args.cursor,undefined);f.client.dispose();
});

test('account change and disposal prevent an in-flight response from repainting old private data',async()=>{
  let reads=0;const f=fixture({onInvalidate:()=>{reads++;}});await f.tick();f.retire();f.calls[0].resolve(response('other-account',{reset:true}));await settle();assert.equal(reads,0);assert.equal(f.tasks.size,0);f.client.dispose();
});

test('device revocation stops waits and calls the authority-loss path',async()=>{
  let lost=0;const f=fixture({onAccessLost:()=>{lost++;}});await f.tick();f.calls[0].reject(Object.assign(new Error('revoked'),{code:'device_revoked'}));await settle();assert.equal(lost,1);assert.equal(f.states.at(-1),'unavailable');assert.equal(f.tasks.size,0);f.client.dispose();
});
test('exact server authentication errors retire the feed, including a failed cleanup view',async()=>{
  for(const code of ['live_authentication_required','live_account_changed','authentication_required']){
    let lost=0;const f=fixture({onAccessLost:()=>{lost++;throw new Error('view offline');}});await f.tick();
    f.calls[0].reject(Object.assign(new Error(code),{code}));await settle();assert.equal(lost,1);assert.equal(f.client.status().paused,true);assert.equal(f.tasks.size,0);f.client.dispose();
  }
});

test('a single app revoke retries its fresh view without retiring the whole account feed',async()=>{
  let lost=0,attempts=0;const f=fixture({onInvalidate:()=>{attempts++;if(attempts===1)throw Object.assign(new Error('one app'),{code:'app_access_revoked'});},onAccessLost:()=>{lost++;}});
  await f.tick();f.calls[0].resolve(response('new',{changed:true}));await settle();
  assert.equal(lost,0);assert.equal(f.client.status().paused,false);await f.tick();
  assert.equal(f.calls[1].args.cursor,undefined);f.calls[1].resolve(response('new',{changed:true}));await settle();
  assert.equal(attempts,2);assert.equal(f.states.at(-1),'live');f.client.dispose();
});

test('negative control: the old unobserved canvas fails the same fresh-update oracle',async()=>{
  const assertFresh=reads=>assert.equal(reads,1,'an open canvas must refresh after an incoming change');
  assert.throws(()=>assertFresh(0),/an open canvas must refresh/);
  let reads=0;const f=fixture({onInvalidate:()=>{reads++;}});await f.tick();f.calls[0].resolve(response('new',{changed:true}));await settle();assertFresh(reads);f.client.dispose();
});
