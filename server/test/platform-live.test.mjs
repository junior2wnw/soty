import test from 'node:test';
import assert from 'node:assert/strict';
import {createPlatformLive} from '../platform-live.js';
import {createWorldService} from '../../modules/world/server/index.mjs';

const alice={accountId:'alice',deviceId:'alice-device'},bob={accountId:'bob',deviceId:'bob-device'},ref={kind:'app',id:'app-one'};
const call=(live,actor,args={})=>live.executeAsync({actor,args:{expectedAccountId:actor.accountId,...args}});
const tick=()=>new Promise(resolve=>queueMicrotask(resolve));

test('private changes reach current/previous authorized readers and never an outsider',async t=>{
  let allowed=true;const live=createPlatformLive({actorActive:()=>true,canSee:(actor)=>actor===alice||allowed&&actor===bob});t.after(()=>live.close());
  let b=await call(live,bob,{watch:[ref]}),a=await call(live,alice),outsider=await call(live,{accountId:'c',deviceId:'c'});
  const waiting=call(live,bob,{cursor:b.cursor,waitMs:1000});allowed=false;
  live.publish({accountIds:['alice'],ref});b=await waiting;assert.equal(b.changed,true);assert.equal(b.reset,false);
  assert.equal((await call(live,{accountId:'c',deviceId:'c'},{cursor:outsider.cursor,waitMs:0})).changed,false);
  live.publish({accountIds:['alice'],ref});await tick();
  assert.equal((await call(live,bob,{cursor:b.cursor,waitMs:0})).changed,false,'former access gets one removal hint, not future private activity');
  a=await call(live,alice,{cursor:a.cursor,waitMs:0});assert.equal(a.changed,true);
  assert.deepEqual(Object.keys(a).sort(),['changed','cursor','reset','schema']);
});

test('public transition wakes readers, while an unrelated private change cannot',async t=>{
  const live=createPlatformLive({actorActive:()=>true,canSee:()=>false});t.after(()=>live.close());
  const initial=await call(live,bob);live.publish({accountIds:['alice'],ref});await tick();
  assert.equal((await call(live,bob,{cursor:initial.cursor,waitMs:0})).changed,false);
  const pending=call(live,bob,{cursor:initial.cursor,waitMs:1000});live.publish({accountIds:['alice'],ref,publicTransition:true});assert.equal((await pending).changed,true);
});

test('cursor is device/account bound, authenticated, resets on restart and detects missed events',async t=>{
  const live=createPlatformLive({actorActive:()=>true,canSee:()=>false});t.after(()=>live.close());const initial=await call(live,alice);
  await assert.rejects(call(live,bob,{cursor:initial.cursor,waitMs:0}),{code:'live_cursor_invalid'});
  await assert.rejects(call(live,{...alice,deviceId:'second'},{cursor:initial.cursor,waitMs:0}),{code:'live_cursor_invalid'});
  const [body,mac]=initial.cursor.split('.');await assert.rejects(call(live,alice,{cursor:`${body}.${mac[0]==='a'?'b':'a'}${mac.slice(1)}`,waitMs:0}),{code:'live_cursor_invalid'});
  live.publish({accountIds:['alice'],ref});await tick();assert.equal((await call(live,alice,{cursor:initial.cursor,waitMs:0})).changed,true);
  const restarted=createPlatformLive({actorActive:()=>true,canSee:()=>false});t.after(()=>restarted.close());assert.equal((await call(restarted,alice,{cursor:initial.cursor,waitMs:0})).reset,true);
});

test('pending waits recheck revocation, close promptly, bound fanout and time out unchanged',async()=>{
  let authorized=true;const live=createPlatformLive({actorActive:()=>authorized,canSee:()=>false}),initial=await call(live,alice);
  assert.equal((await call(live,alice,{cursor:initial.cursor,waitMs:1})).changed,false);
  const pending=call(live,alice,{cursor:initial.cursor,waitMs:1000});authorized=false;live.recheck();await assert.rejects(pending,{code:'live_authentication_required'});
  authorized=true;const again=await call(live,alice),cancelled=call(live,alice,{cursor:again.cursor,waitMs:1000});live.close();await assert.rejects(cancelled,{code:'live_closed'});assert.equal(live.stats().waiting,0);
});

test('negative control rejects a global private broadcast using the same isolation oracle',async()=>{
  const live=createPlatformLive({actorActive:()=>true,canSee:()=>false}),initial=await call(live,bob);
  live.publish({accountIds:['alice'],ref,publicTransition:true});await tick();
  const broken=await call(live,bob,{cursor:initial.cursor,waitMs:0});assert.throws(()=>assert.equal(broken.changed,false));live.close();
});
test('trusted HTTP cancellation releases the waiter without publishing a change',async()=>{
  const live=createPlatformLive({actorActive:()=>true,canSee:()=>false}),initial=await call(live,alice),controller=new AbortController();
  const pending=live.executeAsync({actor:alice,args:{expectedAccountId:alice.accountId,cursor:initial.cursor,waitMs:1000},signal:controller.signal});
  assert.equal(live.stats().waiting,1);controller.abort();await assert.rejects(pending,{code:'live_cancelled'});assert.equal(live.stats().waiting,0);
  assert.equal((await call(live,alice,{cursor:initial.cursor,waitMs:0})).changed,false);live.close();
});
test('temporary authority-read contention retains a private change until fresh authorization succeeds',async()=>{
  let busy=true;const live=createPlatformLive({actorActive:()=>true,canSee:()=>{if(busy)throw new Error('authority busy');return true;}}),initial=await call(live,bob);
  live.publish({accountIds:['alice'],ref});await tick();assert.equal((await call(live,bob,{cursor:initial.cursor,waitMs:0})).changed,false);
  busy=false;assert.equal((await call(live,bob,{cursor:initial.cursor,waitMs:0})).changed,true);live.close();
});
test('negative control: disconnecting the real SQLite writer hook reproduces the old non-live profile',async()=>{
  async function observe(wired){
    let world;const live=createPlatformLive({actorActive:()=>true,canSee:(actor,ref)=>world.execute({actor,op:'world.directory.resolve',args:{expectedAccountId:actor.accountId,entities:[ref]}}).items[0].available});
    world=createWorldService({databasePath:':memory:',projectId:'live-negative',onChange:wired?change=>live.publish(change):()=>{}});
    try{const owner={accountId:'acct_live_author',deviceId:'dev_live_author',label:'Author'},viewer={accountId:'acct_live_viewer',deviceId:'dev_live_viewer',label:'Reader'};
      const profile=world.execute({actor:owner,op:'world.profile.get'}).profile;world.execute({actor:viewer,op:'world.profile.get'});
      const initial=await call(live,viewer);
      world.execute({actor:owner,op:'world.profile.update',args:{expectedRevision:profile.revision,audience:'public'}});await tick();
      assert.equal(world.execute({actor:viewer,op:'world.directory.resolve',args:{expectedAccountId:viewer.accountId,entities:[{kind:'person',id:owner.accountId}]}}).items[0].available,true,'source mutation really committed in both control cases');
      return (await call(live,viewer,{cursor:initial.cursor,waitMs:0})).changed;
    }finally{live.close();world.close();}
  }
  const broken=await observe(false);assert.throws(()=>assert.equal(broken,true));assert.equal(await observe(true),true);
});
