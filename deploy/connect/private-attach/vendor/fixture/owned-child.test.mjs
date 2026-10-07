import test from 'node:test';
import assert from 'node:assert/strict';
import { withOwnedChild } from './owned-child.mjs';
const OPTIONS={stdio:['pipe','pipe','pipe','pipe','pipe'],windowsHide:true};
test('actual post-spawn setup throw closes the returned native child before rejection without private getter reads',async()=>{
  let status,reads=0;
  const poison={};for(const key of ['message','code','stack','reason'])Object.defineProperty(poison,key,{get(){reads++;throw Error('private_getter_must_not_run');}});
  await assert.rejects(withOwnedChild(process.execPath,['-e','setInterval(()=>{},1000)'],OPTIONS,async(_child,owner)=>{
    status=owner;throw poison; // Same work boundary as client/collector/Writable setup.
  }),error=>error===poison);
  assert.equal(status.snapshot().closed,true);assert.equal(status.snapshot().forced,true);assert.equal(reads,0);
});
test('post-spawn stream/client setup failure on an already exiting child still waits actual close',async()=>{
  let status;
  const original=new Error('synthetic_setup');
  await assert.rejects(withOwnedChild(process.execPath,['-e','process.exit(0)'],OPTIONS,async(_child,owner)=>{
    status=owner;throw original;
  }),error=>error===original);
  assert.equal(status.snapshot().closed,true);assert.equal(status.snapshot().forced,false);
});
test('successful work may not claim success when native teardown required a forced child signal',async()=>{
  let status;
  await assert.rejects(withOwnedChild(process.execPath,['-e','setInterval(()=>{},1000)'],OPTIONS,async(_child,owner)=>{
    status=owner;return true;
  }),{code:'fixture_child_forced_or_failed_cleanup'});
  assert.equal(status.snapshot().closed,true);assert.equal(status.snapshot().forced,true);
});
test('native successful exit with all five stdios preserves work result and closure',async()=>{
  let status;
  const value=await withOwnedChild(process.execPath,['-e','process.exit(0)'],OPTIONS,async(child,owner)=>{
    assert.equal(child.stdio.length,5);status=owner;await owner.closedPromise;return 7;
  });
  assert.equal(value,7);assert.equal(status.snapshot().closed,true);assert.equal(status.snapshot().forced,false);
});
