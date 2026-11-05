import test from 'node:test';
import assert from 'node:assert/strict';
import {createRetainedAppRuntimes} from './app-retained-runtime.mjs';
function owner(id){return{closeEpoch:0,host:{dataset:{},style:{},classList:{toggle(){}},setAttribute(){},remove(){this.removed=true;}},stage:{matches:intent=>intent.id===id,dispose(){this.disposed=(this.disposed??0)+1;}}};}
test('A→canvas→B→A keeps all physical owners and does not dispose or reparent',()=>{
  const shown=[],pool=createRetainedAppRuntimes(row=>shown.push(row)),a=owner('a'),b=owner('b');
  pool.add(a);pool.present(a);pool.present(null);pool.add(b);pool.present(b);pool.present(a);
  assert.equal(pool.active(),a);assert.equal(pool.find({id:'a'}),a);assert.equal(a.stage.disposed,undefined);assert.equal(b.stage.disposed,undefined);
  assert.equal(a.host.inert,false);assert.equal(b.host.inert,true);assert.equal(b.host.style.visibility,'hidden');assert.equal(pool.all().length,2);assert.equal(shown.length,4);
});
test('capacity8 never evicts another owner; retirement frees exactly one and cancel changes nothing',()=>{
  const pool=createRetainedAppRuntimes(),rows=Array.from({length:9},(_,i)=>owner(String(i)));
  for(const row of rows.slice(0,8))assert.equal(pool.add(row),true);
  pool.present(rows[0]);assert.equal(pool.add(rows[8]),false);assert.equal(pool.all().length,8);
  assert.ok(rows.every(row=>!row.stage.disposed));pool.retire(rows[3]);assert.equal(rows[3].stage.disposed,1);assert.equal(pool.active(),rows[0]);
  assert.equal(pool.add(rows[8]),true);pool.clear();assert.equal(pool.active(),null);assert.equal(pool.all().length,0);assert.ok(rows.every(row=>row.stage.disposed===1));
});
test('new presentation retires a close intent; different entry and invitation cannot match by appId alone',()=>{
  const pool=createRetainedAppRuntimes(),a=owner('entry-a');pool.add(a);pool.present(a);const before=a.closeEpoch;
  pool.present(null);assert.ok(a.closeEpoch>before);assert.equal(pool.find({id:'entry-b'}),null);assert.equal(pool.find({id:'entry-a',takeInvitation(){}}),null);
  assert.equal(pool.present(owner('foreign')),false);assert.equal(pool.active(),null);assert.equal(pool.all()[0],a);
});
test('a later matching incarnation is selected without silently retiring an older owner',()=>{
  const pool=createRetainedAppRuntimes(),old=owner('a'),fresh=owner('a');pool.add(old);pool.add(fresh);
  assert.equal(pool.find({id:'a'}),fresh);assert.equal(old.stage.disposed,undefined);assert.equal(pool.all().length,2);
});
