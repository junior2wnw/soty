import test from 'node:test';
import assert from 'node:assert/strict';
import { signedFixture } from './support/direct-signed-fixture.mjs';
const denies = (fn,code)=>assert.throws(fn,error=>error.code===code);

test('actual signed contacts expose current relationship identity only within their original host fence',async t=>{
  let connect,captured,capturedActor;
  const extension = {operations:new Set(['test.directContact']),execute({actor,args}) {
    capturedActor=actor;
    return connect.withCurrentContacts(actor,view=>{captured=view;return {relationshipId:view.relationshipId(args.peerAccountId)};});
  }};
  const f=signedFixture(t,[extension]);connect=f.service;
  const a=await f.person('Alice'),b=await f.person('Bob'),c=await f.person('Other');
  assert.equal((await f.call(a,'test.directContact',{peerAccountId:b.accountId})).relationshipId,null);
  const relationshipId=await f.contacts(a,b);
  assert.equal((await f.call(a,'test.directContact',{peerAccountId:b.accountId})).relationshipId,relationshipId);
  assert.equal((await f.call(b,'test.directContact',{peerAccountId:a.accountId})).relationshipId,relationshipId);
  assert.equal((await f.call(c,'test.directContact',{peerAccountId:b.accountId})).relationshipId,null);
  denies(()=>captured.relationshipId(a.accountId),'connect_contact_authority_expired');
  denies(()=>connect.withCurrentContacts({...capturedActor,accountId:a.accountId},()=>assert.fail()),'connect_authority_actor_mismatch');
});

test('remove, block and explicit new acceptance have distinct actual relationship IDs without Connect schema change',async t=>{
  let connect;
  const extension={operations:new Set(['test.directContact']),execute:({actor,args})=>connect.withCurrentContacts(actor,view=>({relationshipId:view.relationshipId(args.peerAccountId)}))};
  const f=signedFixture(t,[extension]);connect=f.service;
  const a=await f.person('Alice'),b=await f.person('Bob'),first=await f.contacts(a,b);
  const read=()=>f.call(a,'test.directContact',{peerAccountId:b.accountId});
  assert.equal((await read()).relationshipId,first);
  await f.call(a,'contacts.remove',{relationshipId:first}); assert.equal((await read()).relationshipId,null);
  f.advance();const second=await f.contacts(a,b);assert.notEqual(second,first);assert.equal((await read()).relationshipId,second);
  await f.call(b,'contacts.block',{peerAccountId:a.accountId});assert.equal((await read()).relationshipId,null);
  await f.call(b,'contacts.unblock',{peerAccountId:a.accountId});assert.equal((await read()).relationshipId,null);
  f.advance();const third=await f.contacts(a,b);assert.notEqual(third,second);assert.equal((await read()).relationshipId,third);
});

test('relationship host method refuses async fence use and malformed peer without widening contact rights',async t=>{
  let connect,actorRef;
  const extension={operations:new Set(['test.directContact']),execute({actor,args}){actorRef=actor;
    return connect.withCurrentContacts(actor,view=>({relationshipId:view.relationshipId(args.peerAccountId)}));}};
  const f=signedFixture(t,[extension]);connect=f.service;const a=await f.person('Alice');
  assert.equal((await f.call(a,'test.directContact',{peerAccountId:a.accountId})).relationshipId,null);
  assert.equal((await f.raw(a,'test.directContact',{peerAccountId:null})).ok,false);
  denies(()=>connect.withCurrentContacts(actorRef,async()=>({})),'connect_authority_callback_invalid');
});

test('actual signed device revocation removes host relationship access on that device',async t=>{
  let connect;
  const extension={operations:new Set(['test.directContact']),execute:({actor,args})=>connect.withCurrentContacts(actor,view=>({relationshipId:view.relationshipId(args.peerAccountId)}))};
  const f=signedFixture(t,[extension]);connect=f.service;
  const a=await f.person('Alice'),b=await f.person('Bob'),rel=await f.contacts(a,b),phone=await f.enroll(a,'Phone');
  assert.equal((await f.call(phone,'test.directContact',{peerAccountId:b.accountId})).relationshipId,rel);
  await f.call(a,'device.revoke',{deviceId:phone.deviceId});
  const result=await f.raw(phone,'test.directContact',{peerAccountId:b.accountId});assert.equal(result.ok,false);assert.equal(result.error.code,'device_revoked');
  assert.equal((await f.call(a,'test.directContact',{peerAccountId:b.accountId})).relationshipId,rel);
});
