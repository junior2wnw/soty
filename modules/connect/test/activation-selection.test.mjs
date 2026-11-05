import test from 'node:test';
import assert from 'node:assert/strict';
import {createActivationFixture as fixture,password,selection} from './support/activation-fixture.mjs';

for(const mutateInput of[false,true])test(`credential activation retains the consent selection across the SDK queue${mutateInput?' and copies caller metadata synchronously':''}`,async t=>{
 const f=await fixture(t);
 await f.main.switchProfile(f.c.accountId);const c=selection(await f.main.getLocalState());await f.main.switchProfile(f.a.accountId);
 const expectedState=selection(await f.main.getLocalState()),before=f.operations.filter(op=>op==='credentials.login').length;
 const switching=f.main.switchProfile(f.c.accountId);
 const attempt=f.main.loginWithCredential('password',password,'Own recipient',{expectedState});
 if(mutateInput)Object.assign(expectedState,c);
 await switching;await assert.rejects(attempt,error=>error.code==='ACTIVE_PROFILE_CHANGED');
 assert.equal((await f.main.getLocalState()).accountId,f.c.accountId);
 assert.equal(f.operations.filter(op=>op==='credentials.login').length,before,'stale consent cannot register a credential recipient');
});

test('saved-profile activation retains the consent selection across the SDK queue',async t=>{
 const f=await fixture(t),expectedState=selection(await f.main.getLocalState());
 const switching=f.main.switchProfile(f.c.accountId),attempt=f.main.switchProfile(f.b.accountId,{expectedState});
 await switching;await assert.rejects(attempt,error=>error.code==='ACTIVE_PROFILE_CHANGED');
 assert.equal((await f.main.getLocalState()).accountId,f.c.accountId);
});

test('a matching consent selection permits genuine credential entry and retains prior profiles',async t=>{
 const f=await fixture(t),expectedState=selection(await f.main.getLocalState());
 const result=await f.main.loginWithCredential('password',password,'Own matching recipient',{expectedState});
 assert.equal(result.accountId,f.b.accountId);const state=await f.main.getLocalState();assert.equal(state.accountId,f.b.accountId);
 assert.ok(state.profiles.some(profile=>profile.accountId===f.a.accountId));assert.ok(state.profiles.some(profile=>profile.accountId===f.c.accountId));
});

test('the exact empty public selection permits credential entry without inventing an initial profile',async t=>{
 const f=await fixture(t),fresh=f.newClient(),expectedState=selection(await fresh.getLocalState());
 assert.deepEqual(expectedState,{accountId:null,deviceId:null,localRevision:null});
 const result=await fresh.loginWithCredential('password',password,'Own empty recipient',{expectedState});
 assert.equal(result.accountId,f.b.accountId);assert.equal((await fresh.getLocalState()).accountId,f.b.accountId);
});

test('expected selection metadata is closed and malformed values cannot queue a profile mutation',async t=>{
 const f=await fixture(t),good=selection(await f.main.getLocalState()),before=f.operations.length;
 for(const expectedState of [{...good,extra:true},{...good,localRevision:'1'},{...good,localRevision:-1},{accountId:good.accountId,deviceId:good.deviceId},{...good,accountId:''},null]){
  await assert.rejects(f.main.switchProfile(f.b.accountId,{expectedState}),error=>error.code==='INVALID_ARGUMENT');
 }
 assert.equal(f.operations.length,before);assert.equal((await f.main.getLocalState()).accountId,f.a.accountId);
});
