import test from 'node:test';
import assert from 'node:assert/strict';
import {createHumanCallIce} from '../human-call-ice.js';
const scope={accountId:'account_one',deviceId:'device_one',callId:'call_one',epoch:1,joinRef:'join_one'};
const denies=(fn,code)=>assert.throws(fn,e=>e.code===code&&e.message===code);
function fixture(options={}){
  let at=1_800_000_000_000,issues=0,authorized=true;
  const issuer=createHumanCallIce({clock:()=>at,policy:{schema:'soty.human-call-ice-policy.v1',servers:[{urls:['stun:stun.example.test:3478']},
    {urls:['turns:relay.example.test:443?transport=tcp']}],ttlMs:120_000,
    issueCredentials:value=>{issues++;return options.issue?options.issue(value):{username:'synthetic_user',credential:'SYNTHETIC_SECRET',expiresAt:value.expiresAt};}}});
  const current=()=>authorized,issue=(args={requestId:'ice_0',expectedRevision:0},bound=scope)=>issuer.issue(bound,args,current);
  return{issuer,issue,get count(){return issues;},advance:value=>{at+=value;},deny:()=>{authorized=false;},current};
}
test('unconfigured issuer performs no authority/provider work and advertises no qualified media',()=>{
  const issuer=createHumanCallIce();denies(()=>issuer.issue(scope,{requestId:'ice_0',expectedRevision:0},()=>assert.fail()),'human_call_ice_unconfigured');
  assert.deepEqual(issuer.status(),{configured:false,turnConfigured:false,qualified:false,retainedJoins:0});
});
test('short bounded issuance uses current scope, exact retry and two-receipt refresh without growing retained joins',()=>{
  const f=fixture(),first=f.issue();assert.equal(f.count,1);assert.equal(first.revision,1);assert.equal(first.expiresAt,1_800_000_120_000);
  assert.deepEqual(f.issue(),first);assert.equal(f.count,1);
  denies(()=>f.issue({requestId:'ice_1',expectedRevision:1}),'human_call_ice_rate_limited');
  f.advance(60_000);const second=f.issue({requestId:'ice_1',expectedRevision:1});assert.equal(second.revision,2);assert.deepEqual(f.issue(),first);
  f.advance(60_000);f.issue({requestId:'ice_2',expectedRevision:2});
  denies(()=>f.issue(),'human_call_ice_revision_conflict');assert.equal(f.issuer.status().retainedJoins,1);
});
test('foreign actor/device/epoch/join and revoked authority never receive a cached credential',()=>{
  const f=fixture();f.issue();
  for(const mutation of [{accountId:'other_account'},{deviceId:'other_device'},{epoch:2},{callId:'other_call'}])
    denies(()=>f.issue(undefined,{...scope,...mutation}),'human_call_ice_denied');
  f.deny();denies(()=>f.issue(),'human_call_ice_denied');assert.equal(f.count,1);
});
test('authority is rechecked after actual synchronous issuer, with no sensitive result after revoke',()=>{
  let permitted=true;
  const issuer=createHumanCallIce({clock:()=>1_800_000_000_000,policy:{schema:'soty.human-call-ice-policy.v1',servers:[{urls:['turn:relay.example.test:3478?transport=udp']}],ttlMs:60_000,
    issueCredentials:value=>{permitted=false;return{username:'synthetic',credential:'DO_NOT_DISCLOSE',expiresAt:value.expiresAt};}}});
  denies(()=>issuer.issue(scope,{requestId:'ice_0',expectedRevision:0},()=>permitted),'human_call_ice_denied');
  assert.doesNotMatch(JSON.stringify(issuer.status()),/DO_NOT_DISCLOSE|account_one|join_one/u);
});
test('unknown or async issuer cannot repeat work under a new or old request identity',async()=>{
  for(const issue of [()=>{throw Error('private secret diagnostic');},value=>Promise.resolve({username:'x',credential:'y',expiresAt:value.expiresAt})]){
    const f=fixture({issue});assert.throws(()=>f.issue(),e=>['human_call_ice_unknown','human_call_ice_issuer_async'].includes(e.code)&&!e.message.includes('private'));
    denies(()=>f.issue(),'human_call_ice_unknown');denies(()=>f.issue({requestId:'ice_1',expectedRevision:1}),'human_call_ice_unknown');assert.equal(f.count,1);
  }
});
test('expiry, request-body conflict, retire and close are explicit and do not silently renew',()=>{
  const f=fixture();f.issue();denies(()=>f.issue({requestId:'ice_0',expectedRevision:1}),'human_call_ice_request_conflict');
  f.advance(120_000);denies(()=>f.issue(),'human_call_ice_expired');assert.equal(f.count,1);
  f.issuer.retire(scope.joinRef);assert.equal(f.issuer.status().retainedJoins,0);f.issuer.close();denies(()=>f.issue(),'human_call_ice_unconfigured');
});
test('issuer has a hard 64-live-join cache cap without evicting active receipts',()=>{
  const f=fixture();for(let n=0;n<64;n++)f.issue({requestId:'ice_0',expectedRevision:0},{...scope,joinRef:'join_'+n});
  assert.equal(f.count,64);denies(()=>f.issue({requestId:'ice_0',expectedRevision:0},{...scope,joinRef:'join_overflow'}),'human_call_ice_capacity');
  assert.equal(f.issuer.status().retainedJoins,64);
});
