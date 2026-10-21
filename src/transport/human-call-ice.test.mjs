import test from 'node:test';
import assert from 'node:assert/strict';
import {canonicalCallIceUrl,validateCallIceReply} from './human-call-ice.mjs';
const scope={accountId:'account_one',deviceId:'device_one',callId:'call_one',epoch:1,joinRef:'join_one'},now=1_800_000_000_000;
const reply=()=>({schema:'soty.human-call-ice.v1',...scope,revision:1,expiresAt:now+120_000,
  iceServers:[{urls:['stun:stun.example.test:3478']},{urls:['turns:relay.example.test:443?transport=tcp'],username:'synthetic_user',credential:'SYNTHETIC_SECRET'}]});
const denied=fn=>assert.throws(fn,error=>error.code==='human_call_ice_invalid'&&error.message==='human_call_ice_invalid');
test('bounded signed ICE tuple is copied and frozen, never redirects legacy peer defaults',()=>{
  const source=reply(),valid=validateCallIceReply(source,scope,{now});assert.equal(valid.iceServers.length,2);source.iceServers[1].credential='changed';
  assert.equal(valid.iceServers[1].credential,'SYNTHETIC_SECRET');assert.equal(Object.isFrozen(valid.iceServers[1].urls),true);
});
test('closed ICE tuple rejects every foreign identity, epoch, join, expiry and extra field',()=>{
  for(const change of [{accountId:'account_other'},{deviceId:'device_other'},{callId:'call_other'},{epoch:2},{joinRef:'join_other'},
    {expiresAt:now},{expiresAt:now+300_001},{revision:0},{url:'https://not-ice.test'}])denied(()=>validateCallIceReply({...reply(),...change},scope,{now}));
});
test('narrow ICE URL policy rejects credentials, arbitrary schemes, paths, malformed hosts and transport abuse',()=>{
  for(const url of ['https://relay.test','turn:user:pass@relay.test','turn:relay.test/','turn:relay.test#x','turn:relay.test:0','turn:relay.test:65536',
    'turn:-bad.test','turn:bad..test','stun:relay.test?transport=tcp','turns:relay.test?transport=udp','turn:relay.test?transport=tcp&x=1'])denied(()=>canonicalCallIceUrl(url));
  assert.equal(canonicalCallIceUrl('turn:Relay.Example.test:03478?transport=udp'),'turn:relay.example.test:3478?transport=udp');
  assert.equal(canonicalCallIceUrl('turn:[::1]:3478?transport=tcp'),'turn:[::1]:3478?transport=tcp');
});
test('credentials belong only to exact TURN entries and configured allowed URLs',()=>{
  const source=reply();denied(()=>validateCallIceReply(source,scope,{now,allowedUrls:['stun:stun.example.test:3478']}));
  for(const server of [{urls:['stun:relay.test'],username:'x',credential:'y'},{urls:['turn:relay.test'],username:'x'},
    {urls:['stun:relay.test','turn:relay.test'],username:'x',credential:'y'},{urls:['turn:relay.test'],username:'x',credential:'bad secret'}])
    denied(()=>validateCallIceReply({...source,iceServers:[server]},scope,{now}));
});
test('oversized arrays, sparse/accessor data and errors do not disclose TURN material',()=>{
  const bad=reply();bad.iceServers=Array.from({length:5},()=>({urls:['stun:relay.test']}));denied(()=>validateCallIceReply(bad,scope,{now}));
  const sparse=[];sparse.length=1;denied(()=>validateCallIceReply({...reply(),iceServers:sparse},scope,{now}));
  const entry={urls:['turn:relay.test'],username:'x'};Object.defineProperty(entry,'credential',{get(){assert.fail('getter must not run');},enumerable:true});
  denied(()=>validateCallIceReply({...reply(),iceServers:[entry]},scope,{now}));
  try{validateCallIceReply({...reply(),expiresAt:now},scope,{now});assert.fail();}catch(error){assert.doesNotMatch(error.message,/SYNTHETIC_SECRET|relay\.example/u);}
});

test('nonenumerable ICE identity cannot be omitted from scope binding or accepted in a reply',()=>{
  const hiddenScope={...scope};Object.defineProperty(hiddenScope,'accountId',{value:scope.accountId,enumerable:false});
  denied(()=>validateCallIceReply({...reply(),accountId:'foreign_account'},hiddenScope,{now}));
  const hiddenReply=reply();Object.defineProperty(hiddenReply,'accountId',{value:scope.accountId,enumerable:false});
  denied(()=>validateCallIceReply(hiddenReply,scope,{now}));
});
