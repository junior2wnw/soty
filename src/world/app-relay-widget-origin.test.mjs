import test from 'node:test';
import assert from 'node:assert/strict';
import {createRelayIdentityBridge} from './app-relay-identity-bridge.mjs';

const tick=()=>new Promise(done=>setTimeout(done,0));
const instanceId='widget-11111111-1111-4111-8111-111111111111';
const runtimeOrigin='https://app-w11111111111141118111111111111111-'+ 'c'.repeat(24)+'.apps.example';
function fixture(request){
  let receive,scope={instanceId,runtimeOrigin},current=true,hostSession=null;
  const calls=[],sent=[],entry={appId:'app-'+ 'a'.repeat(32),domainId:'dom_'+ 'b'.repeat(32),origin:'https://reader.apps.example'};
  const frame={isConnected:true,contentWindow:{postMessage:(message,origin)=>sent.push({message,origin})}};
  const view={performance:{now:()=>0},setTimeout,clearTimeout,addEventListener:(_,f)=>{receive=f;},removeEventListener:()=>{receive=null;}};
  const bridge=createRelayIdentityBridge({view,isCurrent:()=>current,getFrame:()=>frame,getEntry:()=>entry,getBinding:()=>({targetRevision:2}),getRuntimeScope:()=>scope,
    getHostSession:()=>hostSession,
    request:async value=>{calls.push(value);return request?request(value):{schema:'soty.relay-window-grant.v1',grant:'A'.repeat(43),expiresAt:100};},close:async()=>({closed:true})});
  const message=(origin=runtimeOrigin,source=frame.contentWindow,extra={})=>receive?.({origin,source,data:{schema:'soty.relay-window-identity.v1',type:'request',requestId:'a'.repeat(36),...extra}});
  return{bridge,calls,sent,entry,frame,message,rejectGrant:grant=>receive?.({origin:scope?.runtimeOrigin??entry.origin,source:frame.contentWindow,data:{schema:'soty.relay-window-identity.v1',type:'invalidate',grant}}),
    setScope:value=>{scope=value;},setHost:value=>{hostSession=value;},invalidate:()=>{current=false;}};
}

test('widget identity uses accepted browser origin and forwards its Root-owned scope with canonical app entry',async()=>{
  const f=fixture();
  try{
    f.message(f.entry.origin);f.message(runtimeOrigin,{});f.message(runtimeOrigin,f.frame.contentWindow,{runtimeOrigin:'https://evil.example'});
    await tick();assert.equal(f.calls.length,0);
    f.message();await tick();assert.equal(f.calls.length,1);
    assert.deepEqual(f.calls[0],{appId:f.entry.appId,domainId:f.entry.domainId,launchBinding:{targetRevision:2},instanceId,runtimeOrigin});
    assert.equal(f.sent.length,1);assert.equal(f.sent[0].origin,runtimeOrigin);assert.equal(f.sent[0].message.type,'grant');
  }finally{f.bridge.dispose();}
});

test('same Window cannot receive a late grant after its runtime scope changes',async()=>{
  let finish;const f=fixture(()=>new Promise(done=>{finish=done;}));
  try{
    f.message();await tick();assert.equal(f.calls.length,1);
    f.setScope({instanceId,runtimeOrigin:runtimeOrigin.replace('c'.repeat(24),'e'.repeat(24))});
    finish({schema:'soty.relay-window-grant.v1',grant:'A'.repeat(43),expiresAt:100});
    await tick();assert.equal(f.sent.length,0,'late old-scope response must not reach current peer');
  }finally{f.bridge.dispose();}
});

test('invalid or expanded Root scope fails closed instead of falling back to main',async()=>{
  const f=fixture();
  try{
    for(const scope of [{instanceId:'main',runtimeOrigin},{instanceId,runtimeOrigin:runtimeOrigin+'/'},{instanceId,runtimeOrigin,extra:true},{instanceId,runtimeOrigin:f.entry.origin}]){
      f.setScope(scope);f.message();f.message(f.entry.origin);await tick();assert.equal(f.calls.length,0);
    }
    f.setScope({instanceId,runtimeOrigin});f.invalidate();f.message();await tick();assert.equal(f.calls.length,0);
  }finally{f.bridge.dispose();}
});

test('a managed main peer uses only its app-specific runtime origin and explicit main pair',async()=>{
  const f=fixture(),mainOrigin='https://app-m'+f.entry.appId.slice(4)+'-'+ 'd'.repeat(24)+'.apps.example';
  try{
    f.setHost({frameId:'F'.repeat(43),lease:'L'.repeat(43),revision:1});
    f.setScope({instanceId:'main',runtimeOrigin:mainOrigin});f.message(f.entry.origin);f.message(runtimeOrigin);await tick();assert.equal(f.calls.length,0);
    f.message(mainOrigin);await tick();assert.equal(f.calls.length,1);assert.equal(f.calls[0].instanceId,'main');assert.equal(f.calls[0].runtimeOrigin,mainOrigin);assert.equal(f.sent[0].origin,mainOrigin);
  }finally{f.bridge.dispose();}
});

test('managed main cannot request a grant without an exact private current member proof; Source sees only the resulting opaque grant',async()=>{
 const f=fixture(),mainOrigin='https://app-m'+f.entry.appId.slice(4)+'-'+ 'd'.repeat(24)+'.apps.example';f.setScope({instanceId:'main',runtimeOrigin:mainOrigin});
 try{
  for(const proof of [null,{frameId:'short',lease:'L'.repeat(43),revision:1},{frameId:'F'.repeat(43),lease:'L'.repeat(43),revision:0},{frameId:'F'.repeat(43),lease:'L'.repeat(43),revision:1,actor:'forged'}]){
   f.setHost(proof);f.message(mainOrigin);await tick();assert.equal(f.calls.length,0);
  }
  const proof={frameId:'F'.repeat(43),lease:'L'.repeat(43),revision:1};f.setHost(proof);f.message(mainOrigin);await tick();assert.deepEqual(f.calls[0].hostSession,proof);
  assert.deepEqual(Object.keys(f.sent[0].message).sort(),['grant','requestId','schema','type']);assert.equal(JSON.stringify(f.sent).includes(proof.lease),false);
 }finally{f.bridge.dispose();}
});

test('same physical member CAS retains cached grant TTL, while the next issue reads the exact current lease and siblings cannot reuse it',async()=>{
 let issued=0;const f=fixture(async()=>({schema:'soty.relay-window-grant.v1',grant:String.fromCharCode(65+issued++).repeat(43),expiresAt:100}));
 try{
  const first={frameId:'F'.repeat(43),lease:'L'.repeat(43),revision:1};f.setHost(first);f.message();await tick();assert.equal(f.calls.length,1);
  const next={...first,lease:'N'.repeat(43),revision:2};f.setHost(next);f.message();await tick();assert.equal(f.calls.length,1,'routine same-member lease CAS must not issue or extend a new grant');
  f.rejectGrant('A'.repeat(43));f.message();await tick();assert.equal(f.calls.length,2);assert.deepEqual(f.calls[1].hostSession,next);
  f.setHost({...first,frameId:'S'.repeat(43)});f.message();await tick();assert.equal(f.calls.length,3,'a different physical member has a different cache identity');
 }finally{f.bridge.dispose();}
});

test('a main runtime cannot impersonate another app, widget, zone or canonical origin',async()=>{
  const f=fixture(),mainOrigin='https://app-m'+f.entry.appId.slice(4)+'-'+ 'd'.repeat(24)+'.apps.example';
  try{
    for(const origin of [mainOrigin.replace('a'.repeat(32),'b'.repeat(32)),mainOrigin.replace('.apps.example','.foreign.example'),runtimeOrigin,f.entry.origin]){
      f.setScope({instanceId:'main',runtimeOrigin:origin});f.message(origin);await tick();assert.equal(f.calls.length,0);
    }
  }finally{f.bridge.dispose();}
});
