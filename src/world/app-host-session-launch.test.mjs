import test from 'node:test';
import assert from 'node:assert/strict';
import {createAppLauncher} from './app-launch.mjs';

const appId='app-'+ 'a'.repeat(32),domainId='dom_'+ 'b'.repeat(32),accountId='own-user';
const entry={appId,domainId,origin:'https://reader.apps.example',path:'/reader'};
const runtimeOrigin='https://app-m'+appId.slice(4)+'-'+ 'c'.repeat(24)+'.apps.example';
const pin={schema:'soty.app-launch-binding.v1',policyEpoch:1,targetRevision:2,targetDigest:'d'.repeat(64),profile:'soty.relay-restricted.v1',bindingFloor:2};
const member={schema:'soty.app-host-session.v1',frameId:'F'.repeat(43),lease:'A'.repeat(43),revision:1,expiresAt:Date.now()+86400000};
const reply=()=>({url:runtimeOrigin+'/_soty/boot?path=%2Freader#'+'T'.repeat(43),entry:{...entry},runtimeOrigin,launchBinding:{...pin},hostSession:{...member},hostSessionExpiresAt:Date.now()+120000});
function fixture(read){
 const calls=[],closed=[];let current=true;
 const launcher=createAppLauncher({target:{appId,domainId,path:entry.path},accountId,instanceId:'main',shellUrl:'https://soty.example/#mine',isCurrent:()=>current,
  request:async args=>{calls.push(args);return read?read(args):reply();},resolveEntry:async args=>{calls.push({resolve:true,...args});return{entry:{...entry}};},
  abandonHost:async value=>{closed.push(value);return{closed:true};}});
 return{launcher,calls,closed,retire(){current=false;}};
}
const flush=()=>new Promise(resolve=>setImmediate(resolve));

test('a Root-owned launch opts in, pins closed host metadata and keeps lease proof only in its current RAM capture',async()=>{
 const f=fixture();await f.launcher.launch();const capture=f.launcher.hostSession();
 assert.equal(f.calls[0].hostSession,true);assert.deepEqual(capture.hostSession,member);assert.deepEqual(capture.entry,entry);assert.equal(capture.runtimeOrigin,runtimeOrigin);
 assert.equal(Object.hasOwn(f.launcher.parameters,'lease'),false);assert.equal(Object.hasOwn(f.launcher.parameters,'hostSession'),false,'passive entry selectors contain no admission flag or proof');
 f.retire();assert.equal(f.launcher.hostSession(),null);f.launcher.dispose();
});

test('entry-only resolution never asks the server to create a member',async()=>{
 const f=fixture();await f.launcher.resolve();assert.equal(f.calls.length,1);assert.equal(f.calls[0].resolve,true);assert.equal(Object.hasOwn(f.calls[0],'hostSession'),false);
 await f.launcher.launch();assert.equal(f.calls[1].hostSession,true);f.launcher.dispose();
});

test('external popups keep their legacy ephemeral admission and do not leak or close durable Root-owned members',async()=>{
 const f=fixture(args=>{const value=reply();if(!args.hostSession){delete value.hostSession;delete value.hostSessionExpiresAt;delete value.runtimeOrigin;value.url=value.url.replace(runtimeOrigin,entry.origin);}return value;});
 await f.launcher.launch();const before=f.launcher.hostSession(),visits=[];
 const popup={closed:false,opener:{},location:{replace:value=>visits.push(value)},close(){this.closed=true;}};
 assert.equal(await f.launcher.openExternal(()=>popup),'opened');assert.equal(f.calls.length,2);assert.equal(Object.hasOwn(f.calls[1],'hostSession'),false);
 assert.equal(f.launcher.hostSession(),before);assert.equal(f.closed.length,0);assert.equal(visits.length,1);assert.equal(popup.closed,false);assert.equal(popup.opener,null);
 f.launcher.dispose();assert.equal(popup.closed,false,'completed external windows belong to the person');
});

test('a server unexpectedly allocating a durable external member fails closed before popup navigation and retires that unused proof',async()=>{
 const f=fixture(),visits=[];const popup={closed:false,opener:{},location:{replace:value=>visits.push(value)},close(){this.closed=true;}};
 await assert.rejects(f.launcher.openExternal(()=>popup),{code:'invalid_app_host_session'});await flush();
 assert.equal(popup.closed,true);assert.equal(visits.length,0);assert.equal(f.closed.length,1);assert.equal(f.closed[0].lease,member.lease);f.launcher.dispose();
});

test('late initial own-member ACK is retired after the original account/stage departed',async()=>{
 let finish;const f=fixture(()=>new Promise(resolve=>{finish=resolve;})),pending=f.launcher.launch();await Promise.resolve();f.retire();finish(reply());
 assert.equal(await pending,null);await flush();assert.equal(f.closed.length,1);assert.deepEqual(f.closed[0],{appId,frameId:member.frameId,lease:member.lease,revision:member.revision});assert.equal(f.launcher.hostSession(),null);f.launcher.dispose();
});

test('expanded/incomplete host metadata cannot become a continuity capture',async()=>{
 for(const change of [value=>({...value,hostSession:{...member,actor:'untrusted'}}),value=>({...value,hostSessionExpiresAt:member.expiresAt+1}),value=>({...value,hostSessionExpiresAt:undefined}),value=>({...value,hostSession:undefined})]){
  const f=fixture(()=>change(reply()));await assert.rejects(f.launcher.launch(),{code:'invalid_app_host_session'});assert.equal(f.launcher.hostSession(),null);f.launcher.dispose();
 }
});

test('only an exact one-revision same-scope successor may replace the current Root lease',async()=>{
 const f=fixture();await f.launcher.launch();const first=f.launcher.hostSession(),next={...first,hostSession:{...member,lease:'B'.repeat(43),revision:2}};
 for(const change of [value=>({...value,entry:{...entry,path:'/other'}}),value=>({...value,launchBinding:{...pin,policyEpoch:2}}),value=>({...value,runtimeOrigin:'https://other.apps.example'}),value=>({...value,hostSession:{...value.hostSession,frameId:'Q'.repeat(43)}}),value=>({...value,hostSession:{...value.hostSession,revision:3}})]){
  assert.throws(()=>f.launcher.commitHostSession(first,change(next)),{code:'app_host_session_changed'});assert.equal(f.launcher.hostSession(),first);
 }
 assert.equal(f.launcher.commitHostSession(first,next),true);assert.equal(f.launcher.commitHostSession(first,next),false,'stale CAS cannot replace or acknowledge a successor');f.launcher.dispose();
});

test('the main runtime namespace is exact for the admitted app, zone, protocol, port and boot path',async()=>{
 for(const [change,code] of [[value=>({...value,runtimeOrigin:value.runtimeOrigin.replace('a'.repeat(32),'b'.repeat(32))}),'invalid_app_runtime_origin'],
  [value=>({...value,runtimeOrigin:value.runtimeOrigin.replace('app-m','app-w')}),'invalid_app_runtime_origin'],[value=>({...value,runtimeOrigin:value.runtimeOrigin.replace('.apps.example','.elsewhere.example')}),'invalid_app_runtime_origin'],
  [value=>({...value,runtimeOrigin:value.runtimeOrigin+':444'}),'invalid_app_runtime_origin'],[value=>({...value,runtimeOrigin:value.runtimeOrigin.replace('https:','http:')}),'invalid_app_launch_url'],
  [value=>{delete value.runtimeOrigin;value.url=value.url.replace(runtimeOrigin,entry.origin);return value;},'invalid_app_runtime_origin']]){
  const f=fixture(()=>{const value=change(reply());if(value.runtimeOrigin)value.url=value.url.replace(runtimeOrigin,value.runtimeOrigin);return value;});
  await assert.rejects(f.launcher.launch(),{code});assert.equal(f.launcher.hostSession(),null);f.launcher.dispose();
 }
});

test('signed same-proof expiry metadata may advance without pretending a new lease or accepting a rollback',async()=>{
 const f=fixture();await f.launcher.launch();const first=f.launcher.hostSession(),expiresAt=first.hostSession.expiresAt+1000;
 assert.equal(f.launcher.commitHostSession(first,{...first,hostSession:{...first.hostSession,expiresAt},hostSessionExpiresAt:null}),true);
 const current=f.launcher.hostSession();assert.equal(current.hostSession.lease,first.hostSession.lease);assert.equal(current.hostSession.revision,first.hostSession.revision);assert.equal(current.hostSessionExpiresAt,null);
 assert.throws(()=>f.launcher.commitHostSession(current,{...current,hostSession:{...current.hostSession,expiresAt:first.hostSession.expiresAt}}),{code:'app_host_session_changed'});f.launcher.dispose();
});
