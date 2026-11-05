import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import {createHash} from 'node:crypto';
const source=readFileSync(new URL('../../../scripts/agent-modules/local-apps.mjs',import.meta.url),'utf8');
const profile='soty.relay-self-ip.v1';
const capabilities=(profiles=[profile])=>new Response(JSON.stringify({schema:'soty.apps-runtime-capabilities.v1',channelSchema:'soty.apps-channel.v1',targetBindingVersions:[1,2],runtimeProfiles:profiles,selfIpTargetSchema:'soty.runtime-target.v2'}),{headers:{'content-type':'application/json'}});
const later=()=>{let resolve;const promise=new Promise(done=>{resolve=done;});return{promise,resolve};};
async function flush(){for(let n=0;n<10;n++)await new Promise(done=>setImmediate(done));}
class Socket{
 constructor(url){this.url=url;this.readyState=1;this.bufferedAmount=0;this.listeners=new Map();this.sent=[];}
 addEventListener(type,cb){this.listeners.set(type,cb);}emit(type,event={}){this.listeners.get(type)?.(event);}send(text){this.sent.push(JSON.parse(text));}close(){this.readyState=3;this.emit('close');}
 frame(value){this.emit('message',{data:JSON.stringify(value)});}
}
function fixture(t){
 const intervals=new Map(),timeouts=new Map(),sockets=[],reads=[],responses=[new Response('temporary outage',{status:503})];let server='http://127.0.0.1:5901',identity={linkId:'recovery_link',hostDeviceId:'recovery_host',connectorId:'recovery_connector'};
 const timer=(store,callback,ms)=>{const value={callback,ms,unref(){},refresh(){}};store.set(value,value);return value;};
 const context={exports:{},URL,TextEncoder,TextDecoder,Uint8Array,AbortSignal,Date,Response,
  setInterval:(callback,ms)=>timer(intervals,callback,ms),clearInterval:value=>intervals.delete(value),setTimeout:(callback,ms)=>timer(timeouts,callback,ms),clearTimeout:value=>timeouts.delete(value)};
 vm.runInNewContext(source.replace(/^export /gmu,'')+'\nexports.make=createLocalAppsRuntime;',context);
 const runtime=context.exports.make({randomSecret:()=> 'n'.repeat(43),digest:text=>createHash('sha256').update(text).digest('hex'),fetch:async url=>{reads.push(url);return responses.length?responses.shift():capabilities();},
  networkInterfaces:()=>({fixture:[{family:'IPv4',internal:false,address:'192.168.50.19'}]}),createWebSocket:url=>{const ws=new Socket(url);sockets.push(ws);return ws;},httpRequest(){assert.fail('metadata negotiation must not open a Source request');}},
 {serverUrl:()=>server,identity:()=>identity,token:'synthetic'});
 t.after(()=>runtime.stop());
 return{runtime,sockets,reads,responses,setServer(value){server=value;},setIdentity(value){identity=value;},
  async start(){runtime.start();await flush();return this.open();},open(){const ws=sockets.at(-1);assert.ok(ws);ws.emit('open');ws.frame({type:'ready',schema:'soty.apps-channel.v1',bindingVersion:2,channelId:('channel'+sockets.length).padEnd(43,'c')});return ws;},
  async interval(){for(const timer of intervals.values())if(timer.ms===20000)timer.callback();await flush();},
  async reconnect(){for(const timer of [...timeouts.values()])if(timer.ms===2000){timeouts.delete(timer);timer.callback();}await flush();},
 };
}
test('a transient capabilities failure keeps loopback negotiated but retries on the existing bounded lifecycle and upgrades once',async t=>{
 const f=fixture(t),first=await f.start();assert.equal(first.sent[0].type,'auth');assert.equal(first.sent[0].capabilities.runtimeProfiles,undefined);assert.equal(f.runtime.status().connected,true);
 await f.interval();assert.equal(f.reads.length,2,'the old permanently downgraded channel never rereads capabilities');await f.reconnect();assert.equal(f.sockets.length,2);const fresh=f.open();assert.ok(fresh.sent[0].capabilities.runtimeProfiles.includes(profile));
 assert.equal(first.readyState,3);assert.equal(f.runtime.status().connected,true);const count=f.reads.length;await f.interval();assert.equal(f.reads.length,count,'a negotiated channel does not poll capabilities forever');assert.equal(f.sockets.length,2);
});
test('failed refresh remains bounded and a valid old Root settles the fallback without reconnect or Source downgrade',async t=>{
 const f=fixture(t),first=await f.start();f.responses.push(new Response('temporary failure',{status:503}));await f.interval();assert.equal(f.reads.length,2);assert.equal(f.sockets.length,1);assert.equal(f.runtime.status().connected,true);
 f.responses.push(capabilities(['soty.relay-restricted.v1']));await f.interval();assert.equal(f.reads.length,3);const reads=f.reads.length;await f.interval();assert.equal(f.reads.length,reads);assert.equal(f.sockets.length,1);assert.equal(first.readyState,1);
});
test('only one bounded capabilities reread owns a fallback connection at a time',async t=>{
 const f=fixture(t);await f.start();const pending=later();f.responses.push(pending.promise);await f.interval();assert.equal(f.reads.length,2);await f.interval();assert.equal(f.reads.length,2);pending.resolve(capabilities());await flush();await f.reconnect();assert.equal(f.sockets.length,2);
});
for(const change of ['stop','dropped-channel','server-change','identity-change','stop-start-ABA'])test(`late capability refresh cannot reconnect after ${change}`,async t=>{
 const f=fixture(t),first=await f.start(),pending=later();f.responses.push(pending.promise);await f.interval();assert.equal(f.reads.length,2);
 if(change==='stop')f.runtime.stop();
 if(change==='dropped-channel')first.close();
 if(change==='server-change')f.setServer('http://127.0.0.1:5902');
 if(change==='identity-change')f.setIdentity({linkId:'another_link',hostDeviceId:'another_host',connectorId:'another_connector'});
 if(change==='stop-start-ABA'){f.runtime.stop();await f.start();}
 const before=f.sockets.length;pending.resolve(capabilities());await flush();assert.equal(f.sockets.length,before,'a stale read cannot act on the new current context');
 if(change==='stop')assert.equal(f.runtime.status().connected,false);
 if(change==='stop-start-ABA')assert.equal(f.runtime.status().connected,true);
});
