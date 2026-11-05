import test from 'node:test';
import assert from 'node:assert/strict';
import {setImmediate as nextTurn} from 'node:timers/promises';
import {createCanvasPageLive} from './canvas-page-live.mjs';

const view=character=>({viewRevision:character.repeat(64),marker:character});
const update=value=>({changed:true,viewRevision:value.viewRevision,view:value});
const deferred=()=>{let resolve,reject;const promise=new Promise((done,fail)=>{resolve=done;reject=fail;});return{promise,resolve,reject};};
const flush=()=>nextTurn();
class Events extends EventTarget{
  listeners=new Map();
  addEventListener(type,listener,options){super.addEventListener(type,listener,options);this.listeners.set(type,listener);}
  removeEventListener(type,listener,options){super.removeEventListener(type,listener,options);if(this.listeners.get(type)===listener)this.listeners.delete(type);}
  send(type){this.dispatchEvent(new Event(type));}
}
class Clock{
  now=0;sequence=0;pending=new Map();delays=[];
  setTimeout(callback,delay){this.delays.push(delay);const id=++this.sequence;this.pending.set(id,{callback,at:this.now+delay});return id;}
  clearTimeout(id){this.pending.delete(id);}
  async advance(ms){const target=this.now+ms;for(;;){const next=[...this.pending.entries()].sort((a,b)=>a[1].at-b[1].at)[0];if(!next||next[1].at>target)break;this.now=next[1].at;this.pending.delete(next[0]);next[1].callback();await flush();}this.now=target;await flush();}
}
function harness(factory=createCanvasPageLive){
  const events=new Events(),clock=new Clock(),applied=[],unavailable=[],waits=[],calls=[];
  let online=true,current=true,activeWaits=0,maxWaits=0,painted=null,loadError=null;
  const client={
    async load(first,options){calls.push({op:'load',first,signal:options?.signal});if(loadError)throw loadError;if(!online)throw TypeError('network offline');return first??view('a');},
    wait(after,options){
      calls.push({op:'wait',after,signal:options?.signal});const pending=deferred();let settled=false;activeWaits++;maxWaits=Math.max(maxWaits,activeWaits);
      const finish=(callback,value)=>{if(settled)return;settled=true;activeWaits--;options?.signal?.removeEventListener('abort',abort);callback(value);};
      const abort=()=>finish(pending.reject,Object.assign(Error('aborted'),{name:'AbortError'}));options?.signal?.addEventListener('abort',abort,{once:true});
      waits.push({signal:options?.signal,resolve:value=>finish(pending.resolve,value),reject:error=>finish(pending.reject,error)});return pending.promise;
    },
  };
  const live=factory({client,events,timers:clock,online:()=>online,isCurrent:()=>current,
    apply(value){painted=value;applied.push(value);},onUnavailable(error){painted=null;unavailable.push(error);}});
  return{live,client,events,clock,applied,unavailable,waits,calls,get painted(){return painted;},get maxWaits(){return maxWaits;},
    offline(){online=false;events.send('offline');},online(){online=true;events.send('online');},setCurrent(value){current=value;},setLoadError(value){loadError=value;}};
}

// Faithful control of the former screen: catch scrubs and empties revision;
// its finally releases flight, but nothing observes online or retries.
function formerFollow({client,apply,onUnavailable,isCurrent}){
  let disposed=false,revision='',flight=null;
  const active=()=>!disposed&&isCurrent();
  function follow(){if(flight||!active()||!revision)return;
    flight=(async()=>{while(active()&&revision){const value=await client.wait(revision);if(!active())return;if(value.changed&&value.view){const next=await client.load(value.view);if(active()){revision=next.viewRevision;apply(next);}}}})()
      .catch(error=>{if(active()){revision='';onUnavailable(error);}}).finally(()=>{flight=null;});
  }
  const refresh=async()=>{const value=await client.load();if(active()){revision=value.viewRevision;apply(value);follow();}};
  return{start:refresh,refresh,dispose(){disposed=true;}};
}
async function reconnectOracle(factory){
  const h=harness(factory);
  try{
    await h.live.start();await flush();assert.equal(h.painted?.marker,'a');
    h.offline();h.waits[0].reject(TypeError('connection lost'));await flush();assert.equal(h.painted,null,'offline content must be scrubbed');
    h.online();await h.clock.advance(18000);
    assert.equal(h.painted?.marker,'a','restored network must restore the same permitted content without Retry or reload');
  }finally{h.live.dispose();}
}

test('unchanged reconnect oracle detects former empty-until-Retry behavior and passes the new controller',async()=>{
  await assert.rejects(()=>reconnectOracle(formerFollow),error=>error.code==='ERR_ASSERTION'&&error.message.includes('without Retry or reload'));
  await reconnectOracle(createCanvasPageLive);
});

test('offline scrubs immediately, coalesces timers/events and online resumes one current poll',async()=>{
  const h=harness();try{
    const first=h.live.start();assert.equal(h.live.start(),first);await first;await flush();assert.equal(h.waits.length,1);
    const signal=h.waits[0].signal;h.offline();assert.equal(h.painted,null);assert.equal(signal.aborted,true);
    h.offline();h.offline();assert.equal(h.clock.pending.size,1);assert.equal(h.unavailable.length,1);
    await h.clock.advance(18000);assert.ok(h.clock.delays.every(value=>value<=15000));assert.equal(h.calls.filter(value=>value.op==='load').length,1,'offline retry timers must not issue network requests');
    h.online();h.online();h.online();await flush();assert.equal(h.painted.marker,'a');assert.equal(h.clock.pending.size,0);
    assert.equal(h.calls.filter(value=>value.op==='load').length,2);assert.equal(h.maxWaits,1);
  }finally{h.live.dispose();}
});

test('transient 503 uses bounded backoff and automatically restores a healthy client',async()=>{
  const h=harness();try{
    h.setLoadError(Object.assign(Error('busy'),{status:503,code:'canvas_page_wait_busy'}));
    await assert.rejects(h.live.start(),{status:503});assert.equal(h.clock.pending.size,1);
    for(const delay of[500,1000,2000,4000,8000,15000,15000]){assert.equal(h.clock.delays.at(-1),delay);await h.clock.advance(delay);assert.equal(h.clock.pending.size,1);}
    h.setLoadError(null);await h.clock.advance(15000);assert.equal(h.painted.marker,'a');assert.equal(h.clock.pending.size,0);
  }finally{h.live.dispose();}
});

test('initial offline startup does not read, and reconnect has no authority fallback',async()=>{
  const h=harness();try{
    h.offline();await assert.rejects(h.live.start(),{code:'canvas_page_offline'});assert.equal(h.calls.length,0);assert.equal(h.painted,null);
    h.online();await flush();assert.equal(h.painted.marker,'a');assert.equal(h.calls.filter(value=>value.op==='load').length,1);
  }finally{h.live.dispose();}
});

for(const error of[Object.assign(Error('expired'),{status:401}),Object.assign(Error('revoked'),{status:403,code:'DEVICE_REVOKED'}),Object.assign(Error('changed'),{code:'ACTIVE_PROFILE_CHANGED'})])
test(`identity failure ${error.code??error.status} scrubs and cannot retry under the old identity`,async()=>{
  const h=harness();try{
    await h.live.start();await flush();h.waits[0].reject(error);await flush();assert.equal(h.painted,null);assert.equal(h.clock.pending.size,0);
    const count=h.calls.length;h.offline();h.online();await h.clock.advance(60000);assert.equal(h.calls.length,count);
    await assert.rejects(h.live.refresh(),value=>value===error);assert.equal(h.calls.length,count,'manual Retry must not manufacture a different actor');
  }finally{h.live.dispose();}
});

test('an explicit refresh aborts an old wait and fences a Source response which ignores cancellation',async()=>{
  const old=deferred(),fresh=deferred(),events=new Events(),applied=[],unavailable=[];let waitingSignal,loads=0,waitCount=0;
  const live=createCanvasPageLive({events,isCurrent:()=>true,online:()=>true,
    client:{async load(first){loads++;return first??view(loads===1?'a':'c');},wait(_after,{signal}){waitingSignal=signal;return ++waitCount===1?old.promise:fresh.promise;}},
    apply:value=>applied.push(value.marker),onUnavailable:error=>unavailable.push(error)});
  try{
    await live.start();await flush();const original=waitingSignal;
    await live.refresh();assert.equal(original.aborted,true);assert.deepEqual(applied,['a','c']);
    old.resolve(update(view('b')));await flush();assert.deepEqual(applied,['a','c'],'late old wait must not reload or paint its old Source');assert.equal(loads,2);assert.deepEqual(unavailable,[]);
  }finally{live.dispose();}
});

test('superseded loads and a refresh burst never paint stale results or create concurrent polls',async()=>{
  const old=deferred(),h=harness();let first=true;
  h.client.load=async(firstView,options)=>{if(first){first=false;return old.promise;}return view('c');};
  try{
    const obsolete=h.live.start();await flush();const one=h.live.refresh(),two=h.live.refresh();await Promise.all([one,two]);old.resolve(view('a'));await obsolete;await flush();
    assert.deepEqual(h.applied.map(value=>value.marker),['c']);assert.equal(h.maxWaits,1);assert.deepEqual(h.unavailable,[]);
  }finally{h.live.dispose();}
});

test('changed poll snapshots finish through client.load before applying and unchanged polls do not repaint',async()=>{
  const h=harness();try{
    await h.live.start();await flush();h.waits[0].resolve(update(view('b')));await flush();assert.deepEqual(h.applied.map(value=>value.marker),['a','b']);
    assert.equal(h.calls.filter(value=>value.op==='load').at(-1).first.marker,'b');
    h.waits[1].resolve({changed:false,viewRevision:view('b').viewRevision,view:null});await flush();assert.equal(h.applied.length,2);assert.equal(h.waits.length,3);assert.equal(h.maxWaits,1);
  }finally{h.live.dispose();}
});

test('an offline event during recovery aborts the recovery load before it can reapply cached content',async()=>{
  const h=harness(),pending=deferred();try{
    await h.live.start();await flush();h.offline();await flush();h.client.load=async()=>pending.promise;
    h.online();await flush();h.offline();pending.resolve(view('b'));await flush();assert.equal(h.painted,null);assert.deepEqual(h.applied.map(value=>value.marker),['a']);assert.equal(h.clock.pending.size,1);
  }finally{h.live.dispose();}
});

test('dispose and an obsolete account remove listeners/timers and discard all late data',async()=>{
  for(const stale of[false,true]){
    const h=harness();await h.live.start();await flush();const signal=h.waits[0].signal;
    if(stale){h.setCurrent(false);h.waits[0].resolve(update(view('b')));await flush();}else{h.offline();h.live.dispose();}
    assert.equal(signal.aborted,true);assert.equal(h.events.listeners.size,0);assert.equal(h.clock.pending.size,0);const count=h.calls.length;
    h.events.send('online');await h.clock.advance(60000);assert.equal(h.calls.length,count);assert.deepEqual(h.applied.map(value=>value.marker),['a']);h.live.dispose();
  }
});
