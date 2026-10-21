import test from 'node:test';
import assert from 'node:assert/strict';
import {createCameraLifecycle,CAMERA_LIMITS,CAMERA_CONSTRAINTS} from './camera-lifecycle.mjs';
const deferred=()=>{let resolve;const promise=new Promise(done=>{resolve=done;});return{promise,resolve};};
const flush=async()=>{for(let n=0;n<12;n++)await Promise.resolve();};
const scope=()=>({accountId:'account_one',deviceId:'device_one',callId:'call_one',epoch:1,joinRef:'join_one',generation:1});
class Track extends EventTarget{
  kind='video';enabled=true;readyState='live';stops=0;
  constructor(settings={width:1280,height:720,frameRate:24}){super();this.settings=settings;}
  getSettings(){return this.settings;}
  stop(){this.stops++;this.enabled=false;this.readyState='ended';}
}
function coordinator(){let active=null;return{
  busy:()=>Boolean(active),acquire(resource,owner){assert.equal(resource,'camera');if(active)return null;const token={owner};active=token;
    return{isCurrent:()=>active===token,release(){if(active===token)active=null;}};}};}
function fixture(options={}){
  const coord=options.coordinator??coordinator(),tracks=[],stages=[],events=[],local=[],state={captures:0,authorizations:0,prepares:0};
  const now=1_800_000_000_000;
  const admission=value=>({schema:'soty.human-camera-admission.v1',callId:value.callId,epoch:value.epoch,accountId:value.accountId,
    deviceId:value.deviceId,grantRef:'GRANT_DO_NOT_LEAK',cameraGeneration:1,expiresAt:now+30_000});
  const camera=createCameraLifecycle({enabled:options.enabled??true,coordinator:coord,clock:()=>now,isCurrent:options.isCurrent??(()=>true),
    authorize:async(value,request)=>{state.authorizations++;return options.authorize?options.authorize(value,request,admission(value)):admission(value);},
    acquire:async(constraints,request)=>{state.captures++;assert.deepEqual(constraints,CAMERA_CONSTRAINTS);if(options.acquire)return options.acquire(constraints,request);
      const track=new Track(options.settings);tracks.push(track);return{owner:request.owner,stream:{getTracks:()=>[track]}};},
    prepare:async value=>{state.prepares++;assert.equal(value.track.enabled,false);assert.deepEqual(value.limits,CAMERA_LIMITS);
      if(options.prepare)return options.prepare(value);
      const stage={disposed:0,dispose(){this.disposed++;},async commit(){return options.ack??{type:'call.video.published',callId:value.scope.callId,
        epoch:value.scope.epoch,grantRef:value.admission.grantRef,cameraGeneration:value.admission.cameraGeneration};}};stages.push(stage);return stage;},
    onState:value=>events.push(value),onLocalStream:(stream,owner)=>local.push({stream,owner})});
  camera.setSession(scope());camera.setReady(true);
  return{camera,coord,tracks,stages,state,events,local,admission};
}

test('constructor, identity, ready and route-like same scope never request camera; default unconfigured is honest',async()=>{
  const f=fixture({enabled:false});await flush();f.camera.setSession({...scope()});f.camera.setReady(true);
  assert.equal((await f.camera.enable()).code,'camera_unconfigured');assert.equal(f.state.captures,0);assert.equal(f.state.authorizations,0);
  assert.equal(f.camera.snapshot().configured,false);f.camera.dispose();
});
test('explicit enable only publishes after exact ACK and propagates hard resolution/fps/bitrate caps',async()=>{
  const f=fixture();assert.deepEqual(await f.camera.enable(),{ok:true});assert.equal(f.camera.snapshot().state,'on');
  assert.equal(f.tracks[0].enabled,true);assert.equal(f.state.captures,1);assert.equal(CAMERA_LIMITS.bitrate,1_200_000);
  assert.deepEqual(await f.camera.enable(),{ok:true});assert.equal(f.state.captures,1);
  f.camera.setSession({...scope()});assert.equal(f.camera.snapshot().state,'on');
  f.camera.disable();await flush();assert.equal(f.tracks[0].readyState,'ended');assert.equal(f.stages[0].disposed,1);assert.equal(f.coord.busy(),false);
});
test('wrong publication ACK cannot enable or retain local capture',async()=>{
  const f=fixture({ack:{type:'call.video.published',callId:'call_other',epoch:1,grantRef:'GRANT_DO_NOT_LEAK',cameraGeneration:1}});
  assert.equal((await f.camera.enable()).ok,false);await flush();assert.equal(f.tracks[0].enabled,false);assert.equal(f.tracks[0].readyState,'ended');
  assert.equal(f.camera.snapshot().state,'off');assert.equal(f.coord.busy(),false);
});
test('wrong account, device, call, epoch or expired camera admission never requests capture',async()=>{
  for(const mutation of [{accountId:'other_account'},{deviceId:'other_device'},{callId:'other_call'},{epoch:2},{expiresAt:1},{expiresAt:Number.MAX_SAFE_INTEGER}]){
    const f=fixture({authorize:async(_scope,_request,admission)=>({...admission,...mutation})});
    assert.equal((await f.camera.enable()).ok,false);await flush();assert.equal(f.state.captures,0);assert.equal(f.coord.busy(),false);
  }
});
test('cancel pending authorization settles promptly, holds lease until actual settlement and never captures late',async()=>{
  const held=deferred(),f=fixture({authorize:(_value,_request,admission)=>held.promise.then(()=>admission)});
  const pending=f.camera.enable();await flush();f.camera.disable();assert.equal((await pending).ok,false);
  assert.equal(f.coord.busy(),true);assert.equal((await f.camera.enable()).code,'camera_cleanup_pending');
  held.resolve();await flush();assert.equal(f.state.captures,0);assert.equal(f.coord.busy(),false);
});
test('late own capture after cancellation is stopped and its lease then released',async()=>{
  const held=deferred(),track=new Track();let owner;
  const f=fixture({acquire:(_constraints,request)=>{owner=request.owner;return held.promise;}});
  const pending=f.camera.enable();await flush();assert.equal(f.state.captures,1);f.camera.disable();assert.equal((await pending).ok,false);
  assert.equal(f.coord.busy(),true);held.resolve({owner,stream:{getTracks:()=>[track]}});await flush();
  assert.equal(track.readyState,'ended');assert.equal(track.enabled,false);assert.equal(f.state.prepares,0);assert.equal(f.coord.busy(),false);
});
test('foreign owner capsule is neither enumerated nor stopped and unknown ownership quarantines own lease',async()=>{
  const foreign=new Track();let reads=0;
  const f=fixture({acquire:async()=>({owner:{},stream:{getTracks(){reads++;return[foreign];}}})});
  assert.equal((await f.camera.enable()).ok,false);await flush();assert.equal(reads,0);assert.equal(foreign.stops,0);assert.equal(foreign.enabled,true);
  assert.equal(f.camera.snapshot().cleanupFailed,true);assert.equal(f.coord.busy(),true);
});
test('existing QR camera lease rejects enable without admission, capture or stopping QR track',async()=>{
  const coord=coordinator(),qr=coord.acquire('camera',{}),foreign=new Track(),f=fixture({coordinator:coord});
  assert.equal((await f.camera.enable()).code,'camera_busy');assert.equal(f.state.authorizations,0);assert.equal(f.state.captures,0);assert.equal(foreign.stops,0);
  qr.release();assert.equal((await f.camera.enable()).ok,true);f.camera.end();await flush();
});
test('oversized native settings fail before preparing publication',async()=>{
  for(const settings of [{width:1920,height:1080,frameRate:30},{width:1280,height:720},{width:1280,height:720,frameRate:60}]){
    const f=fixture({settings});assert.equal((await f.camera.enable()).ok,false);await flush();assert.equal(f.state.prepares,0);assert.equal(f.tracks[0].readyState,'ended');assert.equal(f.coord.busy(),false);
  }
});
test('account ABA or interruption stops video without re-capture when connection recovers',async()=>{
  const f=fixture();await f.camera.enable();f.camera.setSession({...scope(),accountId:'account_other',generation:2});
  f.camera.setSession({...scope(),generation:3});f.camera.setReady(true);await flush();assert.equal(f.tracks[0].readyState,'ended');assert.equal(f.state.captures,1);
  await f.camera.enable();f.camera.setReady(false);f.camera.setReady(true);await flush();assert.equal(f.state.captures,2);assert.equal(f.camera.snapshot().state,'off');
});
test('cancel during stage commit stops capture and old ACK cannot reopen camera',async()=>{
  const held=deferred();let admission;
  const f=fixture({prepare:async value=>{admission=value.admission;return{dispose(){},commit:()=>held.promise};}});
  const pending=f.camera.enable();await flush();f.camera.disable();assert.equal((await pending).ok,false);assert.equal(f.tracks[0].readyState,'ended');
  held.resolve({type:'call.video.published',callId:'call_one',epoch:1,grantRef:admission.grantRef,cameraGeneration:1});await flush();
  assert.equal(f.camera.snapshot().state,'off');assert.equal(f.tracks[0].enabled,false);assert.equal(f.coord.busy(),false);
});
test('late publication stage is disposed after cancellation',async()=>{
  const held=deferred();let disposed=0;
  const f=fixture({prepare:()=>held.promise});const pending=f.camera.enable();await flush();f.camera.disable();await pending;
  held.resolve({dispose(){disposed++;},commit(){assert.fail('late commit');}});await flush();assert.equal(disposed,1);assert.equal(f.coord.busy(),false);
});
test('track ended and disposal failure never claim a free camera or restart capture',async()=>{
  const f=fixture({prepare:async value=>({dispose(){throw Error('private failure');},async commit(){return{type:'call.video.published',callId:value.scope.callId,epoch:1,grantRef:value.admission.grantRef,cameraGeneration:1};}})});
  await f.camera.enable();f.tracks[0].readyState='ended';f.tracks[0].dispatchEvent(new Event('ended'));await flush();
  assert.equal(f.camera.snapshot().state,'off');assert.equal(f.camera.snapshot().cleanupFailed,true);assert.equal(f.coord.busy(),true);
  assert.equal((await f.camera.enable()).code,'camera_cleanup_pending');assert.equal(f.state.captures,1);
});
test('malformed own mixed stream stops all owned tracks and public snapshots contain no admission identifiers',async()=>{
  const video=new Track(),audio=new Track();audio.kind='audio';
  const f=fixture({acquire:async(_constraints,request)=>({owner:request.owner,stream:{getTracks:()=>[video,audio]}})});
  assert.equal((await f.camera.enable()).ok,false);await flush();assert.equal(video.readyState,'ended');assert.equal(audio.readyState,'ended');
  assert.doesNotMatch(JSON.stringify(f.events),/GRANT_DO_NOT_LEAK|join_one|call_one|account_one/u);
});

test('nonenumerable camera generation is rejected before cloning and a valid generation change retires capture',async()=>{
  const f=fixture();f.camera.setSession(null);
  const hidden=scope();Object.defineProperty(hidden,'generation',{value:1,enumerable:false});
  assert.deepEqual(f.camera.setSession(hidden),{ok:false,code:'camera_scope_invalid'});
  f.camera.setReady(true);assert.equal((await f.camera.enable()).ok,false);assert.equal(f.state.captures,0);
  assert.equal(f.camera.setSession(scope()).ok,true);f.camera.setReady(true);assert.equal((await f.camera.enable()).ok,true);
  f.camera.setSession({...scope(),generation:2});await flush();assert.equal(f.tracks[0].readyState,'ended');assert.equal(f.camera.snapshot().state,'off');
  f.camera.dispose();
});
