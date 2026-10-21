export const CAMERA_LIMITS = Object.freeze({width:1280,height:720,frameRate:24,bitrate:1_200_000,tracks:1});
export const CAMERA_CONSTRAINTS = Object.freeze({audio:false,video:Object.freeze({
  width:Object.freeze({ideal:960,max:CAMERA_LIMITS.width}),height:Object.freeze({ideal:540,max:CAMERA_LIMITS.height}),
  frameRate:Object.freeze({ideal:20,max:CAMERA_LIMITS.frameRate})})});
const validId=value=>typeof value==='string'&&/^[A-Za-z0-9_.:-]{1,160}$/u.test(value);
const positive=value=>Number.isSafeInteger(value)&&value>0;
const result=(ok,code)=>Object.freeze(ok?{ok:true}:{ok:false,code});
function closed(value,keys){
  if(!value||typeof value!=='object'||![Object.prototype,null].includes(Object.getPrototypeOf(value)))return false;
  const descriptors=Object.getOwnPropertyDescriptors(value);
  return Reflect.ownKeys(descriptors).length===keys.length&&keys.every(key=>Object.hasOwn(descriptors,key)&&Object.hasOwn(descriptors[key],'value')&&descriptors[key].enumerable===true);
}
const validScope=scope=>closed(scope,['accountId','deviceId','callId','epoch','joinRef','generation'])
  &&['accountId','deviceId','callId','joinRef'].every(key=>validId(scope[key]))&&positive(scope.epoch)&&positive(scope.generation);
const same=(left,right)=>Boolean(left&&right&&Object.keys(left).every(key=>left[key]===right[key]));
const acknowledge=(ack,admission)=>closed(ack,['type','callId','epoch','grantRef','cameraGeneration'])&&ack.type==='call.video.published'
  &&ack.callId===admission.callId&&ack.epoch===admission.epoch&&ack.grantRef===admission.grantRef&&ack.cameraGeneration===admission.cameraGeneration;

/** Explicit camera owner. acquire returns {owner,stream}; only that exact owner's
 * capsule may be adopted/stopped. Real browser acquire is supplied by the host. */
export function createCameraLifecycle({enabled=false,coordinator,authorize,acquire,prepare,isCurrent=()=>true,onState=()=>{},onLocalStream=()=>{},clock=Date.now}={}){
  if(typeof enabled!=='boolean'||typeof coordinator?.acquire!=='function'||[authorize,acquire,prepare,isCurrent,onState,onLocalStream,clock].some(fn=>typeof fn!=='function'))throw TypeError('camera_ports_invalid');
  let session=null,ready=false,disposed=false,active=null,owned=null,teardown=0,notification=false,code=null;
  const at=()=>{const value=clock();if(!Number.isSafeInteger(value)||value<0)throw Error('camera_clock_invalid');return value;};
  const snapshot=()=>Object.freeze({state:active?.phase??'off',configured:enabled,ready:Boolean(session&&ready&&!disposed),
    code,cleanupPending:Boolean(owned&&!active),cleanupFailed:Boolean(owned?.cleanupFailed),ownedTracks:owned?.tracks.length??0});
  const notify=()=>{if(notification)return;notification=true;queueMicrotask(()=>{notification=false;try{onState(snapshot());}catch{}});};
  const live=frame=>!disposed&&active===frame&&!frame.abort.signal.aborted&&same(session,frame.scope)&&ready
    &&frame.lease.isCurrent()===true&&isCurrent(frame.scope)===true;
  function safeLive(frame){try{return live(frame);}catch{return false;}}
  function stopTracks(frame){
    for(const track of frame.tracks){
      try{track.enabled=false;}catch{frame.cleanupFailed=true;}
      try{track.stop();}catch{frame.cleanupFailed=true;}
      try{if(track.readyState!=='ended')frame.cleanupFailed=true;}catch{frame.cleanupFailed=true;}
    }
  }
  function finishLease(frame){
    if(frame.retired&&!frame.workPending&&!frame.disposalPending&&!frame.cleanupFailed){
      try{frame.lease.release();}catch{frame.cleanupFailed=true;}
      if(!frame.cleanupFailed&&owned===frame)owned=null;
    }
    notify();
  }
  function retire(frame,reason='camera_cancelled'){
    if(!frame||frame.retired)return;
    teardown++;
    try{
      frame.retired=true;if(active===frame)active=null;
      frame.abort.abort(reason);frame.settle(result(false,reason));
      for(const remove of frame.removers){try{remove();}catch{frame.cleanupFailed=true;}}frame.removers=[];
      stopTracks(frame);
      try{onLocalStream(null,frame.owner);}catch{}
      if(frame.stage&&!frame.stageDisposed){
        frame.stageDisposed=true;frame.disposalPending=true;
        try{Promise.resolve(frame.stage.dispose()).catch(()=>{frame.cleanupFailed=true;}).finally(()=>{frame.disposalPending=false;finishLease(frame);});}
        catch{frame.cleanupFailed=true;frame.disposalPending=false;}
      }
      finishLease(frame);
    }finally{teardown--;}
  }
  function adopt(frame,capsule){
    // Never enumerate, disable or stop another capture owner's returned stream.
    let proper=false;
    try{proper=closed(capsule,['owner','stream'])&&capsule.owner===frame.owner;}catch{}
    if(!proper){frame.cleanupFailed=true;throw Error('camera_foreign_capture');}
    let tracks;
    try{tracks=capsule.stream.getTracks();}catch{frame.cleanupFailed=true;throw Error('camera_capture_unknown');}
    let keys,length;
    try{keys=Reflect.ownKeys(tracks);length=Object.getOwnPropertyDescriptor(tracks,'length')?.value;}catch{frame.cleanupFailed=true;throw Error('camera_capture_unknown');}
    let malformed,indices=0;
    try{malformed=!Array.isArray(tracks)||Object.getPrototypeOf(tracks)!==Array.prototype||!Number.isSafeInteger(length)||length<0;}
    catch{frame.cleanupFailed=true;throw Error('camera_capture_unknown');}
    const seen=new Set();
    for(const key of keys){
      if(key==='length')continue;
      if(typeof key!=='string'||!/^(0|[1-9][0-9]*)$/u.test(key)||Number(key)>=4294967295){malformed=true;frame.cleanupFailed=true;continue;}
      let descriptor;try{descriptor=Object.getOwnPropertyDescriptor(tracks,key);}catch{frame.cleanupFailed=true;malformed=true;continue;}
      indices++;if(Number(key)>=length||!descriptor||!Object.hasOwn(descriptor,'value')){malformed=true;frame.cleanupFailed=true;continue;}
      if(!seen.has(descriptor.value)){seen.add(descriptor.value);frame.tracks.push(descriptor.value);}
    }
    if(indices!==length){malformed=true;frame.cleanupFailed=true;}
    for(const track of frame.tracks){try{track.enabled=false;}catch{frame.cleanupFailed=true;malformed=true;}}
    if(malformed||frame.tracks.length!==CAMERA_LIMITS.tracks)throw Error('camera_capture_invalid');
    const track=frame.tracks[0];
    if(track?.kind!=='video'||track.readyState!=='live'||typeof track.stop!=='function'||typeof track.getSettings!=='function'
      ||typeof track.addEventListener!=='function'||typeof track.removeEventListener!=='function')throw Error('camera_capture_invalid');
    checkSettings(track);
    return track;
  }
  function checkSettings(track){
    const settings=track.getSettings();
    if(!settings||![settings.width,settings.height,settings.frameRate].every(value=>Number.isFinite(value)&&value>0)
      ||settings.width>CAMERA_LIMITS.width||settings.height>CAMERA_LIMITS.height||settings.frameRate>CAMERA_LIMITS.frameRate)throw Error('camera_constraints_unverified');
  }
  function enable(){
    if(disposed)return Promise.resolve(result(false,'camera_disposed'));
    if(teardown)return Promise.resolve(result(false,'camera_transition'));
    if(!enabled)return Promise.resolve(result(false,'camera_unconfigured'));
    if(active)return active.promise;
    if(!session||!ready||isCurrent(session)!==true)return Promise.resolve(result(false,'camera_not_connected'));
    if(owned)return Promise.resolve(result(false,'camera_cleanup_pending'));
    const owner=Object.freeze({}),lease=coordinator.acquire('camera',owner);
    if(!lease)return Promise.resolve(result(false,'camera_busy'));
    if(typeof lease.isCurrent!=='function'||typeof lease.release!=='function')throw TypeError('camera_lease_invalid');
    let settle;const promise=new Promise(resolve=>{settle=resolve;});
    const frame={owner,lease,scope:session,abort:new AbortController(),phase:'permission',retired:false,workPending:true,
      disposalPending:false,cleanupFailed:false,stage:null,stageDisposed:false,tracks:[],removers:[],settle,promise};
    active=owned=frame;code=null;notify();
    void(async()=>{
      try{
        if(!safeLive(frame))throw Error('camera_stale');
        const offered=await authorize(frame.scope,{signal:frame.abort.signal});
        if(!safeLive(frame))throw Error('camera_stale');
        if(!closed(offered,['schema','callId','epoch','accountId','deviceId','grantRef','cameraGeneration','expiresAt'])
          ||offered.schema!=='soty.human-camera-admission.v1'||offered.callId!==frame.scope.callId||offered.epoch!==frame.scope.epoch
          ||offered.accountId!==frame.scope.accountId||offered.deviceId!==frame.scope.deviceId||!validId(offered.grantRef)
          ||!positive(offered.cameraGeneration)||!Number.isSafeInteger(offered.expiresAt)||offered.expiresAt<=at()||offered.expiresAt>at()+60_000)throw Error('camera_admission_invalid');
        const admission=Object.freeze({...offered});
        if(!safeLive(frame))throw Error('camera_stale');
        const capsule=await acquire(CAMERA_CONSTRAINTS,{owner:frame.owner,signal:frame.abort.signal});
        const track=adopt(frame,capsule);
        if(!safeLive(frame)||admission.expiresAt<=at())throw Error('camera_stale');
        const ended=()=>{if(active===frame){code='camera_ended';retire(frame,'camera_ended');notify();}};
        track.addEventListener('ended',ended);frame.removers.push(()=>track.removeEventListener('ended',ended));
        frame.phase='publishing';notify();
        const stage=await prepare({scope:frame.scope,admission,track,limits:CAMERA_LIMITS,signal:frame.abort.signal});
        frame.stage=stage;
        const commit=stage?.commit;
        if(!stage||typeof stage.dispose!=='function'||typeof commit!=='function')throw Error('camera_publication_invalid');
        if(!safeLive(frame))throw Error('camera_stale');
        const ack=await Reflect.apply(commit,stage,[]);
        if(!safeLive(frame)||!acknowledge(ack,admission)||admission.expiresAt<=at())throw Error('camera_publish_unconfirmed');
        checkSettings(track);if(!safeLive(frame))throw Error('camera_stale');track.enabled=true;
        if(!safeLive(frame)){track.enabled=false;throw Error('camera_stale');}
        frame.phase='on';try{onLocalStream(capsule.stream,frame.owner);}catch{}
        frame.settle(result(true));notify();
      }catch(error){
        if(active===frame)code=['camera_foreign_capture','camera_capture_unknown','camera_capture_invalid','camera_constraints_unverified',
          'camera_admission_invalid','camera_publication_invalid','camera_publish_unconfirmed'].includes(error?.message)?error.message:'camera_failed';
        // A stage may arrive after retirement; still retire that owned result.
        if(frame.retired)frame.retired=false;
        retire(frame,code??'camera_cancelled');
      }finally{frame.workPending=false;finishLease(frame);}
    })();
    return promise;
  }
  return Object.freeze({snapshot,enable,
    setSession(value){
      if(disposed)return result(false,'camera_disposed');if(teardown)return result(false,'camera_transition');
      if(value!==null&&!validScope(value))return result(false,'camera_scope_invalid');
      if(same(session,value))return result(true);
      const next=value===null?null:Object.freeze({...value});session=next;ready=false;retire(active,'camera_session_changed');notify();return result(true);
    },
    setReady(value){if(typeof value!=='boolean')throw TypeError('camera_ready_invalid');ready=value&&Boolean(session)&&!disposed;
      if(!ready)retire(active,'camera_interrupted');notify();return snapshot();},
    disable(){retire(active);notify();return snapshot();},
    end(){session=null;ready=false;retire(active,'camera_ended');notify();return snapshot();},
    dispose(){disposed=true;session=null;ready=false;retire(active,'camera_disposed');notify();return snapshot();},
  });
}
