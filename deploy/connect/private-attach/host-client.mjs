import { Writable } from 'node:stream';
import { types } from 'node:util';
import { captureSpec,bindingFrame,warmFrame,boundFrame,dataRecord,captureSignal,signalIsAborted } from './vendor/warm/wire-protocol.mjs';
import { Channel,TYPE,LIMIT,record,equal,physicalReceipt,fail,failure,codeOf } from './framing.mjs';

export function createHostBridge(options){
  const v=record(options,['input','output','spec','bodyLimit','signal']);
  const spec=captureSpec(v.spec);
  try{captureSignal(v.signal);}catch{fail('bridge_signal');}
  if(!Number.isSafeInteger(v.bodyLimit)||v.bodyLimit<1||v.bodyLimit>LIMIT.body)fail('bridge_size');
  const abort=new AbortController(),wire=new Channel(v.input,v.output,abort.signal);
  let phase='created',seq=1,bytes=0,binding,payload,finalReceipt;
  let resolveDone;const completed=new Promise(resolve=>{resolveDone=resolve;});
  const cancel=()=>{
    if(phase==='closed'||phase==='failed')return;phase='failed';abort.abort();
    payload?.destroy(failure('bridge_cancelled'));
    try{v.output.destroy();}catch{}try{v.input.destroy();}catch{}
    resolveDone({ok:false,unknown:true,code:'bridge_cancelled'});detach();
  };
  const parentAbort=()=>cancel();
  let deadline=setTimeout(cancel,wire.remainingWall());
  const detach=()=>{clearTimeout(deadline);if(v.signal)EventTarget.prototype.removeEventListener.call(v.signal,'abort',parentAbort);wire.dispose();};
  const check=()=>{wire.check();if(phase==='failed')fail('bridge_cancelled');};
  if(v.signal){EventTarget.prototype.addEventListener.call(v.signal,'abort',parentAbort,{once:true});if(signalIsAborted(v.signal))cancel();}
  async function receive(type,index){const r=await wire.read();check();if(r.type!==type||r.seq!==index)fail('bridge_stage');return r.value;}
  async function guarded(work){try{return await work();}catch(error){const code=codeOf(error);cancel();throw failure(code);}}
  payload=new Writable({highWaterMark:65536,autoDestroy:true,emitClose:true,
    write(chunk,_encoding,callback){
      void guarded(async()=>{
        if(phase!=='body'||!Buffer.isBuffer(chunk)||chunk.length>LIMIT.chunk)fail('bridge_stage');
        if(bytes+chunk.length>v.bodyLimit)fail('bridge_size');
        for(let offset=0;offset<chunk.length;offset+=LIMIT.chunk){
          check();const part=chunk.subarray(offset,offset+LIMIT.chunk),next=bytes+part.length;
          await wire.send(TYPE.BODY,seq,part);check();
          const ack=record(await receive(TYPE.ACK,seq),['nonce','bytes']);
          if(ack.nonce!==spec.nonce||ack.bytes!==next)fail('bridge_ack');
          check();bytes=next;seq++;
        }
      }).then(()=>callback(),()=>callback(failure('bridge_io')));
    },
    final(callback){
      void guarded(async()=>{
        if(phase!=='body'||bytes<1)fail('bridge_stage');phase='ending';
        await wire.send(TYPE.END,seq,{nonce:spec.nonce,bytes});check();await wire.end();check();
        const done=record(await receive(TYPE.DONE,seq),['nonce','receipt','nativeClosed']);
        if(done.nonce!==spec.nonce||done.nativeClosed!==true)fail('bridge_native_unknown');
        const receipt=physicalReceipt(done.receipt,spec,binding,bytes);
        await wire.eof();check();phase='closed';finalReceipt=receipt;detach();
        resolveDone({ok:true,receipt,nativeClosed:true,unknown:false});
      }).then(()=>callback(),()=>callback(failure('bridge_io')));
    },
    destroy(error,callback){if(error)cancel();callback(error?failure('bridge_io'):null);},
  });
  payload.on('error',()=>{});
  return Object.freeze({
    signal:abort.signal,output:payload,completed,cancel,
    snapshot:()=>Object.freeze({phase,bytes,unknown:phase!=='closed',receipt:finalReceipt}),
    async warmBeforeServingStop(){
      if(phase!=='created')fail('bridge_stage');phase='warming';
      return guarded(async()=>{
        await wire.send(TYPE.PREPARE,0,{spec,bodyLimit:v.bodyLimit});check();
        const response=record(await receive(TYPE.WARM,0),['nonce','ack']);
        if(response.nonce!==spec.nonce||!equal(response.ack,warmFrame(spec)))fail('bridge_ack');
        check();phase='warm';return response.ack;
      });
    },
    async bindInsideAuthenticatedHook(receipt,inputFence){
      if(phase!=='warm')fail('bridge_stage');phase='binding';
      return guarded(async()=>{
        const fence=record(inputFence,['check','authenticated']);
        if(typeof fence.check!=='function'||types.isProxy(fence.check))fail('bridge_fence');
        binding=bindingFrame(spec,receipt);
        const current=()=>{check();if(phase!=='binding')fail('bridge_stage');Reflect.apply(fence.check,undefined,[]);check();
          if(phase!=='binding'||!equal(bindingFrame(spec,dataRecord(fence.authenticated,['expectedSha256','expectedManifestSha256','sourceWitness'])),binding))fail('bridge_binding');};
        current();await wire.send(TYPE.BIND,0,{nonce:spec.nonce,receipt});current();
        const response=record(await receive(TYPE.BOUND,0),['nonce','ack']);current();
        if(response.nonce!==spec.nonce||!equal(response.ack,boundFrame(binding)))fail('bridge_ack');
        current();wire.admitBody();clearTimeout(deadline);deadline=setTimeout(cancel,wire.remainingWall());
        phase='body';return response.ack;
      });
    },
  });
}
