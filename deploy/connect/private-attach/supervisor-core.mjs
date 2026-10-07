// Trusted local seam; no CLI, engine, stop/start or authority provider.
import { createWarmControlClient } from './vendor/warm/warm-receiver.mjs';
import { captureSpec, bindingFrame,captureSignal,signalIsAborted } from './vendor/warm/wire-protocol.mjs';
import { withOwnedChild } from './vendor/fixture/owned-child.mjs';
import { Channel,TYPE,LIMIT,record,equal,physicalReceipt,fail,codeOf } from './framing.mjs';

export async function superviseNativeChild({input,output,spec:specInput,command,args,signal}) {
  const spec=captureSpec(specInput),abort=new AbortController();
  try{captureSignal(signal);}catch{fail('bridge_signal');}
  const parentAbort=()=>abort.abort();
  if(signal){EventTarget.prototype.addEventListener.call(signal,'abort',parentAbort,{once:true});if(signalIsAborted(signal))abort.abort();}
  const wire=new Channel(input,output,abort.signal);
  let phase='prepare',bytes=0,seq=1,bodyLimit,binding,receipt,nativeClosed=false,unknown=true;
  const broken=()=>abort.abort();input.on('error',broken);output.on('error',broken);
  const eof=()=>{if(phase!=='ending'&&phase!=='closed')broken();}; input.on('end',eof);input.on('close',eof);
  try {
    const first=await wire.read(LIMIT.wallMs);
    if(first.type!==TYPE.PREPARE||first.seq!==0)fail('bridge_stage');
    const prepare=record(first.value,['spec','bodyLimit']);
    if(!equal(captureSpec(prepare.spec),spec)||!Number.isSafeInteger(prepare.bodyLimit)||prepare.bodyLimit<1||prepare.bodyLimit>LIMIT.body)fail('bridge_spec');
    bodyLimit=prepare.bodyLimit;phase='warming';wire.check();
    // Fixed command/args supplied only by reviewed wrapper, never wire values.
    receipt=await withOwnedChild(command,args,{stdio:['pipe','pipe','pipe','pipe','pipe'],cwd:process.cwd(),
      env:{PATH:'',NODE_OPTIONS:'',NODE_PATH:'',LD_PRELOAD:'',LD_LIBRARY_PATH:''}},async(child,owner)=>{
      let stdout=Buffer.alloc(0),stderrBytes=0,stdoutEnded=false,client,closedEarly=false;
      child.stdout.on('data',chunk=>{if(stdout.length+chunk.length>LIMIT.stderr)broken();else stdout=Buffer.concat([stdout,chunk]);});
      child.stderr.on('data',chunk=>{stderrBytes+=chunk.length;if(stderrBytes>LIMIT.stderr)broken();});
      child.stdout.once('end',()=>{stdoutEnded=true;});
      child.once('close',()=>{if(phase!=='ending'){closedEarly=true;broken();}});
      const check=()=>{wire.check();if(closedEarly)fail('bridge_child_closed');};
      try {
        client=createWarmControlClient({spec,announcementInput:child.stdio[4],controlOutput:child.stdio[3],signal:abort.signal});
        const warm=await client.warmBeforeServingStop();check();
        await wire.send(TYPE.WARM,0,{nonce:spec.nonce,ack:warm});check();phase='warm';
        const request=await wire.read(LIMIT.wallMs);check();
        if(request.type!==TYPE.BIND||request.seq!==0)fail('bridge_stage');
        const bind=record(request.value,['nonce','receipt']);if(bind.nonce!==spec.nonce)fail('bridge_nonce');
        binding=bindingFrame(spec,bind.receipt);phase='binding';
        // This fence binds wire identity/cancel only. Authentication is performed
        // by host staged sender before it sends BIND, not proved by JSON here.
        const ack=await client.bindInsideAuthenticatedHook(bind.receipt,{check,authenticated:bind.receipt});check();
        if(child.stdio[3].writableFinished!==true||child.stdio[4].readableEnded!==true)fail('bridge_native_eof');
        wire.admitBody();
        await wire.send(TYPE.BOUND,0,{nonce:spec.nonce,ack});check();phase='body';
        for(;;){
          const item=await wire.read();check();if(item.seq!==seq)fail('bridge_sequence');
          if(item.type===TYPE.BODY){
            if(bytes+item.value.length>bodyLimit)fail('bridge_size');bytes+=item.value.length;
            // Exactly one outstanding frame: ACK only after the ACTUAL native
            // receiver child's stdin callback, never after attach buffering.
            await wire.operation((ok,bad)=>{child.stdin.write(item.value,error=>error?bad():ok());});check();
            await wire.send(TYPE.ACK,seq,{nonce:spec.nonce,bytes});seq++;continue;
          }
          if(item.type!==TYPE.END)fail('bridge_stage');
          const end=record(item.value,['nonce','bytes']);if(end.nonce!==spec.nonce||end.bytes!==bytes||bytes<1)fail('bridge_end');
          phase='ending';await wire.eof();check();
          await wire.operation((ok,bad)=>{child.stdin.end(error=>error?bad():ok());});check();
          await wire.operation((ok)=>{owner.closedPromise.then(ok);});
          const status=owner.snapshot();
          if(!status.closed||status.failed||status.forced||status.code!==0||status.signal!==null||!stdoutEnded)fail('bridge_native_unknown');
          let value;try{const text=stdout.toString('utf8');value=JSON.parse(text);if(JSON.stringify(value)+'\n'!==text)fail('bridge_receipt');}catch{fail('bridge_receipt');}
          return physicalReceipt(value,spec,binding,bytes);
        }
      }finally{client?.cancel();}
    });
    nativeClosed=true;unknown=false;wire.check();phase='closed';
    await wire.send(TYPE.DONE,seq,{nonce:spec.nonce,receipt,nativeClosed:true});await wire.end();
    return Object.freeze({ok:true,phase,bytes,nativeClosed,unknown:false,receipt});
  }catch(error){
    unknown=true;abort.abort();
    // Failure is returned to the private owning caller. A broken attach cannot
    // carry trustworthy final receipt; no recovery message or false EOF success.
    return Object.freeze({ok:false,code:codeOf(error),phase,bytes,nativeClosed,unknown});
  }finally{
    wire.dispose();input.off('error',broken);output.off('error',broken);input.off('end',eof);input.off('close',eof);
    if(signal)EventTarget.prototype.removeEventListener.call(signal,'abort',parentAbort);
    if(phase!=='closed'){try{input.destroy();}catch{}try{output.destroy();}catch{}}
  }
}
