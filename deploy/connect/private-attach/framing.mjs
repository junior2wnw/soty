import { Readable, Writable } from 'node:stream';
import { performance } from 'node:perf_hooks';
import { types } from 'node:util';
import { encodeFrame, decodeCanonicalFrame, dataRecord, captureSignal, signalIsAborted } from './vendor/warm/wire-protocol.mjs';

export const TYPE = Object.freeze({ PREPARE:1, WARM:2, BIND:3, BOUND:4, BODY:5, ACK:6, END:7, DONE:8, ERROR:9 });
export const LIMIT = Object.freeze({ chunk:65536, control:8192, body:256*1024*1024, wallMs:120000, idleMs:15000, stderr:16384 });
const known = new WeakMap();
export function failure(code='bridge_failed') { const e=new Error(code); known.set(e,code); e.code=code; e.stack=`Error: ${code}`; return e; }
export const codeOf = e => known.get(e) || 'bridge_failed';
export const fail = code => { throw failure(code); };
export function record(v, keys) { try { return dataRecord(v,keys); } catch { fail('bridge_record'); } }
export function equal(a,b) { return encodeFrame(a).equals(encodeFrame(b)); }
export function frame(type,seq,value) {
  if (!Object.values(TYPE).includes(type) || !Number.isSafeInteger(seq) || seq<0 || seq>0xffffffff) fail('bridge_frame');
  let bytes;
  try { bytes=type===TYPE.BODY ? Buffer.from(value) : encodeFrame(value); } catch { fail('bridge_frame'); }
  if (!bytes.length || bytes.length>(type===TYPE.BODY?LIMIT.chunk:LIMIT.control)) fail('bridge_size');
  const header=Buffer.alloc(9); header[0]=type; header.writeUInt32BE(bytes.length,1); header.writeUInt32BE(seq,5);
  return Buffer.concat([header,bytes]);
}

// Pull reads request at most one bounded frame. No data listener or work queue.
export class Channel {
  constructor(input,output,signal) {
    if (types.isProxy(input)||types.isProxy(output)||!(input instanceof Readable)||!(output instanceof Writable)
      || input===output||input.readableObjectMode||input.readableEncoding!==null||output.writableObjectMode
      || input.readableHighWaterMark>65536||output.writableHighWaterMark>65536) fail('bridge_stream');
    captureSignal(signal); this.input=input;this.output=output;this.signal=signal;
    this.started=performance.now();this.clockPhase='warm'; this.failed=null;this.reading=false;this.writing=false;this.pending=new Set();
    this.onError=()=>this.stop('bridge_io');this.onAbort=()=>this.stop('bridge_cancelled');
    input.on('error',this.onError);output.on('error',this.onError);
    if(signal) EventTarget.prototype.addEventListener.call(signal,'abort',this.onAbort,{once:true});
    if(signalIsAborted(signal)) this.stop('bridge_cancelled');
  }
  check() { if(this.failed)throw this.failed;if(signalIsAborted(this.signal))fail('bridge_cancelled');if(performance.now()-this.started>=LIMIT.wallMs)fail('bridge_timeout'); }
  remainingWall() {this.check();return Math.max(1,LIMIT.wallMs-(performance.now()-this.started));}
  admitBody() {
    this.check();
    if(this.clockPhase!=='warm'||this.reading||this.writing||this.pending.size!==0)fail('bridge_clock_stage');
    // The only clock transition, after actual BOUND admission. Neither incoming
    // requests, progress nor retries can renew warm or re-start body time.
    this.clockPhase='body';this.started=performance.now();
  }
  stop(code='bridge_failed') { this.failed??=failure(code);for(const reject of [...this.pending])reject(this.failed); }
  async operation(work,ms) {
    this.check();
    return new Promise((resolve,reject)=>{
      let done=false,cleanup=()=>{};
      const finish=(error,value)=>{if(done)return;done=true;clearTimeout(timer);this.pending.delete(cancel);try{cleanup();}catch{error=failure('bridge_cleanup_unknown');}error?reject(error):resolve(value);};
      const cancel=error=>finish(error);
      const phaseWait=this.clockPhase==='warm'?LIMIT.wallMs:LIMIT.idleMs;
      const timer=setTimeout(()=>{this.stop('bridge_timeout');finish(this.failed);},Math.max(1,Math.min(ms??phaseWait,phaseWait,LIMIT.wallMs-(performance.now()-this.started))));
      this.pending.add(cancel);
      try { cleanup=work((value)=>finish(null,value),()=>finish(failure('bridge_io'))) || (()=>{}); }
      catch {finish(failure('bridge_io'));}
      if(done)try{cleanup();}catch{}
    });
  }
  async take(size,ms) {
    if(size<1||size>LIMIT.chunk)fail('bridge_size');
    const result=Buffer.alloc(size);let offset=0;
    await this.operation((ok,bad)=>{
      const pull=()=>{
        try {
          while(offset<size){const chunk=this.input.read(size-offset);if(chunk===null)break;if(!Buffer.isBuffer(chunk)||chunk.length>size-offset)return bad();chunk.copy(result,offset);offset+=chunk.length;}
          if(offset===size)ok();else if(this.input.readableEnded||this.input.destroyed)bad();
        } catch {bad();}
      };
      this.input.on('readable',pull);this.input.on('end',pull);this.input.on('close',pull);pull();
      return()=>{this.input.off('readable',pull);this.input.off('end',pull);this.input.off('close',pull);};
    },ms);
    this.check();return result;
  }
  async read(ms) {
    if(this.reading)fail('bridge_concurrent_read');this.reading=true;
    try {
      const head=await this.take(9,ms),type=head[0],size=head.readUInt32BE(1),seq=head.readUInt32BE(5);
      if(!Object.values(TYPE).includes(type)||size<1||size>(type===TYPE.BODY?LIMIT.chunk:LIMIT.control))fail('bridge_size');
      const bytes=await this.take(size,ms);let value=bytes;
      if(type!==TYPE.BODY)try{value=decodeCanonicalFrame(bytes);}catch{fail('bridge_frame');}
      return {type,seq,value};
    }finally{this.reading=false;}
  }
  async send(type,seq,value) {
    if(this.writing)fail('bridge_concurrent_write');this.writing=true;
    try {
      const bytes=frame(type,seq,value);
      await this.operation((ok,bad)=>{
        let callback=false,drained=false,returned=false;
        const done=()=>{if(returned&&callback&&drained)ok();};
        const drain=()=>{drained=true;done();};this.output.on('drain',drain);
        const accepted=this.output.write(bytes,error=>{if(error)bad();else{callback=true;done();}});
        drained=accepted||drained;returned=true;done();return()=>this.output.off('drain',drain);
      });this.check();
    }finally{this.writing=false;}
  }
  async end() { await this.operation((ok,bad)=>{this.output.end(error=>error?bad():ok());});this.check(); }
  async eof() {
    await this.operation((ok,bad)=>{
      const inspect=()=>{if(this.input.readableLength!==0)bad();else if(this.input.readableEnded)ok();else if(this.input.destroyed)bad();else if(this.input.read(1)!==null)bad();};
      this.input.on('readable',inspect);this.input.on('end',inspect);this.input.on('close',inspect);inspect();
      return()=>{this.input.off('readable',inspect);this.input.off('end',inspect);this.input.off('close',inspect);};
    });this.check();
  }
  dispose() {if(this.signal)EventTarget.prototype.removeEventListener.call(this.signal,'abort',this.onAbort);this.input.off('error',this.onError);this.output.off('error',this.onError);}
}

export function physicalReceipt(value,spec,binding,bytes) {
  const r=record(value,['extracted','targetId','manifestSha256','plaintextSha256','plaintextBytes','entries','fileBytes','readbackVerified']);
  if(r.extracted!==true||r.readbackVerified!==true||r.targetId!==spec.targetId||r.manifestSha256!==binding.expectedManifestSha256
    ||typeof r.plaintextSha256!=='string'||!/^[a-f0-9]{64}$/.test(r.plaintextSha256)||r.plaintextBytes!==bytes
    ||!Number.isSafeInteger(r.entries)||r.entries<1||r.entries>10000||!Number.isSafeInteger(r.fileBytes)||r.fileBytes<0||r.fileBytes>240*1024*1024)fail('bridge_receipt');
  return Object.freeze(r);
}
