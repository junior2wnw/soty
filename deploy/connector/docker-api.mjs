import http from 'node:http';
import { parseContractJson } from '../../modules/app-contract/json.mjs';
import { validateUniversalPreparedness } from '../../modules/app-contract/universal-preparedness.mjs';

const UNIVERSAL_EXEC = Object.freeze(['node','--input-type=module','-e',
  "try { const {readUniversalOperator}=await import('./server/universal-operator.js'); const value=await readUniversalOperator(); process.stdout.write(JSON.stringify(value)+'\\n'); } catch { process.exitCode=1; }"]);

export class SafeError extends Error { constructor(code) { super(code); this.code=code; } }
const measurementDiagnostics=new WeakMap();
/** Only this fixed reader can produce journal-safe diagnostics; never serialize an arbitrary error object. */
export const universalMeasurementDiagnostic=error=>measurementDiagnostics.get(error)||null;
export class DockerApi {
  constructor({socketPath='/var/run/docker.sock',timeoutMs=10000}={}) { this.socketPath=socketPath;this.timeoutMs=timeoutMs; }
  request(method,path,body,raw=false,maxBytes=4*1024*1024,options={}) {
    return new Promise((resolve,reject)=>{
      if(options.timeoutMs!==undefined&&(!Number.isSafeInteger(options.timeoutMs)||options.timeoutMs<=0||options.timeoutMs>this.timeoutMs))return reject(new SafeError('engine_request_deadline_invalid'));
      const data=body===undefined?null:Buffer.from(JSON.stringify(body));
      const req=http.request({socketPath:this.socketPath,path:'/v1.45'+path,method,headers:data?{'content-type':'application/json','content-length':data.length}:{}},res=>{
        options.onStatus?.(res.statusCode);
        let length=0;const chunks=[];
        res.on('aborted',()=>reject(new SafeError('engine_response_ambiguous')));
        res.on('error',()=>reject(new SafeError('engine_response_ambiguous')));
        res.on('data',b=>{length+=b.length;if(length>maxBytes){req.destroy();reject(new SafeError('engine_response_limit'));}else chunks.push(b);});
        res.on('end',()=>{const bytes=Buffer.concat(chunks);if(res.statusCode<200||res.statusCode>=300){reject(new SafeError('engine_http_'+res.statusCode));return;}if(raw)return resolve(bytes);try{resolve(bytes.length?JSON.parse(bytes):null);}catch{reject(new SafeError('engine_invalid_json'));}});
      });
      const deadline=setTimeout(()=>{req.destroy();reject(new SafeError('engine_response_ambiguous'));},options.timeoutMs??this.timeoutMs);
      req.on('close',()=>clearTimeout(deadline));
      req.on('error',()=>reject(new SafeError('engine_response_ambiguous')));if(data)req.write(data);req.end();
    });
  }
  inspect(id,options){return this.request('GET',`/containers/${encodeURIComponent(id)}/json`,undefined,false,4*1024*1024,options);}
  image(id){return this.request('GET',`/images/${encodeURIComponent(id)}/json`);}
  create(name,config){return this.request('POST','/containers/create?name='+encodeURIComponent(name),config);}
  start(id){return this.request('POST',`/containers/${id}/start`);}
  stop(id){return this.request('POST',`/containers/${id}/stop?t=10`);}
  rename(id,name){return this.request('POST',`/containers/${id}/rename?name=${encodeURIComponent(name)}`);}
  remove(id){return this.request('DELETE',`/containers/${id}?force=false&v=false`);}
  /** Fixed read-only exec. No arbitrary command/path/arguments or secret output. */
  async universalPreparedness(id) {
    if(!/^[a-f0-9]{64}$/u.test(id||''))throw new SafeError('universal_measurement_identity');
    const started=Date.now(),timeoutMs=this.timeoutMs??10000,deadline=started+timeoutMs;
    const diagnostic={schema:'soty.universal-measurement-diagnostic.v1',containerId:id,execId:null,stage:'container-before',httpStatus:null,errorCategory:null,exec:null,inspectPolls:0};
    const fail=(code,category)=>{diagnostic.errorCategory=category;throw new SafeError(code);};
    const options=stage=>{
      diagnostic.stage=stage;diagnostic.httpStatus=null;
      const remaining=deadline-Date.now();if(remaining<=0)fail('universal_measurement_unresolved','deadline');
      return {timeoutMs:remaining,onStatus:status=>{diagnostic.httpStatus=status;}};
    };
    const metadata=execution=>({idMatches:execution?.ID===diagnostic.execId,containerMatches:execution?.ContainerID===id,
      running:typeof execution?.Running==='boolean'?execution.Running:null,exitCode:Number.isSafeInteger(execution?.ExitCode)?execution.ExitCode:null,
      privilegedFalse:execution?.ProcessConfig?.privileged===false,ttyFalse:execution?.ProcessConfig?.tty===false,
      entrypointMatches:execution?.ProcessConfig?.entrypoint===UNIVERSAL_EXEC[0],argumentsMatch:JSON.stringify(execution?.ProcessConfig?.arguments)===JSON.stringify(UNIVERSAL_EXEC.slice(1))});
    const inspectExec=async()=>{
      const execution=await this.request('GET',`/exec/${diagnostic.execId}/json`,undefined,false,8192,options('exec-inspect'));
      diagnostic.exec=metadata(execution);diagnostic.inspectPolls++;
      const m=diagnostic.exec;
      if(!m.idMatches||!m.containerMatches||!m.privilegedFalse||!m.ttyFalse||!m.entrypointMatches||!m.argumentsMatch||m.running===null||m.exitCode===null)
        fail('universal_measurement_unresolved','exec_metadata_mismatch');
      return execution;
    };
    try {
      const before=await this.inspect(id,options('container-before'));
      if(before.Id!==id||before.State?.Running!==true||!/^sha256:[a-f0-9]{64}$/u.test(before.Image||''))fail('universal_measurement_identity','container_identity');
      const created=await this.request('POST',`/containers/${id}/exec`,{AttachStdout:true,AttachStderr:false,AttachStdin:false,Tty:false,Privileged:false,
        WorkingDir:'/app',Cmd:[...UNIVERSAL_EXEC]},false,4096,options('exec-create'));
      if(!/^[a-f0-9]{64}$/u.test(created?.Id||''))fail('universal_measurement_identity','exec_identity');diagnostic.execId=created.Id;
      // A timeout does not authorize a second CREATE/START, even for a read port.
      const output=await this.request('POST',`/exec/${diagnostic.execId}/start`,{Detach:false,Tty:false},true,66560,options('exec-start'));
      const outputStatus=diagnostic.httpStatus;
      let execution=await inspectExec();
      if(!execution.Running&&execution.ExitCode!==0)fail('universal_measurement_unresolved','exec_nonzero_exit');
      diagnostic.stage='exec-output';diagnostic.httpStatus=outputStatus;
      const pieces=[];let cursor=0,size=0;
      while(cursor<output.length){
        if(cursor+8>output.length||output[cursor]!==1||output[cursor+1]!==0||output[cursor+2]!==0||output[cursor+3]!==0)fail('universal_measurement_invalid','runtime_output_invalid');
        const length=output.readUInt32BE(cursor+4);if(length>output.length-cursor-8||(size+=length)>65536)fail('universal_measurement_invalid','runtime_output_invalid');
        pieces.push(output.subarray(cursor+8,cursor+8+length));cursor+=8+length;
      }
      let value;try{value=validateUniversalPreparedness(parseContractJson(Buffer.concat(pieces,size)));}catch(error){diagnostic.errorCategory='runtime_output_invalid';throw error;}
      // Attached stdout can close before Docker publishes terminal exec metadata. Only GET this SAME exec, with the full DTO already held.
      while(execution.Running){
        diagnostic.stage='exec-inspect';
        const remaining=deadline-Date.now();if(remaining<=0)fail('universal_measurement_unresolved','exec_running_deadline');
        await new Promise(resolve=>setTimeout(resolve,Math.min(25,remaining)));
        if(Date.now()>=deadline)fail('universal_measurement_unresolved','exec_running_deadline');
        execution=await inspectExec();
      }
      if(execution.ExitCode!==0)fail('universal_measurement_unresolved','exec_nonzero_exit');
      const after=await this.inspect(id,options('container-after'));
      if(after.Id!==before.Id||after.Image!==before.Image||after.State?.Running!==true||after.State.StartedAt!==before.State.StartedAt)fail('universal_measurement_identity','container_changed');
      return value;
    }catch(error){
      if(!diagnostic.errorCategory)diagnostic.errorCategory=error?.code==='engine_response_ambiguous'?'http_ambiguous':/^engine_http_\d{3}$/u.test(error?.code||'')?'http_status':error?.code==='engine_response_limit'?'response_limit':error?.code==='engine_invalid_json'?'http_invalid_json':'reader_failed';
      const code=error?.code==='engine_response_ambiguous'||error?.code==='universal_measurement_unresolved'?'universal_measurement_unresolved'
        :error instanceof SafeError&&/^universal_measurement_/u.test(error.code)?error.code:'universal_measurement_failed';
      const failure=new SafeError(code);
      measurementDiagnostics.set(failure,Object.freeze({...diagnostic,exec:diagnostic.exec?Object.freeze(diagnostic.exec):null,elapsedMs:Math.max(0,Date.now()-started),deadlineMs:timeoutMs}));
      throw failure;
    }
  }
  async helperOutput(id) {
    const data=await this.request('GET',`/containers/${id}/logs?stdout=true&stderr=false`,undefined,true);
    const pieces=[];let i=0;while(i<data.length){if(i+8>data.length)throw new SafeError('helper_output_invalid');const n=data.readUInt32BE(i+4);if(n>data.length-i-8)throw new SafeError('helper_output_invalid');pieces.push(data.subarray(i+8,i+8+n));i+=8+n;}
    const text=Buffer.concat(pieces).toString('utf8');if(text.length>65536)throw new SafeError('helper_output_limit');
    try{return JSON.parse(text);}catch{throw new SafeError('helper_output_invalid');}
  }
}
export function httpJson(origin,path,timeoutMs=10000) {
  const url=new URL(path,origin);if(url.protocol!=='http:'||!['127.0.0.1','[::1]'].includes(url.hostname)||url.username||url.password)throw new SafeError('readiness_origin_not_loopback');
  return new Promise((resolve,reject)=>{const req=http.get(url,res=>{let text='';res.on('data',b=>{text+=b;if(text.length>65536){req.destroy();reject(new SafeError('readiness_limit'));}});res.on('end',()=>{if(res.statusCode!==200)return reject(new SafeError('readiness_http'));try{resolve(JSON.parse(text));}catch{reject(new SafeError('readiness_invalid'));}});});const deadline=setTimeout(()=>{req.destroy();reject(new SafeError('readiness_timeout'));},timeoutMs);req.on('close',()=>clearTimeout(deadline));req.on('error',()=>reject(new SafeError('readiness_unavailable')));});
}
