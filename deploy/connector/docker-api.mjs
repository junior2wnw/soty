import http from 'node:http';
import { parseContractJson } from '../../modules/app-contract/json.mjs';
import { validateUniversalPreparedness } from '../../modules/app-contract/universal-preparedness.mjs';

const UNIVERSAL_EXEC = Object.freeze(['node','--input-type=module','-e',
  "try { const {readUniversalOperator}=await import('./server/universal-operator.js'); const value=await readUniversalOperator(); process.stdout.write(JSON.stringify(value)+'\\n'); } catch { process.exitCode=1; }"]);

export class SafeError extends Error { constructor(code) { super(code); this.code=code; } }
export class DockerApi {
  constructor({socketPath='/var/run/docker.sock',timeoutMs=10000}={}) { this.socketPath=socketPath;this.timeoutMs=timeoutMs; }
  request(method,path,body,raw=false,maxBytes=4*1024*1024) {
    return new Promise((resolve,reject)=>{
      const data=body===undefined?null:Buffer.from(JSON.stringify(body));
      const req=http.request({socketPath:this.socketPath,path:'/v1.45'+path,method,headers:data?{'content-type':'application/json','content-length':data.length}:{}},res=>{
        let length=0;const chunks=[];
        res.on('aborted',()=>reject(new SafeError('engine_response_ambiguous')));
        res.on('error',()=>reject(new SafeError('engine_response_ambiguous')));
        res.on('data',b=>{length+=b.length;if(length>maxBytes){req.destroy();reject(new SafeError('engine_response_limit'));}else chunks.push(b);});
        res.on('end',()=>{const bytes=Buffer.concat(chunks);if(res.statusCode<200||res.statusCode>=300){reject(new SafeError('engine_http_'+res.statusCode));return;}if(raw)return resolve(bytes);try{resolve(bytes.length?JSON.parse(bytes):null);}catch{reject(new SafeError('engine_invalid_json'));}});
      });
      const deadline=setTimeout(()=>{req.destroy();reject(new SafeError('engine_response_ambiguous'));},this.timeoutMs);
      req.on('close',()=>clearTimeout(deadline));
      req.on('error',()=>reject(new SafeError('engine_response_ambiguous')));if(data)req.write(data);req.end();
    });
  }
  inspect(id){return this.request('GET',`/containers/${encodeURIComponent(id)}/json`);}
  image(id){return this.request('GET',`/images/${encodeURIComponent(id)}/json`);}
  create(name,config){return this.request('POST','/containers/create?name='+encodeURIComponent(name),config);}
  start(id){return this.request('POST',`/containers/${id}/start`);}
  stop(id){return this.request('POST',`/containers/${id}/stop?t=10`);}
  rename(id,name){return this.request('POST',`/containers/${id}/rename?name=${encodeURIComponent(name)}`);}
  remove(id){return this.request('DELETE',`/containers/${id}?force=false&v=false`);}
  /** Fixed read-only exec. No arbitrary command/path/arguments or secret output. */
  async universalPreparedness(id) {
    if(!/^[a-f0-9]{64}$/u.test(id||''))throw new SafeError('universal_measurement_identity');
    let execId;
    try {
      const before=await this.inspect(id);
      if(before.Id!==id||before.State?.Running!==true||!/^sha256:[a-f0-9]{64}$/u.test(before.Image||''))throw new SafeError('universal_measurement_identity');
      const created=await this.request('POST',`/containers/${id}/exec`,{AttachStdout:true,AttachStderr:false,AttachStdin:false,Tty:false,Privileged:false,
        WorkingDir:'/app',Cmd:[...UNIVERSAL_EXEC]},false,4096);
      if(!/^[a-f0-9]{64}$/u.test(created?.Id||''))throw new SafeError('universal_measurement_identity');execId=created.Id;
      // A timeout does not authorize a second CREATE/START, even for a read port.
      const output=await this.request('POST',`/exec/${execId}/start`,{Detach:false,Tty:false},true,66560);
      const execution=await this.request('GET',`/exec/${execId}/json`,undefined,false,8192);
      if(execution.ID!==execId||execution.ContainerID!==id||execution.Running!==false||execution.ExitCode!==0
        ||execution.ProcessConfig?.privileged!==false||execution.ProcessConfig?.tty!==false
        ||execution.ProcessConfig?.entrypoint!==UNIVERSAL_EXEC[0]||JSON.stringify(execution.ProcessConfig?.arguments)!==JSON.stringify(UNIVERSAL_EXEC.slice(1)))throw new SafeError('universal_measurement_unresolved');
      const pieces=[];let cursor=0,size=0;
      while(cursor<output.length){
        if(cursor+8>output.length||output[cursor]!==1||output[cursor+1]!==0||output[cursor+2]!==0||output[cursor+3]!==0)throw new SafeError('universal_measurement_invalid');
        const length=output.readUInt32BE(cursor+4);if(length>output.length-cursor-8||(size+=length)>65536)throw new SafeError('universal_measurement_invalid');
        pieces.push(output.subarray(cursor+8,cursor+8+length));cursor+=8+length;
      }
      const value=validateUniversalPreparedness(parseContractJson(Buffer.concat(pieces,size)));
      const after=await this.inspect(id);
      if(after.Id!==before.Id||after.Image!==before.Image||after.State?.Running!==true||after.State.StartedAt!==before.State.StartedAt)throw new SafeError('universal_measurement_identity');
      return value;
    }catch(error){
      if(error?.code==='engine_response_ambiguous'||error?.code==='universal_measurement_unresolved')throw new SafeError('universal_measurement_unresolved');
      if(error instanceof SafeError&&/^universal_measurement_/u.test(error.code))throw error;
      throw new SafeError('universal_measurement_failed');
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
