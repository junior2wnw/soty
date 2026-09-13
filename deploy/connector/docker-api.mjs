import http from 'node:http';

export class SafeError extends Error { constructor(code) { super(code); this.code=code; } }
export class DockerApi {
  constructor({socketPath='/var/run/docker.sock',timeoutMs=10000}={}) { this.socketPath=socketPath;this.timeoutMs=timeoutMs; }
  request(method,path,body,raw=false) {
    return new Promise((resolve,reject)=>{
      const data=body===undefined?null:Buffer.from(JSON.stringify(body));
      const req=http.request({socketPath:this.socketPath,path:'/v1.45'+path,method,headers:data?{'content-type':'application/json','content-length':data.length}:{}},res=>{
        let length=0;const chunks=[];
        res.on('data',b=>{length+=b.length;if(length>4*1024*1024){req.destroy();reject(new SafeError('engine_response_limit'));}else chunks.push(b);});
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
