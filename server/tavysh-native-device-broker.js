import { randomBytes } from 'node:crypto';
import { types } from 'node:util';
import { captureQueryData } from '../modules/capabilities/server/readonly-queries.mjs';
import { canonicalHash, AccessError } from '../modules/capabilities/server/validation.mjs';
import { verifyTavyshNativeBrokerDevice } from './tavysh-device-read-adapter.js';

const BASE='/api/source/device/v1/',nonce=()=>randomBytes(32).toString('base64url');
const failure=(code,status)=>Object.assign(new AccessError(code),{status});
const need=(ok,code='tavysh_native_broker_denied',status=403)=>{if(!ok)throw failure(code,status);};
const data=value=>captureQueryData(value);
function fields(value,keys){const v=data(value);need(v&&typeof v==='object'&&!Array.isArray(v)&&Object.keys(v).length===keys.length&&keys.every(k=>Object.hasOwn(v,k)),'tavysh_native_broker_invalid',400);return v;}
const nativeDenials=new Set(['tavysh_native_read_denied','tavysh_native_read_closed','tavysh_native_read_clock','tavysh_native_profile_required','tavysh_native_cloud_scope_changed','tavysh_native_profile_changed','tavysh_native_project_denied','tavysh_native_consent_changed','tavysh_native_consent_required','tavysh_native_device_changed','tavysh_native_read_changed']);

/** Optional installed Native HTTP transport. Root methods are private CODE
 * ports called by its existing admitted adapter, never HTTP commands. Native
 * receives one private response nonce only after a fresh P256 challenge. Only
 * consent/status are delivered; there is no actor creation, eval, job or DB.
 * Serve under the ORIGINAL Native HTTPS origin through its approved connector.
 * Full scoped-embed profiles are not accepted or manufactured here. */
export function createTavyshNativeDeviceBroker(options){
  const v=fields(options,['nativeOrigin','deviceId','sourceRelease']);
  const origin=new URL(v.nativeOrigin);need(origin.origin===v.nativeOrigin&&origin.protocol==='https:');
  need(/^dev_[A-Za-z0-9_-]{32}$/.test(v.deviceId));
  fields(v.sourceRelease,['id','version','digest']);
  let closed=false;const instanceId=nonce(),challenges=new Map(),pending=new Map();
  const live=()=>need(!closed,'tavysh_native_broker_closed',503);
  function retire(){const at=Date.now();for(const [id,c]of challenges)if(c.expiresAt<=at)challenges.delete(id);}
  function enqueue(kind,value,signal){
    live();need(!signal?.aborted,'external_query_unconfirmed',503);need(pending.size<32,'tavysh_native_broker_capacity',503);
    const id=nonce(),expiresAt=Date.now()+(kind==='consent'?120000:8000);let entry;
    const promise=new Promise((resolve,reject)=>{
      const finish=(error,result)=>{if(pending.get(id)!==entry)return;pending.delete(id);clearTimeout(entry.timer);signal?.removeEventListener('abort',entry.abort);error?reject(error):resolve(result);};
      entry={id,kind,value:data(value),expiresAt,delivered:false,finish,abort:()=>finish(failure('external_query_unconfirmed',503))};
      entry.timer=setTimeout(entry.abort,expiresAt-Date.now());entry.timer.unref?.();pending.set(id,entry);signal?.addEventListener('abort',entry.abort,{once:true});
    });
    return promise;
  }
  async function body(req){
    let bytes=0;const chunks=[],timer=setTimeout(()=>req.destroy(failure('tavysh_native_broker_deadline',503)),8000);timer.unref?.();
    try{for await(const part of req){bytes+=part.length;need(bytes<=16384,'tavysh_native_broker_limit',413);chunks.push(part);}const text=new TextDecoder('utf8',{fatal:true}).decode(Buffer.concat(chunks));return data(JSON.parse(text));}
    finally{clearTimeout(timer);}
  }
  function response(res,status,value){const raw=JSON.stringify(value);need(Buffer.byteLength(raw)<=16384,'tavysh_native_broker_limit',503);res.writeHead(status,{'content-type':'application/json','cache-control':'no-store'});res.end(raw);}
  async function handle(req,res){
    if(!req.url?.startsWith(BASE))return false;
    try{
      live();need(req.method==='POST'&&req.headers.origin===v.nativeOrigin,'tavysh_native_broker_origin_denied');
      const path=req.url.slice(BASE.length);need(['installation','challenge','poll','complete'].includes(path),'tavysh_native_broker_invalid',404);
      const input=await body(req);live();
      if(path==='installation'){fields(input,[]);response(res,200,{schema:'tavysh.native-device-broker-installation.v1',nativeOrigin:v.nativeOrigin,deviceId:v.deviceId,sourceRelease:v.sourceRelease,instanceId});return true;}
      if(path==='challenge'){
        fields(input,[]);retire();need(challenges.size<64,'tavysh_native_broker_capacity',503);
        const challenge={schema:'tavysh.native-device-broker-challenge.v1',nativeOrigin:v.nativeOrigin,instanceId,nonce:nonce(),expiresAt:Date.now()+10000};
        challenges.set(challenge.nonce,challenge);response(res,200,challenge);return true;
      }
      if(path==='poll'){
        const {proof}=fields(input,['proof']),p=await verifyTavyshNativeBrokerDevice(proof,v.deviceId);live();
        const expected=challenges.get(p.payload.nonce);need(expected&&expected.expiresAt>Date.now()&&canonicalHash(p.payload)===canonicalHash(expected));challenges.delete(expected.nonce);
        const entry=[...pending.values()].find(e=>!e.delivered&&e.expiresAt>Date.now());
        if(entry)entry.delivered=true;
        response(res,200,{schema:'tavysh.native-device-broker-poll.v1',command:entry?{id:entry.id,kind:entry.kind,value:entry.value,expiresAt:entry.expiresAt}:null});return true;
      }
      // This nonce was delivered ONLY to the fresh authenticated Native page.
      // The current key may have disappeared during readSigned; do not require
      // a replacement key to transport its genuine closed denial. Consume once.
      const {id,result}=fields(input,['id','result']),entry=pending.get(id);
      need(entry&&entry.delivered&&entry.expiresAt>Date.now());fields(result,['ok',...(result.ok===true?['value']:['code'])]);need(typeof result.ok==='boolean');
      if(result.ok){entry.finish(null,result.value);}
      else entry.finish(failure(nativeDenials.has(result.code)?result.code:'external_query_unconfirmed',nativeDenials.has(result.code)?403:503));
      response(res,200,{schema:'tavysh.native-device-broker-ack.v1',accepted:true});return true;
    }catch(error){
      // Never echo error messages, fields, device IDs, signatures or a stack.
      const own=error&&typeof error==='object'&&!types.isProxy(error)&&Object.getPrototypeOf(error)===AccessError.prototype;
      const c=own?Object.getOwnPropertyDescriptor(error,'code'):undefined,s=own?Object.getOwnPropertyDescriptor(error,'status'):undefined;
      const known=c&&Object.hasOwn(c,'value')&&typeof c.value==='string'&&(c.value.startsWith('tavysh_native_broker_')||c.value==='external_query_unconfirmed');
      const badDevice=own&&c?.value==='external_resource_denied';
      if(!res.headersSent&&!res.destroyed)response(res,badDevice?403:known&&[400,404,413,503].includes(s?.value)?s.value:known?403:503,{ok:false,error:{code:badDevice?'tavysh_native_broker_denied':known?c.value:'external_query_unconfirmed'}});
      else res.destroy();return true;
    }
  }
  return Object.freeze({handle,
    requestConsent:(scope,signal)=>enqueue('consent',scope,signal),
    readSigned:(challenge,signal)=>enqueue('read',challenge,signal),
    close(){if(closed)return;closed=true;for(const row of [...pending.values()])row.abort();challenges.clear();},
  });
}
