import { createHmac, timingSafeEqual, randomBytes } from 'node:crypto';
import { createLocalScopedEmbedBroker } from './local-broker.mjs';
import { capture, closed, hash, need, scopedEmbedProfile, SCOPED_EMBED_PROFILE, SCOPED_EMBED_LIMITS } from './profile.mjs';

const mac=(key,text)=>createHmac('sha256',key).update(text).digest('base64url');
const secret=value=>typeof value==='string'&&/^[A-Za-z0-9_-]{43}$/.test(value);
const equal=(a,b)=>typeof a==='string'&&typeof b==='string'&&a.length===b.length&&timingSafeEqual(Buffer.from(a),Buffer.from(b));

/** Installed connector host configuration only. Keys are private Source mounts,
 * not manifests, HTTP bodies, URLs or bearer credentials in iframe JavaScript. */
export function createConnectorScopedFactory({ entries = [], clock = Date.now } = {}) {
  need(Array.isArray(entries)&&entries.length<=64);
  const approved=new Map(),brokers=new Map(),ipcNonces=new Map();let reader=null,channelIdentity=null;
  for(const entry of entries) {
    closed(entry,['profile','key']);const profile=scopedEmbedProfile(entry.profile);
    need(secret(entry.key),'scoped_embed_key_required');const key=Buffer.from(entry.key,'base64url');need(key.length===32);
    const {digest:_derived,...raw}=profile;const id=profile.appId+':'+profile.target.revision;
    need(!approved.has(id));approved.set(id,{profile,raw,key});
  }
  function item(target) {
    const value=approved.get(target.appId+':'+target.revision);
    need(value&&target.profile===SCOPED_EMBED_PROFILE&&target.digest===value.profile.target.digest
      && target.ownerAccountId===value.profile.resource.tenantId,'scoped_embed_binding_unapproved',403);
    return value;
  }
  function rootRead(request) {
    need(reader&&channelIdentity&&hash(request.connector)===hash(channelIdentity),'scoped_embed_channel_required',503);
    return reader(request);
  }
  return Object.freeze({
    profiles: Object.freeze(entries.length?[SCOPED_EMBED_PROFILE]:[]),
    connect(readAuthority,identity) {need(typeof readAuthority==='function');reader=readAuthority;channelIdentity=capture({linkId:identity.linkId,hostDeviceId:identity.hostDeviceId,connectorId:identity.connectorId});},
    disconnected() {reader=null;channelIdentity=null;for(const broker of brokers.values())broker.close();brokers.clear();ipcNonces.clear();},
    accepts(target) {try{item(target);return true;}catch{return false;}},
    broker(target,assertBinding,generation) {
      need(generation && typeof generation==='object');const approvedItem=item(target),id=generation;
      let broker=brokers.get(id);if(!broker){broker=createLocalScopedEmbedBroker({profile:approvedItem.raw,localPort:target.port,key:approvedItem.key,
        clock,readAuthority:rootRead,assertBinding});brokers.set(id,broker);}return broker;
    },
    async probe(target,signal) {
      const approvedItem=item(target),broker=createLocalScopedEmbedBroker({profile:approvedItem.raw,localPort:target.port,key:approvedItem.key,
        clock,readAuthority:rootRead,assertBinding:()=>true});
      try{return await broker.probe(signal);}finally{broker.close();}
    },
    forget(reference) {for(const broker of brokers.values())broker.forget(reference);},
    async sourceAuthority(request,response) {
      const path='/apps/scoped/authority';
      need(request.url===path && request.method==='POST'&&!request.headers.origin
        && ['127.0.0.1','localhost','[::1]'].includes(new URL('http://'+request.headers.host).hostname),'scoped_embed_ipc_denied',403);
      let size=0;const parts=[];for await(const part of request){size+=part.length;need(size<=16384,'scoped_embed_ipc_limit',413);parts.push(part);}
      const text=Buffer.concat(parts).toString('utf8'),value=capture(JSON.parse(text));
      closed(value,['schema','appId','profileDigest','reference','nonce','expiresAt']);
      need(value.schema==='soty.selected-source-authority.v1'&&secret(value.nonce)&&Number.isSafeInteger(value.expiresAt)
        && value.expiresAt>clock()&&value.expiresAt<=clock()+10000,'scoped_embed_ipc_invalid',403);
      const approvedItem=[...approved.values()].find(entry=>entry.profile.appId===value.appId&&entry.profile.digest===value.profileDigest);
      need(approvedItem&&equal(request.headers['x-soty-source-mac'],mac(approvedItem.key,'authority\0'+text)),'scoped_embed_ipc_invalid',403);
      for(const [id,expires]of ipcNonces)if(expires<=clock())ipcNonces.delete(id);
      need(!ipcNonces.has(value.nonce)&&ipcNonces.size<4096,'scoped_embed_ipc_replayed',403);ipcNonces.set(value.nonce,value.expiresAt);
      const context=await rootRead({reference:value.reference,connector:approvedItem.profile.connector});
      need(context.profileDigest===approvedItem.profile.digest,'scoped_embed_ipc_mismatch',403);
      const body=JSON.stringify({nonce:value.nonce,context});need(Buffer.byteLength(body)<=16384,'scoped_embed_ipc_limit',502);
      response.writeHead(200,{'content-type':'application/json','cache-control':'no-store','x-soty-source-mac':mac(approvedItem.key,'authority-result\0'+body)});
      response.end(body);
    },
  });
}
