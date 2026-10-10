import { createHmac, timingSafeEqual, randomBytes } from 'node:crypto';
import { request as httpRequest } from 'node:http';
import { capture, hash, need } from './profile.mjs';
import { requireEmbedAdapter } from './profile-dispatch.mjs';

// Spend1s less than the unchanged10s receiver bound; only transport issuance
// tolerates bounded issuer-ahead skew. No Root/Native lifetime is extended.
const AUTHORITY_ISSUANCE_CLOCK_SKEW_MS = 1000;

/** Source server to its installed connector: one authenticated read-only IPC
 * route. The caller cannot choose a URL, subject, destination or command. */
export function createSourceAuthorityClient({profile:raw,key,connectorPort=49424,clock=Date.now}={}) {
  const profile=requireEmbedAdapter(raw);need(Buffer.isBuffer(key)&&key.length===32&&Number.isSafeInteger(connectorPort)&&connectorPort>=1024&&connectorPort<=65535);
  const privateKey=Buffer.from(key),mac=text=>createHmac('sha256',privateKey).update(text).digest('base64url');
  return async function readAuthority({reference,connector}) {
    need(hash(connector)===hash(profile.connector),'scoped_embed_connector_mismatch',403);
    const nonce=randomBytes(32).toString('base64url'),text=JSON.stringify({schema:'soty.selected-source-authority.v1',appId:profile.appId,
      profileDigest:profile.digest,reference:capture(reference),nonce,expiresAt:clock()+10000-AUTHORITY_ISSUANCE_CLOCK_SKEW_MS});
    return new Promise((resolve,reject)=>{
      const request=httpRequest({hostname:'127.0.0.1',port:connectorPort,path:'/apps/scoped/authority',method:'POST',agent:false,
        headers:{'content-type':'application/json','x-soty-source-mac':mac('authority\0'+text),'content-length':Buffer.byteLength(text)},
        signal:AbortSignal.timeout(8000)},response=>{
        const parts=[];let bytes=0;response.on('data',part=>{bytes+=part.length;if(bytes>16384)response.destroy();else parts.push(part);});
        response.on('error',()=>reject(Object.assign(new Error('scoped_embed_authority_unavailable'),{status:503,code:'scoped_embed_authority_unavailable'})));
        response.on('end',()=>{try{const body=Buffer.concat(parts).toString('utf8'),signature=response.headers['x-soty-source-mac'];
          need(response.statusCode===200&&typeof signature==='string'&&signature.length===43&&timingSafeEqual(Buffer.from(signature),Buffer.from(mac('authority-result\0'+body))),
            'scoped_embed_authority_denied',response.statusCode===403?403:503);
          const value=capture(JSON.parse(body));need(value.nonce===nonce&&value.context.profileDigest===profile.digest&&hash(value.context.reference)===hash(reference),
            'scoped_embed_authority_mismatch',403);resolve(value.context);
        }catch(error){reject(error);}});
      });request.on('error',()=>reject(Object.assign(new Error('scoped_embed_authority_unavailable'),{status:503,code:'scoped_embed_authority_unavailable'})));request.end(text);
    });
  };
}
