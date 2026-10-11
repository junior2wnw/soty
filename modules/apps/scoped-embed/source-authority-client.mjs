import { createHmac, timingSafeEqual, randomBytes } from 'node:crypto';
import { request as httpRequest } from 'node:http';
import { capture, hash, need } from './profile.mjs';
import { requireEmbedAdapter } from './profile-dispatch.mjs';
import { isRootSignedWorkspaceSource } from '../../source-app/server/standard-profile.mjs';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';

const verifiedRoot = new WeakMap();
let jose;
const maintainedJose = () => jose ??= import(pathToFileURL(createRequire(createRequire(import.meta.url).resolve('openid-client')).resolve('jose')).href);
export function isVerifiedRootContext(proof) { const row=verifiedRoot.get(proof);return !!row&&row.expiresAt>row.clock(); }
export function readVerifiedRootContext(proof, profileDigest) {
  need(isVerifiedRootContext(proof)&&proof.profileDigest===profileDigest,'source_root_witness_invalid',403);return proof;
}

// Spend1s less than the unchanged10s receiver bound; only transport issuance
// tolerates bounded issuer-ahead skew. No Root/Native lifetime is extended.
const AUTHORITY_ISSUANCE_CLOCK_SKEW_MS = 1000;

/** Source server to its installed connector: one authenticated read-only IPC
 * route. The caller cannot choose a URL, subject, destination or command. */
export function createSourceAuthorityClient({profile:raw,key,connectorPort=49424,clock=Date.now}={}) {
  const profile=requireEmbedAdapter(raw);need(Buffer.isBuffer(key)&&key.length===32&&Number.isSafeInteger(connectorPort)&&connectorPort>=1024&&connectorPort<=65535);
  const privateKey=Buffer.from(key),mac=text=>createHmac('sha256',privateKey).update(text).digest('base64url');
  const rootSigned=isRootSignedWorkspaceSource(profile.sourceProfile);let jwks;
  async function verifyIdentityWitness(value,challenge) {
    const {jwtVerify,createRemoteJWKSet,customFetch}=await maintainedJose();
    need(typeof value.identityWitness==='string'&&value.identityWitness.length<=8192,'source_root_witness_invalid',403);
    jwks??=createRemoteJWKSet(new URL(profile.issuer+'/jwks'),{timeoutDuration:5000,[customFetch]:async(url,init)=>{
      need(String(url)===profile.issuer+'/jwks','source_root_witness_invalid',403);
      const response=await fetch(url,{...init,redirect:'error',signal:AbortSignal.timeout(5000)});
      need(response.status===200&&response.body,'source_root_witness_unavailable',503);
      const parts=[];let size=0;for await(const part of response.body){size+=part.length;need(size<=65536,'source_root_witness_unavailable',503);parts.push(part);}
      return new Response(Buffer.concat(parts),{status:200,headers:{'content-type':'application/json'}});
    }});
    try {
      const {payload,protectedHeader}=await jwtVerify(value.identityWitness,jwks,{algorithms:['RS256'],issuer:profile.issuer,
        audience:'urn:soty:source-authority',typ:'soty-source-authority+jwt',currentDate:new Date(clock()),clockTolerance:0});
      need(Object.keys(protectedHeader).every(key=>['alg','typ','kid'].includes(key))
        &&Object.keys(payload).sort().join(',')==='aud,contextDigest,exp,iat,iss,sourceChallenge,sub'
        &&payload.aud==='urn:soty:source-authority'&&payload.sub===value.context.humanPrincipal.subject
        &&payload.sourceChallenge===challenge&&payload.contextDigest===hash(value.context)
        &&Number.isSafeInteger(payload.iat)&&Number.isSafeInteger(payload.exp)&&payload.exp>payload.iat
        &&payload.exp-payload.iat<=10&&payload.iat*1000<=clock()+1000&&payload.exp*1000>clock()
        &&payload.exp*1000<=value.context.expiresAt,'source_root_witness_invalid',403);
      verifiedRoot.set(value.context,{expiresAt:payload.exp*1000,clock});return value.context;
    }catch{need(false,'source_root_witness_invalid',403);}
  }
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
        response.on('end',async()=>{try{const body=Buffer.concat(parts).toString('utf8'),signature=response.headers['x-soty-source-mac'];
          need(response.statusCode===200&&typeof signature==='string'&&signature.length===43&&timingSafeEqual(Buffer.from(signature),Buffer.from(mac('authority-result\0'+body))),
            'scoped_embed_authority_denied',response.statusCode===403?403:503);
          const value=capture(JSON.parse(body));need(value.nonce===nonce&&value.context.profileDigest===profile.digest&&hash(value.context.reference)===hash(reference),
            'scoped_embed_authority_mismatch',403);resolve(rootSigned?await verifyIdentityWitness(value,nonce):value.context);
        }catch(error){reject(error);}});
      });request.on('error',()=>reject(Object.assign(new Error('scoped_embed_authority_unavailable'),{status:503,code:'scoped_embed_authority_unavailable'})));request.end(text);
    });
  };
}
