import { randomBytes, webcrypto } from 'node:crypto';
import { types } from 'node:util';
import { createTrustedReadonlyQueryAdapter, captureQueryData } from '../modules/capabilities/server/readonly-queries.mjs';
import { canonicalHash, AccessError } from '../modules/capabilities/server/validation.mjs';

const need=(ok,code='external_resource_denied')=>{if(!ok)throw new AccessError(code);};
const canonical=value=>Array.isArray(value)?'['+value.map(canonical).join(',')+']':value&&typeof value==='object'?'{'+Object.keys(value).sort().map(key=>JSON.stringify(key)+':'+canonical(value[key])).join(',')+'}':JSON.stringify(value);
function closed(value,required,optional=[]){need(value&&typeof value==='object'&&!types.isProxy(value)&&!Array.isArray(value),'external_query_invalid');const d=Object.getOwnPropertyDescriptors(value);need(Reflect.ownKeys(d).every(key=>typeof key==='string'&&[...required,...optional].includes(key)&&d[key].enumerable&&Object.hasOwn(d[key],'value'))&&required.every(key=>Object.hasOwn(d,key)),'external_query_invalid');return Object.fromEntries(Object.entries(d).map(([key,item])=>[key,item.value]));}
async function deviceKey(raw){const value=closed(raw,['deviceId','publicJwk','signature']),jwk=closed(value.publicJwk,['crv','kty','x','y']);need(jwk.crv==='P-256'&&jwk.kty==='EC'&&/^[A-Za-z0-9_-]{43}$/.test(jwk.x)&&/^[A-Za-z0-9_-]{43}$/.test(jwk.y)&&/^[A-Za-z0-9_-]{86}$/.test(value.signature));const bytes=await webcrypto.subtle.digest('SHA-256',new TextEncoder().encode(JSON.stringify({crv:jwk.crv,kty:jwk.kty,x:jwk.x,y:jwk.y})));need(value.deviceId==='dev_'+Buffer.from(bytes).toString('base64url').slice(0,32));return {value,key:await webcrypto.subtle.importKey('jwk',jwk,{name:'ECDSA',namedCurve:'P-256'},false,['verify'])};}
async function verified(packet,prefix,expectedDeviceId){const p=closed(packet,['payload','device']),device=await deviceKey(p.device);if(expectedDeviceId!==undefined)need(device.value.deviceId===expectedDeviceId);need(Buffer.byteLength(canonical(p.payload))<=8192&&await webcrypto.subtle.verify({name:'ECDSA',hash:'SHA-256'},device.key,Buffer.from(device.value.signature,'base64url'),new TextEncoder().encode(prefix+canonical(p.payload))));return p;}

// A transport challenge proves only the current installed Native device. It
// cannot approve a project grant or replace the Root/Native read predicates.
export async function verifyTavyshNativeBrokerDevice(packet,deviceId){return verified(captureQueryData(packet),'TAVYSH/SOURCE/DEVICE-READ/V1\0',deviceId);}

/** Native-side signer/consent are supplied by the existing owned device broker.
 * This is a readonly capability adapter, NOT the unsupported Source BFF carrier.
 * Root metadata never grants a local project; approved Native consent + fresh
 * signed device checks are required in addition to existing Root/Apps fences.
 */
export async function createTavyshDeviceStatusAdapter(options){
  const v=closed(options,['consent','queryDevice'],['clock']);need(typeof v.queryDevice==='function','external_query_invalid');
  const clock=v.clock??Date.now;need(typeof clock==='function','external_query_invalid');
  const consent=await verified(captureQueryData(v.consent),'TAVYSH/SOURCE/DEVICE-CONSENT/V1\0');
  const p=closed(consent.payload,['schema','origin','instanceId','grantId','scope','scopeDigest','issuedAt','expiresAt']),scope=closed(p.scope,['appId','resourceId','target','rootPrincipal','selection','sourceRelease']);
  const principal=closed(scope.rootPrincipal,['accountId','clientId','principalId']);closed(scope.target,['revision','digest']);closed(scope.selection,['projectId','incarnationId']);closed(scope.sourceRelease,['id','version','digest']);
  need(p.schema==='tavysh.native-device-read-consent.v1'&&new URL(p.origin).origin===p.origin&&new URL(p.origin).protocol==='https:'&&/^[a-f0-9]{32}$/.test(p.instanceId)&&/^[a-f0-9]{32}$/.test(p.grantId)
    &&p.scopeDigest===canonicalHash(scope)&&Number.isSafeInteger(p.issuedAt)&&Number.isSafeInteger(p.expiresAt)&&p.expiresAt>clock()&&p.expiresAt<=p.issuedAt+300000);
  let stopped=false;
  const live=()=>need(!stopped&&p.expiresAt>clock());
  function authority(request){live();if(request.authorization){const a=request.authorization;need(a.accountId===principal.accountId&&a.clientId===principal.clientId&&a.principalId===principal.principalId
    &&a.resources?.length===1&&a.resources[0]===scope.resourceId&&a.recipients?.length===1&&a.recipients[0]===scope.resourceId&&a.effects?.length===0);}else need(request.actor?.accountId===principal.accountId);}
  const adapter=createTrustedReadonlyQueryAdapter({
    withAuthority(request,callback){authority(request);return callback();},
    async query(raw,signal,currentAuthority){
      const request=captureQueryData(raw);closed(request,['requestId','input','authorization']);closed(request.input,[]);authority(request);
      const fresh=()=>{live();need(!signal?.aborted,'external_query_unconfirmed');need(typeof currentAuthority==='function'&&currentAuthority()===true);};fresh();
      const nativeDenials=new Set(['tavysh_native_read_denied','tavysh_native_read_closed','tavysh_native_read_clock','tavysh_native_profile_required','tavysh_native_cloud_scope_changed','tavysh_native_profile_changed','tavysh_native_project_denied','tavysh_native_consent_changed','tavysh_native_consent_required','tavysh_native_device_changed','tavysh_native_read_changed']);
      async function callNative(nonce){try{return await v.queryDevice(Object.freeze({grantId:p.grantId,scopeDigest:p.scopeDigest,nonce,operation:'project.status'}),signal);}catch(error){fresh();const code=error&&typeof error==='object'&&!types.isProxy(error)?Object.getOwnPropertyDescriptor(error,'code'):undefined;need(false,code&&Object.hasOwn(code,'value')&&nativeDenials.has(code.value)?'external_resource_denied':'external_query_unconfirmed');}}
      const nonce=randomBytes(32).toString('base64url'),start=clock();
      const rawReply=await callNative(nonce);fresh();
      const reply=await verified(captureQueryData(rawReply),'TAVYSH/SOURCE/DEVICE-READ/V1\0',consent.device.deviceId);fresh();
      const data=closed(reply.payload,['schema','origin','instanceId','grantId','scopeDigest','nonce','projectId','incarnationId','revision','cloudIncarnationId','issuedAt','expiresAt','status']);
      need(data.schema==='tavysh.native-device-project-read.v1'&&data.origin===p.origin&&data.instanceId===p.instanceId&&data.grantId===p.grantId&&data.scopeDigest===p.scopeDigest&&data.nonce===nonce
        &&data.projectId===scope.selection.projectId&&data.incarnationId===scope.selection.incarnationId
        &&Number.isSafeInteger(data.issuedAt)&&data.issuedAt>=start&&data.issuedAt<=clock()&&Number.isSafeInteger(data.expiresAt)&&data.expiresAt>clock()
        &&data.expiresAt<=data.issuedAt+10000&&data.expiresAt<=p.expiresAt);
      const status=closed(data.status,['schema','status','revision','versions','performances','publications','visibility','readOnly','publishes','playsAudio']);
      need(status.schema==='tavysh.source-own-project-status.v1'&&data.revision===status.revision&&Number.isSafeInteger(status.revision)&&status.revision>0
        &&['draft','ready','archived'].includes(status.status)
        &&[status.versions,status.performances,status.publications].every(value=>Number.isSafeInteger(value)&&value>=0&&value<=512)
        &&['private','circle','unlisted','public'].includes(status.visibility)&&status.readOnly===true&&status.publishes===false&&status.playsAudio===false,'external_query_output_invalid');
      // One additional readonly attestation rechecks the Native device/epoch
      // after verification/Root awaits. No effect or automatic request retry.
      const finalNonce=randomBytes(32).toString('base64url'),finalStart=clock(),finalReply=await verified(captureQueryData(await callNative(finalNonce)),'TAVYSH/SOURCE/DEVICE-READ/V1\0',consent.device.deviceId);fresh();
      const final=closed(finalReply.payload,['schema','origin','instanceId','grantId','scopeDigest','nonce','projectId','incarnationId','revision','cloudIncarnationId','issuedAt','expiresAt','status']);
      need(final.schema===data.schema&&final.origin===data.origin&&final.instanceId===data.instanceId&&final.grantId===data.grantId&&final.scopeDigest===data.scopeDigest&&final.nonce===finalNonce
        &&final.projectId===data.projectId&&final.incarnationId===data.incarnationId&&final.revision===data.revision&&final.cloudIncarnationId===data.cloudIncarnationId
        &&Number.isSafeInteger(final.issuedAt)&&final.issuedAt>=finalStart&&final.issuedAt<=clock()&&Number.isSafeInteger(final.expiresAt)&&final.expiresAt>clock()
        &&final.expiresAt<=final.issuedAt+10000&&final.expiresAt<=p.expiresAt&&canonicalHash(final.status)===canonicalHash(status));
      fresh();return captureQueryData({resourceId:scope.resourceId,...status});
    },
  });
  const binding=Object.freeze({id:'app.'+scope.appId+':tavysh-device-status',version:1,digest:canonicalHash({protocol:'tavysh.native-device-project-read.v1',scope,origin:p.origin,instanceId:p.instanceId,grantId:p.grantId,deviceId:consent.device.deviceId})});
  return Object.freeze({adapter,binding,scope:scope,close(){stopped=true;}});
}
