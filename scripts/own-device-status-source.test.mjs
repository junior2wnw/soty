import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { createHmac,generateKeyPairSync,randomBytes } from 'node:crypto';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { createOwnDeviceStatusNative } from './agent-modules/own-device-status-source.mjs';
import * as sdk from '../modules/source-app/server/index.mjs';
import { createNativeAuthorityRuntime } from '../modules/source-app/server/native-authority.mjs';
import { createSourceAuthorityClient } from '../modules/apps/scoped-embed/source-authority-client.mjs';
import { selectedResourceProfile } from '../modules/apps/scoped-embed/resource-profile.mjs';
import { canonicalHash as digest } from '../modules/capabilities/server/validation.mjs';
import { createSelectedTransitSourceApplications } from '../server/transit-native-source.js';

test('genuine Root witness and current Native device admit only status READ; foreign, revoke and invoke deny', async t => {
  let now=Math.floor(Date.now()/1000)*1000;
  const clock=()=>now,key=randomBytes(32),pair=generateKeyPairSync('rsa',{modulusLength:2048}),identity={principalId:'synthetic-os-owner',deviceId:'native-own-device',installId:'native-install'};
  let current={...identity},calls=0,profile,context;
  const require=createRequire(import.meta.url),jose=await import(pathToFileURL(createRequire(require.resolve('openid-client')).resolve('jose')).href);
  const publicJwk={...pair.publicKey.export({format:'jwk'}),kid:'synthetic',alg:'RS256',use:'sig'};
  const signer=await jose.importJWK({...pair.privateKey.export({format:'jwk'}),kid:'synthetic',alg:'RS256',use:'sig'},'RS256');
  const issuer=createServer((req,res)=>res.writeHead(200,{'content-type':'application/json'}).end(JSON.stringify({keys:[publicJwk]})));
  const ipc=createServer(async(req,res)=>{
    const parts=[];for await(const p of req)parts.push(p);const input=JSON.parse(Buffer.concat(parts));
    const claims={iss:profile.issuer,aud:'urn:soty:source-authority',sub:context.humanPrincipal.subject,iat:now/1000,exp:now/1000+9,sourceChallenge:input.nonce,contextDigest:digest(context)};
    const jwt=await new jose.SignJWT(claims).setProtectedHeader({alg:'RS256',kid:'synthetic',typ:'soty-source-authority+jwt'}).sign(signer);
    const body=JSON.stringify({nonce:input.nonce,context,identityWitness:jwt});
    res.writeHead(200,{'content-type':'application/json','x-soty-source-mac':createHmac('sha256',key).update('authority-result\0'+body).digest('base64url')}).end(body);
  });
  for(const s of [issuer,ipc])await new Promise(resolve=>s.listen(0,'127.0.0.1',resolve));
  t.after(async()=>{for(const s of [issuer,ipc]){s.closeAllConnections();await new Promise(resolve=>s.close(resolve));}});
  const issuerOrigin=`http://127.0.0.1:${issuer.address().port}`,appId='app-'+'a'.repeat(32);
  profile=selectedResourceProfile({schema:'soty.selected-human-embed.v2',appId,connector:{linkId:'synthetic',hostDeviceId:identity.deviceId,connectorId:'synthetic-connector'},
    target:{revision:1,digest:'1'.repeat(64)},sourceProfile:sdk.STANDARD_ROOT_WORKSPACE_SOURCE,
    resource:{registryId:'soty',environmentId:'synthetic',tenantId:'root-owner',appId,resourceId:'own-device',selection:{kind:'soty.workspace.v1',nativeId:identity.deviceId,incarnationId:identity.installId}},
    issuer:issuerOrigin+'/human-identity',clientId:'native-own-device',embedOrigin:'http://localhost:4433',nativeOrigin:'http://127.0.0.1:4433',parentOrigin:issuerOrigin});
  context={schema:'soty.verified-launch-continuation.v2',reference:{id:randomBytes(32).toString('base64url'),version:1,digest:'2'.repeat(64)},
    profileDigest:profile.digest,appId,sourceProfile:profile.sourceProfile,resource:profile.resource,instanceId:'main',rootPrincipal:{accountId:'root-owner',deviceId:'root-browser'},
    humanPrincipal:{issuer:profile.issuer,subject:'root-subject',clientId:profile.clientId,clientProfileDigest:'3'.repeat(64),clientGeneration:1},
    entry:{domainId:'domain',origin:profile.embedOrigin},target:profile.target,policyEpoch:1,expiresAt:now+300000};
  const readRoot=createSourceAuthorityClient({profile,key,connectorPort:ipc.address().port,clock});
  const verified=await readRoot({reference:context.reference,connector:profile.connector});
  const binding={identity:{issuer:profile.issuer,subject:'root-subject'},rootPrincipal:context.rootPrincipal,humanPrincipal:context.humanPrincipal,resource:profile.resource,
    semanticDigest:'4'.repeat(64),operation:'root-session',sessionIdHash:'5'.repeat(64),rootReferenceDigest:digest(context.reference),rootExpiresAt:context.expiresAt,instanceId:'main'};
  const native=createOwnDeviceStatusNative({sdk,profileDigest:profile.digest,ownerAccountId:'root-owner',expectedIdentity:identity,currentIdentity:()=>current,clock,digest,
    readStatus:async()=>{calls++;return{name:'TRANSIT',version:'4.1.1',running:false,nativeState:'off',binaryPinned:true,tunReady:false};}});
  const runtime=createNativeAuthorityRuntime(native,{sourceProfile:sdk.STANDARD_ROOT_WORKSPACE_SOURCE,clock});
  await assert.rejects(runtime.captureRoot(binding,structuredClone(verified)),{code:'source_root_witness_invalid'});
  await assert.rejects(runtime.captureRoot({...binding,rootPrincipal:{accountId:'foreign',deviceId:'root-browser'}},verified));
  const proof=await runtime.captureRoot(binding,verified),selected=await runtime.call(proof,'read',{requestId:'synthetic-current',input:{operation:'soty.current_context'}});
  const value=await runtime.call(proof,'read',{requestId:'synthetic-status',contextRef:selected.context.ref,input:{operation:'transit.native.status'}});
  assert.equal(value.nativeState,'off');assert.equal(calls,1);
  await assert.rejects(runtime.call(proof,'execute',{requestId:'synthetic-write',contextRef:selected.context.ref,input:{operation:'transit.on'}}),{code:'source_app_capability_disabled'});
  current={...identity,installId:'reinstalled'};
  await assert.rejects(runtime.call(proof,'read',{requestId:'synthetic-revoked',contextRef:selected.context.ref,input:{operation:'transit.native.status'}}),{code:'native_device_status_denied'});
  assert.equal(calls,1);
  const rows=createSelectedTransitSourceApplications({appId:profile.appId,target:profile.target,resource:profile.resource,sourceProfile:profile.sourceProfile});
  assert.equal(rows.length,2);assert.equal(rows[1].catalog.effects.length,0);assert.equal(rows[1].catalog.inputSchema.properties.input.properties.operation.enum[0],'transit.native.status');
});
