import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {randomBytes,createHmac,generateKeyPairSync} from 'node:crypto';
import {createRequire} from 'node:module';
import {pathToFileURL} from 'node:url';
import {mkdtemp,rm,mkdir,symlink} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,dirname} from 'node:path';
import * as source from '../server/index.mjs';
import {buildSourceAppPackage} from '../scripts/build.mjs';
import {createOrdinaryAppStore} from '../examples/ordinary-app/store.mjs';
import {selectedResourceProfile} from '../../apps/scoped-embed/resource-profile.mjs';
import {createResourceSourceProofSigner} from '../../apps/scoped-embed/resource-proof.mjs';
import {digest} from '../server/wire.mjs';
const require=createRequire(import.meta.url),oidcRequire=createRequire(require.resolve('openid-client'));
const {SignJWT,importJWK}=await import(pathToFileURL(oidcRequire.resolve('jose')).href);
const random=()=>randomBytes(32).toString('base64url');

// Real RSA/JWKS HTTP, connector MAC, BFF, encrypted SQLite and final Native TX.
// This issuer is synthetic; the separate Human/Apps gate tests actual Root signing.
async function fixture(t,portable=false,{crossBrand=false,sameOrigin=false}={}){
  const directory=await mkdtemp(join(tmpdir(),'root-workspace-http-'));let sdk=source;
  if(portable){await buildSourceAppPackage({outdir:directory});await mkdir(join(directory,'node_modules'));
    await symlink(dirname(dirname(require.resolve('openid-client'))),join(directory,'node_modules/openid-client'),process.platform==='win32'?'junction':'dir');
    sdk=await import(pathToFileURL(join(directory,'server.mjs')).href);}
  let now=Date.now(),mode='',bff,context,profile,rootLive=true,nativeLive=true,commits=0,rootReads=0,revokeAt=0;
  const clock=()=>now,key=randomBytes(32),pair=generateKeyPairSync('rsa',{modulusLength:2048});
  const privateJwk={...pair.privateKey.export({format:'jwk'}),kid:'synthetic',alg:'RS256',use:'sig'};
  const publicJwk={...pair.publicKey.export({format:'jwk'}),kid:'synthetic',alg:'RS256',use:'sig'},signing=await importJWK(privateJwk,'RS256');
  const issuerServer=createServer((_req,res)=>res.writeHead(200,{'content-type':'application/json'}).end(JSON.stringify({keys:[publicJwk]})));
  const ipc=createServer(async(req,res)=>{try{
    const parts=[];for await(const part of req)parts.push(part);const input=JSON.parse(Buffer.concat(parts));
    rootReads++;if(revokeAt===rootReads)nativeLive=false;
    if(!rootLive){res.writeHead(403).end();return;}
    let delivered=context,claims={iss:profile.issuer,aud:'urn:soty:source-authority',sub:context.humanPrincipal.subject,
      iat:Math.floor(now/1000),exp:Math.floor(Math.min(now+10000,context.expiresAt)/1000),sourceChallenge:input.nonce,contextDigest:digest(context)};
    if(mode==='challenge')claims.sourceChallenge=random();if(mode==='audience')claims.aud='other';if(mode==='long')claims.exp+=60;
    if(mode==='MAC-forged')delivered={...context,humanPrincipal:{...context.humanPrincipal,subject:'forged'}};
    const identityWitness=await new SignJWT(claims).setProtectedHeader({alg:'RS256',typ:'soty-source-authority+jwt',kid:'synthetic'}).sign(signing);
    const body=JSON.stringify({nonce:input.nonce,context:delivered,...(mode==='unsigned'?{}:{identityWitness})});
    res.writeHead(200,{'content-type':'application/json','x-soty-source-mac':createHmac('sha256',key).update('authority-result\0'+body).digest('base64url')}).end(body);
  }catch{res.writeHead(503).end();}});
  const server=createServer((req,res)=>void bff.handleRequest(req,res));
  t.after(async()=>{bff?.close();for(const s of [server,ipc,issuerServer]){s.closeAllConnections();if(s.listening)await new Promise(done=>s.close(done));}});
  for(const listener of [issuerServer,ipc,server])await new Promise(done=>listener.listen(0,'127.0.0.1',done));
  const origin='http://127.0.0.1:'+server.address().port,issuer='http://127.0.0.1:'+issuerServer.address().port+'/human-identity',appId='app-'+'a'.repeat(32);
  profile=selectedResourceProfile({schema:'soty.selected-human-embed.v2',appId,connector:{linkId:'fixture',hostDeviceId:'host',connectorId:'connector'},
    target:{revision:1,digest:'1'.repeat(64)},sourceProfile:sdk.STANDARD_ROOT_WORKSPACE_SOURCE,
    resource:{registryId:'soty',environmentId:'fixture',tenantId:'owner',appId,resourceId:'space',selection:{kind:'soty.workspace.v1',nativeId:'space',incarnationId:'one'}},
    issuer,clientId:'native-existing',embedOrigin:origin,nativeOrigin:sameOrigin?origin:origin.replace('127.0.0.1','localhost'),parentOrigin:new URL(issuer).origin});
  const newContext=()=>{context={schema:'soty.verified-launch-continuation.v2',reference:{id:random(),version:1,digest:randomBytes(32).toString('hex')},
    profileDigest:profile.digest,appId,sourceProfile:profile.sourceProfile,resource:profile.resource,instanceId:'main',rootPrincipal:{accountId:'root-person',deviceId:'root-device'},
    humanPrincipal:{issuer,subject:'root-subject',clientId:profile.clientId,clientProfileDigest:'2'.repeat(64),clientGeneration:1},
    entry:{domainId:'domain',origin},target:profile.target,policyEpoch:1,expiresAt:now+300000};};newContext();
  const store=createOrdinaryAppStore({databasePath:join(directory,'native.sqlite'),realmId:'fixture',key:randomBytes(32),keyId:'fixture',clock,initialize:true});
  store.createResource({id:'space',incarnationId:'one',title:'Native field'});store.createPrincipal('native-owner');
  const proofs=new WeakMap();
  function inspect(proof,binding){const held=proofs.get(proof);assert.ok(held&&nativeLive);const verified=sdk.readVerifiedRootContext(held.verified,profile.digest);
    assert.equal(digest(binding.rootPrincipal),digest(verified.rootPrincipal));assert.equal(binding.rootReferenceDigest,digest(verified.reference));
    assert.equal(verified.humanPrincipal.subject,'root-subject');assert.equal(binding.instanceId,verified.instanceId);
    if(binding.operation!=='root-session')assert.equal(store.db.prepare('SELECT active FROM source_sessions WHERE id_hash=?').get(binding.sessionIdHash)?.active,1);
    return held;}
  const native=sdk.createSourceNativeAuthorityPort({capture:async()=>{throw Error('OIDC path must not run');},
    async captureRoot(binding,verified){(crossBrand?source:sdk).readVerifiedRootContext(verified,profile.digest);assert.equal(sdk.isVerifiedRootContext(structuredClone(verified)),false);
      assert.ok(Object.isFrozen(verified.rootPrincipal));const proof={};proofs.set(proof,{verified});inspect(proof,binding);return proof;},
    withCurrent(proof,binding,action){inspect(proof,binding);return action();},
    commitRootSession(proof,binding){inspect(proof,binding);assert.equal(store.inTransaction(),true);
      const rows=store.db.prepare('SELECT * FROM source_sessions WHERE active=1 AND expires_at>? LIMIT 9').all(now);assert.ok(rows.length<=8);
      for(const row of rows){const old=store.decrypt('Session',row.id_hash,0,row.cipher,row.key_id);if(old.rootReferenceDigest===binding.rootReferenceDigest&&old.instanceId===binding.instanceId)
        return{nativeSessionHash:row.native_session_hash,principalId:'native-owner',resourceId:'space'};}
      const hash=digest(random());store.db.prepare('INSERT INTO native_sessions VALUES(?,?,1,1,?)').run(hash,'native-owner',binding.rootExpiresAt);commits++;
      return{nativeSessionHash:hash,principalId:'native-owner',resourceId:'space'};},
    currentContext(proof,binding){inspect(proof,binding);return{key:'native-field',generation:1,title:'Native field',role:'owner',revision:1,expiresAt:binding.rootExpiresAt};},
    read:async()=>({})});
  const {digest: _profileDigest,...rawProfile}=profile;
  bff=sdk.createSourceAppBff({profile:rawProfile,transportKey:key,connectorPort:ipc.address().port,storage:store.storage,native,clock});
  const signer=createResourceSourceProofSigner({profile,key,clock});let token;
  async function request(path='/api/embed/session-continue',{transport=true}={}){
    rootReads=0;
    const body=Buffer.from(JSON.stringify(path.endsWith('/query')?{requestId:'root-workspace-query',input:{operation:'soty.current_context'}}:{requestId:'root-workspace-continue'})),cookie=token?'soty_rp_session='+token:'';
    const response=await fetch(origin+path,{method:'POST',headers:{origin,'content-type':'application/json',...(cookie?{cookie}:{}),...(transport?signer.headers({context,method:'POST',path,body,cookie}):{})},body});
    const set=response.headers.get('set-cookie');if(set)token=set.split(';')[0].split('=')[1];return{status:response.status,body:await response.json(),cookie:!!set};}
  t.after(async()=>{store.close();
    await rm(directory,{recursive:true,force:true,maxRetries:5,retryDelay:40});});
  return{request,store,commits:()=>commits,clearCookie:()=>{token=undefined;},setMode:value=>{mode=value;},closeRoot:()=>{rootLive=false;},revokeNative:()=>{nativeLive=false;},
    advance:ms=>{now+=ms;},newContext,profile,rootReads:()=>rootReads,revokeOnRootRead:count=>{revokeAt=count;}};
}

for(const portable of [false,true])test('Root witness workspace short-session bootstrap/replay/new-reference '+(portable?'portable SDK':'Source'),async t=>{
  const f=await fixture(t,portable);let reply=await f.request();assert.equal(reply.status,200,reply.body.error?.code);assert.equal(reply.body.renewable,true);assert.equal(f.commits(),1);
  assert.equal((await f.request('/api/embed/query')).body.data.title,'Native field');
  const before=f.store.db.prepare('SELECT id_hash,expires_at FROM source_sessions').get();f.clearCookie();reply=await f.request();assert.equal(reply.status,200);assert.equal(f.commits(),1);
  f.advance(310000);f.newContext();reply=await f.request();assert.equal(reply.status,200);assert.equal(f.commits(),2);
  assert.equal(f.store.db.prepare('SELECT expires_at FROM source_sessions WHERE id_hash=?').get(before.id_hash).expires_at,before.expires_at);
  assert.equal(f.store.db.prepare('SELECT count(*) n FROM source_interactions').get().n,0,'no OIDC interactions or RT family');
  f.closeRoot();assert.ok((await f.request('/api/embed/query')).status>=400);
});
test('valid connector MAC cannot substitute independent Root signature/challenge/context/TTL; Native revoke denies',async t=>{
  const f=await fixture(t);for(const mode of ['unsigned','MAC-forged','challenge','audience','long']){f.setMode(mode);assert.ok((await f.request()).status>=400);assert.equal(f.commits(),0);}
  f.setMode('');f.revokeNative();assert.ok((await f.request()).status>=400);assert.equal(f.commits(),0);
});
for(const portable of [false,true])test('same existing Native origin admits only the Root-signed workspace, not cookies or forged authority '+(portable?'portable SDK':'Source'),async t=>{
  const f=await fixture(t,portable,{sameOrigin:true});
  const {digest: _profileDigest,...raw}=f.profile;
  assert.throws(()=>selectedResourceProfile({...raw,sourceProfile:source.STANDARD_WORKSPACE_SOURCE}));
  assert.throws(()=>selectedResourceProfile({...raw,sourceProfile:{...source.STANDARD_ROOT_WORKSPACE_SOURCE,digest:'0'.repeat(64)}}));
  assert.throws(()=>selectedResourceProfile({...raw,nativeOrigin:raw.nativeOrigin.replace(/:\d+$/u,':1')}));
  assert.throws(()=>selectedResourceProfile({...raw,parentOrigin:raw.embedOrigin,issuer:raw.embedOrigin+'/human-identity'}));
  for(const mode of ['unsigned','MAC-forged']){f.setMode(mode);assert.ok((await f.request()).status>=400);assert.equal(f.commits(),0);}
  f.setMode('');assert.equal((await f.request()).status,200);assert.equal(f.commits(),1);
  assert.equal((await f.request('/api/embed/query')).body.data.title,'Native field');
  assert.ok((await f.request('/api/embed/query',{transport:false})).status>=400,'ambient cookie is not Source authority');
  f.closeRoot();assert.ok((await f.request('/api/embed/query')).status>=400);
});
test('portable SDK proof cannot cross another SDK brand; HTTP creates no Native anchor or Source session',async t=>{
  const f=await fixture(t,true,{crossBrand:true});const response=await f.request();assert.ok(response.status>=400);assert.equal(f.commits(),0);
  assert.equal(f.store.db.prepare('SELECT count(*) n FROM native_sessions').get().n,0);
  assert.equal(f.store.db.prepare('SELECT count(*) n FROM source_sessions').get().n,0);
});
test('Native revoke during final Root await suppresses an otherwise successful workspace query',async t=>{
  const f=await fixture(t);assert.equal((await f.request()).status,200);
  assert.equal((await f.request('/api/embed/query')).status,200);const lastRead=f.rootReads();assert.ok(lastRead>1);
  f.revokeOnRootRead(lastRead);const response=await f.request('/api/embed/query');assert.ok(response.status>=400);
  assert.equal(response.body.data,undefined);assert.equal(f.commits(),1);
});
test('expired short anchor GC is a cookie cache miss; fresh Root creates a new finite session',async t=>{
  const f=await fixture(t);assert.equal((await f.request()).status,200);
  const old=f.store.db.prepare('SELECT id_hash,native_session_hash,expires_at FROM source_sessions').get();
  f.advance(310000);f.newContext();
  f.store.db.prepare('DELETE FROM source_sessions WHERE id_hash=?').run(old.id_hash);
  f.store.db.prepare('DELETE FROM native_sessions WHERE id_hash=?').run(old.native_session_hash);
  const response=await f.request();assert.equal(response.status,200);
  const current=f.store.db.prepare('SELECT id_hash,expires_at FROM source_sessions').get();
  assert.notEqual(current.id_hash,old.id_hash);assert.equal(current.expires_at-old.expires_at,310000);assert.equal(f.commits(),2);
  f.closeRoot();assert.ok((await f.request()).status>=400);assert.equal(f.commits(),2);
});
test('existing foreign or revoked cookie row remains denied under a fresh Root reference',async t=>{
  const f=await fixture(t);assert.equal((await f.request()).status,200);f.advance(310000);f.newContext();
  const old=f.store.db.prepare('SELECT * FROM source_sessions').get(),sealed=f.store.decrypt('Session',old.id_hash,0,old.cipher,old.key_id);
  const foreign=f.store.encrypt('Session',old.id_hash,0,{...sealed,rootPrincipal:{...sealed.rootPrincipal,deviceId:'foreign-device'}});
  f.store.db.prepare('UPDATE source_sessions SET cipher=? WHERE id_hash=?').run(foreign,old.id_hash);
  assert.equal((await f.request()).status,401);assert.equal(f.commits(),1);
  f.store.db.prepare('UPDATE source_sessions SET cipher=?,active=0 WHERE id_hash=?').run(old.cipher,old.id_hash);
  assert.equal((await f.request()).status,401);assert.equal(f.commits(),1);
});
