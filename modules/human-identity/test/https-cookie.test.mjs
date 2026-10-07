import test from 'node:test';
import assert from 'node:assert/strict';
import express from 'express';
import {createServer,request as httpsRequest} from 'node:https';
import {randomBytes,generateKeyPairSync,createPrivateKey,X509Certificate} from 'node:crypto';
import {spawnSync} from 'node:child_process';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,resolve,basename} from 'node:path';
import * as oidc from 'openid-client';
import {createClientWithStorage} from '../../connect/browser/client.mjs';
import {attachConnectModule} from '../../../server/connect-module.js';
import {createHumanIdentityHostProfile} from '../profile.mjs';
import {createHumanIdentityService} from '../service.mjs';
import {attachHumanIdentity} from '../../../server/human-identity.js';

const random=()=>randomBytes(32).toString('base64url');
function tlsKeys(t){
  const executable=process.env.SOTY_TEST_OPENSSL||(process.platform==='win32'?'C:/Program Files/Git/usr/bin/openssl.exe':'openssl');
  const probe=spawnSync(executable,['version'],{encoding:'utf8',timeout:5000,windowsHide:true});
  if(probe.status!==0){t.skip('Installed OpenSSL required for real HTTPS certificate fixture');return null;}
  const result=spawnSync(executable,['req','-x509','-newkey','rsa:2048','-nodes','-keyout','-','-out','-','-days','1','-subj','/CN=localhost',
    '-addext','subjectAltName=DNS:localhost,IP:127.0.0.1','-addext','basicConstraints=critical,CA:TRUE'],{encoding:'utf8',timeout:15000,maxBuffer:262144,windowsHide:true});
  assert.equal(result.status,0,'fixture certificate generation');
  const key=/-----BEGIN PRIVATE KEY-----[\s\S]+?-----END PRIVATE KEY-----/u.exec(result.stdout)?.[0],cert=/-----BEGIN CERTIFICATE-----[\s\S]+?-----END CERTIFICATE-----/u.exec(result.stdout)?.[0];
  assert.equal(createPrivateKey(key).asymmetricKeyType,'rsa');assert.ok(new X509Certificate(cert));return {key,cert};
}
function cookieBrowser(){
  const jar=new Map(),rejected=[],accepted=[];
  function receive(url,raw){
    const [pair,...attributes]=raw.split(';'),equal=pair.indexOf('='),name=pair.slice(0,equal);
    const attrs=Object.fromEntries(attributes.map(part=>{const index=part.indexOf('=');return index<0?[part.trim().toLowerCase(),true]:[part.slice(0,index).trim().toLowerCase(),part.slice(index+1).trim()];}));
    const secure=attrs.secure===true,path=attrs.path||'/';
    // Independent browser prefix acceptance: the old server passed HTTP jars,
    // but its __Host resume with a scoped Path was rejected by actual Chrome.
    if((name.startsWith('__Secure-')||name.startsWith('__Host-'))&&(!secure||url.protocol!=='https:')
      ||name.startsWith('__Host-')&&(path!=='/'||attrs.domain!==undefined)){rejected.push(name);return false;}
    assert.equal(attrs.domain,undefined,'all issuer cookies are host-only');
    const entry={name,value:pair.slice(equal+1),host:url.hostname,path,secure,httpOnly:attrs.httponly===true,sameSite:attrs.samesite};
    const key=JSON.stringify([entry.host,path,name]);if(!entry.value||attrs['max-age']==='0')jar.delete(key);else jar.set(key,entry);
    accepted.push({name,path,secure,httpOnly:entry.httpOnly,sameSite:entry.sameSite});return true;
  }
  function send(url){return [...jar.values()].filter(value=>value.host===url.hostname&&(!value.secure||url.protocol==='https:')
    &&(url.pathname===value.path||url.pathname.startsWith(value.path.endsWith('/')?value.path:value.path+'/'))).sort((a,b)=>b.path.length-a.path.length)
    .map(value=>value.name+'='+value.value).join('; ');}
  return {receive,send,rejected,accepted};
}
function trustedFetch(tls){return async(input,options={})=>{
  const url=new URL(input);assert.equal(url.hostname,'127.0.0.1','only owned HTTPS loopback');assert.equal(url.protocol,'https:');
  let body=options.body??undefined;if(body instanceof URLSearchParams)body=body.toString();if(body!==undefined&&typeof body!=='string'&&!(body instanceof Uint8Array))throw new Error('fixture_body_type');
  const headers=new Headers(options.headers);if(body!==undefined)headers.set('content-length',String(Buffer.byteLength(body)));
  return new Promise((done,reject)=>{const request=httpsRequest(url,{method:options.method||'GET',ca:tls.cert,headers:Object.fromEntries(headers),signal:options.signal||AbortSignal.timeout(10000)},response=>{
    const pieces=[];let size=0;response.on('data',part=>{size+=part.length;if(size>65536)response.destroy();else pieces.push(part);});response.on('error',reject);
    response.on('end',()=>{const responseHeaders=new Headers();for(let index=0;index<response.rawHeaders.length;index+=2)responseHeaders.append(response.rawHeaders[index],response.rawHeaders[index+1]);
      done(new Response(Buffer.concat(pieces),{status:response.statusCode,headers:responseHeaders}));});});request.on('error',reject);request.end(body);});
};}
const memory=()=>{let value;return{async read(){return structuredClone(value??null);},async claim(next){value??=structuredClone(next);return structuredClone(value);},async compareAndSwap(revision,next){assert.equal(value.localRevision,revision);value=structuredClone(next);return structuredClone(value);}};};

test('real HTTPS issuer respects browser cookie prefixes and reaches only the approved Native callback using Code/PKCE and signed current Connect proof',async t=>{
  const tls=tlsKeys(t);if(!tls)return;
  const directory=mkdtempSync(join(tmpdir(),'soty-human-https-')),browser=cookieBrowser(),fetcher=trustedFetch(tls);let app,service,connect,client;
  const server=createServer(tls,(req,res)=>app?app(req,res):res.writeHead(503).end());await new Promise(done=>server.listen(0,'127.0.0.1',done));
  const origin=`https://127.0.0.1:${server.address().port}`,issuer=origin+'/human-identity',clientId='native.https.fixture',clientSecret=random();
  let callbackCount=0,callbackFailure='none',callbackPhase='entry',configuration,start;
  const native=createServer(tls,async(req,res)=>{try{
    if(req.url.split('?')[0]!=='/account/soty/callback'){res.writeHead(404).end();return;}
    const current=new URL(req.url,nativeOrigin);assert.equal(current.searchParams.get('iss'),issuer);
    callbackPhase='exchange';const tokens=await oidc.authorizationCodeGrant(configuration,current,{pkceCodeVerifier:start.verifier,expectedState:start.state,expectedNonce:start.nonce});
    callbackPhase='claims';assert.equal(tokens.claims().iss,issuer);assert.equal(tokens.claims().aud,clientId);callbackPhase='userinfo';assert.equal((await oidc.fetchUserInfo(configuration,tokens.access_token,tokens.claims().sub)).sub,tokens.claims().sub);
    callbackCount++;res.writeHead(200,{'content-type':'application/json'}).end('{"approvedNativeCallback":true}');
  }catch(error){callbackFailure=typeof error?.code==='string'&&/^(?:OAUTH|ERR)_[A-Z_]{1,64}$/u.test(error.code)?error.code:
    ['invalid_grant','invalid_client','temporarily_unavailable','server_error','access_denied'].includes(error?.error)?error.error:['TypeError','ResponseBodyError','ClientError'].includes(error?.constructor?.name)?error.constructor.name:'unclassified';res.writeHead(403).end();}});await new Promise(done=>native.listen(0,'127.0.0.1',done));const nativeOrigin=`https://127.0.0.1:${native.address().port}`,redirectUri=nativeOrigin+'/account/soty/callback';
  t.after(async()=>{client?.dispose();server.closeAllConnections();native.closeAllConnections();await Promise.all([new Promise(done=>server.close(done)),new Promise(done=>native.close(done))]);
    service?.close();connect?.close();assert.equal(dirname(resolve(directory)),resolve(tmpdir()));assert.match(basename(directory),/^soty-human-https-/u);rmSync(directory,{recursive:true,force:true,maxRetries:5,retryDelay:100});});
  const privateJwk=generateKeyPairSync('rsa',{modulusLength:2048}).privateKey.export({format:'jwk'});Object.assign(privateJwk,{kid:'https-fixture',alg:'RS256',use:'sig'});
  const profile=createHumanIdentityHostProfile({enabled:true,issuer,registryId:'REG.soty',environmentId:'fixture',clients:[{id:clientId,label:'Approved Native HTTPS',redirectUri,clientSecret}],
    jwks:{keys:[privateJwk]},cookieKeys:[random()],artifactKey:randomBytes(32),artifactKeyId:'fixture'}, {shellOrigins:[origin]});
  app=express();service=createHumanIdentityService({databasePath:join(directory,'human-identity','identity.sqlite'),profile,actorActive:actor=>connect.isActorActive(actor),withAuthorityFence:callback=>connect.withAuthorityFence(callback),readProfile:()=>({})});
  connect=attachConnectModule(app,{dataDir:directory,origins:[origin],extensions:[service]});attachHumanIdentity(app,{profile,service});
  client=createClientWithStorage({projectId:'soty',endpoint:origin+'/api/connect/rpc',fetch:(url,options)=>fetcher(url,{...options,headers:{...options.headers,origin}})},memory());const actor=await client.bootstrap('Synthetic current HTTPS actor');
  const prefixInvariant=cookieBrowser();assert.equal(prefixInvariant.receive(new URL(origin),'__Host-old_resume=synthetic;Secure;HttpOnly;Path=/human-identity/authorize/synthetic'),false,'original secure scoped cookie is rejected');
  configuration=await oidc.discovery(new URL(issuer),clientId,clientSecret,oidc.ClientSecretBasic(clientSecret),{[oidc.customFetch]:fetcher});
  start={state:oidc.randomState(),nonce:oidc.randomNonce(),verifier:oidc.randomPKCECodeVerifier()};
  const parameters={redirect_uri:redirectUri,response_type:'code',scope:'openid',state:start.state,nonce:start.nonce,code_challenge:await oidc.calculatePKCECodeChallenge(start.verifier),code_challenge_method:'S256'};
  async function request(input,{fields}={}){const url=new URL(input),response=await fetcher(url,{...(fields?{method:'POST',body:new URLSearchParams(fields)}:{}),
    headers:{accept:'text/html',cookie:browser.send(url),...(fields?{origin:url.origin,'sec-fetch-site':'same-origin','content-type':'application/x-www-form-urlencoded'}:{})}});
    for(const raw of response.headers.getSetCookie())browser.receive(url,raw);const text=await response.text();return {status:response.status,location:response.headers.get('location')?new URL(response.headers.get('location'),url):null,body:response.headers.get('content-type')?.includes('application/json')?JSON.parse(text):null};}
  const wrong=await request(oidc.buildAuthorizationUrl(configuration,{...parameters,redirect_uri:nativeOrigin+'/account/soty/unapproved'}));assert.equal(wrong.status,400);assert.equal(callbackCount,0);
  const authorized=await request(oidc.buildAuthorizationUrl(configuration,parameters));assert.ok([302,303].includes(authorized.status));assert.equal(authorized.location.origin,origin);
  await request(authorized.location);const context=(await request(authorized.location.href+'/context')).body;
  await client.extension('identity.human.approve',{interactionId:context.interactionId,browserNonce:context.browserNonce,csrf:context.csrf,requestId:'https-native-approved',decision:'approve',expectedAccountId:actor.accountId},{expectedAccountId:actor.accountId});
  const completed=await request(authorized.location.href+'/complete',{fields:{csrf:context.csrf}});assert.equal(completed.status,303);
  const resumed=await request(completed.location);assert.ok([302,303].includes(resumed.status),'browser-accepted scoped resume reaches Native HTTPS callback');assert.equal(resumed.location.origin+resumed.location.pathname,redirectUri);
  assert.equal((await request(resumed.location)).status,200,'actual Native maintained SDK callback, safe stage='+callbackPhase+' enum='+callbackFailure);assert.equal(callbackCount,1);
  assert.deepEqual(browser.rejected,[]);const resume=browser.accepted.find(value=>value.name==='__Secure-soty_human_resume');assert.ok(resume);assert.ok(resume.path.startsWith('/human-identity/authorize/'));assert.equal(resume.secure,true);assert.equal(resume.httpOnly,true);assert.equal(resume.sameSite.toLowerCase(),'lax');
  for(const name of ['__Host-soty_human_interaction','__Host-soty_human_browser']){const cookie=browser.accepted.find(value=>value.name===name);assert.ok(cookie);assert.equal(cookie.path,'/');assert.equal(cookie.secure,true);assert.equal(cookie.httpOnly,true);}
});
