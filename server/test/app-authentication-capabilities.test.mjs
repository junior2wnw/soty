import test from 'node:test';
import assert from 'node:assert/strict';
import {generateKeyPairSync,randomBytes} from 'node:crypto';
import {createHttpApp} from '../http-app.js';
import {nativeHttpFixture} from './support/native-capability-http.mjs';

for(const enabled of [false,true])test(`public authentication capability reports the actual Root signer (${enabled}) without changing legacy connector JSON`,async t=>{
  const key=generateKeyPairSync('rsa',{modulusLength:2048}).privateKey.export({format:'jwk'});
  const f=await nativeHttpFixture(t,{appFactory:(distDir,options)=>{
    const origin=options.connectOrigins[0];
    return createHttpApp(distDir,{...options,...(enabled?{humanIdentity:{enabled:true,issuer:origin+'/human-identity',registryId:'soty',environmentId:'production',clients:[],
      jwks:{keys:[{...key,kid:'test-key',alg:'RS256',use:'sig'}]},cookieKeys:[randomBytes(32).toString('base64url')],artifactKey:randomBytes(32),artifactKeyId:'test'}}:{})});
  }});
  const response=await fetch(f.origin+'/api/apps/capabilities'),full=await response.json();assert.equal(response.status,200);
  assert.equal(response.headers.get('cache-control'),'no-store');assert.equal(full.humanIdentityConfigured,enabled);
  assert.equal(full.authentication.schema,'soty.app-authentication-capability.v1');assert.equal(full.authentication.configured,enabled);
  assert.equal(full.authentication.issuer,enabled?f.origin+'/human-identity':null);
  assert.equal(full.authentication.default,'soty-window');assert.equal(full.authentication.contract,'soty.relay-window-identity.v1');
  const runtime=await fetch(f.origin+'/api/apps/runtime-capabilities'),legacy=await runtime.json();
  assert.deepEqual(Object.keys(legacy).sort(),['channelSchema','runtimeProfiles','schema','targetBindingVersions'],'old strict connector keeps its original closed response');
  const opt=await fetch(f.origin+'/api/apps/runtime-capabilities?authentication=1'),current=await opt.json();
  assert.equal(opt.status,200);assert.equal(opt.headers.get('cache-control'),'no-store');
  assert.deepEqual(current.authentication,full.authentication);delete current.authentication;assert.deepEqual(current,legacy);
  for(const name of ['jwks','cookieKeys','artifactKey','token'])assert.equal(JSON.stringify(full.authentication).includes(name),false);
});
