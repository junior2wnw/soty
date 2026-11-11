import test from 'node:test';
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import WebSocket from 'ws';
import {createAppsService} from '../server/index.mjs';
import {connectorKey} from '../server/protocol.mjs';
import {appAuthenticationCapability,appAuthenticationStatus} from '../server/authentication.mjs';

const issuer='https://root.example/human-identity';
const owner={accountId:'owner',deviceId:'owner-device'},member={accountId:'member',deviceId:'member-device'},other={accountId:'other',deviceId:'other-device'};
const identity={linkId:'test-link',hostDeviceId:'test-host',connectorId:'test-connector'};
async function fixture(t,{signer=true,issuerValue=issuer}={}){
  const directory=await mkdtemp(join(tmpdir(),'soty-authentication-status-')),databasePath=join(directory,'registry.sqlite');
  let service,revoked=false,fences=0;
  const server=createServer((_req,res)=>res.writeHead(404).end());
  server.on('upgrade',(req,socket,head)=>{if(!service.handleUpgrade(req,socket,head))socket.destroy();});
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const origin='http://127.0.0.1:'+server.address().port;
  service=createAppsService({databasePath,appOriginTemplate:`http://{appId}.localhost:${server.address().port}`,shellOrigins:[origin],
    actorActive:actor=>[owner,member,other].some(value=>value.accountId===actor?.accountId&&value.deviceId===actor?.deviceId)&&(!revoked||actor.accountId!==member.accountId),withAuthorityFence:callback=>{fences++;return callback();},
    authenticateConnector:async()=>true,relayIdentityIssuer:issuerValue===null?undefined:issuerValue,...(signer?{signRelayIdentity:async()=>{throw Error('diagnosis never signs a token');}}:{})});
  const database=new DatabaseSync(databasePath);
  database.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(connectorKey(identity),owner.accountId,JSON.stringify(identity),'Device',1);
  const call=(actor,op,args={})=>service.execute({actor,op,args:{expectedAccountId:actor.accountId,...args}});
  const registration=call(owner,'apps.register',{hostDeviceId:identity.hostDeviceId,connectorId:identity.connectorId,name:'New embedded app',port:8123,grants:{accountIds:[member.accountId]}});
  const appId=registration.app.id,sockets=[];
  t.after(async()=>{for(const socket of sockets)socket.terminate();service.close();database.close();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));await rm(directory,{recursive:true,force:true});});
  async function connector(capabilities={targetBindingVersions:[1,2],relayIdentity:true}){
    const socket=new WebSocket(origin.replace('http:','ws:')+'/api/apps/channel');sockets.push(socket);
    await new Promise((resolve,reject)=>{socket.once('open',resolve);socket.once('error',reject);});
    const ready=new Promise((resolve,reject)=>{socket.on('message',data=>{const value=JSON.parse(data);if(value.type==='ready')resolve(value);});socket.once('error',reject);});
    socket.send(JSON.stringify({type:'auth',schema:'soty.apps-channel.v1',...identity,token:'synthetic-test-only',capabilities}));
    await ready;return socket;
  }
  return {service,call,registration,appId,connector,revokeMember:()=>{revoked=true;},fences:()=>fences,
    addAppWithoutConnector(){
      const second={...identity,connectorId:'other-connector'};
      database.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(connectorKey(second),owner.accountId,JSON.stringify(second),'Other device',1);
      return call(owner,'apps.register',{hostDeviceId:second.hostDeviceId,connectorId:second.connectorId,name:'Other app',port:8124}).app.id;
    }};
}

test('public runtime capability describes the default contract without claiming browser/server verification',async t=>{
  const f=await fixture(t),capability=f.service.runtimeCapabilities({authentication:true}).authentication;
  assert.equal('authentication' in f.service.runtimeCapabilities(),false,'legacy connector capability JSON stays closed');
  assert.equal(capability.schema,'soty.app-authentication-capability.v1');
  assert.equal(capability.default,'soty-window');assert.equal(capability.contract,'soty.relay-window-identity.v1');
  assert.equal(capability.configured,true);assert.equal(capability.issuer,issuer);
  assert.equal(capability.documentation,'/agents/shared-auth.md');assert.equal(capability.sourceSdk,'/sdk/source-app/index.json');
  assert.equal('productionReady' in capability,false);
});

test('registration preserves the app and reports the exact missing shared-login dependency',async t=>{
  const f=await fixture(t),status=f.registration.authentication;
  assert.equal(status.schema,'soty.app-authentication-status.v1');assert.equal(status.appId,f.appId);
  assert.equal(status.supported,false);assert.deepEqual(status.missing,['connector_offline']);
  assert.deepEqual(status.verification,{browser:'not_checked',server:'not_checked',endToEnd:'not_checked'});
  assert.equal(f.call(owner,'apps.list').apps[0].id,f.appId,'diagnosis must not replace or disable the existing app');
});

test('signed authentication status uses current exact app authority and live connector capabilities',async t=>{
  const f=await fixture(t),read=(actor=owner,args={})=>f.call(actor,'apps.authentication.get',{appId:f.appId,...args});
  assert.throws(()=>read(other),{code:'apps_access_denied'});
  assert.throws(()=>read(owner,{expectedAccountId:other.accountId}),{code:'authentication_required'});
  assert.throws(()=>read(owner,{issuer:'https://attacker.example/human-identity'}),{code:'unexpected_argument'});
  assert.throws(()=>read(owner,{surface:'unbound'}),{code:'invalid_arguments'});
  assert.throws(()=>f.service.execute({actor:owner,op:'apps.authentication.get',args:{appId:f.appId}}),{code:'apps_authentication_account_required'});
  const before=f.fences();assert.deepEqual(read().missing,['connector_offline']);assert.ok(f.fences()>before);
  await f.connector();const status=read(member);
  assert.equal(status.supported,true);assert.deepEqual(status.missing,[]);assert.equal(status.transport,'relay');
  assert.deepEqual(status.verification,{browser:'not_checked',server:'not_checked',endToEnd:'not_checked'});
  assert.equal(status.authentication.issuer,issuer);
  const otherApp=f.addAppWithoutConnector();
  assert.deepEqual(f.call(owner,'apps.authentication.get',{appId:otherApp}).missing,['connector_offline'],'one compatible channel never qualifies another app connector');
  for(const privateField of ['connectorKey','connectorId','hostDeviceId','port','token','ownerId','accountId'])assert.equal(JSON.stringify(status).includes('"'+privateField+'"'),false);
  assert.deepEqual(read(owner,{surface:'native'}).missing,['embedded_window_required']);
  f.revokeMember();assert.throws(()=>read(member),{code:'apps_authentication_required'});
  f.call(owner,'apps.revoke',{appId:f.appId});assert.throws(()=>read(),{code:'app_unavailable'});
});

test('legacy connectors and disabled Root identity produce precise independent missing codes',async t=>{
  const f=await fixture(t,{signer:false,issuerValue:null});
  await f.connector({targetBindingVersions:[1],relayIdentity:false});
  const status=f.call(owner,'apps.authentication.get',{appId:f.appId});
  assert.equal(status.supported,false);
  assert.deepEqual(status.missing,['root_identity_signer_unavailable','root_identity_issuer_unavailable','connector_binding_v2_required','connector_relay_identity_required']);
  assert.equal(status.authentication.configured,false);assert.equal(status.authentication.issuer,null);
});

test('a selected Source or separate Native address cannot claim ordinary relay authentication',()=>{
  const capability=appAuthenticationCapability({signer:()=>{},issuer}),channel={ws:{readyState:1},bindingVersion:2,relayIdentity:true};
  const source=appAuthenticationStatus({capability,appId:'app-'+'a'.repeat(32),target:{profile:'soty.selected-human-embed.v2'},channel});
  assert.equal(source.transport,'source');assert.equal(source.supported,false);assert.deepEqual(source.missing,['source_identity_contract_required']);
  const native=appAuthenticationStatus({capability,appId:source.appId,target:{profile:'soty.relay-restricted.v1'},channel,surface:'native'});
  assert.equal(native.transport,'native');assert.equal(native.supported,false);assert.deepEqual(native.missing,['embedded_window_required']);
});

test('host authentication metadata refuses an untrusted or ambiguous issuer',()=>{
  for(const issuer of ['https://root.example','https://root.example/human-identity?other=1','https://user:pass@root.example/human-identity','http://public.example/human-identity','https://root.example/human-identity/'])
    assert.throws(()=>appAuthenticationCapability({signer:()=>{},issuer}),{code:'app_identity_configuration_invalid'});
  assert.equal(appAuthenticationCapability({issuer:'http://127.0.0.1:5111/human-identity'}).configured,false);
});
