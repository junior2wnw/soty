import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync,realpathSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,basename} from 'node:path';
import {DatabaseSync} from 'node:sqlite';
import {createAppsService} from '../server/index.mjs';
import {connectorKey} from '../server/protocol.mjs';

test('known legacy canvas uses current public named Source including unlisted; private/retired/inactive alias never falls back to catalog or owner',{timeout:15000},t=>{
 const parent=realpathSync(tmpdir()),folder=mkdtempSync(join(parent,'soty-public-canvas-source-')),databasePath=join(folder,'apps.sqlite'),owner={accountId:'canvas_app_owner',deviceId:'canvas_app_owner_device'},viewer={accountId:'canvas_app_viewer',deviceId:'canvas_app_viewer_device'},outsider={accountId:'canvas_app_outsider',deviceId:'canvas_app_outside_device'};let actorChecks=0,serial=0;
 const service=createAppsService({databasePath,appOriginTemplate:'https://{appId}.legacy.example',namedAppZone:'https://named.example',shellOrigins:['https://root.example'],actorActive:actor=>{actorChecks++;return [owner,viewer,outsider].some(value=>value.accountId===actor?.accountId&&value.deviceId===actor?.deviceId);},withAuthorityFence:callback=>callback()}),db=new DatabaseSync(databasePath),identity={linkId:'canvas_link',hostDeviceId:'canvas_host',connectorId:'canvas_connector'};
 db.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(connectorKey(identity),owner.accountId,JSON.stringify(identity),'Canvas test source',1);
 t.after(()=>{db.close();service.close();const actual=realpathSync(folder);assert.equal(dirname(actual).toLowerCase(),parent.toLowerCase());assert.ok(basename(actual).startsWith('soty-public-canvas-source-'));rmSync(actual,{recursive:true,force:true});});
 const call=(actor,op,args={})=>service.execute({actor,op:'apps.'+op,args}),appId=call(owner,'register',{hostDeviceId:identity.hostDeviceId,connectorId:identity.connectorId,name:'Real current app Source',port:30222,entryPath:'/source/start'}).app.id;
 const alias=slug=>call(owner,'domains.claim',{appId,slug,requestId:'alias_'+slug,expectedDomainsRevision:call(owner,'domains.get',{appId}).revision}).receipt,first=alias('canvas-first'),second=alias('canvas-second');
 const read=actor=>service.withPublicCanvasAuthority({appId,...(actor?{actor}:{})},snapshot=>snapshot);
 assert.equal(read(owner).entry.path,'/source/start');assert.throws(()=>read(),{code:'apps_access_denied'});assert.throws(()=>read(viewer),{code:'apps_access_denied'});
 call(owner,'update',{appId,grants:{accountIds:[viewer.accountId],communityIds:[]}});assert.equal(read(viewer).runtimeAllowed,true);assert.throws(()=>read(outsider),{code:'apps_access_denied'});
 function publish(domainId,listed=false){const current=call(owner,'publication.get',{appId});return call(owner,'publication.update',{appId,requestId:'publish_canvas_'+(++serial),expectedPolicyEpoch:current.policyEpoch,expectedTargetRevision:current.activeTargetRevision,launchPolicy:'anyone',listed,activeDomainIds:[domainId],exposureAck:{scope:'whole-port',targetRevision:current.target.revision,targetDigest:current.target.digest,profile:current.target.profile}});}
 publish(first.domainId);const checks=actorChecks,snapshot=read();assert.equal(actorChecks,checks,'anonymous snapshot never impersonates the owner');assert.equal(snapshot.entry.domainId,first.domainId);assert.equal(snapshot.entry.path,'/source/start');assert.equal(snapshot.runtimeAllowed,false);assert.equal(snapshot.app.name,'Real current app Source');assert.match(snapshot.target.digest,/^[a-f0-9]{64}$/u);
 assert.throws(()=>service.withPublicAppAuthority({appId},()=>true),{code:'apps_access_denied'},'known unlisted access must not expand listed agent discovery');assert.equal(call(viewer,'catalog').apps.length,0);
 assert.doesNotMatch(JSON.stringify(snapshot),/connectorKey|hostDeviceId|launchUrl|ticket|grants|"port"/u);assert.throws(()=>service.withPublicCanvasAuthority({appId,publicCanvasDefault:true},()=>true),{code:'invalid_arguments'});assert.throws(()=>service.withPublicCanvasAuthority({appId,domainId:second.domainId},()=>true),{code:'invalid_arguments'});
 call(owner,'domains.retire',{appId,domainId:first.domainId,requestId:'retire_canvas_active',expectedDomainsRevision:call(owner,'domains.get',{appId}).revision});assert.throws(()=>read(),{code:'apps_access_denied'},'bound but inactive second alias cannot rescue a retired publication');
 publish(second.domainId);assert.equal(read().entry.domainId,second.domainId);publish(second.domainId,true);assert.equal(read().entry.domainId,second.domainId);assert.ok(call(viewer,'catalog').apps.some(value=>value.id===appId));
 call(owner,'revoke',{appId});assert.throws(()=>read(),{code:'app_unavailable'});
});
