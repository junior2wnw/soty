import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {randomBytes} from 'node:crypto';
import {mkdtempSync,rmSync,realpathSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,basename} from 'node:path';
import {createServer} from 'node:http';
import WebSocket from 'ws';
import {createAppsService} from '../apps/server/index.mjs';
import {migrateAppsSchema,ensureCanonicalDomain,ensureInitialPublication,runtimeTargetDigest} from '../apps/server/schema.mjs';
import {createPublicationRegistry} from '../apps/server/publications.mjs';
import {createSourceRegistry} from '../apps/server/sources.mjs';
import {createScopedAdmissionRegistry} from '../apps/scoped-embed/admissions.mjs';
import {HIVE_SELECTED_SOURCE} from '../apps/scoped-embed/resource-route-adapters.mjs';
import {createCanvasWidgetsService} from './server.mjs';

const until=async fn=>{const end=Date.now()+4000;while(!await fn()){assert.ok(Date.now()<end,'channel binding ready');await new Promise(resolve=>setTimeout(resolve,5));}};
test('actual selected renewal: a registered stale widget rejects Source-only cutover and cached ticket mint; unwired guard reproduces both',{timeout:15000},async t=>{
 const parent=realpathSync(tmpdir()),directory=mkdtempSync(join(parent,'soty-widget-renewal-')),path=join(directory,'apps.sqlite'),widgetPath=join(directory,'widgets.sqlite');
 const owner=Object.freeze({accountId:'scope_owner',deviceId:'scope_browser'}),identity={linkId:'scope_link',hostDeviceId:'scope_host',connectorId:'scope_connector'},key=[identity.linkId,identity.hostDeviceId,identity.connectorId].join('|'),appId='app-'+randomBytes(16).toString('hex'),template='https://{appId}.apps.test';let clock=1900000000000,guard=true,apps,widgets;
 const db=new DatabaseSync(path);migrateAppsSchema(db,{legacyTemplate:template,allowScopedEmbedMigration:true,allowSelectedResourceMigration:true});
 db.exec('BEGIN IMMEDIATE');db.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(key,owner.accountId,JSON.stringify(identity),'Selected Source fixture',1);db.prepare('INSERT INTO local_apps VALUES(?,?,?,?,?,?,?,?,?,?,?)').run(appId,owner.accountId,key,'Selected widget',63345,'/',JSON.stringify({accountIds:[],communityIds:[]}), 'enabled',1,1,1);const app=db.prepare('SELECT * FROM local_apps').get();ensureCanonicalDomain(db,app,template);ensureInitialPublication(db,app);db.exec('COMMIT');
 const targets=[2,3].map(revision=>{const value={appId,revision,ownerAccountId:owner.accountId,connectorKey:key,port:63343+revision,entryPath:'/embed',profile:'soty.selected-human-embed.v2'};return {...value,digest:runtimeTargetDigest(value)};});
 const profiles=targets.map(target=>({schema:target.profile,appId,connector:identity,target:{revision:target.revision,digest:target.digest},sourceProfile:{...HIVE_SELECTED_SOURCE},resource:{registryId:'soty',tenantId:owner.accountId,appId,environmentId:'production',resourceId:'selected',selection:{kind:'hive.project.v1',nativeId:'synthetic_project',incarnationId:'one'}},issuer:'https://root.test/human-identity',clientId:'scope_fixture',embedOrigin:template.replace('{appId}',appId),nativeOrigin:'https://native.test',parentOrigin:'https://root.test'}));
 const publications=createPublicationRegistry({db,now:()=>clock,assertActor:()=>{},canUse:actor=>actor.accountId===owner.accountId}),admissions=createScopedAdmissionRegistry({db,profiles,clock:()=>clock});
 // A second legitimate writer owns its constructor evidence and commits the real SourceRegistry.
 const external=createSourceRegistry({db,now:()=>clock,assertActor:()=>{},publications,scopedAdmission:admissions,prepareTarget:async({target})=>({target}),verifyPreparedTarget:({evidence,target})=>evidence.target===target});
 async function promote(){const current=publications.execute({actor:owner,op:'apps.publication.get',args:{appId}}),prepared=await external.execute({actor:owner,op:'apps.source.prepare',args:{appId,expectedPolicyEpoch:current.policyEpoch,expectedTargetRevision:current.activeTargetRevision,source:{hostDeviceId:identity.hostDeviceId,connectorId:identity.connectorId,port:current.activeTargetRevision===1?63345:63346,entryPath:'/embed',profile:'soty.selected-human-embed.v2'}}});return external.execute({actor:owner,op:'apps.source.promote',args:{appId,requestId:'source_'+prepared.target.revision,preparationId:prepared.preparationId,expectedPolicyEpoch:prepared.expectedPolicyEpoch,expectedTargetRevision:prepared.expectedTargetRevision,launchPolicy:'restricted',listed:false}});}
 await promote();
 const live=actor=>actor?.accountId===owner.accountId&&actor.deviceId===owner.deviceId;
 apps=createAppsService({databasePath:path,appOriginTemplate:template,shellOrigins:['https://root.test'],allowScopedEmbedMigration:true,allowSelectedResourceMigration:true,scopedEmbedProfiles:profiles,now:()=>clock,actorActive:live,withAuthorityFence:callback=>callback(),authenticateConnector:async()=>true,
  withHumanSubjectAuthority:({issuer,clientId},callback)=>callback({issuer,clientId,subject:'scope_subject',clientGeneration:1,clientProfileDigest:'c'.repeat(64)}),
  checkWidgetLaunch:request=>guard?widgets.checkLaunch(request):true});
 widgets=createCanvasWidgetsService({databasePath:widgetPath,projectId:'scope_tests',actorActive:live,withActorAuthorityFence:(_actor,callback)=>callback(),withAppSourceAuthority:(request,callback)=>apps.withWidgetSourceAuthority(request,callback),clock:()=>clock});
 const peers=new Set(),channels=new Set(),server=createServer((req,res)=>{if(!apps.handleRequest(req,res))res.writeHead(404).end();});server.on('connection',socket=>{peers.add(socket);socket.once('close',()=>peers.delete(socket));});server.on('upgrade',(req,socket,head)=>{if(!apps.handleUpgrade(req,socket,head))socket.destroy();});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 t.after(async()=>{for(const channel of channels)channel.terminate();widgets.close();apps.close();external.close();db.close();const stopped=new Promise(resolve=>server.close(resolve));for(const socket of peers)socket.destroy();await stopped;const actual=realpathSync(directory);assert.equal(dirname(actual).toLowerCase(),parent.toLowerCase());assert.ok(basename(actual).startsWith('soty-widget-renewal-'));rmSync(actual,{recursive:true,force:true});});
 async function channel(){const ws=new WebSocket(`ws://127.0.0.1:${server.address().port}/api/apps/channel`);channels.add(ws);ws.on('error',()=>{});let acknowledged=0;ws.on('message',raw=>{const frame=JSON.parse(raw);if(frame.type==='binding-set'){acknowledged=frame.target.revision;ws.send(JSON.stringify({type:'binding-ack',channelId:frame.channelId,syncId:frame.syncId,appId:frame.target.appId,revision:frame.target.revision,digest:frame.target.digest,profile:frame.target.profile}));}});await new Promise((resolve,reject)=>{ws.once('open',resolve);ws.once('error',reject);});ws.send(JSON.stringify({type:'auth',schema:'soty.apps-channel.v1',...identity,token:'constructor_fixture',capabilities:{targetBindingVersions:[1,2],runtimeProfiles:['soty.relay-restricted.v1','soty.selected-human-embed.v2']}}));await until(()=>acknowledged===Number(db.prepare('SELECT active_target_revision r FROM app_publications').get().r));return ws;}
 const firstChannel=await channel(),widget=widgets.execute({actor:owner,op:'canvas.widgets.create',args:{expectedAccountId:owner.accountId,requestId:'widget',appId}}).current;
 let launch;await until(()=>{try{launch=apps.execute({actor:owner,op:'apps.launch',args:{appId,...widget.entry,instanceId:widget.widgetId}});return true;}catch(error){if(['app_offline','app_binding_pending'].includes(error.code))return false;throw error;}});
 clock+=301000;
 await assert.rejects(apps.sourcePreparationExtension.executeAsync({actor:owner,op:'apps.scoped.context',args:{appId,handle:launch.scopedCloseHandle}}));
 await promote();firstChannel.terminate();await channel();
 const args={appId,handle:launch.scopedCloseHandle,requestId:'registered-widget-renewal-1'},renew=()=>apps.sourcePreparationExtension.executeAsync({actor:owner,op:'apps.scoped.renew',args});
 await assert.rejects(renew(),error=>error.code==='canvas_widget_needs_refresh');
 guard=false;const unguarded=await renew();assert.equal(unguarded.entry.domainId,widget.entry.domainId);assert.equal(typeof unguarded.launchUrl,'string','the original unguarded Source-only path mints a replacement ticket');
 guard=true;await assert.rejects(renew(),error=>error.code==='canvas_widget_needs_refresh','cached retry also needs the current registered-widget pin check');
 widgets.execute({actor:owner,op:'canvas.widgets.refresh',args:{expectedAccountId:owner.accountId,widgetId:widget.widgetId,expectedRevision:1,requestId:'explicit_confirmation'}});
 assert.equal((await renew()).scopedCloseHandle,unguarded.scopedCloseHandle,'explicit confirmation permits the same durable retry');
 widgets.execute({actor:owner,op:'canvas.widgets.remove',args:{expectedAccountId:owner.accountId,widgetId:widget.widgetId,expectedRevision:2,requestId:'remove_after_confirmation'}});await assert.rejects(renew(),error=>error.code==='canvas_widget_entry_changed');
});
