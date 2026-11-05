import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {DatabaseSync} from 'node:sqlite';
import {fileURLToPath} from 'node:url';
import {join} from 'node:path';
import {createOrdinaryInstalledFixture} from '../../source-app/test/support/ordinary-installed.mjs';
import {STANDARD_ROOT_WORKSPACE_SOURCE,STANDARD_WORKSPACE_SOURCE} from '../../source-app/server/standard-profile.mjs';
import {createRootWorkspaceServer,createSqliteRootWorkspaceHost} from '../../source-app/server/index.mjs';
import {createDomain} from '../../source-app/examples/reading-list/domain.mjs';
import {createSourceWorkspaceClient} from '../../source-app/browser/index.mjs';

async function fixture(t){return createOrdinaryInstalledFixture(t,{workspace:true,sourceProfile:STANDARD_ROOT_WORKSPACE_SOURCE,realmIds:['board'],withoutRpClients:['board'],
 connectorScript:fileURLToPath(new URL('../../../scripts/soty-connector.mjs',import.meta.url)),
 nativeFactory({realm,initialize}){
  const host=createSqliteRootWorkspaceHost({profile:realm.profile,databasePath:realm.databasePath,cipherKey:realm.cipherKey,keyId:'frame-'+realm.realmId,initialize,
   personalWorkspaces:true,title:'Reading fixture',domain:createDomain()});
  let pause,started;
  const app=createRootWorkspaceServer({profile:realm.profile,transportKey:realm.key,connectorPort:realm.options.connectorPort,host,
   listen:{host:'127.0.0.1',port:realm.port},ui:{document:'<!doctype html><title>Native</title>',script:';',style:'body{margin:0}',sdk:';'}});
  app.server.prependListener('request',(req,res)=>{if(req.url!=='/api/embed/session-continue'||!pause)return;const end=res.end.bind(res);res.end=(chunk,...rest)=>{
   let body;try{body=JSON.parse(String(chunk));}catch{}if(res.statusCode===200&&body?.ready===true&&pause){const waiting=pause;pause=null;started?.();void waiting.then(()=>end(chunk,...rest));return res;}return end(chunk,...rest);};});
  return{...app,pauseReady(){let release;pause=new Promise(done=>{release=done;});const waiting=new Promise(done=>{started=done;});return{waiting,release};}};
 }
 });}
async function boot(f,r,launch){const candidate=await f.wire(r.embedded+'/_soty/session',{body:{ticket:new URL(launch.launchUrl).hash.slice(1)}});assert.equal(candidate.status,200,'real cookie carrier admitted');
 const headers={'x-soty-frame':candidate.value.frame,...(candidate.value.renewal?{'x-soty-boot-check':candidate.value.sessionCheck}:{})};
 const continued=await f.wire(r.embedded+'/api/embed/session-continue',{body:{requestId:candidate.value.scopedRequestId},headers});return{candidate,continued,headers};}
async function initial(f,r,extra={},signer=f.reader){const launch=await signer.client.extension('apps.launch',{appId:r.appId,...extra}),opened=await boot(f,r,launch);assert.equal(opened.continued.status,200);assert.equal(opened.continued.value.ready,true);return{launch,...opened};}
test('actual signed Root restart reauthorizes same frame; foreign actor, explicit close, stale cleanup and late private ACK remain denied',{timeout:120000},async t=>{
 const f=await fixture(t),r=f.realms[0],invite='i'.repeat(43),one=await initial(f,r,{path:'/embed#document=pag_frame&invite='+invite});assert.equal(typeof one.launch.scopedFrameId,'string');
 const db=new DatabaseSync(join(f.directory,'root','apps','registry.sqlite'),{readOnly:true});try{assert.equal(JSON.stringify(db.prepare('SELECT * FROM app_frame_continuity').all()).includes(invite),false,'real Root persists no Source invitation bearer');}finally{db.close();}
 await f.restartRoot();const args={appId:r.appId,handle:one.launch.scopedCloseHandle,frameId:one.launch.scopedFrameId,currentEntryPath:one.launch.entry.path,requestId:randomUUID()};
 await assert.rejects(f.foreign.client.extension('apps.scoped.renew',args));
 const{currentEntryPath:_path,...missingPath}=args;
 await assert.rejects(f.reader.client.extension('apps.scoped.renew',missingPath));
 await assert.rejects(f.reader.client.extension('apps.scoped.renew',{...args,currentEntryPath:'/embed'}));
 const next=await f.reader.client.extension('apps.scoped.renew',args);assert.ok(next.scopedFrameId===one.launch.scopedFrameId);
 await assert.rejects(f.reader.client.extension('apps.scoped.renew',{...args,currentEntryPath:'/embed'}),{code:'app_scoped_renew_conflict'});
 const opened=await boot(f,r,next);assert.equal(opened.continued.status,200);assert.equal(opened.continued.value.ready,true);
 await f.reader.client.extension('apps.scoped.close',{appId:r.appId,handle:one.launch.scopedCloseHandle});
 assert.equal((await f.wire(r.embedded+'/embed',{headers:{'x-soty-frame':one.launch.scopedFrameId}})).status,200,'old cleanup cannot close current Native frame');
 await f.reader.client.extension('apps.scoped.close',{appId:r.appId,handle:next.scopedCloseHandle});await f.restartRoot();
 await assert.rejects(f.reader.client.extension('apps.scoped.renew',{...args,handle:next.scopedCloseHandle,requestId:randomUUID()}));
 const two=await initial(f,r),renewed=await f.reader.client.extension('apps.scoped.renew',{appId:r.appId,handle:two.launch.scopedCloseHandle,frameId:two.launch.scopedFrameId,currentEntryPath:two.launch.entry.path,requestId:randomUUID()});
 const paused=r.instance.pauseReady(),pending=boot(f,r,renewed).catch(error=>({continued:{status:0,errorCode:error.code}}));await paused.waiting;
 await f.reader.client.extension('apps.scoped.close',{appId:r.appId,handle:two.launch.scopedCloseHandle});paused.release();const late=await pending;
 assert.notEqual(late.continued.status,200,'late Source ACK cannot revive explicitly closed family');
 await assert.rejects(f.reader.client.extension('apps.scoped.context',{appId:r.appId,handle:renewed.scopedCloseHandle}));
});
test('actual actor/device and app revocation do not become restart authorization',{timeout:120000},async t=>{
 const f=await fixture(t),r=f.realms[0],one=await initial(f,r);await f.owner.client.extension('apps.update',{appId:r.appId,grants:{accountIds:[],communityIds:[]}});await f.restartRoot();
 await assert.rejects(f.reader.client.extension('apps.scoped.renew',{appId:r.appId,handle:one.launch.scopedCloseHandle,frameId:one.launch.scopedFrameId,currentEntryPath:one.launch.entry.path,requestId:randomUUID()}));
 await f.grantBrowser(f.reader.account.accountId);const two=await initial(f,r);const oldDevice=f.reader.account.deviceId;
 const recovery=await f.reader.client.prepareRecovery();await f.reader.client.confirmRecovery(recovery);await f.reader.client.revokeDevice(oldDevice);await f.restartRoot();
 await assert.rejects(f.reader.client.extension('apps.scoped.renew',{appId:r.appId,handle:two.launch.scopedCloseHandle,frameId:two.launch.scopedFrameId,currentEntryPath:two.launch.entry.path,requestId:randomUUID()}));
});

test('two actual recovery carriers cannot both promote the same previous lease',{timeout:120000},async t=>{
 const f=await fixture(t),r=f.realms[0],one=await initial(f,r);await f.restartRoot();
 const args={appId:r.appId,handle:one.launch.scopedCloseHandle,frameId:one.launch.scopedFrameId,currentEntryPath:one.launch.entry.path};
 const a=await f.reader.client.extension('apps.scoped.renew',{...args,requestId:randomUUID()}),b=await f.reader.client.extension('apps.scoped.renew',{...args,requestId:randomUUID()});
 assert.equal((await boot(f,r,a)).continued.status,200);
 const stale=await f.wire(r.embedded+'/_soty/session',{body:{ticket:new URL(b.launchUrl).hash.slice(1)}});
 assert.notEqual(stale.status,200,'the second stale candidate cannot replace the promoted lease');
 assert.equal((await f.wire(r.embedded+'/embed',{headers:{'x-soty-frame':one.launch.scopedFrameId}})).status,200);
 await f.restartRoot();await assert.rejects(f.reader.client.extension('apps.scoped.renew',{...args,requestId:randomUUID()}),'a replaced handle cannot claim the current lease after another host restart');
 const current=await f.reader.client.extension('apps.scoped.renew',{...args,handle:a.scopedCloseHandle,requestId:randomUUID()});assert.equal((await boot(f,r,current)).continued.status,200);
});

test('changed operator resource, Source pin and an explicitly promoted old target cannot recover the previous exact Source',{timeout:120000},async t=>{
 const f=await fixture(t),r=f.realms[0],one=await initial(f,r),previous=r.profile.resource.resourceId;
 r.profile.resource.resourceId=previous+':new';await f.restartRoot({awaitReady:false});
 await assert.rejects(f.reader.client.extension('apps.scoped.renew',{appId:r.appId,handle:one.launch.scopedCloseHandle,frameId:one.launch.scopedFrameId,currentEntryPath:one.launch.entry.path,requestId:randomUUID()}));
 r.profile.resource.resourceId=previous;await f.restartRoot();const two=await initial(f,r);
 r.profile.sourceProfile=STANDARD_WORKSPACE_SOURCE;await f.restartRoot({awaitReady:false});
 await assert.rejects(f.reader.client.extension('apps.scoped.renew',{appId:r.appId,handle:two.launch.scopedCloseHandle,frameId:two.launch.scopedFrameId,currentEntryPath:two.launch.entry.path,requestId:randomUUID()}));
 r.profile.sourceProfile=STANDARD_ROOT_WORKSPACE_SOURCE;await f.restartRoot();const three=await initial(f,r);
 const pub=await f.owner.client.extension('apps.publication.get',{appId:r.appId});
 const prepared=await f.owner.client.extension('apps.source.prepare',{appId:r.appId,expectedPolicyEpoch:pub.policyEpoch,expectedTargetRevision:pub.activeTargetRevision,targetRevision:1});
 await f.owner.client.extension('apps.source.promote',{appId:r.appId,requestId:'frame-target-rollback-001',expectedPolicyEpoch:pub.policyEpoch,expectedTargetRevision:pub.activeTargetRevision,preparationId:prepared.preparationId,launchPolicy:'restricted',listed:false});
 await f.restartRoot();await assert.rejects(f.reader.client.extension('apps.scoped.renew',{appId:r.appId,handle:three.launch.scopedCloseHandle,frameId:three.launch.scopedFrameId,currentEntryPath:three.launch.entry.path,requestId:randomUUID()}));
});

test('actual password placement barrier denies recovery but keeps independent main app usable',{timeout:120000},async t=>{
 const f=await fixture(t),r=f.realms[0],scope={expectedAccountId:f.owner.account.accountId},widget=(await f.owner.client.extension('canvas.widgets.create',{...scope,requestId:'frame-password-widget-001',appId:r.appId})).current;
 const object=(await f.owner.client.extension('canvas.objects.create',{...scope,requestId:'frame-password-object-001',binding:{providerId:'soty.apps',ref:widget.widgetId},presentation:{mask:{shape:'rect'}},
  visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'none'}})).current;
 const one=await initial(f,r,{...widget.entry,instanceId:widget.widgetId},f.owner);
 await f.owner.client.extension('canvas.vault.protect',{...scope,requestId:'frame-password-protect-001',objectId:object.objectId,expectedObjectRevision:object.revision,password:'Isolated-test-password-781'});
 await f.restartRoot();await assert.rejects(f.owner.client.extension('apps.scoped.renew',{appId:r.appId,handle:one.launch.scopedCloseHandle,frameId:one.launch.scopedFrameId,currentEntryPath:one.launch.entry.path,requestId:randomUUID()}));
 assert.equal((await initial(f,r,{},f.owner)).continued.status,200,'password controls this placement, not unrelated main Source');
});

test('lost original mutation acknowledgement stays receipt recovery only across Root restart',{timeout:120000},async t=>{
 const f=await fixture(t),r=f.realms[0],one=await initial(f,r),paths=[];
 const client=createSourceWorkspaceClient({href:r.embedded+'/embed?sotyFrame='+one.launch.scopedFrameId,
  fetch:async(path,init)=>{paths.push(path);const reply=await f.wire(r.embedded+path,{method:init.method,body:JSON.parse(init.body),headers:Object.fromEntries(new Headers(init.headers))});
   return new Response(reply.text,{status:reply.status,headers:reply.headers});}});t.after(()=>client.dispose());
 await client.connect({requestId:'frame-receipt-connect-001'});
 const intent=await client.prepareInvoke({requestId:'frame-receipt-write-001',input:{operation:'reading.add',title:'One durable item'}});
 r.dropPath='/api/embed/invoke';await assert.rejects(client.invoke(intent),e=>e.effectUnknown===true);
 assert.equal(paths.filter(p=>p==='/api/embed/invoke').length,1);
 await f.restartRoot();const next=await f.reader.client.extension('apps.scoped.renew',{appId:r.appId,handle:one.launch.scopedCloseHandle,frameId:one.launch.scopedFrameId,currentEntryPath:one.launch.entry.path,requestId:randomUUID()});
 assert.equal((await boot(f,r,next)).continued.status,200);await client.connect({requestId:'frame-receipt-current-001'});
 await assert.rejects(client.invoke(intent),e=>e.effectUnknown===true,'old unknown intent is never automatically invoked under a fresh Native context');
 assert.equal((await client.recoverReceipt(intent)).outcome,'committed');
 assert.deepEqual((await client.query({requestId:'frame-receipt-read-001',input:{operation:'reading.list'}})).items,[{id:intent.requestId,title:'One durable item'}]);
 assert.equal(paths.filter(p=>p==='/api/embed/invoke').length,1,'restart recovery does not replay the original mutation');
 const db=new DatabaseSync(r.databasePath,{readOnly:true});try{assert.equal(db.prepare('SELECT count(*) n FROM soty_workspace_documents').get().n,1);assert.equal(db.prepare('SELECT count(*) n FROM soty_workspace_receipts').get().n,1);}finally{db.close();}
});
