import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {mkdtempSync,rmSync,realpathSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,basename} from 'node:path';
import {createAppsService} from '../apps/server/index.mjs';
import {connectorKey} from '../apps/server/protocol.mjs';
import {createCanvasObjectsService} from '../canvas-objects/server.mjs';
import {createCanvasWidgetsService,createCanvasWidgetProvider} from './server.mjs';

const alice={accountId:'alice',deviceId:'alice_browser'},bob={accountId:'bob',deviceId:'bob_browser'},carol={accountId:'carol',deviceId:'carol_browser'};
const code=value=>error=>{assert.equal(error.code,value);return true;};
function fixture(t){
 const parent=realpathSync(tmpdir()),folder=mkdtempSync(join(parent,'soty-real-app-widgets-')),appsPath=join(folder,'apps.sqlite'),widgetPath=join(folder,'widgets.sqlite'),objectPath=join(folder,'objects.sqlite');let apps,widgets,objects,serial=0;const changes=[];
 const issued=new WeakSet([alice,bob,carol]),active=actor=>[alice,bob,carol].some(value=>value.accountId===actor?.accountId&&value.deviceId===actor?.deviceId),fence=(actor,callback)=>{assert.ok(issued.has(actor),'only original issued actor');return callback();};
 const options={databasePath:appsPath,appOriginTemplate:'https://{appId}.legacy.example',namedAppZone:'https://apps.example',shellOrigins:['https://root.example'],actorActive:active,withAuthorityFence:callback=>callback(),checkWidgetLaunch:request=>widgets.checkLaunch(request)};
 function open(){apps=createAppsService(options);widgets=createCanvasWidgetsService({databasePath:widgetPath,projectId:'widgets_tests',actorActive:active,withActorAuthorityFence:fence,withAppSourceAuthority:(request,callback)=>apps.withWidgetSourceAuthority(request,callback),onChange:change=>changes.push(change)});
  const provider=createCanvasWidgetProvider({widgets,withObjectAuthority:(context,callback)=>objects.withObjectAuthority(context,callback)});objects=createCanvasObjectsService({databasePath:objectPath,projectId:'widgets_tests',actorActive:active,withActorAuthorityFence:fence,providers:[provider]});}
 open();const db=new DatabaseSync(appsPath);
 for(const [actor,name]of[[alice,'Alice'],[bob,'Bob']]){const identity={linkId:name+'_link',hostDeviceId:name+'_host',connectorId:name+'_connector'};db.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(connectorKey(identity),actor.accountId,JSON.stringify(identity),name+' actual device',1);}
 db.close();
 const a=(actor,op,args={})=>apps.execute({actor,op:'apps.'+op,args}),w=(actor,op,args={})=>widgets.execute({actor,op:'canvas.widgets.'+op,args:{expectedAccountId:actor.accountId,...args}}),wm=(actor,op,args)=>w(actor,op,{requestId:'widget_cmd_'+(++serial),...args});
 const first=a(alice,'register',{hostDeviceId:'Alice_host',connectorId:'Alice_connector',name:'First independently owned app',port:30111}).app;
 const second=a(bob,'register',{hostDeviceId:'Bob_host',connectorId:'Bob_connector',name:'Second independently owned app',port:30112,grants:{accountIds:[alice.accountId,carol.accountId],communityIds:[]}}).app;
 const objectFor=(widget,visibility={audience:'private',accountIds:[],communityIds:[]})=>objects.execute({actor:alice,op:'canvas.objects.create',args:{expectedAccountId:alice.accountId,requestId:'object_cmd_'+(++serial),binding:{providerId:'soty.apps',ref:widget.widgetId},presentation:{mask:{shape:'rect'}},visibility,hidden:{owner:false,others:false},protection:{kind:'none'}}}).current;
 t.after(()=>{objects.close();widgets.close();apps.close();const actual=realpathSync(folder);assert.equal(dirname(actual).toLowerCase(),parent.toLowerCase());assert.ok(basename(actual).startsWith('soty-real-app-widgets-'));rmSync(actual,{recursive:true,force:true});});
 return {a,w,wm,first,second,widgetPath,objectFor,changes,get objects(){return objects;},get widgets(){return widgets;},get apps(){return apps;},restart(){objects.close();widgets.close();apps.close();open();}};
}

test('one provider pins two real apps of independent owners; a participant pins foreign app without copying metadata or inheriting rights',t=>{
 const f=fixture(t),own=f.wm(alice,'create',{appId:f.first.id}).current,foreign=f.wm(alice,'create',{appId:f.second.id,requestId:'stable_foreign'}).current;assert.notEqual(own.widgetId,foreign.widgetId);assert.equal(foreign.ownerId,alice.accountId);assert.equal(foreign.appId,f.second.id);
 const ownObject=f.objectFor(own),foreignObject=f.objectFor(foreign,{audience:'public',accountIds:[],communityIds:[]});assert.equal(f.objects.project({actor:alice,objectIds:[ownObject.objectId,foreignObject.objectId]}).items.every(item=>item.available),true);
 const viewer=f.objects.project({actor:carol,objectIds:[foreignObject.objectId]}).items[0];assert.equal(viewer.view.app.ownerAccountId,bob.accountId);assert.equal(viewer.view.runtimeAllowed,true);assert.equal(viewer.view.instanceId,foreign.widgetId);assert.equal(viewer.view.app.status,'offline','offline metadata is available and does not masquerade as revoked ACL');assert.equal(JSON.stringify(viewer).includes('launchUrl'),false);
 assert.equal(f.objects.project({objectIds:[foreignObject.objectId]}).items[0].available,false,'a public object must not publish a private app');
 f.a(bob,'update',{appId:f.second.id,name:'Fresh app title'});assert.equal(f.objects.project({actor:alice,objectIds:[foreignObject.objectId]}).items[0].view.app.name,'Fresh app title');
 f.a(bob,'update',{appId:f.second.id,grants:{accountIds:[alice.accountId],communityIds:[]}});assert.equal(f.objects.project({actor:carol,objectIds:[foreignObject.objectId]}).items[0].available,false);assert.equal(f.objects.project({actor:alice,objectIds:[foreignObject.objectId]}).items[0].available,true);
 f.restart();assert.equal(f.w(alice,'get',{widgetId:foreign.widgetId}).widget.widgetId,foreign.widgetId);assert.equal(f.wm(alice,'create',{appId:f.second.id,requestId:'stable_foreign'}).replayed,true);assert.throws(()=>f.w(carol,'get',{widgetId:foreign.widgetId}),code('canvas_widget_owner_required'));
 const db=new DatabaseSync(f.widgetPath);try{assert.doesNotMatch(JSON.stringify(db.prepare('SELECT * FROM canvas_widgets').all()),/Fresh app title|First independently|launchUrl|cover|width|font/u);}finally{db.close();}
});

test('host protection authority accepts only its exact purpose without forwarding it into the Apps Source contract',t=>{
 const f=fixture(t),widget=f.wm(alice,'create',{appId:f.second.id}).current,object=f.objectFor(widget);
 const snapshot=f.objects.withProtectionTarget({actor:alice,objectId:object.objectId,mode:'owner'},value=>value);
 assert.equal(snapshot.binding.ref,widget.widgetId);assert.equal(snapshot.ownerId,alice.accountId);
 assert.throws(()=>f.widgets.withSourceAuthority({actor:alice,objectOwnerId:alice.accountId,objectId:object.objectId,ref:widget.widgetId,mode:'read',purpose:'arbitrary_source_grant'},()=>true));
 f.a(bob,'update',{appId:f.second.id,grants:{accountIds:[],communityIds:[]}});
 assert.throws(()=>f.objects.withProtectionTarget({actor:alice,objectId:object.objectId,mode:'owner'},()=>true),code('canvas_provider_access_denied'));
});

test('two real widget app relations remain owner-only metadata when hidden and never enter Source authority',t=>{
 const f=fixture(t),first=f.wm(alice,'create',{appId:f.first.id}).current,second=f.wm(alice,'create',{appId:f.second.id}).current;
 const objects=[f.objectFor(first),f.objectFor(second)];
 for(const object of objects)f.objects.execute({actor:alice,op:'canvas.objects.update',args:{expectedAccountId:alice.accountId,objectId:object.objectId,requestId:'hide_'+object.objectId,expectedRevision:object.revision,hidden:{owner:true,others:false}}});
 const provider=createCanvasWidgetProvider({widgets:f.widgets,withObjectAuthority:(context,callback)=>f.objects.withObjectAuthority(context,callback)});
 f.objects.unregisterProvider('soty.apps');f.objects.registerProvider({...provider,withBindingAuthority(){assert.fail('hidden relation must not open Source authority');},project(){assert.fail('hidden relation must not project Source');}});
 let items=f.objects.project({actor:alice,objectIds:objects.map(object=>object.objectId)}).items;
 assert.deepEqual(items.map(item=>item.ownerRepresents),[[{kind:'app',id:f.first.id}],[{kind:'app',id:f.second.id}]]);assert.ok(items.every(item=>!item.available));
 assert.equal(f.widgets.ownerRepresents({ownerId:carol.accountId,objectId:objects[0].objectId,ref:first.widgetId}),undefined);
 f.wm(alice,'remove',{widgetId:first.widgetId,expectedRevision:first.revision});items=f.objects.project({actor:alice,objectIds:objects.map(object=>object.objectId)}).items;
 assert.deepEqual(items[0].ownerRepresents,[]);assert.deepEqual(items[1].ownerRepresents,[{kind:'app',id:f.second.id}]);
 f.restart();assert.deepEqual(f.objects.project({actor:alice,objectIds:[objects[1].objectId]}).items[0].ownerRepresents,[{kind:'app',id:f.second.id}]);
});

test('known unlisted public alias previews anonymously without runtime credentials, and retiring its exact entry never falls back',t=>{
 const f=fixture(t),aliases=[];for(const slug of ['known','other'])aliases.push(f.a(bob,'domains.claim',{appId:f.second.id,slug,requestId:'alias_'+slug,expectedDomainsRevision:f.a(bob,'domains.get',{appId:f.second.id}).revision}).receipt);
 const current=f.a(bob,'publication.get',{appId:f.second.id});f.a(bob,'publication.update',{appId:f.second.id,requestId:'public_unlisted',expectedPolicyEpoch:current.policyEpoch,expectedTargetRevision:current.target.revision,launchPolicy:'anyone',listed:false,activeDomainIds:aliases.map(alias=>alias.domainId),exposureAck:{scope:'whole-port',targetRevision:current.target.revision,targetDigest:current.target.digest,profile:current.target.profile}});
 const widget=f.wm(alice,'create',{appId:f.second.id,entry:{domainId:aliases[0].domainId,path:'/'}}).current,object=f.objectFor(widget,{audience:'public',accountIds:[],communityIds:[]});
 const preview=f.objects.project({objectIds:[object.objectId]}).items[0];assert.equal(preview.available,true);assert.equal(preview.view.runtimeAllowed,false);assert.equal(preview.view.entry.domainId,aliases[0].domainId);assert.equal(f.a(carol,'catalog').apps.length,0,'explicit known widget does not alter discovery listing');
 f.a(bob,'domains.retire',{appId:f.second.id,domainId:aliases[0].domainId,requestId:'retire_pinned',expectedDomainsRevision:f.a(bob,'domains.get',{appId:f.second.id}).revision});
 assert.equal(f.objects.project({actor:alice,objectIds:[object.objectId]}).items[0].available,false);assert.equal(f.w(alice,'get',{widgetId:widget.widgetId}).widget.entry.domainId,aliases[0].domainId);
});

test('a failed widget receipt write commits no instance; exact retry and immutable app/entry survive reopen',t=>{
 const f=fixture(t),db=new DatabaseSync(f.widgetPath);db.exec("CREATE TRIGGER fail_widget_command BEFORE INSERT ON widget_commands BEGIN SELECT RAISE(ABORT,'widget_receipt_fault'); END");
 try{assert.throws(()=>f.wm(alice,'create',{appId:f.second.id,requestId:'retry_widget'}),/widget_receipt_fault/u);}finally{db.exec('DROP TRIGGER fail_widget_command');db.close();}
 assert.equal(f.w(alice,'list').widgets.length,0);const accepted=f.wm(alice,'create',{appId:f.second.id,requestId:'retry_widget'});assert.equal(f.wm(alice,'create',{appId:f.second.id,requestId:'retry_widget'}).replayed,true);
 assert.throws(()=>f.wm(alice,'create',{appId:f.first.id,requestId:'retry_widget'}),code('canvas_widget_request_conflict'));f.restart();assert.deepEqual(f.w(alice,'get',{widgetId:accepted.current.widgetId}).widget,accepted.current);
});

test('host source hints query active instances without metadata copies/revision writes, and known removed IDs never become legacy',t=>{
 const f=fixture(t),first=f.wm(alice,'create',{appId:f.first.id}).current,second=f.wm(alice,'create',{appId:f.second.id}).current;assert.equal(f.widgets.hasInstance(first.widgetId),true);assert.equal(f.widgets.hasInstance('widget-12345678-1234-1234-1234-123456789abc'),false);
 f.changes.length=0;assert.equal(f.widgets.notifyAppChanged({appId:f.first.id}).notified,1);assert.deepEqual(f.changes,[{widgetId:first.widgetId,ownerId:alice.accountId,appId:f.first.id,revision:1}]);assert.deepEqual(f.w(alice,'get',{widgetId:first.widgetId}).widget,first);assert.deepEqual(f.w(alice,'get',{widgetId:second.widgetId}).widget,second);
 f.wm(alice,'remove',{widgetId:first.widgetId,expectedRevision:1});assert.equal(f.widgets.hasInstance(first.widgetId),true);assert.equal(f.widgets.notifyAppChanged({appId:f.first.id}).notified,0);assert.throws(()=>f.widgets.checkLaunch({actor:alice,widgetId:first.widgetId,appId:f.first.id,entry:first.entry,target:first.source}),code('canvas_widget_entry_changed'));
});
