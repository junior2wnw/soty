import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {mkdtempSync,rmSync,realpathSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,basename} from 'node:path';
import {createCanvasObjectsService} from '../canvas-objects/server.mjs';
import {CanvasObjectError} from '../canvas-objects/contract.mjs';
import {createWorldService} from '../world/server/index.mjs';
import {createCanvasPagesService} from './server.mjs';

const owner={accountId:'page_owner',deviceId:'page_owner_device',label:'Private owner identity'},friend={accountId:'page_friend',deviceId:'page_friend_device',label:'Friend'},outside={accountId:'page_outside',deviceId:'page_outside_device',label:'Outsider'};
const code=value=>error=>{assert.equal(error.code,value);return true;};
function fixture(t){
 const parent=realpathSync(tmpdir()),folder=mkdtempSync(join(parent,'soty-canvas-pages-')),worldFile=join(folder,'world.sqlite'),objectFile=join(folder,'objects.sqlite'),pageFile=join(folder,'pages.sqlite');let world,objects,pages,serial=0,depth=0,bypass=false,projectSideEffect;
 const issued=new WeakSet([owner,friend,outside]),activeDevices=new Set([owner.deviceId,friend.deviceId,outside.deviceId]),contacts=new Set(),sources=new Map(),calls=[],legacyCalls=[];
 const active=actor=>activeDevices.has(actor?.deviceId),fence=(actor,callback)=>{assert.ok(issued.has(actor),'original issued actor required');assert.equal(depth,0,'no nested Connect original-actor fence');depth++;try{return callback();}finally{depth--;}};
 const provider={providerId:'soty.fixture',withBindingAuthority(request,callback){calls.push(['authority',request.ref,request.actor?.accountId]);const source=sources.get(request.ref),allowed=()=>Boolean(source&&source.ownerId===request.objectOwnerId&&(source.public||source.ownerId===request.actor?.accountId||source.viewers?.has(request.actor?.accountId)));const wasAllowed=allowed();const result=callback({allowed:wasAllowed,protection:source?.password?{kind:'password',sealed:true}:{kind:'none'}});if(wasAllowed&&!allowed())throw new CanvasObjectError('canvas_provider_access_denied',403);return result;},project(context){calls.push(['project',context.ref,context.viewer.kind]);if(projectSideEffect)projectSideEffect(context);return objects.withObjectAuthority(context,()=>({kind:'note',text:sources.get(context.ref).text}));}};
 const withContacts=(actor,callback)=>callback({accountId:actor.accountId,deviceId:actor.deviceId,has:id=>contacts.has(actor.accountId+':'+id),accountIds:()=>[...contacts].filter(value=>value.startsWith(actor.accountId+':')).map(value=>value.split(':')[1])});
 const legacy=(request,callback)=>{legacyCalls.push(request);if(request.appId!=='allowed_legacy_app')return callback({allowed:false});return callback({app:{appId:request.appId,name:'Current actual app',status:'ready'},entry:{appId:request.appId,domainId:'domain_current',origin:'https://known.example',path:'/'},runtimeAllowed:Boolean(request.actor)});};
 function open(){world=createWorldService({databasePath:worldFile,projectId:'pages_test',withCurrentContacts:withContacts});objects=createCanvasObjectsService({databasePath:objectFile,projectId:'pages_test',actorActive:active,withActorAuthorityFence:fence,providers:[provider]});pages=createCanvasPagesService({databasePath:pageFile,projectId:'pages_test',actorActive:active,withActorAuthorityFence:fence,withCanvasSource:(request,callback)=>world.withCanvasSource(request,callback),objects:{project:request=>objects.project(bypass?{...request,actor:owner}:request),withObjectRead:(request,callback)=>objects.withObjectRead(request,callback),withObjectAuthority:(context,callback)=>objects.withObjectAuthority(context,callback)},withLegacyAppAuthority:legacy});}
 open();for(const actor of [owner,friend,outside])world.execute({actor,op:'world.profile.get'});
 t.after(()=>{pages.close();objects.close();world.close();const actual=realpathSync(folder);assert.equal(dirname(actual).toLowerCase(),parent.toLowerCase());assert.ok(basename(actual).startsWith('soty-canvas-pages-'));rmSync(actual,{recursive:true,force:true});});
 const call=(actor,op,args={})=>pages.execute({actor,op:'canvas.pages.'+op,args:{expectedAccountId:actor.accountId,...args}});
 const put=(revision=0,extra={},actor=owner)=>call(actor,'put',{requestId:'page_cmd_'+(++serial),expectedRevision:revision,enabled:true,audience:'public',...extra});
 const objectCall=(actor,op,args)=>objects.execute({actor,op:'canvas.objects.'+op,args:{expectedAccountId:actor.accountId,...args}});
 const create=(ref,extra={},sourceExtra={})=>{sources.set(ref,{ownerId:owner.accountId,public:true,text:'Visible source '+ref,...sourceExtra});return objectCall(owner,'create',{requestId:'object_cmd_'+(++serial),binding:{providerId:'soty.fixture',ref},presentation:{mask:{shape:'rect'}},visibility:{audience:'public',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'none'},...extra}).current;};
 const field=()=>world.execute({actor:owner,op:'world.field.get',args:{expectedAccountId:owner.accountId}});
 const save=document=>world.execute({actor:owner,op:'world.field.put',args:{expectedAccountId:owner.accountId,expectedRevision:field().revision,requestId:'field_cmd_'+(++serial),document}});
 const doc=shortcuts=>({schema:'soty.field.v2',contexts:[{contextId:'private_context_name',title:'Private context title',x:120,y:340},{contextId:'secret_empty_context',title:'Secret empty context',x:987654,y:887654}],shortcuts:shortcuts.map((entity,index)=>({shortcutId:'private_shortcut_label_'+index,entity,contextId:'private_context_name',placement:{kind:'free',x:index*100,y:index*200,width:240,height:160}}))});
 const read=(pageId,actor,extra={})=>pages.read({pageId,...(actor?{actor}:{}),...extra});
 const sql=(file,callback)=>{const db=new DatabaseSync(file);try{return callback(db);}finally{db.close();}};
 return {call,put,create,objectCall,sources,calls,legacyCalls,contacts,activeDevices,field,save,doc,read,worldFile,objectFile,pageFile,sql,get world(){return world;},get objects(){return objects;},get pages(){return pages;},bypass(value){bypass=value;},projectSideEffect(callback){projectSideEffect=callback;},restart(){pages.close();objects.close();world.close();open();}};
}

test('thin publication config CAS/receipts keep stable pageId and canonical World bytes unchanged across reopen and account isolation',t=>{
 const f=fixture(t),object=f.create('first');f.save(f.doc([{kind:'object',id:object.objectId}]));const field=f.field();assert.equal(f.call(owner,'get').page,null);
 const args={requestId:'publish_once'},first=f.put(0,args);assert.equal(first.current.enabled,true);assert.equal(f.put(0,args).replayed,true);assert.throws(()=>f.put(0,{requestId:'wrong_cas'}),code('canvas_page_revision_conflict'));
 assert.throws(()=>f.put(0,{...args,audience:'unlisted'}),code('canvas_page_request_conflict'));assert.equal(f.call(friend,'get').page,null);assert.throws(()=>f.call({...owner},'get'),/original issued actor/u);
 f.restart();assert.equal(f.call(owner,'get').page.pageId,first.current.pageId);const changed=f.put(1,{enabled:false});assert.equal(changed.current.pageId,first.current.pageId);assert.deepEqual(f.field(),field);
 assert.doesNotMatch(f.sql(f.pageFile,db=>JSON.stringify(db.prepare('SELECT * FROM canvas_pages').all())),/Visible source|private_context|placement|object_/u);
});

test('legacy hex v1 source is projected into public geometry without rewriting its bytes, hash, receipt or reader floor',t=>{
 const f=fixture(t),document={schema:'soty.field.v1',contexts:[{contextId:'old_context',title:'Private old context name',x:101,y:202}],shortcuts:[{shortcutId:'old_shortcut',entity:{kind:'app',id:'allowed_legacy_app'},contextId:'old_context',slot:[4,-2]}]};f.save(document);const before=f.field(),pageId=f.put().current.pageId,view=f.read(pageId);
 assert.deepEqual(view.field.shortcuts[0].placement,{kind:'hex',slot:[4,-2]});assert.equal(view.field.contexts[0].x,101);assert.equal(view.field.contexts[0].y,202);assert.deepEqual(f.field(),before);assert.equal(f.field().document.schema,'soty.field.v1');assert.equal(f.sql(f.worldFile,db=>db.prepare("SELECT value FROM world_meta WHERE key='field_schema_version'").get().value),'1');
});

test('anonymous projection removes private/hidden/removed/unsupported types before refs, coordinates and metadata leave the server',t=>{
 const f=fixture(t),visible=f.create('visible'),privateObject=f.create('PRIVATE_SOURCE',{visibility:{audience:'private',accountIds:[],communityIds:[]}}),hidden=f.create('HIDDEN_SOURCE',{hidden:{owner:false,others:true}}),removed=f.create('REMOVED_SOURCE');
 f.objectCall(owner,'remove',{objectId:removed.objectId,expectedRevision:1,requestId:'remove_object'});
 f.save(f.doc([{kind:'object',id:visible.objectId},{kind:'object',id:privateObject.objectId},{kind:'object',id:hidden.objectId},{kind:'object',id:removed.objectId},{kind:'device',id:'SECRET_DEVICE'},{kind:'person',id:'SECRET_CONTACT'},{kind:'community',id:'SECRET_GROUP'},{kind:'builtin',id:'SECRET_NATIVE_SECTION'},{kind:'app',id:'allowed_legacy_app'}]));const pageId=f.put().current.pageId;f.calls.length=0;
 const view=f.read(pageId),encoded=JSON.stringify(view);assert.equal(view.items.length,2);assert.equal(view.field.shortcuts.length,2);assert.equal(view.field.contexts.length,1);assert.equal(view.field.contexts[0].x,120);assert.equal(view.field.contexts[0].y,340);assert.deepEqual(view.field.shortcuts[0].placement,{kind:'free',x:0,y:0,width:240,height:160});
 for(const value of [privateObject.objectId,hidden.objectId,removed.objectId,'PRIVATE_SOURCE','HIDDEN_SOURCE','REMOVED_SOURCE','SECRET_DEVICE','SECRET_CONTACT','SECRET_GROUP','SECRET_NATIVE_SECTION','Private context title','private_context_name','private_shortcut_label','secret_empty_context','987654','887654',owner.accountId,owner.label])assert.equal(encoded.includes(value),false,value);
 assert.equal(f.calls.filter(value=>value[0]==='project').length,1);assert.equal(f.legacyCalls.length,1);assert.equal(f.legacyCalls[0].actor,null);assert.equal(view.items[1].view.runtimeAllowed,false);
 assert.deepEqual(view.banner,{kind:'generic',title:'Полотно'});
});

test('signed foreign viewers retain only issued object rights; source revocation removes visible geometry without owner impersonation',t=>{
 const f=fixture(t),restricted=f.create('granted',{visibility:{audience:'restricted',accountIds:[friend.accountId],communityIds:[]}});f.sources.get('granted').public=false;f.sources.get('granted').viewers=new Set([friend.accountId]);f.save(f.doc([{kind:'object',id:restricted.objectId}]));const pageId=f.put().current.pageId;
 assert.equal(f.read(pageId).items.length,0);assert.equal(f.read(pageId,outside).items.length,0);assert.equal(f.read(pageId,friend).items[0].view.text,'Visible source granted');assert.ok(f.calls.some(value=>value[2]===friend.accountId));
 f.sources.get('granted').viewers.clear();assert.equal(f.read(pageId,friend).items.length,0);assert.equal(f.read(pageId,friend).field.contexts.length,0);
});

test('a page can pin another owner public object without inheriting ownership; that real owner can revoke it immediately',t=>{
 const f=fixture(t);f.sources.set('foreign_source',{ownerId:friend.accountId,public:true,text:'Published by a different real Source owner'});
 const foreign=f.objectCall(friend,'create',{requestId:'foreign_object',binding:{providerId:'soty.fixture',ref:'foreign_source'},presentation:{mask:{shape:'ellipse'}},visibility:{audience:'public',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'none'}}).current;
 f.save(f.doc([{kind:'object',id:foreign.objectId}]));const pageId=f.put().current.pageId;assert.equal(f.read(pageId).items[0].view.text,'Published by a different real Source owner');
 f.objectCall(friend,'update',{objectId:foreign.objectId,expectedRevision:1,requestId:'foreign_revoke',visibility:{audience:'private',accountIds:[],communityIds:[]}});assert.equal(f.read(pageId).items.length,0);assert.throws(()=>f.pages.withPageObjectRead({pageId,objectId:foreign.objectId},()=>''),code('canvas_object_unavailable'));
});

test('a published password object shows only its lock and never resolves Source content or grants an anonymous content callback',t=>{
 const f=fixture(t),locked=f.create('SECRET_PASSWORD_SOURCE',{protection:{kind:'password'}},{password:true,text:'SECRET_PASSWORD_PLAINTEXT'});f.save(f.doc([{kind:'object',id:locked.objectId}]));const pageId=f.put().current.pageId;f.calls.length=0;const view=f.read(pageId);
 assert.equal(view.items.length,1);assert.equal(view.items[0].view.state,'locked');assert.equal(f.calls.length,0);assert.equal(JSON.stringify(view).includes('SECRET_PASSWORD'),false);let invoked=false;
 assert.throws(()=>f.pages.withPageObjectRead({pageId,objectId:locked.objectId},()=>{invoked=true;return 'leaked';}),code('canvas_object_locked'));assert.equal(invoked,false);
});

test('visible hash is quiet for private geometry/contexts/profile/owner-hide edits and changes for actual visible Source edits',t=>{
 const f=fixture(t),visible=f.create('public'),privateObject=f.create('private',{visibility:{audience:'private',accountIds:[],communityIds:[]}});f.save(f.doc([{kind:'object',id:visible.objectId},{kind:'object',id:privateObject.objectId}]));const pageId=f.put().current.pageId,first=f.read(pageId);
 const document=f.field().document;document.shortcuts[1].placement.x=93829;document.contexts[1].title='A different private title';f.save(document);assert.equal(f.read(pageId).viewRevision,first.viewRevision);
 f.world.execute({actor:owner,op:'world.profile.update',args:{expectedRevision:f.world.execute({actor:owner,op:'world.profile.get'}).profile.revision,displayName:'Another private identity'}});assert.equal(f.read(pageId).viewRevision,first.viewRevision);
 f.objectCall(owner,'update',{objectId:visible.objectId,expectedRevision:1,requestId:'hide_only_owner',hidden:{owner:true,others:false}});assert.equal(f.read(pageId).viewRevision,first.viewRevision,'raw Object revision is excluded when it changes only owner visibility');
 f.sources.get('public').text='Fresh visible Source';assert.notEqual(f.read(pageId).viewRevision,first.viewRevision);
});

test('banner obeys current Source profile privacy including reciprocal friends, with no contacts, devices or presence projection',t=>{
 const f=fixture(t),pageId=f.put().current.pageId;
 const profileUpdate=audience=>f.world.execute({actor:owner,op:'world.profile.update',args:{expectedRevision:f.world.execute({actor:owner,op:'world.profile.get'}).profile.revision,audience}});
 profileUpdate('friends');assert.equal(f.read(pageId,friend).banner.kind,'generic');f.contacts.add(friend.accountId+':'+owner.accountId);assert.equal(f.read(pageId,friend).banner.profile.displayName,owner.label);f.contacts.clear();assert.equal(f.read(pageId,friend).banner.kind,'generic');
 profileUpdate('public');const banner=f.read(pageId).banner;assert.deepEqual(Object.keys(banner.profile).sort(),['avatarColor','avatarRevision','displayName','profileId']);assert.equal(banner.profile.displayName,owner.label);
 profileUpdate('private');assert.equal(f.read(pageId).banner.kind,'generic');assert.equal(f.read(pageId,owner).banner.kind,'profile');profileUpdate('link');assert.equal(f.read(pageId).banner.kind,'profile','known page follows existing link-profile Source policy');
});

test('canonical page membership and weak content frame are mandatory; JSON clones/late context/foreign public object cannot read',t=>{
 const f=fixture(t),object=f.create('content'),notPlaced=f.create('other_public');f.save(f.doc([{kind:'object',id:object.objectId}]));const pageId=f.put().current.pageId;let captured;
 const read=context=>f.pages.withPageObjectAuthority(context,proof=>f.objects.withObjectAuthority(proof,()=>({text:f.sources.get(proof.ref).text})));
 const value=f.pages.withPageObjectRead({pageId,objectId:object.objectId},context=>{captured=context;assert.throws(()=>f.pages.withPageObjectAuthority({...context},()=>true),code('canvas_page_authority_required'));assert.throws(()=>f.pages.withPageObjectAuthority(JSON.parse(JSON.stringify(context)),()=>true),code('canvas_page_authority_required'));return read(context);});assert.equal(value.text,'Visible source content');assert.throws(()=>read(captured),code('canvas_page_authority_required'));
 assert.throws(()=>f.pages.withPageObjectRead({pageId,objectId:notPlaced.objectId},()=>''),code('canvas_page_object_unavailable'));
 f.save(f.doc([]));assert.throws(()=>f.pages.withPageObjectRead({pageId,objectId:object.objectId},()=>''),code('canvas_page_object_unavailable'));
});

test('page unpublish or canonical unlink during callback discards bytes before returning; unrelated private field edits do not revoke content',t=>{
 const f=fixture(t),object=f.create('late_content'),privateObject=f.create('private_content',{visibility:{audience:'private',accountIds:[],communityIds:[]}});f.save(f.doc([{kind:'object',id:object.objectId},{kind:'object',id:privateObject.objectId}]));const pageId=f.put().current.pageId;
 const read=effect=>f.pages.withPageObjectRead({pageId,objectId:object.objectId},context=>f.pages.withPageObjectAuthority(context,proof=>{const value=f.objects.withObjectAuthority(proof,()=>({bytes:Buffer.from('returned only after final authority')}));effect?.();return value;}));
 const permitted=read(()=>{const document=f.field().document;document.shortcuts[1].placement.x=887711;f.save(document);});assert.equal(Buffer.from(permitted.bytes).toString(),'returned only after final authority');
 assert.throws(()=>read(()=>f.save(f.doc([{kind:'object',id:privateObject.objectId}]))),code('canvas_page_object_unavailable'));
 f.save(f.doc([{kind:'object',id:object.objectId}]));assert.throws(()=>read(()=>f.sql(f.pageFile,db=>db.prepare('UPDATE canvas_pages SET revision=revision+1,enabled=0 WHERE page_id=?').run(pageId))),code('canvas_page_unavailable'));
});

test('page policy disable/private/unlisted and account changes deny content and reopen preserves the same public link',t=>{
 const f=fixture(t),object=f.create('policy');f.save(f.doc([{kind:'object',id:object.objectId}]));let page=f.put().current;assert.equal(f.read(page.pageId).items.length,1);page=f.put(page.revision,{audience:'unlisted'}).current;assert.equal(f.read(page.pageId).items.length,1);
 page=f.put(page.revision,{audience:'private'}).current;assert.throws(()=>f.read(page.pageId),code('canvas_page_unavailable'));assert.throws(()=>f.read(page.pageId,friend),code('canvas_page_unavailable'));assert.equal(f.read(page.pageId,owner).items.length,1);
 page=f.put(page.revision,{audience:'public'}).current;f.restart();assert.equal(f.read(page.pageId).items.length,1);f.activeDevices.delete(friend.deviceId);assert.throws(()=>f.read(page.pageId,friend),code('canvas_page_authentication_required'));page=f.put(page.revision,{enabled:false}).current;assert.throws(()=>f.read(page.pageId),code('canvas_page_unavailable'));assert.throws(()=>f.read(page.pageId,owner),code('canvas_page_unavailable'));
});

test('failed SQLite publication receipt rolls configuration back, and the same retry succeeds without copying a field',t=>{
 const f=fixture(t);f.sql(f.pageFile,db=>db.exec("CREATE TRIGGER fail_page BEFORE INSERT ON canvas_page_commands BEGIN SELECT RAISE(ABORT,'page_write_fault'); END"));assert.throws(()=>f.put(0,{requestId:'retry_page'}),/page_write_fault/u);assert.equal(f.call(owner,'get').page,null);
 f.sql(f.pageFile,db=>db.exec('DROP TRIGGER fail_page'));const accepted=f.put(0,{requestId:'retry_page'});assert.equal(accepted.current.revision,1);assert.equal(f.put(0,{requestId:'retry_page'}).replayed,true);
});

test('bounded chunks and cursors are based on visible composition; a private edit keeps the cursor, visible edit invalidates it',t=>{
 const f=fixture(t),objects=Array.from({length:5},(_,index)=>f.create('chunk_'+index)),privateObject=f.create('private_chunk',{visibility:{audience:'private',accountIds:[],communityIds:[]}});f.save(f.doc([...objects,privateObject].map(object=>({kind:'object',id:object.objectId}))));const pageId=f.put().current.pageId,first=f.read(pageId,undefined,{limit:2});assert.equal(first.items.length,2);assert.ok(first.nextCursor);assert.ok(Buffer.byteLength(JSON.stringify(first))<=512*1024);
 const document=f.field().document;document.shortcuts.at(-1).placement.x=77221;f.save(document);const second=f.read(pageId,undefined,{limit:2,cursor:first.nextCursor});assert.equal(second.items.length,2);assert.equal(second.viewRevision,first.viewRevision);assert.notEqual(second.items[0].shortcutId,first.items[0].shortcutId);
 f.sources.get('chunk_0').text='Visible change invalidates pagination snapshot';assert.throws(()=>f.read(pageId,undefined,{cursor:first.nextCursor}),code('canvas_page_cursor_stale'));
});

test('negative control: owner-impersonating/CSS-only projection leaks private object refs and geometry and fails the same privacy oracle',t=>{
 const f=fixture(t),visible=f.create('public_negative'),privateObject=f.create('PRIVATE_NEGATIVE',{visibility:{audience:'private',accountIds:[],communityIds:[]}});f.save(f.doc([{kind:'object',id:visible.objectId},{kind:'object',id:privateObject.objectId}]));const pageId=f.put().current.pageId;
 const oracle=()=>{const view=f.read(pageId);assert.equal(JSON.stringify(view).includes(privateObject.objectId),false);assert.equal(view.field.shortcuts.length,1);};oracle();f.bypass(true);assert.throws(oracle,assert.AssertionError);f.bypass(false);oracle();
});

test('World canonical source is a short host read, never owner provisioning or a writable/async/nested World operation',t=>{
 const f=fixture(t);assert.throws(()=>f.world.withCanvasSource({ownerId:'missing_owner'},()=>{}),code('world_canvas_owner_unavailable'));
 assert.throws(()=>f.world.withCanvasSource({ownerId:owner.accountId},()=>f.world.execute({actor:owner,op:'world.profile.get'})),code('world_authority_mutation_forbidden'));
 assert.throws(()=>f.world.withCanvasSource({ownerId:owner.accountId},()=>Promise.resolve('later')),code('world_canvas_callback_async'));
 const snapshot=f.world.withCommunityAuthorityFence(()=>f.world.withCanvasSource({ownerId:owner.accountId},value=>value));assert.equal(snapshot.ownerId,owner.accountId);assert.equal(snapshot.banner.kind,'generic');
});
