import test from 'node:test';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {mkdtempSync,rmSync,realpathSync,readFileSync} from 'node:fs';
import {pathToFileURL,fileURLToPath} from 'node:url';
import {join,dirname,basename} from 'node:path';
import {tmpdir} from 'node:os';
import {createCanvasObjectsService} from '../canvas-objects/server.mjs';
import {createCanvasPagesService} from './server.mjs';
import {createWorldService} from '../world/server/index.mjs';
const owner={accountId:'selection_owner',deviceId:'selection_device',label:'Selection owner'},visitor={accountId:'selection_visitor',deviceId:'visitor_device',label:'Selection visitor'};
const policy=audience=>({audience,accountIds:[],communityIds:[]});
function fixture(t,{objectsFactory=createCanvasObjectsService,pagesFactory=createCanvasPagesService,format2=true}={}){
 const parent=realpathSync(tmpdir()),folder=realpathSync(mkdtempSync(join(parent,'soty-page-selection-'))),pageFile=join(folder,'pages.sqlite'),objectFile=join(folder,'objects.sqlite'),worldFile=join(folder,'world.sqlite');
 let objects,pages,world,serial=0,depth=0,sourceUnavailable=false;const sources=new Map(),calls=[],active=new Set([owner.deviceId,visitor.deviceId]),issued=new WeakSet([owner,visitor]);
 const fence=(actor,callback)=>{assert.ok(issued.has(actor),'an actual issued original actor crosses the fence');assert.equal(depth,0,'one actor fence per frame');depth++;try{return callback();}finally{depth--;}};
 const provider=id=>({providerId:id,withBindingAuthority(request,callback){calls.push({kind:'authority',providerId:id,ref:request.ref,viewer:request.actor?.accountId??null});const source=sources.get(request.ref),allowed=Boolean(source?.active&&source.ownerId===request.objectOwnerId&&(id==='soty.notes'||source.public||request.actor?.accountId===source.ownerId||source.viewers?.has(request.actor?.accountId)));return callback({allowed,protection:source?.password?{kind:'password',sealed:true}:{kind:'none'}});},
  project(context){calls.push({kind:'project',providerId:id,ref:context.ref,viewer:context.viewer});return objects.withObjectAuthority(context,scope=>({kind:id==='soty.apps'?'widget':'note',text:sources.get(scope.ref).text}));}});
 const open=()=>{
  world=createWorldService({databasePath:worldFile,projectId:'selection_test'});
  objects=objectsFactory({databasePath:objectFile,projectId:'selection_test',allowFormat2Migration:format2,actorActive:actor=>active.has(actor?.deviceId),withActorAuthorityFence:fence,providers:[provider('soty.notes'),provider('soty.apps')],withProtectionAuthority({object},callback){calls.push({kind:'gate',ref:object.binding.ref});return callback({kind:object.protection.kind,unlocked:object.protection.kind==='none',generation:0});}});
  pages=pagesFactory({databasePath:pageFile,projectId:'selection_test',allowFormat2Migration:format2,actorActive:actor=>active.has(actor?.deviceId),withActorAuthorityFence:fence,withCanvasSource:(request,callback)=>{assert.equal(sourceUnavailable,false,'field Source is unavailable');return world.withCanvasSource(request,callback);},objects});
  // The old service must reach the real new-request/read oracle, rather than
  // failing setup solely because its host registration method does not exist.
  objects.registerPageSelectionAuthority?.({verifyContext:(context,callback)=>pages.withSelectionAuthority(context,callback),nativeProviders:['soty.notes']});
 };open();world.execute({actor:owner,op:'world.profile.get'});
 t.after(()=>{pages.close();objects.close();world.close();assert.equal(dirname(realpathSync(folder)),parent);assert.ok(basename(folder).startsWith('soty-page-selection-'));rmSync(folder,{recursive:true,force:true});});
 const objectCall=(op,args)=>objects.execute({actor:owner,op:'canvas.objects.'+op,args:{expectedAccountId:owner.accountId,...args}});
 const create=(ref,extra={},sourceExtra={})=>{sources.set(ref,{ownerId:owner.accountId,active:true,public:false,text:'Private Native body '+ref,...sourceExtra});return objectCall('create',{requestId:'create_'+(++serial),binding:{providerId:'soty.notes',ref},presentation:{mask:{shape:'rect'},fontSize:16},visibility:policy('private'),hidden:{owner:false,others:false},protection:{kind:'none'},...extra}).current;};
 const field=()=>world.execute({actor:owner,op:'world.field.get',args:{expectedAccountId:owner.accountId}});
 const save=document=>world.execute({actor:owner,op:'world.field.put',args:{expectedAccountId:owner.accountId,requestId:'field_'+(++serial),expectedRevision:field().revision,document}});
 const place=values=>save({schema:'soty.field.v2',contexts:[{contextId:'private_space',title:'Secret own name',x:100,y:200}],shortcuts:values.map((object,index)=>({shortcutId:'shortcut_'+object.objectId,entity:{kind:'object',id:object.objectId},contextId:'private_space',placement:{kind:'free',x:index*300,y:0,width:240,height:160}}))});
 const selection=values=>({schema:'soty.canvas-page-selection.v1',fieldRevision:field().revision,items:values.map(object=>({shortcutId:'shortcut_'+object.objectId,entity:{kind:'object',id:object.objectId},expectedAccessRevision:object.accessRevision}))});
 const put=(revision,values,extra={})=>pages.execute({actor:owner,op:'canvas.pages.put',args:{expectedAccountId:owner.accountId,requestId:'publish_'+(++serial),expectedRevision:revision,enabled:true,audience:'unlisted',selection:selection(values),...extra}});
 const read=(pageId,actor)=>pages.read({pageId,...(actor?{actor}:{})});
 const content=(pageId,objectId,callback=context=>objects.withObjectAuthority(context,scope=>sources.get(scope.ref).text),actor)=>pages.withPageObjectRead({pageId,objectId,...(actor?{actor}:{})},pageContext=>pages.withPageObjectAuthority(pageContext,callback));
 const sql=callback=>{const db=new DatabaseSync(pageFile);try{return callback(db);}finally{db.close();}};
 return{create,place,selection,put,read,content,field,save,objectCall,sources,calls,active,sql,failFieldSource(){sourceUnavailable=true;},get objects(){return objects;},get pages(){return pages;},restart(){pages.close();objects.close();world.close();open();}};
}
test('one explicit selection publishes only that private Native object to the original anonymous/visitor page frame',t=>{
 const f=fixture(t),selected=f.create('selected'),unselected=f.create('UNSELECTED_SECRET');f.place([selected,unselected]);const field=f.field(),page=f.put(0,[selected]).current;
 f.calls.length=0;const view=f.read(page.pageId);assert.equal(view.items.length,1);assert.equal(view.items[0].view.text,'Private Native body selected');assert.equal(JSON.stringify(view).includes(unselected.objectId),false);assert.equal(f.calls.some(call=>call.ref==='UNSELECTED_SECRET'),false,'validation/projection must not resolve every field Source');
 assert.equal(f.content(page.pageId,selected.objectId),'Private Native body selected');assert.equal(f.content(page.pageId,selected.objectId,undefined,visitor),'Private Native body selected');assert.equal(f.objects.project({objectIds:[selected.objectId]}).items[0].available,false,'ordinary object ACL remains private');assert.deepEqual(f.field(),field);
 assert.equal(f.calls.filter(call=>call.kind==='project').some(call=>call.viewer?.kind==='account'&&call.viewer.accountId===owner.accountId),false,'page does not impersonate owner');f.restart();assert.equal(f.read(page.pageId).items.length,1);
});
test('Native Source edits and font/shape keep the selected page live; new field objects stay out without a canonical write',t=>{
 const f=fixture(t),object=f.create('live');f.place([object]);const page=f.put(0,[object]).current,first=f.read(page.pageId);f.sources.get('live').text='Current edited Native body';assert.notEqual(f.read(page.pageId).viewRevision,first.viewRevision);
 const visual=f.objectCall('update',{objectId:object.objectId,expectedRevision:object.revision,requestId:'font',presentation:{mask:{shape:'ellipse'},fontSize:27}}).current;assert.equal(visual.accessRevision,object.accessRevision);assert.equal(f.read(page.pageId).items[0].presentation.fontSize,27);assert.equal(f.content(page.pageId,object.objectId),'Current edited Native body');
 const fresh=f.create('NEW_PRIVATE_SECRET');const before=f.read(page.pageId);f.place([visual,fresh]);assert.equal(f.read(page.pageId).viewRevision,before.viewRevision);assert.equal(f.read(page.pageId).items.length,1);assert.equal(f.read(page.pageId).items[0].entity.id,object.objectId);
});
test('privacy/explicit Only me/hidden/remove and provider revoke stop page-only reads without changing Source text',t=>{
 for(const change of ['explicit','hidden','remove','provider']){
  const f=fixture(t),object=f.create('constant');f.place([object]);const page=f.put(0,[object]).current;assert.equal(f.content(page.pageId,object.objectId),'Private Native body constant');
  if(change==='explicit')f.objectCall('update',{objectId:object.objectId,expectedRevision:1,requestId:'only_me',visibility:policy('private'),revokePublication:true});
  if(change==='hidden')f.objectCall('update',{objectId:object.objectId,expectedRevision:1,requestId:'hide',hidden:{owner:false,others:true}});
  if(change==='remove')f.objectCall('remove',{objectId:object.objectId,expectedRevision:1,requestId:'remove'});
  if(change==='provider')f.sources.get('constant').active=false;
  assert.equal(f.read(page.pageId).items.length,0,change);assert.throws(()=>f.content(page.pageId,object.objectId));assert.equal(f.sources.get('constant').text,'Private Native body constant');
 }
});
test('deselect and disable invalidate nested content frames; cloned/late/async contexts give no grant',t=>{
 const f=fixture(t),object=f.create('context');f.place([object]);const page=f.put(0,[object]).current;let captured;
 f.pages.withPageObjectRead({pageId:page.pageId,objectId:object.objectId},context=>{captured=context;assert.throws(()=>f.pages.withPageObjectAuthority({...context},()=>''));return f.pages.withPageObjectAuthority(context,()=> 'allowed');});assert.throws(()=>f.pages.withPageObjectAuthority(captured,()=> 'late'));
 assert.throws(()=>f.content(page.pageId,object.objectId,()=>Promise.resolve('ASYNC_BYTES')),{code:'canvas_page_async_authority'});
 assert.throws(()=>f.content(page.pageId,object.objectId,()=>{f.put(1,[]);return 'NO_BYTES_AFTER_DESELECT';}));assert.equal(f.read(page.pageId).items.length,0);
 const next=f.put(2,[object]).current;assert.throws(()=>f.content(next.pageId,object.objectId,()=>{f.put(3,[object],{enabled:false});return 'NO_BYTES_AFTER_CLOSE';}));assert.throws(()=>f.read(next.pageId));
});

test('selected passwords check their Source header after the gate without resolving payload or advertising Page-only unlock',t=>{
 const f=fixture(t),object=f.create('encrypted',{protection:{kind:'password'}},{password:true});f.place([object]);const page=f.put(0,[object]).current;f.calls.length=0;
 for(const actor of [undefined,visitor]){const item=f.read(page.pageId,actor).items[0];assert.equal(item.protection.kind,'password');assert.equal(item.canUnlock,false);assert.equal(item.view.state,'locked');assert.throws(()=>f.content(page.pageId,object.objectId,undefined,actor),{code:'canvas_object_locked'});}
 assert.equal(f.read(page.pageId,owner).items[0].canUnlock,true,'normal owner ACL permits the separate ordinary unlock flow');assert.equal(f.calls.some(call=>call.kind==='project'),false,'locked payload is never projected/decrypted');
 for(let index=0;index<f.calls.length;index++)if(f.calls[index].kind==='authority')assert.equal(f.calls[index-1]?.kind,'gate','the bounded Source header follows the password gate');
 f.sources.get('encrypted').active=false;assert.equal(f.read(page.pageId).items.length,0,'a lock does not preserve removed Source geometry');assert.equal(f.calls.some(call=>call.kind==='project'),false);
});

test('selecting a widget never substitutes Page owner rights for the actual viewer Source grant',t=>{
 const f=fixture(t),object=f.create('foreign_widget',{binding:{providerId:'soty.apps',ref:'foreign_widget'},visibility:policy('public')});f.place([object]);const page=f.put(0,[object]).current;
 assert.equal(f.read(page.pageId).items.length,0);assert.equal(f.read(page.pageId,visitor).items.length,0);f.sources.get('foreign_widget').viewers=new Set([visitor.accountId]);assert.equal(f.read(page.pageId,visitor).items.length,1);assert.equal(f.read(page.pageId).items.length,0);
 f.sources.get('foreign_widget').viewers.clear();assert.equal(f.read(page.pageId,visitor).items.length,0);assert.equal(f.read(page.pageId,owner).items.length,1);
});

test('the private Page exception is Root Native-only: a public app Source still needs its current Object ACL',t=>{
 const f=fixture(t),object=f.create('public_app_private_object',{binding:{providerId:'soty.apps',ref:'public_app_private_object'}},{public:true});f.place([object]);const page=f.put(0,[object]).current;f.calls.length=0;
 assert.equal(f.read(page.pageId).items.length,0);assert.equal(f.read(page.pageId,visitor).items.length,0);assert.equal(f.calls.some(call=>call.kind==='authority'||call.kind==='project'),false,'Page owner selection cannot delegate arbitrary provider rights');assert.equal(f.read(page.pageId,owner).items.length,1);
});

test('selection CAS, malformed/bounded choices and receipt replay protect the canonical publication intent',t=>{
 const f=fixture(t),object=f.create('cas');f.place([object]);const originalField=f.field(),args={expectedAccountId:owner.accountId,requestId:'durable_selection',expectedRevision:0,enabled:true,audience:'unlisted',selection:f.selection([object])};
 const call=extra=>f.pages.execute({actor:owner,op:'canvas.pages.put',args:{...args,requestId:'bad_selection',...extra}});
 assert.throws(()=>call({selection:{...args.selection,fieldRevision:originalField.revision+1}}),{code:'canvas_page_selection_stale'});
 assert.throws(()=>call({selection:{...args.selection,items:[{shortcutId:'not_in_field',entity:{kind:'object',id:object.objectId},expectedAccessRevision:1}]}}),{code:'canvas_page_selection_invalid'});
 assert.throws(()=>call({selection:{...args.selection,items:[args.selection.items[0],args.selection.items[0]]}}),{code:'canvas_page_selection_duplicate'});
 assert.throws(()=>call({selection:{...args.selection,items:[{...args.selection.items[0],expectedAccessRevision:2}]}}),{code:'canvas_page_selection_stale'});
 assert.throws(()=>call({selection:{...args.selection,items:Array.from({length:257},(_,i)=>({shortcutId:'choice_'+i,expectedAccessRevision:1}))}}),{code:'canvas_page_selection_invalid'});
 const accepted=f.pages.execute({actor:owner,op:'canvas.pages.put',args});assert.equal(accepted.current.revision,1);f.sources.get('cas').active=false;
 const replay=f.pages.execute({actor:owner,op:'canvas.pages.put',args});assert.equal(replay.replayed,true);assert.deepEqual(replay.receipt,accepted.receipt,'a durable command replay does not re-run current Source admission');
 f.pages.execute({actor:owner,op:'canvas.pages.put',args:{expectedAccountId:owner.accountId,requestId:'old_client_update',expectedRevision:1,enabled:true,audience:'public'}});assert.deepEqual(f.pages.execute({actor:owner,op:'canvas.pages.get',args:{expectedAccountId:owner.accountId}}).page.selection,args.selection,'old config writes preserve explicit choices');assert.deepEqual(f.field(),originalField);
});

test('a saved Page choice cannot silently follow the same shortcut id rebound to another private object with the same policy revision',t=>{
 const f=fixture(t),first=f.create('confirmed_identity'),replacement=f.create('UNCONFIRMED_REPLACEMENT');assert.equal(first.accessRevision,replacement.accessRevision);f.place([first]);const page=f.put(0,[first]).current,captured=f.pages.execute({actor:owner,op:'canvas.pages.get',args:{expectedAccountId:owner.accountId}}).page.selection;
 const changed=f.field().document;changed.shortcuts[0].entity={kind:'object',id:replacement.objectId};f.save(changed);assert.equal(f.read(page.pageId).items.length,0,'the previously stored selection never follows the new binding');
 assert.throws(()=>f.pages.execute({actor:owner,op:'canvas.pages.put',args:{expectedAccountId:owner.accountId,requestId:'audience_only_after_reopen',expectedRevision:1,enabled:true,audience:'public',selection:{...captured,fieldRevision:f.field().revision}}}),{code:'canvas_page_selection_stale'},'refreshing field revision or audience does not express consent to a replacement object');
 const current=f.pages.execute({actor:owner,op:'canvas.pages.get',args:{expectedAccountId:owner.accountId}}).page;assert.equal(current.revision,1);assert.deepEqual(current.selection.items[0].entity,{kind:'object',id:first.objectId});assert.equal(f.read(page.pageId).items.length,0);
});

test('legacy app choices pin the confirmed entity too, forbid Source binding input and require explicit consent to a rebound app',t=>{
 const f=fixture(t),first={kind:'app',id:'confirmed_legacy_app'},next={kind:'app',id:'replacement_legacy_app'},document={schema:'soty.field.v2',contexts:[{contextId:'app_context',title:'Apps',x:0,y:0}],shortcuts:[{shortcutId:'stable_legacy_shortcut',entity:first,contextId:'app_context',placement:{kind:'hex',slot:[0,0]}}]};f.save(document);
 const selection={schema:'soty.canvas-page-selection.v1',fieldRevision:f.field().revision,items:[{shortcutId:'stable_legacy_shortcut',entity:first}]},args={expectedAccountId:owner.accountId,requestId:'confirmed_app_selection',expectedRevision:0,enabled:true,audience:'unlisted',selection},put=extra=>f.pages.execute({actor:owner,op:'canvas.pages.put',args:{...args,...extra}});
 const page=put({}).current;assert.deepEqual(page.selection.items[0].entity,first);document.shortcuts[0].entity=next;f.save(document);assert.deepEqual(f.pages.execute({actor:owner,op:'canvas.pages.get',args:{expectedAccountId:owner.accountId}}).page.selection.items[0].entity,first,'GET returns stored consent, not the live shortcut entity');
 assert.throws(()=>put({requestId:'audience_only_legacy',expectedRevision:1,selection:{...page.selection,fieldRevision:f.field().revision}}),{code:'canvas_page_selection_stale'});
 for(const item of [{shortcutId:'stable_legacy_shortcut'},{shortcutId:'stable_legacy_shortcut',entity:next,binding:{providerId:'soty.notes',ref:'arbitrary'}},{shortcutId:'stable_legacy_shortcut',entity:next,expectedAccessRevision:1}])assert.throws(()=>put({requestId:'invalid_legacy',expectedRevision:1,selection:{...selection,fieldRevision:f.field().revision,items:[item]}}));
 assert.equal(put({requestId:'explicit_new_app',expectedRevision:1,selection:{...selection,fieldRevision:f.field().revision,items:[{shortcutId:'stable_legacy_shortcut',entity:next}]}}).current.selection.items[0].entity.id,next.id);
});

test('literal policy-revision check is a necessary control: removing it leaks an explicitly revoked private selection',async t=>{
 const file=fileURLToPath(new URL('../canvas-objects/server.mjs',import.meta.url)),original=readFileSync(file,'utf8'),needle='object.accessRevision===scope.accessRevision&&';assert.ok(original.includes(needle));
 const source=original.replace(needle,'').replaceAll("from './contract.mjs'","from '"+pathToFileURL(fileURLToPath(new URL('../canvas-objects/contract.mjs',import.meta.url))).href+"'").replaceAll("from './storage.mjs'","from '"+pathToFileURL(fileURLToPath(new URL('../canvas-objects/storage.mjs',import.meta.url))).href+"'");
 const mutant=(await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'))).createCanvasObjectsService;
 const oracle=f=>{const object=f.create('revoked');f.place([object]);const page=f.put(0,[object]).current;assert.equal(f.content(page.pageId,object.objectId),'Private Native body revoked');f.objectCall('update',{objectId:object.objectId,expectedRevision:1,requestId:'explicit_revoke',revokePublication:true});assert.equal(f.read(page.pageId).items.length,0,'explicit Only me must immediately remove the selected item');assert.throws(()=>f.content(page.pageId,object.objectId));};
 oracle(fixture(t));assert.throws(()=>oracle(fixture(t,{objectsFactory:mutant})),{code:'ERR_ASSERTION'},'the exact same privacy oracle rejects the deliberately missing accessRevision check');
});

test('format1 cannot silently accept selection while the current reader exposes an honest unavailable capability',t=>{
 const f=fixture(t,{format2:false}),object=f.create('legacy');f.place([object]);assert.equal(f.pages.execute({actor:owner,op:'canvas.pages.get',args:{expectedAccountId:owner.accountId}}).selectionAvailable,false);assert.throws(()=>f.put(0,[object]),{code:'canvas_page_selection_migration_required'});
});

test('owner can always revoke the page with omitted selection even after its field Source fails; old unconditional Source-read control fails',async t=>{
 const closeOracle=(f,privateAudience=false)=>{const object=f.create('close');f.place([object]);const page=f.put(0,[object]).current;f.failFieldSource();assert.doesNotThrow(()=>f.pages.execute({actor:owner,op:'canvas.pages.put',args:{expectedAccountId:owner.accountId,requestId:'close_without_sources',expectedRevision:page.revision,enabled:privateAudience,audience:privateAudience?'private':'unlisted'}}));const current=f.pages.execute({actor:owner,op:'canvas.pages.get',args:{expectedAccountId:owner.accountId}}).page;assert.equal(current.revision,2);assert.deepEqual(current.selection,page.selection);assert.equal(current.enabled,privateAudience);};
 closeOracle(fixture(t));closeOracle(fixture(t),true);
 const file=new URL('./server.mjs',import.meta.url),original=readFileSync(file,'utf8'),needle="if(args.selection!==undefined||args.enabled&&args.audience!=='private')source(actor.accountId,actor);";assert.ok(original.includes(needle));
 const source=original.replace(needle,'source(actor.accountId,actor);').replace(/from '(\.[^']+)'/gu,(_all,specifier)=>"from '"+new URL(specifier,file).href+"'");const mutant=(await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'))).createCanvasPagesService;
 assert.throws(()=>closeOracle(fixture(t,{pagesFactory:mutant})),{code:'ERR_ASSERTION'},'the old unconditional Source dependency must fail the exact owner revocation oracle');
});
