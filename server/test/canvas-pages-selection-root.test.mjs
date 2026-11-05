import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,createHash} from 'node:crypto';
import {DatabaseSync} from 'node:sqlite';
import {join} from 'node:path';
import {nativeHttpFixture,nativeIdentity,good} from './support/native-capability-http.mjs';
import {createHttpApp} from '../http-app.js';
import {pngFixture} from '../../modules/world/test/avatar-fixtures.mjs';
import {materialManifest} from '../../modules/materials/contract.mjs';

const id=prefix=>prefix+'_'+randomUUID().replaceAll('-','');
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
const policy=audience=>({audience,accountIds:[],communityIds:[]});
async function fixture(t){
 const factory=allowCanvasPageSelectionMigration=>(dist,options)=>createHttpApp(dist,{...options,allowCanvasPageSelectionMigration});
 const f=await nativeHttpFixture(t,{appFactory:factory(true)}),owner=nativeIdentity('Selected page owner'),visitor=nativeIdentity('Selected page visitor'),accounts=new Map();
 for(const actor of [owner,visitor]){accounts.set(actor,(await f.bootstrap(actor)).accountId);good(await f.call(actor,'world.profile.get'));}
 const call=(actor,op,args={})=>f.call(actor,op,{expectedAccountId:accounts.get(actor),...args});
 const mutate=(actor,op,args={})=>call(actor,op,{requestId:id('request'),...args});
 const note=async(body,title='Selected private note')=>good(await call(owner,'notes.put',{noteId:id('note'),expectedRevision:0,mutationId:id('note_create'),title,body,items:[],color:'plain',pinned:false,state:'active'}));
 const object=async(binding,extra={})=>good(await mutate(owner,'canvas.objects.create',{binding,presentation:{mask:{shape:'rect'},fontSize:17},visibility:policy('private'),hidden:{owner:false,others:false},protection:{kind:'none'},...extra})).current;
 const field=async()=>good(await call(owner,'world.field.get'));
 const save=async document=>good(await mutate(owner,'world.field.put',{expectedRevision:(await field()).revision,document}));
 const place=async objects=>save({schema:'soty.field.v2',contexts:[{contextId:'PRIVATE_CONTEXT',title:'Never disclose own title',x:140,y:280}],shortcuts:objects.map((o,index)=>({shortcutId:'PRIVATE_SHORTCUT_'+o.objectId,entity:{kind:'object',id:o.objectId},contextId:'PRIVATE_CONTEXT',placement:{kind:'free',x:index*250,y:20,width:220,height:180}}))});
 const publish=async(objects,extra={})=>good(await mutate(owner,'canvas.pages.put',{expectedRevision:good(await call(owner,'canvas.pages.get')).page?.revision??0,enabled:true,audience:'unlisted',selection:{schema:'soty.canvas-page-selection.v1',fieldRevision:(await field()).revision,items:objects.map(o=>({shortcutId:'PRIVATE_SHORTCUT_'+o.objectId,entity:{kind:'object',id:o.objectId},expectedAccessRevision:o.accessRevision}))},...extra})).current;
 const url=pageId=>'/api/canvas/pages/'+pageId,contentUrl=(pageId,objectId,part='note')=>url(pageId)+'/objects/'+objectId+'/content?part='+part;
 let updates=0;f.setFront((req,res)=>{if(req.url.includes('/updates?'))updates++;return f.app(req,res);});
 const pending=async(pageId,after)=>{const previous=updates;let settled=false;const promise=f.http(url(pageId)+'/updates?after='+after+'&waitMs=3000').then(v=>{settled=true;return v;});const end=Date.now()+3000;while(updates===previous){assert.ok(Date.now()<end);await pause(5);}return{promise,get settled(){return settled;}};};
 return{...f,get app(){return f.app;},owner,visitor,accounts,call,mutate,note,object,field,save,place,publish,url,contentUrl,pending,restartFlagOff:()=>f.restart({enabled:true,appFactory:factory(false)})};
}

test('Root explicit private selection: original signed owner→canonical shortcut→anonymous/actual visitor Source read→restart flag-off; direct APIs stay private',{timeout:25000},async t=>{
 const f=await fixture(t),selectedNote=await f.note('ACTUAL_SELECTED_PRIVATE_BODY'),otherNote=await f.note('UNSELECTED_PRIVATE_BODY_SECRET'),selected=await f.object({providerId:'soty.notes',ref:selectedNote.noteId}),other=await f.object({providerId:'soty.notes',ref:otherNote.noteId});await f.place([selected,other]);const original=await f.field(),page=await f.publish([selected]);
 const response=await f.http(f.url(page.pageId));assert.equal(response.status,200);assert.equal(response.headers['cache-control'],'no-store');assert.match(response.headers['x-robots-tag'],/noindex/u);assert.equal(response.body.items.length,1);assert.equal(response.body.items[0].view.note.preview,'ACTUAL_SELECTED_PRIVATE_BODY');
 for(const secret of [other.objectId,otherNote.noteId,'UNSELECTED_PRIVATE_BODY_SECRET','PRIVATE_SHORTCUT_','PRIVATE_CONTEXT','Never disclose own title'])assert.equal(JSON.stringify(response.body).includes(secret),false,secret);
 assert.equal((await f.http(f.contentUrl(page.pageId,selected.objectId))).body.note.body,'ACTUAL_SELECTED_PRIVATE_BODY');assert.equal((await f.http(f.contentUrl(page.pageId,other.objectId))).status,404);
 assert.equal(good(await f.call(f.visitor,'canvas.pages.content',{pageId:page.pageId,objectId:selected.objectId,part:'note'})).note.body,'ACTUAL_SELECTED_PRIVATE_BODY');assert.equal((await f.call(f.visitor,'notes.get',{noteId:selectedNote.noteId})).ok,false);assert.equal(good(await f.call(f.visitor,'canvas.objects.project',{objectIds:[selected.objectId]})).items[0].available,false);assert.deepEqual(await f.field(),original);
 const own=good(await f.call(f.owner,'canvas.objects.get',{objectId:selected.objectId})).object;assert.equal(own.visibility.audience,'private');assert.equal(own.accessRevision,1);
 await f.restartFlagOff();assert.equal(good(await f.call(f.owner,'canvas.pages.get')).selectionAvailable,true,'known physical2 is usable without opting into another migration');assert.equal((await f.http(f.url(page.pageId))).body.viewRevision,response.body.viewRevision);assert.equal((await f.http(f.contentUrl(page.pageId,selected.objectId))).body.note.body,'ACTUAL_SELECTED_PRIVATE_BODY');
 for(const [file,version]of[['objects.sqlite',2],['pages.sqlite',2]])assert.equal(f.sql(join(f.directory,'canvas',file),db=>db.prepare('PRAGMA user_version').get().user_version),version);
});

test('Root selected live Source edits wake anonymous wait; unselected activity stays quiet; font preserves grant and explicit Only me immediately revokes it',{timeout:25000},async t=>{
 const f=await fixture(t),visible=await f.note('Selected before'),privateNote=await f.note('Unselected before'),object=await f.object({providerId:'soty.notes',ref:visible.noteId}),privateObject=await f.object({providerId:'soty.notes',ref:privateNote.noteId});await f.place([object,privateObject]);const page=await f.publish([object]),first=(await f.http(f.url(page.pageId))).body,wait=await f.pending(page.pageId,first.viewRevision);
 good(await f.call(f.owner,'notes.put',{noteId:privateNote.noteId,expectedRevision:1,mutationId:id('private_edit'),title:'Still private',body:'NEW_UNSELECTED_SECRET',items:[],color:'plain',pinned:false,state:'active'}));const doc=(await f.field()).document;doc.shortcuts[1].placement.x=772299;await f.save(doc);await pause(50);assert.equal(wait.settled,false);assert.equal((await f.http(f.url(page.pageId))).body.viewRevision,first.viewRevision);
 good(await f.call(f.owner,'notes.put',{noteId:visible.noteId,expectedRevision:1,mutationId:id('live_edit'),title:'Current selected',body:'LIVE_SELECTED_NATIVE_BODY',items:[],color:'plain',pinned:false,state:'active'}));const changed=await wait.promise;assert.equal(changed.status,200);assert.equal(changed.body.changed,true);assert.equal(changed.body.view.items[0].view.note.preview,'LIVE_SELECTED_NATIVE_BODY');
 const camera=await f.field(),styled=good(await f.mutate(f.owner,'canvas.objects.update',{objectId:object.objectId,expectedRevision:1,presentation:{mask:{shape:'ellipse'},fontSize:31}})).current;assert.equal(styled.accessRevision,object.accessRevision);assert.equal((await f.http(f.url(page.pageId))).body.items[0].presentation.fontSize,31);assert.equal((await f.http(f.contentUrl(page.pageId,object.objectId))).body.note.body,'LIVE_SELECTED_NATIVE_BODY');assert.deepEqual(await f.field(),camera);
 const waiting=await f.pending(page.pageId,(await f.http(f.url(page.pageId))).body.viewRevision);good(await f.mutate(f.owner,'canvas.objects.update',{objectId:object.objectId,expectedRevision:styled.revision,visibility:policy('private'),revokePublication:true}));const removed=await waiting.promise;assert.equal(removed.body.changed,true);assert.equal(removed.body.view.items.length,0);assert.equal((await f.http(f.contentUrl(page.pageId,object.objectId))).status,404);assert.equal(good(await f.call(f.owner,'notes.get',{noteId:visible.noteId})).note.body,'LIVE_SELECTED_NATIVE_BODY');assert.deepEqual(await f.field(),camera);
});

test('Root selected encrypted Source has a real lock with Page-only canUnlock=false, while ordinary owner unlock and style retain usable bytes',{timeout:30000},async t=>{
 const f=await fixture(t),password='Synthetic-page-password-e81d',secret='ACTUAL_ENCRYPTED_SELECTED_BODY',source=good(await f.mutate(f.owner,'canvas.vault.create',{password,content:{contentType:'application/vnd.soty.note+json',bytesBase64:Buffer.from(JSON.stringify({title:'Sealed',body:secret})).toString('base64url')}})),object=await f.object(source.binding,{protection:{kind:'password'}});await f.place([object]);const page=await f.publish([object]);
 const anonymous=(await f.http(f.url(page.pageId))).body.items[0],visitor=good(await f.call(f.visitor,'canvas.pages.read',{pageId:page.pageId})).items[0];for(const item of [anonymous,visitor]){assert.equal(item.view.state,'locked');assert.equal(item.canUnlock,false);assert.equal(JSON.stringify(item).includes(secret),false);}
 assert.equal((await f.http(f.contentUrl(page.pageId,object.objectId,'vault'))).status,403);assert.equal((await f.call(f.visitor,'canvas.vault.unlock',{objectId:object.objectId,password})).error.code,'canvas_object_unavailable','Page-only unlock remains explicitly unsupported rather than granting ordinary object ACL');
 good(await f.call(f.owner,'canvas.vault.unlock',{objectId:object.objectId,password}));const bytes=()=>f.call(f.owner,'canvas.pages.content',{pageId:page.pageId,objectId:object.objectId,part:'vault'});assert.equal(JSON.parse(Buffer.from(good(await bytes()).bytesBase64,'base64url')).body,secret);
 good(await f.mutate(f.owner,'canvas.objects.update',{objectId:object.objectId,expectedRevision:1,presentation:{mask:{shape:'ellipse'},fontSize:25}}));assert.equal(JSON.parse(Buffer.from(good(await bytes()).bytesBase64,'base64url')).body,secret,'presentation keeps the existing owner permit while exact current read frame stays fresh');assert.equal((await f.http(f.url(page.pageId))).body.items[0].canUnlock,false);
 await f.publish([]);assert.equal((await f.http(f.url(page.pageId))).body.items.length,0);assert.equal((await f.http(f.contentUrl(page.pageId,object.objectId,'vault'))).status,404);
});

test('Root selected real Materials chunks stay exact, Source trash removes the geometry, and closing a page retires its pending read',{timeout:25000},async t=>{
 const f=await fixture(t),bytes=pngFixture(),sha=value=>createHash('sha256').update(value).digest('hex'),materialId=id('material'),chunks=[{bytes:bytes.length,sha256:sha(bytes)}];
 good(await f.call(f.owner,'materials.begin',{materialId,mutationId:id('begin'),name:'Selected photo.png',kind:'image',mediaType:'image/png',size:bytes.length,fileSha256:sha(bytes)}));
 good(await f.call(f.owner,'materials.chunk',{materialId,index:0,sha256:sha(bytes),data:bytes.toString('base64')}));const committed=good(await f.call(f.owner,'materials.commit',{materialId,mutationId:id('commit'),manifestSha256:sha(materialManifest(bytes.length,chunks))}));
 const object=await f.object({providerId:'soty.materials',ref:materialId});await f.place([object]);const page=await f.publish([object]),view=(await f.http(f.url(page.pageId))).body;assert.equal(view.items[0].view.kind,'image');
 assert.equal((await f.call(f.visitor,'materials.read',{materialId,index:0})).ok,false);const read=await f.http(f.contentUrl(page.pageId,object.objectId,'chunk')+'&index=0');assert.equal(read.status,200);assert.equal(sha(Buffer.from(read.body.data,'base64')),sha(bytes));
 const waiting=await f.pending(page.pageId,view.viewRevision);good(await f.call(f.owner,'materials.trash',{materialId,mutationId:id('trash'),expectedRevision:committed.material.revision}));const removed=await waiting.promise;assert.equal(removed.body.changed,true);assert.equal(removed.body.view.items.length,0);assert.equal(removed.body.view.field.shortcuts.length,0);assert.equal((await f.http(f.contentUrl(page.pageId,object.objectId,'chunk')+'&index=0')).status,404);
 const closedWait=await f.pending(page.pageId,removed.body.viewRevision);await f.publish([],{enabled:false});assert.equal((await closedWait.promise).status,404);assert.equal((await f.http(f.url(page.pageId))).status,404);
});

test('Root selected Native frame discards read bytes when the actual Source changes before output; broken Source header control leaks and fails that oracle',{timeout:25000},async t=>{
 const f=await fixture(t),note=await f.note('NO_BYTES_AFTER_SOURCE_REVOKE'),object=await f.object({providerId:'soty.notes',ref:note.noteId});await f.place([object]);const page=await f.publish([object]),pages=f.app.locals.canvasPagesService,notes=f.app.locals.notesService;
 const readDuringRevoke=()=>pages.withPageObjectRead({pageId:page.pageId,objectId:object.objectId},pageContext=>pages.withPageObjectAuthority(pageContext,context=>{
  const result=notes.canvas.readNote({context});const db=new DatabaseSync(f.notesFile);try{db.prepare("UPDATE notes SET state='trashed',revision=revision+1 WHERE account_id=? AND id=?").run(f.accounts.get(f.owner),note.noteId);}finally{db.close();}return result;
 }));
 let returned;const privacyOracle=()=>assert.throws(()=>{returned=readDuringRevoke();},'a current Native Source header fence must discard the retained bytes');privacyOracle();assert.equal(returned,undefined);assert.equal((await f.http(f.contentUrl(page.pageId,object.objectId))).status,404);
 good(await f.call(f.owner,'notes.put',{noteId:note.noteId,expectedRevision:2,mutationId:id('restore'),title:'Same Native source',body:'NO_BYTES_AFTER_SOURCE_REVOKE',items:[],color:'plain',pinned:false,state:'active'}));
 f.app.locals.canvasObjectsService.unregisterProvider('soty.notes');f.app.locals.canvasObjectsService.registerProvider({providerId:'soty.notes',withBindingAuthority(_request,callback){return callback({allowed:true,protection:{kind:'none'}});},project(){return {kind:'note'};}});
 assert.throws(privacyOracle,{code:'ERR_ASSERTION'},'the exact same read-during-revocation oracle must reject a provider without a fresh Source fence');assert.equal(returned.note.body,'NO_BYTES_AFTER_SOURCE_REVOKE');
});

test('Root captured selection GET never silently publishes a different private Source after canonical shortcut rebinding, even with the same policy revision',{timeout:25000},async t=>{
 const f=await fixture(t),one=await f.note('CONFIRMED_ROOT_PAGE_SOURCE'),two=await f.note('UNCONFIRMED_ROOT_PAGE_SOURCE'),first=await f.object({providerId:'soty.notes',ref:one.noteId}),second=await f.object({providerId:'soty.notes',ref:two.noteId});assert.equal(first.accessRevision,second.accessRevision);await f.place([first]);const page=await f.publish([first]),captured=good(await f.call(f.owner,'canvas.pages.get')).page.selection;
 const document=(await f.field()).document;document.shortcuts[0].entity={kind:'object',id:second.objectId};await f.save(document);const currentField=await f.field();assert.equal((await f.http(f.url(page.pageId))).body.items.length,0);
 const update={expectedRevision:page.revision,enabled:true,audience:'public',selection:{...captured,fieldRevision:currentField.revision}},rejected=await f.mutate(f.owner,'canvas.pages.put',update);assert.equal(rejected.ok,false);assert.equal(rejected.error.code,'canvas_page_selection_stale');assert.deepEqual(good(await f.call(f.owner,'canvas.pages.get')).page.selection,captured);assert.equal((await f.http(f.url(page.pageId))).body.items.length,0);
 const explicit=good(await f.mutate(f.owner,'canvas.pages.put',{...update,selection:{...update.selection,items:[{shortcutId:document.shortcuts[0].shortcutId,entity:{kind:'object',id:second.objectId},expectedAccessRevision:second.accessRevision}]}}));assert.equal(explicit.current.revision,2);assert.equal((await f.http(f.contentUrl(page.pageId,second.objectId))).body.note.body,'UNCONFIRMED_ROOT_PAGE_SOURCE','a separate explicit new choice is supported');assert.equal(good(await f.call(f.owner,'canvas.objects.get',{objectId:second.objectId})).object.visibility.audience,'private');
});
