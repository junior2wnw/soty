import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {DatabaseSync} from 'node:sqlite';
import {request as httpRequest} from 'node:http';
import {join} from 'node:path';
import {nativeHttpFixture,nativeIdentity,good} from './support/native-capability-http.mjs';
import {createHttpApp} from '../http-app.js';
import {connectorKey} from '../../modules/apps/server/protocol.mjs';

const id=prefix=>prefix+'_'+randomUUID().replaceAll('-','');
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function until(check){const end=Date.now()+5000;while(!check()){assert.ok(Date.now()<end,'actual HTTP request reached Root');await pause(5);}}
async function fixture(t){
 const f=await nativeHttpFixture(t,{appFactory:(dist,options)=>{const port=new URL(options.connectOrigins[0]).port;return createHttpApp(dist,{...options,appOriginTemplate:`http://{appId}.legacy.localhost:${port}`,namedAppZone:`http://named.localhost:${port}`});}}),owner=nativeIdentity('Private page owner'),viewer=nativeIdentity('Page viewer'),accounts=new Map(),devices=new Map();
 for(const actor of [owner,viewer]){const boot=await f.bootstrap(actor);accounts.set(actor,boot.accountId);devices.set(actor,boot.deviceId);good(await f.call(actor,'world.profile.get'));}
 const call=(actor,op,args={})=>f.call(actor,op,{...(/^(?:canvas|apps|notes|world.field)\./u.test(op)?{expectedAccountId:accounts.get(actor)}:{}),...args});
 const mutate=(actor,op,args={})=>call(actor,op,{requestId:id('request'),...args});
 const createNote=async(body='Public exact Source body')=>({...good(await call(owner,'notes.put',{noteId:id('note'),expectedRevision:0,mutationId:id('note_create'),title:'Real published note',body,items:[],color:'plain',pinned:false,state:'active'})),expectedBody:body});
 const createObject=async(note,extra={})=>good(await mutate(owner,'canvas.objects.create',{binding:{providerId:'soty.notes',ref:note.noteId},presentation:{mask:{shape:'rect'},fontSize:18},visibility:{audience:'public',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'none'},...extra})).current;
 const field=async()=>good(await call(owner,'world.field.get'));
 const save=async document=>good(await mutate(owner,'world.field.put',{expectedRevision:(await field()).revision,document}));
 const doc=entities=>({schema:'soty.field.v2',contexts:[{contextId:'PRIVATE_CONTEXT_ID',title:'Private context name',x:214,y:516},{contextId:'PRIVATE_EMPTY_ID',title:'Private unused context',x:887123,y:991234}],shortcuts:entities.map((entity,index)=>({shortcutId:'PRIVATE_SHORTCUT_'+index,entity,contextId:'PRIVATE_CONTEXT_ID',placement:{kind:'free',x:index*240,y:index*30,width:220,height:180}}))});
 const publish=async(extra={})=>good(await mutate(owner,'canvas.pages.put',{expectedRevision:good(await call(owner,'canvas.pages.get')).page?.revision??0,enabled:true,audience:'public',...extra})).current;
 const pageUrl=pageId=>'/api/canvas/pages/'+pageId,contentUrl=(pageId,objectId)=>pageUrl(pageId)+'/objects/'+objectId+'/content?part=note';
 let updateRequests=0;f.setFront((req,res)=>{if(req.url.includes('/updates?'))updateRequests++;return f.app(req,res);});
 const pending=async(pageId,viewRevision,waitMs=3000)=>{const before=updateRequests;let settled=false;const promise=f.http(pageUrl(pageId)+'/updates?after='+viewRevision+'&waitMs='+waitMs).then(value=>{settled=true;return value;});await until(()=>updateRequests>before);return {promise,get settled(){return settled;}};};
 return {...f,get app(){return f.app;},owner,viewer,accounts,devices,call,mutate,createNote,createObject,field,save,doc,publish,pageUrl,contentUrl,pending};
}

test('Root public page: signed publish→canonical field→anonymous real content; private/hidden/device refs and coordinates never arrive; restart keeps link',{timeout:20000},async t=>{
 const f=await fixture(t),visibleNote=await f.createNote(),privateNote=await f.createNote('PRIVATE_SOURCE_BODY_SENTINEL'),hiddenNote=await f.createNote('HIDDEN_SOURCE_BODY_SENTINEL'),visible=await f.createObject(visibleNote),privateObject=await f.createObject(privateNote,{visibility:{audience:'private',accountIds:[],communityIds:[]}}),hidden=await f.createObject(hiddenNote,{hidden:{owner:false,others:true}});
 await f.save(f.doc([{kind:'object',id:visible.objectId},{kind:'object',id:privateObject.objectId},{kind:'object',id:hidden.objectId},{kind:'device',id:'SECRET_DEVICE_REF'},{kind:'person',id:'SECRET_CONTACT_REF'}]));const page=await f.publish(),response=await f.http(f.pageUrl(page.pageId));assert.equal(response.status,200);assert.equal(response.headers['cache-control'],'no-store');assert.match(response.headers['x-robots-tag'],/noindex/u);
 const view=response.body;assert.equal(view.schema,'soty.canvas-page-view.v1');assert.equal(view.items.length,1);assert.equal(view.items[0].view.note.preview,visibleNote.expectedBody);assert.deepEqual(view.field.shortcuts[0].placement,{kind:'free',x:0,y:0,width:220,height:180});assert.equal(view.field.contexts[0].x,214);assert.equal(view.field.contexts[0].y,516);assert.equal(view.banner.kind,'generic');
 const encoded=JSON.stringify(view);for(const value of [privateObject.objectId,hidden.objectId,'PRIVATE_SOURCE_BODY','HIDDEN_SOURCE_BODY','SECRET_DEVICE_REF','SECRET_CONTACT_REF','PRIVATE_CONTEXT_ID','PRIVATE_SHORTCUT_','Private context name','887123','991234',f.accounts.get(f.owner)])assert.equal(encoded.includes(value),false,value);
 const content=await f.http(f.contentUrl(page.pageId,visible.objectId));assert.equal(content.status,200);assert.equal(content.body.note.body,visibleNote.expectedBody);assert.equal((await f.http(f.contentUrl(page.pageId,privateObject.objectId))).status,404);assert.equal((await f.http(f.contentUrl(page.pageId,hidden.objectId))).status,404);
 assert.equal((await f.call(f.viewer,'notes.get',{noteId:visibleNote.noteId})).ok,false,'page read does not grant direct private Notes API');assert.equal(good(await f.call(f.viewer,'canvas.pages.get')).page,null);
 const copied={accountId:f.accounts.get(f.owner),deviceId:f.devices.get(f.owner)};assert.throws(()=>f.app.locals.canvasPagesService.read({pageId:page.pageId,actor:copied}),{code:'connect_authority_actor_invalid'});
 let captured;f.app.locals.canvasPagesService.withPageObjectRead({pageId:page.pageId,objectId:visible.objectId},context=>{captured=context;assert.throws(()=>f.app.locals.canvasPagesService.withPageObjectAuthority({...context},()=>{}),{code:'canvas_page_authority_required'});return 'current proof';});assert.throws(()=>f.app.locals.canvasPagesService.withPageObjectAuthority(captured,()=>{}),{code:'canvas_page_authority_required'});
 await f.restart({enabled:true});const reopened=await f.http(f.pageUrl(page.pageId));assert.equal(reopened.status,200);assert.equal(reopened.body.viewRevision,view.viewRevision);assert.equal((await f.http(f.contentUrl(page.pageId,visible.objectId))).body.note.body,visibleNote.expectedBody);
});

test('Root public wait ignores private field/Source edits and wakes on real public Source commit; unpublish aborts old wait and content grant',{timeout:20000},async t=>{
 const f=await fixture(t),note=await f.createNote('Public before live edit'),privateNote=await f.createNote('Private before edit'),object=await f.createObject(note),privateObject=await f.createObject(privateNote,{visibility:{audience:'private',accountIds:[],communityIds:[]}});await f.save(f.doc([{kind:'object',id:object.objectId},{kind:'object',id:privateObject.objectId}]));const page=await f.publish(),first=(await f.http(f.pageUrl(page.pageId))).body,pending=await f.pending(page.pageId,first.viewRevision);
 const document=(await f.field()).document;document.shortcuts[1].placement.x=777111;document.contexts[1].title='Another private context';await f.save(document);
 good(await f.call(f.owner,'notes.put',{noteId:privateNote.noteId,expectedRevision:1,mutationId:id('private_edit'),title:'Private new title',body:'Still private Source bytes',items:[],color:'plain',pinned:false,state:'active'}));await pause(70);assert.equal(pending.settled,false);assert.equal((await f.http(f.pageUrl(page.pageId))).body.viewRevision,first.viewRevision);
 good(await f.call(f.owner,'notes.put',{noteId:note.noteId,expectedRevision:1,mutationId:id('public_edit'),title:'Visible current title',body:'Public Source edit without page reload',items:[],color:'plain',pinned:false,state:'active'}));const changed=await pending.promise;assert.equal(changed.status,200);assert.equal(changed.body.changed,true);assert.equal(changed.body.view.items[0].view.note.preview,'Public Source edit without page reload');assert.equal((await f.http(f.contentUrl(page.pageId,object.objectId))).body.note.body,'Public Source edit without page reload');
 const waiting=await f.pending(page.pageId,changed.body.viewRevision);await f.publish({enabled:false});const closed=await waiting.promise;assert.equal(closed.status,404);assert.equal(closed.body.code,'canvas_page_unavailable');assert.equal((await f.http(f.contentUrl(page.pageId,object.objectId))).status,404);assert.equal((await f.http(f.pageUrl(page.pageId))).status,404);
});

test('Root public query and content membership bounds reject spoofed actor/ref/part, cross-page object and stale cursor',{timeout:20000},async t=>{
 const f=await fixture(t),note=await f.createNote(),one=await f.createObject(note),two=await f.createObject(note),notPlaced=await f.createObject(note);await f.save(f.doc([{kind:'object',id:one.objectId},{kind:'object',id:two.objectId}]));const page=await f.publish(),base=f.pageUrl(page.pageId),first=await f.http(base+'?limit=1');assert.equal(first.status,200);assert.ok(first.body.nextCursor);
 for(const suffix of ['?limit=33','?limit=-1','?actor=owner','?limit=1&limit=2','?cursor=not_a_valid_cursor'])assert.equal((await f.http(base+suffix)).status,400,suffix);
 for(const suffix of ['/updates?after=abc','/updates?after='+first.body.viewRevision+'&waitMs=15001','/updates?after='+first.body.viewRevision+'&actor=owner'])assert.equal((await f.http(base+suffix)).status,400,suffix);
 assert.equal((await f.http(f.contentUrl(page.pageId,notPlaced.objectId))).status,404);assert.equal((await f.http(f.contentUrl(page.pageId,one.objectId)+'&ownerId='+f.accounts.get(f.owner))).status,400);assert.equal((await f.http(base+'/objects/'+one.objectId+'/content?part=note&index=-1')).status,400);
 const document=(await f.field()).document;document.shortcuts[0].placement.x=32;await f.save(document);const stale=await f.http(base+'?cursor='+encodeURIComponent(first.body.nextCursor));assert.equal(stale.status,409);assert.equal(stale.body.code,'canvas_page_cursor_stale');
 const immediate=await f.http(base+'/updates?after='+(await f.http(base)).body.viewRevision+'&waitMs=0');assert.equal(immediate.status,200);assert.equal(immediate.body.changed,false);
});

test('Root legacy app ref uses real known-unlisted public Source, then retirement removes its metadata and geometry without catalog substitution',{timeout:20000},async t=>{
 const f=await fixture(t),accountId=f.accounts.get(f.owner),identity={linkId:'pages_fixture_link',hostDeviceId:'pages_fixture_host',connectorId:'pages_fixture_connector'},db=new DatabaseSync(join(f.directory,'apps','registry.sqlite'));
 try{db.prepare('INSERT INTO app_devices VALUES(?,?,?,?,?)').run(connectorKey(identity),accountId,JSON.stringify(identity),'Root fixture owned connector',1);}finally{db.close();}
 const app=good(await f.call(f.owner,'apps.register',{hostDeviceId:identity.hostDeviceId,connectorId:identity.connectorId,name:'Real legacy page app',port:30228,entryPath:'/current'})).app,claim=good(await f.call(f.owner,'apps.domains.claim',{appId:app.id,slug:'page-app',requestId:id('alias'),expectedDomainsRevision:good(await f.call(f.owner,'apps.domains.get',{appId:app.id})).revision})).receipt;
 await f.save(f.doc([{kind:'app',id:app.id}]));const page=await f.publish(),base=f.pageUrl(page.pageId);assert.equal((await f.http(base)).body.items.length,0,'private app ref is denied even on public page');
 const publication=good(await f.call(f.owner,'apps.publication.get',{appId:app.id}));good(await f.call(f.owner,'apps.publication.update',{appId:app.id,requestId:id('public_unlisted'),expectedPolicyEpoch:publication.policyEpoch,expectedTargetRevision:publication.activeTargetRevision,launchPolicy:'anyone',listed:false,activeDomainIds:[claim.domainId],exposureAck:{scope:'whole-port',targetRevision:publication.target.revision,targetDigest:publication.target.digest,profile:publication.target.profile}}));
 const view=(await f.http(base)).body;assert.equal(view.items.length,1);assert.equal(view.items[0].view.kind,'app');assert.equal(view.items[0].view.app.name,'Real legacy page app');assert.equal(view.items[0].view.entry.domainId,claim.domainId);assert.equal(view.items[0].view.entry.path,'/current');assert.equal(view.items[0].view.runtimeAllowed,false);assert.equal(good(await f.call(f.viewer,'apps.catalog')).apps.some(value=>value.id===app.id),false);
 const wait=await f.pending(page.pageId,view.viewRevision);good(await f.call(f.owner,'apps.domains.retire',{appId:app.id,domainId:claim.domainId,requestId:id('retire'),expectedDomainsRevision:good(await f.call(f.owner,'apps.domains.get',{appId:app.id})).revision}));const removed=await wait.promise;assert.equal(removed.body.changed,true);assert.equal(removed.body.view.items.length,0);assert.equal(removed.body.view.field.shortcuts.length,0);assert.equal(JSON.stringify(removed.body.view).includes(app.id),false);
});

test('Root signed page reads use the actual visitor ACL, retain private owner preview, and retire held content on access removal',{timeout:20000},async t=>{
 const f=await fixture(t),card=good(await f.call(f.viewer,'card.get')),contact=good(await f.call(f.owner,'contacts.request',{cardId:card.cardId}));good(await f.call(f.viewer,'contacts.accept',{requestId:contact.requestId}));
 const note=await f.createNote('Recipient-bound actual Source body'),object=await f.createObject(note,{visibility:{audience:'restricted',accountIds:[f.accounts.get(f.viewer)],communityIds:[]}});await f.save(f.doc([{kind:'object',id:object.objectId}]));const page=await f.publish();
 assert.equal((await f.http(f.pageUrl(page.pageId))).body.items.length,0,'anonymous must not gain the signed visitor grant');
 const view=good(await f.call(f.viewer,'canvas.pages.read',{pageId:page.pageId}));assert.equal(view.items.length,1);assert.equal(view.items[0].entity.id,object.objectId);
 assert.equal(good(await f.call(f.viewer,'canvas.pages.content',{pageId:page.pageId,objectId:object.objectId,part:'note'})).note.body,note.expectedBody);
 assert.equal((await f.call(f.viewer,'canvas.pages.read',{pageId:page.pageId,expectedAccountId:f.accounts.get(f.owner)})).error.code,'canvas_page_account_changed');
 assert.equal((await f.call(f.viewer,'canvas.pages.content',{pageId:page.pageId,objectId:object.objectId,part:'note',ownerId:f.accounts.get(f.owner)})).error.code,'canvas_page_arguments_invalid');
 // Observe the actual signed RPC arrival, then wait for a committed Source change.
 let waiting=0;f.setFront((req,res)=>{if(req.url==='/api/connect/rpc'&&req.method==='POST')waiting++;return f.app(req,res);});
 const before=waiting,pending=f.call(f.viewer,'canvas.pages.wait',{pageId:page.pageId,after:view.viewRevision,waitMs:3000});await until(()=>waiting>=before+2);await pause(20);
 good(await f.mutate(f.owner,'canvas.objects.update',{objectId:object.objectId,expectedRevision:object.revision,presentation:object.presentation,visibility:{audience:'private',accountIds:[],communityIds:[]},hidden:object.hidden}));
 const removed=good(await pending);assert.equal(removed.changed,true);assert.equal(removed.view.items.length,0);assert.equal(JSON.stringify(removed.view).includes(object.objectId),false);
 assert.equal((await f.call(f.viewer,'canvas.pages.content',{pageId:page.pageId,objectId:object.objectId,part:'note'})).error.code,'canvas_object_unavailable');
 await f.publish({audience:'private'});assert.equal((await f.call(f.viewer,'canvas.pages.read',{pageId:page.pageId})).error.code,'canvas_page_unavailable');assert.equal((await f.http(f.pageUrl(page.pageId))).status,404);
 const own=good(await f.call(f.owner,'canvas.pages.read',{pageId:page.pageId}));assert.equal(own.items.length,1);assert.equal(good(await f.call(f.owner,'canvas.pages.content',{pageId:page.pageId,objectId:object.objectId,part:'note'})).note.body,note.expectedBody);
});

test('Root signed page wait is revoked with its actual enrolled device and never returns retained private Source bytes',{timeout:20000},async t=>{
 const f=await fixture(t),phone=nativeIdentity('Synthetic second page device'),started=good(await f.call(phone,'enrollment.start',{label:phone.label,encryptionPublicJwk:phone.encryptionPublicJwk}));
 good(await f.call(f.owner,'enrollment.approve',{requestId:started.requestId,wrappedKey:{schema:'test.encrypted',ciphertext:'synthetic'}}));const enrolled=good(await f.call(phone,'enrollment.finish',{requestId:started.requestId,expectedAccountId:f.accounts.get(f.owner)}));f.accounts.set(phone,enrolled.accountId);f.devices.set(phone,enrolled.deviceId);
 const note=await f.createNote('OWNER_PRIVATE_PAGE_SENTINEL'),object=await f.createObject(note,{visibility:{audience:'private',accountIds:[],communityIds:[]}});await f.save(f.doc([{kind:'object',id:object.objectId}]));const page=await f.publish({audience:'private'}),view=good(await f.call(phone,'canvas.pages.read',{pageId:page.pageId}));
 let calls=0;f.setFront((req,res)=>{if(req.url==='/api/connect/rpc'&&req.method==='POST')calls++;return f.app(req,res);});const before=calls,pending=f.call(phone,'canvas.pages.wait',{pageId:page.pageId,after:view.viewRevision,waitMs:5000});await until(()=>calls>=before+2);await pause(20);
 good(await f.call(f.owner,'device.revoke',{deviceId:enrolled.deviceId}));const lost=await pending;assert.equal(lost.ok,false);assert.ok(['canvas_page_authentication_required','device_revoked'].includes(lost.error.code),lost.error.code);assert.equal(JSON.stringify(lost).includes('OWNER_PRIVATE_PAGE_SENTINEL'),false);
 assert.equal((await f.call(phone,'canvas.pages.content',{pageId:page.pageId,objectId:object.objectId,part:'note'})).error.code,'device_revoked');
});

test('Root signed page waits leave no RAM subscription after the actual client HTTP connection closes',{timeout:20000},async t=>{
 const f=await fixture(t),note=await f.createNote(),object=await f.createObject(note);await f.save(f.doc([{kind:'object',id:object.objectId}]));const page=await f.publish(),view=good(await f.call(f.owner,'canvas.pages.read',{pageId:page.pageId}));
 for(let n=0;n<6;n++){
  const proof=await f.proof(f.owner,'canvas.pages.wait',{expectedAccountId:f.accounts.get(f.owner),pageId:page.pageId,after:view.viewRevision,waitMs:15000});
  const request=httpRequest(new URL('/api/connect/rpc',f.origin),{method:'POST',headers:{origin:f.origin,'content-type':'application/json'}});request.on('error',()=>{});request.end(JSON.stringify({protocol:1,...proof}));
  await until(()=>f.app.locals.canvasPagesHttp.stats().waiting===1);request.destroy();await until(()=>f.app.locals.canvasPagesHttp.stats().waiting===0);assert.equal(f.app.locals.canvasPagesHttp.stats().scopes,0);
 }
 assert.equal(good(await f.call(f.owner,'canvas.pages.wait',{pageId:page.pageId,after:view.viewRevision,waitMs:0})).changed,false);
});
