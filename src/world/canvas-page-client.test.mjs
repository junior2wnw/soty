import test from 'node:test';
import assert from 'node:assert/strict';
import {createCanvasPageClient,parseCanvasPageRoute,canvasPageLink} from './canvas-page-client.mjs';
import {readWorkspaceReturn} from './workspace-return.mjs';
const pageId='page_'+'a'.repeat(32),revision='a'.repeat(64),nextRevision='b'.repeat(64);
function chunk(start,count,{nextCursor=null,viewRevision=revision}={}){
 const context={contextId:'context_public',title:'Полотно',x:0,y:0},items=[],shortcuts=[];
 for(let n=start;n<start+count;n++){const entity={kind:'object',id:'object_'+n},shortcutId='shortcut_'+n;
  items.push({shortcutId,entity,presentation:{mask:{shape:'rect'}},protection:{kind:'none'},view:{kind:'note',title:'Visible '+n,providerId:'soty.notes',note:{noteId:'note_'+n},canEdit:false}});
  shortcuts.push({shortcutId,entity,contextId:context.contextId,placement:{kind:'free',x:n*240,y:0,width:220,height:180}});
 }
 return{schema:'soty.canvas-page-view.v1',pageId,viewRevision,banner:{kind:'generic',title:'Полотно'},field:{schema:'soty.field.v2',contexts:count?[context]:[],shortcuts},items,nextCursor};
}
test('page routes and app return locations are exact local destinations with no embedded grant',()=>{
 const route='#space/'+pageId;assert.equal(parseCanvasPageRoute(route),pageId);assert.equal(canvasPageLink(pageId,'https://soty.example'), 'https://soty.example/'+route);
 for(const wrong of [route+'?actor=owner',route+'/other',route+'#more','#space/other','https://soty.example/'+route])assert.equal(parseCanvasPageRoute(wrong),null);
 assert.deepEqual(readWorkspaceReturn({accountId:'viewer',route,scroll:44,token:'discard'},'viewer'),{accountId:'viewer',route,scroll:44});
 assert.equal(readWorkspaceReturn({accountId:'viewer',route},'other'),null);
 assert.equal(readWorkspaceReturn({accountId:'viewer',route:route+'?owner=secret'},'viewer'),null);
});
test('guest preview is an exact permission-reducing route and remains a valid app return',()=>{
 const guest='#space/'+pageId+'?view=public';assert.equal(parseCanvasPageRoute(guest),pageId);
 assert.equal(readWorkspaceReturn({accountId:'owner',route:guest},'owner')?.route,guest);
 for(const tail of ['?view=owner','?view=public&actor=owner','?view=public&view=public','?view=public#extra'])assert.equal(parseCanvasPageRoute('#space/'+pageId+tail),null);
});
test('all 73 placements survive bounded paging with their exact order and geometry',async()=>{
 const calls=[],api={async request(op,args){calls.push({op,args});return args.cursor==='next1'?chunk(32,32,{nextCursor:'next2'}):args.cursor==='next2'?chunk(64,9):chunk(0,32,{nextCursor:'next1'});}};
 const view=await createCanvasPageClient({pageId,api,accountId:'recipient'}).load();assert.equal(calls.length,3);assert.equal(view.items.length,73);assert.equal(view.field.shortcuts.length,73);assert.equal(view.field.contexts.length,1);
 assert.deepEqual(view.items.map(value=>value.entity.id),Array.from({length:73},(_,n)=>'object_'+n));assert.equal(view.field.shortcuts[72].placement.x,17280);
 assert.ok(calls.every(call=>call.op==='canvas.pages.read'&&call.args.expectedAccountId==='recipient'&&!('ownerId'in call.args)));
});
test('an obsolete cursor discards the partial snapshot and restarts at the current visible revision',async()=>{
 let step=0;const api={async request(){step++;if(step===1)return chunk(0,1,{nextCursor:'old'});if(step===2)throw Object.assign(Error('changed'),{code:'canvas_page_cursor_stale'});return chunk(50,1,{viewRevision:nextRevision});}};
 const view=await createCanvasPageClient({pageId,api,accountId:'recipient'}).load();assert.equal(step,3);assert.equal(view.viewRevision,nextRevision);assert.deepEqual(view.items.map(value=>value.entity.id),['object_50']);
});
test('duplicate, reordered or cross-page parts cannot masquerade as a complete field',async()=>{
 const mixed=chunk(1,1);mixed.pageId='page_'+'b'.repeat(32);
 const reordered=chunk(0,2);reordered.items.reverse();
 for(const bad of [mixed,reordered])await assert.rejects(createCanvasPageClient({pageId,api:{request:async()=>bad},accountId:'recipient'}).load(),{code:'canvas_page_response_invalid'});
 let calls=0;const api={request:async()=>{calls++;return chunk(0,1,{nextCursor:'same_cursor'});}};
 await assert.rejects(createCanvasPageClient({pageId,api,accountId:'recipient'}).load(),{code:'canvas_page_response_invalid'});assert.equal(calls,2);
});
test('anonymous reads send no account or cookies and use only the current page object route',async()=>{
 const calls=[],fetch=async(url,options)=>{calls.push({url,options});return new Response(JSON.stringify(url.includes('/content?')?{note:{body:'permitted'}}:chunk(0,1)),{status:200});};
 const client=createCanvasPageClient({pageId,fetch});await client.load();assert.deepEqual(await client.content('object_0','note'),{note:{body:'permitted'}});
 assert.ok(calls.every(value=>value.options.credentials==='omit'&&value.options.cache==='no-store'&&!value.url.includes('owner')));assert.ok(calls[1].url.endsWith('/objects/object_0/content?part=note'));
});
test('a late response after leaving the page is discarded, and signed transport receives the actual abort signal',async()=>{
 let resolve,active=true,context;const controller=new AbortController(),api={request:(_op,_args,options)=>{context=options;return new Promise(done=>{resolve=done;});}};
 const client=createCanvasPageClient({pageId,api,accountId:'recipient',signal:controller.signal,isCurrent:()=>active}),pending=client.load();assert.equal(context.signal,controller.signal);active=false;resolve(chunk(0,1));await assert.rejects(pending,{code:'canvas_page_cancelled'});
});
test('an explicit refresh cancels one load while keeping the page lifetime usable',async()=>{
 const lifetime=new AbortController(),request=new AbortController();let context,resolve;
 const api={request:(_op,_args,options)=>{context=options;return new Promise(done=>{resolve=done;});}},client=createCanvasPageClient({pageId,api,accountId:'viewer',signal:lifetime.signal});
 const pending=client.load(undefined,{signal:request.signal});request.abort();resolve(chunk(0,1));await assert.rejects(pending,{code:'canvas_page_cancelled'});assert.equal(context.signal.aborted,true);assert.equal(lifetime.signal.aborted,false);
});
test('an access loss remains a denial rather than an empty accepted snapshot or an owner retry',async()=>{
 let calls=0;const failure=Object.assign(Error('denied'),{code:'canvas_page_unavailable',status:404}),client=createCanvasPageClient({pageId,api:{request:async()=>{calls++;throw failure;}},accountId:'recipient'});
 await assert.rejects(client.load(),error=>error===failure);assert.equal(calls,1);
});
test('an unchanged live result cannot carry a changed revision or a retained old view',async()=>{
 for(const result of [{schema:'soty.canvas-page-update.v1',changed:false,viewRevision:nextRevision,view:null},{schema:'soty.canvas-page-update.v1',changed:false,viewRevision:revision,view:chunk(0,1)}])
  await assert.rejects(createCanvasPageClient({pageId,api:{request:async()=>result},accountId:'recipient'}).wait(revision),{code:'canvas_page_response_invalid'});
});
