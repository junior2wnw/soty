import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import * as geometry from './unified-field-layout.mjs';
import * as camera from './unified-field-camera.mjs';
import {fieldPresentationClip} from './unified-field-presentation.mjs';
import {fieldEntityKey} from '../../modules/field/contract.mjs';
import {resolveAppArt} from './app-art.mjs';
import {createNotesService} from '../../modules/notes/server/index.mjs';
import {createNoteSession,blankNote,noteErrorCode} from '../../modules/notes/browser/state.mjs';
import {readNoteWithCache} from '../../modules/notes/browser/cache.mjs';
import * as nativeState from './native-field-objects-state.mjs';
import {nativeFieldSafeArea} from './native-field-safe-area.mjs';
import {createCanvasVaultClient,CANVAS_VAULT_NOTE_TYPE} from './canvas-vault-client.mjs';
import {closeAppRuntime} from './app-runtime-close.mjs';

// Actual content bridge + native note renderer/session + field render lifecycle.
// This DOM/RPC port checks identity, plaintext removal and API scope; it does not
// claim real browser iframe isolation, persistence, media decode or visual QA.
const source=readFileSync(new URL('./canvas-field-content.ts',import.meta.url),'utf8');
const nativeSource=readFileSync(new URL('./native-field-objects.ts',import.meta.url),'utf8');
const overviewSource=readFileSync(new URL('./field-object-overview.ts',import.meta.url),'utf8');
const fieldSource=readFileSync(new URL('./unified-field.ts',import.meta.url),'utf8');
const compile=value=>ts.transpileModule(value,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText
  .replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'');
const parsed=ts.createSourceFile('field.ts',fieldSource,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS),printer=ts.createPrinter();
const named=(nodes,name)=>{const node=nodes.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text===name);assert.ok(node,`Actual production ${name}`);return node;};
const field=named(parsed.statements,'createUnifiedField');
const renderCode=compile([named(parsed.statements,'compactOverviewTitles'),...['renderNode','render','focusPreview','inspectShortcut'].map(name=>named(field.body.statements,name))]
  .map(node=>printer.printNode(ts.EmitHint.Unspecified,node,parsed)).join('\n'));
const until=async predicate=>{for(let index=0;index<100;index++){if(predicate())return;await new Promise(resolve=>setImmediate(resolve));}assert.fail('Expected content lifecycle did not settle');};
const flatten=node=>[node,...node.children.flatMap(flatten)];
const byClass=(node,name)=>flatten(node).find(node=>node.className?.split(/\s+/).includes(name));
const actor={accountId:'content_owner',deviceId:'content_device'},objectId='object_content',ref={kind:'object',id:objectId};

class NodePort{
  constructor(tag='div',className='',text=''){
    this.tag=tag;this.tagName=tag.toUpperCase();this.className=className;this.children=[];this.dataset={};this.attributes=new Map();this.listeners=new Map();
    this.style={setProperty(name,value){this[name]=value;}};this.value='';this._text=text;this.parentElement=null;this.hidden=false;this.disabled=false;
  }
  get textContent(){return this._text+this.children.map(value=>value.textContent).join('');}
  set textContent(value){this._text=String(value);for(const child of this.children)child.parentElement=null;this.children=[];}
  get classList(){return{add:(...names)=>{this.className+=' '+names.join(' ');},remove:name=>{this.className=this.className.split(/\s+/).filter(value=>value!==name).join(' ');},
    toggle:(name,on)=>{const values=new Set(this.className.split(/\s+/));on?values.add(name):values.delete(name);this.className=[...values].join(' ');}};}
  append(...nodes){for(const node of nodes){node.remove();node.parentElement=this;this.children.push(node);}}
  replaceChildren(...nodes){this.textContent='';this.append(...nodes);}
  remove(){if(this.parentElement)this.parentElement.children=this.parentElement.children.filter(value=>value!==this);this.parentElement=null;}
  contains(node){return node===this||this.children.some(child=>child.contains(node));}
  setAttribute(key,value){this.attributes.set(key,String(value));}
  getAttribute(key){return this.attributes.get(key)??null;}
  removeAttribute(key){this.attributes.delete(key);if(key==='src')this.src='';}
  querySelector(selector){const choices=selector.split(',');return flatten(this).slice(1).find(node=>choices.some(value=>value.startsWith('.')?node.className.split(/\s+/).includes(value.slice(1)):node.tag===value))??null;}
  closest(){return null;}
  addEventListener(type,handler){if(!this.listeners.has(type))this.listeners.set(type,[]);this.listeners.get(type).push(handler);}
  removeEventListener(type,handler){const list=this.listeners.get(type)??[];this.listeners.set(type,list.filter(value=>value!==handler));}
  emit(type,event={}){for(const handler of this.listeners.get(type)??[])handler({target:this,currentTarget:this,preventDefault(){},...event});}
  focus(){this.ownerDocument.activeElement=this;}
  pause(){this.paused=true;}load(){}async play(){this.paused=false;}
}
function draftPort(){const values=new Map();return{async put(record){values.set(record.branchId,structuredClone(record));},async list(){return[...values.values()].map(value=>structuredClone(value));},
  async remove(noteId,branchId,savedAt){const value=values.get(branchId);if(value?.note.noteId===noteId&&(savedAt===undefined||value.savedAt===savedAt))values.delete(branchId);}};}
function cachePort(){const values=new Map();return{async capture(){return'epoch';},async isCurrent(){return true;},async remember(note){values.set(note.noteId,{note:structuredClone(note),verifiedAt:Date.now()});return true;},
  async get(id){return values.get(id)??null;},async list(){return[...values.values()];},async remove(id){values.delete(id);},async clear(){values.clear();}};}

function harness(t,{code=source,overviewCode=overviewSource,vault,vaultViewFactory,sealingViewFactory,controls=true}={}){
  const service=createNotesService({databasePath:':memory:',projectId:'content-test'});t.after(()=>service.close());
  const store=draftPort(),cache=cachePort(),calls=[],factories=[],stages=[],urls=[],revoked=[],dialogs=[];let projection=null,current=true;
  const document={activeElement:null,createElement:tag=>node(tag),createElementNS:(_,tag)=>node(tag)};
  function node(tag='div',className='',text=''){const value=new NodePort(tag,className,text);value.ownerDocument=document;return value;}
  const plane=node(),viewport=node(),doc={schema:'soty.field.v2',contexts:[{contextId:'personal',title:'Личное',x:0,y:0}],
    shortcuts:[{shortcutId:'placement_content',entity:ref,contextId:'personal',placement:{kind:'free',x:0,y:0,width:340,height:300}}]};
  const entities=[{entity:ref,title:'Private source name',source:'owner',presentation:{shape:'rect'}}];
  const data={layout:geometry.layoutUnifiedField(doc,entities)},view={width:1280,height:800},position={x:0,y:0,scale:1};
  const env={exports:{},...geometry,...camera,...nativeState,nativeFieldSafeArea,createCanvasVaultClient,CANVAS_VAULT_NOTE_TYPE,createCanvasVaultView(...args){if(vaultViewFactory)return vaultViewFactory(...args);throw Error('Protected lifecycle is exercised by its dedicated renderer fixture');},createCanvasSealingView(...args){if(sealingViewFactory)return sealingViewFactory(...args);throw Error('Pending lifecycle is exercised by its dedicated renderer fixture');},createNoteSession,blankNote,noteErrorCode,readNoteWithCache,fieldPresentationClip,fieldEntityKey,resolveAppArt,
    // This existing renderer/authority suite has an explicit no-plaintext-cache
    // transition port. Native cache protocol and reveal failure are tested below
    // and in the real Notes/Object/Vault composition fixture.
    createNativeCacheTransition:()=>({remember(){},observeProtected(){},snapshot:()=>({applicable:false,device:'complete'}),async resumeAfterUnlock(){return{applicable:false,device:'complete'};},dispose(){}}),
    createDraftStore:()=>store,createNoteCache:()=>cache,readMaterialBlob:async()=>{throw Error('Unexpected media load in note/widget lifecycle test');},
    structuredClone,AbortController,queueMicrotask,Promise,performance,clearTimeout,Date,
    URL:{createObjectURL:blob=>{const url='blob:content-'+urls.length;urls.push({url,blob});return url;},revokeObjectURL:url=>revoked.push(url)},
    document,HTMLElement:NodePort,window:{innerWidth:view.width},destroyed:false,inPointerFrame:false,activeLayout:()=>data.layout,closeAppRuntime,
    button(label,_icon,className,action){const value=node('button',className,label);value.addEventListener('click',action);return value;},
    createDialog(title,onClose){const element=node('dialog'),body=node();element.open=true;element.append(body);
      const dialog={element,body,title,close(){if(!element.open)return;element.open=false;element.remove();onClose();}};dialogs.push(dialog);return dialog;},
    gesture:null,pinch:null,lastInspection:null,suppressClickUntil:0,spacePanning:false,liveScene:{contexts:[],shortcuts:[]},
    viewportSize:()=>view,camera:()=>position,cameraFit:{mine:'context'},contextFocus:{mine:'personal'},mode:'mine',arranging:false,moving:null,contextPreview:null,
    viewport,plane,signal:new AbortController().signal,metrics:geometry.UNIFIED_FIELD_METRICS,SVG_NS:'svg',index:geometry.createFieldSpatialIndex(data.layout.nodes),
    nodeElements:new Map(),contextElements:new Map(),inspectElements:new Map(),objectViews:new Map(),resizeElements:new Map(),selectedId:'',persistence:'saved',fieldDocument:doc,
    available:id=>data.layout.nodes.find(node=>node.shortcutId===id),activeNode:id=>data.layout.nodes.find(node=>node.shortcutId===id),
    focusContext(){},focusNode(){},remember(){},schedule(){},emit(){},
    el:node,icon:(_,className='sw-icon')=>node('svg',className),hexFrame:()=>node('svg','uf-hex-frame'),
    initials:title=>title.slice(0,2),localImage:()=>null,safeImageUrl:()=>null,nounCount:(count,word)=>`${count} ${word}`,
    tools:node(),moveHint:node(),readySettled:true,
  };
  const context=vm.createContext(env);
  vm.runInContext(compile(overviewCode),context);
  vm.runInContext(compile(nativeSource)+'\nexports.native=createNativeFieldObjects;',context);
  const actualNative=env.exports.native;
  env.createNativeFieldObjects=options=>{factories.push(options);return actualNative({...options,store,cache});};
  const api={async request(operation,args){calls.push({operation,args:structuredClone(args)});
    if(operation==='canvas.objects.content'){
      assert.equal(args.expectedAccountId,actor.accountId);assert.equal(args.objectId,objectId);assert.equal(args.part,'note');
      assert.ok(current&&projection?.available&&projection?.protection?.kind==='none','The RPC port also rejects a revoked source');
      return service.execute({op:'notes.get',args:{expectedAccountId:actor.accountId,noteId:projection.view.note.noteId},actor});
    }
    if(operation==='notes.put')return service.execute({op:operation,args:structuredClone(args),actor});
    assert.fail(`Unexpected unscoped request ${operation}`);
  }};
  const mountApp=(host,value,isCurrent)=>{
    const frame=node('iframe');frame.src=value.entry.origin+value.entry.path;host.append(frame);
    const state={host,frame,isCurrent,updates:[],modes:[],disposed:0,flushes:0,dirty:false};stages.push(state);
    return{ready:Promise.resolve(),updateApp:app=>state.updates.push(structuredClone(app)),setWidgetMode:mode=>state.modes.push(mode),mountRuntimeControls(_target,owner){state.runtimeOwner=owner;return controls;},
      async flush(){state.flushes++;if(state.flushWait)await state.flushWait;},hasUnsavedChanges:()=>state.dirty,hasUnconfirmedWork:()=>state.unconfirmed===true,
      async renewHostSession(reason){(state.renewals??=[]).push(reason);return true;},
      dispose(){state.disposed++;frame.removeAttribute('src');frame.remove();}};
  };
  vm.runInContext(compile(code)+'\nexports.content=createCanvasFieldContent;',context);
  const content=env.exports.content({api,accountId:actor.accountId,isCurrent:()=>current,projection:()=>projection,mountApp,...(vault?{vault}:{})});
  env.options={createObject:content.createObject,objectKey:content.objectKey,resolveArt:()=>null};
  vm.runInContext(renderCode+'\nexports.render=render;',context);
  t.after(()=>content.dispose());
  return{content,env,plane,data,calls,factories,store,stages,urls,revoked,service,render:env.exports.render,document,dialogs,
    closeStage(index=0,{confirm=false,confirmation}={}){const state=stages[index],frame=state.frame;
      return state.runtimeOwner.requestClose({isCurrent:()=>state.isCurrent()&&state.host.querySelector('iframe')===frame,
        async confirmUnknown(isCurrent){state.confirmations=(state.confirmations??0)+1;return !!(confirmation?await confirmation:confirm)&&isCurrent();}});},
    get projection(){return projection;},set projection(value){projection=value;},changeAccount(){current=false;},
    async note(body='Private body'){
      const note=blankNote();await service.execute({op:'notes.put',actor,args:{expectedAccountId:actor.accountId,noteId:note.noteId,mutationId:crypto.randomUUID(),expectedRevision:0,title:'Записка',body,items:[],color:'plain',pinned:false,state:'active'}});
      return(await service.execute({op:'notes.get',actor,args:{expectedAccountId:actor.accountId,noteId:note.noteId}})).note;
    }};
}
const noteProjection=note=>({objectId,available:true,revision:1,protection:{kind:'none'},presentation:{mask:{shape:'rect'},fontSize:16},
  view:{schema:'soty.canvas-native-view.v1',providerId:'soty.notes',kind:'note',canEdit:true,title:note.title,note:{...note,body:undefined}}});
const widgetProjection=()=>({objectId,available:true,revision:1,protection:{kind:'none'},view:{schema:'soty.canvas-widget-view.v1',kind:'widget',widgetId:'widget_content',instanceId:'instance_content',state:'active',
  app:{appId:'app-'+ 'a'.repeat(32),name:'Live calculator',status:'ready',ownerAccountId:'foreign_creator'},entry:{appId:'app-'+ 'a'.repeat(32),domainId:'dom_'+ 'b'.repeat(32),origin:'https://example.invalid',path:'/calculator?mode=compact'},
  source:{revision:7,digest:'digest_7',profile:'profile_7'},expectedSource:{revision:7,digest:'digest_7',profile:'profile_7'},runtimeAllowed:true,canRefresh:false}});

function activatedCleanWidget(t,{ignoreRetention=false}={}){
  const h=harness(t);h.projection=widgetProjection();h.render();
  const provider=h.env.objectViews.get('placement_content').view,frame=h.stages[0].frame;
  frame.volatileState={filter:'only mine',scrollTop:173,selection:[2,8]};
  provider.setPresentation('interactive');provider.setPresentation('glance');
  if(ignoreRetention)provider.retainWhileHidden=()=>false;
  h.env.camera().x=100000;h.env.selectedId='';h.document.activeElement=null;h.render();
  return{h,provider,frame};
}
test('explicitly activated clean app retains SAME frame/RAM beyond viewport without claiming dirty work',t=>{
  const {h,provider,frame}=activatedCleanWidget(t);
  assert.equal(provider.hasUnsavedChanges(),false);assert.equal(provider.hasUnconfirmedWork(),false);
  assert.equal(h.env.objectViews.get('placement_content')?.view,provider,'Viewport movement must retain activated work');
  assert.equal(h.stages[0].disposed,0);assert.equal(h.stages.length,1);
  h.env.camera().x=0;h.render();
  assert.equal(h.stages[0].frame,frame);assert.equal(h.stages.length,1);
  assert.deepEqual(frame.volatileState,{filter:'only mine',scrollTop:173,selection:[2,8]});
});
test('old offscreen cleanup control fails the SAME activated-clean continuity criterion',t=>{
  const {h,provider}=activatedCleanWidget(t,{ignoreRetention:true});
  assert.throws(()=>assert.equal(h.env.objectViews.get('placement_content')?.view,provider),/undefined/);
  assert.equal(h.stages[0].disposed,1);
});
test('retained offscreen work cannot override a fresh source revoke or account loss',t=>{
  for(const mode of ['revoke','account']){
    const {h,frame}=activatedCleanWidget(t);
    if(mode==='revoke')h.projection={...h.projection,view:{...h.projection.view,runtimeAllowed:false}};else h.changeAccount();
    h.render();assert.equal(h.stages[0].disposed,1);assert.equal(frame.src,'');assert.equal(h.env.objectViews.size,0);
  }
});
test('runtime admission is bounded; explicit release frees one slot without evicting active siblings',async t=>{
  const h=harness(t);h.projection=widgetProjection();const node=h.data.layout.nodes[0],views=[];
  for(let i=0;i<9;i++){const view=h.content.createObject({...node,shortcutId:'admission_'+i});views.push(view);view.setVisible(true);view.setPresentation('interactive');}
  assert.equal(h.stages.length,8,'The ninth activation must not silently discard another working session');
  assert.ok(views[8].element.textContent.includes('Открыто много окон'));
  assert.equal(byClass(views[0].element,'cf-widget-close'),undefined,'No permanent destructive × beside the existing menu');
  h.closeStage();await until(()=>h.stages[0].disposed===1);
  views[8].activate();assert.equal(h.stages.length,9);
  assert.equal(h.stages.slice(1,8).reduce((n,s)=>n+s.disposed,0),0);
});
test('Keep and save failure do not free capacity; confirmed Close releases exactly its own slot',async t=>{
  const h=harness(t);h.projection=widgetProjection();const node=h.data.layout.nodes[0],views=[];
  for(let i=0;i<9;i++){const view=h.content.createObject({...node,shortcutId:'close_capacity_'+i});views.push(view);view.setVisible(true);view.setPresentation('interactive');}
  h.stages[0].unconfirmed=true;await h.closeStage();views[8].activate();assert.equal(h.stages.length,8);assert.equal(h.stages[0].flushes,0);
  h.stages[0].dirty=true;await h.closeStage(0,{confirm:true});views[8].activate();assert.equal(h.stages.length,8);assert.equal(h.stages[0].disposed,0);
  h.stages[0].dirty=false;await h.closeStage(0,{confirm:true});views[8].activate();assert.equal(h.stages.length,9);assert.equal(h.stages[0].disposed,1);
  assert.equal(h.stages.slice(1,8).reduce((n,state)=>n+state.disposed,0),0);
});
test('Close stays closed across visibility/presentation updates; only explicit reopen creates a new runtime',async t=>{
  const h=harness(t);h.projection=widgetProjection();h.render();const view=h.env.objectViews.get('placement_content').view;
  view.setPresentation('interactive');h.closeStage();await until(()=>h.stages[0].disposed===1);
  for(let i=0;i<4;i++){view.setVisible(false);view.setVisible(true);view.setPresentation('interactive');}
  assert.equal(h.stages.length,1,'Render must not reopen an explicitly closed window');
  view.activate();assert.equal(h.stages.length,2);assert.equal(h.stages[1].disposed,0);
});
async function closeAcrossVirtualization(t,{code=source}={}){
  const h=harness(t,{code});h.projection=widgetProjection();h.render();const view=h.env.objectViews.get('placement_content').view;
  view.setPresentation('interactive');h.closeStage();await until(()=>h.stages[0].disposed===1);
  h.env.selectedId='';h.document.activeElement=null;h.env.camera().x=100000;h.render();assert.equal(h.env.objectViews.size,0,'A closed lightweight preview may virtualize');
  h.env.camera().x=0;h.render();assert.equal(h.stages.length,1,'no implicit reopened stage after provider recreation');
  const replacement=h.env.objectViews.get('placement_content').view;replacement.activate();assert.equal(h.stages.length,2);
}
test('Close survives actual engine offscreen disposal/recreation without reopening or consuming another slot',closeAcrossVirtualization);
test('removing the Close-intent memory fails that SAME actual-engine lifecycle criterion',async t=>{
  const anchor='closedPlacements.has(node.shortcutId)';assert.equal(source.split(anchor).length,2);
  await assert.rejects(()=>closeAcrossVirtualization(t,{code:source.replace(anchor,'false')}),error=>error.code==='ERR_ASSERTION'&&/no implicit reopened stage/.test(error.message));
});
test('only explicit activation can yield an untouched clean preview, and idle render never loops eviction',t=>{
  const h=harness(t);h.projection=widgetProjection();const node=h.data.layout.nodes[0],views=[];
  for(let i=0;i<9;i++){const view=h.content.createObject({...node,shortcutId:'passive_'+i});views.push(view);view.setVisible(true);}
  assert.equal(h.stages.length,8);h.stages[0].dirty=true;h.stages[1].unconfirmed=true;
  views[8].setPresentation('interactive');assert.equal(h.stages.length,9);
  assert.equal(h.stages[0].disposed,0);assert.equal(h.stages[1].disposed,0);assert.equal(h.stages[2].disposed,1);
  for(let i=0;i<5;i++){views[2].setVisible(true);views[2].setPresentation('glance');}
  assert.equal(h.stages.length,9,'Passive retries cannot evict another preview in a render loop');
});
test('unknown work needs a visible explicit confirmation; unsuccessful save retains the current window',async t=>{
  const h=harness(t);h.projection=widgetProjection();h.render();const view=h.env.objectViews.get('placement_content').view;
  view.setPresentation('interactive');const notice=byClass(view.element,'cf-widget-notice');
  h.stages[0].unconfirmed=true;await h.closeStage();
  assert.equal(h.stages[0].disposed,0);assert.equal(h.stages[0].flushes,0);assert.equal(h.stages[0].confirmations,1,'Actual controls own the visible warning and Keep default');
  h.stages[0].dirty=true;h.closeStage(0,{confirm:true});await until(()=>h.stages[0].flushes===1);
  assert.equal(h.stages[0].disposed,0);assert.match(notice.textContent,/Сохранение не подтверждено/);
});
test('leaving the close confirmation cancels it without flushing or releasing the window',async t=>{
  const h=harness(t);h.projection=widgetProjection();h.render();const view=h.env.objectViews.get('placement_content').view;
  view.setPresentation('interactive');h.stages[0].unconfirmed=true;let answer;const confirmation=new Promise(resolve=>{answer=resolve;});
  const pending=h.closeStage(0,{confirmation});view.setPresentation('glance');view.setPresentation('interactive');answer(true);await pending;await h.closeStage();
  await new Promise(resolve=>setImmediate(resolve));assert.equal(h.stages[0].flushes,0);assert.equal(h.stages[0].disposed,0);
  assert.equal(h.stages[0].confirmations,2,'A new close requires its own explicit confirmation, never a blind second click');
});
test('legacy stage without attached menu retains named close and whole Keep/discard dialog',async t=>{
  const h=harness(t,{controls:false});h.projection=widgetProjection();h.render();const view=h.env.objectViews.get('placement_content').view;
  view.setPresentation('interactive');const close=byClass(view.element,'cf-widget-legacy-close');assert.equal(close.hidden,false);
  assert.equal(close.textContent,'Закрыть окно');h.stages[0].unconfirmed=true;close.emit('click');
  let prompt=h.dialogs.at(-1);assert.equal(prompt.element.open,true);assert.equal(h.document.activeElement.textContent,'Оставить открытым');
  assert.match(prompt.body.textContent,/может пропасть/);prompt.close();await new Promise(resolve=>setImmediate(resolve));
  assert.equal(h.stages[0].flushes,0);assert.equal(h.stages[0].disposed,0);
  close.emit('click');prompt=h.dialogs.at(-1);const discard=flatten(prompt.body).find(value=>value.textContent==='Закрыть без сохранения');
  discard.emit('click');await until(()=>h.stages[0].disposed===1);assert.equal(prompt.element.open,false);
  assert.equal(h.stages[0].flushes,1);assert.equal(h.env.fieldDocument.shortcuts.length,1,'Close never removes the canonical placement');
});
test('attached menu hides the legacy destructive control without losing explicit close',async t=>{
  const h=harness(t);h.projection=widgetProjection();h.render();const view=h.env.objectViews.get('placement_content').view;view.setPresentation('interactive');
  assert.equal(byClass(view.element,'cf-widget-legacy-close').hidden,true);assert.equal(typeof h.stages[0].runtimeOwner.requestClose,'function');
  await h.closeStage();assert.equal(h.stages[0].disposed,1);
});
test('held save cannot close a replaced frame, foreign account or newly revoked Source',async t=>{
  for(const mode of ['frame','account','revoke','source','edit','reject']){
    const h=harness(t);h.projection=widgetProjection();h.render();const view=h.env.objectViews.get('placement_content').view;view.setPresentation('interactive');
    let finish,fail;h.stages[0].flushWait=new Promise((resolve,reject)=>{finish=resolve;fail=reject;});const pending=h.closeStage();
    assert.equal(h.stages[0].flushes,1);
    if(mode==='frame'){h.stages[0].frame.remove();const replacement=h.document.createElement('iframe');h.stages[0].host.append(replacement);}
    if(mode==='account')h.changeAccount();if(mode==='revoke')h.projection={...h.projection,view:{...h.projection.view,runtimeAllowed:false}};
    if(mode==='source')h.projection={...h.projection,view:{...h.projection.view,source:{...h.projection.view.source,revision:8},expectedSource:{...h.projection.view.expectedSource,revision:8}}};
    if(mode==='edit')h.stages[0].dirty=true;if(mode==='reject')fail(Error('synthetic save unavailable'));else finish();await pending;
    assert.equal(h.stages[0].disposed,0,mode);
    if(mode==='edit'||mode==='reject')assert.match(byClass(view.element,'cf-widget-notice').textContent,/Сохранение не подтверждено/u,mode);
    if(mode==='revoke'||mode==='account'||mode==='source'){h.render();assert.equal(h.stages[0].disposed,1,mode+' authority cleanup still applies');}
  }
});
test('pending legacy confirmation is synchronously cleaned on owner dispose without save',async t=>{
  const h=harness(t,{controls:false});h.projection=widgetProjection();h.render();const view=h.env.objectViews.get('placement_content').view;view.setPresentation('interactive');
  h.stages[0].unconfirmed=true;byClass(view.element,'cf-widget-legacy-close').emit('click');const dialog=h.dialogs.at(-1);assert.equal(dialog.element.open,true);
  view.dispose();assert.equal(dialog.element.open,false);await new Promise(resolve=>setImmediate(resolve));assert.equal(h.stages[0].flushes,0);assert.equal(h.stages[0].disposed,1);
});
test('legacy same-iframe load retires pending discard consent and its temporary listener',async t=>{
  const h=harness(t,{controls:false});h.projection=widgetProjection();h.render();const view=h.env.objectViews.get('placement_content').view;view.setPresentation('interactive');
  h.stages[0].unconfirmed=true;byClass(view.element,'cf-widget-legacy-close').emit('click');const dialog=h.dialogs.at(-1);
  h.stages[0].frame.emit('load');assert.equal(dialog.element.open,false);await new Promise(resolve=>setImmediate(resolve));
  assert.equal(h.stages[0].flushes,0);assert.equal(h.stages[0].disposed,0);assert.equal(h.stages[0].frame.listeners.get('load').length,0);
});
async function reopenDuringClose(t,{code=source}={}){
  const h=harness(t,{code});h.projection=widgetProjection();h.render();const view=h.env.objectViews.get('placement_content').view;
  view.setPresentation('interactive');let release;h.stages[0].flushWait=new Promise(resolve=>{release=resolve;});
  h.closeStage();assert.equal(h.stages[0].flushes,1);
  view.setPresentation('glance');view.setPresentation('interactive');h.stages[0].frame.volatileState={filter:'new user work'};
  release();await new Promise(resolve=>setImmediate(resolve));
  assert.equal(h.stages[0].disposed,0,'stale Close must not destroy newly resumed work');assert.equal(h.stages.length,1);
  assert.deepEqual(h.stages[0].frame.volatileState,{filter:'new user work'});
}
test('held Close save cannot destroy the same runtime after a newer explicit open intention',reopenDuringClose);
test('removing Close-intent epoch fails the SAME held-save/new-work criterion',async t=>{
  const anchor='intent!==closeEpoch||';assert.equal(source.split(anchor).length,2);
  await assert.rejects(()=>reopenDuringClose(t,{code:source.replace(anchor,'')}),error=>error.code==='ERR_ASSERTION'&&/stale Close must not destroy/.test(error.message));
});
test('reset renews only existing runtime owners, never mounts a dormant or closed preview',async t=>{
  const h=harness(t);h.projection=widgetProjection();const node=h.data.layout.nodes[0],view=h.content.createObject(node);
  await h.content.renewHostSessions('reset');assert.equal(h.stages.length,0);
  view.setVisible(true);view.setPresentation('interactive');view.setVisible(false);
  h.content.createObject({...node,shortcutId:'still_dormant'});
  await h.content.renewHostSessions('reset');assert.equal(h.stages.length,1);assert.deepEqual(h.stages[0].renewals,['reset']);
  h.closeStage();await until(()=>h.stages[0].disposed===1);
  await h.content.renewHostSessions('reset');assert.equal(h.stages.length,1);assert.deepEqual(h.stages[0].renewals,['reset']);
  h.changeAccount();await h.content.renewHostSessions('reset');assert.equal(h.stages.length,1);
});

async function nativeRevokeScenario(t,code=source){
  const h=harness(t,{code}),note=await h.note('Secret initial body');h.projection=noteProjection(note);h.render();
  await until(()=>byClass(h.plane,'nf-body')?.disabled===false);
  const editor=byClass(h.plane,'nf-body'),host=h.env.nodeElements.get('placement_content');
  assert.equal(editor.value,'Secret initial body');assert.equal(h.calls[0].operation,'canvas.objects.content');
  editor.focus();editor.value='Secret unsaved local thought';editor.selectionStart=9;editor.emit('input');
  h.projection={...h.projection,revision:2,view:{...h.projection.view,title:'Renamed remotely',note:{...h.projection.view.note,revision:2,preview:'Fresh preview'}}};h.render();
  assert.equal(h.env.nodeElements.get('placement_content'),host);assert.equal(byClass(h.plane,'nf-body'),editor);assert.equal(editor.selectionStart,9);assert.equal(editor.value,'Secret unsaved local thought');
  h.projection={...h.projection,available:false};h.render();
  assert.equal(editor.value,'','Revocation scrubs even a focused dirty textarea immediately');
  assert.equal(byClass(h.plane,'nf-body'),undefined,'No old native plaintext remains attached');
  assert.equal(h.env.objectViews.size,0);assert.equal(h.env.nodeElements.get('placement_content').getAttribute('aria-label'),'Объект недоступен');
  await new Promise(resolve=>setImmediate(resolve));
  assert.ok((await h.store.list()).some(record=>record.note.body==='Secret unsaved local thought'),'The authorized private draft remains in its account store');
  await assert.rejects(h.factories[0].api.request('notes.get',{noteId:note.noteId}),error=>error.code==='canvas_access_denied');
  return h;
}
test('native scope forwards current object IDs, preserves a focused caret on rename and scrubs a dirty source on revoke',nativeRevokeScenario);

test('public note scope stays read-only and never forwards an attempted private Notes mutation',async t=>{
  const h=harness(t),note=await h.note();h.projection=noteProjection(note);h.projection.view.canEdit=false;h.render();
  await until(()=>byClass(h.plane,'nf-body')?.disabled===false);const editor=byClass(h.plane,'nf-body');assert.equal(editor.readOnly,true);
  editor.value='Forbidden viewer edit';editor.emit('input');await h.env.objectViews.get('placement_content').view.flush();
  assert.equal(h.calls.filter(call=>call.operation==='notes.put').length,0);assert.equal((await h.store.list()).length,0);
  await assert.rejects(h.factories[0].api.request('notes.put',{noteId:note.noteId}),error=>error.code==='canvas_access_denied');
});

function widgetRenameScenario(t,code=source){
  const h=harness(t,{code});h.projection=widgetProjection();h.render();const stage=h.stages[0],host=h.env.nodeElements.get('placement_content');
  assert.equal(h.stages.length,1);assert.equal(h.plane.querySelector('iframe'),stage.frame);assert.equal(stage.frame.src,'https://example.invalid/calculator?mode=compact');
  stage.dirty=true;h.projection={...h.projection,revision:20,view:{...h.projection.view,app:{...h.projection.view.app,name:'Новое имя без перезапуска'}}};h.render();
  assert.equal(h.stages.length,1,'App metadata rename must preserve the existing mounted frame');assert.equal(stage.disposed,0);
  assert.equal(h.env.nodeElements.get('placement_content'),host);assert.equal(h.plane.querySelector('iframe'),stage.frame);assert.equal(stage.updates.at(-1).name,'Новое имя без перезапуска');
  h.projection={...h.projection,available:false};h.render();assert.equal(stage.disposed,1);assert.equal(stage.frame.src,'');assert.equal(h.plane.querySelector('iframe'),null);assert.equal(stage.isCurrent(),false);
  assert.equal(h.env.nodeElements.get('placement_content').getAttribute('aria-label'),'Объект недоступен');return h;
}
test('a live foreign widget keeps its actual frame on metadata rename and removes it on revoke',widgetRenameScenario);
function widgetModeScenario(t,code=source){
 const h=harness(t,{code});h.projection=widgetProjection();h.render();const stage=h.stages[0],view=[...h.env.objectViews.values()][0].view;
 assert.equal(stage.modes.at(-1),'glance','A compact widget never advertises inactive Source commands');
 view.setPresentation('interactive');assert.equal(stage.modes.at(-1),'interactive','Readable focus must enable the real Source actions');
 h.data.layout.nodes[0].width=700;h.render();assert.equal(h.stages.length,1);assert.equal(h.plane.querySelector('iframe'),stage.frame);
 view.setPresentation('glance');assert.equal(stage.modes.at(-1),'glance');assert.equal(stage.disposed,0);
}
test('explicit widget mode follows readable focus while resize retains the actual Source frame',widgetModeScenario);
test('negative control: permanently passive Source actions fail the unchanged readable-focus criterion',t=>{
 const expression="stage?.setWidgetMode?.(value==='interactive'?'interactive':'glance')";assert.equal(source.split(expression).length,2);
 assert.throws(()=>widgetModeScenario(t,source.replace(expression,"stage?.setWidgetMode?.('glance')")),/Readable focus must enable the real Source actions/u);
});
test('an unmodified legacy widget is retained offscreen without claiming an unsaved edit or blocking the shell',t=>{
 const h=harness(t);h.projection=widgetProjection();h.render();const stage=h.stages[0];stage.unconfirmed=true;
 const object=h.env.objectViews.get('placement_content').view;
 assert.equal(object.hasUnsavedChanges(),false,'Unknown save protocol is not an unsaved edit');assert.equal(object.hasUnconfirmedWork(),true);
 h.env.index={query:()=>[]};h.render();assert.equal(stage.disposed,0);assert.equal(h.plane.querySelector('iframe'),stage.frame,'Unknown legacy state is retained during pan');
 stage.dirty=true;assert.equal(object.hasUnsavedChanges(),true);stage.dirty=false;
 h.projection={...h.projection,available:false};h.render();assert.equal(stage.disposed,1);assert.equal(h.plane.querySelector('iframe'),null,'Retention never overrides revocation');
});
test('widget source-pin change remounts one frame; runtime permission loss and account change remove the current one',t=>{
  const h=harness(t);h.projection=widgetProjection();h.render();
  h.projection.view.expectedSource={revision:8,digest:'digest_8',profile:'profile_8'};h.render();assert.equal(h.stages.length,2);assert.equal(h.stages[0].disposed,1);
  h.projection.view.runtimeAllowed=false;h.render();assert.equal(h.stages[1].disposed,1);assert.equal(h.plane.querySelector('iframe'),null);
  h.projection.view.runtimeAllowed=true;h.render();assert.equal(h.stages.length,3);h.changeAccount();h.render();assert.equal(h.stages[2].disposed,1);assert.equal(h.plane.querySelector('iframe'),null);
});

test('negative control: a bridge trusting a stale unavailable projection is caught by dirty-plaintext removal',async t=>{
  const from="return value?.available&&value.protection?.kind==='none'&&value.view?value:null;";
  assert.equal(source.split(from).length,2);const broken=source.replace(from,'return value?.view?value:null;');
  await assert.rejects(nativeRevokeScenario(t,broken),/Revocation scrubs even a focused dirty textarea/);
});
test('negative control: putting app display name into runtime identity is caught by the same frame-preservation criterion',t=>{
  const from='${widget.expectedSource.revision}:${widget.expectedSource.digest}';assert.equal(source.split(from).length,2);
  const broken=source.replace(from,from+':${widget.app.name}');
  assert.throws(()=>widgetRenameScenario(t,broken),/App metadata rename must preserve the existing mounted frame/);
});

// An explicit client event transport; actual Vault crypto/ACL/TTL are exercised
// separately. Here the observable boundary is the live frame before Root fetch.
function vaultEvents(){let unlocked=true,generation=1;const listeners=new Set();return{
  isUnlocked:()=>unlocked,generation:()=>generation,subscribe(listener){listeners.add(listener);return()=>listeners.delete(listener);},
  invalidate(id,reason='relock'){unlocked=false;generation++;for(const listener of listeners)listener(id,reason);},
  reopen(id){unlocked=true;generation++;for(const listener of listeners)listener(id,'unlocked');},
};}
function protectedWidgetScenario(t,code=source){
  const vault=vaultEvents(),h=harness(t,{code,vault});h.projection={...widgetProjection(),protection:{kind:'password'}};h.render();
  const original=h.stages[0];assert.equal(h.plane.querySelector('iframe'),original.frame);
  vault.invalidate(objectId);
  assert.equal(original.disposed,1,'Relock removes the existing protected frame before a delayed metadata refresh');
  assert.equal(h.plane.querySelector('iframe'),null);assert.equal(original.isCurrent(),false);
  vault.reopen(objectId);h.render();assert.equal(h.stages.length,2,'A later unlock never reuses an already disposed provider with the same runtime pin');
  assert.equal(h.plane.querySelector('iframe'),h.stages[1].frame);
}
test('protected widget relock scrubs immediately and a new unlock generation mounts a live fresh frame',protectedWidgetScenario);

function overviewRelockScenario(t,code=source,overviewCode=overviewSource){
 const vault=vaultEvents(),h=harness(t,{code,vault,overviewCode});h.projection={...widgetProjection(),protection:{kind:'password'}};h.render();
 const view=h.env.objectViews.get('placement_content').view,face=view.overview,mark=byClass(face,'cf-widget-overview-mark'),name=byClass(face,'cf-widget-overview-title');
 view.setPresentation('overview');assert.equal(name.textContent,h.projection.view.app.name);assert.ok(face.parentElement,'The identity is a sibling of the Source mask');
 vault.invalidate(objectId);
 assert.equal(face.parentElement,null,'Relock removes sibling identity BEFORE any project/render');
 assert.equal(name.textContent,'','A held face cannot retain the private app association');assert.equal(mark.textContent,'');
 assert.equal(h.stages[0].disposed,1);
}
test('a protected overview face is synchronously scrubbed with its Source on relock',overviewRelockScenario);
test('negative control: root-only cleanup fails the unchanged sibling relock criterion',t=>{
 const anchor='identity.dispose();';assert.equal(source.split(anchor).length,2);
 assert.throws(()=>overviewRelockScenario(t,source.replace(anchor,'identity.clear();')),/Relock removes sibling identity BEFORE any project\/render/);
});
test('negative control: removed but uncleared sibling retains a private association and fails the same scrub criterion',t=>{
 const anchor='clear();dead=true;element.remove();';assert.equal(overviewSource.split(anchor).length,2);
 assert.throws(()=>overviewRelockScenario(t,source,overviewSource.replace(anchor,'dead=true;element.remove();')),/A held face cannot retain the private app association/);
});
test('initial far metadata defers Source launch; rename updates identity; source pin, runtime and account loss scrub held faces',t=>{
 const h=harness(t);h.projection=widgetProjection();Object.assign(h.data.layout.nodes[0],{width:640,height:420});h.env.camera().scale=.2;
 h.env.options.readableObjectFocus=true;Object.assign(h.env,{objectFocus:null,objectFocusButtons:new Map(),focusBar:new NodePort(),focusBackdrop:new NodePort(),focusTitle:new NodePort(),releaseFocusControls(){}});h.render();
 const view=h.env.objectViews.get('placement_content').view,face=view.overview;
 assert.equal(h.stages.length,0,'Far identity does not require booting an iframe');assert.equal(face.dataset.presentation,'overview');
 h.projection.view.app.name='Текущее разрешённое имя';h.render();assert.equal(byClass(face,'cf-widget-overview-title').textContent,'Текущее разрешённое имя');
 h.env.camera().scale=.51;h.render();assert.equal(h.stages.length,1);assert.equal(face.dataset.presentation,'glance');
 h.projection.view.expectedSource={revision:8,digest:'digest_8',profile:'profile_8'};h.render();assert.equal(face.parentElement,null);assert.equal(face.textContent,'');
 const next=h.env.objectViews.get('placement_content').view.overview;h.projection.view.runtimeAllowed=false;h.render();assert.equal(next.parentElement,null);assert.equal(next.textContent,'');
 h.projection.view.runtimeAllowed=true;h.render();const last=h.env.objectViews.get('placement_content').view.overview;h.changeAccount();h.render();assert.equal(last.parentElement,null);assert.equal(last.textContent,'');
});
test('a legacy host without readable focus retains its previous Source glance instead of an inactive overview',t=>{
 const h=harness(t);h.projection=widgetProjection();h.env.camera().scale=.2;h.render();
 assert.equal(h.stages.length,1);assert.equal(h.env.objectViews.get('placement_content').view.overview.dataset.presentation,'glance');
});
test('widget shape/resize updates the whole safe viewport without remounting its frame',t=>{
  const h=harness(t);h.projection=widgetProjection();h.data.layout.nodes[0].presentation={shape:'ellipse'};h.render();
  const first=h.stages[0],root=h.plane.querySelector('.cf-widget'),inset=nativeFieldSafeArea({shape:'ellipse'},340,300);
  for(const[side,value]of Object.entries(inset))assert.equal(root.style[`--cf-safe-${side}`],`${value}px`);
  h.data.layout.nodes[0].width=700;h.data.layout.nodes[0].height=180;h.render();assert.equal(h.stages.length,1);assert.equal(h.plane.querySelector('iframe'),first.frame);
  const next=nativeFieldSafeArea({shape:'ellipse'},700,180);for(const[side,value]of Object.entries(next))assert.equal(root.style[`--cf-safe-${side}`],`${value}px`);
});
test('negative control: waiting for metadata instead of handling relock immediately fails the same frame-scrub oracle',t=>{
  const line='for(const[view,id]of [...protectedWidgets])if(id===objectId)view.dispose();';assert.equal(source.split(line).length,2);
  assert.throws(()=>protectedWidgetScenario(t,source.replace(line,'')),/Relock removes the existing protected frame/);
});
function protectedNativeRoute(t,kind,code=source){
  const vault=vaultEvents();let mounts=0,captured;
  const h=harness(t,{code,vault,vaultViewFactory:(_node,options)=>{
    mounts++;captured=options.projection();return{element:new NodePort('article','protected-route'),update(){},setVisible(){},async flush(){},hasUnsavedChanges:()=>false,dispose(){}};
  }});
  const mime=kind==='image'?'image/png':kind==='audio'?'audio/webm':'video/webm';
  h.projection={objectId,available:true,protection:{kind:'password'},presentation:{mask:{shape:'rect'},fontSize:20},view:kind==='note'
    ?{schema:'native',providerId:'soty.notes',kind,title:'Sealed original',canEdit:false,note:{noteId:'original_note',revision:1,createdAt:123,updatedAt:124,title:'Sealed original',preview:'',state:'active',pinned:false,color:'plain'}}
    :{schema:'native',providerId:'soty.materials',kind,canEdit:false,material:{materialId:'original_material',kind,mediaType:mime,size:64,revision:1,name:'Private material'}}};
  h.render();assert.equal(mounts,1,'Every password Native uses the protected renderer even when its original provider/kind is retained');
  assert.equal(h.factories.length,0,'The ordinary Notes/Materials factories are never entered');assert.equal(h.calls.length,0);
  assert.equal(captured.view.kind,'vault');assert.equal(captured.view.contentType,kind==='note'?CANVAS_VAULT_NOTE_TYPE:mime);assert.equal(captured.view.canEdit,false);assert.equal(captured.presentation.fontSize,20);
}
test('sealed original Notes/Materials DTOs retain identity while routing every kind to encrypted ports',t=>{
  for(const kind of['note','image','audio','video'])protectedNativeRoute(t,kind);
});
test('pending sealing is represented as pending with no plaintext/native provider or password claim',t=>{
  let mounts=0;const h=harness(t,{sealingViewFactory:()=>{mounts++;return{element:new NodePort('article','sealing-route'),update(){},setVisible(){},async flush(){},hasUnsavedChanges:()=>false,dispose(){}};}});
  h.projection={objectId,available:true,protection:{kind:'none'},view:{schema:'soty.canvas-object-lock.v1',state:'sealing'}};h.render();
  assert.equal(mounts,1);assert.equal(h.factories.length,0);assert.equal(h.calls.length,0);assert.ok(h.content.objectKey(h.data.layout.nodes[0]).startsWith('vault:sealing:'));
});
test('negative control: a vault-kind-only router fails the unchanged converted-native route oracle',t=>{
  const from="if(view.providerId==='soty.notes'&&view.kind==='note'&&view.note)return{schema:'soty.canvas-vault-view.v1'";
  assert.equal(source.split(from).length,2);assert.throws(()=>protectedNativeRoute(t,'note',source.replace(from,"if(false)return{schema:'soty.canvas-vault-view.v1'")),/Every password Native uses the protected renderer/);
});

test('a signed Page-only password placeholder has no dead unlock action; permission changes replace its renderer',t=>{
 const mounted=[],vault={subscribe(){return()=>{};},isUnlocked:()=>false,generation:()=>0,invalidate(){},dispose(){}};
 const f=harness(t,{vault,vaultViewFactory:(_node,options)=>{mounted.push(options);return{element:new NodePort('article'),update(){},setVisible(){},activate(){},async flush(){},hasUnsavedChanges:()=>false,dispose(){}};}});
 f.projection={objectId,available:true,revision:1,canUnlock:false,presentation:{mask:{shape:'rect'}},protection:{kind:'password'},view:{schema:'soty.canvas-object-lock.v1',state:'locked'}};
 const node=f.data.layout.nodes[0],lockedKey=f.content.objectKey(node);f.content.createObject(node);
 assert.equal(mounted[0].allowUnlock,false,'Page selection alone does not authorize an ordinary Vault unlock');
 assert.match(mounted[0].lockedMessage,/владельца/u,'a signed visitor needs an access explanation, not a second sign-in instruction');
 f.projection={...f.projection,canUnlock:true};assert.notEqual(f.content.objectKey(node),lockedKey,'a changed current unlock right replaces the old form even at the same crypto epoch');
 f.content.createObject(node);assert.notEqual(mounted[1].allowUnlock,false);
});

function runtimeProfileWitnessScenario(t,code=source){
 const h=harness(t,{code});h.projection=widgetProjection();
 assert.equal(h.content.hasUnconfirmedWork(),false,'An unmounted widget is not an unknown running document');
 const empty=h.content.readRuntimeEpoch();h.render();const view=h.env.objectViews.get('placement_content').view;
 const first=h.stages[0],mounted=h.content.readRuntimeEpoch();assert.ok(mounted>empty,'Mount changes the work-owner witness');
 first.unconfirmed=true;assert.equal(h.content.hasUnconfirmedWork(),true,'Unsupported running work must reach the account-change guard');
 view.setPresentation('interactive');view.update(h.data.layout.nodes[0]);view.setPresentation('glance');
 assert.equal(h.content.readRuntimeEpoch(),mounted,'Presentation and metadata do not retire a live work owner');
 first.unconfirmed=false;view.dispose();assert.ok(h.content.readRuntimeEpoch()>mounted,'Disposal retires the captured work-owner witness');
 assert.equal(h.content.hasUnconfirmedWork(),false,'Disposed work is no longer an active unknown owner');
}
test('account-change work witness follows real widget owners without changing on presentation',t=>runtimeProfileWitnessScenario(t));
test('negative control: reporting unknown widget work as clean fails the account-change criterion',t=>{
 const from="hasUnconfirmedWork:()=>[...widgetViews].some(view=>view.hasUnconfirmedWork?.()===true)";assert.equal(source.split(from).length,2);
 assert.throws(()=>runtimeProfileWitnessScenario(t,source.replace(from,'hasUnconfirmedWork:()=>false')),/Unsupported running work/);
});
test('negative control: unchanged lifecycle witness fails the same owner-retirement criterion',t=>{
 assert.ok(source.includes('runtimeEpoch++'));
 assert.throws(()=>runtimeProfileWitnessScenario(t,source.replaceAll('runtimeEpoch++','void runtimeEpoch')),/Mount changes the work-owner witness/);
});
