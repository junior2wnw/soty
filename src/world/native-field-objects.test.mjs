import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {createNotesService} from '../../modules/notes/server/index.mjs';
import {createNoteSession,blankNote,noteErrorCode} from '../../modules/notes/browser/state.mjs';
import {readNoteWithCache} from '../../modules/notes/browser/cache.mjs';
import {nativeFieldRef,nativeNoteFontSize,mayRefreshNativeNote,nativeNoteStatus} from './native-field-objects-state.mjs';
import {nativeFieldSafeArea} from './native-field-safe-area.mjs';
const source=readFileSync(new URL('./native-field-objects.ts',import.meta.url),'utf8');
const overviewSource=readFileSync(new URL('./field-object-overview.ts',import.meta.url),'utf8');
const actor={accountId:'account_native',deviceId:'device_native'};
const until=async predicate=>{for(let index=0;index<100;index++){if(predicate())return;await new Promise(resolve=>setImmediate(resolve));}assert.fail('asynchronous state did not settle');};
function memoryStore(){const records=new Map();return{async put(value){records.set(value.branchId,structuredClone(value));},async list(){return [...records.values()].map(value=>structuredClone(value));},async remove(noteId,branchId,savedAt){const record=records.get(branchId);if(record?.note.noteId===noteId&&(savedAt===undefined||record.savedAt===savedAt))records.delete(branchId);}};}
function memoryCache(){const records=new Map();return{async capture(){return'epoch';},async isCurrent(){return true;},async remember(note){records.set(note.noteId,{note:structuredClone(note),verifiedAt:Date.now()});return true;},async get(id){return records.get(id)??null;},async list(){return[...records.values()];},async remove(id){records.delete(id);},async clear(){records.clear();}};}
function harness(t,{code=source,readBlob,refreshPolicy=mayRefreshNativeNote}={}){
  const service=createNotesService({databasePath:':memory:',projectId:'native-test'});t.after(()=>service.close());
  let current=true;const calls=[],urls=[],revoked=[],store=memoryStore(),cache=memoryCache();
  const api={async request(op,args){calls.push({op,args:structuredClone(args)});return service.execute({op,args:structuredClone(args),actor});}};
  class Node{
    constructor(tag){this.tagName=tag.toUpperCase();this.children=[];this.parentElement=null;this.dataset={};this.attributes=new Map();this.listeners=new Map();this.style={setProperty:(key,value)=>{this.style[key]=value;}};this.value='';this.textContent='';this.hidden=false;this.disabled=false;this.playCalls=0;this.loadCalls=0;this.pauseCalls=0;}
    setAttribute(key,value){this.attributes.set(key,String(value));}removeAttribute(key){this.attributes.delete(key);if(key==='src')this.src='';}getAttribute(key){return this.attributes.get(key)??null;}
    append(...nodes){for(const node of nodes){node.parentElement=this;this.children.push(node);}}replaceChildren(...nodes){this.children=[];this.append(...nodes);}remove(){if(this.parentElement)this.parentElement.children=this.parentElement.children.filter(value=>value!==this);this.parentElement=null;}
    contains(node){return node===this||this.children.some(value=>value.contains(node));}focus(){document.activeElement=this;}
    addEventListener(type,handler){if(!this.listeners.has(type))this.listeners.set(type,[]);this.listeners.get(type).push(handler);}emit(type,event={}){for(const handler of this.listeners.get(type)??[])handler(event);}
    pause(){this.pauseCalls++;}load(){this.loadCalls++;}async play(){this.playCalls++;}
  }
  const document={activeElement:null,createElement:tag=>new Node(tag)};
  const context=vm.createContext({exports:{},document,createNoteSession,blankNote,noteErrorCode,readNoteWithCache,nativeFieldRef,nativeNoteFontSize,nativeFieldSafeArea,mayRefreshNativeNote:refreshPolicy,nativeNoteStatus,
    createDraftStore:()=>store,createNoteCache:()=>cache,readMaterialBlob:readBlob??(async()=>{throw Error('unexpected_material_read');}),
    URL:{createObjectURL:blob=>{const url=`blob:test-${urls.length}`;urls.push({url,blob});return url;},revokeObjectURL:url=>revoked.push(url)},queueMicrotask,structuredClone,AbortController,Promise});
  const compiled=ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'');
  context.icon=()=>document.createElement('svg');
  vm.runInContext(ts.transpileModule(overviewSource,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,''),context);
  vm.runInContext(compiled+'\nexports.create=createNativeFieldObjects;',context);
  const options={api,accountId:actor.accountId,projectId:'native-test',isCurrent:()=>current,store,cache};
  const flatten=node=>[node,...node.children.flatMap(flatten)];
  return{service,api,calls,urls,revoked,store,cache,document,flatten,options,create:context.exports.create,changeAccount(){current=false;},
    async seed(body='Исходный текст'){const note=blankNote();await api.request('notes.put',{expectedAccountId:actor.accountId,noteId:note.noteId,mutationId:crypto.randomUUID(),expectedRevision:0,title:'Идея',body,items:[],color:'plain',pinned:false,state:'active'});return (await api.request('notes.get',{expectedAccountId:actor.accountId,noteId:note.noteId})).note;}};
}
const node=(id,title='Идея')=>({shortcutId:'shortcut_native',entity:{entity:{kind:'builtin',id},title}});

test('inline note lazily opens real Notes API, edits body on canvas, persists through provider recreation',async t=>{
  const f=harness(t),note=await f.seed(),provider=f.create(f.options),item=node(`notes/${note.noteId}`),view=provider.createObject(item);
  t.after(()=>provider.dispose());assert.equal(view.element.tagName,'ARTICLE');const before=f.calls.length;
  assert.equal(f.calls.length,before);view.setVisible(true);await until(()=>f.flatten(view.element).some(value=>value.className==='nf-body'&&!value.disabled));
  const title=f.flatten(view.element).find(value=>value.className==='nf-title'),body=f.flatten(view.element).find(value=>value.className==='nf-body');assert.equal(body.value,'Исходный текст');
  body.focus();body.value='Мысль прямо на полотне';body.selectionStart=7;body.emit('input');
  view.update({...item,entity:{...item.entity,title:'Метаданные обновились'}});
  assert.equal(f.flatten(view.element).find(value=>value.className==='nf-body'),body);assert.equal(body.selectionStart,7);assert.equal(body.value,'Мысль прямо на полотне');
  await view.flush();assert.equal((await f.api.request('notes.get',{expectedAccountId:actor.accountId,noteId:note.noteId})).note.body,'Мысль прямо на полотне');
  assert.equal(title.disabled,false);provider.dispose();
  const reopened=f.create(f.options),again=reopened.createObject(item);again.setVisible(true);await until(()=>f.flatten(again.element).some(value=>value.className==='nf-body'&&value.value==='Мысль прямо на полотне'&&!value.disabled));reopened.dispose();
});
test('metadata refresh keeps focused caret; later unfocused clean revision refreshes same editor DOM',async t=>{
  const f=harness(t),note=await f.seed();let summary={...note};
  const provider=f.create({...f.options,resolve:()=>({kind:'note',noteId:note.noteId,summary})}),item=node(`notes/${note.noteId}`),view=provider.createObject(item);t.after(()=>provider.dispose());
  view.setVisible(true);await until(()=>f.flatten(view.element).some(value=>value.className==='nf-body'&&!value.disabled));
  const body=f.flatten(view.element).find(value=>value.className==='nf-body');body.focus();body.selectionStart=4;
  await f.api.request('notes.put',{expectedAccountId:actor.accountId,noteId:note.noteId,mutationId:crypto.randomUUID(),expectedRevision:1,title:note.title,body:'Вторая версия',items:[],color:'plain',pinned:false,state:'active'});
  summary={...summary,revision:2,preview:'Вторая версия'};view.update(item);await new Promise(resolve=>setImmediate(resolve));
  assert.equal(body.value,'Исходный текст');assert.equal(body.selectionStart,4);
  f.document.activeElement=null;view.element.emit('focusout');await until(()=>body.value==='Вторая версия');assert.equal(f.flatten(view.element).find(value=>value.className==='nf-body'),body);
});
test('an offline inline edit stays an existing durable Notes outbox and recovers after recreation',async t=>{
  const f=harness(t),note=await f.seed();let offline=false;
  const options={...f.options,api:{async request(op,args){if(offline)throw Error('NETWORK_TIMEOUT');return f.api.request(op,args);}}};
  const provider=f.create(options),item=node(`notes/${note.noteId}`),view=provider.createObject(item);view.setVisible(true);await until(()=>f.flatten(view.element).some(value=>value.className==='nf-body'&&!value.disabled));
  const body=f.flatten(view.element).find(value=>value.className==='nf-body');offline=true;body.value='Долгий офлайн черновик';body.emit('input');await view.flush();
  assert.equal((await f.store.list()).length,1);provider.dispose();offline=false;
  const reopened=f.create(options),again=reopened.createObject(item);again.setVisible(true);await until(()=>f.flatten(again.element).some(value=>value.className==='nf-body'&&value.value==='Долгий офлайн черновик'&&!value.disabled));await again.flush();
  assert.equal((await f.api.request('notes.get',{expectedAccountId:actor.accountId,noteId:note.noteId})).note.body,'Долгий офлайн черновик');reopened.dispose();
});

test('inline conflict offers an explicit local save without switching away from the canvas',async t=>{
  const f=harness(t),note=await f.seed(),provider=f.create(f.options),view=provider.createObject(node(`notes/${note.noteId}`));t.after(()=>provider.dispose());view.setVisible(true);
  await until(()=>f.flatten(view.element).some(value=>value.className==='nf-body'&&!value.disabled));
  await f.api.request('notes.put',{expectedAccountId:actor.accountId,noteId:note.noteId,mutationId:crypto.randomUUID(),expectedRevision:1,title:note.title,body:'На сервере',items:[],color:'plain',pinned:false,state:'active'});
  const body=f.flatten(view.element).find(value=>value.className==='nf-body');body.value='Выбранный текст на полотне';body.emit('input');await view.flush();
  assert.equal(view.element.dataset.saveState,'conflict');const keep=f.flatten(view.element).find(value=>value.textContent==='Сохранить мой текст');
  assert.ok(keep&&!keep.hidden,'The actual conflicting card must expose an explicit local choice');keep.emit('click');
  await until(()=>view.element.dataset.saveState==='saved');assert.equal(body.value,'Выбранный текст на полотне');assert.equal(keep.hidden,true);
  const saved=(await f.api.request('notes.get',{expectedAccountId:actor.accountId,noteId:note.noteId})).note;assert.equal(saved.revision,3);assert.equal(saved.body,body.value);assert.equal((await f.store.list()).length,0);
});
test('a read-only public note permits copy/focus but never reads viewer drafts, changes a checklist or writes a Notes outbox/API mutation',async t=>{
  const f=harness(t),initial=await f.seed();await f.api.request('notes.put',{expectedAccountId:actor.accountId,noteId:initial.noteId,mutationId:crypto.randomUUID(),expectedRevision:1,title:initial.title,body:initial.body,items:[{id:'item_public',text:'Проверить',done:false}],color:'plain',pinned:false,state:'active'});
  const note=(await f.api.request('notes.get',{expectedAccountId:actor.accountId,noteId:initial.noteId})).note,before=f.calls.filter(call=>call.op==='notes.put').length;
  const originalPut=f.store.put,originalList=f.store.list;let draftReads=0,draftWrites=0,forks=0;
  await originalPut({note:{...note,body:'Чужой по смыслу private draft с совпавшим ID'},branchId:'existing_private_branch',generation:1,committedGeneration:0,pending:null,conflict:false,savedAt:1});
  f.store.put=async value=>{draftWrites++;return originalPut(value);};f.store.list=async()=>{draftReads++;return originalList();};
  const provider=f.create({...f.options,resolve:()=>({kind:'note',noteId:note.noteId,readOnly:true}),onNoteFork:()=>forks++}),view=provider.createObject(node(`notes/${note.noteId}`));view.setVisible(true);
  await until(()=>f.flatten(view.element).some(value=>value.className==='nf-body'&&!value.disabled));
  const fields=f.flatten(view.element),title=fields.find(value=>value.className==='nf-title'),body=fields.find(value=>value.className==='nf-body'),checkbox=fields.find(value=>value.tagName==='INPUT'&&value.type==='checkbox'),text=fields.find(value=>value.className==='nf-check-text');
  assert.equal(body.value,note.body);assert.equal(body.readOnly,true);assert.equal(title.readOnly,true);assert.equal(body.disabled,false);body.focus();assert.equal(f.document.activeElement,body);
  assert.equal(checkbox.disabled,true);assert.equal(text.disabled,true);assert.equal(draftReads,0);assert.equal((await f.cache.list()).length,0);
  body.value='Недопустимое изменение';body.emit('input');title.value='Новый заголовок';title.emit('input');checkbox.checked=true;checkbox.emit('change');text.value='Новый пункт';text.emit('input');
  view.element.emit('keydown',{ctrlKey:true,key:'s',preventDefault(){}});fields.find(value=>value.textContent==='Сохранить копию').emit('click');await view.flush();await new Promise(resolve=>setImmediate(resolve));
  assert.equal(f.calls.filter(call=>call.op==='notes.put').length,before);assert.equal(draftWrites,0);assert.equal(forks,0);assert.equal(view.hasUnsavedChanges(),false);assert.equal((await originalList()).length,1);
  const unchanged=(await f.api.request('notes.get',{expectedAccountId:actor.accountId,noteId:note.noteId})).note;assert.equal(unchanged.body,note.body);assert.equal(unchanged.items[0].done,false);assert.equal(unchanged.items[0].text,'Проверить');provider.dispose();
});
test('fresh readOnly changes stop session autosave, retain its private branch, then recover edits only when writable again',async t=>{
  const f=harness(t),note=await f.seed();let readOnly=false;const provider=f.create({...f.options,resolve:()=>({kind:'note',noteId:note.noteId,readOnly,summary:note})}),item=node(`notes/${note.noteId}`),view=provider.createObject(item);view.setVisible(true);
  await until(()=>f.flatten(view.element).some(value=>value.className==='nf-body'&&!value.disabled));const body=f.flatten(view.element).find(value=>value.className==='nf-body'),before=f.calls.filter(call=>call.op==='notes.put').length;
  body.value='Локальная мысль владельца';body.emit('input');readOnly=true;view.update(item);assert.equal(body.readOnly,true);
  await until(()=>body.value===note.body&&!body.disabled);await view.flush();assert.equal(f.calls.filter(call=>call.op==='notes.put').length,before);assert.ok((await f.store.list()).some(value=>value.note.body==='Локальная мысль владельца'));
  body.value='Читать нельзя менять';body.emit('input');await view.flush();assert.equal(f.calls.filter(call=>call.op==='notes.put').length,before);
  readOnly=false;view.update(item);await until(()=>body.value==='Локальная мысль владельца'&&!body.disabled);assert.equal(body.readOnly,false);await view.flush();
  assert.equal((await f.api.request('notes.get',{expectedAccountId:actor.accountId,noteId:note.noteId})).note.body,'Локальная мысль владельца');provider.dispose();
});
test('a delayed writable draft read cannot install an outbox after the source has become read-only',async t=>{
  const f=harness(t),note=await f.seed();let readOnly=false,release,started=false;const originalList=f.store.list;
  f.store.list=async()=>{started=true;return new Promise(resolve=>{release=()=>resolve([{note:{...note,body:'Запоздалый черновик'},branchId:'late_branch',generation:1,committedGeneration:0,pending:null,conflict:false,savedAt:1}]);});};
  const before=f.calls.filter(call=>call.op==='notes.put').length,provider=f.create({...f.options,resolve:()=>({kind:'note',noteId:note.noteId,readOnly})}),item=node(`notes/${note.noteId}`),view=provider.createObject(item);view.setVisible(true);await until(()=>started);
  readOnly=true;view.update(item);release();await until(()=>f.flatten(view.element).some(value=>value.className==='nf-body'&&value.value===note.body&&!value.disabled));
  await view.flush();assert.equal(f.calls.filter(call=>call.op==='notes.put').length,before);assert.equal((await originalList()).length,0);provider.dispose();
});
test('negative control detects the previous behavior when the renderer ignores the read-only source flag',async t=>{
  const broken=source.replace('const readOnly=()=>source.readOnly===true;','const readOnly=()=>false;');assert.notEqual(broken,source);
  const f=harness(t,{code:broken}),note=await f.seed(),before=f.calls.filter(call=>call.op==='notes.put').length,provider=f.create({...f.options,resolve:()=>({kind:'note',noteId:note.noteId,readOnly:true})}),view=provider.createObject(node(`notes/${note.noteId}`));view.setVisible(true);
  await until(()=>f.flatten(view.element).some(value=>value.className==='nf-body'&&!value.disabled));const body=f.flatten(view.element).find(value=>value.className==='nf-body');body.value='Ошибка: изменение public note';body.emit('input');await view.flush();
  assert.throws(()=>assert.equal(f.calls.filter(call=>call.op==='notes.put').length,before),error=>error.code==='ERR_ASSERTION');provider.dispose();
});
test('photos load only when visible and release object URLs; audio bytes and play require a user action',async t=>{
  let reads=0;const metadata={revision:1,name:'Синтетический тест',kind:'image'};
  const f=harness(t,{readBlob:async()=>{reads++;return{material:{...metadata},blob:new Blob(['fixture'],{type:metadata.kind==='image'?'image/png':'audio/webm'})};}});
  let kind='image';const provider=f.create({...f.options,resolve:()=>({kind,materialId:'material_native',summary:{...metadata}})}),image=provider.createObject(node('materials/material_native'));
  assert.equal(reads,0);image.setVisible(true);await until(()=>f.urls.length===1);assert.equal(reads,1);image.dispose();assert.equal(f.revoked.length,1);
  kind=metadata.kind='audio';const audio=provider.createObject(node('materials/material_native'));audio.setVisible(true);await new Promise(resolve=>setImmediate(resolve));assert.equal(reads,1);
  const player=f.flatten(audio.element).find(value=>value.tagName==='AUDIO'),play=f.flatten(audio.element).find(value=>value.className==='nf-play');assert.equal(player.autoplay,false);assert.equal(player.playCalls,0);
  play.emit('click');await until(()=>player.playCalls===1);assert.equal(reads,2);provider.dispose();assert.equal(f.revoked.length,2);assert.ok(player.pauseCalls>0);
});
test('late note read after account change creates no editor/session',async t=>{
  const f=harness(t),note=await f.seed(),provider=f.create(f.options),view=provider.createObject(node(`notes/${note.noteId}`));view.setVisible(true);f.changeAccount();await new Promise(resolve=>setImmediate(resolve));
  assert.equal(f.flatten(view.element).find(value=>value.className==='nf-body').disabled,true);provider.dispose();
});
test('disposing a revoked dirty note immediately scrubs the old textarea/DOM while preserving its account-scoped draft',async t=>{
  const f=harness(t),note=await f.seed(),provider=f.create(f.options),view=provider.createObject(node(`notes/${note.noteId}`));view.setVisible(true);await until(()=>f.flatten(view.element).some(value=>value.className==='nf-body'&&!value.disabled));
  const body=f.flatten(view.element).find(value=>value.className==='nf-body');body.value='Секретный локальный черновик';body.emit('input');view.dispose();
  assert.equal(body.value,'');assert.equal(view.element.children.length,0);assert.equal(view.element.textContent,'');await new Promise(resolve=>setImmediate(resolve));
  const drafts=await f.store.list();assert.ok(drafts.some(value=>value.note.body==='Секретный локальный черновик'));provider.dispose();
});

async function safeAreaScenario(t,code=source){
  const f=harness(t,{code}),note=await f.seed(),provider=f.create(f.options);t.after(()=>provider.dispose());
  const item={...node(`notes/${note.noteId}`),presentation:{shape:'ellipse'},width:340,height:300},view=provider.createObject(item);view.setVisible(true);
  await until(()=>f.flatten(view.element).some(value=>value.className==='nf-body'&&!value.disabled));
  const body=f.flatten(view.element).find(value=>value.className==='nf-body'),inset=nativeFieldSafeArea(item.presentation,item.width,item.height);
  for(const[side,value]of Object.entries(inset))assert.equal(view.element.style[`--nf-safe-${side}`],`${value}px`,'The real provider installs the safe viewport rather than clipping its header');
  body.focus();body.value='Сохранённая позиция ввода';body.selectionStart=8;body.emit('input');
  const resized={...item,width:700,height:180,presentation:{shape:'polygon',points:[{x:0,y:0},{x:1,y:0},{x:1,y:1},{x:.7,y:1},{x:.7,y:.28},{x:.3,y:.28},{x:.3,y:1},{x:0,y:1}]}};view.update(resized);
  const next=nativeFieldSafeArea(resized.presentation,resized.width,resized.height);for(const[side,value]of Object.entries(next))assert.equal(view.element.style[`--nf-safe-${side}`],`${value}px`);
  assert.equal(f.flatten(view.element).find(value=>value.className==='nf-body'),body);assert.equal(body.selectionStart,8);assert.equal(body.value,'Сохранённая позиция ввода');
}
test('native shape and resize update the safe viewport without replacing the focused note editor',safeAreaScenario);
test('negative control: the old provider without safe-area application fails the same header viewport criterion',async t=>{
  const from='function update(node:FieldNode) {\n      identityNode=node;\n      applySafeArea(node);';assert.equal(source.split(from).length,2);
  await assert.rejects(safeAreaScenario(t,source.replace(from,'function update(node:FieldNode) {\n      identityNode=node;')),/real provider installs the safe viewport/);
});
test('video is an inline controls/playsInline provider with no visibility autoplay or eager bytes',async t=>{
  let reads=0;const f=harness(t,{readBlob:async()=>{reads++;return{material:{kind:'video',revision:1},blob:new Blob(['fixture'],{type:'video/webm'})};}}),provider=f.create({...f.options,resolve:()=>({kind:'video',materialId:'material_video'})}),view=provider.createObject(node('materials/material_video'));
  view.setVisible(true);await new Promise(resolve=>setImmediate(resolve));assert.equal(reads,0);
  const player=f.flatten(view.element).find(value=>value.tagName==='VIDEO');assert.equal(player.controls,true);assert.equal(player.playsInline,true);assert.equal(player.autoplay,false);
  view.activate();await until(()=>player.playCalls===1);assert.equal(reads,1);provider.dispose();assert.equal(f.revoked.length,1);
});
test('native namespace is strict and foreign provider shapes remain unclaimed',()=>{
  assert.deepEqual(nativeFieldRef({kind:'builtin',id:'notes/note_initial'}),{provider:'notes',id:'note_initial'});
  for(const value of [{kind:'app',id:'notes/note_initial'},{kind:'builtin',id:'notes/../../etc'},{kind:'builtin',id:'notes/a'},{kind:'builtin',id:'materials/id?x=1'}])assert.equal(nativeFieldRef(value),null);
  assert.equal(nativeNoteFontSize(100),100);assert.equal(nativeNoteFontSize(1000),128);assert.equal(nativeNoteFontSize(1),8);assert.equal(nativeNoteFontSize(NaN),16);
});

async function identityDisposeScenario(t,code=source){
 const f=harness(t,{code}),note=await f.seed('Never mirror this Source body');let permitted=true;
 const provider=f.create({...f.options,resolve:()=>({kind:'note',noteId:note.noteId,summary:{...note,preview:'Private source preview'}}),resolveOverview:()=>permitted?{title:'Current permitted metadata',symbol:'note'}:null}),view=provider.createObject(node(`notes/${note.noteId}`));
 const host=f.document.createElement('div');host.append(view.element,view.overview);view.setVisible(true);await until(()=>f.flatten(view.element).some(node=>node.className==='nf-body'&&!node.disabled));
 const body=f.flatten(view.element).find(node=>node.className==='nf-body');body.focus();body.value='Native draft stays put';body.selectionStart=6;body.emit('input');
 view.setPresentation('overview');view.setPresentation('glance');assert.equal(f.flatten(view.element).find(node=>node.className==='nf-body'),body);assert.equal(body.selectionStart,6);
 const title=view.overview.children[1];assert.equal(title.textContent,'Current permitted metadata');permitted=false;view.update(node(`notes/${note.noteId}`));assert.equal(title.textContent,'');
 permitted=true;view.update(node(`notes/${note.noteId}`));provider.dispose();assert.equal(view.overview.parentElement,null,'Factory disposal must remove its sibling before any host render');assert.equal(title.textContent,'');
}
test('native identity uses explicit fresh metadata, retains the editor, and is owned by factory disposal',identityDisposeScenario);
test('broken leaf cleanup cannot be hidden by an external factory wrapper',async t=>{
 const anchor='overview?.dispose();request++;';assert.equal(source.split(anchor).length,2);await assert.rejects(identityDisposeScenario(t,source.replace(anchor,'request++;')),/Factory disposal must remove its sibling/);
});
async function latePreviewScenario(t,code=source){
 const f=harness(t,{code}),note=await f.seed('Private preview must retire'),provider=f.create({...f.options,resolve:()=>({kind:'note',noteId:note.noteId,summary:{...note,preview:'Private preview must retire'}})}),item=node(`notes/${note.noteId}`),view=provider.createObject(item);
 const body=f.flatten(view.element).find(node=>node.className==='nf-body');provider.dispose();assert.equal(body.value,'');view.update(item);
 assert.equal(body.value,'','A late update cannot resurrect a disposed private preview');
}
test('a retired native note cannot repaint its cached private preview through a late update',latePreviewScenario);
test('the old missing active gate fails the same late-preview retirement criterion',async t=>{
 const anchor='applySafeArea(node);\n      if(!active()){overview?.clear();return;}';assert.equal(source.split(anchor).length,2);
 await assert.rejects(latePreviewScenario(t,source.replace(anchor,'applySafeArea(node);')),/A late update cannot resurrect/);
});
test('negative control catches the original icon-only behavior and a focused-editor overwrite',async t=>{
  const iconOnly=source.replace("const view=source.kind==='note'?noteView(node,source):mediaView(node,source);","const view=mediaView(node,{kind:'image',materialId:'material_native'});");assert.notEqual(iconOnly,source);
  const f=harness(t,{code:iconOnly}),note=await f.seed(),provider=f.create(f.options),view=provider.createObject(node(`notes/${note.noteId}`));
  assert.throws(()=>assert.ok(f.flatten(view.element).some(value=>value.className==='nf-body')),error=>error.code==='ERR_ASSERTION');provider.dispose();
  const overwrite=source.replace('session.state().dirty||focused()','session.state().dirty');assert.notEqual(overwrite,source);
  const broken=harness(t,{code:overwrite,refreshPolicy:(state,_focused,revision)=>Boolean(state&&revision>state.note.revision)}),original=await broken.seed();let summary={...original};
  const brokenProvider=broken.create({...broken.options,resolve:()=>({kind:'note',noteId:original.noteId,summary})}),item=node(`notes/${original.noteId}`),editor=brokenProvider.createObject(item);editor.setVisible(true);
  await until(()=>broken.flatten(editor.element).some(value=>value.className==='nf-body'&&!value.disabled));const body=broken.flatten(editor.element).find(value=>value.className==='nf-body');body.focus();
  await broken.api.request('notes.put',{expectedAccountId:actor.accountId,noteId:original.noteId,mutationId:crypto.randomUUID(),expectedRevision:1,title:original.title,body:'Перезаписан сервером',items:[],color:'plain',pinned:false,state:'active'});
  summary={...summary,revision:2};editor.update(item);await until(()=>body.value==='Перезаписан сервером');
  assert.throws(()=>assert.equal(body.value,'Исходный текст'),error=>error.code==='ERR_ASSERTION');brokenProvider.dispose();
});
