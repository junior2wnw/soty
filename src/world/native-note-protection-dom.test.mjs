import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {createNotesService} from '../../modules/notes/server/index.mjs';
import {createNoteSession,blankNote,noteErrorCode} from '../../modules/notes/browser/state.mjs';
import {createNativeDraftStore} from '../../modules/notes/browser/native-local.mjs';
import {createNoteCache,createIndexedNoteCacheStorage,readNoteWithCache} from '../../modules/notes/browser/cache.mjs';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
import * as nativeState from './native-field-objects-state.mjs';
import {nativeFieldSafeArea} from './native-field-safe-area.mjs';
const source=readFileSync(new URL('./native-field-objects.ts',import.meta.url),'utf8'),legacy=readFileSync(new URL('./notes.ts',import.meta.url),'utf8');
const actor={accountId:'dom_protection_owner',deviceId:'dom_protection_device'};
const flatten=node=>[node,...node.children.flatMap(flatten)];
const byClass=(node,name)=>flatten(node).find(value=>value.className.split(/\s+/u).includes(name));
const turn=()=>new Promise(resolve=>setImmediate(resolve));
const until=async predicate=>{for(let index=0;index<300;index++){if(await predicate())return;await turn();}assert.fail('Expected actual note DOM lifecycle');};
class NodePort{
  constructor(tag='div'){this.tagName=tag.toUpperCase();this.children=[];this.parentElement=null;this.className='';this.textContent='';this.value='';this.dataset={};this.style={setProperty(){}};this.attributes=new Map();this.listeners=new Map();this.disabled=false;this.hidden=false;}
  append(...nodes){for(const node of nodes){node.parentElement=this;this.children.push(node);}}replaceChildren(...nodes){this.children=[];this.append(...nodes);}remove(){if(this.parentElement)this.parentElement.children=this.parentElement.children.filter(node=>node!==this);}
  setAttribute(name,value){this.attributes.set(name,String(value));}getAttribute(name){return this.attributes.get(name)??null;}contains(node){return flatten(this).includes(node);}addEventListener(type,callback){if(!this.listeners.has(type))this.listeners.set(type,[]);this.listeners.get(type).push(callback);}emit(type){for(const callback of this.listeners.get(type)??[])callback({});}
  querySelectorAll(selector){return flatten(this).filter(node=>selector.startsWith('.')&&node.className.split(/\s+/u).includes(selector.slice(1)));}
}
async function nativeFixture(t,code=source){
  const service=createNotesService({databasePath:':memory:',projectId:'dom_native'}),noteId=crypto.randomUUID();
  service.execute({actor,op:'notes.put',args:{expectedAccountId:actor.accountId,noteId,mutationId:crypto.randomUUID(),expectedRevision:0,title:'Private title',body:'Private before gate',items:[],color:'plain',pinned:false,state:'active'}});
  const port=indexedNativePort(),origin='https://'+crypto.randomUUID()+'.example',store=createNativeDraftStore(actor.accountId,'dom_native',{origin,indexedDB:port.indexedDB}),cache=createNoteCache(actor.accountId,'dom_native',{origin,storage:createIndexedNoteCacheStorage(port.indexedDB)}),calls=[];
  const document={activeElement:null,createElement:tag=>new NodePort(tag)},api={async request(op,args){calls.push(op);return service.execute({actor,op,args});}};
  const env=vm.createContext({document,HTMLElement:NodePort,AbortController,queueMicrotask,structuredClone,Map,Set,...nativeState,nativeFieldSafeArea,createNoteSession,blankNote,noteErrorCode,readNoteWithCache});
  const compiled=ts.transpileModule(code,{compilerOptions:{module:ts.ModuleKind.ESNext,target:ts.ScriptTarget.ES2022}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'');vm.runInContext(compiled,env);
  const factory=env.createNativeFieldObjects({api,accountId:actor.accountId,projectId:'dom_native',isCurrent:()=>true,store,cache,resolve:()=>({kind:'note',noteId})});
  const node={entity:{entity:{kind:'builtin',id:'notes/'+noteId},title:'Записка'},width:340,height:300,presentation:{shape:'rect'}},view=factory.createObject(node);view.setVisible(true);
  const body=byClass(view.element,'nf-body');await until(()=>body.value==='Private before gate');
  t.after(async()=>{factory.dispose();await turn();service.close();});return{factory,view,body,store,noteId,document,calls};
}

test('a focused real native editor scrubs on the Source fence and cannot accept a stale input',async t=>{
  const f=await nativeFixture(t);f.document.activeElement=f.body;f.body.value='Unsaved original private draft';f.body.emit('input');
  await until(async()=>(await f.store.native.inspect(f.noteId)).some(value=>value.note.body==='Unsaved original private draft'));
  const gate=await f.store.native.mark({providerId:'soty.notes',ref:f.noteId,operationId:'dom_convert_123'});
  assert.equal(f.body.value,'');assert.equal(f.body.disabled,true);assert.equal(f.view.element.dataset.saveState,'protection-pending');
  f.body.value='Stale event text';f.body.emit('input');assert.equal(f.body.value,'');
  assert.equal((await f.store.native.inspect(f.noteId))[0].note.body,'Unsaved original private draft');assert.equal(f.calls.filter(op=>op==='notes.put').length,0);
  await f.store.native.cancel({providerId:'soty.notes',ref:f.noteId,epoch:gate.epoch});
  assert.equal(f.body.value,'Unsaved original private draft','A safe rollback must restore the suspended editor even if focus stayed on its disabled textarea');
});

test('the caret-preservation privacy oracle catches removing the explicit protection scrub',async t=>{
  const broken=source.replace(/      if\(state\.localProtection\)\{[\s\S]*?retry\.hidden=copy\.hidden=latest\.hidden=keep\.hidden=true;return;\n      \}/u,'');assert.notEqual(broken,source);
  const f=await nativeFixture(t,broken);f.document.activeElement=f.body;await f.store.native.mark({providerId:'soty.notes',ref:f.noteId,operationId:'dom_broken_123'});
  assert.equal(f.body.value,'Private before gate');assert.throws(()=>assert.equal(f.body.value,''),'The original focused-body scrub criterion must reject this regression');
});

test('the actual legacy Notes state handler removes the editor and old list/draft previews while retaining private storage',()=>{
  const parsed=ts.createSourceFile('notes.ts',legacy,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS),mount=parsed.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='mountNotes'),handler=mount.body.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='updateStatus');assert.ok(handler);
  const editor=new NodePort(),textarea=new NodePort('textarea');textarea.value='Legacy private text';editor.append(textarea);
  const node=(tag,_class,text)=>{const value=new NodePort(tag);value.className=_class;value.textContent=text;return value;};
  let closed=0;const env=vm.createContext({editor,disposed:false,status:new NodePort(),statusText:new NodePort(),openedSnapshot:{note:{body:'Legacy private snapshot'}},noteActionsDialog:{close(options){assert.equal(options.restoreFocus,false);closed++;}},rows:[{noteId:'legacy_note_123',title:'Legacy private title',preview:'Legacy private preview'}],drafts:[{note:{noteId:'legacy_note_123',body:'Legacy private draft'}}],resizeObserver:{unobserve(){}},renderRows(){},renderDrafts(){},el:node});
  const text=ts.transpileModule(handler.getText(parsed),{compilerOptions:{module:ts.ModuleKind.ESNext,target:ts.ScriptTarget.ES2022}}).outputText;vm.runInContext(text,env);
  env.updateStatus({localProtection:'pending',dirty:true,note:{noteId:'legacy_note_123'}});
  assert.equal(editor.children.some(child=>child.tagName==='TEXTAREA'),false);assert.equal(env.openedSnapshot,undefined);assert.equal(env.rows[0].preview,'');assert.equal(env.drafts.length,0);assert.equal(closed,1);assert.doesNotMatch(flatten(editor).map(value=>value.value+' '+value.textContent).join(' '),/Legacy private/u);
});
