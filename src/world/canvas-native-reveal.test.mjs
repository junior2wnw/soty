import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {nativeFieldSafeArea} from './native-field-safe-area.mjs';
import {createCanvasVaultMemoryCache,vaultErrorCode} from './canvas-vault-local.mjs';
import {CANVAS_VAULT_NOTE_TYPE} from './canvas-vault-client.mjs';

const bridge=readFileSync(new URL('./canvas-field-content.ts',import.meta.url),'utf8'),renderer=readFileSync(new URL('./canvas-vault-renderer.ts',import.meta.url),'utf8');
const overview=readFileSync(new URL('./field-object-overview.ts',import.meta.url),'utf8');
const portSource=readFileSync(new URL('./canvas-field-content.test.mjs',import.meta.url),'utf8'),parsed=ts.createSourceFile('port.mjs',portSource,ts.ScriptTarget.Latest,true,ts.ScriptKind.JS),declaration=parsed.statements.find(node=>ts.isClassDeclaration(node)&&node.name?.text==='NodePort');assert.ok(declaration);
const flatten=node=>[node,...node.children.flatMap(flatten)],byClass=(root,name)=>flatten(root).find(node=>node.className.split(/\s+/u).includes(name));
const NodePort=vm.runInNewContext(`(${ts.createPrinter().printNode(ts.EmitHint.Unspecified,declaration,parsed)})`,{flatten});
Object.defineProperty(NodePort.prototype,'firstElementChild',{get(){return this.children[0]??null;},configurable:true});
const compile=value=>ts.transpileModule(value,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'');
const until=async predicate=>{for(let index=0;index<200;index++){if(predicate())return;await new Promise(resolve=>setImmediate(resolve));}assert.fail('Expected Native reveal boundary');};

async function scenario(t,rendererCode=renderer){
  const objectId='reveal_native_object',accountId='reveal_owner',listeners=new Set();let reads=0,mounts=0,ready=false,refreshes=0,current=true;
  const note={noteId:'original_source_note',title:'Private original',body:'Protected body after device cleanup',items:[],color:'plain',pinned:false,state:'active',revision:1,createdAt:1,updatedAt:1};
  const projection={objectId,available:true,protection:{kind:'password'},view:{schema:'native',providerId:'soty.notes',kind:'note',canEdit:true,note:{...note,body:undefined}}};
  const transition={remember(){},observeProtected(){},snapshot:()=>({applicable:true,server:'protected',device:ready?'complete':'pending'}),async resumeAfterUnlock(){return this.snapshot();},dispose(){}};
  const vault={isUnlocked:()=>true,generation:()=>1,noteId:()=>note.noteId,subscribe(fn){listeners.add(fn);return()=>listeners.delete(fn);},draftStore:()=>({}),async nativeRecoveries(){return[];},async readNote(){reads++;return structuredClone(note);}};
  const document={activeElement:null,createElement(tag){const node=new NodePort(tag);node.ownerDocument=document;return node;}};
  const env={exports:{},document,HTMLElement:NodePort,HTMLImageElement:class extends NodePort{},HTMLMediaElement:class extends NodePort{},HTMLVideoElement:class extends NodePort{},createCanvasVaultMemoryCache,vaultErrorCode,CANVAS_VAULT_NOTE_TYPE,nativeFieldSafeArea,queueMicrotask,Promise,AbortController,structuredClone,
    createNativeFieldObjects(options){mounts++;return{createObject(){const root=document.createElement('article'),body=document.createElement('textarea');body.className='protected-body';root.append(body);void options.api.request('notes.get',{noteId:note.noteId}).then(value=>{body.value=value.note.body;});
      return{element:root,update(){},setVisible(){},async flush(){},hasUnsavedChanges:()=>false,dispose(){body.value='';root.remove();}};},dispose(){}};}};
  env.icon=()=>document.createElement('svg');
  const context=vm.createContext(env);vm.runInContext(compile(overview)+'\n'+compile(rendererCode)+'\n'+compile(bridge)+'\nexports.create=createCanvasFieldContent;',context);
  const content=env.exports.create({api:{async request(){throw Error('No ordinary Source API');}},nativeTransition:transition,vault,accountId,isCurrent:()=>current,projection:()=>projection,onObjectChanged:async()=>{refreshes++;view.update(node);}});
  const node={shortcutId:'reveal_placement',entity:{entity:{kind:'object',id:objectId},title:'Under password'},presentation:{shape:'rect'},width:340,height:300};
  const view=content.createObject(node);t.after(()=>{current=false;view.dispose();content.dispose();});view.setVisible(true);
  assert.equal(mounts,0,'An existing password permit cannot reveal a Native while this device still holds plaintext');assert.equal(reads,0);assert.ok(byClass(view.element,'cv-locked'));
  const form=byClass(view.element,'cv-locked');form.emit('submit');await until(()=>form.textContent.includes('Прежние черновики сохранены'));assert.equal(mounts,0);assert.equal(reads,0);assert.equal(refreshes,0);
  ready=true;form.emit('submit');await until(()=>byClass(view.element,'protected-body')?.value===note.body);assert.equal(mounts,1);assert.equal(reads,1);assert.equal(refreshes,1);
  ready=false;view.update(node);assert.equal(byClass(view.element,'protected-body'),undefined,'A newly pending device cleanup also closes already revealed content');assert.ok(byClass(view.element,'cv-locked'));
}
test('actual bridge and protected renderer keep Native body closed until the device cleanup is complete',scenario);
test('negative control: bypassing device readiness fails the unchanged no-reveal oracle',async t=>{
  const needle='&&(!options.canReveal||options.canReveal())';assert.equal(renderer.split(needle).length,2);
  await assert.rejects(scenario(t,renderer.replace(needle,'')),/An existing password permit cannot reveal a Native/);
});
