import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';

const contentSource=readFileSync(new URL('./canvas-field-content.ts',import.meta.url),'utf8');
const engineSource=readFileSync(new URL('./unified-field.ts',import.meta.url),'utf8');
const screenSource=readFileSync(new URL('./unified-field-screen.ts',import.meta.url),'utf8');
const compile=source=>ts.transpileModule(source,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'');
const parse=source=>ts.createSourceFile('fixture.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS);
const findFunction=(nodes,name)=>{const node=nodes.find(value=>ts.isFunctionDeclaration(value)&&value.name?.text===name);assert.ok(node,name);return node;};
const ast=parse(engineSource),mount=findFunction(ast.statements,'createUnifiedField'),render=findFunction(mount.body.statements,'render');
const focusGuards=render.body.statements.filter(value=>ts.isIfStatement(value)&&value.expression.getText(ast).startsWith('options.readableObjectFocus')).slice(0,2);
assert.equal(focusGuards.length,2);const guardSource=focusGuards.map(value=>value.getText(ast)).join('\n');
const objectId='object_unlock_focus',shortcutId='placement_unlock_focus',appId='app-'+ 'a'.repeat(32),widgetId='widget-11111111-1111-4111-8111-111111111111';
const clone=value=>structuredClone(value),noteType='application/vnd.soty.note+json';
const pin={revision:1,digest:'b'.repeat(64),profile:'soty.relay-restricted.v1'};
const locked=()=>({objectId,available:true,revision:7,accessRevision:3,canUnlock:true,protection:{kind:'password'},ownerRepresents:[{kind:'app',id:appId}],view:{schema:'soty.canvas-object-lock.v1',state:'locked'}});
const widget=()=>({...locked(),view:{schema:'soty.canvas-widget-view.v1',kind:'widget',widgetId,instanceId:widgetId,state:'ready',
 app:{appId,name:'Current allowed app',status:'ready',ownerAccountId:'owner-A'},entry:{appId,domainId:'dom_'+ 'c'.repeat(32),origin:'https://app.example',path:'/'},
 source:clone(pin),expectedSource:clone(pin),runtimeAllowed:true,canRefresh:true}});

// Actual CFC closure, its existing locked provider, and actual engine focus/bar
// statements. Renderer/Vault events and authoritative projection data are
// explicit synthetic ports, not Root grants, password validation or browser QA.
function fixture({initial=locked(),code=contentSource,engine=guardSource}={}){
 let projection=initial,active=true,unlocked=false,generation=1;const listeners=new Set(),calls=[];
 const vault={isUnlocked:()=>unlocked,generation:()=>generation,subscribe(fn){listeners.add(fn);return()=>listeners.delete(fn);},
  invalidate(id,reason='relock'){unlocked=false;generation++;for(const listener of listeners)listener(id,reason);},dispose(){}};
 const env={exports:{},createCanvasVaultClient:()=>assert.fail('fixture always supplies the explicit Vault port'),CANVAS_VAULT_NOTE_TYPE:noteType,
  createCanvasVaultView:()=>({element:{},dispose(){}}),
  createNativeCacheTransition:()=>({remember(){},observeProtected(){},snapshot:()=>({applicable:false}),async resumeAfterUnlock(){return{applicable:false};},async flush(){},hasUnsavedChanges:()=>false,dispose(){}}),
 };vm.createContext(env);new vm.Script(compile(code)+'\nexports.create=createCanvasFieldContent;').runInContext(env);
 const content=env.exports.create({accountId:'owner-A',isCurrent:()=>active,projection:()=>projection,vault,
  api:{request(){calls.push('unexpected IO');throw Error('no authority or content IO belongs to focus continuation');}},mountApp:()=>assert.fail('pure guard fixture never mounts a Source')});
 const node={shortcutId,entity:{entity:{kind:'object',id:objectId},title:'Allowed object'}};
 let view=content.createObject(node);assert.ok(view,'readable focus owns an already-created locked provider');
 const oldKey=content.objectKey(node),focusEnv={objectFocus:{shortcutId,key:oldKey,selection:'previous-selection'},options:{readableObjectFocus:true,objectKey:content.objectKey,
  canContinueObjectFocus:(...args)=>content.canContinueObjectFocus?.(...args)},activeNode:()=>node,
  viewport:{dataset:{}},focusBar:{hidden:false},focusBackdrop:{hidden:false,style:{}},focusTitle:{textContent:''},releaseFocusControls(){},cam:{x:0,y:0,scale:1},view:{width:390,height:844}};
 vm.createContext(focusEnv);
 return{content,node,get view(){return view;},vault,calls,focusEnv,oldKey,get projection(){return projection;},set projection(value){projection=value;},
  unlock(){unlocked=true;generation++;for(const listener of listeners)listener(objectId,'unlocked');},
  staleGeneration(){generation++;},changeAccount(){active=false;},
  can(old=oldKey,next=content.objectKey(node)){return content.canContinueObjectFocus?.(node,old,next)===true;},
  replaceLockedProvider(){view.dispose();view=content.createObject(node);assert.ok(view);},
  render(){new vm.Script(compile(engine)).runInContext(focusEnv);},dispose(){content.dispose();},
 };
}
function focused(f){assert.equal(f.focusEnv.focusBar.hidden,false,'explicit same-object password unlock must retain the readable Back control');assert.equal(f.focusEnv.objectFocus?.shortcutId,shortcutId);assert.equal(f.focusEnv.viewport.dataset.objectFocus,'true');}

test('actual locked→fresh widget provider replacement keeps the same readable focus after explicit unlock',()=>{
 const f=fixture();try{f.unlock();f.projection=widget();f.render();focused(f);assert.equal(f.focusEnv.objectFocus.key,f.content.objectKey(f.node));assert.equal(f.focusEnv.objectFocus.selection,'previous-selection');assert.equal(f.calls.length,0);}finally{f.dispose();}
});
test('the intermediate successful-unlock generation remains focused before fresh protected metadata arrives',()=>{
 const f=fixture();try{f.unlock();f.render();focused(f);const intermediate=f.focusEnv.objectFocus.key;
  // The real engine disposes/recreates a changed provider independently of its
  // focus bar (unified-field.ts previousObject.key reconciliation).
  f.replaceLockedProvider();
  f.projection=widget();f.render();focused(f);assert.notEqual(f.focusEnv.objectFocus.key,intermediate);
 }finally{f.dispose();}
});
test('a sibling placement cannot borrow the existing locked view as its focus witness',()=>{
 const f=fixture();try{f.unlock();f.projection=widget();const next=f.content.objectKey(f.node);
  assert.equal(f.content.canContinueObjectFocus?.({...f.node,shortcutId:'other-placement'},f.oldKey,next)===true,false);
 }finally{f.dispose();}
});
test('the same shortcut cannot continue focus for a different object even with an inconsistent metadata port',()=>{
 const f=fixture();try{f.unlock();f.projection=widget();const next=f.content.objectKey(f.node);
  const other={...f.node,entity:{...f.node.entity,entity:{kind:'object',id:'another-object'}}};
  assert.equal(f.content.canContinueObjectFocus?.(other,f.oldKey,next)===true,false);
 }finally{f.dispose();}
});
test('a disposed locked view cannot authorize a delayed focus continuation',()=>{
 const f=fixture();try{f.view.dispose();f.unlock();f.projection=widget();f.render();assert.equal(f.focusEnv.objectFocus,null);assert.equal(f.focusEnv.focusBar.hidden,true);}finally{f.dispose();}
});
test('protected Native metadata follows the same focus-only continuation without loading plaintext',()=>{
 const f=fixture();try{f.unlock();f.projection={...locked(),view:{schema:'soty.canvas-vault-view.v1',kind:'vault',contentType:noteType,byteLength:12,revision:1,canEdit:true}};f.render();focused(f);assert.equal(f.calls.length,0);}finally{f.dispose();}
});
test('Back before a late unlock cannot be undone by a metadata refresh',()=>{
 const f=fixture();try{f.focusEnv.objectFocus=null;f.unlock();f.projection=widget();f.render();assert.equal(f.focusEnv.objectFocus,null);assert.equal(f.focusEnv.focusBar.hidden,true);}finally{f.dispose();}
});
test('generic providers without the opt-in continue to lose focus on key replacement',()=>{
 const f=fixture();try{delete f.focusEnv.options.canContinueObjectFocus;f.unlock();f.projection=widget();f.render();assert.equal(f.focusEnv.objectFocus,null);assert.equal(f.focusEnv.focusBar.hidden,true);}finally{f.dispose();}
});
for(const mode of ['relock','account','stale-generation','unavailable','hidden','unprotected','changed-object-revision','readonly-lock','stale-target','changed-known-target'])test('focus continuation refuses '+mode,()=>{
 const f=fixture(mode==='changed-known-target'?{initial:widget()}:{});try{
  f.unlock();f.projection=widget();
  if(mode==='relock')f.vault.invalidate(objectId);
  if(mode==='account')f.changeAccount();
  if(mode==='stale-generation')f.staleGeneration();
  if(mode==='unavailable')f.projection={...f.projection,available:false};
  if(mode==='hidden')f.projection={...f.projection,hidden:true};
  if(mode==='unprotected')f.projection={...f.projection,protection:{kind:'none'}};
  if(mode==='changed-object-revision')f.projection={...f.projection,revision:8,accessRevision:4};
  if(mode==='readonly-lock')f.focusEnv.objectFocus.key=f.oldKey.replace(/:unlock$/u,':readonly');
  if(mode==='stale-target')f.projection={...f.projection,view:{...f.projection.view,runtimeAllowed:false}};
  if(mode==='changed-known-target'){const next={revision:2,digest:'d'.repeat(64),profile:pin.profile};f.projection={...f.projection,view:{...f.projection.view,source:next,expectedSource:next}};}
  f.render();assert.equal(f.focusEnv.objectFocus,null);assert.equal(f.focusEnv.focusBar.hidden,true);
 }finally{f.dispose();}
});

test('actual Screen forwards only the trusted CFC continuation port to its engine',()=>{
 const tree=parse(screenSource);let found;function visit(node){if(ts.isCallExpression(node)&&node.expression.getText(tree)==='createUnifiedField'){
  found=node.arguments[0].properties.find(value=>value.name?.getText(tree)==='canContinueObjectFocus')?.initializer;
 }ts.forEachChild(node,visit);}visit(tree);assert.ok(found,'Screen must forward its own current CFC guard');
 const calls=[],env={canvasContent:{canContinueObjectFocus(...args){calls.push(args);return true;}},exports:{}};vm.createContext(env);
 new vm.Script(compile('exports.forward='+found.getText(tree)+';')).runInContext(env);assert.equal(env.exports.forward('node','old','new'),true);assert.deepEqual(calls,[['node','old','new']]);
});

test('the old key-change focus clear is caught by the same explicit-unlock oracle',()=>{
 const anchor="key!==objectFocus.key";assert.ok(guardSource.includes(anchor));
 // Kept literal so this control keeps detecting the original behavior after
 // the opt-in guard is added; the source model and assertions are unchanged.
 const original="if(options.readableObjectFocus&&objectFocus){const node=activeNode(objectFocus.shortcutId),key=node?options.objectKey(node):null;if(!node||key!==objectFocus.key){objectFocus=null;options.onObjectFocusChange?.(null);}}\nif(options.readableObjectFocus){viewport.dataset.objectFocus=String(!!objectFocus);focusBar.hidden=focusBackdrop.hidden=!objectFocus;}";
 const f=fixture({engine:original});try{f.unlock();f.projection=widget();f.render();assert.throws(()=>focused(f),/must retain the readable Back control/u);}finally{f.dispose();}
});
test('negative control: removing the original object revision fence is caught by the same focus-clear oracle',()=>{
 const anchor='||prior.revision!==value.revision';assert.equal(contentSource.split(anchor).length,2);
 const f=fixture({code:contentSource.replace(anchor,'')});try{
  f.unlock();f.projection={...widget(),revision:8,accessRevision:4};f.render();
  assert.throws(()=>assert.equal(f.focusEnv.objectFocus,null,'An object revision change must clear readable focus'),/An object revision change must clear readable focus/u);
 }finally{f.dispose();}
});
test('negative control: removing placement ownership is caught by the same sibling refusal oracle',()=>{
 const anchor='&&next.shortcutId===node.shortcutId';assert.equal(contentSource.split(anchor).length,2);
 const f=fixture({code:contentSource.replace(anchor,'')});try{
  f.unlock();f.projection=widget();const next=f.content.objectKey(f.node);
  assert.throws(()=>assert.equal(f.content.canContinueObjectFocus({...f.node,shortcutId:'other-placement'},f.oldKey,next),false,'A sibling placement must not authorize readable focus'),/A sibling placement must not authorize readable focus/u);
 }finally{f.dispose();}
});
