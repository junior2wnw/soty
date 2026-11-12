import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {createPersonalFieldDocument} from './app-field-placement.mjs';
import {createLiveAppScene} from './live-app-scene.mjs';
import {combineFieldScene} from './unified-field-live.mjs';
import {layoutUnifiedField} from './unified-field-layout.mjs';
import {fieldScreenToWorld} from './unified-field-camera.mjs';
import {fieldEntityKey} from '../../modules/field/contract.mjs';

const parse=(name,source)=>ts.createSourceFile(name,source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS),printer=ts.createPrinter();
const field=parse('field.ts',readFileSync(new URL('./unified-field.ts',import.meta.url),'utf8'));
const screen=parse('screen.ts',readFileSync(new URL(process.env.SOTY_PREVIEW_SOURCE??'./unified-field-screen.ts',import.meta.url),'utf8'));
const named=(nodes,name)=>{const node=nodes.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text===name);assert.ok(node,name);return node;};
const engine=named(field.statements,'createUnifiedField'),host=named(screen.statements,'createUnifiedFieldScreen');
const print=(node,file)=>printer.printNode(ts.EmitHint.Unspecified,node,file);
const compile=source=>ts.transpileModule(source,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
const listener=(owner,event)=>{
  let found;const visit=node=>{if(ts.isCallExpression(node)&&node.expression.getText(field)===owner+'.addEventListener'&&node.arguments[0]?.text===event){assert.equal(found,undefined,'unique actual listener');found=node.arguments[1];}ts.forEachChild(node,visit);};visit(engine);
  assert.ok(found,event);return print(found,field);
};
const pointerCode=compile(['localPoint','flushPointerSample','finishGesture','resetPointers','capturePointer','inspectShortcut','emit'].map(name=>print(named(engine.body.statements,name),field)).join('\n')
  +'\nexports.api={down:'+listener('viewport','pointerdown')+',up:'+listener('viewport','pointerup')+',focus:'+listener('target','focus')+',emit};');
const screenCode=compile(print(named(host.body.statements,'updateSummary'),screen)+'\nexports.update=updateSummary;');
class NodePort{
  constructor(){this.children=[];this.attributes=new Map();}append(...nodes){this.children.push(...nodes);}replaceChildren(...nodes){this.children=nodes;}
  setAttribute(key,value){this.attributes.set(key,value);}querySelector(){return this.children[0]??new NodePort();}get firstElementChild(){return this.children[0];}
}
function fixture({personal=false}={}){
  const ref={kind:'app',id:'app-'+'a'.repeat(32)},item={entity:ref,title:'Admitted application',source:'public'};
  const document=createPersonalFieldDocument({includeBuiltins:false});
  if(personal)document.shortcuts.push({shortcutId:'owned-placement',contextId:document.contexts[0].contextId,slot:[0,0],entity:ref});
  let liveItems=[item],active=true,preview=null,closes=0,paints=0,focusContextId='',camera={x:520,y:0,scale:.69};
  const projection=createLiveAppScene(),scene=()=>projection.update(document,liveItems);
  const layout=()=>layoutUnifiedField(combineFieldScene(document,scene()),[item]);
  const shortcut=personal?document.shortcuts[0]:scene().shortcuts[0];focusContextId=shortcut.contextId;
  const node=()=>layout().nodes.find(value=>value.shortcutId===shortcut.shortcutId);
  const panelNode=()=>Object.assign(new NodePort(),{className:'uf-preview'});
  const screenContext={exports:{},mode:'mine',current:()=>active,selected:null,summary:null,options:{},fieldEntityKey,
    liveAppScene:()=>scene(),closeList(){},closePreview(){preview=null;screenContext.selected=null;closes++;},
    arrange:new NodePort(),arrangeHint:new NodePort(),percent:new NodePort(),scaleControl:new NodePort(),viewPercent:null,undo:new NodePort(),
    element:{dataset:{}},contextSelector:new NodePort(),contextKey:'',searchScene:()=>({contexts:[]}),
    publishKnownApps(){},refreshAppCoverage(){},updateMineEmpty(){},updateStatus(){},renderOwnApps(){},
    el:()=>new NodePort(),
  };
  vm.runInNewContext(screenCode,screenContext);
  const pointers=new Map(),captures=new Set(),timers=new Map(),element={dataset:{preview:'false'}};let nextTimer=0;
  const summary=()=>({document,mode:'mine',focusContextId,arranging:false,camera:{...camera},canUndo:false});
  const engineContext={exports:{},performance,destroyed:false,mode:'mine',fieldDocument:document,layout:layout(),metrics:{},objectFocus:null,
    gesture:null,pinch:null,pendingPointer:null,inPointerFrame:false,moving:null,contextPreview:null,suppressClickUntil:0,lastInspection:null,arranging:false,spacePanning:false,
    pointers,nodeElements:new Map([[shortcut.shortcutId,element]]),selectedId:'',node:node(),
    available:id=>layout().nodes.find(value=>value.shortcutId===id),activeNode:id=>layout().nodes.find(value=>value.shortcutId===id),
    viewport:{clientWidth:390,clientHeight:844,dataset:{},getBoundingClientRect:()=>({left:0,top:0}),hasPointerCapture:id=>captures.has(id),setPointerCapture:id=>captures.add(id),releasePointerCapture:id=>captures.delete(id)},
    viewportSize:()=>({width:390,height:844}),camera:()=>camera,summary,fieldScreenToWorld,
    options:{commit:()=>assert.fail('inspection must not write the private document'),onStateChange:value=>screenContext.exports.update(value),
      onInspect:(entity,id)=>{assert.equal(fieldEntityKey(entity.entity),fieldEntityKey(ref));screenContext.selected={item:entity,shortcutId:id};preview=panelNode();}},
    setTimeout:(callback,delay)=>{assert.equal(delay,450);timers.set(++nextTimer,callback);return nextTimer;},clearTimeout:id=>timers.delete(id),
    remember(){},schedule(){},render(){paints++;},cancel(){},announce(){},focusPreview(){assert.fail('actual non-preview node must inspect directly');},
    setCamera(){assert.fail('inspection must not move the camera');},
  };
  vm.runInNewContext(pointerCode,engineContext);
  const target={dataset:{shortcutId:shortcut.shortcutId,preview:'false'},closest:selector=>selector==='[data-shortcut-id]'?target:null};
  const event=()=>({pointerId:1,pointerType:'touch',button:0,buttons:1,isPrimary:true,clientX:180,clientY:240,target,preventDefault(){},stopPropagation(){}});
  return{document,shortcut,get preview(){return preview;},get closes(){return closes;},get paints(){return paints;},camera:()=>({...camera}),
    hold(){engineContext.exports.api.down(event());assert.equal(timers.size,1);const callback=timers.values().next().value;timers.clear();callback();assert.ok(preview,'the genuine long-press inspector opened before release');},
    release(){engineContext.exports.api.up(event());engineContext.exports.api.focus();},
    emit(){engineContext.exports.api.emit();},withdraw(){liveItems=[];},retire(){active=false;},focusElsewhere(){focusContextId='another-context';},
    substitute(){screenContext.selected.item={...item,entity:{kind:'app',id:'app-'+'b'.repeat(32)}};},
  };
}

test('production touch long-press survives pointerup and focus-listener state emission for an admitted live app',()=>{
  const f=fixture(),before=JSON.stringify(f.document),camera=f.camera();assert.equal(f.document.shortcuts.length,0);
  f.hold();f.release();assert.ok(f.preview,'release/state emission must not close a live-only admitted shortcut');
  f.emit();assert.ok(f.preview);assert.equal(f.closes,0);assert.ok(f.paints>0);
  assert.equal(JSON.stringify(f.document),before,'live inspection never pins or copies a public app into the private layout');assert.deepEqual(f.camera(),camera);
});

test('withdrawal or unavailable live admission closes the same captured preview even when its metadata and ID remain known',()=>{
  const f=fixture();f.hold();f.withdraw();f.release();assert.equal(f.preview,null,'withdrawn admitted scene must close the captured preview');assert.equal(f.closes,1);assert.equal(f.document.shortcuts.length,0);
});

test('account retirement, context mismatch and substituted entity cannot preserve a preview by shortcut ID alone',()=>{
  for(const personal of [false,true])for(const change of ['retire','focusElsewhere','substitute']){const f=fixture({personal});f.hold();f[change]();f.release();assert.equal(f.preview,null,change);assert.equal(f.closes,1);}
});

test('a genuine private placement retains its existing inspector behavior across the same release sequence',()=>{
  const f=fixture({personal:true}),before=JSON.stringify(f.document);f.hold();f.release();assert.ok(f.preview);assert.equal(f.closes,0);assert.equal(JSON.stringify(f.document),before);
});
