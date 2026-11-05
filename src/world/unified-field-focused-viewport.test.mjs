import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import * as geometry from './unified-field-layout.mjs';
import * as camera from './unified-field-camera.mjs';
import {fieldObjectFocusFrame,fieldObjectNeedsFocus} from './unified-field-object-focus.mjs';
import {fieldPresentationClip} from './unified-field-presentation.mjs';
import {fieldEntityKey} from '../../modules/field/contract.mjs';

const source=readFileSync(process.env.SOTY_FOCUSED_VIEWPORT_SOURCE??new URL('./unified-field.ts',import.meta.url),'utf8');
function renderer(text=source){
 const ast=ts.createSourceFile('field.ts',text,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS);
 const engine=ast.statements.find(value=>ts.isFunctionDeclaration(value)&&value.name?.text==='createUnifiedField');
 const render=engine.body.statements.find(value=>ts.isFunctionDeclaration(value)&&value.name?.text==='render');assert.ok(render);
 return ts.transpileModule(render.getText(ast)+'\nexports.render=render;',{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText;
}

class Port{
 constructor(tag='div',className='',onWrite=()=>{}){
  this.tag=tag;this.className=className;this.children=[];this.dataset={};this.listeners=new Map();this.attributes=new Map();this.parentElement=null;
  this.style=new Proxy({setProperty(name,value){this[name]=value;},removeProperty(name){delete this[name];}},
   {set:(styles,name,value)=>{styles[name]=value;onWrite(this,name,value);return true;}});
 }
 get classList(){return{add:(...values)=>{this.className+=' '+values.join(' ');},toggle:(name,value)=>{const names=new Set(this.className.split(/\s+/));value?names.add(name):names.delete(name);this.className=[...names].join(' ');}};}
 append(...values){for(const value of values){value.remove();value.parentElement=this;this.children.push(value);}}
 replaceChildren(...values){for(const value of [...this.children])value.remove();this.append(...values);}
 after(value){const parent=this.parentElement;if(!parent)return;value.remove();value.parentElement=parent;parent.children.splice(parent.children.indexOf(this)+1,0,value);}
 remove(){if(this.parentElement)this.parentElement.children=this.parentElement.children.filter(value=>value!==this);this.parentElement=null;}
 contains(value){return value===this||this.children.some(child=>child.contains(value));}
 addEventListener(name,callback){this.listeners.set(name,callback);}
 setAttribute(name,value){this.attributes.set(name,String(value));}
 querySelector(selector){for(const child of this.children){if(selector.startsWith('.')?child.className.split(/\s+/).includes(selector.slice(1)):child.tag===selector)return child;const nested=child.querySelector(selector);if(nested)return nested;}return null;}
}

// The shipped full render function and real layout/focus math execute here.
// Providers synchronously read their host style at every callback, as a native
// layout read can do. These DOM ports do NOT measure real iframe/compositor paint.
function fixture({width=390,height=400,scale=.51,visible,legacy=false,text=source}={}){
 let collect=false,disposed=0;const reads=[],writes=[],view={width,height},position={x:17,y:-23,scale};
 const reference={kind:'object',id:'focused-object'},neighbor={kind:'object',id:'neighbor-object'};
 const document={schema:'soty.field.v2',contexts:[{contextId:'personal',title:'Personal',x:0,y:0}],shortcuts:[
  {shortcutId:'focused',entity:reference,contextId:'personal',placement:{kind:'free',x:0,y:0,width:640,height:420}},
  {shortcutId:'neighbor',entity:neighbor,contextId:'personal',placement:{kind:'free',x:600,y:0,width:340,height:300}},
 ]};
 const entities=[{entity:reference,title:'Allowed app',source:'owner',presentation:{shape:'hex'}},{entity:neighbor,title:'Allowed note',source:'owner',presentation:{shape:'rect'}}];
 const layout=geometry.layoutUnifiedField(document,entities),node=layout.nodes.find(value=>value.shortcutId==='focused');
 const element=(tag='div',name='')=>new Port(tag,name,(owner,property,value)=>{if(collect&&owner.dataset.shortcutId==='focused'&&['left','top','width','height','transform'].includes(property))writes.push({property,value});});
 const viewport=element(),plane=element(),root=element('article'),frame=element('iframe'),editor=element('textarea');
 editor.value='Local draft';editor.selectionStart=2;editor.selectionEnd=8;frame.volatile={scrollTop:179};root.append(frame,editor);
 viewport.getBoundingClientRect=()=>({left:0,top:0,width:view.width,height:view.height});
 let env;
 const sample=phase=>{if(!collect)return;const target=env.nodeElements.get('focused');reads.push({phase,width:parseFloat(target.style.width),height:parseFloat(target.style.height),transform:target.style.transform??'',focus:target.dataset.objectFocus});};
 const provider={element:root,interaction:'focus',update:()=>sample('update'),setVisible:()=>sample('visible'),
  hasUnsavedChanges:()=>false,hasUnconfirmedWork:()=>false,retainWhileHidden:()=>true,dispose(){disposed++;root.remove();}};
 if(legacy)provider.setFocused=()=>sample('legacy-focus');else{provider.glanceSize=()=>{sample('metrics');return{width:480,height:320};};provider.setPresentation=()=>sample('presentation');}
 const byId=id=>layout.nodes.find(value=>value.shortcutId===id);
 env={exports:{},...geometry,...camera,fieldObjectFocusFrame,fieldObjectNeedsFocus,fieldPresentationClip,fieldEntityKey,
  destroyed:false,inPointerFrame:false,activeLayout:()=>layout,viewportSize:()=>view,camera:()=>position,
  viewport,plane,window:{innerWidth:width,...(visible?{visualViewport:visible}:{})},document:{activeElement:null,createElementNS:(_,tag)=>element(tag)},SVG_NS:'http://www.w3.org/2000/svg',nounCount:(count,word)=>`${count} ${word}`,
  mode:'mine',contextFocus:{mine:'personal'},cameraFit:{mine:'manual'},arranging:false,moving:null,contextPreview:null,
  index:geometry.createFieldSpatialIndex(layout.nodes),metrics:geometry.UNIFIED_FIELD_METRICS,liveScene:{contexts:[],shortcuts:[]},
  nodeElements:new Map(),contextElements:new Map(),inspectElements:new Map(),objectViews:new Map(),objectFocusButtons:new Map(),resizeElements:new Map(),
  selectedId:'focused',objectFocus:null,fieldDocument:document,persistence:'saved',gesture:null,spacePanning:false,
  focusBar:element(),focusBackdrop:element(),focusTitle:element(),releaseFocusControls(){},useFocusControls(){sample('controls');},
  options:{readableObjectFocus:true,createObject:node=>node.shortcutId==='focused'?provider:null,objectKey:node=>node.shortcutId==='focused'?'current-authorized-source':null},
  available:byId,activeNode:byId,tools:element(),readySettled:true,el:element,icon:()=>element('svg'),
  renderNode(){},signal:new AbortController().signal,remember(){},schedule(){},emit(){},
 };
 vm.createContext(env);new vm.Script(renderer(text)).runInContext(env);
 const render=()=>env.exports.render();render();
 const originalTarget=env.nodeElements.get('focused'),snapshot=JSON.stringify([document,layout,position]);
 function measure(){reads.length=0;writes.length=0;collect=true;try{render();}finally{collect=false;}return{reads:[...reads],writes:[...writes]};}
 return{env,view,position,document,layout,node,frame,editor,provider,originalTarget,measure,
  focus(){env.objectFocus={shortcutId:'focused',key:'current-authorized-source',selection:'focused'};render();},
  back(){env.objectFocus=null;},expected:()=>fieldObjectFocusFrame(view,visible?{left:visible.offsetLeft??0,top:visible.offsetTop??0,width:visible.width,height:visible.height}:undefined),
  assertRetained(){assert.equal(env.nodeElements.get('focused'),originalTarget);assert.equal(originalTarget.querySelector('iframe'),frame);assert.equal(provider.element.querySelector('textarea'),editor);assert.equal(editor.value,'Local draft');assert.deepEqual([editor.selectionStart,editor.selectionEnd],[2,8]);assert.equal(frame.volatile.scrollTop,179);assert.equal(disposed,0);assert.equal(JSON.stringify([document,layout,position]),snapshot);},
 };
}
function stableFocus(f,result){
 const expected=f.expected(),transform=`scale(${1/f.position.scale})`;
 assert.ok(result.reads.length>0,'the actual provider callbacks must have sampled their host');
 for(const read of result.reads){assert.equal(read.width,expected.width,`${read.phase} must observe final focused width`);assert.equal(read.height,expected.height,`${read.phase} must observe final focused height`);assert.equal(read.transform,transform,`${read.phase} must observe the final inverse scale`);assert.equal(read.focus,'true');}
 for(const write of result.writes){if(write.property==='width')assert.equal(write.value,`${expected.width}px`,'focused render must never publish canonical width');if(write.property==='height')assert.equal(write.value,`${expected.height}px`,'focused render must never publish canonical height');if(write.property==='transform')assert.equal(write.value,transform,'focused render must never clear the inverse scale');}
 f.assertRetained();
}

for(const config of [{width:390,height:400,scale:.2},{width:390,height:400,scale:.51},{width:390,height:400,scale:1},{width:390,height:400,scale:2},
 {width:390,height:844,scale:.51,visible:{offsetLeft:0,offsetTop:0,width:390,height:400}},{width:1280,height:800,scale:.51},
 {width:390,height:400,scale:.51,legacy:true}])test(`already-focused render never publishes placement geometry to providers ${JSON.stringify(config)}`,()=>{
 const f=fixture(config);f.focus();stableFocus(f,f.measure());stableFocus(f,f.measure());
});
test('Back restores canonical placement geometry before provider callbacks without replacing local work',()=>{
 const f=fixture();f.focus();f.back();const result=f.measure();assert.ok(result.reads.length>0);
 for(const read of result.reads){assert.equal(read.width,640);assert.equal(read.height,420);assert.equal(read.transform,'');assert.equal(read.focus,'false');}f.assertRetained();
});

test('negative control: restoring the temporary canonical write is caught by the same provider oracle',()=>{
 const anchor='target.dataset.layout=';assert.equal(source.split(anchor).length,2);
 const broken=source.replace(anchor,"target.style.width=`${node.width}px`;target.style.height=`${node.height}px`;\n      "+anchor);
 const f=fixture({text:broken});f.focus();assert.throws(()=>stableFocus(f,f.measure()),/must observe final focused width|must never publish canonical width/u);
});
