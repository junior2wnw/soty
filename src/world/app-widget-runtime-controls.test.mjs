import test from 'node:test';import assert from 'node:assert/strict';import vm from 'node:vm';
import {readFileSync} from 'node:fs';import path from 'node:path';import {fileURLToPath,pathToFileURL} from 'node:url';import {createRequire} from 'node:module';
import {createWidgetRuntimeControls} from './app-widget-runtime-controls.mjs';
const HERE=path.dirname(fileURLToPath(import.meta.url)),APP=path.resolve(HERE,'../..');
const ts=createRequire(path.join(APP,'package.json'))('typescript');
const {captureSourcePin}=await import(pathToFileURL(path.join(HERE,'app-project-capture.mjs')));
const dialogsText=readFileSync(path.join(HERE,'dialogs.ts'),'utf8');
const dialogsCompiled=ts.transpileModule(dialogsText,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
const stageText=readFileSync(path.join(HERE,'app-stage.ts'),'utf8'),ast=ts.createSourceFile('stage.ts',stageText,ts.ScriptTarget.Latest,true);
const mount=ast.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='mountAppStage');
const widget=mount.body.statements.filter(ts.isVariableStatement).flatMap(n=>[...n.declarationList.declarations]).find(n=>n.name.getText(ast)==='widgetRuntimeControls');
const props=widget.initializer.whenTrue.arguments[0].properties;
const callback=name=>props.find(n=>n.name.getText(ast)===name).initializer.getText(ast);
const portsCode=ts.transpileModule(`exports.create=(state,runtime)=>{let displayEpoch=1,runtimePending=false;const current=()=>state.current,launcher={scopedCapture:()=>state.binding};return {
 capture:${callback('capture')},matchesCapture:${callback('matchesCapture')},canAct:${callback('canAct')},epoch(){displayEpoch++},pending(v){runtimePending=v}};};`,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;

// Real production createDialog/visibility algorithm and exact app-stage capture
// callbacks. The native platform/DOM below are deterministic ports, not browser QA.
function fixture(){
 const tasks=[],listeners=new Map(),intervals=new Map(),observers=[],modalEvents=[];let next=0,document;
 class Node extends EventTarget{
  constructor(tag,cls='',text=''){super();Object.assign(this,{tagName:tag.toUpperCase(),className:cls,textContent:text,children:[],parentElement:null,attributes:new Map(),dataset:{},hidden:false,disabled:false,open:false,ownerDocument:null,displayNone:false});this.classList={add:(...v)=>{this.className=[...new Set([...this.className.split(' '),...v])].join(' ');},remove:(...v)=>{this.className=this.className.split(' ').filter(n=>!v.includes(n)).join(' ');},contains:v=>this.className.split(' ').includes(v)};}
  get isConnected(){return this===document.body||!!this.parentElement?.isConnected;}
  get tabIndex(){return this.attributes.has('tabindex')?Number(this.attributes.get('tabindex')):/^(BUTTON|IFRAME|INPUT|TEXTAREA|SUMMARY)$/u.test(this.tagName)?0:-1;}set tabIndex(v){this.attributes.set('tabindex',String(v));}
  setAttribute(k,v){this.attributes.set(k,String(v));}getAttribute(k){return this.attributes.get(k)??null;}hasAttribute(k){return k==='open'?this.open:this.attributes.has(k);}removeAttribute(k){this.attributes.delete(k);}
  append(...values){for(const n of values){if(n.parentElement)n.parentElement.children=n.parentElement.children.filter(v=>v!==n);n.parentElement=this;this.children.push(n);}}
  after(n){const p=this.parentElement;if(!p)return;n.remove();n.parentElement=p;p.children.splice(p.children.indexOf(this)+1,0,n);}
  contains(n){return n===this||this.children.some(c=>c.contains(n));}
  remove(){if(this.contains(document.activeElement))document.activeElement=document.body;if(this.parentElement)this.parentElement.children=this.parentElement.children.filter(v=>v!==this);this.parentElement=null;}
  getClientRects(){for(let n=this;n;n=n.parentElement)if(n.hidden||n.inert||n.displayNone)return [];return this.isConnected?[{width:44,height:44}]:[];}
  matches(s){if(s.includes(','))return s.split(',').some(v=>this.matches(v.trim()));if(s===':disabled')return this.disabled;if(s==='dialog[open]')return this.tagName==='DIALOG'&&this.open;if(s==='[hidden]')return this.hidden;if(s==='[inert]')return !!this.inert;if(s==='[aria-disabled="true"]')return this.getAttribute('aria-disabled')==='true';if(s.startsWith('.'))return this.classList.contains(s.slice(1));return this.tagName.toLowerCase()===s;}
  closest(s){return this.matches(s)?this:this.parentElement?.closest(s)??null;}
  querySelectorAll(s){return this.children.flatMap(n=>[...(n.matches(s)?[n]:[]),...n.querySelectorAll(s)]);}
  querySelector(s){return this.querySelectorAll(s)[0]??null;}
  focus(){if(!this.isConnected||this.disabled||!this.getClientRects().length)return;document.activeElement=this;for(const f of listeners.get('focusin')??[])f({target:this});}
  click(){if(!this.disabled)this.dispatchEvent(new Event('click'));}
  showModal(){this.previous=document.activeElement;this.open=true;this.querySelector('button')?.focus();}
  close(){if(!this.open)return;this.open=false;if(this.contains(document.activeElement))document.activeElement=document.body;this.previous?.focus();tasks.push(()=>this.dispatchEvent(new Event('close')));}
  requestClose(){const e=new Event('cancel',{cancelable:true});if(this.dispatchEvent(e))this.close();}
 }
 document={body:new Node('body'),activeElement:null,addEventListener(t,f){if(!listeners.has(t))listeners.set(t,new Set());listeners.get(t).add(f);},removeEventListener(t,f){listeners.get(t)?.delete(f);},querySelectorAll:s=>document.body.querySelectorAll(s)};
 document.body.ownerDocument=document;document.activeElement=document.body;
 const el=(...args)=>Object.assign(new Node(...args),{ownerDocument:document});
 const button=(text,_icon,cls,act)=>{const n=el('button',cls,text);if(act)n.addEventListener('click',act);return n;},iconButton=(text,icon,act)=>button(text,icon,'sw-icon-button',act);
 const globals={document,HTMLElement:Node,getComputedStyle:()=>({visibility:'visible'}),crypto:{randomUUID:()=>String(++next)},Event,exports:{},require:n=>n==='./dom'?{el,button,iconButton}: {}};
 vm.runInNewContext(dialogsCompiled,globals);const dialogs=globals.exports;
 const view={crypto:globals.crypto,setInterval(fn){const id=++next;intervals.set(id,fn);return id;},clearInterval:id=>intervals.delete(id),MutationObserver:class{constructor(fn){this.fn=fn;this.connected=true;observers.push(this);}observe(){}disconnect(){this.connected=false;}}};
 document.defaultView=view;
 const screen=el('section'),header=el('header'),grip=button('grip'),message=el('p','','Запуск не подтверждён'),runtime=el('div'),runtimeActions=el('div'),frame=el('iframe'),name=el('h2');name.displayNone=true;name.tabIndex=-1;
 header.append(grip);runtime.append(frame);screen.append(name,message,runtimeActions,runtime);document.body.append(header,screen);
 const counts={retry:0,external:0,dismiss:0,currentEntry:0},sequence=[];
 const state={current:true,ownerCurrent:true,binding:{slot:{},handle:'TEST_HANDLE',source:{id:'fixture.source',version:1,digest:'f'.repeat(64)}}};
 const portsGlobal={exports:{},captureSourcePin};vm.runInNewContext(portsCode,portsGlobal);const stagePorts=portsGlobal.exports.create(state,runtime);
 const retry=button('retry','', '',()=>{counts.retry++;sequence.push('retry');stagePorts.epoch();});
 const external=button('external','','',()=>{counts.external++;sequence.push('popup-sync');});
 const dismiss=button('dismiss','','',()=>{counts.dismiss++;message.textContent='';message.hidden=true;name.focus();});
 const currentEntry=button('current','','',()=>{counts.currentEntry++;});currentEntry.hidden=true;
 runtimeActions.append(retry,currentEntry,external,dismiss);
 const options={view,screen,message,runtime,runtimeActions,current:()=>state.current,...stagePorts,createDialog:dialogs.createDialog,iconButton,button,el,isFocusTarget:dialogs.isDialogFocusTarget,retry,currentEntry,external,dismiss,onModalChange:(_d,open)=>{modalEvents.push(open);if(!open)sequence.push('dialog-closed');}};
 const api=createWidgetRuntimeControls(options),control=screen.querySelector('.sa-widget-runtime-action');
 const attach=()=>api.attach(header,{isCurrent:()=>state.ownerCurrent,resolve:()=>grip});
 const open=()=>{control.focus();control.click();return document.querySelectorAll('dialog[open]')[0]??null;};
 const action=(d,label)=>d.querySelectorAll('button').find(n=>n.textContent===label);
 const flush=()=>{while(tasks.length)tasks.shift()();};
 return{api,options,state,screen,header,grip,frame,runtime,message,runtimeActions,name,control,counts,sequence,modalEvents,document,el,button,attach,open,action,flush,stagePorts,intervals,
  mutate(){for(const o of observers)if(o.connected)o.fn();},tick(){for(const fn of [...intervals.values()])fn();}};
}

test('one compact fallback; attaching moves one control and never the Native iframe',()=>{const f=fixture();assert.equal(f.control.parentElement.className,'sa-widget-runtime-launcher');assert.equal(f.attach(),true);assert.equal(f.control.parentElement,f.header);assert.equal(f.screen.querySelector('.sa-widget-runtime-launcher').hidden,true);assert.equal(f.frame.parentElement,f.runtime);assert.equal(f.api.attach(f.header,{isCurrent:()=>true,resolve:()=>f.grip}),false);f.api.dispose();});
test('actual createDialog cleanup returns Dismiss focus to visible owner grip; no launch',()=>{const f=fixture();f.attach();const d=f.open();f.action(d,'Скрыть сообщение').click();f.flush();assert.equal(f.document.activeElement,f.grip);assert.equal(f.counts.dismiss,1);assert.equal(f.counts.retry,0);assert.equal(f.frame.parentElement,f.runtime);assert.deepEqual(f.modalEvents,[true,false]);});
test('actual native cancel/queued close restores visible grip with no action, cleanup-before-return',()=>{const f=fixture();f.attach();const d=f.open();d.requestClose();f.flush();assert.equal(f.document.activeElement,f.grip);assert.deepEqual(f.counts,{retry:0,external:0,dismiss:0,currentEntry:0});assert.equal(f.intervals.size,0);});
test('standalone widget dialog returns to its visible fallback instead of hidden stage title',()=>{const f=fixture();const d=f.open();f.action(d,'Скрыть сообщение').click();f.flush();assert.equal(f.document.activeElement,f.control);assert.notEqual(f.document.activeElement,f.name);});
test('manual Retry forwards exactly once then closes; old dialog button cannot replay',()=>{const f=fixture();f.attach();const d=f.open(),old=f.action(d,'Обновить окно');old.click();f.flush();old.click();assert.equal(f.counts.retry,1);assert.equal(f.document.activeElement,f.grip);});

test('stale old-dialog action cannot close a newer dialog or change its focus',()=>{const f=fixture();f.attach();const first=f.open(),old=f.action(first,'Обновить окно');f.action(first,'Скрыть сообщение').click();f.flush();const second=f.open(),focused=f.action(second,'Обновить окно');focused.focus();old.click();f.flush();assert.equal(f.document.querySelectorAll('dialog[open]')[0],second);assert.equal(f.document.activeElement,focused);assert.equal(f.counts.retry,0);assert.equal(f.intervals.size,1);f.api.dispose();});

test('already-queued old lifetime tick cannot close a newer dialog or steal focus',()=>{const f=fixture();f.attach();const first=f.open(),queued=[...f.intervals.values()][0];f.action(first,'Скрыть сообщение').click();f.flush();const second=f.open(),focused=f.action(second,'Открыть в новой вкладке');focused.focus();queued();f.flush();assert.equal(f.document.querySelectorAll('dialog[open]')[0],second);assert.equal(f.document.activeElement,focused);assert.equal(f.counts.external,0);assert.equal(f.intervals.size,1);f.api.dispose();});
test('External forwarding is synchronous before close; no auto popup',()=>{const f=fixture();f.attach();const d=f.open();assert.equal(f.counts.external,0);f.action(d,'Открыть в новой вкладке').click();assert.deepEqual(f.sequence,['popup-sync','dialog-closed']);assert.equal(f.counts.external,1);});
test('pending existing launch disables all writes/popup; no hidden original action bypass',()=>{const f=fixture();f.stagePorts.pending(true);const d=f.open();const a=f.action(d,'Обновить окно');assert.equal(a.disabled,true);a.dispatchEvent(new Event('click'));assert.equal(f.counts.retry,0);f.api.dispose();});
test('explicit current-entry recovery remains offered and guarded',()=>{const f=fixture();f.options.currentEntry.hidden=false;f.options.retry.hidden=true;const d=f.open();assert.equal(f.action(d,'Обновить окно').hidden,true);f.action(d,'Открыть актуальный вход').click();assert.equal(f.counts.currentEntry,1);assert.equal(f.counts.retry,0);});
test('exact app-stage fence rejects epoch/frame/slot/handle/source and actor takeover before dispatch',()=>{for(const mode of ['epoch','frame','slot','handle','source','actor']){const f=fixture();f.attach();const d=f.open(),old=f.action(d,'Обновить окно');if(mode==='epoch')f.stagePorts.epoch();if(mode==='frame'){f.frame.remove();f.runtime.append(f.el('iframe'));}if(mode==='slot')f.state.binding={...f.state.binding,slot:{}};if(mode==='handle')f.state.binding={...f.state.binding,handle:'OTHER'};if(mode==='source')f.state.binding.source={...f.state.binding.source,version:2};if(mode==='actor')f.state.current=false;old.click();f.flush();assert.equal(f.counts.retry,0,mode);assert.notEqual(f.document.activeElement,f.grip,mode);f.api.dispose();}});
test('legacy null binding remains usable without Source privileges',()=>{const f=fixture();f.state.binding=null;const d=f.open();f.action(d,'Обновить окно').click();assert.equal(f.counts.retry,1);});
test('owner loss closes dialog on local lifetime tick and cannot restore stale focus',()=>{const f=fixture();f.attach();f.open();f.state.ownerCurrent=false;f.tick();f.flush();assert.equal(f.document.querySelectorAll('dialog[open]').length,0);assert.notEqual(f.document.activeElement,f.grip);assert.equal(f.intervals.size,0);});
test('dispose closes managed dialog, disconnects observer/timer and removes only its own UI',()=>{const f=fixture();f.attach();f.open();f.api.dispose();f.flush();f.control.click();assert.equal(f.document.querySelectorAll('dialog[open]').length,0);assert.equal(f.control.isConnected,false);assert.equal(f.frame.parentElement,f.runtime);assert.equal(f.intervals.size,0);assert.equal(f.counts.retry,0);});
test('Source/local notice changes update current dialog controls without replacing focused nodes',()=>{const f=fixture();f.attach();const d=f.open(),a=f.action(d,'Обновить окно');a.focus();f.message.textContent='Другой конечный статус';f.mutate();assert.equal(f.action(d,'Обновить окно'),a);assert.equal(f.document.activeElement,a);assert.equal(f.counts.retry,0);f.api.dispose();});
test('queued native close after managed action never steals later deliberate focus',()=>{const f=fixture();f.attach();const d=f.open();f.action(d,'Скрыть сообщение').click();const other=f.button('other');f.document.body.append(other);other.focus();f.flush();assert.equal(f.document.activeElement,other);});
test('invalid or detached header owner cannot take the control',()=>{const f=fixture();const detached=f.el('header');assert.equal(f.api.attach(detached,{isCurrent:()=>true,resolve:()=>f.grip}),false);f.state.ownerCurrent=false;assert.equal(f.attach(),false);assert.equal(f.control.parentElement.className,'sa-widget-runtime-launcher');f.api.dispose();});
test('actual micro-widgets callsite binds the visible header/grip and preserves owner current witness',()=>{
 const text=readFileSync(path.join(HERE,'micro-widgets.ts'),'utf8'),p=ts.createSourceFile('micro.ts',text,ts.ScriptTarget.Latest,true);
 const outer=p.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='mountMicroWidgets'),inner=outer.body.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='mount');
 const call=inner.body.statements.find(n=>ts.isExpressionStatement(n)&&n.getText(p).startsWith('row.stage.mountRuntimeControls'));
 assert.ok(call);const code=ts.transpileModule('exports.bind=(row,header,current,grip)=>{'+call.getText(p)+'}',{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
 const context={exports:{}};vm.runInNewContext(code,context);const f=fixture();let current=true;
 context.exports.bind({stage:{mountRuntimeControls:f.api.attach}},f.header,()=>current,f.grip);
 assert.equal(f.control.parentElement,f.header);const d=f.open();current=false;f.action(d,'Обновить окно').click();assert.equal(f.counts.retry,0);
 assert.doesNotThrow(()=>context.exports.bind({stage:{}},f.header,()=>true,f.grip));f.api.dispose();
});
