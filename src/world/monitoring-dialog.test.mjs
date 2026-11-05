import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import ts from 'typescript';
import {createMonitoringClient} from '../../modules/monitoring/sdk.mjs';
import * as panelModel from './monitoring-panel-model.mjs';

const source=readFileSync(new URL('./monitoring-dialog.ts',import.meta.url),'utf8');
const dialogsSource=readFileSync(new URL('./dialogs.ts',import.meta.url),'utf8');
const appSource=readFileSync(new URL('./app.ts',import.meta.url),'utf8');
const stageSource=readFileSync(new URL('./app-stage.ts',import.meta.url),'utf8');
const compile=text=>ts.transpileModule(text,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
const settle=async()=>{for(let n=0;n<5;n++)await new Promise(resolve=>setImmediate(resolve));};
const deferred=()=>{let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return{promise,resolve,reject};};

// Actual common createDialog + actual monitoring helper. These deterministic
// DOM/platform ports model capture-before-target listeners and queued native
// close/focus; they do not claim native browser layout or keyboard acceptance.
function fixture({code=source,dirty=false,unknown=false}={}){
 const tasks=[],documentListeners=new Map(),dialogs=[],modalEvents=[],panels=[];let next=0,document;
 const top=()=>document.body.querySelectorAll('dialog[open]').at(-1);
 class Element{
  constructor(tag='div',className='',text=''){this.tagName=tag.toUpperCase();this.className=className;this._text=text;this.children=[];this.attributes=new Map();this.dataset={};this.style={};this.listeners=new Map();this.parentElement=null;this.open=false;this.hidden=false;this.disabled=false;this.inert=false;}
  get ownerDocument(){return document;}get isConnected(){return this===document.body||!!this.parentElement?.isConnected;}
  get classList(){return{add:(...names)=>{this.className=[...new Set([...this.className.split(' '),...names])].join(' ');}};}
  get tabIndex(){return this.attributes.has('tabindex')?Number(this.attributes.get('tabindex')):/^(BUTTON|INPUT|TEXTAREA|SELECT|SUMMARY)$/u.test(this.tagName)?0:-1;}
  set tabIndex(value){this.attributes.set('tabindex',String(value));}get textContent(){return this._text+this.children.map(node=>node.textContent).join('');}
  set textContent(value){this.replaceChildren();this._text=String(value);}
  setAttribute(name,value){this.attributes.set(name,String(value));}getAttribute(name){return this.attributes.get(name)??null;}hasAttribute(name){return name==='open'?this.open:this.attributes.has(name);}removeAttribute(name){this.attributes.delete(name);}
  append(...nodes){for(const node of nodes){node.remove();node.parentElement=this;this.children.push(node);}}
  replaceChildren(...nodes){for(const child of this.children){child.parentElement=null;}this.children=[];this._text='';this.append(...nodes);}
  contains(node){return this===node||this.children.some(child=>child.contains(node));}
  remove(){if(this.contains(document.activeElement))document.activeElement=document.body;if(this.parentElement)this.parentElement.children=this.parentElement.children.filter(child=>child!==this);this.parentElement=null;}
  getClientRects(){return this.isConnected&&!this.closest('[hidden], [inert]')?[{width:44,height:44}]:[];}
  matches(selector){if(selector.includes(','))return selector.split(',').some(item=>this.matches(item.trim()));if(selector===':disabled')return this.disabled;if(selector==='[hidden]')return this.hidden;if(selector==='[inert]')return this.inert;if(selector==='[aria-disabled="true"]')return this.getAttribute('aria-disabled')==='true';if(selector==='dialog[open]')return this.tagName==='DIALOG'&&this.open;if(selector.startsWith('.'))return this.className.split(' ').includes(selector.slice(1));if(selector==='[tabindex]')return this.hasAttribute('tabindex');return this.tagName.toLowerCase()===selector;}
  closest(selector){return this.matches(selector)?this:this.parentElement?.closest(selector)??null;}
  querySelectorAll(selector){return this.children.flatMap(child=>[...(child.matches(selector)?[child]:[]),...child.querySelectorAll(selector)]);}
  querySelector(selector){if(selector==='.sw-dialog-header button')return this.querySelector('.sw-dialog-header')?.querySelector('button')??null;if(selector===':scope > summary')return this.children.find(child=>child.tagName==='SUMMARY')??null;return this.querySelectorAll(selector)[0]??null;}
  addEventListener(type,callback,options={}){if(!this.listeners.has(type))this.listeners.set(type,[]);this.listeners.get(type).push({callback,capture:options===true||options.capture===true,once:options.once===true});}
  removeEventListener(type,callback){this.listeners.set(type,(this.listeners.get(type)??[]).filter(item=>item.callback!==callback));}
  dispatchEvent(event){const path=[];for(let node=this;node;node=node.parentElement)path.push(node);let stopped=false,immediate=false;Object.defineProperty(event,'target',{value:this,configurable:true});
   event.stopPropagation=()=>{stopped=true;};event.stopImmediatePropagation=()=>{stopped=immediate=true;};
   const invoke=(node,capture)=>{for(const item of [...(node.listeners.get(event.type)??[])]){if(immediate)break;if(item.capture!==capture)continue;if(item.once)node.removeEventListener(event.type,item.callback);item.callback.call(node,event);}};
   for(const callback of documentListeners.get(event.type)??[])callback(event);
   for(const node of path.toReversed()){invoke(node,true);if(stopped)break;}
   if(!stopped)invoke(this,false);if(event.bubbles&&!stopped)for(const node of path.slice(1)){invoke(node,false);if(stopped)break;}
   return!event.defaultPrevented;
  }
  focus(){if(!this.isConnected||this.disabled||!this.getClientRects().length)return;document.activeElement=this;for(const callback of documentListeners.get('focusin')??[])callback({target:this});}
  click(){if(!this.disabled)this.dispatchEvent(new Event('click',{cancelable:true,bubbles:true}));}
  showModal(){this.previous=document.activeElement;this.open=true;this.querySelector('button')?.focus();}
  close(){if(!this.open)return;this.open=false;if(this.contains(document.activeElement))document.activeElement=document.body;this.previous?.focus();tasks.push(()=>this.dispatchEvent(new Event('close')));}
  requestClose(){if(this.dispatchEvent(new Event('cancel',{cancelable:true})))this.close();}
 }
 document={body:new Element('body'),activeElement:null,querySelectorAll:selector=>document.body.querySelectorAll(selector),
  addEventListener(type,callback){if(!documentListeners.has(type))documentListeners.set(type,new Set());documentListeners.get(type).add(callback);},
  removeEventListener(type,callback){documentListeners.get(type)?.delete(callback);}};document.activeElement=document.body;
 const el=(...args)=>new Element(...args),button=(label,_icon,className='',action)=>{const result=el('button',className,label);if(action)result.addEventListener('click',action);return result;};
 const dom={el,button,iconButton:(label,icon,action)=>button(label,icon,'sw-icon-button',action)};
 function evaluate(text,ports){const exports={};new Function('exports','require','document','HTMLElement','getComputedStyle','crypto','Event',compile(text))
  (exports,name=>ports[name]??{},document,Element,()=>({visibility:'visible'}),{randomUUID:()=>String(++next)},Event);return exports;}
 const common=evaluate(dialogsSource,{'./dom':dom});
 const createDialog=(...args)=>{const result=common.createDialog(...args);dialogs.push(result);return result;};
 const work={dirty,unknown,verified:true,visible:true,disposes:0,flushes:0,refreshes:0,closed:0,current:true,flushFlight:null};
 const mountMonitoringPanel=(host,options)=>{const input=el('input');input.value='UNSAVED_INPUT';host.append(input);
  const panel={ready:Promise.resolve(),refresh:async()=>{work.refreshes++;},setVisible(value){work.visible=value;},hasUnsavedChanges:()=>work.dirty,hasUnconfirmedWork:()=>work.unknown,
   async flush({allowDurablePending=false}={}){work.flushes++;await work.flushFlight;if(work.dirty||work.unknown&&(!allowDurablePending||!work.verified))throw Object.assign(Error('pending'),{code:'monitoring_pending_unconfirmed'});},
   dispose(){work.disposes++;host.replaceChildren();}};panels.push({panel,options,input});return panel;};
 const {openMonitoringDialog}=evaluate(code,{'./dom':dom,'./dialogs':{createDialog},'./monitoring-panel':{mountMonitoringPanel}});
 const opener=button('Показатели');document.body.append(opener);opener.focus();
 const options={api:{},accountId:'owner-A',scope:{kind:'app',appId:'app-'+'a'.repeat(32)},isCurrent:()=>work.current,
  returnTarget:{isCurrent:()=>work.current,resolve:()=>opener},onClosed(){work.closed++;},onModalChange:(dialog,open)=>modalEvents.push({dialog,open})};
 return{work,openMonitoringDialog,options,document,dialogs,panels,modalEvents,opener,el,button,top,
  open(){return openMonitoringDialog(options);},action(dialog,label){return dialog.querySelectorAll('button').find(node=>node.textContent===label);},
  drain(){while(tasks.length)tasks.shift()();},common,createDialog,
 };
}

async function dirtyCloseOracle(code=source){
 const f=fixture({code,dirty:true}),handle=f.open(),parent=f.top(),input=f.panels[0].input;
 parent.querySelector('.sw-dialog-header button').click();await settle();
 try{assert.equal(parent.open,true,'Closing a dirty panel must keep its original editor alive until explicit choice');assert.equal(f.work.disposes,0);assert.equal(input.value,'UNSAVED_INPUT');assert.notEqual(f.top(),parent);assert.equal(f.document.activeElement,f.action(f.top(),'Остаться'));}
 finally{handle.forceClose();f.drain();}
}
test('actual header capture guard keeps the original dirty editor until a safe default confirmation',()=>dirtyCloseOracle());
test('negative control: disabling the actual dirty/pending guard fails the SAME native-close oracle',async()=>{
 const anchor='if(!panel.hasUnsavedChanges()&&!panel.hasUnconfirmedWork())';assert.equal(source.split(anchor).length,2);await assert.rejects(dirtyCloseOracle(source.replace(anchor,'if(true)')),/keep its original editor alive/u);
});
test('cancel, Stay and explicit Close share the real native dialog lifecycle, restore focus and dispose once',async()=>{
 const f=fixture({dirty:true}),handle=f.open(),parent=f.top();parent.requestClose();await settle();let choice=f.top();assert.notEqual(choice,parent);
 choice.requestClose();f.drain();assert.equal(parent.open,true);assert.equal(f.work.disposes,0);
 parent.requestClose();await settle();choice=f.top();f.action(choice,'Остаться').click();f.drain();assert.equal(parent.open,true);assert.equal(f.work.disposes,0);
 parent.requestClose();await settle();f.action(f.top(),'Закрыть панель').click();f.drain();assert.equal(f.document.querySelectorAll('dialog[open]').length,0);assert.equal(f.work.disposes,1);assert.equal(f.work.closed,1);assert.equal(f.document.activeElement,f.opener);handle.forceClose();assert.equal(f.work.disposes,1);
 assert.equal(f.modalEvents.filter(event=>event.open).length,f.modalEvents.filter(event=>!event.open).length);
});
test('clean close is immediate; an unknown durable command needs explicit close and never erases its recovery state',async()=>{
 const clean=fixture(),handle=clean.open();clean.top().requestClose();clean.drain();assert.equal(clean.work.disposes,1);handle.forceClose();
 const f=fixture({unknown:true}),pending=f.open();f.top().requestClose();await settle();const choice=f.top();assert.match(choice.textContent,/результат|Результат/u);f.action(choice,'Закрыть панель').click();f.drain();assert.equal(f.work.disposes,1);assert.equal(f.work.unknown,true);pending.forceClose();
});
test('dirty plus an unconfirmed command warns about losing unapplied form edits as well as resumable command recovery',async()=>{
 const f=fixture({dirty:true,unknown:true}),handle=f.open();f.top().requestClose();await settle();
 try{assert.match(f.top().textContent,/форм[аеуы]|не применены|пропадут/iu,'Resumable SDK intent must not conceal that unsaved form input will be discarded');assert.match(f.top().textContent,/подтвержд|провер|следующ/iu);}
 finally{handle.forceClose();f.drain();}
});
test('host work ports forward flush failures/pending state and forced account cleanup retires all dialogs once',async()=>{
 const f=fixture({dirty:true,unknown:true}),handle=f.open();assert.equal(handle.hasUnsavedChanges(),true);assert.equal(handle.hasUnconfirmedWork(),true);await assert.rejects(handle.flush(),{code:'monitoring_pending_unconfirmed'});
 const held=deferred();f.work.dirty=f.work.unknown=false;f.work.flushFlight=held.promise;let drained=false;const flush=handle.flush().then(()=>{drained=true;});await settle();assert.equal(drained,false);held.resolve();await flush;assert.equal(drained,true);
 f.work.dirty=true;f.top().requestClose();await settle();const retired=f.top();f.work.current=false;handle.forceClose();f.drain();assert.equal(f.document.querySelectorAll('dialog[open]').length,0);assert.equal(f.work.disposes,1);assert.equal(f.panels[0].options.isCurrent(),false);assert.equal(f.panels[0].input.isConnected,false);assert.equal(retired.contains(f.document.activeElement),false);
});

function appMethod(name){const tree=ts.createSourceFile('app.ts',appSource,ts.ScriptTarget.Latest,true),cls=tree.statements.find(node=>ts.isClassDeclaration(node)&&node.name?.text==='WorldApplication');const method=cls.members.find(node=>node.name?.getText(tree)===name);assert.ok(method,name);return method.getText(tree);}
test('actual account-change handler honours explicit confirmation of a durable journal without dropping dirty form work',async()=>{
 for(const dirty of [false,true]){const f=fixture({unknown:true,dirty}),handle=f.open();const code=compile(`class Controller{${appMethod('prepareAccountChange')} ${appMethod('flushScreen')}};exports.Controller=Controller;`),exports={};
 new Function('exports','button','el','hasRetainedNativeDrafts','flushRetainedNativeDrafts',code)(exports,f.button,f.el,()=>false,async()=>{});
 const controller=Object.create(exports.Controller.prototype);Object.assign(controller,{root:f.document.body,monitoringPanel:handle,screenSequence:1,mainOpenEpoch:0,
  accountTask:()=>()=>f.work.current,appRuntimeStages:()=>[],screenHasUnsavedChanges:()=>handle.hasUnsavedChanges(),chatDrafts:{hasVolatile:()=>false},formEdits:{hasUnsavedChanges:()=>false},chatDraftSessions:[],
  toast(){},dialog:f.createDialog});
 const attempt=controller.prepareAccountChange();await settle();f.action(f.top(),'Переключить без сохранения').click();const allowed=await attempt;
 try{assert.equal(allowed,!dirty,dirty?'Explicit confirmation never discards volatile form input':'A user-confirmed durable recovery journal alone must not permanently prevent account switching');assert.equal(f.work.unknown,true,'The original journal remains for its owning account');}
 finally{handle.forceClose();f.drain();}
 }
});

function stagePort(f){
 const tree=ts.createSourceFile('app-stage.ts',stageSource,ts.ScriptTarget.Latest,true),mount=tree.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='mountAppStage');
 const action=mount.body.statements.filter(ts.isVariableStatement).flatMap(node=>[...node.declarationList.declarations]).find(node=>node.name.getText(tree)==='monitoringAction');
 const returned=mount.body.statements.filter(ts.isReturnStatement).at(-1),sync=returned.expression.properties.find(node=>node.name.getText(tree)==='syncPresentation');
 const callback=action.initializer.arguments[3].getText(tree),expected="import('./monitoring-dialog')";assert.equal(callback.split(expected).length,2);
 const code=compile(`exports.create=(state,loadDialog)=>{let monitoringPanel=null,monitoringOpening=false,monitoringOpenEpoch=0;const accountId='owner-A',app={ownerAccountId:accountId,appId:'app-'+ 'a'.repeat(32),name:'Fixture app'},options={api:{},onModalChange:()=>{}};
 const current=()=>state.ownerCurrent!==false,presented=()=>current()&&state.presented,liveAuthorityDenied=false,closeMore=()=>{},showMessage=()=>{},more=state.opener,showPanel=()=>{},syncShortToolbar=()=>{},stopAssist=()=>{},closeDiagnostic=null,fullscreen={leave:async()=>{}};
 const handle={open:${callback.replace(expected,'loadDialog()')},${sync.getText(tree)},panel:()=>monitoringPanel};return handle;};`),exports={};
 new Function('exports',code)(exports);const state={presented:true,opener:f.opener},loads=[];
 const port=exports.create(state,()=>{const held=deferred();loads.push(held);return held.promise;});
 return{state,loads,port,resolve(index){loads[index].resolve({openMonitoringDialog:f.openMonitoringDialog});}};
}
test('actual retained-stage hide does not strand a native monitoring modal with a dead close action',async()=>{
 const f=fixture(),stage=stagePort(f);stage.port.open();stage.resolve(0);await settle();const original=f.top();assert.ok(original);stage.state.presented=false;stage.port.syncPresentation();
 if(original.open){original.requestClose();await settle();}
 try{assert.equal(f.document.querySelectorAll('dialog[open]').length,0,'A hidden stage must never leave an uncloseable body-owned modal above the canvas');}
 finally{stage.port.panel()?.forceClose();f.drain();}
});
test('a late import from a hidden stage never opens a stale dialog after another presentation takes ownership',async()=>{
 const f=fixture(),stage=stagePort(f);stage.port.open();stage.state.presented=false;stage.port.syncPresentation();stage.resolve(0);await settle();assert.equal(f.document.querySelectorAll('dialog[open]').length,0);assert.equal(f.panels.length,0);
});

test('retained native suspend waits for its own queued close before resuming the same editor and caret',async()=>{
 const f=fixture({dirty:true}),handle=f.open(),parent=f.top(),input=f.panels[0].input;input.selectionStart=4;input.selectionEnd=13;input.focus();
 handle.suspend();assert.equal(parent.open,false);assert.equal(parent.hidden,true);assert.equal(input.isConnected,true);assert.equal(f.work.visible,false);assert.equal(f.work.disposes,0);
 const resumed=handle.resume();assert.equal(parent.open,false,'the pending native close must be consumed before showModal');f.drain();assert.equal(await resumed,true);
 assert.equal(f.top(),parent);assert.equal(f.panels.length,1);assert.equal(f.document.activeElement,input);assert.equal(input.value,'UNSAVED_INPUT');assert.equal(input.selectionStart,4);assert.equal(input.selectionEnd,13);assert.equal(f.work.dirty,true);assert.equal(f.work.visible,true);handle.forceClose();f.drain();
});
test('a second hide cancels queued resume and forced owner revocation cannot resurrect a parked modal',async()=>{
 const f=fixture({dirty:true}),handle=f.open();handle.suspend();const oldResume=handle.resume();handle.suspend();f.drain();assert.equal(await oldResume,false);assert.equal(f.document.querySelectorAll('dialog[open]').length,0);assert.equal(f.work.disposes,0);
 assert.equal(await handle.resume(),true);handle.suspend();const late=handle.resume();f.work.current=false;handle.forceClose();f.drain();assert.equal(await late,false);assert.equal(f.work.disposes,1);assert.equal(f.work.closed,1);assert.equal(f.document.querySelectorAll('dialog[open]').length,0);assert.equal(f.panels[0].input.isConnected,false);
});
test('actual stage hide/reopen retains the same dirty admin field and opens no duplicate panel',async()=>{
 const f=fixture({dirty:true}),stage=stagePort(f);stage.port.open();stage.resolve(0);await settle();const input=f.panels[0].input;input.focus();stage.state.presented=false;stage.port.syncPresentation();f.drain();
 assert.equal(f.work.disposes,0,'presentation hiding is not consent to discard a form');assert.equal(input.isConnected,true);assert.equal(stage.port.panel().hasUnsavedChanges(),true);assert.equal(f.document.querySelectorAll('dialog[open]').length,0);
 stage.state.presented=true;stage.port.syncPresentation();await settle();assert.equal(f.panels.length,1);assert.equal(f.panels[0].input,input);assert.equal(input.value,'UNSAVED_INPUT');assert.equal(f.document.activeElement,input);stage.port.panel().forceClose();f.drain();
});
test('a retired lazy import cannot clear the newer presentation opening guard',async()=>{
 const f=fixture(),stage=stagePort(f);stage.port.open();stage.state.presented=false;stage.port.syncPresentation();stage.state.presented=true;stage.port.syncPresentation();stage.port.open();assert.equal(stage.loads.length,2);
 stage.resolve(0);await settle();stage.port.open();assert.equal(stage.loads.length,2,'the old finally must not admit a third import while the new opening is pending');stage.resolve(1);await settle();assert.equal(f.panels.length,1);stage.port.panel().forceClose();f.drain();
});

// Reuse the existing deterministic panel service/DOM ports, executing the full
// current panel TypeScript and canonical SDK. No browser or authority claim.
const panelSource=readFileSync(new URL('./monitoring-panel.ts',import.meta.url),'utf8');
const panelTestSource=readFileSync(new URL('./monitoring-panel.test.mjs',import.meta.url),'utf8');
const panelTree=ts.createSourceFile('panel-fixture.mjs',panelTestSource,ts.ScriptTarget.Latest,true,ts.ScriptKind.JS);
const fixtureNames=new Set(['settle','clone','scope','now','storagePort','deferred','service','adminService','harness']);
const reused=panelTree.statements.filter(node=>ts.isFunctionDeclaration(node)&&fixtureNames.has(node.name?.text)||ts.isVariableStatement(node)&&node.declarationList.declarations.every(part=>fixtureNames.has(part.name.getText(panelTree)))).map(node=>node.getText(panelTree)).join('\n');
const panelHarness=new Function('assert','ts','createMonitoringClient','model','source','css',reused+'\nreturn harness;')(assert,ts,createMonitoringClient,panelModel,panelSource,readFileSync(new URL('./monitoring-panel.css',import.meta.url),'utf8'));
test('actual panel suspension retires reads and polling but retains its dirty form through fresh resume',async()=>{
 const h=panelHarness();await h.panel.ready;h.backend.adminAction={id:'app.listing',label:'Каталог',right:'admin',enabled:true,revision:4,current:{listed:false},fields:[{name:'listed',label:'Показывать',type:'boolean'}]};await h.panel.refresh();
 const field=h.field('listed');field.checked=true;field.emit('input');assert.equal(h.panel.hasUnsavedChanges(),true);h.panel.setVisible(false);const before=h.backend.calls.length;
 assert.equal(h.timers.size,0);await h.panel.refresh();assert.equal(h.backend.calls.length,before);assert.equal(field.isConnected,true);assert.equal(field.checked,true);
 h.panel.setVisible(true);await settle();assert.ok(h.backend.calls.length>before);assert.equal(h.field('listed'),field);assert.equal(field.checked,true);assert.equal(h.panel.hasUnsavedChanges(),true);h.panel.dispose();
});
test('actual SDK journal may survive confirmed account drain; volatile form and storage errors still block it',async()=>{
 const h=panelHarness();await h.panel.ready;h.backend.mode='lost-ack';h.node('mon-collection').children[0].checked=false;h.node('mon-collection').children[0].emit('change');await settle();
 assert.equal(h.panel.hasUnconfirmedWork(),true);await assert.rejects(h.panel.flush(),{code:'monitoring_pending_unconfirmed'});await h.panel.flush({allowDurablePending:true});assert.equal(h.storage.rows.size,1,'confirmation never deletes or re-IDs a durable command');
 h.storage.getItem=()=>{throw Error('storage unavailable');};assert.equal(h.panel.hasUnconfirmedWork(),true);await assert.rejects(h.panel.flush({allowDurablePending:true}),{code:'monitoring_journal_unavailable'});h.panel.dispose();
});
test('actual lost-ACK admin draft transfers only to its exact verified SDK journal, while a failed journal retains volatile input',async()=>{
 const h=panelHarness();await h.panel.ready;h.backend.adminAction={id:'app.listing',label:'Каталог',right:'admin',enabled:true,revision:4,current:{listed:false},fields:[{name:'listed',label:'Показывать',type:'boolean'}]};await h.panel.refresh();
 h.backend.adminMode='lost-ack';h.field('listed').checked=true;h.field('listed').emit('input');h.node('mon-admin-action').emit('submit');h.node('mon-action-confirm').children[2].click();await settle();
 assert.equal(h.panel.hasUnsavedChanges(),false,'the exact input is now in the verified SDK journal');assert.equal(h.panel.hasUnconfirmedWork(),true);assert.equal(h.field('listed').checked,true);await h.panel.flush({allowDurablePending:true});assert.equal(h.storage.rows.size,1);h.panel.dispose();
 const failed=panelHarness();await failed.panel.ready;failed.backend.adminAction={id:'app.listing',label:'Каталог',right:'admin',enabled:true,revision:4,current:{listed:false},fields:[{name:'listed',label:'Показывать',type:'boolean'}]};await failed.panel.refresh();failed.storage.setItem=()=>{throw Error('quota');};
 failed.field('listed').checked=true;failed.field('listed').emit('input');failed.node('mon-admin-action').emit('submit');failed.node('mon-action-confirm').children[2].click();await settle();assert.equal(failed.backend.adminEffects,0);assert.equal(failed.panel.hasUnsavedChanges(),true);assert.equal(failed.field('listed').checked,true);await assert.rejects(failed.panel.flush({allowDurablePending:true}),{code:'monitoring_pending_unconfirmed'});failed.panel.dispose();
});

test('actual global lazy opening is retired by browser route intent even while the same canvas/account remain alive',async()=>{
 const f=fixture(),load=deferred(),location={hash:'#mine'},exports={};const method=appMethod('openMonitoring'),anchor="import('./monitoring-dialog')";assert.equal(method.split(anchor).length,2);
 const code=compile(`class Controller{${method.replace(anchor,'loadDialog()')}};exports.Controller=Controller;`);
 new Function('exports','loadDialog','isDialogFocusTarget','location',code)(exports,()=>load.promise,()=>true,location);
 const controller=Object.create(exports.Controller.prototype);Object.assign(controller,{deskAccount:'owner-A',screenSequence:1,accountGeneration:7,activeRoute:'#mine',monitoringOpening:false,monitoringOpenEpoch:0,monitoringPanel:null,main:f.document.body,api:{},accountTask:()=>()=>f.work.current,toast(){}});
 const opening=controller.openMonitoring();controller.activeRoute='#world';location.hash='#world';load.resolve({openMonitoringDialog:f.openMonitoringDialog});await opening;
 try{assert.equal(f.panels.length,0,'Late import must not reopen a modal after the user went Back to another route in the same live canvas');}
 finally{controller.monitoringPanel?.forceClose();f.drain();}
});
