import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
const tick=()=>new Promise(resolve=>setImmediate(resolve));
const result={operationId:'connected_request',objectId:'object_connected',shortcutId:'shortcut_connected',status:'saved',localDurable:true};
class Element{
 constructor(tag,className='',text=''){Object.assign(this,{tagName:tag.toUpperCase(),className,textContent:text,value:'',children:[],listeners:new Map(),attributes:new Map(),style:{},hidden:false,disabled:false,open:true});}
 append(...nodes){for(const node of nodes){node.parentElement=this;this.children.push(node);if(this.tagName==='SELECT'&&(!this.value||node.selected))this.value=node.value;}}
 replaceChildren(...nodes){this.children=[];this.append(...nodes);}setAttribute(k,v){this.attributes.set(k,String(v));}removeAttribute(k){this.attributes.delete(k);}getAttribute(k){return this.attributes.get(k);}
 get classList(){return{add:name=>{this.className+=' '+name;},remove:name=>{this.className=this.className.split(/\s/u).filter(x=>x!==name).join(' ');},toggle(){}};}
 addEventListener(type,handler,options){if(!this.listeners.has(type))this.listeners.set(type,[]);this.listeners.get(type).push(handler);options?.signal?.addEventListener('abort',()=>this.removeEventListener(type,handler),{once:true});}
 removeEventListener(type,handler){this.listeners.set(type,(this.listeners.get(type)??[]).filter(fn=>fn!==handler));}
 emit(type){for(const handler of this.listeners.get(type)??[])handler({target:this,preventDefault(){}});}focus(){}reportValidity(){return true;}remove(){this.parentElement?.children.splice(this.parentElement.children.indexOf(this),1);}
 querySelector(selector){return this.querySelectorAll(selector)[0]??null;}querySelectorAll(selector){return this.children.flatMap(node=>[...(node.tagName===selector.toUpperCase()?[node]:[]),...node.querySelectorAll(selector)]);}
}
function fixture(){
 const calls={legacy:0,pair:0,mount:[],prepared:[],complete:[],settled:[],done:[],dialogs:0,footers:0,start:0};let current=true;
 const element=new Element('dialog'),body=new Element('div');element.append(body);const button=(label,_icon,css='',callback)=>{const node=new Element('button',css,label);node.append(new Element('span','',label));if(callback)node.addEventListener('click',callback);return node;};
 const flow={async listPending(){return[];},dispose(){},async prepareConnectedWidget(id,options){calls.prepared.push({id,options});return{operationId:id};},async completeConnectedWidget(value){calls.complete.push(value);return result;},async settleConnectedWidget(id){calls.settled.push(id);},async startWidget(){calls.start++;return result;}};
 const ports={
  './dialogs':{createDialog(_title,close){calls.dialogs++;return{element,body,close(){if(!element.open)return;element.open=false;close();}};},pinDialogSubmit(_dialog,form,submit){calls.footers++;element.append(submit);},errorText:()=>''},
  './dom':{el:(...args)=>new Element(...args),button},
  './canvas-create-flow.mjs':{createCanvasCreateFlow:()=>flow,canvasCreateFileKind:()=> 'image'},
  './canvas-voice-capture':{mountCanvasVoiceCapture:()=>({dispose(){},setDisabled(){}})},
  './app-connect-dialog':{async mountAppConnectDialog(options){calls.mount.push(options);}},
 };
 const source=readFileSync(new URL('./canvas-create-dialog.ts',import.meta.url),'utf8'),code=ts.transpileModule(source,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText,module={exports:{}};
 vm.runInNewContext(code,{module,exports:module.exports,require:name=>ports[name]??{},AbortController,setTimeout,clearTimeout,crypto:globalThis.crypto,localStorage:{},URL:{}});
 const handle=module.exports.openCanvasCreateDialog({api:{async request(){return{apps:[]};}},accountId:'account_creator',projectId:'soty',isCurrent:()=>current,place:async()=>result,onDone:value=>calls.done.push(value),onConnectApp:()=>calls.legacy++,onConnectDevice:()=>calls.pair++});
 const all=node=>[node,...node.children.flatMap(all)],find=css=>all(element).find(node=>node.className.split(/\s/u).includes(css));return{calls,handle,element,find,all:()=>all(element),retire(){current=false;}};
}
test('Connect own continues in the existing composer/form/footer instead of opening another chooser',async()=>{
 const f=fixture();await tick();f.find('cc-body').value='Retained note';f.find('cc-title').value='My draft';f.all().find(node=>node.textContent==='Окно').emit('click');await tick();f.all().find(node=>node.tagName==='SELECT').value='hex';f.find('cc-connect-app').emit('click');await tick();
 assert.equal(f.calls.legacy,0,'the old nested-dialog exit must not run for the inline composer path');assert.equal(f.calls.mount.length,1);const mount=f.calls.mount[0];assert.equal(mount.embedded.form,f.find('cc-form'));assert.equal(f.calls.dialogs,1);assert.equal(f.calls.footers,1);assert.equal(f.calls.start,0);assert.equal(mount.resultKind,'canvas');
 await mount.canvas.prepare('connected_request');assert.equal(f.calls.prepared[0].options.mask.shape,'hex');await mount.canvas.complete({requestId:'connected_request',app:{id:'app-'+ 'a'.repeat(32),name:'New app'}});assert.equal(f.calls.complete[0].requestId,'connected_request');
 assert.equal(f.find('cc-body').value,'Retained note');assert.equal(f.find('cc-title').value,'My draft');await mount.onComplete({resultKind:'canvas',requestId:'connected_request',creation:result,app:{id:'app-'+ 'a'.repeat(32),name:'New app'}});assert.equal(f.calls.done.length,1);assert.equal(f.element.open,true,'a separate unsaved note remains available after the widget is already placed');assert.equal(f.find('cc-body').value,'Retained note');assert.equal(f.find('cc-body').parentElement.hidden,false);assert.equal(f.calls.start,0,'completion must not ask for or start a second widget creation');
});
test('successful inline placement closes an empty composer with no second confirmation',async()=>{
 const f=fixture();await tick();f.all().find(node=>node.textContent==='Окно').emit('click');f.find('cc-connect-app').emit('click');await tick();await f.calls.mount[0].onComplete({resultKind:'canvas',creation:result});assert.equal(f.calls.done.length,1);assert.equal(f.element.open,false);assert.equal(f.calls.start,0);
});

test('connecting an app hides the unrelated file-drop instruction and restores it for a note',async()=>{
 const f=fixture();await tick();f.all().find(node=>node.textContent==='Окно').emit('click');f.find('cc-connect-app').emit('click');await tick();
 assert.equal(f.find('cc-drop-hint').hidden,true,'the connection form must not advertise file paste/drop');
 f.find('cc-connect-back').emit('click');f.all().find(node=>node.textContent==='Записка').emit('click');assert.equal(f.find('cc-drop-hint').hidden,false);
});
test('return from inline connection preserves drafts; late completion cannot close a different account UI',async()=>{
 const f=fixture();await tick();f.find('cc-body').value='Keep me';f.all().find(node=>node.textContent==='Окно').emit('click');f.find('cc-connect-app').emit('click');await tick();assert.equal(f.calls.mount.length,1);f.find('cc-connect-back').emit('click');assert.equal(f.find('cc-body').value,'Keep me');assert.equal(f.element.open,true);
 f.retire();await f.calls.mount[0].onComplete({resultKind:'canvas',creation:result});assert.equal(f.calls.done.length,0);assert.equal(f.element.open,true);
});

async function embeddedConnection({pending=null}={}){
 const calls={runs:[],done:[],legacy:0},dialog={element:new Element('dialog'),body:new Element('div'),close(){assert.fail('an embedded completion cannot close a second dialog');}},form=new Element('form'),retained=new Element('textarea'),host=new Element('div'),submit=new Element('button','', 'On canvas');retained.value='Retained parent text';form.append(retained,host);dialog.body.append(form);dialog.element.append(dialog.body,submit);let active=true,resolveRun;
 const button=(label,_icon,css='',callback)=>{const node=new Element('button',css,label);node.append(new Element('span','',label));if(callback)node.addEventListener('click',callback);return node;};
 const ports={
  './dom':{el:(...args)=>new Element(...args),button,textInput:(value='',placeholder='',maxLength)=>Object.assign(new Element('input'),{value,placeholder,maxLength}),labeledField:(label,input)=>{const row=new Element('label','',label);row.append(input);return row;},emptyState:()=>new Element('div')},
  './dialogs':{pinDialogSubmit(){assert.fail('embedded form must reuse the one parent footer');},errorText:()=> 'Connection error'},
  './app-connect-locks.mjs':{createAppConnectLocks:()=>({request:(_key,callback)=>Promise.resolve(callback())})},
  './app-connect-flow.mjs':{createAppConnectJournal:()=>({read:()=>pending}),createAppConnectFlow:()=>({pending:()=>pending,run(input){calls.runs.push(input);return new Promise(resolve=>{resolveRun=resolve;});},confirmSelection(){assert.fail('new Canvas mode must not return to a chooser');}})},
  './app-field-placement-browser':{},
 };
 const code=ts.transpileModule(readFileSync(new URL('./app-connect-dialog.ts',import.meta.url),'utf8'),{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText,module={exports:{}};
 vm.runInNewContext(code,{module,exports:module.exports,require:name=>ports[name]??{},crypto:globalThis.crypto,localStorage:{}});
 const abort=new AbortController();await module.exports.mountAppConnectDialog({dialog,api:{async request(){return{devices:[{claimed:true,online:true,name:'Owned computer',hostDeviceId:'owned_host',connectorId:'owned_connector'}]};}},accountId:'account_creator',current:()=>active,accountCurrent:()=>active,resultKind:'canvas',signal:abort.signal,embedded:{host,form,submit},
  canvas:{projectId:'soty',async prepare(){},async complete(){return result;},async settle(){}},connectDevice(){},onLegacyResume(){calls.legacy++;},onComplete:value=>calls.done.push(value)});
 const all=node=>[node,...node.children.flatMap(all)];return{calls,dialog,form,retained,host,submit,all:()=>all(dialog.element),resolve:value=>resolveRun(value),retire(){active=false;abort.abort();}};
}
test('the actual embedded connection uses the parent form and plain primary button, names the two audiences and completes once',async()=>{
 const f=await embeddedConnection();assert.equal(f.dialog.element.querySelectorAll('form').length,1);assert.equal(f.retained.value,'Retained parent text');assert.equal(f.submit.textContent,'Подключить и разместить');assert.ok(f.all().some(node=>node.textContent==='Приложение'));assert.ok(f.all().some(node=>node.textContent==='Окно появится только у вас.'));
 const address=f.all().find(node=>node.getAttribute('aria-label')==='Адрес приложения');address.value='localhost:3033';f.form.emit('submit');assert.equal(f.calls.runs.length,1);assert.equal(f.calls.runs[0].resultKind,'canvas');assert.equal(f.calls.runs[0].canvasProjectId,'soty');f.resolve({resultKind:'canvas',requestId:'connected_request',creation:result});await tick();assert.equal(f.calls.done.length,1);assert.equal(f.retained.value,'Retained parent text');
});
test('embedding never upgrades an old unfinished chooser intent, and abort removes its form submit listener',async()=>{
 const pending={schema:3,id:'old_selection',complete:false,intent:{resultKind:'selection',address:'http://127.0.0.1:3033/',name:'Previous source',hostDeviceId:'owned_host',connectorId:'owned_connector',audience:'private'}};
 const f=await embeddedConnection({pending});assert.equal(f.submit.disabled,true);f.form.emit('submit');assert.equal(f.calls.runs.length,0);f.all().find(node=>node.textContent==='Продолжить прежнее подключение').emit('click');assert.equal(f.calls.legacy,1);
 const fresh=await embeddedConnection();fresh.retire();fresh.form.emit('submit');assert.equal(fresh.calls.runs.length,0);
});
