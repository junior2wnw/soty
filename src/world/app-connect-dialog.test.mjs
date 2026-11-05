import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFile} from 'node:fs/promises';
import ts from 'typescript';
const source=await readFile(new URL('./app-connect-dialog.ts',import.meta.url),'utf8');
const code=ts.transpileModule(source,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
class Element{
  constructor(tag,className='',text=''){Object.assign(this,{tagName:tag,className,textContent:text,value:'',children:[],handlers:new Map(),disabled:false,hidden:false,checked:false,classList:{toggle(){}}});}
  append(...nodes){this.children.push(...nodes);if(this.tagName==='select')for(const node of nodes)if(!this.value||node.selected)this.value=node.value;}
  replaceChildren(...nodes){this.children=[];this.append(...nodes);}setAttribute(name,value){this[name]=value;}focus(){this.focused=true;}reportValidity(){return true;}
  addEventListener(type,fn){this.handlers.set(type,fn);}emit(type){this.handlers.get(type)?.({preventDefault(){}});}
  removeEventListener(type,fn){if(this.handlers.get(type)===fn)this.handlers.delete(type);}
  querySelector(selector){return this.children.find(node=>node.tagName===selector)||this.children.map(node=>node.querySelector?.(selector)).find(Boolean)||null;}
}
async function fixture({pending=null,noDevices=false,storageFailure=false,resultKind,onComplete,confirmError}={}){
  let captured,resolveRun,accountActive=true,devicesMissing=noDevices;const runs=[],completions=[],confirmed=[],footers=[],body=new Element('div'),dialog={body,element:Object.assign(new Element('dialog'),{open:true}),close(){this.element.open=false;this.element.emit('close');}};
  const makeButton=(label,_symbol,css,callback)=>{const e=new Element('button',css);e.append(new Element('span','',label));if(callback)e.addEventListener('click',callback);return e;};
  const ports={
    './dom':{el:(...args)=>new Element(...args),button:makeButton,textInput:(value='',placeholder='',maxLength)=>Object.assign(new Element('input'),{value,placeholder,maxLength}),labeledField:(label,input)=>{const e=new Element('label','',label);e.append(input);return e;},emptyState:(title,_copy,action)=>{const e=new Element('section','',title);e.append(action);return e;}},
    './dialogs':{pinDialogSubmit:(_dialog,_form,submit)=>footers.push(submit),errorText:()=> 'Ошибка подключения'},
    './app-connect-flow.mjs':{createAppConnectJournal(){if(storageFailure)throw Object.assign(Error('storage denied'),{code:'app_connect_storage_unavailable'});return{};},createAppConnectFlow(options){captured=options;return{pending:()=>pending,run(input){runs.push(input);return new Promise(resolve=>{resolveRun=resolve;});},async confirmSelection(value){if(confirmError)throw confirmError;confirmed.push(value);}};}},
    './app-connect-locks.mjs':{createAppConnectLocks:()=>({request:(_name,fn)=>Promise.resolve(fn())})},
    './app-field-placement-browser':{},
  };
  const module={exports:{}};vm.runInNewContext(code,{module,exports:module.exports,require:name=>ports[name]||{},crypto:globalThis.crypto,localStorage:{}});
  await module.exports.mountAppConnectDialog({dialog,accountId:'account-owner',...(resultKind?{resultKind}:{}),api:{async request(){return{devices:devicesMissing?[]:[{claimed:true,online:true,name:'Мой компьютер',hostDeviceId:'host-device',connectorId:'connector-device'}]};}},current:()=>accountActive&&dialog.element.open,accountCurrent:()=>accountActive,connectDevice(){},onComplete:async result=>{completions.push(result);await onComplete?.(result);}});
  const form=body.querySelector('form'),all=(node,tag)=>node.children.flatMap(child=>[...(child.tagName===tag?[child]:[]),...all(child,tag)]);
  return{body,form,runs,completions,confirmed,footers,all,dialog,retire(){accountActive=false;},deviceReady(){devicesMissing=false;dialog.element.emit('focusin');},get captured(){return captured;},resolve:result=>resolveRun(result)};
}

const selection={resultKind:'selection',requestId:'selection-request',app:{id:'app-'+ 'a'.repeat(32),name:'Fresh app'},audience:'private',publication:{}};
const tick=()=>new Promise(resolve=>setImmediate(resolve));
test('Window intent hands off before its local ACK and closes only the exact current child',async()=>{
  let release;const f=await fixture({resultKind:'selection',onComplete:()=>new Promise(resolve=>{release=resolve;})});
  f.all(f.form,'input').find(e=>e['aria-label']==='Адрес приложения').value='localhost:3000';f.form.emit('submit');assert.equal(f.runs[0].resultKind,'selection');f.resolve(selection);await tick();
  assert.equal(f.completions.length,1);assert.equal(f.confirmed.length,0);assert.equal(f.dialog.element.open,true);release();await tick();assert.equal(f.confirmed.length,1);assert.equal(f.dialog.element.open,false);
});
test('an unfinished selection cannot become a shortcut when reopened through the ordinary entry',async()=>{
  const f=await fixture({pending:{complete:false,rejected:false,intent:{name:'Saved Window intent',address:'http://127.0.0.1:3000/',hostDeviceId:'host-device',connectorId:'connector-device',audience:'private',resultKind:'selection'}}});
  f.form.emit('submit');assert.equal(f.runs[0].resultKind,'selection');f.resolve(selection);await tick();assert.equal(f.confirmed.length,1);assert.equal(f.dialog.element.open,false);
});
test('failed or retired chooser handoff leaves the connection unfinished without closing another view',async()=>{
  for(const retired of [false,true]){let release;const f=await fixture({resultKind:'selection',onComplete:()=>new Promise((resolve,reject)=>{release=retired?resolve:()=>reject(Object.assign(Error('unavailable'),{code:'canvas_create_app_unavailable'}));})});
    f.all(f.form,'input').find(e=>e['aria-label']==='Адрес приложения').value='localhost:3000';f.form.emit('submit');f.resolve(selection);await tick();if(retired)f.retire();release();await tick();assert.equal(f.confirmed.length,0);assert.equal(f.dialog.element.open,true);}
});
test('device pairing returns to the same Window connection rather than abandoning its intent',async()=>{
  const f=await fixture({noDevices:true,resultKind:'selection'});f.body.querySelector('button').emit('click');assert.equal(f.dialog.element.open,true);f.deviceReady();await tick();const form=f.body.querySelector('form');assert.ok(form,'return focus must reread the now-owned device');
  f.all(form,'input').find(e=>e['aria-label']==='Адрес приложения').value='localhost:3000';form.emit('submit');assert.equal(f.runs[0].resultKind,'selection');
});
test('one contextual form offers an address, connected device and three audiences, with one pinned primary action',async()=>{
  const f=await fixture();assert.ok(f.form);assert.equal(f.footers.length,1);assert.equal(f.footers[0].querySelector('span').textContent,'Подключить');
  const inputs=f.all(f.form,'input');assert.equal(inputs.find(e=>e['aria-label']==='Адрес приложения').placeholder,'localhost:3000');assert.deepEqual(inputs.filter(e=>e.type==='radio').map(e=>e.value),['private','link','public']);assert.equal(inputs.filter(e=>e.type==='radio').find(e=>e.checked).value,'private');assert.equal(f.all(f.form,'select').length,1);
});
test('submit captures the actual address/device/audience, disables controls and does not add another step',async()=>{
  const f=await fixture(),inputs=f.all(f.form,'input');inputs.find(e=>e['aria-label']==='Адрес приложения').value='localhost:3000';inputs.find(e=>e['aria-label']==='Название приложения').value='Мой проект';
  for(const e of inputs.filter(e=>e.type==='radio'))e.checked=e.value==='public';f.form.emit('submit');f.form.emit('submit');
  assert.equal(f.runs.length,1);assert.equal(f.runs[0].audience,'public');assert.equal(f.runs[0].hostDeviceId,'host-device');assert.equal(f.runs[0].address,'localhost:3000');assert.ok(f.footers[0].disabled);
  f.resolve({app:{id:'real-app'},contextId:'personal',shortcutId:'server-shortcut'});await new Promise(resolve=>setImmediate(resolve));assert.equal(f.completions.length,1);assert.equal(f.footers.length,1);
});
test('unfinished intent reopens with original values locked and one Continue action',async()=>{
  const f=await fixture({pending:{complete:false,rejected:false,intent:{name:'Ранее начатый проект',address:'http://127.0.0.1:3000/',hostDeviceId:'host-device',audience:'link'}}});
  assert.equal(f.footers[0].querySelector('span').textContent,'Продолжить подключение');assert.ok(f.all(f.form,'input').every(e=>e.disabled));assert.equal(f.runs.length,0);
});
test('missing device gives one clear setup action and storage failure gives no false connection success',async()=>{
  const empty=await fixture({noDevices:true});assert.equal(empty.form,null);assert.equal(empty.body.children[0].textContent,'Подключите устройство');assert.equal(empty.footers.length,0);
  const denied=await fixture({storageFailure:true});assert.equal(denied.footers[0].disabled,true);assert.equal(denied.runs.length,0);assert.equal(denied.completions.length,0);
});
test('unfinished community intent remains selectable even when reopened from the ordinary plus entry',async()=>{
  const f=await fixture({pending:{complete:false,rejected:false,intent:{name:'Групповой проект',address:'http://127.0.0.1:3000/',hostDeviceId:'host-device',audience:'community',communityId:'group-project'}}});
  assert.equal(f.all(f.form,'input').filter(e=>e.type==='radio').find(e=>e.checked).value,'community');assert.equal(f.footers.length,1);
});
