import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {canvasCreateFileKind} from './canvas-create-flow.mjs';
const source=readFileSync(new URL('./canvas-create-dialog.ts',import.meta.url),'utf8'),css=readFileSync(new URL('./canvas-create-dialog.css',import.meta.url),'utf8');
const tick=()=>new Promise(resolve=>setImmediate(resolve));
const result={operationId:'creation_intent',objectId:'object_created',shortcutId:'shortcut_created',status:'saved',localDurable:true};
const connectedAppId='app-'+ 'a'.repeat(32),connectedEntry={appId:connectedAppId,domainId:'dom_'+ 'b'.repeat(32),origin:'https://fixture.invalid',path:'/fresh?q=1'};
function harness({code=source,initial,request=async()=>({apps:[]}),start,callbacks=true}={}){
  const calls={note:[],file:[],widget:[],resume:[],remove:[],connect:0,more:0,done:[],disposed:0,picker:0,urls:[],revoked:[],voiceDisposed:0};let current=true,onClose,flowOptions,voiceOptions,closed=false;const pending=[];
  class Node {
    constructor(tag,className='',text=''){this.tagName=tag.toUpperCase();this.className=className;this.textContent=text;this.children=[];this.parentElement=null;this.dataset={};this.attributes=new Map();this.listeners=new Map();this.value='';this.hidden=false;this.disabled=false;this.style={};this.files=[];}
    get classList(){return{add:name=>{this.className+=' '+name;},remove:name=>{this.className=this.className.split(/\s+/u).filter(value=>value!==name).join(' ');}};}
    setAttribute(key,value){this.attributes.set(key,String(value));}removeAttribute(key){this.attributes.delete(key);if(key==='src')this.src='';}getAttribute(key){return this.attributes.get(key)??null;}
    append(...nodes){for(const node of nodes){node.parentElement=this;this.children.push(node);}}replaceChildren(...nodes){this.children=[];this.append(...nodes);}remove(){if(this.parentElement)this.parentElement.children=this.parentElement.children.filter(node=>node!==this);this.parentElement=null;}
    querySelectorAll(selector){return this.children.flatMap(child=>[...(selector==='button'&&child.tagName==='BUTTON'||selector.startsWith('.')&&child.className.split(/\s+/u).includes(selector.slice(1))?[child]:[]),...child.querySelectorAll(selector)]);}
    querySelector(selector){return this.querySelectorAll(selector)[0]??null;}
    insertBefore(node,before){node.remove();const index=this.children.indexOf(before);if(index<0)this.children.push(node);else this.children.splice(index,0,node);node.parentElement=this;}
    addEventListener(type,handler){if(!this.listeners.has(type))this.listeners.set(type,[]);this.listeners.get(type).push(handler);}emit(type,event={}){const value={target:this,preventDefault(){this.prevented=true;},...event};for(const callback of this.listeners.get(type)??[])callback(value);return value;}
    focus(){document.activeElement=this;}click(){if(this.type==='file')calls.picker++;else this.emit('click');}
  }
  const document={activeElement:null},element=new Node('dialog'),dialogBody=new Node('div'),dialogHeader=new Node('header','sw-dialog-header');element.append(dialogHeader,dialogBody);
  const button=(label,_icon,className='',callback)=>{const node=new Node('button',className,label);node.type='button';if(callback)node.addEventListener('click',callback);return node;};
  const execute=(kind,value)=>{calls[kind].push(value);return start?start(kind,value):Promise.resolve(result);};
  const fakeFlow={startNote:input=>execute('note',input),startFile:(file,options)=>execute('file',{file,options}),startWidget:input=>execute('widget',input),resume:id=>execute('resume',id),async listPending(){return pending;},async removeLocal(id){calls.remove.push(id);const index=pending.findIndex(value=>value.operationId===id);if(index>=0)pending.splice(index,1);},dispose(){calls.disposed++;}};
  const context=vm.createContext({exports:{},document,AbortController,setTimeout,clearTimeout,canvasCreateFileKind,
    createDialog:(_title,close)=>{onClose=close;return{element,body:dialogBody,close(){if(closed)return;closed=true;onClose?.();element.remove();}};},
    pinDialogSubmit:(dialog,form,submit)=>{submit.type='submit';const actions=new Node('div','sw-dialog-form-actions');actions.append(submit);dialog.element.append(actions);},
    el:(tag,className='',text='')=>new Node(tag,className,text),button,
    createCanvasCreateFlow:options=>{flowOptions=options;return fakeFlow;},
    mountCanvasVoiceCapture:(_host,options)=>{voiceOptions=options;return{dispose(){calls.voiceDisposed++;},setDisabled(){}};},
    URL:{createObjectURL:file=>{const url=`blob:preview-${calls.urls.length}`;calls.urls.push({url,file});return url;},revokeObjectURL:url=>calls.revoked.push(url)},
  });
  const compiled=ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'');
  vm.runInContext(compiled+'\nexports.open=openCanvasCreateDialog;exports.errorText=canvasCreateErrorText;',context);
  const handle=context.exports.open({api:{request},accountId:'account_creator',projectId:'dialog-tests',isCurrent:()=>current,place:async()=>result,...(initial?{initial}:{}),
    ...(callbacks?{onConnectApp:()=>calls.connect++,onMore:()=>calls.more++}:{}),onDone:value=>calls.done.push(value)});
  const all=node=>[node,...node.children.flatMap(all)],find=className=>all(element).find(node=>node.className.split(/\s+/u).includes(className));
  return{calls,handle,pending,element,find,all:()=>all(element),flowOptions:()=>flowOptions,voiceOptions:()=>voiceOptions,changeAccount(){current=false;},get closed(){return closed;},api:context.exports};
}
test('a newly connected app is selected from its fresh exact authority without erasing a retained note or shape',async()=>{
 const requests=[],f=harness({request:async(op,args)=>{requests.push({op,args});if(op==='apps.directory.search')return{apps:[]};assert.equal(op,'apps.directory.resolve');assert.equal(args.expectedAccountId,'account_creator');assert.deepEqual(Array.from(args.appIds),[connectedAppId]);return{items:[{ref:{kind:'app',id:connectedAppId},available:true,app:{id:connectedAppId,name:'Fresh connected title',entry:connectedEntry}}]};}});
 await tick();f.find('cc-body').value='Незавершённая личная записка';f.find('cc-title').value='Не терять';f.all().find(node=>node.textContent==='Окно').emit('click');await tick();f.all().find(node=>node.tagName==='SELECT').value='hex';
 assert.equal(typeof f.handle.selectConnectedApp,'function','the existing chooser needs a current selection handoff');
 assert.equal(await f.handle.selectConnectedApp(connectedAppId),true);assert.equal(f.closed,false);assert.equal(f.find('cc-body').value,'Незавершённая личная записка');assert.equal(f.find('cc-title').value,'Не терять');
 assert.ok(f.all().some(node=>node.textContent==='Fresh connected title'));assert.equal(f.calls.widget.length,0,'selection alone never pretends to have placed a widget');
 f.find('cc-form').emit('submit');await tick();assert.equal(f.calls.widget.length,1);assert.equal(f.calls.widget[0].appId,connectedAppId);assert.equal(f.calls.widget[0].entry.domainId,connectedEntry.domainId);assert.equal(f.calls.widget[0].entry.path,connectedEntry.path);assert.equal(f.calls.widget[0].mask.shape,'hex');
});
test('a missing/revoked exact app never falls back to a different discovery result',async()=>{
 const f=harness({initial:{kind:'widget',appId:'app-'+ 'c'.repeat(32),name:'Previous selection'},request:async op=>op==='apps.directory.search'?{apps:[{id:'app-'+ 'd'.repeat(32),name:'Unrelated first result'}]}:{items:[{ref:{kind:'app',id:connectedAppId},available:false}]}});
 await tick();assert.equal(typeof f.handle.selectConnectedApp,'function');await assert.rejects(f.handle.selectConnectedApp(connectedAppId),{code:'canvas_create_app_unavailable'});
 assert.equal(f.closed,false);assert.equal(f.calls.widget.length,0);assert.ok(f.all().some(node=>node.textContent==='Previous selection'));assert.equal(f.all().some(node=>node.textContent==='Fresh connected title'),false);
});
test('a late connected-app resolution after account retirement cannot populate the old chooser',async()=>{
 let release;const f=harness({initial:{kind:'widget',appId:'app-'+ 'c'.repeat(32),name:'Previous selection'},request:async op=>op==='apps.directory.search'?{apps:[]}:new Promise(resolve=>{release=resolve;})});
 await tick();assert.equal(typeof f.handle.selectConnectedApp,'function');const pending=f.handle.selectConnectedApp(connectedAppId);await tick();f.changeAccount();release({items:[{ref:{kind:'app',id:connectedAppId},available:true,app:{id:connectedAppId,name:'Late private name',entry:connectedEntry}}]});
 assert.equal(await pending,false);assert.equal(f.all().some(node=>node.textContent==='Late private name'),false);assert.equal(f.calls.widget.length,0);
});

test('a recorded RAM Opus file enters the ordinary source journal only after explicit submit',async()=>{
 const f=harness();await tick();f.all().find(node=>node.textContent==='Голос').emit('click');assert.equal(f.find('cc-voice').hidden,false);
 f.voiceOptions().onRecordingChange(true);assert.equal(f.all().find(node=>node.textContent==='Ещё').disabled,true);f.find('cc-form').emit('submit');assert.equal(f.calls.file.length,0);
 const file=new File(['real capture bytes'],'Голосовая запись.webm',{type:'audio/webm'});f.voiceOptions().onRecordingChange(false);f.voiceOptions().onFile(file);
 assert.equal(f.find('cc-voice').hidden,true);assert.equal(f.find('cc-file').hidden,false);assert.match(f.find('cc-file-name').textContent,/Б/u);assert.equal(f.calls.file.length,0);
 f.find('cc-form').emit('submit');await tick();assert.equal(f.calls.file[0].file,file);assert.equal(f.calls.voiceDisposed,1);
});
test('one note composer submits its real fields once, uses defaults and closes only after the placement result',async()=>{
  let release;const f=harness({start:()=>new Promise(resolve=>{release=resolve;})});await tick();
  f.find('cc-title').value='Мысль';f.find('cc-body').value='Прямо на полотно';const form=f.find('cc-form');form.emit('submit');form.emit('submit');assert.equal(f.calls.note.length,1);assert.equal(f.closed,false);assert.equal(f.find('cc-body').readOnly,true);
  assert.deepEqual(JSON.parse(JSON.stringify(f.calls.note[0])),{title:'Мысль',body:'Прямо на полотно',mask:{shape:'rect'},fontSize:16});release(result);await tick();assert.equal(f.calls.done.length,1);assert.equal(f.closed,true);assert.equal(f.calls.disposed,1);
});
test('picker/drop/paste accept one supported local file and release its preview on close; invalid MIME never reaches creation',async()=>{
  const f=harness();await tick();const file=new File(['pixels'],'Моя фотография.png',{type:'image/png'}),picker=f.all().find(node=>node.type==='file');
  f.all().find(node=>node.textContent==='Файл').emit('click');assert.equal(f.calls.picker,1);picker.files=[file];picker.emit('change');assert.equal(f.find('cc-file').hidden,false);assert.equal(f.calls.urls.length,1);assert.match(f.find('cc-file-name').textContent,/Моя фотография/u);
  f.find('cc-form').emit('submit');await tick();assert.equal(f.calls.file[0].file,file);assert.equal(f.calls.note.length,0);assert.equal(f.calls.revoked.length,1);
  for(const [type,expected]of [['',/определить формат/u],['image/svg+xml',/не поддерживается/u]]){
    const bad=harness();await tick();bad.find('cc-form').emit('drop',{dataTransfer:{files:[new File(['x'],'unknown',{type})]}});assert.match(bad.find('cc-error').textContent,expected);assert.equal(bad.calls.file.length,0);assert.equal(bad.calls.urls.length,0);bad.handle.dispose();
  }
  const pasted=harness();pasted.find('cc-form').emit('paste',{clipboardData:{files:[file]}});assert.equal(pasted.find('cc-file').hidden,false);pasted.handle.dispose();
});
test('app picker searches all permitted apps, supports foreign apps and preserves a Stage supplied exact entry',async()=>{
  const appId='app-'+'b'.repeat(32),f=harness({request:async(op,args)=>{assert.equal(op,'apps.directory.search');assert.equal(args.scope,'all');return{apps:[{id:appId,name:'Чужое доступное приложение',access:'public'}]};}});
  f.all().find(node=>node.textContent==='Окно').emit('click');await tick();f.find('cc-app-result').emit('click');f.find('cc-form').emit('submit');await tick();assert.equal(f.calls.widget[0].appId,appId);assert.equal(f.calls.widget[0].entry,undefined);
  const exact={domainId:'named-entry',path:'/one?mode=two'},stage=harness({initial:{kind:'widget',appId,name:'Открытая страница',entry:exact}});stage.find('cc-form').emit('submit');await tick();assert.deepEqual(JSON.parse(JSON.stringify(stage.calls.widget[0].entry)),exact);
});
test('lost-ACK UI retry resumes its frozen operation rather than creating another note, and pending rows remain recoverable',async()=>{
  const f=harness({start:(kind)=>kind==='note'?Promise.reject(Object.assign(Error('NETWORK_LOST_ACK'),{operationId:'same_intent',localDurable:true})):Promise.resolve(result)});f.find('cc-body').value='Не дублировать';f.find('cc-form').emit('submit');await tick();
  assert.equal(f.find('cc-body').readOnly,true);assert.equal(f.find('cc-fresh').hidden,false);assert.match(f.find('cc-status').textContent,/сохранён на устройстве/u);f.find('cc-form').emit('submit');await tick();assert.deepEqual(f.calls.resume,['same_intent']);assert.equal(f.calls.note.length,1);
});
test('recovery rows resume the exact intent; deleting a local draft does not create or place its source',async()=>{
  const entry={operationId:'pending_operation',kind:'note',stage:'source',createdAt:1,label:'Не дописано',objectId:null,localDurable:true};
  const f=harness();f.pending.push(entry);await tick();assert.equal(f.find('cc-recovery').hidden,false);f.all().find(node=>node.textContent==='Продолжить').emit('click');await tick();assert.deepEqual(f.calls.resume,['pending_operation']);
  const removed=harness();removed.pending.push(entry);await tick();removed.find('cc-remove').emit('click');await tick();assert.deepEqual(removed.calls.remove,['pending_operation']);assert.equal(removed.find('cc-recovery').hidden,true);assert.equal(removed.calls.note.length+removed.calls.file.length+removed.calls.widget.length,0);removed.handle.dispose();
});
test('a new composition is explicit after a failed attempt and a failed app search has a working retry',async()=>{
  const f=harness({start:()=>Promise.reject(Object.assign(Error('NETWORK_LOST_ACK'),{operationId:'kept_intent',localDurable:true}))});f.find('cc-form').emit('submit');await tick();f.find('cc-fresh').emit('click');assert.equal(f.find('cc-body').readOnly,false);f.find('cc-form').emit('submit');await tick();assert.equal(f.calls.note.length,2);assert.equal(f.calls.remove.length,0);f.handle.dispose();
  let attempts=0;const apps=harness({request:async()=>{if(++attempts===1)throw Error('network');return{apps:[]};}});apps.all().find(node=>node.textContent==='Окно').emit('click');await tick();assert.equal(apps.find('cc-app-retry').hidden,false);apps.find('cc-app-retry').emit('click');await tick();assert.equal(attempts,2);assert.equal(apps.find('cc-app-retry').hidden,true);apps.handle.dispose();
});
test('connect/more callbacks stay optional and neither starts source mutation; closing and account changes suppress late actions',async()=>{
  const f=harness();f.all().find(node=>node.textContent==='Окно').emit('click');await tick();f.find('cc-connect-app').emit('click');f.find('cc-more').emit('click');assert.equal(f.calls.connect,1);assert.equal(f.calls.more,1);assert.equal(f.calls.note.length+f.calls.file.length+f.calls.widget.length,0);
  f.changeAccount();f.find('cc-form').emit('submit');assert.equal(f.calls.widget.length,0);f.handle.dispose();assert.equal(f.calls.disposed,1);
  const hidden=harness({callbacks:false});assert.equal(hidden.find('cc-connect-app').hidden,true);assert.equal(hidden.find('cc-more').hidden,true);hidden.handle.dispose();
});
test('late app discovery from a former mode cannot paint over the current composer',async()=>{
  let release;const f=harness({request:()=>new Promise(resolve=>{release=resolve;})});f.all().find(node=>node.textContent==='Окно').emit('click');f.all().find(node=>node.textContent==='Записка').emit('click');release({apps:[{id:'app-'+'c'.repeat(32),name:'Запоздалый'}]});await tick();assert.equal(f.find('cc-apps').hidden,true);assert.equal(f.find('cc-app-result'),undefined);f.handle.dispose();
});
test('negative retry control catches a duplicate source creation, and CSS keeps the submit outside the keyboard-scrolled body',async()=>{
  const broken=source.replace('flow.resume(retryId!)',"flow.startNote({body:body.value})");assert.notEqual(broken,source);
  const f=harness({code:broken,start:()=>Promise.reject(Object.assign(Error('NETWORK_LOST_ACK'),{operationId:'same_intent',localDurable:true}))});f.find('cc-form').emit('submit');await tick();f.find('cc-form').emit('submit');await tick();assert.throws(()=>assert.equal(f.calls.note.length,1));f.handle.dispose();
  assert.match(css,/grid-template-rows:auto minmax\(0,1fr\) auto/u);assert.match(css,/--soty-visible-height/u);assert.match(css,/\.cc-dialog \[hidden\]\{display:none!important\}/u);assert.match(source,/pinDialogSubmit\(dialog,form,submit\)/u);assert.doesNotMatch(source,/getUserMedia|MediaRecorder/u);
});
