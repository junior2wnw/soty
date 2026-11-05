import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {canvasCreateFileKind} from './canvas-create-flow.mjs';
import {vaultErrorCode} from './canvas-vault-local.mjs';
const source=readFileSync(new URL('./canvas-vault-create-dialog.ts',import.meta.url),'utf8'),css=readFileSync(new URL('./canvas-vault-create-dialog.css',import.meta.url),'utf8');
const fixture=ts.createSourceFile('dialog-fixture.mjs',readFileSync(new URL('./canvas-create-dialog.test.mjs',import.meta.url),'utf8'),ts.ScriptTarget.Latest,true,ts.ScriptKind.JS),printer=ts.createPrinter();
const harnessFunction=fixture.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='harness'),nodeClass=harnessFunction.body.statements.find(ts.isClassDeclaration);assert.ok(nodeClass);
const renderer=ts.createSourceFile('renderer.ts',readFileSync(new URL('./canvas-vault-renderer.ts',import.meta.url),'utf8'),ts.ScriptTarget.Latest,true,ts.ScriptKind.TS),errorFunction=renderer.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='canvasVaultErrorText');assert.ok(errorFunction);
const compile=code=>ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'');
const tick=()=>new Promise(resolve=>setImmediate(resolve)),result={operationId:'protected_operation',objectId:'protected_object',shortcutId:'protected_placement',status:'saved',localDurable:true},password='Synthetic dialog password';
function harness({code=source,execute}={}){
  const calls={note:[],file:[],resume:[],remove:[],done:[],disposed:0,picker:0,urls:[],revoked:[]},pending=[];let current=true,closed=false,options;
  const document={activeElement:null},env={exports:{},document,calls,TextEncoder,canvasCreateFileKind,vaultErrorCode,AbortController};vm.createContext(env);
  vm.runInContext(compile(printer.printNode(ts.EmitHint.Unspecified,nodeClass,fixture))+'\nexports.Node=Node;',env);const Node=env.exports.Node;
  const element=new Node('dialog'),dialogBody=new Node('div');element.append(dialogBody);
  const call=(kind,value)=>{calls[kind].push(value);return execute?execute(kind,value):Promise.resolve(result);};
  Object.assign(env,{createDialog:(_title,close)=>({element,body:dialogBody,close(){if(closed)return;closed=true;close();element.remove();}}),
    pinDialogSubmit:(dialog,form,submit)=>{submit.type='submit';const actions=new Node('div','sw-dialog-form-actions');actions.append(submit);dialog.element.append(actions);},
    el:(tag,className='',text='')=>new Node(tag,className,text),button:(text,_icon,className='',callback)=>{const value=new Node('button',className,text);value.type='button';if(callback)value.addEventListener('click',callback);return value;},
    URL:{createObjectURL:file=>{const url='blob:private-preview-'+calls.urls.length;calls.urls.push({url,file});return url;},revokeObjectURL:url=>calls.revoked.push(url)},
    createCanvasVaultCreateFlow:value=>{options=value;return{startNote:(input,password)=>call('note',{input,password}),startFile:(file,password,input)=>call('file',{file,password,input}),resume:(id,password)=>call('resume',{id,password}),async listPending(){return pending;},async removeLocal(id){calls.remove.push(id);const index=pending.findIndex(value=>value.operationId===id);if(index>=0)pending.splice(index,1);},dispose(){calls.disposed++;}};},
  });
  vm.runInContext(compile(printer.printNode(ts.EmitHint.Unspecified,errorFunction,renderer)),env);
  vm.runInContext(compile(code)+'\nexports.open=openCanvasVaultCreateDialog;',env);
  const handle=env.exports.open({api:{request:async()=>{throw Error('No direct UI Source request');}},accountId:'dialog_account',projectId:'dialog_project',isCurrent:()=>current,place:async()=>result,onDone:value=>calls.done.push(value)});
  const flatten=node=>[node,...node.children.flatMap(flatten)],all=()=>flatten(element),find=name=>all().find(node=>node.className.split(/\s+/).includes(name)),label=text=>all().find(node=>node.getAttribute('aria-label')===text);
  function credentials(){label('Пароль нового объекта').value=password;label('Повторите пароль').value=password;}
  return{handle,calls,pending,all,find,label,credentials,options:()=>options,element,changeAccount(){current=false;},get closed(){return closed;}};
}
test('protected composer submits actual text/password once and waits for canonical placement before closing',async()=>{
  let release;const f=harness({execute:()=>new Promise(resolve=>{release=resolve;})});await tick();f.credentials();f.label('Название защищённой записки').value='Личная мысль';f.find('cvc-body').value='Только в шифре';
  const form=f.find('cvc-form');form.emit('submit');form.emit('submit');assert.equal(f.calls.note.length,1);assert.equal(f.calls.note[0].password,password);assert.equal(f.calls.note[0].input.body,'Только в шифре');assert.equal(f.calls.note[0].input.mask.shape,'rect');assert.equal(f.closed,false);
  assert.equal(f.find('cvc-body').readOnly,true);release(result);await tick();assert.equal(f.closed,true);assert.equal(f.calls.done.length,1);assert.equal(f.calls.disposed,1);
});
test('mismatched/short/oversized passwords and unsupported or large files reach no creation method',async()=>{
  const f=harness();f.credentials();f.label('Повторите пароль').value='Different';f.find('cvc-form').emit('submit');assert.match(f.find('cvc-error').textContent,/не совпали/u);
  for(const value of['short','😀'.repeat(300)]){f.label('Пароль нового объекта').value=value;f.label('Повторите пароль').value=value;f.find('cvc-form').emit('submit');}
  for(const file of[new File(['<svg>'],'unsafe.svg',{type:'image/svg+xml'}),new File([new Uint8Array(1024*1024+1)],'large.png',{type:'image/png'})])f.find('cvc-form').emit('drop',{dataTransfer:{files:[file]}});
  assert.equal(f.calls.note.length+f.calls.file.length,0);assert.equal(f.calls.urls.length,0);f.handle.dispose();
});
test('picker/drop/paste preserves one real local file, and close scrubs all entered text/password/preview',async()=>{
  const f=harness(),file=new File(['fixture pixels'],'private.png',{type:'image/png'});f.credentials();f.find('cvc-body').value='Private composer text';f.all().find(node=>node.textContent==='Файл').emit('click');assert.equal(f.calls.picker,1);
  const picker=f.all().find(node=>node.type==='file');picker.files=[file];picker.emit('change');assert.equal(f.calls.urls.length,1);assert.equal(f.find('cvc-file').hidden,false);
  f.find('cvc-form').emit('submit');await tick();assert.equal(f.calls.file[0].file,file);assert.equal(f.calls.file[0].password,password);assert.equal(f.calls.revoked.length,1);
  for(const node of f.all())if(['INPUT','TEXTAREA'].includes(node.tagName))assert.equal(node.value,'');
  const paste=harness();paste.find('cvc-form').emit('paste',{clipboardData:{files:[file]}});assert.equal(paste.find('cvc-file').hidden,false);paste.handle.dispose();
});
async function retryScenario(code=source){
  const f=harness({code,execute:kind=>kind==='note'?Promise.reject(Object.assign(new Error('network_error'),{code:'network_error',operationId:'same_cipher_intent',localDurable:true})):Promise.resolve(result)});f.credentials();f.find('cvc-body').value='Do not duplicate';f.find('cvc-form').emit('submit');await tick();
  assert.equal(f.label('Пароль нового объекта').value,'');assert.equal(f.label('Повторите пароль').hidden,true);assert.equal(f.find('cvc-body').readOnly,true);assert.match(f.find('cvc-status').textContent,/Зашифрованный черновик сохранён/u);
  f.label('Пароль нового объекта').value=password;f.find('cvc-form').emit('submit');await tick();assert.equal(f.calls.note.length,1,'A retry must not create another Source');assert.equal(f.calls.resume[0].id,'same_cipher_intent');assert.equal(f.calls.resume[0].password,password);return f;
}
test('failed creation clears credentials and resumes the same encrypted intent with a freshly entered password',async()=>{await retryScenario();});
test('encrypted recovery rows ask for a password before resume and local deletion performs no Source operation',async()=>{
  const f=harness();f.pending.push({operationId:'pending_cipher',label:'Защищённый черновик',localDurable:true});await tick();f.all().find(node=>node.textContent==='Продолжить').emit('click');assert.equal(f.calls.resume.length,0);assert.equal(f.find('cvc-note').hidden,true);
  f.label('Пароль нового объекта').value=password;f.find('cvc-form').emit('submit');await tick();assert.equal(f.calls.resume[0].id,'pending_cipher');
  const remove=harness();remove.pending.push({operationId:'delete_cipher',label:'Защищённый черновик',localDurable:true});await tick();remove.all().find(node=>node.textContent==='Удалить черновик').emit('click');await tick();assert.deepEqual(remove.calls.remove,['delete_cipher']);assert.equal(remove.calls.note.length+remove.calls.file.length,0);remove.handle.dispose();
});
test('close/account switch prevent late placement callbacks and never keep passwords in dialog fields',async()=>{
  let release;const f=harness({execute:()=>new Promise(resolve=>{release=resolve;})});f.credentials();f.find('cvc-form').emit('submit');f.handle.dispose();release(result);await tick();assert.equal(f.calls.done.length,0);assert.equal(f.label('Пароль нового объекта').value,'');
  const other=harness();other.credentials();other.changeAccount();other.find('cvc-form').emit('submit');assert.equal(other.calls.note.length,0);other.handle.dispose();
});
test('negative retry control catches a new Source; submit has its own pinned keyboard-safe row',async()=>{
  const from='flow.resume(retryId,secret)';assert.equal(source.split(from).length,2);await assert.rejects(retryScenario(source.replace(from,'flow.startNote({body:body.value},secret)')),/A retry must not create another Source/);
  assert.match(source,/pinDialogSubmit\(dialog,form,submit\)/u);assert.match(css,/grid-template-rows:auto minmax\(0,1fr\) auto/u);assert.match(css,/--soty-visible-height/u);assert.match(css,/\.cv-create \[hidden\]\{display:none!important\}/u);assert.doesNotMatch(source,/localStorage|sessionStorage|getUserMedia|MediaRecorder/u);
});
