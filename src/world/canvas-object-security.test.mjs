import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';

const source=readFileSync(new URL('./canvas-object-security.ts',import.meta.url),'utf8');
const fixture=ts.createSourceFile('fixture.mjs',readFileSync(new URL('./canvas-create-dialog.test.mjs',import.meta.url),'utf8'),ts.ScriptTarget.Latest,true,ts.ScriptKind.JS);
const declaration=fixture.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='harness').body.statements.find(ts.isClassDeclaration),printer=ts.createPrinter();
const compile=code=>ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'');
const tick=()=>new Promise(resolve=>setImmediate(resolve));
const unprotected={locked:false,protection:{kind:'none'},capabilities:{nativeConversion:true,placementProtection:false}};
const complete={server:'protected',device:'complete',protectionComplete:true};
const pending={server:'protected',device:'pending',protectionComplete:false};
function harness({code=source,status=unprotected,protect=async()=>complete,rotate=async()=>{},localRecoveryState,recoverLocalPassword}={}){
 const calls={protect:[],rotate:[],relock:[],recover:[],done:0,busy:[],pending:[]};let current=true;
 const env=vm.createContext({exports:{},document:{activeElement:null},calls,AbortController,TextEncoder,crypto});
 vm.runInContext(compile(printer.printNode(ts.EmitHint.Unspecified,declaration,fixture))+'\nexports.Node=Node;',env);const Node=env.exports.Node,host=new Node('div');
 Object.assign(env,{el:(tag,cls='',text='')=>new Node(tag,cls,text),button:(text,_icon,cls='')=>new Node('button',cls,text),
  labeledField:(text,input)=>{const row=new Node('label');row.append(new Node('span','',text),input);return row;},canvasVaultErrorText:reason=>reason.code??'error'});
 vm.runInContext(compile(code)+'\nexports.mount=mountCanvasObjectSecurity;',env);
 const handle=env.exports.mount(host,{isCurrent:()=>current,actions:{status:async()=>typeof status==='function'?status():status,
  protect:async(...args)=>{calls.protect.push(args);return protect(...args);},rotate:async(...args)=>{calls.rotate.push(args);return rotate(...args);},relock:async(...args)=>{calls.relock.push(args);},...(localRecoveryState?{localRecoveryState}:{}),...(recoverLocalPassword?{async recoverLocalPassword(value){calls.recover.push(value);return recoverLocalPassword(value);}}:{})},
  onDone:async()=>{calls.done++;},onBusyChange:busy=>calls.busy.push(busy),onPendingChange:pending=>calls.pending.push(pending)});
 const all=node=>[node,...node.children.flatMap(all)],nodes=()=>all(host),find=cls=>nodes().find(n=>n.className.split(/\s+/).includes(cls)),label=text=>nodes().find(n=>n.getAttribute('aria-label')===text);
 return{handle,calls,find,label,nodes,changeAccount(){current=false;},credentials(){const field=label('Новый пароль объекта')??label('Пароль объекта');field.value='synthetic-panel-password';label('Повторите новый пароль').value=field.value;},submit(){find('co-security-form').emit('submit');}};
}

test('security panel waits for a real completion receipt and submits once',async()=>{
 let release;const f=harness({protect:()=>new Promise(resolve=>{release=resolve;})});await tick();f.credentials();f.submit();f.submit();await tick();
 assert.equal(f.calls.protect.length,1);assert.equal(f.calls.done,0);assert.equal(f.find('co-security-form').children.at(-1).disabled,true);
 release(complete);await tick();assert.equal(f.calls.done,1);assert.equal(f.label('Новый пароль объекта').value,'');
});

async function assertPendingNotDone(code=source){
 let attempt=0;const f=harness({code,protect:async()=>++attempt===1?pending:complete});await tick();f.credentials();f.submit();await tick();
 assert.equal(f.calls.done,0,'server protection with unresolved device copies is not complete');
 assert.equal(f.calls.pending.at(-1),true);assert.equal(f.calls.rotate.length,0);
 f.credentials();f.submit();await tick();
 assert.equal(f.calls.protect.length,2);assert.equal(f.calls.protect[0][1],f.calls.protect[1][1],'retry keeps the same operation identity');
 assert.equal(f.calls.rotate.length,0,'pending cleanup must not become password rotation');assert.equal(f.calls.done,1);
}
test('server protected / device pending stays visibly unfinished and resumes the same protection',()=>assertPendingNotDone());
test('reopening a pending Source resumes its durable operation identity',async()=>{
 const f=harness({status:{...unprotected,device:'pending',operationId:'durable_source_operation'}});await tick();f.credentials();f.submit();await tick();
 assert.equal(f.calls.protect[0][1],'durable_source_operation');assert.equal(f.calls.done,1);
});
test('negative control catches a panel that treats partial protection as complete',async()=>{
 const mutant=source.replace('if(!result.protectionComplete){','if(false){');assert.notEqual(mutant,source);
 await assert.rejects(assertPendingNotDone(mutant),error=>error.code==='ERR_ASSERTION');
});
test('failed rotation clears secrets, stays open, and leaves retry identity to the durable Vault client',async()=>{
 let attempt=0;const f=harness({status:{locked:false,protection:{kind:'password'}},rotate:async()=>{if(++attempt===1)throw{code:'wrong_password'};}});await tick();
 const set=()=>{f.credentials();f.label('Текущий пароль объекта').value='old-synthetic-password';};set();f.submit();await tick();
 assert.equal(f.calls.done,0);assert.equal(f.label('Текущий пароль объекта').value,'');assert.equal(f.label('Новый пароль объекта').value,'');
 set();f.submit();await tick();assert.equal(f.calls.done,1);
 assert.equal(f.calls.rotate.length,2);for(const args of f.calls.rotate)assert.equal(args.length,2,'the panel never imposes a fresh ID over a retained Vault rotation');
});
test('an unsupported native capability never promises protection or submits a hidden action',async()=>{
 const f=harness({status:{...unprotected,capabilities:{nativeConversion:false,placementProtection:false}}});await tick();
 assert.equal(f.find('co-security-form').hidden,true);assert.equal(f.calls.protect.length,0);assert.match(f.find('co-status').textContent,/пока нельзя/u);
});
test('device and all-device relock stay distinct, and a late account result never calls done',async()=>{
 const f=harness({status:{locked:false,protection:{kind:'password'}}});await tick();
 f.nodes().find(n=>n.textContent==='Закрыть здесь').emit('click');await tick();
 f.nodes().find(n=>n.textContent==='Закрыть на всех устройствах').emit('click');await tick();
 assert.deepEqual(f.calls.relock.map(args=>args[0]),['device','all']);
 let release;const held=harness({protect:()=>new Promise(resolve=>{release=resolve;})});await tick();held.credentials();held.submit();await tick();held.changeAccount();held.handle.dispose();release(complete);await tick();
 assert.equal(held.calls.done,0);assert.equal(held.handle.hasUnsavedChanges(),false);
});

test('unknown status disables the primary action until the capability read actually finishes',async()=>{
 let release;const f=harness({status:()=>new Promise(resolve=>{release=resolve;})});
 assert.equal(f.find('co-security-form').hidden,true);assert.equal(f.find('co-security-form').children.at(-1).disabled,true);
 f.credentials();f.submit();assert.equal(f.calls.protect.length,0);
 release(unprotected);await tick();assert.equal(f.find('co-security-form').hidden,false);assert.equal(f.find('co-security-form').children.at(-1).disabled,false);f.handle.dispose();
});

test('a transient status failure exposes only a real retry and clears secrets before rechecking',async()=>{
 let reads=0;const f=harness({status:()=>{if(++reads===1)throw{code:'network_timeout'};return unprotected;}});await tick();
 assert.equal(f.find('co-security-form').hidden,true);assert.equal(f.find('co-security-form').children.at(-1).disabled,true);
 f.credentials();f.submit();assert.equal(f.calls.protect.length,0);
 const retry=f.nodes().find(n=>n.textContent==='Повторить проверку');assert.equal(retry.hidden,false);retry.emit('click');await tick();
 assert.equal(reads,2);assert.equal(f.label('Новый пароль объекта').value,'');assert.equal(f.label('Повторите новый пароль').value,'');
 assert.equal(f.find('co-error').hidden,true);assert.equal(f.find('co-security-form').children.at(-1).disabled,false);assert.equal(f.calls.done,0);f.handle.dispose();
});

async function assertLatestStatusWins(code=source){
 let reads=0,older,newer;const f=harness({code,status:()=>{reads++;if(reads===1)return unprotected;if(reads>3)return{locked:true,protection:{kind:'password'}};return new Promise(resolve=>{if(reads===2)older=resolve;else newer=resolve;});}});await tick();
 const first=f.handle.refresh(),second=f.handle.refresh();
 newer({locked:true,protection:{kind:'password'},device:'pending',operationId:'durable_latest_operation'});await second;
 older(unprotected);await first;
 assert.equal(f.calls.pending.at(-1),true,'an older status cannot clear the current unfinished device proof');
 assert.equal(f.find('co-security').children[0].textContent,'Защита не завершена');f.credentials();f.submit();await tick();
 assert.equal(f.calls.protect[0][1],'durable_latest_operation');assert.equal(f.calls.rotate.length,0);f.handle.dispose();
}
test('a late older status never replaces the newest pending proof or its durable ID',()=>assertLatestStatusWins());
test('negative control detects a status read without its generation fence',async()=>{
 const mutant=source.replace('if(!current()||generation!==loadGeneration)return;','if(!current())return;');assert.notEqual(mutant,source);
 await assert.rejects(assertLatestStatusWins(mutant),error=>error.code==='ERR_ASSERTION');
});

test('a live refresh during a write is deferred and coalesced, then reads the current proof',async()=>{
 let reads=0,release;const f=harness({status:()=>++reads===1?unprotected:{locked:true,protection:{kind:'password'},device:'complete',protectionComplete:true},protect:()=>new Promise(resolve=>{release=resolve;})});await tick();f.credentials();f.submit();await tick();
 await f.handle.refresh();await f.handle.refresh();assert.equal(reads,1,'a status read cannot overtake the in-flight mutation');
 release(pending);await tick();await tick();assert.equal(reads,2,'the live signal is retained as exactly one post-write read');assert.equal(f.calls.pending.at(-1),false);assert.equal(f.find('co-security').children[0].textContent,'Под паролем');f.handle.dispose();
});

test('missing local coordinator stays blocked and offers recheck without password entry or false completion',async()=>{
 const f=harness({status:{locked:true,protection:{kind:'password'},device:'pending',protectionComplete:false,reason:'native_coordinator_unavailable',capabilities:{nativeConversion:false,placementProtection:false}}});await tick();
 assert.equal(f.calls.pending.at(-1),true);assert.equal(f.find('co-security').children[0].textContent,'Защита не проверена');assert.equal(f.find('co-security-form').hidden,true);
 assert.equal(f.nodes().find(n=>n.textContent==='Повторить проверку').hidden,false);f.credentials();f.submit();await tick();assert.equal(f.calls.protect.length,0);assert.equal(f.calls.rotate.length,0);assert.equal(f.calls.done,0);f.handle.dispose();
});

for(const retirement of ['dispose','account-change'])test(`a late retry cannot update state after ${retirement}`,async()=>{
 let reads=0,release;const f=harness({status:()=>{if(++reads===1)throw{code:'network_timeout'};return new Promise(resolve=>{release=resolve;});}});await tick();
 const retry=f.nodes().find(n=>n.textContent==='Повторить проверку');retry.emit('click');const field=f.label('Новый пароль объекта');field.value='Synthetic volatile password';
 if(retirement==='dispose')f.handle.dispose();else f.changeAccount();
 release({...unprotected,device:'pending',operationId:'late_wrong_account'});await tick();assert.deepEqual(f.calls.pending,[]);assert.equal(f.calls.done,0);
 if(retirement==='dispose')assert.equal(field.value,'');f.handle.dispose();assert.equal(field.value,'');
});

const oldLocalCopies={schema:'soty.vault.local-recovery-state.v1',objectId:'object_local',knownSource:'ready',complete:false,needsPriorLocalPassword:true,unresolvedLegacy:[{slot:'operation_old',kind:'create',reason:'source-unknown'}]};
const protectedStatus={locked:false,protection:{kind:'password'},device:'complete',protectionComplete:true};
test('old local copies use a separate deliberate two-password recovery, keep partial state and never rotate or protect again',async()=>{
 let release;const f=harness({status:protectedStatus,localRecoveryState:async()=>oldLocalCopies,recoverLocalPassword:()=>new Promise(resolve=>{release=resolve;})});await tick();
 const recovery=f.find('co-recovery'),form=recovery.children.find(node=>node.tagName==='FORM');assert.equal(recovery.hidden,false);
 f.label('Текущий пароль для копий').value='current-source-password';f.label('Прежний пароль на этом устройстве').value='prior-local-password';assert.equal(f.handle.hasUnsavedChanges(),true);form.emit('submit');form.emit('submit');await tick();
 assert.equal(f.calls.recover.length,1);assert.deepEqual(JSON.parse(JSON.stringify(f.calls.recover[0])),{currentSourcePassword:'current-source-password',priorLocalPassword:'prior-local-password'});assert.equal(f.calls.rotate.length,0);assert.equal(f.calls.protect.length,0);assert.equal(f.calls.done,0);
 release(oldLocalCopies);await tick();assert.equal(recovery.hidden,false);assert.match(f.find('co-status').textContent,/ещё закрыта/u);assert.equal(f.label('Текущий пароль для копий').value,'');assert.equal(f.label('Прежний пароль на этом устройстве').value,'');assert.equal(f.handle.hasUnsavedChanges(),false);
 f.label('Текущий пароль для копий').value='current-source-password';f.label('Прежний пароль на этом устройстве').value='another-prior-password';form.emit('submit');await tick();release({...oldLocalCopies,complete:true,needsPriorLocalPassword:false,unresolvedLegacy:[]});await tick();assert.equal(recovery.hidden,true);assert.match(f.find('co-status').textContent,/готовы на этом устройстве/u);f.handle.dispose();
});
async function rotationKeepsRecovery(code=source){
 const f=harness({code,status:protectedStatus,localRecoveryState:async()=>oldLocalCopies,recoverLocalPassword:async()=>oldLocalCopies});await tick();f.credentials();f.label('Текущий пароль объекта').value='previous-source-password';f.submit();await tick();await tick();
 assert.equal(f.calls.rotate.length,1);assert.equal(f.calls.done,0,'Source password success must leave unresolved local-copy recovery visible');assert.equal(f.find('co-recovery').open,true);assert.match(f.find('co-status').textContent,/Пароль изменён/u);f.handle.dispose();
}
test('successful Source rotation keeps unresolved old copies visible instead of closing the whole settings panel',()=>rotationKeepsRecovery());
test('negative control: closing after every rotation fails the same unresolved-copy visibility oracle',async()=>{
 const broken=source.replace('if(current()&&(localRecoveryFailed||localRecovery&&!localRecovery.complete))','if(false)');assert.notEqual(broken,source);await assert.rejects(rotationKeepsRecovery(broken),/unresolved local-copy recovery visible/u);
});
test('late local-copy recovery after account retirement clears both passwords and never calls done',async()=>{
 let release;const f=harness({status:protectedStatus,localRecoveryState:async()=>oldLocalCopies,recoverLocalPassword:()=>new Promise(resolve=>{release=resolve;})});await tick();f.label('Текущий пароль для копий').value='current-source-password';f.label('Прежний пароль на этом устройстве').value='prior-local-password';f.find('co-recovery').children.find(node=>node.tagName==='FORM').emit('submit');await tick();f.changeAccount();release({...oldLocalCopies,complete:true});await tick();assert.equal(f.calls.done,0);assert.equal(f.label('Текущий пароль для копий').value,'');assert.equal(f.label('Прежний пароль на этом устройстве').value,'');f.handle.dispose();
});
test('failed local inventory does not turn confirmed Source protection into a new conversion',async()=>{
 const f=harness({status:protectedStatus,localRecoveryState:async()=>{throw Error('local metadata unavailable');},recoverLocalPassword:async()=>oldLocalCopies});await tick();assert.equal(f.find('co-recovery').hidden,false);assert.equal(f.find('co-recovery').children.find(node=>node.tagName==='FORM').hidden,true);assert.equal(f.calls.pending.at(-1),false);assert.equal(f.calls.protect.length,0);f.handle.dispose();
});

test('a complete metadata inventory still offers an explicit two-password recovery after a password change elsewhere',async()=>{
 const ready={...oldLocalCopies,complete:true,needsPriorLocalPassword:false,unresolvedLegacy:[]};
 const f=harness({status:{...protectedStatus,locked:true},localRecoveryState:async()=>ready,recoverLocalPassword:async()=>ready});await tick();
 const route=f.nodes().find(node=>node.tagName==='BUTTON'&&node.textContent==='Пароль менялся на другом устройстве?');
 assert.ok(route&&!route.hidden,'Metadata completeness must not hide the only route to old local copies');assert.equal(f.find('co-recovery').hidden,true);
 route.emit('click');assert.equal(f.find('co-recovery').hidden,false);assert.equal(f.find('co-recovery').open,true);
 f.label('Текущий пароль для копий').value='current-source-password';f.label('Прежний пароль на этом устройстве').value='prior-local-password';
 f.find('co-recovery').children.find(node=>node.tagName==='FORM').emit('submit');await tick();
 assert.equal(f.calls.recover.length,1);assert.equal(f.calls.protect.length,0);assert.equal(f.calls.rotate.length,0);assert.equal(f.calls.done,0);assert.equal(f.label('Текущий пароль для копий').value,'');assert.equal(f.label('Прежний пароль на этом устройстве').value,'');f.handle.dispose();
});
