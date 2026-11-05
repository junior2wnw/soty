import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFile} from 'node:fs/promises';
import ts from 'typescript';

// The shell bridge executes for real. app-connect-flow.test tests the workflow
// against real Apps/World SQLite rather than rebuilding its receipts in mocks.
const source=await readFile(new URL('./app.ts',import.meta.url),'utf8');
const compiled=ts.transpileModule(source,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
const result={resultKind:'shortcut',app:{id:'app-'+'a'.repeat(32),name:'Real project'},contextId:'personal',shortcutId:'created-shortcut',audience:'private'};
const deferred=()=>{let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return{promise,resolve,reject};};
async function settle(){for(let n=0;n<5;n++)await new Promise(resolve=>setImmediate(resolve));}
function fixture({fieldReady=Promise.resolve(),blocked=false,refreshTask,selectTask,selectAllowed=true}={}){
  let mounted,composerOptions;const selections=[],module={exports:{}},ports={
    './app-connect-dialog':{async mountAppConnectDialog(options){mounted=options;}},
    './canvas-create-dialog':{openCanvasCreateDialog(options){composerOptions=options;return{element:{open:true},focus(){},async selectConnectedApp(id){selections.push(id);if(selectTask)await selectTask.promise;return selectAllowed;},hasUnsavedChanges:()=>true};}},
    './dom':{el:(tag,className,text)=>({tag,className,textContent:text})},
    './dialogs':{errorText:()=> 'Connection failed'},
  };
  vm.runInNewContext(compiled+'\nexports.Controller=WorldApplication;',{module,exports:module.exports,require:name=>ports[name]||{}});
  const app=Object.create(module.exports.Controller.prototype),dialogs=[],refreshes=[],ownRefreshes=[],toasts=[],opened=[],focused=[];
  const field={ready:fieldReady,hasUnsavedChanges:()=>blocked,flush:async()=>{if(blocked)throw Error('unsaved work');},async refresh(options){refreshes.push(structuredClone(options));if(refreshTask)await refreshTask.promise;},async refreshOwnApps(){ownRefreshes.push(true);},focusContext:id=>focused.push(id)};
  Object.assign(app,{destroyed:false,deskAccount:'account-A',accountGeneration:1,screenSequence:1,view:'world',query:'query',group:null,appStage:null,dialogs:new Set(),desk:{pinnedApps:[]},unifiedField:field,
    homeQuery:'mine-query',unifiedFieldFilters:{mine:'app',world:'all'},unifiedFieldView:{mineContext:'personal',mineFit:'context'},api:{request(){assert.fail('bridge must delegate the complete connection workflow');}},options:{connectDevice(){}},
    toast:message=>toasts.push(message),loading:label=>({textContent:label}),openApplication:async value=>opened.push(value),openAppFieldPlacement(){assert.fail('a second placement chooser is forbidden');},
    dialog(title){const value={title,element:{open:true},body:{append(){},replaceChildren(){}},close(){value.element.open=false;}};dialogs.push(value);return value;},
  });
  app.openAddApp();assert.ok(mounted);
  return{app,field,dialogs,refreshes,ownRefreshes,toasts,opened,focused,selections,get composerOptions(){return composerOptions;},get mounted(){return mounted;},complete:(value=result)=>mounted.onComplete(structuredClone(value))};
}

const selected={resultKind:'selection',requestId:'selected-request',app:result.app,audience:'private',publication:{}};
test('Window connect returns a fresh selection to its exact retained composer without pretending to have placed an app',async()=>{
  const f=fixture();f.app.openCanvasComposer();const composer=f.app.canvasComposer;f.composerOptions.onConnectApp();assert.equal(f.mounted.resultKind,'selection');
  await f.complete(selected);assert.deepEqual(f.selections,[result.app.id]);assert.equal(f.app.canvasComposer,composer);assert.equal(composer.element.open,true);assert.equal(f.dialogs.at(-1).element.open,true,'only dialog confirmation closes the child');
  assert.equal(f.refreshes.length,0);assert.equal(f.focused.length,0);assert.equal(f.toasts.includes('Приложение на полотне'),false);
});
test('a pending Window intent reopened by plain connect uses the composer and never a shortcut fallback',async()=>{
  const f=fixture();await f.complete(selected);assert.deepEqual(f.selections,[result.app.id]);assert.equal(f.composerOptions.initial.kind,'widget');assert.equal(f.refreshes.length,0);assert.equal(f.focused.length,0);
});
test('a changed chooser, current actor, or blocked retained mode leaves selection unconfirmed',async()=>{
  for(const change of ['retired','replaced','busy']){const wait=deferred(),f=fixture({selectTask:wait,selectAllowed:change!=='busy'});f.app.openCanvasComposer();f.composerOptions.onConnectApp();
    const completed=f.complete(selected);await settle();if(change==='retired'){f.app.accountGeneration++;}else if(change==='replaced')f.app.canvasComposer={element:{open:true}};wait.resolve();
    await assert.rejects(completed,{code:'app_connect_selection_context_changed'});assert.equal(f.refreshes.length,0);assert.equal(f.toasts.length,0);assert.equal(f.dialogs.at(-1).element.open,true);}
});
test('device pairing keeps the exact connection child and outer composer available for return',()=>{
  const f=fixture();f.app.runHook=hook=>hook();let paired=0;f.app.options.connectDevice=()=>{paired++;};f.app.openCanvasComposer();f.composerOptions.onConnectApp();f.mounted.connectDevice();assert.equal(paired,1);assert.equal(f.dialogs.at(-1).element.open,true);assert.equal(f.app.canvasComposer.element.open,true);
});

test('connected app stays on the canvas and focuses its persisted context after field readiness',async()=>{
  const ready=deferred(),f=fixture({fieldReady:ready.promise}),finished=f.complete();
  assert.equal(f.dialogs.length,1);assert.equal(f.dialogs[0].element.open,false);assert.equal(f.opened.length,0);assert.equal(f.refreshes.length,0);
  ready.resolve();await finished;assert.deepEqual(f.refreshes,[{preserveView:true}]);assert.deepEqual(f.focused,['personal']);assert.ok(f.toasts.includes('Приложение на полотне'));assert.equal(f.app.unifiedField,f.field);
});
test('unsaved work stays in place without navigation, replacement or query/filter loss',async()=>{
  const f=fixture({blocked:true});await f.complete();assert.equal(f.opened.length,0);assert.equal(f.app.unifiedField,f.field);assert.deepEqual(f.refreshes,[{preserveView:true}]);
  assert.equal(f.app.query,'query');assert.equal(f.app.homeQuery,'mine-query');assert.equal(f.app.view,'world');assert.equal(f.app.unifiedFieldFilters.mine,'app');assert.equal(f.app.unifiedFieldView.mineContext,'personal');
});
test('initial account and context reach the workflow without fabricated app IDs',()=>{
  const f=fixture();assert.equal(f.mounted.accountId,'account-A');assert.equal(f.mounted.preferredContextId,'personal');assert.deepEqual([...f.mounted.initialAppIds],[]);assert.equal(f.mounted.current(),true);assert.equal(f.mounted.accountCurrent(),true);
  f.dialogs[0].close();assert.equal(f.mounted.current(),false);assert.equal(f.mounted.accountCurrent(),true);
});
test('late successful connection refreshes only the same field without opening or a toast',async()=>{
  const ready=deferred(),f=fixture({fieldReady:ready.promise});f.dialogs[0].close();const finished=f.complete();assert.equal(f.ownRefreshes.length,0);ready.resolve();await finished;
  assert.equal(f.ownRefreshes.length,1);assert.equal(f.opened.length,0);assert.equal(f.toasts.length,0);assert.equal(f.focused.length,0);
});
test('late completion after logout, account ABA, screen or field change does nothing',async()=>{
  for(const change of[f=>{f.app.deskAccount='';f.app.accountGeneration++;},f=>{f.app.deskAccount='account-B';f.app.accountGeneration++;f.app.deskAccount='account-A';f.app.accountGeneration++;},f=>{f.app.screenSequence++;},f=>{f.app.unifiedField={ready:Promise.resolve(),refresh(){assert.fail('foreign field');}};}]){
    const f=fixture();change(f);assert.equal(f.mounted.accountCurrent(),false);await f.complete();assert.equal(f.refreshes.length,0);assert.equal(f.ownRefreshes.length,0);assert.equal(f.opened.length,0);assert.equal(f.toasts.length,0);assert.equal(f.dialogs[0].element.open,true);
  }
});
test('late readiness is fenced again if account, screen or field changes while waiting',async()=>{
  for(const change of[f=>{f.app.deskAccount='account-B';f.app.accountGeneration++;},f=>{f.app.screenSequence++;},f=>{f.app.unifiedField=null;}]){
    const ready=deferred(),f=fixture({fieldReady:ready.promise});f.dialogs[0].close();const finished=f.complete();change(f);ready.resolve();await finished;assert.equal(f.refreshes.length,0);assert.equal(f.ownRefreshes.length,0);assert.equal(f.toasts.length,0);
  }
});
test('refresh failure is reported only to the original still-current account and field',async()=>{
  for(const changed of[false,true]){const refresh=deferred(),f=fixture({refreshTask:refresh});const finished=f.complete();await settle();assert.equal(f.refreshes.length,1);if(changed){f.app.deskAccount='account-B';f.app.accountGeneration++;}refresh.reject(Error('read unavailable'));await finished;
    assert.equal(f.toasts.includes('Сота сохранена. Полотно пока не обновилось — повторите обновление.'),!changed);assert.equal(f.toasts.includes('Приложение на полотне'),false);assert.equal(f.opened.length,0);
  }
});
test('primary create goes directly to the contextual form with selected device preserved',()=>{
  const f=fixture(),calls=[];f.app.openAddApp=(...args)=>calls.push(args);f.app.openPreparedApp('selected-device');assert.deepEqual(calls,[[undefined,'selected-device']]);
});
test('a community entry passes its original group context to the shared form',()=>{
  const f=fixture();f.app.openAddApp('group-project');assert.equal(f.mounted.communityId,'group-project');
});
