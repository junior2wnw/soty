import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {createRequire} from 'node:module';
import {createWidgetRuntimeControls} from './app-widget-runtime-controls.mjs';
const HERE=path.dirname(fileURLToPath(import.meta.url)),APP=path.resolve(HERE,'../..');
const ts=createRequire(path.join(APP,'package.json'))('typescript');
const {captureSourcePin}=await import(pathToFileURL(path.join(HERE,'app-project-capture.mjs')));
const compile=text=>ts.transpileModule(text,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
const dialogsCompiled=compile(readFileSync(path.join(HERE,'dialogs.ts'),'utf8'));
const stageText=readFileSync(path.join(HERE,'app-stage.ts'),'utf8'),stageAst=ts.createSourceFile('stage.ts',stageText,ts.ScriptTarget.Latest,true);
const mount=stageAst.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='mountAppStage');
const declarations=mount.body.statements.filter(ts.isVariableStatement).flatMap(n=>[...n.declarationList.declarations]);
const main=declarations.find(n=>n.name.getText(stageAst)==='mainRuntimeControls');
const widget=declarations.find(n=>n.name.getText(stageAst)==='widgetRuntimeControls');
const mainProps=main.initializer.whenFalse.arguments[0].properties,widgetProps=widget.initializer.whenTrue.arguments[0].properties;
const callback=(props,name)=>props.find(n=>n.name.getText(stageAst)===name).initializer.getText(stageAst);
const portsCode=compile(`exports.create=(state,runtime)=>{let displayEpoch=1,runtimePending=false;const current=()=>state.current,launcher={scopedCapture:()=>state.binding};return {
 capture:${callback(mainProps,'capture')},matchesCapture:${callback(mainProps,'matchesCapture')},canAct:${callback(mainProps,'canAct')},epoch(){displayEpoch++},pending(v){runtimePending=v}};};`);
// Reuse the already established deterministic platform port unchanged, while
// running actual production createDialog and the actual main Stage fences.
// This is a component/Source test, never a browser geometry or native focus claim.
const widgetTest=readFileSync(path.join(HERE,'app-widget-runtime-controls.test.mjs'),'utf8');
const from=widgetTest.indexOf('function fixture(){'),to=widgetTest.indexOf('\ntest(',from);
let fixtureText=widgetTest.slice(from,to)
 .replace("name.displayNone=true","name.displayNone=false")
 .replace('const options={view,screen,message,',"const options={presentation:'main',returnTarget:{isCurrent:()=>state.current,resolve:()=>name},view,screen,message,")
 .replace("control=screen.querySelector('.sa-widget-runtime-action')","control=screen.querySelector('.sa-stage-runtime-action')??screen.querySelector('.sa-widget-runtime-action')");
const fixtureFor=factory=>new Function('createWidgetRuntimeControls','portsCode','dialogsCompiled','vm','captureSourcePin','Event','EventTarget',fixtureText+';return fixture;')(factory,portsCode,dialogsCompiled,vm,captureSourcePin,Event,EventTarget)();
const fixture=()=>fixtureFor(createWidgetRuntimeControls);
const css=readFileSync(path.join(HERE,'app-stage.css'),'utf8');
const applyStyles=f=>{
 const main=f.screen.classList.contains('has-stage-runtime-controls');
 f.runtimeActions.displayNone=main&&css.includes('.sa-stage.has-stage-runtime-controls > .sa-runtime-actions { display: none; }');
};
const rect=f=>{
 applyStyles(f);const notice=f.screen.querySelector('.sa-stage-runtime-notice');
 if(!notice||notice.hidden)return{noticeHeight:0,inlineActions:f.runtimeActions.getClientRects().length?4:0};
 const rule=css.match(/\.sa-stage-runtime-notice \{([^}]+)\}/u)?.[1]??'';
 const messageRule=css.match(/\.sa-stage-runtime-notice > \.sa-message \{([^}]+)\}/u)?.[1]??'';
 const controlRule=css.match(/\.sa-stage-runtime-action \{([^}]+)\}/u)?.[1]??'';
 assert.match(rule,/padding: 6px 12px/u);assert.match(rule,/gap: 8px/u);assert.match(messageRule,/max-height: 36px/u);assert.match(controlRule,/height: 44px/u);
 return{noticeHeight:Math.max(36,44)+6+6+1,inlineActions:f.runtimeActions.getClientRects().length?4:0,kind:'CSS budget model, not native pixels'};
};

test('actual Stage wiring preserves widget ports and routes main controls into their explicit mode',()=>{
 assert.equal(callback(mainProps,'presentation'),"'main'");
 for(const name of ['capture','matchesCapture','canAct'])assert.equal(callback(mainProps,name),callback(widgetProps,name),name);
 assert.equal(widget.initializer.whenFalse.getText(stageAst),'null');
 for(const text of ["let messageOwner: 'manual' | 'runtime' | 'renewal' = 'manual';","else if(state==='ready'&&messageOwner==='renewal')showMessage(runtimeDisplay?.snapshot().message??'','runtime');","if (!message.textContent || messageOwner === 'runtime') showMessage(state.message, 'runtime');"]){assert(stageText.includes(text));}
});
test('main notice keeps actual full status visible, one compact action and same iframe; no widget masquerade',()=>{
 const f=fixture();try{const notice=f.screen.querySelector('.sa-stage-runtime-notice');applyStyles(f);assert(notice);assert.equal(f.message.parentElement,notice);assert.equal(f.message.hidden,false);assert.equal(f.message.textContent,'Запуск не подтверждён');assert.equal(f.message.title,f.message.textContent);assert.equal(f.screen.classList.contains('has-widget-runtime-controls'),false);assert.equal(f.screen.querySelector('.sa-widget-runtime-action'),null);assert.equal(f.frame.parentElement,f.runtime);assert.equal(f.runtime.querySelectorAll('iframe').length,1);assert.equal(f.api.attach(f.header,{isCurrent:()=>true,resolve:()=>f.grip}),false);const model=rect(f);assert.equal(model.noticeHeight,57);assert.equal(model.inlineActions,0);assert.deepEqual(f.counts,{retry:0,external:0,dismiss:0,currentEntry:0});}finally{f.api.dispose();}
});
test('main dialog contains full message and all guarded actions, Escape returns to its visible launcher',()=>{
 const f=fixture();try{const d=f.open();assert(d.classList.contains('sa-stage-runtime-dialog'));assert.equal(d.querySelector('.sa-stage-runtime-copy').textContent,f.message.textContent);assert(f.action(d,'Открыть приложение заново'));assert(f.action(d,'Открыть актуальный вход'));assert(f.action(d,'Открыть в новой вкладке'));assert(f.action(d,'Скрыть сообщение'));assert.match(css,/\.sa-stage-runtime-dialog button \{ min-width: 44px; min-height: 44px; \}/u);assert.equal(f.control.getAttribute('aria-expanded'),'true');d.requestClose();f.flush();assert.equal(f.document.activeElement,f.control);assert.equal(f.control.getAttribute('aria-expanded'),'false');assert.equal(f.intervals.size,0);assert.equal(f.frame.parentElement,f.runtime);assert.equal(f.counts.retry,0);}finally{f.api.dispose();}
});
test('notice hide/return and local message updates never move, replace, reload or navigate iframe',()=>{
 const f=fixture();try{const frame=f.frame,parent=frame.parentElement;f.message.hidden=true;f.mutate();assert.equal(rect(f).noticeHeight,0);f.message.textContent='Полное ручное сообщение остаётся доступно в диалоге';f.message.hidden=false;f.mutate();assert.equal(rect(f).noticeHeight,57);const d=f.open(),focused=f.action(d,'Открыть в новой вкладке');focused.focus();f.message.textContent='Новый текущий статус';f.mutate();assert.equal(f.action(d,'Открыть в новой вкладке'),focused);assert.equal(f.document.activeElement,focused);assert.equal(f.frame,frame);assert.equal(frame.parentElement,parent);assert.equal(f.runtime.querySelectorAll('iframe').length,1);assert.equal(f.counts.retry,0);assert.equal(f.counts.external,0);}finally{f.api.dispose();}
});
test('manual retry forwards exactly once; stale old dialog cannot replay or affect a newer dialog',()=>{
 const f=fixture();try{const first=f.open(),old=f.action(first,'Открыть приложение заново');old.click();f.flush();old.click();assert.equal(f.counts.retry,1);const second=f.open(),target=f.action(second,'Открыть в новой вкладке');target.focus();old.click();f.flush();assert.equal(f.document.querySelectorAll('dialog[open]')[0],second);assert.equal(f.document.activeElement,target);assert.equal(f.counts.retry,1);assert.equal(f.frame.parentElement,f.runtime);}finally{f.api.dispose();}
});
test('external remains synchronous within user activation before closing, with no automatic popup',()=>{
 const f=fixture();try{const d=f.open();assert.equal(f.counts.external,0);f.action(d,'Открыть в новой вкладке').click();assert.deepEqual(f.sequence,['popup-sync','dialog-closed']);assert.equal(f.counts.external,1);assert.equal(f.counts.retry,0);}finally{f.api.dispose();}
});
test('dismiss closes notice without Source action and focuses the actual visible main heading',()=>{
 const f=fixture();try{const d=f.open();f.action(d,'Скрыть сообщение').click();f.mutate();f.flush();assert.equal(f.message.hidden,true);assert.equal(rect(f).noticeHeight,0);assert.equal(f.document.activeElement,f.name);assert.equal(f.counts.dismiss,1);assert.equal(f.counts.retry,0);assert.equal(f.counts.external,0);}finally{f.api.dispose();}
});
test('current-entry and pending actions preserve original availability checks',()=>{
 const f=fixture();try{f.options.currentEntry.hidden=false;f.options.retry.hidden=true;let d=f.open();assert.equal(f.action(d,'Открыть приложение заново').hidden,true);f.action(d,'Открыть актуальный вход').click();assert.equal(f.counts.currentEntry,1);f.stagePorts.pending(true);d=f.open();const external=f.action(d,'Открыть в новой вкладке');assert.equal(external.disabled,true);external.dispatchEvent(new Event('click'));assert.equal(f.counts.external,0);}finally{f.api.dispose();}
});
test('actual main fence rejects late epoch/frame/slot/handle/source and current account/route loss',()=>{
 for(const mode of ['epoch','frame','slot','handle','source','account/route']){const f=fixture();try{const d=f.open(),old=f.action(d,'Открыть приложение заново');if(mode==='epoch')f.stagePorts.epoch();if(mode==='frame'){f.frame.remove();f.runtime.append(f.el('iframe'));}if(mode==='slot')f.state.binding={...f.state.binding,slot:{}};if(mode==='handle')f.state.binding={...f.state.binding,handle:'OTHER'};if(mode==='source')f.state.binding.source={...f.state.binding.source,version:2};if(mode==='account/route')f.state.current=false;old.click();f.flush();assert.equal(f.counts.retry,0,mode);assert.notEqual(f.document.activeElement,f.name,mode);}finally{f.api.dispose();}}
});
test('Source hides a notice while dialog is open: Escape returns to heading rather than hidden launcher',()=>{
 const f=fixture();try{const d=f.open();f.message.hidden=true;f.mutate();applyStyles(f);assert.equal(f.screen.querySelector('.sa-stage-runtime-notice').hidden,true);d.requestClose();f.flush();assert.equal(f.document.activeElement,f.name);assert.equal(f.counts.retry,0);}finally{f.api.dispose();}
});
test('dispose restores only status node, closes dialog, removes observers/timer and keeps iframe attached',()=>{
 const f=fixture();f.open();f.api.dispose();f.flush();assert.equal(f.control.isConnected,false);assert.equal(f.message.parentElement,f.screen);assert.equal(f.frame.parentElement,f.runtime);assert.equal(f.intervals.size,0);assert.equal(f.screen.classList.contains('has-stage-runtime-controls'),false);const before=f.counts.retry;f.control.click();assert.equal(f.counts.retry,before);
});
