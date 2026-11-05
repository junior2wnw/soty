import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
const source=readFileSync(new URL('./app.ts',import.meta.url),'utf8'),ast=ts.createSourceFile('app.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS),printer=ts.createPrinter();
let beforeUnload,updateGuard,stagesMethod;
function visit(node){
 if(ts.isMethodDeclaration(node)&&node.name.getText(ast)==='appRuntimeStages')stagesMethod=node;
 if(ts.isCallExpression(node)&&node.expression.getText(ast)==='window.addEventListener'&&node.arguments[0]?.text==='beforeunload')beforeUnload=node.arguments[1];
 if(ts.isCallExpression(node)&&node.expression.getText(ast)==='registerUpdateGuard')updateGuard=node.arguments[0];
 ts.forEachChild(node,visit);
}visit(ast);assert.ok(beforeUnload&&updateGuard);
const print=node=>printer.printNode(ts.EmitHint.Unspecified,node,ast),compile=code=>ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
function harness({retained=true,broken=false,mainStage=null,dropHidden=false}={}){
 let prevented=0,drains=0;const exports={},app={retainedMainApps:mainStage?{all:()=>[{stage:mainStage}]}:null,chatDrafts:{hasVolatile:()=>false},screenHasUnsavedChanges:()=>false,chatDraftSessions:new Set(),agentOrb:null,widgets:null,appStage:null,formEdits:{hasUnsavedChanges:()=>false},async flushScreen(){}};
 let before=print(beforeUnload);if(broken){const old=before;before=before.replace('hasRetainedNativeDrafts() || ','');assert.notEqual(before,old);}
 vm.runInNewContext(compile(`class ActualRuntimeHelpers{${print(stagesMethod)}};exports.stages=ActualRuntimeHelpers.prototype.appRuntimeStages;exports.bind=function(){return{before:${before},update:${print(updateGuard)}}};`),{exports,document:{querySelector:()=>null},hasRetainedNativeDrafts:()=>retained,flushRetainedNativeDrafts:async()=>{drains++;}});
 app.appRuntimeStages=dropHidden?()=>[]:exports.stages;
 const handlers=exports.bind.call(app),event={preventDefault(){prevented++;},returnValue:undefined};
 return{handlers,event,prevented:()=>prevented,drains:()=>drains};
}
function lostScreenOracle(broken=false){const f=harness({broken});f.handlers.before(f.event);assert.equal(f.prevented(),1,'private retained work must survive the loss of its old Screen');assert.equal(f.event.returnValue,'');}
test('the global close guard sees retained work even with a new clean account Screen',()=>lostScreenOracle());
test('negative control catches a close guard that checks only the active Screen',()=>assert.throws(()=>lostScreenOracle(true),error=>error.code==='ERR_ASSERTION'));
test('an update attempts only local drain and remains blocked while retained work is not durable',async()=>{const f=harness();assert.equal(await f.handlers.update(),'work');assert.equal(f.drains(),1);});
test('a clean app can close and update normally',async()=>{const f=harness({retained:false});f.handlers.before(f.event);assert.equal(f.prevented(),0);assert.equal(await f.handlers.update(),true);});

function hiddenRuntimeOracle(dropHidden=false){
 const stage={hasUnsavedChanges:()=>false,hasUnconfirmedWork:()=>true};
 const f=harness({retained:false,mainStage:stage,dropHidden});f.handlers.before(f.event);
 assert.equal(f.prevented(),1,'A hidden ordinary app must participate in the global unload guard');
 return f;
}
test('a hidden ordinary app blocks unload and update even while the active canvas is clean',async()=>{
 const f=hiddenRuntimeOracle();assert.equal(await f.handlers.update(),'app');
});
test('negative control: omitting hidden main owners fails the unchanged global work guard',()=>{
 assert.throws(()=>hiddenRuntimeOracle(true),/hidden ordinary app/);
});
test('hidden known dirty work also blocks update without a visible app stage',async()=>{
 const f=harness({retained:false,mainStage:{hasUnsavedChanges:()=>true,hasUnconfirmedWork:()=>false}});
 f.handlers.before(f.event);assert.equal(f.prevented(),1);assert.equal(await f.handlers.update(),'work');
});
