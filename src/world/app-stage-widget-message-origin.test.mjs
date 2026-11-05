import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {isAppExternalRequest} from './app-actions.mjs';

// Real production message listener + predicate, with synthetic Window/event ports.
// No browser, session, network or Source permission is simulated as live proof.
const source=readFileSync(new URL('./app-stage.ts',import.meta.url),'utf8');
const ast=ts.createSourceFile('app-stage.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS);
const listeners=[];
function visit(node){
  if(ts.isCallExpression(node)&&node.expression.getText(ast)==='view.addEventListener'
    &&node.arguments[0]?.text==='message'&&node.arguments[1]?.getText(ast).includes('isAppExternalRequest'))listeners.push(node.arguments[1]);
  ts.forEachChild(node,visit);
}
visit(ast);
assert.equal(listeners.length,1,'select the one actual app-action listener');
const listenerSource=listeners[0].getText(ast);
const stageMount=ast.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='mountAppStage');
const presented=stageMount.body.statements.filter(ts.isVariableStatement).flatMap(node=>[...node.declarationList.declarations]).find(node=>node.name.getText(ast)==='presented').initializer.getText(ast);
const canonical='https://reader.apps.example';
const runtimeOrigin='https://app-w11111111111141118111111111111111-'+ 'c'.repeat(24)+'.apps.example';

function fixture({code=listenerSource,peerOrigin=runtimeOrigin}={}){
  const state={peerOrigin,current:true,presented:true,activated:true,calls:0,frame:{contentWindow:{}}};
  const env={handler:null,isAppExternalRequest,
    runtime:{querySelector:selector=>selector==='iframe'?state.frame:null},
    selectedEntry:{origin:canonical},launcher:{runtimeOrigin:()=>state.peerOrigin},
    options:{isPresented:()=>state.presented},current:()=>state.current,view:{navigator:{userActivation:{get isActive(){return state.activated;}}}},
    external:{click(){state.calls++;}}};
  vm.createContext(env);
  new vm.Script(ts.transpileModule('const presented='+presented+';handler='+code,{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText).runInContext(env);
  return{state,send(change={}){env.handler({source:state.frame?.contentWindow,origin:state.peerOrigin,
    data:{schema:'soty.app-action.v1',action:'open-separately'},...change});}};
}
function requireOneOpen(f){f.send();assert.equal(f.state.calls,1,'activated current widget peer opens exactly once');}

test('actual app-stage consumer accepts the current widget runtime origin, preserving canonical metadata',()=>{
  const f=fixture();requireOneOpen(f);
  assert.notEqual(f.state.peerOrigin,canonical);
});

test('a canonical/main document in the same WindowProxy is not the widget runtime peer',()=>{
  const f=fixture();f.send({origin:canonical});assert.equal(f.state.calls,0);
});

test('main/legacy consumers keep the exact canonical-origin behavior',()=>{
  for(const peerOrigin of [canonical,null]){
    const f=fixture({peerOrigin});f.send({origin:canonical});assert.equal(f.state.calls,1);
    f.send({origin:'https://sibling.apps.example'});assert.equal(f.state.calls,1);
  }
});

test('activation, current account and actual selected Window stay mandatory',()=>{
  for(const change of [f=>{f.state.activated=false;},f=>{f.state.current=false;},f=>{f.state.frame=null;}]){
    const f=fixture();change(f);f.send();assert.equal(f.state.calls,0);
  }
  const f=fixture();f.send({source:{}});assert.equal(f.state.calls,0);
  f.send({origin:'https://sibling.apps.example'});assert.equal(f.state.calls,0);
});

test('a retained but nonpresented stage cannot open an external window until presented again',()=>{
  const f=fixture();f.state.presented=false;f.send();assert.equal(f.state.calls,0);
  f.state.presented=true;requireOneOpen(f);
});

test('old runtime origins and old iframe Windows cannot activate a replacement peer',()=>{
  const f=fixture(),oldOrigin=f.state.peerOrigin,oldWindow=f.state.frame.contentWindow;
  f.state.peerOrigin=runtimeOrigin.replace('c'.repeat(24),'e'.repeat(24));
  f.state.frame={contentWindow:{}};
  f.send({origin:oldOrigin});f.send({source:oldWindow});assert.equal(f.state.calls,0);
  requireOneOpen(f);
});

test('valid runtime identity does not admit URL payloads or unknown app actions',()=>{
  const f=fixture();
  for(const data of [null,[],{schema:'soty.app-action.v1',action:'navigate'},
    {schema:'soty.app-action.v1',action:'open-separately',url:canonical}])f.send({data});
  assert.equal(f.state.calls,0);requireOneOpen(f);
});

test('the same opening oracle detects the original canonical-only call-site defect',()=>{
  const pattern=/origin:\s*(?:launcher\.runtimeOrigin\(\)\s*\?\?\s*)?selectedEntry\?\.origin/u;
  assert.equal((listenerSource.match(new RegExp(pattern.source,'gu'))??[]).length,1,'mutate exactly the actual origin argument');
  const f=fixture({code:listenerSource.replace(pattern,'origin: selectedEntry?.origin')});
  assert.throws(()=>requireOneOpen(f),assert.AssertionError);
  f.send({origin:canonical});assert.equal(f.state.calls,1,'broken control wrongly trusts the canonical peer');
});
