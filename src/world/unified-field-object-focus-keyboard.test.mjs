import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import ts from 'typescript';
import {zoomFieldCamera} from './unified-field-camera.mjs';

const source=readFileSync(new URL('./unified-field.ts',import.meta.url),'utf8');
const ast=ts.createSourceFile('field.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS);
const declaration=ast.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='createUnifiedField');
const handler=declaration.body.statements.find(n=>ts.isExpressionStatement(n)&&ts.isCallExpression(n.expression)
  &&n.expression.expression.getText(ast)==='plane.addEventListener'&&n.expression.arguments[0]?.text==='keydown').expression.arguments[1];
const compiled=ts.transpileModule('handler='+handler.getText(ast),{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText;

function fixture(focused){
  const calls=[],environment={handler:null,options:{readableObjectFocus:true,createObject:()=>null,commit:()=>{}},objectFocus:focused?{shortcutId:'window'}:null,
    objectViews:new Map(),selectedId:'window',moving:null,contextPreview:null,mode:'mine',arranging:false,spacePanning:false,
    camera:()=>({x:12,y:48,scale:.5}),viewportSize:()=>({width:1280,height:800}),zoomFieldCamera,
    fitOverview:()=>calls.push('overview'),setCamera:()=>calls.push('camera'),beginMove:()=>calls.push('move'),schedule:()=>calls.push('schedule'),
    cancel:()=>calls.push('cancel'),announce:()=>{},setArrange:()=>calls.push('arrange')};
  vm.createContext(environment);new vm.Script(compiled).runInContext(environment);
  return {calls,environment,press(key){const event={key,defaultPrevented:false,isComposing:false,altKey:false,ctrlKey:false,metaKey:false,shiftKey:false,target:{closest:()=>null},preventDefault(){this.defaultPrevented=true;}};environment.handler(event);return event;}};
}

test('reading an object never applies world camera, arrange or pan shortcuts to its article',()=>{
  for(const key of ['0','+','-','m',' ']){
    const f=fixture(true);f.press(key);
    assert.deepEqual(f.calls,[],`Focused article must not execute world shortcut ${JSON.stringify(key)}`);
    assert.equal(f.environment.spacePanning,false);
  }
});

test('the same shortcuts still work on the canvas outside readable focus',()=>{
  for(const [key,expected]of [['0','overview'],['+','camera'],['m','move'],[' ','schedule']]){
    const f=fixture(false);assert.equal(f.press(key).defaultPrevented,true);assert.deepEqual(f.calls,[expected]);
  }
});

test('Escape during text composition belongs to the input method; an ordinary host Escape still returns',()=>{
  let capture;
  const visit=node=>{if(ts.isCallExpression(node)&&node.expression.getText(ast)==='viewport.addEventListener'&&node.arguments[0]?.text==='keydown')capture=node.arguments[1];ts.forEachChild(node,visit);};visit(declaration);
  assert.ok(capture,'Actual capture handler must exist');
  const environment={handler:null,objectFocus:{shortcutId:'window'},leaves:0,leaveObjectFocus(){environment.leaves++;}};
  vm.createContext(environment);new vm.Script(ts.transpileModule('handler='+capture.getText(ast),{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText).runInContext(environment);
  const composing={key:'Escape',isComposing:true,defaultPrevented:false,preventDefault(){this.defaultPrevented=true;},stopPropagation(){this.stopped=true;}};
  environment.handler(composing);assert.equal(environment.leaves,0);assert.equal(composing.defaultPrevented,false);
  environment.handler({...composing,isComposing:false});assert.equal(environment.leaves,1);
});

// Execute the real expand-button listener: a drag's trailing pointer click is
// different from a deliberate native keyboard/assistive click (detail=0).
function expandClick({detail,suppressed=true,pinch=null}={}){
 let listener;
 const visit=node=>{if(ts.isCallExpression(node)&&node.expression.getText(ast)==='expand.addEventListener'&&node.arguments[0]?.text==='click')listener=node.arguments[1];ts.forEachChild(node,visit);};visit(declaration);
 assert.ok(listener,'Actual expand listener exists');
 const calls=[],environment={handler:null,performance:{now:()=>100},suppressClickUntil:suppressed?200:0,pinch,gesture:null,
  node:{shortcutId:'source'},available:()=>({shortcutId:'source'}),nodeElements:new Map([['source',{dataset:{preview:'false'}}]]),enterObjectFocus:id=>calls.push(id),focusPreview:()=>assert.fail('This is a real placed Source')};
 vm.createContext(environment);new vm.Script(ts.transpileModule('handler='+listener.getText(ast),{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText).runInContext(environment);
 environment.handler({detail,stopPropagation(){}});return calls;
}
test('a deliberate keyboard activation immediately after pan/pinch opens the real window',()=>{
 assert.deepEqual(expandClick({detail:0}),['source']);
});
test('the same listener still suppresses trailing pointer clicks and active pinch, then accepts a later pointer click',()=>{
 assert.deepEqual(expandClick({detail:1}),[]);assert.deepEqual(expandClick({detail:0,pinch:{}}),[]);assert.deepEqual(expandClick({detail:1,suppressed:false}),['source']);
});

test('a fresh pointer action in readable focus is accepted while a gesture trailing click remains suppressed',()=>{
 const find=(eventName)=>{let found;const visit=node=>{if(ts.isCallExpression(node)&&node.expression.getText(ast)==='viewport.addEventListener'&&node.arguments[0]?.text===eventName)found=node.arguments[1];ts.forEachChild(node,visit);};visit(declaration);assert.ok(found);return found.getText(ast);};
 const environment={down:null,click:null,options:{readableObjectFocus:true},objectFocus:{shortcutId:'source'},suppressClickUntil:200,performance:{now:()=>100},moving:null};
 vm.createContext(environment);new vm.Script(ts.transpileModule('down='+find('pointerdown')+';click='+find('click'),{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText).runInContext(environment);
 const attempt=()=>{const event={detail:1,defaultPrevented:false,preventDefault(){this.defaultPrevented=true;},stopPropagation(){}};environment.click(event);return!event.defaultPrevented;};
 assert.equal(attempt(),false,'The trailing click without a new press is still rejected');
 environment.down({button:0});assert.equal(attempt(),true,'A deliberate Back/Settings pointer press must be usable immediately after keyboard focus');
});
