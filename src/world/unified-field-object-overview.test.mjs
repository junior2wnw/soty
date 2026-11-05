import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {fieldLevelOfDetail} from './unified-field-camera.mjs';
import {fieldObjectNeedsFocus} from './unified-field-object-focus.mjs';

const source=readFileSync(new URL('./unified-field.ts',import.meta.url),'utf8');
function find(code,predicate){const ast=ts.createSourceFile('field.ts',code,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS);let found;const visit=node=>{if(predicate(node,ast))found=node;ts.forEachChild(node,visit);};visit(ast);assert.ok(found,'Actual production path must exist');return found.getText(ast);}
const compile=code=>ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText;
const expression=find(source,node=>ts.isVariableDeclaration(node)&&node.name.getText()==='detail'&&node.initializer?.getText().startsWith('fieldLevelOfDetail(Math.min(cam.scale'));
const metrics=find(source,node=>ts.isVariableDeclaration(node)&&node.name.getText()==='glance'&&node.initializer?.getText().startsWith('object.glanceSize'));
const detail=(scale,width,height,previous='detail',size,metricCode=metrics)=>{const context={object:size?{glanceSize:()=>size}:{},cam:{scale},screenWidth:width*scale,screenHeight:height*scale,target:{dataset:{objectLod:previous}},fieldLevelOfDetail,result:null};vm.createContext(context);vm.runInContext(compile(metricCode+';'+expression+';result=detail;'),context);return context.result;};

test('physical presentation retains existing hysteresis:20% overview,51% Source glance, small objects even at1:1',()=>{
 assert.equal(detail(.2,640,420),'overview');
 assert.equal(detail(.51,640,420,'overview'),'context','A normal320px Source must return to its real glance');
 assert.equal(detail(.43,640,420,'overview'),'overview');assert.equal(detail(.43,640,420,'context'),'context');
 assert.equal(detail(1,100,80),'overview','Camera detail cannot make a tiny physical window readable');
 assert.equal(detail(1,640,420),'detail');
});
test('a provider note reference admits the actual174x153 glance while the default preserves Counter128x84 overview',()=>{
 assert.equal(detail(.509375,340,300,'overview',{width:320,height:240}),'context');
 assert.equal(detail(.2,640,420,'overview'),'overview');
});
test('the old default-metrics control rejects the same small note readability criterion',()=>{
 assert.throws(()=>assert.equal(detail(.509375,340,300,'overview',{width:320,height:240},metrics.replace('object.glanceSize?.()','undefined')),'context'),/overview/);
});

function activate(code,detail=1){const handler=find(code,(node,ast)=>ts.isCallExpression(node)&&node.expression.getText(ast)==='expand.addEventListener'&&node.arguments[0]?.text==='click');
 const calls=[],environment={bind:null,performance:{now:()=>100},suppressClickUntil:0,pinch:null,gesture:null,node:{shortcutId:'source'},available:()=>({shortcutId:'source'}),
  nodeElements:new Map([['source',{dataset:{preview:'true',objectPresentation:'overview'}}]]),focusPreview:()=>calls.push('extra-camera-step'),enterObjectFocus:id=>calls.push(id)};
 const callback=handler.slice(handler.indexOf(',')+1,handler.lastIndexOf(',{signal}'));
 vm.createContext(environment);vm.runInContext(compile('bind='+callback+';'),environment);environment.bind({detail,stopPropagation(){}});return calls;
}
test('a tiny metadata object opens the readable SAME instance in one pointer or native keyboard activation',()=>{
 assert.deepEqual(activate(source,1),['source']);assert.deepEqual(activate(source,0),['source']);
});
test('old extra-camera-step control fails the unchanged one-step oracle',()=>{
 const anchor="target?.dataset.preview==='true'&&!target.dataset.objectPresentation";assert.equal(source.split(anchor).length,2);
 assert.throws(()=>assert.deepEqual(activate(source.replace(anchor,"target?.dataset.preview==='true'")),['source']),/extra-camera-step/);
});

function filtered(code,{retained=true,mounted=true,kind='object',provider=false}={}){
 const predicate=find(code,(node,ast)=>ts.isCallExpression(node)&&node.expression.getText(ast)==='current.nodes.filter'&&node.arguments[0]?.getText(ast).includes('overviewIds.has'));
 const environment={current:{nodes:[{shortcutId:'fourth',contextId:'personal',kind}]},mobileContext:'',protectedObjects:new Set(),retained:new Set(retained?['fourth']:[]),level:'overview',overviewIds:new Set(['first','second','third']),selectedId:'',options:{createObject(){},objectKey:()=>provider?'authorized-provider':null},objectViews:new Map(mounted?[['fourth',{}]]:[]),result:null};
 vm.createContext(environment);vm.runInContext('result='+predicate+';',environment);return environment.result.length;
}
test('overview preserves canonical metadata and mounted visible fourth views, without changing offscreen virtualization',()=>{
 assert.equal(filtered(source),1);assert.equal(filtered(source,{mounted:false}),1,'An initial far object must still have its identity');
 assert.equal(filtered(source,{kind:'app'}),1,'A mounted legacy provider keeps the same lifecycle');
 assert.equal(filtered(source,{retained:false}),0,'LOD must not pin a clean offscreen iframe');
});
test('old first-three-only control fails the unchanged fourth-view criterion',()=>{
 const anchor="||options.createObject&&(node.kind==='object'||objectViews.has(node.shortcutId)||options.objectKey?.(node)!=null)";assert.equal(source.split(anchor).length,2);
 assert.throws(()=>assert.equal(filtered(source.replace(anchor,'')),1),/0 !== 1/);
});
test('an admitted first-class provider keeps its fourth overview before its first mount; denied/offscreen providers stay excluded',()=>{
 assert.equal(filtered(source,{mounted:false,kind:'community',provider:true}),1);
 assert.equal(filtered(source,{mounted:false,kind:'community',provider:false}),0);
 assert.equal(filtered(source,{mounted:false,kind:'community',provider:true,retained:false}),0);
 const old=source.replace('||options.objectKey?.(node)!=null','');
 assert.throws(()=>assert.equal(filtered(old,{mounted:false,kind:'community',provider:true}),1),/0 !== 1/);
});

function tinyUnreadable(code=source){
 const declaration=find(code,node=>ts.isVariableDeclaration(node)&&node.name.getText()==='unreadable');
 const environment={focused:false,arranging:false,target:{dataset:{objectPresentation:'overview',needsFocus:'false'}},object:{},cam:{scale:1},fieldObjectNeedsFocus,result:null};
 vm.createContext(environment);vm.runInContext(compile(declaration+';result=unreadable;'),environment);return environment.result;
}
test('a physically tiny native overview at camera1 is inert and uses readable focus instead of a hidden editor',()=>{
 assert.equal(tinyUnreadable(),true);
});
test('old camera-only focus guard fails the unchanged tiny native visibility criterion',()=>{
 const anchor="target.dataset.objectPresentation==='overview'||";assert.equal(source.split(anchor).length,2);assert.throws(()=>assert.equal(tinyUnreadable(source.replace(anchor,'')),true),/false !== true/);
});
