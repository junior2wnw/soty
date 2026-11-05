import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
const source=readFileSync(new URL('./field-object-overview.ts',import.meta.url),'utf8'),ports=readFileSync(new URL('./canvas-field-content.test.mjs',import.meta.url),'utf8');
const ast=ts.createSourceFile('port.mjs',ports,ts.ScriptTarget.Latest,true,ts.ScriptKind.JS),node=ast.statements.find(node=>ts.isClassDeclaration(node)&&node.name?.text==='NodePort');
const flatten=node=>[node,...node.children.flatMap(flatten)],Node=vm.runInNewContext(`(${ts.createPrinter().printNode(ts.EmitHint.Unspecified,node,ast)})`,{flatten});
function fixture(code=source){
 const document={createElement:tag=>new Node(tag)},content=new Node('article'),host=new Node('div');host.append(content);
 let current=true,identity={title:'Allowed title',symbol:'note',privateBody:'must never be mirrored'};
 const env={document,icon:()=>new Node('svg'),exports:{}};vm.createContext(env);vm.runInContext(ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'')+'\nexports.create=createFieldObjectOverview;',env);
 const view=env.exports.create(content,{isCurrent:()=>current,readIdentity:()=>identity});host.append(view.element);
 return{view,host,content,set identity(value){identity=value;},deny(){current=false;view.update();}};
}
test('one identity composition uses only permitted title/mark and current permission, without touching provider DOM',()=>{
 const h=fixture(),editor=new Node('textarea');editor.value='Unsubmitted editor body';h.content.append(editor);h.view.setPresentation('overview');
 assert.equal(h.view.element.textContent,'Allowed title');assert.equal(h.view.element.textContent.includes('must never'),false);assert.equal(h.content.children[0],editor);assert.equal(editor.value,'Unsubmitted editor body');
 h.identity={title:'Fresh title',mark:'FT',visual:true};h.view.update();assert.equal(h.view.element.textContent,'FTFresh title');assert.equal(h.content.dataset.overviewVisual,'true');assert.equal(h.content.getAttribute('aria-hidden'),'false','Existing pixels stay visible');
 h.deny();assert.equal(h.view.element.textContent,'');assert.equal(h.content.dataset.overviewVisual,'false');
});
function disposeScenario(code=source){const h=fixture(code),heldTitle=h.view.element.children[1];h.view.dispose();assert.equal(h.view.element.parentElement,null);assert.equal(heldTitle.textContent,'','Disposal must scrub detached identity as well as remove it');h.view.update();assert.equal(h.view.element.textContent,'');}
test('disposing a provider removes/scrubs its identity and ignores late updates',()=>disposeScenario());
test('broken detached-but-uncleared cleanup fails the unchanged privacy criterion',()=>{
 const anchor='clear();dead=true;element.remove();';assert.equal(source.split(anchor).length,2);assert.throws(()=>disposeScenario(source.replace(anchor,'dead=true;element.remove();')),/Disposal must scrub detached identity/);
});
