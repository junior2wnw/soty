import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
const source=readFileSync(new URL('./unified-field-screen.ts',import.meta.url),'utf8'),printer=ts.createPrinter();
function harness(text=source){
 const ast=ts.createSourceFile('screen.ts',text,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS),screen=ast.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='createUnifiedFieldScreen');
 const update=screen.body.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='updateIdentity'),method=screen.body.statements.find(ts.isReturnStatement).expression.properties.find(n=>n.name?.getText(ast)==='setInboxSummary');
 const print=node=>printer.printNode(ts.EmitHint.Unspecified,node,ast);
 const compile=code=>ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
 class Node{constructor(tag,cls='',text=''){this.tag=tag;this.className=cls;this.textContent=text;this.children=[];this.attrs={};this.style={};}append(...nodes){this.children.push(...nodes);}replaceChildren(...nodes){this.children=nodes;}setAttribute(k,v){this.attrs[k]=v;}}
 let current=true;const profile={displayName:'Synthetic owner',avatarRevision:1},control=new Node('button');
 const env=vm.createContext({exports:{},options:{accountId:'owner',readIdentity:()=>profile},current:()=>current,canvasIdentityKey:'',inboxSummary:{messages:0,requests:0,incomplete:false},profileControl:control,avatar:()=>new Node('span','avatar'),el:(...args)=>new Node(...args)});
 vm.runInContext(compile(`${print(update)}\nconst api={${print(method)}};exports.set=api.setInboxSummary;exports.paint=updateIdentity;`),env);
 env.exports.paint();return{set:env.exports.set,control,profile,retire(){current=false;}};
}
function changingCountOracle(text=source){
 const f=harness(text);f.set({messages:2,requests:1,incomplete:false});assert.equal(f.control.children.at(-1).textContent,'3');
 assert.match(f.control.attrs['aria-label'],/Новых сообщений: 2; запросов в контакты: 1/u);
 assert.equal(f.control.children[0].className,'avatar','notification does not replace the person');
 f.set({messages:0,requests:0,incomplete:false});assert.equal(f.control.children.length,1);assert.equal(f.control.attrs['aria-label'],'Меню, Synthetic owner');
}
test('a new message or request changes the existing canvas avatar without a profile revision or screen rebuild',()=>changingCountOracle());
test('negative control catches identity-only memoization suppressing the incoming indicator',()=>{
 const mutant=source.replace('identity.avatarRevision,inboxSummary]','identity.avatarRevision]');assert.notEqual(mutant,source);assert.throws(()=>changingCountOracle(mutant),error=>error.code==='ERR_ASSERTION');
});
test('partial counts stay visibly partial and a retired account cannot change the indicator',()=>{
 const f=harness();f.set({messages:2,requests:0,incomplete:true});assert.equal(f.control.children.at(-1).textContent,'2+');f.retire();f.set({messages:90,requests:4,incomplete:false});assert.equal(f.control.children.at(-1).textContent,'2+');
});

test('pending room invitations update the same avatar while retaining distinct accessible meaning',()=>{
 const f=harness();f.set({messages:0,requests:0,invitations:2,incomplete:false});
 assert.equal(f.control.children.at(-1).textContent,'2');assert.match(f.control.attrs['aria-label'],/приглашений в комнаты: 2/u);
 assert.match(f.control.attrs['aria-label'],/Новых сообщений: 0; запросов в контакты: 0/u);
 f.set({messages:0,requests:0,incomplete:false});assert.equal(f.control.children.length,1);assert.equal(f.control.attrs['aria-label'],'Меню, Synthetic owner');
 f.set({messages:0,requests:0,invitations:1,incomplete:false});f.retire();f.set({messages:0,requests:0,invitations:90,incomplete:false});assert.equal(f.control.children.at(-1).textContent,'1');
});
