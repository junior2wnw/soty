import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import ts from 'typescript';

const source=readFileSync(process.env.SOTY_TEST_CHAT_JUMP_SOURCE??new URL('./app.ts',import.meta.url),'utf8');
const ast=ts.createSourceFile('app.ts',source,ts.ScriptTarget.Latest,true);
let method;const visit=node=>{if(ts.isMethodDeclaration(node)&&node.name.getText(ast)==='mountChat')method=node;ts.forEachChild(node,visit);};visit(ast);assert(method);
const declarations=method.body.statements.filter(ts.isVariableStatement).flatMap(n=>[...n.declarationList.declarations]);
const initializer=name=>{const found=declarations.filter(n=>n.name.getText(ast)===name);assert.equal(found.length,1);return found[0].initializer.getText(ast);};
const observe=method.body.statements.filter(n=>ts.isExpressionStatement(n)&&ts.isCallExpression(n.expression)&&n.expression.expression.getText(ast)==='composerResize.observe').map(n=>n.getText(ast)).join('\n');
const cleanup=method.body.statements.find(n=>ts.isExpressionStatement(n)&&ts.isBinaryExpression(n.expression)&&n.expression.left.getText(ast)==='this.chatCleanup').expression.right.getText(ast);
const compile=code=>ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.None},reportDiagnostics:true});

function fixture({height=240,initial=false,knownCount=1,unseen=2,scrollHeight=2000,scrollTop=0}={}) {
 const trace=[],listeners=[],messages={clientHeight:height,scrollHeight,scrollTop,getClientRects(){return this.clientHeight>0?[{}]:[];}};
 const jump={attributes:new Map(),value:true,set hidden(value){trace.push(['jump',value]);this.value=value;},get hidden(){return this.value;},setAttribute(name,value){this.attributes.set(name,value);}},jumpCount={hidden:true,textContent:''};
 const composer={},dock={height:44,getBoundingClientRect(){trace.push(['dock-measure',this.height]);return{height:this.height};}},chat={style:{setProperty(name,value){trace.push(['dock-var',name,value]);}}};
 class ResizeObserver {
  constructor(callback){this.callback=callback;this.nodes=new Set();this.active=true;this.disconnects=0;}
  observe(node){this.nodes.add(node);}
  disconnect(){this.active=false;this.nodes.clear();this.disconnects++;}
  deliver(entries){const selected=entries.filter(entry=>this.nodes.has(entry.target));if(this.active&&selected.length)this.callback(selected);}
 }
 const calls={fit:0,media:0,outbox:0,previous:0,menu:0,draft:0};
 const ports={messages,jump,jumpCount,composer,dock,chat,ResizeObserver,fitComposer:()=>{calls.fit++;},
  known:new Map(Array.from({length:knownCount},(_,i)=>[i,{}])),initial,unseen,
  attachmentMedia:{close(){calls.media++;}},outbox:{close(){calls.outbox++;}},previousCleanup:()=>{calls.previous++;},closeMenu:()=>{calls.menu++;},unsubscribeDraft:()=>{calls.draft++;},
  document:{removeEventListener:(...args)=>listeners.push(args)},returned:()=>{},outsideMenu:()=>{},catchupTimer:undefined,highlightTimers:new Set()};
 const code=`const {messages,jump,jumpCount,composer,dock,chat,ResizeObserver,fitComposer,known,attachmentMedia,outbox,previousCleanup,closeMenu,unsubscribeDraft,document,returned,outsideMenu,catchupTimer,highlightTimers}=ports;
 let initial=ports.initial,unseen=ports.unseen,composerWidth=0;
 const nearEnd=${initializer('nearEnd')};const updateJump=${initializer('updateJump')};
 const composerResize=${initializer('composerResize')};${observe}
 const dispose=${cleanup};return {updateJump,composerResize,dispose,unseen:()=>unseen,setInitial:value=>{initial=value;}};`;
 const compiled=compile(code);assert.equal((compiled.diagnostics??[]).filter(d=>d.category===ts.DiagnosticCategory.Error).length,0);
 return {...new Function('ports',compiled.outputText)(ports),messages,jump,jumpCount,composer,dock,chat,trace,calls,listeners};
}

for(const height of [0,43,59]) test(`jump hides when message pane has only ${height}px, protecting header/back targets`,()=>{
 const f=fixture({height});f.updateJump();assert.equal(f.jump.hidden,true);assert.equal(f.unseen(),2,'geometry is not read acknowledgement');
});

test('60px and larger retain the full-sized jump when loaded, nonempty and away from the end',()=>{
 for(const height of [60,61,240,540,900]){const f=fixture({height});f.updateJump();assert.equal(f.jump.hidden,false);assert.equal(f.jumpCount.textContent,'2');}
});

test('existing initial/nearEnd/empty guards and unread/accessible count semantics remain',()=>{
 const initial=fixture({height:240,initial:true});initial.updateJump();assert.equal(initial.jump.hidden,true);assert.equal(initial.unseen(),2);
 const empty=fixture({height:240,knownCount:0});empty.updateJump();assert.equal(empty.jump.hidden,true);
 const end=fixture({height:240,scrollHeight:260});end.updateJump();assert.equal(end.jump.hidden,true);assert.equal(end.unseen(),0);assert.equal(end.jumpCount.hidden,true);assert.equal(end.jump.attributes.get('aria-label'),'К последним сообщениям');
 const many=fixture({height:60,unseen:150});many.updateJump();assert.equal(many.jumpCount.textContent,'99+');assert.equal(many.jump.attributes.get('aria-label'),'К последним сообщениям, 150 новых');
});

test('the same existing observer watches messages as well as composer/dock',()=>{
 const f=fixture();assert.deepEqual([...f.composerResize.nodes],[f.composer,f.dock,f.messages]);
});

test('message-only resize hides and restores jump without requiring a scroll event',()=>{
 const f=fixture({height:240});f.updateJump();assert.equal(f.jump.hidden,false);
 for(const height of [0,43,59,60,240]){f.messages.clientHeight=height;f.composerResize.deliver([{target:f.messages,contentRect:{width:390}}]);assert.equal(f.jump.hidden,height<60);}
 assert.equal(f.calls.fit,0);assert.equal(f.unseen(),2);
});

test('dock measurement updates its CSS variable before jump geometry is rechecked',()=>{
 const f=fixture({height:240});f.updateJump();f.trace.length=0;f.messages.clientHeight=0;f.dock.height=180;
 f.composerResize.deliver([{target:f.dock,contentRect:{width:390}}]);
 assert.equal(f.jump.hidden,true);assert.deepEqual(f.trace.slice(0,3),[['dock-measure',180],['dock-var','--sw-chat-dock-height','180px'],['jump',true]]);
});

test('composer width fitting remains change-only; real cleanup disconnects the one observer and all old subscriptions',()=>{
 const f=fixture({height:240});f.composerResize.deliver([{target:f.composer,contentRect:{width:320}}]);assert.equal(f.calls.fit,1);
 f.composerResize.deliver([{target:f.composer,contentRect:{width:320}}]);assert.equal(f.calls.fit,1);
 f.dispose();assert.equal(f.composerResize.disconnects,1);assert.equal(f.composerResize.nodes.size,0);
 assert.deepEqual(f.calls,{fit:1,media:1,outbox:1,previous:1,menu:1,draft:1});assert.deepEqual(f.listeners.map(row=>row[0]),['visibilitychange','pointerdown']);
 f.trace.length=0;f.messages.clientHeight=0;f.composerResize.deliver([{target:f.messages,contentRect:{width:320}}]);assert.equal(f.trace.length,0);
});

test('complete app source transpiles without syntax errors and observer teardown stays single',()=>{
 const result=compile(source);assert.equal((result.diagnostics??[]).filter(d=>d.category===ts.DiagnosticCategory.Error).length,0);
 assert.equal((cleanup.match(/composerResize\.disconnect\(\)/g)??[]).length,1);
});
