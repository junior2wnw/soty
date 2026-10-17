import test from 'node:test';
import assert from 'node:assert/strict';
import{readFileSync}from'node:fs';
import vm from'node:vm';
import ts from'typescript';
import{createAppRuntimePhaseTrace}from'./app-runtime-phase-trace.mjs';
import{isScopedRuntimeProfile}from'./app-launch.mjs';

// Execute the production manual buttons, launch/watch/reset/route functions and
// diagnostic dialog, with only DOM, network and unrelated stage sinks stubbed.
const source=readFileSync(new URL('./app-stage.ts',import.meta.url),'utf8');
const parsed=ts.createSourceFile('app-stage.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS),printer=ts.createPrinter();
const mount=parsed.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='mountAppStage');assert.ok(mount);
const statements=mount.body.statements,print=n=>printer.printNode(ts.EmitHint.Unspecified,n,parsed);
const fn=name=>{const value=statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text===name);assert.ok(value,name);return print(value);};
const declaration=name=>{for(const n of statements)if(ts.isVariableStatement(n))for(const d of n.declarationList.declarations)if(d.name.getText(parsed)===name)return`const ${print(d)};`;assert.fail(name);};
const compiled=ts.transpileModule(`
exports.harness=(host,options,state)=>{
 const view=host.view,runtime=host.runtime,controller=new AbortController(),message=host.message;
 let disposed=false,runtimeStarted=false,runtimePending=false,displayEpoch=0,releaseRuntimeDisplay=null,entryRecoveryEpoch=0,entryRecoveryPending=false,lastRuntimeLaunchAt=0,frameGeneration=0;
 let diagnosticOpen=false,closeDiagnostic=null,renderDiagnostic=null,routeGeneration=0,intent={},presentationVersion=0;
 const current=()=>state.current,matches=()=>true,app={appId:'app-synthetic',name:'Synthetic'},panel=host.panel;
 const currentEntryRetry={hidden:true,disabled:false},externalRecovery={hidden:true},more={},recovery={bind(){state.calls.bind++;}};
 const stopAssist=()=>{state.calls.stopAssist++;},captureEntry=()=>{state.calls.captureEntry++;},ensureDiscussion=()=>{},showPanel=()=>{},applyPresentation=async()=>{},discussion=null;
 const closeMore=()=>{},showMessage=text=>{message.textContent=text;state.messages.push(text);};
 const launcher={launch:async()=>{state.calls.launch++;if(state.failure)throw state.failure;return state.held?await state.held:'https://frame.example/_soty/boot#PRIVATE_TICKET';},runtimeProfile:()=> 'soty.selected-human-embed.v2',entry:()=>({origin:'https://frame.example'}),binding:()=>null};
 const button=host.button,el=host.el,emptyState=()=>host.el('div'),createDialog=host.createDialog;
 const appLaunchFailure=()=>({title:'Existing error',detail:'Existing detail'});
 ${declaration('runtimeTrace')}
 ${declaration('refresh')}
 ${declaration('recoveryRetry')}
 ${fn('resetRuntimeDisplay')}
 ${fn('watchRuntimeDisplay')}
 ${fn('launchRuntime')}
 ${fn('openRuntimeDiagnostics')}
 ${fn('updateRoute')}
 return{api:runtimeTrace,refresh:()=>refresh.action(),retry:()=>recoveryRetry.action(),initial:()=>launchRuntime(),automatic:()=>launchRuntime(true,()=>state.recoveryCurrent),route:()=>updateRoute({}),dialog:openRuntimeDiagnostics,
 frame:()=>runtime.querySelector('iframe'),pending:value=>runtimePending=value};
};`,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;

class Node {
 constructor(tag,state){this.tag=tag;this.state=state;this.children=[];this.listeners=new Map();this.classList={add(){}};this.hidden=false;this.disabled=false;this.textContent='';this.style={display:'block',visibility:'visible',opacity:'1'};this.isConnected=true;this.parentElement=null;this.contentWindow={postMessage:(data,origin)=>state.probes.push({data,origin})};}
 addEventListener(type,fn){if(!this.listeners.has(type))this.listeners.set(type,new Set());this.listeners.get(type).add(fn);this.state.order.push('listen:'+this.tag+':'+type);}
 removeEventListener(type,fn){this.listeners.get(type)?.delete(fn);}
 emit(type,event={}){for(const fn of [...(this.listeners.get(type)??[])])fn(event);}
 append(...nodes){for(const n of nodes){n.parentElement=this;this.children.push(n);}}
 replaceChildren(...nodes){for(const n of this.children)n.isConnected=false;this.children=[];this.append(...nodes);this.state.order.push('insert:'+this.tag);}
 querySelector(){return this.children.find(x=>x.tag==='iframe')??null;}
 setAttribute(){}
 contains(){return false;}
 getBoundingClientRect(){return{left:10,top:20,width:400,height:300};}
 set src(value){this.state.order.push('src');this.privateSrc=value;}
}
const flush=()=>new Promise(resolve=>setImmediate(resolve));
function fixture(presentation){
 const state={current:true,recoveryCurrent:true,calls:{launch:0,bind:0,stopAssist:0,captureEntry:0},messages:[],order:[],probes:[],copies:[],dialogs:[],draft:'retained discussion draft'};
 let now=0,next=0;const tasks=new Map(),doc=new Node('document',state),view=new Node('window',state),runtime=new Node('runtime',state);
 Object.assign(view,{document:doc,performance:{now:()=>now},crypto:{randomUUID:()=> 'a'.repeat(32)},getComputedStyle:n=>n.style,navigator:{clipboard:{writeText:async text=>state.copies.push(text)}},
   setTimeout:(fn,ms)=>{const id=++next;tasks.set(id,{fn,at:now+ms});return id;},clearTimeout:id=>tasks.delete(id)});
 doc.elementsFromPoint=()=>[runtime.querySelector('iframe')];doc.activeElement=null;
 const button=(text,_icon,_cls,action)=>Object.assign(new Node('button',state),{textContent:text,action});
 const el=(tag,_class,text='')=>Object.assign(new Node(tag,state),{textContent:text});
 const createDialog=(_title,onClose)=>{const dialog={element:new Node('dialog',state),body:new Node('body',state),close(){onClose();}};state.dialogs.push(dialog);return dialog;};
 const host={view,runtime,ownerDocument:doc,message:new Node('message',state),panel:Object.assign(new Node('panel',state),{hidden:true}),button,el,createDialog};
 const context=vm.createContext({exports:{},AbortController,URL,createAppRuntimePhaseTrace,isScopedRuntimeProfile});new vm.Script(compiled).runInContext(context);
 const h=context.exports.harness(host,{...(presentation?{presentation}:{}),onModalChange(){}},state);
 const tick=ms=>{const target=now+ms;while(true){const t=[...tasks].sort((a,b)=>a[1].at-b[1].at)[0];if(!t||t[1].at>target)break;tasks.delete(t[0]);now=t[1].at;t[1].fn();}now=target;};
 return{h,state,tasks,tick,view,host};
}

test('actual manual buttons record one ordinary launch; initial/automatic/pending/widget paths cannot consume arm',async()=>{
 for(const action of ['refresh','retry']){const f=fixture();f.h.api.arm();f.h[action]();await flush();assert.equal(f.state.calls.launch,1);assert.equal(f.h.api.snapshot().events[0].phase,'root_launch');}
 for(const action of ['initial','automatic']){const f=fixture();f.h.api.arm();await f.h[action]();assert.equal(f.state.calls.launch,1);assert.equal(f.h.api.snapshot(),null);assert.equal(f.h.api.state(),'armed');}
 const pending=fixture();pending.h.api.arm();pending.h.pending(true);pending.h.refresh();await flush();assert.equal(pending.state.calls.launch,0);assert.equal(pending.h.api.state(),'armed');pending.h.pending(false);pending.h.refresh();await flush();assert.equal(pending.state.calls.launch,1);
 const widget=fixture('widget');widget.h.api.arm();widget.h.refresh();await flush();assert.equal(widget.state.calls.launch,1);assert.equal(widget.h.api.snapshot(),null);assert.equal(widget.state.probes.length,0);
});

test('actual launch wiring observes before src/insert and preserves existing load/error/timeout behavior and drafts',async()=>{
 const run=async diagnostic=>{const f=fixture();if(diagnostic)f.h.api.arm();f.h.refresh();await flush();const frame=f.h.frame();assert.ok(frame);if(diagnostic){assert.ok(f.state.order.indexOf('listen:iframe:load')<f.state.order.indexOf('src'));assert.ok(f.state.order.indexOf('src')<f.state.order.lastIndexOf('insert:runtime'));}
   frame.emit('load');f.tick(12_001);assert.equal(f.state.draft,'retained discussion draft');return f;};
 const off=await run(false),on=await run(true);assert.deepEqual(on.state.calls,off.state.calls);assert.deepEqual(on.state.messages,off.state.messages);assert.equal(on.state.calls.launch,1);assert.equal(on.h.api.snapshot().events.some(x=>x.phase==='root_timeout'),false);assert.equal(on.h.api.snapshot().observation,'partial');
 const late=fixture();late.h.api.arm();late.h.refresh();await flush();late.tick(12_000);assert.match(late.state.messages.at(-1),/Загрузка окна не подтверждена/u);late.h.frame().emit('load');assert.equal(late.state.messages.at(-1),'');assert.equal(late.h.api.snapshot().observation,'partial');
});

test('actual route reset rejects old peers and erases diagnostics without remounting iframe or touching drafts',async()=>{
 const f=fixture();f.h.api.arm();f.h.refresh();await flush();const frame=f.h.frame();f.h.route();assert.equal(f.h.api.snapshot(),null);assert.equal(f.h.frame(),frame);assert.equal(f.state.calls.launch,1);assert.equal(f.state.draft,'retained discussion draft');
 f.view.emit('message',{source:frame.contentWindow,origin:'https://frame.example',ports:[],data:{schema:'soty.runtime-phase.v1',phase:'boot_cookie_pending'}});assert.equal(f.h.api.snapshot(),null);
});

test('actual stage distinguishes load from a correlated Boot reply without changing transport UI or the retained draft',async()=>{
 const f=fixture();f.h.api.arm();f.h.refresh();await flush();f.tick(0);const frame=f.h.frame();
 assert.equal(f.state.probes[0].data.schema,'soty.runtime-phase-watch.v2');
 const watchId=f.state.probes[0].data.watchId;
 frame.emit('load');f.tick(12_001);
 assert.equal(f.h.api.snapshot().events.at(-1).phase,'root_boot_unobserved');
 assert.equal(f.state.messages.at(-1),'');
 f.view.emit('message',{source:frame.contentWindow,origin:'https://frame.example',ports:[],data:{schema:'soty.runtime-phase.v2',watchId,phase:'boot_failed'}});
 assert.equal(f.h.api.snapshot().events.at(-1).phase,'boot_failed');
 assert.equal(f.h.api.snapshot().events.filter(x=>x.phase==='boot_document_ack').length,1);
 assert.equal(f.state.calls.launch,1);assert.equal(f.state.messages.at(-1),'');assert.equal(f.state.draft,'retained discussion draft');
});

test('actual diagnostic dialog arms/copies/clears only on explicit controls and never starts a launch',async()=>{
 const f=fixture();f.h.dialog();let dialog=f.state.dialogs.at(-1),actions=dialog.body.children.at(-1).children;
 assert.equal(f.state.calls.launch,0);assert.equal(f.state.copies.length,0);actions[0].action();assert.equal(f.h.api.state(),'armed');assert.equal(f.state.calls.launch,0);
 f.h.refresh();await flush();f.h.dialog();dialog=f.state.dialogs.at(-1);actions=dialog.body.children.at(-1).children;
 const technical=dialog.body.children.find(node=>node.tag==='details');assert.ok(technical);assert.notEqual(technical.open,true);assert.equal(technical.children[0].tag,'summary');assert.equal(technical.children[1].tag,'textarea');
 const visible=dialog.body.children.filter(node=>node!==technical).flatMap(node=>[node.textContent,...node.children.map(child=>child.textContent)]).join(' ');assert.doesNotMatch(visible,/root_|boot_|page_csp_|CSP|load/u);
 assert.equal(f.state.calls.launch,1);actions[1].action();await flush();assert.equal(f.state.copies.length,1);assert.doesNotMatch(f.state.copies[0],/PRIVATE|https:|appId|requestId/u);
 actions[2].action();assert.equal(f.h.api.snapshot(),null);assert.equal(f.state.calls.launch,1);assert.equal(f.state.draft,'retained discussion draft');
 const widget=fixture('widget');widget.h.dialog();assert.equal(widget.state.dialogs.length,0);
});
