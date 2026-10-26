import test from 'node:test';
import assert from 'node:assert/strict';
import{readFileSync}from'node:fs';
import vm from'node:vm';
import ts from'typescript';
import{createAppRuntimePhaseTrace}from'./app-runtime-phase-trace.mjs';
import{isScopedRuntimeProfile}from'./app-launch.mjs';
import{createAppRuntimeDisplay}from'./app-runtime-display.mjs';
import{captureSourcePin,matchesScopedCaptureContext}from'./app-project-capture.mjs';
import{createBootDisplayClient}from'../../modules/apps/server/runtime-display-client.mjs';

// Execute the production manual buttons, launch/watch/reset/route functions and
// diagnostic dialog, with only DOM, network and unrelated stage sinks stubbed.
const source=readFileSync(new URL('./app-stage.ts',import.meta.url),'utf8');
const parsed=ts.createSourceFile('app-stage.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS),printer=ts.createPrinter();
const mount=parsed.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='mountAppStage');assert.ok(mount);
const statements=mount.body.statements,print=n=>printer.printNode(ts.EmitHint.Unspecified,n,parsed);
// Keep the real optional voice gate in the extracted launch closure. These
// phase-only cases have no descriptor resolver; the production default decides that path.
const declarationIndex=name=>statements.findIndex(n=>ts.isVariableStatement(n)&&n.declarationList.declarations.some(d=>ts.isIdentifier(d.name)&&d.name.text===name));
const voiceStart=declarationIndex('nativeVoice'),voiceEnd=declarationIndex('withVoiceAdmission');
assert.ok(voiceStart>=0&&voiceEnd>=voiceStart,'actual voice closure anchors');
const voiceCode=statements.slice(voiceStart,voiceEnd+1).map(print).join('\n');
const fn=name=>{const value=statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text===name);assert.ok(value,name);return print(value);};
const declaration=name=>{for(const n of statements)if(ts.isVariableStatement(n))for(const d of n.declarationList.declarations)if(d.name.getText(parsed)===name)return`const ${print(d)};`;assert.fail(name);};
const compiled=ts.transpileModule(`
exports.harness=(host,options,state)=>{
 const view=host.view,runtime=host.runtime,controller=new AbortController(),message=host.message;
 let disposed=false,runtimeStarted=false,runtimePending=false,displayEpoch=0,releaseRuntimeDisplay=null,runtimeDisplay=null,entryRecoveryEpoch=0,entryRecoveryPending=false,lastRuntimeLaunchAt=0,frameGeneration=0,externalAvailability='unknown';
 let diagnosticOpen=false,closeDiagnostic=null,renderDiagnostic=null,routeGeneration=0,intent={},presentationVersion=0;
 const current=()=>state.current,matches=()=>true,app={appId:'app-synthetic',name:'Synthetic'},panel=host.panel;
 const dismissRuntimeNotice={hidden:true},currentEntryRetry={hidden:true,disabled:false},externalRecovery={hidden:true},external={hidden:true},more={},recovery={bind(){state.calls.bind++;}};
 const stopAssist=()=>{state.calls.stopAssist++;},captureEntry=()=>{state.calls.captureEntry++;},ensureDiscussion=()=>{},showPanel=()=>{},applyPresentation=async()=>{},discussion=null;
 const closeMore=()=>{};
 const accountId='account-synthetic',saved={setEntryVerified:value=>state.verified.push(value)};
 const mountRootVoiceCarrier=()=>{throw Error('unexpected voice carrier in phase-only fixture');};
 ${voiceCode}
 ${declaration('messageOwner').replace(/^const /u,'let ')}
 ${fn('showMessage')}
 const launcher={launch:async()=>{state.calls.launch++;if(state.failure)throw state.failure;return state.held?await state.held:'https://frame.example/_soty/boot#PRIVATE_TICKET';},canOpenExternal:async()=>true,runtimeProfile:()=> 'soty.selected-human-embed.v2',entry:()=>({origin:'https://frame.example'}),binding:()=>null,scopedCapture:()=>state.binding??null};
 const button=host.button,el=host.el,emptyState=()=>host.el('div'),createDialog=host.createDialog;
 const appLaunchFailure=()=>({title:'Existing error',detail:'Existing detail'});
 ${declaration('runtimeTrace')}
 ${declaration('refresh')}
 ${declaration('recoveryRetry')}
 ${fn('resetRuntimeDisplay')}
 ${fn('syncExternalControls')}
 ${fn('checkExternalAvailability')}
 ${fn('watchRuntimeDisplay')}
 ${fn('launchRuntime')}
 ${fn('openRuntimeDiagnostics')}
 ${fn('updateRoute')}
 return{api:runtimeTrace,refresh:()=>refresh.action(),retry:()=>recoveryRetry.action(),initial:()=>launchRuntime(),automatic:()=>launchRuntime(true,()=>state.recoveryCurrent),route:()=>updateRoute({}),dialog:openRuntimeDiagnostics,
 frame:()=>runtime.querySelector('iframe'),pending:value=>runtimePending=value,controls:()=>({recoveryRetry,externalRecovery,dismissRuntimeNotice})};
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
 const state={current:true,recoveryCurrent:true,verified:[],calls:{launch:0,bind:0,stopAssist:0,captureEntry:0},messages:[],order:[],probes:[],copies:[],dialogs:[],reads:[],draft:'retained discussion draft'};
 let now=0,next=0;const tasks=new Map(),doc=new Node('document',state),view=new Node('window',state),runtime=new Node('runtime',state);
 Object.assign(view,{document:doc,performance:{now:()=>now},crypto:{randomUUID:()=> 'a'.repeat(32)},getComputedStyle:n=>n.style,navigator:{clipboard:{writeText:async text=>state.copies.push(text)}},
   setTimeout:(fn,ms)=>{const id=++next;tasks.set(id,{fn,at:now+ms});return id;},clearTimeout:id=>tasks.delete(id)});
 doc.elementsFromPoint=()=>[runtime.querySelector('iframe')];doc.activeElement=null;
 const button=(text,_icon,_cls,action)=>Object.assign(new Node('button',state),{textContent:text,action});
 const el=(tag,_class,text='')=>Object.assign(new Node(tag,state),{textContent:text});
 const createDialog=(_title,onClose)=>{const dialog={element:new Node('dialog',state),body:new Node('body',state),close(){onClose();}};state.dialogs.push(dialog);return dialog;};
 const host={view,runtime,ownerDocument:doc,message:new Node('message',state),panel:Object.assign(new Node('panel',state),{hidden:true}),button,el,createDialog};
 let observedMessage=host.message.textContent;
 Object.defineProperty(host.message,'textContent',{get:()=>observedMessage,set:text=>{observedMessage=text;state.messages.push(text);}});
 const context=vm.createContext({exports:{},AbortController,URL,createAppRuntimePhaseTrace,createAppRuntimeDisplay,captureSourcePin,matchesScopedCaptureContext,isScopedRuntimeProfile});new vm.Script(compiled).runInContext(context);
 const h=context.exports.harness(host,{...(presentation?{presentation}:{}),onModalChange(){},api:{request:async(op,args)=>{state.reads.push({op,args});return state.context;}}},state);
 const tick=ms=>{const target=now+ms;while(true){const t=[...tasks].sort((a,b)=>a[1].at-b[1].at)[0];if(!t||t[1].at>target)break;tasks.delete(t[0]);now=t[1].at;t[1].fn();}now=target;};
 return{h,state,tasks,tick,view,host};
}

test('actual manual buttons record one ordinary launch; initial/automatic/pending/widget paths cannot consume arm',async()=>{
 for(const action of ['refresh','retry']){const f=fixture();f.h.api.arm();f.h[action]();await flush();assert.equal(f.state.calls.launch,1);assert.equal(f.h.api.snapshot().events[0].phase,'root_launch');assert.deepEqual(f.state.verified,[false,true]);}
 for(const action of ['initial','automatic']){const f=fixture();f.h.api.arm();await f.h[action]();assert.equal(f.state.calls.launch,1);assert.equal(f.h.api.snapshot(),null);assert.equal(f.h.api.state(),'armed');}
 const pending=fixture();pending.h.api.arm();pending.h.pending(true);pending.h.refresh();await flush();assert.equal(pending.state.calls.launch,0);assert.deepEqual(pending.state.verified,[]);assert.equal(pending.h.api.state(),'armed');pending.h.pending(false);pending.h.refresh();await flush();assert.equal(pending.state.calls.launch,1);
 const widget=fixture('widget');widget.h.api.arm();widget.h.refresh();await flush();assert.equal(widget.state.calls.launch,1);assert.equal(widget.h.api.snapshot(),null);assert.equal(widget.state.probes.length,0);
 const failed=fixture();failed.state.failure=new Error('synthetic launch refusal');failed.h.refresh();await flush();assert.deepEqual(failed.state.verified,[false]);assert.equal(failed.state.calls.launch,1);assert.equal(failed.h.frame(),null);
});

test('actual launch wiring observes before src/insert and preserves existing load/error/timeout behavior and drafts',async()=>{
 const run=async diagnostic=>{const f=fixture();if(diagnostic)f.h.api.arm();f.h.refresh();await flush();const frame=f.h.frame();assert.ok(frame);if(diagnostic){assert.ok(f.state.order.indexOf('listen:iframe:load')<f.state.order.indexOf('src'));assert.ok(f.state.order.indexOf('src')<f.state.order.lastIndexOf('insert:runtime'));}
   frame.emit('load');f.tick(12_001);assert.equal(f.state.draft,'retained discussion draft');return f;};
 const off=await run(false),on=await run(true);assert.deepEqual(on.state.calls,off.state.calls);assert.deepEqual(on.state.messages,off.state.messages);assert.equal(on.state.calls.launch,1);assert.equal(on.h.api.snapshot().events.some(x=>x.phase==='root_timeout'),false);assert.equal(on.h.api.snapshot().observation,'partial');
 const late=fixture();late.h.api.arm();late.h.refresh();await flush();late.tick(12_000);assert.match(late.state.messages.at(-1),/Загрузка окна не подтверждена/u);late.h.frame().emit('load');assert.match(late.state.messages.at(-1),/Окно загружено/u);assert.equal(late.h.api.snapshot().observation,'partial');
});

test('actual route reset rejects old peers and erases diagnostics without remounting iframe or touching drafts',async()=>{
 const f=fixture();f.h.api.arm();f.h.refresh();await flush();const frame=f.h.frame();f.h.route();assert.equal(f.h.api.snapshot(),null);assert.equal(f.h.frame(),frame);assert.equal(f.state.calls.launch,1);assert.equal(f.state.draft,'retained discussion draft');
 f.view.emit('message',{source:frame.contentWindow,origin:'https://frame.example',ports:[],data:{schema:'soty.runtime-phase.v1',phase:'boot_cookie_pending'}});assert.equal(f.h.api.snapshot(),null);
});

test('actual stage distinguishes load from a correlated Boot reply and preserves visible unconfirmed recovery and the retained draft',async()=>{
 const f=fixture();f.h.api.arm();f.h.refresh();await flush();f.tick(0);const frame=f.h.frame();
 assert.ok(f.state.probes.some(p=>p.data.schema==='soty.runtime-phase-watch.v2'));
 const watchId=f.state.probes.find(p=>p.data.schema==='soty.runtime-phase-watch.v2').data.watchId;
 frame.emit('load');f.tick(12_001);
 assert.equal(f.h.api.snapshot().events.at(-1).phase,'root_boot_unobserved');
 assert.match(f.state.messages.at(-1),/запуск приложения не подтверждён/u);
 f.view.emit('message',{source:frame.contentWindow,origin:'https://frame.example',ports:[],data:{schema:'soty.runtime-phase.v2',watchId,phase:'boot_failed'}});
 assert.equal(f.h.api.snapshot().events.at(-1).phase,'boot_failed');
 assert.equal(f.h.api.snapshot().events.filter(x=>x.phase==='boot_document_ack').length,1);
 assert.equal(f.state.calls.launch,1);assert.match(f.state.messages.at(-1),/запуск приложения не подтверждён/u);assert.equal(f.state.draft,'retained discussion draft');
});

for(const missedFirstProbe of [false,true])test(`actual stage exposes ready-to-failed Boot without replay or frame replacement; initial watch ${missedFirstProbe?'lost':'delivered'}`,async()=>{
 const f=fixture(),source={id:'app.selected-workspace',version:1,digest:'a'.repeat(64)},expiresAt=Date.now()+120000;
 f.state.binding={source,slot:{},handle:'synthetic-slot'};
 f.state.context={ready:true,appId:'app-synthetic',scopedSource:source,target:{revision:1,digest:'b'.repeat(64)},expiresAt,
  sourceSession:{ready:true,accessExpiresAt:expiresAt,sessionExpiresAt:expiresAt}};
 await f.h.initial();const frame=f.h.frame();
 if(missedFirstProbe){f.tick(0);assert.equal(f.state.probes.filter(p=>p.data.schema==='soty.runtime-display-watch.v1').length,1);}
 const child=new Node('boot-window',f.state);
 const parent={postMessage(data,target){assert.equal(target,'https://shell.example');
  f.view.emit('message',{source:child,origin:'https://frame.example',ports:[],data});}};
 child.parent=parent;child.postMessage=(data,target)=>{assert.equal(target,'https://frame.example');
  child.emit('message',{source:parent,origin:'https://shell.example',ports:[],data});};
 frame.contentWindow=child;
 // The managed Boot sender is byte-identical to the serving cbeb version.
 // Execute its real watch/phase protocol; no browser paint claim is made here.
 const bootContext=vm.createContext({window:child,setTimeout:f.view.setTimeout,clearTimeout:f.view.clearTimeout});
 const boot=vm.runInContext(`(${createBootDisplayClient.toString()})({mode:'boot',parentOrigins:['https://shell.example']})`,bootContext);
 boot.phase('boot_continuation_pending');if(!missedFirstProbe)f.tick(0);frame.emit('load');await flush();
 assert.equal(f.host.message.textContent,'');assert.equal(f.h.controls().recoveryRetry.hidden,true);
 assert.equal(f.state.reads.length,1);assert.equal(f.state.reads[0].op,'apps.scoped.context');
 boot.phase('boot_failed');
 assert.match(f.host.message.textContent,/Не удалось подтвердить запуск/u);assert.equal(f.host.message.hidden,false);
 for(const control of Object.values(f.h.controls()))assert.equal(control.hidden,false);
 assert.equal(f.h.frame(),frame);assert.equal(f.state.calls.launch,1);assert.equal(f.state.reads.length,1);
 assert.equal(f.state.draft,'retained discussion draft');
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
