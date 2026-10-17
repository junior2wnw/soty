import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import ts from 'typescript';
import vm from 'node:vm';
import * as state from './assistant-state.mjs';
import * as device from './device-key.mjs';
import * as personal from './personal-agent-client.ts';

// Execute the actual mount with inert DOM and controllable timer ports.
// No browser, network request, app execution or model request is started.
const source=ts.transpileModule(readFileSync(new URL('./assistant.ts',import.meta.url),'utf8'),
  {compilerOptions:{module:ts.ModuleKind.CommonJS,target:ts.ScriptTarget.ES2022}}).outputText;
const defer=()=>{let resolve;const promise=new Promise(done=>{resolve=done;});return{promise,resolve};};
const tick=async()=>{for(let i=0;i<8;i++)await new Promise(done=>setImmediate(done));};

async function fixture(status) {
  const nodes=[],timers=new Map(),calls=[],doc=new EventTarget(),window=new EventTarget(),exports={};let nextTimer=0;
  let accountId='review_poll_account';const id='review_poll_original_01',records=new Map(),saved={schema:'soty.personal-agent-ui.v1',
    draft:'Original exact retained question',active:id,pending:[{requestId:id,prompt:'Original exact retained question'}]};
  records.set(`soty.personal-agent-ui.v1:${accountId}`,JSON.stringify(saved));
  const storage={getItem:key=>records.get(key)??null,setItem:(key,value)=>records.set(key,value)};
  class Port extends EventTarget {
    constructor(tag,className='',text=''){super();Object.assign(this,{tagName:tag.toUpperCase(),className,children:[],dataset:{},style:{},hidden:false,disabled:false,value:'',ownerDocument:doc});this.textContent=text;nodes.push(this);}
    append(...children){for(const child of children){this.children.push(child);child.parent=this;}}
    replaceChildren(...children){for(const child of this.children)child.parent=null;this.children=[];this.text='';this.append(...children);}
    get textContent(){return this.text+this.children.map(child=>child.textContent).join('');}
    set textContent(text){this.text=text;this.children=[];}
    get childElementCount(){return this.children.length;}
    get options(){return this.children.filter(child=>child.tagName==='OPTION');}
    get isConnected(){return this===host||!!this.parent?.isConnected;}
    setAttribute(name,value){this[name]=value;}
    removeAttribute(name){delete this[name];}
    contains(node){return this===node||this.children.some(child=>child.contains(node));}
    querySelectorAll(selector){if(selector.includes(' '))return[];const matches=node=>selector.startsWith('.')?node.className.split(' ').includes(selector.slice(1)):node.tagName.toLowerCase()===selector;return this.children.flatMap(child=>[...(matches(child)?[child]:[]),...child.querySelectorAll(selector)]);}
    querySelector(selector){return this.querySelectorAll(selector)[0]??null;}
    focus(){doc.activeElement=this;}
    setSelectionRange(){}
  }
  const el=(...args)=>new Port(...args),button=(label,_icon,className='',callback)=>{const node=el('button',className);node.append(el('span','',label));if(callback)node.addEventListener('click',callback);return node;};
  const dom={el,button,iconButton:(label,icon,callback)=>button(label,icon,'',callback),heading:(title,description)=>el('div','',title+description),
    textInput:(value,placeholder)=>Object.assign(el('input'),{value,placeholder}),labeledField:(_label,input)=>input};
  const client={getLocalState:async()=>({accountId}),async extension(op,args,options){calls.push({op,args,options});
    if(op==='apps.assistant.history')return{jobs:[],nextCursor:null};if(op==='apps.devices')return{devices:[]};
    assert.equal(op,'agent.status');return status({id,calls,accountId});}};
  doc.hidden=false;doc.activeElement=null;const host=el('main');
  const dependencies={'../world/dom':dom,'../world/icons':{icon:()=>el('i')},'./assistant-state.mjs':state,'./device-key.mjs':device,
    './personal-agent-client':personal,'./assistant.css':{},'./assistant-answer-note':{createAssistantAnswerNote(){throw Error('not part of polling review');}},
    './copy-visible-text':{},'../../modules/notes/browser/state.mjs':{createDraftStore(){throw Error('not part of polling review');}}};
  vm.runInNewContext(source,{exports,require:name=>{assert.ok(name in dependencies,name);return dependencies[name];},
    window,document:doc,HTMLElement:Port,localStorage:storage,sessionStorage:storage,crypto:globalThis.crypto,AbortController,AbortSignal,
    matchMedia:()=>({matches:false}),fetch:async()=>({ok:true,json:async()=>({agentConfigured:true,personalAgent:{configured:true}})}),
    setTimeout:(callback,ms)=>{const id=++nextTimer;timers.set(id,{callback,ms});return id;},clearTimeout:id=>timers.delete(id),location:{reload(){throw Error('unexpected navigation');}}});
  const handle=await exports.mountAssistant(host,{client,connectDevice(){},createApp(){throw Error('unexpected app');},resumeApp(){}});
  await tick();
  const click=async label=>{const node=nodes.filter(node=>node.tagName==='BUTTON'&&node.textContent===label&&host.contains(node)).at(-1);assert.ok(node,label);assert.equal(node.disabled,false,label);node.dispatchEvent(new Event('click'));await tick();};
  return {host,handle,calls,timers,nodes,click,records,id,account:value=>{accountId=value;},
    async fire(){assert.equal(timers.size,1);const[id,timer]=timers.entries().next().value;timers.delete(id);timer.callback();await tick();}};
}
const running=id=>({schema:'soty.personal-agent-status.v1',requestId:id,state:'running',stage:'model',elapsedMs:1000,modelRounds:1,completedTools:0,cancelRequested:false});

test('the actual assistant stops automatic polling after three failures and manual check can resume it',async t=>{
  let reads=0,fail=true;const f=await fixture(({id})=>{reads++;if(reads>1&&fail)throw TypeError('synthetic status outage');return running(id);});t.after(()=>f.handle.dispose());
  await f.click('Проверить выполнение');assert.equal(reads,1);assert.equal(f.timers.size,1);
  await f.fire();await f.fire();await f.fire();assert.equal(reads,4);assert.equal(f.timers.size,0);
  fail=false;await f.click('Проверить выполнение');assert.equal(reads,5);assert.equal(f.timers.size,1);
  assert.equal(f.calls.filter(call=>call.op==='agent.turn').length,0);
});

test('a disposed assistant cannot publish a late status, restart polling or erase the retained intent',async()=>{
  const gate=defer(),entered=defer();const f=await fixture(async({id})=>{entered.resolve();await gate.promise;return running(id);});
  const checking=f.click('Проверить выполнение');await entered.promise;const record=[...f.records.values()].find(text=>text.includes('review_poll_original_01'));
  f.handle.dispose();gate.resolve();await checking;
  assert.equal(f.host.childElementCount,0);assert.equal(f.timers.size,0);
  assert.ok([...f.records.values()].includes(record));assert.equal(f.calls.filter(call=>call.op==='agent.turn').length,0);
});

test('switching to device mode while status is pending cannot restart personal polling',async t=>{
  const gate=defer(),entered=defer();const f=await fixture(async({id})=>{entered.resolve();await gate.promise;return running(id);});t.after(()=>f.handle.dispose());
  const checking=f.click('Проверить выполнение');await entered.promise;await f.click('На компьютере');gate.resolve();await checking;
  assert.equal(f.timers.size,0);assert.equal(f.calls.filter(call=>call.op==='agent.status').length,1);
  assert.equal(f.calls.filter(call=>call.op==='agent.turn').length,0);
});
