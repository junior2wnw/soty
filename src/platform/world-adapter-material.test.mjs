import assert from 'node:assert/strict';
import test from 'node:test';
import vm from 'node:vm';
import {readFile} from 'node:fs/promises';
import ts from 'typescript';

const source=await readFile(process.env.WORLD_MATERIAL_SOURCE||new URL('./world-adapter.ts',import.meta.url),'utf8');
const start=source.indexOf('  const openAccount = async '),end=source.indexOf('  let connectionOpening',start);
assert.ok(start>=0&&end>start,'test executes the actual account entry, unchanged');
const compiled=ts.transpileModule(source.slice(start,end)+'\nglobalThis.openAccount=openAccount;',{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
const turn=()=>new Promise(resolve=>setImmediate(resolve));
async function until(predicate){for(let n=0;n<30&&!predicate();n++)await turn();assert.ok(predicate(),'actual host reached the expected boundary');}
function fixture({holdState=false}={}){
  const panels=[],mounts=[],calls=[],observers=new Set(),window=new EventTarget(),pageLifetime=new AbortController();let release;
  window.location={href:'https://synthetic.invalid/#mine'};
  const world={supportsDirectMessages:()=>true,async openDirectContact(value){calls.push(['contact',value]);},editProfile(){calls.push(['edit']);}};
  const context={world,activeAccount:'account-A',mountGeneration:1,currentCallView:null,pageLifetime,window,document:{querySelector(){throw Error('global dialog lookup must not run');}},observeAccount(callback){observers.add(callback);return()=>observers.delete(callback);},accountClient:{getLocalState(){const result={accountId:context.activeAccount,label:'Synthetic'};return holdState?new Promise(resolve=>{release=()=>resolve(result);}):Promise.resolve(result);}},async refresh(){calls.push(['refresh']);},openLegacy(){}};
  const connect={openConnectPanel(options){const element=new EventTarget();element.ownerDocument={defaultView:{HTMLCanvasElement:class{}}};element.close=()=>element.dispatchEvent(new Event('close'));const panel={element,options,close:element.close,refresh:async()=>{}};panels.push(panel);return panel;}};
  const material={mountMaterialGlass(element,options){const row={element,options,disposals:0,dispose(){this.disposals++;}};mounts.push(row);return row;}};
  context.require=name=>name.includes('modules/connect/ui/index.mjs')?connect:name==='qrcode'?{default:{toDataURL:async()=>''}}:name==='../world/shell-glass'?material:{};
  vm.runInNewContext(compiled,context);
  return {context,panels,mounts,calls,observers,pageLifetime,release:()=>release(),change(){for(const callback of [...observers])callback();},open:()=>context.openAccount()};
}

test('host mounts only the returned panel element and disposes each close/reopen exactly once',async()=>{
  const f=fixture();
  for(let n=0;n<2;n++){const opened=f.open();await until(()=>f.mounts.length===n+1);assert.equal(f.mounts[n].element,f.panels[n].element);assert.equal(f.mounts[n].options.renderPolicy,'auto');f.panels[n].close();await opened;assert.equal(f.mounts[n].disposals,1);assert.equal(f.observers.size,0);}
  assert.equal(f.calls.filter(row=>row[0]==='refresh').length,2);assert.notEqual(f.panels[0].element,f.panels[1].element);
});

test('late local identity read cannot mount a dialog for the previous account',async()=>{
  const f=fixture({holdState:true}),opened=f.open();await until(()=>typeof f.context.accountClient.getLocalState==='function');await turn();f.context.activeAccount='account-B';f.context.mountGeneration++;f.release();await opened;assert.equal(f.panels.length,0);assert.equal(f.mounts.length,0);
});

test('account change closes its own panel and rejects late contact/edit callbacks',async()=>{
  const f=fixture(),opened=f.open();await until(()=>f.mounts.length===1);const panel=f.panels[0];
  f.context.activeAccount='account-B';f.context.mountGeneration++;panel.options.onContactMessage({peerAccountId:'foreign'});panel.options.onEditProfile();f.change();await opened;
  assert.equal(f.mounts[0].disposals,1);assert.deepEqual(f.calls,[]);assert.equal(f.observers.size,0);
});

test('page teardown closes its material root while readiness remains an independent runtime concern',async()=>{
  const f=fixture(),opened=f.open();await until(()=>f.mounts.length===1);f.pageLifetime.abort();await opened;assert.equal(f.mounts[0].disposals,1);assert.equal(f.observers.size,0);assert.deepEqual(f.calls,[]);
});
