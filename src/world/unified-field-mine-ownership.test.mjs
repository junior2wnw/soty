import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {createLiveAppScene} from './live-app-scene.mjs';
import {combineFieldScene} from './unified-field-live.mjs';
import {unplacedOwnApps} from './own-apps-overview.mjs';
import {coverageTestSource,installCoverageTestPorts} from './unified-field-coverage-test-ports.mjs';

const source=readFileSync(new URL('./unified-field-screen.ts',import.meta.url),'utf8');
const parsed=ts.createSourceFile('screen.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS),printer=ts.createPrinter();
const factory=parsed.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='createUnifiedFieldScreen');
const refresh=factory.body.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='refreshLiveApps');
const code=ts.transpileModule(coverageTestSource(factory.body.statements,parsed,printer)+'\n'+printer.printNode(ts.EmitHint.Unspecified,refresh,parsed)+'\nexports.refresh=refreshLiveApps;',
 {compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
const appId=index=>'app-'+index.toString(16).padStart(32,'0');
const metadata=(id,source='public')=>({entity:{kind:'app',id},title:id,source});
const own=index=>({id:appId(index),name:'Own '+index,ownerAccountId:'account-owner',state:'ready',createdAt:index});
const personal=()=>({schema:'soty.field.v1',contexts:[{contextId:'personal',title:'Моё',x:19,y:31}],
 shortcuts:[{shortcutId:'explicit-foreign',entity:{kind:'app',id:appId(9000)},contextId:'personal',slot:[0,0]}]});

function fixture({ownerApps=[own(1)],publicApps=[metadata(appId(2)),metadata(appId(1),'owner')],beforeRead=async()=>{},resolveError}={}){
 const document=personal(),updates=[],calls=[],metadataMap=new Map(),state={current:true},exports={};
 const environment={exports,liveRead:0,livePages:1,liveApps:[],liveCursor:null,liveProjection:createLiveAppScene(),element:{dataset:{}},
  current:()=>state.current,unplacedOwnApps,fieldEntityKey:ref=>ref.kind+':'+ref.id,
  options:{accountId:'account-owner',api:{async request(op,args){calls.push({op,args});assert.equal(op,'apps.list');assert.equal(args.expectedAccountId,'account-owner');await beforeRead();return{apps:ownerApps};}}},
  directory:{async search(args){calls.push({op:'public-discovery',args});await beforeRead();return{items:publicApps,cursor:null,errors:[]};},
   async resolve(refs){calls.push({op:'exact-resolve',refs});assert.ok(refs.length<=60);if(resolveError)throw resolveError;return{items:refs.map(ref=>metadata(ref.id,'owner')),unavailable:[],errors:[]};}},
  engine:{snapshot:()=>document,update(value){updates.push(value);}},
  merge(items){for(const item of items)metadataMap.set(item.entity.id,item);},allMetadata:()=>[...metadataMap.values()],
 };
 installCoverageTestPorts(environment);vm.runInNewContext(code,environment);
 return{document,updates,calls,environment,state,refresh:exports.refresh};
}

test('Mine contains own registered apps and explicit foreign refs, without pouring public discovery onto it',async()=>{
 const f=fixture(),before=JSON.stringify(f.document);await f.refresh();
 const update=f.updates.at(-1);assert.ok(update?.liveScene);
 assert.deepEqual(update.liveScene.shortcuts.map(shortcut=>shortcut.entity.id),[appId(1)]);
 const combined=combineFieldScene(f.document,update.liveScene);
 assert.ok(combined.shortcuts.some(shortcut=>shortcut.entity.id===appId(9000)),'an explicitly kept foreign app must remain');
 assert.equal(JSON.stringify(f.document),before);assert.equal(f.updates.some(value=>'document'in value||'camera'in value),false);
 assert.equal(f.calls.some(call=>call.op==='public-discovery'),false,'Mine does not enumerate the public catalogue');
});
test('all owned registry apps survive even when global discovery pages are full of newer foreign apps',async()=>{
 const ownerApps=Array.from({length:100},(_,index)=>own(index+1)),f=fixture({ownerApps,publicApps:Array.from({length:60},(_,index)=>metadata(appId(index+1000)))});
 await f.refresh();const ids=f.updates.at(-1).liveScene.shortcuts.map(shortcut=>shortcut.entity.id);
 assert.equal(ids.length,100);assert.ok(ids.includes(appId(1))&&ids.includes(appId(100)));
 assert.equal(f.calls.filter(call=>call.op==='exact-resolve').length,2,'the existing resolver limit is preserved');
});
test('an empty own registry keeps Mine empty regardless of how many public apps exist',async()=>{
 const f=fixture({ownerApps:[],publicApps:[metadata(appId(2))]});await f.refresh();
 assert.equal(f.updates.at(-1).liveScene.shortcuts.length,0);assert.equal(f.environment.liveApps.length,0);
});
test('a late owned read after account retirement cannot render, move the camera or publish watch refs',async()=>{
 let release;const f=fixture({beforeRead:()=>new Promise(resolve=>{release=resolve;})}),pending=f.refresh();
 await new Promise(resolve=>setImmediate(resolve));f.state.current=false;release();await pending;
 assert.equal(f.updates.length,0);assert.equal(f.environment.liveApps.length,0);
});
