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

function fixture({ownerApps=[own(1)],catalogApps=[],unavailableIds=[],beforeRead=async()=>{},resolveError}={}){
 const document=personal(),updates=[],calls=[],metadataMap=new Map(),state={current:true},exports={};
 const environment={exports,mode:'mine',liveRead:0,livePages:1,liveApps:[],liveCursor:null,liveAppsFramed:false,liveProjection:createLiveAppScene(),element:{dataset:{}},
  current:()=>state.current,unplacedOwnApps,fieldEntityKey:ref=>ref.kind+':'+ref.id,renderMineSearchResults(){},renderOpenList:null,
  options:{accountId:'account-owner',api:{async request(op,args){calls.push({op,args});assert.ok(['apps.list','apps.catalog'].includes(op));assert.equal(args.expectedAccountId,'account-owner');if(op==='apps.catalog')return{apps:catalogApps};await beforeRead();return{apps:ownerApps};}}},
  directory:{async search(args){calls.push({op:'public-discovery',args});assert.fail('live apps must use catalogue IDs and the exact resolver, not paged discovery');},
   async resolve(refs){calls.push({op:'exact-resolve',refs});assert.ok(refs.length<=60);if(resolveError)throw resolveError;return{
    items:refs.filter(ref=>!unavailableIds.includes(ref.id)).map(ref=>metadata(ref.id,ownerApps.some(app=>app.id===ref.id&&app.ownerAccountId==='account-owner')?'owner':'public')),
    unavailable:refs.filter(ref=>unavailableIds.includes(ref.id)),errors:[]};}},
  engine:{snapshot:()=>document,update(value){updates.push(value);}},
  merge(items){for(const item of items)metadataMap.set(item.entity.id,item);},allMetadata:()=>[...metadataMap.values()],
 };
 installCoverageTestPorts(environment);vm.runInNewContext(code,environment);
 return{document,updates,calls,environment,state,refresh:exports.refresh};
}

test('Mine keeps own registered apps and explicit foreign refs when the public catalogue is empty',async()=>{
 const f=fixture(),before=JSON.stringify(f.document);await f.refresh();
 const update=f.updates.at(-1);assert.ok(update?.liveScene);
 assert.deepEqual(update.liveScene.shortcuts.map(shortcut=>shortcut.entity.id),[appId(1)]);
 const combined=combineFieldScene(f.document,update.liveScene);
 assert.ok(combined.shortcuts.some(shortcut=>shortcut.entity.id===appId(9000)),'an explicitly kept foreign app must remain');
 assert.equal(JSON.stringify(f.document),before);assert.equal(f.updates.some(value=>'document'in value||'camera'in value),false);
 assert.equal(f.calls.some(call=>call.op==='public-discovery'),false,'the live layer does not depend on discovery paging');
 assert.equal(f.calls.filter(call=>call.op==='apps.catalog').length,1);
});
test('all 100 owned registry apps survive alongside newer public apps within the resolver batch limit',async()=>{
 const ownerApps=Array.from({length:100},(_,index)=>own(index+1)),catalogApps=Array.from({length:60},(_,index)=>({id:appId(index+1000)})),f=fixture({ownerApps,catalogApps});
 await f.refresh();const ids=f.updates.at(-1).liveScene.shortcuts.map(shortcut=>shortcut.entity.id);
 assert.equal(ids.length,160);assert.ok(ownerApps.every(app=>ids.includes(app.id)),'all 100 owned apps survive the public catalogue union');
 assert.ok(catalogApps.every(app=>ids.includes(app.id)));
 assert.deepEqual(f.calls.filter(call=>call.op==='exact-resolve').map(call=>call.refs.length),[60,60,40],'the existing resolver limit is preserved');
});
test('an empty registry and public catalogue keep the live layer empty',async()=>{
 const f=fixture({ownerApps:[]});await f.refresh();
 assert.equal(f.updates.at(-1).liveScene.shortcuts.length,0);assert.equal(f.environment.liveApps.length,0);
});
test('catalogue IDs appear only after exact authority resolves them, without duplicate or denied launchers',async()=>{
 const publicId=appId(2),deniedId=appId(3),f=fixture({ownerApps:[],catalogApps:[{id:publicId,name:'Untrusted catalogue title'},{id:publicId},{id:deniedId}],unavailableIds:[deniedId]});
 const before=JSON.stringify(f.document);await f.refresh();
 assert.deepEqual(f.updates.at(-1).liveScene.shortcuts.map(shortcut=>shortcut.entity.id),[publicId]);
 assert.deepEqual(Array.from(f.environment.liveApps,item=>[item.entity.id,item.source,item.title]),[[publicId,'public',publicId]],'only resolver metadata can become live');
 assert.deepEqual(f.calls.filter(call=>call.op==='exact-resolve').flatMap(call=>call.refs.map(ref=>ref.id)),[publicId,deniedId]);
 assert.equal(JSON.stringify(f.document),before);assert.equal(f.updates.some(value=>'document'in value||'camera'in value),false);
});
test('a late owned read after account retirement cannot render, move the camera or publish watch refs',async()=>{
 let release;const f=fixture({beforeRead:()=>new Promise(resolve=>{release=resolve;})}),pending=f.refresh();
 await new Promise(resolve=>setImmediate(resolve));f.state.current=false;release();await pending;
 assert.equal(f.updates.length,0);assert.equal(f.environment.liveApps.length,0);
});
