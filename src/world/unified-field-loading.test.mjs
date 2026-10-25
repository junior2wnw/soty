import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import { createFieldPersistence } from './field-persistence.ts';
import { fieldEntityKey,validateFieldDocument } from '../../modules/field/contract.mjs';
import { createPersonalFieldDocument } from './app-field-placement.mjs';

// Execute the complete production ready IIFE, cache subscription/mount and
// dispose. Persistence/validation are real; paint and directory requests are
// held ports. This is not a screenshot or a claim that placeholders grant access.
const source=readFileSync(new URL('./unified-field-screen.ts',import.meta.url),'utf8');
const parsed=ts.createSourceFile('unified-field-screen.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS);
const screen=parsed.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='createUnifiedFieldScreen');
const printer=ts.createPrinter(),print=node=>printer.printNode(ts.EmitHint.Unspecified,node,parsed);
const variable=(nodes,name)=>nodes.find(node=>ts.isVariableStatement(node)&&node.declarationList.declarations.some(item=>item.name.getText(parsed)===name));
const functions=['mountEngine','revealReadyMine','applyPersistence','changeMode','loadInitialMine'].map(name=>screen.body.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text===name)).filter(Boolean);
const subscribe=screen.body.statements.find(node=>ts.isExpressionStatement(node)&&node.getText(parsed)==='persistence.subscribe(applyPersistence);');
const returned=screen.body.statements.find(ts.isReturnStatement).expression;
const dispose=returned.properties.find(node=>ts.isMethodDeclaration(node)&&node.name.getText(parsed)==='dispose');
const ready=variable(screen.body.statements,'ready');assert.ok(ready&&subscribe&&dispose,'genuine full lifecycle must exist');
const compiled=ts.transpileModule([print(variable(parsed.statements,'BUILTINS')),print(variable(screen.body.statements,'initialMineTask')),...functions.map(print),print(subscribe),print(ready),
  'exports.api = { ready, changeMode, mountEngine, '+print(dispose)+' };'].join('\n'),{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
const deferred=()=>{let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return{promise,resolve,reject};};
const turn=()=>new Promise(done=>setImmediate(done));
async function until(predicate){const end=Date.now()+3000;while(!predicate()){assert.ok(Date.now()<end,'bounded ready/cache phase completed');await turn();}}

function fixture(t,{mode='mine',builtin=true,unresolved=true,width=1000}={}){
  const app={kind:'app',id:'app-'+'a'.repeat(32)};
  const doc=validateFieldDocument({schema:'soty.field.v1',contexts:[{contextId:'personal',title:'Личное',x:0,y:0}],shortcuts:[
    ...(builtin?[{shortcutId:'notes',entity:{kind:'builtin',id:'notes'},contextId:'personal',slot:[0,0]}]:[]),
    ...(unresolved?[{shortcutId:'unknown_app',entity:app,contextId:'personal',slot:[3,0]}]:[]),
  ]});
  const base={revision:7,document:doc,contentHash:createHash('sha256').update(JSON.stringify(doc)).digest('hex'),updatedAt:1000};
  let saved={schema:'soty.field.local.v1',scope:JSON.stringify(['local','soty','owner']),localRevision:1,base,pending:[],conflict:false,remote:null,conflictDocument:null};
  const remote=deferred(),directory=deferred(),paint=deferred(),fields=[],calls=[],messages=[];let accountGeneration=1;
  const owned=()=>accountGeneration===1;
  const persistence=createFieldPersistence({accountId:'owner',localFirst:true,isCurrent:owned,
    store:{read:async()=>structuredClone(saved),write:async(value,expected)=>{assert.equal(saved.localRevision,expected);saved=structuredClone(value);}},
    api:{async request(op,args){calls.push(op);assert.equal(op,'world.field.get');assert.equal(args.expectedAccountId,'owner');await remote.promise;return structuredClone(base);}}});
  const metadata=new Map(),loading={hidden:false},element={dataset:{ready:'loading'},remove(){}},abort=new AbortController();
  const environment={exports:{},crypto,disposed:false,mode,filter:'all',filtersByMode:{mine:'all',search:'all'},options:{accountId:'owner',isCurrent:owned,onMessage(){}},
    engine:null,state:null,metadata,fieldEntityKey,persistence,current:()=>!environment.disposed&&owned(),
    allMetadata:()=>[...metadata.values()],updateChrome(){},updateStatus(){},updateMineFilter(){},closeList(){},closePreview(){},route(){},
    empty:{hidden:true},mineResolvedGeneration:-1,mineFilterAdmitted:false,firstResolve:false,element,loading,
    activate(){},inspect(){},updateSummary(){},searchScene:()=>({}),searchPlacement:{contextEntity:()=>null},openPreview(){},
    window:{innerWidth:width},resolveFieldAppArt:()=>null,host:{prepend(){}},
    createPersonalFieldDocument,nextFieldSlot:()=>[0,0],merge(){},
    async loadMine(){element.dataset.mineState='loading';calls.push('mine-directory');await directory.promise;},
    async searchDirectory(){calls.push('public-directory');await directory.promise;},
    report(error){if(environment.current())messages.push(error.code??error.message);},run(promise){void promise?.catch(environment.report);},
    searchGeneration:0,mineGeneration:0,searchTimer:null,queuedSearch:null,queuedMine:null,abort,dialogs:new Set(),listDialog:null,
    search:{remove(){}},listButton:{remove(){}},directory:{dispose(){}},
    createUnifiedField(options){let document=options.document;const fieldPaint=fields.length?deferred():paint;const field={options,paint:fieldPaint,ready:fieldPaint.promise,element:{clientWidth:width,clientHeight:700},
      snapshot:()=>document,update(value){if(value.document)document=value.document;},hasUnsavedChanges:()=>false,
      cancel(){},destroy(){fieldPaint.resolve();}};fields.push(field);return field;},
  };
  vm.createContext(environment);vm.runInContext(compiled,environment);
  // Static builtins are metadata only until the validated document places one.
  for(const id of ['notes','chess','assistant'])metadata.set(fieldEntityKey({kind:'builtin',id}),{entity:{kind:'builtin',id},title:id,source:'builtin'});
  t.after(async()=>{environment.exports.api.dispose();remote.resolve();directory.resolve();paint.resolve();await environment.exports.api.ready;});
  return{environment,api:environment.exports.api,fields,calls,messages,metadata,loading,remote,directory,paint,doc,
    remoteDocument(document){base.document=validateFieldDocument(document);base.revision++;base.contentHash=createHash('sha256').update(JSON.stringify(base.document)).digest('hex');},
    accountAba(){accountGeneration=2;accountGeneration=3;}};
}

test('cached refs start a fresh directory read before the held field response, without granting cached metadata or duplicating the initial read',async t=>{
  const f=fixture(t);await until(()=>f.fields.length===1&&f.calls.includes('world.field.get'));
  assert.equal(f.loading.hidden,false);assert.equal(f.calls.includes('mine-directory'),true);
  const cached=JSON.stringify(f.fields[0].snapshot());f.paint.resolve();await turn();
  assert.equal(f.loading.hidden,true,'an opaque loading overlay must not cover a genuine usable cached tile');
  assert.equal(f.environment.element.dataset.ready,'loading');assert.equal(f.calls.filter(op=>op==='mine-directory').length,1,'the remote layout and fresh directory read run together');
  assert.equal(f.metadata.get(fieldEntityKey(f.doc.shortcuts.at(-1).entity)).title,'Загрузка…');
  assert.equal(JSON.stringify(f.fields[0].snapshot()),cached);
  f.remote.resolve();await turn();
  assert.equal(f.loading.hidden,true);assert.equal(f.environment.element.dataset.mineState,'loading');assert.equal(f.environment.element.dataset.ready,'loading');
  assert.equal(f.calls.filter(op=>op==='mine-directory').length,1,'the same remote references reuse the initial read');
  f.directory.resolve();await f.api.ready;assert.equal(f.environment.element.dataset.ready,'ready');
});

test('new remote identities trigger their own fresh read while the original cached lookup is held',async t=>{
  const f=fixture(t);await until(()=>f.fields.length===1&&f.calls.includes('mine-directory'));
  const next=structuredClone(f.doc),ref={kind:'app',id:'app-'+'b'.repeat(32)};
  next.shortcuts.push({shortcutId:'new_remote_app',entity:ref,contextId:'personal',slot:[6,0]});f.remoteDocument(next);
  f.paint.resolve();f.remote.resolve();await until(()=>f.calls.filter(op=>op==='mine-directory').length===2);
  assert.equal(f.metadata.has(fieldEntityKey(ref)),false,'a new reference carries no cached display authority');
  f.directory.resolve();await f.api.ready;assert.equal(f.calls.filter(op=>op==='mine-directory').length,2);
});

test('empty or unresolved cached contexts and a zero-size viewport do not masquerade as a usable field',async t=>{
  for(const options of [{builtin:false},{builtin:false,unresolved:false},{width:0}]){
    const f=fixture(t,options);await until(()=>f.fields.length===1);f.paint.resolve();await turn();assert.equal(f.loading.hidden,false);
  }
});

test('Search keeps its initial loading overlay while all public results are held',async t=>{
  const f=fixture(t,{mode:'search'});await until(()=>f.fields.length===1);f.paint.resolve();await turn();assert.equal(f.loading.hidden,false);
});

test('returning from pending Search reveals the already measured useful Mine without waiting for any response',async t=>{
  const f=fixture(t,{mode:'search'});await until(()=>f.fields.length===1);f.paint.resolve();await turn();
  f.api.changeMode('mine');await turn();assert.equal(f.loading.hidden,true);assert.equal(f.calls.filter(op=>op==='mine-directory').length,1);
});

test('an old engine resolving after a replacement cannot reveal a new engine before its own paint',async t=>{
  const f=fixture(t);await until(()=>f.fields.length===1);f.api.mountEngine(f.environment.state);assert.equal(f.fields.length,2);
  f.paint.resolve();await turn();assert.equal(f.loading.hidden,false);
  f.fields[1].paint.resolve();await turn();assert.equal(f.loading.hidden,true);
});

test('late paint after account A-B-A or actual screen disposal cannot reveal retired cached content',async t=>{
  for(const stop of ['account','dispose']){
    const f=fixture(t);await until(()=>f.fields.length===1);
    if(stop==='account')f.accountAba();else f.api.dispose();
    f.paint.resolve();await turn();assert.equal(f.loading.hidden,false);assert.equal(f.environment.element.dataset.ready,'loading');
  }
});

test('a rejected engine ready cannot reveal the cache while the remote field read is still pending',async t=>{
  const f=fixture(t);await until(()=>f.fields.length===1);
  // Old code has no observer of the paint port until field.get finishes.
  void f.paint.promise.catch(()=>{});f.paint.reject(Object.assign(Error('synthetic paint failure'),{code:'paint_unavailable'}));await turn();
  assert.equal(f.loading.hidden,false);assert.deepEqual(f.messages,['paint_unavailable']);
});
