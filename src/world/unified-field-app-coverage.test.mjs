import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {dirname,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import ts from 'typescript';

// Actual Screen orchestration, Directory and pure coverage. Only RPC and
// painting are controlled ports; no provider/crypto/registry stand-ins exist.
const root=fileURLToPath(new URL('../../',import.meta.url)),path=resolve(root,'src/world/unified-field-screen.ts');
const original=readFileSync(path,'utf8'),printer=ts.createPrinter();
const compile=text=>ts.transpileModule(text,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
const parse=text=>ts.createSourceFile(path,text,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS);
const appA='app-'+ 'a'.repeat(32),appB='app-'+ 'b'.repeat(32),foreign='app-'+ 'c'.repeat(32);
const objectId='object_'+ 'a'.repeat(32),widgetId='widget-11111111-1111-4111-8111-111111111111',accountId='coverage-owner';
const document=(object=true)=>({schema:'soty.field.v2',contexts:[{contextId:'personal',title:'Моё',x:21,y:-31}],shortcuts:[
 ...(object?[{shortcutId:'placed-widget',entity:{kind:'object',id:objectId},contextId:'personal',placement:{kind:'free',x:57,y:89,width:320,height:240}}]:[]),
 ...[0,1].map(index=>({shortcutId:'explicit-foreign-'+index,entity:{kind:'app',id:foreign},contextId:'personal',placement:{kind:'hex',slot:[index,2]}})),
]});
const projection=()=>({objectId,available:true,revision:1,ownerRepresents:[{kind:'app',id:appA}],view:{schema:'soty.canvas-widget-view.v1',kind:'widget',state:'ready',widgetId,instanceId:widgetId,canRefresh:true,app:{appId:appA,name:'Counter'}}});
const defer=()=>{let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return{promise,resolve,reject};};
const turn=()=>new Promise(resolve=>setImmediate(resolve));
async function settle(){for(let index=0;index<8;index++)await turn();}

function harness({text=original,hasObject=true,project=projection(),onRequest,viewerAccountId=accountId}={}){
 const parsed=parse(text),factory=parsed.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='createUnifiedFieldScreen');
 const statements=[...factory.body.statements],print=node=>printer.printNode(ts.EmitHint.Unspecified,node,parsed);
 const names=['merge','pruneMetadata','allMetadata','refreshLiveApps','loadOwnedApps','loadMine','loadMinePass','updateSummary','applyPersistence',
  'readAppCoverage','liveAppScene','refreshAppCoverage'];
 const functions=names.flatMap(name=>{const node=statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text===name);return node?[print(node)]:[];});
 const variables=statements.filter(node=>ts.isVariableStatement(node)&&node.declarationList.declarations.some(item=>item.name.getText(parsed)==='appProjectionGeneration')).map(print);
 const returned=statements.find(ts.isReturnStatement).expression;
 const methods=['refresh','dispose','watchEntities'].map(name=>returned.properties.find(node=>node.name?.getText(parsed)===name));
 const code=compile([...variables,...functions,`exports.api={refreshLiveApps,loadMine,applyPersistence,updateSummary,${methods.map(print).join(',')}};`].join('\n'));
 const state={current:true,project},calls=[],updates=[],exports={};
 const node=()=>({hidden:false,value:'',textContent:'',dataset:{},setAttribute(){},replaceChildren(){},append(){},contains(){return false;},remove(){},querySelector(){return{...node(),textContent:''};},firstElementChild:{setAttribute(){}}});
 const abort=new AbortController(),environment={exports,AbortController,TextEncoder,TextDecoder,crypto,performance,Buffer,URL,queueMicrotask,clearTimeout,setTimeout,
  disposed:false,current:()=>!environment.disposed&&state.current,options:{accountId:viewerAccountId},abort,signal:abort.signal,
  mode:'mine',filter:'all',queries:{mine:'',search:''},metadata:new Map(),metadataRevision:0,BUILTINS:[],searchItems:[],available:[],availableCursor:null,
  liveRead:0,livePages:1,liveApps:[],liveCursor:null,liveAppsFramed:false,mineGeneration:0,mineResolvedGeneration:-1,mineConfirmedGeneration:-1,mineBusy:false,queuedMine:null,
  mineKnownRefs:new Set(),firstResolve:false,selected:null,previewCommunity:null,summary:null,contextKey:'',state:null,emptyDocument:null,
  ownedApps:[],ownedAppsState:'ready',ownAppsReadSequence:0,ownAppsAppliedRead:0,knownAppSignature:'',mineFilterAdmitted:false,
  viewPercent:null,element:node(),arrange:node(),arrangeHint:node(),percent:node(),scaleControl:node(),undo:node(),contextSelector:node(),loading:node(),empty:node(),
  document:{activeElement:null},wide:{matches:true},summaryEvents:[],messages:[],ready:Promise.resolve(),
  closeList(){},closePreview(){},publishKnownApps(){},updateMineEmpty(){},updateStatus(){},renderOwnApps(){},renderMineSearchResults(){},renderOpenList:null,
  updateMineFilter(){},updateIdentity(){},searchDirectory:async()=>{},loadInitialMine:async()=>{},run:promise=>{promise?.catch(error=>environment.messages.push(error));},
  report:error=>environment.messages.push(error),el:()=>node(),
  searchGeneration:0,searchTimer:null,queuedSearch:null,dialogs:new Set(),fieldGlass:null,canvasContent:{dispose(){}},unobservePersistence(){},
  listDialog:null,search:node(),listButton:node(),
 };
 const context=vm.createContext(environment),clone=vm.runInContext('(value)=>JSON.parse(JSON.stringify(value))',context);environment.structuredClone=clone;
 const modules=new Map(),require=createRequire(import.meta.url);
 function module(code,modulePath){const output={};vm.compileFunction(code,['exports','require'],{parsingContext:context,filename:modulePath})(output,name=>name.startsWith('.')?load(resolve(dirname(modulePath),name)):require(name));return output;}
 function load(modulePath){if(!modules.has(modulePath))modules.set(modulePath,module(compile(readFileSync(modulePath,'utf8')),modulePath));return modules.get(modulePath);}
 Object.assign(environment,load(resolve(root,'modules/field/contract.mjs')),load(resolve(root,'src/world/live-app-scene.mjs')),
  load(resolve(root,'src/world/canonical-app-coverage.mjs')),load(resolve(root,'src/world/own-apps-overview.mjs')));
 let canonical=clone(document(hasObject)),liveScene={contexts:[],shortcuts:[]};const camera={x:321,y:-456,scale:.65};
 const summary=()=>({document:canonical,mode:'mine',camera:{...camera},focusContextId:'',selectedShortcutId:'',arranging:false,canUndo:false});
 const app=id=>({id,name:id===appA?'Counter':'Unplaced',ownerAccountId:viewerAccountId,state:'ready',access:'owner',createdAt:1,entry:{appId:id,domainId:'fixture',origin:'https://fixture.invalid',path:'/'}});
 const api={async request(op,args={},requestOptions){calls.push({op,args:clone(args),signal:requestOptions?.signal});
  if(onRequest){const custom=onRequest({op,args,requestOptions,state});if(custom!==undefined)return clone(await custom);}
  if(op==='apps.list')return clone({apps:[app(appA),app(appB)]});
  if(op==='apps.catalog')return clone({apps:[]});
  if(op==='apps.directory.resolve')return clone({items:args.appIds.map(id=>({ref:{kind:'app',id},available:true,app:app(id)}))});
  if(op==='canvas.objects.project'){assert.equal(args.expectedAccountId,viewerAccountId);return clone({items:args.objectIds.map(id=>({...state.project,objectId:id}))});}
  if(op==='world.directory.resolve')return clone({items:[]});
  if(op==='apps.directory.search')return clone({apps:[],nextCursor:null});
  if(op==='world.directory.search')return clone({people:[],communities:[],nextCursor:null});
  if(op==='contacts.list')return clone({contacts:[]});
  if(op==='apps.devices')return clone({devices:[]});
  throw Error('Forbidden/unexpected transport: '+op);
 }};environment.options={api,accountId:viewerAccountId,onMessage(){}};
 const directory=load(resolve(root,'src/world/field-directory.ts')).createFieldDirectory({api,accountId:viewerAccountId,isCurrent:environment.current});environment.directory=directory;
 environment.liveProjection=environment.createLiveAppScene();
 environment.engine={snapshot:()=>clone(canonical),getCamera:()=>({...camera}),hasUnsavedChanges:()=>false,destroy(){},
  update(value){updates.push(clone(value));if(value.document)canonical=clone(value.document);if(value.liveScene)liveScene=clone(value.liveScene);exports.api?.updateSummary(summary());}};
 environment.persistence={dispose(){},getState:()=>({document:clone(canonical)})};environment.summary=summary();
 vm.runInContext(code,context);
 return{api:exports.api,environment,state,calls,updates,camera,get document(){return clone(canonical);},get live(){return clone(liveScene);},setDocument(doc){environment.engine.update({document:clone(doc)});}};
}
const ids=f=>Array.from(f.live.shortcuts,item=>item.entity.id).sort();
const noOwnerReads=f=>assert.equal(f.calls.some(call=>['canvas.objects.get','canvas.widgets.get'].includes(call.op)),false,'coverage uses only the existing accepted batch');

test('public applications appear on the initial canvas for both their owner and another account',async()=>{
 const nfcId='app-'+'d'.repeat(32);
 for(const viewerAccountId of [accountId,'another-account']){
  const nfc={id:nfcId,name:'Метки · NFC',ownerAccountId:accountId,state:'ready',createdAt:1,
   publication:{launchPolicy:'anyone',listed:true},entry:{appId:nfcId,domainId:'public-nfc',origin:'https://nfc.example',path:'/'}};
  const f=harness({hasObject:false,viewerAccountId,onRequest:({op,args})=>{
   if(op==='apps.list')return{apps:viewerAccountId===accountId?[nfc]:[]};
   if(op==='apps.catalog')return{apps:[nfc,nfc]};
   if(op==='apps.directory.resolve')return{items:args.appIds.map(id=>({ref:{kind:'app',id},available:true,
    app:{...nfc,access:viewerAccountId===accountId?'owner':'public'}}))};
  }}),before=JSON.stringify(f.document),camera={...f.camera};
  await f.api.refreshLiveApps();
  assert.deepEqual(ids(f),[nfcId],`${viewerAccountId}: a public application is visible once without a saved shortcut`);
  assert.equal(f.environment.directory.getRecord({kind:'app',id:nfcId})?.entry.origin,'https://nfc.example');
  assert.equal(JSON.stringify(f.document),before,'discovery does not write into the personal canvas');
  assert.deepEqual(f.camera,camera,'new public apps do not move the personal camera');
  assert.equal(f.calls.some(call=>/put|create|remove/u.test(call.op)),false);
 }
});

test('catalog and personal registry failures do not hide the other available source',async()=>{
 const nfcId='app-'+'d'.repeat(32),nfc={id:nfcId,name:'Public NFC',ownerAccountId:'publisher',state:'ready',access:'public',createdAt:1,
  entry:{appId:nfcId,domainId:'public-nfc',origin:'https://nfc.example',path:'/'}};
 for(const failed of ['apps.catalog','apps.list']){
  const f=harness({hasObject:false,onRequest:({op,args})=>{
   if(op===failed)throw Error('source unavailable');
   if(op==='apps.catalog')return{apps:[nfc]};
   if(op==='apps.directory.resolve'&&failed==='apps.list')return{items:args.appIds.map(id=>({ref:{kind:'app',id},available:true,app:nfc}))};
  }}),messages=[];f.environment.options.onMessage=(...message)=>messages.push(message);
  await f.api.refreshLiveApps();
  assert.deepEqual(ids(f),failed==='apps.catalog'?[appA,appB]:[nfcId]);
  assert.equal(f.environment.element.dataset.liveAppsState,'partial');assert.equal(messages.length,1);assert.equal(messages[0][1],true);
 }
});

test('the first available apps frame an empty canvas once and never replace a manual camera',async()=>{
 for(const manual of [false,true]){
  const held=defer(),f=harness({hasObject:false,onRequest:({op})=>op==='apps.catalog'?held.promise:undefined}),focused=[];
  f.setDocument({...f.document,shortcuts:[]});f.environment.engine.focusContext=id=>focused.push(id);
  const pending=f.api.refreshLiveApps();await settle();
  if(manual)f.environment.options.viewState={mineFit:'manual'};
  held.resolve({apps:[]});await pending;
  assert.equal(focused.length,manual?0:1);
  await f.api.refreshLiveApps();assert.equal(focused.length,manual?0:1,'later catalogue updates leave the camera alone');
  assert.equal(f.document.shortcuts.length,0);
 }
});

test('public catalogue is not authority and withdrawal removes its live launchers without changing personal placements',async()=>{
 const nfcId='app-'+'d'.repeat(32),deniedId='app-'+'e'.repeat(32);
 const nfc={id:nfcId,name:'Public NFC',ownerAccountId:'publisher',state:'ready',access:'public',createdAt:1,
  entry:{appId:nfcId,domainId:'public-nfc',origin:'https://nfc.example',path:'/'}};
 let listed=true;
 const f=harness({hasObject:false,onRequest:({op,args})=>{
  if(op==='apps.list')return{apps:[]};
  if(op==='apps.catalog')return{apps:listed?[nfc,{...nfc,id:deniedId,name:'Not admitted'}]:[]};
  if(op==='apps.directory.resolve')return{items:args.appIds.map(id=>id===nfcId?{ref:{kind:'app',id},available:true,app:nfc}:{ref:{kind:'app',id},available:false})};
 }}),before=JSON.stringify(f.document),camera={...f.camera};
 await f.api.refreshLiveApps();assert.deepEqual(ids(f),[nfcId]);assert.equal(f.environment.metadata.has('app:'+deniedId),false);
 assert.equal(f.environment.directory.getRecord({kind:'app',id:deniedId}),null);
 listed=false;await f.api.refreshLiveApps();assert.deepEqual(ids(f),[]);
 assert.equal(JSON.stringify(f.document),before);assert.deepEqual(f.camera,camera);
});

test('late accepted Object projection removes only its automatic launcher after a faster registry read',async()=>{
 const held=defer(),f=harness({onRequest:({op})=>op==='canvas.objects.project'?held.promise:undefined}),before=JSON.stringify(f.document),camera={...f.camera};
 const mine=f.api.loadMine();await f.api.refreshLiveApps();assert.ok(ids(f).includes(appB));assert.equal(f.environment.element.dataset.appCoverage,'loading');
 held.resolve({items:[projection()]});await mine;assert.deepEqual(ids(f),[appB]);assert.equal(f.environment.element.dataset.appCoverage,'ready');
 assert.equal(JSON.stringify(f.document),before);assert.deepEqual(f.camera,camera);assert.equal(f.document.shortcuts.filter(item=>item.entity.id===foreign).length,2);noOwnerReads(f);
});
test('registry arriving after Object projection uses the same representation and retains all searchable apps',async()=>{
 const f=harness();await f.api.loadMine();await f.api.refreshLiveApps();assert.deepEqual(ids(f),[appB]);assert.equal(f.environment.liveApps.length,2);noOwnerReads(f);
});
test('canonical last-ref drop and re-add derive from current accepted metadata without a registry or layout write',async()=>{
 const f=harness();await f.api.loadMine();await f.api.refreshLiveApps();const calls=f.calls.length;
 f.setDocument(document(false));assert.deepEqual(ids(f),[appA,appB]);f.setDocument(document(true));assert.deepEqual(ids(f),[appB]);
 assert.equal(f.calls.length,calls);assert.equal(f.calls.some(call=>/put|create|remove/u.test(call.op)),false);noOwnerReads(f);
});
test('hidden and locked own representation survives without runtime authority, then the next batch removes its association',async()=>{
 for(const hidden of [false,true]){
  const f=harness({project:{objectId,available:!hidden,revision:1,ownerRepresents:[{kind:'app',id:appA}],...(hidden?{hidden:true}:{protection:{kind:'password'}})}});
  await f.api.loadMine();await f.api.refreshLiveApps();assert.deepEqual(ids(f),[appB]);
  if(hidden)assert.equal(f.environment.directory.getRecord({kind:'object',id:objectId}),null,'metadata must not resurrect an unavailable runtime record');
  const before=JSON.stringify(f.document);f.state.project={...f.state.project,ownerRepresents:[]};await f.api.refresh({preserveView:true});
  assert.deepEqual(ids(f),[appA,appB]);assert.equal(JSON.stringify(f.document),before);assert.equal(f.environment.element.dataset.appCoverage,'ready');noOwnerReads(f);
 }
});
test('old backend, unsupported provider and foreign missing field settle to unknown, not fictional pending or absence',async()=>{
 for(const project of [{...projection(),ownerRepresents:undefined},{objectId,available:true,protection:{kind:'password'}},{objectId,available:false}]){
  const f=harness({project});await f.api.loadMine();await f.api.refreshLiveApps();await settle();
  assert.deepEqual(ids(f),[appA,appB]);assert.equal(f.environment.element.dataset.appCoverage,'unknown');
  assert.equal(f.environment.element.dataset.appCoveragePending,'0');assert.equal(f.environment.element.dataset.appCoverageUnknown,'1');noOwnerReads(f);
 }
});
test('projection failure stays unknown with a visible diagnostic error and no fallback request',async()=>{
 const f=harness({onRequest:({op})=>op==='canvas.objects.project'?Promise.reject(Object.assign(Error('offline'),{code:'network_unavailable'})):undefined});
 await f.api.loadMine();await f.api.refreshLiveApps();assert.equal(f.environment.element.dataset.appCoverage,'unknown');assert.equal(f.environment.element.dataset.appCoverageErrors,'1');noOwnerReads(f);
});
test('late accepted batch after canonical removal cannot restore old app coverage',async()=>{
 const held=defer(),f=harness({onRequest:({op})=>op==='canvas.objects.project'?held.promise:undefined});
 const mine=f.api.loadMine();await f.api.refreshLiveApps();f.setDocument(document(false));held.resolve({items:[projection()]});await mine;
 assert.deepEqual(ids(f),[appA,appB]);assert.equal(f.environment.element.dataset.appCoverage,'ready');noOwnerReads(f);
});
test('account retirement and actual Screen disposal reject late batch metadata without future publication',async()=>{
 for(const dispose of [false,true]){
  const held=defer(),f=harness({onRequest:({op})=>op==='canvas.objects.project'?held.promise:undefined});
  const mine=f.api.loadMine();mine.catch(()=>{});await f.api.refreshLiveApps();if(dispose)f.api.dispose();else f.state.current=false;
  const count=f.updates.length;held.resolve({items:[projection()]});await Promise.allSettled([mine]);await settle();assert.equal(f.updates.length,count);noOwnerReads(f);
 }
});
test('paged canonical object reads use the same three existing batch calls and no per-object metadata calls',async()=>{
 const f=harness({project:{objectId,available:false,hidden:true,ownerRepresents:[]}}),doc=document(false);
 for(let index=0;index<83;index++)doc.shortcuts.push({shortcutId:'page-'+index,entity:{kind:'object',id:'object_'+index.toString(16).padStart(32,'0')},contextId:'personal',placement:{kind:'hex',slot:[index,3]}});
 f.setDocument(doc);await f.api.loadMine();await f.api.refreshLiveApps();assert.equal(f.environment.element.dataset.appCoverage,'ready');
 assert.deepEqual(f.calls.filter(call=>call.op==='canvas.objects.project').map(call=>call.args.objectIds.length),[40,20,23]);noOwnerReads(f);
});
test('negative control dropping representation filtering restores the original duplicate',async()=>{
 const from='representedAppIds: coverage.appIds';assert.equal(original.split(from).length,2);
 const f=harness({text:original.replace(from,'representedAppIds: new Set()')});await f.api.loadMine();await f.api.refreshLiveApps();
 assert.throws(()=>assert.deepEqual(ids(f),[appB]),{code:'ERR_ASSERTION'});noOwnerReads(f);
});
