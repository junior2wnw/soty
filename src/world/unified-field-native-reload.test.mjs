import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {dirname,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import ts from 'typescript';
import {coverageTestSource,installCoverageTestPorts} from './unified-field-coverage-test-ports.mjs';

// Execute the current production screen/directory and the engine's actual
// command, save, filtering and camera paths. DOM painting and RPC are ports;
// reopening below means new closures from serialized field JSON, not browser QA.
const repo=fileURLToPath(new URL('../../',import.meta.url));
const screenPath=resolve(repo,'src/world/unified-field-screen.ts');
const screenText=readFileSync(screenPath,'utf8');
const enginePath=resolve(repo,'src/world/unified-field.ts');
const parse=(path,text=readFileSync(path,'utf8'))=>ts.createSourceFile(path,text,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS);
const printer=ts.createPrinter();
const named=(nodes,name)=>{const result=nodes.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text===name);assert.ok(result,`Production ${name} exists`);return result;};
const compile=source=>ts.transpileModule(source,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
const engineParsed=parse(enginePath),production=named(engineParsed.statements,'createUnifiedField');
const statements=[...production.body.statements],print=node=>printer.printNode(ts.EmitHint.Unspecified,node,engineParsed);
const declaration=name=>statements.find(node=>ts.isVariableStatement(node)&&node.declarationList.declarations.some(item=>ts.isIdentifier(item.name)&&item.name.text===name));
const variables=statements.slice(statements.indexOf(declaration('viewState')),statements.indexOf(named(statements,'summary'))).filter(ts.isVariableStatement);
const engineResult=statements.find(ts.isReturnStatement).expression;
const property=name=>{const result=engineResult.properties.find(node=>node.name?.getText(engineParsed)===name);assert.ok(result,`Production engine.${name} exists`);return result;};
const engineCode=compile([
  print(engineParsed.statements.find(node=>ts.isVariableStatement(node)&&node.declarationList.declarations.some(item=>item.name.getText(engineParsed)==='same'))),
  print(named(engineParsed.statements,'thinEntities')),
  'function engineHarness(options) {',print(declaration('accountId')),...variables.map(print),
  ...['summary','emit','announce','remember','setCamera','fitOverview','focusContext','revealShortcut','rebuild','matchesMineFilter','available','activeNode','cancel','finishGesture','resetPointers','save','persist','execute'].map(name=>print(named(statements,name))),
  `const api={${['update','addContext','addFreeShortcut'].map(name=>print(property(name))).join(',')}};`,
  'rebuild();return {...api,element:viewport,state:summary,snapshot:()=>structuredClone(fieldDocument),getLayout:()=>layout,available,getCamera:()=>({...camera()}),setCamera,focusContext,revealShortcut,fitOverview};}',
  'exports.engineHarness=engineHarness;',
].join('\n'));

function screenCode(text){
  const parsed=parse(screenPath,text),production=named(parsed.statements,'createUnifiedFieldScreen');
  const print=node=>printer.printNode(ts.EmitHint.Unspecified,node,parsed);
  const returned=production.body.statements.find(ts.isReturnStatement).expression;
  const add=returned.properties.find(node=>node.name?.getText(parsed)==='addObject');assert.ok(add,'Production screen.addObject exists');
  return compile(coverageTestSource(production.body.statements,parsed,printer)+'\n'+['merge','pruneMetadata','allMetadata','currentEntity','updateMineFilter','updateMineEmpty','loadMine','loadMinePass','loadOwnedApps'].map(name=>print(named(production.body.statements,name))).join('\n')
    +`\nObject.assign(exports,{loadMine,updateMineFilter,allMetadata,${print(add)}});`);
}

const objectId='object_'+ 'a'.repeat(32),ref={kind:'object',id:objectId};
function harness({serialized,text=screenText,hidden=false}={}){
  const calls=[],state={persisted:serialized??JSON.stringify({schema:'soty.field.v1',contexts:[{contextId:'personal',title:'Личное',x:0,y:0}],shortcuts:[]}),revision:serialized?1:0,hidden};
  const environment={exports:{},TextEncoder,TextDecoder,crypto,performance,clearTimeout,setTimeout,Buffer,URL,
    document:{activeElement:null},HTMLElement:class{},viewport:{clientWidth:1280,clientHeight:800,hasPointerCapture:()=>false},
    plane:{contains:()=>false},live:{},render(){},schedule(){},flushPointerSample(){},errorText:error=>String(error),
    mode:'mine',queries:{mine:'old search that must clear',search:''},filter:'person',filtersByMode:{mine:'person',search:'all'},
    mineGeneration:0,mineResolvedGeneration:-1,mineConfirmedGeneration:-1,mineBusy:false,queuedMine:null,mineKnownRefs:new Set(),mineFilterAdmitted:false,
    metadata:new Map(),metadataRevision:0,emptyDocument:null,emptyScope:'',emptyMetadataRevision:-1,available:[],availableCursor:null,
    liveApps:[],BUILTINS:[],searchItems:[],summary:null,state:null,selected:null,firstResolve:false,wide:{matches:true},
    element:{dataset:{}},loading:{hidden:false},empty:{hidden:true,replaceChildren(){}},searchInput:{value:'old search that must clear'},
    current:()=>true,ready:Promise.resolve(),renderMineSearchResults(){},publishKnownApps(){},renderOwnApps(){},renderOpenList:null,
    ownedApps:[],ownedAppsState:'ready',ownAppsReadSequence:0,ownAppsAppliedRead:0,overviewApps:()=>[],closeList(){},closePreview(){},
    updateChrome(){},changeMode(){throw Error('The fixture starts in Mine');},
    matches:(item,query,kind)=>(kind==='all'||kind===item.entity.kind)&&item.title.toLowerCase().includes(query.trim().toLowerCase()),
    el:()=>({}),button:()=>({}),
  };
  const context=vm.createContext(environment);
  // All DTO validation and generated commands share the ordinary browser realm.
  const clone=vm.runInContext('(value)=>JSON.parse(JSON.stringify(value))',context);environment.structuredClone=clone;
  const modules=new Map(),require=createRequire(import.meta.url);
  function module(code,path){
    const exports={};const localRequire=name=>{
      if(!name.startsWith('.'))return require(name);
      return load(resolve(dirname(path),name));
    };
    vm.compileFunction(code,['exports','require'],{parsingContext:context,filename:path})(exports,localRequire);return exports;
  }
  function load(path){if(!modules.has(path))modules.set(path,module(compile(readFileSync(path,'utf8')),path));return modules.get(path);}
  const contract=load(resolve(repo,'modules/field/contract.mjs'));
  Object.assign(environment,contract,load(resolve(repo,'src/world/unified-field-camera.mjs')),load(resolve(repo,'src/world/unified-field-layout.mjs')),
    load(resolve(repo,'src/world/unified-field-state.mjs')),load(resolve(repo,'src/world/unified-field-live.mjs')),load(resolve(repo,'src/world/unified-field-presentation.mjs')),
    {validateFieldDocument:contract.validateAnyFieldDocument,validateFieldEntity:contract.validateAnyFieldEntity});
  const projection=()=>state.hidden?clone({objectId,available:false,hidden:true,ownerRepresents:[]}):clone({objectId,available:true,revision:3,ownerRepresents:[],presentation:{mask:{shape:'rect'},fontSize:16},protection:{kind:'none'},
    view:{schema:'soty.canvas-native-view.v1',providerId:'soty.notes',kind:'note',title:'Настоящая заметка',canEdit:true,note:{noteId:'note_1',title:'Настоящая заметка',revision:1,updatedAt:1}}});
  const api={async request(op,args={}){
    calls.push({op,args:structuredClone(args)});
    if(op==='canvas.objects.project'){assert.equal(args.expectedAccountId,'owner');assert.deepEqual([...args.objectIds],[objectId]);return{items:[projection()]};}
    if(op==='apps.list')return{apps:[]};
    if(op==='apps.devices')return{devices:[]};
    if(op==='contacts.list')return{contacts:[]};
    if(op==='apps.directory.search')return{apps:[],nextCursor:null};
    if(op==='world.directory.search')return{people:[],communities:[],nextCursor:null};
    throw Error(`Unexpected RPC ${op}`);
  }};
  environment.options={api,accountId:'owner',onMessage(){},onRoute(mode,query,kind){state.route={mode,query,kind};}};
  const directory=load(resolve(repo,'src/world/field-directory.ts')).createFieldDirectory({api,accountId:'owner',isCurrent:()=>true});environment.directory=directory;
  const field=module(engineCode,enginePath).engineHarness({accountId:'owner',document:vm.runInContext(`JSON.parse(${JSON.stringify(state.persisted)})`,context),
    entities:[],revision:state.revision,dimNonMatches:true,viewState:{mine:{x:200,y:-30,scale:.8},mineFit:'manual'},
    onStateChange:value=>{environment.summary=value;},
    async commit(document,options){assert.equal(options.expectedRevision,state.revision);state.persisted=JSON.stringify(document);state.revision++;
      return{status:'saved',revision:state.revision,document:clone(JSON.parse(state.persisted))};},
  });
  environment.engine=field;environment.persistence={getState:()=>({state:'saved',localDurable:true})};
  environment.summary=field.state();
  environment.liveProjection={update:()=>({contexts:[],shortcuts:[]})};
  installCoverageTestPorts(environment);const screen=module(screenCode(text),screenPath);
  return{screen,field,environment,directory,calls,state,clone};
}

function assertNativeUsable(h,shortcutId,phase){
  assert.equal(h.directory.getRecord(ref)?.view?.note?.noteId,'note_1',`${phase}: source is freshly resolved rather than unavailable fallback`);
  const node=h.field.getLayout().nodes.find(node=>node.shortcutId===shortcutId);
  assert.ok(node,`${phase}: canonical placement has a layout node`);
  assert.equal(node.entity.title,'Настоящая заметка',`${phase}: source is freshly resolved rather than unavailable fallback`);
  assert.ok(h.field.available(shortcutId),`${phase}: the actual engine interaction filter admits the native object`);
}

async function createAndReopen(text=screenText){
  const h=harness({text});await h.screen.loadMine();
  const created=await h.screen.addObject(objectId,{width:340,height:300});
  assert.equal(created.status,'saved');assert.equal(created.localDurable,true);
  assert.equal(h.environment.queries.mine,'','creation clears an unrelated query');assert.equal(h.environment.filter,'all');
  assertNativeUsable(h,created.shortcutId,'after creation');
  const durable=JSON.parse(h.state.persisted);assert.equal(durable.schema,'soty.field.v2');assert.equal(durable.shortcuts.length,1);
  assert.deepEqual(durable.shortcuts[0].entity,ref);
  assert.deepEqual(Object.keys(durable.shortcuts[0]).sort(),['contextId','entity','placement','shortcutId']);
  assert.equal(durable.shortcuts[0].placement.width,340);assert.equal(durable.shortcuts[0].placement.height,300);
  const count=h.calls.filter(call=>call.op==='canvas.objects.project').length;
  const replay=await h.screen.addObject(objectId,{width:999,height:999});assert.equal(replay.shortcutId,created.shortcutId);
  assert.equal(JSON.parse(h.state.persisted).shortcuts.length,1,'same object retry never duplicates placement');
  assert.ok(h.calls.filter(call=>call.op==='canvas.objects.project').length>count,'retry still checks fresh object access');
  const reopened=harness({serialized:h.state.persisted,text});
  reopened.environment.queries.mine='';reopened.environment.filter='all';
  assert.equal(reopened.directory.getRecord(ref),null,'reopening starts with no cached authority');
  await reopened.screen.loadMine();
  assertNativeUsable(reopened,created.shortcutId,'after fresh screen load');
  assert.equal(reopened.environment.element.dataset.mineState,'ready');
  assert.ok(reopened.calls.some(call=>call.op==='canvas.objects.project'),'loadMinePass freshly projects object refs');
  assert.equal(reopened.state.persisted,h.state.persisted,'reading metadata never rewrites canonical geometry');
  return{created,h,reopened};
}

test('native creation, all-items interaction and a fresh screen reload keep a real object available',async()=>{
  await createAndReopen();
});

test('the same confirmed source enters the genuine engine once its new ref is present in the document',async()=>{
  const h=harness();await h.screen.loadMine();const created=await h.screen.addObject(objectId);
  assert.equal(h.environment.metadata.get('object:'+objectId)?.title,'Настоящая заметка');
  h.field.update({entities:h.screen.allMetadata()});
  assertNativeUsable(h,created.shortcutId,'confirmed source control');
});

test('hidden metadata reaches the actual engine on live refresh and cold reopen, without changing placement or camera',async()=>{
  const{created,h}=await createAndReopen(),serialized=h.state.persisted,camera=JSON.stringify(h.field.getCamera());
  h.state.hidden=true;await h.screen.loadMine();
  assert.equal(h.directory.getRecord(ref),null,'hidden Source records are retired');
  assert.equal(h.field.getLayout().nodes.length,0,'the hidden placement must have no layout node');
  assert.equal(h.field.available(created.shortcutId),undefined,'the hidden placement cannot be activated');
  assert.equal(h.state.persisted,serialized,'hiding does not remove the canonical placement');
  assert.equal(JSON.stringify(h.field.getCamera()),camera,'hiding does not move the camera');
  const reopened=harness({serialized,hidden:true});reopened.environment.queries.mine='';reopened.environment.filter='all';
  await reopened.screen.loadMine();assert.equal(reopened.field.getLayout().nodes.length,0,'reopening does not bring back a hidden placement');
  h.state.hidden=false;await h.screen.loadMine();assertNativeUsable(h,created.shortcutId,'after restoring visibility');
  assert.equal(h.state.persisted,serialized);assert.equal(JSON.stringify(h.field.getCamera()),camera);
});

function mutated(from,to){assert.equal(screenText.split(from).length,2,`Control has one exact production target: ${from}`);return screenText.replace(from,to);}
test('negative control: omitting object from all-items kinds is detected by actual engine availability',async()=>{
  const text=mutated("['app', 'builtin', 'person', 'community', 'device','object']","['app', 'builtin', 'person', 'community', 'device']");
  await assert.rejects(createAndReopen(text),/after creation: the actual engine interaction filter admits/);
});
test('negative control: omitting object source group is detected only after reopening without cached authority',async()=>{
  const text=mutated("refs.filter(ref => ref.kind === 'object'),",'');
  await assert.rejects(createAndReopen(text),/after fresh screen load: source is freshly resolved/);
});
test('negative control: metadata admitted only before placement is detected without a compensating live refresh',async()=>{
  const text=mutated('field.update({entities:allMetadata(),selectedShortcutId:existing.shortcutId});','field.update({selectedShortcutId:existing.shortcutId});');
  await assert.rejects(createAndReopen(text),/after creation: canonical placement has a layout node/);
});
