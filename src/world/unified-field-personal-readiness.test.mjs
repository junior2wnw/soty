import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {createFieldPersistence} from './field-persistence.ts';
import {createFieldDirectory} from './field-directory.ts';
import {createPersonalFieldDocument} from './app-field-placement.mjs';
import {validateAnyFieldDocument,convertFieldDocumentToV2,fieldEntityKey} from '../../modules/field/contract.mjs';
import {coverageTestSource,installCoverageTestPorts} from './unified-field-coverage-test-ports.mjs';

// The production lifecycle, empty-state renderer, persistence and directory run
// unchanged. Only paint/DOM and bounded, deliberately held RPC ports are local.
const source=readFileSync(new URL(process.env.SOTY_READINESS_SOURCE??'./unified-field-screen.ts',import.meta.url),'utf8');
const parsed=ts.createSourceFile('screen.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS),printer=ts.createPrinter();
const screen=parsed.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='createUnifiedFieldScreen');
const variable=(nodes,name)=>nodes.find(n=>ts.isVariableStatement(n)&&n.declarationList.declarations.some(d=>d.name.getText(parsed)===name));
const print=n=>{assert.ok(n,'actual production declaration');return printer.printNode(ts.EmitHint.Unspecified,n,parsed);};
const returned=screen.body.statements.find(ts.isReturnStatement).expression;
const methods=['refresh','reconnect','dispose','addObject'].map(name=>returned.properties.find(n=>ts.isMethodDeclaration(n)&&n.name.getText(parsed)===name));
const names=['mountEngine','revealReadyMine','applyPersistence','loadInitialMine','loadMine','loadMinePass','loadOwnedApps','updateMineEmpty','updateSummary','updateMineFilter'];
const subscribe=variable(screen.body.statements,'unobservePersistence');
const initial=screen.body.statements.find(n=>ts.isIfStatement(n)&&n.getText(parsed)==='if (options.persistence) applyPersistence(persistence.getState());');
const compiled=ts.transpileModule([
  print(variable(parsed.statements,'BUILTINS')),print(variable(parsed.statements,'matches')),
  print(variable(screen.body.statements,'initialMineTask')),coverageTestSource(screen.body.statements,parsed,printer),
  ...screen.body.statements.filter(n=>ts.isFunctionDeclaration(n)&&n.name?.text==='hasConfirmedPersonalLayout').map(print),
  ...names.map(name=>print(screen.body.statements.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text===name))),
  print(subscribe),print(initial),print(variable(screen.body.statements,'ready')),
  'exports.api={ready,'+methods.map(print).join(',')+'};',
].join('\n'),{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
const deferred=()=>{let resolve;const promise=new Promise(done=>{resolve=done;});return{promise,resolve};};
const turn=()=>new Promise(done=>setImmediate(done));
async function until(predicate){const end=Date.now()+1500;while(!predicate()){assert.ok(Date.now()<end,'controlled phase must become observable');await turn();}}
class NodePort{
  constructor(tag='div',className='',text=''){Object.assign(this,{tagName:tag.toUpperCase(),className,textContent:text,children:[],hidden:false,attributes:new Map(),handlers:new Map(),dataset:{}});}
  append(...children){this.children.push(...children);}replaceChildren(...children){this.children=children;}
  setAttribute(k,v){this.attributes.set(k,String(v));}getAttribute(k){return this.attributes.get(k)??null;}
  get firstElementChild(){return this.children[0];}querySelector(){return this.children[0]??new NodePort('span');}
  addEventListener(type,fn){this.handlers.set(type,fn);}click(){this.handlers.get('click')?.();}remove(){}
}
function fixture(t,{held='contacts.list',virgin=false,nonempty=false,fieldHeld=false,putHeld=false,cached=false,objectAvailable=true,objectHeld=false}={}){
  const gates=new Map(),gate=key=>{if(!gates.has(key))gates.set(key,deferred());return gates.get(key);};
  for(const key of ['world.field.get','world.field.put','world.directory.search','contacts.list','apps.directory.search','apps.devices','apps.list','catalogue','canvas.objects.project']){
    const value=gate(key);if(key!==held&&!(fieldHeld&&key==='world.field.get')&&!(putHeld&&key==='world.field.put')&&!(objectHeld&&key==='canvas.objects.project'))value.resolve();
  }
  const initial=virgin?{schema:'soty.field.v2',contexts:[],shortcuts:[]}:createPersonalFieldDocument({includeBuiltins:false});
  if(nonempty)initial.shortcuts.push({shortcutId:'existing-note',entity:{kind:'builtin',id:'notes'},contextId:initial.contexts[0].contextId,slot:[0,0]});
  let server={revision:virgin?0:1,document:validateAnyFieldDocument(initial),updatedAt:1},saved=null,generation=1;
  const current=()=>generation===1,calls=[],messages=[],composers=[],fields=[];
  const envelope=()=>({...server,document:structuredClone(server.document),contentHash:createHash('sha256').update(JSON.stringify(server.document)).digest('hex')});
  if(cached)saved={schema:'soty.field.local.v1',scope:JSON.stringify(['local','soty','owner']),localRevision:1,base:envelope(),pending:[],conflict:false,remote:null,conflictDocument:null};
  const api={async request(op,args={}){
    calls.push(op);if(op.startsWith('world.field.'))assert.equal(args.expectedAccountId,'owner');
    if(op==='world.field.get'){const result=envelope();await gate(op).promise;return result;}
    if(op==='world.field.put'){
      assert.equal(args.expectedRevision,server.revision);server={revision:server.revision+1,document:validateAnyFieldDocument(args.document),updatedAt:2};
      const result={replayed:false,current:envelope(),receipt:{revision:server.revision,contentHash:envelope().contentHash,committedAt:2}};
      await gate(op).promise;return result;
    }
    await gate(op).promise;
    if(op==='world.directory.search')return{communities:[],people:[],nextCursor:null};
    if(op==='contacts.list')return{contacts:[]};
    if(op==='apps.directory.search')return{apps:[],nextCursor:null};
    if(op==='apps.devices')return{devices:[]};
    if(op==='apps.list')return{apps:[]};
    if(op==='canvas.objects.project'){assert.equal(args.expectedAccountId,'owner');return{items:args.objectIds.map(objectId=>({objectId,available:objectAvailable,view:{kind:'note',title:'Created note',canEdit:true}}))};}
    assert.fail('unexpected RPC '+op);
  }};
  const persistence=createFieldPersistence({api,accountId:'owner',localFirst:true,isCurrent:current,store:{
    async read(){return structuredClone(saved);},async write(value,expected){assert.equal(saved?.localRevision??0,expected);saved=structuredClone(value);},
  }});
  const node=(tag='div',className='',text='')=>new NodePort(tag,className,text),element=node(),metadata=new Map();element.dataset={ready:'loading',search:'closed',liveAppsState:'loading'};
  const environment={exports:{},crypto,structuredClone,TextEncoder,disposed:false,current,mode:'mine',filter:'all',queries:{mine:'',search:''},
    options:{api,accountId:'owner',persistence,isCurrent:current,onCreateMenu:()=>composers.push('existing-owner-composer'),onMessage(){},onRoute(){}},persistence,
    engine:null,state:null,summary:null,metadata,metadataRevision:0,fieldEntityKey,allMetadata:()=>[...metadata.values()],
    merge(items){for(const item of items)metadata.set(fieldEntityKey(item.entity),item);environment.metadataRevision++;},pruneMetadata(){},
    liveApps:[],liveAppsFramed:false,liveProjection:{update:()=>({contexts:[],shortcuts:[]})},element,loading:{hidden:false},
    async refreshLiveApps(){calls.push('catalogue');await gate('catalogue').promise;if(!current())return;element.dataset.liveAppsState='ready';environment.liveApps=[{entity:{kind:'app',id:'public-app'},title:'Public app',source:'public'}];if(environment.engine)environment.exports.render();},
    fieldGlass:null,mountFieldGlass:()=>({dispose(){}}),canvasContent:{dispose(){},hasUnsavedChanges:()=>false,hasUnconfirmedWork:()=>false},createObject:()=>null,objectKey:()=>null,
    activate(){},inspect(){},publishKnownApps(){},renderOwnApps(){},renderOpenList:null,renderMineSearchResults(){},updateChrome(){},updateStatus(){},updateIdentity(){},
    empty:Object.assign(node('div','uf-empty'),{hidden:true}),emptyDocument:null,emptyScope:'',emptyMetadataRevision:-1,
    mineGeneration:0,mineResolvedGeneration:-1,mineConfirmedGeneration:-1,mineKnownRefs:new Set(),mineBusy:false,queuedMine:null,mineFilterAdmitted:false,available:[],availableCursor:null,
    ownedApps:[],ownedAppsState:'loading',ownAppsReadSequence:0,ownAppsAppliedRead:0,overviewApps:()=>[],overviewEntity:value=>value,
    previewCommunity:null,selected:null,firstResolve:false,wide:{matches:false},closeList(){},closePreview(){},currentEntity:()=>null,
    arrange:node(),arrangeHint:node(),percent:node(),scaleControl:node(),viewPercent:null,undo:node(),contextSelector:node(),contextKey:'',
    window:{innerWidth:390},resolveFieldAppArt:()=>null,host:{prepend(){}},createPersonalFieldDocument,nextFieldSlot:()=>[0,0],layoutUnifiedField:()=>({nodes:[]}),
    filtersByMode:{mine:'all',search:'all'},searchInput:{value:''},
    el:node,icon:()=>node('svg'),button(label,_icon,className,onClick){const value=node('button',className,label);value.type='button';value.addEventListener('click',onClick);return value;},
    searchGeneration:0,searchTimer:null,queuedSearch:null,abort:new AbortController(),dialogs:new Set(),listDialog:null,search:node(),listButton:node(),
    run(promise){if(promise)void promise.catch(environment.report);},report(error){if(current())messages.push(error.code??error.message);},
    createUnifiedField(options){let document=options.document;const field={ready:Promise.resolve(),element:{clientWidth:390,clientHeight:700},snapshot:()=>document,
      update(value){if(value.document)document=value.document;options.onStateChange({document,mode:'mine',focusContextId:document.contexts[0]?.contextId??'',arranging:false,camera:{x:0,y:0,scale:1},canUndo:false});},
      async addFreeShortcut(ref,contextId,placement){const next=convertFieldDocumentToV2(document);next.shortcuts.push({shortcutId:'created-object',entity:structuredClone(ref),contextId,placement:structuredClone(placement)});
        const receipt=await persistence.commit(next,{expectedRevision:persistence.getState().projectedRevision,requestId:'created_object_placement'});field.update({document:receipt.document});return receipt;},
      getCamera:()=>({x:0,y:0,scale:1}),hasUnsavedChanges:()=>false,focusContext(){},fitOverview(){},revealShortcut(){},destroy(){}};fields.push(field);return field;},
  };
  const directory=createFieldDirectory({api,accountId:'owner',isCurrent:current});
  // Synthetic VM-to-native bridge copies all DTO fields; the actual production
  // directory/contract still validates them strictly in its own JS realm.
  environment.directory={...directory,resolve:refs=>directory.resolve(structuredClone(refs))};installCoverageTestPorts(environment);
  vm.runInNewContext(compiled+'\nexports.render=()=>updateMineEmpty(engine.snapshot());',environment);
  const hint=()=>!environment.empty.hidden?environment.empty.children.find(n=>n.getAttribute('aria-label')==='Создать первую записку'):undefined;
  t.after(async()=>{for(const value of gates.values())value.resolve();await environment.exports.api.ready;environment.exports.api.dispose();persistence.dispose();});
  return{environment,persistence,calls,messages,composers,fields,hint,get api(){return environment.exports.api;},release:key=>gate(key).resolve(),
    retire(){generation=2;generation=3;},render(){environment.exports.render();},server:()=>structuredClone(server),
    async place(){const document=structuredClone(environment.engine.snapshot());document.shortcuts.push({shortcutId:'accepted-note',entity:{kind:'builtin',id:'notes'},contextId:document.contexts[0].contextId,slot:[0,0]});await persistence.commit(document,{expectedRevision:persistence.getState().projectedRevision,requestId:'first_note_accepted'});await until(()=>persistence.getState().state==='saved');},
  };
}

test('confirmed empty personal action is usable while each unrelated signed discovery source remains held',async t=>{
  for(const held of ['world.directory.search','contacts.list','apps.directory.search','apps.devices','apps.list','catalogue'])await t.test(held,async t=>{
    const f=fixture(t,{held});await until(()=>f.calls.includes(held)&&f.environment.engine&&f.persistence.getState().state==='saved');f.render();
    assert.ok(f.hint(),'a verified empty own layout must not await '+held);assert.equal(f.environment.element.dataset.ready,'loading','directory readiness is not forged');
    f.hint().click();assert.deepEqual(f.composers,['existing-owner-composer']);
    const before=JSON.stringify(f.environment.engine.snapshot());f.release(held);await f.api.ready;
    assert.equal(JSON.stringify(f.environment.engine.snapshot()),before,'late discovery cannot insert or duplicate saved objects');assert.deepEqual(f.messages,[]);
  });
});

test('withheld field receipt, nonempty layout and retired account epoch cannot show or activate the first-note action',async t=>{
  const held=fixture(t,{fieldHeld:true,cached:true});await until(()=>held.calls.includes('world.field.get')&&held.environment.engine);
  assert.equal(held.persistence.getState().state,'saved');assert.equal(held.persistence.getState().serverConfirmed,false);held.render();assert.equal(held.hint(),undefined,'a visible saved cache with no fresh receipt must remain unconfirmed');
  held.release('world.field.get');await until(()=>held.hint());assert.ok(held.hint());
  const stale=held.hint();held.retire();stale.click();assert.equal(held.composers.length,0);held.render();assert.equal(held.hint(),undefined);
  const nonempty=fixture(t,{nonempty:true});await until(()=>nonempty.environment.engine);await turn();nonempty.render();assert.equal(nonempty.hint(),undefined);
});

test('virgin personal context requires an accepted durable receipt, then its first accepted placement removes the hint',async t=>{
  const f=fixture(t,{virgin:true,putHeld:true});await until(()=>f.calls.includes('world.field.put'));
  f.render();assert.equal(f.hint(),undefined);assert.equal(f.persistence.getState().pendingCount,1);
  f.release('world.field.put');await until(()=>f.persistence.getState().state==='saved');f.render();assert.ok(f.hint());
  await f.place();f.render();assert.equal(f.hint(),undefined);const before=JSON.stringify(f.server());
  f.release('contacts.list');await f.api.ready;assert.equal(JSON.stringify(f.server()),before,'late catalogue and metadata do not write a second note');assert.equal(f.calls.filter(op=>op==='world.field.put').length,2);
});

test('reconnect uses the existing refresh coordination to renew live catalogue and personal discovery',async t=>{
  const f=fixture(t);f.release('contacts.list');await f.api.ready;
  const catalogues=f.calls.filter(op=>op==='catalogue').length,mines=f.calls.filter(op=>op==='contacts.list').length;
  f.environment.element.dataset.liveAppsState='partial';f.api.reconnect();
  await until(()=>f.calls.filter(op=>op==='catalogue').length===catalogues+1&&f.calls.filter(op=>op==='contacts.list').length===mines+1);
  assert.equal(f.environment.element.dataset.liveAppsState,'ready');assert.deepEqual(f.messages,[]);
});

test('the existing addObject path places a freshly admitted note before unrelated discovery completes',async t=>{
  const f=fixture(t);await until(()=>f.hint());const before=f.calls.filter(op=>op==='world.field.put').length;
  const placing=f.api.addObject('object_prepared_note');await until(()=>f.calls.includes('canvas.objects.project'));
  const placement=await placing;await until(()=>f.persistence.getState().state==='saved');
  assert.ok(placement.shortcutId);assert.equal(placement.localDurable,true);assert.equal(f.environment.element.dataset.ready,'loading');
  assert.equal(f.calls.includes('canvas.objects.project'),true,'the exact new object still requires its own fresh admission');
  assert.equal(f.server().document.shortcuts.length,1);assert.equal(f.hint(),undefined);
  assert.equal(f.calls.filter(op=>op==='world.field.put').length,before+1);
  f.release('contacts.list');await f.api.ready;assert.equal(f.server().document.shortcuts.length,1);
  assert.equal(f.calls.filter(op=>op==='world.field.put').length,before+1,'late directory completion cannot place a duplicate');
});

test('early placement cannot bypass exact object admission or publish after account retirement',async t=>{
  const denied=fixture(t,{objectAvailable:false});await until(()=>denied.hint());
  await assert.rejects(denied.api.addObject('object_denied_note'),error=>error.code==='canvas_object_unavailable');
  assert.equal(denied.calls.includes('world.field.put'),false);
  const held=fixture(t,{objectHeld:true});await until(()=>held.hint());const placing=held.api.addObject('object_delayed_note');
  await until(()=>held.calls.includes('canvas.objects.project'));assert.equal(held.calls.includes('world.field.put'),false);
  held.retire();held.release('canvas.objects.project');await assert.rejects(placing,error=>/account/u.test(error.code));
  assert.equal(held.calls.includes('world.field.put'),false);
});
