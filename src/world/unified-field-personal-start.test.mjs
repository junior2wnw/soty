import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {resolve,join} from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import vm from 'node:vm';

const repo=resolve(process.env.SOTY_HINT_REPO??fileURLToPath(new URL('../../',import.meta.url)));
const sourcePath=resolve(process.env.SOTY_HINT_SOURCE??join(repo,'src/world/unified-field-screen.ts'));
const require=createRequire(pathToFileURL(join(repo,'package.json'))),ts=require('typescript');
const source=readFileSync(sourcePath,'utf8'),domSource=readFileSync(join(repo,'src/world/dom.ts'),'utf8');
const parsed=ts.createSourceFile('screen.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS);
const domParsed=ts.createSourceFile('dom.ts',domSource,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS),printer=ts.createPrinter();
const fn=(nodes,name)=>{const node=nodes.find(n=>ts.isFunctionDeclaration(n)&&n.name?.text===name);assert.ok(node,`actual production ${name}`);return node;};
const screen=fn(parsed.statements,'createUnifiedFieldScreen');
const print=(node,file=parsed)=>printer.printNode(ts.EmitHint.Unspecified,node,file);
const matches=parsed.statements.find(node=>ts.isVariableStatement(node)&&node.declarationList.declarations.some(item=>item.name.getText(parsed)==='matches'));
const code=ts.transpileModule([print(fn(domParsed.statements,'el'),domParsed),print(fn(domParsed.statements,'button'),domParsed),print(matches),
  print(fn(screen.body.statements,'updateMineEmpty')),print(fn(screen.body.statements,'updateSummary')),
  'exports.api={updateMineEmpty,updateSummary};'].join('\n').replace(/^export /gmu,''),{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;

function fixture({mode='mine',query='',filter='all',confirmed=true,create=true,savedDocument}={}){
  let active=true,doc=savedDocument??{schema:'soty.field.v2',contexts:[{contextId:'personal',title:'Личное',x:0,y:0}],shortcuts:[]};
  const calls=[],exports={};let document;
  class Node {
    constructor(tag='div',className='',text=''){this.tagName=tag.toUpperCase();this.className=className;this.textContent=text;this.children=[];this.attributes=new Map();this.handlers=new Map();this.hidden=false;this.isConnected=true;}
    append(...nodes){this.children.push(...nodes);}replaceChildren(...nodes){this.children=[...nodes];}
    setAttribute(k,v){this.attributes.set(k,String(v));}getAttribute(k){return this.attributes.get(k)??null;}
    addEventListener(k,v){this.handlers.set(k,v);}querySelector(){return this.children[0]??new Node('span');}
    get firstElementChild(){return this.children[0];}focus(){document.activeElement=this;}click(){this.handlers.get('click')?.();}
  }
  document={activeElement:null,createElement:tag=>new Node(tag)};
  const node=()=>new Node();
  const fieldEntityKey=ref=>ref.kind+':'+ref.id;
  const environment={exports,document,icon:()=>new Node('svg'),mode,queries:{mine:query,search:''},filter,filtersByMode:{mine:filter,search:'all'},
    current:()=>active,engine:{snapshot:()=>doc},options:{...(create?{onCreateMenu:()=>calls.push('existing-owner-composer')}:{}),onMessage(){}},
    empty:Object.assign(new Node('div','uf-empty'),{hidden:true}),emptyDocument:null,emptyScope:'',emptyMetadataRevision:-1,metadataRevision:0,
    mineGeneration:1,mineResolvedGeneration:confirmed?1:0,mineKnownRefs:new Set(),
    metadata:new Map(),liveApps:[{entity:{kind:'app',id:'app-public'},title:'Каталог',source:'public'}],
    element:{dataset:{mineState:confirmed?'ready':'loading',liveAppsState:'ready',search:'closed'}},ownedAppsState:'ready',
    fieldEntityKey,overviewApps:()=>[],overviewEntity:value=>value,renderMineSearchResults(){},
    summary:null,selected:null,arrange:node(),arrangeHint:node(),percent:node(),scaleControl:node(),viewPercent:null,undo:node(),contextSelector:node(),contextKey:'',
    closeList(){},closePreview(){},publishKnownApps(){},refreshAppCoverage(){},updateStatus(){},renderOwnApps(){},searchScene:()=>({contexts:[]}),
    button:undefined,el:undefined,run(){throw Error('unexpected RPC/retry from a creation hint');},loadMine(){throw Error('unexpected source read');},refreshLiveApps(){throw Error('unexpected catalogue read');},
    searchInput:{value:query,focus(){}},inputChanged(){},updateChrome(){},openAdd(){throw Error('creation must use the existing authenticated owner composer');},
  };
  vm.runInNewContext(code,environment);
  const render=()=>environment.exports.api.updateMineEmpty(doc);
  const hint=()=>!environment.empty.hidden?(environment.empty.children.find(n=>n.tagName==='BUTTON'&&n.getAttribute('aria-label')==='Создать первую записку')??null):null;
  return{environment,document,calls,render,hint,get doc(){return doc;},retire(){active=false;},
    place(){doc={...doc,shortcuts:[{shortcutId:'first-note',entity:{kind:'object',id:'object-first'},contextId:'personal',placement:{kind:'free',x:0,y:0,width:340,height:300}}]};
      environment.metadata.set('object:object-first',{entity:{kind:'object',id:'object-first'},title:'Моя записка',source:'owner'});
      environment.exports.api.updateSummary({document:doc,mode:'mine',focusContextId:'personal',arranging:false,camera:{x:0,y:0,scale:1},canUndo:false});},
  };
}

test('a confirmed empty OWN canvas keeps one accessible first-note action alongside a nonempty catalogue',()=>{
  const f=fixture(),before=JSON.stringify(f.doc);f.render();const hint=f.hint();assert.ok(hint,'visible catalogue must not suppress the first personal note');
  assert.equal(hint.type,'button');assert.equal(hint.getAttribute('aria-haspopup'),'dialog');assert.equal(hint.getAttribute('data-glass-role'),'control');
  hint.click();assert.deepEqual(f.calls,['existing-owner-composer'],'one native activation opens the existing note-first composer');
  assert.equal(JSON.stringify(f.doc),before,'the prompt must not insert a fake note or mutate storage');
});

test('an accepted placement document removes the hint through actual updateSummary and stays absent in a new closure from serialized layout',()=>{
  const f=fixture();f.render();assert.ok(f.hint());f.place();assert.equal(f.hint(),null);assert.equal(f.environment.empty.hidden,true);
  const reopened=fixture({savedDocument:JSON.parse(JSON.stringify(f.doc))});reopened.render();assert.equal(reopened.hint(),null,'new closure from the saved nonempty document remains clean');
});

test('Search, absent owner create port, pending layout, filters and open search never get a personal creation hint',()=>{
  for(const args of [{mode:'search'},{create:false},{confirmed:false},{query:'другой'},{filter:'app'}]){const f=fixture(args);f.render();assert.equal(f.hint(),null);assert.equal(f.calls.length,0);}
  const f=fixture();f.environment.element.dataset.search='open';f.render();assert.equal(f.hint(),null);
});

test('retired identity and a stale hint after a new placement cannot launch another account composer',()=>{
  const f=fixture();f.render();const button=f.hint();assert.ok(button);f.retire();button.click();assert.equal(f.calls.length,0);
  const next=fixture();next.render();const stale=next.hint();next.place();stale.click();assert.equal(next.calls.length,0);
});

test('catalogue state arriving after the same empty document invalidates the hint visibility cache',()=>{
  const f=fixture();f.environment.element.dataset.liveAppsState='loading';f.render();assert.equal(f.hint(),null);
  f.environment.element.dataset.liveAppsState='ready';f.render();assert.ok(f.hint());
  f.environment.mode='search';f.render();assert.equal(f.hint(),null);
});

test('public Search owns its no-results/error/offline state after private hint retirement',()=>{
  const f=fixture();f.render();assert.ok(f.hint());f.environment.mode='search';f.render();
  assert.equal(f.environment.empty.hidden,true);assert.equal(f.environment.empty.className,'uf-empty','retiring the private hint must release its layout marker');
  for(const label of ['Пока ничего не найдено','Не удалось обновить поиск','Поиск ждёт подключения']){
    const heading=f.document.createElement('h2');heading.textContent=label;f.environment.empty.replaceChildren(heading);f.environment.empty.hidden=false;
    f.render();assert.equal(f.environment.empty.hidden,false,'a camera/state update must not erase the Search diagnostic');
    assert.equal(f.environment.empty.children[0],heading);assert.equal(f.environment.empty.className,'uf-empty');assert.equal(f.hint(),null);
  }
});

test('arrange, filtered/no-match and private source errors retain their existing empty-state owner',()=>{
  const arranging=fixture();arranging.render();assert.ok(arranging.hint());arranging.environment.summary={arranging:true};arranging.render();assert.equal(arranging.hint(),null);
  for(const [args,title]of [[{filter:'person'},'Здесь пока пусто'],[{query:'несуществующее'},'Ничего не найдено']]){
    const f=fixture(args);f.render();assert.equal(f.hint(),null);assert.equal(f.environment.empty.hidden,false);assert.equal(f.environment.empty.children[0].textContent,title);
  }
  for(const failure of ['registry','layout','offline']){
    const f=fixture();f.environment.liveApps=[];
    if(failure==='registry')f.environment.ownedAppsState='offline';
    if(failure==='layout')f.environment.element.dataset.mineState='error';
    if(failure==='offline')f.environment.element.dataset.liveAppsState='offline';
    f.render();assert.equal(f.hint(),null);assert.equal(f.environment.empty.hidden,false);assert.equal(f.environment.empty.children[0].textContent,'Доступ пока не подтверждён');
  }
});
