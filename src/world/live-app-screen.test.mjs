import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
import ts from 'typescript';
import {createFieldDirectory} from './field-directory.ts';
import {createFieldSearchPlacement} from './unified-field-search.mjs';
import {createPersonalFieldDocument} from './app-field-placement.mjs';
import {fieldEntityKey} from '../../modules/field/contract.mjs';

const source=readFileSync(new URL('./unified-field-screen.ts',import.meta.url),'utf8');
const parsed=ts.createSourceFile('screen.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS);
const screen=parsed.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='createUnifiedFieldScreen');
// Availability belongs to World; Mine ownership is independently required by
// unified-field-mine-ownership.test.mjs. Keep the original real-tile oracle here.
const refresh=screen.body.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='searchDirectory');
assert.ok(refresh,'production must project discovery into the World canvas, beyond refreshing metadata');
const code=ts.transpileModule(ts.createPrinter().printNode(ts.EmitHint.Unspecified,refresh,parsed)+';exports.refresh=()=>searchDirectory(false,true);',
  {compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
const record=(id,name=id)=>({id,name,state:'ready',ownerAccountId:'owner',access:'public',canManage:false,createdAt:1,updatedAt:1,
  entry:{appId:id,domainId:'dom-test',origin:'https://fixture.invalid',path:'/'}});
function harness(read){
  const calls=[],updates=[],personal=createPersonalFieldDocument(),before=JSON.stringify(personal),metadata=new Map();let current=true;
  const directory=createFieldDirectory({accountId:'viewer',isCurrent:()=>current,api:{async request(op,args){calls.push({op,args});return read(op,args);}}});
  const port={exports:{},current:()=>current,directory,engine:{snapshot:()=>personal,update:value=>updates.push(value),setCamera(){assert.fail('live refresh moves the camera');}},
    mode:'search',queries:{search:''},filter:'app',searchGeneration:0,searchResponseScope:'',searchItems:[],searchCursor:null,selected:null,summary:null,BUILTINS:[],matches:()=>false,
    closeList(){},closePreview(){},pruneMetadata(){},searchSlot:async()=>true,releaseSearchSlot(){},searchLimit:{hidden:true},more:{hidden:true,disabled:false},empty:{hidden:false,replaceChildren(){}},
    el:()=>({}),button:()=>({}),
    options:{onMessage(){}},wide:{matches:true},searchPlacement:createFieldSearchPlacement(),merge:items=>items.forEach(item=>metadata.set(fieldEntityKey(item.entity),item)),
    allMetadata:()=>[...metadata.values()],fieldEntityKey,element:{dataset:{}}};
  vm.createContext(port);new vm.Script(code).runInContext(port);
  return {port,calls,updates,personal,before,refresh:port.exports.refresh,retire:()=>{current=false;directory.dispose();}};
}

test('actual World screen reads the authorized public directory and projects a new app without personal layout writes',async()=>{
  let rows=[record('a','Первое')];const h=harness(async()=>({apps:rows,nextCursor:null}));
  await h.refresh();const first=h.updates.at(-1).searchDocument.shortcuts[0];
  rows=[record('b','Новое приложение'),...rows];await h.refresh();
  assert.equal(h.calls[0].op,'apps.directory.search');assert.equal(h.calls[0].args.scope,'all');assert.equal(h.calls[0].args.expectedAccountId,'viewer');
  assert.equal(h.updates.at(-1).searchDocument.shortcuts.length,2);assert.deepEqual(h.updates.at(-1).searchDocument.shortcuts.find(s=>s.entity.id==='a').slot,first.slot);
  assert.equal(JSON.stringify(h.personal),h.before);assert.equal(h.updates.some(value=>value.document!==undefined),false);h.retire();
});
test('revocation removes the app on a fresh read, without deleting the personal field',async()=>{
  let rows=[record('private')];const h=harness(async()=>({apps:rows,nextCursor:null}));await h.refresh();
  rows=[];await h.refresh();assert.equal(h.updates.at(-1).searchDocument.shortcuts.length,0);assert.equal(JSON.stringify(h.personal),h.before);h.retire();
});
test('an older in-flight directory response cannot overwrite the newer app set',async()=>{
  let finish;const h=harness(async()=>h.calls.length===1?new Promise(resolve=>{finish=resolve;}):{apps:[record('new')],nextCursor:null});
  const old=h.refresh();await new Promise(resolve=>setImmediate(resolve));await h.refresh();finish({apps:[record('old')],nextCursor:null});await old;
  assert.deepEqual(h.updates.at(-1).searchDocument.shortcuts.map(s=>s.entity.id),['new']);h.retire();
});
test('negative control rejects refreshing only the directory metadata while leaving the canvas unchanged',async()=>{
  const requireTile=updates=>assert.ok(updates.some(value=>value.searchDocument?.shortcuts.some(s=>s.entity.id==='new')),'new registry app must appear in the canvas projection');
  assert.throws(()=>requireTile([{entities:[record('new')]}]),/must appear/);
  const h=harness(async()=>({apps:[record('new')],nextCursor:null}));await h.refresh();requireTile(h.updates);h.retire();
});
