import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {createNativeDraftStore} from '../../modules/notes/browser/native-local.mjs';
import {createNoteCache,createIndexedNoteCacheStorage} from '../../modules/notes/browser/cache.mjs';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
const source=readFileSync(new URL('./notes.ts',import.meta.url),'utf8');
const parsed=ts.createSourceFile('notes.ts',source,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS),mount=parsed.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='mountNotes'),printer=ts.createPrinter();
const compile=name=>{const node=mount.body.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text===name);assert.ok(node);return ts.transpileModule(printer.printNode(ts.EmitHint.Unspecified,node,parsed),{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText;};
const accountId='catalog_owner',projectId='catalog_test',origin='https://catalog.example',noteId='catalog_note_123';
const draft={note:{noteId,title:'Private captured title',body:'Private captured draft body',preview:'Private preview',items:[],state:'active',color:'plain',pinned:false,revision:1,createdAt:1,updatedAt:1},branchId:'catalog_branch_123',generation:1,committedGeneration:0,pending:null,conflict:false,savedAt:1};
function ports(){const port=indexedNativePort(),store=createNativeDraftStore(accountId,projectId,{origin,indexedDB:port.indexedDB}),cache=createNoteCache(accountId,projectId,{origin,storage:createIndexedNoteCacheStorage(port.indexedDB)});return{port,store,cache};}

test('actual legacy catalog watches remove an unopened Source preview/draft without changing the other open note',async()=>{
  const f=ports();await f.store.put(draft);let rendered=0,changed=0;
  const env=vm.createContext({store:f.store,sourceWatches:new Map(),quarantinedSources:new Set(),rows:[draft.note,{...draft.note,noteId:'catalog_other_123',title:'Other public thought'}],drafts:[draft],session:{state:()=>({note:{noteId:'catalog_other_123'}})},openedSnapshot:undefined,disposed:false,renderRows(){rendered++;},renderDrafts(){rendered++;},updateStatus(){changed++;},loadList(){},loadDrafts(){}});
  try{vm.runInContext(compile('watchLocalSources'),env);env.watchLocalSources();await f.store.native.mark({providerId:'soty.notes',ref:noteId,operationId:'catalog_convert_123'});
    assert.equal(env.rows.length,1);assert.equal(env.rows[0].noteId,'catalog_other_123');assert.deepEqual(env.drafts,[]);assert.equal(changed,0,'Another open note is not cleared');assert.ok(rendered>=2);
    assert.equal((await f.store.native.inspect(noteId))[0].note.body,draft.note.body,'Only the visible catalog is scrubbed; unsaved private storage is preserved');
  }finally{for(const dispose of env.sourceWatches.values())dispose();}
});

async function capturedDraftScenario(code=compile('openDraft')){
  const f=ports();await f.store.put(draft);await f.store.native.mark({providerId:'soty.notes',ref:noteId,operationId:'captured_convert_123'});let installed=0;
  const env=vm.createContext({openRequest:0,disposed:false,cache:f.cache,releaseSession:async()=>{},installSession(){installed++;},root:{classList:{add(){}}},message:{textContent:''},notesErrorText:()=> 'Перенос ещё не завершён'});
  vm.runInContext(code,env);await env.openDraft(draft);assert.equal(installed,0,'A retained old draft button cannot bypass the durable Source privacy fence');assert.equal(env.message.textContent,'Перенос ещё не завершён');
}
test('a captured legacy draft click rereads the actual privacy fence before installing any text',()=>capturedDraftScenario());
test('negative control: skipping the final draft fence exposes the captured draft and fails the same install oracle',async()=>{
  const code=compile('openDraft'),from="if (cache.canRead && !await cache.canRead(draft.note.noteId))\n            throw new Error('notes_native_protection_pending');";assert.ok(code.includes(from));
  await assert.rejects(capturedDraftScenario(code.replace(from,'')),/A retained old draft button cannot bypass/);
});
