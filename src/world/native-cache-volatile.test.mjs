import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createNoteSession} from '../../modules/notes/browser/state.mjs';
import {createNativeDraftStore,hasRetainedNativeDrafts,flushRetainedNativeDrafts} from '../../modules/notes/browser/native-local.mjs';
import {indexedNativePort} from '../../modules/notes/test/indexed-native-port.mjs';
import {createNativeCacheTransition} from './native-cache-transition.mjs';
const turn=()=>new Promise(resolve=>setImmediate(resolve));

async function scenario(t,create=createNativeCacheTransition){
  const accountId='volatile_cache_owner',projectId='volatile_cache',noteId='volatile_note_123',objectId='volatile_object_123',port=indexedNativePort(),base=createNativeDraftStore(accountId,projectId,{origin:'https://volatile.example',indexedDB:port.indexedDB});let fail=true,writes=0;
  const store={...base,async put(value){if(fail)throw Error('notes_local_quota');return base.put(value);}};
  const note={noteId,title:'',body:'Source',preview:'',items:[],state:'active',color:'plain',pinned:false,revision:1,createdAt:1,updatedAt:1};
  const session=createNoteSession({api:{async request(){writes++;throw Error('No legacy network writes allowed');}},accountId,store,note,debounceMs:60000});
  const flow=create({api:{async request(){throw Error('No Source operation during local retention');}},accountId,projectId,isCurrent:()=>true,store,cache:{},vault:{},inspectCreation:async()=>[]});
  flow.remember(objectId,{providerId:'soty.notes',ref:noteId,ownerId:accountId});t.after(async()=>{fail=false;await session.flushLocal().catch(()=>{});session.dispose();flow.dispose();});
  session.edit({body:'Only RAM holds this unsaved private thought'});await assert.rejects(session.flushLocal(),/notes_local_quota/);session.dispose();await turn();
  assert.equal(base.native.sessions(noteId).length,1,'Failed disposal must retain its private recovery port');
  assert.equal(flow.hasUnsavedChanges(),true,'A removed editor must still report volatile unsaved work to the shell');
  await assert.rejects(flow.flush(),/notes_local_quota/);assert.equal(flow.hasUnsavedChanges(),true);
  fail=false;await flow.flush();assert.equal(flow.hasUnsavedChanges(),false);assert.equal((await base.native.inspect(noteId))[0].note.body,'Only RAM holds this unsaved private thought');assert.equal(writes,0);
}
test('detached editor RAM survives a failed local COMMIT and blocks silent closing until local recovery is durable',scenario);
test('negative control: reporting only attached editors fails the volatile-retention close oracle',async t=>{
  const url=new URL('./native-cache-transition.mjs',import.meta.url),source=readFileSync(url,'utf8'),needle='hasUnsavedChanges:()=>retainedPorts().some(port=>port.hasUnsavedChanges?.()===true)';assert.equal(source.split(needle).length,2);
  const broken=source.replace(needle,'hasUnsavedChanges:()=>false'),absolute=broken.replace(/from '(\.\.?\/[^']+)'/gu,(_match,path)=>`from '${new URL(path,url).href}'`),module=await import(`data:text/javascript;base64,${Buffer.from(absolute).toString('base64')}`);
  await assert.rejects(scenario(t,module.createNativeCacheTransition),/A removed editor must still report volatile unsaved work/);
});

test('a disposed account A cannot lose its volatile guard or expose its body to account B',async()=>{
  const port=indexedNativePort(),origin='https://volatile-switch.example',accountId='volatile_old_owner',projectId='volatile_switch',noteId='volatile_old_note_123',base=createNativeDraftStore(accountId,projectId,{origin,indexedDB:port.indexedDB});let fail=true,network=0;
  const store={...base,async put(value){if(fail)throw Error('notes_local_unavailable');return base.put(value);}},note={noteId,title:'A private title',body:'A Source',preview:'',items:[],state:'active',color:'plain',pinned:false,revision:1,createdAt:1,updatedAt:1};
  const session=createNoteSession({api:{async request(){network++;}},accountId,store,note,debounceMs:60000});
  try{
    session.edit({body:'A only private RAM, never visible to B'});await assert.rejects(session.flushLocal());session.dispose();await turn();
    const other=createNativeDraftStore('volatile_new_owner',projectId,{origin,indexedDB:port.indexedDB});assert.deepEqual(await other.list(),[]);assert.deepEqual(other.native.sessions(noteId),[]);
    assert.equal(hasRetainedNativeDrafts(),true,'Global close guard survives old owner view/helper disposal');await assert.rejects(flushRetainedNativeDrafts());assert.equal(hasRetainedNativeDrafts(),true);
    fail=false;await flushRetainedNativeDrafts();assert.equal(hasRetainedNativeDrafts(),false);assert.equal((await base.native.inspect(noteId))[0].note.body,'A only private RAM, never visible to B');assert.deepEqual(await other.list(),[]);assert.equal(network,0);
  }finally{fail=false;await session.flushLocal().catch(()=>{});session.dispose();}
});
