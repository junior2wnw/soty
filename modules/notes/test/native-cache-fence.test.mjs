import test from 'node:test';
import assert from 'node:assert/strict';
import {createIndexedNoteCacheStorage,createNoteCache,readNoteWithCache} from '../browser/cache.mjs';
import {createNativeDraftStore} from '../browser/native-local.mjs';
import {indexedNativePort} from './indexed-native-port.mjs';

const accountId='native_cache_owner',projectId='soty',origin='https://soty.example',noteId='note_original_123';
const note={noteId,title:'Before protection',body:'Synthetic secret that must not return',preview:'Synthetic secret',items:[],color:'plain',pinned:false,state:'active',revision:7,createdAt:1,updatedAt:1};
function setup(){const port=indexedNativePort(),drafts=createNativeDraftStore(accountId,projectId,{origin,indexedDB:port.indexedDB});
  const cache=createNoteCache(accountId,projectId,{origin,storage:createIndexedNoteCacheStorage(port.indexedDB)});return{port,drafts,cache};}

test('a durable native Source fence blocks old Notes snapshots after reopen and offline fallback',async()=>{
  const{port,drafts,cache}=setup();const token=await cache.capture();assert.equal(await cache.remember(note,token),true);
  await drafts.native.mark({providerId:'soty.notes',ref:noteId,operationId:'convert_cache_123'});
  assert.equal(await cache.get(noteId),null,'Pending native protection must not expose the previous plaintext snapshot');
  const reopened=createNoteCache(accountId,projectId,{origin,storage:createIndexedNoteCacheStorage(port.indexedDB)});
  await assert.rejects(readNoteWithCache({api:{async request(){throw new Error('network_error');}},accountId,noteId,cache:reopened}));
});

test('a newer cache epoch cannot reintroduce an already sealed native Source',async()=>{
  const{drafts,cache}=setup(),gate=await drafts.native.mark({providerId:'soty.notes',ref:noteId,operationId:'convert_cache_456'});
  await drafts.native.finalize({providerId:'soty.notes',ref:noteId,epoch:gate.epoch});
  const newerToken=await cache.capture();assert.equal(await cache.remember(note,newerToken),false,'A fresh global epoch is not permission to cache a sealed Source');assert.deepEqual(await cache.list(),[]);
});
