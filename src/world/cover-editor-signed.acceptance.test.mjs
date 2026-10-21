import test from 'node:test';
import assert from 'node:assert/strict';
import { resolveObjectURL } from 'node:buffer';
import { DatabaseSync } from 'node:sqlite';
import sharp from 'sharp';
import { signedAppsCoverFixture, raster } from './test-support/cover-signed-apps.mjs';
import { signedFixture, good } from '../../modules/world/test/support/world6-signed-fixture.mjs';
import { pngFixture } from '../../modules/world/test/avatar-fixtures.mjs';
import { createCoverApi } from './cover-api.mjs';
import { createCoverEditorState, createCoverJournal } from './cover-editor-state.mjs';
import { createAppCoverPresenter } from './app-cover-presenter.mjs';
const tick=()=>new Promise(r=>setImmediate(r));
async function until(predicate) { for(let n=0;n<1000;n++){if(predicate())return;await tick();}assert.fail('Actual presenter did not settle'); }
const storage=()=>{const map=new Map();return {getItem:k=>map.get(k)??null,setItem:(k,v)=>map.set(k,v)};};
const upload=base64=>async()=>({url:'blob:local-preview-only',width:512,height:512,mime:'image/png',byteCount:Buffer.from(base64,'base64').length,base64:()=>base64,dispose(){}});

test('actual signed Apps → SDK → editor commit → private presenter → service restart, restore and live ACL read',async t=>{
  const f=await signedAppsCoverFixture(t);f.enable();const disk=storage(),owner=f.accounts.owner.accountId,stranger=f.accounts.stranger.accountId;
  let generation=1,loseCommit=true;const observed=[],wire=[];
  const api=name=>({async request(op,args){wire.push({op,args:structuredClone(args)});const result=await f.raw(name,op,args);if(!result.ok)throw Object.assign(Error(result.error.code),{code:result.error.code});
    if(op==='apps.cover.commit'&&loseCommit){loseCommit=false;throw Object.assign(Error('lost'),{code:'network_lost'});}return result;}});
  const ownerApi=api('owner'),sdk=()=>createCoverApi({api:ownerApi,kind:'app',targetId:f.id,accountId:owner,isCurrent:()=>generation===1});
  const presenter=createAppCoverPresenter({accountId:owner,isCurrent:()=>generation===1,readApp:(_,width)=>sdk().call('read',{width})});
  const binding=presenter.watch({identity:'source-one',appId:f.id,isCurrent:()=>generation===1,onVisual:value=>observed.push(value)});binding.setVisible(true);await until(()=>observed.at(-1)?.mode==='inherit');
  const originalRevision=f.read(db=>db.prepare('SELECT revision FROM local_apps WHERE id=?').get(f.id).revision),input=await raster('#397d88');
  const editor=createCoverEditorState({sdk:sdk(),prepareFile:upload(input),journal:createCoverJournal(disk),isCurrent:()=>generation===1,onCommitted:()=>presenter.invalidate(f.id)});
  await editor.load();await editor.choose({});editor.setCrop({x:.2,y:.8,zoom:2});await editor.save();assert.equal(editor.read().phase,'unknown');
  const sent=wire.filter(v=>v.op==='apps.cover.commit');assert.equal(sent.length,1);await editor.recheck();assert.equal(editor.read().pending,null);
  assert.deepEqual(wire.filter(v=>v.op==='apps.cover.commit').map(v=>v.args),[sent[0].args,sent[0].args]);
  await until(()=>observed.at(-1)?.mode==='custom');const firstUrl=observed.at(-1).url,blob=resolveObjectURL(firstUrl);assert.ok(blob);
  assert.equal((await sharp(Buffer.from(await blob.arrayBuffer())).metadata()).format,'webp');
  const savedImage=(await sdk().call('read',{width:320})).image.bytes,version=editor.read().current.version;
  assert.equal(f.read(db=>db.prepare('SELECT revision FROM local_apps WHERE id=?').get(f.id).revision),originalRevision,'private cover persistence does not rely on an app revision bump');
  editor.dispose();binding.dispose();presenter.dispose();assert.equal(resolveObjectURL(firstUrl),undefined);
  f.reopen();const after=[],p2=createAppCoverPresenter({accountId:owner,isCurrent:()=>generation===1,readApp:(_,width)=>sdk().call('read',{width})});
  const b2=p2.watch({identity:'source-one',appId:f.id,isCurrent:()=>generation===1,onVisual:value=>after.push(value)});b2.setVisible(true);await until(()=>after.at(-1)?.mode==='custom');
  assert.deepEqual(Buffer.from(await resolveObjectURL(after.at(-1).url).arrayBuffer()),Buffer.from(savedImage),'presenter reads the real persistent current image after reopening services');
  const reloaded=createCoverEditorState({sdk:sdk(),prepareFile:upload(input),journal:createCoverJournal(disk),isCurrent:()=>generation===1,onCommitted:()=>p2.invalidate(f.id)});await reloaded.load();
  await reloaded.changeMode('none');await until(()=>after.at(-1)?.mode==='none');assert.equal(p2.stats().bytes,0);
  await reloaded.changeMode('inherit');await until(()=>after.at(-1)?.mode==='inherit');await reloaded.restore(version);await until(()=>after.at(-1)?.mode==='custom');
  const foreign=[],foreignSdk=createCoverApi({api:api('stranger'),kind:'app',targetId:f.id,accountId:stranger,isCurrent:()=>generation===1});
  const viewer=createAppCoverPresenter({accountId:stranger,isCurrent:()=>generation===1,readApp:(_,width)=>foreignSdk.call('read',{width})});
  const v=viewer.watch({identity:'source-one',appId:f.id,isCurrent:()=>generation===1,onVisual:value=>foreign.push(value)});v.setVisible(true);await until(()=>foreign.at(-1)?.mode==='custom');const privateUrl=foreign.at(-1).url;
  await f.publish('private');viewer.invalidateAll();assert.equal(resolveObjectURL(privateUrl),undefined);await until(()=>foreign.at(-1)?.mode==='inherit');assert.equal(viewer.stats().bytes,0);
  assert.equal(wire.some(v=>Object.keys(v.args).some(key=>['url','origin','assetRef','publicURL','sourcePath','coverKey'].includes(key))),false,'SDK dispatches exact IDs and bytes, never a guessed public resource URL');
  reloaded.dispose();v.dispose();viewer.dispose();b2.dispose();p2.dispose();generation++;
});

test('actual signed World6 profile editor preserves legacy full/thumbnail storage and history across restart',async t=>{
  const f=await signedFixture(t),person=f.actors[0];await f.profile(person);let generation=1;const changed=[];
  const api={async request(op,args){const result=await f.rpc(person,op,args);if(!result.ok)throw Object.assign(Error(result.error.code),{code:result.error.code});return result;}};
  const sdk=()=>createCoverApi({api,kind:'profile',targetId:person.accountId,accountId:person.accountId,isCurrent:()=>generation===1});
  const disk=storage(),input=pngFixture(100,80).toString('base64');
  const create=()=>createCoverEditorState({sdk:sdk(),prepareFile:upload(input),journal:createCoverJournal(disk,()=>f.now),now:()=>f.now,isCurrent:()=>generation===1,onCommitted:value=>changed.push(value)});
  const editor=create();await editor.load();await editor.choose({});await editor.save();assert.equal(editor.read().pending,null);assert.equal(changed.length,1);
  const first=await sdk().call('read'),version=editor.read().current.version,profile=await f.profile(person);assert.equal(profile.avatarRevision,profile.revision);
  const legacy=good(await f.rpc(person,'world.profile.avatar.read',{profileId:person.accountId}));assert.equal(legacy.avatarUrl,`data:image/webp;base64,${Buffer.from(first.image.bytes).toString('base64')}`);
  const db=new DatabaseSync(f.worldPath,{readOnly:true});let backing;
  try{backing=db.prepare('SELECT mime,image,thumbnail_mime,thumbnail FROM profile_avatars WHERE account_id=?').get(person.accountId);}finally{db.close();}
  assert.equal(backing.mime,'image/webp');assert.equal(backing.thumbnail_mime,'image/webp');assert.deepEqual(Buffer.from(backing.image),Buffer.from(first.image.bytes));
  const thumbnail=await sharp(Buffer.from(backing.thumbnail)).metadata();assert.ok(thumbnail.width>0&&thumbnail.width<=192);assert.equal(thumbnail.width,thumbnail.height);
  editor.dispose();f.reopen();const second=create();await second.load();assert.equal(second.read().current.version,version);assert.ok(second.read().source.url.startsWith('blob:'));
  await second.changeMode('inherit');assert.equal((await f.profile(person)).avatarRevision,null);assert.equal((await sdk().call('read')).image,null);
  await second.restore(version);assert.deepEqual((await sdk().call('read')).image.bytes,first.image.bytes);assert.equal((await f.profile(person)).avatarRevision,(await f.profile(person)).revision);
  second.dispose();generation++;
});
