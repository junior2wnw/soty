import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import ts from 'typescript';
import {createCanvasObjectsService} from '../../modules/canvas-objects/server.mjs';
import {createCanvasVaultService} from '../../modules/canvas-vault/server.mjs';
import {createNoteSession,blankNote,noteErrorCode} from '../../modules/notes/browser/state.mjs';
import {readNoteWithCache} from '../../modules/notes/browser/cache.mjs';
import {vaultIndexedPort as indexedPort} from './test-support/vault-indexed-port.mjs';
import * as nativeState from './native-field-objects-state.mjs';
import {nativeFieldSafeArea} from './native-field-safe-area.mjs';
import {createCanvasVaultClient,canvasVaultNoteContent,CANVAS_VAULT_NOTE_TYPE} from './canvas-vault-client.mjs';
import {createCanvasVaultLocalStore,createCanvasVaultMemoryCache,vaultErrorCode} from './canvas-vault-local.mjs';
const rendererSource=readFileSync(new URL('./canvas-vault-renderer.ts',import.meta.url),'utf8'),nativeSource=readFileSync(new URL('./native-field-objects.ts',import.meta.url),'utf8');
const overviewSource=readFileSync(new URL('./field-object-overview.ts',import.meta.url),'utf8');
const compile=value=>ts.transpileModule(value,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'');
const portSource=readFileSync(new URL('./canvas-field-content.test.mjs',import.meta.url),'utf8'),parsed=ts.createSourceFile('port.mjs',portSource,ts.ScriptTarget.Latest,true,ts.ScriptKind.JS);
const node=parsed.statements.find(node=>ts.isClassDeclaration(node)&&node.name?.text==='NodePort');assert.ok(node);
const flatten=node=>[node,...node.children.flatMap(flatten)],byClass=(root,name)=>flatten(root).find(node=>node.className.split(/\s+/).includes(name));
const NodePort=vm.runInNewContext(`(${ts.createPrinter().printNode(ts.EmitHint.Unspecified,node,parsed)})`,{flatten});
const until=async predicate=>{const deadline=Date.now()+10000;while(Date.now()<deadline){if(predicate())return;await new Promise(resolve=>setTimeout(resolve,5));}assert.fail('Expected protected renderer did not settle');};
const owner={accountId:'renderer_owner',deviceId:'renderer_owner_device'},viewer={accountId:'renderer_viewer',deviceId:'renderer_viewer_device'},password='Renderer synthetic protected password',secret='Never in a Notes disk cache 9bc0e4';

async function setup(t,{code=rendererSource,type='note',readOnly=false,allowUnlock,sourceNoteId}={}){
  let objects,vault,time=1800000000000,current=true,allowed=true;const actor=readOnly?viewer:owner,calls=[],timers=new Map(),urls=[],revoked=[],defaults={store:0,cache:0};let timerId=0;
  const issued=new WeakSet([owner,viewer]),authority=(actor,callback)=>{assert.ok(issued.has(actor));return callback();};
  objects=createCanvasObjectsService({databasePath:':memory:',projectId:'renderer',actorActive:()=>true,withActorAuthorityFence:authority,withProtectionAuthority:(request,callback)=>vault.withProtectionAuthority(request,callback)});
  vault=createCanvasVaultService({databasePath:':memory:',projectId:'renderer',actorActive:()=>true,withActorAuthorityFence:authority,withProtectionTarget:(request,callback)=>objects.withProtectionTarget(request,callback),withObjectAuthority:(context,callback)=>objects.withObjectAuthority(context,callback),clock:()=>time});objects.registerProvider(vault.provider);
  const mime=type==='image'?'image/png':type==='audio'?'audio/webm':'video/webm',file=type==='note'?null:readFileSync(new URL(type==='image'?'../../public/icons/soty-192.png':`../../modules/materials/test/fixtures/${type==='audio'?'tone':'video'}.webm`,import.meta.url));
  const content=type==='note'?canvasVaultNoteContent({title:'Private title',body:secret,...(sourceNoteId?{noteId:sourceNoteId}:{})}):{contentType:mime,bytesBase64:file.toString('base64url')};
  const source=await vault.execute({actor:owner,op:'canvas.vault.create',args:{expectedAccountId:owner.accountId,requestId:crypto.randomUUID(),password,content}});
  const object=objects.execute({actor:owner,op:'canvas.objects.create',args:{expectedAccountId:owner.accountId,requestId:crypto.randomUUID(),binding:source.binding,presentation:{mask:{shape:'rect'},fontSize:16},visibility:{audience:readOnly?'public':'private',accountIds:[],communityIds:[]},hidden:{owner:false,others:false},protection:{kind:'password'}}}).current;
  const objectId=object.objectId,port=indexedPort(),store=createCanvasVaultLocalStore(actor.accountId,'renderer',{origin:'http://localhost',indexedDB:port.indexedDB});
  let projection=objects.project({actor,objectIds:[objectId]}).items[0],view;
  const api={async request(op,args){calls.push({op,args:structuredClone(args)});
    if(op==='canvas.objects.content'){const {bytes,...value}=objects.withObjectRead({actor,objectId:args.objectId},context=>vault.readContent({context,index:args.index??0}));return{...value,bytesBase64:Buffer.from(bytes).toString('base64url')};}
    assert.ok(op.startsWith('canvas.vault.'),'Protected renderer never uses ordinary Notes or Materials transport');return vault.execute({actor,op,args:structuredClone(args)});
  }};
  const client=createCanvasVaultClient({api,accountId:actor.accountId,projectId:'renderer',isCurrent:()=>current,store,clock:()=>time,setTimer:(callback,delay)=>{const id=++timerId;timers.set(id,{callback,delay});return id;},clearTimer:id=>timers.delete(id)});
  class ImagePort extends NodePort{}
  class MediaPort extends NodePort{playCalls=0;pauseCalls=0;loadCalls=0;async play(){this.playCalls++;}pause(){this.pauseCalls++;}load(){this.loadCalls++;}}
  class VideoPort extends MediaPort{}
  const document={activeElement:null,createElement(tag){const C=tag==='img'?ImagePort:tag==='video'?VideoPort:tag==='audio'?MediaPort:NodePort;const value=new C(tag);value.ownerDocument=document;return value;}};
  Object.defineProperty(NodePort.prototype,'firstElementChild',{get(){return this.children[0]??null;},configurable:true});
  const env={exports:{},document,HTMLElement:NodePort,HTMLImageElement:ImagePort,HTMLMediaElement:MediaPort,HTMLVideoElement:VideoPort,createNoteSession,blankNote,noteErrorCode,readNoteWithCache,...nativeState,nativeFieldSafeArea,createCanvasVaultMemoryCache,vaultErrorCode,CANVAS_VAULT_NOTE_TYPE,
    createDraftStore(){defaults.store++;throw Error('plaintext DraftStore default forbidden');},createNoteCache(){defaults.cache++;throw Error('plaintext NoteCache default forbidden');},readMaterialBlob(){throw Error('Materials reader forbidden for Vault');},
    structuredClone,AbortController,queueMicrotask,Promise,Blob,URL:{createObjectURL:blob=>{const url='blob:protected-'+urls.length;urls.push({url,blob});return url;},revokeObjectURL:url=>revoked.push(url)}};
  env.icon=()=>document.createElement('svg');
  const context=vm.createContext(env);vm.runInContext(compile(overviewSource),context);vm.runInContext(compile(nativeSource)+'\nexports.native=createNativeFieldObjects;',context);env.createNativeFieldObjects=env.exports.native;
  vm.runInContext('(function(){'+compile(code)+'\nexports.create=createCanvasVaultView;exports.sealing=createCanvasSealingView;})()',context);
  const item={shortcutId:'vault-placement',entity:{entity:{kind:'object',id:objectId},title:'Под паролем'},presentation:{shape:'rect'},width:340,height:300};
  const refresh=async()=>{projection=objects.project({actor,objectIds:[objectId]}).items[0];view?.update(item);};
  view=env.exports.create(item,{client,accountId:actor.accountId,objectId,isCurrent:()=>current&&allowed,projection:()=>projection,onObjectChanged:refresh,...(allowUnlock===undefined?{}:{allowUnlock})});
  t.after(()=>{view.dispose();client.dispose();store.close();vault.close();objects.close();});
  return{view,client,store,port,objectId,item,calls,defaults,urls,revoked,refresh,document,createSealing:env.exports.sealing,
    async unlock(){const form=byClass(view.element,'cv-locked'),input=flatten(form).find(node=>node.tag==='input');input.value=password;form.emit('submit');await until(()=>client.isUnlocked(objectId));await refresh();},
    advance(){time+=300001;for(const[id,entry]of[...timers]){timers.delete(id);entry.callback();}},deny(){allowed=false;client.invalidate(objectId,'projection');},
  };
}

test('locked protected note reads nothing; explicit form unlock uses both encrypted draft and RAM cache overrides',async t=>{
  const f=await setup(t);f.view.setVisible(true);assert.equal(f.calls.length,0);assert.ok(byClass(f.view.element,'cv-locked'));assert.equal(byClass(f.view.element,'nf-body'),undefined);
  await f.unlock();await until(()=>byClass(f.view.element,'nf-body')?.disabled===false);const body=byClass(f.view.element,'nf-body');assert.equal(body.value,secret);assert.deepEqual(f.defaults,{store:0,cache:0});
  body.focus();body.value='Protected typed text b49cd1';body.selectionStart=7;body.emit('input');await f.view.flush();assert.equal(body.selectionStart,7);assert.equal((await f.client.readNote(f.objectId)).body,'Protected typed text b49cd1');
  for(const row of f.port.rows.values())for(const text of[secret,password,'Protected typed text'])assert.equal(JSON.stringify(row).includes(text),false);
  await f.client.relock(f.objectId);assert.equal(body.value,'');assert.equal(byClass(f.view.element,'nf-body'),undefined);assert.ok(byClass(f.view.element,'cv-locked'));
});

async function lockedPreviewScenario(t,code=rendererSource){
 const f=await setup(t,{code});f.view.setVisible(true);const form=byClass(f.view.element,'cv-locked'),input=flatten(form).find(value=>value.tag==='input');
 for(const presentation of ['glance','overview']){
  f.view.setPresentation(presentation);
  assert.equal(f.view.element.dataset.presentation,'overview','A locked noninteractive object must render its semantic lock face, never a miniature form');
  assert.equal(f.view.element.inert,true,'Hidden password controls must not take pointer or keyboard input');
  assert.equal(f.view.interaction,'focus','The existing whole-object action must enter readable focus in one step');
  assert.equal(f.view.overview.dataset.presentation,'overview');
 }
 f.view.setPresentation('interactive');assert.equal(f.view.element.dataset.presentation,'interactive');assert.equal(f.view.element.inert,false);
 assert.equal(byClass(f.view.element,'cv-locked'),form);assert.equal(flatten(form).find(value=>value.tag==='input'),input,'Focus reveals the same provider form');
 assert.equal(f.calls.length,0,'Presentation cannot grant permission or eagerly read protected content');
 await f.unlock();assert.equal(f.view.interaction,undefined,'An unlocked provider keeps its existing inline content interaction');
 f.view.setPresentation('glance');assert.equal(f.view.element.dataset.presentation,'glance');assert.equal(f.view.element.inert,false);
 await f.client.relock(f.objectId);assert.equal(f.view.element.dataset.presentation,'overview');assert.equal(f.view.element.inert,true);
 f.deny();assert.equal(f.view.overview.textContent,'');assert.equal(f.view.element.children.length,0);
}
test('locked glance is a semantic preview until actual readable focus, while unlock/relock/revoke preserve their authority',lockedPreviewScenario);

async function identityRevokeScenario(t,code=rendererSource){
 const f=await setup(t,{code}),face=f.view.overview,host=f.document.createElement('div');host.append(f.view.element,face);f.view.setVisible(true);f.view.setPresentation('overview');
 assert.equal(face.children[1].textContent,'Сота под паролем');await f.unlock();assert.equal(face.children[1].textContent,'Записка');assert.equal(face.textContent.includes('Private title'),false,'Decrypted note title is never used as Root identity');
 const body=byClass(f.view.element,'nf-body');await until(()=>!body.disabled);body.value='Protected retained draft';body.emit('input');f.advance();
 assert.equal(body.value,'');assert.equal(face.children[1].textContent,'Сота под паролем','TTL updates generic identity synchronously before project');
 f.deny();assert.equal(face.textContent,'','Authority loss scrubs sibling before any host update');
 f.view.dispose();assert.equal(face.parentElement,null);
}
test('Vault identity remains generic, clears on TTL/revoke before render, and never mirrors decrypted title',identityRevokeScenario);
test('broken Vault clear leaves a denied sibling and fails the same immediate privacy criterion',async t=>{
 const anchor='function clear(){overview.clear();';assert.equal(rendererSource.split(anchor).length,2);
 await assert.rejects(identityRevokeScenario(t,rendererSource.replace(anchor,'function clear(){')),/Authority loss scrubs sibling/);
});

test('a wrong valid-length password displays an opaque retry message rather than a false minimum-length hint',async t=>{
  const f=await setup(t),form=byClass(f.view.element,'cv-locked'),input=flatten(form).find(node=>node.tag==='input');input.value='Valid-length incorrect synthetic password';assert.ok(input.value.length>=8);form.emit('submit');
  await until(()=>f.view.element.textContent.includes('Проверьте пароль'));
  assert.match(f.view.element.textContent,/Не удалось открыть объект\. Проверьте пароль\./u);assert.doesNotMatch(f.view.element.textContent,/не меньше|8 символов/u);assert.equal(f.client.isUnlocked(f.objectId),false);assert.equal(f.calls.some(call=>call.op==='canvas.objects.content'),false);
});
test('TTL and fresh projection denial scrub a dirty focused protected textarea before any fresh server response',async t=>{
  const f=await setup(t);f.view.setVisible(true);await f.unlock();await until(()=>byClass(f.view.element,'nf-body')?.disabled===false);
  const body=byClass(f.view.element,'nf-body');body.value='Unsaved protected thought 2ebc1';body.emit('input');await until(()=>[...f.port.rows.values()].some(row=>row.value.purpose==='draft'));
  f.advance();assert.equal(body.value,'');assert.equal(byClass(f.view.element,'nf-body'),undefined);assert.ok(byClass(f.view.element,'cv-locked'));
  await f.client.unlock(f.objectId,password);await f.refresh();await until(()=>byClass(f.view.element,'nf-body')?.disabled===false);const recovered=byClass(f.view.element,'nf-body');assert.equal(recovered.value,'Unsaved protected thought 2ebc1');
  f.deny();assert.equal(recovered.value,'');assert.equal(f.view.element.children.length,0);
});
test('protected public note stays copyable/read-only and creates neither plain nor encrypted edit outbox',async t=>{
  const f=await setup(t,{readOnly:true});f.view.setVisible(true);await f.unlock();await until(()=>byClass(f.view.element,'nf-body')?.disabled===false);const body=byClass(f.view.element,'nf-body');
  assert.equal(body.readOnly,true);body.value='Forbidden viewer write';body.emit('input');await f.view.flush();assert.equal(f.calls.some(call=>call.op==='canvas.vault.write'),false);assert.equal((await f.store.list()).length,0);assert.deepEqual(f.defaults,{store:0,cache:0});
});
for(const type of['image','audio','video'])test(`protected ${type} uses Blob only after unlock/user intent and releases all media on relock`,async t=>{
  const f=await setup(t,{type});f.view.setVisible(true);assert.equal(f.calls.length,0);await f.unlock();
  if(type!=='image'){assert.equal(f.calls.some(call=>call.op==='canvas.objects.content'),false);byClass(f.view.element,'nf-play').emit('click');}
  await until(()=>f.urls.length===1);const media=flatten(f.view.element).find(node=>node.tag===(type==='image'?'img':type));if(type!=='image'){await until(()=>media.playCalls===1);assert.equal(media.autoplay,false);assert.equal(media.controls,true);if(type==='video')assert.equal(media.playsInline,true);}
  await f.client.relock(f.objectId);assert.equal(media.src,'');assert.equal(f.revoked.length,1);if(type!=='image')assert.ok(media.pauseCalls>0);assert.ok(byClass(f.view.element,'cv-locked'));
});
test('negative cache control: a protected renderer omitting its cache override attempts the forbidden plaintext factory',async t=>{
  const from='store:options.client.draftStore(options.objectId),cache,';assert.equal(rendererSource.split(from).length,2);
  const f=await setup(t,{code:rendererSource.replace(from,'store:options.client.draftStore(options.objectId),')});f.view.setVisible(true);await f.client.unlock(f.objectId,password);
  await assert.rejects(f.refresh(),/plaintext NoteCache default forbidden/);assert.ok(f.defaults.cache>0);
});
test('anonymous host policy renders an informative locked article with no password form, eager Source read or API action',async t=>{
  const f=await setup(t,{allowUnlock:false});f.view.setVisible(true);f.view.activate();assert.equal(f.calls.length,0);
  assert.equal(flatten(f.view.element).some(node=>node.tag==='form'||node.type==='password'),false);assert.match(f.view.element.textContent,/Войдите в Соты/u);
  // A host-provided client permit still cannot make an anonymous UI reveal data.
  await f.client.unlock(f.objectId,password);await f.refresh();assert.equal(f.calls.some(call=>call.op==='canvas.objects.content'),false);assert.equal(byClass(f.view.element,'nf-body'),undefined);
});
test('pending sealing never claims protection completion and its only action refreshes metadata',async t=>{
  const f=await setup(t);let reads=0;const view=f.createSealing(f.item,{isCurrent:()=>true,onObjectChanged:async id=>{assert.equal(id,f.objectId);reads++;}});t.after(()=>view.dispose());
  assert.match(view.element.textContent,/ещё не завершена/u);assert.equal(flatten(view.element).some(node=>node.type==='password'),false);assert.equal(f.calls.length,0);
  flatten(view.element).find(node=>node.tag==='button').emit('click');await until(()=>reads===1);assert.equal(f.calls.length,0);
});

async function recoveryScenario(t,code=rendererSource){
  const sourceNoteId='native_recovery_source_123',f=await setup(t,{code,sourceNoteId});f.view.setVisible(true);await f.client.unlock(f.objectId,password);
  const current=await f.client.readNote(f.objectId),oldMutation=crypto.randomUUID(),old={...current,noteId:sourceNoteId,body:'Archived newer Native thought c7ac'};
  await f.client.archiveNativeDrafts(f.objectId,{providerId:'soty.notes',ref:sourceNoteId,ownerId:owner.accountId},[{note:old,branchId:'old_native_branch_123',generation:2,committedGeneration:0,pending:{mutationId:oldMutation,expectedRevision:37,generation:1,document:{...old,body:'Older submitted text'}},conflict:false,savedAt:100}]);
  await f.refresh();await until(()=>byClass(f.view.element,'nf-body')?.disabled===false&&byClass(f.view.element,'cv-native-recovery')?.children.some(node=>node.tag==='button'));
  const body=byClass(f.view.element,'nf-body'),button=byClass(f.view.element,'cv-native-recovery').children.find(node=>node.tag==='button'),write=f.client.writeNote,restore=f.client.restoreNativeRecovery;let restores=0;
  f.client.restoreNativeRecovery=async(...args)=>{restores++;return restore(...args);};
  body.focus();body.value='Current encrypted thought still needs saving';body.emit('input');f.client.writeNote=async()=>{throw Error('network_offline');};
  button.emit('click');await until(()=>restores>0||f.view.element.textContent.includes('Сначала сохраните текущую версию'));
  assert.equal(restores,0,'An unconfirmed current encrypted branch cannot be replaced');
  assert.equal(body.value,'Current encrypted thought still needs saving');assert.equal((await f.client.nativeRecoveries(f.objectId)).length,1,'Pending current work cannot silently be replaced by an archive');
  assert.equal((await f.client.draftStore(f.objectId).list()).some(row=>row.note.body===old.body),false,'No recovery branch is created before current work settles');
  f.client.writeNote=write;await f.view.flush();button.emit('click');await until(()=>byClass(f.view.element,'nf-body')?.value===old.body);
  await f.view.flush();assert.equal((await f.client.readNote(f.objectId)).body,old.body);await until(()=>byClass(f.view.element,'cv-native-recovery')?.hidden===true);
  assert.deepEqual(await f.client.nativeRecoveries(f.objectId),[]);assert.ok(f.calls.filter(value=>value.op==='canvas.vault.write').every(value=>value.args.requestId!==oldMutation));
  assert.deepEqual(f.defaults,{store:0,cache:0});
}
test('explicit recovery preserves pending current work, then creates a fresh encrypted write and retires only the matching archive',recoveryScenario);
test('negative control: local durability alone cannot authorize replacing an unconfirmed encrypted branch',async t=>{
  const from='inner?.hasUnsavedChanges()||inner?.hasUnconfirmedWork?.()';assert.equal(rendererSource.split(from).length,2);
  await assert.rejects(recoveryScenario(t,rendererSource.replace(from,'inner?.hasUnsavedChanges()')),/An unconfirmed current encrypted branch cannot be replaced/);
});
