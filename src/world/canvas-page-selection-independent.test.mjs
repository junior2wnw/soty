import test from 'node:test';
import assert from 'node:assert/strict';
import {pageFrontendFixture,deferred,until,settle} from './test-support/canvas-page-share-independent.mjs';
import {loadCanvasPageChoices} from './canvas-page-selection.mjs';
import {randomUUID} from 'node:crypto';
import {readFile} from 'node:fs/promises';

test('existing public page can close while field/Source publication preparation is still held',async t=>{
 const f=await pageFrontendFixture(t),a=await f.create('Previously selected A');await f.place([a]);await f.publish([a]);
 const held=deferred();let preparing=false;const ui=f.frontend({prepare:()=>{preparing=true;return held.promise;}});t.after(()=>held.resolve());
 await until(()=>preparing);ui.select().value='private';ui.select().emit('change');ui.form.emit('submit');await settle();
 assert.equal(f.calls.filter(value=>value.operation==='canvas.pages.put').length,1,'closing a known page must not wait for unrelated Source/draft flush');
 await until(()=>ui.footer[0].textContent==='Сохранить');assert.equal((await f.raw('canvas.pages.get')).page.enabled,false);
});

async function replacementOracle(t,options){
 const f=await pageFrontendFixture(t,options),a=await f.create('Selected original A'),b=await f.create('Never selected PRIVATE B'),shortcutId='same_canonical_shortcut';assert.equal(a.accessRevision,b.accessRevision);
 await f.place([{...a,shortcutId}]);const page=(await f.publish([{...a,shortcutId}])).current;
 await f.place([{...b,shortcutId}]);assert.equal((await f.anonymous(page.pageId)).items.length,0,'stored Page entity pin refuses the substituted Source before UI save');
 const ui=f.frontend();await ui.view.ready;assert.equal(ui.checkboxes().length,1);assert.equal(ui.checkboxes()[0].checked,true);
 ui.select().value='public';ui.select().emit('change');
 assert.equal(ui.footer[0].disabled,true,'a different private entity needs explicit re-selection even when its access revision happens to match');
 assert.match(ui.find('cp-share-notice').textContent,/измен|обнов|недоступ/iu,'the old selection remains an explicit stale choice');
 ui.form.emit('submit');await settle();
 const visible=await f.anonymous(page.pageId),current=(await f.raw('canvas.pages.get')).page;
 assert.deepEqual({exposedEntities:visible.items.map(item=>item.entity.id),pageRevision:current.revision},{exposedEntities:[],pageRevision:1},'changing audience must not grant a different private Source without explicit re-selection');
}
test('same shortcutId and policy revision cannot silently reauthorize a replacement private Object',async t=>replacementOracle(t));

test('the same replacement oracle detects removal of only the frontend captured-entity comparison',async t=>{
 const source=await readFile(new URL('./canvas-page-share.ts',import.meta.url),'utf8'),guard='||choice.entity.kind!==entry.entity.kind||choice.entity.id!==entry.entity.id';assert.equal(source.split(guard).length,2,'one exact necessary guard');
 await assert.rejects(replacementOracle(t,{shareSource:source.replace(guard,'')}),error=>{assert.equal(error.code,'ERR_ASSERTION');assert.match(error.message,/different private entity needs explicit re-selection/u);return true;});
});

test('choices come only from canonical placements, excluding personal conversations and unplaced owner objects',async t=>{
 const f=await pageFrontendFixture(t),placed=await f.create('Canonical placed'),unplaced=await f.create('Unplaced owner Source');
 await f.place([placed,{shortcutId:'private_conversation',entity:{kind:'person',id:'friend_not_for_publication'}},{shortcutId:'private_device',entity:{kind:'device',id:'device_not_for_publication'}},{shortcutId:'private_tool',entity:{kind:'builtin',id:'notes'}}]);
 const calls=[];const snapshot=await loadCanvasPageChoices({accountId:f.accountId,api:{request(operation,args){calls.push({operation,args});return f.raw(operation,args);}}});
 assert.deepEqual(snapshot.choices.map(value=>value.entity.id),[placed.objectId]);assert.equal(snapshot.excludedCount,3);
 assert.deepEqual(calls.filter(value=>value.operation==='canvas.objects.project').flatMap(value=>value.args.objectIds),[placed.objectId]);
 assert.equal(JSON.stringify(snapshot).includes(unplaced.objectId),false);assert.equal(calls.some(value=>/discovery|objects.list|apps.list/u.test(value.operation)),false);
});

test('explicit empty selection stays empty instead of falling back to all currently public objects',async t=>{
 const f=await pageFrontendFixture(t),a=await f.create('Public object',{visibility:{audience:'public',accountIds:[],communityIds:[]}});await f.place([a]);const page=(await f.publish([])).current;
 const ui=f.frontend();await ui.view.ready;assert.equal(ui.checkboxes().length,1);assert.equal(ui.checkboxes()[0].checked,false);assert.match(ui.find('cp-share-count').textContent,/0/u);
 ui.select().value='public';ui.select().emit('change');ui.form.emit('submit');await until(()=>f.calls.some(value=>value.operation==='canvas.pages.put'));await until(()=>ui.footer[0].textContent!=='Сохраняем…');
 const command=f.calls.find(value=>value.operation==='canvas.pages.put').args;assert.deepEqual(command.selection.items,[]);assert.equal((await f.anonymous(page.pageId)).items.length,0);
});

test('lost publication ACK retries the original complete command instead of generating a second intent',async t=>{
 const f=await pageFrontendFixture(t),a=await f.create('Lost ACK original choice');await f.place([a]);let lose=true;
 const ui=f.frontend({intercept(operation,_args,result){if(operation==='canvas.pages.put'&&lose){lose=false;throw Object.assign(Error('network_timeout'),{code:'network_timeout'});}return result;}});await ui.view.ready;
 ui.checkboxes()[0].checked=true;ui.checkboxes()[0].emit('change');ui.select().value='unlisted';ui.select().emit('change');ui.form.emit('submit');await until(()=>ui.footer[0].textContent==='Повторить');
 assert.equal(ui.find('cp-share').children.find(value=>value.tagName==='fieldset').disabled,true);ui.form.emit('submit');await until(()=>f.calls.filter(value=>value.operation==='canvas.pages.put').length===2);await until(()=>ui.footer[0].textContent==='Сохранить');
 const commands=f.calls.filter(value=>value.operation==='canvas.pages.put').map(value=>value.args);assert.deepEqual(commands[1],commands[0]);const page=(await f.raw('canvas.pages.get')).page;assert.equal(page.revision,1);assert.equal((await f.anonymous(page.pageId)).items.length,1);
});

test('known revision conflict retains choices and audience until explicit refresh obtains the current Page revision',async t=>{
 const f=await pageFrontendFixture(t),a=await f.create('Original choice'),b=await f.create('Additional explicit choice');await f.place([a,b]);await f.publish([a]);const ui=f.frontend();await ui.view.ready;
 const bInput=ui.checkboxes().find(value=>value['aria-label']==='Additional explicit choice');bInput.checked=true;bInput.emit('change');ui.select().value='public';ui.select().emit('change');
 await f.raw('canvas.pages.put',{requestId:randomUUID(),expectedRevision:1,enabled:true,audience:'unlisted'});
 ui.form.emit('submit');await until(()=>ui.find('co-error').hidden===false);assert.equal(bInput.checked,true);assert.equal(ui.select().value,'public');assert.match(ui.find('co-error').textContent,/Обновите выбор/u);
 ui.find('cp-share-selection-head').children.find(value=>value.tagName==='button').emit('click');await until(()=>f.calls.filter(value=>value.operation==='canvas.pages.get').length===2);await until(()=>ui.footer[0].disabled===false);assert.equal(ui.checkboxes().find(value=>value['aria-label']==='Additional explicit choice').checked,true);
 ui.form.emit('submit');await until(()=>f.calls.filter(value=>value.operation==='canvas.pages.put').length===2);await until(()=>ui.footer[0].textContent==='Сохранить');
 const commands=f.calls.filter(value=>value.operation==='canvas.pages.put').map(value=>value.args);assert.equal(commands[0].expectedRevision,1);assert.equal(commands[1].expectedRevision,2);assert.notEqual(commands[1].requestId,commands[0].requestId);assert.equal(commands[1].selection.items.length,2);assert.equal((await f.raw('canvas.pages.get')).page.revision,3);
});

test('missing or revoked selected Object remains visible and removable, never silently dropped or reauthorized',async t=>{
 const f=await pageFrontendFixture(t),a=await f.create('Revoked selected object');await f.place([a]);await f.publish([a]);await f.raw('canvas.objects.update',{requestId:randomUUID(),objectId:a.objectId,expectedRevision:1,hidden:{owner:false,others:true}});
 const ui=f.frontend();await ui.view.ready;assert.equal(ui.checkboxes().length,1);const input=ui.checkboxes()[0];assert.equal(input.checked,true);assert.equal(input.disabled,false);ui.select().value='public';ui.select().emit('change');assert.equal(ui.footer[0].disabled,true);assert.match(ui.find('cp-share-notice').textContent,/недоступ|измен/u);
 input.checked=false;input.emit('change');assert.equal(ui.footer[0].disabled,false);ui.form.emit('submit');await until(()=>f.calls.some(value=>value.operation==='canvas.pages.put'));await until(()=>ui.footer[0].textContent==='Сохранить');assert.deepEqual(f.calls.find(value=>value.operation==='canvas.pages.put').args.selection.items,[]);
});

test('late account response after disposal cannot populate choices, link, QR or clipboard',async t=>{
 const f=await pageFrontendFixture(t),a=await f.create('ACCOUNT_A_PRIVATE_TITLE');await f.place([a]);await f.publish([a]);const held=deferred();let got=false;
 const ui=f.frontend({intercept(operation,_args,result){if(operation==='canvas.pages.get'){got=true;return held.promise.then(()=>result);}return result;}});await until(()=>got);ui.retire();ui.dispose();held.resolve();await ui.view.ready;await settle();
 assert.equal(ui.text().includes('ACCOUNT_A_PRIVATE_TITLE'),false);assert.equal(ui.find('cp-share-choices').children.length,0);assert.equal(ui.root.all().find(value=>value.tagName==='input'&&value.type==='url').value,'');assert.equal(ui.find('cp-share-qr').width,0);
 ui.find('cp-share-actions').children[0].emit('click');assert.deepEqual(ui.copied,[]);assert.equal(f.calls.some(value=>value.operation==='canvas.objects.project'),false);
});
