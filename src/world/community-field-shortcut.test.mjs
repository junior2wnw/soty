import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import vm from 'node:vm';
import ts from 'typescript';
import { createWorldService } from '../../modules/world/server/index.mjs';
import { fieldEntityKey } from '../../modules/field/contract.mjs';
import { createFieldDirectory } from './field-directory.ts';
import { createFieldPersistence } from './field-persistence.ts';
import { ensureFieldEntityPlacement } from './app-field-placement.mjs';
import { applyFieldCommand } from './unified-field-state.mjs';
import { layoutUnifiedField } from './unified-field-layout.mjs';
import { createCommunityConversation } from './conversation-target.mjs';
import { createHash } from 'node:crypto';

// Actual admitted App/preview actions -> shared placement commands -> real field
// persistence -> World SQLite. DOM/engine scheduling are explicit test ports;
// the old removed renderGroup header is not presented as the current UI.
const appText=readFileSync(new URL('./app.ts',import.meta.url),'utf8'),screenText=readFileSync(new URL('./unified-field-screen.ts',import.meta.url),'utf8');
const appSource=ts.createSourceFile('app.ts',appText,ts.ScriptTarget.Latest,true),screenSource=ts.createSourceFile('unified-field-screen.ts',screenText,ts.ScriptTarget.Latest,true),printer=ts.createPrinter();
const print=(node,source)=>printer.printNode(ts.EmitHint.Unspecified,node,source);
const screen=screenSource.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text==='createUnifiedFieldScreen');
const returned=screen.body.statements.find(ts.isReturnStatement).expression;
const addEntity=returned.properties.find(node=>ts.isMethodDeclaration(node)&&node.name.getText(screenSource)==='addEntity');
const actual=name=>screen.body.statements.find(node=>ts.isFunctionDeclaration(node)&&node.name?.text===name);
assert.ok(addEntity&&actual('addToContext')&&actual('placementCommands')&&actual('openPlacementActions'));
const appClass=appSource.statements.find(ts.isClassDeclaration);
const appMethod=name=>appClass.members.find(node=>ts.isMethodDeclaration(node)&&node.name.getText(appSource)===name);
const appAdd=appMethod('addCommunityToField');assert.ok(appAdd);
const find=(root,predicate)=>{let found;function visit(node){if(predicate(node)){assert.equal(found,undefined);found=node;}ts.forEachChild(node,visit);}visit(root);assert.ok(found);return found;};
const preview=actual('openPreview');
const pin=find(preview,node=>ts.isVariableDeclaration(node)&&node.name.getText(screenSource)==='pin').initializer;
const placed=find(preview,node=>ts.isVariableDeclaration(node)&&node.name.getText(screenSource)==='placed');
function compiled(omitPlacementActions=false){
 let commands=print(actual('placementCommands'),screenSource);
 if(omitPlacementActions){const anchor='if (placementCurrent())',position=commands.indexOf(anchor);assert.ok(position>=0);assert.match(commands.slice(position),/^if \(placementCurrent\(\)\)\s+commands\.push/u);commands=commands.slice(0,position)+commands.slice(position).replace(anchor,'if (false)');}
 return ts.transpileModule([
  ...['addToContext','currentEntity','openPlacementActions','inspect'].map(name=>print(actual(name),screenSource)),commands,
  `exports.field={${print(addEntity,screenSource)}};exports.commands=placementCommands;exports.hostActions=inspect;`,
  `exports.previewButton=function(item){const ${print(placed,screenSource)};return ${print(pin,screenSource)};};`,
  `class AppPort{${print(appMethod('accountTask'),appSource)} ${print(appAdd,appSource)}}exports.AppPort=AppPort;`,
 ].join('\n'),{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
}

class Port {
  children = []; handlers = new Map(); disabled = false; textContent = ''; classList = { add() {}, toggle() {} };
  constructor(tag = 'div', text = '') { this.tag = tag; this.textContent = text; }
  append(...values) { this.children.push(...values); }
  addEventListener(name, callback) { this.handlers.set(name, callback); }
  setAttribute() {}
  focus() {}
  get value() { return this.chosen ?? this.children.find(node => node.tag === 'option')?.value ?? ''; }
  set value(value) { this.chosen = value; }
  click() { if (!this.disabled) this.handlers.get('click')?.(); }
  queryAll(tag) { return [this, ...this.children.flatMap(node => node?.queryAll?.(tag) ?? [])].filter(node => node.tag === tag); }
}
const defer = () => { let done; return { promise: new Promise(resolvePromise => { done = resolvePromise; }), resolve: value => done(value) }; };
const until = async predicate => { for (let n = 0; n < 200; n++) { if (predicate()) return; await new Promise(done => setTimeout(done, 5)); } assert.fail('actual controller phase not reached'); };
const actor = { accountId: 'community-field-owner', deviceId: 'community-field-device', label: 'Проверка' };
const other = { accountId: 'community-field-other', deviceId: 'community-field-other-device', label: 'Другой' };

async function fixture(t, { loseAck = false, delayedReady = false, omitPlacementActions=false } = {}) {
  const base = resolve(tmpdir()), directory = mkdtempSync(join(base, 'soty-community-field-'));
  let world = createWorldService({ databasePath: join(directory, 'world.sqlite'), projectId: 'community-field-tests' });
  const call = (op, args = {}, who = actor) => world.execute({ actor: who, op: 'world.' + op, args });
  call('profile.get'); call('profile.get', {}, other);
  let group = call('community.create', { requestId: 'community_field_create', name: 'Моё сообщество' }).community;
  group = call('membership.preferences', { communityId: group.communityId, pinned: true }).community;
  const initial = { schema: 'soty.field.v1', contexts: [{ contextId: 'work', title: 'Работа', x: 0, y: 0 }, { contextId: 'home', title: 'Дома', x: 900, y: 0 }],
    shortcuts: [{ shortcutId: 'unrelated-notes', entity: { kind: 'builtin', id: 'notes' }, contextId: 'work', slot: [0, 0] }] };
  call('field.put', { expectedAccountId: actor.accountId, expectedRevision: 0, requestId: 'community_field_initial', document: initial });
  let generation = 1, current = true, record = null, beforeResolve = null, beforePut = null, quota = false;
  const ready = defer(), tasks = [], dialogs = [], notices = [], calls = [], metadata = new Map();
  const state = { mode: 'search', route: '', focused: '', selected:'', moves:[], closedObjects:0,navigations: 0, afterSave: null };
  const ownerCurrent = () => current && generation === 1;
  const api = { async request(op, args) {
    calls.push({ op, args: structuredClone(args) });
    if (op === 'world.directory.resolve' && beforeResolve) { const callback = beforeResolve; beforeResolve = null; await callback(); }
    if (op === 'world.field.put' && beforePut) { const callback = beforePut; beforePut = null; await callback(); }
    const result = world.execute({ actor, op, args });
    if (op === 'world.field.put' && loseAck) { loseAck = false; throw Object.assign(Error('network_lost_ack'), { code: 'network_lost_ack' }); }
    return result;
  } };
  const store = { async read() { return structuredClone(record); }, async write(next, expected) {
    if (quota) throw Object.assign(Error('field_local_storage_unavailable'), { code: 'field_local_storage_unavailable' });
    assert.equal(record?.localRevision ?? 0, expected); record = structuredClone(next);
  }, subscribe() { return () => {}; } };
  const persistence = createFieldPersistence({ api, accountId: actor.accountId, isCurrent: ownerCurrent, store });
  await persistence.load();
  const directoryPort = createFieldDirectory({ api, accountId: actor.accountId, isCurrent: ownerCurrent });
  const apply = async command => {
    const previous = persistence.getState(), change = applyFieldCommand(previous.document, command);
    return persistence.commit(change.document, { expectedRevision: previous.projectedRevision, requestId: crypto.randomUUID() });
  };
  const engine = { snapshot: () => structuredClone(persistence.getState().document),
    update() {}, focusContext: id => { state.focused = id; },
    revealShortcut(id){state.selected=id;state.focused=this.snapshot().shortcuts.find(row=>row.shortcutId===id)?.contextId??'';},
    closeObject(){state.closedObjects++;},beginMove(id){state.moves.push(id);},
    addShortcut: (entity, contextId) => apply({ type: 'add-shortcut', shortcutId: crypto.randomUUID(), entity: structuredClone(entity), contextId }),
    removeShortcut: shortcutId => apply({ type: 'remove-shortcut', shortcutId }) };
  const element = (tag, _className, text) => new Port(tag, text);
  const button = (text, _symbol, _className, onClick) => { const result = new Port('button', text); if (onClick) result.addEventListener('click', onClick); return result; };
  const context = vm.createContext({ exports: {}, crypto, fieldEntityKey, ensureFieldEntityPlacement,
    ready: ready.promise, current: ownerCurrent,mode:'search',metadata,searchItems:[],
    directory: { resolve: refs => directoryPort.resolve(structuredClone(refs)) }, engine, persistence,
    options: {mountCommunity(){}, onMessage: (text, error) => notices.push({ text, error: error === true }) },
    merge: items => { for (const item of items) metadata.set(fieldEntityKey(item.entity), item); },
    allMetadata: () => [...metadata.values()], summary: { focusContextId: 'work' },
    changeMode: mode => { state.mode = mode; context.mode=mode; }, updateMineFilter() {}, closePreview() {}, newContext() {},
    dialog(title) { const value = { title, body: new Port(), element: { open: true }, close() { this.element.open = false; } }; dialogs.push(value); return value; },
    el: element, labeledField: (_text, target) => { const label = new Port('label'); label.append(target); return label; },
    button, iconButton: (text, symbol, onClick) => button(text, symbol, '', onClick), errorText: error => String(error?.code ?? 'unresolved'),
    report: error => notices.push({ code: String(error?.code), error: true }),
    run: promise => { if (promise) tasks.push(promise.catch(error => notices.push({ code: String(error?.code), error: true }))); },
  });
  new vm.Script(compiled(omitPlacementActions)).runInContext(context);
  const app = new context.exports.AppPort();
  app.deskAccount = actor.accountId; app.accountGeneration = generation; app.destroyed = false;
  app.afterNoteSaved = action => { if (state.afterSave) state.afterSave(action); else action(); };
  app.navigateReady = route => { state.route = route; state.navigations++; app.unifiedField = context.exports.field; };
  app.toast = (text, error) => notices.push({ text, error });
  app.mutateMembership = (op, args) => { call(op.replace('world.', ''), { communityId: group.communityId, ...args }); };
  if (!delayedReady) ready.resolve();
  t.after(() => {
    current = false; directoryPort.dispose(); persistence.dispose(); world.close();
    assert.equal(dirname(resolve(directory)), base); assert.ok(directory.startsWith(join(base, 'soty-community-field-')));
    rmSync(directory, { recursive: true });
  });
  const opened = () => dialogs.findLast(value => value.element.open);
  return { world, call, group, initial, persistence, api, state, notices, calls, metadata, ready, app,
    field: context.exports.field,
    clickAdd(value = group) { app.addCommunityToField(value); },
    async clickPin(value=group){const conversation=createCommunityConversation({api,community:value,isCurrent:ownerCurrent});await conversation.setPreference('pinned',!conversation.preferences.pinned);},
    async admittedPreview(){const result=await directoryPort.resolve([{kind:'community',id:group.communityId}]);const item=result.items.find(value=>value.entity.id===group.communityId);assert.ok(item);metadata.set(fieldEntityKey(item.entity),item);return context.exports.previewButton(item);},
    previewButton(){const item=metadata.get(fieldEntityKey({kind:'community',id:group.communityId}));assert.ok(item);return context.exports.previewButton(item);},
    hostMenu(shortcutId){const item=metadata.get(fieldEntityKey({kind:'community',id:group.communityId}));assert.ok(item);return context.exports.hostActions(item,shortcutId);},
    commands(shortcutId){const item=metadata.get(fieldEntityKey({kind:'community',id:group.communityId}));return context.exports.commands(item,shortcutId);},
    reopenWorld(){world.close();world=createWorldService({databasePath:join(directory,'world.sqlite'),projectId:'community-field-tests'});},
    async replacePlacementEntity(shortcutId,ref){const result=await directoryPort.resolve([ref]);assert.ok(result.items.some(value=>fieldEntityKey(value.entity)===fieldEntityKey(ref)));for(const item of result.items)metadata.set(fieldEntityKey(item.entity),item);const state=persistence.getState(),document=structuredClone(state.document);document.shortcuts.find(value=>value.shortcutId===shortcutId).entity=ref;await persistence.commit(document,{expectedRevision:state.projectedRevision,requestId:crypto.randomUUID()});},
    async choose(contextId = 'work') { await until(() => !!opened()); const modal = opened(); modal.body.queryAll('select')[0].value = contextId;
      modal.body.queryAll('button').find(node => node.textContent === 'Добавить').click(); await until(() => tasks.length > 0 || !modal.element.open || notices.length > 0); await Promise.all(tasks); },
    async remove(shortcutId) {const item=metadata.get(fieldEntityKey({kind:'community',id:group.communityId}));const remove=context.exports.commands(item,shortcutId).find(action=>action.label==='Убрать с поля');assert.ok(remove,'Remove placement remains available');remove.run();await Promise.all(tasks);},
    document: () => call('field.get', { expectedAccountId: actor.accountId }).document,
    projection: () => layoutUnifiedField(persistence.getState().document, [...metadata.values()]),
    activeDialog: opened, settle: () => Promise.all(tasks),
    changeAccount(accountId = other.accountId) { generation++; app.accountGeneration = generation; app.deskAccount = accountId; },
    closeField() { current = false; app.unifiedField = null; },
    holdResolve(callback) { beforeResolve = callback; }, holdPut(callback) { beforePut = callback; },
    refuseStorage() { quota = true; },
  };
}

test('actual community Add creates a visible admitted shortcut; repeat is idempotent and remove preserves membership, chat pin and unrelated positions', async t => {
  const f = await fixture(t); f.clickAdd(); await f.choose();
  assert.equal(f.state.route, 'mine'); assert.equal(f.state.mode, 'mine'); assert.equal(f.state.focused, 'work');
  let placed = f.document().shortcuts.filter(value => value.entity.kind === 'community');
  assert.equal(placed.length, 1); assert.equal(placed[0].entity.id, f.group.communityId);
  assert.ok(f.projection().nodes.some(node => node.shortcutId === placed[0].shortcutId && node.entity.title === f.group.name));
  const revision = f.call('field.get', { expectedAccountId: actor.accountId }).revision;
  f.clickAdd(); await f.choose(); assert.equal(f.call('field.get', { expectedAccountId: actor.accountId }).revision, revision);
  assert.equal(f.document().shortcuts.filter(value => value.entity.kind === 'community').length, 1);
  assert.equal(f.call('community.get', { communityId: f.group.communityId }).community.membership.pinned, true);
  const durable=f.document();f.reopenWorld();assert.deepEqual(f.document(),durable,'the actual accepted placement survives a fresh World SQLite reader');
  await f.remove(placed[0].shortcutId); assert.deepEqual(f.document(), f.initial);
  const group = f.call('community.get', { communityId: f.group.communityId }).community;
  assert.equal(group.membership.state, 'active'); assert.equal(group.membership.pinned, true); assert.equal(group.permissions.canWrite, true);
});

test('the existing chat pin remains a separate explicit membership preference and does not create a shortcut', async t => {
  const f = await fixture(t); await f.clickPin();
  assert.equal(f.call('community.get', { communityId: f.group.communityId }).community.membership.pinned, false);
  assert.deepEqual(f.document(), f.initial);
  f.clickAdd(); await f.choose(); assert.equal(f.call('community.get', { communityId: f.group.communityId }).community.membership.pinned, false);
});

test('the existing App action waits for existing unsaved-work navigation guard and field readiness', async t => {
  const f = await fixture(t, { delayedReady: true }); let saved;
  f.state.afterSave = action => { saved = action; }; f.clickAdd();
  assert.equal(f.state.navigations, 0); saved(); assert.equal(f.state.navigations, 1);
  await Promise.resolve(); assert.equal(f.activeDialog(), undefined);
  f.ready.resolve(); await f.choose(); assert.equal(f.document().shortcuts.length, 2);
});

test('actual account A-B-A fence while navigation is guarded or directory response is pending cannot add or leak a late error', async t => {
  const guarded = await fixture(t); let saved;
  guarded.state.afterSave = action => { saved = action; }; guarded.clickAdd(); guarded.changeAccount(); guarded.changeAccount(actor.accountId); saved();
  assert.equal(guarded.state.navigations, 0); assert.deepEqual(guarded.document(), guarded.initial);
  const delayed = await fixture(t), gate = defer(); delayed.holdResolve(() => gate.promise); delayed.clickAdd();
  await until(() => delayed.calls.some(value => value.op === 'world.directory.resolve')); delayed.changeAccount(); delayed.changeAccount(actor.accountId); gate.resolve();
  await new Promise(done => setTimeout(done, 10)); assert.equal(delayed.activeDialog(), undefined);
  assert.equal(delayed.notices.length, 0); assert.deepEqual(delayed.document(), delayed.initial);
});

test('navigation away during admission retires the old field without opening a chooser or showing a late error', async t => {
  const f = await fixture(t), gate = defer(); f.holdResolve(() => gate.promise); f.clickAdd();
  await until(() => f.calls.some(value => value.op === 'world.directory.resolve')); f.closeField(); gate.resolve();
  await new Promise(done => setTimeout(done, 10)); assert.equal(f.activeDialog(), undefined); assert.equal(f.notices.length, 0);
  assert.deepEqual(f.document(), f.initial);
});

test('wrong kinds or forged metadata are rejected by the actual directory contract before field mutation', async t => {
  const f = await fixture(t);
  for (const ref of [{ kind: 'foreign-kind', id: f.group.communityId }, { kind: 'community', id: f.group.communityId, url: 'https://foreign.invalid/' }]) {
    await assert.rejects(f.field.addEntity(ref), { code: 'field_document_invalid' });
  }
  assert.equal(f.activeDialog(), undefined); assert.deepEqual(f.document(), f.initial);
});

test('a foreign private or already archived community cannot bypass the fresh directory resolution', async t => {
  const f = await fixture(t);
  const privateGroup = f.call('community.create', { requestId: 'foreign_private_group', name: 'Приватное', joinPolicy: 'invite' }, other).community;
  await f.field.addEntity({ kind: 'community', id: privateGroup.communityId });
  f.call('community.archive', { communityId: f.group.communityId, expectedRevision: f.group.revision });
  await f.field.addEntity({ kind: 'community', id: f.group.communityId });
  assert.equal(f.activeDialog(), undefined); assert.deepEqual(f.document(), f.initial);
  assert.equal(f.notices.filter(value => value.error).length, 2);
});

test('lost field ACK retains one durable intent and a retry proves the same accepted placement without another revision', async t => {
  const f = await fixture(t, { loseAck: true }); f.clickAdd(); await f.choose();
  assert.ok(f.notices.some(value => /ждём подключения/u.test(value.text ?? '')));
  assert.equal(f.persistence.getState().localDurable, true); assert.equal(f.persistence.getState().pendingCount, 1);
  const revision = f.call('field.get', { expectedAccountId: actor.accountId }).revision;
  await f.persistence.retry(); assert.equal(f.persistence.getState().pendingCount, 0);
  assert.equal(f.call('field.get', { expectedAccountId: actor.accountId }).revision, revision);
  assert.equal(f.document().shortcuts.filter(value => value.entity.kind === 'community').length, 1);
});

test('a competing field CAS cannot be overwritten by the community chooser and is not reported as saved', async t => {
  const f = await fixture(t); f.clickAdd(); await until(() => !!f.activeDialog());
  f.holdPut(() => {
    const current = f.call('field.get', { expectedAccountId: actor.accountId });
    const moved = applyFieldCommand(current.document, { type: 'move-context', contextId: 'home', x: 1200, y: 40 }).document;
    f.call('field.put', { expectedAccountId: actor.accountId, expectedRevision: current.revision, requestId: 'competing_field_write', document: moved });
  });
  await f.choose(); assert.equal(f.document().contexts[1].x, 1200);
  assert.equal(f.document().shortcuts.filter(value => value.entity.kind === 'community').length, 0);
  assert.equal(f.activeDialog()?.element.open, true); assert.ok(f.notices.some(value => /Поле изменилось/u.test(value.text ?? '')));
});

test('local storage refusal leaves the chooser open and never dispatches an undurable community placement', async t => {
  const f = await fixture(t); f.refuseStorage(); f.clickAdd(); await f.choose();
  assert.equal(f.calls.filter(value => value.op === 'world.field.put').length, 0);
  assert.deepEqual(f.document(), f.initial); assert.equal(f.activeDialog()?.element.open, true);
});

function assertPlacementMenu(menu) {
  const labels=menu.body.queryAll('button').map(button=>button.textContent);
  assert.ok(labels.includes('Переместить')&&labels.includes('Убрать с поля'),'both Move and Remove placement commands must remain available');
  return menu.body.queryAll('button');
}

test('admitted community preview adds to the field, then preview gear and native host settings share working Move and Remove',async t=>{
  const f=await fixture(t),add=await f.admittedPreview();assert.equal(add.textContent,'На моё поле');
  add.click();await f.choose();const placement=f.document().shortcuts.find(value=>value.entity.kind==='community');assert.ok(placement);
  assert.equal(f.state.selected,placement.shortcutId,'the new admitted placement is revealed rather than merely fitting its context');
  const bytes=JSON.stringify(f.document()),writes=f.calls.filter(value=>value.op==='world.field.put').length;
  const gear=f.previewButton();assert.equal(gear.textContent,'Действия с сотой');gear.click();
  const previewMenu=f.activeDialog(),previewButtons=assertPlacementMenu(previewMenu);
  previewButtons.find(button=>button.textContent==='Переместить').click();assert.deepEqual(f.state.moves,[placement.shortcutId]);
  assert.equal(previewMenu.element.open,false);assert.equal(JSON.stringify(f.document()),bytes,'beginning Move alone must not persist geometry');
  assert.equal(f.calls.filter(value=>value.op==='world.field.put').length,writes);
  const nativeMenu=f.hostMenu(placement.shortcutId),nativeButtons=assertPlacementMenu(nativeMenu);
  nativeButtons.find(button=>button.textContent==='Убрать с поля').click();await f.settle();
  assert.equal(nativeMenu.element.open,false);assert.deepEqual(f.document(),f.initial);
  const record=f.call('community.get',{communityId:f.group.communityId}).community;assert.equal(record.membership.state,'active');assert.equal(record.membership.pinned,true);
});

for(const retired of['account-ABA','removed-shortcut','rebound-entity'])test(`captured placement actions reject ${retired} without mutating the current field`,async t=>{
  const f=await fixture(t);f.clickAdd();await f.choose();const id=f.document().shortcuts.find(value=>value.entity.kind==='community').shortcutId;
  const captured=f.commands(id).filter(action=>['Переместить','Убрать с поля'].includes(action.label));assert.equal(captured.length,2);
  if(retired==='account-ABA'){f.changeAccount();f.changeAccount(actor.accountId);}
  else if(retired==='removed-shortcut')await f.remove(id);
  else{const replacement=f.call('community.create',{requestId:'replacement_identity',name:'Другая комната'}).community;await f.replacePlacementEntity(id,{kind:'community',id:replacement.communityId});}
  const before=f.document(),writes=f.calls.filter(value=>value.op==='world.field.put').length;
  for(const action of captured)action.run();await f.settle();assert.deepEqual(f.state.moves,[]);assert.deepEqual(f.document(),before);
  assert.equal(f.calls.filter(value=>value.op==='world.field.put').length,writes,'a stale remove must not submit another CAS');
});

for(const origin of['preview','native'])test(`BROKEN omitted Move/Remove is detected through the same ${origin} host-menu oracle`,async t=>{
  await assert.rejects((async()=>{const f=await fixture(t,{omitPlacementActions:true});f.clickAdd();await f.choose();const id=f.document().shortcuts.find(value=>value.entity.kind==='community').shortcutId;
    let menu;if(origin==='preview'){f.previewButton().click();menu=f.activeDialog();}else menu=f.hostMenu(id);assertPlacementMenu(menu);
  })(),/both Move and Remove/);
});

test('pins the real admission/action sources without claiming Root browser Move/Undo acceptance',t=>{
  t.diagnostic('app.ts sha256 '+createHash('sha256').update(appText).digest('hex'));
  t.diagnostic('unified-field-screen.ts sha256 '+createHash('sha256').update(screenText).digest('hex'));
});
