import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { nativeHttpFixture, nativeIdentity, good } from './support/native-capability-http.mjs';
import { materialManifest } from '../../modules/materials/contract.mjs';
import { pngFixture } from '../../modules/world/test/avatar-fixtures.mjs';

const sha = value => createHash('sha256').update(value).digest('hex');
const id = prefix => `${prefix}_${randomUUID().replaceAll('-', '')}`;
async function fixture(t) {
  const f = await nativeHttpFixture(t), owner = nativeIdentity('Canvas review owner'), viewer = nativeIdentity('Canvas review viewer'), accounts = new Map(), devices = new Map();
  for (const actor of [owner, viewer]) { const boot = await f.bootstrap(actor); accounts.set(actor, boot.accountId); devices.set(actor, boot.deviceId); }
  const call = (actor, op, args = {}) => f.call(actor, op, { expectedAccountId: accounts.get(actor), ...args });
  const noteArgs = (noteId, expectedRevision, overrides = {}) => ({ noteId, expectedRevision, mutationId: id('note_mutation'),
    title: 'Canvas real note', body: 'Private Source content shared through one object', items: [], color: 'plain', pinned: false, state: 'active', ...overrides });
  const createNote = async () => good(await call(owner, 'notes.put', noteArgs(id('note'), 0)));
  const objectArgs = (providerId, ref, overrides = {}) => ({ requestId: id('canvas_create'), binding: { providerId, ref },
    presentation: { mask: { shape: 'rect' }, fontSize: 16 }, visibility: { audience: 'public', accountIds: [], communityIds: [] },
    hidden: { owner: false, others: false }, protection: { kind: 'none' }, ...overrides });
  const createObject = async (providerId, ref, overrides) => good(await call(owner, 'canvas.objects.create', objectArgs(providerId, ref, overrides))).current;
  const updateObject = async (object, overrides) => good(await call(owner, 'canvas.objects.update', { objectId: object.objectId,
    requestId: id('canvas_update'), expectedRevision: object.revision, ...overrides })).current;
  const project = async (actor, objectId) => good(await call(actor, 'canvas.objects.project', { objectIds: [objectId] })).items[0];
  const content = (actor, objectId, part = 'note', extra = {}) => call(actor, 'canvas.objects.content', { objectId, part, ...extra });
  const wait = async (actor, cursor, objectId) => good(await call(actor, 'platform.live.wait', { waitMs: 0,
    ...(cursor ? { cursor } : {}), watch: [{ kind: 'object', id: objectId }] }));
  return { ...f, get app() { return f.app; }, owner, viewer, accounts, devices, call, noteArgs, createNote, objectArgs, createObject, updateObject, project, content, wait };
}

test('review: actual signed Root delegates one real note; public reads do not grant direct Source, owner rights, copied proofs or plaintext password locks', { timeout: 20000 }, async t => {
  const f = await fixture(t), note = await f.createNote(), object = await f.createObject('soty.notes', note.noteId);
  assert.equal((await f.call(f.viewer, 'notes.get', { noteId: note.noteId })).ok, false, 'object visibility does not grant direct notes RPC scope');
  const publicProjection = await f.project(f.viewer, object.objectId);
  assert.equal(publicProjection.available, true); assert.equal(publicProjection.view.note.noteId, note.noteId); assert.equal(publicProjection.view.canEdit, false);
  const copiedActor = { accountId: f.accounts.get(f.viewer), deviceId: f.devices.get(f.viewer) };
  assert.equal(f.app.locals.connectService.isActorActive(copiedActor), true, 'the copied fields name a real current installation');
  assert.throws(() => f.app.locals.canvasObjectsService.project({ actor: copiedActor, objectIds: [object.objectId] }), { code: 'connect_authority_actor_invalid' },
    'only the original signed issued actor may enter the Root authority fence');
  assert.equal((await f.call(f.viewer, 'canvas.objects.create', f.objectArgs('soty.notes', note.noteId))).ok, false, 'a public object does not let its viewer republish the private Source as owner');
  assert.equal(good(await f.content(f.viewer, object.objectId)).note.body, 'Private Source content shared through one object');
  assert.equal(f.app.locals.canvasObjectsService.project({ objectIds: [object.objectId] }).items[0].available, true, 'actual Root anonymous host gate');
  let leaked;
  const native = f.app.locals.canvasObjectsService.withObjectRead({ objectId: object.objectId }, context => {
    leaked = context;
    for (const copy of [{ ...context }, JSON.parse(JSON.stringify(context))])
      assert.throws(() => f.app.locals.notesService.canvas.readNote({ context: copy }), { code: 'canvas_object_authority_required' });
    return f.app.locals.notesService.canvas.readNote({ context });
  });
  assert.equal(native.note.noteId, note.noteId);
  assert.throws(() => f.app.locals.notesService.canvas.readNote({ context: leaked }), { code: 'canvas_object_authority_required' }, 'the original branded frame also expires');
  assert.equal((await f.call(f.viewer, 'canvas.objects.update', { objectId: object.objectId, expectedRevision: 1,
    requestId: id('no_owner'), hidden: { owner: true, others: true } })).ok, false);
  const protectedAttempt = await f.call(f.owner, 'canvas.objects.create', f.objectArgs('soty.notes', note.noteId, { protection: { kind: 'password' } }));
  assert.equal(protectedAttempt.ok, false); assert.equal(protectedAttempt.error.code, 'canvas_plaintext_password_forbidden');
  assert.equal(good(await f.call(f.owner, 'canvas.objects.list')).objects.length, 1, 'rejected lock never creates a plaintext-looking protected object');
});

test('review: hide/restore/private/remove/restart and missing Source/provider deny real public content while the owner retains Source data', { timeout: 20000 }, async t => {
  const f = await fixture(t), note = await f.createNote(); let object = await f.createObject('soty.notes', note.noteId);
  object = await f.updateObject(object, { hidden: { owner: false, others: true } });
  const hidden = await f.project(f.viewer, object.objectId); assert.equal(hidden.available, false);
  assert.doesNotMatch(JSON.stringify(hidden), /Canvas real note|Private Source|noteId|preview/u);
  assert.equal((await f.content(f.viewer, object.objectId)).ok, false);
  assert.equal(good(await f.call(f.owner, 'notes.get', { noteId: note.noteId })).note.body, 'Private Source content shared through one object');
  object = await f.updateObject(object, { hidden: { owner: false, others: false } });
  assert.equal(good(await f.content(f.viewer, object.objectId)).note.noteId, note.noteId);
  object = await f.updateObject(object, { visibility: { audience: 'private', accountIds: [], communityIds: [] } });
  assert.equal((await f.project(f.viewer, object.objectId)).available, false); assert.equal((await f.content(f.viewer, object.objectId)).ok, false);
  await f.restart({ enabled: true });
  assert.equal((await f.content(f.viewer, object.objectId)).ok, false);
  assert.equal(good(await f.content(f.owner, object.objectId)).note.noteId, note.noteId);
  object = good(await f.call(f.owner, 'canvas.objects.get', { objectId: object.objectId })).object;
  object = await f.updateObject(object, { visibility: { audience: 'public', accountIds: [], communityIds: [] } });
  good(await f.call(f.owner, 'notes.put', f.noteArgs(note.noteId, 1, { state: 'trashed' })));
  assert.equal((await f.project(f.viewer, object.objectId)).available, false); assert.equal((await f.content(f.viewer, object.objectId)).ok, false);
  good(await f.call(f.owner, 'notes.put', f.noteArgs(note.noteId, 2)));
  assert.equal((await f.project(f.viewer, object.objectId)).available, true);
  f.app.locals.canvasObjectsService.unregisterProvider('soty.notes');
  assert.equal((await f.project(f.viewer, object.objectId)).available, false); assert.equal((await f.content(f.viewer, object.objectId)).ok, false);
  await f.restart({ enabled: true });
  assert.equal((await f.project(f.viewer, object.objectId)).available, true);
  good(await f.call(f.owner, 'canvas.objects.remove', { objectId: object.objectId, expectedRevision: object.revision, requestId: id('remove') }));
  assert.equal((await f.project(f.viewer, object.objectId)).available, false); assert.equal((await f.content(f.viewer, object.objectId)).ok, false);
});

test('review: native Materials bytes use the same ObjectService gates without changing the direct owner-scoped Source API', { timeout: 20000 }, async t => {
  const f = await fixture(t), bytes = pngFixture(), materialId = id('material'), chunks = [{ bytes: bytes.length, sha256: sha(bytes) }];
  good(await f.call(f.owner, 'materials.begin', { materialId, mutationId: id('material_begin'), name: 'Review image.png',
    kind: 'image', mediaType: 'image/png', size: bytes.length, fileSha256: sha(bytes) }));
  good(await f.call(f.owner, 'materials.chunk', { materialId, index: 0, sha256: sha(bytes), data: bytes.toString('base64') }));
  const committed = good(await f.call(f.owner, 'materials.commit', { materialId, mutationId: id('material_commit'), manifestSha256: sha(materialManifest(bytes.length, chunks)) }));
  const object = await f.createObject('soty.materials', materialId);
  assert.equal((await f.call(f.viewer, 'materials.read', { materialId, index: 0 })).ok, false);
  assert.equal((await f.project(f.viewer, object.objectId)).view.kind, 'image');
  const content = good(await f.content(f.viewer, object.objectId, 'chunk', { index: 0 }));
  assert.equal(sha(Buffer.from(content.data, 'base64')), sha(bytes));
  assert.equal((await f.content(f.viewer, object.objectId, 'chunk', { index: -1 })).ok, false);
  assert.equal((await f.content(f.viewer, object.objectId, 'chunk', { index: 999999 })).ok, false);
  const beforeTrash = await f.wait(f.viewer, null, object.objectId);
  good(await f.call(f.owner, 'materials.trash', { materialId, mutationId: id('material_trash'), expectedRevision: committed.material.revision }));
  assert.equal((await f.project(f.viewer, object.objectId)).available, false); assert.equal((await f.content(f.viewer, object.objectId, 'chunk', { index: 0 })).ok, false);
  const trashed = await f.wait(f.viewer, beforeTrash.cursor, object.objectId);
  assert.equal(trashed.changed, true, 'the real Source trash ACK must wake its already-visible object viewer');
  good(await f.call(f.owner, 'materials.restore', { materialId, mutationId: id('material_restore'), expectedRevision: committed.material.revision + 1 }));
  const restored = await f.wait(f.viewer, trashed.cursor, object.objectId); assert.equal(restored.changed, true);
  const privateObject = await f.updateObject(object, { visibility: { audience: 'private', accountIds: [], communityIds: [] } });
  const hidden = await f.wait(f.viewer, restored.cursor, object.objectId); assert.equal(hidden.changed, true);
  good(await f.call(f.owner, 'materials.trash', { materialId, mutationId: id('private_material_trash'), expectedRevision: committed.material.revision + 2 }));
  assert.equal((await f.wait(f.viewer, hidden.cursor, object.objectId)).changed, false, 'later private Source work must not continue to wake a removed viewer');
  assert.equal((await f.content(f.viewer, privateObject.objectId, 'chunk', { index: 0 })).ok, false);
});

test('review: the real Root Source guard detects the old always-allowed dummy provider, rather than passing any binding', { timeout: 20000 }, async t => {
  const f = await fixture(t), noteId = id('missing_note');
  const checkMissingSource = async () => {
    const before = good(await f.call(f.owner, 'canvas.objects.list')).objects.length;
    const response = await f.call(f.owner, 'canvas.objects.create', f.objectArgs('soty.notes', noteId));
    assert.equal(response.ok, false, 'a missing real Source must never be accepted');
    assert.equal(good(await f.call(f.owner, 'canvas.objects.list')).objects.length, before);
  };
  await checkMissingSource();
  // Intentional old-behavior control in this isolated Root instance only.
  f.app.locals.canvasObjectsService.unregisterProvider('soty.notes');
  f.app.locals.canvasObjectsService.registerProvider({ providerId: 'soty.notes',
    withBindingAuthority(_request, callback) { return callback({ allowed: true, protection: { kind: 'none' } }); }, project() { return { kind: 'note' }; } });
  await assert.rejects(checkMissingSource(), error => error.code === 'ERR_ASSERTION');
});

test('review: committed edits to a publicly shared native note must invalidate the other viewer without a canvas metadata write', { timeout: 20000 }, async t => {
  const f = await fixture(t), note = await f.createNote(), object = await f.createObject('soty.notes', note.noteId),
    ownerBefore = await f.wait(f.owner, null, object.objectId), viewerBefore = await f.wait(f.viewer, null, object.objectId);
  const rejected = await f.call(f.owner, 'notes.put', f.noteArgs(note.noteId, 999, { body: 'This write must fail' })); assert.equal(rejected.ok, false);
  assert.equal((await f.wait(f.owner, ownerBefore.cursor, object.objectId)).changed, false, 'failed CAS cannot emit a successful Source change');
  good(await f.call(f.owner, 'notes.put', f.noteArgs(note.noteId, 1, { body: 'Committed shared note edit' })));
  assert.equal(good(await f.content(f.viewer, object.objectId)).note.body, 'Committed shared note edit', 'real Source committed and authorized content is fresh');
  const ownerAfter = await f.wait(f.owner, ownerBefore.cursor, object.objectId), viewerAfter = await f.wait(f.viewer, viewerBefore.cursor, object.objectId);
  assert.equal(ownerAfter.changed, true, 'Source after-commit onChange is running');
  const reshaped = await f.updateObject(object, { presentation: { mask: { shape: 'ellipse' }, fontSize: 18 } });
  const changed = await f.wait(f.viewer, viewerAfter.cursor, object.objectId);
  assert.equal(changed.changed, true, 'the same real Root watch receives object changes');
  assert.equal(viewerAfter.changed, true, 'a shared Source edit must notify the same visible object viewer');
  await f.updateObject(reshaped, { visibility: { audience: 'private', accountIds: [], communityIds: [] } });
  const removed = await f.wait(f.viewer, changed.cursor, object.objectId); assert.equal(removed.changed, true);
  good(await f.call(f.owner, 'notes.put', f.noteArgs(note.noteId, 2, { body: 'Later private edit must not signal former viewer' })));
  assert.equal((await f.wait(f.viewer, removed.cursor, object.objectId)).changed, false);
  assert.equal((await f.content(f.viewer, object.objectId)).ok, false);
});

test('review: Root rejects asynchronous or oversized trusted provider output before sending content and never lets JSON choose provider code', { timeout: 20000 }, async t => {
  const f = await fixture(t), providers = f.app.locals.canvasObjectsService;
  assert.equal((await f.call(f.owner, 'canvas.objects.create', f.objectArgs('unregistered.provider', id('ref')))).ok, false);
  assert.equal((await f.call(f.owner, 'canvas.objects.create', { ...f.objectArgs('soty.notes', id('missing')), url: 'https://untrusted.invalid', command: 'execute' })).ok, false);
  providers.registerProvider({ providerId: 'review.bounds', withBindingAuthority(_request, callback) { return callback({ allowed: true, protection: { kind: 'none' } }); },
    project() { return { schema: 'review.bounds.v1', text: 'x'.repeat(65537) }; } });
  const oversized = await f.createObject('review.bounds', id('bounded_ref'));
  const denied = await f.call(f.viewer, 'canvas.objects.project', { objectIds: [oversized.objectId] }); assert.equal(denied.ok, false);
  assert.equal(denied.error.code, 'canvas_projection_too_large'); assert.doesNotMatch(JSON.stringify(denied), /x{100}/u);
  providers.registerProvider({ providerId: 'review.async', withBindingAuthority(_request, callback) { return callback({ allowed: true, protection: { kind: 'none' } }); },
    project() { return Promise.resolve({ text: 'Late content must not escape' }); } });
  const asynchronous = await f.createObject('review.async', id('async_ref'));
  const asyncDenied = await f.call(f.viewer, 'canvas.objects.project', { objectIds: [asynchronous.objectId] }); assert.equal(asyncDenied.ok, false);
  assert.equal(asyncDenied.error.code, 'canvas_async_authority'); assert.doesNotMatch(JSON.stringify(asyncDenied), /Late content/u);
});
