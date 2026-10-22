import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';
import { createWorldService } from '../server/index.mjs';
import { migrateWorld as frozen6 } from './support/world6-schema-before.mjs';
import { signedFixture, good, denied, PROJECT } from './support/world6-signed-fixture.mjs';
import { attachmentHttpFixture } from './support/attachment-http-fixture.mjs';
import { ATTACHMENT_LIMITS as L, attachmentManifest } from '../attachment-contract.mjs';
import { pngFixture } from './avatar-fixtures.mjs';
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
function snapshot(path) {
  const db = new DatabaseSync(path, { readOnly: true });
  try { return Object.fromEntries(db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT GLOB 'world_chat_attach*' ORDER BY name").all().map(({ name }) => {
    const rows = db.prepare(`SELECT * FROM "${name}"`).all(); if (name === 'world_meta') for (const row of rows) if (row.key === 'schema_version') row.value = 'storage-version';
    return [name, rows];
  })); } finally { db.close(); }
}

test('reader7 keeps6 capability OFF; explicit6→7 preserves actual profile/avatar/history and frozen6 refuses7 unchanged', async t => {
  const f = await signedFixture(t), [alice] = f.actors;
  await f.publish(alice, { bytes: pngFixture(36, 24) });
  const beforeCover = (await f.status(alice)).scope.coverRevision;
  const before = snapshot(f.worldPath), pixels = good(await f.readBytes(alice)).image;
  assert.equal(f.world.schemaVersion, 6); assert.equal((await f.profile(alice)).chatAttachmentsVersion, undefined);
  f.reopen(); assert.equal(f.world.schemaVersion, 6); assert.deepEqual(snapshot(f.worldPath), before);
  f.reopen({ allowWorld7Migration: true }); assert.equal(f.world.schemaVersion, 7);
  assert.deepEqual(snapshot(f.worldPath), before); assert.equal((await f.profile(alice)).chatAttachmentsVersion, 1);
  assert.deepEqual(good(await f.readBytes(alice)).image, pixels);
  assert.equal((await f.status(alice)).scope.coverRevision, beforeCover);
  f.world.close(); f.connect.close();
  const beforeBytes = sha(readFileSync(f.worldPath)), db = new DatabaseSync(f.worldPath);
  try { assert.throws(() => frozen6(db, PROJECT), /world_schema_unsupported/u); } finally { db.close(); }
  assert.equal(sha(readFileSync(f.worldPath)), beforeBytes);
  f.reopen(); assert.equal(f.world.schemaVersion, 7); assert.deepEqual(good(await f.readBytes(alice)).image, pixels);
});

test('format7 refuses wrong metadata without repair and6 partial attachment fragments cannot be silently completed', async t => {
  const f = await signedFixture(t, { world7: true }); f.world.close(); f.connect.close();
  const db = new DatabaseSync(f.worldPath); db.prepare("UPDATE world_meta SET value='6' WHERE key='schema_version'").run(); db.close();
  const before = sha(readFileSync(f.worldPath));
  assert.throws(() => createWorldService({ databasePath: f.worldPath, projectId: PROJECT }), /world_schema_metadata_mismatch/u);
  assert.equal(sha(readFileSync(f.worldPath)), before);
  assert.throws(() => createWorldService({ databasePath: ':memory:', projectId: PROJECT, allowWorld7Migration: 'true' }), { code: 'invalid_world7_migration_mode' });
  const fresh = new DatabaseSync(':memory:'); frozen6(fresh, PROJECT);
  fresh.exec('CREATE TABLE world_chat_attachments(incomplete TEXT)');
  const { migrateWorld } = await import('../server/schema.mjs');
  assert.throws(() => migrateWorld(fresh, PROJECT, { allowWorld7Migration: true }), /world_attachment_partial_schema/u);
  assert.equal(fresh.prepare('PRAGMA user_version').get().user_version, 6); fresh.close();
});

for (const kind of ['room', 'direct']) test(`signed ${kind}: original25MiB files, immutable chunks, publication/replay, exact recipient and revoke deny`, async t => {
  const f = await attachmentHttpFixture(t), [alice, bob, carol] = f.actors;
  let left = f.base(alice), right = f.base(bob), outsider = f.base(carol), send = 'world.chat.send', remove = 'world.chat.remove';
  if (kind === 'direct') {
    const relationshipId = await f.contact(alice, bob);
    const thread = good(await f.call(alice, 'world.direct.open', { expectedAccountId: alice.accountId, peerAccountId: bob.accountId, expectedRelationshipId: relationshipId, requestId: 'file_direct_open' })).current;
    const args = { threadId: thread.threadId, expectedRelationshipId: relationshipId };
    left = { ...args, expectedAccountId: alice.accountId }; right = { ...args, expectedAccountId: bob.accountId }; outsider = { ...args, expectedAccountId: carol.accountId };
    send = 'world.direct.chat.send'; remove = 'world.direct.chat.remove';
  }
  const bytes = Buffer.alloc(L.fileBytes, 73), ref = await f.upload(alice, bytes, 'Оригинал фото 🏡.bin', left);
  assert.equal(ref.size, 25 * 1024 * 1024); assert.equal(ref.chunks, 50); assert.equal(ref.fileSha256, sha(bytes));
  const body = { ...left, clientId: 'file_message_original', text: '', attachments: [ref.attachmentId] };
  const message = good(await f.call(alice, send, body)).message;
  assert.equal(good(await f.call(alice, send, body)).message.messageId, message.messageId);
  denied(await f.call(alice, send, { ...body, text: 'A different request' }), 'request_conflict');
  const listing = good(await f.call(bob, kind === 'direct' ? 'world.direct.chat.list' : 'world.chat.list', kind === 'direct' ? right : { communityId: right.communityId }));
  assert.equal(JSON.stringify(listing).includes('data'), false); assert.equal(listing.messages.at(-1).attachments[0].mediaType, 'application/octet-stream');
  assert.equal(sha(await f.readChunks(bob, ref, right)), sha(bytes));
  if (kind === 'direct') {
    denied(await f.call(carol, 'world.attachment.read', { ...outsider, attachmentId: ref.attachmentId, index: 0 }), 'direct_not_found');
    good(await f.call(bob, 'contacts.block', { peerAccountId: alice.accountId }));
    denied(await f.call(bob, 'world.attachment.read', { ...right, attachmentId: ref.attachmentId, index: 0 }), 'direct_not_found');
  } else {
    for (let index = 0; index < 9; index++) good(await f.call(alice, 'world.attachment.begin', { ...left, attachmentId: `quota_reservation_${index}`, name: 'Reserved original', size: L.fileBytes, mediaType: '' }));
    denied(await f.call(alice, 'world.attachment.begin', { ...left, attachmentId: 'account_quota_excess', name: 'Too much', size: L.fileBytes, mediaType: '' }), 'attachment_storage_quota');
    good(await f.call(bob, 'world.membership.leave', { communityId: left.communityId }));
    denied(await f.call(bob, 'world.attachment.read', { ...right, attachmentId: ref.attachmentId, index: 0 }), 'community_membership_required');
    good(await f.call(alice, remove, { communityId: left.communityId, messageId: message.messageId }));
    denied(await f.call(alice, 'world.attachment.read', { ...left, attachmentId: ref.attachmentId, index: 0 }), 'attachment_unavailable');
  }
});

test('chunk admission/manifest tamper, pending TTL and quota reservation stay bounded before bytes', async t => {
  const f = await attachmentHttpFixture(t), [alice] = f.actors, scope = { ...f.base(alice), attachmentId: 'tamper_file' }, bytes = Buffer.alloc(L.chunkBytes + 1, 12);
  good(await f.call(alice, 'world.attachment.begin', { ...scope, name: 'safe.bin', size: bytes.length, mediaType: '' }));
  const first = bytes.subarray(0, L.chunkBytes), last = bytes.subarray(L.chunkBytes), chunk = { ...scope, index: 0, sha256: sha(first), data: first.toString('base64') };
  denied(await f.call(alice, 'world.attachment.chunk', { ...chunk, index: 1, sha256: sha(last), data: last.toString('base64') }), 'attachment_chunk_order');
  good(await f.call(alice, 'world.attachment.chunk', chunk)); good(await f.call(alice, 'world.attachment.chunk', chunk));
  denied(await f.call(alice, 'world.attachment.chunk', { ...chunk, sha256: '0'.repeat(64) }), 'attachment_integrity_failed');
  denied(await f.call(alice, 'world.attachment.complete', { ...scope, manifestSha256: '0'.repeat(64) }), 'attachment_chunk_missing');
  f.advance(L.pendingMs + 1);
  denied(await f.call(alice, 'world.attachment.chunk', { ...scope, index: 1, sha256: sha(last), data: last.toString('base64') }), 'attachment_unavailable');
  good(await f.call(alice, 'world.attachment.begin', { ...scope, attachmentId: 'after_expiry', name: 'new.bin', size: 0, mediaType: '' }));
  const db = new DatabaseSync(f.worldPath, { readOnly: true }); try { assert.equal(db.prepare('SELECT COUNT(*) AS n FROM world_chat_attachment_chunks').get().n, 0); } finally { db.close(); }
});

test('signed direct file grants do not transfer to a re-added contact on the same historical thread', async t => {
  const f = await attachmentHttpFixture(t), [alice, bob] = f.actors;
  const relationshipId = await f.contact(alice, bob);
  const first = good(await f.call(alice, 'world.direct.open', { expectedAccountId: alice.accountId, peerAccountId: bob.accountId,
    expectedRelationshipId: relationshipId, requestId: 'file_first_relationship' })).current;
  const scope = actor => ({ expectedAccountId: actor.accountId, threadId: first.threadId, expectedRelationshipId: relationshipId });
  const original = Buffer.from('Original file belongs to the original contact grant');
  const ref = await f.upload(alice, original, 'original.bin', scope(alice));
  const sent = good(await f.call(alice, 'world.direct.chat.send', { ...scope(alice), clientId: 'file_old_relationship', text: 'Retained history', attachments: [ref.attachmentId] })).message;
  assert.equal(good(await f.call(bob, 'world.attachment.status', { ...scope(bob), attachmentId: ref.attachmentId })).attachment.state, 'published');
  assert.deepEqual(await f.readChunks(bob, ref, scope(bob)), original);
  good(await f.call(bob, 'contacts.remove', { relationshipId }));
  denied(await f.call(bob, 'world.attachment.read', { ...scope(bob), attachmentId: ref.attachmentId, index: 0 }), 'direct_not_found');
  f.advance(1); const nextRelationshipId = await f.contact(alice, bob);
  assert.notEqual(nextRelationshipId, relationshipId);
  const reopened = good(await f.call(alice, 'world.direct.open', { expectedAccountId: alice.accountId, peerAccountId: bob.accountId,
    expectedRelationshipId: nextRelationshipId, requestId: 'file_second_relationship' })).current;
  assert.equal(reopened.threadId, first.threadId);
  const next = actor => ({ ...scope(actor), expectedRelationshipId: nextRelationshipId });
  // Explicit reopen retains text history; it does not renew the old file grant.
  assert.equal(good(await f.call(bob, 'world.direct.chat.list', next(bob))).messages[0].messageId, sent.messageId);
  for (const actor of [alice, bob]) for (const op of ['world.attachment.status', 'world.attachment.read']) {
    await t.test(`${actor.label}: ${op} refuses old bytes under the new relationship`, async () => {
      denied(await f.call(actor, op, { ...next(actor), attachmentId: ref.attachmentId, ...(op.endsWith('.read') ? { index: 0 } : {}) }), 'attachment_unavailable');
    });
  }
  const current = await f.upload(alice, Buffer.from('Current relationship file'), 'current.bin', next(alice));
  good(await f.call(alice, 'world.direct.chat.send', { ...next(alice), clientId: 'file_new_relationship', text: '', attachments: [current.attachmentId] }));
  assert.deepEqual(await f.readChunks(bob, current, next(bob)), Buffer.from('Current relationship file'));
});
