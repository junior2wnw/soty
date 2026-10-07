import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { resolve, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createPeremetrikaSignedRoot } from './support/peremetrika-signed-root.mjs';
import { createPeremetrikaReadonlyQueryAdapter } from '../server/peremetrika-readonly-adapter.mjs';

const sourceRoot = process.env.SOTY_PEREMETRIKA_SOURCE_ROOT ? resolve(process.env.SOTY_PEREMETRIKA_SOURCE_ROOT) : null;
const available = sourceRoot && existsSync(join(sourceRoot, 'server/soty-selected-read.mjs'))
  && existsSync(join(sourceRoot, 'tests/support/soty-read-fixture.mjs'));
const sourceFixture = available ? (await import(pathToFileURL(join(sourceRoot, 'tests/support/soty-read-fixture.mjs')))).createSotyReadFixture : null;
const actual = { skip: !available && 'Explicit reviewed Peremetrika Source checkout not supplied; not an actual Source pass.', timeout: 30000 };

test('closed host scope rejects owner-default/arbitrary origin/getter options before network', () => {
  let called = 0;
  const resource = { registryId: 'soty', tenantId: 'tenant', appId: 'app', environmentId: 'production', resourceId: 'resource', sourceActorId: 'owner', sourceMode: 'owner-session' };
  assert.throws(() => createPeremetrikaReadonlyQueryAdapter({ resource, selection: { kind: 'page', pageId: 'pag_one' },
    expectedProfile: { id: 'profile', version: 1, digest: 'a'.repeat(64) }, sourceRelease: { id: 'release', version: 1, digest: 'a'.repeat(64) } }));
  assert.throws(() => createPeremetrikaReadonlyQueryAdapter({ resource: { ...resource, sourceMode: 'agent-token', get extra() { called++; return 'x'; } } }));
  assert.equal(called, 0);
});

test('actual signed Root query reads selected Native page, exact retry is metadata, restart keeps Native grant, no source writes', actual, async t => {
  const native = await sourceFixture(t), page = await native.createPage('Selected'), foreign = await native.createPage('Foreign');
  const credential = await native.grant([page.pageId]), original = await native.storedBytes();
  const root = await createPeremetrikaSignedRoot(t, native), query = await root.connect({ kind: 'page', pageId: page.pageId }, credential);
  const routesBefore = native.routes().length;
  const first = await query.query('selected-page-one', { blockId: 'note' });
  assert.equal(first.status, 200); assert.equal(first.body.result.metadata.pageId, page.pageId);
  const block = JSON.parse(first.body.result.content.blockJson);
  assert.equal(block.id, 'note'); assert.match(block.body, /Synthetic selected private body/u);
  assert.equal(JSON.stringify(first.body).includes(credential.token), false); assert.equal(JSON.stringify(first.body).includes(foreign.pageId), false);
  assert.equal(native.routes().slice(routesBefore).every(route => route.method === 'GET' && route.path.startsWith('/api/v1/soty-read/pages/' + page.pageId)), true);
  const count = native.routes().length, replay = await query.query('selected-page-one', { blockId: 'note' });
  assert.equal(replay.status, 200); assert.equal(replay.body.resultUnavailable, true); assert.equal(native.routes().length, count);
  for (const input of [{ token: credential.token }, { url: native.origin }, { pageId: foreign.pageId }, { command: 'edit' }])
    assert.equal((await query.query('bad-args-' + Object.keys(input)[0], input)).status, 400);
  assert.equal(native.routes().length, count);
  await native.restart(); await query.restartRoot();
  assert.equal((await query.query('after-both-restarts')).status, 200);
  assert.ok(original.equals(await native.storedBytes()));
  await native.revoke(credential.grantId);
  const denied = await query.query('after-native-revoke'); assert.equal(denied.status, 403);
  assert.equal(JSON.stringify(denied.body).includes('Synthetic selected private body'), false);
});

test('actual signed Root library query follows current Native participant source-page ACL', actual, async t => {
  const native = await sourceFixture(t), page = await native.createPage('Library');
  const library = await native.library(page.pageId), participant = await native.participant(page.pageId, 'reader');
  const root = await createPeremetrikaSignedRoot(t, native), query = await root.connect({ kind: 'library-version', itemId: library.id, version: 1, sourcePageId: page.pageId }, participant);
  const result = await query.query('selected-library-one', { blockId: 'note' });
  assert.equal(result.status, 200); assert.equal(result.body.result.selectedVersion.version, 1);
  await native.membership(page.pageId, participant.memberId, null);
  assert.equal((await query.query('library-after-membership-revoke')).status, 403);
});

for (const revoke of ['root', 'native']) test('actual ' + revoke + ' revoke during Source response denies private result', actual, async t => {
  const native = await sourceFixture(t), page = await native.createPage('Held');
  const credential = await native.grant([page.pageId]), root = await createPeremetrikaSignedRoot(t, native);
  const query = await root.connect({ kind: 'page', pageId: page.pageId }, credential);
  const held = native.holdResponse('/api/v1/soty-read/pages/' + page.pageId + '/blocks/note');
  const pending = query.query('held-' + revoke, { blockId: 'note' }); await held.reached;
  if (revoke === 'root') await query.revokeRoot(); else await native.revoke(credential.grantId);
  held.release(); const result = await pending;
  assert.equal(result.status >= 400, true); assert.equal(JSON.stringify(result.body).includes('Synthetic selected private body'), false);
});
