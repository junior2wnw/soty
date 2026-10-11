import test from 'node:test';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { createOrdinaryInstalledFixture } from './support/ordinary-installed.mjs';
import { STANDARD_SELECTED_SOURCE_V3 } from '../server/standard-profile.mjs';
import { createCurrentContextSourceApplication } from '../../../server/current-context-source.js';
import { captureExternalApplications } from '../../../server/external-applications.js';

// Actual Root grant/coordinator -> Apps HTTP/WS -> Source BFF -> Native SQL.
// Two app identities and independent Native ports/databases, one Root factory.
test('standard3 current context: two Native apps, current HTTP projection and independent revocation through one Root', { timeout: 120000 }, async t => {
  let entries;
  const f = await createOrdinaryInstalledFixture(t, { sourceProfile: STANDARD_SELECTED_SOURCE_V3, libraryNativeOwner: true,
    connectorScript: fileURLToPath(new URL('../../../scripts/soty-connector.mjs', import.meta.url)),
    externalApplications(realms) {
      const applications = realms.map(({ profile }) => createCurrentContextSourceApplication({ appId: profile.appId,
        target: profile.target, resource: profile.resource, sourceProfile: profile.sourceProfile }));
      entries = captureExternalApplications(applications); return applications;
    } });
  const [board, library] = f.realms, caps = f.root().locals.capabilitiesService;
  assert.notEqual(board.appId, library.appId); assert.notEqual(board.port, library.port);
  const principal = (await f.reader.client.extension('access.principals.create', {
    expectedAccountId: f.reader.account.accountId, label: 'Synthetic two-app current context' })).principal;
  const grant = (await f.reader.client.extension('access.grants.issue', { expectedAccountId: f.reader.account.accountId,
    principalId: principal.id, capabilities: entries.map(row => ({ capabilityId: row.contract.capabilityId, version: 1 })),
    resources: entries.flatMap(row => row.catalog.resources), effects: [], recipients: entries.flatMap(row => row.catalog.recipients),
    expiresAt: Date.now() + 600000, budget: { unit: 'invocations', limit: 30 } })).grant;
  const issued = await f.reader.client.extension('access.credentials.issue', {
    expectedAccountId: f.reader.account.accountId, grantId: grant.id, audience: f.origin });
  const credential = { token: issued.token, audience: f.origin }, wires = [];
  let sequence = 0;
  const query = (realm, handle = realm.lastLaunch.scopedCloseHandle) => caps.external.query({
    actor: caps.authenticateCredential(credential), reference: entries[realm.index].contract,
    idempotencyKey: 'standard_context_read_' + (++sequence), input: { handle } });
  async function denied(action) {
    let result; try { result = await action(); } catch (error) { assert.equal(typeof error.code, 'string'); return; }
    assert.notEqual(result.invocation.status, 'succeeded'); assert.equal(result.resultUnavailable, true); assert.equal(result.result, undefined);
  }
  for (const realm of f.realms) {
    realm.instance.store.db.prepare('UPDATE native_resources SET title=? WHERE id=?').run('Native\n' + realm.realmId, 'selected');
    realm.instance.server.prependListener('request', req => {
      if (req.url !== '/api/embed/query') return;
      const parts = []; req.on('data', part => parts.push(part)); req.on('end', () => {
        const body = JSON.parse(Buffer.concat(parts).toString('utf8'));
        wires.push({ appId: realm.appId, method: req.method, body });
      });
    });
    await realm.launch(); await denied(() => query(realm)); // Native login/consent required.
    await f.login(realm);
    const continued = await f.wire(realm.embedded + '/api/embed/session-continue', { body: { requestId: 'context-ready-' + realm.realmId + '-0001' } });
    assert.equal(continued.status, 200); assert.equal(continued.value.ready, true);
    const current = await f.reader.client.extension('apps.scoped.context', { appId: realm.appId, handle: realm.lastLaunch.scopedCloseHandle });
    assert.equal(current.sourceSession.ready, true, 'actual Source continuation ACK, not a fixture ready flag');
    const ordinary = await f.wire(realm.embedded + '/api/embed/context');
    assert.equal(ordinary.status, 200); assert.equal(ordinary.value.ready, true); assert.equal(ordinary.value.title, undefined);
    const result = await query(realm);
    assert.equal(result.invocation.status, 'succeeded'); assert.equal(result.resultUnavailable, false);
    assert.deepEqual(result.result, { title: 'Native ' + realm.realmId, role: realm === board ? 'editor' : 'owner', revision: 1 });
  }
  assert.ok(wires.length >= 2);
  for (const wire of wires) {
    assert.equal(wire.method, 'POST'); assert.deepEqual(wire.body.input, { operation: 'soty.current_context' });
    assert.deepEqual(Object.keys(wire.body).sort(), ['input', 'requestId']); assert.match(wire.body.requestId, /^[A-Za-z0-9_.:-]{16,128}$/u);
  }
  await denied(() => query(library, board.lastLaunch.scopedCloseHandle));
  const longTitle = ' \n' + 'A'.repeat(237) + '😀😀\n tail  ';
  library.instance.store.db.prepare('UPDATE native_resources SET title=? WHERE id=?').run(longTitle, 'selected');
  const bounded = (await query(library)).result.title;
  assert.equal(bounded, 'A'.repeat(237) + '😀'); assert.equal(bounded.isWellFormed(), true);
  assert.equal(library.instance.store.db.prepare('SELECT title FROM native_resources WHERE id=?').get('selected').title, longTitle, 'display projection never edits Native title');
  board.instance.store.revokeMembership('selected', 'native-participant');
  await denied(() => query(board));
  assert.equal((await query(library)).result.role, 'owner', 'another app keeps its independent Native access');
  let received, release;
  const responseHeld = new Promise(resolve => { received = resolve; });
  library.instance.server.prependListener('request', (req, res) => {
    if (req.url !== '/api/embed/query') return;
    const end = res.end.bind(res);
    res.end = (...args) => { release = () => end(...args); received(); return res; };
  });
  t.after(() => release?.());
  const pending = query(library), lateDenied = denied(() => pending);
  await Promise.race([responseHeld, pending.then(() => { throw new Error('context_native_response_not_held'); })]);
  await f.reader.client.extension('apps.scoped.close', { appId: library.appId, handle: library.lastLaunch.scopedCloseHandle });
  release(); await lateDenied;
  await denied(() => query(library));
  for (const realm of f.realms) assert.equal(realm.instance.store.db.prepare('SELECT count(*) AS n FROM native_items').get().n, 0);
});
