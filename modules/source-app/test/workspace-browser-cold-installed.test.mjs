import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { createSourceWorkspaceClient } from '../browser/index.mjs';
import { createSourceAppBff, createSourceNativeAuthorityPort, isSourceNativeReceiptRecoveryPort, readVerifiedRootContext, STANDARD_ROOT_WORKSPACE_SOURCE } from '../server/index.mjs';
import { digest } from '../server/wire.mjs';
import { createOrdinaryAppStore } from '../examples/ordinary-app/store.mjs';
import { selectedResourceProfile } from '../../apps/scoped-embed/resource-profile.mjs';
import { createOrdinaryInstalledFixture } from './support/ordinary-installed.mjs';

// Small Native SQL host for a new-empty personal field. The Root, connector,
// signed authority, private Source cookie broker and maintained BFF are real.
// No Native login/cookie, pre-acquired Source session or RP client is installed.
function nativeFactory({ realm, initialize }) {
  const profile = selectedResourceProfile(realm.profile), proofs = new WeakMap();
  let offset = 0, currentCalls = 0;
  const clock = () => Date.now() + offset;
  const store = createOrdinaryAppStore({ databasePath: realm.databasePath, realmId: realm.realmId, key: realm.cipherKey, keyId: 'cold-browser', initialize, clock });
  const inspect = (proof, binding) => {
    const held = proofs.get(proof); assert.ok(held); assert.equal(binding.identity.subject, held.subject);
    if (binding.operation !== 'root-session') {
      const row = store.db.prepare('SELECT n.active,n.expires_at FROM native_sessions n JOIN source_sessions s ON s.native_session_hash=n.id_hash WHERE s.id_hash=? AND s.active=1 AND s.principal_id=? AND s.resource_id=?').get(binding.sessionIdHash, held.principalId, held.resourceId);
      assert.ok(row?.active === 1 && row.expires_at > clock());
    }
    return held;
  };
  const native = createSourceNativeAuthorityPort({ capture: async () => { throw Error('Native cookie login is not available'); },
    async captureRoot(binding, proof) {
      const root = readVerifiedRootContext(proof, profile.digest); assert.equal(root.humanPrincipal.subject, binding.identity.subject);
      assert.equal(digest(root.rootPrincipal), digest(binding.rootPrincipal));
      const value = Object.freeze({}), suffix = digest(binding.identity.subject).slice(0, 24);
      proofs.set(value, { subject: root.humanPrincipal.subject, principalId: 'person-' + suffix, resourceId: 'personal-' + suffix }); return value;
    },
    withCurrent(proof, binding, action) { inspect(proof, binding); return action(); },
    commitRootSession(proof, binding) {
      const held = inspect(proof, binding); assert.equal(store.inTransaction(), true);
      const principalId = held.principalId, anchor = digest([principalId, binding.rootReferenceDigest]);
      store.db.prepare('INSERT OR IGNORE INTO native_principals VALUES(?,?)').run(principalId, realm.realmId);
      store.db.prepare('INSERT OR IGNORE INTO native_resources VALUES(?,?,?,?,0)').run(held.resourceId, 'one', realm.realmId, 'Personal workspace');
      store.db.prepare('INSERT OR IGNORE INTO native_sessions VALUES(?,?,1,1,?)').run(anchor, principalId, binding.rootExpiresAt);
      return { nativeSessionHash: anchor, principalId, resourceId: held.resourceId };
    },
    currentContext(proof, binding) { const held = inspect(proof, binding); currentCalls++;
      const revision = store.db.prepare('SELECT COUNT(*)+1 AS n FROM native_items WHERE resource_id=?').get(held.resourceId).n;
      return { key: held.resourceId, generation: 1, title: 'Personal workspace', role: 'owner', revision, expiresAt: binding.rootExpiresAt }; },
    read(proof, binding, args) { const held = inspect(proof, binding); assert.equal(args.input.operation, 'notes.list');
      return { requestId: args.requestId, notes: store.db.prepare('SELECT id,title FROM native_items WHERE resource_id=?').all(held.resourceId) }; },
    execute(proof, binding, args, final) {
      assert.equal(args.input.operation, 'notes.create');
      return store.tx(() => final.commit(() => {
        const held = inspect(proof, binding), inputDigest = digest(args.input);
        const old = store.db.prepare('SELECT * FROM native_receipts WHERE principal_id=? AND request_id=?').get(held.principalId, args.requestId);
        if (old) { assert.equal(old.input_digest, inputDigest); return JSON.parse(old.result_json).original; }
        const itemId = held.principalId + ':' + args.requestId;
        store.db.prepare('INSERT INTO native_items VALUES(?,?,?,?,?)').run(itemId, held.resourceId, args.input.title, 1, clock());
        const original = { requestId: args.requestId, inputDigest, outcome: 'committed', receipt: { accepted: true, etag: 'opaque-domain-v1' },
          contextAfter: final.contextAfter(), workspaceReceipt: { schema: 'soty.workspace-receipt.v1',
            effects: [{ kind: 'created', resourceType: 'browser-note', resourceId: itemId }],
            receipt: { verificationMethod: 'domain_read', artifacts: [{ type: 'browser-note', id: itemId }] } } };
        // Declared test-host private envelope in the existing immutable JSON
        // column: no silent change to the ordinary example's strict SQL format.
        const envelope = { schema: 'soty.browser-fixture-receipt.v1', original, binding: final.receiptBinding(original) };
        store.db.prepare('INSERT INTO native_receipts VALUES(?,?,?,?,?)').run(held.resourceId, held.principalId, args.requestId, inputDigest, JSON.stringify(envelope));
        return original;
      }));
    },
    readProof(proof, binding, args, recovery) {
      assert.equal(isSourceNativeReceiptRecoveryPort(recovery), true);
      return store.tx(() => {
        const held = inspect(proof, binding);
        const row = store.db.prepare('SELECT * FROM native_receipts WHERE principal_id=? AND request_id=?').get(held.principalId, args.requestId);
        if (!row) return { requestId: args.requestId, inputDigest: args.input.inputDigest, outcome: 'not_applied' };
        assert.equal(row.input_digest, args.input.inputDigest);
        const envelope = JSON.parse(row.result_json); assert.equal(envelope.schema, 'soty.browser-fixture-receipt.v1');
        return recovery.recover(envelope.original, envelope.binding);
      });
    },
  });
  const { digest: _digest, ...raw } = profile;
  const bff = createSourceAppBff({ profile: raw, transportKey: realm.key, connectorPort: realm.options.connectorPort, storage: store.storage, native, clock });
  const server = createServer((req, res) => void bff.handleRequest(req, res));
  return { server, store, contextCalls: () => currentCalls, advance: ms => { offset += ms; },
    listen: () => new Promise(done => server.listen(realm.port, '127.0.0.1', done)),
    close: async () => { bff.close(); server.closeAllConnections(); await new Promise(done => server.close(done)); store.close(); } };
}
async function fixture(t) {
  const f = await createOrdinaryInstalledFixture(t, { workspace: true, realmIds: ['cold'], withoutRpClients: ['cold'], sourceProfile: STANDARD_ROOT_WORKSPACE_SOURCE,
    connectorScript: fileURLToPath(new URL('../../../scripts/soty-connector.mjs', import.meta.url)), nativeFactory });
  const realm = f.realms[0], launch = await f.reader.client.extension('apps.launch', { appId: realm.appId });
  const boot = await f.wire(realm.embedded + '/_soty/session', { body: { ticket: new URL(launch.launchUrl).hash.slice(1) } });
  assert.equal(boot.status, 200); assert.equal(typeof boot.value.frame, 'string');
  const paths = []; let live = true, afterContinue = null;
  const makeClient = frame => createSourceWorkspaceClient({ href: realm.embedded + '/embed?sotyFrame=' + frame, isCurrent: () => live,
    fetch: async (path, init) => {
      paths.push(path); const response = await f.wire(realm.embedded + path, { method: init.method, body: JSON.parse(init.body), headers: Object.fromEntries(new Headers(init.headers)) });
      if (path.endsWith('/session-continue') && afterContinue) { assert.equal(response.status, 200); const callback = afterContinue; afterContinue = null; await callback(); }
      return new Response(response.text, { status: response.status, headers: response.headers });
    } });
  const client = makeClient(boot.value.frame);
  return { f, realm, client, paths, retireAfterContinue: () => { afterContinue = () => { live = false; }; },
    async newFrame(signer = f.reader) {
      if (signer === f.reader) await f.reader.client.extension('apps.scoped.close', { appId: realm.appId, handle: launch.scopedCloseHandle });
      const next = await signer.client.extension('apps.launch', { appId: realm.appId });
      const ready = await f.wire(realm.embedded + '/_soty/session', { body: { ticket: new URL(next.launchUrl).hash.slice(1) } });
      assert.equal(ready.status, 200); return makeClient(ready.value.frame);
    } };
}

test('legacy Native absent receipt is not exposed as not applied by the BFF', {timeout:120000},async t=>{
  const {client}=await fixture(t);
  await client.connect({requestId:'cold-legacy-receipt-connect'});
  await assert.rejects(client.recoverReceipt({requestId:'cold-legacy-receipt-missing',contextRef:client.context().context.ref,inputDigest:'0'.repeat(64)}),
    error=>error.code==='source_receipt_unknown'&&error.status===409&&error.effectUnknown===true);
  client.dispose();
});

test('one SDK connect cold-starts an installed Root frame with no Source/Native cookie or RP registration, then resumes', { timeout: 120000 }, async t => {
  const { f, realm, client, paths } = await fixture(t);
  assert.equal(realm.instance.store.db.prepare('SELECT COUNT(*) n FROM source_sessions').get().n, 0);
  assert.equal((await client.connect({ requestId: 'cold-installed-connect-001' })).title, 'Personal workspace');
  assert.deepEqual(paths, ['/api/embed/session-continue', '/api/embed/query']);
  assert.equal(realm.instance.store.db.prepare('SELECT COUNT(*) n FROM source_sessions').get().n, 1);
  assert.equal((await client.connect({ requestId: 'cold-installed-resume-001' })).title, 'Personal workspace');
  assert.equal(realm.instance.store.db.prepare('SELECT COUNT(*) n FROM source_sessions').get().n, 1, 'valid resume retains the existing finite session');
  assert.equal(Object.entries(f.oauthCounts).filter(([name]) => /^(token|userinfo):/u.test(name)).reduce((sum, [, count]) => sum + count, 0), 0);
  await f.owner.client.extension('apps.update', { appId: realm.appId, grants: { accountIds: [], communityIds: [] } });
  await assert.rejects(client.connect({ requestId: 'cold-installed-revoked-001' }));
  assert.equal(client.context(), null); client.dispose();
});

test('profile departure between successful continuation and context read cannot expose the preceding profile', { timeout: 120000 }, async t => {
  const { realm, client, paths, retireAfterContinue } = await fixture(t);
  retireAfterContinue(); await assert.rejects(client.connect({ requestId: 'cold-installed-departed-001' }), /source_client_context_changed/);
  assert.deepEqual(paths, ['/api/embed/session-continue']); assert.equal(realm.instance.contextCalls(), 0);
  assert.equal(client.context(), null); client.dispose();
});

for (const fault of ['expiry', 'native-revoke']) test('installed SDK connect denies ' + fault + ' and clears the cached workspace', { timeout: 120000 }, async t => {
  const { realm, client, paths } = await fixture(t);
  await client.connect({ requestId: 'cold-installed-before-' + fault });
  const before = paths.length;
  if (fault === 'expiry') realm.instance.advance(310000);
  else realm.instance.store.db.prepare('UPDATE native_sessions SET active=0,generation=generation+1').run();
  await assert.rejects(client.connect({ requestId: 'cold-installed-after-' + fault }));
  assert.equal(client.context(), null); assert.equal(paths.slice(before).includes('/api/embed/query'), false);
  assert.equal(realm.instance.store.db.prepare('SELECT COUNT(*) n FROM source_sessions').get().n, 1);
  client.dispose();
});

test('installed browser recovery after Native restart and a new Root frame retains an opaque original receipt and isolates another person', { timeout: 120000 }, async t => {
  const { f, realm, client, paths, newFrame } = await fixture(t);
  await client.connect({ requestId: 'browser-recovery-initial-connect' });
  const intent = await client.prepareInvoke({ requestId: 'browser-recovery-original-write', input: { operation: 'notes.create', title: 'Original private note' } });
  realm.dropPath = '/api/embed/invoke';
  await assert.rejects(client.invoke(intent), error => error.effectUnknown === true);
  const originalBytes = realm.instance.store.db.prepare('SELECT result_json FROM native_receipts').get().result_json;
  const original = JSON.parse(originalBytes).original;
  client.dispose(); await realm.restart();
  const next = await newFrame(); await next.connect({ requestId: 'browser-recovery-new-connect' });
  assert.notEqual(next.context().context.ref, intent.contextRef);
  const later = await next.prepareInvoke({ requestId: 'browser-recovery-later-write', input: { operation: 'notes.create', title: 'Current private note' } });
  await next.invoke(later);
  const before = paths.filter(path => path === '/api/embed/invoke').length;
  const recovered = await next.recoverReceipt(intent);
  assert.equal(recovered.outcome, 'committed'); assert.deepEqual(recovered.receipt, { accepted: true, etag: 'opaque-domain-v1' });
  assert.deepEqual(recovered.contextAfter, original.contextAfter); assert.equal(recovered.recovery.contextAfter.revision, 3);
  assert.equal(realm.instance.store.db.prepare('SELECT result_json FROM native_receipts WHERE request_id=?').get(intent.requestId).result_json, originalBytes);
  const current = await next.query({ requestId: 'browser-recovery-current-read', input: { operation: 'notes.list' } });
  assert.equal(current.notes.length, 2); assert.equal(paths.filter(path => path === '/api/embed/invoke').length, before);
  await f.grantBrowser(f.foreign.account.accountId);
  const foreign = await newFrame(f.foreign); await foreign.connect({ requestId: 'browser-recovery-foreign-connect' });
  assert.equal((await foreign.query({ requestId: 'browser-recovery-foreign-read', input: { operation: 'notes.list' } })).notes.length, 0);
  await assert.rejects(foreign.recoverReceipt(intent), error => error.effectUnknown === true);
  assert.equal(realm.instance.store.db.prepare('SELECT COUNT(*) n FROM native_items').get().n, 2);
  next.dispose(); foreign.dispose();
});
