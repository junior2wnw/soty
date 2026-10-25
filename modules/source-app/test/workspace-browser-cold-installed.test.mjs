import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { createSourceWorkspaceClient } from '../browser/index.mjs';
import { createSourceAppBff, createSourceNativeAuthorityPort, readVerifiedRootContext, STANDARD_ROOT_WORKSPACE_SOURCE } from '../server/index.mjs';
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
  if (initialize) store.createResource({ id: 'personal', incarnationId: 'one', title: 'Personal workspace' });
  const inspect = (proof, binding) => {
    const held = proofs.get(proof); assert.ok(held); assert.equal(binding.identity.subject, held.subject);
    if (binding.operation !== 'root-session') {
      const row = store.db.prepare('SELECT n.active,n.expires_at FROM native_sessions n JOIN source_sessions s ON s.native_session_hash=n.id_hash WHERE s.id_hash=? AND s.active=1').get(binding.sessionIdHash);
      assert.ok(row?.active === 1 && row.expires_at > clock());
    }
    return held;
  };
  const native = createSourceNativeAuthorityPort({ capture: async () => { throw Error('Native cookie login is not available'); },
    async captureRoot(binding, proof) {
      const root = readVerifiedRootContext(proof, profile.digest); assert.equal(root.humanPrincipal.subject, binding.identity.subject);
      assert.equal(digest(root.rootPrincipal), digest(binding.rootPrincipal));
      const value = Object.freeze({}); proofs.set(value, { subject: root.humanPrincipal.subject }); return value;
    },
    withCurrent(proof, binding, action) { inspect(proof, binding); return action(); },
    commitRootSession(proof, binding) {
      inspect(proof, binding); assert.equal(store.inTransaction(), true);
      const principalId = 'person-' + digest(binding.identity.subject).slice(0, 24), anchor = digest([principalId, binding.rootReferenceDigest]);
      store.db.prepare('INSERT OR IGNORE INTO native_principals VALUES(?,?)').run(principalId, realm.realmId);
      store.db.prepare('INSERT OR IGNORE INTO native_sessions VALUES(?,?,1,1,?)').run(anchor, principalId, binding.rootExpiresAt);
      return { nativeSessionHash: anchor, principalId, resourceId: 'personal' };
    },
    currentContext(proof, binding) { const held = inspect(proof, binding); currentCalls++;
      return { key: held.subject, generation: 1, title: 'Personal workspace', role: 'owner', revision: 1, expiresAt: binding.rootExpiresAt }; },
    read(proof, binding, args) { inspect(proof, binding); assert.equal(args.input.operation, 'notes.list'); return { requestId: args.requestId, notes: [] }; },
  });
  const { digest: _digest, ...raw } = profile;
  const bff = createSourceAppBff({ profile: raw, transportKey: realm.key, connectorPort: realm.options.connectorPort, storage: store.storage, native, clock });
  const server = createServer((req, res) => void bff.handleRequest(req, res));
  return { server, store, contextCalls: () => currentCalls, advance: ms => { offset += ms; },
    listen: () => new Promise(done => server.listen(realm.port, '127.0.0.1', done)),
    close: async () => { bff.close(); server.closeAllConnections(); await new Promise(done => server.close(done)); store.close(); } };
}
async function fixture(t) {
  const f = await createOrdinaryInstalledFixture(t, { workspace: true, realmIds: ['cold', 'other'], withoutRpClients: ['cold'], sourceProfile: STANDARD_ROOT_WORKSPACE_SOURCE,
    connectorScript: fileURLToPath(new URL('../../../scripts/soty-connector.mjs', import.meta.url)), nativeFactory });
  const realm = f.realms[0], launch = await f.reader.client.extension('apps.launch', { appId: realm.appId });
  const boot = await f.wire(realm.embedded + '/_soty/session', { body: { ticket: new URL(launch.launchUrl).hash.slice(1) } });
  assert.equal(boot.status, 200); assert.equal(typeof boot.value.frame, 'string');
  const paths = []; let live = true, afterContinue = null;
  const client = createSourceWorkspaceClient({ href: realm.embedded + '/embed?sotyFrame=' + boot.value.frame, isCurrent: () => live,
    fetch: async (path, init) => {
      paths.push(path); const response = await f.wire(realm.embedded + path, { method: init.method, body: JSON.parse(init.body), headers: Object.fromEntries(new Headers(init.headers)) });
      if (path.endsWith('/session-continue') && afterContinue) { assert.equal(response.status, 200); const callback = afterContinue; afterContinue = null; await callback(); }
      return new Response(response.text, { status: response.status, headers: response.headers });
    } });
  return { f, realm, client, paths, retireAfterContinue: () => { afterContinue = () => { live = false; }; } };
}

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
