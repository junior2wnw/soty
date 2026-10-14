import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { createOrdinaryInstalledFixture } from './support/ordinary-installed.mjs';
import { STANDARD_ROOT_WORKSPACE_SOURCE } from '../server/standard-profile.mjs';
import { createResourceSourceProofVerifier } from '../../apps/scoped-embed/resource-proof.mjs';
import { createSourceAuthorityClient, isVerifiedRootContext } from '../../apps/scoped-embed/source-authority-client.mjs';

test('four installed Root workspaces use original Connect identity with only three RP registrations', { timeout: 120000 }, async t => {
  const f = await createOrdinaryInstalledFixture(t, {
    workspace: true, sourceProfile: STANDARD_ROOT_WORKSPACE_SOURCE,
    realmIds: ['board', 'library', 'third', 'planner'], withoutRpClients: ['planner'],
    connectorScript: fileURLToPath(new URL('../../../scripts/soty-connector.mjs', import.meta.url)),
    nativeFactory({ realm }) {
      const nonces = new Set(), contexts = [];
      const verifier = createResourceSourceProofVerifier({ profile: realm.profile, key: realm.key,
        consumeNonce(value) { if (nonces.has(value)) return false; nonces.add(value); return true; } });
      const readRoot = createSourceAuthorityClient({ profile: realm.profile, key: realm.key, connectorPort: realm.options.connectorPort });
      const server = createServer(async (req, res) => {
        try {
          if (req.method === 'HEAD') { res.writeHead(204, await verifier.verifyReady(req)).end(); return; }
          const context = await verifier.verify(req);
          const signed = await readRoot({ reference: context.reference, connector: realm.profile.connector });
          assert.ok(isVerifiedRootContext(signed)); assert.deepEqual(signed, context); contexts.push(signed);
          res.writeHead(200, { 'content-type': 'text/html' }).end('<!doctype html><title>Native workspace</title>');
        } catch (error) { res.writeHead(403, { 'content-type': 'application/json' }).end(JSON.stringify({ code: error.code ?? 'fixture_denied' })); }
      });
      return { server, contexts, readRoot,
        listen: () => new Promise(done => server.listen(realm.port, '127.0.0.1', done)),
        close: () => new Promise(done => { server.closeAllConnections(); server.close(done); }) };
    },
  });
  for (const realm of f.realms) {
    const launch = await f.reader.client.extension('apps.launch', { appId: realm.appId });
    const boot = await f.wire(realm.embedded + '/_soty/session', { body: { ticket: new URL(launch.launchUrl).hash.slice(1) } });
    assert.equal(boot.status, 200);
    const page = await f.wire(realm.embedded + '/embed', { headers: { 'x-soty-frame': boot.value.frame } });
    assert.equal(page.status, 200, JSON.stringify(page.value));
    const context = realm.instance.contexts.at(-1);
    assert.equal(context.humanPrincipal.subject, f.reader.account.accountId);
    assert.equal(context.humanPrincipal.clientGeneration, realm.target.revision);
    assert.equal(context.humanPrincipal.clientId, realm.profile.clientId);
  }
  const planner = f.realms.at(-1), context = planner.instance.contexts.at(-1);
  await assert.rejects(f.foreign.client.extension('apps.launch', { appId: planner.appId }));
  await f.owner.client.extension('apps.update', { appId: planner.appId, grants: { accountIds: [], communityIds: [] } });
  await assert.rejects(planner.instance.readRoot({ reference: context.reference, connector: planner.profile.connector }));
  assert.equal(planner.instance.contexts.length, 1, 'revoked reference cannot reach Native again');
  const board = f.realms[0], boardContext = board.instance.contexts.at(-1);
  const recovery = await f.reader.client.prepareRecovery();
  await f.reader.client.confirmRecovery(recovery);
  await f.reader.client.revokeDevice(boardContext.rootPrincipal.deviceId);
  await assert.rejects(board.instance.readRoot({ reference: boardContext.reference, connector: board.profile.connector }));
  assert.equal(Object.entries(f.oauthCounts).filter(([key]) => /^(token|userinfo):/u.test(key)).reduce((sum, [, count]) => sum + count, 0), 0);
});
