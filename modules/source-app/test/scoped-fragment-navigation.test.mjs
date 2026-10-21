import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { createOrdinaryInstalledFixture } from './support/ordinary-installed.mjs';
import { STANDARD_ROOT_WORKSPACE_SOURCE } from '../server/standard-profile.mjs';
import { createResourceSourceProofVerifier } from '../../apps/scoped-embed/resource-proof.mjs';
import { createSourceAuthorityClient, isVerifiedRootContext } from '../../apps/scoped-embed/source-authority-client.mjs';
import { createAppLauncher, parseAppLaunchRoute, formatAppLaunchRoute } from '../../../src/world/app-launch.mjs';

const connectorScript = fileURLToPath(new URL('../../../scripts/soty-connector.mjs', import.meta.url));
const invitation = 'I'.repeat(43);
const navigation = '/embed#document=pag_x&block=hero&invite=' + invitation;
const bootToken = launch => new URL(launch.launchUrl).hash.slice(1);

test('actual signed Root/SDK keeps document navigation through boot, external launch and same-slot renewal without changing authority', { timeout: 90000 }, async t => {
  const f = await createOrdinaryInstalledFixture(t, {
    workspace: true, sourceProfile: STANDARD_ROOT_WORKSPACE_SOURCE, realmIds: ['board'], connectorScript,
    nativeFactory({ realm }) {
      const contexts = [], paths = [], nonces = new Set();
      const verifier = createResourceSourceProofVerifier({ profile: realm.profile, key: realm.key,
        consumeNonce(value) { if (nonces.has(value)) return false; nonces.add(value); return true; } });
      const readRoot = createSourceAuthorityClient({ profile: realm.profile, key: realm.key, connectorPort: realm.options.connectorPort });
      const server = createServer(async (req, res) => {
        try {
          if (req.method === 'HEAD') { res.writeHead(204, await verifier.verifyReady(req)).end(); return; }
          const context = await verifier.verify(req), signed = await readRoot({ reference: context.reference, connector: realm.profile.connector });
          assert.ok(isVerifiedRootContext(signed)); assert.deepEqual(signed, context);
          contexts.push(signed); paths.push(req.url);
          res.writeHead(200, { 'content-type': 'text/html' }).end('<!doctype html><title>Selected fixture</title>');
        } catch { res.writeHead(403).end(); }
      });
      return { server, contexts, paths,
        listen: () => new Promise(done => server.listen(realm.port, '127.0.0.1', done)),
        close: () => new Promise(done => { server.closeAllConnections(); server.close(done); }) };
    },
  });
  const [realm] = f.realms, pin = structuredClone(realm.profile), replies = [];
  const target = parseAppLaunchRoute('#' + formatAppLaunchRoute({ appId: realm.appId, path: navigation })).target;
  const launcher = createAppLauncher({ target, accountId: f.reader.account.accountId, shellUrl: f.origin + '/#mine',
    instanceId: 'main', isCurrent: () => true,
    request: async ({ expectedAccountId, ...args }) => {
      const result = await f.reader.client.extension('apps.launch', args, { expectedAccountId }); replies.push(result);
      return { ...result, url: result.launchUrl, scopedCleanup: f.reader.client.appSlotCleanup(result) };
    } });
  t.after(() => launcher.dispose());
  const url = await launcher.launch(), first = replies.at(-1), captured = launcher.scopedCapture();
  assert.equal(new URL(url).searchParams.get('path'), navigation);
  assert.equal(first.entry.path, navigation); assert.deepEqual(first.scopedSource, pin.sourceProfile);
  async function boot(launch) {
    const reply = await f.wire(realm.embedded + '/_soty/session', { body: { ticket: bootToken(launch) } });
    assert.equal(reply.status, 200); assert.equal(reply.value.entryPath, navigation);
    assert.match(reply.value.frame, /^[A-Za-z0-9_-]{43}$/u); return reply.value;
  }
  const initial = await boot(first), headers = { 'x-soty-frame': initial.frame };
  assert.equal((await f.wire(realm.embedded + '/embed?sotyFrame=' + initial.frame + navigation.slice(6))).status, 200);
  const original = realm.instance.contexts.at(-1);
  assert.deepEqual(original.entry, { domainId: first.entry.domainId, origin: realm.embedded });
  assert.equal(JSON.stringify(original).includes('pag_x'), false, 'navigation never enters the authority DTO');
  assert.equal(original.target.digest, realm.target.digest);
  assert.equal(original.humanPrincipal.subject, f.reader.account.accountId);
  assert.deepEqual(realm.instance.paths, ['/embed']);
  const popup = { opener: {}, closed: false, location: { replace(value) { this.url = value; } }, close() { this.closed = true; } };
  assert.equal(await launcher.openExternal(() => popup), 'opened');
  assert.equal(new URL(popup.location.url).searchParams.get('path'), navigation);
  assert.equal(launcher.scopedCapture().handle, captured.handle, 'external entry cannot replace the visible slot');
  const external = await boot(replies.at(-1)); assert.notEqual(external.frame, initial.frame);
  assert.equal((await f.wire(realm.embedded + '/embed', { headers })).status, 200, 'original frame remains live');
  const args = { appId: realm.appId, handle: captured.handle, requestId: 'fragment-renew-request-0001' };
  const renewed = await f.reader.client.extension('apps.scoped.renew', args);
  const prepared = launcher.prepareRenewal({ ...renewed, url: renewed.launchUrl, scopedCleanup: f.reader.client.appSlotCleanup(renewed) });
  assert.equal(new URL(prepared.url).searchParams.get('path'), navigation);
  assert.equal(new URL(prepared.url).searchParams.get('mode'), 'renew');
  const repeated = await f.reader.client.extension('apps.scoped.renew', args);
  assert.equal(repeated.entry.path, navigation); assert.equal(repeated.scopedCloseHandle, renewed.scopedCloseHandle);
  const candidate = await boot(renewed); assert.equal(candidate.frame, initial.frame);
  assert.equal(launcher.entry().path, navigation); assert.equal(launcher.scopedCapture().handle, captured.handle);
  assert.deepEqual(realm.profile, pin, 'no admission, profile or pin mutation');
  for (const path of ['/embed?x=1#document=pag_x', '/other#document=pag_x', '/%65mbed#x', '/a/../embed#x',
    '/_soty/boot#document=pag_x', '//other.example/embed#x', 'https://other.example/embed#x', '/embed#%0a', '/embed#' + 'x'.repeat(2049)]) {
    await assert.rejects(f.reader.client.extension('apps.launch', { appId: realm.appId, path }));
  }
  await assert.rejects(f.foreign.client.extension('apps.launch', { appId: realm.appId, path: navigation }),
    'a document/invite fragment grants no Root app access');
  await assert.rejects(f.foreign.client.extension('apps.scoped.renew', args));
  await assert.rejects(f.reader.client.extension('apps.scoped.renew', { ...args, requestId: 'fragment-retarget-request', path: '/embed#other' }));
  assert.equal((await f.wire(realm.embedded + '/_soty/session', { body: { ticket: invitation } })).status, 403);
  assert.ok([401, 403].includes((await f.wire(realm.embedded + '/embed', { headers: { 'x-soty-frame': invitation } })).status));
  assert.ok(realm.instance.paths.every(path => path === '/embed'));
});

test('ordinary selected Source uses the same unchanged SDK for fragment launch and renewal', { timeout: 90000 }, async t => {
  const f = await createOrdinaryInstalledFixture(t, { realmIds: ['board'], connectorScript }), [realm] = f.realms;
  const launch = await f.reader.client.extension('apps.launch', { appId: realm.appId, path: navigation });
  const boot = await f.wire(realm.embedded + '/_soty/session', { body: { ticket: bootToken(launch) } });
  assert.equal(boot.status, 200); assert.equal(boot.value.entryPath, navigation); assert.equal(boot.value.frame, undefined);
  await f.login(realm);
  const renewed = await f.reader.client.extension('apps.scoped.renew', {
    appId: realm.appId, handle: launch.scopedCloseHandle, requestId: 'ordinary-fragment-renew-0001' });
  assert.equal(renewed.entry.path, navigation); assert.deepEqual(renewed.scopedSource, launch.scopedSource);
  const candidate = await f.wire(realm.embedded + '/_soty/session', { body: { ticket: bootToken(renewed) } });
  assert.equal(candidate.status, 200); assert.equal(candidate.value.entryPath, navigation);
  assert.equal(candidate.value.renewal, true);
});
