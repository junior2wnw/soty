import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { createOrdinaryInstalledFixture } from '../../modules/source-app/test/support/ordinary-installed.mjs';
import { STANDARD_ROOT_WORKSPACE_SOURCE } from '../../modules/source-app/server/standard-profile.mjs';
import { createSqliteRootWorkspaceHost } from '../../modules/source-app/server/sqlite-root-workspace-host.mjs';
import { createRootWorkspaceServer } from '../../modules/source-app/server/workspace-server.mjs';
import { createAppSourceState } from './app-source-state.mjs';
import { createAppSettingsState, dispatchAppSettingsIntent } from './app-settings-state.mjs';

// Actual signed Root/Apps HTTP, installed connector WS, SDK MAC probe and Source
// SQLite host. Temporary accounts/data only; no Native Human login is claimed.
for (const activateSource of [false, true]) test(`initial public Native PRIMARY1 becomes Source2 plus the exact retained Native1 through the real UI controller and signed APIs (${activateSource ? 'inactive alias atomic activation and lost ACK' : 'legacy active alias'})`, { timeout: 90000 }, async t => {
  const native = createServer((_req, response) => response.writeHead(200, { 'content-type': 'text/plain' }).end('Synthetic legacy Native'));
  await new Promise(done => native.listen(0, '127.0.0.1', done));
  t.after(() => new Promise(done => { native.closeAllConnections(); native.close(done); }));
  let nativeAlias, sourceAlias;
  const f = await createOrdinaryInstalledFixture(t, { workspace: true, realmIds: ['pair'], sourceProfile: STANDARD_ROOT_WORKSPACE_SOURCE,
    withoutRpClients: ['pair'], registrationPorts: [native.address().port], namedAppZone: 'https://ui-pair.localhost',
    appOriginTemplate: 'https://{appId}.legacy-pair.localhost',
    realmOrigins: () => ({ embed: 'https://source.ui-pair.localhost', native: 'https://native.ui-pair.localhost' }),
    connectorScript: fileURLToPath(new URL('../../scripts/soty-connector.mjs', import.meta.url)),
    nativeFactory({ realm, initialize }) {
      const host = createSqliteRootWorkspaceHost({ profile: realm.profile, databasePath: realm.databasePath, cipherKey: realm.cipherKey,
        keyId: 'initial-pair-fixture', initialize, personalWorkspaces: true, domain: { read() { return { synthetic: true }; } } });
      return createRootWorkspaceServer({ profile: realm.profile, transportKey: realm.key, connectorPort: realm.options.connectorPort,
        host, listen: { host: '127.0.0.1', port: realm.port }, ui: { document: '<!doctype html><title>Synthetic Source</title>',
          script: 'export {};', style: 'body{}', sdk: 'export {};' } });
    },
    async promoteSource({ realm, owner }) {
      let snapshot = await owner.client.extension('apps.inspect', { appId: realm.appId });
      nativeAlias = (await owner.client.extension('apps.domains.claim', { appId: realm.appId, slug: 'native',
        expectedDomainsRevision: snapshot.addresses.revision, requestId: 'initial-pair-native-name' })).receipt;
      snapshot = await owner.client.extension('apps.inspect', { appId: realm.appId });
      sourceAlias = (await owner.client.extension('apps.domains.claim', { appId: realm.appId, slug: 'source',
        expectedDomainsRevision: snapshot.addresses.revision, requestId: 'initial-pair-source-name' })).receipt;
      snapshot = await owner.client.extension('apps.inspect', { appId: realm.appId });
      await owner.client.extension('apps.publication.update', { appId: realm.appId, requestId: 'initial-pair-publish-native',
        expectedPolicyEpoch: snapshot.publication.policyEpoch, expectedTargetRevision: 1, launchPolicy: 'anyone', listed: true,
        activeDomainIds: [nativeAlias.domainId, ...(activateSource ? [] : [sourceAlias.domainId])], exposureAck: { scope: 'whole-port', targetRevision: 1,
          targetDigest: snapshot.source.digest, profile: 'soty.relay-restricted.v1' } });
    } });
  const realm = f.realms[0], read = () => f.owner.client.extension('apps.inspect', { appId: realm.appId });
  let snapshot = await read();
  assert.equal(snapshot.source.revision, 1); assert.equal(snapshot.publication.nativeEntry, undefined);
  assert.equal(snapshot.actions.canEdit, true);
  const source = createAppSourceState({ accountId: f.owner.account.accountId, appId: realm.appId, snapshot });
  t.after(() => source.dispose());
  source.patch({ profile: 'soty.selected-human-embed.v2', port: String(realm.port), entryPath: '/embed' });
  assert.equal(source.read().nativeChoices.length, activateSource ? 1 : 2);
  assert.equal(source.read().canPrepare, activateSource);
  assert.equal(snapshot.addresses.aliases.find(value => value.id === sourceAlias.domainId).active, !activateSource);
  assert.equal(source.read().draft.sourceDomainId, activateSource ? sourceAlias.domainId : null);
  const argsFor = alias => ({ appId: realm.appId, expectedAccountId: f.owner.account.accountId,
    expectedPolicyEpoch: snapshot.publication.policyEpoch, expectedTargetRevision: 1,
    source: { hostDeviceId: snapshot.source.hostDeviceId, connectorId: snapshot.source.connectorId, port: realm.port, entryPath: '/embed', profile: 'soty.selected-human-embed.v2' },
    nativeEntry: { origin: alias.origin, targetRevision: 1 },
    ...(activateSource ? { sourceEntryAction: { action: 'activate', domainId: sourceAlias.domainId } } : {}) });
  await assert.rejects(f.foreign.client.extension('apps.source.prepare', argsFor(nativeAlias)), error => /^(authentication_required|account_mismatch|owner_required)$/u.test(error.code));
  await assert.rejects(f.foreign.client.extension('apps.source.prepare', { ...argsFor(nativeAlias), expectedAccountId: f.foreign.account.accountId }),
    error => error.code === 'apps_owner_required');
  await assert.rejects(f.owner.client.extension('apps.source.prepare', argsFor(sourceAlias)), error => error.code === 'app_native_entry_origin_invalid');
  await assert.rejects(f.owner.client.extension('apps.source.prepare', { ...argsFor(nativeAlias), nativeEntry: { origin: nativeAlias.origin, targetRevision: 2 } }),
    error => /native_entry|source_target/u.test(error.code));
  assert.equal((await read()).source.revision, 1);
  source.patch({ nativeDomainId: nativeAlias.domainId });
  const api = { request: (op, args) => f.owner.client.extension(op, args, { expectedAccountId: f.owner.account.accountId }) };
  assert.deepEqual(await source.prepare({ api, isCurrent: () => true }), { status: 'ready' });
  assert.equal((await read()).addresses.aliases.find(value => value.id === sourceAlias.domainId).active, !activateSource, 'private controller CHECK never activates the alias');
  source.patch({ exposureConfirmed: true }); assert.equal(source.read().canPromote, false);
  source.patch({ nativeConfirmed: true }); const intent = source.promoteIntent();
  assert.equal(intent.args.nativeExposureAck.targetDigest, snapshot.source.digest);
  const values = new Map(); let serial = 0;
  const settingsOptions = { accountId: f.owner.account.accountId, appId: realm.appId,
    storage: { getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value) },
    locks: { request: async (_key, callback) => callback() }, randomId: () => 'initial-pair-intent-' + (++serial) };
  const settings = createAppSettingsState(settingsOptions); let result;
  if (activateSource) {
    assert.deepEqual(intent.args.sourceEntryAction, { action: 'activate', domainId: sourceAlias.domainId });
    assert.deepEqual(intent.expectedSourceEntry, { origin: sourceAlias.origin, domainId: sourceAlias.domainId });
    await assert.rejects(dispatchAppSettingsIntent({ ...intent, state: settings, isCurrent: () => true, api: { async request(op, args) {
      await api.request(op, args); throw new TypeError('Own lost atomic promotion ACK');
    } } }));
    const pending = settings.read().pending, reloaded = createAppSettingsState(settingsOptions);
    assert.deepEqual(reloaded.read().pending, pending);
    result = await dispatchAppSettingsIntent({ state: reloaded, expectedPending: pending, api, isCurrent: () => true });
    assert.equal(result.response.replayed, true); assert.equal(reloaded.read().pending, null);
  } else result = await dispatchAppSettingsIntent({ ...intent, state: settings, api, isCurrent: () => true });
  assert.equal(result.status, 'accepted'); assert.equal(settings.read().pending, null);
  const paired = await read(); assert.equal(paired.source.revision, 2); assert.equal(paired.publication.nativeEntry.origin, nativeAlias.origin);
  assert.equal(paired.addresses.aliases.find(value => value.id === sourceAlias.domainId).active, true);
  assert.equal(paired.publication.nativeEntry.target.revision, 1); assert.equal(paired.publication.nativeEntry.target.digest, snapshot.source.digest);
  const history = await f.owner.client.extension('apps.source.history', { appId: realm.appId, limit: 10 });
  assert.equal(history.targets.length, 2, 'one app with two targets, not a second card');
  assert.equal((await f.owner.client.extension('apps.list')).apps.filter(app => app.id === realm.appId).length, 1);
  await assert.rejects(f.foreign.client.extension('apps.source.promote', { ...intent.args, requestId: 'foreign-copy' }),
    error => /^(authentication_required|account_mismatch|owner_required)$/u.test(error.code));
  await assert.rejects(f.owner.client.extension('apps.source.promote', { ...intent.args, requestId: 'stale-original-epoch' }),
    error => /revision_conflict|source_preparation/u.test(error.code));
  assert.equal((await read()).source.digest, paired.source.digest);
});
