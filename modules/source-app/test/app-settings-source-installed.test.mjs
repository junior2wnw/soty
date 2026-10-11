import test from 'node:test';
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { createOrdinaryInstalledFixture } from './support/ordinary-installed.mjs';
import { STANDARD_SELECTED_SOURCE_V3 } from '../server/standard-profile.mjs';
import { createAppSourceState } from '../../../src/world/app-source-state.mjs';
import { createAppSettingsState, createAppSettingsDraftState, dispatchAppSettingsIntent } from '../../../src/world/app-settings-state.mjs';

// Real signed owner RPC, installed connector, HTTP Source probe, Apps admission
// and SQL receipts. Local storage/locks replace browser APIs only; no UI or
// production deployment is claimed by this gate.
test('owner selects installed integration for two ordinary apps; actual prepare/promote survives lost ACK and selected readback', { timeout: 120000 }, async t => {
  let promoted = 0;
  const f = await createOrdinaryInstalledFixture(t, { sourceProfile: STANDARD_SELECTED_SOURCE_V3,
    connectorScript: fileURLToPath(new URL('../../../scripts/soty-connector.mjs', import.meta.url)),
    async promoteSource({ realm, owner, reader }) {
      const { appId } = realm, accountId = owner.account.accountId;
      const api = { request: (op, args) => owner.client.extension(op, args) };
      await assert.rejects(reader.client.extension('apps.inspect', { appId }), { code: 'apps_owner_required' });
      const before = await api.request('apps.inspect', { appId });
      assert.deepEqual(before.source.integrations, [{ profile: realm.profile.schema, entryPath: '/embed' }]);
      assert.equal(before.source.profile, 'soty.relay-restricted.v1');
      const source = createAppSourceState({ accountId, appId, snapshot: before });
      source.patch({ profile: before.source.integrations[0].profile });
      assert.equal((await source.prepare({ api, isCurrent: () => true })).status, 'ready');
      const intent = source.promoteIntent(); assert.equal(intent.expectedSource.digest, realm.target.digest);
      const values = new Map(); let serial = 0, repliesLost = 0;
      const options = { accountId, appId, storage: { getItem: key => values.get(key) ?? null, setItem: (key, value) => values.set(key, value) },
        locks: { request: async (_key, fn) => fn() }, randomId: () => 'installed-ui-' + realm.realmId + '-' + (++serial) };
      const state = createAppSettingsState(options);
      await assert.rejects(dispatchAppSettingsIntent({ ...intent, state, isCurrent: () => true, api: { async request(op, args) {
        await api.request(op, args); repliesLost++; throw Error('synthetic lost HTTP reply after durable commit');
      } } }), error => { assert.equal(error.message, 'synthetic lost HTTP reply after durable commit', 'promotion error code=' + error.code); return true; });
      assert.equal(repliesLost, 1); const pending = state.read().pending; assert.equal(pending.expectedSource.profile, realm.profile.schema);
      source.dispose(); const reloaded = createAppSettingsState(options);
      const result = await dispatchAppSettingsIntent({ state: reloaded, expectedPending: reloaded.read().pending, isCurrent: () => true,
        api: { async request(op, args) {
          assert.deepEqual(args, pending.args); const response = await api.request(op, args); assert.equal(response.replayed, true); return response;
        } } });
      assert.equal(result.status, 'accepted'); assert.equal(serial, 1); assert.equal(reloaded.read().pending, null);
      const after = await api.request('apps.inspect', { appId });
      assert.equal(after.source.profile, realm.profile.schema); assert.equal(after.source.digest, realm.target.digest);
      assert.equal(after.source.integrations, undefined); assert.deepEqual(after.app.grants, before.app.grants);
      const readback = createAppSourceState({ accountId, appId, snapshot: createAppSettingsDraftState(after).read().snapshot });
      assert.equal(readback.read().dirty, false); assert.equal(readback.read().draft.profile, realm.profile.schema);
      const history = await api.request('apps.source.history', { appId });
      assert.equal(history.targets.length, 2); assert.equal(history.targets[0].profile, realm.profile.schema);
      readback.dispose(); promoted++;
    } });
  assert.equal(promoted, 2);
  for (const realm of f.realms) {
    await realm.launch(); await f.login(realm);
    const ready = await f.wire(realm.embedded + '/api/embed/session-status');
    assert.equal(ready.status, 200); assert.equal(ready.value.ready, true);
    assert.equal(realm.instance.store.db.prepare('SELECT count(*) AS n FROM native_items').get().n, 0);
  }
});
