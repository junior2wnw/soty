import { createRequire } from 'node:module';
import { appId, assertApps, textId } from './protocol.mjs';
import { runtimeTargetDigest, selectedRuntimeProfile, supportedRuntimeProfile } from './schema.mjs';
import { synchronous } from './engagement-transaction.mjs';

export const appCoverOperations = new Set(['apps.cover.status','apps.cover.prepare','apps.cover.prepareFromVersion',
  'apps.cover.commit','apps.cover.restore','apps.cover.remove','apps.cover.read','apps.cover.versions','apps.cover.cancel']);
const require = createRequire(import.meta.url), maximumInput = 320 * 1024;
const expected = ['requestId','expectedTargetRevision','expectedPolicyEpoch','expectedCoverRevision'];
const shapes = Object.freeze({
  status: [[],['requestId','draftId']], prepare: [[...expected,'mime','bytesBase64'],['crop']],
  prepareFromVersion: [[...expected,'versionId'],['crop']], commit: [['requestId','draftId'],[]],
  restore: [[...expected,'versionId'],[]], remove: [[...expected],['mode']],
  read: [[],['versionId','width']], versions: [[],['limit']], cancel: [['requestId','draftId'],[]],
});
function inputBytes(value) {
  assertApps(typeof value === 'string' && value.length > 0 && value.length <= 4 * Math.ceil(maximumInput / 3)
    && value.length % 4 === 0 && /^[A-Za-z0-9+/]+={0,2}$/u.test(value), 'cover_input_limit');
  const bytes = Buffer.from(value, 'base64');
  assertApps(bytes.length > 0 && bytes.length <= maximumInput && bytes.toString('base64') === value, 'cover_input_limit');
  return bytes;
}

/** Only a configured host constructs this adapter. Lazy require keeps native
 * Sharp out of the feature-OFF Apps import path; Node24 supports sync ESM. */
export function createAppCoverAdapter({ db, assertActor, withAuthorityFence, resolveEntry, requireScopedTarget, decoder, clock, limits }) {
  assertApps(typeof assertActor === 'function' && typeof withAuthorityFence === 'function'
    && typeof resolveEntry === 'function' && typeof requireScopedTarget === 'function'
    && decoder && typeof decoder.decode === 'function', 'apps_cover_configuration_invalid', 503);
  const { createCoverStore } = require('../../cover-art/server/index.mjs');
  const instanceId = db.prepare('SELECT store_id FROM app_cover_usage WHERE id=1').get()?.store_id;
  function withAuthority(actor, callback) {
    assertActor(actor); let active = true, entered = false, outcome;
    try {
      const returned = withAuthorityFence(() => {
        assertApps(active && !entered, 'apps_authority_fence_invalid', 500); entered = true; assertActor(actor);
        outcome = synchronous(callback(), 'apps_async_authority'); assertActor(actor); return outcome;
      }, actor);
      synchronous(returned, 'apps_async_authority'); assertApps(entered, 'apps_authority_fence_invalid', 500); return outcome;
    } finally { active = false; }
  }
  function readTarget(actor, id) {
    assertApps(db.isTransaction, 'apps_transaction_required', 500); assertActor(actor);
    const app = db.prepare('SELECT * FROM local_apps WHERE id=?').get(appId(id));
    if (!app || app.state !== 'enabled') return null;
    const policy = db.prepare('SELECT * FROM app_publications WHERE app_id=?').get(id);
    const target = policy && db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=? AND revision=?').get(id, policy.active_target_revision);
    assertApps(policy && target && policy.owner_account_id === app.owner_account_id && target.owner_account_id === app.owner_account_id
      && supportedRuntimeProfile(target.profile) && target.digest === runtimeTargetDigest({ appId: id, revision: target.revision,
        ownerAccountId: app.owner_account_id, connectorKey: target.connector_key, port: target.port,
        entryPath: target.entry_path, profile: target.profile })
      && db.prepare('SELECT owner_account_id FROM app_devices WHERE connector_key=?').get(target.connector_key)?.owner_account_id === app.owner_account_id,
    'apps_registry_corrupt', 500);
    let entry = resolveEntry({ actor, appId: id });
    if (!entry && policy.launch_policy === 'anyone') {
      const domain = db.prepare(`SELECT d.id FROM app_domains d JOIN app_publication_domains p ON p.domain_id=d.id
        AND p.app_id=d.app_id AND p.owner_account_id=d.owner_account_id
        WHERE d.app_id=? AND d.role='alias' AND d.state='bound' ORDER BY d.created_at,d.id LIMIT 1`).get(id);
      if (domain) entry = resolveEntry({ actor, appId: id, domainId: domain.id });
    }
    const canManage = app.owner_account_id === actor.accountId;
    if ((canManage || entry) && selectedRuntimeProfile(target.profile)) requireScopedTarget({ appId: id, revision: target.revision,
      ownerAccountId: target.owner_account_id, connectorKey: target.connector_key, port: target.port,
      entryPath: target.entry_path, profile: target.profile, digest: target.digest });
    return { ownerAccountId: app.owner_account_id, targetRevision: target.revision, policyEpoch: policy.policy_epoch,
      state: 'active', canManage, canRead: Boolean(entry) };
  }
  const store = createCoverStore({ db, kind: 'app', instanceId, withAuthority, readTarget, decoder, clock, limits });
  return Object.freeze({
    async execute({ op, args, actor }) {
      assertApps(appCoverOperations.has(op), 'unsupported_operation');
      const method = op.slice('apps.cover.'.length), [required, optional] = shapes[method];
      assertApps(args && typeof args === 'object' && !Array.isArray(args) && Object.hasOwn(args,'appId')
        && required.every(key => Object.hasOwn(args,key))
        && Object.keys(args).every(key => key === 'appId' || required.includes(key) || optional.includes(key)), 'cover_arguments_invalid');
      const { appId: id, ...rest } = args, input = { ...rest, targetId: appId(id) };
      if (input.draftId !== undefined) textId(input.draftId);
      if (input.versionId !== undefined) textId(input.versionId);
      if (method === 'status' && input.requestId !== undefined) assertApps(/^cover1\.[0-9]{13}\.[a-f0-9]{32}$/u.test(input.requestId), 'cover_request_invalid');
      if (method === 'read' && input.width !== undefined) assertApps(Number.isSafeInteger(input.width) && input.width > 0 && input.width <= 960, 'cover_width_invalid');
      if (method === 'prepare') {
        const bytes = inputBytes(input.bytesBase64); delete input.bytesBase64;
        return store.prepareUpload(actor,{ ...input, bytes });
      }
      if (method === 'read') {
        const result = store.read(actor,input); if (!result.image) return result;
        const { bytes, ...image } = result.image;
        return { ...result, image: { ...image, byteCount: bytes.length, base64: bytes.toString('base64') } };
      }
      return store[method](actor,input);
    },
    close() { store.dispose(); },
  });
}
