import { assertApps, appId, cleanGrants, textId } from './protocol.mjs';
import { runtimeTargetDigest, supportedRuntimeProfile, SCOPED_RUNTIME_PROFILE } from './schema.mjs';
import { createEngagementTransaction, synchronous } from './engagement-transaction.mjs';

/** Host-only port: signed Connect owns the outer installation fence. World and
 * Apps remain locked through a synchronous downstream commit. A runtime tuple
 * pin deliberately does not attest executable source-code or a release image. */
export function createAppAuthorityPort({ db, assertActor, withAuthorityFence, resolveEntry, requireScopedTarget }) {
  const run = createEngagementTransaction({ db, assertActor, withAuthorityFence, responseBytes: 2 * 1024 * 1024,
    busyCode: 'apps_authority_busy', responseCode: 'apps_authority_response_too_large' });
  return function withAppAuthority(request, callback) {
    assertApps(request && typeof request === 'object' && !Array.isArray(request)
      && Object.keys(request).every(key => ['actor', 'appId', 'mode', 'domainId', 'path'].includes(key)), 'invalid_arguments');
    const id = appId(request.appId), mode = request.mode ?? 'owner';
    assertApps(['owner', 'participant'].includes(mode) && typeof callback === 'function', 'invalid_arguments');
    if (request.domainId !== undefined) textId(request.domainId);
    return run(request.actor, actor => {
      const app = db.prepare('SELECT * FROM local_apps WHERE id=?').get(id);
      assertApps(app && app.state === 'enabled', 'app_unavailable', 404);
      if (mode === 'owner') assertApps(app.owner_account_id === actor.accountId, 'apps_owner_required', 403);
      let entry = resolveEntry({ actor, appId: id, ...(request.domainId === undefined ? {} : { domainId: request.domainId }),
        ...(request.path === undefined ? {} : { path: request.path }) });
      const policy = db.prepare('SELECT * FROM app_publications WHERE app_id=?').get(id);
      if (!entry && mode === 'participant' && request.domainId === undefined && policy?.launch_policy === 'anyone') {
        const alias = db.prepare(`SELECT d.id FROM app_domains d JOIN app_publication_domains p ON p.domain_id=d.id
          AND p.app_id=d.app_id AND p.owner_account_id=d.owner_account_id
          WHERE d.app_id=? AND d.state='bound' AND d.role='alias' ORDER BY d.created_at,d.id LIMIT 1`).get(id);
        if (alias) entry = resolveEntry({ actor, appId: id, domainId: alias.id, ...(request.path === undefined ? {} : { path: request.path }) });
      }
      assertApps(entry, 'apps_access_denied', 403);
      const target = policy && db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=? AND revision=?').get(id, policy.active_target_revision);
      assertApps(policy && target && policy.owner_account_id === app.owner_account_id && target.owner_account_id === app.owner_account_id
        && supportedRuntimeProfile(target.profile) && target.digest === runtimeTargetDigest({ appId: id, revision: target.revision,
          ownerAccountId: app.owner_account_id, connectorKey: target.connector_key, port: target.port,
          entryPath: target.entry_path, profile: target.profile }), 'apps_registry_corrupt', 500);
      if(target.profile===SCOPED_RUNTIME_PROFILE) {
        assertApps(typeof requireScopedTarget==='function','app_scoped_admission_required',503);
        synchronous(requireScopedTarget({appId:id,revision:target.revision,ownerAccountId:target.owner_account_id,
          connectorKey:target.connector_key,port:target.port,entryPath:target.entry_path,profile:target.profile,digest:target.digest}),'apps_async_authority');
      }
      const grants = cleanGrants(JSON.parse(app.grants_json));
      Object.freeze(grants.accountIds); Object.freeze(grants.communityIds); Object.freeze(grants);
      const snapshot = Object.freeze({ appId: id, ownerId: app.owner_account_id, accountId: actor.accountId,
        appRevision: app.revision, policyEpoch: policy.policy_epoch, title: app.name,
        visibility: policy.launch_policy === 'anyone' ? 'public' : 'private', grants,
        target: Object.freeze({ revision: target.revision, digest: target.digest, profile: target.profile }),
        entry: Object.freeze({ appId: id, domainId: entry.domainId, origin: entry.origin, path: entry.path }),
        canManage: actor.accountId === app.owner_account_id });
      const result = synchronous(callback(snapshot), 'apps_async_authority');
      assertActor(actor);
      return result;
    });
  };
}
