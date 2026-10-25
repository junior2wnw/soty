import { AppsError, assertApps, appId, textId } from './protocol.mjs';
import { createLaunchPath } from './launch-path.mjs';

const sourceStates = Object.freeze({ offline: 'offline', unknown: 'starting', responding: 'ready', unreachable: 'stopped' });

// Called only inside the engagement registry's Apps transaction, itself inside
// the trusted World authority fence. This function neither opens another
// transaction nor returns the branded runtime decision or connector identity.
export function createEngagementEntryResolver({ db, assertActor, publications, inspectSource }) {
  return function resolveEntry({ actor, appId: id, domainId, path }) {
    assertApps(db.isTransaction, 'apps_transaction_required', 500);
    assertActor(actor); appId(id);
    if (domainId !== undefined) textId(domainId);
    const app = db.prepare('SELECT * FROM local_apps WHERE id=?').get(id);
    if (!app || app.state !== 'enabled') return null;
    const domain = domainId === undefined
      ? db.prepare("SELECT * FROM app_domains WHERE app_id=? AND role='canonical'").get(id)
      : db.prepare('SELECT * FROM app_domains WHERE app_id=? AND id=?').get(id, domainId);
    if (!domain || domain.state !== 'bound') return null;
    let decision;
    try { decision = publications.decideAccess({ domainId: domain.id, origin: domain.origin, actor }); }
    catch (error) {
      // A private/retired entry has a generic unavailable projection. Storage,
      // authentication and programming failures must remain actual failures.
      if (error instanceof AppsError && error.code === 'apps_access_denied') return null;
      throw error;
    }
    const { entryPath } = createLaunchPath(path ?? decision.route.entryPath);
    const observed = inspectSource({ app, target: { connectorKey: decision.route.connectorKey,
      revision: decision.targetRevision, digest: decision.targetDigest } });
    const status = sourceStates[observed?.state];
    assertApps(status, 'apps_source_observation_invalid', 500);
    return { appId: id, domainId: domain.id, origin: domain.origin, path: entryPath,
      name: app.name, status, canManage: actor.accountId === app.owner_account_id };
  };
}
