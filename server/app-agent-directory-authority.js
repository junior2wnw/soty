import { AppAgentDirectoryError } from '../modules/app-agent-directory/server/index.mjs';

const denied = () => { throw new AppAgentDirectoryError('apps_access_denied', 403); };
/** Current Connect humans and existing Root credential-to-owner authority, never a JSON owner claim. */
export function createAppAgentDirectoryAuthority({ apps, connect, capabilities }) {
  function active(actor) {
    try {
      if (actor?.type === 'service') { capabilities().authorize({ actor, action: 'history' }); return true; }
      return connect().isActorActive(actor) === true;
    } catch { return false; }
  }
  function withAppAuthority(request, callback) {
    if(request.actor?.type!=='service')return connect().withActorAuthorityFence(request.actor,()=>apps().withAppAuthority(request,callback));
    // getContract acquires the existing adapter's own Connect/Apps fence.
    // It must not run inside another Connect transaction.
    if ((request.mode ?? 'owner') === 'owner') return denied();
    const host = capabilities(), references = host?.external?.contracts ?? [];
    let selected;
    for (const reference of references) {
      try {
        const contract = host.external.getContract({ actor: request.actor, reference });
        if (contract.appId === request.appId) { selected = reference; break; }
      } catch (error) {
        if (!['AccessError', 'CapabilityHttpError', 'AppsError'].includes(error?.name)) throw error;
      }
    }
    if (!selected) {
      host.authorize({ actor: request.actor, action: 'history' });
      const result = apps().withPublicAppAuthority({ appId: request.appId }, callback);
      host.authorize({ actor: request.actor, action: 'history' }); return result;
    }
    const result = connect().withAuthorityFence(() => host.withOwnerAuthority({ actor: request.actor }, owner =>
      apps().withAppAuthority({ ...request, actor: owner }, callback)));
    // Recheck the same original grant/Source after the downstream fence.
    host.external.getContract({ actor: request.actor, reference: selected }); return result;
  }
  return Object.freeze({ actorActive: active, withAppAuthority,
    withPublicAppAuthority: (request, callback) => apps().withPublicAppAuthority(request, callback),
    readPublicApps: () => apps().publicCatalog() });
}
