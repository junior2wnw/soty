import { ConnectError } from '../modules/connect/server/index.mjs';

const operations = new Set(['apps.prepared.create', 'apps.prepared.read']);
const need = (value, code) => { if (!value) throw new ConnectError(code); };

/** The connector performs only a bounded read of an already running app.
 * A durable receipt fixes the request before the Apps transaction, so retries
 * always read the same card or tombstone even across a lost response. */
export function createPreparedAppsExtension({ store, apps, universal, connect }) {
  return { operations, async executeAsync({ op, args, actor }) {
    need(operations.has(op) && connect.isActorActive(actor), 'authentication_required');
    need(args && typeof args === 'object' && !Array.isArray(args)
      && Object.keys(args).every(key => ['expectedAccountId', 'hostDeviceId', 'connectorId', 'requestId', 'location', 'manifestDigest'].includes(key))
      && args.expectedAccountId === actor.accountId, 'invalid_arguments');
    const device = apps.resolveOwnedDevice(actor, args.hostDeviceId, args.connectorId);
    const intent = { schema: 'soty.prepared-request.v1', ownerAccountId: actor.accountId, sourceActorDeviceId: actor.deviceId,
      linkId: device.linkId, hostDeviceId: args.hostDeviceId, connectorId: args.connectorId, requestId: args.requestId,
      location: args.location, manifestDigest: args.manifestDigest };
    const guard = () => {
      if (!connect.isActorActive(actor)) return false;
      try { const current = apps.resolveOwnedDevice(actor, args.hostDeviceId, args.connectorId); return current.linkId === device.linkId; }
      catch { return false; }
    };
    const checked = value => {
      need(guard(), 'authentication_required');
      need(value.ok, String(value.error || 'prepared_request_failed').replaceAll('-', '_'));
      return value;
    };
    const currentApp = receipt => connect.withCurrentContacts(actor, () => {
      need(guard(), 'authentication_required'); return apps.creationJobApp(actor, receipt.id);
    });
    const status = app => {
      if (!app) return null;
      if (app.state === 'revoked') return { state: 'revoked', app };
      let feedback = universal ? 'waiting' : 'unavailable';
      try {
        const result = universal && connect.withCurrentContacts(actor, () => universal.completeExisting(actor, app));
        if (result?.universalRegistration?.state === 'ready' && result.universalRegistration.feedback?.state === 'ready') feedback = 'ready';
      } catch { /* The registered app remains usable if optional feedback is unavailable. */ }
      need(guard(), 'authentication_required');
      return { state: 'ready', app, feedback };
    };
    let prior = checked(await store.readPreparedRequest(intent, guard));
    if (prior.found) {
      const value = status(currentApp(prior.receipt));
      if (value) return value;
      if (op === 'apps.prepared.read') return { state: 'pending' };
    } else {
      if (op === 'apps.prepared.read') return { state: 'not-found' };
      prior = checked(await store.reservePreparedRequest(intent, guard));
    }
    need(apps.configured, 'prepared_host_unavailable');
    const inspected = await apps.inspectPrepared(actor, { hostDeviceId: intent.hostDeviceId, connectorId: intent.connectorId,
      location: intent.location, digest: intent.manifestDigest });
    // Re-read the immutable request after the channel round trip. The final
    // synchronous fence also checks the actor, ownership and exact channel.
    const saved = checked(await store.readPreparedRequest(intent, guard));
    need(saved.found && saved.receipt.id === prior.receipt.id, 'prepared_request_changed');
    let app;
    try {
      const result = connect.withCurrentContacts(actor, () => {
        need(guard(), 'authentication_required'); inspected.assertCurrent();
        return universal ? universal.registerPreparedApp(actor, saved.receipt, inspected.manifest)
          : apps.registerPreparedApp(actor, saved.receipt, inspected.manifest);
      });
      app = result.app;
    } catch (error) {
      // Registration and feedback use different databases. Never repeat the
      // mutation without first checking the durable Apps card/tombstone.
      app = currentApp(saved.receipt); if (!app) throw error;
    }
    return status(app);
  } };
}
