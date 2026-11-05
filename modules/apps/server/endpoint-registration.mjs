import { createHash, randomBytes } from 'node:crypto';
import { assertApps, appName, appPort, runtimePath, textId, cleanGrants } from './protocol.mjs';
import { SELF_IP_PROFILE, ENDPOINT_SCHEMA, normalizeSelfEndpoint } from './endpoint-targets.mjs';
import { runtimeTargetDigest } from './schema.mjs';

const hash = value => createHash('sha256').update(value).digest('hex');
const exact = (value, keys) => assertApps(value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).every(key => keys.includes(key)), 'unexpected_argument');

/** Async probe owns no SQL/authority lock. Its original issued actor and live
 * connector witness are checked again in the synchronous commit. A request ID
 * identifies the semantic write, not a probe nonce or a volatile interface list. */
export function createEndpointRegistration({ db, enabled, blockedPorts, assertActor, withAuthorityFence,
  transaction, resolveDevice, inventoryFor, assertGrants, prepareTarget, verifyPreparedTarget, insertApp, readApp, afterCommit }) {
  function normalize(args) {
    exact(args, ['requestId', 'hostDeviceId', 'connectorId', 'name', 'source', 'grants', 'inventoryWitness']);
    assertApps(typeof args.requestId === 'string' && /^[A-Za-z0-9_.:-]{16,128}$/u.test(args.requestId), 'apps_endpoint_request_invalid');
    exact(args.source, ['schema', 'endpoint', 'port', 'entryPath']);
    assertApps(args.source.schema === 'soty.app-http-source.v2', 'apps_endpoint_invalid');
    const source = { schema: args.source.schema, endpoint: normalizeSelfEndpoint(args.source.endpoint),
      port: appPort(args.source.port, blockedPorts), entryPath: runtimePath(args.source.entryPath) };
    return { requestId: args.requestId, hostDeviceId: textId(args.hostDeviceId), connectorId: textId(args.connectorId),
      name: appName(args.name), source, grants: cleanGrants(args.grants), inventoryWitness: args.inventoryWitness };
  }
  function receipt(actor, intent, intentHash) {
    const row = db.prepare('SELECT * FROM app_endpoint_registration_receipts WHERE account_id=? AND request_key=?').get(actor.accountId, intent.requestId);
    if (!row) return null;
    assertApps(row.intent_hash === intentHash, 'apps_endpoint_request_conflict', 409);
    return { app: readApp(row.app_id, actor), registrationReceipt: { schema: 'soty.app-endpoint-registration-receipt.v1',
      requestId: intent.requestId, appId: row.app_id, intentHash, committed: true } };
  }
  const fence = (actor, callback) => withAuthorityFence(() => transaction(() => { assertActor(actor); return callback(); }), actor);
  async function register(actor, args) {
    assertApps(enabled, 'apps_endpoint_connector_upgrade_required', 503); assertActor(actor);
    const intent = normalize(args), intentHash = hash(JSON.stringify([intent.hostDeviceId, intent.connectorId, intent.name, intent.source, intent.grants]));
    const first = fence(actor, () => {
      const replay = receipt(actor, intent, intentHash); if (replay) return { replay };
      const device = resolveDevice(actor, intent), witness = inventoryFor(device, intent.inventoryWitness, intent.source.endpoint);
      assertGrants(actor, intent.grants);
      const tuple = { schema: ENDPOINT_SCHEMA, appId: 'app-' + hash(JSON.stringify(['soty.endpoint-registration.v1', actor.accountId, intent.requestId])).slice(0, 32),
        revision: 1, ownerAccountId: actor.accountId, connectorKey: device.connector_key, profile: SELF_IP_PROFILE,
        endpoint: intent.source.endpoint, port: intent.source.port, entryPath: intent.source.entryPath };
      return { target: Object.freeze({ ...tuple, digest: runtimeTargetDigest(tuple) }), witness };
    });
    if (first.replay) { afterCommit(actor, first.replay.app.id); return first.replay; }
    const preparationId = randomBytes(32).toString('base64url');
    const evidence = await prepareTarget({ actor, target: first.target, requiredBindingVersion: 2, preparationId });
    const result = fence(actor, () => {
      const replay = receipt(actor, intent, intentHash); if (replay) return replay;
      const device = resolveDevice(actor, intent), witness = inventoryFor(device, intent.inventoryWitness, intent.source.endpoint);
      assertApps(witness === first.witness && device.connector_key === first.target.connectorKey
        && verifyPreparedTarget({ evidence, preparationId, actor, target: first.target, requiredBindingVersion: 2 }), 'apps_source_preparation_stale', 409);
      assertGrants(actor, intent.grants);
      insertApp(actor, intent, first.target);
      db.prepare('INSERT INTO app_endpoint_registration_receipts VALUES (?,?,?,?,?)').run(actor.accountId, intent.requestId, intentHash, first.target.appId, Date.now());
      return receipt(actor, intent, intentHash);
    });
    // Notify only after COMMIT. On any failed commit the same semantic request
    // can safely probe/retry; an unknown ACK replays this durable receipt first.
    afterCommit(actor, result.app.id); return result;
  }
  return Object.freeze({ operations: new Set(['apps.registerEndpoint']), executeAsync: ({ op, actor, args }) => {
    assertApps(op === 'apps.registerEndpoint', 'unsupported_operation'); return register(actor, args);
  } });
}
