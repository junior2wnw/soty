import { assertApps, appPort, runtimePath } from './protocol.mjs';
import { createHash } from 'node:crypto';
import { RUNTIME_PROFILE, runtimeTargetDigest, validExposureAck } from './schema.mjs';
import { isRootSignedWorkspaceSource } from '../../source-app/server/standard-profile.mjs';

const positive = value => Number.isSafeInteger(value) && value >= 1;
const digest = value => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
export const nativeTargetTuple = row => Object.freeze({ appId: row.app_id, revision: row.revision,
  ownerAccountId: row.owner_account_id, connectorKey: row.connector_key, port: row.port,
  entryPath: row.entry_path, profile: row.profile, digest: row.digest });

function boundAlias(db, app, origin, domainId) {
  const row = db.prepare(`SELECT d.* FROM app_domains d JOIN app_publication_domains p
    ON p.app_id=d.app_id AND p.domain_id=d.id AND p.owner_account_id=d.owner_account_id
    WHERE d.app_id=? AND d.owner_account_id=? AND d.origin=? AND d.role='alias' AND d.state='bound'`)
    .get(app.id, app.owner_account_id, origin);
  assertApps(row && (domainId === undefined || row.id === domainId), 'app_native_entry_domain_required', 403);
  return row;
}

/** Only an independently reviewed Root workspace can acquire a second entry.
 * A different hostname keeps its frame from borrowing Native global cookies. */
export function verifyNativeEntryCandidate({ db, app, primary, native, origin, scopedAdmission, blockedPorts = [] }) {
  assertApps(primary.profile === 'soty.selected-human-embed.v2' && native.profile === RUNTIME_PROFILE
    && native.appId === app.id && native.ownerAccountId === app.owner_account_id
    && native.connectorKey === primary.connectorKey && native.port !== primary.port
    && native.digest === runtimeTargetDigest(native), 'app_native_entry_target_invalid', 403);
  appPort(native.port, blockedPorts); runtimePath(native.entryPath);
  const profile = scopedAdmission.verifyCandidate(primary).profile;
  assertApps(isRootSignedWorkspaceSource(profile.sourceProfile) && origin === profile.nativeOrigin
    && new URL(origin).hostname !== new URL(profile.embedOrigin).hostname,
  'app_native_entry_origin_invalid', 403);
  const domain = boundAlias(db, app, origin);
  boundAlias(db, app, profile.embedOrigin);
  return Object.freeze({ origin, domainId: domain.id, target: native });
}

/** Read ONLY the last overall source promotion, never an older pairing. This
 * deliberately understands no v1 pairing and uses no extra schema or history ACK. */
export function readPairedNativeEntry(db, appId) {
  const state = db.prepare(`SELECT a.id,a.owner_account_id,a.state,p.active_target_revision,p.policy_epoch,
    t.digest,t.profile,t.connector_key FROM local_apps a JOIN app_publications p ON p.app_id=a.id
    AND p.owner_account_id=a.owner_account_id JOIN app_runtime_targets t ON t.app_id=a.id
    AND t.revision=p.active_target_revision AND t.owner_account_id=a.owner_account_id WHERE a.id=?`).get(appId);
  if (!state || state.profile !== 'soty.selected-human-embed.v2') return null;
  const row = db.prepare('SELECT * FROM app_source_receipts WHERE app_id=? ORDER BY committed_epoch DESC LIMIT 1').get(appId);
  if (!row) return null;
  let receipt; try { receipt = JSON.parse(row.value_json); } catch { return null; }
  const entry = receipt?.nativeEntry, target = entry?.target, ack = entry?.exposureAck;
  if (receipt?.schema !== 'soty.app-source-receipt.v2' || receipt.namespace !== 'apps.source.promote.v2'
    || receipt.appId !== appId || receipt.targetRevision !== state.active_target_revision
    || receipt.targetDigest !== state.digest || receipt.profile !== state.profile
    || !positive(receipt.policyEpoch) || receipt.policyEpoch !== row.committed_epoch || receipt.policyEpoch > state.policy_epoch
    || row.account_id !== state.owner_account_id || receipt.requestKeyHash !== row.request_key
    || !positive(target?.revision) || !digest(target?.digest) || target?.profile !== RUNTIME_PROFILE
    || typeof entry?.origin !== 'string' || !/^dom_[a-f0-9]{32}$/u.test(entry?.domainId)
    || !digest(receipt.preparationKeyHash) || !digest(receipt.requestKeyHash)
    || ack?.scope !== 'whole-port' || ack.targetRevision !== target.revision
    || ack.targetDigest !== target.digest || ack.profile !== target.profile) return null;
  const retained = db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=? AND revision=?').get(appId, target.revision);
  if (!retained || retained.owner_account_id !== state.owner_account_id || retained.profile !== RUNTIME_PROFILE
    || retained.digest !== target.digest || retained.connector_key !== state.connector_key) return null;
  const native = nativeTargetTuple(retained);
  if (native.digest !== runtimeTargetDigest(native)) return null;
  return Object.freeze({ origin: entry.origin, domainId: entry.domainId, target: native,
    primaryTargetRevision: receipt.targetRevision, primaryTargetDigest: receipt.targetDigest,
    preparationKeyHash: receipt.preparationKeyHash, requestKeyHash: receipt.requestKeyHash,
    committedPolicyEpoch: receipt.policyEpoch });
}

export function assertNativePortAvailable(db, exceptAppId, connectorKey, port) {
  // App registrations are bounded per owner. One latest receipt per matching
  // app is inspected; no scanning through historical acknowledgments.
  const apps = db.prepare(`SELECT a.id FROM local_apps a JOIN app_publications p ON p.app_id=a.id
    JOIN app_runtime_targets t ON t.app_id=a.id AND t.revision=p.active_target_revision
    WHERE a.state='enabled' AND a.id<>? AND t.connector_key=? AND t.profile='soty.selected-human-embed.v2'`)
    .all(exceptAppId, connectorKey);
  for (const app of apps) assertApps(readPairedNativeEntry(db, app.id)?.target.port !== port, 'app_port_already_registered', 409);
}

/** Current PUBLIC app policy only, never a Human identity assertion. */
function loadNativeEntry({ db, appId, origin, primaryDigest, nativeDigest, scopedAdmission }) {
  const app = db.prepare('SELECT * FROM local_apps WHERE id=?').get(appId);
  const publication = db.prepare('SELECT * FROM app_publications WHERE app_id=?').get(appId);
  assertApps(app?.state === 'enabled' && publication?.launch_policy === 'anyone', 'app_native_entry_closed', 403);
  const active = db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=? AND revision=?').get(appId, publication.active_target_revision);
  if (active?.profile === RUNTIME_PROFILE) {
    // Ordinary Native-only apps and an explicit approved rollback use the SAME
    // wire. Only their CURRENT primary target/ACK is authority; no historical
    // Source receipt or earlier whole-port acknowledgment can restore access.
    const target = nativeTargetTuple(active);
    assertApps(active.owner_account_id === app.owner_account_id && target.digest === runtimeTargetDigest(target)
      && primaryDigest === target.digest && nativeDigest === target.digest, 'app_native_entry_closed', 403);
    let ack; try { ack = JSON.parse(publication.exposure_ack_json); } catch { /* denied below */ }
    assertApps(ack && typeof ack === 'object' && !Array.isArray(ack)
      && Object.keys(ack).every(key => ['scope','targetRevision','targetDigest','profile'].includes(key))
      && publication.exposure_ack_revision === target.revision && validExposureAck(ack, active, 'whole-port'),
    'app_native_entry_closed', 403);
    const domain = boundAlias(db, app, origin);
    const device = db.prepare('SELECT owner_account_id FROM app_devices WHERE connector_key=?').get(target.connectorKey);
    assertApps(device?.owner_account_id === app.owner_account_id, 'app_native_entry_closed', 403);
    const requestKeyHash = createHash('sha256').update(JSON.stringify(['soty.native-primary-policy.v1', appId,
      origin, target.revision, target.digest, ack])).digest('hex');
    return Object.freeze({ origin, domainId: domain.id, target, primary: target,
      primaryTargetRevision: target.revision, primaryTargetDigest: target.digest,
      policyEpoch: publication.policy_epoch, committedPolicyEpoch: publication.policy_epoch, requestKeyHash });
  }
  const entry = readPairedNativeEntry(db, appId);
  assertApps(entry && entry.origin === origin && entry.primaryTargetDigest === primaryDigest
    && entry.target.digest === nativeDigest, 'app_native_entry_closed', 403);
  boundAlias(db, app, origin, entry.domainId);
  const primaryRow = db.prepare('SELECT * FROM app_runtime_targets WHERE app_id=? AND revision=?').get(appId, entry.primaryTargetRevision);
  const primary = nativeTargetTuple(primaryRow), profile = scopedAdmission.require(primary);
  assertApps(isRootSignedWorkspaceSource(profile.sourceProfile) && profile.nativeOrigin === origin
    && new URL(origin).hostname !== new URL(profile.embedOrigin).hostname, 'app_native_entry_closed', 403);
  boundAlias(db, app, profile.embedOrigin);
  const device = db.prepare('SELECT owner_account_id FROM app_devices WHERE connector_key=?').get(entry.target.connectorKey);
  assertApps(device?.owner_account_id === app.owner_account_id, 'app_native_entry_closed', 403);
  return Object.freeze({ ...entry, policyEpoch: publication.policy_epoch, primary });
}

export function requireNativeEntry(options) {
  const { db } = options;
  if (db.isTransaction) return loadNativeEntry(options);
  db.exec('BEGIN');
  try { const entry = loadNativeEntry(options); db.exec('COMMIT'); return entry; }
  catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
}

export function nativeEntryView(entry) {
  if (!entry) return undefined;
  return { schema: 'soty.native-entry.v1', origin: entry.origin, domainId: entry.domainId,
    target: { revision: entry.target.revision, digest: entry.target.digest, profile: entry.target.profile,
      port: entry.target.port, entryPath: entry.target.entryPath },
    committedPolicyEpoch: entry.committedPolicyEpoch, admissionKeyHash: entry.requestKeyHash, runtimeReady: false };
}
