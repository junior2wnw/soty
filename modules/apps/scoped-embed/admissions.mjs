import { scopedEmbedProfile, canonical, hash, need, SCOPED_EMBED_PROFILE } from './profile.mjs';
import { connectorKey } from '../server/protocol.mjs';
import { inspectAppsSchema } from '../server/schema.mjs';

/** Private approved host catalogue. Persisted public tuple/manifest fields do
 * not install a profile or create a Source grant. Historical pins are immutable. */
export function createScopedAdmissionRegistry({ db, profiles = [], clock = Date.now } = {}) {
  need(Array.isArray(profiles) && profiles.length <= 64);
  const approved = new Map();
  for (const raw of profiles) {
    const profile = scopedEmbedProfile(raw), id = profile.appId + ':' + profile.target.revision;
    const { digest: _derived, ...pin } = profile;
    need(!approved.has(id)); approved.set(id, Object.freeze({ raw: Object.freeze(pin), profile }));
  }
  function verifyTarget(target) {
    const item = approved.get(target.appId + ':' + target.revision);
    need(item, 'app_scoped_admission_required', 503);
    const { profile } = item;
    need(target.profile === SCOPED_EMBED_PROFILE && profile.target.digest === target.digest
      && profile.resource.tenantId === target.ownerAccountId && connectorKey(profile.connector) === target.connectorKey
      && target.entryPath === '/embed', 'app_scoped_admission_mismatch', 403);
    return item;
  }
  function read(target) {
    const item = verifyTarget(target);
    // Full DDL/row recognition belongs to startup. Per-request authority stays
    // bounded and checks the exact current immutable admission plus host pin.
    need(Number(db.prepare('PRAGMA user_version').get().user_version)===7
      &&db.prepare("SELECT value FROM apps_meta WHERE key='schema'").get()?.value==='soty.apps-registry.v7', 'app_scoped_migration_required', 503);
    const row = db.prepare('SELECT * FROM app_scoped_embed_admissions WHERE app_id=? AND target_revision=?').get(target.appId, target.revision);
    need(row && row.target_digest === target.digest && row.profile_digest === item.profile.digest
      && row.approved_pin_json === canonical(item.raw), 'app_scoped_admission_required', 503);
    return item.profile;
  }
  return Object.freeze({
    verifyCandidate: verifyTarget,
    commit(target) {
      need(db.isTransaction && inspectAppsSchema(db) === 'v7', 'app_scoped_transaction_required', 500);
      const item = verifyTarget(target);
      const existing = db.prepare('SELECT * FROM app_scoped_embed_admissions WHERE app_id=? AND target_revision=?').get(target.appId, target.revision);
      if (existing) { read(target); return; }
      db.prepare('INSERT INTO app_scoped_embed_admissions VALUES(?,?,?,?,?,?)')
        .run(target.appId, target.revision, target.digest, item.profile.digest, canonical(item.raw), clock());
    },
    require: read,
    profiles: () => [...approved.values()].map(item => item.raw),
    digest: () => hash([...approved.values()].map(item => item.profile.digest).sort()),
  });
}
