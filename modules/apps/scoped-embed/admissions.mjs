import { canonical, hash, need } from './profile.mjs';
import { approvedEmbedProfile, requireEmbedAdapter } from './profile-dispatch.mjs';
import { connectorKey } from '../server/protocol.mjs';
import { inspectAppsSchema } from '../server/schema.mjs';

/** Private approved host catalogue. Persisted public tuple/manifest fields do
 * not install a profile or create a Source grant. Historical pins are immutable. */
export function createScopedAdmissionRegistry({ db, profiles = [], clock = Date.now } = {}) {
  need(Array.isArray(profiles) && profiles.length <= 64);
  const approved = new Map();
  for (const raw of profiles) {
    const profile = requireEmbedAdapter(raw), id = profile.appId + ':' + profile.target.revision;
    const { digest: _derived, ...pin } = profile;
    need(!approved.has(id)); approved.set(id, Object.freeze({ raw: Object.freeze(pin), profile }));
  }
  function verifyTarget(target) {
    const item = approved.get(target.appId + ':' + target.revision);
    need(item, 'app_scoped_admission_required', 503);
    const { profile } = item;
    need(target.profile === profile.schema && profile.target.digest === target.digest
      && profile.resource.tenantId === target.ownerAccountId && connectorKey(profile.connector) === target.connectorKey
      && target.entryPath === '/embed', 'app_scoped_admission_mismatch', 403);
    return item;
  }
  function read(target) {
    const item = verifyTarget(target);
    // Full DDL/row recognition belongs to startup. Per-request authority stays
    // bounded and checks the exact current immutable admission plus host pin.
    const version = Number(db.prepare('PRAGMA user_version').get().user_version);
    need([7,8].includes(version) && db.prepare("SELECT value FROM apps_meta WHERE key='schema'").get()?.value==='soty.apps-registry.v'+version
      && (item.profile.schema !== 'soty.selected-human-embed.v2' || version === 8), 'app_scoped_migration_required', 503);
    const row = db.prepare('SELECT * FROM app_scoped_embed_admissions WHERE app_id=? AND target_revision=?').get(target.appId, target.revision);
    need(row && row.target_digest === target.digest && row.profile_digest === item.profile.digest
      && row.approved_pin_json === canonical(item.raw), 'app_scoped_admission_required', 503);
    return item.profile;
  }
  return Object.freeze({
    verifyCandidate: verifyTarget,
    commit(target) {
      need(db.isTransaction && ['v7','v8'].includes(inspectAppsSchema(db)), 'app_scoped_transaction_required', 500);
      const item = verifyTarget(target);
      need(item.profile.schema !== 'soty.selected-human-embed.v2' || inspectAppsSchema(db) === 'v8', 'app_scoped_migration_required', 503);
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
