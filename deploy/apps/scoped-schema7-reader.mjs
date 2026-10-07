// Literal independent v7 admission reader. It imports no current Apps schema,
// profile parser or migration. Storage readers must opt in before START.
import { createHash } from 'node:crypto';
const legacyProfile = 'soty.relay-restricted.v1', selectedProfile = 'soty.selected-human-embed.v1';
const canonical = value => Array.isArray(value) ? '[' + value.map(canonical).join(',') + ']'
  : value && typeof value === 'object' ? '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}' : JSON.stringify(value);
const sha = value => createHash('sha256').update(value).digest('hex');
function require(ok) { if (!ok) throw Object.assign(new Error('apps_reader7_refused'), { code: 'apps_reader7_refused' }); }
export function readScopedApps7(db) {
  require(Number(db.prepare('PRAGMA user_version').get().user_version) === 7);
  require(db.prepare("SELECT value FROM apps_meta WHERE key='schema'").get()?.value === 'soty.apps-registry.v7');
  const columns = db.prepare('PRAGMA table_info(app_scoped_embed_admissions)').all();
  require(JSON.stringify(columns.map(row => row.name)) === JSON.stringify(['app_id','target_revision','target_digest','profile_digest','approved_pin_json','created_at']));
  require(columns[0].pk === 1 && columns[1].pk === 2 && columns.slice(2).every(row => row.pk === 0));
  require(db.prepare('PRAGMA foreign_key_check').all().length === 0);
  const rows = db.prepare('SELECT * FROM app_runtime_targets ORDER BY app_id,revision').all();
  let selected = 0;
  for (const target of rows) {
    require([legacyProfile, selectedProfile].includes(target.profile));
    require(target.digest === sha(JSON.stringify(['soty.runtime-target.v1', target.app_id, target.revision,
      target.owner_account_id, target.connector_key, target.port, target.entry_path, target.profile])));
    if (target.profile !== selectedProfile) continue;
    selected++;
    const admission = db.prepare('SELECT * FROM app_scoped_embed_admissions WHERE app_id=? AND target_revision=?').get(target.app_id, target.revision);
    require(admission && admission.target_digest === target.digest && /^[a-f0-9]{64}$/.test(admission.profile_digest));
    const pin = JSON.parse(admission.approved_pin_json);
    require(pin.schema === selectedProfile && pin.appId === target.app_id && pin.target.revision === target.revision
      && pin.target.digest === target.digest && pin.resource.appId === target.app_id && pin.resource.tenantId === target.owner_account_id
      && [pin.connector.linkId,pin.connector.hostDeviceId,pin.connector.connectorId].join('|') === target.connector_key
      && target.entry_path === '/embed' && admission.approved_pin_json === canonical(pin) && admission.profile_digest === sha(canonical(pin)));
  }
  require(Number(db.prepare('SELECT count(*) AS n FROM app_scoped_embed_admissions').get().n) === selected);
  return Object.freeze({ schema: 'soty.apps-registry.v7', targets: rows.length, selected });
}

export function requireApps7BeforeStart({ storedSchema, imageReaders }) {
  require(storedSchema === 'soty.apps-registry.v7' && Array.isArray(imageReaders) && imageReaders.includes('soty.apps-registry.v7'));
  return true;
}
