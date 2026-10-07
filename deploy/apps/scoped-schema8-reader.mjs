// Independent frozen Apps8 reader. No current application schema, migrator,
// adapter registry or profile parser import; unknown DDL fails before START.
import { createHash } from 'node:crypto';
const layoutPin = 'e3f5cdde4005ad32d7e28c3ef90e3736096265c97e84eb3c531221e3fc003267';
const canonical = value => Array.isArray(value) ? '[' + value.map(canonical).join(',') + ']'
  : value && typeof value === 'object' ? '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}' : JSON.stringify(value);
const sha = value => createHash('sha256').update(value).digest('hex');
const normalized = value => value.split(/('(?:[^']|'')*')/gu).map((part, i) => i % 2 ? part
  : part.replace(/\bIF\s+NOT\s+EXISTS\b/giu, '').replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase()).join('');
const refuse = ok => { if (!ok) throw Object.assign(new Error('apps_reader8_refused'), { code: 'apps_reader8_refused' }); };
const closed = (value, names) => value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).sort().join(',') === [...names].sort().join(',');
const id = value => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,179}$/u.test(value) && !value.includes('..');
const hash = value => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
function pin(value) { return closed(value, ['id', 'version', 'digest']) && id(value.id) && Number.isSafeInteger(value.version) && value.version > 0 && hash(value.digest); }
function resource(value, v2) {
  if (!v2) return closed(value, ['registryId', 'tenantId', 'appId', 'environmentId', 'resourceId', 'workspaceId']) && Object.values(value).every(id);
  if (!closed(value, ['registryId', 'tenantId', 'appId', 'environmentId', 'resourceId', 'selection'])
    || !['registryId', 'tenantId', 'appId', 'environmentId', 'resourceId'].every(key => id(value[key]))) return false;
  const item = value.selection;
  return closed(item, ['kind', 'nativeId', 'incarnationId']) && typeof item.kind === 'string' && item.kind.length <= 128
    && /^[a-z][a-z0-9]*(?:[.-][a-z0-9]+)*\.v[1-9][0-9]*$/u.test(item.kind) && id(item.incarnationId)
    && typeof item.nativeId === 'string' && item.nativeId.length > 0 && item.nativeId.isWellFormed()
    && Buffer.byteLength(item.nativeId, 'utf8') <= 4096 && !/[\u0000-\u001f\u007f-\u009f]/u.test(item.nativeId);
}
function profile(value, target) {
  const keys = ['schema', 'appId', 'connector', 'target', 'sourceProfile', 'resource', 'issuer', 'clientId', 'embedOrigin', 'nativeOrigin', 'parentOrigin'];
  if (!closed(value, keys) || value.schema !== target.profile || value.appId !== target.app_id
    || !closed(value.connector, ['linkId', 'hostDeviceId', 'connectorId']) || !Object.values(value.connector).every(id)
    || !closed(value.target, ['revision', 'digest']) || value.target.revision !== target.revision || value.target.digest !== target.digest
    || !pin(value.sourceProfile) || !resource(value.resource, value.schema.endsWith('.v2'))
    || value.resource.appId !== target.app_id || value.resource.tenantId !== target.owner_account_id
    || [value.connector.linkId, value.connector.hostDeviceId, value.connector.connectorId].join('|') !== target.connector_key
    || !id(value.clientId)) return false;
  try {
    for (const key of ['embedOrigin', 'nativeOrigin', 'parentOrigin']) {
      const origin = new URL(value[key]);
      if (origin.origin !== value[key] || origin.username || origin.password || !(origin.protocol === 'https:'
        || origin.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(origin.hostname) && Number(origin.port) >= 1024)) return false;
    }
    const issuer = new URL(value.issuer);
    return issuer.href === value.issuer && issuer.origin === value.parentOrigin && issuer.pathname === '/human-identity' && !issuer.search && !issuer.hash
      && new URL(value.nativeOrigin).hostname !== new URL(value.embedOrigin).hostname && value.parentOrigin !== value.embedOrigin;
  } catch { return false; }
}
export function readScopedApps8(db) {
  refuse(Number(db.prepare('PRAGMA user_version').get().user_version) === 8
    && db.prepare("SELECT value FROM apps_meta WHERE key='schema'").get()?.value === 'soty.apps-registry.v8');
  const rows = db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name LIMIT 68").all();
  refuse(rows.length === 67 && rows.every(value => typeof value.sql === 'string' && value.sql.length <= 32768));
  refuse(sha(JSON.stringify(rows.map(row => [row.type, row.name, row.tbl_name, normalized(row.sql)]))) === layoutPin);
  refuse(db.prepare('PRAGMA foreign_key_check').get() === undefined);
  let targets = 0, selected = 0;
  for (const target of db.prepare('SELECT * FROM app_runtime_targets ORDER BY app_id,revision').iterate()) {
    targets++; refuse(targets <= 65536 && ['soty.relay-restricted.v1', 'soty.selected-human-embed.v1', 'soty.selected-human-embed.v2'].includes(target.profile));
    refuse(target.digest === sha(JSON.stringify(['soty.runtime-target.v1', target.app_id, target.revision, target.owner_account_id,
      target.connector_key, target.port, target.entry_path, target.profile])));
    if (target.profile === 'soty.relay-restricted.v1') continue;
    selected++;
    const admission = db.prepare('SELECT * FROM app_scoped_embed_admissions WHERE app_id=? AND target_revision=?').get(target.app_id, target.revision);
    refuse(admission && admission.target_digest === target.digest && hash(admission.profile_digest)
      && typeof admission.approved_pin_json === 'string' && Buffer.byteLength(admission.approved_pin_json) <= 16384 && target.entry_path === '/embed');
    let parsed; try { parsed = JSON.parse(admission.approved_pin_json); } catch { refuse(false); }
    refuse(profile(parsed, target) && admission.approved_pin_json === canonical(parsed) && admission.profile_digest === sha(canonical(parsed)));
  }
  refuse(Number(db.prepare('SELECT count(*) AS n FROM app_scoped_embed_admissions').get().n) === selected);
  return Object.freeze({ schema: 'soty.apps-registry.v8', targets, selected });
}
export function requireApps8BeforeStart({ storedSchema, imageReaders }) {
  refuse(storedSchema === 'soty.apps-registry.v8' && Array.isArray(imageReaders) && imageReaders.includes('soty.apps-registry.v8'));
  return true;
}
