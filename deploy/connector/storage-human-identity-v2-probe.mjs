import { createHash } from 'node:crypto';

// Independent literal schema2 pin. No imports from the writer, decryption,
// artifact enumeration, migration, repair, callbacks or network access.
export const humanIdentityV2StoragePin = Object.freeze({ version: 2, objects: 33,
  lineage: 'soty.human-identity.sqlite.v2', profile: 'oidc-provider-9.12.2-human-v1',
  layoutSha256: 'b096f63d2b27fa301d9fd336a57205278495ea80355fd27477f46115d4f8a2c9',
  ddlSha256: '36d71d26ec13f3129923f0742730174df67637a2219d036a9ca79b98e0265a19' });
const fail = () => { throw new Error('storage_format_unknown'); };
const normalized = sql => sql.split(/('(?:[^']|'')*')/gu).map((part, i) => i % 2 ? part
  : part.replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase()).join('');
const identityId = value => typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$/u.test(value)
  && !value.includes('..') && !value.includes('//');
function approvedIssuer(value) {
  if (typeof value !== 'string' || value.length > 512 || /[\s%\\?#]/u.test(value)) return false;
  try {
    const url = new URL(value);
    return !url.username && !url.password && value === url.origin + '/human-identity'
      && (url.protocol === 'https:' || url.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname));
  } catch { return false; }
}
/** The caller owns the read-only connection and filesystem/WAL custody checks. */
export function inspectHumanIdentityV2(db) {
  try {
    if (db.prepare('PRAGMA user_version').get().user_version !== 2) fail();
    const rowsLayout = db.prepare(`SELECT
      CASE WHEN typeof(type)='text' AND length(CAST(type AS BLOB))<=16 THEN type ELSE NULL END AS type,
      CASE WHEN typeof(name)='text' AND length(CAST(name AS BLOB))<=128 THEN name ELSE NULL END AS name,
      CASE WHEN typeof(tbl_name)='text' AND length(CAST(tbl_name AS BLOB))<=128 THEN tbl_name ELSE NULL END AS tbl_name,
      CASE WHEN typeof(sql)='text' AND length(CAST(sql AS BLOB))<=16384 THEN sql ELSE NULL END AS sql
      FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name LIMIT 34`).all();
    if (rowsLayout.length !== humanIdentityV2StoragePin.objects || rowsLayout.some(row => Object.values(row).some(value => typeof value !== 'string'))) fail();
    const layout = rowsLayout
      .map(row => [row.type, row.name, row.tbl_name, normalized(row.sql)]);
    const hash = createHash('sha256').update(JSON.stringify(layout)).digest('hex');
    if (layout.length !== humanIdentityV2StoragePin.objects || hash !== humanIdentityV2StoragePin.layoutSha256) fail();
    const rows = db.prepare(`SELECT
      CASE WHEN typeof(key)='text' AND length(CAST(key AS BLOB))<=32 THEN key ELSE NULL END AS key,
      CASE WHEN typeof(value)='text' AND length(CAST(value AS BLOB))<=512 THEN value ELSE NULL END AS value
      FROM human_identity_meta ORDER BY key LIMIT 6`).all();
    const keys = ['environment_id', 'issuer', 'lineage', 'profile', 'registry_id'];
    if (rows.length !== keys.length || rows.some((row, index) => row.key !== keys[index] || typeof row.value !== 'string')) fail();
    const meta = Object.fromEntries(rows.map(row => [row.key, row.value]));
    if (rows.length !== 5 || meta.lineage !== humanIdentityV2StoragePin.lineage || meta.profile !== humanIdentityV2StoragePin.profile
      || !identityId(meta.registry_id) || !identityId(meta.environment_id) || !approvedIssuer(meta.issuer)) fail();
    if (db.prepare('PRAGMA quick_check').get()?.quick_check !== 'ok' || db.prepare('PRAGMA foreign_key_check').get()) fail();
    // Private host scope for consistency checks. Public format receipts carry only version.
    return Object.freeze({ version: 2, registryId: meta.registry_id, environmentId: meta.environment_id });
  } catch { fail(); }
}
