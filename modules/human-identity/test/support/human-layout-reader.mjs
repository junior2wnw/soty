import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { HUMAN_LAYOUT_V2_SHA256, HUMAN_LAYOUT_V2_OBJECTS } from './human-layout-v2.mjs';
import { HUMAN_LAYOUT_V3_SHA256, HUMAN_LAYOUT_V3_OBJECTS } from './human-layout-v3.mjs';

// Independent, bounded, read-only reader. Its literal pins do not import the
// candidate writer. It checks structural compatibility, not keys or authority.
export function assertHumanLayout(path, { maximumVersion = 3, expectedMetadata } = {}) {
  const fail = () => { throw new Error('human_identity_storage_unknown'); };
  const db = new DatabaseSync(path, { readOnly: true });
  try {
    const version = db.prepare('PRAGMA user_version').get().user_version;
    if (![2, 3].includes(version) || version > maximumVersion) fail();
    const rows = db.prepare(`SELECT CASE WHEN type IN('table','index','trigger') AND length(name)<=128
      AND length(tbl_name)<=128 AND typeof(sql)='text' AND length(sql)<=32768 THEN type END AS type,
      CASE WHEN length(name)<=128 THEN name END AS name,CASE WHEN length(tbl_name)<=128 THEN tbl_name END AS tbl_name,
      CASE WHEN typeof(sql)='text' AND length(sql)<=32768 THEN sql END AS sql
      FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name LIMIT 39`).all();
    if (rows.length !== (version === 2 ? HUMAN_LAYOUT_V2_OBJECTS : HUMAN_LAYOUT_V3_OBJECTS)
      || rows.some(row => Object.values(row).some(value => typeof value !== 'string'))) fail();
    const normalize = sql => sql.split(/('(?:[^']|'')*')/gu).map((part, index) => index % 2 ? part
      : part.replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase()).join('');
    const hash = createHash('sha256').update(JSON.stringify(rows.map(row => [row.type, row.name, row.tbl_name, normalize(row.sql)]))).digest('hex');
    if (hash !== (version === 2 ? HUMAN_LAYOUT_V2_SHA256 : HUMAN_LAYOUT_V3_SHA256)) fail();
    const metadata = db.prepare(`SELECT CASE WHEN typeof(key)='text' AND length(key)<=128 THEN key END AS key,
      CASE WHEN typeof(value)='text' AND length(value)<=1024 THEN value END AS value
      FROM human_identity_meta ORDER BY key LIMIT 6`).all();
    if (metadata.length !== 5 || metadata.some(row => typeof row.key !== 'string' || typeof row.value !== 'string')) fail();
    const values = Object.fromEntries(metadata.map(row => [row.key, row.value]));
    if (JSON.stringify(Object.keys(values)) !== JSON.stringify(['environment_id', 'issuer', 'lineage', 'profile', 'registry_id'])
      || values.lineage !== `soty.human-identity.sqlite.v${version}` || values.profile !== 'oidc-provider-9.12.2-human-v1'
      || expectedMetadata && Object.entries(expectedMetadata).some(([key, value]) => values[key] !== value)
      || db.prepare('PRAGMA foreign_key_check').get()) fail();
    return version;
  } finally { db.close(); }
}
