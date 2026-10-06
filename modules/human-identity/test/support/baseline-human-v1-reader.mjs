import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';

// Independent pre-renewal baseline pin, also present in commit f5931914f2e4bca.
// This is a layout/readability check; it never opens a candidate writer or decrypts.
export function assertBaselineHumanV1(path) {
  const db = new DatabaseSync(path, { readOnly: true });
  try {
    const normalized = sql => sql.split(/('(?:[^']|'')*')/gu).map((part, i) => i % 2 ? part
      : part.replace(/\s+/gu, '').replace(/;$/u, '').toLowerCase()).join('');
    const rows = db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()
      .map(row => [row.type, row.name, row.tbl_name, normalized(row.sql)]);
    const hash = createHash('sha256').update(JSON.stringify(rows)).digest('hex');
    if (db.prepare('PRAGMA user_version').get().user_version !== 1 || hash !== '68a7925ae4ab38923841308906d4ff35a9adf630e30631a718aab80ef8014d22') {
      throw new Error('human_identity_storage_unknown');
    }
    return true;
  } finally { db.close(); }
}
