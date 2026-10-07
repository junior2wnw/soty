// Independent literal reader; no imports of the production migration/parser/SDK.
import { DatabaseSync } from 'node:sqlite';
import { existsSync } from 'node:fs';
const columns = {
  planner_soty_rp_format: 'id,format,version',
  planner_soty_rp_anchors:
    'session_hash,locator_hash,profile_digest,binding_digest,payload_cipher,key_id,expires_at,created_at,generation,active',
  planner_soty_rp_heads: 'session_hash,document',
  planner_soty_rebind_receipts:
    'request_hash,intent_digest,session_hash,continuation_digest,payload_cipher,key_id,expires_at,created_at',
};
const definitions = {
  planner_soty_rp_format:
    "CREATE TABLE planner_soty_rp_format(id INTEGER PRIMARY KEY CHECK(id=1),format TEXT NOT NULL CHECK(format='planner.source-rp.v2'),version INTEGER NOT NULL CHECK(version=2))",
  planner_soty_rp_anchors:
    'CREATE TABLE planner_soty_rp_anchors(session_hash TEXT PRIMARY KEY,locator_hash TEXT NOT NULL,profile_digest TEXT NOT NULL,binding_digest TEXT NOT NULL,payload_cipher TEXT NOT NULL,key_id TEXT NOT NULL,expires_at INTEGER NOT NULL,created_at INTEGER NOT NULL,generation INTEGER NOT NULL CHECK(generation>0),active INTEGER NOT NULL CHECK(active IN(0,1)))',
  planner_soty_rp_heads:
    'CREATE TABLE planner_soty_rp_heads(session_hash TEXT PRIMARY KEY REFERENCES planner_soty_rp_anchors(session_hash) ON DELETE CASCADE,document TEXT NOT NULL CHECK(json_valid(document)))',
  planner_soty_rebind_receipts:
    'CREATE TABLE planner_soty_rebind_receipts(request_hash TEXT PRIMARY KEY,intent_digest TEXT NOT NULL,session_hash TEXT NOT NULL REFERENCES planner_soty_rp_anchors(session_hash) ON DELETE CASCADE,continuation_digest TEXT NOT NULL,payload_cipher TEXT NOT NULL,key_id TEXT NOT NULL,expires_at INTEGER NOT NULL,created_at INTEGER NOT NULL)',
};
// Independent character scanner preserves whitespace inside quoted literals.
function wireSql(sql) {
  let output = '',
    quote = null;
  for (let i = 0; i < sql.length; i++) {
    const character = sql[i];
    if (quote) {
      output += character;
      if (character === quote) {
        if (sql[i + 1] === quote) output += sql[++i];
        else quote = null;
      }
    } else if (character === "'" || character === '"') {
      quote = character;
      output += character;
    } else if (!/\s/u.test(character) && character !== ';') output += character;
  }
  return output;
}
export function readCompatiblePlannerRp(path) {
  if (!existsSync(path)) return { supported: true, version: 1 };
  const db = new DatabaseSync(path, { readOnly: true });
  try {
    const objects = db
      .prepare(
        "SELECT type,name,sql FROM sqlite_master WHERE name LIKE 'planner_soty_rp_%' OR name='planner_soty_rebind_receipts' ORDER BY name",
      )
      .all();
    if (!objects.length) return { supported: true, version: 1 };
    const reject = () => {
      throw Object.assign(new Error('planner_rp_reader_incompatible'), {
        code: 'planner_rp_reader_incompatible',
      });
    };
    if (
      objects.length !== 4 ||
      objects.some((row) => row.type !== 'table' || !Object.hasOwn(columns, row.name))
    )
      reject();
    if (objects.some((row) => wireSql(row.sql) !== wireSql(definitions[row.name]))) reject();
    for (const [name, pin] of Object.entries(columns))
      if (
        db
          .prepare('PRAGMA table_info(' + name + ')')
          .all()
          .map((row) => row.name)
          .join(',') !== pin
      )
        reject();
    const rows = db.prepare('SELECT id,format,version FROM planner_soty_rp_format').all();
    if (
      rows.length !== 1 ||
      rows[0].id !== 1 ||
      rows[0].format !== 'planner.source-rp.v2' ||
      rows[0].version !== 2
    )
      reject();
    if (db.prepare('PRAGMA foreign_key_check').all().length) reject();
    return { supported: true, version: 2 };
  } finally {
    db.close();
  }
}
