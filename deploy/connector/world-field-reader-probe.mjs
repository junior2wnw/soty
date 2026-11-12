import { createHash } from 'node:crypto';

// Frozen independently from the reviewed eight epoch1 field objects. The
// target runtime constructs its own tables, but cannot choose this oracle.
export const WORLD_FIELD_LAYOUT_SHA256 = '5445f5e0bdfb40478d5676010e5de07b1b4467c45073ab805e9ded869b5d9a08';
export const WORLD_FIELD_FIXTURE_JSON = '{"schema":"soty.field.v2","contexts":[{"contextId":"probe_context","title":"Synthetic","x":0,"y":0}],"shortcuts":[{"shortcutId":"probe_shortcut","entity":{"kind":"object","id":"probe_object"},"contextId":"probe_context","placement":{"kind":"free","x":128,"y":64,"width":320,"height":180}}]}';
const sha = value => createHash('sha256').update(value).digest('hex');
export const WORLD_FIELD_FIXTURE_SHA256 = sha(WORLD_FIELD_FIXTURE_JSON);
export const WORLD_FIELD_READER_RESULT = Object.freeze({ ok: true, schema: 'soty.world-field-reader.v1',
  fieldEpoch: 2, baseWorldFormat: 6, layoutSha256: WORLD_FIELD_LAYOUT_SHA256, fixtureSha256: WORLD_FIELD_FIXTURE_SHA256, noRewrite: true });
const need = condition => { if (!condition) throw Error('world_field_reader_incompatible'); };
export function worldFieldReaderResult(baseWorldFormat) {
  need([6, 7].includes(baseWorldFormat));
  return Object.freeze({ ...WORLD_FIELD_READER_RESULT, baseWorldFormat });
}

export async function checkWorldFieldReader(moduleUrl = 'file:///app/modules/world/server/field.mjs', baseWorldFormat = 6) {
  need(Number(process.versions.node.split('.')[0]) === 24 && [6, 7].includes(baseWorldFormat));
  const { DatabaseSync } = await import('node:sqlite');
  const target = await import(moduleUrl), db = new DatabaseSync(':memory:');
  try {
    need(typeof target.migrateField === 'function' && typeof target.fieldOperation === 'function');
    db.exec(`PRAGMA foreign_keys=ON; CREATE TABLE world_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
      CREATE TABLE profiles(account_id TEXT PRIMARY KEY);
      INSERT INTO world_meta VALUES('project_id','soty'),('lineage','soty.world.sqlite.v1'),('schema_version','${baseWorldFormat}');
      PRAGMA user_version=${baseWorldFormat}; INSERT INTO profiles VALUES('synthetic_field_probe');`);
    const schema = () => db.prepare('SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name').all();
    const profiles = () => db.prepare('SELECT * FROM profiles ORDER BY account_id').all();
    const initialCore = JSON.stringify([schema(), profiles()]);
    target.migrateField(db);
    const epoch = () => db.prepare("SELECT value FROM world_meta WHERE key='field_schema_version'").get()?.value;
    need(epoch() === '1');
    const layout = () => db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name GLOB 'world_field_*' ORDER BY name")
      .all().map(row => [row.type, row.name, row.tbl_name, row.sql?.replace(/\s+/gu, ' ').trim()]);
    need(layout().length === 8 && sha(JSON.stringify(layout())) === WORLD_FIELD_LAYOUT_SHA256);
    need(db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'").get().n === 10);
    const coreSchema = schema().filter(row => !row.name.startsWith('world_field_') && !row.name.startsWith('sqlite_autoindex_world_field_'));
    need(JSON.stringify([coreSchema, profiles()]) === initialCore);
    const initializedSchema = JSON.stringify(schema()), initializedProfiles = JSON.stringify(profiles());
    const m = { get: (sql, ...args) => db.prepare(sql).get(...args), run: (sql, ...args) => db.prepare(sql).run(...args) };
    const actor = { accountId: 'synthetic_field_probe' }, time = 1800000000123, requestId = 'synthetic_field_v2_probe';
    db.exec('BEGIN IMMEDIATE');
    let result;
    try {
      result = target.fieldOperation(m, 'world.field.put', { expectedAccountId: actor.accountId,
        expectedRevision: 0, requestId, document: JSON.parse(WORLD_FIELD_FIXTURE_JSON) }, actor, time);
      db.exec('COMMIT');
    } catch (error) { db.exec('ROLLBACK'); throw error; }
    need(epoch() === '2' && result.replayed === false && result.receipt?.revision === 1
      && result.receipt.contentHash === WORLD_FIELD_FIXTURE_SHA256 && result.receipt.committedAt === time);
    need(JSON.stringify(schema()) === initializedSchema && JSON.stringify(profiles()) === initializedProfiles
      && db.prepare('PRAGMA user_version').get().user_version === baseWorldFormat);
    need(JSON.stringify(db.prepare('SELECT key,value FROM world_meta ORDER BY key').all()) === JSON.stringify([
      { key: 'field_schema_version', value: '2' }, { key: 'lineage', value: 'soty.world.sqlite.v1' },
      { key: 'project_id', value: 'soty' }, { key: 'schema_version', value: String(baseWorldFormat) }]));
    const documents = () => db.prepare('SELECT account_id,revision,document_json,content_hash,updated_at FROM world_field_documents ORDER BY account_id').all();
    const receipts = () => db.prepare('SELECT account_id,request_key,intent_hash,content_hash,accepted_revision,committed_at FROM world_field_receipts ORDER BY account_id,request_key').all();
    need(JSON.stringify(documents()) === JSON.stringify([{ account_id: actor.accountId, revision: 1,
      document_json: WORLD_FIELD_FIXTURE_JSON, content_hash: WORLD_FIELD_FIXTURE_SHA256, updated_at: time }]));
    need(JSON.stringify(receipts()) === JSON.stringify([{ account_id: actor.accountId, request_key: sha(requestId),
      intent_hash: sha(JSON.stringify(['soty.field.put.v1', 0, WORLD_FIELD_FIXTURE_SHA256])),
      content_hash: WORLD_FIELD_FIXTURE_SHA256, accepted_revision: 1, committed_at: time }]));
    const snapshot = () => JSON.stringify([db.prepare('PRAGMA user_version').get(), schema(), profiles(),
      db.prepare('SELECT * FROM world_meta ORDER BY key').all(), documents(), receipts()]);
    const before = snapshot();
    target.migrateField(db);
    need(typeof target.readCanvasFieldSource === 'function');
    const value = target.readCanvasFieldSource(m, actor.accountId);
    need(value.revision === 1 && value.updatedAt === time && value.contentHash === WORLD_FIELD_FIXTURE_SHA256
      && JSON.stringify(value.document) === WORLD_FIELD_FIXTURE_JSON);
    const read = target.fieldOperation(m, 'world.field.get', { expectedAccountId: actor.accountId }, actor, time + 1);
    need(JSON.stringify(read) === JSON.stringify(value) && snapshot() === before);
    return worldFieldReaderResult(baseWorldFormat);
  } finally { db.close(); }
}

if (process.env.SOTY_WORLD_FIELD_PROBE === '1') {
  try { process.stdout.write(JSON.stringify(await checkWorldFieldReader(undefined, Number(process.env.SOTY_WORLD_FIELD_BASE)))); }
  catch { process.stdout.write(JSON.stringify({ ok: false, schema: 'soty.world-field-reader.v1', code: 'world_field_reader_incompatible' })); process.exitCode = 1; }
}
