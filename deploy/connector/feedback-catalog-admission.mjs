import { createHash } from 'node:crypto';

// Independently pinned ae4c75f6 catalog metadata. This is not a configurable
// migration filter, a grant, or an import of the candidate's initializer.
export const FEEDBACK_CATALOG_ADMISSION = Object.freeze({
  schema: 'soty.feedback-catalog-admission.v1',
  path: 'capabilities/capabilities.sqlite',
  rowsSha256: '458c283f449224e8458f543b4e306a219ff3a4e003fda5b3294d8c163c671102',
  tableSchemaSha256: '961676ba1779cc5afd9d1a1f0c293e567e957ca2b4ae6870117ba42eb043ed23',
  rows: Object.freeze([
    ['accept', 'd20701b60d76427bf6b0360b0018f6191a6f44afb28bc3919c9cb65795297fc2'],
    ['context', '92ff669e5452257fc702970528af8059aa17000cf569e1b503aa6434cc662ba2'],
    ['get', '26ed2b61a0c3db25cf893a727c4da3e2b9ebb62f7cc3b16095bc56cf9f216c83'],
    ['list', '96f196ba897e9ef23597899b9b7c9f7c9ac9d0345c8ff966f705d86423be2b58'],
    ['reply', 'f62f4deae46af8a2a9ec20babc3b00b8c42aa32be38cf13b269e38a8e7038529'],
    ['status', '0408193eb74f58934b139186b2e7c4513c67952a52cd23cadf77f24908eed205'],
    ['submit', '82fb16d1ae16acd7701db9ca9c90773ed28caa5d1f79d545df497b05b4ff07cc'],
  ].map(([operation, digest]) => Object.freeze({ capability_id: 'apps.feedback.' + operation, version: 1, digest }))),
});
const fail = () => { throw Object.assign(new Error('root_state_catalog_admission_invalid'), {
  code: 'root_state_catalog_admission_invalid', stack: 'Error: root_state_catalog_admission_invalid',
}); };
const need = value => { if (!value) fail(); };
const digest = value => createHash('sha256').update(JSON.stringify(value)).digest('hex');

export function captureFeedbackCatalogAdmission(input) {
  need(input && typeof input === 'object' && !Array.isArray(input)
    && [Object.prototype, null].includes(Object.getPrototypeOf(input)));
  const fields = Object.getOwnPropertyDescriptors(input);
  need(Reflect.ownKeys(fields).length === 2 && ['phase', 'rowsSha256'].every(key =>
    fields[key]?.enumerable && Object.hasOwn(fields[key], 'value')));
  need(['before', 'after'].includes(fields.phase.value) && fields.rowsSha256.value === FEEDBACK_CATALOG_ADMISSION.rowsSha256);
  return Object.freeze({ phase: fields.phase.value, rowsSha256: fields.rowsSha256.value });
}

// Invoked on the observer's independently copied, read-only SQLite snapshot.
// The full observer still hashes every raw row and enforces its original caps.
export function inspectFeedbackCatalogAdmission(db, relativePath, input) {
  const admission = captureFeedbackCatalogAdmission(input), policy = FEEDBACK_CATALOG_ADMISSION;
  need(relativePath === policy.path);
  const schema = db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE tbl_name='cap_contracts' ORDER BY type,name LIMIT 3").all();
  need(schema.length === 2 && digest(schema) === policy.tableSchemaSha256);
  const select = db.prepare('SELECT rowid,capability_id,version,digest FROM cap_contracts WHERE capability_id IN ('
    + policy.rows.map(() => '?').join(',') + ') LIMIT 8');
  select.setReadBigInts(true);
  const rows = select.all(...policy.rows.map(row => row.capability_id));
  if (admission.phase === 'before') need(rows.length === 0);
  else {
    need(rows.length === 7);
    const seen = new Set();
    for (const row of rows) {
      const expected = policy.rows.find(value => value.capability_id === row.capability_id);
      need(expected && !seen.has(row.capability_id) && row.version === 1n && row.digest === expected.digest
        && typeof row.rowid === 'bigint');
      seen.add(row.capability_id);
    }
  }
  return { rowids: new Set(rows.map(row => row.rowid)), proof: Object.freeze({ schema: policy.schema,
    phase: admission.phase, rowsSha256: policy.rowsSha256, tableSchemaSha256: policy.tableSchemaSha256,
    beforeAbsent: admission.phase === 'before', afterExact: admission.phase === 'after', addedRows: rows.length }) };
}
