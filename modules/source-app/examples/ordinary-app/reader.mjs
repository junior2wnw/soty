import { createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';

// Literal independent reader witness: no import of writer schema/DDL. Future
// extra tables/format need an explicit compatible reader and migration gate.
const literal = Object.freeze({
  native_consents: '8b4f6b727bfdbd1e05717d65b465f0475a171edf6a2e5b15cf0ae70a0b875cc4',
  native_items: 'e8ab468bb2e124a45eb167f767469f397e491a862027a23315fce3308265d855',
  native_links: '94af76a997dffdd309d646aacde66f528b7fedccce4c32fa006d9424fa9405d0',
  native_memberships: '1611afa49badb09b66571f262f48b135a1ec818d2b575805254eee7a1cbb59cf',
  native_meta: '36e68c08ecd63285101fd112c083f5863ec0b88b4b9af277e6fcc568d863a4bc',
  native_principals: 'd1e6a788010f1de8144415ae9cdd9a0cbeae4696298b3db4ae6055ec433623ff',
  native_receipts: '644c408c5d7d56584149d7f1bfffc4e56d997ac6a65a6f4382fda3effa67735b',
  native_resources: '6cb2dedcc13075e70436f469caaa3920a02c89a42dea6cf25f484f527cf70b78',
  native_sessions: '24104127f7f30fdea81437273c3eb43ed0735c44fba1a1429faa85e0a8d7d595',
  native_ticket_media: '40c3e800ea67b1c7061d576f7024629b967b27bfc56f53fe3e44cb5dcaa4bfd8',
  native_ticket_messages: '2b44d5aa923e2af0fd36f79fa413a3228e1310b25fd08777d8ac74283f7642f4',
  native_tickets: '48f98387796db64321892ed3280b55264f60225b46a9451f051fe728ded47c60',
  source_completions: '05c315ff0c320e8365ce03ddbf8bba3cb5a6151127334940306bebde9fdee551',
  source_interactions: '48a990754645cf478cb5d228c7a77032c2e7751a953e2abe9fa18beaa599763a',
  source_nonces: '25617a5ce84874d0f8b763388a11dfec8877077384fa0403644ff1d44ba79bb4',
  source_sessions: '833a3f28aee1d0ca9c2c30e786a54c0e2547c4ccb235deea437ce459390ca121',
  native_link_immutable: '261f6dd039302349515b82892764b89f1ed33468fbf45a575c488929ed925ffb',
  native_receipt_immutable: '7e05d368fc59331124ec44bc52bd27d74e5c38989cf703db523e0473cf4e73d8',
});
const refuse = () => { throw Object.assign(new Error('ordinary_source_reader_refused'), { code: 'ordinary_source_reader_refused', status: 503 }); };
export function verifyOrdinaryReader1(db, realmId) {
  const rows = db.prepare("SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").all();
  if (rows.length !== Object.keys(literal).length || !rows.every(row => Object.hasOwn(literal, row.name)
    && row.type === (row.name.endsWith('_immutable') ? 'trigger' : 'table') && createHash('sha256').update(row.sql).digest('hex') === literal[row.name])) refuse();
  const meta = db.prepare('SELECT * FROM native_meta').all();
  if (meta.length !== 1 || meta[0].format !== 1 || meta[0].realm_id !== realmId || db.prepare('PRAGMA foreign_key_check').all().length !== 0) refuse();
  return Object.freeze({ format: 1, objects: rows.length, realmId });
}
export function readOrdinaryFormat1(databasePath, realmId) {
  const db = new DatabaseSync(databasePath, { readOnly: true }); try { return verifyOrdinaryReader1(db, realmId); } finally { db.close(); }
}
