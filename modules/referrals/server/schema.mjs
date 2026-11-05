import { randomUUID } from 'node:crypto';
import { check } from './validation.mjs';

export const SCHEMA_VERSION = 1;
export function initialize(db, projectId) {
  const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all();
  if (tables.length) {
    check(tables.some(row => row.name === 'referrals_meta'), 'referrals_storage_not_recognized');
    const meta = db.prepare('SELECT * FROM referrals_meta WHERE id=1').get();
    check(meta?.project_id === projectId, 'referrals_project_mismatch');
    check(meta?.schema_version === SCHEMA_VERSION, 'referrals_schema_unsupported');
  }
  db.exec('PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=1000;');
  if (!tables.length) {
    db.exec('BEGIN IMMEDIATE');
    try {
      db.exec(`
        CREATE TABLE referrals_meta(id INTEGER PRIMARY KEY CHECK(id=1),project_id TEXT NOT NULL,registry_id TEXT NOT NULL,schema_version INTEGER NOT NULL) STRICT;
        CREATE TABLE referral_configs(app_id TEXT PRIMARY KEY,revision INTEGER NOT NULL,rates_json TEXT NOT NULL,updated_at INTEGER NOT NULL) STRICT;
        CREATE TABLE referral_config_versions(app_id TEXT NOT NULL,revision INTEGER NOT NULL,rates_json TEXT NOT NULL,configured_by TEXT NOT NULL,updated_at INTEGER NOT NULL,PRIMARY KEY(app_id,revision)) STRICT;
        CREATE TABLE referral_attributions(app_id TEXT NOT NULL,account_id TEXT NOT NULL,parent_id TEXT,created_at INTEGER NOT NULL,PRIMARY KEY(app_id,account_id),CHECK(parent_id IS NULL OR parent_id<>account_id)) STRICT;
        CREATE TABLE referral_payments(app_id TEXT NOT NULL,payment_id TEXT NOT NULL,payer_id TEXT NOT NULL,currency TEXT NOT NULL,eligible_amount INTEGER NOT NULL CHECK(eligible_amount>=0),refunded_amount INTEGER NOT NULL DEFAULT 0 CHECK(refunded_amount>=0 AND refunded_amount<=eligible_amount),config_revision INTEGER NOT NULL,rates_json TEXT NOT NULL,created_at INTEGER NOT NULL,PRIMARY KEY(app_id,payment_id)) STRICT;
        CREATE TABLE referral_commissions(app_id TEXT NOT NULL,payment_id TEXT NOT NULL,level INTEGER NOT NULL,beneficiary_id TEXT NOT NULL,amount INTEGER NOT NULL CHECK(amount>=0),reversed_amount INTEGER NOT NULL DEFAULT 0 CHECK(reversed_amount>=0 AND reversed_amount<=amount),PRIMARY KEY(app_id,payment_id,level),FOREIGN KEY(app_id,payment_id) REFERENCES referral_payments(app_id,payment_id)) STRICT;
        CREATE TABLE referral_ledger(id INTEGER PRIMARY KEY AUTOINCREMENT,app_id TEXT NOT NULL,beneficiary_id TEXT NOT NULL,currency TEXT NOT NULL,payment_id TEXT NOT NULL,event_kind TEXT NOT NULL CHECK(event_kind IN('payment','refund')),event_id TEXT NOT NULL,level INTEGER NOT NULL,amount INTEGER NOT NULL,created_at INTEGER NOT NULL,UNIQUE(app_id,event_kind,event_id,level),FOREIGN KEY(app_id,payment_id) REFERENCES referral_payments(app_id,payment_id)) STRICT;
        CREATE INDEX referral_ledger_owner ON referral_ledger(app_id,beneficiary_id,id);
        CREATE TABLE referral_balances(app_id TEXT NOT NULL,beneficiary_id TEXT NOT NULL,currency TEXT NOT NULL,amount TEXT NOT NULL CHECK(length(amount) BETWEEN 1 AND 32 AND amount NOT GLOB '*[^0-9]*'),PRIMARY KEY(app_id,beneficiary_id,currency)) STRICT;
        CREATE TABLE referral_receipts(app_id TEXT NOT NULL,kind TEXT NOT NULL,event_id TEXT NOT NULL,digest TEXT NOT NULL,result TEXT NOT NULL,PRIMARY KEY(app_id,kind,event_id)) STRICT;
      `);
      db.prepare('INSERT INTO referrals_meta VALUES(1,?,?,?)').run(projectId, randomUUID(), SCHEMA_VERSION);
      for (const table of ['referrals_meta', 'referral_config_versions', 'referral_attributions', 'referral_ledger', 'referral_receipts']) {
        for (const operation of ['UPDATE', 'DELETE']) db.exec(`CREATE TRIGGER ${table}_no_${operation.toLowerCase()} BEFORE ${operation} ON ${table} BEGIN SELECT RAISE(ABORT,'referrals_record_immutable'); END;`);
      }
      db.exec('COMMIT');
    } catch (error) { if (db.isTransaction) db.exec('ROLLBACK'); throw error; }
  }
  return db.prepare('SELECT * FROM referrals_meta WHERE id=1').get();
}
