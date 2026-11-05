import {DatabaseSync} from 'node:sqlite';
import {randomUUID} from 'node:crypto';
import {check,digest} from './validation.mjs';
export const MONITORING_FORMAT=1;
export const MONITORING_LINEAGE='soty.monitoring.sqlite.v1';
/** Release probes must pin this literal independently; this initializer cannot attest its own image. */
export const MONITORING_DDL=`
CREATE TABLE monitoring_meta(id INTEGER PRIMARY KEY CHECK(id=1),project_id TEXT NOT NULL,lineage TEXT NOT NULL,registry_id TEXT NOT NULL) STRICT;
CREATE TABLE monitoring_configs(scope_key TEXT PRIMARY KEY,enabled INTEGER NOT NULL CHECK(enabled IN(0,1)),revision INTEGER NOT NULL CHECK(revision>0),updated_at INTEGER NOT NULL) STRICT;
CREATE TABLE monitoring_metrics(scope_key TEXT NOT NULL,metric_id TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN('counter','gauge','distribution')),unit TEXT NOT NULL,PRIMARY KEY(scope_key,metric_id)) STRICT;
CREATE TABLE monitoring_buckets(scope_key TEXT NOT NULL,metric_id TEXT NOT NULL,bucket_at INTEGER NOT NULL,samples INTEGER NOT NULL CHECK(samples>0),sum REAL NOT NULL,min REAL NOT NULL,max REAL NOT NULL,last REAL NOT NULL,last_at INTEGER NOT NULL,PRIMARY KEY(scope_key,metric_id,bucket_at)) STRICT;
CREATE INDEX monitoring_retention ON monitoring_buckets(bucket_at);
CREATE TABLE monitoring_receipts(scope_key TEXT NOT NULL,request_id TEXT NOT NULL,operation TEXT NOT NULL,digest TEXT NOT NULL,result_json TEXT NOT NULL,created_at INTEGER NOT NULL,PRIMARY KEY(scope_key,request_id)) STRICT;
CREATE INDEX monitoring_receipt_retention ON monitoring_receipts(created_at);
CREATE TABLE monitoring_audit(sequence INTEGER PRIMARY KEY AUTOINCREMENT,scope_key TEXT NOT NULL,operation TEXT NOT NULL,revision INTEGER NOT NULL,created_at INTEGER NOT NULL) STRICT;
CREATE INDEX monitoring_audit_scope ON monitoring_audit(scope_key,sequence);
PRAGMA user_version=1;
`;
const layout=db=>db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all().map(row=>[row.type,row.name,row.tbl_name,row.sql?.replace(/\s+/gu,' ').trim()]);
let expected;
export function monitoringSchemaFingerprint(){if(!expected){const db=new DatabaseSync(':memory:');try{db.exec(MONITORING_DDL);expected=digest(layout(db));}finally{db.close();}}return expected;}
export function inspectMonitoringStorage(db,projectId){check(db.prepare('PRAGMA user_version').get().user_version===MONITORING_FORMAT&&digest(layout(db))===monitoringSchemaFingerprint(),'monitoring_storage_format_unsupported',503);const rows=db.prepare('SELECT * FROM monitoring_meta LIMIT 2').all(),row=rows[0];check(rows.length===1&&row.id===1&&row.project_id===projectId&&row.lineage===MONITORING_LINEAGE&&/^[a-f0-9-]{36}$/u.test(row.registry_id),'monitoring_storage_identity_invalid',503);const result=db.prepare('PRAGMA quick_check(1)').all();check(result.length===1&&Object.values(result[0])[0]==='ok','monitoring_storage_unavailable',503);return Object.freeze({schemaVersion:MONITORING_FORMAT,registryId:row.registry_id,projectId});}
export function initializeMonitoringStorage(db,projectId){const version=db.prepare('PRAGMA user_version').get().user_version;const empty=db.prepare("SELECT count(*) AS n FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'").get().n===0;if(empty)check(version===0,'monitoring_storage_format_unsupported',503);else inspectMonitoringStorage(db,projectId);db.exec('PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=1000;');if(empty){db.exec('BEGIN IMMEDIATE');try{db.exec(MONITORING_DDL);db.prepare('INSERT INTO monitoring_meta VALUES(1,?,?,?)').run(projectId,MONITORING_LINEAGE,randomUUID());db.exec('COMMIT');}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}}return inspectMonitoringStorage(db,projectId);}
