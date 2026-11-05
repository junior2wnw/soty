import {DatabaseSync} from 'node:sqlite';
import {createHash} from 'node:crypto';
import {check} from './model.mjs';

const baseNames=new Set(['policies','links','links_app','requests','requests_app','members','members_account','commands']);
const baseHash='8aad8c6eb9c6f097818b39bb39c90c12f4d4b103794c0354b1f6a21b6a33a75b';
export const INVITATION_STORAGE_VERSION=2;
const extraSql=`
CREATE TABLE target_kinds(target_id TEXT PRIMARY KEY,kind TEXT NOT NULL CHECK(kind IN('app','community')));
CREATE TABLE link_assignments(link_id TEXT PRIMARY KEY REFERENCES links(id),recipient_label TEXT NOT NULL,recipient_account_id TEXT);
CREATE TABLE request_subjects(request_id TEXT PRIMARY KEY REFERENCES requests(id),device_id TEXT NOT NULL,account_label TEXT NOT NULL);
CREATE TABLE admissions(claim_id TEXT PRIMARY KEY,request_id TEXT NOT NULL UNIQUE REFERENCES requests(id),
 target_id TEXT NOT NULL,target_kind TEXT NOT NULL CHECK(target_kind IN('app','community')),owner_id TEXT NOT NULL,
 issuer_id TEXT NOT NULL,recipient_id TEXT NOT NULL,recipient_device_id TEXT NOT NULL,recipient_account_label TEXT NOT NULL,
 recipient_label TEXT NOT NULL,state TEXT NOT NULL CHECK(state IN('pending','admitted','blocked')),error_code TEXT,updated_at INTEGER NOT NULL);
CREATE INDEX admissions_target ON admissions(target_id,recipient_id);
`;
const layout=db=>db.prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").all()
 .map(row=>[row.type,row.name,row.tbl_name,typeof row.sql==='string'?row.sql.replace(/\s+/gu,' ').trim():null]);
const hash=value=>createHash('sha256').update(JSON.stringify(value)).digest('hex');
const expectedExtra=(()=>{const db=new DatabaseSync(':memory:');try{db.exec(extraSql);return layout(db);}finally{db.close();}})();
function assertLayout(db,version){const rows=layout(db),base=rows.filter(row=>baseNames.has(row[1]));
 check(base.length===8&&hash(base)===baseHash,'invitation_store_invalid',503);
 const extra=rows.filter(row=>!baseNames.has(row[1]));check(version===1?!extra.length:JSON.stringify(extra)===JSON.stringify(expectedExtra),'invitation_store_invalid',503);
 check(db.prepare('PRAGMA quick_check(1)').get().quick_check==='ok'&&!db.prepare('PRAGMA foreign_key_check').get(),'invitation_store_invalid',503);
}
export function initializeInvitationStorage(db,allowMigration2){
 const version=Number(db.prepare('PRAGMA user_version').get().user_version);assertLayout(db,version);
 if(version===2)return;
 check(version===1&&allowMigration2===true,'invitation_migration_required',503);
 db.exec('BEGIN IMMEDIATE');try{db.exec(extraSql);db.exec(`INSERT INTO target_kinds SELECT app_id,'app' FROM policies UNION SELECT app_id,'app' FROM links UNION SELECT app_id,'app' FROM requests UNION SELECT app_id,'app' FROM members; PRAGMA user_version=2;`);assertLayout(db,2);db.exec('COMMIT');}catch(error){if(db.isTransaction)db.exec('ROLLBACK');throw error;}
}
