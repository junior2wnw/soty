import { ORDINARY_SCHEMA_2, ORDINARY_META_2 } from './schema.mjs';

// Additive, opt-in Source format3. Reader3 must be shipped before initialization
// or explicit migration. Existing Native2 rows/cipher/FKs remain byte-identical.
export const ORDINARY_META_3 = 'CREATE TABLE native_meta(format INTEGER NOT NULL CHECK(format IN(1,2,3)),realm_id TEXT NOT NULL UNIQUE)';
export const ORDINARY_JOB_DDL = `
CREATE TABLE native_processing_consents(id TEXT PRIMARY KEY,ticket_id TEXT NOT NULL REFERENCES native_tickets(id),reporter_id TEXT NOT NULL REFERENCES native_principals(id),ticket_revision INTEGER NOT NULL,attachment_digest TEXT NOT NULL,purpose TEXT NOT NULL CHECK(purpose IN('asr','ocr','triage')),policy_digest TEXT NOT NULL,native_session_hash TEXT NOT NULL REFERENCES native_sessions(id_hash),native_session_generation INTEGER NOT NULL,source_session_hash TEXT NOT NULL REFERENCES source_sessions(id_hash),source_key_id TEXT NOT NULL,membership_revision INTEGER NOT NULL,expires_at INTEGER NOT NULL,created_at INTEGER NOT NULL);
CREATE TRIGGER native_processing_consent_immutable BEFORE UPDATE ON native_processing_consents BEGIN SELECT RAISE(ABORT,'processing consent immutable'); END;
CREATE TABLE native_processor_grants(id TEXT PRIMARY KEY,resource_id TEXT NOT NULL REFERENCES native_resources(id),ticket_id TEXT NOT NULL REFERENCES native_tickets(id),owner_id TEXT NOT NULL REFERENCES native_principals(id),native_session_hash TEXT NOT NULL REFERENCES native_sessions(id_hash),source_session_hash TEXT NOT NULL REFERENCES source_sessions(id_hash),consent_id TEXT NOT NULL REFERENCES native_processing_consents(id),request_id TEXT NOT NULL,input_digest TEXT NOT NULL,expires_at INTEGER NOT NULL,created_at INTEGER NOT NULL,cipher TEXT NOT NULL,key_id TEXT NOT NULL,UNIQUE(resource_id,owner_id,request_id));
CREATE TRIGGER native_processor_grant_immutable BEFORE UPDATE ON native_processor_grants BEGIN SELECT RAISE(ABORT,'processor grant immutable'); END;
CREATE TABLE native_processor_revocations(grant_id TEXT PRIMARY KEY REFERENCES native_processor_grants(id),created_at INTEGER NOT NULL);
CREATE TRIGGER native_processor_revocation_immutable BEFORE UPDATE ON native_processor_revocations BEGIN SELECT RAISE(ABORT,'processor revoke immutable'); END;
CREATE TABLE native_feedback_jobs(id TEXT PRIMARY KEY,grant_id TEXT NOT NULL UNIQUE REFERENCES native_processor_grants(id),state TEXT NOT NULL CHECK(state IN('queued','started','completed','unknown','revoked')),revision INTEGER NOT NULL,claim_hash TEXT,started_at INTEGER,finished_at INTEGER);
CREATE TABLE native_processor_receipts(job_id TEXT PRIMARY KEY REFERENCES native_feedback_jobs(id),input_digest TEXT NOT NULL,result_digest TEXT NOT NULL,created_at INTEGER NOT NULL,cipher TEXT NOT NULL,key_id TEXT NOT NULL);
CREATE TRIGGER native_processor_receipt_immutable BEFORE UPDATE ON native_processor_receipts BEGIN SELECT RAISE(ABORT,'processor receipt immutable'); END;
`;
export const ORDINARY_SCHEMA_3 = ORDINARY_SCHEMA_2.replace(ORDINARY_META_2, ORDINARY_META_3) + ORDINARY_JOB_DDL;
