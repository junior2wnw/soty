/** New empty example Source, not a migration/shadow of an existing app. */
export const ORDINARY_SCHEMA = `
CREATE TABLE native_meta(format INTEGER NOT NULL CHECK(format=1),realm_id TEXT NOT NULL UNIQUE);
CREATE TABLE native_resources(id TEXT PRIMARY KEY,incarnation_id TEXT NOT NULL,realm_id TEXT NOT NULL,title TEXT NOT NULL,guest_empty INTEGER NOT NULL CHECK(guest_empty IN(0,1)));
CREATE TABLE native_principals(id TEXT PRIMARY KEY,realm_id TEXT NOT NULL);
CREATE TABLE native_sessions(id_hash TEXT PRIMARY KEY,principal_id TEXT NOT NULL REFERENCES native_principals(id),generation INTEGER NOT NULL,active INTEGER NOT NULL CHECK(active IN(0,1)),expires_at INTEGER NOT NULL);
CREATE TABLE native_memberships(resource_id TEXT NOT NULL REFERENCES native_resources(id),principal_id TEXT NOT NULL REFERENCES native_principals(id),role TEXT NOT NULL CHECK(role IN('owner','participant')),active INTEGER NOT NULL CHECK(active IN(0,1)),revision INTEGER NOT NULL,PRIMARY KEY(resource_id,principal_id));
CREATE TABLE native_links(issuer TEXT NOT NULL,subject TEXT NOT NULL,principal_id TEXT NOT NULL REFERENCES native_principals(id),PRIMARY KEY(issuer,subject));
CREATE TABLE native_consents(principal_id TEXT NOT NULL REFERENCES native_principals(id),resource_id TEXT NOT NULL REFERENCES native_resources(id),semantic_digest TEXT NOT NULL,root_device_id TEXT NOT NULL,native_session_hash TEXT NOT NULL REFERENCES native_sessions(id_hash),PRIMARY KEY(principal_id,resource_id,semantic_digest,root_device_id));
CREATE TABLE native_items(id TEXT PRIMARY KEY,resource_id TEXT NOT NULL REFERENCES native_resources(id),title TEXT NOT NULL,revision INTEGER NOT NULL,created_at INTEGER NOT NULL);
CREATE TABLE native_receipts(resource_id TEXT NOT NULL REFERENCES native_resources(id),principal_id TEXT NOT NULL REFERENCES native_principals(id),request_id TEXT NOT NULL,input_digest TEXT NOT NULL,result_json TEXT NOT NULL,PRIMARY KEY(resource_id,principal_id,request_id));
CREATE TABLE native_tickets(id TEXT PRIMARY KEY,resource_id TEXT NOT NULL REFERENCES native_resources(id),reporter_id TEXT NOT NULL REFERENCES native_principals(id),body TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN('received','in_progress','needs_action','ready_to_check','resolved')),revision INTEGER NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
CREATE TABLE native_ticket_media(ticket_id TEXT NOT NULL REFERENCES native_tickets(id),ordinal INTEGER NOT NULL,metadata_json TEXT NOT NULL,bytes BLOB NOT NULL,PRIMARY KEY(ticket_id,ordinal));
CREATE TABLE native_ticket_messages(id TEXT PRIMARY KEY,ticket_id TEXT NOT NULL REFERENCES native_tickets(id),kind TEXT NOT NULL CHECK(kind IN('reporter','support')),body TEXT NOT NULL,created_at INTEGER NOT NULL);
CREATE TABLE source_interactions(id_hash TEXT PRIMARY KEY,revision INTEGER NOT NULL,phase TEXT NOT NULL CHECK(phase IN('pending','claimed','exchanging','completed')),expires_at INTEGER NOT NULL,cipher TEXT NOT NULL,key_id TEXT NOT NULL);
CREATE TABLE source_sessions(id_hash TEXT PRIMARY KEY,native_session_hash TEXT NOT NULL REFERENCES native_sessions(id_hash),principal_id TEXT NOT NULL REFERENCES native_principals(id),resource_id TEXT NOT NULL REFERENCES native_resources(id),active INTEGER NOT NULL CHECK(active IN(0,1)),expires_at INTEGER NOT NULL,cipher TEXT NOT NULL,key_id TEXT NOT NULL);
CREATE TABLE source_completions(id_hash TEXT PRIMARY KEY,interaction_hash TEXT NOT NULL UNIQUE REFERENCES source_interactions(id_hash),session_hash TEXT NOT NULL REFERENCES source_sessions(id_hash),consumed INTEGER NOT NULL CHECK(consumed IN(0,1)));
CREATE TABLE source_nonces(nonce TEXT PRIMARY KEY,expires_at INTEGER NOT NULL);
CREATE TRIGGER native_link_immutable BEFORE UPDATE ON native_links BEGIN SELECT RAISE(ABORT,'native_link_immutable'); END;
CREATE TRIGGER native_receipt_immutable BEFORE UPDATE ON native_receipts BEGIN SELECT RAISE(ABORT,'native_receipt_immutable'); END;
`;
