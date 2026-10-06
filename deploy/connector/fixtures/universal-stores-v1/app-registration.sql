
CREATE TABLE registration_metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL) STRICT;
CREATE TABLE registration_authorities(
 scope_key TEXT PRIMARY KEY CHECK(length(scope_key)=64),scope_json TEXT NOT NULL CHECK(json_valid(scope_json)),
 owner_id TEXT NOT NULL,generation INTEGER NOT NULL CHECK(generation BETWEEN 1 AND 1000000),
 fingerprint TEXT NOT NULL CHECK(length(fingerprint)=64),authority_digest TEXT NOT NULL CHECK(length(authority_digest)=64),
 updated_at INTEGER NOT NULL CHECK(updated_at BETWEEN 0 AND 9007199254740991)
) STRICT;
CREATE TABLE registration_heads(
 scope_key TEXT PRIMARY KEY REFERENCES registration_authorities(scope_key),app_id TEXT NOT NULL,owner_id TEXT NOT NULL,
 revision INTEGER NOT NULL CHECK(revision BETWEEN 1 AND 1000000),generation INTEGER NOT NULL CHECK(generation BETWEEN 1 AND 1000000),
 descriptor_digest TEXT NOT NULL CHECK(length(descriptor_digest)=64),authority_digest TEXT NOT NULL CHECK(length(authority_digest)=64),
 authority_revision INTEGER NOT NULL CHECK(authority_revision BETWEEN 1 AND 1000000),intent_digest TEXT NOT NULL CHECK(length(intent_digest)=64),
 request_id TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN ('pending-feedback','ready')),
 feedback_json TEXT CHECK(feedback_json IS NULL OR json_valid(feedback_json)),updated_at INTEGER NOT NULL CHECK(updated_at BETWEEN 0 AND 9007199254740991),
 CHECK((status='pending-feedback' AND feedback_json IS NULL) OR (status='ready' AND feedback_json IS NOT NULL))
) STRICT;
CREATE TABLE registration_versions(
 scope_key TEXT NOT NULL REFERENCES registration_heads(scope_key),generation INTEGER NOT NULL CHECK(generation BETWEEN 1 AND 1000000),
 committed_revision INTEGER NOT NULL CHECK(committed_revision BETWEEN 1 AND 1000000),
 descriptor_digest TEXT NOT NULL CHECK(length(descriptor_digest)=64),descriptor_json TEXT NOT NULL CHECK(json_valid(descriptor_json) AND length(CAST(descriptor_json AS BLOB))<=65536),
 authority_digest TEXT NOT NULL CHECK(length(authority_digest)=64),authority_revision INTEGER NOT NULL CHECK(authority_revision BETWEEN 1 AND 1000000),
 plan_json TEXT NOT NULL CHECK(json_valid(plan_json)),created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),
 PRIMARY KEY(scope_key,generation)
) STRICT;
CREATE TABLE registration_receipts(
 account_id TEXT NOT NULL,request_id TEXT NOT NULL,intent_hash TEXT NOT NULL CHECK(length(intent_hash)=64),
 scope_key TEXT NOT NULL,generation INTEGER NOT NULL,receipt_json TEXT NOT NULL CHECK(json_valid(receipt_json)),
 created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),
 PRIMARY KEY(account_id,request_id),FOREIGN KEY(scope_key,generation) REFERENCES registration_versions(scope_key,generation)
) STRICT;
CREATE TABLE registration_reference_history(
 kind TEXT NOT NULL,ref_id TEXT NOT NULL,ref_version INTEGER NOT NULL CHECK(ref_version BETWEEN 1 AND 1000000),
 content_digest TEXT NOT NULL CHECK(length(content_digest)=64),content_json TEXT NOT NULL CHECK(json_valid(content_json)),
 PRIMARY KEY(kind,ref_id,ref_version)
) STRICT;
CREATE TABLE registration_feedback_outbox(
 scope_key TEXT NOT NULL,generation INTEGER NOT NULL,provisioning_key TEXT NOT NULL CHECK(length(provisioning_key)=64),
 request_json TEXT NOT NULL CHECK(json_valid(request_json)),created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),
 PRIMARY KEY(scope_key,generation),FOREIGN KEY(scope_key,generation) REFERENCES registration_versions(scope_key,generation)
) STRICT;
CREATE INDEX registration_heads_owner ON registration_heads(owner_id,app_id);
CREATE INDEX registration_receipts_account ON registration_receipts(account_id,created_at);

CREATE TRIGGER registration_metadata_no_update BEFORE UPDATE ON registration_metadata BEGIN SELECT RAISE(ABORT,'registration_immutable'); END;
CREATE TRIGGER registration_metadata_no_delete BEFORE DELETE ON registration_metadata BEGIN SELECT RAISE(ABORT,'registration_immutable'); END;
CREATE TRIGGER registration_metadata_no_replace BEFORE INSERT ON registration_metadata
WHEN EXISTS(SELECT 1 FROM registration_metadata WHERE key=NEW.key)
BEGIN SELECT RAISE(ABORT,'registration_immutable'); END;

CREATE TRIGGER registration_versions_no_update BEFORE UPDATE ON registration_versions BEGIN SELECT RAISE(ABORT,'registration_immutable'); END;
CREATE TRIGGER registration_versions_no_delete BEFORE DELETE ON registration_versions BEGIN SELECT RAISE(ABORT,'registration_immutable'); END;
CREATE TRIGGER registration_versions_no_replace BEFORE INSERT ON registration_versions
WHEN EXISTS(SELECT 1 FROM registration_versions WHERE scope_key=NEW.scope_key AND generation=NEW.generation)
BEGIN SELECT RAISE(ABORT,'registration_immutable'); END;

CREATE TRIGGER registration_receipts_no_update BEFORE UPDATE ON registration_receipts BEGIN SELECT RAISE(ABORT,'registration_immutable'); END;
CREATE TRIGGER registration_receipts_no_delete BEFORE DELETE ON registration_receipts BEGIN SELECT RAISE(ABORT,'registration_immutable'); END;
CREATE TRIGGER registration_receipts_no_replace BEFORE INSERT ON registration_receipts
WHEN EXISTS(SELECT 1 FROM registration_receipts WHERE account_id=NEW.account_id AND request_id=NEW.request_id)
BEGIN SELECT RAISE(ABORT,'registration_immutable'); END;

CREATE TRIGGER registration_reference_history_no_update BEFORE UPDATE ON registration_reference_history BEGIN SELECT RAISE(ABORT,'registration_immutable'); END;
CREATE TRIGGER registration_reference_history_no_delete BEFORE DELETE ON registration_reference_history BEGIN SELECT RAISE(ABORT,'registration_immutable'); END;
CREATE TRIGGER registration_reference_history_no_replace BEFORE INSERT ON registration_reference_history
WHEN EXISTS(SELECT 1 FROM registration_reference_history WHERE kind=NEW.kind AND ref_id=NEW.ref_id AND ref_version=NEW.ref_version)
BEGIN SELECT RAISE(ABORT,'registration_immutable'); END;

CREATE TRIGGER registration_feedback_outbox_no_update BEFORE UPDATE ON registration_feedback_outbox BEGIN SELECT RAISE(ABORT,'registration_immutable'); END;
CREATE TRIGGER registration_feedback_outbox_no_delete BEFORE DELETE ON registration_feedback_outbox BEGIN SELECT RAISE(ABORT,'registration_immutable'); END;
CREATE TRIGGER registration_feedback_outbox_no_replace BEFORE INSERT ON registration_feedback_outbox
WHEN EXISTS(SELECT 1 FROM registration_feedback_outbox WHERE scope_key=NEW.scope_key AND generation=NEW.generation)
BEGIN SELECT RAISE(ABORT,'registration_immutable'); END;
CREATE TRIGGER registration_authorities_no_delete BEFORE DELETE ON registration_authorities BEGIN SELECT RAISE(ABORT,'registration_authority_required'); END;
CREATE TRIGGER registration_authorities_no_replace BEFORE INSERT ON registration_authorities
WHEN EXISTS(SELECT 1 FROM registration_authorities WHERE scope_key=NEW.scope_key) BEGIN SELECT RAISE(ABORT,'registration_authority_required'); END;
CREATE TRIGGER registration_authorities_monotonic BEFORE UPDATE ON registration_authorities
WHEN NEW.scope_key<>OLD.scope_key OR NEW.scope_json<>OLD.scope_json OR NEW.owner_id<>OLD.owner_id
 OR NEW.generation<>OLD.generation+1 OR NEW.fingerprint=OLD.fingerprint
BEGIN SELECT RAISE(ABORT,'registration_authority_required'); END;
CREATE TRIGGER registration_heads_no_delete BEFORE DELETE ON registration_heads BEGIN SELECT RAISE(ABORT,'registration_head_required'); END;
CREATE TRIGGER registration_heads_no_replace BEFORE INSERT ON registration_heads
WHEN EXISTS(SELECT 1 FROM registration_heads WHERE scope_key=NEW.scope_key) BEGIN SELECT RAISE(ABORT,'registration_head_required'); END;
CREATE TRIGGER registration_heads_monotonic BEFORE UPDATE ON registration_heads
WHEN NEW.scope_key<>OLD.scope_key OR NEW.app_id<>OLD.app_id OR NEW.owner_id<>OLD.owner_id OR NEW.revision<>OLD.revision+1
 OR NEW.generation NOT IN (OLD.generation,OLD.generation+1)
 OR (NEW.generation=OLD.generation AND (NEW.descriptor_digest<>OLD.descriptor_digest OR NEW.authority_digest<>OLD.authority_digest
   OR NEW.authority_revision<>OLD.authority_revision OR NEW.intent_digest<>OLD.intent_digest OR NEW.request_id<>OLD.request_id
   OR OLD.status='ready' OR NEW.status<>'ready'))
 OR (NEW.generation=OLD.generation+1 AND NEW.status<>'pending-feedback')
BEGIN SELECT RAISE(ABORT,'registration_head_required'); END;
