
CREATE TABLE feedback_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL) STRICT;
CREATE TABLE feedback_installations(
 id TEXT PRIMARY KEY,provisioning_key TEXT NOT NULL UNIQUE,registry_id TEXT NOT NULL,tenant_id TEXT NOT NULL,
 app_id TEXT NOT NULL,environment_id TEXT NOT NULL,created_at INTEGER NOT NULL
) STRICT;
CREATE TABLE feedback_provider_receipts(
 receipt_key TEXT PRIMARY KEY,installation_id TEXT NOT NULL REFERENCES feedback_installations(id),
 proof_json TEXT NOT NULL,created_at INTEGER NOT NULL
) STRICT;
CREATE TABLE feedback_tickets(
 id TEXT PRIMARY KEY,installation_id TEXT NOT NULL REFERENCES feedback_installations(id),app_id TEXT NOT NULL,
 reporter_id TEXT NOT NULL,owner_id TEXT NOT NULL,body TEXT NOT NULL,status TEXT NOT NULL,
 revision INTEGER NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL
) STRICT;
CREATE TABLE feedback_attachments(
 id TEXT PRIMARY KEY,ticket_id TEXT NOT NULL REFERENCES feedback_tickets(id),ordinal INTEGER NOT NULL,
 kind TEXT NOT NULL,name TEXT NOT NULL,mime_type TEXT NOT NULL,byte_length INTEGER NOT NULL,bytes BLOB NOT NULL,
 UNIQUE(ticket_id,ordinal)
) STRICT;
CREATE TABLE feedback_messages(
 id TEXT PRIMARY KEY,ticket_id TEXT NOT NULL REFERENCES feedback_tickets(id),ordinal INTEGER NOT NULL,
 actor_id TEXT NOT NULL,kind TEXT NOT NULL,body TEXT NOT NULL,created_at INTEGER NOT NULL,
 UNIQUE(ticket_id,ordinal)
) STRICT;
CREATE TABLE feedback_receipts(
 installation_id TEXT NOT NULL REFERENCES feedback_installations(id),account_id TEXT NOT NULL,
 request_key TEXT NOT NULL,intent_digest TEXT NOT NULL,result_json TEXT NOT NULL,
 PRIMARY KEY(installation_id,account_id,request_key)
) STRICT;
CREATE INDEX feedback_ticket_page ON feedback_tickets(installation_id,created_at DESC,id DESC);
CREATE INDEX feedback_reporter_page ON feedback_tickets(installation_id,reporter_id,created_at DESC,id DESC);
PRAGMA user_version=1;

CREATE TRIGGER feedback_meta_no_update BEFORE UPDATE ON feedback_meta BEGIN SELECT RAISE(ABORT,'feedback_immutable'); END;
CREATE TRIGGER feedback_meta_no_delete BEFORE DELETE ON feedback_meta BEGIN SELECT RAISE(ABORT,'feedback_immutable'); END;
CREATE TRIGGER feedback_meta_no_replace BEFORE INSERT ON feedback_meta
WHEN EXISTS(SELECT 1 FROM feedback_meta WHERE key=NEW.key)
BEGIN SELECT RAISE(ABORT,'feedback_immutable'); END;
CREATE TRIGGER feedback_installations_no_update BEFORE UPDATE ON feedback_installations BEGIN SELECT RAISE(ABORT,'feedback_immutable'); END;
CREATE TRIGGER feedback_installations_no_delete BEFORE DELETE ON feedback_installations BEGIN SELECT RAISE(ABORT,'feedback_immutable'); END;
CREATE TRIGGER feedback_installations_no_replace BEFORE INSERT ON feedback_installations
WHEN EXISTS(SELECT 1 FROM feedback_installations WHERE id=NEW.id OR provisioning_key=NEW.provisioning_key)
BEGIN SELECT RAISE(ABORT,'feedback_immutable'); END;
CREATE TRIGGER feedback_provider_receipts_no_update BEFORE UPDATE ON feedback_provider_receipts BEGIN SELECT RAISE(ABORT,'feedback_immutable'); END;
CREATE TRIGGER feedback_provider_receipts_no_delete BEFORE DELETE ON feedback_provider_receipts BEGIN SELECT RAISE(ABORT,'feedback_immutable'); END;
CREATE TRIGGER feedback_provider_receipts_no_replace BEFORE INSERT ON feedback_provider_receipts
WHEN EXISTS(SELECT 1 FROM feedback_provider_receipts WHERE receipt_key=NEW.receipt_key)
BEGIN SELECT RAISE(ABORT,'feedback_immutable'); END;
CREATE TRIGGER feedback_receipts_no_update BEFORE UPDATE ON feedback_receipts BEGIN SELECT RAISE(ABORT,'feedback_immutable'); END;
CREATE TRIGGER feedback_receipts_no_delete BEFORE DELETE ON feedback_receipts BEGIN SELECT RAISE(ABORT,'feedback_immutable'); END;
CREATE TRIGGER feedback_receipts_no_replace BEFORE INSERT ON feedback_receipts
WHEN EXISTS(SELECT 1 FROM feedback_receipts WHERE installation_id=NEW.installation_id AND account_id=NEW.account_id AND request_key=NEW.request_key)
BEGIN SELECT RAISE(ABORT,'feedback_immutable'); END;