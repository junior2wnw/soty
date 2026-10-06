
CREATE TABLE human_identity_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL) STRICT;
CREATE TABLE "human_identity_artifacts"(
 model TEXT NOT NULL CHECK(model IN ('Session','Interaction','Grant','AuthorizationCode','RefreshToken','AccessToken')),
 id_hash TEXT NOT NULL CHECK(length(id_hash)=64),payload_cipher BLOB NOT NULL CHECK(length(payload_cipher) BETWEEN 30 AND 16412),
 payload_digest TEXT NOT NULL CHECK(length(payload_digest)=64),key_id TEXT NOT NULL,
 account_id TEXT,device_id TEXT,client_id TEXT,grant_hash TEXT,uid_hash TEXT,browser_hash TEXT,
 expires_at INTEGER NOT NULL,consumed_at INTEGER,created_at INTEGER NOT NULL,
 retain_until INTEGER NOT NULL CHECK(retain_until>=expires_at),PRIMARY KEY(model,id_hash),CHECK((account_id IS NULL AND device_id IS NULL) OR (account_id IS NOT NULL AND device_id IS NOT NULL))
) STRICT;
CREATE INDEX human_identity_artifact_uid ON human_identity_artifacts(model,uid_hash);
CREATE INDEX human_identity_artifact_grant ON human_identity_artifacts(grant_hash);
CREATE INDEX human_identity_artifact_expiry ON human_identity_artifacts(expires_at);
CREATE INDEX human_identity_artifact_pending ON human_identity_artifacts(model,client_id,browser_hash,expires_at);
CREATE TABLE human_identity_interactions(
 uid_hash TEXT PRIMARY KEY,browser_hash TEXT NOT NULL,csrf_hash TEXT NOT NULL,client_id TEXT NOT NULL,profile_digest TEXT NOT NULL,
 client_generation INTEGER NOT NULL CHECK(client_generation>=1),
 params_digest TEXT NOT NULL,params_cipher BLOB NOT NULL,key_id TEXT NOT NULL,expires_at INTEGER NOT NULL,
 decision TEXT NOT NULL CHECK(decision IN ('pending','approved','denied')),account_id TEXT,device_id TEXT,approved_at INTEGER,
 stay_in_app_seconds INTEGER NOT NULL DEFAULT 0 CHECK(stay_in_app_seconds IN(0,86400)),CHECK((decision='pending' AND account_id IS NULL AND device_id IS NULL AND approved_at IS NULL)
  OR (decision!='pending' AND account_id IS NOT NULL AND device_id IS NOT NULL AND approved_at IS NOT NULL))
) STRICT;
CREATE INDEX human_identity_interaction_expiry ON human_identity_interactions(decision,expires_at);
CREATE TABLE human_identity_decisions(
 account_id TEXT NOT NULL,request_id TEXT NOT NULL,intent_hash TEXT NOT NULL,uid_hash TEXT NOT NULL,
 result_json TEXT NOT NULL CHECK(json_valid(result_json)),created_at INTEGER NOT NULL,PRIMARY KEY(account_id,request_id)
) STRICT;
CREATE INDEX human_identity_decision_uid ON human_identity_decisions(uid_hash);
CREATE TABLE human_identity_grant_bindings(
 grant_hash TEXT PRIMARY KEY,account_id TEXT NOT NULL,device_id TEXT NOT NULL,client_id TEXT NOT NULL,
 interaction_hash TEXT NOT NULL UNIQUE REFERENCES human_identity_interactions(uid_hash),profile_digest TEXT NOT NULL,
 client_generation INTEGER NOT NULL CHECK(client_generation>=1),
 created_at INTEGER NOT NULL,revoked_at INTEGER,
 stay_in_app_seconds INTEGER NOT NULL DEFAULT 0 CHECK(stay_in_app_seconds IN(0,86400)),
 session_expires_at INTEGER CHECK((stay_in_app_seconds=0 AND session_expires_at IS NULL)
  OR (stay_in_app_seconds=86400 AND session_expires_at IS NOT NULL AND session_expires_at>created_at))
) STRICT;
CREATE TABLE human_identity_client_versions(
 client_id TEXT NOT NULL,version INTEGER NOT NULL CHECK(version BETWEEN 1 AND 1000000),
 profile_digest TEXT NOT NULL,created_at INTEGER NOT NULL,PRIMARY KEY(client_id,version)
) STRICT;
CREATE TABLE human_identity_client_heads(
 client_id TEXT PRIMARY KEY,version INTEGER NOT NULL CHECK(version BETWEEN 1 AND 1000000),profile_digest TEXT NOT NULL,
 generation INTEGER NOT NULL CHECK(generation BETWEEN 1 AND 1000000),active INTEGER NOT NULL CHECK(active IN (0,1)),updated_at INTEGER NOT NULL
) STRICT;
CREATE TRIGGER human_identity_meta_no_update BEFORE UPDATE ON human_identity_meta BEGIN SELECT RAISE(ABORT,'human_identity_immutable'); END;
CREATE TRIGGER human_identity_meta_no_delete BEFORE DELETE ON human_identity_meta BEGIN SELECT RAISE(ABORT,'human_identity_immutable'); END;
CREATE TRIGGER human_identity_meta_no_replace BEFORE INSERT ON human_identity_meta WHEN EXISTS(SELECT 1 FROM human_identity_meta WHERE key=NEW.key)
BEGIN SELECT RAISE(ABORT,'human_identity_immutable'); END;CREATE TRIGGER human_identity_decisions_no_update BEFORE UPDATE ON human_identity_decisions BEGIN SELECT RAISE(ABORT,'human_identity_immutable'); END;
CREATE TRIGGER human_identity_decisions_no_replace BEFORE INSERT ON human_identity_decisions
WHEN EXISTS(SELECT 1 FROM human_identity_decisions WHERE account_id=NEW.account_id AND request_id=NEW.request_id)
BEGIN SELECT RAISE(ABORT,'human_identity_immutable'); END;
CREATE TRIGGER human_identity_decisions_no_delete BEFORE DELETE ON human_identity_decisions
WHEN NOT EXISTS(SELECT 1 FROM human_identity_interactions i WHERE i.uid_hash=OLD.uid_hash AND human_identity_gc_epoch()>0 AND i.expires_at<=human_identity_gc_epoch()
 AND (i.decision='pending' OR i.expires_at<=human_identity_gc_epoch()-3660)
 AND NOT EXISTS(SELECT 1 FROM human_identity_artifacts a WHERE a.retain_until>human_identity_gc_epoch()
  AND (a.id_hash=i.uid_hash AND a.model='Interaction' OR a.grant_hash IN
   (SELECT grant_hash FROM human_identity_grant_bindings WHERE interaction_hash=i.uid_hash))))
BEGIN SELECT RAISE(ABORT,'human_identity_immutable'); END;CREATE TRIGGER human_identity_client_versions_no_update BEFORE UPDATE ON human_identity_client_versions BEGIN SELECT RAISE(ABORT,'human_identity_immutable'); END;
CREATE TRIGGER human_identity_client_versions_no_delete BEFORE DELETE ON human_identity_client_versions BEGIN SELECT RAISE(ABORT,'human_identity_immutable'); END;
CREATE TRIGGER human_identity_client_versions_no_replace BEFORE INSERT ON human_identity_client_versions WHEN EXISTS(SELECT 1 FROM human_identity_client_versions WHERE client_id=NEW.client_id AND version=NEW.version)
BEGIN SELECT RAISE(ABORT,'human_identity_immutable'); END;
CREATE TRIGGER human_identity_interaction_pin BEFORE UPDATE ON human_identity_interactions
WHEN NEW.uid_hash<>OLD.uid_hash OR NEW.browser_hash<>OLD.browser_hash OR NEW.csrf_hash<>OLD.csrf_hash OR NEW.client_id<>OLD.client_id
 OR NEW.profile_digest<>OLD.profile_digest OR NEW.client_generation<>OLD.client_generation OR NEW.params_digest<>OLD.params_digest OR NEW.params_cipher<>OLD.params_cipher
 OR NEW.key_id<>OLD.key_id OR NEW.expires_at<>OLD.expires_at OR OLD.decision!='pending'
BEGIN SELECT RAISE(ABORT,'human_identity_interaction_immutable'); END;
CREATE TRIGGER human_identity_interaction_no_replace BEFORE INSERT ON human_identity_interactions
WHEN EXISTS(SELECT 1 FROM human_identity_interactions WHERE uid_hash=NEW.uid_hash)
BEGIN SELECT RAISE(ABORT,'human_identity_interaction_immutable'); END;
CREATE TRIGGER human_identity_interaction_no_delete BEFORE DELETE ON human_identity_interactions
WHEN NOT (human_identity_gc_epoch()>0 AND OLD.expires_at<=human_identity_gc_epoch()
 AND (OLD.decision='pending' OR OLD.expires_at<=human_identity_gc_epoch()-3660)
 AND NOT EXISTS(SELECT 1 FROM human_identity_artifacts a WHERE a.retain_until>human_identity_gc_epoch()
  AND (a.id_hash=OLD.uid_hash AND a.model='Interaction' OR a.grant_hash IN
   (SELECT grant_hash FROM human_identity_grant_bindings WHERE interaction_hash=OLD.uid_hash))))
BEGIN SELECT RAISE(ABORT,'human_identity_interaction_immutable'); END;
CREATE TRIGGER human_identity_grant_pin BEFORE UPDATE ON human_identity_grant_bindings
WHEN NEW.grant_hash<>OLD.grant_hash OR NEW.account_id<>OLD.account_id OR NEW.device_id<>OLD.device_id OR NEW.client_id<>OLD.client_id
 OR NEW.stay_in_app_seconds<>OLD.stay_in_app_seconds OR NEW.session_expires_at IS NOT OLD.session_expires_at OR NEW.interaction_hash<>OLD.interaction_hash OR NEW.profile_digest<>OLD.profile_digest OR NEW.client_generation<>OLD.client_generation OR NEW.created_at<>OLD.created_at
 OR OLD.revoked_at IS NOT NULL OR NEW.revoked_at IS NULL
BEGIN SELECT RAISE(ABORT,'human_identity_grant_immutable'); END;
CREATE TRIGGER human_identity_grant_no_delete BEFORE DELETE ON human_identity_grant_bindings
WHEN NOT EXISTS(SELECT 1 FROM human_identity_interactions i WHERE i.uid_hash=OLD.interaction_hash AND human_identity_gc_epoch()>0 AND i.expires_at<=human_identity_gc_epoch()
 AND (i.decision='pending' OR i.expires_at<=human_identity_gc_epoch()-3660)
 AND NOT EXISTS(SELECT 1 FROM human_identity_artifacts a WHERE a.retain_until>human_identity_gc_epoch()
  AND (a.id_hash=i.uid_hash AND a.model='Interaction' OR a.grant_hash IN
   (SELECT grant_hash FROM human_identity_grant_bindings WHERE interaction_hash=i.uid_hash))))
BEGIN SELECT RAISE(ABORT,'human_identity_grant_immutable'); END;
CREATE TRIGGER human_identity_grant_no_replace BEFORE INSERT ON human_identity_grant_bindings
WHEN EXISTS(SELECT 1 FROM human_identity_grant_bindings WHERE grant_hash=NEW.grant_hash)
BEGIN SELECT RAISE(ABORT,'human_identity_grant_immutable'); END;
CREATE TRIGGER human_identity_client_head_no_delete BEFORE DELETE ON human_identity_client_heads
BEGIN SELECT RAISE(ABORT,'human_identity_client_required'); END;
CREATE TRIGGER human_identity_client_head_no_replace BEFORE INSERT ON human_identity_client_heads
WHEN EXISTS(SELECT 1 FROM human_identity_client_heads WHERE client_id=NEW.client_id)
BEGIN SELECT RAISE(ABORT,'human_identity_client_required'); END;
CREATE TRIGGER human_identity_client_head_monotonic BEFORE UPDATE ON human_identity_client_heads
WHEN NEW.client_id<>OLD.client_id OR NEW.version<OLD.version OR NEW.generation<>OLD.generation+1
BEGIN SELECT RAISE(ABORT,'human_identity_client_required'); END;

CREATE INDEX human_identity_artifact_retention ON human_identity_artifacts(retain_until);
CREATE INDEX human_identity_grant_family_expiry ON human_identity_grant_bindings(stay_in_app_seconds,session_expires_at);
