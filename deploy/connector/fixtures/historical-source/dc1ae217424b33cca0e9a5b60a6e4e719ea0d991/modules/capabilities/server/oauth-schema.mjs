// Additive Capabilities v3 objects. Historical v1/v2 SQL stays in schema-v2.mjs.
export const OAUTH_DDL = `CREATE TABLE cap_oauth_connections(
  id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  client_id TEXT NOT NULL UNIQUE REFERENCES cap_clients(id),
  principal_id TEXT NOT NULL UNIQUE REFERENCES cap_principals(id),
  root_grant_id TEXT NOT NULL UNIQUE REFERENCES cap_grants(id),
  creator_device_id TEXT NOT NULL,
  issuer TEXT NOT NULL,
  static_client_id TEXT NOT NULL
    CHECK(static_client_id IN ('soty-codex-cli','soty-opencode-cli')),
  resource TEXT NOT NULL,
  scope TEXT NOT NULL CHECK(scope='notes.createDraft'),
  consent_digest TEXT NOT NULL
    CHECK(length(consent_digest)=64 AND consent_digest NOT GLOB '*[^0-9a-f]*'),
  provider_grant_id TEXT UNIQUE,
  state TEXT NOT NULL CHECK(state IN ('active','revoked')),
  created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),
  expires_at INTEGER NOT NULL CHECK(expires_at BETWEEN 1 AND 9007199254740991),
  revoked_at INTEGER CHECK(revoked_at BETWEEN 0 AND 9007199254740991),
  CHECK(expires_at>created_at AND expires_at-created_at<=86400000),
  CHECK((state='active' AND revoked_at IS NULL)
     OR (state='revoked' AND revoked_at IS NOT NULL AND revoked_at>=created_at))
) STRICT;
CREATE INDEX cap_oauth_connections_account
  ON cap_oauth_connections(account_id,created_at,id);

CREATE TABLE cap_oauth_interactions(
  uid_hash TEXT PRIMARY KEY
    CHECK(length(uid_hash)=64 AND uid_hash NOT GLOB '*[^0-9a-f]*'),
  issuer TEXT NOT NULL,
  static_client_id TEXT NOT NULL
    CHECK(static_client_id IN ('soty-codex-cli','soty-opencode-cli')),
  resource TEXT NOT NULL,
  redirect_uri TEXT NOT NULL,
  request_digest TEXT NOT NULL
    CHECK(length(request_digest)=64 AND request_digest NOT GLOB '*[^0-9a-f]*'),
  browser_nonce_hash TEXT NOT NULL
    CHECK(length(browser_nonce_hash)=64 AND browser_nonce_hash NOT GLOB '*[^0-9a-f]*'),
  duration_ms INTEGER NOT NULL CHECK(duration_ms BETWEEN 1000 AND 86400000),
  budget_limit INTEGER NOT NULL CHECK(budget_limit BETWEEN 1 AND 20),
  created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),
  expires_at INTEGER NOT NULL CHECK(expires_at BETWEEN 1 AND 9007199254740991),
  decision TEXT NOT NULL CHECK(decision IN ('pending','approved','denied')),
  decided_at INTEGER CHECK(decided_at BETWEEN 0 AND 9007199254740991),
  decided_account_id TEXT,
  decided_device_id TEXT,
  connection_id TEXT UNIQUE REFERENCES cap_oauth_connections(id),
  CHECK(expires_at>created_at AND expires_at-created_at<=600000),
  CHECK((decision='pending' AND decided_at IS NULL AND decided_account_id IS NULL
          AND decided_device_id IS NULL AND connection_id IS NULL)
     OR (decision='approved' AND decided_at IS NOT NULL AND decided_account_id IS NOT NULL
          AND decided_device_id IS NOT NULL AND connection_id IS NOT NULL)
     OR (decision='denied' AND decided_at IS NOT NULL AND decided_account_id IS NOT NULL
          AND decided_device_id IS NOT NULL AND connection_id IS NULL)),
  CHECK(decided_at IS NULL OR (decided_at>=created_at AND decided_at<expires_at))
) STRICT;
CREATE INDEX cap_oauth_interactions_expiry
  ON cap_oauth_interactions(expires_at,uid_hash);

CREATE TABLE cap_oauth_artifacts(
  model TEXT NOT NULL CHECK(model IN
    ('Session','Interaction','Grant','AuthorizationCode','RefreshToken','AccessToken')),
  id_hash TEXT NOT NULL
    CHECK(length(id_hash)=64 AND id_hash NOT GLOB '*[^0-9a-f]*'),
  issuer TEXT NOT NULL,
  profile TEXT NOT NULL CHECK(profile='oidc-provider-9.12.2-c1'),
  key_id TEXT NOT NULL,
  payload_cipher BLOB NOT NULL CHECK(length(payload_cipher) BETWEEN 30 AND 16412),
  payload_digest TEXT NOT NULL
    CHECK(length(payload_digest)=64 AND payload_digest NOT GLOB '*[^0-9a-f]*'),
  connection_id TEXT REFERENCES cap_oauth_connections(id),
  provider_grant_id TEXT,
  session_uid_hash TEXT CHECK(session_uid_hash IS NULL OR
    (length(session_uid_hash)=64 AND session_uid_hash NOT GLOB '*[^0-9a-f]*')),
  created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),
  expires_at INTEGER NOT NULL CHECK(expires_at BETWEEN 1 AND 9007199254740991),
  retain_until INTEGER NOT NULL CHECK(retain_until BETWEEN 1 AND 9007199254740991),
  consumed_at INTEGER CHECK(consumed_at BETWEEN 0 AND 9007199254740991),
  PRIMARY KEY(model,id_hash),
  CHECK(expires_at>created_at AND retain_until>=expires_at),
  CHECK((model IN ('Session','Interaction') AND connection_id IS NULL AND provider_grant_id IS NULL)
     OR (model IN ('Grant','AuthorizationCode','RefreshToken','AccessToken')
          AND connection_id IS NOT NULL AND provider_grant_id IS NOT NULL)),
  CHECK(consumed_at IS NULL OR
    (model IN ('AuthorizationCode','RefreshToken')
      AND consumed_at>=created_at AND consumed_at<expires_at)),
  CHECK(model!='Session' OR session_uid_hash IS NOT NULL)
) STRICT;
CREATE INDEX cap_oauth_artifacts_retention
  ON cap_oauth_artifacts(retain_until,model,id_hash);
CREATE INDEX cap_oauth_artifacts_connection
  ON cap_oauth_artifacts(connection_id,model,expires_at,id_hash);
CREATE INDEX cap_oauth_artifacts_grant
  ON cap_oauth_artifacts(provider_grant_id,model,id_hash);
CREATE UNIQUE INDEX cap_oauth_artifacts_session
  ON cap_oauth_artifacts(session_uid_hash) WHERE model='Session';

CREATE TABLE cap_oauth_credentials(
  credential_id TEXT PRIMARY KEY REFERENCES cap_credentials(id),
  connection_id TEXT NOT NULL REFERENCES cap_oauth_connections(id),
  token_digest TEXT NOT NULL UNIQUE
    CHECK(length(token_digest)=64 AND token_digest NOT GLOB '*[^0-9a-f]*'),
  created_at INTEGER NOT NULL CHECK(created_at BETWEEN 0 AND 9007199254740991),
  expires_at INTEGER NOT NULL CHECK(expires_at BETWEEN 1 AND 9007199254740991),
  CHECK(expires_at>created_at AND expires_at-created_at<=300000)
) STRICT;
CREATE INDEX cap_oauth_credentials_expiry
  ON cap_oauth_credentials(expires_at,credential_id);
CREATE INDEX cap_oauth_credentials_connection
  ON cap_oauth_credentials(connection_id,credential_id);

CREATE INDEX cap_invocations_original_credential
  ON cap_invocations(json_extract(authorization_json,'$.credentialId'));
CREATE INDEX cap_invocations_oauth_request
  ON cap_invocations(account_id,request_key,client_id);`;

export const OAUTH_GUARDS = `
CREATE TRIGGER cap_oauth_connection_admission
BEFORE INSERT ON cap_oauth_connections
WHEN NEW.state!='active' OR NEW.revoked_at IS NOT NULL OR NEW.provider_grant_id IS NOT NULL
  OR NOT EXISTS(SELECT 1 FROM cap_clients c
    JOIN cap_principals p ON p.client_id=c.id
    JOIN cap_grants g ON g.client_id=c.id AND g.principal_id=p.id
    JOIN cap_budgets b ON b.root_grant_id=g.id
    WHERE c.id=NEW.client_id AND c.account_id=NEW.account_id AND c.state='active' AND c.revoked_at IS NULL
      AND p.id=NEW.principal_id AND p.account_id=NEW.account_id AND p.kind='service'
      AND p.state='active' AND p.revoked_at IS NULL AND p.creator_device_id=NEW.creator_device_id
      AND g.id=NEW.root_grant_id AND g.account_id=NEW.account_id AND g.creator_device_id=NEW.creator_device_id
      AND g.root_id=g.id AND g.parent_id IS NULL AND g.depth=0 AND g.allow_delegation=0 AND g.max_depth=0
      AND g.revoked_at IS NULL AND g.expires_at=NEW.expires_at AND g.not_before<=NEW.created_at
      AND g.capabilities_json='[{"capabilityId":"notes.createDraft","version":1}]'
      AND g.resources_json='["notes:new"]' AND g.effects_json='["create"]' AND g.recipients_json='["soty:notes"]'
      AND b.unit='invocations' AND b.limit_amount BETWEEN 1 AND 20
      AND b.reserved_amount=0 AND b.spent_amount=0)
  OR EXISTS(SELECT 1 FROM cap_invocations WHERE client_id=NEW.client_id)
  OR EXISTS(SELECT 1 FROM cap_credentials WHERE client_id=NEW.client_id)
  OR EXISTS(SELECT 1 FROM cap_principals WHERE client_id=NEW.client_id AND id!=NEW.principal_id)
  OR EXISTS(SELECT 1 FROM cap_grants WHERE client_id=NEW.client_id AND id!=NEW.root_grant_id)
BEGIN SELECT RAISE(ABORT,'oauth_connection_binding_invalid'); END;
CREATE TRIGGER cap_oauth_connection_no_replace
BEFORE INSERT ON cap_oauth_connections
WHEN EXISTS(SELECT 1 FROM cap_oauth_connections WHERE id=NEW.id OR client_id=NEW.client_id
  OR principal_id=NEW.principal_id OR root_grant_id=NEW.root_grant_id
  OR (NEW.provider_grant_id IS NOT NULL AND provider_grant_id=NEW.provider_grant_id))
BEGIN SELECT RAISE(ABORT,'oauth_connection_immutable'); END;
CREATE TRIGGER cap_oauth_connection_no_delete
BEFORE DELETE ON cap_oauth_connections
BEGIN SELECT RAISE(ABORT,'oauth_connection_immutable'); END;
CREATE TRIGGER cap_oauth_connection_update_guard
BEFORE UPDATE ON cap_oauth_connections
WHEN NEW.id IS NOT OLD.id OR NEW.account_id IS NOT OLD.account_id OR NEW.client_id IS NOT OLD.client_id
  OR NEW.principal_id IS NOT OLD.principal_id OR NEW.root_grant_id IS NOT OLD.root_grant_id
  OR NEW.creator_device_id IS NOT OLD.creator_device_id OR NEW.issuer IS NOT OLD.issuer
  OR NEW.static_client_id IS NOT OLD.static_client_id OR NEW.resource IS NOT OLD.resource OR NEW.scope IS NOT OLD.scope
  OR NEW.consent_digest IS NOT OLD.consent_digest OR NEW.created_at IS NOT OLD.created_at OR NEW.expires_at IS NOT OLD.expires_at
  OR (NEW.provider_grant_id IS NOT OLD.provider_grant_id AND
    (OLD.provider_grant_id IS NOT NULL OR NEW.provider_grant_id IS NULL OR OLD.state!='active'
      OR NOT EXISTS(SELECT 1 FROM cap_oauth_artifacts a WHERE a.model='Grant'
        AND a.connection_id=OLD.id AND a.issuer=OLD.issuer AND a.provider_grant_id=NEW.provider_grant_id)))
  OR (OLD.state='revoked' AND (NEW.state IS NOT OLD.state OR NEW.revoked_at IS NOT OLD.revoked_at))
  OR (OLD.state='active' AND NOT ((NEW.state='active' AND NEW.revoked_at IS NULL)
    OR (NEW.state='revoked' AND NEW.revoked_at IS NOT NULL AND NEW.revoked_at>=OLD.created_at)))
BEGIN SELECT RAISE(ABORT,'oauth_connection_immutable'); END;
CREATE TRIGGER cap_oauth_interaction_no_replace
BEFORE INSERT ON cap_oauth_interactions
WHEN EXISTS(SELECT 1 FROM cap_oauth_interactions WHERE uid_hash=NEW.uid_hash)
BEGIN SELECT RAISE(ABORT,'oauth_interaction_immutable'); END;
CREATE TRIGGER cap_oauth_interaction_update_guard
BEFORE UPDATE ON cap_oauth_interactions
WHEN NEW.uid_hash IS NOT OLD.uid_hash OR NEW.issuer IS NOT OLD.issuer OR NEW.static_client_id IS NOT OLD.static_client_id
  OR NEW.resource IS NOT OLD.resource OR NEW.redirect_uri IS NOT OLD.redirect_uri
  OR NEW.request_digest IS NOT OLD.request_digest OR NEW.browser_nonce_hash IS NOT OLD.browser_nonce_hash
  OR NEW.duration_ms IS NOT OLD.duration_ms OR NEW.budget_limit IS NOT OLD.budget_limit
  OR NEW.created_at IS NOT OLD.created_at OR NEW.expires_at IS NOT OLD.expires_at
  OR (OLD.decision!='pending' AND (NEW.decision IS NOT OLD.decision OR NEW.decided_at IS NOT OLD.decided_at
    OR NEW.decided_account_id IS NOT OLD.decided_account_id OR NEW.decided_device_id IS NOT OLD.decided_device_id
    OR NEW.connection_id IS NOT OLD.connection_id))
  OR (NEW.decision='approved' AND NOT EXISTS(SELECT 1 FROM cap_oauth_connections c
    JOIN cap_budgets b ON b.root_grant_id=c.root_grant_id AND b.unit='invocations'
    WHERE c.id=NEW.connection_id AND c.issuer=NEW.issuer AND c.static_client_id=NEW.static_client_id
      AND c.resource=NEW.resource AND c.consent_digest=NEW.request_digest
      AND c.account_id=NEW.decided_account_id AND c.creator_device_id=NEW.decided_device_id
      AND c.created_at=NEW.decided_at AND c.expires_at=NEW.decided_at+NEW.duration_ms AND b.limit_amount=NEW.budget_limit))
BEGIN SELECT RAISE(ABORT,'oauth_interaction_immutable'); END;
CREATE TRIGGER cap_oauth_artifact_no_replace
BEFORE INSERT ON cap_oauth_artifacts
WHEN EXISTS(SELECT 1 FROM cap_oauth_artifacts WHERE model=NEW.model AND id_hash=NEW.id_hash)
BEGIN SELECT RAISE(ABORT,'oauth_artifact_immutable'); END;
CREATE TRIGGER cap_oauth_artifact_update_guard
BEFORE UPDATE ON cap_oauth_artifacts
WHEN NEW.model IS NOT OLD.model OR NEW.id_hash IS NOT OLD.id_hash OR NEW.issuer IS NOT OLD.issuer
  OR NEW.profile IS NOT OLD.profile OR NEW.key_id IS NOT OLD.key_id OR NEW.connection_id IS NOT OLD.connection_id
  OR NEW.provider_grant_id IS NOT OLD.provider_grant_id OR NEW.session_uid_hash IS NOT OLD.session_uid_hash
  OR NEW.created_at IS NOT OLD.created_at OR NEW.retain_until IS NOT OLD.retain_until
  OR (OLD.consumed_at IS NOT NULL AND NEW.consumed_at IS NOT OLD.consumed_at)
  OR (OLD.model NOT IN ('Session','Interaction') AND
    (NEW.expires_at IS NOT OLD.expires_at OR NEW.payload_digest IS NOT OLD.payload_digest))
  OR (OLD.model IN ('Session','Interaction') AND
    (NEW.expires_at>OLD.created_at+600000 OR NEW.expires_at>OLD.retain_until))
BEGIN SELECT RAISE(ABORT,'oauth_artifact_immutable'); END;
CREATE TRIGGER cap_oauth_credential_admission
BEFORE INSERT ON cap_oauth_credentials
WHEN NOT EXISTS(SELECT 1 FROM cap_oauth_connections c
  JOIN cap_credentials k ON k.id=NEW.credential_id
  JOIN cap_oauth_artifacts a ON a.model='AccessToken' AND a.id_hash=NEW.token_digest
  WHERE c.id=NEW.connection_id AND c.state='active' AND c.provider_grant_id IS NOT NULL
    AND k.digest=NEW.token_digest AND k.account_id=c.account_id AND k.client_id=c.client_id
    AND k.principal_id=c.principal_id AND k.grant_id=c.root_grant_id AND k.audience=c.resource
    AND k.created_at=NEW.created_at AND k.expires_at=NEW.expires_at AND k.revoked_at IS NULL
    AND NEW.created_at>=c.created_at AND NEW.expires_at<=c.expires_at
    AND a.connection_id=c.id AND a.issuer=c.issuer AND a.provider_grant_id=c.provider_grant_id
    AND a.created_at=NEW.created_at AND a.expires_at=NEW.expires_at)
BEGIN SELECT RAISE(ABORT,'oauth_credential_binding_invalid'); END;
CREATE TRIGGER cap_oauth_credential_no_replace
BEFORE INSERT ON cap_oauth_credentials
WHEN EXISTS(SELECT 1 FROM cap_oauth_credentials WHERE credential_id=NEW.credential_id OR token_digest=NEW.token_digest)
BEGIN SELECT RAISE(ABORT,'oauth_credential_immutable'); END;
CREATE TRIGGER cap_oauth_credential_no_update
BEFORE UPDATE ON cap_oauth_credentials
BEGIN SELECT RAISE(ABORT,'oauth_credential_immutable'); END;
CREATE TRIGGER cap_oauth_credential_delete_guard
BEFORE DELETE ON cap_oauth_credentials
WHEN EXISTS(SELECT 1 FROM cap_invocations WHERE json_extract(authorization_json,'$.credentialId')=OLD.credential_id)
BEGIN SELECT RAISE(ABORT,'oauth_credential_referenced'); END;
CREATE TRIGGER cap_oauth_credential_row_guard
BEFORE UPDATE ON cap_credentials
WHEN EXISTS(SELECT 1 FROM cap_oauth_credentials WHERE credential_id=OLD.id)
  AND (NEW.id IS NOT OLD.id OR NEW.digest IS NOT OLD.digest OR NEW.account_id IS NOT OLD.account_id
    OR NEW.client_id IS NOT OLD.client_id OR NEW.principal_id IS NOT OLD.principal_id OR NEW.grant_id IS NOT OLD.grant_id
    OR NEW.audience IS NOT OLD.audience OR NEW.created_at IS NOT OLD.created_at OR NEW.expires_at IS NOT OLD.expires_at
    OR (OLD.revoked_at IS NOT NULL AND NEW.revoked_at IS NOT OLD.revoked_at)
    OR (NEW.revoked_at IS NOT NULL AND (NEW.revoked_at<OLD.created_at OR NEW.revoked_at>9007199254740991)))
BEGIN SELECT RAISE(ABORT,'oauth_credential_immutable'); END;
CREATE TRIGGER cap_oauth_credential_row_no_replace
BEFORE INSERT ON cap_credentials
WHEN EXISTS(SELECT 1 FROM cap_credentials k JOIN cap_oauth_credentials l ON l.credential_id=k.id
  WHERE k.id=NEW.id OR k.digest=NEW.digest)
BEGIN SELECT RAISE(ABORT,'oauth_credential_immutable'); END;`;
