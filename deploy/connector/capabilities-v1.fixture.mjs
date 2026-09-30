// Test-only literal Capabilities1 DDL from 6cd724a167f3bbca6bdd3120c3d72dfa0f89b60e.
// Do not derive this fixture from a current application migration.
export function createHistoricalCapabilitiesV1(db) {
  db.exec(`BEGIN IMMEDIATE;
    CREATE TABLE cap_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
    INSERT INTO cap_metadata VALUES('lineage', 'soty.capabilities.sqlite.v1');
    CREATE TABLE cap_contracts(
      capability_id TEXT NOT NULL, version INTEGER NOT NULL, digest TEXT NOT NULL,
      PRIMARY KEY(capability_id,version)
    ) STRICT;
    CREATE TABLE cap_clients(
      id TEXT PRIMARY KEY, account_id TEXT NOT NULL, label TEXT NOT NULL,
      state TEXT NOT NULL CHECK(state IN ('active','revoked')), policy_epoch INTEGER NOT NULL DEFAULT 1,
      created_at INTEGER NOT NULL, revoked_at INTEGER
    ) STRICT;
    CREATE INDEX cap_clients_account ON cap_clients(account_id,created_at,id);
    CREATE TABLE cap_principals(
      id TEXT PRIMARY KEY, account_id TEXT NOT NULL, client_id TEXT NOT NULL REFERENCES cap_clients(id),
      kind TEXT NOT NULL CHECK(kind='service'), label TEXT NOT NULL,
      state TEXT NOT NULL CHECK(state IN ('active','revoked')), creator_device_id TEXT NOT NULL,
      created_at INTEGER NOT NULL, revoked_at INTEGER
    ) STRICT;
    CREATE INDEX cap_principals_account ON cap_principals(account_id,created_at,id);
    CREATE TABLE cap_grants(
      id TEXT PRIMARY KEY, account_id TEXT NOT NULL, client_id TEXT NOT NULL REFERENCES cap_clients(id),
      principal_id TEXT NOT NULL REFERENCES cap_principals(id), parent_id TEXT REFERENCES cap_grants(id),
      root_id TEXT NOT NULL REFERENCES cap_grants(id), creator_device_id TEXT NOT NULL,
      capabilities_json TEXT NOT NULL, resources_json TEXT NOT NULL, effects_json TEXT NOT NULL, recipients_json TEXT NOT NULL,
      allow_delegation INTEGER NOT NULL CHECK(allow_delegation IN (0,1)), max_depth INTEGER NOT NULL CHECK(max_depth BETWEEN 0 AND 8),
      depth INTEGER NOT NULL CHECK(depth BETWEEN 0 AND 8), not_before INTEGER NOT NULL, expires_at INTEGER NOT NULL,
      policy_epoch INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, revoked_at INTEGER
    ) STRICT;
    CREATE INDEX cap_grants_account ON cap_grants(account_id,created_at,id);
    CREATE INDEX cap_grants_root ON cap_grants(root_id,id);
    CREATE TABLE cap_credentials(
      id TEXT PRIMARY KEY, digest TEXT NOT NULL UNIQUE, account_id TEXT NOT NULL, client_id TEXT NOT NULL REFERENCES cap_clients(id),
      principal_id TEXT NOT NULL REFERENCES cap_principals(id), grant_id TEXT NOT NULL REFERENCES cap_grants(id),
      audience TEXT NOT NULL, expires_at INTEGER NOT NULL, created_at INTEGER NOT NULL, revoked_at INTEGER
    ) STRICT;
    CREATE INDEX cap_credentials_grant ON cap_credentials(grant_id,id);
    CREATE TABLE cap_audit(
      id TEXT PRIMARY KEY, account_id TEXT NOT NULL, kind TEXT NOT NULL,
      object_type TEXT NOT NULL, object_id TEXT NOT NULL, actor_type TEXT NOT NULL,
      actor_id TEXT NOT NULL, created_at INTEGER NOT NULL
    ) STRICT;
    CREATE INDEX cap_audit_account ON cap_audit(account_id,created_at,id);
    CREATE TABLE cap_budgets(
      root_grant_id TEXT NOT NULL REFERENCES cap_grants(id), unit TEXT NOT NULL CHECK(unit='invocations'),
      limit_amount INTEGER NOT NULL CHECK(limit_amount>=0), reserved_amount INTEGER NOT NULL DEFAULT 0 CHECK(reserved_amount>=0),
      spent_amount INTEGER NOT NULL DEFAULT 0 CHECK(spent_amount>=0),
      PRIMARY KEY(root_grant_id,unit), CHECK(reserved_amount+spent_amount<=limit_amount)
    ) STRICT;
    CREATE TABLE cap_budget_reservations(
      id TEXT PRIMARY KEY, invocation_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
      root_grant_id TEXT NOT NULL REFERENCES cap_grants(id), unit TEXT NOT NULL CHECK(unit='invocations'),
      amount INTEGER NOT NULL CHECK(amount>0), actual_amount INTEGER,
      disposition TEXT NOT NULL CHECK(disposition IN ('reserved','spent','released','uncertain')),
      request_digest TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
      UNIQUE(root_grant_id,invocation_id,attempt_id,unit)
    ) STRICT;
    CREATE TABLE cap_invocations(
      id TEXT PRIMARY KEY, account_id TEXT NOT NULL, client_id TEXT NOT NULL, principal_id TEXT NOT NULL,
      grant_id TEXT NOT NULL, root_grant_id TEXT NOT NULL, policy_epoch INTEGER NOT NULL,
      capability_id TEXT NOT NULL, capability_version INTEGER NOT NULL, capability_digest TEXT NOT NULL,
      request_key TEXT NOT NULL, request_digest TEXT NOT NULL, internal_request_id TEXT NOT NULL UNIQUE,
      input_json TEXT NOT NULL, target_json TEXT NOT NULL, authorization_json TEXT NOT NULL,
      status TEXT NOT NULL, effect_state TEXT NOT NULL DEFAULT 'none' CHECK(effect_state IN ('none','committed','partial','unknown')),
      cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK(cancel_requested IN (0,1)), effects_json TEXT NOT NULL DEFAULT '[]',
      reservation_id TEXT, job_id TEXT UNIQUE, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, completed_at INTEGER,
      UNIQUE(account_id,client_id,request_key)
    ) STRICT;
    CREATE INDEX cap_invocations_history ON cap_invocations(account_id,client_id,created_at,id);
    CREATE TABLE cap_dispatch_intents(
      invocation_id TEXT PRIMARY KEY REFERENCES cap_invocations(id), internal_request_id TEXT NOT NULL UNIQUE,
      state TEXT NOT NULL CHECK(state IN ('pending','dispatching','bound','cancelled','uncertain')),
      created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    ) STRICT;
    CREATE INDEX cap_dispatch_pending ON cap_dispatch_intents(state,created_at,invocation_id);
    CREATE TABLE cap_receipts(
      invocation_id TEXT PRIMARY KEY REFERENCES cap_invocations(id), value_json TEXT NOT NULL, digest TEXT NOT NULL, created_at INTEGER NOT NULL
    ) STRICT;
    PRAGMA user_version=1;
    COMMIT;`);
}
