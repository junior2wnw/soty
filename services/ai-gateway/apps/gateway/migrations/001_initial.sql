CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now());

CREATE TABLE tenants (
  id uuid PRIMARY KEY, name text NOT NULL, kind text NOT NULL CHECK (kind IN ('anonymous','legacy','operator')),
  timezone text NOT NULL DEFAULT 'Asia/Yekaterinburg', created_at timestamptz NOT NULL DEFAULT now(),
  settings jsonb NOT NULL DEFAULT '{"routingMode":"fallback","autoFallback":false}'::jsonb
);
CREATE TABLE device_sessions (
  fingerprint text PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id),
  created_at timestamptz NOT NULL DEFAULT now(), expires_at timestamptz NOT NULL, last_seen_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX device_sessions_expiry ON device_sessions(expires_at);
CREATE TABLE wallets (
  tenant_id uuid PRIMARY KEY REFERENCES tenants(id), balance_urub bigint NOT NULL DEFAULT 0 CHECK (balance_urub >= 0),
  held_urub bigint NOT NULL DEFAULT 0 CHECK (held_urub >= 0 AND held_urub <= balance_urub)
);
CREATE TABLE api_keys (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), name text NOT NULL,
  prefix text NOT NULL, last_four text NOT NULL,
  status text NOT NULL DEFAULT 'active' CHECK (status IN ('active','paused','revoked','pending_activation')),
  purpose text NOT NULL DEFAULT 'customer' CHECK (purpose IN ('customer','chat','legacy')),
  legacy boolean NOT NULL DEFAULT false, legacy_id text,
  policy_revision integer NOT NULL DEFAULT 1, limits jsonb NOT NULL,
  allowed_models jsonb, auto_fallback boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz, last_used_at timestamptz,
  UNIQUE (tenant_id,id)
);
CREATE UNIQUE INDEX api_keys_chat_tenant ON api_keys(tenant_id) WHERE purpose='chat';
CREATE UNIQUE INDEX api_keys_legacy_id ON api_keys(legacy_id) WHERE legacy_id IS NOT NULL;
CREATE TABLE credentials (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL, key_id uuid NOT NULL,
  fingerprint text NOT NULL UNIQUE, pepper_version integer NOT NULL DEFAULT 1,
  version integer NOT NULL DEFAULT 1, created_at timestamptz NOT NULL DEFAULT now(), expires_at timestamptz,
  revoked_at timestamptz,
  FOREIGN KEY (tenant_id,key_id) REFERENCES api_keys(tenant_id,id)
);
CREATE TABLE policy_history (
  key_id uuid NOT NULL REFERENCES api_keys(id), revision integer NOT NULL, policy jsonb NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(key_id,revision)
);
CREATE TABLE idempotency_operations (
  tenant_id uuid NOT NULL REFERENCES tenants(id), operation text NOT NULL, digest text NOT NULL,
  body_digest text NOT NULL, resource_id uuid NOT NULL, safe_result jsonb,
  created_at timestamptz NOT NULL DEFAULT now(), expires_at timestamptz NOT NULL DEFAULT (now()+interval '24 hours'),
  PRIMARY KEY(tenant_id,operation,digest)
);
CREATE TABLE service_daily_budget (
  day date PRIMARY KEY, spent_urub bigint NOT NULL DEFAULT 0 CHECK(spent_urub>=0),
  held_urub bigint NOT NULL DEFAULT 0 CHECK(held_urub>=0)
);
CREATE TABLE service_budget_holds (
  id uuid PRIMARY KEY, day date NOT NULL REFERENCES service_daily_budget(day), amount_urub bigint NOT NULL CHECK(amount_urub>=0),
  state text NOT NULL DEFAULT 'active' CHECK(state IN ('active','settled','released')),
  purpose text NOT NULL, actual_urub bigint NOT NULL DEFAULT 0, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE inference_requests (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL, key_id uuid NOT NULL,
  credential_id uuid REFERENCES credentials(id), purpose text NOT NULL, model_requested text NOT NULL, model_resolved text,
  provider_id text, status text NOT NULL DEFAULT 'running' CHECK(status IN ('running','completed','failed','usage_pending','abandoned')),
  input_tokens bigint, output_tokens bigint, reserved_input_tokens bigint NOT NULL, reserved_output_tokens bigint NOT NULL,
  hold_urub bigint NOT NULL CHECK(hold_urub>=0), charge_urub bigint NOT NULL DEFAULT 0 CHECK(charge_urub>=0),
  price_snapshot jsonb NOT NULL, policy_snapshot jsonb NOT NULL, policy_revision integer NOT NULL,
  local_day date NOT NULL, local_month date NOT NULL, service_budget_hold_id uuid REFERENCES service_budget_holds(id),
  error_code text, finish_reason text, created_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
  lease_expires_at timestamptz NOT NULL DEFAULT (now()+interval '12 minutes'), duration_ms integer,
  body_digest text, idempotency_digest text,
  FOREIGN KEY(tenant_id,key_id) REFERENCES api_keys(tenant_id,id), UNIQUE(tenant_id,id)
);
CREATE UNIQUE INDEX inference_idempotency ON inference_requests(tenant_id,key_id,idempotency_digest) WHERE idempotency_digest IS NOT NULL;
CREATE INDEX inference_key_time ON inference_requests(key_id,created_at DESC);
CREATE INDEX inference_tenant_time ON inference_requests(tenant_id,created_at DESC);
CREATE INDEX inference_running ON inference_requests(lease_expires_at) WHERE status='running';
CREATE TABLE ledger_transactions (
  id uuid PRIMARY KEY, tenant_id uuid NOT NULL REFERENCES tenants(id), operation text NOT NULL,
  business_key text NOT NULL UNIQUE, request_id uuid REFERENCES inference_requests(id),
  amount_urub bigint NOT NULL CHECK(amount_urub>=0), is_demo boolean NOT NULL DEFAULT true,
  recorded_at timestamptz NOT NULL DEFAULT now(), metadata jsonb NOT NULL DEFAULT '{}'
);
CREATE TABLE ledger_entries (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, transaction_id uuid NOT NULL REFERENCES ledger_transactions(id),
  account text NOT NULL CHECK(account IN ('wallet','demo_credit','consumption')), amount_urub bigint NOT NULL
);
CREATE FUNCTION immutable_record() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'append-only record'; END $$;
CREATE TRIGGER immutable_ledger_transactions BEFORE UPDATE OR DELETE ON ledger_transactions FOR EACH ROW EXECUTE FUNCTION immutable_record();
CREATE TRIGGER immutable_ledger_entries BEFORE UPDATE OR DELETE ON ledger_entries FOR EACH ROW EXECUTE FUNCTION immutable_record();
CREATE TRIGGER immutable_policy_history BEFORE UPDATE OR DELETE ON policy_history FOR EACH ROW EXECUTE FUNCTION immutable_record();
CREATE FUNCTION balanced_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE total bigint; count_entries integer;
BEGIN
 SELECT coalesce(sum(amount_urub),0), count(*) INTO total,count_entries FROM ledger_entries WHERE transaction_id=NEW.id;
 IF total<>0 OR count_entries<2 THEN RAISE EXCEPTION 'unbalanced ledger transaction'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER ledger_must_balance AFTER INSERT ON ledger_transactions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION balanced_transaction();
CREATE TABLE provider_attempts (
  id uuid PRIMARY KEY, request_id uuid REFERENCES inference_requests(id), provider_id text NOT NULL, model text NOT NULL,
  purpose text NOT NULL, outcome text NOT NULL, duration_ms integer, ttft_ms integer,
  input_tokens bigint, output_tokens bigint, error_code text, recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX attempts_request ON provider_attempts(request_id);
CREATE TABLE catalog_snapshots (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, digest text NOT NULL UNIQUE,
  models jsonb NOT NULL, recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE health_observations (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, provider_id text, model text NOT NULL, purpose text NOT NULL DEFAULT 'probe',
  outcome text NOT NULL, duration_ms integer, ttft_ms integer, error_code text, recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX health_model_time ON health_observations(model,recorded_at DESC);
CREATE TABLE audit_events (
  id uuid PRIMARY KEY, tenant_id uuid REFERENCES tenants(id), type text NOT NULL, resource_id text,
  data jsonb NOT NULL DEFAULT '{}', recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE TRIGGER immutable_audit BEFORE UPDATE OR DELETE ON audit_events FOR EACH ROW EXECUTE FUNCTION immutable_record();
CREATE TABLE outbox (
  id uuid PRIMARY KEY, tenant_id uuid REFERENCES tenants(id), type text NOT NULL, data jsonb NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now(), processed_at timestamptz
);
