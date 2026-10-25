ALTER TABLE tenants DROP CONSTRAINT tenants_kind_check;
ALTER TABLE tenants ADD CONSTRAINT tenants_kind_check CHECK (kind IN ('anonymous','legacy','operator','soty'));
ALTER TABLE ledger_entries DROP CONSTRAINT ledger_entries_account_check;
ALTER TABLE ledger_entries ADD CONSTRAINT ledger_entries_account_check CHECK (account IN ('wallet','demo_credit','sponsor_credit','consumption'));

CREATE TABLE soty_accounts (
  issuer text NOT NULL CHECK (length(issuer) BETWEEN 1 AND 512),
  account_id text NOT NULL CHECK (length(account_id) BETWEEN 1 AND 256),
  tenant_id uuid NOT NULL UNIQUE REFERENCES tenants(id),
  state text NOT NULL DEFAULT 'active' CHECK (state IN ('active','suspended','deleted')),
  revision bigint NOT NULL DEFAULT 1 CHECK (revision>0),
  limits jsonb NOT NULL,
  granted_urub bigint NOT NULL CHECK (granted_urub>0),
  created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (issuer,account_id)
);
CREATE FUNCTION immutable_soty_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.issuer<>OLD.issuer OR NEW.account_id<>OLD.account_id OR NEW.tenant_id<>OLD.tenant_id OR NEW.granted_urub<>OLD.granted_urub THEN
   RAISE EXCEPTION 'immutable Soty account identity and grant';
 END IF;
 IF NEW.state IS DISTINCT FROM OLD.state OR NEW.limits IS DISTINCT FROM OLD.limits THEN
   NEW.revision=OLD.revision+1; NEW.updated_at=now();
 ELSE
   NEW.revision=OLD.revision;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER soty_identity_immutable BEFORE UPDATE ON soty_accounts FOR EACH ROW EXECUTE FUNCTION immutable_soty_identity();

CREATE TABLE soty_bridge_nonces (
  issuer text NOT NULL, jti text NOT NULL, expires_at timestamptz NOT NULL,
  PRIMARY KEY (issuer,jti)
);
CREATE INDEX soty_bridge_nonce_expiry ON soty_bridge_nonces(expires_at);

ALTER TABLE inference_requests ADD COLUMN source_context jsonb NOT NULL DEFAULT '{}';
ALTER TABLE inference_requests ADD COLUMN quota_urub bigint NOT NULL DEFAULT 0 CHECK (quota_urub>=0);
ALTER TABLE inference_requests ADD COLUMN quota_tokens bigint NOT NULL DEFAULT 0 CHECK (quota_tokens>=0);
ALTER TABLE inference_requests ADD COLUMN quota_pending boolean NOT NULL DEFAULT false;
ALTER TABLE inference_requests ADD COLUMN dispatch_count integer NOT NULL DEFAULT 0 CHECK (dispatch_count BETWEEN 0 AND 2);
ALTER TABLE provider_attempts ADD COLUMN dispatched boolean NOT NULL DEFAULT true;
ALTER TABLE provider_attempts ADD COLUMN price_snapshot jsonb;
CREATE INDEX inference_tenant_period ON inference_requests(tenant_id,local_month,local_day);

CREATE FUNCTION permanent_account_no_demo_credit() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.operation='demo_topup' AND EXISTS(SELECT 1 FROM tenants WHERE id=NEW.tenant_id AND kind='soty') THEN
   RAISE EXCEPTION 'permanent account cannot receive demo credit';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER soty_no_demo_credit BEFORE INSERT ON ledger_transactions FOR EACH ROW EXECUTE FUNCTION permanent_account_no_demo_credit();
