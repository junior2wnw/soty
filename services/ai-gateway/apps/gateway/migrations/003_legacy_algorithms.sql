ALTER TABLE credentials ADD COLUMN fingerprint_algorithm text NOT NULL DEFAULT 'hmac_sha256'
  CHECK(fingerprint_algorithm IN ('hmac_sha256','legacy_sha256'));
ALTER TABLE api_keys ADD COLUMN allowed_paths jsonb NOT NULL DEFAULT '["/v1","/api/inference/v1"]';
